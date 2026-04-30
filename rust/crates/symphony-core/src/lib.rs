use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use thiserror::Error;
use tokio::time::{Duration, Instant};
use tracing::{error, info};

use symphony_codex::{CodexClient, CursorCliClient, DynamicToolContext, LinearGraphqlTool};
use symphony_config::Settings;
use symphony_tracker::{Issue, Tracker, TrackerError};
use symphony_workflow::{PromptContext, WorkflowStore};
use symphony_workspace::{HookSet, create_workspace, run_hook};

#[derive(Debug, Clone)]
pub struct RunningWorker {
    pub issue_id: String,
    pub issue_identifier: String,
    pub started_at: DateTime<Utc>,
    pub attempt: u32,
}

#[derive(Debug, Clone)]
pub struct RetryEntry {
    pub due_at: Instant,
    pub attempt: u32,
    pub token: u64,
}

#[derive(Debug, Default, Clone)]
pub struct OrchestratorSnapshot {
    pub running: Vec<RunningWorker>,
    pub retrying: Vec<String>,
    pub total_tokens: u64,
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("config invalid: {0}")]
    Config(String),
    #[error("tracker error: {0}")]
    Tracker(#[from] TrackerError),
    #[error("workflow error: {0}")]
    Workflow(String),
    #[error("runtime error: {0}")]
    Runtime(String),
}

pub struct Orchestrator<T: Tracker> {
    settings: Settings,
    tracker: T,
    workflow_store: WorkflowStore,
    running: HashMap<String, RunningWorker>,
    claimed: BTreeSet<String>,
    retries: BTreeMap<String, RetryEntry>,
    total_tokens: u64,
    retry_token_seq: u64,
}

impl<T: Tracker> Orchestrator<T> {
    pub fn new(settings: Settings, tracker: T, workflow_store: WorkflowStore) -> Result<Self, CoreError> {
        settings
            .validate()
            .map_err(|e| CoreError::Config(e.to_string()))?;
        Ok(Self {
            settings,
            tracker,
            workflow_store,
            running: HashMap::new(),
            claimed: BTreeSet::new(),
            retries: BTreeMap::new(),
            total_tokens: 0,
            retry_token_seq: 0,
        })
    }

    pub fn snapshot(&self) -> OrchestratorSnapshot {
        OrchestratorSnapshot {
            running: self.running.values().cloned().collect(),
            retrying: self.retries.keys().cloned().collect(),
            total_tokens: self.total_tokens,
        }
    }

    pub async fn tick(&mut self) -> Result<(), CoreError> {
        info!("orchestrator tick started");
        let _ = self.workflow_store.reload_if_changed();
        self.reconcile_retries();
        let mut candidates = self.tracker.fetch_candidate_issues().await?;
        sort_candidates(&mut candidates);
        info!(candidate_count = candidates.len(), "fetched candidate issues");

        for issue in candidates {
            if !self.is_dispatch_eligible(&issue) {
                continue;
            }
            if self.running.len() >= 1 {
                break;
            }
            self.claimed.insert(issue.id.clone());
            if let Err(err) = self.run_issue(issue.clone()).await {
                error!(issue=%issue.identifier, error=%err, "issue run failed");
                self.enqueue_failure_retry(&issue);
            } else {
                self.enqueue_continuation_retry(&issue);
            }
            self.claimed.remove(&issue.id);
        }
        info!("orchestrator tick finished");
        Ok(())
    }

    fn reconcile_retries(&mut self) {
        let now = Instant::now();
        let due: Vec<(String, u64)> = self
            .retries
            .iter()
            .filter_map(|(k, v)| (v.due_at <= now).then_some((k.clone(), v.token)))
            .collect();
        for (issue_id, token) in due {
            self.consume_retry_if_current(&issue_id, token);
        }
    }

    fn is_dispatch_eligible(&self, issue: &Issue) -> bool {
        if issue.is_terminal() {
            return false;
        }
        if self.running.contains_key(&issue.id) || self.claimed.contains(&issue.id) {
            return false;
        }
        if !issue.blocked_by.is_empty() && issue.state.eq_ignore_ascii_case("todo") {
            return false;
        }
        true
    }

    fn enqueue_continuation_retry(&mut self, issue: &Issue) {
        self.retry_token_seq = self.retry_token_seq.saturating_add(1);
        self.retries.insert(
            issue.id.clone(),
            RetryEntry {
                due_at: Instant::now() + Duration::from_secs(1),
                attempt: 1,
                token: self.retry_token_seq,
            },
        );
    }

    fn enqueue_failure_retry(&mut self, issue: &Issue) {
        let attempt = self.retries.get(&issue.id).map(|r| r.attempt + 1).unwrap_or(1);
        let delay = (10_u64.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1))))
            .min(300);
        self.retry_token_seq = self.retry_token_seq.saturating_add(1);
        self.retries.insert(
            issue.id.clone(),
            RetryEntry {
                due_at: Instant::now() + Duration::from_secs(delay),
                attempt,
                token: self.retry_token_seq,
            },
        );
    }

    fn consume_retry_if_current(&mut self, issue_id: &str, token: u64) {
        if self
            .retries
            .get(issue_id)
            .map(|entry| entry.token == token)
            .unwrap_or(false)
        {
            self.retries.remove(issue_id);
        }
    }

    async fn run_issue(&mut self, issue: Issue) -> Result<(), CoreError> {
        info!(issue=%issue.identifier, "starting issue run");
        let workspace_root = PathBuf::from(&self.settings.workspace.root);
        let workspace_path = create_workspace(&workspace_root, &issue.identifier)
            .await
            .map_err(|e| CoreError::Runtime(e.to_string()))?;
        info!(issue=%issue.identifier, workspace=%workspace_path.display(), "workspace ready");
        let hooks = HookSet {
            after_create: self.settings.hooks.after_create.clone(),
            before_run: self.settings.hooks.before_run.clone(),
            after_run: self.settings.hooks.after_run.clone(),
            before_remove: self.settings.hooks.before_remove.clone(),
            timeout_ms: self.settings.hooks.timeout_ms,
        };
        if let Some(cmd) = hooks.after_create.as_deref() {
            run_hook("after_create", cmd, &workspace_path, hooks.timeout_ms, true)
                .await
                .map_err(|e| CoreError::Runtime(e.to_string()))?;
        }
        if let Some(cmd) = hooks.before_run.as_deref() {
            run_hook("before_run", cmd, &workspace_path, hooks.timeout_ms, true)
                .await
                .map_err(|e| CoreError::Runtime(e.to_string()))?;
        }

        self.running.insert(
            issue.id.clone(),
            RunningWorker {
                issue_id: issue.id.clone(),
                issue_identifier: issue.identifier.clone(),
                started_at: Utc::now(),
                attempt: 1,
            },
        );

        let mut rendered = self
            .workflow_store
            .current()
            .render(&PromptContext {
                issue: serde_json::to_value(&issue).map_err(|e| CoreError::Workflow(e.to_string()))?,
                attempt: 1,
            })
            .map_err(|e| CoreError::Workflow(e.to_string()))?;

        // Cursor CLI does not support dynamic tool injection in the same way as Codex
        // app-server. Provide a workspace-local helper command for linear_graphql access.
        if self
            .settings
            .runtime
            .interface
            .eq_ignore_ascii_case("cursor_cli")
        {
            if let (Some(endpoint), Some(token)) = (
                self.settings.tracker.endpoint.as_deref(),
                self.settings.tracker.token.as_deref(),
            ) {
                if !endpoint.is_empty() && !token.is_empty() {
                    let helper_path = workspace_path.join("linear_graphql");
                    let helper = render_linear_graphql_helper(endpoint, token);
                    tokio::fs::write(&helper_path, helper)
                        .await
                        .map_err(|e| CoreError::Runtime(e.to_string()))?;
                    tokio::fs::set_permissions(
                        &helper_path,
                        std::fs::Permissions::from_mode(0o700),
                    )
                    .await
                    .map_err(|e| CoreError::Runtime(e.to_string()))?;
                    rendered.push_str(
                        "\n\nRuntime note: `linear_graphql` is available as an executable in the current workspace. Use it for Linear GraphQL calls when needed.\nUsage: `./linear_graphql '<query>' '{\"variables\":{...}}'`\n",
                    );
                    info!(path=%helper_path.display(), "cursor cli linear_graphql helper provisioned");
                }
            }
        }

        let tool_context = DynamicToolContext {
            linear_graphql: if self.settings.tracker.kind.eq_ignore_ascii_case("linear") {
                match (
                    self.settings.tracker.endpoint.clone(),
                    self.settings.tracker.token.clone(),
                ) {
                    (Some(endpoint), Some(token)) if !endpoint.is_empty() && !token.is_empty() => {
                        Some(LinearGraphqlTool::new(endpoint, token))
                    }
                    _ => None,
                }
            } else {
                None
            },
        };
        let turn = if self
            .settings
            .runtime
            .interface
            .eq_ignore_ascii_case("cursor_cli")
        {
            let mut client =
                CursorCliClient::spawn(&self.settings.codex.command, &rendered, &workspace_path)
                    .await
                    .map_err(|e| CoreError::Runtime(e.to_string()))?;
            info!(issue=%issue.identifier, runtime="cursor_cli", "cursor cli process spawned");
            let turn = client
                .initialize(
                    &rendered,
                    self.settings.codex.turn_timeout_ms,
                    self.settings.codex.stall_timeout_ms,
                    tool_context.clone(),
                    self.settings.codex.detailed_app_server_logs,
                )
                .await
                .map_err(|e| CoreError::Runtime(e.to_string()))?;
            let _ = client.kill().await;
            turn
        } else {
            let mut client = CodexClient::spawn(&self.settings.codex.command, &workspace_path)
                .await
                .map_err(|e| CoreError::Runtime(e.to_string()))?;
            info!(issue=%issue.identifier, runtime="codex", "codex process spawned");
            let turn = client
                .initialize(
                    &rendered,
                    self.settings.codex.turn_timeout_ms,
                    self.settings.codex.stall_timeout_ms,
                    tool_context,
                    self.settings.codex.detailed_app_server_logs,
                )
                .await
                .map_err(|e| CoreError::Runtime(e.to_string()))?;
            let _ = client.kill().await;
            turn
        };
        info!(
            issue=%issue.identifier,
            status=%turn.status,
            thread_id=?turn.thread_id,
            turn_id=?turn.turn_id,
            "codex turn finished"
        );
        if let Some(usage) = turn.usage {
            self.total_tokens = self.total_tokens.saturating_add(usage.total_tokens);
        }

        if let Some(cmd) = hooks.after_run.as_deref() {
            let _ = run_hook("after_run", cmd, &workspace_path, hooks.timeout_ms, false).await;
        }
        self.running.remove(&issue.id);
        info!(issue=%issue.identifier, "issue run completed");
        Ok(())
    }
}

fn render_linear_graphql_helper(endpoint: &str, token: &str) -> String {
    format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
QUERY="${{1:-}}"
VARS="${{2:-{{}}}}"
if [[ -z "$QUERY" ]]; then
  echo "usage: ./linear_graphql '<query>' '{{\"variables\":{{...}}}}'" >&2
  exit 1
fi
python3 - "$QUERY" "$VARS" <<'PY'
import json
import sys
import urllib.request

query = sys.argv[1]
vars_raw = sys.argv[2]
try:
    variables = json.loads(vars_raw)
except Exception:
    variables = {{}}

payload = json.dumps({{"query": query, "variables": variables}}).encode("utf-8")
req = urllib.request.Request(
    "{endpoint}",
    data=payload,
    headers={{
        "Authorization": "Bearer {token}",
        "Content-Type": "application/json",
    }},
    method="POST",
)
with urllib.request.urlopen(req) as resp:
    sys.stdout.write(resp.read().decode("utf-8"))
PY
"#
    )
}

pub fn sort_candidates(candidates: &mut [Issue]) {
    candidates.sort_by_key(|i| (i.priority, i.created_at, i.identifier.clone()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_by_priority_created_and_identifier() {
        let t0 = Utc::now();
        let mut issues = vec![
            Issue {
                id: "2".into(),
                identifier: "B-2".into(),
                title: "".into(),
                description: "".into(),
                labels: vec![],
                url: "".into(),
                priority: 2,
                state: "todo".into(),
                blocked_by: vec![],
                assigned_to_worker: None,
                created_at: t0,
            },
            Issue {
                id: "1".into(),
                identifier: "A-1".into(),
                title: "".into(),
                description: "".into(),
                labels: vec![],
                url: "".into(),
                priority: 1,
                state: "todo".into(),
                blocked_by: vec![],
                assigned_to_worker: None,
                created_at: t0,
            },
        ];
        sort_candidates(&mut issues);
        assert_eq!(issues[0].id, "1");
    }

    #[test]
    fn retry_entry_has_monotonic_token() {
        let mut entry = RetryEntry {
            due_at: Instant::now(),
            attempt: 1,
            token: 10,
        };
        let next = RetryEntry {
            due_at: Instant::now(),
            attempt: entry.attempt + 1,
            token: entry.token + 1,
        };
        entry = next;
        assert_eq!(entry.token, 11);
        assert_eq!(entry.attempt, 2);
    }
}
