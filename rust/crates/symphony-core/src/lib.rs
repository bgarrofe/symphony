use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};
use tracing::{error, info};

use symphony_codex::{CodexClient, CodexSessionPolicies, CursorCliClient, DynamicToolContext, LinearGraphqlTool};
use symphony_config::Settings;
use symphony_tracker::{Issue, Tracker, TrackerError};
use symphony_workflow::{PromptContext, WorkflowStore};
use symphony_workspace::{
    HookSet, create_workspace, remove_workspace, run_hook, sanitize_issue_identifier,
};

#[derive(Debug, Clone, Serialize)]
pub struct RunningWorker {
    pub issue_id: String,
    pub issue_identifier: String,
    /// Tracker state captured at dispatch time (for per-state concurrency accounting).
    pub issue_state: String,
    pub worker_host: Option<String>,
    pub workspace_path: PathBuf,
    pub started_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub turns_completed: u32,
    pub attempt: u32,
    pub stall_restarts: u32,
}

#[derive(Debug, Clone)]
pub struct RetryEntry {
    pub due_at: Instant,
    pub attempt: u32,
    pub token: u64,
    /// Preferred SSH worker destination when retries are flushed (parity with Elixir metadata).
    pub worker_host: Option<String>,
    pub issue_identifier: String,
    pub workspace_path: PathBuf,
    pub last_error: Option<String>,
}

/// Serializable retry queue row for observability APIs.
#[derive(Debug, Clone, Serialize)]
pub struct RetrySnapshot {
    pub issue_id: String,
    pub issue_identifier: String,
    pub attempt: u32,
    pub due_at: Option<String>,
    pub worker_host: Option<String>,
    pub workspace_path: PathBuf,
    pub error: Option<String>,
}

#[derive(Debug)]
enum RunDisposition {
    Completed,
    Failed(String),
    Stalled(String),
}

#[derive(Debug)]
struct IssueRunSummary {
    issue: Issue,
    turns_completed: u32,
    last_activity_at: DateTime<Utc>,
    usage_tokens: u64,
    disposition: RunDisposition,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct OrchestratorSnapshot {
    pub running: Vec<RunningWorker>,
    pub retrying: Vec<RetrySnapshot>,
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
    /// Preferences captured when retry timers fire; consumed on dispatch.
    retry_worker_pref: HashMap<String, Option<String>>,
    total_tokens: u64,
    retry_token_seq: u64,
    snapshot_publisher: Option<Arc<RwLock<OrchestratorSnapshot>>>,
}

impl<T: Tracker> Orchestrator<T> {
    pub fn new(
        settings: Settings,
        tracker: T,
        workflow_store: WorkflowStore,
        snapshot_publisher: Option<Arc<RwLock<OrchestratorSnapshot>>>,
    ) -> Result<Self, CoreError> {
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
            retry_worker_pref: HashMap::new(),
            total_tokens: 0,
            retry_token_seq: 0,
            snapshot_publisher,
        })
    }

    pub fn snapshot(&self) -> OrchestratorSnapshot {
        OrchestratorSnapshot {
            running: self.running.values().cloned().collect(),
            retrying: self
                .retries
                .iter()
                .map(|(issue_id, entry)| retry_entry_to_snapshot(issue_id, entry))
                .collect(),
            total_tokens: self.total_tokens,
        }
    }

    async fn publish_snapshot(&self) {
        if let Some(p) = &self.snapshot_publisher {
            *p.write().await = self.snapshot();
        }
    }

    pub async fn tick(&mut self) -> Result<(), CoreError> {
        info!("orchestrator tick started");
        let _ = self.workflow_store.reload_if_changed();
        self.reconcile_retries();
        self.reconcile_running_states().await?;
        self.cleanup_terminal_issue_workspaces().await?;
        let mut candidates = self.tracker.fetch_candidate_issues().await?;
        sort_candidates(&mut candidates);
        info!(
            candidate_count = candidates.len(),
            "fetched candidate issues"
        );

        if self.running.len()
            >= self.settings.agent.max_concurrent_agents as usize
        {
            info!(
                max_concurrency = self.settings.agent.max_concurrent_agents,
                "no dispatch capacity available this tick"
            );
            self.publish_snapshot().await;
            return Ok(());
        }

        let mut selected = Vec::new();
        for issue in candidates {
            if !self.is_dispatch_eligible(&issue) {
                continue;
            }
            let global_cap = self.settings.agent.max_concurrent_agents as usize;
            if self.running.len() >= global_cap {
                break;
            }
            let state_limit =
                symphony_config::max_concurrent_agents_for_state(&self.settings, &issue.state)
                    as usize;
            if running_count_for_normalized_state(&self.running, &issue.state) >= state_limit {
                continue;
            }
            let preferred = self.retry_worker_pref.remove(&issue.id).flatten();
            let worker_host = match resolve_worker_execution_host(
                &self.settings,
                &self.running,
                preferred.as_deref(),
            ) {
                WorkerHostDisposition::NoCapacity => continue,
                WorkerHostDisposition::Local => None,
                WorkerHostDisposition::Remote(ref h) => {
                    tracing::debug!(
                        worker_host = %h,
                        issue = %issue.identifier,
                        "worker host labeled for concurrency; Codex invocation is still local-only in Rust builds"
                    );
                    Some(h.clone())
                }
            };

            self.claimed.insert(issue.id.clone());
            let retry_attempt = self.retries.get(&issue.id).map(|r| r.attempt).unwrap_or(1);
            self.running.insert(
                issue.id.clone(),
                RunningWorker {
                    issue_id: issue.id.clone(),
                    issue_identifier: issue.identifier.clone(),
                    issue_state: issue.state.clone(),
                    worker_host,
                    workspace_path: issue_workspace_path(&self.settings, &issue),
                    started_at: Utc::now(),
                    last_activity_at: Utc::now(),
                    turns_completed: 0,
                    attempt: retry_attempt,
                    stall_restarts: retry_attempt.saturating_sub(1),
                },
            );
            selected.push((issue, retry_attempt));
        }

        self.publish_snapshot().await;

        let mut runs = FuturesUnordered::new();
        for (issue, retry_attempt) in &selected {
            runs.push(run_issue_once(
                self.settings.clone(),
                self.workflow_store.clone(),
                issue.clone(),
                *retry_attempt,
            ));
        }

        while let Some(result) = runs.next().await {
            match result {
                Ok(summary) => {
                    self.total_tokens = self.total_tokens.saturating_add(summary.usage_tokens);
                    if let Some(worker) = self.running.get_mut(&summary.issue.id) {
                        worker.turns_completed = summary.turns_completed;
                        worker.last_activity_at = summary.last_activity_at;
                    }
                    match summary.disposition {
                        RunDisposition::Completed => {
                            let host_pref = self
                                .running
                                .get(&summary.issue.id)
                                .and_then(|w| w.worker_host.clone());
                            self.enqueue_continuation_retry(&summary.issue, host_pref);
                        }
                        RunDisposition::Failed(reason) => {
                            error!(issue=%summary.issue.identifier, reason=%reason, "issue run failed");
                            let host_pref = self
                                .running
                                .get(&summary.issue.id)
                                .and_then(|w| w.worker_host.clone());
                            self.enqueue_failure_retry(&summary.issue, host_pref, Some(reason));
                        }
                        RunDisposition::Stalled(reason) => {
                            info!(issue=%summary.issue.identifier, reason=%reason, "run stalled, scheduling restart");
                            let host_pref = self
                                .running
                                .get(&summary.issue.id)
                                .and_then(|w| w.worker_host.clone());
                            self.enqueue_stall_retry(&summary.issue, host_pref, Some(reason));
                        }
                    }
                }
                Err(err) => {
                    error!(error=%err, "issue task failed unexpectedly");
                }
            }
        }

        for (issue, _) in selected {
            self.claimed.remove(&issue.id);
            self.running.remove(&issue.id);
        }
        self.publish_snapshot().await;
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
        if !issue.assigned_to_worker {
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

    fn enqueue_continuation_retry(&mut self, issue: &Issue, worker_host: Option<String>) {
        self.insert_retry_entry(
            issue,
            Instant::now() + Duration::from_secs(1),
            1,
            worker_host,
            None,
        );
    }

    fn enqueue_failure_retry(
        &mut self,
        issue: &Issue,
        worker_host: Option<String>,
        last_error: Option<String>,
    ) {
        let attempt = self
            .retries
            .get(&issue.id)
            .map(|r| r.attempt + 1)
            .unwrap_or(1);
        let delay_ms = failure_retry_delay_ms(attempt, self.settings.agent.max_retry_backoff_ms);
        self.insert_retry_entry(
            issue,
            Instant::now() + Duration::from_millis(delay_ms),
            attempt,
            worker_host,
            last_error,
        );
    }

    fn enqueue_stall_retry(
        &mut self,
        issue: &Issue,
        worker_host: Option<String>,
        last_error: Option<String>,
    ) {
        let attempt = self
            .retries
            .get(&issue.id)
            .map(|r| r.attempt + 1)
            .unwrap_or(1);
        let delay_ms = failure_retry_delay_ms(attempt, self.settings.agent.max_retry_backoff_ms);
        self.insert_retry_entry(
            issue,
            Instant::now() + Duration::from_millis(delay_ms),
            attempt,
            worker_host,
            last_error,
        );
    }

    fn insert_retry_entry(
        &mut self,
        issue: &Issue,
        due_at: Instant,
        attempt: u32,
        worker_host: Option<String>,
        last_error: Option<String>,
    ) {
        self.retry_token_seq = self.retry_token_seq.saturating_add(1);
        self.retries.insert(
            issue.id.clone(),
            RetryEntry {
                due_at,
                attempt,
                token: self.retry_token_seq,
                worker_host,
                issue_identifier: issue.identifier.clone(),
                workspace_path: issue_workspace_path(&self.settings, issue),
                last_error,
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
            if let Some(entry) = self.retries.remove(issue_id) {
                self.retry_worker_pref
                    .insert(issue_id.to_string(), entry.worker_host);
            }
        }
    }

    async fn reconcile_running_states(&mut self) -> Result<(), CoreError> {
        if self.running.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = self.running.keys().cloned().collect();
        let states = self.tracker.fetch_issue_states_by_ids(&ids).await?;
        let by_id: HashMap<String, String> = states.into_iter().map(|s| (s.id, s.state)).collect();
        let stale_ids: Vec<String> = self
            .running
            .keys()
            .filter(|id| {
                by_id
                    .get(*id)
                    .map(|state| self.is_terminal_state(state))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        for issue_id in stale_ids {
            if let Some(worker) = self.running.remove(&issue_id) {
                info!(issue=%worker.issue_identifier, "reconciled running issue no longer active");
                self.cleanup_workspace_with_hooks(&worker.workspace_path)
                    .await?;
            }
        }
        Ok(())
    }

    async fn cleanup_terminal_issue_workspaces(&mut self) -> Result<(), CoreError> {
        let terminal = self.settings.tracker.terminal_states.clone();
        if terminal.is_empty() {
            return Ok(());
        }
        let issues = self.tracker.fetch_issues_by_states(&terminal).await?;
        for issue in issues {
            let workspace_path = issue_workspace_path(&self.settings, &issue);
            self.cleanup_workspace_with_hooks(&workspace_path).await?;
        }
        Ok(())
    }

    async fn cleanup_workspace_with_hooks(
        &self,
        workspace_path: &PathBuf,
    ) -> Result<(), CoreError> {
        if !workspace_path.exists() {
            return Ok(());
        }
        if let Some(cmd) = self.settings.hooks.before_remove.as_deref() {
            let _ = run_hook(
                "before_remove",
                cmd,
                workspace_path,
                self.settings.hooks.timeout_ms,
                false,
            )
            .await;
        }
        remove_workspace(workspace_path)
            .await
            .map_err(|e| CoreError::Runtime(e.to_string()))?;
        Ok(())
    }

    fn is_terminal_state(&self, state: &str) -> bool {
        let normalized = state.to_ascii_lowercase();
        self.settings
            .tracker
            .terminal_states
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&normalized))
    }
}

fn failure_retry_delay_ms(attempt: u32, max_backoff_ms: u64) -> u64 {
    let max_delay_power = attempt.saturating_sub(1).min(10);
    10_000_u64
        .saturating_mul(1u64 << max_delay_power)
        .min(max_backoff_ms)
}

fn running_count_for_normalized_state(
    running: &HashMap<String, RunningWorker>,
    issue_state: &str,
) -> usize {
    let n = symphony_config::normalized_issue_state(issue_state);
    running
        .values()
        .filter(|w| symphony_config::normalized_issue_state(&w.issue_state) == n)
        .count()
}

fn running_count_for_ssh_host(running: &HashMap<String, RunningWorker>, host: &str) -> usize {
    running
        .values()
        .filter(|w| w.worker_host.as_deref() == Some(host))
        .count()
}

#[derive(Debug)]
enum WorkerHostDisposition {
    Local,
    Remote(String),
    NoCapacity,
}

fn resolve_worker_execution_host(
    settings: &Settings,
    running: &HashMap<String, RunningWorker>,
    preferred: Option<&str>,
) -> WorkerHostDisposition {
    let hosts: Vec<String> = settings
        .worker
        .ssh_hosts
        .iter()
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
        .collect();
    if hosts.is_empty() {
        return WorkerHostDisposition::Local;
    }

    let filtered: Vec<&String> = hosts
        .iter()
        .filter(|h| {
            settings
                .worker
                .max_concurrent_agents_per_host
                .map(|limit| running_count_for_ssh_host(running, h) < limit as usize)
                .unwrap_or(true)
        })
        .collect();

    if filtered.is_empty() {
        return WorkerHostDisposition::NoCapacity;
    }

    if let Some(p) = preferred {
        if !p.is_empty() && filtered.iter().any(|h| h.as_str() == p) {
            return WorkerHostDisposition::Remote(p.to_string());
        }
    }

    let picked = filtered
        .iter()
        .enumerate()
        .min_by_key(|(i, host)| (running_count_for_ssh_host(running, host), *i))
        .map(|(_, host)| (*host).clone())
        .expect("non-empty filtered worker hosts");

    WorkerHostDisposition::Remote(picked)
}

fn issue_workspace_path(settings: &Settings, issue: &Issue) -> PathBuf {
    PathBuf::from(&settings.workspace.root).join(sanitize_issue_identifier(&issue.identifier))
}

fn retry_entry_to_snapshot(issue_id: &str, entry: &RetryEntry) -> RetrySnapshot {
    RetrySnapshot {
        issue_id: issue_id.to_string(),
        issue_identifier: entry.issue_identifier.clone(),
        attempt: entry.attempt,
        due_at: Some(due_instant_wall_rfc3339(entry.due_at)),
        worker_host: entry.worker_host.clone(),
        workspace_path: entry.workspace_path.clone(),
        error: entry.last_error.clone(),
    }
}

fn due_instant_wall_rfc3339(deadline: Instant) -> String {
    let now_i = Instant::now();
    let utc = if deadline <= now_i {
        Utc::now()
    } else {
        let d = deadline.duration_since(now_i);
        Utc::now()
            + chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::zero())
    };
    utc.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

async fn run_issue_once(
    settings: Settings,
    workflow_store: WorkflowStore,
    issue: Issue,
    attempt: u32,
) -> Result<IssueRunSummary, CoreError> {
    info!(issue=%issue.identifier, "starting issue run");
    let workspace_root = PathBuf::from(&settings.workspace.root);
    let workspace_path = create_workspace(&workspace_root, &issue.identifier)
        .await
        .map_err(|e| CoreError::Runtime(e.to_string()))?;
    info!(issue=%issue.identifier, workspace=%workspace_path.display(), "workspace ready");
    let hooks = HookSet {
        after_create: settings.hooks.after_create.clone(),
        before_run: settings.hooks.before_run.clone(),
        after_run: settings.hooks.after_run.clone(),
        before_remove: settings.hooks.before_remove.clone(),
        timeout_ms: settings.hooks.timeout_ms,
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

    let mut turns_completed = 0_u32;
    let mut usage_tokens = 0_u64;
    let mut last_activity_at = Utc::now();
    let mut disposition = RunDisposition::Completed;

    for turn_attempt in 1..=settings.agent.max_turns {
        let mut rendered = workflow_store
            .current()
            .render(&PromptContext {
                issue: serde_json::to_value(&issue)
                    .map_err(|e| CoreError::Workflow(e.to_string()))?,
                attempt: attempt.saturating_add(turn_attempt.saturating_sub(1)),
            })
            .map_err(|e| CoreError::Workflow(e.to_string()))?;

        // Cursor CLI does not support dynamic tool injection in the same way as Codex app-server.
        if settings
            .runtime
            .interface
            .eq_ignore_ascii_case("cursor_cli")
        {
            if let (Some(endpoint), Some(token)) = (
                settings.tracker.endpoint.as_deref(),
                settings.tracker.token.as_deref(),
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
            linear_graphql: if settings.tracker.kind.eq_ignore_ascii_case("linear") {
                match (
                    settings.tracker.endpoint.clone(),
                    settings.tracker.token.clone(),
                ) {
                    (Some(endpoint), Some(token)) if !endpoint.is_empty() && !token.is_empty() => {
                        Some(LinearGraphqlTool::new(
                            endpoint,
                            token,
                            settings.codex.read_timeout_ms,
                        ))
                    }
                    _ => None,
                }
            } else {
                None
            },
        };

        let turn = execute_turn(&settings, &workspace_path, &issue, &rendered, tool_context).await;
        match turn {
            Ok(turn) => {
                turns_completed = turns_completed.saturating_add(1);
                last_activity_at = Utc::now();
                info!(
                    issue=%issue.identifier,
                    status=%turn.status,
                    thread_id=?turn.thread_id,
                    turn_id=?turn.turn_id,
                    "codex turn finished"
                );
                if let Some(usage) = turn.usage {
                    usage_tokens = usage_tokens.saturating_add(usage.total_tokens);
                }
                let status = turn.status.to_ascii_lowercase();
                if status == "failed" || status == "cancelled" || status == "canceled" {
                    disposition =
                        RunDisposition::Failed(format!("turn ended with status: {}", turn.status));
                    break;
                }
                if status == "completed" {
                    disposition = RunDisposition::Completed;
                    break;
                }
            }
            Err(err) => {
                if is_stall_error(&err) {
                    disposition = RunDisposition::Stalled(err.to_string());
                } else {
                    disposition = RunDisposition::Failed(err.to_string());
                }
                break;
            }
        }

        if turn_attempt == settings.agent.max_turns {
            disposition = RunDisposition::Failed(format!(
                "reached max_turns={} without terminal completion",
                settings.agent.max_turns
            ));
        }
    }

    if let Some(cmd) = hooks.after_run.as_deref() {
        let _ = run_hook("after_run", cmd, &workspace_path, hooks.timeout_ms, false).await;
    }

    info!(issue=%issue.identifier, "issue run completed");
    Ok(IssueRunSummary {
        issue,
        turns_completed,
        last_activity_at,
        usage_tokens,
        disposition,
    })
}

async fn execute_turn(
    settings: &Settings,
    workspace_path: &PathBuf,
    issue: &Issue,
    rendered: &str,
    tool_context: DynamicToolContext,
) -> Result<symphony_codex::TurnOutcome, CoreError> {
    let cwd = workspace_path.display().to_string();
    let policies = CodexSessionPolicies {
        approval_policy: settings.codex.approval_policy.clone(),
        thread_sandbox: settings.codex.thread_sandbox.clone(),
        turn_sandbox_policy: settings.codex.turn_sandbox_policy.clone(),
    };
    let turn_title = Some(format!("{}: {}", issue.identifier, issue.title));
    if settings
        .runtime
        .interface
        .eq_ignore_ascii_case("cursor_cli")
    {
        let mut client = CursorCliClient::spawn(&settings.codex.command, rendered, workspace_path)
            .await
            .map_err(|e| CoreError::Runtime(e.to_string()))?;
        info!(issue=%issue.identifier, runtime="cursor_cli", "cursor cli process spawned");
        let turn = client
            .initialize(
                "",
                None::<&str>,
                settings.codex.turn_timeout_ms,
                settings.codex.read_timeout_ms,
                settings.codex.stall_timeout_ms,
                tool_context,
                policies.clone(),
                settings.codex.detailed_app_server_logs,
            )
            .await
            .map_err(|e| CoreError::Runtime(e.to_string()))?;
        let _ = client.kill().await;
        Ok(turn)
    } else {
        let mut client = CodexClient::spawn(&settings.codex.command, workspace_path)
            .await
            .map_err(|e| CoreError::Runtime(e.to_string()))?;
        info!(issue=%issue.identifier, runtime="codex", "codex process spawned");
        let turn = client
            .initialize(
                &cwd,
                rendered,
                turn_title.as_deref(),
                settings.codex.turn_timeout_ms,
                settings.codex.read_timeout_ms,
                settings.codex.stall_timeout_ms,
                tool_context,
                policies,
                settings.codex.detailed_app_server_logs,
            )
            .await
            .map_err(|e| CoreError::Runtime(e.to_string()))?;
        let _ = client.kill().await;
        Ok(turn)
    }
}

fn is_stall_error(err: &CoreError) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("stalled waiting for app-server events")
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
    use symphony_config::Settings;

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
                assigned_to_worker: true,
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
                assigned_to_worker: true,
                created_at: t0,
            },
        ];
        sort_candidates(&mut issues);
        assert_eq!(issues[0].id, "1");
    }

    #[test]
    fn retry_entry_has_monotonic_token() {
        let wp = PathBuf::from("/tmp/w");
        let mut entry = RetryEntry {
            due_at: Instant::now(),
            attempt: 1,
            token: 10,
            worker_host: None,
            issue_identifier: "X-1".into(),
            workspace_path: wp.clone(),
            last_error: None,
        };
        let next = RetryEntry {
            due_at: Instant::now(),
            attempt: entry.attempt + 1,
            token: entry.token + 1,
            worker_host: None,
            issue_identifier: "X-1".into(),
            workspace_path: wp,
            last_error: Some("boom".into()),
        };
        entry = next;
        assert_eq!(entry.token, 11);
        assert_eq!(entry.attempt, 2);
    }

    #[test]
    fn workspace_path_uses_sanitized_identifier() {
        let settings = Settings::default();
        let issue = Issue {
            id: "1".into(),
            identifier: "SYM 1/#".into(),
            title: "".into(),
            description: "".into(),
            labels: vec![],
            url: "".into(),
            priority: 1,
            state: "todo".into(),
            blocked_by: vec![],
            assigned_to_worker: true,
            created_at: Utc::now(),
        };
        let workspace = issue_workspace_path(&settings, &issue);
        assert!(workspace.ends_with("SYM_1__"));
    }

    #[test]
    fn failure_retry_delay_respects_max_backoff() {
        assert_eq!(failure_retry_delay_ms(1, 300_000), 10_000);
        assert_eq!(failure_retry_delay_ms(22, 300_000), 300_000);
    }

    #[test]
    fn worker_host_resolution_local_when_no_ssh_hosts() {
        let mut settings = Settings::default();
        settings.worker.ssh_hosts.clear();
        let running: HashMap<String, RunningWorker> = HashMap::new();
        assert!(matches!(
            resolve_worker_execution_host(&settings, &running, None),
            WorkerHostDisposition::Local
        ));
    }

    #[test]
    fn state_concurrency_limit_uses_config_map() {
        let mut settings = Settings::default();
        settings.agent.max_concurrent_agents = 10;
        settings
            .agent
            .max_concurrent_agents_by_state
            .insert("todo".into(), 2);
        assert_eq!(
            symphony_config::max_concurrent_agents_for_state(&settings, "Todo"),
            2
        );
    }

    #[test]
    fn detects_stall_error_signature() {
        let err = CoreError::Runtime("stalled waiting for app-server events".into());
        assert!(is_stall_error(&err));
    }
}
