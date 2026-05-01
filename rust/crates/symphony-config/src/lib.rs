use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("unable to read workflow file at {path}: {source}")]
    WorkflowRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("workflow front matter parse error: {0}")]
    FrontMatterParse(serde_yaml::Error),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    #[serde(default)]
    pub polling: PollingConfig,
    #[serde(default)]
    pub workspace: WorkspaceConfig,
    #[serde(default)]
    pub hooks: HookConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub codex: CodexConfig,
    #[serde(default)]
    pub tracker: TrackerConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollingConfig {
    #[serde(default = "defaults::poll_interval_ms")]
    pub interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    #[serde(default = "defaults::workspace_root")]
    pub root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookConfig {
    #[serde(default)]
    pub after_create: Option<String>,
    #[serde(default)]
    pub before_run: Option<String>,
    #[serde(default)]
    pub after_run: Option<String>,
    #[serde(default)]
    pub before_remove: Option<String>,
    #[serde(default = "defaults::hook_timeout_ms")]
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    #[serde(default)]
    pub ssh_hosts: Vec<String>,
    #[serde(default)]
    pub max_concurrent_agents_per_host: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    #[serde(default = "defaults::max_turns")]
    pub max_turns: u32,
    #[serde(default = "defaults::max_concurrent_agents")]
    pub max_concurrent_agents: u32,
    #[serde(default = "defaults::max_retry_backoff_ms")]
    pub max_retry_backoff_ms: u64,
    #[serde(default)]
    pub max_concurrent_agents_by_state: HashMap<String, u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexConfig {
    #[serde(default = "defaults::codex_command")]
    pub command: String,
    #[serde(default = "defaults::codex_approval_policy")]
    pub approval_policy: serde_json::Value,
    #[serde(default = "defaults::thread_sandbox")]
    pub thread_sandbox: String,
    #[serde(default)]
    pub turn_sandbox_policy: Option<serde_json::Value>,
    #[serde(default = "defaults::turn_timeout_ms")]
    pub turn_timeout_ms: u64,
    #[serde(default = "defaults::read_timeout_ms")]
    pub read_timeout_ms: u64,
    #[serde(default = "defaults::stall_timeout_ms")]
    pub stall_timeout_ms: u64,
    #[serde(default = "defaults::detailed_app_server_logs")]
    pub detailed_app_server_logs: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerConfig {
    #[serde(default = "defaults::tracker_kind")]
    pub kind: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default, alias = "project_slug")]
    pub project: Option<String>,
    #[serde(default = "defaults::active_states")]
    pub active_states: Vec<String>,
    #[serde(default = "defaults::terminal_states")]
    pub terminal_states: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "defaults::runtime_interface")]
    pub interface: String,
    /// When true, the CLI must pass `--i-understand-that-this-will-be-running-without-the-usual-guardrails`.
    #[serde(default)]
    pub require_guardrails_ack: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "defaults::server_host")]
    pub host: String,
    /// When zero, the observability HTTP server is not started (unless overridden by CLI `--port`).
    #[serde(default)]
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// When true with `server.port > 0`, exposes JSON API routes. If `server.port` is non-zero, the API is enabled regardless (operators can set port only).
    #[serde(default)]
    pub api_enabled: bool,
    #[serde(default = "defaults::observability_web_dashboard_enabled")]
    pub web_dashboard_enabled: bool,
    #[serde(default = "defaults::observability_refresh_ms")]
    pub refresh_ms: u64,
}

/// True when settings (after CLI merge) should bind an observability HTTP server.
pub fn observability_http_enabled(settings: &Settings) -> bool {
    settings.server.port > 0
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            polling: PollingConfig::default(),
            workspace: WorkspaceConfig::default(),
            hooks: HookConfig::default(),
            worker: WorkerConfig::default(),
            agent: AgentConfig::default(),
            codex: CodexConfig::default(),
            tracker: TrackerConfig::default(),
            runtime: RuntimeConfig::default(),
            server: ServerConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            interval_ms: defaults::poll_interval_ms(),
        }
    }
}

impl Default for WorkspaceConfig {
    fn default() -> Self {
        Self {
            root: defaults::workspace_root(),
        }
    }
}

impl Default for HookConfig {
    fn default() -> Self {
        Self {
            after_create: None,
            before_run: None,
            after_run: None,
            before_remove: None,
            timeout_ms: defaults::hook_timeout_ms(),
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            ssh_hosts: Vec::new(),
            max_concurrent_agents_per_host: None,
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            max_turns: defaults::max_turns(),
            max_concurrent_agents: defaults::max_concurrent_agents(),
            max_retry_backoff_ms: defaults::max_retry_backoff_ms(),
            max_concurrent_agents_by_state: HashMap::new(),
        }
    }
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            command: defaults::codex_command(),
            approval_policy: defaults::codex_approval_policy(),
            thread_sandbox: defaults::thread_sandbox(),
            turn_sandbox_policy: None,
            turn_timeout_ms: defaults::turn_timeout_ms(),
            read_timeout_ms: defaults::read_timeout_ms(),
            stall_timeout_ms: defaults::stall_timeout_ms(),
            detailed_app_server_logs: defaults::detailed_app_server_logs(),
        }
    }
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self {
            kind: defaults::tracker_kind(),
            endpoint: None,
            token: None,
            project: None,
            active_states: defaults::active_states(),
            terminal_states: defaults::terminal_states(),
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            interface: defaults::runtime_interface(),
            require_guardrails_ack: false,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: defaults::server_host(),
            port: 0,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            api_enabled: false,
            web_dashboard_enabled: defaults::observability_web_dashboard_enabled(),
            refresh_ms: defaults::observability_refresh_ms(),
        }
    }
}

impl Settings {
    pub fn from_workflow_front_matter(front_matter: Option<&str>) -> Result<Self, ConfigError> {
        let Some(front_matter) = front_matter else {
            return Ok(Self::default());
        };
        if front_matter.trim().is_empty() {
            return Ok(Self::default());
        }
        let mut config: Self =
            serde_yaml::from_str(front_matter).map_err(ConfigError::FrontMatterParse)?;
        config.resolve_env_overrides();
        config.validate()?;
        Ok(config)
    }

    pub fn load_from_workflow_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).map_err(|source| ConfigError::WorkflowRead {
            path: path.to_path_buf(),
            source,
        })?;
        let front_matter = extract_front_matter(&raw);
        Self::from_workflow_front_matter(front_matter)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.polling.interval_ms == 0 {
            return Err(ConfigError::Invalid(
                "polling.interval_ms must be > 0".into(),
            ));
        }
        if self.workspace.root.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "workspace.root cannot be empty".into(),
            ));
        }
        if self.workspace.root.trim() == "/" {
            return Err(ConfigError::Invalid(
                "workspace.root cannot be filesystem root".into(),
            ));
        }
        if self.agent.max_turns == 0 {
            return Err(ConfigError::Invalid("agent.max_turns must be > 0".into()));
        }
        if self.agent.max_concurrent_agents == 0 {
            return Err(ConfigError::Invalid(
                "agent.max_concurrent_agents must be > 0".into(),
            ));
        }
        if self.codex.command.trim().is_empty() {
            return Err(ConfigError::Invalid("codex.command cannot be empty".into()));
        }
        if self.codex.turn_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "codex.turn_timeout_ms must be > 0".into(),
            ));
        }
        if self.codex.read_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "codex.read_timeout_ms must be > 0".into(),
            ));
        }
        if self.codex.stall_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "codex.stall_timeout_ms must be > 0".into(),
            ));
        }
        if self.codex.thread_sandbox.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "codex.thread_sandbox cannot be empty".into(),
            ));
        }
        if self.agent.max_retry_backoff_ms == 0 {
            return Err(ConfigError::Invalid(
                "agent.max_retry_backoff_ms must be > 0".into(),
            ));
        }
        for (state, limit) in &self.agent.max_concurrent_agents_by_state {
            if state.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "agent.max_concurrent_agents_by_state keys cannot be blank".into(),
                ));
            }
            if *limit == 0 {
                return Err(ConfigError::Invalid(format!(
                    "agent.max_concurrent_agents_by_state[{state:?}] must be > 0"
                )));
            }
        }
        if let Some(per_host) = self.worker.max_concurrent_agents_per_host {
            if per_host == 0 {
                return Err(ConfigError::Invalid(
                    "worker.max_concurrent_agents_per_host must be > 0 when set".into(),
                ));
            }
            if self.worker.ssh_hosts.is_empty() {
                return Err(ConfigError::Invalid(
                    "worker.max_concurrent_agents_per_host requires worker.ssh_hosts to be non-empty"
                        .into(),
                ));
            }
        }
        let iface = self.runtime.interface.trim().to_ascii_lowercase();
        if iface != "codex" && iface != "cursor_cli" {
            return Err(ConfigError::Invalid(
                "runtime.interface must be either \"codex\" or \"cursor_cli\"".into(),
            ));
        }
        if self.server.host.trim().is_empty() {
            return Err(ConfigError::Invalid("server.host cannot be empty".into()));
        }
        if self.observability.api_enabled && self.server.port == 0 {
            return Err(ConfigError::Invalid(
                "observability.api_enabled requires server.port > 0".into(),
            ));
        }
        if self.observability.refresh_ms == 0 {
            return Err(ConfigError::Invalid(
                "observability.refresh_ms must be > 0".into(),
            ));
        }
        Ok(())
    }

    pub fn resolve_env_overrides(&mut self) {
        self.workspace.root = resolve_env_ref(&self.workspace.root);
        self.codex.command = resolve_env_ref(&self.codex.command);
        self.tracker.endpoint = self.tracker.endpoint.as_deref().map(resolve_env_ref);
        self.tracker.token = self.tracker.token.as_deref().map(resolve_env_ref);
        self.tracker.project = self.tracker.project.as_deref().map(resolve_env_ref);
    }
}

/// Normalizes Linear-style state names for map lookups (trim + ASCII lowercase).
pub fn normalized_issue_state(state: &str) -> String {
    state.trim().to_ascii_lowercase()
}

/// Effective concurrency cap for an issue in its current tracker state.
pub fn max_concurrent_agents_for_state(settings: &Settings, issue_state: &str) -> u32 {
    let normalized = normalized_issue_state(issue_state);
    settings
        .agent
        .max_concurrent_agents_by_state
        .iter()
        .find(|(k, _)| normalized_issue_state(k) == normalized)
        .map(|(_, limit)| *limit)
        .unwrap_or(settings.agent.max_concurrent_agents)
}

/// When `true`, the Codex client should auto-respond to in-session approval prompts
/// (`approval_policy == "never"` matches Elixir behavior).
pub fn codex_auto_approve_incoming_requests(approval_policy: &serde_json::Value) -> bool {
    matches!(
        approval_policy,
        serde_json::Value::String(s) if s.eq_ignore_ascii_case("never")
    )
}

pub fn workflow_path(explicit: Option<PathBuf>) -> PathBuf {
    explicit.unwrap_or_else(|| PathBuf::from("./WORKFLOW.md"))
}

pub fn extract_front_matter(raw: &str) -> Option<&str> {
    if !raw.starts_with("---\n") {
        return None;
    }
    let rest = &raw[4..];
    rest.find("\n---\n").map(|idx| &rest[..idx])
}

fn resolve_env_ref(value: &str) -> String {
    if let Some(key) = value.strip_prefix('$') {
        std::env::var(key).unwrap_or_default()
    } else {
        value.to_owned()
    }
}

mod defaults {
    pub fn poll_interval_ms() -> u64 {
        5_000
    }
    pub fn workspace_root() -> String {
        "./.symphony/workspaces".to_string()
    }
    pub fn hook_timeout_ms() -> u64 {
        30_000
    }
    pub fn max_turns() -> u32 {
        4
    }
    pub fn max_concurrent_agents() -> u32 {
        1
    }
    pub fn max_retry_backoff_ms() -> u64 {
        300_000
    }
    pub fn codex_command() -> String {
        "codex app-server".to_string()
    }
    pub fn stall_timeout_ms() -> u64 {
        120_000
    }
    pub fn turn_timeout_ms() -> u64 {
        900_000
    }
    pub fn read_timeout_ms() -> u64 {
        5_000
    }
    pub fn codex_approval_policy() -> serde_json::Value {
        serde_json::json!({
          "reject": {
            "sandbox_approval": true,
            "rules": true,
            "mcp_elicitations": true
          }
        })
    }
    pub fn thread_sandbox() -> String {
        "workspace-write".into()
    }
    pub fn tracker_kind() -> String {
        "linear".to_string()
    }
    pub fn detailed_app_server_logs() -> bool {
        false
    }
    pub fn runtime_interface() -> String {
        "codex".to_string()
    }
    pub fn server_host() -> String {
        "127.0.0.1".to_string()
    }
    pub fn observability_web_dashboard_enabled() -> bool {
        true
    }
    pub fn observability_refresh_ms() -> u64 {
        3_000
    }
    pub fn active_states() -> Vec<String> {
        vec![
            "todo".to_string(),
            "in progress".to_string(),
            "merging".to_string(),
            "rework".to_string(),
        ]
    }
    pub fn terminal_states() -> Vec<String> {
        vec![
            "closed".to_string(),
            "cancelled".to_string(),
            "canceled".to_string(),
            "duplicate".to_string(),
            "done".to_string(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn parses_front_matter() {
        let parsed = Settings::from_workflow_front_matter(Some(
            r#"
polling:
  interval_ms: 1200
workspace:
  root: /tmp/s
"#,
        ))
        .expect("must parse");
        assert_eq!(parsed.polling.interval_ms, 1200);
        assert_eq!(parsed.workspace.root, "/tmp/s");
    }

    #[test]
    fn workflow_path_prefers_explicit() {
        let explicit = PathBuf::from("/tmp/wf.md");
        assert_eq!(workflow_path(Some(explicit.clone())), explicit);
        assert_eq!(workflow_path(None), PathBuf::from("./WORKFLOW.md"));
    }

    #[test]
    fn runtime_interface_defaults_to_codex() {
        let cfg = Settings::default();
        assert_eq!(cfg.runtime.interface, "codex");
    }

    #[test]
    fn runtime_interface_validation_rejects_unknown_value() {
        let mut cfg = Settings::default();
        cfg.runtime.interface = "unknown_runtime".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn detailed_app_server_logs_defaults_to_disabled() {
        let cfg = Settings::default();
        assert!(!cfg.codex.detailed_app_server_logs);
    }

    #[test]
    fn max_concurrent_agents_defaults_to_one() {
        let cfg = Settings::default();
        assert_eq!(cfg.agent.max_concurrent_agents, 1);
    }

    #[test]
    fn max_concurrent_agents_must_be_positive() {
        let mut cfg = Settings::default();
        cfg.agent.max_concurrent_agents = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn parses_worker_agent_and_codex_extensions() {
        let parsed = Settings::from_workflow_front_matter(Some(
            r#"
worker:
  ssh_hosts: ["worker-a:22", "worker-b"]
  max_concurrent_agents_per_host: 2
agent:
  max_retry_backoff_ms: 120000
  max_concurrent_agents_by_state:
    Todo: 1
    In Progress: 3
codex:
  approval_policy: never
  thread_sandbox: workspace-write
  turn_sandbox_policy:
    type: workspaceWrite
  read_timeout_ms: 8000
"#,
        ))
        .expect("must parse");
        assert_eq!(parsed.worker.ssh_hosts.len(), 2);
        assert_eq!(
            parsed.worker.max_concurrent_agents_per_host,
            Some(2)
        );
        assert_eq!(parsed.agent.max_retry_backoff_ms, 120_000);
        assert_eq!(
            *parsed
                .agent
                .max_concurrent_agents_by_state
                .get("Todo")
                .expect("todo limit"),
            1
        );
        assert!(codex_auto_approve_incoming_requests(
            &parsed.codex.approval_policy
        ));
        assert_eq!(parsed.codex.read_timeout_ms, 8000);
        assert_eq!(parsed.codex.thread_sandbox, "workspace-write");
        parsed.validate().expect("valid");
    }

    #[test]
    fn rejects_per_host_limit_without_ssh_hosts() {
        let mut cfg = Settings::default();
        cfg.worker.max_concurrent_agents_per_host = Some(2);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn observability_defaults_http_off() {
        let cfg = Settings::default();
        assert!(!observability_http_enabled(&cfg));
        assert_eq!(cfg.server.port, 0);
        assert!(cfg.observability.web_dashboard_enabled);
    }

    #[test]
    fn observability_api_enabled_requires_port() {
        let mut cfg = Settings::default();
        cfg.observability.api_enabled = true;
        assert!(cfg.validate().is_err());
        cfg.server.port = 8080;
        cfg.validate().expect("valid with port");
        assert!(observability_http_enabled(&cfg));
    }

    #[test]
    fn parses_server_and_observability() {
        let parsed = Settings::from_workflow_front_matter(Some(
            r#"
server:
  host: 0.0.0.0
  port: 9090
observability:
  api_enabled: true
  web_dashboard_enabled: false
  refresh_ms: 5000
runtime:
  require_guardrails_ack: true
"#,
        ))
        .expect("must parse");
        assert_eq!(parsed.server.host, "0.0.0.0");
        assert_eq!(parsed.server.port, 9090);
        assert!(parsed.observability.api_enabled);
        assert!(!parsed.observability.web_dashboard_enabled);
        assert_eq!(parsed.observability.refresh_ms, 5000);
        assert!(parsed.runtime.require_guardrails_ack);
        parsed.validate().expect("valid");
    }
}
