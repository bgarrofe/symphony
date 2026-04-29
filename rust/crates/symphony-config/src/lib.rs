use serde::{Deserialize, Serialize};
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
    pub agent: AgentConfig,
    #[serde(default)]
    pub codex: CodexConfig,
    #[serde(default)]
    pub tracker: TrackerConfig,
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
pub struct AgentConfig {
    #[serde(default = "defaults::max_turns")]
    pub max_turns: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexConfig {
    #[serde(default = "defaults::codex_command")]
    pub command: String,
    #[serde(default = "defaults::turn_timeout_ms")]
    pub turn_timeout_ms: u64,
    #[serde(default = "defaults::stall_timeout_ms")]
    pub stall_timeout_ms: u64,
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

impl Default for Settings {
    fn default() -> Self {
        Self {
            polling: PollingConfig::default(),
            workspace: WorkspaceConfig::default(),
            hooks: HookConfig::default(),
            agent: AgentConfig::default(),
            codex: CodexConfig::default(),
            tracker: TrackerConfig::default(),
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

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            max_turns: defaults::max_turns(),
        }
    }
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            command: defaults::codex_command(),
            turn_timeout_ms: defaults::turn_timeout_ms(),
            stall_timeout_ms: defaults::stall_timeout_ms(),
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
            return Err(ConfigError::Invalid("polling.interval_ms must be > 0".into()));
        }
        if self.workspace.root.trim().is_empty() {
            return Err(ConfigError::Invalid("workspace.root cannot be empty".into()));
        }
        if self.workspace.root.trim() == "/" {
            return Err(ConfigError::Invalid(
                "workspace.root cannot be filesystem root".into(),
            ));
        }
        if self.agent.max_turns == 0 {
            return Err(ConfigError::Invalid("agent.max_turns must be > 0".into()));
        }
        if self.codex.command.trim().is_empty() {
            return Err(ConfigError::Invalid("codex.command cannot be empty".into()));
        }
        if self.codex.turn_timeout_ms == 0 {
            return Err(ConfigError::Invalid("codex.turn_timeout_ms must be > 0".into()));
        }
        if self.codex.stall_timeout_ms == 0 {
            return Err(ConfigError::Invalid("codex.stall_timeout_ms must be > 0".into()));
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
    pub fn codex_command() -> String {
        "codex app-server".to_string()
    }
    pub fn stall_timeout_ms() -> u64 {
        120_000
    }
    pub fn turn_timeout_ms() -> u64 {
        900_000
    }
    pub fn tracker_kind() -> String {
        "linear".to_string()
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
}
