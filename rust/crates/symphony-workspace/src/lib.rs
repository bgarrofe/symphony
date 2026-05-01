use regex::Regex;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::process::Command;
use tokio::time::{Duration, timeout};

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("path escaped workspace root")]
    PathEscape,
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("hook `{hook}` failed: {reason}")]
    HookFailure { hook: &'static str, reason: String },
}

#[derive(Debug, Clone)]
pub struct HookSet {
    pub after_create: Option<String>,
    pub before_run: Option<String>,
    pub after_run: Option<String>,
    pub before_remove: Option<String>,
    pub timeout_ms: u64,
}

impl Default for HookSet {
    fn default() -> Self {
        Self {
            after_create: None,
            before_run: None,
            after_run: None,
            before_remove: None,
            timeout_ms: 30_000,
        }
    }
}

pub fn sanitize_issue_identifier(identifier: &str) -> String {
    let re = Regex::new(r"[^A-Za-z0-9._-]").expect("static regex compiles");
    re.replace_all(identifier, "_").to_string()
}

pub fn issue_workspace_path(
    root: &Path,
    issue_identifier: &str,
) -> Result<PathBuf, WorkspaceError> {
    let safe = sanitize_issue_identifier(issue_identifier);
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let child = root.join(safe);
    if !child.starts_with(&root) {
        return Err(WorkspaceError::PathEscape);
    }
    Ok(child)
}

pub async fn create_workspace(
    root: &Path,
    issue_identifier: &str,
) -> Result<PathBuf, WorkspaceError> {
    let path = issue_workspace_path(root, issue_identifier)?;
    tokio::fs::create_dir_all(&path).await?;
    Ok(path)
}

pub async fn remove_workspace(path: &Path) -> Result<(), WorkspaceError> {
    if path.exists() {
        tokio::fs::remove_dir_all(path).await?;
    }
    Ok(())
}

pub async fn run_hook(
    hook_name: &'static str,
    command: &str,
    cwd: &Path,
    timeout_ms: u64,
    fatal: bool,
) -> Result<(), WorkspaceError> {
    let fut = Command::new("bash")
        .arg("-lc")
        .arg(command)
        .current_dir(cwd)
        .output();
    let out = timeout(Duration::from_millis(timeout_ms), fut)
        .await
        .map_err(|_| WorkspaceError::HookFailure {
            hook: hook_name,
            reason: "timed out".into(),
        })?
        .map_err(WorkspaceError::Io)?;
    if !out.status.success() && fatal {
        return Err(WorkspaceError::HookFailure {
            hook: hook_name,
            reason: String::from_utf8_lossy(&out.stderr).to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_identifier() {
        assert_eq!(sanitize_issue_identifier("SYM-1"), "SYM-1");
        assert_eq!(sanitize_issue_identifier("SYM 1/#"), "SYM_1__");
    }

    #[tokio::test]
    async fn creates_workspace_under_root() {
        let base = std::env::temp_dir().join("symphony-ws-test");
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).expect("base dir");
        let path = create_workspace(&base, "SYM 1").await.expect("create");
        assert!(path.starts_with(&base));
        assert!(path.exists());
        let _ = remove_workspace(&path).await;
        let _ = std::fs::remove_dir_all(&base);
    }
}
