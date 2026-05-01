use minijinja::{Environment, ErrorKind, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub front_matter: BTreeMap<String, serde_yaml::Value>,
    pub prompt_template: String,
}

#[derive(Debug, Clone)]
pub struct WorkflowStore {
    path: PathBuf,
    modified_at: Option<SystemTime>,
    current: Workflow,
}

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("missing workflow file at {0}")]
    MissingWorkflowFile(PathBuf),
    #[error("workflow parse error: {0}")]
    Parse(String),
    #[error("workflow front matter must decode to a map")]
    FrontMatterNotMap,
    #[error("template parse error: {0}")]
    TemplateParse(String),
    #[error("template render error: {0}")]
    TemplateRender(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Serialize)]
pub struct PromptContext {
    pub issue: serde_json::Value,
    pub attempt: u32,
}

impl Workflow {
    pub fn parse(raw: &str) -> Result<Self, WorkflowError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Self {
                front_matter: BTreeMap::new(),
                prompt_template: default_prompt(),
            });
        }

        if !trimmed.starts_with("---") {
            return Ok(Self {
                front_matter: BTreeMap::new(),
                prompt_template: trimmed.to_string(),
            });
        }

        let raw = raw.strip_prefix("---\n").ok_or_else(|| {
            WorkflowError::Parse("front matter must begin with a newline delimiter".into())
        })?;
        let split_idx = raw.find("\n---\n").ok_or_else(|| {
            WorkflowError::Parse("front matter closing delimiter not found".into())
        })?;
        let fm_raw = &raw[..split_idx];
        let body = raw[split_idx + 5..].trim();

        let fm_value: serde_yaml::Value =
            serde_yaml::from_str(fm_raw).map_err(|e| WorkflowError::Parse(e.to_string()))?;
        let front_matter = match fm_value {
            serde_yaml::Value::Mapping(map) => map
                .into_iter()
                .filter_map(|(k, v)| k.as_str().map(|kk| (kk.to_string(), v)))
                .collect(),
            _ => return Err(WorkflowError::FrontMatterNotMap),
        };

        let prompt_template = if body.is_empty() {
            default_prompt()
        } else {
            body.to_string()
        };

        Ok(Self {
            front_matter,
            prompt_template,
        })
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, WorkflowError> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(WorkflowError::MissingWorkflowFile(path.to_path_buf()));
        }
        let raw = fs::read_to_string(path)?;
        Self::parse(&raw)
    }

    pub fn render(&self, ctx: &PromptContext) -> Result<String, WorkflowError> {
        let mut env = Environment::new();
        env.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
        env.add_template("prompt", &self.prompt_template)
            .map_err(|e| WorkflowError::TemplateParse(e.to_string()))?;
        let tmpl = env
            .get_template("prompt")
            .map_err(|e| WorkflowError::TemplateParse(e.to_string()))?;
        tmpl.render(Value::from_serialize(ctx))
            .map_err(|e| match e.kind() {
                ErrorKind::UndefinedError => WorkflowError::TemplateRender(e.to_string()),
                _ => WorkflowError::TemplateRender(e.to_string()),
            })
    }
}

impl WorkflowStore {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, WorkflowError> {
        let path = path.as_ref().to_path_buf();
        let wf = Workflow::from_file(&path)?;
        let modified_at = fs::metadata(&path).ok().and_then(|m| m.modified().ok());
        Ok(Self {
            path,
            modified_at,
            current: wf,
        })
    }

    pub fn current(&self) -> &Workflow {
        &self.current
    }

    pub fn reload_if_changed(&mut self) -> Result<bool, WorkflowError> {
        let metadata = fs::metadata(&self.path)?;
        let modified = metadata.modified().ok();
        if modified == self.modified_at {
            return Ok(false);
        }
        match Workflow::from_file(&self.path) {
            Ok(new_workflow) => {
                self.current = new_workflow;
                self.modified_at = modified;
                Ok(true)
            }
            Err(err) => {
                // Keep the previous known-good workflow in memory.
                let _ = err;
                Ok(false)
            }
        }
    }
}

fn default_prompt() -> String {
    "Work issue {{ issue.identifier }} (attempt {{ attempt }}).\n{{ issue.description }}"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn strict_render_fails_for_unknown_variable() {
        let wf = Workflow {
            front_matter: BTreeMap::new(),
            prompt_template: "{{ issue.nope }}".to_string(),
        };
        let ctx = PromptContext {
            issue: serde_json::json!({"identifier":"S-1","description":"desc"}),
            attempt: 1,
        };
        assert!(wf.render(&ctx).is_err());
    }

    #[test]
    fn reload_invalid_keeps_last_known_good() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("wf-{ts}.md"));
        fs::write(
            &path,
            "---\npolling:\n  interval_ms: 1\n---\nHello {{ issue.identifier }}",
        )
        .expect("write");
        let mut store = WorkflowStore::load(&path).expect("load");
        assert_eq!(
            store
                .current()
                .render(&PromptContext {
                    issue: serde_json::json!({"identifier":"A-1","description":""}),
                    attempt: 1
                })
                .expect("render"),
            "Hello A-1"
        );
        fs::write(&path, "---\nnot: [valid\n---\nOops").expect("write invalid");
        let changed = store
            .reload_if_changed()
            .expect("reload does not fail hard");
        assert!(!changed);
        assert!(store.current().prompt_template.contains("Hello"));
        let _ = fs::remove_file(path);
    }
}
