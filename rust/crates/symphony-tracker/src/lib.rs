use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Issue {
    pub id: String,
    pub identifier: String,
    pub title: String,
    pub description: String,
    pub labels: Vec<String>,
    pub url: String,
    pub priority: i32,
    pub state: String,
    pub blocked_by: Vec<String>,
    pub assigned_to_worker: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl Issue {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state.to_ascii_lowercase().as_str(),
            "done" | "canceled" | "cancelled"
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssueState {
    pub id: String,
    pub state: String,
}

#[derive(Debug, Error)]
pub enum TrackerError {
    #[error("tracker request failed: {0}")]
    Request(String),
    #[error("tracker auth/config invalid: {0}")]
    Config(String),
}

#[async_trait]
pub trait Tracker: Send + Sync {
    async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>, TrackerError>;
    async fn fetch_issues_by_states(&self, states: &[String]) -> Result<Vec<Issue>, TrackerError>;
    async fn fetch_issue_states_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<IssueState>, TrackerError>;
}
