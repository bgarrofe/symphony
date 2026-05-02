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
    /// When false, Elixir skips dispatch (`issue_routable_to_worker?/1`). Defaults to true when unknown.
    #[serde(default = "default_assigned_to_worker")]
    pub assigned_to_worker: bool,
    pub created_at: DateTime<Utc>,
}

fn default_assigned_to_worker() -> bool {
    true
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

    async fn create_comment(&self, issue_id: &str, body: &str) -> Result<(), TrackerError>;
    async fn update_issue_state(
        &self,
        issue_id: &str,
        state_name: &str,
    ) -> Result<(), TrackerError>;
}
