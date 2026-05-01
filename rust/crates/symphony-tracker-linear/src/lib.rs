use async_trait::async_trait;
use reqwest::Client;
use reqwest::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::json;
use symphony_tracker::{Issue, IssueState, Tracker, TrackerError};

#[derive(Debug, Clone)]
pub struct LinearTracker {
    endpoint: String,
    token: String,
    client: Client,
    project: Option<String>,
    active_states: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphQLResponse<T> {
    data: Option<T>,
    #[serde(default)]
    errors: Vec<GraphQLError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphQLError {
    message: String,
}

impl LinearTracker {
    pub fn new(
        endpoint: String,
        token: String,
        project: Option<String>,
        active_states: Vec<String>,
    ) -> Self {
        Self {
            endpoint,
            token,
            client: Client::new(),
            project,
            active_states: active_states
                .into_iter()
                .map(|s| s.to_ascii_lowercase())
                .collect(),
        }
    }

    async fn send_graphql_with_auth_fallback(
        &self,
        payload: Value,
    ) -> Result<reqwest::Response, TrackerError> {
        let token = self.token.trim();
        let first = self
            .client
            .post(&self.endpoint)
            .header(AUTHORIZATION, token)
            .json(&payload)
            .send()
            .await
            .map_err(|e| TrackerError::Request(format!("linear_api_request: {e}")))?;

        if first.status() != reqwest::StatusCode::UNAUTHORIZED || token.starts_with("Bearer ") {
            return Ok(first);
        }

        // Some Linear credentials require explicit Bearer prefix.
        self.client
            .post(&self.endpoint)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .json(&payload)
            .send()
            .await
            .map_err(|e| TrackerError::Request(format!("linear_api_request: {e}")))
    }
}

#[async_trait]
impl Tracker for LinearTracker {
    async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>, TrackerError> {
        let (query, variables) = if self
            .project
            .as_deref()
            .is_some_and(|v| !v.trim().is_empty())
        {
            (
                r#"
query CandidateIssues($projectSlug: String!) {
  issues(filter: { project: { slugId: { eq: $projectSlug } } }) {
    nodes {
      id
      identifier
      title
      description
      url
      priority
      state { name }
      labels { nodes { name } }
      createdAt
    }
  }
}
"#,
                json!({ "projectSlug": self.project }),
            )
        } else {
            (
                r#"
query CandidateIssues {
  issues {
    nodes {
      id
      identifier
      title
      description
      url
      priority
      state { name }
      labels { nodes { name } }
      createdAt
    }
  }
}
"#,
                json!({}),
            )
        };
        #[derive(Deserialize)]
        struct StateNode {
            name: String,
        }
        #[derive(Deserialize)]
        struct IssueNode {
            id: String,
            identifier: String,
            title: String,
            #[serde(default)]
            description: String,
            #[serde(default)]
            url: String,
            #[serde(default)]
            priority: i32,
            state: StateNode,
            #[serde(default)]
            labels: LabelNodes,
            #[serde(rename = "createdAt")]
            created_at: chrono::DateTime<chrono::Utc>,
        }
        #[derive(Default, Deserialize)]
        struct LabelNode {
            #[serde(default)]
            name: String,
        }
        #[derive(Default, Deserialize)]
        struct LabelNodes {
            #[serde(default)]
            nodes: Vec<LabelNode>,
        }
        #[derive(Deserialize)]
        struct Nodes {
            nodes: Vec<IssueNode>,
        }
        #[derive(Deserialize)]
        struct Data {
            issues: Nodes,
        }
        let response = self
            .send_graphql_with_auth_fallback(json!({
                "query": query,
                "variables": variables
            }))
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable body>".to_string());
            return Err(TrackerError::Request(format!(
                "linear_api_status: http {status}; body={body}"
            )));
        }

        let resp: GraphQLResponse<Data> = response
            .json()
            .await
            .map_err(|e| TrackerError::Request(format!("linear_unknown_payload: {e}")))?;

        if !resp.errors.is_empty() {
            let messages = resp
                .errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
                .join(" | ");
            return Err(TrackerError::Request(format!(
                "linear_graphql_errors: {messages}"
            )));
        }

        let data = resp.data.ok_or_else(|| {
            TrackerError::Request(
                "linear_unknown_payload: response had no data and no graphql errors".to_string(),
            )
        })?;
        let mut issues: Vec<Issue> = data
            .issues
            .nodes
            .into_iter()
            .map(|i| Issue {
                id: i.id,
                identifier: i.identifier,
                title: i.title,
                description: i.description,
                labels: i
                    .labels
                    .nodes
                    .into_iter()
                    .map(|l| l.name.to_ascii_lowercase())
                    .collect(),
                url: i.url,
                priority: i.priority,
                state: i.state.name.to_ascii_lowercase(),
                blocked_by: vec![],
                assigned_to_worker: true,
                created_at: i.created_at,
            })
            .collect();

        if !self.active_states.is_empty() {
            issues.retain(|i| {
                self.active_states
                    .iter()
                    .any(|state| i.state.eq_ignore_ascii_case(state))
            });
        }

        Ok(issues)
    }

    async fn fetch_issues_by_states(&self, states: &[String]) -> Result<Vec<Issue>, TrackerError> {
        let mut all = self.fetch_candidate_issues().await?;
        all.retain(|i| states.iter().any(|s| i.state.eq_ignore_ascii_case(s)));
        Ok(all)
    }

    async fn fetch_issue_states_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<IssueState>, TrackerError> {
        let issues = self.fetch_candidate_issues().await?;
        Ok(issues
            .into_iter()
            .filter(|i| ids.contains(&i.id))
            .map(|i| IssueState {
                id: i.id,
                state: i.state,
            })
            .collect())
    }
}
