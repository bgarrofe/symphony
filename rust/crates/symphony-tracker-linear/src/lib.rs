//! Linear GraphQL tracker adapter with paged state queries, batched id lookups, and optional assignee routing.

use async_trait::async_trait;
use reqwest::Client;
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use symphony_tracker::{Issue, IssueState, Tracker, TrackerError};

const ISSUE_PAGE_SIZE: i64 = 50;

#[derive(Debug)]
pub struct LinearTracker {
    endpoint: String,
    token: String,
    client: Client,
    project: Option<String>,
    active_states: Vec<String>,
    assignee: Option<String>,
    assignee_cache: Mutex<AssigneeCache>,
}

#[derive(Debug)]
enum AssigneeCache {
    Unresolved,
    Resolved(Option<AssigneeFilter>),
}

#[derive(Debug, Clone)]
struct AssigneeFilter {
    match_values: HashSet<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GraphQLResponse<T> {
    data: Option<T>,
    #[serde(default)]
    errors: Vec<GraphQLError>,
}

#[derive(Debug, Clone, Deserialize)]
struct GraphQLError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct ViewerData {
    viewer: ViewerId,
}

#[derive(Debug, Deserialize)]
struct ViewerId {
    id: String,
}

#[derive(Debug, Deserialize)]
struct PageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IssuesPageData {
    issues: IssuesConnection,
}

#[derive(Debug, Deserialize)]
struct IssuesConnection {
    nodes: Vec<IssueNode>,
    #[serde(rename = "pageInfo")]
    page_info: PageInfo,
}

#[derive(Debug, Deserialize)]
struct IssueStatesOnlyData {
    issues: IssueStatesOnlyNodes,
}

#[derive(Debug, Deserialize)]
struct IssueStatesOnlyNodes {
    nodes: Vec<IssueStateNode>,
}

#[derive(Debug, Deserialize)]
struct IssueStateNode {
    id: String,
    state: StateNode,
}

#[derive(Debug, Deserialize)]
struct StateNode {
    name: String,
}

#[derive(Debug, Deserialize)]
struct IssueNode {
    id: String,
    identifier: String,
    title: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    url: String,
    #[serde(default)]
    priority: i32,
    state: StateNode,
    #[serde(default)]
    assignee: Option<AssigneeNode>,
    #[serde(default)]
    labels: LabelNodes,
    #[serde(default, rename = "inverseRelations")]
    inverse_relations: Option<InverseRelations>,
    #[serde(rename = "createdAt")]
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
struct AssigneeNode {
    id: String,
}

#[derive(Debug, Default, Deserialize)]
struct LabelNodes {
    #[serde(default)]
    nodes: Vec<LabelNode>,
}

#[derive(Debug, Default, Deserialize)]
struct LabelNode {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Default, Deserialize)]
struct InverseRelations {
    #[serde(default)]
    nodes: Vec<InverseRelation>,
}

#[derive(Debug, Default, Deserialize)]
struct InverseRelation {
    #[serde(rename = "type")]
    relation_type: Option<String>,
    #[serde(default)]
    issue: Option<BlockerIssue>,
}

#[derive(Debug, Default, Deserialize)]
struct BlockerIssue {
    identifier: Option<String>,
}

impl LinearTracker {
    pub fn new(
        endpoint: String,
        token: String,
        project: Option<String>,
        active_states: Vec<String>,
        assignee: Option<String>,
    ) -> Self {
        Self {
            endpoint,
            token,
            client: Client::new(),
            project,
            active_states,
            assignee,
            assignee_cache: Mutex::new(AssigneeCache::Unresolved),
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

        self.client
            .post(&self.endpoint)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .json(&payload)
            .send()
            .await
            .map_err(|e| TrackerError::Request(format!("linear_api_request: {e}")))
    }

    async fn graphql_json<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        variables: Value,
    ) -> Result<T, TrackerError> {
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

        let resp: GraphQLResponse<T> = response
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

        resp.data.ok_or_else(|| {
            TrackerError::Request(
                "linear_unknown_payload: response had no data and no graphql errors".to_string(),
            )
        })
    }

    async fn effective_assignee_filter(&self) -> Result<Option<AssigneeFilter>, TrackerError> {
        {
            let guard = self
                .assignee_cache
                .lock()
                .map_err(|_| TrackerError::Config("linear_assignee_cache_poisoned".to_string()))?;
            if let AssigneeCache::Resolved(opt) = &*guard {
                return Ok(opt.clone());
            }
        }

        let resolved = self.resolve_assignee_filter_once().await?;

        let mut guard = self
            .assignee_cache
            .lock()
            .map_err(|_| TrackerError::Config("linear_assignee_cache_poisoned".to_string()))?;
        *guard = AssigneeCache::Resolved(resolved.clone());
        Ok(resolved)
    }

    async fn resolve_assignee_filter_once(&self) -> Result<Option<AssigneeFilter>, TrackerError> {
        let Some(raw) = self
            .assignee
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };

        if raw.eq_ignore_ascii_case("me") {
            let viewer_id = self.fetch_viewer_id().await?;
            let mut set = HashSet::new();
            set.insert(viewer_id);
            return Ok(Some(AssigneeFilter { match_values: set }));
        }

        let mut set = HashSet::new();
        set.insert(raw.to_string());
        Ok(Some(AssigneeFilter { match_values: set }))
    }

    async fn fetch_viewer_id(&self) -> Result<String, TrackerError> {
        let query = r#"
query SymphonyLinearViewer {
  viewer {
    id
  }
}
"#;
        let data: ViewerData = self.graphql_json(query, json!({})).await?;
        let id = data.viewer.id.trim();
        if id.is_empty() {
            return Err(TrackerError::Config(
                "missing_linear_viewer_identity".to_string(),
            ));
        }
        Ok(id.to_string())
    }

    /// Pages through issues filtered by workflow states (and optionally project).
    async fn fetch_issues_pages_with_state_filter(
        &self,
        state_names: &[String],
        assignee_filter: Option<&AssigneeFilter>,
    ) -> Result<Vec<Issue>, TrackerError> {
        if state_names.is_empty() {
            return Ok(vec![]);
        }

        let relation_first = ISSUE_PAGE_SIZE;
        let mut after: Option<String> = None;
        let mut acc: Vec<Issue> = Vec::new();

        loop {
            let variables = if self
                .project
                .as_deref()
                .is_some_and(|p| !p.trim().is_empty())
            {
                let slug = self.project.clone().unwrap();
                json!({
                    "projectSlug": slug,
                    "stateNames": state_names,
                    "first": ISSUE_PAGE_SIZE,
                    "relationFirst": relation_first,
                    "after": after,
                })
            } else {
                json!({
                    "stateNames": state_names,
                    "first": ISSUE_PAGE_SIZE,
                    "relationFirst": relation_first,
                    "after": after,
                })
            };

            let query = if self
                .project
                .as_deref()
                .is_some_and(|p| !p.trim().is_empty())
            {
                r#"
query SymphonyLinearPollProject($projectSlug: String!, $stateNames: [String!]!, $first: Int!, $relationFirst: Int!, $after: String) {
  issues(filter: { project: { slugId: { eq: $projectSlug } }, state: { name: { in: $stateNames } } }, first: $first, after: $after) {
    nodes {
      id
      identifier
      title
      description
      url
      priority
      state { name }
      assignee { id }
      labels { nodes { name } }
      inverseRelations(first: $relationFirst) {
        nodes {
          type
          issue {
            identifier
            state { name }
          }
        }
      }
      createdAt
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"#
            } else {
                r#"
query SymphonyLinearPollNoProject($stateNames: [String!]!, $first: Int!, $relationFirst: Int!, $after: String) {
  issues(filter: { state: { name: { in: $stateNames } } }, first: $first, after: $after) {
    nodes {
      id
      identifier
      title
      description
      url
      priority
      state { name }
      assignee { id }
      labels { nodes { name } }
      inverseRelations(first: $relationFirst) {
        nodes {
          type
          issue {
            identifier
            state { name }
          }
        }
      }
      createdAt
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"#
            };

            let page: IssuesPageData = self.graphql_json(query, variables).await?;
            for node in page.issues.nodes {
                if let Some(issue) = normalize_issue_node(node, assignee_filter) {
                    acc.push(issue);
                }
            }

            if page.issues.page_info.has_next_page {
                let cursor = page.issues.page_info.end_cursor.filter(|c| !c.is_empty());
                match cursor {
                    Some(c) => after = Some(c),
                    None => {
                        return Err(TrackerError::Request(
                            "linear_missing_end_cursor".to_string(),
                        ));
                    }
                }
            } else {
                break;
            }
        }

        Ok(acc)
    }

    /// When `active_states` is empty: page through issues with optional project filter only (legacy behavior).
    async fn fetch_issues_pages_without_state_filter(
        &self,
        assignee_filter: Option<&AssigneeFilter>,
    ) -> Result<Vec<Issue>, TrackerError> {
        let relation_first = ISSUE_PAGE_SIZE;
        let mut after: Option<String> = None;
        let mut acc: Vec<Issue> = Vec::new();

        loop {
            let variables = if self
                .project
                .as_deref()
                .is_some_and(|p| !p.trim().is_empty())
            {
                let slug = self.project.clone().unwrap();
                json!({
                    "projectSlug": slug,
                    "first": ISSUE_PAGE_SIZE,
                    "relationFirst": relation_first,
                    "after": after,
                })
            } else {
                json!({
                    "first": ISSUE_PAGE_SIZE,
                    "relationFirst": relation_first,
                    "after": after,
                })
            };

            let query = if self
                .project
                .as_deref()
                .is_some_and(|p| !p.trim().is_empty())
            {
                r#"
query SymphonyLinearIssuesProjectOnly($projectSlug: String!, $first: Int!, $relationFirst: Int!, $after: String) {
  issues(filter: { project: { slugId: { eq: $projectSlug } } }, first: $first, after: $after) {
    nodes {
      id
      identifier
      title
      description
      url
      priority
      state { name }
      assignee { id }
      labels { nodes { name } }
      inverseRelations(first: $relationFirst) {
        nodes {
          type
          issue {
            identifier
            state { name }
          }
        }
      }
      createdAt
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"#
            } else {
                r#"
query SymphonyLinearIssuesAll($first: Int!, $relationFirst: Int!, $after: String) {
  issues(first: $first, after: $after) {
    nodes {
      id
      identifier
      title
      description
      url
      priority
      state { name }
      assignee { id }
      labels { nodes { name } }
      inverseRelations(first: $relationFirst) {
        nodes {
          type
          issue {
            identifier
            state { name }
          }
        }
      }
      createdAt
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"#
            };

            let page: IssuesPageData = self.graphql_json(query, variables).await?;
            for node in page.issues.nodes {
                if let Some(issue) = normalize_issue_node(node, assignee_filter) {
                    acc.push(issue);
                }
            }

            if page.issues.page_info.has_next_page {
                let cursor = page.issues.page_info.end_cursor.filter(|c| !c.is_empty());
                match cursor {
                    Some(c) => after = Some(c),
                    None => {
                        return Err(TrackerError::Request(
                            "linear_missing_end_cursor".to_string(),
                        ));
                    }
                }
            } else {
                break;
            }
        }

        Ok(acc)
    }
}

#[async_trait]
impl Tracker for LinearTracker {
    async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>, TrackerError> {
        let assignee_filter = self.effective_assignee_filter().await?;

        let mut issues = if self.active_states.is_empty() {
            self.fetch_issues_pages_without_state_filter(assignee_filter.as_ref())
                .await?
        } else {
            self.fetch_issues_pages_with_state_filter(&self.active_states, assignee_filter.as_ref())
                .await?
        };

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
        let normalized: Vec<String> = states
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if normalized.is_empty() {
            return Ok(vec![]);
        }

        // Terminal cleanup and similar paths must not apply assignee routing (matches Elixir).
        self.fetch_issues_pages_with_state_filter(&normalized, None)
            .await
    }

    async fn fetch_issue_states_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<IssueState>, TrackerError> {
        let uniq = dedupe_preserve_order(ids);
        if uniq.is_empty() {
            return Ok(vec![]);
        }

        let query = r#"
query SymphonyLinearIssueStatesByIds($ids: [ID!]!, $first: Int!) {
  issues(filter: { id: { in: $ids } }, first: $first) {
    nodes {
      id
      state { name }
    }
  }
}
"#;

        let mut by_id: HashMap<String, String> = HashMap::new();

        for chunk in uniq.chunks(ISSUE_PAGE_SIZE as usize) {
            let chunk_vec: Vec<&String> = chunk.iter().collect();
            let ids_json: Vec<String> = chunk_vec.iter().map(|s| (*s).clone()).collect();
            let variables = json!({
                "ids": ids_json,
                "first": chunk.len() as i64,
            });

            let data: IssueStatesOnlyData = self.graphql_json(query, variables).await?;
            for node in data.issues.nodes {
                by_id.insert(node.id, node.state.name.to_ascii_lowercase());
            }
        }

        let mut out = Vec::new();
        for id in &uniq {
            if let Some(state) = by_id.get(id) {
                out.push(IssueState {
                    id: id.clone(),
                    state: state.clone(),
                });
            }
        }

        Ok(out)
    }

    async fn create_comment(&self, issue_id: &str, body: &str) -> Result<(), TrackerError> {
        let query = r#"
mutation SymphonyCreateComment($issueId: String!, $body: String!) {
  commentCreate(input: {issueId: $issueId, body: $body}) {
    success
  }
}
"#;
        let variables = json!({
            "issueId": issue_id,
            "body": body,
        });
        #[derive(Deserialize)]
        struct CommentCreateData {
            #[serde(rename = "commentCreate")]
            comment_create: SuccessPayload,
        }
        #[derive(Deserialize)]
        struct SuccessPayload {
            success: bool,
        }
        let data: CommentCreateData = self.graphql_json(query, variables).await?;
        if data.comment_create.success {
            Ok(())
        } else {
            Err(TrackerError::Request(
                "linear_comment_create_failed".to_string(),
            ))
        }
    }

    async fn update_issue_state(
        &self,
        issue_id: &str,
        state_name: &str,
    ) -> Result<(), TrackerError> {
        let state_id = self.resolve_state_id(issue_id, state_name).await?;
        let query = r#"
mutation SymphonyUpdateIssueState($issueId: String!, $stateId: String!) {
  issueUpdate(id: $issueId, input: {stateId: $stateId}) {
    success
  }
}
"#;
        let variables = json!({
            "issueId": issue_id,
            "stateId": state_id,
        });
        #[derive(Deserialize)]
        struct IssueUpdateData {
            #[serde(rename = "issueUpdate")]
            issue_update: SuccessPayload,
        }
        #[derive(Deserialize)]
        struct SuccessPayload {
            success: bool,
        }
        let data: IssueUpdateData = self.graphql_json(query, variables).await?;
        if data.issue_update.success {
            Ok(())
        } else {
            Err(TrackerError::Request(
                "linear_issue_update_failed".to_string(),
            ))
        }
    }
}

impl LinearTracker {
    async fn resolve_state_id(
        &self,
        issue_id: &str,
        state_name: &str,
    ) -> Result<String, TrackerError> {
        let query = r#"
query SymphonyResolveStateId($issueId: String!, $stateName: String!) {
  issue(id: $issueId) {
    team {
      states(filter: {name: {eq: $stateName}}, first: 1) {
        nodes {
          id
        }
      }
    }
  }
}
"#;
        let variables = json!({
            "issueId": issue_id,
            "stateName": state_name.trim(),
        });
        #[derive(Deserialize)]
        struct StateLookupData {
            issue: Option<IssueWithTeam>,
        }
        #[derive(Deserialize)]
        struct IssueWithTeam {
            team: LookupTeam,
        }
        #[derive(Deserialize)]
        struct LookupTeam {
            states: LookupStatesConn,
        }
        #[derive(Deserialize)]
        struct LookupStatesConn {
            nodes: Vec<LookupStateNode>,
        }
        #[derive(Deserialize)]
        struct LookupStateNode {
            id: String,
        }
        let data: StateLookupData = self.graphql_json(query, variables).await?;
        let id = data
            .issue
            .and_then(|i| i.team.states.nodes.into_iter().next())
            .map(|n| n.id)
            .ok_or_else(|| TrackerError::Request("linear_state_not_found".to_string()))?;
        Ok(id)
    }
}

fn dedupe_preserve_order(ids: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for id in ids {
        if seen.insert(id.clone()) {
            out.push(id.clone());
        }
    }
    out
}

fn normalize_issue_node(
    node: IssueNode,
    assignee_filter: Option<&AssigneeFilter>,
) -> Option<Issue> {
    let assignee_id = node.assignee.as_ref().map(|a| a.id.trim().to_string());

    let assigned_to_worker = match assignee_filter {
        None => true,
        Some(f) => assignee_id
            .as_ref()
            .map(|id| f.match_values.contains(id))
            .unwrap_or(false),
    };

    let blocked_by = extract_blockers(&node.inverse_relations);

    Some(Issue {
        id: node.id,
        identifier: node.identifier,
        title: node.title,
        description: node.description.unwrap_or_default(),
        labels: node
            .labels
            .nodes
            .into_iter()
            .map(|l| l.name.to_ascii_lowercase())
            .collect(),
        url: node.url,
        priority: node.priority,
        state: node.state.name.to_ascii_lowercase(),
        blocked_by,
        assigned_to_worker,
        created_at: node.created_at,
    })
}

fn extract_blockers(inverse: &Option<InverseRelations>) -> Vec<String> {
    let Some(inv) = inverse else {
        return vec![];
    };
    let mut out = Vec::new();
    for rel in &inv.nodes {
        let Some(t) = rel.relation_type.as_deref().map(|s| s.trim()) else {
            continue;
        };
        if !t.eq_ignore_ascii_case("blocks") {
            continue;
        }
        if let Some(issue) = &rel.issue {
            if let Some(id) = issue
                .identifier
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                out.push(id.to_string());
            }
        }
    }
    out
}

#[cfg(test)]
fn normalize_issue_json_for_test(
    raw: &serde_json::Value,
    assignee_filter: Option<&AssigneeFilter>,
) -> Option<Issue> {
    let node: IssueNode = serde_json::from_value(raw.clone()).ok()?;
    normalize_issue_node(node, assignee_filter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupe_preserves_first_occurrence_order() {
        let ids = vec!["a".into(), "b".into(), "a".into(), "c".into()];
        assert_eq!(
            dedupe_preserve_order(&ids),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn normalize_without_assignee_filter_always_routable() {
        let raw = serde_json::json!({
            "id": "i1",
            "identifier": "X-1",
            "title": "t",
            "description": "",
            "url": "",
            "priority": 1,
            "state": { "name": "Todo" },
            "assignee": { "id": "user-other" },
            "labels": { "nodes": [] },
            "createdAt": "2024-01-01T00:00:00Z"
        });
        let issue = normalize_issue_json_for_test(&raw, None).expect("parsed");
        assert!(issue.assigned_to_worker);
    }

    #[test]
    fn normalize_assignee_filter_match_and_mismatch() {
        let filter = AssigneeFilter {
            match_values: HashSet::from(["user-a".to_string()]),
        };
        let raw_match = serde_json::json!({
            "id": "i1",
            "identifier": "X-1",
            "title": "t",
            "description": "",
            "url": "",
            "priority": 1,
            "state": { "name": "Todo" },
            "assignee": { "id": "user-a" },
            "labels": { "nodes": [] },
            "createdAt": "2024-01-01T00:00:00Z"
        });
        assert!(
            normalize_issue_json_for_test(&raw_match, Some(&filter))
                .expect("parsed")
                .assigned_to_worker
        );

        let raw_mismatch = serde_json::json!({
            "id": "i2",
            "identifier": "X-2",
            "title": "t",
            "description": "",
            "url": "",
            "priority": 1,
            "state": { "name": "Todo" },
            "assignee": { "id": "user-b" },
            "labels": { "nodes": [] },
            "createdAt": "2024-01-01T00:00:00Z"
        });
        assert!(
            !normalize_issue_json_for_test(&raw_mismatch, Some(&filter))
                .expect("parsed")
                .assigned_to_worker
        );

        let raw_unassigned = serde_json::json!({
            "id": "i3",
            "identifier": "X-3",
            "title": "t",
            "description": "",
            "url": "",
            "priority": 1,
            "state": { "name": "Todo" },
            "labels": { "nodes": [] },
            "createdAt": "2024-01-01T00:00:00Z"
        });
        assert!(
            !normalize_issue_json_for_test(&raw_unassigned, Some(&filter))
                .expect("parsed")
                .assigned_to_worker
        );
    }

    #[test]
    fn fetch_issue_states_order_follows_request_order() {
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert("z".into(), "done".into());
        map.insert("a".into(), "todo".into());

        let uniq: Vec<String> = vec!["z".into(), "a".into()];
        let mut out = Vec::new();
        for id in &uniq {
            if let Some(state) = map.get(id) {
                out.push(IssueState {
                    id: id.clone(),
                    state: state.clone(),
                });
            }
        }
        assert_eq!(out[0].id, "z");
        assert_eq!(out[1].id, "a");
    }
}
