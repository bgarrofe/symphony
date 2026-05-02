//! HTTP observability API and minimal dashboard (Elixir route parity baseline).

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use symphony_core::{OrchestratorSnapshot, RetrySnapshot, RunningWorker};
use tokio::net::TcpListener;
use tokio::sync::{Notify, RwLock};

/// Shared handles for Axum handlers and the orchestrator poll loop.
#[derive(Clone)]
pub struct ObservabilityState {
    pub snapshot: Arc<RwLock<OrchestratorSnapshot>>,
    pub refresh: Arc<Notify>,
    pub refresh_ms: u64,
}

#[derive(Serialize)]
struct Counts {
    running: usize,
    retrying: usize,
}

#[derive(Serialize)]
struct StateResponse {
    generated_at: String,
    counts: Counts,
    running: Vec<RunningWorker>,
    retrying: Vec<RetrySnapshot>,
    codex_totals: serde_json::Value,
    rate_limits: serde_json::Value,
}

#[derive(Serialize)]
struct ApiErrorBody {
    error: ApiErrorInner,
}

#[derive(Serialize)]
struct ApiErrorInner {
    code: String,
    message: String,
}

fn api_error(status: StatusCode, code: &str, message: &str) -> impl IntoResponse {
    let body = ApiErrorBody {
        error: ApiErrorInner {
            code: code.to_string(),
            message: message.to_string(),
        },
    };
    (status, Json(body))
}

async fn get_state(State(state): State<ObservabilityState>) -> impl IntoResponse {
    let snap = state.snapshot.read().await;
    let generated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let codex_totals = serde_json::to_value(&snap.codex_totals).unwrap_or_else(|_| {
        json!({
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": snap.total_tokens,
            "seconds_running": 0
        })
    });
    let response = StateResponse {
        generated_at,
        counts: Counts {
            running: snap.running.len(),
            retrying: snap.retrying.len(),
        },
        running: snap.running.clone(),
        retrying: snap.retrying.clone(),
        codex_totals,
        rate_limits: snap.rate_limits.clone(),
    };
    Json(response)
}

async fn post_refresh(State(state): State<ObservabilityState>) -> impl IntoResponse {
    state.refresh.notify_one();
    let requested_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let body = json!({
        "queued": true,
        "coalesced": false,
        "requested_at": requested_at,
        "operations": ["poll", "reconcile"]
    });
    (StatusCode::ACCEPTED, Json(body))
}

#[derive(Serialize)]
struct WorkspaceHost {
    path: String,
    host: Option<String>,
}

#[derive(Serialize)]
struct AttemptsSummary {
    restart_count: u32,
    current_retry_attempt: u32,
}

#[derive(Serialize)]
struct IssueResponse {
    issue_identifier: String,
    issue_id: Option<String>,
    status: String,
    workspace: WorkspaceHost,
    attempts: AttemptsSummary,
    running: Option<RunningWorker>,
    retry: Option<RetrySnapshot>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    recent_events: Vec<serde_json::Value>,
    last_error: Option<String>,
}

async fn get_issue(
    State(state): State<ObservabilityState>,
    Path(issue_identifier): Path<String>,
) -> impl IntoResponse {
    let snap = state.snapshot.read().await;
    let running = snap
        .running
        .iter()
        .find(|w| w.issue_identifier == issue_identifier)
        .cloned();
    let retry = snap
        .retrying
        .iter()
        .find(|r| r.issue_identifier == issue_identifier)
        .cloned();
    if running.is_none() && retry.is_none() {
        return api_error(StatusCode::NOT_FOUND, "issue_not_found", "Issue not found")
            .into_response();
    }
    let issue_id = running
        .as_ref()
        .map(|w| w.issue_id.clone())
        .or_else(|| retry.as_ref().map(|r| r.issue_id.clone()));
    let status = if running.is_some() {
        "running".to_string()
    } else {
        "retrying".to_string()
    };
    let workspace_path = running
        .as_ref()
        .map(|w| w.workspace_path.display().to_string())
        .or_else(|| {
            retry
                .as_ref()
                .map(|r| r.workspace_path.display().to_string())
        })
        .unwrap_or_default();
    let workspace_host = running
        .as_ref()
        .and_then(|w| w.worker_host.clone())
        .or_else(|| retry.as_ref().and_then(|r| r.worker_host.clone()));
    let restart_count = retry
        .as_ref()
        .map(|r| r.attempt.saturating_sub(1))
        .unwrap_or(0);
    let current_retry_attempt = retry.as_ref().map(|r| r.attempt).unwrap_or(0);
    let last_error = retry.as_ref().and_then(|r| r.error.clone());
    let body = IssueResponse {
        issue_identifier: issue_identifier.clone(),
        issue_id,
        status,
        workspace: WorkspaceHost {
            path: workspace_path,
            host: workspace_host,
        },
        attempts: AttemptsSummary {
            restart_count,
            current_retry_attempt,
        },
        running,
        retry,
        recent_events: vec![],
        last_error,
    };
    Json(body).into_response()
}

async fn dashboard(State(state): State<ObservabilityState>) -> impl IntoResponse {
    let html =
        include_str!("dashboard.html").replace("__REFRESH_MS__", &state.refresh_ms.to_string());
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
}

/// TCP server for the observability router (blocks until the process exits or the server errors).
pub async fn serve(
    addr: SocketAddr,
    state: ObservabilityState,
    web_dashboard_enabled: bool,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        router(state, web_dashboard_enabled).into_make_service(),
    )
    .await
}

/// Build the observability router. Mount at `/` on the TCP listener.
pub fn router(state: ObservabilityState, web_dashboard_enabled: bool) -> Router {
    let mut app = Router::new()
        .route("/api/v1/state", get(get_state))
        .route("/api/v1/refresh", post(post_refresh))
        .route("/api/v1/{issue_identifier}", get(get_issue));
    if web_dashboard_enabled {
        app = app.route("/", get(dashboard));
    }
    app.with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::path::PathBuf;
    use symphony_core::{CodexTotalsSnapshot, WorkerTokenBreakdown};
    use tower::ServiceExt;

    fn test_state() -> ObservabilityState {
        ObservabilityState {
            snapshot: Arc::new(RwLock::new(OrchestratorSnapshot {
                running: vec![RunningWorker {
                    issue_id: "i1".into(),
                    issue_identifier: "PRJ-1".into(),
                    issue_state: "In Progress".into(),
                    worker_host: None,
                    workspace_path: PathBuf::from("/tmp/w/PRJ-1"),
                    started_at: Utc::now(),
                    last_activity_at: Utc::now(),
                    turns_completed: 1,
                    attempt: 1,
                    stall_restarts: 0,
                    process_id: None,
                    usage_tokens_this_run: 0,
                    current_step: String::new(),
                    session_id: None,
                    tokens: WorkerTokenBreakdown::default(),
                    last_event_at: None,
                }],
                retrying: vec![RetrySnapshot {
                    issue_id: "i2".into(),
                    issue_identifier: "PRJ-2".into(),
                    attempt: 2,
                    due_at: Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)),
                    worker_host: None,
                    workspace_path: PathBuf::from("/tmp/w/PRJ-2"),
                    error: Some("boom".into()),
                }],
                total_tokens: 42,
                codex_totals: CodexTotalsSnapshot {
                    input_tokens: 10,
                    output_tokens: 20,
                    total_tokens: 42,
                    seconds_running: 5,
                },
                rate_limits: json!({}),
            })),
            refresh: Arc::new(Notify::new()),
            refresh_ms: 3000,
        }
    }

    #[tokio::test]
    async fn get_state_returns_counts_and_rows() {
        let app = router(test_state(), false);
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/state")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["counts"]["running"], 1);
        assert_eq!(v["counts"]["retrying"], 1);
        assert_eq!(v["codex_totals"]["total_tokens"], 42);
    }

    #[tokio::test]
    async fn get_issue_404_when_missing() {
        let app = router(test_state(), false);
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/UNKNOWN-99")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn post_refresh_is_accepted() {
        let app = router(test_state(), false);
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/refresh")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
    }
}
