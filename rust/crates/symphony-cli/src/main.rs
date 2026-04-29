use anyhow::{Context, Result};
use std::path::PathBuf;
use symphony_config::{Settings, workflow_path};
use symphony_core::Orchestrator;
use symphony_tracker::{Issue, IssueState, Tracker, TrackerError};
use symphony_tracker_linear::LinearTracker;
use symphony_workflow::WorkflowStore;
use tokio::time::{Duration, sleep};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct MemoryTracker {
    issues: Vec<Issue>,
}

#[async_trait::async_trait]
impl Tracker for MemoryTracker {
    async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>, TrackerError> {
        Ok(self.issues.clone())
    }
    async fn fetch_issues_by_states(&self, _states: &[String]) -> Result<Vec<Issue>, TrackerError> {
        Ok(self.issues.clone())
    }
    async fn fetch_issue_states_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<IssueState>, TrackerError> {
        Ok(self
            .issues
            .iter()
            .filter(|i| ids.contains(&i.id))
            .map(|i| IssueState {
                id: i.id.clone(),
                state: i.state.clone(),
            })
            .collect())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let workflow_file = workflow_path(std::env::args().nth(1).map(PathBuf::from));
    let workflow_store = WorkflowStore::load(&workflow_file)
        .with_context(|| format!("failed to load workflow file: {}", workflow_file.display()))?;
    let settings = Settings::load_from_workflow_file(&workflow_file)?;

    if settings.tracker.kind.eq_ignore_ascii_case("linear") {
        let endpoint = settings
            .tracker
            .endpoint
            .clone()
            .context("tracker.endpoint is required for linear tracker")?;
        let token = settings
            .tracker
            .token
            .clone()
            .context("tracker.token is required for linear tracker")?;
        let tracker = LinearTracker::new(
            endpoint,
            token,
            settings.tracker.project.clone(),
            settings.tracker.active_states.clone(),
        );
        let poll_ms = settings.polling.interval_ms;
        let mut orchestrator = Orchestrator::new(settings, tracker, workflow_store)?;
        run_poll_loop(&mut orchestrator, poll_ms).await?;
    } else {
        let tracker = MemoryTracker { issues: vec![] };
        let poll_ms = settings.polling.interval_ms;
        let mut orchestrator = Orchestrator::new(settings, tracker, workflow_store)?;
        run_poll_loop(&mut orchestrator, poll_ms).await?;
    }

    Ok(())
}

async fn run_poll_loop<T: Tracker>(
    orchestrator: &mut Orchestrator<T>,
    poll_interval_ms: u64,
) -> Result<()> {
    info!(poll_interval_ms, "symphony orchestrator started");
    loop {
        if let Err(err) = orchestrator.tick().await {
            // Keep host alive even when one polling cycle fails.
            error!(error=%err, "orchestrator tick failed");
        }

        tokio::select! {
            _ = sleep(Duration::from_millis(poll_interval_ms)) => {}
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received, exiting");
                break;
            }
        }
    }
    Ok(())
}
