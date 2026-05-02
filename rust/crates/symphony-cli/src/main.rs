use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use symphony_config::{Settings, observability_http_enabled, workflow_path};
use symphony_core::Orchestrator;
use symphony_core::OrchestratorSnapshot;
use symphony_observability::ObservabilityState;
use symphony_tracker::{Issue, IssueState, Tracker, TrackerError};
use symphony_tracker_linear::LinearTracker;
use symphony_workflow::WorkflowStore;
use tokio::sync::{Notify, RwLock};
use tokio::time::{Duration, sleep};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

mod tui;

const GUARDRAILS_ACK_LONG: &str =
    "i-understand-that-this-will-be-running-without-the-usual-guardrails";

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

    async fn create_comment(&self, _issue_id: &str, _body: &str) -> Result<(), TrackerError> {
        Ok(())
    }

    async fn update_issue_state(
        &self,
        _issue_id: &str,
        _state_name: &str,
    ) -> Result<(), TrackerError> {
        Ok(())
    }
}

#[derive(Parser, Debug)]
#[command(name = "symphony")]
#[command(about = "Symphony orchestrator (Rust)")]
struct Cli {
    /// Override `server.port` from workflow config (enables observability HTTP API when > 0).
    #[arg(long)]
    port: Option<u16>,
    /// Override `server.host` from workflow config.
    #[arg(long)]
    host: Option<String>,
    /// Write tracing logs to `<logs-root>/symphony.log` (non-blocking) in addition to stderr.
    #[arg(long, value_name = "DIR")]
    logs_root: Option<PathBuf>,
    /// Enable terminal dashboard UI.
    #[arg(long)]
    tui: bool,
    #[arg(
        long = GUARDRAILS_ACK_LONG,
        help = "Required when runtime.require_guardrails_ack is true in WORKFLOW.md"
    )]
    guardrails_ack: bool,
    /// Path to WORKFLOW.md (YAML front matter + prompt body).
    #[arg(value_name = "WORKFLOW.md", default_value = "./WORKFLOW.md")]
    workflow: PathBuf,
}

fn guardrails_banner() -> String {
    let lines = [
        "This Symphony implementation is a low key engineering preview.",
        "Codex will run without any guardrails.",
        "Symphony is not a supported product and is presented as-is.",
        "To proceed, pass --i-understand-that-this-will-be-running-without-the-usual-guardrails",
    ];
    let width = lines.iter().map(|s| s.len()).max().unwrap_or(0);
    let border = "─".repeat(width + 2);
    let mut out = String::new();
    out.push_str(&format!("╭{border}╮\n"));
    out.push_str(&format!("│ {} │\n", " ".repeat(width)));
    for line in lines {
        out.push_str(&format!("│ {:width$} │\n", line, width = width));
    }
    out.push_str(&format!("│ {} │\n", " ".repeat(width)));
    out.push_str(&format!("╰{border}╯\n"));
    out
}

fn init_tracing(
    logs_root: Option<&std::path::Path>,
) -> Result<Option<tracing_appender::non_blocking::WorkerGuard>> {
    let filter = EnvFilter::from_default_env();
    if let Some(root) = logs_root {
        std::fs::create_dir_all(root)
            .with_context(|| format!("failed to create logs root {}", root.display()))?;
        let file_appender = tracing_appender::rolling::never(root, "symphony.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .try_init()
            .map_err(|e| anyhow::anyhow!("tracing init failed: {e}"))?;
        Ok(Some(guard))
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|e| anyhow::anyhow!("tracing init failed: {e}"))?;
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let workflow_file = workflow_path(Some(cli.workflow.clone()));

    let mut settings = Settings::load_from_workflow_file(&workflow_file)
        .with_context(|| format!("failed to load workflow file: {}", workflow_file.display()))?;

    if let Some(p) = cli.port {
        settings.server.port = p;
    }
    if let Some(h) = cli.host.as_ref() {
        if !h.trim().is_empty() {
            settings.server.host = h.trim().to_string();
        }
    }
    settings
        .validate()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    if settings.runtime.require_guardrails_ack && !cli.guardrails_ack {
        eprint!("{}", guardrails_banner());
        std::process::exit(1);
    }

    let _log_guard = init_tracing(cli.logs_root.as_deref())?;

    let workflow_store = WorkflowStore::load(&workflow_file)
        .with_context(|| format!("failed to load workflow file: {}", workflow_file.display()))?;

    let tui_enabled = cli.tui || settings.observability.tui_enabled;
    let http_enabled = observability_http_enabled(&settings);
    let refresh = Arc::new(Notify::new());
    let shutdown = Arc::new(Notify::new());
    let snapshot = Arc::new(RwLock::new(OrchestratorSnapshot::default()));
    let publisher = if http_enabled || tui_enabled {
        Some(snapshot.clone())
    } else {
        None
    };

    if http_enabled {
        let addr_str = format!("{}:{}", settings.server.host.trim(), settings.server.port);
        let addr = addr_str
            .parse()
            .with_context(|| format!("invalid bind address: {addr_str}"))?;
        let obs_state = ObservabilityState {
            snapshot: snapshot.clone(),
            refresh: refresh.clone(),
            refresh_ms: settings.observability.refresh_ms,
        };
        let web = settings.observability.web_dashboard_enabled;
        tokio::spawn(async move {
            if let Err(e) = symphony_observability::serve(addr, obs_state, web).await {
                error!(error = %e, "observability HTTP server exited");
            }
        });
        info!(%addr, "observability API listening");
    }

    if tui_enabled {
        let tui_snapshot = snapshot.clone();
        let tui_shutdown = shutdown.clone();
        let tui_ctx = tui::TuiContext {
            started_at: std::time::Instant::now(),
            poll_interval_ms: settings.polling.interval_ms,
            refresh_ms: settings.observability.refresh_ms,
            project_url: linear_project_url(&settings),
        };
        tokio::spawn(async move {
            if let Err(err) = tui::run_tui(tui_snapshot, tui_ctx, tui_shutdown.clone()).await {
                error!(error = %err, "tui exited with error");
                tui_shutdown.notify_waiters();
            }
        });
        info!("terminal dashboard enabled");
    }

    let poll_ms = settings.polling.interval_ms;

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
            settings.tracker.assignee.clone(),
        );
        let mut orchestrator = Orchestrator::new(settings, tracker, workflow_store, publisher)?;
        run_poll_loop(&mut orchestrator, poll_ms, refresh, shutdown).await?;
    } else {
        let tracker = MemoryTracker { issues: vec![] };
        let mut orchestrator = Orchestrator::new(settings, tracker, workflow_store, publisher)?;
        run_poll_loop(&mut orchestrator, poll_ms, refresh, shutdown).await?;
    }

    Ok(())
}

fn linear_project_url(settings: &Settings) -> Option<String> {
    let endpoint = settings
        .tracker
        .endpoint
        .as_deref()?
        .trim()
        .trim_end_matches('/');
    let project = settings.tracker.project.as_deref()?.trim();
    if endpoint.is_empty() || project.is_empty() {
        return None;
    }
    if endpoint.contains("linear.app") {
        Some(format!("{endpoint}/app/project/{project}/issues"))
    } else {
        Some(endpoint.to_string())
    }
}

async fn run_poll_loop<T: Tracker>(
    orchestrator: &mut Orchestrator<T>,
    poll_interval_ms: u64,
    refresh: Arc<Notify>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    info!(poll_interval_ms, "symphony orchestrator started");
    loop {
        if let Err(err) = orchestrator.tick().await {
            error!(error=%err, "orchestrator tick failed");
        }

        tokio::select! {
            _ = sleep(Duration::from_millis(poll_interval_ms)) => {}
            _ = refresh.notified() => {
                info!("observability refresh requested, running next tick immediately");
            }
            _ = shutdown.notified() => {
                info!("shutdown requested by dashboard, exiting");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received, exiting");
                shutdown.notify_waiters();
                break;
            }
        }
    }
    Ok(())
}
