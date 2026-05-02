use anyhow::Result;
use chrono::{DateTime, Utc};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::execute;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::{Frame, Terminal};
use std::io::stdout;
use std::sync::Arc;
use std::time::{Duration, Instant};
use symphony_core::{OrchestratorSnapshot, RetrySnapshot, RunningWorker};
use tokio::sync::{Notify, RwLock};

#[derive(Debug, Clone)]
pub struct TuiContext {
    pub started_at: Instant,
    pub poll_interval_ms: u64,
    pub refresh_ms: u64,
    pub project_url: Option<String>,
}

pub async fn run_tui(
    snapshot: Arc<RwLock<OrchestratorSnapshot>>,
    ctx: TuiContext,
    shutdown: Arc<Notify>,
) -> Result<()> {
    enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut repaint = tokio::time::interval(Duration::from_millis(ctx.refresh_ms.max(100)));
    let mut next_refresh = Instant::now() + Duration::from_millis(ctx.poll_interval_ms.max(100));
    let result = async {
        loop {
            let snap = snapshot.read().await.clone();
            terminal.draw(|frame| render(frame, &snap, &ctx, next_refresh))?;
            if should_exit()? {
                shutdown.notify_waiters();
                break;
            }
            tokio::select! {
                _ = repaint.tick() => {}
                _ = shutdown.notified() => break,
            }
            if Instant::now() >= next_refresh {
                next_refresh = Instant::now() + Duration::from_millis(ctx.poll_interval_ms.max(100));
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let _ = disable_raw_mode();
    let _ = execute!(stdout(), LeaveAlternateScreen);
    result
}

fn should_exit() -> Result<bool> {
    if !event::poll(Duration::from_millis(10))? {
        return Ok(false);
    }
    let Event::Key(key) = event::read()? else {
        return Ok(false);
    };
    if key.kind != KeyEventKind::Press {
        return Ok(false);
    }
    Ok(matches!(key.code, KeyCode::Char('q') | KeyCode::Char('Q'))
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL)))
}

fn render(frame: &mut Frame<'_>, snap: &OrchestratorSnapshot, ctx: &TuiContext, next_refresh: Instant) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(9),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
            Constraint::Min(4),
        ])
        .split(frame.area());

    render_header(frame, chunks[0], snap, ctx, next_refresh);
    render_section_title(frame, chunks[1], "Running");
    render_running_table(frame, chunks[2], snap);
    render_section_title(frame, chunks[3], "Backoff queue");
    render_retry_queue(frame, chunks[4], snap);
}

fn render_header(
    frame: &mut Frame<'_>,
    area: Rect,
    snap: &OrchestratorSnapshot,
    ctx: &TuiContext,
    next_refresh: Instant,
) {
    let runtime = format_elapsed(ctx.started_at.elapsed().as_secs());
    let in_tokens = "n/a";
    let out_tokens = "n/a";
    let limits = "codex primary n/a | secondary n/a | credits n/a";
    let refresh = format!("{}s", next_refresh.saturating_duration_since(Instant::now()).as_secs());
    let project = ctx.project_url.as_deref().unwrap_or("n/a");
    let lines = vec![
        Line::from(vec![Span::styled(
            " SYMPHONY STATUS",
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled(" Agents: ", Style::default().fg(Color::White)),
            Span::styled(
                format!("{}/{}", snap.running.len(), snap.running.len() + snap.retrying.len()),
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled(" Throughput: ", Style::default().fg(Color::White)),
            Span::styled("n/a", Style::default().fg(Color::Green)),
            Span::styled(" tps", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled(" Runtime: ", Style::default().fg(Color::White)),
            Span::styled(runtime, Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::styled(" Tokens: ", Style::default().fg(Color::White)),
            Span::styled(format!("in {in_tokens}"), Style::default().fg(Color::Yellow)),
            Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("out {out_tokens}"), Style::default().fg(Color::Yellow)),
            Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("total {}", format_number(snap.total_tokens)),
                Style::default().fg(Color::Yellow),
            ),
        ]),
        Line::from(vec![
            Span::styled(" Rate Limits: ", Style::default().fg(Color::White)),
            Span::styled(limits, Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled(" Project: ", Style::default().fg(Color::White)),
            Span::styled(truncate(project, area.width as usize - 12), Style::default().fg(Color::Cyan)),
        ]),
        Line::from(vec![
            Span::styled(" Next refresh ", Style::default().fg(Color::White)),
            Span::styled(refresh, Style::default().fg(Color::Green)),
            Span::styled("  (q to quit)", Style::default().fg(Color::DarkGray)),
        ]),
    ];
    let header = Paragraph::new(lines).block(Block::default().borders(Borders::LEFT | Borders::RIGHT));
    frame.render_widget(header, area);
}

fn render_section_title(frame: &mut Frame<'_>, area: Rect, title: &str) {
    let paragraph = Paragraph::new(Line::from(vec![
        Span::styled(" ", Style::default()),
        Span::styled(
            title,
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
    ]));
    frame.render_widget(paragraph, area);
}

fn render_running_table(frame: &mut Frame<'_>, area: Rect, snap: &OrchestratorSnapshot) {
    let header = Row::new(vec!["ID", "STAGE", "PID", "AGE / TURN", "TOKENS", "SESSION", "EVENT"])
        .style(Style::default().fg(Color::DarkGray))
        .bottom_margin(1);

    let rows = if snap.running.is_empty() {
        vec![Row::new(vec!["-", "-", "-", "-", "-", "-", "No running workers"])
            .style(Style::default().fg(Color::DarkGray))]
    } else {
        snap.running
            .iter()
            .map(|row| running_row(row))
            .collect::<Vec<_>>()
    };

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Min(20),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::TOP))
    .column_spacing(1);
    frame.render_widget(table, area);
}

fn running_row(row: &RunningWorker) -> Row<'static> {
    let age = format_age(row.started_at);
    let turn = format!("{age} / {}", row.turns_completed.max(1));
    let stage_style = state_style(&row.issue_state);
    let event = format!(
        "running in {}",
        row.workspace_path
            .file_name()
            .map(|x| x.to_string_lossy().to_string())
            .unwrap_or_else(|| "workspace".to_string())
    );
    Row::new(vec![
        Cell::from(truncate(&row.issue_identifier, 10)),
        Cell::from(Span::styled(
            truncate(&row.issue_state, 12),
            stage_style,
        )),
        Cell::from("n/a"),
        Cell::from(truncate(&turn, 14)),
        Cell::from("n/a"),
        Cell::from("n/a"),
        Cell::from(event),
    ])
}

fn render_retry_queue(frame: &mut Frame<'_>, area: Rect, snap: &OrchestratorSnapshot) {
    if snap.retrying.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                " No queued retries",
                Style::default().fg(Color::DarkGray),
            )))
            .block(Block::default().borders(Borders::TOP | Borders::LEFT)),
            area,
        );
        return;
    }

    let header = Row::new(vec!["ID", "ATTEMPT", "DUE", "ERROR"])
        .style(Style::default().fg(Color::DarkGray))
        .bottom_margin(1);
    let rows = snap
        .retrying
        .iter()
        .map(retry_row)
        .collect::<Vec<_>>();
    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(24),
            Constraint::Min(20),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::TOP))
    .column_spacing(1);
    frame.render_widget(table, area);
}

fn retry_row(row: &RetrySnapshot) -> Row<'static> {
    Row::new(vec![
        Cell::from(truncate(&row.issue_identifier, 12)),
        Cell::from(row.attempt.to_string()),
        Cell::from(row.due_at.clone().unwrap_or_else(|| "n/a".to_string())),
        Cell::from(truncate(row.error.as_deref().unwrap_or(""), 80)),
    ])
}

fn state_style(state: &str) -> Style {
    let s = state.to_ascii_lowercase();
    if s.contains("progress") {
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
    } else if s.contains("rework") {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else if s.contains("todo") {
        Style::default().fg(Color::Blue)
    } else {
        Style::default().fg(Color::White)
    }
}

fn format_age(started_at: DateTime<Utc>) -> String {
    let secs = (Utc::now() - started_at).num_seconds().max(0) as u64;
    format_elapsed(secs)
}

fn format_elapsed(secs: u64) -> String {
    let mins = secs / 60;
    let seconds = secs % 60;
    if mins > 0 {
        format!("{mins}m {seconds}s")
    } else {
        format!("{seconds}s")
    }
}

fn truncate(value: &str, max_len: usize) -> String {
    if max_len == 0 {
        return String::new();
    }
    let len = value.chars().count();
    if len <= max_len {
        return value.to_string();
    }
    if max_len == 1 {
        return "…".to_string();
    }
    let mut out = String::new();
    for ch in value.chars().take(max_len - 1) {
        out.push(ch);
    }
    if len > max_len {
        out.push('…');
    }
    out
}

fn format_number(value: u64) -> String {
    let s = value.to_string();
    let mut out = String::new();
    for (idx, ch) in s.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncates_with_ellipsis() {
        assert_eq!(truncate("ABCDEFGHIJK", 6), "ABCDE…");
    }

    #[test]
    fn keeps_short_strings() {
        assert_eq!(truncate("ABC", 6), "ABC");
    }

    #[test]
    fn formats_elapsed_minutes_and_seconds() {
        assert_eq!(format_elapsed(125), "2m 5s");
    }

    #[test]
    fn formats_elapsed_seconds_only() {
        assert_eq!(format_elapsed(42), "42s");
    }

    #[test]
    fn formats_numbers_with_thousands_separator() {
        assert_eq!(format_number(1_234_567), "1,234,567");
    }
}
