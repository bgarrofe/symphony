use anyhow::Result;
use chrono::Utc;
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Cell, Paragraph, Row, Table, TableState},
};
use std::io::{self};
use std::sync::Arc;
use std::time::{Duration, Instant};
use symphony_core::{OrchestratorSnapshot, RetrySnapshot, RunningWorker};
use tokio::sync::{Notify, RwLock};

const BG: Color = Color::Rgb(30, 32, 38);
const LABEL: Color = Color::Rgb(220, 220, 220);
const VALUE: Color = Color::Rgb(97, 175, 239);
const YELLOW: Color = Color::Rgb(229, 192, 123);
const GREEN: Color = Color::Rgb(152, 195, 121);
const CYAN: Color = Color::Rgb(86, 182, 194);
const ORANGE: Color = Color::Rgb(209, 154, 102);
const DIM: Color = Color::Rgb(92, 99, 112);
const HEADER: Color = Color::Rgb(92, 99, 112);
const ROW_ALT: Color = Color::Rgb(33, 35, 42);
const SEL_BG: Color = Color::Rgb(44, 49, 60);

#[derive(Debug, Clone)]
pub struct TuiContext {
    pub started_at: Instant,
    pub poll_interval_ms: u64,
    pub refresh_ms: u64,
    pub project_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum Stage {
    Backlog,
    Todo,
    InProgress,
    Rework,
    InReview,
    Done,
}

impl Stage {
    const ALL: [Stage; 6] = [
        Stage::Backlog,
        Stage::Todo,
        Stage::InProgress,
        Stage::Rework,
        Stage::InReview,
        Stage::Done,
    ];

    fn label(&self) -> &'static str {
        match self {
            Stage::Backlog => "Backlog",
            Stage::Todo => "Todo",
            Stage::InProgress => "In Progress",
            Stage::Rework => "Rework",
            Stage::InReview => "In Review",
            Stage::Done => "Done",
        }
    }

    fn short_label(&self) -> &'static str {
        match self {
            Stage::Backlog => "Bk",
            Stage::Todo => "Td",
            Stage::InProgress => "IP",
            Stage::Rework => "Rw",
            Stage::InReview => "Rv",
            Stage::Done => "Dn",
        }
    }

    fn color(&self) -> Color {
        match self {
            Stage::Backlog => DIM,
            Stage::Todo => YELLOW,
            Stage::InProgress => GREEN,
            Stage::Rework => ORANGE,
            Stage::InReview => CYAN,
            Stage::Done => Color::Rgb(128, 132, 140),
        }
    }
}

#[derive(Clone, Debug)]
struct Agent {
    id: String,
    stage: Stage,
    pid: String,
    age: String,
    turn: u32,
    tokens: String,
    session: String,
    /// Codex/Cursor activity line (RPC `method`, stream `type`, etc.).
    event: String,
}

struct AnimationState {
    table_state: TableState,
    tick: u64,
    throughput: u64,
    tokens_in: u64,
    tokens_out: u64,
}

impl AnimationState {
    fn new() -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));
        Self {
            table_state,
            tick: 0,
            throughput: 658_875,
            tokens_in: 38_183_882,
            tokens_out: 368_361,
        }
    }

    fn next_row(&mut self, row_count: usize) {
        if row_count == 0 {
            self.table_state.select(None);
            return;
        }
        let i = match self.table_state.selected() {
            Some(i) => (i + 1).min(row_count - 1),
            None => 0,
        };
        self.table_state.select(Some(i));
    }

    fn prev_row(&mut self, row_count: usize) {
        if row_count == 0 {
            self.table_state.select(None);
            return;
        }
        let i = self.table_state.selected().unwrap_or(0).saturating_sub(1);
        self.table_state.select(Some(i));
    }

    fn on_tick(&mut self) {
        self.tick += 1;
        self.throughput = 658_875_u64.saturating_add((self.tick % 7) * 150);
        self.tokens_in += 12_000;
        self.tokens_out += 500;
    }

    fn clamp_selection(&mut self, row_count: usize) {
        if row_count == 0 {
            self.table_state.select(None);
            return;
        }
        let i = self.table_state.selected().unwrap_or(0).min(row_count - 1);
        self.table_state.select(Some(i));
    }
}

fn stage_from_issue_state(state: &str) -> Stage {
    let s = state.trim();
    let n = s.to_ascii_lowercase();
    match n.as_str() {
        "backlog" => return Stage::Backlog,
        "todo" => return Stage::Todo,
        "in progress" => return Stage::InProgress,
        "rework" => return Stage::Rework,
        "in review" => return Stage::InReview,
        "done" => return Stage::Done,
        _ => {}
    }
    // Fuzzy fallbacks for minor tracker naming differences / extra punctuation.
    if n.contains("in review") || (n.contains("review") && !n.contains("progress")) {
        return Stage::InReview;
    }
    if n.contains("progress") {
        return Stage::InProgress;
    }
    if n.contains("rework") {
        return Stage::Rework;
    }
    if n.contains("backlog") {
        return Stage::Backlog;
    }
    if n.contains("done") || n.contains("complete") || n.contains("closed") {
        return Stage::Done;
    }
    if n.contains("todo") {
        return Stage::Todo;
    }
    Stage::Todo
}

fn running_worker_to_agent(w: &RunningWorker) -> Agent {
    let secs = (Utc::now() - w.started_at).num_seconds().max(0) as u64;
    let age = format!("{}m {}s", secs / 60, secs % 60);
    let turn = w.turns_completed.max(1);
    let pid = w
        .process_id
        .map(|p| p.to_string())
        .unwrap_or_else(|| "—".to_string());
    let tok_n = w.usage_tokens_this_run.max(w.tokens.total_tokens);
    let tokens = if tok_n > 0 {
        format_num(tok_n)
    } else {
        "—".to_string()
    };
    let workspace_hint = format!(
        "running in {}",
        w.workspace_path
            .file_name()
            .map(|x| x.to_string_lossy().to_string())
            .unwrap_or_else(|| "workspace".to_string())
    );
    let event = if w.current_step.is_empty() {
        workspace_hint
    } else {
        w.current_step.clone()
    };
    let session_src = w
        .session_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(w.issue_id.as_str());
    Agent {
        id: w.issue_identifier.clone(),
        stage: stage_from_issue_state(&w.issue_state),
        pid,
        age,
        turn,
        tokens,
        session: truncate(session_src, 16),
        event,
    }
}

pub async fn run_tui(
    snapshot: Arc<RwLock<OrchestratorSnapshot>>,
    ctx: TuiContext,
    shutdown: Arc<Notify>,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let poll_interval = Duration::from_millis(ctx.poll_interval_ms.max(100));
    let tick_rate = Duration::from_millis(ctx.refresh_ms.max(100));
    let mut next_pull_deadline = Instant::now() + poll_interval;
    let mut anim = AnimationState::new();
    let mut last_anim_tick = Instant::now();

    let result = async {
        loop {
            if Instant::now() >= next_pull_deadline {
                next_pull_deadline = Instant::now() + poll_interval;
            }
            let next_refresh_secs = next_pull_deadline
                .saturating_duration_since(Instant::now())
                .as_secs();

            let snap = snapshot.read().await.clone();
            let agents: Vec<Agent> = snap.running.iter().map(running_worker_to_agent).collect();
            let table_row_count = if agents.is_empty() { 1 } else { agents.len() };
            anim.clamp_selection(table_row_count);

            terminal.draw(|f| {
                draw(
                    f,
                    &ctx,
                    &agents,
                    &snap.retrying,
                    &mut anim,
                    next_refresh_secs,
                )
            })?;

            tokio::select! {
                biased;
                _ = shutdown.notified() => break,
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }

            while event::poll(Duration::ZERO)? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => {
                                shutdown.notify_waiters();
                                break;
                            }
                            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                                shutdown.notify_waiters();
                                break;
                            }
                            KeyCode::Down | KeyCode::Char('j') => anim.next_row(table_row_count),
                            KeyCode::Up | KeyCode::Char('k') => anim.prev_row(table_row_count),
                            KeyCode::Char('r') => anim.on_tick(),
                            _ => {}
                        }
                    }
                }
            }

            if last_anim_tick.elapsed() >= tick_rate {
                anim.on_tick();
                last_anim_tick = Instant::now();
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    result
}

fn format_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn draw(
    f: &mut Frame,
    ctx: &TuiContext,
    agents: &[Agent],
    retries: &[RetrySnapshot],
    anim: &mut AnimationState,
    next_refresh_secs: u64,
) {
    let area = f.area();
    f.render_widget(Block::default().style(Style::default().bg(BG)), area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10),
            Constraint::Min(0),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(area);

    draw_status(f, ctx, agents, anim, chunks[0], next_refresh_secs);
    draw_table(f, agents, anim, chunks[1]);
    draw_backoff(f, retries, chunks[2]);
    draw_help(f, chunks[3]);
}

fn draw_status(
    f: &mut Frame,
    ctx: &TuiContext,
    agents: &[Agent],
    anim: &AnimationState,
    area: Rect,
    next_refresh_secs: u64,
) {
    let b = Modifier::BOLD;
    let lb = Style::default().fg(LABEL).add_modifier(Modifier::BOLD);
    let vs = Style::default().fg(VALUE);
    let gs = Style::default().fg(GREEN);
    let ds = Style::default().fg(DIM);
    let cs = Style::default().fg(CYAN);

    let secs = ctx.started_at.elapsed().as_secs();
    let elapsed = format!("{}m {}s", secs / 60, secs % 60);
    let project = ctx.project_url.as_deref().unwrap_or("[no project URL]");

    let lines = vec![
        Line::from(Span::styled("─ SYMPHONY STATUS", lb)),
        Line::from(vec![
            Span::styled("Agents: ", lb),
            Span::styled(agents.len().to_string(), gs),
            Span::styled("/50", ds),
        ]),
        {
            let mut spans: Vec<Span> = vec![Span::styled("By stage:  ", lb)];
            for (i, st) in Stage::ALL.iter().enumerate() {
                if i > 0 {
                    spans.push(Span::styled(" · ", ds));
                }
                let c = agents.iter().filter(|a| a.stage == *st).count();
                let ch = Style::default().fg(st.color());
                spans.push(Span::styled(
                    st.short_label(),
                    ch.add_modifier(Modifier::BOLD),
                ));
                spans.push(Span::styled(":", ds));
                spans.push(Span::styled(c.to_string(), ch));
            }
            Line::from(spans)
        },
        Line::from(vec![
            Span::styled("Throughput:  ", lb),
            Span::styled(format!("{} tps", format_num(anim.throughput)), vs),
        ]),
        Line::from(vec![
            Span::styled("Runtime:     ", lb),
            Span::styled(elapsed, vs),
        ]),
        Line::from(vec![
            Span::styled("Tokens:      ", lb),
            Span::styled("in ", ds),
            Span::styled(format_num(anim.tokens_in), cs),
            Span::styled(" | out ", ds),
            Span::styled(format_num(anim.tokens_out), cs),
            Span::styled(" | total ", ds),
            Span::styled(format_num(anim.tokens_in + anim.tokens_out), cs),
        ]),
        Line::from(vec![
            Span::styled("Rate Limits: ", lb),
            Span::styled("codex", ds),
            Span::styled(" | primary n/a | secondary n/a | credits n/a", ds),
        ]),
        Line::from(vec![
            Span::styled("Project:     ", lb),
            Span::styled(
                project,
                Style::default()
                    .fg(Color::Rgb(86, 156, 214))
                    .add_modifier(Modifier::UNDERLINED),
            ),
        ]),
        Line::from(vec![
            Span::styled("Next refresh: ", lb),
            Span::styled(format!("{next_refresh_secs}s"), vs),
        ]),
        Line::from(vec![
            Span::styled("─ ", ds),
            Span::styled("Running", gs.add_modifier(b)),
        ]),
    ];

    f.render_widget(Paragraph::new(lines).style(Style::default().bg(BG)), area);
}

fn draw_table(f: &mut Frame, agents: &[Agent], anim: &mut AnimationState, area: Rect) {
    let header = Row::new(
        [
            "ID",
            "STAGE",
            "PID",
            "AGE / TURN",
            "TOKENS",
            "SESSION",
            "EVENT",
        ]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().fg(HEADER).add_modifier(Modifier::BOLD))),
    )
    .style(Style::default().bg(BG))
    .height(1);

    let rows: Vec<Row> = if agents.is_empty() {
        vec![
            Row::new(vec![
                Cell::from("-"),
                Cell::from("-"),
                Cell::from("-"),
                Cell::from("-"),
                Cell::from("-"),
                Cell::from("-"),
                Cell::from("No running workers").style(Style::default().fg(DIM)),
            ])
            .style(Style::default().bg(BG))
            .height(1),
        ]
    } else {
        agents
            .iter()
            .enumerate()
            .map(|(i, agent)| {
                let bg = if i % 2 == 0 { BG } else { ROW_ALT };

                Row::new(vec![
                    Cell::from(Line::from(vec![
                        Span::styled("● ", Style::default().fg(agent.stage.color())),
                        Span::styled(truncate(&agent.id, 24), Style::default().fg(LABEL)),
                    ])),
                    Cell::from(agent.stage.label()).style(Style::default().fg(agent.stage.color())),
                    Cell::from(agent.pid.clone()).style(Style::default().fg(DIM)),
                    Cell::from(format!("{} / {}", agent.age, agent.turn))
                        .style(Style::default().fg(DIM)),
                    Cell::from(agent.tokens.clone()).style(Style::default().fg(CYAN)),
                    Cell::from(agent.session.clone())
                        .style(Style::default().fg(Color::Rgb(130, 100, 180))),
                    Cell::from(truncate(&agent.event, 96))
                        .style(Style::default().fg(Color::Rgb(180, 180, 180))),
                ])
                .style(Style::default().bg(bg))
                .height(1)
            })
            .collect()
    };

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(13),
            Constraint::Length(9),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(16),
            Constraint::Min(20),
        ],
    )
    .header(header)
    .highlight_style(Style::default().bg(SEL_BG).add_modifier(Modifier::BOLD))
    .highlight_symbol("▶ ")
    .block(Block::default().style(Style::default().bg(BG)));

    f.render_stateful_widget(table, area, &mut anim.table_state);
}

fn draw_backoff(f: &mut Frame, retries: &[RetrySnapshot], area: Rect) {
    let mut lines = vec![Line::from(vec![
        Span::styled("─ ", Style::default().fg(DIM)),
        Span::styled(
            "Backoff queue",
            Style::default().fg(LABEL).add_modifier(Modifier::BOLD),
        ),
    ])];
    if retries.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No queued retries",
            Style::default().fg(DIM),
        )));
    } else {
        for r in retries.iter().take(4) {
            let err = truncate(r.error.as_deref().unwrap_or(""), 64);
            lines.push(Line::from(vec![
                Span::styled("  ", Style::default().fg(DIM)),
                Span::styled(
                    format!("{} ", truncate(&r.issue_identifier, 12)),
                    Style::default().fg(LABEL),
                ),
                Span::styled(err, Style::default().fg(DIM)),
            ]));
        }
    }
    f.render_widget(Paragraph::new(lines).style(Style::default().bg(BG)), area);
}

fn draw_help(f: &mut Frame, area: Rect) {
    let line = Line::from(vec![
        Span::styled(
            " ↑↓/jk ",
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
        ),
        Span::styled("navigate   ", Style::default().fg(DIM)),
        Span::styled("q ", Style::default().fg(CYAN).add_modifier(Modifier::BOLD)),
        Span::styled("quit   ", Style::default().fg(DIM)),
        Span::styled("r ", Style::default().fg(CYAN).add_modifier(Modifier::BOLD)),
        Span::styled("force refresh", Style::default().fg(DIM)),
    ]);
    f.render_widget(
        Paragraph::new(line).style(Style::default().bg(Color::Rgb(25, 27, 32))),
        area,
    );
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
    fn formats_numbers_with_thousands_separator() {
        assert_eq!(format_num(1_234_567), "1,234,567");
    }
}
