# Symphony Rust

Rust implementation of Symphony’s core orchestration model, based on the repository spec at [`../SPEC.md`](../SPEC.md) and behavior patterns from the Elixir reference implementation in [`../elixir`](../elixir).

This workspace is organized as a modular multi-crate system focused on:

- Polling tracker work
- Rendering strict workflow prompts from `WORKFLOW.md`
- Running per-issue isolated agent sessions via Codex app-server
- Applying retry and safety controls suitable for unattended operation

This README is intentionally detailed so it can be used as both onboarding and operator documentation.

## Status

Current implementation is **core-focused** and actively evolving.

Implemented:

- Multi-crate workspace and runtime wiring
- `WORKFLOW.md` parsing with YAML front matter + prompt body
- Strict template rendering (`minijinja` with strict undefined behavior)
- Workflow reload with last-known-good fallback behavior
- Typed config defaults + validation
- Workspace lifecycle primitives and hook execution
- Tracker trait and Linear GraphQL adapter scaffold
- Codex app-server process + JSON message loop baseline
- Dual runtime interface selection (`codex` default, optional `cursor_cli` translation path)
- Core orchestrator dispatch/retry/token bookkeeping baseline
- Unit tests covering major primitives
- Optional **HTTP observability API** and minimal web dashboard (`symphony-observability`, Elixir-aligned routes) when `server.port > 0` or via CLI `--port`
- Optional **terminal dashboard (TUI)** with live running/retry status (`--tui` or `observability.tui_enabled: true`)
- CLI flags: `--port`, `--host`, `--logs-root`, optional guardrails acknowledgment (see below)

Not yet complete:

- Full one-to-one parity with all Elixir runtime semantics
- Full acceptance test matrix from `SPEC.md`
- Parity gaps for observability JSON (per-issue Codex session fields, live rate limits) — see Observability section
- **SSH remote execution**: `worker.ssh_hosts` and per-host limits are enforced for **scheduling/capacity**; the Rust runtime still launches Codex/Cursor **locally** in the workspace (remote process launch is not wired yet).

---

## Repository Layout

Workspace root:

- `symphony/rust/Cargo.toml`

Crates:

- `crates/symphony-cli` - Binary entrypoint, runtime bootstrap, tracker selection.
- `crates/symphony-core` - Orchestrator state and issue execution lifecycle.
- `crates/symphony-config` - Settings schema, defaults, workflow-path handling, validation.
- `crates/symphony-workflow` - `WORKFLOW.md` parser, strict prompt rendering, reload store.
- `crates/symphony-tracker` - Tracker trait and normalized domain types.
- `crates/symphony-tracker-linear` - Linear GraphQL tracker adapter.
- `crates/symphony-workspace` - Workspace naming/path safety, hooks, create/remove helpers.
- `crates/symphony-codex` - Codex app-server process integration and protocol loop.
- `crates/symphony-observability` - Axum HTTP server: `/api/v1/state`, `/api/v1/refresh`, `/api/v1/{issue_identifier}`, optional `/` dashboard.

---

## Requirements

- Rust stable toolchain (edition 2024 compatible)
- `cargo`
- `bash` available on PATH
- `codex` CLI available on PATH (for runtime sessions)
- A `WORKFLOW.md` file at repository root, or explicit path argument
- For Linear mode:
  - Linear API endpoint
  - Linear token
  - Optional project key/filter

---

## Quick Start

From the repository root:

```bash
cd symphony/rust
cargo test
```

Run the CLI:

```bash
cd symphony/rust
cargo run -p symphony-cli -- ../WORKFLOW.md
# or (same default path)
cargo run -p symphony-cli
```

Use `cargo run -p symphony-cli -- --help` for flags. Common options:

- `--port <u16>` — overrides `server.port` (non-zero starts the observability listener).
- `--host <str>` — overrides `server.host` (bind address).
- `--logs-root <dir>` — writes tracing output to `<dir>/symphony.log` (non-blocking) in addition to stderr.
- `--tui` — enables the terminal dashboard UI.
- `--i-understand-that-this-will-be-running-without-the-usual-guardrails` — required when `runtime.require_guardrails_ack: true` in workflow front matter (default is `false`).

If no positional workflow argument is given, the default is `./WORKFLOW.md` (relative to the process working directory).

---

## `WORKFLOW.md` Format

The workflow file supports optional YAML front matter followed by the prompt template body.

Example:

```md
---
polling:
  interval_ms: 5000
workspace:
  root: ./.symphony/workspaces
agent:
  max_turns: 4
codex:
  command: codex app-server
  stall_timeout_ms: 120000
tracker:
  kind: linear
  endpoint: $LINEAR_ENDPOINT
  token: $LINEAR_TOKEN
  project: $LINEAR_PROJECT
---
You are working on issue {{ issue.identifier }}.
Attempt {{ attempt }}.

Title: {{ issue.title }}
Description:
{{ issue.description }}
```

### Parsing Rules

- If front matter exists, it must decode to a map/object.
- If body is empty, a default prompt template is used.
- Unknown template variables fail rendering (strict mode).
- Invalid reloads do not crash the process; last-known-good workflow remains active.

---

## Configuration Model

Current typed settings tree (from `symphony-config`):

- `polling.interval_ms` (must be `> 0`)
- `workspace.root` (must be non-empty and not `/`)
- `hooks.after_create` / `hooks.before_run` / `hooks.after_run` / `hooks.before_remove`
- `hooks.timeout_ms`
- `agent.max_turns` (must be `> 0`)
- `agent.max_concurrent_agents` (must be `> 0`, default `1`)
- `agent.max_retry_backoff_ms` (must be `> 0`, default `300000`; caps exponential failure/stall retry delays, Elixir-aligned base `10s * 2^n`)
- `agent.max_concurrent_agents_by_state` (optional map `state_name -> limit`; compares case-insensitively to tracker state)
- `worker.ssh_hosts` (optional list of SSH destinations, e.g. `host` or `host:port`)
- `worker.max_concurrent_agents_per_host` (optional; must be `> 0` and requires non-empty `ssh_hosts`)
- `codex.command` (non-empty)
- `codex.approval_policy` (JSON string or map; default is Elixir-style `reject:{sandbox_approval,rules,mcp_elicitations}` map; string `"never"` enables auto-approval of session prompts — matches Elixir)
- `codex.thread_sandbox` (required when set; default `workspace-write`; sent as `sandbox` on `thread/start`)
- `codex.turn_sandbox_policy` (optional map; sent as `sandboxPolicy` on `turn/start` when present)
- `codex.turn_timeout_ms` (total turn stream timeout, default `900000`)
- `codex.read_timeout_ms` (must be `> 0`, default `5000`; shorter idle deadline **before** the first Codex protocol event in a turn, analogous to Elixir handshake read timeout)
- `codex.stall_timeout_ms`
- `codex.detailed_app_server_logs` (default `false`; when true, logs full app-server message payloads)
- `runtime.interface` (`codex` default, or `cursor_cli`)
- `runtime.require_guardrails_ack` (default `false`; when `true`, CLI must pass the long acknowledgment flag above)
- `server.host` (default `127.0.0.1`)
- `server.port` (default `0` = no HTTP server; set `> 0` to enable observability API on that port)
- `observability.api_enabled` (default `false`; if `true`, `server.port` must be `> 0` — you can also enable HTTP by setting `server.port` alone)
- `observability.web_dashboard_enabled` (default `true`; serves a minimal HTML page at `/` that polls `/api/v1/state`)
- `observability.refresh_ms` (default `3000`; interval hint for the dashboard auto-refresh)
- `observability.tui_enabled` (default `false`; renders a local terminal dashboard; CLI `--tui` also enables it)
- `tracker.kind`
- `tracker.endpoint` / `tracker.token` / `tracker.project`
- `tracker.active_states` (workflow states for candidate polling; filtered server-side when non-empty; when empty, the Linear adapter pages issues without a state filter—optional project filter only)
- `tracker.terminal_states` (case-insensitive workflow state names; used for running reconciliation / terminal workspace cleanup **and** for dispatch suppression—custom terminal labels must appear here to avoid routing terminal issues)
- `tracker.assignee` (optional Linear user id filter for dispatch routing; maps Elixir’s assignee semantics onto `assigned_to_worker`. Use string `me` to resolve the authenticated viewer via Linear `viewer { id }`; whitespace-only values are treated as unset)

Environment indirection is supported for selected string fields via `$VAR_NAME` (including `tracker.endpoint`, `tracker.token`, `tracker.project`, and `tracker.assignee`, e.g. `$LINEAR_ASSIGNEE`).

### Example: observability server

```yaml
server:
  host: 127.0.0.1
  port: 8787
observability:
  api_enabled: true
  web_dashboard_enabled: true
  refresh_ms: 3000
```

Equivalent: `cargo run -p symphony-cli -- --port 8787 ./WORKFLOW.md` (CLI overrides `server.port` after loading the file).

### Example: terminal dashboard (TUI)

```yaml
observability:
  tui_enabled: true
  refresh_ms: 500
```

Or enable ad-hoc from CLI:

```bash
cargo run -p symphony-cli -- --tui ./WORKFLOW.md
```

---

## Observability (HTTP)

When `server.port > 0` (after config + CLI merge), the CLI spawns **`symphony-observability`** on `server.host:server.port`.

**Routes** (aligned with the Elixir Phoenix router where practical):

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/state` | JSON snapshot: `generated_at`, `counts`, `running`, `retrying`, `codex_totals` (subset), `rate_limits` (empty object). |
| `POST` | `/api/v1/refresh` | `202` — wakes the poll loop so the next orchestrator tick runs without waiting the full `polling.interval_ms`. |
| `GET` | `/api/v1/{issue_identifier}` | Single-issue aggregate; `404` with `{ "error": { "code", "message" } }` if not running or retrying. |
| `GET` | `/` | Minimal HTML dashboard (only if `observability.web_dashboard_enabled` is true). |

**Stale snapshots:** the orchestrator publishes into a shared `RwLock<OrchestratorSnapshot>` after dispatch is scheduled (issues appear in `running`) and again after the tick finishes. While Codex work is in flight inside a tick, HTTP readers see the last published copy; `turns_completed` and similar fields may lag until the tick completes. This avoids holding a lock across long-running agent I/O.

**JSON parity vs Elixir:** running rows use Rust field names (e.g. `issue_state` instead of Elixir’s `state`); per-issue Codex session telemetry (`session_id`, token breakdown per turn, `last_event`) is not wired in this baseline. `codex_totals` exposes aggregate `total_tokens` with other counters zero-filled until deeper instrumentation exists.

## Observability (TUI)

When enabled, the terminal dashboard renders a compact status view inspired by the Elixir dashboard:

- Header metrics (`SYMPHONY STATUS`, agents, runtime, token totals, project URL, next refresh)
- Running workers table (`ID`, `STAGE`, `PID`, `AGE / TURN`, `TOKENS`, `SESSION`, `EVENT`)
- Backoff queue section

Controls:

- `q` — quit
- `Ctrl+C` — quit

---

## Runtime Architecture

High-level flow:

1. CLI loads workflow and settings.
2. Orchestrator runs a tick:
   - reload workflow if changed
   - reconcile due retries (persists preferred `worker_host` metadata for the next dispatch when a timer fires)
   - fetch candidate issues from tracker
   - sort candidates by priority/creation/identifier
   - dispatch eligible issues subject to **global** `agent.max_concurrent_agents`, **per-state** `agent.max_concurrent_agents_by_state`, and **per-SSH-host** `worker.max_concurrent_agents_per_host` (when `worker.ssh_hosts` is configured)
3. Issue run:
   - create issue workspace
   - run lifecycle hooks
   - render prompt with `issue` + `attempt`
   - spawn Codex app-server via `bash -lc <codex.command>`
   - process turn completion/failure and usage signals
   - update token totals, schedule retries

---

## Safety and Isolation

Workspace safety primitives (`symphony-workspace`):

- Issue identifiers are sanitized to `[A-Za-z0-9._-]` (others become `_`).
- Per-issue workspace path is derived under configured workspace root.
- Hook execution has timeout control and fatal/non-fatal behavior.

Hook behavior baseline:

- Fatal on selected phases (e.g. `after_create`, `before_run`) when configured as such.
- Non-fatal hook phases are logged/ignored by orchestrator logic.

---

## Tracker Integration

### Tracker Trait (`symphony-tracker`)

Defines required tracker operations:

- `fetch_candidate_issues`
- `fetch_issues_by_states`
- `fetch_issue_states_by_ids`

### Linear Adapter (`symphony-tracker-linear`)

The adapter uses GraphQL over HTTP and normalizes fields into `Issue` / `IssueState`.

Behavior aligned with the Elixir reference:

- **Paged polling:** candidate issues are fetched with a workflow-state filter (`tracker.active_states`) and cursor pagination; terminal cleanup uses `fetch_issues_by_states` with the same paged query shape **without** assignee filtering.
- **Batched id lookups:** `fetch_issue_states_by_ids` uses targeted `issues(filter: { id: { in: $ids } })` queries in batches of 50, preserving the caller’s id order in the result (matching Elixir batching).
- **Assignee routing:** when `tracker.assignee` is set, issues include Linear `assignee { id }`; `assigned_to_worker` is true only when that id matches the configured filter (or the resolved viewer id for `me`).
- **Auth fallback:** requests send `Authorization: <token>` first and retry with `Bearer <token>` on `401`, consistent with previous Rust behavior.

Recommended environment variables for workflow front matter:

- `LINEAR_ENDPOINT`
- `LINEAR_TOKEN`
- `LINEAR_PROJECT` (optional)
- `LINEAR_ASSIGNEE` (optional; use via `tracker.assignee: $LINEAR_ASSIGNEE` in YAML front matter if desired)

---

## Codex App-Server Integration

`symphony-codex` currently provides:

- Child process management using `tokio::process`
- Initialization + thread/turn start message dispatch including `approvalPolicy`, `sandbox` / `sandboxPolicy`, and absolute workspace `cwd` (Codex path)
- Newline-oriented JSON message stream handling
- Graceful tolerance for malformed/non-JSON output lines
- Extraction of:
  - completion status
  - `threadId`, `turnId`
  - usage token payloads when present
- Stall timeout boundary

### Runtime Interface Selection

Symphony supports two runtime interfaces:

- `runtime.interface: codex` (default): existing Codex app-server JSON-RPC path.
- `runtime.interface: cursor_cli`: an in-process translation app-server path that keeps the same
  Symphony-side contract and bridges to Cursor CLI stream-json output.

Example front matter:

```yaml
runtime:
  interface: cursor_cli
codex:
  command: cursor-agent -p --output-format stream-json
```

### Optional Extension: `linear_graphql` Client-Side Tool

This extension is now implemented in the Rust runtime.

Behavior:

- Advertised to app-server sessions during `thread/start` when:
  - `tracker.kind == "linear"`
  - `tracker.endpoint` and `tracker.token` are configured
- Handles `item/tool/call` requests for `linear_graphql` without stalling the session
- Accepts input as:
  - raw query string, or
  - object `{ "query": "...", "variables": { ... } }`
- Enforces:
  - non-empty query string
  - `variables` must be an object when present
  - exactly one GraphQL operation per call
- Executes against configured Linear endpoint/auth (agent never needs to read token files)
- Returns structured tool result with:
  - `success=true` for GraphQL responses without top-level `errors`
  - `success=false` for invalid input, transport failures, unsupported tools, or GraphQL `errors`
  - JSON payload included in `output` and `contentItems` for in-session inspection

---

## Retry and Scheduling Semantics

Core orchestrator includes:

- Candidate sorting by:
  1. `priority` ascending
  2. `created_at` oldest first
  3. `identifier`
- Claim/running sets to avoid duplicate dispatch in a tick
- Continuation retry scheduling (short delay)
- Failure retry scheduling (exponential delay with cap)
- Retry tokening baseline to mitigate stale retry consumption
- Aggregate token accounting from turn outcomes

These semantics are still being tightened toward full parity with `SPEC.md` and Elixir behavior.

---

## Logging and Diagnostics

The CLI initializes `tracing` with env-filter support.

Typical usage:

```bash
RUST_LOG=info cargo run -p symphony-cli -- ../WORKFLOW.md
```

---

## Testing

Run all tests:

```bash
cd symphony/rust
cargo test
```

Current tests cover:

- Workflow strict template behavior
- Workflow invalid reload fallback
- Config workflow path precedence
- Workspace sanitization and creation behavior
- Candidate sort behavior
- Codex message parsing behavior

---

## Conformance Notes

This Rust workspace is intended to align with the Symphony spec in [`../SPEC.md`](../SPEC.md), with current effort focused on core orchestration behavior.

When evaluating conformance, treat this implementation as:

- A functional, test-backed core baseline
- Not yet a complete replacement for every behavior from the Elixir reference
- Ready for incremental hardening toward full acceptance coverage

---

## Common Commands

Build all crates:

```bash
cd symphony/rust
cargo build
```

Run tests:

```bash
cd symphony/rust
cargo test
```

Run CLI with default `WORKFLOW.md`:

```bash
cd symphony/rust
cargo run -p symphony-cli
```

Run CLI with explicit path:

```bash
cd symphony/rust
cargo run -p symphony-cli -- /absolute/path/to/WORKFLOW.md
```

---

## Contribution Guidance

When adding features, prefer to keep boundaries clean:

- Domain/scheduler logic in `symphony-core`
- External system protocol logic in dedicated adapter crates
- Config/workflow parsing behavior isolated in their own crates
- Add tests for each behavior introduced, especially scheduler and protocol transitions

If adding spec-sensitive behavior, include:

- A unit or integration test asserting expected behavior
- Documentation update in this README and/or crate docs

