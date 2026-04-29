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
- Core orchestrator dispatch/retry/token bookkeeping baseline
- Unit tests covering major primitives

Not yet complete:

- Full one-to-one parity with all Elixir runtime semantics
- Full acceptance test matrix from `SPEC.md`
- Optional extensions (HTTP observability API, dynamic tools, SSH worker mode)

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
```

If no argument is provided, workflow path resolution defaults to:

- `./WORKFLOW.md` (relative to process working directory)

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
- `codex.command` (non-empty)
- `codex.turn_timeout_ms` (total turn stream timeout, default `900000`)
- `codex.stall_timeout_ms`
- `tracker.kind`
- `tracker.endpoint` / `tracker.token` / `tracker.project`

Environment indirection is supported for selected string fields via `$VAR_NAME`.

---

## Runtime Architecture

High-level flow:

1. CLI loads workflow and settings.
2. Orchestrator runs a tick:
   - reload workflow if changed
   - reconcile due retries
   - fetch candidate issues from tracker
   - sort candidates by priority/creation/identifier
   - dispatch eligible issues
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

Current adapter uses GraphQL over HTTP and normalizes fields into `Issue` / `IssueState`.

Current recommended environment variables for workflow front matter:

- `LINEAR_ENDPOINT`
- `LINEAR_TOKEN`
- `LINEAR_PROJECT` (optional)

---

## Codex App-Server Integration

`symphony-codex` currently provides:

- Child process management using `tokio::process`
- Initialization + thread/turn start message dispatch
- Newline-oriented JSON message stream handling
- Graceful tolerance for malformed/non-JSON output lines
- Extraction of:
  - completion status
  - `threadId`, `turnId`
  - usage token payloads when present
- Stall timeout boundary

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

