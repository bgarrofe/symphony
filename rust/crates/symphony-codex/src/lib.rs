use graphql_parser::parse_query;
use graphql_parser::query::Document;
use reqwest::Client;
use reqwest::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::{Duration, Instant, timeout};
use tracing::{debug, info, warn};

#[derive(Debug, Error)]
pub enum CodexError {
    #[error("spawn failed: {0}")]
    Spawn(std::io::Error),
    #[error("missing stdio handle")]
    MissingIo,
    #[error("json io error: {0}")]
    Io(std::io::Error),
    #[error("json serialization error: {0}")]
    Serialize(serde_json::Error),
    #[error("request timed out")]
    Timeout,
    #[error("stalled waiting for app-server events")]
    StallTimeout,
    #[error("http error: {0}")]
    Http(reqwest::Error),
    #[error("protocol response error: {0}")]
    ResponseError(String),
    #[error("turn requires user input")]
    TurnInputRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnOutcome {
    pub status: String,
    pub usage: Option<Usage>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DynamicToolContext {
    pub linear_graphql: Option<LinearGraphqlTool>,
}

#[derive(Debug, Clone)]
pub struct LinearGraphqlTool {
    endpoint: String,
    token: String,
    client: Client,
}

impl LinearGraphqlTool {
    pub fn new(endpoint: String, token: String) -> Self {
        Self {
            endpoint,
            token,
            client: Client::new(),
        }
    }

    fn spec() -> Value {
        json!({
            "name": "linear_graphql",
            "description": "Execute a raw GraphQL query or mutation against Linear using Symphony configured auth.",
            "inputSchema": {
              "type": "object",
              "additionalProperties": false,
              "required": ["query"],
              "properties": {
                "query": {"type":"string", "description":"Single GraphQL query or mutation document"},
                "variables": {"type":["object","null"], "additionalProperties": true}
              }
            }
        })
    }

    async fn execute(&self, arguments: Value) -> Value {
        match normalize_linear_graphql_arguments(arguments) {
            Ok((query, variables)) => {
                if !contains_exactly_one_operation(&query) {
                    return failure_payload("`linear_graphql.query` must contain exactly one GraphQL operation.");
                }
                match self
                    .send_graphql_with_auth_fallback(json!({"query": query, "variables": variables}))
                    .await
                {
                    Ok(resp) => match resp.json::<Value>().await {
                        Ok(body) => {
                            let has_errors = body
                                .get("errors")
                                .and_then(Value::as_array)
                                .map(|arr| !arr.is_empty())
                                .unwrap_or(false);
                            dynamic_tool_response(!has_errors, body)
                        }
                        Err(err) => failure_payload(&format!("failed to decode Linear GraphQL response: {err}")),
                    },
                    Err(err) => failure_payload(&format!(
                        "Linear GraphQL transport failure: {err}"
                    )),
                }
            }
            Err(msg) => failure_payload(&msg),
        }
    }

    async fn send_graphql_with_auth_fallback(
        &self,
        payload: Value,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let token = self.token.trim();
        let first = self
            .client
            .post(&self.endpoint)
            .header(AUTHORIZATION, token)
            .json(&payload)
            .send()
            .await?;
        if first.status() != reqwest::StatusCode::UNAUTHORIZED || token.starts_with("Bearer ") {
            return Ok(first);
        }
        self.client
            .post(&self.endpoint)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .json(&payload)
            .send()
            .await
    }
}

pub struct CodexClient {
    child: Child,
}

pub struct CursorCliClient {
    child: Child,
}

impl CodexClient {
    pub async fn spawn(command: &str, cwd: &std::path::Path) -> Result<Self, CodexError> {
        let child = Command::new("bash")
            .arg("-lc")
            .arg(command)
            .current_dir(cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(CodexError::Spawn)?;
        Ok(Self { child })
    }

    pub async fn initialize(
        &mut self,
        input_prompt: &str,
        turn_timeout_ms: u64,
        stall_timeout_ms: u64,
        tool_context: DynamicToolContext,
        detailed_app_server_logs: bool,
    ) -> Result<TurnOutcome, CodexError> {
        let mut stdin = self.child.stdin.take().ok_or(CodexError::MissingIo)?;
        info!("codex initialize: sending initialize request");
        let init = serde_json::json!({
          "jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "clientInfo": {
              "name": "symphony-orchestrator",
              "title": "Symphony Orchestrator",
              "version": "0.1.0"
            },
            "capabilities": {
              "experimentalApi": true
            }
          }
        });
        let initialized = serde_json::json!({"jsonrpc":"2.0","method":"initialized","params":{}});
        let thread_start = serde_json::json!({
          "jsonrpc":"2.0","id":2,"method":"thread/start","params":{
            "cwd":".",
            "dynamicTools": dynamic_tool_specs(&tool_context)
          }
        });
        for payload in [init, initialized, thread_start] {
            let serialized = serde_json::to_string(&payload).map_err(CodexError::Serialize)?;
            stdin
                .write_all(format!("{serialized}\n").as_bytes())
                .await
                .map_err(CodexError::Io)?;
        }
        info!("codex initialize: waiting for thread/start response");

        let stdout = self.child.stdout.take().ok_or(CodexError::MissingIo)?;
        let mut reader = BufReader::new(stdout).lines();
        let mut outcome = TurnOutcome {
            status: "unknown".to_string(),
            usage: None,
            thread_id: None,
            turn_id: None,
        };
        timeout(Duration::from_millis(turn_timeout_ms), async {
            let stall = Duration::from_millis(stall_timeout_ms);
            let started = Instant::now();
            let mut saw_turn_start = false;
            loop {
                let next = timeout(stall, reader.next_line())
                    .await
                    .map_err(|_| CodexError::StallTimeout)?
                    .map_err(CodexError::Io)?;
                let Some(line) = next else {
                    return Err(CodexError::ResponseError(
                        "codex stream closed before turn terminal event".to_string(),
                    ));
                };
                if line.trim().is_empty() {
                    continue;
                }
                debug!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "codex stream message received"
                );
                // Non-JSON lines can appear in mixed stdout streams; ignore them.
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    debug!("codex stream non-json line ignored");
                    continue;
                };
                if detailed_app_server_logs {
                    let raw = serde_json::to_string(&value).unwrap_or_else(|_| line.clone());
                    let preview: String = raw.chars().take(2000).collect();
                    info!(message=%preview, "codex protocol: inbound app-server message");
                }
                if let Some(method) = value.get("method").and_then(Value::as_str) {
                    debug!(method=%method, "codex protocol method");
                } else if let Some(id) = value.get("id") {
                    debug!(id=%id, "codex protocol response");
                }
                if let Some(thread_id) = extract_thread_start_thread_id(&value) {
                    info!(thread_id=%thread_id, "codex initialize: thread started");
                    outcome.thread_id = Some(thread_id.clone());
                    let turn_start = serde_json::json!({
                      "jsonrpc":"2.0","id":3,"method":"turn/start","params":{
                        "threadId": thread_id,
                        "input":[{"type":"text","text":input_prompt}],
                        "cwd": "."
                      }
                    });
                    let serialized =
                        serde_json::to_string(&turn_start).map_err(CodexError::Serialize)?;
                    stdin
                        .write_all(format!("{serialized}\n").as_bytes())
                        .await
                        .map_err(CodexError::Io)?;
                    info!("codex initialize: turn/start sent");
                    saw_turn_start = true;
                    continue;
                }
                if let Some((approval_id, decision)) = maybe_auto_approval_request(&value) {
                    let response = json!({"id": approval_id.clone(), "result": {"decision": decision}});
                    let serialized =
                        serde_json::to_string(&response).map_err(CodexError::Serialize)?;
                    stdin
                        .write_all(format!("{serialized}\n").as_bytes())
                        .await
                        .map_err(CodexError::Io)?;
                    debug!(id=%approval_id, decision=%decision, "codex approval auto-approved");
                    continue;
                }
                if let Some(tool_call_id) = maybe_tool_call_id(&value) {
                    debug!(tool_call_id=%tool_call_id, "codex tool call requested");
                    let result = match extract_tool_call(&value) {
                        Some((tool_name, arguments)) => {
                            execute_tool_call(&tool_context, &tool_name, arguments).await
                        }
                        None => failure_payload("invalid tool call payload"),
                    };
                    let response = json!({"id": tool_call_id.clone(), "result": result});
                    let serialized =
                        serde_json::to_string(&response).map_err(CodexError::Serialize)?;
                    stdin
                        .write_all(format!("{serialized}\n").as_bytes())
                        .await
                        .map_err(CodexError::Io)?;
                    debug!(tool_call_id=%tool_call_id, "codex tool call result sent");
                    continue;
                }
                if let Some(err) = maybe_protocol_error(&value) {
                    warn!(error=%err, "codex protocol error");
                    return Err(CodexError::ResponseError(err));
                }
                if indicates_input_required(&value) {
                    warn!("codex reported input required");
                    return Err(CodexError::TurnInputRequired);
                }
                if apply_server_message(&value, &mut outcome) {
                    info!(status=%outcome.status, "codex turn terminal event received");
                    break;
                }
            }
            if !saw_turn_start {
                return Err(CodexError::ResponseError(
                    "turn/start was never accepted by app-server".to_string(),
                ));
            }
            Ok::<(), CodexError>(())
        })
        .await
        .map_err(|_| CodexError::Timeout)??;
        Ok(outcome)
    }

    pub async fn kill(&mut self) -> Result<(), CodexError> {
        self.child.kill().await.map_err(CodexError::Io)
    }
}

impl CursorCliClient {
    pub async fn spawn(
        command: &str,
        prompt: &str,
        cwd: &std::path::Path,
    ) -> Result<Self, CodexError> {
        let full_command = format!("{command} {}", shell_quote_single(prompt));
        let log_command = format!("{command} <WORKFLOW.md>");
        info!(command=%log_command, cwd=%cwd.display(), "cursor translation: launching cursor cli command");
        let child = Command::new("bash")
            .arg("-lc")
            .arg(&full_command)
            .current_dir(cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(CodexError::Spawn)?;
        Ok(Self { child })
    }

    pub async fn initialize(
        &mut self,
        _input_prompt: &str,
        turn_timeout_ms: u64,
        stall_timeout_ms: u64,
        tool_context: DynamicToolContext,
        detailed_app_server_logs: bool,
    ) -> Result<TurnOutcome, CodexError> {
        let mut stdin = self.child.stdin.take().ok_or(CodexError::MissingIo)?;
        if let Some(stderr) = self.child.stderr.take() {
            tokio::spawn(async move {
                let mut stderr_lines = BufReader::new(stderr).lines();
                loop {
                    match stderr_lines.next_line().await {
                        Ok(Some(line)) => {
                            if !line.trim().is_empty() {
                                info!(line=%line, "cursor translation: child stderr");
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            info!(error=%err, "cursor translation: stderr read failed");
                            break;
                        }
                    }
                }
            });
        }
        // Prompt is provided as a launch argument. stdin remains available for
        // interactive protocol replies (tool results/approvals) if requested.
        info!("cursor translation: outbound turn/start (prompt provided as launch argument)");

        let stdout = self.child.stdout.take().ok_or(CodexError::MissingIo)?;
        let mut reader = BufReader::new(stdout).lines();
        let mut outcome = TurnOutcome {
            status: "unknown".to_string(),
            usage: None,
            thread_id: None,
            turn_id: None,
        };

        timeout(Duration::from_millis(turn_timeout_ms), async {
            let stall = Duration::from_millis(stall_timeout_ms);
            let started = Instant::now();
            let mut saw_any_event = false;
            loop {
                let next = match timeout(stall, reader.next_line()).await {
                    Ok(result) => result.map_err(CodexError::Io)?,
                    Err(_) => {
                        info!(
                            stall_timeout_ms,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "cursor translation: stall timeout waiting for stdout"
                        );
                        return Err(CodexError::StallTimeout);
                    }
                };
                let Some(line) = next else {
                    info!("cursor translation: stream reached EOF");
                    break;
                };
                if line.trim().is_empty() {
                    continue;
                }
                saw_any_event = true;
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    if detailed_app_server_logs {
                        let preview: String = line.chars().take(240).collect();
                        info!(line=%preview, "cursor translation: inbound non-json line");
                    } else {
                        info!("cursor translation: inbound non-json line");
                    }
                    continue;
                };
                if detailed_app_server_logs {
                    let raw_message = serde_json::to_string(&value).unwrap_or_else(|_| line.clone());
                    let raw_preview: String = raw_message.chars().take(2000).collect();
                    info!(message=%raw_preview, "cursor translation: inbound app-server message");
                }
                if let Some(method) = value.get("method").and_then(Value::as_str) {
                    info!(method=%method, "cursor translation: inbound rpc message");
                } else if let Some(event_type) = value.get("type").and_then(Value::as_str) {
                    info!(event_type=%event_type, "cursor translation: inbound stream event");
                } else if let Some(id) = value.get("id") {
                    info!(id=%id, "cursor translation: inbound rpc response");
                }
                debug!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "cursor stream message received"
                );

                if let Some(tool_call_id) = maybe_tool_call_id(&value) {
                    let result = match extract_tool_call(&value) {
                        Some((tool_name, arguments)) => {
                            execute_tool_call(&tool_context, &tool_name, arguments).await
                        }
                        None => failure_payload("invalid tool call payload"),
                    };
                    let response = json!({"id": tool_call_id.clone(), "result": result});
                    let serialized =
                        serde_json::to_string(&response).map_err(CodexError::Serialize)?;
                    stdin
                        .write_all(format!("{serialized}\n").as_bytes())
                        .await
                        .map_err(CodexError::Io)?;
                    info!(id=%tool_call_id, "cursor translation: outbound tool call result");
                    continue;
                }
                if let Some((approval_id, decision)) = maybe_auto_approval_request(&value) {
                    let response = json!({"id": approval_id.clone(), "result": {"decision": decision}});
                    let serialized =
                        serde_json::to_string(&response).map_err(CodexError::Serialize)?;
                    stdin
                        .write_all(format!("{serialized}\n").as_bytes())
                        .await
                        .map_err(CodexError::Io)?;
                    info!(id=%approval_id, decision=%decision, "cursor translation: outbound approval decision");
                    continue;
                }
                if let Some(err) = maybe_protocol_error(&value) {
                    return Err(CodexError::ResponseError(err));
                }

                let previous_status = outcome.status.clone();
                apply_cursor_translated_message(&value, &mut outcome);
                if previous_status != outcome.status {
                    info!(status=%outcome.status, "cursor translation: mapped turn status updated");
                }
            }
            if !saw_any_event {
                return Err(CodexError::ResponseError(
                    "cursor stream closed without events".to_string(),
                ));
            }
            if outcome.status == "unknown" || outcome.status == "in_progress" {
                outcome.status = "completed".to_string();
                info!(status=%outcome.status, "cursor translation: synthesized terminal status");
            }
            match self.child.try_wait() {
                Ok(Some(status)) => info!(exit_status=%status, "cursor translation: child process exited"),
                Ok(None) => info!("cursor translation: child process still running after stream"),
                Err(err) => info!(error=%err, "cursor translation: failed to query child exit status"),
            }
            Ok::<(), CodexError>(())
        })
        .await
        .map_err(|_| {
            info!(
                turn_timeout_ms,
                "cursor translation: total turn timeout elapsed"
            );
            CodexError::Timeout
        })??;

        Ok(outcome)
    }

    pub async fn kill(&mut self) -> Result<(), CodexError> {
        self.child.kill().await.map_err(CodexError::Io)
    }
}

fn dynamic_tool_specs(tool_context: &DynamicToolContext) -> Vec<Value> {
    let mut specs = Vec::new();
    if tool_context.linear_graphql.is_some() {
        specs.push(LinearGraphqlTool::spec());
    }
    specs
}

fn shell_quote_single(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn maybe_tool_call_id(value: &Value) -> Option<Value> {
    if value.get("method").and_then(Value::as_str) != Some("item/tool/call") {
        return None;
    }
    value.get("id").cloned()
}

fn maybe_protocol_error(value: &Value) -> Option<String> {
    let err = value.get("error")?;
    let id = value.get("id").cloned().unwrap_or(Value::Null);
    Some(format!(
        "id={id} error={}",
        serde_json::to_string(err).unwrap_or_else(|_| err.to_string())
    ))
}

fn maybe_auto_approval_request(value: &Value) -> Option<(Value, &'static str)> {
    let method = value.get("method").and_then(Value::as_str)?;
    let id = value.get("id")?.clone();
    let decision = match method {
        "item/commandExecution/requestApproval" => "acceptForSession",
        "item/fileChange/requestApproval" => "acceptForSession",
        "execCommandApproval" => "approved_for_session",
        "applyPatchApproval" => "approved_for_session",
        _ => return None,
    };
    Some((id, decision))
}

fn extract_thread_start_thread_id(value: &Value) -> Option<String> {
    if value.get("id").and_then(Value::as_i64) != Some(2) {
        return None;
    }
    value
        .get("result")
        .and_then(|r| r.get("thread"))
        .and_then(|t| t.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn indicates_input_required(value: &Value) -> bool {
    if let Some(method) = value.get("method").and_then(Value::as_str) {
        return method.contains("input")
            || method.eq_ignore_ascii_case("approval_required");
    }
    false
}

fn extract_tool_call(value: &Value) -> Option<(String, Value)> {
    let params = value.get("params")?;
    let tool_name = params
        .get("toolName")
        .or_else(|| params.get("name"))
        .and_then(Value::as_str)?
        .to_string();
    let arguments = params
        .get("arguments")
        .or_else(|| params.get("input"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    Some((tool_name, arguments))
}

async fn execute_tool_call(tool_context: &DynamicToolContext, tool_name: &str, arguments: Value) -> Value {
    match tool_name {
        "linear_graphql" => match &tool_context.linear_graphql {
            Some(tool) => tool.execute(arguments).await,
            None => failure_payload("`linear_graphql` requires tracker.kind=linear with valid auth."),
        },
        _ => failure_payload(&format!("Unsupported dynamic tool: {tool_name}")),
    }
}

fn normalize_linear_graphql_arguments(arguments: Value) -> Result<(String, Value), String> {
    match arguments {
        Value::String(query) => {
            let query = query.trim().to_string();
            if query.is_empty() {
                return Err("`linear_graphql` requires a non-empty `query` string.".to_string());
            }
            Ok((query, json!({})))
        }
        Value::Object(map) => {
            let query = map
                .get("query")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`linear_graphql` requires a non-empty `query` string.".to_string())?
                .to_string();
            let variables = map.get("variables").cloned().unwrap_or_else(|| json!({}));
            if !variables.is_object() && !variables.is_null() {
                return Err("`linear_graphql.variables` must be a JSON object when provided.".to_string());
            }
            Ok((query, if variables.is_null() { json!({}) } else { variables }))
        }
        _ => Err(
            "`linear_graphql` expects a query string or an object with `query` and optional `variables`."
                .to_string(),
        ),
    }
}

fn contains_exactly_one_operation(query: &str) -> bool {
    let parsed: Result<Document<'_, String>, _> = parse_query(query);
    parsed.map(|doc| doc.definitions.len() == 1).unwrap_or(false)
}

fn dynamic_tool_response(success: bool, payload: Value) -> Value {
    let text = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string());
    json!({
      "success": success,
      "output": text,
      "contentItems": [{"type":"inputText","text": text}]
    })
}

fn failure_payload(message: &str) -> Value {
    dynamic_tool_response(false, json!({"error":{"message": message}}))
}

fn apply_server_message(value: &Value, outcome: &mut TurnOutcome) -> bool {
    if let Some(params) = value.get("params") {
        maybe_extract_ids(params, outcome);
        maybe_extract_usage(params, outcome);
    }
    if let Some(result) = value.get("result") {
        maybe_extract_ids(result, outcome);
        maybe_extract_usage(result, outcome);
    }
    match value.get("method").and_then(|m| m.as_str()) {
        Some("turn/completed") => {
            outcome.status = "completed".into();
            true
        }
        Some("turn/failed") | Some("turn/cancelled") => {
            outcome.status = "failed".into();
            true
        }
        _ => false,
    }
}

fn apply_cursor_translated_message(value: &Value, outcome: &mut TurnOutcome) {
    if let Some(params) = value.get("params") {
        maybe_extract_ids(params, outcome);
        maybe_extract_usage(params, outcome);
    }
    if let Some(result) = value.get("result") {
        maybe_extract_ids(result, outcome);
        maybe_extract_usage(result, outcome);
    }
    match value.get("type").and_then(Value::as_str) {
        Some("error") => outcome.status = "failed".to_string(),
        Some("result") => {
            let is_error = value
                .get("is_error")
                .or_else(|| value.get("isError"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            outcome.status = if is_error {
                "failed".to_string()
            } else {
                "completed".to_string()
            };
            maybe_extract_usage(value, outcome);
        }
        Some("assistant")
        | Some("tool_call")
        | Some("tool_result")
        | Some("turn.started")
        | Some("turn.delta")
        | Some("turn.tool_call")
        | Some("turn.tool_result") => {
            if outcome.status == "unknown" {
                outcome.status = "in_progress".to_string();
            }
        }
        Some("turn.completed") => outcome.status = "completed".to_string(),
        _ => {
            if apply_server_message(value, outcome) {
                return;
            }
        }
    }
}

fn maybe_extract_ids(src: &Value, outcome: &mut TurnOutcome) {
    if let Some(thread_id) = src.get("threadId").and_then(|v| v.as_str()) {
        outcome.thread_id = Some(thread_id.to_string());
    }
    if let Some(thread_id) = src
        .get("thread")
        .and_then(|v| v.get("id"))
        .and_then(Value::as_str)
    {
        outcome.thread_id = Some(thread_id.to_string());
    }
    if let Some(turn_id) = src.get("turnId").and_then(|v| v.as_str()) {
        outcome.turn_id = Some(turn_id.to_string());
    }
    if let Some(turn_id) = src
        .get("turn")
        .and_then(|v| v.get("id"))
        .and_then(Value::as_str)
    {
        outcome.turn_id = Some(turn_id.to_string());
    }
}

fn maybe_extract_usage(src: &Value, outcome: &mut TurnOutcome) {
    let Some(usage) = src.get("usage").or_else(|| src.get("total_token_usage")) else {
        return;
    };
    let input_tokens = usage
        .get("input_tokens")
        .or_else(|| usage.get("inputTokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output_tokens = usage
        .get("output_tokens")
        .or_else(|| usage.get("outputTokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let total_tokens = usage
        .get("total_tokens")
        .or_else(|| usage.get("totalTokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(input_tokens.saturating_add(output_tokens));
    outcome.usage = Some(Usage {
        input_tokens,
        output_tokens,
        total_tokens,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_message_extracts_ids_usage_and_completion() {
        let mut outcome = TurnOutcome {
            status: "unknown".into(),
            usage: None,
            thread_id: None,
            turn_id: None,
        };
        let msg = serde_json::json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thr_1",
                "turnId": "turn_1",
                "usage": {"inputTokens": 10, "outputTokens": 3, "totalTokens": 13}
            }
        });
        assert!(apply_server_message(&msg, &mut outcome));
        assert_eq!(outcome.status, "completed");
        assert_eq!(outcome.thread_id.as_deref(), Some("thr_1"));
        assert_eq!(outcome.turn_id.as_deref(), Some("turn_1"));
        assert_eq!(outcome.usage.as_ref().map(|u| u.total_tokens), Some(13));
    }

    #[test]
    fn linear_graphql_argument_validation() {
        assert!(normalize_linear_graphql_arguments(json!({"query":"query { viewer { id } }"})).is_ok());
        assert!(normalize_linear_graphql_arguments(json!({"query":"","variables":{}})).is_err());
        assert!(normalize_linear_graphql_arguments(json!({"query":"query { x }","variables":"bad"})).is_err());
    }

    #[test]
    fn rejects_multiple_graphql_operations() {
        let query = "query A { viewer { id } } query B { viewer { name } }";
        assert!(!contains_exactly_one_operation(query));
    }

    #[test]
    fn cursor_message_marks_in_progress_for_assistant_events() {
        let mut outcome = TurnOutcome {
            status: "unknown".into(),
            usage: None,
            thread_id: None,
            turn_id: None,
        };
        let msg = serde_json::json!({
            "type": "assistant",
            "message": { "content": [{ "type": "text", "text": "hello" }] }
        });
        apply_cursor_translated_message(&msg, &mut outcome);
        assert_eq!(outcome.status, "in_progress");
    }

    #[test]
    fn cursor_message_terminal_completed() {
        let mut outcome = TurnOutcome {
            status: "unknown".into(),
            usage: None,
            thread_id: None,
            turn_id: None,
        };
        let msg = serde_json::json!({
            "type": "turn.completed"
        });
        apply_cursor_translated_message(&msg, &mut outcome);
        assert_eq!(outcome.status, "completed");
    }
}
