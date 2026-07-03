use crate::adapters::anthropic_to_responses;
use crate::adapters::chat_completions;
use crate::adapters::chat_completions::ChatCompletionCollector;
use crate::adapters::chat_completions::ChatCompletionStreamConverter;
use crate::adapters::responses_to_anthropic::AnthropicStreamCollector;
use crate::adapters::responses_to_anthropic::AnthropicStreamConverter;
use crate::config::Config;
use crate::debug_ui::debug_index;
use crate::debug_ui::debug_ws;
use crate::engine::Gateway;
use crate::error::AppError;
use crate::error::AppResult;
use crate::models::anthropic::AnthropicRequest;
use crate::models::chat::ChatCompletionRequest;
use crate::models::responses::ResponsesRequest;
use crate::upstream::collect_models_response;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::extract::Query;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderName;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::Sse;
use axum::routing::MethodFilter;
use axum::routing::get;
use axum::routing::on;
use axum::routing::post;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

const API_LOG_BODY_LIMIT_BYTES: usize = 256 * 1024 * 1024;
const API_LOG_PAYLOAD_DUMP_LIMIT_BYTES: usize = 16 * 1024;
const API_LOG_PREVIEW_CHARS: usize = 160;
const UNKNOWN_MODEL_CREATED_AT: &str = "1970-01-01T00:00:00Z";

#[derive(Debug, Clone, Copy, Default)]
pub struct RouterOptions {
    pub with_debug_ui: bool,
}

pub fn build_router(gateway: Arc<Gateway>, options: RouterOptions) -> Router {
    let router = Router::new()
        .route("/v1/responses", post(post_responses))
        .route("/v1/messages", post(post_messages))
        .route("/v1/messages/count_tokens", post(post_count_tokens))
        .route("/v1/messages", on(MethodFilter::HEAD, probe_messages))
        .route("/v1/messages", on(MethodFilter::OPTIONS, probe_messages))
        .route("/v1/chat/completions", post(post_chat_completions))
        .route("/v1/completions", post(post_completions))
        .route("/v1/models", get(get_models))
        .route("/health", get(get_health))
        .route("/", get(get_root));

    let router = if options.with_debug_ui {
        router
            .route("/debug", get(debug_index))
            .route("/debug/ws", get(debug_ws))
    } else {
        router
    };

    router
        .fallback(api_not_found)
        .layer(middleware::from_fn(log_api_call))
        .with_state(gateway)
}

async fn log_api_call(request: Request, next: Next) -> Response {
    let api_call_id = format!("api_{}", Uuid::new_v4().simple());
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let started_at = Instant::now();

    let (parts, body) = request.into_parts();
    let body_bytes = match to_bytes(body, API_LOG_BODY_LIMIT_BYTES).await {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::warn!(
                api_call_id = %api_call_id,
                method = %method,
                path = %uri.path(),
                error = %err,
                "failed to read inbound API request body"
            );
            return (
                StatusCode::BAD_REQUEST,
                format!("failed to read request body: {err}"),
            )
                .into_response();
        }
    };

    let body_sha256 = hex::encode(Sha256::digest(&body_bytes));
    let body_summary = summarize_api_body(uri.path(), &body_bytes);
    tracing::info!(
        api_call_id = %api_call_id,
        method = %method,
        path = %uri.path(),
        query = uri.query().unwrap_or(""),
        content_type = %header_for_log(&headers, header::CONTENT_TYPE.as_str()),
        user_agent = %header_for_log(&headers, header::USER_AGENT.as_str()),
        anthropic_version = %header_for_log(&headers, "anthropic-version"),
        anthropic_beta = %header_for_log(&headers, "anthropic-beta"),
        openai_beta = %header_for_log(&headers, "openai-beta"),
        request_id = %header_for_log(&headers, "x-request-id"),
        authorization_present = headers.contains_key(header::AUTHORIZATION),
        x_api_key_present = headers.contains_key("x-api-key"),
        body_bytes = body_bytes.len(),
        body_sha256 = %body_sha256,
        body_summary = %body_summary,
        "inbound API request"
    );
    if body_bytes.len() <= API_LOG_PAYLOAD_DUMP_LIMIT_BYTES {
        tracing::info!(
            api_call_id = %api_call_id,
            method = %method,
            path = %uri.path(),
            body_payload = %payload_for_log(&body_bytes),
            "inbound API request payload"
        );
    }

    let request = Request::from_parts(parts, Body::from(body_bytes));
    let response = next.run(request).await;
    tracing::info!(
        api_call_id = %api_call_id,
        method = %method,
        path = %uri.path(),
        status = response.status().as_u16(),
        elapsed_ms = started_at.elapsed().as_millis(),
        "inbound API response prepared"
    );
    response
}

async fn api_not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}

fn probe_response(allow: &str) -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::ALLOW,
        HeaderValue::try_from(allow).expect("valid header value"),
    );
    response
}

async fn probe_messages() -> Response {
    probe_response("POST, HEAD, OPTIONS")
}

async fn get_health() -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({"status": "healthy"})),
    )
        .into_response()
}

async fn get_root() -> Response {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
}

fn header_for_log(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(compact_for_log)
        .unwrap_or_default()
}

fn summarize_api_body(path: &str, body: &Bytes) -> String {
    if body.is_empty() {
        return "empty".to_string();
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(value) => summarize_json_api_body(path, &value),
        Err(err) => {
            // Redact image URIs from the raw preview before logging (round-4 #2):
            // a non-JSON body could still embed a `data:`/signed image URL.
            let preview = crate::vision::redact_image_uris(&String::from_utf8_lossy(body));
            format!(
                "non_json parse_error={} preview={}",
                compact_for_log(&err.to_string()),
                compact_for_log(&preview)
            )
        }
    }
}

fn payload_for_log(body: &Bytes) -> String {
    match serde_json::from_slice::<Value>(body) {
        Ok(mut value) => {
            redact_payload_secrets(&mut value);
            // G4 round-4 #2: an inbound body under the dump limit would otherwise
            // log raw `data:` image bytes / signed `image_url`s. Strip image URIs
            // from every remaining string via the shared redactor BEFORE
            // serializing, so no logged surface carries request image content.
            crate::vision::redact_image_uris_in_value(&mut value);
            serde_json::to_string(&value)
                .unwrap_or_else(|_| "<failed to serialize json>".to_string())
        }
        Err(_) => {
            // Non-JSON body: still strip image URIs from the raw text so a
            // `data:`/signed URL in a malformed/odd payload is not logged raw.
            crate::vision::redact_image_uris(&String::from_utf8_lossy(body))
        }
    }
}

fn redact_payload_secrets(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if is_sensitive_payload_key(key) {
                    *value = Value::String("[redacted]".to_string());
                } else {
                    redact_payload_secrets(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_payload_secrets(value);
            }
        }
        _ => {}
    }
}

fn is_sensitive_payload_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', '_'], "");
    matches!(
        normalized.as_str(),
        "apikey"
            | "xapikey"
            | "authorization"
            | "password"
            | "passwd"
            | "secret"
            | "clientsecret"
            | "accesstoken"
            | "refreshtoken"
            | "authtoken"
            | "bearertoken"
    )
}

fn summarize_json_api_body(path: &str, value: &Value) -> String {
    let Some(map) = value.as_object() else {
        return format!("json_type={}", json_type(value));
    };

    let mut parts = Vec::new();
    parts.push(format!(
        "keys={}",
        summarized_list(map.keys().cloned().collect(), 24)
    ));
    append_common_json_fields(&mut parts, map);

    if path.contains("/messages") {
        append_anthropic_json_summary(&mut parts, map);
    } else if path.contains("/responses") {
        append_responses_json_summary(&mut parts, map);
    } else if path.contains("/chat/completions") || path.ends_with("/completions") {
        append_chat_json_summary(&mut parts, map);
    } else {
        append_generic_json_summary(&mut parts, map);
    }

    parts.join(" ")
}

fn append_common_json_fields(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    for key in [
        "model",
        "stream",
        "max_tokens",
        "max_output_tokens",
        "max_completion_tokens",
        "store",
        "parallel_tool_calls",
        "temperature",
        "top_p",
    ] {
        append_scalar_field(parts, map, key);
    }
    append_typed_field(parts, map, "tool_choice");
    append_typed_field(parts, map, "thinking");
    append_typed_field(parts, map, "reasoning");
}

fn append_anthropic_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    append_anthropic_system_summary(parts, map.get("system"));
    if let Some(messages) = map.get("messages").and_then(Value::as_array) {
        parts.push(format!("messages={}", messages.len()));
        append_anthropic_message_summary(parts, messages);
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
    append_metadata_summary(parts, map.get("metadata"));
    append_array_len(parts, map, "stop_sequences");
}

fn append_responses_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    if let Some(instructions) = map.get("instructions").and_then(Value::as_str) {
        parts.push(format!(
            "instructions_chars={}",
            instructions.chars().count()
        ));
    }
    match map.get("input") {
        Some(Value::String(text)) => {
            parts.push("input=string".to_string());
            parts.push(format!("input_chars={}", text.chars().count()));
        }
        Some(Value::Array(items)) => {
            parts.push(format!("input_items={}", items.len()));
            append_responses_input_summary(parts, items);
        }
        Some(other) => {
            parts.push(format!("input_type={}", json_type(other)));
        }
        None => {}
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
    append_array_len(parts, map, "include");
    append_metadata_summary(parts, map.get("metadata"));
}

fn append_chat_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    if let Some(messages) = map.get("messages").and_then(Value::as_array) {
        parts.push(format!("messages={}", messages.len()));
        append_chat_message_summary(parts, messages);
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
    append_typed_field(parts, map, "response_format");
    append_typed_field(parts, map, "stream_options");
}

fn append_generic_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    if let Some(messages) = map.get("messages").and_then(Value::as_array) {
        parts.push(format!("messages={}", messages.len()));
        append_chat_message_summary(parts, messages);
    }
    match map.get("input") {
        Some(Value::String(text)) => {
            parts.push("input=string".to_string());
            parts.push(format!("input_chars={}", text.chars().count()));
        }
        Some(Value::Array(items)) => {
            parts.push(format!("input_items={}", items.len()));
        }
        _ => {}
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
}

fn append_anthropic_system_summary(parts: &mut Vec<String>, system: Option<&Value>) {
    match system {
        Some(Value::String(text)) => {
            parts.push("system=string".to_string());
            parts.push(format!("system_chars={}", text.chars().count()));
        }
        Some(Value::Array(blocks)) => {
            let mut text_chars = 0usize;
            let mut counts = BTreeMap::new();
            for block in blocks {
                let kind = typed_json_value(block);
                increment_count(&mut counts, kind);
                if let Some(text) = block.get("text").and_then(Value::as_str) {
                    text_chars += text.chars().count();
                }
            }
            parts.push(format!("system_blocks={}", blocks.len()));
            parts.push(format!("system_chars={text_chars}"));
            push_counts(parts, "system_block_types", &counts);
        }
        Some(other) => {
            parts.push(format!("system_type={}", json_type(other)));
        }
        None => {}
    }
}

fn append_anthropic_message_summary(parts: &mut Vec<String>, messages: &[Value]) {
    let mut roles = Vec::new();
    let mut content_counts = BTreeMap::new();
    let mut text_chars = 0usize;

    for message in messages {
        roles.push(
            message
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        );
        if let Some(content) = message.get("content") {
            accumulate_anthropic_content(content, &mut text_chars, &mut content_counts);
        }
    }

    parts.push(format!("message_roles={}", summarized_list(roles, 16)));
    parts.push(format!("message_text_chars={text_chars}"));
    push_counts(parts, "message_content", &content_counts);
}

fn append_chat_message_summary(parts: &mut Vec<String>, messages: &[Value]) {
    let mut roles = Vec::new();
    let mut content_counts = BTreeMap::new();
    let mut text_chars = 0usize;
    let mut tool_calls = 0usize;

    for message in messages {
        roles.push(
            message
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        );
        if let Some(content) = message.get("content") {
            accumulate_chat_content(content, &mut text_chars, &mut content_counts);
        }
        tool_calls += message
            .get("tool_calls")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
    }

    parts.push(format!("message_roles={}", summarized_list(roles, 16)));
    parts.push(format!("message_text_chars={text_chars}"));
    parts.push(format!("message_tool_calls={tool_calls}"));
    push_counts(parts, "message_content", &content_counts);
}

fn append_responses_input_summary(parts: &mut Vec<String>, items: &[Value]) {
    let mut roles = Vec::new();
    let mut item_counts = BTreeMap::new();
    let mut text_chars = 0usize;

    for item in items {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_else(|| {
            if item.get("role").is_some() {
                "message"
            } else {
                "unknown"
            }
        });
        increment_count(&mut item_counts, item_type.to_string());
        if let Some(role) = item.get("role").and_then(Value::as_str) {
            roles.push(role.to_string());
        }
        if let Some(content) = item.get("content") {
            accumulate_responses_content(content, &mut text_chars);
        }
    }

    parts.push(format!("input_roles={}", summarized_list(roles, 16)));
    parts.push(format!("input_text_chars={text_chars}"));
    push_counts(parts, "input_item_types", &item_counts);
}

fn accumulate_anthropic_content(
    content: &Value,
    text_chars: &mut usize,
    counts: &mut BTreeMap<String, usize>,
) {
    match content {
        Value::String(text) => {
            *text_chars += text.chars().count();
            increment_count(counts, "string".to_string());
        }
        Value::Array(blocks) => {
            for block in blocks {
                let kind = typed_json_value(block);
                increment_count(counts, kind.clone());
                match kind.as_str() {
                    "text" => {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            *text_chars += text.chars().count();
                        }
                    }
                    "thinking" => {
                        if let Some(text) = block.get("thinking").and_then(Value::as_str) {
                            *text_chars += text.chars().count();
                        }
                    }
                    "tool_result" => {
                        if let Some(nested) = block.get("content") {
                            accumulate_anthropic_content(nested, text_chars, counts);
                        }
                    }
                    _ => {}
                }
            }
        }
        other => {
            increment_count(counts, json_type(other).to_string());
        }
    }
}

fn accumulate_chat_content(
    content: &Value,
    text_chars: &mut usize,
    counts: &mut BTreeMap<String, usize>,
) {
    match content {
        Value::String(text) => {
            *text_chars += text.chars().count();
            increment_count(counts, "string".to_string());
        }
        Value::Array(parts) => {
            for part in parts {
                let kind = typed_json_value(part);
                increment_count(counts, kind.clone());
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    *text_chars += text.chars().count();
                }
            }
        }
        other => {
            increment_count(counts, json_type(other).to_string());
        }
    }
}

fn accumulate_responses_content(content: &Value, text_chars: &mut usize) {
    match content {
        Value::String(text) => {
            *text_chars += text.chars().count();
        }
        Value::Array(parts) => {
            for part in parts {
                for key in ["text", "input_text", "output_text"] {
                    if let Some(text) = part.get(key).and_then(Value::as_str) {
                        *text_chars += text.chars().count();
                    }
                }
            }
        }
        _ => {}
    }
}

fn append_tool_summary(parts: &mut Vec<String>, label: &str, tools: &[Value]) {
    let names = tools
        .iter()
        .filter_map(tool_name_for_summary)
        .collect::<Vec<_>>();
    parts.push(format!("{label}={}", tools.len()));
    if !names.is_empty() {
        parts.push(format!("{label}_names={}", summarized_list(names, 12)));
    }
}

fn tool_name_for_summary(tool: &Value) -> Option<String> {
    tool.get("name")
        .and_then(Value::as_str)
        .or_else(|| {
            tool.get("function")
                .and_then(|function| function.get("name"))
                .and_then(Value::as_str)
        })
        .map(ToString::to_string)
}

fn append_metadata_summary(parts: &mut Vec<String>, metadata: Option<&Value>) {
    if let Some(Value::Object(map)) = metadata {
        parts.push(format!(
            "metadata_keys={}",
            summarized_list(map.keys().cloned().collect(), 16)
        ));
    }
}

fn append_array_len(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>, key: &str) {
    if let Some(values) = map.get(key).and_then(Value::as_array) {
        parts.push(format!("{key}={}", values.len()));
    }
}

fn append_scalar_field(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>, key: &str) {
    if let Some(value) = map.get(key).and_then(scalar_for_log) {
        parts.push(format!("{key}={value}"));
    }
}

fn append_typed_field(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>, key: &str) {
    if let Some(value) = map.get(key) {
        parts.push(format!("{key}={}", typed_json_value(value)));
    }
}

fn typed_json_value(value: &Value) -> String {
    match value {
        Value::Object(map) => map
            .get("type")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .unwrap_or_else(|| "object".to_string()),
        Value::Array(_) => "array".to_string(),
        Value::String(text) => compact_for_log(text),
        other => json_type(other).to_string(),
    }
}

fn scalar_for_log(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(compact_for_log(text)),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Null => Some("null".to_string()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn push_counts(parts: &mut Vec<String>, label: &str, counts: &BTreeMap<String, usize>) {
    if counts.is_empty() {
        return;
    }
    let values = counts
        .iter()
        .map(|(key, count)| format!("{key}:{count}"))
        .collect::<Vec<_>>();
    parts.push(format!("{label}={}", summarized_list(values, 16)));
}

fn increment_count(counts: &mut BTreeMap<String, usize>, key: String) {
    *counts.entry(key).or_default() += 1;
}

fn summarized_list(mut values: Vec<String>, max: usize) -> String {
    let total = values.len();
    values.truncate(max);
    if total > max {
        values.push(format!("+{}", total - max));
    }
    format!("[{}]", values.join(","))
}

fn compact_for_log(value: &str) -> String {
    let mut compact = String::new();
    for ch in value.chars().take(API_LOG_PREVIEW_CHARS) {
        if ch.is_control() {
            compact.push(' ');
        } else {
            compact.push(ch);
        }
    }
    if value.chars().count() > API_LOG_PREVIEW_CHARS {
        compact.push_str("...");
    }
    compact
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

async fn post_responses(
    State(gateway): State<Arc<Gateway>>,
    Json(request): Json<ResponsesRequest>,
) -> AppResult<Response> {
    let wants_stream = request.stream;
    let stream = gateway.stream_responses(request).await?;
    if wants_stream {
        Ok(stream_responses_response(stream))
    } else {
        collect_responses_response(stream).await
    }
}

async fn post_messages(
    State(gateway): State<Arc<Gateway>>,
    Json(request): Json<AnthropicRequest>,
) -> Response {
    match handle_post_messages(gateway, request).await {
        Ok(response) => response,
        Err(err) => anthropic_error_response(err),
    }
}

async fn post_chat_completions(
    State(gateway): State<Arc<Gateway>>,
    Json(request): Json<ChatCompletionRequest>,
) -> AppResult<Response> {
    let model = gateway.resolve_request_model(&request.model).await;
    let wants_stream = request.stream;
    let include_usage = request
        .stream_options
        .as_ref()
        .is_some_and(|options| options.include_usage);
    let responses_request = chat_completions::convert_request(request)?;
    let stream = gateway.stream_responses(responses_request).await?;

    if wants_stream {
        Ok(stream_chat_completions_response(
            model,
            include_usage,
            stream,
        ))
    } else {
        collect_chat_completions_response(model, stream).await
    }
}

async fn post_completions(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Response> {
    let response = gateway
        .upstream_client()
        .proxy_completions(headers, body)
        .await?;
    Ok(proxy_upstream_response(response))
}

async fn handle_post_messages(
    gateway: Arc<Gateway>,
    request: AnthropicRequest,
) -> AppResult<Response> {
    let model = gateway.resolve_request_model(&request.model).await;
    let wants_stream = request.stream;
    let responses_request = anthropic_to_responses::convert_request(request)?;
    let stream = gateway.stream_responses(responses_request).await?;

    if wants_stream {
        stream_anthropic_response(model, stream)
    } else {
        collect_anthropic_response(model, stream).await
    }
}

async fn post_count_tokens(State(gateway): State<Arc<Gateway>>, body: Bytes) -> Response {
    let request: AnthropicRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(err) => {
            return anthropic_error_response(AppError::bad_request(format!(
                "invalid request body: {err}"
            )));
        }
    };
    match handle_count_tokens(gateway, request).await {
        Ok(response) => response,
        Err(err) => anthropic_error_response(err),
    }
}

async fn handle_count_tokens(
    gateway: Arc<Gateway>,
    request: AnthropicRequest,
) -> AppResult<Response> {
    use crate::engine::TokenizeCapability;

    if gateway.tokenize_capability() == TokenizeCapability::Unsupported {
        return Err(AppError::not_found("upstream does not support /tokenize"));
    }

    let original_model = request.model.clone();
    let responses_request = anthropic_to_responses::convert_request(request)?;
    let resolved_model = gateway.resolve_request_model(&original_model).await;
    // Mirror the generation path: the configured system-prompt prefix is part
    // of the real upstream prompt, so it must be counted here too.
    let responses_request = gateway.apply_system_prompt_prefix(responses_request, &resolved_model);
    let lowered = crate::adapters::responses_to_chat::lower_request_with_options(
        &responses_request,
        Vec::new(),
        &gateway.config().default_reasoning_effort,
        false,
    )?;
    let body = serde_json::json!({
        "model": resolved_model,
        "messages": lowered.messages,
    });

    match gateway.upstream_client().count_tokens(&body).await {
        Ok(Some(count)) => {
            gateway.set_tokenize_capability(TokenizeCapability::Supported);
            Ok((
                StatusCode::OK,
                Json(serde_json::json!({ "input_tokens": count })),
            )
                .into_response())
        }
        Ok(None) | Err(_) => {
            gateway.set_tokenize_capability(TokenizeCapability::Unsupported);
            Err(AppError::not_found("upstream does not support /tokenize"))
        }
    }
}

fn stream_chat_completions_response(
    model: String,
    include_usage: bool,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> Response {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        let mut converter = ChatCompletionStreamConverter::new(model, include_usage);
        let mut stream = std::pin::pin!(stream);
        'streaming: while let Some(event) = stream.next().await {
            let chat_events = converter.convert(&event);
            for chat_event in chat_events {
                if tx.send(chat_event).await.is_err() {
                    break 'streaming;
                }
            }
        }
    });

    let mapped = ReceiverStream::new(rx).map(|event| {
        Ok::<_, Infallible>(axum::response::sse::Event::default().data(event.to_sse_data()))
    });

    let mut response = Sse::new(mapped)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    response
}

fn stream_anthropic_response(
    model: String,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        let mut converter = AnthropicStreamConverter::new(model);
        let mut stream = std::pin::pin!(stream);
        while let Some(event) = stream.next().await {
            let anthropic_events = converter.convert(&event);
            for anthropic_event in anthropic_events {
                if tx.send(anthropic_event).await.is_err() {
                    return;
                }
            }
        }
        // The upstream event stream ended. If it never produced a
        // `response.completed` (engine error, dropped/stalled turn, aborted
        // web-search round-trip), emit a terminal `message_delta` +
        // `message_stop` so the client is not left hanging behind the SSE
        // keep-alive forever.
        for anthropic_event in converter.finalize() {
            if tx.send(anthropic_event).await.is_err() {
                return;
            }
        }
    });

    let mapped = ReceiverStream::new(rx).map(|event| {
        Ok::<_, Infallible>(
            axum::response::sse::Event::default()
                .event(event.sse_event_type())
                .data(event.to_json()),
        )
    });

    let mut response = Sse::new(mapped)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    Ok(response)
}

fn proxy_upstream_response(response: reqwest::Response) -> Response {
    let status = response.status();
    let upstream_headers = response.headers().clone();
    let mut builder = Response::builder().status(status);
    if let Some(headers) = builder.headers_mut() {
        copy_proxy_response_headers(&upstream_headers, headers);
    }
    builder
        .body(Body::from_stream(response.bytes_stream()))
        .expect("valid upstream proxy response")
}

fn copy_proxy_response_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for (name, value) in source {
        if should_proxy_response_header(name) {
            target.append(name.clone(), value.clone());
        }
    }
}

fn should_proxy_response_header(name: &HeaderName) -> bool {
    !is_hop_by_hop_header(name) && !header_name_eq(name, "content-length")
}

fn is_hop_by_hop_header(name: &HeaderName) -> bool {
    [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ]
    .iter()
    .any(|header| header_name_eq(name, header))
}

fn header_name_eq(name: &HeaderName, other: &str) -> bool {
    name.as_str().eq_ignore_ascii_case(other)
}

fn stream_responses_response(stream: ReceiverStream<crate::engine::SseEvent>) -> Response {
    let mapped = stream.map(|event| {
        Ok::<_, Infallible>(
            axum::response::sse::Event::default()
                .event(event.event)
                .data(event.data.to_string()),
        )
    });
    let mut response = Sse::new(mapped)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    response
}

async fn collect_responses_response(
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let mut final_payload: Option<Value> = None;
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        match event.event.as_str() {
            "response.completed" | "response.incomplete" => {
                final_payload = event.data.get("response").cloned();
            }
            "response.failed" => {
                let message = event
                    .data
                    .get("response")
                    .and_then(|response| response.get("error"))
                    .and_then(|error| error.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("upstream request failed");
                return Err(AppError::upstream(message));
            }
            _ => {}
        }
    }

    match final_payload {
        Some(payload) => Ok(Json(payload).into_response()),
        None => Err(AppError::upstream(
            "stream ended before a final response resource was emitted",
        )),
    }
}

async fn collect_chat_completions_response(
    model: String,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let mut collector = ChatCompletionCollector::new(model);
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        collector.process(&event);
    }
    Ok(Json(collector.into_response()?).into_response())
}

async fn collect_anthropic_response(
    model: String,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let mut collector = AnthropicStreamCollector::new(model);
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        collector.process(&event);
    }
    match collector.into_response() {
        Ok(msg) => Ok(Json(msg).into_response()),
        Err(err) => Ok(anthropic_error_response(AppError::upstream(err.message))),
    }
}

fn anthropic_error_response(err: AppError) -> Response {
    let status = err.status_code();
    let error_type = match err.status_code() {
        axum::http::StatusCode::BAD_REQUEST => "invalid_request_error",
        axum::http::StatusCode::CONFLICT => "invalid_request_error",
        axum::http::StatusCode::NOT_FOUND => "not_found_error",
        _ => "api_error",
    };
    let body = serde_json::json!({
        "type": "error",
        "error": {
            "type": error_type,
            "message": err.to_string(),
        }
    });
    (status, Json(body)).into_response()
}

#[derive(Debug, Default, Deserialize)]
struct ModelsListQuery {
    after_id: Option<String>,
    before_id: Option<String>,
    limit: Option<String>,
}

async fn get_models(
    headers: HeaderMap,
    Query(query): Query<ModelsListQuery>,
    State(gateway): State<Arc<Gateway>>,
) -> AppResult<Response> {
    let anthropic_models = is_anthropic_models_request(&headers);
    let response = gateway.upstream_client().list_models().await?;
    let (status, body, etag) = collect_models_response(response).await?;
    let body = if anthropic_models {
        transform_models_response_for_anthropic(body, &query, gateway.config())?
    } else {
        body
    };
    let mut headers = HeaderMap::new();
    if !anthropic_models && let Some(etag) = etag {
        headers.insert(
            http::header::ETAG,
            HeaderValue::from_str(&etag)
                .map_err(|err| AppError::internal(format!("invalid ETag header: {err}")))?,
        );
    }
    Ok((status, headers, Json(body)).into_response())
}

fn is_anthropic_models_request(headers: &HeaderMap) -> bool {
    headers.contains_key("anthropic-version") || headers.contains_key("anthropic-beta")
}

fn transform_models_response_for_anthropic(
    body: Value,
    query: &ModelsListQuery,
    config: &Config,
) -> AppResult<Value> {
    if query.after_id.is_some() && query.before_id.is_some() {
        return Err(AppError::bad_request(
            "after_id and before_id cannot both be specified",
        ));
    }

    let limit = parse_anthropic_models_limit(query.limit.as_deref())?;
    let models = extract_model_entries(&body)
        .into_iter()
        .filter_map(|entry| anthropic_model_entry(&entry, config))
        .collect::<Vec<_>>();

    let (page, has_more) = page_anthropic_models(&models, query, limit)?;
    let first_id = page
        .first()
        .and_then(model_id_from_value)
        .map(Value::String)
        .unwrap_or(Value::Null);
    let last_id = page
        .last()
        .and_then(model_id_from_value)
        .map(Value::String)
        .unwrap_or(Value::Null);

    Ok(serde_json::json!({
        "data": page,
        "first_id": first_id,
        "has_more": has_more,
        "last_id": last_id,
    }))
}

fn parse_anthropic_models_limit(limit: Option<&str>) -> AppResult<usize> {
    match limit {
        Some(raw) => {
            let parsed = raw
                .parse::<usize>()
                .map_err(|_| AppError::bad_request("limit must be an integer from 1 to 1000"))?;
            if !(1..=1000).contains(&parsed) {
                return Err(AppError::bad_request("limit must be between 1 and 1000"));
            }
            Ok(parsed)
        }
        None => Ok(20),
    }
}

fn extract_model_entries(body: &Value) -> Vec<Value> {
    match body {
        Value::Array(entries) => entries.clone(),
        Value::Object(map) => map
            .get("data")
            .or_else(|| map.get("models"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn anthropic_model_entry(entry: &Value, config: &Config) -> Option<Value> {
    match entry {
        Value::String(id) => {
            let caps = infer_capabilities_from_model_id(id);
            let caps = merge_configured_capabilities(config, id, caps);
            Some(build_anthropic_model_entry(
                id,
                id,
                UNKNOWN_MODEL_CREATED_AT,
                None,
                None,
                Some(&caps),
            ))
        }
        Value::Object(map) => {
            let id = map.get("id").and_then(Value::as_str)?;
            let display_name = map
                .get("display_name")
                .and_then(Value::as_str)
                .or_else(|| map.get("id").and_then(Value::as_str))
                .unwrap_or(id);
            let created_at =
                parse_created_at(map).unwrap_or_else(|| UNKNOWN_MODEL_CREATED_AT.to_string());

            let max_input_tokens = map
                .get("max_input_tokens")
                .or_else(|| map.get("context_length"))
                .or_else(|| map.get("context_window"))
                .or_else(|| map.get("max_context_length"))
                .or_else(|| map.get("max_model_len"));
            let max_tokens = map
                .get("max_tokens")
                .or_else(|| map.get("max_output_tokens"));
            let capabilities = map
                .get("capabilities")
                .filter(|value| value.is_object())
                .map(Value::clone)
                .unwrap_or_else(|| infer_capabilities_from_model_id(id));
            let capabilities = merge_configured_capabilities(config, id, capabilities);

            Some(build_anthropic_model_entry(
                id,
                display_name,
                &created_at,
                max_input_tokens,
                max_tokens,
                Some(&capabilities),
            ))
        }
        _ => None,
    }
}

/// Parse a creation timestamp from common upstream formats.
///
/// - `created_at` → ISO 8601 string (passed through)
/// - `created`    → Unix epoch integer / float (⇒ ISO 8601 string)
fn parse_created_at(map: &serde_json::Map<String, Value>) -> Option<String> {
    match map.get("created_at").and_then(Value::as_str) {
        Some(iso) => Some(iso.to_string()),
        None => {
            let epoch = map.get("created").and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_f64().and_then(|f| (f as u64).checked_add(0)))
            })?;
            chrono::DateTime::from_timestamp(epoch as i64, 0)
                .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        }
    }
}

fn infer_capabilities_from_model_id(_id: &str) -> Value {
    default_anthropic_model_capabilities()
}

/// Override the base capabilities with any `capabilities` block configured on the
/// profile that resolves to this upstream id (id-keyed, else an alias whose
/// `upstream_model` targets the id, else the reserved `*` profile). Unconfigured
/// caps keep the base value; configured caps replace the base per cap key, wholesale.
fn merge_configured_capabilities(config: &Config, id: &str, base: Value) -> Value {
    match config.resolve_capabilities_for_upstream(id) {
        Some(caps) => caps.merge_into(base),
        None => base,
    }
}

fn build_anthropic_model_entry(
    id: &str,
    display_name: &str,
    created_at: &str,
    max_input_tokens: Option<&Value>,
    max_tokens: Option<&Value>,
    capabilities: Option<&Value>,
) -> Value {
    serde_json::json!({
        "id": id,
        "capabilities": capabilities
            .filter(|value| value.is_object())
            .cloned()
            .unwrap_or_else(default_anthropic_model_capabilities),
        "created_at": created_at,
        "display_name": display_name,
        "max_input_tokens": numeric_field_or_zero(max_input_tokens),
        "max_tokens": numeric_field_or_zero(max_tokens),
        "type": "model",
    })
}

fn numeric_field_or_zero(value: Option<&Value>) -> Value {
    value
        .and_then(Value::as_u64)
        .map(|number| serde_json::json!(number))
        .unwrap_or_else(|| serde_json::json!(0))
}

fn default_anthropic_model_capabilities() -> Value {
    let unsupported = || serde_json::json!({ "supported": false });
    serde_json::json!({
        "batch": unsupported(),
        "citations": unsupported(),
        "code_execution": unsupported(),
        "context_management": {
            "clear_thinking_20251015": unsupported(),
            "clear_tool_uses_20250919": unsupported(),
            "compact_20260112": unsupported(),
            "supported": false
        },
        "effort": {
            "high": unsupported(),
            "low": unsupported(),
            "max": unsupported(),
            "medium": unsupported(),
            "supported": false
        },
        "image_input": unsupported(),
        "pdf_input": unsupported(),
        "structured_outputs": unsupported(),
        "thinking": {
            "supported": false,
            "types": {
                "adaptive": unsupported(),
                "enabled": unsupported()
            }
        }
    })
}

fn page_anthropic_models(
    models: &[Value],
    query: &ModelsListQuery,
    limit: usize,
) -> AppResult<(Vec<Value>, bool)> {
    if let Some(before_id) = query.before_id.as_deref() {
        let end = model_index(models, before_id)?;
        let start = end.saturating_sub(limit);
        return Ok((models[start..end].to_vec(), start > 0));
    }

    let start = match query.after_id.as_deref() {
        Some(after_id) => model_index(models, after_id)? + 1,
        None => 0,
    };
    let end = (start + limit).min(models.len());
    Ok((models[start..end].to_vec(), end < models.len()))
}

fn model_index(models: &[Value], id: &str) -> AppResult<usize> {
    models
        .iter()
        .position(|model| model_id_from_value(model).as_deref() == Some(id))
        .ok_or_else(|| AppError::bad_request(format!("model cursor not found: {id}")))
}

fn model_id_from_value(model: &Value) -> Option<String> {
    model
        .as_object()
        .and_then(|map| map.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}
