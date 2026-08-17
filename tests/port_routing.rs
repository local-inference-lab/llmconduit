//! Ported surface: backend-routing (claude-relay test_backend.py).
//!
//! claude-relay injected per-model `model_sampling` defaults and let client
//! params override them. llmconduit's equivalent contract (AGENTS.md): explicit
//! request fields WIN over configured `upstream_chat_kwargs`, and typed fields
//! remove the conflicting default key rather than double-sending it.
//!
//! The model-family reshaping behaviors (Kimi/DeepSeek) are gap G2: family is
//! detected from the FINAL per-provider model in the UPSTREAM CLIENT (not the
//! engine) — routing/failover/exposed-alias paths rewrite the real provider
//! model there — and family-specific `chat_template_kwargs` are injected (Kimi
//! `thinking`/`preserve_thinking`, DeepSeek `enable_thinking`), composing with
//! configured/provider kwargs while request `extra_body` still wins. Detection
//! honors a case-insensitive model id or a `template_family` override. The mock
//! upstream mirrors the production leaf, so the recorded request reflects the
//! injected kwargs. Output-side nested-`thinking` reshape is already handled in
//! `chat_to_responses.rs`; this composes with it rather than duplicating it.
//!
//! Finding 2: forced-but-unrequested Kimi reasoning must not leak to a Chat
//! client — the Chat output converter suppresses it when the client did not ask
//! (no `reasoning_effort`), while leaving normal Chat reasoning unchanged.
//!
//! These tests drive the full gateway with `MockUpstream` and inspect the
//! serialized upstream request body, so the effective wire value is asserted
//! regardless of whether it lands in a typed field or flattened `extra_body`.

mod common;

use common::MockSearch;
use common::MockUpstream;
use common::base_request;
use common::collect_stream;
use common::content_chunk;
use common::done_items;
use common::nested_thinking_chunk;
use common::test_config;
use common::test_gateway_with_config;
use common::usage_chunk;
use common::user_message;
use futures::StreamExt;
use llmconduit::adapters::chat_completions;
use llmconduit::adapters::chat_completions::ChatCompletionStreamConverter;
use llmconduit::adapters::chat_completions::ChatSseEvent;
use llmconduit::models::chat::ChatCompletionRequest;
use llmconduit::models::responses::ResponseItem;
use serde_json::json;

/// Build a config whose configured upstream default sets temperature = 0.1.
fn config_with_default_temperature() -> llmconduit::config::Config {
    let mut config = test_config();
    let mut kwargs = serde_json::Map::new();
    kwargs.insert("temperature".to_string(), json!(0.1));
    config.upstream_chat_kwargs = kwargs;
    config
}

/// A request whose resolved upstream model is `model` (no `upstream_model`
/// remap in `test_config`, and the mock catalog is empty, so the request model
/// flows through unchanged as the resolved model). `reasoning` is `None`, i.e.
/// the client did NOT request reasoning — the "inactive" case that Kimi must
/// still force `thinking:true` on to stop reasoning leakage.
fn inactive_request_for_model(model: &str) -> llmconduit::models::responses::ResponsesRequest {
    let mut request = base_request(vec![user_message("hello")]);
    request.model = model.to_string();
    request.reasoning = None;
    request
}

async fn run_and_capture_upstream_body(
    config: llmconduit::config::Config,
    request: llmconduit::models::responses::ResponsesRequest,
) -> serde_json::Value {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "hi")),
            Ok(usage_chunk("chat-1", 5, 1, 6)),
        ])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1, "exactly one upstream request");
    serde_json::to_value(&requests[0]).expect("serialize upstream request")
}

/// test_send_to_backend_client_params_beat_sampling_defaults:
/// an explicit request temperature overrides the configured default.
#[tokio::test]
async fn explicit_request_temperature_beats_configured_default() {
    let mut request = base_request(vec![user_message("hello")]);
    request.temperature = Some(0.9);

    let body = run_and_capture_upstream_body(config_with_default_temperature(), request).await;

    assert_eq!(
        body["temperature"],
        json!(0.9),
        "explicit request temperature must win over the configured 0.1 default"
    );
}

/// test_send_to_backend_injects_sampling_from_detected_model:
/// when the request omits temperature, the configured default is applied.
#[tokio::test]
async fn configured_default_temperature_applies_when_request_omits_it() {
    let mut request = base_request(vec![user_message("hello")]);
    request.temperature = None;

    let body = run_and_capture_upstream_body(config_with_default_temperature(), request).await;

    assert_eq!(
        body["temperature"],
        json!(0.1),
        "configured default temperature should flow upstream when request omits it"
    );
}

// ---------------------------------------------------------------------------
// GAP G2 — backend model-family reshaping (Kimi / DeepSeek). The gateway
// detects the resolved backend family and injects family-specific
// chat_template_kwargs, composing with the existing partial Kimi handling
// (sentinel cleanup + nested-thinking parsing in chat_to_responses.rs).
// ---------------------------------------------------------------------------

/// test_kimi_thinking_always_enabled_even_when_client_inactive:
/// a Kimi-family backend always sends thinking=true to stop reasoning leakage,
/// EVEN when the client did not request reasoning (`reasoning: None`).
#[tokio::test]
async fn kimi_backend_forces_thinking_kwargs() {
    let request = inactive_request_for_model("kimi-k2-instruct");
    let body = run_and_capture_upstream_body(test_config(), request).await;
    assert_eq!(body["chat_template_kwargs"]["thinking"], json!(true));
    assert_eq!(
        body["chat_template_kwargs"]["preserve_thinking"],
        json!(true)
    );
}

/// test_deepseek_thinking_active_regression:
/// a DeepSeek-family backend injects enable_thinking + reasoning_effort.
#[tokio::test]
async fn deepseek_backend_injects_enable_thinking() {
    let mut request = base_request(vec![user_message("hello")]);
    request.model = "deepseek-v3".to_string();
    let body = run_and_capture_upstream_body(test_config(), request).await;
    assert_eq!(body["chat_template_kwargs"]["enable_thinking"], json!(true));
    // base_request asks for `medium` effort, normalized to `high` upstream and
    // mirrored into the DeepSeek chat_template_kwargs.
    assert_eq!(
        body["chat_template_kwargs"]["reasoning_effort"],
        json!("high")
    );
}

/// Family is detected from the RESOLVED model id case-insensitively, not from
/// stale config — an upper/mixed-case `Kimi` id still triggers Kimi kwargs.
#[tokio::test]
async fn family_detection_is_case_insensitive_on_resolved_model() {
    let request = inactive_request_for_model("Moonshot-KIMI-K2");
    let body = run_and_capture_upstream_body(test_config(), request).await;
    assert_eq!(body["chat_template_kwargs"]["thinking"], json!(true));
}

/// A non-Kimi/non-DeepSeek model (e.g. glm) gets NO injected family kwargs:
/// the gateway only reshapes families it positively recognizes.
#[tokio::test]
async fn unrecognized_family_injects_no_chat_template_kwargs() {
    let request = inactive_request_for_model("glm-5.1");
    let body = run_and_capture_upstream_body(test_config(), request).await;
    assert!(
        body.get("chat_template_kwargs").is_none()
            || body["chat_template_kwargs"].as_object().unwrap().is_empty(),
        "no family kwargs for an unrecognized model, got {:?}",
        body.get("chat_template_kwargs")
    );
}

/// A `template_family` override forces the family regardless of model name:
/// a glm-named model configured as `kimi` gets Kimi kwargs.
#[tokio::test]
async fn template_family_override_forces_family_regardless_of_name() {
    let mut config = test_config();
    config.template_family = Some("kimi".to_string());
    let request = inactive_request_for_model("glm-5.1");
    let body = run_and_capture_upstream_body(config, request).await;
    assert_eq!(body["chat_template_kwargs"]["thinking"], json!(true));
    assert_eq!(
        body["chat_template_kwargs"]["preserve_thinking"],
        json!(true)
    );
}

/// The resolved model beats a stale configured `upstream_model`: a `kimi`
/// `upstream_model` that the backend does NOT serve normalizes to the served
/// DeepSeek model, and the DeepSeek contract (not Kimi) is injected — the
/// concretely resolved model is authoritative (claude-relay lesson).
#[tokio::test]
async fn resolved_model_wins_over_stale_configured_upstream_model() {
    let upstream = MockUpstream::default();
    // Backend serves only deepseek-v3; a stale kimi-k2 upstream_model in config
    // does not match, so normalization falls back to the served model.
    upstream.set_supported_models(["deepseek-v3"]).await;
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "hi")),
            Ok(usage_chunk("chat-1", 5, 1, 6)),
        ])
        .await;
    let mut config = test_config();
    config.upstream_model = Some("kimi-k2".to_string());
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let request = base_request(vec![user_message("hello")]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let body = serde_json::to_value(&upstream.requests().await[0]).expect("serialize");
    assert_eq!(
        body["model"],
        json!("deepseek-v3"),
        "resolved to served model"
    );
    assert_eq!(body["chat_template_kwargs"]["enable_thinking"], json!(true));
    assert!(
        body["chat_template_kwargs"].get("thinking").is_none(),
        "DeepSeek contract must not carry the Kimi `thinking` key"
    );
}

/// Explicit request `extra_body.chat_template_kwargs` wins over injected family
/// defaults on conflict (deep-merge, request-source-wins).
#[tokio::test]
async fn request_chat_template_kwargs_override_family_default() {
    let mut request = inactive_request_for_model("kimi-k2");
    request.extra_body.insert(
        "chat_template_kwargs".to_string(),
        json!({ "thinking": false, "custom_flag": 1 }),
    );
    let body = run_and_capture_upstream_body(test_config(), request).await;
    // Request value wins on the conflicting key...
    assert_eq!(body["chat_template_kwargs"]["thinking"], json!(false));
    // ...the request-only key survives...
    assert_eq!(body["chat_template_kwargs"]["custom_flag"], json!(1));
    // ...and the non-conflicting injected family key is still present.
    assert_eq!(
        body["chat_template_kwargs"]["preserve_thinking"],
        json!(true)
    );
}

/// Family kwargs DEEP-MERGE with configured upstream_chat_kwargs rather than
/// clobbering them: a configured non-conflicting kwarg coexists with the
/// injected Kimi keys.
#[tokio::test]
async fn family_kwargs_deep_merge_with_configured_upstream_kwargs() {
    let mut config = test_config();
    config.upstream_chat_kwargs.insert(
        "chat_template_kwargs".to_string(),
        json!({ "preserve_thinking": false, "configured_only": true }),
    );
    let request = inactive_request_for_model("kimi-k2");
    let body = run_and_capture_upstream_body(config, request).await;
    // Kimi forces thinking=true and OVERRIDES the configured preserve_thinking
    // (a stale configured default must not re-enable leakage).
    assert_eq!(body["chat_template_kwargs"]["thinking"], json!(true));
    assert_eq!(
        body["chat_template_kwargs"]["preserve_thinking"],
        json!(true)
    );
    // The configured non-conflicting key is preserved (deep-merge, not clobber).
    assert_eq!(body["chat_template_kwargs"]["configured_only"], json!(true));
}

/// Compose with the existing OUTPUT-side reshape: a Kimi backend that emits a
/// nested assistant `thinking{}` chunk is reshaped to a flat reasoning item by
/// `chat_to_responses.rs`. G2 (kwargs injection) does not duplicate or break
/// that path — the canonical output still carries flat reasoning text.
#[tokio::test]
async fn kimi_nested_thinking_chunk_reshaped_to_flat_reasoning() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(nested_thinking_chunk("chat-1", "hidden chain", "sig_xyz")),
            Ok(content_chunk("chat-1", "final answer")),
            Ok(usage_chunk("chat-1", 5, 2, 7)),
        ])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());
    let request = inactive_request_for_model("kimi-k2");
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    // The upstream request carried the injected Kimi kwargs...
    let sent = serde_json::to_value(&upstream.requests().await[0]).expect("serialize");
    assert_eq!(sent["chat_template_kwargs"]["thinking"], json!(true));

    // ...and the nested thinking{} was reshaped into a flat reasoning item with
    // its content and signature (composed with the existing parser).
    let items = done_items(&events);
    let reasoning = items
        .iter()
        .find_map(|item| match item {
            ResponseItem::Reasoning {
                content,
                encrypted_content,
                ..
            } => Some((content.clone(), encrypted_content.clone())),
            _ => None,
        })
        .expect("a reasoning item in the output");
    let (content, signature) = reasoning;
    let flat_text = content
        .and_then(|items| items.into_iter().next())
        .map(|item| match item {
            llmconduit::models::responses::ReasoningContentItem::ReasoningText { text }
            | llmconduit::models::responses::ReasoningContentItem::Text { text } => text,
        })
        .unwrap_or_default();
    assert_eq!(flat_text, "hidden chain");
    assert_eq!(signature.as_deref(), Some("sig_xyz"));
}

// ---------------------------------------------------------------------------
// GAP G2 Finding 1 — Chat reasoning suppression is FAMILY-INDEPENDENT. Whenever
// the inbound Chat client did NOT request reasoning, the Chat OUTPUT suppresses
// reasoning_content for ALL models (independent of backend family), so
// server-side chain-of-thought never leaks to a client that never asked for it.
// A client that DID request reasoning (reasoning_effort or an explicit thinking
// knob) still receives reasoning_content unchanged.
// ---------------------------------------------------------------------------

/// Build a Chat-Completions inbound request for `model`. `reasoning_effort`
/// None means the client did NOT request reasoning.
fn chat_request(model: &str, reasoning_effort: Option<&str>) -> ChatCompletionRequest {
    ChatCompletionRequest {
        model: model.to_string(),
        messages: vec![llmconduit::models::chat::ChatMessage {
            role: "user".to_string(),
            content: Some(json!("hello")),
            tool_call_id: None,
            name: None,
            reasoning_content: None,
            thinking: None,
            tool_calls: None,
        }],
        stream: true,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: Some(false),
        reasoning_effort: reasoning_effort.map(ToString::to_string),
        response_format: None,
        stream_options: None,
        temperature: None,
        top_p: None,
        max_output_tokens: None,
        frequency_penalty: None,
        presence_penalty: None,
        stop: None,
        extra_body: std::collections::BTreeMap::new(),
    }
}

/// Run the full Chat-Completions path the way `post_chat_completions` does:
/// decide reasoning suppression from the inbound request, convert to Responses,
/// stream through the gateway, then re-encode with the Chat stream converter.
/// Returns (upstream request body, collected Chat `reasoning_content` strings).
async fn run_chat_path(request: ChatCompletionRequest) -> (serde_json::Value, Vec<String>) {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(common::reasoning_chunk("chat-1", "secret thinking")),
            Ok(content_chunk("chat-1", "visible answer")),
            Ok(usage_chunk("chat-1", 5, 2, 7)),
        ])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());

    let suppress = gateway.chat_reasoning_suppressed(&request);
    let model = request.model.clone();
    let responses_request = chat_completions::convert_request(request).expect("convert");
    let stream = gateway
        .stream_responses(responses_request)
        .await
        .expect("stream");

    let mut converter =
        ChatCompletionStreamConverter::with_reasoning_suppression(model, false, suppress);
    let mut reasoning = Vec::new();
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        for chat_event in converter.convert(&event) {
            if let ChatSseEvent::Data(value) = chat_event
                && let Some(text) = value["choices"][0]["delta"]["reasoning_content"].as_str()
            {
                reasoning.push(text.to_string());
            }
        }
    }

    let body = serde_json::to_value(&upstream.requests().await[0]).expect("serialize");
    (body, reasoning)
}

/// A Chat client that did NOT request reasoning, against a Kimi backend:
/// thinking is forced ON upstream, but the Chat response surfaces NO
/// reasoning_content (no server-side leak).
#[tokio::test]
async fn chat_kimi_forced_reasoning_is_suppressed_when_client_did_not_request() {
    let (body, reasoning) = run_chat_path(chat_request("kimi-k2-instruct", None)).await;
    // Forced upstream...
    assert_eq!(body["chat_template_kwargs"]["thinking"], json!(true));
    // ...but NOT surfaced to the Chat client.
    assert!(
        reasoning.is_empty(),
        "forced unrequested reasoning leaked to Chat: {reasoning:?}"
    );
}

/// A Chat client that DID request reasoning (`reasoning_effort`) against the
/// same Kimi backend still receives reasoning_content — normal Chat behavior
/// is unchanged.
#[tokio::test]
async fn chat_kimi_reasoning_surfaces_when_client_requested() {
    let (body, reasoning) = run_chat_path(chat_request("kimi-k2-instruct", Some("high"))).await;
    assert_eq!(body["chat_template_kwargs"]["thinking"], json!(true));
    assert_eq!(
        reasoning,
        vec!["secret thinking"],
        "client-requested reasoning must still surface unchanged"
    );
}

/// `reasoning_effort: none` keeps Kimi reasoning enabled upstream for parser
/// correctness while omitting the reasoning channel from Chat output.
#[tokio::test]
async fn chat_kimi_reasoning_effort_none_suppresses_reasoning_output() {
    let (body, reasoning) = run_chat_path(chat_request("kimi-k2-instruct", Some("none"))).await;
    assert_eq!(body["chat_template_kwargs"]["thinking"], json!(true));
    assert!(
        reasoning.is_empty(),
        "reasoning_effort=none surfaced reasoning_content: {reasoning:?}"
    );
}

/// A NON-family (non-Kimi/non-DeepSeek) backend: a Chat client that did NOT
/// request reasoning sees NO reasoning_content. Suppression is family-
/// independent — it fires off the inbound request alone, not the backend family.
#[tokio::test]
async fn chat_non_family_unrequested_reasoning_is_suppressed() {
    let (_body, reasoning) = run_chat_path(chat_request("glm-5.1", None)).await;
    assert!(
        reasoning.is_empty(),
        "unrequested reasoning must be suppressed for non-family backends too: {reasoning:?}"
    );
}

/// The same NON-family backend, but the Chat client DID request reasoning via
/// `reasoning_effort`: reasoning_content surfaces unchanged.
#[tokio::test]
async fn chat_non_family_reasoning_surfaces_when_client_requested() {
    let (_body, reasoning) = run_chat_path(chat_request("glm-5.1", Some("high"))).await;
    assert_eq!(
        reasoning,
        vec!["secret thinking"],
        "client-requested reasoning must surface from a non-family backend"
    );
}
