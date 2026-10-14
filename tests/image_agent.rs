//! G4 — Image agent (vision offload) integration tests.
//!
//! The in-proxy vision offload (strip images to `[Image #N]` placeholders, inject
//! the `analyzeImage` server tool, run it against a vision backend, and feed the
//! description back into the chat history) is a self-contained topic, so its
//! suite lives in its own crate here rather than in `tests/gateway.rs`. Shared
//! gateway/config/request builders and the recording `MockVisionClient` come from
//! `tests/common`; everything image-agent-specific is asserted below.

mod common;

use common::MockSearch;
use common::MockUpstream;
use common::MockVisionClient;
use common::TEST_IMAGE_DATA_URL;
use common::base_request;
use common::chat_completion_sse_body;
use common::collect_stream;
use common::content_chunk;
use common::event_names;
use common::image_agent_config;
use common::test_config;
use common::test_gateway_with_config;
use common::test_gateway_with_config_and_replay_store;
use common::test_gateway_with_vision;
use common::tool_call_chunk;
use common::user_message;
use common::user_message_with_image;

use axum::body::Body;
use axum::http::Request;
use futures::StreamExt;
use llmconduit::config::FallbackUpstreamConfig;
use llmconduit::config::UnsupportedImagePolicy;
use llmconduit::config::UpstreamConfig;
use llmconduit::models::chat::ChatChunkChoice;
use llmconduit::models::chat::ChatCompletionChunk;
use llmconduit::models::chat::ChatDelta;
use llmconduit::models::chat::ChatFunctionCall;
use llmconduit::models::chat::ChatMessage;
use llmconduit::models::chat::ChatToolCall;
use llmconduit::models::responses::ContentItem;
use llmconduit::models::responses::ResponseItem;
use llmconduit::models::responses::ToolSpec;
use llmconduit::replay::ReplayRecord;
use llmconduit::replay::ReplayStore;
use pretty_assertions::assert_eq;
use serde_json::Map as JsonMap;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Notify;
use tower::ServiceExt;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_partial_json;
use wiremock::matchers::method;
use wiremock::matchers::path;

// ===========================================================================
// G4 — Image agent (vision offload) integration tests.
//
// Shared fixtures (`image_agent_config`, `user_message_with_image`,
// `TEST_IMAGE_DATA_URL`, `MockVisionClient`, `test_gateway_with_vision`) live in
// `tests/common`; only the assertions are below.
// ===========================================================================

#[tokio::test]
async fn image_agent_runs_analyze_image_then_answers() {
    // Round 1: model calls analyzeImage. Round 2: model answers with the
    // injected vision description. Two upstream calls; analyzeImage never leaks.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"1\"],\"task\":\"describe\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk(
            "chat-2",
            "The image shows a red square.",
        ))])
        .await;
    let vision = MockVisionClient::default();
    vision
        .push_outcome(Ok(llmconduit::vision::VisionOutcome {
            text: "A small red square on white.".to_string(),
        }))
        .await;
    let gateway = test_gateway_with_vision(upstream.clone(), vision.clone(), image_agent_config());

    let request = base_request(vec![user_message_with_image(
        "what is this?",
        TEST_IMAGE_DATA_URL,
    )]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    // Two upstream rounds.
    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 2, "expected analyze round + answer round");

    // The vision backend saw the cached image (by url) and the task.
    let vision_requests = vision.requests().await;
    assert_eq!(vision_requests.len(), 1);
    assert_eq!(vision_requests[0].image_ids, vec!["1"]);
    assert_eq!(vision_requests[0].images.len(), 1);
    assert_eq!(vision_requests[0].images[0].image_url, TEST_IMAGE_DATA_URL);

    // Round 2 carries the vision description as a tool result.
    let round2 = &requests[1];
    let tool_msg = round2
        .messages
        .iter()
        .find(|m| m.role == "tool")
        .expect("vision tool result injected");
    assert_eq!(
        tool_msg.content.as_ref().and_then(|v| v.as_str()),
        Some("A small red square on white.")
    );

    // analyzeImage never surfaces in the public Responses stream.
    let names = event_names(&events);
    assert!(
        !names.contains(&"response.function_call_arguments.delta"),
        "analyzeImage deltas must be suppressed"
    );
    for event in &events {
        if event["_event"] == "response.output_item.done" {
            assert_ne!(
                event["item"]["name"].as_str(),
                Some("analyzeImage"),
                "analyzeImage must not appear as an output item"
            );
        }
    }
    // The final answer is present.
    let answer: String = events
        .iter()
        .filter(|e| e["_event"] == "response.output_text.delta")
        .filter_map(|e| e["delta"].as_str())
        .collect();
    assert!(answer.contains("red square"));
}

#[tokio::test]
async fn image_agent_strips_raw_image_bytes_from_upstream() {
    // The text backend must NEVER receive the raw image bytes: round 1 carries
    // only the [Image #N] placeholder, and the injected analyzeImage tool.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk(
            "chat-1",
            "I cannot see images directly.",
        ))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "raw image base64 must not reach the text upstream"
    );
    assert!(
        serialized.contains("[Image #1]"),
        "placeholder must be present"
    );
    // The analyzeImage tool was injected for the text backend.
    let tools = requests[0].tools.as_ref().expect("tools present");
    assert!(
        tools.iter().any(|t| t.function.name == "analyzeImage"),
        "analyzeImage tool injected"
    );
    // System prompt instructs the model about [Image #N].
    let system = requests[0]
        .messages
        .iter()
        .find(|m| m.role == "system")
        .and_then(|m| m.content.as_ref())
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(system.contains("You CANNOT see images"));
}

#[tokio::test]
async fn image_agent_handles_multiple_image_ids() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"1\",\"2\"],\"task\":\"compare\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "Both differ."))])
        .await;
    let vision = MockVisionClient::default();
    let gateway = test_gateway_with_vision(upstream.clone(), vision.clone(), image_agent_config());

    let request = base_request(vec![ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![
            ContentItem::InputText {
                text: "compare".to_string(),
            },
            ContentItem::InputImage {
                image_url: Some("data:image/png;base64,AAAA".to_string()),
                file_id: None,
                detail: None,
            },
            ContentItem::InputImage {
                image_url: Some("data:image/png;base64,BBBB".to_string()),
                file_id: None,
                detail: None,
            },
        ],
        phase: None,
    }]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let vision_requests = vision.requests().await;
    assert_eq!(vision_requests[0].image_ids, vec!["1", "2"]);
    assert_eq!(vision_requests[0].images.len(), 2);
    assert_eq!(
        vision_requests[0].images[0].image_url,
        "data:image/png;base64,AAAA"
    );
    assert_eq!(
        vision_requests[0].images[1].image_url,
        "data:image/png;base64,BBBB"
    );
}

#[tokio::test]
async fn image_agent_vision_error_becomes_model_visible_text() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"1\"],\"task\":\"x\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "Sorry, no image."))])
        .await;
    let vision = MockVisionClient::default();
    vision
        .push_outcome(Err(llmconduit::error::AppError::upstream(
            "backend exploded",
        )))
        .await;
    let gateway = test_gateway_with_vision(upstream.clone(), vision, image_agent_config());

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    // Turn still completes (no response.failed).
    let names = event_names(&events);
    assert!(names.contains(&"response.completed"));
    assert!(!names.contains(&"response.failed"));
    // Round 2 tool result carries the failure text.
    let requests = upstream.requests().await;
    let tool_msg = requests[1]
        .messages
        .iter()
        .find(|m| m.role == "tool")
        .and_then(|m| m.content.as_ref())
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(tool_msg.contains("Vision analysis failed"));
}

#[tokio::test]
async fn image_agent_cache_miss_becomes_model_visible_text() {
    // Model asks for an image id that was never cached (e.g. #5 when only #1
    // exists). The executor injects a "no cached image" message, not an error.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"5\"],\"task\":\"x\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "ok"))])
        .await;
    let vision = MockVisionClient::default();
    let gateway = test_gateway_with_vision(upstream.clone(), vision.clone(), image_agent_config());

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    assert!(event_names(&events).contains(&"response.completed"));
    // Vision backend was never called (no image resolved).
    assert!(vision.requests().await.is_empty());
    let requests = upstream.requests().await;
    let tool_msg = requests[1]
        .messages
        .iter()
        .find(|m| m.role == "tool")
        .and_then(|m| m.content.as_ref())
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(tool_msg.contains("no cached image found"));
}

#[tokio::test]
async fn image_agent_vision_timeout_becomes_model_visible_text() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"1\"],\"task\":\"x\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "done"))])
        .await;
    let vision = MockVisionClient::default();
    // Block the vision call forever; a tiny request_timeout forces a timeout.
    vision.block_on(Arc::new(Notify::new())).await;
    let mut config = image_agent_config();
    config.request_timeout = std::time::Duration::from_millis(50);
    let gateway = test_gateway_with_vision(upstream.clone(), vision, config);

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    assert!(event_names(&events).contains(&"response.completed"));
    let requests = upstream.requests().await;
    let tool_msg = requests[1]
        .messages
        .iter()
        .find(|m| m.role == "tool")
        .and_then(|m| m.content.as_ref())
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(tool_msg.contains("timed out"));
}

#[tokio::test]
async fn image_agent_cancellation_drops_vision_work() {
    // Client disconnects while the vision call is blocked: the turn must cancel
    // (drop the receiver) instead of hanging on the vision future.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"1\"],\"task\":\"x\"}",
        ))])
        .await;
    let vision = MockVisionClient::default();
    vision.block_on(Arc::new(Notify::new())).await;
    let gateway = test_gateway_with_vision(upstream.clone(), vision.clone(), image_agent_config());

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let mut stream = gateway.stream_responses(request).await.expect("stream");
    // Capture both `notified()` futures UP FRONT — before the actions that fire
    // them — because `notify_waiters()` stores no permit and a future created
    // after its trigger would miss the wake and hang to the 1s timeout.
    let entered = vision.entered();
    let dropped = vision.dropped();
    // Drain the prologue, then await the vision future actually starting (its
    // request already recorded) before dropping the stream.
    let _ = stream.next().await;
    let _ = stream.next().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), entered)
        .await
        .expect("vision analyze should have been entered");
    drop(stream);
    // Await the spawned turn reacting to the closed channel: the blocked vision
    // future is dropped, firing the drop-guard signal. No wall-clock sleep.
    tokio::time::timeout(std::time::Duration::from_secs(1), dropped)
        .await
        .expect("vision work should be dropped after client disconnect");
    // Vision was entered but the turn was cancelled; no panic / hang.
    assert_eq!(vision.requests().await.len(), 1);
}

#[tokio::test]
async fn image_agent_not_leaked_in_chat_completions() {
    // Through the Chat ingress/egress, analyzeImage must be hidden end to end.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"1\"],\"task\":\"x\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "A cat."))])
        .await;
    let vision = MockVisionClient::default();
    let gateway = test_gateway_with_vision(upstream.clone(), vision, image_agent_config());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": true,
        "messages": [{
            "role": "user",
            "content": [
                { "type": "text", "text": "what is this?" },
                { "type": "image_url", "image_url": { "url": TEST_IMAGE_DATA_URL } }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        !text.contains("analyzeImage"),
        "analyzeImage must not leak to Chat"
    );
    assert!(text.contains("A cat."));
}

#[tokio::test]
async fn image_agent_not_leaked_in_anthropic_messages() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"1\"],\"task\":\"x\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "A dog."))])
        .await;
    let vision = MockVisionClient::default();
    let gateway = test_gateway_with_vision(upstream.clone(), vision, image_agent_config());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{
            "role": "user",
            "content": [
                { "type": "text", "text": "what is this?" },
                { "type": "image", "source": { "type": "base64", "media_type": "image/png", "data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA=" } }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        !text.contains("analyzeImage"),
        "analyzeImage must not leak to Anthropic"
    );
    assert!(
        !text.contains("tool_use"),
        "no tool_use block for the internal tool"
    );
    assert!(text.contains("A dog."));
}

#[tokio::test]
async fn image_agent_preserves_parallel_tool_calls_upstream() {
    for parallel_tool_calls in [None, Some(true), Some(false)] {
        let upstream = MockUpstream::default();
        upstream
            .push_response(vec![Ok(content_chunk("chat-1", "no images visible"))])
            .await;
        let gateway = test_gateway_with_vision(
            upstream.clone(),
            MockVisionClient::default(),
            image_agent_config(),
        );
        let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
        request.parallel_tool_calls = parallel_tool_calls;
        let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
        assert!(event_names(&events).contains(&"response.completed"));
        let requests = upstream.requests().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].parallel_tool_calls, parallel_tool_calls);
        assert!(
            requests[0]
                .tools
                .as_ref()
                .unwrap()
                .iter()
                .any(|tool| tool.function.name == "analyzeImage")
        );
    }
}

// --- Gating ---

#[tokio::test]
async fn image_agent_skips_when_disabled() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let mut config = image_agent_config();
    config.image_agent_enabled = false;
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    // No analyzeImage tool injected; raw image flows through.
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(!has_analyze, "disabled agent must not inject analyzeImage");
}

#[tokio::test]
async fn image_agent_skips_when_vision_url_missing() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let mut config = image_agent_config();
    config.vision_url = None;
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(!has_analyze, "missing vision_url must skip the agent");
}

#[tokio::test]
async fn image_agent_skips_when_no_image_in_latest_turn() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    // Text-only latest turn.
    let request = base_request(vec![user_message("just text")]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(!has_analyze, "no-image turn must skip the agent");
}

#[tokio::test]
async fn image_agent_skips_for_native_vision_kimi() {
    // The resolved model is Kimi (native-vision), so the agent must not strip;
    // the raw image flows to the multimodal backend.
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["Kimi-K2.6"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "I see a square."))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "Kimi-K2.6".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        serialized.contains("iVBORw0KGgo"),
        "native-vision Kimi must receive the raw image"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(
        !has_analyze,
        "native-vision backend must not get analyzeImage"
    );
}

#[tokio::test]
async fn image_agent_used_for_text_backend_with_images() {
    // A non-Kimi text backend WITH images activates the agent (contrast to the
    // Kimi skip test): analyzeImage is injected and bytes are stripped.
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["deepseek-v3"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "deepseek-v3".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(
        has_analyze,
        "text backend with images must activate the agent"
    );
}

#[tokio::test]
async fn image_agent_profile_native_vision_override_skips() {
    // A profile `native_vision: true` forces the agent OFF even for a non-Kimi
    // name. Resolved AFTER model resolution (gate uses the profile chain).
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "I see it"))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "glm-5.1".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            native_vision: Some(true),
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        serialized.contains("iVBORw0KGgo"),
        "native_vision profile override must pass the raw image through"
    );
}

#[tokio::test]
async fn image_agent_skips_when_tool_choice_none() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.tool_choice = json!("none");
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(!has_analyze, "tool_choice=none must skip the agent");
}

// --- Hard rules: mixed tools, server batch, round ceiling ---

#[tokio::test]
async fn image_agent_rejects_mixed_client_and_analyze_image() {
    // analyzeImage (server) + a client function tool in the same batch must be
    // rejected, exactly like web_search + client.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(ChatCompletionChunk {
            id: "chat-1".to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: None,
                    reasoning_content: None,
                    tool_calls: Some(vec![
                        ChatToolCall {
                            id: Some("call_img".to_string()),
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: Some("analyzeImage".to_string()),
                                arguments: Some(serde_json::Value::String(
                                    "{\"imageId\":[\"1\"],\"task\":\"x\"}".to_string(),
                                )),
                            },
                        },
                        ChatToolCall {
                            id: Some("call_fn".to_string()),
                            index: Some(1),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: Some("get_weather".to_string()),
                                arguments: Some(serde_json::Value::String("{}".to_string())),
                            },
                        },
                    ]),
                    function_call: None,
                    refusal: None,
                    extra: Default::default(),
                },
                finish_reason: Some("tool_calls".to_string()),
                stop_reason: None,
            }],
            usage: None,
        })])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.tools = vec![ToolSpec::Function {
        name: "get_weather".to_string(),
        description: "d".to_string(),
        strict: false,
        parameters: json!({"type": "object", "properties": {}}),
    }];
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(
        event_names(&events).contains(&"response.failed"),
        "mixed analyzeImage + client tool must fail"
    );
}

#[tokio::test]
async fn image_agent_runs_analyze_and_web_search_sequentially() {
    // A server-only batch mixing analyzeImage + web_search runs both tools
    // sequentially, then the model answers.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(ChatCompletionChunk {
            id: "chat-1".to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: None,
                    reasoning_content: None,
                    tool_calls: Some(vec![
                        ChatToolCall {
                            id: Some("call_img".to_string()),
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: Some("analyzeImage".to_string()),
                                arguments: Some(serde_json::Value::String(
                                    "{\"imageId\":[\"1\"],\"task\":\"x\"}".to_string(),
                                )),
                            },
                        },
                        ChatToolCall {
                            id: Some("call_ws".to_string()),
                            index: Some(1),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: Some("web_search".to_string()),
                                arguments: Some(serde_json::Value::String(
                                    "{\"query\":\"latest\"}".to_string(),
                                )),
                            },
                        },
                    ]),
                    function_call: None,
                    refusal: None,
                    extra: Default::default(),
                },
                finish_reason: Some("tool_calls".to_string()),
                stop_reason: None,
            }],
            usage: None,
        })])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "answer"))])
        .await;
    let vision = MockVisionClient::default();
    // Brave must be configured for web_search to be server-runnable.
    let mut config = image_agent_config();
    config.brave_api_key = Some("test-key".to_string());
    let gateway = test_gateway_with_vision(upstream.clone(), vision.clone(), config);
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.tools = vec![ToolSpec::WebSearch {
        external_web_access: Some(true),
        filters: None,
        user_location: None,
        search_context_size: None,
        search_content_types: None,
    }];
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));
    // Vision ran once.
    assert_eq!(vision.requests().await.len(), 1);
    // Round 2 has both a vision tool result and a web_search tool result.
    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 2);
    let tool_contents: Vec<String> = requests[1]
        .messages
        .iter()
        .filter(|m| m.role == "tool")
        .filter_map(|m| {
            m.content
                .as_ref()
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
        })
        .collect();
    assert!(
        tool_contents
            .iter()
            .any(|c| c.contains("Search result for latest"))
    );
    assert_eq!(tool_contents.len(), 2, "both server tools injected results");
}

#[tokio::test]
async fn image_agent_round_ceiling_terminates_loop() {
    // A model that calls analyzeImage every round must hit the round ceiling and
    // error out rather than loop forever. Queue many analyzeImage rounds.
    let upstream = MockUpstream::default();
    for n in 0..12 {
        upstream
            .push_response(vec![Ok(tool_call_chunk(
                &format!("chat-{n}"),
                &format!("call_img_{n}"),
                "analyzeImage",
                "{\"imageId\":[\"1\"],\"task\":\"x\"}",
            ))])
            .await;
    }
    let vision = MockVisionClient::default();
    let gateway = test_gateway_with_vision(upstream.clone(), vision, image_agent_config());
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(
        event_names(&events).contains(&"response.failed"),
        "image-analysis round ceiling must terminate the loop"
    );
}

#[tokio::test]
async fn image_agent_hides_analyze_deltas_when_args_precede_name_chat() {
    // Review #1: a sparse upstream can stream analyzeImage `arguments` BEFORE the
    // function name. The leading arg fragment must be buffered and dropped, so
    // ZERO function_call_arguments deltas reach the Chat client.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            // Chunk 1: arguments only, no name, no id.
            Ok(ChatCompletionChunk {
                id: "chat-1".to_string(),
                choices: vec![ChatChunkChoice {
                    index: 0,
                    delta: ChatDelta {
                        content: None,
                        reasoning_content: None,
                        tool_calls: Some(vec![ChatToolCall {
                            id: None,
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: None,
                                arguments: Some(serde_json::Value::String(
                                    "{\"imageId\":[\"1\"]".to_string(),
                                )),
                            },
                        }]),
                        function_call: None,
                        refusal: None,
                        extra: Default::default(),
                    },
                    finish_reason: None,
                    stop_reason: None,
                }],
                usage: None,
            }),
            // Chunk 2: name (+ id) arrives with the rest of the arguments.
            Ok(ChatCompletionChunk {
                id: "chat-1".to_string(),
                choices: vec![ChatChunkChoice {
                    index: 0,
                    delta: ChatDelta {
                        content: None,
                        reasoning_content: None,
                        tool_calls: Some(vec![ChatToolCall {
                            id: Some("call_img_1".to_string()),
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: Some("analyzeImage".to_string()),
                                arguments: Some(serde_json::Value::String(
                                    ",\"task\":\"x\"}".to_string(),
                                )),
                            },
                        }]),
                        function_call: None,
                        refusal: None,
                        extra: Default::default(),
                    },
                    finish_reason: Some("tool_calls".to_string()),
                    stop_reason: None,
                }],
                usage: None,
            }),
        ])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "A leaf."))])
        .await;
    let vision = MockVisionClient::default();
    let gateway = test_gateway_with_vision(upstream.clone(), vision.clone(), image_agent_config());

    // Direct Responses stream: assert no function_call_arguments deltas at all.
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let delta_count = events
        .iter()
        .filter(|e| e["_event"] == "response.function_call_arguments.delta")
        .count();
    assert_eq!(
        delta_count, 0,
        "no analyzeImage arg deltas may leak (Responses)"
    );
    // The vision call still received the FULL reconstructed arguments.
    let vision_requests = vision.requests().await;
    assert_eq!(vision_requests[0].image_ids, vec!["1"]);
    assert_eq!(vision_requests[0].task, "x");
}

#[tokio::test]
async fn image_agent_hides_analyze_deltas_when_args_precede_name_anthropic() {
    // Same sparse ordering, but through the Anthropic egress: no tool_use block
    // and no input_json_delta for the internal analyzeImage tool.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(ChatCompletionChunk {
                id: "chat-1".to_string(),
                choices: vec![ChatChunkChoice {
                    index: 0,
                    delta: ChatDelta {
                        content: None,
                        reasoning_content: None,
                        tool_calls: Some(vec![ChatToolCall {
                            id: None,
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: None,
                                arguments: Some(serde_json::Value::String(
                                    "{\"imageId\":[\"1\"]".to_string(),
                                )),
                            },
                        }]),
                        function_call: None,
                        refusal: None,
                        extra: Default::default(),
                    },
                    finish_reason: None,
                    stop_reason: None,
                }],
                usage: None,
            }),
            Ok(ChatCompletionChunk {
                id: "chat-1".to_string(),
                choices: vec![ChatChunkChoice {
                    index: 0,
                    delta: ChatDelta {
                        content: None,
                        reasoning_content: None,
                        tool_calls: Some(vec![ChatToolCall {
                            id: Some("call_img_1".to_string()),
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: Some("analyzeImage".to_string()),
                                arguments: Some(serde_json::Value::String(
                                    ",\"task\":\"x\"}".to_string(),
                                )),
                            },
                        }]),
                        function_call: None,
                        refusal: None,
                        extra: Default::default(),
                    },
                    finish_reason: Some("tool_calls".to_string()),
                    stop_reason: None,
                }],
                usage: None,
            }),
        ])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "A leaf."))])
        .await;
    let vision = MockVisionClient::default();
    let gateway = test_gateway_with_vision(upstream.clone(), vision, image_agent_config());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{
            "role": "user",
            "content": [
                { "type": "text", "text": "what is this?" },
                { "type": "image", "source": { "type": "base64", "media_type": "image/png", "data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA=" } }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        !text.contains("analyzeImage"),
        "no analyzeImage in Anthropic output"
    );
    assert!(
        !text.contains("input_json_delta"),
        "no tool args streamed for analyzeImage"
    );
    assert!(
        !text.contains("tool_use"),
        "no tool_use block for analyzeImage"
    );
    assert!(text.contains("A leaf."));
}

#[tokio::test]
async fn image_agent_resolved_alias_to_kimi_passes_images_through() {
    // Review #2: an exposed-fallback alias that resolves to a native-vision
    // backend (Kimi) must NOT strip images — gating uses the FINAL routed model.
    // Build a routing gateway whose exposed alias "vision-alias" maps to the
    // fallback's upstream_model "Kimi-K2.6".
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [{ "id": "primary-text-model", "object": "model" }]
        })))
        .mount(&server)
        .await;
    // The chat endpoint must receive the RAW image (proof it was not stripped).
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({ "model": "Kimi-K2.6" })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[
                    json!({
                        "id": "chat-1",
                        "choices": [{ "index": 0, "delta": { "content": "I see a square." }, "finish_reason": "stop" }]
                    }),
                ])),
        )
        .mount(&server)
        .await;

    let base = format!("{}/v1", server.uri());
    let mut config = image_agent_config();
    config.upstreams = vec![UpstreamConfig {
        name: "primary".to_string(),
        upstream_base_url: base.parse().expect("url"),
        upstream_api_key: None,
        upstream_model: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstream_request_log_path: None,
        fallback_upstreams: vec![FallbackUpstreamConfig {
            name: "kimi-fallback".to_string(),
            upstream_base_url: base.parse().expect("url"),
            upstream_api_key: None,
            upstream_model: Some("Kimi-K2.6".to_string()),
            exposed_model: Some("vision-alias".to_string()),
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
        }],
    }];
    let app = llmconduit::build_app(config);

    let body = json!({
        "model": "vision-alias",
        "stream": true,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                { "type": "input_text", "text": "what is this?" },
                { "type": "input_image", "image_url": TEST_IMAGE_DATA_URL }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(
        response.status().as_u16(),
        200,
        "the body_partial_json model=Kimi-K2.6 matcher proves the raw image reached Kimi unstripped"
    );
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        !text.contains("analyzeImage"),
        "native-vision alias must not inject analyzeImage"
    );
    assert!(text.contains("I see a square."));
}

#[tokio::test]
async fn image_agent_passes_through_when_all_failover_candidates_native() {
    // Round-2 #1 (safe invariant): EVERY candidate backend model is native-
    // vision (both Kimi variants), so images pass through unstripped and no
    // analyzeImage tool is injected.
    let upstream = MockUpstream::default();
    upstream.set_candidate_models(["Kimi-K2.6", "kimi-vl-a3b"]);
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "I see a square."))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        serialized.contains("iVBORw0KGgo"),
        "all-native failover chain must receive the raw image"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(
        !has_analyze,
        "all-native chain must not inject analyzeImage"
    );
}

#[tokio::test]
async fn image_agent_strips_when_any_failover_candidate_is_non_native() {
    // Round-2 #1: the primary is native (Kimi) but a fallback is a text-only
    // model. The SAFE invariant must strip+offload (works for every backend),
    // since the request could be served by the non-native fallback.
    let upstream = MockUpstream::default();
    upstream.set_candidate_models(["Kimi-K2.6", "deepseek-v3"]);
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see images"))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "a non-native fallback must force strip+offload"
    );
    assert!(serialized.contains("[Image #1]"), "placeholder present");
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(has_analyze, "mixed chain must inject analyzeImage");
}

#[tokio::test]
async fn image_agent_oversized_unresolved_tool_buffer_fails_cleanly() {
    // Round-2 #4 (DoS guard): the upstream streams a huge analyzeImage argument
    // run WITHOUT ever sending the tool name. The pending buffer is capped, so
    // the turn fails cleanly (response.failed) instead of growing memory.
    let upstream = MockUpstream::default();
    // ~1.5 MiB of args across many nameless chunks for one tool-call index.
    let mut chunks = Vec::new();
    for i in 0..192 {
        chunks.push(Ok(ChatCompletionChunk {
            id: "chat-1".to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: None,
                    reasoning_content: None,
                    tool_calls: Some(vec![ChatToolCall {
                        id: None,
                        index: Some(0),
                        kind: "function".to_string(),
                        function: ChatFunctionCall {
                            name: None, // name NEVER arrives
                            arguments: Some(serde_json::Value::String("x".repeat(8 * 1024))),
                        },
                    }]),
                    function_call: None,
                    refusal: None,
                    extra: Default::default(),
                },
                finish_reason: if i == 191 {
                    Some("tool_calls".to_string())
                } else {
                    None
                },
                stop_reason: None,
            }],
            usage: None,
        }));
    }
    upstream.push_response(chunks).await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(
        event_names(&events).contains(&"response.failed"),
        "oversized unresolved tool buffer must fail the turn cleanly"
    );
}

#[tokio::test]
async fn image_agent_client_tool_args_before_name_still_emits_all_deltas() {
    // Round-2 #5: a CLIENT tool whose arguments stream before its name (name
    // arrives name-only, no further arg delta) must still have all its buffered
    // deltas flushed — only analyzeImage is dropped. Image agent active.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            // Chunk 1: client tool args only, no name, no id.
            Ok(ChatCompletionChunk {
                id: "chat-1".to_string(),
                choices: vec![ChatChunkChoice {
                    index: 0,
                    delta: ChatDelta {
                        content: None,
                        reasoning_content: None,
                        tool_calls: Some(vec![ChatToolCall {
                            id: None,
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: None,
                                arguments: Some(serde_json::Value::String(
                                    "{\"location\":".to_string(),
                                )),
                            },
                        }]),
                        function_call: None,
                        refusal: None,
                        extra: Default::default(),
                    },
                    finish_reason: None,
                    stop_reason: None,
                }],
                usage: None,
            }),
            // Chunk 2: NAME-ONLY (+ id) — no arguments, so no delta is produced
            // by the resolution path; the post-stream flush must emit chunk 1.
            Ok(ChatCompletionChunk {
                id: "chat-1".to_string(),
                choices: vec![ChatChunkChoice {
                    index: 0,
                    delta: ChatDelta {
                        content: None,
                        reasoning_content: None,
                        tool_calls: Some(vec![ChatToolCall {
                            id: Some("call_fn_1".to_string()),
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: Some("get_weather".to_string()),
                                arguments: None,
                            },
                        }]),
                        function_call: None,
                        refusal: None,
                        extra: Default::default(),
                    },
                    finish_reason: None,
                    stop_reason: None,
                }],
                usage: None,
            }),
            // Chunk 3: remaining client args (resolved → emitted live).
            Ok(ChatCompletionChunk {
                id: "chat-1".to_string(),
                choices: vec![ChatChunkChoice {
                    index: 0,
                    delta: ChatDelta {
                        content: None,
                        reasoning_content: None,
                        tool_calls: Some(vec![ChatToolCall {
                            id: Some("call_fn_1".to_string()),
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: Some("get_weather".to_string()),
                                arguments: Some(serde_json::Value::String(
                                    "\"Seattle\"}".to_string(),
                                )),
                            },
                        }]),
                        function_call: None,
                        refusal: None,
                        extra: Default::default(),
                    },
                    finish_reason: Some("tool_calls".to_string()),
                    stop_reason: None,
                }],
                usage: None,
            }),
        ])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    // A client tool is defined; image in the latest turn activates the agent.
    let mut request = base_request(vec![user_message_with_image(
        "weather?",
        TEST_IMAGE_DATA_URL,
    )]);
    request.tools = vec![ToolSpec::Function {
        name: "get_weather".to_string(),
        description: "d".to_string(),
        strict: false,
        parameters: json!({"type": "object", "properties": {"location": {"type": "string"}}}),
    }];
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    // The full client-tool arguments must be reconstructible from the deltas:
    // chunk1 `{"location":` (buffered, flushed) + chunk3 `"Seattle"}` (live).
    let streamed: String = events
        .iter()
        .filter(|e| e["_event"] == "response.function_call_arguments.delta")
        .filter_map(|e| e["delta"].as_str())
        .collect();
    assert_eq!(
        streamed, "{\"location\":\"Seattle\"}",
        "all client-tool arg deltas must be emitted, including the pre-name buffer"
    );
}

#[tokio::test]
async fn image_agent_strips_when_candidate_set_is_empty() {
    // Round-3 #1: an EMPTY candidate set (unknown routing/catalog state) must
    // NOT pass raw images through, even if the request model looks native.
    let upstream = MockUpstream::default();
    upstream.set_candidate_models(Vec::<String>::new());
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    // Even a Kimi-looking request model must strip when candidates are unknown.
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "Kimi-K2.6".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "empty candidate set must force strip+offload"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(has_analyze, "empty candidate set must inject analyzeImage");
}

#[tokio::test]
async fn image_agent_request_native_vision_override_does_not_leak_onto_fallback() {
    // Round-3 #2: a per-REQUEST `native_vision: true` profile must NOT make a
    // non-native fallback candidate look native. Per-candidate resolution means
    // a non-native candidate forces strip+offload despite the request override.
    let upstream = MockUpstream::default();
    // Primary is the request model (native via override); fallback is text-only.
    upstream.set_candidate_models(["my-vision-alias", "deepseek-v3"]);
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let mut config = image_agent_config();
    // Profile keyed on the REQUEST model only.
    config.model_profiles = std::collections::BTreeMap::from([(
        "my-vision-alias".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            native_vision: Some(true),
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "my-vision-alias".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "a non-native fallback must force strip despite a per-request native_vision override"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(has_analyze, "mixed chain must inject analyzeImage");
}

#[tokio::test]
async fn image_agent_redacts_data_url_in_successful_vision_text() {
    // Round-3 #3: a SUCCESSFUL vision description that echoes a data:/signed URL
    // must be redacted before it is injected as the tool result (and logged).
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"1\"],\"task\":\"x\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "Done."))])
        .await;
    let vision = MockVisionClient::default();
    vision
        .push_outcome(Ok(llmconduit::vision::VisionOutcome {
            text: "It shows data:image/png;base64,LEAKEDB64 and a logo at https://signed.example.com/x?sig=LEAKEDSIG".to_string(),
        }))
        .await;
    let gateway = test_gateway_with_vision(upstream.clone(), vision, image_agent_config());
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let tool_msg = requests[1]
        .messages
        .iter()
        .find(|m| m.role == "tool")
        .and_then(|m| m.content.as_ref())
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        !tool_msg.contains("LEAKEDB64"),
        "data: payload redacted from success text"
    );
    assert!(
        !tool_msg.contains("LEAKEDSIG"),
        "signed-url token redacted from success text"
    );
    assert!(tool_msg.contains("<redacted uri>"));
    // And it definitely did not reach the upstream as raw bytes anywhere.
    let serialized = serde_json::to_string(&requests[1]).expect("serialize");
    assert!(!serialized.contains("LEAKEDB64"));
    assert!(!serialized.contains("LEAKEDSIG"));
}

#[tokio::test]
async fn image_agent_strips_when_kimi_alias_remaps_to_text_backend() {
    // Round-4 #1: gating must enumerate candidates from the RESOLVED model the
    // upstream actually receives, not the raw request model. A Kimi-LOOKING
    // request alias whose profile `upstream_model` remaps to a TEXT backend must
    // STRIP (the old code judged "kimi-alias" by name and wrongly passed raw
    // images to the text backend). MockUpstream's candidate_backend_models
    // echoes the model it is given, so the resolved model drives the decision.
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["deepseek-v3"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let mut config = image_agent_config();
    // Profile on the request alias remaps to a text backend via upstream_model.
    config.model_profiles = std::collections::BTreeMap::from([(
        "kimi-fast".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: Some("deepseek-v3".to_string()),
            system_prompt_prefix: None,
            native_vision: None,
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "kimi-fast".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    // The upstream request must carry the text backend model and NO raw image.
    assert_eq!(requests[0].model, "deepseek-v3");
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "a kimi-alias remapped to a text backend must strip, not pass raw images"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(
        has_analyze,
        "remap-to-text-backend must inject analyzeImage"
    );
}

#[tokio::test]
async fn upstream_request_log_redacts_image_data_when_agent_disabled() {
    // Round-4 #3: with the image agent OFF, the raw image flows to the upstream,
    // but the on-disk JSONL request log must NOT contain the raw data: bytes.
    use std::io::Read;
    let log_dir = std::env::temp_dir().join(format!(
        "llmconduit-img-log-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&log_dir).expect("mkdir");
    let log_path = log_dir.join("upstream.jsonl");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [{ "id": "text-model", "object": "model" }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-1",
                    "choices": [{ "index": 0, "delta": { "content": "ok" }, "finish_reason": "stop" }]
                })])),
        )
        .mount(&server)
        .await;

    let mut config = test_config();
    config.brave_api_key = None;
    config.image_agent_enabled = false; // agent OFF
    // E2b: with the agent off AND a non-native backend, the engine's residual-
    // image pass now degrades the image to a text placeholder BEFORE it ever
    // reaches the upstream request/log — there would be no raw URI left to
    // redact. Force `native_vision: true` so the raw image still reaches the
    // wire (and thus the log), keeping this test's actual target intact: the
    // JSONL log redaction for a raw image that DOES flow through (still a
    // live path — native-vision passthrough is intentionally unstripped).
    config.model_profiles = std::collections::BTreeMap::from([(
        "text-model".to_string(),
        llmconduit::config::ModelProfile {
            native_vision: Some(true),
            ..Default::default()
        },
    )]);
    config.upstream_base_url = format!("{}/v1", server.uri()).parse().expect("url");
    config.upstream_request_log_path = Some(log_path.clone());
    let app = llmconduit::build_app(config);

    let body = json!({
        "model": "text-model",
        "stream": true,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                { "type": "input_text", "text": "what is this?" },
                { "type": "input_image", "image_url": TEST_IMAGE_DATA_URL }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body");
    // Await the spawn_blocking JSONL writer flushing via a bounded poll, so a
    // real regression fails fast instead of relying on elapsed real time.
    let contents = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let mut contents = String::new();
            if std::fs::File::open(&log_path)
                .and_then(|mut f| f.read_to_string(&mut contents))
                .is_ok()
                && !contents.is_empty()
            {
                break contents;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upstream JSONL log should become non-empty");
    let _ = std::fs::remove_dir_all(&log_dir);
    assert!(!contents.is_empty(), "log should have an entry");
    assert!(
        !contents.contains("iVBORw0KGgo"),
        "upstream JSONL log must not contain raw image base64"
    );
    assert!(
        contents.contains("<redacted uri>"),
        "image uri redacted in log"
    );
}

#[tokio::test]
async fn image_agent_request_native_vision_false_on_kimi_primary_strips() {
    // Round-7 #1 (a): an explicit `native_vision:false` profile on the REQUEST
    // model must force strip even when the primary candidate is Kimi-NAMED
    // (which the name sniff would call native).
    let upstream = MockUpstream::default();
    upstream.set_candidate_models(["Kimi-K2.6"]);
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "kimi-front".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            native_vision: Some(false),
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "kimi-front".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "native_vision:false on the request must strip even a Kimi-named primary"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(has_analyze, "native_vision:false must inject analyzeImage");
}

#[tokio::test]
async fn image_agent_request_native_vision_true_on_text_named_primary_passes_through() {
    // Round-7 #1 (b): an explicit `native_vision:true` on the REQUEST model must
    // pass images through to a non-native-NAMED primary (when ALL candidates are
    // native — here a single primary candidate).
    let upstream = MockUpstream::default();
    upstream.set_candidate_models(["my-multimodal-v1"]);
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "I see a square."))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "vision-front".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            native_vision: Some(true),
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "vision-front".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        serialized.contains("iVBORw0KGgo"),
        "native_vision:true on the request must pass the raw image through"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(
        !has_analyze,
        "native_vision:true passthrough must not inject analyzeImage"
    );
}

#[tokio::test]
async fn image_agent_request_native_vision_true_does_not_flip_nonnative_fallback() {
    // Round-7 #1 (c): a per-request `native_vision:true` must NOT make a
    // genuinely non-native FALLBACK candidate look native — the request override
    // applies to the primary only; the non-native fallback still forces strip.
    let upstream = MockUpstream::default();
    // Primary native-named, fallback a text-only model.
    upstream.set_candidate_models(["my-multimodal-v1", "deepseek-v3"]);
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "vision-front".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            native_vision: Some(true),
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "vision-front".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "a non-native fallback must force strip despite a per-request native_vision:true"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(has_analyze, "non-native fallback must inject analyzeImage");
}

#[tokio::test]
async fn client_tool_named_analyze_image_flows_through_chat_when_agent_inactive() {
    // Round-7 #2 (inactive): with the image agent OFF, a CLIENT tool literally
    // named `analyzeImage` must surface to the Chat client normally.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_user_1",
            "analyzeImage",
            "{\"q\":\"client-owned\"}",
        ))])
        .await;
    let mut config = test_config();
    config.brave_api_key = None;
    config.image_agent_enabled = false; // agent OFF
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": true,
        "messages": [{ "role": "user", "content": "use my tool" }],
        "tools": [{
            "type": "function",
            "function": {
                "name": "analyzeImage",
                "description": "client's own tool",
                "parameters": { "type": "object", "properties": { "q": { "type": "string" } } }
            }
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        text.contains("analyzeImage"),
        "an inactive-agent client tool named analyzeImage must reach the Chat client"
    );
    assert!(
        text.contains("client-owned"),
        "client tool arguments must surface"
    );
}

#[tokio::test]
async fn client_tool_named_analyze_image_flows_through_anthropic_when_agent_inactive() {
    // Round-7 #2 (inactive): same, through the Anthropic egress — the client
    // tool must become a tool_use block.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_user_1",
            "analyzeImage",
            "{\"q\":\"client-owned\"}",
        ))])
        .await;
    let mut config = test_config();
    config.brave_api_key = None;
    config.image_agent_enabled = false; // agent OFF
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "use my tool" }],
        "tools": [{
            "name": "analyzeImage",
            "description": "client's own tool",
            "input_schema": { "type": "object", "properties": { "q": { "type": "string" } } }
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        text.contains("analyzeImage"),
        "an inactive-agent client tool named analyzeImage must reach the Anthropic client"
    );
    assert!(
        text.contains("tool_use"),
        "client tool must become a tool_use block"
    );
}

// ===========================================================================
// G4 round-8: native-vision gating decision-table cells.
// ===========================================================================

#[tokio::test]
async fn gating_table_unmatched_request_override_does_not_apply_to_default() {
    // Cell (a) / round-8 #1 HIGH: a STALE/unmatched request alias carrying
    // native_vision:true normalizes to a DIFFERENT, non-native catalog default.
    // The request override must NOT attach to that default candidate ⇒ STRIP.
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["text-default-v1"]).await; // non-native default
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "stale-kimi-alias".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            native_vision: Some(true),
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    // Request a model NOT in the catalog → normalizes to text-default-v1.
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "stale-kimi-alias".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    assert_eq!(
        requests[0].model, "text-default-v1",
        "normalized to default"
    );
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "a stale-alias native_vision override must NOT pass raw images to the default backend"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(
        has_analyze,
        "default-fallback non-native ⇒ inject analyzeImage"
    );
}

#[tokio::test]
async fn gating_table_blank_request_override_does_not_apply_to_default() {
    // Round-8 #1 (blank-model half): a BLANK request model carrying a
    // `native_vision:true` profile keyed on the empty string must NOT attach
    // that override to the non-native catalog default. A blank request has no
    // model identity, so it cannot "genuinely map" to the default backend —
    // `genuine` must be false for a default-fallback regardless of blankness,
    // and the gate must STRIP. Regression guard for the T2 `genuine` flag
    // (engine.rs `normalize_upstream_model` blank branch).
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["text-default-v1"]).await; // non-native default
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        String::new(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            native_vision: Some(true),
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    // Blank model → normalizes to text-default-v1 (the catalog default).
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = String::new();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    assert_eq!(
        requests[0].model, "text-default-v1",
        "blank model normalized to default"
    );
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "a blank-model native_vision override must NOT pass raw images to the default backend"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(
        has_analyze,
        "blank-model default-fallback non-native ⇒ inject analyzeImage"
    );
}

#[tokio::test]
async fn gating_table_genuine_resolution_native_true_passes_through() {
    // Cell (b): request model GENUINELY resolves (exact catalog id) to a
    // non-native-NAMED primary with native_vision:true ⇒ passthrough.
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["my-multimodal-v1"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "I see a square."))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "my-multimodal-v1".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            native_vision: Some(true),
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "my-multimodal-v1".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        serialized.contains("iVBORw0KGgo"),
        "genuine resolution + native_vision:true ⇒ raw image passes through"
    );
}

#[tokio::test]
async fn gating_table_native_false_on_resolved_primary_strips() {
    // Cell (c): native_vision:false on the genuinely-resolved primary ⇒ STRIP
    // even when the model is Kimi-NAMED.
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["Kimi-K2.6"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "Kimi-K2.6".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            native_vision: Some(false),
            upstream_chat_kwargs: JsonMap::new(),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "Kimi-K2.6".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "native_vision:false on the resolved primary ⇒ strip"
    );
}

#[tokio::test]
async fn gating_table_all_native_candidates_pass_through() {
    // Cell (d): every candidate native (by name) ⇒ passthrough.
    let upstream = MockUpstream::default();
    upstream.set_candidate_models(["Kimi-K2.6", "kimi-vl-a3b"]);
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "I see a square."))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        serialized.contains("iVBORw0KGgo"),
        "all-native ⇒ passthrough"
    );
}

#[tokio::test]
async fn gating_table_any_nonnative_fallback_strips() {
    // Cell (e): one non-native fallback ⇒ STRIP (all-native invariant).
    let upstream = MockUpstream::default();
    upstream.set_candidate_models(["Kimi-K2.6", "deepseek-v3"]);
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "any non-native fallback ⇒ strip"
    );
}

#[tokio::test]
async fn gating_table_empty_candidate_set_strips() {
    // Cell (f): empty candidate set ⇒ STRIP (unknown state, safe default).
    let upstream = MockUpstream::default();
    upstream.set_candidate_models(Vec::<String>::new());
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );
    // Even a Kimi-looking request model must strip when candidates are unknown.
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "Kimi-K2.6".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "empty candidate set ⇒ strip"
    );
}

#[tokio::test]
async fn gating_table_candidate_lookup_uses_own_profile_not_remap_target() {
    // Round-9 #1 (double-remap): a fallback candidate is judged by ITS OWN
    // profile native_vision, NOT the profile of its `upstream_model` remap
    // target. Here the fallback `text-backend` is non-native (own profile
    // native_vision:false) but its remap target `kimi-native` is native — the
    // OLD re-remapping path would have wrongly called the fallback native and
    // passed raw images through. The fix must STRIP.
    let upstream = MockUpstream::default();
    upstream.set_candidate_models(["Kimi-K2.6", "text-backend"]);
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([
        (
            "text-backend".to_string(),
            llmconduit::config::ModelProfile {
                // Remap target points at a "native" model — must be IGNORED for
                // this candidate's native-vision decision.
                upstream_model: Some("kimi-native".to_string()),
                system_prompt_prefix: None,
                native_vision: Some(false), // the candidate's OWN truth
                upstream_chat_kwargs: JsonMap::new(),
                ..Default::default()
            },
        ),
        (
            "kimi-native".to_string(),
            llmconduit::config::ModelProfile {
                upstream_model: None,
                system_prompt_prefix: None,
                native_vision: Some(true), // remap target — must NOT leak in
                upstream_chat_kwargs: JsonMap::new(),
                ..Default::default()
            },
        ),
    ]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "a fallback's own native_vision:false must STRIP despite a native remap target"
    );
    let has_analyze = requests[0]
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"));
    assert!(
        has_analyze,
        "non-native fallback (own profile) ⇒ inject analyzeImage"
    );
}

#[tokio::test]
async fn gating_table_request_override_not_displaced_by_remap_target_profile() {
    // Round-9 #1: the request override consults the LITERAL request model's
    // profile, not the profile of its `upstream_model` remap target. Request
    // model `kimi-front` (native by name) sets native_vision:false on ITSELF but
    // remaps to `kimi-native` (native_vision:true). The OLD path could let the
    // remap target's true override displace the request's false ⇒ wrongly pass
    // raw images. The fix must honor the request's own false ⇒ STRIP.
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["kimi-front"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let mut config = image_agent_config();
    config.model_profiles = std::collections::BTreeMap::from([
        (
            "kimi-front".to_string(),
            llmconduit::config::ModelProfile {
                upstream_model: Some("kimi-native".to_string()),
                system_prompt_prefix: None,
                native_vision: Some(false), // request's OWN truth
                upstream_chat_kwargs: JsonMap::new(),
                ..Default::default()
            },
        ),
        (
            "kimi-native".to_string(),
            llmconduit::config::ModelProfile {
                upstream_model: None,
                system_prompt_prefix: None,
                native_vision: Some(true), // remap target — must NOT displace
                upstream_chat_kwargs: JsonMap::new(),
                ..Default::default()
            },
        ),
    ]);
    let gateway = test_gateway_with_vision(upstream.clone(), MockVisionClient::default(), config);
    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.model = "kimi-front".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "the request's own native_vision:false must STRIP, not be displaced by the remap target"
    );
}

#[tokio::test]
async fn upstream_chat_error_body_with_image_url_is_redacted_in_failed() {
    // Round-9 #2: an upstream 4xx whose error body echoes a data:/signed image
    // URL must surface a REDACTED response.failed message (and redacted logs) —
    // the raw image bytes / signed URL must not leak through the error path.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [{ "id": "text-model", "object": "model" }]
        })))
        .mount(&server)
        .await;
    // The provider echoes the submitted image back in its 400 body.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_string(
            "invalid request near data:image/png;base64,ERRBODYLEAK and https://signed.x/i?sig=ERRSIGLEAK",
        ))
        .mount(&server)
        .await;

    let mut config = test_config();
    config.brave_api_key = None;
    // Agent OFF. E2b now degrades this request's own image to a text
    // placeholder before dispatch (non-native backend), but that is orthogonal
    // to what this test targets: the LEAKED text below comes from the MOCK's
    // 400 error BODY, not from the request, so the placeholder swap does not
    // affect these assertions.
    config.image_agent_enabled = false;
    config.upstream_base_url = format!("{}/v1", server.uri()).parse().expect("url");
    let (_app, gateway) = llmconduit::build_app_with_gateway(config);

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let mut req = request;
    req.model = "text-model".to_string();
    let events = collect_stream(gateway.stream_responses(req).await.expect("stream")).await;

    let failed = events
        .iter()
        .find(|e| e["_event"] == "response.failed")
        .expect("expected a response.failed event");
    let message = failed["response"]["error"]["message"]
        .as_str()
        .unwrap_or_default();
    assert!(
        message.contains("upstream chat failed with 400"),
        "surfaces the upstream error"
    );
    assert!(
        !message.contains("ERRBODYLEAK"),
        "data: payload redacted from response.failed"
    );
    assert!(
        !message.contains("ERRSIGLEAK"),
        "signed-url token redacted from response.failed"
    );
    assert!(
        message.contains("<redacted uri>"),
        "image uris redacted in error body"
    );
}

// ===========================================================================
// E2b — residual-image safety pass: no raw image reaches a non-native-vision
// backend, regardless of whether the G4 agent above activated.
// ===========================================================================

fn file_id_image(file_id: &str) -> ContentItem {
    ContentItem::InputImage {
        image_url: None,
        file_id: Some(file_id.to_string()),
        detail: None,
    }
}

fn message_with_role_and_image(role: &str, image: ContentItem) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: role.to_string(),
        content: vec![image],
        phase: None,
    }
}

// --- AC-4: policy=Placeholder, non-native backend, agent inactive ---

#[tokio::test]
async fn e2b_placeholder_degrades_user_image_url() {
    // (a) image_url user image.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());

    let request = base_request(vec![user_message_with_image(
        "what is this?",
        TEST_IMAGE_DATA_URL,
    )]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(!serialized.contains("iVBORw0KGgo"), "no raw image bytes");
    assert!(!serialized.contains("image_url"), "no image part at all");
    assert!(serialized.contains("this model is text-only and cannot view images"));
    assert!(serialized.contains("1 image(s) were attached here"));
}

#[tokio::test]
async fn e2b_placeholder_degrades_user_file_id_image() {
    // (b) file_id user image -- the ACTIVE strip's own blind spot
    // (`vision/strip.rs` only matches `image_url: Some`).
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());

    let request = base_request(vec![message_with_role_and_image(
        "user",
        file_id_image("file-abc123"),
    )]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(!serialized.contains("file-abc123"), "no raw file_id");
    assert!(!serialized.contains("\"file_id\""), "no image part at all");
    assert!(serialized.contains("this model is text-only and cannot view images"));
}

#[tokio::test]
async fn e2b_placeholder_degrades_tool_output_image_from_anthropic_tool_result() {
    // (c) tool-output image: an Anthropic tool_result image lowers to a
    // `FunctionCallOutput` + a following synthetic user-role image message
    // (`adapters/anthropic_to_responses.rs:339`) -- exercised end-to-end
    // through the REAL Anthropic HTTP ingress, mirroring
    // `anthropic_messages_converts_tool_result_history` in `tests/gateway.rs`
    // but WITHOUT a native-vision override, so the image must degrade.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "It's 72F in Seattle."))])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [
            { "role": "user", "content": "What's the weather in Seattle?" },
            { "role": "assistant", "content": [
                { "type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": { "location": "Seattle" } }
            ]},
            { "role": "user", "content": [
                { "type": "tool_result", "tool_use_id": "toolu_1", "content": [
                    { "type": "text", "text": "72F sunny" },
                    { "type": "image", "source": { "type": "url", "url": "https://example.com/radar.png" } }
                ]}
            ]}
        ],
        "tools": [{
            "name": "get_weather",
            "description": "Get the weather",
            "input_schema": {
                "type": "object",
                "properties": { "location": { "type": "string" } },
                "required": ["location"]
            }
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("radar.png"),
        "no raw image URL upstream"
    );
    assert!(
        serialized.contains("the tool returned an image"),
        "tool-output wording expected: {serialized}"
    );
    // The DEFAULT wording must NOT be used for a tool-output continuation.
    assert!(!serialized.contains("ask the user to describe"));
}

#[tokio::test]
async fn e2b_placeholder_degrades_image_in_non_user_message() {
    // (d) an image in a NON-user message -- role-agnostic sweep.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());

    let request = base_request(vec![
        message_with_role_and_image(
            "assistant",
            ContentItem::InputImage {
                image_url: Some("data:image/png;base64,ASSISTANTRAW".to_string()),
                file_id: None,
                detail: None,
            },
        ),
        user_message("what did you just show me?"),
    ]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(!serialized.contains("ASSISTANTRAW"));
    assert!(serialized.contains("this model is text-only and cannot view images"));
}

// --- AC-5: determinism + replay bypass ---

#[tokio::test]
async fn e2b_lowering_same_request_twice_is_byte_identical() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "ok"))])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());

    let request = base_request(vec![user_message_with_image(
        "describe this",
        TEST_IMAGE_DATA_URL,
    )]);
    let _ = collect_stream(
        gateway
            .clone()
            .stream_responses(request.clone())
            .await
            .expect("stream"),
    )
    .await;
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(
        serde_json::to_value(&requests[0]).unwrap(),
        serde_json::to_value(&requests[1]).unwrap(),
        "the SAME inbound request lowered twice must produce byte-identical upstream JSON"
    );
    let serialized = serde_json::to_string(&requests[0]).unwrap();
    assert!(!serialized.contains("iVBORw0KGgo"));
}

#[tokio::test]
async fn e2b_multi_image_order_preserved_after_degrade() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());

    let message = ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![
            ContentItem::InputText {
                text: "A".to_string(),
            },
            ContentItem::InputImage {
                image_url: Some("data:image/png;base64,IMAGEONE".to_string()),
                file_id: None,
                detail: None,
            },
            ContentItem::InputText {
                text: "B".to_string(),
            },
            ContentItem::InputImage {
                image_url: Some("data:image/png;base64,IMAGETWO".to_string()),
                file_id: None,
                detail: None,
            },
            ContentItem::InputText {
                text: "C".to_string(),
            },
        ],
        phase: None,
    };
    let request = base_request(vec![message]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let content = serde_json::to_string(&requests[0].messages[0].content).expect("serialize");
    assert!(!content.contains("IMAGEONE") && !content.contains("IMAGETWO"));
    // Sequential search from the previous match proves the ORIGINAL relative
    // order (text, placeholder, text, placeholder, text) survived, without
    // assuming a specific flattened-vs-array JSON shape.
    let pos_a = content.find('A').expect("A present");
    let pos_ph1 = content[pos_a..]
        .find("text-only")
        .map(|p| p + pos_a)
        .expect("first placeholder present");
    let pos_b = content[pos_ph1..]
        .find('B')
        .map(|p| p + pos_ph1)
        .expect("B present");
    let pos_ph2 = content[pos_b..]
        .find("text-only")
        .map(|p| p + pos_b)
        .expect("second placeholder present");
    let pos_c = content[pos_ph2..]
        .find('C')
        .map(|p| p + pos_ph2)
        .expect("C present");
    assert!(
        pos_a < pos_ph1 && pos_ph1 < pos_b && pos_b < pos_ph2 && pos_ph2 < pos_c,
        "order not preserved: {content}"
    );
    assert!(content.contains("2 image(s) were attached here"));
}

#[tokio::test]
async fn e2b_degraded_turn_does_not_write_to_replay_cache() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "42F"))])
        .await;
    let replay_store = ReplayStore::new(1000);
    let gateway = test_gateway_with_config_and_replay_store(
        upstream.clone(),
        MockSearch::default(),
        test_config(),
        replay_store.clone(),
    );

    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.store = true; // caller opts in; the degrade must override this.
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));

    assert!(
        replay_store.is_empty().await,
        "a degraded turn must never be written to the replay cache, \
         even when the caller requested store=true"
    );
}

#[tokio::test]
async fn e2b_degraded_turn_does_not_read_from_replay_cache() {
    // Prove two DISTINCT images at the same position do not collide: seed a
    // baseline whose key is EXACTLY what this turn's degraded history would
    // hash to (computed via the SAME public `degrade_residual_images` fn the
    // engine calls, so this does not hardcode placeholder wording separately
    // from the implementation), carrying a distinctive marker that must NOT
    // leak into the dispatched request if the lookup is correctly bypassed.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let replay_store = ReplayStore::new(1000);

    let mut request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    request.store = true;
    let mut would_be_degraded = request.input.clone();
    llmconduit::vision::degrade_residual_images(&mut would_be_degraded);
    replay_store
        .insert(ReplayRecord {
            model: request.model.clone(),
            instructions: request.instructions.clone(),
            visible_history: would_be_degraded,
            internal_messages: vec![ChatMessage {
                role: "system".to_string(),
                content: Some(json!("POISONED_BASELINE_MARKER_ZZZ")),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
        })
        .await;
    assert_eq!(replay_store.len().await, 1);

    let gateway = test_gateway_with_config_and_replay_store(
        upstream.clone(),
        MockSearch::default(),
        test_config(),
        replay_store.clone(),
    );
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("POISONED_BASELINE_MARKER_ZZZ"),
        "a degraded turn must not read a pre-existing replay baseline, \
         even an exactly-hash-matching one"
    );
}

// --- AC-7: no regression to the active-agent path / native-vision passthrough ---

#[tokio::test]
async fn e2b_active_agent_degrades_residual_image_without_double_transform() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );

    // History: an OLDER turn's file_id image (the ACTIVE strip's blind spot)
    // plus the LATEST user turn's normal image_url image (what the active
    // agent DOES strip+cache+offer analyzeImage for).
    let request = base_request(vec![
        message_with_role_and_image("user", file_id_image("file-old-residual")),
        ResponseItem::message_text("assistant", "ok, what next?"),
        user_message_with_image("look at this one", TEST_IMAGE_DATA_URL),
    ]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    // The residual file_id image is degraded by E2b (no raw file_id leaked).
    assert!(!serialized.contains("file-old-residual"));
    assert!(serialized.contains("text-only and cannot view images"));
    // The ACTIVE agent's own placeholder for the LATEST image survives
    // untouched -- not double-transformed into the E2b wording.
    assert!(serialized.contains("[Image #1]"));
    assert!(serialized.contains("analyzeImage"));
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "no raw bytes leaked either way"
    );
}

#[tokio::test]
async fn e2b_native_vision_passthrough_skips_residual_pass_entirely() {
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["Kimi-K2.6"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "I see it"))])
        .await;
    let gateway = test_gateway_with_vision(
        upstream.clone(),
        MockVisionClient::default(),
        image_agent_config(),
    );

    // A residual image in a NON-user message -- exactly what E2b would catch
    // on a non-native backend. On native-vision passthrough the WHOLE
    // residual pass must be skipped, so this must ALSO survive untouched.
    let mut request = base_request(vec![
        message_with_role_and_image(
            "assistant",
            ContentItem::InputImage {
                image_url: Some("data:image/png;base64,ASSISTANTBYTES".to_string()),
                file_id: None,
                detail: None,
            },
        ),
        user_message_with_image("look", TEST_IMAGE_DATA_URL),
    ]);
    request.model = "Kimi-K2.6".to_string();
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        serialized.contains("ASSISTANTBYTES"),
        "native-vision must forward the non-user residual image untouched"
    );
    assert!(
        serialized.contains("iVBORw0KGgo"),
        "native-vision must forward the latest user image untouched"
    );
    assert!(
        !serialized.contains("text-only and cannot view images"),
        "residual pass must not fire on native-vision passthrough"
    );
}

// --- AC-8: policy=Reject fails pre-dispatch with a 4xx, never 502 ---

#[tokio::test]
async fn e2b_reject_policy_anthropic_returns_4xx_not_502_and_skips_provider() {
    let upstream = MockUpstream::default();
    let mut config = test_config();
    config.unsupported_image_policy = UnsupportedImagePolicy::Reject;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "text-model",
        "max_tokens": 1024,
        "stream": false,
        "messages": [{
            "role": "user",
            "content": [
                { "type": "text", "text": "look" },
                { "type": "image", "source": { "type": "url", "url": "https://example.com/x.png" } }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("body");
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");

    assert_eq!(
        status, 400,
        "Reject must fail pre-dispatch with a 4xx, never 502"
    );
    assert_eq!(parsed["type"], "error");
    assert_eq!(parsed["error"]["type"], "invalid_request_error");
    assert!(
        parsed["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("text-only"),
        "structured Anthropic error body: {parsed}"
    );
    assert!(
        upstream.requests().await.is_empty(),
        "provider must never be contacted on Reject"
    );
}

#[tokio::test]
async fn e2b_reject_policy_chat_returns_4xx_not_502_and_skips_provider() {
    let upstream = MockUpstream::default();
    let mut config = test_config();
    config.unsupported_image_policy = UnsupportedImagePolicy::Reject;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [{
            "role": "user",
            "content": [
                { "type": "text", "text": "look" },
                { "type": "image_url", "image_url": { "url": "https://example.com/x.png" } }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("body");
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");

    assert_eq!(
        status, 400,
        "Reject must fail pre-dispatch with a 4xx, never 502"
    );
    assert!(
        parsed["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("text-only"),
        "structured error body: {parsed}"
    );
    assert!(
        upstream.requests().await.is_empty(),
        "provider must never be contacted on Reject"
    );
}

#[tokio::test]
async fn e2b_reject_policy_responses_returns_4xx_not_502_and_skips_provider() {
    let upstream = MockUpstream::default();
    let mut config = test_config();
    config.unsupported_image_policy = UnsupportedImagePolicy::Reject;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                { "type": "input_text", "text": "look" },
                { "type": "input_image", "image_url": "https://example.com/x.png" }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("body");
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");

    assert_eq!(
        status, 400,
        "Reject must fail pre-dispatch with a 4xx, never 502"
    );
    assert!(
        parsed["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("text-only"),
        "structured error body: {parsed}"
    );
    assert!(
        upstream.requests().await.is_empty(),
        "provider must never be contacted on Reject"
    );
}

// --- AC-9: no image bytes anywhere for a degraded turn ---

#[tokio::test]
async fn e2b_ac9_no_image_bytes_in_upstream_jsonl_log_for_degraded_turn() {
    use std::io::Read;
    let log_dir = std::env::temp_dir().join(format!(
        "llmconduit-e2b-log-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&log_dir).expect("mkdir");
    let log_path = log_dir.join("upstream.jsonl");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [{ "id": "text-model", "object": "model" }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-1",
                    "choices": [{ "index": 0, "delta": { "content": "ok" }, "finish_reason": "stop" }]
                })])),
        )
        .mount(&server)
        .await;

    // Default config: agent inactive, non-native backend -- the field
    // incident's exact shape. Placeholder policy is the default.
    let mut config = test_config();
    config.brave_api_key = None;
    config.upstream_base_url = format!("{}/v1", server.uri()).parse().expect("url");
    config.upstream_request_log_path = Some(log_path.clone());
    let app = llmconduit::build_app(config);

    let body = json!({
        "model": "text-model",
        "stream": true,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                { "type": "input_text", "text": "what is this?" },
                { "type": "input_image", "image_url": TEST_IMAGE_DATA_URL }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body");

    let contents = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let mut contents = String::new();
            if std::fs::File::open(&log_path)
                .and_then(|mut f| f.read_to_string(&mut contents))
                .is_ok()
                && !contents.is_empty()
            {
                break contents;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upstream JSONL log should become non-empty");
    let _ = std::fs::remove_dir_all(&log_dir);

    assert!(!contents.is_empty(), "log should have an entry");
    assert!(
        !contents.contains("iVBORw0KGgo"),
        "no raw image base64 in the degraded turn's upstream log"
    );
    assert!(
        contents.contains("text-only and cannot view images"),
        "the placeholder text (not the image) reached the wire: {contents}"
    );
}

#[tokio::test]
async fn e2b_ac9_no_image_bytes_in_failed_error_text_for_degraded_turn() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Err(llmconduit::error::AppError::upstream(
            "upstream exploded before first token",
        ))])
        .await;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let failed = events
        .iter()
        .find(|e| e["_event"] == "response.failed")
        .expect("expected a response.failed event");
    let message = failed["response"]["error"]["message"]
        .as_str()
        .unwrap_or_default();
    assert!(
        !message.contains("iVBORw0KGgo"),
        "no raw image bytes in the failed-turn error text"
    );
    assert!(
        !message.contains("data:image"),
        "no image data URI scheme in the failed-turn error text"
    );
}
