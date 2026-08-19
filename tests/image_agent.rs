//! G4 - Image agent (per-profile vision offload) integration tests.
//!
//! A profile that declares `image_analysis` opts its (text-only) backend into
//! the in-proxy vision offload: images in the latest user turn are stripped to
//! `[Image #N]` placeholders, cached, and an `analyzeImage` server tool is
//! injected. When the model calls `analyzeImage`, the engine resolves the cached
//! image(s) and dispatches them to the analyzer model THROUGH the gateway's own
//! upstreams (the router resolves that model like any request), then feeds the
//! description back into the chat history as a tool result. A profile WITHOUT
//! `image_analysis` is native passthrough: images flow to the upstream untouched.
//!
//! Because the analyzer is dispatched over the gateway's own upstreams, the
//! suite asserts against the mock upstream (or, for the routing test, a second
//! wiremock server) rather than a dedicated vision-client mock. Shared
//! gateway/config/request builders live in `tests/common`.

mod common;

use common::MockSearch;
use common::MockUpstream;
use common::TEST_IMAGE_DATA_URL;
use common::base_request;
use common::chat_completion_sse_body;
use common::collect_stream;
use common::content_chunk;
use common::event_names;
use common::image_agent_gateway;
use common::image_analysis_config;
use common::test_config;
use common::test_gateway_with_config;
use common::test_gateway_with_config_and_replay_store;
use common::tool_call_chunk;
use common::user_message;
use common::user_message_with_image;

use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use llmconduit::config::ResidualImagePolicy;
use llmconduit::error::AppError;
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
use serde_json::json;
use tower::ServiceExt;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

// ---------------------------------------------------------------------------
// Local helpers.
// ---------------------------------------------------------------------------

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

/// Whether the recorded chat request offers the injected `analyzeImage` tool.
fn offers_analyze_image(request: &llmconduit::models::chat::ChatCompletionRequest) -> bool {
    request
        .tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|t| t.function.name == "analyzeImage"))
}

// ===========================================================================
// Active path: a profile WITH `image_analysis` strips + offloads.
// ===========================================================================

#[tokio::test]
async fn image_analysis_strips_offloads_to_analyzer_then_answers() {
    // Round 1: the text model calls analyzeImage. The analyzer dispatch (a
    // normal internal chat request to the `analyzer` model) answers with the
    // description. Round 2: the text model answers using it. Three upstream
    // calls; analyzeImage never leaks to the client.
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
            "vis-1",
            "A small red square on white.",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk(
            "chat-2",
            "The image shows a red square.",
        ))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );

    let request = base_request(vec![user_message_with_image(
        "what is this?",
        TEST_IMAGE_DATA_URL,
    )]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(
        requests.len(),
        3,
        "text round 1 + analyzer dispatch + text round 2"
    );

    // Round 1 to the text model carries only the placeholder, never the bytes,
    // plus the injected analyzeImage tool.
    let round1 = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !round1.contains("iVBORw0KGgo"),
        "raw image base64 must not reach the text model"
    );
    assert!(offers_analyze_image(&requests[0]));

    // The analyzer dispatch (round 2 recorded) is a chat request to the analyzer
    // model carrying the raw cached image.
    assert_eq!(requests[1].model, "analyzer", "analyzer model dispatched");
    let analyzer_body = serde_json::to_string(&requests[1]).expect("serialize");
    assert!(
        analyzer_body.contains("iVBORw0KGgo"),
        "the analyzer receives the raw cached image: {analyzer_body}"
    );

    // The text model's continuation (round 3 recorded) carries the description
    // as a tool result.
    let tool_msg = requests[2]
        .messages
        .iter()
        .find(|m| m.role == "tool")
        .and_then(|m| m.content.as_ref())
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert_eq!(tool_msg, "A small red square on white.");

    // analyzeImage never surfaces in the public Responses stream.
    let names = event_names(&events);
    assert!(!names.contains(&"response.function_call_arguments.delta"));
    for event in &events {
        if event["_event"] == "response.output_item.done" {
            assert_ne!(event["item"]["name"].as_str(), Some("analyzeImage"));
        }
    }
    let answer: String = events
        .iter()
        .filter(|e| e["_event"] == "response.output_text.delta")
        .filter_map(|e| e["delta"].as_str())
        .collect();
    assert!(answer.contains("red square"));
}

#[tokio::test]
async fn analysis_call_lands_on_analyzer_profile_upstream_with_served_model() {
    // The single required routing assertion (controller 5a): the analyzeImage
    // call is dispatched through the gateway's own upstreams by the analyzer
    // profile's model name, so it lands on the ANALYZER profile's upstream and
    // posts that profile's served model. Two independent wiremock servers.
    let text_server = MockServer::start().await;
    // Round 1 on the text upstream: emit an analyzeImage tool call.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-1",
                    "choices": [{
                        "index": 0,
                        "delta": { "tool_calls": [{
                            "id": "call_img_1",
                            "index": 0,
                            "type": "function",
                            "function": {
                                "name": "analyzeImage",
                                "arguments": "{\"imageId\":[\"1\"],\"task\":\"describe\"}"
                            }
                        }]},
                        "finish_reason": "tool_calls"
                    }]
                })])),
        )
        .up_to_n_times(1)
        .mount(&text_server)
        .await;
    // Round 2 on the text upstream: the final answer.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-2",
                    "choices": [{"index": 0, "delta": {"content": "It is a red square."}, "finish_reason": "stop"}]
                })])),
        )
        .mount(&text_server)
        .await;

    let vision_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "vis-1",
                    "choices": [{"index": 0, "delta": {"content": "A red square."}, "finish_reason": "stop"}]
                })])),
        )
        .mount(&vision_server)
        .await;

    let config = common::config_from_yaml(&format!(
        "upstreams:\n  \
           - name: text_up\n    url: \"{text}/v1/\"\n  \
           - name: vision_up\n    url: \"{vision}/v1/\"\n\
         model_profiles:\n  \
           \"glm-5.1\":\n    upstream: text_up\n    image_analysis:\n      model: \"vision-analyzer\"\n  \
           \"vision-analyzer\":\n    upstream: vision_up\n    upstream_model: \"served-vision-model\"\n",
        text = text_server.uri(),
        vision = vision_server.uri(),
    ));
    let (_app, gateway) = llmconduit::build_app_with_gateway(config);

    let request = base_request(vec![user_message_with_image(
        "what is this?",
        TEST_IMAGE_DATA_URL,
    )]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    // Filter to chat-completions POSTs: the routing client may also probe
    // `/v1/models` on the upstream, which is not the analysis call.
    let analyzer_posts: Vec<_> = vision_server
        .received_requests()
        .await
        .expect("vision server recorded requests")
        .into_iter()
        .filter(|req| req.url.path().ends_with("/chat/completions"))
        .collect();
    assert_eq!(
        analyzer_posts.len(),
        1,
        "the analyzer upstream serves the analysis call exactly once"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&analyzer_posts[0].body).expect("analyzer body is JSON");
    assert_eq!(
        body["model"].as_str(),
        Some("served-vision-model"),
        "the analyzer profile's served model is posted"
    );
    let body_text = serde_json::to_string(&body).expect("serialize");
    assert!(
        body_text.contains("iVBORw0KGgo"),
        "the analyzer upstream receives the raw image: {body_text}"
    );
}

#[tokio::test]
async fn image_analysis_strips_raw_image_bytes_from_upstream() {
    // The text backend must NEVER receive raw image bytes: round 1 carries only
    // the [Image #N] placeholder plus the injected analyzeImage tool.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "I cannot see images."))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("iVBORw0KGgo"),
        "raw image base64 must not reach the text upstream"
    );
    assert!(serialized.contains("[Image #1]"), "placeholder present");
    assert!(offers_analyze_image(&requests[0]));
}

#[tokio::test]
async fn image_analysis_handles_multiple_image_ids() {
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
        .push_response(vec![Ok(content_chunk("vis-1", "They differ."))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "Both differ."))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );

    let request = base_request(vec![ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![
            ContentItem::InputText {
                text: "compare".to_string(),
            },
            ContentItem::InputImage {
                image_url: Some("data:image/png;base64,AAAAMARKER".to_string()),
                file_id: None,
                detail: None,
            },
            ContentItem::InputImage {
                image_url: Some("data:image/png;base64,BBBBMARKER".to_string()),
                file_id: None,
                detail: None,
            },
        ],
        phase: None,
    }]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    // The analyzer dispatch (recorded second) carries BOTH cached images.
    let requests = upstream.requests().await;
    let analyzer_body = serde_json::to_string(&requests[1]).expect("serialize");
    assert!(analyzer_body.contains("AAAAMARKER"));
    assert!(analyzer_body.contains("BBBBMARKER"));
}

#[tokio::test]
async fn image_analysis_cache_miss_becomes_model_visible_text() {
    // The model asks for an image id that was never cached (#5 when only #1
    // exists). The executor injects a "no cached image" message and never
    // dispatches the analyzer, so there are only two upstream calls.
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
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    assert!(event_names(&events).contains(&"response.completed"));
    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 2, "analyzer not dispatched on a cache miss");
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
async fn analyzer_failure_surfaces_as_tool_result_error_without_killing_turn() {
    // Controller 5e: an analyzer dispatch failure degrades to model-visible tool
    // text (matching the Brave web_search contract) so the turn still completes.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_img_1",
            "analyzeImage",
            "{\"imageId\":[\"1\"],\"task\":\"x\"}",
        ))])
        .await;
    // The analyzer dispatch stream yields an error.
    upstream
        .push_response(vec![Err(AppError::upstream("analyzer backend exploded"))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "Sorry, no image."))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let names = event_names(&events);
    assert!(names.contains(&"response.completed"));
    assert!(!names.contains(&"response.failed"), "turn must not fail");
    let requests = upstream.requests().await;
    let tool_msg = requests[2]
        .messages
        .iter()
        .find(|m| m.role == "tool")
        .and_then(|m| m.content.as_ref())
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        tool_msg.contains("Vision analysis failed"),
        "analyzer failure becomes model-visible tool text: {tool_msg}"
    );
}

#[tokio::test]
async fn image_analysis_redacts_data_url_in_successful_analyzer_text() {
    // A SUCCESSFUL analyzer description that echoes a data:/signed URL must be
    // redacted before it is injected as the tool result (and logged).
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
        .push_response(vec![Ok(content_chunk(
            "vis-1",
            "It shows data:image/png;base64,LEAKEDB64 and a logo at https://signed.example.com/x?sig=LEAKEDSIG",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "Done."))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let tool_msg = requests[2]
        .messages
        .iter()
        .find(|m| m.role == "tool")
        .and_then(|m| m.content.as_ref())
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(!tool_msg.contains("LEAKEDB64"));
    assert!(!tool_msg.contains("LEAKEDSIG"));
    assert!(tool_msg.contains("<redacted uri>"));
}

#[tokio::test]
async fn image_analysis_keeps_parallel_tool_calls_false_upstream() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "no images visible"))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );
    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    assert_eq!(
        requests[0].parallel_tool_calls,
        Some(false),
        "parallel_tool_calls must stay false for the gateway-owned server tool"
    );
}

#[tokio::test]
async fn image_analysis_rejects_mixed_client_and_analyze_image() {
    // analyzeImage (server) + a client function tool in the same batch is
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
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
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
async fn image_analysis_not_leaked_in_chat_completions() {
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
        .push_response(vec![Ok(content_chunk("vis-1", "A cat."))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "A cat."))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );
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
    assert!(!text.contains("analyzeImage"), "analyzeImage must not leak");
    assert!(text.contains("A cat."));
}

#[tokio::test]
async fn image_analysis_not_leaked_in_anthropic_messages() {
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
        .push_response(vec![Ok(content_chunk("vis-1", "A dog."))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "A dog."))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );
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
    assert!(!text.contains("analyzeImage"), "analyzeImage must not leak");
    assert!(
        !text.contains("tool_use"),
        "no tool_use block for the internal tool"
    );
    assert!(text.contains("A dog."));
}

// ===========================================================================
// Passthrough: a profile WITHOUT `image_analysis` never mutates the request.
// ===========================================================================

#[tokio::test]
async fn no_image_analysis_passes_data_url_through_unmodified() {
    // Controller 5b: a profile with no `image_analysis` is native passthrough.
    // The data-URL image reaches the profile's upstream UNMODIFIED, even though
    // the model name ("glm-5.1") is not a known vision model - the name sniff is
    // gone.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "I can see it."))])
        .await;
    // `test_config()`'s catch-all profile declares no `image_analysis`.
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), test_config());

    let request = base_request(vec![user_message_with_image("look", TEST_IMAGE_DATA_URL)]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1, "single passthrough round");
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        serialized.contains("iVBORw0KGgo"),
        "the raw image survives to the upstream unmodified: {serialized}"
    );
    assert!(
        !serialized.contains("[Image #1]"),
        "no placeholder rewrite on the passthrough path"
    );
    assert!(
        !offers_analyze_image(&requests[0]),
        "passthrough must not inject analyzeImage"
    );
}

#[tokio::test]
async fn image_analysis_skips_when_no_image_present() {
    // A pure-text turn to an `image_analysis` profile must NOT be saddled with
    // the analyzeImage tool + mandatory-analyze system prompt.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );
    let request = base_request(vec![user_message("just text")]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let requests = upstream.requests().await;
    assert!(
        !offers_analyze_image(&requests[0]),
        "no-image turn must not activate the agent"
    );
}

// ===========================================================================
// Residual sweep: policy from the profile's `image_analysis.residual_images`.
// Only images the strip did not consume (file_id, non-user role, old history).
// ===========================================================================

#[tokio::test]
async fn reject_policy_file_id_in_old_history_fails_4xx_before_dispatch() {
    // Controller 5c: with `residual_images: reject`, a `file_id` image in older
    // history (the active strip only rewrites `image_url` user images) fails the
    // turn with a 4xx BEFORE any upstream call.
    let upstream = MockUpstream::default();
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Reject),
    );

    let request = base_request(vec![
        message_with_role_and_image("user", file_id_image("file-old-abc123")),
        ResponseItem::message_text("assistant", "ok"),
        user_message("what was that?"),
    ]);
    let err = gateway
        .stream_responses(request)
        .await
        .expect_err("reject must fail the turn");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    assert!(
        err.to_string().contains("text-only"),
        "message names the text-only backend: {err}"
    );
    assert!(
        upstream.requests().await.is_empty(),
        "the provider must never be contacted on Reject"
    );
}

#[tokio::test]
async fn reject_policy_responses_ingress_returns_4xx_not_502() {
    // The Reject 4xx surfaces as a structured error through the /v1/responses
    // ingress, never a 502 (the provider is not contacted, so never cooled).
    let upstream = MockUpstream::default();
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Reject),
    );
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                { "type": "input_text", "text": "look" },
                { "type": "input_image", "file_id": "file-xyz" }
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
    assert_eq!(status, 400, "Reject must fail pre-dispatch with a 4xx");
    assert!(
        parsed["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("text-only"),
        "structured error body: {parsed}"
    );
    assert!(upstream.requests().await.is_empty());
}

#[tokio::test]
async fn placeholder_policy_degrades_file_id_image() {
    // The default `placeholder` policy degrades a residual `file_id` image (the
    // active strip's blind spot) to a stable text placeholder, so no raw file
    // reference reaches the text-only backend.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );

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
async fn placeholder_policy_degrades_image_in_non_user_message() {
    // Role-agnostic: a residual image in a NON-user message is swept too.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );

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
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(!serialized.contains("ASSISTANTRAW"));
    assert!(serialized.contains("this model is text-only and cannot view images"));
}

#[tokio::test]
async fn active_agent_degrades_old_residual_but_strips_latest_image() {
    // The two seams cooperate: the LATEST user image_url is stripped+cached
    // (analyzeImage-referenceable), while an OLDER `file_id` residual the strip
    // cannot see is degraded to a placeholder by the sweep.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "cannot see"))])
        .await;
    let gateway = image_agent_gateway(
        upstream.clone(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
    );

    let request = base_request(vec![
        message_with_role_and_image("user", file_id_image("file-old-residual")),
        ResponseItem::message_text("assistant", "ok, what next?"),
        user_message_with_image("look at this one", TEST_IMAGE_DATA_URL),
    ]);
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(!serialized.contains("iVBORw0KGgo"), "latest image stripped");
    assert!(
        !serialized.contains("file-old-residual"),
        "old file_id degraded"
    );
    assert!(
        serialized.contains("[Image #1]"),
        "latest image placeholder"
    );
    assert!(serialized.contains("this model is text-only and cannot view images"));
}

#[tokio::test]
async fn degraded_turn_does_not_write_to_replay_cache() {
    // A degraded turn (a residual `file_id` image collapses to a positionally
    // deterministic placeholder that two DISTINCT images would share) must bypass
    // the replay cache, even when the caller opts in with store=true.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let replay_store = ReplayStore::new(1000);
    let gateway = test_gateway_with_config_and_replay_store(
        upstream.clone(),
        MockSearch::default(),
        image_analysis_config(ResidualImagePolicy::Placeholder),
        replay_store.clone(),
    );

    let mut request = base_request(vec![message_with_role_and_image(
        "user",
        file_id_image("file-abc123"),
    )]);
    request.store = true;
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));
    assert!(
        replay_store.is_empty().await,
        "a degraded turn must never be written to the replay cache"
    );
}

#[tokio::test]
async fn degraded_turn_does_not_read_from_replay_cache() {
    // A degraded turn must bypass the replay LOOKUP too, so a pre-existing
    // baseline whose key exactly matches the degraded history is never served.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let replay_store = ReplayStore::new(1000);

    let mut request = base_request(vec![message_with_role_and_image(
        "user",
        file_id_image("file-abc123"),
    )]);
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
        image_analysis_config(ResidualImagePolicy::Placeholder),
        replay_store.clone(),
    );
    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let serialized = serde_json::to_string(&requests[0]).expect("serialize");
    assert!(
        !serialized.contains("POISONED_BASELINE_MARKER_ZZZ"),
        "a degraded turn must not read a pre-existing replay baseline"
    );
}
