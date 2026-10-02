//! Acceptance tests for the upstream SSE per-frame DoS guard (gap G6), exercised
//! through the real `ReqwestUpstreamClient` upstream-read path.
//!
//! llmconduit reads the UPSTREAM SSE response via
//! `response.bytes_stream().eventsource()` in `upstream::stream_success_response`.
//! The `eventsource-stream` parser accumulates every byte it receives into an
//! internal buffer and only flushes on an SSE event boundary (a blank line); it
//! does NOT cap that buffer, so a hostile/buggy upstream streaming an oversized
//! or never-terminated frame would grow memory without bound. The configured inbound cap in
//! `http.rs` is the INBOUND request-body limit and does not cover this
//! response-read path. We therefore wrap the byte stream in a configurable
//! per-frame ceiling (`sse_guard::SseFrameGuard`, default 8 MiB, env
//! `LLMCONDUIT_MAX_SSE_FRAME_BYTES`) that rejects an over-cap frame as a clean
//! `AppError` before the parser over-accumulates.
//!
//! The guard's byte-accounting state machine is unit-tested white-box in
//! `src/sse_guard.rs`; these tests prove the guard is WIRED into the real client
//! end-to-end through `eventsource()` — normal streaming is unaffected and an
//! oversized frame surfaces as an error rather than a silent truncation.

use futures::StreamExt;
use llmconduit::models::chat::ChatCompletionRequest;
use llmconduit::models::chat::ChatMessage;
use llmconduit::upstream::ReqwestUpstreamClient;
use llmconduit::upstream::UpstreamClient;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

/// A complete, well-formed SSE event terminated by the blank-line boundary.
fn frame(data: &str) -> String {
    format!("data: {data}\n\n")
}

/// Build a one-message streaming chat request for the real upstream client.
fn chat_request() -> ChatCompletionRequest {
    ChatCompletionRequest {
        model: "test-model".to_string(),
        messages: vec![ChatMessage {
            role: "user".to_string(),
            content: Some(serde_json::json!("hello")),
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
        reasoning_effort: None,
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

/// A normal small-framed upstream SSE stream is parsed and yielded as chunks:
/// the cap is not triggered and streaming works through the real
/// `ReqwestUpstreamClient` -> `bounded_sse_byte_stream` -> `eventsource()` chain.
#[tokio::test]
async fn real_upstream_normal_stream_is_unaffected() {
    let server = MockServer::start().await;
    let body = format!(
        "{}{}{}",
        frame(
            r#"{"id":"c","object":"chat.completion.chunk","created":0,"model":"m","choices":[{"index":0,"delta":{"content":"hello"}}]}"#
        ),
        frame(
            r#"{"id":"c","object":"chat.completion.chunk","created":0,"model":"m","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":"stop"}]}"#
        ),
        "data: [DONE]\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(body, "text/event-stream"),
        )
        .mount(&server)
        .await;

    // Generous cap: normal frames are well under it.
    let client = ReqwestUpstreamClient::with_options(
        reqwest::Client::new(),
        format!("{}/v1/", server.uri()).parse().expect("url"),
        None,
        None,
        true,
        4096,
        1024 * 1024,
    );

    let mut stream = client
        .stream_chat_completion(&llmconduit::upstream::BackendChatRequest::new(
            chat_request(),
            None,
            None,
            None,
        ))
        .await
        .expect("stream opens");

    let mut contents = Vec::new();
    while let Some(item) = stream.next().await {
        let chunk = item.expect("normal frames parse without error");
        if let Some(choice) = chunk.choices.first()
            && let Some(content) = &choice.delta.content
        {
            contents.push(content.clone());
        }
    }
    assert_eq!(contents, vec!["hello".to_string(), " world".to_string()]);
}

/// An oversized SINGLE upstream SSE frame surfaces as an `AppError` through the
/// real client's `eventsource()` chain — proving the `SseFrameGuard` is wired
/// into `stream_success_response`, not merely tested in isolation. The frame is
/// well-formed SSE but its single data line exceeds the configured cap, so it is
/// rejected before the parser can buffer the whole thing.
#[tokio::test]
async fn real_upstream_oversized_frame_is_rejected_as_error() {
    let server = MockServer::start().await;
    // One enormous data line (no early boundary) ~512 KiB, then its terminator.
    let huge = "x".repeat(512 * 1024);
    let body = format!("data: {huge}\n\n");
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(body, "text/event-stream"),
        )
        .mount(&server)
        .await;

    // Cap of 64 KiB (floored value) is far below the 512 KiB frame.
    let client = ReqwestUpstreamClient::with_options(
        reqwest::Client::new(),
        format!("{}/v1/", server.uri()).parse().expect("url"),
        None,
        None,
        true,
        4096,
        64 * 1024,
    );

    let mut stream = client
        .stream_chat_completion(&llmconduit::upstream::BackendChatRequest::new(
            chat_request(),
            None,
            None,
            None,
        ))
        .await
        .expect("stream opens");

    // The chain must yield an error (never silently truncate to a clean end).
    let mut saw_error = false;
    while let Some(item) = stream.next().await {
        if let Err(err) = item {
            assert!(
                err.to_string().contains("SSE"),
                "rejection should be an upstream SSE read error, got: {err}"
            );
            saw_error = true;
            break;
        }
    }
    assert!(
        saw_error,
        "an oversized upstream SSE frame must surface as an AppError"
    );
}
