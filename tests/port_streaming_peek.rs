//! G3 keep-alive-peek contract (the *second half* of G3, separate from G3's
//! pre-flight context budgeting). This file was promoted to a dedicated gap
//! ("G3-peek") because the
//! behavior is redundant with G8 + axum's SSE keep-alive — see the plan.
//!
//! claude-relay's `_peek_with_keepalive` did two things for a streaming turn:
//! it sent SSE keep-alive comment frames while the upstream was silent, and it
//! buffered a reasoning-only stream until the first client-visible output (or
//! `[DONE]`), promoting/holding the reasoning accordingly. After studying the
//! llmconduit architecture, BOTH halves already exist here — built differently —
//! so the contracted, testable behavior is to LOCK what exists, not to port a
//! redundant peek (a re-implemented peek would duplicate the egress converter
//! and add cancellation complexity to the engine for no observable gain).
//!
//! SCOPE OF THIS FILE (deliberately narrow): the two surfaces that genuinely
//! provide the G3 behavior in llmconduit —
//!   1. SSE TRANSPORT KEEP-ALIVE — every streaming HTTP response is built with
//!      axum's `KeepAlive::new()` (`http.rs` `stream_anthropic_response` /
//!      `stream_chat_completions_response` / `stream_responses_response`),
//!      whose `KeepAliveStream` emits a `:`-comment ping after each idle
//!      interval (15s default). The test is PARAMETERIZED across all three
//!      front-end streaming ingress routes (`/v1/messages`,
//!      `/v1/chat/completions`, `/v1/responses`): for each it drives a real,
//!      IDLE SSE response (a `PendingUpstream` that stalls after the engine's
//!      prologue) through the http.rs path, advances the paused clock past the
//!      interval, and asserts a keep-alive comment frame appears in the response
//!      body — so deleting `.keep_alive(...)` from any of the three builders
//!      makes the matching route fail (no ping is ever emitted).
//!   2. ANTHROPIC-EGRESS reasoning-only deferral/promotion — the
//!      `responses_to_anthropic::AnthropicStreamConverter` (gap G8) DEFERS
//!      reasoning: it emits NO content block while only reasoning has arrived,
//!      and resolves the buffer at the terminal (promote reasoning-only@`stop`
//!      to a single `text` block). The test asserts event-by-event that no
//!      content block is emitted before `response.completed`, then exactly one
//!      promoted `text` block — so an impl that streamed reasoning as text
//!      mid-stream (no deferral) fails.
//!
//! NOT claimed: this is Anthropic-EGRESS + SSE-transport only. The canonical
//! Responses and Chat egress paths are NOT covered by the G8 reasoning-deferral
//! behavior (they stream reasoning progressively), so nothing here asserts a
//! reasoning-only buffering contract for them. The full reasoning-deferral
//! matrix (length/signed/late/then-text) lives in
//! `tests/port_response_translation.rs` and the `responses_to_anthropic.rs`
//! unit tests; this file adds only the G3-specific ORDERING (deferral) lock and
//! the transport keep-alive lock. Assertions are on the event sequence, never
//! on wall-clock timing.

mod common;

use common::MockSearch;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::Request;
use futures::StreamExt;
use llmconduit::adapters::responses_to_anthropic::AnthropicStreamConverter;
use llmconduit::engine::Gateway;
use llmconduit::engine::SseEvent;
use llmconduit::error::AppError;
use llmconduit::models::anthropic::AnthropicContentBlockStart;
use llmconduit::models::anthropic::AnthropicDelta;
use llmconduit::models::anthropic::AnthropicStreamEvent;
use llmconduit::monitor::MonitorHub;
use llmconduit::replay::ReplayStore;
use llmconduit::upstream::UpstreamClient;
use llmconduit::upstream::UpstreamModelEntry;
use llmconduit::upstream::UpstreamStream;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

/// Build a gateway over an arbitrary `UpstreamClient` (the shared
/// `common::test_gateway` is fixed to `MockUpstream`), reusing the standard
/// `common::test_config`.
fn gateway_with_upstream<U: UpstreamClient + 'static>(upstream: U) -> Arc<Gateway> {
    let config = common::test_config();
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(Gateway::new(
        config,
        ReplayStore::new(1000),
        Arc::new(upstream),
        Arc::new(MockSearch::default()),
        image_cache,
        MonitorHub::disabled(),
        None,
        llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
    ))
}

// --------------------------------------------------------------------------
// (1) SSE transport keep-alive — observe the ACTUAL axum KeepAlive output
// --------------------------------------------------------------------------

/// An upstream whose chat stream never yields a chunk: it stays pending forever.
/// This forces the engine's SSE output to go IDLE after its prologue
/// (`response.created` / `response.in_progress` → Anthropic `ping` /
/// `message_start`), which is exactly the state axum's keep-alive timer guards.
#[derive(Clone, Default)]
struct PendingUpstream;

#[async_trait]
impl UpstreamClient for PendingUpstream {
    async fn stream_chat_completion(
        &self,
        _backend: &llmconduit::upstream::BackendChatRequest,
    ) -> Result<UpstreamStream, AppError> {
        // A stream that is immediately and permanently pending.
        let stream = async_stream::stream! {
            std::future::pending::<()>().await;
            // Unreachable; present only to fix the stream item type.
            yield Err(AppError::internal("unreachable"));
        };
        Ok(Box::pin(stream))
    }

    async fn list_models(&self) -> Result<reqwest::Response, AppError> {
        Err(AppError::internal("unused in this test"))
    }

    async fn supported_model_catalog(&self) -> Result<Vec<UpstreamModelEntry>, AppError> {
        Ok(vec![UpstreamModelEntry {
            id: "glm-5.1".to_string(),
            context_limit: None,
        }])
    }
}

/// The three front-end streaming ingress routes, each built with
/// `.keep_alive(axum::response::sse::KeepAlive::new())` in its own `http.rs`
/// builder (`stream_anthropic_response` / `stream_chat_completions_response` /
/// `stream_responses_response`). A minimal STREAMING request body for each — the
/// `PendingUpstream` makes every one of them go idle after the engine prologue.
struct IngressRoute {
    /// Test label, also the human name in assertion messages.
    name: &'static str,
    /// Ingress path under test.
    uri: &'static str,
    /// `http.rs` builder this route exercises (for the failure message).
    builder: &'static str,
}

fn streaming_routes() -> [IngressRoute; 3] {
    [
        IngressRoute {
            name: "anthropic /v1/messages",
            uri: "/v1/messages",
            builder: "http.rs::stream_anthropic_response",
        },
        IngressRoute {
            name: "chat /v1/chat/completions",
            uri: "/v1/chat/completions",
            builder: "http.rs::stream_chat_completions_response",
        },
        IngressRoute {
            name: "responses /v1/responses",
            uri: "/v1/responses",
            builder: "http.rs::stream_responses_response",
        },
    ]
}

/// A minimal streaming request body for `route`, all asking for the same model
/// the `PendingUpstream` serves. The wire shape differs per ingress but each
/// reaches the engine and stalls on the pending upstream, going idle.
fn streaming_body(uri: &str) -> serde_json::Value {
    match uri {
        "/v1/messages" => json!({
            "model": "glm-5.1",
            "max_tokens": 1024,
            "stream": true,
            "messages": [{ "role": "user", "content": "hi" }]
        }),
        "/v1/chat/completions" => json!({
            "model": "glm-5.1",
            "stream": true,
            "messages": [{ "role": "user", "content": "hi" }]
        }),
        "/v1/responses" => json!({
            "model": "glm-5.1",
            "stream": true,
            "input": "hi"
        }),
        other => panic!("no streaming body defined for {other}"),
    }
}

/// Drive a real, IDLE SSE response through the `http.rs` streaming path for one
/// ingress route and assert axum's keep-alive timer emits a `:`-comment ping
/// once the idle interval elapses. Paused time (`start_paused`) keeps it
/// deterministic and instant.
///
/// Harness (no scheduler protocol): advance the paused clock PAST the 15s
/// keep-alive interval FIRST, so its `Sleep` is already expired, then drain
/// frames. Every frame is now wanted — the engine prologue, then the ping the
/// expired timer produces the moment the inner stream is idle — so we read until
/// the `:` comment.
///
/// Each read is wrapped in a paused-time `tokio::time::timeout`, so an ABSENT
/// ping cannot hang the test: deleting `.keep_alive(...)` removes the
/// `KeepAliveStream`, the body then stays pending on the permanently-pending
/// upstream, and the timeout's own `Sleep` auto-advances to its deadline and
/// fires `Elapsed` — a clean, deterministic failure naming the route, NOT a
/// hang. (An unbounded `body.next().await` would never resolve, since the frame
/// budget only counts completed reads.) The frame budget additionally guards
/// against a stream that keeps emitting non-ping frames.
async fn assert_idle_stream_emits_keepalive_ping(route: &IngressRoute) {
    let app = llmconduit::build_app_from_gateway(gateway_with_upstream(PendingUpstream));

    let body = streaming_body(route.uri);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(route.uri)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(
        response.status().as_u16(),
        200,
        "{} streaming request must start a 200 SSE response",
        route.name
    );

    let mut body = response.into_body().into_data_stream();

    // Expire the keep-alive interval up front. The builder's `KeepAlive::new()`
    // started its `Sleep` when the response was constructed; advancing past it
    // means the very next moment the inner stream is idle, `KeepAliveStream`
    // yields the `:` ping.
    tokio::time::advance(Duration::from_secs(16)).await;

    // Each read is bounded by a paused-time timeout so an absent ping fails fast
    // instead of hanging. The timeout is generous relative to the keep-alive
    // interval; under paused time it only elapses if no frame is forthcoming.
    const READ_TIMEOUT: Duration = Duration::from_secs(60);
    // Defense against a stream that keeps emitting non-ping frames: the prologue
    // is a handful of frames, so the ping must arrive well within this budget.
    const KEEPALIVE_FRAME_BUDGET: usize = 64;
    let mut saw_keepalive = false;
    for _ in 0..KEEPALIVE_FRAME_BUDGET {
        let read = tokio::time::timeout(READ_TIMEOUT, body.next()).await;
        let frame = read.unwrap_or_else(|_| {
            panic!(
                "{}: idle SSE body produced NO keep-alive ping before the read timed out — \
                 `.keep_alive(...)` appears removed from {} (deterministic timeout, not a hang)",
                route.name, route.builder
            )
        });
        match frame {
            Some(Ok(bytes)) => {
                if bytes.starts_with(b":") {
                    saw_keepalive = true;
                    break;
                }
                // Engine prologue / converter frame; keep draining.
            }
            Some(Err(err)) => panic!("{}: stream error draining frames: {err}", route.name),
            None => panic!(
                "{}: idle SSE stream ended without a keep-alive ping ({} appears to have dropped \
                 `.keep_alive(...)`)",
                route.name, route.builder
            ),
        }
    }

    assert!(
        saw_keepalive,
        "{}: an idle SSE response produced NO `:` keep-alive comment within {} frames after the \
         interval elapsed — `.keep_alive(...)` appears removed from {} (deterministic failure, \
         not a hang)",
        route.name, KEEPALIVE_FRAME_BUDGET, route.builder
    );
}

/// G3-peek keep-alive, parameterized across ALL THREE streaming ingress routes
/// (Anthropic, Chat, Responses): each must emit axum's `:` keep-alive comment on
/// an idle stream once the interval elapses. Deleting `.keep_alive(...)` from any
/// of the three `http.rs` builders fails the corresponding route here.
#[tokio::test(start_paused = true)]
async fn idle_streams_emit_keepalive_ping_across_all_routes() {
    for route in streaming_routes() {
        assert_idle_stream_emits_keepalive_ping(&route).await;
    }
}

/// Companion sanity lock (the idle-ping test above is the STRONG behavioral
/// lock): every streaming route also advertises the keep-alive transport headers
/// set in the same `http.rs` builder block as `.keep_alive(...)`
/// (`Connection: keep-alive`, `X-Accel-Buffering: no`). Cheap header coverage
/// across all three routes; not a substitute for observing the ping.
#[tokio::test]
async fn streams_advertise_keep_alive_headers_across_all_routes() {
    for route in streaming_routes() {
        let app = llmconduit::build_app_from_gateway(gateway_with_upstream(PendingUpstream));
        let body = streaming_body(route.uri);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(route.uri)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status().as_u16(), 200, "{}", route.name);
        let headers = response.headers();
        assert_eq!(
            headers.get("connection").and_then(|v| v.to_str().ok()),
            Some("keep-alive"),
            "{}: streaming response must advertise Connection: keep-alive",
            route.name
        );
        assert_eq!(
            headers
                .get("x-accel-buffering")
                .and_then(|v| v.to_str().ok()),
            Some("no"),
            "{}: streaming response must disable proxy buffering",
            route.name
        );
    }
}

// --------------------------------------------------------------------------
// (2) Anthropic-egress reasoning-only deferral — event-by-event ORDERING lock
// --------------------------------------------------------------------------

fn created_event() -> SseEvent {
    SseEvent {
        event: "response.created".to_string(),
        data: json!({ "type": "response.created", "response": { "id": "resp_g3" } }),
    }
}

fn reasoning_item_added_event() -> SseEvent {
    SseEvent {
        event: "response.output_item.added".to_string(),
        data: json!({
            "type": "response.output_item.added",
            "item": { "type": "reasoning", "role": "" }
        }),
    }
}

fn reasoning_delta_event(text: &str) -> SseEvent {
    SseEvent {
        event: "response.reasoning_text.delta".to_string(),
        data: json!({
            "type": "response.reasoning_text.delta",
            "delta": text,
            "content_index": 0
        }),
    }
}

fn reasoning_item_done_event() -> SseEvent {
    SseEvent {
        event: "response.output_item.done".to_string(),
        data: json!({
            "type": "response.output_item.done",
            "item": { "type": "reasoning" }
        }),
    }
}

fn completed_event() -> SseEvent {
    SseEvent {
        event: "response.completed".to_string(),
        data: json!({ "type": "response.completed", "response": { "id": "resp_g3" } }),
    }
}

/// G3 contract (Anthropic egress only) — reasoning-only@`stop` is DEFERRED then
/// PROMOTED. Feed the canonical Responses SSE for a reasoning-only turn ending
/// at a clean completion straight into `AnthropicStreamConverter` and assert,
/// event by event:
///   - NO `content_block_start` (text, thinking, or otherwise) is emitted while
///     only reasoning has arrived — i.e. before the terminal — proving the
///     reasoning is BUFFERED, not streamed; and
///   - the reasoning then appears as exactly one PROMOTED `text` block whose
///     `content_block_start` arrives at/after the terminal `response.completed`,
///     with NO `thinking` block.
///
/// FAILS IF the converter loses deferral: an impl that emitted reasoning as a
/// text/thinking block mid-stream would produce a `content_block_start` before
/// the completion event, tripping the "no content block before terminal" assert.
#[test]
fn anthropic_reasoning_only_is_buffered_until_terminal_then_promoted_to_text() {
    let mut converter = AnthropicStreamConverter::new("claude-3-7-sonnet".to_string());

    // Per-input emissions, kept separate so we can assert WHAT was emitted at
    // each step (the ordering is the contract, not just the final set).
    let after_created = converter.convert(&created_event());
    let after_item_added = converter.convert(&reasoning_item_added_event());
    let after_reasoning_1 = converter.convert(&reasoning_delta_event("The answer "));
    let after_reasoning_2 = converter.convert(&reasoning_delta_event("is 42"));
    let after_item_done = converter.convert(&reasoning_item_done_event());
    let after_completed = converter.convert(&completed_event());

    // While only reasoning has been seen, the converter must NOT open any content
    // block — the reasoning is buffered/deferred. (message_start/ping are
    // allowed; a `content_block_start` is NOT. Nor is a `message_delta`: C1
    // made `record_output_delta` bookkeeping-only, so the only `message_delta`
    // in the whole stream is the terminal one emitted at `response.completed`.)
    let pre_terminal = [
        ("created", &after_created),
        ("reasoning_item_added", &after_item_added),
        ("reasoning_delta_1", &after_reasoning_1),
        ("reasoning_delta_2", &after_reasoning_2),
        ("reasoning_item_done", &after_item_done),
    ];
    for (label, batch) in pre_terminal {
        assert!(
            !batch
                .iter()
                .any(|event| matches!(event, AnthropicStreamEvent::ContentBlockStart { .. })),
            "no content block may be opened before the terminal (deferral); \
             one was opened at step `{label}`: {batch:?}"
        );
        assert!(
            !batch
                .iter()
                .any(|event| matches!(event, AnthropicStreamEvent::ContentBlockDelta { .. })),
            "no content_block_delta may be emitted before the terminal; \
             one was emitted at step `{label}`: {batch:?}"
        );
        assert!(
            !batch
                .iter()
                .any(|event| matches!(event, AnthropicStreamEvent::MessageDelta { .. })),
            "no message_delta may appear before the terminal (C1: bookkeeping-only \
             progress, no wire event); one was emitted at step `{label}`: {batch:?}"
        );
    }

    // The promotion happens AT the terminal: the completed event is where the
    // buffer is resolved into a single `text` block.
    let block_starts: Vec<&AnthropicContentBlockStart> = after_completed
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::ContentBlockStart { content_block, .. } => Some(content_block),
            _ => None,
        })
        .collect();
    assert_eq!(
        block_starts.len(),
        1,
        "the terminal must open exactly one content block (the promoted text): {after_completed:?}"
    );
    assert!(
        matches!(block_starts[0], AnthropicContentBlockStart::Text { .. }),
        "reasoning-only@stop must promote to a TEXT block, not thinking/other: {after_completed:?}"
    );

    // The promoted text carries the buffered reasoning, and NO thinking block or
    // thinking delta is ever emitted across the whole stream.
    let all: Vec<AnthropicStreamEvent> = [
        after_created,
        after_item_added,
        after_reasoning_1,
        after_reasoning_2,
        after_item_done,
        after_completed,
    ]
    .into_iter()
    .flatten()
    .collect();

    let promoted_text: String = all
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::ContentBlockDelta {
                delta: AnthropicDelta::TextDelta { text },
                ..
            } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(promoted_text, "The answer is 42");

    assert!(
        !all.iter().any(|event| matches!(
            event,
            AnthropicStreamEvent::ContentBlockStart {
                content_block: AnthropicContentBlockStart::Thinking { .. },
                ..
            }
        )),
        "promoted reasoning must NOT also emit a thinking block: {all:?}"
    );
    assert!(
        !all.iter().any(|event| matches!(
            event,
            AnthropicStreamEvent::ContentBlockDelta {
                delta: AnthropicDelta::ThinkingDelta { .. },
                ..
            }
        )),
        "no thinking_delta may be emitted for a promoted reasoning-only turn: {all:?}"
    );

    // The stream still terminates cleanly.
    assert!(
        all.iter()
            .any(|event| matches!(event, AnthropicStreamEvent::MessageStop)),
        "stream must end with message_stop: {all:?}"
    );

    // T5: full harness proof on this REAL converter run. `Surface::TextOnly`
    // fits -- the promoted block is `text`, never `thinking`, so invariant 4
    // (signature requirement) is irrelevant here.
    llmconduit::adapters::responses_to_anthropic::conformance::assert_stream_conformant(
        &all,
        llmconduit::adapters::responses_to_anthropic::conformance::Surface::TextOnly,
    );
}

/// Prove the conformance harness is reachable from THIS integration
/// crate at the public path `llmconduit::adapters::responses_to_anthropic::
/// conformance`, operating on real `AnthropicStreamEvent`s the way the other
/// tests in this file construct `AnthropicStreamConverter` output. Hand-built
/// events, NOT real converter output (the converter is not wired to the
/// harness yet).
#[test]
fn conformance_harness_is_reachable_from_port_streaming_peek_crate() {
    use llmconduit::adapters::responses_to_anthropic::conformance::Surface;
    use llmconduit::adapters::responses_to_anthropic::conformance::assert_stream_conformant;
    use llmconduit::models::anthropic::AnthropicMessageDeltaBody;
    use llmconduit::models::anthropic::AnthropicMessageStart;
    use llmconduit::models::anthropic::AnthropicUsage;

    let events = vec![
        AnthropicStreamEvent::MessageStart {
            message: AnthropicMessageStart {
                id: "msg_1".to_string(),
                kind: "message".to_string(),
                role: "assistant".to_string(),
                content: Vec::new(),
                model: "m".to_string(),
                stop_reason: None,
                stop_sequence: None,
                usage: AnthropicUsage {
                    input_tokens: Some(1),
                    output_tokens: Some(0),
                    output_tokens_details: None,
                    server_tool_use: None,
                },
            },
        },
        AnthropicStreamEvent::ContentBlockStart {
            index: 0,
            content_block: AnthropicContentBlockStart::Text {
                text: String::new(),
            },
        },
        AnthropicStreamEvent::ContentBlockDelta {
            index: 0,
            delta: AnthropicDelta::TextDelta {
                text: "hi".to_string(),
            },
        },
        AnthropicStreamEvent::ContentBlockStop { index: 0 },
        AnthropicStreamEvent::MessageDelta {
            delta: AnthropicMessageDeltaBody {
                stop_reason: Some("end_turn".to_string()),
                stop_sequence: None,
            },
            usage: AnthropicUsage {
                input_tokens: Some(1),
                output_tokens: Some(1),
                output_tokens_details: None,
                server_tool_use: None,
            },
        },
        AnthropicStreamEvent::MessageStop,
    ];
    assert_stream_conformant(&events, Surface::TextOnly);
}
