use super::*;
use crate::models::anthropic::AnthropicResponseContentBlock;
use crate::models::anthropic::AnthropicStreamEvent;
use serde_json::json;

fn created_event() -> SseEvent {
    SseEvent {
        event: "response.created".to_string(),
        data: json!({
            "type": "response.created",
            "response": { "id": "resp_123" }
        }),
    }
}

/// C3: `response.created` carrying the engine's early G3 estimate
/// (`ResponseStub::estimated_input_tokens`), as `created_event` (`engine.rs`)
/// produces once wired. The bare `created_event()` above deliberately omits
/// the field, exercising the "no estimate available" fallback the vast
/// majority of these fixtures don't care about.
fn created_event_with_estimate(estimate: u64) -> SseEvent {
    SseEvent {
        event: "response.created".to_string(),
        data: json!({
            "type": "response.created",
            "response": { "id": "resp_123", "estimated_input_tokens": estimate }
        }),
    }
}

fn item_added_event(item_type: &str, role: &str) -> SseEvent {
    SseEvent {
        event: "response.output_item.added".to_string(),
        data: json!({
            "type": "response.output_item.added",
            "item": { "type": item_type, "role": role }
        }),
    }
}

fn text_delta_event(text: &str) -> SseEvent {
    SseEvent {
        event: "response.output_text.delta".to_string(),
        data: json!({
            "type": "response.output_text.delta",
            "delta": text
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

fn reasoning_signature_delta_event(signature: &str) -> SseEvent {
    SseEvent {
        event: "response.reasoning_summary_text.signature_delta".to_string(),
        data: json!({
            "type": "response.reasoning_summary_text.signature_delta",
            "signature": signature,
            "summary_index": 0
        }),
    }
}

fn function_call_arguments_delta_event(call_id: &str, name: &str, delta: &str) -> SseEvent {
    SseEvent {
        event: "response.function_call_arguments.delta".to_string(),
        data: json!({
            "type": "response.function_call_arguments.delta",
            "call_id": call_id,
            "name": name,
            "delta": delta,
        }),
    }
}

fn function_call_arguments_done_event(call_id: &str, name: &str, arguments: &str) -> SseEvent {
    SseEvent {
        event: "response.function_call_arguments.done".to_string(),
        data: json!({
            "type": "response.function_call_arguments.done",
            "call_id": call_id,
            "name": name,
            "arguments": arguments,
        }),
    }
}

fn item_done_event(item_type: &str, extra: Value) -> SseEvent {
    let mut item = serde_json::json!({ "type": item_type });
    if let Value::Object(map) = extra {
        for (k, v) in map {
            item.as_object_mut().unwrap().insert(k, v);
        }
    }
    SseEvent {
        event: "response.output_item.done".to_string(),
        data: json!({
            "type": "response.output_item.done",
            "item": item
        }),
    }
}

fn completed_event() -> SseEvent {
    SseEvent {
        event: "response.completed".to_string(),
        data: json!({
            "type": "response.completed",
            "response": { "id": "resp_123" }
        }),
    }
}

fn completed_event_with_usage(input_tokens: u64, output_tokens: u64) -> SseEvent {
    SseEvent {
        event: "response.completed".to_string(),
        data: json!({
            "type": "response.completed",
            "response": {
                "id": "resp_123",
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                }
            }
        }),
    }
}

fn incomplete_event(reason: &str, input_tokens: u64, output_tokens: u64) -> SseEvent {
    SseEvent {
        event: "response.incomplete".to_string(),
        data: json!({
            "type": "response.incomplete",
            "response": {
                "id": "resp_123",
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                },
                "incomplete_details": {
                    "reason": reason,
                }
            }
        }),
    }
}

fn failed_event(message: &str) -> SseEvent {
    SseEvent {
        event: "response.failed".to_string(),
        data: json!({
            "type": "response.failed",
            "response": {
                "error": { "code": "gateway_error", "message": message }
            }
        }),
    }
}

fn event_types(events: &[AnthropicStreamEvent]) -> Vec<&str> {
    events.iter().map(|e| e.sse_event_type()).collect()
}

#[test]
fn converts_simple_text_response() {
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Hello"),
        text_delta_event(" world"),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    assert_eq!(
        event_types(&events),
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta", // terminal stop reason -- the ONLY message_delta (C1)
            "message_stop",
        ]
    );
}

#[test]
fn converts_reasoning_then_text_response() {
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("reasoning", ""),
        reasoning_delta_event("Thinking..."),
        item_added_event("message", "assistant"),
        text_delta_event("Answer"),
        item_done_event("reasoning", json!({})),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    // Reasoning is deferred (G8): it is buffered (no progressive output-usage
    // on the wire -- C1) until the following text forces a flush, so the
    // `thinking` block opens contiguously with the reasoning it buffered.
    // Exactly one terminal `message_delta` ends the stream (C1). No upstream
    // signature was provided, so the thinking block closes with a synthetic
    // one (C2, invariant 4) before the text block opens.
    assert_eq!(
        event_types(&events),
        vec![
            "message_start",
            "content_block_start", // thinking (flushed on text arrival)
            "content_block_delta", // thinking delta
            "content_block_delta", // synthetic signature delta (C2, no upstream sig)
            "content_block_stop",  // close thinking before text
            "content_block_start", // text
            "content_block_delta", // text delta
            "content_block_stop",  // close text
            "message_delta",       // terminal stop reason -- the ONLY message_delta
            "message_stop",
        ]
    );
}

#[test]
fn unsigned_reasoning_gets_a_synthetic_signature() {
    // C2: when the upstream reasoning channel carries no signature (the
    // DeepSeek `reasoning_content` case -- no `signature_delta` events at
    // all), the emitted thinking block must still close signed (invariant 4)
    // with a NON-EMPTY, clearly-synthetic marker.
    fn run() -> Vec<AnthropicStreamEvent> {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        [
            created_event(),
            item_added_event("reasoning", ""),
            reasoning_delta_event("Thinking..."),
            item_added_event("message", "assistant"),
            text_delta_event("Answer"),
            item_done_event("reasoning", json!({})),
            item_done_event("message", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect()
    }

    let events = run();
    let signatures: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::ContentBlockDelta {
                delta: AnthropicDelta::SignatureDelta { signature },
                ..
            } => Some(signature.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(signatures.len(), 1, "expected exactly one signature_delta");
    let signature = signatures[0];
    assert!(
        !signature.is_empty(),
        "synthetic signature must be non-empty"
    );
    assert!(
        signature.starts_with(SYNTHETIC_SIGNATURE_PREFIX),
        "synthetic signature must carry the recognizable marker prefix, got {signature:?}"
    );

    // Determinism: the SAME canonical input run through a FRESH converter
    // instance (a different, randomly-generated `message_id`) must produce
    // the IDENTICAL synthetic signature -- no wall-clock/RNG involvement.
    let events_again = run();
    let signature_again = events_again
        .iter()
        .find_map(|event| match event {
            AnthropicStreamEvent::ContentBlockDelta {
                delta: AnthropicDelta::SignatureDelta { signature },
                ..
            } => Some(signature.as_str()),
            _ => None,
        })
        .expect("second run must also emit a signature_delta");
    assert_eq!(
        signature, signature_again,
        "same input must yield the same synthetic signature across independent converter runs"
    );
}

#[test]
fn converts_reasoning_signature_delta() {
    // A signed reasoning-only turn: the buffer carries a signature, so at the
    // terminal event it is flushed as a `thinking` block (never promoted) and
    // the buffered `signature_delta` is surfaced.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("reasoning", ""),
        reasoning_delta_event("Thinking..."),
        reasoning_signature_delta_event("sig_123"),
        item_done_event("reasoning", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    let signatures: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::ContentBlockDelta {
                delta: AnthropicDelta::SignatureDelta { signature },
                ..
            } => Some(signature.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(signatures, vec!["sig_123"]);
    // Signed reasoning must stay a thinking block, not be promoted to text.
    assert!(
        events.iter().any(|event| matches!(
            event,
            AnthropicStreamEvent::ContentBlockStart {
                content_block: AnthropicContentBlockStart::Thinking { .. },
                ..
            }
        )),
        "signed reasoning should produce a thinking block"
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            AnthropicStreamEvent::ContentBlockStart {
                content_block: AnthropicContentBlockStart::Text { .. },
                ..
            }
        )),
        "signed reasoning must never be promoted to a text block"
    );
}

#[test]
fn reasoning_only_incomplete_non_length_stays_thinking() {
    // A reasoning-only turn ending in `response.incomplete` with a reason
    // OTHER than length/max-tokens (e.g. content_filter) is NOT a clean
    // stop, so it must flush as a thinking block, never be promoted to text.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("reasoning", ""),
        reasoning_delta_event("partial think"),
        item_done_event("reasoning", json!({})),
        incomplete_event("content_filter", 12, 5),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    assert!(
        events.iter().any(|event| matches!(
            event,
            AnthropicStreamEvent::ContentBlockStart {
                content_block: AnthropicContentBlockStart::Thinking { .. },
                ..
            }
        )),
        "non-length incomplete reasoning must stay a thinking block"
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            AnthropicStreamEvent::ContentBlockStart {
                content_block: AnthropicContentBlockStart::Text { .. },
                ..
            }
        )),
        "non-length incomplete reasoning must never be promoted to text"
    );
}

#[test]
fn accumulates_multi_part_signature_deltas() {
    // A thinking signature can be streamed in multiple `signature_delta`
    // chunks; the emitted thinking block's signature must be the full
    // concatenation, not just the last fragment.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("reasoning", ""),
        reasoning_delta_event("Thinking..."),
        reasoning_signature_delta_event("sig_"),
        reasoning_signature_delta_event("part2_"),
        reasoning_signature_delta_event("end"),
        item_done_event("reasoning", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    let signatures: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::ContentBlockDelta {
                delta: AnthropicDelta::SignatureDelta { signature },
                ..
            } => Some(signature.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        signatures,
        vec!["sig_part2_end"],
        "multi-part signature must be concatenated in order"
    );
}

#[test]
fn converts_function_call_response() {
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Let me check."),
        item_done_event("message", json!({})),
        item_done_event(
            "function_call",
            json!({
                "call_id": "call_1",
                "name": "get_weather",
                "arguments": "{\"location\":\"Seattle\"}"
            }),
        ),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    assert_eq!(
        event_types(&events),
        vec![
            "message_start",
            "content_block_start", // text
            "content_block_delta", // text delta
            "content_block_stop",  // close text
            "content_block_start", // tool_use
            "content_block_delta", // input_json_delta
            "content_block_stop",  // close tool_use
            "message_delta",       // terminal stop reason -- the ONLY message_delta
            "message_stop",
        ]
    );

    // Verify stop_reason is tool_use
    let message_delta = events
        .iter()
        .find(|e| matches!(e, AnthropicStreamEvent::MessageDelta { .. }));
    assert!(message_delta.is_some());
}

#[test]
fn streams_function_call_argument_deltas_progressively() {
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        function_call_arguments_delta_event("call_1", "get_weather", r#"{"loc"#),
        function_call_arguments_delta_event("call_1", "get_weather", r#"ation":"Seattle"}"#),
        function_call_arguments_done_event("call_1", "get_weather", r#"{"location":"Seattle"}"#),
        item_done_event(
            "function_call",
            json!({
                "call_id": "call_1",
                "name": "get_weather",
                "arguments": "{\"location\":\"Seattle\"}"
            }),
        ),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    assert_eq!(
        event_types(&events),
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta", // terminal stop reason -- the ONLY message_delta
            "message_stop",
        ]
    );
    let tool_starts = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                AnthropicStreamEvent::ContentBlockStart {
                    content_block: AnthropicContentBlockStart::ToolUse { .. },
                    ..
                }
            )
        })
        .count();
    assert_eq!(tool_starts, 1);
    let partials: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::ContentBlockDelta {
                delta: AnthropicDelta::InputJsonDelta { partial_json },
                ..
            } => Some(partial_json.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(partials, vec![r#"{"loc"#, r#"ation":"Seattle"}"#]);
}

#[test]
fn converts_tool_use_only_response() {
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_done_event(
            "function_call",
            json!({
                "call_id": "call_1",
                "name": "search",
                "arguments": "{\"query\":\"test\"}"
            }),
        ),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    // Should have tool_use block with tool_use stop_reason
    let has_tool_use = events.iter().any(|e| {
        matches!(
            e,
            AnthropicStreamEvent::ContentBlockStart {
                content_block: AnthropicContentBlockStart::ToolUse { .. },
                ..
            }
        )
    });
    assert!(has_tool_use);
}

#[test]
fn converts_failure_event() {
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events = converter.convert(&failed_event("upstream timeout"));

    assert_eq!(event_types(&events), vec!["error"]);
}

#[test]
fn emits_usage_from_completed_response() {
    // C3: `response.created` carries the engine's early estimate (20) --
    // `message_start.usage.input_tokens` must reflect it instead of the old
    // hardcoded `0`. The terminal `message_delta` must still carry the REAL
    // upstream count (12) from `response.completed`'s usage -- the estimate
    // is a message_start-only stand-in, never a substitute for real usage.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event_with_estimate(20),
        completed_event_with_usage(12, 5),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    let message_start = events
        .iter()
        .find_map(|event| match event {
            AnthropicStreamEvent::MessageStart { message } => Some(message),
            _ => None,
        })
        .expect("message_start");
    assert_eq!(message_start.usage.input_tokens, Some(20));
    assert_eq!(message_start.usage.output_tokens, Some(0));

    let message_delta = events
        .iter()
        .find_map(|event| match event {
            AnthropicStreamEvent::MessageDelta { usage, .. } => Some(usage),
            _ => None,
        })
        .expect("message_delta");
    assert_eq!(message_delta.input_tokens, Some(12));
    assert_eq!(message_delta.output_tokens, Some(5));
}

#[test]
fn message_start_defaults_to_zero_input_tokens_when_no_estimate() {
    // Residual/default path: a `response.created` that carries no
    // `estimated_input_tokens` (the bare `created_event()` fixture, matching
    // an engine event that predates C3 or otherwise omits it) must keep the
    // pre-C3 behavior -- `message_start.usage.input_tokens` stays `Some(0)`,
    // never `None` or a panic.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events = converter.convert(&created_event());

    let message_start = events
        .iter()
        .find_map(|event| match event {
            AnthropicStreamEvent::MessageStart { message } => Some(message),
            _ => None,
        })
        .expect("message_start");
    assert_eq!(message_start.usage.input_tokens, Some(0));
}

#[test]
fn terminal_delta_carries_reasoning_output_tokens() {
    // C1: `record_output_delta` is bookkeeping-only now -- buffered reasoning
    // must not emit any `message_delta` while the model is "thinking". The
    // byte-count bookkeeping it still accumulates (`last_output_tokens`) must
    // surface exactly once, on the terminal delta, so `output_tokens` is
    // non-zero even though nothing was pushed to the wire mid-stream.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let reasoning_events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("reasoning", ""),
        reasoning_delta_event("abcd"),
        reasoning_delta_event("efgh"),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();
    assert!(
        !reasoning_events
            .iter()
            .any(|event| matches!(event, AnthropicStreamEvent::MessageDelta { .. })),
        "no message_delta may appear while reasoning is buffered, got {:?}",
        event_types(&reasoning_events)
    );

    let terminal_events = converter.convert(&completed_event());
    let message_deltas: Vec<_> = terminal_events
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::MessageDelta { delta, usage } => Some((delta, usage)),
            _ => None,
        })
        .collect();
    assert_eq!(
        message_deltas.len(),
        1,
        "exactly one terminal message_delta, got {:?}",
        event_types(&terminal_events)
    );
    let (delta, usage) = message_deltas[0];
    assert!(
        delta.stop_reason.is_some(),
        "the sole message_delta must carry a stop_reason"
    );
    assert!(
        usage.output_tokens.is_some_and(|tokens| tokens > 0),
        "terminal output_tokens must be non-zero from the kept bookkeeping, got {:?}",
        usage.output_tokens
    );
}

#[test]
fn completed_without_upstream_usage_uses_bookkeeping_output_tokens() {
    // When upstream never reports usage (no `response.usage` on the terminal
    // event), the terminal `output_tokens` must still be non-zero -- sourced
    // from the kept `last_output_tokens` byte-count bookkeeping (C1). There is
    // no separate "progress" wire event anymore: this must be the ONE and
    // ONLY message_delta in the stream.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("abcd"),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    let message_deltas: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::MessageDelta { delta, usage } => Some((delta, usage)),
            _ => None,
        })
        .collect();
    assert_eq!(
        message_deltas.len(),
        1,
        "exactly one (terminal) message_delta, no progressive deltas, got {:?}",
        event_types(&events)
    );
    let (delta, usage) = message_deltas[0];
    assert_eq!(delta.stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(usage.output_tokens, Some(1));
}

#[test]
fn converts_incomplete_to_max_tokens_stop_reason() {
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        incomplete_event("max_output_tokens", 12, 5),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    let message_delta = events
        .iter()
        .find_map(|event| match event {
            AnthropicStreamEvent::MessageDelta { delta, .. } => Some(delta),
            _ => None,
        })
        .expect("message_delta");
    assert_eq!(message_delta.stop_reason.as_deref(), Some("max_tokens"));
}

#[test]
fn collector_returns_final_usage() {
    let mut collector = AnthropicStreamCollector::new("claude-3".to_string());
    for event in [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Hello"),
        item_done_event("message", json!({})),
        completed_event_with_usage(12, 5),
    ] {
        collector.process(&event);
    }

    let response = collector.into_response().expect("response");
    assert_eq!(response.usage.input_tokens, 12);
    assert_eq!(response.usage.output_tokens, 5);
}

#[test]
fn collector_output_tokens_nonzero_without_upstream_usage() {
    // Collector-level proof of the kept bookkeeping path (collector.rs:150-156,
    // C1's doc comment): when the terminal upstream event carries NO `usage` at
    // all (`completed_event()`, not `completed_event_with_usage`), the
    // non-stream collector's final `output_tokens` must still be non-zero --
    // sourced from the converter's `last_output_tokens` byte-count estimate,
    // which rides the terminal `message_delta.usage.output_tokens` the
    // collector reads at `AnthropicStreamEvent::MessageDelta` (collector.rs:154).
    // A regression here (e.g. losing the bookkeeping, or the collector
    // ignoring the terminal delta's usage) would silently zero out reported
    // output_tokens for every upstream that doesn't report usage.
    let mut collector = AnthropicStreamCollector::new("claude-3".to_string());
    for event in [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Hello world"),
        item_done_event("message", json!({})),
        completed_event(), // no `response.usage` field
    ] {
        collector.process(&event);
    }

    let response = collector.into_response().expect("response");
    assert!(
        response.usage.output_tokens > 0,
        "collector output_tokens must stay non-zero via bookkeeping when upstream \
         usage is absent, got {}",
        response.usage.output_tokens
    );
}

#[test]
fn collector_preserves_thinking_signature() {
    let mut collector = AnthropicStreamCollector::new("claude-3".to_string());
    for event in [
        created_event(),
        item_added_event("reasoning", ""),
        reasoning_delta_event("private chain"),
        reasoning_signature_delta_event("sig_123"),
        item_done_event("reasoning", json!({})),
        completed_event(),
    ] {
        collector.process(&event);
    }

    let response = collector.into_response().expect("response");
    assert!(matches!(
        &response.content[0],
        AnthropicResponseContentBlock::Thinking {
            thinking,
            signature: Some(signature),
        } if thinking == "private chain" && signature == "sig_123"
    ));
}

#[test]
fn collector_suppresses_unrequested_reasoning() {
    let mut collector =
        AnthropicStreamCollector::with_reasoning_suppression("claude-3".to_string(), true);
    for event in [
        created_event(),
        item_added_event("reasoning", ""),
        reasoning_delta_event("private chain"),
        reasoning_signature_delta_event("sig_123"),
        item_done_event("reasoning", json!({})),
        item_added_event("message", "assistant"),
        text_delta_event("OK"),
        item_done_event("message", json!({})),
        completed_event(),
    ] {
        collector.process(&event);
    }

    let response = collector.into_response().expect("response");
    assert_eq!(response.content.len(), 1);
    assert!(matches!(
        &response.content[0],
        AnthropicResponseContentBlock::Text { text } if text == "OK"
    ));
}

#[test]
fn skips_web_search_call_events() {
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Searching..."),
        item_done_event("message", json!({})),
        // web_search_call events should be skipped
        SseEvent {
            event: "response.output_item.added".to_string(),
            data: json!({
                "type": "response.output_item.added",
                "item": { "type": "web_search_call", "id": "ws_1", "status": "in_progress" }
            }),
        },
        SseEvent {
            event: "response.output_item.done".to_string(),
            data: json!({
                "type": "response.output_item.done",
                "item": { "type": "web_search_call", "id": "ws_1", "status": "completed" }
            }),
        },
        // After internal web search, more text comes in a new turn
        // (simulated by another item_added + delta)
        item_added_event("message", "assistant"),
        text_delta_event("Here are the results."),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    // No tool_use events should appear for web_search_call
    let has_web_search_tool_use = events.iter().any(|e| {
        matches!(
            e,
            AnthropicStreamEvent::ContentBlockStart {
                content_block: AnthropicContentBlockStart::ToolUse { name, .. },
                ..
            } if name == "web_search"
        )
    });
    assert!(!has_web_search_tool_use);
}

#[test]
fn finalize_terminates_stream_when_upstream_ends_without_completed() {
    // Regression: a web-search round-trip that stalls/aborts ends the
    // upstream event stream without `response.completed`. Without an
    // explicit finalize the client only ever sees `message_start` and
    // hangs forever behind the SSE keep-alive.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let mut events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Searching the web"),
        // ...stream ends here: no response.output_item.done, no completed.
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();
    events.extend(converter.finalize());

    let names = event_types(&events);
    assert_eq!(
        names.last(),
        Some(&"message_stop"),
        "stream must end with message_stop, got {names:?}"
    );
    // The dangling text content block must be closed before message_stop.
    let stop_idx = names.iter().position(|n| *n == "content_block_stop");
    let msg_stop_idx = names.iter().position(|n| *n == "message_stop");
    assert!(stop_idx < msg_stop_idx, "open block not closed: {names:?}");
}

#[test]
fn finalize_terminates_stream_with_no_events_at_all() {
    // The engine can stall before producing any output. finalize() must
    // still synthesize a complete, valid message envelope.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events = converter.finalize();
    assert_eq!(
        event_types(&events),
        vec!["message_start", "message_delta", "message_stop"]
    );
}

#[test]
fn finalize_is_noop_after_normal_completion() {
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let _: Vec<_> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Hello"),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();
    // No duplicate message_delta/message_stop after a clean completion.
    assert!(converter.finalize().is_empty());
}

fn web_search_results_event(tool_use_id: &str, query: &str, results: Value) -> SseEvent {
    SseEvent {
        event: "response.web_search_results".to_string(),
        data: json!({
            "type": "response.web_search_results",
            "tool_use_id": tool_use_id,
            "query": query,
            "results": results,
        }),
    }
}

#[test]
fn web_search_results_emit_server_tool_use_then_result_block() {
    // Regression: resp2chat ran Brave server-side but swallowed the call,
    // so Claude Code reported "Did 0 searches" and listed no sources. The
    // converter must surface the Anthropic server-side web-search blocks
    // (`server_tool_use` + `web_search_tool_result`) so the client counts
    // the search and renders source chips.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let results = json!([
        {"type": "web_search_result", "url": "https://example.com/a", "title": "Site A"},
        {"type": "web_search_result", "url": "https://example.com/b", "title": "Site B"}
    ]);
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Let me search."),
        web_search_results_event("srvtoolu_1", "current weather Boppard", results.clone()),
        text_delta_event("It is 11C."),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    let jsons: Vec<Value> = events
        .iter()
        .map(|e| serde_json::from_str(&e.to_json()).unwrap())
        .collect();

    // server_tool_use block, query streamed via input_json_delta, then stop.
    let stu_pos = jsons
        .iter()
        .position(|j| j["content_block"]["type"] == "server_tool_use")
        .expect("server_tool_use content_block_start");
    assert_eq!(jsons[stu_pos]["type"], "content_block_start");
    assert_eq!(jsons[stu_pos]["content_block"]["name"], "web_search");
    let stu_id = jsons[stu_pos]["content_block"]["id"].as_str().unwrap();
    assert!(!stu_id.is_empty(), "server_tool_use must carry an id");
    let stu_idx = jsons[stu_pos]["index"].clone();

    assert_eq!(jsons[stu_pos + 1]["type"], "content_block_delta");
    assert_eq!(jsons[stu_pos + 1]["delta"]["type"], "input_json_delta");
    let partial = jsons[stu_pos + 1]["delta"]["partial_json"]
        .as_str()
        .unwrap();
    let parsed: Value = serde_json::from_str(partial).expect("partial_json is JSON");
    assert_eq!(parsed["query"], "current weather Boppard");
    assert_eq!(jsons[stu_pos + 2]["type"], "content_block_stop");
    assert_eq!(jsons[stu_pos + 2]["index"], stu_idx);

    // web_search_tool_result block carrying the structured sources.
    let res_pos = stu_pos + 3;
    assert_eq!(jsons[res_pos]["type"], "content_block_start");
    assert_eq!(
        jsons[res_pos]["content_block"]["type"],
        "web_search_tool_result"
    );
    assert_eq!(jsons[res_pos]["content_block"]["tool_use_id"], stu_id);
    assert_eq!(jsons[res_pos]["content_block"]["content"], results);
    assert_eq!(jsons[res_pos + 1]["type"], "content_block_stop");

    // Server-side tool: turn still ends with end_turn, not tool_use.
    let msg_delta = jsons
        .iter()
        .find(|j| j["type"] == "message_delta" && j["delta"]["stop_reason"].is_string())
        .expect("message_delta");
    assert_eq!(msg_delta["delta"]["stop_reason"], "end_turn");
}

#[test]
fn web_search_stream_is_fully_conformant() {
    // WebSearch surface, real converter run: server_tool_use +
    // web_search_tool_result blocks are emitted directly (mod.rs:534) and never
    // call `record_output_delta`, so this surface proves invariants 2/3/5
    // hold even though NOTHING drives the progressive-usage bookkeeping the
    // ClientToolUse/ReasoningText surfaces exercise -- C1 removed the
    // progressive push globally, so the terminal message_delta is still the
    // ONLY one in the stream. Neither block type is `thinking`, so invariant 4
    // (signature requirement) never applies to them.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let results = json!([
        {"type": "web_search_result", "url": "https://example.com/a", "title": "Site A"}
    ]);
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Let me search."),
        web_search_results_event("srvtoolu_1", "current weather Boppard", results),
        text_delta_event("It is 11C."),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    conformance::assert_stream_conformant(&events, conformance::Surface::WebSearch);
}

#[test]
fn post_web_search_reasoning_and_answer_are_preserved() {
    // Regression: a web-search turn is reasoning -> web_search_results ->
    // CONTINUATION reasoning -> text answer. The web_search_results block is
    // additive and must NOT trip the late-reasoning drop gate, otherwise the
    // legitimate post-search reasoning is silently dropped. Both the
    // post-search reasoning (as a thinking block) and the text answer must
    // survive.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let results = json!([
        {"type": "web_search_result", "url": "https://example.com/a", "title": "Site A"}
    ]);
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("reasoning", ""),
        reasoning_delta_event("Pre-search thought."),
        web_search_results_event("srvtoolu_1", "weather", results.clone()),
        // Continuation reasoning AFTER the search results.
        item_added_event("reasoning", ""),
        reasoning_delta_event("Post-search thought."),
        // The actual answer.
        item_added_event("message", "assistant"),
        text_delta_event("It is sunny."),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    // Both reasoning segments must surface as thinking deltas (pre-search
    // flushed at the search boundary, post-search flushed when text begins).
    let thinking: String = events
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::ContentBlockDelta {
                delta: AnthropicDelta::ThinkingDelta { thinking },
                ..
            } => Some(thinking.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        thinking.contains("Pre-search thought."),
        "pre-search reasoning must be preserved as thinking, got {thinking:?}"
    );
    assert!(
        thinking.contains("Post-search thought."),
        "post-search continuation reasoning must NOT be dropped, got {thinking:?}"
    );

    // The text answer must survive intact.
    let text: String = events
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::ContentBlockDelta {
                delta: AnthropicDelta::TextDelta { text },
                ..
            } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "It is sunny.", "the text answer must be preserved");
}

#[test]
fn web_search_count_is_reported_in_terminal_usage() {
    // Claude Code's "Did N searches" reads usage.server_tool_use
    // .web_search_requests. resp2chat must report it so a server-side
    // search isn't shown as "Did 0 searches".
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let r = json!([{"type": "web_search_result", "url": "https://x", "title": "X"}]);
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        web_search_results_event("srvtoolu_1", "q1", r.clone()),
        web_search_results_event("srvtoolu_2", "q2", r.clone()),
        text_delta_event("answer"),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    let md = events
        .iter()
        .filter_map(|e| match e {
            AnthropicStreamEvent::MessageDelta { .. } => {
                Some(serde_json::from_str::<Value>(&e.to_json()).unwrap())
            }
            _ => None,
        })
        .find(|j| j["usage"].get("server_tool_use").is_some())
        .expect("message_delta");
    assert_eq!(md["usage"]["server_tool_use"]["web_search_requests"], 2);
}

#[test]
fn no_server_tool_use_usage_when_no_web_search() {
    // Pristine: a turn without web search must not emit server_tool_use usage.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("hi"),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();
    let md = events
        .iter()
        .find(|e| matches!(e, AnthropicStreamEvent::MessageDelta { .. }))
        .map(|e| serde_json::from_str::<Value>(&e.to_json()).unwrap())
        .expect("message_delta");
    assert!(
        md["usage"].get("server_tool_use").is_none(),
        "no web search => no server_tool_use usage, got {}",
        md["usage"]
    );
}

#[test]
fn finalize_is_noop_after_failure_event() {
    // C4: `handle_failed` now marks the turn `completed`, so the stream ends
    // AT `error` -- matching Anthropic's real mid-stream error shape (never
    // followed by a synthetic `message_delta` + `message_stop`). The caller
    // (`http.rs::stream_anthropic_response`) still unconditionally calls
    // `finalize()` after the upstream loop ends; that call must be a no-op
    // here, same as the already-completed-normally case below.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let mut events = converter.convert(&created_event());
    events.extend(converter.convert(&failed_event("web search round limit exceeded")));

    let names = event_types(&events);
    assert_eq!(
        names.last(),
        Some(&"error"),
        "stream must end with error, got {names:?}"
    );
    assert!(
        converter.finalize().is_empty(),
        "finalize() must be a no-op after a failure event"
    );

    // Full harness proof: this is exactly the `Surface::Error` contract
    // (conformance.rs invariant 6) -- reconciles the harness with the real
    // converter/http-layer behavior instead of leaving it self-tested only.
    conformance::assert_stream_conformant(&events, conformance::Surface::Error);
}

// -- C1 conformance proof: a REAL converter run, checked against the harness
// from Task 0B1 (`super::conformance`), not a hand-built event vector. --

#[test]
fn real_converter_reasoning_text_stream_satisfies_ordering_invariants() {
    // C1 + C2 win, proven via the harness: a real reasoning+text converter run
    // satisfies ALL invariants 1-5 (exactly one terminal message_delta with a
    // stop_reason, never before the first content_block_start, never inside an
    // open block, the thinking block is signed, stream ends message_delta ->
    // message_stop). This turn carries no upstream reasoning signature (the
    // DeepSeek `reasoning_content` case), so invariant 4 is satisfied by C2's
    // synthesized signature, not a forwarded real one -- see
    // `flush_reasoning_as_thinking` / `synthetic_signature`.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("reasoning", ""),
        reasoning_delta_event("Thinking..."),
        item_added_event("message", "assistant"),
        text_delta_event("Answer"),
        item_done_event("reasoning", json!({})),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    conformance::assert_stream_conformant(&events, conformance::Surface::ReasoningText);
}

#[test]
fn text_only_stream_is_fully_conformant() {
    // C1 win, TextOnly surface: the simplest real converter run (no thinking
    // block, no tool call) still satisfies every invariant -- zero progressive
    // deltas, one terminal delta, correct start/stop ordering.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Hello"),
        text_delta_event(" world"),
        item_done_event("message", json!({})),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    conformance::assert_stream_conformant(&events, conformance::Surface::TextOnly);
}

#[test]
fn client_tool_use_stream_is_fully_conformant() {
    // C1 win, CLIENT tool_use surface (deliberately not web_search, which
    // never calls `record_output_delta` -- see `conformance::Surface::ClientToolUse`
    // doc comment). No thinking block is involved, so this surface is NOT
    // blocked on C2: exactly zero progressive deltas, one terminal delta, full
    // harness pass.
    let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
    let events: Vec<AnthropicStreamEvent> = [
        created_event(),
        item_added_event("message", "assistant"),
        text_delta_event("Let me check."),
        item_done_event("message", json!({})),
        item_done_event(
            "function_call",
            json!({
                "call_id": "call_1",
                "name": "get_weather",
                "arguments": "{\"location\":\"Seattle\"}"
            }),
        ),
        completed_event(),
    ]
    .iter()
    .flat_map(|e| converter.convert(e))
    .collect();

    let message_deltas: Vec<bool> = events
        .iter()
        .filter_map(|event| match event {
            AnthropicStreamEvent::MessageDelta { delta, .. } => Some(delta.stop_reason.is_some()),
            _ => None,
        })
        .collect();
    assert_eq!(
        message_deltas,
        vec![true],
        "expected zero progressive deltas and exactly one terminal delta \
         (stop_reason set), got {:?}",
        event_types(&events)
    );

    conformance::assert_stream_conformant(&events, conformance::Surface::ClientToolUse);
}
