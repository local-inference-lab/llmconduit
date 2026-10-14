//! Ported surface: request-translation (claude-relay test_convert_request.py).
//!
//! claude-relay translated inbound requests into OpenAI **Chat** shape; llmconduit
//! translates into the canonical **Responses** shape. So each behavior is ported
//! at the contract level, not the literal output shape:
//!   - "system merged into a system string"  ->  Anthropic: system lands in
//!     `instructions`; Chat: system stays a system-role item in `input`.
//!   - "tool_choice any -> required"          ->  asserted against llmconduit's
//!     own tool_choice mapping (see port_tools coverage).
//!
//! Pure-function tests: they call the adapter `convert_request` directly -- no
//! gateway/mock needed, so this file does not pull in the shared `common` lever.

use llmconduit::adapters::anthropic_to_responses;
use llmconduit::adapters::chat_completions;
use llmconduit::models::anthropic::AnthropicRequest;
use llmconduit::models::chat::ChatCompletionRequest;
use llmconduit::upstream::BackendChatRequest;
use llmconduit::upstream::BackendFinalizationPolicies;
use llmconduit::upstream::finalize_request_for_backend;
use serde_json::json;
use std::sync::Arc;

fn anthropic(value: serde_json::Value) -> AnthropicRequest {
    serde_json::from_value(value).expect("valid AnthropicRequest")
}

fn chat(value: serde_json::Value) -> ChatCompletionRequest {
    serde_json::from_value(value).expect("valid ChatCompletionRequest")
}

// ===========================================================================
// Anthropic Messages -> Responses
// ===========================================================================

/// claude-relay rejected unsupported sampling knobs; llmconduit 400s on top_k.
#[test]
fn anthropic_top_k_is_rejected() {
    let err = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "top_k": 40,
        "messages": [{"role": "user", "content": "hi"}],
    })))
    .expect_err("top_k must be rejected");
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
}

/// test_system_role / test_multiple_system_messages: system content is surfaced.
/// llmconduit lifts the system string into canonical `instructions`.
#[test]
fn anthropic_system_string_becomes_instructions() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "system": "You are a helpful assistant.",
        "messages": [{"role": "user", "content": "hi"}],
    })))
    .expect("convert");
    assert!(
        result.instructions.contains("You are a helpful assistant."),
        "system text should land in instructions, got {:?}",
        result.instructions
    );
}

/// test_system_message ... list content: system text blocks are joined into one
/// instruction string (claude-relay collapsed list content to a string).
#[test]
fn anthropic_system_blocks_join_into_instructions() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "system": [
            {"type": "text", "text": "First rule."},
            {"type": "text", "text": "Second rule."}
        ],
        "messages": [{"role": "user", "content": "hi"}],
    })))
    .expect("convert");
    assert!(result.instructions.contains("First rule."));
    assert!(result.instructions.contains("Second rule."));
}

/// stop_sequences flow through normalize_stop into the typed `stop` field (same
/// gate as the Chat path), not smuggled raw into extra_body["stop"].
#[test]
fn anthropic_stop_sequences_map_to_typed_stop() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "stop_sequences": ["STOP", "END"],
        "messages": [{"role": "user", "content": "hi"}],
    })))
    .expect("convert");
    assert_eq!(
        result.stop,
        Some(vec!["STOP".to_string(), "END".to_string()])
    );
    assert!(!result.extra_body.contains_key("stop"));
}

/// More than 4 stop_sequences must 400 at convert time
/// (OPENAI_MAX_STOP_SEQUENCES=4), never silently truncate and never reach upstream.
#[test]
fn anthropic_too_many_stop_sequences_are_rejected() {
    let err = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "stop_sequences": ["A", "B", "C", "D", "E"],
        "messages": [{"role": "user", "content": "hi"}],
    })))
    .expect_err("more than 4 stop_sequences must be rejected");
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
}

/// Empty / all-empty stop_sequences collapse to None — no stray extra_body entry.
#[test]
fn anthropic_empty_stop_sequences_collapse_to_none() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "stop_sequences": ["", ""],
        "messages": [{"role": "user", "content": "hi"}],
    })))
    .expect("convert");
    assert_eq!(result.stop, None);
    assert!(!result.extra_body.contains_key("stop"));
}

// ===========================================================================
// Leaf-finalize gap-fill wire path (U2): a typed client `stop` must beat a
// configured `upstream_chat_kwargs.stop`, with exactly ONE `"stop"` on the wire
// (the client value) and NO `extra_body["stop"]`. Drives the real
// `finalize_request_for_backend` / `merge_chat_kwargs_gap_fill` leaf path and
// inspects `serde_json::to_value(&request)` (mirroring `reqwest .json`).
// ===========================================================================

/// Build leaf policies whose GLOBAL `upstream_chat_kwargs` carries `kwargs`.
fn policies_with_global_kwargs(
    kwargs: serde_json::Map<String, serde_json::Value>,
) -> BackendFinalizationPolicies {
    BackendFinalizationPolicies {
        effort: Arc::new(std::collections::BTreeMap::new()),
        template_family: Arc::new(std::collections::BTreeMap::new()),
        global_template_family: None,
        upstream_chat_kwargs: Arc::new(std::collections::BTreeMap::new()),
        global_upstream_chat_kwargs: Arc::new(kwargs),
    }
}

#[test]
fn leaf_finalize_typed_stop_beats_configured_stop_single_wire_key() {
    let mut request = chat(json!({
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "stop": ["CLIENT"],
    }));
    assert_eq!(request.stop, Some(vec!["CLIENT".to_string()]));

    let policies = policies_with_global_kwargs(serde_json::Map::from_iter([(
        "stop".to_string(),
        json!(["CONFIGURED"]),
    )]));
    let mut backend = BackendChatRequest::new(request.clone(), None, None, None);
    finalize_request_for_backend(&mut backend, &policies);
    request = backend.request;

    // Typed client value survives; the configured default never gap-fills.
    assert_eq!(request.stop, Some(vec!["CLIENT".to_string()]));
    assert!(
        !request.extra_body.contains_key("stop"),
        "configured stop must not land in extra_body"
    );

    // Real wire shape (mirrors reqwest .json(&request)): exactly one "stop".
    let wire = serde_json::to_value(&request).expect("serialize wire request");
    assert_eq!(wire["stop"], json!(["CLIENT"]));
    let stop_keys = wire
        .as_object()
        .expect("object")
        .keys()
        .filter(|k| *k == "stop")
        .count();
    assert_eq!(stop_keys, 1, "exactly one stop key on the wire");
}

/// Provider-fallback alias collision (U2): the collapsed gap-fill helper now
/// ALWAYS applies the max-token-alias skip, so a configured/provider
/// `max_tokens` default cannot land alongside a client `max_completion_tokens`
/// surviving in `extra_body` (the `/v1/responses` shape). Driven through the
/// leaf path, which shares the SAME helper as the provider-fallback call site.
#[test]
fn leaf_finalize_skips_provider_max_tokens_when_client_alias_present() {
    let mut request = chat(json!({
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
    }));
    // Reproduce the `/v1/responses` shape: `ResponsesRequest` types only
    // `max_output_tokens`, so a client `max_completion_tokens` is NOT captured by
    // a typed field and survives as a flattened `extra_body` key threaded to the
    // backend. (On the Chat path the serde alias would absorb it; here we inject
    // it directly to mirror the Responses-origin alias the fallback path sees.)
    request
        .extra_body
        .insert("max_completion_tokens".to_string(), json!(256));
    assert_eq!(request.max_output_tokens, None);
    assert_eq!(
        request.extra_body.get("max_completion_tokens"),
        Some(&json!(256))
    );

    let policies = policies_with_global_kwargs(serde_json::Map::from_iter([(
        "max_tokens".to_string(),
        json!(4096),
    )]));
    let mut backend = BackendChatRequest::new(request, None, None, None);
    finalize_request_for_backend(&mut backend, &policies);
    let request = backend.request;

    assert!(
        !request.extra_body.contains_key("max_tokens"),
        "provider max_tokens alias must not land alongside the client alias"
    );
    assert_eq!(
        request.extra_body.get("max_completion_tokens"),
        Some(&json!(256)),
        "client alias must survive untouched"
    );
}

#[test]
fn anthropic_max_tokens_maps_to_max_output_tokens() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 256,
        "messages": [{"role": "user", "content": "hi"}],
    })))
    .expect("convert");
    assert_eq!(result.max_output_tokens, Some(256));
}

#[test]
fn anthropic_omitted_parallel_tool_choice_leaves_upstream_default_unset() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
    })))
    .expect("convert");
    assert_eq!(result.parallel_tool_calls, None);
}

/// llmconduit's output_config carries structured-output `format`, not effort.
/// A json_schema format must survive into canonical `text` controls.
#[test]
fn anthropic_output_config_json_schema_maps_to_text() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
        "output_config": {
            "format": {
                "type": "json_schema",
                "name": "answer",
                "schema": {"type": "object", "properties": {}}
            }
        }
    })))
    .expect("convert");
    let text =
        serde_json::to_value(result.text.expect("text controls present")).expect("serialize");
    assert_eq!(text["format"]["type"], "json_schema");
    assert_eq!(text["format"]["name"], "answer");
}

/// Non-json_schema structured-output formats are not supported -> 400.
#[test]
fn anthropic_output_config_non_json_schema_rejected() {
    let err = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
        "output_config": {"format": {"type": "text"}}
    })))
    .expect_err("non json_schema format must be rejected");
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
}

/// output_config without a `format` key is a no-op (no text controls emitted).
#[test]
fn anthropic_output_config_without_format_is_noop() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
        "output_config": {"effort": "high"}
    })))
    .expect("convert");
    assert!(result.text.is_none());
}

/// claude-relay mapped output_config.effort + adaptive thinking onto
/// chat_template_kwargs.reasoning_effort. llmconduit derives baseline effort
/// from `thinking`, then lets `output_config.effort` override it onto canonical
/// `reasoning.effort` while thinking is adaptive (Claude Code's `/effort`).
#[test]
fn anthropic_output_config_effort_maps_to_reasoning_effort() {
    for level in ["low", "medium", "high", "max"] {
        let result = anthropic_to_responses::convert_request(anthropic(json!({
            "model": "claude-3",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
            "thinking": {"type": "adaptive"},
            "output_config": {"effort": level}
        })))
        .expect("convert");
        let effort = result.reasoning.and_then(|r| r.effort);
        assert_eq!(
            effort.as_deref(),
            Some(level),
            "adaptive thinking should adopt output_config.effort {level:?}"
        );
    }
}

/// Effort normalization is split across two layers: `responses_to_chat::
/// normalize_reasoning_effort` trims + lowercases only (it no longer
/// clamps/maps), and the per-model clamp/map happens at the upstream leaf in
/// `upstream::finalize_request_for_backend`. The adapter must NOT allow-list or
/// clamp: case variants, surrounding whitespace, and otherwise
/// unrecognized-but-non-empty values pass through to canonical
/// `reasoning.effort` verbatim (trimmed) so the lowering step can normalize
/// them. Dropping them here would silently divert to budget-derived effort.
#[test]
fn anthropic_output_config_effort_passes_through_raw_for_lowering() {
    for raw in ["HIGH", " high ", "xhigh", "ultra"] {
        let result = anthropic_to_responses::convert_request(anthropic(json!({
            "model": "claude-3",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
            "thinking": {"type": "adaptive"},
            "output_config": {"effort": raw}
        })))
        .expect("convert");
        let effort = result.reasoning.and_then(|r| r.effort);
        assert_eq!(
            effort.as_deref(),
            Some(raw.trim()),
            "adapter must pass effort {raw:?} through raw (trimmed), not drop or clamp it"
        );
    }
}

/// An empty / whitespace-only effort string carries no signal and must be
/// dropped so it cannot clobber a budget-derived effort.
#[test]
fn anthropic_output_config_effort_empty_is_dropped() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
        "thinking": {"type": "adaptive", "budget_tokens": 30000},
        "output_config": {"effort": "   "}
    })))
    .expect("convert");
    let effort = result.reasoning.and_then(|r| r.effort);
    assert_eq!(
        effort.as_deref(),
        Some("high"),
        "blank effort must be ignored, leaving the budget-derived effort intact"
    );
}

/// output_config.effort is an adaptive-thinking signal only. When thinking is
/// disabled, effort must not synthesize reasoning out of nothing.
#[test]
fn anthropic_output_config_effort_ignored_without_active_thinking() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
        "output_config": {"effort": "high"}
    })))
    .expect("convert");
    assert!(
        result.reasoning.is_none(),
        "no thinking block => no reasoning effort, got {:?}",
        result.reasoning
    );
}

/// When thinking is `enabled`, the explicit budget pins the effort; a coexisting
/// output_config.effort must not override the budget-derived effort.
#[test]
fn anthropic_output_config_effort_ignored_when_thinking_enabled() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
        "thinking": {"type": "enabled", "budget_tokens": 30000},
        "output_config": {"effort": "low"}
    })))
    .expect("convert");
    let effort = result.reasoning.and_then(|r| r.effort);
    assert_eq!(
        effort.as_deref(),
        Some("high"),
        "enabled thinking keeps its budget-derived effort, ignoring output_config.effort"
    );
}

/// effort and a json_schema format can ride in one output_config: the format
/// still lands in `text` controls while effort drives `reasoning.effort`.
#[test]
fn anthropic_output_config_effort_and_format_coexist() {
    let result = anthropic_to_responses::convert_request(anthropic(json!({
        "model": "claude-3",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
        "thinking": {"type": "adaptive"},
        "output_config": {
            "effort": "high",
            "format": {
                "type": "json_schema",
                "name": "answer",
                "schema": {"type": "object", "properties": {}}
            }
        }
    })))
    .expect("convert");
    let effort = result.reasoning.and_then(|r| r.effort);
    assert_eq!(effort.as_deref(), Some("high"));
    let text =
        serde_json::to_value(result.text.expect("text controls present")).expect("serialize");
    assert_eq!(text["format"]["type"], "json_schema");
    assert_eq!(text["format"]["name"], "answer");
}

// ===========================================================================
// OpenAI Chat Completions -> Responses
// ===========================================================================

/// Chat conversion never populates `instructions`; system context is carried as
/// a system-role item in `input` and hoisted later during lowering.
#[test]
fn chat_instructions_always_empty_system_stays_in_input() {
    let result = chat_completions::convert_request(chat(json!({
        "model": "glm-5.1",
        "messages": [
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "hi"}
        ],
    })))
    .expect("convert");
    assert_eq!(result.instructions, "");
    let input = serde_json::to_value(&result.input).expect("serialize input");
    assert_eq!(input[0]["role"], "system");
}

#[test]
fn chat_reasoning_effort_maps_to_reasoning() {
    let result = chat_completions::convert_request(chat(json!({
        "model": "glm-5.1",
        "messages": [{"role": "user", "content": "hi"}],
        "reasoning_effort": "high",
    })))
    .expect("convert");
    assert_eq!(
        result.reasoning.and_then(|r| r.effort).as_deref(),
        Some("high")
    );
}

#[test]
fn chat_reasoning_only_turns_round_trip_in_order() {
    use llmconduit::adapters::responses_to_chat::lower_request;

    let request = chat(json!({
        "model": "test",
        "messages": [
            {"role": "system", "content": "Return a chess action."},
            {"role": "user", "content": "Board, legal moves, and history."},
            {"role": "assistant", "content": null, "reasoning_content": "First analysis"},
            {"role": "user", "content": "Continue and return the final action."},
            {"role": "user", "content": "Remaining token budget: 10000"},
            {"role": "assistant", "content": null, "reasoning_content": "Second analysis"},
            {"role": "user", "content": "No final action content. Return JSON."},
            {"role": "user", "content": "Remaining token budget: 9000"},
            {"role": "assistant", "reasoning_content": "Third analysis"},
            {"role": "user", "content": "Continue."},
            {"role": "assistant", "content": "done", "reasoning_content": "Fourth analysis"},
            {"role": "user", "content": "Invalid JSON. Return a corrected action."}
        ]
    }));
    for end in 1..=request.messages.len() {
        let mut prefix = request.clone();
        prefix.messages.truncate(end);
        let expected = prefix.messages.clone();
        let canonical = chat_completions::convert_request(prefix).expect("convert");
        let lowered = lower_request(&canonical, vec![]).expect("lower");
        assert_eq!(lowered.messages, expected, "prefix length {end}");
    }
}

#[test]
fn chat_consecutive_reasoning_only_turns_keep_their_boundaries() {
    use llmconduit::adapters::responses_to_chat::lower_request;

    let request = chat(json!({
        "model": "test",
        "messages": [
            {"role": "user", "content": "Think."},
            {"role": "assistant", "content": null, "reasoning_content": "First turn"},
            {"role": "assistant", "content": "", "reasoning_content": "Second turn"},
            {"role": "assistant", "reasoning_content": "Third turn"},
            {"role": "user", "content": "Continue."}
        ]
    }));
    let expected = request.messages.clone();
    let canonical = chat_completions::convert_request(request).expect("convert");
    let lowered = lower_request(&canonical, vec![]).expect("lower");
    assert_eq!(lowered.messages, expected);
}

#[test]
fn chat_reasoning_only_turn_before_tool_calls_is_not_merged() {
    use llmconduit::adapters::responses_to_chat::lower_request;

    let request = chat(json!({
        "model": "test",
        "messages": [
            {"role": "user", "content": "Inspect and reply."},
            {"role": "assistant", "content": null, "reasoning_content": "Before tools"},
            {"role": "assistant", "content": null, "reasoning_content": "Using tools", "tool_calls": [
                {"id": "call_a", "index": 0, "type": "function", "function": {"name": "inspect", "arguments": {"query": "a"}}},
                {"id": "call_b", "index": 1, "type": "function", "function": {"name": "inspect", "arguments": {"query": "b"}}}
            ]},
            {"role": "tool", "tool_call_id": "call_a", "content": "A"},
            {"role": "tool", "tool_call_id": "call_b", "content": "B"},
            {"role": "assistant", "content": "Answer", "reasoning_content": "After tools"},
            {"role": "user", "content": "Continue."}
        ]
    }));
    let expected = request.messages.clone();
    let canonical = chat_completions::convert_request(request).expect("convert");
    let lowered = lower_request(&canonical, vec![]).expect("lower");
    assert_eq!(lowered.messages, expected);
}

#[test]
fn chat_assistant_text_reasoning_and_calls_round_trip_in_one_message() {
    use llmconduit::adapters::responses_to_chat::lower_request;

    for content in [serde_json::Value::Null, json!(""), json!("Looking it up.")] {
        let request = chat(json!({
            "model": "test",
            "messages": [
                {"role": "user", "content": "Inspect and reply."},
                {"role": "assistant", "content": "Earlier assistant turn."},
                {"role": "assistant", "content": content, "reasoning_content": "Need both results.", "tool_calls": [
                    {"id": "call_a", "index": 0, "type": "function", "function": {"name": "inspect", "arguments": {"query": "a"}}},
                    {"id": "call_b", "index": 1, "type": "function", "function": {"name": "inspect", "arguments": {"query": "b"}}}
                ]},
                {"role": "tool", "tool_call_id": "call_a", "content": "A"},
                {"role": "tool", "tool_call_id": "call_b", "content": "B"},
                {"role": "assistant", "content": "Answer", "reasoning_content": "Both results agree."},
                {"role": "user", "content": "Continue."}
            ]
        }));
        for end in 1..=request.messages.len() {
            let mut prefix = request.clone();
            prefix.messages.truncate(end);
            let expected = prefix.messages.clone();
            let canonical = chat_completions::convert_request(prefix).unwrap();
            let lowered = lower_request(&canonical, vec![]).unwrap();
            assert_eq!(lowered.messages, expected, "prefix length {end}");
        }
    }
}

#[test]
fn responses_reasoning_flushes_before_non_assistant_messages_and_tool_outputs() {
    use llmconduit::adapters::responses_to_chat::lower_request_with_image_agent_and_roles;
    use llmconduit::config::RolesConfig;
    use llmconduit::models::responses::ResponsesRequest;

    for boundary in [
        json!({"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Continue."}]}),
        json!({"type": "message", "role": "system", "content": [{"type": "input_text", "text": "An inline instruction."}]}),
        json!({"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "An inline instruction."}]}),
        json!({"type": "function_call_output", "call_id": "call_a", "output": "Result"}),
        json!({"type": "custom_tool_call_output", "call_id": "call_a", "output": "Result"}),
        json!({"type": "tool_search_output", "call_id": "call_a", "status": "completed", "execution": "client", "tools": []}),
    ] {
        let request: ResponsesRequest = serde_json::from_value(json!({
            "model": "test",
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Start."}]},
                {"type": "reasoning", "id": "r1", "summary": [], "content": [{"type": "reasoning_text", "text": "First reasoning"}], "encrypted_content": "signature_a"},
                boundary,
                {"type": "reasoning", "id": "r2", "summary": [], "content": [{"type": "reasoning_text", "text": "Second reasoning"}]},
                {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Answer"}]}
            ]
        })).expect("Responses request");
        let accepting_roles: RolesConfig = serde_json::from_value(json!({"*": {}})).unwrap();
        for roles in [None, Some(accepting_roles)] {
            let lowered =
                lower_request_with_image_agent_and_roles(&request, vec![], false, roles.as_ref())
                    .expect("lower");
            assert_eq!(lowered.messages.len(), 4, "boundary {boundary}");
            assert_eq!(lowered.messages[1].role, "assistant");
            assert_eq!(lowered.messages[1].content, None);
            assert_eq!(
                lowered.messages[1].reasoning_content.as_deref(),
                Some("First reasoning")
            );
            assert_eq!(
                lowered.messages[1]
                    .thinking
                    .as_ref()
                    .unwrap()
                    .signature
                    .as_deref(),
                Some("signature_a")
            );
            assert_eq!(
                lowered.messages[3].reasoning_content.as_deref(),
                Some("Second reasoning")
            );
            assert_eq!(lowered.messages[3].content, Some(json!("Answer")));
            assert_eq!(lowered.messages[3].thinking, None);
        }
    }
}

#[test]
fn chat_tool_choice_defaults_to_auto() {
    let result = chat_completions::convert_request(chat(json!({
        "model": "glm-5.1",
        "messages": [{"role": "user", "content": "hi"}],
    })))
    .expect("convert");
    assert_eq!(result.tool_choice, json!("auto"));
}

/// extra_body is the catch-all: an unknown provider knob round-trips untyped.
#[test]
fn chat_unknown_knob_round_trips_through_extra_body() {
    let result = chat_completions::convert_request(chat(json!({
        "model": "glm-5.1",
        "messages": [{"role": "user", "content": "hi"}],
        "provider_specific_knob": {"nested": 42},
    })))
    .expect("convert");
    assert_eq!(
        result.extra_body.get("provider_specific_knob"),
        Some(&json!({"nested": 42}))
    );
}
