//! Ported error-classification behaviors for the context-window-limit retry
//! (gap G1), adapted from claude-relay's `tests/test_server.py::test_non_200_retry_*`.
//!
//! The unit tests exercise the pure classifier
//! [`llmconduit::upstream::classify_context_overflow`] directly: each upstream
//! overflow shape must parse into the recomputed `max_completion_tokens`, the
//! right `reason`/lower-bound flags, the min-floor clamp, and unrelated text
//! must yield `None` (no retry). The integration test proves the classifier is
//! wired into the upstream non-2xx path so a real overflow triggers exactly one
//! shrink-and-retry.

use llmconduit::upstream::classify_context_overflow;

const MIN_FLOOR: i64 = 4096;

#[test]
fn detects_completion_token_limit() {
    let error_body = "max_completion_tokens=250000 cannot be greater than max_model_len=202,752";

    let retry = classify_context_overflow(error_body, MIN_FLOOR, None).expect("retry decision");

    assert_eq!(retry.reason, "completion_limit");
    assert_eq!(retry.ctx_limit, 202752);
    assert_eq!(retry.input_tokens, None);
    // ctx_limit - CONTEXT_RETRY_MARGIN(100), no input estimate available.
    assert_eq!(retry.max_completion_tokens, 202652);
}

#[test]
fn uses_available_context_for_completion_token_limit() {
    let error_body = "max_completion_tokens=250000 cannot be greater than max_model_len=202,752";

    let retry =
        classify_context_overflow(error_body, MIN_FLOOR, Some(139000)).expect("retry decision");

    assert_eq!(retry.reason, "completion_limit");
    assert_eq!(retry.ctx_limit, 202752);
    assert_eq!(retry.input_tokens, Some(139000));
    // 202752 - 100 - 139000.
    assert_eq!(retry.max_completion_tokens, 63652);
}

#[test]
fn detects_vllm_context_limit() {
    let error_body = "This model's maximum context length is 202752 tokens. \
        However, you requested 64000 output tokens and your prompt contains 139000 input tokens.";

    let retry = classify_context_overflow(error_body, MIN_FLOOR, None).expect("retry decision");

    assert_eq!(retry.reason, "context_limit");
    assert_eq!(retry.ctx_limit, 202752);
    assert_eq!(retry.input_tokens, Some(139000));
    assert!(!retry.input_tokens_is_lower_bound);
    // 202752 - 100 - 139000.
    assert_eq!(retry.max_completion_tokens, 63652);
}

#[test]
fn detects_vllm_at_least_context_limit() {
    let error_body = "This model's maximum context length is 65536 tokens. \
        However, you requested 64000 output tokens and your prompt contains at least 1537 input tokens, \
        for a total of at least 65537 tokens. Please reduce the length of the input prompt or the number \
        of requested output tokens. (parameter=input_tokens, value=1537)";

    let retry = classify_context_overflow(error_body, MIN_FLOOR, None).expect("retry decision");

    assert_eq!(retry.reason, "context_limit");
    assert_eq!(retry.ctx_limit, 65536);
    assert_eq!(retry.input_tokens, Some(1537));
    assert!(retry.input_tokens_is_lower_bound);
    // Lower-bound margin (1024): 65536 - 1024 - 1537.
    assert_eq!(retry.max_completion_tokens, 62975);
}

#[test]
fn uses_larger_margin_for_vllm_at_least_boundary_error() {
    let error_body = "This model's maximum context length is 262144 tokens. \
        However, you requested 63798 output tokens and your prompt contains at least 198347 input tokens, \
        for a total of at least 262145 tokens. Please reduce the length of the input prompt or the number \
        of requested output tokens. (parameter=input_tokens, value=198347)";

    let retry = classify_context_overflow(error_body, MIN_FLOOR, None).expect("retry decision");

    assert_eq!(retry.reason, "context_limit");
    assert_eq!(retry.ctx_limit, 262144);
    assert_eq!(retry.input_tokens, Some(198347));
    assert!(retry.input_tokens_is_lower_bound);
    // Lower-bound margin (1024): 262144 - 1024 - 198347.
    assert_eq!(retry.max_completion_tokens, 62773);
}

#[test]
fn detects_openai_compatible_context_limit() {
    let error_body = "This model's maximum context length is 202752 tokens. \
        However, you requested 203000 tokens (139000 in the messages, 64000 in the completion).";

    let retry = classify_context_overflow(error_body, MIN_FLOOR, None).expect("retry decision");

    assert_eq!(retry.reason, "context_limit");
    assert_eq!(retry.ctx_limit, 202752);
    assert_eq!(retry.input_tokens, Some(139000));
    // 202752 - 100 - 139000.
    assert_eq!(retry.max_completion_tokens, 63652);
}

#[test]
fn detects_openai_compatible_context_limit_in_the_prompt_variant() {
    // Canonical OpenAI overflow wording uses "in the prompt" (not "in the
    // messages"). Branch 3 must classify this exactly like the "in the messages"
    // variant and extract the same input (139000) / output (64000) split.
    let error_body = "This model's maximum context length is 202752 tokens. \
        However, you requested 203000 tokens (139000 in the prompt, 64000 in the completion).";

    let retry = classify_context_overflow(error_body, MIN_FLOOR, None).expect("retry decision");

    assert_eq!(retry.reason, "context_limit");
    assert_eq!(retry.ctx_limit, 202752);
    assert_eq!(retry.input_tokens, Some(139000));
    // 202752 - 100 - 139000.
    assert_eq!(retry.max_completion_tokens, 63652);
}

#[test]
fn detects_requested_token_count_context_limit() {
    let error_body = "Requested token count exceeds the model's maximum context length of 202752 tokens. \
        You requested a total of 206272 tokens: 142272 tokens from the input messages \
        and 64000 tokens for the completion. Please reduce the number of tokens in the \
        input messages or the completion to fit within the limit.";

    let retry = classify_context_overflow(error_body, MIN_FLOOR, None).expect("retry decision");

    assert_eq!(retry.reason, "context_limit");
    assert_eq!(retry.ctx_limit, 202752);
    assert_eq!(retry.input_tokens, Some(142272));
    // 202752 - 100 - 142272.
    assert_eq!(retry.max_completion_tokens, 60380);
}

#[test]
fn respects_min_completion_token_floor() {
    let error_body = "This model's maximum context length is 202752 tokens. \
        However, you requested 64000 output tokens and your prompt contains 202000 input tokens.";

    let retry = classify_context_overflow(error_body, MIN_FLOOR, None).expect("retry decision");

    // 202752 - 100 - 202000 = 652, clamped up to the configured floor.
    assert_eq!(retry.max_completion_tokens, MIN_FLOOR);
}

#[test]
fn ignores_unrelated_errors() {
    assert_eq!(
        classify_context_overflow("backend is unavailable", MIN_FLOOR, None),
        None
    );
}

#[test]
fn ignores_unrelated_body_that_merely_mentions_token_keywords() {
    // A validation/echo error that names the max-token parameters (and even
    // carries numbers next to them) but is NOT a context-window overflow. The
    // classifier must require the actual overflow wording, not bare keyword
    // presence, so this yields None (no retry, no shrink).
    let error_body = "Invalid request: parameter max_completion_tokens=64000 is not allowed \
        together with max_model_len=202752 for this deployment; remove one of them.";

    assert_eq!(
        classify_context_overflow(error_body, MIN_FLOOR, None),
        None,
        "an unrelated error mentioning max_completion_tokens/max_model_len must not trigger a retry"
    );
}

#[test]
fn ignores_body_with_overflow_anchors_but_no_requested_token_count_exceeds() {
    // The "Requested token count exceeds" shape is gated on that literal phrase.
    // A 4xx body that merely carries its generic anchors ("maximum context
    // length of …" + "tokens from input messages") WITHOUT the overflow
    // assertion must NOT be classified as an overflow (no shrink-and-retry).
    let error_body = "Configuration error: the maximum context length of 202752 tokens \
        was computed from 142272 tokens from input messages and the reserved completion \
        budget; adjust your deployment, this is not a per-request limit.";

    assert_eq!(
        classify_context_overflow(error_body, MIN_FLOOR, None),
        None,
        "anchors without the literal 'requested token count exceeds' must not trigger a retry"
    );
}

#[test]
fn ignores_requested_total_anchors_without_requested_token_count_exceeds_literal() {
    // Branch 4 (Codex round 6 overmatch regression): the "requested a total of"
    // shape's DISTINCTIVE LEADING literal is `Requested token count exceeds`.
    // This body carries EVERY generic anchor and token clause the branch keys on
    // -- "maximum context length of N tokens", "requested a total of N tokens",
    // "N tokens from input messages", "N tokens for the completion" -- but it is
    // a validation/diagnostics 400 rejecting a different field ("temperature"),
    // NOT a context overflow, and so it LACKS the leading "requested token count
    // exceeds" assertion. Before the fix this still classified and triggered a
    // shrink/retry; the leading-literal anchor must now reject it (None).
    let error_body = "Diagnostics: maximum context length of 202752 tokens. You requested \
        a total of 206272 tokens: 142272 tokens from input messages and 64000 tokens for \
        the completion, but the rejected field was temperature.";

    assert_eq!(
        classify_context_overflow(error_body, MIN_FLOOR, None),
        None,
        "all 'requested a total of' anchors present but the leading 'requested token count \
         exceeds' literal absent must not trigger a retry"
    );
}

#[test]
fn ignores_vllm_combined_anchors_without_requested_output_tokens_literal() {
    // Branch 2 (vLLM combined input+output) negative: a body carrying the
    // generic anchors ("maximum context length is …", bare "output tokens",
    // "prompt contains … input tokens") but WITHOUT the request-side
    // `requested N output tokens` literal must NOT classify. Here the request
    // side reads "we reserved N output tokens" (no "requested N output
    // tokens"), and the bare "output tokens" / "prompt contains" anchors are
    // present — proving co-presence of generic anchors is insufficient.
    let error_body = "Capacity note: this model's maximum context length is 202752 tokens. \
        We reserved 64000 output tokens for streaming; the cached prompt contains \
        139000 input tokens from a prior turn. This is informational, not a limit.";

    assert_eq!(
        classify_context_overflow(error_body, MIN_FLOOR, None),
        None,
        "generic combined anchors without the 'requested N output tokens' literal must not retry"
    );
}

#[test]
fn ignores_vllm_combined_when_request_side_follows_context_side() {
    // Branch 2 ordering negative: all the literal pieces exist, but the
    // request-side `requested N output tokens` clause appears AFTER the
    // context-side `prompt contains N input tokens` clause. The required
    // ordering (request side before context side) rejects it.
    let error_body = "This model's maximum context length is 202752 tokens. The prompt contains \
        139000 input tokens, which is fine; separately you requested 64000 output tokens earlier.";

    assert_eq!(
        classify_context_overflow(error_body, MIN_FLOOR, None),
        None,
        "out-of-order combined clauses (context side before request side) must not retry"
    );
}

#[test]
fn ignores_openai_compatible_anchors_without_completion_clause() {
    // Branch 3 (OpenAI-compatible) negative: a body with the generic anchors
    // ("maximum context length is …" + "in the messages") but missing both the
    // request-side `requested N tokens` literal and the `… in the completion`
    // half of the parenthetical. Co-presence of "maximum context length is" and
    // "in the messages" alone must NOT classify.
    let error_body = "Diagnostics: this model's maximum context length is 202752 tokens. \
        We counted 139000 tokens in the messages cache for telemetry purposes. No request was rejected.";

    assert_eq!(
        classify_context_overflow(error_body, MIN_FLOOR, None),
        None,
        "OpenAI-compatible anchors without the 'requested N tokens' + 'in the completion' literals must not retry"
    );
}

#[test]
fn ignores_openai_compatible_when_clauses_out_of_order() {
    // Branch 3 ordering negative: the request-side `requested N tokens`, the
    // `in the messages` split, and the `in the completion` half all exist, but
    // the completion clause precedes the messages clause. The enforced ordering
    // (requested -> messages -> completion) rejects it.
    let error_body = "This model's maximum context length is 202752 tokens. However, you requested \
        203000 tokens; 64000 in the completion budget were set before 139000 in the messages were tallied.";

    assert_eq!(
        classify_context_overflow(error_body, MIN_FLOOR, None),
        None,
        "out-of-order OpenAI-compatible clauses (completion before messages) must not retry"
    );
}

#[test]
fn ignores_requested_without_adjacent_token_count() {
    // Branch 3 (OpenAI-compatible) adjacency negative (Codex round 5): the body
    // carries every generic anchor -- "maximum context length is N tokens", the
    // word "requested", and a parenthetical with "in the prompt" + "in the
    // completion" -- but the word "requested" is NOT immediately followed by a
    // number ("requested operation", not "requested N tokens"). The reference
    // regex's tight `requested\s*([\d,]+)\s*tokens` adjacency rejects it, so it
    // must NOT classify (no genuine `requested N tokens` overflow).
    let error_body = "Invalid requested operation. This model's maximum context length is 202752 \
        tokens. Diagnostics: 139000 in the prompt, 64000 in the completion.";

    assert_eq!(
        classify_context_overflow(error_body, MIN_FLOOR, None),
        None,
        "a body whose 'requested' is not adjacent to 'N tokens' must not trigger a retry"
    );
}

#[test]
fn custom_floor_clamps_reduced_budget() {
    // A higher floor than the available budget wins, proving the floor knob is
    // honored rather than hard-coded.
    let error_body = "This model's maximum context length is 202752 tokens. \
        However, you requested 64000 output tokens and your prompt contains 200000 input tokens.";

    let retry = classify_context_overflow(error_body, 8192, None).expect("retry decision");

    // 202752 - 100 - 200000 = 2652, clamped up to 8192.
    assert_eq!(retry.max_completion_tokens, 8192);
}

mod integration {
    use serde_json::Value;
    use serde_json::json;
    use tower::ServiceExt;
    use wiremock::matchers::method;
    use wiremock::matchers::path;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use axum::body::Body;
    use axum::http::Request;
    use llmconduit::config::Config;
    use llmconduit::config::PersistedConfig;
    use uuid::Uuid;

    /// Parse an `upstreams:` / `model_profiles:` YAML through the production
    /// loader and return the resolved routing fields, so a test config's compiled
    /// "*" glob and fallbacks match production.
    fn routing_from_yaml(
        yaml: &str,
    ) -> (
        Vec<llmconduit::config::UpstreamConfig>,
        Vec<llmconduit::config::CompiledProfile>,
    ) {
        let persisted: PersistedConfig = serde_yaml::from_str(yaml).expect("routing yaml");
        let routed = Config::from_persisted(&persisted).expect("routing config");
        (routed.upstreams, routed.model_profiles)
    }

    fn config_for(server_uri: &str) -> Config {
        let (upstreams, model_profiles) = routing_from_yaml(&format!(
            "upstreams:\n  - name: primary\n    url: \"{server_uri}/v1/\"\nmodel_profiles:\n  \"*\":\n    upstream: primary\n"
        ));
        Config {
            bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: serde_json::Map::new(),
            upstreams,
            upstream_failure_cooldown_secs: 30,
            model_profiles,
            template_family: None,
            brave_base_url: "https://example.com/".parse().expect("url"),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout: std::time::Duration::from_secs(30),
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            debug_log_max_age_hours: None,
            min_completion_tokens: 4096,
            max_sse_frame_bytes: 8 * 1024 * 1024,
            max_request_body_bytes: 10 * 1024 * 1024,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            price_table: std::collections::HashMap::new(),
        }
    }

    fn chat_sse_body() -> String {
        let chunk = json!({
            "id": "chat-retry",
            "object": "chat.completion.chunk",
            "choices": [{
                "index": 0,
                "delta": {"role": "assistant", "content": "hello after retry"},
                "finish_reason": null
            }],
            "usage": null
        });
        format!("data: {}\n\ndata: [DONE]\n\n", chunk)
    }

    /// Upstream returns a context-limit 400 on the first chat POST, then 200 on
    /// the retry. The leaf client must shrink `max_completion_tokens` and retry
    /// exactly once (two POSTs total), and the turn must succeed. The reduced
    /// budget is checked on the second (retried) upstream request body.
    #[tokio::test]
    async fn upstream_context_limit_400_then_200_retries_once_with_reduced_budget() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": "test-model"}]
            })))
            .mount(&server)
            .await;

        // First chat POST -> context-limit 400. ctx=65536, input=1000,
        // margin=100 => reduced budget = 65536 - 100 - 1000 = 64436.
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_string(
                "This model's maximum context length is 65536 tokens. However, you requested \
                 250000 output tokens and your prompt contains 1000 input tokens.",
            ))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;

        // Retry -> 200 with a normal SSE body.
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(chat_sse_body()),
            )
            .with_priority(2)
            .mount(&server)
            .await;

        let app = llmconduit::build_app(config_for(&server.uri()));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "test-model",
                            "stream": false,
                            "max_tokens": 250000,
                            "messages": [{"role": "user", "content": "hi"}]
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status().as_u16(), 200, "turn should succeed");
        let body_bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read body");
        let body: Value = serde_json::from_slice(&body_bytes).expect("json body");
        assert_eq!(
            body["choices"][0]["message"]["content"].as_str(),
            Some("hello after retry")
        );

        let chat_requests: Vec<_> = server
            .received_requests()
            .await
            .expect("requests")
            .into_iter()
            .filter(|request| {
                request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
            })
            .collect();

        // Exactly ONE retry: the original POST plus a single re-request.
        assert_eq!(
            chat_requests.len(),
            2,
            "expected exactly one shrink-and-retry"
        );

        let first: Value = serde_json::from_slice(&chat_requests[0].body).expect("first body json");
        assert_eq!(
            first["max_tokens"].as_i64(),
            Some(250000),
            "first attempt keeps the caller's requested budget"
        );

        let retried: Value =
            serde_json::from_slice(&chat_requests[1].body).expect("retry body json");
        assert_eq!(
            retried["max_tokens"].as_i64(),
            Some(64436),
            "retry carries the reduced completion budget"
        );
    }

    /// A configured `upstream_chat_kwargs` max-token ALIAS (`max_completion_tokens`)
    /// flows into the request `extra_body` when the caller sends no max-token
    /// field. On a context-overflow retry the leaf shrinks the typed
    /// `max_output_tokens` (serialized as `max_tokens`) but must also STRIP the
    /// stale alias so the upstream cannot honor the oversized value and defeat the
    /// retry. The retried body must carry only the reduced `max_tokens` and none
    /// of the max-token aliases.
    #[tokio::test]
    async fn retry_strips_max_token_aliases_from_extra_body() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": "test-model"}]
            })))
            .mount(&server)
            .await;

        // First chat POST -> context-limit 400. ctx=65536, input=1000,
        // margin=100 => reduced budget = 65536 - 100 - 1000 = 64436.
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_string(
                "This model's maximum context length is 65536 tokens. However, you requested \
                 250000 output tokens and your prompt contains 1000 input tokens.",
            ))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(chat_sse_body()),
            )
            .with_priority(2)
            .mount(&server)
            .await;

        let mut config = config_for(&server.uri());
        // A configured max-token alias default. Because the caller sends no
        // max-token field, the engine leaves this in the request extra_body, so
        // the leaf retry sees a stale alias that must be stripped.
        config.upstream_chat_kwargs =
            serde_json::Map::from_iter([("max_completion_tokens".to_string(), json!(250000))]);

        let app = llmconduit::build_app(config);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "test-model",
                            "stream": false,
                            "messages": [{"role": "user", "content": "hi"}]
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status().as_u16(), 200, "turn should succeed");

        let chat_requests: Vec<_> = server
            .received_requests()
            .await
            .expect("requests")
            .into_iter()
            .filter(|request| {
                request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
            })
            .collect();
        assert_eq!(
            chat_requests.len(),
            2,
            "expected exactly one shrink-and-retry"
        );

        // First attempt: the configured alias is present (no typed field yet).
        let first: Value = serde_json::from_slice(&chat_requests[0].body).expect("first body json");
        assert_eq!(
            first["max_completion_tokens"].as_i64(),
            Some(250000),
            "first attempt carries the configured alias default"
        );

        // Retry: only the reduced typed `max_tokens` survives; every alias is gone.
        let retried: Value =
            serde_json::from_slice(&chat_requests[1].body).expect("retry body json");
        assert_eq!(
            retried["max_tokens"].as_i64(),
            Some(64436),
            "retry carries the reduced completion budget on the typed field"
        );
        assert!(
            retried.get("max_completion_tokens").is_none(),
            "retry must strip the stale max_completion_tokens alias"
        );
        assert!(
            retried.get("max_output_tokens").is_none(),
            "retry must not leak a max_output_tokens alias"
        );
    }

    /// A context-window overflow that PERSISTS after the single shrink-and-retry
    /// is a same-provider sizing problem, not a provider failure. With a fallback
    /// upstream configured, the leaf must surface a TERMINAL error so the failover
    /// client does NOT re-send the same oversized prompt to the next provider.
    /// We prove the fallback is never contacted and the turn fails (it does not
    /// silently succeed via the fallback).
    #[tokio::test]
    async fn persistent_overflow_after_retry_does_not_fail_over_to_next_provider() {
        let primary = MockServer::start().await;
        let fallback = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": "primary-model"}]
            })))
            .mount(&primary)
            .await;

        // Primary returns a context-overflow on BOTH the first POST and the retry.
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_string(
                "This model's maximum context length is 65536 tokens. However, you requested \
                 250000 output tokens and your prompt contains 1000 input tokens.",
            ))
            .mount(&primary)
            .await;

        // Fallback would happily succeed -- it must NEVER be reached.
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(chat_sse_body()),
            )
            .mount(&fallback)
            .await;

        // The `primary-model` profile serves the primary upstream and declares a
        // `fallback` hop (rewriting to `fallback-model`); a persistent overflow must
        // surface terminally rather than trip that fallback.
        let mut config = config_for(&primary.uri());
        let (upstreams, model_profiles) = routing_from_yaml(&format!(
            "upstreams:\n  - name: primary\n    url: \"{}/v1/\"\n  - name: fallback\n    url: \"{}/v1/\"\nmodel_profiles:\n  primary-model:\n    upstream: primary\n    fallbacks:\n      - upstream: fallback\n        model: fallback-model\n",
            primary.uri(),
            fallback.uri()
        ));
        config.upstreams = upstreams;
        config.model_profiles = model_profiles;
        config.upstream_failure_cooldown_secs = 3600;

        let app = llmconduit::build_app(config);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "primary-model",
                            "stream": false,
                            "max_tokens": 250000,
                            "messages": [{"role": "user", "content": "hi"}]
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        // Terminal upstream error (502), NOT a 200 produced by failing over.
        assert_eq!(
            response.status().as_u16(),
            502,
            "a persistent overflow must surface terminally, not succeed via failover"
        );
        let body_bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read body");
        let body: Value = serde_json::from_slice(&body_bytes).expect("json body");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("persisted after shrink-and-retry")),
            "error should describe the persistent overflow, got: {body}"
        );

        // Primary saw exactly the original POST plus one retry.
        let primary_posts = primary
            .received_requests()
            .await
            .expect("primary requests")
            .into_iter()
            .filter(|request| {
                request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
            })
            .count();
        assert_eq!(
            primary_posts, 2,
            "primary should get exactly one shrink-and-retry (2 POSTs)"
        );

        // The fallback provider must NEVER have been contacted.
        let fallback_posts = fallback
            .received_requests()
            .await
            .expect("fallback requests")
            .into_iter()
            .filter(|request| {
                request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
            })
            .count();
        assert_eq!(
            fallback_posts, 0,
            "a same-provider context overflow must NOT fail over to the next provider"
        );
    }

    /// Both the original POST and the G1 shrink-and-retry POST must land in the
    /// JSONL request log so the reduced budget is observable to `analyze-log`.
    /// Before T10 only the first POST was logged; here we assert the file holds
    /// two lines (original budget, then the reduced one) and that `analyze-log`
    /// reports the `max_tokens` change between them.
    #[tokio::test]
    async fn shrink_and_retry_post_is_logged() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": "test-model"}]
            })))
            .mount(&server)
            .await;

        // First chat POST -> context-limit 400. ctx=65536, input=1000,
        // margin=100 => reduced budget = 65536 - 100 - 1000 = 64436.
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_string(
                "This model's maximum context length is 65536 tokens. However, you requested \
                 250000 output tokens and your prompt contains 1000 input tokens.",
            ))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(chat_sse_body()),
            )
            .with_priority(2)
            .mount(&server)
            .await;

        let log_path = std::env::temp_dir().join(format!(
            "llmconduit-retry-log-{}.jsonl",
            Uuid::new_v4().simple()
        ));
        let mut config = config_for(&server.uri());
        config.upstreams[0].request_log_path = Some(log_path.clone());

        let app = llmconduit::build_app(config);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "test-model",
                            "stream": false,
                            "max_tokens": 250000,
                            "messages": [{"role": "user", "content": "hi"}]
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status().as_u16(), 200, "turn should succeed");
        let _ = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read body");

        let logged = std::fs::read_to_string(&log_path).expect("read request log");
        let lines: Vec<&str> = logged.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(
            lines.len(),
            2,
            "both the original and the retry POST must be logged, got: {logged}"
        );

        let first: Value = serde_json::from_str(lines[0]).expect("first logged request");
        assert_eq!(
            first["max_tokens"].as_i64(),
            Some(250000),
            "first logged request keeps the caller's budget"
        );
        let retried: Value = serde_json::from_str(lines[1]).expect("retried logged request");
        assert_eq!(
            retried["max_tokens"].as_i64(),
            Some(64436),
            "second logged request carries the reduced retry budget"
        );

        // `analyze-log` diffs consecutive request lines, so the reduced budget
        // surfaces as a changed `max_tokens` path -- both POSTs are visible to it.
        let report =
            llmconduit::request_log::analyze_request_log(&log_path, 10).expect("analyze log");
        assert!(
            report.contains("$.max_tokens"),
            "analyze-log should report the reduced-budget retry diff, got: {report}"
        );

        let _ = std::fs::remove_file(&log_path);
    }
}
