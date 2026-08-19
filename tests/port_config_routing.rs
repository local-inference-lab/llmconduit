//! Routing behaviors for the config port (claude-relay `test_config.py`). The
//! pure config/TOML resolution tests live in the sibling `tests/port_config.rs`;
//! this file covers profile ROUTE RESOLUTION (`Config::resolve_route`) plus the
//! per-model leaf finalization driven through the full gateway with a wiremock
//! upstream.
//!
//! Request-model routing is expressed through `model_profiles`: an exact key
//! (trimmed, case-insensitive) beats any glob, globs match first in declaration
//! order, and `upstream_model` names what the leaf serves.

mod common;

use common::config_from_yaml;
use llmconduit::config::Config;
use serde_json::json;
use tower::ServiceExt;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_partial_json;
use wiremock::matchers::method;
use wiremock::matchers::path;

use axum::body::Body;
use http::Request;

/// Minimal single-chunk chat-completions SSE body for a wiremock upstream.
fn chat_sse_body(id: &str, content: &str) -> String {
    let chunk = json!({
        "id": id,
        "choices": [{
            "index": 0,
            "delta": {"content": content},
            "finish_reason": null
        }],
        "usage": null
    });
    format!("data: {chunk}\n\ndata: [DONE]\n\n")
}

/// Mount a `/v1/models` catalog on `server` exposing exactly `ids`.
async fn mount_models_catalog(server: &MockServer, ids: &[&str]) {
    let data: Vec<_> = ids.iter().map(|id| json!({ "id": id })).collect();
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": data })))
        .mount(server)
        .await;
}

// ---------------------------------------------------------------------------
// `Config::resolve_route`: exact/glob/order/404/blank + `upstream_model`
// defaulting
// ---------------------------------------------------------------------------

#[test]
fn exact_profile_beats_glob_and_globs_match_in_declaration_order() {
    let yaml = r#"
upstreams: [{ name: a, url: "http://a/v1" }, { name: b, url: "http://b/v1" }, { name: c, url: "http://c/v1" }]
model_profiles:
  "claude-*":      { upstream: a }
  "claude-opus-*": { upstream: b }
  claude-opus-4:   { upstream: c, upstream_model: real-opus }
"#;
    let config = Config::from_persisted(&serde_yaml::from_str(yaml).unwrap()).unwrap();
    let exact = config.resolve_route("Claude-Opus-4").unwrap();
    assert_eq!(
        (exact.profile.upstream.as_str(), exact.served_model.as_str()),
        ("c", "real-opus")
    );
    assert!(!exact.matched_glob);
    let glob = config.resolve_route("claude-opus-4.1").unwrap();
    assert_eq!(glob.profile.upstream, "a"); // first declared glob wins
    assert_eq!(glob.served_model, "claude-opus-4.1"); // glob passes model through
    assert!(glob.matched_glob);
    assert!(config.resolve_route("gpt-4").is_none());
    assert!(config.resolve_route("  ").is_none());
}

#[test]
fn resolve_route_serves_upstream_model_then_key_then_request_model() {
    let yaml = r#"
upstreams: [{ name: u, url: "http://u/v1" }]
model_profiles:
  remapped:  { upstream: u, upstream_model: backend-x }
  identity:  { upstream: u }
  "pass-*":  { upstream: u }
"#;
    let config = Config::from_persisted(&serde_yaml::from_str(yaml).unwrap()).unwrap();
    // `upstream_model` wins.
    assert_eq!(
        config.resolve_route("remapped").unwrap().served_model,
        "backend-x"
    );
    // Else the exact key (trimmed) is served.
    assert_eq!(
        config.resolve_route(" identity ").unwrap().served_model,
        "identity"
    );
    // A glob with no `upstream_model` passes the request model through.
    assert_eq!(
        config.resolve_route("pass-7").unwrap().served_model,
        "pass-7"
    );
}

#[test]
fn resolve_route_blank_model_needs_glob_with_upstream_model() {
    // A `*` glob that sets `upstream_model` resolves a blank request model.
    let with_model = Config::from_persisted(
        &serde_yaml::from_str(
            r#"
upstreams: [{ name: u, url: "http://u/v1" }]
model_profiles:
  "*": { upstream: u, upstream_model: fallback-model }
"#,
        )
        .unwrap(),
    )
    .unwrap();
    let route = with_model.resolve_route("").unwrap();
    assert!(route.matched_glob);
    assert_eq!(route.served_model, "fallback-model");

    // The same `*` glob WITHOUT `upstream_model` cannot serve a blank model
    // (there is no request model to pass through).
    let without_model = Config::from_persisted(
        &serde_yaml::from_str(
            r#"
upstreams: [{ name: u, url: "http://u/v1" }]
model_profiles:
  "*": { upstream: u }
"#,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(without_model.resolve_route("   ").is_none());
    // A non-blank request still matches the `*` glob and passes through.
    assert_eq!(
        without_model
            .resolve_route("anything")
            .unwrap()
            .served_model,
        "anything"
    );
}

#[test]
fn validation_rejects_bad_refs() {
    for (profiles, needle) in [
        ("M: { upstream: missing }", "unknown upstream"),
        ("M: { upstream: l, fallbacks: [l] }", "primary"),
        ("M: { upstream: l, fallbacks: [o, o] }", "duplicate"),
        ("M: { upstream: l, image_analysis: { model: M } }", "itself"),
        (
            "M: { upstream: l, image_analysis: { model: nope } }",
            "unknown",
        ),
        (
            "M: { upstream: l, image_analysis: { model: \"v-*\" } }\n  \"v-*\": { upstream: l }",
            "glob",
        ),
        (
            "M: { upstream: l, image_analysis: { model: V } }\n  V: { upstream: l, image_analysis: { model: M } }",
            "image_analysis",
        ),
        ("M: {}", "upstream"),
    ] {
        let yaml = format!(
            "upstreams: [{{ name: l, url: \"http://h/v1\" }}, {{ name: o, url: \"http://o/v1\" }}]\nmodel_profiles:\n  {profiles}\n"
        );
        let err = Config::from_persisted(&serde_yaml::from_str(&yaml).unwrap()).unwrap_err();
        assert!(err.contains(needle), "{profiles}: {err}");
    }
}

// ---------------------------------------------------------------------------
// Per-model reasoning-effort map (applied at the upstream leaf)
// ---------------------------------------------------------------------------

/// End-to-end through the REAL upstream leaf: a request whose effort maps via a
/// model profile's `reasoning_effort_map` reaches the backend as
/// `chat_template_kwargs.reasoning_effort`, keyed by the FINAL resolved model.
/// The POST mock only fires when the body carries the mapped knob, so a 200 (vs
/// wiremock's 404 on no-match) proves the map was applied at the leaf.
#[tokio::test]
async fn reasoning_effort_map_reaches_backend_chat_template_kwargs() {
    let backend = MockServer::start().await;
    mount_models_catalog(&backend, &["GLM-test"]).await;
    // Only matches when the leaf placed the mapped effort in chat_template_kwargs
    // for the served backend model.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({
            "model": "GLM-test",
            "chat_template_kwargs": {"reasoning_effort": "high"}
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_sse_body("chat-glm", "ok")),
        )
        .mount(&backend)
        .await;

    let config = config_from_yaml(&format!(
        r#"
upstreams:
  - name: "backend"
    upstream_base_url: "{}/v1/"
model_profiles:
  GLM-test:
    upstream: backend
    reasoning_effort_default: max
    reasoning_effort_map:
      high: {{ chat_template_kwargs: {{ reasoning_effort: high }} }}
      max: {{ chat_template_kwargs: {{ reasoning_effort: max }} }}
"#,
        backend.uri()
    ));

    // Anthropic request for the profiled model with Claude Code's adaptive
    // thinking + output_config.effort=high.
    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .header("anthropic-version", "2023-06-01")
                .body(Body::from(
                    json!({
                        "model": "GLM-test",
                        "max_tokens": 16,
                        "stream": false,
                        "thinking": {"type": "adaptive"},
                        "output_config": {"effort": "high"},
                        "messages": [{"role": "user", "content": "hi"}]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(
        response.status().as_u16(),
        200,
        "leaf must POST chat_template_kwargs.reasoning_effort=high for the resolved GLM-test model"
    );
}
