//! HTTP routing behaviors for gap G7 (claude-relay `test_config.py`), the
//! gateway-driving half of the config port. The pure config/TOML resolution
//! tests live in the sibling `tests/port_config.rs`; this file drives the full
//! gateway with wiremock upstreams and asserts which upstream received the
//! routed request and with what rewritten model.
//!
//! See `tests/port_config.rs` for the precedence model these tests exercise:
//! exact id > exact route > glob route > canonical key > default.

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

// ---------------------------------------------------------------------------
// Small route-target helpers: the repeated wiremock `/v1/models` + streaming
// `/v1/chat/completions` setup, factored so each test states only what differs.
// ---------------------------------------------------------------------------

/// Minimal single-chunk chat-completions SSE body for a wiremock upstream.
fn chat_sse_body(id: &str, content: &str) -> String {
    let chunk = json!({
        "id": id,
        "choices": [{
            "index": 0,
            "delta": {"content": content},
            "finish_reason": "stop"
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

/// Mount an UNCONDITIONAL streaming `/v1/chat/completions` target that always
/// answers with `chat_sse_body("chat", label)`. Used where the test asserts on
/// hit COUNT (a target that must, or must not, be reached) rather than the body.
async fn mount_chat_target(server: &MockServer, label: &str) {
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_sse_body("chat", label)),
        )
        .mount(server)
        .await;
}

/// Mount a streaming `/v1/chat/completions` target that only matches when the
/// request body carries `{"model": model}` — proving the leaf rewrote the model
/// to `model` before dispatch. Replies with `chat_sse_body(id, label)`.
async fn mount_chat_target_for_model(server: &MockServer, model: &str, id: &str, label: &str) {
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({ "model": model })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_sse_body(id, label)),
        )
        .mount(server)
        .await;
}

/// POST `model` to `/v1/chat/completions` on `app` (non-streaming) and return
/// the decoded JSON response body.
async fn post_chat(app: axum::Router, model: &str) -> serde_json::Value {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": model,
                        "stream": false,
                        "messages": [{"role": "user", "content": "hi"}]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");
    serde_json::from_slice(&body_bytes).expect("json body")
}

/// The `message.content` text of a non-streaming chat response.
fn chat_content(body: &serde_json::Value) -> Option<&str> {
    body["choices"][0]["message"]["content"].as_str()
}

/// Count POST `/v1/chat/completions` requests recorded by a wiremock `server`.
async fn chat_post_hits(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .expect("recorded requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .count()
}

// ---------------------------------------------------------------------------
// HTTP routing tests: glob match, exact-beats-glob, no-match -> default
// ---------------------------------------------------------------------------

/// A glob route (`claude-opus-*`) matches a request model and routes to the
/// configured upstream, rewriting the model to the route's `upstream_model`.
/// (claude-relay `test_resolve_backend_supports_glob_routes`.)
#[tokio::test]
async fn glob_route_matches_and_routes_to_configured_upstream() {
    let opus = MockServer::start().await;
    // Route providers are NOT catalog providers, so no `/v1/models` mock is
    // needed; only the dispatch endpoint matters. Match on the REWRITTEN model.
    mount_chat_target_for_model(&opus, "Kimi-K2.6", "chat-opus", "from-opus").await;

    let config = config_from_yaml(&format!(
        r#"
upstream_base_url: "http://unused.invalid/v1"
model_routes:
  "claude-opus-*":
    upstream_base_url: "{}/v1/"
    upstream_model: "Kimi-K2.6"
"#,
        opus.uri()
    ));

    let body = post_chat(llmconduit::build_app(config), "claude-opus-4-5-20251101").await;
    assert_eq!(chat_content(&body), Some("from-opus"));
}

/// T1: a routed model gets ITS OWN `template_family` override, not the request
/// alias's. A route maps alias "glm-alias" to an opaque target "opaque-target".
/// The alias's profile sets `template_family: kimi`; the TARGET's profile sets
/// `template_family: deepseek`. Pre-T1 the engine resolved the family from the
/// alias PRE-routing, so the alias's `kimi` bled onto the target (wrong family
/// kwargs). Post-T1 the leaf resolves family from the FINAL provider model, so
/// the target's `deepseek` wins — DeepSeek `enable_thinking` is injected and the
/// Kimi `thinking` knob is absent.
#[tokio::test]
async fn route_target_family_wins_over_alias_family_override() {
    let target = MockServer::start().await;
    mount_chat_target_for_model(&target, "opaque-target", "chat-tgt", "from-target").await;

    let config = config_from_yaml(&format!(
        r#"
upstream_base_url: "http://unused.invalid/v1"
model_routes:
  "glm-alias":
    upstream_base_url: "{}/v1/"
    upstream_model: "opaque-target"
model_profiles:
  "glm-alias":
    template_family: kimi
  "opaque-target":
    template_family: deepseek
"#,
        target.uri()
    ));

    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "glm-alias",
                        "stream": false,
                        "messages": [{"role": "user", "content": "hi"}]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);

    let posted: Vec<_> = target
        .received_requests()
        .await
        .expect("recorded requests")
        .into_iter()
        .filter(|r| r.method.as_str() == "POST" && r.url.path() == "/v1/chat/completions")
        .collect();
    assert_eq!(posted.len(), 1, "exactly one upstream POST");
    let body: serde_json::Value = posted[0].body_json().expect("chat request json");
    assert_eq!(body["model"].as_str(), Some("opaque-target"));
    // The TARGET's `deepseek` family applies (leaf resolved from the FINAL model).
    assert_eq!(body["chat_template_kwargs"]["enable_thinking"], json!(true));
    assert!(
        body["chat_template_kwargs"].get("thinking").is_none(),
        "Kimi `thinking` from the alias's family override must NOT bleed onto the deepseek target (T1): {:?}",
        body["chat_template_kwargs"]
    );
}

/// An exact catalog model id beats an overlapping glob route: the request goes
/// to the catalog upstream, NOT the glob route target. (AGENTS.md "Exact model
/// id wins"; claude-relay exact-before-glob.)
#[tokio::test]
async fn exact_catalog_id_beats_overlapping_glob_route() {
    let catalog = MockServer::start().await;
    let glob_target = MockServer::start().await;

    // The catalog upstream exposes an exact model id that ALSO matches the glob.
    mount_models_catalog(&catalog, &["claude-opus-exact"]).await;
    mount_chat_target_for_model(
        &catalog,
        "claude-opus-exact",
        "chat-catalog",
        "from-catalog",
    )
    .await;

    // The glob target must NEVER be hit for the exact id.
    mount_chat_target(&glob_target, "from-glob").await;

    let config = config_from_yaml(&format!(
        r#"
upstream_base_url: "http://unused.invalid/v1"
upstreams:
  - name: "catalog"
    upstream_base_url: "{}/v1/"
model_routes:
  "claude-opus-*":
    upstream_base_url: "{}/v1/"
    upstream_model: "glob-model"
"#,
        catalog.uri(),
        glob_target.uri()
    ));

    let body = post_chat(llmconduit::build_app(config), "claude-opus-exact").await;
    assert_eq!(
        chat_content(&body),
        Some("from-catalog"),
        "exact catalog id must win over the overlapping glob route"
    );

    assert_eq!(
        chat_post_hits(&glob_target).await,
        0,
        "glob route must not be hit for an exact id"
    );
}

/// A request model that matches no route and no catalog id falls back to the
/// default (first model of the first non-empty provider catalog), NOT a glob
/// route. (claude-relay `test_resolve_backend_falls_back_to_default_backend`.)
#[tokio::test]
async fn no_route_match_falls_back_to_default_catalog_model() {
    let catalog = MockServer::start().await;
    let glob_target = MockServer::start().await;

    mount_models_catalog(&catalog, &["default-model"]).await;
    // Default model is the catalog's first id; the request model is rewritten to it.
    mount_chat_target_for_model(&catalog, "default-model", "chat-default", "from-default").await;
    mount_chat_target(&glob_target, "from-glob").await;

    let config = config_from_yaml(&format!(
        r#"
upstream_base_url: "http://unused.invalid/v1"
upstreams:
  - name: "catalog"
    upstream_base_url: "{}/v1/"
model_routes:
  "claude-opus-*":
    upstream_base_url: "{}/v1/"
    upstream_model: "glob-model"
"#,
        catalog.uri(),
        glob_target.uri()
    ));

    let body = post_chat(llmconduit::build_app(config), "totally-unknown-model").await;
    assert_eq!(
        chat_content(&body),
        Some("from-default"),
        "a no-match request must fall back to the default catalog model"
    );

    assert_eq!(
        chat_post_hits(&glob_target).await,
        0,
        "non-matching model must not hit a glob route"
    );
}

/// A `--model-route` CLI spec injects a route that successfully dispatches a
/// matching request through the full gateway (parse + route end-to-end).
#[tokio::test]
async fn cli_model_route_injects_and_routes_request() {
    let target = MockServer::start().await;
    mount_chat_target_for_model(&target, "Qwen2.5", "chat-cli", "from-cli-route").await;

    // No config file; the route comes only from the CLI spec.
    let config = Config::from_env_file_and_routes(
        None,
        &[format!("claude-haiku={}/v1/,Qwen2.5", target.uri())],
    )
    .expect("config with CLI route");

    let body = post_chat(llmconduit::build_app(config), "claude-haiku").await;
    assert_eq!(chat_content(&body), Some("from-cli-route"));
}

// ---------------------------------------------------------------------------
// Declaration-order regression (overlapping globs) through the routing path
// ---------------------------------------------------------------------------

/// End-to-end: with two OVERLAPPING globs, the FIRST declared glob wins for an
/// ambiguous request model — and reversing the declaration flips the winner.
/// This locks first-match-by-declaration-order through the real routing path,
/// not alphabetical key order.
#[tokio::test]
async fn overlapping_glob_first_declared_wins_through_routing() {
    async fn route_target(label: &'static str) -> MockServer {
        let server = MockServer::start().await;
        mount_chat_target(&server, label).await;
        server
    }

    async fn winner_for_declaration(specific_first: bool) -> String {
        let opus = route_target("from-opus").await;
        let star = route_target("from-star").await;
        let opus_block = format!(
            "  \"claude-opus-*\":\n    upstream_base_url: \"{}/v1/\"\n",
            opus.uri()
        );
        let star_block = format!(
            "  \"claude-*\":\n    upstream_base_url: \"{}/v1/\"\n",
            star.uri()
        );
        let routes = if specific_first {
            format!("{opus_block}{star_block}")
        } else {
            format!("{star_block}{opus_block}")
        };
        let config = config_from_yaml(&format!(
            "upstream_base_url: \"http://unused.invalid/v1\"\nmodel_routes:\n{routes}"
        ));
        let body = post_chat(llmconduit::build_app(config), "claude-opus-x").await;
        chat_content(&body).expect("content").to_string()
    }

    // `claude-opus-*` declared first => it wins for `claude-opus-x`.
    assert_eq!(
        winner_for_declaration(true).await,
        "from-opus",
        "first-declared overlapping glob must win"
    );
    // Reverse the declaration => the broader `claude-*` now wins, proving order
    // (not alphabetical sorting) decides.
    assert_eq!(
        winner_for_declaration(false).await,
        "from-star",
        "reversing declaration order must flip the winning glob"
    );
}

// ---------------------------------------------------------------------------
// MIXED mode: `upstreams` catalog AND `model_routes` configured together. The
// engine must NOT pre-normalize a route-bound request model to the catalog
// default before the routing client can dispatch the route.
// ---------------------------------------------------------------------------

/// (a) With BOTH a catalog upstream and a glob route, a request matching ONLY
/// the glob (not any catalog id) routes to the glob target, NOT the catalog
/// default. Without the engine route-aware normalization fix this would collapse
/// to `default-model` and hit the catalog.
#[tokio::test]
async fn mixed_mode_glob_route_beats_catalog_default() {
    let catalog = MockServer::start().await;
    let glob_target = MockServer::start().await;

    mount_models_catalog(&catalog, &["default-model"]).await;
    // Catalog must NOT receive the routed chat request.
    mount_chat_target(&catalog, "from-catalog").await;
    mount_chat_target_for_model(&glob_target, "Kimi-K2.6", "chat-glob", "from-glob").await;

    let config = config_from_yaml(&format!(
        r#"
upstream_base_url: "http://unused.invalid/v1"
upstreams:
  - name: "catalog"
    upstream_base_url: "{}/v1/"
model_routes:
  "claude-opus-*":
    upstream_base_url: "{}/v1/"
    upstream_model: "Kimi-K2.6"
"#,
        catalog.uri(),
        glob_target.uri()
    ));

    let body = post_chat(llmconduit::build_app(config), "claude-opus-x").await;
    assert_eq!(
        chat_content(&body),
        Some("from-glob"),
        "a glob-only model must route to the glob target, not the catalog default"
    );

    assert_eq!(
        chat_post_hits(&catalog).await,
        0,
        "the catalog must not receive a route-bound request"
    );
}

/// (b) An EXACT route name beats a canonical-alias catalog match. The request
/// model's canonical key (`kimik2`) matches the catalog id `kimi-k2`, but its
/// exact name is a route — and exact-route precedence sits ABOVE canonical-key,
/// so the route wins. Without the fix the engine would canonicalize to the
/// catalog id and bypass the route.
#[tokio::test]
async fn mixed_mode_exact_route_name_beats_canonical_alias() {
    let catalog = MockServer::start().await;
    let route_target = MockServer::start().await;

    mount_models_catalog(&catalog, &["kimi-k2"]).await;
    mount_chat_target(&catalog, "from-catalog").await;
    mount_chat_target_for_model(&route_target, "routed-kimi", "chat-route", "from-route").await;

    // Route name "Kimi K2" canonicalizes to `kimik2` == catalog `kimi-k2`.
    let config = config_from_yaml(&format!(
        r#"
upstream_base_url: "http://unused.invalid/v1"
upstreams:
  - name: "catalog"
    upstream_base_url: "{}/v1/"
model_routes:
  "Kimi K2":
    upstream_base_url: "{}/v1/"
    upstream_model: "routed-kimi"
"#,
        catalog.uri(),
        route_target.uri()
    ));

    let body = post_chat(llmconduit::build_app(config), "Kimi K2").await;
    assert_eq!(
        chat_content(&body),
        Some("from-route"),
        "an exact route name must beat a canonical-alias catalog match"
    );

    assert_eq!(
        chat_post_hits(&catalog).await,
        0,
        "the canonical-alias catalog must not be hit"
    );
}

// ---------------------------------------------------------------------------
// Per-request model-resolution headers (debug aid for model mismatches)
// ---------------------------------------------------------------------------

/// A request for a model the backend does not serve falls back to the loaded
/// model, and the response is tagged with `x-llmconduit-model` (served) plus
/// `x-llmconduit-requested` (the original) so the mismatch is visible per
/// request — un-throttled, unlike the engine WARN. An exact match tags only the
/// served model and omits `x-llmconduit-requested`.
#[tokio::test]
async fn response_headers_expose_model_fallback() {
    let backend = MockServer::start().await;
    mount_models_catalog(&backend, &["served-model"]).await;
    mount_chat_target(&backend, "ok").await;

    let config = config_from_yaml(&format!(
        "upstreams:\n  - name: \"local\"\n    upstream_base_url: \"{}/v1/\"\n",
        backend.uri()
    ));

    let header = |response: &axum::response::Response, name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    let request = |model: &str| {
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": model,
                    "stream": false,
                    "messages": [{"role": "user", "content": "hi"}]
                })
                .to_string(),
            ))
            .expect("request")
    };

    // Mismatch: requested model is not served -> fallback + BOTH headers.
    let mismatch = llmconduit::build_app(config.clone())
        .oneshot(request("claude-opus-4"))
        .await
        .expect("response");
    assert_eq!(mismatch.status().as_u16(), 200);
    assert_eq!(
        header(&mismatch, "x-llmconduit-model").as_deref(),
        Some("served-model")
    );
    assert_eq!(
        header(&mismatch, "x-llmconduit-requested").as_deref(),
        Some("claude-opus-4")
    );

    // Exact match: served model requested -> served tag only, no `requested`.
    let exact = llmconduit::build_app(config)
        .oneshot(request("served-model"))
        .await
        .expect("response");
    assert_eq!(exact.status().as_u16(), 200);
    assert_eq!(
        header(&exact, "x-llmconduit-model").as_deref(),
        Some("served-model")
    );
    assert_eq!(header(&exact, "x-llmconduit-requested"), None);
}

// ---------------------------------------------------------------------------
// Per-model reasoning-effort map (applied at the upstream leaf)
// ---------------------------------------------------------------------------

/// End-to-end through the REAL upstream leaf: a request whose effort maps via a
/// model profile's `reasoning_effort_map` reaches the backend as
/// `chat_template_kwargs.reasoning_effort`, against the FINAL resolved model.
/// The POST mock only fires when the body carries the mapped knob, so a 200 (vs
/// wiremock's 404 on no-match) proves the map was applied at the leaf.
#[tokio::test]
async fn reasoning_effort_map_reaches_backend_chat_template_kwargs() {
    let backend = MockServer::start().await;
    mount_models_catalog(&backend, &["GLM-test"]).await;
    // Only matches when the leaf placed the mapped effort in chat_template_kwargs
    // AND resolved the request model to the served backend model.
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
    reasoning_effort_default: max
    reasoning_effort_map:
      high: {{ chat_template_kwargs: {{ reasoning_effort: high }} }}
      max: {{ chat_template_kwargs: {{ reasoning_effort: max }} }}
"#,
        backend.uri()
    ));

    // Anthropic request with Claude Code's adaptive-thinking + output_config.effort=high,
    // model unserved by the backend so it falls back to GLM-test.
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
                        "model": "claude-opus-4",
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
