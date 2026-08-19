//! Inbound request-body size cap (`max_request_body_bytes`).
//!
//! llmconduit replaces axum's stock 2 MiB `DefaultBodyLimit` with a configurable
//! cap (default 10 MiB). A body larger than the cap is rejected with HTTP 413 by
//! the JSON extractor BEFORE model resolution or any upstream call; a body under
//! the cap passes the size gate and is handled normally. These tests pin the
//! default, the 413 rejection, and that the real-incident size (~3 MiB, which the
//! 2 MiB default used to reject) now passes.

use axum::body::Body;
use axum::body::Bytes;
use axum::http::Request;
use llmconduit::config::Config;
use llmconduit::config::PersistedConfig;
use serde_json::json;
use tower::ServiceExt;

/// A config with the given inbound body cap whose upstream points at a closed
/// port. Any request that passes the size gate then fails fast with a connection
/// error (5xx) — never 413 — so a 413 unambiguously means the body cap fired.
fn config_with_limit(max_request_body_bytes: usize) -> Config {
    let mut persisted = PersistedConfig {
        max_request_body_bytes,
        ..PersistedConfig::default()
    };
    // Point the default upstream at a closed port so any request that clears the
    // size gate then fails fast with a connection error (5xx), never a 413.
    persisted.upstreams[0].url = "http://127.0.0.1:1/v1".to_string();
    Config::from_persisted(&persisted).expect("resolve config")
}

fn chat_body(content_len: usize) -> String {
    json!({
        "model": "test-model",
        "stream": false,
        "messages": [{"role": "user", "content": "x".repeat(content_len)}],
    })
    .to_string()
}

async fn post_chat_status(app: axum::Router, body: String) -> u16 {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("request"),
    )
    .await
    .expect("response")
    .status()
    .as_u16()
}

#[test]
fn default_request_body_limit_is_10_mib() {
    let config = Config::from_persisted(&PersistedConfig::default()).expect("resolve config");
    assert_eq!(config.max_request_body_bytes, 10 * 1024 * 1024);
}

#[tokio::test]
async fn body_over_configured_limit_is_rejected_with_413() {
    let app = llmconduit::build_app(config_with_limit(4096));
    let body = chat_body(8192);
    assert!(
        body.len() > 4096,
        "test body must exceed the configured cap"
    );

    let status = post_chat_status(app, body).await;

    assert_eq!(
        status, 413,
        "body over the configured cap must be rejected with 413"
    );
}

#[tokio::test]
async fn three_mib_body_within_default_limit_is_not_size_rejected() {
    // The real incident: a ~3.1 MiB body that axum's stock 2 MiB default rejected
    // with 413. Under the 10 MiB default it must clear the size gate (then fail
    // downstream against the closed upstream) — any status but 413 proves the cap
    // did not fire.
    let app = llmconduit::build_app(config_with_limit(10 * 1024 * 1024));
    let body = chat_body(3 * 1024 * 1024);

    let status = post_chat_status(app, body).await;

    assert_ne!(status, 413, "a 3 MiB body must pass the 10 MiB size gate");
}

#[tokio::test]
async fn content_length_over_limit_is_rejected_before_routing() {
    // Hard-cap boundary: a declared Content-Length over the cap is refused with
    // 413 by the logging middleware BEFORE routing or body buffering, even on a
    // route that never reads a body. This is what makes the cap a real
    // memory-DoS bound rather than merely the JSON extractor's default.
    let app = llmconduit::build_app(config_with_limit(4096));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/health")
                .header("content-length", "1000000")
                .body(Body::from("x"))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(
        response.status().as_u16(),
        413,
        "over-limit Content-Length must be 413"
    );
}

#[tokio::test]
async fn length_less_body_over_limit_on_raw_bytes_route_is_413_not_400() {
    // No Content-Length header (length-less / chunked) on the raw-`Bytes` route:
    // the capped buffered read must classify the oversize as 413, not the
    // generic 400 used for genuinely broken / truncated streams.
    let app = llmconduit::build_app(config_with_limit(4096));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/completions")
                .header("content-type", "application/json")
                .body(Body::from("x".repeat(8192)))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(
        response.status().as_u16(),
        413,
        "length-less oversize must be 413, not 400"
    );
}

#[tokio::test]
async fn broken_under_limit_stream_is_400_not_413() {
    // A body that fails mid-read WITHOUT exceeding the cap is a truncated/broken
    // stream, not an oversize: it must map to 400, not 413. Proves the
    // classification keys on the actual `LengthLimitError`, not on whether a
    // Content-Length was present.
    let app = llmconduit::build_app(config_with_limit(1024 * 1024));
    let stream = futures::stream::iter(vec![
        Ok::<_, std::io::Error>(Bytes::from_static(b"partial")),
        Err(std::io::Error::other("connection reset")),
    ]);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from_stream(stream))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(
        response.status().as_u16(),
        400,
        "broken under-cap stream must be 400, not 413"
    );
}
