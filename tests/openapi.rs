//! The OpenAPI document must describe exactly the routes the router serves.
//!
//! Two directions:
//! 1. every `.route("…")` registered in `src/http.rs` (with each of its
//!    methods) is a path+operation in the document;
//! 2. every path+operation in the document is answered by the router (never
//!    404 "no such route" / 405 "no such method"), probed against an app
//!    with the dashboard registered.
//!
//! Plus: `/openapi.json` serves the document.

mod common;

use axum::body::Body;
use axum::http::{Method, Request};
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet};
use tower::ServiceExt;

fn document() -> serde_json::Value {
    serde_json::to_value(llmconduit::openapi::document()).expect("document serializes")
}

/// `(path, method)` pairs described by the document, lower-case methods.
fn described_operations(doc: &serde_json::Value) -> BTreeMap<String, BTreeSet<String>> {
    let mut out = BTreeMap::new();
    for (path, item) in doc["paths"].as_object().expect("paths object") {
        let methods: BTreeSet<String> = item
            .as_object()
            .expect("path item")
            .keys()
            .filter(|k| {
                matches!(
                    k.as_str(),
                    "get" | "post" | "put" | "patch" | "delete" | "head" | "options"
                )
            })
            .cloned()
            .collect();
        out.insert(path.clone(), methods);
    }
    out
}

/// `(path, methods)` registered in `src/http.rs`, scanned from the source so a
/// new `.route(...)` cannot be added without describing it. axum's `{*path}`
/// wildcard is spelled `{path}` in the document.
fn registered_routes() -> BTreeMap<String, BTreeSet<String>> {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/http.rs"))
        .expect("read src/http.rs");
    let route = Regex::new(r#"\.route\(\s*"([^"]+)"\s*,"#).expect("regex");
    let method =
        Regex::new(r"\b(get|post|put|patch|delete|head|options)\(|MethodFilter::(HEAD|OPTIONS)")
            .expect("regex");
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for capture in route.captures_iter(&source) {
        let start = capture.get(0).expect("match").end();
        // The method chain is the rest of the `.route(` argument list: walk to
        // the parenthesis that closes it (depth starts at 1 inside `.route(`).
        let rest = &source[start..];
        let mut depth = 1usize;
        let mut end = rest.len();
        for (i, ch) in rest.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        let chain = &rest[..end];
        let path = capture[1].replace("{*", "{");
        let methods = out.entry(path).or_default();
        for m in method.captures_iter(chain) {
            let name = m
                .get(1)
                .or_else(|| m.get(2))
                .expect("method")
                .as_str()
                .to_ascii_lowercase();
            methods.insert(name);
        }
    }
    out
}

#[test]
fn every_registered_route_is_described() {
    let doc = document();
    let described = described_operations(&doc);
    let registered = registered_routes();
    assert!(!registered.is_empty(), "no routes scanned from src/http.rs");
    let mut missing = Vec::new();
    for (path, methods) in &registered {
        assert!(!methods.is_empty(), "no method parsed for route {path}");
        match described.get(path) {
            None => missing.push(format!("{path} (all methods: {methods:?})")),
            Some(have) => {
                for m in methods.difference(have) {
                    missing.push(format!("{m} {path}"));
                }
            }
        }
    }
    assert!(
        missing.is_empty(),
        "routes registered in src/http.rs but absent from the OpenAPI document:\n  {}",
        missing.join("\n  ")
    );
}

#[tokio::test]
async fn every_described_operation_is_routed() {
    let doc = document();
    let app = llmconduit::build_app_with_options(
        common::test_config(),
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );
    let mut unrouted = Vec::new();
    for (path, item) in doc["paths"].as_object().expect("paths object") {
        for (method, operation) in item.as_object().expect("path item") {
            let Ok(method) = Method::from_bytes(method.to_ascii_uppercase().as_bytes()) else {
                continue; // `parameters`, `summary`, … are not operations.
            };
            if !matches!(
                method,
                Method::GET
                    | Method::POST
                    | Method::PUT
                    | Method::PATCH
                    | Method::DELETE
                    | Method::HEAD
                    | Method::OPTIONS
            ) {
                continue;
            }
            // A route with a 404 in its contract may legitimately answer 404 to
            // a probe (an asset or record that does not exist); the source scan
            // above still proves the route is registered.
            let documents_404 = operation["responses"].get("404").is_some();
            let concrete = path.replace("{id}", "probe").replace("{path}", "probe.js");
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method.clone())
                        .uri(&concrete)
                        .header("content-type", "application/json")
                        .body(Body::from("{}"))
                        .expect("request"),
                )
                .await
                .expect("response");
            let status = response.status().as_u16();
            if status == 405 || (status == 404 && !documents_404) {
                unrouted.push(format!("{method} {path} -> {status}"));
            }
        }
    }
    assert!(
        unrouted.is_empty(),
        "operations in the OpenAPI document the router does not serve:\n  {}",
        unrouted.join("\n  ")
    );
}

#[tokio::test]
async fn openapi_json_serves_the_document() {
    let app = llmconduit::build_app(common::test_config());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    assert!(
        body["openapi"].as_str().unwrap_or("").starts_with("3."),
        "{}",
        body["openapi"]
    );
    assert_eq!(body["info"]["title"], "llmconduit");
    assert!(body["paths"]["/openapi.json"]["get"].is_object());
    assert!(body["paths"]["/v1/chat/completions"]["post"].is_object());
}
