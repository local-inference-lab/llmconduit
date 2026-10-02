//! Regression coverage for the public control-plane dependency-injection seam.
//!
//! These checks intentionally exercise the exported builder rather than only
//! validating the underlying config types: embedders can call this seam without
//! going through the binary's startup validation.

use axum::body::Body;
use http::Request;
use llmconduit::AppOptions;
use llmconduit::ControlPlaneRuntime;
use llmconduit::config::{Config, ModelProfile, ModelRoute, PersistedConfig};
use llmconduit::control_plane::{
    OperationalProviderPlan, OperationalRoutePlan, UnknownModelPolicy,
};
use serde_json::{Map as JsonMap, json};
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn base_config(upstream_base_url: &str) -> Config {
    Config::from_persisted(&PersistedConfig {
        upstream_base_url: upstream_base_url.to_string(),
        ..PersistedConfig::default()
    })
    .expect("test config")
}

fn operational_route(name: &str, base_url: &str) -> OperationalRoutePlan {
    let profile_id = Uuid::new_v4();
    OperationalRoutePlan {
        route_id: Uuid::new_v4(),
        name: name.to_string(),
        primary_profile_id: profile_id,
        primary_profile_name: format!("{name}-profile"),
        providers: vec![OperationalProviderPlan {
            backend_id: Uuid::new_v4(),
            backend_name: format!("{name}-backend"),
            profile_id,
            profile_name: format!("{name}-profile"),
            base_url: Url::parse(base_url).expect("provider URL"),
            api_key: None,
            request_log_path: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
        }],
    }
}

fn builder_error_for_header(header: &str) -> String {
    let result = llmconduit::build_app_with_gateway_control_plane_runtime(
        base_config("http://127.0.0.1:9/v1/"),
        None,
        AppOptions::default(),
        Vec::new(),
        UnknownModelPolicy::Passthrough,
        None,
        ControlPlaneRuntime {
            conversation_id_header: header.to_string(),
            ..ControlPlaneRuntime::default()
        },
    );
    match result {
        Ok(_) => panic!("header {header:?} unexpectedly passed builder validation"),
        Err(error) => error,
    }
}

#[test]
fn public_builder_rejects_sensitive_conversation_headers() {
    for header in ["authorization", "Authorization", "x-api-key", "api-key"] {
        let error = builder_error_for_header(header);
        assert!(
            error.contains("sensitive credential carrier"),
            "unexpected error for {header:?}: {error}"
        );
    }
}

#[test]
fn public_builder_rejects_invalid_conversation_header_syntax() {
    let error = builder_error_for_header("x-conversation id");
    assert!(
        error.contains("invalid conversation id header name"),
        "unexpected error: {error}"
    );
}

#[test]
fn public_builder_rejects_exact_config_and_operational_route_collision() {
    let mut config = base_config("http://127.0.0.1:9/v1/");
    config.model_routes.push(ModelRoute {
        name: "shared-model".to_string(),
        glob: None,
        upstream_base_url: Url::parse("http://127.0.0.1:10/v1/").expect("route URL"),
        upstream_model: None,
    });

    let result = llmconduit::build_app_with_gateway_control_plane(
        config,
        None,
        AppOptions::default(),
        vec![operational_route("SHARED-MODEL", "http://127.0.0.1:11/v1/")],
        UnknownModelPolicy::Passthrough,
        None,
    );
    let error = match result {
        Ok(_) => panic!("case-insensitive exact route collision was accepted"),
        Err(error) => error,
    };
    assert!(
        error.contains(
            "operational route 'SHARED-MODEL' conflicts with configured model route 'shared-model'"
        ),
        "unexpected error: {error}"
    );
}

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

#[tokio::test]
async fn operational_routes_preserve_primary_passthrough_for_unmatched_models() {
    let primary = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "passthrough-model"}]
        })))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_sse_body("chat-primary", "served by primary")),
        )
        .expect(1)
        .mount(&primary)
        .await;

    let config = base_config(&format!("{}/v1/", primary.uri()));
    assert!(
        config.upstreams.is_empty(),
        "fixture must remain routes-only"
    );
    assert!(
        config.model_routes.is_empty(),
        "only the operational route should engage routing mode"
    );
    let (app, _gateway) = llmconduit::build_app_with_gateway_control_plane(
        config,
        None,
        AppOptions::default(),
        vec![operational_route(
            "managed-alias",
            "http://127.0.0.1:11/v1/",
        )],
        UnknownModelPolicy::Passthrough,
        None,
    )
    .expect("routes-only control-plane app");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "passthrough-model",
                        "stream": false,
                        "messages": [{"role": "user", "content": "hello"}]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("gateway response");

    assert_eq!(response.status(), http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("response body");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "served by primary"
    );
}

#[tokio::test]
async fn reject_policy_rejects_backendless_profile_before_catalog_default_dispatch() {
    let primary = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "wielick-large"}]
        })))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&primary)
        .await;

    let mut config = base_config(&format!("{}/v1/", primary.uri()));
    config
        .model_profiles
        .insert("DeepSeek-V4.1-Flash".to_string(), ModelProfile::default());
    let (app, _gateway) = llmconduit::build_app_with_gateway_control_plane(
        config,
        None,
        AppOptions::default(),
        Vec::new(),
        UnknownModelPolicy::Reject,
        None,
    )
    .expect("reject-mode app");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "deepseek-v4.1-flash",
                        "stream": false,
                        "messages": [{"role": "user", "content": "hello"}]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("gateway response");

    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn reject_policy_allows_case_insensitive_genuine_catalog_match() {
    let primary = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "DeepSeek-V4.1-Flash"}]
        })))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({"model": "DeepSeek-V4.1-Flash"})))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_sse_body("chat-deepseek", "served by deepseek")),
        )
        .expect(1)
        .mount(&primary)
        .await;

    let mut config = base_config(&format!("{}/v1/", primary.uri()));
    config
        .model_profiles
        .insert("DeepSeek-V4.1-Flash".to_string(), ModelProfile::default());
    let (app, _gateway) = llmconduit::build_app_with_gateway_control_plane(
        config,
        None,
        AppOptions::default(),
        Vec::new(),
        UnknownModelPolicy::Reject,
        None,
    )
    .expect("reject-mode app");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "deepseek-v4.1-flash",
                        "stream": false,
                        "messages": [{"role": "user", "content": "hello"}]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("gateway response");

    assert_eq!(response.status(), http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("response body");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "served by deepseek"
    );
}

#[tokio::test]
async fn reject_policy_allows_genuine_catalog_model_without_profile() {
    let primary = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "wielick-large"}]
        })))
        .mount(&primary)
        .await;

    let config = base_config(&format!("{}/v1/", primary.uri()));
    let (_app, gateway) = llmconduit::build_app_with_gateway_control_plane(
        config,
        None,
        AppOptions::default(),
        Vec::new(),
        UnknownModelPolicy::Reject,
        None,
    )
    .expect("reject-mode app");

    let (resolved, genuine) = gateway
        .checked_resolve_request_model("wielick-large")
        .await
        .expect("genuine catalog model should be accepted under reject policy");
    assert_eq!(resolved, "wielick-large");
    assert!(genuine);
}

#[tokio::test]
async fn passthrough_policy_preserves_catalog_default_dispatch() {
    let primary = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "wielick-large"}]
        })))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({"model": "wielick-large"})))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_sse_body("chat-default", "served by default")),
        )
        .expect(1)
        .mount(&primary)
        .await;

    let config = base_config(&format!("{}/v1/", primary.uri()));
    let (app, _gateway) = llmconduit::build_app_with_gateway_control_plane(
        config,
        None,
        AppOptions::default(),
        Vec::new(),
        UnknownModelPolicy::Passthrough,
        None,
    )
    .expect("passthrough app");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "not-loaded",
                        "stream": false,
                        "messages": [{"role": "user", "content": "hello"}]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("gateway response");

    assert_eq!(response.status(), http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("response body");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "served by default"
    );
}

#[tokio::test]
async fn operational_alias_precedes_global_upstream_model_remap() {
    let primary = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "primary-catalog-model"}]
        })))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&primary)
        .await;

    let operational = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({"model": "operational-target"})))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_sse_body(
                    "chat-operational",
                    "served by operational backend",
                )),
        )
        .expect(1)
        .mount(&operational)
        .await;

    let mut config = base_config(&format!("{}/v1/", primary.uri()));
    config.upstream_model = Some("primary-remap".to_string());
    let mut route = operational_route("managed-model", &format!("{}/v1/", operational.uri()));
    route.providers[0].upstream_model = Some("operational-target".to_string());
    let (app, _gateway) = llmconduit::build_app_with_gateway_control_plane(
        config,
        None,
        AppOptions::default(),
        vec![route],
        UnknownModelPolicy::Passthrough,
        None,
    )
    .expect("operational alias app");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "managed-model",
                        "stream": false,
                        "messages": [{"role": "user", "content": "hello"}]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("gateway response");

    assert_eq!(response.status(), http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("response body");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "served by operational backend"
    );

    let primary_chat_requests = primary
        .received_requests()
        .await
        .expect("primary requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .count();
    assert_eq!(
        primary_chat_requests, 0,
        "global upstream_model must not steal an operational alias"
    );
}
