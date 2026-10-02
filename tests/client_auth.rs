use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use llmconduit::client_auth::ClientAuth;
use llmconduit::client_auth::VirtualKeySpec;
use llmconduit::client_auth::hash_secret;
use llmconduit::config::Config;
use llmconduit::config::PersistedConfig;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

const CLIENT_SECRET: &str = "client-secret";

fn client_auth(allowed_models: &[&str]) -> ClientAuth {
    ClientAuth::from_specs(
        false,
        [VirtualKeySpec {
            id: "key-1".to_string(),
            label: Some("integration key".to_string()),
            owner_id: Some("owner-1".to_string()),
            secret_hash: hash_secret(CLIENT_SECRET),
            allowed_models: allowed_models
                .iter()
                .map(|model| (*model).to_string())
                .collect(),
        }],
    )
    .expect("valid client-auth registry")
}

fn client_auth_app(
    server: &MockServer,
    allowed_models: &[&str],
    upstream_api_key: Option<&str>,
) -> Router {
    let persisted = PersistedConfig {
        upstream_base_url: format!("{}/v1", server.uri()),
        upstream_api_key: upstream_api_key.map(str::to_string),
        ..PersistedConfig::default()
    };
    let config = Config::from_persisted(&persisted).expect("valid test config");

    let (_unused_open_router, gateway) = llmconduit::build_app_with_gateway(config);
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_client_auth(client_auth(allowed_models)),
    );
    llmconduit::build_app_from_gateway(gateway)
}

fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("JSON response")
}

#[tokio::test]
async fn openai_surface_rejects_missing_and_invalid_keys_with_openai_shape() {
    let server = MockServer::start().await;
    let app = client_auth_app(&server, &["allowed-model"], None);
    let body = json!({"model": "allowed-model", "input": "hello"});

    for request in [json_request("POST", "/v1/responses", body.clone()), {
        let mut request = json_request("POST", "/v1/responses", body.clone());
        request
            .headers_mut()
            .insert("authorization", "Bearer wrong-secret".parse().unwrap());
        request
    }] {
        let response = app.clone().oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response_json(response).await,
            json!({
                "error": {
                    "message": "missing or invalid API key",
                    "type": "authentication_error"
                }
            })
        );
    }

    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty(),
        "authentication failures must not reach the upstream"
    );
}

#[tokio::test]
async fn anthropic_surface_uses_anthropic_auth_and_permission_error_shapes() {
    let server = MockServer::start().await;
    let app = client_auth_app(&server, &["allowed-model"], None);

    let missing = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/messages",
            json!({
                "model": "allowed-model",
                "max_tokens": 16,
                "messages": [{"role": "user", "content": "hello"}]
            }),
        ))
        .await
        .expect("response");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response_json(missing).await,
        json!({
            "type": "error",
            "error": {
                "type": "authentication_error",
                "message": "missing or invalid API key"
            }
        })
    );

    let mut forbidden = json_request(
        "POST",
        "/v1/messages",
        json!({
            "model": "blocked-model",
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "hello"}]
        }),
    );
    forbidden
        .headers_mut()
        .insert("x-api-key", CLIENT_SECRET.parse().unwrap());
    let forbidden = app.oneshot(forbidden).await.expect("response");
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        response_json(forbidden).await,
        json!({
            "type": "error",
            "error": {
                "type": "permission_error",
                "message": "API key is not authorized for the requested model"
            }
        })
    );
}

#[tokio::test]
async fn scoped_key_cannot_bypass_model_policy_by_omitting_model() {
    let server = MockServer::start().await;
    let app = client_auth_app(&server, &["allowed-model"], None);
    let mut request = json_request(
        "POST",
        "/v1/responses",
        json!({"input": "the engine would otherwise select a catalog default"}),
    );
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {CLIENT_SECRET}").parse().unwrap(),
    );

    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        response_json(response).await["error"]["message"],
        "a model is required for a model-scoped API key"
    );
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "authorization must run before catalog defaulting or upstream dispatch"
    );
}

#[tokio::test]
async fn anthropic_models_auth_failure_uses_anthropic_error_shape() {
    let server = MockServer::start().await;
    let app = client_auth_app(&server, &["allowed-model"], None);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header("anthropic-version", "2023-06-01")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response_json(response).await;
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "authentication_error");
    assert!(body.get("error").unwrap().get("message").is_some());
}

#[tokio::test]
async fn scoped_identity_filters_models_and_removes_the_shared_catalog_etag() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"upstream-catalog\"")
                .set_body_json(json!({
                    "object": "list",
                    "data": [
                        {"id": "allowed-model", "object": "model", "created": 1},
                        {"id": "blocked-model", "object": "model", "created": 2}
                    ]
                })),
        )
        .mount(&server)
        .await;
    let app = client_auth_app(&server, &["ALLOWED-MODEL"], None);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header("authorization", format!("Bearer {CLIENT_SECRET}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().get("etag").is_none(),
        "an upstream ETag cannot describe a key-filtered catalog"
    );
    let body = response_json(response).await;
    assert_eq!(body["object"], "list");
    assert_eq!(body["data"].as_array().expect("model list").len(), 1);
    assert_eq!(body["data"][0]["id"], "allowed-model");
}

#[tokio::test]
async fn allowed_raw_completion_reaches_upstream_without_the_client_x_api_key() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "cmpl-1",
            "object": "text_completion",
            "model": "allowed-model",
            "choices": []
        })))
        .mount(&server)
        .await;
    let app = client_auth_app(&server, &["allowed-model"], Some("upstream-secret"));

    let mut request = json_request(
        "POST",
        "/v1/completions",
        json!({"model": "allowed-model", "prompt": "hello"}),
    );
    request
        .headers_mut()
        .insert("x-api-key", CLIENT_SECRET.parse().unwrap());
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let received = server.received_requests().await.expect("received requests");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].url.path(), "/v1/completions");
    assert!(
        received[0].headers.get("x-api-key").is_none(),
        "the client credential must never be proxied upstream"
    );
    assert_eq!(
        received[0]
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer upstream-secret"),
        "only the configured upstream credential is sent"
    );
}

#[tokio::test]
async fn anthropic_head_and_options_probes_remain_open_when_client_auth_is_enabled() {
    let server = MockServer::start().await;
    let app = client_auth_app(&server, &["allowed-model"], None);

    for method in ["HEAD", "OPTIONS"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri("/v1/messages")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT, "{method}");
        assert_eq!(
            response
                .headers()
                .get("allow")
                .and_then(|value| value.to_str().ok()),
            Some("POST, HEAD, OPTIONS")
        );
    }
}
