use axum::body::Body;
use axum::body::Bytes;
use futures::StreamExt;
use http::{HeaderMap, Request};
use llmconduit::config::{Config, PersistedConfig};
use llmconduit::upstream::{
    BackendChatRequest, FailoverUpstreamProvider, ReqwestUpstreamClient, RoutingUpstreamClient,
    RoutingUpstreamProvider, UpstreamClient, UpstreamStream,
};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn sse(label: &str) -> String {
    let chunk = json!({"id": label, "choices": [{"index": 0,
        "delta": {"content": label}, "finish_reason": null}]});
    let end = json!({"id": label, "choices": [{"index": 0,
        "delta": {}, "finish_reason": "stop"}]});
    format!("data: {chunk}\n\ndata: {end}\n\ndata: [DONE]\n\n")
}

async fn server(label: &str, models: &[(&str, i64)]) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": models.iter()
            .map(|(id, limit)| json!({"id": id, "max_model_len": limit})).collect::<Vec<_>>()})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse(label)),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-replica", label)
                .set_body_string(label),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/tokenize"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"count": 42})))
        .mount(&server)
        .await;
    server
}

fn leaf(server: &MockServer) -> ReqwestUpstreamClient {
    ReqwestUpstreamClient::new(
        reqwest::Client::new(),
        format!("{}/v1", server.uri()).parse().unwrap(),
        None,
        None,
        false,
        1,
    )
}

fn provider(server: &MockServer, name: &str, group: Option<&str>) -> RoutingUpstreamProvider {
    RoutingUpstreamProvider::new(
        name,
        leaf(server),
        None,
        Default::default(),
        vec![],
        Duration::from_secs(60),
    )
    .with_load_balance_group(group.map(str::to_owned))
}

fn pair(a: &MockServer, b: &MockServer) -> RoutingUpstreamClient {
    RoutingUpstreamClient::new(vec![
        provider(a, "a", Some("replicas")),
        provider(b, "b", Some("replicas")),
    ])
}

fn request(model: &str) -> BackendChatRequest {
    BackendChatRequest::new(
        serde_json::from_value(json!({"model": model,
        "messages": [{"role": "user", "content": "hi"}], "stream": true}))
        .unwrap(),
        None,
        None,
        None,
    )
}

async fn open(client: &RoutingUpstreamClient, model: &str, label: &str) -> UpstreamStream {
    let mut stream = client
        .stream_chat_completion(&request(model))
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().id, label);
    stream
}

async fn drain(mut stream: UpstreamStream) {
    while let Some(chunk) = stream.next().await {
        chunk.unwrap();
    }
}

async fn hits(server: &MockServer, endpoint: &str) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.method == "POST" && request.url.path() == endpoint)
        .count()
}

#[test]
fn groups_round_trip_and_blank_names_are_rejected() {
    let yaml = "upstreams:\n  - name: a\n    upstream_base_url: http://localhost:8000/v1\n    load_balance_group: replicas\n";
    let persisted: PersistedConfig = serde_yaml::from_str(yaml).unwrap();
    let round_trip: PersistedConfig =
        serde_yaml::from_str(&serde_yaml::to_string(&persisted).unwrap()).unwrap();
    assert_eq!(
        Config::from_persisted(&round_trip).unwrap().upstreams[0]
            .load_balance_group
            .as_deref(),
        Some("replicas")
    );
    let toml: PersistedConfig = toml::from_str("[[upstreams]]\nupstream_base_url = 'http://localhost:8000/v1'\nload_balance_group = ' replicas '\n").unwrap();
    assert_eq!(
        Config::from_persisted(&toml).unwrap().upstreams[0]
            .load_balance_group
            .as_deref(),
        Some("replicas")
    );
    let mut blank = persisted.clone();
    blank.upstreams[0].load_balance_group = Some("  ".into());
    assert!(
        Config::from_persisted(&blank)
            .unwrap_err()
            .contains("upstreams[0].load_balance_group must not be blank")
    );
    blank.upstreams[0].load_balance_group = None;
    assert!(
        !serde_yaml::to_string(&blank)
            .unwrap()
            .contains("load_balance_group")
    );
}

#[tokio::test]
async fn equal_load_rotates_and_finished_streams_release_reservations() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    let client = pair(&a, &b);
    for label in ["a", "b", "a", "b"] {
        drain(open(&client, "model", label).await).await;
    }
    assert_eq!(
        client
            .supported_model_catalog()
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        vec!["model"]
    );
}

#[tokio::test]
async fn streams_remain_outstanding_until_eof_or_drop_across_client_clones() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    let client = pair(&a, &b);
    let a1 = open(&client, "model", "a").await;
    let b1 = open(&client.clone(), "model", "b").await;
    let a2 = open(&client, "model", "a").await;
    drop(b1);
    let b2 = open(&client, "model", "b").await;
    drain(b2).await;
    drain(open(&client, "model", "b").await).await;
    drop((a1, a2));
    drain(open(&client, "model", "a").await).await;
}

#[tokio::test]
async fn ungrouped_providers_keep_first_provider_routing() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    let client = RoutingUpstreamClient::new(vec![
        provider(&a, "a", None),
        provider(&b, "b", Some("replicas")),
    ]);
    let first = open(&client, "model", "a").await;
    drain(open(&client, "model", "a").await).await;
    drop(first);
    assert_eq!(hits(&b, "/v1/chat/completions").await, 0);
}

#[tokio::test]
async fn group_selection_requires_the_same_group_and_exact_resolved_model() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("other", 8192)]).await;
    let c = server("c", &[("model", 8192)]).await;
    let client = RoutingUpstreamClient::new(vec![
        provider(&a, "a", Some("replicas")),
        provider(&b, "b", Some("replicas")),
        provider(&c, "c", Some("different")),
    ]);
    let first = open(&client, "MODEL", "a").await;
    drain(open(&client, "model", "a").await).await;
    drain(open(&client, "other", "b").await).await;
    drop(first);
    assert_eq!(hits(&c, "/v1/chat/completions").await, 0);
}

#[tokio::test]
async fn plan_includes_each_members_own_context_limit_and_nested_models() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 4096)]).await;
    let provider_b = RoutingUpstreamProvider::new(
        "b",
        leaf(&b),
        None,
        Default::default(),
        vec![FailoverUpstreamProvider::new(
            "backup",
            leaf(&b),
            Some("backup-model".into()),
            Some("backup-alias".into()),
            Default::default(),
        )],
        Duration::from_secs(60),
    )
    .with_load_balance_group(Some("replicas".into()));
    let client = RoutingUpstreamClient::new(vec![provider(&a, "a", Some("replicas")), provider_b]);
    let plan = client.backend_candidate_plan("model").await;
    assert_eq!(
        plan.candidates
            .into_iter()
            .map(|c| (c.model, c.context_limit))
            .collect::<Vec<_>>(),
        vec![
            ("model".into(), Some(8192)),
            ("model".into(), Some(4096)),
            ("backup-model".into(), None)
        ]
    );
    let plan = client.backend_candidate_plan("backup-alias").await;
    assert_eq!(plan.candidates.len(), 1);
    assert_eq!(plan.candidates[0].model, "backup-model");
    assert_eq!(plan.candidates[0].context_limit, None);
    drain(open(&client, "backup-alias", "b").await).await;
    assert_eq!(hits(&a, "/v1/chat/completions").await, 0);
}

#[tokio::test]
async fn failed_dispatch_cools_member_without_retrying_another_member() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .mount(&a)
        .await;
    let client = pair(&a, &b);
    assert!(
        client
            .stream_chat_completion(&request("model"))
            .await
            .is_err()
    );
    assert_eq!(hits(&b, "/v1/chat/completions").await, 0);
    for _ in 0..3 {
        drain(open(&client, "model", "b").await).await;
    }
    assert_eq!(hits(&a, "/v1/chat/completions").await, 1);
}

#[tokio::test]
async fn intrinsic_errors_do_not_cool_members_or_leak_load() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400))
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&a)
        .await;
    let client = pair(&a, &b);
    assert!(
        client
            .stream_chat_completion(&request("model"))
            .await
            .is_err()
    );
    let busy_b = open(&client, "model", "b").await;
    drain(open(&client, "model", "a").await).await;
    drain(open(&client, "model", "a").await).await;
    drop(busy_b);
}

#[tokio::test]
async fn pending_headers_are_counted_and_cancellation_releases_load() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(move |_: &wiremock::Request| {
            signal.notify_one();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(30))
                .set_body_string(sse("a"))
        })
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&a)
        .await;
    let client = pair(&a, &b);
    let pending_client = client.clone();
    let pending = tokio::spawn(async move {
        pending_client
            .stream_chat_completion(&request("model"))
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    // Repeated sequential requests must avoid the member still waiting for headers.
    for _ in 0..3 {
        drain(open(&client, "model", "b").await).await;
    }
    pending.abort();
    assert!(pending.await.err().unwrap().is_cancelled());
    let held_a = open(&client, "model", "a").await;
    let held_b = open(&client, "model", "b").await;
    drop(held_a);
    for _ in 0..3 {
        drain(open(&client, "model", "a").await).await;
    }
    drop(held_b);
}

#[tokio::test]
async fn legacy_response_bodies_hold_load_and_preserve_headers() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    let client = pair(&a, &b);
    let body = Bytes::from_static(br#"{"model":"model","prompt":"hi"}"#);
    let a_response = client
        .proxy_completions(HeaderMap::new(), body.clone())
        .await
        .unwrap();
    assert_eq!(a_response.headers()["x-replica"], "a");
    for _ in 0..3 {
        let response = client
            .proxy_completions(HeaderMap::new(), body.clone())
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["x-replica"], "b");
        assert_eq!(response.text().await.unwrap(), "b");
    }
    drop(a_response);
    assert_eq!(
        client
            .proxy_completions(HeaderMap::new(), body)
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "a"
    );
}

#[tokio::test]
async fn token_counting_uses_available_group_members() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    let client = pair(&a, &b);
    let held_a = open(&client, "model", "a").await;
    for _ in 0..3 {
        assert_eq!(
            client.count_tokens(&request("model")).await.unwrap(),
            Some(42)
        );
    }
    assert_eq!(hits(&a, "/tokenize").await, 0);
    assert_eq!(hits(&b, "/tokenize").await, 3);
    drop(held_a);
}

#[tokio::test]
async fn configured_groups_balance_all_public_generation_protocols_in_strict_mode() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    let persisted: PersistedConfig = serde_json::from_value(json!({"strict_routing": true,
        "upstreams": [
            {"name": "a", "upstream_base_url": format!("{}/v1", a.uri()), "load_balance_group": "replicas",
                "upstream_chat_kwargs": {"temperature": 0.1}},
            {"name": "b", "upstream_base_url": format!("{}/v1", b.uri()), "load_balance_group": "replicas",
                "upstream_chat_kwargs": {"temperature": 0.2}}
        ]})).unwrap();
    let app = llmconduit::build_app(Config::from_persisted(&persisted).unwrap());
    for (endpoint, request) in [
        (
            "/v1/chat/completions",
            json!({"model": "model", "temperature": 0.7, "messages": [{"role": "user", "content": "hi"}]}),
        ),
        (
            "/v1/responses",
            json!({"model": "model", "temperature": 0.7, "input": "hi"}),
        ),
        (
            "/v1/messages",
            json!({"model": "model", "temperature": 0.7, "max_tokens": 16, "messages": [{"role": "user", "content": "hi"}]}),
        ),
        ("/v1/completions", json!({"model": "model", "prompt": "hi"})),
    ] {
        for label in ["a", "b"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(endpoint)
                        .header("content-type", "application/json")
                        .body(Body::from(request.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), 200, "{endpoint}");
            let bytes = axum::body::to_bytes(response.into_body(), 65536)
                .await
                .unwrap();
            let text = String::from_utf8(bytes.to_vec()).unwrap();
            if endpoint == "/v1/completions" {
                assert_eq!(text, label);
            } else {
                assert!(text.contains(&format!("\"{label}\"")), "{endpoint}: {text}");
            }
        }
    }
    for server in [&a, &b] {
        assert_eq!(hits(server, "/v1/chat/completions").await, 3);
        for request in server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == "/v1/chat/completions")
        {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["temperature"], 0.7);
        }
    }
}

#[tokio::test]
async fn healthy_primaries_precede_nested_fallbacks_and_cooling_groups_can_use_fallbacks() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    let backup_a = server("backup-a", &[]).await;
    let backup_b = server("backup-b", &[]).await;
    let with_backup = |primary, backup, name| {
        RoutingUpstreamProvider::new(
            name,
            leaf(primary),
            None,
            Default::default(),
            vec![FailoverUpstreamProvider::new(
                format!("{name}-backup"),
                leaf(backup),
                None,
                None,
                Default::default(),
            )],
            Duration::from_secs(60),
        )
        .with_load_balance_group(Some("replicas".into()))
    };
    let client = RoutingUpstreamClient::new(vec![
        with_backup(&a, &backup_a, "a"),
        with_backup(&b, &backup_b, "b"),
    ]);
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .mount(&a)
        .await;
    drain(open(&client, "model", "backup-a").await).await;
    drain(open(&client, "model", "b").await).await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .mount(&b)
        .await;
    drain(open(&client, "model", "backup-b").await).await;
    drain(open(&client, "model", "backup-a").await).await;
    assert_eq!(hits(&a, "/v1/chat/completions").await, 1);
    assert_eq!(hits(&b, "/v1/chat/completions").await, 2);
    assert_eq!(hits(&backup_a, "/v1/chat/completions").await, 2);
    assert_eq!(hits(&backup_b, "/v1/chat/completions").await, 1);
}

#[tokio::test]
async fn midstream_errors_surface_without_retry_and_cool_the_member() {
    let a = server("a", &[("model", 8192)]).await;
    let b = server("b", &[("model", 8192)]).await;
    let first = json!({"id": "a", "choices": [{"index": 0, "delta": {"content": "a"}, "finish_reason": null}]});
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(format!("data: {first}\n\ndata: invalid-json\n\n")),
        )
        .with_priority(1)
        .mount(&a)
        .await;
    let client = pair(&a, &b);
    let mut stream = open(&client, "model", "a").await;
    assert!(stream.next().await.unwrap().is_err());
    assert!(stream.next().await.is_none());
    assert_eq!(hits(&b, "/v1/chat/completions").await, 0);
    drain(open(&client, "model", "b").await).await;
}
