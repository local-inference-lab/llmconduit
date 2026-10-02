//! Durable terminal persistence at the engine's authoritative result seam.

mod common;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use common::{MockSearch, base_request, collect_stream, content_chunk, finish_chunk, usage_chunk};
use futures::stream;
use llmconduit::client_auth::{ClientAuth, VirtualKeySpec, hash_secret};
use llmconduit::config::{Config, PersistedConfig, PersistedFallbackUpstream};
use llmconduit::control_plane_store::{
    BodyWrite, EventRow, PersistenceQueue, PersistenceWriter, RequestFinish, RequestRow,
    StoreResult,
};
use llmconduit::dashboard_flow::{Attempt, AttemptStatus, DashboardFlowStore};
use llmconduit::engine::Gateway;
use llmconduit::error::AppError;
use llmconduit::monitor::MonitorHub;
use llmconduit::replay::ReplayStore;
use llmconduit::upstream::{
    BackendChatRequest, UpstreamClient, UpstreamModelEntry, UpstreamModelsResponse, UpstreamStream,
};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Default)]
struct RecordingWriter {
    begins: Mutex<Vec<RequestRow>>,
    events: Mutex<Vec<EventRow>>,
    bodies: Mutex<Vec<BodyWrite>>,
    sessions: Mutex<Vec<llmconduit::sessions::SessionRow>>,
    finishes: Mutex<Vec<(String, RequestFinish)>>,
}

impl RecordingWriter {
    /// Reassemble the full body of one request-side hop from its skeleton
    /// event and recorded blobs, the way the history API does.
    fn assembled_body(&self, seq: i64, protocol: &str) -> serde_json::Value {
        let bodies = self.bodies.lock().unwrap();
        let body = bodies
            .iter()
            .find(|body| body.event.seq == seq)
            .expect("hop stored as a split body");
        let envelope = event_envelope(&body.event);
        let skeleton = envelope["content"].as_str().expect("skeleton content");
        let blobs = body
            .blobs
            .iter()
            .map(|blob| (blob.hash.clone(), blob.content.clone()))
            .collect::<std::collections::HashMap<_, _>>();
        let assembled = llmconduit::content_store::assemble(protocol, skeleton, |hash| {
            blobs.get(hash).cloned()
        })
        .expect("every referenced blob was written with the body");
        serde_json::from_str(&assembled).expect("assembled body is JSON")
    }
}

#[tokio::test]
async fn persistence_upstream_hops_follow_the_final_context_shrink_retry() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"id": "served-model"}]
        })))
        .mount(&server)
        .await;
    let overflow = "This model's maximum context length is 202752 tokens. \
        However, you requested 64000 output tokens and your prompt contains 139000 input tokens.";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_string(overflow))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse(&[
                    serde_json::json!({
                        "id": "chat-retry",
                        "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": null}]
                    }),
                    serde_json::json!({
                        "id": "chat-retry",
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
                    }),
                ])),
        )
        .mount(&server)
        .await;

    let config = Config::from_persisted(&PersistedConfig {
        upstream_base_url: format!("{}/v1", server.uri()),
        ..PersistedConfig::default()
    })
    .unwrap();
    let (_unused, gateway) = llmconduit::build_app_with_gateway(config);
    let writer = Arc::new(RecordingWriter::default());
    let queue = PersistenceQueue::spawn(
        Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
        NonZeroUsize::new(32).unwrap(),
    );
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_persistence_queue(queue.clone()),
    );
    let app = llmconduit::build_app_from_gateway(gateway);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "model": "served-model",
                        "input": "hello",
                        "stream": false,
                        "max_output_tokens": 64000
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(response.into_body(), 256 * 1024)
        .await
        .unwrap();
    queue.flush().await.unwrap();

    let received = server.received_requests().await.unwrap();
    let posts = received
        .iter()
        .filter(|request| request.method.as_str() == "POST")
        .collect::<Vec<_>>();
    assert_eq!(posts.len(), 2);
    let retry: serde_json::Value = serde_json::from_slice(&posts[1].body).unwrap();
    assert_eq!(retry["max_tokens"], 63_652);

    let request = writer.assembled_body(2, llmconduit::content_store::PROTOCOL_CHAT_COMPLETIONS);
    assert_eq!(request, retry, "durable request is the final on-wire retry");
    let events = writer.events.lock().unwrap();
    let request_event = event_envelope(events.iter().find(|event| event.seq == 2).unwrap());
    assert_eq!(
        request_event["items"], 1,
        "one message item on the upstream hop"
    );
    let response = event_envelope(events.iter().find(|event| event.seq == 3).unwrap());
    assert!(
        !response["content"]
            .as_str()
            .unwrap()
            .contains("maximum context")
    );
    assert!(response["content"].as_str().unwrap().contains("chat-retry"));
}

#[tokio::test]
async fn extractor_rejection_records_absent_upstream_hops_as_partial() {
    let server = MockServer::start().await;
    let config = Config::from_persisted(&PersistedConfig {
        upstream_base_url: format!("{}/v1", server.uri()),
        ..PersistedConfig::default()
    })
    .unwrap();
    let (_unused, gateway) = llmconduit::build_app_with_gateway(config);
    let writer = Arc::new(RecordingWriter::default());
    let queue = PersistenceQueue::spawn(
        Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
        NonZeroUsize::new(32).unwrap(),
    );
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_persistence_queue(queue.clone()),
    );
    let app = llmconduit::build_app_from_gateway(gateway);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                // Valid JSON passes ingress; the typed request is invalid because
                // `messages` is required.
                .body(Body::from(r#"{"model":"served-model"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let _ = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    queue.flush().await.unwrap();

    let mut events = writer.events.lock().unwrap().clone();
    events.sort_by_key(|event| event.seq);
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(event_envelope(&events[1])["partial"], true);
    assert_eq!(event_envelope(&events[2])["partial"], true);
    assert_eq!(event_envelope(&events[3])["partial"], false);
    {
        let finishes = writer.finishes.lock().unwrap();
        assert_eq!(finishes.len(), 1);
        assert_eq!(finishes[0].1.status, "failed");
        assert_eq!(finishes[0].1.terminal_reason.as_deref(), Some("http_422"));
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn persistence_begin_row_carries_the_detected_harness_identity() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"id": "served-model"}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse(&[
                    serde_json::json!({
                        "id": "chat-1",
                        "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": null}]
                    }),
                    serde_json::json!({
                        "id": "chat-1",
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
                    }),
                ])),
        )
        .mount(&server)
        .await;
    let config = Config::from_persisted(&PersistedConfig {
        upstream_base_url: format!("{}/v1", server.uri()),
        ..PersistedConfig::default()
    })
    .unwrap();
    let (_unused, gateway) = llmconduit::build_app_with_gateway(config);
    let writer = Arc::new(RecordingWriter::default());
    let queue = PersistenceQueue::spawn(
        Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
        NonZeroUsize::new(32).unwrap(),
    );
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_persistence_queue(queue.clone()),
    );
    let app = llmconduit::build_app_from_gateway(gateway);

    // A Claude Code sub-agent request on the Anthropic route: session header,
    // agent id on top, and the JSON-string metadata.user_id.
    let user_id = serde_json::json!({
        "device_id": "dev",
        "account_uuid": "0f0f0f0f-0000-4000-8000-000000000001",
        "session_id": "11111111-1111-4111-8111-111111111111"
    })
    .to_string();
    let body = serde_json::json!({
        "model": "served-model",
        "max_tokens": 32,
        "messages": [{"role": "user", "content": "hello"}],
        "metadata": {"user_id": user_id}
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .header("user-agent", "claude-cli/2.1.205 (external, cli)")
                .header(
                    "x-claude-code-session-id",
                    "11111111-1111-4111-8111-111111111111",
                )
                .header("x-claude-code-agent-id", "agent-7")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(response.into_body(), 256 * 1024)
        .await
        .unwrap();
    queue.flush().await.unwrap();

    let begins = writer.begins.lock().unwrap();
    assert_eq!(begins.len(), 1);
    let begin = &begins[0];
    assert_eq!(begin.client_protocol, "anthropic_messages");
    assert_eq!(begin.harness.as_deref(), Some("claude-code"));
    assert_eq!(begin.harness_version.as_deref(), Some("2.1.205"));
    assert_eq!(
        begin.harness_session_id.as_deref(),
        Some("11111111-1111-4111-8111-111111111111")
    );
    assert_eq!(begin.harness_sub_session_id.as_deref(), Some("agent-7"));
    assert_eq!(begin.harness_parent_session_id, None);
    assert_eq!(begin.session_kind, None);
}

#[tokio::test]
async fn persistence_links_requests_into_sessions_and_flags_cache_busts() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"id": "served-model"}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse(&[
                    serde_json::json!({
                        "id": "chat-1",
                        "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": null}]
                    }),
                    serde_json::json!({
                        "id": "chat-1",
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
                    }),
                ])),
        )
        .mount(&server)
        .await;
    let config = Config::from_persisted(&PersistedConfig {
        upstream_base_url: format!("{}/v1", server.uri()),
        ..PersistedConfig::default()
    })
    .unwrap();
    let (_unused, gateway) = llmconduit::build_app_with_gateway(config);
    let writer = Arc::new(RecordingWriter::default());
    let queue = PersistenceQueue::spawn(
        Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
        NonZeroUsize::new(64).unwrap(),
    );
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_persistence_queue(queue.clone()),
    );
    let app = llmconduit::build_app_from_gateway(gateway);

    let send = |messages: serde_json::Value, tools: serde_json::Value| {
        let app = app.clone();
        async move {
            let body = serde_json::json!({
                "model": "served-model",
                "messages": messages,
                "tools": tools,
                "stream": false
            });
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/chat/completions")
                        .header("content-type", "application/json")
                        .header("user-agent", "codex_cli_rs/0.104.0 (linux)")
                        .header("originator", "codex_cli_rs")
                        .header("session-id", "codex-session-1")
                        .header("thread-id", "codex-session-1")
                        .header("authorization", "Bearer client-key-xyz")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let _ = axum::body::to_bytes(response.into_body(), 256 * 1024)
                .await
                .unwrap();
        }
    };
    let tools =
        serde_json::json!([{"type": "function", "function": {"name": "read", "parameters": {}}}]);
    let sys = serde_json::json!({"role": "system", "content": "be brief"});
    let u1 = serde_json::json!({"role": "user", "content": "hi"});
    let a1 = serde_json::json!({"role": "assistant", "content": "ok"});
    let u2 = serde_json::json!({"role": "user", "content": "more"});
    // Turn 1, turn 2 (pure append), turn 3 with a changed tool list (cache bust).
    send(serde_json::json!([sys, u1]), tools.clone()).await;
    send(serde_json::json!([sys, u1, a1, u2]), tools.clone()).await;
    let changed_tools =
        serde_json::json!([{"type": "function", "function": {"name": "write", "parameters": {}}}]);
    send(serde_json::json!([sys, u1, a1, u2, a1, u1]), changed_tools).await;
    queue.flush().await.unwrap();

    let begins = writer.begins.lock().unwrap();
    assert_eq!(begins.len(), 3);
    let session_id = begins[0]
        .session_id
        .clone()
        .expect("linked to a session node");
    assert!(
        begins
            .iter()
            .all(|row| row.session_id.as_deref() == Some(session_id.as_str()))
    );
    assert_eq!(begins[0].divergence_kind.as_deref(), Some("new_chain"));
    assert_eq!(begins[0].chain_parent_request_id, None);
    assert_eq!(begins[0].item_count, Some(3));
    assert_eq!(begins[0].cache_bust, Some(false));
    assert_eq!(begins[0].harness.as_deref(), Some("codex"));
    assert_eq!(
        begins[0]
            .client_label
            .as_deref()
            .map(|l| l.starts_with("key-")),
        Some(true)
    );
    assert_eq!(begins[0].client_source.as_deref(), Some("key_hash"));

    assert_eq!(begins[1].divergence_kind.as_deref(), Some("append"));
    assert_eq!(
        begins[1].chain_parent_request_id.as_deref(),
        Some(begins[0].id.as_str())
    );
    assert_eq!(begins[1].shared_prefix_items, Some(3));
    assert_eq!(begins[1].cache_bust, Some(false));

    assert_eq!(begins[2].divergence_kind.as_deref(), Some("tools_changed"));
    assert_eq!(
        begins[2].chain_parent_request_id.as_deref(),
        Some(begins[1].id.as_str())
    );
    assert_eq!(
        begins[2].divergence_index,
        Some(0),
        "tools precede messages in item order"
    );
    assert_eq!(begins[2].cache_bust, Some(true));

    let sessions = writer.sessions.lock().unwrap();
    assert!(!sessions.is_empty());
    let last = sessions.last().unwrap();
    assert_eq!(last.id, session_id);
    assert_eq!(last.kind, "declared");
    assert_eq!(last.harness, "codex");
    assert_eq!(last.external_id.as_deref(), Some("codex-session-1"));
    assert_eq!(last.request_count, 3);
    assert_eq!(last.root_request_id.as_deref(), Some(begins[0].id.as_str()));
    assert_eq!(last.depth, 0);
}

#[async_trait]
impl PersistenceWriter for RecordingWriter {
    async fn begin_request(&self, row: RequestRow) -> StoreResult<()> {
        self.begins.lock().unwrap().push(row);
        Ok(())
    }

    async fn append_event(&self, event: EventRow) -> StoreResult<()> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }

    async fn store_body(&self, body: BodyWrite) -> StoreResult<()> {
        self.events.lock().unwrap().push(body.event.clone());
        self.bodies.lock().unwrap().push(body);
        Ok(())
    }

    async fn upsert_session(&self, row: llmconduit::sessions::SessionRow) -> StoreResult<()> {
        self.sessions.lock().unwrap().push(row);
        Ok(())
    }

    async fn finish_request(&self, id: &str, finish: RequestFinish) -> StoreResult<()> {
        self.finishes.lock().unwrap().push((id.to_owned(), finish));
        Ok(())
    }
}

fn sse(chunks: &[serde_json::Value]) -> String {
    let mut body = chunks
        .iter()
        .map(|chunk| format!("data: {chunk}\n\n"))
        .collect::<String>();
    body.push_str("data: [DONE]\n\n");
    body
}

fn event_envelope(event: &EventRow) -> serde_json::Value {
    serde_json::from_str(event.payload.as_deref().expect("event payload"))
        .expect("payload envelope")
}

/// A leaf-shaped upstream that stamps the same shared ServingToken fields the
/// production routing/failover stack owns, then yields one normal response.
#[derive(Clone, Default)]
struct ServingUpstream;

#[async_trait]
impl UpstreamClient for ServingUpstream {
    async fn stream_chat_completion(
        &self,
        backend: &BackendChatRequest,
    ) -> Result<UpstreamStream, AppError> {
        let serving = backend.serving.as_ref().expect("engine serving token");
        let now = llmconduit::flow_persistence::now_epoch_ms();
        serving.set_provider("fallback-b");
        serving.set_model_served_final(backend.request.model.clone());
        serving.record_attempt(Attempt {
            provider: Some("fallback-b".to_owned()),
            model: Some(backend.request.model.clone()),
            start_ms: now,
            end_ms: now + 2,
            first_upstream_byte_ms: Some(now + 1),
            status: AttemptStatus::Served,
            error_class: None,
            failover_reason: None,
        });
        Ok(Box::pin(stream::iter(vec![
            Ok(content_chunk("chat-1", "hello")),
            Ok(usage_chunk("chat-1", 11, 4, 15)),
            Ok(finish_chunk("chat-1", "stop")),
        ])))
    }

    async fn list_models(&self) -> Result<UpstreamModelsResponse, AppError> {
        Err(AppError::internal("unused"))
    }

    async fn supported_model_catalog(&self) -> Result<Vec<UpstreamModelEntry>, AppError> {
        Ok(vec![UpstreamModelEntry {
            id: "glm-5.1".to_owned(),
            context_limit: None,
        }])
    }
}

#[tokio::test]
async fn direct_engine_api_call_id_keeps_dashboard_but_cannot_synthesize_persistence() {
    let config = common::test_config();
    let vision: Arc<dyn llmconduit::vision::VisionClient> = Arc::new(
        llmconduit::vision::ReqwestVisionClient::new(reqwest::Client::new(), &config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    let writer = Arc::new(RecordingWriter::default());
    let queue = PersistenceQueue::spawn(
        Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
        NonZeroUsize::new(16).unwrap(),
    );
    let gateway = Arc::new(
        Gateway::new(
            config,
            ReplayStore::new(16),
            Arc::new(ServingUpstream),
            Arc::new(MockSearch::default()),
            vision,
            image_cache,
            MonitorHub::disabled(),
            None,
            DashboardFlowStore::new(),
        )
        .with_persistence_queue(queue.clone()),
    );
    let api_call_id = "api_direct_dashboard_1";
    gateway.flow_store().open(
        api_call_id.to_owned(),
        "POST".to_owned(),
        "/v1/responses".to_owned(),
        llmconduit::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
        None,
        llmconduit::dashboard_flow::ClientAttribution::none(),
    );

    let stream = Arc::clone(&gateway)
        .stream_responses_with_api_call_id(
            base_request(vec![common::user_message("hi")]),
            Some(api_call_id.to_owned()),
        )
        .await
        .expect("stream starts");
    let events = collect_stream(stream).await;
    assert_eq!(
        events.last().unwrap()["_event"],
        "response.completed",
        "client stream completes before the async store is flushed"
    );
    queue.flush().await.unwrap();

    let mut terminal = None;
    for _ in 0..1000 {
        if let Some(record) = gateway.flow_store().detail(api_call_id)
            && record.status != llmconduit::dashboard_flow::FlowStatus::Open
        {
            terminal = Some(record);
            break;
        }
        tokio::task::yield_now().await;
    }
    let record = terminal.expect("dashboard flow reached a terminal state");
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Completed
    );
    assert!(
        record
            .response_id
            .as_deref()
            .is_some_and(|id| id.starts_with("resp_")),
        "dashboard telemetry keeps its engine response-id join"
    );
    assert!(writer.begins.lock().unwrap().is_empty());
    assert!(writer.events.lock().unwrap().is_empty());
    assert!(writer.finishes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn persistence_only_http_records_redacted_final_four_hops_after_failover() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"id": "FRIENDLY"}]
        })))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "error": "superseded https://primary.invalid/signed?token=LEAK",
            "api_key": "primary-response-secret"
        })))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse(&[
                    serde_json::json!({
                        "id": "chat-fallback",
                        "choices": [{
                            "index": 0,
                            "delta": {"content": "hello https://fallback.invalid/image?sig=LEAK"},
                            "finish_reason": null
                        }]
                    }),
                    serde_json::json!({
                        "id": "chat-fallback",
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 7, "completion_tokens": 2, "total_tokens": 9}
                    }),
                ])),
        )
        .mount(&fallback)
        .await;

    let config = Config::from_persisted(&PersistedConfig {
        upstream_base_url: format!("{}/v1", primary.uri()),
        fallback_upstreams: vec![PersistedFallbackUpstream {
            name: Some("fallback-b".to_owned()),
            upstream_base_url: format!("{}/v1", fallback.uri()),
            upstream_model: Some("served-model".to_owned()),
            ..PersistedFallbackUpstream::default()
        }],
        ..PersistedConfig::default()
    })
    .expect("config");
    let (_unused, gateway) = llmconduit::build_app_with_gateway(config);
    let writer = Arc::new(RecordingWriter::default());
    let queue = PersistenceQueue::spawn(
        Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
        NonZeroUsize::new(64).unwrap(),
    );
    let auth = ClientAuth::from_specs(
        true,
        [VirtualKeySpec {
            id: "virtual-key-1".to_owned(),
            label: None,
            owner_id: None,
            secret_hash: hash_secret("client-secret"),
            allowed_models: Vec::new(),
        }],
    )
    .unwrap();
    let gateway = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_client_auth(auth)
            .with_conversation_id_header("x-conversation-id")
            .with_operational_models(
                [("friendly".to_owned(), "profile-a".to_owned())],
                llmconduit::control_plane::UnknownModelPolicy::Passthrough,
            )
            .with_persistence_queue(queue.clone())
            // This test pins the redaction contract, so media is stripped too.
            .with_persistence_keep_media(false),
    );
    assert!(!gateway.flow_store().is_enabled());
    assert!(!gateway.turn_capture().is_enabled());
    let app = llmconduit::build_app_from_gateway(gateway);
    let raw_request = serde_json::json!({
        "model": "FRIENDLY",
        "input": "hello",
        "stream": false,
        "api_key": "body-secret",
        "image_url": "data:image/png;base64,IMAGELEAK"
    })
    .to_string();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .header("authorization", "Bearer client-secret")
                .header("x-conversation-id", "conversation-42")
                .body(Body::from(raw_request))
                .unwrap(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let served = axum::body::to_bytes(response.into_body(), 256 * 1024)
        .await
        .expect("served body");
    assert!(String::from_utf8_lossy(&served).contains("fallback.invalid"));
    queue.flush().await.unwrap();

    let begins = writer.begins.lock().unwrap();
    assert_eq!(begins.len(), 1);
    let begin = &begins[0];
    assert_eq!(begin.conversation_id.as_deref(), Some("conversation-42"));
    assert_eq!(begin.virtual_key_id.as_deref(), Some("virtual-key-1"));
    assert_eq!(begin.client_protocol, "responses");
    assert_eq!(begin.client_model, "FRIENDLY");
    assert_eq!(begin.alias.as_deref(), Some("friendly"));
    drop(begins);

    let mut events = writer.events.lock().unwrap().clone();
    events.sort_by_key(|event| event.seq);
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.hop.as_str())
            .collect::<Vec<_>>(),
        ["client_in", "upstream_out", "upstream_in", "client_out"]
    );
    for event in &events {
        let payload = event.payload.as_deref().unwrap();
        assert!(!payload.contains("body-secret"));
        assert!(!payload.contains("IMAGELEAK"));
        assert!(!payload.contains("fallback.invalid"));
        assert!(!payload.contains("primary.invalid"));
    }
    let upstream_request = event_envelope(&events[1]);
    let upstream_request: serde_json::Value =
        serde_json::from_str(upstream_request["content"].as_str().unwrap()).unwrap();
    assert_eq!(upstream_request["model"], "served-model");
    assert_eq!(upstream_request["api_key"], "[redacted]");
    // Items and blobs carry the redaction too, on both request-side hops.
    let inbound = writer.assembled_body(1, llmconduit::content_store::PROTOCOL_RESPONSES);
    assert_eq!(inbound["api_key"], "[redacted]");
    assert_eq!(inbound["input"], "hello");
    assert!(!inbound["image_url"].as_str().unwrap().contains("IMAGELEAK"));
    for body in writer.bodies.lock().unwrap().iter() {
        for blob in &body.blobs {
            assert!(!blob.content.contains("body-secret"));
            assert!(!blob.content.contains("IMAGELEAK"));
        }
    }
    assert_eq!(event_envelope(&events[2])["partial"], false);
    assert_eq!(event_envelope(&events[3])["partial"], false);

    let finishes = writer.finishes.lock().unwrap();
    assert_eq!(finishes.len(), 1);
    assert_eq!(finishes[0].1.status, "completed");
    assert_eq!(finishes[0].1.backend.as_deref(), Some("fallback-b"));
    assert_eq!(
        finishes[0].1.resolved_model.as_deref(),
        Some("served-model")
    );
}
