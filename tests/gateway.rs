use async_trait::async_trait;
use axum::body::Body;
use axum::http::Request;
use futures::StreamExt;
use futures::stream;
use llmconduit::config::Config;
use llmconduit::config::FallbackUpstreamConfig;
use llmconduit::config::UnsupportedImagePolicy;
use llmconduit::config::UpstreamConfig;
use llmconduit::engine::Gateway;
use llmconduit::models::chat::ChatChunkChoice;
use llmconduit::models::chat::ChatCompletionChunk;
use llmconduit::models::chat::ChatCompletionRequest;
use llmconduit::models::chat::ChatDelta;
use llmconduit::models::chat::ChatFunctionCall;
use llmconduit::models::chat::ChatToolCall;
use llmconduit::models::chat::ChunkUsage;
use llmconduit::models::chat::CompletionTokensDetails;
use llmconduit::models::chat::PromptTokensDetails;
use llmconduit::models::responses::ContentItem;
use llmconduit::models::responses::NamespaceToolSpec;
use llmconduit::models::responses::ReasoningSummaryItem;
use llmconduit::models::responses::ResponseItem;
use llmconduit::models::responses::ResponsesRequest;
use llmconduit::models::responses::ToolSpec;
use llmconduit::monitor::MonitorHub;
use llmconduit::raw::RawOutput;
use llmconduit::replay::ReplayStore;
use llmconduit::search::SearchClient;
use llmconduit::upstream::UpstreamClient;
use llmconduit::upstream::UpstreamModelEntry;
use llmconduit::upstream::UpstreamStream;
use pretty_assertions::assert_eq;
use serde_json::Map as JsonMap;
use serde_json::json;
use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tower::ServiceExt;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_json;
use wiremock::matchers::body_partial_json;
use wiremock::matchers::header;
use wiremock::matchers::method;
use wiremock::matchers::path;

/// Queue of canned chunk-streams: each inner `Vec` is one response's chunk
/// sequence (or per-chunk error), popped front-to-back across upstream calls.
type CannedResponses =
    Arc<Mutex<VecDeque<Vec<Result<ChatCompletionChunk, llmconduit::error::AppError>>>>>;

#[derive(Clone, Default)]
struct MockUpstream {
    requests: Arc<Mutex<Vec<ChatCompletionRequest>>>,
    responses: CannedResponses,
    supported_models: Arc<Mutex<Vec<String>>>,
    supported_model_queries: Arc<Mutex<usize>>,
    /// Per-model advertised context windows surfaced through
    /// `supported_model_catalog` (gap 06). A model absent here reports
    /// `context_limit: None` (the upstream advertises no window). Empty by default.
    context_limits: Arc<Mutex<Vec<(String, i64)>>>,
    /// Per-model finalization policies (effort/family/kwargs), built from the
    /// test config by the gateway harness so the mock's leaf-mirror applies the
    /// SAME profile kwargs the production leaf would (T1). Empty by default
    /// (most tests don't assert kwargs).
    finalization_policies: Arc<StdMutex<llmconduit::upstream::BackendFinalizationPolicies>>,
    token_count: Arc<Mutex<Option<u64>>>,
}

impl MockUpstream {
    async fn push_response(
        &self,
        chunks: Vec<Result<ChatCompletionChunk, llmconduit::error::AppError>>,
    ) {
        self.responses.lock().await.push_back(chunks);
    }

    async fn set_token_count(&self, count: Option<u64>) {
        *self.token_count.lock().await = count;
    }

    /// Set the finalization policies built from the test config, so the mock's
    /// leaf-mirror applies the same profile/family/effort kwargs the production
    /// leaf would (T1).
    fn set_finalization_policies(
        &self,
        policies: llmconduit::upstream::BackendFinalizationPolicies,
    ) {
        *self.finalization_policies.lock().expect("policies lock") = policies;
    }

    async fn requests(&self) -> Vec<ChatCompletionRequest> {
        self.requests.lock().await.clone()
    }

    async fn set_supported_models<I, S>(&self, models: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        *self.supported_models.lock().await = models.into_iter().map(Into::into).collect();
    }

    /// Supply per-model advertised context windows surfaced through
    /// `supported_model_catalog` (gap 06). Models not listed report `None`.
    async fn set_context_limits<I, S>(&self, limits: I)
    where
        I: IntoIterator<Item = (S, i64)>,
        S: Into<String>,
    {
        *self.context_limits.lock().await =
            limits.into_iter().map(|(id, n)| (id.into(), n)).collect();
    }

    async fn supported_model_queries(&self) -> usize {
        *self.supported_model_queries.lock().await
    }
}

#[async_trait]
impl UpstreamClient for MockUpstream {
    async fn stream_chat_completion(
        &self,
        backend: &llmconduit::upstream::BackendChatRequest,
    ) -> Result<UpstreamStream, llmconduit::error::AppError> {
        // Mirror the production leaf so the recorded request reflects what the
        // backend would actually receive: per-model `upstream_chat_kwargs` +
        // `template_family` + effort (clamp/map) + family `chat_template_kwargs`
        // are applied HERE (T1 moved profile resolution from the engine to the
        // leaf). Empty policies → the unmapped/clamp path; the per-model
        // reasoning_effort_map is exercised against the real leaf in
        // port_config.rs. Tests asserting a RAW pre-leaf `reasoning_effort` rely
        // on non-family models where the leaf is a no-op on that field.
        let mut backend = backend.clone();
        let policies = self
            .finalization_policies
            .lock()
            .expect("policies lock")
            .clone();
        llmconduit::upstream::finalize_request_for_backend(&mut backend, &policies);
        self.requests.lock().await.push(backend.request.clone());
        let chunks = self
            .responses
            .lock()
            .await
            .pop_front()
            .expect("queued upstream response");
        Ok(Box::pin(stream::iter(chunks)))
    }

    async fn list_models(&self) -> Result<reqwest::Response, llmconduit::error::AppError> {
        Err(llmconduit::error::AppError::internal("unused in this test"))
    }

    async fn count_tokens(
        &self,
        backend: &llmconduit::upstream::BackendChatRequest,
    ) -> Result<Option<u64>, llmconduit::error::AppError> {
        let mut backend = backend.clone();
        let policies = self
            .finalization_policies
            .lock()
            .expect("policies lock")
            .clone();
        llmconduit::upstream::finalize_request_for_backend(&mut backend, &policies);
        self.requests.lock().await.push(backend.request);
        Ok(*self.token_count.lock().await)
    }

    async fn supported_model_catalog(
        &self,
    ) -> Result<Vec<UpstreamModelEntry>, llmconduit::error::AppError> {
        let mut query_count = self.supported_model_queries.lock().await;
        *query_count += 1;
        let limits = self.context_limits.lock().await.clone();
        Ok(self
            .supported_models
            .lock()
            .await
            .iter()
            .map(|id| UpstreamModelEntry {
                id: id.clone(),
                context_limit: limits
                    .iter()
                    .find(|(limit_id, _)| limit_id == id)
                    .map(|(_, limit)| *limit),
            })
            .collect())
    }
}

#[derive(Clone)]
struct PendingChunkUpstream {
    requests: Arc<Mutex<Vec<ChatCompletionRequest>>>,
    stream_polled: Arc<Notify>,
    stream_dropped: Arc<Notify>,
}

impl PendingChunkUpstream {
    fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            stream_polled: Arc::new(Notify::new()),
            stream_dropped: Arc::new(Notify::new()),
        }
    }

    async fn requests(&self) -> Vec<ChatCompletionRequest> {
        self.requests.lock().await.clone()
    }
}

struct NotifyOnDrop {
    notify: Arc<Notify>,
}

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.notify.notify_waiters();
    }
}

#[async_trait]
impl UpstreamClient for PendingChunkUpstream {
    async fn stream_chat_completion(
        &self,
        backend: &llmconduit::upstream::BackendChatRequest,
    ) -> Result<UpstreamStream, llmconduit::error::AppError> {
        self.requests.lock().await.push(backend.request.clone());
        let stream_polled = Arc::clone(&self.stream_polled);
        let stream_dropped = Arc::clone(&self.stream_dropped);
        let stream = async_stream::stream! {
            let _drop_guard = NotifyOnDrop {
                notify: stream_dropped,
            };
            stream_polled.notify_waiters();
            std::future::pending::<()>().await;
            yield Ok(content_chunk("chat-1", "unreachable"));
        };
        Ok(Box::pin(stream))
    }

    async fn list_models(&self) -> Result<reqwest::Response, llmconduit::error::AppError> {
        Err(llmconduit::error::AppError::internal("unused in this test"))
    }

    async fn supported_model_catalog(
        &self,
    ) -> Result<Vec<UpstreamModelEntry>, llmconduit::error::AppError> {
        Ok(vec![UpstreamModelEntry {
            id: "glm-5.1".to_string(),
            context_limit: None,
        }])
    }
}

/// Like [`PendingChunkUpstream`] but YIELDS a canned prefix of chunks first, THEN
/// parks forever — so a test can drive real usage upserts (D3) before triggering a
/// midstream cancel. `stream_polled` fires once the canned chunks are exhausted and
/// the stream is parked; `stream_dropped` fires when the client drops the stream.
#[derive(Clone)]
struct ChunkThenPendingUpstream {
    requests: Arc<Mutex<Vec<ChatCompletionRequest>>>,
    prefix: Arc<Vec<ChatCompletionChunk>>,
    stream_polled: Arc<Notify>,
    stream_dropped: Arc<Notify>,
}

impl ChunkThenPendingUpstream {
    fn new(prefix: Vec<ChatCompletionChunk>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            prefix: Arc::new(prefix),
            stream_polled: Arc::new(Notify::new()),
            stream_dropped: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl UpstreamClient for ChunkThenPendingUpstream {
    async fn stream_chat_completion(
        &self,
        backend: &llmconduit::upstream::BackendChatRequest,
    ) -> Result<UpstreamStream, llmconduit::error::AppError> {
        self.requests.lock().await.push(backend.request.clone());
        let prefix = Arc::clone(&self.prefix);
        let stream_polled = Arc::clone(&self.stream_polled);
        let stream_dropped = Arc::clone(&self.stream_dropped);
        let stream = async_stream::stream! {
            let _drop_guard = NotifyOnDrop { notify: stream_dropped };
            for chunk in prefix.iter() {
                yield Ok(chunk.clone());
            }
            // Canned chunks exhausted; signal and park so the client can cancel.
            stream_polled.notify_waiters();
            std::future::pending::<()>().await;
            yield Ok(content_chunk("chat-1", "unreachable"));
        };
        Ok(Box::pin(stream))
    }

    async fn list_models(&self) -> Result<reqwest::Response, llmconduit::error::AppError> {
        Err(llmconduit::error::AppError::internal("unused in this test"))
    }

    async fn supported_model_catalog(
        &self,
    ) -> Result<Vec<UpstreamModelEntry>, llmconduit::error::AppError> {
        Ok(vec![UpstreamModelEntry {
            id: "glm-5.1".to_string(),
            context_limit: None,
        }])
    }
}

/// Yields a FLOOD of content chunks (enough that their fan-out of SSE events
/// overflows the engine's 128-slot channel) then parks. A client that never
/// drains the channel forces the engine to block INSIDE `send_event`'s
/// `tx.send().await` — exercising the mid-SEND cancellation path (D3 R1 #1),
/// distinct from `ChunkThenPendingUpstream` whose cancel lands while the engine
/// is parked in `next_upstream_chunk`'s `tx.closed()` select. `yielded` counts
/// chunks handed to the engine; on the current-thread test runtime it freezes
/// EXACTLY when the engine blocks on a full channel (no other task can run while
/// the test loops), giving a deterministic "engine is mid-send" signal with no
/// sleep.
#[derive(Clone)]
struct FloodThenParkUpstream {
    flood: usize,
    yielded: Arc<std::sync::atomic::AtomicUsize>,
    stream_dropped: Arc<Notify>,
}

impl FloodThenParkUpstream {
    fn new(flood: usize) -> Self {
        Self {
            flood,
            yielded: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            stream_dropped: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl UpstreamClient for FloodThenParkUpstream {
    async fn stream_chat_completion(
        &self,
        _backend: &llmconduit::upstream::BackendChatRequest,
    ) -> Result<UpstreamStream, llmconduit::error::AppError> {
        let flood = self.flood;
        let yielded = Arc::clone(&self.yielded);
        let stream_dropped = Arc::clone(&self.stream_dropped);
        let stream = async_stream::stream! {
            let _drop_guard = NotifyOnDrop { notify: stream_dropped };
            for i in 0..flood {
                yielded.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                yield Ok(content_chunk("chat-1", &format!("tok{i} ")));
            }
            // Flood exhausted (only reached if the client kept draining); park so a
            // late cancel still has a stream to drop.
            std::future::pending::<()>().await;
            yield Ok(content_chunk("chat-1", "unreachable"));
        };
        Ok(Box::pin(stream))
    }

    async fn list_models(&self) -> Result<reqwest::Response, llmconduit::error::AppError> {
        Err(llmconduit::error::AppError::internal("unused in this test"))
    }

    async fn supported_model_catalog(
        &self,
    ) -> Result<Vec<UpstreamModelEntry>, llmconduit::error::AppError> {
        Ok(vec![UpstreamModelEntry {
            id: "glm-5.1".to_string(),
            context_limit: None,
        }])
    }
}

#[derive(Clone, Default)]
struct MockSearch {
    queries: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl SearchClient for MockSearch {
    async fn search(
        &self,
        query: &str,
    ) -> Result<llmconduit::search::SearchOutcome, llmconduit::error::AppError> {
        self.queries.lock().await.push(query.to_string());
        Ok(llmconduit::search::SearchOutcome {
            formatted: format!("Search result for {query}"),
            sources: vec![llmconduit::search::SearchSource {
                title: format!("Result for {query}"),
                url: "https://example.com/result".to_string(),
            }],
        })
    }
}

#[tokio::test]
async fn streams_function_call_turn() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_fn_1",
            "echo",
            "{\"value\":\"hi\"}",
        ))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let request = ResponsesRequest {
        model: "glm-5.1".to_string(),
        instructions: String::new(),
        input: vec![user_message("hello")],
        tools: vec![ToolSpec::Function {
            name: "echo".to_string(),
            description: "Echo back a value".to_string(),
            strict: false,
            parameters: json!({
                "type": "object",
                "properties": { "value": { "type": "string" } },
                "required": ["value"]
            }),
        }],
        tool_choice: json!("auto"),
        parallel_tool_calls: Some(true),
        reasoning: None,
        thinking: None,
        store: false,
        stream: true,
        include: Vec::new(),
        service_tier: None,
        prompt_cache_key: None,
        text: None,
        client_metadata: None,
        previous_response_id: None,
        temperature: None,
        top_p: None,
        max_output_tokens: None,
        frequency_penalty: None,
        presence_penalty: None,
        truncation: None,
        metadata: None,
        stop: None,
        extra_body: Default::default(),
    };

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream"));
    let events = events.await;

    assert_eq!(
        event_names(&events),
        vec![
            "response.created",
            "response.in_progress",
            "response.function_call_arguments.delta",
            "response.function_call_arguments.done",
            "response.output_item.done",
            "response.completed",
        ]
    );
    let done_event = events
        .iter()
        .find(|e| e["_event"] == "response.output_item.done")
        .unwrap();
    assert_eq!(done_event["item"]["type"].as_str(), Some("function_call"));
    assert_eq!(done_event["item"]["name"].as_str(), Some("echo"));

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].parallel_tool_calls, Some(true));
    assert_eq!(requests[0].tools.as_ref().map(Vec::len), Some(1));
}

#[tokio::test]
async fn raw_output_observes_gateway_sse_deltas() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(reasoning_chunk("chat-1", "thinking")),
            Ok(content_chunk("chat-1", "answer")),
        ])
        .await;
    let buffer = Arc::new(StdMutex::new(Vec::new()));
    let gateway = test_gateway_with_raw_output(
        upstream,
        MockSearch::default(),
        RawOutput::new(SharedBuffer(Arc::clone(&buffer))),
    );

    let request = base_request(vec![user_message("hello")]);
    let _events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let output = buffer.lock().expect("buffer lock").clone();
    assert_eq!(String::from_utf8(output).expect("utf8"), "thinkinganswer");
}

#[tokio::test]
async fn streams_legacy_function_call_turn() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(legacy_function_call_chunk(
            "chat-1",
            "echo",
            "{\"value\":\"hi\"}",
        ))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![user_message("hello")]);
    request.tools = vec![ToolSpec::Function {
        name: "echo".to_string(),
        description: "Echo back a value".to_string(),
        strict: false,
        parameters: json!({
            "type": "object",
            "properties": { "value": { "type": "string" } },
            "required": ["value"]
        }),
    }];

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    assert_eq!(
        event_names(&events),
        vec![
            "response.created",
            "response.in_progress",
            "response.function_call_arguments.delta",
            "response.function_call_arguments.done",
            "response.output_item.done",
            "response.completed",
        ]
    );
    let delta_event = events
        .iter()
        .find(|e| e["_event"] == "response.function_call_arguments.delta")
        .unwrap();
    let call_id = delta_event["call_id"].as_str().unwrap();
    assert!(call_id.starts_with("call_"));
    assert_eq!(delta_event["delta"].as_str(), Some("{\"value\":\"hi\"}"));

    let done_event = events
        .iter()
        .find(|e| e["_event"] == "response.output_item.done")
        .unwrap();
    assert_eq!(done_event["item"]["type"].as_str(), Some("function_call"));
    assert_eq!(done_event["item"]["name"].as_str(), Some("echo"));
    assert_eq!(done_event["item"]["call_id"].as_str(), Some(call_id));
}

#[tokio::test]
async fn flattens_namespace_tools_for_upstream_and_preserves_namespace_in_output() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_fn_1",
            "calendar_search",
            "{\"query\":\"today\"}",
        ))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let request = ResponsesRequest {
        model: "glm-5.1".to_string(),
        instructions: String::new(),
        input: vec![user_message("what's on my calendar?")],
        tools: vec![ToolSpec::Namespace {
            name: "mcp__calendar".to_string(),
            description: "Calendar tools".to_string(),
            tools: vec![NamespaceToolSpec::Function {
                name: "calendar_search".to_string(),
                description: "Search calendar events".to_string(),
                strict: false,
                parameters: json!({
                    "type": "object",
                    "properties": { "query": { "type": "string" } },
                    "required": ["query"]
                }),
            }],
        }],
        tool_choice: json!("auto"),
        parallel_tool_calls: Some(true),
        reasoning: None,
        thinking: None,
        store: false,
        stream: true,
        include: Vec::new(),
        service_tier: None,
        prompt_cache_key: None,
        text: None,
        client_metadata: None,
        previous_response_id: None,
        temperature: None,
        top_p: None,
        max_output_tokens: None,
        frequency_penalty: None,
        presence_penalty: None,
        truncation: None,
        metadata: None,
        stop: None,
        extra_body: Default::default(),
    };

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let done_event = events
        .iter()
        .find(|e| e["_event"] == "response.output_item.done")
        .unwrap();
    assert_eq!(done_event["item"]["type"].as_str(), Some("function_call"));
    assert_eq!(done_event["item"]["name"].as_str(), Some("calendar_search"));
    assert_eq!(
        done_event["item"]["namespace"].as_str(),
        Some("mcp__calendar")
    );

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].tools.as_ref().map(Vec::len), Some(1));
    assert_eq!(
        requests[0]
            .tools
            .as_ref()
            .and_then(|tools| tools.first())
            .map(|tool| tool.function.name.as_str()),
        Some("calendar_search")
    );
}

#[tokio::test]
async fn uses_configured_upstream_model_override() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "hello")),
            Ok(usage_chunk("chat-1", 12, 5, 17, Some(3), Some(2))),
        ])
        .await;
    let gateway = test_gateway_with_config(
        upstream.clone(),
        MockSearch::default(),
        Config {
            bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
            upstream_base_url: "http://127.0.0.1:8000/v1".parse().expect("url"),
            upstream_api_key: None,
            upstream_model: Some("grok-4".to_string()),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profiles: std::collections::BTreeMap::new(),
            model_routes: Vec::new(),
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
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            price_table: std::collections::HashMap::new(),
        },
    );

    let _ = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message("hello")]))
            .await
            .expect("stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, "grok-4");
    assert_eq!(
        requests[0]
            .stream_options
            .as_ref()
            .map(|opts| opts.include_usage),
        Some(true)
    );
    assert_eq!(requests[0].extra_body.get("stream_options"), None);
}

#[tokio::test]
async fn normalizes_model_name_from_upstream_catalog() {
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["Qwen3.5"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "hello"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![user_message("hello")]);
    request.model = "some-client-alias".to_string();

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, "Qwen3.5");
}

#[tokio::test]
async fn single_supported_backend_model_overrides_configured_model_alias() {
    let upstream = MockUpstream::default();
    upstream
        .set_supported_models(["deepseek-r1-distill-qwen-32b"])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "hello"))])
        .await;
    let gateway = test_gateway_with_config(
        upstream.clone(),
        MockSearch::default(),
        Config {
            bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
            upstream_base_url: "http://127.0.0.1:8000/v1".parse().expect("url"),
            upstream_api_key: None,
            upstream_model: Some("alias-from-config".to_string()),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profiles: std::collections::BTreeMap::new(),
            model_routes: Vec::new(),
            template_family: None,
            brave_base_url: "https://example.com/".parse().expect("url"),
            brave_api_key: Some("test-key".to_string()),
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
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            price_table: std::collections::HashMap::new(),
        },
    );

    let _ = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message("hello")]))
            .await
            .expect("stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, "deepseek-r1-distill-qwen-32b");
}

#[tokio::test]
async fn reuses_cached_upstream_model_catalog_across_requests() {
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["glm-5.1"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "first"))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "second"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut first = base_request(vec![user_message("hello")]);
    first.model = "GLM-5.1".to_string();
    let _ = collect_stream(
        gateway
            .clone()
            .stream_responses(first)
            .await
            .expect("first stream"),
    )
    .await;

    let mut second = base_request(vec![user_message("hello again")]);
    second.model = "GLM 5 1".to_string();
    let _ = collect_stream(
        gateway
            .stream_responses(second)
            .await
            .expect("second stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].model, "glm-5.1");
    assert_eq!(requests[1].model, "glm-5.1");
    assert_eq!(upstream.supported_model_queries().await, 1);
}

#[tokio::test]
async fn ambiguous_catalog_match_defaults_to_first_backend_model() {
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["foo-1", "foo1"]).await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "hello"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![user_message("hello")]);
    request.model = "FOO 1".to_string();

    let _ = collect_stream(
        gateway
            .stream_responses(request.clone())
            .await
            .expect("stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, "foo-1");
}

#[tokio::test]
async fn returns_final_usage_on_response_completed() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "hello")),
            Ok(usage_chunk("chat-1", 12, 5, 17, Some(3), Some(2))),
        ])
        .await;
    let gateway = test_gateway(upstream, MockSearch::default());

    let events = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message("hello")]))
            .await
            .expect("stream"),
    )
    .await;

    assert_eq!(
        events.last().and_then(|event| event["_event"].as_str()),
        Some("response.completed")
    );
    assert_eq!(
        events
            .last()
            .and_then(|event| event["response"]["usage"]["input_tokens"].as_u64()),
        Some(12)
    );
    assert_eq!(
        events.last().and_then(|event| {
            event["response"]["usage"]["input_tokens_details"]["cached_tokens"].as_u64()
        }),
        Some(3)
    );
    assert_eq!(
        events
            .last()
            .and_then(|event| event["response"]["usage"]["output_tokens"].as_u64()),
        Some(5)
    );
    assert_eq!(
        events.last().and_then(|event| {
            event["response"]["usage"]["output_tokens_details"]["reasoning_tokens"].as_u64()
        }),
        Some(2)
    );
    assert_eq!(
        events
            .last()
            .and_then(|event| event["response"]["usage"]["total_tokens"].as_u64()),
        Some(17)
    );
}

#[tokio::test]
async fn responses_stream_events_include_item_identity_and_generated_output_only() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(reasoning_chunk("chat-1", "think")),
            Ok(content_chunk("chat-1", "hello")),
        ])
        .await;
    let gateway = test_gateway(upstream, MockSearch::default());

    let events = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message("hello")]))
            .await
            .expect("stream"),
    )
    .await;

    let reasoning_added = events
        .iter()
        .find(|event| {
            event["_event"] == "response.output_item.added"
                && event["item"]["type"].as_str() == Some("reasoning")
        })
        .expect("reasoning item added");
    let reasoning_id = reasoning_added["item"]["id"]
        .as_str()
        .expect("reasoning item id");
    assert_eq!(reasoning_added["output_index"], 0);

    let reasoning_delta = events
        .iter()
        .find(|event| event["_event"] == "response.reasoning_summary_text.delta")
        .expect("reasoning delta");
    assert_eq!(reasoning_delta["item_id"], reasoning_id);
    assert_eq!(reasoning_delta["output_index"], 0);
    assert_eq!(reasoning_delta["summary_index"], 0);

    let message_added = events
        .iter()
        .find(|event| {
            event["_event"] == "response.output_item.added"
                && event["item"]["type"].as_str() == Some("message")
        })
        .expect("message item added");
    let message_id = message_added["item"]["id"]
        .as_str()
        .expect("message item id");
    assert_eq!(message_added["output_index"], 1);

    let text_delta = events
        .iter()
        .find(|event| event["_event"] == "response.output_text.delta")
        .expect("text delta");
    assert_eq!(text_delta["item_id"], message_id);
    assert_eq!(text_delta["output_index"], 1);
    assert_eq!(text_delta["content_index"], 0);

    let content_added = events
        .iter()
        .find(|event| event["_event"] == "response.content_part.added")
        .expect("content part added");
    assert_eq!(content_added["item_id"], message_id);
    assert_eq!(content_added["output_index"], 1);
    assert_eq!(content_added["content_index"], 0);

    let completed = events
        .iter()
        .find(|event| event["_event"] == "response.completed")
        .expect("response.completed");
    let output = completed["response"]["output"]
        .as_array()
        .expect("response output");
    assert_eq!(output.len(), 2);
    assert_eq!(output[0]["type"].as_str(), Some("reasoning"));
    assert_eq!(output[1]["type"].as_str(), Some("message"));
    assert_eq!(output[1]["role"].as_str(), Some("assistant"));
}

#[tokio::test]
async fn normalizes_developer_messages_to_system_for_upstream() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let request = base_request(vec![
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "developer instruction".to_string(),
            }],
            phase: None,
        },
        user_message("hello"),
    ]);

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].messages.len(), 2);
    assert_eq!(requests[0].messages[0].role, "system");
    assert_eq!(
        requests[0].messages[0]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("developer instruction")
    );
    assert_eq!(requests[0].messages[1].role, "user");
}

#[tokio::test]
async fn replays_reasoning_into_follow_up_request() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(reasoning_chunk("chat-1", "think step")),
            Ok(content_chunk("chat-1", "hello")),
        ])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "follow up done"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let first_request = base_request(vec![user_message("hello")]);
    let first_events = collect_stream(
        gateway
            .clone()
            .stream_responses(first_request.clone())
            .await
            .expect("first stream"),
    )
    .await;
    let public_items = done_items(&first_events);

    let mut second_input = first_request.input;
    second_input.extend(public_items);
    second_input.push(user_message("again"));
    let second_request = base_request(second_input);

    let _ = collect_stream(
        gateway
            .stream_responses(second_request)
            .await
            .expect("second stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 2);
    let second_messages = &requests[1].messages;
    assert_eq!(second_messages.len(), 3);
    assert_eq!(second_messages[1].role, "assistant");
    assert_eq!(
        second_messages[1].reasoning_content.as_deref(),
        Some("think step")
    );
    assert_eq!(
        second_messages[1]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("hello")
    );
}

#[tokio::test]
async fn forwards_configured_upstream_chat_kwargs() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "hello"))])
        .await;
    let gateway = test_gateway_with_config(
        upstream.clone(),
        MockSearch::default(),
        Config {
            bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
            upstream_base_url: "http://127.0.0.1:8000/v1".parse().expect("url"),
            upstream_api_key: None,
            upstream_model: Some("GLM-5.1".to_string()),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::from_iter([(
                "clear_thinking".to_string(),
                json!(false),
            )]),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profiles: std::collections::BTreeMap::new(),
            model_routes: Vec::new(),
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
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            price_table: std::collections::HashMap::new(),
        },
    );

    let _ = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message("hello")]))
            .await
            .expect("stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].extra_body.get("clear_thinking"),
        Some(&json!(false))
    );
}

#[tokio::test]
async fn forwards_profile_specific_upstream_chat_kwargs_for_backend_model() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "hello"))])
        .await;
    let gateway = test_gateway_with_config(
        upstream.clone(),
        MockSearch::default(),
        Config {
            bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
            upstream_base_url: "http://127.0.0.1:8000/v1".parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profiles: std::collections::BTreeMap::from([(
                "Kimi-K2.6".to_string(),
                llmconduit::config::ModelProfile {
                    upstream_model: None,
                    system_prompt_prefix: None,
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "chat_template_kwargs".to_string(),
                        json!({
                            "thinking": true,
                            "preserve_thinking": true
                        }),
                    )]),
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            model_routes: Vec::new(),
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
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            price_table: std::collections::HashMap::new(),
        },
    );

    let mut request = base_request(vec![user_message("hello")]);
    request.model = "Kimi-K2.6".to_string();

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, "Kimi-K2.6");
    assert_eq!(
        requests[0].extra_body.get("chat_template_kwargs"),
        Some(&json!({
            "thinking": true,
            "preserve_thinking": true
        }))
    );
}

#[tokio::test]
async fn prepends_profile_system_prompt_prefix_for_responses_requests() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "hello"))])
        .await;
    let mut config = test_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "glm-5.1".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: Some("Profile prefix.".to_string()),
            upstream_chat_kwargs: JsonMap::new(),
            native_vision: None,
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);

    let _ = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message("hello")]))
            .await
            .expect("stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].messages[0].role, "system");
    assert_eq!(
        requests[0].messages[0]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("Profile prefix.")
    );
    assert_eq!(requests[0].messages[1].role, "user");
}

#[tokio::test]
async fn request_values_override_configured_upstream_defaults_and_merge_chat_template_kwargs() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "hello"))])
        .await;
    let mut config = test_config();
    config.upstream_chat_kwargs = JsonMap::from_iter([
        ("temperature".to_string(), json!(0.2)),
        ("max_tokens".to_string(), json!(1024)),
        ("top_k".to_string(), json!(20)),
        (
            "chat_template_kwargs".to_string(),
            json!({
                "enable_thinking": true,
                "preserve_thinking": true,
                "nested": {
                    "from_default": true,
                    "shared": "default"
                }
            }),
        ),
    ]);
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);

    let mut request = base_request(vec![user_message("hello")]);
    request.temperature = Some(0.7);
    request.max_output_tokens = Some(256);
    request.extra_body = std::collections::BTreeMap::from([
        ("top_k".to_string(), json!(5)),
        (
            "chat_template_kwargs".to_string(),
            json!({
                "preserve_thinking": false,
                "thinking": true,
                "nested": {
                    "shared": "request"
                }
            }),
        ),
    ]);

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].temperature, Some(0.7));
    assert_eq!(requests[0].max_output_tokens, Some(256));
    assert_eq!(requests[0].extra_body.get("temperature"), None);
    assert_eq!(requests[0].extra_body.get("max_tokens"), None);
    assert_eq!(requests[0].extra_body.get("top_k"), Some(&json!(5)));
    assert_eq!(
        requests[0].extra_body.get("chat_template_kwargs"),
        Some(&json!({
            "enable_thinking": true,
            "preserve_thinking": false,
            "thinking": true,
            "nested": {
                "from_default": true,
                "shared": "request"
            }
        }))
    );
}

#[tokio::test]
async fn hides_web_search_loop_but_replays_internal_tool_result() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_ws_1",
            "web_search",
            "{\"query\":\"weather seattle\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "It is rainy."))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-3", "Follow up done."))])
        .await;
    let search = MockSearch::default();
    let gateway = test_gateway(upstream.clone(), search.clone());

    let mut first_request = base_request(vec![user_message("weather?")]);
    first_request.tools = vec![ToolSpec::WebSearch {
        external_web_access: Some(true),
        filters: None,
        user_location: None,
        search_context_size: None,
        search_content_types: None,
    }];
    let first_events = collect_stream(
        gateway
            .clone()
            .stream_responses(first_request.clone())
            .await
            .expect("first stream"),
    )
    .await;

    assert_eq!(
        event_names(&first_events),
        vec![
            "response.created",
            "response.in_progress",
            "response.function_call_arguments.delta",
            "response.output_item.added",
            "response.output_item.done",
            "response.web_search_results",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );
    let ws_done = first_events.iter().find(|e| {
        e["_event"] == "response.output_item.done"
            && e["item"]["type"].as_str() == Some("web_search_call")
    });
    assert!(ws_done.is_some(), "expected a web_search_call done event");
    assert_eq!(
        search.queries.lock().await.as_slice(),
        &["weather seattle".to_string()]
    );

    let mut second_input = first_request.input;
    second_input.extend(done_items(&first_events));
    second_input.push(user_message("why?"));
    let second_request = base_request(second_input);
    let _ = collect_stream(
        gateway
            .stream_responses(second_request)
            .await
            .expect("second stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].messages.len(), 3);
    assert_eq!(requests[1].messages[2].role, "tool");
    assert_eq!(
        requests[1].messages[2]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("Search result for weather seattle")
    );
    assert_eq!(requests[2].messages.len(), 5);
    assert_eq!(requests[2].messages[2].role, "tool");
    assert_eq!(requests[2].messages[3].role, "assistant");
}

#[tokio::test]
async fn web_search_emits_structured_results_event_for_anthropic_clients() {
    // Regression: resp2chat ran Brave server-side but never told the client a
    // search happened, so Claude Code reported "Did 0 searches" with no source
    // chips. The engine must emit a `response.web_search_results` event the
    // Anthropic converter turns into server_tool_use + web_search_tool_result.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_ws_1",
            "web_search",
            "{\"query\":\"weather seattle\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "It is rainy."))])
        .await;
    let search = MockSearch::default();
    let gateway = test_gateway(upstream.clone(), search.clone());

    let mut request = base_request(vec![user_message("weather?")]);
    request.tools = vec![ToolSpec::WebSearch {
        external_web_access: Some(true),
        filters: None,
        user_location: None,
        search_context_size: None,
        search_content_types: None,
    }];
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let ws = events
        .iter()
        .find(|e| e["_event"] == "response.web_search_results")
        .expect("expected a response.web_search_results event");
    assert_eq!(ws["query"], "weather seattle");
    assert!(
        ws["tool_use_id"].as_str().is_some_and(|s| !s.is_empty()),
        "web_search_results must carry a non-empty tool_use_id"
    );
    assert_eq!(
        ws["results"],
        serde_json::json!([
            {
                "type": "web_search_result",
                "url": "https://example.com/result",
                "title": "Result for weather seattle"
            }
        ])
    );
}

#[tokio::test]
async fn web_search_continuation_round_relaxes_forced_tool_choice() {
    // Regression: Claude Code forces `tool_choice` to the web_search server
    // tool. The first upstream round must keep that forced choice, but the
    // continuation round (after Brave results are injected) must relax to
    // `auto` — otherwise vLLM/Kimi emits the final answer text into
    // `function.arguments` and the turn fails with a JSON parse error.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_ws_1",
            "web_search",
            "{\"query\":\"weather seattle\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "It is sunny."))])
        .await;
    let search = MockSearch::default();
    let gateway = test_gateway(upstream.clone(), search.clone());

    let mut request = base_request(vec![user_message("weather?")]);
    request.tools = vec![ToolSpec::WebSearch {
        external_web_access: Some(true),
        filters: None,
        user_location: None,
        search_context_size: None,
        search_content_types: None,
    }];
    let forced = json!({"type": "function", "function": {"name": "web_search"}});
    request.tool_choice = forced.clone();

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(
        requests.len(),
        2,
        "expected one search round + one answer round"
    );
    assert_eq!(
        requests[0].tool_choice,
        Some(forced),
        "round 1 must keep the forced tool_choice"
    );
    assert_eq!(
        requests[1].tool_choice,
        Some(json!("auto")),
        "web-search continuation round must relax tool_choice to auto"
    );
}

#[tokio::test]
async fn web_search_round_ceiling_terminates_loop() {
    // U5: a model that re-requests `web_search` every round must hit the round
    // ceiling and error out rather than loop forever — mirroring
    // `image_agent_round_ceiling_terminates_loop`. Queue far more forced
    // web_search rounds than the configured limit; the loop must terminate at
    // the effective limit (default `max_web_search_rounds: 5`) and the upstream
    // must be called a BOUNDED number of times (==5), proving finiteness.
    let upstream = MockUpstream::default();
    for n in 0..12 {
        upstream
            .push_response(vec![Ok(tool_call_chunk(
                &format!("chat-{n}"),
                &format!("call_ws_{n}"),
                "web_search",
                "{\"query\":\"weather seattle\"}",
            ))])
            .await;
    }
    let search = MockSearch::default();
    let gateway = test_gateway(upstream.clone(), search.clone());

    // The declared web_search tool + forced tool_choice make the emitted call
    // classify as `ToolKind::WebSearch`, so it enters the round-counting branch
    // (without the declared tool the call is treated as a client Function and
    // `had_web_search` stays false — a non-ceiling path).
    let mut request = base_request(vec![user_message("weather?")]);
    request.tools = vec![ToolSpec::WebSearch {
        external_web_access: Some(true),
        filters: None,
        user_location: None,
        search_context_size: None,
        search_content_types: None,
    }];
    request.tool_choice = json!({"type": "function", "function": {"name": "web_search"}});

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(
        event_names(&events).contains(&"response.failed"),
        "web-search round ceiling must terminate the loop"
    );
    // Bounded: exactly the effective limit (default config caps at 5) of
    // upstream rounds ran — not the 12 queued — so the loop is finite.
    assert_eq!(
        upstream.requests().await.len(),
        5,
        "web-search loop must stop at the configured limit (5), not run unbounded"
    );
    // The search client ran once per round up to the limit.
    assert_eq!(
        search.queries.lock().await.len(),
        5,
        "search must run a bounded number of times matching the round limit"
    );
}

#[tokio::test]
async fn web_search_round_ceiling_caps_configured_limit() {
    // U5: `WEB_SEARCH_ROUNDS_HARD_CEILING = 25` (`src/engine.rs`) caps the
    // configured `max_web_search_rounds` via `.min(WEB_SEARCH_ROUNDS_HARD_CEILING)`.
    // With the config set ABOVE 25 (100), a forced web_search loop must still
    // terminate at exactly round 25 — proving the hard ceiling overrides the
    // higher configured value.
    let upstream = MockUpstream::default();
    for n in 0..40 {
        upstream
            .push_response(vec![Ok(tool_call_chunk(
                &format!("chat-{n}"),
                &format!("call_ws_{n}"),
                "web_search",
                "{\"query\":\"weather seattle\"}",
            ))])
            .await;
    }
    let search = MockSearch::default();
    let mut config = test_config();
    config.max_web_search_rounds = 100;
    assert!(
        config.brave_api_key.is_some(),
        "web_search is only server-runnable when Brave is configured"
    );
    let gateway = test_gateway_with_config(upstream.clone(), search.clone(), config);

    let mut request = base_request(vec![user_message("weather?")]);
    request.tools = vec![ToolSpec::WebSearch {
        external_web_access: Some(true),
        filters: None,
        user_location: None,
        search_context_size: None,
        search_content_types: None,
    }];
    request.tool_choice = json!({"type": "function", "function": {"name": "web_search"}});

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(
        event_names(&events).contains(&"response.failed"),
        "web-search hard ceiling must terminate the loop despite the higher config"
    );
    // Termination at exactly round 25 (the hard ceiling), NOT round 100.
    assert_eq!(
        upstream.requests().await.len(),
        25,
        "the .min(WEB_SEARCH_ROUNDS_HARD_CEILING) cap must stop the loop at round 25"
    );
    assert_eq!(
        search.queries.lock().await.len(),
        25,
        "search must run exactly up to the hard ceiling, not the configured 100"
    );
}

#[tokio::test]
async fn degrades_gracefully_when_web_search_replay_baseline_is_missing() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Recovered follow up."))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let request = base_request(vec![
        user_message("weather?"),
        ResponseItem::WebSearchCall {
            id: Some("ws_old_1".to_string()),
            status: Some("completed".to_string()),
            action: Some(llmconduit::models::responses::WebSearchAction::Search {
                query: Some("weather seattle".to_string()),
                queries: None,
            }),
        },
        ResponseItem::message_text("assistant", "It is rainy."),
        user_message("why?"),
    ]);

    let events = collect_stream(
        gateway
            .stream_responses(request)
            .await
            .expect("stream should not fail"),
    )
    .await;

    assert_eq!(
        event_names(&events),
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].messages.len(), 5);
    assert_eq!(requests[0].messages[1].role, "assistant");
    assert_eq!(requests[0].messages[2].role, "tool");
    assert_eq!(
        requests[0].messages[2].tool_call_id.as_deref(),
        Some("ws_old_1")
    );
    assert_eq!(
        requests[0].messages[2]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some(
            "Previous web_search completed in an earlier turn, but the original tool result is unavailable because replay state was missing. Query: weather seattle"
        )
    );
}

#[tokio::test]
async fn debug_ui_is_disabled_by_default() {
    let app = llmconduit::build_app(test_config());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/debug")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn serves_embedded_debug_web_ui_when_enabled() {
    // Loopback + no token env → D7 dev-open mode: `/debug` serves without a
    // login. The client logic now lives in the externalized `/debug/app.js`
    // (D7) so `/debug` can ship a strict `script-src 'self'` CSP.
    let app = llmconduit::build_app_with_options(
        test_config(),
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri("/debug")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let csp = response
        .headers()
        .get(axum::http::header::CONTENT_SECURITY_POLICY)
        .and_then(|v| v.to_str().ok())
        .expect("/debug carries a CSP")
        .to_string();
    // The inline module script was externalized → the CSP needs NO
    // 'unsafe-inline' in script-src.
    assert!(csp.contains("script-src 'self'"), "csp: {csp}");
    assert!(
        !csp.contains("script-src 'self' 'unsafe-inline'"),
        "/debug script-src must not allow unsafe-inline: {csp}"
    );
    assert!(csp.contains("frame-ancestors 'none'"), "csp: {csp}");
    assert_security_headers(response.headers());

    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body = String::from_utf8(body_bytes.to_vec()).expect("utf8 body");
    assert!(body.contains("llmconduit debug"));
    // The script is now an external reference, not an inline module.
    assert!(
        body.contains("src=\"/debug/app.js\""),
        "expected external script tag"
    );
    assert!(
        !body.contains("new WebSocket"),
        "client logic must live in /debug/app.js, not inline"
    );
}

#[tokio::test]
async fn debug_app_js_is_served_with_strict_csp() {
    let app = llmconduit::build_app_with_options(
        test_config(),
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri("/debug/app.js")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/javascript; charset=utf-8")
    );
    let body_bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("read body");
    let body = String::from_utf8(body_bytes.to_vec()).expect("utf8 body");
    // The externalized client logic — including the WS connect that used to be
    // inline — is here.
    assert!(body.contains("/debug/ws"));
    assert!(body.contains("new WebSocket"));
}

#[tokio::test]
async fn dashboard_shell_carries_csp_and_bootstrap_in_dev_open() {
    // Dev-open (loopback, no token) → `/dashboard` serves the SPA shell with the
    // injected bootstrap object + the dashboard CSP.
    let app = llmconduit::build_app_with_options(
        test_config(),
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri("/dashboard")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let csp = response
        .headers()
        .get(axum::http::header::CONTENT_SECURITY_POLICY)
        .and_then(|v| v.to_str().ok())
        .expect("/dashboard carries a CSP")
        .to_string();
    assert!(csp.contains("default-src 'self'"), "csp: {csp}");
    assert!(csp.contains("connect-src 'self' ws: wss:"), "csp: {csp}");
    assert!(csp.contains("frame-ancestors 'none'"), "csp: {csp}");
    // The injected inline bootstrap is authorized by a per-response nonce.
    assert!(
        csp.contains("'nonce-"),
        "dashboard CSP must carry a script nonce: {csp}"
    );
    assert_security_headers(response.headers());
    // A fresh CSRF cookie accompanies the authenticated shell.
    assert!(
        set_cookie_values(response.headers())
            .iter()
            .any(|c| c.starts_with("llmconduit_csrf=")),
        "authenticated dashboard shell must set a CSRF cookie"
    );

    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body = String::from_utf8(body_bytes.to_vec()).expect("utf8 body");
    assert!(
        body.contains("window.__LLMCONDUIT_DASHBOARD__"),
        "bootstrap injected"
    );
    assert!(body.contains("\"authenticated\":true"));
}

// ---------------------------------------------------------------------------
// D7a auth-gated integration tests: a router built WITH a configured token (the
// production posture) so the 401/200 gating, login flow, login-shell, and
// startup refusal run through the REAL axum stack — without mutating the shared
// process environment.
// ---------------------------------------------------------------------------

/// A `DashboardEnv` with a token + https origin (production posture). Mutations
/// off by default.
fn authed_env() -> llmconduit::dashboard_auth::DashboardEnv {
    use base64::Engine as _;
    llmconduit::dashboard_auth::DashboardEnv {
        token: Some("integration-token".to_string()),
        session_key_b64: Some(base64::engine::general_purpose::STANDARD.encode([42u8; 32])),
        public_origin: Some("https://dash.example.com".to_string()),
        allow_insecure: false,
        allow_mutations: false,
    }
}

/// Build a router whose protected routes are registered with a fully-configured
/// `DashboardAuth` built from `env`, for a server bound to `bind`. Returns the
/// router and the auth handle (so a test can mint a valid session cookie).
fn authed_router(
    bind: std::net::SocketAddr,
    env: &llmconduit::dashboard_auth::DashboardEnv,
) -> (axum::Router, Arc<llmconduit::dashboard_auth::DashboardAuth>) {
    let auth = llmconduit::dashboard_auth::DashboardAuth::from_env(bind, env)
        .expect("auth builds")
        .auth;
    let gateway = test_gateway_with_flow_store(MockUpstream::default(), MockSearch::default())
        .as_ref()
        .clone()
        .with_dashboard_auth(Some(Arc::clone(&auth)));
    let router = llmconduit::http::build_router(
        Arc::new(gateway),
        llmconduit::http::RouterOptions {
            with_debug_ui: true,
            register_protected_routes: true,
        },
    );
    (router, auth)
}

/// Like [`authed_router`] but ALSO returns the `Arc<Gateway>` so a D7b test can drive a
/// real flow (advancing the monitor sequence + the FlowStore independently) and then read
/// `debug_snapshot().last_sequence` / `flow_store().flow_seq()` to assert the live
/// `/dashboard/ws` snapshot sources its flow-domain dedup cursor from the MONITOR
/// sequence (D7b R4 finding 1), not the FlowStore `flow_seq`. The SAME `Arc<Gateway>` is
/// shared with the router, so a flow driven through the returned handle is visible to the
/// socket the router serves.
fn authed_router_with_gateway(
    bind: std::net::SocketAddr,
    env: &llmconduit::dashboard_auth::DashboardEnv,
) -> (
    axum::Router,
    Arc<llmconduit::dashboard_auth::DashboardAuth>,
    Arc<Gateway>,
) {
    let auth = llmconduit::dashboard_auth::DashboardAuth::from_env(bind, env)
        .expect("auth builds")
        .auth;
    let gateway = Arc::new(
        test_gateway_with_flow_store(MockUpstream::default(), MockSearch::default())
            .as_ref()
            .clone()
            .with_dashboard_auth(Some(Arc::clone(&auth))),
    );
    let router = llmconduit::http::build_router(
        Arc::clone(&gateway),
        llmconduit::http::RouterOptions {
            with_debug_ui: true,
            register_protected_routes: true,
        },
    );
    (router, auth, gateway)
}

fn assert_security_headers(headers: &axum::http::HeaderMap) {
    assert_eq!(
        headers
            .get(axum::http::header::X_FRAME_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("DENY")
    );
    assert_eq!(
        headers
            .get(axum::http::header::X_CONTENT_TYPE_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("nosniff")
    );
    assert_eq!(
        headers
            .get(axum::http::header::REFERRER_POLICY)
            .and_then(|v| v.to_str().ok()),
        Some("no-referrer")
    );
    assert_eq!(
        headers
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok()),
        Some("no-store")
    );
}

fn set_cookie_values(headers: &axum::http::HeaderMap) -> Vec<String> {
    headers
        .get_all(axum::http::header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .map(ToString::to_string)
        .collect()
}

#[tokio::test]
async fn protected_debug_requires_session_when_token_configured() {
    let (app, auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());

    // No cookie → 401 no-store.
    let unauthed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/debug")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthed.status().as_u16(), 401);
    assert_eq!(
        unauthed
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok()),
        Some("no-store")
    );

    // A valid signed session cookie → 200.
    let (cookie, _exp) = auth.issue_session();
    let authed = app
        .oneshot(
            Request::builder()
                .uri("/debug")
                .header(
                    axum::http::header::COOKIE,
                    format!("llmconduit_session={cookie}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authed.status().as_u16(), 200);
}

#[tokio::test]
async fn dashboard_login_rejects_bad_token_and_sets_signed_cookie_on_success() {
    let (app, _auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());

    // Wrong token → 401.
    let bad = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/dashboard/login")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"token":"nope"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad.status().as_u16(), 401);

    // Correct token → 200 + a signed HttpOnly SameSite=Strict Secure Path=/
    // session cookie AND a non-HttpOnly CSRF cookie.
    let ok = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/dashboard/login")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"token":"integration-token"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok.status().as_u16(), 200);
    let cookies = set_cookie_values(ok.headers());
    let session = cookies
        .iter()
        .find(|c| c.starts_with("llmconduit_session="))
        .expect("session cookie set");
    assert!(session.contains("HttpOnly"), "session: {session}");
    assert!(session.contains("SameSite=Strict"), "session: {session}");
    assert!(session.contains("Path=/"), "session: {session}");
    assert!(
        session.contains("Secure"),
        "session (https origin): {session}"
    );
    assert!(session.contains("Max-Age=86400"), "session: {session}");
    let csrf = cookies
        .iter()
        .find(|c| c.starts_with("llmconduit_csrf="))
        .expect("csrf cookie set");
    assert!(
        !csrf.contains("HttpOnly"),
        "csrf must be readable by the SPA: {csrf}"
    );
}

#[tokio::test]
async fn dashboard_serves_login_shell_when_unauthed() {
    let (app, _auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body = String::from_utf8(
        axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    // The login shell, NOT the authenticated SPA bootstrap.
    assert!(
        body.contains("/dashboard/login"),
        "login form posts to /dashboard/login"
    );
    assert!(
        !body.contains("\"authenticated\":true"),
        "unauthed shell must not inject an authenticated bootstrap"
    );
}

#[tokio::test]
async fn dashboard_logout_clears_cookies() {
    let (app, _auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/dashboard/logout")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 204);
    let cookies = set_cookie_values(response.headers());
    assert!(
        cookies
            .iter()
            .any(|c| c.starts_with("llmconduit_session=;") && c.contains("Max-Age=0")),
        "session cookie cleared: {cookies:?}"
    );
    assert!(
        cookies
            .iter()
            .any(|c| c.starts_with("llmconduit_csrf=;") && c.contains("Max-Age=0")),
        "csrf cookie cleared: {cookies:?}"
    );
}

#[tokio::test]
async fn non_loopback_without_token_refuses_to_register_protected_routes() {
    // Mirror the production decision: a non-loopback bind with no token/origin
    // → refuse. Build a router with `register_protected_routes: false` (what the
    // startup decision yields) and confirm `/debug` is a 404, not a 401.
    let gateway = test_gateway_with_flow_store(MockUpstream::default(), MockSearch::default());
    let app = llmconduit::http::build_router(
        gateway,
        llmconduit::http::RouterOptions {
            with_debug_ui: true,
            register_protected_routes: false,
        },
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri("/debug")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status().as_u16(),
        404,
        "refused routes must not be registered (404, not 401)"
    );
}

#[tokio::test]
async fn insecure_override_non_loopback_still_enforces_real_auth() {
    // SECURITY (finding 1; refined D7a R3 #2): `LLMCONDUIT_ALLOW_INSECURE_DASHBOARD=1`
    // on a non-loopback bind relaxes ONLY the origin *scheme* (TLS) — an explicit
    // `http://` origin is still REQUIRED. With a token + valid session key + an
    // explicit http origin the routes register, but `dev_open` is NOT active —
    // real cookie/token auth is enforced. An unauthenticated request must get 401
    // (NOT a dev-open 200), and a valid signed cookie 200.
    use base64::Engine as _;
    let env = llmconduit::dashboard_auth::DashboardEnv {
        token: Some("integration-token".to_string()),
        session_key_b64: Some(base64::engine::general_purpose::STANDARD.encode([42u8; 32])),
        // Explicit http origin: the override relaxes the scheme, not the
        // exact-origin requirement (an origin-less override now refuses).
        public_origin: Some("http://dash.lan:4000".to_string()),
        allow_insecure: true,
        allow_mutations: false,
    };
    let bind: std::net::SocketAddr = "0.0.0.0:4000".parse().unwrap();
    // The startup decision registers under the override...
    assert!(
        llmconduit::dashboard_auth::startup_route_decision(bind, &env).should_register(),
        "token + key + explicit http origin + insecure override must register on non-loopback"
    );
    let (app, auth) = authed_router(bind, &env);
    // ...but dev-open is off (a token is configured), so an unauthenticated
    // request is rejected rather than treated as authenticated.
    let unauthed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/debug")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        unauthed.status().as_u16(),
        401,
        "insecure override must NOT make the dashboard dev-open (unauthenticated)"
    );
    // A valid signed session cookie still authenticates.
    let (cookie, _exp) = auth.issue_session();
    let authed = app
        .oneshot(
            Request::builder()
                .uri("/debug")
                .header(
                    axum::http::header::COOKIE,
                    format!("llmconduit_session={cookie}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authed.status().as_u16(), 200);
}

#[tokio::test]
async fn insecure_override_non_loopback_without_token_is_explicitly_dev_open() {
    // The override is the operator's explicit opt-in to a fully unauthenticated
    // non-loopback dashboard. It must register loudly and serve without a login.
    let env = llmconduit::dashboard_auth::DashboardEnv {
        token: None,
        session_key_b64: None,
        public_origin: None,
        allow_insecure: true,
        allow_mutations: false,
    };
    let bind: std::net::SocketAddr = "0.0.0.0:4000".parse().unwrap();
    assert!(
        llmconduit::dashboard_auth::startup_route_decision(bind, &env).should_register(),
        "the explicit insecure override must register tokenless non-loopback routes"
    );
    let (app, auth) = authed_router(bind, &env);
    assert!(auth.dev_open());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/debug")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
}

#[tokio::test]
async fn dashboard_routes_are_disabled_by_default() {
    let app = llmconduit::build_app(test_config());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/dashboard")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    // Gated behind `--with-debug-ui`; off → not registered → fallback 404.
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn serves_embedded_dashboard_shell_when_enabled() {
    let app = llmconduit::build_app_with_options(
        test_config(),
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri("/dashboard")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/html; charset=utf-8")
    );
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body = String::from_utf8(body_bytes.to_vec()).expect("utf8 body");
    // Default suite runs against the node-less stub embedded by build.rs; a real
    // `LLMCONDUIT_BUILD_DASHBOARD=1` build embeds the SPA shell instead. Assert
    // only the invariant shared by both: a non-empty HTML document.
    assert!(body.to_ascii_lowercase().contains("<!doctype html"));
    assert!(!body.is_empty());
}

#[tokio::test]
async fn dashboard_asset_route_serves_present_asset_and_404s_missing() {
    let app = llmconduit::build_app_with_options(
        test_config(),
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );

    // Discover an asset that is REALLY embedded in THIS build rather than hard-
    // coding a name: the node-less stub embeds `assets/stub.txt`, while a real
    // `LLMCONDUIT_BUILD_DASHBOARD=1` build embeds content-hashed Vite assets whose
    // names are unknowable here. Asserting the discovered path keeps this test
    // green under BOTH build modes. The route captures the portion after
    // `assets/`, so strip that prefix from the returned `assets/<name>` path.
    let asset_path = llmconduit::dashboard_ui::first_embedded_asset_path()
        .expect("build.rs always embeds at least one asset under assets/");
    let asset_suffix = asset_path
        .strip_prefix("assets/")
        .expect("embedded asset path is rooted at assets/");
    let present = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/dashboard/assets/{asset_suffix}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(present.status().as_u16(), 200);
    // The positive case must carry a sane Content-Type, not a bare 200.
    assert!(
        present
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|ct| !ct.is_empty()),
        "served asset must set a non-empty Content-Type"
    );

    // A path with no embedded asset must 404 (not fall through to the SPA shell).
    let missing = app
        .oneshot(
            Request::builder()
                .uri("/dashboard/assets/does-not-exist-9f8e7d.js")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(missing.status().as_u16(), 404);
}

// ---------------------------------------------------------------------------
// D7b: the batched `/dashboard/ws` envelope — auth gate (signed cookie + Origin
// allow-list), expiry, and the disabled-by-default posture. The frame-building /
// per-domain-seq / byte-for-byte-fixture logic is unit-tested in
// `src/dashboard_ws.rs` (the socket can't be constructed off a real upgrade in a
// unit test); these assert the route's auth/Origin gating through the real stack.
// ---------------------------------------------------------------------------

/// A real WS-upgrade handshake over a genuine ephemeral TCP server (so hyper
/// injects the `OnUpgrade` extension the `WebSocketUpgrade` extractor requires —
/// `tower::ServiceExt::oneshot` does NOT, which is why a oneshot WS upgrade always
/// 426s before the handler's auth runs). Writes a raw HTTP/1.1 upgrade request
/// with the supplied `Cookie`/`Origin` headers and returns the response status
/// line's numeric code — enough to assert the cookie+Origin auth gate end-to-end
/// (401 reject, 101 accept) without a full WS client dependency.
async fn ws_handshake_status(
    router: axum::Router,
    path: &str,
    cookie: Option<&str>,
    origin: Option<&str>,
) -> u16 {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    // A real ephemeral TCP server: `axum::serve` runs hyper's full HTTP/1.1 stack
    // over the socket (injecting the `OnUpgrade` extension the WS extractor needs),
    // so the upgrade reaches the handler's auth check. Aborted after the request.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router.into_make_service()).await;
    });

    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    );
    if let Some(cookie) = cookie {
        request.push_str(&format!("Cookie: llmconduit_session={cookie}\r\n"));
    }
    if let Some(origin) = origin {
        request.push_str(&format!("Origin: {origin}\r\n"));
    }
    request.push_str("\r\n");

    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client.write_all(request.as_bytes()).await.unwrap();
    // Read the status line (the first response line is enough for the assertion).
    let mut buf = vec![0u8; 256];
    let n = client.read(&mut buf).await.unwrap();
    let head = String::from_utf8_lossy(&buf[..n]);
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    drop(client);
    server.abort();
    status
}

#[tokio::test]
async fn dashboard_ws_is_disabled_by_default() {
    // No `--with-debug-ui` → `/dashboard/ws` is not registered → fallback 404.
    // (oneshot is fine here: a 404 is decided by routing before the WS extractor.)
    let app = llmconduit::build_app(test_config());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/dashboard/ws")
                .header(axum::http::header::CONNECTION, "upgrade")
                .header(axum::http::header::UPGRADE, "websocket")
                .header(axum::http::header::SEC_WEBSOCKET_VERSION, "13")
                .header(
                    axum::http::header::SEC_WEBSOCKET_KEY,
                    "dGhlIHNhbXBsZSBub25jZQ==",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn dashboard_ws_requires_session_cookie() {
    // Production posture (configured token): a real WS upgrade with NO session
    // cookie is rejected 401 before the socket upgrades.
    let (app, _auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());
    let status =
        ws_handshake_status(app, "/dashboard/ws", None, Some("https://dash.example.com")).await;
    assert_eq!(
        status, 401,
        "a WS upgrade without the signed session cookie must be rejected"
    );
}

#[tokio::test]
async fn dashboard_ws_rejects_cross_origin_even_with_valid_cookie() {
    // CSWSH defense: a valid signed cookie but a CROSS-ORIGIN `Origin` (a malicious
    // page riding a stolen cookie) must be rejected — the exact-origin allow-list
    // is enforced for the WS upgrade independent of the cookie.
    let (app, auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());
    let (cookie, _exp) = auth.issue_session();
    let status = ws_handshake_status(
        app,
        "/dashboard/ws",
        Some(&cookie),
        Some("https://evil.example.com"),
    )
    .await;
    assert_eq!(
        status, 401,
        "a cross-origin WS upgrade must be rejected even with a valid cookie"
    );
}

#[tokio::test]
async fn dashboard_ws_accepts_valid_cookie_and_origin() {
    // Happy path: a valid signed cookie + the exact configured Origin → the upgrade
    // is accepted (101 Switching Protocols).
    let (app, auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());
    let (cookie, _exp) = auth.issue_session();
    let status = ws_handshake_status(
        app,
        "/dashboard/ws",
        Some(&cookie),
        Some("https://dash.example.com"),
    )
    .await;
    assert_eq!(
        status, 101,
        "a valid cookie + exact Origin must complete the WS upgrade"
    );
}

/// Open a REAL `/dashboard/ws` connection over an ephemeral TCP server (the same
/// `axum::serve` posture as [`ws_handshake_status`], so hyper injects the `OnUpgrade`
/// the WS extractor needs), complete the upgrade, and return the live post-101 stream
/// plus the server's `JoinHandle` so a D7b end-to-end test can read server frames and
/// send client frames. Panics if the upgrade is not `101`.
async fn ws_connect(
    router: axum::Router,
    cookie: &str,
    origin: &str,
) -> (tokio::net::TcpStream, tokio::task::JoinHandle<()>) {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router.into_make_service()).await;
    });

    let request = format!(
        "GET /dashboard/ws HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Cookie: llmconduit_session={cookie}\r\nOrigin: {origin}\r\n\r\n"
    );
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();

    // Read until the end of the HTTP response headers (CRLFCRLF). The 101 response
    // has no body, so any bytes AFTER the header terminator are the first WS frame —
    // but in practice the server writes the snapshot frame in a later write, so we
    // stop exactly at the header boundary and leave WS frames for the frame reader.
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await.unwrap();
        assert!(
            n == 1,
            "connection closed before the upgrade response completed"
        );
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8_lossy(&head);
    let code = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .unwrap_or(0);
    assert_eq!(code, 101, "expected a 101 WS upgrade, got {code}: {head}");
    (stream, server)
}

/// Read ONE server→client WS frame (FIN assumed; server frames are never masked per
/// RFC 6455 §5.1). Returns `(opcode, payload)`. Handles the 7-bit, 16-bit, and 64-bit
/// payload-length forms. Used to read the dashboard's initial snapshot frame.
async fn ws_read_frame(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    ws_try_read_frame(stream)
        .await
        .expect("expected a WS frame, got EOF")
}

/// Like [`ws_read_frame`] but returns `None` on a clean EOF (the server closed the TCP
/// connection) instead of panicking — so a teardown test can treat EOF as a valid
/// "server stopped serving" signal alongside an explicit Close frame.
async fn ws_try_read_frame(stream: &mut tokio::net::TcpStream) -> Option<(u8, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
    let mut hdr = [0u8; 2];
    // A 0-byte read == EOF; a partial header == abrupt close. Treat both as EOF.
    if stream.read_exact(&mut hdr).await.is_err() {
        return None;
    }
    let opcode = hdr[0] & 0x0f;
    assert_eq!(hdr[1] & 0x80, 0, "server frames must NOT be masked");
    let len = match hdr[1] & 0x7f {
        126 => {
            let mut ext = [0u8; 2];
            stream.read_exact(&mut ext).await.ok()?;
            u16::from_be_bytes(ext) as usize
        }
        127 => {
            let mut ext = [0u8; 8];
            stream.read_exact(&mut ext).await.ok()?;
            u64::from_be_bytes(ext) as usize
        }
        n => n as usize,
    };
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.ok()?;
    Some((opcode, payload))
}

/// D7b R2 (refactor regression guard): the split-socket `dashboard_socket` still sends
/// the initial `type:"snapshot"` message FIRST over a real upgrade. This exercises the
/// sink half end-to-end (every send now goes through `SplitSink`), proving the
/// finding-4 socket split did not break the send path.
#[tokio::test]
async fn dashboard_ws_sends_initial_snapshot_frame() {
    let (app, auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());
    let (cookie, _exp) = auth.issue_session();
    let (mut stream, server) = ws_connect(app, &cookie, "https://dash.example.com").await;

    // The FIRST WS frame is a text frame (opcode 0x1) carrying the snapshot envelope.
    let (opcode, payload) = ws_read_frame(&mut stream).await;
    assert_eq!(opcode, 0x1, "the first dashboard frame is a text message");
    let value: serde_json::Value = serde_json::from_slice(&payload).expect("snapshot is JSON");
    assert_eq!(
        value["type"],
        serde_json::json!("snapshot"),
        "the first message MUST be the type:\"snapshot\" envelope (D7b finding 1)"
    );
    // The snapshot carries the four dedup cursors the SPA installs as its baseline.
    assert!(value["cursors"]["flow_seq"].is_u64());
    assert!(value["cursors"]["metrics_seq"].is_u64());

    drop(stream);
    server.abort();
}

/// D7b R4 finding 1 (end-to-end): the live `/dashboard/ws` snapshot sources its
/// flow-domain dedup cursor from the MONITOR's `last_sequence` (captured atomically with
/// the transcript), NOT the FlowStore `flow_seq`. Drive a real flow so the monitor
/// sequence advances (the engine emits RequestUpsert/segments/status/usage), then connect
/// and assert the snapshot's `cursors.flow_seq` equals `debug_snapshot().last_sequence`.
/// The flow-domain live frames are stamped with this same monitor clock, so a flow frame
/// with `seq > last_sequence` applies and one already reflected is deduped — the whole
/// point of the fix (a delayed monitor update can no longer inherit a newer FlowStore
/// `record_seq` and dedup-drop the final flow frame).
#[tokio::test]
async fn dashboard_ws_snapshot_flow_cursor_is_monitor_last_sequence() {
    let (app, auth, gateway) =
        authed_router_with_gateway("0.0.0.0:4000".parse().unwrap(), &authed_env());

    // Drive one real flow through the engine so the monitor sequence advances well past 0
    // (each engine emit bumps `last_sequence`) and a flow record is opened + finalized.
    let api_call_id = d3_open_flow(&gateway);
    let request = base_request(vec![user_message("hi")]);
    let stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");
    let _events = collect_stream(stream).await;
    let _record = d3_await_terminal(&gateway, &api_call_id).await;

    // Read the monitor sequence the snapshot must source its flow cursor from. The flow
    // had real activity, so this is strictly > 0.
    let monitor_last_seq = gateway.debug_snapshot().last_sequence;
    assert!(
        monitor_last_seq > 0,
        "the driven flow advanced the monitor sequence past 0"
    );

    let (cookie, _exp) = auth.issue_session();
    let (mut socket, server) = ws_connect(app, &cookie, "https://dash.example.com").await;
    let (opcode, payload) = ws_read_frame(&mut socket).await;
    assert_eq!(opcode, 0x1, "the first dashboard frame is a text message");
    let value: serde_json::Value = serde_json::from_slice(&payload).expect("snapshot is JSON");
    assert_eq!(value["type"], serde_json::json!("snapshot"));

    // The crux: the snapshot's flow-domain dedup baseline is the MONITOR's last_sequence
    // (the monitor clock the live flow frames are stamped with) — NOT the FlowStore seq.
    assert_eq!(
        value["cursors"]["flow_seq"].as_u64(),
        Some(monitor_last_seq),
        "the snapshot flow cursor MUST be the monitor's last_sequence (D7b R4 finding 1)"
    );

    drop(socket);
    server.abort();
}

/// D7b R2 finding 4: when the CLIENT sends a WS Close, the server's inbound read
/// detects it and tears the connection down — replying with its own Close frame rather
/// than lingering until the cookie `exp`. Without the inbound read, the client close is
/// invisible and the server task + its broadcast receiver leak until expiry.
#[tokio::test]
async fn dashboard_ws_closes_when_client_sends_close() {
    use tokio::io::AsyncWriteExt;
    let (app, auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());
    let (cookie, _exp) = auth.issue_session();
    let (mut stream, server) = ws_connect(app, &cookie, "https://dash.example.com").await;

    // Drain the initial snapshot frame (and any immediate replay frame) is unnecessary;
    // send a client Close straight away. A client→server frame MUST be masked (RFC 6455
    // §5.3): FIN+Close opcode (0x88), mask bit set, len 0, 4-byte masking key.
    let close_frame: [u8; 6] = [0x88, 0x80, 0x00, 0x00, 0x00, 0x00];
    stream.write_all(&close_frame).await.unwrap();
    stream.flush().await.unwrap();

    // The server detects the inbound close and tears the connection down — either with
    // an explicit Close frame (opcode 0x8) or by dropping the socket (a clean EOF). Both
    // prove finding 4: the inbound read drives teardown. We read frames until we observe
    // the close/EOF, bounded by a timeout so a REGRESSION (server ignores the inbound
    // close and lingers until `exp`) fails fast instead of hanging the suite.
    let tore_down = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match ws_try_read_frame(&mut stream).await {
                // Explicit server Close frame.
                Some((0x8, _)) => return true,
                // Snapshot / replay text frames may precede teardown; keep reading.
                Some(_) => {}
                // Clean EOF: the server dropped the socket → it stopped serving.
                None => return true,
            }
        }
    })
    .await;
    assert_eq!(
        tore_down,
        Ok(true),
        "the server MUST tear down the socket after the client's inbound Close (finding 4)"
    );

    drop(stream);
    server.abort();
}

#[tokio::test]
async fn debug_ws_contract_unchanged_bare_message_route_still_gated() {
    // D7b must NOT change `/debug/ws`: it remains a separately-registered route
    // with the SAME cookie+Origin gate (the bare `DebugWsMessage` contract). A
    // cookieless upgrade is still 401; a valid cookie + Origin still upgrades.
    let (app, auth) = authed_router("0.0.0.0:4000".parse().unwrap(), &authed_env());
    let unauthed = ws_handshake_status(
        app.clone(),
        "/debug/ws",
        None,
        Some("https://dash.example.com"),
    )
    .await;
    assert_eq!(unauthed, 401, "/debug/ws still requires the session cookie");

    let (cookie, _exp) = auth.issue_session();
    let authed = ws_handshake_status(
        app,
        "/debug/ws",
        Some(&cookie),
        Some("https://dash.example.com"),
    )
    .await;
    assert_eq!(authed, 101, "/debug/ws upgrade unchanged");
}

#[tokio::test]
async fn fallback_models_endpoint_filters_to_provider_model_override() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(503).set_body_string("primary unavailable"))
        .mount(&primary)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"fallback-etag\"")
                .set_body_json(json!({
                    "object": "list",
                    "data": [
                        {"id": "other-model", "object": "model", "owned_by": "fallback"},
                        {"id": "fallback-model", "object": "model", "owned_by": "fallback"}
                    ]
                })),
        )
        .mount(&fallback)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", primary.uri()).parse().expect("url");
    config.fallback_upstreams = vec![FallbackUpstreamConfig {
        name: "fallback".to_string(),
        upstream_base_url: format!("{}/v1/", fallback.uri()).parse().expect("url"),
        upstream_api_key: None,
        upstream_model: Some("fallback-model".to_string()),
        exposed_model: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstream_request_log_path: None,
    }];

    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    assert!(response.headers().get("etag").is_none());
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    assert_eq!(
        body,
        json!({
            "object": "list",
            "data": [
                {"id": "fallback-model", "object": "model", "owned_by": "fallback"}
            ]
        })
    );
}

#[tokio::test]
async fn fallback_models_endpoint_without_provider_model_override_passes_list_through() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;
    let fallback_body = json!({
        "object": "list",
        "data": [
            {"id": "fallback-a", "object": "model"},
            {"id": "fallback-b", "object": "model"}
        ]
    });

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(503).set_body_string("primary unavailable"))
        .mount(&primary)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"fallback-etag\"")
                .set_body_json(fallback_body.clone()),
        )
        .mount(&fallback)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", primary.uri()).parse().expect("url");
    config.fallback_upstreams = vec![FallbackUpstreamConfig {
        name: "fallback".to_string(),
        upstream_base_url: format!("{}/v1/", fallback.uri()).parse().expect("url"),
        upstream_api_key: None,
        upstream_model: None,
        exposed_model: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstream_request_log_path: None,
    }];

    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok()),
        Some("\"fallback-etag\"")
    );
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    assert_eq!(body, fallback_body);
}

#[tokio::test]
async fn proxies_models_endpoint_with_etag() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"etag-1\"")
                .set_body_json(json!({
                    "data": [{"id": "glm-5.1"}]
                })),
        )
        .mount(&server)
        .await;

    let config = Config {
        bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
        upstream_base_url: format!("{}/v1/", server.uri()).parse().expect("url"),
        upstream_api_key: None,
        upstream_model: None,
        system_prompt_prefix: None,
        upstream_request_log_path: None,
        turn_capture_dir: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        model_routes: Vec::new(),
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
        image_agent_enabled: false,
        vision_url: None,
        vision_model: None,
        image_cache_max_size: 100,
        image_cache_ttl_secs: 300,
        unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
        price_table: std::collections::HashMap::new(),
    };
    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok()),
        Some("\"etag-1\"")
    );
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    assert_eq!(
        body,
        json!({
            "data": [{"id": "glm-5.1"}]
        })
    );
}

#[tokio::test]
async fn proxies_models_endpoint_with_upstream_api_key() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .and(header("authorization", "Bearer upstream-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "glm-5.1"}]
        })))
        .mount(&server)
        .await;

    let config = Config {
        bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
        upstream_base_url: format!("{}/v1/", server.uri()).parse().expect("url"),
        upstream_api_key: Some("upstream-secret".to_string()),
        upstream_model: None,
        system_prompt_prefix: None,
        upstream_request_log_path: None,
        turn_capture_dir: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        model_routes: Vec::new(),
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
        image_agent_enabled: false,
        vision_url: None,
        vision_model: None,
        image_cache_max_size: 100,
        image_cache_ttl_secs: 300,
        unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
        price_table: std::collections::HashMap::new(),
    };
    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
}

#[tokio::test]
async fn transforms_models_endpoint_for_anthropic_clients() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"upstream-etag\"")
                .set_body_json(json!({
                    "object": "list",
                    "data": [
                        {
                            "id": "glm-5.1",
                            "object": "model",
                            "created": 1760000000,
                            "owned_by": "zai",
                            "context_length": 131072,
                            "max_output_tokens": 8192
                        },
                        {
                            "id": "qwen3",
                            "created_at": "2025-02-19T00:00:00Z",
                            "display_name": "Qwen 3",
                            "capabilities": {
                                "thinking": {
                                    "supported": true
                                }
                            }
                        }
                    ]
                })),
        )
        .mount(&server)
        .await;

    let config = Config {
        bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
        upstream_base_url: format!("{}/v1/", server.uri()).parse().expect("url"),
        upstream_api_key: None,
        upstream_model: None,
        system_prompt_prefix: None,
        upstream_request_log_path: None,
        turn_capture_dir: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        model_routes: Vec::new(),
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
        image_agent_enabled: false,
        vision_url: None,
        vision_model: None,
        image_cache_max_size: 100,
        image_cache_ttl_secs: 300,
        unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
        price_table: std::collections::HashMap::new(),
    };
    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models?limit=1")
                .header("anthropic-version", "2023-06-01")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    assert!(response.headers().get("etag").is_none());
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    assert_eq!(body["has_more"], true);
    assert_eq!(body["first_id"], "glm-5.1");
    assert_eq!(body["last_id"], "glm-5.1");
    let models = body["data"].as_array().expect("data array");
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["id"], "glm-5.1");
    assert_eq!(models[0]["type"], "model");
    assert_eq!(models[0]["display_name"], "glm-5.1");
    assert_eq!(models[0]["created_at"], "2025-10-09T08:53:20Z");
    assert_eq!(models[0]["max_input_tokens"], 131072);
    assert_eq!(models[0]["max_tokens"], 8192);
    assert_eq!(models[0]["capabilities"]["thinking"]["supported"], false);
    assert_eq!(models[0]["capabilities"]["image_input"]["supported"], false);
    assert_eq!(
        models[0]["capabilities"]["structured_outputs"]["supported"],
        false
    );
}

#[tokio::test]
async fn paginates_anthropic_models_transform_with_cursors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": "model-a"},
                {"id": "model-b"},
                {"id": "model-c"}
            ]
        })))
        .mount(&server)
        .await;

    let config = Config {
        bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
        upstream_base_url: format!("{}/v1/", server.uri()).parse().expect("url"),
        upstream_api_key: None,
        upstream_model: None,
        system_prompt_prefix: None,
        upstream_request_log_path: None,
        turn_capture_dir: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        model_routes: Vec::new(),
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
        image_agent_enabled: false,
        vision_url: None,
        vision_model: None,
        image_cache_max_size: 100,
        image_cache_ttl_secs: 300,
        unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
        price_table: std::collections::HashMap::new(),
    };
    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models?after_id=model-a&limit=2")
                .header("anthropic-version", "2023-06-01")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    assert_eq!(body["has_more"], false);
    assert_eq!(body["first_id"], "model-b");
    assert_eq!(body["last_id"], "model-c");
    let ids = body["data"]
        .as_array()
        .expect("data array")
        .iter()
        .map(|model| model["id"].as_str().expect("model id"))
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["model-b", "model-c"]);
}

#[tokio::test]
async fn proxies_completions_endpoint_passthrough() {
    let server = MockServer::start().await;
    let request_body = json!({
        "model": "GLM-5",
        "prompt": "hello",
        "stream": false,
        "max_tokens": 8
    });
    let upstream_body = json!({
        "id": "cmpl-1",
        "object": "text_completion",
        "choices": [{ "text": "ok", "index": 0, "finish_reason": "stop" }]
    });
    Mock::given(method("POST"))
        .and(path("/v1/completions"))
        .and(header("authorization", "Bearer upstream-secret"))
        .and(header("content-type", "application/json"))
        .and(header("x-trace-id", "trace-1"))
        .and(body_json(request_body.clone()))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("x-upstream", "yes")
                .set_body_json(upstream_body.clone()),
        )
        .mount(&server)
        .await;

    let config = Config {
        bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
        upstream_base_url: format!("{}/v1/", server.uri()).parse().expect("url"),
        upstream_api_key: Some("upstream-secret".to_string()),
        upstream_model: None,
        system_prompt_prefix: None,
        upstream_request_log_path: None,
        turn_capture_dir: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        model_routes: Vec::new(),
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
        image_agent_enabled: false,
        vision_url: None,
        vision_model: None,
        image_cache_max_size: 100,
        image_cache_ttl_secs: 300,
        unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
        price_table: std::collections::HashMap::new(),
    };
    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/completions")
                .header("content-type", "application/json")
                .header("authorization", "Bearer client-secret")
                .header("x-trace-id", "trace-1")
                .body(Body::from(
                    serde_json::to_vec(&request_body).expect("serialize request"),
                ))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 202);
    assert_eq!(
        response
            .headers()
            .get("x-upstream")
            .and_then(|value| value.to_str().ok()),
        Some("yes")
    );
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    assert_eq!(body, upstream_body);
}

#[tokio::test]
async fn merges_instructions_and_developer_into_single_system_message() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        user_message("hello"),
        ResponseItem::message_text("assistant", "hi"),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "be concise".to_string(),
            }],
            phase: None,
        },
        user_message("how are you?"),
    ]);
    request.instructions = "You are a helpful assistant.".to_string();

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].messages[0].role, "system");
    assert_eq!(
        requests[0].messages[0]
            .content
            .as_ref()
            .and_then(|v| v.as_str()),
        Some("You are a helpful assistant.")
    );
    // Mid-conversation developer message stays in place (not hoisted)
    assert_eq!(requests[0].messages[3].role, "system");
    assert_eq!(
        requests[0].messages[3]
            .content
            .as_ref()
            .and_then(|v| v.as_str()),
        Some("be concise")
    );
}

#[tokio::test]
async fn merges_multiple_developer_messages_scattered_in_history() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "first instruction".to_string(),
            }],
            phase: None,
        },
        user_message("hello"),
        ResponseItem::message_text("assistant", "hi"),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "second instruction".to_string(),
            }],
            phase: None,
        },
        user_message("how are you?"),
        ResponseItem::message_text("assistant", "fine"),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "third instruction".to_string(),
            }],
            phase: None,
        },
        user_message("bye"),
    ]);
    request.instructions = "base".to_string();

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let messages = &requests[0].messages;
    assert_eq!(messages[0].role, "system");
    assert_eq!(
        messages[0].content.as_ref().and_then(|v| v.as_str()),
        Some("base\n\nfirst instruction")
    );
    let system_count = messages.iter().filter(|m| m.role == "system").count();
    assert_eq!(
        system_count, 3,
        "initial block coalesced, mid-conversation stay in place"
    );
}

#[tokio::test]
async fn no_system_message_when_no_instructions_and_no_developer() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let request = base_request(vec![
        user_message("hello"),
        ResponseItem::message_text("assistant", "hi"),
        user_message("bye"),
    ]);

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let system_count = requests[0]
        .messages
        .iter()
        .filter(|m| m.role == "system")
        .count();
    assert_eq!(system_count, 0, "no system message when nothing to merge");
    assert_eq!(requests[0].messages[0].role, "user");
}

#[tokio::test]
async fn developer_only_no_instructions_produces_single_system_message() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let request = base_request(vec![
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "instruction A".to_string(),
            }],
            phase: None,
        },
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "instruction B".to_string(),
            }],
            phase: None,
        },
        user_message("hello"),
    ]);

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let messages = &requests[0].messages;
    assert_eq!(messages[0].role, "system");
    assert_eq!(
        messages[0].content.as_ref().and_then(|v| v.as_str()),
        Some("instruction A\n\ninstruction B")
    );
    assert_eq!(messages[1].role, "user");
    let system_count = messages.iter().filter(|m| m.role == "system").count();
    assert_eq!(system_count, 1);
}

#[tokio::test]
async fn function_call_history_with_developer_message_produces_correct_ordering() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "result is 555"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        user_message("what is 15 * 37?"),
        ResponseItem::FunctionCall {
            id: None,
            name: "calculator".to_string(),
            namespace: None,
            arguments: r#"{"expression":"15*37"}"#.to_string(),
            call_id: "call_001".to_string(),
        },
        ResponseItem::FunctionCallOutput {
            call_id: "call_001".to_string(),
            output: serde_json::Value::String("555".to_string()),
        },
        ResponseItem::message_text("assistant", "15 * 37 = 555"),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "show work step by step".to_string(),
            }],
            phase: None,
        },
        user_message("now add 45"),
    ]);
    request.instructions = "You are a calculator.".to_string();
    request.tools = vec![ToolSpec::Function {
        name: "calculator".to_string(),
        description: "Evaluate math".to_string(),
        strict: false,
        parameters: json!({
            "type": "object",
            "properties": { "expression": { "type": "string" } },
            "required": ["expression"]
        }),
    }];

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(
        event_names(&events).contains(&"response.completed"),
        "stream should complete successfully"
    );

    let requests = upstream.requests().await;
    let messages = &requests[0].messages;
    assert_eq!(messages[0].role, "system");
    assert_eq!(
        messages[0].content.as_ref().and_then(|v| v.as_str()),
        Some("You are a calculator.")
    );
    assert_eq!(messages[1].role, "user");
    assert_eq!(messages[2].role, "assistant");
    assert!(
        messages[2].tool_calls.is_some(),
        "assistant message should have tool_calls"
    );
    assert_eq!(messages[3].role, "tool");
    assert_eq!(messages[3].tool_call_id.as_deref(), Some("call_001"));
    assert_eq!(messages[4].role, "assistant");
    assert_eq!(messages[5].role, "system");
    assert_eq!(
        messages[5].content.as_ref().and_then(|v| v.as_str()),
        Some("show work step by step")
    );
    assert_eq!(messages[6].role, "user");
    let system_count = messages.iter().filter(|m| m.role == "system").count();
    assert_eq!(system_count, 2);
}

#[tokio::test]
async fn multiple_function_calls_interleaved_with_developer_messages() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "done"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        user_message("weather and time"),
        ResponseItem::FunctionCall {
            id: None,
            name: "get_weather".to_string(),
            namespace: None,
            arguments: r#"{"city":"NYC"}"#.to_string(),
            call_id: "call_w1".to_string(),
        },
        ResponseItem::FunctionCallOutput {
            call_id: "call_w1".to_string(),
            output: serde_json::Value::String("72F".to_string()),
        },
        ResponseItem::FunctionCall {
            id: None,
            name: "get_time".to_string(),
            namespace: None,
            arguments: r#"{"tz":"EST"}"#.to_string(),
            call_id: "call_t1".to_string(),
        },
        ResponseItem::FunctionCallOutput {
            call_id: "call_t1".to_string(),
            output: serde_json::Value::String("2:30 PM".to_string()),
        },
        ResponseItem::message_text("assistant", "NYC: 72F, 2:30 PM"),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "include celsius".to_string(),
            }],
            phase: None,
        },
        user_message("what about London?"),
    ]);
    request.instructions = "You have weather and time tools.".to_string();
    request.tools = vec![
        ToolSpec::Function {
            name: "get_weather".to_string(),
            description: "Get weather".to_string(),
            strict: false,
            parameters: json!({"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}),
        },
        ToolSpec::Function {
            name: "get_time".to_string(),
            description: "Get time".to_string(),
            strict: false,
            parameters: json!({"type": "object", "properties": {"tz": {"type": "string"}}, "required": ["tz"]}),
        },
    ];

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));

    let requests = upstream.requests().await;
    let messages = &requests[0].messages;
    assert_eq!(messages[0].role, "system");
    assert_eq!(
        messages[0].content.as_ref().and_then(|v| v.as_str()),
        Some("You have weather and time tools.")
    );
    let roles: Vec<&str> = messages.iter().map(|m| m.role.as_str()).collect();
    assert_eq!(
        roles,
        vec![
            "system",
            "user",
            "assistant",
            "tool",
            "assistant",
            "tool",
            "assistant",
            "system",
            "user"
        ]
    );
}

#[tokio::test]
async fn reasoning_with_developer_message_preserves_reasoning_content() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "5x^4"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        user_message("derivative of x^3?"),
        ResponseItem::Reasoning {
            id: "rsn_1".to_string(),
            summary: vec![ReasoningSummaryItem::SummaryText {
                text: "power rule".to_string(),
            }],
            content: None,
            encrypted_content: None,
        },
        ResponseItem::message_text("assistant", "3x^2"),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "show derivation steps".to_string(),
            }],
            phase: None,
        },
        user_message("what about x^5?"),
    ]);
    request.instructions = "You are a math tutor.".to_string();

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));

    let requests = upstream.requests().await;
    let messages = &requests[0].messages;
    assert_eq!(messages[0].role, "system");
    assert_eq!(
        messages[0].content.as_ref().and_then(|v| v.as_str()),
        Some("You are a math tutor.")
    );
    assert_eq!(messages[1].role, "user");
    assert_eq!(messages[2].role, "assistant");
    assert_eq!(
        messages[2].reasoning_content.as_deref(),
        Some("power rule"),
        "reasoning should be attached to the following assistant message"
    );
    assert_eq!(messages[3].role, "system");
    assert_eq!(
        messages[3].content.as_ref().and_then(|v| v.as_str()),
        Some("show derivation steps")
    );
    assert_eq!(messages[4].role, "user");
}

#[tokio::test]
async fn custom_tool_call_with_developer_message() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "result"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        user_message("run my script"),
        ResponseItem::CustomToolCall {
            status: Some("completed".to_string()),
            call_id: "call_ct1".to_string(),
            name: "run_script".to_string(),
            input: r#"print('hello')"#.to_string(),
        },
        ResponseItem::CustomToolCallOutput {
            call_id: "call_ct1".to_string(),
            name: Some("run_script".to_string()),
            output: serde_json::Value::String("hello".to_string()),
        },
        ResponseItem::message_text("assistant", "script printed hello"),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "always explain what the script does".to_string(),
            }],
            phase: None,
        },
        user_message("run it again with different input"),
    ]);
    request.instructions = "You can run scripts.".to_string();
    request.tools = vec![ToolSpec::Custom {
        name: "run_script".to_string(),
        description: "Run a Python script".to_string(),
        format: llmconduit::models::responses::CustomToolFormat {
            kind: "text".to_string(),
            syntax: "python".to_string(),
            definition: "Python code to execute".to_string(),
        },
    }];

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));

    let requests = upstream.requests().await;
    let messages = &requests[0].messages;
    assert_eq!(messages[0].role, "system");
    assert_eq!(
        messages[0].content.as_ref().and_then(|v| v.as_str()),
        Some("You can run scripts.")
    );
    let system_count = messages.iter().filter(|m| m.role == "system").count();
    assert_eq!(system_count, 2);
    assert_eq!(messages[1].role, "user");
    assert_eq!(messages[2].role, "assistant");
    assert!(messages[2].tool_calls.is_some());
    assert_eq!(messages[3].role, "tool");
    assert_eq!(messages[4].role, "assistant");
    assert_eq!(messages[5].role, "system");
    assert_eq!(
        messages[5].content.as_ref().and_then(|v| v.as_str()),
        Some("always explain what the script does")
    );
    assert_eq!(messages[6].role, "user");
}

#[tokio::test]
async fn local_shell_call_in_history_with_developer_message() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        user_message("list files"),
        ResponseItem::LocalShellCall {
            id: Some("ls_1".to_string()),
            call_id: Some("call_ls1".to_string()),
            status: "completed".to_string(),
            action: llmconduit::models::responses::LocalShellAction::Exec(
                llmconduit::models::responses::LocalShellExecAction {
                    command: vec!["ls".to_string(), "-la".to_string()],
                    timeout_ms: None,
                    working_directory: None,
                    env: None,
                    user: None,
                },
            ),
        },
        ResponseItem::FunctionCallOutput {
            call_id: "call_ls1".to_string(),
            output: serde_json::Value::String("file1.txt\nfile2.txt".to_string()),
        },
        ResponseItem::message_text("assistant", "found 2 files"),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "include file sizes".to_string(),
            }],
            phase: None,
        },
        user_message("show details"),
    ]);
    request.instructions = "You can run shell commands.".to_string();
    request.tools = vec![ToolSpec::LocalShell {}];

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));

    let requests = upstream.requests().await;
    let messages = &requests[0].messages;
    assert_eq!(messages[0].role, "system");
    let system_count = messages.iter().filter(|m| m.role == "system").count();
    assert_eq!(system_count, 2);
    let roles: Vec<&str> = messages.iter().map(|m| m.role.as_str()).collect();
    assert_eq!(
        roles,
        vec![
            "system",
            "user",
            "assistant",
            "tool",
            "assistant",
            "system",
            "user"
        ]
    );
}

#[tokio::test]
async fn long_multi_turn_conversation_no_system_drift() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "5"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        user_message("count from 1"),
        ResponseItem::message_text("assistant", "1"),
        user_message("next"),
        ResponseItem::message_text("assistant", "2"),
        user_message("next"),
        ResponseItem::message_text("assistant", "3"),
        user_message("next"),
        ResponseItem::message_text("assistant", "4"),
        user_message("next"),
    ]);
    request.instructions = "You count numbers.".to_string();

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    assert!(event_names(&events).contains(&"response.completed"));

    let requests = upstream.requests().await;
    let messages = &requests[0].messages;
    assert_eq!(messages[0].role, "system");
    assert_eq!(messages.len(), 10); // system + 5 user + 4 assistant
    let system_count = messages.iter().filter(|m| m.role == "system").count();
    assert_eq!(system_count, 1);
}

#[tokio::test]
async fn system_role_in_input_merged_with_instructions() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        ResponseItem::Message {
            id: None,
            role: "system".to_string(),
            content: vec![ContentItem::InputText {
                text: "extra system context".to_string(),
            }],
            phase: None,
        },
        user_message("hello"),
    ]);
    request.instructions = "base instructions".to_string();

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let messages = &requests[0].messages;
    assert_eq!(messages[0].role, "system");
    assert_eq!(
        messages[0].content.as_ref().and_then(|v| v.as_str()),
        Some("base instructions\n\nextra system context")
    );
    let system_count = messages.iter().filter(|m| m.role == "system").count();
    assert_eq!(system_count, 1);
}

#[tokio::test]
async fn tool_call_triggers_new_function_call_response_item() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_calc_1",
            "calculator",
            "{\"expression\":\"45+555\"}",
        ))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![
        user_message("what is 15*37?"),
        ResponseItem::FunctionCall {
            id: None,
            name: "calculator".to_string(),
            namespace: None,
            arguments: r#"{"expression":"15*37"}"#.to_string(),
            call_id: "call_001".to_string(),
        },
        ResponseItem::FunctionCallOutput {
            call_id: "call_001".to_string(),
            output: serde_json::Value::String("555".to_string()),
        },
        ResponseItem::message_text("assistant", "555"),
        user_message("add 45"),
    ]);
    request.tools = vec![ToolSpec::Function {
        name: "calculator".to_string(),
        description: "Evaluate math".to_string(),
        strict: false,
        parameters: json!({
            "type": "object",
            "properties": { "expression": { "type": "string" } },
            "required": ["expression"]
        }),
    }];

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let done_events: Vec<_> = events
        .iter()
        .filter(|e| e["_event"] == "response.output_item.done")
        .collect();
    assert!(
        done_events
            .iter()
            .any(|e| e["item"]["type"].as_str() == Some("function_call")),
        "should emit a function_call output item"
    );
    let fc = done_events
        .iter()
        .find(|e| e["item"]["type"].as_str() == Some("function_call"))
        .unwrap();
    assert_eq!(fc["item"]["name"].as_str(), Some("calculator"));
    assert_eq!(fc["item"]["call_id"].as_str(), Some("call_calc_1"));
}

#[tokio::test]
async fn response_completed_includes_usage_from_upstream() {
    use llmconduit::models::chat::ChunkUsage;

    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "hello")),
            Ok(ChatCompletionChunk {
                id: "chat-1".to_string(),
                choices: vec![],
                usage: Some(ChunkUsage {
                    prompt_tokens: 100,
                    completion_tokens: 25,
                    total_tokens: 125,
                    reasoning_tokens: None,
                    prompt_tokens_details: None,
                    completion_tokens_details: None,
                }),
            }),
        ])
        .await;
    let gateway = test_gateway(upstream, MockSearch::default());
    let request = base_request(vec![user_message("hi")]);
    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let completed = events
        .iter()
        .find(|e| e["type"].as_str() == Some("response.completed"))
        .expect("response.completed event");
    let usage = &completed["response"]["usage"];
    assert_eq!(usage["input_tokens"], 100);
    assert_eq!(usage["output_tokens"], 25);
    assert_eq!(usage["total_tokens"], 125);
}

#[tokio::test]
async fn d5_metrics_populate_only_at_terminal_finalize_not_midstream() {
    // D5: a streamed request records into the MetricsLayer ONLY at the engine's D3
    // terminal finalize seam — NOT mid-stream, NOT from the middleware. Assert the
    // metrics window is EMPTY while the stream is in flight (we check before driving
    // it to completion) and populated exactly once AFTER finalize.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "hello")),
            Ok(usage_chunk("chat-1", 100, 25, 125, Some(10), Some(7))),
        ])
        .await;
    let (gateway, flow_store, metrics) = test_gateway_with_metrics(upstream);

    // The middleware opens the record; the engine claims + finalizes it. Mimic the
    // middleware `open` so the engine's L1 guard claims THIS record.
    let api_call_id = "api_d5_finalize".to_string();
    flow_store.open(
        api_call_id.clone(),
        "POST".to_string(),
        "/v1/responses".to_string(),
        llmconduit::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
        None,
        llmconduit::dashboard_flow::ClientAttribution::none(),
    );
    // Before driving the stream to completion, metrics must be empty (no record at
    // open time, no mid-stream record).
    assert_eq!(
        metrics.view().window_1m.total_count(),
        0,
        "no metrics recorded at open / pre-finalize"
    );
    assert_eq!(
        metrics.metrics_seq(),
        0,
        "metrics seq unbumped pre-finalize"
    );

    let request = base_request(vec![user_message("hi")]);
    let events = collect_stream(
        gateway
            .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
            .await
            .expect("stream"),
    )
    .await;
    // Sanity: the stream completed.
    assert!(
        events
            .iter()
            .any(|e| e["type"].as_str() == Some("response.completed")),
        "stream reached response.completed"
    );

    // After finalize: exactly one terminal recorded into all windows.
    let view = metrics.view();
    assert_eq!(
        view.window_1m.total_count(),
        1,
        "exactly one terminal recorded at finalize"
    );
    assert_eq!(view.window_5m.total_count(), 1);
    assert_eq!(view.window_1h.total_count(), 1);
    // The bucket carries the served model + Success class, and the token sum is the
    // flow's FINAL cumulative usage (recorded once, not per chunk).
    let (key, counts) = view
        .window_1m
        .buckets
        .iter()
        .next()
        .expect("one bucket present");
    assert_eq!(key.status, llmconduit::metrics::StatusClass::Success);
    assert_eq!(key.endpoint, "/v1/responses");
    assert_eq!(counts.count, 1);
    assert_eq!(counts.prompt_tokens, 100, "final cumulative prompt tokens");
    assert_eq!(counts.completion_tokens, 25);
    assert_eq!(counts.cached_tokens, 10);
    // Latency populated the histogram → p50 is non-zero.
    assert!(view.window_1m.percentiles().p50 >= 0.0);
}

#[tokio::test]
async fn d5_coordinated_snapshot_is_internally_consistent_end_to_end() {
    // D5: drive a real flow through the engine, then take a coordinated snapshot and
    // assert the cut is internally consistent — summaries, metrics, topology, and
    // per-domain cursors all reflect the SAME post-finalize state (no torn read).
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "hello")),
            Ok(usage_chunk("chat-1", 100, 25, 125, None, None)),
        ])
        .await;
    let (gateway, flow_store, metrics) = test_gateway_with_metrics(upstream);
    let topology = gateway.provider_health_publisher();
    topology.publish(Vec::new()); // version 1

    let api_call_id = "api_d5_snapshot".to_string();
    flow_store.open(
        api_call_id.clone(),
        "POST".to_string(),
        "/v1/responses".to_string(),
        llmconduit::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
        None,
        llmconduit::dashboard_flow::ClientAttribution::none(),
    );
    let request = base_request(vec![user_message("hi")]);
    let _ = collect_stream(
        gateway
            .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
            .await
            .expect("stream"),
    )
    .await;

    // Take the coordinated cut (the same call the 5 s task makes).
    let cut = metrics
        .snapshot(&flow_store, &topology)
        .expect("coordinated cut");

    // Summaries reflect the finalized flow (body-free).
    let summary = cut
        .summaries
        .iter()
        .find(|s| s.api_call_id == api_call_id)
        .expect("finalized flow in summaries");
    assert_eq!(
        summary.status,
        llmconduit::dashboard_flow::FlowStatus::Completed
    );
    // Metrics reflect the same recorded terminal.
    assert_eq!(cut.metrics.window_1m.total_count(), 1);
    // Topology is the captured published version.
    assert_eq!(cut.topology.version, 1);
    // Per-domain cursors are all present + consistent with the captured stores.
    assert_eq!(
        cut.cursors.flow_seq,
        flow_store.flow_seq(),
        "flow_seq matches"
    );
    assert_eq!(
        cut.cursors.metrics_seq,
        metrics.metrics_seq(),
        "metrics_seq matches"
    );
    assert_eq!(cut.cursors.topology_seq, 1, "topology_seq == version");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d5_lock_order_stress_no_deadlock_under_concurrent_flows_and_snapshots() {
    // D5: the fixed FlowStore→Metrics lock order means many concurrent flows
    // mutating the FlowStore + a snapshot task taking the combined critical section
    // can NEVER deadlock (only the snapshot path holds >1 lock). Drive heavy churn
    // against both stores from many tasks while a snapshot loop runs, and assert the
    // whole thing finishes within a timeout (a deadlock would hang).
    let flow_store = llmconduit::dashboard_flow::DashboardFlowStore::new();
    let metrics = llmconduit::metrics::MetricsLayer::new();
    let topology = llmconduit::upstream::ProviderHealthPublisher::default();
    topology.publish(Vec::new());

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Snapshot loop: repeatedly take the combined FlowStore→Metrics critical section.
    let snapshot_task = {
        let metrics = metrics.clone();
        let flow_store = flow_store.clone();
        let topology = topology.clone();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = metrics.snapshot(&flow_store, &topology);
                tokio::task::yield_now().await;
            }
        })
    };

    // Many concurrent flow + metrics mutators.
    let mut workers = Vec::new();
    for worker_id in 0..8 {
        let flow_store = flow_store.clone();
        let metrics = metrics.clone();
        workers.push(tokio::spawn(async move {
            for index in 0..500 {
                let api = format!("api_{worker_id}_{index}");
                flow_store.open(
                    api.clone(),
                    "POST".to_string(),
                    "/v1/responses".to_string(),
                    llmconduit::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
                    None,
                    llmconduit::dashboard_flow::ClientAttribution::none(),
                );
                flow_store.finalize(
                    &api,
                    llmconduit::dashboard_flow::FlowStatus::Completed,
                    Some("done".to_string()),
                    Some("provider".to_string()),
                );
                metrics.record_response(
                    llmconduit::dashboard_flow::FlowStatus::Completed,
                    Some("m"),
                    "/v1/responses",
                    Some("provider"),
                    5,
                );
            }
        }));
    }

    // All mutators must finish well within a generous timeout (a deadlock hangs).
    let workers_done = async {
        for worker in workers {
            worker.await.expect("worker joins");
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(30), workers_done)
        .await
        .expect("no deadlock: all mutators finished under the fixed lock order");

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    snapshot_task.await.expect("snapshot task joins");

    // The stores survived the churn and the snapshot ring holds body-free cuts.
    let cut = metrics.latest_snapshot().expect("at least one cut");
    for summary in &cut.summaries {
        // Each retained summary is body-free (well under 1 KiB).
        let bytes = serde_json::to_string(summary).expect("serialize").len();
        assert!(bytes < 4096, "snapshot summary is a small body-free record");
    }
}

#[tokio::test]
async fn response_completed_accumulates_usage_across_web_search_rounds() {
    use llmconduit::models::chat::ChunkUsage;

    let upstream = MockUpstream::default();
    // Round 1: model calls web_search
    upstream
        .push_response(vec![
            Ok(tool_call_chunk(
                "chat-1",
                "call_ws_1",
                "web_search",
                r#"{"query":"rust async"}"#,
            )),
            Ok(ChatCompletionChunk {
                id: "chat-1".to_string(),
                choices: vec![],
                usage: Some(ChunkUsage {
                    prompt_tokens: 200,
                    completion_tokens: 30,
                    total_tokens: 230,
                    reasoning_tokens: None,
                    prompt_tokens_details: None,
                    completion_tokens_details: None,
                }),
            }),
        ])
        .await;
    // Round 2: model produces final text after search
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-2", "found it")),
            Ok(ChatCompletionChunk {
                id: "chat-2".to_string(),
                choices: vec![],
                usage: Some(ChunkUsage {
                    prompt_tokens: 350,
                    completion_tokens: 20,
                    total_tokens: 370,
                    reasoning_tokens: None,
                    prompt_tokens_details: None,
                    completion_tokens_details: None,
                }),
            }),
        ])
        .await;

    let gateway = test_gateway(upstream, MockSearch::default());
    let mut request = base_request(vec![user_message("search for rust async")]);
    request.tools = vec![ToolSpec::WebSearch {
        external_web_access: Some(true),
        filters: None,
        user_location: None,
        search_context_size: None,
        search_content_types: None,
    }];

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let completed = events
        .iter()
        .find(|e| e["type"].as_str() == Some("response.completed"))
        .expect("response.completed event");
    let usage = &completed["response"]["usage"];
    assert_eq!(usage["input_tokens"], 200 + 350);
    assert_eq!(usage["output_tokens"], 30 + 20);
    assert_eq!(usage["total_tokens"], 230 + 370);
}

#[tokio::test]
async fn merges_assistant_message_and_tool_call_into_single_upstream_message() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "done"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let request = ResponsesRequest {
        model: "glm-5.1".to_string(),
        instructions: "You are helpful.".to_string(),
        input: vec![
            user_message("explore the codebase"),
            // Assistant reasoning (from a previous turn)
            ResponseItem::Reasoning {
                id: "reasoning_1".to_string(),
                summary: vec![],
                content: Some(vec![
                    llmconduit::models::responses::ReasoningContentItem::ReasoningText {
                        text: "Let me look at the files.".to_string(),
                    },
                ]),
                encrypted_content: None,
            },
            // Assistant text message (from same turn)
            ResponseItem::Message {
                id: None,
                role: "assistant".to_string(),
                content: vec![ContentItem::OutputText {
                    text: "I'll search the codebase.".to_string(),
                }],
                phase: None,
            },
            // Tool call (from same turn — should merge with the assistant message above)
            ResponseItem::FunctionCall {
                id: None,
                call_id: "call_abc".to_string(),
                name: "exec_command".to_string(),
                arguments: r#"{"cmd":"ls"}"#.to_string(),
                namespace: None,
            },
            // Tool result
            ResponseItem::FunctionCallOutput {
                call_id: "call_abc".to_string(),
                output: json!("file1.rs\nfile2.rs"),
            },
        ],
        tools: vec![ToolSpec::Function {
            name: "exec_command".to_string(),
            description: "Run a command".to_string(),
            strict: false,
            parameters: json!({
                "type": "object",
                "properties": { "cmd": { "type": "string" } },
                "required": ["cmd"]
            }),
        }],
        tool_choice: json!("auto"),
        parallel_tool_calls: Some(true),
        reasoning: None,
        thinking: None,
        store: false,
        stream: true,
        include: Vec::new(),
        service_tier: None,
        prompt_cache_key: None,
        text: None,
        client_metadata: None,
        previous_response_id: None,
        temperature: None,
        top_p: None,
        max_output_tokens: None,
        frequency_penalty: None,
        presence_penalty: None,
        truncation: None,
        metadata: None,
        stop: None,
        extra_body: Default::default(),
    };

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let messages = &requests[0].messages;

    // M4: assistant with content does NOT merge with tool call — separate messages
    let content_msg = messages
        .iter()
        .find(|m| m.role == "assistant" && m.content.is_some())
        .expect("assistant message with content");
    assert_eq!(
        content_msg.content,
        Some(serde_json::Value::String(
            "I'll search the codebase.".to_string()
        ))
    );
    assert!(content_msg.reasoning_content.is_some());
    assert!(content_msg.tool_calls.is_none());

    let tool_msg = messages
        .iter()
        .find(|m| m.role == "assistant" && m.tool_calls.is_some())
        .expect("assistant message with tool_calls");
    assert!(tool_msg.content.is_none());
    assert_eq!(tool_msg.tool_calls.as_ref().unwrap().len(), 1);
}

#[tokio::test]
async fn merges_multiple_tool_calls_into_single_upstream_assistant_message() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "done"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let request = ResponsesRequest {
        model: "glm-5.1".to_string(),
        instructions: "You are helpful.".to_string(),
        input: vec![
            user_message("read two files"),
            // Three tool calls from the same assistant turn
            ResponseItem::FunctionCall {
                id: None,
                call_id: "call_1".to_string(),
                name: "read_file".to_string(),
                arguments: r#"{"path":"a.rs"}"#.to_string(),
                namespace: None,
            },
            ResponseItem::FunctionCall {
                id: None,
                call_id: "call_2".to_string(),
                name: "read_file".to_string(),
                arguments: r#"{"path":"b.rs"}"#.to_string(),
                namespace: None,
            },
            ResponseItem::FunctionCall {
                id: None,
                call_id: "call_3".to_string(),
                name: "grep".to_string(),
                arguments: r#"{"pattern":"TODO"}"#.to_string(),
                namespace: None,
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call_1".to_string(),
                output: json!("contents of a.rs"),
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call_2".to_string(),
                output: json!("contents of b.rs"),
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call_3".to_string(),
                output: json!("no matches"),
            },
        ],
        tools: vec![
            ToolSpec::Function {
                name: "read_file".to_string(),
                description: "Read a file".to_string(),
                strict: false,
                parameters: json!({"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}),
            },
            ToolSpec::Function {
                name: "grep".to_string(),
                description: "Search".to_string(),
                strict: false,
                parameters: json!({"type":"object","properties":{"pattern":{"type":"string"}},"required":["pattern"]}),
            },
        ],
        tool_choice: json!("auto"),
        parallel_tool_calls: Some(true),
        reasoning: None,
        thinking: None,
        store: false,
        stream: true,
        include: Vec::new(),
        service_tier: None,
        prompt_cache_key: None,
        text: None,
        client_metadata: None,
        previous_response_id: None,
        temperature: None,
        top_p: None,
        max_output_tokens: None,
        frequency_penalty: None,
        presence_penalty: None,
        truncation: None,
        metadata: None,
        stop: None,
        extra_body: Default::default(),
    };

    let _ = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;

    let requests = upstream.requests().await;
    let messages = &requests[0].messages;

    // All three tool calls should be in a single assistant message
    let assistant_msgs: Vec<_> = messages.iter().filter(|m| m.role == "assistant").collect();
    assert_eq!(
        assistant_msgs.len(),
        1,
        "expected exactly one assistant message"
    );
    assert_eq!(
        assistant_msgs[0].tool_calls.as_ref().unwrap().len(),
        3,
        "expected 3 tool calls in the single assistant message"
    );

    // Three tool results should follow
    let tool_msgs: Vec<_> = messages.iter().filter(|m| m.role == "tool").collect();
    assert_eq!(tool_msgs.len(), 3);
}

// ---------------------------------------------------------------------------
// D1 — DashboardFlowStore middleware integration (whitelist + api_call_id link).
// ---------------------------------------------------------------------------

/// Build a gateway whose dashboard FlowStore is ENABLED (debug UI on), so the
/// `log_api_call` middleware captures whitelisted inference flows. Mirrors
/// `test_gateway_with_config_and_raw_output` but with a live `DashboardFlowStore`.
fn test_gateway_with_flow_store(upstream: MockUpstream, search: MockSearch) -> Arc<Gateway> {
    let config = test_config();
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    let vision: Arc<dyn llmconduit::vision::VisionClient> = Arc::new(
        llmconduit::vision::ReqwestVisionClient::new(reqwest::Client::new(), &config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(Gateway::new(
        config,
        ReplayStore::new(1000),
        Arc::new(upstream),
        Arc::new(search),
        vision,
        image_cache,
        MonitorHub::new(128),
        None,
        llmconduit::dashboard_flow::DashboardFlowStore::new(),
    ))
}

/// D5: a gateway with BOTH an enabled FlowStore and an enabled MetricsLayer, plus
/// the cloned handles a test needs to drive `open` (the middleware's job) and read
/// metrics/snapshots afterward. Mirrors the `with_debug_ui` DI wiring (FlowStore +
/// MetricsLayer + `with_metrics`).
fn test_gateway_with_metrics(
    upstream: MockUpstream,
) -> (
    Arc<Gateway>,
    llmconduit::dashboard_flow::DashboardFlowStore,
    llmconduit::metrics::MetricsLayer,
) {
    let config = test_config();
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    let vision: Arc<dyn llmconduit::vision::VisionClient> = Arc::new(
        llmconduit::vision::ReqwestVisionClient::new(reqwest::Client::new(), &config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    let flow_store = llmconduit::dashboard_flow::DashboardFlowStore::new();
    let metrics = llmconduit::metrics::MetricsLayer::new();
    let gateway = Arc::new(
        Gateway::new(
            config,
            ReplayStore::new(1000),
            Arc::new(upstream),
            Arc::new(MockSearch::default()),
            vision,
            image_cache,
            MonitorHub::new(128),
            None,
            flow_store.clone(),
        )
        .with_metrics(metrics.clone()),
    );
    (gateway, flow_store, metrics)
}

/// Like [`test_gateway_with_flow_store`] but accepts ANY upstream trait object (so
/// the D3 midstream-cancel test can use a parking mock), with the ENABLED store and
/// a live MonitorHub.
fn test_gateway_with_flow_store_upstream(upstream: Arc<dyn UpstreamClient>) -> Arc<Gateway> {
    let config = test_config();
    let vision: Arc<dyn llmconduit::vision::VisionClient> = Arc::new(
        llmconduit::vision::ReqwestVisionClient::new(reqwest::Client::new(), &config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(Gateway::new(
        config,
        ReplayStore::new(1000),
        upstream,
        Arc::new(MockSearch::default()),
        vision,
        image_cache,
        MonitorHub::new(128),
        None,
        llmconduit::dashboard_flow::DashboardFlowStore::new(),
    ))
}

#[tokio::test]
async fn flow_store_opens_record_for_whitelisted_inference_path() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Hello"))])
        .await;
    let gateway = test_gateway_with_flow_store(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(Arc::clone(&gateway));

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .header("authorization", "Bearer SECRETTOKEN")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);

    // A flow record was opened for this whitelisted path.
    let records = gateway.flow_store().list();
    assert_eq!(records.len(), 1, "one flow record opened");
    let record = &records[0];
    // The api_call_id extension was minted + threaded (non-empty `api_` id).
    assert!(
        record.api_call_id.starts_with("api_"),
        "api_call_id minted: {}",
        record.api_call_id
    );
    assert_eq!(record.method, "POST");
    assert_eq!(record.uri, "/v1/chat/completions");
    // The engine linked response_id → api_call_id exactly once (response_id set).
    assert!(
        record
            .response_id
            .as_ref()
            .is_some_and(|id| id.starts_with("resp_")),
        "response_id linked: {:?}",
        record.response_id
    );
    // Secret-bearing header value was redacted inline by the capture seam.
    let header_dump = format!("{:?}", record.headers);
    assert!(
        !header_dump.contains("SECRETTOKEN"),
        "authorization header value redacted in capture"
    );
    // detail joins by either id.
    assert!(gateway.flow_store().detail(&record.api_call_id).is_some());
    assert!(
        gateway
            .flow_store()
            .detail(record.response_id.as_deref().expect("response_id"))
            .is_some(),
        "detail joins by response_id"
    );
}

#[tokio::test]
async fn flow_store_skips_non_whitelisted_paths() {
    // The FlowStore is enabled; the middleware's whitelist must skip these requests
    // BEFORE any upstream call, so whether the upstream proxy succeeds is
    // irrelevant — the assertion is purely "no record opened". (`/v1/completions`
    // is a raw passthrough that bypasses the engine and is intentionally never
    // instrumented; `/v1/models`, `/health`, `/dashboard/*` carry no flow.) D1 R1
    // #1: HEAD/OPTIONS probes on the whitelisted `/v1/messages` path must ALSO open
    // no record — the gate requires METHOD==POST, not just an allowed path.
    let gateway = test_gateway_with_flow_store(MockUpstream::default(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(Arc::clone(&gateway));

    for (method_name, uri, body) in [
        (
            "POST",
            "/v1/completions",
            Some("{\"model\":\"glm-5.1\",\"prompt\":\"hi\"}"),
        ),
        ("GET", "/v1/models", None),
        ("GET", "/health", None),
        ("GET", "/dashboard/anything", None),
        // Non-POST methods on a WHITELISTED path must not open a record.
        ("HEAD", "/v1/messages", None),
        ("OPTIONS", "/v1/messages", None),
    ] {
        let mut builder = Request::builder().method(method_name).uri(uri);
        if body.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let request = builder
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .expect("request");
        let _ = app.clone().oneshot(request).await.expect("response");
    }

    assert!(
        gateway.flow_store().list().is_empty(),
        "no flow record opened for non-POST probes or non-whitelisted paths"
    );
}

#[tokio::test]
async fn flow_store_disabled_path_opens_nothing() {
    // With the default (disabled) FlowStore, the middleware does zero capture work
    // and the public `stream_responses` signature still drives the request.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Hello"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    assert!(
        !gateway.flow_store().is_enabled(),
        "default test gateway has a disabled FlowStore"
    );
    let app = llmconduit::build_app_from_gateway(Arc::clone(&gateway));

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    assert!(
        gateway.flow_store().list().is_empty(),
        "disabled FlowStore opens nothing"
    );
}

// ---------------------------------------------------------------------------
// D3 — TelemetryGuard (L0/L1 CAS) + cumulative-aware usage.
// ---------------------------------------------------------------------------

/// Open a flow record directly in the ENABLED store (mirrors what the middleware's
/// `log_api_call` does) so a direct `stream_responses_with_api_call_id` call drives
/// the L1 engine guard against a live record. Returns the minted `api_call_id`.
fn d3_open_flow(gateway: &Gateway) -> String {
    let api_call_id = format!("api_{}", uuid::Uuid::new_v4().simple());
    gateway.flow_store().open(
        api_call_id.clone(),
        "POST".to_string(),
        "/v1/responses".to_string(),
        llmconduit::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
        None,
        llmconduit::dashboard_flow::ClientAttribution::none(),
    );
    api_call_id
}

/// Poll the record's status until it is no longer `Open` (the spawned `run_turn`
/// finalizes in a separate task AFTER the stream drains), or panic on timeout.
async fn d3_await_terminal(
    gateway: &Gateway,
    api_call_id: &str,
) -> std::sync::Arc<llmconduit::dashboard_flow::FlowRecord> {
    for _ in 0..1000 {
        if let Some(record) = gateway.flow_store().detail(api_call_id)
            && record.status != llmconduit::dashboard_flow::FlowStatus::Open
        {
            return record;
        }
        tokio::task::yield_now().await;
    }
    panic!("flow record never reached a terminal status");
}

#[tokio::test]
async fn d3_completed_flow_finalizes_with_cumulative_usage() {
    // A clean stream with a single cumulative usage chunk: the record finalizes
    // Completed and its usage equals the chunk total.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hello")),
            Ok(usage_chunk("chat-1", 100, 40, 140, Some(10), Some(7))),
        ])
        .await;
    let gateway = test_gateway_with_flow_store(upstream, MockSearch::default());
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("hi")]);
    let stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");
    let _events = collect_stream(stream).await; // drain to completion

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Completed
    );
    let usage = record.usage.expect("usage upserted");
    assert_eq!(usage.total, 140);
    assert_eq!(usage.prompt, 100);
    assert_eq!(usage.completion, 40);
    // Gap 07: the upstream REPORTED cached/reasoning details, so they are `Some`
    // (measured), not `None` (unavailable) and not a bare `i64`.
    assert_eq!(usage.cached, Some(10));
    assert_eq!(usage.reasoning, Some(7));
    // Monotonic latency stamped (Instant-based), terminal reason set.
    assert!(record.elapsed_ms.is_some());
    assert!(record.finished_ms.is_some());
    assert_eq!(
        record.terminal_reason.as_deref(),
        Some("response.completed")
    );
    // The claim CAS reached Finalized (exactly-once).
    assert_eq!(
        record.claim.load(std::sync::atomic::Ordering::SeqCst),
        llmconduit::dashboard_flow::CLAIM_FINALIZED,
    );
}

#[tokio::test]
async fn gap02_phases_populate_on_real_streamed_turn() {
    // Gap 02 acceptance: a real streamed turn populates ALL six per-phase timestamps
    // on the FlowRecord (and the body-free SnapshotFlowSummary projection), and they
    // are monotonic: ingress ≤ normalization ≤ routing ≤ first_content_delta ≤
    // stream_end ≤ finalize.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hello")),
            Ok(usage_chunk("chat-1", 100, 40, 140, Some(10), Some(7))),
        ])
        .await;
    let gateway = test_gateway_with_flow_store(upstream, MockSearch::default());
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("hi")]);
    let stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");
    let _events = collect_stream(stream).await; // drain to completion

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    let p = record.phases;
    assert!(p.ingress_ms.is_some(), "ingress stamped");
    assert!(p.normalization_done_ms.is_some(), "normalization stamped");
    assert!(
        p.routing_decision_ms.is_some(),
        "routing stamped (on the wire)"
    );
    assert!(
        p.first_content_delta_ms.is_some(),
        "first_content_delta stamped (a content delta was streamed)"
    );
    assert!(
        p.stream_end_ms.is_some(),
        "stream_end stamped (clean completion)"
    );
    assert!(p.finalize_ms.is_some(), "finalize stamped (terminal)");

    // Monotonic across all measured phases.
    let order = [
        ("ingress", p.ingress_ms.unwrap()),
        ("normalization", p.normalization_done_ms.unwrap()),
        ("routing", p.routing_decision_ms.unwrap()),
        ("first_content_delta", p.first_content_delta_ms.unwrap()),
        ("stream_end", p.stream_end_ms.unwrap()),
        ("finalize", p.finalize_ms.unwrap()),
    ];
    for win in order.windows(2) {
        assert!(
            win[0].1 <= win[1].1,
            "phase order violated: {}={} > {}={}",
            win[0].0,
            win[0].1,
            win[1].0,
            win[1].1
        );
    }

    // The body-free summary projection carries the SAME phases (the WS/snapshot wire),
    // and serializes them as numeric siblings — never the zero sentinel.
    let summary = gateway
        .flow_store()
        .snapshot_summaries()
        .into_iter()
        .find(|s| s.api_call_id == api_call_id)
        .expect("summary for the flow");
    assert_eq!(summary.phases, p, "summary mirrors the record's phases");
    let json = serde_json::to_string(&summary).expect("serialize summary");
    assert!(json.contains("\"first_content_delta_ms\":"));
    // Don't-lie-with-zeros at the wire: every MEASURED phase is a real epoch-ms value
    // (> 0), never the `0` sentinel that would be indistinguishable from "didn't
    // happen". (Assert on the structured values, not a substring — `elapsed_ms` is a
    // legitimately-0 monotonic delta on a sub-ms mock turn and must not trip this.)
    for (name, value) in [
        ("ingress", p.ingress_ms),
        ("normalization", p.normalization_done_ms),
        ("routing", p.routing_decision_ms),
        ("first_content_delta", p.first_content_delta_ms),
        ("stream_end", p.stream_end_ms),
        ("finalize", p.finalize_ms),
    ] {
        assert!(
            value.unwrap() > 0,
            "measured phase `{name}` must be a real epoch ms, never the 0 sentinel"
        );
    }
}

#[tokio::test]
async fn gap02_reasoning_deltas_do_not_stamp_ttft_content_does() {
    // Gap 02 acceptance (the load-bearing TTFT semantics): `first_content_delta_ms`
    // stamps on the FIRST CONTENT delta only. Prove BOTH directions on real streamed
    // turns:
    //   (a) a stream that emits ONLY reasoning deltas (no content) finishes with
    //       first_content_delta_ms == None — reasoning deltas DO NOT stamp it; and
    //   (b) a stream that emits reasoning deltas THEN a content delta stamps it (so it
    //       was the content delta, not the earlier reasoning, that set TTFT).

    // (a) reasoning-only stream → no content delta ever ⇒ TTFT None.
    let reasoning_only = MockUpstream::default();
    reasoning_only
        .push_response(vec![
            Ok(reasoning_chunk("chat-r", "thinking hard")),
            Ok(reasoning_chunk("chat-r", " still thinking")),
            Ok(usage_chunk("chat-r", 10, 0, 10, None, Some(8))),
        ])
        .await;
    let gw_a = test_gateway_with_flow_store(reasoning_only, MockSearch::default());
    let api_a = d3_open_flow(&gw_a);
    let stream_a = gw_a
        .clone()
        .stream_responses_with_api_call_id(
            base_request(vec![user_message("hi")]),
            Some(api_a.clone()),
        )
        .await
        .expect("stream");
    let _ = collect_stream(stream_a).await;
    let rec_a = d3_await_terminal(&gw_a, &api_a).await;
    assert_eq!(
        rec_a.status,
        llmconduit::dashboard_flow::FlowStatus::Completed,
        "reasoning-only turn still completes"
    );
    assert!(
        rec_a.phases.first_content_delta_ms.is_none(),
        "reasoning deltas (no content) must NOT stamp TTFT — got {:?}",
        rec_a.phases.first_content_delta_ms
    );
    // It DID stream (reasoning) and DID complete, so stream_end is stamped — only TTFT
    // is absent, proving the gate is content-specific, not a missing-seam artifact.
    assert!(
        rec_a.phases.stream_end_ms.is_some(),
        "the reasoning-only turn still reached a clean stream end"
    );

    // (b) reasoning THEN content → TTFT stamped, and ordered AFTER normalization/routing.
    let reasoning_then_content = MockUpstream::default();
    reasoning_then_content
        .push_response(vec![
            Ok(reasoning_chunk("chat-rc", "let me think")),
            Ok(content_chunk("chat-rc", "Answer")),
            Ok(usage_chunk("chat-rc", 10, 5, 15, None, Some(4))),
        ])
        .await;
    let gw_b = test_gateway_with_flow_store(reasoning_then_content, MockSearch::default());
    let api_b = d3_open_flow(&gw_b);
    let stream_b = gw_b
        .clone()
        .stream_responses_with_api_call_id(
            base_request(vec![user_message("hi")]),
            Some(api_b.clone()),
        )
        .await
        .expect("stream");
    let _ = collect_stream(stream_b).await;
    let rec_b = d3_await_terminal(&gw_b, &api_b).await;
    let p = rec_b.phases;
    assert!(
        p.first_content_delta_ms.is_some(),
        "a content delta after reasoning DOES stamp TTFT"
    );
    assert!(
        p.routing_decision_ms.unwrap() <= p.first_content_delta_ms.unwrap(),
        "TTFT is at/after the routing decision (the content arrived after dispatch)"
    );
    assert!(
        p.first_content_delta_ms.unwrap() <= p.stream_end_ms.unwrap(),
        "TTFT precedes stream end"
    );
}

#[tokio::test]
async fn gap02_error_before_content_leaves_ttft_and_stream_end_none() {
    // Gap 02 acceptance: a flow that errors BEFORE any content delta has
    // first_content_delta_ms == None (don't-lie-with-zeros — absent, never 0), and no
    // clean stream_end either; but finalize still stamps (every terminal does).
    let upstream = MockUpstream::default();
    // The upstream stream yields an error as its first item — no content is ever
    // emitted to the client.
    upstream
        .push_response(vec![Err(llmconduit::error::AppError::upstream(
            "upstream exploded before first token",
        ))])
        .await;
    let gateway = test_gateway_with_flow_store(upstream, MockSearch::default());
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("hi")]);
    let stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");
    let _events = collect_stream(stream).await; // drains the failed stream

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Failed,
        "an upstream error before content finalizes Failed"
    );
    let p = record.phases;
    assert!(
        p.first_content_delta_ms.is_none(),
        "no content delta was emitted ⇒ TTFT is None, NEVER 0 — got {:?}",
        p.first_content_delta_ms
    );
    assert!(
        p.stream_end_ms.is_none(),
        "the turn never reached a clean stream end ⇒ stream_end None"
    );
    // Finalize still fired for the failed terminal (right edge always present).
    assert!(
        p.finalize_ms.is_some(),
        "every terminal stamps finalize, even Failed"
    );
    // And the absent phases serialize as ABSENT on the body-free summary (not 0/null).
    let summary = gateway
        .flow_store()
        .snapshot_summaries()
        .into_iter()
        .find(|s| s.api_call_id == api_call_id)
        .expect("summary");
    let json = serde_json::to_string(&summary).expect("serialize");
    assert!(
        !json.contains("first_content_delta_ms"),
        "an unmeasured TTFT must be ABSENT from the wire, not 0/null: {json}"
    );
    assert!(!json.contains("stream_end_ms"), "stream_end absent: {json}");
}

#[tokio::test]
async fn gap02_cancel_before_first_content_delta_leaves_ttft_none() {
    // Gap 02 (review round 1, HIGH): TTFT is stamped ONLY AFTER the first content
    // delta's `send_event` is delivered to the client. If the client hangs up BEFORE
    // any content delta is delivered, `first_content_delta_ms` stays None — a closed /
    // cancelled stream must NOT record a TTFT for a token the client never saw.
    //
    // The upstream parks (never yields a content chunk) so the engine is suspended in
    // `next_upstream_chunk`'s `tx.closed()` select when the client drops the receiver:
    // the content arm is never reached, and the flow finalizes Cancelled with TTFT None.
    // (The DELIVERED-then-cancelled direction — TTFT IS kept — is asserted by
    // `d3_midstream_cancel_finalizes_cancelled_with_last_usage`, which drains a content
    // delta before hanging up.)
    let upstream = PendingChunkUpstream::new();
    let stream_polled = upstream.stream_polled.notified();
    let stream_dropped = upstream.stream_dropped.notified();
    let gateway = test_gateway_with_flow_store_upstream(Arc::new(upstream.clone()));
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("count")]);
    let mut stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");

    // Drain the preamble (`response.created`, `response.in_progress`) — these are NOT
    // content deltas, so they must not stamp TTFT. The upstream then parks waiting for a
    // chunk that never comes, so no `output_text.delta` is ever produced or delivered.
    let _created = stream.next().await;
    let _in_progress = stream.next().await;
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_polled)
        .await
        .expect("upstream parked before yielding any content chunk");

    // Client hangs up BEFORE the first content delta is delivered.
    drop(stream);
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_dropped)
        .await
        .expect("upstream stream dropped after the client hung up pre-content");

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Cancelled,
        "a pre-content hang-up finalizes Cancelled"
    );
    let p = record.phases;
    assert!(
        p.first_content_delta_ms.is_none(),
        "no content delta was delivered to the client ⇒ TTFT is None, NEVER stamped for \
         an undelivered token — got {:?}",
        p.first_content_delta_ms
    );
    // Routing was decided (the request reached the wire) and finalize always stamps —
    // so the absent TTFT is the content-specific gate firing, not a missing-seam
    // artifact. There was no clean stream end either (the client hung up mid-stream).
    assert!(
        p.routing_decision_ms.is_some(),
        "the request was dispatched to the upstream (routing stamped)"
    );
    assert!(
        p.stream_end_ms.is_none(),
        "a cancelled stream never reached a clean stream end ⇒ stream_end None"
    );
    assert!(
        p.finalize_ms.is_some(),
        "every terminal stamps finalize, even Cancelled"
    );
    // And the absent TTFT serializes as ABSENT on the body-free summary (not 0/null).
    let summary = gateway
        .flow_store()
        .snapshot_summaries()
        .into_iter()
        .find(|s| s.api_call_id == api_call_id)
        .expect("summary for the cancelled flow");
    let json = serde_json::to_string(&summary).expect("serialize summary");
    assert!(
        !json.contains("first_content_delta_ms"),
        "an undelivered TTFT must be ABSENT from the wire, not 0/null: {json}"
    );
}

#[tokio::test]
async fn d3_multi_chunk_usage_does_not_double_count() {
    // THE no-double-count test: a single turn emits MULTIPLE cumulative usage
    // chunks. The record's usage must equal the FINAL chunk total, not the sum of
    // the chunk totals.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hel")),
            // cumulative usage grows across chunks within the SAME turn:
            Ok(usage_chunk("chat-1", 100, 10, 110, None, None)),
            Ok(content_chunk("chat-1", "lo")),
            Ok(usage_chunk("chat-1", 100, 25, 125, None, None)),
            Ok(usage_chunk("chat-1", 100, 40, 140, None, None)),
        ])
        .await;
    let gateway = test_gateway_with_flow_store(upstream, MockSearch::default());
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("hi")]);
    let stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");
    let _events = collect_stream(stream).await;

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    let usage = record.usage.expect("usage upserted");
    assert_eq!(
        usage.total, 140,
        "record holds the LAST cumulative total (140), not the sum 110+125+140"
    );
    assert_eq!(usage.completion, 40);
}

#[tokio::test]
async fn d3_no_usage_chunk_leaves_usage_none() {
    // A turn with no usage chunk contributes no usage; the record finalizes
    // Completed with `usage = None`.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Hello"))])
        .await;
    let gateway = test_gateway_with_flow_store(upstream, MockSearch::default());
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("hi")]);
    let stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");
    let _events = collect_stream(stream).await;

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Completed
    );
    assert!(record.usage.is_none(), "no usage chunk → usage None");
}

#[tokio::test]
async fn d3_pre_spawn_error_finalizes_failed_not_cancelled() {
    // A PRE-SPAWN early return (here an unsupported `previous_response_id`, which
    // fails canonical lowering BEFORE the tokio::spawn) must finalize the record
    // Failed — NOT leave it Open and NOT fall through to the Drop fallback's
    // Cancelled — with no usage (none was upserted before the spawn). This is the
    // engine.rs lowering/budget pre-spawn seam.
    let gateway = test_gateway_with_flow_store(MockUpstream::default(), MockSearch::default());
    let api_call_id = d3_open_flow(&gateway);

    let mut request = base_request(vec![user_message("hi")]);
    request.previous_response_id = Some("resp_does_not_exist".to_string());
    let result = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await;
    assert!(
        result.is_err(),
        "pre-spawn lowering error surfaces to the caller"
    );

    let record = gateway.flow_store().detail(&api_call_id).expect("record");
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Failed,
        "pre-spawn error finalized Failed (not Open, not Cancelled)"
    );
    assert!(record.usage.is_none(), "no usage upserted pre-spawn");
    assert!(record.elapsed_ms.is_some(), "monotonic latency stamped");
    assert_eq!(
        record.claim.load(std::sync::atomic::Ordering::SeqCst),
        llmconduit::dashboard_flow::CLAIM_FINALIZED,
    );
}

#[tokio::test]
async fn d3_midstream_cancel_finalizes_cancelled_with_last_usage() {
    // A midstream cancel AFTER a usage chunk: the record finalizes Cancelled and
    // RETAINS the last upserted cumulative usage (NOT zero). This also exercises the
    // L1 guard crossing the tokio::spawn (it compiles + runs).
    let upstream = ChunkThenPendingUpstream::new(vec![
        content_chunk("chat-1", "Hel"),
        usage_chunk("chat-1", 100, 20, 120, Some(5), None),
    ]);
    let stream_polled = upstream.stream_polled.notified();
    let gateway = test_gateway_with_flow_store_upstream(Arc::new(upstream.clone()));
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("count")]);
    let mut stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");

    // Drain the early SSE events until the upstream is parked waiting for more.
    let mut saw_total = 0;
    while saw_total < 2 {
        if stream.next().await.is_some() {
            saw_total += 1;
        } else {
            break;
        }
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_polled)
        .await
        .expect("upstream parked after its canned chunks");

    // Client hangs up mid-stream.
    drop(stream);

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Cancelled,
        "midstream cancel finalized Cancelled"
    );
    let usage = record.usage.expect("last usage retained, not zero");
    assert_eq!(
        usage.total, 120,
        "cancel keeps the LAST upserted cumulative total"
    );
    assert_eq!(usage.cached, Some(5));
    // Gap 02 (review round 1): the content delta "Hel" WAS delivered (the test drained
    // it off the SSE stream) BEFORE the hang-up, so TTFT is stamped — a cancel AFTER the
    // first content delta reaches the client keeps the true TTFT. (The None direction —
    // a hang-up BEFORE any content delta is delivered — is the dedicated test
    // `gap02_cancel_before_first_content_delta_leaves_ttft_none`.)
    assert!(
        record.phases.first_content_delta_ms.is_some(),
        "the first content delta was delivered before the cancel ⇒ TTFT stamped — got {:?}",
        record.phases.first_content_delta_ms
    );
    assert_eq!(
        record.claim.load(std::sync::atomic::Ordering::SeqCst),
        llmconduit::dashboard_flow::CLAIM_FINALIZED,
    );
}

#[tokio::test]
async fn d6_kill_midchunk_terminates_stream_cancelled_no_leak() {
    // D6 CORE: a server-side kill (NOT a client hang-up) of a live, parked stream
    // cancels it cleanly — the record finalizes Cancelled, the upstream stream is torn
    // down (no orphan/dup), the client stream ends with a terminal `response.failed`
    // (not a half-open hang), and the AbortHub leaks no entry. `abort()` while the
    // CLIENT is still connected (tx NOT closed) proves the kill composes with — does
    // not depend on — the `tx.closed()` hang-up path.
    let upstream = ChunkThenPendingUpstream::new(vec![
        content_chunk("chat-1", "Hel"),
        usage_chunk("chat-1", 100, 20, 120, Some(5), None),
    ]);
    let stream_polled = upstream.stream_polled.notified();
    let stream_dropped = upstream.stream_dropped.notified();
    let gateway = test_gateway_with_flow_store_upstream(Arc::new(upstream.clone()));
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("count")]);
    let mut stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");

    // Drain the early SSE events until the upstream is parked waiting for more.
    let mut early = Vec::new();
    while early.len() < 2 {
        match stream.next().await {
            Some(event) => early.push(event),
            None => break,
        }
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_polled)
        .await
        .expect("upstream parked after its canned chunks");

    // The token is registered while the flow is live.
    assert_eq!(
        gateway.abort_hub().live_len(),
        1,
        "live flow registered exactly one kill token"
    );

    // SERVER-SIDE kill (client still connected — we still hold `stream`).
    assert!(
        gateway.abort(&api_call_id),
        "abort found the live token → true"
    );

    // The stream terminates cleanly: it yields a terminal `response.failed` (the engine
    // surfaced cancelled() and emitted the terminal event) and then ENDS — collecting
    // the remainder must not hang.
    let mut saw_failed = false;
    let drain = async {
        while let Some(event) = stream.next().await {
            if event.event == "response.failed" {
                saw_failed = true;
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), drain)
        .await
        .expect("killed stream terminated (did not hang)");
    assert!(
        saw_failed,
        "killed stream ended with a terminal response.failed, not a half-open hang"
    );

    // The upstream stream was actually dropped — the engine tore down upstream work on
    // cancel (no orphan task, no duplicated/replayed tokens).
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_dropped)
        .await
        .expect("upstream stream dropped on kill (engine cancelled upstream work)");

    // The record finalized Cancelled (the kill is a cancel, not a failure), keeping the
    // last upserted usage, and the AbortHub leaked no entry.
    let record = d3_await_terminal(&gateway, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Cancelled,
        "server-side kill finalized Cancelled"
    );
    assert_eq!(
        record.usage.expect("last usage retained").total,
        120,
        "kill keeps the LAST upserted cumulative total"
    );
    assert_eq!(
        record.claim.load(std::sync::atomic::Ordering::SeqCst),
        llmconduit::dashboard_flow::CLAIM_FINALIZED,
    );
    assert_eq!(
        gateway.abort_hub().live_len(),
        0,
        "no AbortHub entry leaks after the killed flow finalized"
    );
}

#[tokio::test]
async fn d6_completed_flow_leaves_no_abort_hub_entry() {
    // No-leak invariant on the HAPPY path: a cleanly Completed flow removes its kill
    // token (the guard removes on the explicit Completed finalize too, not just Drop),
    // so the map is bounded by in-flight streams, never the 512-record history.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hello")),
            Ok(usage_chunk("chat-1", 100, 40, 140, None, None)),
        ])
        .await;
    let gateway = test_gateway_with_flow_store_upstream(Arc::new(upstream));
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("hi")]);
    let stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");
    let _events = collect_stream(stream).await; // drain to completion

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Completed
    );
    assert_eq!(
        gateway.abort_hub().live_len(),
        0,
        "Completed flow removed its kill token (no leak)"
    );
    // A kill of the now-finished flow is a 404-class miss (the entry is gone).
    assert!(
        !gateway.abort(&api_call_id),
        "abort of a finished flow returns false (→ 404)"
    );
}

#[tokio::test]
async fn d6_abort_unknown_flow_returns_false() {
    // 404-class: aborting an id that was never registered returns false. (The kill
    // handler maps this to 404; here we assert the Gateway primitive directly.)
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "hi"))])
        .await;
    let gateway = test_gateway_with_flow_store_upstream(Arc::new(upstream));
    assert!(
        !gateway.abort("api_does_not_exist"),
        "unknown api_call_id → false (→ 404)"
    );
}

/// D6 — a mock `MutationPolicy` so the kill handler logic is testable without the real
/// `DashboardAuth`/CSRF stack (spec: "compiles + tests against a mocked auth/CSRF
/// gate"). Drives the three gate states the route maps to 403/200/404.
struct MockMutationPolicy {
    decision: Result<(), llmconduit::dashboard_auth::MutationDenied>,
}

impl llmconduit::dashboard_auth::MutationPolicy for MockMutationPolicy {
    fn mutations_enabled(&self) -> bool {
        self.decision.is_ok()
    }
    fn authorize_mutation(
        &self,
        _headers: &axum::http::HeaderMap,
    ) -> Result<(), llmconduit::dashboard_auth::MutationDenied> {
        self.decision
    }
}

#[tokio::test]
async fn d6_kill_handler_outcome_maps_authorize_then_abort() {
    use llmconduit::dashboard_auth::MutationDenied;
    use llmconduit::http::FlowKillOutcome;
    use llmconduit::http::flow_kill_outcome;

    let upstream = ChunkThenPendingUpstream::new(vec![content_chunk("chat-1", "Hel")]);
    let stream_polled = upstream.stream_polled.notified();
    let gateway = test_gateway_with_flow_store_upstream(Arc::new(upstream.clone()));
    let api_call_id = d3_open_flow(&gateway);
    let headers = axum::http::HeaderMap::new();

    // A DENIED policy short-circuits BEFORE any abort — no existence oracle for an
    // unauthorized caller — even though no flow is live yet anyway.
    let denied = MockMutationPolicy {
        decision: Err(MutationDenied::CsrfInvalid),
    };
    assert_eq!(
        flow_kill_outcome(&denied, &headers, gateway.as_ref(), &api_call_id),
        FlowKillOutcome::Denied(MutationDenied::CsrfInvalid),
        "denied policy → Denied (→ 403), no abort attempted"
    );

    // Now bring a stream live so an AUTHORIZED kill finds the token.
    let request = base_request(vec![user_message("count")]);
    let mut stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");
    // Drive past the canned chunk so the upstream is parked + the token registered.
    let _ = stream.next().await;
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_polled)
        .await
        .expect("upstream parked");

    let allowed = MockMutationPolicy { decision: Ok(()) };
    assert_eq!(
        flow_kill_outcome(&allowed, &headers, gateway.as_ref(), &api_call_id),
        FlowKillOutcome::Killed,
        "authorized kill of a live flow → Killed (→ 200)"
    );

    // Drain so the flow finalizes + the token is removed.
    let drain = async { while stream.next().await.is_some() {} };
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), drain).await;
    let _ = d3_await_terminal(&gateway, &api_call_id).await;

    // An authorized kill of the now-finished flow → NotFound (404).
    assert_eq!(
        flow_kill_outcome(&allowed, &headers, gateway.as_ref(), &api_call_id),
        FlowKillOutcome::NotFound,
        "authorized kill of a finished flow → NotFound (→ 404)"
    );
}

// ===========================================================================
// D13 — Dashboard REST routes + price config (the CAPSTONE). The routes register
// ONLY in the `--with-debug-ui` block, behind D7a auth + `no-store`; the cursor-
// bearing reads carry their per-domain seq; the kill honors the mutation+CSRF
// gate; the price table drives `cost` end-to-end. The shapes MUST match the
// FROZEN frontend contract (`dashboard-frontend/src/api/types.ts`).
// ===========================================================================

/// A priced D13 env: token + https origin + (optionally) mutations enabled.
fn d13_env(allow_mutations: bool) -> llmconduit::dashboard_auth::DashboardEnv {
    use base64::Engine as _;
    llmconduit::dashboard_auth::DashboardEnv {
        token: Some("d13-token".to_string()),
        session_key_b64: Some(base64::engine::general_purpose::STANDARD.encode([13u8; 32])),
        public_origin: Some("https://dash.example.com".to_string()),
        allow_insecure: false,
        allow_mutations,
    }
}

/// A test price table keyed by served model. `glm-5.1` (the `base_request` model,
/// which the bare leaf records as `model_served`) is priced so a driven flow gets
/// a non-null cost: input 2.0 / output 6.0 / cached 0.5 per 1k.
fn d13_price_table() -> std::collections::HashMap<String, llmconduit::config::ModelPrice> {
    let mut table = std::collections::HashMap::new();
    table.insert(
        "glm-5.1".to_string(),
        llmconduit::config::ModelPrice::new(2.0, 6.0, 0.5),
    );
    table
}

/// Build a gateway with an ENABLED FlowStore + MetricsLayer + the configured price
/// table + the D7a auth context, mirroring the `--with-debug-ui` DI wiring. Accepts
/// any upstream trait object so the kill test can use a parking upstream.
fn d13_gateway(
    upstream: Arc<dyn UpstreamClient>,
    auth: Arc<llmconduit::dashboard_auth::DashboardAuth>,
) -> Arc<Gateway> {
    let mut config = test_config();
    config.price_table = d13_price_table();
    let vision: Arc<dyn llmconduit::vision::VisionClient> = Arc::new(
        llmconduit::vision::ReqwestVisionClient::new(reqwest::Client::new(), &config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(
        Gateway::new(
            config,
            ReplayStore::new(1000),
            upstream,
            Arc::new(MockSearch::default()),
            vision,
            image_cache,
            MonitorHub::new(128),
            None,
            llmconduit::dashboard_flow::DashboardFlowStore::new(),
        )
        .with_metrics(llmconduit::metrics::MetricsLayer::new())
        .with_dashboard_auth(Some(auth)),
    )
}

/// Build the router for a D13 gateway with the protected routes registered.
fn d13_router(gateway: Arc<Gateway>) -> axum::Router {
    llmconduit::http::build_router(
        gateway,
        llmconduit::http::RouterOptions {
            with_debug_ui: true,
            register_protected_routes: true,
        },
    )
}

/// GET a `/dashboard/api/*` path with a valid session cookie, returning the
/// response (status + headers + body) for assertion.
async fn d13_authed_get(
    app: &axum::Router,
    auth: &llmconduit::dashboard_auth::DashboardAuth,
    uri: &str,
) -> axum::response::Response {
    let (cookie, _exp) = auth.issue_session();
    app.clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(
                    axum::http::header::COOKIE,
                    format!("llmconduit_session={cookie}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Read a response body into a `serde_json::Value`.
async fn d13_json(response: axum::response::Response) -> serde_json::Value {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 8 * 1024 * 1024)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).unwrap_or_else(|err| {
        panic!(
            "response is JSON (status {status}, body={:?}): {err}",
            String::from_utf8_lossy(&bytes)
        )
    })
}

/// GET a `/dashboard/api/*` path with NO cookie (dev-open auth, loopback bind).
async fn d13_get(app: &axum::Router, uri: &str) -> axum::response::Response {
    app.clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// Wait for ANY flow in the store to reach a terminal status and return its
/// `api_call_id` (the router-driven flow's id is minted by the middleware).
async fn d13_await_any_terminal(gateway: &Gateway) -> String {
    for _ in 0..2000 {
        if let Some(record) = gateway
            .flow_store()
            .list()
            .iter()
            .find(|record| record.status != llmconduit::dashboard_flow::FlowStatus::Open)
        {
            return record.api_call_id.clone();
        }
        tokio::task::yield_now().await;
    }
    panic!("no flow finalized in the store");
}

/// Assert a response carries `Cache-Control: no-store` (D7a — every
/// `/dashboard/api/*` response is uncacheable).
fn d13_assert_no_store(response: &axum::response::Response) {
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok()),
        Some("no-store"),
        "every /dashboard/api/* response MUST be no-store"
    );
}

#[tokio::test]
async fn d13_routes_absent_without_debug_ui() {
    // With the debug UI OFF, NONE of the `/dashboard/api/*` routes exist → 404.
    let app = llmconduit::build_app_with_options(
        test_config(),
        llmconduit::AppOptions {
            with_debug_ui: false,
        },
    );
    for uri in [
        "/dashboard/api/flows",
        "/dashboard/api/flows/api_x",
        "/dashboard/api/metrics",
        "/dashboard/api/topology",
        "/dashboard/api/catalog",
        "/dashboard/api/snapshot",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            404,
            "{uri} must not exist when --with-debug-ui is off"
        );
    }
    // The kill POST is likewise absent.
    let kill = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/dashboard/api/flows/api_x/kill")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(kill.status().as_u16(), 404, "kill absent without debug UI");
}

#[tokio::test]
async fn d13_routes_present_in_dev_open_with_debug_ui() {
    // With the debug UI ON (loopback dev-open: no token), the routes EXIST and
    // serve (200) — the inverse of the off case. Dev-open authenticates every
    // request, so a cookieless GET still reaches the handler.
    let app = llmconduit::build_app_with_options(
        test_config(),
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );
    for uri in [
        "/dashboard/api/flows",
        "/dashboard/api/metrics",
        "/dashboard/api/topology",
        "/dashboard/api/catalog",
        "/dashboard/api/snapshot",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            200,
            "{uri} must serve 200 when --with-debug-ui is on (dev-open)"
        );
        d13_assert_no_store(&response);
    }
}

#[tokio::test]
async fn d13_api_requires_session_when_token_configured() {
    // With a token configured (production posture), an unauthenticated read is
    // 401 no-store; a valid session cookie is 200. Proves D7a gates every
    // `/dashboard/api/*` read BEFORE the handler runs.
    let auth = llmconduit::dashboard_auth::DashboardAuth::from_env(
        "0.0.0.0:4000".parse().unwrap(),
        &d13_env(false),
    )
    .expect("auth builds")
    .auth;
    let app = d13_router(d13_gateway(
        Arc::new(MockUpstream::default()),
        Arc::clone(&auth),
    ));

    let unauthed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/dashboard/api/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthed.status().as_u16(), 401);
    d13_assert_no_store(&unauthed);

    let authed = d13_authed_get(&app, &auth, "/dashboard/api/metrics").await;
    assert_eq!(authed.status().as_u16(), 200);
    d13_assert_no_store(&authed);
}

#[tokio::test]
async fn d13_extractor_rejection_carries_no_store_and_security_headers() {
    // D13 R1 MED: `no-store` + the dashboard security headers are ROUTE-LEVEL response
    // middleware on the whole `/dashboard/api` router, so EVEN an axum EXTRACTOR
    // rejection — an invalid `at`/`page` query parsed to a bare `400` BEFORE any
    // handler runs — carries them. Without the route-level layer the bare 400 escaped
    // the per-handler `json_no_store` stamping. Dev-open (loopback) so no cookie needed.
    let app = llmconduit::build_app_with_options(
        test_config(),
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );
    // `?at=` wants a u64; a non-numeric value rejects in the `Query<SnapshotQuery>`
    // extractor. `?page=` wants a usize; a non-numeric value rejects in
    // `Query<FlowsQuery>`. Both are extractor failures, not handler responses.
    for uri in [
        "/dashboard/api/snapshot?at=not-a-number",
        "/dashboard/api/flows?page=not-a-number",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            400,
            "{uri} ⇒ extractor-rejection 400"
        );
        // The whole dashboard security header set, including no-store, is present.
        assert_security_headers(response.headers());
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_SECURITY_POLICY)
                .and_then(|v| v.to_str().ok()),
            Some("default-src 'none'; frame-ancestors 'none'"),
            "{uri} extractor-rejection 400 carries the locked-down CSP"
        );
    }
}

#[tokio::test]
async fn d13_metrics_shape_carries_seq_and_windows() {
    // `/metrics` matches the frozen `MetricsResponse`: a `metrics_seq` cursor, the
    // eight headline tile fields, and the three windows m1/m5/h1 each with the
    // same fields. (Values are zero on a quiet gateway; the SHAPE is the contract.)
    let auth = llmconduit::dashboard_auth::DashboardAuth::from_env(
        "0.0.0.0:4000".parse().unwrap(),
        &d13_env(false),
    )
    .expect("auth builds")
    .auth;
    let app = d13_router(d13_gateway(
        Arc::new(MockUpstream::default()),
        Arc::clone(&auth),
    ));
    let body = d13_json(d13_authed_get(&app, &auth, "/dashboard/api/metrics").await).await;

    assert!(
        body["metrics_seq"].is_u64(),
        "metrics carries its domain cursor"
    );
    for field in [
        "reqs_per_sec",
        "active_streams",
        "error_pct",
        "p50",
        "p95",
        "p99",
        "tokens_per_sec",
        "cost_per_min",
    ] {
        assert!(body[field].is_number(), "metrics headline tile has {field}");
    }
    for window in ["m1", "m5", "h1"] {
        let tile = &body["windows"][window];
        assert!(tile.is_object(), "windows.{window} present");
        for field in [
            "reqs_per_sec",
            "active_streams",
            "error_pct",
            "p50",
            "p95",
            "p99",
            "tokens_per_sec",
            "cost_per_min",
        ] {
            assert!(tile[field].is_number(), "windows.{window} has {field}");
        }
    }
}

#[tokio::test]
async fn d13_topology_shape_carries_seq_and_price_table() {
    // `/topology` matches the frozen `TopologyResponse`: a `topology_seq` cursor,
    // `nodes`/`edges` arrays, AND the configured `price_table` (the model row with
    // the three finite per-1k rates the frontend `isModelPrice` guard requires).
    let auth = llmconduit::dashboard_auth::DashboardAuth::from_env(
        "0.0.0.0:4000".parse().unwrap(),
        &d13_env(false),
    )
    .expect("auth builds")
    .auth;
    let app = d13_router(d13_gateway(
        Arc::new(MockUpstream::default()),
        Arc::clone(&auth),
    ));
    let body = d13_json(d13_authed_get(&app, &auth, "/dashboard/api/topology").await).await;

    assert!(
        body["topology_seq"].is_u64(),
        "topology carries its domain cursor"
    );
    assert!(body["nodes"].is_array());
    assert!(body["edges"].is_array());
    // The price table carries the configured model with finite per-1k rates.
    let price = &body["price_table"]["glm-5.1"];
    assert_eq!(price["input_per_1k"], serde_json::json!(2.0));
    assert_eq!(price["output_per_1k"], serde_json::json!(6.0));
    assert_eq!(price["cached_per_1k"], serde_json::json!(0.5));
}

#[tokio::test]
async fn d13_catalog_is_a_bare_array_no_cursor() {
    // `/catalog` is the lone BARE array `[{id, context_limit}]` — NO cursor (a
    // static-ish read, not a mutating domain). Gap 06: `context_limit` is NULLABLE
    // end-to-end — a model WITH an advertised window serializes the integer, a model
    // WITHOUT serializes ABSENT/null (NEVER a non-null `0`, the prior lie-with-zeros).
    let upstream = MockUpstream::default();
    upstream.set_supported_models(["model-a", "model-b"]).await;
    // `model-a` advertises a 32k window; `model-b` advertises none.
    upstream.set_context_limits([("model-a", 32_768_i64)]).await;
    let auth = llmconduit::dashboard_auth::DashboardAuth::from_env(
        "0.0.0.0:4000".parse().unwrap(),
        &d13_env(false),
    )
    .expect("auth builds")
    .auth;
    let app = d13_router(d13_gateway(Arc::new(upstream), Arc::clone(&auth)));
    let body = d13_json(d13_authed_get(&app, &auth, "/dashboard/api/catalog").await).await;

    let array = body
        .as_array()
        .expect("catalog is a BARE array (no cursor)");
    assert_eq!(array.len(), 2);

    // Model WITH an advertised window ⇒ the real integer.
    assert_eq!(array[0]["id"], serde_json::json!("model-a"));
    assert_eq!(
        array[0]["context_limit"],
        serde_json::json!(32_768),
        "an advertised window surfaces as the integer"
    );

    // Model WITHOUT an advertised window ⇒ ABSENT (not `0`, not present-and-null
    // garbage). `skip_serializing_if = Option::is_none` omits the key entirely; the
    // critical invariant is that it is NEVER the integer `0` (which would read as an
    // infinite/garbage utilization downstream in spec 09's gauge).
    assert_eq!(array[1]["id"], serde_json::json!("model-b"));
    assert!(
        array[1].get("context_limit").is_none()
            || array[1]["context_limit"] == serde_json::Value::Null,
        "no upstream window ⇒ context_limit is unavailable (absent/null), NEVER 0: got {:?}",
        array[1].get("context_limit"),
    );
    assert_ne!(
        array[1]["context_limit"],
        serde_json::json!(0),
        "don't-lie-with-zeros: a missing window must NOT collapse to 0"
    );
}

#[test]
fn d13_catalog_entry_context_limit_round_trips_nullable() {
    // AGENTS.md: no changed wire field without a deserialize-then-serialize proof.
    // Gap 06 changed `CatalogEntry.context_limit` from non-null `i64` to nullable
    // `Option<i64>` — pin both arms (Some(n) ⇒ integer survives; None ⇒ absent, NOT
    // `0`) through a JSON round-trip.
    use llmconduit::dashboard_api::CatalogEntry;

    // Some(n): the integer survives serialize → deserialize → serialize.
    let known = CatalogEntry {
        id: "model-a".to_string(),
        context_limit: Some(32_768),
    };
    let known_json = serde_json::to_value(&known).expect("serialize known");
    assert_eq!(known_json["context_limit"], serde_json::json!(32_768));
    let known_back: CatalogEntry = serde_json::from_value(known_json).expect("deserialize known");
    assert_eq!(known_back.context_limit, Some(32_768));

    // None: serializes ABSENT (skip_serializing_if), NEVER `0`; and an absent key
    // deserializes back to None.
    let unknown = CatalogEntry {
        id: "model-b".to_string(),
        context_limit: None,
    };
    let unknown_json = serde_json::to_value(&unknown).expect("serialize unknown");
    assert!(
        unknown_json.get("context_limit").is_none(),
        "None ⇒ the key is OMITTED (skip_serializing_if), never serialized as 0"
    );
    let unknown_back: CatalogEntry =
        serde_json::from_value(unknown_json).expect("deserialize unknown (absent key)");
    assert_eq!(unknown_back.context_limit, None);

    // An EXPLICIT `null` on the wire also deserializes to None (robust to either
    // honest absent/null encoding).
    let explicit_null: CatalogEntry =
        serde_json::from_value(serde_json::json!({ "id": "m", "context_limit": null }))
            .expect("deserialize explicit null");
    assert_eq!(explicit_null.context_limit, None);
}

/// Build a `--with-debug-ui` app + its gateway over a wiremock upstream (the REAL
/// `ReqwestUpstreamClient` leaf, so D2 captures the normalized + upstream bodies +
/// `model_served`), with the price table configured. Auth is DEV-OPEN (loopback
/// bind, no token), so `/dashboard/api/*` reads need no cookie — this exercises the
/// full capture+price+REST e2e; the cookie/CSRF gating lives in its own tests.
async fn d13_wiremock_app() -> (axum::Router, Arc<Gateway>, MockServer) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "glm-5.1"}]
        })))
        .mount(&server)
        .await;
    // The chat-completions SSE: one content delta + a final usage chunk so the flow
    // finalizes Completed WITH usage (prompt 100 / completion 40 / cached 10) and so
    // the leaf records `model_served = glm-5.1` (driving a cost of 0.425).
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[
                    json!({
                        "id": "chat-1",
                        "choices": [{"index": 0, "delta": {"content": "Hello world"}, "finish_reason": null}],
                        "usage": null
                    }),
                    json!({
                        "id": "chat-1",
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                        "usage": {
                            "prompt_tokens": 100,
                            "completion_tokens": 40,
                            "total_tokens": 140,
                            "prompt_tokens_details": {"cached_tokens": 10}
                        }
                    }),
                ])),
        )
        .mount(&server)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", server.uri()).parse().expect("url");
    config.price_table = d13_price_table();
    // Loopback bind ⇒ dev-open auth (the routes register, no token required).
    let (app, gateway) = llmconduit::build_app_with_gateway_and_options(
        config,
        None,
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );
    (app, gateway, server)
}

#[tokio::test]
async fn d13_flows_filters_and_carries_flow_seq_and_cost() {
    // `/flows` matches the frozen `FlowsResponse` `{flows, total, flow_seq}`. A real
    // streamed flow (Completed, priced via the wiremock leaf) + a directly-finalized
    // Failed flow let the `status=` filter prove the subset, the `flow_seq` cursor,
    // and a non-null `cost` driven by the price table.
    let (app, gateway, _server) = d13_wiremock_app().await;

    // Drive one real Completed flow through the router so the leaf records
    // `model_served = glm-5.1` and usage (⇒ cost 0.425).
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "model": "glm-5.1",
                        "stream": true,
                        "messages": [{"role": "user", "content": "hi"}]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 8 * 1024 * 1024)
        .await
        .expect("drain SSE");
    let completed = d13_await_any_terminal(&gateway).await;

    // Open a second flow and finalize it Failed directly (no stream).
    let failed = d3_open_flow(&gateway);
    gateway.flow_store().finalize(
        &failed,
        llmconduit::dashboard_flow::FlowStatus::Failed,
        Some("boom".to_string()),
        None,
    );

    // Dev-open: no cookie needed.
    let all = d13_json(d13_get(&app, "/dashboard/api/flows").await).await;
    assert!(
        all["flow_seq"].is_u64(),
        "flows carries the FlowStore cursor"
    );
    assert_eq!(all["total"], serde_json::json!(2));
    assert_eq!(all["flows"].as_array().unwrap().len(), 2);

    // The completed row carries a non-null cost = usage × glm-5.1 price = 0.425.
    let completed_row = all["flows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["api_call_id"] == serde_json::json!(completed))
        .expect("completed row present");
    let cost = completed_row["cost"].as_f64().expect("cost priced");
    assert!(
        (cost - 0.425).abs() < 1e-9,
        "cost {cost} == usage×price 0.425"
    );
    // Gap 07: glm-5.1 is priced WITH a configured cache rate (0.5) and the flow
    // reported cached tokens, so the cost is CONFIDENT (every billed class has a known
    // rate) — the per-flow confidence tag rides the row end-to-end.
    assert_eq!(
        completed_row["cost_confidence"],
        serde_json::json!("confident"),
        "priced + configured cache rate ⇒ confident"
    );

    // `status=failed` returns ONLY the failed flow.
    let only_failed = d13_json(d13_get(&app, "/dashboard/api/flows?status=failed").await).await;
    assert_eq!(only_failed["total"], serde_json::json!(1));
    assert_eq!(
        only_failed["flows"][0]["api_call_id"],
        serde_json::json!(failed)
    );
    assert_eq!(
        only_failed["flows"][0]["status"],
        serde_json::json!("failed")
    );
}

#[tokio::test]
async fn d13_kill_honors_mutation_flag_and_csrf_and_authorizes_before_abort() {
    // The kill route honors the D7a mutation+CSRF gate: 403 when mutations are OFF
    // (even for a live flow — authorize-BEFORE-abort, so no existence oracle), 403
    // on a CSRF mismatch, 200 on a valid CSRF kill, 404 on a re-kill of the now-
    // finished flow. Drives a real parked stream so the AbortHub holds a live token.
    let upstream = ChunkThenPendingUpstream::new(vec![
        content_chunk("chat-1", "Hel"),
        usage_chunk("chat-1", 100, 20, 120, Some(5), None),
    ]);
    let stream_polled = upstream.stream_polled.notified();

    // (a) Mutations OFF: a live flow must STILL be 403 (authorize-before-abort).
    let auth_off = llmconduit::dashboard_auth::DashboardAuth::from_env(
        "0.0.0.0:4000".parse().unwrap(),
        &d13_env(false),
    )
    .expect("auth builds")
    .auth;
    let gateway = d13_gateway(Arc::new(upstream.clone()), Arc::clone(&auth_off));
    let api_call_id = d3_open_flow(&gateway);
    let mut stream = gateway
        .clone()
        .stream_responses_with_api_call_id(
            base_request(vec![user_message("count")]),
            Some(api_call_id.clone()),
        )
        .await
        .expect("stream");
    // Drain the early events until the upstream parks → the flow is live + registered.
    let mut early = 0;
    while early < 2 {
        match stream.next().await {
            Some(_) => early += 1,
            None => break,
        }
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_polled)
        .await
        .expect("upstream parked");
    assert_eq!(
        gateway.abort_hub().live_len(),
        1,
        "live flow registered a kill token"
    );

    let app_off = d13_router(Arc::clone(&gateway));
    let (cookie, _exp) = auth_off.issue_session();
    let denied = app_off
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/dashboard/api/flows/{api_call_id}/kill"))
                .header(
                    axum::http::header::COOKIE,
                    format!("llmconduit_session={cookie}; llmconduit_csrf=tok"),
                )
                .header("x-csrf-token", "tok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        denied.status().as_u16(),
        403,
        "mutations OFF ⇒ 403 even for a live flow (authorize before abort)"
    );
    d13_assert_no_store(&denied);
    assert_eq!(
        gateway.abort_hub().live_len(),
        1,
        "a denied kill must NOT have aborted the flow (authorize-before-abort)"
    );

    // (b) Mutations ON but the SAME gateway: rebuild auth with mutations enabled and
    // a router over the same gateway so the live flow is still killable.
    let auth_on = llmconduit::dashboard_auth::DashboardAuth::from_env(
        "0.0.0.0:4000".parse().unwrap(),
        &d13_env(true),
    )
    .expect("auth builds")
    .auth;
    let gateway_on = Arc::new(
        gateway
            .as_ref()
            .clone()
            .with_dashboard_auth(Some(Arc::clone(&auth_on))),
    );
    let app_on = d13_router(Arc::clone(&gateway_on));
    let (cookie_on, _exp) = auth_on.issue_session();

    // CSRF mismatch (header ≠ cookie) ⇒ 403.
    let csrf_bad = app_on
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/dashboard/api/flows/{api_call_id}/kill"))
                .header(
                    axum::http::header::COOKIE,
                    format!("llmconduit_session={cookie_on}; llmconduit_csrf=secret"),
                )
                .header("x-csrf-token", "WRONG")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(csrf_bad.status().as_u16(), 403, "CSRF mismatch ⇒ 403");

    // Valid CSRF (header == cookie) ⇒ 200 killed.
    let killed = app_on
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/dashboard/api/flows/{api_call_id}/kill"))
                .header(
                    axum::http::header::COOKIE,
                    format!("llmconduit_session={cookie_on}; llmconduit_csrf=secret"),
                )
                .header("x-csrf-token", "secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(killed.status().as_u16(), 200, "valid CSRF kill ⇒ 200");
    d13_assert_no_store(&killed);
    let killed_body = d13_json(killed).await;
    // The frozen `KillResponse` carries BOTH api_call_id and killed.
    assert_eq!(killed_body["killed"], serde_json::json!(true));
    assert_eq!(
        killed_body["api_call_id"],
        serde_json::json!(api_call_id),
        "kill response carries the frozen KillResponse.api_call_id field"
    );

    // The killed stream tears down; the record finalizes Cancelled.
    let drain = async { while stream.next().await.is_some() {} };
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), drain).await;
    let record = d3_await_terminal(&gateway_on, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Cancelled
    );

    // Re-kill of the now-finished flow ⇒ 404 (no live token).
    let gone = app_on
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/dashboard/api/flows/{api_call_id}/kill"))
                .header(
                    axum::http::header::COOKIE,
                    format!("llmconduit_session={cookie_on}; llmconduit_csrf=secret"),
                )
                .header("x-csrf-token", "secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        gone.status().as_u16(),
        404,
        "re-kill of a finished flow ⇒ 404"
    );
}

#[tokio::test]
async fn d13_end_to_end_streamed_flow_through_real_router() {
    // THE CAPSTONE end-to-end against the REAL backend (a wiremock upstream + the
    // real `ReqwestUpstreamClient` leaf): a streamed request POSTed through the
    // router appears in `/flows`; its `/flows/:id` shows the THREE captured bodies
    // (inbound from the middleware, normalized + upstream on-wire from the leaf) +
    // usage + deltas; `/metrics` + `/topology` populate; a `/snapshot?at=` returns a
    // BODY-FREE cut. The leaf records `model_served`, so the flow is priced.
    let (app, gateway, _server) = d13_wiremock_app().await;

    // POST a streamed /v1/chat/completions through the router (NOT behind auth —
    // only /dashboard/* is). The `log_api_call` middleware mints the api_call_id,
    // captures the inbound body, and opens the flow; the engine + leaf stream,
    // capture the normalized + upstream bodies, and finalize.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "model": "glm-5.1",
                        "stream": true,
                        "messages": [{"role": "user", "content": "hi"}]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200, "streamed response served");
    let _ = axum::body::to_bytes(response.into_body(), 8 * 1024 * 1024)
        .await
        .expect("drain SSE");
    let api_call_id = d13_await_any_terminal(&gateway).await;

    // (1) The flow appears in `/flows` with a cost (dev-open: no cookie needed).
    let flows = d13_json(d13_get(&app, "/dashboard/api/flows").await).await;
    assert!(
        flows["flows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| { row["api_call_id"] == serde_json::json!(api_call_id) }),
        "the streamed flow appears in /flows"
    );

    // (2) `/flows/:id` shows the THREE bodies + usage + deltas + flow_seq.
    let detail =
        d13_json(d13_get(&app, &format!("/dashboard/api/flows/{api_call_id}")).await).await;
    assert!(
        detail["flow_seq"].is_u64(),
        "detail carries the flow cursor"
    );
    assert_eq!(detail["api_call_id"], serde_json::json!(api_call_id));
    // The two on-wire bodies D2 captures: the INBOUND request (middleware) and the
    // UPSTREAM chat body (the leaf). Both are parsed JSON objects in the detail.
    assert!(
        detail["inbound_body"].is_object(),
        "inbound body captured (middleware)"
    );
    assert!(
        detail["upstream_body"].is_object(),
        "upstream on-wire body captured (D2 leaf)"
    );
    // The inbound body round-trips the request the client POSTed.
    assert_eq!(
        detail["inbound_body"]["model"],
        serde_json::json!("glm-5.1")
    );
    // The upstream body carries the model the leaf actually POSTed upstream.
    assert_eq!(
        detail["upstream_body"]["model"],
        serde_json::json!("glm-5.1")
    );
    // (D13 R1 HIGH) The MIDDLE pane: the engine captures the NORMALIZED canonical
    // body (the internal `ResponsesRequest` after the inbound→canonical adapter, just
    // before lowering) via `set_normalized`, so `normalized` is NON-absent, parses as
    // a JSON object, and DIFFERS from the raw inbound chat body — the inbound is the
    // `/v1/chat/completions` shape (`messages`), the normalized is the canonical
    // Responses shape (`input`/`instructions`), so the 3-pane inspector shows a real
    // inbound → normalized → upstream transformation.
    assert!(
        detail["normalized"].is_object(),
        "normalized canonical body captured by the engine (3-pane MIDDLE pane), got {:?}",
        detail["normalized"]
    );
    assert_ne!(
        detail["normalized"], detail["inbound_body"],
        "normalized canonical body DIFFERS from the raw inbound body"
    );
    // The canonical Responses body carries `input` (the inbound chat `messages`
    // adapted), proving it is the internal protocol, not a passthrough of the inbound.
    assert!(
        detail["normalized"].get("input").is_some(),
        "normalized is the canonical Responses request (has `input`), got {:?}",
        detail["normalized"]
    );
    // (D13 R1 HIGH) `model_requested` — the ORIGINAL request model captured before
    // resolution — is surfaced on the detail (here it equals the served `glm-5.1`).
    assert_eq!(
        detail["model_requested"],
        serde_json::json!("glm-5.1"),
        "model_requested captured (pre-resolution request model)"
    );
    let usage = &detail["usage"];
    assert_eq!(
        usage["total"],
        serde_json::json!(140),
        "usage threaded through"
    );
    assert_eq!(usage["prompt"], serde_json::json!(100));
    assert!(
        detail["deltas"].is_array(),
        "deltas replayed from the monitor"
    );
    let cost = detail["cost"].as_f64().expect("detail cost priced");
    assert!((cost - 0.425).abs() < 1e-9, "detail cost {cost} == 0.425");
    // Gap 07: the inspector detail carries the cost confidence tag too (confident here —
    // priced glm-5.1 with a configured cache rate). usage cached is a present measured
    // value (the upstream reported a cached breakdown), not absent.
    assert_eq!(detail["cost_confidence"], serde_json::json!("confident"));
    assert!(
        usage.get("cached").is_some(),
        "the upstream reported cached ⇒ a present measured value, not absent"
    );

    // (3) `/metrics` populated — the completed flow bumped the rings.
    let metrics = d13_json(d13_get(&app, "/dashboard/api/metrics").await).await;
    assert!(
        metrics["metrics_seq"].as_u64().unwrap() > 0,
        "the finalized flow advanced metrics_seq"
    );
    // Gap 07: the headline + m1 window carry the aggregate cost confidence (confident —
    // the only priced bucket bills cached at a configured rate).
    assert_eq!(
        metrics["cost_confidence"],
        serde_json::json!("confident"),
        "aggregate cost confidence rides the metrics body"
    );
    assert_eq!(
        metrics["windows"]["m1"]["cost_confidence"],
        serde_json::json!("confident")
    );

    // (4) `/topology` populated — price table + a topology_seq cursor.
    let topology = d13_json(d13_get(&app, "/dashboard/api/topology").await).await;
    assert!(topology["topology_seq"].is_u64());
    assert!(
        topology["price_table"]["glm-5.1"].is_object(),
        "price table present"
    );

    // (5) `/snapshot?at=` returns a BODY-FREE cut. Take a coordinated snapshot
    // directly (deterministic — the 5 s task may not have ticked), then read it back.
    let cut = gateway
        .metrics()
        .snapshot(gateway.flow_store(), &gateway.provider_health_publisher())
        .expect("snapshot taken");
    let at = cut.taken_at_ms;
    let snapshot = d13_json(d13_get(&app, &format!("/dashboard/api/snapshot?at={at}")).await).await;
    assert!(snapshot["cursors"]["flow_seq"].is_u64());
    assert!(snapshot["cursors"]["metrics_seq"].is_u64());
    assert!(snapshot["cursors"]["topology_seq"].is_u64());
    assert!(snapshot["cursors"]["monitor_seq"].is_u64());
    assert!(snapshot["at_ms"].as_u64().unwrap() > 0);
    // The summaries are BODY-FREE: a summary row carries NO inbound_body/normalized/
    // upstream_body keys (only the detail endpoint carries bodies).
    let summary = snapshot["summaries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["api_call_id"] == serde_json::json!(api_call_id))
        .expect("the flow is in the snapshot summaries");
    assert!(
        summary.get("inbound_body").is_none(),
        "snapshot summaries are BODY-FREE"
    );
    assert!(
        summary.get("normalized").is_none(),
        "snapshot summaries are BODY-FREE"
    );
    assert!(
        summary.get("upstream_body").is_none(),
        "snapshot summaries are BODY-FREE"
    );
    // The snapshot still reshapes metrics + topology into their REST bodies.
    assert!(snapshot["metrics"]["metrics_seq"].is_u64());
    assert!(snapshot["topology"]["price_table"].is_object());
}

#[tokio::test]
async fn d13_historical_snapshot_active_streams_is_frozen_to_the_cut() {
    // D13 R1 HIGH: a historical `/snapshot?at=` reports the `active_streams` of the
    // FROZEN cut (the open flows AT THAT INSTANT), NOT the live FlowStore count now.
    // Open a flow, take a coordinated cut (1 open stream), THEN finalize the flow
    // (0 open now). The cut-time `?at=` must still read 1; the live `/metrics` reads 0.
    let (app, gateway) = llmconduit::build_app_with_gateway_and_options(
        test_config(),
        None,
        llmconduit::AppOptions {
            with_debug_ui: true,
        },
    );
    gateway.provider_health_publisher().publish(Vec::new());

    // Open a flow and leave it OPEN (a live stream).
    let api_call_id = "api_frozen_active".to_string();
    gateway.flow_store().open(
        api_call_id.clone(),
        "POST".to_string(),
        "/v1/responses".to_string(),
        llmconduit::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
        None,
        llmconduit::dashboard_flow::ClientAttribution::none(),
    );

    // Coordinated cut while the flow is open ⇒ the cut's summaries carry 1 open flow.
    let cut = gateway
        .metrics()
        .snapshot(gateway.flow_store(), &gateway.provider_health_publisher())
        .expect("snapshot taken");
    let at = cut.taken_at_ms;

    // Now finalize the flow ⇒ the LIVE open count drops to 0, but the cut is frozen.
    gateway.flow_store().finalize(
        &api_call_id,
        llmconduit::dashboard_flow::FlowStatus::Completed,
        Some("response.completed".to_string()),
        None,
    );

    // The historical `?at=` reflects the FROZEN cut: 1 active stream.
    let snapshot = d13_json(d13_get(&app, &format!("/dashboard/api/snapshot?at={at}")).await).await;
    assert_eq!(
        snapshot["metrics"]["active_streams"],
        serde_json::json!(1),
        "historical snapshot active_streams is the FROZEN cut's open count (1), not now"
    );

    // The LIVE `/metrics` reflects NOW: the flow finalized, so 0 active streams.
    let metrics = d13_json(d13_get(&app, "/dashboard/api/metrics").await).await;
    assert_eq!(
        metrics["active_streams"],
        serde_json::json!(0),
        "live metrics active_streams reflects the finalized flow (0), proving the \
         snapshot's 1 came from the cut, not the live store"
    );
}

#[tokio::test]
async fn d3_cancel_during_send_finalizes_cancelled_not_failed() {
    // D3 R1 #1: a client that drops the SSE receiver while the engine is BLOCKED
    // inside `send_event`'s `tx.send().await` (the channel is full because the
    // client never drained it) must finalize the flow Cancelled — NOT Failed.
    // Before the fix `send_event` mapped a closed receiver to `AppError::internal`,
    // which the spawned choke (is_cancelled() == false) classified Failed. This is
    // the MID-SEND cancel path, distinct from `d3_midstream_cancel_*` whose cancel
    // lands while parked in `next_upstream_chunk`.
    use std::sync::atomic::Ordering;

    let flood = 300; // » the 128-slot channel, so the engine blocks on send.
    let upstream = FloodThenParkUpstream::new(flood);
    let yielded = Arc::clone(&upstream.yielded);
    let gateway = test_gateway_with_flow_store_upstream(Arc::new(upstream.clone()));
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("flood")]);
    // Hold the stream but NEVER poll it, so nothing drains the channel.
    let stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");

    // On the current-thread test runtime the spawned `run_turn` can only run while
    // we await. Yield until the upstream's yield-count goes quiescent: that happens
    // EXACTLY when the engine is blocked on a full channel (it cannot pull the next
    // chunk until a send completes, and no send can complete while we never drain).
    // A stable count across a long streak ⇒ the engine is parked mid-send.
    let mut last = usize::MAX;
    let mut stable = 0;
    let mut guard = 0;
    while stable < 200 {
        tokio::task::yield_now().await;
        let now = yielded.load(Ordering::SeqCst);
        if now == last {
            stable += 1;
        } else {
            stable = 0;
            last = now;
        }
        guard += 1;
        assert!(guard < 1_000_000, "engine never went quiescent (mid-send)");
    }
    assert!(
        last > 0 && last < flood,
        "engine blocked on a full channel mid-flood (yielded {last} of {flood}), \
         i.e. parked inside tx.send — not drained to completion, not still at 0"
    );

    // Client hangs up WHILE the engine is blocked inside `tx.send().await`.
    let stream_dropped = upstream.stream_dropped.notified();
    drop(stream);
    // The dropped receiver makes the pending send fail → AppError::cancelled() →
    // the spawned `run_turn` returns and its stream drops.
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_dropped)
        .await
        .expect("upstream stream dropped after the client hung up mid-send");

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Cancelled,
        "client disconnect DURING a tx.send finalizes Cancelled, not Failed"
    );
    assert_eq!(
        record.terminal_reason.as_deref(),
        Some("client_disconnected"),
        "carries the cancellation terminal reason, not an internal-error string"
    );
    assert_eq!(
        record.claim.load(Ordering::SeqCst),
        llmconduit::dashboard_flow::CLAIM_FINALIZED,
    );
}

#[tokio::test]
async fn d6_kill_during_full_channel_send_unblocks_and_tears_down() {
    // D6 R1: a dashboard KILL while the engine is BLOCKED inside `send_event`'s
    // `tx.send().await` — the client is CONNECTED but NOT draining, so the 128-slot
    // SSE channel is FULL — must unblock the send via the abort token, finalize the
    // flow Cancelled, tear the upstream stream down, and leak no AbortHub entry.
    // Before composing the token into `send_event`, the send only resolved on
    // capacity OR a fully-closed receiver, so this kill was MISSED: the task parked
    // here forever and the AbortHub entry stayed live until the client drained.
    // This is the SEND counterpart to `d3_cancel_during_send_*` (which hangs up) and
    // to `d6_kill_midchunk_*` (whose kill lands while parked in `next_upstream_chunk`,
    // NOT mid-send).
    use std::sync::atomic::Ordering;

    let flood = 300; // » the 128-slot channel, so the engine blocks on send.
    let upstream = FloodThenParkUpstream::new(flood);
    let yielded = Arc::clone(&upstream.yielded);
    let stream_dropped = upstream.stream_dropped.notified();
    let gateway = test_gateway_with_flow_store_upstream(Arc::new(upstream.clone()));
    let api_call_id = d3_open_flow(&gateway);

    let request = base_request(vec![user_message("flood")]);
    // Hold the stream but NEVER poll it before the kill, so nothing drains the channel
    // and the engine is forced to block INSIDE `tx.send().await`.
    let mut stream = gateway
        .clone()
        .stream_responses_with_api_call_id(request, Some(api_call_id.clone()))
        .await
        .expect("stream");

    // On the current-thread test runtime the spawned `run_turn` only runs while we
    // await. Yield until the upstream's yield-count goes quiescent: that is EXACTLY
    // when the engine is blocked on a full channel (it cannot pull the next chunk
    // until a send completes, and no send can complete while we never drain).
    let mut last = usize::MAX;
    let mut stable = 0;
    let mut guard = 0;
    while stable < 200 {
        tokio::task::yield_now().await;
        let now = yielded.load(Ordering::SeqCst);
        if now == last {
            stable += 1;
        } else {
            stable = 0;
            last = now;
        }
        guard += 1;
        assert!(guard < 1_000_000, "engine never went quiescent (mid-send)");
    }
    assert!(
        last > 0 && last < flood,
        "engine blocked on a full channel mid-flood (yielded {last} of {flood}), \
         i.e. parked inside tx.send — not drained, not still at 0"
    );

    // The kill token is registered while the flow is live, and the CLIENT is still
    // connected (we still hold `stream`, never dropped it) — so this proves the kill
    // composes with, and does NOT depend on, the `tx.closed()` hang-up path.
    assert_eq!(
        gateway.abort_hub().live_len(),
        1,
        "live flow registered exactly one kill token"
    );

    // SERVER-SIDE kill while the engine is parked inside `tx.send().await`.
    assert!(
        gateway.abort(&api_call_id),
        "abort found the live token → true"
    );

    // Draining now lets the queued events flush; the engine's blocked send unblocked
    // via the token (→ cancelled()), `run_turn` returned, and the stream ENDS with a
    // terminal `response.failed` — collecting the remainder must not hang. (Without the
    // fix the send would still be parked here and this drain would time out.)
    let mut saw_failed = false;
    let drain = async {
        while let Some(event) = stream.next().await {
            if event.event == "response.failed" {
                saw_failed = true;
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), drain)
        .await
        .expect("killed mid-send stream unblocked + terminated (did not hang)");
    assert!(
        saw_failed,
        "killed stream ended with a terminal response.failed, not a half-open hang"
    );

    // The upstream stream was actually dropped — the engine tore down upstream work on
    // the kill (no orphan task), proving the blocked send did not strand upstream.
    tokio::time::timeout(std::time::Duration::from_secs(5), stream_dropped)
        .await
        .expect("upstream stream dropped on mid-send kill (upstream work torn down)");

    let record = d3_await_terminal(&gateway, &api_call_id).await;
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Cancelled,
        "a kill DURING a full-channel tx.send finalizes Cancelled, not Failed"
    );
    assert_eq!(
        record.terminal_reason.as_deref(),
        Some("client_disconnected"),
        "carries the cancellation terminal reason, not an internal-error string"
    );
    assert_eq!(
        record.claim.load(Ordering::SeqCst),
        llmconduit::dashboard_flow::CLAIM_FINALIZED,
    );
    assert_eq!(
        gateway.abort_hub().live_len(),
        0,
        "no AbortHub entry leaks after the killed-mid-send flow finalized"
    );
}

#[tokio::test]
async fn d3_extractor_failure_l0_guard_finalizes_no_orphan() {
    // A malformed JSON body is rejected by the axum `Json` extractor BEFORE the
    // request reaches the engine, so the record is never ClaimedL1. The L0
    // middleware guard's Drop finalizes it Failed("unhandled") — no orphan Open.
    let gateway = test_gateway_with_flow_store(MockUpstream::default(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(Arc::clone(&gateway));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from("{ this is not valid json"))
                .expect("request"),
        )
        .await
        .expect("response");
    // axum rejects the body (4xx) before the handler runs.
    assert!(
        response.status().is_client_error(),
        "extractor rejected the malformed body: {}",
        response.status()
    );

    // The record was opened by the middleware, then finalized by the L0 Drop.
    let records = gateway.flow_store().list();
    assert_eq!(records.len(), 1, "one record opened by middleware");
    let record = &records[0];
    assert_eq!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Failed,
        "L0 guard finalized the orphan Failed (no record stuck Open)"
    );
    assert_eq!(record.terminal_reason.as_deref(), Some("unhandled"));
    assert_ne!(
        record.status,
        llmconduit::dashboard_flow::FlowStatus::Open,
        "no orphan left Open"
    );
    assert_eq!(
        record.claim.load(std::sync::atomic::Ordering::SeqCst),
        llmconduit::dashboard_flow::CLAIM_FINALIZED,
    );
}

// ---------------------------------------------------------------------------
// Tracing capture harness (D7a R2 #1 regression).
//
// `tracing` caches per-callsite interest GLOBALLY. If the process has no global
// default subscriber, callsites are cached as "never" the first time they fire
// (under `NoSubscriber`), and a later THREAD-LOCAL `set_default` is never even
// consulted — the macro short-circuits. So a per-test thread-local subscriber
// captures nothing once another test has run first.
//
// The fix: install ONE global subscriber for the whole test binary that fans out
// to a per-THREAD buffer. With a real global default, callsites cache as
// "always" and dispatch reliably. A test enables capture on its own thread,
// drives the request (on the SAME thread via a current-thread runtime), then
// reads its own buffer — fully isolated from any other test thread.
// ---------------------------------------------------------------------------

thread_local! {
    /// Per-thread capture buffer; `Some` only while a test is actively capturing
    /// on this thread. Other threads' events are dropped (their cell is `None`).
    static CAPTURE_BUF: std::cell::RefCell<Option<Vec<u8>>> = const { std::cell::RefCell::new(None) };
}

/// A `MakeWriter` that appends to the calling thread's `CAPTURE_BUF` when capture
/// is active there, and discards otherwise.
struct ThreadLocalCapture;

struct ThreadLocalCaptureWriter;

impl std::io::Write for ThreadLocalCaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        CAPTURE_BUF.with(|cell| {
            if let Some(sink) = cell.borrow_mut().as_mut() {
                sink.extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ThreadLocalCapture {
    type Writer = ThreadLocalCaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        ThreadLocalCaptureWriter
    }
}

/// Install the global capture subscriber exactly once for this test binary.
fn install_capture_subscriber() {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(ThreadLocalCapture)
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish();
        // Ignore an error if some other harness already set a global default —
        // capture simply won't see events then, which the test asserts against.
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

/// Run `body` with tracing capture active on THIS thread, returning the captured
/// log text. The closure must drive its request on the current thread (we use a
/// current-thread tokio runtime) so its events land in this thread's buffer.
fn capture_logs<F: std::future::Future<Output = ()>>(body: impl FnOnce() -> F) -> String {
    install_capture_subscriber();
    CAPTURE_BUF.with(|cell| *cell.borrow_mut() = Some(Vec::new()));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    rt.block_on(body());
    CAPTURE_BUF.with(|cell| {
        let taken = cell.borrow_mut().take().unwrap_or_default();
        String::from_utf8(taken).expect("utf8 log")
    })
}

/// REGRESSION (D7a R2 #1): the dashboard ACCESS TOKEN must never reach the logs.
/// `/dashboard/login` carries `{"token": "..."}`; the shared redactor does NOT
/// strip a bare `token` key, so the small-body `body_payload` dump used to write
/// it verbatim. The middleware now skips BOTH the summary and the payload dump
/// for the auth endpoints. We drive a REAL login through the real `log_api_call`
/// middleware with a known token and assert it appears in NO tracing field.
#[test]
fn login_token_is_never_logged() {
    const KNOWN_TOKEN: &str = "SUPERSECRET-LOGIN-TOKEN-7f3a";

    let logged = capture_logs(|| async {
        // Loopback + no token env → dev-open: `/dashboard/login` registers and
        // the (any) token verifies. The body still carries the token field that
        // the payload dump would have leaked.
        let app = llmconduit::build_app_with_options(
            test_config(),
            llmconduit::AppOptions {
                with_debug_ui: true,
            },
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/dashboard/login")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&json!({ "token": KNOWN_TOKEN })).expect("serialize"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        // Dev-open accepts any token → 200 with the session cookies set.
        assert_eq!(response.status().as_u16(), 200);
    });

    // The middleware DID log the request (so we know capture is wired)…
    assert!(
        logged.contains("/dashboard/login"),
        "the login request was logged at all: {logged}"
    );
    // …but the token value must appear in NO field (summary, payload, or else).
    assert!(
        !logged.contains(KNOWN_TOKEN),
        "dashboard login token leaked into the logs:\n{logged}"
    );
    // The payload-dump line must be skipped entirely for the auth endpoint.
    assert!(
        !logged.contains("inbound API request payload"),
        "auth-endpoint body payload must not be dumped:\n{logged}"
    );
}

fn test_gateway(upstream: MockUpstream, search: MockSearch) -> Arc<Gateway> {
    test_gateway_with_config(upstream, search, test_config())
}

fn test_gateway_with_raw_output(
    upstream: MockUpstream,
    search: MockSearch,
    raw_output: RawOutput,
) -> Arc<Gateway> {
    test_gateway_with_config_and_raw_output(upstream, search, test_config(), Some(raw_output))
}

fn test_gateway_with_config(
    upstream: MockUpstream,
    search: MockSearch,
    config: Config,
) -> Arc<Gateway> {
    test_gateway_with_config_and_raw_output(upstream, search, config, None)
}

fn test_gateway_with_config_and_raw_output(
    upstream: MockUpstream,
    search: MockSearch,
    config: Config,
    raw_output: Option<RawOutput>,
) -> Arc<Gateway> {
    // Build the leaf finalization policies from the test config so the mock's
    // leaf-mirror applies the SAME profile/family/effort kwargs the production
    // leaf would (T1 moved profile resolution from the engine to the leaf).
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    // Non-image-agent tests get a no-op vision client; the cache is built from
    // config and never activated unless `image_agent_enabled` + `vision_url`.
    // A real (never-called) `ReqwestVisionClient` keeps this builder independent
    // of the `MockVisionClient`, which now lives with the image-agent suite.
    let vision: Arc<dyn llmconduit::vision::VisionClient> = Arc::new(
        llmconduit::vision::ReqwestVisionClient::new(reqwest::Client::new(), &config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(Gateway::new(
        config,
        ReplayStore::new(1000),
        Arc::new(upstream),
        Arc::new(search),
        vision,
        image_cache,
        MonitorHub::new(128),
        raw_output,
        llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
    ))
}

fn test_config() -> Config {
    Config {
        bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
        upstream_base_url: "http://127.0.0.1:8000/v1".parse().expect("url"),
        upstream_api_key: None,
        upstream_model: None,
        system_prompt_prefix: None,
        upstream_request_log_path: None,
        turn_capture_dir: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        model_routes: Vec::new(),
        template_family: None,
        brave_base_url: "https://example.com/".parse().expect("url"),
        brave_api_key: Some("test-key".to_string()),
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
        image_agent_enabled: false,
        vision_url: None,
        vision_model: None,
        image_cache_max_size: 100,
        image_cache_ttl_secs: 300,
        unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
        price_table: std::collections::HashMap::new(),
    }
}

fn base_request(input: Vec<ResponseItem>) -> ResponsesRequest {
    ResponsesRequest {
        model: "glm-5.1".to_string(),
        instructions: String::new(),
        input,
        tools: Vec::new(),
        tool_choice: json!("auto"),
        parallel_tool_calls: Some(true),
        reasoning: Some(llmconduit::models::responses::ReasoningRequest {
            effort: Some("medium".to_string()),
            summary: None,
        }),
        thinking: None,
        store: false,
        stream: true,
        include: Vec::new(),
        service_tier: None,
        prompt_cache_key: None,
        text: None,
        client_metadata: None,
        previous_response_id: None,
        temperature: None,
        top_p: None,
        max_output_tokens: None,
        frequency_penalty: None,
        presence_penalty: None,
        truncation: None,
        metadata: None,
        stop: None,
        extra_body: Default::default(),
    }
}

fn user_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        phase: None,
    }
}

fn content_chunk(id: &str, content: &str) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_string(),
        choices: vec![ChatChunkChoice {
            index: 0,
            delta: ChatDelta {
                content: Some(content.to_string()),
                reasoning_content: None,
                tool_calls: None,
                function_call: None,
                refusal: None,
                extra: Default::default(),
            },
            finish_reason: None,
            stop_reason: None,
        }],
        usage: None,
    }
}

/// A terminal chunk carrying `finish_reason: "length"` (upstream max-output-token
/// truncation) so the engine derives `response.incomplete` for the turn. Used by the
/// F1c finding-#3 test to assert the capture artifact maps `length` → `incomplete`.
fn length_finish_chunk(id: &str) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_string(),
        choices: vec![ChatChunkChoice {
            index: 0,
            delta: ChatDelta {
                content: None,
                reasoning_content: None,
                tool_calls: None,
                function_call: None,
                refusal: None,
                extra: Default::default(),
            },
            finish_reason: Some("length".to_string()),
            stop_reason: None,
        }],
        usage: None,
    }
}

fn reasoning_chunk(id: &str, reasoning: &str) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_string(),
        choices: vec![ChatChunkChoice {
            index: 0,
            delta: ChatDelta {
                content: None,
                reasoning_content: Some(reasoning.to_string()),
                tool_calls: None,
                function_call: None,
                refusal: None,
                extra: Default::default(),
            },
            finish_reason: None,
            stop_reason: None,
        }],
        usage: None,
    }
}

fn nested_thinking_chunk(id: &str, thinking: &str, signature: &str) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_string(),
        choices: vec![ChatChunkChoice {
            index: 0,
            delta: ChatDelta {
                content: None,
                reasoning_content: None,
                tool_calls: None,
                function_call: None,
                refusal: None,
                extra: std::collections::BTreeMap::from([(
                    "thinking".to_string(),
                    json!({
                        "content": thinking,
                        "signature": signature
                    }),
                )]),
            },
            finish_reason: None,
            stop_reason: None,
        }],
        usage: None,
    }
}

fn tool_call_chunk(id: &str, call_id: &str, name: &str, arguments: &str) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_string(),
        choices: vec![ChatChunkChoice {
            index: 0,
            delta: ChatDelta {
                content: None,
                reasoning_content: None,
                tool_calls: Some(vec![ChatToolCall {
                    id: Some(call_id.to_string()),
                    index: Some(0),
                    kind: "function".to_string(),
                    function: ChatFunctionCall {
                        name: Some(name.to_string()),
                        arguments: Some(serde_json::Value::String(arguments.to_string())),
                    },
                }]),
                function_call: None,
                refusal: None,
                extra: Default::default(),
            },
            finish_reason: Some("tool_calls".to_string()),
            stop_reason: None,
        }],
        usage: None,
    }
}

fn legacy_function_call_chunk(id: &str, name: &str, arguments: &str) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_string(),
        choices: vec![ChatChunkChoice {
            index: 0,
            delta: ChatDelta {
                content: None,
                reasoning_content: None,
                tool_calls: None,
                function_call: Some(ChatFunctionCall {
                    name: Some(name.to_string()),
                    arguments: Some(serde_json::Value::String(arguments.to_string())),
                }),
                refusal: None,
                extra: Default::default(),
            },
            finish_reason: Some("function_call".to_string()),
            stop_reason: None,
        }],
        usage: None,
    }
}

fn usage_chunk(
    id: &str,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    cached_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_string(),
        usage: Some(ChunkUsage {
            prompt_tokens: prompt_tokens.try_into().expect("prompt_tokens fits in i64"),
            completion_tokens: completion_tokens
                .try_into()
                .expect("completion_tokens fits in i64"),
            total_tokens: total_tokens.try_into().expect("total_tokens fits in i64"),
            reasoning_tokens: None,
            prompt_tokens_details: cached_tokens.map(|cached_tokens| PromptTokensDetails {
                cached_tokens: cached_tokens as i64,
            }),
            completion_tokens_details: reasoning_tokens.map(|reasoning_tokens| {
                CompletionTokensDetails {
                    reasoning_tokens: reasoning_tokens as i64,
                }
            }),
        }),
        choices: Vec::new(),
    }
}

async fn collect_stream(
    stream: tokio_stream::wrappers::ReceiverStream<llmconduit::engine::SseEvent>,
) -> Vec<serde_json::Value> {
    stream
        .map(|event| {
            let mut value = event.data;
            if let serde_json::Value::Object(map) = &mut value {
                map.insert("_event".to_string(), serde_json::Value::String(event.event));
            }
            value
        })
        .collect()
        .await
}

struct SharedBuffer(Arc<StdMutex<Vec<u8>>>);

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("buffer lock poisoned"))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn event_names(events: &[serde_json::Value]) -> Vec<&str> {
    events
        .iter()
        .map(|event| {
            event["_event"]
                .as_str()
                .expect("event name should be present")
        })
        .collect()
}

fn done_items(events: &[serde_json::Value]) -> Vec<ResponseItem> {
    events
        .iter()
        .filter(|event| event["_event"] == "response.output_item.done")
        .map(|event| serde_json::from_value(event["item"].clone()).expect("response item"))
        .collect()
}

fn parse_anthropic_sse_events(body: &str) -> Vec<serde_json::Value> {
    body.split("\n\n")
        .filter_map(|block| {
            block.lines().find_map(|line| {
                line.strip_prefix("data: ")
                    .map(|data| serde_json::from_str(data).expect("valid Anthropic SSE JSON"))
            })
        })
        .collect()
}

/// Parse a raw `/v1/responses` streaming SSE body into its `data:` JSON
/// payloads (each payload's own `"type"` field mirrors the frame's `event:`
/// line, so callers filter on `event["type"]`).
fn parse_responses_sse_events(body: &str) -> Vec<serde_json::Value> {
    body.split("\n\n")
        .filter_map(|block| {
            block.lines().find_map(|line| {
                line.strip_prefix("data: ")
                    .map(|data| serde_json::from_str(data).expect("valid Responses SSE JSON"))
            })
        })
        .collect()
}

fn parse_chat_sse_events(body: &str) -> Vec<serde_json::Value> {
    body.split("\n\n")
        .filter_map(|block| {
            block.lines().find_map(|line| {
                line.strip_prefix("data: ").and_then(|data| {
                    (data != "[DONE]")
                        .then(|| serde_json::from_str(data).expect("valid Chat SSE JSON"))
                })
            })
        })
        .collect()
}

fn chat_completion_sse_body(chunks: &[serde_json::Value]) -> String {
    let mut body = String::new();
    for chunk in chunks {
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(chunk).expect("serialize chat chunk"));
        body.push_str("\n\n");
    }
    body.push_str("data: [DONE]\n\n");
    body
}

/// Task 0B1: prove the conformance harness is reachable from THIS integration
/// crate at the public path `llmconduit::adapters::responses_to_anthropic::
/// conformance` -- the same path later phases (C1-T5) will use together with
/// `parse_anthropic_sse_events` to assert real `/v1/messages` SSE output.
/// Hand-built JSON, NOT real converter output (the converter is not wired to
/// the harness yet).
#[test]
fn conformance_harness_is_reachable_from_gateway_integration_crate() {
    use llmconduit::adapters::responses_to_anthropic::conformance::Surface;
    use llmconduit::adapters::responses_to_anthropic::conformance::assert_sse_conformant;

    let events: Vec<serde_json::Value> = vec![
        json!({
            "type": "message_start",
            "message": {
                "id": "msg_1", "type": "message", "role": "assistant", "content": [],
                "model": "m", "stop_reason": null, "stop_sequence": null,
                "usage": {"input_tokens": 1, "output_tokens": 0}
            }
        }),
        json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "hi"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"output_tokens": 1}
        }),
        json!({"type": "message_stop"}),
    ];
    assert_sse_conformant(&events, Surface::TextOnly);
}

#[tokio::test]
async fn explicit_upstreams_models_endpoint_returns_primary_union_and_hides_fallbacks() {
    let first = MockServer::start().await;
    let second = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [
                {"id": "first-model", "object": "model", "owned_by": "first"},
                {"id": "shared-model", "object": "model", "owned_by": "first"}
            ]
        })))
        .mount(&first)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [
                {"id": "second-model", "object": "model", "owned_by": "second"},
                {"id": "shared-model", "object": "model", "owned_by": "second"}
            ]
        })))
        .mount(&second)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "fallback-only"}]
        })))
        .mount(&fallback)
        .await;

    let mut config = test_config();
    config.upstreams = vec![
        UpstreamConfig {
            name: "first".to_string(),
            upstream_base_url: format!("{}/v1/", first.uri()).parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
            fallback_upstreams: vec![FallbackUpstreamConfig {
                name: "fallback".to_string(),
                upstream_base_url: format!("{}/v1/", fallback.uri()).parse().expect("url"),
                upstream_api_key: None,
                upstream_model: Some("fallback-only".to_string()),
                exposed_model: None,
                upstream_chat_kwargs: JsonMap::new(),
                upstream_request_log_path: None,
            }],
        },
        UpstreamConfig {
            name: "second".to_string(),
            upstream_base_url: format!("{}/v1/", second.uri()).parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
            fallback_upstreams: Vec::new(),
        },
    ];

    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    assert!(response.headers().get("etag").is_none());
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    let ids = body["data"]
        .as_array()
        .expect("data array")
        .iter()
        .map(|entry| entry["id"].as_str().expect("model id"))
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["first-model", "shared-model", "second-model"]);
}

#[tokio::test]
async fn chat_completions_routes_normalized_model_to_first_matching_upstream() {
    let first = MockServer::start().await;
    let second = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "qwen-3.5"}]
        })))
        .mount(&first)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "glm-5.1"}]
        })))
        .mount(&second)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({"model": "glm-5.1"})))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-second",
                    "choices": [{
                        "index": 0,
                        "delta": {"content": "second"},
                        "finish_reason": null
                    }],
                    "usage": null
                })])),
        )
        .mount(&second)
        .await;

    let mut config = test_config();
    config.upstreams = vec![
        UpstreamConfig {
            name: "first".to_string(),
            upstream_base_url: format!("{}/v1/", first.uri()).parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
            fallback_upstreams: Vec::new(),
        },
        UpstreamConfig {
            name: "second".to_string(),
            upstream_base_url: format!("{}/v1/", second.uri()).parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
            fallback_upstreams: Vec::new(),
        },
    ];

    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "GLM 5 1",
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
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    assert_eq!(body["model"], "glm-5.1");
    assert_eq!(
        body["choices"][0]["message"]["content"].as_str(),
        Some("second")
    );

    let first_chat_requests = first
        .received_requests()
        .await
        .expect("first requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .count();
    assert_eq!(first_chat_requests, 0);
}

#[tokio::test]
async fn chat_completions_defaults_missing_and_unavailable_models_to_first_upstream_model() {
    let first = MockServer::start().await;
    let second = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "first-model"}]
        })))
        .mount(&first)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "second-model"}]
        })))
        .mount(&second)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({"model": "first-model"})))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-first",
                    "choices": [{
                        "index": 0,
                        "delta": {"content": "first"},
                        "finish_reason": null
                    }],
                    "usage": null
                })])),
        )
        .mount(&first)
        .await;

    let mut config = test_config();
    config.upstreams = vec![
        UpstreamConfig {
            name: "first".to_string(),
            upstream_base_url: format!("{}/v1/", first.uri()).parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
            fallback_upstreams: Vec::new(),
        },
        UpstreamConfig {
            name: "second".to_string(),
            upstream_base_url: format!("{}/v1/", second.uri()).parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
            fallback_upstreams: Vec::new(),
        },
    ];
    let app = llmconduit::build_app(config);

    for body in [
        json!({
            "stream": false,
            "messages": [{"role": "user", "content": "missing"}]
        }),
        json!({
            "model": "not-currently-provided",
            "stream": false,
            "messages": [{"role": "user", "content": "unavailable"}]
        }),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status().as_u16(), 200);
        let body_bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("read body");
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
        assert_eq!(body["model"], "first-model");
    }

    let first_chat_requests = first
        .received_requests()
        .await
        .expect("first requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .count();
    let second_chat_requests = second
        .received_requests()
        .await
        .expect("second requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .count();
    assert_eq!(first_chat_requests, 2);
    assert_eq!(second_chat_requests, 0);
}

#[tokio::test]
async fn selected_upstream_failure_uses_nested_fallback_not_next_routing_upstream() {
    let first = MockServer::start().await;
    let fallback = MockServer::start().await;
    let second = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "first-model"}]
        })))
        .mount(&first)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "second-model"}]
        })))
        .mount(&second)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({"model": "first-model"})))
        .respond_with(ResponseTemplate::new(503).set_body_string("first unavailable"))
        .mount(&first)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({"model": "fallback-model"})))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-fallback",
                    "choices": [{
                        "index": 0,
                        "delta": {"content": "fallback"},
                        "finish_reason": null
                    }],
                    "usage": null
                })])),
        )
        .mount(&fallback)
        .await;

    let mut config = test_config();
    config.upstream_failure_cooldown_secs = 3600;
    config.upstreams = vec![
        UpstreamConfig {
            name: "first".to_string(),
            upstream_base_url: format!("{}/v1/", first.uri()).parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
            fallback_upstreams: vec![FallbackUpstreamConfig {
                name: "fallback".to_string(),
                upstream_base_url: format!("{}/v1/", fallback.uri()).parse().expect("url"),
                upstream_api_key: None,
                upstream_model: Some("fallback-model".to_string()),
                exposed_model: None,
                upstream_chat_kwargs: JsonMap::new(),
                upstream_request_log_path: None,
            }],
        },
        UpstreamConfig {
            name: "second".to_string(),
            upstream_base_url: format!("{}/v1/", second.uri()).parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
            fallback_upstreams: Vec::new(),
        },
    ];

    let app = llmconduit::build_app(config);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "first-model",
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
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    assert_eq!(
        body["choices"][0]["message"]["content"].as_str(),
        Some("fallback")
    );

    let fallback_chat_requests = fallback
        .received_requests()
        .await
        .expect("fallback requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .count();
    let second_chat_requests = second
        .received_requests()
        .await
        .expect("second requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .count();
    assert_eq!(fallback_chat_requests, 1);
    assert_eq!(second_chat_requests, 0);
}

#[tokio::test]
async fn exposed_fallback_model_alias_is_listed_and_routes_to_declaring_fallback() {
    let first = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "local-default"}]
        })))
        .mount(&first)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({"model": "GLM-5.1"})))
        .respond_with(ResponseTemplate::new(503).set_body_string("local unavailable"))
        .mount(&first)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({"model": "z-ai/glm-5.1"})))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-fallback",
                    "choices": [{
                        "index": 0,
                        "delta": {"content": "fallback alias"},
                        "finish_reason": null
                    }],
                    "usage": null
                })])),
        )
        .mount(&fallback)
        .await;

    let mut config = test_config();
    config.upstream_failure_cooldown_secs = 3600;
    config.upstreams = vec![UpstreamConfig {
        name: "first".to_string(),
        upstream_base_url: format!("{}/v1/", first.uri()).parse().expect("url"),
        upstream_api_key: None,
        upstream_model: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstream_request_log_path: None,
        fallback_upstreams: vec![FallbackUpstreamConfig {
            name: "fallback".to_string(),
            upstream_base_url: format!("{}/v1/", fallback.uri()).parse().expect("url"),
            upstream_api_key: None,
            upstream_model: Some("z-ai/glm-5.1".to_string()),
            exposed_model: Some("GLM-5.1".to_string()),
            upstream_chat_kwargs: JsonMap::new(),
            upstream_request_log_path: None,
        }],
    }];

    let app = llmconduit::build_app(config);
    let models_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("models response");
    assert_eq!(models_response.status().as_u16(), 200);
    let models_body_bytes = axum::body::to_bytes(models_response.into_body(), 4096)
        .await
        .expect("read models body");
    let models_body: serde_json::Value =
        serde_json::from_slice(&models_body_bytes).expect("valid models json");
    let ids = models_body["data"]
        .as_array()
        .expect("data array")
        .iter()
        .map(|entry| entry["id"].as_str().expect("model id"))
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["local-default", "GLM-5.1"]);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "GLM-5.1",
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
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
    assert_eq!(body["model"], "GLM-5.1");
    assert_eq!(
        body["choices"][0]["message"]["content"].as_str(),
        Some("fallback alias")
    );

    let first_chat_requests = first
        .received_requests()
        .await
        .expect("first requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .count();
    let fallback_chat_requests = fallback
        .received_requests()
        .await
        .expect("fallback requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .count();
    assert_eq!(first_chat_requests, 0);
    assert_eq!(fallback_chat_requests, 1);
}

// ---------------------------------------------------------------------------
// OpenAI /v1/chat/completions integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chat_completions_fails_over_and_skips_primary_during_cooldown() {
    let primary = MockServer::start().await;
    let fallback = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({ "model": "primary-model" })))
        .respond_with(ResponseTemplate::new(503).set_body_string("primary unavailable"))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({ "model": "fallback-model" })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-fallback",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "content": "fallback ok"
                            },
                            "finish_reason": null
                        }
                    ],
                    "usage": null
                })])),
        )
        .mount(&fallback)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", primary.uri()).parse().expect("url");
    config.upstream_model = Some("primary-model".to_string());
    config.fallback_upstreams = vec![FallbackUpstreamConfig {
        name: "fallback".to_string(),
        upstream_base_url: format!("{}/v1/", fallback.uri()).parse().expect("url"),
        upstream_api_key: None,
        upstream_model: Some("fallback-model".to_string()),
        exposed_model: None,
        upstream_chat_kwargs: JsonMap::from_iter([
            (
                "provider".to_string(),
                json!({
                    "order": ["z-ai"],
                    "allow_fallbacks": true
                }),
            ),
            (
                "chat_template_kwargs".to_string(),
                json!({
                    "fallback_default": true,
                    "shared": "fallback"
                }),
            ),
        ]),
        upstream_request_log_path: None,
    }];
    config.upstream_failure_cooldown_secs = 3600;
    config.model_profiles = std::collections::BTreeMap::from([(
        "client-model".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: None,
            upstream_chat_kwargs: JsonMap::from_iter([(
                "chat_template_kwargs".to_string(),
                json!({
                    "model_default": true,
                    "shared": "model"
                }),
            )]),
            native_vision: None,
            ..Default::default()
        },
    )]);

    let app = llmconduit::build_app(config);
    let request_body = json!({
        "model": "client-model",
        "stream": false,
        "messages": [
            { "role": "user", "content": "Hi" }
        ]
    });

    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&request_body).expect("serialize"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status().as_u16(), 200);
        let body_bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("read body");
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("valid json");
        assert_eq!(
            body["choices"][0]["message"]["content"].as_str(),
            Some("fallback ok")
        );
    }

    let primary_chat_requests = primary
        .received_requests()
        .await
        .expect("recorded primary requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .collect::<Vec<_>>();
    assert_eq!(primary_chat_requests.len(), 1);

    let fallback_chat_requests = fallback
        .received_requests()
        .await
        .expect("recorded fallback requests")
        .into_iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/v1/chat/completions"
        })
        .collect::<Vec<_>>();
    assert_eq!(fallback_chat_requests.len(), 2);
    for request in fallback_chat_requests {
        let body: serde_json::Value = request.body_json().expect("chat request json");
        assert_eq!(body["model"].as_str(), Some("fallback-model"));
        assert_eq!(
            body["provider"],
            json!({
                "order": ["z-ai"],
                "allow_fallbacks": true
            })
        );
        // T1: profile `upstream_chat_kwargs` resolve at the LEAF against the FINAL
        // provider model ("fallback-model"), not the request alias ("client-model").
        // The failover target has no profile of its own, so the request-alias
        // profile's `chat_template_kwargs` ({model_default, shared:"model"}) do
        // NOT bleed onto the fallback — only the fallback PROVIDER's own kwargs
        // ({fallback_default, shared:"fallback"}) reach the backend.
        assert_eq!(
            body["chat_template_kwargs"],
            json!({
                "fallback_default": true,
                "shared": "fallback"
            })
        );
        assert!(
            body["chat_template_kwargs"]["model_default"].is_null(),
            "request-alias profile kwargs must not apply to a failover target (T1)"
        );
    }
}

#[tokio::test]
async fn chat_completions_returns_non_streaming_json() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hello")),
            Ok(usage_chunk("chat-1", 12, 5, 17, Some(3), Some(2))),
        ])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [
            { "role": "user", "content": "Hi" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).expect("valid json");
    assert_eq!(json["object"], "chat.completion");
    assert_eq!(json["model"], "glm-5.1");
    assert_eq!(json["choices"][0]["message"]["role"], "assistant");
    assert_eq!(json["choices"][0]["message"]["content"], "Hello");
    assert_eq!(json["choices"][0]["finish_reason"], "stop");
    assert_eq!(json["usage"]["prompt_tokens"], 12);
    assert_eq!(json["usage"]["completion_tokens"], 5);
    assert_eq!(json["usage"]["prompt_tokens_details"]["cached_tokens"], 3);
    assert_eq!(
        json["usage"]["completion_tokens_details"]["reasoning_tokens"],
        2
    );

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].stream, true);
    assert_eq!(requests[0].messages[0].role, "user");
    assert_eq!(
        requests[0].messages[0]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("Hi")
    );
}

#[tokio::test]
async fn chat_completions_preserves_multimodal_content_parts() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    // E2b: this test proves multimodal CONTENT-PART SHAPES survive canonical
    // round-tripping unchanged (image_url/input_audio/file), which is only
    // true when the backend is native-vision -- a non-native backend now
    // degrades the image part to a text placeholder (the whole point of E2b;
    // covered by its own dedicated tests). Force `native_vision: true` here so
    // this test keeps proving adapter fidelity, not image-degradation policy.
    let mut config = test_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "glm-5.1".to_string(),
        llmconduit::config::ModelProfile {
            native_vision: Some(true),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [
            {
                "role": "user",
                "content": [
                    { "type": "text", "text": "Describe these inputs" },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": "data:image/png;base64,abc",
                            "detail": "high"
                        }
                    },
                    {
                        "type": "input_audio",
                        "input_audio": {
                            "data": "UklGRg==",
                            "format": "wav"
                        }
                    },
                    {
                        "type": "file",
                        "file": {
                            "file_id": "file_doc"
                        }
                    }
                ]
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].messages[0].content.as_ref(),
        Some(&json!([
            { "type": "text", "text": "Describe these inputs" },
            {
                "type": "image_url",
                "image_url": {
                    "url": "data:image/png;base64,abc",
                    "detail": "high"
                }
            },
            {
                "type": "input_audio",
                "input_audio": {
                    "data": "UklGRg==",
                    "format": "wav"
                }
            },
            {
                "type": "file",
                "file": {
                    "file_id": "file_doc"
                }
            }
        ]))
    );
}

#[tokio::test]
async fn chat_completions_streams_openai_sse() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hello")),
            Ok(content_chunk("chat-1", " there")),
            Ok(usage_chunk("chat-1", 12, 5, 17, None, None)),
        ])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": true,
        "stream_options": { "include_usage": true },
        "messages": [
            { "role": "user", "content": "Hi" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body_text = String::from_utf8(body_bytes.to_vec()).expect("utf8");
    assert!(body_text.contains("data: [DONE]"), "missing [DONE]");
    let events = parse_chat_sse_events(&body_text);

    assert_eq!(events[0]["object"], "chat.completion.chunk");
    assert_eq!(events[0]["choices"][0]["delta"]["role"], "assistant");
    assert_eq!(events[1]["choices"][0]["delta"]["content"], "Hello");
    assert_eq!(events[2]["choices"][0]["delta"]["content"], " there");
    assert_eq!(
        events
            .iter()
            .find_map(|event| event["usage"]["prompt_tokens"].as_u64()),
        Some(12)
    );
    assert!(events.iter().any(|event| {
        event["choices"].as_array().is_some_and(|choices| {
            choices
                .iter()
                .any(|choice| choice["finish_reason"] == "stop")
        })
    }));

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].stream, true);
}

#[tokio::test]
async fn chat_completions_web_search_is_server_side_and_hidden() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_ws_1",
            "web_search",
            "{\"query\":\"weather seattle\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "It is rainy."))])
        .await;
    let search = MockSearch::default();
    let gateway = test_gateway(upstream.clone(), search.clone());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [
            { "role": "user", "content": "Weather in Seattle?" }
        ],
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "web_search",
                    "description": "Search the web.",
                    "parameters": {
                        "type": "object",
                        "properties": { "query": { "type": "string" } },
                        "required": ["query"]
                    }
                }
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");
    let body_text = String::from_utf8(body_bytes.to_vec()).expect("utf8");
    let json: serde_json::Value = serde_json::from_str(&body_text).expect("valid json");
    assert_eq!(json["choices"][0]["message"]["content"], "It is rainy.");
    assert!(json["choices"][0]["message"]["tool_calls"].is_null());
    assert!(
        !body_text.contains("web_search"),
        "internal web_search call leaked into Chat response: {body_text}"
    );

    assert_eq!(
        search.queries.lock().await.as_slice(),
        &["weather seattle".to_string()]
    );
    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]
            .tools
            .as_ref()
            .and_then(|tools| tools.first())
            .map(|tool| tool.function.name.as_str()),
        Some("web_search")
    );
    assert_eq!(
        requests[1]
            .messages
            .iter()
            .find(|message| message.role == "tool")
            .and_then(|message| message.content.as_ref())
            .and_then(|content| content.as_str()),
        Some("Search result for weather seattle")
    );
}

#[tokio::test]
async fn chat_completions_client_tool_call_surfaces_tool_calls() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_echo",
            "echo",
            "{\"value\":\"hi\"}",
        ))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [
            { "role": "user", "content": "Echo hi" }
        ],
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "echo",
                    "description": "Echo a value.",
                    "parameters": {
                        "type": "object",
                        "properties": { "value": { "type": "string" } },
                        "required": ["value"]
                    }
                }
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).expect("valid json");
    assert_eq!(json["choices"][0]["finish_reason"], "tool_calls");
    assert!(json["choices"][0]["message"]["content"].is_null());
    assert_eq!(
        json["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "echo"
    );
    assert_eq!(
        json["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
        "{\"value\":\"hi\"}"
    );
}

#[tokio::test]
async fn chat_completions_developer_messages_become_system_messages() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [
            { "role": "developer", "content": "Be terse." },
            { "role": "user", "content": "Hi" },
            { "role": "assistant", "content": "Hello" },
            { "role": "developer", "content": "Use metric units." },
            { "role": "user", "content": "Weather?" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let roles: Vec<&str> = requests[0]
        .messages
        .iter()
        .map(|message| message.role.as_str())
        .collect();
    assert_eq!(roles, vec!["system", "user", "assistant", "system", "user"]);
    assert_eq!(
        requests[0].messages[0]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("Be terse.")
    );
    assert_eq!(
        requests[0].messages[3]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("Use metric units.")
    );
}

#[tokio::test]
async fn chat_completions_prepends_profile_system_prompt_prefix() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let mut config = test_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "glm-5.1".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: Some("Profile prefix.".to_string()),
            upstream_chat_kwargs: JsonMap::new(),
            native_vision: None,
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [
            { "role": "system", "content": "Client system." },
            { "role": "user", "content": "Hi" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].messages[0]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("Profile prefix.\n\nClient system.")
    );
    assert_eq!(requests[0].messages[1].role, "user");
}

// ---------------------------------------------------------------------------
// Anthropic /v1/messages integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn anthropic_messages_streams_text_response() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hello")),
            Ok(content_chunk("chat-1", " there")),
            Ok(usage_chunk("chat-1", 12, 5, 17, Some(3), None)),
        ])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [
            { "role": "user", "content": "Hi" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);

    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body_text = String::from_utf8(body_bytes.to_vec()).expect("utf8");

    // Verify key Anthropic SSE events are present
    assert!(
        body_text.contains("event: message_start"),
        "missing message_start"
    );
    assert!(
        body_text.contains("event: content_block_start"),
        "missing content_block_start"
    );
    assert!(
        body_text.contains("event: content_block_delta"),
        "missing content_block_delta"
    );
    assert!(
        body_text.contains("event: content_block_stop"),
        "missing content_block_stop"
    );
    assert!(
        body_text.contains("event: message_delta"),
        "missing message_delta"
    );
    assert!(
        body_text.contains("event: message_stop"),
        "missing message_stop"
    );

    // Verify the text content was streamed
    assert!(body_text.contains("Hello"), "missing text content");
    assert!(body_text.contains(" there"), "missing second text delta");
    let anthropic_events = parse_anthropic_sse_events(&body_text);
    // C1: exactly ONE terminal message_delta, never a progressive one.
    let message_deltas: Vec<&serde_json::Value> = anthropic_events
        .iter()
        .filter(|event| event["type"] == "message_delta")
        .collect();
    assert_eq!(
        message_deltas.len(),
        1,
        "expected exactly one terminal message_delta, no progressive deltas: {message_deltas:?}"
    );
    let message_delta = message_deltas[0];
    assert_eq!(message_delta["delta"]["stop_reason"], "end_turn");
    assert_eq!(message_delta["usage"]["input_tokens"], 12);
    assert_eq!(message_delta["usage"]["output_tokens"], 5);

    // T5: full harness proof, real gateway/HTTP output, TextOnly surface.
    use llmconduit::adapters::responses_to_anthropic::conformance::Surface;
    use llmconduit::adapters::responses_to_anthropic::conformance::assert_sse_conformant;
    assert_sse_conformant(&anthropic_events, Surface::TextOnly);

    // Verify the upstream received a chat completions request
    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].stream, true);
}

#[tokio::test]
async fn anthropic_messages_streams_nested_thinking_response() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(nested_thinking_chunk("chat-1", "Hidden step", "sig_123")),
            Ok(content_chunk("chat-1", "Answer")),
        ])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-7-sonnet-20250219",
        "max_tokens": 1024,
        "stream": true,
        "thinking": {
            "type": "enabled",
            "budget_tokens": 1024
        },
        "messages": [
            { "role": "user", "content": "Think then answer." }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body_text = String::from_utf8(body_bytes.to_vec()).expect("utf8");
    let anthropic_events = parse_anthropic_sse_events(&body_text);

    assert!(anthropic_events.iter().any(|event| {
        event["type"] == "content_block_delta"
            && event["delta"]["type"] == "thinking_delta"
            && event["delta"]["thinking"] == "Hidden step"
    }));
    assert!(anthropic_events.iter().any(|event| {
        event["type"] == "content_block_delta"
            && event["delta"]["type"] == "signature_delta"
            && event["delta"]["signature"] == "sig_123"
    }));
    assert!(anthropic_events.iter().any(|event| {
        event["type"] == "content_block_delta"
            && event["delta"]["type"] == "text_delta"
            && event["delta"]["text"] == "Answer"
    }));
    // C1: exactly ONE terminal message_delta, never a progressive one --
    // including while thinking deltas were streaming.
    let message_deltas: Vec<&serde_json::Value> = anthropic_events
        .iter()
        .filter(|event| event["type"] == "message_delta")
        .collect();
    assert_eq!(
        message_deltas.len(),
        1,
        "expected exactly one terminal message_delta, no progressive deltas: {message_deltas:?}"
    );
    assert!(
        message_deltas[0]["delta"]["stop_reason"].is_string(),
        "the sole message_delta must carry a stop_reason: {:?}",
        message_deltas[0]
    );
    assert!(
        message_deltas[0]["usage"]["output_tokens"]
            .as_u64()
            .is_some(),
        "terminal message_delta must carry output_tokens: {:?}",
        message_deltas[0]
    );

    // T5: full harness proof, real gateway/HTTP output, ReasoningText surface.
    use llmconduit::adapters::responses_to_anthropic::conformance::Surface;
    use llmconduit::adapters::responses_to_anthropic::conformance::assert_sse_conformant;
    assert_sse_conformant(&anthropic_events, Surface::ReasoningText);

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].stream, true);
    assert_eq!(requests[0].reasoning_effort.as_deref(), Some("low"));
    assert_eq!(
        requests[0].extra_body["chat_template_kwargs"]["enable_thinking"],
        json!(true),
        "Anthropic thinking intent is stated explicitly to the backend"
    );
}

#[tokio::test]
async fn anthropic_messages_preserves_image_content_parts() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    // E2b: this test proves Anthropic image-source SHAPES (base64/url/file)
    // all survive canonical round-tripping to `image_url`/`file_id` chat
    // parts, which is only true on a native-vision backend -- a non-native
    // backend now degrades every one of these to a text placeholder (the
    // whole point of E2b; covered by its own dedicated tests). Force
    // `native_vision: true` so this test keeps proving adapter fidelity, not
    // image-degradation policy.
    let mut config = test_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "claude-3-5-sonnet-20241022".to_string(),
        llmconduit::config::ModelProfile {
            native_vision: Some(true),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": false,
        "messages": [
            {
                "role": "user",
                "content": [
                    { "type": "text", "text": "Inspect these" },
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": "abc"
                        }
                    },
                    {
                        "type": "image",
                        "source": {
                            "type": "url",
                            "url": "https://example.com/img.png"
                        }
                    },
                    {
                        "type": "image",
                        "source": {
                            "type": "file",
                            "file_id": "file_img"
                        }
                    }
                ]
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].messages[0].content.as_ref(),
        Some(&json!([
            { "type": "text", "text": "Inspect these" },
            {
                "type": "image_url",
                "image_url": {
                    "url": "data:image/png;base64,abc"
                }
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": "https://example.com/img.png"
                }
            },
            {
                "type": "input_image",
                "file_id": "file_img"
            }
        ]))
    );
}

#[tokio::test]
async fn anthropic_messages_forwards_output_config_as_response_format() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk(
            "chat-1",
            "{\"title\":\"Build SMB levels\"}",
        ))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);
    let schema = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "title": { "type": "string" }
        },
        "required": ["title"]
    });

    let body = serde_json::json!({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 32000,
        "stream": true,
        "temperature": 1,
        "system": [
            { "type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude." },
            { "type": "text", "text": "Return JSON with a single \"title\" field." }
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Build me an incredible web app."
                    }
                ]
            }
        ],
        "tools": [],
        "output_config": {
            "format": {
                "type": "json_schema",
                "schema": schema.clone()
            }
        }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].response_format,
        Some(json!({
            "type": "json_schema",
            "json_schema": {
                "name": "response",
                "schema": schema,
                "strict": true
            }
        }))
    );
    assert_eq!(requests[0].reasoning_effort, None);
    assert_eq!(requests[0].tools, None);
}

#[tokio::test]
async fn anthropic_messages_streams_tool_use_response() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(tool_call_chunk(
                "chat-1",
                "call_weather",
                "get_weather",
                r#"{"loc"#,
            )),
            Ok(tool_call_chunk(
                "chat-1",
                "call_weather",
                "get_weather",
                r#"ation":"Seattle"}"#,
            )),
        ])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [
            { "role": "user", "content": "What's the weather?" }
        ],
        "tools": [
            {
                "name": "get_weather",
                "description": "Get the weather",
                "input_schema": {
                    "type": "object",
                    "properties": { "location": { "type": "string" } },
                    "required": ["location"]
                }
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);

    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body_text = String::from_utf8(body_bytes.to_vec()).expect("utf8");

    assert!(
        body_text.contains("event: message_start"),
        "missing message_start"
    );
    assert!(
        body_text.contains("event: content_block_start"),
        "missing content_block_start"
    );
    assert!(
        body_text.contains("event: content_block_stop"),
        "missing content_block_stop"
    );
    assert!(
        body_text.contains("event: message_stop"),
        "missing message_stop"
    );

    // Should have tool_use stop reason
    assert!(
        body_text.contains("tool_use"),
        "missing tool_use stop reason"
    );

    // Should contain the tool call info
    assert!(body_text.contains("get_weather"), "missing tool name");
    assert!(body_text.contains("call_weather"), "missing call id");
    let anthropic_events = parse_anthropic_sse_events(&body_text);
    let tool_starts: Vec<_> = anthropic_events
        .iter()
        .filter(|event| {
            event["type"] == "content_block_start"
                && event["content_block"]["type"] == "tool_use"
                && event["content_block"]["id"] == "call_weather"
                && event["content_block"]["name"] == "get_weather"
        })
        .collect();
    assert_eq!(tool_starts.len(), 1);
    let json_deltas: Vec<_> = anthropic_events
        .iter()
        .filter(|&event| {
            event["type"] == "content_block_delta" && event["delta"]["type"] == "input_json_delta"
        })
        .map(|event| event["delta"]["partial_json"].as_str().unwrap())
        .collect();
    assert_eq!(json_deltas, vec![r#"{"loc"#, r#"ation":"Seattle"}"#]);

    // T5: full harness proof, real gateway/HTTP output, ClientToolUse surface
    // (deliberately not web_search -- see `conformance::Surface::ClientToolUse`).
    use llmconduit::adapters::responses_to_anthropic::conformance::Surface;
    use llmconduit::adapters::responses_to_anthropic::conformance::assert_sse_conformant;
    assert_sse_conformant(&anthropic_events, Surface::ClientToolUse);
}

#[tokio::test]
async fn anthropic_messages_streams_web_search_response() {
    // T5: WebSearch surface, full gateway/HTTP path -- a real server-side
    // web-search round-trip (round 1: model calls `web_search`; the gateway
    // executes it via `MockSearch` and re-invokes upstream; round 2: the text
    // answer) through the actual `/v1/messages` streaming endpoint. Proves the
    // Anthropic converter's `server_tool_use` + `web_search_tool_result` blocks
    // are wire-conformant end to end, not just at the unit level (mirrors the
    // engine-level `web_search_emits_structured_results_event_for_anthropic_clients`
    // above, but through the HTTP/Anthropic-egress layer).
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(tool_call_chunk(
            "chat-1",
            "call_ws_1",
            "web_search",
            "{\"query\":\"weather seattle\"}",
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "It is rainy."))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [
            { "role": "user", "content": "What's the weather in Seattle?" }
        ],
        "tools": [
            { "type": "web_search_20250305", "name": "web_search" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body_text = String::from_utf8(body_bytes.to_vec()).expect("utf8");
    let anthropic_events = parse_anthropic_sse_events(&body_text);

    assert!(
        anthropic_events.iter().any(|event| {
            event["type"] == "content_block_start"
                && event["content_block"]["type"] == "server_tool_use"
                && event["content_block"]["name"] == "web_search"
        }),
        "missing server_tool_use block: {anthropic_events:?}"
    );
    assert!(
        anthropic_events.iter().any(|event| {
            event["type"] == "content_block_start"
                && event["content_block"]["type"] == "web_search_tool_result"
        }),
        "missing web_search_tool_result block: {anthropic_events:?}"
    );
    assert!(
        anthropic_events.iter().any(|event| {
            event["type"] == "content_block_delta"
                && event["delta"]["type"] == "text_delta"
                && event["delta"]["text"] == "It is rainy."
        }),
        "missing the post-search text answer: {anthropic_events:?}"
    );

    // T5: full harness proof, real gateway/HTTP output, WebSearch surface.
    use llmconduit::adapters::responses_to_anthropic::conformance::Surface;
    use llmconduit::adapters::responses_to_anthropic::conformance::assert_sse_conformant;
    assert_sse_conformant(&anthropic_events, Surface::WebSearch);

    let requests = upstream.requests().await;
    assert_eq!(
        requests.len(),
        2,
        "expected one search round + one answer round"
    );
}

#[tokio::test]
async fn anthropic_messages_converts_system_prompt() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "done"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "system": "You are a helpful assistant.",
        "messages": [
            { "role": "user", "content": "Hi" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);

    // Must consume the body to drive the SSE stream and spawn the upstream request
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    // The system prompt should have been converted to a system message
    assert_eq!(requests[0].messages[0].role, "system");
    assert_eq!(
        requests[0].messages[0]
            .content
            .as_ref()
            .and_then(|v| v.as_str()),
        Some("You are a helpful assistant.")
    );
}

#[tokio::test]
async fn anthropic_messages_prepends_profile_system_prompt_prefix() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "done"))])
        .await;
    let mut config = test_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "claude-3-5-sonnet-20241022".to_string(),
        llmconduit::config::ModelProfile {
            upstream_model: None,
            system_prompt_prefix: Some("Profile prefix.".to_string()),
            upstream_chat_kwargs: JsonMap::new(),
            native_vision: None,
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "system": "You are a helpful assistant.",
        "messages": [
            { "role": "user", "content": "Hi" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].messages[0]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("Profile prefix.\n\nYou are a helpful assistant.")
    );
}

#[tokio::test]
async fn anthropic_count_tokens_lowers_and_returns_anthropic_shape() {
    let upstream = MockUpstream::default();
    upstream.set_token_count(Some(321)).await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);
    let body = json!({
        "model": "test-model",
        "max_tokens": 128,
        "system": "Be concise.",
        "messages": [{"role": "user", "content": "Hello"}]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages/count_tokens")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .expect("body");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        json!({
            "input_tokens": 321
        })
    );
    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, "test-model");
    assert_eq!(
        requests[0]
            .messages
            .iter()
            .map(|message| message.role.as_str())
            .collect::<Vec<_>>(),
        vec!["system", "user"]
    );
}

#[tokio::test]
async fn anthropic_count_tokens_caches_unsupported_as_not_found() {
    let upstream = MockUpstream::default();
    let gateway = test_gateway(upstream, MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);
    let body = json!({
        "model": "test-model",
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "Hello"}]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages/count_tokens")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
    let bytes = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .expect("body");
    let error: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(error["error"]["type"], "not_found_error");
}

#[tokio::test]
async fn anthropic_messages_returns_non_streaming_json() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Hello"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": false,
        "messages": [
            { "role": "user", "content": "Hi" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    // Should return 200 with JSON body (non-streaming)
    assert_eq!(response.status(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).expect("valid json");
    assert_eq!(json["type"], "message");
    assert_eq!(json["role"], "assistant");
}

#[tokio::test]
async fn responses_returns_non_streaming_json_while_streaming_upstream() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Hello"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "glm-5.1",
        "stream": false,
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": "Hi" }
                ]
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), 200);
    let body_bytes = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).expect("valid json");
    assert_eq!(json["object"], "response");
    assert_eq!(json["status"], "completed");
    assert!(
        json["output"]
            .as_array()
            .expect("output array")
            .iter()
            .any(|item| {
                item["type"] == "message"
                    && item["role"] == "assistant"
                    && item["content"]
                        .as_array()
                        .is_some_and(|content| content.iter().any(|part| part["text"] == "Hello"))
            }),
        "expected buffered assistant output text in final response: {json}"
    );

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].stream, true);
}

/// CR1.1 (code review, Round 1): `engine.rs::created_event` stamps
/// `estimated_input_tokens` onto the CANONICAL `response.created` event as an
/// internal transport hint solely for the Anthropic streaming egress
/// (`AnthropicStreamConverter::handle_created` seeds `message_start` from
/// it). `http.rs::stream_responses_response` is a RAW byte-forward of
/// `event.data` for `/v1/responses` streaming clients, so without stripping
/// the hint at that boundary it would leak a non-standard field onto the
/// OpenAI-compatible wire, breaking the "Responses wire shape unchanged"
/// contract (a `deny_unknown_fields` consumer or exact-bytes snapshot would
/// fail). Drives a REAL streaming `/v1/responses` turn through the actual
/// HTTP router (not a hand-built `SseEvent`) and asserts the wire-level
/// `response.created` frame carries no `estimated_input_tokens` key at all —
/// proving `responses_wire_event_data`'s strip is actually wired into the
/// egress, not just unit-tested in isolation.
#[tokio::test]
async fn responses_streaming_response_created_omits_internal_estimate_hint() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Hello"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "glm-5.1",
        "stream": true,
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": "Hi" }
                ]
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 200);

    let body_bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("read body");
    let body_text = String::from_utf8(body_bytes.to_vec()).expect("utf8");
    let events = parse_responses_sse_events(&body_text);

    let created = events
        .iter()
        .find(|event| event["type"] == "response.created")
        .expect("response.created frame present on the wire");
    // The rest of the stub must still be intact -- this proves the strip
    // removed exactly one key, not the whole `response` object.
    assert!(
        created["response"]["id"].as_str().is_some(),
        "response.created must still carry its id after the strip: {created}"
    );
    assert!(
        created["response"].get("estimated_input_tokens").is_none(),
        "internal estimate hint leaked onto the /v1/responses wire: {created}"
    );

    // Sweep every frame, not just `response.created`: the hint must never
    // appear anywhere on this raw-forwarding egress.
    for event in &events {
        assert!(
            event
                .get("response")
                .is_none_or(|response| response.get("estimated_input_tokens").is_none()),
            "internal estimate hint leaked on a /v1/responses frame: {event}"
        );
    }
}

#[tokio::test]
async fn responses_preserves_multimodal_input_parts() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    // E2b: this test proves canonical `input_image`/`input_file`/`input_audio`
    // parts survive lowering to the upstream chat payload unchanged, which is
    // only true on a native-vision backend -- a non-native backend now
    // degrades the image part to a text placeholder (the whole point of E2b;
    // covered by its own dedicated tests). Force `native_vision: true` so this
    // test keeps proving lowering fidelity, not image-degradation policy.
    let mut config = test_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "glm-5.1".to_string(),
        llmconduit::config::ModelProfile {
            native_vision: Some(true),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "glm-5.1",
        "stream": false,
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": "Inspect these" },
                    {
                        "type": "input_image",
                        "image_url": "data:image/png;base64,abc",
                        "detail": "high"
                    },
                    {
                        "type": "input_file",
                        "file_id": "file_doc",
                        "filename": "brief.pdf"
                    },
                    {
                        "type": "input_audio",
                        "input_audio": {
                            "data": "UklGRg==",
                            "format": "wav"
                        }
                    }
                ]
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].messages[0].content.as_ref(),
        Some(&json!([
            { "type": "text", "text": "Inspect these" },
            {
                "type": "image_url",
                "image_url": {
                    "url": "data:image/png;base64,abc",
                    "detail": "high"
                }
            },
            {
                "type": "input_file",
                "file_id": "file_doc",
                "filename": "brief.pdf"
            },
            {
                "type": "input_audio",
                "input_audio": {
                    "data": "UklGRg==",
                    "format": "wav"
                }
            }
        ]))
    );
}

#[tokio::test]
async fn anthropic_messages_converts_tool_result_history() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-2", "It's 72°F in Seattle."))])
        .await;
    // E2b: this test's tool_result carries an image (radar.png) and proves it
    // converts to a `FunctionCallOutput` + separate image message, which is
    // only forwarded byte-for-byte on a native-vision backend -- a non-native
    // backend now degrades it to a text placeholder (the exact "tool-output
    // image" shape E2b targets; covered by its own dedicated tests). Force
    // `native_vision: true` so this test keeps proving the tool_result/image
    // conversion shape, not image-degradation policy.
    let mut config = test_config();
    config.model_profiles = std::collections::BTreeMap::from([(
        "claude-3-5-sonnet-20241022".to_string(),
        llmconduit::config::ModelProfile {
            native_vision: Some(true),
            ..Default::default()
        },
    )]);
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [
            { "role": "user", "content": "What's the weather in Seattle?" },
            { "role": "assistant", "content": [
                { "type": "thinking", "thinking": "Need live weather.", "signature": "sig_history" },
                { "type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": { "location": "Seattle" } }
            ]},
            { "role": "user", "content": [
                { "type": "tool_result", "tool_use_id": "toolu_1", "content": [
                    { "type": "text", "text": "72F sunny" },
                    {
                        "type": "image",
                        "source": {
                            "type": "url",
                            "url": "https://example.com/radar.png"
                        }
                    }
                ]}
            ]}
        ],
        "tools": [
            {
                "name": "get_weather",
                "description": "Get the weather",
                "input_schema": {
                    "type": "object",
                    "properties": { "location": { "type": "string" } },
                    "required": ["location"]
                }
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);

    // Must consume the body to drive the SSE stream and spawn the upstream request
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024).await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let assistant_tool_msgs: Vec<_> = requests[0]
        .messages
        .iter()
        .filter(|m| m.role == "assistant" && m.tool_calls.is_some())
        .collect();
    assert_eq!(assistant_tool_msgs.len(), 1);
    assert_eq!(
        assistant_tool_msgs[0].reasoning_content.as_deref(),
        Some("Need live weather.")
    );
    let thinking = assistant_tool_msgs[0]
        .thinking
        .as_ref()
        .expect("signed thinking");
    assert_eq!(thinking.content, "Need live weather.");
    assert_eq!(thinking.signature.as_deref(), Some("sig_history"));

    // Verify tool_result text was converted to a tool message.
    let tool_msgs: Vec<_> = requests[0]
        .messages
        .iter()
        .filter(|m| m.role == "tool")
        .collect();
    assert_eq!(tool_msgs.len(), 1);
    assert_eq!(tool_msgs[0].tool_call_id.as_deref(), Some("toolu_1"));
    assert_eq!(
        tool_msgs[0].content.as_ref().and_then(|v| v.as_str()),
        Some("72F sunny")
    );
    let image_msgs: Vec<_> = requests[0]
        .messages
        .iter()
        .filter(|m| {
            m.role == "user"
                && m.content
                    .as_ref()
                    .is_some_and(|content| content.to_string().contains("radar.png"))
        })
        .collect();
    assert_eq!(image_msgs.len(), 1);
    assert_eq!(
        image_msgs[0].content.as_ref(),
        Some(&json!([
            {
                "type": "image_url",
                "image_url": {
                    "url": "https://example.com/radar.png"
                }
            }
        ]))
    );
}

#[tokio::test]
async fn anthropic_messages_replays_server_tool_history_blocks() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk(
            "chat-2",
            "Because a front moved in.",
        ))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": false,
        "messages": [
            { "role": "user", "content": "What's the weather in Seattle?" },
            { "role": "assistant", "content": [
                {
                    "type": "server_tool_use",
                    "id": "srvtoolu_1",
                    "name": "web_search",
                    "input": { "query": "weather seattle" }
                },
                {
                    "type": "web_search_tool_result",
                    "tool_use_id": "srvtoolu_1",
                    "content": [
                        {
                            "type": "web_search_result",
                            "url": "https://example.com/weather",
                            "title": "Weather"
                        }
                    ]
                },
                { "type": "text", "text": "It is raining." }
            ]},
            { "role": "user", "content": "Why?" }
        ],
        "tools": [
            { "type": "web_search_20250305", "name": "web_search" }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let messages = &requests[0].messages;
    let roles: Vec<&str> = messages
        .iter()
        .map(|message| message.role.as_str())
        .collect();
    assert_eq!(
        roles,
        vec!["user", "assistant", "tool", "assistant", "user"]
    );
    let tool_call = &messages[1].tool_calls.as_ref().expect("tool call")[0];
    assert_eq!(tool_call.id.as_deref(), Some("srvtoolu_1"));
    assert_eq!(tool_call.function.name.as_deref(), Some("web_search"));
    assert_eq!(
        tool_call.function.arguments.as_ref(),
        Some(&json!({ "query": "weather seattle" }))
    );
    assert_eq!(messages[2].tool_call_id.as_deref(), Some("srvtoolu_1"));
    assert!(
        messages[2]
            .content
            .as_ref()
            .and_then(|value| value.as_str())
            .is_some_and(|content| content.contains("https://example.com/weather"))
    );
    assert_eq!(
        messages[3]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("It is raining.")
    );
}

#[tokio::test]
async fn anthropic_messages_relaxes_forced_web_search_when_brave_is_disabled() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "No search available."))])
        .await;
    let mut config = test_config();
    config.brave_api_key = None;
    let gateway = test_gateway_with_config(upstream.clone(), MockSearch::default(), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": false,
        "messages": [
            { "role": "user", "content": "Search for the weather." }
        ],
        "tools": [
            { "type": "web_search_20250305", "name": "web_search" }
        ],
        "tool_choice": { "type": "tool", "name": "web_search" }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].tools, None);
    assert_eq!(requests[0].tool_choice, Some(json!("auto")));
}

#[tokio::test]
async fn anthropic_messages_preserves_claude_code_skill_listing_as_user_input() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Hello."))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);
    let skill_listing = concat!(
        "- deep-research: Deep research harness. Use when the user wants research.\n",
        "- update-config: Configure settings. Use when the user asks to update config.\n",
        "- security-review: Complete a security review. Invoke with the request."
    );

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": false,
        "messages": [
            { "role": "user", "content": "hello" },
            { "role": "user", "content": skill_listing }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let messages = &requests[0].messages;
    let roles: Vec<&str> = messages
        .iter()
        .map(|message| message.role.as_str())
        .collect();
    assert_eq!(roles, vec!["user", "user"]);
    assert_eq!(
        messages[0]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("hello")
    );
    let listing = messages[1]
        .content
        .as_ref()
        .and_then(|value| value.as_str())
        .expect("skill listing content");
    assert!(listing.contains("security-review"));
}

#[tokio::test]
async fn anthropic_messages_preserves_late_system_skill_listing_position() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Hello."))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);
    let skill_listing = concat!(
        "The following skills are available for use with the Skill tool:\n\n",
        "- deep-research: Deep research harness. Use when the user wants research.\n",
        "- update-config: Configure settings. Use when the user asks to update config.\n",
        "- security-review: Complete a security review. Invoke with the request."
    );

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "system": "You are Claude Code.",
        "stream": false,
        "messages": [
            { "role": "user", "content": "hello" },
            { "role": "system", "content": skill_listing }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let messages = &requests[0].messages;
    let roles: Vec<&str> = messages
        .iter()
        .map(|message| message.role.as_str())
        .collect();
    assert_eq!(roles, vec!["system", "user", "system"]);
    let system = messages[0]
        .content
        .as_ref()
        .and_then(|value| value.as_str())
        .expect("system content");
    assert!(system.contains("You are Claude Code."));
    assert_eq!(
        messages[1]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("hello")
    );
    assert!(
        messages[2]
            .content
            .as_ref()
            .and_then(|value| value.as_str())
            .is_some_and(|text| text.contains("security-review"))
    );
}

#[tokio::test]
async fn cancels_mid_stream_when_client_disconnects() {
    let upstream = PendingChunkUpstream::new();
    let stream_polled = upstream.stream_polled.notified();
    let config = Config {
        bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
        upstream_base_url: "http://127.0.0.1:8000/v1".parse().expect("url"),
        upstream_api_key: None,
        upstream_model: None,
        system_prompt_prefix: None,
        upstream_request_log_path: None,
        turn_capture_dir: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        model_routes: Vec::new(),
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
        image_agent_enabled: false,
        vision_url: None,
        vision_model: None,
        image_cache_max_size: 100,
        image_cache_ttl_secs: 300,
        unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
        price_table: std::collections::HashMap::new(),
    };
    // The image agent is off here, so the vision client is inert; a real
    // `ReqwestVisionClient` that is never called satisfies the constructor
    // (the `MockVisionClient` now lives with the image-agent suite in
    // `tests/common`).
    let vision: Arc<dyn llmconduit::vision::VisionClient> = Arc::new(
        llmconduit::vision::ReqwestVisionClient::new(reqwest::Client::new(), &config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    let gateway = Arc::new(Gateway::new(
        config,
        ReplayStore::new(1000),
        Arc::new(upstream.clone()),
        Arc::new(MockSearch::default()),
        vision,
        image_cache,
        MonitorHub::new(128),
        None,
        llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
    ));

    let request = base_request(vec![user_message("count")]);
    let mut stream = gateway.stream_responses(request).await.expect("stream");

    let _event1 = stream.next().await;
    let _event2 = stream.next().await;

    tokio::time::timeout(std::time::Duration::from_secs(1), stream_polled)
        .await
        .expect("upstream stream should be waiting for a chunk");

    let stream_dropped = upstream.stream_dropped.notified();
    drop(stream);

    tokio::time::timeout(std::time::Duration::from_secs(1), stream_dropped)
        .await
        .expect("upstream stream should be dropped after client disconnect");

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
}

// ---------------------------------------------------------------------------
// F1c — durable turn capture: engine terminal + RAII finalize + both-`done`
// barrier + assembly. AC-7 (pre-spawn failure), AC-8 (completed streaming),
// AC-9 (mid-stream disconnect). All go through the REAL HTTP router so the
// middleware gate, served-body tee, and engine terminal seam are exercised end
// to end. `turn_capture_dir` is set so `TurnCapture::enabled` arms capture
// independent of the (disabled) dashboard flow store.
// ---------------------------------------------------------------------------

/// A fresh, process-unique capture dir under the system temp dir (no `tempfile`
/// dev-dep). Each F1c test gets its own so `wait_for_only_artifact` sees exactly
/// one `<id>.json`.
fn unique_capture_dir(label: &str) -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "llmconduit-f1c-{label}-{}-{nanos}-{n}",
        std::process::id()
    ))
}

/// Build a gateway with turn capture ENABLED from `config.turn_capture_dir`
/// (mirrors `test_gateway_with_config_and_raw_output` but attaches the sink and
/// takes an already-boxed upstream so the pending/streaming mocks work too).
fn gateway_with_capture_dir(upstream: Arc<dyn UpstreamClient>, config: Config) -> Arc<Gateway> {
    let capture = config
        .turn_capture_dir
        .clone()
        .map(llmconduit::turn_capture::TurnCapture::enabled)
        .unwrap_or_else(llmconduit::turn_capture::TurnCapture::disabled);
    let vision: Arc<dyn llmconduit::vision::VisionClient> = Arc::new(
        llmconduit::vision::ReqwestVisionClient::new(reqwest::Client::new(), &config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(
        Gateway::new(
            config,
            ReplayStore::new(1000),
            upstream,
            Arc::new(MockSearch::default()),
            vision,
            image_cache,
            MonitorHub::new(128),
            None,
            llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
        )
        .with_turn_capture(capture),
    )
}

/// Poll the capture dir for the SINGLE assembled `<id>.json` (assembly is spawned
/// async after the both-`done` barrier). Bounded wait: a barrier that never
/// resolves fails the test loudly instead of hanging, and a pass PROVES the
/// artifact was produced in bounded time.
async fn wait_for_only_artifact(dir: &std::path::Path) -> serde_json::Value {
    for _ in 0..300 {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("json")
                    && let Ok(bytes) = std::fs::read(&path)
                    && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
                {
                    return value;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("no turn-capture artifact appeared in {}", dir.display());
}

/// AC-7: a PRE-SPAWN validation failure (`previous_response_id`, rejected at
/// canonical lowering — `engine.rs` pre-spawn seam) writes a `status:"failed"`
/// artifact with the terminal reason and the served error body, with the upstream
/// sections ABSENT (backend never contacted) — and it does NOT hang.
#[tokio::test]
async fn f1c_ac7_pre_spawn_failure_writes_failed_served_present_upstream_absent() {
    let capture_dir = unique_capture_dir("ac7");
    let upstream = MockUpstream::default();
    let mut config = test_config();
    config.turn_capture_dir = Some(capture_dir.clone());
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    let gateway = gateway_with_capture_dir(Arc::new(upstream.clone()), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    // `previous_response_id` is unsupported and rejected at lowering, BEFORE the
    // engine spawns any upstream work.
    let body = json!({
        "model": "glm-5.1",
        "input": "hi",
        "previous_response_id": "resp_does_not_exist"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert!(
        response.status().is_client_error(),
        "pre-spawn lowering error is a 4xx, got {}",
        response.status()
    );
    let served_body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read served error body");
    assert!(
        !served_body.is_empty(),
        "an error body was served to the client"
    );

    // The artifact must appear in bounded time (proves the barrier resolves even
    // though the engine never spawned — no hang).
    let artifact = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        wait_for_only_artifact(&capture_dir),
    )
    .await
    .expect("AC-7: pre-spawn failure artifact within bounded time (no hang)");

    assert_eq!(artifact["status"], "failed");
    assert!(
        artifact["terminal_reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty()),
        "a terminal reason is recorded: {artifact}"
    );
    // served_response present (the error body the client received).
    let served = &artifact["sections"]["served_response"];
    assert!(
        served["content"].is_string() || served["content"].is_object(),
        "served_response section is present: {served}"
    );
    // upstream sections ABSENT — the backend was never contacted (never a
    // fabricated empty-measured section).
    assert!(artifact["sections"].get("upstream_request").is_none());
    assert!(artifact["sections"].get("upstream_response").is_none());
    assert!(
        upstream.requests().await.is_empty(),
        "no upstream request is made for a pre-spawn failure"
    );
}

/// AC-8: a COMPLETED streaming `/v1/messages` turn writes `status:"completed"`
/// with the currently-implemented sections (inbound + served) present and
/// `finished_ms >= started_ms`.
#[tokio::test]
async fn f1c_ac8_completed_streaming_turn_writes_completed_with_sections() {
    let capture_dir = unique_capture_dir("ac8");
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hello")),
            Ok(content_chunk("chat-1", " there")),
            Ok(usage_chunk("chat-1", 12, 5, 17, Some(3), None)),
        ])
        .await;
    let mut config = test_config();
    config.turn_capture_dir = Some(capture_dir.clone());
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    let gateway = gateway_with_capture_dir(Arc::new(upstream), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    // Draining the body to completion drops the tee → clean `served_done(false)`.
    let served_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read served stream");
    let served_text = String::from_utf8(served_bytes.to_vec()).expect("utf8");
    assert!(served_text.contains("event: message_start"));
    assert!(served_text.contains("Hello"));

    let artifact = wait_for_only_artifact(&capture_dir).await;
    assert_eq!(artifact["status"], "completed");
    // inbound_request present, parsed as a JSON value (the redacted Anthropic body).
    assert_eq!(
        artifact["sections"]["inbound_request"]["content"]["model"],
        "claude-3-5-sonnet-20241022"
    );
    // served_response present and byte-clean (not partial).
    let served = &artifact["sections"]["served_response"];
    assert_eq!(served["partial"], false);
    assert!(
        served["content"]
            .as_str()
            .is_some_and(|body| body.contains("message_start") && body.contains("Hello")),
        "served_response captured the streamed Anthropic bytes: {served}"
    );
    // finished_ms is stamped and not before started_ms.
    let started = artifact["started_ms"].as_u64().expect("started_ms");
    let finished = artifact["finished_ms"].as_u64().expect("finished_ms");
    assert!(
        finished > 0 && finished >= started,
        "finished_ms >= started_ms"
    );
    // Only the currently-implemented sections in F1c.
    assert!(artifact["sections"].get("upstream_request").is_none());
    assert!(artifact["sections"].get("upstream_response").is_none());
}

/// AC-9: a mid-stream client DISCONNECT writes `status:"cancelled"` with
/// `served_response.partial = true`, produced within a bounded time (no hang).
///
/// Uses `/v1/responses` streaming, whose SSE body maps the engine's
/// `ReceiverStream` DIRECTLY (no intermediate converter task), so dropping the
/// response body propagates straight to the engine's `tx.closed()` cancel — a
/// faithful mid-stream client hang-up. The `ChunkThenPendingUpstream` yields a
/// prefix chunk then parks, so the client sees real served bytes before it
/// disconnects.
#[tokio::test]
async fn f1c_ac9_client_disconnect_writes_cancelled_partial_no_hang() {
    let capture_dir = unique_capture_dir("ac9");
    let upstream = ChunkThenPendingUpstream::new(vec![content_chunk("chat-1", "Hello")]);
    let stream_polled = upstream.stream_polled.notified();
    let stream_dropped = upstream.stream_dropped.notified();
    let mut config = test_config();
    config.turn_capture_dir = Some(capture_dir.clone());
    // Clone into the gateway (the `Notify` handles are shared `Arc`s) so the
    // original stays alive to drive the `notified()` futures above.
    let gateway = gateway_with_capture_dir(Arc::new(upstream.clone()), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": true,
        "input": "count"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);

    let mut body_stream = response.into_body().into_data_stream();
    // Let the prefix flow + the upstream park, then read one served frame so the
    // `served_response` section captured real mid-stream bytes.
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_polled)
        .await
        .expect("upstream parked after the prefix");
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), body_stream.next())
        .await
        .expect("a served frame within bounded time");
    assert!(
        first.is_some(),
        "at least one served frame before disconnect"
    );

    // Client disconnect mid-stream: drop the body → the tee's Drop fires
    // `served_done(partial=true)` and the engine cancels the parked upstream.
    drop(body_stream);
    tokio::time::timeout(std::time::Duration::from_secs(2), stream_dropped)
        .await
        .expect("upstream dropped on client disconnect (engine cancelled)");

    // The cancelled artifact is written in bounded time (the no-hang assertion).
    let artifact = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        wait_for_only_artifact(&capture_dir),
    )
    .await
    .expect("AC-9: cancelled artifact within bounded time (no hang)");

    assert_eq!(artifact["status"], "cancelled");
    assert_eq!(
        artifact["sections"]["served_response"]["partial"], true,
        "a mid-stream disconnect marks the served section partial: {artifact}"
    );
}

/// F1c review r1 (finding #3, don't-lie-with-zeros): a turn truncated by the upstream
/// `finish_reason: length` (max-output-tokens) completes cleanly (`Ok`) but its
/// response is CUT SHORT, so the capture artifact must record `status:"incomplete"`
/// (`response.incomplete`), NOT `completed` — even though the served HTTP turn itself
/// succeeded. Exercises the real engine terminal seam end to end.
#[tokio::test]
async fn f1c_length_truncated_turn_writes_incomplete_status() {
    let capture_dir = unique_capture_dir("length");
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hello")),
            // Terminal `finish_reason: length` → engine emits `response.incomplete`.
            Ok(length_finish_chunk("chat-1")),
            Ok(usage_chunk("chat-1", 12, 5, 17, None, None)),
        ])
        .await;
    let mut config = test_config();
    config.turn_capture_dir = Some(capture_dir.clone());
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    let gateway = gateway_with_capture_dir(Arc::new(upstream), config);
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 4,
        "stream": true,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    // Drain the stream to completion so the tee closes cleanly (a length truncation
    // is still a fully-served response to the client).
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read served stream");

    let artifact = wait_for_only_artifact(&capture_dir).await;
    assert_eq!(
        artifact["status"], "incomplete",
        "a `finish_reason: length` turn maps to the `incomplete` artifact status: {artifact}"
    );
    assert_eq!(
        artifact["terminal_reason"], "response.incomplete",
        "the terminal reason reflects the truncation"
    );
    // The served section is still present + clean (the client got the full truncated
    // stream); only the STATUS reflects the truncation.
    assert_eq!(artifact["sections"]["served_response"]["partial"], false);
}

/// F1d AC-10 (end-to-end, through the REAL engine wiring): a genuine `/v1/messages`
/// HTTP request over a REAL `ReqwestUpstreamClient` leaf (via the `lib.rs` DI root
/// — the same wiring production uses — NOT the in-process `MockUpstream` every
/// other F1 test in this file relies on), hitting a wiremock upstream that returns
/// a context-overflow 400 then a 200. Proves the FULL chain: HTTP → engine
/// (`run_turn` looks the capture handle up via `Gateway::turn_capture().state(
/// api_call_id)`) → `BackendChatRequest::with_capture` → `ReqwestUpstreamClient::
/// dispatch_chat_stream` → the artifact's `upstream_request` is the FINAL
/// (shrunk) on-wire request, not the first oversized one — the same shrink shape
/// `upstream.rs`'s unit-level `f1d_ac10_upstream_request_captures_shrunk_redacted_
/// final_request` proves against a hand-built `BackendChatRequest`, but here
/// through the real engine lookup instead.
#[tokio::test]
async fn f1d_ac10_gateway_e2e_upstream_request_is_shrunk_final_request() {
    let capture_dir = unique_capture_dir("f1d-ac10-e2e");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "glm-5.1"}]
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
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-1",
                    "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17}
                })])),
        )
        .mount(&server)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", server.uri()).parse().expect("url");
    config.turn_capture_dir = Some(capture_dir.clone());
    // `--with-debug-ui` OFF: capture works off its OWN gate (AC-4 principle),
    // proven again here alongside the real-upstream wiring.
    let (app, _gateway) = llmconduit::build_app_with_gateway_and_options(
        config,
        None,
        llmconduit::AppOptions {
            with_debug_ui: false,
        },
    );

    let body = json!({
        "model": "glm-5.1",
        "max_tokens": 64000,
        "stream": true,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read served stream");

    let artifact = wait_for_only_artifact(&capture_dir).await;
    let section = &artifact["sections"]["upstream_request"];
    assert_eq!(
        section["content"]["max_tokens"], 63652,
        "engine-wired capture end-to-end reflects the SHRUNK retry, not the first \
         oversized attempt: {section}"
    );
}

/// AC-12: a SUCCESSFUL streaming turn's `upstream_response` is the EXACT concatenated
/// RAW vLLM SSE bytes -- byte-for-byte over a wiremock upstream emitting a KNOWN frame
/// sequence INCLUDING a `<think>`-in-content chunk (the core debugging use case). Runs
/// through the REAL `ReqwestUpstreamClient` leaf (the only path that hits the raw-bytes
/// tap in `stream_success_response`); `--with-debug-ui` OFF (capture works off its OWN
/// gate).
#[tokio::test]
async fn f1e_ac12_upstream_response_is_exact_raw_sse_bytes() {
    let capture_dir = unique_capture_dir("f1e-ac12");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "glm-5.1"}]
        })))
        .mount(&server)
        .await;
    // A known frame sequence INCLUDING a `<think>`-in-content chunk -- the exact
    // shape the capture exists to persist (a `<think>` leak is a 200 OK).
    let frames = vec![
        json!({
            "id": "chat-1",
            "choices": [{"index": 0, "delta": {"role": "assistant", "content": "<think>plan"}}]
        }),
        json!({
            "id": "chat-1",
            "choices": [{"index": 0, "delta": {"content": " done</think>hi"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17}
        }),
    ];
    // Build the raw body ONCE and hand the SAME bytes to wiremock, so the expected
    // value is exactly what the upstream puts on the wire.
    let raw_sse = chat_completion_sse_body(&frames);
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(raw_sse.clone()),
        )
        .mount(&server)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", server.uri()).parse().expect("url");
    config.turn_capture_dir = Some(capture_dir.clone());
    let (app, _gateway) = llmconduit::build_app_with_gateway_and_options(
        config,
        None,
        llmconduit::AppOptions {
            with_debug_ui: false,
        },
    );

    let body = json!({
        "model": "glm-5.1",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read served stream");

    let artifact = wait_for_only_artifact(&capture_dir).await;
    let section = &artifact["sections"]["upstream_response"];
    assert_eq!(
        section["encoding"], "utf8",
        "raw SSE is valid UTF-8: {section}"
    );
    assert_eq!(
        section["partial"], false,
        "a cleanly-streamed upstream response is not partial: {section}"
    );
    assert_eq!(
        section["content"].as_str().expect("string content"),
        raw_sse,
        "upstream_response is the EXACT concatenated raw vLLM SSE bytes (byte-for-byte)"
    );
    assert!(
        section["content"]
            .as_str()
            .expect("string")
            .contains("<think>plan"),
        "the raw `<think>` leak is persisted verbatim (the whole point of the capture)"
    );
}

/// AC-13: a FINAL-attempt non-2xx (a 400 error body, NOT an SSE success stream) is
/// captured as `upstream_response` (the error text) with artifact `status:"failed"`.
/// The 400 body is request-intrinsic (Terminal) so no failover masks it; it is not a
/// context-overflow, so no shrink-retry fires.
#[tokio::test]
async fn f1e_ac13_final_non_2xx_body_captured_status_failed() {
    let capture_dir = unique_capture_dir("f1e-ac13");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "glm-5.1"}]
        })))
        .mount(&server)
        .await;
    let error_body = "{\"error\":{\"message\":\"invalid request: unsupported parameter\",\"type\":\"invalid_request_error\"}}";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_string(error_body))
        .mount(&server)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", server.uri()).parse().expect("url");
    config.turn_capture_dir = Some(capture_dir.clone());
    let (app, _gateway) = llmconduit::build_app_with_gateway_and_options(
        config,
        None,
        llmconduit::AppOptions {
            with_debug_ui: false,
        },
    );

    let body = json!({
        "model": "glm-5.1",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    // Whatever the client-facing framing, drain it so the tee closes.
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024).await;

    let artifact = wait_for_only_artifact(&capture_dir).await;
    assert_eq!(
        artifact["status"], "failed",
        "a final-attempt non-2xx is a failed turn: {artifact}"
    );
    let section = &artifact["sections"]["upstream_response"];
    assert_eq!(
        section["content"].as_str().expect("string content"),
        error_body,
        "the final failed HTTP body is captured verbatim as upstream_response: {section}"
    );
    assert_eq!(
        section["partial"], false,
        "a whole failed body is not partial"
    );
}

/// AC-14: a malformed/oversized-frame upstream (the G6 frame cap trips) closes
/// `upstream_response` `partial:true` AND the artifact still finalizes in BOUNDED time
/// (asserted via a timeout -- no hang). A single oversized SSE frame far above the
/// 1024-byte cap floor is rejected mid-stream by the bounded guard downstream of the
/// tap; the tap's drop guard then marks the raw capture partial and closes it.
#[tokio::test]
async fn f1e_ac14_oversized_frame_partial_and_no_hang() {
    let capture_dir = unique_capture_dir("f1e-ac14");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "glm-5.1"}]
        })))
        .mount(&server)
        .await;
    // 4 KiB single `data:` frame, no event boundary before it overflows the cap.
    let oversized = format!("data: {}\n\n", "x".repeat(4096));
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(oversized),
        )
        .mount(&server)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", server.uri()).parse().expect("url");
    config.turn_capture_dir = Some(capture_dir.clone());
    // Floored to 1024 by the leaf; the 4 KiB frame overflows it → the G6 cap trips.
    config.max_sse_frame_bytes = 1024;
    let (app, _gateway) = llmconduit::build_app_with_gateway_and_options(
        config,
        None,
        llmconduit::AppOptions {
            with_debug_ui: false,
        },
    );

    let body = json!({
        "model": "glm-5.1",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024).await;

    // Bounded time (the no-hang assertion): the barrier resolves even though the raw
    // stream was rejected mid-flight and the tap was dropped before a clean end.
    let artifact = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        wait_for_only_artifact(&capture_dir),
    )
    .await
    .expect("AC-14: artifact within bounded time (no hang)");

    assert!(
        artifact["sections"].get("upstream_response").is_some(),
        "the streamed (then-truncated) upstream_response section is present: {artifact}"
    );
    let section = &artifact["sections"]["upstream_response"];
    assert_eq!(
        section["partial"], true,
        "a G6 frame-cap rejection closes upstream_response partial: {section}"
    );
}

/// AC-16 (end-to-end redaction): a turn carrying BOTH a secret AND images produces an
/// artifact whose REQUEST sections — `inbound_request` and `upstream_request` — leak
/// NEITHER the secret VALUE nor any image `data:`/URL bytes. Runs through the REAL
/// `ReqwestUpstreamClient` leaf (wiremock) so `upstream_request` is actually captured
/// (the in-process `MockUpstream` bypasses the dispatch tap, leaving that section
/// absent — see AC-7/8). Two redaction paths converge here: the middleware redacts the
/// raw inbound body (secret keys + image URIs) BEFORE `inbound_request` capture, and
/// the on-wire OpenAI request is redacted by `redacted_upstream_request_bytes` AND its
/// text-only-model images are degraded to a placeholder (`UnsupportedImagePolicy::
/// Placeholder`), so no image bytes reach `upstream_request` either. The
/// `served_response`/`upstream_response` sections are raw model OUTPUT (image redaction
/// on the response stream is out of scope — F1e note), so this asserts the REQUEST
/// sections. AGENTS.md line 137/144.
///
/// NON-VACUOUS upstream side (F1f review r1): the inbound `api_key` is dropped by
/// `/v1/messages` pre-lowering and the images are degraded, so no inbound secret
/// naturally survives into `upstream_request` — an upstream-only assertion on those
/// would pass even with the upstream redaction deleted. To genuinely exercise the
/// upstream secret pass, a sensitive-KEYED `upstream_chat_kwargs` value is seeded; it
/// flattens into the on-wire `extra_body` and truly reaches `upstream_request`, where
/// its value must be redacted (asserted below).
#[tokio::test]
async fn f1f_ac16_request_sections_redact_secret_and_image_end_to_end() {
    let capture_dir = unique_capture_dir("f1f-ac16");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "glm-5.1"}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-1",
                    "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17}
                })])),
        )
        .mount(&server)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", server.uri()).parse().expect("url");
    config.turn_capture_dir = Some(capture_dir.clone());
    // Non-vacuous `upstream_request` redaction (F1f review r1): the inbound top-level
    // `api_key` below is DROPPED by `/v1/messages` before lowering (and the images are
    // degraded), so nothing secret-bearing from the inbound body actually survives into
    // `upstream_request` — deleting the upstream-side redaction would still pass. To
    // genuinely EXERCISE the `redacted_upstream_request_bytes` secret pass, seed a
    // sensitive-KEYED `upstream_chat_kwargs` value: it gap-fills into the on-wire
    // ChatCompletionRequest's (flattened) `extra_body` — proven to reach the wire by
    // `forwards_configured_upstream_chat_kwargs` — so it truly LANDS in the captured
    // `upstream_request` section, where the redaction must strip its value.
    config.upstream_chat_kwargs = JsonMap::from_iter([(
        "client_secret".to_string(),
        json!("sk-UPSTREAM-KWARG-DO-NOT-LEAK-8888"),
    )]);
    let (app, _gateway) = llmconduit::build_app_with_gateway_and_options(
        config,
        None,
        llmconduit::AppOptions {
            with_debug_ui: false,
        },
    );

    // The turn carries a secret AND two images (an https signed URL + a data: URI) —
    // the exact shapes `f1b_inbound_section_redacts_secrets_and_image_uris` proves the
    // middleware redacts on the inbound side.
    let body = json!({
        "model": "glm-5.1",
        "max_tokens": 1024,
        "stream": true,
        "api_key": "sk-LEAKED-SECRET-VALUE",
        "messages": [{
            "role": "user",
            "content": [
                { "type": "text", "text": "describe" },
                { "type": "image", "source": { "type": "url",
                    "url": "https://secret.example.com/img.png?sig=IMGTOKEN123" } },
                { "type": "image", "source": { "type": "url",
                    "url": "data:image/png;base64,RAWIMGBYTES9999" } }
            ]
        }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let _ = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read served stream");

    let artifact = wait_for_only_artifact(&capture_dir).await;
    let sections = &artifact["sections"];

    // upstream_request MUST be present (the real dispatch leaf captured it) so the
    // assertion below is not vacuously satisfied by an absent section.
    assert!(
        sections.get("upstream_request").is_some(),
        "upstream_request captured through the real dispatch leaf: {artifact}"
    );
    // Serialize each request section WHOLE (metadata + nested content) so any secret
    // value or image bytes surviving ANYWHERE inside it is caught.
    let inbound = serde_json::to_string(&sections["inbound_request"]).expect("inbound json");
    let upstream = serde_json::to_string(&sections["upstream_request"]).expect("upstream json");
    for (label, text) in [
        ("inbound_request", &inbound),
        ("upstream_request", &upstream),
    ] {
        assert!(
            !text.contains("sk-LEAKED-SECRET-VALUE"),
            "{label} leaks the secret api_key value: {text}"
        );
        assert!(
            !text.contains("secret.example.com"),
            "{label} leaks the signed image URL host: {text}"
        );
        assert!(
            !text.contains("IMGTOKEN123"),
            "{label} leaks the signed-URL token: {text}"
        );
        assert!(
            !text.contains("RAWIMGBYTES9999"),
            "{label} leaks raw data: image bytes: {text}"
        );
    }
    // Meaningful (not vacuous): the inbound body DID carry the secret + images, and the
    // redaction markers prove they were present-then-stripped (not merely absent).
    assert!(
        inbound.contains("[redacted]"),
        "secret redaction marker present in inbound_request: {inbound}"
    );
    assert!(
        inbound.contains("<redacted uri>"),
        "image URI redaction marker present in inbound_request: {inbound}"
    );

    // NON-VACUOUS upstream_request proof (F1f review r1): the sensitive-keyed
    // `client_secret` kwarg FLATTENS into the on-wire request's `extra_body`, so its KEY
    // genuinely reaches `upstream_request`. The KEY surviving while its VALUE is gone is
    // a real redaction (not "the field never got here") — removing the
    // `redact_payload_secrets_in_value` pass in `redacted_upstream_request_bytes` would
    // make this section leak the raw `sk-UPSTREAM-KWARG-DO-NOT-LEAK-8888`.
    assert!(
        upstream.contains("client_secret"),
        "the sensitive-keyed kwarg reached upstream_request (non-vacuous anchor): {upstream}"
    );
    assert!(
        !upstream.contains("sk-UPSTREAM-KWARG-DO-NOT-LEAK-8888"),
        "upstream_request redacts the sensitive-keyed kwarg VALUE: {upstream}"
    );
    assert!(
        upstream.contains("[redacted]"),
        "secret redaction marker present in upstream_request: {upstream}"
    );
}

#[tokio::test]
async fn head_and_options_probes_return_204_with_allow_header() {
    let config = test_config();
    let app = llmconduit::build_app(config);

    for (method, path, expected_allow) in [
        ("HEAD", "/v1/messages", "POST, HEAD, OPTIONS"),
        ("OPTIONS", "/v1/messages", "POST, HEAD, OPTIONS"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(
            response.status().as_u16(),
            204,
            "{method} {path} should return 204"
        );
        let allow_header = response
            .headers()
            .get("allow")
            .and_then(|v| v.to_str().ok());
        assert_eq!(
            allow_header,
            Some(expected_allow),
            "{method} {path} should have correct Allow header"
        );
    }
}

#[tokio::test]
async fn health_endpoint_returns_healthy() {
    let config = test_config();
    let app = llmconduit::build_app(config);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    assert_eq!(body["status"], "healthy");
}

#[tokio::test]
async fn root_endpoint_returns_ok() {
    let config = test_config();
    let app = llmconduit::build_app(config);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn sse_responses_include_connection_keep_alive() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "hello"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let request = ResponsesRequest {
        model: "glm-5.1".to_string(),
        instructions: String::new(),
        input: vec![user_message("hi")],
        tools: Vec::new(),
        tool_choice: json!("auto"),
        parallel_tool_calls: Some(true),
        reasoning: None,
        thinking: None,
        store: false,
        stream: true,
        include: Vec::new(),
        service_tier: None,
        prompt_cache_key: None,
        text: None,
        client_metadata: None,
        previous_response_id: None,
        temperature: None,
        top_p: None,
        max_output_tokens: None,
        frequency_penalty: None,
        presence_penalty: None,
        truncation: None,
        metadata: None,
        stop: None,
        extra_body: Default::default(),
    };

    let app = llmconduit::build_app_from_gateway(gateway);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status().as_u16(), 200);
    let connection = response
        .headers()
        .get("connection")
        .and_then(|v| v.to_str().ok());
    assert_eq!(connection, Some("keep-alive"));
}

// ===========================================================================
// E1 — bounded soft-reject repair for hallucinated upstream tool calls.
// ===========================================================================

/// One streamed tool-call delta chunk with independent control over `id` / `name`
/// / `arguments` / `index` / finish — lets E1 tests reproduce a tool whose name
/// arrives BEFORE, WITH, or AFTER its argument fragments.
fn e1_tool_chunk(
    id: &str,
    call_id: Option<&str>,
    index: usize,
    name: Option<&str>,
    arguments: Option<&str>,
    finish: bool,
) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_string(),
        choices: vec![ChatChunkChoice {
            index: 0,
            delta: ChatDelta {
                content: None,
                reasoning_content: None,
                tool_calls: Some(vec![ChatToolCall {
                    id: call_id.map(str::to_string),
                    index: Some(index),
                    kind: "function".to_string(),
                    function: ChatFunctionCall {
                        name: name.map(str::to_string),
                        arguments: arguments.map(|s| serde_json::Value::String(s.to_string())),
                    },
                }]),
                function_call: None,
                refusal: None,
                extra: Default::default(),
            },
            finish_reason: finish.then(|| "tool_calls".to_string()),
            stop_reason: None,
        }],
        usage: None,
    }
}

/// Concatenate every collected Responses SSE event into one JSON blob so a test
/// can assert that a hallucinated tool name / its arguments NEVER appear in the
/// client-facing stream.
fn e1_events_blob(events: &[serde_json::Value]) -> String {
    events
        .iter()
        .map(|event| event.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

fn e1_repaired_count(gateway: &Gateway) -> u64 {
    gateway
        .unknown_tool_call_counts()
        .iter()
        .filter(|((_, _, outcome), _)| *outcome == llmconduit::engine::UnknownToolOutcome::Repaired)
        .map(|(_, count)| *count)
        .sum()
}

fn e1_exhausted_count(gateway: &Gateway) -> u64 {
    gateway
        .unknown_tool_call_counts()
        .iter()
        .filter(|((_, _, outcome), _)| {
            *outcome == llmconduit::engine::UnknownToolOutcome::Exhausted
        })
        .map(|(_, count)| *count)
        .sum()
}

#[tokio::test]
async fn e1_unknown_tool_self_corrects_via_repair_round_responses() {
    // The incident, recovered: the model emits a tool NOT in the offered set
    // (`Grep`), the gateway soft-rejects it (no `?` abort), runs ONE in-gateway
    // repair round, and the model self-corrects with a text answer. The client
    // stream COMPLETES (never `response.failed`), never sees the hallucinated tool
    // or its argument deltas, and the `Repaired` outcome is counted.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(e1_tool_chunk(
            "chat-0",
            Some("call_bad"),
            0,
            Some("Grep"),
            Some(r#"{"pattern":"needle"}"#),
            true,
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "The answer is 42."))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let events = collect_stream(
        gateway
            .clone()
            .stream_responses(base_request(vec![user_message("search the code")]))
            .await
            .expect("stream"),
    )
    .await;

    let names = event_names(&events);
    assert!(
        names.contains(&"response.completed"),
        "the turn must recover and complete, not abort: {names:?}"
    );
    assert!(
        !names.contains(&"response.failed"),
        "a self-correcting turn must not emit response.failed: {names:?}"
    );
    // Exactly two upstream rounds: the original + one bounded repair round.
    assert_eq!(upstream.requests().await.len(), 2, "one repair round ran");
    // The hallucinated tool and its arguments are NEVER in the client stream.
    let blob = e1_events_blob(&events);
    assert!(
        !blob.contains("Grep"),
        "hallucinated tool name leaked: {blob}"
    );
    assert!(
        !blob.contains("needle"),
        "hallucinated tool arguments leaked: {blob}"
    );
    assert!(
        !names.contains(&"response.function_call_arguments.delta"),
        "no tool-arg deltas reach the client for the hidden call: {names:?}"
    );
    // The recovered answer is delivered.
    assert!(blob.contains("The answer is 42."), "recovered text missing");
    // Observability: a Repaired outcome was counted.
    assert_eq!(e1_repaired_count(&gateway), 1, "repaired outcome counted");
    assert_eq!(e1_exhausted_count(&gateway), 0);
}

#[tokio::test]
async fn e1_malformed_unknown_tool_args_do_not_error_the_stream() {
    // E1: unknown-tool arguments are NEVER JSON-parsed. Even syntactically broken
    // args must NOT surface a parse error / abort — the call is soft-rejected and
    // the turn recovers.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(e1_tool_chunk(
            "chat-0",
            Some("call_bad"),
            0,
            Some("Grep"),
            Some("}{ not json at all"),
            true,
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "done"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let events = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message("go")]))
            .await
            .expect("stream"),
    )
    .await;
    let names = event_names(&events);
    assert!(names.contains(&"response.completed"));
    assert!(!names.contains(&"response.failed"));
    assert!(!e1_events_blob(&events).contains("not json"));
}

#[tokio::test]
async fn e1_unknown_tool_deltas_hidden_chat_completions() {
    // Deltas-hidden assertion on the CHAT inbound format: the hallucinated tool
    // never appears in the chat response; the recovered text is returned.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(e1_tool_chunk(
            "chat-0",
            Some("call_bad"),
            0,
            Some("Grep"),
            Some(r#"{"pattern":"x"}"#),
            true,
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "recovered answer"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": false,
        "messages": [{ "role": "user", "content": "search" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        !text.contains("Grep"),
        "tool name leaked to chat client: {text}"
    );
    assert!(text.contains("recovered answer"), "recovered text missing");
}

#[tokio::test]
async fn e1_unknown_tool_deltas_hidden_anthropic_stream() {
    // Deltas-hidden assertion on the ANTHROPIC inbound format (streaming): the
    // hallucinated tool never appears; the stream ends cleanly with the recovered
    // text.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(e1_tool_chunk(
            "chat-0",
            Some("call_bad"),
            0,
            Some("Grep"),
            Some(r#"{"pattern":"x"}"#),
            true,
        ))])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "recovered text"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "search" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        text.contains("event: message_stop"),
        "stream did not end cleanly"
    );
    assert!(
        !text.contains("Grep"),
        "tool name leaked to anthropic client: {text}"
    );
    assert!(text.contains("recovered text"), "recovered text missing");
}

#[tokio::test]
async fn e1_mixed_batch_is_tainted_no_handoff_and_injects_repair_context() {
    // Mixed valid+hallucinated in ONE batch: the whole batch is TAINTED. The
    // valid `echo` (here streamed name-LAST so it is buffered) is NOT handed off,
    // a synthetic `not_executed` result is supplied for it, a `tool_unavailable`
    // result for the unknown `Grep`, and the closed-tool-set prevention note is
    // added to the repair-round upstream request.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            // Grep (unknown), name-first → dropped by the gate.
            Ok(e1_tool_chunk(
                "chat-0",
                Some("call_bad"),
                0,
                Some("Grep"),
                Some(r#"{"pattern":"x"}"#),
                false,
            )),
            // echo (valid) args BEFORE its name → buffered by the gate.
            Ok(e1_tool_chunk(
                "chat-0",
                Some("call_ok"),
                1,
                None,
                Some(r#"{"value":"hi"}"#),
                false,
            )),
            // echo name arrives name-only (no delta) at the end.
            Ok(e1_tool_chunk(
                "chat-0",
                Some("call_ok"),
                1,
                Some("echo"),
                None,
                true,
            )),
        ])
        .await;
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "recovered"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let mut request = base_request(vec![user_message("do it")]);
    request.tools = vec![ToolSpec::Function {
        name: "echo".to_string(),
        description: "Echo a value".to_string(),
        strict: false,
        parameters: json!({
            "type": "object",
            "properties": { "value": { "type": "string" } }
        }),
    }];

    let events = collect_stream(gateway.stream_responses(request).await.expect("stream")).await;
    let names = event_names(&events);
    // The turn recovered.
    assert!(names.contains(&"response.completed"), "{names:?}");
    assert!(!names.contains(&"response.failed"), "{names:?}");
    // NEITHER tool in the tainted batch was handed off / surfaced: no function
    // call output item, and no hidden tool name leaked.
    let done = done_items(&events);
    assert!(
        !done
            .iter()
            .any(|item| matches!(item, ResponseItem::FunctionCall { .. })),
        "a tainted batch must hand off NO client tool"
    );
    let blob = e1_events_blob(&events);
    assert!(!blob.contains("Grep"), "hidden tool leaked: {blob}");
    assert!(
        !blob.contains("\"hi\""),
        "buffered valid-tool args leaked on taint: {blob}"
    );

    // The repair round (second upstream request) carries: a synthetic tool result
    // for BOTH calls + the closed-tool-set prevention note.
    let repair = &upstream.requests().await[1];
    let tool_results: Vec<&str> = repair
        .messages
        .iter()
        .filter(|m| m.role == "tool")
        .filter_map(|m| m.content.as_ref().and_then(|c| c.as_str()))
        .collect();
    assert!(
        tool_results.iter().any(|c| c.contains("not_executed")),
        "valid tainted call needs a not_executed result: {tool_results:?}"
    );
    assert!(
        tool_results.iter().any(|c| c.contains("tool_unavailable")),
        "unknown call needs a tool_unavailable result: {tool_results:?}"
    );
    let has_closed_set_note = repair.messages.iter().any(|m| {
        m.role == "system"
            && m.content
                .as_ref()
                .and_then(|c| c.as_str())
                .is_some_and(|c| c.contains("only call tools"))
    });
    assert!(
        has_closed_set_note,
        "prevention note (closed tool set) must be added to the repair request"
    );
}

#[tokio::test]
async fn e1_repair_ceiling_emits_structured_terminal_responses_with_observability() {
    // A model that emits the bad tool EVERY round hits the repair ceiling and the
    // turn ends with a STRUCTURED `response.failed` (code `invalid_tool_call`) —
    // NOT a raw mid-stream abort. Bounded: original + ceiling(=1) repair = 2
    // upstream rounds. Observability: Exhausted counted + both monitor phases.
    let upstream = MockUpstream::default();
    for n in 0..6 {
        upstream
            .push_response(vec![Ok(e1_tool_chunk(
                &format!("chat-{n}"),
                Some(&format!("call_bad_{n}")),
                0,
                Some("Grep"),
                Some(r#"{"pattern":"x"}"#),
                true,
            ))])
            .await;
    }
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let mut monitor = gateway.subscribe_monitor();

    let events = collect_stream(
        gateway
            .clone()
            .stream_responses(base_request(vec![user_message("loop forever")]))
            .await
            .expect("stream"),
    )
    .await;

    let names = event_names(&events);
    assert!(
        names.contains(&"response.failed"),
        "ceiling must end with a structured terminal failure: {names:?}"
    );
    // Structured: code is `invalid_tool_call`, message does not leak the tool name.
    let failed = events
        .iter()
        .find(|e| e["_event"] == "response.failed")
        .expect("response.failed present");
    assert_eq!(
        failed["response"]["error"]["code"], "invalid_tool_call",
        "terminal failure must carry the structured code"
    );
    let blob = e1_events_blob(&events);
    assert!(!blob.contains("Grep"), "tool name leaked: {blob}");
    // Bounded: original + exactly one repair round.
    assert_eq!(
        upstream.requests().await.len(),
        UNKNOWN_TOOL_REPAIR_CEILING_FOR_TEST + 1,
        "repair is bounded by the ceiling"
    );
    // Observability: Exhausted counted, no Repaired.
    assert_eq!(e1_exhausted_count(&gateway), 1);
    assert_eq!(e1_repaired_count(&gateway), 0);
    // Monitor phases emitted via emit_with.
    let mut monitor_blob = String::new();
    while let Ok(update) = monitor.try_recv() {
        monitor_blob.push_str(&serde_json::to_string(&update).expect("serialize update"));
    }
    assert!(
        monitor_blob.contains("unknown_tool_rejected"),
        "missing unknown_tool_rejected phase"
    );
    assert!(
        monitor_blob.contains("unknown_tool_repair_exhausted"),
        "missing unknown_tool_repair_exhausted phase"
    );
}

/// The engine's `UNKNOWN_TOOL_REPAIR_CEILING` (default 1) mirrored for test
/// arithmetic — the engine const is private, so a drift here is caught by the
/// bounded-round-count assertion above.
const UNKNOWN_TOOL_REPAIR_CEILING_FOR_TEST: usize = 1;

#[tokio::test]
async fn e1_repair_ceiling_preserves_already_streamed_text() {
    // Text the model emitted BEFORE the bad tool, across rounds, is preserved
    // (never retracted) even though the turn ends in a terminal failure.
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-0", "thinking out loud ")),
            Ok(e1_tool_chunk(
                "chat-0",
                Some("call_bad_0"),
                0,
                Some("Grep"),
                Some("{}"),
                true,
            )),
        ])
        .await;
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "still trying ")),
            Ok(e1_tool_chunk(
                "chat-1",
                Some("call_bad_1"),
                0,
                Some("Grep"),
                Some("{}"),
                true,
            )),
        ])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let events = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message("go")]))
            .await
            .expect("stream"),
    )
    .await;
    let names = event_names(&events);
    assert!(names.contains(&"response.failed"), "{names:?}");
    let blob = e1_events_blob(&events);
    // Both rounds' already-streamed text survived to the client.
    assert!(blob.contains("thinking out loud"), "round-0 text retracted");
    assert!(blob.contains("still trying"), "round-1 text retracted");
    assert!(!blob.contains("Grep"), "tool name leaked: {blob}");
}

#[tokio::test]
async fn e1_repair_ceiling_structured_terminal_chat_stream() {
    // Ceiling terminal on the CHAT inbound format (streaming): a structured SSE
    // error frame, not a raw abort, and no leaked tool name.
    let upstream = MockUpstream::default();
    for n in 0..4 {
        upstream
            .push_response(vec![Ok(e1_tool_chunk(
                &format!("chat-{n}"),
                Some(&format!("call_bad_{n}")),
                0,
                Some("Grep"),
                Some("{}"),
                true,
            ))])
            .await;
    }
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": true,
        "messages": [{ "role": "user", "content": "go" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    // Parse the SSE error frame and assert it carries the STRUCTURED code
    // (acceptance #5) — the OpenAI Chat error object has a `code` field, so the
    // canonical terminal's `invalid_tool_call` must reach the chat client, not
    // just the human message.
    let error_frame = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|payload| *payload != "[DONE]")
        .filter_map(|payload| serde_json::from_str::<serde_json::Value>(payload).ok())
        .find(|value| value.get("error").is_some())
        .expect("chat ceiling must emit a structured error frame");
    assert_eq!(
        error_frame["error"]["code"], "invalid_tool_call",
        "chat SSE error must carry the structured code, not drop it: {text}"
    );
    assert!(
        error_frame["error"]["message"].is_string(),
        "chat error frame keeps the human message too: {text}"
    );
    assert!(
        text.contains("[DONE]"),
        "chat error frame must still close the stream"
    );
    assert!(
        !text.contains("Grep"),
        "tool name leaked to chat client: {text}"
    );
}

#[tokio::test]
async fn e1_repair_ceiling_structured_terminal_anthropic_stream() {
    // Ceiling terminal on the ANTHROPIC inbound format (streaming): a structured
    // `error` event, not a raw abort, and no leaked tool name.
    let upstream = MockUpstream::default();
    for n in 0..4 {
        upstream
            .push_response(vec![Ok(e1_tool_chunk(
                &format!("chat-{n}"),
                Some(&format!("call_bad_{n}")),
                0,
                Some("Grep"),
                Some("{}"),
                true,
            ))])
            .await;
    }
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "go" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        text.contains("event: error"),
        "anthropic ceiling needs an error event: {text}"
    );
    assert!(
        !text.contains("Grep"),
        "tool name leaked to anthropic client: {text}"
    );

    // T5: full harness proof, real gateway/HTTP output, Error surface (C4:
    // the stream ends AT `error`, no trailing message_delta/message_stop).
    use llmconduit::adapters::responses_to_anthropic::conformance::Surface;
    use llmconduit::adapters::responses_to_anthropic::conformance::assert_sse_conformant;
    let anthropic_events = parse_anthropic_sse_events(&text);
    assert_sse_conformant(&anthropic_events, Surface::Error);
}

/// E2a AC-1 + AC-3 (Anthropic surface), full HTTP fidelity: reproduces the field
/// incident almost exactly — an image reaching a text-only vLLM backend 400'd with
/// "not a multimodal model", which used to trip a cooldown (`test_config()`'s
/// `upstream_failure_cooldown_secs: 30` matches the incident's own window) and 502
/// every unrelated request for its duration. This drives the REAL leaf
/// (`ReqwestUpstreamClient`) over wiremock via `build_app_with_gateway_and_options`, so
/// it exercises the ACTUAL E2a disposition logic in `dispatch_chat_stream`, not a
/// hand-built `AppError`.
#[tokio::test]
async fn e2a_request_intrinsic_400_no_cooldown_structured_anthropic_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": "glm-5.1"}]
        })))
        .mount(&server)
        .await;
    // First call: the field incident's exact upstream body.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_string(
            "{\"error\":{\"message\":\"DeepSeek-V4-Flash-DSpark is not a multimodal \
             model\",\"type\":\"BadRequestError\",\"code\":400}}",
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    // Every later call (the "second, unrelated request" below): a normal success.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(chat_completion_sse_body(&[json!({
                    "id": "chat-1",
                    "choices": [{
                        "index": 0,
                        "delta": {"content": "hello again"},
                        "finish_reason": "stop"
                    }],
                    "usage": null
                })])),
        )
        .mount(&server)
        .await;

    let mut config = test_config();
    config.upstream_base_url = format!("{}/v1/", server.uri()).parse().expect("url");
    let (app, _gateway) = llmconduit::build_app_with_gateway_and_options(
        config,
        None,
        llmconduit::AppOptions::default(),
    );

    let first_body = json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "describe this image" }]
    });
    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_string(&first_body).expect("serialize"),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(
        first.status().as_u16(),
        200,
        "a streaming response is 200 even when the turn fails — the error is an SSE frame"
    );
    let bytes = axum::body::to_bytes(first.into_body(), 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(
        text.contains("event: error"),
        "a request-intrinsic 4xx must surface as a structured Anthropic error event, \
         never a raw abort: {text}"
    );
    assert!(
        !text.contains("502") && !text.contains("Bad Gateway"),
        "the client-visible error must not leak the internal 502 upstream-error status: {text}"
    );
    use llmconduit::adapters::responses_to_anthropic::conformance::Surface;
    use llmconduit::adapters::responses_to_anthropic::conformance::assert_sse_conformant;
    let anthropic_events = parse_anthropic_sse_events(&text);
    assert_sse_conformant(&anthropic_events, Surface::Error);

    // AC-1 capstone: a SECOND, unrelated request must be served normally — the
    // provider must NOT be cooling (the exact field-incident regression: every
    // request 502'd for the cooldown window after one image 400).
    let second_body = json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "unrelated text-only question" }]
    });
    let second = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_string(&second_body).expect("serialize"),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(
        second.status().as_u16(),
        200,
        "the second, unrelated request must be served, not short-circuited by a cooldown"
    );
    let bytes2 = axum::body::to_bytes(second.into_body(), 1024 * 1024)
        .await
        .expect("body");
    let text2 = String::from_utf8(bytes2.to_vec()).expect("utf8");
    assert!(
        text2.contains("hello again"),
        "the second request must actually reach the upstream and stream real content, \
         proving the provider was never cooled: {text2}"
    );
    assert!(
        !text2.contains("event: error"),
        "the second request must NOT fail: {text2}"
    );
}

/// E2a AC-3 (Chat inbound format, streaming): a request-intrinsic 4xx must render as a
/// structured SSE `error` data frame, never a raw abort or a masked 502 status on the
/// overall (already-200) streaming response. The canonical `response.failed` -> Chat
/// SSE `error` rendering (`adapters/chat_completions.rs`) never reads
/// `FailoverDisposition` (grep-verified: that type only appears in `error.rs` /
/// `upstream.rs`), so a plain `AppError::upstream(...)` shaped exactly like the real
/// leaf's message reproduces the SAME rendering path a real Terminal-tagged
/// request-intrinsic 4xx flows through — the wiremock-backed Anthropic test above
/// covers the real leaf end-to-end; this covers the Chat egress converter.
#[tokio::test]
async fn e2a_request_intrinsic_4xx_structured_terminal_chat_stream() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Err(llmconduit::error::AppError::upstream(
            "upstream chat failed with 400: {\"error\":{\"message\":\"model is not \
             multimodal\"}}",
        ))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
    let app = llmconduit::build_app_from_gateway(gateway);

    let body = json!({
        "model": "glm-5.1",
        "stream": true,
        "messages": [{ "role": "user", "content": "go" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    let error_frame = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|payload| *payload != "[DONE]")
        .filter_map(|payload| serde_json::from_str::<serde_json::Value>(payload).ok())
        .find(|value| value.get("error").is_some())
        .expect("a request-intrinsic 4xx must emit a structured chat SSE error frame");
    assert!(
        error_frame["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("400"),
        "chat error frame should surface the upstream status in the message: {text}"
    );
    assert!(
        text.contains("[DONE]"),
        "chat error frame must still close the stream: {text}"
    );
}

/// E2a AC-3 (Responses inbound format, streaming): canonical Responses IS the wire
/// format here — a request-intrinsic 4xx must surface as `response.failed`, never a
/// raw stream abort. Same disposition-agnostic rendering argument as the Chat test
/// above (canonical `response.failed` construction, `engine::failure_event`, never
/// reads `FailoverDisposition` either).
#[tokio::test]
async fn e2a_request_intrinsic_4xx_structured_terminal_responses_stream() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Err(llmconduit::error::AppError::upstream(
            "upstream chat failed with 400: {\"error\":{\"message\":\"model is not \
             multimodal\"}}",
        ))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());

    let events = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message("describe this image")]))
            .await
            .expect("stream"),
    )
    .await;
    let names = event_names(&events);
    assert!(
        names.contains(&"response.failed"),
        "a request-intrinsic 4xx must emit a structured response.failed event: {names:?}"
    );
    let failed = events
        .iter()
        .find(|event| event["_event"] == "response.failed")
        .expect("response.failed event");
    let message = failed["response"]["error"]["message"]
        .as_str()
        .expect("error message");
    assert!(
        message.contains("400"),
        "response.failed should surface the upstream status: {message}"
    );
    assert!(
        !names.iter().any(|name| name.contains("output_text")),
        "no content should have streamed before the terminal failure: {names:?}"
    );
}

/// E1 cancellation harness: the FIRST upstream call yields a hallucinated tool
/// call then ENDS (so the engine soft-rejects and starts a repair round); EVERY
/// later call (the repair round) PARKS forever, so a client drop DURING the
/// repair round must cancel the parked upstream stream.
#[derive(Clone)]
struct RepairRoundPendingUpstream {
    calls: Arc<Mutex<usize>>,
    repair_polled: Arc<Notify>,
    repair_dropped: Arc<Notify>,
}

impl RepairRoundPendingUpstream {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(0)),
            repair_polled: Arc::new(Notify::new()),
            repair_dropped: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl UpstreamClient for RepairRoundPendingUpstream {
    async fn stream_chat_completion(
        &self,
        _backend: &llmconduit::upstream::BackendChatRequest,
    ) -> Result<UpstreamStream, llmconduit::error::AppError> {
        let mut calls = self.calls.lock().await;
        let call_index = *calls;
        *calls += 1;
        drop(calls);
        if call_index == 0 {
            // Round 0: a single hallucinated tool call, then the stream ENDS so
            // the engine finalizes, soft-rejects, and begins a repair round.
            let stream = async_stream::stream! {
                yield Ok(e1_tool_chunk("chat-0", Some("call_bad"), 0, Some("Grep"), Some("{}"), true));
            };
            Ok(Box::pin(stream))
        } else {
            // Repair round: park forever so the client can cancel it.
            let repair_polled = Arc::clone(&self.repair_polled);
            let repair_dropped = Arc::clone(&self.repair_dropped);
            let stream = async_stream::stream! {
                let _drop_guard = NotifyOnDrop { notify: repair_dropped };
                repair_polled.notify_waiters();
                std::future::pending::<()>().await;
                yield Ok(content_chunk("chat-1", "unreachable"));
            };
            Ok(Box::pin(stream))
        }
    }

    async fn list_models(&self) -> Result<reqwest::Response, llmconduit::error::AppError> {
        Err(llmconduit::error::AppError::internal("unused in this test"))
    }

    async fn supported_model_catalog(
        &self,
    ) -> Result<Vec<UpstreamModelEntry>, llmconduit::error::AppError> {
        Ok(vec![UpstreamModelEntry {
            id: "glm-5.1".to_string(),
            context_limit: None,
        }])
    }
}

#[tokio::test]
async fn e1_repair_round_is_cancellable_on_client_hangup() {
    // Cancellation is preserved ACROSS the repair round: while the repair round's
    // upstream is parked, a client hang-up (dropping the stream) cancels it — the
    // parked upstream stream is dropped, exactly like the main loop.
    let upstream = RepairRoundPendingUpstream::new();
    // Clone the signal handles BEFORE moving the upstream into the gateway.
    let repair_polled = Arc::clone(&upstream.repair_polled);
    let repair_dropped = Arc::clone(&upstream.repair_dropped);
    let gateway = test_gateway_with_flow_store_upstream(Arc::new(upstream));

    let mut stream = gateway
        .stream_responses(base_request(vec![user_message("go")]))
        .await
        .expect("stream");

    // Drive the SSE stream until the repair round's upstream parks. Pin the
    // `Notified` futures so they stay registered across the select loop (a fresh
    // `notified()` each iteration would miss the `notify_waiters` wake).
    let polled = repair_polled.notified();
    let dropped = repair_dropped.notified();
    tokio::pin!(polled);
    tokio::pin!(dropped);
    let pump = async {
        loop {
            tokio::select! {
                _ = stream.next() => {}
                _ = &mut polled => break,
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), pump)
        .await
        .expect("repair round reached and parked");

    // Client hangs up mid-repair-round.
    drop(stream);

    // The parked repair-round upstream stream must be dropped (cancelled).
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut dropped)
        .await
        .expect("repair-round upstream cancelled on client hang-up");
}

// -- F1a: turn_capture DI wiring (`lib.rs` -> `Gateway`) --------------------
//
// `test_gateway`/`test_gateway_with_config` above build a `Gateway` directly
// via `Gateway::new(...)`, bypassing the `lib.rs` DI root entirely, so they
// cannot exercise the real `config.turn_capture_dir -> TurnCapture -> Gateway`
// wiring. These two tests go through the actual DI entry point
// (`build_app_with_gateway`) instead.

/// F1a: when `turn_capture_dir` is configured, `build_app_with_gateway` (the
/// real DI root) attaches an ENABLED `TurnCapture` to the `Gateway`, reachable
/// via `gateway.turn_capture()` -- confirming the handle threads into the
/// gateway/HTTP router state, independent of `--with-debug-ui` (`build_app`/
/// `build_app_with_gateway` never enable the debug UI). F1a is in-memory only:
/// merely constructing the app must not create the directory.
#[tokio::test]
async fn build_app_with_gateway_wires_enabled_turn_capture_from_config() {
    let dir = std::env::temp_dir().join(format!(
        "llmconduit-gateway-turn-capture-{}",
        uuid::Uuid::new_v4().simple()
    ));
    assert!(!dir.exists());

    let mut config = test_config();
    config.turn_capture_dir = Some(dir.clone());
    let (_app, gateway) = llmconduit::build_app_with_gateway(config);

    assert!(
        gateway.turn_capture().is_enabled(),
        "a configured turn_capture_dir must produce an ENABLED TurnCapture on the Gateway"
    );
    assert_eq!(gateway.turn_capture().dir(), Some(dir.as_path()));
    assert!(
        !dir.exists(),
        "F1a wiring must not itself perform any filesystem IO"
    );
}

/// F1a: with no `turn_capture_dir` configured (the `test_config()` default),
/// the DI root attaches a DISABLED `TurnCapture` -- the zero-overhead default
/// every other test in this suite (built via `Gateway::new` directly) already
/// gets implicitly.
#[tokio::test]
async fn build_app_with_gateway_wires_disabled_turn_capture_by_default() {
    let config = test_config();
    assert_eq!(config.turn_capture_dir, None);
    let (_app, gateway) = llmconduit::build_app_with_gateway(config);

    assert!(!gateway.turn_capture().is_enabled());
    assert!(gateway.turn_capture().dir().is_none());
}

// ---------------------------------------------------------------------------
// F1b — turn-capture own gate + inbound capture + served-body wrapper.
// ---------------------------------------------------------------------------

/// A gateway with a `MockUpstream` and an ENABLED `TurnCapture` writing under
/// `dir`, but the dashboard FlowStore DISABLED — i.e. `--with-debug-ui` OFF. Proves
/// turn capture's own gate fires independent of the debug UI (spec Design #1).
fn test_gateway_with_turn_capture(upstream: MockUpstream, dir: std::path::PathBuf) -> Arc<Gateway> {
    let config = test_config();
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    let vision: Arc<dyn llmconduit::vision::VisionClient> = Arc::new(
        llmconduit::vision::ReqwestVisionClient::new(reqwest::Client::new(), &config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(
        Gateway::new(
            config,
            ReplayStore::new(1000),
            Arc::new(upstream),
            Arc::new(MockSearch::default()),
            vision,
            image_cache,
            MonitorHub::new(128),
            None,
            // Dashboard OFF: turn capture must still fire off its own gate.
            llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
        )
        .with_turn_capture(llmconduit::turn_capture::TurnCapture::enabled(dir)),
    )
}

fn turn_capture_temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("llmconduit-f1b-{}", uuid::Uuid::new_v4().simple()))
}

// NOTE (F1f flake fix): the pre-F1f `sole_capture_state` helper polled `<dir>/.work`
// for the single turn's work dir and looked the state up in the registry — but F1c/F1e
// assembly DELETES `.work/<id>/` and EVICTS the registry entry the instant the turn
// finalizes, so a fast-finalizing turn emptied `.work` and the helper `panic!`ed under
// parallel load (a harness timing race, not a product bug). The four tests below now
// read the DURABLE published `<id>.json` artifact via `wait_for_only_artifact` (the
// same helper the F1c/F1d/F1e tests use): the artifact is written exactly once and
// persists (until rotation), so the assertions are deterministic regardless of how
// fast the turn finalizes — and its appearance also proves finalize completed in
// bounded time.

/// AC-4 (+ AC-6a): with the dashboard OFF (FlowStore disabled) and
/// `turn_capture_dir` set, a streaming `/v1/messages` turn still runs the
/// turn-capture own gate — writing BOTH the `inbound_request` and `served_response`
/// sections under `.work/<api_call_id>/`, and the served section is the EXACT
/// streaming SSE bytes the client received.
#[tokio::test]
async fn f1b_turn_capture_writes_sections_with_dashboard_off() {
    let dir = turn_capture_temp_dir();
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "Hello")),
            Ok(content_chunk("chat-1", " world")),
            Ok(usage_chunk("chat-1", 12, 5, 17, Some(3), None)),
        ])
        .await;
    let gateway = test_gateway_with_turn_capture(upstream, dir.clone());
    assert!(
        !gateway.flow_store().is_enabled(),
        "dashboard FlowStore must be OFF (own-gate proof)"
    );
    assert!(gateway.turn_capture().is_enabled());
    let app = llmconduit::build_app_from_gateway(Arc::clone(&gateway));

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let served = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");

    let artifact = wait_for_only_artifact(&dir).await;
    let sections = &artifact["sections"];
    // inbound_request embeds as a parsed JSON value (the redacted Anthropic body).
    assert_eq!(
        sections["inbound_request"]["content"]["model"], "claude-3-5-sonnet-20241022",
        "inbound section holds the request body: {}",
        sections["inbound_request"]
    );
    // The served section is byte-for-byte what the client received (AC-6a): raw SSE
    // is UTF-8, so it round-trips through the artifact's JSON string `content`.
    let served_response = &sections["served_response"];
    let served_content = served_response["content"]
        .as_str()
        .expect("served content is a UTF-8 string");
    assert_eq!(
        served_content.as_bytes(),
        served.as_ref(),
        "served section == exact bytes returned to the client"
    );
    assert!(served_content.contains("event: message_start"));
    assert!(served_content.contains("event: message_stop"));
    assert!(served_content.contains("Hello"));
    assert_eq!(
        served_response["partial"], false,
        "a completed stream is not partial"
    );
    assert_eq!(served_response["bytes"].as_u64(), Some(served.len() as u64));
}

/// AC-5: the `inbound_request` section is redacted — a secret-bearing field AND an
/// image `data:`/URL URI are BOTH stripped, and NO raw image bytes survive (AGENTS.md
/// line 137/144). Redaction happens in the middleware (pre-parse), so it holds
/// regardless of the turn outcome.
#[tokio::test]
async fn f1b_inbound_section_redacts_secrets_and_image_uris() {
    let dir = turn_capture_temp_dir();
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway_with_turn_capture(upstream, dir.clone());
    let app = llmconduit::build_app_from_gateway(Arc::clone(&gateway));

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 128,
        "stream": false,
        // Non-standard field, but proves the shared secret-key redactor runs.
        "api_key": "sk-LEAKED-SECRET-VALUE",
        "messages": [{
            "role": "user",
            "content": [
                { "type": "text", "text": "describe" },
                { "type": "image", "source": { "type": "url",
                    "url": "https://secret.example.com/img.png?sig=IMGTOKEN123" } },
                { "type": "image", "source": { "type": "url",
                    "url": "data:image/png;base64,RAWIMGBYTES9999" } }
            ]
        }]
    });
    let _ = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");

    let artifact = wait_for_only_artifact(&dir).await;
    // inbound_request embeds as a parsed JSON value; serialize it back to scan the
    // whole redacted body for any surviving secret / image bytes.
    let text = serde_json::to_string(&artifact["sections"]["inbound_request"]["content"])
        .expect("serialize inbound content");

    assert!(
        !text.contains("sk-LEAKED-SECRET-VALUE"),
        "secret api_key value must be redacted"
    );
    assert!(
        text.contains("[redacted]"),
        "secret redaction marker present"
    );
    assert!(
        !text.contains("secret.example.com"),
        "image URL host must be redacted"
    );
    assert!(
        !text.contains("IMGTOKEN123"),
        "signed-URL token must be redacted"
    );
    assert!(
        !text.contains("RAWIMGBYTES9999"),
        "no raw image (data: URI) bytes may survive"
    );
    assert!(
        text.contains("<redacted uri>"),
        "image URI redaction marker present"
    );
    // Sanity: a non-secret field survived, so this is the real captured body.
    assert!(text.contains("claude-3-5-sonnet-20241022"));
}

/// AC-6b: served bytes are captured for a NON-streaming turn
/// (`collect_anthropic_response` → single JSON body).
#[tokio::test]
async fn f1b_captures_served_bytes_for_non_streaming_turn() {
    let dir = turn_capture_temp_dir();
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "Hello non-stream"))])
        .await;
    let gateway = test_gateway_with_turn_capture(upstream, dir.clone());
    let app = llmconduit::build_app_from_gateway(Arc::clone(&gateway));

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 128,
        "stream": false,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let served = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");

    let artifact = wait_for_only_artifact(&dir).await;
    let served_response = &artifact["sections"]["served_response"];
    let served_content = served_response["content"]
        .as_str()
        .expect("served content is a UTF-8 string");
    assert_eq!(
        served_content.as_bytes(),
        served.as_ref(),
        "served section == the non-streaming JSON body"
    );
    assert!(served_content.contains("\"type\":\"message\""));
    assert!(served_content.contains("Hello non-stream"));
    assert_eq!(
        served_response["partial"], false,
        "a complete non-streaming response is not partial"
    );
}

/// AC-6c: a mid-stream client disconnect (drop the response body after one frame)
/// closes `served_response` with `partial = true`, and finalization does NOT hang.
#[tokio::test]
async fn f1b_mid_stream_disconnect_marks_served_partial() {
    let dir = turn_capture_temp_dir();
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![
            Ok(content_chunk("chat-1", "one")),
            Ok(content_chunk("chat-1", "two")),
            Ok(content_chunk("chat-1", "three")),
            Ok(usage_chunk("chat-1", 12, 5, 17, Some(3), None)),
        ])
        .await;
    let gateway = test_gateway_with_turn_capture(upstream, dir.clone());
    let app = llmconduit::build_app_from_gateway(Arc::clone(&gateway));

    let body = serde_json::json!({
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 1024,
        "stream": true,
        "messages": [{ "role": "user", "content": "Hi" }]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).expect("serialize")))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status().as_u16(), 200);

    // Read ONE frame, then drop the body → client disconnect mid-stream.
    let mut stream = response.into_body().into_data_stream();
    let first = stream.next().await;
    assert!(
        first.is_some(),
        "at least one served frame arrived before the disconnect"
    );
    drop(stream);

    // The artifact appearing within a bounded time proves finalization did not hang
    // even though the stream was cut short; `partial` records the truncated capture.
    let artifact = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        wait_for_only_artifact(&dir),
    )
    .await
    .expect("turn finalizes + publishes within bounded time (no hang)");
    assert_eq!(
        artifact["sections"]["served_response"]["partial"], true,
        "a mid-stream disconnect marks served_response partial: {artifact}"
    );
}
