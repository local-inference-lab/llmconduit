use async_trait::async_trait;
use axum::body::Body;
use axum::http::Request;
use futures::StreamExt;
use futures::stream;
use llmconduit::config::Config;
use llmconduit::config::FallbackUpstreamConfig;
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

#[derive(Clone, Default)]
struct MockUpstream {
    requests: Arc<Mutex<Vec<ChatCompletionRequest>>>,
    responses: Arc<Mutex<VecDeque<Vec<Result<ChatCompletionChunk, llmconduit::error::AppError>>>>>,
    supported_models: Arc<Mutex<Vec<String>>>,
    supported_model_queries: Arc<Mutex<usize>>,
}

impl MockUpstream {
    async fn push_response(
        &self,
        chunks: Vec<Result<ChatCompletionChunk, llmconduit::error::AppError>>,
    ) {
        self.responses.lock().await.push_back(chunks);
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

    async fn supported_model_queries(&self) -> usize {
        *self.supported_model_queries.lock().await
    }
}

#[async_trait]
impl UpstreamClient for MockUpstream {
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> Result<UpstreamStream, llmconduit::error::AppError> {
        self.requests.lock().await.push(request.clone());
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

    async fn supported_model_ids(&self) -> Result<Vec<String>, llmconduit::error::AppError> {
        let mut query_count = self.supported_model_queries.lock().await;
        *query_count += 1;
        Ok(self.supported_models.lock().await.clone())
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
        request: &ChatCompletionRequest,
    ) -> Result<UpstreamStream, llmconduit::error::AppError> {
        self.requests.lock().await.push(request.clone());
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

    async fn supported_model_ids(&self) -> Result<Vec<String>, llmconduit::error::AppError> {
        Ok(vec!["glm-5.1".to_string()])
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
        parallel_tool_calls: true,
        reasoning: None,
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
    assert_eq!(requests[0].parallel_tool_calls, false);
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
        parallel_tool_calls: true,
        reasoning: None,
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
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profiles: std::collections::BTreeMap::new(),
            brave_base_url: "https://example.com/".parse().expect("url"),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout: std::time::Duration::from_secs(30),
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            compaction: Default::default(),
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
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profiles: std::collections::BTreeMap::new(),
            brave_base_url: "https://example.com/".parse().expect("url"),
            brave_api_key: Some("test-key".to_string()),
            brave_max_results: 5,
            request_timeout: std::time::Duration::from_secs(30),
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            compaction: Default::default(),
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
            upstream_chat_kwargs: JsonMap::from_iter([(
                "clear_thinking".to_string(),
                json!(false),
            )]),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profiles: std::collections::BTreeMap::new(),
            brave_base_url: "https://example.com/".parse().expect("url"),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout: std::time::Duration::from_secs(30),
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            compaction: Default::default(),
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
                },
            )]),
            brave_base_url: "https://example.com/".parse().expect("url"),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout: std::time::Duration::from_secs(30),
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            compaction: Default::default(),
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
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body = String::from_utf8(body_bytes.to_vec()).expect("utf8 body");
    assert!(body.contains("/debug/ws"));
    assert!(body.contains("llmconduit debug"));
    assert!(body.contains("new WebSocket"));
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
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        brave_base_url: "https://example.com/".parse().expect("url"),
        brave_api_key: None,
        brave_max_results: 5,
        request_timeout: std::time::Duration::from_secs(30),
        connect_timeout_secs: 10,
        max_web_search_rounds: 5,
        flatten_content: true,
        max_replay_entries: 1000,
        compaction: Default::default(),
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
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        brave_base_url: "https://example.com/".parse().expect("url"),
        brave_api_key: None,
        brave_max_results: 5,
        request_timeout: std::time::Duration::from_secs(30),
        connect_timeout_secs: 10,
        max_web_search_rounds: 5,
        flatten_content: true,
        max_replay_entries: 1000,
        compaction: Default::default(),
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
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        brave_base_url: "https://example.com/".parse().expect("url"),
        brave_api_key: None,
        brave_max_results: 5,
        request_timeout: std::time::Duration::from_secs(30),
        connect_timeout_secs: 10,
        max_web_search_rounds: 5,
        flatten_content: true,
        max_replay_entries: 1000,
        compaction: Default::default(),
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
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        brave_base_url: "https://example.com/".parse().expect("url"),
        brave_api_key: None,
        brave_max_results: 5,
        request_timeout: std::time::Duration::from_secs(30),
        connect_timeout_secs: 10,
        max_web_search_rounds: 5,
        flatten_content: true,
        max_replay_entries: 1000,
        compaction: Default::default(),
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
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        brave_base_url: "https://example.com/".parse().expect("url"),
        brave_api_key: None,
        brave_max_results: 5,
        request_timeout: std::time::Duration::from_secs(30),
        connect_timeout_secs: 10,
        max_web_search_rounds: 5,
        flatten_content: true,
        max_replay_entries: 1000,
        compaction: Default::default(),
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
        parallel_tool_calls: true,
        reasoning: None,
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
        parallel_tool_calls: true,
        reasoning: None,
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
    Arc::new(Gateway::new(
        config,
        ReplayStore::new(1000),
        Arc::new(upstream),
        Arc::new(search),
        MonitorHub::new(128),
        raw_output,
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
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        brave_base_url: "https://example.com/".parse().expect("url"),
        brave_api_key: Some("test-key".to_string()),
        brave_max_results: 5,
        request_timeout: std::time::Duration::from_secs(30),
        connect_timeout_secs: 10,
        max_web_search_rounds: 5,
        flatten_content: true,
        max_replay_entries: 1000,
        compaction: Default::default(),
    }
}

fn base_request(input: Vec<ResponseItem>) -> ResponsesRequest {
    ResponsesRequest {
        model: "glm-5.1".to_string(),
        instructions: String::new(),
        input,
        tools: Vec::new(),
        tool_choice: json!("auto"),
        parallel_tool_calls: true,
        reasoning: Some(llmconduit::models::responses::ReasoningRequest {
            effort: Some("medium".to_string()),
            summary: None,
        }),
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
        assert_eq!(
            body["chat_template_kwargs"],
            json!({
                "fallback_default": true,
                "model_default": true,
                "shared": "model"
            })
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
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
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
    let progress_tokens: Vec<u64> = anthropic_events
        .iter()
        .filter(|event| event["type"] == "message_delta" && event["delta"]["stop_reason"].is_null())
        .filter_map(|event| event["usage"]["output_tokens"].as_u64())
        .collect();
    assert!(
        !progress_tokens.is_empty(),
        "expected progressive output-token usage"
    );
    assert!(
        progress_tokens.windows(2).all(|pair| pair[0] < pair[1]),
        "progress output tokens must increase monotonically: {progress_tokens:?}"
    );
    let message_delta = anthropic_events
        .iter()
        .find(|event| {
            event["type"] == "message_delta" && event["delta"]["stop_reason"] == "end_turn"
        })
        .expect("message_delta event");
    assert_eq!(message_delta["usage"]["input_tokens"], 12);
    assert_eq!(message_delta["usage"]["output_tokens"], 5);

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
    let thinking_progress_tokens: Vec<u64> = anthropic_events
        .iter()
        .filter(|event| event["type"] == "message_delta" && event["delta"]["stop_reason"].is_null())
        .filter_map(|event| event["usage"]["output_tokens"].as_u64())
        .collect();
    assert!(
        !thinking_progress_tokens.is_empty(),
        "expected progressive output-token usage while thinking"
    );
    assert!(
        thinking_progress_tokens
            .windows(2)
            .all(|pair| pair[0] < pair[1]),
        "thinking progress output tokens must increase monotonically: {thinking_progress_tokens:?}"
    );

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].stream, true);
    assert_eq!(requests[0].reasoning_effort.as_deref(), Some("low"));
    assert!(requests[0].extra_body.is_empty());
}

#[tokio::test]
async fn anthropic_messages_preserves_image_content_parts() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
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
        .filter_map(|event| {
            (event["type"] == "content_block_delta" && event["delta"]["type"] == "input_json_delta")
                .then(|| event["delta"]["partial_json"].as_str().unwrap())
        })
        .collect();
    assert_eq!(json_deltas, vec![r#"{"loc"#, r#"ation":"Seattle"}"#]);
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

#[tokio::test]
async fn responses_preserves_multimodal_input_parts() {
    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
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
    let gateway = test_gateway(upstream.clone(), MockSearch::default());
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
async fn anthropic_messages_lifts_claude_code_skill_listing_before_user_prompt() {
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
    assert_eq!(roles, vec!["system", "user"]);
    let system = messages[0]
        .content
        .as_ref()
        .and_then(|value| value.as_str())
        .expect("system content");
    assert!(system.contains("skill listing"));
    assert!(system.contains("security-review"));
    assert!(system.contains("Do not quote"));
    assert_eq!(
        messages[1]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("hello")
    );
}

#[tokio::test]
async fn anthropic_messages_lifts_late_system_skill_listing_before_user_prompt() {
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
    assert_eq!(roles, vec!["system", "user"]);
    let system = messages[0]
        .content
        .as_ref()
        .and_then(|value| value.as_str())
        .expect("system content");
    assert!(system.contains("You are Claude Code."));
    assert!(system.contains("skill listing"));
    assert!(system.contains("security-review"));
    assert!(system.contains("Do not quote"));
    assert_eq!(
        messages[1]
            .content
            .as_ref()
            .and_then(|value| value.as_str()),
        Some("hello")
    );
}

#[tokio::test]
async fn cancels_mid_stream_when_client_disconnects() {
    let upstream = PendingChunkUpstream::new();
    let stream_polled = upstream.stream_polled.notified();
    let gateway = Arc::new(Gateway::new(
        Config {
            bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
            upstream_base_url: "http://127.0.0.1:8000/v1".parse().expect("url"),
            upstream_api_key: None,
            upstream_model: None,
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profiles: std::collections::BTreeMap::new(),
            brave_base_url: "https://example.com/".parse().expect("url"),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout: std::time::Duration::from_secs(30),
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            compaction: Default::default(),
        },
        ReplayStore::new(1000),
        Arc::new(upstream.clone()),
        Arc::new(MockSearch::default()),
        MonitorHub::new(128),
        None,
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
        parallel_tool_calls: true,
        reasoning: None,
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

// ---- context compaction (opt-in external compactor) -------------------------

fn compaction_test_config(endpoint: String, max_input_tokens: usize) -> Config {
    Config {
        bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
        upstream_base_url: "http://127.0.0.1:8000/v1".parse().expect("url"),
        upstream_api_key: None,
        upstream_model: Some("test-model".to_string()),
        upstream_request_log_path: None,
        upstream_chat_kwargs: JsonMap::new(),
        system_prompt_prefix: None,
        upstreams: Vec::new(),
        fallback_upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        model_profiles: std::collections::BTreeMap::new(),
        brave_base_url: "https://example.com/".parse().expect("url"),
        brave_api_key: None,
        brave_max_results: 5,
        request_timeout: std::time::Duration::from_secs(30),
        connect_timeout_secs: 10,
        max_web_search_rounds: 5,
        flatten_content: true,
        max_replay_entries: 1000,
        compaction: llmconduit::compaction::CompactionConfig {
            enabled: true,
            mode: "external".to_string(),
            endpoint: Some(endpoint),
            max_input_tokens,
            keep_recent_turns: 1,
            timeout_ms: 5000,
        },
    }
}

fn upstream_message_blob(requests: &[ChatCompletionRequest]) -> Vec<String> {
    requests[0]
        .messages
        .iter()
        .filter_map(|m| m.content.as_ref().map(|c| c.to_string()))
        .collect()
}

#[tokio::test]
async fn over_budget_request_is_compacted_before_upstream() {
    let compactor = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/compact"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "compacted": true,
            "messages": [ { "role": "user", "content": "COMPACTED_STATE_BLOCK" } ]
        })))
        .mount(&compactor)
        .await;

    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway_with_config(
        upstream.clone(),
        MockSearch::default(),
        compaction_test_config(compactor.uri(), 5), // tiny budget -> always triggers
    );

    let _ = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message(
                "this user message is comfortably longer than the tiny budget",
            )]))
            .await
            .expect("stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let blob = upstream_message_blob(&requests);
    assert!(
        blob.iter().any(|c| c.contains("COMPACTED_STATE_BLOCK")),
        "upstream should receive the COMPACTED messages, got {blob:?}"
    );
}

#[tokio::test]
async fn compaction_failure_forwards_original_request() {
    let compactor = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/compact"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&compactor)
        .await;

    let upstream = MockUpstream::default();
    upstream
        .push_response(vec![Ok(content_chunk("chat-1", "ok"))])
        .await;
    let gateway = test_gateway_with_config(
        upstream.clone(),
        MockSearch::default(),
        compaction_test_config(compactor.uri(), 5),
    );

    let _ = collect_stream(
        gateway
            .stream_responses(base_request(vec![user_message(
                "ORIGINAL_USER_TEXT that also exceeds the tiny budget for sure",
            )]))
            .await
            .expect("stream"),
    )
    .await;

    let requests = upstream.requests().await;
    assert_eq!(requests.len(), 1);
    let blob = upstream_message_blob(&requests);
    assert!(
        blob.iter().any(|c| c.contains("ORIGINAL_USER_TEXT")),
        "compactor failure must forward the ORIGINAL request, got {blob:?}"
    );
}
