//! Shared fixtures for the claude-relay -> llmconduit behavior port.
//!
//! Authored fresh (not extracted from `gateway.rs`) so the per-surface
//! `tests/port_*.rs` files can share mocks, chunk builders, and SSE collectors
//! without touching the existing 5700-line integration suite.
//!
//! Each `tests/*.rs` file compiles as its own crate, so not every surface uses
//! every helper here -- `allow(dead_code)` keeps unused-in-this-crate helpers
//! from failing the build.
#![allow(dead_code)]

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream;
use llmconduit::config::Config;
use llmconduit::config::PersistedConfig;
use llmconduit::engine::Gateway;
use llmconduit::engine::SseEvent;
use llmconduit::error::AppError;
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
use llmconduit::models::responses::ReasoningRequest;
use llmconduit::models::responses::ResponseItem;
use llmconduit::models::responses::ResponsesRequest;
use llmconduit::monitor::MonitorHub;
use llmconduit::replay::ReplayStore;
use llmconduit::search::SearchClient;
use llmconduit::search::SearchOutcome;
use llmconduit::search::SearchSource;
use llmconduit::upstream::UpstreamClient;
use llmconduit::upstream::UpstreamModelEntry;
use llmconduit::upstream::UpstreamStream;
use serde_json::Map as JsonMap;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;

/// One queued upstream turn: the ordered chunk results a single
/// `stream_chat_completion` call will yield.
type ChunkBatch = Vec<Result<ChatCompletionChunk, AppError>>;

// ---------------------------------------------------------------------------
// Mock upstream: queues canned chunk batches, records requests it received.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct MockUpstream {
    requests: Arc<Mutex<Vec<ChatCompletionRequest>>>,
    responses: Arc<Mutex<VecDeque<ChunkBatch>>>,
    supported_models: Arc<Mutex<Vec<String>>>,
    supported_model_queries: Arc<Mutex<usize>>,
    context_limits: Arc<Mutex<Vec<(String, i64)>>>,
    /// Per-model finalization policies (effort/family/kwargs), built from the
    /// test config by the gateway harness so the mock's leaf-mirror applies the
    /// SAME profile kwargs the production leaf would (T1). Empty by default.
    finalization_policies: Arc<std::sync::Mutex<llmconduit::upstream::BackendFinalizationPolicies>>,
}

impl MockUpstream {
    pub async fn push_response(&self, chunks: ChunkBatch) {
        self.responses.lock().await.push_back(chunks);
    }

    /// Set the finalization policies built from the test config, so the mock's
    /// leaf-mirror applies the same profile/family/effort kwargs the production
    /// leaf would (T1).
    pub fn set_finalization_policies(
        &self,
        policies: llmconduit::upstream::BackendFinalizationPolicies,
    ) {
        *self.finalization_policies.lock().expect("policies lock") = policies;
    }

    pub async fn requests(&self) -> Vec<ChatCompletionRequest> {
        self.requests.lock().await.clone()
    }

    pub async fn set_supported_models<I, S>(&self, models: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        *self.supported_models.lock().await = models.into_iter().map(Into::into).collect();
    }

    pub async fn supported_model_queries(&self) -> usize {
        *self.supported_model_queries.lock().await
    }

    /// Supply per-model context-window lengths for G3 pre-flight budgeting.
    pub async fn set_context_limits<I, S>(&self, limits: I)
    where
        I: IntoIterator<Item = (S, i64)>,
        S: Into<String>,
    {
        *self.context_limits.lock().await =
            limits.into_iter().map(|(id, n)| (id.into(), n)).collect();
    }
}

#[async_trait]
impl UpstreamClient for MockUpstream {
    async fn stream_chat_completion(
        &self,
        backend: &llmconduit::upstream::BackendChatRequest,
    ) -> Result<UpstreamStream, AppError> {
        // Mirror the production leaf (`ReqwestUpstreamClient`): family-specific
        // `chat_template_kwargs` (G2) are injected in the upstream client from
        // the FINAL provider model, so the recorded request reflects what the
        // backend would actually receive.
        let mut backend = backend.clone();
        // Mirror the production leaf finalize (clamp/map reasoning effort + inject
        // family kwargs from the FINAL model, apply per-model upstream_chat_kwargs)
        // so the recorded request reflects what the backend would receive. The
        // policies are built from the test config by the gateway harness (T1).
        let policies = self
            .finalization_policies
            .lock()
            .expect("policies lock")
            .clone();
        llmconduit::upstream::finalize_request_for_backend(&mut backend, &policies);
        let request = &backend.request;
        self.requests.lock().await.push(request.clone());
        let chunks = self
            .responses
            .lock()
            .await
            .pop_front()
            .expect("queued upstream response");
        Ok(Box::pin(stream::iter(chunks)))
    }

    async fn list_models(&self) -> Result<reqwest::Response, AppError> {
        Err(AppError::internal("unused in this test"))
    }

    async fn supported_model_catalog(&self) -> Result<Vec<UpstreamModelEntry>, AppError> {
        *self.supported_model_queries.lock().await += 1;
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

    // G3 budgeting projection: a single `requested_model` candidate carrying its
    // configured context limit (from `set_context_limits`). The trait default
    // returns `None`, which would make budgeting a no-op.
    async fn backend_candidate_plan(
        &self,
        requested_model: &str,
    ) -> llmconduit::upstream::BackendCandidatePlan {
        let limits = self.context_limits.lock().await.clone();
        let context_limit = limits
            .iter()
            .find(|(id, _)| id == requested_model)
            .map(|(_, limit)| *limit);
        let candidates = vec![llmconduit::upstream::BackendCandidate {
            model: requested_model.to_string(),
            context_limit,
        }];
        llmconduit::upstream::BackendCandidatePlan { candidates }
    }
}

// ---------------------------------------------------------------------------
// Mock Brave search: records queries, returns a deterministic outcome.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct MockSearch {
    queries: Arc<Mutex<Vec<String>>>,
}

impl MockSearch {
    pub async fn queries(&self) -> Vec<String> {
        self.queries.lock().await.clone()
    }
}

#[async_trait]
impl SearchClient for MockSearch {
    async fn search(&self, query: &str) -> Result<SearchOutcome, AppError> {
        self.queries.lock().await.push(query.to_string());
        Ok(SearchOutcome {
            formatted: format!("Search result for {query}"),
            sources: vec![SearchSource {
                title: format!("Result for {query}"),
                url: "https://example.com/result".to_string(),
            }],
        })
    }
}

// ---------------------------------------------------------------------------
// Gateway / Config / request builders.
// ---------------------------------------------------------------------------

pub fn test_gateway(upstream: MockUpstream, search: MockSearch) -> Arc<Gateway> {
    test_gateway_with_config(upstream, search, test_config())
}

pub fn test_gateway_with_config(
    upstream: MockUpstream,
    search: MockSearch,
    config: Config,
) -> Arc<Gateway> {
    // Build the leaf finalization policies from the test config so the mock's
    // leaf-mirror applies the SAME profile/family/effort kwargs the production
    // leaf would (T1 moved profile resolution from the engine to the leaf).
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    // The shared image cache satisfies the constructor; a profile without
    // `image_analysis` never activates the agent, so these port_* tests pass
    // images through untouched.
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(Gateway::new(
        config,
        ReplayStore::new(1000),
        Arc::new(upstream),
        Arc::new(search),
        image_cache,
        MonitorHub::new(128),
        None,
        llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
    ))
}

/// Like [`test_gateway_with_config`], but takes an EXTERNALLY-owned
/// `ReplayStore` instead of minting a fresh one, so a test can retain its own
/// clone (it wraps an `Arc<RwLock<..>>` internally, so cloning shares state)
/// and inspect the store directly after a turn — e.g. proving a degraded turn
/// (E2b) never wrote a replay entry, without needing to construct an
/// observably-different second turn to detect a would-be cache hit.
pub fn test_gateway_with_config_and_replay_store(
    upstream: MockUpstream,
    search: MockSearch,
    config: Config,
    replay_store: ReplayStore,
) -> Arc<Gateway> {
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(Gateway::new(
        config,
        replay_store,
        Arc::new(upstream),
        Arc::new(search),
        image_cache,
        MonitorHub::new(128),
        None,
        llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
    ))
}

// ---------------------------------------------------------------------------
// G4 image-agent fixtures: the gateway / config / request builders the
// `tests/image_agent.rs` suite shares. The analyzer is dispatched through the
// gateway's own upstreams, so the suite asserts against the mock upstream (or a
// second wiremock server) rather than a dedicated vision-client mock.
// ---------------------------------------------------------------------------

/// Build a gateway from a `MockUpstream` and an explicit config, wiring a shared
/// `ImageCache` sized from the config. Both the strip seam and the `analyzeImage`
/// executor read that one cache. The analyzer's own dispatch goes back through
/// this same `MockUpstream`, so image-agent tests queue the analyzer round on it.
pub fn image_agent_gateway(upstream: MockUpstream, config: Config) -> Arc<Gateway> {
    upstream.set_finalization_policies(
        llmconduit::upstream::BackendFinalizationPolicies::from_config(&config),
    );
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(Gateway::new(
        config,
        ReplayStore::new(1000),
        Arc::new(upstream),
        Arc::new(MockSearch::default()),
        image_cache,
        MonitorHub::new(128),
        None,
        llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
    ))
}

/// Config whose catch-all profile redirects images to an `analyzer` profile via
/// `image_analysis`, so a request resolving to it activates the image agent
/// (strip + `analyzeImage`). `residual_images` sets the policy the residual
/// sweep applies to an image the strip did not consume. `web_search` is
/// disabled so the image agent is isolated from web-search gating.
pub fn image_analysis_config(residual: llmconduit::config::ResidualImagePolicy) -> Config {
    let mut config = test_config();
    config.brave_api_key = None;
    let text_profile = llmconduit::config::ModelProfile {
        image_analysis: Some(llmconduit::config::ImageAnalysisConfig {
            model: "analyzer".to_string(),
            residual_images: residual,
        }),
        ..llmconduit::config::ModelProfile::default()
    };
    config.model_profiles = vec![
        llmconduit::config::CompiledProfile {
            key: "*".to_string(),
            glob: llmconduit::config::compile_model_glob("*").expect("catch-all glob"),
            profile: text_profile,
        },
        llmconduit::config::CompiledProfile {
            key: "analyzer".to_string(),
            glob: None,
            profile: llmconduit::config::ModelProfile::default(),
        },
    ];
    config
}

/// A user message carrying a single `input_image` (a data URL), the shape the
/// canonical pipeline produces for an uploaded image.
pub fn user_message_with_image(text: &str, data_url: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![
            ContentItem::InputText {
                text: text.to_string(),
            },
            ContentItem::InputImage {
                image_url: Some(data_url.to_string()),
                file_id: None,
                detail: None,
            },
        ],
        phase: None,
    }
}

pub const TEST_IMAGE_DATA_URL: &str =
    "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA=";

pub fn test_config() -> Config {
    Config {
        bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
        system_prompt_prefix: None,
        upstream_request_log_path: None,
        turn_capture_dir: None,
        upstream_chat_kwargs: JsonMap::new(),
        upstreams: Vec::new(),
        upstream_failure_cooldown_secs: 30,
        // A `"*"` catch-all so a bare request model resolves to a pass-through
        // route (served model = request model). Tests that inject their own
        // upstream client bypass the router, but the engine still resolves the
        // served-model label through the profiles.
        model_profiles: vec![llmconduit::config::CompiledProfile {
            key: "*".to_string(),
            glob: llmconduit::config::compile_model_glob("*").expect("catch-all glob"),
            profile: llmconduit::config::ModelProfile::default(),
        }],
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
        image_cache_max_size: 100,
        image_cache_ttl_secs: 300,
        price_table: std::collections::HashMap::new(),
    }
}

/// Build a `Config` from inline YAML, applying the standard `from_persisted`
/// resolution. Keeps routing/profile resolution identical to production for the
/// config + routing test suites.
pub fn config_from_yaml(yaml: &str) -> Config {
    let persisted: PersistedConfig = serde_yaml::from_str(yaml).expect("yaml config");
    Config::from_persisted(&persisted).expect("resolve config")
}

pub fn base_request(input: Vec<ResponseItem>) -> ResponsesRequest {
    ResponsesRequest {
        model: "glm-5.1".to_string(),
        instructions: String::new(),
        input,
        tools: Vec::new(),
        tool_choice: json!("auto"),
        parallel_tool_calls: Some(true),
        reasoning: Some(ReasoningRequest {
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

pub fn user_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        phase: None,
    }
}

// ---------------------------------------------------------------------------
// Upstream chunk builders (mirror the OpenAI chat-completions chunk shape).
// ---------------------------------------------------------------------------

pub fn content_chunk(id: &str, content: &str) -> ChatCompletionChunk {
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

pub fn finish_chunk(id: &str, finish_reason: &str) -> ChatCompletionChunk {
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
            finish_reason: Some(finish_reason.to_string()),
            stop_reason: None,
        }],
        usage: None,
    }
}

pub fn reasoning_chunk(id: &str, reasoning: &str) -> ChatCompletionChunk {
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

pub fn nested_thinking_chunk(id: &str, thinking: &str, signature: &str) -> ChatCompletionChunk {
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
                extra: BTreeMap::from([(
                    "thinking".to_string(),
                    json!({ "content": thinking, "signature": signature }),
                )]),
            },
            finish_reason: None,
            stop_reason: None,
        }],
        usage: None,
    }
}

pub fn tool_call_chunk(
    id: &str,
    call_id: &str,
    name: &str,
    arguments: &str,
) -> ChatCompletionChunk {
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
                        arguments: Some(Value::String(arguments.to_string())),
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

/// Deliberately slimmer than gateway.rs's `usage_chunk` (4 args vs 6): omits the
/// `cached_tokens` / `reasoning_tokens` detail fields. Add them here if a
/// usage-detail surface is ported.
pub fn usage_chunk(
    id: &str,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
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
            prompt_tokens_details: None::<PromptTokensDetails>,
            completion_tokens_details: None::<CompletionTokensDetails>,
        }),
        choices: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// SSE collectors. Canonical Responses events carry an injected `_event` key so
// tests can assert on the event-name sequence as well as the payload.
// ---------------------------------------------------------------------------

pub async fn collect_stream(stream: ReceiverStream<SseEvent>) -> Vec<Value> {
    stream
        .map(|event| {
            let mut value = event.data;
            if let Value::Object(map) = &mut value {
                map.insert("_event".to_string(), Value::String(event.event));
            }
            value
        })
        .collect()
        .await
}

pub fn event_names(events: &[Value]) -> Vec<&str> {
    events
        .iter()
        .map(|event| event["_event"].as_str().expect("event name present"))
        .collect()
}

pub fn done_items(events: &[Value]) -> Vec<ResponseItem> {
    events
        .iter()
        .filter(|event| event["_event"] == "response.output_item.done")
        .map(|event| serde_json::from_value(event["item"].clone()).expect("response item"))
        .collect()
}

pub fn parse_anthropic_sse_events(body: &str) -> Vec<Value> {
    body.split("\n\n")
        .filter_map(|block| {
            block.lines().find_map(|line| {
                line.strip_prefix("data: ")
                    .map(|data| serde_json::from_str(data).expect("valid Anthropic SSE JSON"))
            })
        })
        .collect()
}

pub fn parse_chat_sse_events(body: &str) -> Vec<Value> {
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

/// Parse a raw `/v1/responses` streaming SSE body into its `data:` JSON
/// payloads, one per event (each payload's own `"type"` field mirrors the
/// SSE `event:` line, so callers filter on `event["type"]` rather than
/// needing the frame's `event:` line separately).
pub fn parse_responses_sse_events(body: &str) -> Vec<Value> {
    body.split("\n\n")
        .filter_map(|block| {
            block.lines().find_map(|line| {
                line.strip_prefix("data: ")
                    .map(|data| serde_json::from_str(data).expect("valid Responses SSE JSON"))
            })
        })
        .collect()
}

/// Serialize raw chunk JSON values into an OpenAI chat-completions SSE body
/// (`data: {...}` frames terminated by `data: [DONE]`), the wire shape a wiremock
/// upstream returns. Inverse of [`parse_chat_sse_events`].
pub fn chat_completion_sse_body(chunks: &[Value]) -> String {
    let mut body = String::new();
    for chunk in chunks {
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(chunk).expect("serialize chat chunk"));
        body.push_str("\n\n");
    }
    body.push_str("data: [DONE]\n\n");
    body
}
