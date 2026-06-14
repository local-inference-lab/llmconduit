use crate::adapters::chat_to_responses::FinalizedAssistantTurn;
use crate::adapters::chat_to_responses::ResolvedToolCall;
use crate::adapters::chat_to_responses::StreamEmission;
use crate::adapters::chat_to_responses::StreamState;
use crate::adapters::responses_to_chat::ToolKind;
use crate::adapters::responses_to_chat::lower_request;
use crate::config::Config;
use crate::error::AppError;
use crate::error::AppResult;
use crate::models::chat::ChatCompletionChunk;
use crate::models::chat::ChatCompletionRequest;
use crate::models::chat::ChatMessage;
use crate::models::chat::ChunkUsage;
use crate::models::chat::StreamOptions;
use crate::models::responses::DeltaPayload;
use crate::models::responses::FailedError;
use crate::models::responses::FailedPayload;
use crate::models::responses::FailedResponse;
use crate::models::responses::OutputItemPayload;
use crate::models::responses::ReasoningDeltaPayload;
use crate::models::responses::ReasoningSignatureDeltaPayload;
use crate::models::responses::ResponseCompletedPayload;
use crate::models::responses::ResponseCreatedPayload;
use crate::models::responses::ResponseInputTokensDetails;
use crate::models::responses::ResponseItem;
use crate::models::responses::ResponseOutputTokensDetails;
use crate::models::responses::ResponseResource;
use crate::models::responses::ResponseStub;
use crate::models::responses::ResponseUsage;
use crate::models::responses::ResponsesEnvelope;
use crate::models::responses::ResponsesRequest;
use crate::models::responses::WebSearchAction;
use crate::monitor::DebugEventImage;
use crate::monitor::MonitorEventKind;
use crate::monitor::MonitorHub;
use crate::raw::RawOutput;
use crate::replay::ReplayRecord;
use crate::replay::ReplayStore;
use crate::search::SearchClient;
use crate::search::SearchOutcome;
use crate::upstream::UpstreamClient;
use crate::upstream::canonical_model_key;
use crate::upstream::sanitize_chat_request;
use futures::StreamExt;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

const UPSTREAM_MODEL_CATALOG_TTL_SECS: u64 = 300;

#[derive(Clone)]
pub struct Gateway {
    config: Config,
    replay_store: ReplayStore,
    upstream: Arc<dyn UpstreamClient>,
    search: Arc<dyn SearchClient>,
    monitor: MonitorHub,
    raw_output: Option<RawOutput>,
    upstream_model_catalog: Arc<Mutex<Option<CachedUpstreamModelCatalog>>>,
}

#[derive(Debug, Clone)]
pub struct SseEvent {
    pub event: String,
    pub data: Value,
}

#[derive(Clone)]
struct CachedUpstreamModelCatalog {
    fetched_at: std::time::Instant,
    catalog: UpstreamModelCatalog,
}

#[derive(Clone, Default)]
struct UpstreamModelCatalog {
    ids: Vec<String>,
    ids_by_key: HashMap<String, Vec<String>>,
}

impl UpstreamModelCatalog {
    fn from_ids(ids: Vec<String>) -> Self {
        let mut ids_by_key: HashMap<String, Vec<String>> = HashMap::new();
        for id in &ids {
            let key = canonical_model_key(id);
            if key.is_empty() {
                continue;
            }
            ids_by_key.entry(key).or_default().push(id.clone());
        }
        Self { ids, ids_by_key }
    }

    fn normalize(&self, model: &str) -> Option<String> {
        let trimmed = model.trim();
        if !trimmed.is_empty() {
            if let Some(exact) = self.ids.iter().find(|id| id.as_str() == trimmed) {
                return Some(exact.clone());
            }
            let key = canonical_model_key(trimmed);
            if let Some(matches) = self.ids_by_key.get(&key) {
                let unique_ids = matches
                    .iter()
                    .map(String::as_str)
                    .collect::<std::collections::HashSet<_>>();
                if unique_ids.len() == 1 {
                    return matches.first().cloned();
                }
            }
        }
        self.ids.first().cloned()
    }
}

fn build_upstream_extra_body(
    defaults: serde_json::Map<String, Value>,
    request: &ResponsesRequest,
    response_format: &Option<Value>,
    reasoning_effort: &Option<String>,
) -> BTreeMap<String, Value> {
    let mut extra_body = defaults.into_iter().collect();
    remove_defaults_for_explicit_request_fields(
        &mut extra_body,
        request,
        response_format,
        reasoning_effort,
    );
    remove_defaults_shadowed_by_request_extra(&mut extra_body, &request.extra_body);
    for (key, value) in &request.extra_body {
        merge_request_extra_value(&mut extra_body, key, value);
    }
    extra_body
}

fn remove_defaults_for_explicit_request_fields(
    extra_body: &mut BTreeMap<String, Value>,
    request: &ResponsesRequest,
    response_format: &Option<Value>,
    reasoning_effort: &Option<String>,
) {
    if request.temperature.is_some() {
        remove_keys(extra_body, &["temperature"]);
    }
    if request.top_p.is_some() {
        remove_keys(extra_body, &["top_p"]);
    }
    if request.max_output_tokens.is_some() {
        remove_keys(
            extra_body,
            &["max_tokens", "max_output_tokens", "max_completion_tokens"],
        );
    }
    if request.frequency_penalty.is_some() {
        remove_keys(extra_body, &["frequency_penalty"]);
    }
    if request.presence_penalty.is_some() {
        remove_keys(extra_body, &["presence_penalty"]);
    }
    if response_format.is_some() {
        remove_keys(extra_body, &["response_format"]);
    }
    if reasoning_effort.is_some() {
        remove_keys(extra_body, &["reasoning_effort"]);
    }
}

fn remove_defaults_shadowed_by_request_extra(
    extra_body: &mut BTreeMap<String, Value>,
    request_extra: &BTreeMap<String, Value>,
) {
    for aliases in [&["max_tokens", "max_output_tokens", "max_completion_tokens"][..]] {
        if aliases.iter().any(|key| request_extra.contains_key(*key)) {
            remove_keys(extra_body, aliases);
        }
    }
}

fn remove_keys(extra_body: &mut BTreeMap<String, Value>, keys: &[&str]) {
    for key in keys {
        extra_body.remove(*key);
    }
}

fn merge_request_extra_value(extra_body: &mut BTreeMap<String, Value>, key: &str, value: &Value) {
    if key == "chat_template_kwargs"
        && let Some(existing) = extra_body.get_mut(key)
    {
        merge_json_value_prefer_source(existing, value);
        return;
    }
    extra_body.insert(key.to_string(), value.clone());
}

fn merge_json_value_prefer_source(destination: &mut Value, source: &Value) {
    if let Value::Object(destination_object) = destination
        && let Value::Object(source_object) = source
    {
        for (key, source_value) in source_object {
            match destination_object.get_mut(key) {
                Some(destination_value) => {
                    merge_json_value_prefer_source(destination_value, source_value);
                }
                None => {
                    destination_object.insert(key.clone(), source_value.clone());
                }
            }
        }
        return;
    }
    *destination = source.clone();
}

impl Gateway {
    pub fn new(
        config: Config,
        replay_store: ReplayStore,
        upstream: Arc<dyn UpstreamClient>,
        search: Arc<dyn SearchClient>,
        monitor: MonitorHub,
        raw_output: Option<RawOutput>,
    ) -> Self {
        Self {
            config,
            replay_store,
            upstream,
            search,
            monitor,
            raw_output,
            upstream_model_catalog: Arc::new(Mutex::new(None)),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn upstream_client(&self) -> Arc<dyn UpstreamClient> {
        Arc::clone(&self.upstream)
    }

    pub async fn resolve_request_model(&self, request_model: &str) -> String {
        let configured_model = self.config.resolve_upstream_model(request_model);
        self.normalize_upstream_model(&configured_model).await
    }

    pub fn subscribe_monitor(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::monitor::DebugUpdate> {
        self.monitor.subscribe()
    }

    pub fn debug_snapshot(&self) -> crate::monitor::DebugSnapshot {
        self.monitor.snapshot()
    }

    async fn send_event(
        &self,
        tx: &mpsc::Sender<SseEvent>,
        event: SseEvent,
        failure_message: &'static str,
    ) -> AppResult<()> {
        let raw_event = event.clone();
        tx.send(event)
            .await
            .map_err(|_| AppError::internal(failure_message))?;
        if let Some(raw_output) = &self.raw_output {
            raw_output
                .write_sse_event(&raw_event)
                .map_err(|err| AppError::internal(format!("failed to write raw output: {err}")))?;
        }
        Ok(())
    }

    pub async fn stream_responses(
        self: Arc<Self>,
        request: ResponsesRequest,
    ) -> AppResult<ReceiverStream<SseEvent>> {
        let resolved_model = self.resolve_request_model(&request.model).await;
        let mut request = self.apply_system_prompt_prefix(request, &resolved_model);
        let (baseline_record, prefix_len) = self.find_replay_baseline(&request).await?;
        let mut tail_request = request.clone();
        tail_request.input = request.input[prefix_len..].to_vec();
        if self.config.brave_api_key.is_none() {
            let original_tool_count = tail_request.tools.len();
            tail_request
                .tools
                .retain(|t| !matches!(t, crate::models::responses::ToolSpec::WebSearch { .. }));
            if tail_request.tools.len() != original_tool_count {
                relax_tool_choice_after_stripping_tool(
                    &mut tail_request.tool_choice,
                    "web_search",
                    tail_request.tools.is_empty(),
                );
                relax_tool_choice_after_stripping_tool(
                    &mut request.tool_choice,
                    "web_search",
                    tail_request.tools.is_empty(),
                );
            }
        }
        let lowered = lower_request(
            &tail_request,
            baseline_record
                .as_ref()
                .map(|record| record.internal_messages.clone())
                .unwrap_or_default(),
        )?;

        let (tx, rx) = mpsc::channel(128);
        let gateway = Arc::clone(&self);
        let response_id = format!("resp_{}", Uuid::new_v4().simple());
        tokio::spawn(async move {
            let result = gateway
                .run_turn(
                    response_id.clone(),
                    request,
                    lowered.messages,
                    lowered.tools,
                    lowered.tool_registry,
                    lowered.response_format,
                    lowered.reasoning_effort,
                    resolved_model,
                    tx.clone(),
                )
                .await;
            if let Err(err) = &result {
                if tx.is_closed() {
                    gateway.monitor.emit(
                        response_id,
                        MonitorEventKind::Failed {
                            message: "client disconnected".to_string(),
                        },
                    );
                    return;
                }
                gateway.monitor.emit(
                    response_id,
                    MonitorEventKind::Failed {
                        message: err.to_string(),
                    },
                );
                let _ = gateway
                    .send_event(&tx, failure_event(err), "failed to send response.failed")
                    .await;
            }
        });
        Ok(ReceiverStream::new(rx))
    }

    fn apply_system_prompt_prefix(
        &self,
        mut request: ResponsesRequest,
        resolved_model: &str,
    ) -> ResponsesRequest {
        let Some(prefix) = self
            .config
            .resolve_system_prompt_prefix_for_resolved_model(&request.model, resolved_model)
        else {
            return request;
        };
        request.instructions = if request.instructions.is_empty() {
            prefix
        } else {
            format!("{prefix}\n\n{}", request.instructions)
        };
        request
    }

    async fn find_replay_baseline(
        &self,
        request: &ResponsesRequest,
    ) -> AppResult<(Option<ReplayRecord>, usize)> {
        if !request.store {
            return Ok((None, 0));
        }
        let record = self
            .replay_store
            .longest_prefix_match(&request.model, &request.instructions, &request.input)
            .await;
        if let Some(record) = record {
            let prefix_len = record.visible_history.len();
            return Ok((Some(record), prefix_len));
        }
        Ok((None, 0))
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_turn(
        &self,
        response_id: String,
        request: ResponsesRequest,
        mut current_messages: Vec<ChatMessage>,
        tools: Vec<crate::models::chat::ChatTool>,
        tool_registry: crate::adapters::responses_to_chat::ToolRegistry,
        response_format: Option<Value>,
        reasoning_effort: Option<String>,
        upstream_model: String,
        tx: mpsc::Sender<SseEvent>,
    ) -> AppResult<()> {
        self.monitor.emit(
            response_id.clone(),
            MonitorEventKind::RequestStarted {
                model: request.model.clone(),
                input_items: request.input.len(),
                tool_count: request.tools.len(),
                turn_count: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "user"
                        )
                    })
                    .count(),
                user_messages: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "user"
                        )
                    })
                    .count(),
                assistant_messages: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "assistant"
                        )
                    })
                    .count(),
                system_messages: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "system"
                        )
                    })
                    .count(),
                developer_messages: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "developer"
                        )
                    })
                    .count(),
                reasoning_items: request
                    .input
                    .iter()
                    .filter(|item| matches!(item, ResponseItem::Reasoning { .. }))
                    .count(),
                function_calls: request
                    .input
                    .iter()
                    .filter(|item| matches!(item, ResponseItem::FunctionCall { .. }))
                    .count(),
                function_outputs: request
                    .input
                    .iter()
                    .filter(|item| matches!(item, ResponseItem::FunctionCallOutput { .. }))
                    .count(),
                tool_items: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::FunctionCall { .. }
                                | ResponseItem::FunctionCallOutput { .. }
                                | ResponseItem::CustomToolCall { .. }
                                | ResponseItem::CustomToolCallOutput { .. }
                                | ResponseItem::ToolSearchCall { .. }
                                | ResponseItem::ToolSearchOutput { .. }
                                | ResponseItem::LocalShellCall { .. }
                                | ResponseItem::WebSearchCall { .. }
                                | ResponseItem::ImageGenerationCall { .. }
                        )
                    })
                    .count(),
                input_chars: request
                    .input
                    .iter()
                    .map(|item| match item {
                        ResponseItem::Message { content, .. } => content
                            .iter()
                            .map(|content| match content {
                                crate::models::responses::ContentItem::InputText { text }
                                | crate::models::responses::ContentItem::OutputText { text } => {
                                    text.chars().count()
                                }
                                crate::models::responses::ContentItem::InputImage {
                                    image_url,
                                    file_id,
                                    detail,
                                } => image_url
                                    .iter()
                                    .chain(file_id.iter())
                                    .chain(detail.iter())
                                    .map(|value| value.chars().count())
                                    .sum(),
                                crate::models::responses::ContentItem::InputFile {
                                    file_id,
                                    file_url,
                                    filename,
                                    file_data,
                                } => file_id
                                    .iter()
                                    .chain(file_url.iter())
                                    .chain(filename.iter())
                                    .chain(file_data.iter())
                                    .map(|value| value.chars().count())
                                    .sum(),
                                crate::models::responses::ContentItem::Other(value) => {
                                    value.to_string().chars().count()
                                }
                            })
                            .sum::<usize>(),
                        ResponseItem::Reasoning { content, .. } => content
                            .as_ref()
                            .map(|items| {
                                items.iter()
                                    .map(|item| match item {
                                        crate::models::responses::ReasoningContentItem::ReasoningText {
                                            text,
                                        }
                                        | crate::models::responses::ReasoningContentItem::Text {
                                            text,
                                        } => text.chars().count(),
                                    })
                                    .sum()
                            })
                            .unwrap_or(0),
                        ResponseItem::FunctionCall {
                            name, arguments, ..
                        } => name.chars().count() + arguments.chars().count(),
                        ResponseItem::FunctionCallOutput { call_id, output } => {
                            call_id.chars().count() + output.to_string().chars().count()
                        }
                        ResponseItem::CustomToolCall { name, input, .. } => {
                            name.chars().count() + input.chars().count()
                        }
                        ResponseItem::CustomToolCallOutput {
                            call_id,
                            name,
                            output,
                        } => {
                            call_id.chars().count()
                                + name.as_ref().map(|name| name.chars().count()).unwrap_or(0)
                                + output.to_string().chars().count()
                        }
                        ResponseItem::ToolSearchCall { arguments, .. } => {
                            arguments.to_string().chars().count()
                        }
                        ResponseItem::ToolSearchOutput { tools, .. } => tools
                            .iter()
                            .map(|tool| tool.to_string().chars().count())
                            .sum(),
                        ResponseItem::LocalShellCall { action, .. } => match action {
                            crate::models::responses::LocalShellAction::Exec(exec) => exec
                                .command
                                .iter()
                                .map(|part| part.chars().count())
                                .sum(),
                        },
                        ResponseItem::WebSearchCall { action, .. } => action
                            .as_ref()
                            .map(|action| match action {
                                crate::models::responses::WebSearchAction::Search {
                                    query,
                                    queries,
                                } => {
                                    query.as_ref().map(|q| q.chars().count()).unwrap_or(0)
                                        + queries
                                            .as_ref()
                                            .map(|queries| {
                                                queries
                                                    .iter()
                                                    .map(|query| query.chars().count())
                                                    .sum()
                                            })
                                            .unwrap_or(0)
                                }
                                crate::models::responses::WebSearchAction::OpenPage {
                                    url,
                                } => url.as_ref().map(|url| url.chars().count()).unwrap_or(0),
                                crate::models::responses::WebSearchAction::FindInPage {
                                    url,
                                    pattern,
                                } => {
                                    url.as_ref().map(|url| url.chars().count()).unwrap_or(0)
                                        + pattern
                                            .as_ref()
                                            .map(|pattern| pattern.chars().count())
                                            .unwrap_or(0)
                                }
                                crate::models::responses::WebSearchAction::Other => 0,
                            })
                            .unwrap_or(0),
                        ResponseItem::ImageGenerationCall {
                            revised_prompt,
                            result,
                            ..
                        } => {
                            revised_prompt
                                .as_ref()
                                .map(|text| text.chars().count())
                                .unwrap_or(0)
                                + result.chars().count()
                        }
                    })
                    .sum(),
                instructions_chars: request.instructions.chars().count(),
            },
        );
        if self.monitor.is_enabled() {
            let request_preview = preview_json_limited_with_images(&request, 128 * 1024);
            self.monitor.emit(
                response_id.clone(),
                MonitorEventKind::RequestPayload {
                    payload_preview: request_preview.text,
                    images: request_preview.images,
                },
            );
        }
        for item in trailing_tool_output_items(&request.input) {
            self.monitor.emit(
                response_id.clone(),
                MonitorEventKind::ToolPhase {
                    phase: "client_tool_result".to_string(),
                    detail: summarize_response_item(item),
                },
            );
        }
        self.send_event(
            &tx,
            created_event(&response_id),
            "failed to send response.created",
        )
        .await?;
        self.send_event(
            &tx,
            in_progress_event(&response_id),
            "failed to send response.in_progress",
        )
        .await?;

        let mut public_history = request.input.clone();
        let mut response_output = Vec::new();
        let mut event_state = ResponseEventState::default();

        let mut accumulated_usage = AccumulatedUsage::default();
        let mut upstream_request_index = 0usize;
        let mut web_search_rounds = 0usize;
        // A forced `tool_choice` (e.g. an Anthropic `web_search` server tool,
        // which Claude Code always forces) must apply only to the first
        // upstream request. After a provider-side web search runs and its
        // results are injected, the model has to be free to answer in prose.
        // Re-sending the forced tool_choice makes vLLM/Kimi emit the final
        // answer text into `function.arguments`, which then fails to parse.
        let mut current_tool_choice = request.tool_choice.clone();
        #[allow(unused_assignments)]
        let mut last_finish_reason: Option<String> = None;
        let upstream_extra_body = build_upstream_extra_body(
            self.config
                .resolve_upstream_chat_kwargs_for_resolved_model(&request.model, &upstream_model),
            &request,
            &response_format,
            &reasoning_effort,
        );
        let normalized_stop = crate::models::chat::normalize_stop(request.stop.clone())?;

        // Optional, opt-in context compaction (off by default). Best-effort:
        // on any failure `current_messages` is returned unchanged, so the
        // user's request never fails because of compaction.
        let compaction_session = crate::compaction::derive_session_id(&current_messages);
        current_messages = crate::compaction::maybe_compact(
            &self.config.compaction,
            current_messages,
            &compaction_session,
            &upstream_model,
        )
        .await;

        loop {
            if tx.is_closed() {
                return Err(AppError::cancelled());
            }
            upstream_request_index += 1;
            let taken_messages = std::mem::take(&mut current_messages);
            let upstream_request = ChatCompletionRequest {
                model: upstream_model.clone(),
                messages: taken_messages,
                stream: true,
                tools: (!tools.is_empty()).then_some(tools.clone()),
                tool_choice: Some(current_tool_choice.clone()),
                parallel_tool_calls: false,
                reasoning_effort: reasoning_effort.clone(),
                response_format: response_format.clone(),
                stream_options: Some(StreamOptions {
                    include_usage: true,
                }),
                temperature: request.temperature,
                top_p: request.top_p,
                max_output_tokens: request.max_output_tokens,
                frequency_penalty: request.frequency_penalty,
                presence_penalty: request.presence_penalty,
                stop: normalized_stop.clone(),
                extra_body: upstream_extra_body.clone(),
            };
            if self.monitor.is_enabled() {
                let upstream_debug_request =
                    sanitize_chat_request(upstream_request.clone(), self.config.flatten_content);
                let upstream_preview =
                    preview_json_limited_with_images(&upstream_debug_request, 128 * 1024);
                self.monitor.emit(
                    response_id.clone(),
                    MonitorEventKind::UpstreamRequest {
                        request_index: upstream_request_index,
                        message_count: upstream_debug_request.messages.len(),
                        prompt_chars: upstream_debug_request
                            .messages
                            .iter()
                            .map(|message| {
                                message.role.chars().count()
                                    + message
                                        .name
                                        .as_ref()
                                        .map(|name| name.chars().count())
                                        .unwrap_or(0)
                                    + message
                                        .tool_call_id
                                        .as_ref()
                                        .map(|call_id| call_id.chars().count())
                                        .unwrap_or(0)
                                    + message
                                        .reasoning_content
                                        .as_ref()
                                        .map(|text| text.chars().count())
                                        .unwrap_or(0)
                                    + message
                                        .content
                                        .as_ref()
                                        .map(|content| content.to_string().chars().count())
                                        .unwrap_or(0)
                                    + message
                                        .tool_calls
                                        .as_ref()
                                        .map(|tool_calls| {
                                            tool_calls
                                                .iter()
                                                .map(|tool_call| {
                                                    serde_json::to_string(tool_call)
                                                        .unwrap_or_default()
                                                        .chars()
                                                        .count()
                                                })
                                                .sum::<usize>()
                                        })
                                        .unwrap_or(0)
                            })
                            .sum::<usize>()
                            + upstream_debug_request
                                .tools
                                .as_ref()
                                .map(|tools| {
                                    tools
                                        .iter()
                                        .map(|tool| {
                                            serde_json::to_string(tool)
                                                .unwrap_or_default()
                                                .chars()
                                                .count()
                                        })
                                        .sum::<usize>()
                                })
                                .unwrap_or(0)
                            + upstream_debug_request
                                .extra_body
                                .values()
                                .map(|value| value.to_string().chars().count())
                                .sum::<usize>(),
                        payload_preview: upstream_preview.text,
                        images: upstream_preview.images,
                    },
                );
            }
            if tx.is_closed() {
                return Err(AppError::cancelled());
            }
            let mut stream = tokio::select! {
                biased;
                _ = tx.closed() => return Err(AppError::cancelled()),
                result = self.upstream.stream_chat_completion_with_timeout(
                    &upstream_request,
                    self.config.request_timeout,
                ) => result?,
            };
            let mut state = StreamState::default();
            let mut turn_usage: Option<ChunkUsage> = None;
            loop {
                let Some(chunk) = Self::next_upstream_chunk(&mut stream, &tx).await? else {
                    break;
                };
                if chunk.usage.is_some() {
                    turn_usage = chunk.usage.clone();
                }
                let emissions = state.apply_chunk(&chunk);
                for emission in emissions {
                    match emission {
                        StreamEmission::OutputItemAdded(item) => {
                            let target = event_state.register_item(&item);
                            self.monitor.emit(
                                response_id.clone(),
                                MonitorEventKind::ResponseItem {
                                    event: "response.output_item.added".to_string(),
                                    summary: summarize_response_item(&item),
                                    payload_preview: preview_json(&item),
                                },
                            );
                            self.send_event(
                                &tx,
                                output_item_added_event(item, target.output_index),
                                "failed to stream message start",
                            )
                            .await?;
                        }
                        StreamEmission::OutputTextDelta(delta) => {
                            let target = event_state.active_message_target()?;
                            self.monitor.emit(
                                response_id.clone(),
                                MonitorEventKind::OutputTextDelta {
                                    delta: delta.clone(),
                                },
                            );
                            self.send_event(
                                &tx,
                                output_text_delta_event(target.item_id, target.output_index, delta),
                                "failed to stream text delta",
                            )
                            .await?;
                        }
                        StreamEmission::ReasoningItemAdded(item) => {
                            let target = event_state.register_item(&item);
                            self.monitor.emit(
                                response_id.clone(),
                                MonitorEventKind::ResponseItem {
                                    event: "response.output_item.added".to_string(),
                                    summary: summarize_response_item(&item),
                                    payload_preview: preview_json(&item),
                                },
                            );
                            self.send_event(
                                &tx,
                                output_item_added_event(item, target.output_index),
                                "failed to stream reasoning start",
                            )
                            .await?;
                        }
                        StreamEmission::ReasoningTextDelta(delta) => {
                            let target = event_state.active_reasoning_target()?;
                            self.monitor.emit(
                                response_id.clone(),
                                MonitorEventKind::ReasoningTextDelta {
                                    delta: delta.clone(),
                                },
                            );
                            self.send_event(
                                &tx,
                                reasoning_text_delta_event(
                                    target.item_id,
                                    target.output_index,
                                    delta,
                                ),
                                "failed to stream reasoning delta",
                            )
                            .await?;
                        }
                        StreamEmission::ReasoningSignatureDelta(signature) => {
                            let target = event_state.active_reasoning_target()?;
                            self.send_event(
                                &tx,
                                reasoning_signature_delta_event(
                                    target.item_id,
                                    target.output_index,
                                    signature,
                                ),
                                "failed to stream reasoning signature delta",
                            )
                            .await?;
                        }
                        StreamEmission::FunctionCallArgumentsDelta {
                            call_id,
                            name,
                            delta,
                        } => {
                            self.monitor.emit(
                                response_id.clone(),
                                MonitorEventKind::FunctionCallArgumentsDelta {
                                    call_id: call_id.clone(),
                                    delta: delta.clone(),
                                },
                            );
                            self.send_event(
                                &tx,
                                function_call_args_delta_event(call_id, name, delta),
                                "failed to stream function call args delta",
                            )
                            .await?;
                        }
                        StreamEmission::ContentPartAdded => {
                            let target = event_state.active_message_target()?;
                            self.send_event(
                                &tx,
                                content_part_added_event(target.item_id, target.output_index),
                                "failed to send content_part.added",
                            )
                            .await?;
                        }
                        StreamEmission::ContentPartDone { text } => {
                            let target = event_state.active_message_target()?;
                            self.send_event(
                                &tx,
                                content_part_done_event(target.item_id, target.output_index, text),
                                "failed to send content_part.done",
                            )
                            .await?;
                        }
                        StreamEmission::ReasoningSummaryPartAdded => {
                            let target = event_state.active_reasoning_target()?;
                            self.send_event(
                                &tx,
                                reasoning_summary_part_added_event(
                                    target.item_id,
                                    target.output_index,
                                ),
                                "failed to send reasoning_summary_part.added",
                            )
                            .await?;
                        }
                        StreamEmission::ReasoningSummaryPartDone { text } => {
                            let target = event_state.active_reasoning_target()?;
                            self.send_event(
                                &tx,
                                reasoning_summary_part_done_event(
                                    target.item_id,
                                    target.output_index,
                                    text,
                                ),
                                "failed to send reasoning_summary_part.done",
                            )
                            .await?;
                        }
                        StreamEmission::RefusalDelta(delta) => {
                            self.monitor.emit(
                                response_id.clone(),
                                MonitorEventKind::RefusalDelta {
                                    delta: delta.clone(),
                                },
                            );
                            self.send_event(
                                &tx,
                                refusal_delta_event(delta),
                                "failed to send refusal.delta",
                            )
                            .await?;
                        }
                    }
                }
            }
            if let Some(usage) = turn_usage {
                accumulated_usage.add(usage);
            }
            let finalized = state.finalize(&tool_registry)?;
            last_finish_reason = finalized.finish_reason.clone();
            current_messages = upstream_request.messages;
            self.emit_completed_public_items(
                &response_id,
                &tx,
                &finalized,
                &mut public_history,
                &mut response_output,
                &mut event_state,
            )
            .await?;
            if tx.is_closed() {
                return Err(AppError::cancelled());
            }
            if let Some(message) = finalized.internal_assistant_message.clone() {
                current_messages.push(message);
            }
            if finalized.tool_calls.is_empty() {
                break;
            }
            self.handle_tool_calls(
                &response_id,
                &finalized,
                &tx,
                &mut current_messages,
                &mut public_history,
                &mut response_output,
                &mut event_state,
            )
            .await?;
            if self.config.brave_api_key.is_some()
                && finalized
                    .tool_calls
                    .iter()
                    .all(|call| matches!(call.kind, ToolKind::WebSearch))
            {
                web_search_rounds += 1;
                // `max_web_search_rounds == 0` is treated as "unlimited" by
                // configuration, but an unbounded loop lets a model that keeps
                // choosing web_search every round hang the turn forever. Always
                // enforce an absolute ceiling so the turn is guaranteed to end.
                const WEB_SEARCH_ROUNDS_HARD_CEILING: usize = 25;
                let configured_limit = if self.config.max_web_search_rounds > 0 {
                    self.config.max_web_search_rounds
                } else {
                    WEB_SEARCH_ROUNDS_HARD_CEILING
                };
                let effective_limit = configured_limit.min(WEB_SEARCH_ROUNDS_HARD_CEILING);
                if web_search_rounds >= effective_limit {
                    return Err(AppError::upstream("web search round limit exceeded"));
                }
                // Results are now in the message history; let the model answer
                // (or decide to search again) instead of forcing another
                // web_search tool call.
                current_tool_choice = Value::String("auto".to_string());
                continue;
            }
            break;
        }

        let model_name = upstream_model.clone();
        let completed_output = response_output.clone();
        let metadata = request.metadata.clone();
        if request.store {
            self.replay_store
                .insert(ReplayRecord {
                    model: model_name.clone(),
                    instructions: request.instructions,
                    visible_history: public_history,
                    internal_messages: current_messages,
                })
                .await;
        }

        let usage = accumulated_usage.into_response_usage();
        let is_incomplete = last_finish_reason.as_deref() == Some("length");
        let resource = ResponseResource {
            id: response_id.clone(),
            object: "response".to_string(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            status: if is_incomplete {
                "incomplete".to_string()
            } else {
                "completed".to_string()
            },
            output: completed_output,
            model: model_name,
            usage,
            metadata,
            incomplete_details: if is_incomplete {
                Some(crate::models::responses::IncompleteDetails {
                    reason: "max_output_tokens".to_string(),
                })
            } else {
                None
            },
        };
        if self.monitor.is_enabled() {
            let final_preview = preview_json_limited_with_images(&resource, 128 * 1024);
            self.monitor.emit(
                response_id.clone(),
                MonitorEventKind::FinalResponse {
                    status: resource.status.clone(),
                    payload_preview: final_preview.text,
                    images: final_preview.images,
                },
            );
        }
        if is_incomplete {
            self.send_event(
                &tx,
                incomplete_event(resource),
                "failed to send response.incomplete",
            )
            .await?;
        } else {
            self.send_event(
                &tx,
                completed_event(resource),
                "failed to send response.completed",
            )
            .await?;
        }
        self.monitor.emit(response_id, MonitorEventKind::Completed);
        Ok(())
    }

    async fn normalize_upstream_model(&self, model: &str) -> String {
        let catalog = match self.load_upstream_model_catalog().await {
            Ok(catalog) => catalog,
            Err(err) => {
                tracing::warn!(model, error = %err, "failed to refresh upstream model catalog");
                return model.to_string();
            }
        };
        let normalized = catalog
            .normalize(model)
            .unwrap_or_else(|| model.to_string());
        if normalized != model {
            tracing::info!(
                requested_model = %model,
                normalized_model = %normalized,
                "normalized upstream model name from backend catalog"
            );
        }
        normalized
    }

    async fn load_upstream_model_catalog(&self) -> AppResult<UpstreamModelCatalog> {
        let mut cache = self.upstream_model_catalog.lock().await;
        if let Some(cached) = cache.as_ref()
            && cached.fetched_at.elapsed().as_secs() < UPSTREAM_MODEL_CATALOG_TTL_SECS
        {
            return Ok(cached.catalog.clone());
        }
        let ids = self.upstream.supported_model_ids().await?;
        let catalog = UpstreamModelCatalog::from_ids(ids);
        *cache = Some(CachedUpstreamModelCatalog {
            fetched_at: std::time::Instant::now(),
            catalog: catalog.clone(),
        });
        Ok(catalog)
    }

    async fn next_upstream_chunk(
        stream: &mut crate::upstream::UpstreamStream,
        tx: &mpsc::Sender<SseEvent>,
    ) -> AppResult<Option<ChatCompletionChunk>> {
        tokio::select! {
            biased;
            _ = tx.closed() => Err(AppError::cancelled()),
            result = stream.next() => match result {
                Some(chunk) => chunk.map(Some),
                None => Ok(None),
            },
        }
    }

    async fn emit_completed_public_items(
        &self,
        response_id: &str,
        tx: &mpsc::Sender<SseEvent>,
        finalized: &FinalizedAssistantTurn,
        public_history: &mut Vec<ResponseItem>,
        response_output: &mut Vec<ResponseItem>,
        event_state: &mut ResponseEventState,
    ) -> AppResult<()> {
        if let Some(reasoning) = finalized.reasoning_item.clone() {
            let target = event_state.target_for_item(&reasoning);
            public_history.push(reasoning.clone());
            response_output.push(reasoning.clone());
            if finalized.reasoning_part_emitted
                && let ResponseItem::Reasoning { ref content, .. } = reasoning
            {
                let reasoning_text = content
                    .as_ref()
                    .and_then(|items| items.first())
                    .map(|item| match item {
                        crate::models::responses::ReasoningContentItem::ReasoningText { text }
                        | crate::models::responses::ReasoningContentItem::Text { text } => {
                            text.clone()
                        }
                    })
                    .unwrap_or_default();
                self.send_event(
                    tx,
                    reasoning_summary_part_done_event(
                        target.item_id.clone(),
                        target.output_index,
                        reasoning_text,
                    ),
                    "failed to send reasoning_summary_part.done",
                )
                .await?;
            }
            self.monitor.emit(
                response_id.to_string(),
                MonitorEventKind::ResponseItem {
                    event: "response.output_item.done".to_string(),
                    summary: summarize_response_item(&reasoning),
                    payload_preview: preview_json(&reasoning),
                },
            );
            self.send_event(
                tx,
                output_item_done_event(reasoning, target.output_index),
                "failed to send reasoning done",
            )
            .await?;
        }
        if let Some(message) = finalized.message_item.clone() {
            let target = event_state.target_for_item(&message);
            if let ResponseItem::Message { ref content, .. } = message {
                let full_text: String = content
                    .iter()
                    .filter_map(|c| match c {
                        crate::models::responses::ContentItem::OutputText { text } => {
                            Some(text.as_str())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("");
                if !full_text.is_empty() {
                    self.send_event(
                        tx,
                        output_text_done_event(
                            target.item_id.clone(),
                            target.output_index,
                            full_text.clone(),
                        ),
                        "failed to send output_text.done",
                    )
                    .await?;
                    if finalized.content_part_emitted {
                        self.send_event(
                            tx,
                            content_part_done_event(
                                target.item_id.clone(),
                                target.output_index,
                                full_text,
                            ),
                            "failed to send content_part.done",
                        )
                        .await?;
                    }
                }
            }
            public_history.push(message.clone());
            response_output.push(message.clone());
            self.monitor.emit(
                response_id.to_string(),
                MonitorEventKind::ResponseItem {
                    event: "response.output_item.done".to_string(),
                    summary: summarize_response_item(&message),
                    payload_preview: preview_json(&message),
                },
            );
            self.send_event(
                tx,
                output_item_done_event(message, target.output_index),
                "failed to send message done",
            )
            .await?;
        }
        if !finalized.refusal_text.is_empty() {
            self.send_event(
                tx,
                refusal_done_event(finalized.refusal_text.clone()),
                "failed to send refusal.done",
            )
            .await?;
        }
        Ok(())
    }

    async fn handle_tool_calls(
        &self,
        response_id: &str,
        finalized: &FinalizedAssistantTurn,
        tx: &mpsc::Sender<SseEvent>,
        current_messages: &mut Vec<ChatMessage>,
        public_history: &mut Vec<ResponseItem>,
        response_output: &mut Vec<ResponseItem>,
        event_state: &mut ResponseEventState,
    ) -> AppResult<()> {
        if tx.is_closed() {
            return Err(AppError::cancelled());
        }
        let can_search = self.config.brave_api_key.is_some();
        let has_web_search = can_search
            && finalized
                .tool_calls
                .iter()
                .any(|call| matches!(call.kind, ToolKind::WebSearch));
        let has_client_tool = finalized
            .tool_calls
            .iter()
            .any(|call| !matches!(call.kind, ToolKind::WebSearch) || !can_search);
        if has_web_search && has_client_tool {
            return Err(AppError::upstream(
                "mixed provider-side and client-side tool calls are not supported in v1",
            ));
        }
        if has_client_tool {
            for tool_call in &finalized.tool_calls {
                if let ResponseItem::FunctionCall {
                    ref call_id,
                    ref name,
                    ref arguments,
                    ..
                } = tool_call.public_item
                {
                    self.send_event(
                        tx,
                        function_call_args_done_event(
                            call_id.clone(),
                            name.clone(),
                            arguments.clone(),
                        ),
                        "failed to send function call args done",
                    )
                    .await?;
                }
                self.monitor.emit(
                    response_id.to_string(),
                    MonitorEventKind::ToolPhase {
                        phase: "client_tool_handoff".to_string(),
                        detail: summarize_response_item(&tool_call.public_item),
                    },
                );
                let target = event_state.target_for_item(&tool_call.public_item);
                public_history.push(tool_call.public_item.clone());
                response_output.push(tool_call.public_item.clone());
                self.monitor.emit(
                    response_id.to_string(),
                    MonitorEventKind::ResponseItem {
                        event: "response.output_item.done".to_string(),
                        summary: summarize_response_item(&tool_call.public_item),
                        payload_preview: preview_json(&tool_call.public_item),
                    },
                );
                self.send_event(
                    tx,
                    output_item_done_event(tool_call.public_item.clone(), target.output_index),
                    "failed to send tool call item",
                )
                .await?;
            }
            return Ok(());
        }
        for tool_call in &finalized.tool_calls {
            self.run_web_search(
                response_id,
                tool_call,
                tx,
                current_messages,
                public_history,
                response_output,
                event_state,
            )
            .await?;
        }
        Ok(())
    }

    async fn run_web_search(
        &self,
        response_id: &str,
        tool_call: &ResolvedToolCall,
        tx: &mpsc::Sender<SseEvent>,
        current_messages: &mut Vec<ChatMessage>,
        public_history: &mut Vec<ResponseItem>,
        response_output: &mut Vec<ResponseItem>,
        event_state: &mut ResponseEventState,
    ) -> AppResult<()> {
        let ResponseItem::WebSearchCall {
            id,
            status: _,
            action,
        } = &tool_call.public_item
        else {
            return Err(AppError::internal("expected web_search_call item"));
        };
        let partial = ResponseItem::WebSearchCall {
            id: id.clone(),
            status: Some("in_progress".to_string()),
            action: None,
        };
        self.monitor.emit(
            response_id.to_string(),
            MonitorEventKind::ToolPhase {
                phase: "provider_tool_detected".to_string(),
                detail: summarize_response_item(&tool_call.public_item),
            },
        );
        self.monitor.emit(
            response_id.to_string(),
            MonitorEventKind::ResponseItem {
                event: "response.output_item.added".to_string(),
                summary: summarize_response_item(&partial),
                payload_preview: preview_json(&partial),
            },
        );
        let partial_target = event_state.register_item(&partial);
        self.send_event(
            tx,
            output_item_added_event(partial, partial_target.output_index),
            "failed to send web_search start",
        )
        .await?;

        let query = extract_web_search_query(action, &tool_call.arguments)?;
        self.monitor.emit(
            response_id.to_string(),
            MonitorEventKind::ToolPhase {
                phase: "provider_tool_running".to_string(),
                detail: format!("web_search {query}"),
            },
        );
        if tx.is_closed() {
            return Err(AppError::cancelled());
        }
        // The search backend (Brave) has no internal timeout; without this
        // bound a slow or stalled search request would block the turn forever
        // and the client would hang behind the SSE keep-alive. Degrade
        // gracefully so the model can still produce a final answer.
        let outcome: SearchOutcome = tokio::select! {
            biased;
            _ = tx.closed() => return Err(AppError::cancelled()),
            result = timeout(self.config.request_timeout, self.search.search(&query)) => match result {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(err)) => SearchOutcome {
                    formatted: format!("web_search failed: {err}"),
                    sources: Vec::new(),
                },
                Err(_) => SearchOutcome {
                    formatted: "web_search timed out before returning results.".to_string(),
                    sources: Vec::new(),
                },
            },
        };

        let completed = ResponseItem::WebSearchCall {
            id: id.clone(),
            status: Some("completed".to_string()),
            action: action.clone(),
        };
        let completed_target = event_state.target_for_item(&completed);
        public_history.push(completed.clone());
        response_output.push(completed.clone());
        self.monitor.emit(
            response_id.to_string(),
            MonitorEventKind::ResponseItem {
                event: "response.output_item.done".to_string(),
                summary: summarize_response_item(&completed),
                payload_preview: preview_json(&completed),
            },
        );
        self.send_event(
            tx,
            output_item_done_event(completed, completed_target.output_index),
            "failed to send web_search done",
        )
        .await?;

        // Surface the search to Anthropic clients. The OpenAI `web_search_call`
        // item above carries no results (matching OpenAI's schema), so this
        // additive event hands the structured sources to the Anthropic
        // converter, which renders them as `server_tool_use` +
        // `web_search_tool_result` blocks. Non-Anthropic clients ignore the
        // unknown SSE event, keeping the Responses stream OpenAI-compatible.
        let tool_use_id = id
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("srvtoolu_{}", Uuid::new_v4().simple()));
        let result_items: Vec<Value> = outcome
            .sources
            .iter()
            .map(|source| {
                serde_json::json!({
                    "type": "web_search_result",
                    "url": source.url,
                    "title": source.title,
                })
            })
            .collect();
        self.send_event(
            tx,
            SseEvent {
                event: "response.web_search_results".to_string(),
                data: serde_json::json!({
                    "type": "response.web_search_results",
                    "tool_use_id": tool_use_id,
                    "query": query,
                    "results": result_items,
                }),
            },
            "failed to send web_search results",
        )
        .await?;
        self.monitor.emit(
            response_id.to_string(),
            MonitorEventKind::ToolPhase {
                phase: "provider_tool_completed".to_string(),
                detail: format!("web_search result {}", preview_text(&outcome.formatted)),
            },
        );

        current_messages.push(ChatMessage {
            role: "tool".to_string(),
            content: Some(Value::String(outcome.formatted.clone())),
            tool_call_id: tool_call.internal_call.id.clone(),
            name: None,
            reasoning_content: None,
            thinking: None,
            tool_calls: None,
        });
        Ok(())
    }
}

fn relax_tool_choice_after_stripping_tool(
    tool_choice: &mut Value,
    stripped_name: &str,
    no_tools_remaining: bool,
) {
    match tool_choice {
        Value::String(choice) if choice == "required" && no_tools_remaining => {
            *tool_choice = Value::String("auto".to_string());
        }
        Value::Object(map)
            if map.get("type").and_then(Value::as_str) == Some("function")
                && map
                    .get("function")
                    .and_then(Value::as_object)
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str)
                    == Some(stripped_name) =>
        {
            *tool_choice = Value::String("auto".to_string());
        }
        _ => {}
    }
}

fn preview_json<T>(value: &T) -> String
where
    T: Serialize,
{
    preview_json_limited(value, 4_000)
}

fn preview_json_limited<T>(value: &T, limit: usize) -> String
where
    T: Serialize,
{
    preview_json_limited_with_images(value, limit).text
}

#[derive(Debug)]
struct JsonPreview {
    text: String,
    images: Vec<DebugEventImage>,
}

fn preview_json_limited_with_images<T>(value: &T, limit: usize) -> JsonPreview
where
    T: Serialize,
{
    let mut images = Vec::new();
    let rendered = match serde_json::to_value(value) {
        Ok(mut value) => {
            redact_data_image_urls(&mut value, "$", &mut images);
            serde_json::to_string_pretty(&value)
        }
        Err(err) => Err(err),
    }
    .unwrap_or_else(|err| format!("{{\"serialization_error\":\"{err}\"}}"));
    if rendered.chars().count() <= limit {
        JsonPreview {
            text: rendered,
            images,
        }
    } else {
        let end = rendered
            .char_indices()
            .nth(limit)
            .map(|(index, _)| index)
            .unwrap_or(rendered.len());
        JsonPreview {
            text: format!("{}...\n[truncated]", &rendered[..end]),
            images,
        }
    }
}

fn redact_data_image_urls(value: &mut Value, path: &str, images: &mut Vec<DebugEventImage>) {
    match value {
        Value::String(text) => {
            if let Some(image) = extract_data_image(text, path, images.len() + 1) {
                *text = redacted_data_image_label(&image);
                images.push(image);
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                redact_data_image_urls(item, &format!("{path}[{index}]"), images);
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                redact_data_image_urls(item, &json_path_child(path, key), images);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn extract_data_image(value: &str, path: &str, index: usize) -> Option<DebugEventImage> {
    if !value.starts_with("data:image/") {
        return None;
    }
    let comma_index = value.find(',')?;
    let header = &value["data:".len()..comma_index];
    if !header
        .split(';')
        .any(|part| part.eq_ignore_ascii_case("base64"))
    {
        return None;
    }
    let mime_type = header
        .split(';')
        .next()
        .filter(|part| part.starts_with("image/"))?
        .to_string();
    Some(DebugEventImage {
        id: format!("image-{index}"),
        label: format!("image {index}"),
        path: path.to_string(),
        mime_type,
        size_bytes: estimate_base64_payload_bytes(&value[comma_index + 1..]),
        src: value.to_string(),
    })
}

fn redacted_data_image_label(image: &DebugEventImage) -> String {
    match image.size_bytes {
        Some(size_bytes) => format!(
            "data:{};base64,<redacted {}>",
            image.mime_type,
            format_byte_count(size_bytes)
        ),
        None => format!("data:{};base64,<redacted>", image.mime_type),
    }
}

fn estimate_base64_payload_bytes(encoded: &str) -> Option<usize> {
    let base64_len = encoded.chars().filter(|ch| !ch.is_whitespace()).count();
    if base64_len == 0 {
        return Some(0);
    }
    let padding = encoded
        .chars()
        .rev()
        .filter(|ch| !ch.is_whitespace())
        .take_while(|ch| *ch == '=')
        .count()
        .min(2);
    Some((base64_len.saturating_mul(3) / 4).saturating_sub(padding))
}

fn format_byte_count(bytes: usize) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    if bytes >= 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / MIB)
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn json_path_child(parent: &str, key: &str) -> String {
    if key
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        format!("{parent}.{key}")
    } else {
        format!(
            "{parent}[{}]",
            serde_json::to_string(key).unwrap_or_default()
        )
    }
}

fn summarize_response_item(item: &ResponseItem) -> String {
    match item {
        ResponseItem::Message { role, content, .. } => {
            format!("{role}: {}", summarize_content(content))
        }
        ResponseItem::Reasoning { content, .. } => content
            .as_ref()
            .and_then(|items| items.first())
            .map(|item| match item {
                crate::models::responses::ReasoningContentItem::ReasoningText { text }
                | crate::models::responses::ReasoningContentItem::Text { text } => {
                    format!("reasoning: {}", preview_text(text))
                }
            })
            .unwrap_or_else(|| "reasoning".to_string()),
        ResponseItem::FunctionCall {
            name, arguments, ..
        } => {
            format!("function_call {name} {}", preview_text(arguments))
        }
        ResponseItem::FunctionCallOutput { call_id, output } => {
            format!(
                "function_call_output {call_id} {}",
                preview_text(&output.to_string())
            )
        }
        ResponseItem::CustomToolCall { name, input, .. } => {
            format!("custom_tool_call {name} {}", preview_text(input))
        }
        ResponseItem::CustomToolCallOutput {
            call_id, output, ..
        } => {
            format!(
                "custom_tool_call_output {call_id} {}",
                preview_text(&output.to_string())
            )
        }
        ResponseItem::ToolSearchCall { arguments, .. } => {
            format!("tool_search_call {}", preview_text(&arguments.to_string()))
        }
        ResponseItem::ToolSearchOutput { tools, .. } => {
            format!("tool_search_output {} tools", tools.len())
        }
        ResponseItem::LocalShellCall { action, .. } => match action {
            crate::models::responses::LocalShellAction::Exec(exec) => {
                format!("local_shell {}", exec.command.join(" "))
            }
        },
        ResponseItem::WebSearchCall { action, .. } => match action {
            Some(crate::models::responses::WebSearchAction::Search { query, .. }) => {
                format!("web_search {}", query.clone().unwrap_or_default())
            }
            Some(_) => "web_search".to_string(),
            None => "web_search in_progress".to_string(),
        },
        ResponseItem::ImageGenerationCall { id, .. } => format!("image_generation_call {id}"),
    }
}

fn summarize_content(content: &[crate::models::responses::ContentItem]) -> String {
    let mut text = String::new();
    for item in content {
        match item {
            crate::models::responses::ContentItem::InputText { text: item_text }
            | crate::models::responses::ContentItem::OutputText { text: item_text } => {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str(item_text);
            }
            crate::models::responses::ContentItem::InputImage { .. } => {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str("[image]");
            }
            crate::models::responses::ContentItem::InputFile { .. } => {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str("[file]");
            }
            crate::models::responses::ContentItem::Other(_) => {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str("[input]");
            }
        }
    }
    preview_text(&text)
}

fn trailing_tool_output_items(input: &[ResponseItem]) -> Vec<&ResponseItem> {
    let mut items = input
        .iter()
        .rev()
        .take_while(|item| is_tool_output_item(item))
        .collect::<Vec<_>>();
    items.reverse();
    items
}

fn is_tool_output_item(item: &ResponseItem) -> bool {
    matches!(
        item,
        ResponseItem::FunctionCallOutput { .. }
            | ResponseItem::CustomToolCallOutput { .. }
            | ResponseItem::ToolSearchOutput { .. }
    )
}

fn preview_text(text: &str) -> String {
    const LIMIT: usize = 1024;
    if text.chars().count() <= LIMIT {
        text.to_string()
    } else {
        let end = text
            .char_indices()
            .nth(LIMIT)
            .map(|(index, _)| index)
            .unwrap_or(text.len());
        format!("{}...", &text[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::preview_json;
    use super::preview_json_limited_with_images;
    use super::preview_text;
    use super::trailing_tool_output_items;
    use crate::models::responses::ResponseItem;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn preview_text_truncates_on_char_boundary() {
        let text = format!("{}é", "a".repeat(1023));
        assert_eq!(preview_text(&text), format!("{}é", "a".repeat(1023)));

        let text = format!("{}éβ", "a".repeat(1023));
        assert_eq!(preview_text(&text), format!("{}é...", "a".repeat(1023)));
    }

    #[test]
    fn preview_json_truncates_on_char_boundary() {
        let value = json!({ "text": format!("{}éβ", "a".repeat(4_100)) });
        let preview = preview_json(&value);
        assert!(preview.ends_with("...\n[truncated]"));
        assert!(preview.is_char_boundary(preview.len()));
    }

    #[test]
    fn preview_json_redacts_data_image_urls_and_collects_images() {
        let data_url = "data:image/jpeg;base64,/9j/AA==";
        let value = json!({
            "type": "input_image",
            "image_url": data_url,
            "text": "keep me visible"
        });

        let preview = preview_json_limited_with_images(&value, 4_000);

        assert!(preview.text.contains("keep me visible"));
        assert!(preview.text.contains("data:image/jpeg;base64,<redacted"));
        assert!(!preview.text.contains("/9j/AA=="));
        assert_eq!(preview.images.len(), 1);
        assert_eq!(preview.images[0].src, data_url);
        assert_eq!(preview.images[0].mime_type, "image/jpeg");
        assert_eq!(preview.images[0].path, "$.image_url");
    }

    #[test]
    fn trailing_tool_output_items_returns_only_tail_outputs() {
        let input = vec![
            ResponseItem::FunctionCallOutput {
                call_id: "old".to_string(),
                output: json!("old"),
            },
            ResponseItem::message_text("assistant", "done"),
            ResponseItem::FunctionCallOutput {
                call_id: "fn".to_string(),
                output: json!("fn out"),
            },
            ResponseItem::CustomToolCallOutput {
                call_id: "custom".to_string(),
                name: Some("tool".to_string()),
                output: json!("custom out"),
            },
            ResponseItem::ToolSearchOutput {
                call_id: Some("search".to_string()),
                status: "completed".to_string(),
                execution: "search".to_string(),
                tools: vec![json!({ "name": "tool" })],
            },
        ];

        let result = trailing_tool_output_items(&input);
        assert_eq!(result.len(), 3);
        assert!(matches!(
            result[0],
            ResponseItem::FunctionCallOutput { call_id, .. } if call_id == "fn"
        ));
        assert!(matches!(
            result[1],
            ResponseItem::CustomToolCallOutput { call_id, .. } if call_id == "custom"
        ));
        assert!(matches!(
            result[2],
            ResponseItem::ToolSearchOutput {
                call_id: Some(call_id),
                ..
            } if call_id == "search"
        ));
    }

    use super::AccumulatedUsage;
    use super::failure_event;
    use crate::models::chat::ChunkUsage;

    #[test]
    fn accumulated_usage_cached_tokens() {
        let mut usage = AccumulatedUsage::default();
        usage.add(ChunkUsage {
            prompt_tokens: 100,
            completion_tokens: 25,
            total_tokens: 125,
            reasoning_tokens: None,
            prompt_tokens_details: Some(crate::models::chat::PromptTokensDetails {
                cached_tokens: 50,
            }),
            completion_tokens_details: None,
        });
        let result = usage.into_response_usage().unwrap();
        assert_eq!(result.input_tokens, 100);
        assert_eq!(result.input_tokens_details.unwrap().cached_tokens, 50);
    }

    #[test]
    fn accumulated_usage_reasoning_tokens() {
        let mut usage = AccumulatedUsage::default();
        usage.add(ChunkUsage {
            prompt_tokens: 100,
            completion_tokens: 25,
            total_tokens: 125,
            reasoning_tokens: None,
            prompt_tokens_details: None,
            completion_tokens_details: Some(crate::models::chat::CompletionTokensDetails {
                reasoning_tokens: 30,
            }),
        });
        let result = usage.into_response_usage().unwrap();
        assert_eq!(result.output_tokens, 25);
        assert_eq!(result.output_tokens_details.unwrap().reasoning_tokens, 30);
    }

    #[test]
    fn accumulated_usage_top_level_reasoning_tokens() {
        let mut usage = AccumulatedUsage::default();
        usage.add(ChunkUsage {
            prompt_tokens: 100,
            completion_tokens: 25,
            total_tokens: 125,
            reasoning_tokens: Some(30),
            prompt_tokens_details: None,
            completion_tokens_details: None,
        });
        let result = usage.into_response_usage().unwrap();
        assert_eq!(result.output_tokens, 25);
        assert_eq!(result.output_tokens_details.unwrap().reasoning_tokens, 30);
    }

    #[test]
    fn accumulated_usage_prefers_nested_reasoning_tokens() {
        let mut usage = AccumulatedUsage::default();
        usage.add(ChunkUsage {
            prompt_tokens: 100,
            completion_tokens: 25,
            total_tokens: 125,
            reasoning_tokens: Some(10),
            prompt_tokens_details: None,
            completion_tokens_details: Some(crate::models::chat::CompletionTokensDetails {
                reasoning_tokens: 30,
            }),
        });
        let result = usage.into_response_usage().unwrap();
        assert_eq!(result.output_tokens_details.unwrap().reasoning_tokens, 30);
    }

    #[test]
    fn accumulated_usage_zero_returns_none() {
        let usage = AccumulatedUsage::default();
        assert!(usage.into_response_usage().is_none());
    }

    use super::extract_web_search_query;
    use crate::models::responses::WebSearchAction;

    #[test]
    fn test_run_web_search_rejects_open_page() {
        let action = Some(WebSearchAction::OpenPage {
            url: Some("https://example.com".to_string()),
        });
        let args = json!({"query": "test"});
        let result = extract_web_search_query(&action, &args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported web_search action")
        );
    }

    #[test]
    fn test_run_web_search_rejects_find_in_page() {
        let action = Some(WebSearchAction::FindInPage {
            url: Some("https://example.com".to_string()),
            pattern: Some("test".to_string()),
        });
        let args = json!({"query": "test"});
        let result = extract_web_search_query(&action, &args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported web_search action")
        );
    }

    #[test]
    fn test_run_web_search_rejects_other_action() {
        let action = Some(WebSearchAction::Other);
        let args = json!({"query": "test"});
        let result = extract_web_search_query(&action, &args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported web_search action")
        );
    }

    #[test]
    fn test_extract_web_search_query_from_action() {
        let action = Some(WebSearchAction::Search {
            query: Some("rust async".to_string()),
            queries: None,
        });
        let args = json!({});
        let result = extract_web_search_query(&action, &args).unwrap();
        assert_eq!(result, "rust async");
    }

    #[test]
    fn test_extract_web_search_query_fallback_to_arguments() {
        let action = None;
        let args = json!({"query": "fallback query"});
        let result = extract_web_search_query(&action, &args).unwrap();
        assert_eq!(result, "fallback query");
    }

    #[test]
    fn test_extract_web_search_query_search_action_none_query_falls_back() {
        let action = Some(WebSearchAction::Search {
            query: None,
            queries: None,
        });
        let args = json!({"query": "from args"});
        let result = extract_web_search_query(&action, &args).unwrap();
        assert_eq!(result, "from args");
    }

    #[test]
    fn test_max_web_search_rounds_default() {
        let config =
            crate::config::Config::from_persisted(&crate::config::PersistedConfig::default())
                .unwrap();
        assert_eq!(config.max_web_search_rounds, 5);
    }

    #[test]
    fn failure_event_shape() {
        let error = crate::error::AppError::internal("test error");
        let event = failure_event(&error);
        assert_eq!(event.event, "response.failed");
        assert_eq!(event.data["type"], "response.failed");
        assert_eq!(event.data["response"]["error"]["code"], "gateway_error");
        assert_eq!(
            event.data["response"]["error"]["message"].as_str().unwrap(),
            "internal server error"
        );
    }
}

fn extract_web_search_query(
    action: &Option<WebSearchAction>,
    arguments: &Value,
) -> AppResult<String> {
    match action {
        Some(WebSearchAction::Search { query, .. }) => {
            if let Some(q) = query {
                Ok(q.clone())
            } else {
                arguments
                    .get("query")
                    .and_then(Value::as_str)
                    .map(String::from)
                    .ok_or_else(|| AppError::upstream("web_search call missing query"))
            }
        }
        Some(WebSearchAction::OpenPage { .. })
        | Some(WebSearchAction::FindInPage { .. })
        | Some(WebSearchAction::Other) => Err(AppError::upstream("unsupported web_search action")),
        None => arguments
            .get("query")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| AppError::upstream("web_search call missing query")),
    }
}

fn created_event(response_id: &str) -> SseEvent {
    json_event(
        "response.created",
        ResponsesEnvelope {
            kind: "response.created".to_string(),
            payload: ResponseCreatedPayload {
                response: ResponseStub {
                    id: response_id.to_string(),
                },
            },
        },
    )
}

fn completed_event(response: ResponseResource) -> SseEvent {
    json_event(
        "response.completed",
        ResponsesEnvelope {
            kind: "response.completed".to_string(),
            payload: ResponseCompletedPayload { response },
        },
    )
}

fn incomplete_event(response: ResponseResource) -> SseEvent {
    json_event(
        "response.incomplete",
        ResponsesEnvelope {
            kind: "response.incomplete".to_string(),
            payload: ResponseCompletedPayload { response },
        },
    )
}

fn content_part_added_event(item_id: String, output_index: usize) -> SseEvent {
    json_event(
        "response.content_part.added",
        ResponsesEnvelope {
            kind: "response.content_part.added".to_string(),
            payload: crate::models::responses::ContentPartPayload {
                item_id,
                output_index,
                content_index: 0,
                part: crate::models::responses::ContentPartRef {
                    kind: "output_text".to_string(),
                    text: String::new(),
                    annotations: Vec::new(),
                },
            },
        },
    )
}

fn content_part_done_event(item_id: String, output_index: usize, text: String) -> SseEvent {
    json_event(
        "response.content_part.done",
        ResponsesEnvelope {
            kind: "response.content_part.done".to_string(),
            payload: crate::models::responses::ContentPartPayload {
                item_id,
                output_index,
                content_index: 0,
                part: crate::models::responses::ContentPartRef {
                    kind: "output_text".to_string(),
                    text,
                    annotations: Vec::new(),
                },
            },
        },
    )
}

fn reasoning_summary_part_added_event(item_id: String, output_index: usize) -> SseEvent {
    json_event(
        "response.reasoning_summary_part.added",
        ResponsesEnvelope {
            kind: "response.reasoning_summary_part.added".to_string(),
            payload: crate::models::responses::ReasoningSummaryPartPayload {
                item_id,
                output_index,
                summary_index: 0,
                part: crate::models::responses::ReasoningSummaryPartRef {
                    kind: "summary_text".to_string(),
                    text: String::new(),
                },
            },
        },
    )
}

fn reasoning_summary_part_done_event(
    item_id: String,
    output_index: usize,
    text: String,
) -> SseEvent {
    json_event(
        "response.reasoning_summary_part.done",
        ResponsesEnvelope {
            kind: "response.reasoning_summary_part.done".to_string(),
            payload: crate::models::responses::ReasoningSummaryPartPayload {
                item_id,
                output_index,
                summary_index: 0,
                part: crate::models::responses::ReasoningSummaryPartRef {
                    kind: "summary_text".to_string(),
                    text,
                },
            },
        },
    )
}

fn refusal_delta_event(delta: String) -> SseEvent {
    json_event(
        "response.refusal.delta",
        ResponsesEnvelope {
            kind: "response.refusal.delta".to_string(),
            payload: crate::models::responses::RefusalDeltaPayload { delta },
        },
    )
}

fn refusal_done_event(refusal: String) -> SseEvent {
    json_event(
        "response.refusal.done",
        ResponsesEnvelope {
            kind: "response.refusal.done".to_string(),
            payload: crate::models::responses::RefusalDonePayload { refusal },
        },
    )
}

#[derive(Debug, Clone)]
struct OutputTarget {
    item_id: String,
    output_index: usize,
}

#[derive(Default)]
struct ResponseEventState {
    next_output_index: usize,
    output_indices: HashMap<String, usize>,
    active_message: Option<OutputTarget>,
    active_reasoning: Option<OutputTarget>,
}

impl ResponseEventState {
    fn register_item(&mut self, item: &ResponseItem) -> OutputTarget {
        let item_id = response_item_event_id(item)
            .unwrap_or_else(|| format!("item_{}", self.next_output_index));
        let output_index = match self.output_indices.get(&item_id) {
            Some(index) => *index,
            None => {
                let index = self.next_output_index;
                self.next_output_index += 1;
                self.output_indices.insert(item_id.clone(), index);
                index
            }
        };
        let target = OutputTarget {
            item_id,
            output_index,
        };
        match item {
            ResponseItem::Message { .. } => self.active_message = Some(target.clone()),
            ResponseItem::Reasoning { .. } => self.active_reasoning = Some(target.clone()),
            _ => {}
        }
        target
    }

    fn target_for_item(&mut self, item: &ResponseItem) -> OutputTarget {
        self.register_item(item)
    }

    fn active_message_target(&self) -> AppResult<OutputTarget> {
        self.active_message
            .clone()
            .ok_or_else(|| AppError::internal("missing active message output item"))
    }

    fn active_reasoning_target(&self) -> AppResult<OutputTarget> {
        self.active_reasoning
            .clone()
            .ok_or_else(|| AppError::internal("missing active reasoning output item"))
    }
}

fn response_item_event_id(item: &ResponseItem) -> Option<String> {
    match item {
        ResponseItem::Message { id, .. } => id.clone(),
        ResponseItem::Reasoning { id, .. } => Some(id.clone()),
        ResponseItem::FunctionCall { id, call_id, .. } => {
            id.clone().or_else(|| Some(call_id.clone()))
        }
        ResponseItem::FunctionCallOutput { call_id, .. } => Some(call_id.clone()),
        ResponseItem::CustomToolCall { call_id, .. } => Some(call_id.clone()),
        ResponseItem::CustomToolCallOutput { call_id, .. } => Some(call_id.clone()),
        ResponseItem::ToolSearchCall { call_id, .. } => call_id.clone(),
        ResponseItem::ToolSearchOutput { call_id, .. } => call_id.clone(),
        ResponseItem::LocalShellCall { id, call_id, .. } => id.clone().or_else(|| call_id.clone()),
        ResponseItem::WebSearchCall { id, .. } => id.clone(),
        ResponseItem::ImageGenerationCall { id, .. } => Some(id.clone()),
    }
}

#[derive(Default)]
struct AccumulatedUsage {
    input_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
    cached_input_tokens: i64,
    reasoning_output_tokens: i64,
}

impl AccumulatedUsage {
    fn add(&mut self, usage: ChunkUsage) {
        self.input_tokens += usage.prompt_tokens;
        self.output_tokens += usage.completion_tokens;
        self.total_tokens += usage.total_tokens;
        self.cached_input_tokens += usage
            .prompt_tokens_details
            .map(|d| d.cached_tokens)
            .unwrap_or(0);
        let reasoning_tokens = usage
            .completion_tokens_details
            .map(|d| d.reasoning_tokens)
            .or(usage.reasoning_tokens);
        self.reasoning_output_tokens += reasoning_tokens.unwrap_or(0);
    }

    fn into_response_usage(self) -> Option<ResponseUsage> {
        if self.total_tokens == 0 {
            return None;
        }
        Some(ResponseUsage {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            total_tokens: self.total_tokens,
            input_tokens_details: Some(ResponseInputTokensDetails {
                cached_tokens: self.cached_input_tokens,
            }),
            output_tokens_details: Some(ResponseOutputTokensDetails {
                reasoning_tokens: self.reasoning_output_tokens,
            }),
        })
    }
}

fn output_item_added_event(item: ResponseItem, output_index: usize) -> SseEvent {
    json_event(
        "response.output_item.added",
        ResponsesEnvelope {
            kind: "response.output_item.added".to_string(),
            payload: OutputItemPayload { output_index, item },
        },
    )
}

fn output_item_done_event(item: ResponseItem, output_index: usize) -> SseEvent {
    json_event(
        "response.output_item.done",
        ResponsesEnvelope {
            kind: "response.output_item.done".to_string(),
            payload: OutputItemPayload { output_index, item },
        },
    )
}

fn output_text_delta_event(item_id: String, output_index: usize, delta: String) -> SseEvent {
    json_event(
        "response.output_text.delta",
        ResponsesEnvelope {
            kind: "response.output_text.delta".to_string(),
            payload: DeltaPayload {
                item_id,
                output_index,
                content_index: 0,
                delta,
            },
        },
    )
}

fn reasoning_text_delta_event(item_id: String, output_index: usize, delta: String) -> SseEvent {
    json_event(
        "response.reasoning_summary_text.delta",
        ResponsesEnvelope {
            kind: "response.reasoning_summary_text.delta".to_string(),
            payload: ReasoningDeltaPayload {
                item_id,
                output_index,
                summary_index: 0,
                delta,
            },
        },
    )
}

fn reasoning_signature_delta_event(
    item_id: String,
    output_index: usize,
    signature: String,
) -> SseEvent {
    json_event(
        "response.reasoning_summary_text.signature_delta",
        ResponsesEnvelope {
            kind: "response.reasoning_summary_text.signature_delta".to_string(),
            payload: ReasoningSignatureDeltaPayload {
                item_id,
                output_index,
                summary_index: 0,
                signature,
            },
        },
    )
}

fn failure_event(error: &AppError) -> SseEvent {
    json_event(
        "response.failed",
        ResponsesEnvelope {
            kind: "response.failed".to_string(),
            payload: FailedPayload {
                response: FailedResponse {
                    error: FailedError {
                        code: "gateway_error".to_string(),
                        message: error.client_message.clone(),
                    },
                },
            },
        },
    )
}

fn in_progress_event(response_id: &str) -> SseEvent {
    json_event(
        "response.in_progress",
        ResponsesEnvelope {
            kind: "response.in_progress".to_string(),
            payload: ResponseCreatedPayload {
                response: ResponseStub {
                    id: response_id.to_string(),
                },
            },
        },
    )
}

fn output_text_done_event(item_id: String, output_index: usize, text: String) -> SseEvent {
    json_event(
        "response.output_text.done",
        ResponsesEnvelope {
            kind: "response.output_text.done".to_string(),
            payload: crate::models::responses::TextDonePayload {
                item_id,
                output_index,
                content_index: 0,
                text,
            },
        },
    )
}

fn function_call_args_delta_event(
    call_id: String,
    name: Option<String>,
    delta: String,
) -> SseEvent {
    json_event(
        "response.function_call_arguments.delta",
        ResponsesEnvelope {
            kind: "response.function_call_arguments.delta".to_string(),
            payload: crate::models::responses::FunctionCallArgsDeltaPayload {
                call_id,
                name,
                delta,
            },
        },
    )
}

fn function_call_args_done_event(call_id: String, name: String, arguments: String) -> SseEvent {
    json_event(
        "response.function_call_arguments.done",
        ResponsesEnvelope {
            kind: "response.function_call_arguments.done".to_string(),
            payload: crate::models::responses::FunctionCallArgsDonePayload {
                call_id,
                name,
                arguments,
            },
        },
    )
}

fn json_event<T>(event: &str, payload: T) -> SseEvent
where
    T: Serialize,
{
    SseEvent {
        event: event.to_string(),
        data: serde_json::to_value(payload).unwrap_or(Value::Null),
    }
}
