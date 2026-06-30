use crate::engine::SseEvent;
use crate::models::anthropic::AnthropicContentBlockStart;
use crate::models::anthropic::AnthropicDelta;
use crate::models::anthropic::AnthropicErrorBody;
use crate::models::anthropic::AnthropicMessageDeltaBody;
use crate::models::anthropic::AnthropicMessageResponse;
use crate::models::anthropic::AnthropicMessageStart;
use crate::models::anthropic::AnthropicMessageUsage;
use crate::models::anthropic::AnthropicOutputTokensDetails;
use crate::models::anthropic::AnthropicResponseContentBlock;
use crate::models::anthropic::AnthropicServerToolUse;
use crate::models::anthropic::AnthropicStreamEvent;
use crate::models::anthropic::AnthropicUsage;
use serde_json::Value;
use std::collections::HashSet;
use uuid::Uuid;

const ESTIMATED_OUTPUT_TOKEN_BYTES: usize = 4;

enum ContentBlockState {
    Thinking { index: usize },
    Text { index: usize },
    ToolUse { index: usize, call_id: String },
}

pub struct AnthropicStreamConverter {
    model: String,
    message_id: String,
    next_block_index: usize,
    open_block: Option<ContentBlockState>,
    has_tool_calls: bool,
    started: bool,
    completed: bool,
    pending_input_tokens: Option<u64>,
    estimated_output_bytes: usize,
    estimated_thinking_bytes: usize,
    last_output_tokens: u64,
    last_thinking_tokens: u64,
    web_search_count: u64,
    emitted_tool_call_ids: HashSet<String>,
    closed_tool_call_ids: HashSet<String>,
}

impl AnthropicStreamConverter {
    pub fn new(model: String) -> Self {
        Self {
            model,
            message_id: format!("msg_{}", Uuid::new_v4().simple()),
            next_block_index: 0,
            open_block: None,
            has_tool_calls: false,
            started: false,
            completed: false,
            pending_input_tokens: None,
            estimated_output_bytes: 0,
            estimated_thinking_bytes: 0,
            last_output_tokens: 0,
            last_thinking_tokens: 0,
            web_search_count: 0,
            emitted_tool_call_ids: HashSet::new(),
            closed_tool_call_ids: HashSet::new(),
        }
    }

    /// `usage.server_tool_use` for terminal events: `Some` once a server-side
    /// web search has run this turn, otherwise `None` so token-only turns stay
    /// byte-identical.
    fn server_tool_use_usage(&self) -> Option<AnthropicServerToolUse> {
        (self.web_search_count > 0).then_some(AnthropicServerToolUse {
            web_search_requests: self.web_search_count,
        })
    }

    pub fn convert(&mut self, event: &SseEvent) -> Vec<AnthropicStreamEvent> {
        let mut output = Vec::new();
        match event.event.as_str() {
            "response.created" => self.handle_created(&mut output),
            "response.output_item.added" => self.handle_item_added(&event.data, &mut output),
            "response.output_text.delta" => self.handle_text_delta(&event.data, &mut output),
            "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                self.handle_reasoning_delta(&event.data, &mut output);
            }
            "response.reasoning_summary_text.signature_delta" => {
                self.handle_reasoning_signature_delta(&event.data, &mut output);
            }
            "response.function_call_arguments.delta" => {
                self.handle_function_call_arguments_delta(&event.data, &mut output);
            }
            "response.function_call_arguments.done" => {
                self.handle_function_call_arguments_done(&event.data, &mut output);
            }
            "response.output_item.done" => self.handle_item_done(&event.data, &mut output),
            "response.completed" | "response.incomplete" => {
                self.handle_completed(&event.data, &mut output)
            }
            "response.failed" => self.handle_failed(&event.data, &mut output),
            "response.web_search_results" => {
                self.handle_web_search_results(&event.data, &mut output)
            }
            _ => {}
        }
        output
    }

    fn ensure_started(&mut self, output: &mut Vec<AnthropicStreamEvent>) {
        if !self.started {
            self.started = true;
            output.push(AnthropicStreamEvent::Ping);
            output.push(AnthropicStreamEvent::MessageStart {
                message: AnthropicMessageStart {
                    id: self.message_id.clone(),
                    kind: "message".to_string(),
                    role: "assistant".to_string(),
                    content: Vec::new(),
                    model: self.model.clone(),
                    stop_reason: None,
                    stop_sequence: None,
                    usage: AnthropicUsage {
                        input_tokens: Some(self.pending_input_tokens.unwrap_or(0)),
                        output_tokens: Some(0),
                        output_tokens_details: None,
                        server_tool_use: None,
                    },
                },
            });
        }
    }

    fn handle_created(&mut self, output: &mut Vec<AnthropicStreamEvent>) {
        self.ensure_started(output);
    }

    fn handle_item_added(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.ensure_started(output);
        let Some(item) = data.get("item") else { return };
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
        match item_type {
            "message" => {
                self.close_open_block(output);
                self.start_text_block(output);
            }
            "reasoning" => {
                self.close_open_block(output);
                self.start_thinking_block(output);
            }
            _ => {}
        }
    }

    fn handle_text_delta(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.ensure_started(output);
        let Some(delta) = data.get("delta").and_then(Value::as_str) else {
            return;
        };
        match self.open_block {
            Some(ContentBlockState::Text { index }) => {
                output.push(AnthropicStreamEvent::ContentBlockDelta {
                    index,
                    delta: AnthropicDelta::TextDelta {
                        text: delta.to_string(),
                    },
                });
                self.record_output_delta(delta, false, output);
            }
            _ => {
                self.close_open_block(output);
                self.start_text_block(output);
                if let Some(ContentBlockState::Text { index }) = self.open_block {
                    output.push(AnthropicStreamEvent::ContentBlockDelta {
                        index,
                        delta: AnthropicDelta::TextDelta {
                            text: delta.to_string(),
                        },
                    });
                    self.record_output_delta(delta, false, output);
                }
            }
        }
    }

    fn handle_reasoning_delta(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.ensure_started(output);
        let Some(delta) = data.get("delta").and_then(Value::as_str) else {
            return;
        };
        match self.open_block {
            Some(ContentBlockState::Thinking { index }) => {
                output.push(AnthropicStreamEvent::ContentBlockDelta {
                    index,
                    delta: AnthropicDelta::ThinkingDelta {
                        thinking: delta.to_string(),
                    },
                });
                self.record_output_delta(delta, true, output);
            }
            _ => {
                self.close_open_block(output);
                self.start_thinking_block(output);
                if let Some(ContentBlockState::Thinking { index }) = self.open_block {
                    output.push(AnthropicStreamEvent::ContentBlockDelta {
                        index,
                        delta: AnthropicDelta::ThinkingDelta {
                            thinking: delta.to_string(),
                        },
                    });
                    self.record_output_delta(delta, true, output);
                }
            }
        }
    }

    fn handle_reasoning_signature_delta(
        &mut self,
        data: &Value,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        self.ensure_started(output);
        let Some(signature) = data.get("signature").and_then(Value::as_str) else {
            return;
        };
        if let Some(ContentBlockState::Thinking { index }) = self.open_block {
            output.push(AnthropicStreamEvent::ContentBlockDelta {
                index,
                delta: AnthropicDelta::SignatureDelta {
                    signature: signature.to_string(),
                },
            });
        }
    }

    fn handle_function_call_arguments_delta(
        &mut self,
        data: &Value,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        self.ensure_started(output);
        let Some(call_id) = data.get("call_id").and_then(Value::as_str) else {
            return;
        };
        if self.emitted_tool_call_ids.contains(call_id)
            || self.closed_tool_call_ids.contains(call_id)
        {
            return;
        }
        let Some(delta) = data.get("delta").and_then(Value::as_str) else {
            return;
        };
        if delta.is_empty() {
            return;
        }
        self.has_tool_calls = true;
        let name = data.get("name").and_then(Value::as_str).unwrap_or_default();
        self.ensure_tool_block(call_id, name, output);
        if let Some(ContentBlockState::ToolUse { index, .. }) = self.open_block {
            output.push(AnthropicStreamEvent::ContentBlockDelta {
                index,
                delta: AnthropicDelta::InputJsonDelta {
                    partial_json: delta.to_string(),
                },
            });
            self.record_output_delta(delta, false, output);
        }
    }

    fn handle_function_call_arguments_done(
        &mut self,
        data: &Value,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        self.ensure_started(output);
        let Some(call_id) = data.get("call_id").and_then(Value::as_str) else {
            return;
        };
        self.has_tool_calls = true;
        if self.emitted_tool_call_ids.contains(call_id) {
            return;
        }
        if self.closed_tool_call_ids.contains(call_id) {
            self.emitted_tool_call_ids.insert(call_id.to_string());
            return;
        }
        if matches!(
            &self.open_block,
            Some(ContentBlockState::ToolUse {
                call_id: open_call_id,
                ..
            }) if open_call_id == call_id
        ) {
            self.close_open_block(output);
            self.emitted_tool_call_ids.insert(call_id.to_string());
            return;
        }

        let name = data.get("name").and_then(Value::as_str).unwrap_or_default();
        let arguments = data
            .get("arguments")
            .and_then(Value::as_str)
            .unwrap_or("{}");
        self.ensure_tool_block(call_id, name, output);
        if !arguments.is_empty()
            && let Some(ContentBlockState::ToolUse { index, .. }) = self.open_block
        {
            output.push(AnthropicStreamEvent::ContentBlockDelta {
                index,
                delta: AnthropicDelta::InputJsonDelta {
                    partial_json: arguments.to_string(),
                },
            });
            self.record_output_delta(arguments, false, output);
        }
        self.close_open_block(output);
        self.emitted_tool_call_ids.insert(call_id.to_string());
    }

    fn handle_item_done(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        let Some(item) = data.get("item") else { return };
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
        match item_type {
            "message" => {
                if matches!(self.open_block, Some(ContentBlockState::Text { .. })) {
                    self.close_open_block(output);
                }
            }
            "reasoning" => {
                if matches!(self.open_block, Some(ContentBlockState::Thinking { .. })) {
                    self.close_open_block(output);
                }
            }
            "function_call" | "custom_tool_call" => {
                self.close_open_block(output);
                self.emit_tool_use_block(item, output);
            }
            "web_search_call" => {
                // Server-side tool: deliberately not surfaced. Brave Search runs
                // server-side, so the Anthropic client must not see a tool_use
                // block for it; the final answer (generated after results are
                // injected) is emitted as regular text. Same effect as the `_`
                // arm — kept explicit to document the intentional omission.
            }
            _ => {}
        }
    }

    /// Guarantees the Anthropic stream is terminated.
    ///
    /// The converter only emits `message_delta` + `message_stop` when it sees a
    /// `response.completed` event. If the upstream turn ends any other way (an
    /// error, a dropped/stalled engine task, an aborted web-search round-trip),
    /// the client would otherwise be left waiting forever behind the SSE
    /// keep-alive. Callers MUST invoke this once the upstream event stream is
    /// exhausted so every connection ends with a terminal event.
    pub fn finalize(&mut self) -> Vec<AnthropicStreamEvent> {
        let mut output = Vec::new();
        if self.completed {
            return output;
        }
        self.ensure_started(&mut output);
        self.close_open_block(&mut output);
        output.push(AnthropicStreamEvent::MessageDelta {
            delta: AnthropicMessageDeltaBody {
                stop_reason: Some(if self.has_tool_calls {
                    "tool_use".to_string()
                } else {
                    "end_turn".to_string()
                }),
                stop_sequence: None,
            },
            usage: AnthropicUsage {
                input_tokens: self.pending_input_tokens,
                output_tokens: Some(self.last_output_tokens),
                output_tokens_details: self.output_tokens_details(),
                server_tool_use: self.server_tool_use_usage(),
            },
        });
        output.push(AnthropicStreamEvent::MessageStop);
        self.completed = true;
        output
    }

    fn handle_completed(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.completed = true;
        self.close_open_block(output);
        let usage = response_usage(data);
        if let Some(usage) = usage.as_ref() {
            self.pending_input_tokens = Some(usage.input_tokens);
            if let Some(details) = usage.output_tokens_details.as_ref() {
                self.last_thinking_tokens = self.last_thinking_tokens.max(details.thinking_tokens);
            }
        }
        let output_tokens = usage
            .as_ref()
            .map(|usage| usage.output_tokens)
            .unwrap_or(self.last_output_tokens)
            .max(self.last_output_tokens);
        self.last_output_tokens = output_tokens;
        let (stop_reason, stop_sequence) = if let Some(reason) = response_stop_reason(data) {
            (reason, None)
        } else if self.has_tool_calls {
            // Anthropic stop_reason precedence is tool_use > stop_sequence >
            // end_turn: a turn with both tool calls and a matched stop string
            // reports tool_use, so `stop_sequence` is dropped here.
            ("tool_use".to_string(), None)
        } else if let Some(stop) = response_stop_sequence(data) {
            ("stop_sequence".to_string(), Some(Value::String(stop)))
        } else {
            ("end_turn".to_string(), None)
        };
        output.push(AnthropicStreamEvent::MessageDelta {
            delta: AnthropicMessageDeltaBody {
                stop_reason: Some(stop_reason),
                stop_sequence,
            },
            usage: AnthropicUsage {
                input_tokens: usage.as_ref().map(|usage| usage.input_tokens),
                output_tokens: Some(output_tokens),
                output_tokens_details: self.output_tokens_details(),
                server_tool_use: self.server_tool_use_usage(),
            },
        });
        output.push(AnthropicStreamEvent::MessageStop);
    }

    fn handle_failed(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        let message = data
            .get("response")
            .and_then(|r| r.get("error"))
            .and_then(|e| e.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("gateway error")
            .to_string();
        output.push(AnthropicStreamEvent::Error {
            error: AnthropicErrorBody {
                kind: "api_error".to_string(),
                message,
            },
        });
    }

    /// Surface a server-side web search to the Anthropic client.
    ///
    /// resp2chat runs Brave server-side, so the client never sees the model's
    /// own tool call. Without explicit `server_tool_use` + `web_search_tool_result`
    /// blocks, Claude Code reports "Did 0 searches" and renders no source
    /// citations. This mirrors Anthropic's native web-search streaming shape:
    /// a `server_tool_use` block (query streamed via `input_json_delta`) followed
    /// by a `web_search_tool_result` block carrying the structured sources. The
    /// turn still ends with `end_turn` (the search is resolved server-side), so
    /// `has_tool_calls` is intentionally left untouched.
    fn handle_web_search_results(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.ensure_started(output);
        self.close_open_block(output);
        self.web_search_count += 1;

        let tool_use_id = data
            .get("tool_use_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let query = data.get("query").and_then(Value::as_str).unwrap_or("");
        let results = data
            .get("results")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));

        let stu_index = self.next_block_index;
        self.next_block_index += 1;
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index: stu_index,
            content_block: AnthropicContentBlockStart::ServerToolUse {
                id: tool_use_id.clone(),
                name: "web_search".to_string(),
            },
        });
        output.push(AnthropicStreamEvent::ContentBlockDelta {
            index: stu_index,
            delta: AnthropicDelta::InputJsonDelta {
                partial_json: serde_json::json!({ "query": query }).to_string(),
            },
        });
        output.push(AnthropicStreamEvent::ContentBlockStop { index: stu_index });

        let result_index = self.next_block_index;
        self.next_block_index += 1;
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index: result_index,
            content_block: AnthropicContentBlockStart::WebSearchToolResult {
                tool_use_id,
                content: results,
            },
        });
        output.push(AnthropicStreamEvent::ContentBlockStop {
            index: result_index,
        });
    }

    fn close_open_block(&mut self, output: &mut Vec<AnthropicStreamEvent>) {
        if let Some(block) = self.open_block.take() {
            let index = match block {
                ContentBlockState::Thinking { index } | ContentBlockState::Text { index } => index,
                ContentBlockState::ToolUse { index, call_id } => {
                    self.closed_tool_call_ids.insert(call_id);
                    index
                }
            };
            output.push(AnthropicStreamEvent::ContentBlockStop { index });
        }
    }

    fn record_output_delta(
        &mut self,
        delta: &str,
        is_thinking: bool,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        if delta.is_empty() {
            return;
        }
        self.estimated_output_bytes = self.estimated_output_bytes.saturating_add(delta.len());
        let estimated_tokens = self
            .estimated_output_bytes
            .div_ceil(ESTIMATED_OUTPUT_TOKEN_BYTES) as u64;
        if is_thinking {
            self.estimated_thinking_bytes =
                self.estimated_thinking_bytes.saturating_add(delta.len());
            self.last_thinking_tokens =
                self.estimated_thinking_bytes
                    .div_ceil(ESTIMATED_OUTPUT_TOKEN_BYTES) as u64;
        }
        if estimated_tokens <= self.last_output_tokens {
            return;
        }
        self.last_output_tokens = estimated_tokens;
        output.push(AnthropicStreamEvent::MessageDelta {
            delta: AnthropicMessageDeltaBody {
                stop_reason: None,
                stop_sequence: None,
            },
            usage: AnthropicUsage {
                input_tokens: None,
                output_tokens: Some(estimated_tokens),
                // Claude Code's live spinner counts progressive
                // `usage.output_tokens`, but appears to ignore progressive
                // usage payloads once `output_tokens_details` is present.
                // Keep the detail on terminal usage; keep live deltas simple.
                output_tokens_details: None,
                server_tool_use: None,
            },
        });
    }

    fn output_tokens_details(&self) -> Option<AnthropicOutputTokensDetails> {
        (self.last_thinking_tokens > 0).then_some(AnthropicOutputTokensDetails {
            thinking_tokens: self.last_thinking_tokens,
        })
    }

    fn start_text_block(&mut self, output: &mut Vec<AnthropicStreamEvent>) {
        let index = self.next_block_index;
        self.next_block_index += 1;
        self.open_block = Some(ContentBlockState::Text { index });
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index,
            content_block: AnthropicContentBlockStart::Text {
                text: String::new(),
            },
        });
    }

    fn start_thinking_block(&mut self, output: &mut Vec<AnthropicStreamEvent>) {
        let index = self.next_block_index;
        self.next_block_index += 1;
        self.open_block = Some(ContentBlockState::Thinking { index });
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index,
            content_block: AnthropicContentBlockStart::Thinking {
                thinking: String::new(),
            },
        });
    }

    fn ensure_tool_block(
        &mut self,
        call_id: &str,
        name: &str,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        if matches!(
            &self.open_block,
            Some(ContentBlockState::ToolUse {
                call_id: open_call_id,
                ..
            }) if open_call_id == call_id
        ) {
            return;
        }
        self.close_open_block(output);
        let index = self.next_block_index;
        self.next_block_index += 1;
        self.open_block = Some(ContentBlockState::ToolUse {
            index,
            call_id: call_id.to_string(),
        });
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index,
            content_block: AnthropicContentBlockStart::ToolUse {
                id: call_id.to_string(),
                name: name.to_string(),
                input: Value::Object(Default::default()),
            },
        });
    }

    fn emit_tool_use_block(&mut self, item: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.has_tool_calls = true;
        let call_id = item
            .get("call_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if self.emitted_tool_call_ids.contains(call_id) {
            return;
        }
        if self.closed_tool_call_ids.contains(call_id) {
            self.emitted_tool_call_ids.insert(call_id.to_string());
            return;
        }
        let name = item.get("name").and_then(Value::as_str).unwrap_or_default();
        let arguments = item
            .get("arguments")
            .and_then(Value::as_str)
            .unwrap_or("{}");

        self.ensure_tool_block(call_id, name, output);
        if let Some(ContentBlockState::ToolUse { index, .. }) = self.open_block {
            output.push(AnthropicStreamEvent::ContentBlockDelta {
                index,
                delta: AnthropicDelta::InputJsonDelta {
                    partial_json: arguments.to_string(),
                },
            });
            self.record_output_delta(arguments, false, output);
        }
        self.close_open_block(output);
        self.emitted_tool_call_ids.insert(call_id.to_string());
    }
}

// ---------------------------------------------------------------------------
// Non-streaming collector: accumulates stream events into a single response
// ---------------------------------------------------------------------------

enum AccumulatedBlock {
    Thinking {
        text: String,
        signature: Option<String>,
    },
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: String,
    },
    ServerToolUse {
        id: String,
        name: String,
        input: String,
    },
    WebSearchToolResult {
        tool_use_id: String,
        content: Value,
    },
}

pub struct AnthropicStreamCollector {
    inner: AnthropicStreamConverter,
    message_id: Option<String>,
    model: Option<String>,
    stop_reason: Option<String>,
    stop_sequence: Option<Value>,
    blocks: Vec<AccumulatedBlock>,
    current_block: Option<AccumulatedBlock>,
    input_tokens: u64,
    output_tokens: u64,
    output_tokens_details: Option<AnthropicOutputTokensDetails>,
    error: Option<AnthropicErrorBody>,
}

impl AnthropicStreamCollector {
    pub fn new(model: String) -> Self {
        Self {
            inner: AnthropicStreamConverter::new(model.clone()),
            message_id: None,
            model: Some(model),
            stop_reason: None,
            stop_sequence: None,
            blocks: Vec::new(),
            current_block: None,
            input_tokens: 0,
            output_tokens: 0,
            output_tokens_details: None,
            error: None,
        }
    }

    pub fn process(&mut self, event: &SseEvent) {
        if let Some(usage) = response_usage(&event.data) {
            self.input_tokens = usage.input_tokens;
            self.output_tokens = usage.output_tokens;
            self.output_tokens_details = usage.output_tokens_details;
        }
        let stream_events = self.inner.convert(event);
        for se in stream_events {
            match se {
                AnthropicStreamEvent::MessageStart { message } => {
                    self.message_id = Some(message.id);
                    self.input_tokens = message.usage.input_tokens.unwrap_or(0);
                    self.output_tokens = message.usage.output_tokens.unwrap_or(0);
                }
                AnthropicStreamEvent::ContentBlockStart { content_block, .. } => {
                    self.current_block = match content_block {
                        AnthropicContentBlockStart::Text { .. } => Some(AccumulatedBlock::Text {
                            text: String::new(),
                        }),
                        AnthropicContentBlockStart::Thinking { .. } => {
                            Some(AccumulatedBlock::Thinking {
                                text: String::new(),
                                signature: None,
                            })
                        }
                        AnthropicContentBlockStart::ToolUse { id, name, .. } => {
                            Some(AccumulatedBlock::ToolUse {
                                id,
                                name,
                                input: String::new(),
                            })
                        }
                        AnthropicContentBlockStart::ServerToolUse { id, name } => {
                            Some(AccumulatedBlock::ServerToolUse {
                                id,
                                name,
                                input: String::new(),
                            })
                        }
                        AnthropicContentBlockStart::WebSearchToolResult {
                            tool_use_id,
                            content,
                        } => Some(AccumulatedBlock::WebSearchToolResult {
                            tool_use_id,
                            content,
                        }),
                    };
                }
                AnthropicStreamEvent::ContentBlockDelta { delta, .. } => {
                    match (&mut self.current_block, delta) {
                        (
                            Some(AccumulatedBlock::Text { text }),
                            AnthropicDelta::TextDelta { text: t },
                        ) => {
                            text.push_str(&t);
                        }
                        (
                            Some(AccumulatedBlock::Thinking { text, .. }),
                            AnthropicDelta::ThinkingDelta { thinking: t },
                        ) => {
                            text.push_str(&t);
                        }
                        (
                            Some(AccumulatedBlock::Thinking { signature, .. }),
                            AnthropicDelta::SignatureDelta { signature: sig },
                        ) => {
                            *signature = Some(sig);
                        }
                        (
                            Some(AccumulatedBlock::ToolUse { input, .. })
                            | Some(AccumulatedBlock::ServerToolUse { input, .. }),
                            AnthropicDelta::InputJsonDelta { partial_json },
                        ) => {
                            input.push_str(&partial_json);
                        }
                        _ => {}
                    }
                }
                AnthropicStreamEvent::ContentBlockStop { .. } => {
                    if let Some(block) = self.current_block.take() {
                        self.blocks.push(block);
                    }
                }
                AnthropicStreamEvent::MessageDelta { delta, usage } => {
                    if let Some(stop_reason) = delta.stop_reason {
                        self.stop_reason = Some(stop_reason);
                    }
                    if let Some(stop_sequence) = delta.stop_sequence {
                        self.stop_sequence = Some(stop_sequence);
                    }
                    if let Some(output_tokens) = usage.output_tokens {
                        self.output_tokens = output_tokens;
                    }
                    if let Some(details) = usage.output_tokens_details {
                        self.output_tokens_details = Some(details);
                    }
                }
                AnthropicStreamEvent::Error { error } => {
                    self.error = Some(error);
                }
                _ => {}
            }
        }
    }

    pub fn into_response(self) -> Result<AnthropicMessageResponse, AnthropicErrorBody> {
        if let Some(error) = self.error {
            return Err(error);
        }

        let content: Vec<AnthropicResponseContentBlock> = self
            .blocks
            .into_iter()
            .map(|block| match block {
                AccumulatedBlock::Text { text } => AnthropicResponseContentBlock::Text { text },
                AccumulatedBlock::Thinking { text, signature } => {
                    AnthropicResponseContentBlock::Thinking {
                        thinking: text,
                        signature,
                    }
                }
                AccumulatedBlock::ToolUse { id, name, input } => {
                    let parsed_input: Value =
                        serde_json::from_str(&input).unwrap_or(Value::Object(Default::default()));
                    AnthropicResponseContentBlock::ToolUse {
                        id,
                        name,
                        input: parsed_input,
                    }
                }
                AccumulatedBlock::ServerToolUse { id, name, input } => {
                    let parsed_input: Value =
                        serde_json::from_str(&input).unwrap_or(Value::Object(Default::default()));
                    AnthropicResponseContentBlock::ServerToolUse {
                        id,
                        name,
                        input: parsed_input,
                    }
                }
                AccumulatedBlock::WebSearchToolResult {
                    tool_use_id,
                    content,
                } => AnthropicResponseContentBlock::WebSearchToolResult {
                    tool_use_id,
                    content,
                },
            })
            .collect();

        Ok(AnthropicMessageResponse {
            id: self
                .message_id
                .unwrap_or_else(|| format!("msg_{}", Uuid::new_v4().simple())),
            kind: "message".to_string(),
            role: "assistant".to_string(),
            content,
            model: self.model.unwrap_or_default(),
            stop_reason: self.stop_reason.unwrap_or_else(|| "end_turn".to_string()),
            stop_sequence: self.stop_sequence,
            usage: AnthropicMessageUsage {
                input_tokens: self.input_tokens,
                output_tokens: self.output_tokens,
                output_tokens_details: self.output_tokens_details,
            },
        })
    }
}

fn response_usage(data: &Value) -> Option<AnthropicMessageUsage> {
    let usage = data.get("response")?.get("usage")?;
    Some(AnthropicMessageUsage {
        input_tokens: usage.get("input_tokens")?.as_u64()?,
        output_tokens: usage.get("output_tokens")?.as_u64()?,
        output_tokens_details: usage
            .get("output_tokens_details")
            .and_then(|details| details.get("thinking_tokens"))
            .and_then(Value::as_u64)
            .map(|thinking_tokens| AnthropicOutputTokensDetails { thinking_tokens }),
    })
}

fn response_stop_reason(data: &Value) -> Option<String> {
    if let Some(reason) = data
        .get("response")
        .and_then(|response| response.get("incomplete_details"))
        .and_then(|details| details.get("reason"))
        .and_then(Value::as_str)
    {
        return Some(match reason {
            "max_output_tokens" => "max_tokens".to_string(),
            other => other.to_string(),
        });
    }
    None
}

/// The stop string vLLM reported as matched, carried on the internal
/// `response.completed` event by the engine. Present only when a stop
/// *sequence* (not natural EOS) ended the turn; maps to Anthropic
/// `stop_reason: "stop_sequence"`.
fn response_stop_sequence(data: &Value) -> Option<String> {
    data.get("response")
        .and_then(|response| response.get("stop_sequence"))
        .and_then(Value::as_str)
        .filter(|stop| !stop.is_empty())
        .map(|stop| stop.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::anthropic::AnthropicStreamEvent;
    use serde_json::json;

    fn created_event() -> SseEvent {
        SseEvent {
            event: "response.created".to_string(),
            data: json!({
                "type": "response.created",
                "response": { "id": "resp_123" }
            }),
        }
    }

    fn item_added_event(item_type: &str, role: &str) -> SseEvent {
        SseEvent {
            event: "response.output_item.added".to_string(),
            data: json!({
                "type": "response.output_item.added",
                "item": { "type": item_type, "role": role }
            }),
        }
    }

    fn text_delta_event(text: &str) -> SseEvent {
        SseEvent {
            event: "response.output_text.delta".to_string(),
            data: json!({
                "type": "response.output_text.delta",
                "delta": text
            }),
        }
    }

    fn reasoning_delta_event(text: &str) -> SseEvent {
        SseEvent {
            event: "response.reasoning_text.delta".to_string(),
            data: json!({
                "type": "response.reasoning_text.delta",
                "delta": text,
                "content_index": 0
            }),
        }
    }

    fn reasoning_signature_delta_event(signature: &str) -> SseEvent {
        SseEvent {
            event: "response.reasoning_summary_text.signature_delta".to_string(),
            data: json!({
                "type": "response.reasoning_summary_text.signature_delta",
                "signature": signature,
                "summary_index": 0
            }),
        }
    }

    fn function_call_arguments_delta_event(call_id: &str, name: &str, delta: &str) -> SseEvent {
        SseEvent {
            event: "response.function_call_arguments.delta".to_string(),
            data: json!({
                "type": "response.function_call_arguments.delta",
                "call_id": call_id,
                "name": name,
                "delta": delta,
            }),
        }
    }

    fn function_call_arguments_done_event(call_id: &str, name: &str, arguments: &str) -> SseEvent {
        SseEvent {
            event: "response.function_call_arguments.done".to_string(),
            data: json!({
                "type": "response.function_call_arguments.done",
                "call_id": call_id,
                "name": name,
                "arguments": arguments,
            }),
        }
    }

    fn item_done_event(item_type: &str, extra: Value) -> SseEvent {
        let mut item = serde_json::json!({ "type": item_type });
        if let Value::Object(map) = extra {
            for (k, v) in map {
                item.as_object_mut().unwrap().insert(k, v);
            }
        }
        SseEvent {
            event: "response.output_item.done".to_string(),
            data: json!({
                "type": "response.output_item.done",
                "item": item
            }),
        }
    }

    fn completed_event() -> SseEvent {
        SseEvent {
            event: "response.completed".to_string(),
            data: json!({
                "type": "response.completed",
                "response": { "id": "resp_123" }
            }),
        }
    }

    fn completed_event_with_usage(input_tokens: u64, output_tokens: u64) -> SseEvent {
        SseEvent {
            event: "response.completed".to_string(),
            data: json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_123",
                    "usage": {
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                    }
                }
            }),
        }
    }

    fn completed_event_with_usage_details(
        input_tokens: u64,
        output_tokens: u64,
        thinking_tokens: u64,
    ) -> SseEvent {
        SseEvent {
            event: "response.completed".to_string(),
            data: json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_123",
                    "usage": {
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "output_tokens_details": {
                            "thinking_tokens": thinking_tokens,
                        },
                    }
                }
            }),
        }
    }

    fn completed_event_with_stop_sequence(stop: &str) -> SseEvent {
        SseEvent {
            event: "response.completed".to_string(),
            data: json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_123",
                    "stop_sequence": stop,
                }
            }),
        }
    }

    fn incomplete_event(reason: &str, input_tokens: u64, output_tokens: u64) -> SseEvent {
        SseEvent {
            event: "response.incomplete".to_string(),
            data: json!({
                "type": "response.incomplete",
                "response": {
                    "id": "resp_123",
                    "usage": {
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                    },
                    "incomplete_details": {
                        "reason": reason,
                    }
                }
            }),
        }
    }

    fn failed_event(message: &str) -> SseEvent {
        SseEvent {
            event: "response.failed".to_string(),
            data: json!({
                "type": "response.failed",
                "response": {
                    "error": { "code": "gateway_error", "message": message }
                }
            }),
        }
    }

    fn event_types(events: &[AnthropicStreamEvent]) -> Vec<&str> {
        events.iter().map(|e| e.sse_event_type()).collect()
    }

    #[test]
    fn converts_simple_text_response() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("Hello"),
            text_delta_event(" world"),
            item_done_event("message", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        assert_eq!(
            event_types(&events),
            vec![
                "ping",
                "message_start",
                "content_block_start",
                "content_block_delta",
                "message_delta",
                "content_block_delta",
                "message_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
    }

    #[test]
    fn converts_reasoning_then_text_response() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("reasoning", ""),
            reasoning_delta_event("Thinking..."),
            item_added_event("message", "assistant"),
            text_delta_event("Answer"),
            item_done_event("reasoning", json!({})),
            item_done_event("message", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        assert_eq!(
            event_types(&events),
            vec![
                "ping",
                "message_start",
                "content_block_start", // thinking
                "content_block_delta", // thinking delta
                "message_delta",       // progressive usage
                "content_block_stop",  // close thinking before text
                "content_block_start", // text
                "content_block_delta", // text delta
                "message_delta",       // progressive usage
                "content_block_stop",  // close text
                "message_delta",       // terminal stop reason
                "message_stop",
            ]
        );
    }

    #[test]
    fn converts_reasoning_signature_delta() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("reasoning", ""),
            reasoning_delta_event("Thinking..."),
            reasoning_signature_delta_event("sig_123"),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        let signatures: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                AnthropicStreamEvent::ContentBlockDelta {
                    delta: AnthropicDelta::SignatureDelta { signature },
                    ..
                } => Some(signature.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(signatures, vec!["sig_123"]);
    }

    #[test]
    fn converts_function_call_response() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("Let me check."),
            item_done_event("message", json!({})),
            item_done_event(
                "function_call",
                json!({
                    "call_id": "call_1",
                    "name": "get_weather",
                    "arguments": "{\"location\":\"Seattle\"}"
                }),
            ),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        assert_eq!(
            event_types(&events),
            vec![
                "ping",
                "message_start",
                "content_block_start", // text
                "content_block_delta", // text delta
                "message_delta",       // progressive usage
                "content_block_stop",  // close text
                "content_block_start", // tool_use
                "content_block_delta", // input_json_delta
                "message_delta",       // progressive usage
                "content_block_stop",  // close tool_use
                "message_delta",       // terminal stop reason
                "message_stop",
            ]
        );

        // Verify stop_reason is tool_use
        let message_delta = events
            .iter()
            .find(|e| matches!(e, AnthropicStreamEvent::MessageDelta { .. }));
        assert!(message_delta.is_some());
    }

    #[test]
    fn streams_function_call_argument_deltas_progressively() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            function_call_arguments_delta_event("call_1", "get_weather", r#"{"loc"#),
            function_call_arguments_delta_event("call_1", "get_weather", r#"ation":"Seattle"}"#),
            function_call_arguments_done_event(
                "call_1",
                "get_weather",
                r#"{"location":"Seattle"}"#,
            ),
            item_done_event(
                "function_call",
                json!({
                    "call_id": "call_1",
                    "name": "get_weather",
                    "arguments": "{\"location\":\"Seattle\"}"
                }),
            ),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        assert_eq!(
            event_types(&events),
            vec![
                "ping",
                "message_start",
                "content_block_start",
                "content_block_delta",
                "message_delta",
                "content_block_delta",
                "message_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
        let tool_starts = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    AnthropicStreamEvent::ContentBlockStart {
                        content_block: AnthropicContentBlockStart::ToolUse { .. },
                        ..
                    }
                )
            })
            .count();
        assert_eq!(tool_starts, 1);
        let partials: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                AnthropicStreamEvent::ContentBlockDelta {
                    delta: AnthropicDelta::InputJsonDelta { partial_json },
                    ..
                } => Some(partial_json.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(partials, vec![r#"{"loc"#, r#"ation":"Seattle"}"#]);
    }

    #[test]
    fn converts_tool_use_only_response() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_done_event(
                "function_call",
                json!({
                    "call_id": "call_1",
                    "name": "search",
                    "arguments": "{\"query\":\"test\"}"
                }),
            ),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        // Should have tool_use block with tool_use stop_reason
        let has_tool_use = events.iter().any(|e| {
            matches!(
                e,
                AnthropicStreamEvent::ContentBlockStart {
                    content_block: AnthropicContentBlockStart::ToolUse { .. },
                    ..
                }
            )
        });
        assert!(has_tool_use);
    }

    #[test]
    fn converts_failure_event() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events = converter.convert(&failed_event("upstream timeout"));

        assert_eq!(event_types(&events), vec!["error"]);
    }

    #[test]
    fn emits_usage_from_completed_response() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> =
            [created_event(), completed_event_with_usage(12, 5)]
                .iter()
                .flat_map(|e| converter.convert(e))
                .collect();

        let message_start = events
            .iter()
            .find_map(|event| match event {
                AnthropicStreamEvent::MessageStart { message } => Some(message),
                _ => None,
            })
            .expect("message_start");
        assert_eq!(message_start.usage.input_tokens, Some(0));
        assert_eq!(message_start.usage.output_tokens, Some(0));

        let message_delta = events
            .iter()
            .find_map(|event| match event {
                AnthropicStreamEvent::MessageDelta { usage, .. } => Some(usage),
                _ => None,
            })
            .expect("message_delta");
        assert_eq!(message_delta.input_tokens, Some(12));
        assert_eq!(message_delta.output_tokens, Some(5));
        assert!(message_delta.output_tokens_details.is_none());
    }

    #[test]
    fn emits_completed_response_thinking_token_details() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            completed_event_with_usage_details(12, 20, 7),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        let message_delta = events
            .iter()
            .find_map(|event| match event {
                AnthropicStreamEvent::MessageDelta { delta, usage }
                    if delta.stop_reason.is_some() =>
                {
                    Some(usage)
                }
                _ => None,
            })
            .expect("terminal message_delta");

        assert_eq!(message_delta.input_tokens, Some(12));
        assert_eq!(message_delta.output_tokens, Some(20));
        assert_eq!(
            message_delta
                .output_tokens_details
                .as_ref()
                .map(|details| details.thinking_tokens),
            Some(7)
        );
    }

    #[test]
    fn emits_progress_usage_for_reasoning_deltas() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("reasoning", ""),
            reasoning_delta_event("abcd"),
            reasoning_delta_event("efgh"),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        let message_deltas: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                AnthropicStreamEvent::MessageDelta { delta, usage } => Some((delta, usage)),
                _ => None,
            })
            .collect();
        let output_tokens: Vec<u64> = message_deltas
            .iter()
            .filter_map(|(_, usage)| usage.output_tokens)
            .collect();
        assert_eq!(output_tokens, vec![1, 2]);
        assert!(
            message_deltas
                .iter()
                .all(|(delta, _)| delta.stop_reason.is_none()),
            "progress usage must not terminate the message"
        );
        assert!(
            message_deltas
                .iter()
                .all(|(_, usage)| usage.output_tokens_details.is_none()),
            "progress usage must not include output token details"
        );
    }

    #[test]
    fn emits_terminal_thinking_token_details_for_reasoning_deltas() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("reasoning", ""),
            reasoning_delta_event("abcdefgh"),
            item_done_event("reasoning", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        let terminal_usage = events
            .iter()
            .find_map(|event| match event {
                AnthropicStreamEvent::MessageDelta { delta, usage }
                    if delta.stop_reason.as_deref() == Some("end_turn") =>
                {
                    Some(usage)
                }
                _ => None,
            })
            .expect("terminal message_delta");

        assert_eq!(terminal_usage.output_tokens, Some(2));
        assert_eq!(
            terminal_usage
                .output_tokens_details
                .as_ref()
                .map(|details| details.thinking_tokens),
            Some(2)
        );
    }

    #[test]
    fn non_streaming_collector_preserves_terminal_thinking_token_details() {
        let mut collector = AnthropicStreamCollector::new("claude-3".to_string());
        for event in [
            created_event(),
            item_added_event("reasoning", ""),
            reasoning_delta_event("abcdefgh"),
            item_done_event("reasoning", json!({})),
            completed_event(),
        ] {
            collector.process(&event);
        }

        let response = collector.into_response().expect("response");
        assert_eq!(response.usage.output_tokens, 2);
        assert_eq!(
            response
                .usage
                .output_tokens_details
                .as_ref()
                .map(|details| details.thinking_tokens),
            Some(2)
        );
    }

    #[test]
    fn completed_without_upstream_usage_preserves_progress_usage() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("abcd"),
            item_done_event("message", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        let terminal_delta = events
            .iter()
            .filter_map(|event| match event {
                AnthropicStreamEvent::MessageDelta { delta, usage } => {
                    delta.stop_reason.as_ref().map(|reason| (reason, usage))
                }
                _ => None,
            })
            .next()
            .expect("terminal message_delta");
        assert_eq!(terminal_delta.0, "end_turn");
        assert_eq!(terminal_delta.1.output_tokens, Some(1));
    }

    #[test]
    fn converts_incomplete_to_max_tokens_stop_reason() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            incomplete_event("max_output_tokens", 12, 5),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        let message_delta = events
            .iter()
            .find_map(|event| match event {
                AnthropicStreamEvent::MessageDelta { delta, .. } => Some(delta),
                _ => None,
            })
            .expect("message_delta");
        assert_eq!(message_delta.stop_reason.as_deref(), Some("max_tokens"));
    }

    #[test]
    fn converts_completed_stop_sequence_to_stop_sequence_reason() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("Hello"),
            item_done_event("message", json!({})),
            completed_event_with_stop_sequence("</block>"),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        let message_delta = events
            .iter()
            .filter_map(|event| match event {
                AnthropicStreamEvent::MessageDelta { delta, .. } if delta.stop_reason.is_some() => {
                    Some(delta)
                }
                _ => None,
            })
            .next()
            .expect("terminal message_delta");
        assert_eq!(message_delta.stop_reason.as_deref(), Some("stop_sequence"));
        assert_eq!(message_delta.stop_sequence, Some(json!("</block>")));
    }

    #[test]
    fn collector_returns_final_usage() {
        let mut collector = AnthropicStreamCollector::new("claude-3".to_string());
        for event in [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("Hello"),
            item_done_event("message", json!({})),
            completed_event_with_usage(12, 5),
        ] {
            collector.process(&event);
        }

        let response = collector.into_response().expect("response");
        assert_eq!(response.usage.input_tokens, 12);
        assert_eq!(response.usage.output_tokens, 5);
    }

    #[test]
    fn collector_surfaces_stop_sequence() {
        let mut collector = AnthropicStreamCollector::new("claude-3".to_string());
        for event in [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("Hello"),
            item_done_event("message", json!({})),
            completed_event_with_stop_sequence("</block>"),
        ] {
            collector.process(&event);
        }

        let response = collector.into_response().expect("response");
        assert_eq!(response.stop_reason, "stop_sequence");
        assert_eq!(response.stop_sequence, Some(json!("</block>")));
    }

    #[test]
    fn collector_preserves_thinking_signature() {
        let mut collector = AnthropicStreamCollector::new("claude-3".to_string());
        for event in [
            created_event(),
            item_added_event("reasoning", ""),
            reasoning_delta_event("private chain"),
            reasoning_signature_delta_event("sig_123"),
            item_done_event("reasoning", json!({})),
            completed_event(),
        ] {
            collector.process(&event);
        }

        let response = collector.into_response().expect("response");
        assert!(matches!(
            &response.content[0],
            AnthropicResponseContentBlock::Thinking {
                thinking,
                signature: Some(signature),
            } if thinking == "private chain" && signature == "sig_123"
        ));
    }

    #[test]
    fn skips_web_search_call_events() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("Searching..."),
            item_done_event("message", json!({})),
            // web_search_call events should be skipped
            SseEvent {
                event: "response.output_item.added".to_string(),
                data: json!({
                    "type": "response.output_item.added",
                    "item": { "type": "web_search_call", "id": "ws_1", "status": "in_progress" }
                }),
            },
            SseEvent {
                event: "response.output_item.done".to_string(),
                data: json!({
                    "type": "response.output_item.done",
                    "item": { "type": "web_search_call", "id": "ws_1", "status": "completed" }
                }),
            },
            // After internal web search, more text comes in a new turn
            // (simulated by another item_added + delta)
            item_added_event("message", "assistant"),
            text_delta_event("Here are the results."),
            item_done_event("message", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        // No tool_use events should appear for web_search_call
        let has_web_search_tool_use = events.iter().any(|e| {
            matches!(
                e,
                AnthropicStreamEvent::ContentBlockStart {
                    content_block: AnthropicContentBlockStart::ToolUse { name, .. },
                    ..
                } if name == "web_search"
            )
        });
        assert!(!has_web_search_tool_use);
    }

    #[test]
    fn finalize_terminates_stream_when_upstream_ends_without_completed() {
        // Regression: a web-search round-trip that stalls/aborts ends the
        // upstream event stream without `response.completed`. Without an
        // explicit finalize the client only ever sees `message_start` and
        // hangs forever behind the SSE keep-alive.
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let mut events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("Searching the web"),
            // ...stream ends here: no response.output_item.done, no completed.
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();
        events.extend(converter.finalize());

        let names = event_types(&events);
        assert_eq!(
            names.last(),
            Some(&"message_stop"),
            "stream must end with message_stop, got {names:?}"
        );
        // The dangling text content block must be closed before message_stop.
        let stop_idx = names.iter().position(|n| *n == "content_block_stop");
        let msg_stop_idx = names.iter().position(|n| *n == "message_stop");
        assert!(stop_idx < msg_stop_idx, "open block not closed: {names:?}");
    }

    #[test]
    fn finalize_terminates_stream_with_no_events_at_all() {
        // The engine can stall before producing any output. finalize() must
        // still synthesize a complete, valid message envelope.
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events = converter.finalize();
        assert_eq!(
            event_types(&events),
            vec!["ping", "message_start", "message_delta", "message_stop"]
        );
    }

    #[test]
    fn finalize_is_noop_after_normal_completion() {
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let _: Vec<_> = [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("Hello"),
            item_done_event("message", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();
        // No duplicate message_delta/message_stop after a clean completion.
        assert!(converter.finalize().is_empty());
    }

    fn web_search_results_event(tool_use_id: &str, query: &str, results: Value) -> SseEvent {
        SseEvent {
            event: "response.web_search_results".to_string(),
            data: json!({
                "type": "response.web_search_results",
                "tool_use_id": tool_use_id,
                "query": query,
                "results": results,
            }),
        }
    }

    #[test]
    fn web_search_results_emit_server_tool_use_then_result_block() {
        // Regression: resp2chat ran Brave server-side but swallowed the call,
        // so Claude Code reported "Did 0 searches" and listed no sources. The
        // converter must surface the Anthropic server-side web-search blocks
        // (`server_tool_use` + `web_search_tool_result`) so the client counts
        // the search and renders source chips.
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let results = json!([
            {"type": "web_search_result", "url": "https://example.com/a", "title": "Site A"},
            {"type": "web_search_result", "url": "https://example.com/b", "title": "Site B"}
        ]);
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("Let me search."),
            web_search_results_event("srvtoolu_1", "current weather Boppard", results.clone()),
            text_delta_event("It is 11C."),
            item_done_event("message", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        let jsons: Vec<Value> = events
            .iter()
            .map(|e| serde_json::from_str(&e.to_json()).unwrap())
            .collect();

        // server_tool_use block, query streamed via input_json_delta, then stop.
        let stu_pos = jsons
            .iter()
            .position(|j| j["content_block"]["type"] == "server_tool_use")
            .expect("server_tool_use content_block_start");
        assert_eq!(jsons[stu_pos]["type"], "content_block_start");
        assert_eq!(jsons[stu_pos]["content_block"]["name"], "web_search");
        let stu_id = jsons[stu_pos]["content_block"]["id"].as_str().unwrap();
        assert!(!stu_id.is_empty(), "server_tool_use must carry an id");
        let stu_idx = jsons[stu_pos]["index"].clone();

        assert_eq!(jsons[stu_pos + 1]["type"], "content_block_delta");
        assert_eq!(jsons[stu_pos + 1]["delta"]["type"], "input_json_delta");
        let partial = jsons[stu_pos + 1]["delta"]["partial_json"]
            .as_str()
            .unwrap();
        let parsed: Value = serde_json::from_str(partial).expect("partial_json is JSON");
        assert_eq!(parsed["query"], "current weather Boppard");
        assert_eq!(jsons[stu_pos + 2]["type"], "content_block_stop");
        assert_eq!(jsons[stu_pos + 2]["index"], stu_idx);

        // web_search_tool_result block carrying the structured sources.
        let res_pos = stu_pos + 3;
        assert_eq!(jsons[res_pos]["type"], "content_block_start");
        assert_eq!(
            jsons[res_pos]["content_block"]["type"],
            "web_search_tool_result"
        );
        assert_eq!(jsons[res_pos]["content_block"]["tool_use_id"], stu_id);
        assert_eq!(jsons[res_pos]["content_block"]["content"], results);
        assert_eq!(jsons[res_pos + 1]["type"], "content_block_stop");

        // Server-side tool: turn still ends with end_turn, not tool_use.
        let msg_delta = jsons
            .iter()
            .find(|j| j["type"] == "message_delta" && j["delta"]["stop_reason"].is_string())
            .expect("message_delta");
        assert_eq!(msg_delta["delta"]["stop_reason"], "end_turn");
    }

    #[test]
    fn web_search_count_is_reported_in_terminal_usage() {
        // Claude Code's "Did N searches" reads usage.server_tool_use
        // .web_search_requests. resp2chat must report it so a server-side
        // search isn't shown as "Did 0 searches".
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let r = json!([{"type": "web_search_result", "url": "https://x", "title": "X"}]);
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("message", "assistant"),
            web_search_results_event("srvtoolu_1", "q1", r.clone()),
            web_search_results_event("srvtoolu_2", "q2", r.clone()),
            text_delta_event("answer"),
            item_done_event("message", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();

        let md = events
            .iter()
            .filter_map(|e| match e {
                AnthropicStreamEvent::MessageDelta { .. } => {
                    Some(serde_json::from_str::<Value>(&e.to_json()).unwrap())
                }
                _ => None,
            })
            .find(|j| j["usage"].get("server_tool_use").is_some())
            .expect("message_delta");
        assert_eq!(md["usage"]["server_tool_use"]["web_search_requests"], 2);
    }

    #[test]
    fn no_server_tool_use_usage_when_no_web_search() {
        // Pristine: a turn without web search must not emit server_tool_use usage.
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let events: Vec<AnthropicStreamEvent> = [
            created_event(),
            item_added_event("message", "assistant"),
            text_delta_event("hi"),
            item_done_event("message", json!({})),
            completed_event(),
        ]
        .iter()
        .flat_map(|e| converter.convert(e))
        .collect();
        let md = events
            .iter()
            .find(|e| matches!(e, AnthropicStreamEvent::MessageDelta { .. }))
            .map(|e| serde_json::from_str::<Value>(&e.to_json()).unwrap())
            .expect("message_delta");
        assert!(
            md["usage"].get("server_tool_use").is_none(),
            "no web search => no server_tool_use usage, got {}",
            md["usage"]
        );
    }

    #[test]
    fn finalize_terminates_stream_after_failure_event() {
        // handle_failed only emits `error`; the client still needs a terminal
        // message_stop to stop waiting.
        let mut converter = AnthropicStreamConverter::new("claude-3".to_string());
        let mut events = converter.convert(&created_event());
        events.extend(converter.convert(&failed_event("web search round limit exceeded")));
        events.extend(converter.finalize());

        let names = event_types(&events);
        assert!(names.contains(&"error"), "expected error event: {names:?}");
        assert_eq!(
            names.last(),
            Some(&"message_stop"),
            "stream must still end with message_stop, got {names:?}"
        );
    }
}
