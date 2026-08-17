//! Non-streaming collector: accumulates the stream converter's events into
//! a single `AnthropicMessageResponse` (T8: split out of the main module).

use super::AnthropicStreamConverter;
use super::response_usage;
use crate::engine::SseEvent;
use crate::models::anthropic::AnthropicContentBlockStart;
use crate::models::anthropic::AnthropicDelta;
use crate::models::anthropic::AnthropicErrorBody;
use crate::models::anthropic::AnthropicMessageResponse;
use crate::models::anthropic::AnthropicMessageUsage;
use crate::models::anthropic::AnthropicOutputTokensDetails;
use crate::models::anthropic::AnthropicResponseContentBlock;
use crate::models::anthropic::AnthropicStreamEvent;
use serde_json::Value;
use uuid::Uuid;

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
        Self::with_reasoning_suppression(model, false)
    }

    pub fn with_reasoning_suppression(model: String, suppress_reasoning: bool) -> Self {
        Self {
            inner: AnthropicStreamConverter::with_reasoning_suppression(
                model.clone(),
                suppress_reasoning,
            ),
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
