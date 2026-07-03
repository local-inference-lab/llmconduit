use crate::adapters::responses_to_chat::ToolKind;
use crate::adapters::responses_to_chat::ToolRegistry;
use crate::adapters::responses_to_chat::tool_call_arguments_object;
use crate::error::AppError;
use crate::error::AppResult;
use crate::models::chat::ChatCompletionChunk;
use crate::models::chat::ChatMessage;
use crate::models::chat::ChatThinking;
use crate::models::chat::ChatToolCall;
use crate::models::responses::ContentItem;
use crate::models::responses::LocalShellAction;
use crate::models::responses::LocalShellExecAction;
use crate::models::responses::ReasoningContentItem;
use crate::models::responses::ResponseItem;
use crate::models::responses::WebSearchAction;
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum StreamEmission {
    OutputItemAdded(ResponseItem),
    OutputTextDelta(String),
    ContentPartAdded,
    ContentPartDone {
        text: String,
    },
    ReasoningItemAdded(ResponseItem),
    ReasoningTextDelta(String),
    ReasoningSignatureDelta(String),
    ReasoningSummaryPartAdded,
    ReasoningSummaryPartDone {
        text: String,
    },
    FunctionCallArgumentsDelta {
        call_id: String,
        name: Option<String>,
        delta: String,
    },
    RefusalDelta(String),
}

#[derive(Debug, Clone)]
pub struct ResolvedToolCall {
    pub kind: ToolKind,
    pub arguments: Value,
    pub public_item: ResponseItem,
    pub internal_call: ChatToolCall,
}

#[derive(Debug, Clone)]
pub struct FinalizedAssistantTurn {
    pub message_item: Option<ResponseItem>,
    pub reasoning_item: Option<ResponseItem>,
    pub tool_calls: Vec<ResolvedToolCall>,
    pub internal_assistant_message: Option<ChatMessage>,
    pub content_part_emitted: bool,
    pub reasoning_part_emitted: bool,
    pub refusal_text: String,
    pub finish_reason: Option<String>,
    pub stop_sequence: Option<String>,
}

#[derive(Debug, Default)]
pub struct StreamState {
    message_id: Option<String>,
    reasoning_id: Option<String>,
    output_text: String,
    reasoning_text: String,
    reasoning_signature: Option<String>,
    tool_calls: BTreeMap<usize, ToolCallAccumulator>,
    content_part_emitted: bool,
    reasoning_part_emitted: bool,
    refusal_text: String,
    finish_reason: Option<String>,
    stop_sequence: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct ToolCallAccumulator {
    id: Option<String>,
    name: Option<String>,
    arguments_text: String,
}

impl ToolCallAccumulator {
    fn ensure_call_id(&mut self, upstream_id: Option<&str>) -> String {
        if self.id.is_none() {
            self.id = Some(
                upstream_id
                    .map(ToString::to_string)
                    .unwrap_or_else(|| new_item_id("call")),
            );
        }
        self.id.clone().expect("call id should be initialized")
    }
}

impl StreamState {
    pub fn apply_chunk(&mut self, chunk: &ChatCompletionChunk) -> Vec<StreamEmission> {
        let mut emissions = Vec::new();
        for choice in &chunk.choices {
            if let Some(reasoning_delta) = choice
                .delta
                .reasoning_delta()
                .filter(|delta| !delta.is_empty())
            {
                if self.reasoning_id.is_none() {
                    let item = ResponseItem::Reasoning {
                        id: new_item_id("rsn"),
                        summary: Vec::new(),
                        content: Some(Vec::new()),
                        encrypted_content: None,
                    };
                    self.reasoning_id = item_reasoning_id(&item);
                    self.reasoning_part_emitted = false;
                    emissions.push(StreamEmission::ReasoningItemAdded(item));
                }
                if !self.reasoning_part_emitted {
                    emissions.push(StreamEmission::ReasoningSummaryPartAdded);
                    self.reasoning_part_emitted = true;
                }
                self.reasoning_text.push_str(reasoning_delta);
                emissions.push(StreamEmission::ReasoningTextDelta(
                    reasoning_delta.to_string(),
                ));
            }
            if let Some(signature) = choice
                .delta
                .reasoning_signature_delta()
                .filter(|signature| !signature.is_empty())
                && self.reasoning_id.is_some()
            {
                self.reasoning_signature = Some(signature.to_string());
                emissions.push(StreamEmission::ReasoningSignatureDelta(
                    signature.to_string(),
                ));
            }
            if let Some(content_delta) = choice
                .delta
                .content
                .as_deref()
                .filter(|delta| !delta.is_empty())
            {
                if self.message_id.is_none() {
                    let item = ResponseItem::Message {
                        id: Some(new_item_id("msg")),
                        role: "assistant".to_string(),
                        content: vec![ContentItem::OutputText {
                            text: String::new(),
                        }],
                        phase: None,
                    };
                    self.message_id = item_message_id(&item);
                    self.content_part_emitted = false;
                    emissions.push(StreamEmission::OutputItemAdded(item));
                }
                if !self.content_part_emitted {
                    emissions.push(StreamEmission::ContentPartAdded);
                    self.content_part_emitted = true;
                }
                self.output_text.push_str(content_delta);
                emissions.push(StreamEmission::OutputTextDelta(content_delta.to_string()));
            }
            if let Some(refusal) = choice
                .delta
                .refusal
                .as_deref()
                .filter(|delta| !delta.is_empty())
            {
                self.refusal_text.push_str(refusal);
                emissions.push(StreamEmission::RefusalDelta(refusal.to_string()));
            }
            if let Some(reason) = &choice.finish_reason {
                self.finish_reason = Some(reason.clone());
            }
            // vLLM's `stop_reason` carries the matched stop token: a string when
            // a stop string fired, an integer token id (incl. EOS) otherwise.
            // Only a string is a real stop-sequence match we can surface.
            if let Some(Value::String(stop)) = &choice.stop_reason
                && !stop.is_empty()
            {
                self.stop_sequence = Some(stop.clone());
            }
            if let Some(tool_calls) = &choice.delta.tool_calls {
                for tool_call in tool_calls {
                    let index = tool_call.index.unwrap_or(0);
                    self.apply_tool_call_delta(
                        index,
                        tool_call.id.as_deref(),
                        tool_call.function.name.as_deref(),
                        tool_call.function.arguments.as_ref(),
                        &mut emissions,
                    );
                }
            }
            if let Some(function_call) = &choice.delta.function_call {
                self.apply_tool_call_delta(
                    0,
                    None,
                    function_call.name.as_deref(),
                    function_call.arguments.as_ref(),
                    &mut emissions,
                );
            }
        }
        emissions
    }

    fn apply_tool_call_delta(
        &mut self,
        index: usize,
        upstream_id: Option<&str>,
        name: Option<&str>,
        arguments: Option<&Value>,
        emissions: &mut Vec<StreamEmission>,
    ) {
        let entry = self.tool_calls.entry(index).or_default();
        if let Some(name) = name {
            entry.name = Some(name.to_string());
        }
        if upstream_id.is_some() && entry.id.is_none() {
            entry.ensure_call_id(upstream_id);
        }
        if let Some(arguments) = arguments {
            let call_id = entry.ensure_call_id(upstream_id);
            let before_len = entry.arguments_text.len();
            append_argument_fragment(&mut entry.arguments_text, arguments);
            let delta = entry.arguments_text[before_len..].to_string();
            if !delta.is_empty() {
                emissions.push(StreamEmission::FunctionCallArgumentsDelta {
                    call_id,
                    name: entry.name.clone(),
                    delta,
                });
            }
        }
    }

    pub fn finalize(self, registry: &ToolRegistry) -> AppResult<FinalizedAssistantTurn> {
        let message_item = if self.message_id.is_some() {
            Some(ResponseItem::Message {
                id: self.message_id,
                role: "assistant".to_string(),
                content: vec![ContentItem::OutputText {
                    text: self.output_text.clone(),
                }],
                phase: None,
            })
        } else {
            None
        };
        let reasoning_item = if self.reasoning_id.is_some() {
            Some(ResponseItem::Reasoning {
                id: self.reasoning_id.unwrap_or_else(|| new_item_id("rsn")),
                summary: Vec::new(),
                content: Some(vec![ReasoningContentItem::ReasoningText {
                    text: self.reasoning_text.clone(),
                }]),
                encrypted_content: self.reasoning_signature.clone(),
            })
        } else {
            None
        };
        let mut resolved_tool_calls = Vec::new();
        let mut internal_tool_calls = Vec::new();
        for accumulator in self.tool_calls.into_values() {
            let name = accumulator.name.ok_or_else(|| {
                AppError::upstream("upstream tool call chunk missing function name")
            })?;
            let name_lc = name.to_ascii_lowercase();
            let tool_kind = registry.get(&name_lc).cloned().ok_or_else(|| {
                AppError::upstream(format!("unknown tool returned by upstream: {name}"))
            })?;
            let arguments = if accumulator.arguments_text.trim().is_empty() {
                Value::Object(Default::default())
            } else {
                let cleaned = extract_json_arguments(&accumulator.arguments_text);
                serde_json::from_str(cleaned).map_err(|err| {
                    AppError::upstream(format!(
                        "failed to parse upstream tool arguments for {name}: {err}"
                    ))
                })?
            };
            let call_id = accumulator
                .id
                .unwrap_or_else(|| format!("call_{}", Uuid::new_v4().simple()));
            let public_item = match &tool_kind {
                ToolKind::Function {
                    public_name,
                    namespace,
                } => ResponseItem::FunctionCall {
                    id: None,
                    name: public_name.clone(),
                    namespace: namespace.clone(),
                    arguments: serde_json::to_string(&arguments).map_err(|err| {
                        AppError::internal(format!("failed to serialize function arguments: {err}"))
                    })?,
                    call_id: call_id.clone(),
                },
                ToolKind::Custom { public_name } => ResponseItem::CustomToolCall {
                    status: None,
                    call_id: call_id.clone(),
                    name: public_name.clone(),
                    input: arguments
                        .get("input")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                        .unwrap_or_else(|| arguments.to_string()),
                },
                ToolKind::LocalShell => ResponseItem::LocalShellCall {
                    id: None,
                    call_id: Some(call_id.clone()),
                    status: "completed".to_string(),
                    action: LocalShellAction::Exec(
                        serde_json::from_value::<LocalShellExecAction>(arguments.clone()).map_err(
                            |err| {
                                AppError::upstream(format!(
                                    "invalid upstream local_shell arguments: {err}"
                                ))
                            },
                        )?,
                    ),
                },
                ToolKind::ToolSearch => ResponseItem::ToolSearchCall {
                    call_id: Some(call_id.clone()),
                    status: None,
                    execution: "client".to_string(),
                    arguments: arguments.clone(),
                },
                ToolKind::WebSearch => ResponseItem::WebSearchCall {
                    id: Some(call_id.clone()),
                    status: Some("completed".to_string()),
                    action: Some(WebSearchAction::Search {
                        query: arguments
                            .get("query")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        queries: None,
                    }),
                },
                // G4: the `analyzeImage` call is a server-side tool. We carry it
                // as a FunctionCall public_item so the engine's executor can read
                // its arguments and `internal_call`, but the engine keeps it OUT
                // of `response_output`/`public_history` and suppresses its
                // streamed deltas — exactly like `web_search` is never surfaced
                // to the client.
                ToolKind::ImageAnalysis => ResponseItem::FunctionCall {
                    id: None,
                    name: crate::vision::ANALYZE_IMAGE_TOOL_NAME.to_string(),
                    namespace: None,
                    arguments: serde_json::to_string(&arguments).map_err(|err| {
                        AppError::internal(format!(
                            "failed to serialize analyzeImage arguments: {err}"
                        ))
                    })?,
                    call_id: call_id.clone(),
                },
            };
            let internal_call = ChatToolCall {
                id: Some(call_id.clone()),
                index: Some(0),
                kind: "function".to_string(),
                function: crate::models::chat::ChatFunctionCall {
                    name: Some(name),
                    arguments: Some(tool_call_arguments_object(&Some(arguments.clone()))),
                },
            };
            internal_tool_calls.push(internal_call.clone());
            resolved_tool_calls.push(ResolvedToolCall {
                kind: tool_kind,
                arguments,
                public_item,
                internal_call,
            });
        }
        let internal_assistant_message = if message_item.is_some()
            || reasoning_item.is_some()
            || !internal_tool_calls.is_empty()
        {
            Some(ChatMessage {
                role: "assistant".to_string(),
                content: message_item.as_ref().map(|item| match item {
                    ResponseItem::Message { content, .. } => Value::String(output_text(content)),
                    _ => Value::Null,
                }),
                tool_call_id: None,
                name: None,
                reasoning_content: reasoning_item.as_ref().map(|item| match item {
                    ResponseItem::Reasoning { content, .. } => content
                        .as_ref()
                        .and_then(|items| items.first())
                        .map(reasoning_content_text)
                        .unwrap_or_default(),
                    _ => String::new(),
                }),
                thinking: reasoning_item.as_ref().and_then(|item| match item {
                    ResponseItem::Reasoning {
                        content,
                        encrypted_content,
                        ..
                    } => encrypted_content.as_ref().map(|signature| ChatThinking {
                        content: content
                            .as_ref()
                            .and_then(|items| items.first())
                            .map(reasoning_content_text)
                            .unwrap_or_default(),
                        signature: Some(signature.clone()),
                    }),
                    _ => None,
                }),
                tool_calls: (!internal_tool_calls.is_empty()).then_some(internal_tool_calls),
            })
        } else {
            None
        };
        Ok(FinalizedAssistantTurn {
            message_item,
            reasoning_item,
            tool_calls: resolved_tool_calls,
            internal_assistant_message,
            content_part_emitted: self.content_part_emitted,
            reasoning_part_emitted: self.reasoning_part_emitted,
            refusal_text: self.refusal_text,
            finish_reason: self.finish_reason,
            stop_sequence: self.stop_sequence,
        })
    }
}

fn append_argument_fragment(buffer: &mut String, value: &Value) {
    match value {
        Value::String(fragment) => buffer.push_str(fragment),
        other => {
            buffer.push_str(&serde_json::to_string(other).unwrap_or_else(|_| "null".to_string()))
        }
    }
}

/// Extract the JSON payload from an upstream tool-call `arguments` string.
///
/// vLLM's Kimi/Moonshot tool-call parser can leak the model's internal
/// tool-call sentinel tokens (e.g. `<|tool_calls_section_begin|>`) into
/// `function.arguments`, particularly when `tool_choice` forces a specific
/// function — which is exactly what an Anthropic `web_search` server tool
/// produces. The leaked prefix/suffix makes an otherwise-valid object fail
/// strict JSON parsing ("expected value at line 1 column 2"). Locate the first
/// balanced JSON object/array and ignore anything around it. String-aware so
/// braces inside string literals don't confuse the scan. Falls back to the
/// trimmed input (which will then surface a real parse error) when no JSON
/// value is present.
fn extract_json_arguments(raw: &str) -> &str {
    let Some(start) = raw.find(['{', '[']) else {
        return raw.trim();
    };
    let open = raw.as_bytes()[start] as char;
    let close = if open == '{' { '}' } else { ']' };
    let mut depth = 0usize;
    let mut in_str = false;
    let mut escaped = false;
    for (idx, ch) in raw[start..].char_indices() {
        if in_str {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_str = false;
            }
            continue;
        }
        match ch {
            '"' => in_str = true,
            c if c == open => depth += 1,
            c if c == close => {
                depth -= 1;
                if depth == 0 {
                    return &raw[start..start + idx + ch.len_utf8()];
                }
            }
            _ => {}
        }
    }
    raw.trim()
}

fn new_item_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

fn item_message_id(item: &ResponseItem) -> Option<String> {
    match item {
        ResponseItem::Message { id, .. } => id.clone(),
        _ => None,
    }
}

fn item_reasoning_id(item: &ResponseItem) -> Option<String> {
    match item {
        ResponseItem::Reasoning { id, .. } => Some(id.clone()),
        _ => None,
    }
}

fn output_text(content: &[ContentItem]) -> String {
    content
        .iter()
        .filter_map(|item| match item {
            ContentItem::OutputText { text } | ContentItem::InputText { text } => {
                Some(text.clone())
            }
            ContentItem::InputImage { .. }
            | ContentItem::InputFile { .. }
            | ContentItem::Other(_) => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn reasoning_content_text(item: &ReasoningContentItem) -> String {
    match item {
        ReasoningContentItem::ReasoningText { text } | ReasoningContentItem::Text { text } => {
            text.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::responses_to_chat::{ToolKind, ToolRegistry};
    use crate::models::chat::*;
    fn content_chunk(id: &str, text: &str) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: id.to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: Some(text.to_string()),
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

    fn reasoning_chunk(id: &str, text: &str) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: id.to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: None,
                    reasoning_content: Some(text.to_string()),
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

    fn tool_call_chunk(
        id: &str,
        call_id: Option<&str>,
        index: usize,
        name: Option<&str>,
        arguments: Option<&str>,
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
                finish_reason: None,
                stop_reason: None,
            }],
            usage: None,
        }
    }

    fn legacy_function_call_chunk(
        id: &str,
        name: Option<&str>,
        arguments: Option<&str>,
    ) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: id.to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: None,
                    reasoning_content: None,
                    tool_calls: None,
                    function_call: Some(ChatFunctionCall {
                        name: name.map(str::to_string),
                        arguments: arguments.map(|s| serde_json::Value::String(s.to_string())),
                    }),
                    refusal: None,
                    extra: Default::default(),
                },
                finish_reason: None,
                stop_reason: None,
            }],
            usage: None,
        }
    }

    fn simple_registry(entries: Vec<(&str, ToolKind)>) -> ToolRegistry {
        ToolRegistry::from_map(
            entries
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    #[test]
    fn apply_chunk_content_delta() {
        let mut state = StreamState::default();
        let emissions = state.apply_chunk(&content_chunk("c1", "hello"));
        assert_eq!(emissions.len(), 3);
        assert!(matches!(&emissions[0], StreamEmission::OutputItemAdded(_)));
        assert!(matches!(&emissions[1], StreamEmission::ContentPartAdded));
        assert!(matches!(&emissions[2], StreamEmission::OutputTextDelta(d) if d == "hello"));
    }

    #[test]
    fn empty_content_delta_is_silent() {
        let mut state = StreamState::default();
        let emissions = state.apply_chunk(&content_chunk("c1", ""));
        assert!(emissions.is_empty());

        let emissions = state.apply_chunk(&content_chunk("c1", "hello"));
        assert_eq!(emissions.len(), 3);
        assert!(matches!(&emissions[0], StreamEmission::OutputItemAdded(_)));
        assert!(matches!(&emissions[1], StreamEmission::ContentPartAdded));
        assert!(matches!(&emissions[2], StreamEmission::OutputTextDelta(d) if d == "hello"));
    }

    #[test]
    fn empty_reasoning_delta_is_silent() {
        let mut state = StreamState::default();
        let emissions = state.apply_chunk(&reasoning_chunk("c1", ""));
        assert!(emissions.is_empty());

        let emissions = state.apply_chunk(&reasoning_chunk("c1", "thinking"));
        assert_eq!(emissions.len(), 3);
        assert!(matches!(
            &emissions[0],
            StreamEmission::ReasoningItemAdded(_)
        ));
        assert!(matches!(
            &emissions[1],
            StreamEmission::ReasoningSummaryPartAdded
        ));
        assert!(matches!(&emissions[2], StreamEmission::ReasoningTextDelta(d) if d == "thinking"));
    }

    #[test]
    fn apply_chunk_interleaved_reasoning_and_content() {
        let mut state = StreamState::default();
        let e1 = state.apply_chunk(&reasoning_chunk("c1", "thinking"));
        assert_eq!(e1.len(), 3);
        assert!(matches!(&e1[0], StreamEmission::ReasoningItemAdded(_)));
        assert!(matches!(&e1[1], StreamEmission::ReasoningSummaryPartAdded));
        assert!(matches!(&e1[2], StreamEmission::ReasoningTextDelta(d) if d == "thinking"));
        let e2 = state.apply_chunk(&content_chunk("c1", "answer"));
        assert_eq!(e2.len(), 3);
        assert!(matches!(&e2[0], StreamEmission::OutputItemAdded(_)));
        assert!(matches!(&e2[1], StreamEmission::ContentPartAdded));
        assert!(matches!(&e2[2], StreamEmission::OutputTextDelta(d) if d == "answer"));
    }

    #[test]
    fn apply_chunk_multi_index_tool_calls() {
        let mut state = StreamState::default();
        state.apply_chunk(&tool_call_chunk(
            "c1",
            Some("call_1"),
            0,
            Some("fn_a"),
            Some("{}"),
        ));
        state.apply_chunk(&tool_call_chunk(
            "c1",
            Some("call_2"),
            1,
            Some("fn_b"),
            Some("{}"),
        ));
        let registry = simple_registry(vec![
            (
                "fn_a",
                ToolKind::Function {
                    public_name: "fn_a".to_string(),
                    namespace: None,
                },
            ),
            (
                "fn_b",
                ToolKind::Function {
                    public_name: "fn_b".to_string(),
                    namespace: None,
                },
            ),
        ]);
        let finalized = state.finalize(&registry).unwrap();
        assert_eq!(finalized.tool_calls.len(), 2);
    }

    #[test]
    fn finalize_missing_tool_name() {
        let mut state = StreamState::default();
        state.apply_chunk(&tool_call_chunk("c1", Some("call_1"), 0, None, Some("{}")));
        let registry = simple_registry(vec![]);
        let result = state.finalize(&registry);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing function name")
        );
    }

    #[test]
    fn finalize_unknown_tool() {
        let mut state = StreamState::default();
        state.apply_chunk(&tool_call_chunk(
            "c1",
            Some("call_1"),
            0,
            Some("unknown_fn"),
            Some("{}"),
        ));
        let registry = simple_registry(vec![]);
        let result = state.finalize(&registry);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown tool"));
    }

    #[test]
    fn finalize_empty_arguments() {
        let mut state = StreamState::default();
        let emissions = state.apply_chunk(&tool_call_chunk(
            "c1",
            Some("call_1"),
            0,
            Some("echo"),
            Some(""),
        ));
        assert!(emissions.is_empty());
        let registry = simple_registry(vec![(
            "echo",
            ToolKind::Function {
                public_name: "echo".to_string(),
                namespace: None,
            },
        )]);
        let finalized = state.finalize(&registry).unwrap();
        assert_eq!(finalized.tool_calls.len(), 1);
        assert_eq!(finalized.tool_calls[0].arguments, serde_json::json!({}));
    }

    #[test]
    fn finalize_invalid_json_arguments() {
        let mut state = StreamState::default();
        state.apply_chunk(&tool_call_chunk(
            "c1",
            Some("call_1"),
            0,
            Some("echo"),
            Some("not json{"),
        ));
        let registry = simple_registry(vec![(
            "echo",
            ToolKind::Function {
                public_name: "echo".to_string(),
                namespace: None,
            },
        )]);
        let result = state.finalize(&registry);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("failed to parse"));
    }

    #[test]
    fn finalize_invalid_local_shell_arguments() {
        let mut state = StreamState::default();
        state.apply_chunk(&tool_call_chunk(
            "c1",
            Some("call_1"),
            0,
            Some("local_shell"),
            Some(r#"{"bad":"schema"}"#),
        ));
        let registry = simple_registry(vec![("local_shell", ToolKind::LocalShell)]);
        let result = state.finalize(&registry);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid upstream local_shell")
        );
    }

    #[test]
    fn finalize_custom_tool_input_extraction() {
        let mut state = StreamState::default();
        state.apply_chunk(&tool_call_chunk(
            "c1",
            Some("call_1"),
            0,
            Some("my_tool"),
            Some(r#"{"input":"code here"}"#),
        ));
        let registry = simple_registry(vec![(
            "my_tool",
            ToolKind::Custom {
                public_name: "my_tool".to_string(),
            },
        )]);
        let finalized = state.finalize(&registry).unwrap();
        assert_eq!(finalized.tool_calls.len(), 1);
        match &finalized.tool_calls[0].public_item {
            crate::models::responses::ResponseItem::CustomToolCall { input, .. } => {
                assert_eq!(input, "code here");
            }
            other => panic!("expected CustomToolCall, got {other:?}"),
        }
    }

    #[test]
    fn apply_chunk_emits_function_call_arguments_delta() {
        let mut state = StreamState::default();
        let emissions = state.apply_chunk(&tool_call_chunk(
            "c1",
            Some("call_1"),
            0,
            Some("echo"),
            Some(r#"{"val"#),
        ));
        let deltas: Vec<_> = emissions
            .iter()
            .filter_map(|e| match e {
                StreamEmission::FunctionCallArgumentsDelta { call_id, delta, .. } => {
                    Some((call_id.clone(), delta.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].0, "call_1");
        assert_eq!(deltas[0].1, r#"{"val"#);

        let emissions2 =
            state.apply_chunk(&tool_call_chunk("c1", None, 0, None, Some(r#"ue":"hi"}"#)));
        let deltas2: Vec<_> = emissions2
            .iter()
            .filter_map(|e| match e {
                StreamEmission::FunctionCallArgumentsDelta { call_id, delta, .. } => {
                    Some((call_id.clone(), delta.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(deltas2.len(), 1);
        assert_eq!(deltas2[0].1, r#"ue":"hi"}"#);
    }

    #[test]
    fn tool_call_arguments_before_id_are_emitted() {
        let mut state = StreamState::default();
        let emissions = state.apply_chunk(&tool_call_chunk(
            "c1",
            None,
            0,
            Some("echo"),
            Some(r#"{"value":"hi"}"#),
        ));
        let deltas: Vec<_> = emissions
            .iter()
            .filter_map(|e| match e {
                StreamEmission::FunctionCallArgumentsDelta { call_id, delta, .. } => {
                    Some((call_id.clone(), delta.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].0.starts_with("call_"));
        assert_eq!(deltas[0].1, r#"{"value":"hi"}"#);

        let registry = simple_registry(vec![(
            "echo",
            ToolKind::Function {
                public_name: "echo".to_string(),
                namespace: None,
            },
        )]);
        let finalized = state.finalize(&registry).unwrap();
        assert!(matches!(
            &finalized.tool_calls[0].public_item,
            ResponseItem::FunctionCall { call_id, .. } if call_id == &deltas[0].0
        ));
        assert_eq!(
            finalized.tool_calls[0].arguments,
            serde_json::json!({"value": "hi"})
        );
    }

    #[test]
    fn legacy_function_call_arguments_are_emitted() {
        let mut state = StreamState::default();
        state.apply_chunk(&legacy_function_call_chunk("c1", Some("echo"), None));
        let emissions =
            state.apply_chunk(&legacy_function_call_chunk("c1", None, Some(r#"{"val"#)));
        let deltas: Vec<_> = emissions
            .iter()
            .filter_map(|e| match e {
                StreamEmission::FunctionCallArgumentsDelta { call_id, delta, .. } => {
                    Some((call_id.clone(), delta.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].0.starts_with("call_"));
        assert_eq!(deltas[0].1, r#"{"val"#);

        let emissions2 = state.apply_chunk(&legacy_function_call_chunk(
            "c1",
            None,
            Some(r#"ue":"hi"}"#),
        ));
        let deltas2: Vec<_> = emissions2
            .iter()
            .filter_map(|e| match e {
                StreamEmission::FunctionCallArgumentsDelta { call_id, delta, .. } => {
                    Some((call_id.clone(), delta.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(deltas2.len(), 1);
        assert_eq!(deltas2[0].0, deltas[0].0);
        assert_eq!(deltas2[0].1, r#"ue":"hi"}"#);

        let registry = simple_registry(vec![(
            "echo",
            ToolKind::Function {
                public_name: "echo".to_string(),
                namespace: None,
            },
        )]);
        let finalized = state.finalize(&registry).unwrap();
        assert!(matches!(
            &finalized.tool_calls[0].public_item,
            ResponseItem::FunctionCall { call_id, .. } if call_id == &deltas[0].0
        ));
        assert_eq!(
            finalized.tool_calls[0].arguments,
            serde_json::json!({"value": "hi"})
        );
    }

    #[test]
    fn append_argument_fragment_non_string() {
        let mut buffer = String::new();
        append_argument_fragment(&mut buffer, &serde_json::json!(42));
        assert_eq!(buffer, "42");

        let mut buffer2 = String::new();
        append_argument_fragment(&mut buffer2, &serde_json::json!(true));
        assert_eq!(buffer2, "true");
    }

    #[test]
    fn test_content_part_added_before_first_delta() {
        let mut state = StreamState::default();
        let emissions = state.apply_chunk(&content_chunk("c1", "hello"));
        assert!(emissions.len() >= 3);
        assert!(matches!(&emissions[0], StreamEmission::OutputItemAdded(_)));
        assert!(matches!(&emissions[1], StreamEmission::ContentPartAdded));
        assert!(matches!(&emissions[2], StreamEmission::OutputTextDelta(d) if d == "hello"));
    }

    #[test]
    fn test_content_part_not_duplicated() {
        let mut state = StreamState::default();
        let e1 = state.apply_chunk(&content_chunk("c1", "hello"));
        let e2 = state.apply_chunk(&content_chunk("c1", " world"));
        let part_added_count = e1
            .iter()
            .chain(e2.iter())
            .filter(|e| matches!(e, StreamEmission::ContentPartAdded))
            .count();
        assert_eq!(part_added_count, 1);
    }

    #[test]
    fn test_reasoning_part_added_before_first_delta() {
        let mut state = StreamState::default();
        let emissions = state.apply_chunk(&reasoning_chunk("c1", "thinking"));
        assert!(emissions.len() >= 3);
        assert!(matches!(
            &emissions[0],
            StreamEmission::ReasoningItemAdded(_)
        ));
        assert!(matches!(
            &emissions[1],
            StreamEmission::ReasoningSummaryPartAdded
        ));
        assert!(matches!(&emissions[2], StreamEmission::ReasoningTextDelta(d) if d == "thinking"));
    }

    #[test]
    fn test_reasoning_delta_alias_emitted() {
        let mut state = StreamState::default();
        let chunk = ChatCompletionChunk {
            id: "c1".to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: None,
                    reasoning_content: None,
                    tool_calls: None,
                    function_call: None,
                    refusal: None,
                    extra: std::collections::BTreeMap::from([(
                        "reasoning".to_string(),
                        serde_json::json!("hidden step"),
                    )]),
                },
                finish_reason: None,
                stop_reason: None,
            }],
            usage: None,
        };

        let emissions = state.apply_chunk(&chunk);

        assert!(
            emissions
                .iter()
                .any(|emission| matches!(emission, StreamEmission::ReasoningTextDelta(delta) if delta == "hidden step"))
        );
    }

    #[test]
    fn nested_thinking_object_emits_reasoning_and_signature_delta() {
        let mut state = StreamState::default();
        let chunk = ChatCompletionChunk {
            id: "c1".to_string(),
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
                        serde_json::json!({
                            "content": "hidden step",
                            "signature": "sig_123"
                        }),
                    )]),
                },
                finish_reason: None,
                stop_reason: None,
            }],
            usage: None,
        };

        let emissions = state.apply_chunk(&chunk);

        assert!(
            emissions
                .iter()
                .any(|emission| matches!(emission, StreamEmission::ReasoningTextDelta(delta) if delta == "hidden step"))
        );
        assert!(
            emissions
                .iter()
                .any(|emission| matches!(emission, StreamEmission::ReasoningSignatureDelta(signature) if signature == "sig_123"))
        );
        let finalized = state.finalize(&simple_registry(vec![])).unwrap();
        assert!(matches!(
            finalized.reasoning_item,
            Some(ResponseItem::Reasoning {
                encrypted_content: Some(ref signature),
                ..
            }) if signature == "sig_123"
        ));
        let internal = finalized
            .internal_assistant_message
            .expect("internal assistant message");
        assert_eq!(internal.reasoning_content.as_deref(), Some("hidden step"));
        let thinking = internal.thinking.as_ref().expect("signed thinking");
        assert_eq!(thinking.content, "hidden step");
        assert_eq!(thinking.signature.as_deref(), Some("sig_123"));
    }

    #[test]
    fn test_refusal_delta_emitted() {
        let mut state = StreamState::default();
        let chunk = ChatCompletionChunk {
            id: "c1".to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: None,
                    reasoning_content: None,
                    tool_calls: None,
                    function_call: None,
                    refusal: Some("I cannot help".to_string()),
                    extra: Default::default(),
                },
                finish_reason: None,
                stop_reason: None,
            }],
            usage: None,
        };
        let emissions = state.apply_chunk(&chunk);
        assert_eq!(emissions.len(), 1);
        assert!(matches!(&emissions[0], StreamEmission::RefusalDelta(t) if t == "I cannot help"));
        assert_eq!(state.refusal_text, "I cannot help");
    }

    #[test]
    fn test_finish_reason_captured() {
        let mut state = StreamState::default();
        let chunk = ChatCompletionChunk {
            id: "c1".to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: Some("hi".to_string()),
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
        };
        state.apply_chunk(&chunk);
        let registry = simple_registry(vec![]);
        let finalized = state.finalize(&registry).unwrap();
        assert_eq!(finalized.finish_reason, Some("length".to_string()));
    }

    #[test]
    fn test_stop_sequence_captured_from_string_stop_reason() {
        let mut state = StreamState::default();
        let chunk = ChatCompletionChunk {
            id: "c1".to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: Some("hi".to_string()),
                    reasoning_content: None,
                    tool_calls: None,
                    function_call: None,
                    refusal: None,
                    extra: Default::default(),
                },
                finish_reason: Some("stop".to_string()),
                stop_reason: Some(serde_json::json!("</block>")),
            }],
            usage: None,
        };
        state.apply_chunk(&chunk);
        let registry = simple_registry(vec![]);
        let finalized = state.finalize(&registry).unwrap();
        assert_eq!(finalized.stop_sequence, Some("</block>".to_string()));
    }

    #[test]
    fn test_integer_stop_reason_does_not_become_stop_sequence() {
        let mut state = StreamState::default();
        let chunk = ChatCompletionChunk {
            id: "c1".to_string(),
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: Some("hi".to_string()),
                    reasoning_content: None,
                    tool_calls: None,
                    function_call: None,
                    refusal: None,
                    extra: Default::default(),
                },
                finish_reason: Some("stop".to_string()),
                stop_reason: Some(serde_json::json!(163586)),
            }],
            usage: None,
        };
        state.apply_chunk(&chunk);
        let registry = simple_registry(vec![]);
        let finalized = state.finalize(&registry).unwrap();
        assert_eq!(finalized.stop_sequence, None);
    }

    #[test]
    fn extract_json_arguments_strips_kimi_tool_call_sentinel() {
        // Exact bytes observed from vLLM Kimi-K2.6 under forced tool_choice.
        let raw = " <|tool_calls_section_begin|> {\"query\":\"current weather Boppard Germany\"}";
        let cleaned = extract_json_arguments(raw);
        let v: Value = serde_json::from_str(cleaned).expect("must parse after cleaning");
        assert_eq!(v["query"], "current weather Boppard Germany");
    }

    #[test]
    fn extract_json_arguments_handles_clean_and_padded_input() {
        assert_eq!(extract_json_arguments("{\"a\":1}"), "{\"a\":1}");
        assert_eq!(extract_json_arguments("  {\"a\":1}  ").trim(), "{\"a\":1}");
        // Trailing sentinel after the object is ignored too.
        assert_eq!(
            extract_json_arguments("{\"a\":1} <|tool_calls_section_end|>"),
            "{\"a\":1}"
        );
    }

    #[test]
    fn extract_json_arguments_is_string_aware() {
        // Braces inside string literals must not end the scan early.
        let raw = "<|x|> {\"q\":\"a{b}c \\\" }\"}";
        let cleaned = extract_json_arguments(raw);
        let v: Value = serde_json::from_str(cleaned).expect("string-aware parse");
        assert_eq!(v["q"], "a{b}c \" }");
    }

    #[test]
    fn extract_json_arguments_supports_array_payloads() {
        let raw = "<|tool_call_begin|> [{\"a\":1},{\"b\":2}] trailing";
        let cleaned = extract_json_arguments(raw);
        let v: Value = serde_json::from_str(cleaned).expect("array parse");
        assert!(v.is_array() && v.as_array().unwrap().len() == 2);
    }

    #[test]
    fn extract_json_arguments_without_json_returns_trimmed() {
        // No JSON value -> trimmed input, which still surfaces a real error
        // at the call site (we do not silently swallow genuinely-broken args).
        assert_eq!(extract_json_arguments("  not json  "), "not json");
    }

    #[test]
    fn finalize_tolerates_kimi_sentinel_in_web_search_arguments() {
        // End-to-end through finalize(): the leaked sentinel must no longer
        // produce `failed to parse upstream tool arguments`.
        let mut state = StreamState::default();
        state.apply_chunk(&tool_call_chunk(
            "c1",
            Some("call_ws"),
            0,
            Some("web_search"),
            Some(" <|tool_calls_section_begin|> {\"query\":\"boppard weather\"}"),
        ));
        let registry = simple_registry(vec![("web_search", ToolKind::WebSearch)]);
        let finalized = state
            .finalize(&registry)
            .expect("kimi sentinel must be tolerated");
        let call = &finalized.tool_calls[0];
        match &call.public_item {
            ResponseItem::WebSearchCall {
                action: Some(WebSearchAction::Search { query, .. }),
                ..
            } => assert_eq!(query.as_deref(), Some("boppard weather")),
            other => panic!("expected web_search_call, got {other:?}"),
        }
    }
}
