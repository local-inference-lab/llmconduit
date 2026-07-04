use crate::error::AppError;
use crate::error::AppResult;
use crate::models::anthropic::AnthropicContent;
use crate::models::anthropic::AnthropicContentBlock;
use crate::models::anthropic::AnthropicImageSource;
use crate::models::anthropic::AnthropicRequest;
use crate::models::anthropic::AnthropicSystemContent;
use crate::models::anthropic::AnthropicThinking;
use crate::models::anthropic::AnthropicTool;
use crate::models::anthropic::SYNTHETIC_SIGNATURE_PREFIX;
use crate::models::responses::ContentItem;
use crate::models::responses::ReasoningContentItem;
use crate::models::responses::ReasoningRequest;
use crate::models::responses::ResponseItem;
use crate::models::responses::ResponsesRequest;
use crate::models::responses::TextControls;
use crate::models::responses::TextFormat;
use crate::models::responses::ToolSpec;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use uuid::Uuid;

pub fn convert_request(request: AnthropicRequest) -> AppResult<ResponsesRequest> {
    if request.top_k.is_some() {
        return Err(AppError::bad_request(
            "Anthropic top_k is not supported by this gateway",
        ));
    }
    let instructions = extract_system_text(&request.system);
    let input = convert_messages(&request.messages)?;
    let tools = convert_tools(&request.tools);
    let thinking_on = matches!(
        request.thinking.as_ref(),
        Some(AnthropicThinking::Enabled { .. } | AnthropicThinking::Adaptive { .. })
    );
    let (reasoning, extra_body) = convert_thinking(&request.thinking);
    // Route Anthropic stop_sequences through the same normalize_stop gate as the
    // Chat path so OPENAI_MAX_STOP_SEQUENCES=4 → 400 (and empty-drop) apply here too.
    // Assign to the typed field instead of smuggling raw into extra_body["stop"].
    let stop = crate::models::chat::normalize_stop(request.stop_sequences)?;
    let tool_choice = convert_tool_choice(request.tool_choice);
    let metadata = convert_metadata(request.metadata)?;
    let reasoning =
        apply_output_config_effort(reasoning, &request.thinking, &request.output_config);
    let text = convert_output_config(request.output_config)?;
    let max_output_tokens = request.max_tokens.map(|value| {
        i64::try_from(value)
            .map_err(|_| AppError::bad_request("Anthropic max_tokens exceeds supported range"))
    });

    Ok(ResponsesRequest {
        model: request.model,
        instructions,
        input,
        tools,
        tool_choice,
        parallel_tool_calls: None,
        reasoning,
        thinking: Some(thinking_on),
        store: false,
        stream: true,
        include: Vec::new(),
        service_tier: None,
        prompt_cache_key: None,
        text,
        client_metadata: None,
        previous_response_id: None,
        temperature: request.temperature,
        top_p: request.top_p,
        max_output_tokens: max_output_tokens.transpose()?,
        frequency_penalty: None,
        presence_penalty: None,
        truncation: None,
        metadata,
        stop,
        extra_body,
    })
}

fn convert_output_config(output_config: Option<Value>) -> AppResult<Option<TextControls>> {
    let Some(output_config) = output_config else {
        return Ok(None);
    };
    let Value::Object(config) = output_config else {
        return Err(AppError::bad_request(
            "Anthropic output_config must be a JSON object",
        ));
    };
    let Some(format) = config.get("format") else {
        return Ok(None);
    };
    if format.is_null() {
        return Ok(None);
    }
    let Some(format) = format.as_object() else {
        return Err(AppError::bad_request(
            "Anthropic output_config.format must be a JSON object",
        ));
    };
    let kind = format
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::bad_request("Anthropic output_config.format is missing type"))?;
    if kind != "json_schema" {
        return Err(AppError::bad_request(format!(
            "Anthropic output_config.format type \"{kind}\" is not supported by this gateway"
        )));
    }
    let schema = format.get("schema").cloned().ok_or_else(|| {
        AppError::bad_request("Anthropic output_config.format json_schema is missing schema")
    })?;
    let name = format
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("response")
        .to_string();
    let strict = format
        .get("strict")
        .and_then(Value::as_bool)
        .unwrap_or(true);

    Ok(Some(TextControls {
        verbosity: None,
        format: Some(TextFormat {
            kind: "json_schema".to_string(),
            strict,
            schema,
            name,
        }),
    }))
}

/// Adaptive-thinking effort override.
///
/// Claude Code's `/effort` command rides on `output_config.effort`. When the
/// caller leaves thinking *adaptive* (no fixed `budget_tokens`), this is the
/// only signal for how hard to reason, so it sets the canonical
/// `reasoning.effort`. When thinking is `enabled` the budget already pins the
/// effort (see `convert_thinking`), so the field is ignored to avoid fighting
/// the explicit budget; it is likewise ignored when reasoning is off. The raw
/// effort string is passed through unvalidated — lowering
/// (`responses_to_chat::normalize_reasoning_effort`) only normalizes
/// case/whitespace, and the upstream LEAF (`upstream::finalize_request_for_backend`)
/// then maps it per-model (`reasoning_effort_map`) or clamps unsupported values
/// like `max`/`xhigh`, keeping this conversion pure.
fn apply_output_config_effort(
    reasoning: Option<ReasoningRequest>,
    thinking: &Option<AnthropicThinking>,
    output_config: &Option<Value>,
) -> Option<ReasoningRequest> {
    if !matches!(thinking, Some(AnthropicThinking::Adaptive { .. })) {
        return reasoning;
    }
    let Some(effort) = output_config_effort(output_config) else {
        return reasoning;
    };
    Some(ReasoningRequest {
        effort: Some(effort),
        summary: reasoning.and_then(|reasoning| reasoning.summary),
    })
}

/// Pull the raw `output_config.effort` string out of the request. Any non-empty
/// value (trimmed) passes through verbatim as canonical `reasoning.effort`;
/// effort normalization is single-sourced in
/// `responses_to_chat::normalize_reasoning_effort` (trim + lowercase only — it
/// no longer clamps/maps), and the per-model clamp/map happens at the upstream
/// leaf in `upstream::finalize_request_for_backend`. Only missing/empty/
/// non-string values are dropped here so they cannot clobber a budget-derived
/// effort.
fn output_config_effort(output_config: &Option<Value>) -> Option<String> {
    let effort = output_config
        .as_ref()?
        .as_object()?
        .get("effort")?
        .as_str()?
        .trim();
    (!effort.is_empty()).then(|| effort.to_string())
}

fn extract_system_text(system: &Option<AnthropicSystemContent>) -> String {
    match system {
        Some(AnthropicSystemContent::Text(text)) => strip_billing_nonce(text),
        Some(AnthropicSystemContent::Blocks(blocks)) => blocks
            .iter()
            .map(|block| match block {
                crate::models::anthropic::AnthropicTextBlock::Text { text } => {
                    strip_billing_nonce(text)
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        None => String::new(),
    }
}

fn convert_thinking(
    thinking: &Option<AnthropicThinking>,
) -> (Option<ReasoningRequest>, BTreeMap<String, Value>) {
    let Some(thinking) = thinking else {
        return (None, BTreeMap::new());
    };
    match thinking {
        AnthropicThinking::Disabled => (None, BTreeMap::new()),
        AnthropicThinking::Enabled { budget_tokens }
        | AnthropicThinking::Adaptive { budget_tokens } => {
            let budget = budget_tokens.unwrap_or(10_000);
            let effort = thinking_effort_for_budget(budget);
            (
                Some(ReasoningRequest {
                    effort: Some(effort.to_string()),
                    summary: None,
                }),
                BTreeMap::new(),
            )
        }
    }
}

fn thinking_effort_for_budget(budget: u64) -> &'static str {
    if budget <= 5_000 {
        "low"
    } else if budget <= 20_000 {
        "medium"
    } else {
        "high"
    }
}

fn convert_messages(
    messages: &[crate::models::anthropic::AnthropicMessage],
) -> AppResult<Vec<ResponseItem>> {
    let mut items = Vec::new();
    for message in messages {
        convert_message(&message.role, &message.content, &mut items)?;
    }
    Ok(items)
}

fn convert_message(
    role: &str,
    content: &AnthropicContent,
    items: &mut Vec<ResponseItem>,
) -> AppResult<()> {
    match content {
        AnthropicContent::Text(text) => {
            if role == "user" {
                let text = strip_date_injection(text);
                if !text.is_empty() {
                    items.push(text_message_item(role, &text));
                }
            } else {
                let text = normalize_message_text(role, text);
                if !text.is_empty() {
                    items.push(text_message_item(role, &text));
                }
            }
        }
        AnthropicContent::Blocks(blocks) => {
            let mut content_items = Vec::new();
            let flush_message =
                |items: &mut Vec<ResponseItem>, content_items: &mut Vec<ContentItem>| {
                    if !content_items.is_empty() {
                        items.push(ResponseItem::Message {
                            id: None,
                            role: role.to_string(),
                            content: std::mem::take(content_items),
                            phase: None,
                        });
                    }
                };

            for block in blocks {
                match block {
                    AnthropicContentBlock::Text { text } => {
                        if role == "user" {
                            let text = strip_date_injection(text);
                            if !text.is_empty() {
                                content_items.push(ContentItem::InputText { text });
                            }
                        } else {
                            let text = normalize_message_text(role, text);
                            content_items.push(ContentItem::OutputText { text });
                        }
                    }
                    AnthropicContentBlock::Image { source } => {
                        content_items.push(image_source_to_content_item(source)?);
                    }
                    AnthropicContentBlock::ToolUse { id, name, input } => {
                        flush_message(items, &mut content_items);
                        push_function_call(items, id, name, input)?;
                    }
                    AnthropicContentBlock::ToolResult {
                        tool_use_id,
                        content: result_content,
                        ..
                    } => {
                        flush_message(items, &mut content_items);
                        let (output, images) = extract_tool_result_parts(result_content)?;
                        items.push(ResponseItem::FunctionCallOutput {
                            call_id: tool_use_id.clone(),
                            output,
                        });
                        if !images.is_empty() {
                            items.push(ResponseItem::Message {
                                id: None,
                                role: "user".to_string(),
                                content: images,
                                phase: None,
                            });
                        }
                    }
                    AnthropicContentBlock::ServerToolUse { id, name, input } => {
                        flush_message(items, &mut content_items);
                        push_function_call(items, id, name, input)?;
                    }
                    AnthropicContentBlock::WebSearchToolResult {
                        tool_use_id,
                        content,
                    } => {
                        flush_message(items, &mut content_items);
                        items.push(ResponseItem::FunctionCallOutput {
                            call_id: tool_use_id.clone(),
                            output: content.clone(),
                        });
                    }
                    AnthropicContentBlock::Thinking {
                        thinking,
                        signature,
                    } => {
                        flush_message(items, &mut content_items);
                        items.push(ResponseItem::Reasoning {
                            id: format!("rsn_{}", Uuid::new_v4().simple()),
                            summary: Vec::new(),
                            content: Some(vec![ReasoningContentItem::ReasoningText {
                                text: thinking.clone(),
                            }]),
                            // Strip empty AND our own synthetic marker (C2):
                            // a client that round-trips a thinking block we
                            // signed ourselves (no real upstream signature)
                            // must never have it re-forwarded upstream as if
                            // it were a genuine Anthropic signature -- a
                            // proxy can't mint one, and it's meaningless
                            // (possibly misleading) to any upstream. Genuine,
                            // non-empty signatures still forward unchanged.
                            encrypted_content: signature
                                .as_ref()
                                .filter(|signature| {
                                    !signature.is_empty()
                                        && !signature.starts_with(SYNTHETIC_SIGNATURE_PREFIX)
                                })
                                .cloned(),
                        });
                    }
                    AnthropicContentBlock::RedactedThinking { .. } => {
                        // Opaque Anthropic redacted-thinking payloads are only
                        // meaningful to Anthropic. Accept replayed histories
                        // from Claude Code without forwarding unreadable data
                        // to OpenAI-compatible upstreams.
                    }
                    AnthropicContentBlock::Other(value) => {
                        content_items.push(ContentItem::Other(value.clone()));
                    }
                }
            }
            flush_message(items, &mut content_items);
        }
    }
    Ok(())
}

fn push_function_call(
    items: &mut Vec<ResponseItem>,
    call_id: &str,
    name: &str,
    input: &Value,
) -> AppResult<()> {
    let arguments = serde_json::to_string(input)
        .map_err(|err| AppError::bad_request(format!("failed to serialize tool input: {err}")))?;
    items.push(ResponseItem::FunctionCall {
        id: None,
        name: name.to_string(),
        namespace: None,
        arguments,
        call_id: call_id.to_string(),
    });
    Ok(())
}

fn convert_tool_choice(
    tool_choice: Option<crate::models::anthropic::AnthropicToolChoice>,
) -> Value {
    match tool_choice {
        Some(crate::models::anthropic::AnthropicToolChoice::Auto) | None => {
            Value::String("auto".to_string())
        }
        Some(crate::models::anthropic::AnthropicToolChoice::Any) => {
            Value::String("required".to_string())
        }
        Some(crate::models::anthropic::AnthropicToolChoice::None) => {
            Value::String("none".to_string())
        }
        Some(crate::models::anthropic::AnthropicToolChoice::Tool { name }) => {
            json!({"type": "function", "function": {"name": name}})
        }
    }
}

fn convert_metadata(metadata: Option<Value>) -> AppResult<Option<HashMap<String, Value>>> {
    match metadata {
        None => Ok(None),
        Some(Value::Object(map)) => Ok(Some(map.into_iter().collect())),
        Some(_) => Err(AppError::bad_request(
            "Anthropic metadata must be a JSON object",
        )),
    }
}

fn normalize_message_text(role: &str, text: &str) -> String {
    if role == "user" {
        strip_date_injection(text)
    } else {
        text.to_string()
    }
}

fn strip_billing_nonce(text: &str) -> String {
    join_filtered_lines(text, |line| {
        !line.starts_with("x-anthropic-billing-header:")
    })
}

fn strip_date_injection(text: &str) -> String {
    join_filtered_lines(text, |line| !is_date_injection_line(line))
}

fn join_filtered_lines(text: &str, keep: impl Fn(&str) -> bool) -> String {
    let trailing_newline = text.ends_with('\n');
    let lines: Vec<&str> = text.lines().filter(|line| keep(line)).collect();
    if lines.is_empty() {
        return String::new();
    }
    let mut normalized = lines.join("\n");
    if trailing_newline {
        normalized.push('\n');
    }
    normalized
}

fn is_date_injection_line(line: &str) -> bool {
    let Some(rest) = line.strip_prefix("Today's date is ") else {
        return false;
    };
    let Some(date) = rest.strip_suffix('.') else {
        return false;
    };
    let bytes = date.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
}

fn text_message_item(role: &str, text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: role.to_string(),
        content: if role == "user" {
            vec![ContentItem::InputText {
                text: text.to_string(),
            }]
        } else {
            vec![ContentItem::OutputText {
                text: text.to_string(),
            }]
        },
        phase: None,
    }
}

fn image_source_to_content_item(source: &AnthropicImageSource) -> AppResult<ContentItem> {
    match source.kind.as_str() {
        "base64" => {
            let media_type = source.media_type.as_deref().unwrap_or("image/png");
            let data = source
                .data
                .as_deref()
                .filter(|data| !data.is_empty())
                .ok_or_else(|| {
                    AppError::bad_request("Anthropic base64 image source is missing data")
                })?;
            Ok(ContentItem::InputImage {
                image_url: Some(format!("data:{media_type};base64,{data}")),
                file_id: None,
                detail: None,
            })
        }
        "url" => {
            let image_url = source
                .url
                .clone()
                .filter(|url| !url.is_empty())
                .ok_or_else(|| AppError::bad_request("Anthropic image source is missing url"))?;
            Ok(ContentItem::InputImage {
                image_url: Some(image_url),
                file_id: None,
                detail: None,
            })
        }
        "file" => {
            let file_id = source
                .file_id
                .clone()
                .filter(|file_id| !file_id.is_empty())
                .ok_or_else(|| {
                    AppError::bad_request("Anthropic file image source is missing file_id")
                })?;
            Ok(ContentItem::InputImage {
                image_url: None,
                file_id: Some(file_id),
                detail: None,
            })
        }
        other => Err(AppError::bad_request(format!(
            "unsupported Anthropic image source type \"{other}\""
        ))),
    }
}

fn extract_tool_result_parts(
    content: &Option<AnthropicContent>,
) -> AppResult<(Value, Vec<ContentItem>)> {
    match content {
        Some(AnthropicContent::Text(text)) => Ok((Value::String(text.clone()), Vec::new())),
        Some(AnthropicContent::Blocks(blocks)) => {
            let mut text_parts = Vec::new();
            let mut images = Vec::new();
            for block in blocks {
                match block {
                    AnthropicContentBlock::Text { text } => text_parts.push(text.clone()),
                    AnthropicContentBlock::Image { source } => {
                        images.push(image_source_to_content_item(source)?);
                    }
                    _ => {}
                }
            }
            let output = if text_parts.len() == 1 {
                Value::String(text_parts.into_iter().next().unwrap())
            } else if text_parts.is_empty() {
                if images.is_empty() {
                    Value::Null
                } else {
                    Value::String("[image returned]".to_string())
                }
            } else {
                Value::String(text_parts.join("\n"))
            };
            Ok((output, images))
        }
        None => Ok((Value::Null, Vec::new())),
    }
}

fn convert_tools(tools: &Option<Vec<AnthropicTool>>) -> Vec<ToolSpec> {
    match tools {
        Some(tools) => {
            let mut sorted_tools = tools.clone();
            sorted_tools.sort_by(|a, b| a.name.cmp(&b.name));
            sorted_tools
                .into_iter()
                // The Anthropic `web_search` server tool (type
                // `web_search_20250305`, which Claude Code always sends and
                // forces via `tool_choice`) must run server-side via Brave, so
                // it maps to `ToolSpec::WebSearch` rather than a client
                // `Function`. The `type` field is dropped at deserialization,
                // so the tool name is the only signal available here.
                .map(|tool| match tool.name.as_str() {
                    "web_search" => ToolSpec::WebSearch {
                        external_web_access: None,
                        filters: None,
                        user_location: None,
                        search_context_size: None,
                        search_content_types: None,
                    },
                    _ => ToolSpec::Function {
                        name: tool.name,
                        description: tool.description.unwrap_or_default(),
                        strict: false,
                        parameters: tool.input_schema,
                    },
                })
                .collect()
        }
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::anthropic::*;
    use crate::models::responses::ResponseItem;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn converts_simple_text_request() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: Some(AnthropicSystemContent::Text(
                "x-anthropic-billing-header: nonce\nBe helpful".to_string(),
            )),
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: AnthropicContent::Text("Today's date is 2026-04-21.\nHello".to_string()),
            }],
            tools: None,
            tool_choice: None,
            stream: true,
            temperature: Some(0.2),
            top_p: Some(0.7),
            top_k: None,
            stop_sequences: None,
            metadata: Some(json!({"source": "anthropic"})),
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert_eq!(result.model, "claude-3-5-sonnet-20241022");
        assert_eq!(result.instructions, "Be helpful");
        assert_eq!(result.max_output_tokens, Some(1024));
        assert_eq!(result.temperature, Some(0.2));
        assert_eq!(result.top_p, Some(0.7));
        assert_eq!(
            result
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("source")),
            Some(&json!("anthropic"))
        );
        assert_eq!(result.input.len(), 1);
        assert!(matches!(
            &result.input[0],
            ResponseItem::Message { role, content, .. }
                if role == "user"
                    && matches!(&content[0], ContentItem::InputText { text } if text == "Hello")
        ));
        assert_eq!(result.stream, true);
    }

    #[test]
    fn preserves_claude_code_skill_listing_in_user_turns() {
        let skill_listing = concat!(
            "- deep-research: Deep research harness. Use when the user wants research.\n",
            "- update-config: Configure settings. Use when the user asks to update config.\n",
            "- security-review: Review a diff for security problems. Invoke with the request."
        );
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: Some(AnthropicSystemContent::Text(
                "Base instructions.".to_string(),
            )),
            messages: vec![
                AnthropicMessage {
                    role: "user".to_string(),
                    content: AnthropicContent::Text("hello".to_string()),
                },
                AnthropicMessage {
                    role: "user".to_string(),
                    content: AnthropicContent::Text(skill_listing.to_string()),
                },
            ],
            tools: None,
            tool_choice: None,
            stream: false,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert_eq!(result.instructions, "Base instructions.");
        assert_eq!(result.input.len(), 2);
        assert!(matches!(
            &result.input[1],
            ResponseItem::Message { role, content, .. }
                if role == "user"
                    && matches!(&content[0], ContentItem::InputText { text } if text.contains("deep-research"))
        ));
    }

    #[test]
    fn preserves_claude_code_system_history_role() {
        let skill_listing = concat!(
            "The following skills are available for use with the Skill tool:\n\n",
            "- deep-research: Deep research harness. Use when the user wants research.\n",
            "- update-config: Configure settings. Use when the user asks to update config.\n",
            "- security-review: Review a diff for security problems. Invoke with the request."
        );
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: Some(AnthropicSystemContent::Text(
                "Base instructions.".to_string(),
            )),
            messages: vec![
                AnthropicMessage {
                    role: "user".to_string(),
                    content: AnthropicContent::Text("hello".to_string()),
                },
                AnthropicMessage {
                    role: "system".to_string(),
                    content: AnthropicContent::Text(skill_listing.to_string()),
                },
            ],
            tools: None,
            tool_choice: None,
            stream: false,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert_eq!(result.instructions, "Base instructions.");
        assert_eq!(result.input.len(), 2);
        assert!(matches!(
            &result.input[1],
            ResponseItem::Message { role, content, .. }
                if role == "system"
                    && matches!(&content[0], ContentItem::OutputText { text } if text.contains("deep-research"))
        ));
    }

    #[test]
    fn preserves_claude_code_tagged_metadata_in_user_turns() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![
                AnthropicMessage {
                    role: "user".to_string(),
                    content: AnthropicContent::Text(
                        "<local-command-caveat>Do not answer local command output.</local-command-caveat>"
                            .to_string(),
                    ),
                },
                AnthropicMessage {
                    role: "user".to_string(),
                    content: AnthropicContent::Text(
                        "<command-name>/clear</command-name>\n<command-message>clear</command-message>\n<command-args></command-args>"
                            .to_string(),
                    ),
                },
                AnthropicMessage {
                    role: "user".to_string(),
                    content: AnthropicContent::Text(
                        "<system-reminder>Prefer concise answers.</system-reminder>\nhello"
                            .to_string(),
                    ),
                },
            ],
            tools: None,
            tool_choice: None,
            stream: false,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert!(result.instructions.is_empty());
        assert_eq!(result.input.len(), 3);
        assert!(matches!(
            &result.input[2],
            ResponseItem::Message { role, content, .. }
                if role == "user"
                    && matches!(&content[0], ContentItem::InputText { text } if text.contains("system-reminder"))
        ));
    }

    #[test]
    fn converts_stop_sequences_to_typed_stop() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(64),
            system: None,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: AnthropicContent::Text("Decide.".to_string()),
            }],
            tools: None,
            tool_choice: None,
            stream: false,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: Some(vec!["</decision>".to_string()]),
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert_eq!(result.stop, Some(vec!["</decision>".to_string()]));
        assert!(!result.extra_body.contains_key("stop"));
    }

    #[test]
    fn converts_output_config_json_schema_to_text_format() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "title": { "type": "string" }
            },
            "required": ["title"]
        });
        let request = AnthropicRequest {
            model: "claude-haiku-4-5-20251001".to_string(),
            max_tokens: Some(32000),
            system: Some(AnthropicSystemContent::Blocks(vec![
                AnthropicTextBlock::Text {
                    text: "Return JSON with a single \"title\" field.".to_string(),
                },
            ])),
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: AnthropicContent::Text("Build a web app.".to_string()),
            }],
            tools: Some(Vec::new()),
            tool_choice: None,
            stream: true,
            temperature: Some(1.0),
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: Some(json!({
                "format": {
                    "type": "json_schema",
                    "schema": schema
                }
            })),
        };

        let result = convert_request(request).expect("convert");
        let format = result
            .text
            .as_ref()
            .and_then(|text| text.format.as_ref())
            .expect("text format");
        assert_eq!(format.kind, "json_schema");
        assert_eq!(format.name, "response");
        assert!(format.strict);
        assert_eq!(format.schema, schema);
    }

    #[test]
    fn converts_tool_use_and_result_history() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![
                AnthropicMessage {
                    role: "user".to_string(),
                    content: AnthropicContent::Text("What is the weather?".to_string()),
                },
                AnthropicMessage {
                    role: "assistant".to_string(),
                    content: AnthropicContent::Blocks(vec![
                        AnthropicContentBlock::Text {
                            text: "Let me check.".to_string(),
                        },
                        AnthropicContentBlock::ToolUse {
                            id: "toolu_1".to_string(),
                            name: "get_weather".to_string(),
                            input: json!({"location": "Seattle"}),
                        },
                    ]),
                },
                AnthropicMessage {
                    role: "user".to_string(),
                    content: AnthropicContent::Blocks(vec![AnthropicContentBlock::ToolResult {
                        tool_use_id: "toolu_1".to_string(),
                        content: Some(AnthropicContent::Text("72°F sunny".to_string())),
                        is_error: None,
                    }]),
                },
            ],
            tools: None,
            tool_choice: None,
            stream: true,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        // user text, assistant text, function_call, function_call_output
        assert_eq!(result.input.len(), 4);

        let user_msg = &result.input[0];
        assert!(matches!(user_msg, ResponseItem::Message { role, .. } if role == "user"));

        let asst_text = &result.input[1];
        assert!(matches!(asst_text, ResponseItem::Message { role, .. } if role == "assistant"));

        let fn_call = &result.input[2];
        assert!(
            matches!(fn_call, ResponseItem::FunctionCall { name, call_id, .. }
            if name == "get_weather" && call_id == "toolu_1")
        );

        let fn_output = &result.input[3];
        assert!(
            matches!(fn_output, ResponseItem::FunctionCallOutput { call_id, .. } if call_id == "toolu_1")
        );
    }

    #[test]
    fn accepts_replayed_server_tool_history_blocks() {
        let request: AnthropicRequest = serde_json::from_value(json!({
            "model": "claude-3-5-sonnet-20241022",
            "max_tokens": 1024,
            "messages": [
                { "role": "user", "content": "Search first." },
                {
                    "role": "assistant",
                    "content": [
                        { "type": "server_tool_use", "id": "srvtoolu_1", "name": "web_search", "input": { "query": "weather seattle" } },
                        {
                            "type": "web_search_tool_result",
                            "tool_use_id": "srvtoolu_1",
                            "content": [
                                { "type": "web_search_result", "url": "https://example.com/weather", "title": "Weather" }
                            ]
                        },
                        { "type": "text", "text": "It is raining." }
                    ]
                },
                { "role": "user", "content": "Why?" }
            ]
        }))
        .expect("deserialize Anthropic server tool history");

        let result = convert_request(request).expect("convert");
        assert_eq!(result.input.len(), 5);
        assert!(matches!(
            &result.input[1],
            ResponseItem::FunctionCall {
                name,
                call_id,
                arguments,
                ..
            } if name == "web_search"
                && call_id == "srvtoolu_1"
                && arguments == "{\"query\":\"weather seattle\"}"
        ));
        assert!(matches!(
            &result.input[2],
            ResponseItem::FunctionCallOutput { call_id, output }
                if call_id == "srvtoolu_1"
                    && output[0]["type"] == "web_search_result"
        ));
        assert!(matches!(
            &result.input[3],
            ResponseItem::Message { role, content, .. }
                if role == "assistant"
                    && matches!(
                        &content[0],
                        ContentItem::OutputText { text } if text == "It is raining."
                    )
        ));
    }

    #[test]
    fn converts_tool_result_images_to_user_image_messages() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: AnthropicContent::Blocks(vec![AnthropicContentBlock::ToolResult {
                    tool_use_id: "toolu_1".to_string(),
                    content: Some(AnthropicContent::Blocks(vec![
                        AnthropicContentBlock::Text {
                            text: "screenshot attached".to_string(),
                        },
                        AnthropicContentBlock::Image {
                            source: AnthropicImageSource {
                                kind: "url".to_string(),
                                media_type: None,
                                data: None,
                                url: Some("https://example.com/result.png".to_string()),
                                file_id: None,
                            },
                        },
                    ])),
                    is_error: None,
                }]),
            }],
            tools: None,
            tool_choice: None,
            stream: true,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert_eq!(result.input.len(), 2);
        assert!(matches!(
            &result.input[0],
            ResponseItem::FunctionCallOutput { call_id, output }
                if call_id == "toolu_1" && output == &json!("screenshot attached")
        ));
        assert!(matches!(
            &result.input[1],
            ResponseItem::Message { role, content, .. }
                if role == "user"
                    && matches!(
                        &content[0],
                        ContentItem::InputImage { image_url: Some(url), .. }
                            if url == "https://example.com/result.png"
                    )
        ));
    }

    #[test]
    fn converts_tools_to_function_specs() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: AnthropicContent::Text("Hi".to_string()),
            }],
            tools: Some(vec![
                AnthropicTool {
                    name: "zulu".to_string(),
                    description: Some("Z".to_string()),
                    input_schema: json!({"type": "object"}),
                },
                AnthropicTool {
                    name: "alpha".to_string(),
                    description: Some("A".to_string()),
                    input_schema: json!({
                        "type": "object",
                        "properties": { "location": { "type": "string" } },
                        "required": ["location"]
                    }),
                },
            ]),
            tool_choice: None,
            stream: true,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert_eq!(result.tools.len(), 2);
        assert!(matches!(&result.tools[0], ToolSpec::Function { name, .. } if name == "alpha"));
    }

    #[test]
    fn converts_thinking_enabled_to_reasoning() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(16000),
            system: None,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: AnthropicContent::Text("Think".to_string()),
            }],
            tools: None,
            tool_choice: None,
            stream: true,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: Some(AnthropicThinking::Enabled {
                budget_tokens: Some(10000),
            }),
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert!(result.reasoning.is_some());
        assert_eq!(
            result.reasoning.as_ref().unwrap().effort.as_deref(),
            Some("medium")
        );
        assert!(result.extra_body.is_empty());
    }

    #[test]
    fn converts_thinking_history_signature_to_encrypted_content() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![AnthropicMessage {
                role: "assistant".to_string(),
                content: AnthropicContent::Blocks(vec![AnthropicContentBlock::Thinking {
                    thinking: "private chain".to_string(),
                    signature: Some("sig_history".to_string()),
                }]),
            }],
            tools: None,
            tool_choice: None,
            stream: true,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert_eq!(result.input.len(), 1);
        assert!(matches!(
            &result.input[0],
            ResponseItem::Reasoning {
                content: Some(content),
                encrypted_content: Some(signature),
                ..
            } if signature == "sig_history"
                && matches!(
                    &content[0],
                    ReasoningContentItem::ReasoningText { text } if text == "private chain"
            )
        ));
    }

    #[test]
    fn ingress_strips_synthetic_thinking_signature_but_forwards_real_ones() {
        // C2 round-trip (AGENTS.md "no new wire field without a round-trip
        // test"): a client that echoes back one of OUR OWN synthetic-marked
        // thinking signatures (minted by the egress converter when the
        // upstream reasoning channel carried none, see
        // `SYNTHETIC_SIGNATURE_PREFIX`) as history must never have it
        // re-forwarded upstream as if it were a genuine Anthropic signature.
        // A real signature must still forward unchanged.
        let synthetic_signature = format!("{SYNTHETIC_SIGNATURE_PREFIX}deadbeef");
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![AnthropicMessage {
                role: "assistant".to_string(),
                content: AnthropicContent::Blocks(vec![
                    AnthropicContentBlock::Thinking {
                        thinking: "synthetic chain".to_string(),
                        signature: Some(synthetic_signature),
                    },
                    AnthropicContentBlock::Thinking {
                        thinking: "real chain".to_string(),
                        signature: Some("abc123".to_string()),
                    },
                ]),
            }],
            tools: None,
            tool_choice: None,
            stream: true,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert_eq!(result.input.len(), 2);
        assert!(
            matches!(
                &result.input[0],
                ResponseItem::Reasoning {
                    content: Some(content),
                    encrypted_content: None,
                    ..
                } if matches!(
                    &content[0],
                    ReasoningContentItem::ReasoningText { text } if text == "synthetic chain"
                )
            ),
            "a synthetic-prefixed signature must be stripped to None, got {:?}",
            result.input[0]
        );
        assert!(
            matches!(
                &result.input[1],
                ResponseItem::Reasoning {
                    content: Some(content),
                    encrypted_content: Some(signature),
                    ..
                } if signature == "abc123"
                    && matches!(
                        &content[0],
                        ReasoningContentItem::ReasoningText { text } if text == "real chain"
                    )
            ),
            "a genuine signature must still forward unchanged, got {:?}",
            result.input[1]
        );
    }

    #[test]
    fn preserves_assistant_thinking_before_text_history_order() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![AnthropicMessage {
                role: "assistant".to_string(),
                content: AnthropicContent::Blocks(vec![
                    AnthropicContentBlock::Thinking {
                        thinking: "private".to_string(),
                        signature: Some("sig".to_string()),
                    },
                    AnthropicContentBlock::Text {
                        text: "Answer".to_string(),
                    },
                ]),
            }],
            tools: None,
            tool_choice: None,
            stream: true,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request).expect("convert");
        assert_eq!(result.input.len(), 2);
        assert!(matches!(
            &result.input[0],
            ResponseItem::Reasoning {
                content: Some(content),
                encrypted_content: Some(signature),
                ..
            } if signature == "sig"
                && matches!(
                    &content[0],
                    ReasoningContentItem::ReasoningText { text } if text == "private"
                )
        ));
        assert!(matches!(
            &result.input[1],
            ResponseItem::Message { role, content, .. }
                if role == "assistant"
                    && matches!(
                        &content[0],
                        ContentItem::OutputText { text } if text == "Answer"
                    )
        ));
    }

    #[test]
    fn accepts_non_streaming_request() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: AnthropicContent::Text("Hello".to_string()),
            }],
            tools: None,
            tool_choice: None,
            stream: false,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let result = convert_request(request);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().stream, true);
    }

    #[test]
    fn converts_anthropic_tool_choice_variants() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: AnthropicContent::Text("Hello".to_string()),
            }],
            tools: Some(vec![AnthropicTool {
                name: "echo".to_string(),
                description: Some("Echo".to_string()),
                input_schema: json!({"type": "object"}),
            }]),
            tool_choice: Some(AnthropicToolChoice::Tool {
                name: "echo".to_string(),
            }),
            stream: true,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };

        let tool_specific = convert_request(request).expect("convert");
        assert_eq!(
            tool_specific.tool_choice,
            json!({"type": "function", "function": {"name": "echo"}})
        );

        assert_eq!(
            convert_tool_choice(Some(AnthropicToolChoice::Any)),
            Value::String("required".to_string())
        );
        assert_eq!(
            convert_tool_choice(Some(AnthropicToolChoice::None)),
            Value::String("none".to_string())
        );
    }

    #[test]
    fn rejects_unsupported_anthropic_fields() {
        let request = AnthropicRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: Some(1024),
            system: None,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: AnthropicContent::Text("Hello".to_string()),
            }],
            tools: None,
            tool_choice: None,
            stream: true,
            temperature: None,
            top_p: None,
            top_k: Some(5),
            stop_sequences: None,
            metadata: None,
            thinking: None,
            output_config: None,
        };
        assert!(convert_request(request).is_err());
    }

    #[test]
    fn converts_base64_image_to_input_image() {
        let source = AnthropicImageSource {
            kind: "base64".to_string(),
            media_type: Some("image/png".to_string()),
            data: Some("iVBORw0KGgo=".to_string()),
            url: None,
            file_id: None,
        };
        let item = image_source_to_content_item(&source).expect("image item");
        assert!(matches!(
            item,
            ContentItem::InputImage {
                image_url: Some(ref image_url),
                file_id: None,
                ..
            } if image_url == "data:image/png;base64,iVBORw0KGgo="
        ));
    }

    #[test]
    fn converts_url_image_source() {
        let source = AnthropicImageSource {
            kind: "url".to_string(),
            media_type: None,
            data: None,
            url: Some("https://example.com/img.png".to_string()),
            file_id: None,
        };
        let item = image_source_to_content_item(&source).expect("image item");
        assert!(matches!(
            item,
            ContentItem::InputImage {
                image_url: Some(ref image_url),
                file_id: None,
                ..
            } if image_url == "https://example.com/img.png"
        ));
    }

    #[test]
    fn converts_file_image_source_to_file_id() {
        let source = AnthropicImageSource {
            kind: "file".to_string(),
            media_type: None,
            data: None,
            url: None,
            file_id: Some("file_123".to_string()),
        };
        let item = image_source_to_content_item(&source).expect("image item");
        assert!(matches!(
            item,
            ContentItem::InputImage {
                image_url: None,
                file_id: Some(ref file_id),
                ..
            } if file_id == "file_123"
        ));
    }
}
