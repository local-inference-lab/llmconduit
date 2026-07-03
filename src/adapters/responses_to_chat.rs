use crate::error::AppError;
use crate::error::AppResult;
use crate::models::chat::ChatFunctionCall;
use crate::models::chat::ChatMessage;
use crate::models::chat::ChatThinking;
use crate::models::chat::ChatTool;
use crate::models::chat::ChatToolCall;
use crate::models::chat::ChatToolDefinition;
use crate::models::responses::ContentItem;
use crate::models::responses::LocalShellAction;
use crate::models::responses::NamespaceToolSpec;
use crate::models::responses::ResponseItem;
use crate::models::responses::ResponsesRequest;
use crate::models::responses::ToolSpec;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum ToolKind {
    Function {
        public_name: String,
        namespace: Option<String>,
    },
    Custom {
        public_name: String,
    },
    LocalShell,
    ToolSearch,
    WebSearch,
    /// G4 server-side image analysis (`analyzeImage`). Registered ONLY when the
    /// image agent is active for the turn (see `lower_request`'s
    /// `image_agent_active`), so a client-supplied `analyzeImage` tool on a
    /// non-image turn still classifies as a normal client `Function`.
    ImageAnalysis,
}

#[derive(Debug, Clone)]
pub struct ToolRegistry {
    by_name: HashMap<String, ToolKind>,
}

impl ToolRegistry {
    pub fn get(&self, name: &str) -> Option<&ToolKind> {
        self.by_name.get(name)
    }
}

#[cfg(test)]
impl ToolRegistry {
    pub fn from_map(by_name: HashMap<String, ToolKind>) -> Self {
        Self { by_name }
    }
}

#[derive(Debug, Clone)]
pub struct LoweredTurn {
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ChatTool>,
    pub tool_registry: ToolRegistry,
    pub response_format: Option<Value>,
    pub reasoning_effort: Option<String>,
    pub frequency_penalty: Option<f64>,
    pub presence_penalty: Option<f64>,
}

#[derive(Debug, Clone)]
struct PendingReasoning {
    text: String,
    signature: Option<String>,
}

impl PendingReasoning {
    fn from_parts(text: String, signature: Option<String>) -> Self {
        Self { text, signature }
    }

    fn append(&mut self, text: String, signature: Option<String>) {
        if !self.text.is_empty() && !text.is_empty() {
            self.text.push_str("\n\n");
            self.text.push_str(&text);
        } else if self.text.is_empty() {
            self.text = text;
        }
        if self.signature.is_none() {
            self.signature = signature;
        }
    }

    fn into_chat_parts(self) -> (Option<String>, Option<ChatThinking>) {
        let thinking = self.signature.clone().map(|signature| ChatThinking {
            content: self.text.clone(),
            signature: Some(signature),
        });
        (Some(self.text), thinking)
    }
}

pub fn lower_request(
    request: &ResponsesRequest,
    baseline_messages: Vec<ChatMessage>,
) -> AppResult<LoweredTurn> {
    lower_request_with_options(
        request,
        baseline_messages,
        &crate::config::default_reasoning_effort(),
        false,
    )
}

/// Like [`lower_request`], but with an explicit default reasoning effort and
/// `image_agent_active`, which decides whether an `analyzeImage` tool
/// classifies as the server-side [`ToolKind::ImageAnalysis`] (true, the
/// gateway runs it) or a plain client `Function` (false). The engine passes
/// `true` only on turns where G4 gating activated the image agent, so a
/// caller that happens to define its own `analyzeImage` tool on a text turn is
/// unaffected.
pub fn lower_request_with_options(
    request: &ResponsesRequest,
    baseline_messages: Vec<ChatMessage>,
    default_reasoning_effort: &str,
    image_agent_active: bool,
) -> AppResult<LoweredTurn> {
    validate_request(request)?;
    let mut messages = baseline_messages;
    if messages.is_empty() && !request.instructions.is_empty() {
        messages.push(ChatMessage {
            role: "system".to_string(),
            content: Some(Value::String(request.instructions.clone())),
            tool_call_id: None,
            name: None,
            reasoning_content: None,
            thinking: None,
            tool_calls: None,
        });
    }
    let tools = lower_tools(&request.tools)?;
    let registry = build_tool_registry(&request.tools, image_agent_active)?;
    let mut pending_reasoning: Option<PendingReasoning> = None;
    for item in &request.input {
        match item {
            ResponseItem::Message { role, content, .. } => {
                let text = message_content_to_chat_value(content)?;
                let normalized_role = normalize_chat_role(role);
                let (reasoning_content, thinking) = if normalized_role == "assistant" {
                    pending_reasoning
                        .take()
                        .map(PendingReasoning::into_chat_parts)
                        .unwrap_or((None, None))
                } else {
                    (None, None)
                };
                messages.push(ChatMessage {
                    role: normalized_role,
                    content: Some(text),
                    tool_call_id: None,
                    name: None,
                    reasoning_content,
                    thinking,
                    tool_calls: None,
                });
            }
            ResponseItem::Reasoning {
                summary,
                content,
                encrypted_content,
                ..
            } => {
                let text = reasoning_item_text(summary, content);
                let signature = encrypted_content
                    .as_ref()
                    .filter(|signature| !signature.is_empty())
                    .cloned();
                if let Some(existing) = pending_reasoning.as_mut() {
                    existing.append(text, signature);
                } else {
                    pending_reasoning = Some(PendingReasoning::from_parts(text, signature));
                }
            }
            ResponseItem::FunctionCall {
                name,
                arguments,
                call_id,
                ..
            } => append_tool_call(
                &mut messages,
                call_id.clone(),
                name.clone(),
                parse_json_string(arguments)?,
                pending_reasoning.take(),
            ),
            ResponseItem::CustomToolCall {
                call_id,
                name,
                input,
                ..
            } => append_tool_call(
                &mut messages,
                call_id.clone(),
                name.clone(),
                json!({ "input": input }),
                pending_reasoning.take(),
            ),
            ResponseItem::ToolSearchCall {
                call_id,
                arguments,
                execution,
                ..
            } => {
                if execution != "client" {
                    return Err(AppError::bad_request(
                        "only tool_search calls with execution=client are supported",
                    ));
                }
                append_tool_call(
                    &mut messages,
                    call_id
                        .clone()
                        .unwrap_or_else(|| "tool_search_missing_call_id".to_string()),
                    "tool_search".to_string(),
                    arguments.clone(),
                    pending_reasoning.take(),
                );
            }
            ResponseItem::LocalShellCall {
                call_id,
                id,
                action,
                ..
            } => {
                let call_id = call_id
                    .clone()
                    .or_else(|| id.clone())
                    .ok_or_else(|| AppError::bad_request("local_shell_call missing call_id"))?;
                let arguments = match action {
                    LocalShellAction::Exec(exec) => serde_json::to_value(exec).map_err(|err| {
                        AppError::bad_request(format!(
                            "failed to serialize local_shell_call action: {err}"
                        ))
                    })?,
                };
                append_tool_call(
                    &mut messages,
                    call_id,
                    "local_shell".to_string(),
                    arguments,
                    pending_reasoning.take(),
                );
            }
            ResponseItem::FunctionCallOutput { call_id, output }
            | ResponseItem::CustomToolCallOutput {
                call_id, output, ..
            } => messages.push(ChatMessage {
                role: "tool".to_string(),
                content: Some(Value::String(stringify_tool_output(output))),
                tool_call_id: Some(call_id.clone()),
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }),
            ResponseItem::ToolSearchOutput {
                call_id,
                status,
                execution,
                tools,
            } => messages.push(ChatMessage {
                role: "tool".to_string(),
                content: Some(Value::String(
                    serde_json::to_string(&json!({
                        "status": status,
                        "execution": execution,
                        "tools": tools,
                    }))
                    .map_err(|err| {
                        AppError::bad_request(format!(
                            "failed to serialize tool_search_output: {err}"
                        ))
                    })?,
                )),
                tool_call_id: call_id.clone(),
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }),
            ResponseItem::WebSearchCall { id, action, .. } => {
                let call_id = id
                    .clone()
                    .unwrap_or_else(|| format!("web_search_missing_replay_{}", messages.len()));
                append_tool_call(
                    &mut messages,
                    call_id.clone(),
                    "web_search".to_string(),
                    web_search_arguments(action),
                    pending_reasoning.take(),
                );
                messages.push(ChatMessage {
                    role: "tool".to_string(),
                    content: Some(Value::String(web_search_placeholder_result(action))),
                    tool_call_id: Some(call_id),
                    name: None,
                    reasoning_content: None,
                    thinking: None,
                    tool_calls: None,
                });
            }
            ResponseItem::ImageGenerationCall { .. } => {}
        }
    }
    if let Some(reasoning) = pending_reasoning.take() {
        let (reasoning_content, thinking) = reasoning.into_chat_parts();
        messages.push(ChatMessage {
            role: "assistant".to_string(),
            content: None,
            tool_call_id: None,
            name: None,
            reasoning_content,
            thinking,
            tool_calls: None,
        });
    }
    hoist_system_messages(&mut messages);
    let response_format = request
        .text
        .as_ref()
        .and_then(|text| text.format.as_ref())
        .map(|format| {
            json!({
                "type": format.kind,
                "json_schema": {
                    "name": format.name,
                    "schema": format.schema,
                    "strict": format.strict,
                }
            })
        });
    let request_reasoning_effort = request
        .reasoning
        .as_ref()
        .and_then(|reasoning| reasoning.effort.as_deref());
    let reasoning_effort =
        normalize_reasoning_effort(request_reasoning_effort.or(Some(default_reasoning_effort)))?;
    Ok(LoweredTurn {
        messages,
        tools,
        tool_registry: registry,
        response_format,
        reasoning_effort,
        frequency_penalty: request.frequency_penalty,
        presence_penalty: request.presence_penalty,
    })
}

fn normalize_chat_role(role: &str) -> String {
    match role {
        "developer" => "system".to_string(),
        _ => role.to_string(),
    }
}

fn normalize_reasoning_effort(effort: Option<&str>) -> AppResult<Option<String>> {
    match effort.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some(value) => {
            let normalized = match value.to_ascii_lowercase().as_str() {
                "max" | "xhigh" => "max",
                // OSS reasoning models are converging on a smaller upstream
                // vocabulary. Anything below max is sent as high.
                _ => "high",
            };
            Ok(Some(normalized.to_string()))
        }
    }
}

fn hoist_system_messages(messages: &mut Vec<ChatMessage>) {
    // Find the end of the initial contiguous block of system messages.
    let prefix_end = messages
        .iter()
        .position(|m| m.role != "system")
        .unwrap_or(messages.len());
    if prefix_end == 0 {
        return;
    }
    let mut system_texts: Vec<String> = Vec::new();
    for msg in &messages[..prefix_end] {
        if let Some(content) = &msg.content {
            let text = match content {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            if !text.is_empty() {
                system_texts.push(text);
            }
        }
    }
    let rest: Vec<ChatMessage> = messages.drain(prefix_end..).collect();
    messages.clear();
    if !system_texts.is_empty() {
        messages.push(ChatMessage {
            role: "system".to_string(),
            content: Some(Value::String(system_texts.join("\n\n"))),
            tool_call_id: None,
            name: None,
            reasoning_content: None,
            thinking: None,
            tool_calls: None,
        });
    }
    messages.extend(rest);
}

fn validate_request(request: &ResponsesRequest) -> AppResult<()> {
    if request.previous_response_id.is_some() {
        return Err(AppError::bad_request(
            "previous_response_id is not supported in v1",
        ));
    }
    // ImageGeneration tools are silently stripped (not sent to upstream).
    // Client-side MCP servers handle image generation via function tools.
    // Validate tool_choice
    match &request.tool_choice {
        Value::String(s) => match s.as_str() {
            "auto" | "none" => {}
            "required" => {
                if request.tools.is_empty() {
                    return Err(AppError::bad_request(
                        "tool_choice is \"required\" but no tools are provided",
                    ));
                }
            }
            other => {
                return Err(AppError::bad_request(format!(
                    "invalid tool_choice string: \"{other}\"; expected \"auto\", \"none\", or \"required\""
                )));
            }
        },
        Value::Object(map) => {
            let valid = map.get("type").and_then(|v| v.as_str()) == Some("function")
                && map
                    .get("function")
                    .and_then(|f| f.as_object())
                    .and_then(|f| f.get("name"))
                    .and_then(|n| n.as_str())
                    .map(|n| !n.is_empty())
                    == Some(true);
            if !valid {
                return Err(AppError::bad_request(
                    "invalid tool_choice object: expected {\"type\":\"function\",\"function\":{\"name\":\"<non-empty>\"}}",
                ));
            }
            if request.tools.is_empty() {
                return Err(AppError::bad_request(
                    "tool_choice specifies a function but no tools are provided",
                ));
            }
        }
        _ => {
            return Err(AppError::bad_request(
                "invalid tool_choice: expected a string (\"auto\", \"none\", \"required\") or a function object",
            ));
        }
    }
    Ok(())
}

fn lower_tools(specs: &[ToolSpec]) -> AppResult<Vec<ChatTool>> {
    let mut tools = Vec::new();
    let mut seen_names = HashMap::new();
    for spec in specs {
        let lowered_tools = match spec {
            ToolSpec::Function {
                name,
                description,
                strict,
                parameters,
            } => vec![ChatTool {
                kind: "function".to_string(),
                function: ChatToolDefinition {
                    name: name.clone(),
                    description: description.clone(),
                    parameters: Some(parameters.clone()),
                    strict: *strict,
                },
            }],
            ToolSpec::Namespace {
                tools: namespace_tools,
                ..
            } => namespace_tools
                .iter()
                .map(|tool| match tool {
                    NamespaceToolSpec::Function {
                        name,
                        description,
                        strict,
                        parameters,
                    } => ChatTool {
                        kind: "function".to_string(),
                        function: ChatToolDefinition {
                            name: name.clone(),
                            description: description.clone(),
                            parameters: Some(parameters.clone()),
                            strict: *strict,
                        },
                    },
                })
                .collect(),
            ToolSpec::ToolSearch {
                description,
                parameters,
                ..
            } => vec![ChatTool {
                kind: "function".to_string(),
                function: ChatToolDefinition {
                    name: "tool_search".to_string(),
                    description: description.clone(),
                    parameters: Some(parameters.clone()),
                    strict: false,
                },
            }],
            ToolSpec::LocalShell {} => vec![ChatTool {
                kind: "function".to_string(),
                function: ChatToolDefinition {
                    name: "local_shell".to_string(),
                    description: "Execute a shell command locally.".to_string(),
                    parameters: Some(json!({
                        "type": "object",
                        "properties": {
                            "command": {
                                "type": "array",
                                "items": { "type": "string" }
                            },
                            "timeout_ms": { "type": "integer" },
                            "working_directory": { "type": "string" },
                            "env": {
                                "type": "object",
                                "additionalProperties": { "type": "string" }
                            },
                            "user": { "type": "string" }
                        },
                        "required": ["command"]
                    })),
                    strict: false,
                },
            }],
            ToolSpec::WebSearch { .. } => vec![ChatTool {
                kind: "function".to_string(),
                function: ChatToolDefinition {
                    name: "web_search".to_string(),
                    description: "Search the web and return relevant result snippets.".to_string(),
                    parameters: Some(json!({
                        "type": "object",
                        "properties": {
                            "query": { "type": "string" }
                        },
                        "required": ["query"]
                    })),
                    strict: false,
                },
            }],
            ToolSpec::Custom {
                name,
                description,
                format,
            } => vec![ChatTool {
                kind: "function".to_string(),
                function: ChatToolDefinition {
                    name: name.clone(),
                    description: format!(
                        "{description}\n\nReturn the raw tool input as a string matching the {} {} format.",
                        format.kind, format.syntax
                    ),
                    parameters: Some(json!({
                        "type": "object",
                        "properties": {
                            "input": { "type": "string" }
                        },
                        "required": ["input"]
                    })),
                    strict: false,
                },
            }],
            ToolSpec::ImageGeneration { .. } => Vec::new(),
        };
        for tool in lowered_tools {
            let name = tool.function.name.clone();
            if seen_names.insert(name.clone(), ()).is_some() {
                return Err(AppError::bad_request(format!(
                    "duplicate tool name is not supported: {name}"
                )));
            }
            tools.push(tool);
        }
    }
    tools.sort_by(|a, b| a.function.name.cmp(&b.function.name));
    Ok(tools)
}

fn build_tool_registry(specs: &[ToolSpec], image_agent_active: bool) -> AppResult<ToolRegistry> {
    let mut by_name = HashMap::new();
    for spec in specs {
        let lowered_kinds: Vec<(String, ToolKind)> = match spec {
            // G4: on an active image-agent turn, classify the injected (or
            // caller-supplied) `analyzeImage` function as the server-side
            // ImageAnalysis tool so the engine runs it instead of handing it to
            // the client. On a non-image turn it stays a normal client Function.
            ToolSpec::Function { name, .. }
                if image_agent_active
                    && name.eq_ignore_ascii_case(crate::vision::ANALYZE_IMAGE_TOOL_NAME) =>
            {
                vec![(name.clone(), ToolKind::ImageAnalysis)]
            }
            ToolSpec::Function { name, .. } => vec![(
                name.clone(),
                ToolKind::Function {
                    public_name: name.clone(),
                    namespace: None,
                },
            )],
            ToolSpec::Namespace {
                name: namespace,
                tools,
                ..
            } => tools
                .iter()
                .map(|tool| match tool {
                    NamespaceToolSpec::Function { name, .. } => (
                        name.clone(),
                        ToolKind::Function {
                            public_name: name.clone(),
                            namespace: Some(namespace.clone()),
                        },
                    ),
                })
                .collect(),
            ToolSpec::ToolSearch { .. } => {
                vec![("tool_search".to_string(), ToolKind::ToolSearch)]
            }
            ToolSpec::LocalShell {} => vec![("local_shell".to_string(), ToolKind::LocalShell)],
            ToolSpec::WebSearch { .. } => vec![("web_search".to_string(), ToolKind::WebSearch)],
            ToolSpec::Custom { name, .. } => vec![(
                name.clone(),
                ToolKind::Custom {
                    public_name: name.clone(),
                },
            )],
            ToolSpec::ImageGeneration { .. } => Vec::new(),
        };
        for (name, kind) in lowered_kinds {
            let name_lc = name.to_ascii_lowercase();
            if by_name.insert(name_lc.clone(), kind).is_some() {
                return Err(AppError::bad_request(format!(
                    "duplicate tool name is not supported: {name_lc}"
                )));
            }
        }
    }
    Ok(ToolRegistry { by_name })
}

fn append_tool_call(
    messages: &mut Vec<ChatMessage>,
    call_id: String,
    name: String,
    arguments: Value,
    pending_reasoning: Option<PendingReasoning>,
) {
    if let Some(last) = messages.last_mut()
        && last.role == "assistant"
        && (last.tool_calls.is_some() || last.content.is_none())
    {
        let index = last.tool_calls.as_ref().map(|v| v.len()).unwrap_or(0);
        let tool_call = ChatToolCall {
            id: Some(call_id),
            index: Some(index),
            kind: "function".to_string(),
            function: ChatFunctionCall {
                name: Some(name),
                arguments: Some(arguments),
            },
        };
        if let Some(existing) = &mut last.tool_calls {
            existing.push(tool_call);
        } else {
            last.tool_calls = Some(vec![tool_call]);
        }
        if let Some(reasoning) = pending_reasoning
            && last.reasoning_content.is_none()
        {
            let (reasoning_content, thinking) = reasoning.into_chat_parts();
            last.reasoning_content = reasoning_content;
            last.thinking = thinking;
        }
        return;
    }
    let tool_call = ChatToolCall {
        id: Some(call_id),
        index: Some(0),
        kind: "function".to_string(),
        function: ChatFunctionCall {
            name: Some(name),
            arguments: Some(arguments),
        },
    };
    messages.push(ChatMessage {
        role: "assistant".to_string(),
        content: None,
        tool_call_id: None,
        name: None,
        reasoning_content: pending_reasoning
            .as_ref()
            .map(|reasoning| reasoning.text.clone()),
        thinking: pending_reasoning.and_then(|reasoning| {
            reasoning.signature.map(|signature| ChatThinking {
                content: reasoning.text,
                signature: Some(signature),
            })
        }),
        tool_calls: Some(vec![tool_call]),
    });
}

fn parse_json_string(raw: &str) -> AppResult<Value> {
    serde_json::from_str(raw)
        .map_err(|err| AppError::bad_request(format!("invalid JSON tool arguments: {err}")))
}

fn web_search_arguments(action: &Option<crate::models::responses::WebSearchAction>) -> Value {
    match action {
        Some(crate::models::responses::WebSearchAction::Search { query, queries }) => {
            if let Some(query) = query {
                json!({ "query": query })
            } else if let Some(query) = queries.as_ref().and_then(|queries| queries.first()) {
                json!({ "query": query })
            } else {
                json!({})
            }
        }
        Some(crate::models::responses::WebSearchAction::OpenPage { url }) => {
            json!({ "url": url })
        }
        Some(crate::models::responses::WebSearchAction::FindInPage { url, pattern }) => {
            json!({ "url": url, "pattern": pattern })
        }
        Some(crate::models::responses::WebSearchAction::Other) | None => json!({}),
    }
}

fn web_search_placeholder_result(
    action: &Option<crate::models::responses::WebSearchAction>,
) -> String {
    match action {
        Some(crate::models::responses::WebSearchAction::Search { query, queries }) => {
            let query = query
                .clone()
                .or_else(|| queries.as_ref().and_then(|queries| queries.first().cloned()));
            match query {
                Some(query) => format!(
                    "Previous web_search completed in an earlier turn, but the original tool result is unavailable because replay state was missing. Query: {query}"
                ),
                None => "Previous web_search completed in an earlier turn, but the original tool result is unavailable because replay state was missing.".to_string(),
            }
        }
        Some(crate::models::responses::WebSearchAction::OpenPage { url }) => match url {
            Some(url) => format!(
                "Previous web_search open_page completed in an earlier turn, but the original tool result is unavailable because replay state was missing. URL: {url}"
            ),
            None => "Previous web_search open_page completed in an earlier turn, but the original tool result is unavailable because replay state was missing.".to_string(),
        },
        Some(crate::models::responses::WebSearchAction::FindInPage { url, pattern }) => {
            match (url, pattern) {
                (Some(url), Some(pattern)) => format!(
                    "Previous web_search find_in_page completed in an earlier turn, but the original tool result is unavailable because replay state was missing. URL: {url}. Pattern: {pattern}"
                ),
                (Some(url), None) => format!(
                    "Previous web_search find_in_page completed in an earlier turn, but the original tool result is unavailable because replay state was missing. URL: {url}"
                ),
                (None, Some(pattern)) => format!(
                    "Previous web_search find_in_page completed in an earlier turn, but the original tool result is unavailable because replay state was missing. Pattern: {pattern}"
                ),
                (None, None) => "Previous web_search find_in_page completed in an earlier turn, but the original tool result is unavailable because replay state was missing.".to_string(),
            }
        }
        Some(crate::models::responses::WebSearchAction::Other) | None => {
            "Previous web_search completed in an earlier turn, but the original tool result is unavailable because replay state was missing.".to_string()
        }
    }
}

fn reasoning_item_text(
    summary: &[crate::models::responses::ReasoningSummaryItem],
    content: &Option<Vec<crate::models::responses::ReasoningContentItem>>,
) -> String {
    let mut pieces = Vec::new();
    for entry in summary {
        let crate::models::responses::ReasoningSummaryItem::SummaryText { text } = entry;
        if !text.is_empty() {
            pieces.push(text.clone());
        }
    }
    if let Some(content) = content {
        for entry in content {
            match entry {
                crate::models::responses::ReasoningContentItem::ReasoningText { text }
                | crate::models::responses::ReasoningContentItem::Text { text }
                    if !text.is_empty() =>
                {
                    pieces.push(text.clone());
                }
                crate::models::responses::ReasoningContentItem::ReasoningText { .. }
                | crate::models::responses::ReasoningContentItem::Text { .. } => {}
            }
        }
    }
    pieces.join("\n")
}

fn message_content_to_chat_value(content: &[ContentItem]) -> AppResult<Value> {
    if content.is_empty() {
        return Ok(Value::String(String::new()));
    }
    if content.len() == 1 {
        return content_item_to_chat_value(&content[0]);
    }
    let mut parts = Vec::with_capacity(content.len());
    for item in content {
        parts.push(content_item_to_chat_part(item));
    }
    Ok(Value::Array(parts))
}

fn content_item_to_chat_value(item: &ContentItem) -> AppResult<Value> {
    match item {
        ContentItem::InputText { text } | ContentItem::OutputText { text } => {
            Ok(Value::String(text.clone()))
        }
        ContentItem::InputImage { .. } | ContentItem::InputFile { .. } | ContentItem::Other(_) => {
            Ok(Value::Array(vec![content_item_to_chat_part(item)]))
        }
    }
}

fn content_item_to_chat_part(item: &ContentItem) -> Value {
    match item {
        ContentItem::InputText { text } | ContentItem::OutputText { text } => json!({
            "type": "text",
            "text": text,
        }),
        ContentItem::InputImage {
            image_url: Some(image_url),
            detail,
            ..
        } => {
            let mut image_url_value = Map::new();
            image_url_value.insert("url".to_string(), Value::String(image_url.clone()));
            if let Some(detail) = detail {
                image_url_value.insert("detail".to_string(), Value::String(detail.clone()));
            }
            json!({
                "type": "image_url",
                "image_url": Value::Object(image_url_value)
            })
        }
        ContentItem::InputImage {
            image_url: None,
            file_id,
            detail,
        } => {
            let mut part = Map::new();
            part.insert("type".to_string(), Value::String("input_image".to_string()));
            if let Some(file_id) = file_id {
                part.insert("file_id".to_string(), Value::String(file_id.clone()));
            }
            if let Some(detail) = detail {
                part.insert("detail".to_string(), Value::String(detail.clone()));
            }
            Value::Object(part)
        }
        ContentItem::InputFile {
            file_id,
            file_url,
            filename,
            file_data,
        } => {
            let mut part = Map::new();
            part.insert("type".to_string(), Value::String("input_file".to_string()));
            insert_optional_string(&mut part, "file_id", file_id);
            insert_optional_string(&mut part, "file_url", file_url);
            insert_optional_string(&mut part, "filename", filename);
            insert_optional_string(&mut part, "file_data", file_data);
            Value::Object(part)
        }
        ContentItem::Other(value) => value.clone(),
    }
}

fn insert_optional_string(map: &mut Map<String, Value>, key: &str, value: &Option<String>) {
    if let Some(value) = value {
        map.insert(key.to_string(), Value::String(value.clone()));
    }
}

pub fn stringify_tool_output(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

pub fn tool_call_arguments_object(arguments: &Option<Value>) -> Value {
    match arguments {
        Some(Value::Object(map)) => Value::Object(map.clone()),
        Some(other) => other.clone(),
        None => Value::Object(Map::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::responses::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn base_test_request() -> ResponsesRequest {
        ResponsesRequest {
            model: "test".to_string(),
            instructions: String::new(),
            input: vec![],
            tools: vec![],
            tool_choice: serde_json::Value::String("auto".to_string()),
            parallel_tool_calls: false,
            reasoning: None,
            store: false,
            stream: true,
            include: vec![],
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

    fn user_msg(text: &str) -> ResponseItem {
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: text.to_string(),
            }],
            phase: None,
        }
    }

    #[test]
    fn validate_accepts_stream_false() {
        let mut req = base_test_request();
        req.stream = false;
        assert!(validate_request(&req).is_ok());
    }

    #[test]
    fn validate_rejects_previous_response_id() {
        let mut req = base_test_request();
        req.previous_response_id = Some("resp_123".to_string());
        assert!(validate_request(&req).is_err());
    }

    #[test]
    fn validate_accepts_all_tool_choice_values() {
        let req = base_test_request();
        assert!(validate_request(&req).is_ok());

        let mut req2 = base_test_request();
        req2.tool_choice = serde_json::Value::String("required".to_string());
        req2.tools = vec![ToolSpec::Function {
            name: "f".to_string(),
            description: "d".to_string(),
            strict: false,
            parameters: json!({}),
        }];
        assert!(validate_request(&req2).is_ok());

        let mut req3 = base_test_request();
        req3.tool_choice = serde_json::Value::String("none".to_string());
        assert!(validate_request(&req3).is_ok());

        let mut req4 = base_test_request();
        req4.tool_choice = json!({"type": "function", "function": {"name": "echo"}});
        req4.tools = vec![ToolSpec::Function {
            name: "echo".to_string(),
            description: "d".to_string(),
            strict: false,
            parameters: json!({}),
        }];
        assert!(validate_request(&req4).is_ok());
    }

    #[test]
    fn validate_accepts_image_generation_tool() {
        let mut req = base_test_request();
        req.tools = vec![ToolSpec::ImageGeneration {
            output_format: None,
        }];
        assert!(validate_request(&req).is_ok());
    }

    #[test]
    fn trailing_reasoning_flushed_as_message() {
        let mut req = base_test_request();
        req.input = vec![
            user_msg("hello"),
            ResponseItem::Reasoning {
                id: "rsn_1".to_string(),
                summary: vec![ReasoningSummaryItem::SummaryText {
                    text: "thinking".to_string(),
                }],
                content: None,
                encrypted_content: None,
            },
        ];
        let result = lower_request(&req, vec![]).unwrap();
        let last = result.messages.last().unwrap();
        assert_eq!(last.role, "assistant");
        assert!(last.reasoning_content.is_some());
        assert!(last.content.is_none());
    }

    #[test]
    fn signed_reasoning_history_preserves_chat_thinking_signature() {
        let mut req = base_test_request();
        req.input = vec![
            user_msg("hello"),
            ResponseItem::Reasoning {
                id: "rsn_1".to_string(),
                summary: Vec::new(),
                content: Some(vec![ReasoningContentItem::ReasoningText {
                    text: "private chain".to_string(),
                }]),
                encrypted_content: Some("sig_history".to_string()),
            },
            ResponseItem::Message {
                id: None,
                role: "assistant".to_string(),
                content: vec![ContentItem::OutputText {
                    text: "answer".to_string(),
                }],
                phase: None,
            },
        ];

        let result = lower_request(&req, vec![]).unwrap();
        let assistant = result
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .expect("assistant message");
        assert_eq!(
            assistant.reasoning_content.as_deref(),
            Some("private chain")
        );
        let thinking = assistant.thinking.as_ref().expect("signed thinking");
        assert_eq!(thinking.content, "private chain");
        assert_eq!(thinking.signature.as_deref(), Some("sig_history"));
    }

    #[test]
    fn normalizes_medium_reasoning_effort_for_upstream() {
        let mut req = base_test_request();
        req.reasoning = Some(ReasoningRequest {
            effort: Some("medium".to_string()),
            summary: None,
        });

        let result = lower_request(&req, vec![]).expect("lower_request");
        assert_eq!(result.reasoning_effort.as_deref(), Some("high"));
    }

    #[test]
    fn defaults_missing_reasoning_effort_to_max_for_upstream() {
        let req = base_test_request();

        let result = lower_request(&req, vec![]).expect("lower_request");
        assert_eq!(result.reasoning_effort.as_deref(), Some("max"));
    }

    #[test]
    fn preserves_max_reasoning_effort_for_upstream() {
        let mut req = base_test_request();
        req.reasoning = Some(ReasoningRequest {
            effort: Some("max".to_string()),
            summary: None,
        });

        let result = lower_request(&req, vec![]).expect("lower_request");
        assert_eq!(result.reasoning_effort.as_deref(), Some("max"));
    }

    #[test]
    fn normalizes_xhigh_reasoning_effort_to_max_for_upstream() {
        let mut req = base_test_request();
        req.reasoning = Some(ReasoningRequest {
            effort: Some("xhigh".to_string()),
            summary: None,
        });

        let result = lower_request(&req, vec![]).expect("lower_request");
        assert_eq!(result.reasoning_effort.as_deref(), Some("max"));
    }

    #[test]
    fn normalizes_low_reasoning_effort_to_high_for_upstream() {
        let mut req = base_test_request();
        req.reasoning = Some(ReasoningRequest {
            effort: Some("  LoW  ".to_string()),
            summary: None,
        });

        let result = lower_request(&req, vec![]).expect("lower_request");
        assert_eq!(result.reasoning_effort.as_deref(), Some("high"));
    }

    #[test]
    fn duplicate_tool_name_rejected() {
        let tools = vec![
            ToolSpec::Function {
                name: "echo".to_string(),
                description: "a".to_string(),
                strict: false,
                parameters: json!({}),
            },
            ToolSpec::Function {
                name: "echo".to_string(),
                description: "b".to_string(),
                strict: false,
                parameters: json!({}),
            },
        ];
        assert!(lower_tools(&tools).is_err());
    }

    #[test]
    fn mixed_text_and_image_content() {
        let content = vec![
            ContentItem::InputText {
                text: "hi".to_string(),
            },
            ContentItem::InputImage {
                image_url: Some("http://img.png".to_string()),
                file_id: None,
                detail: None,
            },
        ];
        let value = message_content_to_chat_value(&content).unwrap();
        assert!(value.is_array());
        let arr = value.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["type"], "text");
        assert_eq!(arr[1]["type"], "image_url");
        assert_eq!(arr[1]["image_url"]["url"], "http://img.png");
    }

    #[test]
    fn single_input_image_wraps_as_array() {
        let item = ContentItem::InputImage {
            image_url: Some("http://img.png".to_string()),
            file_id: None,
            detail: Some("high".to_string()),
        };
        let value = content_item_to_chat_value(&item).unwrap();
        assert!(value.is_array());
        assert_eq!(value[0]["type"], "image_url");
        assert_eq!(value[0]["image_url"]["url"], "http://img.png");
        assert_eq!(value[0]["image_url"]["detail"], "high");
    }

    #[test]
    fn input_file_passes_through_as_content_part() {
        let item = ContentItem::InputFile {
            file_id: Some("file_123".to_string()),
            file_url: None,
            filename: Some("brief.pdf".to_string()),
            file_data: None,
        };
        let value = content_item_to_chat_value(&item).unwrap();
        assert!(value.is_array());
        assert_eq!(value[0]["type"], "input_file");
        assert_eq!(value[0]["file_id"], "file_123");
        assert_eq!(value[0]["filename"], "brief.pdf");
    }

    #[test]
    fn web_search_arguments_all_actions() {
        let search = Some(WebSearchAction::Search {
            query: Some("test".to_string()),
            queries: None,
        });
        assert_eq!(web_search_arguments(&search), json!({"query": "test"}));

        let open = Some(WebSearchAction::OpenPage {
            url: Some("http://x.com".to_string()),
        });
        assert_eq!(web_search_arguments(&open), json!({"url": "http://x.com"}));

        let find = Some(WebSearchAction::FindInPage {
            url: Some("http://x.com".to_string()),
            pattern: Some("foo".to_string()),
        });
        assert_eq!(
            web_search_arguments(&find),
            json!({"url": "http://x.com", "pattern": "foo"})
        );

        assert_eq!(
            web_search_arguments(&Some(WebSearchAction::Other)),
            json!({})
        );
        assert_eq!(web_search_arguments(&None), json!({}));
    }

    #[test]
    fn web_search_placeholder_result_all_actions() {
        let search = Some(WebSearchAction::Search {
            query: Some("test".to_string()),
            queries: None,
        });
        assert!(web_search_placeholder_result(&search).contains("test"));

        let open = Some(WebSearchAction::OpenPage {
            url: Some("http://x.com".to_string()),
        });
        assert!(web_search_placeholder_result(&open).contains("http://x.com"));

        let find = Some(WebSearchAction::FindInPage {
            url: Some("http://x.com".to_string()),
            pattern: Some("foo".to_string()),
        });
        let result = web_search_placeholder_result(&find);
        assert!(result.contains("http://x.com"));
        assert!(result.contains("foo"));

        assert!(
            web_search_placeholder_result(&Some(WebSearchAction::Other))
                .contains("replay state was missing")
        );
        assert!(web_search_placeholder_result(&None).contains("replay state was missing"));
    }

    #[test]
    fn tool_search_call_non_client_error() {
        let mut req = base_test_request();
        req.input = vec![
            user_msg("hello"),
            ResponseItem::ToolSearchCall {
                call_id: Some("ts_1".to_string()),
                status: None,
                execution: "server".to_string(),
                arguments: json!({}),
            },
        ];
        let result = lower_request(&req, vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("execution=client"));
    }

    #[test]
    fn stringify_tool_output_non_string() {
        assert_eq!(stringify_tool_output(&json!(42)), "42");
        assert_eq!(stringify_tool_output(&json!(true)), "true");
        assert_eq!(stringify_tool_output(&json!({"a": 1})), r#"{"a":1}"#);
    }

    #[test]
    fn tool_call_arguments_object_edge_cases() {
        assert_eq!(tool_call_arguments_object(&None), json!({}));
        assert_eq!(tool_call_arguments_object(&Some(json!("x"))), json!("x"));
        assert_eq!(
            tool_call_arguments_object(&Some(json!({"a": 1}))),
            json!({"a": 1})
        );
    }

    #[test]
    fn hoist_system_messages_non_string_content() {
        let mut messages = vec![
            ChatMessage {
                role: "system".to_string(),
                content: Some(json!({"key": "value"})),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            },
            ChatMessage {
                role: "user".to_string(),
                content: Some(json!("hello")),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            },
        ];
        hoist_system_messages(&mut messages);
        assert_eq!(messages[0].role, "system");
        let content = messages[0].content.as_ref().unwrap().as_str().unwrap();
        assert!(content.contains("key"));
        assert!(content.contains("value"));
    }

    #[test]
    fn reasoning_item_text_variants() {
        let summary = vec![ReasoningSummaryItem::SummaryText {
            text: "summary".to_string(),
        }];
        let content = Some(vec![
            ReasoningContentItem::ReasoningText {
                text: "reasoning".to_string(),
            },
            ReasoningContentItem::Text {
                text: "text".to_string(),
            },
        ]);
        let result = reasoning_item_text(&summary, &content);
        assert!(result.contains("summary"));
        assert!(result.contains("reasoning"));
        assert!(result.contains("text"));
    }

    // --- C1 tests ---

    #[test]
    fn test_validate_tool_choice_valid_strings() {
        for val in &["auto", "none"] {
            let mut req = base_test_request();
            req.tool_choice = Value::String(val.to_string());
            assert!(validate_request(&req).is_ok(), "expected {val} to pass");
        }
        let mut req = base_test_request();
        req.tool_choice = Value::String("required".to_string());
        req.tools = vec![ToolSpec::Function {
            name: "f".to_string(),
            description: "d".to_string(),
            strict: false,
            parameters: json!({}),
        }];
        assert!(validate_request(&req).is_ok());
    }

    #[test]
    fn test_validate_tool_choice_valid_object() {
        let mut req = base_test_request();
        req.tool_choice = json!({"type": "function", "function": {"name": "foo"}});
        req.tools = vec![ToolSpec::Function {
            name: "foo".to_string(),
            description: "d".to_string(),
            strict: false,
            parameters: json!({}),
        }];
        assert!(validate_request(&req).is_ok());
    }

    #[test]
    fn test_validate_tool_choice_rejects_arbitrary_json() {
        let mut req = base_test_request();
        req.tool_choice = json!(42);
        assert!(validate_request(&req).is_err());

        let mut req2 = base_test_request();
        req2.tool_choice = json!([1, 2, 3]);
        assert!(validate_request(&req2).is_err());

        let mut req3 = base_test_request();
        req3.tool_choice = json!({"type": "unknown"});
        assert!(validate_request(&req3).is_err());

        let mut req4 = base_test_request();
        req4.tool_choice = Value::String("bogus".to_string());
        assert!(validate_request(&req4).is_err());
    }

    #[test]
    fn test_validate_tool_choice_required_without_tools_rejected() {
        let mut req = base_test_request();
        req.tool_choice = Value::String("required".to_string());
        req.tools = vec![];
        assert!(validate_request(&req).is_err());
    }

    // --- M1+M4 tests ---

    #[test]
    fn test_append_tool_call_sequential_indices() {
        let mut messages: Vec<ChatMessage> = vec![];
        for i in 0..3 {
            append_tool_call(
                &mut messages,
                format!("call_{i}"),
                format!("fn_{i}"),
                json!({}),
                None,
            );
        }
        assert_eq!(messages.len(), 1);
        let calls = messages[0].tool_calls.as_ref().unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].index, Some(0));
        assert_eq!(calls[1].index, Some(1));
        assert_eq!(calls[2].index, Some(2));
    }

    #[test]
    fn test_append_tool_call_no_merge_into_content_message() {
        let mut messages = vec![ChatMessage {
            role: "assistant".to_string(),
            content: Some(Value::String("some text".to_string())),
            tool_call_id: None,
            name: None,
            reasoning_content: None,
            thinking: None,
            tool_calls: None,
        }];
        append_tool_call(
            &mut messages,
            "call_1".to_string(),
            "fn_1".to_string(),
            json!({}),
            None,
        );
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[0].content,
            Some(Value::String("some text".to_string()))
        );
        assert!(messages[0].tool_calls.is_none());
        assert!(messages[1].tool_calls.is_some());
        assert_eq!(messages[1].tool_calls.as_ref().unwrap()[0].index, Some(0));
    }

    // --- M2 test ---

    #[test]
    fn test_hoist_preserves_mid_conversation_system_messages() {
        let mut messages = vec![
            ChatMessage {
                role: "system".to_string(),
                content: Some(Value::String("top".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            },
            ChatMessage {
                role: "user".to_string(),
                content: Some(Value::String("hello".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            },
            ChatMessage {
                role: "system".to_string(),
                content: Some(Value::String("mid".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            },
            ChatMessage {
                role: "assistant".to_string(),
                content: Some(Value::String("hi".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            },
        ];
        hoist_system_messages(&mut messages);
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].role, "system");
        assert_eq!(
            messages[0].content.as_ref().unwrap().as_str().unwrap(),
            "top"
        );
        assert_eq!(messages[1].role, "user");
        assert_eq!(messages[2].role, "system");
        assert_eq!(
            messages[2].content.as_ref().unwrap().as_str().unwrap(),
            "mid"
        );
        assert_eq!(messages[3].role, "assistant");
    }
}
