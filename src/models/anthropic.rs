use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::json;

// ---------------------------------------------------------------------------
// Default helpers
// ---------------------------------------------------------------------------

fn default_input_schema() -> Value {
    json!({"type": "object"})
}

fn default_object() -> Value {
    json!({})
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicRequest {
    #[serde(default, deserialize_with = "crate::models::chat::deserialize_model")]
    pub model: String,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub system: Option<AnthropicSystemContent>,
    pub messages: Vec<AnthropicMessage>,
    #[serde(default)]
    pub tools: Option<Vec<AnthropicTool>>,
    #[serde(default)]
    pub tool_choice: Option<AnthropicToolChoice>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub top_k: Option<u32>,
    #[serde(default)]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(default)]
    pub metadata: Option<Value>,
    #[serde(default)]
    pub thinking: Option<AnthropicThinking>,
    #[serde(default)]
    pub output_config: Option<Value>,
}

#[derive(Debug, Clone)]
pub enum AnthropicSystemContent {
    Text(String),
    Blocks(Vec<AnthropicTextBlock>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicTextBlock {
    Text { text: String },
}

#[derive(Debug, Clone)]
pub enum AnthropicContent {
    Text(String),
    Blocks(Vec<AnthropicContentBlock>),
}

#[derive(Debug, Clone)]
pub enum AnthropicContentBlock {
    Text {
        text: String,
    },
    Image {
        source: AnthropicImageSource,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    Thinking {
        thinking: String,
        signature: Option<String>,
    },
    RedactedThinking {
        data: String,
    },
    ToolResult {
        tool_use_id: String,
        content: Option<AnthropicContent>,
        is_error: Option<bool>,
    },
    ServerToolUse {
        id: String,
        name: String,
        input: Value,
    },
    WebSearchToolResult {
        tool_use_id: String,
        content: Value,
    },
    Other(Value),
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicImageSource {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub media_type: Option<String>,
    #[serde(default)]
    pub data: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub file_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicMessage {
    pub role: String,
    pub content: AnthropicContent,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicTool {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_input_schema")]
    pub input_schema: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicToolChoice {
    Auto,
    Any,
    None,
    Tool { name: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicThinking {
    Enabled { budget_tokens: Option<u64> },
    Adaptive { budget_tokens: Option<u64> },
    Disabled,
}

// Custom Deserialize for AnthropicSystemContent (string or array)
impl<'de> Deserialize<'de> for AnthropicSystemContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match &value {
            Value::String(text) => Ok(Self::Text(text.clone())),
            Value::Array(_) => serde_json::from_value::<Vec<AnthropicTextBlock>>(value)
                .map(Self::Blocks)
                .map_err(serde::de::Error::custom),
            Value::Null => Ok(Self::Text(String::new())),
            _ => Err(serde::de::Error::custom(
                "expected string or array for system content",
            )),
        }
    }
}

// Custom Deserialize for AnthropicContent (string or array)
impl<'de> Deserialize<'de> for AnthropicContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match &value {
            Value::String(text) => Ok(Self::Text(text.clone())),
            Value::Array(_) => serde_json::from_value::<Vec<AnthropicContentBlock>>(value)
                .map(Self::Blocks)
                .map_err(serde::de::Error::custom),
            Value::Null => Ok(Self::Text(String::new())),
            _ => Err(serde::de::Error::custom(
                "expected string or array for message content",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for AnthropicContentBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            return Ok(Self::Other(value));
        };

        match kind {
            "text" => {
                #[derive(Deserialize)]
                struct TextBlock {
                    text: String,
                }

                parse_block(value).map(|block: TextBlock| Self::Text { text: block.text })
            }
            "image" => {
                #[derive(Deserialize)]
                struct ImageBlock {
                    source: AnthropicImageSource,
                }

                parse_block(value).map(|block: ImageBlock| Self::Image {
                    source: block.source,
                })
            }
            "tool_use" => {
                #[derive(Deserialize)]
                struct ToolUseBlock {
                    id: String,
                    name: String,
                    input: Value,
                }

                parse_block(value).map(|block: ToolUseBlock| Self::ToolUse {
                    id: block.id,
                    name: block.name,
                    input: block.input,
                })
            }
            "thinking" => {
                #[derive(Deserialize)]
                struct ThinkingBlock {
                    thinking: String,
                    #[serde(default)]
                    signature: Option<String>,
                }

                parse_block(value).map(|block: ThinkingBlock| Self::Thinking {
                    thinking: block.thinking,
                    signature: block.signature,
                })
            }
            "redacted_thinking" => {
                #[derive(Deserialize)]
                struct RedactedThinkingBlock {
                    data: String,
                }

                parse_block(value)
                    .map(|block: RedactedThinkingBlock| Self::RedactedThinking { data: block.data })
            }
            "tool_result" => {
                #[derive(Deserialize)]
                struct ToolResultBlock {
                    tool_use_id: String,
                    #[serde(default)]
                    content: Option<AnthropicContent>,
                    #[serde(default)]
                    is_error: Option<bool>,
                }

                parse_block(value).map(|block: ToolResultBlock| Self::ToolResult {
                    tool_use_id: block.tool_use_id,
                    content: block.content,
                    is_error: block.is_error,
                })
            }
            "server_tool_use" => {
                #[derive(Deserialize)]
                struct ServerToolUseBlock {
                    id: String,
                    name: String,
                    #[serde(default = "default_object")]
                    input: Value,
                }

                parse_block(value).map(|block: ServerToolUseBlock| Self::ServerToolUse {
                    id: block.id,
                    name: block.name,
                    input: block.input,
                })
            }
            "web_search_tool_result" => {
                #[derive(Deserialize)]
                struct WebSearchToolResultBlock {
                    tool_use_id: String,
                    #[serde(default)]
                    content: Value,
                }

                parse_block(value).map(|block: WebSearchToolResultBlock| {
                    Self::WebSearchToolResult {
                        tool_use_id: block.tool_use_id,
                        content: block.content,
                    }
                })
            }
            _ => Ok(Self::Other(value)),
        }
    }
}

fn parse_block<T, E>(value: Value) -> Result<T, E>
where
    T: DeserializeOwned,
    E: serde::de::Error,
{
    serde_json::from_value(value).map_err(E::custom)
}

// ---------------------------------------------------------------------------
// Streaming response types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicStreamEvent {
    MessageStart {
        message: AnthropicMessageStart,
    },
    ContentBlockStart {
        index: usize,
        content_block: AnthropicContentBlockStart,
    },
    ContentBlockDelta {
        index: usize,
        delta: AnthropicDelta,
    },
    ContentBlockStop {
        index: usize,
    },
    MessageDelta {
        delta: AnthropicMessageDeltaBody,
        usage: AnthropicUsage,
    },
    MessageStop,
    Ping,
    Error {
        error: AnthropicErrorBody,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct AnthropicMessageStart {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub role: String,
    pub content: Vec<Value>,
    pub model: String,
    pub stop_reason: Option<Value>,
    pub stop_sequence: Option<Value>,
    pub usage: AnthropicUsage,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicContentBlockStart {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    Thinking {
        thinking: String,
    },
    /// Anthropic server-side tool invocation (e.g. server-executed web search).
    /// Streamed without `input`; the query arrives via `input_json_delta`.
    ServerToolUse {
        id: String,
        name: String,
    },
    /// Results of a server-side web search, surfaced so clients count the
    /// search and render source citations.
    WebSearchToolResult {
        tool_use_id: String,
        content: Value,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicDelta {
    TextDelta { text: String },
    InputJsonDelta { partial_json: String },
    ThinkingDelta { thinking: String },
    SignatureDelta { signature: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct AnthropicMessageDeltaBody {
    pub stop_reason: Option<String>,
    pub stop_sequence: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AnthropicUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens_details: Option<AnthropicOutputTokensDetails>,
    /// Server-executed tool usage. Claude Code's "Did N searches" indicator
    /// reads `server_tool_use.web_search_requests`; omitted when no server-side
    /// search ran so token-only turns stay byte-identical.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_tool_use: Option<AnthropicServerToolUse>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AnthropicOutputTokensDetails {
    pub thinking_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AnthropicServerToolUse {
    pub web_search_requests: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AnthropicErrorBody {
    #[serde(rename = "type")]
    pub kind: String,
    pub message: String,
}

impl AnthropicStreamEvent {
    pub fn sse_event_type(&self) -> &'static str {
        match self {
            Self::MessageStart { .. } => "message_start",
            Self::ContentBlockStart { .. } => "content_block_start",
            Self::ContentBlockDelta { .. } => "content_block_delta",
            Self::ContentBlockStop { .. } => "content_block_stop",
            Self::MessageDelta { .. } => "message_delta",
            Self::MessageStop => "message_stop",
            Self::Ping => "ping",
            Self::Error { .. } => "error",
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}

// ---------------------------------------------------------------------------
// Non-streaming response types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct AnthropicMessageResponse {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub role: String,
    pub content: Vec<AnthropicResponseContentBlock>,
    pub model: String,
    pub stop_reason: String,
    pub stop_sequence: Option<Value>,
    pub usage: AnthropicMessageUsage,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicResponseContentBlock {
    Text {
        text: String,
    },
    Thinking {
        thinking: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ServerToolUse {
        id: String,
        name: String,
        input: Value,
    },
    WebSearchToolResult {
        tool_use_id: String,
        content: Value,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct AnthropicMessageUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens_details: Option<AnthropicOutputTokensDetails>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_without_input_schema_gets_default() {
        let value = serde_json::json!({"name": "web_search"});
        let tool: AnthropicTool = serde_json::from_value(value).unwrap();
        assert_eq!(tool.name, "web_search");
        assert_eq!(tool.input_schema, serde_json::json!({"type": "object"}));
    }

    #[test]
    fn tool_with_input_schema_preserves_value() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "query": { "type": "string" } }
        });
        let value = serde_json::json!({"name": "search", "input_schema": schema});
        let tool: AnthropicTool = serde_json::from_value(value).unwrap();
        assert_eq!(tool.name, "search");
        assert_eq!(tool.input_schema, schema);
    }
}
