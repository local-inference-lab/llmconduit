use serde::Deserialize;
use serde::Serialize;
use serde::ser::SerializeMap;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;

fn deserialize_input<'de, D>(deserializer: D) -> Result<Vec<ResponseItem>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum InputHelper {
        Bare(String),
        Items(Vec<ResponseItem>),
    }
    let helper = InputHelper::deserialize(deserializer)?;
    Ok(match helper {
        InputHelper::Bare(text) => vec![ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText { text }],
            phase: None,
        }],
        InputHelper::Items(items) => items,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesRequest {
    #[serde(default, deserialize_with = "crate::models::chat::deserialize_model")]
    pub model: String,
    #[serde(default)]
    pub instructions: String,
    #[serde(default, deserialize_with = "deserialize_input")]
    pub input: Vec<ResponseItem>,
    #[serde(default)]
    pub tools: Vec<ToolSpec>,
    #[serde(default = "default_tool_choice")]
    pub tool_choice: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub reasoning: Option<ReasoningRequest>,
    /// Request-path thinking override resolved from an Anthropic model profile.
    /// This is internal gateway state: native Responses/Chat clients continue to
    /// control backend-specific thinking kwargs through their normal fields.
    #[serde(skip)]
    pub thinking: Option<bool>,
    #[serde(default = "default_store_true")]
    pub store: bool,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub prompt_cache_key: Option<String>,
    #[serde(default)]
    pub text: Option<TextControls>,
    #[serde(default)]
    pub client_metadata: Option<HashMap<String, String>>,
    #[serde(default)]
    pub previous_response_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,
    #[serde(default)]
    pub truncation: Option<Value>,
    #[serde(default)]
    pub metadata: Option<HashMap<String, Value>>,
    #[serde(
        default,
        deserialize_with = "crate::models::chat::deserialize_opt_stop",
        skip_serializing_if = "Option::is_none"
    )]
    pub stop: Option<Vec<String>>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_body: BTreeMap<String, Value>,
}

fn default_tool_choice() -> Value {
    Value::String("auto".to_string())
}

fn default_store_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolSpec {
    Function {
        name: String,
        description: String,
        #[serde(default)]
        strict: bool,
        parameters: Value,
    },
    Namespace {
        name: String,
        description: String,
        tools: Vec<NamespaceToolSpec>,
    },
    ToolSearch {
        execution: String,
        description: String,
        parameters: Value,
    },
    LocalShell {},
    WebSearch {
        #[serde(default)]
        external_web_access: Option<bool>,
        #[serde(default)]
        filters: Option<Value>,
        #[serde(default)]
        user_location: Option<Value>,
        #[serde(default)]
        search_context_size: Option<String>,
        #[serde(default)]
        search_content_types: Option<Vec<String>>,
    },
    Custom {
        name: String,
        description: String,
        format: CustomToolFormat,
    },
    ImageGeneration {
        #[serde(default)]
        output_format: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NamespaceToolSpec {
    Function {
        name: String,
        description: String,
        #[serde(default)]
        strict: bool,
        parameters: Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomToolFormat {
    #[serde(rename = "type")]
    pub kind: String,
    pub syntax: String,
    pub definition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReasoningRequest {
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextControls {
    #[serde(default)]
    pub verbosity: Option<String>,
    #[serde(default)]
    pub format: Option<TextFormat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextFormat {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub strict: bool,
    pub schema: Value,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContentItem {
    InputText {
        text: String,
    },
    InputImage {
        image_url: Option<String>,
        file_id: Option<String>,
        detail: Option<String>,
    },
    InputFile {
        file_id: Option<String>,
        file_url: Option<String>,
        filename: Option<String>,
        file_data: Option<String>,
    },
    OutputText {
        text: String,
    },
    Other(Value),
}

impl Serialize for ContentItem {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ContentItem::InputText { text } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "input_text")?;
                map.serialize_entry("text", text)?;
                map.end()
            }
            ContentItem::InputImage {
                image_url,
                file_id,
                detail,
            } => {
                let len = 1
                    + usize::from(image_url.is_some())
                    + usize::from(file_id.is_some())
                    + usize::from(detail.is_some());
                let mut map = serializer.serialize_map(Some(len))?;
                map.serialize_entry("type", "input_image")?;
                if let Some(image_url) = image_url {
                    map.serialize_entry("image_url", image_url)?;
                }
                if let Some(file_id) = file_id {
                    map.serialize_entry("file_id", file_id)?;
                }
                if let Some(detail) = detail {
                    map.serialize_entry("detail", detail)?;
                }
                map.end()
            }
            ContentItem::InputFile {
                file_id,
                file_url,
                filename,
                file_data,
            } => {
                let len = 1
                    + usize::from(file_id.is_some())
                    + usize::from(file_url.is_some())
                    + usize::from(filename.is_some())
                    + usize::from(file_data.is_some());
                let mut map = serializer.serialize_map(Some(len))?;
                map.serialize_entry("type", "input_file")?;
                if let Some(file_id) = file_id {
                    map.serialize_entry("file_id", file_id)?;
                }
                if let Some(file_url) = file_url {
                    map.serialize_entry("file_url", file_url)?;
                }
                if let Some(filename) = filename {
                    map.serialize_entry("filename", filename)?;
                }
                if let Some(file_data) = file_data {
                    map.serialize_entry("file_data", file_data)?;
                }
                map.end()
            }
            ContentItem::OutputText { text } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "output_text")?;
                map.serialize_entry("text", text)?;
                map.end()
            }
            ContentItem::Other(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ContentItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let Some(object) = value.as_object() else {
            return Ok(ContentItem::Other(value));
        };
        Ok(match object.get("type").and_then(Value::as_str) {
            Some("input_text") => ContentItem::InputText {
                text: optional_string(object, "text").unwrap_or_default(),
            },
            Some("input_image") => ContentItem::InputImage {
                image_url: optional_image_url(object),
                file_id: optional_string(object, "file_id"),
                detail: optional_string(object, "detail"),
            },
            Some("input_file") => ContentItem::InputFile {
                file_id: optional_string(object, "file_id"),
                file_url: optional_string(object, "file_url"),
                filename: optional_string(object, "filename"),
                file_data: optional_string(object, "file_data"),
            },
            Some("output_text") => ContentItem::OutputText {
                text: optional_string(object, "text").unwrap_or_default(),
            },
            _ => ContentItem::Other(value),
        })
    }
}

fn optional_string(object: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn optional_image_url(object: &serde_json::Map<String, Value>) -> Option<String> {
    object.get("image_url").and_then(|value| match value {
        Value::String(url) => Some(url.clone()),
        Value::Object(map) => map
            .get("url")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        _ => None,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReasoningSummaryItem {
    SummaryText { text: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReasoningContentItem {
    ReasoningText { text: String },
    Text { text: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseItem {
    Message {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        role: String,
        content: Vec<ContentItem>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        phase: Option<String>,
    },
    Reasoning {
        #[serde(default = "default_reasoning_id")]
        id: String,
        #[serde(default)]
        summary: Vec<ReasoningSummaryItem>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<Vec<ReasoningContentItem>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_content: Option<String>,
    },
    FunctionCall {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
        arguments: String,
        call_id: String,
    },
    FunctionCallOutput {
        call_id: String,
        output: Value,
    },
    CustomToolCall {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        call_id: String,
        name: String,
        input: String,
    },
    CustomToolCallOutput {
        call_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        output: Value,
    },
    ToolSearchCall {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        execution: String,
        arguments: Value,
    },
    ToolSearchOutput {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        status: String,
        execution: String,
        tools: Vec<Value>,
    },
    LocalShellCall {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        status: String,
        action: LocalShellAction,
    },
    WebSearchCall {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        action: Option<WebSearchAction>,
    },
    ImageGenerationCall {
        id: String,
        status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        revised_prompt: Option<String>,
        result: String,
    },
}

fn default_reasoning_id() -> String {
    "rsn_pending".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LocalShellAction {
    Exec(LocalShellExecAction),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalShellExecAction {
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_directory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WebSearchAction {
    Search {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        queries: Option<Vec<String>>,
    },
    OpenPage {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
    },
    FindInPage {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pattern: Option<String>,
    },
    Other,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponsesEnvelope<T> {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(flatten)]
    pub payload: T,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseCreatedPayload {
    pub response: ResponseStub,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseStub {
    pub id: String,
    /// An EARLY, coarse ESTIMATE of prompt input tokens
    /// (`engine::estimate_input_tokens`,
    /// ~4 bytes/token over the lowered upstream payload), threaded onto
    /// `response.created` ONLY -- never `response.in_progress`, which reuses
    /// this same struct via `ResponseCreatedPayload` -- so the Anthropic
    /// streaming converter can seed `message_start.usage.input_tokens` with a
    /// plausible non-zero value instead of a hardcoded `0`. The REAL upstream
    /// tokenizer count is not known this early; it arrives later on
    /// `response.completed`'s `usage` and always overrides this estimate at
    /// the terminal event. `skip_serializing_if` keeps the OpenAI/Responses
    /// wire shape byte-unchanged for any consumer that never reads this
    /// additive, canonical-protocol-internal field.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub estimated_input_tokens: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseCompletedPayload {
    pub response: ResponseResource,
}

/// Typed terminal reason for a canonical response (T7). The engine sets this
/// from the upstream `finish_reason` at terminal emission; G8 reasoning-
/// promotion gating reads it (`Stop` ⇒ clean stop ⇒ may promote reasoning to
/// text) instead of string-matching the event type (`response.completed` vs
/// `response.incomplete`), so a future non-stop terminal reason arriving as
/// `response.completed` can no longer wrongly promote.
///
/// Serialized as a kebab/snake string on the wire (`"stop"`, `"length"`,
/// `"tool_calls"`, `"content_filter"`, `"other"`). Unknown upstream finish
/// reasons map to `Other` (non-clean ⇒ never promote).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalReason {
    /// `finish_reason: stop` — a clean completion. The ONLY reason that permits
    /// promoting a reasoning-only turn to a `text` block (G8).
    Stop,
    /// `finish_reason: length` — hit the output-token cap (`response.incomplete`,
    /// `incomplete_details.reason: max_output_tokens`). Never promote.
    Length,
    /// `finish_reason: tool_calls` — the turn ended on a tool-call batch. Never
    /// promote (the reasoning prefaced tools, not a final answer). Serialized as
    /// `tool_calls` (matching the upstream finish_reason vocabulary), NOT
    /// `tool_call` (which `rename_all = "snake_case"` would produce).
    #[serde(rename = "tool_calls")]
    ToolCall,
    /// `finish_reason: content_filter` — upstream content filter blocked output.
    /// Never promote.
    ContentFilter,
    /// Any other / unknown finish reason. Never promote (conservative: a reason
    /// the gateway does not recognize is not a clean stop).
    Other,
}

impl TerminalReason {
    /// Whether this is a CLEAN STOP — the only terminal reason that permits G8
    /// reasoning-promotion to a `text` block.
    pub fn is_clean_stop(self) -> bool {
        matches!(self, TerminalReason::Stop)
    }

    /// Map an upstream `finish_reason` string to a typed terminal reason.
    /// Unknown/blank ⇒ `Other` (non-clean).
    pub fn from_finish_reason(finish_reason: Option<&str>) -> Self {
        match finish_reason {
            Some("stop") => TerminalReason::Stop,
            Some("length") => TerminalReason::Length,
            Some("tool_calls") => TerminalReason::ToolCall,
            Some("content_filter") => TerminalReason::ContentFilter,
            _ => TerminalReason::Other,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseResource {
    pub id: String,
    pub object: String,
    pub created_at: i64,
    pub status: String,
    pub output: Vec<ResponseItem>,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<ResponseUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incomplete_details: Option<IncompleteDetails>,
    /// Matched upstream stop string, when vLLM reports one. This internal
    /// extension lets the Anthropic adapter distinguish a configured stop
    /// sequence from a natural end-of-sequence completion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequence: Option<String>,
    /// Typed terminal reason (T7). `None` only on non-terminal `response`
    /// resources (e.g. `response.created`); the terminal `response.completed` /
    /// `response.incomplete` event always carries it so the Anthropic converter
    /// can gate reasoning-promotion on `reason.is_clean_stop()` rather than the
    /// event type string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<TerminalReason>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IncompleteDetails {
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseUsage {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_details: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens_details: Option<ResponseInputTokensDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens_details: Option<ResponseOutputTokensDetails>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseInputTokensDetails {
    pub cached_tokens: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseOutputTokensDetails {
    pub reasoning_tokens: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputItemPayload {
    pub output_index: usize,
    pub item: ResponseItem,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeltaPayload {
    pub item_id: String,
    pub output_index: usize,
    pub content_index: usize,
    pub delta: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReasoningDeltaPayload {
    pub item_id: String,
    pub output_index: usize,
    pub summary_index: usize,
    pub delta: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReasoningSignatureDeltaPayload {
    pub item_id: String,
    pub output_index: usize,
    pub summary_index: usize,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedPayload {
    pub response: FailedResponse,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedResponse {
    pub error: FailedError,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TextDonePayload {
    pub item_id: String,
    pub output_index: usize,
    pub content_index: usize,
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FunctionCallArgsDeltaPayload {
    pub call_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub delta: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FunctionCallArgsDonePayload {
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RefusalDeltaPayload {
    pub delta: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RefusalDonePayload {
    pub refusal: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContentPartPayload {
    pub item_id: String,
    pub output_index: usize,
    pub content_index: usize,
    pub part: ContentPartRef,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContentPartRef {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
    pub annotations: Vec<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReasoningSummaryPartPayload {
    pub item_id: String,
    pub output_index: usize,
    pub summary_index: usize,
    pub part: ReasoningSummaryPartRef,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReasoningSummaryPartRef {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
}

pub enum TerminalStatus {
    Completed,
    Incomplete { reason: String },
}

impl ResponseItem {
    pub fn message_text(role: impl Into<String>, text: impl Into<String>) -> Self {
        Self::Message {
            id: None,
            role: role.into(),
            content: vec![ContentItem::OutputText { text: text.into() }],
            phase: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn terminal_reason_wire_strings_are_pinned() {
        // Load-bearing wire contract: the engine serializes `TerminalReason` onto
        // the terminal resource and the Anthropic converter re-reads these exact
        // strings via `from_finish_reason`. The `ToolCall => "tool_calls"` case is
        // the critical one — without `#[serde(rename = "tool_calls")]` snake_case
        // would emit `"tool_call"`, silently degrading the converter to `Other`.
        for (variant, expected) in [
            (TerminalReason::Stop, "stop"),
            (TerminalReason::Length, "length"),
            (TerminalReason::ToolCall, "tool_calls"),
            (TerminalReason::ContentFilter, "content_filter"),
            (TerminalReason::Other, "other"),
        ] {
            assert_eq!(
                serde_json::to_value(variant).unwrap(),
                serde_json::Value::String(expected.to_string()),
                "wire string for {variant:?}",
            );
            // Belt-and-braces: producer spelling round-trips through the canonical
            // mapper, so producer + consumer agree on every variant.
            assert_eq!(
                TerminalReason::from_finish_reason(Some(expected)),
                variant,
                "round-trip for {variant:?}",
            );
        }
    }

    /// C3 (AGENTS.md: no new wire field without a round-trip test): `ResponseStub`
    /// gained `estimated_input_tokens` (the early G3 estimate the engine threads
    /// onto `response.created` so the Anthropic converter can seed a non-zero
    /// `message_start.usage.input_tokens`). PRESENT survives a
    /// serialize -> deserialize round-trip; ABSENT (`None`) is OMITTED from the
    /// wire entirely (not serialized as `null`), so the OpenAI/Responses shape
    /// stays byte-unchanged for any caller that never reads this field.
    #[test]
    fn response_stub_estimated_input_tokens_round_trips_present_and_absent() {
        let present = ResponseStub {
            id: "resp_123".to_string(),
            estimated_input_tokens: Some(20),
        };
        let value = serde_json::to_value(&present).expect("serialize present");
        assert_eq!(value["estimated_input_tokens"], serde_json::json!(20));
        let roundtripped: ResponseStub =
            serde_json::from_value(value).expect("deserialize present");
        assert_eq!(present, roundtripped);

        let absent = ResponseStub {
            id: "resp_456".to_string(),
            estimated_input_tokens: None,
        };
        let value = serde_json::to_value(&absent).expect("serialize absent");
        assert!(
            !value
                .as_object()
                .unwrap()
                .contains_key("estimated_input_tokens"),
            "None must be OMITTED, not serialized as null: {value}"
        );
        let roundtripped: ResponseStub = serde_json::from_value(value).expect("deserialize absent");
        assert_eq!(absent, roundtripped);
    }

    #[test]
    fn response_item_message_serde_roundtrip() {
        let item = ResponseItem::Message {
            id: Some("msg_1".to_string()),
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: "hello".to_string(),
            }],
            phase: None,
        };
        let json = serde_json::to_string(&item).unwrap();
        let roundtripped: ResponseItem = serde_json::from_str(&json).unwrap();
        assert_eq!(item, roundtripped);
    }

    #[test]
    fn content_item_multimodal_serde_roundtrip() {
        let items = vec![
            ContentItem::InputImage {
                image_url: None,
                file_id: Some("file_img".to_string()),
                detail: Some("high".to_string()),
            },
            ContentItem::InputFile {
                file_id: Some("file_doc".to_string()),
                file_url: None,
                filename: Some("brief.pdf".to_string()),
                file_data: None,
            },
            ContentItem::Other(serde_json::json!({
                "type": "input_audio",
                "input_audio": {
                    "data": "abc",
                    "format": "wav"
                }
            })),
        ];
        let json = serde_json::to_string(&items).unwrap();
        let roundtripped: Vec<ContentItem> = serde_json::from_str(&json).unwrap();
        assert_eq!(items, roundtripped);
    }

    #[test]
    fn response_item_function_call_serde_roundtrip() {
        let item = ResponseItem::FunctionCall {
            id: Some("fc_1".to_string()),
            name: "calculator".to_string(),
            namespace: Some("mcp__math".to_string()),
            arguments: r#"{"expr":"1+1"}"#.to_string(),
            call_id: "call_1".to_string(),
        };
        let json = serde_json::to_string(&item).unwrap();
        let roundtripped: ResponseItem = serde_json::from_str(&json).unwrap();
        assert_eq!(item, roundtripped);
    }

    #[test]
    fn test_store_defaults_to_true() {
        let json = r#"{"model":"gpt-4","input":[]}"#;
        let req: ResponsesRequest = serde_json::from_str(json).unwrap();
        assert!(req.store, "store should default to true");
    }

    #[test]
    fn test_store_explicit_false() {
        let json = r#"{"model":"gpt-4","input":[],"store":false}"#;
        let req: ResponsesRequest = serde_json::from_str(json).unwrap();
        assert!(!req.store, "store should be false when explicitly set");
    }

    #[test]
    fn test_input_bare_string_deserializes() {
        let json = r#"{"model":"gpt-4","input":"hello","stream":true}"#;
        let req: ResponsesRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.input.len(), 1);
        match &req.input[0] {
            ResponseItem::Message { role, content, .. } => {
                assert_eq!(role, "user");
                assert_eq!(content.len(), 1);
                match &content[0] {
                    ContentItem::InputText { text } => assert_eq!(text, "hello"),
                    other => panic!("expected InputText, got {:?}", other),
                }
            }
            other => panic!("expected Message, got {:?}", other),
        }
    }

    #[test]
    fn test_penalties_roundtrip() {
        let json = r#"{"model":"gpt-4","input":[],"frequency_penalty":0.5,"presence_penalty":0.3}"#;
        let req: ResponsesRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.frequency_penalty, Some(0.5));
        assert_eq!(req.presence_penalty, Some(0.3));
    }

    #[test]
    fn response_item_web_search_call_serde_roundtrip() {
        let item = ResponseItem::WebSearchCall {
            id: Some("ws_1".to_string()),
            status: Some("completed".to_string()),
            action: Some(WebSearchAction::Search {
                query: Some("rust async".to_string()),
                queries: None,
            }),
        };
        let json = serde_json::to_string(&item).unwrap();
        let roundtripped: ResponseItem = serde_json::from_str(&json).unwrap();
        assert_eq!(item, roundtripped);
    }
}
