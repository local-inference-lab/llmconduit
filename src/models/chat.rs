use crate::error::AppError;
use crate::error::AppResult;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatCompletionRequest {
    #[serde(default, deserialize_with = "deserialize_model")]
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ChatTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<StreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(
        rename = "max_tokens",
        alias = "max_output_tokens",
        alias = "max_completion_tokens",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_output_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,
    #[serde(
        default,
        deserialize_with = "deserialize_opt_stop",
        skip_serializing_if = "Option::is_none"
    )]
    pub stop: Option<Vec<String>>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_body: BTreeMap<String, Value>,
}

pub(crate) fn deserialize_model<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?
        .map(|model| model.trim().to_string())
        .unwrap_or_default())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrVec {
    One(String),
    Many(Vec<String>),
}

/// OpenAI's `stop` is `string | array | null`; accept all three into `Vec<String>`.
pub(crate) fn deserialize_opt_stop<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(
        Option::<StringOrVec>::deserialize(deserializer)?.map(|value| match value {
            StringOrVec::One(single) => vec![single],
            StringOrVec::Many(many) => many,
        }),
    )
}

pub(crate) const OPENAI_MAX_STOP_SEQUENCES: usize = 4;

/// Drop empty sequences and reject more than OpenAI's documented maximum (it 400s, not truncates).
pub(crate) fn normalize_stop(stop: Option<Vec<String>>) -> AppResult<Option<Vec<String>>> {
    let sequences: Vec<String> = match stop {
        None => return Ok(None),
        Some(values) => values
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect(),
    };
    if sequences.is_empty() {
        return Ok(None);
    }
    if sequences.len() > OPENAI_MAX_STOP_SEQUENCES {
        return Err(AppError::bad_request(format!(
            "stop supports at most {OPENAI_MAX_STOP_SEQUENCES} sequences, got {}",
            sequences.len()
        )));
    }
    Ok(Some(sequences))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamOptions {
    #[serde(default)]
    pub include_usage: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ChatThinking>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatToolCall>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatThinking {
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatTool {
    #[serde(rename = "type")]
    pub kind: String,
    pub function: ChatToolDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatToolDefinition {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    #[serde(default)]
    pub strict: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatToolCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<usize>,
    #[serde(
        rename = "type",
        default = "default_chat_tool_call_kind",
        deserialize_with = "deserialize_chat_tool_call_kind"
    )]
    pub kind: String,
    #[serde(default)]
    pub function: ChatFunctionCall,
}

fn default_chat_tool_call_kind() -> String {
    "function".to_string()
}

fn deserialize_chat_tool_call_kind<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_else(default_chat_tool_call_kind))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ChatFunctionCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub choices: Vec<ChatChunkChoice>,
    #[serde(default)]
    pub usage: Option<ChunkUsage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkUsage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    #[serde(default)]
    pub cost: Option<f64>,
    #[serde(default)]
    pub cost_details: Option<Value>,
    #[serde(default)]
    pub reasoning_tokens: Option<i64>,
    #[serde(default)]
    pub prompt_tokens_details: Option<PromptTokensDetails>,
    #[serde(default)]
    pub completion_tokens_details: Option<CompletionTokensDetails>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PromptTokensDetails {
    #[serde(default)]
    pub cached_tokens: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompletionTokensDetails {
    #[serde(default)]
    pub reasoning_tokens: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatChunkChoice {
    pub index: usize,
    pub delta: ChatDelta,
    pub finish_reason: Option<String>,
    /// vLLM extension: the stop token that ended generation. A string is the
    /// matched stop *string*; an integer is a stop token id (incl. EOS). Only a
    /// string is a real stop-sequence match we can surface to Anthropic.
    #[serde(default)]
    pub stop_reason: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ChatDelta {
    pub content: Option<String>,
    pub reasoning_content: Option<String>,
    pub tool_calls: Option<Vec<ChatToolCall>>,
    pub function_call: Option<ChatFunctionCall>,
    #[serde(default)]
    pub refusal: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl ChatDelta {
    pub fn reasoning_delta(&self) -> Option<&str> {
        // Treat empty reasoning_content as absent so we fall through to alternate
        // fields when an upstream emits an empty placeholder alongside the real text.
        if let Some(text) = self
            .reasoning_content
            .as_deref()
            .filter(|text| !text.is_empty())
        {
            return Some(text);
        }
        for key in [
            "reasoning",
            "reasoning_text",
            "reasoning_delta",
            "reasoning_summary",
            "thinking",
            "thinking_content",
        ] {
            let Some(value) = self.extra.get(key) else {
                continue;
            };
            if let Some(text) = value.as_str().filter(|text| !text.is_empty()) {
                return Some(text);
            }
            if let Some(object) = value.as_object() {
                for nested in ["text", "delta", "content", "summary", "thinking"] {
                    if let Some(text) = object
                        .get(nested)
                        .and_then(Value::as_str)
                        .filter(|text| !text.is_empty())
                    {
                        return Some(text);
                    }
                }
            }
        }
        None
    }

    pub fn reasoning_signature_delta(&self) -> Option<&str> {
        self.thinking_object_field("signature")
            .or_else(|| self.extra.get("signature").and_then(Value::as_str))
    }

    fn thinking_object_field(&self, field: &str) -> Option<&str> {
        self.extra
            .get("thinking")
            .and_then(|value| value.get(field))
            .and_then(Value::as_str)
    }

    pub fn non_null_delta_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        if self.content.is_some() {
            keys.push("content".to_string());
        }
        if self.reasoning_content.is_some() {
            keys.push("reasoning_content".to_string());
        }
        if self.tool_calls.is_some() {
            keys.push("tool_calls".to_string());
        }
        if self.function_call.is_some() {
            keys.push("function_call".to_string());
        }
        if self.refusal.is_some() {
            keys.push("refusal".to_string());
        }
        keys.extend(
            self.extra
                .iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(key, _)| key.clone()),
        );
        keys
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionResponse {
    pub choices: Vec<ChatResponseChoice>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatResponseChoice {
    pub index: usize,
    pub message: ChatResponseMessage,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatResponseMessage {
    pub role: String,
    pub content: Option<String>,
    pub reasoning_content: Option<String>,
    pub tool_calls: Option<Vec<ChatToolCall>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn deserializes_tool_call_chunk_without_type_field() {
        let chunk: ChatCompletionChunk = serde_json::from_value(serde_json::json!({
            "id": "chatcmpl-1",
            "choices": [{
                "index": 0,
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call_1",
                        "function": {
                            "name": "echo",
                            "arguments": "{\"value\":\"hi\"}"
                        }
                    }]
                },
                "finish_reason": null
            }]
        }))
        .expect("chunk should deserialize");

        let tool_call = &chunk.choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tool_call.kind, "function");
        assert_eq!(tool_call.id.as_deref(), Some("call_1"));
        assert_eq!(tool_call.function.name.as_deref(), Some("echo"));
    }

    #[test]
    fn deserializes_openrouter_sparse_tool_call_chunk() {
        let chunk: ChatCompletionChunk = serde_json::from_value(serde_json::json!({
            "id": "gen-1778509792-pCb6B8HesSj8dH3qOYxT",
            "object": "chat.completion.chunk",
            "created": 1778509792,
            "model": "xiaomi/mimo-v2.5-pro-20260422",
            "provider": "Xiaomi",
            "choices": [{
                "index": 0,
                "delta": {
                    "content": null,
                    "role": "assistant",
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": "{\"file_path\":\"/home/luke/.claude/projects/-home-luke-projects-demo/memory/smb_clone.md\"}"
                        }
                    }]
                },
                "finish_reason": null,
                "native_finish_reason": null
            }]
        }))
        .expect("chunk should deserialize");

        let tool_call = &chunk.choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tool_call.kind, "function");
        assert_eq!(tool_call.index, Some(0));
        assert_eq!(tool_call.id, None);
        assert_eq!(tool_call.function.name, None);
        assert_eq!(
            tool_call
                .function
                .arguments
                .as_ref()
                .and_then(Value::as_str),
            Some(
                "{\"file_path\":\"/home/luke/.claude/projects/-home-luke-projects-demo/memory/smb_clone.md\"}"
            )
        );
    }

    #[test]
    fn deserializes_openrouter_usage_cost_fields() {
        let chunk: ChatCompletionChunk = serde_json::from_value(serde_json::json!({
            "id": "gen-1",
            "choices": [],
            "usage": {
                "prompt_tokens": 12,
                "completion_tokens": 3,
                "total_tokens": 15,
                "cost": 0.0042,
                "cost_details": {
                    "upstream_inference_cost": 0.0031
                }
            }
        }))
        .expect("chunk should deserialize");

        let usage = chunk.usage.expect("usage");
        assert_eq!(usage.cost, Some(0.0042));
        assert_eq!(
            usage
                .cost_details
                .as_ref()
                .and_then(|details| details.get("upstream_inference_cost"))
                .and_then(Value::as_f64),
            Some(0.0031)
        );
    }

    #[test]
    fn deserializes_tool_call_chunk_with_null_type_field() {
        let payload = r#"{"id":"chatcmpl-872acb605d617b3c","object":"chat.completion.chunk","created":1778678877,"model":"Kimi-K2.6","choices":[{"index":0,"delta":{"tool_calls":[{"id":null,"type":null,"index":0,"function":{"name":null,"arguments":"\"} "}}]},"logprobs":null,"finish_reason":"tool_calls","stop_reason":163586,"token_ids":null}]}"#;
        let chunk: ChatCompletionChunk =
            serde_json::from_str(payload).expect("chunk with null type should deserialize");

        let tool_call = &chunk.choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tool_call.kind, "function");
        assert!(tool_call.id.is_none());
        assert!(tool_call.function.name.is_none());
        assert_eq!(
            tool_call
                .function
                .arguments
                .as_ref()
                .and_then(Value::as_str),
            Some("\"} ")
        );
    }

    #[test]
    fn deserializes_tool_call_chunk_without_function_field() {
        let chunk: ChatCompletionChunk = serde_json::from_value(serde_json::json!({
            "id": "chatcmpl-1",
            "choices": [{
                "index": 0,
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call_1"
                    }]
                },
                "finish_reason": null
            }]
        }))
        .expect("chunk should deserialize");

        let tool_call = &chunk.choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tool_call.kind, "function");
        assert_eq!(tool_call.id.as_deref(), Some("call_1"));
        assert_eq!(tool_call.function, ChatFunctionCall::default());
    }

    #[test]
    fn deserializes_legacy_function_call_chunk() {
        let chunk: ChatCompletionChunk = serde_json::from_value(serde_json::json!({
            "id": "chatcmpl-1",
            "choices": [{
                "index": 0,
                "delta": {
                    "function_call": {
                        "name": "echo",
                        "arguments": "{\"value\":\"hi\"}"
                    }
                },
                "finish_reason": null
            }]
        }))
        .expect("chunk should deserialize");

        let function_call = chunk.choices[0].delta.function_call.as_ref().unwrap();
        assert_eq!(function_call.name.as_deref(), Some("echo"));
        assert_eq!(
            function_call.arguments.as_ref().and_then(Value::as_str),
            Some("{\"value\":\"hi\"}")
        );
        assert_eq!(
            chunk.choices[0].delta.non_null_delta_keys(),
            vec!["function_call".to_string()]
        );
    }

    #[test]
    fn deserializes_reasoning_delta_alias() {
        let chunk: ChatCompletionChunk = serde_json::from_value(serde_json::json!({
            "id": "chatcmpl-1",
            "choices": [{
                "index": 0,
                "delta": {
                    "reasoning": "hidden step"
                },
                "finish_reason": null
            }]
        }))
        .expect("chunk should deserialize");

        assert_eq!(
            chunk.choices[0].delta.reasoning_delta(),
            Some("hidden step")
        );
        assert_eq!(
            chunk.choices[0].delta.non_null_delta_keys(),
            vec!["reasoning".to_string()]
        );
    }

    #[test]
    fn deserializes_nested_thinking_content_and_signature() {
        let chunk: ChatCompletionChunk = serde_json::from_value(serde_json::json!({
            "id": "chatcmpl-1",
            "choices": [{
                "index": 0,
                "delta": {
                    "thinking": {
                        "content": "hidden step",
                        "signature": "sig_123"
                    }
                },
                "finish_reason": null
            }]
        }))
        .expect("chunk should deserialize");

        let delta = &chunk.choices[0].delta;
        assert_eq!(delta.reasoning_delta(), Some("hidden step"));
        assert_eq!(delta.reasoning_signature_delta(), Some("sig_123"));
        assert_eq!(delta.non_null_delta_keys(), vec!["thinking".to_string()]);
    }

    #[test]
    fn serializes_max_output_tokens_as_chat_max_tokens() {
        let request = ChatCompletionRequest {
            model: "glm-5.1".to_string(),
            messages: Vec::new(),
            stream: true,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: Some(256),
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: Default::default(),
        };

        let value = serde_json::to_value(request).expect("serialize");

        assert_eq!(value["max_tokens"], Value::from(256));
        assert!(
            value.get("max_output_tokens").is_none(),
            "chat backend requests should use max_tokens"
        );
    }

    #[test]
    fn serializes_stop_to_upstream_key() {
        let request = ChatCompletionRequest {
            model: "glm-5.1".to_string(),
            messages: Vec::new(),
            stream: true,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: Some(vec!["</decision>".to_string()]),
            extra_body: Default::default(),
        };

        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(value["stop"], serde_json::json!(["</decision>"]));

        let omitted = ChatCompletionRequest {
            stop: None,
            ..request
        };
        let value = serde_json::to_value(omitted).expect("serialize");
        assert!(
            value.get("stop").is_none(),
            "stop must be omitted when None"
        );
    }

    #[test]
    fn serializes_parallel_tool_calls_once() {
        let request = ChatCompletionRequest {
            model: "glm-5.2".to_string(),
            messages: Vec::new(),
            stream: true,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: Some(true),
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: Default::default(),
        };

        let json = serde_json::to_string(&request).expect("serialize");
        let occurrences = json.matches("parallel_tool_calls").count();
        assert_eq!(
            occurrences, 1,
            "parallel_tool_calls must appear exactly once, got {json}"
        );
    }

    #[test]
    fn omits_parallel_tool_calls_when_none() {
        let request = ChatCompletionRequest {
            model: "glm-5.2".to_string(),
            messages: Vec::new(),
            stream: true,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: Default::default(),
        };

        let json = serde_json::to_string(&request).expect("serialize");
        assert!(
            !json.contains("parallel_tool_calls"),
            "parallel_tool_calls must be omitted when None, got {json}"
        );
    }

    #[test]
    fn deserializes_stop_as_string_or_array() {
        let from_string: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "glm-5.1",
            "messages": [],
            "stop": "</s>"
        }))
        .expect("string stop should deserialize");
        assert_eq!(from_string.stop, Some(vec!["</s>".to_string()]));

        let from_array: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "glm-5.1",
            "messages": [],
            "stop": ["a", "b"]
        }))
        .expect("array stop should deserialize");
        assert_eq!(
            from_array.stop,
            Some(vec!["a".to_string(), "b".to_string()])
        );

        let absent: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "glm-5.1",
            "messages": []
        }))
        .expect("absent stop should deserialize");
        assert_eq!(absent.stop, None);

        let null_stop: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "glm-5.1",
            "messages": [],
            "stop": null
        }))
        .expect("null stop should deserialize");
        assert_eq!(null_stop.stop, None);
    }

    #[test]
    fn normalize_stop_drops_empties_and_rejects_excess() {
        assert_eq!(normalize_stop(None).expect("none"), None);
        assert_eq!(
            normalize_stop(Some(vec![String::new(), String::new()])).expect("all empty"),
            None
        );
        assert_eq!(
            normalize_stop(Some(vec!["x".to_string(), String::new(), "y".to_string()]))
                .expect("filtered"),
            Some(vec!["x".to_string(), "y".to_string()])
        );
        let four = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        assert_eq!(
            normalize_stop(Some(four.clone())).expect("exactly four"),
            Some(four)
        );
        let mut five = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        five.push("e".to_string());
        assert!(
            normalize_stop(Some(five)).is_err(),
            "more than four must error"
        );
    }

    #[test]
    fn deserializes_max_completion_tokens_alias() {
        let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "glm-5.1",
            "messages": [],
            "max_completion_tokens": 512
        }))
        .expect("request should deserialize");

        assert_eq!(request.max_output_tokens, Some(512));
    }

    #[test]
    fn deserializes_chat_tool_without_description() {
        let tool: ChatTool = serde_json::from_value(serde_json::json!({
            "type": "function",
            "function": {
                "name": "echo",
                "parameters": {
                    "type": "object",
                    "properties": {}
                }
            }
        }))
        .expect("tool should deserialize");

        assert_eq!(tool.function.name, "echo");
        assert_eq!(tool.function.description, "");
    }
}
