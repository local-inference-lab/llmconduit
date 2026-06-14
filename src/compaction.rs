//! Optional, opt-in context compaction (OFF by default).
//!
//! Hooks `run_turn`'s `current_messages` before the upstream send: when the
//! lowered chat request exceeds `max_input_tokens`, POST it to an external
//! compactor and forward the compacted messages instead. Best-effort — any
//! error (disabled, no endpoint, network, bad response) forwards the ORIGINAL
//! request unchanged, so a user request never fails because of compaction.
//!
//! Why external: keeps the gateway a single static binary while letting the
//! compaction strategy (summary / structured state-ledger / whatever) live in
//! any language. A builtin fallback can be added behind `mode = "builtin"`.
//!
//! Contract:
//!   POST {endpoint}/compact
//!     req : { session_id, messages, max_input_tokens, keep_recent_turns, model }
//!     resp: { compacted: bool, messages: [ChatMessage, ...] }
//!   compacted == false  =>  gateway forwards the original messages.

use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use tracing::debug;
use tracing::warn;

use crate::models::chat::ChatMessage;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct CompactionConfig {
    pub enabled: bool,
    /// Only "external" is supported today; "builtin" is reserved.
    pub mode: String,
    pub endpoint: Option<String>,
    pub max_input_tokens: usize,
    pub keep_recent_turns: usize,
    pub timeout_ms: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: "external".to_string(),
            endpoint: None,
            max_input_tokens: 96_000,
            keep_recent_turns: 6,
            timeout_ms: 60_000,
        }
    }
}

/// Rough token estimate over the chat messages (~4 chars/token).
fn estimate_tokens(messages: &[ChatMessage]) -> usize {
    let mut chars = 0usize;
    for m in messages {
        if let Some(content) = &m.content {
            chars += match content {
                Value::String(s) => s.len(),
                other => other.to_string().len(),
            };
        }
        if let Some(tool_calls) = &m.tool_calls {
            chars += serde_json::to_string(tool_calls)
                .map(|s| s.len())
                .unwrap_or(0);
        }
    }
    (chars / 4).max(1)
}

/// Stable session id when the client supplies none: hash system + first user
/// message (immutable across a session) so the compactor can keep per-session
/// state and keep its output prefix-stable.
pub fn derive_session_id(messages: &[ChatMessage]) -> String {
    use std::hash::Hash;
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for m in messages {
        if m.role == "system" || m.role == "user" {
            match &m.content {
                Some(Value::String(s)) => s.hash(&mut hasher),
                Some(other) => other.to_string().hash(&mut hasher),
                None => {}
            }
            if m.role == "user" {
                break;
            }
        }
    }
    format!("sess-{:016x}", hasher.finish())
}

#[derive(Deserialize)]
struct CompactResponse {
    #[serde(default)]
    compacted: bool,
    #[serde(default)]
    messages: Option<Vec<ChatMessage>>,
}

/// Best-effort compaction. Returns compacted messages only when enabled, over
/// budget, and the external compactor succeeds; otherwise returns `messages`.
pub async fn maybe_compact(
    cfg: &CompactionConfig,
    messages: Vec<ChatMessage>,
    session_id: &str,
    model: &str,
) -> Vec<ChatMessage> {
    if !cfg.enabled || estimate_tokens(&messages) <= cfg.max_input_tokens {
        return messages;
    }
    if cfg.mode != "external" {
        warn!(mode = %cfg.mode, "unsupported compaction.mode; forwarding unchanged");
        return messages;
    }
    let endpoint = match cfg.endpoint.as_deref() {
        Some(endpoint) => endpoint,
        None => {
            warn!("compaction.enabled but compaction.endpoint not set; forwarding unchanged");
            return messages;
        }
    };
    match call_external(cfg, endpoint, &messages, session_id, model).await {
        Ok(Some(compacted)) => {
            debug!(
                before = messages.len(),
                after = compacted.len(),
                "context compacted"
            );
            compacted
        }
        Ok(None) => messages,
        Err(err) => {
            warn!(error = %err, "compaction failed; forwarding original request");
            messages
        }
    }
}

async fn call_external(
    cfg: &CompactionConfig,
    endpoint: &str,
    messages: &[ChatMessage],
    session_id: &str,
    model: &str,
) -> Result<Option<Vec<ChatMessage>>, Box<dyn std::error::Error + Send + Sync>> {
    let body = json!({
        "session_id": session_id,
        "messages": messages,
        "max_input_tokens": cfg.max_input_tokens,
        "keep_recent_turns": cfg.keep_recent_turns,
        "model": model,
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(cfg.timeout_ms))
        .build()?;
    let url = format!("{}/compact", endpoint.trim_end_matches('/'));
    let parsed: CompactResponse = client
        .post(url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if !parsed.compacted {
        return Ok(None);
    }
    Ok(parsed.messages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg(role: &str, content: &str) -> ChatMessage {
        ChatMessage {
            role: role.to_string(),
            content: Some(json!(content)),
            tool_call_id: None,
            name: None,
            reasoning_content: None,
            thinking: None,
            tool_calls: None,
        }
    }

    #[tokio::test]
    async fn disabled_is_noop() {
        let cfg = CompactionConfig::default(); // enabled:false
        let msgs = vec![msg("user", &"x".repeat(10_000))];
        let out = maybe_compact(&cfg, msgs.clone(), "s", "m").await;
        assert_eq!(out, msgs);
    }

    #[tokio::test]
    async fn under_budget_is_noop() {
        let cfg = CompactionConfig {
            enabled: true,
            endpoint: Some("http://127.0.0.1:1".into()),
            max_input_tokens: 100_000,
            ..Default::default()
        };
        let msgs = vec![msg("user", "hello")];
        let out = maybe_compact(&cfg, msgs.clone(), "s", "m").await;
        assert_eq!(out, msgs);
    }

    #[test]
    fn session_id_stable_and_prefix_based() {
        let a = vec![
            msg("system", "sys"),
            msg("user", "first"),
            msg("assistant", "later"),
        ];
        let b = vec![
            msg("system", "sys"),
            msg("user", "first"),
            msg("assistant", "different later"),
        ];
        assert_eq!(derive_session_id(&a), derive_session_id(&b));
    }
}
