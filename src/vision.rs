//! G4 — Image agent (vision offload).
//!
//! Ports claude-relay's in-proxy vision offload to llmconduit's canonical
//! Responses pipeline. The model the client talks to is (typically) text-only;
//! images in the latest user turn are stripped to `[Image #N]` placeholders,
//! cached, and an `analyzeImage` server tool is injected. When the model calls
//! `analyzeImage`, the engine resolves the cached image(s), forwards them to a
//! vision-capable backend via [`VisionClient`], and injects the description back
//! into the chat history as a tool result — exactly the way Brave `web_search`
//! is run server-side.
//!
//! Mirrors `src/search.rs`'s `SearchClient` seam: a trait object so tests inject
//! a `MockVisionClient`. The cache is intentionally SEPARATE from `ReplayStore`
//! (replay is SHA256 over `(model, instructions, input)` with no TTL); this is a
//! per-session LRU+TTL keyed by `(session_id, image_id)` that is cleared and
//! repopulated every time [`ImageCache::strip_and_cache_images`] runs, so
//! multi-turn placeholder numbering resets like claude-relay's stateless replay.

use crate::config::Config;
use crate::error::AppError;
use crate::error::AppResult;
use crate::models::responses::ContentItem;
use crate::models::responses::ResponseItem;
use crate::models::responses::ResponsesRequest;
use crate::models::responses::ToolSpec;
use async_trait::async_trait;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use url::Url;

/// Server-side tool name the image agent injects and intercepts. Lowercased
/// once for the case-insensitive registry/lookup parity with `web_search`.
pub const ANALYZE_IMAGE_TOOL_NAME: &str = "analyzeImage";

/// System-prompt block prepended when the image agent is active, instructing the
/// (image-blind) text model to call `analyzeImage` before answering. Adapted
/// from claude-relay's `IMAGE_AGENT_SYSTEM_PROMPT`.
pub const IMAGE_AGENT_SYSTEM_PROMPT: &str = "CRITICAL INSTRUCTION — IMAGE HANDLING:\n\
You CANNOT see images. All images are replaced with [Image #N] placeholders which are OPAQUE to you.\n\
When the latest user message contains [Image #N], you MUST call the `analyzeImage` tool \
as your FIRST action BEFORE generating any text response.\n\
- You have NO ability to see, interpret, or guess what is in an image without calling analyzeImage.\n\
- If you respond about an image without calling analyzeImage first, your response WILL be wrong.\n\
- Call analyzeImage with ALL image IDs from the latest message.\n\
This is non-negotiable. ALWAYS call analyzeImage when [Image #N] is present.";

/// System prompt sent to the vision backend itself (claude-relay's
/// `VISION_SYSTEM_PROMPT`).
pub const VISION_SYSTEM_PROMPT: &str = "Analyze the provided image(s) according to the task \
description below. Be thorough, specific, and accurate in your analysis. If conversation context \
is provided, use it to focus your analysis on the most relevant aspects of the image. Describe \
exactly what you observe.";

/// Description for the injected `analyzeImage` tool (claude-relay's
/// `ANALYZE_IMAGE_TOOL` description).
pub const ANALYZE_IMAGE_TOOL_DESCRIPTION: &str = "MANDATORY tool for viewing images. You CANNOT \
see images directly — [Image #N] placeholders are opaque to you. The ONLY way to see image content \
is by calling this tool. You MUST call analyzeImage BEFORE writing ANY response when the latest \
user message contains [Image #N]. NEVER describe, guess, or comment on image content without \
calling this tool first.";

/// JSON-schema parameters for the injected `analyzeImage` tool.
pub fn analyze_image_tool_parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "imageId": {
                "type": "array",
                "description": "Image IDs to analyze — extract from [Image #N] placeholders (e.g. [Image #1] → \"1\")",
                "items": { "type": "string" }
            },
            "task": {
                "type": "string",
                "description": "What to look for in the image, based on conversation context"
            },
            "context": {
                "type": "string",
                "description": "Brief conversation context so the vision model knows what to focus on"
            }
        },
        "required": ["imageId", "task"]
    })
}

/// The canonical `ToolSpec` for the injected `analyzeImage` tool. It lowers to a
/// chat function tool like any other; the server-side classification happens via
/// `ToolKind::ImageAnalysis` (registered only on active image-agent turns).
pub fn analyze_image_tool_spec() -> ToolSpec {
    ToolSpec::Function {
        name: ANALYZE_IMAGE_TOOL_NAME.to_string(),
        description: ANALYZE_IMAGE_TOOL_DESCRIPTION.to_string(),
        strict: false,
        parameters: analyze_image_tool_parameters(),
    }
}

/// Placeholder text that replaces a stripped image. Numbered sequentially within
/// a single request, matching claude-relay's `[Image #N]` format so the model is
/// told exactly how to reference the image when calling `analyzeImage`.
fn image_placeholder_text(number: usize) -> String {
    format!(
        "[Image #{number}] — YOU CANNOT SEE THIS IMAGE. Call analyzeImage(imageId=[\"{number}\"]) to view it."
    )
}

/// A cached image, stored as the canonical `input_image` parts so the vision
/// backend receives the exact `image_url` (data URL or remote URL) the client
/// sent. Kept tiny and `Clone` so the executor can take a snapshot under the
/// lock and release it before the network call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedImage {
    /// The `image_url` value from the canonical `ContentItem::InputImage`
    /// (`data:` URL or remote URL).
    pub image_url: String,
    /// Optional `detail` hint (`low`/`high`/`auto`) carried through to the
    /// vision request unchanged.
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    image: CachedImage,
    stored_at: Instant,
}

/// Per-session LRU image cache with TTL, keyed by `(session_id, image_id)`.
///
/// Separate from `ReplayStore` by design (see module docs). Interior mutability
/// via `Mutex` so a single `Arc<ImageCache>` is shared and the strip seam can
/// mutate it while the executor reads it. Eviction and TTL are per-session,
/// matching claude-relay's `ImageCache` semantics exactly.
#[derive(Debug)]
pub struct ImageCache {
    max_size: usize,
    ttl: Duration,
    sessions: Mutex<HashMap<String, SessionCache>>,
}

#[derive(Debug, Default)]
struct SessionCache {
    /// Insertion/access order for LRU eviction (front = oldest). A small
    /// `VecDeque` of keys paired with a `HashMap` keeps both O(1)-ish lookups
    /// and an explicit recency order without pulling in an LRU crate.
    order: VecDeque<String>,
    entries: HashMap<String, CacheEntry>,
}

impl ImageCache {
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            // A zero max would evict everything immediately and make the agent a
            // no-op; floor at 1 so a misconfigured cap still caches one image.
            max_size: max_size.max(1),
            ttl,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Build a cache from config. Defaults are generous enough for a normal
    /// multi-image turn while bounding memory.
    pub fn from_config(config: &Config) -> Self {
        Self::new(
            config.image_cache_max_size,
            Duration::from_secs(config.image_cache_ttl_secs),
        )
    }

    /// The session-scoped cache key for an image number, matching claude-relay's
    /// `f"{session_id}_Image#{n}"`.
    pub fn image_key(session_id: &str, image_id: &str) -> String {
        format!("{session_id}_Image#{image_id}")
    }

    fn store(&self, session_id: &str, image_key: String, image: CachedImage) {
        let mut sessions = self.sessions.lock().expect("image cache mutex");
        self.cleanup_expired_locked(&mut sessions);
        let cache = sessions.entry(session_id.to_string()).or_default();
        if cache
            .entries
            .insert(
                image_key.clone(),
                CacheEntry {
                    image,
                    stored_at: Instant::now(),
                },
            )
            .is_some()
        {
            cache.order.retain(|key| key != &image_key);
        }
        cache.order.push_back(image_key);
        while cache.order.len() > self.max_size {
            if let Some(oldest) = cache.order.pop_front() {
                cache.entries.remove(&oldest);
            }
        }
    }

    /// Fetch a cached image for a session, honoring TTL and refreshing recency
    /// (LRU touch). Returns `None` for a missing/expired entry.
    pub fn get(&self, session_id: &str, image_key: &str) -> Option<CachedImage> {
        let mut sessions = self.sessions.lock().expect("image cache mutex");
        self.cleanup_expired_locked(&mut sessions);
        let cache = sessions.get_mut(session_id)?;
        let entry = cache.entries.get(image_key)?;
        if self.is_expired(entry) {
            cache.entries.remove(image_key);
            cache.order.retain(|key| key != image_key);
            return None;
        }
        // LRU touch: move to the back as the most-recently-used.
        cache.order.retain(|key| key != image_key);
        cache.order.push_back(image_key.to_string());
        Some(cache.entries.get(image_key)?.image.clone())
    }

    fn is_expired(&self, entry: &CacheEntry) -> bool {
        // A zero TTL means "expire immediately" (claude-relay parity): any
        // elapsed time counts as expired.
        entry.stored_at.elapsed() > self.ttl
    }

    fn cleanup_expired_locked(&self, sessions: &mut HashMap<String, SessionCache>) {
        let ttl_expired = |entry: &CacheEntry| entry.stored_at.elapsed() > self.ttl;
        sessions.retain(|_, cache| {
            let expired_keys: Vec<String> = cache
                .entries
                .iter()
                .filter(|(_, entry)| ttl_expired(entry))
                .map(|(key, _)| key.clone())
                .collect();
            for key in expired_keys {
                cache.entries.remove(&key);
                cache.order.retain(|existing| existing != &key);
            }
            !cache.entries.is_empty()
        });
    }

    /// Clear a single session's cache (used before repopulating on each strip).
    fn clear_session(&self, session_id: &str) {
        let mut sessions = self.sessions.lock().expect("image cache mutex");
        sessions.remove(session_id);
    }

    /// Number of cached images for a session (test helper).
    #[cfg(test)]
    fn session_len(&self, session_id: &str) -> usize {
        let sessions = self.sessions.lock().expect("image cache mutex");
        sessions
            .get(session_id)
            .map(|c| c.entries.len())
            .unwrap_or(0)
    }

    /// Strip images from the request, cache the originals, inject the
    /// `analyzeImage` tool + system-prompt instruction, and dedup any
    /// caller-supplied `analyzeImage` tool.
    ///
    /// This is the SINGLE mutation surface (called from `Gateway::stream_responses`
    /// BEFORE replay/lowering) so replay hashes only ever see placeholder text,
    /// never image bytes. Walks ALL user messages so a multi-turn history is
    /// renumbered consistently; the cache is cleared first so numbering resets
    /// per request exactly like claude-relay's stateless processing.
    ///
    /// `gate` decides activation: the caller must have already confirmed the
    /// latest user turn has images and the backend is not native-vision. We
    /// renumber every user-message image (not just the latest) because the
    /// history the client resends still contains earlier raw images that would
    /// otherwise leak bytes upstream.
    pub fn strip_and_cache_images(&self, request: &mut ResponsesRequest, session_id: &str) {
        self.clear_session(session_id);
        let mut counter = 1usize;
        for item in &mut request.input {
            let ResponseItem::Message { role, content, .. } = item else {
                continue;
            };
            if role != "user" {
                continue;
            }
            for part in content.iter_mut() {
                if let ContentItem::InputImage {
                    image_url: Some(image_url),
                    detail,
                    ..
                } = part
                {
                    let key = Self::image_key(session_id, &counter.to_string());
                    self.store(
                        session_id,
                        key,
                        CachedImage {
                            image_url: std::mem::take(image_url),
                            detail: detail.clone(),
                        },
                    );
                    *part = ContentItem::InputText {
                        text: image_placeholder_text(counter),
                    };
                    counter += 1;
                }
            }
        }
        inject_analyze_image_tool(&mut request.tools);
        prepend_image_agent_system_prompt(request);
    }
}

/// Install the canonical `analyzeImage` tool for an active image-agent turn,
/// REPLACING any caller-supplied tool of the same name (G4 review #4, hardened
/// round-2 #3) and de-duplicating. The gateway runs `analyzeImage` server-side
/// against the stripped images, so the model MUST see the canonical schema
/// (which requires `imageId`); a conflicting caller schema could make the model
/// omit `imageId` after the images were already replaced with placeholders.
///
/// We drop every existing `analyzeImage` (case-insensitive) BOTH as a top-level
/// Function/Custom tool AND as a NAMESPACED child — a surviving namespaced
/// `analyzeImage` would lower to a chat tool of the same name and collide with
/// the appended canonical tool (`lower_tools` rejects duplicate names). A
/// namespace left empty after removal is dropped entirely. Finally append
/// exactly one canonical spec.
fn inject_analyze_image_tool(tools: &mut Vec<ToolSpec>) {
    tools.retain_mut(|spec| match spec {
        // Top-level analyzeImage (Function/Custom): remove.
        spec if tool_is_analyze_image(spec) => false,
        // Namespaced: strip analyzeImage children; drop the namespace if empty.
        ToolSpec::Namespace { tools, .. } => {
            tools.retain(|tool| !namespace_tool_is_analyze_image(tool));
            !tools.is_empty()
        }
        _ => true,
    });
    tools.push(analyze_image_tool_spec());
}

/// Whether a namespace child tool is `analyzeImage` (case-insensitive).
fn namespace_tool_is_analyze_image(tool: &crate::models::responses::NamespaceToolSpec) -> bool {
    match tool {
        crate::models::responses::NamespaceToolSpec::Function { name, .. } => {
            name.eq_ignore_ascii_case(ANALYZE_IMAGE_TOOL_NAME)
        }
    }
}

/// Whether a `ToolSpec` is the `analyzeImage` tool (case-insensitive on the
/// function name), used for dedup and for relaxing a forced `tool_choice`.
pub fn tool_is_analyze_image(spec: &ToolSpec) -> bool {
    match spec {
        ToolSpec::Function { name, .. } | ToolSpec::Custom { name, .. } => {
            name.eq_ignore_ascii_case(ANALYZE_IMAGE_TOOL_NAME)
        }
        _ => false,
    }
}

/// Prepend the image-agent instruction to `instructions` (the canonical home for
/// system text), mirroring `apply_system_prompt_prefix`'s prepend convention.
fn prepend_image_agent_system_prompt(request: &mut ResponsesRequest) {
    request.instructions = if request.instructions.is_empty() {
        IMAGE_AGENT_SYSTEM_PROMPT.to_string()
    } else {
        format!("{IMAGE_AGENT_SYSTEM_PROMPT}\n\n{}", request.instructions)
    };
}

/// Whether the LATEST user message in the canonical input carries at least one
/// `InputImage` with a usable `image_url`. Only the latest user turn activates
/// the agent (claude-relay's `has_images`): old images lingering in history must
/// not re-trigger stripping/tool-injection.
pub fn latest_user_message_has_images(input: &[ResponseItem]) -> bool {
    let Some(content) = latest_user_message_content(input) else {
        return false;
    };
    content.iter().any(|part| {
        matches!(
            part,
            ContentItem::InputImage {
                image_url: Some(url),
                ..
            } if !url.is_empty()
        )
    })
}

fn latest_user_message_content(input: &[ResponseItem]) -> Option<&[ContentItem]> {
    input.iter().rev().find_map(|item| match item {
        ResponseItem::Message { role, content, .. } if role == "user" => Some(content.as_slice()),
        _ => None,
    })
}

/// Parsed `analyzeImage` arguments. `image_ids` may be empty (the executor then
/// surfaces a model-visible "no images" message rather than erroring).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisionRequest {
    pub image_ids: Vec<String>,
    pub task: String,
    pub context: Option<String>,
    pub images: Vec<CachedImage>,
}

impl VisionRequest {
    /// Parse the `analyzeImage` tool arguments into a structured request,
    /// resolving each requested image id against the cache for `session_id`.
    /// Missing ids are simply skipped (claude-relay logs + skips); the executor
    /// decides what model-visible text to inject.
    pub fn from_arguments(arguments: &Value, session_id: &str, cache: &ImageCache) -> Self {
        let image_ids = arguments
            .get("imageId")
            .and_then(Value::as_array)
            .map(|ids| ids.iter().filter_map(value_to_image_id).collect::<Vec<_>>())
            .unwrap_or_default();
        let task = arguments
            .get("task")
            .and_then(Value::as_str)
            .filter(|task| !task.trim().is_empty())
            .unwrap_or("Describe this image in detail")
            .to_string();
        let context = arguments
            .get("context")
            .and_then(Value::as_str)
            .filter(|context| !context.trim().is_empty())
            .map(ToString::to_string);
        let images = image_ids
            .iter()
            .filter_map(|id| cache.get(session_id, &ImageCache::image_key(session_id, id)))
            .collect();
        Self {
            image_ids,
            task,
            context,
            images,
        }
    }
}

/// Coerce a JSON `imageId` array element to a string id. Accepts string or
/// numeric ids (`["1"]` or `[1]`) since models occasionally emit the latter.
fn value_to_image_id(value: &Value) -> Option<String> {
    match value {
        Value::String(id) if !id.trim().is_empty() => Some(id.trim().to_string()),
        Value::Number(num) => Some(num.to_string()),
        _ => None,
    }
}

/// Result of a vision analysis: the description text injected back into the chat
/// history as the `analyzeImage` tool result.
#[derive(Debug, Clone, Default)]
pub struct VisionOutcome {
    pub text: String,
}

#[async_trait]
pub trait VisionClient: Send + Sync {
    /// Analyze the request's images and return the model-visible description.
    /// A backend error is surfaced as `Err`; the engine converts it to
    /// model-visible tool text so the turn can still complete (mirroring the
    /// Brave web_search degrade-gracefully contract).
    async fn analyze(&self, request: &VisionRequest) -> AppResult<VisionOutcome>;
}

/// Production [`VisionClient`]: POSTs an OpenAI-compatible chat completion (with
/// `image_url` content parts) to the configured vision backend and returns the
/// assistant content. Non-streaming, matching claude-relay's `vision_body`.
#[derive(Debug, Clone)]
pub struct ReqwestVisionClient {
    client: reqwest::Client,
    url: Option<Url>,
    model: Option<String>,
}

impl ReqwestVisionClient {
    pub fn new(client: reqwest::Client, config: &Config) -> Self {
        Self {
            client,
            url: config.vision_url.clone(),
            model: config.vision_model.clone(),
        }
    }

    fn build_body(&self, request: &VisionRequest) -> Value {
        let mut user_content: Vec<Value> = request
            .images
            .iter()
            .map(|image| {
                let mut image_url = serde_json::Map::new();
                image_url.insert("url".to_string(), Value::String(image.image_url.clone()));
                if let Some(detail) = &image.detail {
                    image_url.insert("detail".to_string(), Value::String(detail.clone()));
                }
                json!({ "type": "image_url", "image_url": Value::Object(image_url) })
            })
            .collect();
        let mut prompt = format!("Task: {}", request.task);
        if let Some(context) = &request.context {
            prompt.push_str(&format!("\nContext: {context}"));
        }
        user_content.push(json!({ "type": "text", "text": prompt }));
        json!({
            "model": self.model.clone().unwrap_or_default(),
            "messages": [
                { "role": "system", "content": VISION_SYSTEM_PROMPT },
                { "role": "user", "content": user_content },
            ],
            "max_tokens": 4096,
            "stream": false,
        })
    }
}

#[async_trait]
impl VisionClient for ReqwestVisionClient {
    async fn analyze(&self, request: &VisionRequest) -> AppResult<VisionOutcome> {
        let url = self
            .url
            .clone()
            .ok_or_else(|| AppError::internal("image agent is active but vision_url is missing"))?;
        let body = self.build_body(request);
        let response = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|err| AppError::upstream(format!("vision request failed: {err}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            // The body becomes model-visible tool text AND is logged. A vision
            // backend that echoes the submitted `data:image`/signed URL would
            // otherwise re-inject raw image data into the next text-backend
            // request and the logs (G4 review #3), so redact + cap it.
            return Err(AppError::upstream(format!(
                "vision backend failed with {status}: {}",
                redact_vision_text(&text)
            )));
        }
        let payload: Value = response
            .json()
            .await
            .map_err(|err| AppError::upstream(format!("invalid vision JSON: {err}")))?;
        if let Some(error) = payload.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .unwrap_or_else(|| error.to_string());
            return Err(AppError::upstream(format!(
                "vision backend error: {}",
                redact_vision_text(&message)
            )));
        }
        let text = payload
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("message"))
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .ok_or_else(|| AppError::upstream("vision response missing message content"))?;
        // Round-3 #3: even a SUCCESSFUL description can echo a submitted
        // `data:`/signed image URL; redact before it becomes a model-visible
        // tool result or is logged. Redacting here makes EVERY VisionOutcome
        // safe regardless of caller (the engine injects + previews it).
        Ok(VisionOutcome {
            text: redact_vision_text(&text),
        })
    }
}

/// Cap on the redacted vision text snippet that becomes model-visible/logged.
const VISION_TEXT_REDACT_LIMIT: usize = 4096;

/// URI prefixes that could carry raw image data or a signed image URL, in BOTH
/// raw and JSON-escaped (`\/`) forms (G4 round-3 #4). Matched case-insensitively
/// (round-2 #2). `data:` base64 payloads have no slash to escape. Order matters
/// only for disambiguation; we pick the earliest match regardless.
const SENSITIVE_URI_PREFIXES: [&str; 5] = [
    "data:",
    "https://",
    "http://",
    "https:\\/\\/",
    "http:\\/\\/",
];

/// Whether `c` ends a sensitive URI run. A `data:` base64 URL contains a `,`
/// SEPARATING the media type from the payload, so `,` must not terminate it;
/// only whitespace/quote/bracket bounds a `data:` run. For an `http(s)` URL a
/// comma/paren also bounds it in prose/JSON. A backslash is NOT a delimiter so
/// JSON-escaped `\/` inside a URL is consumed as part of the run.
fn is_uri_run_delimiter(c: char, is_data: bool) -> bool {
    c.is_whitespace()
        || matches!(c, '"' | '\'' | ']' | '}' | '<' | '>')
        || (!is_data && matches!(c, ')' | ','))
}

/// THE single image-redaction primitive (G4 round-4 consolidation). Replaces
/// every `data:` and `http(s)` URI run — case-insensitive, raw AND JSON-escaped
/// (`\/`) form, including signed-URL query tokens — with `<redacted uri>`. This
/// is the one place the URI semantics live; ALL logging/echoing surfaces route
/// through it (inbound trace, upstream JSONL, debug monitor + `/debug/ws`, and
/// vision success/error text), so request image bytes / signed URLs cannot leak
/// to any sink (AGENTS.md redact rule). Does NOT truncate — callers that need a
/// length cap layer it on (see [`redact_vision_text`]).
pub fn redact_image_uris(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0usize;
    while cursor < text.len() {
        // Earliest sensitive URI start at/after `cursor` (case-insensitive via
        // the lowercased copy, which preserves byte offsets for ASCII prefixes).
        let next = SENSITIVE_URI_PREFIXES
            .iter()
            .filter_map(|prefix| {
                lower[cursor..]
                    .find(prefix)
                    .map(|rel| (cursor + rel, *prefix))
            })
            .min_by_key(|(pos, _)| *pos);
        let Some((start, prefix)) = next else {
            out.push_str(&text[cursor..]);
            break;
        };
        out.push_str(&text[cursor..start]);
        out.push_str("<redacted uri>");
        let is_data = prefix.starts_with("data:");
        let after = &text[start + prefix.len()..];
        let end = after
            .find(|c: char| is_uri_run_delimiter(c, is_data))
            .unwrap_or(after.len());
        cursor = start + prefix.len() + end;
    }
    out
}

/// Recursively redact image URIs in every string within a JSON value (G4
/// round-4 consolidation). Used by the request-logging surfaces (inbound trace
/// `redact_payload_secrets`, upstream JSONL) so a `data:`/signed `image_url`
/// anywhere in the body — string field, content-part, nested object/array — is
/// stripped before serialization, regardless of key name.
pub fn redact_image_uris_in_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(text) => {
            let redacted = redact_image_uris(text);
            if redacted != *text {
                *text = redacted;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_image_uris_in_value(item);
            }
        }
        serde_json::Value::Object(map) => {
            for (_, item) in map.iter_mut() {
                redact_image_uris_in_value(item);
            }
        }
        _ => {}
    }
}

/// Vision text that becomes model-visible or logged — the successful
/// `VisionOutcome.text` (round-3 #3) and error bodies/messages (review #3,
/// round-2 #2, round-3 #4): image URIs redacted via [`redact_image_uris`], then
/// UTF-8-safely capped so only a bounded, image-free, token-free remainder
/// survives.
pub fn redact_vision_text(text: &str) -> String {
    let trimmed = redact_image_uris(text);
    let trimmed = trimmed.trim();
    if trimmed.chars().count() > VISION_TEXT_REDACT_LIMIT {
        let end = trimmed
            .char_indices()
            .nth(VISION_TEXT_REDACT_LIMIT)
            .map(|(idx, _)| idx)
            .unwrap_or(trimmed.len());
        format!("{}…[truncated]", &trimmed[..end])
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn cache() -> ImageCache {
        ImageCache::new(100, Duration::from_secs(300))
    }

    fn img(url: &str) -> CachedImage {
        CachedImage {
            image_url: url.to_string(),
            detail: None,
        }
    }

    fn user_with(content: Vec<ContentItem>) -> ResponseItem {
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content,
            phase: None,
        }
    }

    fn input_image(url: &str) -> ContentItem {
        ContentItem::InputImage {
            image_url: Some(url.to_string()),
            file_id: None,
            detail: None,
        }
    }

    fn base_request(input: Vec<ResponseItem>) -> ResponsesRequest {
        let mut req: ResponsesRequest =
            serde_json::from_value(serde_json::json!({ "model": "glm-5.1" })).expect("request");
        req.input = input;
        req
    }

    #[test]
    fn cache_store_and_retrieve_same_session() {
        let cache = cache();
        let key = ImageCache::image_key("sess", "1");
        cache.store("sess", key.clone(), img("data:a"));
        assert_eq!(cache.get("sess", &key), Some(img("data:a")));
    }

    #[test]
    fn cache_sessions_are_isolated() {
        let cache = cache();
        cache.store("a", ImageCache::image_key("a", "1"), img("data:a"));
        cache.store("b", ImageCache::image_key("b", "1"), img("data:b"));
        assert_eq!(
            cache.get("a", &ImageCache::image_key("a", "1")),
            Some(img("data:a"))
        );
        assert_eq!(
            cache.get("b", &ImageCache::image_key("b", "1")),
            Some(img("data:b"))
        );
    }

    #[test]
    fn cache_missing_session_and_key_return_none() {
        let cache = cache();
        assert_eq!(cache.get("nope", "Image#1"), None);
        cache.store("x", ImageCache::image_key("x", "1"), img("data:x"));
        assert_eq!(cache.get("x", &ImageCache::image_key("x", "9")), None);
    }

    #[test]
    fn cache_lru_eviction_on_max_size() {
        let cache = ImageCache::new(3, Duration::from_secs(300));
        for n in 1..=4 {
            cache.store(
                "s",
                ImageCache::image_key("s", &n.to_string()),
                img(&format!("d{n}")),
            );
        }
        // Oldest (#1) evicted.
        assert_eq!(cache.get("s", &ImageCache::image_key("s", "1")), None);
        assert_eq!(
            cache.get("s", &ImageCache::image_key("s", "2")),
            Some(img("d2"))
        );
        assert_eq!(
            cache.get("s", &ImageCache::image_key("s", "4")),
            Some(img("d4"))
        );
    }

    #[test]
    fn cache_lru_access_prevents_eviction() {
        let cache = ImageCache::new(3, Duration::from_secs(300));
        for n in 1..=3 {
            cache.store(
                "s",
                ImageCache::image_key("s", &n.to_string()),
                img(&format!("d{n}")),
            );
        }
        // Touch #1 so #2 becomes the oldest.
        assert!(cache.get("s", &ImageCache::image_key("s", "1")).is_some());
        cache.store("s", ImageCache::image_key("s", "4"), img("d4"));
        assert_eq!(
            cache.get("s", &ImageCache::image_key("s", "1")),
            Some(img("d1"))
        );
        assert_eq!(cache.get("s", &ImageCache::image_key("s", "2")), None);
    }

    #[test]
    fn cache_eviction_is_per_session() {
        let cache = ImageCache::new(2, Duration::from_secs(300));
        cache.store("a", ImageCache::image_key("a", "1"), img("a1"));
        cache.store("a", ImageCache::image_key("a", "2"), img("a2"));
        cache.store("b", ImageCache::image_key("b", "1"), img("b1"));
        cache.store("b", ImageCache::image_key("b", "2"), img("b2"));
        cache.store("a", ImageCache::image_key("a", "3"), img("a3"));
        assert_eq!(cache.get("a", &ImageCache::image_key("a", "1")), None);
        assert_eq!(
            cache.get("a", &ImageCache::image_key("a", "2")),
            Some(img("a2"))
        );
        assert_eq!(
            cache.get("b", &ImageCache::image_key("b", "1")),
            Some(img("b1"))
        );
        assert_eq!(
            cache.get("b", &ImageCache::image_key("b", "2")),
            Some(img("b2"))
        );
    }

    #[test]
    fn cache_ttl_expiry_returns_none() {
        let cache = ImageCache::new(10, Duration::from_secs(0));
        cache.store("s", ImageCache::image_key("s", "1"), img("d1"));
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(cache.get("s", &ImageCache::image_key("s", "1")), None);
    }

    #[test]
    fn cache_cleanup_removes_empty_sessions() {
        let cache = ImageCache::new(10, Duration::from_secs(0));
        cache.store("temp", ImageCache::image_key("temp", "1"), img("d1"));
        std::thread::sleep(Duration::from_millis(2));
        // A get triggers cleanup; the now-empty session must be dropped.
        let _ = cache.get("temp", &ImageCache::image_key("temp", "1"));
        assert_eq!(cache.session_len("temp"), 0);
    }

    #[test]
    fn latest_user_detection_true_for_image_in_last_user() {
        let input = vec![
            user_with(vec![ContentItem::InputText { text: "hi".into() }]),
            ResponseItem::message_text("assistant", "hello"),
            user_with(vec![
                ContentItem::InputText { text: "see".into() },
                input_image("data:img"),
            ]),
        ];
        assert!(latest_user_message_has_images(&input));
    }

    #[test]
    fn latest_user_detection_false_for_only_old_images() {
        let input = vec![
            user_with(vec![input_image("data:old")]),
            ResponseItem::message_text("assistant", "I saw it"),
            user_with(vec![ContentItem::InputText {
                text: "thanks".into(),
            }]),
        ];
        assert!(!latest_user_message_has_images(&input));
    }

    #[test]
    fn latest_user_detection_false_without_images_or_users() {
        assert!(!latest_user_message_has_images(&[]));
        let input = vec![ResponseItem::message_text("assistant", "hi")];
        assert!(!latest_user_message_has_images(&input));
        let empty_image = vec![user_with(vec![ContentItem::InputImage {
            image_url: None,
            file_id: Some("f".into()),
            detail: None,
        }])];
        assert!(!latest_user_message_has_images(&empty_image));
    }

    #[test]
    fn strip_replaces_images_with_ordered_placeholders() {
        let cache = cache();
        let mut req = base_request(vec![user_with(vec![
            ContentItem::InputText {
                text: "Look at".into(),
            },
            input_image("data:img1"),
            ContentItem::InputText { text: "and".into() },
            input_image("data:img2"),
        ])]);
        cache.strip_and_cache_images(&mut req, "sess");
        let ResponseItem::Message { content, .. } = &req.input[0] else {
            panic!("expected message");
        };
        assert!(matches!(&content[0], ContentItem::InputText { text } if text == "Look at"));
        let ContentItem::InputText { text } = &content[1] else {
            panic!("expected placeholder");
        };
        assert!(text.contains("[Image #1]"));
        assert!(text.contains("analyzeImage(imageId=[\"1\"])"));
        let ContentItem::InputText { text } = &content[3] else {
            panic!("expected placeholder");
        };
        assert!(text.contains("[Image #2]"));
        // Originals cached under session-scoped keys.
        assert_eq!(
            cache.get("sess", &ImageCache::image_key("sess", "1")),
            Some(img("data:img1"))
        );
        assert_eq!(
            cache.get("sess", &ImageCache::image_key("sess", "2")),
            Some(img("data:img2"))
        );
    }

    #[test]
    fn strip_injects_system_prompt_and_tool_once() {
        let cache = cache();
        let mut req = base_request(vec![user_with(vec![input_image("data:img")])]);
        cache.strip_and_cache_images(&mut req, "sess");
        assert!(req.instructions.starts_with(IMAGE_AGENT_SYSTEM_PROMPT));
        let analyze_count = req
            .tools
            .iter()
            .filter(|t| tool_is_analyze_image(t))
            .count();
        assert_eq!(analyze_count, 1);
    }

    #[test]
    fn strip_preserves_existing_instructions() {
        let cache = cache();
        let mut req = base_request(vec![user_with(vec![input_image("data:img")])]);
        req.instructions = "Be terse.".to_string();
        cache.strip_and_cache_images(&mut req, "sess");
        assert!(req.instructions.starts_with(IMAGE_AGENT_SYSTEM_PROMPT));
        assert!(req.instructions.ends_with("Be terse."));
    }

    #[test]
    fn strip_does_not_duplicate_existing_analyze_tool() {
        let cache = cache();
        let mut req = base_request(vec![user_with(vec![input_image("data:img")])]);
        req.tools = vec![analyze_image_tool_spec()];
        cache.strip_and_cache_images(&mut req, "sess");
        let analyze_count = req
            .tools
            .iter()
            .filter(|t| tool_is_analyze_image(t))
            .count();
        assert_eq!(analyze_count, 1);
    }

    #[test]
    fn strip_replaces_conflicting_caller_analyze_schema_with_canonical() {
        // A caller-supplied analyzeImage with a conflicting schema (no imageId)
        // must be REPLACED by the canonical spec on activation (review #4), so
        // the model is told to pass imageId after images were stripped.
        let cache = cache();
        let mut req = base_request(vec![user_with(vec![input_image("data:img")])]);
        req.tools = vec![ToolSpec::Function {
            name: "analyzeImage".to_string(),
            description: "caller's own bogus tool".to_string(),
            strict: false,
            parameters: serde_json::json!({
                "type": "object",
                "properties": { "q": { "type": "string" } },
                "required": ["q"]
            }),
        }];
        cache.strip_and_cache_images(&mut req, "sess");
        let analyze: Vec<&ToolSpec> = req
            .tools
            .iter()
            .filter(|t| tool_is_analyze_image(t))
            .collect();
        assert_eq!(
            analyze.len(),
            1,
            "exactly one analyzeImage after replace+dedup"
        );
        let ToolSpec::Function {
            description,
            parameters,
            ..
        } = analyze[0]
        else {
            panic!("expected function tool");
        };
        assert_eq!(description, ANALYZE_IMAGE_TOOL_DESCRIPTION);
        assert_eq!(parameters, &analyze_image_tool_parameters());
        // Canonical schema requires imageId, not the caller's `q`.
        assert_eq!(
            parameters["required"],
            serde_json::json!(["imageId", "task"])
        );
    }

    #[test]
    fn strip_replaces_namespaced_caller_analyze_tool() {
        // Round-2 #3: a NAMESPACED analyzeImage child must also be removed (it
        // would otherwise lower to a chat tool of the same name and collide with
        // the appended canonical tool). The now-empty namespace is dropped.
        let cache = cache();
        let mut req = base_request(vec![user_with(vec![input_image("data:img")])]);
        req.tools = vec![
            ToolSpec::Namespace {
                name: "mcp".to_string(),
                description: "an mcp server".to_string(),
                tools: vec![crate::models::responses::NamespaceToolSpec::Function {
                    name: "analyzeImage".to_string(),
                    description: "namespaced bogus analyzeImage".to_string(),
                    strict: false,
                    parameters: serde_json::json!({ "type": "object", "properties": {} }),
                }],
            },
            ToolSpec::Namespace {
                name: "tools".to_string(),
                description: "another server".to_string(),
                tools: vec![crate::models::responses::NamespaceToolSpec::Function {
                    name: "keep_me".to_string(),
                    description: "unrelated".to_string(),
                    strict: false,
                    parameters: serde_json::json!({ "type": "object", "properties": {} }),
                }],
            },
        ];
        cache.strip_and_cache_images(&mut req, "sess");
        // Exactly one canonical top-level analyzeImage; no namespaced survivor.
        let analyze_top = req
            .tools
            .iter()
            .filter(|t| tool_is_analyze_image(t))
            .count();
        assert_eq!(analyze_top, 1);
        // The empty "mcp" namespace was dropped; "tools" (with keep_me) survives.
        let namespaces: Vec<&str> = req
            .tools
            .iter()
            .filter_map(|t| match t {
                ToolSpec::Namespace { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(namespaces, vec!["tools"]);
        // Lowering must NOT error on a duplicate `analyzeImage` name.
        let lowered = crate::adapters::responses_to_chat::lower_request_with_options(
            &req,
            Vec::new(),
            &crate::config::default_reasoning_effort(),
            true,
        );
        assert!(
            lowered.is_ok(),
            "no duplicate-tool error after namespaced replace"
        );
    }

    #[test]
    fn redact_vision_text_strips_data_uris_and_caps_length() {
        let body = "error before data:image/png;base64,AAAABBBBCCCC after";
        let redacted = redact_vision_text(body);
        assert!(
            !redacted.contains("AAAABBBBCCCC"),
            "base64 payload stripped"
        );
        assert!(!redacted.contains("data:image"), "data uri stripped");
        assert!(redacted.contains("<redacted uri>"));
        assert!(redacted.contains("error before"));
        assert!(redacted.contains("after"));

        // UTF-8-safe length cap (limit + the truncation marker).
        let long = "x".repeat(VISION_TEXT_REDACT_LIMIT + 500);
        let capped = redact_vision_text(&long);
        assert!(capped.ends_with("…[truncated]"));
        assert!(
            capped.chars().count() <= VISION_TEXT_REDACT_LIMIT + "…[truncated]".chars().count()
        );
        // Truncation lands on a char boundary even with multi-byte content.
        let multibyte = "é".repeat(VISION_TEXT_REDACT_LIMIT + 10);
        let capped_mb = redact_vision_text(&multibyte);
        assert!(capped_mb.is_char_boundary(capped_mb.len()));

        // A JSON error message embedding a data URL inside quotes is bounded.
        let json_err = "{\"detail\":\"bad input data:image/jpeg;base64,ZZZZ\"}";
        let r = redact_vision_text(json_err);
        assert!(!r.contains("ZZZZ"));
    }

    #[test]
    fn redact_vision_text_is_case_insensitive_and_strips_http_image_urls() {
        // Round-2 #2: uppercase DATA: and http(s) image URLs with signed-URL
        // query tokens must all be redacted.
        let upper = "oops DATA:IMAGE/PNG;BASE64,SECRETPAYLOAD trailing";
        let r = redact_vision_text(upper);
        assert!(!r.contains("SECRETPAYLOAD"), "uppercase data: stripped");
        assert!(r.contains("<redacted uri>"));
        assert!(r.contains("trailing"));

        let signed =
            "fetch failed for https://cdn.example.com/img.png?sig=ABCSECRET123&exp=999 oh no";
        let r = redact_vision_text(signed);
        assert!(!r.contains("ABCSECRET123"), "signed-url token stripped");
        assert!(!r.contains("cdn.example.com"), "image host stripped");
        assert!(r.contains("fetch failed for"));
        assert!(r.contains("oh no"));

        let mixed_case_http = "HTTPS://Host/Path?token=ZZZ done";
        let r = redact_vision_text(mixed_case_http);
        assert!(!r.contains("ZZZ"), "uppercase https stripped");
        assert!(r.contains("done"));
    }

    #[test]
    fn redact_vision_text_strips_json_escaped_signed_urls() {
        // Round-3 #4: a raw non-2xx body often contains JSON-escaped slashes
        // (`https:\/\/...`); the escaped form must be redacted too.
        let escaped =
            r#"{"error":"could not load https:\/\/cdn.example.com\/i.png?sig=ESCAPEDTOKEN&x=1"}"#;
        let r = redact_vision_text(escaped);
        assert!(
            !r.contains("ESCAPEDTOKEN"),
            "escaped signed-url token stripped"
        );
        assert!(
            !r.contains("cdn.example.com"),
            "escaped image host stripped"
        );
        assert!(r.contains("<redacted uri>"));
        assert!(r.contains("could not load"));

        // Escaped http (no TLS) too, uppercase scheme.
        let escaped_http = r#"HTTP:\/\/host\/p?tok=ABC end"#;
        let r = redact_vision_text(escaped_http);
        assert!(!r.contains("ABC"), "escaped http token stripped");
        assert!(r.contains("end"));

        // A successful description echoing a data URL is redacted the same way.
        let success = "Here is your image data:image/png;base64,REALPAYLOAD and analysis.";
        let r = redact_vision_text(success);
        assert!(!r.contains("REALPAYLOAD"));
        assert!(r.contains("and analysis."));
    }

    #[test]
    fn redact_image_uris_in_value_strips_nested_image_fields() {
        // Round-4 #2/#3: the shared JSON redactor used by inbound trace + upstream
        // JSONL must strip data:/signed image URLs anywhere in the body —
        // including nested content-part arrays and object fields — by VALUE, not
        // by key name, leaving non-image content intact.
        let mut value = serde_json::json!({
            "model": "glm-5.1",
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "keep this prose" },
                    { "type": "image_url", "image_url": { "url": "data:image/png;base64,NESTEDLEAK" } },
                    { "type": "image_url", "image_url": { "url": "https://cdn.x/i.png?sig=SIGLEAK" } }
                ]
            }],
            "note": "see https:\\/\\/cdn.x\\/e.png?tok=ESCLEAK now"
        });
        redact_image_uris_in_value(&mut value);
        let dumped = serde_json::to_string(&value).expect("serialize");
        assert!(
            !dumped.contains("NESTEDLEAK"),
            "nested data: payload stripped"
        );
        assert!(
            !dumped.contains("SIGLEAK"),
            "nested signed-url token stripped"
        );
        assert!(
            !dumped.contains("ESCLEAK"),
            "escaped signed-url token stripped"
        );
        assert!(!dumped.contains("cdn.x"), "image host stripped");
        assert!(
            dumped.contains("keep this prose"),
            "non-image text preserved"
        );
        assert!(dumped.contains("glm-5.1"), "model preserved");
        assert!(dumped.contains("<redacted uri>"));
    }

    #[test]
    fn redact_image_uris_handles_multibyte_around_uris() {
        // Round-5: the core redactor walks UNTRUSTED text; multibyte chars
        // adjacent to / before / inside a URI must not panic and must preserve
        // non-image content. `é`/`☕`/`café` straddle byte boundaries near the
        // `data:`/`http` scan points.
        let text = "café ☕ before data:image/png;base64,PAYLÖAD/é+= after — déjà vu";
        let r = redact_image_uris(text);
        assert!(
            !r.contains("PAYL"),
            "base64 payload (with multibyte) stripped"
        );
        assert!(r.contains("<redacted uri>"));
        assert!(r.contains("café ☕ before "));
        assert!(r.contains(" after — déjà vu"));

        // A multibyte run immediately preceding `https://` must not corrupt the
        // boundary handling.
        let text2 = "señor https://hôst.x/p?tok=ZZZé done ☕";
        let r2 = redact_image_uris(text2);
        assert!(!r2.contains("ZZZ"), "signed-url token stripped");
        assert!(r2.contains("señor "));
        assert!(r2.contains(" done ☕"));

        // Pure multibyte with no URI is returned intact (no panic, no change).
        let plain = "完全に日本語のテキスト ☕ café";
        assert_eq!(redact_image_uris(plain), plain);
    }

    #[test]
    fn strip_preserves_non_user_messages_and_empty_content() {
        let cache = cache();
        let mut req = base_request(vec![
            ResponseItem::message_text("assistant", "I see it"),
            user_with(vec![]),
            user_with(vec![input_image("data:img")]),
        ]);
        cache.strip_and_cache_images(&mut req, "sess");
        assert!(matches!(&req.input[0], ResponseItem::Message { role, .. } if role == "assistant"));
        let ResponseItem::Message { content, .. } = &req.input[1] else {
            panic!("expected message");
        };
        assert!(content.is_empty());
    }

    #[test]
    fn strip_renumbers_across_all_user_messages() {
        // History resent with raw images across two user turns must renumber
        // 1,2,3 sequentially so no raw bytes survive in any user message.
        let cache = cache();
        let mut req = base_request(vec![
            user_with(vec![input_image("data:a"), input_image("data:b")]),
            ResponseItem::message_text("assistant", "ok"),
            user_with(vec![input_image("data:c")]),
        ]);
        cache.strip_and_cache_images(&mut req, "sess");
        let collect_placeholders = |item: &ResponseItem| -> Vec<String> {
            let ResponseItem::Message { content, .. } = item else {
                return Vec::new();
            };
            content
                .iter()
                .filter_map(|c| match c {
                    ContentItem::InputText { text } if text.contains("[Image #") => {
                        Some(text.clone())
                    }
                    _ => None,
                })
                .collect()
        };
        let first = collect_placeholders(&req.input[0]);
        let third = collect_placeholders(&req.input[2]);
        assert!(first[0].contains("[Image #1]"));
        assert!(first[1].contains("[Image #2]"));
        assert!(third[0].contains("[Image #3]"));
        assert_eq!(cache.session_len("sess"), 3);
    }

    #[test]
    fn strip_clears_session_so_multi_turn_numbering_resets() {
        // Turn 1: two images -> #1, #2. Turn 2 (fresh request, history carries
        // placeholders, one NEW image) -> cache cleared, new image becomes #1.
        let cache = cache();
        let mut turn1 = base_request(vec![user_with(vec![
            input_image("data:t1a"),
            input_image("data:t1b"),
        ])]);
        cache.strip_and_cache_images(&mut turn1, "sess");
        assert_eq!(cache.session_len("sess"), 2);

        let mut turn2 = base_request(vec![
            // Turn 1 history already stripped to placeholders (text only).
            user_with(vec![
                ContentItem::InputText {
                    text: image_placeholder_text(1),
                },
                ContentItem::InputText {
                    text: image_placeholder_text(2),
                },
            ]),
            ResponseItem::message_text("assistant", "analyzing"),
            user_with(vec![input_image("data:t2new")]),
        ]);
        cache.strip_and_cache_images(&mut turn2, "sess");
        // Only the new image remains, renumbered to #1.
        assert_eq!(cache.session_len("sess"), 1);
        assert_eq!(
            cache.get("sess", &ImageCache::image_key("sess", "1")),
            Some(img("data:t2new"))
        );
        assert_eq!(cache.get("sess", &ImageCache::image_key("sess", "2")), None);
    }

    #[test]
    fn vision_request_parses_arguments_and_resolves_images() {
        let cache = cache();
        cache.store("sess", ImageCache::image_key("sess", "1"), img("data:one"));
        cache.store("sess", ImageCache::image_key("sess", "2"), img("data:two"));
        let args = json!({
            "imageId": ["1", "2", "9"],
            "task": "read the sign",
            "context": "user asked about the sign"
        });
        let request = VisionRequest::from_arguments(&args, "sess", &cache);
        assert_eq!(request.image_ids, vec!["1", "2", "9"]);
        assert_eq!(request.task, "read the sign");
        assert_eq!(
            request.context.as_deref(),
            Some("user asked about the sign")
        );
        // #9 missing from cache, so only two images resolve.
        assert_eq!(request.images, vec![img("data:one"), img("data:two")]);
    }

    #[test]
    fn vision_request_defaults_task_and_accepts_numeric_ids() {
        let cache = cache();
        let args = json!({ "imageId": [1] });
        let request = VisionRequest::from_arguments(&args, "sess", &cache);
        assert_eq!(request.image_ids, vec!["1"]);
        assert_eq!(request.task, "Describe this image in detail");
        assert_eq!(request.context, None);
    }

    #[test]
    fn reqwest_vision_client_builds_body_with_image_parts() {
        let config = crate::config::Config::from_persisted(&crate::config::PersistedConfig {
            vision_url: Some("http://127.0.0.1:9000/v1/chat/completions".to_string()),
            vision_model: Some("vision-model".to_string()),
            ..crate::config::PersistedConfig::default()
        })
        .expect("config");
        let client = ReqwestVisionClient::new(reqwest::Client::new(), &config);
        let request = VisionRequest {
            image_ids: vec!["1".to_string()],
            task: "describe".to_string(),
            context: Some("ctx".to_string()),
            images: vec![CachedImage {
                image_url: "data:image/png;base64,AAAA".to_string(),
                detail: Some("high".to_string()),
            }],
        };
        let body = client.build_body(&request);
        assert_eq!(body["model"], "vision-model");
        assert_eq!(body["stream"], false);
        let user = &body["messages"][1];
        assert_eq!(user["role"], "user");
        assert_eq!(user["content"][0]["type"], "image_url");
        assert_eq!(
            user["content"][0]["image_url"]["url"],
            "data:image/png;base64,AAAA"
        );
        assert_eq!(user["content"][0]["image_url"]["detail"], "high");
        let prompt = user["content"][1]["text"].as_str().unwrap();
        assert!(prompt.contains("Task: describe"));
        assert!(prompt.contains("Context: ctx"));
    }
}
