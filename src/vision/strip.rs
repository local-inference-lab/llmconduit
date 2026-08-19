//! Request-mutation seam for the image agent: strip images to `[Image #N]`
//! placeholders, cache the originals, and inject the `analyzeImage` tool +
//! system-prompt instruction.
//!
//! This is the SINGLE place a [`ResponsesRequest`] is rewritten for the image
//! agent. It runs BEFORE replay/lowering (`Gateway::stream_responses`) so replay
//! hashes only ever see placeholder text, never image bytes. The `analyzeImage`
//! tool schema lives here alongside the [`ImageCache::strip_and_cache_images`]
//! method that ties them together.

use crate::models::responses::ContentItem;
use crate::models::responses::ResponseItem;
use crate::models::responses::ResponsesRequest;
use crate::models::responses::ToolSpec;
use serde_json::Value;
use serde_json::json;

use super::cache::CachedImage;
use super::cache::ImageCache;

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

impl ImageCache {
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
    /// The engine only calls this once it has decided the image agent is active
    /// for the turn. We renumber every user-message image (not just the latest)
    /// because the history the client resends still contains earlier raw images
    /// that would otherwise leak bytes upstream.
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
/// the appended canonical tool (`build_tool_registry` rejects duplicate names,
/// case-insensitively). A
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

// ===========================================================================
// E2b — residual-image safety pass.
//
// The strip above only ever touches `role=="user"` + `image_url` content in the
// process of caching+offloading an image for `analyzeImage`. That leaves gaps
// that would reach the text-only upstream verbatim: `file_id`-only images
// (Anthropic `source.type=="file"`, `anthropic_to_responses.rs:712`) and images
// sitting in a non-`user` role. This role-agnostic sweep catches them, and is
// the LAST thing standing between a canonical request and a backend that will
// 400 on raw image bytes (the field incident's "is not a multimodal model"
// error, which also tripped a cooldown before E2a). See `engine.rs`'s call site
// (after `activate_image_agent`, run only when the image agent is active) for
// the policy dispatch (`Placeholder` vs `Reject`), keyed on the profile's
// `image_analysis.residual_images`.
// ===========================================================================

/// E2b: whether `part` is a residual `ContentItem::InputImage` this pass must
/// not let reach a non-native-vision backend — ANY `InputImage`, regardless of
/// whether `image_url`/`file_id` are populated, empty, or absent.
///
/// Deliberately NOT gated on non-emptiness: lowering (`content_item_to_chat_part`)
/// dispatches purely on the SHAPE of `InputImage` — `image_url: Some(_)`
/// (even `Some("")`) still lowers to a `{"type":"image_url",...}` chat part,
/// and `image_url: None` still lowers to `{"type":"input_image",...}`. A
/// text-only backend 400s on the PRESENCE of a multimodal content-part type,
/// not specifically on whether it carries real bytes, so a structurally-
/// present-but-empty `InputImage` is exactly as unsafe to forward as a
/// populated one (Codex xhigh review round 1, HIGH finding).
fn is_residual_input_image(part: &ContentItem) -> bool {
    matches!(part, ContentItem::InputImage { .. })
}

/// E2b: the fixed placeholder for an image that is the result of a TOOL call
/// rather than something the user attached directly — no `{n}` interpolation,
/// so it is trivially byte-identical across turns. See
/// [`message_follows_tool_output`] for how a message is classified as a
/// tool-output continuation.
const RESIDUAL_TOOL_IMAGE_PLACEHOLDER: &str = "[the tool returned an image, which this text-only model cannot view. Do NOT call the same tool again for image-only output; request text output or ask the user what it shows. Do NOT fabricate its contents.]";

/// E2b: the default placeholder for a residual image, used for every
/// image-bearing message that is NOT a tool-output continuation (regardless
/// of the message's own role). `count` is the number of images in THAT
/// message alone, in wire order — never a running/global counter — so the
/// text is byte-identical across turns for the same message shape (claude-cli
/// resends the same history/images every turn; determinism is load-bearing
/// for replay-cache safety, see `AGENTS.md`).
fn residual_user_image_placeholder_text(count: usize) -> String {
    format!(
        "[image omitted — this model is text-only and cannot view images. {count} image(s) were attached here. Do NOT guess their contents; ask the user to describe them or provide text.]"
    )
}

/// E2b: whether the `ResponseItem` at `index` is a tool-output continuation —
/// detected positionally, the only signal the canonical shape carries.
/// `adapters/anthropic_to_responses.rs` lowers an Anthropic `tool_result`
/// image to a `FunctionCallOutput` (the tool's text stub, e.g.
/// `"[image returned]"`) immediately followed by a synthetic `role: "user"`
/// message carrying the actual image content — there is no field on either
/// item marking the association. A message whose immediate predecessor in
/// `input` is a `FunctionCallOutput` is therefore treated as carrying
/// tool-output images, regardless of its own role.
fn message_follows_tool_output(input: &[ResponseItem], index: usize) -> bool {
    index > 0 && matches!(input[index - 1], ResponseItem::FunctionCallOutput { .. })
}

/// E2b: whether `input` still carries at least one residual image. Read-only
/// — used by the `Reject` policy to detect an offending turn BEFORE any
/// mutation, so a rejected request is never partially rewritten before the
/// turn fails.
pub fn has_residual_images(input: &[ResponseItem]) -> bool {
    input.iter().any(|item| match item {
        ResponseItem::Message { content, .. } => content.iter().any(is_residual_input_image),
        _ => false,
    })
}

/// E2b residual-image safety pass (`Placeholder` policy) — see the module
/// section header above for why this is the true choke point. Sweeps EVERY
/// `ResponseItem::Message` in `input` (any role, any position — role-agnostic)
/// and replaces each residual `InputImage` IN PLACE with a stable text
/// placeholder, preserving message position/count and leaving sibling
/// text/`FunctionCallOutput` content untouched. Trivially safe against
/// double-transformation: an image the active G4 agent already stripped is no
/// longer an `InputImage` (it was already rewritten to `InputText`) by the
/// time this runs, so there is nothing left here to re-transform.
///
/// Returns the number of images replaced (`0` ⇒ nothing to do, so the caller
/// can skip forcing a replay-cache bypass).
pub fn degrade_residual_images(input: &mut [ResponseItem]) -> usize {
    // Positional tool-output classification needs to look at each item's
    // PREDECESSOR, so compute it as one immutable O(n) pass BEFORE taking any
    // mutable borrows below — avoids overlapping-borrow gymnastics entirely.
    let follows_tool_output: Vec<bool> = (0..input.len())
        .map(|index| message_follows_tool_output(input, index))
        .collect();
    let mut degraded = 0usize;
    for (index, item) in input.iter_mut().enumerate() {
        let ResponseItem::Message { content, .. } = item else {
            continue;
        };
        // Wire-order count for THIS message, taken before any replacement so
        // `{n}` reflects the message's true original image count.
        let count = content
            .iter()
            .filter(|part| is_residual_input_image(part))
            .count();
        if count == 0 {
            continue;
        }
        let placeholder = if follows_tool_output[index] {
            RESIDUAL_TOOL_IMAGE_PLACEHOLDER.to_string()
        } else {
            residual_user_image_placeholder_text(count)
        };
        for part in content.iter_mut() {
            if is_residual_input_image(part) {
                *part = ContentItem::InputText {
                    text: placeholder.clone(),
                };
            }
        }
        degraded += count;
    }
    degraded
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn cache() -> ImageCache {
        ImageCache::new(100, std::time::Duration::from_secs(300))
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
        let lowered = crate::adapters::responses_to_chat::lower_request_with_image_agent(
            &req,
            Vec::new(),
            true,
        );
        assert!(
            lowered.is_ok(),
            "no duplicate-tool error after namespaced replace"
        );
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

    // -----------------------------------------------------------------
    // E2b — `degrade_residual_images` / `has_residual_images`.
    // -----------------------------------------------------------------

    fn file_image(file_id: &str) -> ContentItem {
        ContentItem::InputImage {
            image_url: None,
            file_id: Some(file_id.to_string()),
            detail: None,
        }
    }

    fn tool_output(call_id: &str) -> ResponseItem {
        ResponseItem::FunctionCallOutput {
            call_id: call_id.to_string(),
            output: serde_json::json!("[image returned]"),
        }
    }

    #[test]
    fn degrade_replaces_user_image_url_with_default_wording() {
        let mut input = vec![user_with(vec![
            ContentItem::InputText {
                text: "look".into(),
            },
            input_image("data:raw-bytes"),
        ])];
        let degraded = degrade_residual_images(&mut input);
        assert_eq!(degraded, 1);
        let ResponseItem::Message { content, .. } = &input[0] else {
            panic!("expected message");
        };
        assert!(matches!(&content[0], ContentItem::InputText { text } if text == "look"));
        let ContentItem::InputText { text } = &content[1] else {
            panic!("image must become InputText");
        };
        assert!(text.contains("this model is text-only and cannot view images"));
        assert!(text.contains("1 image(s) were attached here"));
        // No raw bytes survive.
        assert!(!text.contains("data:raw-bytes"));
    }

    #[test]
    fn degrade_replaces_file_id_image() {
        // `file_id` images are the ACTIVE strip's blind spot (it only matches
        // `image_url: Some`) — this pass must still catch them.
        let mut input = vec![user_with(vec![file_image("file-abc123")])];
        let degraded = degrade_residual_images(&mut input);
        assert_eq!(degraded, 1);
        let ResponseItem::Message { content, .. } = &input[0] else {
            panic!("expected message");
        };
        let ContentItem::InputText { text } = &content[0] else {
            panic!("file_id image must become InputText");
        };
        assert!(!text.contains("file-abc123"));
        assert!(text.contains("text-only"));
    }

    #[test]
    fn degrade_replaces_image_in_non_user_message() {
        // Role-agnostic: an assistant/system-role message with a residual
        // image must be swept too, not just `role=="user"`.
        let mut input = vec![ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![input_image("data:assistant-image")],
            phase: None,
        }];
        let degraded = degrade_residual_images(&mut input);
        assert_eq!(degraded, 1);
        let ResponseItem::Message { content, .. } = &input[0] else {
            panic!("expected message");
        };
        assert!(matches!(&content[0], ContentItem::InputText { .. }));
    }

    #[test]
    fn degrade_uses_tool_output_wording_immediately_after_function_call_output() {
        // Mirrors `anthropic_to_responses.rs`'s tool_result-image lowering:
        // FunctionCallOutput text stub immediately followed by a synthetic
        // `role: "user"` message carrying the image.
        let mut input = vec![
            tool_output("toolu_1"),
            user_with(vec![input_image("data:tool-image")]),
        ];
        let degraded = degrade_residual_images(&mut input);
        assert_eq!(degraded, 1);
        let ResponseItem::Message { content, .. } = &input[1] else {
            panic!("expected message");
        };
        let ContentItem::InputText { text } = &content[0] else {
            panic!("expected placeholder");
        };
        assert_eq!(text, RESIDUAL_TOOL_IMAGE_PLACEHOLDER);
        assert!(!text.contains("ask the user to describe"));
    }

    #[test]
    fn degrade_uses_default_wording_when_not_after_function_call_output() {
        // A plain user image (no preceding FunctionCallOutput) must NOT get
        // the tool-output wording, even though both are role=="user".
        let mut input = vec![user_with(vec![input_image("data:plain")])];
        degrade_residual_images(&mut input);
        let ResponseItem::Message { content, .. } = &input[0] else {
            panic!("expected message");
        };
        let ContentItem::InputText { text } = &content[0] else {
            panic!("expected placeholder");
        };
        assert_ne!(text, RESIDUAL_TOOL_IMAGE_PLACEHOLDER);
        assert!(text.contains("ask the user to describe"));
    }

    #[test]
    fn degrade_counts_images_per_message_in_wire_order_not_globally() {
        let mut input = vec![
            user_with(vec![input_image("data:a"), input_image("data:b")]),
            ResponseItem::message_text("assistant", "ok"),
            user_with(vec![input_image("data:c")]),
        ];
        let degraded = degrade_residual_images(&mut input);
        assert_eq!(degraded, 3);
        let ResponseItem::Message { content: first, .. } = &input[0] else {
            panic!("expected message");
        };
        for part in first {
            let ContentItem::InputText { text } = part else {
                panic!("expected placeholder");
            };
            // Both images in the FIRST message share that message's own
            // count (2), not a running/global counter.
            assert!(text.contains("2 image(s) were attached here"), "{text}");
        }
        let ResponseItem::Message { content: third, .. } = &input[2] else {
            panic!("expected message");
        };
        let ContentItem::InputText { text } = &third[0] else {
            panic!("expected placeholder");
        };
        assert!(text.contains("1 image(s) were attached here"));
    }

    #[test]
    fn degrade_two_distinct_images_at_same_position_collapse_to_identical_text() {
        // AC-5: two DIFFERENT images (different bytes) at the same message
        // position must produce byte-identical placeholder text -- this is
        // exactly why a degraded turn must bypass the replay cache (proven at
        // the engine/gateway level; this test pins the byte-identity half).
        let mut first = vec![user_with(vec![input_image("data:image-one")])];
        let mut second = vec![user_with(vec![input_image("data:completely-different")])];
        degrade_residual_images(&mut first);
        degrade_residual_images(&mut second);
        assert_eq!(
            serde_json::to_value(&first).unwrap(),
            serde_json::to_value(&second).unwrap(),
            "distinct images at the same position must degrade to byte-identical text"
        );
    }

    #[test]
    fn degrade_is_noop_when_no_residual_images_present() {
        // Already-stripped (InputText) content, and content with no images at
        // all, must be left completely untouched (0 returned).
        let mut input = vec![
            user_with(vec![ContentItem::InputText {
                text: "[Image #1] already stripped by the active agent".into(),
            }]),
            ResponseItem::message_text("assistant", "hi"),
        ];
        let before = input.clone();
        let degraded = degrade_residual_images(&mut input);
        assert_eq!(degraded, 0);
        assert_eq!(
            serde_json::to_value(&input).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
    }

    #[test]
    fn degrade_degrades_empty_or_absent_image_url_and_file_id() {
        // Codex xhigh review round 1 (HIGH): lowering dispatches on the SHAPE
        // of `InputImage`, not on whether its fields are populated --
        // `image_url: Some("")` still lowers to a `{"type":"image_url",...}`
        // chat part, and a fully empty `InputImage` still lowers to
        // `{"type":"input_image",...}`. Either would still present as a
        // multimodal content part to a text-only backend, so BOTH must be
        // treated as residual, not skipped as "nothing to leak".
        let mut empty_url = vec![user_with(vec![ContentItem::InputImage {
            image_url: Some(String::new()),
            file_id: None,
            detail: None,
        }])];
        assert!(has_residual_images(&empty_url));
        assert_eq!(degrade_residual_images(&mut empty_url), 1);

        let mut fully_absent = vec![user_with(vec![ContentItem::InputImage {
            image_url: None,
            file_id: None,
            detail: None,
        }])];
        assert!(has_residual_images(&fully_absent));
        assert_eq!(degrade_residual_images(&mut fully_absent), 1);
    }

    #[test]
    fn has_residual_images_true_and_false_cases() {
        assert!(!has_residual_images(&[]));
        assert!(!has_residual_images(&[ResponseItem::message_text(
            "user", "hi"
        )]));
        assert!(has_residual_images(&[user_with(vec![input_image(
            "data:x"
        )])]));
        assert!(has_residual_images(&[user_with(vec![file_image(
            "file-1"
        )])]));
        // Non-mutating: the input is untouched either way.
        let input = vec![user_with(vec![input_image("data:x")])];
        let before = input.clone();
        assert!(has_residual_images(&input));
        assert_eq!(
            serde_json::to_value(&input).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
    }
}
