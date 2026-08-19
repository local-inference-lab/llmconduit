//! G4 — Image agent (vision offload).
//!
//! Ports claude-relay's in-proxy vision offload to llmconduit's canonical
//! Responses pipeline. When a request resolves to a profile that configures
//! `image_analysis`, images in the latest user turn are stripped to `[Image #N]`
//! placeholders, cached, and an `analyzeImage` server tool is injected. When the
//! model calls `analyzeImage`, the engine resolves the cached image(s) and
//! dispatches them to the profile-selected analyzer model through the gateway's
//! own upstreams, then injects the description back into the chat history as a
//! tool result - exactly the way Brave `web_search` is run server-side.
//!
//! The cache is intentionally SEPARATE from `ReplayStore` (replay is SHA256 over
//! `(model, instructions, input)` with no TTL); this is a per-session LRU+TTL
//! keyed by `(session_id, image_id)` that is cleared and repopulated every time
//! [`ImageCache::strip_and_cache_images`] runs, so multi-turn placeholder
//! numbering resets like claude-relay's stateless replay.
//!
//! Module layout (grouped by concern):
//! - [`cache`] - the per-session LRU+TTL [`ImageCache`] storage/eviction plus
//!   [`VisionRequest`], the parsed `analyzeImage` call with its cached images
//!   resolved.
//! - [`strip`] — request mutation: strip images to placeholders, inject the
//!   `analyzeImage` tool + system prompt. Also
//!   home to the E2b role-agnostic residual-image pass
//!   ([`degrade_residual_images`]/[`has_residual_images`]) that runs at the
//!   engine layer when the image agent is active, so no raw `InputImage` reaches
//!   the text-only backend a profile configured `image_analysis` for.
//!
//! Image-URI redaction lives in the sibling [`crate::redaction`] module (it is
//! not vision-specific); the three redactors are re-exported here so existing
//! `crate::vision::redact_*` call sites keep resolving.

mod cache;
mod strip;

pub use cache::CachedImage;
pub use cache::ImageCache;
pub use cache::VisionRequest;
pub use strip::ANALYZE_IMAGE_TOOL_DESCRIPTION;
pub use strip::ANALYZE_IMAGE_TOOL_NAME;
pub use strip::IMAGE_AGENT_SYSTEM_PROMPT;
pub use strip::analyze_image_tool_parameters;
pub use strip::analyze_image_tool_spec;
pub use strip::degrade_residual_images;
pub use strip::has_residual_images;
pub use strip::tool_is_analyze_image;

// Re-exported from the sibling redaction module so `crate::vision::redact_*`
// consumers compile unchanged after the redaction logic moved out of vision.
pub use crate::redaction::redact_image_uris;
pub use crate::redaction::redact_image_uris_in_value;
pub use crate::redaction::redact_vision_text;
