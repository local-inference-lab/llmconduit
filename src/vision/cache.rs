//! Per-session LRU+TTL image cache (`ImageCache`) and its stored value types,
//! plus [`VisionRequest`] - the parsed `analyzeImage` call with its cached
//! images resolved against this store.
//!
//! This module owns cache storage/eviction and the cache-resolving request
//! type. The request-mutation seam that strips images out of a
//! [`ResponsesRequest`](crate::models::responses::ResponsesRequest) and
//! populates this cache lives in [`super::strip`]; the per-session reset and
//! LRU/TTL invariants documented here are what that seam relies on.
//!
//! Separate from `ReplayStore` by design (see the module-level docs on
//! [`super`]): replay is SHA256 over `(model, instructions, input)` with no TTL,
//! whereas this is a per-session LRU+TTL keyed by `(session_id, image_id)` that
//! the strip seam clears and repopulates on every request so multi-turn
//! placeholder numbering resets like claude-relay's stateless replay.

use crate::config::Config;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

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

    /// Insert an image under `image_key` for `session_id`, evicting the
    /// least-recently-used entry once the per-session cap is exceeded.
    ///
    /// `pub(super)` so the strip seam in [`super::strip`] can populate the cache
    /// while it walks a request; not part of the public crate surface.
    pub(super) fn store(&self, session_id: &str, image_key: String, image: CachedImage) {
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
    ///
    /// `pub(super)` so the strip seam can reset numbering per request.
    pub(super) fn clear_session(&self, session_id: &str) {
        let mut sessions = self.sessions.lock().expect("image cache mutex");
        sessions.remove(session_id);
    }

    /// Number of cached images for a session (test helper). `pub(super)` so the
    /// strip-seam unit tests in [`super::strip`] can assert cache population.
    #[cfg(test)]
    pub(super) fn session_len(&self, session_id: &str) -> usize {
        let sessions = self.sessions.lock().expect("image cache mutex");
        sessions
            .get(session_id)
            .map(|c| c.entries.len())
            .unwrap_or(0)
    }
}

/// Parsed `analyzeImage` arguments with each requested image id resolved against
/// the session cache. `image_ids` may be empty (the executor then surfaces a
/// model-visible "no images" message rather than erroring).
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
    fn vision_request_parses_arguments_and_resolves_images() {
        let cache = cache();
        cache.store("sess", ImageCache::image_key("sess", "1"), img("data:one"));
        cache.store("sess", ImageCache::image_key("sess", "2"), img("data:two"));
        let args = serde_json::json!({
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
        let args = serde_json::json!({ "imageId": [1] });
        let request = VisionRequest::from_arguments(&args, "sess", &cache);
        assert_eq!(request.image_ids, vec!["1"]);
        assert_eq!(request.task, "Describe this image in detail");
        assert_eq!(request.context, None);
    }
}
