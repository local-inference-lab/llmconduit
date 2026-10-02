use crate::models::chat::ChatMessage;
use crate::models::responses::ResponseItem;
use serde::Serialize;
use serde::Serializer as _;
use serde::ser::SerializeMap;
use serde::ser::SerializeSeq;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::mem;
use std::sync::Arc;
use tokio::sync::RwLock;

const DEFAULT_REPLAY_BYTE_LIMIT: usize = 64 * 1024 * 1024;
const REPLAY_ENTRY_STRUCTURAL_OVERHEAD: usize = 512;
const SERIALIZED_RETAINED_BYTE_MULTIPLIER: usize = 8;

#[derive(Debug, Clone, Serialize)]
pub struct ReplayRecord {
    pub model: String,
    pub instructions: String,
    pub visible_history: Vec<ResponseItem>,
    pub internal_messages: Vec<ChatMessage>,
}

#[derive(Debug, Clone)]
struct ReplayEntry {
    record: ReplayRecord,
    retained_bytes: usize,
}

/// Raw SHA-256 digest. Kept binary (not hex) so each retained key is 32 bytes
/// inline instead of a 64-byte heap `String`.
type ReplayKey = [u8; 32];

#[derive(Debug, Clone)]
struct ReplayInner {
    map: HashMap<ReplayKey, ReplayEntry>,
    order: VecDeque<ReplayKey>,
    max_entries: usize,
    max_bytes: usize,
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct ReplayStore {
    inner: Arc<RwLock<ReplayInner>>,
}

impl ReplayStore {
    pub fn new(max_entries: usize) -> Self {
        let max_bytes = if max_entries == 0 {
            0
        } else {
            DEFAULT_REPLAY_BYTE_LIMIT
        };
        Self::with_byte_limit(max_entries, max_bytes)
    }

    pub fn with_byte_limit(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ReplayInner {
                map: HashMap::new(),
                order: VecDeque::new(),
                max_entries,
                max_bytes,
                retained_bytes: 0,
            })),
        }
    }

    pub async fn insert(&self, record: ReplayRecord) {
        let key =
            visible_history_digest(&record.model, &record.instructions, &record.visible_history);
        let Ok(retained_bytes) = retained_record_bytes(&record) else {
            return;
        };
        let mut guard = self.inner.write().await;
        if guard.max_entries == 0 || guard.max_bytes == 0 || retained_bytes > guard.max_bytes {
            return;
        }

        if let Some(old_retained_bytes) = guard.map.get(&key).map(|entry| entry.retained_bytes) {
            guard.retained_bytes = guard.retained_bytes.saturating_sub(old_retained_bytes);
            while guard.retained_bytes.saturating_add(retained_bytes) > guard.max_bytes {
                if !guard.evict_oldest_except(&key) {
                    break;
                }
            }
            if let Some(entry) = guard.map.get_mut(&key) {
                entry.record = record;
                entry.retained_bytes = retained_bytes;
                guard.retained_bytes = guard.retained_bytes.saturating_add(retained_bytes);
            }
            return;
        }

        while guard.map.len() >= guard.max_entries {
            if !guard.evict_oldest() {
                break;
            }
        }
        while guard.retained_bytes.saturating_add(retained_bytes) > guard.max_bytes {
            if !guard.evict_oldest() {
                break;
            }
        }
        guard.order.push_back(key);
        guard.retained_bytes = guard.retained_bytes.saturating_add(retained_bytes);
        guard.map.insert(
            key,
            ReplayEntry {
                record,
                retained_bytes,
            },
        );
    }

    pub async fn longest_prefix_match(
        &self,
        model: &str,
        instructions: &str,
        input: &[ResponseItem],
    ) -> Option<ReplayRecord> {
        // Every prefix digest comes from ONE streaming pass (O(total bytes), not
        // O(N^2) re-serialization) and is computed BEFORE the lock so hashing a
        // long history never holds up concurrent inserts.
        let digests = prefix_digests(model, instructions, input);
        let guard = self.inner.read().await;
        digests
            .iter()
            .rev()
            .find_map(|key| guard.map.get(key))
            .map(|entry| entry.record.clone())
    }

    /// Number of entries currently stored. Observability/testability accessor
    /// (mirrors `ImageCache::session_len`) — e.g. E2b's integration tests use
    /// this to prove a degraded turn wrote NOTHING to the cache, without
    /// needing to reconstruct the exact stored key.
    pub async fn len(&self) -> usize {
        self.inner.read().await.map.len()
    }

    /// `true` when [`len`](Self::len) is `0`.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

impl ReplayInner {
    fn evict_oldest(&mut self) -> bool {
        while let Some(oldest) = self.order.pop_front() {
            if let Some(entry) = self.map.remove(&oldest) {
                self.retained_bytes = self.retained_bytes.saturating_sub(entry.retained_bytes);
                return true;
            }
        }
        false
    }

    fn evict_oldest_except(&mut self, skipped_key: &ReplayKey) -> bool {
        while let Some(index) = self.order.iter().position(|key| key != skipped_key) {
            let Some(oldest) = self.order.remove(index) else {
                return false;
            };
            if let Some(entry) = self.map.remove(&oldest) {
                self.retained_bytes = self.retained_bytes.saturating_sub(entry.retained_bytes);
                return true;
            }
        }
        false
    }
}

pub fn hash_visible_history(model: &str, instructions: &str, items: &[ResponseItem]) -> String {
    hex::encode(visible_history_digest(model, instructions, items))
}

fn visible_history_digest(model: &str, instructions: &str, items: &[ResponseItem]) -> ReplayKey {
    let mut hasher = Sha256::new();
    let mut writer = Sha256Writer(&mut hasher);
    let _ = serialize_visible_history(&mut writer, model, instructions, items);
    hasher.finalize().into()
}

/// Digests of `items[..k]` for every `k` in `0..=items.len()` (index = `k`),
/// byte-identical to [`visible_history_digest`] on each prefix.
///
/// The canonical payload is `{"instructions":I,"items":[i0,i1,...],"model":M}`
/// (sorted keys, compact), so every prefix shares all bytes up to its last item
/// and differs only by the closing `],"model":M}` suffix. Streaming the shared
/// bytes into one hasher and finalizing a CLONE plus that suffix per prefix
/// turns the lookup from O(N^2) serialization into O(total bytes). If an item
/// fails to serialize, longer prefixes are omitted: the legacy path would hash
/// a truncated payload that no successful insert could have produced.
fn prefix_digests(model: &str, instructions: &str, items: &[ResponseItem]) -> Vec<ReplayKey> {
    let mut suffix = Vec::with_capacity(model.len() + 16);
    suffix.extend_from_slice(b"],\"model\":");
    if serde_json::to_writer(&mut suffix, model).is_err() {
        return Vec::new();
    }
    suffix.push(b'}');

    let mut hasher = Sha256::new();
    hasher.update(b"{\"instructions\":");
    if serde_json::to_writer(Sha256Writer(&mut hasher), instructions).is_err() {
        return Vec::new();
    }
    hasher.update(b",\"items\":[");

    let finish = |hasher: &Sha256| -> ReplayKey {
        let mut prefix = hasher.clone();
        prefix.update(&suffix);
        prefix.finalize().into()
    };

    let mut digests = Vec::with_capacity(items.len() + 1);
    digests.push(finish(&hasher));
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            hasher.update(b",");
        }
        // `to_value` is what sorts struct fields into the legacy (BTreeMap) key
        // order the stored hashes were built with; it now runs once per item.
        let Ok(value) = serde_json::to_value(item) else {
            break;
        };
        if serde_json::to_writer(Sha256Writer(&mut hasher), &value).is_err() {
            break;
        }
        digests.push(finish(&hasher));
    }
    digests
}

pub(crate) fn serialized_len<T: Serialize>(value: &T) -> Result<usize, serde_json::Error> {
    let mut writer = CountingWriter::default();
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.bytes)
}

fn retained_record_bytes(record: &ReplayRecord) -> Result<usize, serde_json::Error> {
    // Replay records retain Rust strings, vectors, enum tags, and map buckets, so
    // serialized JSON length is a lower bound. Charge a conservative multiple of
    // the serialized payload to keep the retained heap below the nominal budget.
    Ok(serialized_len(record)?
        .saturating_mul(SERIALIZED_RETAINED_BYTE_MULTIPLIER)
        // Key is held twice: map bucket + FIFO order.
        .saturating_add(mem::size_of::<ReplayKey>().saturating_mul(2))
        .saturating_add(mem::size_of::<ReplayEntry>())
        .saturating_add(REPLAY_ENTRY_STRUCTURAL_OVERHEAD))
}

fn serialize_visible_history<W: io::Write>(
    writer: W,
    model: &str,
    instructions: &str,
    items: &[ResponseItem],
) -> Result<(), serde_json::Error> {
    let mut serializer = serde_json::Serializer::new(writer);
    let mut map = serializer.serialize_map(Some(3))?;
    map.serialize_entry("instructions", instructions)?;
    map.serialize_entry("items", &LegacyResponseItems(items))?;
    map.serialize_entry("model", model)?;
    SerializeMap::end(map)
}

struct LegacyResponseItems<'a>(&'a [ResponseItem]);

impl Serialize for LegacyResponseItems<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for item in self.0 {
            let value = serde_json::to_value(item).map_err(serde::ser::Error::custom)?;
            seq.serialize_element(&value)?;
        }
        SerializeSeq::end(seq)
    }
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
}

impl io::Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Sha256Writer<'a>(&'a mut Sha256);

impl io::Write for Sha256Writer<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::responses::{ContentItem, ResponseItem};

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

    fn user_image(uri: String) -> ResponseItem {
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputImage {
                image_url: Some(uri),
                file_id: None,
                detail: Some("high".to_string()),
            }],
            phase: None,
        }
    }

    fn record_with_history(visible_history: Vec<ResponseItem>) -> ReplayRecord {
        ReplayRecord {
            model: "m".to_string(),
            instructions: "i".to_string(),
            visible_history,
            internal_messages: vec![],
        }
    }

    #[tokio::test]
    async fn test_replay_store_evicts_oldest() {
        let store = ReplayStore::new(2);
        let records: Vec<_> = (0..3)
            .map(|i| ReplayRecord {
                model: "m".to_string(),
                instructions: "i".to_string(),
                visible_history: vec![user_msg(&format!("msg-{i}"))],
                internal_messages: vec![],
            })
            .collect();
        for r in &records {
            store.insert(r.clone()).await;
        }
        // First key should be evicted
        let first = store
            .longest_prefix_match("m", "i", &[user_msg("msg-0")])
            .await;
        assert!(first.is_none(), "oldest entry should be evicted");
        let last = store
            .longest_prefix_match("m", "i", &[user_msg("msg-2")])
            .await;
        assert!(last.is_some(), "newest entry should exist");
    }

    #[tokio::test]
    async fn test_replay_store_respects_capacity() {
        let store = ReplayStore::new(3);
        for i in 0..3 {
            store
                .insert(ReplayRecord {
                    model: "m".to_string(),
                    instructions: "i".to_string(),
                    visible_history: vec![user_msg(&format!("cap-{i}"))],
                    internal_messages: vec![],
                })
                .await;
        }
        for i in 0..3 {
            let found = store
                .longest_prefix_match("m", "i", &[user_msg(&format!("cap-{i}"))])
                .await;
            assert!(found.is_some(), "entry {i} should be present");
        }
    }

    #[tokio::test]
    async fn test_replay_store_duplicate_key_no_double_track() {
        let store = ReplayStore::new(5);
        let record = ReplayRecord {
            model: "m".to_string(),
            instructions: "i".to_string(),
            visible_history: vec![user_msg("dup")],
            internal_messages: vec![],
        };
        store.insert(record.clone()).await;
        store.insert(record.clone()).await;
        let guard = store.inner.read().await;
        assert_eq!(
            guard.order.len(),
            1,
            "VecDeque should not grow on duplicate key"
        );
        assert_eq!(guard.map.len(), 1);
    }

    #[tokio::test]
    async fn test_replay_store_zero_capacity_disables_cache() {
        let store = ReplayStore::new(0);
        store
            .insert(record_with_history(vec![user_msg("disabled")]))
            .await;

        assert!(store.is_empty().await);
        assert!(
            store
                .longest_prefix_match("m", "i", &[user_msg("disabled")])
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_replay_store_byte_limit_evicts_distinct_large_images() {
        let first = record_with_history(vec![user_image(format!(
            "data:image/png;base64,{}",
            "a".repeat(1024)
        ))]);
        let second = record_with_history(vec![user_image(format!(
            "data:image/png;base64,{}",
            "b".repeat(1024)
        ))]);
        let first_bytes = retained_record_bytes(&first).unwrap();
        let second_bytes = retained_record_bytes(&second).unwrap();
        let store = ReplayStore::with_byte_limit(10, first_bytes.max(second_bytes) + 64);

        store.insert(first).await;
        store.insert(second).await;

        assert_eq!(store.len().await, 1);
        assert!(
            store
                .longest_prefix_match(
                    "m",
                    "i",
                    &[user_image(format!(
                        "data:image/png;base64,{}",
                        "a".repeat(1024)
                    ))]
                )
                .await
                .is_none(),
            "oldest large image record should be byte-evicted"
        );
        assert!(
            store
                .longest_prefix_match(
                    "m",
                    "i",
                    &[user_image(format!(
                        "data:image/png;base64,{}",
                        "b".repeat(1024)
                    ))]
                )
                .await
                .is_some(),
            "newest large image record should remain"
        );
    }

    #[tokio::test]
    async fn test_replay_store_oversize_record_does_not_clear_existing() {
        let existing = record_with_history(vec![user_msg("existing")]);
        let existing_bytes = retained_record_bytes(&existing).unwrap();
        let store = ReplayStore::with_byte_limit(10, existing_bytes + 256);
        store.insert(existing).await;

        store
            .insert(record_with_history(vec![user_image(format!(
                "data:image/png;base64,{}",
                "x".repeat(4096)
            ))]))
            .await;

        assert_eq!(store.len().await, 1);
        assert!(
            store
                .longest_prefix_match("m", "i", &[user_msg("existing")])
                .await
                .is_some(),
            "oversize insert should be rejected without evicting existing records"
        );
    }

    #[tokio::test]
    async fn test_replay_store_replacement_updates_byte_accounting() {
        let original = record_with_history(vec![user_msg("same")]);
        let original_bytes = retained_record_bytes(&original).unwrap();
        let replacement = ReplayRecord {
            internal_messages: vec![ChatMessage {
                role: "assistant".to_string(),
                content: Some(serde_json::json!({"text": "x".repeat(2048)})),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            ..original.clone()
        };
        let replacement_bytes = retained_record_bytes(&replacement).unwrap();
        let other = record_with_history(vec![user_msg("other")]);
        let other_bytes = retained_record_bytes(&other).unwrap();
        let store = ReplayStore::with_byte_limit(
            10,
            replacement_bytes
                .saturating_add(other_bytes)
                .saturating_add(128),
        );

        store.insert(original).await;
        store.insert(replacement).await;
        store.insert(other).await;

        let guard = store.inner.read().await;
        assert_eq!(guard.map.len(), 2);
        assert_eq!(
            guard.retained_bytes,
            replacement_bytes + other_bytes,
            "replacement should subtract the old weight before adding the new one"
        );
        assert!(
            guard.retained_bytes > original_bytes,
            "test setup should exercise a larger replacement"
        );
    }

    #[tokio::test]
    async fn test_replay_store_replacement_preserves_fifo_position() {
        let store = ReplayStore::new(2);
        let oldest = record_with_history(vec![user_msg("oldest")]);
        let replacement = ReplayRecord {
            internal_messages: vec![ChatMessage {
                role: "assistant".to_string(),
                content: Some(serde_json::json!({"text": "updated"})),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            ..oldest.clone()
        };
        let middle = record_with_history(vec![user_msg("middle")]);
        let newest = record_with_history(vec![user_msg("newest")]);

        store.insert(oldest).await;
        store.insert(middle).await;
        store.insert(replacement).await;
        store.insert(newest).await;

        assert!(
            store
                .longest_prefix_match("m", "i", &[user_msg("oldest")])
                .await
                .is_none(),
            "replacing the oldest key should not refresh its FIFO position"
        );
        assert!(
            store
                .longest_prefix_match("m", "i", &[user_msg("middle")])
                .await
                .is_some()
        );
        assert!(
            store
                .longest_prefix_match("m", "i", &[user_msg("newest")])
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn longest_prefix_match_partial() {
        let store = ReplayStore::new(1000);
        let history = vec![user_msg("a"), user_msg("b"), user_msg("c")];
        store
            .insert(ReplayRecord {
                model: "m".to_string(),
                instructions: "i".to_string(),
                visible_history: history.clone(),
                internal_messages: vec![],
            })
            .await;

        let mut query = history.clone();
        query.push(user_msg("d"));
        let result = store.longest_prefix_match("m", "i", &query).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().visible_history.len(), 3);
    }

    #[test]
    fn hash_visible_history_deterministic() {
        let items = vec![user_msg("hello")];
        let h1 = hash_visible_history("model", "instr", &items);
        let h2 = hash_visible_history("model", "instr", &items);
        assert_eq!(h1, h2);

        let h3 = hash_visible_history("other_model", "instr", &items);
        assert_ne!(h1, h3);
    }

    #[test]
    fn hash_visible_history_matches_legacy_json_payload() {
        let items = vec![user_msg("hello")];
        let payload = serde_json::json!({
            "model": "model",
            "instructions": "instr",
            "items": items,
        });
        let mut hasher = Sha256::new();
        let legacy_bytes = serde_json::to_vec(&payload).unwrap();
        hasher.update(legacy_bytes);
        let legacy = hex::encode(hasher.finalize());

        assert_eq!(hash_visible_history("model", "instr", &items), legacy);
    }

    /// The pre-optimization lookup key: one `json!` payload per prefix.
    fn legacy_hash(model: &str, instructions: &str, items: &[ResponseItem]) -> String {
        let payload = serde_json::json!({
            "model": model,
            "instructions": instructions,
            "items": items,
        });
        hex::encode(Sha256::digest(serde_json::to_vec(&payload).unwrap()))
    }

    fn varied_items() -> Vec<ResponseItem> {
        let shapes = [
            serde_json::json!({"type":"message","role":"user","content":[{"type":"input_text","text":"hi \"quoted\" \u{2603}\n"}]}),
            serde_json::json!({"type":"message","id":"m1","role":"assistant","phase":"final","content":[{"type":"output_text","text":"ok"}]}),
            serde_json::json!({"type":"reasoning","id":"r1","summary":[{"type":"summary_text","text":"s"}],"encrypted_content":"enc"}),
            serde_json::json!({"type":"function_call","name":"f","namespace":"ns","arguments":"{\"z\":1,\"a\":2}","call_id":"c1"}),
            serde_json::json!({"type":"function_call_output","call_id":"c1","output":{"zeta":1,"alpha":[1,{"y":2,"b":3}]}}),
            serde_json::json!({"type":"custom_tool_call","status":"done","call_id":"c2","name":"g","input":"x"}),
            serde_json::json!({"type":"custom_tool_call_output","call_id":"c2","output":"plain"}),
            serde_json::json!({"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA","detail":"high"}]}),
            serde_json::json!({"type":"message","role":"user","content":[]}),
        ];
        shapes
            .into_iter()
            .map(|v| serde_json::from_value(v).expect("valid response item"))
            .collect()
    }

    #[test]
    fn prefix_digests_match_legacy_hash_for_every_prefix() {
        let items = varied_items();
        for (model, instructions) in [
            ("m", "i"),
            ("", ""),
            ("gpt-\"x\"", "line1\nline2 \u{1F600} \\ /"),
        ] {
            // Every contiguous window exercises different leading shapes.
            for start in 0..items.len() {
                let window = &items[start..];
                let digests = prefix_digests(model, instructions, window);
                assert_eq!(digests.len(), window.len() + 1);
                for (len, digest) in digests.iter().enumerate() {
                    let expected = legacy_hash(model, instructions, &window[..len]);
                    assert_eq!(hex::encode(digest), expected, "prefix len {len}");
                    assert_eq!(
                        hash_visible_history(model, instructions, &window[..len]),
                        expected
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn longest_prefix_match_prefers_longest_stored_prefix() {
        let items = varied_items();
        let store = ReplayStore::new(100);
        for len in [0, 2, 5] {
            store
                .insert(ReplayRecord {
                    model: "m".to_string(),
                    instructions: "i".to_string(),
                    visible_history: items[..len].to_vec(),
                    internal_messages: vec![],
                })
                .await;
        }
        let hit = store.longest_prefix_match("m", "i", &items).await.unwrap();
        assert_eq!(hit.visible_history.len(), 5);
        let hit = store
            .longest_prefix_match("m", "i", &items[..4])
            .await
            .unwrap();
        assert_eq!(hit.visible_history.len(), 2);
        let hit = store.longest_prefix_match("m", "i", &[]).await.unwrap();
        assert!(hit.visible_history.is_empty());
        assert!(
            store
                .longest_prefix_match("m2", "i", &items)
                .await
                .is_none()
        );
    }

    #[test]
    fn serialized_len_counts_json_without_allocating_payload() {
        let value = serde_json::json!({
            "a": "x".repeat(32),
            "b": [1, 2, 3],
        });
        assert_eq!(
            serialized_len(&value).unwrap(),
            serde_json::to_vec(&value).unwrap().len()
        );
    }
}
