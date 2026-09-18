//! First-Message Replay Restore store: pins the environment block of a
//! session's first user message so conversation replays stay byte-stable.
//!
//! Claude Code regenerates the first user message's leading text block (the
//! `<system-reminder>` environment block: memory index, working directory,
//! platform notes) at every CLI process start. A `/resume` therefore replays
//! message position zero with bytes that differ from the session's original
//! process whenever the environment changed in between (an auto-memory index
//! edit, for example). Prefix caches chain hashes from the prompt's first
//! token, so one changed line near the start of that block invalidates every
//! cached block after it — a resumed session re-prefills its whole history
//! even though the conversation itself is untouched.
//!
//! This store closes the loop server-side, mirroring `SystemMessageCache`:
//! the FIRST text seen for the leading block under a pin key is canonical,
//! and a later replay carrying different bytes is restored to the canonical
//! text before conversion. Pinning reproduces exactly what an uninterrupted
//! CLI process would render (a running process never refreshes the block), so
//! the restored bytes are the lineage the upstream already cached.
//!
//! The pin key is supplied by the caller (see
//! `anthropic_to_responses::first_message_pin_key`): a SHA-256 over the
//! client-reported session id and the request's system instructions. The
//! system-instructions component separates aux calls (session-title and
//! similar side calls carry a different system prompt) from the main
//! conversation lineage inside the same session id, so an aux call's first
//! message can never become the canonical text for the main conversation.
//!
//! Only the leading text block of the first user message is restored; every
//! other block (the user's actual prompt text and any further content) is
//! forwarded as the client sent it.
//!
//! Bounded (FIFO eviction, per-entry byte cap) and persisted to an
//! append-only JSONL file (`LLMCONDUIT_FIRST_MESSAGE_CACHE_PATH`, default
//! `<cache>/llmconduit/first-message-cache-v1.jsonl`). A gateway restart
//! reloads the canonical texts, so replayed sessions keep restoring to the
//! lineage the upstream already cached instead of re-prefilling on the first
//! post-restart turn. A missing entry degrades to the pre-fix behavior
//! (client bytes forwarded as-is), never to an error. Evicted keys linger as
//! dead lines until the file passes 4x the key bound, then a compaction
//! rewrite (tmp + rename) drops them.

use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

/// Default bound on stored pins. One key per session lineage (main call plus
/// each aux system prompt); 4096 covers thousands of sessions with headroom.
/// Eviction drops the OLDEST pins first, which matter least for continuation.
pub const DEFAULT_FIRST_MESSAGE_CACHE_KEYS: usize = 4096;

/// Per-entry cap on the pinned block bytes. The environment block runs from
/// a few KiB to a few tens of KiB (memory index, skill listings); an over-cap
/// block is skipped rather than letting one runaway block dominate memory.
pub const MAX_FIRST_MESSAGE_BYTES: usize = 256 * 1024;

/// Persistence schema version for the append-only JSONL store. Bump on any
/// record-shape change; wrong-version lines are skipped on load.
const FIRST_MESSAGE_PERSISTENCE_VERSION: u8 = 1;

/// One persisted canonical text. `key` is the pin hash supplied by the
/// caller, so the file replays verbatim without re-deriving pins from raw
/// client messages.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedFirstMessage {
    v: u8,
    key: String,
    text: String,
}

/// Append-only JSONL persistence target: writes serialize on this lock while
/// store mutations stay on the inner lock, so persistence I/O never blocks a
/// concurrent lookup longer than the record copy.
#[derive(Debug)]
struct FirstMessagePersistence {
    path: PathBuf,
    lock: Mutex<()>,
}

#[derive(Debug)]
struct FirstMessageCacheInner {
    map: HashMap<String, String>,
    order: VecDeque<String>,
    /// Pins whose restore was already logged once (operator signal stays
    /// one-line-per-pin instead of one-line-per-replayed-request).
    restore_logged: HashSet<String>,
    max_keys: usize,
}

/// Result of consulting the store for one first-message environment block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirstMessageResolution {
    /// Text to forward upstream: the canonical first-seen text when the
    /// incoming text differs, otherwise the incoming text unchanged.
    pub text: String,
    /// True when the incoming text differed from the stored canonical text
    /// and was restored.
    pub restored: bool,
    /// True on the first restore for this pin (log this one; subsequent
    /// replays of the same pin are silent).
    pub first_restore: bool,
}

/// Cheaply cloneable, lock-shared handle. Clones share the same store
/// (Gateway -> per-request adapters).
#[derive(Debug, Clone)]
pub struct FirstMessageCache {
    inner: Arc<Mutex<FirstMessageCacheInner>>,
    persistence: Option<Arc<FirstMessagePersistence>>,
}

impl FirstMessageCache {
    /// In-memory store for tests and explicitly ephemeral callers.
    pub fn new(max_keys: usize) -> Self {
        Self::with_persistence(max_keys, None)
    }

    /// Production store persisted under the platform cache directory:
    /// `LLMCONDUIT_FIRST_MESSAGE_CACHE_PATH` overrides the default
    /// `<cache>/llmconduit/first-message-cache-v1.jsonl`.
    pub fn persistent_default(max_keys: usize) -> Self {
        let path = std::env::var_os("LLMCONDUIT_FIRST_MESSAGE_CACHE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                dirs::cache_dir()
                    .unwrap_or_else(std::env::temp_dir)
                    .join("llmconduit")
                    .join("first-message-cache-v1.jsonl")
            });
        Self::with_persistence(max_keys, Some(path))
    }

    /// Store with an explicit persistence path (`None` = in-memory only).
    /// Loads any persisted records before serving.
    pub fn with_persistence(max_keys: usize, path: Option<PathBuf>) -> Self {
        let cache = Self {
            inner: Arc::new(Mutex::new(FirstMessageCacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
                restore_logged: HashSet::new(),
                max_keys,
            })),
            persistence: path.map(|p| {
                Arc::new(FirstMessagePersistence {
                    path: p,
                    lock: Mutex::new(()),
                })
            }),
        };
        cache.load_persisted();
        cache
    }

    /// Reload persisted canonical texts, replaying the JSONL in write order:
    /// each key was appended at its first record, so first-line-wins entry
    /// semantics reproduce the in-memory first-seen-wins rule, and the FIFO
    /// order queue rebuilds from first-appearance order. Malformed,
    /// wrong-version, empty, or over-cap lines are skipped; a missing file
    /// starts empty.
    fn load_persisted(&self) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        let Ok(content) = std::fs::read_to_string(&persistence.path) else {
            return;
        };
        let mut replayed = 0usize;
        let mut skipped = 0usize;
        for line in content.lines() {
            let Ok(record) = serde_json::from_str::<PersistedFirstMessage>(line) else {
                skipped += 1;
                continue;
            };
            if record.v != FIRST_MESSAGE_PERSISTENCE_VERSION
                || record.text.is_empty()
                || record.text.len() > MAX_FIRST_MESSAGE_BYTES
            {
                skipped += 1;
                continue;
            }
            {
                let mut guard = self.inner.lock().expect("first message cache lock poisoned");
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    guard.map.entry(record.key.clone())
                {
                    entry.insert(record.text.clone());
                    guard.order.push_back(record.key.clone());
                }
            }
            replayed += 1;
        }
        // Trim to the in-memory bound: the file may carry already-evicted
        // keys, so FIFO eviction on load keeps the newest window live.
        {
            let mut guard = self.inner.lock().expect("first message cache lock poisoned");
            while guard.max_keys > 0 && guard.map.len() > guard.max_keys {
                let Some(oldest) = guard.order.pop_front() else {
                    break;
                };
                guard.map.remove(&oldest);
                guard.restore_logged.remove(&oldest);
            }
        }
        if replayed > 0 || skipped > 0 {
            tracing::info!(
                path = %persistence.path.display(),
                replayed,
                skipped,
                "first message cache reloaded from persistence"
            );
        }
        self.compact_persisted_if_needed();
    }

    /// Rewrite the persistence file once stale lines outgrow the live window:
    /// evicted keys keep accumulating as dead lines, so past 4x the key bound
    /// the file is compacted to the current map in FIFO order (tmp + rename).
    /// Failures skip the compaction, never the load.
    fn compact_persisted_if_needed(&self) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        let _guard = persistence
            .lock
            .lock()
            .expect("first message persistence mutex");
        let line_count = std::fs::read_to_string(&persistence.path)
            .map(|content| content.lines().count())
            .unwrap_or(0);
        let max_keys = self
            .inner
            .lock()
            .expect("first message cache lock poisoned")
            .max_keys;
        if max_keys == 0 || line_count <= max_keys.saturating_mul(4) {
            return;
        }
        let records: Vec<PersistedFirstMessage> = {
            let guard = self.inner.lock().expect("first message cache lock poisoned");
            guard
                .order
                .iter()
                .filter_map(|key| guard.map.get(key).map(|text| (key, text)))
                .map(|(key, text)| PersistedFirstMessage {
                    v: FIRST_MESSAGE_PERSISTENCE_VERSION,
                    key: key.clone(),
                    text: text.clone(),
                })
                .collect()
        };
        let mut body = Vec::new();
        for record in &records {
            if let Ok(line) = serde_json::to_vec(record) {
                body.extend_from_slice(&line);
                body.push(b'\n');
            }
        }
        let tmp = persistence.path.with_extension("jsonl.tmp");
        if std::fs::write(&tmp, &body).is_ok() && std::fs::rename(&tmp, &persistence.path).is_ok() {
            tracing::info!(
                path = %persistence.path.display(),
                records = records.len(),
                "first message cache persistence compacted"
            );
        }
    }

    /// Append one persisted record for a key inserted by one store call.
    /// Runs outside the store lock; I/O failure is logged and swallowed so
    /// persistence never breaks serving.
    fn append_record(&self, key: String, text: String) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        let _guard = persistence
            .lock
            .lock()
            .expect("first message persistence mutex");
        if let Some(parent) = persistence.path.parent()
            && let Err(err) = std::fs::create_dir_all(parent)
        {
            tracing::warn!(path = %persistence.path.display(), error = %err, "first message cache persistence dir create failed");
            return;
        }
        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&persistence.path)
        {
            Ok(file) => file,
            Err(err) => {
                tracing::warn!(path = %persistence.path.display(), error = %err, "first message cache persistence open failed");
                return;
            }
        };
        let record = PersistedFirstMessage {
            v: FIRST_MESSAGE_PERSISTENCE_VERSION,
            key,
            text,
        };
        match serde_json::to_vec(&record) {
            Ok(line) => {
                if let Err(err) = file.write_all(&line).and_then(|_| file.write_all(b"\n")) {
                    tracing::warn!(path = %persistence.path.display(), error = %err, "first message cache persistence write failed");
                }
            }
            Err(err) => {
                tracing::warn!(path = %persistence.path.display(), error = %err, "first message cache persistence serialize failed");
            }
        }
        let _ = file.flush();
    }

    /// Resolve one first-message environment block against the store.
    ///
    /// Empty keys, empty texts, and over-cap texts pass through unrecorded.
    /// A first-seen text is recorded as canonical. A replay carrying
    /// different bytes is restored to the canonical text; an identical replay
    /// passes through unchanged and leaves the record untouched.
    pub fn canonical_or_record(&self, pin_key: &str, incoming: &str) -> FirstMessageResolution {
        let passthrough = || FirstMessageResolution {
            text: incoming.to_string(),
            restored: false,
            first_restore: false,
        };
        if pin_key.is_empty() || incoming.is_empty() || incoming.len() > MAX_FIRST_MESSAGE_BYTES {
            return passthrough();
        }
        {
            let mut guard = self.inner.lock().expect("first message cache lock poisoned");
            if let Some(canonical) = guard.map.get(pin_key) {
                if canonical == incoming {
                    return passthrough();
                }
                let canonical = canonical.clone();
                let first_restore = guard.restore_logged.insert(pin_key.to_string());
                return FirstMessageResolution {
                    text: canonical,
                    restored: true,
                    first_restore,
                };
            }
            if guard.max_keys > 0
                && guard.map.len() >= guard.max_keys
                && let Some(oldest) = guard.order.pop_front()
            {
                guard.map.remove(&oldest);
                guard.restore_logged.remove(&oldest);
            }
            guard.order.push_back(pin_key.to_string());
            guard.map.insert(pin_key.to_string(), incoming.to_string());
            drop(guard);
            // Persistence I/O runs outside the store lock (mirrors
            // `ThinkingCache`): a slow disk never blocks a lookup.
            self.append_record(pin_key.to_string(), incoming.to_string());
        }
        passthrough()
    }

    /// Entry count, for tests/introspection (mirrors `ToolOutputCache::len`).
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("first message cache lock poisoned")
            .map
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_seen_text_is_canonical_and_changed_replay_is_restored() {
        let cache = FirstMessageCache::new(8);
        let first = cache.canonical_or_record("pin-1", "env v1");
        assert_eq!(first.text, "env v1");
        assert!(!first.restored);
        assert_eq!(cache.len(), 1);

        let replay = cache.canonical_or_record("pin-1", "env v2");
        assert_eq!(replay.text, "env v1");
        assert!(replay.restored);
        assert!(replay.first_restore);

        let again = cache.canonical_or_record("pin-1", "env v2");
        assert!(again.restored);
        assert!(!again.first_restore);

        // Identical replay passes through without a restore.
        let same = cache.canonical_or_record("pin-1", "env v1");
        assert!(!same.restored);
        assert_eq!(same.text, "env v1");
    }

    #[test]
    fn distinct_pins_never_cross_restore() {
        let cache = FirstMessageCache::new(8);
        cache.canonical_or_record("pin-main", "env main");
        cache.canonical_or_record("pin-aux", "aux prompt");
        let aux = cache.canonical_or_record("pin-aux", "aux prompt changed");
        assert_eq!(aux.text, "aux prompt");
        let main = cache.canonical_or_record("pin-main", "env main");
        assert!(!main.restored);
    }

    #[test]
    fn empty_keys_empty_texts_and_over_cap_texts_are_not_recorded() {
        let cache = FirstMessageCache::new(8);
        assert!(!cache.canonical_or_record("", "env").restored);
        assert!(!cache.canonical_or_record("pin-1", "").restored);
        let big = "x".repeat(MAX_FIRST_MESSAGE_BYTES + 1);
        assert!(!cache.canonical_or_record("pin-2", &big).restored);
        assert!(cache.is_empty());
    }

    #[test]
    fn fifo_eviction_drops_oldest_pin() {
        let cache = FirstMessageCache::new(2);
        cache.canonical_or_record("pin-1", "a");
        cache.canonical_or_record("pin-2", "b");
        cache.canonical_or_record("pin-3", "c");
        assert_eq!(cache.len(), 2);
        // pin-1 was evicted: a changed replay misses and is re-recorded as
        // the incoming form, evicting the next-oldest pin (pin-2).
        let rec = cache.canonical_or_record("pin-1", "a changed");
        assert!(!rec.restored);
        assert_eq!(rec.text, "a changed");
        assert!(cache.canonical_or_record("pin-3", "c changed").restored);
        // pin-1 now holds "a changed" as canonical: differing replays
        // restore it.
        let again = cache.canonical_or_record("pin-1", "a changed again");
        assert!(again.restored);
        assert_eq!(again.text, "a changed");
    }

    #[test]
    fn persistence_round_trip_restores_changed_replay_after_reload() {
        let dir = std::env::temp_dir().join(format!("llmconduit-fmc-test-{}", std::process::id()));
        let path = dir.join("first-message-cache.jsonl");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let cache = FirstMessageCache::with_persistence(8, Some(path.clone()));
            cache.canonical_or_record("pin-1", "env v1");
        }
        // A fresh store over the same file (the gateway-restart case) keeps
        // the canonical text, so the changed replay still restores.
        let reloaded = FirstMessageCache::with_persistence(8, Some(path.clone()));
        let replay = reloaded.canonical_or_record("pin-1", "env v2");
        assert!(replay.restored);
        assert_eq!(replay.text, "env v1");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persistence_load_skips_wrong_version_and_malformed_lines() {
        let dir = std::env::temp_dir().join(format!("llmconduit-fmc-test-v-{}", std::process::id()));
        let path = dir.join("first-message-cache.jsonl");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(
            &path,
            concat!(
                "{\"v\":0,\"key\":\"pin-old\",\"text\":\"a\"}\n",
                "not json\n",
                "{\"v\":1,\"key\":\"pin-empty\",\"text\":\"\"}\n",
                "{\"v\":1,\"key\":\"pin-ok\",\"text\":\"env v1\"}\n",
            ),
        )
        .expect("write fixture");
        let cache = FirstMessageCache::with_persistence(8, Some(path.clone()));
        assert_eq!(cache.len(), 1);
        assert!(cache.canonical_or_record("pin-ok", "env v2").restored);
        assert!(!cache.canonical_or_record("pin-old", "a").restored);
        assert!(!cache.canonical_or_record("pin-empty", "x").restored);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
