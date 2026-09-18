//! System-Message Replay Restore store: keeps `role: "system"` history
//! messages byte-stable across conversation replays.
//!
//! Claude Code (observed with claude-cli 2.1.267) sends its per-turn system
//! reminders as `role: "system"` messages inside `messages`. The request's
//! trailing system message arrives as a list of text blocks whose last block
//! is a per-request nudge ("First privately list what you need next; ...")
//! that the client does NOT persist: the next request replays the same
//! message as a plain string holding only the first block. The upstream chat
//! path renders a block list as the blocks joined by "\n", so the replay
//! diverges from the previously computed prompt exactly at the dropped block,
//! and every token after it -- the assistant turn that followed (thinking +
//! tool calls) plus the new tool results -- is prefilled again on every turn.
//! Measured on the production gateway (2026-09-10): 6-12k missed tokens per
//! ~110k-token request, most of it the previous assistant turn.
//!
//! This store closes the loop server-side, mirroring `ToolOutputCache`: the
//! FIRST text-block list seen for a system message at a given conversation
//! position is canonical. A later replay at the same position whose text
//! blocks are a strict prefix of the canonical list (the client dropped
//! trailing blocks) is restored to the canonical list before conversion, so
//! the wire bytes -- and the rendered tokens -- match the lineage the
//! upstream cached. Any other replay (different text, longer list) passes
//! through unchanged and never overwrites the record: first-seen wins.
//!
//! Position identity is supplied by the caller (see
//! `anthropic_to_responses::message_position_key`): a SHA-256 over the
//! conversation scope and the replay-stable identity of every preceding
//! message. Two conversations sharing an identical prefix map to the same
//! key by design: an identical prefix is exactly the case where the same
//! canonical bytes yield a prefix-cache hit.
//!
//! A position first seen as a single-block replay (a history system message
//! whose live blocks form this process never recorded) is recorded with that
//! single block as canonical. The position's rendered bytes then stay fixed
//! on every later replay and across restarts; without a record, the position
//! re-rendered from raw client bytes only until the process lost its memory
//! or the persistence file, flipping the rendered lineage once per restart.
//!
//! Bounded (FIFO eviction, per-entry byte cap) and persisted to an
//! append-only JSONL file (`LLMCONDUIT_SYSTEM_MESSAGE_CACHE_PATH`, default
//! `<cache>/llmconduit/system-message-cache-v1.jsonl`). A gateway restart
//! reloads the canonical lists, so replayed positions keep restoring to the
//! lineage the upstream already cached instead of collapsing to the client's
//! tail-dropped bytes on the first post-restart turn — without persistence,
//! one restart flips every active long conversation to an uncached prompt
//! (measured 2026-09-18: a 09:59 restart dropped 289 restored positions in a
//! 1083-message session, re-prefilling ~346K prompt tokens on its next turn).
//! A missing entry degrades to the pre-fix behavior (client bytes forwarded
//! as-is), never to an error. Evicted keys linger as dead lines until the
//! file passes 4x the key bound, then a compaction rewrite (tmp + rename)
//! drops them.

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

/// Default bound on stored positions. One key per system message that
/// carried droppable trailing blocks (one per agent turn); 4096 covers
/// several long tool-heavy sessions with headroom. Eviction drops the OLDEST
/// positions first, which matter least for continuation.
pub const DEFAULT_SYSTEM_MESSAGE_CACHE_KEYS: usize = 4096;

/// Per-entry cap on the summed block bytes. Claude Code system reminders run
/// from a few hundred bytes to a few tens of KiB (skill listings); an
/// over-cap message is skipped rather than letting one runaway reminder
/// dominate the store's memory budget.
pub const MAX_SYSTEM_MESSAGE_BYTES: usize = 256 * 1024;

/// Persistence schema version for the append-only JSONL store. Bump on any
/// record-shape change; wrong-version lines are skipped on load.
const SYSTEM_MESSAGE_PERSISTENCE_VERSION: u8 = 1;

/// One persisted canonical block list. `key` is the conversation-position
/// hash supplied by the caller, so the file replays verbatim without
/// re-deriving positions from raw client messages.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedSystemMessage {
    v: u8,
    key: String,
    blocks: Vec<String>,
}

/// Append-only JSONL persistence target: writes serialize on this lock while
/// store mutations stay on the inner lock, so persistence I/O never blocks a
/// concurrent lookup longer than the record copy.
#[derive(Debug)]
struct SystemMessagePersistence {
    path: PathBuf,
    lock: Mutex<()>,
}

#[derive(Debug)]
struct SystemMessageCacheInner {
    map: HashMap<String, Vec<String>>,
    order: VecDeque<String>,
    /// Positions whose restore was already logged once (operator signal
    /// stays one-line-per-position instead of one-line-per-replayed-request).
    restore_logged: HashSet<String>,
    max_keys: usize,
}

/// Result of consulting the store for one replayed system message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemMessageResolution {
    /// Text blocks to forward upstream: the canonical first-seen list when
    /// the incoming replay dropped trailing blocks, otherwise the incoming
    /// blocks unchanged.
    pub blocks: Vec<String>,
    /// True when the incoming blocks were a strict prefix of the stored
    /// canonical list and the dropped tail was restored.
    pub restored: bool,
    /// True on the first restore for this position (log this one;
    /// subsequent replays of the same position are silent).
    pub first_restore: bool,
}

/// Cheaply cloneable, lock-shared handle. Clones share the same store
/// (Gateway -> per-request adapters).
#[derive(Debug, Clone)]
pub struct SystemMessageCache {
    inner: Arc<Mutex<SystemMessageCacheInner>>,
    persistence: Option<Arc<SystemMessagePersistence>>,
}

impl SystemMessageCache {
    /// In-memory store for tests and explicitly ephemeral callers.
    pub fn new(max_keys: usize) -> Self {
        Self::with_persistence(max_keys, None)
    }

    /// Production store persisted under the platform cache directory:
    /// `LLMCONDUIT_SYSTEM_MESSAGE_CACHE_PATH` overrides the default
    /// `<cache>/llmconduit/system-message-cache-v1.jsonl`.
    pub fn persistent_default(max_keys: usize) -> Self {
        let path = std::env::var_os("LLMCONDUIT_SYSTEM_MESSAGE_CACHE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                dirs::cache_dir()
                    .unwrap_or_else(std::env::temp_dir)
                    .join("llmconduit")
                    .join("system-message-cache-v1.jsonl")
            });
        Self::with_persistence(max_keys, Some(path))
    }

    /// Store with an explicit persistence path (`None` = in-memory only).
    /// Loads any persisted records before serving.
    pub fn with_persistence(max_keys: usize, path: Option<PathBuf>) -> Self {
        let cache = Self {
            inner: Arc::new(Mutex::new(SystemMessageCacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
                restore_logged: HashSet::new(),
                max_keys,
            })),
            persistence: path.map(|p| {
                Arc::new(SystemMessagePersistence {
                    path: p,
                    lock: Mutex::new(()),
                })
            }),
        };
        cache.load_persisted();
        cache
    }

    /// Reload persisted canonical lists, replaying the JSONL in write order:
    /// each key was appended at its first record, so first-line-wins entry
    /// semantics reproduce the in-memory first-seen-wins rule, and the FIFO
    /// order queue rebuilds from first-appearance order. Malformed,
    /// wrong-version, empty-block, or over-cap lines are skipped; a missing
    /// file starts empty.
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
            let Ok(record) = serde_json::from_str::<PersistedSystemMessage>(line) else {
                skipped += 1;
                continue;
            };
            let blocks_bytes: usize = record.blocks.iter().map(String::len).sum();
            if record.v != SYSTEM_MESSAGE_PERSISTENCE_VERSION
                || record.blocks.is_empty()
                || blocks_bytes == 0
                || blocks_bytes > MAX_SYSTEM_MESSAGE_BYTES
            {
                skipped += 1;
                continue;
            }
            {
                let mut guard = self.inner.lock().expect("system message cache lock poisoned");
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    guard.map.entry(record.key.clone())
                {
                    entry.insert(record.blocks.clone());
                    guard.order.push_back(record.key.clone());
                }
            }
            replayed += 1;
        }
        // Trim to the in-memory bound: the file may carry already-evicted
        // keys, so FIFO eviction on load keeps the newest window live.
        {
            let mut guard = self.inner.lock().expect("system message cache lock poisoned");
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
                "system message cache reloaded from persistence"
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
            .expect("system message persistence mutex");
        let line_count = std::fs::read_to_string(&persistence.path)
            .map(|content| content.lines().count())
            .unwrap_or(0);
        let max_keys = self
            .inner
            .lock()
            .expect("system message cache lock poisoned")
            .max_keys;
        if max_keys == 0 || line_count <= max_keys.saturating_mul(4) {
            return;
        }
        let records: Vec<PersistedSystemMessage> = {
            let guard = self.inner.lock().expect("system message cache lock poisoned");
            guard
                .order
                .iter()
                .filter_map(|key| guard.map.get(key).map(|blocks| (key, blocks)))
                .map(|(key, blocks)| PersistedSystemMessage {
                    v: SYSTEM_MESSAGE_PERSISTENCE_VERSION,
                    key: key.clone(),
                    blocks: blocks.clone(),
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
                "system message cache persistence compacted"
            );
        }
    }

    /// Append one persisted record for a key inserted by one store call.
    /// Runs outside the store lock; I/O failure is logged and swallowed so
    /// persistence never breaks serving.
    fn append_record(&self, key: String, blocks: Vec<String>) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        let _guard = persistence
            .lock
            .lock()
            .expect("system message persistence mutex");
        if let Some(parent) = persistence.path.parent()
            && let Err(err) = std::fs::create_dir_all(parent)
        {
            tracing::warn!(path = %persistence.path.display(), error = %err, "system message cache persistence dir create failed");
            return;
        }
        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&persistence.path)
        {
            Ok(file) => file,
            Err(err) => {
                tracing::warn!(path = %persistence.path.display(), error = %err, "system message cache persistence open failed");
                return;
            }
        };
        let record = PersistedSystemMessage {
            v: SYSTEM_MESSAGE_PERSISTENCE_VERSION,
            key,
            blocks,
        };
        match serde_json::to_vec(&record) {
            Ok(line) => {
                if let Err(err) = file.write_all(&line).and_then(|_| file.write_all(b"\n")) {
                    tracing::warn!(path = %persistence.path.display(), error = %err, "system message cache persistence write failed");
                }
            }
            Err(err) => {
                tracing::warn!(path = %persistence.path.display(), error = %err, "system message cache persistence serialize failed");
            }
        }
        let _ = file.flush();
    }

    /// Resolve one system message's text blocks against the store.
    ///
    /// Empty keys, empty block lists, and over-cap lists pass through
    /// unrecorded. Any other first-seen list is recorded as canonical,
    /// including a single-block list: a position first seen as a one-block
    /// replay locks to that replay form, so its rendered bytes stay stable
    /// across restarts and replay mutations (first-seen wins). A replay whose
    /// blocks are a strict prefix of the canonical list is restored; a replay
    /// equal to the canonical list, or differing from it in any other way,
    /// passes through unchanged and leaves the record untouched.
    pub fn canonical_or_record(
        &self,
        position_key: &str,
        incoming: &[String],
    ) -> SystemMessageResolution {
        let passthrough = || SystemMessageResolution {
            blocks: incoming.to_vec(),
            restored: false,
            first_restore: false,
        };
        let incoming_bytes: usize = incoming.iter().map(String::len).sum();
        if position_key.is_empty()
            || incoming.is_empty()
            || incoming_bytes == 0
            || incoming_bytes > MAX_SYSTEM_MESSAGE_BYTES
        {
            return passthrough();
        }
        {
            let mut guard = self.inner.lock().expect("system message cache lock poisoned");
            if let Some(canonical) = guard.map.get(position_key) {
                let dropped_tail = incoming.len() < canonical.len()
                    && canonical[..incoming.len()] == *incoming;
                if !dropped_tail {
                    return passthrough();
                }
                let canonical = canonical.clone();
                let first_restore = guard.restore_logged.insert(position_key.to_string());
                return SystemMessageResolution {
                    blocks: canonical,
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
            guard.order.push_back(position_key.to_string());
            guard
                .map
                .insert(position_key.to_string(), incoming.to_vec());
            drop(guard);
            // Persistence I/O runs outside the store lock (mirrors
            // `ThinkingCache`): a slow disk never blocks a lookup.
            self.append_record(position_key.to_string(), incoming.to_vec());
        }
        passthrough()
    }

    /// Entry count, for tests/introspection (mirrors `ToolOutputCache::len`).
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("system message cache lock poisoned")
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

    fn blocks(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| part.to_string()).collect()
    }

    #[test]
    fn first_seen_list_is_canonical_and_dropped_tail_is_restored() {
        let cache = SystemMessageCache::new(8);
        let full = blocks(&["reminder", "nudge"]);
        let first = cache.canonical_or_record("pos-1", &full);
        assert_eq!(first.blocks, full);
        assert!(!first.restored);
        assert_eq!(cache.len(), 1);

        let replay = cache.canonical_or_record("pos-1", &blocks(&["reminder"]));
        assert_eq!(replay.blocks, full);
        assert!(replay.restored);
        assert!(replay.first_restore);

        let again = cache.canonical_or_record("pos-1", &blocks(&["reminder"]));
        assert!(again.restored);
        assert!(!again.first_restore);
    }

    #[test]
    fn identical_replay_and_divergent_replay_pass_through() {
        let cache = SystemMessageCache::new(8);
        let full = blocks(&["reminder", "nudge"]);
        cache.canonical_or_record("pos-1", &full);

        let same = cache.canonical_or_record("pos-1", &full);
        assert_eq!(same.blocks, full);
        assert!(!same.restored);

        let other = blocks(&["different", "nudge"]);
        let divergent = cache.canonical_or_record("pos-1", &other);
        assert_eq!(divergent.blocks, other);
        assert!(!divergent.restored);
        // First-seen wins: the divergent replay did not overwrite the record.
        assert!(cache.canonical_or_record("pos-1", &blocks(&["reminder"])).restored);

        let longer = blocks(&["reminder", "nudge", "extra"]);
        let extended = cache.canonical_or_record("pos-1", &longer);
        assert_eq!(extended.blocks, longer);
        assert!(!extended.restored);
    }

    #[test]
    fn single_block_first_sight_is_recorded_and_locks_the_replay_form() {
        let cache = SystemMessageCache::new(8);
        // A position first seen as a one-block replay is recorded, so its
        // rendered bytes stay the replay form on every later replay.
        let first = cache.canonical_or_record("pos-1", &blocks(&["only"]));
        assert!(!first.restored);
        assert_eq!(cache.len(), 1);
        // An identical replay equals the canonical list: passes through
        // un-restored, leaving the record untouched.
        let same = cache.canonical_or_record("pos-1", &blocks(&["only"]));
        assert!(!same.restored);
        assert_eq!(same.blocks, blocks(&["only"]));
        // A later longer list does not overwrite the one-block canonical:
        // first-seen wins.
        let longer = cache.canonical_or_record("pos-1", &blocks(&["only", "nudge"]));
        assert!(!longer.restored);
        assert_eq!(longer.blocks, blocks(&["only", "nudge"]));
        assert!(!cache.canonical_or_record("pos-1", &blocks(&["only"])).restored);
    }

    #[test]
    fn empty_keys_empty_lists_and_over_cap_lists_are_not_recorded() {
        let cache = SystemMessageCache::new(8);
        assert!(!cache.canonical_or_record("", &blocks(&["a", "b"])).restored);
        assert!(!cache.canonical_or_record("pos-2", &[]).restored);
        assert!(!cache.canonical_or_record("pos-3", &blocks(&["", ""])).restored);
        let big = "x".repeat(MAX_SYSTEM_MESSAGE_BYTES + 1);
        assert!(!cache.canonical_or_record("pos-4", &[big, "b".to_string()]).restored);
        assert!(cache.is_empty());
    }

    #[test]
    fn fifo_eviction_drops_oldest_position() {
        let cache = SystemMessageCache::new(2);
        cache.canonical_or_record("pos-1", &blocks(&["a", "n"]));
        cache.canonical_or_record("pos-2", &blocks(&["b", "n"]));
        cache.canonical_or_record("pos-3", &blocks(&["c", "n"]));
        assert_eq!(cache.len(), 2);
        // pos-1 was evicted: its tail-dropped replay misses and is
        // re-recorded as the one-block replay form, evicting the
        // next-oldest position (pos-2).
        let rec = cache.canonical_or_record("pos-1", &blocks(&["a"]));
        assert!(!rec.restored);
        assert!(cache.canonical_or_record("pos-3", &blocks(&["c"])).restored);
        // pos-1 now holds the one-block canonical: identical replays stay
        // byte-stable without a restore.
        let again = cache.canonical_or_record("pos-1", &blocks(&["a"]));
        assert!(!again.restored);
        assert_eq!(again.blocks, blocks(&["a"]));
    }

    #[test]
    fn persistence_round_trip_restores_dropped_tail_after_reload() {
        let dir = std::env::temp_dir().join(format!("llmconduit-smc-test-{}", std::process::id()));
        let path = dir.join("system-message-cache.jsonl");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let cache = SystemMessageCache::with_persistence(8, Some(path.clone()));
            cache.canonical_or_record("pos-1", &blocks(&["reminder", "nudge"]));
        }
        // A fresh store over the same file (the gateway-restart case) keeps
        // the canonical list, so the tail-dropped replay still restores.
        let reloaded = SystemMessageCache::with_persistence(8, Some(path.clone()));
        let replay = reloaded.canonical_or_record("pos-1", &blocks(&["reminder"]));
        assert!(replay.restored);
        assert_eq!(replay.blocks, blocks(&["reminder", "nudge"]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persistence_load_skips_wrong_version_and_malformed_lines() {
        let dir = std::env::temp_dir().join(format!("llmconduit-smc-test-v-{}", std::process::id()));
        let path = dir.join("system-message-cache.jsonl");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(
            &path,
            concat!(
                "{\"v\":0,\"key\":\"pos-old\",\"blocks\":[\"a\",\"n\"]}\n",
                "{\"v\":1,\"key\":\"pos-one\",\"blocks\":[\"only\"]}\n",
                "not json\n",
                "{\"v\":1,\"key\":\"pos-ok\",\"blocks\":[\"reminder\",\"nudge\"]}\n",
            ),
        )
        .expect("write fixture");
        let cache = SystemMessageCache::with_persistence(8, Some(path.clone()));
        // The wrong-version line and the non-JSON line are skipped; both
        // valid records load, including the single-block one.
        assert_eq!(cache.len(), 2);
        assert!(cache.canonical_or_record("pos-ok", &blocks(&["reminder"])).restored);
        assert!(!cache.canonical_or_record("pos-old", &blocks(&["a"])).restored);
        let one = cache.canonical_or_record("pos-one", &blocks(&["only"]));
        assert!(!one.restored);
        assert_eq!(one.blocks, blocks(&["only"]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn single_block_record_survives_persistence_reload() {
        let dir = std::env::temp_dir().join(format!("llmconduit-smc-test-1b-{}", std::process::id()));
        let path = dir.join("system-message-cache.jsonl");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let cache = SystemMessageCache::with_persistence(8, Some(path.clone()));
            cache.canonical_or_record("pos-1", &blocks(&["only"]));
        }
        // A fresh store over the same file (the gateway-restart case) keeps
        // the one-block canonical, so the replay form stays byte-stable.
        let reloaded = SystemMessageCache::with_persistence(8, Some(path.clone()));
        assert_eq!(reloaded.len(), 1);
        let replay = reloaded.canonical_or_record("pos-1", &blocks(&["only"]));
        assert!(!replay.restored);
        assert_eq!(replay.blocks, blocks(&["only"]));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
