//! Session tree and prefix lineage.
//!
//! Every persisted request belongs to exactly one **session node**. Nodes
//! nest arbitrarily (session, sub-session, sub-sub-session) and are either
//! *declared* — the harness put the id on the wire (see `crate::harness`) —
//! or *inferred* — the gateway created them because a request did not extend
//! any known conversation in its parent.
//!
//! Each node owns one **chain**: the sequence of requests that each extend the
//! previous one's item list. Linking a request means finding the chain whose
//! item hashes are the longest prefix of the new request's, recording the
//! predecessor and the **divergence** (where and how the new request differs),
//! and creating nodes as needed. A divergence inside the shared prefix is what
//! breaks an upstream KV/prefix cache, so it is stored as a flag per request.
//!
//! The linker is an in-memory index guarded by a mutex; it returns the rows to
//! persist and never touches the store itself. It is bounded (LRU over nodes)
//! and can be seeded from durable rows after a restart.

use crate::content_store::{ItemSection, SplitItem};
use crate::harness::{HarnessIdentity, SubSessionPolicy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;

/// Upper bound on session nodes kept in memory; oldest-touched are evicted.
/// Evicted nodes are re-warmed from durable rows on their next request, so
/// this only bounds the hot set (a 2 GB host cannot afford 20k chain heads).
pub const MAX_NODES: usize = 2_000;
/// Upper bound on item hashes retained per chain head.
pub const MAX_HEAD_ITEMS: usize = 8_192;
/// Upper bound on item hashes retained across ALL chain heads (~34 bytes each
/// in compact form, so ~34 MiB). `MAX_NODES * MAX_HEAD_ITEMS` alone would
/// still allow >500 MiB of heads; past this budget the oldest-touched nodes
/// are evicted.
pub const MAX_TOTAL_HEAD_ITEMS: usize = 1_000_000;
/// Default idle TTL for an in-memory node; override with
/// `LLMCONDUIT_SESSION_INDEX_TTL_HOURS` (`0` disables the TTL).
pub const DEFAULT_NODE_TTL_HOURS: u64 = 24;
/// Upper bound on inferred children examined when matching a request.
const MAX_CANDIDATE_CHAINS: usize = 256;

pub const KIND_DECLARED: &str = "declared";
pub const KIND_INFERRED: &str = "inferred";

/// Whether two prefix-less item lists are the same conversation with some
/// items changed (a rewritten system prompt, an edited tool) rather than two
/// unrelated requests that happen to share a message: the messages in common
/// (see [`overlap`]) must cover at least half of the conversation messages of
/// the larger side. A title call that repeats the user's one message is not
/// a rewrite of a 34-item chain; a re-sent conversation with a new system
/// prompt still is.
fn overlap_is_a_match(common: usize, head: &[CompactItem], input: &[CompactItem]) -> bool {
    common >= 1 && common * 2 >= conversation_messages(head).max(conversation_messages(input))
}

/// The number of user/assistant/tool turns (the population [`overlap`] counts).
fn conversation_messages(items: &[CompactItem]) -> usize {
    items
        .iter()
        .filter(|item| item.is_conversation_message())
        .count()
}

/// The part of a split item that lineage needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemFingerprint {
    pub hash: String,
    pub section: ItemSection,
    pub kind: Option<String>,
}

impl From<&SplitItem> for ItemFingerprint {
    fn from(item: &SplitItem) -> Self {
        Self {
            hash: item.identity.clone(),
            section: item.section,
            kind: item.kind.clone(),
        }
    }
}

/// In-memory form of an [`ItemFingerprint`]: 34 inline bytes instead of two
/// heap strings (~150 bytes). Lineage only ever compares hashes for equality
/// and asks whether a message is a system/developer turn, so that is all a
/// retained chain head keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactItem {
    hash: [u8; 32],
    section: ItemSection,
    /// `kind` was `system` or `developer`.
    instruction_like: bool,
}

impl CompactItem {
    fn is_conversation_message(&self) -> bool {
        self.section == ItemSection::Message && !self.instruction_like
    }
}

impl From<&ItemFingerprint> for CompactItem {
    fn from(item: &ItemFingerprint) -> Self {
        Self {
            hash: compact_hash(&item.hash),
            section: item.section,
            instruction_like: matches!(item.kind.as_deref(), Some("system") | Some("developer")),
        }
    }
}

fn compact_items(items: &[ItemFingerprint]) -> Vec<CompactItem> {
    items.iter().map(CompactItem::from).collect()
}

/// Map an item hash to 32 bytes while preserving equality. Identities are
/// lowercase hex SHA-256 (decoded losslessly); anything else (legacy rows,
/// tests) is digested with a domain prefix, so two distinct strings collide
/// only with a SHA-256 collision. Only LOWERCASE hex is decoded so `"AB.."`
/// and `"ab.."` stay distinct, exactly as the string comparison had them.
fn compact_hash(hash: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    if hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && hex::decode_to_slice(hash, &mut out).is_ok()
    {
        return out;
    }
    let mut hasher = Sha256::new();
    hasher.update(b"llmconduit-session-item\0");
    hasher.update(hash.as_bytes());
    hasher.finalize().into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DivergenceKind {
    /// The predecessor's items are a full prefix; only new items were added.
    Append,
    /// The instructions/system block (or a leading system message) changed.
    InstructionsChanged,
    /// The tool list changed.
    ToolsChanged,
    /// An earlier message inside the shared history changed or was removed.
    HistoryRewritten,
    /// Nothing in common with any known chain: the start of a conversation.
    NewChain,
}

impl DivergenceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::InstructionsChanged => "instructions_changed",
            Self::ToolsChanged => "tools_changed",
            Self::HistoryRewritten => "history_rewritten",
            Self::NewChain => "new_chain",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lineage {
    pub kind: DivergenceKind,
    /// Items shared with the predecessor before the first difference.
    pub shared_prefix: usize,
    /// Index of the first differing item, absent for a pure append.
    pub index: Option<usize>,
    /// True when the divergence falls inside the predecessor's items, which
    /// is what invalidates an upstream prefix cache.
    pub cache_bust: bool,
}

/// Length of the common prefix of two item sequences.
fn shared_prefix(predecessor: &[CompactItem], current: &[CompactItem]) -> usize {
    predecessor
        .iter()
        .zip(current.iter())
        .take_while(|(a, b)| a.hash == b.hash)
        .count()
}

/// How many of the predecessor's conversation messages reappear anywhere in
/// `current`. Only user/assistant/tool turns count: instructions and tool
/// definitions are shared by every conversation a harness runs (a Claude
/// Code sub-agent has the same tools as the main thread), so they cannot
/// tell "the system prompt changed, same conversation" (cache bust) apart
/// from "a different conversation" (new chain). Shared history can.
fn overlap(predecessor: &[CompactItem], current: &[CompactItem]) -> usize {
    let present: HashSet<&[u8; 32]> = current.iter().map(|item| &item.hash).collect();
    predecessor
        .iter()
        .filter(|item| item.is_conversation_message())
        .filter(|item| present.contains(&item.hash))
        .count()
}

/// Compare a request's items with its chain predecessor's.
pub fn classify(predecessor: &[ItemFingerprint], current: &[ItemFingerprint]) -> Lineage {
    classify_compact(&compact_items(predecessor), &compact_items(current))
}

fn classify_compact(predecessor: &[CompactItem], current: &[CompactItem]) -> Lineage {
    let shared = shared_prefix(predecessor, current);
    if shared == predecessor.len() {
        return Lineage {
            kind: DivergenceKind::Append,
            shared_prefix: shared,
            index: None,
            cache_bust: false,
        };
    }
    // The request is a strict prefix of its predecessor: the conversation was
    // rewound (or a new one started from the same prompt, for a harness with
    // no session id). Everything it sends is still in the KV cache.
    if shared == current.len() {
        return Lineage {
            kind: DivergenceKind::HistoryRewritten,
            shared_prefix: shared,
            index: Some(shared),
            cache_bust: false,
        };
    }
    if shared == 0 && overlap(predecessor, current) == 0 {
        return Lineage {
            kind: DivergenceKind::NewChain,
            shared_prefix: 0,
            index: Some(0),
            cache_bust: false,
        };
    }
    // Look at BOTH sides of the divergence point: an item inserted into the
    // current request (a tool appended to the list) shows up on the current
    // side while the predecessor's item there is the first shifted message.
    let kind = [predecessor.get(shared), current.get(shared)]
        .into_iter()
        .flatten()
        .map(|item| match item.section {
            ItemSection::Instructions => DivergenceKind::InstructionsChanged,
            ItemSection::Tool => DivergenceKind::ToolsChanged,
            ItemSection::Message if item.instruction_like => DivergenceKind::InstructionsChanged,
            ItemSection::Message => DivergenceKind::HistoryRewritten,
        })
        // Instructions and tools outrank a history rewrite when either side says so.
        .min_by_key(|kind| match kind {
            DivergenceKind::InstructionsChanged => 0,
            DivergenceKind::ToolsChanged => 1,
            _ => 2,
        })
        .unwrap_or(DivergenceKind::HistoryRewritten);
    Lineage {
        kind,
        shared_prefix: shared,
        index: Some(shared),
        cache_bust: true,
    }
}

/// Durable shape of a session node (table `sessions`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SessionRow {
    pub id: String,
    pub parent_id: Option<String>,
    pub kind: String,
    pub harness: String,
    pub harness_version: Option<String>,
    pub external_id: Option<String>,
    pub session_kind: Option<String>,
    pub client_label: Option<String>,
    pub virtual_key_id: Option<String>,
    /// Owner of the key that opened the node; `None` for open-mode requests,
    /// legacy rows, and keys without an owner (migration 0012).
    #[serde(default)]
    pub user_id: Option<String>,
    pub depth: i64,
    pub root_request_id: Option<String>,
    pub spawned_by_request_id: Option<String>,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub request_count: i64,
}

/// What linking produced for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLink {
    /// The node whose chain the request extends or starts.
    pub session_id: String,
    pub chain_parent_request_id: Option<String>,
    pub lineage: Lineage,
    pub item_count: usize,
    /// Nodes created or touched; persist them (upsert) in order.
    pub upserts: Vec<SessionRow>,
}
pub struct LinkInput<'a> {
    pub api_call_id: &'a str,
    pub identity: &'a HarnessIdentity,
    pub client_label: Option<&'a str>,
    pub virtual_key_id: Option<&'a str>,
    /// Owner of the authenticating virtual key, when known.
    pub user_id: Option<&'a str>,
    pub items: &'a [ItemFingerprint],
    pub now_ms: i64,
}

/// The latest request of a node's chain and its items.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainHead {
    pub request_id: String,
    pub items: Vec<ItemFingerprint>,
}

/// A node's chain head as retained in memory.
#[derive(Debug)]
struct CompactHead {
    request_id: String,
    items: Box<[CompactItem]>,
}

impl CompactHead {
    fn new(request_id: String, items: &[ItemFingerprint]) -> Self {
        let items = &items[..items.len().min(MAX_HEAD_ITEMS)];
        Self {
            request_id,
            items: items.iter().map(CompactItem::from).collect(),
        }
    }
}

#[derive(Debug)]
struct Node {
    row: SessionRow,
    head: Option<CompactHead>,
    children: Vec<String>,
    /// Generation of this node's latest entry in `Index::order`; older entries
    /// for the same id are stale. Makes "was this its last touch?" O(1).
    touch_generation: u64,
    /// Linker clock (request `now_ms`) at the latest touch, for the idle TTL.
    touched_ms: i64,
}

impl Node {
    fn head_len(&self) -> usize {
        self.head.as_ref().map_or(0, |head| head.items.len())
    }
}

#[derive(Debug)]
struct Index {
    nodes: HashMap<String, Node>,
    declared: HashMap<(String, String), String>,
    anonymous: HashMap<(String, String), String>,
    /// Touch order for eviction (front = oldest), tagged with the touch
    /// generation. May hold stale entries (generation no longer current).
    order: VecDeque<(String, u64)>,
    next_generation: u64,
    /// Latest `now_ms` seen, so seeded nodes count as freshly touched.
    clock_ms: i64,
    /// Sum of every node's retained head items (the byte budget).
    head_items: usize,
    /// Idle TTL in ms; `0` disables it.
    ttl_ms: i64,
}

impl Index {
    fn new(ttl_ms: i64) -> Self {
        Self {
            nodes: HashMap::new(),
            declared: HashMap::new(),
            anonymous: HashMap::new(),
            order: VecDeque::new(),
            next_generation: 0,
            clock_ms: 0,
            head_items: 0,
            ttl_ms,
        }
    }
}

fn ttl_ms_from_env() -> i64 {
    let hours = std::env::var("LLMCONDUIT_SESSION_INDEX_TTL_HOURS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_NODE_TTL_HOURS);
    i64::try_from(hours.saturating_mul(3_600_000)).unwrap_or(i64::MAX)
}

/// In-memory session index. Cheap to clone the handle (`Arc` it).
#[derive(Debug)]
pub struct SessionLinker {
    index: Mutex<Index>,
    infer_sub_sessions: bool,
}

impl SessionLinker {
    pub fn new(infer_sub_sessions: bool) -> Self {
        Self::with_ttl_ms(infer_sub_sessions, ttl_ms_from_env())
    }

    fn with_ttl_ms(infer_sub_sessions: bool, ttl_ms: i64) -> Self {
        Self {
            index: Mutex::new(Index::new(ttl_ms)),
            infer_sub_sessions,
        }
    }

    pub fn infer_sub_sessions(&self) -> bool {
        self.infer_sub_sessions
    }

    /// Whether a declared session is already known in memory. The caller uses
    /// this to decide whether to warm the node from durable rows first.
    pub fn knows_declared(&self, harness: &str, external_id: &str) -> bool {
        let index = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        index
            .declared
            .contains_key(&(harness.to_string(), external_id.to_string()))
    }

    /// Whether the per-client bucket for requests without a session id is
    /// already in memory.
    pub fn knows_anonymous(&self, harness: &str, client_label: Option<&str>) -> bool {
        let index = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        index.anonymous.contains_key(&(
            harness.to_string(),
            client_label.unwrap_or_default().to_string(),
        ))
    }

    /// Seed nodes (and optionally their chain heads) from durable rows, for
    /// example after a restart. Existing in-memory nodes are left untouched.
    pub fn seed(&self, rows: Vec<(SessionRow, Option<ChainHead>)>) {
        let mut index = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Seeding is a resume: the warmed nodes are about to be used, so they
        // count as touched now (not at their durable `last_seen_ms`, which
        // would let the idle TTL evict them before the request links).
        let wall_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
            });
        index.clock_ms = index.clock_ms.max(wall_ms);
        for (row, head) in rows {
            if index.nodes.contains_key(&row.id) {
                continue;
            }
            let head = head.map(|head| CompactHead::new(head.request_id, &head.items));
            index.insert_node(row, head);
        }
        // Seeding can push the index over its budgets; the oldest-touched
        // nodes (never the ones just seeded) go first.
        let now_ms = index.clock_ms;
        index.evict(now_ms);
    }

    pub fn node_count(&self) -> usize {
        self.index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .nodes
            .len()
    }

    /// Link one request. Pure in-memory work; the returned `upserts` are the
    /// rows the caller must persist.
    pub fn link(&self, input: LinkInput<'_>) -> SessionLink {
        let mut index = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let identity = input.identity;
        let mut touched: Vec<String> = Vec::new();
        index.clock_ms = index.clock_ms.max(input.now_ms);
        let items = compact_items(input.items);

        // 1. The anchor: the declared session/sub-session, or the per-client
        //    anonymous bucket when nothing was declared.
        let anchor = match identity.session_id.as_deref() {
            Some(session_id) => {
                let parent = match (identity.sub_sessions, identity.parent_session_id.as_deref()) {
                    // A declared session naming a parent (pi-agent sub-agent
                    // process): nest it under that parent, creating a
                    // placeholder when the parent has not been seen yet.
                    (SubSessionPolicy::Declared, Some(parent_id))
                        if identity.sub_session_id.is_none() =>
                    {
                        Some(index.declared_node(identity, parent_id, None, &input, &mut touched))
                    }
                    _ => None,
                };
                let session = index.declared_node(
                    identity,
                    session_id,
                    parent.as_deref(),
                    &input,
                    &mut touched,
                );
                match (identity.sub_sessions, identity.sub_session_id.as_deref()) {
                    (SubSessionPolicy::Declared, Some(sub_id)) => {
                        let parent = match identity.parent_session_id.as_deref() {
                            Some(parent_id) => index.declared_node(
                                identity,
                                parent_id,
                                Some(&session),
                                &input,
                                &mut touched,
                            ),
                            None => session.clone(),
                        };
                        index.declared_node(identity, sub_id, Some(&parent), &input, &mut touched)
                    }
                    _ => session,
                }
            }
            None => index.anonymous_node(identity, &input, &mut touched),
        };

        // 2. Candidate chains: the anchor's own and its inferred descendants'.
        let mut candidates = vec![anchor.clone()];
        index.collect_descendants(&anchor, &mut candidates);
        // Score = (shared prefix, set overlap): a chain that shares a prefix
        // beats one that only shares items; among prefix-less matches the
        // one with more items in common wins (a changed system prompt keeps
        // the rest of the conversation). An overlap-only match must be
        // substantial (see [`overlap_is_a_match`]): a side call that shares
        // one user message with the main conversation is not its rewrite.
        let mut best: Option<(String, (usize, usize))> = None;
        for candidate in &candidates {
            let Some(head) = index
                .nodes
                .get(candidate)
                .and_then(|node| node.head.as_ref())
            else {
                continue;
            };
            let prefix = shared_prefix(&head.items, &items);
            let common = overlap(&head.items, &items);
            if prefix == 0 && !overlap_is_a_match(common, &head.items, &items) {
                continue;
            }
            let score = (prefix, common);
            if score != (0, 0) && best.as_ref().is_none_or(|(_, current)| score > *current) {
                best = Some((candidate.clone(), score));
            }
        }

        // 3. Decide the node and the lineage.
        let inference_allowed =
            self.infer_sub_sessions && identity.sub_sessions != SubSessionPolicy::None;
        let (node_id, lineage, chain_parent) = match best {
            Some((node_id, _)) => {
                let head = index
                    .nodes
                    .get(&node_id)
                    .and_then(|node| node.head.as_ref())
                    .expect("best candidate has a head");
                let lineage = classify_compact(&head.items, &items);
                let parent = head.request_id.clone();
                (node_id, lineage, Some(parent))
            }
            None => {
                let anchor_head = index
                    .nodes
                    .get(&anchor)
                    .and_then(|node| node.head.as_ref())
                    .map(|head| (head.request_id.clone(), head.items.len()));
                match anchor_head {
                    None => (anchor.clone(), new_chain(), None),
                    // An unrelated conversation is a sub-session only when it
                    // is smaller than the chain it appears next to: a
                    // sub-agent starts from a fresh, shorter prompt while its
                    // parent has grown through tool round-trips. A larger
                    // conversation arriving after a short side call (a title
                    // request, a summary) is the session's real chain.
                    Some((head_request, head_len))
                        if inference_allowed && head_len > input.items.len() =>
                    {
                        let child = index.inferred_child(
                            &anchor,
                            identity,
                            &input,
                            Some(head_request),
                            &mut touched,
                        );
                        (child, new_chain(), None)
                    }
                    // No chain matched (no prefix, no substantial overlap):
                    // a new conversation on the anchor, after the previous one.
                    Some((head_request, _)) => (anchor.clone(), new_chain(), Some(head_request)),
                }
            }
        };

        // 4. Advance the chosen chain and account the request on its node.
        if let Some(node) = index.nodes.get_mut(&node_id) {
            let old_len = node.head_len();
            let mut kept = items;
            kept.truncate(MAX_HEAD_ITEMS);
            node.head = Some(CompactHead {
                request_id: input.api_call_id.to_string(),
                items: kept.into_boxed_slice(),
            });
            let new_len = node.head_len();
            node.row.request_count += 1;
            node.row.last_seen_ms = input.now_ms;
            if node.row.root_request_id.is_none() {
                node.row.root_request_id = Some(input.api_call_id.to_string());
            }
            if node.row.harness_version.is_none() {
                node.row.harness_version = identity.version.clone();
            }
            index.head_items = index.head_items.saturating_sub(old_len) + new_len;
        }
        // The anchor is in use even when an inferred child took the request;
        // touching it keeps a parent from aging out ahead of its children.
        if anchor != node_id {
            index.touch(&anchor);
        }
        index.touch(&node_id);
        if !touched.contains(&node_id) {
            touched.push(node_id.clone());
        }
        index.evict(input.now_ms);

        let upserts = touched
            .iter()
            .filter_map(|id| index.nodes.get(id).map(|node| node.row.clone()))
            .collect();
        SessionLink {
            session_id: node_id,
            chain_parent_request_id: chain_parent,
            lineage,
            item_count: input.items.len(),
            upserts,
        }
    }
}

fn new_chain() -> Lineage {
    Lineage {
        kind: DivergenceKind::NewChain,
        shared_prefix: 0,
        index: None,
        cache_bust: false,
    }
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

impl Index {
    fn insert_node(&mut self, row: SessionRow, head: Option<CompactHead>) {
        if let Some(external_id) = &row.external_id {
            self.declared
                .insert((row.harness.clone(), external_id.clone()), row.id.clone());
        } else if row.kind == KIND_INFERRED && row.parent_id.is_none() {
            let key = row.client_label.clone().unwrap_or_default();
            self.anonymous
                .insert((row.harness.clone(), key), row.id.clone());
        }
        if let Some(parent_id) = &row.parent_id
            && let Some(parent) = self.nodes.get_mut(parent_id)
        {
            parent.children.push(row.id.clone());
        }
        let id = row.id.clone();
        let node = Node {
            row,
            head,
            children: Vec::new(),
            touch_generation: 0,
            touched_ms: self.clock_ms,
        };
        self.head_items += node.head_len();
        self.nodes.insert(id.clone(), node);
        self.touch(&id);
    }

    fn touch(&mut self, id: &str) {
        let generation = self.next_generation;
        let clock_ms = self.clock_ms;
        let Some(node) = self.nodes.get_mut(id) else {
            return;
        };
        self.next_generation += 1;
        node.touch_generation = generation;
        node.touched_ms = clock_ms;
        self.order.push_back((id.to_string(), generation));
    }

    fn over_budget(&self) -> bool {
        self.nodes.len() > MAX_NODES || self.head_items > MAX_TOTAL_HEAD_ITEMS
    }

    /// Evict oldest-touched nodes while over the node/item budget or idle
    /// past the TTL. Each `order` entry is popped at most once, so the
    /// amortized cost per touch is O(1).
    fn evict(&mut self, now_ms: i64) {
        while let Some((candidate, generation)) = self.order.front() {
            let Some(node) = self.nodes.get(candidate) else {
                self.order.pop_front();
                continue;
            };
            // A later touch keeps the node alive; only its latest entry counts.
            if node.touch_generation != *generation {
                self.order.pop_front();
                continue;
            }
            let expired = self.ttl_ms > 0 && now_ms.saturating_sub(node.touched_ms) > self.ttl_ms;
            if !expired && !self.over_budget() {
                break;
            }
            let Some((candidate, _)) = self.order.pop_front() else {
                break;
            };
            self.remove_node(&candidate);
        }
        // Keep the touch log from growing without bound: drop stale entries
        // once they dominate (amortized O(1) per touch).
        if self.order.len() > self.nodes.len().saturating_mul(4).max(1_024) {
            let nodes = &self.nodes;
            self.order.retain(|(id, generation)| {
                nodes
                    .get(id)
                    .is_some_and(|node| node.touch_generation == *generation)
            });
        }
    }

    fn remove_node(&mut self, id: &str) {
        let Some(node) = self.nodes.remove(id) else {
            return;
        };
        self.head_items = self.head_items.saturating_sub(node.head_len());
        if let Some(external_id) = &node.row.external_id {
            self.declared
                .remove(&(node.row.harness.clone(), external_id.clone()));
        } else if node.row.kind == KIND_INFERRED && node.row.parent_id.is_none() {
            let key = node.row.client_label.clone().unwrap_or_default();
            self.anonymous.remove(&(node.row.harness.clone(), key));
        }
        if let Some(parent_id) = &node.row.parent_id
            && let Some(parent) = self.nodes.get_mut(parent_id)
        {
            parent.children.retain(|child| child != id);
        }
    }

    /// Find or create the declared node for `external_id`.
    fn declared_node(
        &mut self,
        identity: &HarnessIdentity,
        external_id: &str,
        parent_id: Option<&str>,
        input: &LinkInput<'_>,
        touched: &mut Vec<String>,
    ) -> String {
        let key = (identity.harness.clone(), external_id.to_string());
        if let Some(id) = self.declared.get(&key).cloned() {
            // A node first seen as a bare session may learn its parent later.
            if let Some(parent_id) = parent_id
                && parent_id != id
                && self
                    .nodes
                    .get(&id)
                    .is_some_and(|node| node.row.parent_id.is_none())
            {
                let depth = self
                    .nodes
                    .get(parent_id)
                    .map(|parent| parent.row.depth + 1)
                    .unwrap_or(1);
                if let Some(node) = self.nodes.get_mut(&id) {
                    node.row.parent_id = Some(parent_id.to_string());
                    node.row.depth = depth;
                }
                if let Some(parent) = self.nodes.get_mut(parent_id) {
                    parent.children.push(id.clone());
                }
                if !touched.contains(&id) {
                    touched.push(id.clone());
                }
            }
            return id;
        }
        let depth = parent_id
            .and_then(|parent| self.nodes.get(parent))
            .map(|parent| parent.row.depth + 1)
            .unwrap_or(0);
        let row = SessionRow {
            id: new_id(),
            parent_id: parent_id.map(str::to_string),
            kind: KIND_DECLARED.to_string(),
            harness: identity.harness.clone(),
            harness_version: identity.version.clone(),
            external_id: Some(external_id.to_string()),
            session_kind: identity.session_kind.clone(),
            client_label: input.client_label.map(str::to_string),
            virtual_key_id: input.virtual_key_id.map(str::to_string),
            user_id: input.user_id.map(str::to_string),
            depth,
            root_request_id: None,
            spawned_by_request_id: None,
            first_seen_ms: input.now_ms,
            last_seen_ms: input.now_ms,
            request_count: 0,
        };
        let id = row.id.clone();
        touched.push(id.clone());
        self.insert_node(row, None);
        id
    }

    /// Find or create the per-client bucket for requests without a session id.
    fn anonymous_node(
        &mut self,
        identity: &HarnessIdentity,
        input: &LinkInput<'_>,
        touched: &mut Vec<String>,
    ) -> String {
        let client_key = input
            .client_label
            .or(input.virtual_key_id)
            .unwrap_or("")
            .to_string();
        let key = (identity.harness.clone(), client_key.clone());
        if let Some(id) = self.anonymous.get(&key).cloned() {
            return id;
        }
        let row = SessionRow {
            id: new_id(),
            parent_id: None,
            kind: KIND_INFERRED.to_string(),
            harness: identity.harness.clone(),
            harness_version: identity.version.clone(),
            external_id: None,
            session_kind: None,
            client_label: input.client_label.map(str::to_string),
            virtual_key_id: input.virtual_key_id.map(str::to_string),
            user_id: input.user_id.map(str::to_string),
            depth: 0,
            root_request_id: None,
            spawned_by_request_id: None,
            first_seen_ms: input.now_ms,
            last_seen_ms: input.now_ms,
            request_count: 0,
        };
        let id = row.id.clone();
        touched.push(id.clone());
        self.insert_node(row, None);
        id
    }

    fn inferred_child(
        &mut self,
        parent_id: &str,
        identity: &HarnessIdentity,
        input: &LinkInput<'_>,
        spawned_by: Option<String>,
        touched: &mut Vec<String>,
    ) -> String {
        let depth = self
            .nodes
            .get(parent_id)
            .map(|parent| parent.row.depth + 1)
            .unwrap_or(1);
        let row = SessionRow {
            id: new_id(),
            parent_id: Some(parent_id.to_string()),
            kind: KIND_INFERRED.to_string(),
            harness: identity.harness.clone(),
            harness_version: identity.version.clone(),
            external_id: None,
            session_kind: identity.session_kind.clone(),
            client_label: input.client_label.map(str::to_string),
            virtual_key_id: input.virtual_key_id.map(str::to_string),
            user_id: input.user_id.map(str::to_string),
            depth,
            root_request_id: None,
            spawned_by_request_id: spawned_by,
            first_seen_ms: input.now_ms,
            last_seen_ms: input.now_ms,
            request_count: 0,
        };
        let id = row.id.clone();
        touched.push(id.clone());
        self.insert_node(row, None);
        id
    }

    /// Inferred descendants of `node`, breadth-first, bounded.
    fn collect_descendants(&self, node: &str, out: &mut Vec<String>) {
        let mut queue: VecDeque<String> = VecDeque::new();
        queue.push_back(node.to_string());
        while let Some(current) = queue.pop_front() {
            if out.len() >= MAX_CANDIDATE_CHAINS {
                break;
            }
            let Some(state) = self.nodes.get(&current) else {
                continue;
            };
            for child in &state.children {
                if let Some(child_node) = self.nodes.get(child)
                    && child_node.row.kind == KIND_INFERRED
                {
                    out.push(child.clone());
                    queue.push_back(child.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(hash: &str, section: ItemSection, kind: Option<&str>) -> ItemFingerprint {
        ItemFingerprint {
            hash: hash.to_string(),
            section,
            kind: kind.map(str::to_string),
        }
    }

    fn msg(hash: &str) -> ItemFingerprint {
        fp(hash, ItemSection::Message, Some("user"))
    }

    fn convo(hashes: &[&str]) -> Vec<ItemFingerprint> {
        let mut items = vec![
            fp("sys", ItemSection::Instructions, None),
            fp("tool-a", ItemSection::Tool, Some("a")),
        ];
        items.extend(hashes.iter().map(|hash| msg(hash)));
        items
    }

    fn identity(harness: &str, session: Option<&str>) -> HarnessIdentity {
        HarnessIdentity {
            harness: harness.to_string(),
            version: Some("1".to_string()),
            session_id: session.map(str::to_string),
            sub_session_id: None,
            parent_session_id: None,
            session_kind: None,
            sub_sessions: SubSessionPolicy::Declared,
        }
    }

    fn link<'a>(
        linker: &SessionLinker,
        id: &'a str,
        identity: &'a HarnessIdentity,
        items: &'a [ItemFingerprint],
        now: i64,
    ) -> SessionLink {
        linker.link(LinkInput {
            api_call_id: id,
            identity,
            client_label: Some("key-abc"),
            virtual_key_id: Some("vk-1"),
            user_id: None,
            items,
            now_ms: now,
        })
    }

    #[test]
    fn classify_covers_every_divergence_kind() {
        let base = convo(&["u1", "a1"]);
        let appended = convo(&["u1", "a1", "u2"]);
        let lineage = classify(&base, &appended);
        assert_eq!(lineage.kind, DivergenceKind::Append);
        assert_eq!(lineage.shared_prefix, 4);
        assert_eq!(lineage.index, None);
        assert!(!lineage.cache_bust);

        let mut new_sys = appended.clone();
        new_sys[0].hash = "sys2".into();
        let lineage = classify(&base, &new_sys);
        assert_eq!(lineage.kind, DivergenceKind::InstructionsChanged);
        assert_eq!(lineage.index, Some(0));
        assert!(lineage.cache_bust);

        let mut new_tools = appended.clone();
        new_tools[1].hash = "tool-b".into();
        let lineage = classify(&base, &new_tools);
        assert_eq!(lineage.kind, DivergenceKind::ToolsChanged);
        assert_eq!(lineage.index, Some(1));

        // A tool APPENDED to the list shifts every message by one: the
        // predecessor's item at the divergence is a message, the current
        // side's is the new tool. That is a tools change (smoke test 2026-09-12).
        let mut added_tool = appended.clone();
        added_tool.insert(2, fp("tool-b", ItemSection::Tool, Some("b")));
        let lineage = classify(&base, &added_tool);
        assert_eq!(lineage.kind, DivergenceKind::ToolsChanged);
        assert_eq!(lineage.index, Some(2));
        assert_eq!(lineage.shared_prefix, 2);
        assert!(lineage.cache_bust);
        // A tool REMOVED from the list: the predecessor side holds the tool.
        let mut removed_tool = base.clone();
        removed_tool.remove(1);
        assert_eq!(
            classify(&base, &removed_tool).kind,
            DivergenceKind::ToolsChanged
        );

        let mut rewritten = appended.clone();
        rewritten[2].hash = "u1-edited".into();
        let lineage = classify(&base, &rewritten);
        assert_eq!(lineage.kind, DivergenceKind::HistoryRewritten);
        assert_eq!(lineage.index, Some(2));
        assert_eq!(lineage.shared_prefix, 2);

        // Truncated history (compaction) is a rewrite inside the prefix.
        let truncated = convo(&["u1"]);
        let lineage = classify(&base, &truncated);
        assert_eq!(lineage.kind, DivergenceKind::HistoryRewritten);
        assert_eq!(lineage.index, Some(3));

        // A chat-protocol system message counts as instructions.
        let chat_base = vec![fp("s", ItemSection::Message, Some("system")), msg("u1")];
        let chat_new = vec![fp("s2", ItemSection::Message, Some("system")), msg("u1")];
        assert_eq!(
            classify(&chat_base, &chat_new).kind,
            DivergenceKind::InstructionsChanged
        );

        // Nothing in common at all: a new conversation, not a rewrite.
        let unrelated = vec![fp("other-sys", ItemSection::Instructions, None), msg("z")];
        let lineage = classify(&base, &unrelated);
        assert_eq!(lineage.kind, DivergenceKind::NewChain);
        assert!(!lineage.cache_bust);
        // A changed system prompt with the same tools and history is a
        // divergence at index 0 with an empty shared prefix, never a new chain.
        let mut resent = base.clone();
        resent[0].hash = "sys-v2".into();
        let lineage = classify(&base, &resent);
        assert_eq!(lineage.kind, DivergenceKind::InstructionsChanged);
        assert_eq!(lineage.shared_prefix, 0);
        assert_eq!(lineage.index, Some(0));
        assert!(lineage.cache_bust);
        // Same tools, different system prompt AND different history: a
        // sub-agent conversation, not a rewrite of this one.
        let mut sub_agent = convo(&["task"]);
        sub_agent[0].hash = "agent-sys".into();
        assert_eq!(classify(&base, &sub_agent).kind, DivergenceKind::NewChain);
        assert_eq!(classify(&[], &base).kind, DivergenceKind::Append);
    }

    #[test]
    fn declared_session_extends_its_chain_and_flags_cache_busts() {
        let linker = SessionLinker::new(true);
        let id = identity("claude-code", Some("s-1"));
        let first = link(&linker, "r1", &id, &convo(&["u1"]), 10);
        assert_eq!(first.lineage.kind, DivergenceKind::NewChain);
        assert_eq!(first.chain_parent_request_id, None);
        assert_eq!(first.upserts.len(), 1);
        let node = &first.upserts[0];
        assert_eq!(node.kind, KIND_DECLARED);
        assert_eq!(node.external_id.as_deref(), Some("s-1"));
        assert_eq!(node.depth, 0);
        assert_eq!(node.request_count, 1);
        assert_eq!(node.root_request_id.as_deref(), Some("r1"));
        assert_eq!(node.client_label.as_deref(), Some("key-abc"));

        let second = link(&linker, "r2", &id, &convo(&["u1", "a1", "u2"]), 20);
        assert_eq!(second.session_id, first.session_id);
        assert_eq!(second.lineage.kind, DivergenceKind::Append);
        assert_eq!(second.chain_parent_request_id.as_deref(), Some("r1"));
        assert_eq!(second.upserts[0].request_count, 2);
        assert_eq!(second.upserts[0].last_seen_ms, 20);

        let mut busted = convo(&["u1", "a1", "u2", "a2", "u3"]);
        busted[1].hash = "tool-b".into();
        let third = link(&linker, "r3", &id, &busted, 30);
        assert_eq!(third.session_id, first.session_id);
        assert_eq!(third.lineage.kind, DivergenceKind::ToolsChanged);
        assert!(third.lineage.cache_bust);
        assert_eq!(third.chain_parent_request_id.as_deref(), Some("r2"));
        assert_eq!(linker.node_count(), 1);
    }

    #[test]
    fn fingerprints_use_the_identity_hash_not_the_storage_hash() {
        let item = crate::content_store::SplitItem {
            ordinal: 0,
            section: ItemSection::Message,
            kind: Some("user".into()),
            hash: "storage".into(),
            identity: "identity".into(),
            canonical: "{}".into(),
        };
        assert_eq!(ItemFingerprint::from(&item).hash, "identity");
    }

    #[test]
    fn a_request_that_is_a_prefix_of_its_predecessor_keeps_the_cache() {
        // oh-my-pi sends no session id: a new conversation from the same
        // system prompt and first message is a prefix of the previous chain.
        let base = convo(&["u1", "a1", "u2", "a2"]);
        let rewound = convo(&["u1"]);
        let lineage = classify(&base, &rewound);
        assert_eq!(lineage.kind, DivergenceKind::HistoryRewritten);
        assert_eq!(lineage.shared_prefix, 3);
        assert!(!lineage.cache_bust, "the prefix it sends is still cached");
    }

    #[test]
    fn a_side_call_sharing_one_message_is_not_a_rewrite_of_the_main_chain() {
        // opencode's title request reuses the user's message verbatim; with a
        // different system prompt and no tools it must not be classified as
        // the main conversation with changed instructions (a false cache bust).
        let linker = SessionLinker::new(true);
        let id = identity("opencode", None);
        let main = link(&linker, "r1", &id, &convo(&["u1", "a1", "u2"]), 10);
        let side = vec![fp("title-sys", ItemSection::Instructions, None), msg("u1")];
        let title = link(&linker, "r2", &id, &side, 11);
        assert_ne!(title.lineage.kind, DivergenceKind::InstructionsChanged);
        assert!(!title.lineage.cache_bust);
        // Nor the other way round: the main conversation arriving after the
        // title call, sharing two of its three items, is not its rewrite.
        let linker2 = SessionLinker::new(true);
        let mut short = vec![fp("title-sys", ItemSection::Instructions, None), msg("u1")];
        short.push(msg("a1"));
        link(&linker2, "t1", &id, &short, 20);
        let big = link(
            &linker2,
            "t2",
            &id,
            &convo(&["u1", "a1", "u2", "a2", "u3", "a3"]),
            21,
        );
        assert_eq!(big.lineage.kind, DivergenceKind::NewChain);
        assert!(!big.lineage.cache_bust);
        // A genuinely rewritten system prompt still matches by overlap.
        let mut resent = convo(&["u1", "a1", "u2"]);
        resent[0].hash = "sys-v2".into();
        let rewrite = link(&linker, "r3", &id, &resent, 12);
        assert_eq!(rewrite.session_id, main.session_id);
        assert_eq!(rewrite.lineage.kind, DivergenceKind::InstructionsChanged);
        assert!(rewrite.lineage.cache_bust);
    }

    #[test]
    fn a_larger_conversation_is_not_a_sub_session_of_a_side_call() {
        // Claude Code fires a two-item "title" request alongside the first real
        // turn. The real conversation (many more items) must stay the session's
        // own chain, not become an inferred child of the side call.
        let linker = SessionLinker::new(true);
        let id = identity("claude-code", Some("s-1"));
        let mut side = vec![fp("title-sys", ItemSection::Instructions, None)];
        side.push(msg("title-prompt"));
        let title = link(&linker, "r1", &id, &side, 10);
        let main = link(&linker, "r2", &id, &convo(&["u1"]), 11);
        assert_eq!(main.session_id, title.session_id);
        assert_eq!(main.lineage.kind, DivergenceKind::NewChain);
        assert!(!main.lineage.cache_bust);
        assert_eq!(linker.node_count(), 1);
        // The conversation then continues as a plain append on the same chain.
        let next = link(&linker, "r3", &id, &convo(&["u1", "a1", "u2"]), 12);
        assert_eq!(next.session_id, title.session_id);
        assert_eq!(next.lineage.kind, DivergenceKind::Append);
        assert_eq!(next.chain_parent_request_id.as_deref(), Some("r2"));
    }

    #[test]
    fn a_new_conversation_inside_a_session_becomes_an_inferred_sub_session() {
        let linker = SessionLinker::new(true);
        let id = identity("claude-code", Some("s-1"));
        // The main chain has already made a tool round-trip when it spawns.
        let main = link(&linker, "r1", &id, &convo(&["u1", "a1", "u2"]), 10);
        // A sub-agent shares the session id but starts from a different prompt.
        let mut agent_items = convo(&["agent-task"]);
        agent_items[0].hash = "agent-sys".into();
        let sub = link(&linker, "r2", &id, &agent_items, 11);
        assert_ne!(sub.session_id, main.session_id);
        assert_eq!(sub.lineage.kind, DivergenceKind::NewChain);
        let sub_row = sub
            .upserts
            .iter()
            .find(|row| row.id == sub.session_id)
            .unwrap();
        assert_eq!(sub_row.kind, KIND_INFERRED);
        assert_eq!(sub_row.parent_id.as_deref(), Some(main.session_id.as_str()));
        assert_eq!(sub_row.depth, 1);
        assert_eq!(sub_row.spawned_by_request_id.as_deref(), Some("r1"));

        // The sub-agent's next turn extends the sub-session, not the main chain.
        let sub_next = link(
            &linker,
            "r3",
            &id,
            &{
                let mut items = agent_items.clone();
                items.push(msg("agent-a1"));
                items.push(msg("agent-u2"));
                items
            },
            12,
        );
        assert_eq!(sub_next.session_id, sub.session_id);
        assert_eq!(sub_next.lineage.kind, DivergenceKind::Append);
        assert_eq!(sub_next.chain_parent_request_id.as_deref(), Some("r2"));

        // And the main thread continues on its own chain afterwards.
        let main_next = link(
            &linker,
            "r4",
            &id,
            &convo(&["u1", "a1", "u2", "a2", "u3"]),
            13,
        );
        assert_eq!(main_next.session_id, main.session_id);
        assert_eq!(main_next.chain_parent_request_id.as_deref(), Some("r1"));
        assert_eq!(linker.node_count(), 2);
    }

    #[test]
    fn inference_off_keeps_everything_on_the_declared_chain() {
        let linker = SessionLinker::new(false);
        let id = identity("codex", Some("s-1"));
        let main = link(&linker, "r1", &id, &convo(&["u1"]), 10);
        // An unrelated conversation stays on the declared chain, recorded as a
        // new chain (not a cache bust) with the previous request as parent.
        let mut other = convo(&["x"]);
        other[0].hash = "other-sys".into();
        let next = link(&linker, "r2", &id, &other, 11);
        assert_eq!(next.session_id, main.session_id);
        assert_eq!(next.lineage.kind, DivergenceKind::NewChain);
        assert!(!next.lineage.cache_bust);
        assert_eq!(next.chain_parent_request_id.as_deref(), Some("r1"));
        // A re-sent conversation with a changed system prompt is a cache bust.
        let mut resent = other.clone();
        resent[0].hash = "other-sys-v2".into();
        resent.push(msg("x-a1"));
        let bust = link(&linker, "r3", &id, &resent, 12);
        assert_eq!(bust.session_id, main.session_id);
        assert_eq!(bust.lineage.kind, DivergenceKind::InstructionsChanged);
        assert!(bust.lineage.cache_bust);
        assert_eq!(bust.chain_parent_request_id.as_deref(), Some("r2"));
        assert_eq!(linker.node_count(), 1);
    }

    #[test]
    fn declared_sub_sessions_nest_under_their_declared_parents() {
        let linker = SessionLinker::new(true);
        // Codex: the root thread, then a spawned thread naming the session as
        // parent, then a nested thread naming the spawned thread.
        let root = identity("codex", Some("s-1"));
        let main = link(&linker, "r1", &root, &convo(&["u1"]), 10);
        let mut spawned = identity("codex", Some("s-1"));
        spawned.sub_session_id = Some("t-2".to_string());
        spawned.session_kind = Some("collab_spawn".to_string());
        let child = link(&linker, "r2", &spawned, &convo(&["task"]), 11);
        assert_ne!(child.session_id, main.session_id);
        let child_row = child
            .upserts
            .iter()
            .find(|row| row.id == child.session_id)
            .unwrap();
        assert_eq!(child_row.kind, KIND_DECLARED);
        assert_eq!(child_row.external_id.as_deref(), Some("t-2"));
        assert_eq!(
            child_row.parent_id.as_deref(),
            Some(main.session_id.as_str())
        );
        assert_eq!(child_row.depth, 1);
        assert_eq!(child_row.session_kind.as_deref(), Some("collab_spawn"));

        let mut nested = identity("codex", Some("s-1"));
        nested.sub_session_id = Some("t-3".to_string());
        nested.parent_session_id = Some("t-2".to_string());
        let grandchild = link(&linker, "r3", &nested, &convo(&["sub-task"]), 12);
        let row = grandchild
            .upserts
            .iter()
            .find(|row| row.id == grandchild.session_id)
            .unwrap();
        assert_eq!(row.parent_id.as_deref(), Some(child.session_id.as_str()));
        assert_eq!(row.depth, 2);
        assert_eq!(linker.node_count(), 3);
    }

    #[test]
    fn pi_agent_child_process_declares_its_parent_session() {
        let linker = SessionLinker::new(true);
        let parent = identity("pi-agent", Some("p-1"));
        let main = link(&linker, "r1", &parent, &convo(&["u1"]), 10);
        let mut child = identity("pi-agent", Some("c-1"));
        child.parent_session_id = Some("p-1".to_string());
        child.session_kind = Some("subagent".to_string());
        let sub = link(&linker, "r2", &child, &convo(&["task"]), 11);
        let row = sub
            .upserts
            .iter()
            .find(|row| row.id == sub.session_id)
            .unwrap();
        assert_eq!(row.parent_id.as_deref(), Some(main.session_id.as_str()));
        assert_eq!(row.external_id.as_deref(), Some("c-1"));
        assert_eq!(row.depth, 1);

        // A child seen before its parent gets a placeholder parent that the
        // parent's own first request later fills in.
        let linker = SessionLinker::new(true);
        let sub = link(&linker, "r1", &child, &convo(&["task"]), 10);
        assert_eq!(sub.upserts.len(), 2, "placeholder parent + child");
        let placeholder = sub
            .upserts
            .iter()
            .find(|row| row.external_id.as_deref() == Some("p-1"))
            .unwrap();
        assert_eq!(placeholder.request_count, 0);
        let main = link(&linker, "r2", &parent, &convo(&["u1"]), 11);
        assert_eq!(main.session_id, placeholder.id);
        assert_eq!(main.upserts[0].request_count, 1);
    }

    #[test]
    fn requests_without_a_session_id_bucket_per_client_and_infer_chains() {
        let linker = SessionLinker::new(true);
        let id = identity("generic", None);
        let first = link(&linker, "r1", &id, &convo(&["u1"]), 10);
        let bucket = &first.upserts[0];
        assert_eq!(bucket.kind, KIND_INFERRED);
        assert_eq!(bucket.external_id, None);
        assert_eq!(bucket.client_label.as_deref(), Some("key-abc"));
        let cont = link(&linker, "r2", &id, &convo(&["u1", "a1", "u2"]), 11);
        assert_eq!(cont.session_id, first.session_id);
        assert_eq!(cont.lineage.kind, DivergenceKind::Append);
        let mut other = convo(&["v1"]);
        other[0].hash = "other".into();
        let second_convo = link(&linker, "r3", &id, &other, 12);
        assert_ne!(second_convo.session_id, first.session_id);
        let row = second_convo
            .upserts
            .iter()
            .find(|row| row.id == second_convo.session_id)
            .unwrap();
        assert_eq!(row.parent_id.as_deref(), Some(first.session_id.as_str()));
    }

    #[test]
    fn seeding_restores_a_declared_session_and_its_head() {
        let linker = SessionLinker::new(true);
        let row = SessionRow {
            id: "node-1".to_string(),
            parent_id: None,
            kind: KIND_DECLARED.to_string(),
            harness: "claude-code".to_string(),
            harness_version: None,
            external_id: Some("s-1".to_string()),
            session_kind: None,
            client_label: None,
            virtual_key_id: None,
            user_id: None,
            depth: 0,
            root_request_id: Some("old-1".to_string()),
            spawned_by_request_id: None,
            first_seen_ms: 1,
            last_seen_ms: 2,
            request_count: 5,
        };
        assert!(!linker.knows_declared("claude-code", "s-1"));
        linker.seed(vec![(
            row,
            Some(ChainHead {
                request_id: "old-1".to_string(),
                items: convo(&["u1"]),
            }),
        )]);
        assert!(linker.knows_declared("claude-code", "s-1"));
        let id = identity("claude-code", Some("s-1"));
        let next = link(&linker, "r1", &id, &convo(&["u1", "a1", "u2"]), 10);
        assert_eq!(next.session_id, "node-1");
        assert_eq!(next.lineage.kind, DivergenceKind::Append);
        assert_eq!(next.chain_parent_request_id.as_deref(), Some("old-1"));
        assert_eq!(next.upserts[0].request_count, 6);
    }

    #[test]
    fn eviction_keeps_the_index_bounded() {
        let linker = SessionLinker::new(true);
        for n in 0..(MAX_NODES + 50) {
            let id = identity("generic", Some(&format!("s-{n}")));
            link(&linker, "r", &id, &convo(&["u"]), n as i64);
        }
        assert!(linker.node_count() <= MAX_NODES);
        assert!(!linker.knows_declared("generic", "s-0"));
        assert!(linker.knows_declared("generic", &format!("s-{}", MAX_NODES + 49)));
    }

    #[test]
    fn a_retouched_node_survives_count_eviction() {
        let linker = SessionLinker::with_ttl_ms(true, 0);
        let hot = identity("generic", Some("hot"));
        link(&linker, "r0", &hot, &convo(&["u"]), 0);
        for n in 0..(MAX_NODES + 50) {
            let id = identity("generic", Some(&format!("s-{n}")));
            link(&linker, "r", &id, &convo(&["u"]), n as i64);
            if n % 100 == 0 {
                link(&linker, "r-hot", &hot, &convo(&["u"]), n as i64);
            }
        }
        assert!(linker.node_count() <= MAX_NODES);
        assert!(linker.knows_declared("generic", "hot"));
        assert!(!linker.knows_declared("generic", "s-0"));
        // The touch log stays proportional to the live node count.
        let index = linker.index.lock().unwrap();
        assert!(index.order.len() <= index.nodes.len().saturating_mul(4).max(1_024));
    }

    #[test]
    fn idle_nodes_expire_after_the_ttl() {
        let ttl_ms = 1_000;
        let linker = SessionLinker::with_ttl_ms(true, ttl_ms);
        let idle = identity("generic", Some("idle"));
        let busy = identity("generic", Some("busy"));
        link(&linker, "r1", &idle, &convo(&["u"]), 0);
        link(&linker, "r2", &busy, &convo(&["u"]), 500);
        link(&linker, "r3", &busy, &convo(&["u", "a"]), ttl_ms + 1);
        assert!(!linker.knows_declared("generic", "idle"));
        assert!(linker.knows_declared("generic", "busy"));
        assert_eq!(linker.node_count(), 1);
        assert_eq!(linker.index.lock().unwrap().head_items, 4);
    }

    #[test]
    fn total_head_items_are_budgeted_across_nodes() {
        let linker = SessionLinker::with_ttl_ms(true, 0);
        let big: Vec<ItemFingerprint> = (0..MAX_HEAD_ITEMS)
            .map(|n| msg(&format!("item-{n}")))
            .collect();
        let sessions = MAX_TOTAL_HEAD_ITEMS / MAX_HEAD_ITEMS + 5;
        for n in 0..sessions {
            let id = identity("generic", Some(&format!("big-{n}")));
            link(&linker, "r", &id, &big, n as i64);
        }
        let index = linker.index.lock().unwrap();
        assert!(index.head_items <= MAX_TOTAL_HEAD_ITEMS);
        let actual: usize = index.nodes.values().map(Node::head_len).sum();
        assert_eq!(index.head_items, actual);
        assert!(index.nodes.len() < sessions);
    }

    #[test]
    fn compact_hash_preserves_string_equality() {
        let hex_lower = "ab".repeat(32);
        let hex_upper = "AB".repeat(32);
        assert_eq!(compact_hash(&hex_lower), [0xab; 32]);
        assert_ne!(compact_hash(&hex_lower), compact_hash(&hex_upper));
        assert_eq!(compact_hash("u1"), compact_hash("u1"));
        assert_ne!(compact_hash("u1"), compact_hash("u2"));
        // A non-hex string never aliases a decoded hex identity.
        assert_ne!(compact_hash(&hex_upper), [0xab; 32]);
        assert_eq!(std::mem::size_of::<CompactItem>(), 34);
    }
}
