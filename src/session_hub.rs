//! SessionHub — the live in-memory cut of ACTIVE sessions for the dashboard.
//!
//! The dashboard's primary view is "what is running right now": the sessions
//! with activity in the last 15 minutes, their request counts in the 1/5/10/15
//! minute windows, and the actual requests underneath. Durable history answers
//! "what happened" from SQL; this hub answers "what is live" without a store
//! round-trip, mirroring how the FlowStore fronts `/dashboard/api/flows`.
//!
//! Design invariants:
//!  - **Gated like the FlowStore**: a `disabled()` no-op hub is used when
//!    `--with-debug-ui` is off, so production (no dashboard) pays zero cost —
//!    every emit returns before building anything (the `MonitorHub::emit_with`
//!    laziness pattern).
//!  - **Two feed seams, no third**: requests are recorded at the persistence
//!    link seam (`http.rs`, where `SessionLinker.link` runs) and finalized at
//!    the persistence terminal seam (`PersistenceTerminalGuard::finalize`).
//!    Both already exist on every instrumented request regardless of the debug
//!    UI, so the hub sees exactly the flows the durable store sees.
//!  - **Bounded memory**: per session a ring of the last [`REQUEST_RING_LEN`]
//!    request stubs; sessions idle past [`SESSION_TTL_MS`] are dropped on the
//!    next cut or emit. The stub carries scalar metadata only — never a body,
//!    never a `Bytes` slice (AGENTS.md line 144).
//!  - **Per-domain cursor**: ONE monotonic `seq` for the sessions domain
//!    (`{domain, seq}` cursors; never a global watermark). The WS loop stamps
//!    each broadcast with the seq captured atomically with the state change.
//!
//! The hub does NOT own session attribution — `SessionRow` (from the linker)
//! carries `client_label`/`virtual_key_id`/`user_id`, and the frontend resolves
//! user/key labels via the existing users/keys queries.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// How long a session stays in the hub after its last request.
pub const SESSION_TTL_MS: u64 = 15 * 60 * 1_000;
/// Per-session request stub ring length.
const REQUEST_RING_LEN: usize = 200;
/// Broadcast channel capacity (subscribers, i.e. dashboard WS connections).
const BROADCAST_CAPACITY: usize = 64;
/// Upper bound on sessions retained in the hub.
const MAX_SESSIONS: usize = 2_000;

/// One request stub: the scalar facts the active-session list needs. Body-free.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct SessionRequestStub {
    pub api_call_id: String,
    pub client_model: String,
    pub created_at_ms: u64,
    /// `running`, `completed` or `failed`.
    pub status: String,
    /// Set at the terminal seam; `None` while running or when the upstream did
    /// not report the class.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
}

/// A session in the active cut: the linker's row plus the live request ring.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct ActiveSession {
    #[serde(flatten)]
    pub row: crate::sessions::SessionRow,
    /// Newest-first request stubs within the activity window.
    pub requests: Vec<SessionRequestStub>,
    /// Request counts in the trailing 1/5/10/15-minute windows (derived at cut
    /// time from the ring; a request older than the ring may undercount the
    /// 15-minute window — acceptable, the ring covers 200 requests).
    pub requests_1m: u64,
    pub requests_5m: u64,
    pub requests_10m: u64,
    pub requests_15m: u64,
}

/// One broadcast event: what changed, with the sessions-domain seq.
#[derive(Debug, Clone)]
pub struct SessionUpdate {
    /// Monotonic sessions-domain sequence at emit time.
    pub seq: u64,
    /// Session ids whose row or requests changed (upsert semantics).
    pub touched: Vec<String>,
}

/// The terminal-seam input for [`SessionHub::record_terminal`] (kept as one
/// struct so the seam reads at the call site and clippy's arg cap holds).
#[derive(Debug, Clone)]
pub struct SessionTerminal<'a> {
    pub api_call_id: &'a str,
    pub status: &'a str,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub error: Option<String>,
    pub terminal_reason: Option<String>,
    pub completed_at_ms: u64,
}

#[derive(Debug, Default)]
struct SessionState {
    sessions: HashMap<String, HubSession>,
    /// api_call_id → owning session id, so the terminal seam is a lookup, not
    /// a scan over every session's ring under the mutex.
    by_request: HashMap<String, String>,
}

#[derive(Debug)]
struct HubSession {
    row: crate::sessions::SessionRow,
    /// Newest-first stubs.
    requests: Vec<SessionRequestStub>,
    last_activity_ms: u64,
}

#[derive(Clone)]
pub struct SessionHub {
    enabled: bool,
    tx: broadcast::Sender<SessionUpdate>,
    state: Arc<Mutex<SessionState>>,
    seq: Arc<AtomicU64>,
}

impl SessionHub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            enabled: true,
            tx,
            state: Arc::new(Mutex::new(SessionState::default())),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The no-op hub for production (no `--with-debug-ui`): every method
    /// early-returns; no allocation, no channel traffic.
    pub fn disabled() -> Self {
        let (tx, _) = broadcast::channel(1);
        Self {
            enabled: false,
            tx,
            state: Arc::new(Mutex::new(SessionState::default())),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SessionUpdate> {
        self.tx.subscribe()
    }

    /// The sessions-domain cursor (per-domain `{domain, seq}`; starts at 0 and
    /// only increases). Used by the WS loop to stamp outgoing frames.
    pub fn last_seq(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }

    /// Record a request BEGIN at the link seam. `rows` are the linker's
    /// upserts (the touched session nodes, newest state); `primary` is the node
    /// the request was linked into. Bounded: a full/absent session is created
    /// from the primary row.
    pub fn record_begin(
        &self,
        rows: &[crate::sessions::SessionRow],
        primary_id: &str,
        stub: SessionRequestStub,
    ) {
        if !self.enabled {
            return;
        }
        let mut state = self.state.lock().expect("session hub lock poisoned");
        let mut touched: Vec<String> = Vec::new();
        let now = stub.created_at_ms;
        for row in rows {
            let entry = state
                .sessions
                .entry(row.id.clone())
                .or_insert_with(|| HubSession {
                    row: row.clone(),
                    requests: Vec::new(),
                    last_activity_ms: now,
                });
            // The linker's row is the authoritative newest state (counters,
            // last_seen, attribution).
            entry.row = row.clone();
            entry.last_activity_ms = entry.last_activity_ms.max(now);
            if !touched.contains(&row.id) {
                touched.push(row.id.clone());
            }
        }
        if state.sessions.contains_key(primary_id) {
            state
                .by_request
                .insert(stub.api_call_id.clone(), primary_id.to_string());
        }
        if let Some(entry) = state.sessions.get_mut(primary_id) {
            entry.requests.insert(0, stub);
            // Insert first, then drain the overflow — the drained stubs are
            // exactly the ones leaving the ring. Drop their index entries so
            // by_request never holds ids whose terminal can never arrive.
            if entry.requests.len() > REQUEST_RING_LEN {
                let evicted: Vec<String> = entry
                    .requests
                    .drain(REQUEST_RING_LEN..)
                    .map(|stub| stub.api_call_id)
                    .collect();
                for id in evicted {
                    state.by_request.remove(&id);
                }
            }
        }
        self.prune_locked(&mut state, now);
        drop(state);
        self.bump_and_broadcast(touched);
    }

    /// Finalize a request at the terminal seam: set its status/usage on the
    /// ring stub. No-op (returns silently) when the begin was never recorded —
    /// the hub may have been enabled after the request started, or the session
    /// aged out.
    pub fn record_terminal(&self, terminal: SessionTerminal<'_>) {
        let SessionTerminal {
            api_call_id,
            status,
            input_tokens,
            output_tokens,
            cached_tokens,
            reasoning_tokens,
            error,
            terminal_reason,
            completed_at_ms,
        } = terminal;
        if !self.enabled {
            return;
        }
        let mut state = self.state.lock().expect("session hub lock poisoned");
        let mut touched: Option<String> = None;
        if let Some(session_id) = state.by_request.remove(api_call_id)
            && let Some(entry) = state.sessions.get_mut(&session_id)
            && let Some(stub) = entry
                .requests
                .iter_mut()
                .find(|stub| stub.api_call_id == api_call_id)
        {
            stub.status = status.to_string();
            stub.input_tokens = input_tokens;
            stub.output_tokens = output_tokens;
            stub.cached_tokens = cached_tokens;
            stub.reasoning_tokens = reasoning_tokens;
            stub.error = error;
            stub.terminal_reason = terminal_reason;
            touched = Some(session_id);
        }
        drop(state);
        if let Some(id) = touched {
            self.bump_and_broadcast(vec![id]);
        }
        let _ = completed_at_ms;
    }

    /// The active-session cut for the REST endpoint: every session with
    /// activity at/after `now - SESSION_TTL_MS`, newest activity first.
    pub fn active_sessions(&self, now_ms: u64) -> Vec<ActiveSession> {
        if !self.enabled {
            return Vec::new();
        }
        let mut state = self.state.lock().expect("session hub lock poisoned");
        self.prune_locked(&mut state, now_ms);
        let cutoff = now_ms.saturating_sub(SESSION_TTL_MS);
        let mut sessions: Vec<ActiveSession> = state
            .sessions
            .values()
            .filter(|entry| entry.last_activity_ms >= cutoff)
            .map(|entry| {
                let (r1, r5, r10, r15) = window_counts(&entry.requests, now_ms);
                ActiveSession {
                    row: entry.row.clone(),
                    requests: entry.requests.clone(),
                    requests_1m: r1,
                    requests_5m: r5,
                    requests_10m: r10,
                    requests_15m: r15,
                }
            })
            .collect();
        sessions.sort_by(|a, b| {
            b.row
                .last_seen_ms
                .cmp(&a.row.last_seen_ms)
                .then_with(|| a.row.id.cmp(&b.row.id))
        });
        sessions
    }

    /// The hub's own seq for a fresh `sessions` cursor baseline (the WS
    /// snapshot doesn't carry sessions, so the SPA starts at 0 and the first
    /// live frame is simply always accepted).
    pub fn snapshot_seq(&self) -> u64 {
        self.last_seq()
    }

    fn prune_locked(&self, state: &mut SessionState, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(SESSION_TTL_MS);
        // Collect the dropped sessions' request ids BEFORE the retain removes
        // the entries, so their by_request index entries can be purged after.
        let mut dropped_request_ids: Vec<String> = Vec::new();
        state.sessions.retain(|_id, entry| {
            let keep = entry.last_activity_ms >= cutoff || entry.row.last_seen_ms as u64 >= cutoff;
            if !keep {
                dropped_request_ids
                    .extend(entry.requests.iter().map(|stub| stub.api_call_id.clone()));
            }
            keep
        });
        for id in dropped_request_ids {
            state.by_request.remove(&id);
        }
        // Hard cap: drop the oldest-active sessions beyond the bound.
        if state.sessions.len() > MAX_SESSIONS {
            let mut by_activity: Vec<(String, u64)> = state
                .sessions
                .iter()
                .map(|(id, entry)| (id.clone(), entry.last_activity_ms))
                .collect();
            by_activity.sort_by_key(|(_, activity)| *activity);
            let excess = state.sessions.len() - MAX_SESSIONS;
            let mut evicted_request_ids: Vec<String> = Vec::new();
            for (id, _) in by_activity.into_iter().take(excess) {
                if let Some(entry) = state.sessions.remove(&id) {
                    evicted_request_ids
                        .extend(entry.requests.into_iter().map(|stub| stub.api_call_id));
                }
            }
            for id in evicted_request_ids {
                state.by_request.remove(&id);
            }
        }
    }

    fn bump_and_broadcast(&self, touched: Vec<String>) {
        if touched.is_empty() {
            return;
        }
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.tx.send(SessionUpdate { seq, touched });
    }
}

impl Default for SessionHub {
    fn default() -> Self {
        Self::new()
    }
}

fn window_counts(requests: &[SessionRequestStub], now_ms: u64) -> (u64, u64, u64, u64) {
    let mut counts = [0u64; 4];
    let windows = [60_000u64, 300_000, 600_000, 900_000];
    for stub in requests {
        for (i, window) in windows.iter().enumerate() {
            if stub.created_at_ms >= now_ms.saturating_sub(*window) {
                counts[i] += 1;
            }
        }
    }
    (counts[0], counts[1], counts[2], counts[3])
}

/// Convenience alias used by the gateway.
pub type SharedSessionHub = Arc<SessionHub>;

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, last_seen: i64) -> crate::sessions::SessionRow {
        crate::sessions::SessionRow {
            id: id.to_string(),
            parent_id: None,
            kind: "declared".to_string(),
            harness: "claude-code".to_string(),
            harness_version: None,
            external_id: Some(format!("ext-{id}")),
            session_kind: None,
            client_label: Some("key-abc".to_string()),
            virtual_key_id: Some("vk-1".to_string()),
            user_id: Some("user-7".to_string()),
            depth: 0,
            root_request_id: None,
            spawned_by_request_id: None,
            first_seen_ms: last_seen,
            last_seen_ms: last_seen,
            request_count: 1,
        }
    }

    fn stub(id: &str, at: u64) -> SessionRequestStub {
        SessionRequestStub {
            api_call_id: id.to_string(),
            client_model: "m".to_string(),
            created_at_ms: at,
            status: "running".to_string(),
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            reasoning_tokens: None,
            error: None,
            terminal_reason: None,
        }
    }

    #[test]
    fn disabled_hub_is_a_no_op() {
        let hub = SessionHub::disabled();
        assert!(!hub.is_enabled());
        hub.record_begin(&[row("s", 1)], "s", stub("r", 1));
        assert!(hub.active_sessions(10_000).is_empty());
        hub.record_terminal(SessionTerminal {
            api_call_id: "r",
            status: "completed",
            input_tokens: Some(1),
            output_tokens: Some(1),
            cached_tokens: None,
            reasoning_tokens: None,
            error: None,
            terminal_reason: None,
            completed_at_ms: 2,
        });
        assert_eq!(hub.last_seq(), 0);
    }

    #[test]
    fn begin_records_row_requests_and_broadcasts() {
        let hub = SessionHub::new();
        let mut rx = hub.subscribe();
        let session_row = row("s1", 100);
        hub.record_begin(std::slice::from_ref(&session_row), "s1", stub("r1", 100));
        let update = rx.try_recv().expect("broadcast");
        assert_eq!(update.seq, 1);
        assert_eq!(update.touched, vec!["s1".to_string()]);

        let active = hub.active_sessions(200);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].row.id, "s1");
        assert_eq!(active[0].row.user_id.as_deref(), Some("user-7"));
        assert_eq!(active[0].requests.len(), 1);
        assert_eq!(active[0].requests_1m, 1);
        assert_eq!(active[0].requests_15m, 1);
    }

    #[test]
    fn terminal_updates_the_stub_status_and_usage() {
        let hub = SessionHub::new();
        hub.record_begin(std::slice::from_ref(&row("s1", 100)), "s1", stub("r1", 100));
        hub.record_terminal(SessionTerminal {
            api_call_id: "r1",
            status: "completed",
            input_tokens: Some(120),
            output_tokens: Some(30),
            cached_tokens: Some(80),
            reasoning_tokens: None,
            error: None,
            terminal_reason: Some("stop".to_string()),
            completed_at_ms: 180,
        });
        let active = hub.active_sessions(200);
        let stub = &active[0].requests[0];
        assert_eq!(stub.status, "completed");
        assert_eq!(stub.input_tokens, Some(120));
        assert_eq!(stub.output_tokens, Some(30));
        assert_eq!(stub.reasoning_tokens, None);
        assert_eq!(stub.terminal_reason.as_deref(), Some("stop"));
    }

    #[test]
    fn terminal_for_unknown_request_is_silent() {
        let hub = SessionHub::new();
        let mut rx = hub.subscribe();
        hub.record_terminal(SessionTerminal {
            api_call_id: "nope",
            status: "failed",
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            reasoning_tokens: None,
            error: Some("e".to_string()),
            terminal_reason: None,
            completed_at_ms: 1,
        });
        assert!(rx.try_recv().is_err());
        assert_eq!(hub.last_seq(), 0);
    }

    #[test]
    fn window_counts_use_the_ring() {
        let hub = SessionHub::new();
        let now = 30 * 60_000u64;
        hub.record_begin(
            std::slice::from_ref(&row("s1", 100)),
            "s1",
            stub("r1", now - 30_000),
        );
        hub.record_begin(
            std::slice::from_ref(&row("s1", 100)),
            "s1",
            stub("r2", now - 4 * 60_000),
        );
        hub.record_begin(
            std::slice::from_ref(&row("s1", 100)),
            "s1",
            stub("r3", now - 12 * 60_000),
        );
        hub.record_begin(
            std::slice::from_ref(&row("s1", 100)),
            "s1",
            stub("r4", now - 20 * 60_000),
        );
        let active = hub.active_sessions(now);
        assert_eq!(
            (
                active[0].requests_1m,
                active[0].requests_5m,
                active[0].requests_10m,
                active[0].requests_15m
            ),
            (1, 2, 2, 3)
        );
        // The 20-minute-old request stays in the ring but out of every window.
        assert_eq!(active[0].requests.len(), 4);
    }

    /// The by_request index stays exact under ring eviction and session prune:
    /// an evicted stub's terminal is a silent no-op (id no longer indexed),
    /// while a surviving stub's terminal still lands.
    #[test]
    fn evicted_stubs_leave_the_index_but_survivors_stay() {
        let hub = SessionHub::new();
        let base = 10_000_000u64;
        let session_row = row("s1", 1);
        // Fill the ring exactly, so the NEXT begin evicts the oldest stub.
        for i in 0..REQUEST_RING_LEN {
            hub.record_begin(
                std::slice::from_ref(&session_row),
                "s1",
                stub(&format!("r{i}"), base + i as u64),
            );
        }
        hub.record_begin(
            std::slice::from_ref(&session_row),
            "s1",
            stub(
                &format!("r{REQUEST_RING_LEN}"),
                base + REQUEST_RING_LEN as u64,
            ),
        );
        // The evicted oldest stub's terminal is a no-op: no broadcast.
        let mut rx = hub.subscribe();
        hub.record_terminal(SessionTerminal {
            api_call_id: "r0",
            status: "completed",
            input_tokens: Some(1),
            output_tokens: Some(1),
            cached_tokens: None,
            reasoning_tokens: None,
            error: None,
            terminal_reason: None,
            completed_at_ms: base,
        });
        assert!(rx.try_recv().is_err(), "evicted stub is not indexed");
        // A survivor's terminal still updates.
        hub.record_terminal(SessionTerminal {
            api_call_id: &format!("r{REQUEST_RING_LEN}"),
            status: "completed",
            input_tokens: Some(5),
            output_tokens: Some(2),
            cached_tokens: None,
            reasoning_tokens: None,
            error: None,
            terminal_reason: None,
            completed_at_ms: base + 1,
        });
        let active = hub.active_sessions(base + 60_000);
        let stub = active[0]
            .requests
            .iter()
            .find(|stub| stub.api_call_id == format!("r{REQUEST_RING_LEN}"))
            .expect("survivor still in ring");
        assert_eq!(stub.status, "completed");
    }

    #[test]
    fn sessions_age_out_after_ttl() {
        let hub = SessionHub::new();
        hub.record_begin(&[row("s1", 1_000)], "s1", stub("r1", 1_000));
        let still = hub.active_sessions(1_000 + SESSION_TTL_MS - 1);
        assert_eq!(still.len(), 1);
        let gone = hub.active_sessions(1_000 + SESSION_TTL_MS + 1);
        assert!(gone.is_empty());
    }

    #[test]
    fn newest_first_and_ring_bounded() {
        let hub = SessionHub::new();
        let base = 10_000_000u64;
        let session_row = row("s1", 1);
        for i in 0..(REQUEST_RING_LEN + 5) {
            hub.record_begin(
                std::slice::from_ref(&session_row),
                "s1",
                stub(&format!("r{i}"), base + i as u64),
            );
        }
        let active = hub.active_sessions(base + REQUEST_RING_LEN as u64 + 10);
        assert_eq!(active[0].requests.len(), REQUEST_RING_LEN);
        assert_eq!(
            active[0].requests[0].api_call_id,
            format!("r{}", REQUEST_RING_LEN + 4)
        );
    }
}
