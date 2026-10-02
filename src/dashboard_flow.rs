//! D1 — `DashboardFlowStore`: the authoritative per-flow record store and the
//! on-wire capture seam that feeds the transformation inspector + metrics.
//!
//! This module holds STORE LOGIC ONLY. The capped + redacting body serializer and
//! the single sensitive-key authority live in [`crate::redaction`] (D1 R1 #10), so
//! there is exactly one definition of "what is sensitive" and one O(CAP) capture
//! primitive shared with the inbound-trace logger.
//!
//! Dashboard flow architecture:
//! - The store is `DashboardFlowStore { state: Mutex<DashboardFlowState> }` with a
//!   `MonitorHub`-style `new()`/`disabled()` split: when `--with-debug-ui` is off
//!   the store is `disabled()` and EVERY operation is a no-op, so the production
//!   hot path keeps zero overhead.
//! - `by_id` (the record map) and `order` (the LRU/insertion deque) are mutated
//!   together under the ONE `Mutex` — there is no exterior LRU that could drift.
//! - Records are `Arc<FlowRecord>` replaced copy-on-write on mutation: each
//!   mutator clones the inner record, edits the clone, and swaps the `Arc`. The
//!   `claim: Arc<AtomicU8>` is CLONED (shared) across COW copies so the atomic
//!   identity persists for D3's `OpenL0 → ClaimedL1 → Finalized` CAS guard. D1
//!   only ALLOCATES `claim` as `OpenL0`; D3 owns the transitions.
//! - Bodies are owned, capped `Arc<[u8]>` produced by the redacting STREAMING JSON
//!   serializer in `redaction`: peak serializer memory is O(CAP), not O(body), and
//!   a `Bytes::slice` of the inbound middleware buffer bounded by `max_request_body_bytes` is NEVER retained (a slice
//!   keeps the whole backing allocation alive — AGENTS.md don't-rule). Secrets are
//!   redacted INLINE (sensitive keys, image URIs incl. `\uXXXX`-escaped forms,
//!   over-long scalars + keys), so no secret persists even in a preview.
//! - Every dynamic scalar string the record retains (ids, method, uri, models,
//!   upstream target, terminal reason) is CAPPED at `SCALAR_CAP` and COUNTED in
//!   `live_summary_bytes` (D1 R1 #5).
//! - The live store enforces three caps under the same lock AFTER EVERY mutation
//!   AND on every read: the record count (512), a 30-minute TTL, and a total live
//!   summary-byte quota (64 MiB) that evicts OLDEST BODIES first (sets their body
//!   `Arc`s to `None`; the record stays as a body-free summary).
//! - Snapshots (`snapshot_summaries`) are BODY-FREE (`SnapshotFlowSummary`) — body
//!   retention on historical snapshots recreates a 135 GiB worst case (D5 owns the
//!   ring; D1 just exposes the body-free summary projection).

use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio_util::sync::CancellationToken;
use utoipa::ToSchema;

/// Record cap: reuse the monitor's `REQUEST_EVENT_LIMIT` (512) so the dashboard
/// store does not invent a second retention dial (D1 constraint).
const FLOW_CAP: usize = crate::monitor::REQUEST_EVENT_LIMIT;
/// Per-record TTL: reuse the monitor's 30-minute `DEBUG_HISTORY_RETENTION_MS`.
const FLOW_TTL_MS: u128 = crate::monitor::DEBUG_HISTORY_RETENTION_MS;
/// Default total live summary-byte quota (bodies + retained scalar strings). When
/// exceeded, the OLDEST bodies are evicted first (body `Arc` → `None`) until the
/// store is back under quota; the record survives as a body-free summary.
const DEFAULT_SUMMARY_QUOTA_BYTES: usize = 64 * 1024 * 1024;

/// Hard cap on a single captured body (inbound/normalized/upstream). The streaming
/// serializer stops writing once it has emitted this many bytes, so peak retained
/// body memory is O(CAP) regardless of the configured inbound request-body cap (default 10 MiB).
const BODY_CAP: usize = 128 * 1024;
/// Per-retained-scalar-string cap: bounds every dynamic string the record keeps
/// (body scalars, headers, ids, method, uri, models, target, terminal reason).
const SCALAR_CAP: usize = 4 * 1024;

/// Gap 05 — env flag that opts INTO upstream RESPONSE/ERROR-body capture. SEPARATE
/// from the debug-UI gate (which arms request-body capture): even with the dashboard
/// on, the upstream response/error body is captured ONLY when this is an explicit
/// affirmative. OFF by default — a diagnostic operator turns it on to answer "what did
/// the upstream actually say back when this turn failed?". Read env-only (never a
/// persisted `Config` field, mirroring the dashboard auth posture).
const ENV_CAPTURE_UPSTREAM_RESPONSE: &str = "LLMCONDUIT_DASHBOARD_CAPTURE_UPSTREAM_RESPONSE";

/// A boolean env flag is true only for the explicit affirmative values `1`/`true`/
/// `yes` (case-insensitive). Anything else — including unset — is false, so upstream
/// response capture stays OFF unless explicitly opted in. Mirrors
/// [`crate::dashboard_auth`]'s `env_flag` (kept local so this module owns its own gate
/// rather than widening that one's visibility).
fn capture_response_env_flag() -> bool {
    matches!(
        std::env::var(ENV_CAPTURE_UPSTREAM_RESPONSE)
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

/// CAS claim state: the flow is open and unclaimed by a telemetry writer (D3 L0).
pub const CLAIM_OPEN_L0: u8 = 0;
/// CAS claim state: a telemetry writer holds the claim (D3 L1).
pub const CLAIM_CLAIMED_L1: u8 = 1;
/// CAS claim state: the flow is finalized; no further telemetry writes (D3).
pub const CLAIM_FINALIZED: u8 = 2;

/// D6 **AbortHub**: the live-cancellation registry keyed by `api_call_id` (= the
/// flow `:id`), so `POST /dashboard/api/flows/:id/kill` can cancel a stuck server-side
/// stream WITHOUT rekeying (the `:id` IS the `api_call_id`; spec D6 decision).
///
/// Lifecycle is owned by the D3 L1 [`TelemetryGuard`], NOT the kill route: the guard
/// REGISTERS its [`CancellationToken`] under `api_call_id` the instant it CASes
/// `OpenL0 → ClaimedL1` (the SAME claim seam that makes it the flow's sole telemetry
/// writer), and REMOVES it on EVERY finalize path (the CAS-winning explicit
/// `Completed`/`Failed` AND the `Drop` fallback). So the map is bounded by the
/// in-flight stream count — never the 512-record history — and a finished flow leaks
/// no entry. Cancellation composes with (never replaces) the engine's existing
/// `tx.closed()` client-hangup checks: a kill flips the token, every cancel site
/// surfaces `AppError::cancelled()` (HTTP 499) exactly like a hang-up, with no token
/// duplication (AGENTS.md "Failover only pre-first-chunk": a mid-stream cancel is a
/// cancel, not a retry).
///
/// `Clone` shares the inner `Arc<Mutex<_>>` (like `DashboardFlowStore`/`MonitorHub`)
/// so it threads into the `#[derive(Clone)] Gateway`. `disabled()` mirrors the
/// zero-overhead pattern: when the debug UI is off the guard never registers and
/// [`abort`](Self::abort) is a no-op `false`, so production keeps no map + no lock.
#[derive(Clone, Debug)]
pub struct AbortHub {
    enabled: bool,
    handles: Arc<Mutex<HashMap<String, CancellationToken>>>,
}

impl AbortHub {
    /// Enabled hub (debug UI on).
    pub fn new() -> Self {
        Self {
            enabled: true,
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// No-op hub (debug UI off). `register`/`remove` early-return and `abort` is
    /// always `false`, so production keeps zero overhead.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, CancellationToken>> {
        self.handles.lock().expect("abort hub lock poisoned")
    }

    /// Register the flow's cancellation token under `api_call_id`. Called ONLY by the
    /// L1 guard at its claim seam, so at most one token per in-flight flow. No-op when
    /// disabled.
    fn register(&self, api_call_id: &str, token: CancellationToken) {
        if !self.enabled {
            return;
        }
        self.lock().insert(api_call_id.to_string(), token);
    }

    /// Remove the flow's token. Called by the L1 guard on EVERY finalize path (so the
    /// map never retains a finished flow). Idempotent — a second remove (Drop after an
    /// explicit finalize) is a no-op. No-op when disabled.
    fn remove(&self, api_call_id: &str) {
        if !self.enabled {
            return;
        }
        self.lock().remove(api_call_id);
    }

    /// Cancel the live stream for `api_call_id`, returning whether a live token was
    /// found (the kill route maps `true → 200`, `false → 404` for an unknown or
    /// already-finished flow). Cancelling flips the shared [`CancellationToken`]; the
    /// engine's compose-with-`tx.closed()` sites then surface `AppError::cancelled()`
    /// (499) and the guard's `Drop` finalizes `Cancelled`, which also removes the
    /// entry. We do NOT remove here: removal stays the guard's single responsibility,
    /// so a double-kill is a harmless idempotent `cancel()` (already-cancelled tokens
    /// stay cancelled) rather than a removal race with the finalizing guard.
    pub fn abort(&self, api_call_id: &str) -> bool {
        if !self.enabled {
            return false;
        }
        let Some(token) = self.lock().get(api_call_id).cloned() else {
            return false;
        };
        token.cancel();
        true
    }

    /// Number of live registered tokens. The no-leak invariant: this returns to 0 once
    /// all in-flight flows finalize (the guard removes on every terminal path), so the
    /// map is bounded by the in-flight stream count, never the 512-record history. `0`
    /// when disabled. Primarily a test/observability seam.
    pub fn live_len(&self) -> usize {
        if !self.enabled {
            return 0;
        }
        self.lock().len()
    }
}

impl Default for AbortHub {
    fn default() -> Self {
        Self::new()
    }
}

/// Lifecycle status of a flow. `Open` at creation; D3 moves it to a terminal
/// state. Serializes snake_case for the dashboard REST/WS surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlowStatus {
    /// Not yet terminal (the request is still in flight / streaming).
    Open,
    /// Finished successfully.
    Completed,
    /// Ended in an error.
    Failed,
    /// Aborted before completion.
    Cancelled,
}

/// Token usage attached to a flow once the upstream response reports it.
///
/// Gap 07 — usage CONFIDENCE. `prompt`/`completion`/`total` are the core counts the
/// upstream always reports. `cached`/`reasoning` are OPTIONAL token classes the
/// upstream may or may not break out: a `0` and "the upstream never reported this
/// class" are DIFFERENT facts. They are therefore `Option<i64>` — `Some(0)` is a
/// provider-reported zero (e.g. "0 cache hits this turn"), `None` is UNAVAILABLE
/// (the upstream omitted `prompt_tokens_details`/`completion_tokens_details`). Serialized
/// with `skip_serializing_if` so an unreported class is ABSENT on the wire (the
/// frontend renders `—`), never a fabricated `0` (don't-lie-with-zeros). The
/// distinction is load-bearing for cost confidence: a `cached` charge against a
/// model with no configured cache rate (or an unreported `cached`) is `estimated`,
/// not `confident`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, ToSchema)]
pub struct FlowUsage {
    /// Prompt (input) tokens.
    pub prompt: i64,
    /// Completion (output) tokens.
    pub completion: i64,
    /// Total tokens as reported by the upstream.
    pub total: i64,
    /// Cache-read prompt tokens. `Some(n)` measured (incl. a reported `0`); `None`
    /// when the upstream did not report a cached breakdown (UNAVAILABLE, not `0`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached: Option<i64>,
    /// Reasoning (thinking) tokens. `Some(n)` measured (incl. a reported `0`); `None`
    /// when the upstream did not report reasoning details (UNAVAILABLE, not `0`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<i64>,
}

/// Gap 04 — the PROVENANCE of a flow's `client_label`: WHICH non-secret signal the
/// attribution was derived from. Tagged so the dashboard (spec 15) can render the
/// weaker User-Agent fallback DIFFERENTLY from the stronger key-hash / configured-id
/// attribution — a `user_agent`-sourced label is NOT an identity claim. Serializes
/// snake_case; `Deserialize` so the body-free [`SnapshotFlowSummary`] round-trips on
/// the WS/snapshot wire (AGENTS.md: no new wire field without a round-trip test).
///
/// Priority order (strongest → weakest), honored by [`ClientAttribution::derive`]:
/// `KeyHash` → `ConfiguredHeader` → `UserAgent`. There is NO proxy auth-principal
/// source today (the proxy forwards keys, it does not authenticate a principal), so
/// one is deliberately absent until such a seam exists (spec 04 / Codex review).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ClientSource {
    /// Derived from a non-reversible SHA-256 digest of the inbound API key (the raw
    /// key is NEVER stored/emitted; only a short hex prefix of its hash becomes the
    /// label). The strongest attribution seam: the same caller key groups stably.
    KeyHash,
    /// Derived from an operator-configured NON-SECRET request header (e.g.
    /// `x-client-id`) — an explicit caller-supplied identity. Stronger than UA, but
    /// caller-asserted (unverified), so weaker than the key-hash.
    ConfiguredHeader,
    /// Derived from the `User-Agent` header. A WEAK, labelled fallback only — NOT an
    /// identity model (any caller can spoof it). Present so a flow with no key and no
    /// configured id still shows SOMETHING, clearly tagged as the weakest source.
    UserAgent,
}

/// Gap 04 — a flow's request-scoped client attribution: a stable, non-secret
/// `label` PLUS the [`ClientSource`] it was derived from. Both `None` when the
/// request carries no key, no configured-id header, and no User-Agent — an absent
/// identity is `None` (renders `—` downstream), NEVER a fabricated id or empty
/// string (don't-lie-with-zeros). Derived ONCE in the middleware at the
/// PRE-redaction seam ([`ClientAttribution::derive`]) — the only point the raw key
/// is still present — and threaded into [`DashboardFlowStore::open`]. The raw key is
/// hashed in-place and dropped; it is never stored on this struct, the
/// [`FlowRecord`], the persisted `Config`, or any log/WS surface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientAttribution {
    pub label: Option<String>,
    pub source: Option<ClientSource>,
}

/// Number of leading hex chars of the key-hash kept as the display id. 12 hex chars
/// = 48 bits: enough to make accidental collisions between distinct caller keys
/// vanishingly unlikely for a dashboard's working set, while keeping the label short
/// and revealing NOTHING about the key (a one-way digest prefix is not invertible).
const KEY_HASH_DISPLAY_HEX_LEN: usize = 12;

impl ClientAttribution {
    /// The empty attribution (no key, no configured id, no UA). Both fields `None`.
    /// The constructor every non-deriving caller (tests, the disabled path) uses so
    /// a flow with no client signal stays honestly unattributed.
    pub fn none() -> Self {
        Self {
            label: None,
            source: None,
        }
    }

    /// Derive the attribution from the RAW (pre-redaction) request headers, honoring
    /// the priority order `KeyHash → ConfiguredHeader → UserAgent → None`. MUST be
    /// called BEFORE the headers are redacted — it is the only point the raw API key
    /// is still readable, and it hashes that key in-place (never retaining it).
    ///
    /// - **Key-hash** (strongest): the FIRST non-blank inbound credential — the
    ///   `Authorization` bearer token (the part after a case-insensitive `Bearer `
    ///   prefix, requiring a NON-EMPTY token), then the `x-api-key` value, then a
    ///   configured header that itself NAMES a sensitive key carrier (per
    ///   [`crate::redaction::is_sensitive_payload_key`]) — is run through
    ///   [`key_hash_label`] → a `key-<12 hex>` display id; the raw key is dropped
    ///   immediately. Each candidate is trimmed and a blank/whitespace-only one is
    ///   SKIPPED (so an empty/`Bearer`-only `Authorization` falls through to
    ///   `x-api-key` rather than fabricating a hash from the literal). A configured
    ///   header that names the `Authorization` carrier is read through the SAME
    ///   `bearer_token` scheme normalization as the canonical bearer candidate (NOT the
    ///   raw header), so a token-less `Bearer`/`Bearer   ` configured `authorization`
    ///   ALSO yields no candidate and falls through — never the scheme literal
    ///   `"Bearer"` hashed (review round 2). The raw key value is NEVER emitted verbatim
    ///   — a key carrier is ALWAYS hashed.
    /// - **Configured header** (caller-asserted id): when `configured_header` names a
    ///   header that is present, non-empty, AND NOT a sensitive key carrier, its value
    ///   becomes the label (verbatim, capped later). A configured header whose NAME is
    ///   sensitive is NEVER emitted verbatim: it is only ever a key-hash source above,
    ///   and if it carried no usable key it is SUPPRESSED here (security: F1) — the
    ///   derivation falls through to the User-Agent fallback instead of leaking the
    ///   raw value of e.g. `LLMCONDUIT_DASHBOARD_CLIENT_HEADER=api-key`.
    /// - **User-Agent** (weakest): a present, non-empty `User-Agent` becomes the (weak)
    ///   label.
    /// - Otherwise [`none`](Self::none).
    pub fn derive(headers: &axum::http::HeaderMap, configured_header: Option<&str>) -> Self {
        // The operator-configured caller-id header name (trimmed; blank ⇒ absent) and
        // whether it names a sensitive KEY carrier. A sensitive name is treated ONLY as
        // a key-hash source (hashed, never verbatim) — never as a verbatim configured
        // label, so it can never leak its raw value (F1).
        let configured = configured_header
            .map(str::trim)
            .filter(|name| !name.is_empty());
        let configured_is_sensitive =
            configured.is_some_and(crate::redaction::is_sensitive_payload_key);

        // 1) Strongest: a one-way hash of the FIRST non-blank inbound credential. Each
        //    candidate is normalized/trimmed BEFORE the fallback decision, so an
        //    empty/`Bearer`-only `Authorization` (a blank candidate) is SKIPPED and the
        //    next carrier is tried (F2) instead of fabricating a hash from the literal.
        //    A configured header that NAMES a key carrier joins this list so its value
        //    is hashed, never emitted verbatim (F1). The raw key is read, hashed, and
        //    dropped HERE — never stored or returned.
        let key_candidates = [
            bearer_token(headers),
            api_key_header(headers),
            // Only when the configured header itself names a sensitive key carrier do we
            // read its value AS A KEY (to be hashed). A non-sensitive configured header
            // is handled by the verbatim branch below, not here.
            //
            // A configured `Authorization` header MUST be read through the SAME
            // `bearer_token` normalization as the canonical candidate above — NOT the raw
            // `header_value` — so a token-less `Bearer`/`Bearer   ` yields an empty token
            // (skipped), never the scheme literal `"Bearer"` hashed into a fabricated
            // label (review round 2). For any other sensitive alias (`api-key`,
            // `bearer-token`, …) there is no scheme word to strip and the canonical
            // candidates do not read it, so its raw value is the key to hash.
            configured
                .filter(|_| configured_is_sensitive)
                .and_then(|name| {
                    if name.eq_ignore_ascii_case(axum::http::header::AUTHORIZATION.as_str()) {
                        bearer_token(headers)
                    } else {
                        header_value(headers, name)
                    }
                }),
        ];
        for candidate in key_candidates.into_iter().flatten() {
            let token = candidate.trim();
            if !token.is_empty() {
                return Self {
                    label: Some(key_hash_label(token)),
                    source: Some(ClientSource::KeyHash),
                };
            }
        }
        // 2) An operator-configured NON-SECRET caller-id header (e.g. `x-client-id`).
        //    A configured header whose NAME is sensitive is NEVER taken verbatim here —
        //    it was a key-hash source above; if it carried no usable key it is dropped
        //    (suppressed), NOT leaked as a verbatim label (F1).
        if !configured_is_sensitive
            && let Some(name) = configured
            && let Some(value) = header_value(headers, name)
            && !value.trim().is_empty()
        {
            return Self {
                label: Some(value.trim().to_string()),
                source: Some(ClientSource::ConfiguredHeader),
            };
        }
        // 3) Weakest: the User-Agent fallback (clearly tagged, NOT an identity claim).
        if let Some(ua) = header_value(headers, axum::http::header::USER_AGENT.as_str())
            && !ua.trim().is_empty()
        {
            return Self {
                label: Some(ua.trim().to_string()),
                source: Some(ClientSource::UserAgent),
            };
        }
        Self::none()
    }
}

/// Read a header value as a `&str`, or `None` when absent / non-UTF-8.
fn header_value<'a>(headers: &'a axum::http::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// The bearer token from `Authorization`, i.e. the part after a case-insensitive
/// `Bearer` scheme word (followed by whitespace OR end-of-string). When the header has
/// no `Bearer` scheme the whole value is returned (still a credential to hash). `None`
/// when the header is absent/non-UTF-8. The returned slice MAY be empty/whitespace (an
/// `Authorization: Bearer`-only or blank header — with OR without a trailing space);
/// the caller ([`ClientAttribution::derive`]) trims each candidate and SKIPS a blank one
/// BEFORE the fallback, so an empty bearer never suppresses a valid `x-api-key` nor
/// fabricates a hash from the literal scheme word `"Bearer"` (F2).
fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    let value = header_value(headers, axum::http::header::AUTHORIZATION.as_str())?;
    let trimmed = value.trim_start();
    // Strip a leading case-insensitive `Bearer` scheme word ONLY when it is the whole
    // value or is followed by whitespace — so `Bearer`/`Bearer ` (token-less) yields an
    // empty token (skipped by the caller), not the literal word treated as a credential.
    if let Some(rest) = trimmed
        .get(..6)
        .filter(|p| p.eq_ignore_ascii_case("Bearer"))
    {
        let after = &trimmed[rest.len()..];
        if after.is_empty() || after.starts_with(char::is_whitespace) {
            return Some(after.trim_start());
        }
    }
    Some(value)
}

/// The raw `x-api-key` header value (the Anthropic-style key carrier). `None` when
/// absent/non-UTF-8.
fn api_key_header(headers: &axum::http::HeaderMap) -> Option<&str> {
    header_value(headers, "x-api-key")
}

/// Gap 04 — turn a raw API key into a STABLE, NON-REVERSIBLE display id:
/// `key-<first 12 hex chars of SHA-256(key)>`. SHA-256 is one-way, so the label
/// reveals nothing about the key; the SAME key always yields the SAME label (stable
/// grouping) and a DIFFERENT key a different label (collision-resistant). The raw
/// key is consumed only to feed the digest and is never stored or logged. This is
/// the SINGLE definition of the key→label mapping so the seam has one audited form.
fn key_hash_label(raw_key: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(raw_key.as_bytes());
    let hex = hex::encode(digest);
    format!("key-{}", &hex[..KEY_HASH_DISPLAY_HEX_LEN])
}

/// Gap 03 — the outcome of one upstream dispatch attempt. Snake_case on the wire so
/// the body-free [`SnapshotFlowSummary`] carries it to the failover/attempt-trace UI
/// (spec 11) and the per-provider metrics aggregation (spec 12).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    /// This attempt produced the first chunk on the wire — it is the SERVING attempt.
    Served,
    /// This attempt failed before producing a chunk (connect error, non-2xx, timeout,
    /// or a parse/stream error before the first chunk). Failover then tried the next.
    Failed,
}

/// Gap 03 — a BOUNDED, sanitized, taxonomic failure code for a failed attempt. This is
/// NOT raw upstream error text (which stays behind spec 05's separately-gated seam): it
/// is a fixed enum so the body-free summary can never become a backdoor for an
/// unbounded/secret-bearing upstream error body. `error_class` is `None` on the served
/// attempt (don't-lie-with-zeros for the success case). Serializes snake_case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttemptErrorClass {
    /// The upstream connection or request transport failed before any response.
    Connect,
    /// The upstream returned a non-2xx HTTP status (a provider-failure-shaped 4xx/5xx).
    HttpStatus,
    /// The attempt timed out waiting for the response / first chunk.
    Timeout,
    /// The response began but the first chunk could not be read/parsed (stream error
    /// before the first chunk, or the stream ended before any chunk).
    Stream,
    /// A same-provider terminal failure surfaced as-is (e.g. a context-window overflow
    /// that persisted after the leaf's shrink-and-retry — failover does NOT retry it on
    /// another provider; AGENTS.md). Carried for provenance even though it ends the flow.
    Terminal,
    /// Any other failover-eligible upstream error not covered above.
    Other,
}

/// Gap 03 — a BOUNDED, sanitized, taxonomic reason a failed attempt triggered failover
/// to the next provider. Like [`AttemptErrorClass`], this is a fixed enum — never raw
/// upstream text — so it is safe on the body-free summary. `None` on the served attempt
/// (it did not fail over). Serializes snake_case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttemptFailoverReason {
    /// The provider failed before the first chunk and failover moved to the next.
    ProviderFailed,
    /// The provider rejected this particular request; failover advanced without
    /// treating the provider as unhealthy or starting its cooldown.
    RequestRejected,
    /// The provider's failure was same-provider terminal — failover did NOT advance
    /// (recorded so the trace shows WHY no further provider was tried).
    TerminalNoFailover,
}

/// Gap 03 — one upstream dispatch attempt's full provenance: WHICH provider, WHAT model,
/// HOW LONG it took, WHEN the first wire byte arrived, and the OUTCOME. The failover loop
/// records one per provider it tries (failed ones + the served one); a non-failover
/// (bare-leaf / single-upstream / routing-with-no-fallback) flow records exactly one.
///
/// All timestamps are MEASURED epoch-ms (`now_ms`). `first_upstream_byte_ms` is the wire
/// instant the attempt's FIRST chunk arrived — `None` when the attempt never received
/// response headers / a first chunk (a connect/timeout/non-2xx failure), so an unmeasured
/// first-byte time is `None`, NEVER `0` (don't-lie-with-zeros). `error_class` /
/// `failover_reason` are `None` on the served attempt and are BOUNDED taxonomic codes
/// (never raw upstream text) on a failed attempt — they ride the body-free summary, so
/// raw error bodies stay behind spec 05's gated seam. Snake_case + `skip_serializing_if`
/// so a `None` field is absent on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize, ToSchema)]
pub struct Attempt {
    /// The provider name this attempt dispatched to (the failover provider's name, the
    /// routing route's name, or the synthetic `"primary"` for a bare single upstream).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// The model actually sent on the wire for this attempt (post provider-remap), when
    /// known. `None` when the attempt failed before the on-wire model was finalized.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Epoch-ms the attempt began (the dispatch was issued). Always measured.
    pub start_ms: u128,
    /// Epoch-ms the attempt resolved (served first chunk, or failed). Always measured.
    pub end_ms: u128,
    /// Epoch-ms the FIRST chunk arrived on the wire for this attempt. `None` when the
    /// attempt never received a first chunk (failed before response headers) — NEVER `0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_upstream_byte_ms: Option<u128>,
    /// Served vs failed.
    pub status: AttemptStatus,
    /// Bounded taxonomic failure code; `None` on the served attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_class: Option<AttemptErrorClass>,
    /// Bounded taxonomic failover reason; `None` on the served attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failover_reason: Option<AttemptFailoverReason>,
}

impl Attempt {
    /// Bytes this attempt contributes to the live summary-byte quota: only the two
    /// dynamic scalar strings it retains (the bounded enums are fixed-size). Counted so
    /// a flood of long provider/model strings on a long failover chain cannot blow the
    /// quota silently (mirrors [`FlowRecord::summary_bytes`]).
    fn summary_bytes(&self) -> usize {
        let opt = |s: &Option<String>| s.as_ref().map(|s| s.len()).unwrap_or(0);
        opt(&self.provider) + opt(&self.model)
    }

    /// Gap 03 (round-1 review F3): cap this attempt's two dynamic scalar strings
    /// (`provider`, `model`) to [`SCALAR_CAP`] BEFORE the attempt is retained on the shared
    /// [`ServingToken`](crate::upstream::ServingToken). The attempt is later cloned UNCHANGED
    /// into both [`FlowRecord::attempts`] and the evict-safe
    /// [`TerminalMetricsInputs::attempts`], so without this cap an attacker-controlled
    /// provider/model string (e.g. a routed alias derived from request input) would bypass
    /// the store's `SCALAR_CAP` invariant on BOTH the record and the terminal payload (the
    /// `record_attempts` upsert replaces the vector wholesale and does not re-cap). Reuses
    /// the SAME [`cap_scalar`] helper every other retained scalar uses — no second cap. The
    /// bounded enums need no capping (fixed size); the timestamps are `u128` scalars.
    pub(crate) fn capped(mut self) -> Self {
        self.provider = self.provider.map(cap_scalar);
        self.model = self.model.map(cap_scalar);
        self
    }
}

/// The D5 metrics inputs the L1 [`TelemetryGuard`] assembles at finalize from its
/// OWN evict-safe sources (D5 R3 MEDIUM): the `endpoint` captured at claim (when the
/// record provably exists) + the shared `ServingToken` (which carries the resolved
/// `model_served`, the serving route/provider, and the final cumulative usage). The
/// engine's `record_terminal` records from THIS copy, NEVER by re-reading the record
/// via `detail()` — the record can be pruned (TTL) or evicted (cap) BEFORE finalize,
/// so a re-read would `None`-out and UNDERCOUNT completed requests. Sourcing from the
/// guard makes the authoritative metrics layer independent of FlowStore retention.
/// `endpoint` is the inbound route; `upstream` is the serving provider/route label;
/// all are owned values (not borrows) so the snapshot survives any eviction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TerminalMetricsInputs {
    pub model_served: Option<String>,
    pub endpoint: String,
    pub upstream: Option<String>,
    pub usage: Option<FlowUsage>,
    /// Measured first-upstream-byte → first-content duration.
    pub prefill_ms: Option<u128>,
    /// Measured first-content → clean-stream-end duration.
    pub decode_ms: Option<u128>,
    /// Gap 03 — the per-attempt trace, read off the shared `ServingToken` at finalize
    /// alongside `usage`. Carried on the evict-safe terminal payload (NOT only the
    /// FlowStore record) so spec 12 can aggregate per-provider metrics without
    /// re-reading the evictable record. Empty when the flow recorded no attempt.
    pub attempts: Vec<Attempt>,
}

/// Request-extension newtype carrying the `api_call_id` minted by `log_api_call`
/// from the HTTP boundary down to the inference handlers + engine, so the engine
/// can `link(response_id, api_call_id)` without re-deriving the id.
#[derive(Debug, Clone)]
pub struct ApiCallId(pub String);

/// A body that has ALREADY been through the capped + redacting capture primitive.
/// The ONLY way to mint one is [`capture_body`] (D1 R2 #2), so a caller (D2/D3)
/// cannot hand the store an unredacted/over-cap `Arc<[u8]>` — the type makes the
/// bypass impossible. Holds an `Arc<[u8]>` ≤ `BODY_CAP` with secrets redacted.
#[derive(Debug, Clone)]
pub struct CapturedBody(Arc<[u8]>);

impl CapturedBody {
    /// The redacted, capped bytes (cheap `Arc` clone).
    fn into_arc(self) -> Arc<[u8]> {
        self.0
    }

    /// Test-only view of the redacted bytes (the field is private so production
    /// callers cannot read raw body bytes back out of the newtype).
    #[cfg(test)]
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Headers that have ALREADY been through the capping + redacting header primitive.
/// The ONLY way to mint one is [`redact_headers`] (D1 R2 #2), so the store can never
/// be handed raw, secret-bearing header pairs.
#[derive(Debug, Clone)]
pub struct CapturedHeaders(Vec<(String, String)>);

/// Gap 05 — an upstream RESPONSE/ERROR body that has ALREADY been through the capped +
/// redacting capture primitive, paired with whether the cap TRUNCATED it. The ONLY way
/// to mint one is [`capture_response_body`] (or its transport-truncation-aware
/// sibling [`capture_response_body_with_truncation`]), so (like [`CapturedBody`])
/// a caller cannot hand the store an unredacted / over-cap / slice-retaining body. Holds an
/// `Arc<[u8]>` ≤ `BODY_CAP` with secrets redacted; `truncated` records whether the raw
/// body exceeded `BODY_CAP` (so the dashboard can flag a partial body honestly rather
/// than presenting it as complete — don't-lie-with-zeros for bodies).
#[derive(Debug, Clone)]
pub struct CapturedResponseBody {
    body: Arc<[u8]>,
    truncated: bool,
}

impl CapturedResponseBody {
    /// Move the inner capture into the record-facing [`UpstreamResponseBody`] (cheap
    /// `Arc` clone of the redacted bytes + the truncation flag).
    fn into_record(self) -> UpstreamResponseBody {
        UpstreamResponseBody {
            bytes: self.body,
            truncated: self.truncated,
        }
    }
}

/// Gap 05 — the upstream RESPONSE/ERROR body as retained on a live [`FlowRecord`]: the
/// redacted, capped bytes plus the truncation flag. `Some(_)` means a body WAS captured
/// (distinct from `None` = capture disabled or no body); an EMPTY `bytes` distinguishes
/// "captured an empty body" from both. NOT placed on [`SnapshotFlowSummary`] — bodies
/// live only on the live record (the 135 GiB worst-case, body-free-snapshot invariant).
#[derive(Debug, Clone)]
pub struct UpstreamResponseBody {
    /// Redacted, capped response/error bytes (≤ `BODY_CAP`). May be EMPTY — that is a
    /// genuinely captured empty body, NOT an absent one (the absent case is the outer
    /// `Option` being `None`).
    pub bytes: Arc<[u8]>,
    /// Whether the cap truncated the raw body (raw length exceeded `BODY_CAP`). When
    /// `true`, `bytes` is a PREFIX, not the whole body — the dashboard must flag it.
    pub truncated: bool,
}

impl UpstreamResponseBody {
    /// Bytes this capture contributes to the live summary-byte quota (the retained
    /// `Arc<[u8]>` length; the `bool` is fixed-size and not counted).
    fn len(&self) -> usize {
        self.bytes.len()
    }
}

/// The authoritative live record for one inference flow. NOT `Serialize`: it holds
/// an `Arc<AtomicU8>` (the D3 claim) and an `Instant`, and carries the raw capped
/// body `Arc<[u8]>`s; the dashboard surface serializes the body-free
/// [`SnapshotFlowSummary`] projection instead. Cloned copy-on-write on every
/// mutation; the `claim` `Arc` is shared across copies so the atomic identity is
/// stable for D3.
#[derive(Debug, Clone)]
pub struct FlowRecord {
    /// D3 telemetry-guard CAS cell (`CLAIM_OPEN_L0` at creation). Shared across COW
    /// copies — D1 only allocates it; D3 owns the transitions.
    pub claim: Arc<AtomicU8>,
    /// The value of the FlowStore's monotonic mutation counter AT THIS RECORD'S LAST
    /// MUTATION (D7b R2 finding 1). Drawn from the SAME global counter as
    /// [`DashboardFlowState::seq`] (so it stays directly comparable to the snapshot's
    /// `flow_seq` dedup baseline), but FROZEN at the record's own last mutation instead
    /// of re-read at send time. [`DashboardFlowStore::detail_with_seq`] returns THIS, so
    /// a delayed/replayed flow update is stamped with the record's own watermark — it
    /// can no longer leapfrog past unrelated flows that mutated in the gap and so cannot
    /// dedup-drop a genuinely newer flow frame on the client. `insert`/`update` set it to
    /// the post-bump `seq`; this field is NOT counted in `summary_bytes` (a fixed-size
    /// `u64`, not a heap scalar).
    pub record_seq: u64,
    pub api_call_id: String,
    pub response_id: Option<String>,
    pub method: String,
    pub uri: String,
    /// Header name/value pairs with sensitive VALUES already redacted.
    pub headers: Vec<(String, String)>,
    /// Capped + redacted inbound request body. `None` once evicted by the
    /// summary-byte quota (the record then survives as a body-free summary).
    pub inbound_body: Option<Arc<[u8]>>,
    /// Whether this key opted into retaining body payloads. Metadata mutations
    /// continue regardless of this flag.
    pub capture_payloads: bool,
    /// Capped + redacted canonical/normalized body (set by D2).
    pub normalized: Option<Arc<[u8]>>,
    /// Capped + redacted upstream chat body (set by D2).
    pub upstream_body: Option<Arc<[u8]>>,
    /// Gap 05 — the capped + redacted upstream RESPONSE/ERROR body (set by
    /// [`set_upstream_response`](DashboardFlowStore::set_upstream_response) at the leaf
    /// when a turn fails with a non-2xx). OPTIONAL and OFF by default: populated ONLY
    /// when the SEPARATE [`ENV_CAPTURE_UPSTREAM_RESPONSE`] gate is on (distinct from the
    /// debug-UI gate that arms request capture) AND the upstream actually returned an
    /// error body. Tri-state, don't-lie-with-zeros: `None` ⇒ capture disabled OR no
    /// body captured; `Some(b)` with `b.bytes` empty ⇒ a genuinely captured EMPTY body;
    /// `Some(b)` with `b.truncated` ⇒ a partial (cap-truncated) body the dashboard must
    /// flag. Copied through the capped/redacting serializer (NEVER a `Bytes` slice of
    /// the inbound middleware buffer bounded by `max_request_body_bytes`); evicted with the other bodies under the summary
    /// quota and NEVER projected onto the body-free [`SnapshotFlowSummary`]. Consumed by
    /// gap 14 (failure taxonomy); the React app ignores it until then.
    pub upstream_response: Option<UpstreamResponseBody>,
    pub model_requested: Option<String>,
    pub model_served: Option<String>,
    pub upstream_target: Option<String>,
    pub usage: Option<FlowUsage>,
    pub status: FlowStatus,
    pub started_at: Instant,
    pub started_ms: u128,
    pub finished_ms: Option<u128>,
    pub elapsed_ms: Option<u128>,
    pub terminal_reason: Option<String>,
    /// Gap 02 — per-phase wall-clock timestamps (epoch ms), the spine the later
    /// waterfall UI (specs 10/16) consumes. Each is a MEASURED phase: `None` until
    /// the engine reaches that seam, serialized as `null`/absent so a phase that did
    /// NOT happen (errored before content, never lowered) renders `—` downstream —
    /// NEVER `0` (a genuine measured `0ms` cannot occur for a wall-clock epoch stamp,
    /// so `Some(_)` unambiguously means "this phase ran"). All are FIRST-WRITE-WINS
    /// via [`PhaseTimings::stamp`]: the outer replay/tool `loop` in `run_turn` and the
    /// per-delta `OutputTextDelta` arm fire their seams repeatedly, but only the FIRST
    /// observation stamps — so `first_content_delta_ms` marks the first content token
    /// the client saw, not a later one, and a multi-turn flow does not re-stamp. The
    /// stamp also CLAMPS each value up to the latest already-recorded phase, so the
    /// fields are monotonic (`ingress ≤ normalization ≤ routing ≤ first_content_delta
    /// ≤ stream_end ≤ finalize`) even if the wall clock steps backwards between seams.
    pub phases: PhaseTimings,
    /// Gap 03 — the per-attempt failover trace: one [`Attempt`] per upstream dispatch the
    /// failover loop tried (failed providers + the served one), or exactly one for a
    /// non-failover (bare-leaf / single-upstream / routing-no-fallback) flow. Threaded
    /// from the shared `ServingToken` into the record at finalize (the SAME evict-safe
    /// source that feeds the terminal metrics payload), so the record and the metrics
    /// path agree. Empty until finalize / when no attempt was recorded.
    pub attempts: Vec<Attempt>,
    /// Gap 03 — the flow-level wire time-to-first-byte: the epoch-ms the FIRST upstream
    /// chunk of the SERVING attempt arrived ON THE WIRE (distinct from gap 02's
    /// client-facing `first_content_delta_ms` TTFT). `None` when no upstream byte ever
    /// arrived (every attempt failed before response headers) — NEVER `0`
    /// (don't-lie-with-zeros); a missing measurement renders `—` downstream. Measured at
    /// the prefetch point (failover) / the bare-leaf first-chunk seam, first-write-wins.
    pub first_upstream_byte_ms: Option<u128>,
    /// Gap 04 — a STABLE, NON-SECRET client attribution label: a key-hash display id
    /// (`key-<hex>`), a configured caller-id header value, or a User-Agent fallback —
    /// per [`ClientAttribution::derive`]'s priority order. Derived ONCE at the
    /// PRE-redaction middleware seam (the only point the raw key is readable) and set
    /// at [`open`](DashboardFlowStore::open); the raw key is hashed in-place and NEVER
    /// stored here. `None` when the request carries no key, no configured id, and no
    /// UA — an absent identity is `None` (renders `—` downstream), NEVER a fabricated
    /// id (don't-lie-with-zeros). Capped to [`SCALAR_CAP`] + counted in
    /// [`summary_bytes`](FlowRecord::summary_bytes).
    pub client_label: Option<String>,
    /// Gap 04 — the [`ClientSource`] `client_label` was derived from, so the weaker
    /// `UserAgent` fallback is visibly distinguishable from a key-hash / configured-id
    /// attribution downstream. `None` exactly when `client_label` is `None`.
    pub client_source: Option<ClientSource>,
    /// Harness identity + session-tree linkage computed at the persistence seam
    /// (see `crate::harness` / `crate::sessions`). Body-free scalar metadata;
    /// every field absent when persistence is off.
    pub session: FlowSessionFacts,
}

/// The per-flow harness/session facts projected onto every live surface
/// (list row, snapshot summary, detail body, `flow_status` frame). Flattened
/// onto the wire as sibling scalar fields, each `skip_serializing_if = None`,
/// so an unknown fact is ABSENT, never a fabricated value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, serde::Deserialize, ToSchema)]
pub struct FlowSessionFacts {
    /// Detected harness profile name (`claude-code`, `codex`, `pi-agent`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    /// Detected harness version, when the harness reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness_version: Option<String>,
    /// Gateway session node id (`sessions.id`) the flow was linked into.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// The chain predecessor's `api_call_id`, when the flow extends a chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_parent_request_id: Option<String>,
    /// `append` | `instructions_changed` | `tools_changed` | `history_rewritten` | `new_chain`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub divergence_kind: Option<String>,
    /// True when the divergence falls inside the predecessor's items (a
    /// prefix-cache miss upstream). Absent when lineage was not computed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_bust: Option<bool>,
    /// Owner of the virtual key that authenticated the request, when known.
    /// Absent for open-mode requests and keys without an owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    /// The virtual key's stable database id (never the credential).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_key_id: Option<String>,
}

impl FlowSessionFacts {
    /// Project the seam's detection + link results. Scalars are capped like
    /// every other retained flow field.
    pub fn from_parts(
        identity: Option<&crate::harness::HarnessIdentity>,
        link: Option<&crate::sessions::SessionLink>,
    ) -> Self {
        Self {
            harness: identity.map(|identity| cap_scalar(identity.harness.clone())),
            harness_version: identity
                .and_then(|identity| identity.version.clone())
                .map(cap_scalar),
            session_id: link.map(|link| cap_scalar(link.session_id.clone())),
            chain_parent_request_id: link
                .and_then(|link| link.chain_parent_request_id.clone())
                .map(cap_scalar),
            divergence_kind: link.map(|link| link.lineage.kind.as_str().to_string()),
            cache_bust: link.map(|link| link.lineage.cache_bust),
            // Key attribution from the PRIMARY linked node's row (the newest
            // upsert state the linker produced for this request).
            user_id: link
                .and_then(|link| {
                    link.upserts
                        .iter()
                        .find(|row| row.id == link.session_id)
                        .and_then(|row| row.user_id.clone())
                })
                .map(cap_scalar),
            virtual_key_id: link
                .and_then(|link| {
                    link.upserts
                        .iter()
                        .find(|row| row.id == link.session_id)
                        .and_then(|row| row.virtual_key_id.clone())
                })
                .map(cap_scalar),
        }
    }

    fn summary_bytes(&self) -> usize {
        let opt = |s: &Option<String>| s.as_ref().map(|s| s.len()).unwrap_or(0);
        opt(&self.harness)
            + opt(&self.harness_version)
            + opt(&self.session_id)
            + opt(&self.chain_parent_request_id)
            + opt(&self.divergence_kind)
            + opt(&self.user_id)
            + opt(&self.virtual_key_id)
    }
}

/// Gap 02 — the per-phase timestamp bundle carried by every [`FlowRecord`] and
/// projected onto the body-free [`SnapshotFlowSummary`]. Flattened onto the wire
/// (`#[serde(flatten)]`) so the dashboard sees `ingress_ms`/`normalization_done_ms`/…
/// as sibling scalar fields on the flow object — scalar metadata only, no body
/// retention (AGENTS.md snapshots-are-body-free invariant holds). Every field is an
/// OPTIONAL measured epoch-ms timestamp; `None` ⇒ the phase did not occur ⇒
/// serialized absent (the `skip_serializing_if` below) so the don't-lie-with-zeros
/// rule holds: a missing phase is NEVER coerced to `0`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize, ToSchema)]
pub struct PhaseTimings {
    /// Request ingress — when the FlowStore first `open`ed the record (≈ `started_ms`).
    /// Always `Some` once a record exists; the explicit phase value the waterfall
    /// anchors the other phases against.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingress_ms: Option<u128>,
    /// Inbound→canonical normalization settled — stamped when the engine captures the
    /// normalized canonical body (`set_normalized`). `None` if the flow errored before
    /// normalization (an extractor/JSON rejection caught by the L0 guard).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalization_done_ms: Option<u128>,
    /// Upstream routing/lowering decision — stamped when the engine commits the actual
    /// on-wire upstream request (`set_upstream` at the leaf). `None` if the flow never
    /// reached the wire (pre-spawn lowering/budget failure, replay-only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_decision_ms: Option<u128>,
    /// True TTFT — the wall-clock instant the FIRST canonical **content** SSE delta was
    /// emitted to the client. NOT reasoning, tool-argument, refusal, or signature
    /// deltas: a stream that emits reasoning/tool deltas before content does NOT stamp
    /// this early (first-write-wins on the content arm only). `None` if the flow errored
    /// before any content delta.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_content_delta_ms: Option<u128>,
    /// Stream completion — stamped when `run_turn` finishes emitting the terminal
    /// `response.completed`/`response.incomplete`. `None` if the flow errored or was
    /// cancelled mid-stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_end_ms: Option<u128>,
    /// Terminal finalize — stamped when the flow reaches its terminal state
    /// (`finalize`), for EVERY terminal (completed, failed, cancelled). Always `Some`
    /// once the flow is terminal; the right edge of the waterfall.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalize_ms: Option<u128>,
}

impl PhaseTimings {
    /// First-write-wins + monotonic stamp of one phase. No-op if the field is already
    /// `Some` (so the per-delta content arm and the replay/tool `loop` stamp only the
    /// FIRST observation). When it does write, the value is CLAMPED up to the latest
    /// already-recorded phase so the bundle stays monotonic even across a backwards
    /// wall-clock step — the seams fire in causal order, so the floor is the max of the
    /// existing measured phases (`ingress ≤ … ≤ finalize`).
    fn stamp(field: &mut Option<u128>, latest_prior: Option<u128>, now: u128) {
        if field.is_some() {
            return;
        }
        *field = Some(match latest_prior {
            Some(floor) => now.max(floor),
            None => now,
        });
    }

    /// The latest (largest) measured phase so far — the monotonic floor for the next
    /// stamp. `None` only before any phase is recorded.
    fn latest(&self) -> Option<u128> {
        [
            self.ingress_ms,
            self.normalization_done_ms,
            self.routing_decision_ms,
            self.first_content_delta_ms,
            self.stream_end_ms,
            self.finalize_ms,
        ]
        .into_iter()
        .flatten()
        .max()
    }

    /// Stamp `ingress` (record open).
    fn stamp_ingress(&mut self, now: u128) {
        let floor = self.latest();
        Self::stamp(&mut self.ingress_ms, floor, now);
    }

    /// Stamp `normalization_done` (canonical body captured).
    fn stamp_normalization(&mut self, now: u128) {
        let floor = self.latest();
        Self::stamp(&mut self.normalization_done_ms, floor, now);
    }

    /// Stamp `routing_decision` (on-wire upstream committed).
    fn stamp_routing(&mut self, now: u128) {
        let floor = self.latest();
        Self::stamp(&mut self.routing_decision_ms, floor, now);
    }

    /// Stamp `first_content_delta` (first canonical CONTENT SSE delta to the client).
    fn stamp_first_content_delta(&mut self, now: u128) {
        let floor = self.latest();
        Self::stamp(&mut self.first_content_delta_ms, floor, now);
    }

    /// Stamp `stream_end` (terminal SSE emitted).
    fn stamp_stream_end(&mut self, now: u128) {
        let floor = self.latest();
        Self::stamp(&mut self.stream_end_ms, floor, now);
    }

    /// Stamp `finalize` (flow reached a terminal state).
    fn stamp_finalize(&mut self, now: u128) {
        let floor = self.latest();
        Self::stamp(&mut self.finalize_ms, floor, now);
    }
}

impl FlowRecord {
    /// Bytes this record contributes to the live summary-byte quota: the three
    /// captured bodies, the retained (already-capped) header strings, AND every
    /// dynamic scalar string the record holds (D1 R1 #5 — none of these were
    /// counted before, so a flood of long model/uri/target strings could blow the
    /// quota silently).
    fn summary_bytes(&self) -> usize {
        let body = |b: &Option<Arc<[u8]>>| b.as_ref().map(|b| b.len()).unwrap_or(0);
        let opt = |s: &Option<String>| s.as_ref().map(|s| s.len()).unwrap_or(0);
        let headers: usize = self
            .headers
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum();
        let attempts: usize = self.attempts.iter().map(Attempt::summary_bytes).sum();
        let response_body = self
            .upstream_response
            .as_ref()
            .map(UpstreamResponseBody::len)
            .unwrap_or(0);
        body(&self.inbound_body)
            + body(&self.normalized)
            + body(&self.upstream_body)
            + response_body
            + headers
            + self.api_call_id.len()
            + opt(&self.response_id)
            + self.method.len()
            + self.uri.len()
            + opt(&self.model_requested)
            + opt(&self.model_served)
            + opt(&self.upstream_target)
            + opt(&self.terminal_reason)
            + self.session.summary_bytes()
            + opt(&self.client_label)
            + attempts
    }

    /// Total bytes held by the captured-body `Arc`s only (the eviction target):
    /// the three request layers plus the gap-05 upstream RESPONSE/ERROR body.
    fn body_bytes(&self) -> usize {
        let body = |b: &Option<Arc<[u8]>>| b.as_ref().map(|b| b.len()).unwrap_or(0);
        let response_body = self
            .upstream_response
            .as_ref()
            .map(UpstreamResponseBody::len)
            .unwrap_or(0);
        body(&self.inbound_body)
            + body(&self.normalized)
            + body(&self.upstream_body)
            + response_body
    }

    /// Whether the record still retains any body (eviction candidate).
    fn has_body(&self) -> bool {
        self.inbound_body.is_some()
            || self.normalized.is_some()
            || self.upstream_body.is_some()
            || self.upstream_response.is_some()
    }
}

/// Body-free projection of a [`FlowRecord`] for the dashboard REST/WS surface and
/// the D5 snapshot ring. Carries every field EXCEPT the three body `Arc<[u8]>`s and
/// the non-serializable `claim`/`started_at` — body retention on snapshots is
/// forbidden (135 GiB worst case; AGENTS.md don't-rule).
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotFlowSummary {
    pub api_call_id: String,
    pub response_id: Option<String>,
    pub method: String,
    pub uri: String,
    pub model_requested: Option<String>,
    pub model_served: Option<String>,
    pub upstream_target: Option<String>,
    pub usage: Option<FlowUsage>,
    pub status: FlowStatus,
    pub started_ms: u128,
    pub finished_ms: Option<u128>,
    pub elapsed_ms: Option<u128>,
    pub terminal_reason: Option<String>,
    /// Gap 02 — the per-phase timestamps, flattened onto the summary as sibling
    /// scalar fields (`ingress_ms`, `normalization_done_ms`, …). Scalar metadata
    /// only — no body retention, so the snapshots-are-body-free invariant holds.
    #[serde(flatten)]
    pub phases: PhaseTimings,
    /// Gap 03 — the per-attempt failover trace (body-free: each [`Attempt`] holds only
    /// scalar provenance + bounded taxonomic codes, never a raw upstream error body).
    /// Empty array when no attempt was recorded. The attempt-trace UI (spec 11) reads it.
    pub attempts: Vec<Attempt>,
    /// Gap 03 — flow-level wire time-to-first-byte (`None` ⇒ absent ⇒ renders `—`, never
    /// `0`). Distinct from `first_content_delta_ms`: this is the upstream's first byte on
    /// the wire, that one is the first content delta to the client.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_upstream_byte_ms: Option<u128>,
    /// Gap 04 — the STABLE, NON-SECRET client attribution label (key-hash display id /
    /// configured caller-id / User-Agent fallback), body-free scalar metadata projected
    /// from the record. `None` (absent on the wire) when the request carried no
    /// attributable signal — renders `—` downstream, NEVER a fabricated id. The raw key
    /// is never present here (only the one-way hash prefix ever existed as a label).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_label: Option<String>,
    /// Gap 04 — the [`ClientSource`] the label was derived from, so the dashboard can
    /// render the weak `user_agent` fallback differently from a key-hash / configured-id
    /// attribution. `None` (absent) exactly when `client_label` is `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_source: Option<ClientSource>,
    /// Harness/session facts, flattened as sibling scalar fields (body-free).
    #[serde(flatten)]
    pub session: FlowSessionFacts,
    /// Gap 07 — the flow's USD cost, `None` when the served model is unpriced or
    /// there is no usage to bill (never a fabricated `0`).
    pub cost: Option<f64>,
    /// Gap 07 — the confidence tag that goes WITH `cost`. REQUIRED on every row:
    /// the SPA's `isFlowSummary` rejects a row without it, and one rejected row
    /// invalidates the whole snapshot, so the dashboard never leaves
    /// `'connecting'`. The FlowStore has no price table, so `from_record` starts
    /// it `Unavailable`; the socket layer prices it
    /// (`dashboard_ws::price_snapshot_summaries`) before the snapshot goes out.
    pub cost_confidence: crate::dashboard_api::CostConfidence,
}

impl SnapshotFlowSummary {
    fn from_record(record: &FlowRecord) -> Self {
        Self {
            api_call_id: record.api_call_id.clone(),
            response_id: record.response_id.clone(),
            method: record.method.clone(),
            uri: record.uri.clone(),
            model_requested: record.model_requested.clone(),
            model_served: record.model_served.clone(),
            upstream_target: record.upstream_target.clone(),
            usage: record.usage,
            status: record.status,
            started_ms: record.started_ms,
            finished_ms: record.finished_ms,
            elapsed_ms: record.elapsed_ms,
            terminal_reason: record.terminal_reason.clone(),
            // Gap 02: `PhaseTimings` is `Copy` — scalar metadata, no body.
            phases: record.phases,
            // Gap 03: the attempt trace is body-free scalar provenance — clone it onto
            // the summary so the WS/snapshot wire carries it.
            attempts: record.attempts.clone(),
            first_upstream_byte_ms: record.first_upstream_byte_ms,
            // Gap 04: the client attribution is body-free scalar metadata — project the
            // label + its source onto the summary (NEVER the raw key; only the one-way
            // hash prefix ever existed). `ClientSource` is `Copy`.
            client_label: record.client_label.clone(),
            client_source: record.client_source,
            session: record.session.clone(),
            // Priced by the socket layer, which owns the gateway's price table.
            cost: None,
            cost_confidence: crate::dashboard_api::CostConfidence::Unavailable,
        }
    }
}

/// One body-free summary SHARED between snapshot cuts. A flow that did not mutate
/// between two 5 s cuts is the SAME allocation in both (not a deep copy per cut), so
/// a flow resident for the whole ring costs one summary, not up to 720. Derefs to the
/// summary; serializes exactly like it (the wire shape is unchanged).
#[derive(Debug, Clone)]
pub struct SharedFlowSummary(Arc<SnapshotFlowSummary>);

impl SharedFlowSummary {
    /// Address of the shared allocation — the identity the snapshot ring uses to
    /// charge each unique summary to its memory quota exactly once.
    pub fn alloc_id(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }
}

impl std::ops::Deref for SharedFlowSummary {
    type Target = SnapshotFlowSummary;
    fn deref(&self) -> &SnapshotFlowSummary {
        &self.0
    }
}

impl Serialize for SharedFlowSummary {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (*self.0).serialize(serializer)
    }
}

/// The immutable, shareable summary list of one snapshot cut. When the FlowStore
/// did not mutate between two cuts (an idle box) both cuts hold the SAME list
/// allocation, so an idle cut copies nothing. Derefs to a slice (`iter()`,
/// `len()`, `for s in &cut.summaries` all work) and serializes as a JSON array.
#[derive(Debug, Clone)]
pub struct SnapshotSummaries(Arc<[SharedFlowSummary]>);

impl SnapshotSummaries {
    /// Address of the shared list allocation (see [`SharedFlowSummary::alloc_id`]).
    pub fn alloc_id(&self) -> usize {
        Arc::as_ptr(&self.0) as *const () as usize
    }
}

impl Default for SnapshotSummaries {
    fn default() -> Self {
        Self(Arc::from(Vec::new()))
    }
}

impl std::ops::Deref for SnapshotSummaries {
    type Target = [SharedFlowSummary];
    fn deref(&self) -> &[SharedFlowSummary] {
        &self.0
    }
}

impl<'a> IntoIterator for &'a SnapshotSummaries {
    type Item = &'a SharedFlowSummary;
    type IntoIter = std::slice::Iter<'a, SharedFlowSummary>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl Serialize for SnapshotSummaries {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter())
    }
}

/// An opaque RAII hold on the FlowStore lock handed to
/// [`DashboardFlowStore::with_summaries_under_lock`]'s closure. It exposes NOTHING of
/// the private interior state — its only purpose is to let the closure decide WHEN the
/// FlowStore lock is released (by dropping it / calling [`release`](Self::release)),
/// so the snapshot can release FlowStore the instant it has nested the metrics lock
/// and captured the cut cursors, then run the heavier metrics aggregation under the
/// metrics lock alone. Dropping the guard releases the lock. Empty (holds nothing) on
/// the disabled path.
pub struct FlowSnapshotGuard<'a> {
    guard: Option<std::sync::MutexGuard<'a, DashboardFlowState>>,
}

impl FlowSnapshotGuard<'_> {
    /// Release the FlowStore lock now (explicit form of dropping the guard). Idempotent.
    pub fn release(mut self) {
        self.guard = None;
    }
}

/// Interior state of the store, guarded by the single `Mutex`. `by_id` and `order`
/// are ALWAYS mutated together (no exterior LRU). `link_index` maps a
/// `response_id` to its owning `api_call_id` so `detail` can join by either id.
/// `live_summary_bytes` is the running total used to enforce the byte quota.
#[derive(Debug, Default)]
struct DashboardFlowState {
    by_id: HashMap<String, Arc<FlowRecord>>,
    order: VecDeque<String>,
    link_index: HashMap<String, String>,
    live_summary_bytes: usize,
    /// Monotonic mutation sequence — the FlowStore's per-domain cursor (D5). Bumped
    /// on every structural mutation (`insert`/`update`/`remove`) so a consumer can
    /// detect a change without a global watermark across the sibling stores
    /// (AGENTS.md: per-domain `{domain, seq}` cursors). The D5 5 s snapshot reads it
    /// at the cut instant; D7/D13 frames carry it.
    seq: u64,
    /// Snapshot-ring sharing: the last projected summary per live flow, tagged
    /// with the `record_seq` it was projected at. Every summary-visible mutation
    /// goes through `insert`/`update`, which stamp a fresh `record_seq`, so an
    /// equal tag proves the summary is still exact and can be reused by the next
    /// cut (body shedding changes no summary field and is correctly ignored).
    summary_cache: HashMap<String, (u64, SharedFlowSummary)>,
    /// The last cut's whole summary list, tagged with the `seq` it was built at:
    /// an unchanged `seq` means no insert/update/remove happened, so the next cut
    /// reuses the list allocation outright.
    shared_list: Option<(u64, SnapshotSummaries)>,
}

/// Authoritative store of per-flow records + the capture seam. Mirrors the
/// `MonitorHub::new()/disabled()` zero-overhead pattern: when `disabled()` every
/// operation early-returns and `is_enabled()` is `false`. `Clone` (the inner state
/// is behind `Arc<Mutex<_>>`, exactly like `MonitorHub`) so it threads into the
/// `#[derive(Clone)] Gateway` like the monitor does.
#[derive(Clone, Debug)]
pub struct DashboardFlowStore {
    enabled: bool,
    /// Gap 05 — whether upstream RESPONSE/ERROR-body capture is armed. SEPARATE from
    /// `enabled` (the debug-UI gate): `set_upstream_response` no-ops unless BOTH are
    /// true. Read env-only from [`ENV_CAPTURE_UPSTREAM_RESPONSE`] in `new()` (OFF by
    /// default); `disabled()` leaves it false. Never derived from a persisted `Config`.
    response_capture_enabled: bool,
    state: Arc<Mutex<DashboardFlowState>>,
    summary_quota_bytes: usize,
}

impl DashboardFlowStore {
    /// Enabled store (debug UI on). Uses the default 64 MiB summary-byte quota.
    /// Upstream RESPONSE/ERROR-body capture is armed ONLY when the separate
    /// [`ENV_CAPTURE_UPSTREAM_RESPONSE`] env flag is an explicit affirmative (OFF by
    /// default) — request capture does not imply response capture.
    pub fn new() -> Self {
        Self {
            enabled: true,
            response_capture_enabled: capture_response_env_flag(),
            state: Arc::new(Mutex::new(DashboardFlowState::default())),
            summary_quota_bytes: DEFAULT_SUMMARY_QUOTA_BYTES,
        }
    }

    /// No-op store (debug UI off). Every operation early-returns; production keeps
    /// zero overhead, mirroring `MonitorHub::disabled()`.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            response_capture_enabled: false,
            state: Arc::new(Mutex::new(DashboardFlowState::default())),
            summary_quota_bytes: DEFAULT_SUMMARY_QUOTA_BYTES,
        }
    }

    /// Test-only: an enabled store with the gap-05 upstream-response capture gate set
    /// deterministically (the production `new()` reads the gate from a process-global
    /// env var, which is racy across parallel tests). Lets a test exercise the
    /// capture-ON and capture-OFF branches without mutating the environment.
    #[cfg(test)]
    pub(crate) fn new_with_response_capture(response_capture_enabled: bool) -> Self {
        Self {
            enabled: true,
            response_capture_enabled,
            state: Arc::new(Mutex::new(DashboardFlowState::default())),
            summary_quota_bytes: DEFAULT_SUMMARY_QUOTA_BYTES,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Gap 05 — whether upstream RESPONSE/ERROR-body capture is armed (debug UI on AND
    /// the separate env gate set). The leaf checks this before reading an error body
    /// into a capture so that, when capture is off, no work and no retention happen.
    pub fn is_response_capture_enabled(&self) -> bool {
        self.enabled && self.response_capture_enabled
    }

    /// The FlowStore's per-domain mutation cursor (D5): a monotonic counter bumped
    /// on every structural mutation. `0` when disabled. The D5 5 s coordinated
    /// snapshot reads this at the cut instant (under the FlowStore lock, FIRST in the
    /// fixed lock order) so the cut's `flow_seq` matches the `snapshot_summaries()`
    /// it captured in the same critical section.
    pub fn flow_seq(&self) -> u64 {
        if !self.enabled {
            return 0;
        }
        self.lock().seq
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DashboardFlowState> {
        self.state
            .lock()
            .expect("dashboard flow state lock poisoned")
    }

    /// Open a new flow record keyed by `api_call_id`. No-op when disabled. The body
    /// and headers are [`CapturedBody`]/[`CapturedHeaders`] — minted ONLY by the
    /// capture primitives, so they are provably redacted + capped (D1 R2 #2); every
    /// other dynamic scalar is `cap_scalar`-bounded here before storage.
    ///
    /// Gap 04: `client` is the pre-derived [`ClientAttribution`] (a key-hash display
    /// id / configured caller-id / UA fallback, or [`ClientAttribution::none`]). It is
    /// already derived from the RAW headers at the middleware seam (the raw key was
    /// hashed + dropped THERE — it never reaches the store); its `label` is
    /// `cap_scalar`-bounded here like every other retained scalar.
    pub fn open(
        &self,
        api_call_id: String,
        method: String,
        uri: String,
        headers: CapturedHeaders,
        inbound_body: Option<CapturedBody>,
        client: ClientAttribution,
    ) {
        self.open_with_session(
            api_call_id,
            method,
            uri,
            headers,
            inbound_body,
            client,
            FlowSessionFacts::default(),
        );
    }

    /// [`open`](Self::open) plus the harness/session facts computed at the
    /// persistence seam (absent when persistence is off).
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_session(
        &self,
        api_call_id: String,
        method: String,
        uri: String,
        headers: CapturedHeaders,
        inbound_body: Option<CapturedBody>,
        client: ClientAttribution,
        session: FlowSessionFacts,
    ) {
        self.open_with_session_capture(
            api_call_id,
            method,
            uri,
            headers,
            inbound_body,
            client,
            session,
            true,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_with_session_capture(
        &self,
        api_call_id: String,
        method: String,
        uri: String,
        headers: CapturedHeaders,
        inbound_body: Option<CapturedBody>,
        client: ClientAttribution,
        session: FlowSessionFacts,
        capture_payloads: bool,
    ) {
        if !self.enabled {
            return;
        }
        let now = now_ms();
        let record = FlowRecord {
            claim: Arc::new(AtomicU8::new(CLAIM_OPEN_L0)),
            // Placeholder — `insert` stamps the post-bump global `seq` so the record's
            // watermark reflects THIS insert (D7b R2 finding 1).
            record_seq: 0,
            api_call_id: cap_scalar(api_call_id.clone()),
            response_id: None,
            method: cap_scalar(method),
            uri: cap_scalar(uri),
            headers: headers.0,
            inbound_body: inbound_body.map(CapturedBody::into_arc),
            capture_payloads,
            normalized: None,
            upstream_body: None,
            // Gap 05: the upstream RESPONSE/ERROR body is captured later (and only when
            // the separate capture gate is on AND the turn failed with an error body) —
            // `None` here = no upstream response captured yet.
            upstream_response: None,
            model_requested: None,
            model_served: None,
            upstream_target: None,
            usage: None,
            status: FlowStatus::Open,
            started_at: Instant::now(),
            started_ms: now,
            finished_ms: None,
            elapsed_ms: None,
            terminal_reason: None,
            // Gap 02: `ingress` is the first phase — stamp it at record open so the
            // waterfall always has a left anchor (≈ `started_ms`). The rest stay `None`
            // until the engine reaches their seams.
            phases: {
                let mut phases = PhaseTimings::default();
                phases.stamp_ingress(now);
                phases
            },
            // Gap 03: no attempt has run yet; the failover/leaf layers record attempts
            // onto the shared `ServingToken`, threaded into the record at finalize.
            attempts: Vec::new(),
            first_upstream_byte_ms: None,
            // Gap 04: the client attribution was derived pre-redaction at the middleware
            // seam; cap the label like every other retained scalar. `None` stays `None`
            // (an unattributed flow renders `—` downstream, never a fabricated id).
            client_label: client.label.map(cap_scalar),
            client_source: client.source,
            session,
        };
        let mut state = self.lock();
        state.prune_expired(now);
        state.insert(cap_scalar(api_call_id), Arc::new(record));
        state.enforce_caps(self.summary_quota_bytes);
    }

    /// Atomically bind a `response_id` to its flow's `api_call_id` (D1 R1 #8):
    /// fires exactly once per flow (first-link-wins). NO-OP when disabled, when the
    /// record is unknown, or when the record is already linked — so an unknown id
    /// can never leak a `link_index` entry and a repeat link can never alias.
    pub fn link(&self, response_id: String, api_call_id: String) {
        if !self.enabled {
            return;
        }
        let response_id = cap_scalar(response_id);
        let mut state = self.lock();
        state.prune_expired(now_ms());
        let Some(existing) = state.by_id.get(&api_call_id) else {
            // Unknown flow → no index entry, no record. (Avoids the leak where an
            // index entry outlives a never-existent record.)
            return;
        };
        if existing.response_id.is_some() {
            // Already linked: first-link-wins, idempotent.
            return;
        }
        state
            .link_index
            .insert(response_id.clone(), api_call_id.clone());
        state.update(&api_call_id, |record| {
            record.response_id = Some(response_id.clone());
        });
        state.enforce_caps(self.summary_quota_bytes);
    }

    /// Attach the upstream target + served model identity + upstream body (D2). The
    /// body is a [`CapturedBody`] (provably redacted + capped); scalars are
    /// `cap_scalar`-bounded. `id` may be the flow's `api_call_id` OR its
    /// `response_id` (the leaf only knows the latter): `update` joins by either via
    /// the link index, mirroring `detail`.
    pub fn set_upstream(
        &self,
        id: &str,
        upstream_target: Option<String>,
        model_served: Option<String>,
        upstream_body: Option<CapturedBody>,
    ) {
        if !self.enabled {
            return;
        }
        let upstream_target = upstream_target.map(cap_scalar);
        let model_served = model_served.map(cap_scalar);
        let upstream_body = upstream_body.map(CapturedBody::into_arc);
        let mut state = self.lock();
        state.prune_expired(now_ms());
        state.update(id, |record| {
            if upstream_target.is_some() {
                record.upstream_target = upstream_target.clone();
            }
            if model_served.is_some() {
                record.model_served = model_served.clone();
            }
            if record.capture_payloads && upstream_body.is_some() {
                record.upstream_body = upstream_body.clone();
            }
        });
        state.enforce_caps(self.summary_quota_bytes);
    }

    /// Gap 05 — attach the upstream RESPONSE/ERROR body to a flow's record. NO-OP
    /// unless [`is_response_capture_enabled`](Self::is_response_capture_enabled) (debug
    /// UI on AND the separate `LLMCONDUIT_DASHBOARD_CAPTURE_UPSTREAM_RESPONSE` gate set)
    /// — so production and a dashboard-without-the-gate retain nothing here. `body` is a
    /// [`CapturedResponseBody`] (provably redacted + capped + truncation-flagged via the
    /// same capped/redacting serializer the request layers use), so a `Bytes` slice of
    /// the inbound middleware buffer bounded by `max_request_body_bytes` is NEVER retained. `id` may be the flow's
    /// `api_call_id` OR its `response_id` (the leaf only knows the latter); `update`
    /// joins by either via the link index, mirroring [`set_upstream`](Self::set_upstream).
    /// First-write-wins is NOT enforced: a shrink-and-retry whose retry also fails
    /// overwrites the first error body with the one that actually ended the turn (the
    /// leaf calls this once per terminal error). Counted in the body quota + evicted with
    /// the other bodies; NEVER projected onto the body-free [`SnapshotFlowSummary`].
    pub fn set_upstream_response(&self, id: &str, body: Option<CapturedResponseBody>) {
        if !self.is_response_capture_enabled() {
            return;
        }
        // Nothing to record (the leaf passes `None` only defensively) — avoid taking the
        // lock + churning the quota for a no-op.
        let Some(body) = body else {
            return;
        };
        let response = body.into_record();
        let mut state = self.lock();
        state.prune_expired(now_ms());
        state.update(id, |record| {
            if record.capture_payloads {
                record.upstream_response = Some(response.clone());
            }
        });
        state.enforce_caps(self.summary_quota_bytes);
    }

    /// Attach the canonical/normalized body + requested model (D2). The body is a
    /// [`CapturedBody`] (provably redacted + capped); the model is `cap_scalar`-bounded.
    pub fn set_normalized(
        &self,
        api_call_id: &str,
        model_requested: Option<String>,
        normalized: Option<CapturedBody>,
    ) {
        if !self.enabled {
            return;
        }
        let model_requested = model_requested.map(cap_scalar);
        let normalized = normalized.map(CapturedBody::into_arc);
        let now = now_ms();
        let mut state = self.lock();
        state.prune_expired(now);
        state.update(api_call_id, |record| {
            // Gap 02: capturing the normalized canonical body IS the
            // normalization-settled seam. First-write-wins so the outer replay/tool
            // `loop` (which re-lowers per round) does not re-stamp.
            record.phases.stamp_normalization(now);
            if model_requested.is_some() {
                record.model_requested = model_requested.clone();
            }
            if record.capture_payloads && normalized.is_some() {
                record.normalized = normalized.clone();
            }
        });
        state.enforce_caps(self.summary_quota_bytes);
    }

    /// Mark a flow terminal (D3). Stamps `status`, `finished_ms`, `elapsed_ms`
    /// (`record.started_at.elapsed()` — a MONOTONIC `Instant` delta, never an
    /// epoch-ms subtraction), the terminal reason, and the serving-provider
    /// attribution read from D2's `ServingToken` at finalize time. The serving
    /// provider name (failover provider, else routing route) lands in
    /// `upstream_target` ONLY when the leaf did not already capture an upstream URL
    /// there — so the URL the leaf saw wins, and a path that never reached the leaf
    /// (pre-spawn early return, midstream cancel before first chunk) still records
    /// WHO would have served it. Idempotent at the store level too: a second
    /// finalize just re-stamps the same terminal fields (the D3 CAS guard is the
    /// real exactly-once authority).
    pub fn finalize(
        &self,
        api_call_id: &str,
        status: FlowStatus,
        terminal_reason: Option<String>,
        serving_provider: Option<String>,
    ) {
        if !self.enabled {
            return;
        }
        let terminal_reason = terminal_reason.map(cap_scalar);
        let serving_provider = serving_provider.map(cap_scalar);
        let now = now_ms();
        let mut state = self.lock();
        state.prune_expired(now);
        state.update(api_call_id, |record| {
            record.status = status;
            record.finished_ms = Some(now);
            record.elapsed_ms = Some(record.started_at.elapsed().as_millis());
            // Gap 02: stamp the `finalize` phase for EVERY terminal (completed, failed,
            // cancelled). First-write-wins: the D3 CAS guard already makes the explicit
            // finalize win, but stamping is idempotent so a store-level re-finalize keeps
            // the first terminal instant.
            record.phases.stamp_finalize(now);
            if terminal_reason.is_some() {
                record.terminal_reason = terminal_reason.clone();
            }
            // Don't clobber the leaf-captured upstream URL; only fill the slot when
            // it is still empty (the pre-leaf / pre-first-chunk paths).
            if record.upstream_target.is_none() && serving_provider.is_some() {
                record.upstream_target = serving_provider.clone();
            }
        });
        state.enforce_caps(self.summary_quota_bytes);
    }

    /// Attach token usage (D3). UPSERT semantics: the caller passes the running
    /// CUMULATIVE total for the flow, never an increment, so a multi-chunk turn
    /// whose usage is already cumulative does NOT double-count.
    pub fn record_usage(&self, api_call_id: &str, usage: FlowUsage) {
        if !self.enabled {
            return;
        }
        let mut state = self.lock();
        state.prune_expired(now_ms());
        state.update(api_call_id, |record| {
            record.usage = Some(usage);
        });
    }

    /// Gap 03 — thread the per-attempt failover trace + the flow-level wire
    /// time-to-first-byte onto the record. Driven at finalize from the shared
    /// `ServingToken` (the SAME evict-safe source the terminal metrics payload reads), so
    /// the record and the metrics path carry identical attempt data. `attempts` REPLACES
    /// (not appends): the token holds the complete ordered trace, so a re-finalize just
    /// re-writes the same vector (idempotent). `first_upstream_byte_ms` is
    /// first-write-wins (only fills the slot when still `None`) so the served attempt's
    /// measured wire-TTFB is never clobbered by a later `None`. No-op when disabled,
    /// unknown, or when `attempts` is empty AND no byte time is supplied (so a bare
    /// non-instrumented path adds nothing). Bumps `record_seq` like the other mutators.
    pub fn record_attempts(
        &self,
        api_call_id: &str,
        attempts: Vec<Attempt>,
        first_upstream_byte_ms: Option<u128>,
    ) {
        if !self.enabled {
            return;
        }
        if attempts.is_empty() && first_upstream_byte_ms.is_none() {
            return;
        }
        let mut state = self.lock();
        state.prune_expired(now_ms());
        state.update(api_call_id, |record| {
            if !attempts.is_empty() {
                record.attempts = attempts.clone();
            }
            // First-write-wins: a measured wire-first-byte must not be overwritten by a
            // later unmeasured (`None`) finalize path.
            if record.first_upstream_byte_ms.is_none()
                && let Some(byte_ms) = first_upstream_byte_ms
            {
                record.first_upstream_byte_ms = Some(byte_ms);
            }
        });
        state.enforce_caps(self.summary_quota_bytes);
    }

    /// Gap 02 — stamp the **routing decision** phase: the engine resolved the served
    /// model + candidate plan and successfully lowered the canonical request to the
    /// upstream chat payload (the point it commits to a backend, just before spawning
    /// the turn). Stamped at the engine seam (NOT the leaf's body-capture) so it fires
    /// for EVERY upstream client — the leaf only sees a flow that already reached the
    /// wire, while a routing decision exists the moment lowering succeeds even if a
    /// later pre-dispatch error occurs. FIRST-WRITE-WINS so a multi-turn flow records
    /// the FIRST routing decision. `api_call_id` keys the record directly (the engine
    /// stamps this pre-spawn, before the `response_id` link). No-op when disabled or
    /// unknown. Phase-only mutation (no body); bumps `record_seq`.
    pub fn stamp_routing_decision(&self, api_call_id: &str) {
        if !self.enabled {
            return;
        }
        let now = now_ms();
        let mut state = self.lock();
        state.prune_expired(now);
        state.update(api_call_id, |record| {
            record.phases.stamp_routing(now);
        });
    }

    /// Gap 02 — stamp the **first content delta** phase (true TTFT): the wall-clock
    /// instant the FIRST canonical CONTENT SSE delta was emitted to the client. The
    /// engine calls this from the `StreamEmission::OutputTextDelta` arm ONLY — never
    /// from reasoning/tool-argument/refusal/signature delta arms — so a stream that
    /// emits reasoning or tool deltas before content does NOT stamp it early.
    /// FIRST-WRITE-WINS: the arm fires per content delta but only the first stamps, so
    /// the value is the first token the client saw. `id` may be the flow's
    /// `api_call_id` OR its `response_id` (the engine keys by the latter mid-stream);
    /// `update` joins by either via the link index. No-op when disabled or unknown.
    /// Stamps no other field — purely the phase timestamp — so it adds no body and is
    /// cheap on the streaming hot path. This is a NEW phase-only mutation, so it bumps
    /// the record's `record_seq` exactly like the other mutators (the flow frame the
    /// dashboard later sends carries the freshly-stamped TTFT).
    pub fn stamp_first_content_delta(&self, id: &str) -> u128 {
        let now = now_ms();
        if !self.enabled {
            return now;
        }
        let mut state = self.lock();
        state.prune_expired(now);
        state.update(id, |record| {
            record.phases.stamp_first_content_delta(now);
        });
        now
    }

    /// Gap 02 — stamp the **stream end** phase: the engine reached the terminal
    /// `response.completed`/`response.incomplete` emission in `run_turn`. Distinct from
    /// `finalize` (which fires for failed/cancelled terminals too) — `stream_end` marks
    /// a clean stream completion. FIRST-WRITE-WINS so the outer replay/tool `loop`
    /// cannot re-stamp. `id` may be the `api_call_id` OR the `response_id`. No-op when
    /// disabled or unknown. Phase-only mutation (no body); bumps `record_seq` like the
    /// other mutators.
    pub fn stamp_stream_end(&self, id: &str) -> u128 {
        let now = now_ms();
        if !self.enabled {
            return now;
        }
        let mut state = self.lock();
        state.prune_expired(now);
        state.update(id, |record| {
            record.phases.stamp_stream_end(now);
        });
        now
    }

    /// Mint the D3 **L0 middleware guard** for a freshly `open`ed flow (the
    /// `compare_exchange(OpenL0 → Finalized-Failed)` RAII fallback). Returns `None`
    /// when the store is disabled OR the record is unknown (so a disabled store
    /// stays a pure no-op). The middleware holds the guard across `next.run`: if the
    /// request never reaches the engine (an extractor/`Json` rejection, a panic in a
    /// layer above the handler) the record is still `OpenL0` at the guard's `Drop`,
    /// which CASes it to `Finalized` and stamps a `Failed("unhandled")` terminal —
    /// no orphan stuck `Open`. If the engine claimed it (`ClaimedL1`), the L0 `Drop`
    /// is inert: the CAS fails and L1 owns finalization. This is SEPARATE from
    /// [`open`](Self::open) (which stays infallible + guardless) so the store's many
    /// internal/test callers and the D2 leaf helper — which manage the lifecycle via
    /// the engine's L1 path, not `next.run` — are unaffected.
    pub fn middleware_guard(&self, api_call_id: &str) -> Option<MiddlewareGuard> {
        if !self.enabled {
            return None;
        }
        let claim = self.lock().by_id.get(api_call_id)?.claim.clone();
        Some(MiddlewareGuard {
            store: self.clone(),
            api_call_id: api_call_id.to_string(),
            claim,
        })
    }

    /// Mint the D3 **L1 engine guard** by CASing the record's claim
    /// `OpenL0 → ClaimedL1`. Returns `None` when the store is disabled, the record
    /// is unknown, or the CAS loses (already `ClaimedL1`/`Finalized`) — so only the
    /// FIRST engine guard for a flow owns finalization, and a disabled store stays a
    /// no-op. On success the engine owns finalization on EVERY exit path (the
    /// pre-spawn early returns, the spawned body, the terminal arms) via
    /// [`TelemetryGuard::finalize`]; its RAII `Drop` is the fallback that finalizes
    /// `Cancelled` if a path is missed (client hang-up / panic / cancel). The guard
    /// holds only `Arc`s + an `Instant` + an owned id, so it is `Send` and crosses
    /// the `tokio::spawn` in `stream_responses`.
    pub fn engine_guard(
        &self,
        api_call_id: &str,
        serving: std::sync::Arc<crate::upstream::ServingToken>,
        abort_hub: &AbortHub,
    ) -> Option<TelemetryGuard> {
        if !self.enabled {
            return None;
        }
        // Read the claim Arc + the inbound route in ONE lock: the record provably
        // exists here, so capturing `uri` now makes the metrics `endpoint` evict-safe
        // (D5 R3 MEDIUM) — it no longer depends on the record surviving until finalize.
        let (claim, endpoint) = {
            let state = self.lock();
            let record = state.by_id.get(api_call_id)?;
            (record.claim.clone(), record.uri.clone())
        };
        // CAS OpenL0 → ClaimedL1. Only the winner gets a guard.
        claim
            .compare_exchange(
                CLAIM_OPEN_L0,
                CLAIM_CLAIMED_L1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .ok()?;
        // D6: the CAS winner is the flow's sole telemetry writer, so it is also the
        // sole owner of the AbortHub entry. Register the kill token HERE (the same
        // claim seam) and remove it on EVERY finalize path below — so the map is
        // bounded by in-flight streams and a finished flow leaks nothing. The token
        // is cloned onto the guard so the engine can compose `token.is_cancelled()`
        // alongside every `tx.closed()` client-hangup check.
        let abort_token = CancellationToken::new();
        abort_hub.register(api_call_id, abort_token.clone());
        Some(TelemetryGuard {
            store: self.clone(),
            api_call_id: api_call_id.to_string(),
            claim,
            serving,
            started: Instant::now(),
            endpoint,
            terminal_metrics: Mutex::new(None),
            abort_hub: abort_hub.clone(),
            abort_token,
        })
    }

    /// Live records, newest-first. Empty when disabled. Prunes expired records
    /// first (D1 R1 #7 — TTL no longer depends on `open` traffic).
    pub fn list(&self) -> Vec<Arc<FlowRecord>> {
        if !self.enabled {
            return Vec::new();
        }
        let mut state = self.lock();
        state.prune_expired(now_ms());
        state
            .order
            .iter()
            .rev()
            .filter_map(|id| state.by_id.get(id).cloned())
            .collect()
    }

    /// Number of live records with `status == Open` (the dashboard's
    /// `active_streams`). Counts in place under the lock — every viewer asks once
    /// a second, and [`list`](Self::list) would clone every record handle into a
    /// fresh `Vec` just to count it. `0` when disabled.
    pub fn open_count(&self) -> u64 {
        if !self.enabled {
            return 0;
        }
        let mut state = self.lock();
        state.prune_expired(now_ms());
        state
            .by_id
            .values()
            .filter(|record| record.status == FlowStatus::Open)
            .count() as u64
    }

    /// Resolve a single record by `api_call_id` OR `response_id` (via the link
    /// index). `None` when disabled or unknown. Prunes expired records first.
    pub fn detail(&self, id: &str) -> Option<Arc<FlowRecord>> {
        if !self.enabled {
            return None;
        }
        let mut state = self.lock();
        state.prune_expired(now_ms());
        if let Some(record) = state.by_id.get(id) {
            return Some(Arc::clone(record));
        }
        let api_call_id = state.link_index.get(id)?;
        state.by_id.get(api_call_id).cloned()
    }

    /// Resolve a record AND **that record's own mutation watermark** (`record_seq`)
    /// read in the SAME lock hold (D7b R1 finding 3, hardened by R2 finding 1). The
    /// returned `seq` is the global counter value AS OF THE RECORD'S LAST MUTATION —
    /// NOT the live global `state.seq` re-read at send time. The dashboard flow frame
    /// is stamped with THIS, so a queued/replayed older monitor update cannot be
    /// stamped with a NEWER global cursor (bumped by UNRELATED flows in the gap): the
    /// earlier `state.seq` form leapfrogged exactly those unrelated mutations, advancing
    /// the client's single flow cursor past a genuinely newer flow frame so it got
    /// whole-frame-deduped. `record_seq` stays drawn from the same global counter (so it
    /// is directly comparable to the snapshot's `flow_seq` dedup baseline) yet is frozen
    /// at the record's own progress, so the stamp matches the record state delivered AND
    /// only advances the cursor by THIS record's own mutations. `None` when disabled or
    /// unknown.
    pub fn detail_with_seq(&self, id: &str) -> Option<(Arc<FlowRecord>, u64)> {
        if !self.enabled {
            return None;
        }
        let mut state = self.lock();
        state.prune_expired(now_ms());
        if let Some(record) = state.by_id.get(id) {
            let seq = record.record_seq;
            return Some((Arc::clone(record), seq));
        }
        let api_call_id = state.link_index.get(id)?.clone();
        let record = state.by_id.get(&api_call_id)?;
        let seq = record.record_seq;
        Some((Arc::clone(record), seq))
    }

    /// Body-free snapshot summaries, newest-first. Empty when disabled. Prunes
    /// expired records first.
    pub fn snapshot_summaries(&self) -> Vec<SnapshotFlowSummary> {
        if !self.enabled {
            return Vec::new();
        }
        let mut state = self.lock();
        state.prune_expired(now_ms());
        state
            .order
            .iter()
            .rev()
            .filter_map(|id| state.by_id.get(id))
            .map(|record| SnapshotFlowSummary::from_record(record))
            .collect()
    }

    /// Body-free snapshot summaries **AND** the FlowStore domain `seq`, captured in ONE
    /// lock hold (D7b R2 finding 2). The `/dashboard/ws` initial snapshot MUST seed its
    /// `flow_seq` dedup baseline from the SAME critical section that produced `flows`:
    /// reading `snapshot_summaries()` and `flow_seq()` as SEPARATE lock acquisitions
    /// leaves a window where a mutation between them seeds an OLDER body with a NEWER
    /// cursor — so that mutation's own live flow frame (stamped at the older `record_seq`
    /// ≤ the leaked newer baseline) is permanently whole-frame-deduped on the client and
    /// the row never updates. Returning the pair atomically guarantees the cursor is
    /// exactly the watermark of the summaries delivered, so every later live frame with a
    /// strictly newer record_seq is accepted. Prunes expired records first (so the seq
    /// already reflects any prune). `(Vec::new(), 0)` when disabled.
    pub fn snapshot_summaries_with_seq(&self) -> (Vec<SnapshotFlowSummary>, u64) {
        if !self.enabled {
            return (Vec::new(), 0);
        }
        let mut state = self.lock();
        state.prune_expired(now_ms());
        let summaries = state
            .order
            .iter()
            .rev()
            .filter_map(|id| state.by_id.get(id))
            .map(|record| SnapshotFlowSummary::from_record(record))
            .collect();
        (summaries, state.seq)
    }

    /// Run `f` with the body-free summaries + the FlowStore `seq` while the FlowStore
    /// mutex is STILL HELD (D5 single-critical-section snapshot, Codex D5 R1 #1). The
    /// 5 s coordinated snapshot uses this so it can acquire the MetricsLayer mutex (the
    /// FIXED FlowStore→Metrics order) and capture the topology `Arc` WHILE this lock is
    /// still held — making the cut a single atomic instant across all three stores. The
    /// alternative — read summaries + seq, RELEASE this lock, THEN take the metrics lock
    /// — leaves a torn-cut window: a concurrent writer could mutate BOTH stores in the
    /// gap, so a cut would include a metrics sample for a flow absent from the summaries.
    ///
    /// `f` receives a [`FlowSnapshotGuard`] it OWNS, so it can `drop` the FlowStore lock
    /// the MOMENT it has acquired the metrics lock + captured the cut-defining cursors —
    /// then run the (heavier) metrics aggregation under the metrics lock ALONE. Holding
    /// the metrics lock is sufficient to keep the cut consistent after the FlowStore
    /// release: no writer can complete a both-stores mutation while the metrics lock is
    /// held (every metrics mutation needs it), and the summaries are already a snapshot
    /// copy. Releasing FlowStore early keeps its critical section minimal so concurrent
    /// writers are not starved by the snapshot's aggregation work.
    ///
    /// The FIXED lock order is FlowStore→Metrics: `f` is the ONLY place that acquires a
    /// second lock while this one is held, and it MUST NOT re-enter the FlowStore (it
    /// touches only Metrics + topology), so no deadlock is possible. Pruning happens
    /// once, before the summaries are built, so the seq already reflects any prune. When
    /// disabled, `f` runs with an empty guard + `(Vec::new(), 0)` and no lock is taken.
    ///
    /// The summaries are SHARED with earlier cuts wherever a flow did not mutate (and
    /// the whole list is shared when nothing mutated), so a cut costs only the flows
    /// that changed since the previous one.
    pub fn with_summaries_under_lock<F, R>(&self, f: F) -> R
    where
        F: FnOnce(FlowSnapshotGuard<'_>, SnapshotSummaries, u64) -> R,
    {
        if !self.enabled {
            return f(
                FlowSnapshotGuard { guard: None },
                SnapshotSummaries::default(),
                0,
            );
        }
        let mut state = self.lock();
        state.prune_expired(now_ms());
        let summaries = state.shared_summaries();
        let seq = state.seq;
        // Hand the guard to `f` so it controls when the FlowStore lock is released
        // (after it has nested the metrics lock under this one).
        f(FlowSnapshotGuard { guard: Some(state) }, summaries, seq)
    }

    /// Test-only: prune at a caller-supplied clock so the TTL is deterministically
    /// observable without sleeping (D1 R1 #7).
    #[cfg(test)]
    fn prune_at(&self, now_ms: u128) {
        let mut state = self.lock();
        state.prune_expired(now_ms);
    }

    /// Test-only: backdate a record's `started_ms` so a deterministic TTL test can
    /// age it past the retention window.
    #[cfg(test)]
    fn force_started_ms(&self, api_call_id: &str, started_ms: u128) {
        let mut state = self.lock();
        state.update(api_call_id, |record| record.started_ms = started_ms);
    }
}

impl Default for DashboardFlowStore {
    fn default() -> Self {
        Self::new()
    }
}

/// D3 **L0 (middleware) telemetry guard**: an RAII fallback that finalizes a flow
/// record IFF it is still `OpenL0` at `Drop`. Held by `log_api_call` across
/// `next.run`. It owns a cheap store handle (`Arc<Mutex<_>>` clone) + the record's
/// shared `claim` `Arc`, so it carries no borrow and is trivially `Send`. The CAS
/// (`OpenL0 → Finalized`) is the ONLY ownership-transfer mechanism — if the engine
/// claimed the record (`ClaimedL1`) the CAS here fails and L1 owns finalization;
/// the L0 `Drop` is then inert. This catches the narrow window where the middleware
/// opened a record but the request NEVER reached the engine (an extractor/`Json`
/// rejection, a layer panic): without L0 that record would be stuck `Open` forever.
#[must_use = "the L0 middleware guard finalizes an orphan flow on Drop; hold it across next.run"]
pub struct MiddlewareGuard {
    store: DashboardFlowStore,
    api_call_id: String,
    claim: Arc<AtomicU8>,
}

impl Drop for MiddlewareGuard {
    fn drop(&mut self) {
        // Finalize ONLY if still OpenL0 (the engine never claimed it). Race-free:
        // the CAS atomically transfers ownership; a concurrent L1 claim makes this
        // fail and L1 finalizes instead.
        if self
            .claim
            .compare_exchange(
                CLAIM_OPEN_L0,
                CLAIM_FINALIZED,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            self.store.finalize(
                &self.api_call_id,
                FlowStatus::Failed,
                Some("unhandled".to_string()),
                None,
            );
        }
    }
}

/// D3 **L1 (engine) telemetry guard**: owns finalization of a flow once it has CASed
/// the record `OpenL0 → ClaimedL1` (via [`DashboardFlowStore::engine_guard`]). The
/// engine calls [`finalize`](Self::finalize) on EVERY deterministic exit path
/// (pre-spawn early returns, the spawned body's `Completed`/`Failed` arms) with the
/// terminal status; the RAII `Drop` is the fallback that finalizes `Cancelled` if a
/// path is missed (client hang-up mid-stream, panic, cancel). Finalization is
/// IDEMPOTENT: it CASes `ClaimedL1 → Finalized`, so the explicit call wins and the
/// `Drop` fallback then no-ops (and a double explicit call no-ops).
///
/// Holds only `Arc`s (`store` handle, shared `claim`, shared `serving`), an owned
/// `String` id, and a monotonic `Instant` — all `Send` — so the guard moves into the
/// `tokio::spawn` in `stream_responses` (the midstream-cancel test compiles + runs
/// across the spawn). The serving provider for attribution is read from the shared
/// `ServingToken` AT finalize time (not at construction), so a provider that the
/// failover/routing layer tags AFTER the guard is built is still recorded.
#[must_use = "the L1 engine guard finalizes the flow on Drop; bind it for the flow's lifetime"]
pub struct TelemetryGuard {
    store: DashboardFlowStore,
    api_call_id: String,
    claim: Arc<AtomicU8>,
    serving: Arc<crate::upstream::ServingToken>,
    started: Instant,
    /// D5 R3 (MEDIUM): the inbound route (`record.uri`), captured at CLAIM time (when
    /// the record provably exists). The metrics `endpoint` dimension reads from this
    /// owned copy, so it survives a later TTL prune / cap eviction of the record.
    endpoint: String,
    /// D5 R3 (MEDIUM): the metrics inputs the engine records at the terminal seam,
    /// assembled at finalize from the guard's OWN evict-safe sources — the captured
    /// `endpoint` + the shared `ServingToken` (which carries the resolved
    /// `model_served`, the serving route/provider, and the final cumulative usage) —
    /// NOT by re-reading the FlowStore record via `detail()`. The FlowStore can prune
    /// (TTL) or evict (cap) a long-running / high-concurrency flow BEFORE finalize, so a
    /// `detail()` re-read would `None`-out and make the authoritative metrics layer
    /// UNDERCOUNT completed requests; sourcing from the guard makes metrics independent
    /// of FlowStore retention. `Mutex` for interior mutability behind the guard's
    /// `&self` finalize; the winning CAS writes it exactly once, before any reader.
    terminal_metrics: Mutex<Option<TerminalMetricsInputs>>,
    /// D6: the AbortHub this guard registered its kill token into at claim time. The
    /// CAS-winning `finalize` (and the `Drop` fallback) REMOVES the `api_call_id` entry
    /// so a finished flow never leaks a token (the map stays bounded by in-flight
    /// streams). Cheap `Arc`-sharing clone; `disabled()` makes remove a no-op.
    abort_hub: AbortHub,
    /// D6: the flow's cancellation token, registered in `abort_hub` under
    /// `api_call_id`. The engine reads it via [`abort_token`](Self::abort_token) and
    /// composes `is_cancelled()`/`cancelled()` alongside every `tx.closed()` check, so a
    /// kill surfaces `AppError::cancelled()` (499) exactly like a client hang-up. The
    /// token is NOT cancelled by `Drop` — a clean Completed/Failed flow must leave it
    /// uncancelled; only an explicit `abort()` flips it.
    abort_token: CancellationToken,
}

impl TelemetryGuard {
    /// The flow's `api_call_id` (so the engine can drive `record_usage` upserts on
    /// the SAME record the guard owns without re-threading the id separately).
    pub fn api_call_id(&self) -> &str {
        &self.api_call_id
    }

    /// D6: the flow's cancellation token (a cheap `Arc`-sharing clone). The engine
    /// threads this into `run_turn` and composes `is_cancelled()` (the poll sites) /
    /// `cancelled()` (the `tokio::select!` sites) ALONGSIDE every existing `tx.closed()`
    /// client-hangup check, so a `Gateway::abort` surfaces `AppError::cancelled()` (499)
    /// just like a hang-up — never duplicating or replaying tokens.
    pub fn abort_token(&self) -> CancellationToken {
        self.abort_token.clone()
    }

    /// Monotonic elapsed since the guard claimed the flow — the latency source the
    /// engine reports, never an epoch-ms subtraction. (`finalize` also stamps the
    /// store's own `started_at.elapsed()`; this accessor exists for callers/tests
    /// that want the guard-relative value.)
    pub fn elapsed(&self) -> std::time::Duration {
        self.started.elapsed()
    }

    /// The metrics inputs the guard assembled at finalize (D5 R3 MEDIUM), or `None` if
    /// the guard has not finalized yet. Once the guard finalizes this is always `Some`
    /// — it is built from the guard's OWN sources (claim-captured endpoint + the shared
    /// ServingToken), NOT the FlowStore record, so it is populated even if the record
    /// was pruned/evicted before finalize. The engine's D5 terminal metrics seam reads
    /// this AFTER calling [`finalize`](Self::finalize), recording independent of
    /// FlowStore retention.
    pub fn terminal_metrics(&self) -> Option<TerminalMetricsInputs> {
        self.terminal_metrics
            .lock()
            .expect("telemetry guard terminal-metrics lock poisoned")
            .clone()
    }

    /// Explicitly finalize the flow with `status` + `terminal_reason`, attributing
    /// the serving provider read from the shared `ServingToken` (failover provider,
    /// else routing route). IDEMPOTENT via `compare_exchange(ClaimedL1 → Finalized)`:
    /// the first finalize (explicit OR the `Drop` fallback) wins; later calls
    /// no-op, so the engine's explicit terminal status is never overwritten by the
    /// `Drop`'s `Cancelled` fallback. The winning finalize ALSO assembles + stashes the
    /// metrics inputs from the guard's own evict-safe sources (claim-captured endpoint +
    /// the shared ServingToken; D5 R3 MEDIUM) so the engine's metrics record never
    /// re-reads the (possibly-evicted) record.
    pub fn finalize(&self, status: FlowStatus, terminal_reason: Option<String>) {
        if self
            .claim
            .compare_exchange(
                CLAIM_CLAIMED_L1,
                CLAIM_FINALIZED,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            let (route, provider) = self.serving.snapshot();
            // Prefer the actual serving provider; fall back to the route name. This is
            // both the FlowStore `upstream_target` fallback AND the metrics `upstream`.
            let serving = provider.or(route);
            // D5 R3 (MEDIUM): assemble the metrics inputs from the guard's OWN
            // evict-safe sources — the `endpoint` captured at claim + the shared
            // `ServingToken`'s resolved `model_served`, serving label, and final usage —
            // so the engine's terminal metrics record never re-reads the (possibly
            // pruned/evicted) FlowStore record. Captured BEFORE the `store.finalize`
            // below so it is independent of whether the record still exists.
            let (model_served, usage) = self.serving.metrics_snapshot();
            // Gap 03: the per-attempt trace + flow-level wire-first-byte ride the SAME
            // evict-safe `ServingToken` as `usage`, so the terminal metrics payload (spec
            // 12's source) carries every attempt even if the FlowStore record is
            // pruned/evicted before finalize. Snapshot them here, BEFORE the
            // `store.finalize`/`record_attempts` below, so the payload is independent of
            // whether the record still exists.
            let (attempts, first_upstream_byte_ms) = self.serving.attempts_snapshot();
            let (first_content_delta_ms, stream_end_ms) = self.serving.phase_snapshot();
            *self
                .terminal_metrics
                .lock()
                .expect("telemetry guard terminal-metrics lock poisoned") =
                Some(TerminalMetricsInputs {
                    model_served,
                    endpoint: self.endpoint.clone(),
                    upstream: serving.clone(),
                    usage,
                    prefill_ms: first_upstream_byte_ms
                        .zip(first_content_delta_ms)
                        .map(|(start, end)| end.saturating_sub(start)),
                    decode_ms: first_content_delta_ms
                        .zip(stream_end_ms)
                        .map(|(start, end)| end.saturating_sub(start)),
                    attempts: attempts.clone(),
                });
            self.store
                .finalize(&self.api_call_id, status, terminal_reason, serving);
            // Gap 03: ALSO thread the attempt trace onto the FlowStore record (the
            // attempt-trace UI reads the record/summary). Same evict-safe source; a
            // pruned/evicted record makes this a no-op, but the terminal payload above
            // still carries the attempts for metrics.
            self.store
                .record_attempts(&self.api_call_id, attempts, first_upstream_byte_ms);
            // Gap 05 round-1 review (F1): commit the turn's PENDING upstream RESPONSE/ERROR
            // body — staged on the shared `ServingToken` by the leaf's terminal-error sites
            // — onto the FlowStore record HERE, at finalize, AFTER the failover layer has
            // decided the turn's FINAL outcome. The token holds a body IFF the turn
            // ultimately FAILED with a captured error body: a provider that served the turn
            // CLEARED it (so a successful turn commits `None` — no stale earlier-attempt
            // error body), while an all-providers-fail turn carries the LAST attempt's body.
            // `take` moves it out so a re-finalize cannot re-commit. `set_upstream_response`
            // is itself gated on the response-capture flag (and no-ops on a pruned/evicted
            // record), so when capture is off this is a cheap no-op — the leaf staged nothing
            // anyway.
            self.store.set_upstream_response(
                &self.api_call_id,
                self.serving.take_pending_response_body(),
            );
            // D6: drop the kill token from the AbortHub on the SAME CAS-winning path
            // that finalizes the record — so EVERY terminal (explicit Completed/Failed
            // here, OR the `Drop` fallback's Cancelled) removes the entry exactly once.
            // No finished flow leaks a token; the map stays bounded by in-flight streams,
            // not the 512-record history. Inside the CAS guard so a double finalize (the
            // Drop after an explicit call) does not double-remove.
            self.abort_hub.remove(&self.api_call_id);
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // Fallback: a path that did not call `finalize` explicitly (client hang-up
        // mid-stream, panic, cancel) lands here while still `ClaimedL1`. Finalize
        // `Cancelled` — the conservative terminal for "the engine stopped owning
        // this flow without a clean Completed/Failed". If the engine already
        // finalized, the CAS in `finalize` already moved the claim to `Finalized`
        // and this no-ops.
        self.finalize(FlowStatus::Cancelled, Some("dropped".to_string()));
    }
}

impl DashboardFlowState {
    /// The newest-first body-free summary list for a snapshot cut, reusing every
    /// allocation that is still exact (see `summary_cache` / `shared_list`).
    fn shared_summaries(&mut self) -> SnapshotSummaries {
        if let Some((built_at, list)) = &self.shared_list
            && *built_at == self.seq
        {
            return list.clone();
        }
        let mut list = Vec::with_capacity(self.order.len());
        for id in self.order.iter().rev() {
            let Some(record) = self.by_id.get(id) else {
                continue;
            };
            let summary = match self.summary_cache.get(id) {
                Some((record_seq, cached)) if *record_seq == record.record_seq => cached.clone(),
                _ => {
                    let fresh =
                        SharedFlowSummary(Arc::new(SnapshotFlowSummary::from_record(record)));
                    self.summary_cache
                        .insert(id.clone(), (record.record_seq, fresh.clone()));
                    fresh
                }
            };
            list.push(summary);
        }
        let list = SnapshotSummaries(Arc::from(list));
        self.shared_list = Some((self.seq, list.clone()));
        list
    }

    /// Insert (or replace) a record, keeping `by_id` + `order` in lockstep and the
    /// `live_summary_bytes` total correct. Bumps the global `seq` and stamps the
    /// record's own `record_seq` with the post-bump value (D7b R2 finding 1), so the
    /// record carries the exact watermark of THIS mutation.
    fn insert(&mut self, api_call_id: String, mut record: Arc<FlowRecord>) {
        if let Some(previous) = self.by_id.remove(&api_call_id) {
            self.live_summary_bytes = self
                .live_summary_bytes
                .saturating_sub(previous.summary_bytes());
            self.order.retain(|id| id != &api_call_id);
        }
        self.seq = self.seq.saturating_add(1);
        // Stamp the record's own watermark with the post-bump global seq. The Arc is
        // freshly minted by `open` (refcount 1), so `make_mut` mutates in place without
        // a clone.
        Arc::make_mut(&mut record).record_seq = self.seq;
        self.live_summary_bytes = self
            .live_summary_bytes
            .saturating_add(record.summary_bytes());
        self.order.push_back(api_call_id.clone());
        self.by_id.insert(api_call_id, record);
    }

    /// Resolve any flow id — an `api_call_id` (direct `by_id` key) OR a
    /// `response_id` (via the link index) — to the owning `api_call_id`. Mirrors
    /// the dual-id join `detail` already performs so the mutators below can be
    /// driven by EITHER id (D2's leaf only knows the flow's `response_id`; D3 also
    /// keys by it). `None` when the id matches no live record.
    fn resolve_id(&self, id: &str) -> Option<String> {
        if self.by_id.contains_key(id) {
            return Some(id.to_string());
        }
        self.link_index.get(id).cloned()
    }

    /// Copy-on-write mutate the record for `id` (an `api_call_id` OR a linked
    /// `response_id`): clone the inner record (the `claim` `Arc` is shared, NOT
    /// deep-copied, so D3's atomic identity persists), apply `edit`, and swap the
    /// `Arc` back in. Adjusts `live_summary_bytes` by the byte delta. No-op when the
    /// id resolves to no record.
    fn update(&mut self, id: &str, edit: impl FnOnce(&mut FlowRecord)) {
        let Some(api_call_id) = self.resolve_id(id) else {
            return;
        };
        let Some(existing) = self.by_id.get(&api_call_id) else {
            return;
        };
        let before = existing.summary_bytes();
        let mut next = (**existing).clone();
        edit(&mut next);
        let after = next.summary_bytes();
        self.live_summary_bytes = self
            .live_summary_bytes
            .saturating_sub(before)
            .saturating_add(after);
        // Bump the global seq and stamp THIS record's own watermark with the post-bump
        // value (D7b R2 finding 1), so `detail_with_seq` returns the record's own
        // mutation seq, not a later global value bumped by unrelated flows.
        self.seq = self.seq.saturating_add(1);
        next.record_seq = self.seq;
        self.by_id.insert(api_call_id, Arc::new(next));
    }

    /// Drop records whose age exceeds the TTL, keyed off `started_ms` vs a
    /// caller-supplied `now`.
    fn prune_expired(&mut self, now: u128) {
        let cutoff = now.saturating_sub(FLOW_TTL_MS);
        let expired: Vec<String> = self
            .order
            .iter()
            .filter(|id| {
                self.by_id
                    .get(*id)
                    .is_some_and(|record| record.started_ms < cutoff)
            })
            .cloned()
            .collect();
        for id in expired {
            self.remove(&id);
        }
    }

    /// Enforce the record-count cap (evict oldest WHOLE records) then the
    /// summary-byte quota (evict OLDEST BODIES first — set body `Arc`s to `None`;
    /// the record survives as a body-free summary).
    fn enforce_caps(&mut self, quota_bytes: usize) {
        while self.order.len() > FLOW_CAP {
            let Some(oldest) = self.order.front().cloned() else {
                break;
            };
            self.remove(&oldest);
        }
        self.enforce_summary_quota(quota_bytes);
    }

    /// Bring the running summary-byte total back under quota. Phase 1: walk records
    /// oldest-first dropping their BODIES (the record survives as a body-free
    /// summary). Phase 2 (D1 R2 #3): if shedding every body is still not enough —
    /// because scalar/header-only records dominate the total — evict OLDEST WHOLE
    /// records until under quota, so the quota is a HARD bound the store cannot
    /// exceed regardless of body presence.
    fn enforce_summary_quota(&mut self, quota_bytes: usize) {
        if self.live_summary_bytes <= quota_bytes {
            return;
        }
        // Phase 1: shed bodies oldest-first.
        let ids: Vec<String> = self.order.iter().cloned().collect();
        for id in ids {
            if self.live_summary_bytes <= quota_bytes {
                return;
            }
            let Some(existing) = self.by_id.get(&id) else {
                continue;
            };
            if !existing.has_body() {
                continue;
            }
            let freed = existing.body_bytes();
            let mut next = (**existing).clone();
            next.inbound_body = None;
            next.normalized = None;
            next.upstream_body = None;
            next.upstream_response = None;
            self.by_id.insert(id, Arc::new(next));
            self.live_summary_bytes = self.live_summary_bytes.saturating_sub(freed);
        }
        // Phase 2: bodies are all gone but still over quota → evict whole records
        // oldest-first. (`remove` keeps `order`/`link_index`/byte-total consistent.)
        while self.live_summary_bytes > quota_bytes {
            let Some(oldest) = self.order.front().cloned() else {
                break;
            };
            self.remove(&oldest);
        }
    }

    /// Remove a record (and its link-index back-references), keeping all three
    /// structures + the byte total consistent.
    fn remove(&mut self, api_call_id: &str) {
        if let Some(record) = self.by_id.remove(api_call_id) {
            self.live_summary_bytes = self
                .live_summary_bytes
                .saturating_sub(record.summary_bytes());
            if let Some(response_id) = &record.response_id {
                self.link_index.remove(response_id);
            }
        }
        self.summary_cache.remove(api_call_id);
        self.order.retain(|id| id != api_call_id);
        // Drop any dangling link-index entries that pointed at this id.
        self.link_index.retain(|_, owner| owner != api_call_id);
        self.seq = self.seq.saturating_add(1);
    }
}

/// Capped + redacting capture of a request/response body → a [`CapturedBody`]
/// (`Arc<[u8]>` ≤ [`BODY_CAP`], secrets redacted inline) via the shared O(CAP)
/// primitive in [`crate::redaction`]. This is the ONLY constructor of
/// `CapturedBody`, so every body that reaches a store mutator is provably sanitized
/// (D1 R2 #2). Never retains a slice of `raw`.
pub fn capture_body(raw: &[u8]) -> CapturedBody {
    let bytes = crate::redaction::capture_capped_redacted(raw, BODY_CAP, SCALAR_CAP);
    CapturedBody(Arc::from(bytes.into_boxed_slice()))
}

/// Gap 05 — capped + redacting capture of an upstream RESPONSE/ERROR body → a
/// [`CapturedResponseBody`], plus a TRUNCATION flag. Same guarantees as
/// [`capture_body`] (the body is redacted + capped via the shared O(CAP) primitive and
/// NEVER retains a slice of `raw`), so the leaf can copy the upstream error body it
/// already read (a `String`/`&[u8]`) without keeping the inbound middleware buffer bounded by `max_request_body_bytes`
/// alive. `truncated` is `raw.len() > BODY_CAP` — i.e. the RAW body exceeded the cap, so
/// the retained bytes are a prefix the dashboard must flag (don't present a partial body
/// as complete). Note the redacted output can be a fixed marker (`[redacted: unparseable
/// body …]`) for a non-JSON/over-bound body; `truncated` reflects the raw input length,
/// independent of that marker, so an over-cap body is flagged truncated even when the
/// stored bytes are the marker.
pub fn capture_response_body(raw: &[u8]) -> CapturedResponseBody {
    capture_response_body_with_truncation(raw, false)
}

/// Capture an already transport-capped upstream body while preserving the fact
/// that bytes existed beyond `raw`. This keeps the dashboard's live-body
/// metadata truthful even though the HTTP reader deliberately stopped early.
pub fn capture_response_body_with_truncation(
    raw: &[u8],
    externally_truncated: bool,
) -> CapturedResponseBody {
    let truncated = externally_truncated || raw.len() > BODY_CAP;
    let bytes = crate::redaction::capture_capped_redacted(raw, BODY_CAP, SCALAR_CAP);
    CapturedResponseBody {
        body: Arc::from(bytes.into_boxed_slice()),
        truncated,
    }
}

/// Capture a `Serialize` value (the typed on-wire request) into a [`CapturedBody`]
/// WITHOUT a full O(body) `serde_json::to_vec` (D2 R1 #1). Serializes directly into
/// a writer bounded at `2 × BODY_CAP` and redacts the bounded bytes via the shared
/// O(CAP) primitive — peak heap is O(`BODY_CAP`), never O(body). Same `CapturedBody`
/// guarantees as [`capture_body`] (provably redacted + capped); the leaf uses THIS
/// so a max-size request body is never serialized in full just to be capped.
pub fn capture_body_from_value<T: serde::Serialize>(value: &T) -> CapturedBody {
    let bytes = crate::redaction::capture_capped_redacted_value(value, BODY_CAP, SCALAR_CAP);
    CapturedBody(Arc::from(bytes.into_boxed_slice()))
}

/// Redact + cap request headers → a [`CapturedHeaders`] (sensitive names →
/// `"[redacted]"`; every other value image-URI-stripped then capped to
/// [`SCALAR_CAP]`; names capped too). The ONLY constructor of `CapturedHeaders`, so
/// the store can never be handed raw header pairs (D1 R2 #2).
pub fn redact_headers(headers: &axum::http::HeaderMap) -> CapturedHeaders {
    CapturedHeaders(crate::redaction::redact_headers_capped(headers, SCALAR_CAP))
}

/// Cap a single retained scalar string to [`SCALAR_CAP`] bytes on a UTF-8 char
/// boundary (D1 R1 #5 — every dynamic scalar the record keeps is bounded). When
/// over cap, the bounded prefix is COPIED into a FRESH `String` (D1 R2... R3 #2):
/// `String::truncate` keeps the ORIGINAL capacity, so an oversized input would
/// retain an unbounded backing allocation while the quota accounting only sees the
/// 4 KiB length. Reallocating makes retained capacity == bounded length.
fn cap_scalar(text: String) -> String {
    if text.len() <= SCALAR_CAP {
        return text;
    }
    let bytes = text.as_bytes();
    let mut end = SCALAR_CAP;
    while end > 0 && (bytes[end] & 0xC0) == 0x80 {
        end -= 1;
    }
    // `to_string()` on the slice allocates exactly `end` bytes — the original
    // (possibly huge) buffer is dropped, not retained behind a shrunk length.
    text[..end].to_string()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use axum::http::HeaderName;
    use axum::http::HeaderValue;

    /// Mint a `CapturedBody` from raw bytes via the real capture primitive (the only
    /// way to build one — that is the point of the newtype).
    fn cap(bytes: &[u8]) -> CapturedBody {
        capture_body(bytes)
    }

    /// An empty `CapturedHeaders` (minted via the real redactor).
    fn no_headers() -> CapturedHeaders {
        redact_headers(&HeaderMap::new())
    }

    fn open_simple(store: &DashboardFlowStore, api: &str) {
        store.open(
            api.to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            no_headers(),
            None,
            ClientAttribution::none(),
        );
    }

    fn shared_cut(store: &DashboardFlowStore) -> SnapshotSummaries {
        store.with_summaries_under_lock(|_guard, summaries, _seq| summaries)
    }

    #[test]
    fn shared_summaries_track_mutation_removal_and_body_shedding() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        open_simple(&store, "api_2");
        let first = shared_cut(&store);
        assert_eq!(
            first
                .iter()
                .map(|s| s.api_call_id.as_str())
                .collect::<Vec<_>>(),
            ["api_2", "api_1"],
            "newest first, like snapshot_summaries"
        );
        // Shared projection is identical to the owned one.
        assert_eq!(
            serde_json::to_value(&first).unwrap(),
            serde_json::to_value(store.snapshot_summaries()).unwrap()
        );
        assert_eq!(shared_cut(&store).alloc_id(), first.alloc_id());

        store.set_upstream("api_1", Some("t".to_string()), None, None);
        let second = shared_cut(&store);
        assert_eq!(second[0].alloc_id(), first[0].alloc_id(), "api_2 reused");
        assert_ne!(
            second[1].alloc_id(),
            first[1].alloc_id(),
            "api_1 re-projected"
        );
        assert_eq!(second[1].upstream_target.as_deref(), Some("t"));

        // TTL removal drops the flow AND its cache entry.
        store.force_started_ms("api_2", 0);
        store.prune_at(FLOW_TTL_MS + 100);
        let third = shared_cut(&store);
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].api_call_id, "api_1");
        assert!(!store.lock().summary_cache.contains_key("api_2"));
    }

    #[test]
    fn disabled_store_is_a_no_op() {
        let store = DashboardFlowStore::disabled();
        assert!(!store.is_enabled());
        store.open(
            "api_1".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            no_headers(),
            Some(cap(b"{}")),
            ClientAttribution::none(),
        );
        store.link("resp_1".to_string(), "api_1".to_string());
        store.record_usage("api_1", FlowUsage::default());
        store.finalize("api_1", FlowStatus::Completed, None, None);
        assert!(
            store.middleware_guard("api_1").is_none(),
            "disabled store mints no L0 guard"
        );
        assert!(store.list().is_empty(), "disabled store records nothing");
        assert!(store.detail("api_1").is_none());
        assert!(store.snapshot_summaries().is_empty());
    }

    #[test]
    fn metadata_only_flow_retains_identity_without_request_bodies() {
        let store = DashboardFlowStore::new();
        store.open_with_session_capture(
            "api_metadata_only".into(),
            "POST".into(),
            "/v1/responses".into(),
            no_headers(),
            None,
            ClientAttribution::none(),
            FlowSessionFacts::default(),
            false,
        );
        store.set_normalized(
            "api_metadata_only",
            Some("model-a".into()),
            Some(cap(br#"{"input":"secret"}"#)),
        );
        store.set_upstream(
            "api_metadata_only",
            Some("provider-a".into()),
            Some("model-a".into()),
            Some(cap(br#"{"messages":["secret"]}"#)),
        );

        let record = store.detail("api_metadata_only").unwrap();
        assert_eq!(record.model_requested.as_deref(), Some("model-a"));
        assert_eq!(record.model_served.as_deref(), Some("model-a"));
        assert_eq!(record.upstream_target.as_deref(), Some("provider-a"));
        assert!(record.inbound_body.is_none());
        assert!(record.normalized.is_none());
        assert!(record.upstream_body.is_none());
    }

    #[test]
    fn open_then_list_and_detail_round_trip() {
        let store = DashboardFlowStore::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/json"),
        );
        store.open(
            "api_1".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            redact_headers(&headers),
            Some(cap(b"{\"model\":\"m\"}")),
            ClientAttribution::none(),
        );
        let records = store.list();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].api_call_id, "api_1");
        assert_eq!(records[0].status, FlowStatus::Open);
        assert_eq!(
            records[0].claim.load(std::sync::atomic::Ordering::SeqCst),
            CLAIM_OPEN_L0,
            "claim allocated OpenL0"
        );
        assert!(store.detail("api_1").is_some());
        assert!(store.detail("nope").is_none());
    }

    /// D7b R2 finding 1: `detail_with_seq` returns the RESOLVED RECORD'S OWN mutation
    /// watermark (`record_seq`), NOT the live global `state.seq`. After flow A is
    /// finalized, MANY unrelated flows mutate the store and advance the global counter
    /// far past A's last mutation — yet `detail_with_seq("api_a")` still returns A's own
    /// (smaller) `record_seq`, so a delayed A frame cannot leapfrog those unrelated
    /// mutations and dedup-drop a genuinely newer flow frame.
    #[test]
    fn detail_with_seq_returns_record_seq_not_global_seq() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_a");
        store.link("resp_a".to_string(), "api_a".to_string());
        store.finalize("api_a", FlowStatus::Completed, None, None);
        // A's record watermark right after its last mutation.
        let (_record_a, seq_a) = store.detail_with_seq("api_a").expect("flow A resolves");
        // The record's own field matches the returned seq.
        assert_eq!(
            _record_a.record_seq, seq_a,
            "returned seq IS the record's own record_seq"
        );

        // Many unrelated flows mutate, bumping the GLOBAL counter past A's watermark.
        for i in 0..10 {
            open_simple(&store, &format!("api_other_{i}"));
        }
        let global_now = store.flow_seq();
        assert!(
            global_now > seq_a,
            "global cursor advanced past A's record_seq (global={global_now}, seq_a={seq_a})"
        );

        // detail_with_seq STILL returns A's own (smaller) record_seq, NOT the global.
        let (_again, seq_a_again) = store
            .detail_with_seq("api_a")
            .expect("flow A still resolves");
        assert_eq!(
            seq_a_again, seq_a,
            "record_seq is stable across unrelated mutations"
        );
        assert!(
            seq_a_again < global_now,
            "record_seq does NOT leapfrog to the global cursor"
        );
        // Resolving by the linked response_id returns the SAME record_seq (the join path
        // must not re-read the global seq either).
        let (_by_resp, seq_by_resp) = store
            .detail_with_seq("resp_a")
            .expect("flow A resolves by response_id");
        assert_eq!(
            seq_by_resp, seq_a,
            "response_id join returns the record's own seq"
        );
    }

    /// D7b R2 finding 1: a record's `record_seq` advances ONLY when THAT record mutates,
    /// and each of its own mutations strictly increases it (so a live frame after a new
    /// mutation always exceeds the snapshot baseline and is accepted).
    #[test]
    fn record_seq_advances_only_on_own_mutation() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_a");
        let (_r0, seq_open) = store.detail_with_seq("api_a").unwrap();

        // An unrelated flow mutates — A's record_seq must NOT move.
        open_simple(&store, "api_b");
        let (_r1, seq_after_unrelated) = store.detail_with_seq("api_a").unwrap();
        assert_eq!(
            seq_after_unrelated, seq_open,
            "an unrelated flow's mutation does not advance A's record_seq"
        );

        // A mutates again (finalize) — its record_seq strictly increases.
        store.finalize("api_a", FlowStatus::Completed, None, None);
        let (_r2, seq_after_own) = store.detail_with_seq("api_a").unwrap();
        assert!(
            seq_after_own > seq_open,
            "A's own mutation strictly advances its record_seq ({seq_after_own} > {seq_open})"
        );
    }

    /// D7b R2 finding 2: `snapshot_summaries_with_seq` captures the body-free summaries
    /// AND the FlowStore domain `seq` in ONE lock hold, so the seq is exactly the
    /// watermark of the summaries returned — never a torn (older-body, newer-cursor)
    /// pair. The captured seq equals the global `flow_seq()` taken immediately after
    /// (no mutation in between), and EVERY summary's own `record_seq` is ≤ the captured
    /// baseline, so a later live frame (strictly newer record_seq) is always accepted.
    #[test]
    fn snapshot_summaries_with_seq_is_atomic_and_consistent() {
        let store = DashboardFlowStore::new();
        for i in 0..3 {
            open_simple(&store, &format!("api_{i}"));
        }
        store.finalize("api_1", FlowStatus::Completed, None, None);

        let (summaries, seq) = store.snapshot_summaries_with_seq();
        assert_eq!(
            summaries.len(),
            3,
            "all three flows present in the snapshot"
        );
        // The atomically-captured seq matches the live global cursor (nothing mutated
        // between the atomic read and this read).
        assert_eq!(
            seq,
            store.flow_seq(),
            "captured seq is the live FlowStore cursor"
        );
        // Every record's own watermark is ≤ the baseline → a strictly-newer live frame
        // for any of them is accepted by the client's `seq > last_seq[flow]` dedup.
        for s in &summaries {
            let (_rec, record_seq) = store.detail_with_seq(&s.api_call_id).unwrap();
            assert!(
                record_seq <= seq,
                "record_seq {record_seq} must be <= the snapshot baseline {seq} ({})",
                s.api_call_id
            );
        }
    }

    /// The disabled store yields the empty/zero pair (mirrors `snapshot_summaries`).
    #[test]
    fn snapshot_summaries_with_seq_disabled_is_empty_zero() {
        let store = DashboardFlowStore::disabled();
        let (summaries, seq) = store.snapshot_summaries_with_seq();
        assert!(summaries.is_empty());
        assert_eq!(seq, 0);
    }

    #[test]
    fn list_is_newest_first() {
        let store = DashboardFlowStore::new();
        for i in 0..3 {
            open_simple(&store, &format!("api_{i}"));
        }
        let ids: Vec<String> = store
            .list()
            .iter()
            .map(|record| record.api_call_id.clone())
            .collect();
        assert_eq!(ids, vec!["api_2", "api_1", "api_0"]);
    }

    #[test]
    fn link_fires_once_and_detail_joins_by_either_id() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        assert!(store.detail("api_1").is_some());
        assert!(store.detail("resp_1").is_none());

        store.link("resp_1".to_string(), "api_1".to_string());
        // A second link must NOT overwrite the response_id (first-link-wins).
        store.link("resp_other".to_string(), "api_1".to_string());

        let record = store.detail("api_1").expect("record");
        assert_eq!(
            record.response_id.as_deref(),
            Some("resp_1"),
            "link fires once; the first response_id wins"
        );
        let by_resp = store.detail("resp_1").expect("join by response_id");
        assert_eq!(by_resp.api_call_id, "api_1");
        // The aliasing response_id was never indexed.
        assert!(
            store.detail("resp_other").is_none(),
            "second link did not create an alias"
        );
    }

    #[test]
    fn link_unknown_id_is_a_no_op_and_leaks_no_index() {
        let store = DashboardFlowStore::new();
        // No record opened for api_404.
        store.link("resp_x".to_string(), "api_404".to_string());
        assert!(store.detail("resp_x").is_none(), "no index entry leaked");
        assert!(store.detail("api_404").is_none());
        assert!(store.list().is_empty());
    }

    #[test]
    fn link_double_call_is_idempotent() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.link("resp_1".to_string(), "api_1".to_string());
        store.link("resp_1".to_string(), "api_1".to_string());
        let record = store.detail("api_1").expect("record");
        assert_eq!(record.response_id.as_deref(), Some("resp_1"));
        assert!(store.detail("resp_1").is_some());
    }

    #[test]
    fn concurrent_flows_link_correctly() {
        let store = Arc::new(DashboardFlowStore::new());
        let mut handles = Vec::new();
        for i in 0..16 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                let api = format!("api_{i}");
                let resp = format!("resp_{i}");
                open_simple(&store, &api);
                store.link(resp, api);
            }));
        }
        for handle in handles {
            handle.join().expect("thread");
        }
        for i in 0..16 {
            let record = store
                .detail(&format!("resp_{i}"))
                .expect("each response_id joins to its own flow");
            assert_eq!(record.api_call_id, format!("api_{i}"));
            assert_eq!(
                record.response_id.as_deref(),
                Some(format!("resp_{i}").as_str())
            );
        }
    }

    #[test]
    fn mutators_resolve_by_response_id_via_link_index() {
        // D2 needs the id-keyed mutators (here `set_upstream`/`set_normalized`) to
        // accept the flow's `response_id` — the leaf only knows that — and join it to
        // the owning `api_call_id` via the link index, exactly like `detail`.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.link("resp_1".to_string(), "api_1".to_string());
        // Drive the mutators by RESPONSE_ID, not api_call_id.
        store.set_upstream(
            "resp_1",
            Some("https://upstream".to_string()),
            Some("served-m".to_string()),
            Some(cap(b"{\"on\":\"wire\"}")),
        );
        store.set_normalized("resp_1", Some("requested-m".to_string()), None);
        let record = store.detail("api_1").expect("record");
        assert_eq!(record.upstream_target.as_deref(), Some("https://upstream"));
        assert_eq!(record.model_served.as_deref(), Some("served-m"));
        assert_eq!(record.model_requested.as_deref(), Some("requested-m"));
        let body = record.upstream_body.as_ref().expect("upstream body set");
        assert_eq!(&**body, b"{\"on\":\"wire\"}");
        // An unknown id still no-ops (no panic, no record).
        store.set_upstream("resp_unknown", Some("x".to_string()), None, None);
        assert!(store.detail("resp_unknown").is_none());
    }

    #[test]
    fn finalize_stamps_terminal_fields() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.record_usage(
            "api_1",
            FlowUsage {
                prompt: 10,
                completion: 5,
                total: 15,
                cached: Some(0),
                reasoning: Some(2),
            },
        );
        store.finalize(
            "api_1",
            FlowStatus::Completed,
            Some("response.completed".to_string()),
            None,
        );
        let record = store.detail("api_1").expect("record");
        assert_eq!(record.status, FlowStatus::Completed);
        assert!(record.finished_ms.is_some());
        assert!(record.elapsed_ms.is_some());
        assert_eq!(
            record.terminal_reason.as_deref(),
            Some("response.completed")
        );
        assert_eq!(record.usage.expect("usage").total, 15);
    }

    use std::sync::atomic::Ordering::SeqCst;

    fn serving() -> Arc<crate::upstream::ServingToken> {
        Arc::new(crate::upstream::ServingToken::default())
    }

    #[test]
    fn d6_guard_registers_token_on_claim_and_removes_on_finalize() {
        // The L1 guard REGISTERS its kill token in the AbortHub at claim time and
        // REMOVES it on an explicit finalize — so a Completed flow leaks no entry.
        let store = DashboardFlowStore::new();
        let hub = AbortHub::new();
        open_simple(&store, "api_1");
        assert_eq!(hub.live_len(), 0, "no token before claim");

        let guard = store.engine_guard("api_1", serving(), &hub).expect("claim");
        assert_eq!(hub.live_len(), 1, "token registered at claim");
        assert!(
            !guard.abort_token().is_cancelled(),
            "token starts uncancelled"
        );

        guard.finalize(FlowStatus::Completed, Some("done".to_string()));
        assert_eq!(
            hub.live_len(),
            0,
            "token removed on explicit finalize (no leak)"
        );
    }

    #[test]
    fn d6_guard_removes_token_on_drop_fallback() {
        // A missed-exit Drop (cancel/panic) also removes the AbortHub entry via the
        // SAME CAS-winning finalize path — no leak even without an explicit finalize.
        let store = DashboardFlowStore::new();
        let hub = AbortHub::new();
        open_simple(&store, "api_1");
        {
            let _guard = store.engine_guard("api_1", serving(), &hub).expect("claim");
            assert_eq!(hub.live_len(), 1);
        } // Drop with no explicit finalize.
        assert_eq!(
            hub.live_len(),
            0,
            "Drop fallback removed the token (no leak)"
        );
    }

    #[test]
    fn d6_abort_cancels_live_token_and_reports_found() {
        // abort() flips the live flow's token and reports `true`; the guard's clone of
        // the SAME token observes the cancellation (shared Arc), so the engine's
        // compose sites will surface cancelled().
        let store = DashboardFlowStore::new();
        let hub = AbortHub::new();
        open_simple(&store, "api_1");
        let guard = store.engine_guard("api_1", serving(), &hub).expect("claim");
        let token = guard.abort_token();

        assert!(hub.abort("api_1"), "live token found + cancelled");
        assert!(token.is_cancelled(), "the guard's token observes the kill");

        // Idempotent: a second abort of the still-registered flow re-cancels (still
        // true) without panicking — removal stays the guard's job.
        assert!(
            hub.abort("api_1"),
            "double-kill is idempotent while registered"
        );
    }

    #[test]
    fn d6_abort_unknown_or_finished_id_reports_not_found() {
        let store = DashboardFlowStore::new();
        let hub = AbortHub::new();
        // Unknown id (never registered).
        assert!(!hub.abort("nope"), "unknown id → false");

        open_simple(&store, "api_1");
        let guard = store.engine_guard("api_1", serving(), &hub).expect("claim");
        guard.finalize(FlowStatus::Completed, None); // removes the entry
        assert!(
            !hub.abort("api_1"),
            "already-finished id → false (entry already removed)"
        );
    }

    #[test]
    fn d6_disabled_hub_is_a_noop() {
        // A disabled hub (debug UI off) never registers and abort is always false, so
        // production keeps zero overhead.
        let hub = AbortHub::disabled();
        hub.register("api_1", CancellationToken::new());
        assert_eq!(hub.live_len(), 0, "disabled hub stores nothing");
        assert!(!hub.abort("api_1"), "disabled hub abort is always false");
    }

    #[test]
    fn l0_guard_finalizes_orphan_left_open() {
        // The L0 middleware guard's Drop finalizes a record that was opened but
        // NEVER claimed by the engine (extractor/JSON rejection) — no orphan Open.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        {
            let _guard = store.middleware_guard("api_1").expect("guard minted");
            // While held, the record is still Open (nothing claimed it).
            assert_eq!(store.detail("api_1").unwrap().status, FlowStatus::Open);
        } // Drop here.
        let record = store.detail("api_1").expect("record survives");
        assert_eq!(
            record.status,
            FlowStatus::Failed,
            "L0 Drop finalized the orphan Failed"
        );
        assert_eq!(record.terminal_reason.as_deref(), Some("unhandled"));
        assert_eq!(
            record.claim.load(SeqCst),
            CLAIM_FINALIZED,
            "claim CASed OpenL0 → Finalized"
        );
    }

    #[test]
    fn l1_claim_disarms_l0_guard() {
        // When the engine claims the record (ClaimedL1) the L0 Drop is inert: its
        // CAS(OpenL0 → Finalized) fails, so L1 owns finalization. No transition to
        // Failed-unhandled occurs.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        let l0 = store.middleware_guard("api_1").expect("L0 guard");
        let l1 = store
            .engine_guard("api_1", serving(), &AbortHub::new())
            .expect("L1 claim wins");
        assert_eq!(
            store.detail("api_1").unwrap().claim.load(SeqCst),
            CLAIM_CLAIMED_L1
        );
        // A SECOND engine guard cannot claim (CAS already lost OpenL0).
        assert!(
            store
                .engine_guard("api_1", serving(), &AbortHub::new())
                .is_none(),
            "second L1 claim loses the CAS"
        );
        drop(l0); // inert — record is ClaimedL1, not OpenL0.
        assert_eq!(
            store.detail("api_1").unwrap().status,
            FlowStatus::Open,
            "L0 Drop did NOT finalize a claimed record"
        );
        l1.finalize(
            FlowStatus::Completed,
            Some("response.completed".to_string()),
        );
        assert_eq!(store.detail("api_1").unwrap().status, FlowStatus::Completed);
    }

    #[test]
    fn l1_finalize_is_idempotent_and_drop_no_ops() {
        // Explicit finalize wins; the Drop fallback then no-ops (does NOT overwrite
        // Completed with the Cancelled fallback).
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        let guard = store
            .engine_guard("api_1", serving(), &AbortHub::new())
            .expect("claim");
        guard.finalize(
            FlowStatus::Completed,
            Some("response.completed".to_string()),
        );
        // A second explicit finalize no-ops (CAS already Finalized).
        guard.finalize(FlowStatus::Failed, Some("late".to_string()));
        assert_eq!(store.detail("api_1").unwrap().status, FlowStatus::Completed);
        drop(guard); // Drop fallback must NOT flip it to Cancelled.
        let record = store.detail("api_1").expect("record");
        assert_eq!(record.status, FlowStatus::Completed);
        assert_eq!(
            record.terminal_reason.as_deref(),
            Some("response.completed")
        );
        assert_eq!(record.claim.load(SeqCst), CLAIM_FINALIZED);
    }

    #[test]
    fn l1_drop_without_explicit_finalize_records_cancelled() {
        // A missed exit path (cancel/panic) → Drop finalizes Cancelled, never an
        // orphan stuck ClaimedL1.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        {
            let _guard = store
                .engine_guard("api_1", serving(), &AbortHub::new())
                .expect("claim");
            assert_eq!(store.detail("api_1").unwrap().status, FlowStatus::Open);
        } // Drop with no explicit finalize.
        let record = store.detail("api_1").expect("record");
        assert_eq!(record.status, FlowStatus::Cancelled);
        assert_eq!(record.terminal_reason.as_deref(), Some("dropped"));
        assert_eq!(record.claim.load(SeqCst), CLAIM_FINALIZED);
    }

    #[test]
    fn l1_finalize_attributes_serving_provider_when_no_leaf_target() {
        // The serving provider is read from the ServingToken AT finalize (so a
        // provider tagged after the guard is built is still recorded), and lands in
        // upstream_target only when the leaf left it empty.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        let token = serving();
        let guard = store
            .engine_guard("api_1", Arc::clone(&token), &AbortHub::new())
            .expect("claim");
        // Provider tagged AFTER the guard exists (mirrors failover first-chunk).
        token.set_provider("backend-b");
        guard.finalize(FlowStatus::Completed, None);
        assert_eq!(
            store.detail("api_1").unwrap().upstream_target.as_deref(),
            Some("backend-b"),
            "serving provider recorded as the target when the leaf set none"
        );
    }

    #[test]
    fn l1_finalize_does_not_clobber_leaf_upstream_target() {
        // When the leaf already captured an upstream URL, the serving-provider
        // attribution must NOT overwrite it.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.set_upstream("api_1", Some("https://real-url".to_string()), None, None);
        let token = serving();
        token.set_provider("backend-b");
        let guard = store
            .engine_guard("api_1", token, &AbortHub::new())
            .expect("claim");
        guard.finalize(FlowStatus::Completed, None);
        assert_eq!(
            store.detail("api_1").unwrap().upstream_target.as_deref(),
            Some("https://real-url"),
            "leaf URL wins over serving-provider fallback"
        );
    }

    #[test]
    fn terminal_metrics_survive_record_eviction_before_finalize() {
        // D5 R3 (MEDIUM): the authoritative metrics layer must NOT undercount a
        // completed request when the FlowStore prunes/evicts the record BEFORE the flow
        // finalizes (a long-running / high-concurrency flow can age past the TTL or be
        // pushed out by the count cap). The L1 guard captures `endpoint` at claim and
        // reads `model_served`/`upstream`/`usage` off the shared ServingToken at
        // finalize — never re-reading the (now-gone) record — so its terminal metrics
        // inputs are fully intact and the request is still counted. Assert it for BOTH
        // eviction mechanisms (TTL prune AND count-cap eviction).
        for evict_via_ttl in [true, false] {
            let store = DashboardFlowStore::new();
            open_simple(&store, "api_1");
            let token = serving();
            // The engine's run_turn sets these on the token (resolved model + upserted
            // usage); the failover/routing layer sets the provider.
            token.set_model_served("served-m");
            token.set_provider("backend-b");
            token.set_usage(FlowUsage {
                prompt: 100,
                completion: 40,
                total: 140,
                cached: Some(10),
                reasoning: Some(7),
            });
            let guard = store
                .engine_guard("api_1", Arc::clone(&token), &AbortHub::new())
                .expect("claim");

            // EVICT the record BEFORE finalize — the whole record is gone either way.
            if evict_via_ttl {
                // Age the record past the TTL, then prune at a clock beyond its window.
                store.force_started_ms("api_1", 0);
                store.prune_at(FLOW_TTL_MS + 100);
            } else {
                // Push past the count cap so the oldest (only) record is evicted.
                for index in 0..(FLOW_CAP + 1) {
                    open_simple(&store, &format!("filler_{index}"));
                }
            }
            assert!(
                store.detail("api_1").is_none(),
                "the flow record is evicted before finalize (evict_via_ttl={evict_via_ttl})"
            );

            // Finalize AFTER the record is gone: the guard sources its metrics inputs
            // from its own captured endpoint + the ServingToken, so they are intact.
            guard.finalize(FlowStatus::Completed, None);
            let inputs = guard
                .terminal_metrics()
                .expect("guard carries terminal metrics despite record eviction");
            assert_eq!(inputs.model_served.as_deref(), Some("served-m"));
            assert_eq!(inputs.endpoint, "/v1/responses");
            assert_eq!(inputs.upstream.as_deref(), Some("backend-b"));
            assert_eq!(
                inputs.usage,
                Some(FlowUsage {
                    prompt: 100,
                    completion: 40,
                    total: 140,
                    cached: Some(10),
                    reasoning: Some(7),
                })
            );

            // Feed the guard's inputs into the metrics layer exactly as the engine's
            // `record_terminal_metrics` does, and assert the completed request IS
            // counted (no undercount) with the real served model — never the "unknown"
            // sentinel a None-out `detail()` re-read would have produced.
            let metrics = crate::metrics::MetricsLayer::new();
            metrics.record_terminal(
                FlowStatus::Completed,
                inputs.model_served.as_deref(),
                &inputs.endpoint,
                inputs.upstream.as_deref(),
                guard.elapsed().as_millis(),
                inputs.usage,
                &[],
            );
            let view = metrics.view();
            assert_eq!(
                view.window_1m.total_count(),
                1,
                "the evicted-then-finalized request is still counted (evict_via_ttl={evict_via_ttl})"
            );
            let (key, counts) = view
                .window_1m
                .buckets
                .iter()
                .next()
                .expect("one metrics bucket");
            assert_eq!(
                key.model, "served-m",
                "served model, not the unknown sentinel"
            );
            assert_eq!(key.upstream, "backend-b");
            assert_eq!(counts.prompt_tokens, 100);
            assert_eq!(counts.completion_tokens, 40);
        }
    }

    /// Gap 12 acceptance: a flow whose FlowStore record is EVICTED before finalize still
    /// counts its FAILED-PRIMARY provider in the per-provider metrics. The attempt trace
    /// rides the SAME evict-safe terminal payload as `usage`/`upstream` (read off the
    /// shared ServingToken at finalize, never the gone record), so per-provider aggregation
    /// cannot undercount an unhealthy provider just because the flow aged out / was capped.
    #[test]
    fn per_provider_metrics_survive_record_eviction_before_finalize() {
        for evict_via_ttl in [true, false] {
            let store = DashboardFlowStore::new();
            open_simple(&store, "api_1");
            let token = serving();
            token.set_model_served("served-m");
            token.set_provider("backend-b");
            // The failover layer records BOTH attempts on the token as it tries them: a
            // failed primary A, then a served B. These ride the token, not the record.
            token.record_attempt(failed_attempt("backend-a", AttemptErrorClass::HttpStatus));
            token.record_attempt(served_attempt("backend-b", Some(200)));
            let guard = store
                .engine_guard("api_1", Arc::clone(&token), &AbortHub::new())
                .expect("claim");

            // EVICT the record before finalize (both mechanisms).
            if evict_via_ttl {
                store.force_started_ms("api_1", 0);
                store.prune_at(FLOW_TTL_MS + 100);
            } else {
                for index in 0..(FLOW_CAP + 1) {
                    open_simple(&store, &format!("filler_{index}"));
                }
            }
            assert!(
                store.detail("api_1").is_none(),
                "record evicted before finalize"
            );

            guard.finalize(FlowStatus::Completed, None);
            let inputs = guard.terminal_metrics().expect("terminal payload intact");
            // The evict-safe payload carried BOTH attempts despite the gone record.
            assert_eq!(inputs.attempts.len(), 2, "both attempts survive eviction");

            let metrics = crate::metrics::MetricsLayer::new();
            metrics.record_terminal(
                FlowStatus::Completed,
                inputs.model_served.as_deref(),
                &inputs.endpoint,
                inputs.upstream.as_deref(),
                guard.elapsed().as_millis(),
                inputs.usage,
                &inputs.attempts,
            );
            // The FAILED PRIMARY is counted toward its own provider (backend-a), not lost
            // behind the served backend-b — even though the record was evicted.
            let view = metrics.view();
            let primary = view
                .window_1m
                .provider_latency("backend-a")
                .expect("the failed primary is counted despite record eviction");
            assert_eq!(primary.failed, 1, "evict_via_ttl={evict_via_ttl}");
            assert_eq!(primary.error_rate, 100.0);
            assert_eq!(primary.errors.http_status, 1);
            // The server is also tracked, as a served sample.
            let served = view.window_1m.provider_latency("backend-b").unwrap();
            assert_eq!(served.served, 1);
            assert_eq!(served.error_rate, 0.0);
        }
    }

    // ---- Gap 03: `attempts[]` + `first_upstream_byte_ms` ----

    fn served_attempt(provider: &str, byte_ms: Option<u128>) -> Attempt {
        Attempt {
            provider: Some(provider.to_string()),
            model: Some("served-m".to_string()),
            start_ms: 100,
            end_ms: 200,
            first_upstream_byte_ms: byte_ms,
            status: AttemptStatus::Served,
            error_class: None,
            failover_reason: None,
        }
    }

    fn failed_attempt(provider: &str, class: AttemptErrorClass) -> Attempt {
        Attempt {
            provider: Some(provider.to_string()),
            model: Some("m".to_string()),
            start_ms: 10,
            end_ms: 50,
            first_upstream_byte_ms: None,
            status: AttemptStatus::Failed,
            error_class: Some(class),
            failover_reason: Some(AttemptFailoverReason::ProviderFailed),
        }
    }

    /// `record_attempts` threads the trace + flow-level wire-first-byte onto the record;
    /// the body-free summary projects them. A non-failover flow has exactly one attempt.
    #[test]
    fn record_attempts_threads_onto_record_and_summary() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.record_attempts(
            "api_1",
            vec![served_attempt("primary", Some(150))],
            Some(150),
        );

        let record = store.detail("api_1").expect("record");
        assert_eq!(
            record.attempts.len(),
            1,
            "single-success flow has exactly one attempt"
        );
        assert_eq!(record.attempts[0].status, AttemptStatus::Served);
        assert_eq!(record.first_upstream_byte_ms, Some(150));

        let summary = store
            .snapshot_summaries()
            .into_iter()
            .find(|s| s.api_call_id == "api_1")
            .expect("summary");
        assert_eq!(summary.attempts.len(), 1);
        assert_eq!(summary.first_upstream_byte_ms, Some(150));
    }

    /// A forced-failover trace (failed then served) threads ≥2 attempts onto the record;
    /// the flow-level first byte is the SERVED attempt's; first-write-wins on re-finalize.
    #[test]
    fn record_attempts_failover_trace_and_first_byte_first_write_wins() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        let attempts = vec![
            failed_attempt("primary", AttemptErrorClass::HttpStatus),
            served_attempt("backup", Some(220)),
        ];
        store.record_attempts("api_1", attempts, Some(220));
        let record = store.detail("api_1").expect("record");
        assert_eq!(record.attempts.len(), 2, "failed provider + served one");
        assert_eq!(record.attempts[0].status, AttemptStatus::Failed);
        assert_eq!(
            record.attempts[0].failover_reason,
            Some(AttemptFailoverReason::ProviderFailed)
        );
        assert!(record.attempts[0].first_upstream_byte_ms.is_none());
        assert_eq!(record.attempts[1].status, AttemptStatus::Served);
        assert_eq!(record.first_upstream_byte_ms, Some(220));

        // A later finalize with no byte time cannot clobber the measured value.
        store.record_attempts("api_1", Vec::new(), None);
        assert_eq!(
            store.detail("api_1").unwrap().first_upstream_byte_ms,
            Some(220),
            "first_upstream_byte_ms is first-write-wins"
        );
    }

    /// don't-lie-with-zeros: a flow where no upstream byte arrived has
    /// `first_upstream_byte_ms == None`, which is ABSENT on the wire (never `0`).
    #[test]
    fn record_attempts_no_upstream_byte_is_none_and_absent() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.record_attempts(
            "api_1",
            vec![failed_attempt("primary", AttemptErrorClass::Connect)],
            None,
        );
        let record = store.detail("api_1").expect("record");
        assert!(record.first_upstream_byte_ms.is_none());

        let summary = SnapshotFlowSummary::from_record(&record);
        let value = serde_json::to_value(&summary).expect("serialize");
        let obj = value.as_object().expect("object");
        assert!(
            !obj.contains_key("first_upstream_byte_ms"),
            "an unmeasured first byte is ABSENT, never 0"
        );
        // The attempt's own unmeasured first-byte is likewise absent, but a failed
        // attempt DOES carry its bounded taxonomic codes.
        let attempt0 = obj["attempts"][0].as_object().expect("attempt object");
        assert!(!attempt0.contains_key("first_upstream_byte_ms"));
        assert_eq!(attempt0["error_class"], serde_json::json!("connect"));
        assert_eq!(
            attempt0["failover_reason"],
            serde_json::json!("provider_failed")
        );
    }

    /// The body-free summary survives a deserialize→serialize round-trip carrying the
    /// attempt trace + flow-level first byte, and `error_class`/`failover_reason` are the
    /// BOUNDED taxonomic snake_case codes — NOT raw upstream text (raw bodies stay
    /// spec-05-gated). Proves the new wire fields round-trip (AGENTS.md no-new-wire-field
    /// -without-round-trip rule).
    #[test]
    fn snapshot_summary_attempts_round_trip_and_bounded_codes() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.record_attempts(
            "api_1",
            vec![
                failed_attempt("primary", AttemptErrorClass::Timeout),
                served_attempt("backup", Some(220)),
            ],
            Some(220),
        );
        let record = store.detail("api_1").expect("record");
        let summary = SnapshotFlowSummary::from_record(&record);

        let value = serde_json::to_value(&summary).expect("serialize");
        // Bounded taxonomic codes on the wire — snake_case enum tags, not raw text.
        let json = value.to_string();
        assert!(json.contains("\"error_class\":\"timeout\""));
        assert!(json.contains("\"failover_reason\":\"provider_failed\""));
        assert!(json.contains("\"status\":\"served\""));
        assert!(json.contains("\"status\":\"failed\""));
        assert_eq!(value["first_upstream_byte_ms"].as_u64(), Some(220));

        // AGENTS.md: a NEW wire field needs a deserialize→serialize round-trip proving it
        // survives. `SnapshotFlowSummary` is Serialize-only (gap-02 precedent), so
        // round-trip the new `attempts[]` payload (`Attempt` derives both) out of the
        // serialized summary and back. The bounded enums survive as the same taxonomic
        // codes — no raw upstream text appears.
        let attempts_json = value["attempts"].to_string();
        let back: Vec<Attempt> =
            serde_json::from_str(&attempts_json).expect("deserialize attempts");
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].error_class, Some(AttemptErrorClass::Timeout));
        assert_eq!(
            back[0].failover_reason,
            Some(AttemptFailoverReason::ProviderFailed)
        );
        assert_eq!(back[0].status, AttemptStatus::Failed);
        assert!(back[0].first_upstream_byte_ms.is_none());
        assert_eq!(back[1].status, AttemptStatus::Served);
        assert_eq!(back[1].first_upstream_byte_ms, Some(220));
        // Re-serialize and confirm the round-trip is lossless (compare as `Value` so
        // struct-field order vs. `Value`'s sorted-key order is not a false mismatch).
        let reser: serde_json::Value = serde_json::to_value(&back).expect("re-serialize attempts");
        assert_eq!(
            reser, value["attempts"],
            "the attempts round-trip is lossless"
        );
    }

    /// F4 (round-1 reviews): a FULL-summary deserialize→serialize round-trip covering
    /// EVERY new optional wire field together — gap-03's TOP-LEVEL `first_upstream_byte_ms`
    /// plus the nested `attempts[]`, AND gap-04's `client_label` plus `client_source`. The
    /// original round-trip test only re-read the nested `attempts` sub-array, so a
    /// regression to a new top-level field's wire shape (or the gap-04 attribution fields)
    /// would have slipped through (AGENTS.md no-new-wire-field-without-round-trip rule).
    /// `SnapshotFlowSummary` is `Serialize`-only by design (gap-02 precedent), so this
    /// round-trips the serialized summary through an EQUIVALENT DTO that deserializes all
    /// new fields, then re-serializes and asserts each survives losslessly — PRESENT and
    /// ABSENT. The bounded taxonomic codes and the `client_source` enum survive as the
    /// same snake_case tags; the `client_label` is only ever a one-way `key-<hex>` prefix.
    #[test]
    fn snapshot_summary_full_dto_round_trip_covers_both_new_wire_fields() {
        /// Equivalent DTO covering EVERY new wire field (gap 03 + gap 04) plus enough
        /// sibling scalars to be a genuine full-summary round-trip (not just the attempts
        /// sub-array). Derives `Deserialize` (the production summary is `Serialize`-only)
        /// + `Serialize` so the re-serialization can be compared for losslessness.
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct RoundTripSummary {
            api_call_id: String,
            method: String,
            uri: String,
            // `FlowStatus` is `Serialize`-only; it serializes as a snake_case string, so a
            // `String` here deserializes and re-serializes byte-identically.
            status: String,
            started_ms: u128,
            // Gap 03 nested wire field.
            attempts: Vec<Attempt>,
            // Gap 03 top-level wire field — ABSENT when unmeasured (skip_serializing_if);
            // `default` so a missing key deserializes back to `None` (never `0`).
            #[serde(default, skip_serializing_if = "Option::is_none")]
            first_upstream_byte_ms: Option<u128>,
            // Gap 04 wire fields — ABSENT when the flow is unattributed; `default` so a
            // missing key deserializes back to `None` (never an empty-string-as-id).
            #[serde(default, skip_serializing_if = "Option::is_none")]
            client_label: Option<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            client_source: Option<ClientSource>,
        }

        // PRESENT case: a flow opened WITH a key-hash attribution carries both gap-04
        // fields. Derive the attribution from a bearer header so the label is a genuine
        // `key-<hex>` prefix (the raw key is hashed in `derive`, never stored).
        let store = DashboardFlowStore::new();
        let mut key_headers = HeaderMap::new();
        key_headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer sk-ROUNDTRIP-SECRET-key"),
        );
        let key_attr = ClientAttribution::derive(&key_headers, None);
        let expected_label = key_attr.label.clone().expect("key-hash label");
        store.open(
            "api_1".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            redact_headers(&key_headers),
            None,
            key_attr,
        );
        store.record_attempts(
            "api_1",
            vec![
                failed_attempt("primary", AttemptErrorClass::HttpStatus),
                served_attempt("backup", Some(321)),
            ],
            Some(321),
        );
        let record = store.detail("api_1").expect("record");
        let summary = SnapshotFlowSummary::from_record(&record);

        // Serialize the WHOLE production summary, then deserialize the WHOLE thing into the
        // equivalent DTO (every new field, top-level + nested, in ONE pass).
        let wire = serde_json::to_value(&summary).expect("serialize summary");
        // The raw key is never on the wire — only the one-way hash prefix.
        assert!(
            !wire.to_string().contains("ROUNDTRIP-SECRET"),
            "raw key absent from the summary wire: {wire}"
        );
        let back: RoundTripSummary =
            serde_json::from_value(wire.clone()).expect("deserialize full summary DTO");

        // Every new wire field survived deserialization.
        assert_eq!(
            back.first_upstream_byte_ms,
            Some(321),
            "top-level first_upstream_byte_ms survives the full-summary round-trip"
        );
        assert_eq!(back.attempts.len(), 2, "nested attempts[] survive");
        assert_eq!(back.attempts[0].status, AttemptStatus::Failed);
        assert_eq!(
            back.attempts[0].error_class,
            Some(AttemptErrorClass::HttpStatus)
        );
        assert!(back.attempts[0].first_upstream_byte_ms.is_none());
        assert_eq!(back.attempts[1].status, AttemptStatus::Served);
        assert_eq!(back.attempts[1].first_upstream_byte_ms, Some(321));
        // Gap 04: both attribution fields survive (label = the key-hash, source = KeyHash).
        assert_eq!(
            back.client_label.as_deref(),
            Some(expected_label.as_str()),
            "client_label survives the full-summary round-trip"
        );
        assert_eq!(
            back.client_source,
            Some(ClientSource::KeyHash),
            "client_source survives the full-summary round-trip"
        );

        // Re-serialize the DTO and confirm EVERY new field is byte-identical to the
        // production wire (compare as `Value` so key order is not a false mismatch).
        let reser = serde_json::to_value(&back).expect("re-serialize full summary DTO");
        assert_eq!(
            reser["first_upstream_byte_ms"], wire["first_upstream_byte_ms"],
            "top-level first_upstream_byte_ms round-trips losslessly"
        );
        assert_eq!(
            reser["attempts"], wire["attempts"],
            "nested attempts[] round-trips losslessly"
        );
        assert_eq!(
            reser["client_label"], wire["client_label"],
            "client_label round-trips losslessly"
        );
        assert_eq!(
            reser["client_source"], wire["client_source"],
            "client_source round-trips losslessly"
        );

        // ABSENT case: an unattributed flow with NO upstream byte omits ALL the optional
        // top-level fields on the wire, and the DTO deserializes them back to `None`
        // (never `0`/empty-string-as-id).
        open_simple(&store, "api_2");
        store.record_attempts(
            "api_2",
            vec![failed_attempt("primary", AttemptErrorClass::Connect)],
            None,
        );
        let record2 = store.detail("api_2").expect("record");
        let summary2 = SnapshotFlowSummary::from_record(&record2);
        let wire2 = serde_json::to_value(&summary2).expect("serialize summary2");
        let obj2 = wire2.as_object().unwrap();
        assert!(
            !obj2.contains_key("first_upstream_byte_ms"),
            "an unmeasured top-level first byte is ABSENT on the wire (never 0)"
        );
        assert!(
            !obj2.contains_key("client_label") && !obj2.contains_key("client_source"),
            "an unattributed flow omits both gap-04 fields on the wire: {wire2}"
        );
        let back2: RoundTripSummary =
            serde_json::from_value(wire2).expect("deserialize summary2 DTO");
        assert!(
            back2.first_upstream_byte_ms.is_none(),
            "absent top-level field deserializes to None"
        );
        assert!(
            back2.client_label.is_none() && back2.client_source.is_none(),
            "absent gap-04 fields deserialize to None"
        );
    }

    /// The L1 guard threads the attempt trace from the shared `ServingToken` into BOTH the
    /// FlowStore record AND the evict-safe terminal metrics payload at finalize.
    #[test]
    fn guard_finalize_threads_attempts_into_record_and_terminal_payload() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        let token = serving();
        // The failover/leaf layers push attempts onto the token before finalize.
        token.record_attempt(failed_attempt("primary", AttemptErrorClass::HttpStatus));
        token.record_attempt(served_attempt("backup", Some(220)));
        let guard = store
            .engine_guard("api_1", Arc::clone(&token), &AbortHub::new())
            .expect("claim");
        guard.finalize(FlowStatus::Completed, None);

        // (1) The record carries the full trace + the served attempt's wire first-byte.
        let record = store.detail("api_1").expect("record");
        assert_eq!(record.attempts.len(), 2);
        assert_eq!(record.attempts[0].status, AttemptStatus::Failed);
        assert_eq!(record.attempts[1].status, AttemptStatus::Served);
        assert_eq!(record.first_upstream_byte_ms, Some(220));

        // (2) The evict-safe terminal payload carries the SAME attempts (spec 12's source).
        let inputs = guard.terminal_metrics().expect("terminal metrics");
        assert_eq!(inputs.attempts.len(), 2);
        assert_eq!(inputs.attempts[0].provider.as_deref(), Some("primary"));
        assert_eq!(inputs.attempts[1].provider.as_deref(), Some("backup"));
    }

    /// Evict-safe acceptance: when the FlowStore record is EVICTED before finalize, the
    /// terminal payload STILL carries ALL attempts (spec 12 aggregates from it without
    /// re-reading the gone record). Asserted for both eviction mechanisms.
    #[test]
    fn attempts_survive_record_eviction_before_finalize() {
        for evict_via_ttl in [true, false] {
            let store = DashboardFlowStore::new();
            open_simple(&store, "api_1");
            let token = serving();
            token.record_attempt(failed_attempt("primary", AttemptErrorClass::Timeout));
            token.record_attempt(served_attempt("backup", Some(220)));
            let guard = store
                .engine_guard("api_1", Arc::clone(&token), &AbortHub::new())
                .expect("claim");

            if evict_via_ttl {
                store.force_started_ms("api_1", 0);
                store.prune_at(FLOW_TTL_MS + 100);
            } else {
                for index in 0..(FLOW_CAP + 1) {
                    open_simple(&store, &format!("filler_{index}"));
                }
            }
            assert!(
                store.detail("api_1").is_none(),
                "record evicted before finalize (evict_via_ttl={evict_via_ttl})"
            );

            guard.finalize(FlowStatus::Completed, None);
            let inputs = guard
                .terminal_metrics()
                .expect("terminal payload survives record eviction");
            assert_eq!(
                inputs.attempts.len(),
                2,
                "ALL attempts ride the terminal payload despite eviction (evict_via_ttl={evict_via_ttl})"
            );
            assert_eq!(inputs.attempts[1].status, AttemptStatus::Served);
            assert_eq!(inputs.attempts[1].first_upstream_byte_ms, Some(220));
        }
    }

    #[test]
    fn disabled_store_mints_no_guards() {
        let store = DashboardFlowStore::disabled();
        assert!(store.middleware_guard("api_1").is_none());
        assert!(
            store
                .engine_guard("api_1", serving(), &AbortHub::new())
                .is_none()
        );
    }

    #[test]
    fn guards_are_send() {
        // The L1 guard must be Send to cross the tokio::spawn in stream_responses.
        fn assert_send<T: Send>() {}
        assert_send::<TelemetryGuard>();
        assert_send::<MiddlewareGuard>();
    }

    #[test]
    fn claim_arc_is_shared_across_cow_mutations() {
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        let before = store.detail("api_1").expect("record").claim.clone();
        store.set_normalized("api_1", Some("model".to_string()), Some(cap(b"{}")));
        let after = store.detail("api_1").expect("record").claim.clone();
        assert!(
            Arc::ptr_eq(&before, &after),
            "claim Arc identity must persist across COW updates for D3"
        );
        before.store(CLAIM_CLAIMED_L1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            after.load(std::sync::atomic::Ordering::SeqCst),
            CLAIM_CLAIMED_L1
        );
    }

    #[test]
    fn summary_quota_evicts_oldest_bodies_keeping_records() {
        let store = DashboardFlowStore {
            enabled: true,
            response_capture_enabled: false,
            state: Arc::new(Mutex::new(DashboardFlowState::default())),
            summary_quota_bytes: 4 * 1024,
        };
        let body = vec![b'a'; 2048];
        let json = {
            let mut v = Vec::new();
            v.extend_from_slice(b"{\"x\":\"");
            v.extend_from_slice(&body);
            v.extend_from_slice(b"\"}");
            v
        };
        for i in 0..3 {
            store.open(
                format!("api_{i}"),
                "POST".to_string(),
                "/v1/responses".to_string(),
                no_headers(),
                Some(cap(&json)),
                ClientAttribution::none(),
            );
        }
        assert_eq!(store.list().len(), 3, "records survive body eviction");
        let oldest = store.detail("api_0").expect("oldest record present");
        assert!(
            oldest.inbound_body.is_none(),
            "oldest body evicted under quota"
        );
        let newest = store.detail("api_2").expect("newest record present");
        assert!(newest.inbound_body.is_some(), "newest body retained");
        assert_eq!(store.snapshot_summaries().len(), 3);
    }

    #[test]
    fn long_scalars_are_capped_counted_and_can_trigger_eviction() {
        // D1 R1 #5: long dynamic scalars (model/uri/target) must be capped to
        // SCALAR_CAP AND counted in `live_summary_bytes`, so their bytes can trip
        // the quota and evict an older body. A small quota makes the scalar bytes —
        // not bodies — the decisive contribution.
        // Quota 16 KiB: api_0's ~10 KiB body fits; api_1's ~12 KiB of CAPPED scalars
        // (3 x SCALAR_CAP) fit on their own; but api_0.body + api_1.scalars exceeds
        // the quota, so the counted scalars force api_0's BODY to be shed while api_1
        // (under-quota by itself) survives.
        let store = DashboardFlowStore {
            enabled: true,
            response_capture_enabled: false,
            state: Arc::new(Mutex::new(DashboardFlowState::default())),
            summary_quota_bytes: 16 * 1024,
        };
        // api_0: a ~10 KiB body (JSON array of many short strings; none individually
        // capped, so the captured body is genuinely large), no large scalars.
        let array_body = format!("{{\"a\":[{}]}}", vec!["\"xx\""; 2000].join(","));
        store.open(
            "api_0".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            no_headers(),
            Some(cap(array_body.as_bytes())),
            ClientAttribution::none(),
        );
        assert!(
            store.detail("api_0").unwrap().inbound_body.is_some(),
            "api_0 body retained under quota initially"
        );
        // api_1: oversized scalars (each capped to SCALAR_CAP, all counted), no body.
        store.open(
            "api_1".to_string(),
            "POST".to_string(),
            "/".to_string() + &"u".repeat(64 * 1024),
            no_headers(),
            None,
            ClientAttribution::none(),
        );
        store.set_upstream(
            "api_1",
            Some("https://".to_string() + &"t".repeat(64 * 1024)),
            Some("m".repeat(64 * 1024)),
            None,
        );
        let record = store
            .detail("api_1")
            .expect("api_1 survives (under quota alone)");
        assert!(record.uri.len() <= SCALAR_CAP, "uri capped");
        assert!(
            record.upstream_target.as_ref().unwrap().len() <= SCALAR_CAP,
            "upstream_target capped"
        );
        assert!(
            record.model_served.as_ref().unwrap().len() <= SCALAR_CAP,
            "model_served capped"
        );
        // The capped-but-counted scalars pushed total over quota → api_0 body shed.
        let oldest = store.detail("api_0").expect("api_0 record still present");
        assert!(
            oldest.inbound_body.is_none(),
            "oldest body evicted once scalars counted toward quota"
        );
    }

    #[test]
    fn whole_records_evicted_when_body_eviction_insufficient() {
        // D1 R2 #3: when bodies are all gone (or absent) but the total still exceeds
        // quota — scalar/header-only records dominate — evict OLDEST WHOLE records
        // until under quota, so the quota is a HARD bound.
        let store = DashboardFlowStore {
            enabled: true,
            response_capture_enabled: false,
            state: Arc::new(Mutex::new(DashboardFlowState::default())),
            // ~10 capped-scalar records fit; many more must force whole-record eviction.
            summary_quota_bytes: 64 * 1024,
        };
        let mut headers = HeaderMap::new();
        // A ~4 KiB header value (capped to SCALAR_CAP) so each record is header-heavy
        // with NO body to shed.
        headers.insert(
            HeaderName::from_static("x-big"),
            HeaderValue::from_str(&"h".repeat(8 * 1024)).unwrap(),
        );
        let captured_headers = redact_headers(&headers);
        for i in 0..40 {
            store.open(
                format!("api_{i}"),
                "POST".to_string(),
                "/v1/responses".to_string(),
                captured_headers.clone(),
                None, // no body → body eviction cannot help
                ClientAttribution::none(),
            );
        }
        // The store stayed under quota by evicting oldest WHOLE records.
        let live = store.list();
        let total: usize = live
            .iter()
            .map(|r| {
                r.headers
                    .iter()
                    .map(|(n, v)| n.len() + v.len())
                    .sum::<usize>()
                    + r.uri.len()
            })
            .sum();
        assert!(
            total <= 64 * 1024,
            "store exceeded quota ({total} > 64 KiB) — whole-record eviction failed"
        );
        // Newest survive, oldest are gone.
        assert!(store.detail("api_39").is_some(), "newest record retained");
        assert!(store.detail("api_0").is_none(), "oldest record evicted");
        assert!(live.len() < 40, "some records were evicted");
    }

    #[test]
    fn cap_scalar_reallocates_to_bounded_capacity() {
        // D1 R3 #2: an oversized scalar must not retain its original (huge) backing
        // capacity behind a shrunk length — cap_scalar copies the bounded prefix into
        // a fresh String so capacity == bounded length.
        let huge = "x".repeat(1024 * 1024); // 1 MiB
        let capped = cap_scalar(huge);
        assert!(capped.len() <= SCALAR_CAP, "length bounded");
        assert!(
            capped.capacity() <= SCALAR_CAP + 4,
            "capacity bounded to ~SCALAR_CAP, not the 1 MiB original: cap={}",
            capped.capacity()
        );
        // A within-cap string is returned unchanged (no needless realloc).
        let small = "hello".to_string();
        assert_eq!(cap_scalar(small), "hello");
    }

    #[test]
    fn record_cap_evicts_oldest_whole_records() {
        let store = DashboardFlowStore::new();
        for i in 0..(FLOW_CAP + 1) {
            open_simple(&store, &format!("api_{i}"));
        }
        assert_eq!(store.list().len(), FLOW_CAP);
        assert!(store.detail("api_0").is_none(), "oldest record evicted");
        assert!(store.detail(&format!("api_{FLOW_CAP}")).is_some());
    }

    #[test]
    fn expired_records_are_pruned_on_read_deterministically() {
        // D1 R1 #7: TTL pruning runs on reads/mutations, not only `open`.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_old");
        // Backdate it well past the retention window.
        store.force_started_ms("api_old", 1);
        open_simple(&store, "api_new");
        // A read at a `now` past the TTL must drop the old record but keep the new.
        store.prune_at(FLOW_TTL_MS + 100);
        assert!(store.detail("api_old").is_none(), "expired record pruned");
        assert!(store.detail("api_new").is_some(), "fresh record retained");
        // And the public read path prunes too (no explicit prune_at needed): age
        // api_new and observe list() drop it at the wall clock.
        store.force_started_ms("api_new", 1);
        // `list()` prunes at `now_ms()`, which is far past `started_ms = 1 + TTL`.
        assert!(
            store.list().iter().all(|r| r.api_call_id != "api_new"),
            "read path prunes expired records"
        );
    }

    // -------------------------------------------------------------------
    // Capture seam (the heavy serializer lives in crate::redaction; these
    // confirm the dashboard wrapper + store integration).
    // -------------------------------------------------------------------

    #[test]
    fn capture_body_peak_allocation_is_bounded_for_10mib_body() {
        // THE crux acceptance criterion: serializing a 10 MiB body keeps PEAK LIVE
        // heap use O(CAP), not O(body) (D1 R2 #5 — peak-live also catches a path
        // doing many small unfreed allocations). Build the input OUTSIDE the armed
        // region so only `capture_body`'s own allocations count.
        const TEN_MIB: usize = 10 * 1024 * 1024;
        let big = "x".repeat(TEN_MIB);
        let json = format!("{{\"text\":\"{big}\"}}");
        let raw = json.into_bytes();
        let ceiling = BODY_CAP + SCALAR_CAP + 64 * 1024;
        let (captured, peak_live) =
            crate::test_alloc_probe::peak_live_alloc_during(|| capture_body(&raw));
        assert!(captured.as_bytes().len() <= BODY_CAP);
        assert!(
            peak_live <= ceiling,
            "capture_body held peak-live {peak_live} bytes (> {ceiling}) for a {TEN_MIB}-byte \
             body — it must stream, not materialize the whole body"
        );
    }

    #[test]
    fn capture_body_peak_bounded_for_10mib_single_key() {
        // D1 R1 #4a: a single 10 MiB OBJECT KEY must also stay O(CAP) (peak-live).
        const TEN_MIB: usize = 10 * 1024 * 1024;
        let big_key = "k".repeat(TEN_MIB);
        let json = format!("{{\"{big_key}\":\"v\"}}");
        let raw = json.into_bytes();
        let ceiling = BODY_CAP + SCALAR_CAP + 64 * 1024;
        let (captured, peak_live) =
            crate::test_alloc_probe::peak_live_alloc_during(|| capture_body(&raw));
        assert!(captured.as_bytes().len() <= BODY_CAP);
        assert!(
            peak_live <= ceiling,
            "huge key held peak-live {peak_live} bytes (> {ceiling}); must be O(CAP)"
        );
    }

    #[test]
    fn capture_body_peak_bounded_for_malformed_body() {
        // D1 R1 #4b + R2 #1a: the fallback must NOT materialize the body (no Value,
        // no retained lossy prefix) — a malformed 10 MiB body must stay O(CAP).
        const TEN_MIB: usize = 10 * 1024 * 1024;
        // Valid UTF-8 but NOT valid JSON (unterminated object) → fallback path.
        let mut raw = Vec::with_capacity(TEN_MIB + 16);
        raw.extend_from_slice(b"{\"a\":\"");
        raw.extend(std::iter::repeat_n(b'z', TEN_MIB));
        // no closing quote/brace
        let ceiling = BODY_CAP + SCALAR_CAP + 64 * 1024;
        let (captured, peak_live) =
            crate::test_alloc_probe::peak_live_alloc_during(|| capture_body(&raw));
        assert!(captured.as_bytes().len() <= BODY_CAP);
        assert!(
            peak_live <= ceiling,
            "malformed-body fallback held peak-live {peak_live} bytes (> {ceiling}); must be O(CAP)"
        );
    }

    #[test]
    fn capture_body_from_value_peak_allocation_is_bounded_for_10mib_prompt() {
        // D2 R1 #1 — THE crux: serializing a typed request whose prompt is 10 MiB
        // must keep PEAK LIVE heap O(CAP), NOT O(body). The old leaf did
        // `serde_json::to_vec` (a full ~10 MiB+ allocation, up to the configured
        // inbound cap for a max body) THEN capped — defeating the guarantee. The Serialize-direct writer is
        // bounded at 2×BODY_CAP, so peak stays well under O(body). Build the input
        // OUTSIDE the armed region so only the capture's own allocations count.
        use serde::Serialize;
        #[derive(Serialize)]
        struct Req {
            model: String,
            prompt: String,
        }
        const TEN_MIB: usize = 10 * 1024 * 1024;
        let req = Req {
            model: "served-model".to_string(),
            prompt: "x".repeat(TEN_MIB),
        };
        // Headroom: the bounded serialize writer holds ≤ 2×BODY_CAP, plus the capped
        // redacting pass's own ≤ BODY_CAP + SCALAR_CAP, plus slack. Crucially this is
        // a CONSTANT independent of the 10 MiB body, proving O(CAP) not O(body).
        let ceiling = 2 * BODY_CAP + BODY_CAP + SCALAR_CAP + 64 * 1024;
        let (captured, peak_live) =
            crate::test_alloc_probe::peak_live_alloc_during(|| capture_body_from_value(&req));
        assert!(captured.as_bytes().len() <= BODY_CAP);
        assert!(
            peak_live <= ceiling,
            "capture_body_from_value held peak-live {peak_live} bytes (> {ceiling}) for a \
             {TEN_MIB}-byte prompt — it must serialize into the bounded writer, NOT to_vec the \
             whole body"
        );
    }

    #[test]
    fn capture_body_from_value_redacts_secrets_and_round_trips_small_request() {
        // A within-bound typed request: secrets redacted, image URIs stripped, and
        // the on-wire field names preserved (valid JSON out). Serialize-direct path
        // must keep every redaction guarantee of the bytes path.
        let value = serde_json::json!({
            "model": "m",
            "api_key": "sk-VALUELEAK",
            "messages": [
                { "role": "user", "content": "see data:image/png;base64,IMGLEAK ok" }
            ]
        });
        let captured = capture_body_from_value(&value);
        let text = String::from_utf8_lossy(captured.as_bytes());
        assert!(!text.contains("VALUELEAK"), "api_key value redacted");
        assert!(!text.contains("IMGLEAK"), "data: payload redacted");
        assert!(text.contains("[redacted]"));
        assert!(text.contains("<redacted uri>"));
        let parsed: serde_json::Value =
            serde_json::from_slice(captured.as_bytes()).expect("captured value body is valid JSON");
        assert_eq!(parsed["model"], serde_json::json!("m"));
    }

    #[test]
    fn capture_body_from_value_overflow_routes_to_fixed_marker_no_leak() {
        // A request whose serialized form exceeds 2×BODY_CAP truncates mid-JSON in the
        // bounded writer → the strict parser bails → fixed marker. No raw/secret bytes
        // survive. Embed a secret PAST the 2×BODY_CAP bound to prove nothing leaks.
        let pad = "p".repeat(2 * BODY_CAP + 4096);
        let value = serde_json::json!({
            "filler": pad,
            "api_key": "sk-TAILLEAK",
        });
        let captured = capture_body_from_value(&value);
        let text = String::from_utf8_lossy(captured.as_bytes());
        assert!(
            text.starts_with("[redacted: unparseable body"),
            "overflowing body → fixed marker: {text}"
        );
        assert!(
            !text.contains("TAILLEAK"),
            "no secret survives the overflow path"
        );
        assert!(captured.as_bytes().len() <= BODY_CAP);
    }

    #[test]
    fn capture_body_redacts_sensitive_keys_inline() {
        let json = br#"{"model":"m","api_key":"sk-SECRETLEAK","nested":{"authorization":"Bearer TOKENLEAK"},"keep":"visible"}"#;
        let captured = capture_body(json);
        let text = String::from_utf8_lossy(captured.as_bytes());
        assert!(!text.contains("SECRETLEAK"), "api_key value redacted");
        assert!(!text.contains("TOKENLEAK"), "nested authorization redacted");
        assert!(text.contains("[redacted]"));
        assert!(text.contains("visible"), "non-sensitive value preserved");
    }

    #[test]
    fn capture_body_redacts_image_uris_in_strings() {
        let json =
            br#"{"content":"see data:image/png;base64,IMGLEAK and https://signed.x/i?sig=SIGLEAK"}"#;
        let captured = capture_body(json);
        let text = String::from_utf8_lossy(captured.as_bytes());
        assert!(!text.contains("IMGLEAK"), "data: payload redacted");
        assert!(!text.contains("SIGLEAK"), "signed-url token redacted");
        assert!(text.contains("<redacted uri>"));
    }

    #[test]
    fn capture_body_redacts_unicode_escaped_image_uris() {
        // D1 R1 #3: a JSON `\uXXXX`-escaped scheme must be DE-ESCAPED before
        // redaction. Build the body so it literally contains JSON unicode escapes
        // for the scheme letters (e.g. the four bytes `\`,`u`,`0`,`0`,`6`,`4` for
        // 'd'), so the stored string spells `data:`/`https:` with escapes;
        // de-escaping must expose them to `redact_image_uris`.
        let esc = |s: &str| -> String {
            s.chars()
                .map(|c| format!("\\u{:04x}", c as u32))
                .collect::<String>()
        };
        // Scheme letters are escaped; the rest is literal so the URI run is intact.
        let data_uri = format!("{}:image/png;base64,UNILEAK", esc("data"));
        let https_uri = format!("{}://h/p?sig=ESCSIGLEAK", esc("https"));
        let json = format!("{{\"a\":\"{data_uri} x\",\"b\":\"{https_uri} y\"}}");
        // Sanity: the body holds the ESCAPED (not literal) scheme.
        assert!(
            json.contains("\\u0064"),
            "test body uses \\u escapes for 'd'"
        );
        assert!(
            !json.contains("data:"),
            "test body has NO literal data: scheme"
        );

        let captured = capture_body(json.as_bytes());
        let text = String::from_utf8_lossy(captured.as_bytes());
        assert!(
            !text.contains("UNILEAK"),
            "unicode-escaped data: payload redacted: {text}"
        );
        assert!(
            !text.contains("ESCSIGLEAK"),
            "unicode-escaped https signed-url token redacted: {text}"
        );
        assert!(text.contains("<redacted uri>"));
    }

    #[test]
    fn capture_body_roundtrips_valid_json_structure() {
        let json = br#"{"a":1,"b":[true,false,null],"c":{"d":"e"}}"#;
        let captured = capture_body(json);
        let value: serde_json::Value =
            serde_json::from_slice(captured.as_bytes()).expect("captured body is valid JSON");
        assert_eq!(value["a"], serde_json::json!(1));
        assert_eq!(value["b"], serde_json::json!([true, false, null]));
        assert_eq!(value["c"]["d"], serde_json::json!("e"));
    }

    #[test]
    fn redact_headers_redacts_sensitive_and_uri_values() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer SECRETHEADER"),
        );
        headers.insert(
            HeaderName::from_static("openai-beta"),
            HeaderValue::from_static("assistants=v2;token=BETASECRET"),
        );
        headers.insert(
            HeaderName::from_static("x-callback-url"),
            HeaderValue::from_static("https://cb.example.com/h?sig=HDRSIGLEAK"),
        );
        headers.insert(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/json"),
        );
        let redacted = redact_headers(&headers);
        let dumped = format!("{redacted:?}");
        assert!(!dumped.contains("SECRETHEADER"), "authorization redacted");
        // openai-beta is in the sensitive-key set now → value redacted.
        assert!(!dumped.contains("BETASECRET"), "openai-beta value redacted");
        // A URI-bearing header value is image-URI-redacted.
        assert!(
            !dumped.contains("HDRSIGLEAK"),
            "signed-url header value redacted"
        );
        assert!(dumped.contains("<redacted uri>") || dumped.contains("[redacted]"));
        assert!(dumped.contains("application/json"));
    }

    #[test]
    fn secret_persistence_prevention_end_to_end() {
        let store = DashboardFlowStore::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer AUTHSECRET"),
        );
        headers.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("XKEYSECRET"),
        );
        headers.insert(
            HeaderName::from_static("api-key"),
            HeaderValue::from_static("APIKEYSECRET"),
        );
        let inbound = br#"{"model":"m","api_key":"BODYKEYSECRET","messages":[]}"#;
        store.open(
            "api_1".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            redact_headers(&headers),
            Some(capture_body(inbound)),
            ClientAttribution::none(),
        );
        let upstream = br#"{"api_key":"UPSTREAMKEYSECRET","model":"m"}"#;
        store.set_upstream(
            "api_1",
            Some("https://upstream".to_string()),
            Some("m".to_string()),
            Some(capture_body(upstream)),
        );

        let record = store.detail("api_1").expect("record");
        let header_dump = format!("{:?}", record.headers);
        assert!(!header_dump.contains("AUTHSECRET"));
        assert!(!header_dump.contains("XKEYSECRET"));
        assert!(!header_dump.contains("APIKEYSECRET"));

        let inbound_text =
            String::from_utf8_lossy(record.inbound_body.as_ref().expect("inbound")).to_string();
        assert!(
            !inbound_text.contains("BODYKEYSECRET"),
            "inbound api_key redacted"
        );

        let upstream_text =
            String::from_utf8_lossy(record.upstream_body.as_ref().expect("upstream")).to_string();
        assert!(
            !upstream_text.contains("UPSTREAMKEYSECRET"),
            "upstream api_key redacted"
        );
    }

    #[test]
    fn snapshot_summaries_are_body_free() {
        let store = DashboardFlowStore::new();
        store.open(
            "api_1".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            no_headers(),
            Some(cap(b"{\"model\":\"m\"}")),
            ClientAttribution::none(),
        );
        let summaries = store.snapshot_summaries();
        assert_eq!(summaries.len(), 1);
        let json = serde_json::to_string(&summaries[0]).expect("serialize summary");
        assert!(json.contains("api_call_id"));
        assert!(!json.contains("inbound_body"));
        assert!(
            !json.contains("\"model\":\"m\""),
            "no body content in summary"
        );
    }

    #[test]
    fn malformed_body_with_api_key_leaks_nothing_into_the_store() {
        // D1 R2 #1a end-to-end: a MALFORMED (non-JSON) inbound body carrying an
        // `api_key` must not persist ANY of the secret in the stored record.
        let store = DashboardFlowStore::new();
        let malformed = b"oops not json api_key=PLAINTEXTLEAK data:image/png;base64,IMGLEAK";
        store.open(
            "api_1".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            no_headers(),
            Some(cap(malformed)),
            ClientAttribution::none(),
        );
        let record = store.detail("api_1").expect("record");
        let body = record.inbound_body.as_ref().expect("inbound stored");
        let text = String::from_utf8_lossy(body);
        assert!(!text.contains("PLAINTEXTLEAK"), "no api_key persisted");
        assert!(!text.contains("IMGLEAK"), "no raw image bytes persisted");
        assert!(
            text.starts_with("[redacted: unparseable body"),
            "fixed marker stored for a malformed body: {text}"
        );
    }

    // -------------------------------------------------------------------
    // Gap 05 — gated upstream RESPONSE/ERROR-body capture (the spine seam).
    // -------------------------------------------------------------------

    #[test]
    fn upstream_response_capture_off_by_default() {
        // The plain debug-UI store (`new()`) does NOT arm response capture unless the
        // separate env flag is set — request capture does not imply response capture.
        let store = DashboardFlowStore::new();
        assert!(
            store.is_enabled(),
            "debug-UI store is enabled for request capture"
        );
        // Whatever the ambient env, a freshly-constructed store reflects the flag; this
        // test only asserts the gate is SEPARATE — a request-capture store with the gate
        // OFF retains no response body.
        if !store.is_response_capture_enabled() {
            open_simple(&store, "api_1");
            store.set_upstream_response("api_1", Some(capture_response_body(b"{\"error\":\"x\"}")));
            let record = store.detail("api_1").expect("record");
            assert!(
                record.upstream_response.is_none(),
                "capture-disabled store retains no upstream response body"
            );
        }
    }

    #[test]
    fn upstream_response_captured_when_gate_on() {
        // Gate ON: a failing turn's error body lands on the LIVE record, keyed by the
        // flow's `response_id` (the only id the leaf knows) via the link index.
        let store = DashboardFlowStore::new_with_response_capture(true);
        assert!(store.is_response_capture_enabled());
        open_simple(&store, "api_1");
        store.link("resp_1".to_string(), "api_1".to_string());
        let body = br#"{"error":{"message":"upstream exploded","type":"server_error"}}"#;
        store.set_upstream_response("resp_1", Some(capture_response_body(body)));

        let record = store.detail("api_1").expect("record");
        let captured = record
            .upstream_response
            .as_ref()
            .expect("response body captured");
        assert!(!captured.truncated, "a small body is not truncated");
        let text = String::from_utf8_lossy(&captured.bytes);
        assert!(
            text.contains("upstream exploded"),
            "the diagnostic error body is retained for the operator: {text}"
        );
        // The capture is an OWNED, capped Arc — never a slice of a larger buffer.
        assert!(captured.bytes.len() <= BODY_CAP);
    }

    #[test]
    fn upstream_response_capture_disabled_store_retains_none() {
        // Explicitly gate OFF: even the dedicated mutator no-ops, so `None` (absent),
        // NOT an empty body, represents "capture disabled" — distinct from a captured
        // empty body (the don't-lie-with-zeros tri-state).
        let store = DashboardFlowStore::new_with_response_capture(false);
        assert!(!store.is_response_capture_enabled());
        open_simple(&store, "api_1");
        store.set_upstream_response("api_1", Some(capture_response_body(b"{\"error\":\"x\"}")));
        let record = store.detail("api_1").expect("record");
        assert!(
            record.upstream_response.is_none(),
            "capture-off ⇒ None (absent), never a fabricated/empty body"
        );
    }

    #[test]
    fn upstream_response_empty_body_is_distinct_from_absent() {
        // A genuinely EMPTY upstream error body (e.g. a 500 with no payload) is still a
        // CAPTURED event: `Some(_)`, distinct from `None` (capture disabled / nothing
        // captured). The redacting serializer records a non-JSON/empty body as the fixed
        // `[redacted: unparseable body 0 bytes]` marker — an HONEST representation that
        // records the body existed AND was empty, never masquerading as "no error" and
        // never a fabricated body. The don't-lie-with-zeros distinction the spec wants is
        // exactly this `Some` (an error happened, body was empty) vs `None` (no capture).
        let store = DashboardFlowStore::new_with_response_capture(true);
        open_simple(&store, "api_1");
        store.set_upstream_response("api_1", Some(capture_response_body(b"")));
        let record = store.detail("api_1").expect("record");
        let captured = record
            .upstream_response
            .as_ref()
            .expect("an empty body is still CAPTURED (Some), not absent");
        let text = String::from_utf8_lossy(&captured.bytes);
        assert_eq!(
            text, "[redacted: unparseable body 0 bytes]",
            "captured-empty is recorded honestly as the 0-bytes marker, not a fake body"
        );
        assert!(!captured.truncated, "an empty body is not truncated");

        // Contrast: a flow with NO response capture has `None` here — the two states are
        // never conflated (capture-disabled / no-body vs captured-empty).
        let off = DashboardFlowStore::new_with_response_capture(false);
        open_simple(&off, "api_off");
        off.set_upstream_response("api_off", Some(capture_response_body(b"")));
        assert!(
            off.detail("api_off").unwrap().upstream_response.is_none(),
            "capture disabled ⇒ None, distinct from the captured-empty Some above"
        );
    }

    #[test]
    fn upstream_response_over_cap_is_truncated_and_flagged() {
        // A body exceeding `BODY_CAP` is capped AND flagged truncated, so the dashboard
        // never presents a partial body as complete. The retained bytes stay ≤ cap.
        let store = DashboardFlowStore::new_with_response_capture(true);
        open_simple(&store, "api_1");
        // A valid-JSON-ish payload far larger than the cap (a long string value). Build
        // it big enough that the raw length exceeds BODY_CAP regardless of redaction.
        let filler = "A".repeat(BODY_CAP * 2);
        let body = format!("{{\"error\":\"{filler}\"}}");
        assert!(body.len() > BODY_CAP, "test body exceeds the cap");
        store.set_upstream_response("api_1", Some(capture_response_body(body.as_bytes())));

        let record = store.detail("api_1").expect("record");
        let captured = record
            .upstream_response
            .as_ref()
            .expect("over-cap body captured");
        assert!(
            captured.truncated,
            "an over-cap body is flagged truncated (don't present partial as complete)"
        );
        assert!(
            captured.bytes.len() <= BODY_CAP,
            "retained bytes stay within the cap regardless of raw size"
        );
    }

    #[test]
    fn upstream_response_external_transport_cap_is_flagged() {
        let store = DashboardFlowStore::new_with_response_capture(true);
        open_simple(&store, "api_1");
        let retained_prefix = br#"{"error":"provider body prefix"}"#;
        assert!(retained_prefix.len() < BODY_CAP);
        store.set_upstream_response(
            "api_1",
            Some(capture_response_body_with_truncation(retained_prefix, true)),
        );

        let record = store.detail("api_1").expect("record");
        let captured = record
            .upstream_response
            .as_ref()
            .expect("externally capped body captured");
        assert!(
            captured.truncated,
            "a transport-capped prefix must not be presented as a complete body"
        );
        assert!(captured.bytes.len() <= BODY_CAP);
    }

    #[test]
    fn upstream_response_body_is_redacted() {
        // The captured error body goes through the SAME redacting serializer as the
        // request layers — a secret echoed back in the upstream error never persists.
        let store = DashboardFlowStore::new_with_response_capture(true);
        open_simple(&store, "api_1");
        let body = br#"{"error":"bad key","api_key":"RESPONSEKEYSECRET"}"#;
        store.set_upstream_response("api_1", Some(capture_response_body(body)));
        let record = store.detail("api_1").expect("record");
        let captured = record.upstream_response.as_ref().expect("captured");
        let text = String::from_utf8_lossy(&captured.bytes);
        assert!(
            !text.contains("RESPONSEKEYSECRET"),
            "upstream-response api_key redacted: {text}"
        );
    }

    #[test]
    fn upstream_response_body_not_on_snapshot_summary() {
        // The body-free invariant (135 GiB worst case): the response body lives ONLY on
        // the live record; the serialized snapshot summary never carries it.
        let store = DashboardFlowStore::new_with_response_capture(true);
        open_simple(&store, "api_1");
        let body = br#"{"error":"SNAPSHOTERRORMARKER"}"#;
        store.set_upstream_response("api_1", Some(capture_response_body(body)));
        // Sanity: it IS on the live record.
        assert!(store.detail("api_1").unwrap().upstream_response.is_some());

        let summaries = store.snapshot_summaries();
        assert_eq!(summaries.len(), 1);
        let json = serde_json::to_string(&summaries[0]).expect("serialize summary");
        assert!(
            !json.contains("upstream_response"),
            "no response-body field on the snapshot summary"
        );
        assert!(
            !json.contains("SNAPSHOTERRORMARKER"),
            "no response-body CONTENT on the snapshot summary: {json}"
        );
    }

    #[test]
    fn upstream_response_body_counts_toward_quota_and_evicts() {
        // The response body is part of the body-eviction target: under quota pressure it
        // is shed (record survives body-free), exactly like the request layers — so it
        // cannot blow the 135 GiB-guard summary quota. D5 evict-safety.
        let store = DashboardFlowStore::new_with_response_capture(true);
        open_simple(&store, "api_1");
        // A non-trivial valid-JSON error body so the redactor keeps the bytes (not a
        // fixed marker) and the retained length is a meaningful quota contribution.
        let mut json = Vec::new();
        json.extend_from_slice(br#"{"error":""#);
        json.extend(std::iter::repeat_n(b'a', 8 * 1024));
        json.extend_from_slice(br#""}"#);
        store.set_upstream_response("api_1", Some(capture_response_body(&json)));
        let before = store.detail("api_1").unwrap();
        let captured_len = before
            .upstream_response
            .as_ref()
            .map(|r| r.bytes.len())
            .unwrap_or(0);
        assert!(captured_len > 0, "captured a non-empty body");

        // Force eviction with a quota BELOW the body size but ABOVE the record's tiny
        // scalars (api_call_id/method/uri ≈ tens of bytes): phase-1 sheds the body and
        // the record SURVIVES body-free (the body is the eviction target, not the row).
        let quota = 1024;
        assert!(captured_len > quota, "the body exceeds the test quota");
        {
            let mut state = store.lock();
            // The response body is counted in the running total (proving it is quota-visible).
            assert!(state.live_summary_bytes >= captured_len);
            state.enforce_summary_quota(quota);
        }
        let after = store.detail("api_1").expect("record survives body-free");
        assert!(
            after.upstream_response.is_none(),
            "response body evicted under quota pressure (record survives)"
        );
    }

    // -------------------------------------------------------------------
    // Gap 02 — per-phase timestamps + true TTFT (the spine).
    // -------------------------------------------------------------------

    #[test]
    fn ingress_phase_stamped_at_open_others_none() {
        // `open` stamps `ingress` (always Some once a record exists); every other
        // phase stays None until its seam runs.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        let phases = store.detail("api_1").expect("record").phases;
        assert!(phases.ingress_ms.is_some(), "ingress stamped at open");
        assert_eq!(
            phases.ingress_ms,
            Some(store.detail("api_1").unwrap().started_ms),
            "ingress ≈ started_ms (same open clock)"
        );
        assert!(phases.normalization_done_ms.is_none());
        assert!(phases.routing_decision_ms.is_none());
        assert!(phases.first_content_delta_ms.is_none());
        assert!(phases.stream_end_ms.is_none());
        assert!(phases.finalize_ms.is_none());
    }

    #[test]
    fn phases_stamp_at_their_seams_in_order() {
        // Drive the full happy path through the store mutators and assert each phase
        // stamps at its seam AND the bundle is monotonic.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.link("resp_1".to_string(), "api_1".to_string());
        store.set_normalized("api_1", Some("m".to_string()), None);
        // Routing decision: the engine stamps this (keyed by api_call_id) once lowering
        // settles, before the leaf's on-wire body capture.
        store.stamp_routing_decision("api_1");
        // The leaf keys by response_id — exercise that join path too.
        store.set_upstream("resp_1", None, Some("served-m".to_string()), None);
        store.stamp_first_content_delta("resp_1");
        store.stamp_stream_end("resp_1");
        store.finalize("api_1", FlowStatus::Completed, None, None);

        let p = store.detail("api_1").expect("record").phases;
        assert!(p.ingress_ms.is_some(), "ingress");
        assert!(p.normalization_done_ms.is_some(), "normalization");
        assert!(p.routing_decision_ms.is_some(), "routing");
        assert!(p.first_content_delta_ms.is_some(), "first_content_delta");
        assert!(p.stream_end_ms.is_some(), "stream_end");
        assert!(p.finalize_ms.is_some(), "finalize");

        // Monotonic: ingress ≤ normalization ≤ routing ≤ first_content ≤ stream_end ≤ finalize.
        let seq = [
            p.ingress_ms.unwrap(),
            p.normalization_done_ms.unwrap(),
            p.routing_decision_ms.unwrap(),
            p.first_content_delta_ms.unwrap(),
            p.stream_end_ms.unwrap(),
            p.finalize_ms.unwrap(),
        ];
        for win in seq.windows(2) {
            assert!(
                win[0] <= win[1],
                "phases must be monotonic: {} > {}",
                win[0],
                win[1]
            );
        }
    }

    #[test]
    fn first_content_delta_is_first_write_wins() {
        // The engine calls `stamp_first_content_delta` per content delta; only the
        // FIRST stamps, so the value marks the first token the client saw.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.stamp_first_content_delta("api_1");
        let first = store
            .detail("api_1")
            .unwrap()
            .phases
            .first_content_delta_ms
            .expect("stamped on first content delta");
        // Later deltas must NOT move it (even if the wall clock advanced).
        for _ in 0..5 {
            store.stamp_first_content_delta("api_1");
        }
        assert_eq!(
            store.detail("api_1").unwrap().phases.first_content_delta_ms,
            Some(first),
            "subsequent content deltas do not re-stamp TTFT"
        );
    }

    #[test]
    fn error_before_content_leaves_first_content_delta_none() {
        // A flow that finalizes Failed without ever emitting a content delta must
        // leave first_content_delta_ms = None (don't-lie-with-zeros: absent, not 0).
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.set_normalized("api_1", Some("m".to_string()), None);
        // No stamp_first_content_delta, no stamp_stream_end — errored mid-flight.
        store.finalize(
            "api_1",
            FlowStatus::Failed,
            Some("upstream 500".to_string()),
            None,
        );
        let p = store.detail("api_1").expect("record").phases;
        assert!(
            p.first_content_delta_ms.is_none(),
            "no content delta ⇒ TTFT None, NEVER 0"
        );
        assert!(
            p.stream_end_ms.is_none(),
            "errored before clean stream end ⇒ stream_end None"
        );
        // But finalize still fired (every terminal stamps it).
        assert!(
            p.finalize_ms.is_some(),
            "failed terminal still stamps finalize"
        );
    }

    #[test]
    fn phase_stamp_clamps_monotonic_against_backwards_clock() {
        // PhaseTimings::stamp clamps a value UP to the latest prior phase, so even if
        // the wall clock steps backwards between seams the bundle stays monotonic.
        let mut p = PhaseTimings::default();
        p.stamp_ingress(1_000);
        p.stamp_normalization(900); // clock went backwards
        assert_eq!(
            p.normalization_done_ms,
            Some(1_000),
            "a backwards clock is clamped up to the prior phase floor"
        );
        p.stamp_routing(2_000);
        assert_eq!(p.routing_decision_ms, Some(2_000));
        p.stamp_first_content_delta(1_500); // backwards again
        assert_eq!(
            p.first_content_delta_ms,
            Some(2_000),
            "clamped to the latest (routing) floor"
        );
    }

    #[test]
    fn phase_stamp_never_emits_zero_for_a_missing_phase() {
        // The crux of don't-lie-with-zeros at the data layer: an unmeasured phase is
        // None (absent in JSON), and a measured phase is the real epoch ms — never a
        // sentinel 0 that would be indistinguishable from "didn't happen".
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        let p = store.detail("api_1").unwrap().phases;
        // Unmeasured phases are None, not Some(0).
        assert_eq!(p.normalization_done_ms, None);
        assert_ne!(p.normalization_done_ms, Some(0));
        // The measured ingress phase is a real (non-zero in practice) wall-clock ms.
        assert!(p.ingress_ms.unwrap() > 0, "ingress is a real epoch ms");
    }

    #[test]
    fn absent_phases_serialize_as_absent_not_zero() {
        // A record with only ingress stamped must serialize the OTHER phases as ABSENT
        // (skip_serializing_if), never as `"...": 0` or `"...": null`. This is the
        // wire-level don't-lie-with-zeros guarantee the later waterfall depends on.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        let summary = store.snapshot_summaries().into_iter().next().expect("one");
        let json = serde_json::to_string(&summary).expect("serialize summary");
        // ingress flattened onto the summary as a sibling field.
        assert!(json.contains("\"ingress_ms\":"), "ingress present: {json}");
        // The unmeasured phases are ABSENT entirely (not 0, not null).
        for field in [
            "normalization_done_ms",
            "routing_decision_ms",
            "first_content_delta_ms",
            "stream_end_ms",
            "finalize_ms",
        ] {
            assert!(
                !json.contains(field),
                "unmeasured phase `{field}` must be ABSENT, not serialized: {json}"
            );
        }
        // And there is no `:0` masquerading as a phase value (the sentinel we forbid).
        assert!(
            !json.contains("_ms\":0"),
            "no phase serialized as the zero sentinel: {json}"
        );
    }

    #[test]
    fn phase_timings_round_trip_deserialize_serialize() {
        // AGENTS.md: a NEW wire field needs a deserialize→serialize round-trip proving
        // it survives. PhaseTimings is the new wire bundle (flattened onto the body-free
        // summary); round-trip it with a mix of present + absent phases.
        let wire = r#"{"ingress_ms":1000,"routing_decision_ms":1200,"first_content_delta_ms":1500,"finalize_ms":1800}"#;
        let parsed: PhaseTimings = serde_json::from_str(wire).expect("deserialize phases");
        assert_eq!(parsed.ingress_ms, Some(1000));
        assert_eq!(parsed.normalization_done_ms, None, "absent stays None");
        assert_eq!(parsed.routing_decision_ms, Some(1200));
        assert_eq!(parsed.first_content_delta_ms, Some(1500));
        assert_eq!(parsed.stream_end_ms, None, "absent stays None");
        assert_eq!(parsed.finalize_ms, Some(1800));
        // Re-serialize: the present fields survive, the absent ones stay absent.
        let reser = serde_json::to_string(&parsed).expect("serialize phases");
        assert!(reser.contains("\"ingress_ms\":1000"));
        assert!(reser.contains("\"routing_decision_ms\":1200"));
        assert!(reser.contains("\"first_content_delta_ms\":1500"));
        assert!(reser.contains("\"finalize_ms\":1800"));
        assert!(
            !reser.contains("normalization_done_ms"),
            "absent phase not re-emitted: {reser}"
        );
        assert!(
            !reser.contains("stream_end_ms"),
            "absent phase not re-emitted: {reser}"
        );
        // The round-trip is stable (parse the re-serialized form back to the same value).
        let reparsed: PhaseTimings = serde_json::from_str(&reser).expect("re-deserialize");
        assert_eq!(reparsed, parsed, "round-trip is lossless");
    }

    #[test]
    fn phases_survive_full_summary_round_trip_via_value() {
        // The flatten places phases as siblings on the summary; confirm a serialized
        // summary's phase siblings survive a JSON Value round-trip (the WS/snapshot
        // wire shape), since SnapshotFlowSummary itself is Serialize-only.
        let store = DashboardFlowStore::new();
        open_simple(&store, "api_1");
        store.link("resp_1".to_string(), "api_1".to_string());
        store.set_normalized("api_1", Some("m".to_string()), None);
        store.stamp_routing_decision("api_1");
        store.set_upstream("resp_1", None, Some("served-m".to_string()), None);
        store.stamp_first_content_delta("resp_1");
        store.stamp_stream_end("resp_1");
        store.finalize("api_1", FlowStatus::Completed, None, None);

        let summary = store.snapshot_summaries().into_iter().next().unwrap();
        let value: serde_json::Value = serde_json::to_value(&summary).expect("to_value");
        // Phases are flattened siblings, all present on a complete flow.
        for field in [
            "ingress_ms",
            "normalization_done_ms",
            "routing_decision_ms",
            "first_content_delta_ms",
            "stream_end_ms",
            "finalize_ms",
        ] {
            assert!(
                value.get(field).and_then(|v| v.as_u64()).is_some(),
                "phase `{field}` is a numeric sibling on the summary: {value}"
            );
        }
        // The phase bundle parses back out via the flattened Deserialize.
        let phases: PhaseTimings =
            serde_json::from_value(value).expect("phases from summary value");
        assert_eq!(
            phases, summary.phases,
            "phases survive the summary round-trip"
        );
    }

    // -----------------------------------------------------------------------
    // Gap 04 — client_label / key-hash attribution.
    // -----------------------------------------------------------------------

    /// `Authorization: Bearer <key>` (or `x-api-key`) → a `key-<hex>` label tagged
    /// `KeyHash`. A request that ALSO carries a User-Agent still resolves to the
    /// key-hash (the strongest source wins the priority order, not the weaker UA).
    #[test]
    fn client_attribution_key_hash_wins_over_user_agent() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer sk-RAWKEYVALUE-123"),
        );
        headers.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("curl/8.1"),
        );
        let attr = ClientAttribution::derive(&headers, None);
        assert_eq!(attr.source, Some(ClientSource::KeyHash));
        let label = attr.label.clone().expect("key-hash label present");
        assert!(label.starts_with("key-"), "label is a key-hash id: {label}");
        // `key-` + 12 hex chars.
        assert_eq!(label.len(), 4 + KEY_HASH_DISPLAY_HEX_LEN);
        assert!(
            label["key-".len()..].chars().all(|c| c.is_ascii_hexdigit()),
            "label hex tail: {label}"
        );
        // The raw key never appears anywhere in the attribution.
        assert!(
            !label.contains("RAWKEYVALUE"),
            "raw key never embedded in label: {label}"
        );
        assert!(!format!("{attr:?}").contains("RAWKEYVALUE"));

        // `x-api-key` is the alternate carrier and yields a key-hash too.
        let mut xkey = HeaderMap::new();
        xkey.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("anthropic-RAWKEYVALUE-123"),
        );
        let attr2 = ClientAttribution::derive(&xkey, None);
        assert_eq!(attr2.source, Some(ClientSource::KeyHash));
        assert!(attr2.label.unwrap().starts_with("key-"));
    }

    /// The SAME key yields the SAME label across requests (stable grouping); a
    /// DIFFERENT key yields a DIFFERENT label (collision-resistant). The label is a
    /// one-way digest prefix, so it is not the key and cannot be inverted.
    #[test]
    fn client_attribution_key_hash_is_stable_and_distinct() {
        let derive = |bearer: &'static str| {
            let mut h = HeaderMap::new();
            h.insert(
                HeaderName::from_static("authorization"),
                HeaderValue::from_str(bearer).unwrap(),
            );
            ClientAttribution::derive(&h, None).label.unwrap()
        };
        let a1 = derive("Bearer sk-aaaaaaaaaaaa");
        let a2 = derive("Bearer sk-aaaaaaaaaaaa");
        let b = derive("Bearer sk-bbbbbbbbbbbb");
        assert_eq!(a1, a2, "same key → same label (stable)");
        assert_ne!(a1, b, "different key → different label");
        // A bare token without the `Bearer ` prefix hashes the same as the equivalent
        // bearer value (the scheme prefix is stripped before hashing).
        let bare = derive("sk-aaaaaaaaaaaa");
        assert_eq!(
            bare, a1,
            "bearer-stripped key hashes identically to bare key"
        );
    }

    /// The configured caller-id header is the SECOND priority: with no key it wins
    /// over the User-Agent; its value (not a hash) is the label, tagged
    /// `ConfiguredHeader`.
    #[test]
    fn client_attribution_configured_header_beats_user_agent() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-client-id"),
            HeaderValue::from_static("team-alpha"),
        );
        headers.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("curl/8.1"),
        );
        let attr = ClientAttribution::derive(&headers, Some("x-client-id"));
        assert_eq!(attr.source, Some(ClientSource::ConfiguredHeader));
        assert_eq!(attr.label.as_deref(), Some("team-alpha"));

        // When NO configured header name is supplied, the same request falls through
        // to the User-Agent fallback instead.
        let ua_only = ClientAttribution::derive(&headers, None);
        assert_eq!(ua_only.source, Some(ClientSource::UserAgent));
        assert_eq!(ua_only.label.as_deref(), Some("curl/8.1"));
    }

    /// Gap 04 review F1 (HIGH key-leak): a CONFIGURED client header whose NAME is a
    /// sensitive key carrier (e.g. `api-key`, `authorization`) must NEVER have its raw
    /// value emitted verbatim into `client_label` — it is treated as a KEY-HASH source
    /// (one-way SHA-256 prefix), exactly like `x-api-key`. Before the fix,
    /// `LLMCONDUIT_DASHBOARD_CLIENT_HEADER=api-key` leaked the raw `api-key` value
    /// because only `x-api-key`/bearer were hashed.
    #[test]
    fn client_attribution_sensitive_configured_header_is_hashed_not_leaked() {
        // `api-key` (the unhyphenated `apikey` alias of `x-api-key`) configured as the
        // caller-id header: its value must be HASHED, never the raw value as a label.
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("api-key"),
            HeaderValue::from_static("RAW-APIKEY-LEAK-9999"),
        );
        let attr = ClientAttribution::derive(&headers, Some("api-key"));
        assert_eq!(
            attr.source,
            Some(ClientSource::KeyHash),
            "a sensitive configured header is a key-hash source, not verbatim"
        );
        let label = attr.label.clone().expect("hashed label present");
        assert!(label.starts_with("key-"), "label is a key-hash id: {label}");
        assert!(
            !label.contains("RAW-APIKEY-LEAK") && !label.contains("9999"),
            "raw configured-header value NEVER leaks into the label: {label}"
        );
        assert!(
            !format!("{attr:?}").contains("RAW-APIKEY-LEAK"),
            "raw value absent from the Debug dump too"
        );

        // `authorization` configured as the caller-id header (a raw token, no `Bearer `
        // scheme): still hashed, never emitted verbatim.
        let mut auth = HeaderMap::new();
        auth.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("RAW-AUTHZ-LEAK-8888"),
        );
        let attr2 = ClientAttribution::derive(&auth, Some("authorization"));
        assert_eq!(attr2.source, Some(ClientSource::KeyHash));
        let label2 = attr2.label.clone().expect("hashed label present");
        assert!(label2.starts_with("key-"));
        assert!(
            !label2.contains("RAW-AUTHZ-LEAK") && !label2.contains("8888"),
            "raw authorization value NEVER leaks: {label2}"
        );

        // The hash of a sensitive configured header equals the hash of the SAME value
        // arriving via the canonical key path — one audited key→label mapping, so the
        // configured-header path is not a second, weaker (leaky) form.
        let mut canonical = HeaderMap::new();
        canonical.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("RAW-APIKEY-LEAK-9999"),
        );
        let canonical_label = ClientAttribution::derive(&canonical, None).label.unwrap();
        assert_eq!(
            label, canonical_label,
            "configured sensitive header hashes identically to the canonical key path"
        );
    }

    /// Gap 04 review F1: a configured header whose NAME is sensitive but which carries
    /// NO usable key value (absent or blank) is SUPPRESSED — the derivation falls
    /// through to the weaker User-Agent fallback rather than ever emitting the sensitive
    /// header verbatim. (A sensitive name is ONLY ever a key-hash source; it can never
    /// become a verbatim `ConfiguredHeader` label.)
    #[test]
    fn client_attribution_sensitive_configured_header_blank_is_suppressed_not_verbatim() {
        // Sensitive configured header present but BLANK → not a key (skipped), and never
        // taken verbatim → falls through to the UA fallback.
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("api-key"),
            HeaderValue::from_static("   "),
        );
        headers.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("curl/8.1"),
        );
        let attr = ClientAttribution::derive(&headers, Some("api-key"));
        assert_eq!(
            attr.source,
            Some(ClientSource::UserAgent),
            "a blank sensitive configured header is suppressed, falling through to UA"
        );
        assert_eq!(attr.label.as_deref(), Some("curl/8.1"));

        // Sensitive configured header ABSENT entirely, no UA → honestly unattributed
        // (never a verbatim/empty configured label).
        let none = ClientAttribution::derive(&HeaderMap::new(), Some("authorization"));
        assert_eq!(none, ClientAttribution::none());
    }

    /// Gap 04 review F2 (MEDIUM): each key candidate is normalized/trimmed BEFORE the
    /// fallback. An `Authorization: Bearer`-only (empty token) header must NOT suppress
    /// a valid `x-api-key` nor fabricate a hash from the literal `"Bearer"` — the blank
    /// bearer is skipped and the real `x-api-key` is hashed instead.
    #[test]
    fn client_attribution_empty_bearer_falls_through_to_x_api_key() {
        let mut headers = HeaderMap::new();
        // `Bearer` with no token (the scheme-stripped, trimmed token is empty).
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer"),
        );
        headers.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("REAL-XKEY-VALUE-7777"),
        );
        let attr = ClientAttribution::derive(&headers, None);
        assert_eq!(
            attr.source,
            Some(ClientSource::KeyHash),
            "the valid x-api-key is used, not suppressed by the empty bearer"
        );
        let label = attr.label.clone().expect("label present");
        // The label is the hash of the x-api-key, identical to hashing it alone — so the
        // empty `Authorization` neither suppressed nor poisoned the candidate.
        let mut xkey_only = HeaderMap::new();
        xkey_only.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("REAL-XKEY-VALUE-7777"),
        );
        let xkey_label = ClientAttribution::derive(&xkey_only, None).label.unwrap();
        assert_eq!(
            label, xkey_label,
            "empty bearer falls through to x-api-key; the literal is never hashed"
        );
        // And the literal scheme word never becomes a label.
        assert!(!label.contains("Bearer"));

        // `Authorization: Bearer    ` (trailing whitespace only) with NO other key and a
        // UA → the blank bearer is skipped entirely and the UA fallback is used (the
        // literal is never fabricated into a hash).
        let mut blank = HeaderMap::new();
        blank.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer    "),
        );
        blank.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("agent/1.0"),
        );
        let attr2 = ClientAttribution::derive(&blank, None);
        assert_eq!(
            attr2.source,
            Some(ClientSource::UserAgent),
            "an empty bearer with no other key falls through to UA, not a fabricated hash"
        );
        assert_eq!(attr2.label.as_deref(), Some("agent/1.0"));
    }

    /// Gap 04 review ROUND 2 (MEDIUM): when `LLMCONDUIT_DASHBOARD_CLIENT_HEADER=authorization`
    /// AND the request carries a token-less `Authorization: Bearer` / `Bearer   `
    /// (whitespace-only), the configured-sensitive-header key path MUST read the value
    /// through the SAME `bearer_token` scheme normalization as the canonical bearer
    /// candidate — so the empty token is skipped and the derivation falls through
    /// (x-api-key → UA → None). The scheme literal `"Bearer"` is NEVER hashed/emitted.
    /// The configured `authorization` happy-path (a real `Bearer <token>`) still hashes
    /// the token (identically to the canonical path), proving the fix did not regress it.
    #[test]
    fn client_attribution_configured_authorization_tokenless_bearer_falls_through() {
        // (a) configured `authorization` + `Authorization: Bearer` (no token) + a valid
        //     `x-api-key` → falls through to the x-api-key hash; the literal is not hashed.
        let mut a = HeaderMap::new();
        a.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer"),
        );
        a.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("REAL-XKEY-CFGAUTH-4242"),
        );
        let attr_a = ClientAttribution::derive(&a, Some("authorization"));
        assert_eq!(
            attr_a.source,
            Some(ClientSource::KeyHash),
            "tokenless configured-authorization bearer falls through to the valid x-api-key"
        );
        let label_a = attr_a.label.clone().expect("label present");
        let mut xkey_only = HeaderMap::new();
        xkey_only.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("REAL-XKEY-CFGAUTH-4242"),
        );
        let xkey_label = ClientAttribution::derive(&xkey_only, None).label.unwrap();
        assert_eq!(
            label_a, xkey_label,
            "the label is the x-api-key hash; the scheme literal was never hashed"
        );
        assert!(
            !label_a.contains("Bearer"),
            "the literal 'Bearer' is never emitted: {label_a}"
        );

        // (b) configured `authorization` + `Authorization: Bearer   ` (whitespace-only
        //     token) + only a UA → falls through to the UA fallback (literal never hashed).
        let mut b = HeaderMap::new();
        b.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer   "),
        );
        b.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("cfgauth/9.9"),
        );
        let attr_b = ClientAttribution::derive(&b, Some("authorization"));
        assert_eq!(
            attr_b.source,
            Some(ClientSource::UserAgent),
            "whitespace-only configured-authorization bearer falls through to UA"
        );
        assert_eq!(attr_b.label.as_deref(), Some("cfgauth/9.9"));
        assert!(!format!("{attr_b:?}").contains("Bearer"));

        // (c) configured `authorization` + `Authorization: Bearer` (no token) and NO
        //     other signal → honestly unattributed (`None`), never a fabricated label.
        let mut c = HeaderMap::new();
        c.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer"),
        );
        let attr_c = ClientAttribution::derive(&c, Some("authorization"));
        assert_eq!(
            attr_c,
            ClientAttribution::none(),
            "tokenless configured-authorization bearer with no other signal is None"
        );

        // (d) HAPPY PATH: configured `authorization` WITH a real `Bearer <token>` → the
        //     token is hashed (KeyHash), identical to the canonical bearer path; the
        //     scheme word is stripped, never part of the hashed value.
        let mut d = HeaderMap::new();
        d.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer sk-CFGAUTH-REALTOKEN-1234"),
        );
        let attr_d = ClientAttribution::derive(&d, Some("authorization"));
        assert_eq!(
            attr_d.source,
            Some(ClientSource::KeyHash),
            "configured-authorization WITH a real token still hashes (happy path intact)"
        );
        let label_d = attr_d.label.clone().expect("label present");
        assert!(label_d.starts_with("key-"));
        // Equals hashing the bare token via the canonical bearer path (scheme stripped).
        let canonical_label = ClientAttribution::derive(&d, None).label.unwrap();
        assert_eq!(
            label_d, canonical_label,
            "configured-authorization hashes the scheme-stripped token, like the canonical path"
        );
        assert!(
            !label_d.contains("Bearer") && !label_d.contains("CFGAUTH-REALTOKEN"),
            "neither the scheme word nor the raw token leaks into the label: {label_d}"
        );
    }

    /// User-Agent is the WEAKEST, labelled fallback — used only when there is no key
    /// and no configured-id header. Tagged `UserAgent` so the dashboard can render it
    /// as the weak source it is.
    #[test]
    fn client_attribution_user_agent_is_the_weak_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("my-app/2.0"),
        );
        let attr = ClientAttribution::derive(&headers, Some("x-client-id"));
        assert_eq!(attr.source, Some(ClientSource::UserAgent));
        assert_eq!(attr.label.as_deref(), Some("my-app/2.0"));
    }

    /// Don't-lie-with-zeros: a request with NO key, NO configured id, and NO
    /// User-Agent yields `None`/`None` — an honestly unattributed flow, never a
    /// fabricated id or empty string. A blank key / blank UA is treated as absent.
    #[test]
    fn client_attribution_none_when_no_signal() {
        // Truly empty headers.
        let empty = ClientAttribution::derive(&HeaderMap::new(), Some("x-client-id"));
        assert_eq!(empty, ClientAttribution::none());
        assert!(empty.label.is_none() && empty.source.is_none());

        // A blank/whitespace bearer token and a blank UA do not fabricate a label.
        let mut blanks = HeaderMap::new();
        blanks.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer    "),
        );
        blanks.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("   "),
        );
        let attr = ClientAttribution::derive(&blanks, None);
        assert_eq!(
            attr,
            ClientAttribution::none(),
            "blank key + blank UA → unattributed, not empty-string-as-id"
        );
    }

    /// End-to-end through the store: a flow opened with a key-hash attribution
    /// carries `client_label`/`client_source` on the `FlowRecord` AND the projected
    /// `SnapshotFlowSummary`, and the raw key NEVER appears in the record, the
    /// summary, or its serialized JSON (the redaction assertion).
    #[test]
    fn client_attribution_flows_to_record_and_summary_without_raw_key() {
        let store = DashboardFlowStore::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer sk-SUPERSECRETKEY-xyz"),
        );
        let attr = ClientAttribution::derive(&headers, None);
        let expected_label = attr.label.clone().unwrap();
        store.open(
            "api_client".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            redact_headers(&headers),
            Some(cap(b"{\"model\":\"m\"}")),
            attr,
        );

        // Record carries the label + source; NOT the raw key.
        let record = store.detail("api_client").expect("record present");
        assert_eq!(
            record.client_label.as_deref(),
            Some(expected_label.as_str())
        );
        assert_eq!(record.client_source, Some(ClientSource::KeyHash));
        assert!(record.client_label.as_deref().unwrap().starts_with("key-"));
        assert!(
            !format!("{record:?}").contains("SUPERSECRETKEY"),
            "raw key never stored on the FlowRecord"
        );

        // Summary projection carries it, and its serialized JSON has the snake_case
        // fields with NO raw key anywhere.
        let summary = store.snapshot_summaries().into_iter().next().unwrap();
        assert_eq!(
            summary.client_label.as_deref(),
            Some(expected_label.as_str())
        );
        assert_eq!(summary.client_source, Some(ClientSource::KeyHash));
        let json = serde_json::to_string(&summary).expect("serialize summary");
        assert!(
            !json.contains("SUPERSECRETKEY"),
            "raw key never on the wire: {json}"
        );
        assert!(json.contains("\"client_source\":\"key_hash\""), "{json}");
        assert!(
            json.contains(&format!("\"client_label\":\"{expected_label}\"")),
            "{json}"
        );
    }

    /// An unattributed flow (`ClientAttribution::none()`) serializes with NO
    /// `client_label`/`client_source` keys at all (absent ⇒ renders `—` downstream,
    /// never `null`-as-zero or an empty string). `ClientSource` itself round-trips
    /// (serialize → deserialize) for the WS/snapshot wire contract.
    #[test]
    fn client_attribution_absent_on_wire_and_source_round_trips() {
        let store = DashboardFlowStore::new();
        store.open(
            "api_anon".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            no_headers(),
            None,
            ClientAttribution::none(),
        );
        let summary = store.snapshot_summaries().into_iter().next().unwrap();
        assert!(summary.client_label.is_none());
        assert!(summary.client_source.is_none());
        let value = serde_json::to_value(&summary).expect("to_value");
        assert!(
            value.get("client_label").is_none(),
            "absent label key omitted: {value}"
        );
        assert!(
            value.get("client_source").is_none(),
            "absent source key omitted: {value}"
        );

        // The wire enum round-trips for every variant (no new wire field without a
        // round-trip test — AGENTS.md).
        for source in [
            ClientSource::KeyHash,
            ClientSource::ConfiguredHeader,
            ClientSource::UserAgent,
        ] {
            let json = serde_json::to_string(&source).expect("serialize source");
            let back: ClientSource = serde_json::from_str(&json).expect("deserialize source");
            assert_eq!(source, back, "ClientSource round-trips: {json}");
        }
        // The snake_case wire spellings are stable.
        assert_eq!(
            serde_json::to_string(&ClientSource::KeyHash).unwrap(),
            "\"key_hash\""
        );
        assert_eq!(
            serde_json::to_string(&ClientSource::UserAgent).unwrap(),
            "\"user_agent\""
        );
    }
}
