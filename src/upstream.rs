use crate::config::merge_json_maps;
use crate::error::AppError;
use crate::error::AppResult;
use crate::error::FailoverDisposition;
use crate::models::chat::ChatCompletionChunk;
use crate::models::chat::ChatCompletionRequest;
use crate::proxy_headers::header_name_eq;
use crate::proxy_headers::is_hop_by_hop_header;
use crate::sse_guard::bounded_sse_byte_stream;
use crate::sse_guard::default_max_sse_frame_bytes;
use crate::turn_capture::TurnCaptureState;
use async_trait::async_trait;
use axum::body::Bytes;
use eventsource_stream::Eventsource;
use futures::Stream;
use futures::StreamExt;
use http::HeaderMap;
use http::HeaderName;
use regex::Regex;
use reqwest::RequestBuilder;
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::Map as JsonMap;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::Mutex as AsyncMutex;
use url::Url;

/// Consecutive-failure count at or above which a cooling provider is reported
/// `Down` (vs. transiently `Cooling`). A provider that has failed this many
/// times in a row AND is still inside its cooldown window is shown as hard-down
/// in the topology map; a single transient failure stays `Cooling`.
const DOWN_THRESHOLD: u32 = 3;

/// Wall-clock epoch-ms, for the serializable [`ProviderHealth`] timestamps. The
/// upstream cooldown bookkeeping uses a monotonic [`Instant`] (immune to clock
/// jumps); health DTOs convert a deadline to epoch-ms only at read time.
fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Convert a future monotonic `Instant` deadline to an epoch-ms wall-clock value
/// for serialization, by measuring how far in the future it is from `now` and
/// adding that to the current epoch-ms. A deadline already in the past yields the
/// current epoch-ms (the cooldown is effectively over). `None` ⇒ no cooldown.
fn instant_deadline_to_epoch_ms(deadline: Option<Instant>) -> Option<u64> {
    let deadline = deadline?;
    let now = Instant::now();
    let remaining = deadline.saturating_duration_since(now);
    Some(now_epoch_ms().saturating_add(remaining.as_millis() as u64))
}

/// Wall-clock epoch-ms as `u128`, matching the dashboard FlowStore's `started_ms`/phase
/// stamps (gap 03 attempt timestamps). A clock that is before `UNIX_EPOCH` yields `0`.
fn now_epoch_ms_u128() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Gap 03 round-1 review (F1): stamp NOW (the instant the upstream response headers
/// arrived) onto the current attempt's wire-byte slot on the shared `ServingToken`.
/// No-op when no token is threaded (tests / non-engine paths). Called by the leaf right
/// after `logged_send_chat_request` returns, for BOTH a 2xx and a non-2xx response, so the
/// failover loop / bare-leaf reads the TRUE wire TTFB even for an HTTP-status failure.
fn stamp_header_byte(serving: Option<&Arc<ServingToken>>) {
    if let Some(serving) = serving {
        serving.stamp_attempt_header_byte(now_epoch_ms_u128());
    }
}

/// Gap 03: map a failed-attempt [`AppError`] to a BOUNDED, sanitized taxonomic
/// [`AttemptErrorClass`](crate::dashboard_flow::AttemptErrorClass) — NEVER raw upstream
/// text (which stays behind spec 05's gated seam), so the body-free summary can never
/// leak an unbounded/secret-bearing upstream error body.
///
/// Round-1 review (F2): classification is driven by STRUCTURED metadata and FIXED
/// gateway-emitted prefixes only — never by a `contains()` substring scan of the full
/// `Display` text, which can include the redacted-but-attacker-influenced upstream
/// response body interpolated into `"upstream chat failed with {status}: {body}"`. A raw
/// body containing the literal text `"timed out"` / `"request failed"` must NOT be able to
/// flip the bounded code. So:
///   - We match the gateway's OWN fixed leaf-error PREFIXES with `starts_with`
///     against the part of the message BEFORE any `{body}` is interpolated. Each
///     body-bearing gateway error is `"<fixed prefix> {status}: <body>"` or
///     `"<fixed prefix>: <reqwest err>"`, so a `starts_with` on the fixed prefix is
///     immune to body content (the body is strictly after the prefix). Body-free fixed
///     markers (timeout / stream-ended) match in full. First-chunk read/parse failures
///     from `stream_success_response` (`"failed to parse upstream chat chunk: …"` /
///     `"failed to read upstream SSE: …"`) match their fixed prefix → `Stream`.
///   - Only once NONE of those fixed prefixes match do we fall back to `Terminal`
///     disposition (E2a: this is now checked LAST, not first — see below) and finally
///     `Other`.
///
/// E2a note: `Terminal` disposition is no longer a same-provider-terminal ⇒
/// `AttemptErrorClass::Terminal` shortcut checked FIRST. E2a additionally tags a
/// request-intrinsic 4xx (`{400,413,415,422}`, `dispatch_chat_stream`) `Terminal` so
/// failover/cooldown skip it, but the DASHBOARD taxonomy must still show that case as
/// `HttpStatus` (it IS a genuine upstream HTTP-status response) — `AttemptErrorClass::
/// Terminal` stays reserved for a context-window overflow that survived shrink-and-retry,
/// whose message (`"upstream context-window overflow persisted …"`) never matches the
/// `"upstream chat failed with "` prefix below. Checking the fixed prefixes BEFORE the
/// disposition fallback lets the SAME message-shape drive the SAME `HttpStatus` code
/// regardless of disposition, while the two Terminal producers stay classified
/// differently: request-intrinsic 4xx → `HttpStatus` (via the prefix match), persisted
/// overflow → `Terminal` (via the fallback, nothing else matches its message). Only
/// `failover_reason` (`AttemptFailoverReason::TerminalNoFailover`, set by the callers
/// below) distinguishes "terminal, no failover" from an ordinary failover-eligible
/// `HttpStatus` in the trace — `error_class` alone does not.
///
/// Every prefix below is a constant string this module constructs in `dispatch_chat_stream`
/// / `send_chat_request` / `prefetch_first_chunk` — none is reachable from upstream text.
fn classify_attempt_error(err: &AppError) -> crate::dashboard_flow::AttemptErrorClass {
    use crate::dashboard_flow::AttemptErrorClass;
    use crate::error::FailoverDisposition;
    let message = err.to_string();
    // Transport/connect failure BEFORE any HTTP response: `send_chat_request` /
    // `list_models` map a reqwest send error to this fixed prefix; the trailing
    // `{err}` is a reqwest transport error, not an upstream response body.
    if message.starts_with("upstream chat request failed:")
        || message.starts_with("upstream models request failed:")
        || message.starts_with("upstream completions request failed:")
    {
        return AttemptErrorClass::Connect;
    }
    // Timeout / stream-ended-before-first-chunk: BODY-FREE fixed markers from
    // `prefetch_first_chunk` / `stream_after_prefetch` — exact, no interpolation.
    if message == "upstream stream timed out" {
        return AttemptErrorClass::Timeout;
    }
    if message == "upstream stream ended before the first chunk" {
        return AttemptErrorClass::Stream;
    }
    // Non-2xx HTTP status: `"upstream chat failed with {status}: {body}"`. The `{body}`
    // is interpolated strictly AFTER this fixed prefix, so `starts_with` cannot be
    // influenced by body text (a body that itself contains "timed out" / "request failed"
    // is already excluded above because those branches require the message to START with /
    // EQUAL their own markers, which this variant never does). E2a: this ALSO catches a
    // request-intrinsic 4xx tagged `Terminal` — same message shape, same bounded code,
    // regardless of disposition (see the function doc comment).
    if message.starts_with("upstream chat failed with ") {
        return AttemptErrorClass::HttpStatus;
    }
    // First-chunk read/parse failure: `stream_success_response` builds these two FIXED
    // gateway prefixes — `"failed to parse upstream chat chunk: {err}; payload={body}"`
    // and `"failed to read upstream SSE: {err}"`. The interpolated tail (`{err}` =
    // serde/transport error, plus a redacted `{body}`) is strictly AFTER the prefix, so a
    // `starts_with` on the fixed prefix is immune to payload/body content (it never
    // reintroduces the F2 substring scan). These belong to the `Stream` taxonomy: the
    // response began but the first chunk could not be read/parsed.
    if message.starts_with("failed to parse upstream chat chunk:")
        || message.starts_with("failed to read upstream SSE:")
    {
        return AttemptErrorClass::Stream;
    }
    // Nothing above matched: a `Terminal` disposition reaching here is the OTHER
    // Terminal producer — a context-window overflow that survived shrink-and-retry
    // (its message never matches the `"upstream chat failed with "` prefix above, so it
    // falls through to here rather than being caught as `HttpStatus`).
    if err.failover_disposition() == FailoverDisposition::Terminal {
        return AttemptErrorClass::Terminal;
    }
    // Anything else is a generic gateway-side failover-eligible condition (cooldown,
    // "no models available", "all providers failed before producing a response"). We do
    // NOT consult `status_code()` as a fallback: every `AppError::upstream(...)` collapses
    // to a fixed 502, so a `(400..600)` status check would mislabel EVERY such generic
    // error as `HttpStatus` — the only true upstream HTTP-status case is the fixed-prefix
    // branch above. `Other` is the honest bounded code for these.
    AttemptErrorClass::Other
}

/// Per-provider serving status for the topology map (D4). `Cooling` while inside
/// the failure cooldown window; `Down` once a cooling provider has also crossed
/// [`DOWN_THRESHOLD`] consecutive failures; `Healthy` otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderStatus {
    Healthy,
    Cooling,
    Down,
}

/// Owned, serializable per-upstream health + counters for the dashboard topology
/// map (D4). Epoch-ms timestamps (never a monotonic `Instant`), so the DTO is
/// self-contained across the WS/REST boundary. Every field serializes
/// UNCONDITIONALLY (no `skip_serializing_if`) so the `Option` keys are always
/// present as JSON `null` — the frontend D9/D10/D12 model validates this exact
/// shape. `base_url` is REQUIRED (non-null).
#[derive(Debug, Clone, Serialize)]
pub struct ProviderHealth {
    /// Stable identifier for the provider (the configured provider/route name).
    pub id: String,
    /// Human-readable provider name (currently identical to `id`).
    pub name: String,
    /// The routing-provider name that owns this entry, when it is reached via a
    /// `RoutingUpstreamClient` (`None` for a bare/failover provider).
    pub route: Option<String>,
    /// The upstream base URL this provider POSTs to (REQUIRED, never null).
    pub base_url: String,
    pub status: ProviderStatus,
    /// Epoch-ms instant the cooldown window ends, when cooling (else `None`).
    pub cooling_until_ms: Option<u64>,
    /// The most recent failure message recorded for this provider (else `None`).
    pub last_error: Option<String>,
    /// Cumulative count of flows this provider served (produced a first chunk).
    pub served_count: u64,
    /// Cumulative count of times this provider was failed over FROM (a recorded
    /// `mark_failure`).
    pub failover_count: u64,
    /// Consecutive failures since the last success (reset to 0 on success).
    pub consecutive_failures: u32,
    /// Epoch-ms instant this provider's `/v1/models` catalog was last fetched
    /// (`None` until the first refresh; only the routing client populates it).
    pub catalog_fetched_ms: Option<u64>,
    /// Number of models in this provider's last catalog snapshot (`None` until
    /// the first refresh; only the routing client populates it).
    pub catalog_size: Option<u64>,
}

/// Cumulative per-provider serving counters held behind an `Arc` so the owning
/// upstream struct keeps its derived `Clone` (a bare atomic field would not be
/// `Clone`). `served_count` / `failover_count` are monotonic totals;
/// `consecutive_failures` is reset to 0 at `mark_provider_success` and bumped at
/// `mark_failure`. All three are plain atomics (lock-free reads for the snapshot).
#[derive(Debug, Default)]
pub struct ProviderMetrics {
    served_count: AtomicU64,
    failover_count: AtomicU64,
    consecutive_failures: AtomicU32,
}

impl ProviderMetrics {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a served flow (first chunk produced) and clear the consecutive-
    /// failure streak — the provider is healthy again. Mirrors the cooldown
    /// clear in `mark_provider_success`.
    fn record_success(&self) {
        self.served_count.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
    }

    /// Record a failover-away and bump the consecutive-failure streak.
    fn record_failure(&self) {
        self.failover_count.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn served_count(&self) -> u64 {
        self.served_count.load(Ordering::Relaxed)
    }

    fn failover_count(&self) -> u64 {
        self.failover_count.load(Ordering::Relaxed)
    }

    fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }
}

/// Immutable `(fetched_ms, size)` catalog metadata pair, swapped as a single
/// `Arc` inside `refresh_catalog` (under the existing `AsyncMutex` hold) so a
/// lock-free `provider_health()` reader can NEVER observe a torn pair — the two
/// fields always move together. `None`-valued `Arc` is the pre-first-refresh
/// state (the default `CatalogMeta { fetched_ms: None, size: None }`).
#[derive(Debug, Clone, Copy, Default)]
struct CatalogMeta {
    fetched_ms: Option<u64>,
    size: Option<u64>,
}

/// A versioned, immutable container for the published per-provider health vector
/// (D4). The `version` monotonically increments on each publication so a consumer
/// (D5's 5 s snapshot cut, D7b's `TopologyUpdate` broadcast) can cheaply detect a
/// change. Published on a coalesced 1 s tick AND a cooldown-deadline wake (so an
/// IDLE cooling→Healthy transition flips with no traffic); the atomics underneath
/// update continuously, and the snapshot reads them at publication time.
#[derive(Debug, Clone, Serialize)]
pub struct ProviderHealthSnapshot {
    pub version: u64,
    pub providers: Vec<ProviderHealth>,
}

impl ProviderHealthSnapshot {
    fn new(version: u64, providers: Vec<ProviderHealth>) -> Arc<Self> {
        Arc::new(Self { version, providers })
    }
}

/// The published-snapshot handle the publisher writes and consumers read (D4). A
/// `Mutex<Arc<…>>` gives an atomic `Arc` swap on write and a cheap `Arc` clone on
/// read (no `arc-swap` dependency); the held snapshot is immutable, so a reader's
/// clone is never mutated underneath it. The `version` counter lives here so each
/// publication bumps it monotonically. Cloning the publisher shares the inner
/// `Arc` (cheap), keeping the owning Gateway `Clone`.
#[derive(Clone)]
pub struct ProviderHealthPublisher {
    inner: Arc<Mutex<Arc<ProviderHealthSnapshot>>>,
    version: Arc<AtomicU64>,
}

impl Default for ProviderHealthPublisher {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProviderHealthSnapshot::new(0, Vec::new()))),
            version: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl ProviderHealthPublisher {
    /// Publish a fresh health vector as the next versioned snapshot. Bumps the
    /// monotonic version, builds an immutable `Arc<ProviderHealthSnapshot>`, and
    /// atomically swaps it in. Called by the coalesced 1 s publication tick and
    /// the cooldown-deadline wake.
    pub fn publish(&self, providers: Vec<ProviderHealth>) {
        let version = self.version.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot = ProviderHealthSnapshot::new(version, providers);
        *self
            .inner
            .lock()
            .expect("provider health publisher poisoned") = snapshot;
    }

    /// The latest published snapshot (a cheap `Arc` clone). The D5 snapshot task
    /// captures exactly this one `Arc`; D7b broadcasts it as a `TopologyUpdate`.
    pub fn latest(&self) -> Arc<ProviderHealthSnapshot> {
        Arc::clone(
            &self
                .inner
                .lock()
                .expect("provider health publisher poisoned"),
        )
    }
}

pub type UpstreamStream =
    Pin<Box<dyn Stream<Item = Result<ChatCompletionChunk, AppError>> + Send + 'static>>;

/// One entry of the upstream `/v1/models` catalog: the model id plus its
/// context-window length (`None` when the upstream reports no positive context
/// length for it). Ids and context limits are derived from a SINGLE
/// `/v1/models` snapshot so they always describe the same provider/state (G3:
/// a separate context-limit fetch could otherwise pair one provider's ids with
/// another's limits under failover).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamModelEntry {
    pub id: String,
    pub context_limit: Option<i64>,
}

const ROUTING_MODEL_CATALOG_TTL_SECS: u64 = 300;

/// One pre-first-chunk serving backend: its FINAL model id (after any
/// routing/route/exposed-alias + per-provider `upstream_model` rewrite — no
/// further remap) and the context-window length the upstream reports for it,
/// for G3 pre-flight budgeting (T9). `context_limit: None` ⇒ the upstream did
/// not report a window for this model (budgeting no-ops for it).
#[derive(Debug, Clone)]
pub struct BackendCandidate {
    pub model: String,
    pub context_limit: Option<i64>,
}

/// Typed backend-candidate plan: the routing/failover layer's answer to "which
/// backend models could serve this request pre-first-chunk, and what context
/// window does each report?" G4 native-vision gating consumes the candidate
/// MODELS instead of re-deriving the set in the engine (T2); G3 pre-flight
/// budgeting consumes the candidate CONTEXT LIMITS (conservative MIN — the
/// strictest window across the failover chain — so a failover to a smaller
/// model cannot overflow; unknown ⇒ no-op) instead of budgeting against the
/// pre-routing `resolved_model` (T9). The `genuine` signal — whether the
/// request model truly resolved vs. fell back to a catalog default — is
/// ENGINE-side (a byproduct of `normalize_upstream_model`, not a re-derived
/// ladder; see `backend_is_native_vision`).
///
/// Empty `candidates` ⇒ unknown candidate set (catalog-load failure): the gate
/// treats it as strip+offload, budgeting no-ops.
#[derive(Debug, Clone)]
pub struct BackendCandidatePlan {
    pub candidates: Vec<BackendCandidate>,
}

#[async_trait]
pub trait UpstreamClient: Send + Sync {
    async fn stream_chat_completion(
        &self,
        request: &BackendChatRequest,
    ) -> AppResult<UpstreamStream>;
    async fn stream_chat_completion_with_timeout(
        &self,
        request: &BackendChatRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        let stream = self.stream_chat_completion(request).await?;
        Ok(timeout_upstream_stream(stream, request_timeout))
    }
    async fn list_models(&self) -> AppResult<reqwest::Response>;
    async fn proxy_completions(
        &self,
        _headers: HeaderMap,
        _body: Bytes,
    ) -> AppResult<reqwest::Response> {
        Err(AppError::internal(
            "upstream completions proxy is not implemented",
        ))
    }
    /// Count the tokens for a fully lowered backend request. Routing and
    /// failover implementations rewrite the model through the same candidate
    /// path as generation before the leaf calls the optional `/tokenize` API.
    async fn count_tokens(&self, _request: &BackendChatRequest) -> AppResult<Option<u64>> {
        Ok(None)
    }
    /// The upstream model catalog (ids + per-model context length) from a single
    /// `/v1/models` snapshot. The default impl fetches `list_models()` once and
    /// parses both the id list and the context-window length per entry, so model
    /// routing/normalization and G3 pre-flight context budgeting always read a
    /// consistent provider state. Clients whose `list_models()` is unavailable
    /// surface the error; entries without a positive context length carry
    /// `context_limit: None` (budgeting no-ops for them).
    async fn supported_model_catalog(&self) -> AppResult<Vec<UpstreamModelEntry>> {
        let response = self.list_models().await?;
        collect_supported_model_catalog(response).await
    }

    /// Every backend model `requested_model` could ACTUALLY be served by
    /// pre-first-chunk, after any routing/route/exposed-alias + per-provider
    /// `upstream_model` rewrite (G4 review #2 + round-2 #1). This enumerates the
    /// full candidate set — the primary AND all eligible failover providers (and
    /// the routing target) — because failover happens before the first chunk, so
    /// the model that ultimately serves may be any of them. Native-vision gating
    /// uses the SAFE invariant over this set (passthrough only if EVERY candidate
    /// is native-vision), so it can never disagree with the provider that serves.
    ///
    /// Default impl: thin projection over
    /// [`backend_candidate_plan`](Self::backend_candidate_plan) — the single
    /// source of truth since T2. A single-provider passthrough sends the request
    /// model unchanged, so the only candidate is `requested_model` itself.
    async fn candidate_backend_models(&self, requested_model: &str) -> Vec<String> {
        self.backend_candidate_plan(requested_model)
            .await
            .candidates
            .into_iter()
            .map(|candidate| candidate.model)
            .collect()
    }

    /// Typed backend-candidate plan for `requested_model` — the routing/failover
    /// layer's candidate set (model + per-candidate context limit) for G4
    /// native-vision gating (T2) and G3 pre-flight budgeting (T9). The `genuine`
    /// signal is engine-side; this method returns only `candidates`.
    ///
    /// Default impl: a single-provider passthrough sends the request model
    /// unchanged, so the only candidate is `requested_model` itself with no
    /// known context limit (budgeting no-ops). Routing/failover clients override
    /// to fold in the failover chain + per-provider context limits.
    async fn backend_candidate_plan(&self, requested_model: &str) -> BackendCandidatePlan {
        BackendCandidatePlan {
            candidates: vec![BackendCandidate {
                model: requested_model.to_string(),
                context_limit: None,
            }],
        }
    }

    /// Per-upstream health + cumulative counters for the dashboard topology map
    /// (D4). A NON-async, dyn-safe default (mirrors `supported_model_catalog`'s
    /// default so `Arc<dyn UpstreamClient>` stays object-safe): the bare leaf and
    /// any client that owns no provider metrics return an EMPTY vector; the
    /// failover/routing clients override to report their providers' live status,
    /// cumulative counters, cooldown deadline, and (routing) catalog metadata.
    /// Reads are lock-free over the per-provider `Arc<ProviderMetrics>` /
    /// `Arc<CatalogMeta>` plus a short cooldown-state `Mutex` hold; safe to call
    /// from a synchronous publication tick.
    fn provider_health(&self) -> Vec<ProviderHealth> {
        Vec::new()
    }
}

#[derive(Debug, Clone)]
pub struct ReqwestUpstreamClient {
    client: reqwest::Client,
    base_url: Url,
    api_key: Option<String>,
    request_logger: Option<UpstreamRequestLogger>,
    flatten_content: bool,
    /// Floor for the shrink-and-retry completion budget on a context-window
    /// overflow (G1). A retry never reduces `max_completion_tokens` below this.
    min_completion_tokens: i64,
    /// Per-frame byte ceiling for the upstream SSE read path (G6 DoS guard).
    /// Bounds bytes accumulated between event boundaries so a hostile/buggy
    /// upstream cannot grow the parser buffer without bound; an oversized or
    /// unterminated frame is rejected as an `AppError`.
    max_sse_frame_bytes: usize,
    /// Per-backend-model finalization policies (effort map, `template_family`
    /// override, `upstream_chat_kwargs`), keyed by resolved model id. Applied at
    /// this leaf because it is the single point that sees the FINAL provider
    /// model after routing/failover/exposed-alias remap (T1). Shared (cheap
    /// clone) across all providers; empty when no profile defines any.
    finalization_policies: BackendFinalizationPolicies,
    /// D2 capture seam: the dashboard FlowStore handle (a cheap `Clone` sharing the
    /// inner `Arc<Mutex<_>>`). The leaf is the SINGLE point that sees the TRUE
    /// on-wire chat-completions body — POST `finalize_request_for_backend` +
    /// `sanitize_chat_request` (and, on the shrink path, the RETRY body) — so it
    /// captures `upstream_body` here via the capped/redacting serializer. Defaults
    /// to `disabled()` (every store op no-ops, zero overhead); the DI root threads
    /// the live store in via `with_flow_store` when the debug UI is on.
    flow_store: crate::dashboard_flow::DashboardFlowStore,
    /// D2 bare-leaf marker: `true` ONLY when this leaf is the engine's upstream
    /// DIRECTLY (lib.rs `Arc::new(primary_upstream)` — no routing/failover wrapper
    /// owns the `provider` serving field). Then the leaf synthesizes
    /// `provider = "primary"`. A leaf nested INSIDE a failover/routing client leaves
    /// this `false` so it never clobbers the real provider name the wrapper records
    /// on first-chunk success (the leaf runs BEFORE that success, so a
    /// first-writer-wins tag here would otherwise win over the true provider).
    tag_primary_provider: bool,
}

#[derive(Debug, Clone)]
pub struct FailoverUpstreamProvider {
    name: String,
    client: ReqwestUpstreamClient,
    upstream_model: Option<String>,
    exposed_model: Option<String>,
    upstream_chat_kwargs: JsonMap<String, Value>,
    /// D4 cumulative serving counters. Behind an `Arc` so this struct keeps its
    /// derived `Clone` (a bare atomic field would not be `Clone`) and so the
    /// same counters survive the routing/failover REBUILD that clones the
    /// provider (the rebuild clones the `Arc`, not the counters).
    metrics: Arc<ProviderMetrics>,
}

impl FailoverUpstreamProvider {
    pub fn new(
        name: impl Into<String>,
        client: ReqwestUpstreamClient,
        upstream_model: Option<String>,
        exposed_model: Option<String>,
        upstream_chat_kwargs: JsonMap<String, Value>,
    ) -> Self {
        Self {
            name: name.into(),
            client,
            upstream_model,
            exposed_model,
            upstream_chat_kwargs,
            metrics: ProviderMetrics::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FailoverUpstreamClient {
    providers: Vec<FailoverUpstreamProvider>,
    cooldown: Duration,
    states: Arc<Mutex<Vec<ProviderCooldownState>>>,
}

#[derive(Debug, Clone)]
pub struct RoutingUpstreamProvider {
    name: String,
    primary_client: ReqwestUpstreamClient,
    primary_upstream_model: Option<String>,
    fallback_exposed_models: Vec<RoutingFallbackExposedModel>,
    client: FailoverUpstreamClient,
}

impl RoutingUpstreamProvider {
    pub fn new(
        name: impl Into<String>,
        primary_client: ReqwestUpstreamClient,
        primary_upstream_model: Option<String>,
        primary_upstream_chat_kwargs: JsonMap<String, Value>,
        fallback_providers: Vec<FailoverUpstreamProvider>,
        cooldown: Duration,
    ) -> Self {
        let name = name.into();
        let mut providers = vec![FailoverUpstreamProvider::new(
            name.clone(),
            primary_client.clone(),
            primary_upstream_model.clone(),
            None,
            primary_upstream_chat_kwargs,
        )];
        let fallback_exposed_models = fallback_providers
            .iter()
            .enumerate()
            .filter_map(|(index, provider)| {
                provider
                    .exposed_model
                    .clone()
                    .map(|model_id| RoutingFallbackExposedModel {
                        model_id,
                        failover_provider_index: index + 1,
                    })
            })
            .collect();
        providers.extend(fallback_providers);
        Self {
            name,
            primary_client,
            primary_upstream_model,
            fallback_exposed_models,
            client: FailoverUpstreamClient::new(providers, cooldown),
        }
    }

    /// The effective backend model of one of this routing provider's nested
    /// failover providers (its `upstream_model` rewrite, if any). Used by G4
    /// native-vision gating for an exposed-alias/fallback target that serves from
    /// exactly one provider. `None` when the index is out of range or the
    /// provider sends the request model through unchanged.
    fn failover_provider_model(&self, failover_provider_index: usize) -> Option<String> {
        self.client.provider_upstream_model(failover_provider_index)
    }
}

/// A synthetic upstream backing one or more ad-hoc model routes (G7). Unlike a
/// catalog provider, a route provider is matched by request-model *name* (in
/// `ModelRouteSpec`), never enumerated into the `/v1/models` union, so routes
/// stay invisible to the model listing exactly like claude-relay's
/// `model_routes`.
#[derive(Debug, Clone)]
pub struct RouteUpstreamProvider {
    name: String,
    client: FailoverUpstreamClient,
}

impl RouteUpstreamProvider {
    pub fn new(name: impl Into<String>, client: ReqwestUpstreamClient, cooldown: Duration) -> Self {
        let name = name.into();
        Self {
            name: name.clone(),
            client: FailoverUpstreamClient::new(
                vec![FailoverUpstreamProvider::new(
                    name,
                    client,
                    None,
                    None,
                    JsonMap::new(),
                )],
                cooldown,
            ),
        }
    }
}

/// A compiled ad-hoc model route (G7): a request-model name/glob that maps to a
/// `RouteUpstreamProvider` and an optional upstream-model rewrite.
#[derive(Debug, Clone)]
pub struct ModelRouteSpec {
    /// Request-model name this route matches (literal or glob source).
    name: String,
    /// Compiled glob matcher (case-insensitive). `None` for a literal name,
    /// which is compared with `eq_ignore_ascii_case`.
    glob: Option<Regex>,
    /// Index into `RoutingUpstreamClient::route_providers`.
    route_provider_index: usize,
    /// Upstream model to send. `None` passes the request model through.
    upstream_model: Option<String>,
}

impl ModelRouteSpec {
    /// Build a route spec. `glob` is the pre-compiled matcher (from
    /// `config::ModelRoute`); `route_provider_index` indexes the matching
    /// `RouteUpstreamProvider`.
    pub fn new(
        name: impl Into<String>,
        glob: Option<Regex>,
        route_provider_index: usize,
        upstream_model: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            glob,
            route_provider_index,
            upstream_model,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoutingUpstreamClient {
    providers: Vec<RoutingUpstreamProvider>,
    /// Synthetic providers backing ad-hoc model routes (G7), indexed by
    /// `ModelRouteSpec::route_provider_index`. Kept separate from catalog
    /// `providers` so routes never enter the `/v1/models` union.
    route_providers: Vec<RouteUpstreamProvider>,
    routes: Vec<ModelRouteSpec>,
    catalog: Arc<AsyncMutex<Option<CachedRoutingModelCatalog>>>,
    /// D4 catalog metadata `(fetched_ms, size)` published for the topology map.
    /// Swapped as a SINGLE immutable `Arc<CatalogMeta>` inside `refresh_catalog`
    /// (under the `catalog` `AsyncMutex` hold) so a lock-free `provider_health()`
    /// reader can never observe a torn `(fetched_ms, size)` pair — the two fields
    /// always move together. Default `Arc<CatalogMeta>` (both `None`) until the
    /// first refresh.
    catalog_meta: Arc<Mutex<Arc<CatalogMeta>>>,
}

#[derive(Debug, Clone)]
struct CachedRoutingModelCatalog {
    fetched_at: Instant,
    catalog: RoutingModelCatalog,
}

#[derive(Debug, Clone, Default)]
struct RoutingModelCatalog {
    provider_catalogs: Vec<RoutingProviderModelCatalog>,
    union_entries: Vec<Value>,
    union_ids: Vec<String>,
    /// Context-window length keyed by union model id, parsed from the SAME
    /// first-seen `/v1/models` entry that populated `union_entries`/`union_ids`.
    /// `supported_model_catalog` reads it directly so the union catalog is parsed
    /// once at refresh rather than reparsed from `union_entries` per call.
    union_context_limit_by_id: HashMap<String, i64>,
    ids_by_key: HashMap<String, Vec<RoutingModelCandidate>>,
    /// Ad-hoc routes (G7), cloned from the client each refresh. Matched purely
    /// by request-model name, independent of the live catalog, so routes still
    /// resolve when an upstream `/v1/models` fetch is unavailable.
    routes: Vec<ModelRouteSpec>,
}

#[derive(Debug, Clone, Default)]
struct RoutingProviderModelCatalog {
    candidates: Vec<RoutingModelCandidate>,
    /// Per-model context-window length for this provider's catalog, keyed by
    /// model id (parsed from the SAME `/v1/models` snapshot as `candidates`).
    /// G3 pre-flight budgeting reads this via `backend_candidate_plan` so it
    /// budgets against the REAL per-provider window (T9), not the pre-routing
    /// union default.
    context_limit_by_id: HashMap<String, i64>,
}

#[derive(Debug, Clone)]
struct RoutingModelCandidate {
    provider_index: usize,
    model_id: String,
    target: RoutingModelTarget,
}

/// Outcome of resolving a request model: either a catalog candidate (served by a
/// `RoutingUpstreamProvider`) or an ad-hoc route candidate (served by a
/// `RouteUpstreamProvider`). Routes slot strictly between an exact catalog id
/// and the canonical-key/default fallbacks (G7), so an exact model id always
/// beats a glob route.
#[derive(Debug, Clone)]
enum RoutingResolution {
    Catalog(RoutingModelCandidate),
    Route {
        route_provider_index: usize,
        model_id: String,
    },
}

#[derive(Debug, Clone)]
enum RoutingModelTarget {
    Primary,
    Fallback { failover_provider_index: usize },
}

/// Which `RoutingModelCatalog::resolve` rule matched a request model. Carried
/// out to the dispatch site purely for diagnostics: a `Default` match means the
/// requested model was NOT served by any upstream and we fell back to the first
/// catalog model — worth a WARN so an operator notices a model-name mismatch
/// (e.g. the loaded vLLM model differs from what clients request). The rest are
/// expected and log at INFO or below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchKind {
    /// Rule 1: exact catalog model id.
    ExactId,
    /// Rules 2-3: ad-hoc route name or glob.
    Route,
    /// Rule 4: unique canonical-key catalog match (case/punctuation normalized).
    CanonicalKey,
    /// Rule 5: no match; fell back to the first catalog model.
    Default,
}

#[derive(Debug, Clone)]
struct RoutingFallbackExposedModel {
    model_id: String,
    failover_provider_index: usize,
}

#[derive(Debug, Clone, Default)]
struct ProviderCooldownState {
    cooling_until: Option<Instant>,
    last_error: Option<String>,
}

/// Cheap case-insensitive scan for a potential image-URI marker (`data:` /
/// `http`) in serialized request bytes, so the log fast-path only pays the
/// redaction round-trip when an image URL might be present (G4 round-4 #3).
fn bytes_contain_image_uri(bytes: &[u8]) -> bool {
    bytes.windows(5).any(|w| w.eq_ignore_ascii_case(b"data:"))
        || bytes.windows(4).any(|w| w.eq_ignore_ascii_case(b"http"))
}

/// F1d: the redacted bytes for the turn-capture `upstream_request` section — the
/// FINAL sanitized on-wire `ChatCompletionRequest` (post profile/lowering/
/// sanitize), redacted through the SAME primitives `UpstreamRequestLogger`/the
/// HTTP inbound-trace capture use: secret keys via
/// [`crate::redaction::redact_payload_secrets_in_value`] (AGENTS.md line 137 — a
/// NEW logged surface must not bypass secret redaction; `UpstreamRequestLogger`
/// only redacts image URIs, so this is a STRICTER pass, not a mirror of it),
/// image/data URIs via [`crate::redaction::redact_image_uris_in_value`].
/// Serializes into a fresh owned `Vec<u8>` — never retains a slice of any larger
/// buffer (AGENTS.md line 144); `request` is already an owned, parsed typed
/// struct, not a slice of the inbound middleware buffer.
fn redacted_upstream_request_bytes(request: &ChatCompletionRequest) -> Vec<u8> {
    match serde_json::to_value(request) {
        Ok(mut value) => {
            crate::redaction::redact_payload_secrets_in_value(&mut value);
            crate::redaction::redact_image_uris_in_value(&mut value);
            serde_json::to_vec(&value).unwrap_or_else(|_| b"<failed to serialize json>".to_vec())
        }
        Err(_) => b"<failed to serialize json>".to_vec(),
    }
}

/// Fable review (Finding 1): requests whose message-content text is at or below this
/// size have their `upstream_request` redaction done INLINE; a larger request moves
/// the serialize+redact+re-serialize onto the blocking pool so it never stalls the
/// tokio worker (mirrors [`UpstreamRequestLogger`]'s `spawn_blocking`). Matches the
/// 16 KiB HTTP journal dump gate (`http::API_LOG_PAYLOAD_DUMP_LIMIT_BYTES`).
const TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES: usize = 16 * 1024;

/// A cheap, allocation-free size estimate for the inline-vs-`spawn_blocking` decision
/// on the `upstream_request` redaction: the total text length of the message
/// contents, which dominate a large Claude Code (1M-context) request. An
/// under-estimate only keeps an unusual request inline; a large message history (the
/// finding's motivating case) is always well over the limit. It does NOT allocate or
/// serialize (unlike [`estimate_leaf_input_tokens`]), so it is safe to run on the
/// worker every dispatch.
fn estimate_request_text_len(request: &ChatCompletionRequest) -> usize {
    fn value_text_len(value: &Value) -> usize {
        match value {
            Value::String(s) => s.len(),
            Value::Array(items) => items.iter().map(value_text_len).sum(),
            Value::Object(map) => map.values().map(value_text_len).sum(),
            _ => 0,
        }
    }
    request
        .messages
        .iter()
        .map(|message| message.content.as_ref().map_or(0, value_text_len))
        .sum()
}

/// Fable review (Finding 1): produce the redacted `upstream_request` bytes, moving the
/// serialize+redact+re-serialize OFF the tokio worker for a LARGE request via
/// `spawn_blocking` (mirroring [`UpstreamRequestLogger`]). A small request redacts
/// inline; a large one is CLONED once (the only on-worker O(body) cost — a struct
/// clone is far cheaper than the alloc-heavy `to_value`+walk+`to_vec` it offloads) and
/// redacted on the blocking pool. Redaction is IDENTICAL either way (secret + image
/// URI, via [`redacted_upstream_request_bytes`]). The caller AWAITS this INLINE, BEFORE
/// the dispatch send and BEFORE `write_upstream_request`, so the last-writer-wins
/// `replace` lands well before the finalize barrier reads the section (no race, no
/// hang). `None` ONLY on a `spawn_blocking` join failure (runtime shutdown) — the
/// caller then leaves the section ABSENT (don't-lie-with-zeros), never a panic.
async fn offload_redacted_upstream_request_bytes(
    request: &ChatCompletionRequest,
) -> Option<Vec<u8>> {
    if estimate_request_text_len(request) <= TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES {
        return Some(redacted_upstream_request_bytes(request));
    }
    let owned = request.clone();
    match tokio::task::spawn_blocking(move || redacted_upstream_request_bytes(&owned)).await {
        Ok(bytes) => Some(bytes),
        Err(err) => {
            tracing::warn!(error = %err, "turn-capture: upstream_request redaction task failed");
            None
        }
    }
}

#[derive(Debug, Clone)]
struct UpstreamRequestLogger {
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl UpstreamRequestLogger {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    async fn log(&self, request: &ChatCompletionRequest) -> std::io::Result<()> {
        // G4 round-4 #3: the JSONL request log is written to DISK and would
        // otherwise serialize raw `data:` image bytes / signed `image_url`s for
        // native-vision-passthrough / disabled-agent / missing-url /
        // tool_choice:"none" image requests (any path that does NOT strip). The
        // common no-image request serializes directly (format unchanged); only
        // when the serialized bytes contain an image-URI marker do we re-redact
        // via a CLONED JSON value through the shared redactor, so the on-disk log
        // never carries request image content.
        let mut payload = serde_json::to_vec(request).map_err(std::io::Error::other)?;
        if bytes_contain_image_uri(&payload) {
            let mut value = serde_json::to_value(request).map_err(std::io::Error::other)?;
            crate::redaction::redact_image_uris_in_value(&mut value);
            payload = serde_json::to_vec(&value).map_err(std::io::Error::other)?;
        }
        payload.push(b'\n');
        let path = self.path.clone();
        let write_lock = self.write_lock.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = write_lock.lock().map_err(|err| {
                std::io::Error::other(format!("request log lock poisoned: {err}"))
            })?;
            let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
            file.write_all(&payload)
        })
        .await
        .map_err(|err| std::io::Error::other(format!("spawn_blocking failed: {err}")))?
    }
}

impl ReqwestUpstreamClient {
    pub fn new(
        client: reqwest::Client,
        base_url: Url,
        api_key: Option<String>,
        request_log_path: Option<PathBuf>,
        flatten_content: bool,
        min_completion_tokens: i64,
    ) -> Self {
        Self::with_options(
            client,
            base_url,
            api_key,
            request_log_path,
            flatten_content,
            min_completion_tokens,
            default_max_sse_frame_bytes(),
        )
    }

    /// Construct with an explicit upstream SSE per-frame cap (G6). The simpler
    /// `new` delegates here with the default ceiling; callers that need to
    /// thread a configured/lowered cap (e.g. the DI root or a test) use this.
    #[allow(clippy::too_many_arguments)]
    pub fn with_options(
        client: reqwest::Client,
        base_url: Url,
        api_key: Option<String>,
        request_log_path: Option<PathBuf>,
        flatten_content: bool,
        min_completion_tokens: i64,
        max_sse_frame_bytes: usize,
    ) -> Self {
        Self {
            client,
            base_url,
            api_key,
            request_logger: request_log_path.map(UpstreamRequestLogger::new),
            flatten_content,
            min_completion_tokens: min_completion_tokens.max(1),
            max_sse_frame_bytes: max_sse_frame_bytes.max(1024),
            finalization_policies: BackendFinalizationPolicies::default(),
            // Default to the no-op store (D2); the DI root threads the live one in
            // via `with_flow_store` only when the debug UI is enabled.
            flow_store: crate::dashboard_flow::DashboardFlowStore::disabled(),
            // Default OFF: a leaf only tags `provider = "primary"` when the DI root
            // marks it the bare/direct engine upstream via `into_bare_primary`.
            tag_primary_provider: false,
        }
    }

    /// Attach the per-backend-model finalization policies (effort map,
    /// `template_family` override, `upstream_chat_kwargs`; built once from
    /// config). Threaded post-construction so the `with_options` signature is
    /// unchanged.
    pub(crate) fn with_finalization_policies(
        mut self,
        policies: BackendFinalizationPolicies,
    ) -> Self {
        self.finalization_policies = policies;
        self
    }

    /// Attach the dashboard FlowStore handle (D2). Threaded post-construction (like
    /// `with_finalization_policies`) so the `with_options` signature is unchanged;
    /// the handle is a cheap `Clone` of the shared store (or `disabled()`). Once
    /// attached, the leaf captures the POST-`sanitize` on-wire upstream body into
    /// the flow's record keyed by its `response_id`.
    pub(crate) fn with_flow_store(
        mut self,
        flow_store: crate::dashboard_flow::DashboardFlowStore,
    ) -> Self {
        self.flow_store = flow_store;
        self
    }

    /// Mark this leaf the BARE/direct engine upstream (D2): the DI root calls this
    /// ONLY for the single-upstream `Arc::new(primary_upstream)` path, where no
    /// routing/failover layer owns the `provider` serving field. Then the leaf
    /// synthesizes `provider = "primary"`. Leaves nested inside a failover/routing
    /// client never call this, so they don't clobber the real provider name.
    pub(crate) fn into_bare_primary(mut self) -> Self {
        self.tag_primary_provider = true;
        self
    }

    /// The configured upstream base URL as a string, for the D4 `ProviderHealth`
    /// DTO (a REQUIRED, non-null field). The leaf is the single owner of the
    /// concrete `base_url`, so the failover/routing layers read it from here.
    fn base_url_string(&self) -> String {
        self.base_url.to_string()
    }

    fn with_auth(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.api_key {
            Some(api_key) => request.bearer_auth(api_key),
            None => request,
        }
    }

    fn endpoint_url(&self, path: &str) -> AppResult<Url> {
        let mut url = self.base_url.clone();
        if !url.path().ends_with('/') {
            let new_path = format!("{}/", url.path());
            url.set_path(&new_path);
        }
        url.join(path)
            .map_err(|err| AppError::internal(format!("invalid upstream URL: {err}")))
    }

    /// vLLM/SGLang expose `/tokenize` at the server root rather than below
    /// their OpenAI-compatible `/v1` prefix.
    fn tokenize_url(&self) -> Url {
        let mut segments: Vec<String> = self
            .base_url
            .path_segments()
            .map(|parts| {
                parts
                    .filter(|segment| !segment.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if segments.last().map(String::as_str) == Some("v1") {
            segments.pop();
        }
        segments.push("tokenize".to_string());
        let mut url = self.base_url.clone();
        url.set_path(&format!("/{}", segments.join("/")));
        url
    }

    async fn send_chat_request(
        &self,
        url: &Url,
        request: &ChatCompletionRequest,
    ) -> AppResult<reqwest::Response> {
        self.with_auth(self.client.post(url.clone()))
            .json(request)
            .send()
            .await
            .map_err(|err| AppError::upstream(format!("upstream chat request failed: {err}")))
    }

    /// Append `request` to the JSONL request log (when configured), capture the
    /// TRUE on-wire upstream body into the dashboard FlowStore (D2), then POST it.
    /// Every actual upstream chat POST goes through here so BOTH the JSONL log and
    /// the dashboard inspector record the original request AND the G1 shrink-and-
    /// retry — otherwise the retry's reduced budget would be invisible. A log-write
    /// failure is logged and the POST proceeds (logging is observability, not a hard
    /// dependency of serving the request).
    ///
    /// `response_id` keys the FlowStore capture; the leaf only knows the flow's
    /// `resp_{uuid}` (threaded on `BackendChatRequest`), and the store joins it to
    /// the owning `api_call_id` via its link index. The captured body is THIS
    /// `request` — the POST-`finalize_request_for_backend` + `sanitize_chat_request`
    /// (and, on the retry call, the shrunk) request — i.e. exactly the bytes
    /// `send_chat_request` serializes onto the wire, copied through the capped +
    /// redacting serializer so no oversized/secret body is retained (a slice of the
    /// inbound middleware buffer bounded by `max_request_body_bytes` is NEVER taken — this is a fresh serialization of the
    /// already-parsed typed request).
    ///
    /// F1d: `capture` (the turn's `Topic F` durable-capture handle, from
    /// `BackendChatRequest::capture`) gets the SAME `request` written, whole, into
    /// its `upstream_request` section — last-writer-wins, so the retry call
    /// overwrites the first oversized attempt's write (AC-10). `None` when capture
    /// is disabled/uninstrumented; a no-op then.
    async fn logged_send_chat_request(
        &self,
        url: &Url,
        request: &ChatCompletionRequest,
        response_id: Option<&str>,
        capture: Option<&Arc<TurnCaptureState>>,
    ) -> AppResult<reqwest::Response> {
        if let Some(ref logger) = self.request_logger
            && let Err(err) = logger.log(request).await
        {
            tracing::warn!(
                path = %logger.path.display(),
                error = %err,
                "failed to append upstream request log"
            );
        }
        self.capture_upstream_body(response_id, request);
        // Finding 1: redact the `upstream_request` section OFF the tokio worker for
        // large requests (spawn_blocking), AWAITED here — before the send and before
        // `write_upstream_request` — so the last-writer-wins `replace` lands well
        // before the finalize barrier reads the section (no race).
        if let Some(capture) = capture {
            // F2 (Fable-fix): on a `spawn_blocking` join FAILURE (runtime shutdown) for
            // THIS attempt, DON'T silently retain a PRIOR attempt's bytes as the FINAL
            // (authoritative) request — that breaks last-writer-wins and lies about
            // which attempt served. Replace with an honest partial marker for THIS
            // attempt instead (don't-lie-with-zeros); normal success is last-writer-wins.
            match offload_redacted_upstream_request_bytes(request).await {
                Some(redacted) => capture.write_upstream_request(&redacted),
                None => capture.mark_upstream_request_redaction_failed(),
            }
        }
        self.send_chat_request(url, request).await
    }

    /// D2 capture: store the on-wire upstream body for `response_id` through the
    /// capped, redacting serializer. No-op when the store is `disabled()` or no
    /// `response_id` is threaded (tests / non-engine paths). Serializes the typed
    /// request DIRECTLY into a writer bounded at `2 × BODY_CAP` (D2 R1 #1 — never a
    /// full `serde_json::to_vec`, which for a multi-MiB prompt would allocate an
    /// O(body) buffer before capping); only the capped `Arc<[u8]>` (≤ `BODY_CAP`) is
    /// retained. A request whose serialized form overflows the bound is recorded as
    /// the fixed `[redacted: unparseable body]` marker (observability-only).
    fn capture_upstream_body(&self, response_id: Option<&str>, request: &ChatCompletionRequest) {
        if !self.flow_store.is_enabled() {
            return;
        }
        let Some(response_id) = response_id else {
            return;
        };
        let captured = crate::dashboard_flow::capture_body_from_value(request);
        // D5 R1 #4: thread the FINALIZED served model into the FlowStore record. This
        // leaf is the single point that sees the post-routing/failover/alias-remap +
        // post-`sanitize_chat_request` `request.model`, i.e. the model the backend
        // actually answered as. Without it the record's `model_served` stays `None`
        // and every metrics bucket collapses to the `"unknown"` model. The store
        // `cap_scalar`-bounds it; `set_upstream` only overwrites when `Some`.
        self.flow_store.set_upstream(
            response_id,
            None,
            Some(request.model.clone()),
            Some(captured),
        );
    }

    /// Gap 05 capture: STAGE the upstream RESPONSE/ERROR `body` of a FAILED attempt as the
    /// turn's pending body on the shared `ServingToken`, copied through the capped +
    /// redacting + truncation-flagging serializer. No-op when the SEPARATE upstream-response
    /// capture gate is off (debug UI off OR the `LLMCONDUIT_DASHBOARD_CAPTURE_UPSTREAM_RESPONSE`
    /// env flag unset) or no `serving` token is threaded — so production and a
    /// dashboard-without-the-gate do no work and retain nothing. `body` is the already-read
    /// upstream error text (a `String`/`&str`, NOT the inbound middleware buffer bounded by `max_request_body_bytes`); it
    /// is COPIED through the capped serializer, so no `Bytes` slice of that buffer is retained.
    ///
    /// Round-1 review (F1): the body is STAGED on the token, NOT committed straight onto the
    /// FlowStore record. The L1 telemetry guard commits the token's pending body at finalize,
    /// AFTER the failover layer has decided the turn's FINAL outcome. Round-2 (HIGH): the
    /// failover loop (and the bare-leaf dispatch) CLEARS the staged body at the START of each
    /// attempt, so the record's `upstream_response` reflects the FINAL attempt's outcome — an
    /// HTTP-status failure stages here and commits its body; a body-less final failure
    /// (connect/timeout/prefetch-stream-error) never reaches this and commits `None`; a
    /// provider that later SERVES also clears it (`clear_pending_response_body`). So A 500 →
    /// B 200 ⇒ `None`; A 500 → B 503 ⇒ B's body; A 500 → B transport/no-first-chunk ⇒ `None`.
    /// Within ONE attempt this is still last-writer-wins (the shrink-and-retry's body replaces
    /// the first send's). The redaction is consistent with the existing request-capture
    /// surface; this is a diagnostic body shown to the authenticated operator, captured ONLY
    /// when explicitly opted in.
    fn capture_upstream_response_body(
        &self,
        serving: Option<&Arc<ServingToken>>,
        capture: Option<&Arc<TurnCaptureState>>,
        body: &str,
    ) {
        // F1e: STAGE the failed HTTP body on the turn-capture handle -- gated ONLY
        // on capture being enabled (independent of the dashboard response-capture
        // flag, so durable capture works without the debug UI). Image-URI redaction
        // only (responses carry no header secrets; keep it cheap + consistent, and
        // it handles non-JSON error bodies). Last-writer-wins within an attempt; the
        // per-attempt clear at dispatch start + the streamed-success discriminator
        // make the committed body the FINAL attempt's (gap-05 semantics).
        if let Some(capture) = capture {
            let redacted = crate::redaction::redact_image_uris(body).into_bytes();
            capture.stage_upstream_response_body(&redacted);
        }
        // gap-05 dashboard staging (separately gated on the response-capture flag).
        if !self.flow_store.is_response_capture_enabled() {
            return;
        }
        let Some(serving) = serving else {
            return;
        };
        let captured = crate::dashboard_flow::capture_response_body(body.as_bytes());
        serving.set_pending_response_body(captured);
    }

    /// Gap 03 (bare-leaf path): wrap a successfully-dispatched upstream stream so the
    /// flow records EXACTLY ONE served [`Attempt`](crate::dashboard_flow::Attempt). Round-1
    /// review (F1): `first_upstream_byte_ms` is `header_byte_ms` — the wire instant the
    /// upstream RESPONSE HEADERS arrived (captured by `dispatch_chat_stream` the moment
    /// `send().await` returned), NOT the later instant the first parsed SSE chunk is
    /// yielded. The attempt is still RECORDED at first-chunk yield (so a stream that yields
    /// NO item becomes a FAILED attempt and exactly one attempt is recorded), but the
    /// measured wire byte time is the header time. Because the dispatch succeeded, headers
    /// arrived, so `header_byte_ms` is `Some` on this path; it is threaded as the byte time
    /// for the served chunk AND the "stream produced nothing" `Stream`-class failure
    /// (headers DID arrive — only the first chunk did not). Pure pass-through of every
    /// chunk afterwards — it never buffers or duplicates tokens (AGENTS.md
    /// failover-only-pre-first-chunk is untouched: this records, it does not retry). No-op
    /// wrapper when no token is threaded.
    fn record_served_attempt_on_first_byte(
        serving: Option<Arc<ServingToken>>,
        mut stream: UpstreamStream,
        start_ms: u128,
        model: String,
        header_byte_ms: Option<u128>,
    ) -> UpstreamStream {
        let Some(serving) = serving else {
            return stream;
        };
        Box::pin(async_stream::stream! {
            let mut first_byte_recorded = false;
            while let Some(item) = stream.next().await {
                if !first_byte_recorded {
                    first_byte_recorded = true;
                    // The first item arrived. A transport/parse error in the FIRST item is
                    // still "no usable first chunk" → a FAILED attempt; a chunk → a SERVED
                    // attempt. Either way exactly one attempt is recorded, and its wire
                    // first-byte is the HEADER time (F1), never the chunk-yield time.
                    match &item {
                        Ok(_) => serving.record_attempt(crate::dashboard_flow::Attempt {
                            provider: Some("primary".to_string()),
                            model: Some(model.clone()),
                            start_ms,
                            end_ms: now_epoch_ms_u128(),
                            first_upstream_byte_ms: header_byte_ms,
                            status: crate::dashboard_flow::AttemptStatus::Served,
                            error_class: None,
                            failover_reason: None,
                        }),
                        Err(err) => serving.record_attempt(Self::failed_bare_attempt(
                            start_ms,
                            model.clone(),
                            header_byte_ms,
                            err,
                        )),
                    }
                }
                yield item;
            }
            if !first_byte_recorded {
                // The stream completed without yielding a single item: no first CHUNK
                // arrived, but the response HEADERS did (the dispatch succeeded) — so the
                // wire first-byte is the header time (F1), and this is a `Stream`-class
                // FAILED attempt.
                serving.record_attempt(crate::dashboard_flow::Attempt {
                    provider: Some("primary".to_string()),
                    model: Some(model.clone()),
                    start_ms,
                    end_ms: now_epoch_ms_u128(),
                    first_upstream_byte_ms: header_byte_ms,
                    status: crate::dashboard_flow::AttemptStatus::Failed,
                    error_class: Some(crate::dashboard_flow::AttemptErrorClass::Stream),
                    failover_reason: None,
                });
            }
        })
    }

    /// Gap 03: build a FAILED bare-leaf [`Attempt`](crate::dashboard_flow::Attempt) (the
    /// dispatch errored before producing a stream, OR the first stream item was an error).
    /// Round-1 review (F1): `first_upstream_byte_ms` is `header_byte_ms` — `Some` (the wire
    /// header time) for an HTTP-status failure whose response headers arrived, `None` for a
    /// connect/timeout-before-response. The bounded taxonomic `error_class`/`failover_reason`
    /// come from `err` — never raw upstream text.
    fn failed_bare_attempt(
        start_ms: u128,
        model: String,
        header_byte_ms: Option<u128>,
        err: &AppError,
    ) -> crate::dashboard_flow::Attempt {
        use crate::dashboard_flow::AttemptFailoverReason;
        use crate::dashboard_flow::AttemptStatus;
        use crate::error::FailoverDisposition;
        let failover_reason = match err.failover_disposition() {
            FailoverDisposition::Terminal => AttemptFailoverReason::TerminalNoFailover,
            FailoverDisposition::FailoverNoCooldown => AttemptFailoverReason::RequestRejected,
            FailoverDisposition::Failover => AttemptFailoverReason::ProviderFailed,
        };
        crate::dashboard_flow::Attempt {
            provider: Some("primary".to_string()),
            model: Some(model),
            start_ms,
            end_ms: now_epoch_ms_u128(),
            first_upstream_byte_ms: header_byte_ms,
            status: AttemptStatus::Failed,
            error_class: Some(classify_attempt_error(err)),
            failover_reason: Some(failover_reason),
        }
    }

    /// The leaf's actual chat-completions dispatch: POST the finalized + sanitized
    /// `request`, with the G1 context-overflow shrink-and-retry. Split out of
    /// [`stream_chat_completion`](Self::stream_chat_completion) so the bare-leaf path can
    /// wrap the result for gap-03 attempt recording WITHOUT duplicating the recording
    /// across this method's several return sites. `response_id` keys the D2 on-wire body
    /// capture (already performed by `logged_send_chat_request`).
    async fn dispatch_chat_stream(
        &self,
        url: &Url,
        request: ChatCompletionRequest,
        response_id: Option<&str>,
        serving: Option<&Arc<ServingToken>>,
        capture: Option<&Arc<TurnCaptureState>>,
    ) -> AppResult<UpstreamStream> {
        // F1e review r1/r2: per-attempt reset of the `upstream_response` capture at
        // the START of this dispatch attempt, so the turn's raw response is
        // FINAL-ATTEMPT-ONLY (mirrors the gap-05 `clear_pending_response_body()`
        // seam). ONE call now does BOTH: swap in a fresh, empty streamed-section AND
        // drop any staged FINAL failed body -- so a SUPERSEDED earlier prefetch
        // attempt (2xx headers → some raw bytes → its tap guard closed the section
        // sticky-`partial`, then failover) and an earlier provider's staged error
        // body both get DISCARDED; only the serving/final attempt wins (last-writer-
        // wins). The reset is SYNCHRONOUS + await-free (review r2 BLOCKING): this seam
        // runs inside the engine's cancellable per-round `select!`, so a cancel/hang-
        // up must never drop the reset mid-swap and leave the superseded attempt
        // authoritative -- the swap + clears are one atomic critical section.
        if let Some(capture) = capture {
            capture.reset_upstream_response();
        }
        // First attempt. On a non-2xx whose body indicates a context/completion
        // token-limit overflow, shrink `max_completion_tokens` and retry ONCE.
        // This happens before any SSE chunk is parsed/yielded downstream, so it
        // can never duplicate already-streamed tokens, and it stays inside the
        // leaf client so the failover/routing layers never see a context-limit
        // error as a provider failure (it is a same-provider shrink-and-retry).
        let response = self
            .logged_send_chat_request(url, &request, response_id, capture)
            .await?;
        // Gap 03 round-1 review (F1): the upstream response HEADERS just arrived — this is
        // the TRUE on-wire first-byte time. Stamp it BEFORE inspecting the status, so a
        // non-2xx (HTTP-status failure) attempt carries a measured wire byte time too, not
        // `None`. A connect/timeout-before-response never reaches here (`?` above
        // propagated the transport error), so the slot stays `None` for those.
        stamp_header_byte(serving);
        let status = response.status();
        if status.is_success() {
            return stream_success_response(response, self.max_sse_frame_bytes, capture.cloned())
                .await;
        }

        let body = response.text().await.unwrap_or_default();
        if let Some(retry) = classify_context_overflow(
            &body,
            self.min_completion_tokens,
            Some(estimate_leaf_input_tokens(&request)),
        ) {
            let mut retried = request.clone();
            retried.max_output_tokens = Some(retry.max_completion_tokens);
            // The reduced budget lives on the typed `max_output_tokens` field
            // (serialized as `max_tokens`). Any max-token ALIAS that flowed into
            // `extra_body` (`max_tokens`, `max_completion_tokens`,
            // `max_output_tokens`) would serialize alongside and let the upstream
            // honor the stale oversized value, defeating the retry. Mirror the
            // engine's "explicit field removes conflicting default keys" rule and
            // strip them so only the reduced typed field applies.
            remove_max_token_aliases(&mut retried.extra_body);
            tracing::warn!(
                reason = retry.reason,
                ctx_limit = retry.ctx_limit,
                input_tokens = retry.input_tokens.unwrap_or(0),
                input_is_lower_bound = retry.input_tokens_is_lower_bound,
                new_max_completion_tokens = retry.max_completion_tokens,
                "upstream context-window overflow; retrying once with reduced completion budget"
            );
            // D2/F1d: the retry is the body that ACTUALLY goes on the wire on the
            // shrink path, so BOTH captures (D2's `upstream_body`, F1d's
            // `upstream_request` section) must reflect the shrunk request (same
            // `response_id`; F1d is last-writer-wins so this overwrites the first
            // oversized attempt's write — AC-10).
            let retry_response = self
                .logged_send_chat_request(url, &retried, response_id, capture)
                .await?;
            let retry_status = retry_response.status();
            if retry_status.is_success() {
                return stream_success_response(
                    retry_response,
                    self.max_sse_frame_bytes,
                    capture.cloned(),
                )
                .await;
            }
            // The retry ALSO failed. If it is again a context overflow, this is a
            // same-provider sizing problem, not a provider failure: surface a
            // TERMINAL error so `FailoverUpstreamClient`/routing does NOT retry
            // the same oversized prompt on another provider (only one shrink-and-
            // retry is allowed; we do not loop). Any other non-2xx is a normal
            // (failover-eligible) upstream error.
            let retry_body = retry_response.text().await.unwrap_or_default();
            // Gap 05: the RETRY is the body that actually ended the turn on the
            // shrink-and-retry path — stage IT (last-writer-wins over the first
            // attempt's body) so the dashboard shows the upstream's final word.
            // F1e also stages it onto the turn-capture handle.
            self.capture_upstream_response_body(serving, capture, &retry_body);
            if classify_context_overflow(
                &retry_body,
                self.min_completion_tokens,
                Some(estimate_leaf_input_tokens(&retried)),
            )
            .is_some()
            {
                return Err(AppError::upstream_with_disposition(
                    format!(
                        "upstream context-window overflow persisted after shrink-and-retry; \
                         failed with {retry_status}: {}",
                        redact_and_truncate_error_body(&retry_body, 500)
                    ),
                    FailoverDisposition::Terminal,
                ));
            }
            // E2a: the retry's status might not be an overflow at all (the shrink fixed
            // the size but the request is unacceptable for another reason) — a
            // request-intrinsic 4xx {400,413,415,422} still must not cool/failover a
            // healthy provider. Every other status (401/403/404/408/429/5xx) keeps the
            // default `Failover` disposition, unchanged.
            let retry_disposition = if status_is_request_intrinsic_4xx(retry_status) {
                FailoverDisposition::FailoverNoCooldown
            } else {
                FailoverDisposition::Failover
            };
            return Err(AppError::upstream_with_disposition(
                format!(
                    "upstream chat failed with {retry_status}: {}",
                    redact_and_truncate_error_body(&retry_body, 500)
                ),
                retry_disposition,
            ));
        }

        // Gap 05: a first-attempt non-2xx that did NOT trigger a context-overflow retry
        // — stage the upstream error body so the operator can see why the turn failed (it
        // is cleared if a later failover provider serves; committed at finalize otherwise).
        // F1e also stages it onto the turn-capture handle (final failed HTTP body).
        self.capture_upstream_response_body(serving, capture, &body);
        // E2a: request-intrinsic 4xx {400,413,415,422} → `Terminal` (never cools or fails
        // over a healthy provider — the request itself is unacceptable to any equivalent
        // backend; e.g. an image reaching a text-only upstream). 401/403/404/408/429/5xx
        // keep the default `Failover` disposition, unchanged. The `== Terminal`
        // gate in the failover loop (`stream_chat_completion_with_provider_indices`) then
        // skips `mark_failure` for this case — no cooldown, no failover.
        let disposition = if status_is_request_intrinsic_4xx(status) {
            FailoverDisposition::FailoverNoCooldown
        } else {
            FailoverDisposition::Failover
        };
        Err(AppError::upstream_with_disposition(
            format!(
                "upstream chat failed with {status}: {}",
                redact_and_truncate_error_body(&body, 500)
            ),
            disposition,
        ))
    }
}

#[async_trait]
impl UpstreamClient for ReqwestUpstreamClient {
    async fn stream_chat_completion(
        &self,
        backend: &BackendChatRequest,
    ) -> AppResult<UpstreamStream> {
        let url = self.endpoint_url("chat/completions")?;
        // Finalize at THIS leaf: the single point that sees the FINAL provider
        // model after routing/failover/exposed-alias remap, with provider kwargs
        // already merged by `request_for_provider`. Resolves the per-model
        // `upstream_chat_kwargs` (global base + per-model) + `template_family`
        // override from policies keyed by `request.model`, maps/clamps reasoning
        // effort, and injects family `chat_template_kwargs` (precedence: config <
        // family < effort-map < client; the wrapper's `client_chat_template_kwargs`
        // is re-asserted last so an explicit client value still wins).
        let mut backend = backend.clone();
        finalize_request_for_backend(&mut backend, &self.finalization_policies);
        // D2: the flow's `response_id` keys the on-wire capture below. Capture it
        // BEFORE `backend.request` is moved into `sanitize_chat_request` so the
        // first + retry send sites can both pass `response_id.as_deref()`.
        let response_id = backend.response_id.clone();
        // D2 bare-leaf path: when this leaf is the engine's upstream DIRECTLY (no
        // routing/failover wrapper — lib.rs `Arc::new(primary_upstream)`, marked via
        // `into_bare_primary`), nothing upstream tags the serving provider. Synthesize
        // `"primary"` so every flow carries a provider. A leaf nested inside a
        // failover/routing client is NOT marked, so it never runs this — otherwise
        // (first-writer-wins) it would clobber the real provider name the wrapper
        // records on first-chunk success, since the leaf runs BEFORE that success.
        if self.tag_primary_provider
            && let Some(serving) = &backend.serving
        {
            serving.set_provider("primary");
        }
        // D5 R4 (MEDIUM): the shared serving token, captured before `backend.request` is
        // moved into `sanitize`. The leaf finalizes the served model onto it below.
        let serving = backend.serving.clone();
        // F1d: the turn's durable-capture handle, captured here (mirrors
        // `response_id`/`serving` above) so both the first-attempt and any
        // shrink-retry send below can write the SANITIZED on-wire request into the
        // SAME turn's `upstream_request` section (last-writer-wins).
        let capture = backend.capture.clone();
        let request = sanitize_chat_request(backend.request, self.flatten_content);
        // D5 R4 (MEDIUM): finalize the ACTUAL on-wire model onto the shared serving
        // token, overwriting the engine's PRE-routing guess (and any earlier
        // failed-provider leaf write). This leaf is the single point that sees the FINAL
        // `request.model` after provider remap + `sanitize_chat_request` — the same model
        // captured into the FlowStore record by `capture_upstream_body` — so the D5 metrics
        // bucket (which reads `model_served` off this token) attributes a routed/failover
        // request to the model the backend actually answered as, not the requested one. On
        // failover/routing the LAST leaf to run (the serving / last-tried provider) wins.
        if let Some(serving) = &serving {
            serving.set_model_served_final(request.model.clone());
        }

        // Gap 03: the bare-leaf path (this leaf IS the engine's upstream directly — no
        // failover loop owns attempt recording) records EXACTLY ONE attempt for the flow.
        // A leaf nested in a failover/routing client leaves `tag_primary_provider` false,
        // so the failover loop is the sole attempt recorder there (this branch is skipped
        // — no double-count). Round-1 review (F1): the served/failed attempt's
        // `first_upstream_byte_ms` is the TRUE wire TTFB — the instant the upstream response
        // HEADERS arrived (`dispatch_chat_stream` stamps the armed slot for both a 2xx and a
        // non-2xx), NOT the later instant the first parsed SSE chunk is yielded. A dispatch
        // that fails AFTER headers (a non-2xx) thus carries a measured byte time; a
        // connect/timeout-before-response leaves it `None` (the slot was never stamped).
        if self.tag_primary_provider {
            let attempt_start_ms = now_epoch_ms_u128();
            let attempt_model = request.model.clone();
            if let Some(serving) = &serving {
                serving.arm_attempt_header_byte();
                // Gap 05 review round 2 (HIGH): per-attempt reset of the staged ERROR body
                // on the bare-leaf (single-attempt, no-failover) path too, so the invariant
                // "the final outcome decides the committed body" holds identically on every
                // dispatch path. A fresh `ServingToken` starts empty, so this is normally a
                // no-op here — but keeping the reset symmetric with the failover loop means
                // the bare leaf can never commit a body from a stale staging, regardless of
                // how the token was constructed. The non-2xx site in `dispatch_chat_stream`
                // re-stages this attempt's own body; a body-less failure leaves it cleared.
                serving.clear_pending_response_body();
            }
            return match self
                .dispatch_chat_stream(
                    &url,
                    request,
                    response_id.as_deref(),
                    serving.as_ref(),
                    capture.as_ref(),
                )
                .await
            {
                Ok(stream) => {
                    // Headers arrived (the slot holds the wire byte time). The attempt is
                    // still RECORDED at first-chunk yield so a stream that produces no chunk
                    // is a FAILED attempt — but its `first_upstream_byte_ms` is the header
                    // time captured here, not the chunk-yield time.
                    let header_byte_ms = serving
                        .as_ref()
                        .and_then(|serving| serving.take_attempt_header_byte());
                    Ok(Self::record_served_attempt_on_first_byte(
                        serving,
                        stream,
                        attempt_start_ms,
                        attempt_model,
                        header_byte_ms,
                    ))
                }
                Err(err) => {
                    if let Some(serving) = &serving {
                        // `take_attempt_header_byte` is `Some` for an HTTP-status failure
                        // (headers arrived) and `None` for a connect/timeout-before-response.
                        let header_byte_ms = serving.take_attempt_header_byte();
                        serving.record_attempt(Self::failed_bare_attempt(
                            attempt_start_ms,
                            attempt_model,
                            header_byte_ms,
                            &err,
                        ));
                    }
                    Err(err)
                }
            };
        }

        self.dispatch_chat_stream(
            &url,
            request,
            response_id.as_deref(),
            serving.as_ref(),
            capture.as_ref(),
        )
        .await
    }

    async fn count_tokens(&self, backend: &BackendChatRequest) -> AppResult<Option<u64>> {
        let mut backend = backend.clone();
        finalize_request_for_backend(&mut backend, &self.finalization_policies);
        let request = sanitize_chat_request(backend.request, self.flatten_content);

        let mut body = JsonMap::new();
        body.insert("model".to_string(), Value::String(request.model));
        body.insert(
            "messages".to_string(),
            serde_json::to_value(request.messages).map_err(|err| {
                AppError::internal(format!("failed to serialize tokenize messages: {err}"))
            })?,
        );
        if let Some(tools) = request.tools {
            body.insert(
                "tools".to_string(),
                serde_json::to_value(tools).map_err(|err| {
                    AppError::internal(format!("failed to serialize tokenize tools: {err}"))
                })?,
            );
        }
        if let Some(chat_template_kwargs) = request.extra_body.get("chat_template_kwargs") {
            body.insert(
                "chat_template_kwargs".to_string(),
                chat_template_kwargs.clone(),
            );
        }

        let response = match self
            .with_auth(self.client.post(self.tokenize_url()))
            .json(&Value::Object(body))
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                tracing::debug!(error = %err, "upstream /tokenize request failed");
                return Ok(None);
            }
        };
        if !response.status().is_success() {
            tracing::debug!(status = %response.status(), "upstream /tokenize returned non-success status");
            return Ok(None);
        }
        let value: Value = match response.json().await {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(error = %err, "upstream /tokenize response was not JSON");
                return Ok(None);
            }
        };
        Ok(value.get("count").and_then(Value::as_u64))
    }

    async fn list_models(&self) -> AppResult<reqwest::Response> {
        let url = self.endpoint_url("models")?;
        let response = self
            .with_auth(self.client.get(url))
            .send()
            .await
            .map_err(|err| AppError::upstream(format!("upstream models request failed: {err}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AppError::upstream(format!(
                "upstream /models failed with {status}: {}",
                redact_and_truncate_error_body(&body, 500)
            )));
        }
        Ok(response)
    }

    async fn proxy_completions(
        &self,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        let url = self.endpoint_url("completions")?;
        let request = copy_proxy_request_headers(self.client.post(url), &headers).body(body);
        self.with_auth(request).send().await.map_err(|err| {
            AppError::upstream(format!("upstream completions request failed: {err}"))
        })
    }
}

impl FailoverUpstreamClient {
    pub fn new(providers: Vec<FailoverUpstreamProvider>, cooldown: Duration) -> Self {
        let states = vec![ProviderCooldownState::default(); providers.len()];
        Self {
            providers,
            cooldown,
            states: Arc::new(Mutex::new(states)),
        }
    }

    /// The configured `upstream_model` rewrite of provider `index`, if any.
    /// `None` when out of range or the provider sends the request model
    /// unchanged. Used by G4 native-vision gating (round-2 #1).
    fn provider_upstream_model(&self, index: usize) -> Option<String> {
        self.providers
            .get(index)
            .and_then(|provider| provider.upstream_model.clone())
    }

    fn available_provider_indices(&self) -> Vec<usize> {
        let now = Instant::now();
        let states = self
            .states
            .lock()
            .expect("upstream provider cooldown state lock poisoned");
        self.providers
            .iter()
            .enumerate()
            .filter_map(|(index, _)| {
                let cooling = states
                    .get(index)
                    .and_then(|state| state.cooling_until)
                    .is_some_and(|until| until > now);
                (!cooling).then_some(index)
            })
            .collect()
    }

    fn provider_is_available(&self, provider_index: usize) -> bool {
        let now = Instant::now();
        let states = self
            .states
            .lock()
            .expect("upstream provider cooldown state lock poisoned");
        states
            .get(provider_index)
            .and_then(|state| state.cooling_until)
            .is_none_or(|until| until <= now)
    }

    /// Build the D4 `ProviderHealth` vector for this failover chain. `route` is
    /// `Some` only when reached via a routing provider (it is then stamped on
    /// every entry); `catalog_meta` carries the routing catalog's `(fetched_ms,
    /// size)` for that same case (a bare failover chain has no per-chain catalog
    /// snapshot, so it is `None`).
    ///
    /// Reads are lock-free over each provider's `Arc<ProviderMetrics>` /
    /// `base_url`, plus ONE short `states` `Mutex` hold to snapshot the cooldown
    /// deadline + last error per provider (cloned out before computing status, so
    /// the lock is released immediately). The `(deadline, consecutive_failures)`
    /// pair both come from this single consistent read.
    fn provider_health_with_route(
        &self,
        route: Option<&str>,
        catalog_meta: CatalogMeta,
    ) -> Vec<ProviderHealth> {
        // Snapshot cooldown state under one short lock hold, then drop it.
        let states: Vec<(Option<Instant>, Option<String>)> = {
            let guard = self
                .states
                .lock()
                .expect("upstream provider cooldown state lock poisoned");
            self.providers
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    guard
                        .get(index)
                        .map(|state| (state.cooling_until, state.last_error.clone()))
                        .unwrap_or((None, None))
                })
                .collect()
        };
        let now = Instant::now();
        self.providers
            .iter()
            .zip(states)
            .map(|(provider, (cooling_until, last_error))| {
                let consecutive_failures = provider.metrics.consecutive_failures();
                let cooling = cooling_until.is_some_and(|until| until > now);
                let status = if cooling {
                    if consecutive_failures >= DOWN_THRESHOLD {
                        ProviderStatus::Down
                    } else {
                        ProviderStatus::Cooling
                    }
                } else {
                    ProviderStatus::Healthy
                };
                // Surface the cooldown deadline only while actually cooling — a
                // stale past deadline (cleared logically by the `now` compare)
                // would otherwise serialize a misleading future-looking value.
                let cooling_until_ms = if cooling {
                    instant_deadline_to_epoch_ms(cooling_until)
                } else {
                    None
                };
                ProviderHealth {
                    id: provider.name.clone(),
                    name: provider.name.clone(),
                    route: route.map(ToString::to_string),
                    base_url: provider.client.base_url_string(),
                    status,
                    cooling_until_ms,
                    last_error,
                    served_count: provider.metrics.served_count(),
                    failover_count: provider.metrics.failover_count(),
                    consecutive_failures,
                    catalog_fetched_ms: catalog_meta.fetched_ms,
                    catalog_size: catalog_meta.size,
                }
            })
            .collect()
    }

    fn cooldown_error(&self) -> AppError {
        let now = Instant::now();
        let states = self
            .states
            .lock()
            .expect("upstream provider cooldown state lock poisoned");
        let next_retry_secs = states
            .iter()
            .filter_map(|state| state.cooling_until)
            .filter(|until| *until > now)
            .map(|until| until.duration_since(now).as_secs().max(1))
            .min()
            .unwrap_or(0);
        let last_error = states
            .iter()
            .rev()
            .find_map(|state| state.last_error.as_deref())
            .unwrap_or("no provider is currently available");
        AppError::upstream(format!(
            "all upstream providers are in cooldown; next retry in {next_retry_secs}s; last error: {last_error}"
        ))
    }

    fn request_for_provider(
        provider: &FailoverUpstreamProvider,
        backend: &BackendChatRequest,
    ) -> BackendChatRequest {
        let mut request = backend.request.clone();
        if let Some(model) = &provider.upstream_model {
            request.model = model.clone();
        }
        merge_chat_kwargs_gap_fill(&mut request, &provider.upstream_chat_kwargs);
        BackendChatRequest {
            request,
            client_chat_template_kwargs: backend.client_chat_template_kwargs.clone(),
            thinking_override: backend.thinking_override,
            // D2: carry the flow identity + shared serving token forward (clone the
            // `Arc`, not the token) so the leaf still captures + tags this flow.
            response_id: backend.response_id.clone(),
            serving: backend.serving.clone(),
            // F1d (AC-11): carry the turn-capture handle forward too -- a captured
            // turn keeps capturing across a failover provider rebuild.
            capture: backend.capture.clone(),
        }
    }

    async fn prefetch_first_chunk(
        mut stream: UpstreamStream,
        request_timeout: Duration,
    ) -> AppResult<(ChatCompletionChunk, UpstreamStream)> {
        match tokio::time::timeout(request_timeout, stream.next()).await {
            Ok(Some(Ok(chunk))) => Ok((chunk, stream)),
            Ok(Some(Err(err))) => Err(err),
            Ok(None) => Err(AppError::upstream(
                "upstream stream ended before the first chunk",
            )),
            Err(_) => Err(AppError::upstream("upstream stream timed out".to_string())),
        }
    }

    fn stream_after_prefetch(
        &self,
        provider_index: usize,
        first_chunk: ChatCompletionChunk,
        mut stream: UpstreamStream,
        request_timeout: Duration,
    ) -> UpstreamStream {
        let states = Arc::clone(&self.states);
        let cooldown = self.cooldown;
        let provider_name = self.providers[provider_index].name.clone();
        // D4: capture this provider's `Arc<ProviderMetrics>` so a mid-stream
        // failure (which runs in the spawned stream, not through `&self`) still
        // records the failover/consecutive-failure counters.
        let metrics = Arc::clone(&self.providers[provider_index].metrics);
        Box::pin(async_stream::stream! {
            yield Ok(first_chunk);
            loop {
                match tokio::time::timeout(request_timeout, stream.next()).await {
                    Ok(Some(Ok(chunk))) => yield Ok(chunk),
                    Ok(Some(Err(err))) => {
                        Self::mark_provider_failure(
                            &states,
                            &metrics,
                            provider_index,
                            &provider_name,
                            cooldown,
                            err.to_string(),
                        );
                        yield Err(err);
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        let err = AppError::upstream("upstream stream timed out".to_string());
                        Self::mark_provider_failure(
                            &states,
                            &metrics,
                            provider_index,
                            &provider_name,
                            cooldown,
                            err.to_string(),
                        );
                        yield Err(err);
                        break;
                    }
                }
            }
        })
    }

    fn mark_provider_success(&self, provider_index: usize) {
        let mut states = self
            .states
            .lock()
            .expect("upstream provider cooldown state lock poisoned");
        if let Some(state) = states.get_mut(provider_index) {
            state.cooling_until = None;
            state.last_error = None;
        }
        // D4: a served flow clears the consecutive-failure streak (mirrors the
        // cooldown clear above) and bumps the cumulative served counter, under
        // the same `states` lock so the cleared deadline + reset streak are
        // observed together (a reader snapshots the deadline under this lock).
        if let Some(provider) = self.providers.get(provider_index) {
            provider.metrics.record_success();
        }
    }

    fn mark_provider_failure(
        states: &Arc<Mutex<Vec<ProviderCooldownState>>>,
        metrics: &Arc<ProviderMetrics>,
        provider_index: usize,
        provider_name: &str,
        cooldown: Duration,
        error: String,
    ) {
        let cooling_until = (cooldown > Duration::ZERO).then(|| Instant::now() + cooldown);
        {
            let mut states = states
                .lock()
                .expect("upstream provider cooldown state lock poisoned");
            if let Some(state) = states.get_mut(provider_index) {
                state.cooling_until = cooling_until;
                state.last_error = Some(error.clone());
            }
            // D4: bump the cumulative failover counter + the consecutive-failure
            // streak (the streak crosses `DOWN_THRESHOLD` → `Down` while cooling)
            // WHILE the `states` lock is held, so the deadline write and the
            // failure-count bump land in one critical section — a reader that
            // snapshots the cooldown deadline under this same lock observes the
            // pair consistently (it reads `consecutive_failures` right after, so
            // a Cooling→Down step is never split across the deadline write).
            metrics.record_failure();
        }
        if cooldown > Duration::ZERO {
            tracing::warn!(
                provider = provider_name,
                cooldown_secs = cooldown.as_secs(),
                error = %error,
                "upstream provider failed; entering cooldown"
            );
        } else {
            tracing::warn!(
                provider = provider_name,
                error = %error,
                "upstream provider failed"
            );
        }
    }

    fn mark_failure(&self, provider_index: usize, error: &AppError) {
        Self::mark_provider_failure(
            &self.states,
            &self.providers[provider_index].metrics,
            provider_index,
            &self.providers[provider_index].name,
            self.cooldown,
            error.to_string(),
        );
    }

    async fn stream_chat_completion_with_timeout_from_provider(
        &self,
        provider_index: usize,
        backend: &BackendChatRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        if provider_index >= self.providers.len() {
            return Err(AppError::internal(
                "resolved fallback provider index was out of range",
            ));
        }
        if !self.provider_is_available(provider_index) {
            return Err(self.cooldown_error());
        }
        self.stream_chat_completion_with_provider_indices(
            vec![provider_index],
            backend,
            request_timeout,
        )
        .await
    }

    async fn stream_chat_completion_with_provider_indices(
        &self,
        provider_indices: Vec<usize>,
        backend: &BackendChatRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        let mut last_error = None;
        for provider_index in provider_indices {
            let provider = &self.providers[provider_index];
            let provider_request = Self::request_for_provider(provider, backend);
            // Gap 03: per-attempt provenance. `start_ms` is the wall-clock the dispatch
            // is issued; the provider's on-wire model is `provider_request.request.model`
            // (post `request_for_provider` remap). The attempt's outcome (served / failed)
            // is recorded onto the shared `ServingToken` at each terminal arm below, so
            // the failover trace shows WHICH provider failed, WHY (bounded taxonomic
            // code), HOW LONG, and WHAT eventually served. The nested leaf is NOT
            // `tag_primary_provider`-marked, so IT records no attempt — only this loop
            // does, exactly once per provider it tries.
            let attempt_start_ms = now_epoch_ms_u128();
            let attempt_model = provider_request.request.model.clone();
            // Gap 03 round-1 review (F1): arm the per-attempt wire-byte slot on the SHARED
            // token (the nested leaf stamps it the instant `send().await` returns response
            // headers, for both a 2xx and a non-2xx). Read it after the dispatch resolves:
            // `Some` once headers arrived (served OR HTTP-status failure), `None` for a
            // connect/timeout-before-response. This is the TRUE wire TTFB the prior code
            // missed (it stamped a served attempt only at first-chunk yield and recorded
            // `None` for an HTTP-status failure despite headers having arrived).
            if let Some(serving) = &backend.serving {
                serving.arm_attempt_header_byte();
                // Gap 05 review round 2 (HIGH): CLEAR any upstream ERROR body staged by an
                // EARLIER provider before THIS attempt dispatches, so the FINAL attempt's
                // outcome — not a stale earlier one — decides the committed body. Without
                // this, `A=500(body) → B=connect/timeout/prefetch-stream-error(no body)`
                // would commit A's stale body even though the turn's final failure carried
                // none: B's body-less failure never re-stages, and the served-path clear
                // (below) is reached only on a SERVE, not on a body-less failure. Mirrors
                // `arm_attempt_header_byte`'s per-attempt reset of scratch state — an
                // HTTP-status failure re-stages its own body, a served attempt clears (as
                // it already does), and a body-less final failure leaves it cleared ⇒ the
                // record commits `None`. Idempotent + gated (no-op when capture is off).
                serving.clear_pending_response_body();
            }
            let stream = match provider
                .client
                .stream_chat_completion(&provider_request)
                .await
            {
                Ok(stream) => stream,
                Err(err) if err.failover_disposition() == FailoverDisposition::Terminal => {
                    // Terminal same-provider error (e.g. a context overflow that
                    // survived the leaf shrink-and-retry). Trying the next
                    // provider would just overflow again, so surface it as-is and
                    // do NOT mark this provider failed (it is not at fault). A context
                    // overflow IS an HTTP-status response, so its headers arrived — the
                    // slot carries the wire byte time for the trace.
                    let header_byte_ms = Self::take_attempt_header_byte(backend);
                    Self::record_attempt(
                        backend,
                        provider,
                        attempt_start_ms,
                        attempt_model,
                        header_byte_ms,
                        Some(&err),
                    );
                    return Err(err);
                }
                Err(err)
                    if err.failover_disposition() == FailoverDisposition::FailoverNoCooldown =>
                {
                    let header_byte_ms = Self::take_attempt_header_byte(backend);
                    Self::record_attempt(
                        backend,
                        provider,
                        attempt_start_ms,
                        attempt_model,
                        header_byte_ms,
                        Some(&err),
                    );
                    last_error = Some(err);
                    continue;
                }
                Err(err) => {
                    self.mark_failure(provider_index, &err);
                    // `Some` for an HTTP-status failure (headers arrived), `None` for a
                    // connect/timeout-before-response (the leaf never reached the stamp).
                    let header_byte_ms = Self::take_attempt_header_byte(backend);
                    Self::record_attempt(
                        backend,
                        provider,
                        attempt_start_ms,
                        attempt_model,
                        header_byte_ms,
                        Some(&err),
                    );
                    last_error = Some(err);
                    continue;
                }
            };
            // Headers arrived (the dispatch returned `Ok`), so the slot holds the wire byte
            // time for THIS served/prefetch-failed attempt. Read it ONCE here, before
            // `prefetch_first_chunk` may take a while waiting for the first chunk — the
            // measured byte is the header time, not the prefetch-completion time.
            let header_byte_ms = Self::take_attempt_header_byte(backend);
            match Self::prefetch_first_chunk(stream, request_timeout).await {
                Ok((first_chunk, stream)) => {
                    self.mark_provider_success(provider_index);
                    // D2: this provider produced the first chunk, so it is the ACTUAL
                    // serving provider (not a fallback that was tried and skipped —
                    // AGENTS.md steering). The failover layer owns the `provider`
                    // serving field; tag the flow's shared token here (first-writer-
                    // wins). The same `Arc` lives on `provider_request`; use `backend`.
                    if let Some(serving) = &backend.serving {
                        serving.set_provider(provider.name.clone());
                        // Gap 05 (F1): this provider SERVED the turn, so any pending upstream
                        // ERROR body staged by an EARLIER failed provider (e.g. a 500 before
                        // this 200) must NOT remain as the turn's `upstream_response` — clear
                        // it here so finalize commits `None` for a served turn. The served
                        // attempt is authoritative (gap-03 model). All-providers-fail never
                        // reaches this seam, so the last attempt's staged body survives to
                        // commit.
                        serving.clear_pending_response_body();
                    }
                    // Gap 03 (F1): this attempt SERVED — its `first_upstream_byte_ms` is the
                    // wire HEADER time (`header_byte_ms`), the instant `send().await`
                    // returned, NOT the later first-chunk-yield instant. This also seeds the
                    // flow-level `first_upstream_byte_ms` via `record_attempt`.
                    Self::record_attempt(
                        backend,
                        provider,
                        attempt_start_ms,
                        attempt_model,
                        header_byte_ms,
                        None,
                    );
                    if provider_index > 0 {
                        tracing::info!(
                            provider = %provider.name,
                            "using fallback upstream provider"
                        );
                    }
                    return Ok(self.stream_after_prefetch(
                        provider_index,
                        first_chunk,
                        stream,
                        request_timeout,
                    ));
                }
                Err(err) => {
                    self.mark_failure(provider_index, &err);
                    // Gap 03 (F1): the response HEADERS arrived but no first chunk did (a
                    // stream-ended / timeout-AFTER-headers). Headers were received, so the
                    // wire byte time IS measured (`header_byte_ms` is `Some`) — only the
                    // first chunk is absent. `None` here would only occur on a path that
                    // never received headers, which this arm is not.
                    Self::record_attempt(
                        backend,
                        provider,
                        attempt_start_ms,
                        attempt_model,
                        header_byte_ms,
                        Some(&err),
                    );
                    last_error = Some(err);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            AppError::upstream("all upstream providers failed before producing a response")
        }))
    }

    /// Gap 03 round-1 review (F1): read the current attempt's wire-header-byte time off the
    /// shared `ServingToken` (`None` when no token is threaded, or when no response headers
    /// were ever received — a connect/timeout-before-response). The caller passes the
    /// result as the attempt's `first_upstream_byte_ms`.
    fn take_attempt_header_byte(backend: &BackendChatRequest) -> Option<u128> {
        backend
            .serving
            .as_ref()
            .and_then(|serving| serving.take_attempt_header_byte())
    }

    /// Gap 03: build and push one [`Attempt`](crate::dashboard_flow::Attempt) onto the
    /// flow's shared `ServingToken` (no-op when no token is threaded — tests / non-engine
    /// paths). Round-1 review (F1): `first_upstream_byte_ms` is the TRUE wire TTFB the
    /// caller measured — the instant the upstream response HEADERS arrived (`Some` for a
    /// served attempt AND for an HTTP-status failure whose headers landed; `None` only for a
    /// connect/timeout-BEFORE-response, never `0` — don't-lie-with-zeros). On a failure,
    /// `err` drives the BOUNDED taxonomic `error_class` + `failover_reason` (never raw
    /// upstream text — those bodies stay spec-05-gated); on a success both are `None`.
    fn record_attempt(
        backend: &BackendChatRequest,
        provider: &FailoverUpstreamProvider,
        start_ms: u128,
        model: String,
        first_upstream_byte_ms: Option<u128>,
        err: Option<&AppError>,
    ) {
        use crate::dashboard_flow::AttemptFailoverReason;
        use crate::dashboard_flow::AttemptStatus;
        use crate::error::FailoverDisposition;
        let Some(serving) = &backend.serving else {
            return;
        };
        let (status, error_class, failover_reason) = match err {
            None => (AttemptStatus::Served, None, None),
            Some(err) => {
                let reason = match err.failover_disposition() {
                    FailoverDisposition::Terminal => {
                        // A terminal disposition ends the flow — failover did NOT advance.
                        AttemptFailoverReason::TerminalNoFailover
                    }
                    FailoverDisposition::FailoverNoCooldown => {
                        AttemptFailoverReason::RequestRejected
                    }
                    FailoverDisposition::Failover => AttemptFailoverReason::ProviderFailed,
                };
                (
                    AttemptStatus::Failed,
                    Some(classify_attempt_error(err)),
                    Some(reason),
                )
            }
        };
        serving.record_attempt(crate::dashboard_flow::Attempt {
            provider: Some(provider.name.clone()),
            model: Some(model),
            start_ms,
            end_ms: now_epoch_ms_u128(),
            first_upstream_byte_ms,
            status,
            error_class,
            failover_reason,
        });
    }

    async fn count_tokens_from_provider(
        &self,
        provider_index: usize,
        backend: &BackendChatRequest,
    ) -> AppResult<Option<u64>> {
        if provider_index >= self.providers.len() {
            return Err(AppError::internal(
                "resolved fallback provider index was out of range",
            ));
        }
        self.count_tokens_with_provider_indices(vec![provider_index], backend)
            .await
    }

    async fn count_tokens_with_provider_indices(
        &self,
        provider_indices: Vec<usize>,
        backend: &BackendChatRequest,
    ) -> AppResult<Option<u64>> {
        for provider_index in provider_indices {
            let provider = &self.providers[provider_index];
            let provider_request = Self::request_for_provider(provider, backend);
            match provider.client.count_tokens(&provider_request).await {
                Ok(Some(count)) => return Ok(Some(count)),
                Ok(None) => {}
                Err(err) => tracing::debug!(
                    provider = %provider.name,
                    error = %err,
                    "provider token counting failed"
                ),
            }
        }
        Ok(None)
    }

    async fn proxy_completions_from_provider(
        &self,
        provider_index: usize,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        if provider_index >= self.providers.len() {
            return Err(AppError::internal(
                "resolved fallback provider index was out of range",
            ));
        }
        if !self.provider_is_available(provider_index) {
            return Err(self.cooldown_error());
        }
        self.proxy_completions_with_provider_indices(vec![provider_index], headers, body)
            .await
    }

    async fn proxy_completions_with_provider_indices(
        &self,
        provider_indices: Vec<usize>,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        let mut last_error = None;
        for provider_index in provider_indices {
            let provider = &self.providers[provider_index];
            let provider_body = proxy_body_for_provider(provider, &body);
            match self.providers[provider_index]
                .client
                .proxy_completions(headers.clone(), provider_body)
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    if !status_is_failover_eligible(status) {
                        return Ok(response);
                    }
                    let body = response.text().await.unwrap_or_default();
                    let err = AppError::upstream(format!(
                        "upstream completions failed with {status}: {}",
                        redact_and_truncate_error_body(&body, 500)
                    ));
                    self.mark_failure(provider_index, &err);
                    last_error = Some(err);
                }
                Err(err) => {
                    self.mark_failure(provider_index, &err);
                    last_error = Some(err);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            AppError::upstream("all upstream providers failed to proxy completions")
        }))
    }
}

impl RoutingUpstreamClient {
    pub fn new(providers: Vec<RoutingUpstreamProvider>) -> Self {
        Self::with_routes(providers, Vec::new(), Vec::new())
    }

    /// Construct with ad-hoc model routes (G7). `route_providers` are the
    /// synthetic upstreams; `routes` map request-model names/globs to them and
    /// must reference valid `route_provider_index` values.
    pub fn with_routes(
        providers: Vec<RoutingUpstreamProvider>,
        route_providers: Vec<RouteUpstreamProvider>,
        routes: Vec<ModelRouteSpec>,
    ) -> Self {
        Self {
            providers,
            route_providers,
            routes,
            catalog: Arc::new(AsyncMutex::new(None)),
            catalog_meta: Arc::new(Mutex::new(Arc::new(CatalogMeta::default()))),
        }
    }

    /// Look up a synthetic route provider by index, mapping an out-of-range
    /// index (a wiring bug) to an internal error rather than a panic.
    fn route_provider(&self, index: usize) -> AppResult<&RouteUpstreamProvider> {
        self.route_providers
            .get(index)
            .ok_or_else(|| AppError::internal("resolved route provider index was out of range"))
    }

    /// Clone the request and apply the resolved upstream-model rewrite, logging
    /// once when the model actually changes. Shared by catalog and route
    /// dispatch so both honor the same rewrite + log behavior. The wrapper's
    /// `client_chat_template_kwargs` is preserved across the rewrite.
    fn routed_request(
        &self,
        backend: &BackendChatRequest,
        model_id: &str,
        provider_name: &str,
        kind: MatchKind,
    ) -> BackendChatRequest {
        let mut routed_request = backend.request.clone();
        if routed_request.model != model_id {
            log_model_resolution(&routed_request.model, model_id, provider_name, kind);
            routed_request.model = model_id.to_string();
        }
        BackendChatRequest {
            request: routed_request,
            client_chat_template_kwargs: backend.client_chat_template_kwargs.clone(),
            thinking_override: backend.thinking_override,
            // D2: carry the flow identity + shared serving token forward (clone the
            // `Arc`, not the token).
            response_id: backend.response_id.clone(),
            serving: backend.serving.clone(),
            // F1d: carry the turn-capture handle forward too (same reasoning as the
            // failover rebuild above, `request_for_provider`).
            capture: backend.capture.clone(),
        }
    }

    async fn load_catalog(&self) -> AppResult<RoutingModelCatalog> {
        let mut cache = self.catalog.lock().await;
        if let Some(cached) = cache.as_ref()
            && cached.fetched_at.elapsed().as_secs() < ROUTING_MODEL_CATALOG_TTL_SECS
        {
            return Ok(cached.catalog.clone());
        }
        let catalog = self.refresh_catalog().await?;
        *cache = Some(CachedRoutingModelCatalog {
            fetched_at: Instant::now(),
            catalog: catalog.clone(),
        });
        Ok(catalog)
    }

    async fn refresh_catalog(&self) -> AppResult<RoutingModelCatalog> {
        let mut provider_catalogs = Vec::with_capacity(self.providers.len());
        let mut union_entries = Vec::new();
        let mut union_ids = Vec::new();
        let mut union_context_limit_by_id: HashMap<String, i64> = HashMap::new();
        let mut ids_by_key: HashMap<String, Vec<RoutingModelCandidate>> = HashMap::new();
        let mut seen_union_ids = HashSet::new();
        let mut last_error = None;

        for (provider_index, provider) in self.providers.iter().enumerate() {
            let entries = match primary_provider_model_entries(provider).await {
                Ok(entries) => entries,
                Err(err) => {
                    tracing::warn!(
                        provider = %provider.name,
                        error = %err,
                        "failed to load upstream model catalog"
                    );
                    last_error = Some(err);
                    Vec::new()
                }
            };

            let mut provider_candidates = Vec::new();
            let mut provider_context_limits: HashMap<String, i64> = HashMap::new();
            for entry in entries {
                let Some((model_id, entry)) = normalized_model_entry(entry) else {
                    continue;
                };
                register_routing_model(
                    provider_index,
                    model_id,
                    entry,
                    RoutingModelTarget::Primary,
                    &mut provider_candidates,
                    &mut provider_context_limits,
                    &mut union_entries,
                    &mut union_ids,
                    &mut union_context_limit_by_id,
                    &mut ids_by_key,
                    &mut seen_union_ids,
                );
            }
            for model in &provider.fallback_exposed_models {
                register_routing_model(
                    provider_index,
                    model.model_id.clone(),
                    single_model_entry(&model.model_id),
                    RoutingModelTarget::Fallback {
                        failover_provider_index: model.failover_provider_index,
                    },
                    &mut provider_candidates,
                    &mut provider_context_limits,
                    &mut union_entries,
                    &mut union_ids,
                    &mut union_context_limit_by_id,
                    &mut ids_by_key,
                    &mut seen_union_ids,
                );
            }
            provider_catalogs.push(RoutingProviderModelCatalog {
                candidates: provider_candidates,
                context_limit_by_id: provider_context_limits,
            });
        }

        // With ad-hoc routes (G7), an empty union is still a usable catalog:
        // routes resolve by name without a live model listing. Only error when
        // there are neither catalog models nor routes to dispatch to.
        if union_ids.is_empty() && self.routes.is_empty() {
            return Err(last_error.unwrap_or_else(|| {
                AppError::upstream("no models are currently available from configured upstreams")
            }));
        }

        // D4: publish the catalog metadata as a SINGLE immutable `Arc<CatalogMeta>`
        // swap. We still hold the `catalog` `AsyncMutex` (the caller `load_catalog`
        // owns it for this whole refresh), so the swap is serialized with the
        // catalog write — a lock-free `provider_health()` reader sees the
        // `(fetched_ms, size)` pair move together, never torn. `fetched_ms` is the
        // refresh wall-clock; `size` is the union model count.
        let meta = Arc::new(CatalogMeta {
            fetched_ms: Some(now_epoch_ms()),
            size: Some(union_ids.len() as u64),
        });
        *self
            .catalog_meta
            .lock()
            .expect("routing catalog meta lock poisoned") = meta;

        Ok(RoutingModelCatalog {
            provider_catalogs,
            union_entries,
            union_ids,
            union_context_limit_by_id,
            ids_by_key,
            routes: self.routes.clone(),
        })
    }
}

impl RoutingModelCatalog {
    /// Resolve a request model to a dispatch target. Precedence (G7):
    /// 1. exact catalog model id (an exact id always wins),
    /// 2. exact ad-hoc route name,
    /// 3. glob ad-hoc route name (first match wins on overlap),
    /// 4. unique canonical-key catalog match,
    /// 5. default (first model of the first non-empty provider catalog).
    ///
    /// Routes therefore slot strictly between an exact id and the
    /// canonical/default fallbacks; a glob never overrides an exact match.
    fn resolve(&self, requested_model: &str) -> Option<(RoutingResolution, MatchKind)> {
        let trimmed = requested_model.trim();
        if !trimmed.is_empty() {
            // 1. Exact catalog model id.
            for (provider_index, provider) in self.provider_catalogs.iter().enumerate() {
                if let Some(candidate) = provider
                    .candidates
                    .iter()
                    .find(|candidate| candidate.model_id == trimmed)
                {
                    debug_assert_eq!(candidate.provider_index, provider_index);
                    return Some((
                        RoutingResolution::Catalog(candidate.clone()),
                        MatchKind::ExactId,
                    ));
                }
            }

            // 2. Exact ad-hoc route name (case-insensitive), then 3. glob route.
            if let Some(route) = self.match_route(trimmed) {
                return Some((
                    RoutingResolution::Route {
                        route_provider_index: route.route_provider_index,
                        model_id: route
                            .upstream_model
                            .clone()
                            .unwrap_or_else(|| trimmed.to_string()),
                    },
                    MatchKind::Route,
                ));
            }

            // 4. Unique canonical-key catalog match.
            let key = canonical_model_key(trimmed);
            if let Some(candidates) = self.ids_by_key.get(&key)
                && let Some(model_id) = unique_candidate_model_id(candidates)
            {
                return candidates
                    .iter()
                    .find(|candidate| candidate.model_id == model_id)
                    .cloned()
                    .map(|candidate| {
                        (
                            RoutingResolution::Catalog(candidate),
                            MatchKind::CanonicalKey,
                        )
                    });
            }
        }

        // 5. Default catalog candidate.
        self.default_candidate()
            .map(|candidate| (RoutingResolution::Catalog(candidate), MatchKind::Default))
    }

    /// Match a request model against ad-hoc routes: an exact name (case
    /// insensitive) beats any glob; among globs the first declared wins.
    fn match_route(&self, requested_model: &str) -> Option<&ModelRouteSpec> {
        if let Some(route) = self
            .routes
            .iter()
            .find(|route| route.glob.is_none() && route.name.eq_ignore_ascii_case(requested_model))
        {
            return Some(route);
        }
        self.routes.iter().find(|route| {
            route
                .glob
                .as_ref()
                .is_some_and(|glob| glob.is_match(requested_model))
        })
    }

    fn default_candidate(&self) -> Option<RoutingModelCandidate> {
        self.provider_catalogs
            .iter()
            .enumerate()
            .find_map(|(provider_index, provider)| {
                provider.candidates.first().map(|candidate| {
                    debug_assert_eq!(candidate.provider_index, provider_index);
                    candidate.clone()
                })
            })
    }

    fn union_body(&self) -> Value {
        serde_json::json!({
            "object": "list",
            "data": self.union_entries,
        })
    }
}

/// Log a request-model rewrite at a level reflecting WHY it happened. A
/// `Default` match means the requested model was not served by any upstream —
/// the operator likely loaded a different model than clients ask for, so it goes
/// to WARN. Expected normalizations (canonical-key) and ad-hoc routes stay at
/// INFO. Callers invoke this only when the model actually changed.
fn log_model_resolution(requested: &str, resolved: &str, provider: &str, kind: MatchKind) {
    if requested == resolved {
        return;
    }
    if kind == MatchKind::Default {
        tracing::warn!(
            requested_model = %requested,
            routed_model = %resolved,
            provider = %provider,
            "requested model is not served by any configured upstream; falling back to the default catalog model"
        );
    } else {
        tracing::info!(
            requested_model = %requested,
            routed_model = %resolved,
            provider = %provider,
            "routed request model to upstream catalog model"
        );
    }
}

fn unique_candidate_model_id(candidates: &[RoutingModelCandidate]) -> Option<String> {
    let mut unique = candidates
        .iter()
        .map(|candidate| candidate.model_id.as_str())
        .collect::<HashSet<_>>();
    if unique.len() == 1 {
        unique.drain().next().map(ToString::to_string)
    } else {
        None
    }
}

async fn primary_provider_model_entries(
    provider: &RoutingUpstreamProvider,
) -> AppResult<Vec<Value>> {
    let Some(model) = provider.primary_upstream_model.as_deref() else {
        let response = provider.primary_client.list_models().await?;
        let (_, body, _) = collect_models_response(response).await?;
        return Ok(model_entries_from_body(&body));
    };

    match provider.primary_client.list_models().await {
        Ok(response) => {
            let (_, body, _) = collect_models_response(response).await?;
            Ok(filter_model_entries(&model_entries_from_body(&body), model))
        }
        Err(err) => {
            tracing::warn!(
                provider = %provider.name,
                model,
                error = %err,
                "failed to load model metadata for configured upstream model; using synthetic entry"
            );
            Ok(vec![single_model_entry(model)])
        }
    }
}

fn normalized_model_entry(entry: Value) -> Option<(String, Value)> {
    match entry {
        Value::String(id) => Some((id.clone(), single_model_entry(&id))),
        Value::Object(map) => {
            let id = map.get("id").and_then(Value::as_str)?.to_string();
            Some((id, Value::Object(map)))
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn register_routing_model(
    provider_index: usize,
    model_id: String,
    entry: Value,
    target: RoutingModelTarget,
    provider_candidates: &mut Vec<RoutingModelCandidate>,
    context_limit_by_id: &mut HashMap<String, i64>,
    union_entries: &mut Vec<Value>,
    union_ids: &mut Vec<String>,
    union_context_limit_by_id: &mut HashMap<String, i64>,
    ids_by_key: &mut HashMap<String, Vec<RoutingModelCandidate>>,
    seen_union_ids: &mut HashSet<String>,
) {
    let key = canonical_model_key(&model_id);
    if key.is_empty() {
        return;
    }
    let candidate = RoutingModelCandidate {
        provider_index,
        model_id: model_id.clone(),
        target,
    };
    if !provider_candidates
        .iter()
        .any(|candidate| candidate.model_id == model_id)
    {
        provider_candidates.push(candidate.clone());
    }
    // Preserve the per-provider context limit for G3 budgeting (T9). Synthetic
    // bare-string entries (no object body) carry no limit ⇒ None.
    if let Value::Object(map) = &entry
        && let Some(limit) = entry_context_limit(map)
    {
        context_limit_by_id.insert(model_id.clone(), limit);
    }
    ids_by_key.entry(key).or_default().push(candidate);
    if seen_union_ids.insert(model_id.clone()) {
        // Capture the first-seen entry's context limit alongside the union id,
        // so the union catalog is parsed once here instead of being reparsed
        // from `union_entries` in `supported_model_catalog`. Reads the SAME
        // `entry_context_limit` over the SAME first-seen entry.
        if let Value::Object(map) = &entry
            && let Some(limit) = entry_context_limit(map)
        {
            union_context_limit_by_id.insert(model_id.clone(), limit);
        }
        union_ids.push(model_id);
        union_entries.push(entry);
    }
}

#[async_trait]
impl UpstreamClient for FailoverUpstreamClient {
    async fn stream_chat_completion(
        &self,
        backend: &BackendChatRequest,
    ) -> AppResult<UpstreamStream> {
        self.stream_chat_completion_with_timeout(backend, Duration::from_secs(60))
            .await
    }

    async fn stream_chat_completion_with_timeout(
        &self,
        backend: &BackendChatRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        let provider_indices = self.available_provider_indices();
        if provider_indices.is_empty() {
            return Err(self.cooldown_error());
        }
        self.stream_chat_completion_with_provider_indices(
            provider_indices,
            backend,
            request_timeout,
        )
        .await
    }

    async fn count_tokens(&self, backend: &BackendChatRequest) -> AppResult<Option<u64>> {
        let provider_indices = self.available_provider_indices();
        if provider_indices.is_empty() {
            return Ok(None);
        }
        self.count_tokens_with_provider_indices(provider_indices, backend)
            .await
    }

    async fn list_models(&self) -> AppResult<reqwest::Response> {
        let mut last_error = None;
        let provider_indices = self.available_provider_indices();
        if provider_indices.is_empty() {
            return Err(self.cooldown_error());
        }
        for provider_index in provider_indices {
            let provider = &self.providers[provider_index];
            match provider.client.list_models().await {
                Ok(response) => {
                    if let Some(model) = &provider.upstream_model {
                        return filter_models_response(response, model).await;
                    }
                    return Ok(response);
                }
                Err(err) => last_error = Some(err),
            }
        }
        Err(last_error
            .unwrap_or_else(|| AppError::upstream("all upstream providers failed to list models")))
    }

    async fn proxy_completions(
        &self,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        let provider_indices = self.available_provider_indices();
        if provider_indices.is_empty() {
            return Err(self.cooldown_error());
        }
        self.proxy_completions_with_provider_indices(provider_indices, headers, body)
            .await
    }

    /// Typed plan: a failover chain's candidate set is each provider's
    /// effective model — its `upstream_model` rewrite (matching
    /// `request_for_provider`) or the request model when it sends through
    /// unchanged (G4 round-2 #1). We enumerate ALL providers (not just
    /// currently-available ones): cooldown is transient, so a fallback that is
    /// non-native must still force strip+offload. Context limits are `None`
    /// (a failover chain does not load per-provider `/v1/models` catalogs, so
    /// G3 budgeting no-ops — matching pre-T9 behavior). `candidate_backend_models`
    /// (trait default) projects from this.
    async fn backend_candidate_plan(&self, requested_model: &str) -> BackendCandidatePlan {
        let candidates = self
            .providers
            .iter()
            .map(|provider| BackendCandidate {
                model: provider
                    .upstream_model
                    .clone()
                    .unwrap_or_else(|| requested_model.to_string()),
                context_limit: None,
            })
            .collect();
        BackendCandidatePlan { candidates }
    }

    /// D4: a bare failover chain (no routing wrapper) reports each provider with
    /// no `route` and no catalog metadata (a failover chain loads no per-chain
    /// `/v1/models` snapshot — only the routing client populates catalog meta).
    fn provider_health(&self) -> Vec<ProviderHealth> {
        self.provider_health_with_route(None, CatalogMeta::default())
    }
}

#[async_trait]
impl UpstreamClient for RoutingUpstreamClient {
    async fn stream_chat_completion(
        &self,
        backend: &BackendChatRequest,
    ) -> AppResult<UpstreamStream> {
        self.stream_chat_completion_with_timeout(backend, Duration::from_secs(60))
            .await
    }

    /// Typed backend-candidate plan using the SAME route/catalog resolution as
    /// `stream_chat_completion` (G4 review #2 + round-2 #1). Each candidate
    /// carries its per-provider context limit for G3 budgeting (T9); the
    /// `genuine` signal is engine-side (see `BackendCandidatePlan`).
    ///
    /// A route resolves to one backend model (route providers are synthetic
    /// single-model upstreams with no `/v1/models` context window ⇒ `None`).
    /// A catalog match dispatches to a routing provider: the PRIMARY target may
    /// fail over across that provider's whole nested failover chain (so all of
    /// its candidate models count), while a fallback/exposed-alias target serves
    /// only that single provider's model. Per-provider context limits come from
    /// the routing catalog's `context_limit_by_id` (populated from the SAME
    /// `/v1/models` snapshot as the candidate ids); nested-failover models not
    /// in this provider's catalog carry `None` (G3 no-ops for them, same as
    /// pre-T9). A catalog-load failure or empty catalog yields an empty candidate
    /// set, which the safe invariant treats as "unknown → strip+offload" and
    /// budgeting treats as no-op.
    async fn backend_candidate_plan(&self, requested_model: &str) -> BackendCandidatePlan {
        let Ok(catalog) = self.load_catalog().await else {
            return BackendCandidatePlan {
                candidates: Vec::new(),
            };
        };
        let Some((resolution, _kind)) = catalog.resolve(requested_model) else {
            return BackendCandidatePlan {
                candidates: Vec::new(),
            };
        };
        let candidates = match resolution {
            RoutingResolution::Route { model_id, .. } => vec![BackendCandidate {
                model: model_id,
                context_limit: None,
            }],
            RoutingResolution::Catalog(candidate) => {
                let Some(provider) = self.providers.get(candidate.provider_index) else {
                    return BackendCandidatePlan {
                        candidates: Vec::new(),
                    };
                };
                // Per-provider context limits come from the SELECTED routing
                // provider's primary `/v1/models` snapshot. They are keyed by
                // the primary's OWN model ids, so the limit is authoritative
                // ONLY for the primary's own model (`candidate.model_id`).
                // Nested-failover / exposed-fallback models are served by OTHER
                // upstreams whose `/v1/models` is not loaded here; looking them
                // up in the primary's map could borrow the WRONG window when an
                // id coincidentally matches, so they carry `None` (G3 no-ops for
                // them — T9 R2 HIGH fix).
                let primary_limits =
                    &catalog.provider_catalogs[candidate.provider_index].context_limit_by_id;
                let primary_limit = primary_limits.get(&candidate.model_id).copied();
                match candidate.target {
                    // Primary: the whole nested failover chain may serve. Only
                    // Primary: the whole nested failover chain may serve. Only
                    // the chain's FIRST candidate (index 0 — the selected
                    // provider's own model, whose `/v1/models` snapshot populates
                    // `primary_limits`) carries `primary_limit`. All later chain
                    // candidates are nested-failover models served by OTHER
                    // upstreams whose `/v1/models` is not loaded here ⇒ `None`
                    // (G3 no-ops for them — never borrow the primary's window for
                    // a different provider, even if the model id matches). This
                    // is provider-identity scoping, not model-string scoping
                    // (T9 R3 HIGH fix).
                    RoutingModelTarget::Primary => provider
                        .client
                        .candidate_backend_models(&candidate.model_id)
                        .await
                        .into_iter()
                        .enumerate()
                        .map(|(index, model)| BackendCandidate {
                            context_limit: (index == 0).then_some(primary_limit).flatten(),
                            model,
                        })
                        .collect(),
                    // Fallback/exposed-alias: served by a nested failover
                    // provider whose own `/v1/models` is not loaded here ⇒ None
                    // (G3 no-ops; the failover provider's window is unknown at
                    // this layer, so no false cap / no wrong-window borrow).
                    RoutingModelTarget::Fallback {
                        failover_provider_index,
                    } => {
                        let model = provider
                            .failover_provider_model(failover_provider_index)
                            .unwrap_or_else(|| candidate.model_id.clone());
                        vec![BackendCandidate {
                            context_limit: None,
                            model,
                        }]
                    }
                }
            }
        };
        BackendCandidatePlan { candidates }
    }

    /// D4: aggregate per-provider health across every routing provider's nested
    /// failover chain (stamping `route = Some(provider_name)` + the published
    /// catalog metadata) AND every synthetic route provider's chain. A catalog
    /// provider's entries carry the routing `(fetched_ms, size)` meta (the union
    /// snapshot that backs catalog resolution); the ad-hoc route providers carry
    /// no catalog meta (they resolve by name, loading no `/v1/models`). Reads are
    /// lock-free over the per-provider metrics + the `Arc<CatalogMeta>` swap, with
    /// only short per-chain cooldown-state lock holds.
    fn provider_health(&self) -> Vec<ProviderHealth> {
        let catalog_meta = **self
            .catalog_meta
            .lock()
            .expect("routing catalog meta lock poisoned");
        let mut health = Vec::new();
        for provider in &self.providers {
            health.extend(
                provider
                    .client
                    .provider_health_with_route(Some(&provider.name), catalog_meta),
            );
        }
        for route_provider in &self.route_providers {
            health.extend(
                route_provider
                    .client
                    .provider_health_with_route(Some(&route_provider.name), CatalogMeta::default()),
            );
        }
        health
    }

    async fn stream_chat_completion_with_timeout(
        &self,
        backend: &BackendChatRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        let catalog = self.load_catalog().await.map_err(|err| {
            tracing::warn!(
                requested_model = %backend.request.model,
                error = %err,
                "failed to load upstream model catalog (is the backend reachable?)"
            );
            err
        })?;
        let (resolution, match_kind) = catalog.resolve(&backend.request.model).ok_or_else(|| {
            tracing::warn!(
                requested_model = %backend.request.model,
                "upstream model catalog is empty; no model to serve (is the backend serving any model?)"
            );
            AppError::upstream("no models are currently available from configured upstreams")
        })?;
        if let RoutingResolution::Route {
            route_provider_index,
            model_id,
        } = &resolution
        {
            let provider = self.route_provider(*route_provider_index)?;
            let routed_request = self.routed_request(backend, model_id, &provider.name, match_kind);
            // D2: this routing layer owns the `route` serving field — tag it with the
            // selected route provider's name (the failover layer below owns `provider`).
            if let Some(serving) = &routed_request.serving {
                serving.set_route(provider.name.clone());
            }
            return provider
                .client
                .stream_chat_completion_with_timeout(&routed_request, request_timeout)
                .await;
        }
        let RoutingResolution::Catalog(resolution) = resolution else {
            unreachable!("route resolution handled above");
        };
        let provider = self
            .providers
            .get(resolution.provider_index)
            .ok_or_else(|| {
                AppError::internal("resolved upstream provider index was out of range")
            })?;
        let routed_request =
            self.routed_request(backend, &resolution.model_id, &provider.name, match_kind);
        // D2: tag the `route` serving field with the selected routing provider's
        // name (first-writer-wins; the nested failover layer owns `provider`).
        if let Some(serving) = &routed_request.serving {
            serving.set_route(provider.name.clone());
        }
        match resolution.target {
            RoutingModelTarget::Primary => {
                provider
                    .client
                    .stream_chat_completion_with_timeout(&routed_request, request_timeout)
                    .await
            }
            RoutingModelTarget::Fallback {
                failover_provider_index,
            } => {
                provider
                    .client
                    .stream_chat_completion_with_timeout_from_provider(
                        failover_provider_index,
                        &routed_request,
                        request_timeout,
                    )
                    .await
            }
        }
    }

    async fn count_tokens(&self, backend: &BackendChatRequest) -> AppResult<Option<u64>> {
        let catalog = match self.load_catalog().await {
            Ok(catalog) => catalog,
            Err(err) => {
                tracing::debug!(error = %err, "cannot route token-count request without model catalog");
                return Ok(None);
            }
        };
        let Some((resolution, match_kind)) = catalog.resolve(&backend.request.model) else {
            return Ok(None);
        };
        if let RoutingResolution::Route {
            route_provider_index,
            model_id,
        } = &resolution
        {
            let provider = self.route_provider(*route_provider_index)?;
            let routed = self.routed_request(backend, model_id, &provider.name, match_kind);
            return provider.client.count_tokens(&routed).await;
        }
        let RoutingResolution::Catalog(resolution) = resolution else {
            unreachable!("route resolution handled above");
        };
        let provider = self
            .providers
            .get(resolution.provider_index)
            .ok_or_else(|| {
                AppError::internal("resolved upstream provider index was out of range")
            })?;
        let routed = self.routed_request(backend, &resolution.model_id, &provider.name, match_kind);
        match resolution.target {
            RoutingModelTarget::Primary => provider.client.count_tokens(&routed).await,
            RoutingModelTarget::Fallback {
                failover_provider_index,
            } => {
                provider
                    .client
                    .count_tokens_from_provider(failover_provider_index, &routed)
                    .await
            }
        }
    }

    async fn list_models(&self) -> AppResult<reqwest::Response> {
        let catalog = self.load_catalog().await?;
        json_response(catalog.union_body())
    }

    async fn proxy_completions(
        &self,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        let catalog = self.load_catalog().await?;
        let requested_model = proxy_body_model(&body).unwrap_or_default();
        let (resolution, match_kind) = catalog.resolve(&requested_model).ok_or_else(|| {
            tracing::warn!(
                requested_model = %requested_model,
                "upstream model catalog is empty; no model to serve (is the backend serving any model?)"
            );
            AppError::upstream("no models are currently available from configured upstreams")
        })?;
        if let RoutingResolution::Route {
            route_provider_index,
            model_id,
        } = &resolution
        {
            let provider = self.route_provider(*route_provider_index)?;
            log_model_resolution(&requested_model, model_id, &provider.name, match_kind);
            let body = proxy_body_with_model(body, model_id);
            return provider.client.proxy_completions(headers, body).await;
        }
        let RoutingResolution::Catalog(resolution) = resolution else {
            unreachable!("route resolution handled above");
        };
        let provider = self
            .providers
            .get(resolution.provider_index)
            .ok_or_else(|| {
                AppError::internal("resolved upstream provider index was out of range")
            })?;
        log_model_resolution(
            &requested_model,
            &resolution.model_id,
            &provider.name,
            match_kind,
        );
        let body = proxy_body_with_model(body, &resolution.model_id);
        match resolution.target {
            RoutingModelTarget::Primary => provider.client.proxy_completions(headers, body).await,
            RoutingModelTarget::Fallback {
                failover_provider_index,
            } => {
                provider
                    .client
                    .proxy_completions_from_provider(failover_provider_index, headers, body)
                    .await
            }
        }
    }

    async fn supported_model_catalog(&self) -> AppResult<Vec<UpstreamModelEntry>> {
        // Build from the cached union catalog directly (single snapshot) rather
        // than re-serializing `union_body()` through the default `list_models()`
        // path: the union ids are authoritative, and any context length is read
        // from `union_context_limit_by_id`, parsed once at catalog refresh from
        // the same first-seen entries.
        let catalog = self.load_catalog().await?;
        Ok(catalog
            .union_ids
            .into_iter()
            .map(|id| {
                let context_limit = catalog.union_context_limit_by_id.get(&id).copied();
                UpstreamModelEntry { id, context_limit }
            })
            .collect())
    }
}

fn timeout_upstream_stream(
    mut stream: UpstreamStream,
    request_timeout: Duration,
) -> UpstreamStream {
    Box::pin(async_stream::stream! {
        loop {
            match tokio::time::timeout(request_timeout, stream.next()).await {
                Ok(Some(chunk)) => yield chunk,
                Ok(None) => break,
                Err(_) => {
                    yield Err(AppError::upstream("upstream stream timed out".to_string()));
                    break;
                }
            }
        }
    })
}

/// A provider-failure-shaped status: a server error, or a request-timeout/rate-limit
/// another provider might succeed on. Used by the raw `/v1/completions` proxy failover
/// (`proxy_completions_with_provider_indices`) and cross-referenced by
/// [`status_is_request_intrinsic_4xx`]'s disjointness check below, so the two "which
/// statuses trigger which behavior" predicates stay auditable side by side. Formerly
/// `should_failover_proxy_status`; renamed because it is no longer proxy-path-only in
/// spirit (still the only literal call site outside this module's tests, but the E2a
/// leaf now reasons about the same status space via its sibling predicate).
fn status_is_failover_eligible(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

/// E2a: statuses where the REQUEST ITSELF (not the provider) is unacceptable to ANY
/// equivalent backend — a malformed/oversized/unsupported-media request, or the more
/// specific "unprocessable" reject. Retrying it on another provider would fail
/// identically (e.g. an image reaching a text-only upstream), so these must never
/// cool/failover a healthy provider. Disjoint BY CONSTRUCTION
/// from [`status_is_failover_eligible`] (no server error, 408, or 429 is in this set);
/// `debug_assert`ed so the two predicates can never silently start overlapping.
fn status_is_request_intrinsic_4xx(status: StatusCode) -> bool {
    let intrinsic = matches!(status.as_u16(), 400 | 413 | 415 | 422);
    debug_assert!(
        !intrinsic || !status_is_failover_eligible(status),
        "status {status} cannot be both request-intrinsic-terminal and failover-eligible"
    );
    intrinsic
}

/// Resolved per-model finalization policies, built ONCE from config and shared
/// (cheap `Arc` clone) across all leaf clients. Each map is keyed by the FINAL
/// resolved model id (profile name); the leaf looks up the policy for the model
/// it actually POSTs to (post routing/failover/exposed-alias remap), so a
/// routed/failover target gets its OWN model's family/kwargs/effort vocabulary
/// rather than the request alias's (T1).
///
/// This is the typed successor to the engine's pre-routing resolution: the
/// engine no longer threads `template_family` / `upstream_chat_kwargs` down the
/// wire DTO, and `ChatCompletionRequest` carries no `#[serde(skip)]` side-channel
/// fields. The leaf is the single point that knows the FINAL `request.model`.
#[derive(Clone, Default, Debug)]
pub struct BackendFinalizationPolicies {
    /// Per-model reasoning-effort policy (`reasoning_effort_map` + default).
    pub effort: Arc<std::collections::BTreeMap<String, crate::config::ReasoningEffortPolicy>>,
    /// Per-model `template_family` override (normalized `kimi`/`deepseek`).
    pub template_family: Arc<std::collections::BTreeMap<String, String>>,
    /// GLOBAL `template_family` fallback (normalized), applied when no per-model
    /// policy matches the FINAL model.
    pub global_template_family: Option<String>,
    /// Per-model extends-merged `upstream_chat_kwargs`.
    pub upstream_chat_kwargs: Arc<std::collections::BTreeMap<String, JsonMap<String, Value>>>,
    /// GLOBAL `upstream_chat_kwargs` (base layer), merged under the per-model
    /// policy at the leaf.
    pub global_upstream_chat_kwargs: Arc<JsonMap<String, Value>>,
}

impl BackendFinalizationPolicies {
    /// Build the leaf finalization policies from config: effort map,
    /// `template_family` override, and `upstream_chat_kwargs` (global base +
    /// per-model). Built once at startup and shared (cheap `Arc` clone) across
    /// all leaf clients. `pub` so the test harness builds the same policies the
    /// production leaf receives (T1).
    pub fn from_config(config: &crate::config::Config) -> Self {
        Self {
            effort: Arc::new(config.reasoning_effort_policies()),
            template_family: Arc::new(config.template_family_policies()),
            global_template_family: config.global_template_family(),
            upstream_chat_kwargs: Arc::new(config.upstream_chat_kwargs_policies()),
            global_upstream_chat_kwargs: Arc::new(config.global_upstream_chat_kwargs().clone()),
        }
    }

    /// Resolve the `template_family` override for the FINAL provider `model`:
    /// the per-model policy wins (exact then canonical-key match, mirroring
    /// `Config::model_profile` / `reasoning_effort_fragment`), else the global
    /// fallback. `None` means sniff the model id instead.
    fn resolve_family_override(&self, model: &str) -> Option<String> {
        policy_for_model(&self.template_family, model)
            .cloned()
            .or_else(|| self.global_template_family.clone())
    }

    /// The extends-merged `upstream_chat_kwargs` for the FINAL provider `model`:
    /// the per-model policy (exact then canonical-key match) layered over the
    /// global base (per-model wins on conflict). Empty when neither applies.
    fn resolve_chat_kwargs(&self, model: &str) -> JsonMap<String, Value> {
        let mut merged = (*self.global_upstream_chat_kwargs).clone();
        if let Some(per_model) = policy_for_model(&self.upstream_chat_kwargs, model) {
            merge_json_maps(&mut merged, per_model);
        }
        merged
    }
}
/// Look up a per-model policy in `map` for the FINAL provider `model` with the
/// SAME semantics as `Config::model_profile` and `RoutingModelCatalog` model
/// matching: exact (case-sensitive) id first, then a canonical-key match
/// (case/punctuation-insensitive) ONLY when unambiguous (exactly one profile
/// shares that canonical key — two would make the pick order-dependent). `None`
/// when no policy applies. Keeps the leaf's per-model policy lookup consistent
/// with how profiles are matched everywhere else (T1).
fn policy_for_model<'a, V>(
    map: &'a std::collections::BTreeMap<String, V>,
    model: &str,
) -> Option<&'a V> {
    if let Some(policy) = map.get(model) {
        return Some(policy);
    }
    let key = canonical_model_key(model);
    let mut matches = map
        .iter()
        .filter(|(name, _)| canonical_model_key(name) == key)
        .map(|(_, policy)| policy);
    match (matches.next(), matches.next()) {
        (Some(policy), None) => Some(policy),
        _ => map.get("*"),
    }
}

/// Mutable serving identity shared (via `Arc<ServingToken>`) from the engine that
/// mints it down to the routing + failover layers that fill it in. The route name
/// and serving provider are written by DIFFERENT actors at DIFFERENT times on the
/// SAME shared token (routing sets `route` when it selects a provider; failover's
/// `mark_provider_success` sets `provider` once a provider produces a first
/// chunk), so the fields live behind a `Mutex` for interior mutability — the
/// `Arc<ServingToken>` itself is cheap to `Clone` (it shares) and the token derives
/// `Debug`, keeping `BackendChatRequest`'s `#[derive(Debug, Clone)]` intact with no
/// `Arc<dyn Fn>` callback (D2).
#[derive(Debug, Default)]
struct ServingInfo {
    route: Option<String>,
    provider: Option<String>,
    /// D5 R3 (MEDIUM): the resolved served model + final cumulative usage, carried on
    /// the token so the L1 telemetry guard can record terminal metrics from the token
    /// (which it already holds) even after the FlowStore record is pruned/evicted — the
    /// authoritative metrics layer must not undercount completed requests when retention
    /// drops the record before finalize. `usage` is last-write (the cumulative total
    /// grows across chunks, mirroring the FlowStore `record_usage` upsert).
    ///
    /// `model_served` is **actual-serving-attempt-wins** (D5 R4 MEDIUM), NOT
    /// first-writer-wins: the engine pre-writes its PRE-routing guess (a fallback for the
    /// no-leaf / error-before-dispatch path), but failover/routing rewrite `request.model`
    /// before the leaf POSTs, so the leaf — the single point that sees the FINAL on-wire
    /// model after provider remap + `sanitize_chat_request` — overwrites the guess with
    /// the model the backend actually answered as. `model_finalized` records whether a
    /// leaf has finalized the model so a later engine guess (e.g. a failover rebuild
    /// re-entering `run_turn`) can never clobber the authoritative leaf value.
    model_served: Option<String>,
    model_finalized: bool,
    usage: Option<crate::dashboard_flow::FlowUsage>,
    /// Gap 03: the ordered per-attempt failover trace. The failover loop pushes one
    /// [`Attempt`](crate::dashboard_flow::Attempt) per provider it tries (failed ones +
    /// the served one); the bare-leaf/single-upstream path pushes exactly one. Carried on
    /// the shared token (like `usage`) so the L1 telemetry guard reads the complete trace
    /// at finalize for BOTH the FlowStore record AND the evict-safe terminal metrics
    /// payload (spec 12's source), independent of FlowStore retention.
    attempts: Vec<crate::dashboard_flow::Attempt>,
    /// Gap 03: the flow-level wire time-to-first-byte (epoch-ms the FIRST upstream chunk
    /// of the SERVING attempt arrived on the wire). First-write-wins so the served
    /// attempt's measured value is never clobbered. `None` until a chunk arrives — NEVER
    /// `0` when unmeasured (don't-lie-with-zeros).
    first_upstream_byte_ms: Option<u128>,
    /// Gap 03 round-1 review (F1): a PER-ATTEMPT scratch slot holding the epoch-ms the
    /// CURRENT attempt's upstream RESPONSE HEADERS arrived on the wire — the instant
    /// `logged_send_chat_request` (the `send().await`) returned, stamped by the leaf for
    /// BOTH a 2xx and a non-2xx response (the status is inspected only AFTER the headers
    /// land). This is the TRUE wire TTFB: the prior code stamped a served attempt only when
    /// the first parsed SSE chunk was yielded, and recorded `None` for an HTTP-status
    /// failure even though headers had already been received. The failover loop / bare-leaf
    /// `arm`s the slot to `None` before each dispatch and `take`s it after, so a connect /
    /// timeout-BEFORE-response (where `send().await` errored and the leaf never stamped)
    /// correctly leaves it `None`. Attempts within one flow are dispatched STRICTLY
    /// sequentially (the failover loop awaits each fully before the next), so a single slot
    /// is race-free; it is scratch state, NOT part of the persisted trace.
    attempt_header_byte_ms: Option<u128>,
    /// Gap 05 round-1 review (F1): the PENDING upstream RESPONSE/ERROR body for the
    /// TURN — staged here on the shared token instead of committed straight onto the
    /// FlowStore record at the leaf, so the body that finally lands reflects the turn's
    /// FINAL outcome, not a transient earlier attempt. Round-2 (HIGH): the slot is CLEARED
    /// at the START of EACH attempt (the failover loop + the bare-leaf dispatch, the same
    /// per-attempt reset seam as `attempt_header_byte_ms`), so it holds at most the CURRENT
    /// attempt's body. A failed HTTP-status attempt stages its body; a body-less failure
    /// (connect/timeout/prefetch-stream-error) stages nothing; a provider that SUCCESSFULLY
    /// serves also CLEARS it. So whatever sits here at finalize is exactly the FINAL
    /// attempt's outcome. The L1 telemetry guard `take`s it at finalize and commits it via
    /// `set_upstream_response`, so the record carries a body IFF the turn's FINAL attempt
    /// failed WITH an HTTP body: A 500 → B 200 leaves this `None` (cleared on B's serve);
    /// A 500 → B 503 leaves B's (final) error body to commit; A 500 → B
    /// connect/timeout/no-first-chunk leaves it `None` (B's start-of-attempt clear wins and
    /// B stages nothing). Already a redacted + capped [`CapturedResponseBody`] (the leaf
    /// passes the same capped/redacting capture), so no `Bytes` slice of the inbound
    /// middleware buffer is retained. Gated at the leaf (`is_response_capture_enabled`), so
    /// when capture is off nothing is staged.
    pending_response_body: Option<crate::dashboard_flow::CapturedResponseBody>,
}

/// Interior-mutable serving identity for one flow. A FRESH one is allocated per
/// `stream_responses` flow by the engine, so concurrent flows never overwrite each
/// other's `{route, provider}` (the rev2 cross-flow race). The layers below set
/// ONLY their own field (`set_route`/`set_provider`, first-writer-wins); D3 reads
/// the pair at finalize via [`ServingToken::snapshot`].
#[derive(Debug, Default)]
pub struct ServingToken {
    inner: Mutex<ServingInfo>,
}

impl ServingToken {
    fn lock(&self) -> std::sync::MutexGuard<'_, ServingInfo> {
        self.inner.lock().expect("serving token lock poisoned")
    }

    /// Record the selected route name (routing layer). First write wins so a
    /// fallback retry cannot clobber the route the flow actually served on.
    pub fn set_route(&self, route: impl Into<String>) {
        let mut info = self.lock();
        if info.route.is_none() {
            info.route = Some(route.into());
        }
    }

    /// Record the serving provider name (failover layer, on first-chunk success;
    /// or the synthetic `"primary"` tag on the bare leaf path). First write wins so
    /// the token reflects the ACTUAL serving provider, not a fallback that was tried
    /// and skipped (AGENTS.md steering).
    pub fn set_provider(&self, provider: impl Into<String>) {
        let mut info = self.lock();
        if info.provider.is_none() {
            info.provider = Some(provider.into());
        }
    }

    /// Record the engine's PRE-routing served-model GUESS (D5 R3/R4 MEDIUM). The engine
    /// sets this once in `run_turn` from the resolved (but still pre-routing)
    /// `upstream_model` so the L1 guard can attribute a metrics bucket even on a path that
    /// never reaches the leaf (an error before dispatch). It is a FALLBACK only: it never
    /// overwrites a leaf-finalized model (`model_finalized`), and among guesses it is
    /// first-writer-wins. The leaf's [`set_model_served_final`] is authoritative — see the
    /// `model_served` field doc for why pre-routing attribution misroutes failover/routing.
    pub fn set_model_served(&self, model: impl Into<String>) {
        let mut info = self.lock();
        if !info.model_finalized && info.model_served.is_none() {
            info.model_served = Some(model.into());
        }
    }

    /// Record the leaf-FINALIZED served model (D5 R4 MEDIUM): the actual on-wire
    /// `request.model` after provider rewrite + `sanitize_chat_request`, the same model
    /// the leaf captures via `set_upstream`. **Actual-serving-attempt-wins** — it
    /// overwrites the engine's pre-routing guess (and any earlier failed-provider leaf
    /// write), and marks the model finalized so a later engine guess cannot clobber it. On
    /// failover/routing the LAST leaf to run is the serving (or last-tried) provider, so
    /// the metrics bucket attributes the model the backend actually answered as — not the
    /// requested / pre-routing one.
    pub fn set_model_served_final(&self, model: impl Into<String>) {
        let mut info = self.lock();
        info.model_served = Some(model.into());
        info.model_finalized = true;
    }

    /// Record the flow's FINAL cumulative usage (D5 R3 MEDIUM), last-write-wins —
    /// mirrors the FlowStore `record_usage` upsert (the cumulative total grows across
    /// chunks). The L1 guard reads it at finalize so the metrics token sum survives a
    /// record eviction.
    pub fn set_usage(&self, usage: crate::dashboard_flow::FlowUsage) {
        self.lock().usage = Some(usage);
    }

    /// Gap 03: append one upstream-dispatch [`Attempt`](crate::dashboard_flow::Attempt)
    /// to the ordered failover trace (the failover loop calls this per provider; the
    /// bare-leaf path calls it once). If the attempt SERVED, also record its wire
    /// first-byte time as the flow-level `first_upstream_byte_ms` (first-write-wins, so
    /// the FIRST served attempt's measured value wins and a later finalize cannot clobber
    /// it with `None`). The order of pushes IS the attempt order the trace renders.
    ///
    /// Round-1 review (F3): the attempt's dynamic scalar strings (`provider`/`model`) are
    /// `cap_scalar`-bounded HERE, at the single retention choke point, so the bounded copy
    /// is what later rides BOTH the `FlowRecord` and the evict-safe `TerminalMetricsInputs`
    /// (the store's `record_attempts` replaces the vector wholesale without re-capping).
    pub fn record_attempt(&self, attempt: crate::dashboard_flow::Attempt) {
        let attempt = attempt.capped();
        let mut info = self.lock();
        if attempt.status == crate::dashboard_flow::AttemptStatus::Served
            && info.first_upstream_byte_ms.is_none()
            && let Some(byte_ms) = attempt.first_upstream_byte_ms
        {
            info.first_upstream_byte_ms = Some(byte_ms);
        }
        info.attempts.push(attempt);
    }

    /// Gap 03: the `(attempts, first_upstream_byte_ms)` snapshot the L1 telemetry guard
    /// reads at finalize — for BOTH the FlowStore record and the evict-safe terminal
    /// metrics payload (spec 12's source). Cloning the small trace under the lock keeps it
    /// independent of FlowStore retention.
    pub fn attempts_snapshot(&self) -> (Vec<crate::dashboard_flow::Attempt>, Option<u128>) {
        let info = self.lock();
        (info.attempts.clone(), info.first_upstream_byte_ms)
    }

    /// Gap 03 round-1 review (F1): arm the per-attempt wire-header-byte slot to `None`
    /// before a dispatch is issued. The leaf stamps it (via [`stamp_attempt_header_byte`])
    /// the instant the upstream response headers arrive; the failover loop / bare-leaf
    /// reads it with [`take_attempt_header_byte`] after the dispatch resolves. Arming
    /// before each attempt is what makes a connect/timeout-BEFORE-response leave the slot
    /// `None` (the leaf never reaches the stamp), while an HTTP-status failure — whose
    /// headers DID arrive — carries the real wire byte time.
    pub fn arm_attempt_header_byte(&self) {
        self.lock().attempt_header_byte_ms = None;
    }

    /// Gap 03 round-1 review (F1): record the epoch-ms the CURRENT attempt's upstream
    /// response headers arrived (the `send().await` in `logged_send_chat_request` just
    /// returned), for BOTH a 2xx and a non-2xx response — this is the TRUE on-wire TTFB.
    /// First-write-wins within the armed window so the FIRST response's header time wins
    /// even across the leaf's single context-overflow shrink-and-retry (the retry's later
    /// headers must not overwrite the original wire-first-byte for this attempt).
    pub fn stamp_attempt_header_byte(&self, ms: u128) {
        let mut info = self.lock();
        if info.attempt_header_byte_ms.is_none() {
            info.attempt_header_byte_ms = Some(ms);
        }
    }

    /// Gap 03 round-1 review (F1): read the current attempt's wire-header-byte time
    /// (`None` when no response headers were ever received — a connect/timeout-before-
    /// response failure). The caller uses it as the attempt's `first_upstream_byte_ms`.
    pub fn take_attempt_header_byte(&self) -> Option<u128> {
        self.lock().attempt_header_byte_ms
    }

    /// Gap 05 round-1 review (F1): stage the captured upstream RESPONSE/ERROR `body` of a
    /// FAILED attempt as the turn's PENDING body. The leaf calls this (gated on
    /// `is_response_capture_enabled`) at its terminal-error sites instead of writing the
    /// FlowStore record directly, so a body is committed only after the failover layer has
    /// decided the turn's final outcome. Round-2 (HIGH): the failover loop (and the
    /// bare-leaf dispatch) CLEARS the slot at the START of each attempt
    /// ([`clear_pending_response_body`]), so the committed body is the FINAL attempt's, not
    /// the last attempt that HAPPENED to carry one: a final HTTP-status failure re-stages
    /// here and commits its body, while a final body-less failure
    /// (connect/timeout/prefetch-stream-error) — which never calls this — leaves the slot
    /// cleared and commits `None`. Within ONE attempt this is still last-writer-wins (the
    /// shrink-and-retry's body overwrites the first send's). The slot is also CLEARED the
    /// instant a later provider serves. `body` is already redacted + capped, so no `Bytes`
    /// slice of the inbound middleware buffer (bounded by `max_request_body_bytes`) is held.
    pub fn set_pending_response_body(&self, body: crate::dashboard_flow::CapturedResponseBody) {
        self.lock().pending_response_body = Some(body);
    }

    /// Gap 05 — discard any pending upstream ERROR body so the FINAL attempt's outcome
    /// decides the committed body. Called at TWO seams: (1) round-2 (HIGH) — at the START
    /// of EACH provider attempt in the failover loop (and the bare-leaf dispatch), the SAME
    /// per-attempt reset point as `arm_attempt_header_byte`, so an earlier provider's staged
    /// body never survives a LATER attempt that fails WITHOUT an HTTP body
    /// (connect/timeout/prefetch-stream-error); (2) round-1 (F1) — at the failover
    /// serve-success seam (the SAME point `set_provider` tags the serving provider), so a
    /// served turn commits no failure body. With the per-attempt clear, an HTTP-status
    /// failure RE-STAGES its own body via `set_pending_response_body` and a body-less
    /// failure leaves the slot cleared, so the committed body is exactly the FINAL attempt's
    /// (its body for a final HTTP-status failure, `None` for a final body-less failure or a
    /// served turn). Idempotent.
    pub fn clear_pending_response_body(&self) {
        self.lock().pending_response_body = None;
    }

    /// Gap 05 round-1 review (F1): the L1 telemetry guard `take`s the turn's pending
    /// upstream ERROR body at finalize and commits it via `set_upstream_response`. `Some`
    /// IFF the turn ultimately FAILED with a captured body (a served turn cleared it); takes
    /// (moves) it out so a re-finalize does not re-commit. `None` when capture was off, no
    /// upstream error body was produced, or a later provider served.
    pub fn take_pending_response_body(
        &self,
    ) -> Option<crate::dashboard_flow::CapturedResponseBody> {
        self.lock().pending_response_body.take()
    }

    /// `(route, provider)` snapshot for D3's finalize attribution.
    pub fn snapshot(&self) -> (Option<String>, Option<String>) {
        let info = self.lock();
        (info.route.clone(), info.provider.clone())
    }

    /// `(model_served, usage)` snapshot for D5's evict-safe terminal metrics (the L1
    /// guard reads this at finalize alongside `snapshot()`'s route/provider).
    pub fn metrics_snapshot(&self) -> (Option<String>, Option<crate::dashboard_flow::FlowUsage>) {
        let info = self.lock();
        (info.model_served.clone(), info.usage)
    }
}

/// Leaf-boundary wrapper carrying finalization metadata that does NOT belong on
/// the wire DTO `ChatCompletionRequest`. Built at the leaf boundary (in the
/// engine, before dispatch) from per-model policies keyed by the FINAL
/// `request.model` after routing/failover rewrite. Never crosses a serde
/// boundary (no serde derives; it is not serialized to the wire).
///
/// `pub` only because the `pub UpstreamClient` trait's `stream_chat_completion`
/// takes it (and `Gateway::upstream_client()` returns `Arc<dyn UpstreamClient>`
/// publicly, so the trait is `pub`). The crate is a binary gateway with no
/// external consumer; the wrapper is an internal seam, not a public API surface.
///
/// `client_chat_template_kwargs` is the client's EXPLICIT `chat_template_kwargs`
/// (from the inbound request `extra_body`), captured PRE-MERGE. It is NOT
/// re-derivable at the leaf: by the time the leaf runs, `request.extra_body[
/// "chat_template_kwargs"]` already contains global+profile+provider merges, and
/// re-asserting that blend over the forced family default would regress leak
/// prevention (a provider/global `thinking:false` would clobber Kimi's forced
/// `thinking:true`). Threading the pure client value lets the leaf re-overlay
/// ONLY the client's keys so an explicit client value still wins over the family
/// default (precedence: config < family < effort-map < client).
///
/// `response_id` + `serving` are the D2 dashboard identity: the flow's
/// `resp_{uuid}` (so the leaf can join its on-wire capture to the FlowStore record)
/// and the shared `Arc<ServingToken>` the routing/failover layers fill in. Both are
/// `Option` and additive; production sets them ONLY at the engine's
/// `BackendChatRequest::new`, and the failover/routing rebuilds CLONE the `Arc`s
/// forward so the same token threads through the whole dispatch.
///
/// `capture` is F1's (Topic F) durable-turn-capture handle -- see its field doc
/// below.
#[derive(Debug, Clone)]
pub struct BackendChatRequest {
    pub request: ChatCompletionRequest,
    pub client_chat_template_kwargs: Option<JsonMap<String, Value>>,
    /// Explicit Anthropic thinking state. `None` for native Chat/Responses
    /// requests; `Some` lets the final provider leaf select family-correct
    /// template kwargs after routing and failover model rewrites.
    pub thinking_override: Option<bool>,
    /// The flow's `resp_{uuid}` API id (D2), used to key the leaf's on-wire
    /// upstream-body capture into the FlowStore. `None` outside the engine path
    /// (tests / the failover rebuild before the engine sets it).
    pub response_id: Option<String>,
    /// Shared mutable serving identity (D2). Allocated fresh per flow by the engine;
    /// the routing layer sets `route`, the failover layer sets `provider`.
    pub serving: Option<Arc<ServingToken>>,
    /// F1d: the turn's durable-capture handle (Topic F, `turn_capture.rs`), when
    /// `turn_capture_dir` is configured and this flow is instrumented -- the SAME
    /// `Arc<TurnCaptureState>` the engine's `CaptureGuard`/registry hold (looked up
    /// from `Gateway::turn_capture().state(api_call_id)` at the single production
    /// construction site, `engine.rs`'s `run_turn`). `None` when capture is
    /// disabled/uninstrumented or in a test that does not exercise it (every
    /// `BackendChatRequest::new` call defaults this to `None`; use
    /// [`with_capture`](Self::with_capture) to attach one).
    ///
    /// The failover/routing rebuilds (`request_for_provider`, `routed_request`)
    /// CLONE this `Arc` forward -- for free, mirroring `response_id`/`serving` --
    /// so a captured turn keeps capturing across a provider rebuild (AC-11).
    /// Diagnostic file IO stays OFF the `ServingToken`/serving-metrics mutex
    /// (spec Design #3) precisely because the carrier is here, not on
    /// `ServingToken`.
    pub capture: Option<Arc<TurnCaptureState>>,
}

impl BackendChatRequest {
    /// Wrap a wire request with the client's explicit `chat_template_kwargs`
    /// (captured pre-merge by the engine) plus the D2 dashboard identity
    /// (`response_id` + the shared `serving` token). The leaf resolves family/kwargs
    /// from policies keyed by `request.model` inside `finalize_request_for_backend`.
    /// This is the SINGLE production construction point that sets `response_id` +
    /// `serving`; the failover/routing rebuilds clone the `Arc`s forward and the
    /// test helpers pass `None` for both. `capture` defaults to `None` here (the
    /// many existing call sites, mostly tests, do not care about it); the engine
    /// attaches one via [`with_capture`](Self::with_capture).
    pub fn new(
        request: ChatCompletionRequest,
        client_chat_template_kwargs: Option<JsonMap<String, Value>>,
        response_id: Option<String>,
        serving: Option<Arc<ServingToken>>,
    ) -> Self {
        Self {
            request,
            client_chat_template_kwargs,
            thinking_override: None,
            response_id,
            serving,
            capture: None,
        }
    }

    /// F1d: attach the turn-capture handle (builder-style, so the many existing
    /// `BackendChatRequest::new(...)` call sites -- almost all tests that do not
    /// exercise capture -- are unaffected). Only the engine's production
    /// construction site (`engine.rs`'s `run_turn`) calls this.
    pub fn with_capture(mut self, capture: Option<Arc<TurnCaptureState>>) -> Self {
        self.capture = capture;
        self
    }

    pub fn with_thinking_override(mut self, thinking: Option<bool>) -> Self {
        self.thinking_override = thinking;
        self
    }
}

/// Backend chat-template contract a model speaks. vLLM/SGLang expose
/// reasoning through different `chat_template_kwargs` per model family, so we
/// inject the right knobs (G2). Mirrors claude-relay `backend.py`.
///
/// Detection + injection live HERE, in the upstream client, rather than in the
/// engine: routing/failover/exposed-alias paths rewrite the actual provider
/// model AFTER the engine resolves its model (`request_for_provider` /
/// `RoutingUpstreamClient::stream_chat_completion`). Sniffing the family from
/// the engine's model would send e.g. Kimi kwargs to a DeepSeek fallback (or
/// none to a Kimi fallback). The leaf is the single point that always sees the
/// FINAL `request.model` with provider `upstream_chat_kwargs` already merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ModelFamily {
    Kimi,
    DeepSeek,
}

/// Resolve the backend family for the FINAL provider model. An explicit
/// `template_family` override (already normalized to `kimi`/`deepseek`) wins;
/// otherwise sniff the model id case-insensitively. The concrete provider model
/// is authoritative — a stale configured `upstream_model`, an exposed alias, or
/// a failover remap must not push real DeepSeek traffic through Kimi mutation
/// (claude-relay lesson, now enforced at the layer that knows the real model).
///
/// Unlike claude-relay (which defaults unrecognized names to DeepSeek),
/// llmconduit returns `None` for anything unrecognized: the gateway serves
/// arbitrary OpenAI-compatible upstreams (glm, qwen, gpt, ...), and injecting
/// DeepSeek `enable_thinking` into those would be a regression.
pub(crate) fn detect_model_family(
    resolved_model: &str,
    family_override: Option<&str>,
) -> Option<ModelFamily> {
    match family_override {
        Some("kimi") => return Some(ModelFamily::Kimi),
        Some("deepseek") => return Some(ModelFamily::DeepSeek),
        _ => {}
    }
    let name = resolved_model.to_ascii_lowercase();
    if name.contains("kimi") {
        Some(ModelFamily::Kimi)
    } else if name.contains("deepseek") {
        Some(ModelFamily::DeepSeek)
    } else {
        None
    }
}

/// Finalize the chat request the backend will receive, at the leaf — the single
/// point that knows the FINAL provider `model` after routing/failover/exposed-
/// alias remap. Resolves and applies that model's `upstream_chat_kwargs` (global
/// base + per-model policy, request-wins), its `template_family` override (per-
/// model policy else global), its reasoning-effort policy, then injects family
/// `chat_template_kwargs`. `request.reasoning_effort` arrives RAW (lowering does
/// not clamp); here it is either MAPPED or clamped.
///
/// MAPPED: the per-model `reasoning_effort_map` produces a fragment, so the
/// top-level field is CLEARED (the fragment relays effort via
/// chat_template_kwargs; a leftover top-level value would be ignored by GLM or
/// seed a contradictory DeepSeek setdefault), then the fragment is applied AFTER
/// family so it overrides family defaults.
///
/// UNMAPPED: clamped to the OpenAI-compatible `none`/`low`/`high` vocabulary.
///
/// The G3 estimate omits `reasoning_effort` entirely, so neither clearing nor
/// clamping here perturbs the pre-flight lower bound.
///
/// `pub` so the integration-test mock upstreams mirror the production leaf.
pub fn finalize_request_for_backend(
    backend: &mut BackendChatRequest,
    policies: &BackendFinalizationPolicies,
) {
    let request = &mut backend.request;
    // 1. Per-model `upstream_chat_kwargs` (global base + per-model policy),
    //    gap-filling into `extra_body` (request-wins: keys already set by the
    //    engine's request-extra merge or by `request_for_provider`'s provider
    //    kwargs are preserved). This replaces the engine's pre-routing
    //    `build_upstream_extra_body` defaults (T1): the leaf now resolves kwargs
    //    against the FINAL model, so a routed/failover cross-family target gets
    //    its OWN kwargs, not the alias's.
    merge_chat_kwargs_gap_fill(request, &policies.resolve_chat_kwargs(&request.model));
    // 2. Reasoning effort: map (→ fragment, top-level cleared) or clamp.
    let effort_policy = policy_for_model(&policies.effort, &request.model);
    let fragment = reasoning_effort_fragment(
        &policies.effort,
        &request.model,
        request.reasoning_effort.as_deref(),
    );
    let effective_thinking = backend.thinking_override.map(|enabled| {
        enabled
            && !request
                .reasoning_effort
                .as_deref()
                .is_some_and(|effort| effort.trim().eq_ignore_ascii_case("none"))
            && !fragment.as_ref().is_some_and(fragment_disables_thinking)
    });
    request.reasoning_effort = if fragment.is_some() {
        None
    } else if effort_policy.is_some_and(|policy| policy.upstream_reasoning.is_some()) {
        request
            .reasoning_effort
            .as_deref()
            .map(str::trim)
            .filter(|effort| !effort.is_empty())
            .map(str::to_string)
    } else {
        clamp_reasoning_effort(request.reasoning_effort.as_deref())
    };
    backend.thinking_override = effective_thinking;
    // 3. Family kwargs from the FINAL model + resolved override.
    apply_family_chat_template_kwargs(backend, policies);
    // 4. Effort fragment after family (effort-map > family default).
    if let Some(fragment) = fragment {
        apply_reasoning_effort_fragment(backend, &fragment);
    }
    // 5. Anthropic thinking intent is dynamic. A typed profile can select a
    // custom template parameter/value; otherwise use the upstream default on
    // generic and DeepSeek backends. Kimi deliberately keeps its parser-side
    // thinking enabled to prevent raw chain-of-thought leakage, with response
    // suppression handling a client-side off request.
    apply_profile_thinking_kwarg(backend, policies);
}

fn fragment_disables_thinking(fragment: &Value) -> bool {
    fragment
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .is_some_and(|effort| effort.eq_ignore_ascii_case("none"))
        || fragment
            .get("chat_template_kwargs")
            .and_then(Value::as_object)
            .is_some_and(|kwargs| {
                kwargs.get("enable_thinking") == Some(&Value::Bool(false))
                    || kwargs.get("thinking") == Some(&Value::Bool(false))
            })
}

fn apply_profile_thinking_kwarg(
    backend: &mut BackendChatRequest,
    policies: &BackendFinalizationPolicies,
) {
    let Some(enabled) = backend.thinking_override else {
        return;
    };
    let reasoning_config = policy_for_model(&policies.effort, &backend.request.model)
        .and_then(|policy| policy.upstream_reasoning.as_ref());
    let family_override = policies.resolve_family_override(&backend.request.model);
    let family = detect_model_family(&backend.request.model, family_override.as_deref());
    let (name, value) = if let Some(config) = reasoning_config {
        (
            config.thinking_param_name.clone(),
            config.thinking_param_value(enabled).clone(),
        )
    } else {
        if family == Some(ModelFamily::Kimi) {
            return;
        }
        ("enable_thinking".to_string(), Value::Bool(enabled))
    };
    let entry = backend
        .request
        .extra_body
        .entry("chat_template_kwargs".to_string())
        .or_insert_with(|| Value::Object(JsonMap::new()));
    if !entry.is_object() {
        *entry = Value::Object(JsonMap::new());
    }
    if let Value::Object(kwargs) = entry {
        // The Anthropic request explicitly states thinking on/off, so it wins
        // over static profile and client passthrough defaults.
        kwargs.insert(name, value);
    }
}

/// Merge `upstream_chat_kwargs` `defaults` into the request `extra_body` with
/// REQUEST-WINS semantics. The single gap-fill helper shared by BOTH the
/// leaf-finalize path (`finalize_request_for_backend`, per-model kwargs) and the
/// provider-fallback path (`request_for_provider`, provider kwargs): a key
/// already explicitly set on the request (typed field or `extra_body`) is
/// preserved; a configured default fills the gap. Deep-merge nested objects so a
/// configured object composes with a request object rather than clobbering
/// sibling keys. The max-token-alias skip is ALWAYS applied (no-op when the
/// client expressed no alias), so the alias-collision guard protects both paths.
fn merge_chat_kwargs_gap_fill(
    request: &mut ChatCompletionRequest,
    defaults: &JsonMap<String, Value>,
) {
    // Max-token aliases (`max_tokens`/`max_output_tokens`/`max_completion_tokens`)
    // are ONE logical knob. If the client expressed ANY of them (typed
    // `max_output_tokens` OR an alias in `request.extra_body`), skip ALL of them
    // from the configured defaults so a stale config alias cannot land alongside
    // the explicit request value and shadow it (mirrors the engine's pre-T1
    // `remove_defaults_shadowed_by_request_extra`).
    let max_token_requested = request.max_output_tokens.is_some()
        || MAX_TOKEN_ALIAS_KEYS
            .iter()
            .any(|alias| request.extra_body.contains_key(*alias));
    for (key, value) in defaults {
        if max_token_requested && MAX_TOKEN_ALIAS_KEYS.contains(&key.as_str()) {
            continue;
        }
        if key == "parallel_tool_calls" {
            if request.parallel_tool_calls.is_none() {
                request.parallel_tool_calls = value.as_bool();
            }
            continue;
        }
        if chat_request_field_is_set(request, key) {
            continue;
        }
        match request.extra_body.get_mut(key) {
            Some(existing) => merge_json_value_preserve_destination(existing, value),
            None => {
                request.extra_body.insert(key.clone(), value.clone());
            }
        }
    }
}

/// Clamp a raw reasoning-effort level to the OpenAI-compatible vocabulary a
/// mapless backend understands: `none`/`low` pass through, everything else
/// (`medium`/`high`/`xhigh`/`max`/unknown) collapses to `high`. This is the
/// model-agnostic default for backends without a `reasoning_effort_map`.
fn clamp_reasoning_effort(effort: Option<&str>) -> Option<String> {
    let level = effort.map(str::trim).filter(|level| !level.is_empty())?;
    let clamped = match level.to_ascii_lowercase().as_str() {
        "none" => "none",
        "low" => "low",
        _ => "high",
    };
    Some(clamped.to_string())
}

/// Resolve the reasoning-effort fragment for the FINAL provider `model` and the
/// raw client effort level, or `None` when the model has no policy or the level
/// (after defaulting) is not mapped. Lookup is exact, then canonical-key
/// (case/punctuation-insensitive), mirroring catalog/profile model matching.
fn reasoning_effort_fragment(
    policies: &std::collections::BTreeMap<String, crate::config::ReasoningEffortPolicy>,
    model: &str,
    raw_effort: Option<&str>,
) -> Option<Value> {
    let policy = policy_for_model(policies, model)?;
    let level = raw_effort
        .map(str::trim)
        .filter(|level| !level.is_empty())
        .map(str::to_ascii_lowercase)
        .or_else(|| policy.default.clone())?;
    policy
        .map
        .get(&level)
        .or_else(|| policy.map.get("*"))
        .cloned()
}

/// Apply a resolved reasoning-effort `fragment` to the request at the leaf, with
/// precedence config < family < effort-map < client. The fragment is deep-merged
/// PREFER-SOURCE into `extra_body` (so it OVERRIDES configured/family defaults
/// already present in `chat_template_kwargs`, fixing the case where a static
/// configured default would otherwise block a per-request mapping), then the
/// client's own explicit `chat_template_kwargs` are re-asserted so an inbound
/// client value still wins. Call AFTER `apply_family_chat_template_kwargs`.
pub fn apply_reasoning_effort_fragment(backend: &mut BackendChatRequest, fragment: &Value) {
    let Value::Object(fragment) = fragment else {
        return;
    };
    let request = &mut backend.request;
    for (key, value) in fragment {
        match request.extra_body.get_mut(key) {
            Some(existing) => merge_json_value_prefer_source(existing, value),
            None => {
                request.extra_body.insert(key.clone(), value.clone());
            }
        }
    }
    // Re-assert the client's explicit chat_template_kwargs over the effort-map
    // fragment (precedence: client > effort-map).
    if let Some(client_kwargs) = &backend.client_chat_template_kwargs
        && let Some(Value::Object(kwargs)) = request.extra_body.get_mut("chat_template_kwargs")
    {
        for (key, value) in client_kwargs {
            match kwargs.get_mut(key) {
                Some(existing) => merge_json_value_prefer_source(existing, value),
                None => {
                    kwargs.insert(key.clone(), value.clone());
                }
            }
        }
    }
}

/// Inject family-specific `chat_template_kwargs` into the request `extra_body`
/// from the FINAL provider model + resolved `template_family` override. Composes
/// with (does not clobber) the already-merged configured/provider
/// `upstream_chat_kwargs`, and re-overlays the client's explicit
/// `chat_template_kwargs` last so an explicit request value still WINS over a
/// forced family default. A no-op when the family is unrecognized.
///
/// Public so the integration test harness's mock upstream can mirror the
/// production leaf and record the request the backend would actually receive.
pub fn apply_family_chat_template_kwargs(
    backend: &mut BackendChatRequest,
    policies: &BackendFinalizationPolicies,
) {
    let family_override = policies.resolve_family_override(&backend.request.model);
    let Some(family) = detect_model_family(&backend.request.model, family_override.as_deref())
    else {
        return;
    };
    let thinking_override = backend.thinking_override;
    let request = &mut backend.request;
    let reasoning_effort = request.reasoning_effort.clone();
    let entry = request
        .extra_body
        .entry("chat_template_kwargs".to_string())
        .or_insert_with(|| Value::Object(JsonMap::new()));
    let kwargs = match entry {
        Value::Object(kwargs) => kwargs,
        // A non-object configured `chat_template_kwargs` is malformed; replace
        // it with a fresh object rather than silently no-op'ing.
        other => {
            *other = Value::Object(JsonMap::new());
            other.as_object_mut().expect("just set to object")
        }
    };
    write_family_kwargs(kwargs, family, &reasoning_effort, thinking_override);
    // Request-supplied keys win over the forced family default (AGENTS.md:
    // request `extra_body` beats configured/injected defaults). Deep-merge so a
    // nested client object overlays the already-merged family/provider object
    // leaf-by-leaf (client wins on conflicts) instead of clobbering sibling keys
    // that came from the deep-merge of configured/provider defaults.
    if let Some(client_kwargs) = &backend.client_chat_template_kwargs {
        for (key, value) in client_kwargs {
            match kwargs.get_mut(key) {
                Some(existing) => merge_json_value_prefer_source(existing, value),
                None => {
                    kwargs.insert(key.clone(), value.clone());
                }
            }
        }
    }
}

/// Recursively overlay `source` onto `destination`, with `source` (the
/// client/request value) winning on conflicting leaves while sibling keys
/// present only in `destination` are preserved. Mirror of
/// `merge_json_value_preserve_destination` with the precedence flipped.
fn merge_json_value_prefer_source(destination: &mut Value, source: &Value) {
    if let Value::Object(destination_object) = destination
        && let Value::Object(source_object) = source
    {
        for (key, source_value) in source_object {
            match destination_object.get_mut(key) {
                Some(destination_value) => {
                    merge_json_value_prefer_source(destination_value, source_value);
                }
                None => {
                    destination_object.insert(key.clone(), source_value.clone());
                }
            }
        }
        return;
    }
    *destination = source.clone();
}

fn write_family_kwargs(
    kwargs: &mut JsonMap<String, Value>,
    family: ModelFamily,
    reasoning_effort: &Option<String>,
    thinking_override: Option<bool>,
) {
    match family {
        ModelFamily::Kimi => {
            // Kimi K2 templates read `thinking`/`preserve_thinking` (bools),
            // NOT the DeepSeek vars. Strip the DeepSeek-only keys so a stale
            // configured default for the wrong family can't leak through.
            for key in ["enable_thinking", "clear_thinking", "reasoning_effort"] {
                kwargs.remove(key);
            }
            // Force thinking ON unconditionally — even when the client did not
            // request reasoning. With `thinking=false` the vLLM kimi parser
            // falls through to the identity parser and leaks the raw chain of
            // thought (plus a stray `</think>`) into `content`. `thinking=true`
            // routes it to `delta.reasoning`, which response adapters forward in
            // their protocol-native reasoning shape. This intentionally
            // OVERRIDES a configured default; an explicit request
            // `chat_template_kwargs.thinking` still wins (re-overlaid after).
            kwargs.insert("thinking".to_string(), Value::Bool(true));
            kwargs.insert("preserve_thinking".to_string(), Value::Bool(true));
        }
        ModelFamily::DeepSeek => {
            // DeepSeek reads `enable_thinking` (+ `reasoning_effort`). Use
            // setdefault semantics: respect any configured default, and don't
            // fight the separately-handled top-level `reasoning_effort`.
            if let Some(thinking) = thinking_override {
                kwargs.insert("enable_thinking".to_string(), Value::Bool(thinking));
            } else {
                kwargs.entry("enable_thinking").or_insert(Value::Bool(true));
            }
            if let Some(effort) = reasoning_effort
                .as_deref()
                .map(str::trim)
                .filter(|effort| !effort.is_empty())
            {
                kwargs
                    .entry("reasoning_effort")
                    .or_insert_with(|| Value::String(effort.to_string()));
            }
        }
    }
}

fn chat_request_field_is_set(request: &ChatCompletionRequest, key: &str) -> bool {
    match key {
        "model" | "messages" | "stream" => true,
        "parallel_tool_calls" => request.parallel_tool_calls.is_some(),
        "tools" => request.tools.is_some(),
        "tool_choice" => request.tool_choice.is_some(),
        "reasoning_effort" => request.reasoning_effort.is_some(),
        "response_format" => request.response_format.is_some(),
        "stream_options" => request.stream_options.is_some(),
        "temperature" => request.temperature.is_some(),
        "top_p" => request.top_p.is_some(),
        "max_tokens" | "max_output_tokens" | "max_completion_tokens" => {
            request.max_output_tokens.is_some()
        }
        "frequency_penalty" => request.frequency_penalty.is_some(),
        "presence_penalty" => request.presence_penalty.is_some(),
        "stop" => request.stop.is_some(),
        _ => false,
    }
}

/// Max-token alias keys that can coexist with the typed `max_output_tokens`
/// field inside the flattened `extra_body`. The engine treats these as one
/// logical knob (`engine.rs` `remove_*` helpers); the leaf shrink-and-retry
/// must strip all of them so a stale oversized alias cannot override the
/// reduced typed `max_tokens` on the retried request.
const MAX_TOKEN_ALIAS_KEYS: [&str; 3] =
    ["max_tokens", "max_output_tokens", "max_completion_tokens"];

/// Remove every max-token alias from `extra_body` so only the typed,
/// reduced `max_output_tokens` (serialized as `max_tokens`) applies on retry.
fn remove_max_token_aliases(extra_body: &mut std::collections::BTreeMap<String, Value>) {
    for key in MAX_TOKEN_ALIAS_KEYS {
        extra_body.remove(key);
    }
}

fn merge_json_value_preserve_destination(destination: &mut Value, source: &Value) {
    if let Value::Object(destination_object) = destination
        && let Value::Object(source_object) = source
    {
        for (key, source_value) in source_object {
            match destination_object.get_mut(key) {
                Some(destination_value) => {
                    merge_json_value_preserve_destination(destination_value, source_value);
                }
                None => {
                    destination_object.insert(key.clone(), source_value.clone());
                }
            }
        }
    }
}

fn proxy_body_for_provider(provider: &FailoverUpstreamProvider, body: &Bytes) -> Bytes {
    match provider.upstream_model.as_deref() {
        Some(model) => proxy_body_with_model(body.clone(), model),
        None => body.clone(),
    }
}

fn proxy_body_model(body: &Bytes) -> Option<String> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("model")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|model| !model.is_empty())
                .map(ToString::to_string)
        })
}

fn proxy_body_with_model(body: Bytes, model: &str) -> Bytes {
    let Ok(mut value) = serde_json::from_slice::<Value>(&body) else {
        return body;
    };
    let Some(object) = value.as_object_mut() else {
        return body;
    };
    object.insert("model".to_string(), Value::String(model.to_string()));
    serde_json::to_vec(&value).map(Bytes::from).unwrap_or(body)
}

pub(crate) fn canonical_model_key(model: &str) -> String {
    model
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
}

fn copy_proxy_request_headers(mut request: RequestBuilder, headers: &HeaderMap) -> RequestBuilder {
    for (name, value) in headers {
        if should_proxy_request_header(name) {
            request = request.header(name.clone(), value.clone());
        }
    }
    request
}

fn should_proxy_request_header(name: &HeaderName) -> bool {
    !is_hop_by_hop_header(name)
        && !header_name_eq(name, "authorization")
        && !header_name_eq(name, "host")
        && !header_name_eq(name, "content-length")
}

/// Small fixed reserve subtracted from the context window when recomputing the
/// completion budget for an exact-token overflow error.
const CONTEXT_RETRY_MARGIN: i64 = 100;
/// Larger reserve used when the reported input-token count is only a lower
/// bound (vLLM's "prompt contains at least N input tokens"). The real prompt is
/// at least that large, so a wider margin avoids chasing a one-token-over
/// boundary across retries.
const CONTEXT_LOWER_BOUND_RETRY_MARGIN: i64 = 1024;

/// A structured decision to retry an upstream request after a context/
/// completion token-limit overflow, carrying the recomputed completion budget.
///
/// `max_completion_tokens` is already clamped to the configured minimum floor;
/// callers reduce the request's `max_completion_tokens` to this value and retry
/// exactly once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextOverflowRetry {
    /// Recomputed `max_completion_tokens` for the retry (>= the min floor).
    pub max_completion_tokens: i64,
    /// The model's context window as reported by the error body.
    pub ctx_limit: i64,
    /// Input/prompt tokens parsed from the error body, when available.
    pub input_tokens: Option<i64>,
    /// Which overflow shape matched: `"completion_limit"` or `"context_limit"`.
    pub reason: &'static str,
    /// True when `input_tokens` is a lower bound ("prompt contains at least N").
    pub input_tokens_is_lower_bound: bool,
}

fn context_retry_completion_budget(
    ctx_limit: i64,
    input_tokens: Option<i64>,
    min_completion_tokens: i64,
    input_tokens_is_lower_bound: bool,
) -> i64 {
    let margin = if input_tokens_is_lower_bound {
        CONTEXT_LOWER_BOUND_RETRY_MARGIN
    } else {
        CONTEXT_RETRY_MARGIN
    };
    let available = ctx_limit - margin - input_tokens.unwrap_or(0);
    available.max(min_completion_tokens)
}

/// vLLM/SGLang output-only limit:
/// `max_completion_tokens=X cannot be greater than max_model_len=Y`.
///
/// Mirrors the reference regex
/// `max_completion_tokens\s*=\s*([\d,]+).*?max_model_len\D*([\d,]+)`
/// (claude-relay `server._context_overflow_retry_from_error`), with the literal
/// `cannot be greater than` comparison pinned BETWEEN the two anchored fields.
/// The reference's bare `.*?` would also match an unrelated validation body
/// that merely names both fields with numbers (e.g. "max_completion_tokens=N is
/// not allowed together with max_model_len=N"); requiring the actual overflow
/// comparison — the exact wording this shape always carries — rejects that
/// false positive while still matching every genuine overflow. Group 1 =
/// requested completion tokens (presence enforces the shape), group 2 =
/// `max_model_len` (the ctx limit).
static COMPLETION_LIMIT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)max_completion_tokens\s*=\s*([\d,]+).*?cannot be greater than.*?max_model_len\D*([\d,]+)",
    )
    .expect("completion-limit overflow regex is valid")
});

/// vLLM combined input+output limit:
/// `maximum context length is X. However, you requested Y output tokens and
/// your prompt contains [at least] Z input tokens`.
///
/// Mirrors the reference regex
/// `maximum context length is\s*([\d,]+).*?requested\s*([\d,]+)\s*output tokens
/// .*?prompt contains(?P<lower_bound>\s+at least)?\s*([\d,]+)\s*input tokens`.
/// The tight `requested\s*([\d,]+)\s*output tokens` adjacency (only whitespace
/// between `requested`, the number and `output tokens`) is what rejects bodies
/// that merely contain the bare words out of shape. Group 1 = ctx limit,
/// `lower_bound` = the optional ` at least` qualifier, group 3 = input tokens.
static VLLM_COMBINED_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)maximum context length is\s*([\d,]+).*?requested\s*([\d,]+)\s*output tokens.*?prompt contains(?P<lower_bound>\s+at least)?\s*([\d,]+)\s*input tokens",
    )
    .expect("vLLM combined overflow regex is valid")
});

/// OpenAI-compatible/SGLang shape:
/// `maximum context length is X tokens. However, you requested Y tokens
/// (Z in the messages, W in the completion)`.
/// The canonical OpenAI wording uses `in the prompt` rather than `in the
/// messages`.
///
/// Mirrors the reference regex
/// `maximum context length is\s*([\d,]+).*?requested\s*([\d,]+)\s*tokens.*?
/// \(([\d,]+)\s*in (?:the )?(?:messages|prompt).*?([\d,]+)\s*in (?:the )?
/// (?:completion|output)`. The literal `\(` before the input count and the
/// tight `requested\s*([\d,]+)\s*tokens` adjacency, plus the required
/// `... in the completion` half, reject bodies that only echo the input-side
/// phrase or carry the clauses out of order. Group 1 = ctx limit, group 3 =
/// input tokens.
static OPENAI_COMPATIBLE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)maximum context length is\s*([\d,]+).*?requested\s*([\d,]+)\s*tokens.*?\(([\d,]+)\s*in (?:the )?(?:messages|prompt).*?([\d,]+)\s*in (?:the )?(?:completion|output)",
    )
    .expect("OpenAI-compatible overflow regex is valid")
});

/// vLLM/SGLang newer shape:
/// `Requested token count exceeds the model's maximum context length of X
/// tokens. You requested a total of Y tokens: Z tokens from the input messages
/// and W tokens for the completion`.
///
/// Mirrors the reference regex
/// `maximum context length of\s*([\d,]+)\s*tokens.*?requested a total of
/// \s*([\d,]+)\s*tokens.*?([\d,]+)\s*tokens from (?:the )?
/// (?:input messages|messages|prompt).*?([\d,]+)\s*tokens for (?:the )?
/// (?:completion|output)`, hardened (like the other shapes) with this shape's
/// DISTINCTIVE LEADING LITERAL `requested token count exceeds`. That phrase is
/// the unique opening sentence this overflow always carries; an unrelated 4xx
/// that merely echoes the generic `maximum context length of …` +
/// `requested a total of N tokens` anchors (e.g. a validation/diagnostics body
/// rejecting some other field) does NOT contain it. Requiring it before the
/// anchors rejects that false positive (Codex round 6: the regex rewrite had
/// dropped this literal an earlier round added) while still matching every
/// genuine overflow. Group 1 = ctx limit, group 3 = input tokens.
static REQUESTED_TOTAL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)requested token count exceeds.*?maximum context length of\s*([\d,]+)\s*tokens.*?requested a total of\s*([\d,]+)\s*tokens.*?([\d,]+)\s*tokens from (?:the )?(?:input messages|messages|prompt).*?([\d,]+)\s*tokens for (?:the )?(?:completion|output)",
    )
    .expect("requested-total overflow regex is valid")
});

/// Coarse, deterministic CONSERVATIVE lower-bound estimate of the input tokens
/// the leaf POSTs for `request`, mirroring `engine::estimate_input_tokens`.
/// Used only by the G1 reactive retry's `COMPLETION_LIMIT_RE` shape (an
/// output-only `max_model_len` error that reports the context window but NOT
/// the prompt size); without it the retry budget would be `ctx_limit - margin`,
/// ignoring the prompt and re-overflowing. `ceil(serialized_bytes / 4)` is the
/// same ~4-bytes-per-token heuristic the engine uses — not a tokenizer. It
/// runs on the sanitized, post-`finalize_request_for_backend` request, so it
/// accounts for any family/effort kwargs merged at the leaf. The other overflow
/// shapes extract the real input count from the error body and ignore this.
fn estimate_leaf_input_tokens(request: &ChatCompletionRequest) -> i64 {
    let bytes = serde_json::to_vec(request).map(|v| v.len()).unwrap_or(0);
    bytes.div_ceil(4) as i64
}

/// Parse a non-streaming upstream error body for a context/completion
/// token-limit overflow and compute the reduced completion budget for a retry.
///
/// Returns `None` for any error text that is not a recognizable token-limit
/// overflow, so the original error is surfaced unchanged (no retry).
///
/// `estimated_input_tokens` supplies the prompt size for the completion-only
/// (`max_model_len`) shape, which does not itself report the input size; the
/// leaf upstream path passes a local estimate from the sanitized request (see
/// [`estimate_leaf_input_tokens`]).
///
/// Classification uses the SAME anchored regexes the reference implementation
/// uses (`server._context_overflow_retry_from_error`, tests
/// `test_non_200_retry_*`): each shape is matched by a precompiled
/// [`Regex`] that mirrors the reference pattern exactly, and token counts are
/// extracted from the capture groups (commas stripped before parsing).
pub fn classify_context_overflow(
    error_body: &str,
    min_completion_tokens: i64,
    estimated_input_tokens: Option<i64>,
) -> Option<ContextOverflowRetry> {
    // vLLM/SGLang output-only limit (group 2 = max_model_len = ctx limit).
    if let Some(captures) = COMPLETION_LIMIT_RE.captures(error_body) {
        let ctx_limit = parse_token_count(&captures[2]);
        return Some(ContextOverflowRetry {
            max_completion_tokens: context_retry_completion_budget(
                ctx_limit,
                estimated_input_tokens,
                min_completion_tokens,
                false,
            ),
            ctx_limit,
            input_tokens: estimated_input_tokens,
            reason: "completion_limit",
            input_tokens_is_lower_bound: false,
        });
    }

    // vLLM combined input+output limit. Groups: 1 = ctx limit, 2 = requested
    // output tokens, 3 (named `lower_bound`) = the optional " at least"
    // qualifier, 4 = input tokens. The `lower_bound` group is a numbered slot in
    // its own right (Rust `regex` counts named captures positionally), so the
    // input count is group 4, not 3.
    if let Some(captures) = VLLM_COMBINED_RE.captures(error_body) {
        let ctx_limit = parse_token_count(&captures[1]);
        let input_tokens = parse_token_count(&captures[4]);
        let is_lower_bound = captures.name("lower_bound").is_some();
        return Some(ContextOverflowRetry {
            max_completion_tokens: context_retry_completion_budget(
                ctx_limit,
                Some(input_tokens),
                min_completion_tokens,
                is_lower_bound,
            ),
            ctx_limit,
            input_tokens: Some(input_tokens),
            reason: "context_limit",
            input_tokens_is_lower_bound: is_lower_bound,
        });
    }

    // OpenAI-compatible/SGLang and the newer "requested a total of" shape both
    // report ctx limit in group 1 and input tokens in group 3.
    if let Some(captures) = OPENAI_COMPATIBLE_RE
        .captures(error_body)
        .or_else(|| REQUESTED_TOTAL_RE.captures(error_body))
    {
        let ctx_limit = parse_token_count(&captures[1]);
        let input_tokens = parse_token_count(&captures[3]);
        return Some(ContextOverflowRetry {
            max_completion_tokens: context_retry_completion_budget(
                ctx_limit,
                Some(input_tokens),
                min_completion_tokens,
                false,
            ),
            ctx_limit,
            input_tokens: Some(input_tokens),
            reason: "context_limit",
            input_tokens_is_lower_bound: false,
        });
    }

    None
}

/// Parse a comma-grouped integer token count from a regex capture (e.g.
/// "202,752" -> 202752). Mirrors the reference `_parse_token_count`.
fn parse_token_count(group: &str) -> i64 {
    group
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap_or(0)
}

/// Convert an already-confirmed-success upstream response into the parsed SSE
/// chunk stream. The first chunk is only produced lazily by the returned
/// stream, so the context-overflow retry (which happens before this is called)
/// remains strictly pre-first-chunk.
async fn stream_success_response(
    response: reqwest::Response,
    max_sse_frame_bytes: usize,
    capture: Option<Arc<TurnCaptureState>>,
) -> AppResult<UpstreamStream> {
    // F1e: tap the RAW pre-parse upstream bytes into the turn's `upstream_response`
    // section BEFORE the G6 frame guard + SSE parser see them -- the raw bytes are
    // the ground truth (a `<think>` leak is a 200 OK; nothing else persists it). The
    // tap COPIES each chunk (no `Bytes`-slice retention, AGENTS.md line 144) and
    // forwards it UNCHANGED, so the guard/parser see identical bytes; a bounded,
    // back-pressured [`TurnCaptureState::upstream_response_sink`] throttles the read
    // to disk pace so a slow disk can never accumulate the whole response in RAM
    // (bounded memory -- NOT an unbounded channel). Only wrapped when capture is
    // enabled, so a disabled turn pays nothing (the byte stream flows straight into
    // the guard as before).
    let byte_stream: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>> =
        match capture {
            // F1e review r3 (Finding 2 -- don't claim a complete empty response): the
            // `upstream_response_streamed` discriminator is NO LONGER set here, at
            // header time. If it were, a bare-leaf cancel AFTER 2xx headers but BEFORE
            // the first `stream.next()` (the tap block never runs) would finalize an
            // EMPTY `upstream_response` with `partial:false` -- lyingly "complete". The
            // tap now marks streamed only once a real byte flows OR the stream reaches a
            // CLEAN end, so a pre-first-byte cancel stays partial/absent (truthful).
            Some(capture) => Box::pin(tap_upstream_response(response.bytes_stream(), capture)),
            None => Box::pin(response.bytes_stream()),
        };
    // Bound the upstream SSE read BEFORE `eventsource()` parses it (G6). The
    // `eventsource-stream` 0.2 parser accumulates bytes into an internal
    // `String`/`Vec` buffer and only flushes on an event boundary (a blank
    // line); a hostile/buggy upstream streaming an oversized or never-terminated
    // frame would grow that buffer without bound (OOM). `bounded_sse_byte_stream`
    // caps the bytes accumulated since the last boundary and surfaces a clean
    // `AppError` before the parser can over-accumulate. The configured request-body
    // cap in `http.rs` is inbound-only and does NOT cover this response path.
    let bounded = bounded_sse_byte_stream(byte_stream, max_sse_frame_bytes);
    let stream = bounded.eventsource().filter_map(|result| async move {
        match result {
            Ok(event) if event.data == "[DONE]" => None,
            Ok(event) => Some(parse_chat_completion_chunk(&event.data).map_err(|err| {
                AppError::upstream(format!(
                    "failed to parse upstream chat chunk: {err}; payload={}",
                    redact_and_truncate_error_body(&event.data, 500)
                ))
            })),
            // The bounded adapter surfaces the frame-cap rejection through the
            // transport-error channel as an already-formed `AppError` (its
            // `Display` carries the cap message); other transport errors are
            // wrapped here. Either way the model output is never silently
            // truncated — the stream ends in an error item.
            Err(err) => Some(Err(AppError::upstream(format!(
                "failed to read upstream SSE: {err}"
            )))),
        }
    });
    Ok(Box::pin(stream))
}

/// F1e: tee the RAW upstream byte stream into the turn's `upstream_response`
/// capture section, forwarding every chunk UNCHANGED. Each chunk is COPIED
/// (`to_vec` -- never a retained `Bytes` slice, AGENTS.md line 144) into the
/// BOUNDED, back-pressured [`ServedSink`]: [`poll_reserve`](crate::turn_capture::ServedSink::poll_reserve)
/// applies flow control BEFORE the copy, so a slow disk throttles the upstream
/// read to disk pace instead of piling the whole response into RAM (bounded
/// memory -- the SAME OOM class F1b's review caught; NO unbounded channel).
///
/// A diagnostic failure must NEVER break or stall the served turn: a sink close /
/// send error, or a mid-stream transport error, drops the sink and marks the
/// section sticky-`partial` -- forwarding continues byte-for-byte WITHOUT capture.
/// The [`UpstreamTapGuard`] closes the section on drop so the assembly barrier
/// resolves in BOUNDED time (no hang): a clean end (the raw stream reached `None`)
/// closes it non-partial; a drop BEFORE that (a G6 frame-cap rejection downstream
/// stops pulling, a client disconnect, an engine drop) closes it `partial`.
fn tap_upstream_response<S>(
    inner: S,
    capture: Arc<TurnCaptureState>,
) -> impl Stream<Item = Result<Bytes, reqwest::Error>> + Send
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static,
{
    async_stream::stream! {
        // `guard` declared FIRST so on an early drop (future dropped mid-await --
        // G6 downstream / disconnect / engine drop) it drops LAST, after `sink`, so
        // both the sink's sender clone and the section close land: the writer then
        // sees all senders gone and terminates (no hang).
        let mut guard = UpstreamTapGuard::new(Arc::clone(&capture));
        let mut sink = capture.upstream_response_sink();
        // F1e review r3 (Finding 2): the streamed discriminator is set on the FIRST
        // real byte (below), NOT at header time -- so a 2xx-headers-then-cancel before
        // any byte never claims a complete (empty) response. A local latch avoids a
        // redundant atomic store per chunk.
        let mut streamed_marked = false;
        let mut inner = std::pin::pin!(inner);
        while let Some(item) = inner.next().await {
            match &item {
                Ok(bytes) if !bytes.is_empty() => {
                    if !streamed_marked {
                        capture.mark_upstream_response_streamed();
                        streamed_marked = true;
                    }
                    if let Some(active) = sink.as_mut() {
                        // Reserve a slot BEFORE copying (bounded memory / back-pressure).
                        match std::future::poll_fn(|cx| active.poll_reserve(cx)).await {
                            Ok(()) => {
                                if active.send(bytes.to_vec()).is_err() {
                                    capture.mark_upstream_response_degraded();
                                    sink = None;
                                }
                            }
                            Err(_) => {
                                capture.mark_upstream_response_degraded();
                                sink = None;
                            }
                        }
                    }
                }
                // An empty frame carries nothing to capture -- forward as-is.
                Ok(_) => {}
                // A mid-stream transport error: the raw capture is truncated.
                Err(_) => {
                    capture.mark_upstream_response_degraded();
                    sink = None;
                }
            }
            // Forward the ORIGINAL chunk UNCHANGED (tee = copy + forward).
            yield item;
        }
        // The raw upstream stream reached a CLEAN end. Mark streamed (idempotent with
        // the first-byte mark above) so that a genuinely empty-but-cleanly-closed
        // upstream response (2xx headers + clean EOS + zero bytes) is a TRUTHFUL
        // `partial:false` empty section -- distinct from a pre-first-byte cancel, which
        // never reaches here and so stays partial/absent (Finding 2). Then drop the
        // sink so the writer sees its clone gone, and mark clean so the guard closes
        // the section non-partial.
        capture.mark_upstream_response_streamed();
        drop(sink);
        guard.mark_clean();
    }
}

/// F1e drop guard for [`tap_upstream_response`]: mirrors `http.rs`'s `TeeBody`
/// `Drop`. Marks the `upstream_response` section sticky-`partial` unless the raw
/// stream reached a clean end, and ALWAYS closes it so the assembly barrier's
/// `await_upstream_response_closed` resolves in bounded time (no hang) -- for the
/// clean end (via the block completing) AND for an early drop (G6 frame-cap
/// rejection downstream, client disconnect, engine drop).
struct UpstreamTapGuard {
    capture: Arc<TurnCaptureState>,
    clean: bool,
}

impl UpstreamTapGuard {
    fn new(capture: Arc<TurnCaptureState>) -> Self {
        Self {
            capture,
            clean: false,
        }
    }

    fn mark_clean(&mut self) {
        self.clean = true;
    }
}

impl Drop for UpstreamTapGuard {
    fn drop(&mut self) {
        // Sticky `partial` for a truncated raw capture; close either way (idempotent
        // with the barrier's own close) so the writer drains + finalize resolves.
        self.capture.upstream_response_done(!self.clean);
    }
}

fn parse_chat_completion_chunk(data: &str) -> Result<ChatCompletionChunk, serde_json::Error> {
    let first_error = match serde_json::from_str::<ChatCompletionChunk>(data) {
        Ok(chunk) => return Ok(chunk),
        Err(err) => err,
    };
    let Ok(mut value) = serde_json::from_str::<Value>(data) else {
        return Err(first_error);
    };
    if !normalize_sparse_tool_call_types(&mut value) {
        return Err(first_error);
    }
    serde_json::from_value(value)
}

fn normalize_sparse_tool_call_types(value: &mut Value) -> bool {
    let Some(choices) = value.get_mut("choices").and_then(Value::as_array_mut) else {
        return false;
    };
    let mut changed = false;
    for choice in choices {
        let Some(tool_calls) = choice
            .get_mut("delta")
            .and_then(|delta| delta.get_mut("tool_calls"))
            .and_then(Value::as_array_mut)
        else {
            continue;
        };
        for tool_call in tool_calls {
            let Some(object) = tool_call.as_object_mut() else {
                continue;
            };
            if !object.contains_key("type") {
                object.insert("type".to_string(), Value::String("function".to_string()));
                changed = true;
            }
        }
    }
    changed
}

pub async fn collect_models_response(
    response: reqwest::Response,
) -> AppResult<(StatusCode, Value, Option<String>)> {
    let status = response.status();
    let etag = response
        .headers()
        .get(http::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    let body = response
        .json::<Value>()
        .await
        .map_err(|err| AppError::upstream(format!("invalid upstream /models JSON: {err}")))?;
    Ok((status, body, etag))
}

pub async fn collect_supported_model_catalog(
    response: reqwest::Response,
) -> AppResult<Vec<UpstreamModelEntry>> {
    let (_, body, _) = collect_models_response(response).await?;
    Ok(extract_supported_model_catalog(&body))
}

async fn filter_models_response(
    response: reqwest::Response,
    model: &str,
) -> AppResult<reqwest::Response> {
    let status = response.status();
    let body = response
        .json::<Value>()
        .await
        .map_err(|err| AppError::upstream(format!("invalid upstream /models JSON: {err}")))?;
    let body = filter_models_body(body, model);
    let body = serde_json::to_string(&body).map_err(|err| {
        AppError::internal(format!("failed to serialize /models response: {err}"))
    })?;
    let response = http::Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .map_err(|err| AppError::internal(format!("failed to build /models response: {err}")))?;
    Ok(reqwest::Response::from(response))
}

fn json_response(body: Value) -> AppResult<reqwest::Response> {
    let body = serde_json::to_string(&body)
        .map_err(|err| AppError::internal(format!("failed to serialize JSON response: {err}")))?;
    let response = http::Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .map_err(|err| AppError::internal(format!("failed to build JSON response: {err}")))?;
    Ok(reqwest::Response::from(response))
}

fn filter_models_body(body: Value, model: &str) -> Value {
    match body {
        Value::Object(mut map) => {
            if let Some(entries) = map.get("data").and_then(Value::as_array) {
                map.insert(
                    "data".to_string(),
                    Value::Array(filter_model_entries(entries, model)),
                );
                Value::Object(map)
            } else if let Some(entries) = map.get("models").and_then(Value::as_array) {
                map.insert(
                    "models".to_string(),
                    Value::Array(filter_model_entries(entries, model)),
                );
                Value::Object(map)
            } else {
                single_model_list_body(model)
            }
        }
        Value::Array(entries) => Value::Array(filter_model_entries(&entries, model)),
        _ => single_model_list_body(model),
    }
}

fn filter_model_entries(entries: &[Value], model: &str) -> Vec<Value> {
    match entries
        .iter()
        .find(|entry| model_entry_id(entry).is_some_and(|id| id == model))
    {
        Some(entry) => vec![entry.clone()],
        None => vec![single_model_entry(model)],
    }
}

fn model_entry_id(entry: &Value) -> Option<&str> {
    match entry {
        Value::String(id) => Some(id.as_str()),
        Value::Object(map) => map.get("id").and_then(Value::as_str),
        _ => None,
    }
}

fn single_model_list_body(model: &str) -> Value {
    serde_json::json!({
        "object": "list",
        "data": [single_model_entry(model)]
    })
}

fn single_model_entry(model: &str) -> Value {
    serde_json::json!({
        "id": model,
        "object": "model",
    })
}

/// Parse the upstream model catalog (id + optional context length) from a
/// `/v1/models` body in a SINGLE pass. Preserves the existing id behavior: bare
/// string entries are ids with no context length; object entries take their
/// `id` and, if present, the first positive context-length field. Entries
/// without an `id` are skipped.
fn extract_supported_model_catalog(body: &Value) -> Vec<UpstreamModelEntry> {
    model_entries_from_body(body)
        .iter()
        .filter_map(|entry| match entry {
            Value::String(id) => Some(UpstreamModelEntry {
                id: id.clone(),
                context_limit: None,
            }),
            Value::Object(map) => {
                let id = map.get("id").and_then(Value::as_str)?;
                Some(UpstreamModelEntry {
                    id: id.to_string(),
                    context_limit: entry_context_limit(map),
                })
            }
            _ => None,
        })
        .collect()
}

/// First positive integer among the context-length keys the Anthropic
/// `/v1/models` reshape uses (`http.rs`): `max_input_tokens`, `context_length`,
/// `context_window`, `max_context_length`, `max_model_len`. Single source for
/// G3 context budgeting and the routing union parse.
fn entry_context_limit(map: &serde_json::Map<String, Value>) -> Option<i64> {
    [
        "max_input_tokens",
        "context_length",
        "context_window",
        "max_context_length",
        "max_model_len",
    ]
    .iter()
    .find_map(|key| map.get(*key).and_then(Value::as_i64).filter(|n| *n > 0))
}

fn model_entries_from_body(body: &Value) -> Vec<Value> {
    match body {
        Value::Array(entries) => entries.clone(),
        Value::Object(map) => map
            .get("data")
            .or_else(|| map.get("models"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// `pub` so the G3 test oracle (`tests/port_server.rs`) can independently
/// normalize a recorded request through the SAME terminal leaf transform the
/// estimator uses, without calling the production estimator (T9: breaks the
/// self-referential oracle, G3 MEDIUM #19).
pub fn sanitize_chat_request(
    mut request: ChatCompletionRequest,
    flatten_content: bool,
) -> ChatCompletionRequest {
    if request.tools.as_ref().is_none_or(Vec::is_empty)
        && request.tool_choice.as_ref().is_none_or(|v| v == "auto")
    {
        request.tool_choice = None;
    }
    for message in &mut request.messages {
        if let Some(content) = message.content.take() {
            message.content = sanitize_message_content(content, flatten_content);
        }
        if let Some(tool_calls) = message.tool_calls.as_mut() {
            for tool_call in tool_calls {
                if let Some(arguments) = tool_call.function.arguments.take() {
                    tool_call.function.arguments = Some(stringify_json_value(arguments));
                }
            }
        }
    }
    request
}

fn sanitize_message_content(content: Value, flatten_content: bool) -> Option<Value> {
    match content {
        Value::Null => None,
        Value::String(text) => Some(Value::String(text)),
        Value::Array(parts) => {
            if flatten_content && content_parts_are_text_only(&parts) {
                Some(Value::String(flatten_content_parts(&parts)))
            } else {
                Some(Value::Array(parts))
            }
        }
        other => Some(stringify_json_value(other)),
    }
}

fn content_parts_are_text_only(parts: &[Value]) -> bool {
    parts.iter().all(|part| {
        let has_text = part.get("text").and_then(Value::as_str).is_some();
        let text_kind = matches!(
            part.get("type").and_then(Value::as_str),
            None | Some("text") | Some("input_text") | Some("output_text")
        );
        has_text && text_kind
    })
}

fn flatten_content_parts(parts: &[Value]) -> String {
    let mut text_parts = Vec::new();
    for part in parts {
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            text_parts.push(text.to_string());
        } else {
            text_parts.push(serde_json::to_string(part).unwrap_or_else(|_| "null".to_string()));
        }
    }
    text_parts.join("\n")
}

fn truncate_for_error(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    match s.char_indices().nth(max) {
        Some((byte_idx, _)) => format!("{}...[truncated]", &s[..byte_idx]),
        None => s.to_string(),
    }
}

/// Redact image `data:`/signed URLs from an upstream RESPONSE error body, then
/// truncate it for an `AppError`/log message (G4 round-9 #2). A provider that
/// echoes a native-vision-passthrough / disabled-agent image request can mirror
/// the submitted `data:` bytes or a signed image URL back in its 4xx/5xx body;
/// without this they would leak through `response.failed` and failover logs
/// (AGENTS.md redact rule). Redaction runs BEFORE truncation so a split image
/// URI cannot survive at the truncation boundary.
fn redact_and_truncate_error_body(body: &str, max: usize) -> String {
    truncate_for_error(&crate::redaction::redact_image_uris(body), max)
}

fn stringify_json_value(value: Value) -> Value {
    Value::String(serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string()))
}

#[cfg(test)]
mod tests {
    use super::ReqwestUpstreamClient;
    use super::UpstreamModelEntry;
    use super::UpstreamRequestLogger;
    use super::classify_attempt_error;
    use super::extract_supported_model_catalog;
    use super::sanitize_chat_request;
    use super::should_proxy_request_header;
    use crate::error::AppError;
    use crate::error::FailoverDisposition;
    use crate::models::chat::ChatCompletionRequest;
    use crate::models::chat::ChatMessage;
    use http::HeaderName;
    use reqwest::StatusCode;
    use serde_json::Value;
    use std::collections::BTreeMap;

    /// The full RFC 7230 §6.1 hop-by-hop set; must match the canonical list and
    /// the response-direction parity test in `http.rs`.
    const HOP_BY_HOP: [&str; 8] = [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ];

    #[test]
    fn request_direction_strips_full_hop_by_hop_set() {
        for header in HOP_BY_HOP {
            let name = HeaderName::from_bytes(header.as_bytes()).unwrap();
            assert!(
                !should_proxy_request_header(&name),
                "request proxy must strip hop-by-hop header {header}",
            );
        }
    }

    #[test]
    fn request_direction_strips_authorization_host_content_length() {
        for header in ["authorization", "host", "content-length"] {
            let name = HeaderName::from_bytes(header.as_bytes()).unwrap();
            assert!(
                !should_proxy_request_header(&name),
                "request proxy must strip {header}",
            );
        }
    }

    #[test]
    fn request_direction_passes_representative_passthrough_header() {
        let name = HeaderName::from_static("content-type");
        assert!(should_proxy_request_header(&name));
    }

    #[test]
    fn endpoint_url_preserves_v1_without_trailing_slash() {
        let client = ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            url::Url::parse("https://api.x.ai/v1").expect("url"),
            None,
            None,
            true,
            4096,
        );

        assert_eq!(
            client
                .endpoint_url("chat/completions")
                .expect("endpoint")
                .as_str(),
            "https://api.x.ai/v1/chat/completions"
        );
    }

    #[test]
    fn endpoint_url_preserves_v1_with_trailing_slash() {
        let client = ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            url::Url::parse("https://api.x.ai/v1/").expect("url"),
            None,
            None,
            true,
            4096,
        );

        assert_eq!(
            client.endpoint_url("models").expect("endpoint").as_str(),
            "https://api.x.ai/v1/models"
        );
    }

    // --- G2: family detection + chat_template_kwargs injection at the leaf ---

    use super::BackendChatRequest;
    use super::BackendFinalizationPolicies;
    use super::FailoverUpstreamClient;
    use super::FailoverUpstreamProvider;
    use super::ModelFamily;
    use super::ProviderStatus;
    use super::UpstreamClient as _;
    use super::apply_family_chat_template_kwargs;
    use super::detect_model_family;
    use super::merge_chat_kwargs_gap_fill;
    use super::write_family_kwargs;
    use serde_json::Map as JsonMap;
    use serde_json::json;
    use std::sync::Arc;

    /// Minimal request for family-injection tests. `model` is the FINAL provider
    /// model the leaf sees (after any routing/failover/alias rewrite).
    fn family_request(model: &str) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: model.to_string(),
            messages: Vec::new(),
            stream: true,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: Some(false),
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        }
    }

    fn kwargs_of(request: &ChatCompletionRequest) -> &serde_json::Map<String, Value> {
        request.extra_body["chat_template_kwargs"]
            .as_object()
            .expect("chat_template_kwargs object")
    }

    fn glm_effort_policies()
    -> std::collections::BTreeMap<String, crate::config::ReasoningEffortPolicy> {
        std::collections::BTreeMap::from([(
            "GLM-5.2-NVFP4-MTP".to_string(),
            crate::config::ReasoningEffortPolicy {
                default: Some("max".to_string()),
                upstream_reasoning: None,
                map: std::collections::BTreeMap::from([
                    (
                        "high".to_string(),
                        json!({"chat_template_kwargs": {"reasoning_effort": "high"}}),
                    ),
                    (
                        "max".to_string(),
                        json!({"chat_template_kwargs": {"reasoning_effort": "max"}}),
                    ),
                    (
                        "none".to_string(),
                        json!({"chat_template_kwargs": {"enable_thinking": false}}),
                    ),
                ]),
            },
        )])
    }

    /// Wrap a family-request with optional client kwargs into the leaf wrapper.
    /// TEST helper — excluded from the D2 dashboard identity (passes `None` for both
    /// `response_id` and `serving`); only the engine's production
    /// `BackendChatRequest::new` sets them.
    fn family_backend(
        model: &str,
        client_kwargs: Option<JsonMap<String, Value>>,
    ) -> BackendChatRequest {
        BackendChatRequest::new(family_request(model), client_kwargs, None, None)
    }

    /// Empty finalization policies (no effort map, no family override, no
    /// per-model kwargs). The leaf's unmapped/clamp path.
    fn empty_policies() -> BackendFinalizationPolicies {
        BackendFinalizationPolicies::default()
    }

    /// GLM effort-map finalization policies (no family override, no kwargs).
    fn glm_backend_policies() -> BackendFinalizationPolicies {
        BackendFinalizationPolicies {
            effort: Arc::new(glm_effort_policies()),
            ..Default::default()
        }
    }

    /// Finalization policies carrying a per-model `template_family` override.
    fn family_policies(per_model: &[(&str, &str)]) -> BackendFinalizationPolicies {
        BackendFinalizationPolicies {
            template_family: Arc::new(
                per_model
                    .iter()
                    .map(|(m, f)| (m.to_string(), f.to_string()))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    /// Finalization policies carrying a global `upstream_chat_kwargs` base.
    fn global_kwargs_policies(kwargs: JsonMap<String, Value>) -> BackendFinalizationPolicies {
        BackendFinalizationPolicies {
            global_upstream_chat_kwargs: Arc::new(kwargs),
            ..Default::default()
        }
    }

    #[test]
    fn reasoning_effort_fragment_resolves_level_default_and_canonical() {
        use super::reasoning_effort_fragment;
        let p = glm_effort_policies();
        let ctk_effort = |frag: Value| frag["chat_template_kwargs"]["reasoning_effort"].clone();
        // Exact id + explicit level.
        assert_eq!(
            ctk_effort(reasoning_effort_fragment(&p, "GLM-5.2-NVFP4-MTP", Some("high")).unwrap()),
            json!("high")
        );
        // Canonical-key match (case/punctuation differences).
        assert_eq!(
            ctk_effort(reasoning_effort_fragment(&p, "glm 5.2 nvfp4 mtp", Some("max")).unwrap()),
            json!("max")
        );
        // Raw absent -> policy default ("max").
        assert_eq!(
            ctk_effort(reasoning_effort_fragment(&p, "GLM-5.2-NVFP4-MTP", None).unwrap()),
            json!("max")
        );
        // Off.
        assert_eq!(
            reasoning_effort_fragment(&p, "GLM-5.2-NVFP4-MTP", Some("none")).unwrap()["chat_template_kwargs"]
                ["enable_thinking"],
            json!(false)
        );
        // Level not in the map -> None (engine keeps its clamped top-level effort).
        assert!(reasoning_effort_fragment(&p, "GLM-5.2-NVFP4-MTP", Some("medium")).is_none());
        // Unknown model -> None.
        assert!(reasoning_effort_fragment(&p, "other-model", Some("high")).is_none());

        // Ambiguous canonical match (two profiles, same canonical key, neither an
        // exact id match) -> no policy, deterministically.
        let mut ambiguous = glm_effort_policies();
        let dup = ambiguous["GLM-5.2-NVFP4-MTP"].clone();
        ambiguous.insert("glm5.2nvfp4mtp".to_string(), dup);
        assert!(reasoning_effort_fragment(&ambiguous, "GLM!5.2!NVFP4!MTP", Some("high")).is_none());
    }

    #[test]
    fn effort_fragment_overrides_config_default_but_client_wins() {
        use super::apply_reasoning_effort_fragment;
        let fragment = json!({"chat_template_kwargs": {"reasoning_effort": "high"}});

        // A configured chat_template_kwargs default is OVERRIDDEN by the map
        // (prefer-source), fixing the "config blocks the map" case.
        let mut backend = family_backend("GLM-5.2-NVFP4-MTP", None);
        backend.request.extra_body.insert(
            "chat_template_kwargs".to_string(),
            json!({"reasoning_effort": "max", "sibling": 1}),
        );
        apply_reasoning_effort_fragment(&mut backend, &fragment);
        assert_eq!(
            kwargs_of(&backend.request)["reasoning_effort"],
            json!("high")
        );
        assert_eq!(kwargs_of(&backend.request)["sibling"], json!(1));

        // An explicit CLIENT value wins over the map (client > effort-map).
        let mut backend = family_backend(
            "GLM-5.2-NVFP4-MTP",
            Some(JsonMap::from_iter([(
                "reasoning_effort".to_string(),
                json!("max"),
            )])),
        );
        apply_reasoning_effort_fragment(&mut backend, &fragment);
        assert_eq!(
            kwargs_of(&backend.request)["reasoning_effort"],
            json!("max")
        );
    }

    #[test]
    fn effort_fragment_survives_kimi_family_injection() {
        use super::apply_reasoning_effort_fragment;
        // Kimi injection wipes enable_thinking/reasoning_effort and forces
        // thinking=true; applying the fragment AFTER family re-asserts the map's
        // enable_thinking:false (fixes the Kimi-map-ignored finding).
        let mut backend = family_backend("kimi-k2-instruct", None);
        apply_family_chat_template_kwargs(&mut backend, &empty_policies());
        assert_eq!(kwargs_of(&backend.request)["thinking"], json!(true));
        apply_reasoning_effort_fragment(
            &mut backend,
            &json!({"chat_template_kwargs": {"enable_thinking": false}}),
        );
        assert_eq!(kwargs_of(&backend.request)["enable_thinking"], json!(false));
    }

    #[test]
    fn clamp_reasoning_effort_collapses_to_openai_vocabulary() {
        use super::clamp_reasoning_effort;
        assert_eq!(clamp_reasoning_effort(None), None);
        assert_eq!(clamp_reasoning_effort(Some("")), None);
        assert_eq!(
            clamp_reasoning_effort(Some("none")).as_deref(),
            Some("none")
        );
        assert_eq!(clamp_reasoning_effort(Some("low")).as_deref(), Some("low"));
        for raw in ["medium", "high", "xhigh", "max", "unknown"] {
            assert_eq!(
                clamp_reasoning_effort(Some(raw)).as_deref(),
                Some("high"),
                "{raw} collapses to high"
            );
        }
        assert_eq!(
            clamp_reasoning_effort(Some("  XHigh ")).as_deref(),
            Some("high")
        );
    }

    #[test]
    fn finalize_unmapped_model_clamps_top_level_effort() {
        use super::finalize_request_for_backend;
        let mut backend = family_backend("some-plain-model", None);
        backend.request.reasoning_effort = Some("xhigh".to_string());
        finalize_request_for_backend(&mut backend, &empty_policies());
        // No policy -> clamp; no family -> no chat_template_kwargs injected.
        assert_eq!(backend.request.reasoning_effort.as_deref(), Some("high"));
        assert!(
            !backend
                .request
                .extra_body
                .contains_key("chat_template_kwargs")
        );
    }

    #[test]
    fn finalize_mapped_model_clears_top_level_and_maps_to_chat_template_kwargs() {
        use super::finalize_request_for_backend;
        let mut backend = family_backend("GLM-5.2-NVFP4-MTP", None);
        backend.request.reasoning_effort = Some("max".to_string());
        finalize_request_for_backend(&mut backend, &glm_backend_policies());
        assert_eq!(backend.request.reasoning_effort, None);
        assert_eq!(
            kwargs_of(&backend.request)["reasoning_effort"],
            json!("max")
        );
    }

    #[test]
    fn detect_family_sniffs_resolved_model_case_insensitively() {
        assert_eq!(
            detect_model_family("kimi-k2-instruct", None),
            Some(ModelFamily::Kimi)
        );
        assert_eq!(
            detect_model_family("Moonshot-KIMI-K2", None),
            Some(ModelFamily::Kimi)
        );
        assert_eq!(
            detect_model_family("DeepSeek-V3", None),
            Some(ModelFamily::DeepSeek)
        );
        assert_eq!(detect_model_family("glm-5.1", None), None);
        assert_eq!(detect_model_family("gpt-4o", None), None);
    }

    #[test]
    fn detect_family_override_beats_model_name() {
        assert_eq!(
            detect_model_family("glm-5.1", Some("kimi")),
            Some(ModelFamily::Kimi)
        );
        assert_eq!(
            detect_model_family("kimi-k2", Some("deepseek")),
            Some(ModelFamily::DeepSeek)
        );
        // An unrecognized override falls through to name sniffing.
        assert_eq!(
            detect_model_family("kimi-k2", Some("bogus")),
            Some(ModelFamily::Kimi)
        );
    }

    #[test]
    fn write_kimi_kwargs_forces_thinking_and_strips_deepseek_keys() {
        let mut kwargs = JsonMap::new();
        kwargs.insert("enable_thinking".to_string(), json!(true));
        kwargs.insert("clear_thinking".to_string(), json!(true));
        kwargs.insert("reasoning_effort".to_string(), json!("low"));
        kwargs.insert("keep_me".to_string(), json!(1));
        write_family_kwargs(
            &mut kwargs,
            ModelFamily::Kimi,
            &Some("high".to_string()),
            None,
        );
        assert_eq!(kwargs["thinking"], json!(true));
        assert_eq!(kwargs["preserve_thinking"], json!(true));
        assert!(!kwargs.contains_key("enable_thinking"));
        assert!(!kwargs.contains_key("clear_thinking"));
        assert!(!kwargs.contains_key("reasoning_effort"));
        assert_eq!(kwargs["keep_me"], json!(1));
    }

    #[test]
    fn write_deepseek_kwargs_setdefaults_enable_thinking_and_effort() {
        let mut kwargs = JsonMap::new();
        write_family_kwargs(
            &mut kwargs,
            ModelFamily::DeepSeek,
            &Some("high".to_string()),
            None,
        );
        assert_eq!(kwargs["enable_thinking"], json!(true));
        assert_eq!(kwargs["reasoning_effort"], json!("high"));

        let mut kwargs = JsonMap::new();
        kwargs.insert("enable_thinking".to_string(), json!(false));
        kwargs.insert("reasoning_effort".to_string(), json!("low"));
        write_family_kwargs(
            &mut kwargs,
            ModelFamily::DeepSeek,
            &Some("high".to_string()),
            None,
        );
        assert_eq!(kwargs["enable_thinking"], json!(false));
        assert_eq!(kwargs["reasoning_effort"], json!("low"));

        let mut kwargs = JsonMap::new();
        write_family_kwargs(&mut kwargs, ModelFamily::DeepSeek, &None, None);
        assert_eq!(kwargs["enable_thinking"], json!(true));
        assert!(!kwargs.contains_key("reasoning_effort"));
    }

    /// Finding 1: the FINAL provider model drives the family. A request that a
    /// failover/exposed-alias path rewrote to a Kimi model gets Kimi kwargs for
    /// THAT provider, even though the engine never saw "kimi".
    #[test]
    fn kimi_final_provider_model_gets_kimi_kwargs() {
        let mut backend = family_backend("kimi-k2-instruct", None);
        apply_family_chat_template_kwargs(&mut backend, &empty_policies());
        let kwargs = kwargs_of(&backend.request);
        assert_eq!(kwargs["thinking"], json!(true));
        assert_eq!(kwargs["preserve_thinking"], json!(true));
    }

    /// Finding 1 (negative): a DeepSeek FINAL provider model does NOT get the
    /// Kimi `thinking` knob — the wrong-family kwargs cannot leak across.
    #[test]
    fn deepseek_final_provider_model_does_not_get_kimi_kwargs() {
        let mut backend = family_backend("deepseek-v3", None);
        backend.request.reasoning_effort = Some("high".to_string());
        apply_family_chat_template_kwargs(&mut backend, &empty_policies());
        let kwargs = kwargs_of(&backend.request);
        assert_eq!(kwargs["enable_thinking"], json!(true));
        assert_eq!(kwargs["reasoning_effort"], json!("high"));
        assert!(
            kwargs.get("thinking").is_none(),
            "DeepSeek must not carry the Kimi `thinking` key"
        );
    }

    /// Finding 1 end-to-end through `request_for_provider`: a failover provider
    /// that remaps the model to Kimi AND carries its own `upstream_chat_kwargs`
    /// produces a request that, once the leaf injects, has BOTH the merged
    /// provider kwarg and the Kimi family knobs (deep-merge, not clobber).
    #[test]
    fn request_for_provider_kimi_remap_then_inject_deep_merges() {
        let mut provider_kwargs = JsonMap::new();
        provider_kwargs.insert(
            "chat_template_kwargs".to_string(),
            json!({ "configured_only": true }),
        );
        let provider = FailoverUpstreamProvider::new(
            "kimi-fallback",
            ReqwestUpstreamClient::new(
                reqwest::Client::new(),
                url::Url::parse("https://example.invalid/v1").expect("url"),
                None,
                None,
                true,
                4096,
            ),
            Some("kimi-k2-instruct".to_string()),
            None,
            provider_kwargs,
        );
        // Engine-level request still names a non-Kimi model; the provider remaps.
        let base = family_backend("glm-5.1", None);
        let mut provider_request = FailoverUpstreamClient::request_for_provider(&provider, &base);
        assert_eq!(provider_request.request.model, "kimi-k2-instruct");
        // Leaf injects family from the FINAL (remapped) model.
        apply_family_chat_template_kwargs(&mut provider_request, &empty_policies());
        let kwargs = kwargs_of(&provider_request.request);
        assert_eq!(kwargs["thinking"], json!(true));
        assert_eq!(kwargs["preserve_thinking"], json!(true));
        assert_eq!(kwargs["configured_only"], json!(true));
    }

    /// U2 provider-fallback path: a typed client `stop` must beat a provider
    /// `upstream_chat_kwargs.stop`, with NO `extra_body["stop"]`, driven through
    /// the real `request_for_provider` call site (the shared gap-fill helper).
    #[test]
    fn request_for_provider_typed_stop_beats_provider_stop() {
        let mut provider_kwargs = JsonMap::new();
        provider_kwargs.insert("stop".to_string(), json!(["CONFIGURED"]));
        let provider = FailoverUpstreamProvider::new(
            "fallback",
            ReqwestUpstreamClient::new(
                reqwest::Client::new(),
                url::Url::parse("https://example.invalid/v1").expect("url"),
                None,
                None,
                true,
                4096,
            ),
            None,
            None,
            provider_kwargs,
        );
        let mut base = family_backend("m", None);
        base.request.stop = Some(vec!["CLIENT".to_string()]);

        let provider_request = FailoverUpstreamClient::request_for_provider(&provider, &base);

        assert_eq!(
            provider_request.request.stop,
            Some(vec!["CLIENT".to_string()])
        );
        assert!(
            !provider_request.request.extra_body.contains_key("stop"),
            "provider stop must not land in extra_body on the fallback path"
        );
    }

    /// U2 provider-fallback alias collision: the collapsed helper applies the
    /// max-token-alias skip on the fallback path too, so a provider
    /// `max_tokens` cannot land alongside a surviving client `max_completion_tokens`
    /// (the `/v1/responses` shape). Driven through `request_for_provider`.
    #[test]
    fn request_for_provider_skips_provider_max_tokens_when_client_alias_present() {
        let mut provider_kwargs = JsonMap::new();
        provider_kwargs.insert("max_tokens".to_string(), json!(4096));
        let provider = FailoverUpstreamProvider::new(
            "fallback",
            ReqwestUpstreamClient::new(
                reqwest::Client::new(),
                url::Url::parse("https://example.invalid/v1").expect("url"),
                None,
                None,
                true,
                4096,
            ),
            None,
            None,
            provider_kwargs,
        );
        let mut base = family_backend("m", None);
        base.request
            .extra_body
            .insert("max_completion_tokens".to_string(), json!(256));

        let provider_request = FailoverUpstreamClient::request_for_provider(&provider, &base);

        assert!(
            !provider_request
                .request
                .extra_body
                .contains_key("max_tokens"),
            "provider max_tokens alias must not land alongside the client alias"
        );
        assert_eq!(
            provider_request
                .request
                .extra_body
                .get("max_completion_tokens"),
            Some(&json!(256))
        );
    }

    /// Finding 1: an explicit client `chat_template_kwargs` value still WINS
    /// over the forced family default, while non-conflicting forced keys remain.
    #[test]
    fn client_chat_template_kwargs_win_over_forced_family_default() {
        let mut backend = family_backend(
            "kimi-k2",
            Some(
                json!({ "thinking": false, "custom": 1 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        );
        // The engine bakes the client value into extra_body; mirror that.
        backend.request.extra_body.insert(
            "chat_template_kwargs".to_string(),
            json!({ "thinking": false, "custom": 1 }),
        );
        apply_family_chat_template_kwargs(&mut backend, &empty_policies());
        let kwargs = kwargs_of(&backend.request);
        assert_eq!(kwargs["thinking"], json!(false), "client value wins");
        assert_eq!(kwargs["custom"], json!(1));
        assert_eq!(kwargs["preserve_thinking"], json!(true));
    }

    /// Finding 2: the client `chat_template_kwargs` re-overlay must DEEP-merge
    /// over the already-deep-merged family/provider object. A nested client
    /// object wins ONLY on its conflicting leaf; sibling keys placed there by the
    /// configured/provider deep-merge survive (a shallow `insert` would clobber
    /// the whole nested object and drop those siblings).
    #[test]
    fn client_kwargs_deep_merge_preserves_sibling_keys() {
        let mut backend = family_backend(
            "kimi-k2-instruct",
            Some(
                json!({ "mm_processor_kwargs": { "shared": "client", "from_client": 1 } })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        );
        // Configured/provider deep-merge already produced a nested object with
        // two siblings under `mm_processor_kwargs`.
        backend.request.extra_body.insert(
            "chat_template_kwargs".to_string(),
            json!({ "mm_processor_kwargs": { "from_config": true, "shared": "config" } }),
        );
        apply_family_chat_template_kwargs(&mut backend, &empty_policies());
        let nested = kwargs_of(&backend.request)["mm_processor_kwargs"]
            .as_object()
            .expect("nested object survives deep-merge");
        assert_eq!(
            nested["shared"],
            json!("client"),
            "client wins the conflict"
        );
        assert_eq!(
            nested["from_config"],
            json!(true),
            "sibling key from the config deep-merge must survive"
        );
        assert_eq!(
            nested["from_client"],
            json!(1),
            "client adds new nested key"
        );
        // Forced family knobs still applied alongside the nested object.
        assert_eq!(kwargs_of(&backend.request)["thinking"], json!(true));
    }

    /// Unrecognized family => no injection at all.
    #[test]
    fn unrecognized_final_model_injects_nothing() {
        let mut backend = family_backend("glm-5.1", None);
        apply_family_chat_template_kwargs(&mut backend, &empty_policies());
        assert!(
            !backend
                .request
                .extra_body
                .contains_key("chat_template_kwargs")
        );
    }

    /// T1: the leaf resolves the `template_family` override from the FINAL
    /// provider model's policy (per-model, else global). A per-model policy
    /// forces a family the model NAME does not sniff, so the right family kwargs
    /// apply to the model the provider actually receives — not the alias's.
    #[test]
    fn leaf_resolves_family_from_per_model_policy_on_final_model() {
        // An opaque-named model with a per-model `deepseek` policy gets DeepSeek
        // kwargs even though the name sniffs nothing.
        let policies = family_policies(&[("opaque-target", "deepseek")]);
        let mut backend = family_backend("opaque-target", None);
        apply_family_chat_template_kwargs(&mut backend, &policies);
        let kwargs = kwargs_of(&backend.request);
        assert_eq!(kwargs["enable_thinking"], json!(true));
        assert!(
            kwargs.get("thinking").is_none(),
            "no Kimi knob for deepseek"
        );

        // An alias's policy does NOT bleed onto a different final model: the
        // final model has its own policy, which wins.
        let policies = family_policies(&[("glm-alias", "kimi"), ("opaque-target", "deepseek")]);
        let mut backend = family_backend("opaque-target", None);
        apply_family_chat_template_kwargs(&mut backend, &policies);
        let kwargs = kwargs_of(&backend.request);
        assert_eq!(kwargs["enable_thinking"], json!(true));
        assert!(
            kwargs.get("thinking").is_none(),
            "alias's kimi override must not bleed onto the deepseek target"
        );
    }

    /// T1 (R1 F1): per-model family policy lookup is case-insensitive (exact
    /// then canonical-key), mirroring `Config::model_profile`. A profile keyed
    /// `Opaque-Target` matches a FINAL model `opaque-target`.
    #[test]
    fn leaf_family_policy_lookup_is_case_insensitive() {
        let policies = family_policies(&[("Opaque-Target", "deepseek")]);
        let mut backend = family_backend("opaque-target", None);
        apply_family_chat_template_kwargs(&mut backend, &policies);
        let kwargs = kwargs_of(&backend.request);
        assert_eq!(
            kwargs["enable_thinking"],
            json!(true),
            "mixed-case profile key must match lowercase final model"
        );
    }

    /// T1: the GLOBAL `template_family` fallback applies when no per-model
    /// policy matches the FINAL model.
    #[test]
    fn leaf_global_family_fallback_when_no_per_model_policy() {
        let policies = BackendFinalizationPolicies {
            global_template_family: Some("kimi".to_string()),
            ..Default::default()
        };
        let mut backend = family_backend("opaque-no-profile", None);
        apply_family_chat_template_kwargs(&mut backend, &policies);
        let kwargs = kwargs_of(&backend.request);
        assert_eq!(kwargs["thinking"], json!(true));
        assert_eq!(kwargs["preserve_thinking"], json!(true));
    }

    /// T1 (R1 F2): max-token aliases are one logical knob. If the client sent
    /// `max_completion_tokens` via `extra_body`, the leaf must NOT insert a
    /// configured `max_tokens` default alongside it (would shadow the explicit
    /// request value). Mirrors the engine's pre-T1
    /// `remove_defaults_shadowed_by_request_extra`.
    #[test]
    fn leaf_kwargs_skip_max_token_aliases_when_request_has_one() {
        use super::finalize_request_for_backend;
        let mut defaults = JsonMap::new();
        defaults.insert("max_tokens".to_string(), json!(8192));
        let policies = global_kwargs_policies(defaults);

        // Client expressed `max_completion_tokens` via extra_body (typed field
        // absent). The configured `max_tokens` default must NOT be inserted.
        let mut backend = family_backend("plain-model", None);
        backend
            .request
            .extra_body
            .insert("max_completion_tokens".to_string(), json!(2048));
        finalize_request_for_backend(&mut backend, &policies);
        assert!(
            !backend.request.extra_body.contains_key("max_tokens"),
            "configured max_tokens must be shadowed by the request's max_completion_tokens alias: {:?}",
            backend.request.extra_body
        );
        assert_eq!(
            backend.request.extra_body["max_completion_tokens"],
            json!(2048),
            "request alias preserved"
        );
    }

    #[test]
    fn normalize_sparse_tool_call_types_fills_chat_function_type() {
        let mut value = serde_json::json!({
            "id": "gen-1778509925-7119UkUjPTix9sGQ4vZf",
            "object": "chat.completion.chunk",
            "created": 1778509925,
            "model": "xiaomi/mimo-v2.5-pro-20260422",
            "provider": "Xiaomi",
            "choices": [{
                "index": 0,
                "delta": {
                    "content": null,
                    "role": "assistant",
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": "{\"file_path\": \"/home/luke/.claude/projects/-home-luke-projects-demo/memory/smb_clone.md\"}"
                        }
                    }]
                },
                "finish_reason": null,
                "native_finish_reason": null
            }]
        });

        assert!(super::normalize_sparse_tool_call_types(&mut value));
        assert_eq!(
            value["choices"][0]["delta"]["tool_calls"][0]["type"],
            Value::String("function".to_string())
        );
    }

    #[test]
    fn parse_chat_completion_chunk_accepts_openrouter_sparse_tool_call() {
        let payload = serde_json::json!({
            "id": "gen-1778509925-7119UkUjPTix9sGQ4vZf",
            "object": "chat.completion.chunk",
            "created": 1778509925,
            "model": "xiaomi/mimo-v2.5-pro-20260422",
            "provider": "Xiaomi",
            "choices": [{
                "index": 0,
                "delta": {
                    "content": null,
                    "role": "assistant",
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": "{\"file_path\": \"/home/luke/.claude/projects/-home-luke-projects-demo/memory/smb_clone.md\"}"
                        }
                    }]
                },
                "finish_reason": null,
                "native_finish_reason": null
            }]
        })
        .to_string();

        let chunk = super::parse_chat_completion_chunk(&payload).expect("parse chunk");
        let tool_call = &chunk.choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tool_call.kind, "function");
        assert_eq!(tool_call.index, Some(0));
        assert_eq!(
            tool_call
                .function
                .arguments
                .as_ref()
                .and_then(Value::as_str),
            Some(
                "{\"file_path\": \"/home/luke/.claude/projects/-home-luke-projects-demo/memory/smb_clone.md\"}"
            )
        );
    }

    #[test]
    fn sanitize_chat_request_clears_auto_tool_choice_and_preserves_reasoning() {
        let request = ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Some(Value::String("hello".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            stream: true,
            tools: None,
            tool_choice: Some(Value::String("auto".to_string())),
            parallel_tool_calls: Some(false),
            reasoning_effort: Some("high".to_string()),
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized = sanitize_chat_request(request, true);

        assert_eq!(sanitized.reasoning_effort, Some("high".to_string()));
        assert_eq!(sanitized.tool_choice, None);
    }

    #[test]
    fn test_sanitize_clears_auto_when_no_tools() {
        let request = ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Some(Value::String("hi".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            stream: true,
            tools: Some(Vec::new()),
            tool_choice: Some(Value::String("auto".to_string())),
            parallel_tool_calls: Some(false),
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized = sanitize_chat_request(request, true);
        assert_eq!(sanitized.tool_choice, None);
    }

    #[test]
    fn test_sanitize_preserves_none_and_required_without_tools() {
        let make = |tc: &str| ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Some(Value::String("hi".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            stream: true,
            tools: None,
            tool_choice: Some(Value::String(tc.to_string())),
            parallel_tool_calls: Some(false),
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized_none = sanitize_chat_request(make("none"), true);
        assert_eq!(
            sanitized_none.tool_choice,
            Some(Value::String("none".to_string()))
        );

        let sanitized_required = sanitize_chat_request(make("required"), true);
        assert_eq!(
            sanitized_required.tool_choice,
            Some(Value::String("required".to_string()))
        );
    }

    #[test]
    fn test_sanitize_preserves_reasoning_effort() {
        let request = ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Some(Value::String("hi".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            stream: true,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: Some(false),
            reasoning_effort: Some("high".to_string()),
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized = sanitize_chat_request(request, true);
        assert_eq!(sanitized.reasoning_effort, Some("high".to_string()));
    }

    #[test]
    fn sanitize_chat_request_stringifies_structured_message_content_and_tool_args() {
        let request = ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "assistant".to_string(),
                content: Some(serde_json::json!([
                    { "type": "text", "text": "hello" },
                    { "type": "text", "text": "world" }
                ])),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: Some(vec![crate::models::chat::ChatToolCall {
                    id: Some("call_1".to_string()),
                    index: Some(0),
                    kind: "function".to_string(),
                    function: crate::models::chat::ChatFunctionCall {
                        name: Some("echo".to_string()),
                        arguments: Some(serde_json::json!({ "value": "hi" })),
                    },
                }]),
            }],
            stream: true,
            tools: Some(Vec::new()),
            tool_choice: Some(Value::String("auto".to_string())),
            parallel_tool_calls: Some(false),
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized = sanitize_chat_request(request, true);

        assert_eq!(
            sanitized.messages[0].content,
            Some(Value::String("hello\nworld".to_string()))
        );
        assert_eq!(
            sanitized.messages[0]
                .tool_calls
                .as_ref()
                .expect("tool calls")[0]
                .function
                .arguments,
            Some(Value::String("{\"value\":\"hi\"}".to_string()))
        );
    }

    #[test]
    fn sanitize_null_content() {
        assert_eq!(super::sanitize_message_content(Value::Null, true), None);
    }

    #[test]
    fn sanitize_non_string_content() {
        let result = super::sanitize_message_content(Value::Bool(true), true);
        assert_eq!(result, Some(Value::String("true".to_string())));
    }

    #[test]
    fn flatten_content_parts_non_text() {
        let parts = vec![serde_json::json!({"image": "data"})];
        let result = super::flatten_content_parts(&parts);
        assert!(result.contains("image"));
        assert!(result.contains("data"));
    }

    #[test]
    fn endpoint_url_bare_domain() {
        let client = ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            url::Url::parse("https://api.example.com").expect("url"),
            None,
            None,
            true,
            4096,
        );
        let url = client.endpoint_url("chat/completions").expect("endpoint");
        assert_eq!(url.as_str(), "https://api.example.com/chat/completions");
    }

    #[tokio::test]
    async fn upstream_request_logger_writes_jsonl() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-upstream-log-{}.jsonl",
            uuid::Uuid::new_v4().simple()
        ));
        let logger = UpstreamRequestLogger::new(path.clone());
        let request = sanitize_chat_request(
            ChatCompletionRequest {
                model: "grok-4".to_string(),
                messages: vec![ChatMessage {
                    role: "user".to_string(),
                    content: Some(serde_json::json!([
                        { "type": "text", "text": "hello" },
                        { "type": "text", "text": "world" }
                    ])),
                    tool_call_id: None,
                    name: None,
                    reasoning_content: None,
                    thinking: None,
                    tool_calls: None,
                }],
                stream: true,
                tools: None,
                tool_choice: Some(Value::String("auto".to_string())),
                parallel_tool_calls: Some(false),
                reasoning_effort: Some("high".to_string()),
                response_format: None,
                stream_options: None,
                temperature: None,
                top_p: None,
                max_output_tokens: None,
                frequency_penalty: None,
                presence_penalty: None,
                stop: None,
                extra_body: BTreeMap::new(),
            },
            true,
        );

        logger.log(&request).await.expect("write request log");

        let contents = std::fs::read_to_string(&path).expect("read request log");
        assert_eq!(
            contents,
            format!(
                "{}\n",
                serde_json::to_string(&request).expect("serialize request")
            )
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_truncate_short_body_unchanged() {
        assert_eq!(super::truncate_for_error("hello", 500), "hello");
    }

    #[test]
    fn test_truncate_long_body() {
        let long = "x".repeat(1000);
        let result = super::truncate_for_error(&long, 500);
        assert!(result.ends_with("...[truncated]"));
        assert_eq!(result.len(), 500 + "...[truncated]".len());
    }

    #[test]
    fn test_truncate_unicode_safe() {
        let base = "héllo wörld ";
        let repeated: String = base.repeat(100);
        let result = super::truncate_for_error(&repeated, 50);
        assert!(result.ends_with("...[truncated]"));
        // Verify truncation happened at a char boundary by checking it's valid UTF-8
        assert_eq!(result, result.to_string());
        let prefix = result.trim_end_matches("...[truncated]");
        assert_eq!(prefix.chars().count(), 50);
    }

    #[test]
    fn test_truncate_exact_boundary() {
        let exact = "a".repeat(500);
        assert_eq!(super::truncate_for_error(&exact, 500), exact);
    }

    #[test]
    fn test_sanitize_preserves_array_when_flatten_disabled() {
        let array = serde_json::json!([
            { "type": "text", "text": "hello" },
            { "type": "image_url", "image_url": { "url": "data:image/png;base64,abc" } }
        ]);
        let result = super::sanitize_message_content(array.clone(), false);
        assert_eq!(result, Some(array));
    }

    #[test]
    fn test_sanitize_flattens_array_when_flatten_enabled() {
        let array = serde_json::json!([
            { "type": "text", "text": "hello" },
            { "type": "text", "text": "world" }
        ]);
        let result = super::sanitize_message_content(array, true);
        assert_eq!(result, Some(Value::String("hello\nworld".to_string())));
    }

    #[test]
    fn test_sanitize_preserves_multimodal_array_when_flatten_enabled() {
        let array = serde_json::json!([
            { "type": "text", "text": "look" },
            { "type": "image_url", "image_url": { "url": "data:image/png;base64,abc" } }
        ]);
        let result = super::sanitize_message_content(array.clone(), true);
        assert_eq!(result, Some(array));
    }

    #[test]
    fn test_sanitize_non_array_unchanged_regardless() {
        let text = Value::String("hello".to_string());
        assert_eq!(
            super::sanitize_message_content(text.clone(), true),
            Some(text.clone())
        );
        assert_eq!(
            super::sanitize_message_content(text.clone(), false),
            Some(text)
        );
    }

    fn catalog_ids(entries: &[UpstreamModelEntry]) -> Vec<&str> {
        entries.iter().map(|entry| entry.id.as_str()).collect()
    }

    #[test]
    fn extract_supported_model_catalog_reads_standard_models_list() {
        let body = serde_json::json!({
            "object": "list",
            "data": [
                {"id": "glm-5.1"},
                {"id": "Qwen3.5"},
                "grok-4"
            ]
        });

        // Ids preserved, including the bare-string entry (context_limit None).
        assert_eq!(
            catalog_ids(&extract_supported_model_catalog(&body)),
            vec!["glm-5.1", "Qwen3.5", "grok-4"]
        );
        assert!(
            extract_supported_model_catalog(&body)
                .iter()
                .all(|entry| entry.context_limit.is_none())
        );
    }

    #[test]
    fn extract_supported_model_catalog_reads_models_array() {
        let body = serde_json::json!({"models": ["glm-5.1"]});
        assert_eq!(
            extract_supported_model_catalog(&body),
            vec![UpstreamModelEntry {
                id: "glm-5.1".to_string(),
                context_limit: None,
            }]
        );
    }

    #[test]
    fn extract_supported_model_catalog_reads_ids_and_context_limits_in_one_pass() {
        // Same snapshot yields ids AND limits. Each of the FIVE alias keys gets
        // its own ISOLATED entry where it is the ONLY positive limit, so deleting
        // any single alias from `entry_context_limit` flips that entry's limit to
        // `None` and fails this test. A separate precedence entry locks the
        // search ORDER (`max_input_tokens` > `context_length` > `context_window`
        // > `max_context_length` > `max_model_len`). An entry with no positive
        // context length (zero, missing, or a bare string) keeps its id with a
        // `None` limit (budgeting no-ops for it, but it still resolves).
        let body = serde_json::json!({
            // Isolated positive coverage: one entry per alias key.
            "data": [
                {"id": "k_max_input_tokens", "max_input_tokens": 1000},
                {"id": "k_context_length", "context_length": 8192},
                {"id": "k_context_window", "context_window": 2048},
                {"id": "k_max_context_length", "max_context_length": 32768},
                {"id": "k_max_model_len", "max_model_len": 4096},
                // Precedence: every key positive but DIFFERENT, so the winner
                // pins the ordering (a lower-priority key winning would change
                // this value). `max_input_tokens` must win.
                {
                    "id": "precedence",
                    "max_input_tokens": 11,
                    "context_length": 22,
                    "context_window": 33,
                    "max_context_length": 44,
                    "max_model_len": 55,
                },
                // No positive limit -> None (still resolves as an id).
                {"id": "zero", "context_length": 0},
                {"id": "missing"},
                "bare",
            ]
        });

        assert_eq!(
            extract_supported_model_catalog(&body),
            vec![
                UpstreamModelEntry {
                    id: "k_max_input_tokens".to_string(),
                    context_limit: Some(1000)
                },
                UpstreamModelEntry {
                    id: "k_context_length".to_string(),
                    context_limit: Some(8192)
                },
                UpstreamModelEntry {
                    id: "k_context_window".to_string(),
                    context_limit: Some(2048)
                },
                UpstreamModelEntry {
                    id: "k_max_context_length".to_string(),
                    context_limit: Some(32768)
                },
                UpstreamModelEntry {
                    id: "k_max_model_len".to_string(),
                    context_limit: Some(4096)
                },
                UpstreamModelEntry {
                    id: "precedence".to_string(),
                    context_limit: Some(11)
                },
                UpstreamModelEntry {
                    id: "zero".to_string(),
                    context_limit: None
                },
                UpstreamModelEntry {
                    id: "missing".to_string(),
                    context_limit: None
                },
                UpstreamModelEntry {
                    id: "bare".to_string(),
                    context_limit: None
                },
            ]
        );
    }

    #[test]
    fn extract_supported_model_catalog_handles_empty_body() {
        assert!(extract_supported_model_catalog(&serde_json::json!({})).is_empty());
    }

    /// A typed `stop` on the request counts as request-set, so a configured
    /// `stop` default must NOT gap-fill into `extra_body["stop"]`. Otherwise the
    /// wire would carry BOTH a typed `stop` and an `extra_body["stop"]` key
    /// (duplicate/conflicting), breaking request-wins. Covers the Anthropic path
    /// (stop_sequences → typed stop) sharing this merge with configured kwargs.
    #[test]
    fn merge_chat_kwargs_gap_fill_does_not_shadow_typed_stop() {
        let mut request = family_request("m");
        request.stop = Some(vec!["STOP".to_string()]);
        let defaults = JsonMap::from_iter([("stop".to_string(), json!(["CONFIGURED"]))]);

        merge_chat_kwargs_gap_fill(&mut request, &defaults);

        assert_eq!(request.stop, Some(vec!["STOP".to_string()]));
        assert!(!request.extra_body.contains_key("stop"));
    }

    /// The collapsed gap-fill helper ALWAYS applies the max-token-alias skip, so
    /// a configured/provider `max_tokens` default cannot land alongside a client
    /// max-token alias surviving in `extra_body` (e.g. a `/v1/responses`
    /// `max_completion_tokens`). This previously only held on the leaf path; the
    /// fallback path now shares the same guard.
    #[test]
    fn merge_chat_kwargs_gap_fill_skips_max_token_alias_when_client_alias_present() {
        let mut request = family_request("m");
        request
            .extra_body
            .insert("max_completion_tokens".to_string(), json!(256));
        let defaults = JsonMap::from_iter([("max_tokens".to_string(), json!(4096))]);

        merge_chat_kwargs_gap_fill(&mut request, &defaults);

        // The provider/config alias must NOT land alongside the client alias.
        assert!(!request.extra_body.contains_key("max_tokens"));
        assert_eq!(
            request.extra_body.get("max_completion_tokens"),
            Some(&json!(256))
        );
    }

    // -------------------------------------------------------------------
    // D2: BackendChatRequest identity (response_id + ServingToken) — the
    // production rebuilds preserve both, and a per-flow token prevents the
    // cross-flow `{route, provider}` overwrite race.
    // -------------------------------------------------------------------

    /// A throwaway leaf client (the URL/auth are irrelevant — these tests exercise
    /// the pure request-rebuild helpers, no network).
    fn d2_leaf_client() -> ReqwestUpstreamClient {
        ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            url::Url::parse("https://upstream.test/v1").expect("url"),
            None,
            None,
            true,
            4096,
        )
    }

    /// A `BackendChatRequest` carrying a fresh serving token + response_id, exactly
    /// as the engine mints it per flow.
    fn d2_backend_with_identity(model: &str, response_id: &str) -> BackendChatRequest {
        BackendChatRequest::new(
            family_request(model),
            None,
            Some(response_id.to_string()),
            Some(Arc::new(super::ServingToken::default())),
        )
    }

    #[test]
    fn failover_rebuild_preserves_response_id_and_shares_serving_arc() {
        // `request_for_provider` is the failover production rebuild. It must carry
        // `response_id` forward AND clone the SAME `Arc<ServingToken>` (so a tag set
        // on the rebuilt request is visible on the original — they share the token).
        let backend = d2_backend_with_identity("glm-x", "resp_failover");
        let provider = FailoverUpstreamProvider::new(
            "p0",
            d2_leaf_client(),
            Some("upstream-model".to_string()),
            None,
            JsonMap::new(),
        );
        let rebuilt = FailoverUpstreamClient::request_for_provider(&provider, &backend);
        assert_eq!(rebuilt.response_id.as_deref(), Some("resp_failover"));
        let orig = backend.serving.as_ref().expect("original token");
        let reb = rebuilt.serving.as_ref().expect("rebuilt token");
        assert!(
            Arc::ptr_eq(orig, reb),
            "the rebuild must clone (share) the Arc, not allocate a new token"
        );
        // Proof they share: a tag through the rebuilt request is observable on the
        // original (same underlying token).
        reb.set_provider("p0");
        assert_eq!(orig.snapshot().1.as_deref(), Some("p0"));
    }

    #[test]
    fn routing_rebuild_preserves_response_id_and_shares_serving_arc() {
        // `routed_request` is the routing production rebuild. Same contract.
        let routing = super::RoutingUpstreamClient::new(Vec::new());
        let backend = d2_backend_with_identity("requested", "resp_routing");
        let rebuilt = routing.routed_request(
            &backend,
            "served-model",
            "prov-a",
            super::MatchKind::ExactId,
        );
        assert_eq!(rebuilt.response_id.as_deref(), Some("resp_routing"));
        assert_eq!(rebuilt.request.model, "served-model");
        let orig = backend.serving.as_ref().expect("original token");
        let reb = rebuilt.serving.as_ref().expect("rebuilt token");
        assert!(
            Arc::ptr_eq(orig, reb),
            "routing rebuild must share the serving Arc"
        );
        reb.set_route("prov-a");
        assert_eq!(orig.snapshot().0.as_deref(), Some("prov-a"));
    }

    #[test]
    fn serving_token_fields_are_first_writer_wins_and_independent() {
        // route and provider are set by different layers; each is first-writer-wins
        // and the two fields are independent.
        let token = super::ServingToken::default();
        token.set_route("route-1");
        token.set_route("route-2"); // ignored
        token.set_provider("prov-1");
        token.set_provider("prov-2"); // ignored
        assert_eq!(
            token.snapshot(),
            (Some("route-1".to_string()), Some("prov-1".to_string()))
        );
    }

    #[test]
    fn concurrent_flows_have_independent_serving_tokens() {
        // The rev2 race: a CLIENT-WIDE token would let flow B's provider overwrite
        // flow A's. A FRESH token per flow (what the engine allocates) keeps each
        // flow's `{route, provider}` distinct even under concurrent writes. Two
        // flows, each with its OWN token threaded through a failover rebuild — assert
        // no cross-flow bleed.
        let provider_a =
            FailoverUpstreamProvider::new("vllm-a", d2_leaf_client(), None, None, JsonMap::new());
        let provider_b =
            FailoverUpstreamProvider::new("sglang-b", d2_leaf_client(), None, None, JsonMap::new());
        let backend_a = d2_backend_with_identity("m", "resp_a");
        let backend_b = d2_backend_with_identity("m", "resp_b");
        let token_a = Arc::clone(backend_a.serving.as_ref().unwrap());
        let token_b = Arc::clone(backend_b.serving.as_ref().unwrap());

        // Distinct tokens up front (each flow minted its own).
        assert!(!Arc::ptr_eq(&token_a, &token_b));

        let handles: Vec<_> = [
            (backend_a, provider_a, "route-a", "vllm-a"),
            (backend_b, provider_b, "route-b", "sglang-b"),
        ]
        .into_iter()
        .map(|(backend, provider, route, provider_name)| {
            std::thread::spawn(move || {
                // The routing layer tags route on the rebuilt request's token; the
                // failover layer tags provider on first-chunk success.
                let rebuilt = FailoverUpstreamClient::request_for_provider(&provider, &backend);
                rebuilt.serving.as_ref().unwrap().set_route(route);
                rebuilt
                    .serving
                    .as_ref()
                    .unwrap()
                    .set_provider(provider_name);
            })
        })
        .collect();
        for handle in handles {
            handle.join().expect("thread");
        }

        // No cross-flow overwrite: each flow's token holds ONLY its own values.
        assert_eq!(
            token_a.snapshot(),
            (Some("route-a".to_string()), Some("vllm-a".to_string())),
            "flow A kept its own serving identity"
        );
        assert_eq!(
            token_b.snapshot(),
            (Some("route-b".to_string()), Some("sglang-b".to_string())),
            "flow B kept its own serving identity — no bleed from A"
        );
    }

    // ---- D2 leaf on-wire capture (real ReqwestUpstreamClient ↔ wiremock) ----

    use crate::dashboard_flow::AbortHub;
    use crate::dashboard_flow::DashboardFlowStore;
    use futures::StreamExt as _;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;
    use wiremock::matchers::method as wm_method;
    use wiremock::matchers::path as wm_path;

    #[tokio::test]
    async fn count_tokens_posts_finalized_payload_to_server_root_tokenize() {
        use super::UpstreamClient as _;

        let server = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/tokenize"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"count": 42})))
            .expect(1)
            .mount(&server)
            .await;

        let client = ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            format!("{}/v1", server.uri()).parse().expect("url"),
            None,
            None,
            true,
            4096,
        );
        let mut request = family_request("served-model");
        request.messages.push(ChatMessage {
            role: "user".to_string(),
            content: Some(json!([
                {"type": "text", "text": "hello"},
                {"type": "text", "text": "world"}
            ])),
            tool_call_id: None,
            name: None,
            reasoning_content: None,
            thinking: None,
            tool_calls: None,
        });
        let backend = BackendChatRequest::new(request, None, None, None);

        assert_eq!(
            client.count_tokens(&backend).await.expect("count"),
            Some(42)
        );
        let received = server.received_requests().await.expect("requests");
        let body: Value = serde_json::from_slice(&received[0].body).expect("json body");
        assert_eq!(body["model"], json!("served-model"));
        assert_eq!(body["messages"][0]["content"], json!("hello\nworld"));
        assert!(body.get("stream").is_none());
    }

    /// A minimal valid upstream SSE success body (`bounded_sse_byte_stream` parses
    /// it, so `stream_chat_completion` returns Ok and the capture is observable).
    fn d2_sse_ok_body() -> String {
        "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"m\",\
         \"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n\
         data: [DONE]\n\n"
            .to_string()
    }

    /// Open + link a flow in the store so the leaf's `set_upstream(response_id, …)`
    /// resolves to a live record. Returns the `(api_call_id, response_id)`.
    fn d2_open_linked_flow(store: &DashboardFlowStore) -> (String, String) {
        let api_call_id = "api_leaf".to_string();
        let response_id = "resp_leaf".to_string();
        store.open(
            api_call_id.clone(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            crate::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
            None,
            crate::dashboard_flow::ClientAttribution::none(),
        );
        store.link(response_id.clone(), api_call_id.clone());
        (api_call_id, response_id)
    }

    /// A real leaf client pointed at `server`, with the ENABLED store attached.
    fn d2_capturing_client(server_uri: &str, store: DashboardFlowStore) -> ReqwestUpstreamClient {
        ReqwestUpstreamClient::with_options(
            reqwest::Client::new(),
            format!("{server_uri}/v1/").parse().expect("url"),
            None,
            None,
            true,
            4096,
            1024 * 1024,
        )
        .with_flow_store(store)
    }

    #[tokio::test]
    async fn leaf_captures_post_sanitize_upstream_body_not_pre_leaf() {
        // The captured `upstream_body` must be the POST-`sanitize_chat_request` body.
        // Probe: send `tool_choice="auto"` with NO tools — the PRE-leaf body carries
        // it, but `sanitize_chat_request` CLEARS it. If capture were pre-leaf, the
        // stored body would still contain `"tool_choice"`. We also set
        // `max_output_tokens` to assert the on-wire `max_tokens` rename round-trips.
        let server = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(d2_sse_ok_body(), "text/event-stream"),
            )
            .mount(&server)
            .await;

        let store = DashboardFlowStore::new();
        let (api_call_id, response_id) = d2_open_linked_flow(&store);
        let client = d2_capturing_client(&server.uri(), store.clone());

        let mut request = family_request("served-model");
        request.tool_choice = Some(json!("auto")); // cleared by sanitize (no tools)
        request.max_output_tokens = Some(1234);
        let backend = BackendChatRequest::new(
            request,
            None,
            Some(response_id.clone()),
            Some(Arc::new(super::ServingToken::default())),
        );

        let mut stream = client
            .stream_chat_completion(&backend)
            .await
            .expect("stream opens");
        while stream.next().await.is_some() {}

        let record = store.detail(&api_call_id).expect("record");
        let body = record
            .upstream_body
            .as_ref()
            .expect("upstream_body captured");
        let value: serde_json::Value =
            serde_json::from_slice(body).expect("captured upstream body is valid JSON");
        assert_eq!(value["model"], json!("served-model"));
        assert_eq!(
            value["max_tokens"],
            json!(1234),
            "on-wire `max_tokens` (renamed from max_output_tokens) captured"
        );
        assert!(
            value.get("tool_choice").is_none(),
            "auto tool_choice with no tools was sanitized away → capture is POST-sanitize, not pre-leaf: {value}"
        );
        // FULL equality (R1 #3): the captured body must equal wiremock's COMPLETE
        // received body — not just the spot-checked fields. This body carries no
        // secrets/URIs, so the capped+redacting serializer is a structural identity;
        // any divergence (a dropped/added/reordered field, a redaction that fired on
        // benign content) fails here.
        let received = server
            .received_requests()
            .await
            .expect("wiremock recorded requests");
        assert_eq!(received.len(), 1, "exactly one upstream POST");
        let on_wire: serde_json::Value =
            serde_json::from_slice(&received[0].body).expect("received body is JSON");
        assert_eq!(
            value, on_wire,
            "captured upstream_body must equal the FULL on-wire body byte-for-byte (structural)"
        );
        // The same record joins by response_id too.
        assert!(store.detail(&response_id).is_some());
        // D5 R1 #4: the leaf now threads the FINALIZED served model into the record,
        // so `model_served` is the real model — NOT `None` (which would collapse every
        // metrics bucket to "unknown").
        assert_eq!(
            record.model_served.as_deref(),
            Some("served-model"),
            "leaf populated model_served with the finalized request model"
        );
    }

    #[tokio::test]
    async fn leaf_populated_model_served_reaches_metrics_bucket_not_unknown() {
        // End-to-end (D5 R1 #4): the leaf threads the finalized served model into the
        // FlowStore record; the engine's terminal seam reads `record.model_served` and
        // records it into the MetricsLayer. Drive that whole path and assert the
        // metrics bucket carries the REAL model, never the "unknown" sentinel.
        let server = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(d2_sse_ok_body(), "text/event-stream"),
            )
            .mount(&server)
            .await;

        let store = DashboardFlowStore::new();
        let (api_call_id, response_id) = d2_open_linked_flow(&store);
        let client = d2_capturing_client(&server.uri(), store.clone());

        let request = family_request("served-model");
        let backend = BackendChatRequest::new(
            request,
            None,
            Some(response_id),
            Some(Arc::new(super::ServingToken::default())),
        );
        let mut stream = client
            .stream_chat_completion(&backend)
            .await
            .expect("stream opens");
        while stream.next().await.is_some() {}

        // The leaf captured the served model onto the record.
        let record = store.detail(&api_call_id).expect("record");
        assert_eq!(record.model_served.as_deref(), Some("served-model"));

        // Simulate the engine terminal seam: finalize + record the terminal into the
        // metrics layer sourcing the served model from the record (exactly as
        // `Gateway::record_terminal_metrics` does).
        store.finalize(
            &api_call_id,
            crate::dashboard_flow::FlowStatus::Completed,
            None,
            None,
        );
        let metrics = crate::metrics::MetricsLayer::new();
        let finalized = store.detail(&api_call_id).expect("finalized record");
        metrics.record_terminal(
            finalized.status,
            finalized.model_served.as_deref(),
            &finalized.uri,
            finalized.upstream_target.as_deref(),
            finalized.elapsed_ms.unwrap_or(0),
            finalized.usage,
            &[],
        );

        // The metrics bucket carries the REAL served model — NOT "unknown".
        let view = metrics.view();
        let (key, _counts) = view
            .window_1m
            .buckets
            .iter()
            .next()
            .expect("a metrics bucket");
        assert_eq!(
            key.model, "served-model",
            "metrics bucket carries the real served model, not the unknown sentinel"
        );
        assert_ne!(
            key.model, "unknown",
            "served model did not collapse to unknown"
        );
    }

    #[tokio::test]
    async fn leaf_captures_retry_body_on_shrink_path() {
        // On a context-overflow, the leaf shrinks `max_tokens` and retries ONCE. The
        // captured `upstream_body` must equal the RETRY (shrunk) body — the bytes
        // that ACTUALLY went on the wire — not the first oversized attempt. wiremock
        // returns the overflow 400 first, then a 200, using `up_to_n_times` + an
        // expect-order scenario.
        let server = MockServer::start().await;
        let overflow = "This model's maximum context length is 202752 tokens. \
            However, you requested 64000 output tokens and your prompt contains 139000 input tokens.";
        // First call: 400 overflow.
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_string(overflow))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Second call: 200 success.
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(d2_sse_ok_body(), "text/event-stream"),
            )
            .mount(&server)
            .await;

        let store = DashboardFlowStore::new();
        let (api_call_id, response_id) = d2_open_linked_flow(&store);
        let client = d2_capturing_client(&server.uri(), store.clone());

        let mut request = family_request("served-model");
        // Oversized budget that the shrink path will reduce. 202752 - 100 - 139000.
        request.max_output_tokens = Some(64000);
        let backend = BackendChatRequest::new(
            request,
            None,
            Some(response_id),
            Some(Arc::new(super::ServingToken::default())),
        );

        let mut stream = client
            .stream_chat_completion(&backend)
            .await
            .expect("retry stream opens");
        while stream.next().await.is_some() {}

        let record = store.detail(&api_call_id).expect("record");
        let body = record
            .upstream_body
            .as_ref()
            .expect("upstream_body captured");
        let value: serde_json::Value =
            serde_json::from_slice(body).expect("captured retry body is valid JSON");
        assert_eq!(
            value["max_tokens"],
            json!(63652),
            "captured body is the SHRUNK retry body (202752-100-139000), not the 64000 first attempt: {value}"
        );
        // FULL equality on the shrink-retry path (R1 #3): wiremock saw TWO POSTs —
        // the oversized first attempt and the shrunk retry. The captured body must
        // equal the SECOND (retry) body byte-for-byte AND differ from the first,
        // proving capture follows the bytes that ACTUALLY went on the wire.
        let received = server
            .received_requests()
            .await
            .expect("wiremock recorded requests");
        assert_eq!(received.len(), 2, "first attempt + one shrink retry");
        let first: serde_json::Value =
            serde_json::from_slice(&received[0].body).expect("first body is JSON");
        let retry: serde_json::Value =
            serde_json::from_slice(&received[1].body).expect("retry body is JSON");
        assert_eq!(
            value, retry,
            "captured body must equal the FULL retry (second) on-wire body (structural)"
        );
        assert_ne!(
            value, first,
            "captured body must DIFFER from the first oversized attempt (it is the retry, not attempt 1)"
        );
        assert_eq!(
            first["max_tokens"],
            json!(64000),
            "first attempt carried the oversized budget"
        );
    }

    use crate::turn_capture::TurnCapture;

    /// A unique scratch dir for one F1d turn-capture test (mirrors
    /// `turn_capture.rs`'s own `temp_dir_path` test helper).
    fn turn_capture_test_dir(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "llmconduit-upstream-turn-capture-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ))
    }

    /// Poll for the assembled `<dir>/<api_call_id>.json` turn-capture artifact.
    /// These F1d unit tests dispatch through the leaf/failover client directly
    /// (bypassing the HTTP/engine layer), so the test itself drives
    /// `served_done`/`engine_done` to complete the both-`done` assembly barrier;
    /// this then polls for the async-spawned result. Bounded wait: a barrier that
    /// never resolves fails the test loudly instead of hanging.
    async fn wait_for_turn_capture_artifact(
        dir: &std::path::Path,
        api_call_id: &str,
    ) -> serde_json::Value {
        let path = dir.join(format!("{api_call_id}.json"));
        for _ in 0..300 {
            if let Ok(bytes) = std::fs::read(&path)
                && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
            {
                return value;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("turn-capture artifact never appeared at {}", path.display());
    }

    /// Finding 1: `offload_redacted_upstream_request_bytes` produces IDENTICAL
    /// redacted output on the inline (small) and `spawn_blocking` (large) paths — a
    /// secret (`api_key` → `extra_body`) and an image `data:` URI are BOTH redacted,
    /// with no raw secret/image bytes surviving. The large request (a padded message
    /// content) trips `estimate_request_text_len > TURN_CAPTURE_INLINE_REDACT_LIMIT`
    /// so the redaction runs OFF the tokio worker; the small one stays inline. Both
    /// paths reuse the SAME `redacted_upstream_request_bytes`, so redaction is
    /// unchanged.
    #[tokio::test]
    async fn f1_offload_upstream_request_redacts_small_inline_and_large_spawn_blocking() {
        let make = |filler: &str| -> ChatCompletionRequest {
            serde_json::from_value(json!({
                "model": "m",
                "stream": true,
                "api_key": "sk-LEAK-UPSTREAM-9999",
                "messages": [{
                    "role": "user",
                    "content": [
                        {"type": "image_url",
                         "image_url": {"url": "data:image/png;base64,RAWUPSTREAMIMG7777"}},
                        {"type": "text", "text": filler}
                    ]
                }]
            }))
            .expect("valid chat request")
        };

        let small = make("hi");
        assert!(
            super::estimate_request_text_len(&small)
                <= super::TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES,
            "small request takes the inline path"
        );
        let red_small = super::offload_redacted_upstream_request_bytes(&small)
            .await
            .expect("inline redaction returns bytes");

        let large = make(&"x".repeat(40 * 1024));
        assert!(
            super::estimate_request_text_len(&large)
                > super::TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES,
            "large request takes the spawn_blocking path"
        );
        let red_large = super::offload_redacted_upstream_request_bytes(&large)
            .await
            .expect("spawn_blocking redaction returns bytes");

        for (label, redacted) in [
            ("small/inline", red_small),
            ("large/spawn_blocking", red_large),
        ] {
            let text = String::from_utf8(redacted).expect("redacted section is UTF-8");
            assert!(
                !text.contains("sk-LEAK-UPSTREAM-9999"),
                "{label}: secret value redacted"
            );
            assert!(
                text.contains("[redacted]"),
                "{label}: secret marker present"
            );
            assert!(
                !text.contains("RAWUPSTREAMIMG7777"),
                "{label}: raw image bytes redacted"
            );
            assert!(
                text.contains("<redacted uri>"),
                "{label}: image URI marker present"
            );
        }
    }

    /// F1d AC-10: the artifact's `upstream_request` equals the FINAL on-wire
    /// OpenAI request (post sanitize/profile lowering), redacted (an image URI
    /// AND a secret both redacted). A shrink-and-retry turn (context-overflow →
    /// shrink) shows the SHRUNK request (reduced `max_tokens`), NOT the first
    /// oversized one. Mirrors `leaf_captures_retry_body_on_shrink_path`'s wiremock
    /// shape, but proves the turn-capture `upstream_request` section (Topic F)
    /// rather than the D2 FlowStore capture — the two are independent surfaces
    /// fed by the SAME dispatch.
    #[tokio::test]
    async fn f1d_ac10_upstream_request_captures_shrunk_redacted_final_request() {
        let server = MockServer::start().await;
        let overflow = "This model's maximum context length is 202752 tokens. \
            However, you requested 64000 output tokens and your prompt contains 139000 input tokens.";
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_string(overflow))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(d2_sse_ok_body(), "text/event-stream"),
            )
            .mount(&server)
            .await;

        let client = ReqwestUpstreamClient::with_options(
            reqwest::Client::new(),
            format!("{}/v1/", server.uri()).parse().expect("url"),
            None,
            None,
            true,
            4096,
            1024 * 1024,
        );

        let dir = turn_capture_test_dir("ac10");
        let capture = TurnCapture::enabled(dir.clone());
        let state = capture.start("api_ac10", None, 0).expect("state");
        // `finalize_and_assemble` awaits every section's close, `inbound_request`
        // included; this unit test bypasses the HTTP layer (which is the only
        // production caller of `write_inbound_request`), so close it manually —
        // otherwise the both-`done` barrier never resolves.
        state.write_inbound_request(b"{}");

        let mut request = family_request("served-model");
        // Oversized budget that the shrink path will reduce (202752-100-139000).
        request.max_output_tokens = Some(64000);
        // Redaction probe (AC-10): a secret + an image URI both present on the
        // SAME request that actually goes on the wire for BOTH attempts.
        request
            .extra_body
            .insert("api_key".to_string(), json!("sk-LEAKED-SECRET-VALUE"));
        request.extra_body.insert(
            "probe_image".to_string(),
            json!("data:image/png;base64,RAWIMGBYTES9999"),
        );
        let backend = BackendChatRequest::new(request, None, None, None)
            .with_capture(Some(Arc::clone(&state)));

        let mut stream = client
            .stream_chat_completion(&backend)
            .await
            .expect("retry stream opens");
        while stream.next().await.is_some() {}

        // This unit test bypasses the HTTP/engine layer, so simulate the turn's
        // terminal seam directly to trigger the both-`done` assembly barrier.
        state.served_done(false);
        state.engine_done("completed", None);

        let artifact = wait_for_turn_capture_artifact(&dir, "api_ac10").await;
        let section = &artifact["sections"]["upstream_request"];
        assert_eq!(
            section["content"]["max_tokens"], 63652,
            "captured upstream_request is the SHRUNK retry, not the first 64000 attempt: {section}"
        );
        let content_str = section["content"].to_string();
        assert!(
            !content_str.contains("sk-LEAKED-SECRET-VALUE"),
            "secret value must be redacted: {content_str}"
        );
        assert!(
            content_str.contains("[redacted]"),
            "secret redaction marker present: {content_str}"
        );
        assert!(
            !content_str.contains("RAWIMGBYTES9999"),
            "no raw image bytes may survive: {content_str}"
        );
        assert!(
            content_str.contains("<redacted uri>"),
            "image URI redaction marker present: {content_str}"
        );
    }

    /// F1d AC-11: a FAILOVER turn (provider A fails → provider B serves) still
    /// captures — the rebuild (`request_for_provider`) preserves the capture
    /// handle — and `upstream_request` is the SERVING (B's) attempt's request,
    /// not A's failed one. Mirrors
    /// `gap05_failover_a500_then_b200_commits_no_stale_error_body`'s two-provider
    /// wiremock shape; the two providers carry DIFFERENT `upstream_model`
    /// rewrites so the captured section's `model` unambiguously identifies WHICH
    /// provider's request was recorded.
    #[tokio::test]
    async fn f1d_ac11_failover_upstream_request_captures_serving_providers_request() {
        let down = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string(r#"{"error":{"message":"provider A is down"}}"#),
            )
            .mount(&down)
            .await;
        let up = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(d2_sse_ok_body()))
            .mount(&up)
            .await;

        let dir = turn_capture_test_dir("ac11");
        let capture = TurnCapture::enabled(dir.clone());
        let state = capture.start("api_ac11", None, 0).expect("state");
        // See the AC-10 test above: close `inbound_request` manually since this
        // unit test bypasses the HTTP layer.
        state.write_inbound_request(b"{}");

        // Cooldown 0 so the failover loop tries provider A then B in one dispatch.
        // Dashboard (D2) capture is OFF on both leaves — F1 turn-capture is
        // independent of the dashboard/FlowStore (AGENTS.md own-gate principle).
        let failover = FailoverUpstreamClient::new(
            vec![
                FailoverUpstreamProvider::new(
                    "primary",
                    d2_capturing_client(&down.uri(), DashboardFlowStore::disabled()),
                    Some("model-a".to_string()),
                    None,
                    JsonMap::new(),
                ),
                FailoverUpstreamProvider::new(
                    "backup",
                    d2_capturing_client(&up.uri(), DashboardFlowStore::disabled()),
                    Some("model-b".to_string()),
                    None,
                    JsonMap::new(),
                ),
            ],
            std::time::Duration::from_secs(0),
        );

        let backend = BackendChatRequest::new(family_request("m"), None, None, None)
            .with_capture(Some(Arc::clone(&state)));
        let mut stream = failover
            .stream_chat_completion(&backend)
            .await
            .expect("failover serves from the backup");
        while stream.next().await.is_some() {}

        state.served_done(false);
        state.engine_done("completed", None);

        let artifact = wait_for_turn_capture_artifact(&dir, "api_ac11").await;
        let section = &artifact["sections"]["upstream_request"];
        assert_eq!(
            section["content"]["model"], "model-b",
            "upstream_request is the SERVING provider's (B's) request, not A's failed one: {section}"
        );
    }

    /// A leaf pointed at `server` that returns the SSE 200 success body; `bare`
    /// marks it the direct engine upstream (D2 `into_bare_primary`).
    async fn d2_ok_leaf(server: &MockServer, bare: bool) -> ReqwestUpstreamClient {
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(d2_sse_ok_body(), "text/event-stream"),
            )
            .mount(server)
            .await;
        let leaf = ReqwestUpstreamClient::with_options(
            reqwest::Client::new(),
            format!("{}/v1/", server.uri()).parse().expect("url"),
            None,
            None,
            true,
            4096,
            1024 * 1024,
        );
        if bare { leaf.into_bare_primary() } else { leaf }
    }

    #[tokio::test]
    async fn bare_leaf_tags_provider_primary() {
        // When the leaf is the engine's upstream DIRECTLY (marked `into_bare_primary`),
        // it synthesizes `provider = "primary"` so every flow carries a provider.
        let server = MockServer::start().await;
        let client = d2_ok_leaf(&server, true).await;
        let token = Arc::new(super::ServingToken::default());
        let backend =
            BackendChatRequest::new(family_request("m"), None, None, Some(Arc::clone(&token)));
        let mut stream = client
            .stream_chat_completion(&backend)
            .await
            .expect("stream opens");
        while stream.next().await.is_some() {}

        assert_eq!(
            token.snapshot().1.as_deref(),
            Some("primary"),
            "bare leaf path tags a synthetic `primary` provider"
        );
    }

    #[tokio::test]
    async fn nested_failover_leaf_does_not_clobber_real_provider() {
        // A leaf NESTED in a failover client must NOT tag `"primary"` — otherwise
        // (first-writer-wins, and the leaf runs before the failover's first-chunk
        // success) it would win over the REAL provider name. Drive the failover
        // client end-to-end and assert the token carries the provider's name.
        let server = MockServer::start().await;
        let leaf = d2_ok_leaf(&server, false).await; // NOT bare — it is wrapped
        let failover = FailoverUpstreamClient::new(
            vec![FailoverUpstreamProvider::new(
                "real-vllm",
                leaf,
                None,
                None,
                JsonMap::new(),
            )],
            std::time::Duration::from_secs(0),
        );
        let token = Arc::new(super::ServingToken::default());
        let backend =
            BackendChatRequest::new(family_request("m"), None, None, Some(Arc::clone(&token)));
        let mut stream = failover
            .stream_chat_completion(&backend)
            .await
            .expect("stream opens");
        while stream.next().await.is_some() {}

        assert_eq!(
            token.snapshot().1.as_deref(),
            Some("real-vllm"),
            "the failover layer's real provider name wins; the nested leaf did NOT tag `primary`"
        );
    }

    #[tokio::test]
    async fn failover_remap_metrics_bucket_is_served_model_not_requested() {
        // D5 R4 (MEDIUM): when failover/routing rewrites `request.model`, the D5 metrics
        // model bucket must attribute the ACTUAL on-wire (served) model — read off the
        // shared `ServingToken` — NOT the engine's PRE-routing guess. Drive a real
        // failover client end-to-end through the real leaf: the client requests
        // `requested-model`, but the provider's `upstream_model` remaps it to
        // `served-model`, so `request_for_provider` rewrites `request.model` BEFORE the
        // leaf POSTs. The leaf finalizes `served-model` onto the token
        // (`set_model_served_final`), overwriting the engine's pre-routing
        // `requested-model` guess seeded below. Assert the token's metrics model (the
        // exact value the L1 guard records) is `served-model`, and confirm it end-to-end
        // through a real guard into the MetricsLayer bucket.
        let server = MockServer::start().await;

        // Enabled store so the leaf also captures the record (joins by response_id).
        let store = DashboardFlowStore::new();
        let (api_call_id, response_id) = d2_open_linked_flow(&store);

        // Real leaf pointed at wiremock, NOT bare (it is wrapped by failover).
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(d2_sse_ok_body(), "text/event-stream"),
            )
            .mount(&server)
            .await;
        let leaf = ReqwestUpstreamClient::with_options(
            reqwest::Client::new(),
            format!("{}/v1/", server.uri()).parse().expect("url"),
            None,
            None,
            true,
            4096,
            1024 * 1024,
        )
        .with_flow_store(store.clone());

        // The provider remaps the requested model to the model it actually serves.
        let failover = FailoverUpstreamClient::new(
            vec![FailoverUpstreamProvider::new(
                "real-vllm",
                leaf,
                Some("served-model".to_string()),
                None,
                JsonMap::new(),
            )],
            std::time::Duration::from_secs(0),
        );

        let token = Arc::new(super::ServingToken::default());
        // The engine pre-writes its PRE-routing guess (the requested model) onto the
        // token, exactly as `run_turn` does before dispatch.
        token.set_model_served("requested-model");

        let backend = BackendChatRequest::new(
            family_request("requested-model"),
            None,
            Some(response_id.clone()),
            Some(Arc::clone(&token)),
        );
        let mut stream = failover
            .stream_chat_completion(&backend)
            .await
            .expect("stream opens");
        while stream.next().await.is_some() {}

        // The authoritative value the metrics layer reads off the token is the ACTUAL
        // served model — the leaf's finalized value overwrote the pre-routing guess.
        let (model_served, _usage) = token.metrics_snapshot();
        assert_eq!(
            model_served.as_deref(),
            Some("served-model"),
            "token metrics model is the leaf-finalized served model, not the pre-routing guess"
        );
        assert_ne!(
            model_served.as_deref(),
            Some("requested-model"),
            "the pre-routing requested model did NOT win"
        );
        // The leaf also captured the remapped model onto the FlowStore record.
        let record = store.detail(&api_call_id).expect("record");
        assert_eq!(record.model_served.as_deref(), Some("served-model"));

        // End-to-end: drive a REAL guard finalize → MetricsLayer (exactly the engine's
        // terminal seam) and assert the bucket model is the served model. The guard reads
        // `model_served` off the token via `metrics_snapshot`, so this proves a
        // failover-remapped request is bucketed under the model the backend answered as.
        let guard = store
            .engine_guard(&api_call_id, Arc::clone(&token), &AbortHub::new())
            .expect("claim");
        guard.finalize(crate::dashboard_flow::FlowStatus::Completed, None);
        let inputs = guard.terminal_metrics().expect("terminal metrics");
        assert_eq!(inputs.model_served.as_deref(), Some("served-model"));

        let metrics = crate::metrics::MetricsLayer::new();
        metrics.record_terminal(
            crate::dashboard_flow::FlowStatus::Completed,
            inputs.model_served.as_deref(),
            &inputs.endpoint,
            inputs.upstream.as_deref(),
            guard.elapsed().as_millis(),
            inputs.usage,
            &[],
        );
        let view = metrics.view();
        let (key, _counts) = view
            .window_1m
            .buckets
            .iter()
            .next()
            .expect("a metrics bucket");
        assert_eq!(
            key.model, "served-model",
            "D5 metrics bucket attributes the actual served model on a failover remap"
        );
    }

    // ---- Gap 03: `attempts[]` + `first_upstream_byte_ms` ----

    /// `record_attempt` pushes attempts in order; the FIRST served attempt's wire
    /// first-byte becomes the flow-level `first_upstream_byte_ms` (first-write-wins), and
    /// a FAILED attempt never sets it (don't-lie-with-zeros — no measured wire byte).
    #[test]
    fn serving_token_records_attempts_and_first_byte_first_write_wins() {
        use crate::dashboard_flow::AttemptErrorClass;
        use crate::dashboard_flow::AttemptFailoverReason;
        use crate::dashboard_flow::AttemptStatus;
        let token = super::ServingToken::default();
        token.record_attempt(crate::dashboard_flow::Attempt {
            provider: Some("p0".to_string()),
            model: Some("m".to_string()),
            start_ms: 10,
            end_ms: 20,
            first_upstream_byte_ms: None,
            status: AttemptStatus::Failed,
            error_class: Some(AttemptErrorClass::HttpStatus),
            failover_reason: Some(AttemptFailoverReason::ProviderFailed),
        });
        token.record_attempt(crate::dashboard_flow::Attempt {
            provider: Some("p1".to_string()),
            model: Some("m".to_string()),
            start_ms: 21,
            end_ms: 40,
            first_upstream_byte_ms: Some(30),
            status: AttemptStatus::Served,
            error_class: None,
            failover_reason: None,
        });
        // A later served attempt cannot clobber the recorded flow-level first byte.
        token.record_attempt(crate::dashboard_flow::Attempt {
            provider: Some("p2".to_string()),
            model: Some("m".to_string()),
            start_ms: 41,
            end_ms: 60,
            first_upstream_byte_ms: Some(50),
            status: AttemptStatus::Served,
            error_class: None,
            failover_reason: None,
        });
        let (attempts, first_byte) = token.attempts_snapshot();
        assert_eq!(attempts.len(), 3, "all attempts recorded in order");
        assert_eq!(attempts[0].provider.as_deref(), Some("p0"));
        assert_eq!(attempts[0].status, AttemptStatus::Failed);
        assert_eq!(attempts[1].status, AttemptStatus::Served);
        assert_eq!(
            first_byte,
            Some(30),
            "first served attempt's wire first-byte wins; a later one cannot clobber it"
        );
    }

    /// The taxonomy maps llmconduit's own bounded error shapes — never raw upstream text.
    #[test]
    fn classify_attempt_error_is_bounded_taxonomy() {
        use crate::dashboard_flow::AttemptErrorClass;
        assert_eq!(
            classify_attempt_error(&AppError::upstream("upstream stream timed out".to_string())),
            AttemptErrorClass::Timeout
        );
        assert_eq!(
            classify_attempt_error(&AppError::upstream(
                "upstream chat request failed: connection refused".to_string()
            )),
            AttemptErrorClass::Connect
        );
        assert_eq!(
            classify_attempt_error(&AppError::upstream(
                "upstream stream ended before the first chunk".to_string()
            )),
            AttemptErrorClass::Stream
        );
        // First-chunk read/parse failures from `stream_success_response` carry FIXED
        // gateway prefixes and must classify as `Stream`, not fall through to `Other`.
        assert_eq!(
            classify_attempt_error(&AppError::upstream(
                "failed to parse upstream chat chunk: expected value at line 1 column 1; \
                 payload=<redacted>"
                    .to_string()
            )),
            AttemptErrorClass::Stream,
            "first-chunk parse failure must classify as Stream"
        );
        assert_eq!(
            classify_attempt_error(&AppError::upstream(
                "failed to read upstream SSE: connection reset by peer".to_string()
            )),
            AttemptErrorClass::Stream,
            "first-chunk SSE read failure must classify as Stream"
        );
        assert_eq!(
            classify_attempt_error(&AppError::upstream("upstream chat failed with 503: x")),
            AttemptErrorClass::HttpStatus
        );
        assert_eq!(
            classify_attempt_error(&AppError::upstream_with_disposition(
                "overflow persisted",
                FailoverDisposition::Terminal
            )),
            AttemptErrorClass::Terminal
        );
        // E2a (dashboard taxonomy): a request-intrinsic 4xx is ALSO tagged `Terminal`
        // disposition (no failover/cooldown — `dispatch_chat_stream`), but it is a
        // genuine upstream HTTP-status response, so it must classify as `HttpStatus`,
        // NOT `AttemptErrorClass::Terminal` (which stays reserved for a context-window
        // overflow that survived shrink-and-retry, asserted above with a message that
        // does NOT share this prefix). Only `failover_reason` (tested separately via
        // `record_attempt`/`failed_bare_attempt`) distinguishes the two Terminal
        // producers in the trace — `error_class` alone must not collapse them.
        assert_eq!(
            classify_attempt_error(&AppError::upstream_with_disposition(
                "upstream chat failed with 400: {\"error\":{\"message\":\"model is not \
                 multimodal\"}}",
                FailoverDisposition::Terminal
            )),
            AttemptErrorClass::HttpStatus,
            "a Terminal-tagged request-intrinsic 4xx must still classify as HttpStatus"
        );
    }

    /// E2a: the exact status set that is `Terminal` (never cools/fails over a healthy
    /// provider) vs. the exact status set that keeps today's failover-eligible behavior.
    /// The two predicates are disjoint by construction (`status_is_request_intrinsic_4xx`
    /// asserts this internally too); this test pins the literal membership so a future
    /// edit cannot silently widen/narrow either set without a red test.
    #[test]
    fn request_intrinsic_4xx_set_is_exactly_400_413_415_422() {
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::PAYLOAD_TOO_LARGE,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            StatusCode::UNPROCESSABLE_ENTITY,
        ] {
            assert!(
                super::status_is_request_intrinsic_4xx(status),
                "{status} must be request-intrinsic (Terminal, no failover/cooldown)"
            );
            assert!(
                !super::status_is_failover_eligible(status),
                "{status} must NOT also be failover-eligible (the two sets are disjoint)"
            );
        }
        // 401/403/404/408/429 and a representative 5xx MUST NOT be request-intrinsic —
        // they keep today's failover + cooldown (disposition matrix, spec E2).
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert!(
                !super::status_is_request_intrinsic_4xx(status),
                "{status} must NOT be request-intrinsic — it keeps failover + cooldown"
            );
        }
        // And the failover-eligible set is exactly what it always was: 5xx/408/429.
        for status in [
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert!(
                super::status_is_failover_eligible(status),
                "{status} must stay failover-eligible (unchanged by E2a)"
            );
        }
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
        ] {
            assert!(
                !super::status_is_failover_eligible(status),
                "{status} is a provider-config 4xx, not server-error/408/429 — it fails \
                 over via the generic error path, not `status_is_failover_eligible`"
            );
        }
    }

    /// F2 (round-1 review): the bounded code must be driven by the gateway's OWN fixed
    /// leaf-error PREFIX, never by a `contains()` scan of the interpolated (redacted but
    /// attacker-influenced) upstream response body. An HTTP-status failure whose body
    /// happens to contain the literal text `"timed out"` / `"request failed"` /
    /// `"failed to parse"` MUST still classify as `HttpStatus` — the body can never flip
    /// the code to `Timeout`/`Connect`/`Stream`.
    #[test]
    fn classify_attempt_error_ignores_upstream_body_text() {
        use crate::dashboard_flow::AttemptErrorClass;
        // The gateway builds this exact shape in `dispatch_chat_stream`; `{body}` is the
        // redacted upstream response body and is fully under the upstream's control.
        for hostile_body in [
            "the request timed out upstream",
            "upstream chat request failed: boom",
            "could not parse: failed to parse json",
            "upstream stream ended before the first chunk",
            "request failed and timed out",
            // Bodies echoing the NEW first-chunk Stream prefixes must NOT flip a
            // genuine HTTP-status failure to `Stream` (the prefix sits AFTER the
            // fixed `"upstream chat failed with {status}: "`, so `starts_with` on
            // the Stream prefixes can never match here).
            "failed to parse upstream chat chunk: injected",
            "failed to read upstream SSE: injected",
        ] {
            let err = AppError::upstream(format!("upstream chat failed with 500: {hostile_body}"));
            assert_eq!(
                classify_attempt_error(&err),
                AttemptErrorClass::HttpStatus,
                "HTTP-status failure must stay HttpStatus regardless of body text: {hostile_body:?}"
            );
        }
        // A generic non-leaf upstream error (cooldown / no-providers) is `Other`, not
        // silently `HttpStatus` via the 502-collapse the old `(400..600)` fallback hit.
        assert_eq!(
            classify_attempt_error(&AppError::upstream(
                "all upstream providers are in cooldown; next retry in 5s; last error: x"
                    .to_string()
            )),
            AttemptErrorClass::Other,
            "a generic 502 gateway error is Other, not HttpStatus"
        );
    }

    /// A real bare-leaf single success records EXACTLY ONE served attempt with a measured
    /// wire `first_upstream_byte_ms` (the bare-leaf analogue of the prefetch point).
    #[tokio::test]
    async fn bare_leaf_single_success_records_one_served_attempt_with_first_byte() {
        use crate::dashboard_flow::AttemptStatus;
        let server = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(d2_sse_ok_body()))
            .mount(&server)
            .await;

        let leaf = ReqwestUpstreamClient::with_options(
            reqwest::Client::new(),
            format!("{}/v1/", server.uri()).parse().expect("url"),
            None,
            None,
            true,
            4096,
            1024 * 1024,
        )
        .into_bare_primary();

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some("resp_bare".to_string()),
            Some(Arc::clone(&token)),
        );
        let mut stream = leaf
            .stream_chat_completion(&backend)
            .await
            .expect("stream opens");
        // The served attempt's first byte is recorded WHEN the first chunk is consumed.
        while stream.next().await.is_some() {}

        let (attempts, first_byte) = token.attempts_snapshot();
        assert_eq!(attempts.len(), 1, "bare leaf records exactly one attempt");
        assert_eq!(attempts[0].status, AttemptStatus::Served);
        assert_eq!(attempts[0].provider.as_deref(), Some("primary"));
        assert!(
            attempts[0].first_upstream_byte_ms.is_some(),
            "served attempt measured a wire first-byte"
        );
        assert!(
            attempts[0].error_class.is_none(),
            "served attempt has no error_class"
        );
        assert_eq!(
            first_byte, attempts[0].first_upstream_byte_ms,
            "flow-level first byte is the served attempt's wire first-byte"
        );
    }

    /// F1 (round-1 review): a bare-leaf HTTP-status FAILURE (non-2xx) records ONE failed
    /// attempt that DOES carry a measured wire first-byte — the response HEADERS arrived
    /// (that is what makes it an HTTP-status failure rather than a connect failure), so the
    /// wire TTFB is real even though the request ultimately failed. The flow-level first
    /// byte stays `None` (no attempt SERVED). Contrast `bare_leaf_connect_failure_*` below,
    /// where headers never arrive and the byte time is `None`.
    #[tokio::test]
    async fn bare_leaf_http_status_failure_records_failed_attempt_with_measured_first_byte() {
        use crate::dashboard_flow::AttemptErrorClass;
        use crate::dashboard_flow::AttemptStatus;
        let server = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
            .mount(&server)
            .await;

        let leaf = ReqwestUpstreamClient::with_options(
            reqwest::Client::new(),
            format!("{}/v1/", server.uri()).parse().expect("url"),
            None,
            None,
            true,
            4096,
            1024 * 1024,
        )
        .into_bare_primary();

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some("resp_bare_fail".to_string()),
            Some(Arc::clone(&token)),
        );
        let result = leaf.stream_chat_completion(&backend).await;
        assert!(result.is_err(), "503 dispatch fails");

        let (attempts, first_byte) = token.attempts_snapshot();
        assert_eq!(attempts.len(), 1, "one failed attempt recorded");
        assert_eq!(attempts[0].status, AttemptStatus::Failed);
        assert_eq!(attempts[0].error_class, Some(AttemptErrorClass::HttpStatus));
        assert!(
            attempts[0].first_upstream_byte_ms.is_some(),
            "F1: an HTTP-status failure received response headers → measured wire first-byte"
        );
        assert!(
            first_byte.is_none(),
            "no served attempt → no flow-level first byte"
        );
    }

    /// Gap 05: with the upstream-response capture gate ARMED, a failing turn's upstream
    /// ERROR body is STAGED on the shared `ServingToken` (copied through the capped/redacting
    /// serializer) and lands on the live FlowStore record when the L1 guard commits it at
    /// finalize. This is the leaf wiring that answers "what did the upstream actually say
    /// back when this failed?". Round-1 review (F1): the body rides the token so it reflects
    /// the turn's FINAL outcome — committed at finalize, here simulated by the guard's
    /// `set_upstream_response(api_call_id, token.take_pending_response_body())` commit.
    #[tokio::test]
    async fn gap05_leaf_captures_upstream_error_body_when_gate_on() {
        let server = MockServer::start().await;
        let error_body =
            r#"{"error":{"message":"backend is on fire","type":"server_error","code":500}}"#;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(500).set_body_string(error_body))
            .mount(&server)
            .await;

        // Capture-armed store (gate ON, deterministic — no env mutation).
        let store = DashboardFlowStore::new_with_response_capture(true);
        let (api_call_id, response_id) = d2_open_linked_flow(&store);
        let leaf = ReqwestUpstreamClient::with_options(
            reqwest::Client::new(),
            format!("{}/v1/", server.uri()).parse().expect("url"),
            None,
            None,
            true,
            4096,
            1024 * 1024,
        )
        .into_bare_primary()
        .with_flow_store(store.clone());

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some(response_id.clone()),
            Some(Arc::clone(&token)),
        );
        assert!(
            leaf.stream_chat_completion(&backend).await.is_err(),
            "500 dispatch fails"
        );

        // F1: the leaf STAGES the error body on the token; the record carries it only after
        // the guard commits at finalize. Simulate that commit (the bare-leaf turn FAILED, so
        // nothing cleared the pending body).
        store.set_upstream_response(&api_call_id, token.take_pending_response_body());

        let record = store.detail(&api_call_id).expect("record");
        let captured = record
            .upstream_response
            .as_ref()
            .expect("gap05: upstream error body captured onto the record");
        assert!(!captured.truncated, "a small error body is not truncated");
        let text = String::from_utf8_lossy(&captured.bytes);
        assert!(
            text.contains("backend is on fire"),
            "the diagnostic upstream error body is retained for the operator: {text}"
        );
    }

    /// Gap 05: with the gate OFF (the DEFAULT — request capture does not imply response
    /// capture), the SAME failing turn retains NO upstream response body. `None` (absent),
    /// not an empty body — the gate is a real opt-in: the leaf stages nothing on the token
    /// AND `set_upstream_response` no-ops, so even the guard's finalize commit lands `None`.
    #[tokio::test]
    async fn gap05_leaf_does_not_capture_upstream_error_body_when_gate_off() {
        let server = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(500).set_body_string("backend is on fire"))
            .mount(&server)
            .await;

        // Request capture ON (default debug-UI store) but the response gate OFF.
        let store = DashboardFlowStore::new_with_response_capture(false);
        let (api_call_id, response_id) = d2_open_linked_flow(&store);
        let leaf = ReqwestUpstreamClient::with_options(
            reqwest::Client::new(),
            format!("{}/v1/", server.uri()).parse().expect("url"),
            None,
            None,
            true,
            4096,
            1024 * 1024,
        )
        .into_bare_primary()
        .with_flow_store(store.clone());

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some(response_id.clone()),
            Some(Arc::clone(&token)),
        );
        assert!(
            leaf.stream_chat_completion(&backend).await.is_err(),
            "500 dispatch fails"
        );

        // Even driving the guard's finalize commit lands nothing: the leaf staged no body
        // (gated), and `set_upstream_response` is itself gated off.
        store.set_upstream_response(&api_call_id, token.take_pending_response_body());

        let record = store.detail(&api_call_id).expect("record");
        assert!(
            record.upstream_response.is_none(),
            "gap05: response capture OFF ⇒ no upstream error body retained (None, not empty)"
        );
    }

    /// Gap 05 round-1 review (F1): the CORRECTNESS case the review flagged — provider A
    /// returns 500, provider B serves 200. Provider A's nested leaf STAGES its 500 error
    /// body on the shared token, but provider B's serve CLEARS it, so the turn's FINAL
    /// `upstream_response` committed at finalize is `None`. Without the fix, provider A's
    /// stale error body would remain on a SUCCESSFUL turn (gap 14 would misclassify it).
    /// The served-attempt-is-authoritative model (gap 03) holds for the captured body too.
    #[tokio::test]
    async fn gap05_failover_a500_then_b200_commits_no_stale_error_body() {
        let down = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string(r#"{"error":{"message":"provider A is down"}}"#),
            )
            .mount(&down)
            .await;
        let up = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(d2_sse_ok_body()))
            .mount(&up)
            .await;

        // One shared capture-ARMED store: the flow is opened here, the nested leaves use it
        // to pass the response-capture gate, and the (simulated) guard commit lands here.
        let store = DashboardFlowStore::new_with_response_capture(true);
        let (api_call_id, response_id) = d2_open_linked_flow(&store);

        // Cooldown 0 so the failover loop tries provider A then B in one dispatch. The
        // nested leaves are NOT `into_bare_primary` — the failover loop owns provider tagging
        // (and the gap-05 serve-success clear).
        let failover = FailoverUpstreamClient::new(
            vec![
                FailoverUpstreamProvider::new(
                    "primary",
                    d2_capturing_client(&down.uri(), store.clone()),
                    None,
                    None,
                    JsonMap::new(),
                ),
                FailoverUpstreamProvider::new(
                    "backup",
                    d2_capturing_client(&up.uri(), store.clone()),
                    None,
                    None,
                    JsonMap::new(),
                ),
            ],
            std::time::Duration::from_secs(0),
        );

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some(response_id.clone()),
            Some(Arc::clone(&token)),
        );
        let mut stream = failover
            .stream_chat_completion(&backend)
            .await
            .expect("failover serves from the backup");
        while stream.next().await.is_some() {}

        // The token's pending body was cleared by provider B's serve, so a served turn
        // commits `None` — exactly the guard's finalize commit (here simulated).
        assert!(
            token.take_pending_response_body().is_none(),
            "F1: a later provider serving CLEARS the earlier provider's staged error body"
        );
        store.set_upstream_response(&api_call_id, token.take_pending_response_body());

        let record = store.detail(&api_call_id).expect("record");
        assert!(
            record.upstream_response.is_none(),
            "F1: a successful (failed-over) turn must carry NO stale upstream error body"
        );
    }

    /// Gap 05 round-1 review (F1): the all-providers-fail case — provider A 500, provider B
    /// 500. No provider serves, so the serve-success clear never fires; the LAST attempt's
    /// (provider B's) error body remains staged on the token and is what finalize commits as
    /// the turn's FINAL `upstream_response`. This is the legitimate "captured body" case the
    /// fix must preserve (the operator sees the upstream's final word on a genuinely-failed
    /// turn), distinct from the served-turn `None` above.
    #[tokio::test]
    async fn gap05_failover_all_fail_commits_final_error_body() {
        let down_a = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(500).set_body_string("provider A error body"))
            .mount(&down_a)
            .await;
        let down_b = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(503)
                    .set_body_string(r#"{"error":{"message":"provider B final word"}}"#),
            )
            .mount(&down_b)
            .await;

        let store = DashboardFlowStore::new_with_response_capture(true);
        let (api_call_id, response_id) = d2_open_linked_flow(&store);

        let failover = FailoverUpstreamClient::new(
            vec![
                FailoverUpstreamProvider::new(
                    "primary",
                    d2_capturing_client(&down_a.uri(), store.clone()),
                    None,
                    None,
                    JsonMap::new(),
                ),
                FailoverUpstreamProvider::new(
                    "backup",
                    d2_capturing_client(&down_b.uri(), store.clone()),
                    None,
                    None,
                    JsonMap::new(),
                ),
            ],
            std::time::Duration::from_secs(0),
        );

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some(response_id.clone()),
            Some(Arc::clone(&token)),
        );
        assert!(
            failover.stream_chat_completion(&backend).await.is_err(),
            "all providers fail ⇒ the failover dispatch errors"
        );

        // No provider served, so the last attempt's (provider B's) body is still staged.
        store.set_upstream_response(&api_call_id, token.take_pending_response_body());

        let record = store.detail(&api_call_id).expect("record");
        let captured = record
            .upstream_response
            .as_ref()
            .expect("F1: an all-fail turn commits the upstream's final error body");
        let text = String::from_utf8_lossy(&captured.bytes);
        assert!(
            text.contains("provider B final word"),
            "F1: the FINAL (last-tried) provider's error body is the turn's upstream_response, \
             not the first provider's: {text}"
        );
        assert!(
            !text.contains("provider A error body"),
            "F1: the earlier provider's body was overwritten by the final one: {text}"
        );
    }

    /// Gap 05 review ROUND 2 (HIGH): the edge case the per-attempt clear fixes — provider
    /// A returns 500 WITH an error body, then provider B fails at the TRANSPORT layer
    /// (connect refused, no response headers, NO HTTP error body). The turn's FINAL failure
    /// carried no body, so the committed `upstream_response` must be `None` — NOT provider
    /// A's stale 500 body. Before the fix, A's body stayed staged through B's body-less
    /// failure (B never re-stages, and the serve-success clear never fires on an all-fail
    /// turn) and was wrongly committed. The fix clears the staged body at the START of
    /// each attempt, so B's start-of-attempt clear discards A's body and nothing re-stages.
    #[tokio::test]
    async fn gap05_failover_a500_then_b_transport_failure_commits_no_stale_body() {
        let down_a = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string(r#"{"error":{"message":"provider A stale body"}}"#),
            )
            .mount(&down_a)
            .await;

        let store = DashboardFlowStore::new_with_response_capture(true);
        let (api_call_id, response_id) = d2_open_linked_flow(&store);

        // Provider B points at a reserved/closed port → TCP connect refused BEFORE any
        // response headers: a Connect-class failure with NO HTTP error body to stage.
        let failover = FailoverUpstreamClient::new(
            vec![
                FailoverUpstreamProvider::new(
                    "primary",
                    d2_capturing_client(&down_a.uri(), store.clone()),
                    None,
                    None,
                    JsonMap::new(),
                ),
                FailoverUpstreamProvider::new(
                    "backup",
                    d2_capturing_client("http://127.0.0.1:1", store.clone()),
                    None,
                    None,
                    JsonMap::new(),
                ),
            ],
            std::time::Duration::from_secs(0),
        );

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some(response_id.clone()),
            Some(Arc::clone(&token)),
        );
        assert!(
            failover.stream_chat_completion(&backend).await.is_err(),
            "all providers fail ⇒ the failover dispatch errors"
        );

        // Round 2 (HIGH): the FINAL attempt (B) failed WITHOUT a body, so the per-attempt
        // clear left the slot empty — finalize commits `None`, never A's stale 500 body.
        assert!(
            token.take_pending_response_body().is_none(),
            "round 2: a body-less FINAL failure leaves no staged body (A's was cleared)"
        );
        store.set_upstream_response(&api_call_id, token.take_pending_response_body());

        let record = store.detail(&api_call_id).expect("record");
        assert!(
            record.upstream_response.is_none(),
            "round 2: a turn whose FINAL failure carried no body commits None, not the \
             earlier provider's stale 500 body"
        );
    }

    /// Gap 05 review ROUND 2 (HIGH): the no-first-chunk variant of the same edge case —
    /// provider A returns 500 WITH a body, then provider B returns 200 headers but its
    /// stream ends before yielding ANY chunk (a prefetch `Stream`-class failure). B's 2xx
    /// path stages NO error body (only non-2xx sites stage), and the serve-success clear is
    /// reached only on a SUCCESSFUL prefetch — not this post-headers stream failure. So the
    /// turn's final failure again carries no body, and the committed `upstream_response`
    /// must be `None`, not provider A's stale 500 body. The per-attempt clear is what makes
    /// this hold (B's start-of-attempt clear discards A's body).
    #[tokio::test]
    async fn gap05_failover_a500_then_b_no_first_chunk_commits_no_stale_body() {
        let down_a = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string(r#"{"error":{"message":"provider A stale body"}}"#),
            )
            .mount(&down_a)
            .await;
        // Provider B: 200 OK headers but an EMPTY SSE body — the stream ends before the
        // first chunk, so `prefetch_first_chunk` fails AFTER headers (a Stream-class,
        // body-less failure on a 2xx that never staged an error body).
        let empty_b = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(String::new(), "text/event-stream"),
            )
            .mount(&empty_b)
            .await;

        let store = DashboardFlowStore::new_with_response_capture(true);
        let (api_call_id, response_id) = d2_open_linked_flow(&store);

        let failover = FailoverUpstreamClient::new(
            vec![
                FailoverUpstreamProvider::new(
                    "primary",
                    d2_capturing_client(&down_a.uri(), store.clone()),
                    None,
                    None,
                    JsonMap::new(),
                ),
                FailoverUpstreamProvider::new(
                    "backup",
                    d2_capturing_client(&empty_b.uri(), store.clone()),
                    None,
                    None,
                    JsonMap::new(),
                ),
            ],
            std::time::Duration::from_secs(0),
        );

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some(response_id.clone()),
            Some(Arc::clone(&token)),
        );
        assert!(
            failover.stream_chat_completion(&backend).await.is_err(),
            "A 500 then B no-first-chunk ⇒ the failover dispatch errors"
        );

        // Round 2 (HIGH): B's 200/no-chunk failure staged no body and cleared A's at the
        // start of its attempt, so the committed body is None — not A's stale 500.
        store.set_upstream_response(&api_call_id, token.take_pending_response_body());

        let record = store.detail(&api_call_id).expect("record");
        assert!(
            record.upstream_response.is_none(),
            "round 2: a 200-but-no-first-chunk FINAL failure commits None, not the earlier \
             provider's stale 500 body"
        );
    }

    /// F1 (round-1 review): a bare-leaf CONNECT failure (no upstream is listening, so
    /// `send().await` errors BEFORE any response headers) records ONE failed attempt whose
    /// wire first-byte is `None` — the slot is never stamped because the leaf never received
    /// headers. This is the ONLY case F1 leaves `None` (don't-lie-with-zeros).
    #[tokio::test]
    async fn bare_leaf_connect_failure_records_failed_attempt_with_no_first_byte() {
        use crate::dashboard_flow::AttemptErrorClass;
        use crate::dashboard_flow::AttemptStatus;
        // A reserved-but-closed port: the TCP connect is refused, so `send().await` fails
        // before any HTTP response — a transport/connect error, not an HTTP status.
        let leaf = ReqwestUpstreamClient::with_options(
            reqwest::Client::new(),
            "http://127.0.0.1:1/v1/".parse().expect("url"),
            None,
            None,
            true,
            4096,
            1024 * 1024,
        )
        .into_bare_primary();

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some("resp_bare_connect_fail".to_string()),
            Some(Arc::clone(&token)),
        );
        let result = leaf.stream_chat_completion(&backend).await;
        assert!(result.is_err(), "connect-refused dispatch fails");

        let (attempts, first_byte) = token.attempts_snapshot();
        assert_eq!(attempts.len(), 1, "one failed attempt recorded");
        assert_eq!(attempts[0].status, AttemptStatus::Failed);
        assert_eq!(
            attempts[0].error_class,
            Some(AttemptErrorClass::Connect),
            "a transport failure before any response is classified Connect"
        );
        assert!(
            attempts[0].first_upstream_byte_ms.is_none(),
            "F1: connect-before-response never received headers → no wire first-byte (None)"
        );
        assert!(
            first_byte.is_none(),
            "no served attempt → no flow-level first byte"
        );
    }

    /// A forced failover (first provider 503, second 200) records ≥2 attempts: the first
    /// FAILED with a `failover_reason`, the LAST SERVED with a measured wire first-byte.
    /// This also covers ROUTING mode's attempt source: routing delegates to the selected
    /// provider's failover client, so attempts come from THIS loop — never a sibling
    /// routing upstream (AGENTS.md hard rule).
    #[tokio::test]
    async fn failover_503_then_200_records_failed_then_served_attempts() {
        use crate::dashboard_flow::AttemptErrorClass;
        use crate::dashboard_flow::AttemptFailoverReason;
        use crate::dashboard_flow::AttemptStatus;
        let down = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(503).set_body_string("primary down"))
            .mount(&down)
            .await;
        let up = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(d2_sse_ok_body()))
            .mount(&up)
            .await;

        // Two providers; cooldown 0 so the failover loop tries both in one dispatch.
        let failover = FailoverUpstreamClient::new(
            vec![
                FailoverUpstreamProvider::new(
                    "primary",
                    d2_capturing_client(&down.uri(), DashboardFlowStore::disabled()),
                    None,
                    None,
                    JsonMap::new(),
                ),
                FailoverUpstreamProvider::new(
                    "backup",
                    d2_capturing_client(&up.uri(), DashboardFlowStore::disabled()),
                    None,
                    None,
                    JsonMap::new(),
                ),
            ],
            std::time::Duration::from_secs(0),
        );

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some("resp_failover".to_string()),
            Some(Arc::clone(&token)),
        );
        let mut stream = failover
            .stream_chat_completion(&backend)
            .await
            .expect("failover serves from the backup");
        while stream.next().await.is_some() {}

        let (attempts, first_byte) = token.attempts_snapshot();
        assert!(
            attempts.len() >= 2,
            "failover records the failed provider AND the served one ({})",
            attempts.len()
        );
        let first = &attempts[0];
        assert_eq!(first.provider.as_deref(), Some("primary"));
        assert_eq!(first.status, AttemptStatus::Failed);
        assert_eq!(first.error_class, Some(AttemptErrorClass::HttpStatus));
        assert_eq!(
            first.failover_reason,
            Some(AttemptFailoverReason::ProviderFailed),
            "the failed provider carries a failover_reason"
        );
        assert!(
            first.first_upstream_byte_ms.is_some(),
            "F1: the 503 failed provider DID receive response headers → measured wire \
             first-byte, even though it never produced a first chunk"
        );
        let last = attempts.last().expect("served attempt");
        assert_eq!(last.provider.as_deref(), Some("backup"));
        assert_eq!(last.status, AttemptStatus::Served);
        assert!(
            last.error_class.is_none(),
            "served attempt has no error_class"
        );
        assert!(
            last.first_upstream_byte_ms.is_some(),
            "the served attempt measured a wire first-byte"
        );
        assert_eq!(
            first_byte, last.first_upstream_byte_ms,
            "flow-level first byte is the SERVED attempt's wire first-byte"
        );
    }

    /// F1 (round-1 review): a forced failover where the FIRST provider's connection is
    /// REFUSED (no response headers ever arrive) then the second serves. The failed
    /// provider's attempt is `Connect`-class with `first_upstream_byte_ms == None` (the
    /// don't-lie-with-zeros case F1 preserves), while the served provider carries a measured
    /// wire first-byte. Contrast the 503 case above, where the failed provider DID receive
    /// headers and so DOES carry a byte time.
    #[tokio::test]
    async fn failover_connect_refused_then_200_leaves_failed_first_byte_none() {
        use crate::dashboard_flow::AttemptErrorClass;
        use crate::dashboard_flow::AttemptStatus;
        let up = MockServer::start().await;
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(d2_sse_ok_body()))
            .mount(&up)
            .await;

        let failover = FailoverUpstreamClient::new(
            vec![
                // 127.0.0.1:1 is reserved/closed → TCP connect refused before any response.
                FailoverUpstreamProvider::new(
                    "primary",
                    d2_capturing_client("http://127.0.0.1:1", DashboardFlowStore::disabled()),
                    None,
                    None,
                    JsonMap::new(),
                ),
                FailoverUpstreamProvider::new(
                    "backup",
                    d2_capturing_client(&up.uri(), DashboardFlowStore::disabled()),
                    None,
                    None,
                    JsonMap::new(),
                ),
            ],
            std::time::Duration::from_secs(0),
        );

        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some("resp_failover_connect".to_string()),
            Some(Arc::clone(&token)),
        );
        let mut stream = failover
            .stream_chat_completion(&backend)
            .await
            .expect("failover serves from the backup after the connect refusal");
        while stream.next().await.is_some() {}

        let (attempts, first_byte) = token.attempts_snapshot();
        assert!(attempts.len() >= 2, "failed + served attempts recorded");
        let first = &attempts[0];
        assert_eq!(first.provider.as_deref(), Some("primary"));
        assert_eq!(first.status, AttemptStatus::Failed);
        assert_eq!(
            first.error_class,
            Some(AttemptErrorClass::Connect),
            "a connect refusal is Connect-class"
        );
        assert!(
            first.first_upstream_byte_ms.is_none(),
            "F1: connect-before-response never received headers → None (never 0)"
        );
        let last = attempts.last().expect("served attempt");
        assert_eq!(last.provider.as_deref(), Some("backup"));
        assert_eq!(last.status, AttemptStatus::Served);
        assert!(
            last.first_upstream_byte_ms.is_some(),
            "the served provider measured a wire first-byte"
        );
        assert_eq!(
            first_byte, last.first_upstream_byte_ms,
            "flow-level first byte is the SERVED attempt's wire first-byte, not the failed one's"
        );
    }

    /// AC-1 (E2a): a synthetic upstream returning 400 on a streaming turn does NOT cool
    /// the provider — a SECOND, unrelated request to the SAME (single, non-failover-peer)
    /// provider is still served, not short-circuited by `cooldown_error()`. Regression
    /// test for the field incident: an image reaching a text-only vLLM backend 400'd with
    /// "not a multimodal model", which used to trip a 30s cooldown and 502 every
    /// unrelated request for its duration (`upstream.rs:1807` pre-E2a).
    #[tokio::test]
    async fn request_intrinsic_400_does_not_cool_provider_second_request_served() {
        use crate::dashboard_flow::AttemptErrorClass;
        use crate::dashboard_flow::AttemptFailoverReason;
        use crate::dashboard_flow::AttemptStatus;
        let server = MockServer::start().await;
        // First call: a request-intrinsic 400 (the field incident's exact upstream body).
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_string(
                "{\"error\":{\"message\":\"DeepSeek-V4-Flash-DSpark is not a multimodal \
                 model\",\"type\":\"BadRequestError\",\"code\":400}}",
            ))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Every later call (including the "second, unrelated request" below): 200.
        Mock::given(wm_method("POST"))
            .and(wm_path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(d2_sse_ok_body()))
            .mount(&server)
            .await;

        // A SINGLE provider — no failover peer, so a wrongly-cooled provider leaves
        // `available_provider_indices()` empty and the second call fails via
        // `cooldown_error()` (a 502) WITHOUT ever reaching the mock server. A generous
        // cooldown window (30s, matching the field incident) makes the test meaningful:
        // if the fix regressed, the second call below would fail immediately.
        let failover = FailoverUpstreamClient::new(
            vec![FailoverUpstreamProvider::new(
                "primary",
                d2_capturing_client(&server.uri(), DashboardFlowStore::disabled()),
                None,
                None,
                JsonMap::new(),
            )],
            std::time::Duration::from_secs(30),
        );

        // First (failing) turn.
        let token = Arc::new(super::ServingToken::default());
        let backend = BackendChatRequest::new(
            family_request("m"),
            None,
            Some("resp_400".to_string()),
            Some(Arc::clone(&token)),
        );
        let err = match failover.stream_chat_completion(&backend).await {
            Ok(_) => panic!("a request-intrinsic 400 must surface as an error, not a stream"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("400"),
            "error must carry the upstream status: {err}"
        );

        // Dashboard trace: HttpStatus (a genuine HTTP-status response), NOT
        // `AttemptErrorClass::Terminal` (reserved for context-overflow) — and
        // `failover_reason == TerminalNoFailover` (terminal, no failover attempted).
        let (attempts, _) = token.attempts_snapshot();
        assert_eq!(attempts.len(), 1, "one failed attempt recorded");
        assert_eq!(attempts[0].status, AttemptStatus::Failed);
        assert_eq!(
            attempts[0].error_class,
            Some(AttemptErrorClass::HttpStatus),
            "a request-intrinsic 4xx is a genuine HTTP-status failure, not Terminal-class"
        );
        assert_eq!(
            attempts[0].failover_reason,
            Some(AttemptFailoverReason::RequestRejected),
            "the trace must distinguish request rejection from provider failure"
        );

        // The provider must NOT be cooling: no deadline, no failure counted.
        let health = failover.provider_health();
        assert_eq!(health.len(), 1);
        assert_eq!(
            health[0].status,
            ProviderStatus::Healthy,
            "a request-intrinsic 4xx must never cool a healthy provider"
        );
        assert!(
            health[0].cooling_until_ms.is_none(),
            "no cooldown deadline must be set"
        );
        assert_eq!(
            health[0].failover_count, 0,
            "mark_failure must be skipped for a Terminal disposition"
        );
        assert_eq!(health[0].consecutive_failures, 0);

        // SECOND, unrelated request to the SAME provider: served 200, not 502 — this is
        // the actual regression the field incident hit (every request 502'd for 30s).
        let token2 = Arc::new(super::ServingToken::default());
        let backend2 = BackendChatRequest::new(
            family_request("m"),
            None,
            Some("resp_unrelated".to_string()),
            Some(Arc::clone(&token2)),
        );
        let mut stream = failover
            .stream_chat_completion(&backend2)
            .await
            .expect("the second, unrelated request must be served — provider not cooling");
        let mut served_any = false;
        while let Some(item) = stream.next().await {
            item.expect("second request must stream successfully, not 502");
            served_any = true;
        }
        assert!(
            served_any,
            "the second request must actually stream content"
        );
    }

    /// AC-2 (E2a): 5xx, 408, 429, AND 401/403/404 must STILL cool the provider down AND
    /// fail over to the next one — E2a's new Terminal path is scoped EXACTLY to
    /// `{400,413,415,422}` and must not leak onto any other status, including the other
    /// 4xx codes (401/403/404 are provider/model/auth config problems another provider
    /// may resolve, unlike a request-intrinsic 4xx). Each status gets its own explicit
    /// assertion — they are not lumped into one "some 4xx" check.
    #[tokio::test]
    async fn failover_eligible_statuses_still_cool_and_fail_over_unchanged() {
        use crate::dashboard_flow::AttemptErrorClass;
        use crate::dashboard_flow::AttemptFailoverReason;
        use crate::dashboard_flow::AttemptStatus;
        for status in [500u16, 502, 503, 408, 429, 401, 403, 404] {
            let down = MockServer::start().await;
            Mock::given(wm_method("POST"))
                .and(wm_path("/v1/chat/completions"))
                .respond_with(ResponseTemplate::new(status).set_body_string("provider unavailable"))
                .mount(&down)
                .await;
            let up = MockServer::start().await;
            Mock::given(wm_method("POST"))
                .and(wm_path("/v1/chat/completions"))
                .respond_with(ResponseTemplate::new(200).set_body_string(d2_sse_ok_body()))
                .mount(&up)
                .await;

            let failover = FailoverUpstreamClient::new(
                vec![
                    FailoverUpstreamProvider::new(
                        "primary",
                        d2_capturing_client(&down.uri(), DashboardFlowStore::disabled()),
                        None,
                        None,
                        JsonMap::new(),
                    ),
                    FailoverUpstreamProvider::new(
                        "backup",
                        d2_capturing_client(&up.uri(), DashboardFlowStore::disabled()),
                        None,
                        None,
                        JsonMap::new(),
                    ),
                ],
                std::time::Duration::from_secs(30),
            );

            let token = Arc::new(super::ServingToken::default());
            let backend = BackendChatRequest::new(
                family_request("m"),
                None,
                Some(format!("resp_{status}")),
                Some(Arc::clone(&token)),
            );
            let mut stream = failover
                .stream_chat_completion(&backend)
                .await
                .unwrap_or_else(|err| panic!("status {status} must fail over to backup: {err}"));
            while stream.next().await.is_some() {}

            let (attempts, _) = token.attempts_snapshot();
            assert!(
                attempts.len() >= 2,
                "status {status} must record the failed primary AND the served backup"
            );
            let first = &attempts[0];
            assert_eq!(
                first.status,
                AttemptStatus::Failed,
                "status {status}: primary must be recorded failed"
            );
            assert_eq!(
                first.failover_reason,
                Some(AttemptFailoverReason::ProviderFailed),
                "status {status} must still fail over (unchanged) — not TerminalNoFailover"
            );
            assert_eq!(
                first.error_class,
                Some(AttemptErrorClass::HttpStatus),
                "status {status} classifies HttpStatus same as a Terminal 4xx would"
            );
            let last = attempts.last().expect("served attempt");
            assert_eq!(
                last.status,
                AttemptStatus::Served,
                "status {status} must fail over onto a served backup"
            );

            // The primary provider must STILL be cooling — E2a must not suppress cooldown
            // for these statuses.
            let health = failover.provider_health();
            assert_eq!(
                health[0].status,
                ProviderStatus::Cooling,
                "status {status} must still cool the primary provider (unchanged by E2a)"
            );
            assert!(
                health[0].cooling_until_ms.is_some(),
                "status {status} must set a cooldown deadline"
            );
            assert_eq!(
                health[0].failover_count, 1,
                "status {status} must still count as a failover (mark_failure runs)"
            );
        }
    }

    /// F3 (round-1 review): an attempt's `provider`/`model` scalars are `cap_scalar`-bounded
    /// (≤ 4 KiB) when recorded onto the shared `ServingToken`, so the bounded copy is what
    /// later rides the `FlowRecord` AND the evict-safe terminal payload. An over-cap string
    /// is truncated before retention; the store's `SCALAR_CAP` invariant cannot be bypassed
    /// via the attempt trace.
    #[test]
    fn serving_token_caps_attempt_scalar_strings() {
        use crate::dashboard_flow::AttemptStatus;
        // 4 KiB cap (mirrors dashboard_flow::SCALAR_CAP, which is module-private).
        const SCALAR_CAP: usize = 4 * 1024;
        let token = super::ServingToken::default();
        token.record_attempt(crate::dashboard_flow::Attempt {
            provider: Some("p".repeat(SCALAR_CAP * 2)),
            model: Some("m".repeat(SCALAR_CAP * 2)),
            start_ms: 1,
            end_ms: 2,
            first_upstream_byte_ms: Some(2),
            status: AttemptStatus::Served,
            error_class: None,
            failover_reason: None,
        });
        let (attempts, _) = token.attempts_snapshot();
        assert_eq!(attempts.len(), 1);
        assert!(
            attempts[0].provider.as_deref().unwrap().len() <= SCALAR_CAP,
            "F3: provider scalar capped to SCALAR_CAP before retention"
        );
        assert!(
            attempts[0].model.as_deref().unwrap().len() <= SCALAR_CAP,
            "F3: model scalar capped to SCALAR_CAP before retention"
        );
    }
}

#[cfg(test)]
mod resolve_match_kind_tests {
    use super::*;
    use std::collections::HashMap;

    /// Single-provider catalog serving exactly `model`, with the canonical-key
    /// index populated so rule-4 normalization is exercised too.
    fn catalog_serving(model: &str) -> RoutingModelCatalog {
        let candidate = RoutingModelCandidate {
            provider_index: 0,
            model_id: model.to_string(),
            target: RoutingModelTarget::Primary,
        };
        let mut ids_by_key = HashMap::new();
        ids_by_key.insert(canonical_model_key(model), vec![candidate.clone()]);
        RoutingModelCatalog {
            provider_catalogs: vec![RoutingProviderModelCatalog {
                candidates: vec![candidate],
                context_limit_by_id: HashMap::new(),
            }],
            union_entries: Vec::new(),
            union_ids: vec![model.to_string()],
            union_context_limit_by_id: HashMap::new(),
            ids_by_key,
            routes: Vec::new(),
        }
    }

    fn resolved_id(resolution: &RoutingResolution) -> &str {
        match resolution {
            RoutingResolution::Catalog(candidate) => &candidate.model_id,
            RoutingResolution::Route { model_id, .. } => model_id,
        }
    }

    #[test]
    fn exact_id_reports_exact_match() {
        let (resolution, kind) = catalog_serving("served-model")
            .resolve("served-model")
            .expect("resolves");
        assert_eq!(kind, MatchKind::ExactId);
        assert_eq!(resolved_id(&resolution), "served-model");
    }

    #[test]
    fn case_or_punctuation_variant_reports_canonical_key() {
        // Not an exact id (case differs), so it must normalize via the canonical
        // key rather than silently dropping to the default fallback.
        let (resolution, kind) = catalog_serving("served-model")
            .resolve("SERVED_MODEL")
            .expect("resolves");
        assert_eq!(kind, MatchKind::CanonicalKey);
        assert_eq!(resolved_id(&resolution), "served-model");
    }

    #[test]
    fn unknown_model_reports_default_fallback() {
        // The claude-relay parity case: an incoming `claude-opus-*` name that the
        // backend does not serve falls back to the first catalog model, tagged
        // Default so the dispatch site logs a WARN.
        let (resolution, kind) = catalog_serving("served-model")
            .resolve("claude-opus-4")
            .expect("falls back to default");
        assert_eq!(kind, MatchKind::Default);
        assert_eq!(resolved_id(&resolution), "served-model");
    }

    #[test]
    fn empty_catalog_resolves_to_none() {
        let empty = RoutingModelCatalog {
            provider_catalogs: Vec::new(),
            union_entries: Vec::new(),
            union_ids: Vec::new(),
            union_context_limit_by_id: HashMap::new(),
            ids_by_key: HashMap::new(),
            routes: Vec::new(),
        };
        assert!(empty.resolve("anything").is_none());
    }
}

#[cfg(test)]
mod d4_provider_health_tests {
    use super::*;
    use serde_json::json;

    /// A non-networked leaf pointed at `base` — `provider_health` reads its
    /// `base_url` and the in-memory cooldown/metrics state only, so no request is
    /// ever sent.
    fn leaf(base: &str) -> ReqwestUpstreamClient {
        ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            url::Url::parse(base).expect("url"),
            None,
            None,
            true,
            4096,
        )
    }

    fn provider(name: &str, base: &str) -> FailoverUpstreamProvider {
        FailoverUpstreamProvider::new(name, leaf(base), None, None, JsonMap::new())
    }

    /// FROZEN DTO contract (D9/D10/D12 validate this exact shape): every field
    /// serializes, the `Option` fields appear as JSON `null` (NO
    /// `skip_serializing_if`), `base_url` is a non-null string, `status` is the
    /// snake_case enum, and timestamps are epoch-ms numbers.
    #[test]
    fn provider_health_dto_serializes_all_keys_with_null_options() {
        let health = ProviderHealth {
            id: "p".to_string(),
            name: "p".to_string(),
            route: None,
            base_url: "https://example.invalid/v1".to_string(),
            status: ProviderStatus::Healthy,
            cooling_until_ms: None,
            last_error: None,
            served_count: 0,
            failover_count: 0,
            consecutive_failures: 0,
            catalog_fetched_ms: None,
            catalog_size: None,
        };
        let value = serde_json::to_value(&health).expect("serialize");
        let obj = value.as_object().expect("object");
        // ALL keys present, even the None-valued ones (as explicit null).
        for key in [
            "id",
            "name",
            "route",
            "base_url",
            "status",
            "cooling_until_ms",
            "last_error",
            "served_count",
            "failover_count",
            "consecutive_failures",
            "catalog_fetched_ms",
            "catalog_size",
        ] {
            assert!(obj.contains_key(key), "key `{key}` must always be present");
        }
        assert!(obj["route"].is_null());
        assert!(obj["cooling_until_ms"].is_null());
        assert!(obj["last_error"].is_null());
        assert!(obj["catalog_fetched_ms"].is_null());
        assert!(obj["catalog_size"].is_null());
        assert_eq!(obj["base_url"], json!("https://example.invalid/v1"));
        assert_eq!(obj["status"], json!("healthy"));
        // snake_case enum rendering for the other variants.
        assert_eq!(
            serde_json::to_value(ProviderStatus::Cooling).unwrap(),
            json!("cooling")
        );
        assert_eq!(
            serde_json::to_value(ProviderStatus::Down).unwrap(),
            json!("down")
        );
    }

    /// Dyn-safety + the bare default: `provider_health()` is callable through
    /// `Arc<dyn UpstreamClient>` (object-safe) and a bare leaf reports nothing
    /// (no provider layer owns health).
    #[test]
    fn bare_leaf_provider_health_is_empty_through_dyn() {
        let client: Arc<dyn UpstreamClient> = Arc::new(leaf("https://example.invalid/v1"));
        assert!(client.provider_health().is_empty());
    }

    /// A failover chain reports one entry per provider with the leaf's base_url,
    /// no route (bare failover), zeroed counters initially, and Healthy status.
    #[test]
    fn failover_provider_health_reports_each_provider() {
        let client = FailoverUpstreamClient::new(
            vec![
                provider("primary", "https://a.invalid/v1"),
                provider("backup", "https://b.invalid/v1"),
            ],
            Duration::from_secs(30),
        );
        let health = client.provider_health();
        assert_eq!(health.len(), 2);
        assert_eq!(health[0].id, "primary");
        assert_eq!(health[0].base_url, "https://a.invalid/v1");
        assert_eq!(health[0].route, None);
        assert_eq!(health[0].status, ProviderStatus::Healthy);
        assert_eq!(health[0].served_count, 0);
        assert_eq!(health[0].failover_count, 0);
        assert_eq!(health[0].consecutive_failures, 0);
        assert_eq!(health[0].cooling_until_ms, None);
        assert_eq!(health[1].id, "backup");
        assert_eq!(health[1].base_url, "https://b.invalid/v1");
    }

    /// `served_count` is cumulative and `mark_provider_success` resets the
    /// consecutive-failure streak to 0 AND clears the cooldown — so a provider
    /// driven to `Down` then served once returns to `Healthy`.
    #[test]
    fn consecutive_failures_reset_on_success_and_status_recovers() {
        let client = FailoverUpstreamClient::new(
            vec![provider("p", "https://a.invalid/v1")],
            Duration::from_secs(3600),
        );
        let err = AppError::upstream("boom");
        // Three consecutive failures while cooling → Down.
        client.mark_failure(0, &err);
        client.mark_failure(0, &err);
        client.mark_failure(0, &err);
        let health = client.provider_health();
        assert_eq!(health[0].consecutive_failures, 3);
        assert_eq!(health[0].failover_count, 3);
        assert_eq!(health[0].status, ProviderStatus::Down);
        assert!(health[0].cooling_until_ms.is_some());
        assert!(health[0].last_error.is_some());

        // One success clears the streak + cooldown → Healthy, served bumped.
        client.mark_provider_success(0);
        let health = client.provider_health();
        assert_eq!(health[0].consecutive_failures, 0);
        assert_eq!(health[0].failover_count, 3, "failover_count is cumulative");
        assert_eq!(health[0].served_count, 1);
        assert_eq!(health[0].status, ProviderStatus::Healthy);
        assert_eq!(health[0].cooling_until_ms, None);
        assert_eq!(health[0].last_error, None);
    }

    /// `Down` requires BOTH cooling AND `>= DOWN_THRESHOLD` consecutive failures:
    /// one failure with a live cooldown is only `Cooling`; the third crosses into
    /// `Down`. With a ZERO cooldown, even many failures stay `Healthy` (not
    /// cooling), so the threshold alone never forces `Down`.
    #[test]
    fn down_requires_cooling_and_threshold_failures() {
        let cooling = FailoverUpstreamClient::new(
            vec![provider("p", "https://a.invalid/v1")],
            Duration::from_secs(3600),
        );
        let err = AppError::upstream("boom");
        cooling.mark_failure(0, &err);
        assert_eq!(
            cooling.provider_health()[0].status,
            ProviderStatus::Cooling,
            "one failure while cooling is Cooling, not Down"
        );
        cooling.mark_failure(0, &err);
        cooling.mark_failure(0, &err);
        assert_eq!(
            cooling.provider_health()[0].status,
            ProviderStatus::Down,
            "three consecutive failures while cooling is Down"
        );

        // Zero cooldown: failures bump the streak but the provider is never
        // cooling, so it stays Healthy (Down needs cooling too).
        let no_cooldown = FailoverUpstreamClient::new(
            vec![provider("p", "https://a.invalid/v1")],
            Duration::ZERO,
        );
        for _ in 0..5 {
            no_cooldown.mark_failure(0, &err);
        }
        let health = no_cooldown.provider_health();
        assert_eq!(health[0].consecutive_failures, 5);
        assert_eq!(
            health[0].status,
            ProviderStatus::Healthy,
            "without a cooldown window, the threshold alone never forces Down"
        );
        assert_eq!(health[0].cooling_until_ms, None);
    }

    /// A routing client stamps `route = Some(provider_name)` on each entry and
    /// reports the synthetic route providers too. (No catalog is loaded here, so
    /// catalog meta stays `None` — exercised separately.)
    #[test]
    fn routing_provider_health_stamps_route() {
        let routing_provider = RoutingUpstreamProvider::new(
            "vllm-a",
            leaf("https://a.invalid/v1"),
            None,
            JsonMap::new(),
            Vec::new(),
            Duration::from_secs(30),
        );
        let route_provider = RouteUpstreamProvider::new(
            "route-claude",
            leaf("https://r.invalid/v1"),
            Duration::from_secs(30),
        );
        let client = RoutingUpstreamClient::with_routes(
            vec![routing_provider],
            vec![route_provider],
            Vec::new(),
        );
        let health = client.provider_health();
        // One catalog provider entry + one route provider entry.
        assert_eq!(health.len(), 2);
        let catalog_entry = health
            .iter()
            .find(|h| h.id == "vllm-a")
            .expect("catalog provider");
        assert_eq!(catalog_entry.route.as_deref(), Some("vllm-a"));
        assert_eq!(catalog_entry.base_url, "https://a.invalid/v1");
        assert_eq!(catalog_entry.catalog_fetched_ms, None);
        assert_eq!(catalog_entry.catalog_size, None);
        let route_entry = health
            .iter()
            .find(|h| h.id == "route-claude")
            .expect("route provider");
        assert_eq!(route_entry.route.as_deref(), Some("route-claude"));
    }

    /// No-torn-pair: concurrent `(fetched_ms, size)` swaps (each a SINGLE
    /// `Arc<CatalogMeta>` store) interleaved with lock-free reads must NEVER yield
    /// a half-updated pair. Each writer stores a self-consistent pair where
    /// `size == fetched_ms` (a sentinel invariant); every reader that sees a
    /// populated meta must observe BOTH fields from the SAME store.
    #[test]
    fn no_torn_catalog_meta_pair_under_concurrent_swaps() {
        let meta: Arc<Mutex<Arc<CatalogMeta>>> =
            Arc::new(Mutex::new(Arc::new(CatalogMeta::default())));
        let mut handles = Vec::new();
        // Writers: each stamps a consistent (n, n) pair.
        for _ in 0..4 {
            let meta = Arc::clone(&meta);
            handles.push(std::thread::spawn(move || {
                for n in 1..2000u64 {
                    let next = Arc::new(CatalogMeta {
                        fetched_ms: Some(n),
                        size: Some(n),
                    });
                    *meta.lock().expect("meta lock") = next;
                }
            }));
        }
        // Readers: every populated read must have matching fields (no tear).
        for _ in 0..4 {
            let meta = Arc::clone(&meta);
            handles.push(std::thread::spawn(move || {
                for _ in 0..4000 {
                    let snapshot = Arc::clone(&meta.lock().expect("meta lock"));
                    if let (Some(f), Some(s)) = (snapshot.fetched_ms, snapshot.size) {
                        assert_eq!(f, s, "fetched_ms and size must come from one swap");
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().expect("thread");
        }
    }

    /// The publisher exposes the latest immutable snapshot with a monotonically
    /// increasing version on each publish; a held clone is never mutated under the
    /// reader.
    #[test]
    fn publisher_versions_increase_monotonically() {
        let publisher = ProviderHealthPublisher::default();
        assert_eq!(publisher.latest().version, 0, "default starts at version 0");
        publisher.publish(Vec::new());
        let first = publisher.latest();
        assert_eq!(first.version, 1);
        publisher.publish(vec![ProviderHealth {
            id: "p".to_string(),
            name: "p".to_string(),
            route: None,
            base_url: "https://a.invalid/v1".to_string(),
            status: ProviderStatus::Healthy,
            cooling_until_ms: None,
            last_error: None,
            served_count: 0,
            failover_count: 0,
            consecutive_failures: 0,
            catalog_fetched_ms: None,
            catalog_size: None,
        }]);
        let second = publisher.latest();
        assert_eq!(second.version, 2);
        // The earlier snapshot is immutable — its version/contents are unchanged.
        assert_eq!(first.version, 1);
        assert!(first.providers.is_empty());
        assert_eq!(second.providers.len(), 1);
    }
}

/// F1e review r3 (Finding 2 -- don't claim a complete empty upstream response): the
/// `upstream_response_streamed` discriminator is set on the FIRST real byte / a CLEAN
/// end-of-stream, never at 2xx-header time -- so a bare-leaf cancel after headers but
/// before any byte leaves the section partial/absent (truthful), never an empty
/// `partial:false` "complete" response. These tests drive the real
/// [`tap_upstream_response`] / [`stream_success_response`] code paths.
#[cfg(test)]
mod f1e_upstream_response_truthful_tests {
    use super::stream_success_response;
    use super::tap_upstream_response;
    use crate::turn_capture::TurnCapture;
    use axum::body::Bytes;
    use futures::StreamExt as _;
    use std::sync::Arc;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "llmconduit-tap-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ))
    }

    /// Poll for the assembled artifact (assembly is spawned async); a bounded wait so a
    /// turn that never assembles fails loudly rather than hanging.
    async fn wait_for_artifact(path: &std::path::Path) -> serde_json::Value {
        for _ in 0..300 {
            if let Ok(bytes) = std::fs::read(path) {
                return serde_json::from_slice(&bytes).unwrap_or_else(|err| {
                    panic!("artifact at {} not valid JSON: {err}", path.display())
                });
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("artifact never appeared at {}", path.display());
    }

    /// Finding 2 (the exact scenario): a 2xx upstream response whose raw byte stream is
    /// DROPPED before the first `stream.next()`. `stream_success_response` must NOT have
    /// marked the response "streamed" at header time, so finalize OMITS
    /// `upstream_response` entirely (absent) -- NEVER an empty `partial:false` complete
    /// section. FAILS on the old code, which set the discriminator at header time.
    #[tokio::test]
    async fn stream_success_dropped_before_first_byte_is_absent_not_empty_complete() {
        let dir = temp_dir("ssr-drop");
        let capture = TurnCapture::enabled(dir.clone());
        let state = capture.start("api_ssr_drop", None, 0).expect("state");
        state.write_inbound_request(b"{}");
        {
            let response = reqwest::Response::from(
                http::Response::builder()
                    .status(200)
                    .body(b"data: {\"choices\":[]}\n\n".to_vec())
                    .expect("build response"),
            );
            let stream = stream_success_response(response, 1024 * 1024, Some(Arc::clone(&state)))
                .await
                .expect("stream built");
            // Dropped WITHOUT a single poll -> the tap block never runs, and (fix) there
            // is no eager header-time streamed mark, so the discriminator stays clear.
            drop(stream);
        }
        state.write_served_response(b"");
        state.served_done(true);
        state.engine_done("cancelled", Some("client_disconnect"));

        let artifact = wait_for_artifact(&dir.join("api_ssr_drop.json")).await;
        assert!(
            artifact["sections"].get("upstream_response").is_none(),
            "2xx headers then cancel before the first byte must leave upstream_response \
             ABSENT, never an empty partial:false complete: {artifact}"
        );
    }

    /// Finding 2 (truthful-empty case): a 2xx upstream response with a CLEAN end-of-
    /// stream and ZERO bytes. The tap marks streamed at the clean end, so the section IS
    /// present and `partial:false` with `bytes:0` -- a truthful empty response (allowed),
    /// distinct from the pre-first-byte cancel above (absent).
    #[tokio::test]
    async fn tap_clean_eos_zero_bytes_is_truthful_empty_partial_false() {
        let dir = temp_dir("clean-empty");
        let capture = TurnCapture::enabled(dir.clone());
        let state = capture.start("api_tap_empty", None, 0).expect("state");
        state.write_inbound_request(b"{}");
        {
            let inner = futures::stream::empty::<Result<Bytes, reqwest::Error>>();
            let mut tapped = Box::pin(tap_upstream_response(inner, Arc::clone(&state)));
            while tapped.next().await.is_some() {}
        }
        state.write_served_response(b"");
        state.served_done(false);
        state.engine_done("completed", None);

        let artifact = wait_for_artifact(&dir.join("api_tap_empty.json")).await;
        let section = &artifact["sections"]["upstream_response"];
        assert_eq!(
            section["partial"], false,
            "a clean EOS with zero bytes is a TRUTHFUL partial:false empty: {artifact}"
        );
        assert_eq!(section["bytes"], 0, "zero bytes streamed: {artifact}");
    }

    /// Finding 2 + AC-14: a real byte flows, THEN the stream is cancelled mid-flight. The
    /// discriminator is marked on the FIRST byte (so the partial bytes are embedded), and
    /// the tap guard's early drop keeps the section sticky-`partial:true`.
    #[tokio::test]
    async fn tap_first_byte_then_cancel_embeds_partial_bytes() {
        let dir = temp_dir("byte-then-cancel");
        let capture = TurnCapture::enabled(dir.clone());
        let state = capture.start("api_tap_partial", None, 0).expect("state");
        state.write_inbound_request(b"{}");
        {
            let inner = futures::stream::iter(vec![Ok::<Bytes, reqwest::Error>(
                Bytes::from_static(b"data: hello\n\n"),
            )])
            .chain(futures::stream::pending::<Result<Bytes, reqwest::Error>>());
            let mut tapped = Box::pin(tap_upstream_response(inner, Arc::clone(&state)));
            // First byte flows -> streamed marked on the FIRST byte.
            assert!(tapped.next().await.is_some());
            // The next poll parks (pending); dropping `tapped` = cancel mid-stream.
            let fut = tapped.next();
            futures::pin_mut!(fut);
            assert!(futures::poll!(fut.as_mut()).is_pending());
        }
        state.write_served_response(b"partial");
        state.served_done(true);
        state.engine_done("cancelled", Some("client_disconnect"));

        let artifact = wait_for_artifact(&dir.join("api_tap_partial.json")).await;
        let section = &artifact["sections"]["upstream_response"];
        let content = section["content"].as_str().expect("content string");
        assert!(
            content.contains("hello"),
            "the first byte's bytes are embedded (streamed marked on first byte): {artifact}"
        );
        assert_eq!(
            section["partial"], true,
            "a byte-then-cancel stays sticky-partial (AC-14): {artifact}"
        );
    }
}
