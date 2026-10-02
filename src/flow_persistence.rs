//! Pure adapters between the upstream flow telemetry spine and durable request rows.
//!
//! This module deliberately owns no task, queue, or database handle. The HTTP and
//! engine seams build small commands here and hand them to
//! [`PersistenceQueue`](crate::control_plane_store::PersistenceQueue) with `try_*`.
//! Keeping the conversion pure makes it impossible for a slow store to add latency
//! or back-pressure to inference.
//!
//! The production terminal path must use [`finish_from_terminal`]. Its inputs are
//! the eviction-safe values already held by the engine/`ServingToken`; it never
//! re-reads `DashboardFlowStore`, whose bounded history may evict a live flow before
//! it finishes. [`finish_from_flow_record`] is provided for retained-record/admin
//! import paths and tests, not as the engine's terminal authority.

use crate::content_store::{self, PROTOCOL_CHAT_COMPLETIONS, SplitBody};
use crate::control_plane_store::{
    BlobRow, BodyWrite, EventRow, ItemRow, PersistenceQueue, RequestFinish, RequestRow,
};
use crate::dashboard_flow::{
    Attempt, AttemptStatus, ClientSource, FlowRecord, PhaseTimings, TerminalMetricsInputs,
};
use base64::Engine as _;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Each request retains at most this many bytes in one durable hop event.
///
/// The writer queue is bounded by command count; this second bound prevents one
/// command from smuggling an arbitrarily large allocation through that queue.
pub const EVENT_PAYLOAD_CAP_BYTES: usize = 16 * 1024 * 1024;
const EVENT_SCALAR_CAP_BYTES: usize = 4 * 1024;

/// Typed terminal outcome from `run_turn`'s single completion choke point.
///
/// `FlowStatus::Completed` cannot distinguish a genuine completion from an
/// output-token truncation, so persistence must receive this typed value directly
/// rather than reconstructing it from a `FlowRecord`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceOutcome {
    Completed,
    Incomplete,
    Failed,
    Cancelled,
}

impl PersistenceOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Incomplete => "incomplete",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    const fn carries_error(self) -> bool {
        matches!(self, Self::Failed | Self::Cancelled)
    }
}

/// Request-owned, eviction-safe phase clock for persistence.
///
/// The dashboard record cannot be the durable writer's terminal source because
/// it may be evicted. Create this clock only when persistence is enabled, carry a
/// cheap clone through the existing engine task, and stamp it beside the existing
/// FlowStore phase calls. It has no registry and dies with the request, so memory
/// is bounded by in-flight requests. First-write-wins plus causal clamping mirrors
/// `PhaseTimings` without making persistence depend on dashboard retention.
#[derive(Debug, Clone)]
pub struct PersistencePhaseClock {
    phases: Arc<Mutex<PhaseTimings>>,
}

impl PersistencePhaseClock {
    pub fn new(ingress_ms: u128) -> Self {
        Self {
            phases: Arc::new(Mutex::new(PhaseTimings {
                ingress_ms: Some(ingress_ms),
                ..PhaseTimings::default()
            })),
        }
    }

    pub fn new_now() -> Self {
        Self::new(now_epoch_ms())
    }

    pub fn stamp_normalization_done(&self) {
        self.stamp(Phase::Normalization, now_epoch_ms());
    }

    pub fn stamp_routing_decision(&self) {
        self.stamp(Phase::Routing, now_epoch_ms());
    }

    /// Call only after the first content delta was successfully handed to the
    /// client-facing channel. Reasoning/tool/signature deltas do not count.
    pub fn stamp_first_content_delta(&self) {
        self.stamp(Phase::FirstContent, now_epoch_ms());
    }

    pub fn stamp_stream_end(&self) {
        self.stamp(Phase::StreamEnd, now_epoch_ms());
    }

    pub fn stamp_finalize(&self) {
        self.stamp(Phase::Finalize, now_epoch_ms());
    }

    pub fn snapshot(&self) -> PhaseTimings {
        *self
            .phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn stamp(&self, phase: Phase, now: u128) {
        let mut phases = self
            .phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let floor = match phase {
            Phase::Normalization => phases.ingress_ms,
            Phase::Routing => max_time([phases.ingress_ms, phases.normalization_done_ms]),
            Phase::FirstContent => max_time([
                phases.ingress_ms,
                phases.normalization_done_ms,
                phases.routing_decision_ms,
            ]),
            Phase::StreamEnd => max_time([
                phases.ingress_ms,
                phases.normalization_done_ms,
                phases.routing_decision_ms,
                phases.first_content_delta_ms,
            ]),
            Phase::Finalize => max_time([
                phases.ingress_ms,
                phases.normalization_done_ms,
                phases.routing_decision_ms,
                phases.first_content_delta_ms,
                phases.stream_end_ms,
            ]),
        };
        let slot = match phase {
            Phase::Normalization => &mut phases.normalization_done_ms,
            Phase::Routing => &mut phases.routing_decision_ms,
            Phase::FirstContent => &mut phases.first_content_delta_ms,
            Phase::StreamEnd => &mut phases.stream_end_ms,
            Phase::Finalize => &mut phases.finalize_ms,
        };
        if slot.is_none() {
            *slot = Some(floor.map_or(now, |floor| now.max(floor)));
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Phase {
    Normalization,
    Routing,
    FirstContent,
    StreamEnd,
    Finalize,
}

fn max_time<const N: usize>(values: [Option<u128>; N]) -> Option<u128> {
    values.into_iter().flatten().max()
}

pub fn now_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Exactly-once, nonblocking owner of one durable terminal update.
///
/// This guard is independent of `DashboardFlowStore`: it holds the request's
/// `api_call_id`, response id, phase clock, and the same eviction-safe
/// `ServingToken` the routing/failover layers populate. Therefore persistence
/// remains correct when the debug UI is disabled or its bounded history evicts
/// the live record. Explicit completion wins the atomic claim; `Drop` is the
/// panic/abandoned-task fallback and cannot enqueue a duplicate.
#[must_use = "dropping the guard emits a cancelled terminal fallback"]
pub struct PersistenceTerminalGuard {
    queue: PersistenceQueue,
    api_call_id: String,
    response_id: Mutex<Option<String>>,
    serving: Arc<crate::upstream::ServingToken>,
    phases: PersistencePhaseClock,
    capture: Arc<PersistenceCapture>,
    finalized: AtomicBool,
    /// Optional live-session hub (set only when the debug UI is on). Finalize
    /// mirrors the terminal outcome onto it so the dashboard's active-session
    /// view updates in real time. A disabled hub is a no-op.
    session_hub: Option<crate::session_hub::SessionHub>,
}

impl PersistenceTerminalGuard {
    pub fn new(
        queue: PersistenceQueue,
        api_call_id: String,
        serving: Arc<crate::upstream::ServingToken>,
        capture: Arc<PersistenceCapture>,
    ) -> Self {
        capture.claim_engine();
        Self {
            queue,
            api_call_id,
            response_id: Mutex::new(None),
            serving,
            phases: PersistencePhaseClock::new_now(),
            capture,
            finalized: AtomicBool::new(false),
            session_hub: None,
        }
    }

    /// Attach the live-session hub (engine-side; only when the debug UI is on).
    pub fn with_session_hub(mut self, hub: crate::session_hub::SessionHub) -> Self {
        self.session_hub = Some(hub);
        self
    }

    /// Attach the hub ONLY when it is enabled (`--with-debug-ui`); a disabled
    /// hub stays `None` so finalize skips the emit entirely.
    pub fn with_session_hub_when_enabled(mut self, hub: &crate::session_hub::SessionHub) -> Self {
        if hub.is_enabled() {
            self.session_hub = Some(hub.clone());
        }
        self
    }

    pub fn phases(&self) -> PersistencePhaseClock {
        self.phases.clone()
    }

    pub fn capture(&self) -> Arc<PersistenceCapture> {
        Arc::clone(&self.capture)
    }

    /// Bind the wire response id once it is minted. First-write-wins, matching
    /// the FlowStore's response-id link semantics.
    pub fn set_response_id(&self, response_id: &str) {
        let mut slot = self
            .response_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(response_id.to_owned());
        }
    }

    /// Try to enqueue the terminal update. Returns `true` only for the caller
    /// that won finalization; queue saturation is deliberately not returned to
    /// inference (the queue's drop counters are the operational signal).
    pub fn finalize(
        &self,
        outcome: PersistenceOutcome,
        terminal_reason: Option<&str>,
        error: Option<&str>,
    ) -> bool {
        if self
            .finalized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }

        self.phases.stamp_finalize();
        let (route, provider) = self.serving.snapshot();
        let (model_served, usage) = self.serving.metrics_snapshot();
        let (attempts, first_upstream_byte_ms) = self.serving.attempts_snapshot();
        let phases = self.phases.snapshot();
        let metrics = TerminalMetricsInputs {
            model_served,
            endpoint: String::new(),
            upstream: provider.or(route),
            usage,
            prefill_ms: first_upstream_byte_ms
                .zip(phases.first_content_delta_ms)
                .map(|(start, end)| end.saturating_sub(start)),
            decode_ms: phases
                .first_content_delta_ms
                .zip(phases.stream_end_ms)
                .map(|(start, end)| end.saturating_sub(start)),
            attempts,
        };
        let response_id = self
            .response_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let finish = match finish_from_terminal(TerminalPersistenceInput {
            response_id: response_id.as_deref(),
            outcome,
            completed_at_ms: now_epoch_ms(),
            phases,
            first_upstream_byte_ms,
            terminal_reason,
            error,
            client_label: None,
            client_source: None,
            metrics: &metrics,
        }) {
            Ok(finish) => finish,
            Err(error) => {
                // The payload contains only JSON-safe Rust structs, so this is
                // defensive. Persistence must never turn observability into an
                // inference failure.
                tracing::warn!(
                    api_call_id = %self.api_call_id,
                    error = %error,
                    "failed to serialize terminal persistence metadata"
                );
                // The hub's stub must not stay `running` forever: mirror a
                // bare failed terminal even on this defensive path.
                if let Some(hub) = &self.session_hub {
                    hub.record_terminal(crate::session_hub::SessionTerminal {
                        api_call_id: &self.api_call_id,
                        status: "failed",
                        input_tokens: None,
                        output_tokens: None,
                        cached_tokens: None,
                        reasoning_tokens: None,
                        error: Some(error.to_string()),
                        terminal_reason: None,
                        completed_at_ms: now_epoch_ms() as u64,
                    });
                }
                return true;
            }
        };

        // The terminal result is the first point at which retry/failover and
        // multi-round tool execution have settled. Emit the staged upstream
        // pair here exactly once, immediately before the aggregate finish.
        self.capture.finish_upstream_hops();
        // `try_finish` is the only store interaction on this path. Full/closed
        // queues update their own bounded counters and return immediately.
        // Mirror the terminal outcome onto the live-session hub (no-op when
        // the debug UI is off or the request never passed the link seam); this
        // runs BEFORE the queue takes ownership of `finish`.
        if let Some(hub) = &self.session_hub {
            hub.record_terminal(crate::session_hub::SessionTerminal {
                api_call_id: &self.api_call_id,
                status: &finish.status,
                input_tokens: finish.input_tokens,
                output_tokens: finish.output_tokens,
                cached_tokens: finish.cached_tokens,
                reasoning_tokens: finish.reasoning_tokens,
                error: finish.error.clone(),
                terminal_reason: finish.terminal_reason.clone(),
                completed_at_ms: u64::try_from(finish.completed_at_ms).unwrap_or(u64::MAX),
            });
        }
        let _ = self.queue.try_finish(self.api_call_id.clone(), finish);
        true
    }
}

impl Drop for PersistenceTerminalGuard {
    fn drop(&mut self) {
        self.finalize(
            PersistenceOutcome::Cancelled,
            Some("dropped"),
            Some("request task ended before explicit persistence finalization"),
        );
    }
}

/// Values known at the HTTP ingress seam.
///
/// The actual backend and resolved model intentionally do not appear here. They
/// are unknown until routing/failover finishes and are stamped by
/// [`finish_from_terminal`] from the last served attempt.
#[derive(Debug, Clone, Copy)]
pub struct BeginPersistenceInput<'a> {
    pub api_call_id: &'a str,
    pub conversation_id: Option<&'a str>,
    pub virtual_key_id: Option<&'a str>,
    pub client_protocol: &'a str,
    pub client_model: &'a str,
    pub alias: Option<&'a str>,
    pub created_at_ms: u128,
    /// Detected harness identity; `None` when detection did not run.
    pub harness: Option<&'a crate::harness::HarnessIdentity>,
    /// Session-tree linkage; `None` when the body could not be split.
    pub link: Option<&'a crate::sessions::SessionLink>,
    pub client_label: Option<&'a str>,
    pub client_source: Option<&'a str>,
    /// Owner of the authenticating virtual key, when known.
    pub user_id: Option<&'a str>,
}

pub fn begin_request(input: BeginPersistenceInput<'_>) -> RequestRow {
    RequestRow {
        id: input.api_call_id.to_owned(),
        response_id: None,
        conversation_id: input.conversation_id.map(bounded_scalar),
        virtual_key_id: input.virtual_key_id.map(bounded_scalar),
        client_protocol: bounded_scalar(input.client_protocol),
        client_model: bounded_scalar(input.client_model),
        alias: input.alias.map(bounded_scalar),
        // Preselecting these at ingress was the old fork's attribution bug: a
        // nested fallback may be the provider/model that actually serves.
        backend: None,
        resolved_model: None,
        status: "running".to_owned(),
        created_at_ms: epoch_ms(input.created_at_ms),
        harness: input
            .harness
            .map(|identity| bounded_scalar(&identity.harness)),
        harness_version: input
            .harness
            .and_then(|identity| identity.version.as_deref())
            .map(bounded_scalar),
        harness_session_id: input
            .harness
            .and_then(|identity| identity.session_id.as_deref())
            .map(bounded_scalar),
        harness_sub_session_id: input
            .harness
            .and_then(|identity| identity.sub_session_id.as_deref())
            .map(bounded_scalar),
        harness_parent_session_id: input
            .harness
            .and_then(|identity| identity.parent_session_id.as_deref())
            .map(bounded_scalar),
        session_kind: input
            .harness
            .and_then(|identity| identity.session_kind.as_deref())
            .map(bounded_scalar),
        session_id: input.link.map(|link| link.session_id.clone()),
        chain_parent_request_id: input
            .link
            .and_then(|link| link.chain_parent_request_id.clone()),
        item_count: input
            .link
            .map(|link| i64::try_from(link.item_count).unwrap_or(i64::MAX)),
        shared_prefix_items: input
            .link
            .map(|link| i64::try_from(link.lineage.shared_prefix).unwrap_or(i64::MAX)),
        divergence_kind: input
            .link
            .map(|link| link.lineage.kind.as_str().to_string()),
        divergence_index: input
            .link
            .and_then(|link| link.lineage.index)
            .map(|index| i64::try_from(index).unwrap_or(i64::MAX)),
        cache_bust: input.link.map(|link| link.lineage.cache_bust),
        client_label: input.client_label.map(bounded_scalar),
        client_source: input.client_source.map(bounded_scalar),
        user_id: input.user_id.map(bounded_scalar),
    }
}

/// Canonical protocol label for one of the three instrumented inference routes.
pub const fn client_protocol_for_path(path: &str) -> Option<&'static str> {
    match path.as_bytes() {
        b"/v1/responses" => Some("responses"),
        b"/v1/chat/completions" => Some("chat_completions"),
        b"/v1/messages" => Some("anthropic_messages"),
        _ => None,
    }
}

/// Eviction-safe terminal values assembled at the engine's typed result seam.
///
/// `metrics.attempts` and `metrics.usage` come from the shared `ServingToken`, not
/// from a possibly evicted flow record. `phases` must likewise be a request-owned
/// terminal snapshot. `first_content_delta_ms` is client-facing TTFT; upstream
/// first-byte timing is retained separately in `timings_json`.
#[derive(Debug, Clone, Copy)]
pub struct TerminalPersistenceInput<'a> {
    pub response_id: Option<&'a str>,
    pub outcome: PersistenceOutcome,
    pub completed_at_ms: u128,
    pub phases: PhaseTimings,
    pub first_upstream_byte_ms: Option<u128>,
    pub terminal_reason: Option<&'a str>,
    pub error: Option<&'a str>,
    pub client_label: Option<&'a str>,
    pub client_source: Option<ClientSource>,
    pub metrics: &'a TerminalMetricsInputs,
}

#[derive(Serialize)]
struct PersistedTimings {
    #[serde(flatten)]
    phases: PhaseTimings,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_upstream_byte_ms: Option<u128>,
}

/// Build the single authoritative terminal update.
///
/// The winner is the *last* `Served` attempt. This matters for multi-round server
/// tool loops: `ServingToken.provider` is first-write-wins while the leaf-finalized
/// model is last-write-wins, so pairing those two convenience fields can produce a
/// provider/model combination that never existed. Failed attempts after an earlier
/// served tool round also cannot overwrite the last actual serving pair. With no
/// served attempt (pre-dispatch or all failed), both winner fields remain `None`.
pub fn finish_from_terminal(
    input: TerminalPersistenceInput<'_>,
) -> Result<RequestFinish, serde_json::Error> {
    let winner = winning_attempt(&input.metrics.attempts);
    let usage = input.metrics.usage;
    let attempts_json = serde_json::to_string(&input.metrics.attempts)?;
    let timings_json = serde_json::to_string(&PersistedTimings {
        phases: input.phases,
        first_upstream_byte_ms: input.first_upstream_byte_ms,
    })?;

    Ok(RequestFinish {
        response_id: input.response_id.map(str::to_owned),
        status: input.outcome.as_str().to_owned(),
        completed_at_ms: epoch_ms(input.completed_at_ms),
        first_token_at_ms: input.phases.first_content_delta_ms.map(epoch_ms),
        input_tokens: usage.map(|value| value.prompt),
        output_tokens: usage.map(|value| value.completion),
        cached_tokens: usage.and_then(|value| value.cached),
        reasoning_tokens: usage.and_then(|value| value.reasoning),
        error: if input.outcome.carries_error() {
            input.error.map(bounded_scalar)
        } else {
            None
        },
        terminal_reason: input.terminal_reason.map(bounded_scalar),
        backend: winner.and_then(|attempt| attempt.provider.clone()),
        resolved_model: winner.and_then(|attempt| attempt.model.clone()),
        // Always persist the measured terminal snapshots, including `[]`/`{}`.
        // `None` remains reserved for rows written before this adapter existed.
        attempts_json: Some(attempts_json),
        timings_json: Some(timings_json),
        client_label: input.client_label.map(bounded_scalar),
        client_source: input
            .client_source
            .map(client_source_name)
            .map(str::to_owned),
    })
}

/// Last attempt that actually produced a first chunk.
pub fn winning_attempt(attempts: &[Attempt]) -> Option<&Attempt> {
    attempts
        .iter()
        .rev()
        .find(|attempt| attempt.status == AttemptStatus::Served)
}

/// Convenience conversion for a record that is already retained by a caller.
///
/// Do not call `flow_store.detail()` at engine completion just to use this helper;
/// the bounded store can evict the record. The explicit `outcome` is required so
/// `Incomplete` is not collapsed into the dashboard's `Completed` status.
pub fn finish_from_flow_record(
    record: &FlowRecord,
    outcome: PersistenceOutcome,
    error: Option<&str>,
) -> Result<RequestFinish, serde_json::Error> {
    let metrics = TerminalMetricsInputs {
        model_served: record.model_served.clone(),
        endpoint: record.uri.clone(),
        upstream: record.upstream_target.clone(),
        usage: record.usage,
        prefill_ms: record
            .first_upstream_byte_ms
            .zip(record.phases.first_content_delta_ms)
            .map(|(start, end)| end.saturating_sub(start)),
        decode_ms: record
            .phases
            .first_content_delta_ms
            .zip(record.phases.stream_end_ms)
            .map(|(start, end)| end.saturating_sub(start)),
        attempts: record.attempts.clone(),
    };
    finish_from_terminal(TerminalPersistenceInput {
        response_id: record.response_id.as_deref(),
        outcome,
        completed_at_ms: record
            .finished_ms
            .or(record.phases.finalize_ms)
            .unwrap_or(record.started_ms),
        phases: record.phases,
        first_upstream_byte_ms: record.first_upstream_byte_ms,
        terminal_reason: record.terminal_reason.as_deref(),
        error: error.or(record.terminal_reason.as_deref()),
        client_label: record.client_label.as_deref(),
        client_source: record.client_source,
        metrics: &metrics,
    })
}

pub const fn client_source_name(source: ClientSource) -> &'static str {
    match source {
        ClientSource::KeyHash => "key_hash",
        ClientSource::ConfiguredHeader => "configured_header",
        ClientSource::UserAgent => "user_agent",
    }
}

fn epoch_ms(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn bounded_scalar(value: &str) -> String {
    if value.len() <= EVENT_SCALAR_CAP_BYTES {
        return value.to_owned();
    }
    let mut end = EVENT_SCALAR_CAP_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

/// The four authoritative body sections shared with durable turn capture.
///
/// Fixed sequence numbers make each section idempotent at the
/// `(request_id, seq)` database key and prevent per-attempt/per-delta duplicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadSection {
    InboundRequest,
    UpstreamRequest,
    UpstreamResponse,
    ServedResponse,
}

const OWNER_UNCLAIMED: u8 = 0;
const OWNER_ENGINE: u8 = 1;
const OWNER_MIDDLEWARE: u8 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum CaptureCompletion {
    #[default]
    Unknown,
    Complete,
    Partial,
}

#[derive(Debug)]
enum CapturedRequestBody {
    Split(SplitBody),
    Marker(Vec<u8>),
}

#[derive(Debug)]
struct CapturedRequest {
    body: CapturedRequestBody,
    original_bytes: u64,
    ts_ms: u128,
    partial: bool,
}

#[derive(Debug, Default)]
struct UpstreamPayloads {
    request: Option<CapturedRequest>,
    response: ModelOutputCapture,
    response_completion: CaptureCompletion,
}

/// Request-owned four-hop capture state shared by HTTP, engine, and leaf seams.
///
/// It has no registry and retains at most one capped request preview plus one
/// capped preview for each response direction. Retrying, failing over, or
/// entering another server-tool round synchronously replaces the upstream pair;
/// the engine terminal seam emits that final pair once with fixed sequence ids.
pub struct PersistenceCapture {
    queue: PersistenceQueue,
    api_call_id: String,
    keep_media: bool,
    capture_payloads: bool,
    owner: AtomicU8,
    upstream_emitted: AtomicBool,
    served_emitted: AtomicBool,
    upstream: Mutex<UpstreamPayloads>,
    served: Mutex<ModelOutputCapture>,
}

impl std::fmt::Debug for PersistenceCapture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PersistenceCapture")
            .field("api_call_id", &self.api_call_id)
            .finish_non_exhaustive()
    }
}

impl PersistenceCapture {
    pub fn new(queue: PersistenceQueue, api_call_id: impl Into<String>) -> Arc<Self> {
        Self::with_options(queue, api_call_id, true)
    }

    /// `keep_media` controls whether image/data URIs survive into stored items.
    pub fn with_options(
        queue: PersistenceQueue,
        api_call_id: impl Into<String>,
        keep_media: bool,
    ) -> Arc<Self> {
        Self::with_capture_options(queue, api_call_id, keep_media, true)
    }

    pub fn with_capture_options(
        queue: PersistenceQueue,
        api_call_id: impl Into<String>,
        keep_media: bool,
        capture_payloads: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            queue,
            api_call_id: api_call_id.into(),
            keep_media,
            capture_payloads,
            owner: AtomicU8::new(OWNER_UNCLAIMED),
            upstream_emitted: AtomicBool::new(false),
            served_emitted: AtomicBool::new(false),
            upstream: Mutex::new(UpstreamPayloads::default()),
            served: Mutex::new(ModelOutputCapture::new()),
        })
    }

    pub fn api_call_id(&self) -> &str {
        &self.api_call_id
    }

    /// Claim terminal ownership for the engine. The middleware fallback only
    /// wins when a JSON extractor/adapter rejects before this seam is reached.
    pub fn claim_engine(&self) {
        let _ = self.owner.compare_exchange(
            OWNER_UNCLAIMED,
            OWNER_ENGINE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Replace the staged on-wire request and reset the raw response for this
    /// send. The request is split into content-addressed items (secrets
    /// redacted per item) so the upstream hop shares storage with the inbound
    /// hop and with every other request in the same conversation.
    pub fn stage_upstream_request<T: Serialize>(&self, request: &T) {
        if !self.capture_payloads {
            return;
        }
        let captured = captured_request_value(request, self.keep_media);
        let mut upstream = self
            .upstream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upstream.request = Some(captured);
        upstream.response = ModelOutputCapture::new();
        upstream.response_completion = CaptureCompletion::Unknown;
    }

    /// Response headers prove the serialized request was accepted by the HTTP
    /// transport. A connect/timeout path retains `partial:true` because whether
    /// every request byte reached the peer is unknowable.
    pub fn mark_upstream_request_dispatched(&self) {
        let mut upstream = self
            .upstream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(request) = &mut upstream.request {
            request.partial = false;
        }
    }

    pub fn push_upstream_response(&self, bytes: &[u8]) {
        if !self.capture_payloads {
            return;
        }
        let mut upstream = self
            .upstream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upstream.response.push(bytes);
    }

    pub fn finish_upstream_response(&self, partial: bool) {
        let mut upstream = self
            .upstream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if partial {
            upstream.response_completion = CaptureCompletion::Partial;
        } else if upstream.response_completion != CaptureCompletion::Partial {
            upstream.response_completion = CaptureCompletion::Complete;
        }
    }

    /// A non-2xx body replaces any bytes from the superseded send within the
    /// same shrink/retry attempt. The transport reader is hard-capped, so
    /// `truncated` records that the retained prefix is not the whole provider
    /// body; `partial` is reserved for an actual body-stream read failure.
    pub fn stage_upstream_error_response(&self, body: &[u8], partial: bool, truncated: bool) {
        if !self.capture_payloads {
            return;
        }
        let mut capture = ModelOutputCapture::new();
        capture.push(body);
        if truncated {
            capture.mark_truncated();
        }
        let mut upstream = self
            .upstream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upstream.response = capture;
        upstream.response_completion = if partial {
            CaptureCompletion::Partial
        } else {
            CaptureCompletion::Complete
        };
    }

    pub fn push_served_response(&self, bytes: &[u8]) {
        if !self.capture_payloads {
            return;
        }
        if self.served_emitted.load(Ordering::Acquire) {
            return;
        }
        self.served
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(bytes);
    }

    /// Emit the actual client-facing bytes once. A body dropped before clean EOS
    /// is explicitly partial; queue pressure is recorded by the queue itself and
    /// never back-pressures the response stream.
    pub fn finish_served_response(&self, partial: bool) {
        if !self.capture_payloads {
            return;
        }
        if self.served_emitted.swap(true, Ordering::AcqRel) {
            return;
        }
        let capture = std::mem::take(
            &mut *self
                .served
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let event = capture.into_event(
            &self.api_call_id,
            PayloadSection::ServedResponse,
            now_epoch_ms(),
            partial,
        );
        let _ = self.queue.try_event(event);
    }

    /// Extractor/adapter fallback. Valid requests normally reach the engine,
    /// which claims ownership synchronously. If they do not, close the missing
    /// upstream hops as partial and terminate the durable row without waiting on
    /// the client body (the served tee still owns sequence 4).
    pub fn finish_unclaimed(&self, http_status: u16) {
        if self
            .owner
            .compare_exchange(
                OWNER_UNCLAIMED,
                OWNER_MIDDLEWARE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        self.finish_upstream_hops();
        let now = epoch_ms(now_epoch_ms());
        let finish = RequestFinish {
            status: "failed".to_owned(),
            completed_at_ms: now,
            terminal_reason: Some(format!("http_{http_status}")),
            error: Some("request was rejected before upstream dispatch".to_owned()),
            ..RequestFinish::default()
        };
        let _ = self.queue.try_finish(self.api_call_id.clone(), finish);
    }

    fn finish_upstream_hops(&self) {
        if self.upstream_emitted.swap(true, Ordering::AcqRel) {
            return;
        }
        if !self.capture_payloads {
            return;
        }
        let mut upstream = self
            .upstream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let request = upstream.request.take();
        let partial = upstream.response_completion != CaptureCompletion::Complete;
        let response = std::mem::take(&mut upstream.response).into_event(
            &self.api_call_id,
            PayloadSection::UpstreamResponse,
            now_epoch_ms(),
            partial,
        );
        drop(upstream);
        match request {
            None => {
                let _ = self.queue.try_event(payload_event(
                    &self.api_call_id,
                    PayloadSection::UpstreamRequest,
                    now_epoch_ms(),
                    0,
                    &[],
                    false,
                    true,
                    None,
                ));
            }
            Some(CapturedRequest {
                body: CapturedRequestBody::Split(split),
                original_bytes,
                ts_ms,
                partial,
            }) => {
                let _ = self.queue.try_body(body_write(
                    &self.api_call_id,
                    PayloadSection::UpstreamRequest,
                    ts_ms,
                    original_bytes,
                    split,
                    partial,
                    None,
                ));
            }
            Some(CapturedRequest {
                body: CapturedRequestBody::Marker(bytes),
                original_bytes,
                ts_ms,
                partial,
            }) => {
                let _ = self.queue.try_event(payload_event(
                    &self.api_call_id,
                    PayloadSection::UpstreamRequest,
                    ts_ms,
                    original_bytes,
                    &bytes,
                    true,
                    partial,
                    None,
                ));
            }
        }
        let _ = self.queue.try_event(response);
    }
}

/// Serialize the finalized upstream request and split it into items. The
/// upstream body is always a Chat Completions request. A serialization failure
/// stores a fixed marker containing none of the request bytes.
fn captured_request_value<T: Serialize>(request: &T, keep_media: bool) -> CapturedRequest {
    let ts_ms = now_epoch_ms();
    let serialized = serde_json::to_value(request);
    let Ok(value) = serialized else {
        return CapturedRequest {
            body: CapturedRequestBody::Marker(
                b"[redacted: failed to serialize upstream request]".to_vec(),
            ),
            original_bytes: 0,
            ts_ms,
            partial: true,
        };
    };
    // Measure the on-wire size before splitting moves the items out.
    let original_bytes = serialized_len(&value);
    match content_store::split_value(PROTOCOL_CHAT_COMPLETIONS, value, keep_media) {
        Ok(split) => CapturedRequest {
            body: CapturedRequestBody::Split(split),
            original_bytes,
            ts_ms,
            // Until response headers arrive, the transport cannot prove the
            // whole serialized request crossed the wire.
            partial: true,
        },
        Err(error) => CapturedRequest {
            body: CapturedRequestBody::Marker(
                format!("[redacted: upstream request not splittable: {error}]").into_bytes(),
            ),
            original_bytes,
            ts_ms,
            partial: true,
        },
    }
}

/// Byte length of a value's compact serialization, without retaining it.
fn serialized_len(value: &serde_json::Value) -> u64 {
    struct Counter(u64);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len() as u64);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    let _ = serde_json::to_writer(&mut counter, value);
    counter.0
}

/// Build the durable write for a split request-side hop: the skeleton event
/// (fixed sequence, envelope contract unchanged) plus item and blob rows.
pub fn body_write(
    api_call_id: &str,
    section: PayloadSection,
    ts_ms: u128,
    original_bytes: u64,
    split: SplitBody,
    partial: bool,
    headers: Option<&axum::http::HeaderMap>,
) -> BodyWrite {
    debug_assert!(matches!(
        section,
        PayloadSection::InboundRequest | PayloadSection::UpstreamRequest
    ));
    let created_at_ms = i64::try_from(ts_ms).unwrap_or(i64::MAX);
    let mut seen = std::collections::HashSet::new();
    let mut blobs = Vec::new();
    let mut items = Vec::with_capacity(split.items.len());
    for item in split.items {
        if seen.insert(item.hash.clone()) {
            blobs.push(BlobRow {
                hash: item.hash.clone(),
                media: content_store::MEDIA_JSON.to_string(),
                size: i64::try_from(item.canonical.len()).unwrap_or(i64::MAX),
                content: item.canonical,
                created_at_ms,
            });
        }
        items.push(ItemRow {
            request_id: api_call_id.to_string(),
            hop: section.hop().to_string(),
            ordinal: item.ordinal,
            section: item.section.as_str().to_string(),
            kind: item.kind,
            blob_hash: item.hash,
            identity_hash: Some(item.identity),
        });
    }
    let mut event = payload_event(
        api_call_id,
        section,
        ts_ms,
        original_bytes,
        split.skeleton.as_bytes(),
        false,
        partial,
        headers,
    );
    // Record the item count in the envelope so a reader knows the payload is a
    // skeleton even before it looks for references.
    if let Some(payload) = event.payload.as_mut()
        && let Ok(mut envelope) = serde_json::from_str::<serde_json::Value>(payload)
        && let Some(object) = envelope.as_object_mut()
    {
        object.insert("items".to_string(), serde_json::json!(items.len()));
        if let Ok(rendered) = serde_json::to_string(&envelope) {
            *payload = rendered;
        }
    }
    BodyWrite {
        event,
        items,
        blobs,
    }
}

impl PayloadSection {
    pub const fn seq(self) -> i64 {
        match self {
            Self::InboundRequest => 1,
            Self::UpstreamRequest => 2,
            Self::UpstreamResponse => 3,
            Self::ServedResponse => 4,
        }
    }

    pub const fn hop(self) -> &'static str {
        match self {
            Self::InboundRequest => "client_in",
            Self::UpstreamRequest => "upstream_out",
            Self::UpstreamResponse => "upstream_in",
            Self::ServedResponse => "client_out",
        }
    }

    pub const fn kind(self) -> &'static str {
        match self {
            Self::InboundRequest | Self::UpstreamRequest => "request",
            Self::UpstreamResponse | Self::ServedResponse => "response",
        }
    }
}

#[derive(Serialize)]
struct EventPayloadEnvelope<'a> {
    original_bytes: u64,
    captured_bytes: usize,
    truncated: bool,
    partial: bool,
    encoding: &'static str,
    content: &'a str,
    /// Client request headers on the `client_in` hop: redacted (credentials
    /// become `[redacted]`), each value capped, and the set bounded. Absent on
    /// upstream hops.
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Upper bound on captured request headers per event.
const EVENT_HEADERS_MAX: usize = 64;

/// The client request headers as a redacted, bounded JSON object (repeated
/// names are joined with `, `, as HTTP allows).
fn captured_headers(headers: &axum::http::HeaderMap) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (name, value) in crate::redaction::redact_headers_capped(headers, EVENT_SCALAR_CAP_BYTES) {
        match map.get_mut(&name) {
            Some(serde_json::Value::String(existing)) => {
                existing.push_str(", ");
                existing.push_str(&value);
            }
            _ => {
                if map.len() >= EVENT_HEADERS_MAX {
                    break;
                }
                map.insert(name, serde_json::Value::String(value));
            }
        }
    }
    map
}

/// Build a request-side hop event with the shared secret/image redactor.
///
/// This is safe for the raw inbound request and safe (idempotently) for the
/// already-redacted final upstream-request section. It scans the source without
/// retaining it and owns at most `EVENT_PAYLOAD_CAP_BYTES` of captured body.
pub fn request_payload_event(
    api_call_id: &str,
    section: PayloadSection,
    ts_ms: u128,
    raw: &[u8],
    partial: bool,
) -> EventRow {
    request_payload_event_with_headers(api_call_id, section, ts_ms, raw, partial, None)
}

/// [`request_payload_event`] that also records the client request headers.
pub fn request_payload_event_with_headers(
    api_call_id: &str,
    section: PayloadSection,
    ts_ms: u128,
    raw: &[u8],
    partial: bool,
    headers: Option<&axum::http::HeaderMap>,
) -> EventRow {
    debug_assert!(matches!(
        section,
        PayloadSection::InboundRequest | PayloadSection::UpstreamRequest
    ));
    let captured = crate::redaction::capture_capped_redacted(
        raw,
        EVENT_PAYLOAD_CAP_BYTES,
        EVENT_SCALAR_CAP_BYTES,
    );
    payload_event(
        api_call_id,
        section,
        ts_ms,
        raw.len() as u64,
        &captured,
        raw.len() > EVENT_PAYLOAD_CAP_BYTES,
        partial,
        headers,
    )
}

/// Build an inbound event from bytes already copied and redacted by the HTTP
/// layer's shared offload. This avoids a second multi-megabyte JSON scan on the
/// Tokio worker while preserving the same fixed sequence/envelope contract.
pub fn redacted_request_payload_event(
    api_call_id: &str,
    section: PayloadSection,
    ts_ms: u128,
    original_bytes: usize,
    redacted: &[u8],
    partial: bool,
    headers: Option<&axum::http::HeaderMap>,
) -> EventRow {
    debug_assert!(matches!(
        section,
        PayloadSection::InboundRequest | PayloadSection::UpstreamRequest
    ));
    let captured_len = redacted.len().min(EVENT_PAYLOAD_CAP_BYTES);
    payload_event(
        api_call_id,
        section,
        ts_ms,
        u64::try_from(original_bytes).unwrap_or(u64::MAX),
        &redacted[..captured_len],
        redacted.len() > captured_len || original_bytes > EVENT_PAYLOAD_CAP_BYTES,
        partial,
        headers,
    )
}

/// Bounded incremental collector for raw upstream or client-served response bytes.
///
/// `push` is O(chunk) but copies only the still-available prefix. The total byte
/// counter continues after truncation, so a persisted preview never pretends to be
/// the complete response. Create one per response section and emit it once at the
/// existing final-section barrier, not once per SSE delta.
#[derive(Debug, Default)]
pub struct ModelOutputCapture {
    bytes: Vec<u8>,
    original_bytes: u64,
}

impl ModelOutputCapture {
    pub fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(EVENT_PAYLOAD_CAP_BYTES.min(8 * 1024)),
            original_bytes: 0,
        }
    }

    pub fn push(&mut self, chunk: &[u8]) {
        self.original_bytes = self
            .original_bytes
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        let remaining = EVENT_PAYLOAD_CAP_BYTES.saturating_sub(self.bytes.len());
        self.bytes
            .extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }

    /// Record a known truncation when the producer itself stopped at an
    /// upstream boundary cap. The exact remote size is unknowable after the
    /// stream is dropped, so retain the smallest truthful lower bound.
    fn mark_truncated(&mut self) {
        self.original_bytes = self.original_bytes.max(
            u64::try_from(self.bytes.len())
                .unwrap_or(u64::MAX)
                .saturating_add(1),
        );
    }

    pub fn into_event(
        self,
        api_call_id: &str,
        section: PayloadSection,
        ts_ms: u128,
        partial: bool,
    ) -> EventRow {
        debug_assert!(matches!(
            section,
            PayloadSection::UpstreamResponse | PayloadSection::ServedResponse
        ));
        let truncated = self.original_bytes > self.bytes.len() as u64;
        let bytes = redact_model_output(&self.bytes);
        payload_event(
            api_call_id,
            section,
            ts_ms,
            self.original_bytes,
            &bytes,
            truncated || bytes.len() >= EVENT_PAYLOAD_CAP_BYTES,
            partial,
            None,
        )
    }
}

/// Preserve JSON/SSE structure while removing sensitive JSON keys and every
/// image/data URI. Unknown plaintext is replaced wholesale: guessing which
/// substring is a credential is less safe than retaining an explicit marker.
fn redact_model_output(raw: &[u8]) -> Vec<u8> {
    if serde_json::from_slice::<serde_json::Value>(raw).is_ok() {
        return crate::redaction::capture_capped_redacted(
            raw,
            EVENT_PAYLOAD_CAP_BYTES,
            EVENT_SCALAR_CAP_BYTES,
        );
    }
    let Ok(text) = std::str::from_utf8(raw) else {
        return format!("[redacted: non-UTF8 model output {} bytes]", raw.len()).into_bytes();
    };
    if !text.lines().any(|line| line.starts_with("data:")) {
        return format!("[redacted: unstructured model output {} bytes]", raw.len()).into_bytes();
    }

    let mut output = Vec::with_capacity(raw.len().min(EVENT_PAYLOAD_CAP_BYTES));
    for line in text.split_inclusive('\n') {
        if output.len() >= EVENT_PAYLOAD_CAP_BYTES {
            break;
        }
        let (content, ending) = line.strip_suffix("\r\n").map_or_else(
            || {
                line.strip_suffix('\n')
                    .map_or((line, ""), |line| (line, "\n"))
            },
            |line| (line, "\r\n"),
        );
        let safe = if let Some(data) = content.strip_prefix("data:") {
            let data = data.trim_start();
            if data == "[DONE]" || data.is_empty() {
                content.to_owned()
            } else {
                let redacted = crate::redaction::capture_capped_redacted(
                    data.as_bytes(),
                    EVENT_PAYLOAD_CAP_BYTES,
                    EVENT_SCALAR_CAP_BYTES,
                );
                format!("data: {}", String::from_utf8_lossy(&redacted))
            }
        } else if let Some(event) = content.strip_prefix("event:") {
            let event = event.trim();
            if event.len() <= 128
                && event
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
            {
                format!("event: {event}")
            } else {
                "event: [redacted]".to_owned()
            }
        } else if content.is_empty() {
            String::new()
        } else if content.starts_with(':') {
            ": [redacted comment]".to_owned()
        } else {
            format!("[redacted: non-SSE output line {} bytes]", content.len())
        };
        extend_capped(&mut output, safe.as_bytes());
        extend_capped(&mut output, ending.as_bytes());
    }
    output
}

fn extend_capped(output: &mut Vec<u8>, bytes: &[u8]) {
    let remaining = EVENT_PAYLOAD_CAP_BYTES.saturating_sub(output.len());
    output.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
}

#[allow(clippy::too_many_arguments)]
fn payload_event(
    api_call_id: &str,
    section: PayloadSection,
    ts_ms: u128,
    original_bytes: u64,
    captured: &[u8],
    truncated: bool,
    partial: bool,
    headers: Option<&axum::http::HeaderMap>,
) -> EventRow {
    let (encoding, content) = match std::str::from_utf8(captured) {
        Ok(text) => ("utf8", text.to_owned()),
        Err(_) => (
            "base64",
            base64::engine::general_purpose::STANDARD.encode(captured),
        ),
    };
    let payload = serde_json::to_string(&EventPayloadEnvelope {
        original_bytes,
        captured_bytes: captured.len(),
        truncated,
        partial,
        encoding,
        content: &content,
        headers: headers.map(captured_headers),
    })
    .expect("serializing a bounded event payload cannot fail");

    EventRow {
        request_id: api_call_id.to_owned(),
        seq: section.seq(),
        ts_ms: epoch_ms(ts_ms),
        hop: section.hop().to_owned(),
        kind: section.kind().to_owned(),
        payload: Some(payload),
        bytes: Some(i64::try_from(original_bytes).unwrap_or(i64::MAX)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_store::{PersistenceWriter, StoreResult};
    use crate::dashboard_flow::{AttemptErrorClass, AttemptFailoverReason, FlowUsage};
    use async_trait::async_trait;
    use std::num::NonZeroUsize;

    #[derive(Default)]
    struct RecordingWriter {
        finishes: Mutex<Vec<(String, RequestFinish)>>,
        events: Mutex<Vec<EventRow>>,
    }

    #[async_trait]
    impl PersistenceWriter for RecordingWriter {
        async fn begin_request(&self, _row: RequestRow) -> StoreResult<()> {
            Ok(())
        }

        async fn append_event(&self, event: EventRow) -> StoreResult<()> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }

        async fn finish_request(&self, id: &str, finish: RequestFinish) -> StoreResult<()> {
            self.finishes.lock().unwrap().push((id.to_owned(), finish));
            Ok(())
        }
    }

    #[tokio::test]
    async fn metadata_only_capture_emits_no_payload_events() {
        let writer = Arc::new(RecordingWriter::default());
        let queue = PersistenceQueue::spawn(
            Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
            NonZeroUsize::new(8).unwrap(),
        );
        let capture = PersistenceCapture::with_capture_options(
            queue.clone(),
            "api_metadata_only",
            false,
            false,
        );
        capture.stage_upstream_request(&serde_json::json!({"messages":[{"content":"secret"}]}));
        capture.push_upstream_response(b"provider output");
        capture.finish_upstream_response(false);
        capture.push_served_response(b"client output");
        capture.finish_served_response(false);
        capture.finish_unclaimed(400);
        queue.flush().await.unwrap();

        assert!(writer.events.lock().unwrap().is_empty());
        assert_eq!(writer.finishes.lock().unwrap().len(), 1);
    }

    fn attempt(provider: &str, model: &str, status: AttemptStatus, start_ms: u128) -> Attempt {
        Attempt {
            provider: Some(provider.to_owned()),
            model: Some(model.to_owned()),
            start_ms,
            end_ms: start_ms + 10,
            first_upstream_byte_ms: (status == AttemptStatus::Served).then_some(start_ms + 2),
            status,
            error_class: (status == AttemptStatus::Failed).then_some(AttemptErrorClass::HttpStatus),
            failover_reason: (status == AttemptStatus::Failed)
                .then_some(AttemptFailoverReason::ProviderFailed),
        }
    }

    #[test]
    fn terminal_uses_last_served_attempt_and_content_ttft() {
        let metrics = TerminalMetricsInputs {
            // These deliberately form a mismatched convenience pair. The attempt
            // trace is the authority for the actual provider/model pair.
            model_served: Some("legacy-last-model".to_owned()),
            endpoint: "/v1/responses".to_owned(),
            upstream: Some("legacy-first-provider".to_owned()),
            usage: Some(FlowUsage {
                prompt: 11,
                completion: 7,
                total: 18,
                cached: Some(3),
                reasoning: Some(2),
            }),
            prefill_ms: Some(38),
            decode_ms: Some(20),
            attempts: vec![
                attempt("a", "model-a", AttemptStatus::Failed, 1_010),
                attempt("b", "model-b", AttemptStatus::Served, 1_030),
                attempt("c", "model-c", AttemptStatus::Served, 1_050),
            ],
        };
        let phases = PhaseTimings {
            ingress_ms: Some(1_000),
            normalization_done_ms: Some(1_005),
            routing_decision_ms: Some(1_008),
            first_content_delta_ms: Some(1_070),
            stream_end_ms: Some(1_090),
            finalize_ms: Some(1_100),
        };

        let finish = finish_from_terminal(TerminalPersistenceInput {
            response_id: Some("resp_1"),
            outcome: PersistenceOutcome::Incomplete,
            completed_at_ms: 1_100,
            phases,
            first_upstream_byte_ms: Some(1_032),
            terminal_reason: Some("response.incomplete"),
            error: None,
            client_label: Some("key-deadbeef"),
            client_source: Some(ClientSource::KeyHash),
            metrics: &metrics,
        })
        .expect("terminal adapter");

        assert_eq!(finish.status, "incomplete");
        assert_eq!(finish.backend.as_deref(), Some("c"));
        assert_eq!(finish.resolved_model.as_deref(), Some("model-c"));
        assert_eq!(finish.first_token_at_ms, Some(1_070));
        assert_eq!(finish.cached_tokens, Some(3));
        assert_eq!(finish.reasoning_tokens, Some(2));
        assert_eq!(finish.client_source.as_deref(), Some("key_hash"));
        let timings: serde_json::Value =
            serde_json::from_str(finish.timings_json.as_deref().unwrap()).unwrap();
        assert_eq!(timings["first_upstream_byte_ms"], 1_032);
        assert_eq!(timings["first_content_delta_ms"], 1_070);
    }

    #[test]
    fn all_failed_attempts_do_not_fabricate_a_winner() {
        let metrics = TerminalMetricsInputs {
            model_served: Some("configured-fallback-is-not-a-winner".to_owned()),
            upstream: Some("configured-primary-is-not-a-winner".to_owned()),
            attempts: vec![
                attempt("a", "model-a", AttemptStatus::Failed, 10),
                attempt("b", "model-b", AttemptStatus::Failed, 30),
            ],
            ..Default::default()
        };
        let finish = finish_from_terminal(TerminalPersistenceInput {
            response_id: None,
            outcome: PersistenceOutcome::Failed,
            completed_at_ms: 50,
            phases: PhaseTimings::default(),
            first_upstream_byte_ms: None,
            terminal_reason: Some("upstream_failed"),
            error: Some("safe error"),
            client_label: None,
            client_source: None,
            metrics: &metrics,
        })
        .unwrap();

        assert_eq!(finish.backend, None);
        assert_eq!(finish.resolved_model, None);
        assert_eq!(finish.error.as_deref(), Some("safe error"));
        assert_eq!(finish.first_token_at_ms, None);
    }

    #[test]
    fn begin_never_preselects_a_backend() {
        let row = begin_request(BeginPersistenceInput {
            api_call_id: "api_1",
            conversation_id: Some("conversation-1"),
            virtual_key_id: Some("key-1"),
            client_protocol: "responses",
            client_model: "small",
            alias: Some("small"),
            created_at_ms: 123,
            harness: Some(&crate::harness::HarnessIdentity {
                harness: "claude-code".to_string(),
                version: Some("2.1.0".to_string()),
                session_id: Some("s-1".to_string()),
                sub_session_id: Some("agent-1".to_string()),
                parent_session_id: None,
                session_kind: None,
                sub_sessions: crate::harness::SubSessionPolicy::Declared,
            }),
            link: Some(&crate::sessions::SessionLink {
                session_id: "node-1".to_string(),
                chain_parent_request_id: Some("api_0".to_string()),
                lineage: crate::sessions::Lineage {
                    kind: crate::sessions::DivergenceKind::ToolsChanged,
                    shared_prefix: 3,
                    index: Some(3),
                    cache_bust: true,
                },
                item_count: 7,
                upserts: Vec::new(),
            }),
            client_label: Some("key-abc"),
            client_source: Some("key_hash"),
            user_id: Some("user-1"),
        });
        assert_eq!(row.user_id.as_deref(), Some("user-1"));
        assert_eq!(row.session_id.as_deref(), Some("node-1"));
        assert_eq!(row.chain_parent_request_id.as_deref(), Some("api_0"));
        assert_eq!(row.divergence_kind.as_deref(), Some("tools_changed"));
        assert_eq!(row.divergence_index, Some(3));
        assert_eq!(row.shared_prefix_items, Some(3));
        assert_eq!(row.item_count, Some(7));
        assert_eq!(row.cache_bust, Some(true));
        assert_eq!(row.client_label.as_deref(), Some("key-abc"));
        assert_eq!(row.id, "api_1");
        assert_eq!(row.harness.as_deref(), Some("claude-code"));
        assert_eq!(row.harness_version.as_deref(), Some("2.1.0"));
        assert_eq!(row.harness_session_id.as_deref(), Some("s-1"));
        assert_eq!(row.harness_sub_session_id.as_deref(), Some("agent-1"));
        assert_eq!(row.harness_parent_session_id, None);
        assert_eq!(row.response_id, None);
        assert_eq!(row.backend, None);
        assert_eq!(row.resolved_model, None);
    }

    #[test]
    fn persistence_phase_clock_is_first_write_wins_and_causally_clamped() {
        let clock = PersistencePhaseClock::new(1_000);
        clock.stamp(Phase::Normalization, 900);
        clock.stamp(Phase::Normalization, 2_000);
        clock.stamp(Phase::Routing, 950);
        clock.stamp(Phase::FirstContent, 1_100);
        clock.stamp(Phase::StreamEnd, 1_050);
        clock.stamp(Phase::Finalize, 1_025);

        let phases = clock.snapshot();
        assert_eq!(phases.normalization_done_ms, Some(1_000));
        assert_eq!(phases.routing_decision_ms, Some(1_000));
        assert_eq!(phases.first_content_delta_ms, Some(1_100));
        assert_eq!(phases.stream_end_ms, Some(1_100));
        assert_eq!(phases.finalize_ms, Some(1_100));
    }

    #[tokio::test]
    async fn terminal_guard_is_exactly_once_without_a_flow_store() {
        let writer = Arc::new(RecordingWriter::default());
        let queue = PersistenceQueue::spawn(
            Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
            NonZeroUsize::new(8).unwrap(),
        );
        let serving = Arc::new(crate::upstream::ServingToken::default());
        serving.record_attempt(attempt("primary", "model-a", AttemptStatus::Failed, 10));
        serving.record_attempt(attempt("fallback", "model-b", AttemptStatus::Served, 30));
        serving.set_usage(FlowUsage {
            prompt: 13,
            completion: 5,
            total: 18,
            cached: Some(2),
            reasoning: None,
        });

        // No DashboardFlowStore is constructed or read: all terminal facts are
        // request-owned or come from the shared ServingToken.
        let guard = PersistenceTerminalGuard::new(
            queue.clone(),
            "api_1".to_owned(),
            serving,
            PersistenceCapture::new(queue.clone(), "api_1"),
        );
        guard.set_response_id("resp_1");
        guard.phases().stamp_first_content_delta();
        assert!(guard.finalize(
            PersistenceOutcome::Completed,
            Some("response.completed"),
            None,
        ));
        assert!(!guard.finalize(
            PersistenceOutcome::Failed,
            Some("must_not_overwrite"),
            Some("must_not_duplicate"),
        ));
        drop(guard); // Drop fallback also loses the same atomic claim.
        queue.flush().await.unwrap();

        let finishes = writer.finishes.lock().unwrap();
        assert_eq!(finishes.len(), 1);
        let (id, finish) = &finishes[0];
        assert_eq!(id, "api_1");
        assert_eq!(finish.response_id.as_deref(), Some("resp_1"));
        assert_eq!(finish.status, "completed");
        assert_eq!(finish.backend.as_deref(), Some("fallback"));
        assert_eq!(finish.resolved_model.as_deref(), Some("model-b"));
        assert_eq!(finish.input_tokens, Some(13));
        assert_eq!(finish.output_tokens, Some(5));
        assert!(finish.first_token_at_ms.is_some());
    }

    #[tokio::test]
    async fn terminal_guard_drop_persists_one_cancelled_fallback() {
        let writer = Arc::new(RecordingWriter::default());
        let queue = PersistenceQueue::spawn(
            Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
            NonZeroUsize::new(8).unwrap(),
        );
        let guard = PersistenceTerminalGuard::new(
            queue.clone(),
            "api_drop".to_owned(),
            Arc::new(crate::upstream::ServingToken::default()),
            PersistenceCapture::new(queue.clone(), "api_drop"),
        );
        guard.set_response_id("resp_drop");
        drop(guard);
        queue.flush().await.unwrap();

        let finishes = writer.finishes.lock().unwrap();
        assert_eq!(finishes.len(), 1);
        assert_eq!(finishes[0].1.status, "cancelled");
        assert_eq!(finishes[0].1.terminal_reason.as_deref(), Some("dropped"));
    }

    #[test]
    fn inbound_events_keep_redacted_request_headers() {
        // The client hop records the request headers (harness detection and
        // session attribution depend on them), with credentials redacted.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "user-agent",
            "codex_exec/0.154.0 (Debian 13.0.0; x86_64)"
                .parse()
                .unwrap(),
        );
        headers.insert("authorization", "Bearer llmc_secret".parse().unwrap());
        headers.insert("x-api-key", "sk-secret".parse().unwrap());
        headers.insert("cookie", "llmconduit_session=abc".parse().unwrap());
        headers.insert("x-llm-session-id", "s-1".parse().unwrap());
        let split = crate::content_store::split_value(
            crate::flow_persistence::PROTOCOL_CHAT_COMPLETIONS,
            serde_json::json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]}),
            true,
        )
        .unwrap();
        let write = body_write(
            "api-call-3",
            PayloadSection::InboundRequest,
            5,
            10,
            split,
            false,
            Some(&headers),
        );
        let envelope: serde_json::Value =
            serde_json::from_str(write.event.payload.as_deref().unwrap()).unwrap();
        let captured = &envelope["headers"];
        assert_eq!(
            captured["user-agent"],
            "codex_exec/0.154.0 (Debian 13.0.0; x86_64)"
        );
        assert_eq!(captured["x-llm-session-id"], "s-1");
        assert_eq!(captured["authorization"], "[redacted]");
        assert_eq!(captured["x-api-key"], "[redacted]");
        assert_eq!(captured["cookie"], "[redacted]");
        assert!(!envelope.to_string().contains("secret"));

        let event = redacted_request_payload_event(
            "api-call-4",
            PayloadSection::InboundRequest,
            5,
            2,
            b"{}",
            false,
            Some(&headers),
        );
        let envelope: serde_json::Value =
            serde_json::from_str(event.payload.as_deref().unwrap()).unwrap();
        assert_eq!(envelope["headers"]["authorization"], "[redacted]");
        // Upstream hops carry no client headers.
        let none = redacted_request_payload_event(
            "api-call-5",
            PayloadSection::UpstreamRequest,
            5,
            2,
            b"{}",
            false,
            None,
        );
        let envelope: serde_json::Value =
            serde_json::from_str(none.payload.as_deref().unwrap()).unwrap();
        assert!(envelope.get("headers").is_none());
    }

    #[test]
    fn payload_events_are_redacted_bounded_and_fixed_sequence() {
        let secret = "never-persist-me";
        let raw = format!(
            r#"{{"api_key":"{secret}","prompt":"{}"}}"#,
            "x".repeat(EVENT_PAYLOAD_CAP_BYTES * 2)
        );
        let request = request_payload_event(
            "api_1",
            PayloadSection::InboundRequest,
            1,
            raw.as_bytes(),
            false,
        );
        assert_eq!(request.seq, 1);
        assert_eq!(request.hop, "client_in");
        assert!(request.bytes.unwrap() > EVENT_PAYLOAD_CAP_BYTES as i64);
        let payload = request.payload.unwrap();
        assert!(!payload.contains(secret));
        let envelope: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(envelope["truncated"], true);
        assert!(envelope["captured_bytes"].as_u64().unwrap() <= EVENT_PAYLOAD_CAP_BYTES as u64);

        let mut response = ModelOutputCapture::new();
        response.push(&vec![0xff; EVENT_PAYLOAD_CAP_BYTES + 7]);
        let response = response.into_event("api_1", PayloadSection::ServedResponse, 2, true);
        assert_eq!(response.seq, 4);
        let envelope: serde_json::Value =
            serde_json::from_str(response.payload.as_deref().unwrap()).unwrap();
        assert_eq!(envelope["encoding"], "utf8");
        assert_eq!(
            envelope["content"],
            format!(
                "[redacted: non-UTF8 model output {} bytes]",
                EVENT_PAYLOAD_CAP_BYTES
            )
        );
        assert_eq!(envelope["truncated"], true);
        assert_eq!(envelope["partial"], true);
    }

    #[test]
    fn upstream_request_capture_splits_items_and_redacts_secrets() {
        let request = serde_json::json!({
            "model": "served-model",
            "api_key": "sk-upstream-secret",
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "hi", "x-api-key": "sk-inline"}
            ],
            "tools": [{"type": "function", "function": {"name": "f", "parameters": {}}}]
        });
        let captured = captured_request_value(&request, true);
        assert!(
            captured.partial,
            "partial until the transport proves dispatch"
        );
        assert_eq!(
            captured.original_bytes,
            serde_json::to_vec(&request).unwrap().len() as u64
        );
        let CapturedRequestBody::Split(split) = captured.body else {
            panic!("upstream request must split into items");
        };
        assert_eq!(split.items.len(), 3);
        assert!(!split.skeleton.contains("sk-upstream-secret"));
        assert!(
            split
                .items
                .iter()
                .all(|item| !item.canonical.contains("sk-inline"))
        );

        let write = body_write(
            "api-call-1",
            PayloadSection::UpstreamRequest,
            1_000,
            captured.original_bytes,
            split,
            captured.partial,
            None,
        );
        assert_eq!(write.event.seq, PayloadSection::UpstreamRequest.seq());
        assert_eq!(write.event.hop, "upstream_out");
        assert_eq!(write.items.len(), 3);
        assert_eq!(write.blobs.len(), 3);
        assert!(write.items.iter().all(|item| item.hop == "upstream_out"));
        let envelope: serde_json::Value =
            serde_json::from_str(write.event.payload.as_deref().unwrap()).unwrap();
        assert_eq!(envelope["items"], 3);
        assert_eq!(envelope["truncated"], false);
        assert_eq!(envelope["partial"], true);
    }

    #[test]
    fn upstream_request_capture_serialization_failure_stores_only_a_marker() {
        struct Broken;
        impl Serialize for Broken {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("nope"))
            }
        }
        let captured = captured_request_value(&Broken, true);
        let CapturedRequestBody::Marker(bytes) = captured.body else {
            panic!("a failed serialization must not produce items");
        };
        assert_eq!(bytes, b"[redacted: failed to serialize upstream request]");
        assert!(captured.partial);
    }

    #[test]
    fn body_write_stores_each_distinct_item_once() {
        let request = serde_json::json!({
            "model": "m",
            "messages": [
                {"role": "user", "content": "same"},
                {"role": "assistant", "content": "reply"},
                {"role": "user", "content": "same"}
            ]
        });
        let split = crate::content_store::split_value(
            crate::content_store::PROTOCOL_CHAT_COMPLETIONS,
            request,
            true,
        )
        .unwrap();
        let write = body_write(
            "api-call-2",
            PayloadSection::InboundRequest,
            5,
            10,
            split,
            false,
            None,
        );
        assert_eq!(write.items.len(), 3);
        assert_eq!(write.blobs.len(), 2, "duplicate item shares one blob");
        assert_eq!(write.items[0].blob_hash, write.items[2].blob_hash);
        assert_eq!(write.items[0].ordinal, 0);
        assert_eq!(write.items[2].ordinal, 2);
        assert!(write.blobs.iter().all(|blob| blob.created_at_ms == 5));
    }
}
