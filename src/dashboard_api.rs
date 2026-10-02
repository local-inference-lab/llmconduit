//! D13 — the `/dashboard/api/*` REST surface: the capstone that makes Phase 0's
//! stores (D1 FlowStore, D4 topology, D5 metrics/snapshots, D6 kill) reachable by
//! the SPA. Every handler takes `State(Arc<Gateway>)`; the routes register ONLY in
//! the `--with-debug-ui` block (http.rs), behind D7a's session auth + `no_store`.
//!
//! ## Wire contract (FROZEN — `dashboard-frontend/src/api/types.ts`)
//! The JSON these handlers emit must match the SPA's runtime validators
//! byte-for-byte (field names, nesting, per-domain `seq` cursors). The cursor-
//! bearing reads (`/flows`, `/flows/:id`, `/metrics`, `/topology`, `/snapshot`)
//! each carry their OWN domain's sequence — never a single global watermark
//! (AGENTS.md per-domain `{domain, seq}` rule). `/catalog` is the lone BARE array
//! (a static-ish read, not a mutating domain).
//!
//! ## Shape reuse (REST == WS)
//! `/metrics` returns a [`crate::dashboard_ws::MetricsSnapshot`] and `/topology` a
//! [`crate::dashboard_ws::TopologySnapshot`] — the SAME structs the `/dashboard/ws`
//! initial snapshot ships, so the REST body and the WS snapshot body are identical
//! shapes (the SPA decodes both with one validator). The flow rows + detail add a
//! `cost` roll-up the body-free [`SnapshotFlowSummary`] does not carry, so this
//! module defines the cost-bearing [`FlowRow`]/[`FlowDetailBody`] projections.
//!
//! ## Rates + cost (D13's job, not D5's)
//! The WS `window_tile` ships RAW window counts in the rate fields and `0.0` cost
//! (it has no window-seconds or price table). D13's REST view divides by the true
//! window seconds and prices every bucket via [`crate::config::Config::price_for`],
//! so `reqs_per_sec`/`tokens_per_sec`/`cost_per_min`/`cost_per_sec` are real rates.
//! `active_streams` is the live count of OPEN flows (the metrics rings don't track
//! liveness; the FlowStore does).

use crate::dashboard_auth::{AuthSession, DashboardAuth, MutationPolicy};
use crate::dashboard_flow::Attempt;
use crate::dashboard_flow::ClientSource;
use crate::dashboard_flow::FlowRecord;
use crate::dashboard_flow::FlowStatus;
use crate::dashboard_flow::FlowUsage;
use crate::dashboard_flow::PhaseTimings;
use crate::dashboard_ws::MetricWindow;
use crate::dashboard_ws::MetricWindows;
use crate::dashboard_ws::MetricsSnapshot;
use crate::dashboard_ws::ModelPrice;
use crate::dashboard_ws::SeqCursors;
use crate::dashboard_ws::TopologyEdge;
use crate::dashboard_ws::TopologyNode;
use crate::dashboard_ws::TopologySnapshot;
use crate::engine::Gateway;
use crate::metrics::MetricsView;
use crate::metrics::StatusClass;
use crate::metrics::WindowReport;
use crate::monitor::DebugWsMessage;
use crate::upstream::{ProviderHealthSnapshot, ProviderInventoryEntry};
use axum::Extension;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use utoipa::IntoParams;
use utoipa::ToSchema;

/// Window lengths in SECONDS (the divisor for the per-window rate fields). Must
/// match the MetricsLayer ring spans (1m/5m/1h at 1 s resolution).
const WINDOW_1M_SECS: f64 = 60.0;
const WINDOW_5M_SECS: f64 = 300.0;
const WINDOW_1H_SECS: f64 = 3600.0;

// ---------------------------------------------------------------------------
// Flow row + detail DTOs (the cost-bearing projections of a FlowRecord)
// ---------------------------------------------------------------------------

/// One row in the flow table (`GET /dashboard/api/flows`) — the body-free
/// [`SnapshotFlowSummary`](crate::dashboard_flow::SnapshotFlowSummary) fields PLUS
/// the D13 `cost` roll-up (usage × the served model's price). Mirrors the frozen
/// `FlowSummary` (types.ts) exactly: the `Option` fields use `skip_serializing_if`
/// to match the frontend's optional-key validators, EXCEPT `usage` (serialized as
/// `null` when absent — the frontend accepts absent/null/usage) and `cost`
/// (`null`-not-absent when the served model has no configured price).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct FlowRow {
    /// The gateway's per-request id (the flow's primary key).
    pub api_call_id: String,
    /// The engine response id the flow is linked to, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    /// Inbound HTTP method.
    pub method: String,
    /// Inbound request URI.
    pub uri: String,
    /// Model id the client asked for, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_requested: Option<String>,
    /// Model id that actually served the flow, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_served: Option<String>,
    /// The upstream (URL or provider name) that served the flow, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_target: Option<String>,
    /// Token usage reported by the upstream; `null` until reported.
    pub usage: Option<FlowUsage>,
    /// Lifecycle status.
    pub status: FlowStatus,
    /// Epoch-ms the flow opened.
    pub started_ms: u128,
    /// Epoch-ms the flow reached its terminal state, when terminal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_ms: Option<u128>,
    /// Monotonic wall-clock duration in ms from open to terminal, when terminal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u128>,
    /// Reason recorded at finalize, when one was given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
    /// Gap 04 — the STABLE, NON-SECRET client attribution label (key-hash `key-<hex>`
    /// display id / configured caller-id / User-Agent fallback), projected from the
    /// flow record/summary. `skip_serializing_if` so an unattributed flow OMITS the key
    /// (absent ⇒ renders `—`, never a fabricated id). Additive/optional: the frontend
    /// ignores it until the client-attribution UI (gap 15). The raw key is never here —
    /// only the one-way hash prefix ever existed as a label.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_label: Option<String>,
    /// Gap 04 — the [`ClientSource`] the label was derived from (so the weak
    /// `user_agent` fallback is distinguishable from a key-hash / configured-id). `None`
    /// (absent) exactly when `client_label` is `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_source: Option<ClientSource>,
    /// USD cost of the flow (usage × the served model's [`ModelPrice`]). `null`
    /// when no price is configured for `model_served` — never a fabricated zero.
    pub cost: Option<f64>,
    /// Gap 07 — the [`CostConfidence`] of `cost`: `confident` (priced + every billed
    /// class has a known rate), `estimated` (a class falls back to the default `0.0`
    /// cached rate / cached unreported), or `unavailable` (unpriced ⇒ `cost: null`).
    /// Always present so the frontend can label an `estimated` figure as such and
    /// distinguish an `unavailable` cost from a measured `$0.00`.
    pub cost_confidence: CostConfidence,
    /// Gap 10b — the gap-02 per-phase wall-clock timestamps, FLATTENED onto the row as
    /// sibling scalar fields (`ingress_ms`/`first_content_delta_ms`/…), mirroring the
    /// Rust `#[serde(flatten)] PhaseTimings` on `SnapshotFlowSummary`. The list row
    /// surfaces TTFT (`first_content_delta_ms`) per spec 10; the full bundle is carried
    /// (each field is `skip_serializing_if = None`, so an unmeasured phase is ABSENT, never
    /// `0`) so the inspector + the gap-16 overview read the same shape off either the
    /// row or the detail. Scalar metadata only — body-free (AGENTS.md snapshots-are-body-
    /// free invariant holds; these are `u128` epochs, not bodies).
    #[serde(flatten)]
    pub phases: PhaseTimings,
    /// Gap 10b — the gap-03 per-attempt failover trace projected onto the row (spec 11's
    /// stepper reads the whole list; spec 10 reads the served attempt's
    /// `first_upstream_byte_ms`). Each [`Attempt`] is body-free scalar provenance + bounded
    /// taxonomic codes — never a raw upstream error body. `skip_serializing_if =
    /// Vec::is_empty` so a flow with no recorded attempt OMITS the key (the frontend's
    /// `attempts?` is absent), matching the body-free summary's wire shape.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<Attempt>,
    /// Gap 10b — the gap-03 flow-level wire time-to-first-byte (the served attempt's first
    /// on-wire chunk). Distinct from `first_content_delta_ms` (the first content delta to
    /// the CLIENT). `None` ⇒ absent ⇒ renders `—` downstream, NEVER `0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_upstream_byte_ms: Option<u128>,
    /// Harness/session facts (harness, harness_version, session_id,
    /// chain_parent_request_id, divergence_kind, cache_bust), flattened as
    /// sibling fields; each absent when unknown.
    #[serde(flatten)]
    pub session: crate::dashboard_flow::FlowSessionFacts,
}

impl FlowRow {
    /// Build a row from a live [`FlowRecord`], pricing it via the gateway's price
    /// table keyed by the SERVED model (the backend that actually answered).
    fn from_record(record: &FlowRecord, gateway: &Gateway) -> Self {
        let (cost, cost_confidence) =
            flow_cost_and_confidence(record.model_served.as_deref(), record.usage, gateway);
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
            // Gap 04: thread the attribution (label + source) onto the row — body-free
            // scalar metadata; the raw key is never here (only the one-way hash prefix).
            client_label: record.client_label.clone(),
            client_source: record.client_source,
            cost,
            cost_confidence,
            // Gap 10b: project the gap-02 phases + gap-03 attempts/wire-TTFB from the live
            // record onto the row. `PhaseTimings` is `Copy`; the attempts vec is cloned
            // (body-free scalar provenance). No recompute — just thread the already-measured
            // spine fields through so the gap-10/11/16 surfaces light up against the row.
            phases: record.phases,
            attempts: record.attempts.clone(),
            first_upstream_byte_ms: record.first_upstream_byte_ms,
            session: record.session.clone(),
        }
    }

    /// Build a row from a body-free snapshot summary (the `/snapshot` summaries),
    /// pricing it the same way. The snapshot summary has no live `FlowRecord`, so
    /// this prices off its own `model_served` + `usage`.
    fn from_summary(
        summary: &crate::dashboard_flow::SnapshotFlowSummary,
        gateway: &Gateway,
    ) -> Self {
        let (cost, cost_confidence) =
            flow_cost_and_confidence(summary.model_served.as_deref(), summary.usage, gateway);
        Self {
            api_call_id: summary.api_call_id.clone(),
            response_id: summary.response_id.clone(),
            method: summary.method.clone(),
            uri: summary.uri.clone(),
            model_requested: summary.model_requested.clone(),
            model_served: summary.model_served.clone(),
            upstream_target: summary.upstream_target.clone(),
            usage: summary.usage,
            status: summary.status,
            started_ms: summary.started_ms,
            finished_ms: summary.finished_ms,
            elapsed_ms: summary.elapsed_ms,
            terminal_reason: summary.terminal_reason.clone(),
            // Gap 04: same attribution projection from the body-free snapshot summary.
            client_label: summary.client_label.clone(),
            client_source: summary.client_source,
            cost,
            cost_confidence,
            // Gap 10b: the body-free `SnapshotFlowSummary` ALREADY carries the gap-02 phases
            // + gap-03 attempts/wire-TTFB (specs 02/03) — thread them straight onto the row
            // so a `/snapshot` cut's rows carry the same measured spine as the live `/flows`
            // rows. No recompute.
            phases: summary.phases,
            attempts: summary.attempts.clone(),
            first_upstream_byte_ms: summary.first_upstream_byte_ms,
            session: summary.session.clone(),
        }
    }
}

/// `GET /dashboard/api/flows` — the paged flow list + total + the FlowStore
/// domain cursor. Matches the frozen `FlowsResponse`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct FlowsResponse {
    /// The requested page of rows, newest first.
    pub flows: Vec<FlowRow>,
    /// Total rows AFTER filtering but BEFORE paging (so the SPA can page).
    pub total: usize,
    /// FlowStore domain cursor at the time of the read.
    pub flow_seq: u64,
}

/// Query params for `GET /dashboard/api/flows`. All optional; `status`/`model`/
/// `upstream` filter, `page`/`limit` page (1-based page; absent ⇒ all rows).
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct FlowsQuery {
    /// Filter by lifecycle status: `open`, `completed`, `failed` or `cancelled`
    /// (an unrecognized value is ignored, not an error).
    pub status: Option<String>,
    /// Case-insensitive substring match on the served OR requested model id.
    pub model: Option<String>,
    /// Case-insensitive substring match on the upstream target.
    pub upstream: Option<String>,
    /// 1-based page number (default 1); only applies together with `limit`.
    pub page: Option<usize>,
    /// Rows per page; absent or `0` returns every filtered row.
    pub limit: Option<usize>,
}

/// One streamed delta replayed into the inspector (from the MonitorHub snapshot,
/// filtered by the flow's `response_id`). Mirrors the frozen `FlowDelta`:
/// `{sequence, kind, payload?, ts_ms?}`. `payload` is the heterogeneous delta body
/// (a segment text, an event summary, a status, …); the SPA narrows at the use
/// site. `sequence` is a per-flow ordinal (the replay order), NOT a domain cursor.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct FlowDelta {
    /// Per-flow replay ordinal (0-based, monitor order) — NOT a domain cursor.
    pub sequence: u64,
    /// `segment.<output|reasoning|tool>`, `event.<kind>` or `status`.
    pub kind: String,
    /// Kind-specific body: `{text}` for a segment, `{summary, payload_preview}` for
    /// an event, `{status, error}` for a status delta.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    /// Epoch-ms of the delta, when the monitor recorded one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<u128>,
}

/// Gap 05 — the captured upstream RESPONSE/ERROR body projected onto the live
/// [`FlowDetailBody`] (the `/dashboard/api/flows/:id` detail path only — NEVER the
/// body-free list rows or snapshot summaries). `body` is the redacted, capped bytes
/// parsed back to a JSON `Value` (or a string `Value` for a non-JSON / `[redacted:
/// unparseable body …]` marker, mirroring the other captured bodies); `truncated`
/// flags that the cap truncated the raw body, so the dashboard shows a PARTIAL body
/// honestly rather than presenting it as complete. Present ONLY when capture is armed
/// AND the turn produced an upstream error body (the live record's
/// [`upstream_response`](crate::dashboard_flow::FlowRecord::upstream_response) is
/// `Some`); absent otherwise (capture off / no body / evicted by the byte quota).
/// Derives `Deserialize` (alongside `Serialize`) so the new wire field round-trips in a
/// test (AGENTS.md: no new wire field without a deserialize-then-serialize proof) — the
/// enclosing [`FlowDetailBody`] stays serialize-only (it is only ever a response), so the
/// round-trip is pinned on THIS self-contained sub-DTO. Consumed by gap 14 (failure
/// taxonomy).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FlowUpstreamResponse {
    /// The redacted, capped upstream response/error body, parsed to JSON (or a string
    /// `Value` for a non-JSON / marker body). An EMPTY captured body parses to a string
    /// `""` — distinct from the whole field being ABSENT (capture off / no body).
    pub body: serde_json::Value,
    /// Whether the cap truncated the raw body (the retained bytes are a PREFIX). The
    /// dashboard must flag a truncated body rather than presenting it as complete.
    pub truncated: bool,
}

/// `GET /dashboard/api/flows/:id` — the 3-pane inspector body. Carries the summary
/// fields, the three captured on-wire bodies (inbound, normalized, upstream —
/// ABSENT, not error, when the summary-byte quota evicted them), the inbound
/// headers, the replayed deltas, usage, the terminal, and cost. Mirrors the frozen
/// `FlowDetail` (`:id == api_call_id`). The three bodies, headers, and deltas are
/// the additive detail fields over a [`FlowRow`].
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct FlowDetailBody {
    /// The record's OWN FlowStore cursor (frozen at its last mutation).
    pub flow_seq: u64,
    /// The gateway's per-request id (the flow's primary key).
    pub api_call_id: String,
    /// The engine response id the flow is linked to, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    /// The captured INBOUND request body (parsed JSON). Absent when evicted by the
    /// D1 summary-byte quota; parsed back to a `Value` so the SPA renders the tree.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_body: Option<serde_json::Value>,
    /// Captured inbound request headers; absent when none were retained.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_headers: Option<BTreeMap<String, String>>,
    /// The captured CANONICAL/normalized body (D2), parsed. Absent when evicted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalized: Option<serde_json::Value>,
    /// The captured UPSTREAM on-wire chat body (D2), parsed. Absent when evicted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_body: Option<serde_json::Value>,
    /// Gap 05 — the captured upstream RESPONSE/ERROR body (parsed) + its `truncated`
    /// flag, projected from the live record's
    /// [`upstream_response`](crate::dashboard_flow::FlowRecord::upstream_response).
    /// Present ONLY on this LIVE detail path (the diagnostic operator endpoint) when
    /// response capture is armed AND the turn produced an upstream error body; absent
    /// otherwise (capture off / no body / evicted by the byte quota). DELIBERATELY kept
    /// OFF the body-free [`FlowRow`] list rows and
    /// [`SnapshotFlowSummary`](crate::dashboard_flow::SnapshotFlowSummary) (the 135 GiB
    /// body-free-snapshot invariant). `skip_serializing_if` so an absent body OMITS the
    /// key. Consumed by gap 14 (failure taxonomy); the React app ignores it until then.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_response: Option<FlowUpstreamResponse>,
    /// Model id the client asked for, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_requested: Option<String>,
    /// Model id that actually served the flow, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_served: Option<String>,
    /// The upstream (URL or provider name) that served the flow, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_target: Option<String>,
    /// Token usage reported by the upstream; `null` until reported.
    pub usage: Option<FlowUsage>,
    /// Lifecycle status.
    pub status: FlowStatus,
    /// The streamed deltas replayed from the monitor, in monitor order.
    pub deltas: Vec<FlowDelta>,
    /// Reason recorded at finalize, when one was given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
    /// Epoch-ms the flow opened.
    pub started_ms: u128,
    /// Epoch-ms the flow reached its terminal state, when terminal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_ms: Option<u128>,
    /// Monotonic wall-clock duration in ms from open to terminal, when terminal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u128>,
    /// USD cost of the flow (usage × the served model's price); `null` when the
    /// served model has no configured price.
    pub cost: Option<f64>,
    /// Gap 07 — the [`CostConfidence`] of `cost` (confident/estimated/unavailable),
    /// mirroring the flow-row tag so the inspector labels an `estimated` figure.
    pub cost_confidence: CostConfidence,
    /// Gap 10b — the gap-02 per-phase wall-clock timestamps, FLATTENED onto the detail
    /// body (mirrors the `#[serde(flatten)] PhaseTimings` on `SnapshotFlowSummary`). The
    /// inspector's gap-10 latency waterfall reads the FULL phase set here (ingress →
    /// normalization → routing → first_content_delta → stream_end → finalize). Each field
    /// is `skip_serializing_if = None`, so an unmeasured phase is ABSENT, never `0`
    /// (don't-lie-with-zeros). Scalar `u128` epochs — not bodies.
    #[serde(flatten)]
    pub phases: PhaseTimings,
    /// Gap 10b — the gap-03 per-attempt failover trace, projected onto the detail body
    /// (spec 11's inspector stepper reads the whole list; spec 10 reads the served
    /// attempt's `first_upstream_byte_ms` to enrich the upstream-wait segment). Each
    /// [`Attempt`] is body-free scalar provenance + bounded taxonomic codes — never a raw
    /// upstream error body. `skip_serializing_if = Vec::is_empty` so a flow with no recorded
    /// attempt OMITS the key (matches the frontend's optional `attempts?`).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<Attempt>,
    /// Gap 10b — the gap-03 flow-level wire time-to-first-byte (the served attempt's first
    /// on-wire chunk). Distinct from `first_content_delta_ms` (first content delta to the
    /// CLIENT). `None` ⇒ absent ⇒ renders `—`, NEVER `0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_upstream_byte_ms: Option<u128>,
    /// Harness/session facts, flattened (see [`FlowRow::session`]).
    #[serde(flatten)]
    pub session: crate::dashboard_flow::FlowSessionFacts,
}

/// One catalog entry (`GET /dashboard/api/catalog` — a BARE array, no cursor).
/// `{id, context_limit}` where `context_limit` is the per-model max-context window
/// surfaced from the upstream `/v1/models` snapshot (gap 06).
///
/// NULLABLE end-to-end (gap 06 contract migration): `context_limit` is
/// `Option<i64>`, serialized as `null` when the upstream advertises no window —
/// distinct from a real `0`. Previously this DTO collapsed a missing window to a
/// non-null `0` (`unwrap_or(0)`), which lies-with-zeros: a `0` ceiling reads as
/// garbage/infinite utilization downstream (spec 09's context-window gauge).
/// `measured` when advertised; `unavailable`/`None` when the upstream omits it.
/// The frontend renders `—` on `null`, NEVER `0`. Derives `Deserialize` alongside
/// `Serialize` so the changed wire field round-trips in a test (AGENTS.md: no
/// changed wire field without a deserialize-then-serialize proof).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CatalogEntry {
    /// Model id as advertised by the upstream `/v1/models` catalog.
    pub id: String,
    /// The per-model max-context window (tokens), or `null`/absent when the
    /// upstream advertises none. `skip_serializing_if` so an unavailable window
    /// OMITS the key rather than emitting `null` — either is honest (the frontend
    /// type is `number | null` with the field optional); both are distinct from a
    /// real `0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<i64>,
}

/// `GET /dashboard/api/providers` — concrete provider resources and their
/// advertised model/capacity metadata. Kept separate from topology so graph
/// snapshots remain small and provider inventory can carry schedules.
#[derive(Debug, Clone, Serialize)]
pub struct ProvidersBody {
    pub providers: Vec<ProviderInventoryEntry>,
}

fn dashboard_error(status: StatusCode, message: impl Into<String>) -> Response {
    json_no_store(
        status,
        &crate::openapi::DashboardError {
            error: message.into(),
        },
    )
}

fn dashboard_admin_denial(session: &AuthSession) -> Option<Response> {
    if session
        .user
        .as_ref()
        .map_or_else(|| session.bootstrap_admin(), |user| user.is_admin)
    {
        None
    } else {
        Some(dashboard_error(
            StatusCode::FORBIDDEN,
            "administrator role required",
        ))
    }
}

fn dashboard_mutation_denial(auth: &DashboardAuth, headers: &HeaderMap) -> Option<Response> {
    auth.authorize_mutation(headers)
        .err()
        .map(|denied| dashboard_error(denied.status(), denied.message()))
}

/// `GET /dashboard/api/snapshot?at=<unix_ms>` — a body-free frozen cut. Mirrors
/// the frozen `SnapshotResponse`: the per-domain `cursors`, the cut instant, the
/// body-free flow summaries (priced), and the metrics/topology cuts reshaped into
/// their REST bodies (`null` when the cut is empty for that domain).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SnapshotResponse {
    /// The cut's per-domain cursors (all zero when no cut exists).
    pub cursors: SeqCursors,
    /// Epoch-ms the cut was taken (the requested `at`, or `0`, when no cut exists).
    pub at_ms: u128,
    /// The cut's body-free flow summaries, priced (empty when no cut exists).
    pub summaries: Vec<FlowRow>,
    /// The cut's metrics in the `/metrics` shape; `null` when no cut exists.
    pub metrics: Option<MetricsSnapshot>,
    /// The cut's topology in the `/topology` shape; `null` when no cut exists.
    pub topology: Option<TopologySnapshot>,
}

/// Query param for `GET /dashboard/api/snapshot` — the wall-clock instant (unix
/// ms) to time-travel to. Absent ⇒ the latest cut. Typed `u64` (NOT `u128`): the
/// axum/serde QUERY deserializer does not support `u128`, and unix-ms fits `u64`
/// for ~580 million years; the handler widens it to the `u128` `snapshot_at` key.
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct SnapshotQuery {
    /// Unix-ms instant to time-travel to (the nearest cut at or before it);
    /// absent ⇒ the latest cut.
    pub at: Option<u64>,
}

// ---------------------------------------------------------------------------
// Cost + rate helpers (pure — unit-testable without the HTTP stack)
// ---------------------------------------------------------------------------

/// The USD cost of one flow's `usage` at `model`'s configured price (`None` when
/// the model has no price, so the row reports `cost: null`, never a fake zero).
///
/// Billing model (the standard prompt/cached/completion split): the `cached`
/// prompt tokens bill at the cache-read rate and the REMAINING prompt tokens at
/// the input rate, so `cached` is treated as a subset of `prompt` (clamped at 0 so
/// a transient `cached > prompt` never yields a negative charge). Reasoning tokens
/// are part of the completion the provider bills, so they are NOT charged
/// separately (the `total`/`completion` already account for them upstream).
///
/// The result is run through [`finite`] so a degenerate configured price (an
/// absurd magnitude that overflows to ±∞, or a serde-loaded NaN) can never poison
/// the JSON: `serde_json::to_vec` ERRORS on a non-finite float, which would 500 the
/// whole `/flows` (or snapshot) read. A non-finite cost collapses to `0.0` instead.
pub fn cost_for_usage(usage: FlowUsage, price: ModelPrice) -> f64 {
    // Gap 07: an UNREPORTED cached count (`None`) bills as 0 cached tokens — the whole
    // prompt then bills at the input rate (the confidence tier flags this as `estimated`
    // when no cached rate is configured; the dollar figure stays a best-effort number).
    let cached = usage.cached.unwrap_or(0).max(0) as f64;
    let prompt = usage.prompt.max(0) as f64;
    let completion = usage.completion.max(0) as f64;
    // Uncached prompt = prompt - cached (never negative).
    let uncached_prompt = (prompt - cached).max(0.0);
    finite(
        (uncached_prompt / 1000.0) * price.input_per_1k
            + (cached / 1000.0) * price.cached_per_1k
            + (completion / 1000.0) * price.output_per_1k,
    )
}

/// Gap 07 — the CONFIDENCE tier of a flow's `cost`, so an operator can tell a trusted
/// figure from a best-effort estimate from an honest gap. Emitted alongside `cost` on
/// every flow row + detail (and aggregated onto the metrics windows). Serializes
/// snake_case to mirror the data-quality vocabulary the frontend already uses
/// (`measured`/`derived`/`estimated`/`unavailable`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CostConfidence {
    /// The model is priced AND every billed token class has a known rate: the prompt
    /// (input) + completion (output) rates always exist when priced, and cached tokens
    /// either were reported as `0` (nothing billed at the cache rate) OR the model has
    /// a CONFIGURED cached rate. The dollar figure is trustworthy.
    Confident,
    /// The model is priced but a billed class would fall back to an APPROXIMATE rate:
    /// cached tokens were reported `> 0` (or are UNAVAILABLE) while the model has NO
    /// configured cached rate, so those tokens silently bill at the default `0.0`
    /// (an undercount). The figure is a best-effort ESTIMATE — surfaced as such.
    Estimated,
    /// No price is configured for the served model: there is NO cost (the row reports
    /// `cost: null`), so the figure is UNAVAILABLE — never a fabricated `0`. The
    /// DEFAULT: a window/flow with nothing priced makes no confident claim.
    #[default]
    Unavailable,
}

/// Gap 07 — classify a flow's cost confidence from its served model's price PRESENCE
/// + the cached-token report. The rules (spec 07 acceptance):
/// - unpriced model ⇒ [`CostConfidence::Unavailable`] (cost is `None`, never `0`).
/// - priced AND (`cached == Some(0)` OR `cached_price_configured`) ⇒ `Confident`
///   (a reported `cached = 0` bills nothing at the cache rate; a configured rate
///   prices it honestly).
/// - priced AND (`cached == Some(n>0)` OR `cached == None`) AND NOT
///   `cached_price_configured` ⇒ `Estimated` (those cached tokens would bill at the
///   default `0.0` — an undercount, so NOT a silently-`confident` total).
fn cost_confidence(price: Option<ModelPrice>, usage: Option<FlowUsage>) -> CostConfidence {
    let Some(price) = price else {
        return CostConfidence::Unavailable;
    };
    // A priced flow with no usage at all still has a (zero-token) cost; with no cached
    // tokens billed it is trivially confident.
    let cached = usage.and_then(|usage| usage.cached);
    match cached {
        // Reported zero cache tokens: nothing bills at the cache rate ⇒ confident
        // regardless of whether a cached rate is configured.
        Some(0) => CostConfidence::Confident,
        // Reported >0 cached, OR UNAVAILABLE (None) cached: confident ONLY if a cached
        // rate is configured; otherwise those tokens fall back to the default 0.0.
        Some(_) | None => {
            if price.cached_price_configured {
                CostConfidence::Confident
            } else {
                CostConfidence::Estimated
            }
        }
    }
}

/// A JSON-safe float: the value if finite, else `0.0`. `serde_json` REFUSES to
/// serialize NaN/±∞ (it errors), so every float that reaches a response body — cost
/// roll-ups, per-second rates — is passed through this so a degenerate input can
/// never turn a read into a 500. (The inputs are operator-configured prices, not
/// attacker data, but a typo'd 1e308 rate should degrade gracefully, not 500.)
fn finite(value: f64) -> f64 {
    if value.is_finite() { value } else { 0.0 }
}

/// Gap 07 — price a flow AND tag its [`CostConfidence`] together, so the two can never
/// disagree: cost is `Some` exactly when a served model + usage + a configured price
/// all exist, and the confidence is then `Confident`/`Estimated` per the cached-rate
/// presence; whenever cost is `None` (unpriced/no-usage/no-model) the confidence is
/// `Unavailable` (don't-lie-with-zeros: an absent cost is never a confident `0`).
fn flow_cost_and_confidence(
    model_served: Option<&str>,
    usage: Option<FlowUsage>,
    gateway: &Gateway,
) -> (Option<f64>, CostConfidence) {
    let price = model_served.and_then(|model| gateway.price_for(model));
    priced_cost_and_confidence(price, usage)
}

/// The cost/confidence rule itself, independent of where the price came from, so
/// the REST rows (`FlowRow`) and the WS snapshot rows
/// (`dashboard_ws::price_snapshot_summaries`) can never tag the same flow
/// differently.
pub(crate) fn priced_cost_and_confidence(
    price: Option<ModelPrice>,
    usage: Option<FlowUsage>,
) -> (Option<f64>, CostConfidence) {
    match (price, usage) {
        // Priced AND usage present: a real cost, tagged confident/estimated by the
        // cached-rate presence.
        (Some(price), Some(usage)) => (
            Some(cost_for_usage(usage, price)),
            cost_confidence(Some(price), Some(usage)),
        ),
        // Unpriced, OR no usage to bill: no cost, so UNAVAILABLE (never a fake 0).
        _ => (None, CostConfidence::Unavailable),
    }
}

/// The total token throughput of one window (prompt + completion + cached +
/// reasoning across every bucket) — the numerator for `tokens_per_sec`.
fn window_total_tokens(report: &WindowReport) -> i64 {
    report
        .buckets
        .values()
        .map(|counts| {
            counts
                .prompt_tokens
                .saturating_add(counts.completion_tokens)
                .saturating_add(counts.cached_tokens)
                .saturating_add(counts.reasoning_tokens)
        })
        .fold(0i64, i64::saturating_add)
}

/// The total USD cost of one window: every bucket's tokens priced by its OWN
/// served model (`BucketKey.model`). Buckets whose model has no configured price
/// contribute nothing. The basis for `cost_per_min` (this ÷ window minutes).
fn window_total_cost(report: &WindowReport, prices: &HashMap<String, ModelPrice>) -> f64 {
    report
        .buckets
        .iter()
        .filter_map(|(key, counts)| {
            price_lookup(prices, &key.model).map(|price| {
                cost_for_usage(
                    FlowUsage {
                        prompt: counts.prompt_tokens,
                        completion: counts.completion_tokens,
                        // The bucket sums are concrete aggregates (gap 07): the cached/
                        // reasoning totals are `Some` measured values, not unreported.
                        cached: Some(counts.cached_tokens),
                        reasoning: Some(counts.reasoning_tokens),
                        total: 0,
                    },
                    price,
                )
            })
        })
        .sum()
}

/// Gap 07 — the AGGREGATE [`CostConfidence`] of one window's `cost_per_min`. An
/// aggregate touching ANY non-confident component is itself `estimated` (spec 07:
/// "no silently-confident totals"):
/// - NO priced bucket (nothing to bill) ⇒ `Unavailable` (`cost_per_min` renders `—`).
/// - A priced bucket would bill cached at the default `0.0` — i.e. it billed cached
///   tokens (`cached_tokens > 0`) OR a usage-bearing flow left cached UNREPORTED
///   (`unreported_cached_samples > 0`) — while that model has NO configured cached
///   rate ⇒ `Estimated`.
/// - An UNPRICED but USAGE-BEARING bucket (`usage_samples > 0`, no configured price)
///   COEXISTS with priced buckets ⇒ `Estimated` (gap 07 review round 1, finding 2). Its
///   real spend is OMITTED from `cost_per_min` entirely (unpriced ⇒ contributes `0`), so
///   the reported total is a PARTIAL undercount of the window's true cost — exactly the
///   kind of silently-confident total the spec forbids. It is NOT `unavailable` (some
///   buckets ARE priced, so `cost_per_min` is a real number that renders), just an
///   incomplete one. A usage-LESS unpriced bucket (a flow that reported no tokens, e.g. a
///   failure) adds no missing cost, so it does NOT taint the window.
/// - Otherwise (every priced bucket either bills no cached tokens AND had none
///   unreported or has a configured cached rate, AND no unpriced bucket bore usage) ⇒
///   `Confident`.
fn window_cost_confidence(
    report: &WindowReport,
    prices: &HashMap<String, ModelPrice>,
) -> CostConfidence {
    let mut any_priced = false;
    let mut any_estimated = false;
    // Tracked SEPARATELY from `any_estimated` because an unpriced usage-bearing bucket
    // only forces `estimated` when a priced bucket ALSO exists (a window that is ALL
    // unpriced stays `unavailable` — nothing was billed at all).
    let mut unpriced_usage_bearing = false;
    for (key, counts) in &report.buckets {
        let Some(price) = price_lookup(prices, &key.model) else {
            // Unpriced bucket: its cost is OMITTED from the window total. If it carried
            // real usage, the total is a partial undercount — flag it (resolved against
            // `any_priced` below). A usage-less unpriced bucket adds no missing cost.
            if counts.usage_samples > 0 {
                unpriced_usage_bearing = true;
            }
            continue;
        };
        any_priced = true;
        // This priced bucket bills cached at the default 0.0 when it has cached tokens
        // (or a flow that didn't report cached) AND no configured cache rate.
        let bills_unknown_cached = counts.cached_tokens > 0 || counts.unreported_cached_samples > 0;
        if bills_unknown_cached && !price.cached_price_configured {
            any_estimated = true;
        }
    }
    // An unpriced usage-bearing bucket taints the total ONLY when something priced
    // contributes to it (otherwise the window is `unavailable`, handled below).
    let partial_from_unpriced = any_priced && unpriced_usage_bearing;
    match (any_priced, any_estimated || partial_from_unpriced) {
        (false, _) => CostConfidence::Unavailable,
        (true, true) => CostConfidence::Estimated,
        (true, false) => CostConfidence::Confident,
    }
}

/// Exact-then-case-insensitive price lookup over a raw price map, mirroring
/// [`crate::config::Config::price_for`] (used where only the map is in hand, e.g.
/// pricing a snapshot cut's metrics buckets).
fn price_lookup(prices: &HashMap<String, ModelPrice>, model: &str) -> Option<ModelPrice> {
    prices.get(model).copied().or_else(|| {
        prices
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(model))
            .map(|(_, price)| *price)
    })
}

/// Collapse one [`WindowReport`] into a flat REST [`MetricWindow`] tile over
/// `window_secs` seconds: TRUE per-second request/token rates, the error %, the
/// p50/p95/p99 latency, and the per-minute cost (this is D13's job — the WS
/// `window_tile` ships raw counts + `0.0` cost). `active_streams` is the live open-
/// flow count (passed in; the rings don't track liveness). An empty window reports
/// all-zero rates (finite — the contract requires finite numbers).
fn rest_window_tile(
    report: &WindowReport,
    window_secs: f64,
    active_streams: u64,
    prices: &HashMap<String, ModelPrice>,
) -> MetricWindow {
    let percentiles = report.percentiles();
    let total = report.total_count();
    let errors: u64 = report
        .buckets
        .iter()
        .filter(|(key, _)| key.status == StatusClass::Error)
        .map(|(_, counts)| counts.count)
        .fold(0u64, u64::saturating_add);
    let error_pct = if total > 0 {
        (errors as f64) / (total as f64) * 100.0
    } else {
        0.0
    };
    let reqs_per_sec = total as f64 / window_secs;
    let tokens_per_sec = window_total_tokens(report) as f64 / window_secs;
    let prefill_tokens_per_sec = report.prefill_tokens_per_sec();
    let decode_tokens_per_sec = report.decode_tokens_per_sec();
    let cost_per_min = window_total_cost(report, prices) / (window_secs / 60.0);
    // Per-metric measurability denominators (gap 01 review round 1, finding 3): token
    // and cost availability are SEPARATE from latency/error. `usage_samples` counts
    // terminal flows that reported usage; `priced_samples` the subset whose served
    // model has a configured price (derived HERE, where the price table lives, so
    // `metrics.rs` stays price-agnostic). A window can have `samples > 0` (latency
    // measured) yet `usage_samples == 0` (no flow reported tokens) → `tokens_per_sec`
    // renders `—`; or `usage_samples > 0` yet `priced_samples == 0` (only unpriced
    // models) → `cost_per_min` renders `—`, distinguishing "unpriced" from `$0.00`.
    let usage_samples = report.usage_sample_count();
    let prefill_samples = report.prefill_sample_count();
    let decode_samples = report.decode_sample_count();
    let priced_samples = report.priced_sample_count(|model| price_lookup(prices, model).is_some());
    // Gap 07: the aggregate cost confidence for this window's `cost_per_min` — `estimated`
    // when any priced bucket would silently bill cached at the default `0.0`, so the strip
    // labels the headline `$/min` rather than presenting a possibly-undercounted total as
    // confident.
    let cost_confidence = window_cost_confidence(report, prices);
    // Every float is `finite`-guarded: a non-finite value would make
    // `serde_json::to_vec` error and 500 the `/metrics` read.
    MetricWindow {
        reqs_per_sec: finite(reqs_per_sec),
        active_streams,
        error_pct: finite(error_pct),
        p50: finite(percentiles.p50),
        p95: finite(percentiles.p95),
        p99: finite(percentiles.p99),
        tokens_per_sec: finite(tokens_per_sec),
        prefill_tokens_per_sec: finite(prefill_tokens_per_sec),
        decode_tokens_per_sec: finite(decode_tokens_per_sec),
        cost_per_min: finite(cost_per_min),
        // `total` is the count of TERMINAL flows in the window — the latency/error
        // measured/unavailable signal. `0` here ≠ "zero throughput"; it means NO
        // finalized flow fed the latency/error fields, so the frontend renders those
        // `—` (while `reqs_per_sec`'s genuine `0` stays a `0`). Token/cost availability
        // use the separate denominators above.
        samples: total,
        usage_samples,
        prefill_samples,
        decode_samples,
        priced_samples,
        cost_confidence,
    }
}

/// Build the full `/metrics`-shaped [`MetricsSnapshot`] body from a collapsed
/// [`MetricsView`] (+ its `metrics_seq`), the live open-flow count, and the price
/// table. The headline tile repeats the `m1` window (the dashboard's headline) and
/// nests all three windows under `windows`. Shared by the live `/metrics` read AND
/// the `/snapshot` metrics reshape so both emit byte-identical shapes.
pub fn metrics_body(
    view: &MetricsView,
    metrics_seq: u64,
    active_streams: u64,
    prices: &HashMap<String, ModelPrice>,
) -> MetricsSnapshot {
    let m1 = rest_window_tile(&view.window_1m, WINDOW_1M_SECS, active_streams, prices);
    let m5 = rest_window_tile(&view.window_5m, WINDOW_5M_SECS, active_streams, prices);
    let h1 = rest_window_tile(&view.window_1h, WINDOW_1H_SECS, active_streams, prices);
    MetricsSnapshot {
        metrics_seq,
        reqs_per_sec: m1.reqs_per_sec,
        active_streams: m1.active_streams,
        error_pct: m1.error_pct,
        p50: m1.p50,
        p95: m1.p95,
        p99: m1.p99,
        tokens_per_sec: m1.tokens_per_sec,
        prefill_tokens_per_sec: m1.prefill_tokens_per_sec,
        decode_tokens_per_sec: m1.decode_tokens_per_sec,
        cost_per_min: m1.cost_per_min,
        samples: m1.samples,
        usage_samples: m1.usage_samples,
        prefill_samples: m1.prefill_samples,
        decode_samples: m1.decode_samples,
        priced_samples: m1.priced_samples,
        cost_confidence: m1.cost_confidence,
        windows: MetricWindows { m1, m5, h1 },
    }
}

/// Build the full `/topology`-shaped [`TopologySnapshot`] body from a D4
/// [`ProviderHealthSnapshot`] + the price table + the live `m1` metrics window
/// (for the edge rate roll-ups). Each provider becomes a node; one gateway→provider
/// edge carries that provider's per-second request/token/cost rates aggregated from
/// the m1 window keyed by `BucketKey.upstream`. Shared by `/topology` AND the
/// `/snapshot` topology reshape.
pub fn topology_body(
    snapshot: &ProviderHealthSnapshot,
    prices: &HashMap<String, ModelPrice>,
    window_1m: &WindowReport,
) -> TopologySnapshot {
    // Gap 12: each node carries its per-provider latency/error metrics from the m1
    // window (aggregated off the evict-safe per-attempt trace), looked up by provider id
    // — absent when the provider had no in-window samples (don't-lie-with-zeros). Same
    // m1 window the edge rates below roll up, so the tiles + edges share one metrics cut.
    let nodes: Vec<TopologyNode> = snapshot
        .providers
        .iter()
        .map(|provider| TopologyNode::from_health_with_metrics(provider, window_1m))
        .collect();
    let edges: Vec<TopologyEdge> = snapshot
        .providers
        .iter()
        .map(|provider| {
            let (reqs, tokens, cost) = upstream_edge_rates(&provider.id, window_1m, prices);
            TopologyEdge {
                from: "gateway".to_string(),
                to: provider.id.clone(),
                throughput: reqs,
                tokens_per_sec: tokens,
                cost_per_sec: cost,
            }
        })
        .collect();
    TopologySnapshot {
        topology_seq: snapshot.version,
        nodes,
        edges,
        price_table: prices
            .iter()
            .map(|(model, price)| (model.clone(), *price))
            .collect(),
    }
}

/// The `(reqs_per_sec, tokens_per_sec, cost_per_sec)` rates for one upstream over
/// the `m1` window: every bucket whose `BucketKey.upstream` matches `upstream_id`,
/// summed and divided by the 60 s window. Cost prices each bucket by its OWN served
/// model. Used to enrich the gateway→provider topology edges.
fn upstream_edge_rates(
    upstream_id: &str,
    window_1m: &WindowReport,
    prices: &HashMap<String, ModelPrice>,
) -> (f64, f64, f64) {
    let mut reqs = 0u64;
    let mut tokens = 0i64;
    let mut cost = 0.0f64;
    for (key, counts) in &window_1m.buckets {
        if key.upstream != upstream_id {
            continue;
        }
        reqs = reqs.saturating_add(counts.count);
        tokens = tokens
            .saturating_add(counts.prompt_tokens)
            .saturating_add(counts.completion_tokens)
            .saturating_add(counts.cached_tokens)
            .saturating_add(counts.reasoning_tokens);
        if let Some(price) = price_lookup(prices, &key.model) {
            cost += cost_for_usage(
                FlowUsage {
                    prompt: counts.prompt_tokens,
                    completion: counts.completion_tokens,
                    // Concrete bucket aggregates → measured `Some` (gap 07).
                    cached: Some(counts.cached_tokens),
                    reasoning: Some(counts.reasoning_tokens),
                    total: 0,
                },
                price,
            );
        }
    }
    (
        finite(reqs as f64 / WINDOW_1M_SECS),
        finite(tokens as f64 / WINDOW_1M_SECS),
        finite(cost / WINDOW_1M_SECS),
    )
}

/// Count the flows currently OPEN (live streams) in the FlowStore — the
/// `active_streams` tile value (the metrics rings count terminals, not liveness).
/// `pub(crate)` so the live `/dashboard/ws` tick + initial snapshot derive the SAME
/// open-flow count as the REST `/metrics` read (gap 01 — one source, no drift).
pub(crate) fn active_stream_count(gateway: &Gateway) -> u64 {
    gateway.flow_store().open_count()
}

/// Count the OPEN flows in a FROZEN snapshot cut's body-free summaries — the
/// `active_streams` value for a historical `/snapshot?at=` (D13 R1 HIGH). Reading the
/// live FlowStore for a time-travel cut would report NOW's open count, not the cut's;
/// the summaries are the cut's own consistent flow projection, so counting their open
/// status keeps the whole snapshot frozen to one instant.
fn cut_active_stream_count(summaries: &[crate::dashboard_flow::SharedFlowSummary]) -> u64 {
    summaries
        .iter()
        .filter(|summary| summary.status == FlowStatus::Open)
        .count() as u64
}

// ---------------------------------------------------------------------------
// Delta replay (MonitorHub snapshot, filtered by response_id)
// ---------------------------------------------------------------------------

/// Replay the streamed deltas for a flow from the MonitorHub snapshot, filtered by
/// the flow's `response_id` (the monitor keys transcript messages by the engine's
/// response id, NOT the `api_call_id`). Returns an empty `Vec` when the flow has no
/// linked `response_id` yet (nothing to correlate). Each matching `SegmentAppend`/
/// `EventAppend`/`RequestStatus` becomes a [`FlowDelta`] in monitor order, with a
/// per-flow `sequence` ordinal. `RequestUpsert`/`Usage`/`RequestRemove`/`Hello`/
/// `SnapshotDone` are not per-token deltas (the row already carries usage/status),
/// so they are skipped — the inspector wants the segment/event timeline.
fn replay_deltas(response_id: Option<&str>, gateway: &Gateway) -> Vec<FlowDelta> {
    let Some(response_id) = response_id else {
        return Vec::new();
    };
    let snapshot = gateway.debug_snapshot();
    let mut deltas = Vec::new();
    let mut sequence = 0u64;
    for message in &snapshot.messages {
        let delta = match message {
            DebugWsMessage::SegmentAppend {
                response_id: rid,
                segment,
            } if rid == response_id => FlowDelta {
                sequence,
                kind: format!("segment.{}", segment_kind_str(segment.kind)),
                payload: Some(serde_json::json!({ "text": segment.text })),
                ts_ms: Some(segment.timestamp_ms),
            },
            DebugWsMessage::EventAppend {
                response_id: rid,
                event,
            } if rid == response_id => FlowDelta {
                sequence,
                kind: format!("event.{}", event.kind),
                payload: Some(serde_json::json!({
                    "summary": event.summary,
                    "payload_preview": event.payload_preview,
                })),
                ts_ms: Some(event.timestamp_ms),
            },
            DebugWsMessage::RequestStatus {
                response_id: rid,
                status,
                completed_at_ms,
                error,
            } if rid == response_id => FlowDelta {
                sequence,
                kind: "status".to_string(),
                payload: Some(serde_json::json!({
                    "status": request_status_str(*status),
                    "error": error,
                })),
                ts_ms: *completed_at_ms,
            },
            _ => continue,
        };
        deltas.push(delta);
        sequence += 1;
    }
    deltas
}

/// The snake_case wire string for a [`crate::monitor::DebugSegmentKind`] (matches
/// the frozen `DebugSegmentKind` union: output/reasoning/tool).
fn segment_kind_str(kind: crate::monitor::DebugSegmentKind) -> &'static str {
    match kind {
        crate::monitor::DebugSegmentKind::Output => "output",
        crate::monitor::DebugSegmentKind::Reasoning => "reasoning",
        crate::monitor::DebugSegmentKind::Tool => "tool",
    }
}

/// The snake_case wire string for a [`crate::monitor::DebugRequestStatus`].
fn request_status_str(status: crate::monitor::DebugRequestStatus) -> &'static str {
    match status {
        crate::monitor::DebugRequestStatus::Running => "running",
        crate::monitor::DebugRequestStatus::Completed => "completed",
        crate::monitor::DebugRequestStatus::Failed => "failed",
    }
}

/// Parse a captured (already-redacted + capped JSON) body `Arc<[u8]>` back into a
/// `serde_json::Value` for the inspector. A body that does not parse as JSON (a
/// truncated capture, a non-JSON payload) falls back to a JSON string of the
/// lossy UTF-8 so the field is still present + renderable rather than dropped.
fn parse_captured_body(body: &Arc<[u8]>) -> serde_json::Value {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(value) => value,
        Err(_) => serde_json::Value::String(String::from_utf8_lossy(body).into_owned()),
    }
}

// ---------------------------------------------------------------------------
// Handlers (each `State(Arc<Gateway>)`; no-store + auth applied by the route layer)
// ---------------------------------------------------------------------------

/// `GET /dashboard/api/flows?status=&model=&upstream=&page=&limit=` — the flow
/// table. Lists newest-first from the FlowStore (D1), filters by status/model/
/// upstream, pages, and stamps the FlowStore domain `flow_seq`. Each row carries
/// its `cost` (usage × served-model price).
#[utoipa::path(
    get,
    path = "/dashboard/api/flows",
    tag = "dashboard",
    operation_id = "dashboard_flows",
    params(FlowsQuery),
    responses(
        (
            status = 200,
            description = "The filtered, paged flow rows plus the FlowStore domain cursor.",
            body = FlowsResponse
        ),
        (status = 400, description = "Unknown or mistyped query parameter (the query string is strict; the text names the field). Plain text from the extractor.", content_type = "text/plain", body = String),
        (
            status = 401,
            description = "No valid dashboard session (plain text `unauthorized`).",
            body = String,
            content_type = "text/plain"
        ),
        (
            status = 500,
            description = "Response serialization failed (plain text `failed to serialize response`).",
            body = String,
            content_type = "text/plain"
        ),
    )
)]
pub async fn dashboard_flows(
    State(gateway): State<Arc<Gateway>>,
    Query(query): Query<FlowsQuery>,
) -> Response {
    let flow_seq = gateway.flow_store().flow_seq();
    let status_filter = query.status.as_deref().and_then(parse_status_filter);
    let model_filter = query.model.as_deref().map(str::to_ascii_lowercase);
    let upstream_filter = query.upstream.as_deref().map(str::to_ascii_lowercase);

    let rows: Vec<FlowRow> = gateway
        .flow_store()
        .list()
        .iter()
        .filter(|record| {
            status_filter.is_none_or(|status| record.status == status)
                && model_filter
                    .as_ref()
                    .is_none_or(|wanted| record_matches_model(record, wanted))
                && upstream_filter.as_ref().is_none_or(|wanted| {
                    record
                        .upstream_target
                        .as_deref()
                        .is_some_and(|target| target.to_ascii_lowercase().contains(wanted))
                })
        })
        .map(|record| FlowRow::from_record(record, gateway.as_ref()))
        .collect();

    let total = rows.len();
    let paged = apply_paging(rows, query.page, query.limit);
    json_no_store(
        StatusCode::OK,
        &FlowsResponse {
            flows: paged,
            total,
            flow_seq,
        },
    )
}

/// `GET /dashboard/api/flows/:id` — the 3-pane inspector body (`:id == api_call_id`,
/// joined by either id via the FlowStore link index). Returns the three captured
/// on-wire bodies (absent, not error, when evicted), the inbound headers, the
/// replayed deltas (MonitorHub snapshot filtered by `response_id`), usage, the
/// terminal, timing, the served identity, and the `cost`. `404` for an unknown id.
#[utoipa::path(
    get,
    path = "/dashboard/api/flows/{id}",
    tag = "dashboard",
    operation_id = "dashboard_flow_detail",
    params(
        (
            "id" = String,
            Path,
            description = "The flow's `api_call_id`; a linked `response_id` also resolves via the FlowStore link index."
        )
    ),
    responses(
        (
            status = 200,
            description = "The inspector detail body for the flow.",
            body = FlowDetailBody
        ),
        (
            status = 404,
            description = "No live flow for that id (also when the flow store is disabled). Body: `{\"error\": \"no flow for that id\"}`.",
            body = crate::openapi::DashboardError
        ),
        (
            status = 401,
            description = "No valid dashboard session (plain text `unauthorized`).",
            body = String,
            content_type = "text/plain"
        ),
        (
            status = 500,
            description = "Response serialization failed (plain text `failed to serialize response`).",
            body = String,
            content_type = "text/plain"
        ),
    )
)]
pub async fn dashboard_flow_detail(
    State(gateway): State<Arc<Gateway>>,
    Path(id): Path<String>,
) -> Response {
    // Capture the record AND its own mutation watermark in one lock hold so the
    // detail's `flow_seq` is the record's own cursor (D7b R1 finding 3), not a
    // later global value bumped by unrelated flows.
    let Some((record, flow_seq)) = gateway.flow_store().detail_with_seq(&id) else {
        return json_no_store(
            StatusCode::NOT_FOUND,
            &serde_json::json!({ "error": "no flow for that id" }),
        );
    };
    let (cost, cost_confidence) = flow_cost_and_confidence(
        record.model_served.as_deref(),
        record.usage,
        gateway.as_ref(),
    );
    let deltas = replay_deltas(record.response_id.as_deref(), gateway.as_ref());
    let inbound_headers = if record.headers.is_empty() {
        None
    } else {
        Some(
            record
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect::<BTreeMap<String, String>>(),
        )
    };
    let body = FlowDetailBody {
        flow_seq,
        api_call_id: record.api_call_id.clone(),
        response_id: record.response_id.clone(),
        inbound_body: record.inbound_body.as_ref().map(parse_captured_body),
        inbound_headers,
        normalized: record.normalized.as_ref().map(parse_captured_body),
        upstream_body: record.upstream_body.as_ref().map(parse_captured_body),
        // Gap 05: project the captured upstream RESPONSE/ERROR body (bytes + truncated)
        // onto the LIVE detail body. Parse the redacted/capped bytes like the other
        // captured bodies; `truncated` rides alongside so the dashboard flags a partial
        // body. Absent when the record's `upstream_response` is `None` (capture off / no
        // body / evicted). Stays OFF the list rows + snapshot summaries.
        upstream_response: record
            .upstream_response
            .as_ref()
            .map(|response| FlowUpstreamResponse {
                body: parse_captured_body(&response.bytes),
                truncated: response.truncated,
            }),
        model_requested: record.model_requested.clone(),
        model_served: record.model_served.clone(),
        upstream_target: record.upstream_target.clone(),
        usage: record.usage,
        status: record.status,
        deltas,
        terminal_reason: record.terminal_reason.clone(),
        started_ms: record.started_ms,
        finished_ms: record.finished_ms,
        elapsed_ms: record.elapsed_ms,
        cost,
        cost_confidence,
        // Gap 10b: project the FULL gap-02 phase spine + gap-03 attempts/wire-TTFB from the
        // live record onto the inspector detail — this is where the gap-10 waterfall + the
        // gap-11 attempt stepper live. `PhaseTimings` is `Copy`; the attempts vec is cloned
        // (body-free scalar provenance). No recompute — the spine was measured by the engine
        // (gaps 02/03); the detail just threads it through.
        phases: record.phases,
        attempts: record.attempts.clone(),
        first_upstream_byte_ms: record.first_upstream_byte_ms,
        session: record.session.clone(),
    };
    json_no_store(StatusCode::OK, &body)
}

/// `GET /dashboard/api/metrics` — the live stats tiles (D5 view) + the metrics
/// domain `metrics_seq` + the live open-flow `active_streams` count + the priced
/// `cost_per_min`. Per-window TRUE per-second rates (D13 divides by the window
/// seconds). The view + its cursor are captured in ONE metrics-lock hold so the
/// body and `metrics_seq` are consistent.
#[utoipa::path(
    get,
    path = "/dashboard/api/metrics",
    tag = "dashboard",
    operation_id = "dashboard_metrics",
    responses(
        (
            status = 200,
            description = "The live metrics tiles (headline `m1` + all three windows) and the metrics domain cursor.",
            body = MetricsSnapshot
        ),
        (
            status = 401,
            description = "No valid dashboard session (plain text `unauthorized`).",
            body = String,
            content_type = "text/plain"
        ),
        (
            status = 500,
            description = "Response serialization failed (plain text `failed to serialize response`).",
            body = String,
            content_type = "text/plain"
        ),
    )
)]
pub async fn dashboard_metrics(State(gateway): State<Arc<Gateway>>) -> Response {
    let (view, metrics_seq) = gateway.metrics().view_with_seq();
    let active = active_stream_count(gateway.as_ref());
    let prices = gateway.price_table();
    let body = metrics_body(&view, metrics_seq, active, &prices);
    json_no_store(StatusCode::OK, &body)
}

/// `GET /dashboard/api/topology` — the provider topology (D4 nodes + edges) + the
/// price table + the topology domain `topology_seq`. Edges carry per-upstream
/// per-second request/token/cost rates rolled up from the live `m1` metrics window.
#[utoipa::path(
    get,
    path = "/dashboard/api/topology",
    tag = "dashboard",
    operation_id = "dashboard_topology",
    responses(
        (
            status = 200,
            description = "Provider nodes, gateway→provider edges, the price table and the topology domain cursor.",
            body = TopologySnapshot
        ),
        (
            status = 401,
            description = "No valid dashboard session (plain text `unauthorized`).",
            body = String,
            content_type = "text/plain"
        ),
        (
            status = 500,
            description = "Response serialization failed (plain text `failed to serialize response`).",
            body = String,
            content_type = "text/plain"
        ),
    )
)]
pub async fn dashboard_topology(State(gateway): State<Arc<Gateway>>) -> Response {
    let snapshot = gateway.provider_health_publisher().latest();
    let view = gateway.metrics().view();
    let prices = gateway.price_table();
    let body = topology_body(&snapshot, &prices, &view.window_1m);
    json_no_store(StatusCode::OK, &body)
}

/// `GET /dashboard/api/catalog` — the model catalog as a BARE array `[{id,
/// context_limit}]` (no cursor; a static-ish read). Sourced from the upstream
/// `/v1/models` snapshot via the `UpstreamClient` (ids + per-model context
/// window), reusing the SAME `context_limit_by_id` parse that feeds G3 budgeting
/// (no second max-context parser — gap 06).
///
/// `context_limit` is surfaced NULLABLE: an upstream that advertises no window
/// yields `None` (serialized absent), NOT a non-null `0`. The prior `unwrap_or(0)`
/// collapse is removed (gap 06): it lied-with-zeros — a `0` ceiling is
/// indistinguishable from a real value and reads as garbage/infinite utilization
/// in spec 09's gauge. The frontend renders `—` on the missing window.
///
/// An upstream catalog-fetch failure yields an empty array (the dashboard simply
/// shows no catalog) rather than a 5xx that would blank the whole view.
#[utoipa::path(
    get,
    path = "/dashboard/api/catalog",
    tag = "dashboard",
    operation_id = "dashboard_catalog",
    responses(
        (
            status = 200,
            description = "Bare array of catalog entries; an empty array when the upstream catalog fetch fails.",
            body = Vec<CatalogEntry>
        ),
        (
            status = 401,
            description = "No valid dashboard session (plain text `unauthorized`).",
            body = String,
            content_type = "text/plain"
        ),
        (
            status = 500,
            description = "Response serialization failed (plain text `failed to serialize response`).",
            body = String,
            content_type = "text/plain"
        ),
    )
)]
pub async fn dashboard_catalog(
    State(gateway): State<Arc<Gateway>>,
    Extension(dashboard_auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    headers: HeaderMap,
) -> Response {
    let access = match crate::http::dashboard_inference_access(
        &gateway,
        &dashboard_auth,
        &session,
        &headers,
    )
    .await
    {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    let entries = match gateway.upstream_client().supported_model_catalog().await {
        Ok(catalog) => catalog
            .into_iter()
            .filter(|entry| access.allows_model(&entry.id))
            .map(|entry| CatalogEntry {
                id: entry.id,
                // Pass the parsed `Option<i64>` THROUGH unchanged: a known window
                // serializes as the integer, an unknown one as absent/null. Do NOT
                // re-collapse to 0 (the gap 06 lie-with-zeros fix).
                context_limit: entry.context_limit,
            })
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    json_no_store(StatusCode::OK, &entries)
}

/// `GET /dashboard/api/providers` — provider-scoped model catalogs, availability
/// schedules, and current capacity. A failed inventory refresh returns an empty
/// list so the rest of the dashboard remains usable while an upstream is down.
#[utoipa::path(
    get,
    path = "/dashboard/api/providers",
    tag = "dashboard",
    responses(
        (status = 200, body = serde_json::Value, description = "Configured provider inventory and current mesh capacity."),
        (status = 401, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn dashboard_providers(State(gateway): State<Arc<Gateway>>) -> Response {
    let providers = gateway
        .upstream_client()
        .provider_inventory()
        .await
        .unwrap_or_default();
    json_no_store(StatusCode::OK, &ProvidersBody { providers })
}

/// `GET /dashboard/api/configured-providers` — dashboard-managed
/// OpenAI-compatible providers. Credentials are never returned.
#[utoipa::path(
    get,
    path = "/dashboard/api/configured-providers",
    tag = "dashboard",
    responses(
        (status = 200, body = serde_json::Value),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 503, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn dashboard_configured_providers(State(gateway): State<Arc<Gateway>>) -> Response {
    let Some(registry) = gateway.managed_providers() else {
        return dashboard_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "SQL storage is required for configured providers",
        );
    };
    match registry.list().await {
        Ok(providers) => json_no_store(
            StatusCode::OK,
            &crate::managed_providers::ConfiguredProvidersBody { providers },
        ),
        Err(err) => dashboard_error(err.status_code(), err.client_message),
    }
}

/// `POST /dashboard/api/configured-providers` — add one provider after
/// server-side `/v1/models` discovery.
#[utoipa::path(
    post,
    path = "/dashboard/api/configured-providers",
    tag = "dashboard",
    request_body(content = crate::managed_providers::CreateConfiguredProviderRequest),
    responses(
        (status = 201, body = serde_json::Value),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 502, body = crate::openapi::DashboardError),
        (status = 503, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn create_configured_provider(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    headers: HeaderMap,
    payload: Result<
        Json<crate::managed_providers::CreateConfiguredProviderRequest>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Response {
    if let Some(response) =
        dashboard_admin_denial(&session).or_else(|| dashboard_mutation_denial(&auth, &headers))
    {
        return response;
    }
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(rejection) => {
            return dashboard_error(
                StatusCode::BAD_REQUEST,
                format!("invalid JSON body, expected {{name, base_url, api_key}}: {rejection}"),
            );
        }
    };
    let Some(registry) = gateway.managed_providers() else {
        return dashboard_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "SQL storage is required for configured providers",
        );
    };
    match registry.add(payload).await {
        Ok(provider) => json_no_store(StatusCode::CREATED, &provider),
        Err(err) => dashboard_error(err.status_code(), err.client_message),
    }
}

/// `PATCH /dashboard/api/configured-providers/{id}` — update discovery and model selection.
#[utoipa::path(
    patch,
    path = "/dashboard/api/configured-providers/{id}",
    tag = "dashboard",
    params(("id" = String, Path)),
    request_body(content = crate::managed_providers::UpdateConfiguredProviderRequest),
    responses(
        (status = 200, body = serde_json::Value),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 503, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn update_configured_provider(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<
        Json<crate::managed_providers::UpdateConfiguredProviderRequest>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Response {
    if let Some(response) =
        dashboard_admin_denial(&session).or_else(|| dashboard_mutation_denial(&auth, &headers))
    {
        return response;
    }
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(rejection) => {
            return dashboard_error(
                StatusCode::BAD_REQUEST,
                format!(
                    "invalid JSON body, expected optional {{auto_discover, allowed_models, disabled_models}}: {rejection}"
                ),
            );
        }
    };
    let Some(registry) = gateway.managed_providers() else {
        return dashboard_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "SQL storage is required for configured providers",
        );
    };
    match registry.update(&id, payload).await {
        Ok(Some(provider)) => json_no_store(StatusCode::OK, &provider),
        Ok(None) => dashboard_error(StatusCode::NOT_FOUND, "configured provider not found"),
        Err(err) => dashboard_error(err.status_code(), err.client_message),
    }
}

/// `DELETE /dashboard/api/configured-providers/{id}` — remove one provider.
#[utoipa::path(
    delete,
    path = "/dashboard/api/configured-providers/{id}",
    tag = "dashboard",
    params(("id" = String, Path)),
    responses(
        (status = 204, description = "Deleted."),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 503, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn delete_configured_provider(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) =
        dashboard_admin_denial(&session).or_else(|| dashboard_mutation_denial(&auth, &headers))
    {
        return response;
    }
    let Some(registry) = gateway.managed_providers() else {
        return dashboard_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "SQL storage is required for configured providers",
        );
    };
    match registry.delete(&id).await {
        Ok(true) => crate::dashboard_auth::no_store(StatusCode::NO_CONTENT.into_response()),
        Ok(false) => dashboard_error(StatusCode::NOT_FOUND, "configured provider not found"),
        Err(err) => dashboard_error(err.status_code(), err.client_message),
    }
}

/// `GET /dashboard/api/snapshot?at=<unix_ms>` — a body-free frozen cut from the D5
/// snapshot ring (`snapshot_at(ts)` nearest ≤ ts, or the latest cut when `at` is
/// absent). Reshapes the cut's metrics ([`MetricsView`]) + topology
/// ([`ProviderHealthSnapshot`]) into their REST bodies and prices the body-free
/// summaries. `200` with empty summaries + `null` metrics/topology + zero cursors
/// when no cut has been taken yet (rather than a 404 the SPA would treat as fatal).
#[utoipa::path(
    get,
    path = "/dashboard/api/snapshot",
    tag = "dashboard",
    operation_id = "dashboard_snapshot",
    params(SnapshotQuery),
    responses(
        (
            status = 200,
            description = "The frozen cut nearest at-or-before `at` (or the latest). When no cut exists: empty `summaries`, `null` `metrics`/`topology`, zero `cursors`.",
            body = SnapshotResponse
        ),
        (status = 400, description = "Unknown or mistyped query parameter (the query string is strict; the text names the field). Plain text from the extractor.", content_type = "text/plain", body = String),
        (
            status = 401,
            description = "No valid dashboard session (plain text `unauthorized`).",
            body = String,
            content_type = "text/plain"
        ),
        (
            status = 500,
            description = "Response serialization failed (plain text `failed to serialize response`).",
            body = String,
            content_type = "text/plain"
        ),
    )
)]
pub async fn dashboard_snapshot(
    State(gateway): State<Arc<Gateway>>,
    Query(query): Query<SnapshotQuery>,
) -> Response {
    // Widen the `u64` query instant to the `u128` `snapshot_at` key (the query
    // deserializer cannot parse `u128`; unix-ms fits `u64`).
    let at_query = query.at.map(u128::from);
    let cut = match at_query {
        Some(at) => gateway.metrics().snapshot_at(at),
        None => gateway.metrics().latest_snapshot(),
    };
    let Some(cut) = cut else {
        // No cut yet (the 5 s task has not run, or every cut is newer than `at`):
        // a contract-valid empty snapshot, not a 404.
        return json_no_store(
            StatusCode::OK,
            &SnapshotResponse {
                cursors: SeqCursors::default(),
                at_ms: at_query.unwrap_or(0),
                summaries: Vec::new(),
                metrics: None,
                topology: None,
            },
        );
    };

    let prices = gateway.price_table();
    let summaries: Vec<FlowRow> = cut
        .summaries
        .iter()
        .map(|summary| FlowRow::from_summary(summary, gateway.as_ref()))
        .collect();
    // Reshape the cut's body-free metrics view into the REST `/metrics` shape, with
    // the cut's own `metrics_seq` cursor. `active_streams` is derived from the FROZEN
    // cut's open summaries (D13 R1 HIGH) — NOT the live FlowStore — so a historical
    // `?at=` reflects how many streams were open AT THAT CUT, not now. The cut's
    // `summaries` are the same body-free flow projections captured in the snapshot's
    // single critical section, so counting `status == Open` among them is consistent
    // with the rest of the frozen cut.
    let active = cut_active_stream_count(&cut.summaries);
    let metrics = Some(metrics_body(
        &cut.metrics,
        cut.cursors.metrics_seq,
        active,
        &prices,
    ));
    let topology = Some(topology_body(
        &cut.topology,
        &prices,
        &cut.metrics.window_1m,
    ));
    json_no_store(
        StatusCode::OK,
        &SnapshotResponse {
            cursors: SeqCursors {
                flow_seq: cut.cursors.flow_seq,
                metrics_seq: cut.cursors.metrics_seq,
                topology_seq: cut.cursors.topology_seq,
                monitor_seq: cut.cursors.monitor_seq,
            },
            at_ms: cut.taken_at_ms,
            summaries,
            metrics,
            topology,
        },
    )
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Whether a record's served OR requested model contains the (lowercased) filter
/// substring — the `model=` filter matches either identity so a row is findable by
/// what the client asked for OR what served it.
fn record_matches_model(record: &FlowRecord, wanted: &str) -> bool {
    record
        .model_served
        .as_deref()
        .is_some_and(|model| model.to_ascii_lowercase().contains(wanted))
        || record
            .model_requested
            .as_deref()
            .is_some_and(|model| model.to_ascii_lowercase().contains(wanted))
}

/// Parse a `status=` filter value into a [`FlowStatus`] (the frozen
/// open/completed/failed/cancelled enum). An unrecognized value yields `None` so
/// the filter is simply ignored (no rows wrongly hidden by a typo).
fn parse_status_filter(value: &str) -> Option<FlowStatus> {
    match value.trim().to_ascii_lowercase().as_str() {
        "open" => Some(FlowStatus::Open),
        "completed" => Some(FlowStatus::Completed),
        "failed" => Some(FlowStatus::Failed),
        "cancelled" => Some(FlowStatus::Cancelled),
        _ => None,
    }
}

/// Apply 1-based `page`/`limit` paging to the filtered rows. Absent `limit` ⇒ all
/// rows (no paging). Absent `page` ⇒ page 1. An out-of-range page yields an empty
/// slice (the SPA shows no rows, with `total` telling it how many exist).
fn apply_paging(rows: Vec<FlowRow>, page: Option<usize>, limit: Option<usize>) -> Vec<FlowRow> {
    let Some(limit) = limit.filter(|limit| *limit > 0) else {
        return rows;
    };
    let page = page.unwrap_or(1).max(1);
    let start = (page - 1).saturating_mul(limit);
    rows.into_iter().skip(start).take(limit).collect()
}

/// Serialize `body` as JSON with the dashboard security headers + `no-store` (D7a):
/// EVERY `/dashboard/api/*` response is uncacheable (auth-scoped, per-request) and
/// carries the locked-down CSP/nosniff/no-referrer/X-Frame-Options set, exactly
/// like the auth-layer responses. A serialization failure (should be unreachable —
/// the DTOs are plain data) degrades to a 500 with the same headers.
pub(crate) fn json_no_store<T: Serialize>(status: StatusCode, body: &T) -> Response {
    let response = match serde_json::to_vec(body) {
        Ok(bytes) => (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to serialize response",
        )
            .into_response(),
    };
    crate::dashboard_auth::no_store(response)
}

// ---------------------------------------------------------------------------
// Active sessions (live hub)
// ---------------------------------------------------------------------------

/// `GET /dashboard/api/sessions/active` — the dashboard's primary live view:
/// every session with activity in the last 15 minutes (the [`SessionHub`] cut),
/// each with its 1/5/10/15-minute request windows, the newest request stubs,
/// and — when durable history is configured — the session's LIFETIME token
/// totals joined from the store. Attribution (`user_id`, `virtual_key_id`,
/// `client_label`) rides the hub's `SessionRow` verbatim.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ActiveSessionsBody {
    /// Newest activity first.
    sessions: Vec<ActiveSessionBody>,
    /// The sessions-domain WS cursor at the cut (monotonic; a live
    /// `session_update` frame with `seq <= this` dedups client-side).
    seq: u64,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ActiveSessionBody {
    #[serde(flatten)]
    session: crate::session_hub::ActiveSession,
    /// Lifetime totals over the node's durable requests; `None` per class
    /// when no row reported it. Absent (null) when history is disabled.
    aggregate: Option<crate::control_plane_store::SessionAggregate>,
}

#[utoipa::path(
    get,
    path = "/dashboard/api/sessions/active",
    tag = "dashboard",
    operation_id = "dashboard_sessions_active",
    responses(
        (status = 200, description = "The active-session cut (last 15 minutes), newest first.", body = ActiveSessionsBody),
        (status = 401, description = "No valid dashboard session (plain text `unauthorized`).", body = String, content_type = "text/plain"),
    )
)]
pub async fn dashboard_sessions_active(State(gateway): State<Arc<Gateway>>) -> Response {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let cut = gateway
        .session_hub()
        .active_sessions(u64::try_from(now_ms).unwrap_or(u64::MAX));
    let seq = gateway.session_hub().last_seq();
    // Join the durable lifetime aggregates when history is configured. The
    // per-session queries are bounded (one per ACTIVE session, and the hub
    // caps actives); a store error degrades that session's aggregate to
    // `None` rather than failing the whole read (don't-lie-with-zeros: absent
    // renders `—`, not a fabricated 0).
    let store = gateway.persistence_store();
    let mut sessions = Vec::with_capacity(cut.len());
    for entry in cut {
        let aggregate = match &store {
            Some(store) => {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    store.session_aggregate(&entry.row.id),
                )
                .await
                {
                    Ok(Ok(aggregate)) => aggregate,
                    Ok(Err(error)) => {
                        tracing::warn!(
                            session_id = %entry.row.id,
                            error = %error,
                            "active-sessions aggregate join failed"
                        );
                        None
                    }
                    Err(_) => None,
                }
            }
            None => None,
        };
        sessions.push(ActiveSessionBody {
            session: entry,
            aggregate,
        });
    }
    json_no_store(StatusCode::OK, &ActiveSessionsBody { sessions, seq })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A price with an EXPLICITLY configured cached rate (presence `true`) — the
    /// default for these cost tests. Confidence-specific tests use
    /// `ModelPrice::without_cached` to exercise the unconfigured-cache path.
    fn price(input: f64, output: f64, cached: f64) -> ModelPrice {
        ModelPrice::new(input, output, cached)
    }

    /// A usage with a REPORTED (measured) cached count and a reported `0` reasoning
    /// (gap 07 `Some` — distinct from the UNAVAILABLE `None` the dedicated tests use).
    fn usage(prompt: i64, completion: i64, cached: i64) -> FlowUsage {
        FlowUsage {
            prompt,
            completion,
            cached: Some(cached),
            reasoning: Some(0),
            total: prompt + completion,
        }
    }

    /// The cost model splits prompt into uncached (input rate) + cached (cache
    /// rate) and bills completion at the output rate. 90 uncached prompt @ 2.0/1k
    /// + 10 cached @ 0.5/1k + 40 completion @ 6.0/1k = 0.18 + 0.005 + 0.24 = 0.425.
    #[test]
    fn cost_for_usage_splits_cached_prompt_and_bills_completion() {
        let cost = cost_for_usage(usage(100, 40, 10), price(2.0, 6.0, 0.5));
        assert!((cost - 0.425).abs() < 1e-9, "cost {cost} == 0.425");
    }

    /// `cached > prompt` (a transient/odd report) never yields a negative input
    /// charge — the uncached prompt floors at 0, so the whole prompt bills at the
    /// (cheaper) cache rate rather than producing a negative number.
    #[test]
    fn cost_for_usage_clamps_cached_over_prompt() {
        let cost = cost_for_usage(usage(10, 0, 50), price(2.0, 6.0, 0.5));
        // uncached = max(10 - 50, 0) = 0; cached billed = 50/1000*0.5 = 0.025.
        assert!(
            (cost - 0.025).abs() < 1e-9,
            "cost {cost} == 0.025 (no negative)"
        );
    }

    /// A degenerate configured price (an absurd magnitude that overflows to ∞, or a
    /// NaN) must NOT yield a non-finite cost — `serde_json` errors on NaN/±∞ and
    /// would 500 the read. `cost_for_usage` collapses a non-finite result to `0.0`
    /// (the `finite` guard), so the JSON stays well-formed.
    #[test]
    fn cost_for_usage_is_finite_even_for_overflowing_prices() {
        // 1e9 tokens × an f64::MAX per-1k rate overflows the product to +inf.
        let cost = cost_for_usage(
            usage(1_000_000_000, 1_000_000_000, 0),
            price(f64::MAX, f64::MAX, 0.0),
        );
        assert!(
            cost.is_finite(),
            "an overflowing price must not produce ±inf cost"
        );
        // A NaN-producing price likewise sanitizes to a finite value.
        assert!(cost_for_usage(usage(1, 1, 0), price(f64::NAN, 1.0, 0.0)).is_finite());
    }

    /// A model with no configured price contributes no cost to a window roll-up
    /// (it is simply skipped — never a fabricated zero that would understate the
    /// per-1k rate of the priced buckets).
    #[test]
    fn price_lookup_is_exact_then_case_insensitive() {
        let mut prices = HashMap::new();
        prices.insert("GLM-5.1".to_string(), price(1.0, 2.0, 0.0));
        assert!(price_lookup(&prices, "glm-5.1").is_some());
        assert!(price_lookup(&prices, "other").is_none());
    }

    /// Gap 01 (the honest strip): a window fed by a real TERMINAL flow reports a
    /// non-zero `samples` count AND real `tokens_per_sec`/`cost_per_min` (priced) +
    /// the passed-in live `active_streams` — NOT the hard-coded zeros the live WS
    /// tile used to ship. This is the "a live-flow is counted in the strip" proof,
    /// exercised through the SAME `metrics_body` builder the live tick now uses.
    #[test]
    fn metrics_body_counts_a_terminal_flow_with_real_rates() {
        use crate::dashboard_flow::FlowStatus as FS;
        use crate::metrics::MetricsLayer;
        let metrics = MetricsLayer::new();
        // One completed flow on a priced model: 1000 prompt + 500 completion tokens.
        metrics.record_terminal_with_phases(
            FS::Completed,
            Some("glm-5.1"),
            "/v1/responses",
            Some("vllm-a"),
            1200,
            Some(usage(1000, 500, 0)),
            Some(100),
            Some(2000),
            &[],
        );
        let (view, seq) = metrics.view_with_seq();
        let mut prices = HashMap::new();
        prices.insert("glm-5.1".to_string(), price(2.0, 6.0, 0.5));
        // Live open-flow count threaded in (3 streams currently in flight).
        let body = metrics_body(&view, seq, 3, &prices);

        // The terminal flow IS counted — the measured/unavailable signal is non-zero.
        assert_eq!(body.samples, 1, "the finalized flow counts as one sample");
        assert_eq!(body.windows.m1.samples, 1);
        // The flow reported usage on a PRICED model → both per-metric denominators are
        // non-zero (gap 01 finding 3): tok/s and $/min are both measurable here.
        assert_eq!(
            body.usage_samples, 1,
            "the usage-bearing flow is a usage sample"
        );
        assert_eq!(body.windows.m1.usage_samples, 1);
        assert_eq!(body.priced_samples, 1, "priced model → a priced sample");
        assert_eq!(body.windows.m1.priced_samples, 1);
        // active_streams carries the live count (was hard-coded 0 on the WS tile).
        assert_eq!(body.active_streams, 3, "live open-flow count is carried");
        // tokens/s + cost/min are REAL (priced), not 0.0. 1500 tok / 60 s = 25 tok/s.
        assert!(
            (body.tokens_per_sec - 25.0).abs() < 1e-9,
            "tok/s {} == 1500/60",
            body.tokens_per_sec
        );
        assert!(
            (body.prefill_tokens_per_sec - 10_000.0).abs() < 1e-9,
            "prefill tok/s {} == 1000/0.1s",
            body.prefill_tokens_per_sec
        );
        assert!(
            (body.decode_tokens_per_sec - 250.0).abs() < 1e-9,
            "decode tok/s {} == 500/2s",
            body.decode_tokens_per_sec
        );
        // cost = 1000 prompt @2.0/1k + 500 completion @6.0/1k = 2.0 + 3.0 = 5.0 over
        // 1 minute → cost_per_min ≈ 5.0.
        assert!(
            (body.cost_per_min - 5.0).abs() < 1e-9,
            "cost/min {} == 5.0",
            body.cost_per_min
        );
        // req/s is a TRUE per-second rate (1 req / 60 s), not the raw count.
        assert!(
            (body.reqs_per_sec - (1.0 / 60.0)).abs() < 1e-9,
            "req/s {} == 1/60",
            body.reqs_per_sec
        );
    }

    /// Gap 01 (don't lie with zeros): an EMPTY window (no finalized flow) reports
    /// `samples == 0` — the signal the frontend reads to render latency/tok-s/cost as
    /// `unavailable` (`—`) — while `reqs_per_sec` stays a genuine measured `0` (legit
    /// zero traffic). The two cases are thus distinguishable on the wire.
    #[test]
    fn metrics_body_empty_window_reports_zero_samples_not_a_fake_zero() {
        use crate::metrics::MetricsView;
        let body = metrics_body(&MetricsView::default(), 0, 0, &HashMap::new());
        assert_eq!(
            body.samples, 0,
            "no finalized flow → zero samples (unavailable)"
        );
        assert_eq!(body.windows.m1.samples, 0);
        assert_eq!(body.windows.m5.samples, 0);
        assert_eq!(body.windows.h1.samples, 0);
        // The per-metric denominators are zero too → tok/s + $/min are unavailable.
        assert_eq!(body.usage_samples, 0);
        assert_eq!(body.priced_samples, 0);
        assert_eq!(body.windows.m1.usage_samples, 0);
        assert_eq!(body.windows.m1.priced_samples, 0);
        // req/s is a genuine measured zero (idle), distinguishable from the unavailable
        // latency/tok-s/cost above precisely BECAUSE samples == 0.
        assert_eq!(body.reqs_per_sec, 0.0);
        assert_eq!(body.tokens_per_sec, 0.0);
        assert_eq!(body.cost_per_min, 0.0);
    }

    /// Gap 01 finding 3 (per-metric availability): a window can have measured LATENCY
    /// (`samples > 0`) yet UNMEASURABLE tokens/cost. Two terminal flows finalize — one
    /// with usage on a PRICED model, one with NO usage at all and one with usage on an
    /// UNPRICED model — so `samples` (latency) and the two token/cost denominators
    /// diverge. The frontend reads each denominator independently to decide `—` vs a
    /// number, so this asserts they are emitted independently and correctly.
    #[test]
    fn metrics_body_per_metric_denominators_diverge() {
        use crate::dashboard_flow::FlowStatus as FS;
        use crate::metrics::MetricsLayer;
        let metrics = MetricsLayer::new();
        // (a) usage on a PRICED model → counts toward samples + usage + priced.
        metrics.record_terminal_with_phases(
            FS::Completed,
            Some("glm-5.1"),
            "/v1/responses",
            Some("vllm-a"),
            900,
            Some(usage(1000, 500, 0)),
            Some(100),
            Some(2000),
            &[],
        );
        // (b) NO usage (e.g. an upstream that omitted it) → samples only.
        metrics.record_terminal(
            FS::Completed,
            Some("glm-5.1"),
            "/v1/responses",
            Some("vllm-a"),
            900,
            None,
            &[],
        );
        // (c) usage on an UNPRICED model → samples + usage, but NOT priced.
        metrics.record_terminal(
            FS::Completed,
            Some("free-model"),
            "/v1/responses",
            Some("vllm-a"),
            900,
            Some(usage(10, 5, 0)),
            &[],
        );
        let (view, seq) = metrics.view_with_seq();
        let mut prices = HashMap::new();
        prices.insert("glm-5.1".to_string(), price(2.0, 6.0, 0.5));
        let body = metrics_body(&view, seq, 0, &prices);

        // Latency is measurable for all three finalized flows.
        assert_eq!(
            body.samples, 3,
            "three finalized flows → latency measurable"
        );
        // Two of the three reported usage → tok/s measurable, but distinct from samples.
        assert_eq!(
            body.usage_samples, 2,
            "two usage-bearing flows → tok/s measurable (≠ samples)"
        );
        // Only one of those two is on a priced model → cost measurable for exactly one.
        assert_eq!(
            body.priced_samples, 1,
            "only the priced-model usage flow → $/min measurable (≠ usage_samples)"
        );
        // The headline mirrors the m1 window's per-metric denominators.
        assert_eq!(body.windows.m1.samples, 3);
        assert_eq!(body.windows.m1.usage_samples, 2);
        assert_eq!(body.windows.m1.priced_samples, 1);
    }

    /// Round-trip (AGENTS.md: no new wire fields without a round-trip test): the new
    /// `usage_samples`/`priced_samples` wire fields survive a serialize → JSON → re-parse
    /// cycle at BOTH the headline and the per-window level, with the exact values the
    /// `metrics_body` builder produced. This pins the byte contract the frozen frontend
    /// validators (`isMetricWindow`/`isMetricsResponse`) decode.
    #[test]
    fn metrics_body_new_sample_fields_round_trip_through_json() {
        use crate::dashboard_flow::FlowStatus as FS;
        use crate::metrics::MetricsLayer;
        let metrics = MetricsLayer::new();
        metrics.record_terminal_with_phases(
            FS::Completed,
            Some("glm-5.1"),
            "/v1/responses",
            Some("vllm-a"),
            900,
            Some(usage(1000, 500, 0)),
            Some(100),
            Some(2000),
            &[],
        );
        let (view, seq) = metrics.view_with_seq();
        let mut prices = HashMap::new();
        prices.insert("glm-5.1".to_string(), price(2.0, 6.0, 0.5));
        let body = metrics_body(&view, seq, 2, &prices);

        // Serialize → JSON bytes → re-parse: the fields must survive intact.
        let json = serde_json::to_string(&body).expect("serialize metrics body");
        let value: serde_json::Value = serde_json::from_str(&json).expect("re-parse");
        // Headline mirrors.
        assert_eq!(value["usage_samples"], serde_json::json!(1));
        assert_eq!(value["priced_samples"], serde_json::json!(1));
        assert_eq!(value["prefill_tokens_per_sec"], serde_json::json!(10_000.0));
        assert_eq!(value["decode_tokens_per_sec"], serde_json::json!(250.0));
        // Per-window (m1 fed the terminal; m5/h1 share the same epoch ⇒ same counts).
        for window in ["m1", "m5", "h1"] {
            assert_eq!(value["windows"][window]["samples"], serde_json::json!(1));
            assert_eq!(
                value["windows"][window]["prefill_tokens_per_sec"],
                serde_json::json!(10_000.0)
            );
            assert_eq!(
                value["windows"][window]["decode_tokens_per_sec"],
                serde_json::json!(250.0)
            );
            assert_eq!(
                value["windows"][window]["usage_samples"],
                serde_json::json!(1)
            );
            assert_eq!(
                value["windows"][window]["priced_samples"],
                serde_json::json!(1)
            );
        }
    }

    /// 1-based paging: page 2 with limit 2 over 5 rows yields rows 3..=4; a limit
    /// of 0 (or absent) returns all rows; an out-of-range page yields empty.
    #[test]
    fn apply_paging_pages_1_based() {
        let rows = |n: usize| -> Vec<FlowRow> {
            (0..n)
                .map(|i| FlowRow {
                    session: Default::default(),
                    api_call_id: format!("api_{i}"),
                    response_id: None,
                    method: "POST".to_string(),
                    uri: "/v1/responses".to_string(),
                    model_requested: None,
                    model_served: None,
                    upstream_target: None,
                    usage: None,
                    status: FlowStatus::Completed,
                    started_ms: 0,
                    finished_ms: None,
                    elapsed_ms: None,
                    terminal_reason: None,
                    client_label: None,
                    client_source: None,
                    cost: None,
                    cost_confidence: CostConfidence::Unavailable,
                    phases: PhaseTimings::default(),
                    attempts: Vec::new(),
                    first_upstream_byte_ms: None,
                })
                .collect()
        };
        let page2 = apply_paging(rows(5), Some(2), Some(2));
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].api_call_id, "api_2");
        assert_eq!(page2[1].api_call_id, "api_3");
        // No limit ⇒ all rows.
        assert_eq!(apply_paging(rows(5), None, None).len(), 5);
        // Out-of-range page ⇒ empty.
        assert!(apply_paging(rows(3), Some(9), Some(2)).is_empty());
    }

    /// Gap 04 review F3: `FlowRow` carries the OPTIONAL `client_label`/`client_source`
    /// attribution fields, serialized with `skip_serializing_if` so a PRESENT pair emits
    /// the snake_case keys (with the key-hash label + `key_hash` source) while an ABSENT
    /// pair OMITS both keys entirely (never `null`/empty-string-as-id). This pins the
    /// `/flows` + `/snapshot` summary wire contract for the new fields.
    #[test]
    fn flow_session_facts_round_trip_and_project_present_and_absent() {
        use crate::dashboard_flow::FlowSessionFacts;
        let facts = FlowSessionFacts {
            harness: Some("claude-code".to_string()),
            harness_version: Some("2.1.205".to_string()),
            session_id: Some("node-1".to_string()),
            chain_parent_request_id: Some("api_prev".to_string()),
            divergence_kind: Some("tools_changed".to_string()),
            cache_bust: Some(true),
            user_id: Some("user-7".to_string()),
            virtual_key_id: Some("vk-1".to_string()),
        };
        // AGENTS.md: every new wire field proves it survives a round trip.
        let encoded = serde_json::to_string(&facts).unwrap();
        let decoded: FlowSessionFacts = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, facts);

        let base = || FlowRow {
            session: Default::default(),
            api_call_id: "api_x".to_string(),
            response_id: None,
            method: "POST".to_string(),
            uri: "/v1/responses".to_string(),
            model_requested: None,
            model_served: None,
            upstream_target: None,
            usage: None,
            status: FlowStatus::Open,
            started_ms: 0,
            finished_ms: None,
            elapsed_ms: None,
            terminal_reason: None,
            client_label: None,
            client_source: None,
            cost: None,
            cost_confidence: CostConfidence::Unavailable,
            phases: PhaseTimings::default(),
            attempts: Vec::new(),
            first_upstream_byte_ms: None,
        };
        let mut row = base();
        row.session = facts;
        let value = serde_json::to_value(&row).unwrap();
        assert_eq!(value["harness"], "claude-code");
        assert_eq!(value["session_id"], "node-1");
        assert_eq!(value["divergence_kind"], "tools_changed");
        assert_eq!(value["cache_bust"], true);
        assert_eq!(value["chain_parent_request_id"], "api_prev");
        // Absent facts are omitted, never null/false.
        let value = serde_json::to_value(base()).unwrap();
        let object = value.as_object().unwrap();
        for key in [
            "harness",
            "harness_version",
            "session_id",
            "chain_parent_request_id",
            "divergence_kind",
            "cache_bust",
        ] {
            assert!(
                !object.contains_key(key),
                "{key} must be absent when unknown"
            );
        }
    }

    #[test]
    fn flow_row_serializes_optional_client_attribution_present_and_absent() {
        let base = || FlowRow {
            session: Default::default(),
            api_call_id: "api_x".to_string(),
            response_id: None,
            method: "POST".to_string(),
            uri: "/v1/responses".to_string(),
            model_requested: None,
            model_served: None,
            upstream_target: None,
            usage: None,
            status: FlowStatus::Open,
            started_ms: 0,
            finished_ms: None,
            elapsed_ms: None,
            terminal_reason: None,
            client_label: None,
            client_source: None,
            cost: None,
            cost_confidence: CostConfidence::Unavailable,
            phases: PhaseTimings::default(),
            attempts: Vec::new(),
            first_upstream_byte_ms: None,
        };

        // PRESENT: a key-hash attribution emits both snake_case keys with the expected
        // values (the label is a `key-<hex>` id — a one-way prefix, never a raw key).
        let present = FlowRow {
            client_label: Some("key-deadbeef0123".to_string()),
            client_source: Some(ClientSource::KeyHash),
            ..base()
        };
        let value = serde_json::to_value(&present).expect("serialize present row");
        assert_eq!(value["client_label"], serde_json::json!("key-deadbeef0123"));
        assert_eq!(value["client_source"], serde_json::json!("key_hash"));

        // ABSENT: an unattributed row OMITS both keys (skip_serializing_if), so the
        // frontend sees no key rather than a fabricated `null`/`0`.
        let absent = serde_json::to_value(base()).expect("serialize absent row");
        let obj = absent.as_object().expect("object");
        assert!(
            !obj.contains_key("client_label"),
            "absent label key omitted: {absent}"
        );
        assert!(
            !obj.contains_key("client_source"),
            "absent source key omitted: {absent}"
        );
    }

    /// Gap 05 review F2: `FlowDetailBody` carries the OPTIONAL `upstream_response`
    /// (the captured upstream RESPONSE/ERROR body + `truncated`) on the LIVE detail path.
    /// This pins the new wire field with a serialize → deserialize ROUND-TRIP (AGENTS.md:
    /// no new wire field without a round-trip proof). The enclosing `FlowDetailBody` is
    /// serialize-only (it is only ever a response), so the round-trip is on the
    /// self-contained `FlowUpstreamResponse` sub-DTO: we SERIALIZE the whole detail body
    /// (proving the field is wired in + that `skip_serializing_if` OMITS it when absent),
    /// then DESERIALIZE the `upstream_response` value back into `FlowUpstreamResponse` and
    /// assert it survives. Covers PRESENT with `truncated` false AND true, and ABSENT.
    #[test]
    fn flow_detail_body_upstream_response_round_trips_present_and_absent() {
        let base = || FlowDetailBody {
            session: Default::default(),
            flow_seq: 7,
            api_call_id: "api_d".to_string(),
            response_id: Some("resp_d".to_string()),
            inbound_body: None,
            inbound_headers: None,
            normalized: None,
            upstream_body: None,
            upstream_response: None,
            model_requested: None,
            model_served: None,
            upstream_target: None,
            usage: None,
            status: FlowStatus::Failed,
            deltas: Vec::new(),
            terminal_reason: None,
            started_ms: 1,
            finished_ms: None,
            elapsed_ms: None,
            cost: None,
            cost_confidence: CostConfidence::Unavailable,
            phases: PhaseTimings::default(),
            attempts: Vec::new(),
            first_upstream_byte_ms: None,
        };

        // PRESENT (not truncated): a JSON error body survives the round-trip intact,
        // with `truncated: false`. Serialize the whole detail body, then deserialize the
        // sub-object back into the typed DTO.
        let present = FlowDetailBody {
            upstream_response: Some(FlowUpstreamResponse {
                body: serde_json::json!({"error": {"message": "backend on fire"}}),
                truncated: false,
            }),
            ..base()
        };
        let value = serde_json::to_value(&present).expect("serialize present detail");
        assert_eq!(
            value["upstream_response"]["body"]["error"]["message"],
            serde_json::json!("backend on fire")
        );
        assert_eq!(
            value["upstream_response"]["truncated"],
            serde_json::json!(false)
        );
        let rt: FlowUpstreamResponse = serde_json::from_value(value["upstream_response"].clone())
            .expect("deserialize present upstream_response");
        assert_eq!(
            rt.body,
            serde_json::json!({"error": {"message": "backend on fire"}}),
            "the captured body survives serialize → deserialize"
        );
        assert!(!rt.truncated, "truncated false survives the round-trip");

        // PRESENT (truncated): a cap-truncated body keeps `truncated: true` across the
        // round-trip so the dashboard can flag a PARTIAL body.
        let truncated = FlowDetailBody {
            upstream_response: Some(FlowUpstreamResponse {
                body: serde_json::Value::String("partial prefix…".to_string()),
                truncated: true,
            }),
            ..base()
        };
        let value = serde_json::to_value(&truncated).expect("serialize truncated detail");
        assert_eq!(
            value["upstream_response"]["truncated"],
            serde_json::json!(true)
        );
        let rt: FlowUpstreamResponse = serde_json::from_value(value["upstream_response"].clone())
            .expect("deserialize truncated upstream_response");
        assert!(rt.truncated, "truncated true survives the round-trip");
        assert_eq!(rt.body, serde_json::json!("partial prefix…"));

        // ABSENT: no captured body OMITS the key entirely (skip_serializing_if), never
        // a `null`.
        let value = serde_json::to_value(base()).expect("serialize absent detail");
        assert!(
            !value
                .as_object()
                .expect("object")
                .contains_key("upstream_response"),
            "absent upstream_response key omitted (not null): {value}"
        );
    }

    /// Gap 10b — a measured attempt the spine-projection tests reuse: a SERVED attempt
    /// with a wire first byte (so `first_upstream_byte_ms` is `Some`) and no error
    /// class/failover reason (the success case). Body-free scalar provenance only.
    fn served_attempt() -> Attempt {
        Attempt {
            provider: Some("vllm-a".to_string()),
            model: Some("llama-3.1-70b".to_string()),
            start_ms: 1_000,
            end_ms: 1_220,
            first_upstream_byte_ms: Some(1_220),
            status: crate::dashboard_flow::AttemptStatus::Served,
            error_class: None,
            failover_reason: None,
        }
    }

    /// Gap 10b — a measured phase bundle the spine-projection tests reuse: every phase
    /// stamped (a fully-completed flow), monotonic. The unit `0` is never used as an
    /// epoch (a real wall-clock stamp is large), so a present value unambiguously means
    /// "this phase ran".
    fn measured_phases() -> PhaseTimings {
        PhaseTimings {
            ingress_ms: Some(1_000),
            normalization_done_ms: Some(1_030),
            routing_decision_ms: Some(1_050),
            first_content_delta_ms: Some(1_500),
            stream_end_ms: Some(5_320),
            finalize_ms: Some(5_340),
        }
    }

    /// Gap 10b — `FlowRow` (the `/flows` list + `/snapshot` summary row) PROJECTS the
    /// gap-02 phases (flattened) + gap-03 attempts + `first_upstream_byte_ms`. This pins
    /// the row wire contract for the spine fields: a measured flow EMITS the flattened
    /// phase scalars (`ingress_ms`/`first_content_delta_ms`/…), the `attempts` array (with
    /// the served attempt's wire first byte), and `first_upstream_byte_ms`; an unmeasured
    /// flow OMITS every spine key (don't-lie-with-zeros: absent, never `0`). The flattened
    /// `PhaseTimings` + each `Attempt` are deserialized BACK into their (Deserialize) DTOs
    /// to prove the round-trip survives (AGENTS.md: no new wire field without a round-trip).
    #[test]
    fn flow_row_projects_spine_fields_present_and_absent() {
        let base = || FlowRow {
            session: Default::default(),
            api_call_id: "api_s".to_string(),
            response_id: None,
            method: "POST".to_string(),
            uri: "/v1/responses".to_string(),
            model_requested: None,
            model_served: None,
            upstream_target: None,
            usage: None,
            status: FlowStatus::Completed,
            started_ms: 1_000,
            finished_ms: None,
            elapsed_ms: None,
            terminal_reason: None,
            client_label: None,
            client_source: None,
            cost: None,
            cost_confidence: CostConfidence::Unavailable,
            phases: PhaseTimings::default(),
            attempts: Vec::new(),
            first_upstream_byte_ms: None,
        };

        // PRESENT: a measured flow projects the flattened phases + attempts + wire TTFB.
        let present = FlowRow {
            phases: measured_phases(),
            attempts: vec![served_attempt()],
            first_upstream_byte_ms: Some(1_220),
            ..base()
        };
        let value = serde_json::to_value(&present).expect("serialize present row");
        // Phases are FLATTENED as sibling scalars on the row (not nested).
        assert_eq!(value["ingress_ms"], serde_json::json!(1_000));
        assert_eq!(value["first_content_delta_ms"], serde_json::json!(1_500));
        assert_eq!(value["finalize_ms"], serde_json::json!(5_340));
        assert_eq!(value["first_upstream_byte_ms"], serde_json::json!(1_220));
        // The attempt survives a round-trip back into the typed `Attempt`.
        let attempts: Vec<Attempt> =
            serde_json::from_value(value["attempts"].clone()).expect("deserialize attempts");
        assert_eq!(attempts.len(), 1);
        assert_eq!(
            attempts[0].status,
            crate::dashboard_flow::AttemptStatus::Served
        );
        assert_eq!(attempts[0].first_upstream_byte_ms, Some(1_220));
        // The flattened phases survive a round-trip back into `PhaseTimings`.
        let phases: PhaseTimings =
            serde_json::from_value(value.clone()).expect("deserialize flattened phases");
        assert_eq!(phases, measured_phases());

        // ABSENT: an unmeasured flow OMITS every spine key — no flattened phase scalar, no
        // `attempts`, no `first_upstream_byte_ms` (don't-lie-with-zeros: absent, never `0`).
        let value = serde_json::to_value(base()).expect("serialize absent row");
        let obj = value.as_object().expect("object");
        for key in [
            "ingress_ms",
            "normalization_done_ms",
            "routing_decision_ms",
            "first_content_delta_ms",
            "stream_end_ms",
            "finalize_ms",
            "attempts",
            "first_upstream_byte_ms",
        ] {
            assert!(
                !obj.contains_key(key),
                "absent spine key {key} omitted (not 0/null): {value}"
            );
        }
    }

    /// Gap 10b — `FlowDetailBody` (the `/flows/:id` inspector) PROJECTS the FULL gap-02
    /// phase set (flattened) + the gap-03 attempts + `first_upstream_byte_ms` (this is where
    /// gap 10's waterfall + gap 11's attempt stepper live). Same present/absent + round-trip
    /// proof as the row, on the detail DTO.
    #[test]
    fn flow_detail_body_projects_spine_fields_present_and_absent() {
        let base = || FlowDetailBody {
            session: Default::default(),
            flow_seq: 3,
            api_call_id: "api_sd".to_string(),
            response_id: None,
            inbound_body: None,
            inbound_headers: None,
            normalized: None,
            upstream_body: None,
            upstream_response: None,
            model_requested: None,
            model_served: None,
            upstream_target: None,
            usage: None,
            status: FlowStatus::Completed,
            deltas: Vec::new(),
            terminal_reason: None,
            started_ms: 1_000,
            finished_ms: None,
            elapsed_ms: None,
            cost: None,
            cost_confidence: CostConfidence::Unavailable,
            phases: PhaseTimings::default(),
            attempts: Vec::new(),
            first_upstream_byte_ms: None,
        };

        // PRESENT: the inspector carries the measured waterfall + attempt trace.
        let present = FlowDetailBody {
            phases: measured_phases(),
            attempts: vec![served_attempt()],
            first_upstream_byte_ms: Some(1_220),
            ..base()
        };
        let value = serde_json::to_value(&present).expect("serialize present detail");
        assert_eq!(value["ingress_ms"], serde_json::json!(1_000));
        assert_eq!(value["stream_end_ms"], serde_json::json!(5_320));
        assert_eq!(value["first_upstream_byte_ms"], serde_json::json!(1_220));
        let attempts: Vec<Attempt> =
            serde_json::from_value(value["attempts"].clone()).expect("deserialize attempts");
        assert_eq!(attempts, vec![served_attempt()]);
        let phases: PhaseTimings =
            serde_json::from_value(value.clone()).expect("deserialize flattened phases");
        assert_eq!(phases, measured_phases());

        // ABSENT: an errored-before-content flow omits the spine keys it never measured.
        let value = serde_json::to_value(base()).expect("serialize absent detail");
        let obj = value.as_object().expect("object");
        for key in [
            "ingress_ms",
            "first_content_delta_ms",
            "attempts",
            "first_upstream_byte_ms",
        ] {
            assert!(
                !obj.contains_key(key),
                "absent spine key {key} omitted on detail (not 0/null): {value}"
            );
        }
    }

    /// The `status=` filter parses the frozen open/completed/failed/cancelled enum
    /// and ignores an unrecognized value (a typo hides no rows).
    #[test]
    fn parse_status_filter_matches_the_frozen_enum() {
        assert_eq!(parse_status_filter("open"), Some(FlowStatus::Open));
        assert_eq!(
            parse_status_filter("CANCELLED"),
            Some(FlowStatus::Cancelled)
        );
        assert_eq!(parse_status_filter("bogus"), None);
    }

    /// Gap 07 — don't-lie-with-zeros for usage: an UNREPORTED (`None`) cached/reasoning
    /// class is ABSENT on the wire (never a fabricated `0`), and a provider-REPORTED `0`
    /// is a present, measured `0` — the two are DISTINCT. Round-trips the changed
    /// `FlowUsage` field through serialize → JSON (AGENTS.md: no changed wire field
    /// without a proof) at the `FlowRow` projection the frontend reads.
    #[test]
    fn flow_usage_unreported_class_is_absent_measured_zero_is_present() {
        let row = |cached: Option<i64>, reasoning: Option<i64>| FlowRow {
            session: Default::default(),
            api_call_id: "api_u".to_string(),
            response_id: None,
            method: "POST".to_string(),
            uri: "/v1/responses".to_string(),
            model_requested: None,
            model_served: None,
            upstream_target: None,
            usage: Some(FlowUsage {
                prompt: 100,
                completion: 40,
                total: 140,
                cached,
                reasoning,
            }),
            status: FlowStatus::Completed,
            started_ms: 0,
            finished_ms: None,
            elapsed_ms: None,
            terminal_reason: None,
            client_label: None,
            client_source: None,
            cost: None,
            cost_confidence: CostConfidence::Unavailable,
            phases: PhaseTimings::default(),
            attempts: Vec::new(),
            first_upstream_byte_ms: None,
        };

        // UNREPORTED cached + reasoning ⇒ both keys OMITTED on the usage object (the
        // frontend renders `—`), never a fake `0`. prompt/completion/total stay present.
        let value = serde_json::to_value(row(None, None)).expect("serialize unreported");
        let usage = value["usage"].as_object().expect("usage object");
        assert!(
            !usage.contains_key("cached"),
            "unreported cached is ABSENT (unavailable), not 0: {usage:?}"
        );
        assert!(
            !usage.contains_key("reasoning"),
            "unreported reasoning is ABSENT (unavailable), not 0"
        );
        assert_eq!(usage["prompt"], serde_json::json!(100));
        assert_eq!(usage["total"], serde_json::json!(140));

        // A provider-REPORTED 0 is a PRESENT measured `0` — DISTINCT from absent.
        let value = serde_json::to_value(row(Some(0), Some(0))).expect("serialize zero");
        let usage = value["usage"].as_object().expect("usage object");
        assert_eq!(
            usage["cached"],
            serde_json::json!(0),
            "a reported cached=0 is a present measured 0 (≠ unavailable)"
        );
        assert_eq!(usage["reasoning"], serde_json::json!(0));
    }

    /// Gap 07 — cost CONFIDENCE tier rules (spec 07 acceptance). Reuses `cost_confidence`
    /// directly so each branch is pinned:
    /// - unpriced ⇒ `unavailable` (cost is `None`, never a fabricated 0);
    /// - priced + reported `cached = 0` ⇒ `confident` even with NO configured cache rate;
    /// - priced + `cached > 0`/UNREPORTED + NO configured cache rate ⇒ `estimated`;
    /// - priced + `cached > 0`/UNREPORTED + a CONFIGURED cache rate ⇒ `confident`.
    #[test]
    fn cost_confidence_tiers_match_the_spec() {
        let priced_no_cache = ModelPrice::without_cached(2.0, 6.0);
        let priced_with_cache = ModelPrice::new(2.0, 6.0, 0.5);
        let some = |cached: Option<i64>| {
            Some(FlowUsage {
                prompt: 100,
                completion: 40,
                total: 140,
                cached,
                reasoning: Some(0),
            })
        };

        // Unpriced ⇒ unavailable regardless of usage.
        assert_eq!(
            cost_confidence(None, some(Some(10))),
            CostConfidence::Unavailable
        );
        // Priced + reported cached = 0 ⇒ confident (nothing bills at the cache rate),
        // even though this price has NO configured cache rate.
        assert_eq!(
            cost_confidence(Some(priced_no_cache), some(Some(0))),
            CostConfidence::Confident,
            "a reported cached=0 stays confident"
        );
        // Priced + cached > 0 + NO configured cache rate ⇒ estimated (those tokens would
        // silently bill at the default 0.0).
        assert_eq!(
            cost_confidence(Some(priced_no_cache), some(Some(10))),
            CostConfidence::Estimated,
            "cached>0 with no configured cache rate ⇒ estimated"
        );
        // Priced + UNREPORTED cached + NO configured cache rate ⇒ estimated.
        assert_eq!(
            cost_confidence(Some(priced_no_cache), some(None)),
            CostConfidence::Estimated,
            "unreported cached with no configured cache rate ⇒ estimated"
        );
        // Priced + cached > 0 + a CONFIGURED cache rate ⇒ confident (priced honestly).
        assert_eq!(
            cost_confidence(Some(priced_with_cache), some(Some(10))),
            CostConfidence::Confident,
            "a configured cache rate prices cached>0 confidently"
        );
        // Priced + UNREPORTED cached + a CONFIGURED cache rate ⇒ still confident.
        assert_eq!(
            cost_confidence(Some(priced_with_cache), some(None)),
            CostConfidence::Confident
        );
    }

    /// Gap 07 — the AGGREGATE window cost confidence is `estimated` if ANY priced bucket
    /// would silently bill cached at the default `0.0`: a window with one priced flow
    /// whose cached was UNREPORTED (against a model with no configured cache rate) reports
    /// `cost_confidence: estimated` even though the summed `cached_tokens == 0` — no
    /// silently-confident total. A window with only a reported-cached-0 priced flow stays
    /// `confident`; an unpriced-only window is `unavailable`.
    #[test]
    fn window_cost_confidence_aggregates_estimated() {
        use crate::dashboard_flow::FlowStatus as FS;
        use crate::metrics::MetricsLayer;
        let mut prices = HashMap::new();
        prices.insert("priced".to_string(), ModelPrice::without_cached(2.0, 6.0));

        // (a) one priced flow with UNREPORTED cached → estimated aggregate.
        let metrics = MetricsLayer::new();
        metrics.record_terminal(
            FS::Completed,
            Some("priced"),
            "/v1/responses",
            Some("vllm-a"),
            900,
            Some(FlowUsage {
                prompt: 1000,
                completion: 500,
                total: 1500,
                cached: None, // unreported
                reasoning: Some(0),
            }),
            &[],
        );
        let body = metrics_body(&metrics.view_with_seq().0, 0, 0, &prices);
        assert_eq!(
            body.cost_confidence,
            CostConfidence::Estimated,
            "unreported cached on a no-cache-rate model ⇒ estimated aggregate (summed cached==0)"
        );
        assert_eq!(body.windows.m1.cost_confidence, CostConfidence::Estimated);

        // (b) one priced flow with a REPORTED cached=0 → confident aggregate.
        let metrics = MetricsLayer::new();
        metrics.record_terminal(
            FS::Completed,
            Some("priced"),
            "/v1/responses",
            Some("vllm-a"),
            900,
            Some(FlowUsage {
                prompt: 1000,
                completion: 500,
                total: 1500,
                cached: Some(0), // reported zero
                reasoning: Some(0),
            }),
            &[],
        );
        let body = metrics_body(&metrics.view_with_seq().0, 0, 0, &prices);
        assert_eq!(
            body.cost_confidence,
            CostConfidence::Confident,
            "a reported cached=0 keeps the aggregate confident"
        );

        // (c) an unpriced-only window ⇒ unavailable (cost_per_min renders —).
        let metrics = MetricsLayer::new();
        metrics.record_terminal(
            FS::Completed,
            Some("free"),
            "/v1/responses",
            Some("vllm-a"),
            900,
            Some(usage(10, 5, 0)),
            &[],
        );
        let body = metrics_body(&metrics.view_with_seq().0, 0, 0, &prices);
        assert_eq!(body.cost_confidence, CostConfidence::Unavailable);
    }

    /// Gap 07 review round 1, finding 2 — a MIXED window (one CONFIDENT priced bucket
    /// PLUS one UNPRICED usage-bearing bucket) is `estimated`, NOT `confident`: the
    /// unpriced bucket's real spend is OMITTED from `cost_per_min`, so the total is a
    /// PARTIAL undercount of the window's true cost — a silently-confident total the
    /// spec forbids. It stays `estimated` (not `unavailable`) because the priced bucket
    /// makes `cost_per_min` a real, rendered number. A contrast case proves a usage-LESS
    /// unpriced bucket (a flow that reported NO tokens) does NOT taint a confident window
    /// (it adds no missing cost), and `unavailable` is still reserved for an all-unpriced
    /// window.
    #[test]
    fn window_cost_confidence_mixed_priced_and_unpriced_usage_is_estimated() {
        use crate::dashboard_flow::FlowStatus as FS;
        use crate::metrics::MetricsLayer;
        // The priced model has a CONFIGURED cache rate, so its own bucket is confident —
        // isolating the unpriced-bucket effect (no cached-rate fallback confounder).
        let mut prices = HashMap::new();
        prices.insert("priced".to_string(), ModelPrice::new(2.0, 6.0, 1.0));

        // (d) confident priced bucket (reported cached=0, configured cache rate) PLUS an
        // unpriced usage-bearing bucket ⇒ estimated (partial total).
        let metrics = MetricsLayer::new();
        metrics.record_terminal(
            FS::Completed,
            Some("priced"),
            "/v1/responses",
            Some("vllm-a"),
            900,
            Some(FlowUsage {
                prompt: 1000,
                completion: 500,
                total: 1500,
                cached: Some(0), // reported zero ⇒ this bucket alone is confident
                reasoning: Some(0),
            }),
            &[],
        );
        metrics.record_terminal(
            FS::Completed,
            Some("free"), // unpriced, but it carried real usage
            "/v1/responses",
            Some("vllm-b"),
            500,
            Some(usage(800, 400, 0)),
            &[],
        );
        let body = metrics_body(&metrics.view_with_seq().0, 0, 0, &prices);
        assert_eq!(
            body.cost_confidence,
            CostConfidence::Estimated,
            "a priced-confident bucket + an unpriced USAGE-BEARING bucket ⇒ estimated \
             (the unpriced spend is omitted from cost_per_min — a partial total)"
        );
        assert_eq!(body.windows.m1.cost_confidence, CostConfidence::Estimated);
        // The priced bucket makes the total a real number (NOT unavailable): a priced
        // sample exists, so $/min renders.
        assert_eq!(
            body.priced_samples, 1,
            "exactly the priced bucket is countable"
        );
        assert!(
            body.cost_per_min > 0.0,
            "cost_per_min is a real (if partial) number, so estimated — not unavailable"
        );

        // (e) confident priced bucket PLUS a usage-LESS unpriced bucket (no tokens
        // reported — e.g. a failure) ⇒ STILL confident: the unpriced bucket adds no
        // missing cost, so the total is complete.
        let metrics = MetricsLayer::new();
        metrics.record_terminal(
            FS::Completed,
            Some("priced"),
            "/v1/responses",
            Some("vllm-a"),
            900,
            Some(FlowUsage {
                prompt: 1000,
                completion: 500,
                total: 1500,
                cached: Some(0),
                reasoning: Some(0),
            }),
            &[],
        );
        // A terminal flow on an unpriced model that reported NO usage (usage_samples == 0
        // for its bucket): bumps the count but contributes no token throughput/cost.
        metrics.record_terminal(
            FS::Failed,
            Some("free"),
            "/v1/responses",
            Some("vllm-b"),
            120,
            None,
            &[],
        );
        let body = metrics_body(&metrics.view_with_seq().0, 0, 0, &prices);
        assert_eq!(
            body.cost_confidence,
            CostConfidence::Confident,
            "a usage-LESS unpriced bucket adds no missing cost ⇒ the window stays confident"
        );
    }

    /// Gap 12 — a minimal `ProviderHealth` for the topology DTO tests.
    fn provider_health(id: &str) -> crate::upstream::ProviderHealth {
        crate::upstream::ProviderHealth {
            id: id.to_string(),
            name: id.to_string(),
            route: None,
            base_url: "https://example.invalid/v1".to_string(),
            status: crate::upstream::ProviderStatus::Healthy,
            cooling_until_ms: None,
            last_error: None,
            served_count: 0,
            failover_count: 0,
            consecutive_failures: 0,
            catalog_fetched_ms: None,
            catalog_size: None,
        }
    }

    #[test]
    fn providers_body_preserves_provider_scoped_models_schedule_and_capacity() {
        let body = ProvidersBody {
            providers: vec![crate::upstream::ProviderInventoryEntry {
                provider_id: "mesh:worker-a".into(),
                provider_name: "local-vllm".into(),
                resource_id: Some("gpu-0".into()),
                route: Some("gpu-0".into()),
                base_url: "mesh://worker-a/gpu-0".into(),
                models: vec![crate::upstream::UpstreamModelEntry {
                    id: "local-model".into(),
                    context_limit: Some(32_768),
                }],
                availability: Some(crate::config::AvailabilitySchedule {
                    timezone: "America/Chicago".into(),
                    default_capacity: 2,
                    weekly: Vec::new(),
                    exceptions: Vec::new(),
                }),
                capacity_limit: Some(2),
                active_requests: Some(1),
                accepting_requests: true,
                healthy: true,
            }],
        };

        let value = serde_json::to_value(body).expect("serialize providers body");
        let provider = &value["providers"][0];
        assert_eq!(provider["provider_id"], "mesh:worker-a");
        assert_eq!(provider["models"][0]["id"], "local-model");
        assert_eq!(provider["models"][0]["context_limit"], 32_768);
        assert_eq!(provider["availability"]["timezone"], "America/Chicago");
        assert_eq!(provider["capacity_limit"], 2);
        assert_eq!(provider["active_requests"], 1);
    }

    /// Gap 12 (AGENTS.md changed-wire-field rule): the per-provider latency/error metrics
    /// are projected onto the `/topology` node as an ADDITIVE `per_provider` field and
    /// survive a JSON round-trip. A provider WITH in-window attempt samples carries a
    /// `derived` tile (real p50/p95/p99 + error rate + the bounded error distribution); a
    /// provider with NO samples OMITS the field entirely (don't-lie-with-zeros — absent,
    /// never a fabricated `0ms`/`0%`), leaving the frozen `TopologyNode` contract intact.
    #[test]
    fn topology_body_projects_per_provider_metrics_and_omits_zero_sample_nodes() {
        use crate::dashboard_flow::AttemptErrorClass;
        use crate::dashboard_flow::AttemptStatus;
        use crate::metrics::MetricsLayer;

        let metrics = MetricsLayer::new();
        // provider-a is hit by a failed primary then a served-elsewhere flow; provider-b
        // never appears in any attempt (a configured-but-idle provider).
        let attempts = vec![
            Attempt {
                provider: Some("provider-a".to_string()),
                model: Some("m".to_string()),
                start_ms: 1_000,
                end_ms: 1_080,
                first_upstream_byte_ms: None,
                status: AttemptStatus::Failed,
                error_class: Some(AttemptErrorClass::HttpStatus),
                failover_reason: Some(crate::dashboard_flow::AttemptFailoverReason::ProviderFailed),
            },
            Attempt {
                provider: Some("provider-c".to_string()),
                model: Some("m".to_string()),
                start_ms: 1_000,
                end_ms: 1_040,
                first_upstream_byte_ms: Some(1_040),
                status: AttemptStatus::Served,
                error_class: None,
                failover_reason: None,
            },
        ];
        metrics.record_terminal(
            FlowStatus::Completed,
            Some("m"),
            "/v1/responses",
            Some("provider-c"),
            40,
            None,
            &attempts,
        );

        let snapshot = ProviderHealthSnapshot {
            version: 7,
            providers: vec![provider_health("provider-a"), provider_health("provider-b")],
        };
        let prices: HashMap<String, ModelPrice> = HashMap::new();
        let body = topology_body(&snapshot, &prices, &metrics.view().window_1m);

        let value = serde_json::to_value(&body).expect("serialize topology body");
        let nodes = value["nodes"].as_array().expect("nodes array");
        let node_a = nodes
            .iter()
            .find(|n| n["id"] == serde_json::json!("provider-a"))
            .expect("provider-a node");
        let node_b = nodes
            .iter()
            .find(|n| n["id"] == serde_json::json!("provider-b"))
            .expect("provider-b node");

        // provider-a has a sample → a `derived` per_provider tile with real values.
        let per_a = &node_a["per_provider"];
        assert_eq!(per_a["data_quality"], serde_json::json!("derived"));
        assert_eq!(per_a["provider"], serde_json::json!("provider-a"));
        assert_eq!(per_a["samples"], serde_json::json!(1));
        assert_eq!(per_a["failed"], serde_json::json!(1));
        assert_eq!(per_a["error_rate"], serde_json::json!(100.0));
        assert_eq!(per_a["errors"]["http_status"], serde_json::json!(1));
        assert!(
            per_a["p99"].as_f64().expect("p99 number") > 0.0,
            "the failed primary's latency feeds p99"
        );

        // provider-b has NO samples → the field is ABSENT (don't-lie-with-zeros).
        assert!(
            node_b.get("per_provider").is_none(),
            "a zero-sample provider omits per_provider entirely (unavailable, not 0)"
        );

        // The per_provider tile round-trips back into the typed DTO.
        let typed: crate::metrics::ProviderLatency =
            serde_json::from_value(per_a.clone()).expect("deserialize ProviderLatency");
        assert_eq!(typed.provider, "provider-a");
        assert_eq!(typed.failed, 1);
        assert_eq!(typed.errors.http_status, 1);
    }
}
