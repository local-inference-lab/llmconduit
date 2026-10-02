/**
 * Wire types — the FROZEN contract between the Rust gateway and this SPA.
 *
 * These mirror the ACTUAL implemented Rust DTOs (not guesses):
 *  - `src/dashboard_flow.rs` (D1): `FlowStatus` (open/completed/failed/cancelled),
 *    `FlowUsage` (i64 prompt/completion/total/cached/reasoning), `SnapshotFlowSummary`
 *    (keyed by `api_call_id`, with optional `response_id`).
 *  - `src/monitor.rs`: `DebugWsMessage` is itself `#[serde(tag="type", rename_all="snake_case")]`
 *    with arms hello/request_upsert/segment_append/event_append/request_status/request_remove/
 *    snapshot_done — modeled as a real discriminated union below.
 *  - `ProviderHealth` (id/name/route/base_url/status/cooling_until_ms/
 *    last_error/served_count/failover_count/consecutive_failures/catalog_fetched_ms/catalog_size).
 *  - The dashboard WS envelope and REST response shapes.
 *
 * Discriminated unions are exhaustive and `any`-free (D9 constraint). `assertNever` turns a
 * missing switch arm into a COMPILE error. Runtime validators (bottom of file) reject any
 * frame whose shape/enum/seq does not match BEFORE it can mutate state.
 */

// ---------------------------------------------------------------------------
// Shared scalars
// ---------------------------------------------------------------------------

/**
 * Authoritative per-call identifier minted at the HTTP boundary (`api_call_id`). The REST
 * routes' `:id` == `api_call_id` (D13); detail + kill route by it. Distinct from
 * `response_id` (the engine's id) — the two COEXIST on a flow and must not be conflated.
 */
export type ApiCallId = string;
/** The engine-assigned response id; optional on a flow until `link()` binds it. */
export type ResponseId = string;

/** Lifecycle status of a flow — the EXACT `FlowStatus` enum from `dashboard_flow.rs`. */
export type FlowStatus = 'open' | 'completed' | 'failed' | 'cancelled';
export const FLOW_STATUSES: readonly FlowStatus[] = ['open', 'completed', 'failed', 'cancelled'];

/** Provider health state — the EXACT D4 `ProviderHealth.status` (snake_case serialize). */
export type ProviderStatus = 'healthy' | 'cooling' | 'down';
export const PROVIDER_STATUSES: readonly ProviderStatus[] = ['healthy', 'cooling', 'down'];

/**
 * Gap 07 — the CONFIDENCE tier of a `cost` figure (Rust `CostConfidence`, snake_case).
 * Lets an operator tell a trusted figure from a best-effort estimate from an honest gap:
 *  - `confident`   — priced AND every billed token class has a known rate (the cached
 *                    charge is either a reported `0` or priced at a CONFIGURED cache rate).
 *  - `estimated`   — priced but a class falls back to the default `0.0` cache rate (cached
 *                    `> 0` or UNREPORTED while no cache rate is configured) — an undercount;
 *                    MUST be LABELLED as an estimate in the UI (the cross-cutting rule).
 *  - `unavailable` — no price for the served model ⇒ `cost` is `null` (never a fake `0`).
 */
export type CostConfidence = 'confident' | 'estimated' | 'unavailable';
export const COST_CONFIDENCES: readonly CostConfidence[] = ['confident', 'estimated', 'unavailable'];

/**
 * Gap 04 — the PROVENANCE of a flow's `client_label` (Rust `ClientSource`, snake_case). It tags
 * HOW the non-secret client identity was derived, so spec 15 can render the WEAK User-Agent
 * fallback visibly DIFFERENTLY from a strong key-hash / configured-id (a `user_agent` label is
 * NOT an identity claim — any caller can spoof it):
 *  - `key_hash`          — a non-reversible `key-<12 hex>` digest of the inbound API key (the raw
 *                          key is NEVER emitted — only the one-way hash prefix). The STRONGEST seam:
 *                          the same caller key groups stably. Rendered `measured`.
 *  - `configured_header` — an operator-configured non-secret caller-id header (e.g. `x-client-id`).
 *                          An explicit caller-asserted (unverified) identity — strong, `measured`.
 *  - `user_agent`        — the `User-Agent` header. A WEAK, labelled fallback only (spoofable, not
 *                          an identity model). Rendered `derived` + visibly weaker, never as a
 *                          confirmed identity.
 * Absent (`client_label`/`client_source` both omitted) ⇒ no key, no configured-id, no UA ⇒ the cell
 * renders `—` (`unavailable`), NEVER a fabricated id (don't-lie-with-zeros).
 */
export type ClientSource = 'key_hash' | 'configured_header' | 'user_agent';
export const CLIENT_SOURCES: readonly ClientSource[] = ['key_hash', 'configured_header', 'user_agent'];

/**
 * Sessions — how a request's items diverge from its chain predecessor (the Rust
 * `sessions::DivergenceKind`). `append` = pure continuation; the three `*_changed`/`rewritten`
 * kinds fall INSIDE the predecessor's items and bust an upstream prefix cache; `new_chain` =
 * nothing in common (the start of a conversation).
 */
export type DivergenceKind = 'append' | 'instructions_changed' | 'tools_changed' | 'history_rewritten' | 'new_chain';
export const DIVERGENCE_KINDS: readonly DivergenceKind[] = ['append', 'instructions_changed', 'tools_changed', 'history_rewritten', 'new_chain'];

// ---------------------------------------------------------------------------
// Gap 02 / 03 spine — per-phase timestamps + the per-attempt failover trace.
// These mirror the Rust `PhaseTimings` (`#[serde(flatten)]`) + `Attempt` /
// `first_upstream_byte_ms` on `FlowRecord`/`SnapshotFlowSummary`. They are the
// PRIMARY (measured) source for the gap-10 latency breakdown; every field is
// OPTIONAL (Rust `Option<_>` + `skip_serializing_if` ⇒ ABSENT, never `0`, when
// the phase/attempt did not occur or was not measured). The frontend consumes
// them when present and falls back to the monitor `output` segments otherwise.
// ---------------------------------------------------------------------------

/**
 * Gap 02 — the per-phase wall-clock timestamps (epoch-ms) a turn passes through, flattened
 * onto the flow object as sibling scalar fields (mirrors the Rust `#[serde(flatten)]
 * PhaseTimings`). Each is a MEASURED epoch-ms instant or ABSENT (the phase did not occur /
 * was not measured) — NEVER `0` (don't-lie-with-zeros: a real wall-clock epoch is never `0`,
 * so a present value unambiguously means "this phase ran"). Where present they are monotonic
 * (`ingress ≤ normalization ≤ routing ≤ first_content_delta ≤ stream_end ≤ finalize`).
 *
 * Mixed into `FlowSummary`/`FlowDetail` rather than nested, because the Rust side flattens
 * them onto the flow. A pair of present neighbours yields a DERIVED sub-duration; a missing
 * endpoint makes that sub-duration UNAVAILABLE (rendered `—`, never a 0ms segment).
 */
export interface PhaseTimings {
  /** Request ingress (≈ `started_ms`); the left edge the waterfall anchors against. */
  ingress_ms?: number | null;
  /** Inbound→canonical normalization settled. Absent if the flow errored pre-normalization. */
  normalization_done_ms?: number | null;
  /** Upstream routing/lowering decision committed. Absent if the flow never reached the wire. */
  routing_decision_ms?: number | null;
  /**
   * True client TTFT — the first canonical **content** SSE delta to the client (NOT reasoning,
   * tool-argument, refusal, or signature deltas). Absent if the flow errored before any content.
   * When present ⇒ the breakdown labels TTFT **measured**.
   */
  first_content_delta_ms?: number | null;
  /** Stream completion (terminal `response.completed`/`incomplete`). Absent on a mid-stream error. */
  stream_end_ms?: number | null;
  /** Terminal finalize (every terminal: completed/failed/cancelled); the right edge. */
  finalize_ms?: number | null;
}

/** Gap 03 — one upstream dispatch attempt's outcome (Rust `AttemptStatus`, snake_case). */
export type AttemptStatus = 'served' | 'failed';
export const ATTEMPT_STATUSES: readonly AttemptStatus[] = ['served', 'failed'];

/**
 * Gap 03 — a BOUNDED, sanitized taxonomic failure code for a failed attempt (Rust
 * `AttemptErrorClass`, snake_case). NOT raw upstream error text — a fixed enum, safe on the
 * body-free summary. `null`/absent on the served attempt.
 */
export type AttemptErrorClass = 'connect' | 'http_status' | 'timeout' | 'stream' | 'terminal' | 'other';
export const ATTEMPT_ERROR_CLASSES: readonly AttemptErrorClass[] = [
  'connect',
  'http_status',
  'timeout',
  'stream',
  'terminal',
  'other',
];

/**
 * Gap 03 — a BOUNDED taxonomic reason a failed attempt triggered failover (Rust
 * `AttemptFailoverReason`, snake_case). `null`/absent on the served attempt.
 */
export type AttemptFailoverReason = 'provider_failed' | 'terminal_no_failover';
export const ATTEMPT_FAILOVER_REASONS: readonly AttemptFailoverReason[] = [
  'provider_failed',
  'terminal_no_failover',
];

// ---------------------------------------------------------------------------
// Gap 12 / 13 — per-provider latency + error distribution (the D4 topology node's
// ADDITIVE `per_provider`). These mirror the Rust `ProviderLatency` /
// `ProviderErrorDistribution` / `ProviderMetricQuality` (`src/metrics.rs`), exposed on
// the REST `/topology` + `/snapshot` node ONLY — the LIVE WS `topology_update` frame
// carries `per_provider` ABSENT (it does not join the metrics window, like its `0.0`
// edge rates). Spec 13 consumes them: a per-provider tile (p50/p95/p99 + error rate +
// per-class distribution) replacing the tooltip's global p99, and node sizing/color.
//
// DON'T-LIE-WITH-ZEROS: a provider with ZERO in-window attempt samples is ABSENT (no
// `per_provider` entry — renders `—`, never `0ms`/`0%`); a PRESENT entry is always a
// real `derived` measurement with `samples >= 1`, and an all-served provider reports a
// genuine MEASURED `error_rate: 0.0` (distinct from absent).
// ---------------------------------------------------------------------------

/**
 * Gap 12 — the DQ tag a per-provider metric carries (Rust `ProviderMetricQuality`,
 * snake_case). Always `derived` for a PRESENT entry (the percentiles are computed off the
 * provider's own attempt-latency histogram). The `unavailable` case (no in-window samples)
 * is the ABSENCE of the whole `ProviderLatency`, not a variant here.
 */
export type ProviderMetricQuality = 'derived';
export const PROVIDER_METRIC_QUALITIES: readonly ProviderMetricQuality[] = ['derived'];

/**
 * Gap 12 — the bounded per-error-class failure tally for one provider (Rust
 * `ProviderErrorDistribution`). Keys are the FIXED gap-03 {@link AttemptErrorClass}
 * taxonomy (never raw upstream text). Each class is OPTIONAL: the Rust side
 * `skip_serializing_if`s a `0`, so an ABSENT class means "that class did not occur" — a
 * present count is honest. (The don't-lie-with-zeros rule lives at the PROVIDER level — a
 * no-sample provider is absent entirely, never a fabricated all-zero distribution.) Every
 * present value is a non-negative integer count.
 */
export interface ProviderErrorDistribution {
  connect?: number;
  http_status?: number;
  timeout?: number;
  stream?: number;
  terminal?: number;
  other?: number;
}

/**
 * Gap 12 — the public per-provider latency + error-distribution DTO (Rust `ProviderLatency`;
 * additive on the D4 topology node, consumed by spec 13). Percentiles are `derived` ms over
 * the provider's ATTEMPT-latency histogram (a FAILED primary's latency is INCLUDED — spec 12 —
 * so a healthy-looking final-served latency cannot hide a degrading provider). `error_rate` is
 * the percentage of the provider's attempts that FAILED (`failed / samples × 100`). A present
 * entry always has `samples >= 1` (a zero-sample provider is ABSENT — don't-lie-with-zeros), so
 * an all-served provider's `error_rate` is a genuine MEASURED `0.0`, distinct from absent. All
 * floats are finite (the frozen finite-number wire contract). `provider` is the bounded
 * provider/route id OR the `__other__` overflow bucket (per-slot cap exceeded) / the `unknown`
 * sentinel (an attempt with no recorded provider) — spec 13 surfaces those overflow keys
 * HONESTLY (labelled), it does not hide them.
 */
export interface ProviderLatency {
  /** The provider label (a real id, or the `__other__` overflow / `unknown` sentinel key). */
  provider: string;
  /** DQ tag — always `derived` for a present entry (the `unavailable` case is absence). */
  data_quality: ProviderMetricQuality;
  /** Total ATTEMPTS in the window (served + failed); the measurability denominator. `>= 1`. */
  samples: number;
  /** Of `samples`, the count that SERVED (produced a first chunk). */
  served: number;
  /** Of `samples`, the count that FAILED before serving (failed primaries included). */
  failed: number;
  /** `derived` p50 attempt latency (ms). */
  p50: number;
  /** `derived` p95 attempt latency (ms). */
  p95: number;
  /** `derived` p99 attempt latency (ms). */
  p99: number;
  /** Percentage of attempts that failed (`failed / samples × 100`); a MEASURED `0.0` is real. */
  error_rate: number;
  /** Bounded per-class failure tally (gap 03 taxonomy); absent classes omitted. */
  errors: ProviderErrorDistribution;
}

/**
 * Gap 03 — one upstream dispatch attempt's full provenance (Rust `Attempt`): WHICH provider,
 * WHAT model, WHEN it began/resolved, WHEN the first wire byte arrived, and the OUTCOME. The
 * failover loop records one per provider it tried (failed ones + the served one); a non-failover
 * flow records exactly one. `first_upstream_byte_ms` is `null`/absent when the attempt never
 * received response headers (failed before a first chunk) — NEVER `0`. `error_class`/
 * `failover_reason` are `null`/absent on the served attempt and bounded taxonomic codes (never
 * raw upstream text) on a failed one. The gap-10 breakdown reads the SERVED attempt's
 * `first_upstream_byte_ms` to ENRICH the upstream-wait segment (wire TTFB).
 */
export interface Attempt {
  provider?: string | null;
  model?: string | null;
  /** Epoch-ms the attempt was dispatched. Always measured. */
  start_ms: number;
  /** Epoch-ms the attempt resolved (served first chunk, or failed). Always measured. */
  end_ms: number;
  /** Epoch-ms the FIRST wire chunk arrived for this attempt; `null`/absent when none did (never `0`). */
  first_upstream_byte_ms?: number | null;
  status: AttemptStatus;
  error_class?: AttemptErrorClass | null;
  failover_reason?: AttemptFailoverReason | null;
}

/** The debug request lifecycle status (`DebugRequestStatus`, monitor.rs). */
export type DebugRequestStatus = 'running' | 'completed' | 'failed';
/** Debug segment kind (`DebugSegmentKind`, monitor.rs). */
export type DebugSegmentKind = 'output' | 'reasoning' | 'tool';

/**
 * Token-accounting block (`FlowUsage`). `prompt`/`completion`/`total` are the core counts
 * the upstream always reports (Rust `i64` → `number`, validated finite).
 *
 * Gap 07 — usage CONFIDENCE: `cached`/`reasoning` are the OPTIONAL token classes an upstream
 * may or may not break out. The Rust side is `Option<i64>` serialized with `skip_serializing_if`,
 * so an UNREPORTED class is ABSENT on the wire — DISTINCT from a provider-reported `0`. They are
 * therefore `number | null` and OPTIONAL here: absent/`null` ⇒ UNAVAILABLE (renderers show `—`,
 * never a fabricated `0`); a present `0` is a measured zero. The distinction is load-bearing for
 * cost (an unpriced cached charge is `estimated`, not `confident` — see `CostConfidence`).
 */
export interface Usage {
  prompt: number;
  completion: number;
  total: number;
  /** Cache-read prompt tokens; absent/`null` ⇒ unreported (UNAVAILABLE), not `0`. */
  cached?: number | null;
  /** Reasoning tokens; absent/`null` ⇒ unreported (UNAVAILABLE), not `0`. */
  reasoning?: number | null;
}

// ---------------------------------------------------------------------------
// DebugWsMessage — the REAL discriminated union from `src/monitor.rs`
// (`#[serde(tag="type", rename_all="snake_case")]`). Carried (nested) inside the
// `Monitor` payload arm. A single `DebugUpdate` (one `sequence`) bundles a `Vec` of
// these, so the batched envelope MUST surface every sibling.
// ---------------------------------------------------------------------------

export interface DebugEventImage {
  id: string;
  label: string;
  path: string;
  mime_type: string;
  size_bytes?: number | null;
}

export interface DebugRequestStats {
  input_items: number;
  tool_count: number;
  turn_count: number;
  user_messages: number;
  assistant_messages: number;
  system_messages: number;
  developer_messages: number;
  reasoning_items: number;
  function_calls: number;
  function_outputs: number;
  tool_items: number;
  input_chars: number;
  instructions_chars: number;
}

export interface DebugRequest {
  response_id: string;
  model: string;
  started_at_ms: number;
  updated_at_ms: number;
  completed_at_ms?: number | null;
  status: DebugRequestStatus;
  stats: DebugRequestStats;
  error?: string | null;
}

export interface DebugSegment {
  timestamp_ms: number;
  kind: DebugSegmentKind;
  text: string;
}

export interface DebugTimelineEvent {
  timestamp_ms: number;
  kind: string;
  summary: string;
  payload_preview?: string | null;
  images: DebugEventImage[];
}

export type DebugWsMessage =
  | { type: 'hello'; protocol_version: number; history_limit: number; history_retention_ms: number }
  | { type: 'request_upsert'; request: DebugRequest }
  | { type: 'segment_append'; response_id: string; segment: DebugSegment }
  | { type: 'event_append'; response_id: string; event: DebugTimelineEvent }
  | {
      type: 'request_status';
      response_id: string;
      status: DebugRequestStatus;
      completed_at_ms?: number | null;
      error?: string | null;
    }
  // D3: cumulative token usage for a flow, keyed by `response_id` (the monitor's id; NOT
  // `api_call_id` — the flow-domain `usage` payload is the api_call_id-keyed one). Emitted live
  // on each usage-bearing chunk AND replayed once per flow right after its `request_upsert` in a
  // `snapshot()`/transcript batch. The theater ignores it (token totals live on the flow rows),
  // but it MUST validate — a batch carrying it (every replayed flow with usage does) is otherwise
  // rejected WHOLESALE, dropping the entire monitor replay (the theater would never initialize).
  | {
      type: 'usage';
      response_id: string;
      prompt: number;
      completion: number;
      total: number;
      cached: number;
      reasoning: number;
    }
  | { type: 'request_remove'; response_id: string; reason: string }
  | { type: 'snapshot_done' };

export const DEBUG_WS_KINDS: readonly DebugWsMessage['type'][] = [
  'hello',
  'request_upsert',
  'segment_append',
  'event_append',
  'request_status',
  'usage',
  'request_remove',
  'snapshot_done',
];

// ---------------------------------------------------------------------------
// DashboardPayload — the discriminated union (D7). Discriminant: `type`.
//
// WIRE CONTRACT (frozen target for D7 — the Rust side MUST match this):
//   `DashboardPayload` is internally tagged `#[serde(tag = "type", rename_all = "snake_case")]`.
//   Its `Monitor` arm holds a `DebugWsMessage`, which is ITSELF `type`-tagged — so the
//   monitor payload NESTS the message under `message` (it CANNOT be flattened: both carry a
//   `type` field and would collide). Wire shape:
//       { "type": "monitor", "message": { "type": "segment_append", "response_id": "...",
//                                          "segment": { ... } } }
//   The other arms are plain internally-tagged structs (fields inline next to `type`).
//   `ws.fixtures.ts` holds the exact byte-for-byte target bytes D7 must emit.
// ---------------------------------------------------------------------------

/** The Monitor arm: one per `DebugWsMessage` in the originating `DebugUpdate` batch. */
export interface MonitorPayload {
  type: 'monitor';
  message: DebugWsMessage;
}

/**
 * Per-flow usage update.
 *
 * CONTRACT RECONCILIATION (orchestrator-reconciled across D1/D7/D13 — specs are frozen and
 * NOT edited): the AUTHORITATIVE wire shape = D1's `FlowRecord` (keyed by `api_call_id`) +
 * D13 (`/flows/:id` with `:id == api_call_id`). D7's spec SKETCH shows `Usage{response_id,
 * …}` — that field name is ILLUSTRATIVE (the spec uses block-comment placeholders) and is
 * SUPERSEDED. D7 emits BOTH ids: `api_call_id` (REQUIRED — the authoritative correlation
 * key for the flow row) plus `response_id` (OPTIONAL — retained as a secondary correlation
 * to the engine's response id). Validator: require `api_call_id`, accept optional `response_id`.
 */
export interface UsagePayload {
  type: 'usage';
  /** REQUIRED authoritative flow key (matches D1 FlowRecord + D13 `:id`). */
  api_call_id: ApiCallId;
  /** OPTIONAL secondary correlation id (the engine response id); coexists with api_call_id. */
  response_id?: ResponseId | null;
  prompt: number;
  completion: number;
  total: number;
  /**
   * Gap 07 — usage CONFIDENCE on the live `usage` frame, mirroring {@link Usage}. The Rust
   * dashboard `Usage` payload sources `cached`/`reasoning` from `FlowRecord.usage`
   * (`Option<i64>` with `skip_serializing_if`), so an UNREPORTED class is ABSENT on the wire —
   * DISTINCT from a provider-reported `0`. They are therefore `number | null` and OPTIONAL:
   * absent/`null` ⇒ UNAVAILABLE (the row renders `—`, never a fabricated `0`); a present `0`
   * is a measured zero. (`prompt`/`completion`/`total` are always-present finite counts.)
   */
  cached?: number | null;
  reasoning?: number | null;
}

/** Sliding-window metric tiles (mirrors `/dashboard/api/metrics`, sans cursor). */
export interface MetricWindow {
  reqs_per_sec: number;
  active_streams: number;
  error_pct: number;
  p50: number;
  p95: number;
  p99: number;
  tokens_per_sec: number;
  prefill_tokens_per_sec: number;
  decode_tokens_per_sec: number;
  cost_per_min: number;
  /**
   * Count of TERMINAL (finalized) flows in this window — the data-quality signal for
   * LATENCY + error-% (gap 01). When `samples === 0` the latency/error-% fields were
   * never MEASURED (no finalized flow fed them), so the strip renders them `unavailable`
   * (`—`); `reqs_per_sec` (a genuine `0` for an idle window) and `active_streams` (live
   * open-flow count) stay numeric. A finite `u64` on the wire.
   */
  samples: number;
  /**
   * Count of those terminal flows that reported token usage (gap 01 review round 1,
   * finding 3) — the SEPARATE `tokens_per_sec` measurability denominator. Token/cost
   * availability is NOT the same as `samples`: a window can have `samples > 0` yet
   * `usage_samples === 0` (every finalized flow omitted usage), and then `tokens_per_sec`
   * is unmeasurable → it renders `—`, NEVER a fabricated `0`. A finite `u64`.
   */
  usage_samples: number;
  /** Flows with prompt usage and a measured non-zero prefill phase. */
  prefill_samples: number;
  /** Flows with completion usage and a measured non-zero decode phase. */
  decode_samples: number;
  /**
   * Count of usage-bearing terminal flows whose served model has a configured price
   * (gap 01 finding 3) — the `cost_per_min` measurability denominator. `0` ⇒ no PRICED
   * usage in the window ⇒ `cost_per_min` renders `—`, distinguishing an unpriced model
   * from a genuine measured `$0.00`. A finite `u64`.
   */
  priced_samples: number;
  /**
   * Gap 07 — the AGGREGATE confidence of this window's `cost_per_min`. `unavailable` when
   * nothing is priced (`cost_per_min` renders `—`); `estimated` when ANY priced bucket bills
   * cached at the default `0.0` (no silently-confident total — labelled in the strip);
   * `confident` only when every priced bucket's billed classes have known rates.
   */
  cost_confidence: CostConfidence;
}

export interface MetricTickPayload {
  type: 'metric_tick';
  reqs_per_sec: number;
  active_streams: number;
  error_pct: number;
  p50: number;
  p95: number;
  p99: number;
  tokens_per_sec: number;
  prefill_tokens_per_sec: number;
  decode_tokens_per_sec: number;
  cost_per_min: number;
  /** Headline (`m1`) terminal-flow sample count — mirrors `windows.m1.samples`. */
  samples: number;
  /** Headline (`m1`) usage-sample count — the tok/s denominator (finding 3). */
  usage_samples: number;
  prefill_samples: number;
  decode_samples: number;
  /** Headline (`m1`) priced-usage-sample count — the $/min denominator (finding 3). */
  priced_samples: number;
  /** Headline (`m1`) aggregate cost confidence (gap 07) — labels the headline `$/min`. */
  cost_confidence: CostConfidence;
  windows: {
    m1: MetricWindow;
    m5: MetricWindow;
    h1: MetricWindow;
  };
}

/**
 * Per-flow status update.
 *
 * CONTRACT RECONCILIATION (orchestrator-reconciled across D1/D7/D13 — specs are frozen and
 * NOT edited): the AUTHORITATIVE wire shape = D1's `FlowRecord` (keyed by `api_call_id`,
 * field `model_served`) + D13 (`/flows/:id` with `:id == api_call_id`). D7's spec SKETCH
 * shows `FlowStatus{response_id, served_model, …}` — those field names are ILLUSTRATIVE
 * (the spec uses block-comment placeholders) and are SUPERSEDED. D7 emits `api_call_id`
 * (REQUIRED — authoritative key + D6 kill key), `response_id` (OPTIONAL — secondary
 * correlation), and `model_served` (the served identity; the sketch's `served_model` is
 * superseded). `model_served`/`upstream_target` may be absent until D2 attaches them
 * (mirrors the `Option<String>` fields on `FlowRecord`). Validator: require `api_call_id`,
 * accept optional `response_id`.
 */
export interface FlowStatusPayload extends PhaseTimings {
  type: 'flow_status';
  /** REQUIRED authoritative flow key (matches D1 FlowRecord + D6 kill + D13 `:id`). */
  api_call_id: ApiCallId;
  /** OPTIONAL secondary correlation id (the engine response id); coexists with api_call_id. */
  response_id?: ResponseId | null;
  status: FlowStatus;
  model_requested?: string | null;
  /** Served identity (D1 `model_served`; supersedes D7 sketch's `served_model`). */
  model_served?: string | null;
  upstream_target?: string | null;
  usage: Usage | null;
  started_ms: number;
  finished_ms?: number | null;
  elapsed_ms?: number | null;
  /** Gap 03 — the per-attempt failover trace (optional; present once the backend projects it). */
  attempts?: Attempt[];
  /** Gap 03 — flow-level wire TTFB; `null`/absent, never `0`. */
  first_upstream_byte_ms?: number | null;
}

/**
 * One provider's health snapshot (topology node) — the EXACT D4 `ProviderHealth` DTO.
 * `route` is set only by the routing client; the counters drive the topology view.
 */
export interface ProviderHealth {
  id: string;
  name: string;
  /** REQUIRED key; value `string | null` (serde emits the key, null-not-absent) — finding 2. */
  route: string | null;
  /** REQUIRED non-null base URL (D4) — finding 2. */
  base_url: string;
  status: ProviderStatus;
  /** REQUIRED keys whose value is `T | null` (serde always emits them) — finding 2. */
  cooling_until_ms: number | null;
  last_error: string | null;
  served_count: number;
  failover_count: number;
  consecutive_failures: number;
  catalog_fetched_ms: number | null;
  catalog_size: number;
  /**
   * Gap 12/13 — this provider's per-provider latency + error distribution over the m1 window
   * (the additive Rust `TopologyNode.per_provider`). PRESENT only on the REST `/topology` +
   * `/snapshot` node (which join the metrics window); the LIVE WS `topology_update` frame
   * carries it ABSENT (no metrics join). ABSENT/`null` ⇒ the provider had ZERO in-window attempt
   * samples (don't-lie-with-zeros: the tile renders `—`, the node a neutral state — never a
   * fabricated `0ms`/`0%`/`0`-sized node). Optional + `| null` so an omitted key (WS frame, or
   * `skip_serializing_if` on a no-sample REST node) and a literal `null` both type.
   */
  per_provider?: ProviderLatency | null;
}

export interface TopologyUpdatePayload {
  type: 'topology_update';
  nodes: ProviderHealth[];
  edges: TopologyEdge[];
}

/**
 * Sessions-domain push tick: the live-session hub announces which session ids
 * changed. Carries NO body — the SPA refetches `/dashboard/api/sessions/active`
 * (the same push-invalidates-REST pattern a metrics frame uses for topology).
 */
export interface SessionUpdatePayload {
  type: 'session_update';
  touched: string[];
}

/**
 * The full `DashboardPayload` union. Every arm carries a `type` discriminant; an
 * exhaustive switch over `type` is enforced at compile time via `assertNever`.
 */
export type DashboardPayload =
  | MonitorPayload
  | UsagePayload
  | MetricTickPayload
  | FlowStatusPayload
  | TopologyUpdatePayload
  | SessionUpdatePayload;

// ---------------------------------------------------------------------------
// DashboardFrame — the batched WS envelope (D7). ONE frame per `DebugUpdate`
// for the Monitor domain (seq = DebugUpdate.sequence). Per-domain whole-frame dedup.
// ---------------------------------------------------------------------------

export type Domain = 'flow' | 'metrics' | 'topology' | 'monitor' | 'sessions';

export interface DashboardFrame {
  domain: Domain;
  seq: number;
  batch: DashboardPayload[];
}

/**
 * Which payload `type`s are legal under each domain (finding 5: a `metric_tick` under
 * domain `flow` is invalid → rejected). Monitor frames carry only `monitor`; flow frames
 * carry flow_status/usage; etc.
 */
export const DOMAIN_PAYLOADS: Record<Domain, ReadonlySet<DashboardPayload['type']>> = {
  flow: new Set(['flow_status', 'usage']),
  metrics: new Set(['metric_tick']),
  topology: new Set(['topology_update']),
  monitor: new Set(['monitor']),
  sessions: new Set(['session_update']),
};


/** The first WS message after connect: a full snapshot the live frames build upon. */
export interface SnapshotFrame {
  type: 'snapshot';
  cursors: SeqCursors;
  flows: FlowSummary[];
  metrics: MetricsResponse | null;
  topology: TopologyResponse | null;
}

/** Either a snapshot (once, first) or a live batched frame. */
export type WsServerMessage = SnapshotFrame | DashboardFrame;

// ---------------------------------------------------------------------------
// REST shapes (D13). Cursor-bearing reads carry their per-domain seq; /catalog is bare.
// ---------------------------------------------------------------------------

export interface SeqCursors {
  flow_seq: number;
  metrics_seq: number;
  topology_seq: number;
  monitor_seq: number;
}

/**
 * Row in the flow table (`GET /flows`) — mirrors D1's `SnapshotFlowSummary` (body-free).
 * Keyed by `api_call_id`; `response_id`, models, target, usage, and the timing fields are
 * optional exactly as the Rust `Option<_>` fields are. `cost` is a D13 roll-up addition.
 */
export interface FlowSummary extends PhaseTimings {
  api_call_id: ApiCallId;
  response_id?: ResponseId | null;
  method: string;
  uri: string;
  model_requested?: string | null;
  model_served?: string | null;
  upstream_target?: string | null;
  usage?: Usage | null;
  status: FlowStatus;
  started_ms: number;
  finished_ms?: number | null;
  elapsed_ms?: number | null;
  terminal_reason?: string | null;
  cost?: number | null;
  /**
   * Gap 07 — the confidence tier of `cost` (always present on the row). `estimated` MUST be
   * labelled as such; `unavailable` ⇒ `cost` is `null` (renders `—`, never a measured `0`).
   */
  cost_confidence: CostConfidence;
  /**
   * Gap 03 — the per-attempt failover trace (each `Attempt` is body-free scalar provenance +
   * bounded taxonomic codes). Absent/empty when no attempt was recorded. The gap-10 breakdown
   * reads the served attempt's `first_upstream_byte_ms`; the gap-11 stepper reads the whole list.
   * (The `PhaseTimings` epoch fields are mixed in via `extends` — flattened on the Rust wire.)
   */
  attempts?: Attempt[];
  /**
   * Gap 03 — flow-level wire time-to-first-byte (the served attempt's first on-wire chunk).
   * Distinct from `first_content_delta_ms` (the first content delta to the CLIENT). `null`/absent
   * ⇒ no upstream byte ever arrived (renders `—`, never `0`).
   */
  first_upstream_byte_ms?: number | null;
  /**
   * Gap 04 — the STABLE, NON-SECRET client attribution label (a key-hash `key-<hex>` display id, a
   * configured caller-id, or a User-Agent fallback), projected onto the `/flows` + `/snapshot` row
   * by the backend (gate F — already on the wire). The raw key is NEVER here (only the one-way hash
   * prefix ever existed as a label). Absent ⇒ the flow carried no key, no configured-id, and no UA ⇒
   * the CLIENT cell renders `—` (never a fabricated id). Consumed by spec 15.
   */
  client_label?: string | null;
  /**
   * Gap 04 — the {@link ClientSource} `client_label` was derived from, so the WEAK `user_agent`
   * fallback renders visibly weaker/different from a strong key-hash / configured-id. Absent exactly
   * when `client_label` is absent.
   */
  client_source?: ClientSource | null;
  /**
   * Sessions — the detected harness profile (`claude-code`, `codex`, `pi-agent`, ...) and version,
   * flattened from the Rust `FlowSessionFacts`. Absent ⇒ persistence off / not detected (renders `—`).
   */
  harness?: string | null;
  harness_version?: string | null;
  /** Sessions — the gateway session node (`sessions.id`) this flow was linked into. */
  session_id?: string | null;
  /** Sessions — the chain predecessor's `api_call_id`, when this flow extends a chain. */
  chain_parent_request_id?: string | null;
  /** Sessions — where/how the flow diverged from its predecessor. Absent ⇒ lineage not computed. */
  divergence_kind?: DivergenceKind | null;
  /** Sessions — true when the divergence falls inside the predecessor's items (prefix-cache miss). */
  cache_bust?: boolean | null;
  /** Sessions — owner of the authenticating virtual key; absent for open-mode keys. */
  user_id?: string | null;
  /** Sessions — the authenticating virtual key's stable database id (never the credential). */
  virtual_key_id?: string | null;
}

/** Body-free frozen summary in a snapshot — identical shape to `FlowSummary` (D1). */
export type SnapshotFlowSummary = FlowSummary;

// ---------------------------------------------------------------------------
// Durable history (SQL-backed `/dashboard/api/history/*`). Sessions are the tree nodes requests
// link into; a request row is the Rust `RequestSummary` (only the fields the UI reads are typed).
// ---------------------------------------------------------------------------

/** One session-tree node (the Rust `sessions::SessionRow`). */
export interface SessionRow {
  id: string;
  parent_id: string | null;
  kind: 'declared' | 'inferred' | string;
  harness: string;
  harness_version: string | null;
  /** The harness's own id when declared; null for inferred nodes. */
  external_id: string | null;
  session_kind: string | null;
  client_label: string | null;
  virtual_key_id: string | null;
  /** Owner of the key that opened the node; null for open-mode/legacy rows. */
  user_id: string | null;
  depth: number;
  root_request_id: string | null;
  spawned_by_request_id: string | null;
  first_seen_ms: number;
  last_seen_ms: number;
  request_count: number;
}

/** `GET /dashboard/api/history/sessions` */
export interface SessionsResponse {
  sessions: SessionRow[];
  since_ms: number;
  limit: number;
  truncated: boolean;
}

/** A durable request row as the history API returns it (subset the UI reads). */
export interface HistoryRequest {
  id: string;
  response_id: string | null;
  client_protocol: string;
  client_model: string;
  backend: string | null;
  resolved_model: string | null;
  status: string;
  created_at_ms: number;
  completed_at_ms: number | null;
  first_token_at_ms: number | null;
  input_tokens: number | null;
  output_tokens: number | null;
  cached_tokens: number | null;
  error: string | null;
  client_label: string | null;
  harness: string | null;
  harness_version: string | null;
  session_id: string | null;
  chain_parent_request_id: string | null;
  item_count: number | null;
  shared_prefix_items: number | null;
  divergence_kind: DivergenceKind | null;
  divergence_index: number | null;
  cache_bust: boolean | null;
}

/** `GET /dashboard/api/history/sessions/:id` */
export interface SessionDetailResponse {
  session: SessionRow;
  /** Nearest first. */
  ancestors: SessionRow[];
  children: SessionRow[];
  /** Oldest first. */
  requests: HistoryRequest[];
  requests_truncated: boolean;
}

// ---------------------------------------------------------------------------
// Active sessions (live hub, `GET /dashboard/api/sessions/active`).
// ---------------------------------------------------------------------------

/** One request stub on a live session's ring (body-free scalar facts). */
export interface SessionRequestStub {
  api_call_id: string;
  client_model: string;
  created_at_ms: number;
  /** `running` | `completed` | `failed`. */
  status: string;
  input_tokens?: number | null;
  output_tokens?: number | null;
  cached_tokens?: number | null;
  reasoning_tokens?: number | null;
  error?: string | null;
  terminal_reason?: string | null;
}

/** A live session: the tree node plus its trailing-window request counts. */
export interface ActiveSession extends SessionRow {
  /** Newest-first request stubs within the activity window. */
  requests: SessionRequestStub[];
  requests_1m: number;
  requests_5m: number;
  requests_10m: number;
  requests_15m: number;
}

/** Lifetime token totals over a session's durable requests. */
export interface SessionAggregate {
  session_id: string;
  input_tokens?: number | null;
  output_tokens?: number | null;
  cached_tokens?: number | null;
  reasoning_tokens?: number | null;
}

/** `GET /dashboard/api/sessions/active` */
export interface ActiveSessionsResponse {
  /** Newest activity first. */
  sessions: Array<ActiveSession & { aggregate: SessionAggregate | null }>;
  /** The sessions-domain WS cursor at the cut. */
  seq: number;
}

/** Which request-side hop of a durable request to reassemble. */
export type HistoryBodyHop = 'client_in' | 'upstream_out';

/** `GET /dashboard/api/flows` */
export interface FlowsResponse {
  flows: FlowSummary[];
  total: number;
  flow_seq: number;
}

/** Query params for the flow list. */
export interface FlowsQuery {
  status?: FlowStatus;
  model?: string;
  upstream?: string;
  page?: number;
  limit?: number;
}

/** A single streamed delta replayed into the inspector (from MonitorHub snapshot). */
export interface FlowDelta {
  sequence: number;
  kind: string;
  /** Heterogeneous delta body; narrow at the use site. */
  payload?: unknown;
  ts_ms?: number;
}

/**
 * `GET /dashboard/api/flows/:id` — the 3-pane inspector body (D13). The summary fields +
 * the three captured bodies (absent, not error, when evicted) + replayed deltas. Keyed by
 * `api_call_id`.
 */
export interface FlowDetail extends PhaseTimings {
  flow_seq: number;
  api_call_id: ApiCallId;
  response_id?: ResponseId | null;
  /** Absent when the body has been evicted by the summary-byte quota (D1). */
  inbound_body?: unknown;
  inbound_headers?: Record<string, string>;
  normalized?: unknown;
  upstream_body?: unknown;
  /**
   * Gap 05 / 14 — the captured upstream RESPONSE/ERROR body (`{body, truncated}`), projected onto
   * this LIVE detail path ONLY by the Rust side. ABSENT ⇒ capture disabled / no captured body
   * (UNAVAILABLE — the ErrorTab shows an explicit "capture disabled" state, never a misleading
   * blank); PRESENT ⇒ a real captured body. Like the three captured bodies above, it is live-only
   * (the inspector reads it from the live detail, never the frozen snapshot cut). See
   * {@link FlowUpstreamResponse}.
   */
  upstream_response?: FlowUpstreamResponse | null;
  model_requested?: string | null;
  model_served?: string | null;
  upstream_target?: string | null;
  usage?: Usage | null;
  status: FlowStatus;
  deltas: FlowDelta[];
  terminal_reason?: string | null;
  started_ms: number;
  finished_ms?: number | null;
  elapsed_ms?: number | null;
  cost?: number | null;
  /** Gap 07 — the confidence tier of `cost` (mirrors `FlowSummary.cost_confidence`). */
  cost_confidence: CostConfidence;
  /**
   * Gap 03 — the per-attempt failover trace (the served attempt's `first_upstream_byte_ms`
   * enriches the gap-10 upstream-wait segment). Optional/absent until the backend projects it
   * onto this detail DTO. (The `PhaseTimings` epoch fields are mixed in via `extends`.)
   */
  attempts?: Attempt[];
  /** Gap 03 — flow-level wire TTFB (the served attempt's first on-wire byte); `null`/absent, never `0`. */
  first_upstream_byte_ms?: number | null;
}

/**
 * Gap 05 / 14 — the captured upstream RESPONSE/ERROR body on the LIVE `/flows/:id` detail (the
 * Rust `FlowUpstreamResponse`, projected from `FlowRecord.upstream_response` by gap 05 review F2).
 * It is the redacted, capped upstream error body the gateway received, parsed back to a JSON value
 * (or a STRING value for a non-JSON / `[redacted: unparseable body …]` marker), plus a `truncated`
 * flag so the inspector shows a PARTIAL body honestly rather than as complete.
 *
 * PRESENCE is load-bearing for gap 14's don't-lie-with-zeros: the whole `upstream_response` field is
 * ABSENT on `FlowDetail` unless capture is ARMED
 * (`LLMCONDUIT_DASHBOARD_CAPTURE_UPSTREAM_RESPONSE=1`) AND the turn produced an upstream error body.
 * So ABSENT ⇒ "capture disabled / no captured body" (UNAVAILABLE — the ErrorTab shows an explicit
 * "capture disabled" state, NEVER a blank that implies "no error"); PRESENT ⇒ a real captured body
 * (even an EMPTY upstream body parses to the string `""`, distinct from the field being absent).
 * DELIBERATELY kept OFF the body-free `FlowSummary` list rows + `SnapshotFlowSummary` (the 135 GiB
 * body-free-snapshot invariant) — it rides ONLY on the live detail, like the three captured bodies.
 */
export interface FlowUpstreamResponse {
  /** The redacted/capped upstream error body, parsed to JSON (or a string for non-JSON/marker). */
  body: unknown;
  /** Whether the cap TRUNCATED the raw body (the retained bytes are a prefix) — flag it honestly. */
  truncated: boolean;
}

/** `GET /dashboard/api/metrics` */
export interface MetricsResponse {
  metrics_seq: number;
  reqs_per_sec: number;
  active_streams: number;
  error_pct: number;
  p50: number;
  p95: number;
  p99: number;
  tokens_per_sec: number;
  prefill_tokens_per_sec: number;
  decode_tokens_per_sec: number;
  cost_per_min: number;
  /** Headline (`m1`) terminal-flow sample count — mirrors `windows.m1.samples`. */
  samples: number;
  /** Headline (`m1`) usage-sample count — the tok/s denominator (finding 3). */
  usage_samples: number;
  prefill_samples: number;
  decode_samples: number;
  /** Headline (`m1`) priced-usage-sample count — the $/min denominator (finding 3). */
  priced_samples: number;
  /** Headline (`m1`) aggregate cost confidence (gap 07) — labels the headline `$/min`. */
  cost_confidence: CostConfidence;
  windows: {
    m1: MetricWindow;
    m5: MetricWindow;
    h1: MetricWindow;
  };
}

export interface TopologyEdge {
  from: string;
  to: string;
  throughput: number;
  tokens_per_sec: number;
  cost_per_sec: number;
}

export interface ModelPrice {
  input_per_1k: number;
  output_per_1k: number;
  cached_per_1k: number;
  /**
   * Gap 07 — cached-price PRESENCE: whether `cached_per_1k` was EXPLICITLY configured,
   * distinguishing a real configured `0.0` cache-read rate from an OMITTED one (which also
   * defaults to `0.0`). Additive — `cached_per_1k` keeps its `number` type. Consumed by the
   * cost-confidence model (a cached charge against a model with no configured cache rate is
   * `estimated`) and by spec 08's "$ saved" (which must read presence, not the numeric `0.0`).
   */
  cached_price_configured: boolean;
}

/** `GET /dashboard/api/topology` — nodes/edges + the price table. */
export interface TopologyResponse {
  topology_seq: number;
  nodes: ProviderHealth[];
  edges: TopologyEdge[];
  price_table: Record<string, ModelPrice>;
}

/**
 * Catalog entry (`GET /dashboard/api/catalog` returns a BARE array — no cursor).
 *
 * `context_limit` is the per-model max-context window (tokens). NULLABLE (gap 06):
 * the backend serializes it ABSENT/`null` when the upstream advertises no window —
 * distinct from a real `0`. Renderers MUST show `—` (unavailable) on `null`/absent,
 * NEVER `0` (a `0` ceiling reads as garbage/infinite utilization in the gap-09
 * gauge). Optional + `| null` so an omitted key and an explicit `null` both type.
 */
export interface CatalogEntry {
  id: string;
  context_limit?: number | null;
}

export interface ProviderInventoryModel {
  id: string;
  context_limit?: number | null;
}

export interface AvailabilitySchedule {
  timezone: string;
  default_capacity: number;
  weekly: AvailabilityWeeklyWindow[];
  exceptions: AvailabilityException[];
}

export interface AvailabilityWeeklyWindow {
  days: string[];
  start_local: string;
  end_local: string;
  capacity: number;
}

export interface AvailabilityException {
  start: string;
  end: string;
  capacity: number;
}

export interface ProviderInventoryEntry {
  provider_id: string;
  provider_name: string;
  resource_id: string | null;
  route: string | null;
  base_url: string;
  models: ProviderInventoryModel[];
  availability: AvailabilitySchedule | null;
  capacity_limit: number | null;
  active_requests: number | null;
  accepting_requests: boolean;
  healthy: boolean;
}

export interface ProvidersResponse {
  providers: ProviderInventoryEntry[];
}

/** Dashboard-managed OpenAI-compatible provider. Credentials never cross this wire. */
export interface ConfiguredProvider {
  id: string;
  name: string;
  base_url: string;
  api_key_present: boolean;
  auto_discover: boolean;
  allowed_models?: string[] | null;
  disabled_models: string[];
  models: ProviderInventoryModel[];
}

export interface ConfiguredProvidersResponse {
  providers: ConfiguredProvider[];
}

export interface CreateConfiguredProviderRequest {
  name: string;
  base_url: string;
  api_key: string;
}

export interface UpdateConfiguredProviderRequest {
  auto_discover?: boolean;
  allowed_models?: string[] | null;
  disabled_models?: string[];
}

export interface FleetModel {
  id: string;
  description?: string;
  image: string;
  gpu_count?: number;
  max_instances?: number;
}

export interface FleetInstanceStatus {
  instance_id?: string;
  index: number;
  port?: number;
  phase: string;
  container_status?: string;
  assigned_gpus?: number[];
  last_error?: string;
}

export interface FleetDeploymentStatus {
  model_id: string;
  phase: string;
  desired_state: string;
  desired_instances?: number;
  ready_instances?: number;
  container_status?: string;
  health?: string;
  exit_code?: number;
  oom_killed?: boolean;
  assigned_gpus: number[];
  instances?: FleetInstanceStatus[];
  last_error?: string;
  last_checked?: string;
}

export interface FleetModelEntry {
  model: FleetModel;
  status: FleetDeploymentStatus;
}

export interface FleetModelsResponse {
  models: FleetModelEntry[];
}

export interface FleetOperation {
  id: string;
  kind: string;
  model_id: string;
  state: string;
  error?: string;
  created_at: string;
  started_at?: string;
  finished_at?: string;
}

export interface FleetOperationResponse {
  changed: boolean;
  operation?: FleetOperation;
}

export interface MeshJoinKey {
  id: string;
  label?: string;
  enabled: boolean;
  created_at_ms: number;
  expires_at_ms?: number;
  max_uses?: number;
  use_count: number;
}

export interface MeshNode {
  endpoint_id: string;
  label?: string;
  enabled: boolean;
  joined_at_ms: number;
  join_key_id?: string;
  last_seen_at_ms?: number;
  model_switching?: MeshModelSwitching;
}

export interface MeshModelSwitching {
  provider: string;
  models: MeshSwitchableModel[];
  revision: number;
}

export interface MeshSwitchableModel {
  id: string;
  description?: string;
  phase: string;
  desired_state: string;
  gpu_count?: number;
  max_instances?: number;
  desired_instances?: number;
  ready_instances?: number;
  assigned_gpus?: number[];
  instances?: FleetInstanceStatus[];
  container_status?: string;
  last_error?: string;
}

export interface MeshDisabledModel {
  endpoint_id: string;
  resource_id: string;
  model: string;
  disabled_at_ms: number;
}

export interface MeshAdminState {
  join_keys: MeshJoinKey[];
  nodes: MeshNode[];
  disabled_models: MeshDisabledModel[];
}

export interface CreateMeshJoinKeyRequest {
  label?: string;
  max_uses?: number;
  expires_in_secs?: number;
}

export interface CreateMeshJoinKeyResponse {
  join_key: MeshJoinKey;
  token: string;
}

export interface RevokeMeshJoinKeyResponse {
  updated: boolean;
  disabled_endpoint_ids: string[];
  evicted_endpoint_ids: string[];
}

export interface SetMeshNodeResponse {
  updated: boolean;
  endpoint_id: string;
  enabled: boolean;
  evicted: boolean;
}

export interface MeshModelOverrideRequest {
  endpoint_id: string;
  resource_id: string;
  model: string;
}

export interface SetMeshModelResponse {
  updated: boolean;
  endpoint_id: string;
  resource_id: string;
  model: string;
  disabled: boolean;
}

export interface SwitchMeshModelResponse {
  endpoint_id: string;
  model_id: string;
  accepted: boolean;
  changed: boolean;
  error?: string;
}

export type ProviderMetricsSource = 'vllm' | 'sglang';

export interface ProviderCacheMetrics {
  provider: string;
  source: ProviderMetricsSource;
  fetched_at_ms: number;
  cache_hits: number | null;
  cache_queries: number | null;
  cache_hit_rate: number | null;
  kv_cache_usage: number | null;
  data_quality: 'derived';
}

export interface ProviderMetricsResponse {
  generated_at_ms: number;
  providers: ProviderCacheMetrics[];
}

/** `GET /dashboard/api/snapshot?at=<unix_ms>` */
export interface SnapshotResponse {
  cursors: SeqCursors;
  at_ms: number;
  summaries: SnapshotFlowSummary[];
  metrics: MetricsResponse | null;
  topology: TopologyResponse | null;
}

/** `POST /dashboard/api/flows/:id/kill` */
export interface KillResponse {
  api_call_id: ApiCallId;
  killed: boolean;
}

// ---------------------------------------------------------------------------
// Auth shapes (D7)
// ---------------------------------------------------------------------------

/** `POST /dashboard/login` body. */
export interface LoginRequest {
  token?: string;
  username?: string;
  password?: string;
}

/** The user behind a dashboard session (embedded in the signed cookie). */
export interface SessionUser {
  id: string;
  username: string;
  is_admin: boolean;
}

export type AuthMode = 'users' | 'token' | 'open' | 'github';

/** `GET /dashboard/api/me` */
export interface MeResponse {
  user: SessionUser | null;
  is_admin: boolean;
  auth_mode: AuthMode;
  accounts_enabled: boolean;
}

export interface UserRecord {
  id: string;
  username: string;
  is_admin: boolean;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface ApiKeyRecord {
  id: string;
  label: string | null;
  user_id: string | null;
  allowed_models: string[];
  created_at_ms: number;
  updated_at_ms: number;
}

/** `POST /dashboard/api/keys` — the plaintext is returned exactly once. */
export interface CreatedKeyResponse {
  key: ApiKeyRecord;
  secret: string;
  registered_keys: number;
}

/** One cell of `GET /dashboard/api/history/throughput` (bucket × model × backend). */
export interface ThroughputBucket {
  bucket_ms: number;
  model: string;
  backend: string | null;
  requests: number;
  completed: number;
  input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
  ttft_ms_sum: number;
  ttft_count: number;
  prefill_tokens: number;
  decode_ms_sum: number;
  decode_tokens: number;
  decode_count: number;
}

export interface ThroughputResponse {
  buckets: ThroughputBucket[];
  since_ms: number;
  bucket_ms: number;
  limit: number;
  truncated: boolean;
}

/** One cell of `GET /dashboard/api/history/activity` (bucket × user × key). */
export interface ActivityBucket {
  bucket_ms: number;
  user_id: string | null;
  virtual_key_id: string | null;
  requests: number;
  failed: number;
  input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
}

export interface ActivityResponse {
  buckets: ActivityBucket[];
  since_ms: number;
  bucket_ms: number;
  limit: number;
  truncated: boolean;
}

/** One row of `GET /dashboard/api/history/metrics`: the sample JSON is in `data`. */
export interface MetricSample {
  backend: string;
  ts_ms: number;
  data: string;
}

export interface HistoryMetricsResponse {
  samples: MetricSample[];
  since_ms: number;
  limit: number;
  truncated: boolean;
}

/** A parsed upstream (vLLM/SGLang) sample, `kind: "upstream"` in `MetricSample.data`. */
export interface UpstreamMetricsSample {
  kind: 'upstream';
  engine: 'vllm' | 'sglang' | 'unknown' | string;
  backend: string;
  scraped_at_ms: number;
  models: Record<string, { values?: Record<string, number>; by_label?: Record<string, Record<string, number>> }>;
  kept_lines: number;
}

/**
 * SPA bootstrap embedded by the Rust shell at `window.__LLMCONDUIT_DASHBOARD__` (D7). The
 * frozen field name for auth state is `authenticated` (boolean). `csrf_token` is the
 * double-submit token echo; `mutations_enabled` gates the kill control.
 */
export interface DashboardBootstrap {
  authenticated: boolean;
  csrf_token: string | null;
  mutations_enabled: boolean;
  /** The signed-in user (null for token / dev-open sessions). */
  user: SessionUser | null;
  auth_mode: AuthMode;
}

// ---------------------------------------------------------------------------
// Access management shapes (RBAC dashboard)
// ---------------------------------------------------------------------------

export type ManagementPermission =
  | 'auth.keys.read'
  | 'auth.keys.create'
  | 'auth.keys.revoke'
  | 'auth.keys.rotate'
  | 'auth.principals.read'
  | 'auth.principals.write'
  | 'auth.groups.read'
  | 'auth.groups.write'
  | 'auth.roles.read'
  | 'auth.roles.write'
  | 'auth.policies.read'
  | 'auth.policies.write'
  | 'auth.usage.read'
  | 'auth.audit.read'
  | 'auth.pricing.read'
  | 'auth.pricing.sync'
  | 'auth.pricing.write'
  | 'auth.sessions.read'
  | 'auth.sessions.terminate'
  | 'fleet.models.read'
  | 'fleet.models.load'
  | 'fleet.models.unload';

export interface AuthSummary {
  enabled: boolean;
  policy_epoch: number;
  actor: {
    kind: 'bootstrap' | 'delegated';
    principal_id: string | null;
    display_name: string;
    permissions: ManagementPermission[];
  };
  counts: {
    users: number;
    groups: number;
    roles: number;
    policies: number;
    api_keys: number;
    active_sessions: number;
  };
}

export interface AuthUser {
  id: string;
  kind: 'user' | 'service_account';
  display_name: string;
  enabled: boolean;
  created_at: string;
}

export interface AuthGroup {
  id: string;
  name: string;
  enabled: boolean;
  member_count: number;
}

export interface AuthRole {
  id: string;
  name: string;
  enabled: boolean;
  permissions: ManagementPermission[];
}

export interface AuthPolicy {
  id: string;
  name: string;
  effect: 'allow' | 'deny';
  enabled: boolean;
  subjects: string[];
  endpoints: string[];
  models: string[];
  requested_models: string[];
  served_models: string[];
  providers: string[];
  routes: string[];
  time_windows: AuthPolicyTimeWindow[];
  max_concurrent_sessions: number | null;
  max_daily_session_starts: number | null;
  management_permissions: ManagementPermission[];
}

export interface AuthPolicyTimeWindow {
  weekday_mask: number;
  start_minute: number;
  end_minute: number;
  absolute_start_ms: number | null;
  absolute_end_ms: number | null;
}

export interface AuthApiKey {
  id: string;
  principal_id: string;
  name: string;
  prefix: string;
  enabled: boolean;
  created_at: string;
  expires_at: string | null;
  last_used_at: string | null;
  capture_payloads: boolean;
}

/** Returned only by create/rotate. `raw_key` must never be persisted or fetched later. */
export interface CreatedAuthApiKey extends AuthApiKey {
  raw_key: string;
}

export interface AuthSession {
  id: string;
  kind: 'inference' | 'dashboard';
  principal_id: string;
  key_id: string;
  endpoint: string | null;
  requested_model: string | null;
  started_at: string;
  expires_at: string | null;
}

export interface AuthUsageRow {
  dimension: string;
  value: string;
  requests: number;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  cached_tokens: number | null;
  reasoning_tokens: number | null;
  cost: number | null;
  cost_confidence: CostConfidence;
}

export interface AuthAuditEvent {
  id: string;
  timestamp: string;
  actor: string;
  action: string;
  target: string;
  outcome: 'ok' | 'denied' | 'error';
  metadata: Record<string, unknown>;
}

export interface AuthPricingRow {
  model: string;
  provider: string;
  source: string;
  fetched_at: string;
  input_per_1k: string;
  output_per_1k: string;
  confidence: CostConfidence;
}

export interface AuthUsersResponse { users: AuthUser[] }
export interface AuthGroupsResponse { groups: AuthGroup[] }
export interface AuthRolesResponse { roles: AuthRole[] }
export interface AuthPoliciesResponse { policies: AuthPolicy[] }
export interface AuthApiKeysResponse { api_keys: AuthApiKey[] }
export interface AuthSessionsResponse { sessions: AuthSession[] }
export interface AuthUsageResponse { usage: AuthUsageRow[] }
export interface AuthAuditResponse { events: AuthAuditEvent[] }
export interface AuthPricingResponse { pricing: AuthPricingRow[] }

export interface KeyLoginRequest { api_key: string }
export interface CreateAuthUserRequest {
  display_name: string;
  kind: AuthUser['kind'];
}
export interface CreateAuthGroupRequest {
  name: string;
  members: string[];
}
export interface CreateAuthRoleRequest {
  name: string;
  permissions: ManagementPermission[];
}
export interface CreateAuthApiKeyRequest {
  principal_id: string;
  name: string;
  expires_at?: string | null;
  capture_payloads?: boolean;
}
export interface CreateAuthPolicyRequest {
  name: string;
  effect: AuthPolicy['effect'];
  subjects: string[];
  endpoints: string[];
  models: string[];
  requested_models: string[];
  served_models: string[];
  providers: string[];
  routes: string[];
  time_windows: AuthPolicyTimeWindow[];
  max_concurrent_sessions: number | null;
  max_daily_session_starts: number | null;
  management_permissions: ManagementPermission[];
}

export function isAuthSummary(v: unknown): v is AuthSummary {
  if (!isObj(v) || typeof v.enabled !== 'boolean' || !isUint(v.policy_epoch) || !isObj(v.actor) || !isObj(v.counts)) return false;
  const actor = v.actor;
  const counts = v.counts;
  return isOneOf(actor.kind, ['bootstrap', 'delegated'] as const)
    && isNullableStr(actor.principal_id)
    && isStr(actor.display_name)
    && isStringArray(actor.permissions)
    && ['users', 'groups', 'roles', 'policies', 'api_keys', 'active_sessions'].every((k) => isUint(counts[k]));
}

function isAuthUser(v: unknown): v is AuthUser {
  return isObj(v) && isStr(v.id) && isOneOf(v.kind, ['user', 'service_account'] as const)
    && isStr(v.display_name) && typeof v.enabled === 'boolean' && isStr(v.created_at);
}
function isAuthGroup(v: unknown): v is AuthGroup {
  return isObj(v) && isStr(v.id) && isStr(v.name) && typeof v.enabled === 'boolean' && isUint(v.member_count);
}
function isAuthRole(v: unknown): v is AuthRole {
  return isObj(v) && isStr(v.id) && isStr(v.name) && typeof v.enabled === 'boolean' && isStringArray(v.permissions);
}
function isAuthPolicy(v: unknown): v is AuthPolicy {
  return isObj(v) && isStr(v.id) && isStr(v.name) && isOneOf(v.effect, ['allow', 'deny'] as const)
    && typeof v.enabled === 'boolean' && isStringArray(v.subjects) && isStringArray(v.endpoints)
    && isStringArray(v.models) && isStringArray(v.requested_models) && isStringArray(v.served_models)
    && isStringArray(v.providers) && isStringArray(v.routes)
    && Array.isArray(v.time_windows) && v.time_windows.every((window) => isObj(window)
      && isUint(window.weekday_mask) && isUint(window.start_minute) && isUint(window.end_minute)
      && (window.absolute_start_ms === null || typeof window.absolute_start_ms === 'number')
      && (window.absolute_end_ms === null || typeof window.absolute_end_ms === 'number'))
    && (v.max_concurrent_sessions === null || isUint(v.max_concurrent_sessions))
    && (v.max_daily_session_starts === null || isUint(v.max_daily_session_starts))
    && isStringArray(v.management_permissions);
}
function isAuthApiKey(v: unknown): v is AuthApiKey {
  return isObj(v) && isStr(v.id) && isStr(v.principal_id) && isStr(v.name) && isStr(v.prefix)
    && typeof v.enabled === 'boolean' && isStr(v.created_at) && isNullableStr(v.expires_at)
    && isNullableStr(v.last_used_at) && typeof v.capture_payloads === 'boolean';
}
function isAuthSession(v: unknown): v is AuthSession {
  return isObj(v) && isStr(v.id) && isOneOf(v.kind, ['inference', 'dashboard'] as const)
    && isStr(v.principal_id) && isStr(v.key_id) && isNullableStr(v.endpoint)
    && isNullableStr(v.requested_model) && isStr(v.started_at) && isNullableStr(v.expires_at);
}
function isAuthUsageRow(v: unknown): v is AuthUsageRow {
  return isObj(v) && isStr(v.dimension) && isStr(v.value) && isUint(v.requests)
    && isNullableUint(v.prompt_tokens) && isNullableUint(v.completion_tokens)
    && isNullableUint(v.cached_tokens) && isNullableUint(v.reasoning_tokens)
    && (v.cost === null || isNum(v.cost)) && isOneOf(v.cost_confidence, COST_CONFIDENCES);
}
function isAuthAuditEvent(v: unknown): v is AuthAuditEvent {
  return isObj(v) && isStr(v.id) && isStr(v.timestamp) && isStr(v.actor) && isStr(v.action)
    && isStr(v.target) && isOneOf(v.outcome, ['ok', 'denied', 'error'] as const) && isObj(v.metadata);
}
function isAuthPricingRow(v: unknown): v is AuthPricingRow {
  return isObj(v) && isStr(v.model) && isStr(v.provider) && isStr(v.source) && isStr(v.fetched_at)
    && isStr(v.input_per_1k) && isStr(v.output_per_1k) && isOneOf(v.confidence, COST_CONFIDENCES);
}

function isArrayEnvelope(v: unknown, key: string, guard: (item: unknown) => boolean): boolean {
  return isObj(v) && Array.isArray(v[key]) && v[key].every(guard);
}
function isStringArray(v: unknown): v is string[] {
  return Array.isArray(v) && v.every(isStr);
}

export const isAuthUsersResponse = (v: unknown): v is AuthUsersResponse => isArrayEnvelope(v, 'users', isAuthUser);
export const isAuthGroupsResponse = (v: unknown): v is AuthGroupsResponse => isArrayEnvelope(v, 'groups', isAuthGroup);
export const isAuthRolesResponse = (v: unknown): v is AuthRolesResponse => isArrayEnvelope(v, 'roles', isAuthRole);
export const isAuthPoliciesResponse = (v: unknown): v is AuthPoliciesResponse => isArrayEnvelope(v, 'policies', isAuthPolicy);
export const isAuthApiKeysResponse = (v: unknown): v is AuthApiKeysResponse => isArrayEnvelope(v, 'api_keys', isAuthApiKey);
export const isAuthSessionsResponse = (v: unknown): v is AuthSessionsResponse => isArrayEnvelope(v, 'sessions', isAuthSession);
export const isAuthUsageResponse = (v: unknown): v is AuthUsageResponse => isArrayEnvelope(v, 'usage', isAuthUsageRow);
export const isAuthAuditResponse = (v: unknown): v is AuthAuditResponse => isArrayEnvelope(v, 'events', isAuthAuditEvent);
export const isAuthPricingResponse = (v: unknown): v is AuthPricingResponse => isArrayEnvelope(v, 'pricing', isAuthPricingRow);
export function isCreatedAuthApiKey(v: unknown): v is CreatedAuthApiKey {
  if (!isObj(v) || !isAuthApiKey(v)) return false;
  const rawKey = (v as unknown as Record<string, unknown>).raw_key;
  return isStr(rawKey) && rawKey.startsWith('llmc_');
}

function isAvailabilitySchedule(v: unknown): v is AvailabilitySchedule {
  return isObj(v) && isStr(v.timezone) && isUint(v.default_capacity)
    && Array.isArray(v.weekly) && v.weekly.every((window) => isObj(window)
      && isStringArray(window.days) && isStr(window.start_local) && isStr(window.end_local)
      && isUint(window.capacity))
    && Array.isArray(v.exceptions) && v.exceptions.every((exception) => isObj(exception)
      && isStr(exception.start) && isStr(exception.end) && isUint(exception.capacity));
}

function isProviderInventoryModel(v: unknown): v is ProviderInventoryModel {
  return isObj(v) && isStr(v.id) && isOptUint(v.context_limit);
}

function isProviderInventoryEntry(v: unknown): v is ProviderInventoryEntry {
  return isObj(v) && isStr(v.provider_id) && isStr(v.provider_name)
    && isNullableStr(v.resource_id) && isNullableStr(v.route) && isStr(v.base_url)
    && Array.isArray(v.models) && v.models.every(isProviderInventoryModel)
    && (v.availability === null || isAvailabilitySchedule(v.availability))
    && isNullableUint(v.capacity_limit) && isNullableUint(v.active_requests)
    && typeof v.accepting_requests === 'boolean'
    && typeof v.healthy === 'boolean';
}

function isProviderCacheMetrics(v: unknown): v is ProviderCacheMetrics {
  return isObj(v) && isStr(v.provider) && isOneOf(v.source, ['vllm', 'sglang'] as const)
    && isUint(v.fetched_at_ms) && isOptNum(v.cache_hits) && isOptNum(v.cache_queries)
    && isOptNum(v.cache_hit_rate) && isOptNum(v.kv_cache_usage)
    && v.data_quality === 'derived';
}

export const isProvidersResponse = (v: unknown): v is ProvidersResponse => isArrayEnvelope(v, 'providers', isProviderInventoryEntry);
function isConfiguredProvider(v: unknown): v is ConfiguredProvider {
  return isObj(v) && isStr(v.id) && isStr(v.name) && isStr(v.base_url)
    && typeof v.api_key_present === 'boolean' && typeof v.auto_discover === 'boolean'
    && (v.allowed_models === undefined || v.allowed_models === null || isStringArray(v.allowed_models))
    && isStringArray(v.disabled_models)
    && Array.isArray(v.models) && v.models.every(isProviderInventoryModel);
}
export const isConfiguredProvidersResponse = (v: unknown): v is ConfiguredProvidersResponse =>
  isArrayEnvelope(v, 'providers', isConfiguredProvider);
export const isConfiguredProviderResponse = isConfiguredProvider;
export const isProviderMetricsResponse = (v: unknown): v is ProviderMetricsResponse =>
  isObj(v) && isUint(v.generated_at_ms) && Array.isArray(v.providers) && v.providers.every(isProviderCacheMetrics);

function isFleetModel(v: unknown): v is FleetModel {
  return isObj(v) && isStr(v.id) && isOptStr(v.description) && isStr(v.image)
    && (v.gpu_count === undefined || isUint(v.gpu_count))
    && (v.max_instances === undefined || isUint(v.max_instances));
}

function isFleetInstanceStatus(v: unknown): v is FleetInstanceStatus {
  return isObj(v) && isOptStr(v.instance_id) && isUint(v.index) && isOptUint(v.port)
    && isStr(v.phase) && isOptStr(v.container_status)
    && (v.assigned_gpus === undefined || (Array.isArray(v.assigned_gpus) && v.assigned_gpus.every(isUint)))
    && isOptStr(v.last_error);
}

function isFleetDeploymentStatus(v: unknown): v is FleetDeploymentStatus {
  return isObj(v) && isStr(v.model_id) && isStr(v.phase) && isStr(v.desired_state)
    && (v.desired_instances === undefined || isUint(v.desired_instances))
    && (v.ready_instances === undefined || isUint(v.ready_instances))
    && isOptStr(v.container_status) && isOptStr(v.health) && isOptNum(v.exit_code)
    && (v.oom_killed === undefined || typeof v.oom_killed === 'boolean')
    && Array.isArray(v.assigned_gpus) && v.assigned_gpus.every(isUint)
    && (v.instances === undefined || (Array.isArray(v.instances) && v.instances.every(isFleetInstanceStatus)))
    && isOptStr(v.last_error) && isOptStr(v.last_checked);
}

function isFleetModelEntry(v: unknown): v is FleetModelEntry {
  return isObj(v) && isFleetModel(v.model) && isFleetDeploymentStatus(v.status);
}

function isFleetOperation(v: unknown): v is FleetOperation {
  return isObj(v) && isStr(v.id) && isStr(v.kind) && isStr(v.model_id) && isStr(v.state)
    && isOptStr(v.error) && isStr(v.created_at) && isOptStr(v.started_at) && isOptStr(v.finished_at);
}

export const isFleetModelsResponse = (v: unknown): v is FleetModelsResponse =>
  isArrayEnvelope(v, 'models', isFleetModelEntry);

export const isFleetOperationResponse = (v: unknown): v is FleetOperationResponse =>
  isObj(v) && typeof v.changed === 'boolean' && (v.operation === undefined || isFleetOperation(v.operation));

function isMeshJoinKey(v: unknown): v is MeshJoinKey {
  return isObj(v) && isStr(v.id) && isOptStr(v.label) && typeof v.enabled === 'boolean'
    && isUint(v.created_at_ms) && isOptUint(v.expires_at_ms) && isOptUint(v.max_uses)
    && isUint(v.use_count);
}

function isMeshNode(v: unknown): v is MeshNode {
  return isObj(v) && isStr(v.endpoint_id) && isOptStr(v.label) && typeof v.enabled === 'boolean'
    && isUint(v.joined_at_ms) && isOptStr(v.join_key_id) && isOptUint(v.last_seen_at_ms)
    && (v.model_switching === undefined || isMeshModelSwitching(v.model_switching));
}

function isMeshModelSwitching(v: unknown): v is MeshModelSwitching {
  return isObj(v) && isStr(v.provider) && isUint(v.revision)
    && Array.isArray(v.models) && v.models.every((model) => isObj(model) && isStr(model.id)
      && isOptStr(model.description) && isStr(model.phase) && isStr(model.desired_state)
      && (model.gpu_count === undefined || isUint(model.gpu_count))
      && (model.max_instances === undefined || isUint(model.max_instances))
      && (model.desired_instances === undefined || isUint(model.desired_instances))
      && (model.ready_instances === undefined || isUint(model.ready_instances))
      && (model.assigned_gpus === undefined || (Array.isArray(model.assigned_gpus) && model.assigned_gpus.every(isUint)))
      && (model.instances === undefined || (Array.isArray(model.instances) && model.instances.every(isFleetInstanceStatus)))
      && isOptStr(model.container_status) && isOptStr(model.last_error));
}

function isMeshDisabledModel(v: unknown): v is MeshDisabledModel {
  return isObj(v) && isStr(v.endpoint_id) && isStr(v.resource_id) && isStr(v.model) && isUint(v.disabled_at_ms);
}

export const isMeshAdminState = (v: unknown): v is MeshAdminState =>
  isObj(v)
  && Array.isArray(v.join_keys) && v.join_keys.every(isMeshJoinKey)
  && Array.isArray(v.nodes) && v.nodes.every(isMeshNode)
  && Array.isArray(v.disabled_models) && v.disabled_models.every(isMeshDisabledModel);

export const isCreateMeshJoinKeyResponse = (v: unknown): v is CreateMeshJoinKeyResponse =>
  isObj(v) && isMeshJoinKey(v.join_key) && isStr(v.token);

export const isRevokeMeshJoinKeyResponse = (v: unknown): v is RevokeMeshJoinKeyResponse =>
  isObj(v) && typeof v.updated === 'boolean' && isStringArray(v.disabled_endpoint_ids) && isStringArray(v.evicted_endpoint_ids);

export const isSetMeshNodeResponse = (v: unknown): v is SetMeshNodeResponse =>
  isObj(v) && typeof v.updated === 'boolean' && isStr(v.endpoint_id) && typeof v.enabled === 'boolean' && typeof v.evicted === 'boolean';

export const isSetMeshModelResponse = (v: unknown): v is SetMeshModelResponse =>
  isObj(v) && typeof v.updated === 'boolean' && isStr(v.endpoint_id) && isStr(v.resource_id)
  && isStr(v.model) && typeof v.disabled === 'boolean';

export const isSwitchMeshModelResponse = (v: unknown): v is SwitchMeshModelResponse =>
  isObj(v) && isStr(v.endpoint_id) && isStr(v.model_id) && typeof v.accepted === 'boolean'
  && typeof v.changed === 'boolean' && isOptStr(v.error);

// ---------------------------------------------------------------------------
// Runtime validation (the WS pipe must NOT trust the wire — findings 4/5/6).
// A frame is validated WHOLLY (envelope + every payload arm, exact enums, unsigned-int
// seq, domain↔payload compatibility) BEFORE the socket touches any cursor or store.
// ---------------------------------------------------------------------------

function isObj(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}
function isNum(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}
/** A non-negative integer (the wire `u64`/`u128`/`usize` fields). */
function isUint(v: unknown): v is number {
  return typeof v === 'number' && Number.isInteger(v) && v >= 0;
}
function isStr(v: unknown): v is string {
  return typeof v === 'string';
}
/** Optional string: absent, null, or a string. */
function isOptStr(v: unknown): boolean {
  return v === undefined || v === null || isStr(v);
}
/** Optional unsigned int: absent, null, or a uint. */
function isOptUint(v: unknown): boolean {
  return v === undefined || v === null || isUint(v);
}
/** Optional finite number: absent, null, or a finite number (gap 07 cached/reasoning). */
function isOptNum(v: unknown): boolean {
  return v === undefined || v === null || isNum(v);
}
function isOneOf<T extends string>(v: unknown, set: readonly T[]): v is T {
  return isStr(v) && (set as readonly string[]).includes(v);
}
/** REQUIRED key whose value is `string | null` (present, but may be null) — finding 2. */
function isNullableStr(v: unknown): v is string | null {
  return v === null || isStr(v);
}
/** REQUIRED key whose value is `uint | null` (present, but may be null) — finding 2. */
function isNullableUint(v: unknown): v is number | null {
  return v === null || isUint(v);
}

const DOMAINS: readonly Domain[] = ['flow', 'metrics', 'topology', 'monitor', 'sessions'];
export function isDomain(v: unknown): v is Domain {
  return isOneOf(v, DOMAINS);
}

/** Validates a `CostConfidence` enum value (gap 07) — confident/estimated/unavailable. */
function isCostConfidence(v: unknown): v is CostConfidence {
  return isOneOf(v, COST_CONFIDENCES);
}

/**
 * Gap 03 — validates one `Attempt` (the WS/snapshot wire is not trusted): `start_ms`/`end_ms`
 * REQUIRED uints, `status` a known enum, the optional fields absent/null or their right shape.
 * `error_class`/`failover_reason` must be the bounded taxonomic enums (rejects raw text leaking
 * onto the body-free summary). Exported so the consuming surfaces (gap 10/11) can re-validate.
 */
export function isAttempt(v: unknown): v is Attempt {
  return (
    isObj(v) &&
    isOptStr(v.provider) && isOptStr(v.model) &&
    isUint(v.start_ms) && isUint(v.end_ms) &&
    isOptUint(v.first_upstream_byte_ms) &&
    isOneOf(v.status, ATTEMPT_STATUSES) &&
    (v.error_class === undefined || v.error_class === null || isOneOf(v.error_class, ATTEMPT_ERROR_CLASSES)) &&
    (v.failover_reason === undefined || v.failover_reason === null || isOneOf(v.failover_reason, ATTEMPT_FAILOVER_REASONS))
  );
}

/** Optional attempts list: absent, null, or an array of valid `Attempt`s (gap 03). */
function isOptAttempts(v: unknown): boolean {
  return v === undefined || v === null || (Array.isArray(v) && v.every(isAttempt));
}

/**
 * Gap 02 — the per-phase timestamps are OPTIONAL flattened siblings on the flow object. Each is
 * absent/null or an unsigned-int epoch (never `0`-as-unmeasured on the wire, but a present `0`
 * would still validate as a uint — the don't-lie-with-zeros distinction is enforced by the Rust
 * `skip_serializing_if`, so an unmeasured phase is ABSENT, not `0`). Validates the bundle is
 * shaped (no negative/fractional epoch sneaks in).
 */
function isOptPhaseTimings(v: Record<string, unknown>): boolean {
  return (
    isOptUint(v.ingress_ms) &&
    isOptUint(v.normalization_done_ms) &&
    isOptUint(v.routing_decision_ms) &&
    isOptUint(v.first_content_delta_ms) &&
    isOptUint(v.stream_end_ms) &&
    isOptUint(v.finalize_ms)
  );
}

function isUsage(v: unknown): v is Usage {
  // Gap 07: `cached`/`reasoning` are OPTIONAL (absent/null ⇒ unreported/UNAVAILABLE,
  // distinct from a present finite `0`); `prompt`/`completion`/`total` are required finite.
  return (
    isObj(v) && isNum(v.prompt) && isNum(v.completion) && isNum(v.total) &&
    isOptNum(v.cached) && isOptNum(v.reasoning)
  );
}
function isUsageOrNull(v: unknown): v is Usage | null {
  return v === null || isUsage(v);
}
/** Optional usage: absent, null, or a valid usage. */
function isOptUsage(v: unknown): boolean {
  return v === undefined || isUsageOrNull(v);
}

function isMetricWindow(v: unknown): v is MetricWindow {
  return (
    isObj(v) && isNum(v.reqs_per_sec) && isNum(v.active_streams) && isNum(v.error_pct) &&
    isNum(v.p50) && isNum(v.p95) && isNum(v.p99) && isNum(v.tokens_per_sec) &&
    isNum(v.prefill_tokens_per_sec) && isNum(v.decode_tokens_per_sec) && isNum(v.cost_per_min) &&
    // The three per-metric measurability denominators are non-negative integer counts
    // (gap 01): `samples` (latency/error), `usage_samples` (tok/s), `priced_samples`
    // ($/min). All REQUIRED — the Rust tile always emits them.
    isUint(v.samples) && isUint(v.usage_samples) && isUint(v.prefill_samples) &&
    isUint(v.decode_samples) && isUint(v.priced_samples) &&
    // Gap 07: the aggregate cost-confidence tag is REQUIRED on every window.
    isCostConfidence(v.cost_confidence)
  );
}
function isMetricWindows(v: unknown): boolean {
  return isObj(v) && isMetricWindow(v.m1) && isMetricWindow(v.m5) && isMetricWindow(v.h1);
}

/**
 * Gap 12/13 — validates a `ProviderErrorDistribution` (the wire is not trusted): every present
 * per-class key is a non-negative integer count; absent keys are fine (`skip_serializing_if` on a
 * `0`). An object whose any present class is non-uint is rejected.
 */
function isProviderErrorDistribution(v: unknown): v is ProviderErrorDistribution {
  return (
    isObj(v) &&
    isOptUint(v.connect) && isOptUint(v.http_status) && isOptUint(v.timeout) &&
    isOptUint(v.stream) && isOptUint(v.terminal) && isOptUint(v.other)
  );
}

/**
 * Gap 12/13 — validates a `ProviderLatency` (the REST/snapshot node's additive `per_provider`).
 * `data_quality` must be the bounded `derived` enum; `samples`/`served`/`failed` non-negative
 * ints; the percentiles + `error_rate` FINITE numbers (rejects NaN/Inf); `errors` a valid bounded
 * distribution. Exported so the consuming surface (spec 13) can re-validate if needed.
 */
export function isProviderLatency(v: unknown): v is ProviderLatency {
  return (
    isObj(v) &&
    isStr(v.provider) &&
    isOneOf(v.data_quality, PROVIDER_METRIC_QUALITIES) &&
    isUint(v.samples) && isUint(v.served) && isUint(v.failed) &&
    isNum(v.p50) && isNum(v.p95) && isNum(v.p99) && isNum(v.error_rate) &&
    isProviderErrorDistribution(v.errors)
  );
}

/** Optional per-provider metrics: absent, null, or a valid `ProviderLatency` (gap 12/13). */
function isOptProviderLatency(v: unknown): boolean {
  return v === undefined || v === null || isProviderLatency(v);
}

/**
 * Gap 05 / 14 — validates the captured upstream error body (`FlowUpstreamResponse`). The `/flows/:id`
 * detail is fetched (not pushed through the strict WS pipe), but the captured body is OPERATOR-FACING
 * and heterogeneous, so the gap-14 model re-checks the shape before rendering it: `truncated` MUST be
 * a boolean and `body` MUST be present (it is `unknown` — any JSON value, incl. the string `""` for an
 * empty/non-JSON body; only `undefined` is rejected, which would mean the field was malformed). A
 * malformed object falls back to the "capture disabled / no body" UNAVAILABLE state — never a blank
 * that implies "no error".
 */
export function isFlowUpstreamResponse(v: unknown): v is FlowUpstreamResponse {
  return isObj(v) && typeof v.truncated === 'boolean' && 'body' in v && v.body !== undefined;
}

function isProviderHealth(v: unknown): v is ProviderHealth {
  return (
    isObj(v) &&
    isStr(v.id) && isStr(v.name) &&
    // base_url REQUIRED non-null; route/cooling_until_ms/last_error/catalog_fetched_ms are
    // REQUIRED keys whose value may be null (serde null-not-absent) — finding 2.
    isNullableStr(v.route) && isStr(v.base_url) &&
    isOneOf(v.status, PROVIDER_STATUSES) &&
    isNullableUint(v.cooling_until_ms) && isNullableStr(v.last_error) &&
    isUint(v.served_count) && isUint(v.failover_count) && isUint(v.consecutive_failures) &&
    isNullableUint(v.catalog_fetched_ms) && isUint(v.catalog_size) &&
    // Gap 12/13: `per_provider` is OPTIONAL — ABSENT on the WS `topology_update` frame (no metrics
    // join) and on a zero-sample REST node (`skip_serializing_if`); validated when present so a
    // populated REST/snapshot node's per-provider tile can trust it.
    isOptProviderLatency(v.per_provider)
  );
}
function isTopologyEdge(v: unknown): v is TopologyEdge {
  return isObj(v) && isStr(v.from) && isStr(v.to) && isNum(v.throughput) && isNum(v.tokens_per_sec) && isNum(v.cost_per_sec);
}

/** Validates a complete `ModelPrice` with FINITE numbers (rejects NaN/Inf/missing) — finding 4.
 * Gap 07: the additive `cached_price_configured` boolean (cached-price presence) is REQUIRED. */
function isModelPrice(v: unknown): v is ModelPrice {
  return (
    isObj(v) && isNum(v.input_per_1k) && isNum(v.output_per_1k) && isNum(v.cached_per_1k) &&
    typeof v.cached_price_configured === 'boolean'
  );
}
/** Validates a `price_table` map: every value a complete finite `ModelPrice` — finding 4. */
function isPriceTable(v: unknown): v is Record<string, ModelPrice> {
  return isObj(v) && Object.values(v).every(isModelPrice);
}

const DEBUG_REQUEST_STATUSES: readonly DebugRequestStatus[] = ['running', 'completed', 'failed'];
const DEBUG_SEGMENT_KINDS: readonly DebugSegmentKind[] = ['output', 'reasoning', 'tool'];

/** Fully validates `DebugRequestStats` — every field a uint (finding 3). */
function isDebugRequestStats(v: unknown): v is DebugRequestStats {
  return (
    isObj(v) &&
    isUint(v.input_items) && isUint(v.tool_count) && isUint(v.turn_count) &&
    isUint(v.user_messages) && isUint(v.assistant_messages) && isUint(v.system_messages) &&
    isUint(v.developer_messages) && isUint(v.reasoning_items) && isUint(v.function_calls) &&
    isUint(v.function_outputs) && isUint(v.tool_items) && isUint(v.input_chars) &&
    isUint(v.instructions_chars)
  );
}

/** Fully validates a `DebugRequest` incl. its stats, status enum, and timestamps (finding 3). */
function isDebugRequest(v: unknown): v is DebugRequest {
  return (
    isObj(v) &&
    isStr(v.response_id) && isStr(v.model) &&
    isUint(v.started_at_ms) && isUint(v.updated_at_ms) && isNullableUint(v.completed_at_ms) &&
    isOneOf(v.status, DEBUG_REQUEST_STATUSES) &&
    isDebugRequestStats(v.stats) &&
    isNullableStr(v.error)
  );
}

function isDebugEventImage(v: unknown): v is DebugEventImage {
  return (
    isObj(v) && isStr(v.id) && isStr(v.label) && isStr(v.path) && isStr(v.mime_type) &&
    (v.size_bytes === undefined || isNullableUint(v.size_bytes))
  );
}

/** Fully validates a `DebugSegment` (kind enum + timestamp + text) — finding 3. */
function isDebugSegment(v: unknown): v is DebugSegment {
  return isObj(v) && isUint(v.timestamp_ms) && isOneOf(v.kind, DEBUG_SEGMENT_KINDS) && isStr(v.text);
}

/** Fully validates a `DebugTimelineEvent` incl. its images array — finding 3. */
function isDebugTimelineEvent(v: unknown): v is DebugTimelineEvent {
  return (
    isObj(v) && isUint(v.timestamp_ms) && isStr(v.kind) && isStr(v.summary) &&
    (v.payload_preview === undefined || isNullableStr(v.payload_preview)) &&
    Array.isArray(v.images) && v.images.every(isDebugEventImage)
  );
}

/**
 * Fully validates a nested `DebugWsMessage` (itself `type`-tagged) — findings 1+3. Every
 * arm's nested DTO is validated (DebugRequest/stats, segment, event/images, status enums,
 * timestamp types); no `as` cast skips validation.
 */
export function isDebugWsMessage(v: unknown): v is DebugWsMessage {
  if (!isObj(v) || !isStr(v.type)) return false;
  switch (v.type) {
    case 'hello':
      return isUint(v.protocol_version) && isUint(v.history_limit) && isUint(v.history_retention_ms);
    case 'request_upsert':
      return isDebugRequest(v.request);
    case 'segment_append':
      return isStr(v.response_id) && isDebugSegment(v.segment);
    case 'event_append':
      return isStr(v.response_id) && isDebugTimelineEvent(v.event);
    case 'request_status':
      return isStr(v.response_id) && isOneOf(v.status, DEBUG_REQUEST_STATUSES) && isNullableUint(v.completed_at_ms) && isNullableStr(v.error);
    case 'usage':
      // D3 cumulative usage (keyed by response_id) — finite token counts; rejecting it would drop
      // the whole monitor batch it rides in (a replayed flow's usage echo).
      return (
        isStr(v.response_id) &&
        isNum(v.prompt) && isNum(v.completion) && isNum(v.total) && isNum(v.cached) && isNum(v.reasoning)
      );
    case 'request_remove':
      return isStr(v.response_id) && isStr(v.reason);
    case 'snapshot_done':
      return true;
    default:
      return false;
  }
}

/** Validates a single decoded payload against its `type` arm. */
export function isDashboardPayload(v: unknown): v is DashboardPayload {
  if (!isObj(v) || !isStr(v.type)) return false;
  switch (v.type) {
    case 'monitor':
      // Nested, itself-tagged DebugWsMessage (NOT flattened) — finding 1.
      return isDebugWsMessage(v.message);
    case 'usage':
      // Gap 07: `cached`/`reasoning` are OPTIONAL (absent/null ⇒ unreported/UNAVAILABLE,
      // distinct from a present finite `0`); `prompt`/`completion`/`total` required finite.
      return (
        isStr(v.api_call_id) && isOptStr(v.response_id) &&
        isNum(v.prompt) && isNum(v.completion) && isNum(v.total) &&
        isOptNum(v.cached) && isOptNum(v.reasoning)
      );
    case 'metric_tick':
      return (
        isNum(v.reqs_per_sec) && isNum(v.active_streams) && isNum(v.error_pct) &&
        isNum(v.p50) && isNum(v.p95) && isNum(v.p99) && isNum(v.tokens_per_sec) &&
        isNum(v.prefill_tokens_per_sec) && isNum(v.decode_tokens_per_sec) && isNum(v.cost_per_min) &&
        isUint(v.samples) && isUint(v.usage_samples) && isUint(v.prefill_samples) &&
        isUint(v.decode_samples) && isUint(v.priced_samples) &&
        isCostConfidence(v.cost_confidence) && isMetricWindows(v.windows)
      );
    case 'flow_status':
      return (
        isStr(v.api_call_id) && isOptStr(v.response_id) &&
        isOneOf(v.status, FLOW_STATUSES) &&
        isOptStr(v.model_requested) && isOptStr(v.model_served) && isOptStr(v.upstream_target) &&
        isUsageOrNull(v.usage) && isUint(v.started_ms) && isOptUint(v.finished_ms) && isOptUint(v.elapsed_ms) &&
        // Gap 02/03: optional spine fields on the live flow update — validated when present.
        isOptPhaseTimings(v) && isOptAttempts(v.attempts) && isOptUint(v.first_upstream_byte_ms)
      );
    case 'topology_update':
      return Array.isArray(v.nodes) && Array.isArray(v.edges) && v.nodes.every(isProviderHealth) && v.edges.every(isTopologyEdge);
    case 'session_update':
      return Array.isArray(v.touched) && v.touched.every(isStr);
    default:
      return false;
  }
}

/**
 * Validates the whole batched envelope: a valid `domain`, an UNSIGNED-INTEGER `seq`
 * (rejects negative/fractional — finding 5), every payload valid, AND every payload's
 * `type` legal under `domain` (domain↔payload compatibility — finding 5).
 */
export function isDashboardFrame(v: unknown): v is DashboardFrame {
  if (!isObj(v) || !isDomain(v.domain) || !isUint(v.seq) || !Array.isArray(v.batch)) {
    return false;
  }
  const allowed = DOMAIN_PAYLOADS[v.domain];
  return v.batch.every((p) => isDashboardPayload(p) && allowed.has(p.type));
}

function isSeqCursors(v: unknown): v is SeqCursors {
  return isObj(v) && isUint(v.flow_seq) && isUint(v.metrics_seq) && isUint(v.topology_seq) && isUint(v.monitor_seq);
}

/** Validates a body-free flow summary (each snapshot summary — finding 4). */
function isFlowSummary(v: unknown): v is FlowSummary {
  return (
    isObj(v) &&
    isStr(v.api_call_id) && isOptStr(v.response_id) &&
    isStr(v.method) && isStr(v.uri) &&
    isOptStr(v.model_requested) && isOptStr(v.model_served) && isOptStr(v.upstream_target) &&
    isOptUsage(v.usage) &&
    isOneOf(v.status, FLOW_STATUSES) &&
    isUint(v.started_ms) && isOptUint(v.finished_ms) && isOptUint(v.elapsed_ms) &&
    isOptStr(v.terminal_reason) &&
    // Gap 07: the per-flow cost-confidence tag is REQUIRED on every row.
    isCostConfidence(v.cost_confidence) &&
    // Gap 02/03: the optional spine fields, when present, must be well-shaped (don't trust the
    // wire). Absent ⇒ the summary predates the spine projection / the phase didn't occur.
    isOptPhaseTimings(v) && isOptAttempts(v.attempts) && isOptUint(v.first_upstream_byte_ms) &&
    // Gap 04 (spec 15): the optional client attribution. `client_label` is a non-secret string;
    // `client_source`, when present, must be a bounded `ClientSource` (don't trust the wire — a
    // bogus source must not paint the weak-UA fallback as a strong identity). Both absent ⇒ the
    // unattributed flow (renders `—`).
    isOptStr(v.client_label) && isOptClientSource(v.client_source) &&
    // Sessions: the optional harness/session facts (flattened `FlowSessionFacts`). A bogus
    // divergence kind must not paint a row as a cache bust; `cache_bust` is a bool when present.
    isOptStr(v.harness) && isOptStr(v.harness_version) && isOptStr(v.session_id) &&
    isOptStr(v.chain_parent_request_id) && isOptDivergenceKind(v.divergence_kind) &&
    (v.cache_bust === undefined || v.cache_bust === null || typeof v.cache_bust === 'boolean') &&
    // Key attribution (flattened FlowSessionFacts): opaque ids when present.
    isOptStr(v.user_id) && isOptStr(v.virtual_key_id)
  );
}

/** Optional `DivergenceKind`: absent, null, or one of the bounded kinds. */
function isOptDivergenceKind(v: unknown): boolean {
  return v === undefined || v === null || isOneOf(v, DIVERGENCE_KINDS);
}

/** Optional `ClientSource` (gap 04): absent, null, or one of the bounded snake_case sources. */
function isOptClientSource(v: unknown): boolean {
  return v === undefined || v === null || isOneOf(v, CLIENT_SOURCES);
}

function isMetricsResponse(v: unknown): v is MetricsResponse {
  return (
    isObj(v) && isUint(v.metrics_seq) &&
    isNum(v.reqs_per_sec) && isNum(v.active_streams) && isNum(v.error_pct) &&
    isNum(v.p50) && isNum(v.p95) && isNum(v.p99) && isNum(v.tokens_per_sec) &&
    isNum(v.prefill_tokens_per_sec) && isNum(v.decode_tokens_per_sec) && isNum(v.cost_per_min) &&
    isUint(v.samples) && isUint(v.usage_samples) && isUint(v.prefill_samples) &&
    isUint(v.decode_samples) && isUint(v.priced_samples) &&
    isCostConfidence(v.cost_confidence) && isMetricWindows(v.windows)
  );
}

function isTopologyResponse(v: unknown): v is TopologyResponse {
  return (
    isObj(v) && isUint(v.topology_seq) &&
    Array.isArray(v.nodes) && v.nodes.every(isProviderHealth) &&
    Array.isArray(v.edges) && v.edges.every(isTopologyEdge) &&
    // Every price_table entry is a complete finite ModelPrice (finding 4).
    isPriceTable(v.price_table)
  );
}

/**
 * Fully validates a snapshot envelope (finding 4): cursors are the four unsigned-int
 * fields, every summary is a valid body-free flow summary, and metrics/topology are either
 * null or their full valid shapes — BEFORE the snapshot can be applied.
 */
export function isSnapshotFrame(v: unknown): v is SnapshotFrame {
  return (
    isObj(v) && v.type === 'snapshot' &&
    isSeqCursors(v.cursors) &&
    Array.isArray(v.flows) && v.flows.every(isFlowSummary) &&
    (v.metrics === null || isMetricsResponse(v.metrics)) &&
    (v.topology === null || isTopologyResponse(v.topology))
  );
}

// ---------------------------------------------------------------------------
// Exhaustiveness helper
// ---------------------------------------------------------------------------

/**
 * Compile-time exhaustiveness guard. Placing `assertNever(x)` in the `default` arm
 * of a switch over a discriminated union turns any unhandled case into a TS error.
 * At runtime it throws (should be unreachable).
 */
export function assertNever(value: never): never {
  throw new Error(`Unhandled discriminated union member: ${JSON.stringify(value)}`);
}
