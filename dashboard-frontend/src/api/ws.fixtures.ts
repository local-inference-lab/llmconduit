/**
 * GOLDEN WIRE FIXTURES — the exact JSON bytes the Rust D7 side MUST emit on
 * `/dashboard/ws`. These are the concrete contract target: the decode tests assert the
 * SPA decodes these verbatim, and D7 should round-trip its serializer against them.
 *
 * Tagging: `DashboardPayload` is internally tagged `#[serde(tag = "type", rename_all =
 * "snake_case")]`. The `Monitor` arm NESTS a `DebugWsMessage` under `message` — and that
 * `DebugWsMessage` is ITSELF `#[serde(tag = "type", rename_all = "snake_case")]` (the real
 * `src/monitor.rs` enum: hello/request_upsert/segment_append/event_append/request_status/
 * request_remove/snapshot_done). It is NOT flattened (both carry `type`). See types.ts
 * "WIRE CONTRACT". Flow keys are `api_call_id`; FlowStatus = open/completed/failed/cancelled.
 *
 * Stored as JSON strings (not objects) so the test exercises the real `JSON.parse`
 * decode path a browser WebSocket `onmessage` would hit.
 */

/**
 * One `DashboardFrame` for the Monitor domain: ONE envelope (seq = `DebugUpdate.sequence`
 * = 6), `batch` = 5 sibling `DashboardPayload::Monitor`s, each nesting a real
 * itself-tagged `DebugWsMessage`. The dedup test asserts ALL 5 apply.
 *
 * The batch mirrors the Rust `monitor.rs` `snapshot()` replay ORDER for one flow:
 * `request_upsert` → `usage` (the D3 cumulative-token echo replayed right after the upsert) →
 * `segment_append`s → `request_status`. The `usage` sibling is the regression guard: a monitor
 * batch carrying it MUST validate WHOLLY (the union/guard include the `usage` arm) — otherwise the
 * whole replay is rejected and the theater never initializes.
 */
export const GOLDEN_MONITOR_FRAME_JSON = JSON.stringify({
  domain: 'monitor',
  seq: 6,
  batch: [
    {
      type: 'monitor',
      message: {
        type: 'request_upsert',
        request: {
          response_id: 'resp_001',
          model: 'llama-3.1-70b',
          started_at_ms: 1718900000000,
          updated_at_ms: 1718900000000,
          completed_at_ms: null,
          status: 'running',
          stats: {
            input_items: 3, tool_count: 0, turn_count: 1, user_messages: 1,
            assistant_messages: 0, system_messages: 1, developer_messages: 0,
            reasoning_items: 0, function_calls: 0, function_outputs: 0, tool_items: 0,
            input_chars: 42, instructions_chars: 0,
          },
          error: null,
        },
      },
    },
    // D3 cumulative-usage echo (keyed by response_id), replayed right after the upsert — the
    // sibling that used to drop the whole batch when the union lacked a `usage` arm.
    { type: 'monitor', message: { type: 'usage', response_id: 'resp_001', prompt: 812, completion: 0, total: 812, cached: 128, reasoning: 0 } },
    { type: 'monitor', message: { type: 'segment_append', response_id: 'resp_001', segment: { timestamp_ms: 1718900000001, kind: 'output', text: 'Hello' } } },
    { type: 'monitor', message: { type: 'segment_append', response_id: 'resp_001', segment: { timestamp_ms: 1718900000002, kind: 'output', text: ', world' } } },
    { type: 'monitor', message: { type: 'request_status', response_id: 'resp_001', status: 'completed', completed_at_ms: 1718900000003, error: null } },
  ],
});

/** A standalone `usage` frame (flow domain), keyed by `api_call_id`. */
// `usage` + `flow_status` carry BOTH `api_call_id` (REQUIRED authoritative key, D1/D13)
// AND `response_id` (OPTIONAL secondary correlation). D7's spec sketch field names
// (response_id/served_model) are illustrative + superseded — see types.ts "CONTRACT
// RECONCILIATION". These bytes are the exact orchestrator-reconciled D7 target.
export const GOLDEN_USAGE_FRAME_JSON = JSON.stringify({
  domain: 'flow',
  seq: 4,
  batch: [
    { type: 'usage', api_call_id: 'api_001', response_id: 'resp_001', prompt: 812, completion: 240, total: 1052, cached: 128, reasoning: 0 },
  ],
});

/** A `flow_status` frame (flow domain), keyed by `api_call_id`; carries `response_id` + `model_served` too. */
export const GOLDEN_FLOW_STATUS_FRAME_JSON = JSON.stringify({
  domain: 'flow',
  seq: 5,
  batch: [
    {
      type: 'flow_status',
      api_call_id: 'api_001',
      response_id: 'resp_001',
      status: 'completed',
      model_requested: 'gpt-4o',
      model_served: 'llama-3.1-70b',
      upstream_target: 'vllm-a',
      usage: { prompt: 812, completion: 512, total: 1324, cached: 128, reasoning: 0 },
      started_ms: 1718900000000,
      elapsed_ms: 3100,
    },
  ],
});

/** A `metric_tick` frame (metrics domain). */
export const GOLDEN_METRIC_TICK_FRAME_JSON = JSON.stringify({
  domain: 'metrics',
  seq: 2,
  batch: [
    {
      type: 'metric_tick',
      reqs_per_sec: 4.2, active_streams: 3, error_pct: 1.1,
      p50: 180, p95: 920, p99: 1840, tokens_per_sec: 142,
      prefill_tokens_per_sec: 10_430.25, decode_tokens_per_sec: 78.34, cost_per_min: 0.21,
      // The three per-metric measurability denominators (gap 01): `samples` (latency/error),
      // `usage_samples` (tok/s), `priced_samples` ($/min). The headline mirrors the m1 window.
      // Matches the Rust golden-shape test byte-for-byte.
      // Gap 07: the aggregate cost-confidence tag (mirrors the Rust golden-shape test).
      samples: 252, usage_samples: 250, prefill_samples: 248, decode_samples: 246,
      priced_samples: 240, cost_confidence: 'estimated',
      windows: {
        m1: { reqs_per_sec: 4.2, active_streams: 3, error_pct: 1.1, p50: 180, p95: 920, p99: 1840, tokens_per_sec: 142, prefill_tokens_per_sec: 10_430.25, decode_tokens_per_sec: 78.34, cost_per_min: 0.21, samples: 252, usage_samples: 250, prefill_samples: 248, decode_samples: 246, priced_samples: 240, cost_confidence: 'estimated' },
        m5: { reqs_per_sec: 3.8, active_streams: 3, error_pct: 1.0, p50: 175, p95: 900, p99: 1800, tokens_per_sec: 128, prefill_tokens_per_sec: 9_840.5, decode_tokens_per_sec: 74.2, cost_per_min: 0.19, samples: 1140, usage_samples: 1130, prefill_samples: 1120, decode_samples: 1118, priced_samples: 1100, cost_confidence: 'estimated' },
        h1: { reqs_per_sec: 2.9, active_streams: 2, error_pct: 0.8, p50: 160, p95: 850, p99: 1700, tokens_per_sec: 100, prefill_tokens_per_sec: 9_120, decode_tokens_per_sec: 69.8, cost_per_min: 0.15, samples: 10440, usage_samples: 10400, prefill_samples: 10350, decode_samples: 10320, priced_samples: 10000, cost_confidence: 'estimated' },
      },
    },
  ],
});

/** A `topology_update` frame (topology domain) — D4 `ProviderHealth` node shape. */
export const GOLDEN_TOPOLOGY_FRAME_JSON = JSON.stringify({
  domain: 'topology',
  seq: 2,
  batch: [
    {
      type: 'topology_update',
      nodes: [
        {
          id: 'vllm-a', name: 'vllm-a (8001)', route: null, base_url: 'http://localhost:8001',
          status: 'healthy', cooling_until_ms: null, last_error: null,
          served_count: 1280, failover_count: 0, consecutive_failures: 0,
          catalog_fetched_ms: 1718899995000, catalog_size: 12,
        },
      ],
      edges: [
        { from: 'gateway', to: 'vllm-a', throughput: 4.2, tokens_per_sec: 142, cost_per_sec: 0.003 },
      ],
    },
  ],
});

/** A malformed frame: valid envelope, but a payload arm missing required fields. */
export const MALFORMED_FRAME_JSON = JSON.stringify({
  domain: 'flow',
  seq: 99,
  batch: [
    // Missing the REQUIRED prompt/completion/total (cached/reasoning are OPTIONAL since
    // gap 07 — their absence alone is valid, so this stays invalid on the required counts).
    { type: 'usage', api_call_id: 'api_x' },
  ],
});

/**
 * GOLDEN BOOTSTRAP — the exact `window.__LLMCONDUIT_DASHBOARD__` object D7 embeds
 * (finding 6). Frozen field names: `authenticated`, `csrf_token`, `mutations_enabled`.
 */
export const GOLDEN_BOOTSTRAP = {
  authenticated: true,
  csrf_token: 'csrf-abc123',
  mutations_enabled: true,
} as const;
