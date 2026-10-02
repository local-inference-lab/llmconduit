import { describe, it, expect, beforeEach, vi } from 'vitest';
import { DashboardSocket, type WsLike } from './ws';
import { buildMonitorFrame, buildUsageFrame } from './mock';
import {
  GOLDEN_MONITOR_FRAME_JSON,
  GOLDEN_USAGE_FRAME_JSON,
  GOLDEN_FLOW_STATUS_FRAME_JSON,
  GOLDEN_METRIC_TICK_FRAME_JSON,
  GOLDEN_TOPOLOGY_FRAME_JSON,
  MALFORMED_FRAME_JSON,
} from './ws.fixtures';
import { dashboardStore } from '../store/dashboardStore';
import { isDashboardFrame, isSnapshotFrame, isDebugWsMessage } from './types';
import type { DashboardFrame, SnapshotFrame } from './types';

function snapshot(): SnapshotFrame {
  return {
    type: 'snapshot',
    cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
    flows: [],
    metrics: null,
    topology: null,
  };
}

/** A fully-valid MetricsResponse (passes `isMetricsResponse`) for staged-snapshot tests. */
const METRIC_WINDOW = {
  reqs_per_sec: 4.2, active_streams: 1, error_pct: 0,
  p50: 1, p95: 2, p99: 3, tokens_per_sec: 10,
  prefill_tokens_per_sec: 1000, decode_tokens_per_sec: 50, cost_per_min: 0.5,
  samples: 7, usage_samples: 7, prefill_samples: 7, decode_samples: 7,
  priced_samples: 7, cost_confidence: 'estimated' as const,
};
const METRICS_SNAP = {
  metrics_seq: 5, reqs_per_sec: 4.2, active_streams: 1, error_pct: 0,
  p50: 1, p95: 2, p99: 3, tokens_per_sec: 10,
  prefill_tokens_per_sec: 1000, decode_tokens_per_sec: 50, cost_per_min: 0.5,
  samples: 7, usage_samples: 7, prefill_samples: 7, decode_samples: 7,
  priced_samples: 7, cost_confidence: 'estimated' as const,
  windows: { m1: METRIC_WINDOW, m5: METRIC_WINDOW, h1: METRIC_WINDOW },
};

/** A controllable fake socket so tests can drive open/message/close/error. */
class FakeSocket implements WsLike {
  onopen: ((ev: unknown) => void) | null = null;
  onclose: ((ev: { code?: number } | undefined) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  closed = false;
  static instances: FakeSocket[] = [];
  constructor() {
    FakeSocket.instances.push(this);
  }
  send(): void {}
  close(): void {
    this.closed = true;
  }
}

describe('DashboardSocket — batched envelope decode + per-domain dedup', () => {
  let socket: DashboardSocket;

  beforeEach(() => {
    dashboardStore.getState().reset();
    socket = new DashboardSocket({ store: dashboardStore });
    socket.handleParsed(snapshot());
  });

  it('applies ALL sibling messages in a multi-message Monitor frame (none dropped by dedup)', () => {
    const frame = buildMonitorFrame(6, 'resp_X'); // 4 sibling DebugWsMessages under one seq
    expect(frame.batch).toHaveLength(4);

    expect(socket.applyFrame(frame)).toBe(true);
    expect(dashboardStore.getState().monitor).toHaveLength(4);
    expect(socket.getCursors().monitor).toBe(6);
  });

  it('decodes the GOLDEN nested Monitor fixture (the exact D7 bytes) → all 5 apply, incl. the usage sibling', () => {
    socket.handleParsed(JSON.parse(GOLDEN_MONITOR_FRAME_JSON));
    const monitor = dashboardStore.getState().monitor;
    // ALL 5 siblings applied — a batch carrying a D3 `usage` echo is NOT rejected wholesale.
    expect(monitor).toHaveLength(5);
    // The NESTED, itself-tagged DebugWsMessage decoded correctly (no flattening).
    expect(monitor[0]?.type).toBe('request_upsert');
    // The `usage` sibling validated + applied (it rides right after the upsert in the replay order).
    const usage = monitor[1];
    expect(usage?.type).toBe('usage');
    if (usage?.type === 'usage') {
      expect(usage.response_id).toBe('resp_001');
      expect(usage.total).toBe(812);
    }
    const last = monitor[4];
    expect(last?.type).toBe('request_status');
    if (last?.type === 'request_status') expect(last.status).toBe('completed');
  });

  it('a Monitor batch with a usage sibling validates as a frame and ALL siblings apply (finding 1 regression)', () => {
    // Construct a monitor frame whose batch interleaves a `usage` echo among the other arms — the
    // exact replay shape; isDashboardFrame must accept it and every sibling must land in the ring.
    const frame: DashboardFrame = {
      domain: 'monitor',
      seq: 9,
      batch: [
        { type: 'monitor', message: { type: 'request_upsert', request: {
          response_id: 'resp_Z', model: 'm', started_at_ms: 1, updated_at_ms: 1, completed_at_ms: null, status: 'running',
          stats: { input_items: 0, tool_count: 0, turn_count: 0, user_messages: 0, assistant_messages: 0, system_messages: 0, developer_messages: 0, reasoning_items: 0, function_calls: 0, function_outputs: 0, tool_items: 0, input_chars: 0, instructions_chars: 0 },
          error: null,
        } } },
        { type: 'monitor', message: { type: 'usage', response_id: 'resp_Z', prompt: 10, completion: 5, total: 15, cached: 0, reasoning: 0 } },
        { type: 'monitor', message: { type: 'segment_append', response_id: 'resp_Z', segment: { timestamp_ms: 2, kind: 'output', text: 'hi' } } },
      ],
    };
    // The whole envelope validates (the `usage` arm is in the union + guard).
    expect(isDashboardFrame(frame)).toBe(true);
    // And applying it lands ALL THREE siblings (no wholesale drop).
    expect(socket.applyFrame(frame)).toBe(true);
    expect(dashboardStore.getState().monitor).toHaveLength(3);
  });

  it('decodes the GOLDEN usage / flow_status / metric_tick / topology fixtures', () => {
    socket.handleParsed(JSON.parse(GOLDEN_USAGE_FRAME_JSON));
    socket.handleParsed(JSON.parse(GOLDEN_FLOW_STATUS_FRAME_JSON));
    socket.handleParsed(JSON.parse(GOLDEN_METRIC_TICK_FRAME_JSON));
    socket.handleParsed(JSON.parse(GOLDEN_TOPOLOGY_FRAME_JSON));
    const st = dashboardStore.getState();
    // Flows are keyed by api_call_id; the reconciled contract carries BOTH ids (R4).
    const flow = st.flows.get('api_001');
    expect(flow?.status).toBe('completed');
    expect(flow?.api_call_id).toBe('api_001');
    expect(flow?.response_id).toBe('resp_001');
    expect(st.metrics?.reqs_per_sec).toBe(4.2);
    expect(st.topologyNodes).toHaveLength(1);
    expect(st.topologyNodes[0]?.status).toBe('healthy');
    expect(st.topologyEdges).toHaveLength(1);
  });

  it('usage/flow_status: api_call_id REQUIRED, response_id OPTIONAL (R4 reconciliation)', () => {
    // response_id ABSENT → still valid (optional secondary correlation).
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'usage', api_call_id: 'api_x', prompt: 1, completion: 1, total: 2, cached: 0, reasoning: 0 }],
    })).toBe(true);
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'api_x', status: 'open', usage: null, started_ms: 1 }],
    })).toBe(true);
    // api_call_id MISSING (only the superseded response_id) → REJECTED.
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'usage', response_id: 'resp_x', prompt: 1, completion: 1, total: 2, cached: 0, reasoning: 0 }],
    })).toBe(false);
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', response_id: 'resp_x', status: 'open', usage: null, started_ms: 1 }],
    })).toBe(false);
  });

  it('usage: cached/reasoning are OPTIONAL — absent/null/0 all valid, distinct on the wire (gap 07 finding 1)', () => {
    // ABSENT cached/reasoning → valid (an unreported class is omitted, not a fake 0).
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'usage', api_call_id: 'api_x', prompt: 1, completion: 1, total: 2 }],
    })).toBe(true);
    // Explicit null → valid (UNAVAILABLE).
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'usage', api_call_id: 'api_x', prompt: 1, completion: 1, total: 2, cached: null, reasoning: null }],
    })).toBe(true);
    // Present 0 → valid (a measured zero, DISTINCT from absent/null).
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'usage', api_call_id: 'api_x', prompt: 1, completion: 1, total: 2, cached: 0, reasoning: 0 }],
    })).toBe(true);
    // The REQUIRED core counts are still enforced (a missing total is rejected).
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'usage', api_call_id: 'api_x', prompt: 1, completion: 1 }],
    })).toBe(false);
    // A non-numeric cached is rejected.
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'usage', api_call_id: 'api_x', prompt: 1, completion: 1, total: 2, cached: 'x' }],
    })).toBe(false);
  });

  it('drops a stale frame WHOLESALE when seq <= last_seq[domain]', () => {
    socket.applyFrame(buildMonitorFrame(6));
    expect(dashboardStore.getState().monitor).toHaveLength(4);

    expect(socket.applyFrame(buildMonitorFrame(6))).toBe(false); // duplicate
    expect(dashboardStore.getState().monitor).toHaveLength(4);

    expect(socket.applyFrame(buildMonitorFrame(5))).toBe(false); // stale
    expect(dashboardStore.getState().monitor).toHaveLength(4);

    expect(socket.applyFrame(buildMonitorFrame(7))).toBe(true); // fresh
    expect(dashboardStore.getState().monitor).toHaveLength(8);
  });

  it('dedups PER DOMAIN — a stale monitor seq does not block a flow frame at the same number', () => {
    socket.applyFrame(buildMonitorFrame(6));
    const flowFrame: DashboardFrame = {
      domain: 'flow',
      seq: 1,
      batch: [{
        type: 'flow_status', api_call_id: 'api_1', status: 'open',
        model_served: 'm', upstream_target: 'u', usage: null, started_ms: 1000, elapsed_ms: 100,
      }],
    };
    expect(socket.applyFrame(flowFrame)).toBe(true);
    expect(dashboardStore.getState().flowOrder).toContain('api_1');
    expect(socket.getCursors().flow).toBe(1);
    expect(socket.getCursors().monitor).toBe(6);
  });

  it('applies a standalone usage frame to the store (finding 9)', () => {
    // Seed a flow so usage can patch it (keyed by api_call_id).
    socket.applyFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'api_001', status: 'open', model_served: 'm', upstream_target: 'u', usage: null, started_ms: 1000, elapsed_ms: 10 }],
    });
    expect(socket.applyFrame(buildUsageFrame(2, 'api_001'))).toBe(true);
    expect(dashboardStore.getState().flows.get('api_001')?.usage).toEqual({
      prompt: 812, completion: 512, total: 1324, cached: 128, reasoning: 16,
    });
  });

  it('a usage frame with UNREPORTED cached/reasoning does not store a fabricated 0 (gap 07 finding 1)', () => {
    socket.applyFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'api_u', status: 'open', model_served: 'm', upstream_target: 'u', usage: null, started_ms: 1000, elapsed_ms: 10 }],
    });
    // A usage frame that OMITS cached/reasoning (the upstream never broke them out).
    expect(socket.applyFrame({
      domain: 'flow', seq: 2,
      batch: [{ type: 'usage', api_call_id: 'api_u', prompt: 100, completion: 50, total: 150 }],
    })).toBe(true);
    const usage = dashboardStore.getState().flows.get('api_u')?.usage;
    expect(usage?.prompt).toBe(100);
    expect(usage?.total).toBe(150);
    // The unreported classes are NOT a measured 0 — they are absent/nullish (render `—`).
    expect(usage?.cached ?? null).toBeNull();
    expect(usage?.reasoning ?? null).toBeNull();
    // And a present 0 is preserved as a measured zero, distinct from the above.
    socket.applyFrame({
      domain: 'flow', seq: 3,
      batch: [{ type: 'usage', api_call_id: 'api_u', prompt: 100, completion: 50, total: 150, cached: 0, reasoning: null }],
    });
    expect(dashboardStore.getState().flows.get('api_u')?.usage?.cached).toBe(0);
  });
});

describe('DashboardSocket — malformed frames do NOT mutate cursor or store (finding 6)', () => {
  let socket: DashboardSocket;
  beforeEach(() => {
    dashboardStore.getState().reset();
    socket = new DashboardSocket({ store: dashboardStore });
    socket.handleParsed(snapshot());
  });

  it('a payload-invalid frame is dropped without advancing the cursor or partial-applying', () => {
    const before = socket.getCursors().flow;
    socket.handleParsed(JSON.parse(MALFORMED_FRAME_JSON)); // valid envelope, bad usage payload
    expect(socket.getCursors().flow).toBe(before); // cursor unchanged → still replayable
    expect(dashboardStore.getState().flows.size).toBe(0); // nothing applied
  });

  it('applyFrame returns false for a malformed frame and leaves the cursor intact', () => {
    expect(socket.applyFrame(JSON.parse(MALFORMED_FRAME_JSON))).toBe(false);
    expect(socket.getCursors().flow).toBe(0);
  });

  it('a non-object / garbage value is ignored', () => {
    socket.handleParsed(42);
    socket.handleParsed(null);
    socket.handleParsed({ domain: 'flow' }); // missing seq + batch
    expect(dashboardStore.getState().flows.size).toBe(0);
  });

  it('fires onFrameApplied ONLY for accepted frames', () => {
    const onFrameApplied = vi.fn();
    const s = new DashboardSocket({ store: dashboardStore, onFrameApplied });
    s.handleParsed(snapshot());
    s.applyFrame(buildMonitorFrame(6)); // accepted
    s.applyFrame(buildMonitorFrame(6)); // duplicate → dropped
    s.applyFrame(JSON.parse(MALFORMED_FRAME_JSON)); // invalid → dropped
    expect(onFrameApplied).toHaveBeenCalledTimes(1);
    expect(onFrameApplied).toHaveBeenCalledWith('monitor');
  });
});

describe('frame validation — enums, unsigned-int seq, domain↔payload compatibility (finding 5)', () => {
  const goodFlow: DashboardFrame = {
    domain: 'flow',
    seq: 1,
    batch: [{ type: 'flow_status', api_call_id: 'a', status: 'open', usage: null, started_ms: 1 }],
  };

  it('accepts a well-formed frame', () => {
    expect(isDashboardFrame(goodFlow)).toBe(true);
  });

  it('rejects a NEGATIVE seq', () => {
    expect(isDashboardFrame({ ...goodFlow, seq: -1 })).toBe(false);
  });

  it('rejects a FRACTIONAL seq', () => {
    expect(isDashboardFrame({ ...goodFlow, seq: 1.5 })).toBe(false);
  });

  it('rejects an unknown flow status string', () => {
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'a', status: 'streaming', usage: null, started_ms: 1 }],
    })).toBe(false);
  });

  it('rejects a metric_tick payload under the FLOW domain (domain↔payload mismatch)', () => {
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'metric_tick', reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, cost_per_min: 0, samples: 1, usage_samples: 1, priced_samples: 1, windows: { m1: win(), m5: win(), h1: win() } }],
    })).toBe(false);
  });

  it('rejects a flow_status payload under the TOPOLOGY domain', () => {
    expect(isDashboardFrame({
      domain: 'topology', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'a', status: 'open', usage: null, started_ms: 1 }],
    })).toBe(false);
  });

  it('rejects an unknown provider status in a topology node', () => {
    expect(isDashboardFrame({
      domain: 'topology', seq: 1,
      batch: [{ type: 'topology_update', nodes: [{ id: 'x', name: 'x', status: 'degraded', served_count: 0, failover_count: 0, consecutive_failures: 0, catalog_size: 0 }], edges: [] }],
    })).toBe(false);
  });

  function win() {
    return { reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, cost_per_min: 0, samples: 1, usage_samples: 1, priced_samples: 1, cost_confidence: 'estimated' };
  }
});

describe('snapshot validation — full shape before applying (finding 4)', () => {
  it('accepts a fully-valid snapshot', () => {
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [{ api_call_id: 'a', method: 'POST', uri: '/v1/responses', status: 'open', started_ms: 1, cost_confidence: 'unavailable' }],
      metrics: null, topology: null,
    })).toBe(true);
  });

  it('rejects a snapshot whose cursors are not all unsigned ints', () => {
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: -1, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [], metrics: null, topology: null,
    })).toBe(false);
  });

  it('rejects a snapshot with an invalid summary (bad status)', () => {
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [{ api_call_id: 'a', method: 'POST', uri: '/x', status: 'bogus', started_ms: 1 }],
      metrics: null, topology: null,
    })).toBe(false);
  });

  // Gap 01 finding 3 — the new per-metric denominators are REQUIRED wire fields.
  it('rejects a snapshot whose metric window is MISSING usage_samples/priced_samples', () => {
    // A window with `samples` but lacking the new denominators must be rejected (the
    // frontend would otherwise be unable to tell tok/s + $/min availability apart).
    const badWindow = { reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, cost_per_min: 0, samples: 1 /* usage_samples/priced_samples missing */ };
    const badMetrics = { metrics_seq: 1, reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, cost_per_min: 0, samples: 1, usage_samples: 1, priced_samples: 1, windows: { m1: badWindow, m5: badWindow, h1: badWindow } };
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 1, topology_seq: 0, monitor_seq: 0 },
      flows: [], metrics: badMetrics, topology: null,
    })).toBe(false);
  });

  it('rejects a metric_tick frame whose headline is MISSING the new denominators', () => {
    const okWindow = { reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, cost_per_min: 0, samples: 1, usage_samples: 1, priced_samples: 1 };
    expect(isDashboardFrame({
      domain: 'metrics', seq: 1,
      // headline omits usage_samples/priced_samples → invalid metric_tick payload.
      batch: [{ type: 'metric_tick', reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, cost_per_min: 0, samples: 1, windows: { m1: okWindow, m5: okWindow, h1: okWindow } }],
    })).toBe(false);
  });

  it('accepts a metric_tick frame carrying the new denominators (regression guard)', () => {
    const okWindow = { reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, prefill_tokens_per_sec: 1000, decode_tokens_per_sec: 50, cost_per_min: 0, samples: 1, usage_samples: 1, prefill_samples: 1, decode_samples: 1, priced_samples: 1, cost_confidence: 'estimated' };
    expect(isDashboardFrame({
      domain: 'metrics', seq: 1,
      batch: [{ type: 'metric_tick', reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, prefill_tokens_per_sec: 1000, decode_tokens_per_sec: 50, cost_per_min: 0, samples: 1, usage_samples: 1, prefill_samples: 1, decode_samples: 1, priced_samples: 1, cost_confidence: 'estimated', windows: { m1: okWindow, m5: okWindow, h1: okWindow } }],
    })).toBe(true);
  });

  // Gap 07 — the metric_tick aggregate cost-confidence tag is REQUIRED + must be a valid enum.
  it('rejects a metric_tick frame MISSING cost_confidence (gap 07)', () => {
    const okWindow = { reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, cost_per_min: 0, samples: 1, usage_samples: 1, priced_samples: 1, cost_confidence: 'estimated' };
    expect(isDashboardFrame({
      domain: 'metrics', seq: 1,
      // headline omits cost_confidence → invalid metric_tick.
      batch: [{ type: 'metric_tick', reqs_per_sec: 1, active_streams: 1, error_pct: 0, p50: 1, p95: 1, p99: 1, tokens_per_sec: 1, cost_per_min: 0, samples: 1, usage_samples: 1, priced_samples: 1, windows: { m1: okWindow, m5: okWindow, h1: okWindow } }],
    })).toBe(false);
  });
});

describe('gap 07 — usage confidence wire validation', () => {
  // A flow_status carrying usage with UNREPORTED (absent) cached/reasoning is ACCEPTED —
  // the optional classes may be absent (unavailable), distinct from a present `0`.
  it('accepts a flow_status whose usage omits cached/reasoning (unreported ⇒ absent)', () => {
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'a', status: 'completed', started_ms: 1,
        usage: { prompt: 100, completion: 40, total: 140 } }],
    })).toBe(true);
    // A present measured `0` for cached/reasoning is likewise accepted (distinct from absent).
    expect(isDashboardFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'a', status: 'completed', started_ms: 1,
        usage: { prompt: 100, completion: 40, total: 140, cached: 0, reasoning: 0 } }],
    })).toBe(true);
  });

  // A snapshot summary MUST carry the per-flow cost_confidence tag, and it must be a valid enum.
  it('rejects a snapshot summary MISSING cost_confidence; rejects a bad enum value', () => {
    const base = { api_call_id: 'a', method: 'POST', uri: '/v1/responses', status: 'completed', started_ms: 1 };
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [base], // cost_confidence absent → rejected
      metrics: null, topology: null,
    })).toBe(false);
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [{ ...base, cost_confidence: 'bogus' }], // invalid enum → rejected
      metrics: null, topology: null,
    })).toBe(false);
    // A valid tag is accepted.
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [{ ...base, cost_confidence: 'estimated' }],
      metrics: null, topology: null,
    })).toBe(true);
  });
});

describe('ProviderHealth + price_table validation (findings 2 + 4)', () => {
  const node = (over: Record<string, unknown> = {}) => ({
    id: 'p', name: 'p', route: null, base_url: 'http://x', status: 'healthy',
    cooling_until_ms: null, last_error: null, served_count: 0, failover_count: 0,
    consecutive_failures: 0, catalog_fetched_ms: null, catalog_size: 0, ...over,
  });
  const topoFrame = (nodes: unknown[]) => ({ domain: 'topology', seq: 1, batch: [{ type: 'topology_update', nodes, edges: [] }] });

  it('accepts a complete D4 ProviderHealth (nullable keys present-but-null)', () => {
    expect(isDashboardFrame(topoFrame([node()]))).toBe(true);
  });

  it('rejects a node MISSING base_url (required non-null) — finding 2', () => {
    const n = node();
    delete (n as Record<string, unknown>).base_url;
    expect(isDashboardFrame(topoFrame([n]))).toBe(false);
  });

  it('rejects a node with null base_url (must be non-null) — finding 2', () => {
    expect(isDashboardFrame(topoFrame([node({ base_url: null })]))).toBe(false);
  });

  it('rejects a node MISSING a required nullable key (cooling_until_ms absent) — finding 2', () => {
    const n = node();
    delete (n as Record<string, unknown>).cooling_until_ms;
    expect(isDashboardFrame(topoFrame([n]))).toBe(false);
  });

  it('accepts a topology snapshot whose price_table entries are complete ModelPrice', () => {
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [],
      metrics: null,
      topology: { topology_seq: 1, nodes: [node()], edges: [], price_table: { 'gpt-4o': { input_per_1k: 0.005, output_per_1k: 0.015, cached_per_1k: 0.0025, cached_price_configured: true } } },
    })).toBe(true);
  });

  // Gap 07: the cached-price PRESENCE flag is a REQUIRED additive ModelPrice field.
  it('rejects a price_table entry MISSING cached_price_configured (gap 07 presence flag)', () => {
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [],
      metrics: null,
      // cached_price_configured absent → an incomplete ModelPrice → rejected.
      topology: { topology_seq: 1, nodes: [node()], edges: [], price_table: { m: { input_per_1k: 0.005, output_per_1k: 0.015, cached_per_1k: 0.0025 } } },
    })).toBe(false);
  });

  it('rejects a price_table entry with a non-finite number (finding 4)', () => {
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [],
      metrics: null,
      topology: { topology_seq: 1, nodes: [node()], edges: [], price_table: { m: { input_per_1k: Number.POSITIVE_INFINITY, output_per_1k: 0, cached_per_1k: 0 } } },
    })).toBe(false);
  });

  it('rejects a price_table entry missing a field (finding 4)', () => {
    expect(isSnapshotFrame({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [],
      metrics: null,
      topology: { topology_seq: 1, nodes: [node()], edges: [], price_table: { m: { input_per_1k: 0.001, output_per_1k: 0.001 } } },
    })).toBe(false);
  });
});

describe('DebugWsMessage nested validation — full DTO, no skipped casts (findings 1 + 3)', () => {
  const fullRequest = {
    response_id: 'r', model: 'm', started_at_ms: 1, updated_at_ms: 1, completed_at_ms: null,
    status: 'running',
    stats: { input_items: 0, tool_count: 0, turn_count: 0, user_messages: 0, assistant_messages: 0, system_messages: 0, developer_messages: 0, reasoning_items: 0, function_calls: 0, function_outputs: 0, tool_items: 0, input_chars: 0, instructions_chars: 0 },
    error: null,
  };

  it('accepts a complete request_upsert (full DebugRequest + stats)', () => {
    expect(isDebugWsMessage({ type: 'request_upsert', request: fullRequest })).toBe(true);
  });

  it('rejects request_upsert whose request is MISSING stats (finding 3)', () => {
    const req = { ...fullRequest } as Record<string, unknown>;
    delete req.stats;
    expect(isDebugWsMessage({ type: 'request_upsert', request: req })).toBe(false);
  });

  it('rejects request_upsert with an incomplete stats object (finding 3)', () => {
    expect(isDebugWsMessage({ type: 'request_upsert', request: { ...fullRequest, stats: { input_items: 1 } } })).toBe(false);
  });

  it('rejects a bad DebugRequest status enum (finding 3)', () => {
    expect(isDebugWsMessage({ type: 'request_upsert', request: { ...fullRequest, status: 'pending' } })).toBe(false);
  });

  it('accepts a complete segment_append; rejects a bad segment kind (finding 3)', () => {
    expect(isDebugWsMessage({ type: 'segment_append', response_id: 'r', segment: { timestamp_ms: 1, kind: 'output', text: 'hi' } })).toBe(true);
    expect(isDebugWsMessage({ type: 'segment_append', response_id: 'r', segment: { timestamp_ms: 1, kind: 'banana', text: 'hi' } })).toBe(false);
  });

  it('validates event_append images fully (rejects a malformed image) — finding 3', () => {
    const okEvent = { timestamp_ms: 1, kind: 'k', summary: 's', payload_preview: null, images: [{ id: 'i', label: 'l', path: 'p', mime_type: 'image/png', size_bytes: 10 }] };
    expect(isDebugWsMessage({ type: 'event_append', response_id: 'r', event: okEvent })).toBe(true);
    const badImg = { ...okEvent, images: [{ id: 'i' /* missing label/path/mime */ }] };
    expect(isDebugWsMessage({ type: 'event_append', response_id: 'r', event: badImg })).toBe(false);
  });

  it('rejects a request_status with a bad status enum (finding 3)', () => {
    expect(isDebugWsMessage({ type: 'request_status', response_id: 'r', status: 'idle', completed_at_ms: null, error: null })).toBe(false);
  });

  it('accepts a D3 usage message; rejects one with missing/non-finite token counts (finding 1)', () => {
    expect(isDebugWsMessage({ type: 'usage', response_id: 'r', prompt: 1, completion: 2, total: 3, cached: 0, reasoning: 0 })).toBe(true);
    // Missing a token field → rejected.
    expect(isDebugWsMessage({ type: 'usage', response_id: 'r', prompt: 1, completion: 2, total: 3, cached: 0 })).toBe(false);
    // Non-finite token → rejected.
    expect(isDebugWsMessage({ type: 'usage', response_id: 'r', prompt: Number.NaN, completion: 2, total: 3, cached: 0, reasoning: 0 })).toBe(false);
    // Missing response_id → rejected.
    expect(isDebugWsMessage({ type: 'usage', prompt: 1, completion: 2, total: 3, cached: 0, reasoning: 0 })).toBe(false);
  });
});

describe('DashboardSocket — auth failure vs transient blip + reconnect (findings 3+4+7)', () => {
  /** Captured pending reconnect timers so a test can fire them synchronously. */
  let pendingTimers: Array<() => void>;
  function makeTimers() {
    pendingTimers = [];
    const setTimer = (cb: () => void) => {
      pendingTimers.push(cb);
      return pendingTimers.length as unknown as ReturnType<typeof setTimeout>;
    };
    const clearTimer = () => {};
    return { setTimer, clearTimer };
  }
  function flushTimers() {
    const due = pendingTimers;
    pendingTimers = [];
    for (const cb of due) cb();
  }

  beforeEach(() => {
    dashboardStore.getState().reset();
    FakeSocket.instances = [];
  });

  it('an EXPLICIT 4401 close bounces to login (no reconnect)', () => {
    const onUnauthorized = vi.fn();
    const { setTimer, clearTimer } = makeTimers();
    const socket = new DashboardSocket({ store: dashboardStore, factory: () => new FakeSocket(), onUnauthorized, setTimer, clearTimer });
    socket.connect();
    FakeSocket.instances[0]!.onclose?.({ code: 4401 });
    expect(onUnauthorized).toHaveBeenCalledOnce();
    expect(pendingTimers).toHaveLength(0); // no reconnect scheduled for a confirmed auth failure
  });

  it('a 1006 blip with a STILL-VALID session reconnects — does NOT log out (finding 7)', async () => {
    const onUnauthorized = vi.fn();
    const probeAuth = vi.fn().mockResolvedValue(true); // session still valid
    const { setTimer, clearTimer } = makeTimers();
    const socket = new DashboardSocket({ store: dashboardStore, factory: () => new FakeSocket(), onUnauthorized, probeAuth, setTimer, clearTimer });
    socket.connect();
    FakeSocket.instances[0]!.onclose?.({ code: 1006 });
    // The probe runs; await its resolution.
    await Promise.resolve();
    await Promise.resolve();
    expect(probeAuth).toHaveBeenCalledOnce();
    expect(onUnauthorized).not.toHaveBeenCalled(); // NOT logged out
    expect(pendingTimers.length).toBeGreaterThan(0); // a reconnect was scheduled
    // Firing the reconnect timer opens a fresh socket.
    flushTimers();
    expect(FakeSocket.instances).toHaveLength(2);
  });

  it('a 1006 blip whose probe returns 401 bounces to login (finding 7)', async () => {
    const onUnauthorized = vi.fn();
    const probeAuth = vi.fn().mockResolvedValue(false); // probe got a 401
    const { setTimer, clearTimer } = makeTimers();
    const socket = new DashboardSocket({ store: dashboardStore, factory: () => new FakeSocket(), onUnauthorized, probeAuth, setTimer, clearTimer });
    socket.connect();
    FakeSocket.instances[0]!.onclose?.({ code: 1006 });
    await Promise.resolve();
    await Promise.resolve();
    expect(probeAuth).toHaveBeenCalledOnce();
    expect(onUnauthorized).toHaveBeenCalledOnce(); // bounced
    expect(pendingTimers).toHaveLength(0); // no reconnect after a confirmed auth failure
  });

  it('an onerror with a valid-session probe reconnects (not logout)', async () => {
    const onUnauthorized = vi.fn();
    const probeAuth = vi.fn().mockResolvedValue(true);
    const { setTimer, clearTimer } = makeTimers();
    const socket = new DashboardSocket({ store: dashboardStore, factory: () => new FakeSocket(), onUnauthorized, probeAuth, setTimer, clearTimer });
    socket.connect();
    FakeSocket.instances[0]!.onerror?.(new Event('error'));
    await Promise.resolve();
    await Promise.resolve();
    expect(onUnauthorized).not.toHaveBeenCalled();
    expect(pendingTimers.length).toBeGreaterThan(0);
  });

  it('a clean close (code 1000) does NOT bounce or reconnect', () => {
    const onUnauthorized = vi.fn();
    const { setTimer, clearTimer } = makeTimers();
    const socket = new DashboardSocket({ store: dashboardStore, factory: () => new FakeSocket(), onUnauthorized, setTimer, clearTimer });
    socket.connect();
    FakeSocket.instances[0]!.onclose?.({ code: 1000 });
    expect(onUnauthorized).not.toHaveBeenCalled();
    expect(dashboardStore.getState().connection).toBe('closed');
    expect(pendingTimers).toHaveLength(0);
  });

  it('disconnect() cancels a pending reconnect (no socket re-open)', async () => {
    const probeAuth = vi.fn().mockResolvedValue(true);
    const { setTimer, clearTimer } = makeTimers();
    const socket = new DashboardSocket({ store: dashboardStore, factory: () => new FakeSocket(), probeAuth, setTimer, clearTimer });
    socket.connect();
    FakeSocket.instances[0]!.onclose?.({ code: 1006 });
    await Promise.resolve();
    await Promise.resolve();
    expect(pendingTimers.length).toBeGreaterThan(0);
    socket.disconnect(); // cancels the reconnect
    flushTimers(); // even if a stale timer fires, stopped guards it
    expect(FakeSocket.instances).toHaveLength(1); // no new socket opened
  });

  it('StrictMode reconnect: an OLD socket late close does NOT clobber the live socket (finding 4)', () => {
    const onUnauthorized = vi.fn();
    const { setTimer, clearTimer } = makeTimers();
    const socket = new DashboardSocket({ store: dashboardStore, factory: () => new FakeSocket(), onUnauthorized, setTimer, clearTimer });
    // mount → unmount → remount (StrictMode dev double-invoke).
    socket.connect();
    const oldWs = FakeSocket.instances[0]!;
    // Capture the OLD close handler BEFORE disconnect detaches it, to simulate a close
    // event the browser had already queued for the stale socket.
    const staleOnClose = oldWs.onclose;
    socket.disconnect();
    socket.connect();
    const newWs = FakeSocket.instances[1]!;
    expect(oldWs).not.toBe(newWs);

    // The captured OLD handler fires a late abnormal close AFTER the new socket is live.
    staleOnClose?.({ code: 1006 });

    // The generation guard makes the stale event a no-op: no bounce, no reconnect, no clobber.
    expect(onUnauthorized).not.toHaveBeenCalled();
    expect(pendingTimers).toHaveLength(0);
    // The new socket still drives state — prove it by applying a snapshot through it.
    newWs.onmessage?.({ data: JSON.stringify(snapshot()) });
    expect(dashboardStore.getState().connection).toBe('live');
  });

  it('a STALE probe resolving after disconnect/remount does NOT bounce a newer connection (finding 5)', async () => {
    const onUnauthorized = vi.fn();
    // A probe we resolve MANUALLY, after the connection has been torn down + remounted.
    let resolveProbe!: (authed: boolean) => void;
    const probeAuth = vi.fn().mockImplementation(() => new Promise<boolean>((res) => { resolveProbe = res; }));
    const { setTimer, clearTimer } = makeTimers();
    const socket = new DashboardSocket({ store: dashboardStore, factory: () => new FakeSocket(), onUnauthorized, probeAuth, setTimer, clearTimer });
    socket.connect();
    // Drop → probe starts (bound to this generation), pending.
    FakeSocket.instances[0]!.onclose?.({ code: 1006 });
    expect(probeAuth).toHaveBeenCalledOnce();
    // Disconnect + reconnect bumps the generation while the probe is still pending.
    socket.disconnect();
    socket.connect();
    // NOW the stale probe resolves with a 401-equivalent.
    resolveProbe(false);
    await Promise.resolve();
    await Promise.resolve();
    // The stale probe must be ignored: the NEWER connection is NOT logged out.
    expect(onUnauthorized).not.toHaveBeenCalled();
  });
});

describe('DashboardSocket — time travel (seek/live shadow buffer)', () => {
  let socket: DashboardSocket;
  beforeEach(() => {
    dashboardStore.getState().reset();
    socket = new DashboardSocket({ store: dashboardStore });
    socket.handleParsed(snapshot());
  });

  it('buffers frames while seeking and replays them on live()', () => {
    socket.seek();
    expect(socket.isPaused()).toBe(true);

    socket.handleParsed(buildMonitorFrame(6));
    socket.handleParsed(buildMonitorFrame(7));
    expect(dashboardStore.getState().monitor).toHaveLength(0);
    expect(socket.shadowBufferLength()).toBe(2);

    socket.live();
    expect(socket.isPaused()).toBe(false);
    expect(dashboardStore.getState().monitor).toHaveLength(8);
    expect(socket.shadowBufferLength()).toBe(0);
  });

  it('live() restores the up-to-date live baseline (frozen applySeekCut cut is gone, nothing rewound) (R2 finding 1)', () => {
    // A live flow row exists before seeking.
    const liveFlow: DashboardFrame = {
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'api_live', status: 'open', usage: null, started_ms: 1000 }],
    };
    expect(socket.applyFrame(liveFlow)).toBe(true);
    expect(dashboardStore.getState().flows.has('api_live')).toBe(true);
    expect(socket.getCursors().flow).toBe(1);

    // Drag-start pauses the live feed; the socket captures the live baseline (incl. `api_live`).
    socket.seek();
    expect(socket.isPaused()).toBe(true);

    // The Scrubber lands the FROZEN historical cut: it OVERWRITES the store with different rows
    // (`api_frozen`, no `api_live`) and flips `connection='seeking'` — exactly what `applySeekCut`
    // does on the real snapshot resolve.
    dashboardStore.getState().applySeekCut({
      rows: [{ api_call_id: 'api_frozen', method: 'POST', uri: '/v1/responses', status: 'completed', started_ms: 500, cost_confidence: 'unavailable' }],
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      atMs: 500, monitorSeq: 0, metrics: null, topology: null,
    });
    expect(dashboardStore.getState().connection).toBe('seeking');
    expect(dashboardStore.getState().flows.has('api_frozen')).toBe(true);
    expect(dashboardStore.getState().flows.has('api_live')).toBe(false); // frozen cut hid the live row

    // While paused, a NEW live flow frame arrives and shadow-buffers (a row that appeared after the
    // cut). Its seq must be > the live cursor so it is not deduped on replay.
    const bufferedFlow: DashboardFrame = {
      domain: 'flow', seq: 2,
      batch: [{ type: 'flow_status', api_call_id: 'api_buffered', status: 'open', usage: null, started_ms: 2000 }],
    };
    socket.handleParsed(bufferedFlow);
    expect(socket.shadowBufferLength()).toBe(1);
    expect(dashboardStore.getState().flows.has('api_buffered')).toBe(false); // buffered, not applied

    // Resume LIVE: the frozen cut is discarded, the live baseline is restored, then the buffered
    // frame replays on top.
    socket.live();

    const st = dashboardStore.getState();
    expect(st.connection).toBe('live');
    // The up-to-date live row is back (restored from the baseline) — NOT rewound/missing.
    expect(st.flows.has('api_live')).toBe(true);
    // The frame that arrived during the pause was replayed on top of the live store.
    expect(st.flows.has('api_buffered')).toBe(true);
    // The frozen historical row is GONE (the cut was fully discarded on resume).
    expect(st.flows.has('api_frozen')).toBe(false);
    // The flow cursor reflects the live progression (1 from baseline → 2 from the replayed frame),
    // not the frozen cut's 0.
    expect(st.cursors.flow_seq).toBe(2);
    expect(socket.getCursors().flow).toBe(2);
    expect(st.seekAtMs).toBeNull();
    expect(socket.shadowBufferLength()).toBe(0);
  });

  it('live() resume NEVER exposes `seeking` with restored/replayed live rows — the flip is atomic with the baseline restore, before replay (R3 finding 1)', () => {
    // A live flow row + advanced cursor exist before seeking (the baseline to restore).
    const liveFlow: DashboardFrame = {
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'api_live', status: 'open', usage: null, started_ms: 1000 }],
    };
    expect(socket.applyFrame(liveFlow)).toBe(true);

    // Drag-start: pause + capture the live baseline, then land the FROZEN cut ('seeking').
    socket.seek();
    dashboardStore.getState().applySeekCut({
      rows: [{ api_call_id: 'api_frozen', method: 'POST', uri: '/v1/responses', status: 'completed', started_ms: 500, cost_confidence: 'unavailable' }],
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      atMs: 500, monitorSeq: 0, metrics: null, topology: null,
    });
    expect(dashboardStore.getState().connection).toBe('seeking');

    // A live frame arrives mid-seek and shadow-buffers (replayed on resume).
    const bufferedFlow: DashboardFrame = {
      domain: 'flow', seq: 2,
      batch: [{ type: 'flow_status', api_call_id: 'api_buffered', status: 'open', usage: null, started_ms: 2000 }],
    };
    socket.handleParsed(bufferedFlow);

    // Invariant checker on EVERY store transition for the duration of live(): if `connection` is ever
    // observed `'seeking'` while a LIVE (non-frozen) row is present, the flip was NOT atomic with the
    // baseline restore — the exact non-atomic window D10 must never see. The frozen-only cut state
    // ('seeking' with `api_frozen` and NO live/buffered row) is the legitimate pre-resume state and
    // is not a violation.
    const violations: string[] = [];
    const unsub = dashboardStore.subscribe((s) => {
      if (s.connection === 'seeking') {
        if (s.flows.has('api_live')) violations.push('seeking with restored live row (api_live)');
        if (s.flows.has('api_buffered')) violations.push('seeking with replayed buffered row (api_buffered)');
      }
    });

    socket.live();
    unsub();

    // No transition during resume showed live data under `connection==='seeking'`.
    expect(violations).toEqual([]);
    // And the end state is correct: live, with the restored + replayed rows and the frozen cut gone.
    const st = dashboardStore.getState();
    expect(st.connection).toBe('live');
    expect(st.flows.has('api_live')).toBe(true);
    expect(st.flows.has('api_buffered')).toBe(true);
    expect(st.flows.has('api_frozen')).toBe(false);
  });

  it('live() with a STAGED reconnect snapshot flips to live ATOMICALLY with the snapshot install — never `seeking` with the staged live rows/metrics, then buffered frames replay on top (R6)', () => {
    // A live row + advanced cursor exist before seeking.
    const liveFlow: DashboardFrame = {
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'api_live', status: 'open', usage: null, started_ms: 1000 }],
    };
    expect(socket.applyFrame(liveFlow)).toBe(true);

    // Drag-start: pause, then land the FROZEN cut ('seeking') via the real Scrubber path.
    socket.seek();
    dashboardStore.getState().applySeekCut({
      rows: [{ api_call_id: 'api_frozen', method: 'POST', uri: '/v1/responses', status: 'completed', started_ms: 500, cost_confidence: 'unavailable' }],
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      atMs: 500, monitorSeq: 0, metrics: null, topology: null,
    });
    expect(dashboardStore.getState().connection).toBe('seeking');

    // A RECONNECT delivers a fresh snapshot mid-seek: it STAGES (pendingSnapshot), NOT applied over
    // the frozen cut. It carries the authoritative live rows (`api_snap`) + cursors + metrics.
    const reconnectSnap: SnapshotFrame = {
      type: 'snapshot',
      cursors: { flow_seq: 5, metrics_seq: 5, topology_seq: 5, monitor_seq: 5 },
      flows: [{ api_call_id: 'api_snap', method: 'POST', uri: '/v1/responses', status: 'open', started_ms: 3000, cost_confidence: 'unavailable' }],
      metrics: METRICS_SNAP,
      topology: null,
    };
    socket.handleParsed(reconnectSnap);
    // Staged, not applied: frozen cut intact, still seeking, no snapshot rows/metrics yet.
    expect(dashboardStore.getState().connection).toBe('seeking');
    expect(dashboardStore.getState().flows.has('api_snap')).toBe(false);
    expect(dashboardStore.getState().metrics).toBeNull();

    // A live frame ALSO arrives mid-seek and shadow-buffers (replayed on resume, on top of the
    // staged snapshot). Its seq must be > the staged snapshot's cursor so it is not deduped.
    const bufferedFlow: DashboardFrame = {
      domain: 'flow', seq: 6,
      batch: [{ type: 'flow_status', api_call_id: 'api_buffered', status: 'open', usage: null, started_ms: 4000 }],
    };
    socket.handleParsed(bufferedFlow);
    expect(socket.shadowBufferLength()).toBe(1);

    // Invariant checker on EVERY store transition for the duration of live(): if `connection` is ever
    // observed `'seeking'` while the STAGED snapshot's live data (rows OR metrics) is present, the flip
    // was NOT atomic with the snapshot install — the exact non-atomic window D10 must never see. The
    // frozen-only cut state ('seeking' with `api_frozen`, no snapshot/buffered data) is legitimate.
    const violations: string[] = [];
    const unsub = dashboardStore.subscribe((s) => {
      if (s.connection === 'seeking') {
        if (s.flows.has('api_snap')) violations.push('seeking with staged snapshot row (api_snap)');
        if (s.flows.has('api_buffered')) violations.push('seeking with replayed buffered row (api_buffered)');
        if (s.metrics !== null) violations.push('seeking with staged snapshot metrics');
      }
    });

    // Explicit resume: install the staged snapshot AS the live store (atomic flip to live), THEN
    // replay the shadow-buffered frame on top.
    socket.live();
    unsub();

    // No transition during resume showed staged/replayed live data under `connection==='seeking'`.
    expect(violations).toEqual([]);
    // End state: live, snapshot rows/cursors/metrics installed, buffered frame replayed, frozen gone.
    const st = dashboardStore.getState();
    expect(st.connection).toBe('live');
    expect(st.flows.has('api_snap')).toBe(true);    // staged snapshot row
    expect(st.flows.has('api_buffered')).toBe(true); // replayed buffered frame on top
    expect(st.flows.has('api_frozen')).toBe(false);  // frozen cut discarded
    expect(st.metrics?.reqs_per_sec).toBe(4.2);      // staged snapshot metrics installed
    expect(st.cursors.flow_seq).toBe(6);             // snapshot cursor 5 → replayed frame 6
    expect(socket.getCursors().flow).toBe(6);
    expect(st.seekAtMs).toBeNull();
    expect(socket.shadowBufferLength()).toBe(0);
  });

  it('a RECONNECT snapshot arriving during seek does NOT clobber the frozen cut or flip to live (finding 6)', () => {
    // Freeze: apply a monitor frame, then seek. `seek()` PAUSES the live feed; the store is flipped
    // to the frozen 'seeking' cut separately (the Scrubber does this atomically via `applySeekCut` —
    // D11 finding 1). Here we model the active-seek state explicitly so the staging assertions below
    // verify a reconnect leaves the frozen cut + 'seeking' connection intact.
    socket.applyFrame(buildMonitorFrame(6));
    expect(dashboardStore.getState().monitor).toHaveLength(4);
    socket.seek();
    expect(socket.isPaused()).toBe(true);
    dashboardStore.getState().enterSeek(Date.now());
    expect(dashboardStore.getState().connection).toBe('seeking');

    // A reconnect delivers a FRESH snapshot (different cut: empty flows, new cursors).
    const reconnectSnap: SnapshotFrame = {
      type: 'snapshot',
      cursors: { flow_seq: 99, metrics_seq: 99, topology_seq: 99, monitor_seq: 99 },
      flows: [], metrics: null, topology: null,
    };
    socket.handleParsed(reconnectSnap);

    // The frozen cut is INTACT: monitor still 4, connection still seeking, cursors unchanged.
    expect(dashboardStore.getState().monitor).toHaveLength(4);
    expect(dashboardStore.getState().connection).toBe('seeking');
    expect(socket.getCursors().monitor).toBe(6);

    // Explicit resume applies the staged snapshot (new cut) + flips to live.
    socket.live();
    expect(dashboardStore.getState().connection).toBe('live');
    expect(socket.getCursors().monitor).toBe(99);
  });

  it('a transient WS drop + reconnect DURING seek PRESERVES the frozen cut (seekAtMs/seekMonitorSeq intact); only live() resumes (R4 finding 1)', async () => {
    // Captured reconnect timers so the test can fire them synchronously.
    const pendingTimers: Array<() => void> = [];
    const setTimer = (cb: () => void) => {
      pendingTimers.push(cb);
      return pendingTimers.length as unknown as ReturnType<typeof setTimeout>;
    };
    const clearTimer = () => {};
    const probeAuth = vi.fn().mockResolvedValue(true); // session still valid → reconnect (no logout)

    dashboardStore.getState().reset();
    FakeSocket.instances = [];
    const s = new DashboardSocket({ store: dashboardStore, factory: () => new FakeSocket(), probeAuth, setTimer, clearTimer });
    s.connect();
    // Snapshot → live.
    FakeSocket.instances[0]!.onmessage?.({ data: JSON.stringify(snapshot()) });
    expect(dashboardStore.getState().connection).toBe('live');

    // Drag-start: pause + land the FROZEN cut via applySeekCut (the real Scrubber path). This stamps
    // seekAtMs/seekMonitorSeq and flips connection='seeking'.
    s.seek();
    dashboardStore.getState().applySeekCut({
      rows: [{ api_call_id: 'api_frozen', method: 'POST', uri: '/v1/responses', status: 'completed', started_ms: 500, cost_confidence: 'unavailable' }],
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 42 },
      atMs: 500, monitorSeq: 42, metrics: null, topology: null,
    });
    expect(dashboardStore.getState().connection).toBe('seeking');
    expect(dashboardStore.getState().seekAtMs).toBe(500);
    expect(dashboardStore.getState().seekMonitorSeq).toBe(42);

    // A transient WS drop fires mid-seek (the bug: this used to setConnection('connecting'), clearing
    // the seek freeze and yanking D10/D12 off the frozen cut BEFORE the user pressed LIVE).
    FakeSocket.instances[0]!.onclose?.({ code: 1006 });
    await Promise.resolve();
    await Promise.resolve();

    // The freeze is INTACT across the drop + probe: still seeking, seek fields preserved, frozen row present.
    expect(dashboardStore.getState().connection).toBe('seeking');
    expect(dashboardStore.getState().seekAtMs).toBe(500);
    expect(dashboardStore.getState().seekMonitorSeq).toBe(42);
    expect(dashboardStore.getState().flows.has('api_frozen')).toBe(true);

    // The reconnect timer fires → a fresh socket opens. The freeze must STILL survive the reconnect.
    expect(pendingTimers.length).toBeGreaterThan(0);
    const fire = pendingTimers.splice(0);
    for (const cb of fire) cb();
    expect(FakeSocket.instances).toHaveLength(2);
    expect(dashboardStore.getState().connection).toBe('seeking');
    expect(dashboardStore.getState().seekAtMs).toBe(500);

    // The reconnected socket's snapshot is STAGED (not applied over the frozen cut).
    const reconnectSnap: SnapshotFrame = {
      type: 'snapshot',
      cursors: { flow_seq: 7, metrics_seq: 7, topology_seq: 7, monitor_seq: 7 },
      flows: [], metrics: null, topology: null,
    };
    FakeSocket.instances[1]!.onmessage?.({ data: JSON.stringify(reconnectSnap) });
    expect(dashboardStore.getState().connection).toBe('seeking'); // still frozen
    expect(dashboardStore.getState().flows.has('api_frozen')).toBe(true);
    expect(dashboardStore.getState().seekAtMs).toBe(500);

    // ONLY an explicit live() exits the freeze — applying the staged reconnect snapshot.
    s.live();
    expect(dashboardStore.getState().connection).toBe('live');
    expect(dashboardStore.getState().seekAtMs).toBeNull();
    expect(dashboardStore.getState().seekMonitorSeq).toBeNull();
    expect(dashboardStore.getState().flows.has('api_frozen')).toBe(false);
    expect(s.getCursors().monitor).toBe(7); // staged reconnect snapshot's cursors took effect
  });
});
