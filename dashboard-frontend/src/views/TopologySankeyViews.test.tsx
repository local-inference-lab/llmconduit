/**
 * View-level integration for the D12 Topology + Sankey screens: the click→shared-filter→navigate
 * cross-link and the SEEK (D11) frozen-cut rendering. The viz internals (layout, colors, particles)
 * are covered by the component tests; here we assert the VIEW wiring.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { cleanup, fireEvent, act } from '@testing-library/react';
import { TopologyView } from './TopologyView';
import { SankeyView } from './SankeyView';
import { dashboardStore } from '../store/dashboardStore';
import { flowFilterStore } from '../store/flowFilterStore';
import { renderWithQuery, resetWorld } from '../components/testHarness';
import type { ProviderHealth, TopologyResponse, FlowSummary, MetricsResponse, Usage } from '../api/types';

/** A metrics sample carrying a `cost_per_min` (the authoritative `$`/min source — finding 3).
 * Gap 07: `priced_samples`/`cost_confidence` are overridable so the `$/min` readout's
 * unavailable (`—/min`) + estimated-label branches can be exercised. */
function metrics(
  costPerMin: number,
  over: Partial<Pick<MetricsResponse, 'priced_samples' | 'cost_confidence'>> = {},
): MetricsResponse {
  const w = { reqs_per_sec: 0, active_streams: 0, error_pct: 0, p50: 0, p95: 0, p99: 0, tokens_per_sec: 0, prefill_tokens_per_sec: 0, decode_tokens_per_sec: 0, cost_per_min: costPerMin, samples: 1, usage_samples: 1, prefill_samples: 1, decode_samples: 1, priced_samples: 1, cost_confidence: 'estimated' as const, ...over };
  return { metrics_seq: 1, ...w, windows: { m1: w, m5: w, h1: w } };
}
function usage(over: Partial<Usage> = {}): Usage {
  return { prompt: 0, completion: 0, total: 0, cached: 0, reasoning: 0, ...over };
}

function provider(over: Partial<ProviderHealth>): ProviderHealth {
  return {
    id: 'p', name: 'p', route: null, base_url: 'http://x', status: 'healthy',
    cooling_until_ms: null, last_error: null, served_count: 0, failover_count: 0,
    consecutive_failures: 0, catalog_fetched_ms: null, catalog_size: 0, ...over,
  };
}

const TOPOLOGY: TopologyResponse = {
  topology_seq: 1,
  nodes: [
    provider({ id: 'vllm-a', name: 'vllm-a', status: 'healthy' }),
    provider({ id: 'vllm-b', name: 'vllm-b', status: 'cooling', cooling_until_ms: Date.now() + 9000 }),
  ],
  edges: [{ from: 'gateway', to: 'vllm-a', throughput: 3, tokens_per_sec: 90, cost_per_sec: 0.002 }],
  price_table: { 'gpt-4o': { input_per_1k: 0.005, output_per_1k: 0.015, cached_per_1k: 0.0025, cached_price_configured: true } },
};

function flow(over: Partial<FlowSummary>): FlowSummary {
  return {
    api_call_id: `api_${Math.random().toString(36).slice(2, 8)}`, method: 'POST', uri: '/v1/responses', cost_confidence: 'unavailable',
    status: 'completed', started_ms: Date.now() - 2000, finished_ms: Date.now() - 500, ...over,
  };
}

function seedLiveTopology(): void {
  act(() => {
    dashboardStore.getState().applySnapshot({
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 1, monitor_seq: 0 },
      flows: [],
      metrics: null,
      topology: TOPOLOGY,
    });
    dashboardStore.getState().setConnection('live');
  });
}

beforeEach(() => {
  // resetWorld installs the real (authenticated) bootstrap → mock fetch is OFF, so the LIVE-only
  // `/topology` query (finding 5) stays pending and the views render from the store slices the
  // tests drive directly (applySnapshot / applySeekCut), matching the useFlowRows test pattern.
  resetWorld();
  window.location.hash = '#/topology';
  cleanup();
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('TopologyView — click node → shared filter + navigate to flows', () => {
  it('clicking a provider node sets the upstream filter and navigates to #/flows', () => {
    seedLiveTopology();
    const { container } = renderWithQuery(<TopologyView />);
    fireEvent.click(container.querySelector('[data-node-id="vllm-a"]')!);
    expect(flowFilterStore.getState().filters.upstream).toBe('vllm-a');
    expect(window.location.hash).toBe('#/flows');
  });

  it('renders the historical affordance while seeking and the frozen topology nodes', () => {
    seedLiveTopology();
    act(() => {
      dashboardStore.getState().applySeekCut({
        rows: [], cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 1, monitor_seq: 0 },
        atMs: Date.now(), monitorSeq: 0, metrics: null, topology: TOPOLOGY,
      });
    });
    const { getByTestId, container } = renderWithQuery(<TopologyView />);
    expect(getByTestId('topology-historical')).not.toBeNull();
    // The frozen cut's nodes are rendered (topology comes from the store's frozen slices).
    expect(container.querySelectorAll('[data-testid="topo-node"]').length).toBe(2);
  });
});

describe('SankeyView — click band → shared filter + navigate; $/min; seek', () => {
  it('renders a band from live usage deltas; clicking it filters BOTH facets + navigates (findings 3,9)', () => {
    act(() => {
      dashboardStore.getState().applySnapshot({
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        // The accumulator diffs each flow's usage GROWTH into a windowed delta; a freshly-seen flow's
        // first cumulative IS its delta (finding 2). `upstream_target` keys the (upstream, model) lane.
        flows: [flow({ model_served: 'gpt-4o', upstream_target: 'vllm-a', usage: usage({ prompt: 1000, completion: 500, total: 1500 }) })],
        // $/min is the authoritative MetricTick.cost_per_min (finding 3), not a local lane projection.
        metrics: metrics(2.5),
        topology: TOPOLOGY,
      });
      dashboardStore.getState().setConnection('live');
    });
    const { container, getByTestId } = renderWithQuery(<SankeyView />);
    expect(getByTestId('sankey-cost-per-min').textContent).toBe('$2.50/min');
    fireEvent.click(container.querySelector('[data-testid="sankey-band"][data-model="gpt-4o"]')!);
    expect(flowFilterStore.getState().filters.model).toBe('gpt-4o');
    expect(flowFilterStore.getState().filters.upstream).toBe('vllm-a');
    expect(window.location.hash).toBe('#/flows');
  });

  // Gap 07 (review round 2): the `$`/min readout honors `cost_confidence` + the priced denominator
  // — `—/min` for an absent/unpriced window (never `$0.00/min`), `est`-labelled when estimated.
  it('$/min: a confident aggregate renders plain, with NO est marker', () => {
    act(() => {
      dashboardStore.getState().applySnapshot({
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        flows: [flow({ model_served: 'gpt-4o', upstream_target: 'vllm-a', usage: usage({ prompt: 100, completion: 50, total: 150 }) })],
        metrics: metrics(2.5, { cost_confidence: 'confident' }), topology: TOPOLOGY,
      });
      dashboardStore.getState().setConnection('live');
    });
    const { getByTestId, queryByTestId } = renderWithQuery(<SankeyView />);
    expect(getByTestId('sankey-cost-per-min').textContent).toBe('$2.50/min');
    expect(getByTestId('sankey-cost-per-min').getAttribute('data-confidence')).toBe('confident');
    expect(queryByTestId('sankey-cost-est')).toBeNull();
  });

  it('$/min: an estimated aggregate is LABELLED with an est marker', () => {
    act(() => {
      dashboardStore.getState().applySnapshot({
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        flows: [flow({ model_served: 'gpt-4o', upstream_target: 'vllm-a', usage: usage({ prompt: 100, completion: 50, total: 150 }) })],
        metrics: metrics(0.21, { cost_confidence: 'estimated' }), topology: TOPOLOGY,
      });
      dashboardStore.getState().setConnection('live');
    });
    const { getByTestId } = renderWithQuery(<SankeyView />);
    expect(getByTestId('sankey-cost-per-min').textContent).toBe('$0.21/min');
    expect(getByTestId('sankey-cost-est')).toBeTruthy();
  });

  it('$/min: an unpriced window (priced_samples 0) renders —/min, NEVER $0.00/min', () => {
    act(() => {
      dashboardStore.getState().applySnapshot({
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        flows: [flow({ model_served: 'gpt-4o', upstream_target: 'vllm-a', usage: usage({ prompt: 100, completion: 50, total: 150 }) })],
        metrics: metrics(0, { priced_samples: 0, cost_confidence: 'unavailable' }), topology: TOPOLOGY,
      });
      dashboardStore.getState().setConnection('live');
    });
    const { getByTestId, queryByTestId } = renderWithQuery(<SankeyView />);
    expect(getByTestId('sankey-cost-per-min').textContent).toBe('—/min');
    expect(queryByTestId('sankey-cost-est')).toBeNull();
  });

  it('$/min: an absent metrics window renders —/min (no tick yet), never $0.00/min', () => {
    act(() => {
      dashboardStore.getState().applySnapshot({
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        flows: [flow({ model_served: 'gpt-4o', upstream_target: 'vllm-a', usage: usage({ prompt: 100, completion: 50, total: 150 }) })],
        metrics: null, topology: TOPOLOGY,
      });
      dashboardStore.getState().setConnection('live');
    });
    const { getByTestId } = renderWithQuery(<SankeyView />);
    expect(getByTestId('sankey-cost-per-min').textContent).toBe('—/min');
  });

  it('renders a band end-to-end from a live flow (the rolling-delta accumulator path — finding 2)', () => {
    // The accumulator's lifetime-total-not-recounted behavior is proven deterministically in
    // useSankeyWindow.test.tsx; here we assert the live view wires the accumulator → a band.
    act(() => {
      dashboardStore.getState().applySnapshot({
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        flows: [flow({ api_call_id: 'api_long', model_served: 'gpt-4o', upstream_target: 'vllm-a', usage: usage({ prompt: 1_000_000, completion: 0, total: 1_000_000 }) })],
        metrics: metrics(0), topology: TOPOLOGY,
      });
      dashboardStore.getState().setConnection('live');
    });
    const { container } = renderWithQuery(<SankeyView />);
    expect(container.querySelector('[data-testid="sankey-band"][data-model="gpt-4o"]')).not.toBeNull();
  });

  it('SEEK: builds the Sankey from the FROZEN cut rows + at_ms (no live data)', () => {
    // A live flow exists, but the seek cut replaces the rows with a different frozen set.
    act(() => {
      dashboardStore.getState().applySnapshot({
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        flows: [flow({ model_served: 'live-only', upstream_target: 'vllm-a', usage: usage({ prompt: 1, completion: 1, total: 2 }) })],
        metrics: metrics(1), topology: TOPOLOGY,
      });
      dashboardStore.getState().setConnection('live');
    });
    const at = Date.now();
    act(() => {
      dashboardStore.getState().applySeekCut({
        rows: [flow({ model_served: 'frozen-model', upstream_target: 'vllm-b', started_ms: at - 2000, finished_ms: at - 500, usage: usage({ prompt: 1000, completion: 1000, total: 2000 }) })],
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        atMs: at, monitorSeq: 0, metrics: metrics(7), topology: TOPOLOGY,
      });
    });
    const { getByTestId, container } = renderWithQuery(<SankeyView />);
    expect(getByTestId('sankey-historical')).not.toBeNull();
    // $/min reads the FROZEN metrics cut (finding 3 + seek coherence).
    expect(getByTestId('sankey-cost-per-min').textContent).toBe('$7.00/min');
    // Only the FROZEN model lane is present; the live-only flow does not bleed in.
    expect(container.querySelector('[data-testid="sankey-band"][data-model="frozen-model"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="sankey-band"][data-model="live-only"]')).toBeNull();
  });

  it('SEEK: $/min reads the FROZEN cut and renders —/min when the frozen window is unpriced (gap 07)', () => {
    seedLiveTopology();
    const at = Date.now();
    act(() => {
      dashboardStore.getState().applySeekCut({
        rows: [flow({ model_served: 'frozen-model', upstream_target: 'vllm-a', started_ms: at - 2000, finished_ms: at - 500, usage: usage({ prompt: 1000, completion: 1000, total: 2000 }) })],
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        atMs: at, monitorSeq: 0, metrics: metrics(0, { priced_samples: 0, cost_confidence: 'unavailable' }), topology: TOPOLOGY,
      });
    });
    const { getByTestId } = renderWithQuery(<SankeyView />);
    expect(getByTestId('sankey-historical')).not.toBeNull();
    expect(getByTestId('sankey-cost-per-min').textContent).toBe('—/min');
  });

  it('SEEK: a flow that finished BEFORE the 30s window of the cut is EXCLUDED (finding 4)', () => {
    seedLiveTopology();
    const at = Date.now();
    act(() => {
      dashboardStore.getState().applySeekCut({
        rows: [
          // Finished WITHIN the 30s window of the cut → its lane shows.
          flow({ model_served: 'recent-model', upstream_target: 'vllm-a', started_ms: at - 5000, finished_ms: at - 2000, usage: usage({ prompt: 100, completion: 100, total: 200 }) }),
          // Finished an HOUR before the cut → its lifetime total must NOT paint a last-30s band.
          flow({ model_served: 'ancient-model', upstream_target: 'vllm-b', started_ms: at - 3_600_000, finished_ms: at - 3_500_000, usage: usage({ prompt: 9000, completion: 9000, total: 18000 }) }),
        ],
        cursors: { flow_seq: 1, metrics_seq: 1, topology_seq: 1, monitor_seq: 0 },
        atMs: at, monitorSeq: 0, metrics: metrics(3), topology: TOPOLOGY,
      });
    });
    const { container } = renderWithQuery(<SankeyView />);
    // The recently-finished flow's band is present; the hour-old flow is filtered out by the window.
    expect(container.querySelector('[data-testid="sankey-band"][data-model="recent-model"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="sankey-band"][data-model="ancient-model"]')).toBeNull();
  });
});
