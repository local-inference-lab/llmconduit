import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { cleanup, waitFor, act } from '@testing-library/react';
import { OverviewView } from './OverviewView';
import { dashboardStore } from '../../store/dashboardStore';
import { renderWithQuery, resetWorld, makeFlow } from '../../components/testHarness';
import type { FlowSummary, MetricsResponse, ProviderLatency, ProviderHealth, TopologyResponse } from '../../api/types';

/**
 * OverviewView (gap 16) component: the CONTROL-ROOM overview that COMPOSES the gap-01–15 surfaces.
 * Asserts the wire-source correctness (per-provider from the snapshot topology node, NOT a derived
 * flow rollup) and the cross-cutting DQ rules (don't-lie-with-zeros: empty ⇒ —, cost weakest-tag,
 * unreported token classes ⇒ —).
 *
 * resetWorld installs the authenticated (non-mock) bootstrap, so the REST polls in useFlowRows /
 * useTopologyQuery / useCatalog / useMetricStream stay pending and the view renders from the store
 * slices the test drives directly (applySnapshot) — the same pattern the Topology/Failure tests use.
 */

function metrics(over: Partial<MetricsResponse> = {}): MetricsResponse {
  const w = { reqs_per_sec: 4.2, active_streams: 3, error_pct: 1.1, p50: 180, p95: 920, p99: 1840, tokens_per_sec: 142, prefill_tokens_per_sec: 10_000, decode_tokens_per_sec: 78.34, cost_per_min: 0.21, samples: 10, usage_samples: 10, prefill_samples: 10, decode_samples: 10, priced_samples: 10, cost_confidence: 'estimated' as const };
  return { metrics_seq: 1, ...w, ...over, windows: { m1: { ...w, ...over }, m5: { ...w }, h1: { ...w } } };
}

function per(over: Partial<ProviderLatency> = {}): ProviderLatency {
  return { provider: 'vllm-a', data_quality: 'derived', samples: 50, served: 48, failed: 2, p50: 82, p95: 190, p99: 240, error_rate: 4, errors: { http_status: 2 }, ...over };
}

function node(over: Partial<ProviderHealth>): ProviderHealth {
  return { id: 'vllm-a', name: 'vllm-a', route: null, base_url: 'http://x', status: 'healthy', cooling_until_ms: null, last_error: null, served_count: 0, failover_count: 0, consecutive_failures: 0, catalog_fetched_ms: null, catalog_size: 0, ...over };
}

function topology(nodes: ProviderHealth[]): TopologyResponse {
  return { topology_seq: 1, nodes, edges: [], price_table: {} };
}

/** Seed the store via the snapshot path (flows + optional metrics + topology), connection live. */
function seed(flows: FlowSummary[], opts: { metrics?: MetricsResponse | null; topology?: TopologyResponse | null } = {}): void {
  act(() => {
    dashboardStore.getState().applySnapshot({
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows,
      metrics: opts.metrics ?? null,
      topology: opts.topology ?? null,
    });
    dashboardStore.getState().setConnection('live');
  });
}

beforeEach(() => resetWorld());
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('OverviewView — empty state (don\'t-lie-with-zeros: — not an all-0 dashboard)', () => {
  it('with no data every composed tile reads its explicit unavailable — state', async () => {
    const { getByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-view')).toBeTruthy());

    // The leaderboards, failures, clients, context, token-mix, providers, headline — all unavailable.
    expect(getByTestId('overview-top-models-volume').getAttribute('data-available')).toBe('false');
    expect(getByTestId('overview-top-models-cost').getAttribute('data-available')).toBe('false');
    expect(getByTestId('overview-top-providers-volume').getAttribute('data-available')).toBe('false');
    expect(getByTestId('overview-top-providers-cost').getAttribute('data-available')).toBe('false'); // review HIGH 1
    expect(getByTestId('overview-failures').getAttribute('data-available')).toBe('false');
    expect(getByTestId('overview-clients').getAttribute('data-available')).toBe('false');
    expect(getByTestId('overview-context').getAttribute('data-available')).toBe('false');
    expect(getByTestId('overview-token-mix').getAttribute('data-available')).toBe('false');
    expect(getByTestId('overview-providers').getAttribute('data-available')).toBe('false');
    // The failure rate reads — (unavailable), NOT 0%.
    expect(getByTestId('overview-failures-rate').getAttribute('data-quality')).toBe('unavailable');
    // Review HIGH 4: with no measurable flow the context near/over reads — / — (unavailable), NOT 0 / 0.
    expect(getByTestId('overview-context-nearover').getAttribute('data-quality')).toBe('unavailable');
    expect(getByTestId('overview-context-nearover').textContent).toContain('—');
    // Headline with no metrics tick: active streams reads — (unavailable), not 0.
    expect(getByTestId('overview-hl-active').getAttribute('data-quality')).toBe('unavailable');
    expect(getByTestId('overview-hl-active').textContent).toContain('—');
    // Review HIGH 2: no priced tick ⇒ the cost trend is tagged unavailable (a gap, not a flat 0 line).
    expect(getByTestId('overview-cost-trend').getAttribute('data-quality')).toBe('unavailable');
  });
});

describe('OverviewView — top models by volume + cost (weakest-tag inheritance)', () => {
  it('counts volume (measured) and inherits the WEAKEST cost confidence (mixed ⇒ estimated, labelled)', async () => {
    seed([
      makeFlow({ api_call_id: 'a', model_served: 'llama', cost: 0.01, cost_confidence: 'confident' }),
      makeFlow({ api_call_id: 'b', model_served: 'llama', cost: 0.02, cost_confidence: 'estimated' }),
      makeFlow({ api_call_id: 'c', model_served: 'gpt-4o', cost: null, cost_confidence: 'unavailable' }),
    ]);
    const { getByTestId, getAllByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-top-models-volume').getAttribute('data-available')).toBe('true'));

    const rows = getAllByTestId('overview-leaderboard-row');
    // llama (2 flows) ranks first by volume.
    const llama = rows.find((r) => r.getAttribute('data-key') === 'llama')!;
    expect(llama.querySelector('[data-testid="overview-leaderboard-volume"]')!.textContent).toBe('2');
    // Its summed cost is estimated (the weaker of confident+estimated) ⇒ data-quality estimated + an est badge.
    const cost = llama.querySelector('[data-testid="overview-leaderboard-cost"]')!;
    expect(cost.getAttribute('data-quality')).toBe('estimated');
    expect(llama.querySelector('[data-testid="overview-leaderboard-est"]')).toBeTruthy();
    // gpt-4o has NO priced flow ⇒ its cost reads — (unavailable), NEVER $0.00.
    const gpt = rows.find((r) => r.getAttribute('data-key') === 'gpt-4o')!;
    const gptCost = gpt.querySelector('[data-testid="overview-leaderboard-cost"]')!;
    expect(gptCost.getAttribute('data-quality')).toBe('unavailable');
    expect(gptCost.textContent).toBe('—');
  });

  it('renders top providers by COST too (review HIGH 1) — same weakest-tag/unpriced rules', async () => {
    seed([
      makeFlow({ api_call_id: 'a', upstream_target: 'vllm-a', cost: 0.01, cost_confidence: 'confident' }),
      makeFlow({ api_call_id: 'b', upstream_target: 'vllm-a', cost: 0.02, cost_confidence: 'estimated' }),
      makeFlow({ api_call_id: 'c', upstream_target: 'openai', cost: null, cost_confidence: 'unavailable' }),
    ]);
    const { getByTestId, getAllByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-top-providers-cost').getAttribute('data-available')).toBe('true'));
    const rows = getAllByTestId('overview-leaderboard-row');
    // vllm-a is priced (mixed confident+estimated ⇒ estimated, labelled); openai unpriced ⇒ excluded from the cost board.
    const vllmA = rows.find((r) => r.getAttribute('data-key') === 'vllm-a' && r.closest('[data-testid="overview-top-providers-cost"]'));
    expect(vllmA).toBeTruthy();
    expect(vllmA!.querySelector('[data-testid="overview-leaderboard-cost"]')!.getAttribute('data-quality')).toBe('estimated');
    // openai (unpriced) is NOT in the provider-cost board.
    expect(rows.some((r) => r.getAttribute('data-key') === 'openai' && r.closest('[data-testid="overview-top-providers-cost"]'))).toBe(false);
  });

  it('the cost board is the empty-state — when NO flow is priced (not a $0.00 board)', async () => {
    seed([makeFlow({ api_call_id: 'a', model_served: 'm', cost: null, cost_confidence: 'unavailable' })]);
    const { getByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-view')).toBeTruthy());
    expect(getByTestId('overview-top-models-cost').getAttribute('data-available')).toBe('false');
    expect(getByTestId('overview-top-models-cost-unavailable').getAttribute('data-quality')).toBe('unavailable');
  });
});

describe('OverviewView — per-provider tiles read the SNAPSHOT topology DTO (NOT a flow rollup)', () => {
  it('renders a provider tile from the snapshot node per_provider (the gap-12/13 wire source)', async () => {
    // The snapshot node carries per_provider (mirrors the REST/snapshot path). A degrading provider.
    seed(
      [makeFlow({ api_call_id: 'a', upstream_target: 'vllm-b', model_served: 'llama' })],
      {
        topology: topology([
          node({ id: 'vllm-b', name: 'vllm-b', status: 'cooling', per_provider: per({ provider: 'vllm-b', error_rate: 16, p50: 240, p95: 1100, p99: 2400, errors: { connect: 7, timeout: 5 } }) }),
        ]),
      },
    );
    const { getByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-providers').getAttribute('data-available')).toBe('true'));
    const tile = getByTestId('overview-provider');
    expect(tile.getAttribute('data-provider')).toBe('vllm-b');
    // The provider latency tile is present + available (real derived percentiles, NOT a fabricated 0).
    expect(tile.querySelector('[data-testid="provider-latency-tile"]')!.getAttribute('data-available')).toBe('true');
    expect(tile.querySelector('[data-testid="provider-p50"]')!.getAttribute('data-quality')).toBe('derived');
    expect(tile.querySelector('[data-testid="provider-p50"]')!.textContent).not.toBe('—');
    // The error rate is measured (a directly-counted ratio).
    expect(tile.querySelector('[data-testid="provider-error-rate"]')!.getAttribute('data-quality')).toBe('measured');
  });

  it('a topology with NO per_provider (the WS-frame-shape node) ⇒ the providers tile is unavailable', async () => {
    // A node WITHOUT per_provider (as the live WS frame strips it) ⇒ no provider tile is derived.
    seed([makeFlow({ api_call_id: 'a', upstream_target: 'vllm-a' })], { topology: topology([node({ id: 'vllm-a' })]) });
    const { getByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-view')).toBeTruthy());
    expect(getByTestId('overview-providers').getAttribute('data-available')).toBe('false');
    expect(getByTestId('overview-providers-unavailable').getAttribute('data-quality')).toBe('unavailable');
  });
});

describe('OverviewView — token mix (measured classes; unreported optional ⇒ —)', () => {
  it('renders the prompt/completion measured split and — for an unreported cached class', async () => {
    seed([
      makeFlow({ api_call_id: 'a', usage: { prompt: 1000, completion: 500, total: 1500 } }), // no cached/reasoning
    ]);
    const { getByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-token-mix').getAttribute('data-available')).toBe('true'));
    // prompt is measured.
    expect(getByTestId('overview-token-prompt').getAttribute('data-quality')).toBe('measured');
    expect(getByTestId('overview-token-prompt').textContent).toContain('1.0k');
    // cached is UNREPORTED ⇒ unavailable (—), NEVER a fabricated 0.
    expect(getByTestId('overview-token-cached').getAttribute('data-quality')).toBe('unavailable');
    expect(getByTestId('overview-token-cached').textContent).toContain('—');
  });
});

describe('OverviewView — top-clients cost is labelled by its FOLDED confidence (review R2 MEDIUM)', () => {
  it('an estimated-confidence client cost is tagged + titled estimated — NEVER "measured"', async () => {
    seed([
      // One client, a priced flow with an ESTIMATED cost (unconfigured cache rate) ⇒ the roll-up
      // folds `estimated` ⇒ the cost cell must be data-quality estimated + the tooltip must say so.
      makeFlow({ api_call_id: 'a', client_label: 'key-A', client_source: 'key_hash', status: 'completed', cost: 0.01, cost_confidence: 'estimated' }),
    ]);
    const { getByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-clients').getAttribute('data-available')).toBe('true'));
    const cost = getByTestId('overview-client-cost');
    expect(cost.getAttribute('data-quality')).toBe('estimated');
    const title = cost.getAttribute('title') ?? '';
    expect(title).toContain('estimated');
    expect(title).not.toContain('measured'); // the round-1-fix contradiction is gone
  });

  it('a confident-only client cost is titled derived (not measured), unavailable when unpriced', async () => {
    seed([
      makeFlow({ api_call_id: 'a', client_label: 'key-C', client_source: 'key_hash', status: 'completed', cost: 0.02, cost_confidence: 'confident' }),
      makeFlow({ api_call_id: 'b', client_label: 'svc-x', client_source: 'configured_header', status: 'failed', cost: null, cost_confidence: 'unavailable' }),
    ]);
    const { getAllByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getAllByTestId('overview-client-row').length).toBeGreaterThanOrEqual(2));
    const costs = getAllByTestId('overview-client-cost');
    const confident = costs.find((c) => (c.getAttribute('title') ?? '').includes('derived'))!;
    expect(confident).toBeTruthy();
    expect(confident.getAttribute('data-quality')).toBe('derived');
    const unavailable = costs.find((c) => c.getAttribute('data-quality') === 'unavailable')!;
    expect(unavailable.getAttribute('title')).toContain('unavailable');
  });
});

describe('OverviewView — headline echoes the gap-01 honest tile (samples-gated)', () => {
  it('reads real metrics when measured, and an estimated $/min is tagged estimated', async () => {
    seed([], { metrics: metrics({ priced_samples: 10, cost_confidence: 'estimated' }) });
    const { getByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-headline')).toBeTruthy());
    expect(getByTestId('overview-hl-active').getAttribute('data-quality')).toBe('measured');
    expect(getByTestId('overview-hl-active').textContent).toContain('3');
    expect(getByTestId('overview-hl-cost').getAttribute('data-quality')).toBe('estimated');
  });

  it('a window with samples 0 renders latency/tok/$ as — (unavailable), never 0', async () => {
    seed([], { metrics: metrics({ samples: 0, usage_samples: 0, prefill_samples: 0, decode_samples: 0, priced_samples: 0, cost_confidence: 'unavailable' }) });
    const { getByTestId } = renderWithQuery(<OverviewView />);
    await waitFor(() => expect(getByTestId('overview-headline')).toBeTruthy());
    expect(getByTestId('overview-hl-err').textContent).toContain('—');
    expect(getByTestId('overview-hl-err').getAttribute('data-quality')).toBe('unavailable');
    expect(getByTestId('overview-hl-prefill').textContent).toContain('—');
    expect(getByTestId('overview-hl-decode').textContent).toContain('—');
    expect(getByTestId('overview-hl-cost').textContent).toContain('—');
    // active streams + samples stay numeric (a genuine 0, not unavailable).
    expect(getByTestId('overview-hl-samples').getAttribute('data-quality')).toBe('measured');
  });
});
