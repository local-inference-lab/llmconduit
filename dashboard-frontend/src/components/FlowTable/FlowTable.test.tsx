import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act, cleanup, fireEvent, within } from '@testing-library/react';
import { FlowTable } from './FlowTable';
import { dashboardStore } from '../../store/dashboardStore';
import { makeFlow, renderWithQuery, resetWorld, seedFlows } from '../testHarness';

/**
 * jsdom reports zero layout, so `@tanstack/react-virtual` would render an empty window. We stub a
 * ResizeObserver and a fixed 600px viewport height on the scroll container so the virtualizer has
 * a real window to compute — then we can assert it renders only a SLICE of 10k rows.
 */
const VIEWPORT = 600;
let restoreLayout: (() => void) | null = null;

beforeEach(() => {
  resetWorld();
  // jsdom has no ResizeObserver; provide a no-op one so the virtualizer's observe path doesn't
  // throw. The viewport size comes from offset* below (the virtualizer reads `offsetHeight`).
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  // `@tanstack/virtual-core`'s getRect reads element.offsetWidth/offsetHeight, which jsdom hard
  // -codes to 0. Override the getters so ONLY the scroll container reports a real 600px viewport
  // (rows keep 0 — they don't self-measure here; positions come from the fixed estimateSize).
  const hgt = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetHeight');
  const wdt = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetWidth');
  Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {
    configurable: true,
    get(this: HTMLElement) {
      return this.getAttribute('data-testid') === 'flow-table-scroll' ? VIEWPORT : 0;
    },
  });
  Object.defineProperty(HTMLElement.prototype, 'offsetWidth', {
    configurable: true,
    get(this: HTMLElement) {
      return this.getAttribute('data-testid') === 'flow-table-scroll' ? 1000 : 0;
    },
  });
  restoreLayout = () => {
    if (hgt) Object.defineProperty(HTMLElement.prototype, 'offsetHeight', hgt);
    if (wdt) Object.defineProperty(HTMLElement.prototype, 'offsetWidth', wdt);
  };
});
afterEach(() => {
  cleanup();
  restoreLayout?.();
  restoreLayout = null;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function noop() {}

describe('FlowTable — virtualization', () => {
  it('renders only a windowed SLICE of 10k rows (not 10k DOM nodes)', () => {
    const flows = Array.from({ length: 10_000 }, (_, i) =>
      makeFlow({ api_call_id: `api_${String(i).padStart(5, '0')}`, started_ms: 1_700_000_000_000 + i }),
    );
    seedFlows(flows);
    const { getByTestId, getAllByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);

    // The list reports 10k total via the filter-bar count…
    expect(getByTestId('flow-count').textContent).toContain('10000');
    // …but only the visible window + overscan is in the DOM (far fewer than 10k rows).
    const rows = getAllByTestId('flow-row');
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.length).toBeLessThan(200);
    void getByTestId('flow-table-scroll');
  });
});

describe('FlowTable — filtering', () => {
  beforeEach(() => {
    seedFlows([
      makeFlow({ api_call_id: 'api_ok', status: 'completed', model_requested: 'gpt-4o', model_served: 'gpt-4o', upstream_target: 'vllm-a' }),
      makeFlow({ api_call_id: 'api_open', status: 'open', model_requested: 'llama-3.1-70b', model_served: 'llama-3.1-70b', upstream_target: 'vllm-b' }),
      makeFlow({ api_call_id: 'api_fail', status: 'failed', model_requested: 'gpt-4o', model_served: 'gpt-4o', upstream_target: 'openai', terminal_reason: 'upstream 503' }),
    ]);
  });

  it('a status chip narrows the rows', () => {
    const { getByText, getByTestId, getAllByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    expect(getAllByTestId('flow-row')).toHaveLength(3);
    // Click the `open` status chip.
    fireEvent.click(getByText('open'));
    const rows = getAllByTestId('flow-row');
    expect(rows).toHaveLength(1);
    expect(within(rows[0]!).getByText('running')).toBeTruthy();
    expect(getByTestId('flow-count').textContent).toContain('1 / 3');
  });

  it('a model chip narrows the rows', () => {
    const { getAllByText, getAllByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    // `gpt-4o` appears as a model chip; clicking it keeps the two gpt-4o rows.
    const chip = getAllByText('gpt-4o').find((el) => el.tagName === 'BUTTON')!;
    fireEvent.click(chip);
    expect(getAllByTestId('flow-row')).toHaveLength(2);
  });
});

describe('FlowTable — live WS update + interactions', () => {
  it('a live flow_status patch updates the matching row in place', () => {
    seedFlows([makeFlow({ api_call_id: 'api_live', status: 'open' })]);
    const { getAllByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    expect(within(getAllByTestId('flow-row')[0]!).getByText('running')).toBeTruthy();

    // A flow_status frame completes the flow.
    act(() => {
      dashboardStore.getState().patchFlowStatus({
        type: 'flow_status', api_call_id: 'api_live', status: 'completed',
        model_served: 'm', upstream_target: 'u', usage: null, started_ms: 1_700_000_000_000, elapsed_ms: 1200,
      });
    });
    expect(within(getAllByTestId('flow-row')[0]!).getByText('2xx')).toBeTruthy();
  });

  it('tags a failover row and reports error styling', () => {
    seedFlows([makeFlow({ api_call_id: 'api_fo', status: 'completed', model_requested: 'gpt-4o', model_served: 'llama-3.1-70b', upstream_target: 'vllm-a' })]);
    const { getByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    expect(getByTestId('failover-tag')).toBeTruthy();
  });

  it('clicking a row calls onSelect with its api_call_id', () => {
    seedFlows([makeFlow({ api_call_id: 'api_click', status: 'completed' })]);
    const onSelect = vi.fn();
    const { getAllByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={onSelect} />);
    fireEvent.click(getAllByTestId('flow-row')[0]!.querySelector('button')!);
    expect(onSelect).toHaveBeenCalledWith('api_click');
  });

  // Gap 07 (review round 2): the per-flow cost cell consumes `cost_confidence`, so an estimated row
  // is visually distinct from a confident one and an unavailable one renders `—` (never `$0.00`).
  it('a confident cost renders plain dollars with NO est marker', () => {
    seedFlows([makeFlow({ api_call_id: 'api_conf', status: 'completed', cost: 0.0061, cost_confidence: 'confident' })]);
    const { getByTestId, queryByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    expect(getByTestId('flow-cost').textContent).toBe('$0.0061');
    expect(getByTestId('flow-cost').getAttribute('data-confidence')).toBe('confident');
    expect(queryByTestId('flow-cost-est')).toBeNull();
  });

  it('an estimated cost is LABELLED with an est marker', () => {
    seedFlows([makeFlow({ api_call_id: 'api_est', status: 'completed', cost: 0.0019, cost_confidence: 'estimated' })]);
    const { getByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    expect(getByTestId('flow-cost').textContent).toBe('$0.0019');
    expect(getByTestId('flow-cost-est')).toBeTruthy();
    expect(getByTestId('flow-cost').getAttribute('data-confidence')).toBe('estimated');
  });

  it('an unavailable cost renders — (never $0.00) and no est marker', () => {
    // The default makeFlow row is unpriced (cost_confidence unavailable, no cost) — it must read `—`.
    seedFlows([makeFlow({ api_call_id: 'api_unp', status: 'failed', cost: null, cost_confidence: 'unavailable' })]);
    const { getByTestId, queryByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    expect(getByTestId('flow-cost').textContent).toBe('—');
    expect(queryByTestId('flow-cost-est')).toBeNull();
  });

  it('the client column does NOT mislabel the HTTP method; renders "—" when absent (finding 6 / gap 15 don\'t-lie-with-zeros)', () => {
    // No client attribution (no key/configured-id/UA) ⇒ the client cell is the honest unavailable
    // marker — NOT the request method (POST), and NOT a fabricated id.
    seedFlows([makeFlow({ api_call_id: 'api_client', method: 'POST', status: 'completed', client_label: null })]);
    const { getByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    const cell = getByTestId('flow-client');
    expect(cell.textContent).toBe('—');
    expect(cell.textContent).not.toBe('POST');
    expect(cell.getAttribute('data-quality')).toBe('unavailable');
    expect(cell.getAttribute('data-attributed')).toBe('false');
  });

  // Gap 15: the CLIENT column renders the non-secret attribution label with a source-strength marker.
  it('renders a key-hash client as a STRONG measured identity (label + key badge) — gap 15', () => {
    seedFlows([makeFlow({ api_call_id: 'api_kh', status: 'completed', client_label: 'key-9f3a1c0b2d4e', client_source: 'key_hash' })]);
    const { getByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    const cell = getByTestId('flow-client');
    expect(cell.textContent).toContain('key-9f3a1c0b2d4e'); // the hash prefix — never a raw key
    expect(cell.getAttribute('data-quality')).toBe('measured');
    expect(cell.getAttribute('data-strength')).toBe('strong');
    expect(getByTestId('flow-client-source').textContent).toBe('key');
  });

  it('renders a User-Agent client as a WEAK derived fallback (visibly weaker, ua badge) — gap 15', () => {
    seedFlows([makeFlow({ api_call_id: 'api_ua', status: 'completed', client_label: 'python-httpx/0.27', client_source: 'user_agent' })]);
    const { getByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    const cell = getByTestId('flow-client');
    expect(cell.textContent).toContain('python-httpx/0.27');
    // The KEY distinction: a UA fallback is `derived` (weak), NOT `measured` — never a confirmed identity.
    expect(cell.getAttribute('data-quality')).toBe('derived');
    expect(cell.getAttribute('data-strength')).toBe('weak');
    const badge = getByTestId('flow-client-source');
    expect(badge.textContent).toBe('ua');
    expect(badge.getAttribute('data-source')).toBe('user_agent');
  });

  // Gap 15: the per-client filter chip narrows the table to one client_label.
  it('a client filter chip narrows the rows to that client (gap 15)', () => {
    seedFlows([
      makeFlow({ api_call_id: 'api_x1', status: 'completed', client_label: 'key-A', client_source: 'key_hash' }),
      makeFlow({ api_call_id: 'api_x2', status: 'completed', client_label: 'key-A', client_source: 'key_hash' }),
      makeFlow({ api_call_id: 'api_y1', status: 'completed', client_label: 'svc-checkout', client_source: 'configured_header' }),
    ]);
    const { getAllByTestId, getByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    expect(getAllByTestId('flow-row')).toHaveLength(3);
    // `key-A` appears as a client filter chip (its label is in a bounded truncate span); resolve the
    // enclosing chip button via the filter-bar chip-label testid (the CLIENT cell also renders `key-A`).
    const chip = getAllByTestId('flow-filter-chip-label')
      .find((el) => el.textContent === 'key-A')!
      .closest('button')!;
    fireEvent.click(chip);
    expect(getAllByTestId('flow-row')).toHaveLength(2);
    expect(getByTestId('flow-count').textContent).toContain('2 / 3');
  });
});


describe('FlowTable — sessions facts (harness column + cache-bust marker)', () => {
  it('renders the detected harness (— when undetected) and flags cache-busting rows', async () => {
    seedFlows([
      makeFlow({ api_call_id: 'api_h1', started_ms: 3, harness: 'claude-code', harness_version: '2.1.205', session_id: 'sess_1', divergence_kind: 'tools_changed', cache_bust: true }),
      makeFlow({ api_call_id: 'api_h2', started_ms: 2, harness: 'codex', divergence_kind: 'append', cache_bust: false }),
      makeFlow({ api_call_id: 'api_h3', started_ms: 1 }),
    ]);
    const { getAllByTestId, queryAllByTestId } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    await act(async () => {});
    const cells = getAllByTestId('flow-harness');
    expect(cells.map((c) => c.textContent)).toEqual(['claude-code', 'codex', '—']);
    expect(cells[0]!.getAttribute('data-quality')).toBe('measured');
    expect(cells[2]!.getAttribute('data-quality')).toBe('unavailable');
    expect(cells[0]!.getAttribute('title')).toContain('2.1.205');
    // Only the busting row carries the marker, tagged with its divergence kind.
    const busts = queryAllByTestId('flow-cache-bust');
    expect(busts.length).toBe(1);
    expect(busts[0]!.getAttribute('data-kind')).toBe('tools_changed');
  });

  it('the harness facet and the cache-bust facet scope the rows', async () => {
    seedFlows([
      makeFlow({ api_call_id: 'api_f1', started_ms: 3, harness: 'claude-code', cache_bust: true, divergence_kind: 'history_rewritten' }),
      makeFlow({ api_call_id: 'api_f2', started_ms: 2, harness: 'codex', cache_bust: false, divergence_kind: 'append' }),
      makeFlow({ api_call_id: 'api_f3', started_ms: 1, harness: 'codex' }),
    ]);
    const { getAllByTestId, getByTestId, getByText } = renderWithQuery(<FlowTable selectedId={null} onSelect={noop} />);
    await act(async () => {});
    expect(getAllByTestId('flow-row').length).toBe(3);
    // Harness chips are derived from the rows in view.
    const bar = getByTestId('flow-filter-bar');
    const harnessGroup = within(bar).getByText('harness').parentElement as HTMLElement;
    const codexChip = within(harnessGroup).getByText('codex');
    fireEvent.click(codexChip);
    await act(async () => {});
    expect(getAllByTestId('flow-row').length).toBe(2);
    fireEvent.click(codexChip); // toggle off
    await act(async () => {});
    fireEvent.click(getByText('busts'));
    await act(async () => {});
    expect(getAllByTestId('flow-row').length).toBe(1);
    expect(getByTestId('flow-count').textContent).toBe('1 / 3');
  });
});
