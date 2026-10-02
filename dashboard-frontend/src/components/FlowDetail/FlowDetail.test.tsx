import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act, cleanup, fireEvent, waitFor, within } from '@testing-library/react';
import { FlowDetail } from './FlowDetail';
import { dashboardStore } from '../../store/dashboardStore';
import { authStore } from '../../store/authStore';
import { mockKillLog, MOCK_KILL_UNAUTHORIZED_ID } from '../../api/mock';
import { makeFlow, renderWithQuery, resetWorld, seedFlows } from '../testHarness';
import type { DebugWsMessage, FlowDetail as FlowDetailDto } from '../../api/types';

function noop() {}

/** Push a monitor message into the live ring, stamped with its arrival `monitor_seq`. */
function pushMonitor(msg: DebugWsMessage, seq = 0): void {
  act(() => dashboardStore.getState().pushMonitor(msg, seq));
}

describe('FlowDetail — 3-pane inspector (mock backend)', () => {
  beforeEach(() => {
    // Mock mode: mockFetch answers /flows/:id + the kill route.
    resetWorld({ mock: true });
    authStore.getState().setMutationsEnabled(true);
    authStore.getState().setCsrfToken('mock-csrf-token');
    mockKillLog.length = 0;
    // The live store knows api_001 (open, linked to resp_001) so the join + kill work.
    seedFlows([makeFlow({ api_call_id: 'api_001', response_id: 'resp_001', status: 'open', model_requested: 'gpt-4o', model_served: 'llama-3.1-70b', upstream_target: 'vllm-a', started_ms: 1_700_000_000_000 })]);
  });
  afterEach(cleanup);

  it('renders all 3 bodies and tints the diff between layers (A→B→C)', async () => {
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    // The /flows/:id query resolves the three bodies.
    await waitFor(() => expect(getByTestId('jsonpane-code-A · inbound').querySelectorAll('.json-line').length).toBeGreaterThan(0));
    const paneB = getByTestId('jsonpane-code-B · normalized');
    const paneC = getByTestId('jsonpane-code-C · upstream');
    expect(paneB.querySelectorAll('.json-line').length).toBeGreaterThan(0);
    expect(paneC.querySelectorAll('.json-line').length).toBeGreaterThan(0);
    // The mock bodies differ at $.model (gpt-4o → llama) and add $.stream on C — tinted lines exist.
    const tintedB = paneB.querySelectorAll('.json-line[data-diff]');
    const tintedC = paneC.querySelectorAll('.json-line[data-diff]');
    expect(tintedB.length + tintedC.length).toBeGreaterThan(0);
  });

  it('scroll-syncs the panes (scrolling A mirrors to B and C)', async () => {
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    await waitFor(() => expect(getByTestId('jsonpane-scroll-A · inbound')).toBeTruthy());
    const a = getByTestId('jsonpane-scroll-A · inbound');
    const b = getByTestId('jsonpane-scroll-B · normalized');
    const c = getByTestId('jsonpane-scroll-C · upstream');
    a.scrollTop = 40;
    fireEvent.scroll(a);
    expect(b.scrollTop).toBe(40);
    expect(c.scrollTop).toBe(40);
  });

  it('Timeline tab populates from monitor event_append; deltas render output + tool card', async () => {
    const { getByTestId, getByRole } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());

    // Stream a timeline event + segments (output + a tool call) for resp_001. The segments arrive
    // AFTER the mock REST replay's coverage — the replay's max delta `sequence` is 3, so the live
    // continuation carries higher `monitor_seq`s (4, 5) and is appended past the replay watermark
    // (deltas merge by MonitorHub SEQUENCE — finding 2).
    pushMonitor({ type: 'event_append', response_id: 'resp_001', event: { timestamp_ms: 1, kind: 'response.created', summary: 'created', images: [] } }, 4);
    pushMonitor({ type: 'segment_append', response_id: 'resp_001', segment: { timestamp_ms: 1, kind: 'output', text: 'Hello world' } }, 4);
    pushMonitor({ type: 'segment_append', response_id: 'resp_001', segment: { timestamp_ms: 2, kind: 'tool', text: JSON.stringify({ name: 'search' }) } }, 5);

    // Deltas sub-panel shows the output text + an expandable tool card.
    expect(getByTestId('deltas-panel').textContent).toContain('Hello world');
    const card = getByTestId('tool-card');
    expect(card.textContent).toContain('search');
    fireEvent.click(card.querySelector('button')!);
    expect(getByTestId('tool-card-body')).toBeTruthy();

    // Timeline tab populates.
    fireEvent.click(getByRole('tab', { name: 'Timeline' }));
    expect(within(getByTestId('tabpanel-timeline')).getByText('response.created')).toBeTruthy();
  });

  it('kill POSTs with the CSRF header and optimistically flips the row to cancelled', async () => {
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    await waitFor(() => expect(getByTestId('kill-button')).toBeTruthy());
    fireEvent.click(getByTestId('kill-button'));

    // Optimistic: the live row flips to cancelled immediately.
    expect(dashboardStore.getState().flows.get('api_001')?.status).toBe('cancelled');
    // The POST carried the X-CSRF-Token (D7 double-submit).
    await waitFor(() => expect(mockKillLog.at(-1)?.id).toBe('api_001'));
    expect(mockKillLog.at(-1)?.csrf).toBe('mock-csrf-token');
    await waitFor(() => expect(getByTestId('kill-done')).toBeTruthy());
  });

  it('a server 403 (no CSRF) is handled: rolls back + shows mutations-disabled', async () => {
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    await waitFor(() => expect(getByTestId('kill-button')).toBeTruthy());
    // Clear the token AFTER the connection seeded it, so the kill omits X-CSRF-Token → mock 403.
    // (getConnection re-seeds auth from the bootstrap on (re)build, so we override post-render.)
    act(() => authStore.getState().setCsrfToken(null));
    document.cookie = 'llmconduit_csrf=; expires=Thu, 01 Jan 1970 00:00:00 GMT';
    fireEvent.click(getByTestId('kill-button'));
    // Forbidden state surfaces and the optimistic flip is rolled back to open.
    await waitFor(() => expect(getByTestId('kill-forbidden')).toBeTruthy());
    expect(dashboardStore.getState().flows.get('api_001')?.status).toBe('open');
  });

  it('a 401 kill lets teardown win: the store stays CLEARED, the prior row is NOT re-inserted (finding 1)', async () => {
    // Seed an OPEN flow whose kill route answers 401 (session raced expiry). A valid CSRF is
    // present, so the request reaches the server and comes back 401 → centralized teardown clears
    // the live store. The optimistic rollback must NOT re-insert the prior row afterwards.
    seedFlows([makeFlow({ api_call_id: MOCK_KILL_UNAUTHORIZED_ID, status: 'open', started_ms: 1_700_000_000_000 })]);
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId={MOCK_KILL_UNAUTHORIZED_ID} onClose={noop} />);
    await waitFor(() => expect(getByTestId('kill-button')).toBeTruthy());
    expect(dashboardStore.getState().flows.get(MOCK_KILL_UNAUTHORIZED_ID)).toBeTruthy();

    fireEvent.click(getByTestId('kill-button'));

    // The 401 routed through teardown (stores reset); the rollback did not re-leak the row.
    await waitFor(() => expect(dashboardStore.getState().flows.size).toBe(0));
    expect(dashboardStore.getState().flows.get(MOCK_KILL_UNAUTHORIZED_ID)).toBeUndefined();
  });

  it('pane B marks a field present in B but dropped by C as removed (finding 4)', async () => {
    // A crafted 3-layer fixture where `b_only` exists in B (normalized) but is dropped in C
    // (upstream). Pane B must visibly tint `b_only` as REMOVED (it leaves on the way to C), which
    // the A→B-only diff never showed. `api_replay` is unknown to the mock so /flows/:id 404s and
    // does NOT overwrite the injected detail.
    seedFlows([makeFlow({ api_call_id: 'api_replay', response_id: 'resp_replay', status: 'completed', started_ms: 1_700_000_000_000 })]);
    const detail: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_replay', response_id: 'resp_replay', status: 'completed', cost_confidence: 'unavailable',
      deltas: [], started_ms: 1_700_000_000_000,
      // `b_only` is present in A and B alike (unchanged A→B) but dropped by C — so the ONLY signal
      // for it is the B→C removal, which must surface in pane B (it would be invisible under the
      // A→B-only diff that pane B previously used).
      inbound_body: { model: 'gpt-4o', keep: 1, b_only: 'dropped-next' },
      normalized: { model: 'llama-3.1-70b', keep: 1, b_only: 'dropped-next' },
      upstream_body: { model: 'llama-3.1-70b', keep: 1 }, // b_only dropped here
    };
    const { getByTestId, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_replay" onClose={noop} />);
    act(() => queryClient.setQueryData(['flows', 'api_replay'], detail));
    await waitFor(() => expect(getByTestId('jsonpane-code-B · normalized').querySelectorAll('.json-line').length).toBeGreaterThan(0));
    const paneB = getByTestId('jsonpane-code-B · normalized');
    const bOnlyLine = paneB.querySelector('.json-line[data-path="$.b_only"]') as HTMLElement | null;
    // The B-only field that C drops is tinted removed in pane B (combined middle diff).
    expect(bOnlyLine?.dataset.diff).toBe('removed');
  });

  it('replays REST detail.deltas into the deltas panel for a completed flow (finding 5)', async () => {
    // A completed flow loaded via REST has NO live monitor segments — its streamed output lives
    // only in `detail.deltas`. The deltas panel must replay them rather than show "no deltas".
    // `api_replay` is unknown to the mock so the 404 won't replace the injected detail.
    seedFlows([makeFlow({ api_call_id: 'api_replay', response_id: 'resp_replay', status: 'completed', started_ms: 1_700_000_000_000 })]);
    const detail: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_replay', response_id: 'resp_replay', status: 'completed', cost_confidence: 'unavailable',
      started_ms: 1_700_000_000_000,
      inbound_body: { model: 'gpt-4o' }, normalized: { model: 'm' }, upstream_body: { model: 'm' },
      deltas: [
        { sequence: 1, kind: 'response.created', payload: {}, ts_ms: 1 }, // lifecycle → dropped
        { sequence: 2, kind: 'response.output_text.delta', payload: { text: 'Replayed ' }, ts_ms: 2 },
        { sequence: 3, kind: 'response.output_text.delta', payload: { text: 'output' }, ts_ms: 3 },
      ],
    };
    const { getByTestId, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_replay" onClose={noop} />);
    act(() => queryClient.setQueryData(['flows', 'api_replay'], detail));
    // The replayed deltas render (coalesced) even with NO live monitor frames pushed.
    await waitFor(() => expect(getByTestId('deltas-panel').textContent).toContain('Replayed output'));
  });

  it('detail cost roll-up shows even when the live row carries no cost (finding 4)', async () => {
    // api_001's live row (seeded in beforeEach) has NO cost/usage; the mock /flows/:id detail
    // carries the server roll-up cost. The header must surface that roll-up, not "—".
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    // Wait for the detail query (with the roll-up cost) to resolve.
    await waitFor(() => expect(getByTestId('jsonpane-code-A · inbound').querySelectorAll('.json-line').length).toBeGreaterThan(0));
    // The seeded mock detail cost for api_001 is 0.0061 → formatted into the cost/elapsed cell.
    await waitFor(() => expect(getByTestId('flow-detail').textContent).toContain('$0.0061'));
  });

  it('kill button is gated OFF (disabled, no POST) when mutations are disabled', async () => {
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    await waitFor(() => expect(getByTestId('kill-button')).toBeTruthy());
    // Disable mutations AFTER the connection seeded the bootstrap value (which is `true` in mock).
    act(() => authStore.getState().setMutationsEnabled(false));
    const btn = getByTestId('kill-button');
    expect(btn).toBeDisabled();
    expect(btn.getAttribute('title')).toBe('mutations disabled');
    // A disabled button cannot dispatch a click → no kill POST is ever attempted.
    fireEvent.click(btn);
    expect(mockKillLog).toHaveLength(0);
  });

  // Gap 07 — the inspector header renders `—` for UNREPORTED cached/reasoning (never a fake
  // `0`) and LABELS an `estimated` cost as such. `api_g07` is unknown to the mock so /flows/:id
  // 404s and the SEEDED live row drives the header (usage cached/reasoning absent, tag estimated).
  it('renders — for unreported cached/reasoning and labels an estimated cost', async () => {
    seedFlows([
      makeFlow({
        api_call_id: 'api_g07',
        status: 'completed',
        model_served: 'llama-3.1-70b',
        upstream_target: 'vllm-a',
        started_ms: 1_700_000_000_000,
        cost: 0.0019,
        cost_confidence: 'estimated',
        // cached/reasoning UNREPORTED (absent) — must render `—`, never `0`.
        usage: { prompt: 1500, completion: 980, total: 2480 },
      }),
    ]);
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_g07" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());

    // The cached/reasoning subcounts both render the unavailable marker, NOT `0`.
    const sub = getByTestId('usage-subcounts');
    expect(sub.textContent).toContain('—');
    expect(sub.textContent).not.toContain('0');

    // The estimated cost is LABELLED (the cross-cutting rule): an `est` badge is present.
    const badge = getByTestId('cost-confidence');
    expect(badge.getAttribute('data-confidence')).toBe('estimated');
  });

  // Gap 08 — the inspector cache-economics line MIRRORS the table popover: a measured priced flow
  // shows a DERIVED cache-hit rate + "$ saved" (labelled `derived`). `api_g08` is unknown to the
  // mock so /flows/:id 404s and the SEEDED live row drives the header. The price table is seeded
  // with gpt-4o (a CONFIGURED cached price → presence licenses the $ figure).
  it('shows the cache-hit rate and a labelled derived $ saved for a priced cached flow (gap 08)', async () => {
    // seedFlows (applySnapshot, topology: null) resets priceTable, so seed the price table AFTER.
    seedFlows([
      makeFlow({
        api_call_id: 'api_g08',
        status: 'completed',
        model_served: 'gpt-4o',
        started_ms: 1_700_000_000_000,
        cost: 0.01,
        cost_confidence: 'confident',
        // 250 cached of 1000 prompt ⇒ 25.0% hit; saved = (250/1000)*(0.005-0.0025) = 0.000625.
        usage: { prompt: 1000, completion: 200, total: 1200, cached: 250, reasoning: 0 },
      }),
    ]);
    act(() => dashboardStore.getState().seedTopology({
      topology_seq: 1, nodes: [], edges: [],
      price_table: { 'gpt-4o': { input_per_1k: 0.005, output_per_1k: 0.015, cached_per_1k: 0.0025, cached_price_configured: true } },
    }));
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_g08" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());

    const hit = getByTestId('cache-hit');
    expect(hit.getAttribute('data-quality')).toBe('derived');
    expect(hit.textContent).toContain('25.0%');
    const saved = getByTestId('cache-saved');
    expect(saved.getAttribute('data-quality')).toBe('derived');
    expect(saved.textContent).toContain('$0.0006');
    // The derived saving is LABELLED so it is never read as a billed (measured) cost.
    expect(getByTestId('saved-derived')).toBeTruthy();
  });

  // Gap 08 — UNREPORTED cached ⇒ the cache-economics line reads `—` for both hit + $ saved
  // (never a fabricated 0% / $0.00), even when the model HAS a configured cache price.
  it('renders — for cache-hit and $ saved when cached is unreported (gap 08)', async () => {
    seedFlows([
      makeFlow({
        api_call_id: 'api_g08b',
        status: 'completed',
        model_served: 'gpt-4o',
        started_ms: 1_700_000_000_000,
        cost: 0.01,
        cost_confidence: 'confident',
        usage: { prompt: 1000, completion: 200, total: 1200 }, // cached unreported
      }),
    ]);
    act(() => dashboardStore.getState().seedTopology({
      topology_seq: 1, nodes: [], edges: [],
      price_table: { 'gpt-4o': { input_per_1k: 0.005, output_per_1k: 0.015, cached_per_1k: 0.0025, cached_price_configured: true } },
    }));
    const { getByTestId, queryByTestId } = renderWithQuery(<FlowDetail apiCallId="api_g08b" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    const econ = getByTestId('cache-economics');
    expect(econ.textContent).toContain('—');
    expect(getByTestId('cache-hit').getAttribute('data-quality')).toBe('unavailable');
    expect(getByTestId('cache-saved').getAttribute('data-quality')).toBe('unavailable');
    // No derived saving figure ⇒ no `derived` badge.
    expect(queryByTestId('saved-derived')).toBeNull();
  });

  // Gap 08 — a model with cached tokens but NO configured cache price: the hit rate is derived
  // (from counts) but "$ saved" is `—` (presence gate — no fabricated saving from the numeric 0.0).
  it('shows a derived hit rate but — for $ saved when no cached price is configured (gap 08)', async () => {
    seedFlows([
      makeFlow({
        api_call_id: 'api_g08c',
        status: 'completed',
        model_served: 'llama-3.1-70b',
        started_ms: 1_700_000_000_000,
        cost: 0.002,
        cost_confidence: 'estimated',
        usage: { prompt: 1000, completion: 200, total: 1200, cached: 300 },
      }),
    ]);
    act(() => dashboardStore.getState().seedTopology({
      topology_seq: 1, nodes: [], edges: [],
      price_table: { 'llama-3.1-70b': { input_per_1k: 0.0008, output_per_1k: 0.0008, cached_per_1k: 0, cached_price_configured: false } },
    }));
    const { getByTestId, queryByTestId } = renderWithQuery(<FlowDetail apiCallId="api_g08c" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    expect(getByTestId('cache-hit').getAttribute('data-quality')).toBe('derived');
    expect(getByTestId('cache-hit').textContent).toContain('30.0%');
    const saved = getByTestId('cache-saved');
    expect(saved.getAttribute('data-quality')).toBe('unavailable');
    expect(saved.textContent).toContain('—');
    expect(queryByTestId('saved-derived')).toBeNull();
  });

  // Gap 07 — a provider-REPORTED measured `0` cached renders `0` (distinct from `—`).
  it('renders a measured 0 cached as 0 (distinct from unavailable)', async () => {
    seedFlows([
      makeFlow({
        api_call_id: 'api_g07b',
        status: 'completed',
        model_served: 'gpt-4o',
        started_ms: 1_700_000_000_000,
        cost: 0.01,
        cost_confidence: 'confident',
        // cached reported as a measured 0; reasoning reported 120.
        usage: { prompt: 1500, completion: 980, total: 2480, cached: 0, reasoning: 120 },
      }),
    ]);
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_g07b" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    const sub = getByTestId('usage-subcounts');
    // A measured 0 reads `0` (not `—`); the reasoning count renders too.
    expect(sub.textContent).toContain('0');
    expect(sub.textContent).toContain('120');
    // A confident cost carries NO estimated badge.
    expect(() => getByTestId('cost-confidence')).toThrow();
  });

  // Gap 07 review round 3 — the cost VALUE and its CONFIDENCE tag must derive from the SAME source.
  // A stale REST detail row carrying `cost: null, cost_confidence: 'unavailable'` must NOT mask an
  // ESTIMATED live roll-up: the header must show the LIVE cost WITH its `est` badge, never `—`.
  // `api_g07c` is unknown to the mock so /flows/:id 404s and never overwrites the injected detail.
  it('does not let an unavailable detail tag mask an ESTIMATED live cost (paired source)', async () => {
    seedFlows([
      makeFlow({
        api_call_id: 'api_g07c',
        status: 'completed',
        model_served: 'llama-3.1-70b',
        upstream_target: 'vllm-a',
        started_ms: 1_700_000_000_000,
        // LIVE roll-up: an estimated cost that MUST surface (value + est marker), not be masked.
        cost: 0.0042,
        cost_confidence: 'estimated',
        usage: { prompt: 1500, completion: 980, total: 2480 },
      }),
    ]);
    const { getByTestId, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_g07c" onClose={noop} />);
    // Inject a stale detail with NO cost + an `unavailable` tag — the desync trap. Were the tag
    // derived independently (detail-first), it would mask the live estimated cost as `—`.
    const staleDetail: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_g07c', status: 'completed',
      cost: null, cost_confidence: 'unavailable',
      deltas: [], started_ms: 1_700_000_000_000,
      inbound_body: { model: 'gpt-4o' }, normalized: { model: 'm' }, upstream_body: { model: 'm' },
    };
    act(() => queryClient.setQueryData(['flows', 'api_g07c'], staleDetail));
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());

    // The LIVE estimated cost is shown (NOT `—`), paired with its own `estimated` tag → est badge.
    const costCell = getByTestId('detail-cost');
    expect(costCell.textContent).toContain('$0.0042');
    expect(costCell.textContent).not.toBe('—');
    expect(costCell.getAttribute('data-confidence')).toBe('estimated');
    expect(getByTestId('cost-confidence').getAttribute('data-confidence')).toBe('estimated');
  });

  // Gap 09 — the inspector header renders a context-window utilization gauge. A flow on a model
  // WITH a known `context_limit` (the mock catalog's gpt-4o = 128000) + reported usage shows a
  // DERIVED % + a filled track + a real headroom — never a fabricated 0%/100%. `api_g09` is unknown
  // to the mock so /flows/:id 404s and the SEEDED live row drives the header; `/catalog` is answered
  // by the mock so the gauge resolves the served model's window.
  it('renders a derived context-utilization gauge for a known context_limit + usage (gap 09)', async () => {
    seedFlows([
      makeFlow({
        api_call_id: 'api_g09',
        status: 'completed',
        model_served: 'gpt-4o', // mock catalog: context_limit 128000
        started_ms: 1_700_000_000_000,
        // PROMPT 64000 / 128000 ⇒ 50.0% (spec 09: prompt ÷ max_context). The completion 4000 is
        // IGNORED — the (buggy) total-based numerator would have read 68000/128000 = 53.1%.
        usage: { prompt: 64000, completion: 4000, total: 68000, cached: 0, reasoning: 0 },
      }),
    ]);
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_g09" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    // The catalog query resolves the gauge to a DERIVED 50.0% (from the PROMPT, not the total).
    await waitFor(() => expect(getByTestId('context-gauge').getAttribute('data-quality')).toBe('derived'));
    expect(getByTestId('context-util-pct').textContent).toBe('50.0%');
    expect(getByTestId('context-util-pct').textContent).not.toBe('53.1%'); // not the inflated total
    // A real fill + a real headroom (64.0k left of 128k).
    expect(getByTestId('context-gauge-fill').style.width).toBe('50%');
    expect(getByTestId('context-headroom').textContent).toContain('64.0k left');
  });

  // Gap 09 — a flow on a model WITHOUT a known window (the mock catalog's `mystery-model` =
  // context_limit null) renders the gauge as `—` (unavailable), NEVER a fabricated 0%/100%, even
  // though usage IS reported. This is the don't-lie-with-zeros core of the gap.
  it('renders — (unavailable) context gauge when the model context_limit is null (gap 09)', async () => {
    seedFlows([
      makeFlow({
        api_call_id: 'api_g09b',
        status: 'completed',
        model_served: 'mystery-model', // mock catalog: context_limit null (unknown capacity)
        started_ms: 1_700_000_000_000,
        usage: { prompt: 9000, completion: 1000, total: 10000, cached: 0, reasoning: 0 },
      }),
    ]);
    const { getByTestId, queryByTestId } = renderWithQuery(<FlowDetail apiCallId="api_g09b" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    // Even after the catalog resolves, an UNKNOWN-window model stays unavailable.
    await waitFor(() => expect(getByTestId('context-gauge').getAttribute('data-quality')).toBe('unavailable'));
    const pct = getByTestId('context-util-pct');
    expect(pct.textContent).toBe('—');
    expect(pct.textContent).not.toBe('0.0%');
    expect(pct.textContent).not.toBe('100.0%');
    // No fill element for an unavailable reading.
    expect(queryByTestId('context-gauge-fill')).toBeNull();
  });

  // Gap 07 review round 3 (companion) — same stale `unavailable` detail, but the live roll-up is
  // CONFIDENT: the header shows the live cost (paired with `confident`) and carries NO est badge.
  it('pairs a CONFIDENT live cost with its own tag despite an unavailable detail (no est badge)', async () => {
    seedFlows([
      makeFlow({
        api_call_id: 'api_g07d',
        status: 'completed',
        model_served: 'gpt-4o',
        started_ms: 1_700_000_000_000,
        cost: 0.0099,
        cost_confidence: 'confident',
        usage: { prompt: 1500, completion: 980, total: 2480, cached: 0, reasoning: 0 },
      }),
    ]);
    const { getByTestId, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_g07d" onClose={noop} />);
    const staleDetail: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_g07d', status: 'completed',
      cost: null, cost_confidence: 'unavailable',
      deltas: [], started_ms: 1_700_000_000_000,
      inbound_body: { model: 'gpt-4o' }, normalized: { model: 'm' }, upstream_body: { model: 'm' },
    };
    act(() => queryClient.setQueryData(['flows', 'api_g07d'], staleDetail));
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());

    const costCell = getByTestId('detail-cost');
    expect(costCell.textContent).toContain('$0.0099');
    expect(costCell.getAttribute('data-confidence')).toBe('confident');
    // A confident cost carries NO estimated badge — the `unavailable` detail tag did not leak.
    expect(() => getByTestId('cost-confidence')).toThrow();
  });

  // Gap 10 — the inspector header renders the latency breakdown. A flow whose live row carries the
  // full gap-02 phase spine + a gap-03 served attempt with a wire first byte ⇒ a MEASURED TTFT +
  // wire TTFB + every waterfall segment. `api_g10` is unknown to the mock so /flows/:id 404s and the
  // SEEDED live row drives the header (the spine fields flatten onto the row).
  it('renders a MEASURED latency breakdown from the live spine (gap 10)', async () => {
    const t0 = 1_700_000_000_000;
    seedFlows([
      makeFlow({
        api_call_id: 'api_g10',
        status: 'completed',
        model_served: 'llama-3.1-70b',
        started_ms: t0,
        usage: { prompt: 100, completion: 500, total: 600, cached: 0, reasoning: 0 },
        ingress_ms: t0,
        normalization_done_ms: t0 + 30,
        routing_decision_ms: t0 + 50,
        first_upstream_byte_ms: t0 + 270,
        first_content_delta_ms: t0 + 450,
        stream_end_ms: t0 + 1450,
        finalize_ms: t0 + 1470,
        attempts: [{ provider: 'vllm-a', model: 'llama-3.1-70b', start_ms: t0 + 50, end_ms: t0 + 270, first_upstream_byte_ms: t0 + 270, status: 'served' }],
      }),
    ]);
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_g10" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());

    // TTFT is MEASURED (first content − ingress = 450ms) — no est badge.
    await waitFor(() => expect(getByTestId('latency-ttft').getAttribute('data-quality')).toBe('measured'));
    expect(getByTestId('latency-ttft').textContent).toContain('450ms');
    expect(getByTestId('latency-ttft').querySelector('[data-testid="latency-quality-badge"]')).toBeNull();
    // Wire TTFB is measured (270ms) and the upstream-wait segment is enriched.
    expect(getByTestId('latency-ttfb').getAttribute('data-quality')).toBe('measured');
    expect(getByTestId('latency-seg-upstream').getAttribute('data-quality')).toBe('measured');
    // Every phase legend reads measured.
    for (const id of ['queue', 'routing', 'upstream', 'prefill', 'generation', 'finalize']) {
      expect(getByTestId(`latency-legend-${id}`).getAttribute('data-quality')).toBe('measured');
    }
  });

  // Gap 10 — when `first_content_delta_ms` is ABSENT for a flow, the breakdown falls back to a
  // DERIVED "first-visible-activity" TTFT from the live monitor `output` segments, LABELLED `est`
  // (never presented as the measured upstream first byte). Drive it via the live monitor ring.
  it('falls back to a DERIVED (est) TTFT from monitor output when first_content_delta is absent (gap 10)', async () => {
    const t0 = 1_700_000_000_000;
    seedFlows([
      makeFlow({
        api_call_id: 'api_g10b',
        response_id: 'resp_g10b',
        status: 'open',
        model_served: 'llama-3.1-70b',
        started_ms: t0,
        // phases present EXCEPT first_content_delta (the spine has not stamped TTFT for this flow).
        ingress_ms: t0,
        normalization_done_ms: t0 + 30,
        routing_decision_ms: t0 + 50,
      }),
    ]);
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_g10b" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());

    // Before any visible activity ⇒ TTFT unavailable (`—`), never 0.
    await waitFor(() => expect(getByTestId('latency-ttft').getAttribute('data-quality')).toBe('unavailable'));
    expect(getByTestId('latency-ttft').textContent).toContain('—');

    // A live monitor `output` segment 300ms after started_ms supplies the DERIVED fallback.
    pushMonitor({ type: 'segment_append', response_id: 'resp_g10b', segment: { timestamp_ms: t0 + 300, kind: 'output', text: 'Hi' } }, 4);
    await waitFor(() => expect(getByTestId('latency-ttft').getAttribute('data-quality')).toBe('estimated'));
    expect(getByTestId('latency-ttft').textContent).toContain('300ms');
    // It is LABELLED `est` (a derived first-visible-activity figure, not the measured upstream byte).
    const badge = getByTestId('latency-ttft').querySelector('[data-testid="latency-quality-badge"]');
    expect(badge?.textContent).toBe('est');
  });

  // Gap 10b review round 2 — the inspector SPINE merge had the same empty-array drop as the table
  // merge: a live row carrying `attempts: []` (a snapshot's "no attempt recorded yet") would, under
  // `liveFlow?.attempts ?? frozenDetail?.attempts`, SUPPRESS the populated REST detail trace — so the
  // latency model's wire TTFB (read from the served attempt's `first_upstream_byte_ms`) wrongly went
  // `unavailable`. `pickAttempts` lets the non-empty REST detail attempts surface. `api_g10c` is
  // unknown to the mock so /flows/:id 404s and never overwrites the injected detail.
  it('a live attempts[] [] does NOT suppress the populated REST detail trace (wire TTFB measured)', async () => {
    const t0 = 1_700_000_000_000;
    // The live store row carries the phase spine but an EMPTY attempts list (snapshot serialization).
    seedFlows([
      makeFlow({
        api_call_id: 'api_g10c',
        status: 'completed',
        model_served: 'llama-3.1-70b',
        started_ms: t0,
        usage: { prompt: 100, completion: 500, total: 600, cached: 0, reasoning: 0 },
        ingress_ms: t0,
        routing_decision_ms: t0 + 50,
        first_content_delta_ms: t0 + 450,
        stream_end_ms: t0 + 1450,
        attempts: [], // EMPTY — the "no attempt recorded yet" serialization; must NOT win.
      }),
    ]);
    const { getByTestId, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_g10c" onClose={noop} />);
    // The REST detail carries the POPULATED served attempt (with a wire first byte) + flow-level TTFB.
    const detail: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_g10c', status: 'completed', cost_confidence: 'unavailable',
      deltas: [], started_ms: t0,
      inbound_body: { model: 'gpt-4o' }, normalized: { model: 'm' }, upstream_body: { model: 'm' },
      attempts: [{ provider: 'vllm-a', model: 'llama-3.1-70b', start_ms: t0 + 50, end_ms: t0 + 270, first_upstream_byte_ms: t0 + 270, status: 'served' }],
      first_upstream_byte_ms: t0 + 270,
    };
    act(() => queryClient.setQueryData(['flows', 'api_g10c'], detail));
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());

    // The REST detail's served attempt surfaces ⇒ wire TTFB is MEASURED (270ms), not unavailable —
    // proving the empty live [] did not suppress the populated trace.
    await waitFor(() => expect(getByTestId('latency-ttfb').getAttribute('data-quality')).toBe('measured'));
    expect(getByTestId('latency-ttfb').textContent).toContain('270ms');
    // The upstream-wait segment is likewise enriched from the served attempt's first byte.
    expect(getByTestId('latency-seg-upstream').getAttribute('data-quality')).toBe('measured');
  });

  // Gap 10b review round 2 (companion) — both sides empty ⇒ the wire TTFB is honestly `unavailable`
  // (`—`), never a fabricated 0ms, and no attempt is invented.
  it('renders an unavailable wire TTFB when both live and detail attempts are empty (no fabrication)', async () => {
    const t0 = 1_700_000_000_000;
    seedFlows([
      makeFlow({
        api_call_id: 'api_g10d',
        status: 'completed',
        model_served: 'llama-3.1-70b',
        started_ms: t0,
        ingress_ms: t0,
        first_content_delta_ms: t0 + 450,
        attempts: [], // no served attempt anywhere…
      }),
    ]);
    const { getByTestId, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_g10d" onClose={noop} />);
    const detail: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_g10d', status: 'completed', cost_confidence: 'unavailable',
      deltas: [], started_ms: t0,
      inbound_body: { model: 'gpt-4o' }, normalized: { model: 'm' }, upstream_body: { model: 'm' },
      attempts: [], // …and the detail is empty too (no fabricated wire byte).
    };
    act(() => queryClient.setQueryData(['flows', 'api_g10d'], detail));
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    // No served attempt + no flow-level first byte ⇒ wire TTFB unavailable, rendered `—`, never 0.
    await waitFor(() => expect(getByTestId('latency-ttfb').getAttribute('data-quality')).toBe('unavailable'));
    expect(getByTestId('latency-ttfb').textContent).toContain('—');
  });
});

describe('FlowDetail — ErrorTab captured upstream body (gap 14)', () => {
  beforeEach(() => {
    // Mock mode: mockFetch serves /flows/:id incl. the gap-05 `upstream_response` (capture ON only
    // for api_003). Seed the two FAILED flows so the inspector opens with their failed status.
    resetWorld({ mock: true });
    seedFlows([
      makeFlow({ api_call_id: 'api_003', status: 'failed', model_requested: 'gpt-4o', model_served: 'gpt-4o', upstream_target: 'openai', terminal_reason: 'upstream 503', started_ms: 1_700_000_000_000 }),
      makeFlow({ api_call_id: 'api_006', status: 'failed', model_requested: 'llama-3.1-70b', model_served: 'llama-3.1-70b', upstream_target: 'vllm-b', terminal_reason: 'upstream timeout', started_ms: 1_700_000_000_000 }),
    ]);
  });
  afterEach(cleanup);

  it('capture ON: shows the captured upstream error body (measured) when present', async () => {
    const { getByTestId, getByRole } = renderWithQuery(<FlowDetail apiCallId="api_003" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    fireEvent.click(getByRole('tab', { name: 'Error' }));
    // The captured-body section is present + measured, and renders the upstream error JSON.
    await waitFor(() => expect(getByTestId('error-capture')).toBeTruthy());
    const capture = getByTestId('error-capture');
    expect(capture.getAttribute('data-state')).toBe('captured');
    expect(capture.getAttribute('data-quality')).toBe('measured');
    expect(getByTestId('error-capture-body').textContent).toContain('Service Unavailable');
    // No "capture disabled" placeholder when a body IS present.
    expect(within(capture).queryByTestId('error-capture-disabled')).toBeNull();
  });

  it('capture OFF: shows an explicit "capture disabled" state — NOT a blank implying "no error"', async () => {
    const { getByTestId, getByRole } = renderWithQuery(<FlowDetail apiCallId="api_006" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    fireEvent.click(getByRole('tab', { name: 'Error' }));
    await waitFor(() => expect(getByTestId('error-capture')).toBeTruthy());
    const capture = getByTestId('error-capture');
    // Unavailable state — explicit "capture disabled", tagged unavailable (don't-lie-with-zeros).
    expect(capture.getAttribute('data-state')).toBe('unavailable');
    expect(capture.getAttribute('data-quality')).toBe('unavailable');
    const disabled = getByTestId('error-capture-disabled');
    expect(disabled.textContent).toMatch(/capture disabled/i);
    expect(disabled.textContent).toMatch(/Not "no error"/);
    // The terminal reason still shows (this IS an error) — the capture-disabled is ADDITIVE, not "no error".
    expect(getByTestId('error-tab').textContent).toContain('upstream timeout');
    // And there is no captured body element.
    expect(within(capture).queryByTestId('error-capture-body')).toBeNull();
  });

  it('FAILED flow + NO terminal_reason + NO monitor error + capture OFF ⇒ the unavailable capture block, NOT "No error." (review round-2 HIGH)', async () => {
    // An id the mock does NOT know (so /flows/:id 404s and never overwrites our injected detail).
    // The store row is FAILED; the injected detail has NO terminal_reason and NO upstream_response.
    seedFlows([makeFlow({ api_call_id: 'api_bare_fail', status: 'failed', started_ms: 1_700_000_000_000 })]);
    const bare: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_bare_fail', status: 'failed', cost_confidence: 'unavailable',
      deltas: [], started_ms: 1_700_000_000_000,
      // terminal_reason + upstream_response intentionally ABSENT.
    };
    const { getByTestId, getByRole, queryByTestId, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_bare_fail" onClose={noop} />);
    act(() => queryClient.setQueryData(['flows', 'api_bare_fail'], bare));
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    fireEvent.click(getByRole('tab', { name: 'Error' }));
    // It must NOT short-circuit to "No error." — the explicit capture-disabled/unavailable block renders.
    await waitFor(() => expect(getByTestId('error-capture')).toBeTruthy());
    expect(queryByTestId('error-empty')).toBeNull();
    const capture = getByTestId('error-capture');
    expect(capture.getAttribute('data-state')).toBe('unavailable');
    expect(capture.getAttribute('data-quality')).toBe('unavailable');
    expect(getByTestId('error-capture-disabled').textContent).toMatch(/capture disabled/i);
  });

  it('SEEKING a FAILED historical flow (no terminal_reason/monitor error) ⇒ the capture block (historical unavailable), NOT "No error." (review round-3 HIGH)', async () => {
    // A historically-FAILED flow in the frozen snapshot, with NO terminal_reason. While seeking, the
    // live body is suppressed (live-only) — so the capture block must render the HISTORICAL unavailable
    // state, NOT fall through to "No error." (the round-3 seek-path violation).
    seedFlows([makeFlow({ api_call_id: 'api_seek_fail', status: 'failed', terminal_reason: null, started_ms: 1_700_000_000_000 })]);
    const { getByTestId, getByRole, queryByTestId } = renderWithQuery(<FlowDetail apiCallId="api_seek_fail" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    // Enter seek (D11 paused) — the connection flips to 'seeking'; the frozen row IS the failed flow.
    act(() => dashboardStore.getState().setConnection('seeking'));
    fireEvent.click(getByRole('tab', { name: 'Error' }));
    // Must NOT read "No error." — the explicit historical-unavailable capture block renders instead.
    await waitFor(() => expect(getByTestId('error-capture')).toBeTruthy());
    expect(queryByTestId('error-empty')).toBeNull();
    const capture = getByTestId('error-capture');
    expect(capture.getAttribute('data-state')).toBe('unavailable');
    expect(capture.getAttribute('data-quality')).toBe('unavailable');
    // It is the HISTORICAL state (live-only), distinct from the live "capture disabled" wording.
    const historical = getByTestId('error-capture-historical');
    expect(historical.textContent).toMatch(/historical view/i);
    expect(historical.textContent).toMatch(/Not "no error"/);
    expect(queryByTestId('error-capture-disabled')).toBeNull();
    expect(queryByTestId('error-capture-body')).toBeNull();
  });

  it('SUCCESSFUL flow with no error still reads "No error." (the distinction is preserved)', async () => {
    // An id the mock does NOT know; a COMPLETED store row + an injected detail with no error/no body.
    seedFlows([makeFlow({ api_call_id: 'api_ok_noerr', status: 'completed', started_ms: 1_700_000_000_000 })]);
    const ok: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_ok_noerr', status: 'completed', cost_confidence: 'unavailable',
      deltas: [], started_ms: 1_700_000_000_000,
    };
    const { getByTestId, getByRole, queryByTestId, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_ok_noerr" onClose={noop} />);
    act(() => queryClient.setQueryData(['flows', 'api_ok_noerr'], ok));
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    fireEvent.click(getByRole('tab', { name: 'Error' }));
    // A clean success ⇒ "No error." (no capture-disabled nag on a non-error flow).
    await waitFor(() => expect(getByTestId('error-empty')).toBeTruthy());
    expect(getByTestId('error-empty').textContent).toContain('No error.');
    expect(queryByTestId('error-capture')).toBeNull();
  });
});

describe('FlowDetail — time-travel seek + body eviction', () => {
  beforeEach(() => {
    resetWorld({ mock: true });
    // An id the mock does NOT know (so /flows/:id 404s and never overwrites our injected
    // body-free detail) — modeling a historical flow whose live body was evicted (D5).
    seedFlows([makeFlow({ api_call_id: 'api_evicted', response_id: 'resp_evicted', status: 'completed', started_ms: 1_700_000_000_000 })]);
  });
  afterEach(cleanup);

  it('shows the snapshot badge and "body evicted (snapshot)" when seeking with no body', async () => {
    // A detail with evicted bodies (absent fields) — what /flows/:id returns post-eviction.
    const evicted: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_evicted', response_id: 'resp_evicted', status: 'completed', cost_confidence: 'unavailable',
      deltas: [], started_ms: 1_700_000_000_000,
      // inbound_body / normalized / upstream_body intentionally ABSENT (evicted).
    };
    const { queryClient } = renderWithQuery(<FlowDetail apiCallId="api_evicted" onClose={noop} />);
    // Seed the body-free detail (the 404 fetch error won't replace manually-set data).
    act(() => queryClient.setQueryData(['flows', 'api_evicted'], evicted));
    // Enter seek (D11 paused) — the store connection flips to 'seeking'.
    act(() => dashboardStore.getState().setConnection('seeking'));

    await waitFor(() => {
      expect(document.querySelector('[data-testid="seek-badge"]')).toBeTruthy();
    });
    // Each pane shows the evicted placeholder (with the snapshot qualifier) rather than JSON.
    const empties = document.querySelectorAll('[data-testid^="jsonpane-empty-"]');
    expect(empties.length).toBe(3);
    expect(empties[0]!.textContent).toContain('body evicted');
  });

  it('DISABLES the kill button while seeking — no mutation against the frozen cut (finding 2)', async () => {
    // A historically-OPEN flow in the frozen snapshot. While seeking, the kill must be disabled so
    // the optimistic patch cannot mutate the frozen store row (and no abort POST is sent).
    seedFlows([makeFlow({ api_call_id: 'api_open_hist', response_id: 'resp_open_hist', status: 'open', started_ms: 1_700_000_000_000 })]);
    authStore.getState().setMutationsEnabled(true);
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_open_hist" onClose={noop} />);
    await waitFor(() => expect(getByTestId('kill-button')).toBeTruthy());
    // Enter seek.
    act(() => dashboardStore.getState().setConnection('seeking'));
    const btn = getByTestId('kill-button');
    expect(btn).toBeDisabled();
    expect(btn.getAttribute('title')).toBe('paused (time-travel)');
    // A disabled button cannot click; even a forced kill is a no-op (store row stays 'open').
    fireEvent.click(btn);
    expect(dashboardStore.getState().flows.get('api_open_hist')?.status).toBe('open');
  });

  it('derives cost + elapsed from the FROZEN cut while seeking — no Date.now / live cost (findings 1+6)', async () => {
    // The frozen snapshot row is OPEN with a server roll-up cost; the LIVE /flows/:id detail
    // carries a DIFFERENT (post-seek) cost and a long elapsed. While seeking, the header must show
    // the frozen row's cost and an elapsed derived from the cut `at_ms`, not wall-clock Date.now().
    const started = 1_700_000_000_000;
    // Gap 07: a real server roll-up cost carries a real confidence tag — pair the $ with `confident`
    // so the header renders it (an `unavailable` tag would correctly render `—`, never `$0.1234`).
    seedFlows([makeFlow({
      api_call_id: 'api_frozen', response_id: 'resp_frozen', status: 'open',
      started_ms: started, cost: 0.1234, cost_confidence: 'confident',
    })]);
    const liveDetail: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_frozen', response_id: 'resp_frozen', status: 'open', cost_confidence: 'unavailable',
      deltas: [], started_ms: started,
      inbound_body: { model: 'gpt-4o' }, normalized: { model: 'm' }, upstream_body: { model: 'm' },
      cost: 0.9999, elapsed_ms: 999_000, // live values that must NOT bleed into the frozen view
    };
    const { getByTestId, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_frozen" onClose={noop} />);
    act(() => queryClient.setQueryData(['flows', 'api_frozen'], liveDetail));
    // Enter seek with a KNOWN cut 5s after the flow started → deterministic frozen elapsed.
    act(() => dashboardStore.getState().enterSeek(started + 5_000));
    await waitFor(() => expect(document.querySelector('[data-testid="seek-badge"]')).toBeTruthy());

    const text = getByTestId('flow-detail').textContent ?? '';
    // Frozen roll-up cost shown; the live REST cost is NOT used.
    expect(text).toContain('$0.1234');
    expect(text).not.toContain('$1.00'); // 0.9999 would round to $1.00
    // Elapsed for the OPEN frozen flow = at_ms - started_ms = 5000ms → 5.0s. NOT the live detail's
    // 999000ms (→ 16m39s) and NOT a wall-clock Date.now() tick.
    expect(text).toContain('5.0s');
    expect(text).not.toContain('16m');
  });

  it('while seeking, an in-cut flow does NOT leak live REST deltas/headers; only the cut-bounded monitor stream shows (finding 1)', async () => {
    // The frozen cut is at monitor_seq=2. A live /flows/:id replay (post-cut) carries deltas + an
    // auth header that MUST NOT render while seeking. A monitor segment stamped at seq=2 is in the
    // cut (shows); one stamped at seq=3 is POST-cut (hidden).
    const started = 1_700_000_000_000;
    seedFlows([makeFlow({ api_call_id: 'api_cut', response_id: 'resp_cut', status: 'open', started_ms: started })]);
    // Cursor reflects the live monitor_seq the cut will freeze at.
    act(() => dashboardStore.getState().setCursor('monitor_seq', 2));
    // Two monitor segments: one AT the cut (seq 2, shown), one AFTER (seq 3, hidden).
    act(() => dashboardStore.getState().pushMonitor({ type: 'segment_append', response_id: 'resp_cut', segment: { timestamp_ms: 10, kind: 'output', text: 'IN-CUT' } }, 2));
    act(() => dashboardStore.getState().pushMonitor({ type: 'segment_append', response_id: 'resp_cut', segment: { timestamp_ms: 20, kind: 'output', text: 'POST-CUT' } }, 3));
    const liveDetail: FlowDetailDto = {
      flow_seq: 1, api_call_id: 'api_cut', response_id: 'resp_cut', status: 'open', cost_confidence: 'unavailable',
      started_ms: started, inbound_headers: { authorization: 'Bearer LEAK' },
      inbound_body: { model: 'gpt-4o' }, normalized: { model: 'm' }, upstream_body: { model: 'm' },
      deltas: [{ sequence: 1, kind: 'response.output_text.delta', payload: { text: 'REST-LEAK' }, ts_ms: 5 }],
    };
    const { getByTestId, getByRole, queryClient } = renderWithQuery(<FlowDetail apiCallId="api_cut" onClose={noop} />);
    act(() => queryClient.setQueryData(['flows', 'api_cut'], liveDetail));
    act(() => dashboardStore.getState().enterSeek(started + 1_000));
    await waitFor(() => expect(document.querySelector('[data-testid="seek-badge"]')).toBeTruthy());

    // Deltas: the in-cut monitor segment shows; the post-cut segment and the REST replay do NOT.
    const deltas = getByTestId('deltas-panel').textContent ?? '';
    expect(deltas).toContain('IN-CUT');
    expect(deltas).not.toContain('POST-CUT');
    expect(deltas).not.toContain('REST-LEAK');
    // Headers: the live REST auth header is withheld while seeking (frozen cut has no headers).
    fireEvent.click(getByRole('tab', { name: 'Headers' }));
    expect(getByTestId('headers-empty')).toBeTruthy();
  });

  it('while seeking, an OUT-of-cut selection fetches no detail and shows evicted panes (finding 1)', async () => {
    // A selection NOT present in the frozen snapshot rows must not fetch /flows/:id at all. The
    // mock answers api_001's detail; selecting it while seeking with an EMPTY cut must still show
    // the evicted placeholders (no live body), proving the detail query was gated off.
    act(() => dashboardStore.getState().enterSeek(1_700_000_000_000));
    const { queryClient } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    // Even after a tick, no detail data lands (the query is disabled out-of-cut while seeking).
    await act(async () => { await Promise.resolve(); });
    expect(queryClient.getQueryData(['flows', 'api_001'])).toBeUndefined();
    const empties = document.querySelectorAll('[data-testid^="jsonpane-empty-"]');
    expect(empties.length).toBe(3);
  });

  it('a kill in-flight when seek begins does NOT roll back into the frozen store (finding 2)', async () => {
    // Dispatch a kill that will FAIL (403, no CSRF) so `onError` fires. BEFORE it resolves, enter
    // seek and swap in a frozen snapshot cut. The optimistic rollback must be SKIPPED (the live
    // epoch changed) — otherwise it would re-insert the killed flow's prior 'open' row into the
    // frozen cut, leaking live data past the seek boundary.
    const started = 1_700_000_000_000;
    resetWorld({ mock: true });
    authStore.getState().setMutationsEnabled(true);
    authStore.getState().setCsrfToken('mock-csrf-token');
    seedFlows([makeFlow({ api_call_id: 'api_001', response_id: 'resp_001', status: 'open', started_ms: started })]);
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    await waitFor(() => expect(getByTestId('kill-button')).toBeTruthy());
    // Clear the CSRF AFTER the connection seeded it (getConnection re-seeds on build), so the kill
    // omits X-CSRF-Token → the mock answers 403 → the mutation's onError path runs.
    act(() => authStore.getState().setCsrfToken(null));
    document.cookie = 'llmconduit_csrf=; expires=Thu, 01 Jan 1970 00:00:00 GMT';

    // Fire the kill (optimistic flip to cancelled in onMutate), THEN immediately enter seek + swap
    // the store to a frozen snapshot cut while the 403 is still in flight.
    fireEvent.click(getByTestId('kill-button'));
    expect(dashboardStore.getState().flows.get('api_001')?.status).toBe('cancelled');
    const frozenRow = makeFlow({ api_call_id: 'api_snap', status: 'completed', started_ms: started });
    act(() => dashboardStore.getState().applySnapshot({ cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 }, flows: [frozenRow], metrics: null, topology: null }));
    act(() => dashboardStore.getState().enterSeek(started + 1_000));

    // Let the 403 resolve. The epoch guard skips the rollback: the frozen cut keeps ONLY api_snap;
    // the killed flow's prior 'open' row is NOT re-inserted.
    await waitFor(() => expect(getByTestId('kill-forbidden')).toBeTruthy());
    expect(dashboardStore.getState().flows.has('api_snap')).toBe(true);
    expect(dashboardStore.getState().flows.has('api_001')).toBe(false);
    // Still frozen + only the snapshot row present.
    expect(dashboardStore.getState().connection).toBe('seeking');
    expect(dashboardStore.getState().flows.size).toBe(1);
  });

  it('a kill failing AFTER a live→seek→live round-trip does NOT re-insert the stale optimistic row (finding 1)', async () => {
    // THE case the connection STRING could not catch: a kill is dispatched while LIVE; before the
    // 403 resolves the app seeks then returns to LIVE. The string epoch is `'live'` at BOTH dispatch
    // and resolve, so the old guard would have re-run `upsertFlow(prev)` — re-inserting the killed
    // flow's prior 'open' row into the NEW live store. The monotonic generation advanced across the
    // round-trip, so the rollback is correctly skipped.
    const started = 1_700_000_000_000;
    resetWorld({ mock: true });
    authStore.getState().setMutationsEnabled(true);
    authStore.getState().setCsrfToken('mock-csrf-token');
    seedFlows([makeFlow({ api_call_id: 'api_001', response_id: 'resp_001', status: 'open', started_ms: started })]);
    const { getByTestId } = renderWithQuery(<FlowDetail apiCallId="api_001" onClose={noop} />);
    await waitFor(() => expect(getByTestId('kill-button')).toBeTruthy());
    // Drop the CSRF so the kill 403s and onError runs.
    act(() => authStore.getState().setCsrfToken(null));
    document.cookie = 'llmconduit_csrf=; expires=Thu, 01 Jan 1970 00:00:00 GMT';

    // Fire the kill (optimistic flip), THEN round-trip live→seek→live while the 403 is in flight.
    fireEvent.click(getByTestId('kill-button'));
    expect(dashboardStore.getState().flows.get('api_001')?.status).toBe('cancelled');
    act(() => dashboardStore.getState().setConnection('seeking'));
    // Return to LIVE via a fresh snapshot that does NOT contain api_001 (it was killed server-side),
    // then flip back to 'live' — exactly the socket's resume path (commitSnapshot → setConnection).
    act(() => {
      dashboardStore.getState().applySnapshot({ cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 }, flows: [], metrics: null, topology: null });
      dashboardStore.getState().setConnection('live');
    });
    expect(dashboardStore.getState().connection).toBe('live');

    // Let the 403 resolve: the generation guard skips the rollback, so api_001 is NOT re-inserted.
    await waitFor(() => expect(getByTestId('kill-forbidden')).toBeTruthy());
    expect(dashboardStore.getState().flows.has('api_001')).toBe(false);
    expect(dashboardStore.getState().flows.size).toBe(0);
  });
});


describe('FlowDetail — Chain tab (predecessor diff from the durable content store)', () => {
  beforeEach(() => {
    resetWorld({ mock: true });
    // api_004 is the codex flow whose tool list changed vs its predecessor api_005 (mock seed).
    seedFlows([makeFlow({ api_call_id: 'api_004', response_id: 'resp_004', status: 'completed', harness: 'codex', session_id: 'sess_codex', chain_parent_request_id: 'api_005', divergence_kind: 'tools_changed', cache_bust: true, started_ms: 1_700_000_000_000 })]);
  });
  afterEach(cleanup);

  it('shows the lineage summary and diffs this body against the predecessor', async () => {
    const { getByTestId, getByRole, queryByTestId } = renderWithQuery(<FlowDetail apiCallId="api_004" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    expect(queryByTestId('chain-pane-row')).toBeNull();
    fireEvent.click(getByRole('tab', { name: 'Chain' }));
    const tab = getByTestId('chain-tab');
    expect(tab.getAttribute('data-cache-bust')).toBe('true');
    expect(getByTestId('chain-harness').textContent).toBe('codex');
    expect(getByTestId('chain-session').textContent).toBe('sess_codex');
    expect(getByTestId('chain-lineage').textContent).toBe('cache bust · tools');
    expect(getByTestId('chain-predecessor').textContent).toBe('api_005');
    // Both reassembled bodies load from the mock history API; the diff tints the added tool.
    await waitFor(() => expect(getByTestId('jsonpane-code-T · this request').querySelectorAll('.json-line').length).toBeGreaterThan(0));
    await waitFor(() => expect(getByTestId('jsonpane-code-P · predecessor').querySelectorAll('.json-line').length).toBeGreaterThan(0));
    const tinted = getByTestId('jsonpane-code-T · this request').querySelectorAll('.json-line[data-diff]');
    expect(tinted.length).toBeGreaterThan(0);
    // The transformation panes come back when leaving the tab.
    fireEvent.click(getByRole('tab', { name: 'Headers' }));
    expect(queryByTestId('chain-pane-row')).toBeNull();
    expect(getByTestId('pane-row')).toBeTruthy();
  });

  it('a chain start has no predecessor pane content and says so', async () => {
    seedFlows([makeFlow({ api_call_id: 'api_005', status: 'completed', harness: 'codex', session_id: 'sess_codex', divergence_kind: 'new_chain', cache_bust: false, started_ms: 1_700_000_000_000 })]);
    const { getByTestId, getByRole } = renderWithQuery(<FlowDetail apiCallId="api_005" onClose={noop} />);
    await waitFor(() => expect(getByTestId('flow-detail')).toBeTruthy());
    fireEvent.click(getByRole('tab', { name: 'Chain' }));
    expect(getByTestId('chain-predecessor').textContent).toBe('—');
    expect(getByTestId('chain-lineage').textContent).toBe('new');
    expect(getByTestId('jsonpane-empty-P · predecessor').textContent).toContain('no predecessor');
  });
});
