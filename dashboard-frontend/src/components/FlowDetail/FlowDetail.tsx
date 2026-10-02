/**
 * FlowDetail — the 3-pane transformation inspector (D10 flagship).
 *
 * Layout:
 *   ┌ header: id, models, status chip, kill button ─────────────────────────────┐
 *   ├ 3 scroll-synced JSON panes ───────────────────────────────────────────────┤
 *   │   A inbound body   →   B normalized Responses   →   C upstream chat body    │
 *   │   (diff A→B left)      (diff A→B right)              (diff B→C right)        │
 *   ├ tabs: Headers / Timeline / Error ─────────────────────────────────────────┤
 *   └ deltas sub-panel (segment_append: output/reasoning/tool cards) ────────────┘
 *
 * The structural diff (./diff) tints each JSON PATH: B is tinted vs A (added/changed), C is
 * tinted vs B, and A surfaces what B removed. The panes scroll together (useScrollSync — the
 * containers, not react-virtual). Bodies absent from `/flows/:id` (evicted under the D5
 * body-free snapshot tradeoff, or while time-travel `seek` shows a historical cut) render the
 * pane's "body evicted" placeholder. Kill POSTs with CSRF, optimistically flips the row, and
 * shows a distinct state on 403.
 */
import { useMemo, useState } from 'react';
import type { CostConfidence, FlowDetail as FlowDetailDto, FlowSummary, Usage } from '../../api/types';
import { useDashboard } from '../../store/hooks';
import { Button } from '../ui/Button';
import { StatusChip } from '../FlowTable/StatusChip';
import { fmtElapsed, fmtModelPair, fmtTokens } from '../FlowTable/format';
import { costDisplay, elapsedMs, flowCost } from '../FlowTable/flowModel';
import { tokenEconomics } from '../FlowTable/tokenEconomics';
import { contextLimitFor, contextUtilization, type ContextUtilization } from '../FlowTable/contextUtilization';
import { ContextGauge } from '../FlowTable/ContextGauge';
import { latencyBreakdown, type LatencyBreakdown as LatencyBreakdownModel, type SpineFlow } from './latencyBreakdown';
import { LatencyBreakdown } from './LatencyBreakdown';
import { attemptTrace, type AttemptTrace as AttemptTraceModel } from './attemptTrace';
import { AttemptTrace } from './AttemptTrace';
import { capturedErrorBody } from '../FlowTable/failureTaxonomy';
import { pickAttempts } from '../../api/attempts';
import { useCatalog } from '../FlowTable/useCatalog';
import { JsonPane } from '../viz/JsonPane';
import { combineMiddleDiff, diffLayers } from './diff';
import { joinMonitor } from './monitorJoin';
import { mergeDeltas, normalizeRestDeltas, type SeqSegment } from './deltas';
import { DeltasPanel } from './DeltasPanel';
import { Timeline } from './Timeline';
import { useScrollSync } from './useScrollSync';
import { useFlowDetail, type KillState } from './useFlowDetail';
import { useChainDiff } from './useChainDiff';
import { divergenceLabel } from '../../views/sessions/sessionsModel';
import { cn } from '../../lib/cn';

type Tab = 'headers' | 'timeline' | 'error' | 'chain';

export function FlowDetail({ apiCallId, onClose }: { apiCallId: string; onClose: () => void }) {
  const { detail, frozenDetail, liveFlow, status, seeking, seekMonitorSeq, seekAtMs, mutationsEnabled, kill, killState } =
    useFlowDetail(apiCallId);
  const monitor = useDashboard((s) => s.monitor);
  const monitorSeqs = useDashboard((s) => s.monitorSeqs);
  const priceTable = useDashboard((s) => s.priceTable);
  // Gap 09: per-model context-window capacities (gap-06 nullable `context_limit`), for the gauge.
  const contextLimits = useCatalog();
  const [tab, setTab] = useState<Tab>('headers');
  // Shared search across all three layers (A inbound · B normalized · C upstream) — find a field
  // once and see how it transformed. Each JsonPane filters to matches + their ancestors.
  const [query, setQuery] = useState('');

  // The flow's response_id (engine id) joins the monitor ring to this flow. While seeking we read
  // it from the FROZEN row (not the live REST detail, which is withheld from non-body surfaces).
  const responseId = liveFlow?.response_id ?? frozenDetail?.response_id ?? null;
  // SEEK BOUND (finding 1): bound the join to the frozen `monitor_seq` so post-cut segments/events/
  // status never leak into the deltas/timeline/error. Live ⇒ no bound (the whole ring is current).
  const join = useMemo(
    () => joinMonitor(monitor, responseId, { seqs: monitorSeqs, maxSeq: seekMonitorSeq }),
    [monitor, monitorSeqs, responseId, seekMonitorSeq],
  );

  // Deltas shown in the sub-panel = the REST replay (base) MERGED with the live monitor segments
  // (appended) — finding 5. While seeking, the live REST replay (`detail.deltas`) is post-cut and
  // withheld; the cut-bounded monitor join alone supplies the frozen stream (finding 1). The merge
  // de-dups the seam by MonitorHub seq (finding 2): the live side carries `join.segmentSeqs` (the
  // per-segment `monitor_seq`) so a coalesced/same-millisecond tail merges without dup or drop.
  const liveSegs = useMemo<SeqSegment[]>(
    () => join.segments.map((segment, i) => ({ segment, seq: join.segmentSeqs[i] ?? null })),
    [join.segments, join.segmentSeqs],
  );
  const segments = useMemo(
    () => mergeDeltas(normalizeRestDeltas(frozenDetail?.deltas), liveSegs),
    [frozenDetail?.deltas, liveSegs],
  );

  // Structural diffs between the captured layers (path → kind).
  const diffAB = useMemo(() => diffLayers(detail?.inbound_body, detail?.normalized), [detail?.inbound_body, detail?.normalized]);
  const diffBC = useMemo(() => diffLayers(detail?.normalized, detail?.upstream_body), [detail?.normalized, detail?.upstream_body]);
  // Pane B sits between both comparisons: it shows what A→B added/changed AND what B→C removes,
  // so it renders the COMBINED middle diff with side `both` (finding 4).
  const diffBMiddle = useMemo(() => combineMiddleDiff(diffAB, diffBC), [diffAB, diffBC]);

  const sync = useScrollSync(3);
  const isActive = status === 'open';
  // Sessions: the chain comparison (this flow's FULL inbound body vs its predecessor's, from the
  // durable content store). Fetched only while the Chain tab is open.
  const chain = useChainDiff(apiCallId, liveFlow?.chain_parent_request_id, tab === 'chain');
  const chainSync = useScrollSync(2);

  // Cost + its confidence tag are derived TOGETHER as a PAIR from the SAME source (gap 07 review
  // round 3): the displayed dollar value and the `estimated`/`unavailable` provenance tag MUST never
  // come from different rows, or a stale `detail` row with `cost: null, cost_confidence: 'unavailable'`
  // can mask an `estimated` LIVE cost as `—` (dropping the figure AND its est marker).
  //
  // Source precedence MIRRORS `flowCost`'s own roll-up-first precedence so the tag follows the value:
  //   1. server detail roll-up (`detail.cost` finite)  ⇒ pair with `detail.cost_confidence`
  //   2. else the live row's roll-up (`liveFlow.cost` finite) ⇒ pair with `liveFlow.cost_confidence`
  //   3. else usage×price fallback (neither roll-up) ⇒ freshest tag, live-first, mirroring the merged
  //      usage source (`liveFlow.usage ?? detail.usage`), so the tag tracks the usage being priced.
  // Building a merged summary (live status/usage wins; roll-up cost + detail fields fill gaps) lets
  // `flowCost` apply its roll-up-first precedence, so a live row LACKING cost no longer hides
  // `detail.cost`.
  //
  // SEEK coherence (finding 1/3): while seeking, `liveFlow` IS the frozen snapshot row and the live
  // REST detail is withheld (`frozenDetail` is null) — so BOTH cost and its tag derive EXCLUSIVELY
  // from the frozen summary, never the live REST roll-up.
  const { cost, costConfidence } = useMemo<{ cost: number | null; costConfidence: CostConfidence }>(() => {
    const summary = frozenDetail;
    if (!liveFlow && !summary) return { cost: null, costConfidence: 'unavailable' };
    const detailRollup = isFiniteNumber(summary?.cost);
    const liveRollup = isFiniteNumber(liveFlow?.cost);
    // The tag is taken from the EXACT source that supplies the displayed cost value.
    const costConfidence: CostConfidence = detailRollup
      ? summary!.cost_confidence
      : liveRollup
        ? liveFlow!.cost_confidence
        : // usage×price fallback (no roll-up either side): freshest tag, mirroring the merged usage.
          liveFlow?.cost_confidence ?? summary?.cost_confidence ?? 'unavailable';
    const merged: FlowSummary = {
      ...(summary ?? {}),
      ...(liveFlow ?? {}),
      api_call_id: apiCallId,
      method: liveFlow?.method ?? 'POST',
      uri: liveFlow?.uri ?? '',
      status: status ?? liveFlow?.status ?? summary?.status ?? 'open',
      started_ms: liveFlow?.started_ms ?? summary?.started_ms ?? 0,
      // Roll-up precedence: server detail roll-up first, then the live row's own roll-up.
      cost: summary?.cost ?? liveFlow?.cost ?? null,
      // Freshest usage/model for the usage×price fallback: live row first, then detail.
      usage: liveFlow?.usage ?? summary?.usage ?? null,
      model_served: liveFlow?.model_served ?? summary?.model_served ?? null,
      model_requested: liveFlow?.model_requested ?? summary?.model_requested ?? null,
      cost_confidence: costConfidence,
    };
    return { cost: flowCost(merged, priceTable), costConfidence };
  }, [liveFlow, frozenDetail, apiCallId, status, priceTable]);
  // Gap 07 — the cumulative token usage for the breakdown row (freshest: live, then detail):
  // cached/reasoning may be UNREPORTED (null/absent) ⇒ `fmtTokens` renders `—`, never `0`.
  const usage = liveFlow?.usage ?? frozenDetail?.usage ?? null;
  // Gap 08 — the token-economics breakdown (cache-hit rate + "$ saved by cache"), MIRRORING the
  // FlowTable tokens-cell popover so the inspector line shows the SAME honest figures. Built from
  // the freshest usage + the served model (for the cached-price PRESENCE gate). `usage`/model are
  // resolved live-first (seek ⇒ frozen-only); the served model gates `$ saved` via the gap-07
  // `cached_price_configured` flag — never the defaulted numeric `0.0`.
  const econ = useMemo(() => {
    const model = liveFlow?.model_served ?? frozenDetail?.model_served ?? liveFlow?.model_requested ?? frozenDetail?.model_requested ?? null;
    const econFlow: FlowSummary = {
      api_call_id: apiCallId,
      method: 'POST',
      uri: '',
      status: status ?? 'open',
      started_ms: liveFlow?.started_ms ?? frozenDetail?.started_ms ?? 0,
      cost_confidence: 'unavailable',
      usage,
      model_served: model,
    };
    return tokenEconomics(econFlow, priceTable);
  }, [liveFlow, frozenDetail, apiCallId, status, usage, priceTable]);
  // Gap 09 — the context-window utilization for the gauge: the freshest usage (live, then detail)
  // against the SERVED model's `context_limit` (gap-06 nullable catalog). `null` limit (unknown
  // capacity) OR unreported usage ⇒ the gauge renders `—`, never a fabricated 0%/100%. The served
  // model (then requested) is the one actually run, so its window is the relevant ceiling.
  const contextUtil = useMemo<ContextUtilization>(() => {
    const limit = contextLimitFor(
      liveFlow?.model_served ?? frozenDetail?.model_served,
      liveFlow?.model_requested ?? frozenDetail?.model_requested,
      contextLimits,
    );
    return contextUtilization(usage, limit);
  }, [liveFlow, frozenDetail, usage, contextLimits]);
  // Gap 10 — the per-flow LATENCY BREAKDOWN (the phase waterfall + the Timing line). The PRIMARY
  // source is the gap-02 phase epochs + gap-03 served-attempt wire TTFB, read live-first
  // (`liveFlow`) then from the frozen detail (seek). When `first_content_delta_ms` is absent the
  // model falls back to the monitor `output` segments for a DERIVED first-visible-activity TTFT
  // (explicitly labelled). Both the spine fields AND the monitor join are already cut-bounded
  // upstream (seek coherence holds): `liveFlow` is the frozen row while seeking, and `join` is the
  // cut-bounded monitor join (finding 1). The merged spine prefers the live row's freshest phase
  // values, filling any gap from the frozen detail.
  const latency = useMemo<LatencyBreakdownModel>(() => {
    // Merge the freshest spine: live row first (its phase/attempt fields win), frozen detail fills
    // gaps. Both carry the same flattened `PhaseTimings` + `attempts`/`first_upstream_byte_ms`.
    const spine: SpineFlow | null = liveFlow || frozenDetail
      ? {
          started_ms: liveFlow?.started_ms ?? frozenDetail?.started_ms ?? 0,
          ingress_ms: liveFlow?.ingress_ms ?? frozenDetail?.ingress_ms,
          normalization_done_ms: liveFlow?.normalization_done_ms ?? frozenDetail?.normalization_done_ms,
          routing_decision_ms: liveFlow?.routing_decision_ms ?? frozenDetail?.routing_decision_ms,
          first_content_delta_ms: liveFlow?.first_content_delta_ms ?? frozenDetail?.first_content_delta_ms,
          stream_end_ms: liveFlow?.stream_end_ms ?? frozenDetail?.stream_end_ms,
          finalize_ms: liveFlow?.finalize_ms ?? frozenDetail?.finalize_ms,
          finished_ms: liveFlow?.finished_ms ?? frozenDetail?.finished_ms,
          elapsed_ms: liveFlow?.elapsed_ms ?? frozenDetail?.elapsed_ms,
          // `attempts` is an ARRAY — it must NOT use the scalar `live ?? rest` rule (gap 10b review
          // round 2). A live/snapshot row can carry `attempts: []` ("no attempt recorded yet"), and
          // `??` would treat that `[]` as authoritative, SUPPRESSING the populated REST detail trace
          // so the latency model's wire TTFB (served attempt) goes `unavailable`. `pickAttempts` lets
          // a NON-EMPTY list (either side) win and treats `[]` as absent for backfill.
          attempts: pickAttempts(liveFlow?.attempts, frozenDetail?.attempts),
          first_upstream_byte_ms: liveFlow?.first_upstream_byte_ms ?? frozenDetail?.first_upstream_byte_ms,
          usage,
        }
      : null;
    // The derived-TTFT fallback source: the cut-bounded monitor `output` segments for this flow.
    const outputs = join.segments.filter((s) => s.kind === 'output');
    return latencyBreakdown(spine, outputs);
  }, [liveFlow, frozenDetail, usage, join.segments]);

  // Gap 11 — the per-flow FAILOVER / attempt trace (the inspector-header stepper). Consumes the
  // gap-03 `attempts[]` projected onto the row + detail + live frame (gap 10b). Merged with the SAME
  // `pickAttempts` rule the latency model uses (a live/snapshot `attempts: []` is "no attempt yet"
  // and must NOT suppress a populated REST detail trace; a non-empty list — either side — wins).
  // A single attempt ⇒ a single node (no fake failover); ≥2 ⇒ the chain; absent ⇒ no stepper.
  const attempts = useMemo<AttemptTraceModel>(
    () => attemptTrace(pickAttempts(liveFlow?.attempts, frozenDetail?.attempts)),
    [liveFlow?.attempts, frozenDetail?.attempts],
  );

  return (
    <section className="flex min-h-0 w-[46%] min-w-[420px] flex-col border-l border-line bg-panel" data-testid="flow-detail" aria-label="flow detail">
      <DetailHeader
        apiCallId={apiCallId}
        flow={liveFlow}
        detail={frozenDetail}
        cost={cost}
        costConfidence={costConfidence}
        usage={usage}
        econ={econ}
        contextUtil={contextUtil}
        latency={latency}
        attempts={attempts}
        seeking={seeking}
        seekAtMs={seekAtMs}
        isActive={isActive}
        mutationsEnabled={mutationsEnabled}
        killState={killState}
        onKill={() => kill(apiCallId)}
        onClose={onClose}
      />

      <SearchBar value={query} onChange={setQuery} />

      {/* Chain tab: the two-pane chain diff REPLACES the transformation panes (same footprint). */}
      {tab === 'chain' ? (
        <div className="grid min-h-0 flex-1 grid-cols-2 divide-x divide-line" data-testid="chain-pane-row">
          <JsonPane
            label="P · predecessor"
            value={chain.previous}
            diff={chain.diff}
            side="left"
            query={query}
            emptyLabel={chain.predecessorId ? (chain.loading ? 'loading…' : 'predecessor body not stored') : 'no predecessor (chain start)'}
            scrollRef={chainSync.refFor(0)}
            onScroll={chainSync.bind(0)}
          />
          <JsonPane
            label="T · this request"
            value={chain.current}
            diff={chain.diff}
            side="right"
            query={query}
            emptyLabel={chain.loading ? 'loading…' : 'body not stored (durable history off)'}
            scrollRef={chainSync.refFor(1)}
            onScroll={chainSync.bind(1)}
          />
        </div>
      ) : (
      <div className="grid min-h-0 flex-1 grid-cols-3 divide-x divide-line" data-testid="pane-row">
        <JsonPane
          label="A · inbound"
          value={detail?.inbound_body}
          diff={diffAB}
          side="left"
          query={query}
          emptyLabel={emptyBodyLabel(seeking)}
          scrollRef={sync.refFor(0)}
          onScroll={sync.bind(0)}
        />
        <JsonPane
          label="B · normalized"
          value={detail?.normalized}
          diff={diffBMiddle}
          side="both"
          query={query}
          emptyLabel={emptyBodyLabel(seeking)}
          scrollRef={sync.refFor(1)}
          onScroll={sync.bind(1)}
        />
        <JsonPane
          label="C · upstream"
          value={detail?.upstream_body}
          diff={diffBC}
          side="right"
          query={query}
          emptyLabel={emptyBodyLabel(seeking)}
          scrollRef={sync.refFor(2)}
          onScroll={sync.bind(2)}
        />
      </div>
      )}

      {/* tabs */}
      <div className="flex shrink-0 items-center gap-1 border-y border-line bg-panel-raised px-2 py-1" role="tablist">
        <TabButton id="headers" active={tab} onClick={setTab}>Headers</TabButton>
        <TabButton id="timeline" active={tab} onClick={setTab}>Timeline</TabButton>
        <TabButton id="error" active={tab} onClick={setTab}>Error</TabButton>
        <TabButton id="chain" active={tab} onClick={setTab}>Chain</TabButton>
      </div>
      <div className="max-h-44 min-h-[3rem] shrink-0 overflow-auto" role="tabpanel" data-testid={`tabpanel-${tab}`}>
        {/* Headers + Error read the FROZEN detail (null while seeking) so no live/post-cut metadata
            leaks; Timeline reads the cut-bounded monitor join (finding 1). */}
        {tab === 'headers' && <HeadersTab headers={frozenDetail?.inbound_headers} />}
        {tab === 'timeline' && <Timeline events={join.events} />}
        {tab === 'error' && <ErrorTab detail={frozenDetail} liveFlow={liveFlow} joinError={join.error} seeking={seeking} />}
        {tab === 'chain' && <ChainTab flow={liveFlow} predecessorId={chain.predecessorId} error={chain.error} />}
      </div>

      {/* deltas sub-panel */}
      <div className="flex max-h-52 shrink-0 flex-col overflow-auto border-t border-line">
        <div className="sticky top-0 z-10 border-b border-line bg-panel-raised px-3 py-1 text-[10px] uppercase tracking-wide text-text-muted">
          deltas
        </div>
        <DeltasPanel segments={segments} />
      </div>
    </section>
  );
}

/** When seeking a historical cut, a missing body is explicitly evicted (D5 tradeoff). */
function emptyBodyLabel(seeking: boolean): string {
  return seeking ? 'body evicted (snapshot)' : 'body evicted';
}

/** A roll-up cost is "present" (drives the displayed value) only when it is a finite number —
 * mirrors `flowCost`'s `typeof === 'number' && Number.isFinite` guard, so the paired confidence
 * tag is chosen from the SAME source `flowCost` will actually read the cost from. */
function isFiniteNumber(v: number | null | undefined): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

/** Shared search across the three layers — one query, every pane filters + highlights. */
function SearchBar({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <div
      className="flex shrink-0 items-center gap-2 border-b border-line bg-panel-raised px-3 py-1.5"
      data-testid="json-search-bar"
    >
      <div className="flex flex-1 items-center gap-2 rounded-md border border-line bg-panel px-2 py-1 transition-colors focus-within:border-accent/60">
        <svg viewBox="0 0 16 16" className="h-3.5 w-3.5 shrink-0 text-text-muted" fill="none" aria-hidden="true">
          <circle cx="7" cy="7" r="4.5" stroke="currentColor" strokeWidth="1.5" />
          <path d="M10.5 10.5 14 14" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        </svg>
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder="search all layers…"
          spellCheck={false}
          className="min-w-0 flex-1 bg-transparent font-mono text-xs text-text placeholder:text-text-muted focus:outline-none"
          data-testid="json-search-input"
        />
        {value && (
          <button
            type="button"
            onClick={() => onChange('')}
            aria-label="clear search"
            className="shrink-0 text-text-muted transition-colors hover:text-text"
          >
            ✕
          </button>
        )}
      </div>
      <span className="hidden shrink-0 font-mono text-[10px] uppercase tracking-[0.14em] text-text-muted sm:inline">
        A · B · C
      </span>
    </div>
  );
}

function DetailHeader({
  apiCallId,
  flow,
  detail,
  cost,
  costConfidence,
  usage,
  econ,
  contextUtil,
  latency,
  attempts,
  seeking,
  seekAtMs,
  isActive,
  mutationsEnabled,
  killState,
  onKill,
  onClose,
}: {
  apiCallId: string;
  flow: FlowSummary | null;
  detail: FlowDetailDto | null;
  cost: number | null;
  costConfidence: CostConfidence;
  usage: Usage | null;
  econ: ReturnType<typeof tokenEconomics>;
  contextUtil: ContextUtilization;
  latency: LatencyBreakdownModel;
  attempts: AttemptTraceModel;
  seeking: boolean;
  seekAtMs: number | null;
  isActive: boolean;
  mutationsEnabled: boolean;
  killState: KillState;
  onKill: () => void;
  onClose: () => void;
}) {
  // Gap 07: render the dollar STRING + the `estimated` flag together via the shared contract, so an
  // `unavailable` cost reads `—` (never `$0.00`) even if a stray number rode with the tag, and an
  // estimated figure is labelled — identical to the FlowTable + Sankey $ surfaces.
  const costView = costDisplay(cost, costConfidence);
  const status = flow?.status ?? detail?.status ?? 'open';
  const modelReq = flow?.model_requested ?? detail?.model_requested;
  const modelServed = flow?.model_served ?? detail?.model_served;
  const upstream = flow?.upstream_target ?? detail?.upstream_target ?? '—';
  // Elapsed: live = `elapsedMs` (which ticks an OPEN flow against `now`). SEEK coherence
  // (finding 6): a frozen cut must NOT read wall-clock `Date.now()` — that would leak time elapsed
  // AFTER the seeked instant. We pass the frozen cut `at_ms` as `now`, so an OPEN historical flow
  // reads its elapsed AS OF the cut (`at_ms - started_ms`), consistent with the table (which uses
  // the same `at_ms`); a finished flow still derives `finished-started` from the frozen row. `detail`
  // here is already the FROZEN detail (null while seeking), so non-body surfaces never read live.
  const elapsed = flow
    ? elapsedMs(flow, seeking ? seekAtMs ?? flow.started_ms : Date.now())
    : (seeking ? null : detail?.elapsed_ms ?? null);

  return (
    <header className="flex shrink-0 flex-col gap-2 border-b border-line bg-panel-raised px-3 py-2">
      <div className="flex items-center gap-2">
        <StatusChip status={status} terminalReason={flow?.terminal_reason ?? detail?.terminal_reason} />
        <span className="font-mono text-sm text-text" title={apiCallId}>{apiCallId}</span>
        {seeking && (
          <span className="rounded-sm bg-status-cooling/15 px-1.5 py-0.5 text-[10px] uppercase text-status-cooling" data-testid="seek-badge">
            snapshot
          </span>
        )}
        <div className="ml-auto flex items-center gap-2">
          <KillControl isActive={isActive} mutationsEnabled={mutationsEnabled} seeking={seeking} killState={killState} onKill={onKill} />
          <button
            type="button"
            onClick={onClose}
            aria-label="close detail"
            className="rounded-md border border-transparent px-2 py-1 text-sm text-text-muted hover:text-text"
          >
            ✕
          </button>
        </div>
      </div>
      <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs">
        <dt className="text-text-muted">model</dt>
        <dd className="font-mono text-text">{fmtModelPair(modelReq, modelServed)}</dd>
        <dt className="text-text-muted">upstream</dt>
        <dd className="font-mono text-text">{upstream}</dd>
        <dt className="text-text-muted">cost / elapsed</dt>
        <dd className="tabular-nums text-text">
          <span className="text-meta" data-testid="detail-cost" data-confidence={costView.confidence}>{costView.value}</span>
          {/* Gap 07: an `estimated` cost MUST be labelled (the cross-cutting rule) — a small
              tag so an operator never mistakes a best-effort figure for a confident one. An
              `unavailable` cost already reads as `—`; `confident` needs no badge. */}
          {costView.estimated && (
            <span
              className="ml-1.5 rounded-sm bg-status-cooling/15 px-1 py-0.5 text-[10px] uppercase tracking-wide text-status-cooling"
              data-testid="cost-confidence"
              data-confidence="estimated"
              title="cost is an estimate — a billed token class has no configured rate"
            >
              est
            </span>
          )}
          <span className="text-line"> · </span>
          {fmtElapsed(elapsed)}
        </dd>
        {/* Gap 07: the cached/reasoning token breakdown. An UNREPORTED class renders `—`
            (via `fmtTokens`), NEVER a fabricated `0`; a measured `0` reads `0`. */}
        <dt className="text-text-muted">cached / reasoning</dt>
        <dd className="tabular-nums text-text" data-testid="usage-subcounts">
          <span title="cache-read prompt tokens (— = upstream did not report)">{fmtTokens(usage?.cached)}</span>
          <span className="text-line"> · </span>
          <span title="reasoning tokens (— = upstream did not report)">{fmtTokens(usage?.reasoning)}</span>
        </dd>
        {/* Gap 08: the cache economics line, MIRRORING the table tokens-cell popover. The cache-hit
            rate is `derived` (`—` when cached unreported, never a 0% miss); "$ saved" is `derived`
            and shows only with a CONFIGURED cached price (presence) + a reported cached count —
            otherwise `—` (no fabricated saving). */}
        <dt className="text-text-muted">cache hit / $ saved</dt>
        <dd className="tabular-nums text-text" data-testid="cache-economics">
          <span data-testid="cache-hit" data-quality={econ.cacheHit.quality} title="cache-hit rate cached/prompt (derived; — = cached unreported)">
            {econ.cacheHit.value}
          </span>
          <span className="text-line"> · </span>
          <span
            data-testid="cache-saved"
            data-quality={econ.saved.quality}
            title={econ.cachedPriceConfigured
              ? '$ saved by serving cached tokens at the cached rate (derived)'
              : '$ saved unavailable — no configured cached price for this model'}
          >
            {econ.saved.value}
          </span>
          {/* The "$ saved" figure is DERIVED — labelled so an operator never reads it as a billed
              (measured) cost. Only rendered when a real saving figure is shown. */}
          {econ.saved.quality === 'derived' && (
            <span
              className="ml-1.5 rounded-sm bg-accent/15 px-1 py-0.5 text-[10px] uppercase tracking-wide text-accent"
              data-testid="saved-derived"
              title="cache saving is a derived figure (input rate − cached rate)"
            >
              derived
            </span>
          )}
        </dd>
        {/* Gap 09: the context-window utilization gauge (% of the input window the PROMPT consumed +
            remaining headroom + a near/over badge). Numerator is `Usage.prompt` only (spec 09 /
            FEATURES item 4) — the completion is not counted. `derived` only with a known model
            `context_limit` (gap-06) + reported prompt usage; UNKNOWN capacity or unreported prompt ⇒
            `—` and an empty dashed track, NEVER a fabricated 0%/100%. Spans the full row (a bar). */}
        <dt className="self-start text-text-muted" title="context-window utilization: prompt (input) tokens vs the model's context window">context</dt>
        <dd className="min-w-0">
          <ContextGauge util={contextUtil} />
        </dd>
        {/* Gap 10: the per-flow latency breakdown — a "Timing" line (TTFT/wire TTFB/total/tok-s)
            + a phase waterfall (queue → routing → upstream → prefill → generation → finalize). TTFT
            is `measured` from the gap-02 first-content-delta, else a labelled `estimated`
            first-visible-activity fallback from the monitor output segments; a phase with a missing
            endpoint renders `—` (no bar), never a fabricated 0ms. Spans the full row. */}
        <dt className="self-start text-text-muted" title="latency breakdown: where the turn spent its wall-clock — provider prefill/TTFT vs generation">timing</dt>
        <dd className="min-w-0">
          <LatencyBreakdown model={latency} />
        </dd>
        {/* Gap 11: the failover / attempt-trace stepper — one node per recorded `attempts[]` entry
            (provider, status/error_class, duration, first upstream byte, failover_reason), the served
            node visually distinct. Rendered ONLY when an attempt was recorded: a single attempt is a
            single node (no fake failover); ≥2 is the chain. A per-attempt unmeasured time reads `—`,
            never `0`. Absent ⇒ the row is omitted entirely (no empty stepper). Spans the full row. */}
        {attempts.hasTrace && (
          <>
            <dt className="self-start text-text-muted" title="failover trace: which provider failed, why, how long, and what served">failover</dt>
            <dd className="min-w-0">
              <AttemptTrace model={attempts} />
            </dd>
          </>
        )}
      </dl>
    </header>
  );
}

function KillControl({
  isActive,
  mutationsEnabled,
  seeking,
  killState,
  onKill,
}: {
  isActive: boolean;
  mutationsEnabled: boolean;
  seeking: boolean;
  killState: KillState;
  onKill: () => void;
}) {
  if (killState.phase === 'forbidden') {
    return <span className="text-xs text-status-down" data-testid="kill-forbidden">mutations disabled</span>;
  }
  if (killState.phase === 'killed') {
    return <span className="text-xs text-text-muted" data-testid="kill-done">killed</span>;
  }
  if (killState.phase === 'error') {
    return <span className="text-xs text-status-down" data-testid="kill-error" title={killState.message}>kill failed</span>;
  }
  if (!isActive) return null;
  // Kill mutates LIVE state; a frozen historical cut must not be mutable (finding 2). While
  // seeking we DISABLE the button — the optimistic `patchFlowStatus` would otherwise mutate the
  // frozen store row and the POST would abort a flow the operator is only inspecting in the past.
  const disabled = !mutationsEnabled || seeking || killState.phase === 'killing';
  const title = seeking ? 'paused (time-travel)' : mutationsEnabled ? 'abort this flow' : 'mutations disabled';
  return (
    <Button
      variant="danger"
      onClick={onKill}
      disabled={disabled}
      data-testid="kill-button"
      title={title}
    >
      {killState.phase === 'killing' ? 'killing…' : 'Kill'}
    </Button>
  );
}

function TabButton({ id, active, onClick, children }: { id: Tab; active: Tab; onClick: (t: Tab) => void; children: React.ReactNode }) {
  const selected = id === active;
  return (
    <button
      type="button"
      role="tab"
      aria-selected={selected}
      onClick={() => onClick(id)}
      className={cn(
        'rounded-md px-2.5 py-1 text-xs transition-colors',
        selected ? 'bg-accent/15 text-accent' : 'text-text-muted hover:text-text',
      )}
    >
      {children}
    </button>
  );
}

/**
 * Sessions — the Chain tab's summary strip: harness, session node, the divergence classification
 * (with its cache-bust verdict) and the predecessor id. The panes above show the structural diff.
 */
function ChainTab({ flow, predecessorId, error }: { flow: FlowSummary | null; predecessorId: string | null; error: string | null }) {
  const lineage = divergenceLabel(flow?.divergence_kind);
  const known = !!flow?.divergence_kind;
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 px-3 py-2 text-xs" data-testid="chain-tab" data-cache-bust={flow?.cache_bust === true ? 'true' : 'false'}>
      <span className="text-text-muted">harness</span>
      <span className="font-mono" data-testid="chain-harness">{flow?.harness ?? '—'}</span>
      <span className="text-text-muted">session</span>
      <span className="truncate font-mono" data-testid="chain-session" title={flow?.session_id ?? undefined}>{flow?.session_id ?? '—'}</span>
      <span className="text-text-muted">lineage</span>
      <span
        className={cn('rounded-sm px-1 text-[9px] uppercase tracking-wide', lineage.bust ? 'bg-status-cooling/15 text-status-cooling' : 'bg-line/40 text-text-muted')}
        data-testid="chain-lineage"
        data-kind={flow?.divergence_kind ?? undefined}
        title={lineage.title}
      >
        {known ? (lineage.bust ? `cache bust · ${lineage.label}` : lineage.label) : '—'}
      </span>
      <span className="text-text-muted">predecessor</span>
      <span className="font-mono" data-testid="chain-predecessor">{predecessorId ?? '—'}</span>
      {error && (
        <span className="text-status-down" data-testid="chain-error" title={error}>
          {error.includes('503') ? 'durable history disabled' : 'body fetch failed'}
        </span>
      )}
    </div>
  );
}

function HeadersTab({ headers }: { headers?: Record<string, string> }) {
  const entries = headers ? Object.entries(headers) : [];
  if (entries.length === 0) {
    return <div className="px-3 py-3 text-xs italic text-text-muted" data-testid="headers-empty">No inbound headers captured.</div>;
  }
  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 px-3 py-2 font-mono text-xs" data-testid="headers-tab">
      {entries.map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="text-accent">{k}</dt>
          <dd className="truncate text-text" title={v}>{v}</dd>
        </div>
      ))}
    </dl>
  );
}

/**
 * The Error tab (gap 14 enriched): the terminal reason + monitor error, PLUS the captured upstream
 * error BODY (gap 05's `upstream_response`, live-detail only) when present. The capture state is
 * EXPLICIT (spec 14 don't-lie-with-zeros): a captured body is shown (`measured`, truncation flagged);
 * when capture is OFF/no body, an ERROR flow shows an explicit `unavailable` line — NEVER a blank that
 * implies "no error".
 *
 * The "No error." empty state depends ONLY on `!isError` (review round-3 HIGH): a FAILED/error flow
 * must NEVER read "No error.", regardless of `seeking` or whether live-body capture is suppressed. So
 * the capture block renders whenever `isError`. The captured body itself is live-detail only (`detail`
 * is `frozenDetail`, `null` while seeking), so on a SEEKED historical flow it is honestly UNAVAILABLE
 * and labelled as the HISTORICAL state ("capture unavailable on historical view"), DISTINCT from the
 * live capture-disabled state — never "No error.".
 */
function ErrorTab({ detail, liveFlow, joinError, seeking }: { detail: FlowDetailDto | null; liveFlow: FlowSummary | null; joinError: string | null; seeking: boolean }) {
  const reason = liveFlow?.terminal_reason ?? detail?.terminal_reason ?? null;
  const status = liveFlow?.status ?? detail?.status ?? null;
  // A GENUINE error/failure — the flow FAILED, or a monitor error was reported. This (NOT a benign
  // `terminal_reason`, which a CLEAN completed flow also carries, e.g. `response.completed`) is what
  // warrants the upstream error-body capture block. Decoupled from `seeking`/capture suppression
  // (review round-3 HIGH): an error/failed flow ALWAYS renders the capture block + NEVER "No error.".
  const isError = status === 'failed' || !!joinError;
  const captured = capturedErrorBody(detail?.upstream_response);

  // "No error." ONLY when there is genuinely nothing to show: NOT an error AND no terminal reason to
  // display. A failed flow (`isError`) NEVER reaches here — regardless of `seeking` or whether the
  // live body is suppressed (review round-3 HIGH). A clean completed flow with a benign
  // `terminal_reason` shows that reason (below) but no capture block (it is not an error).
  if (!isError && !reason) {
    return <div className="px-3 py-3 text-xs italic text-text-muted" data-testid="error-empty">No error.</div>;
  }

  // The captured body is LIVE-detail only. While SEEKING a historical flow there is no live body, so
  // the unavailable reason is the HISTORICAL view (not "capture disabled") — labelled honestly so the
  // operator knows the body may exist live but is unavailable for this frozen cut.
  const captureUnavailableHistorical = seeking;
  return (
    <div className="px-3 py-2 text-xs" data-testid="error-tab">
      {reason && (
        <div className="mb-1">
          <span className="text-text-muted">terminal reason: </span>
          <span className="font-mono text-status-down">{reason}</span>
        </div>
      )}
      {joinError && (
        <div className="mb-1">
          <span className="text-text-muted">monitor error: </span>
          <span className="font-mono text-status-down">{joinError}</span>
        </div>
      )}
      {/* The upstream error-body capture block renders for a GENUINE error/failure ONLY (a benign
          completed `terminal_reason` shows above but warrants no error body). It ALWAYS renders for an
          error flow — captured (`measured`), the historical-view `unavailable` state while seeking, or
          the live capture-disabled `unavailable` state — NEVER omitted into a blank "no error". */}
      {isError && (
        <div className="mt-1 border-t border-line/60 pt-1.5" data-testid="error-capture" data-state={captured.state} data-quality={captured.quality}>
          <div className="mb-1 flex items-center gap-1.5">
            <span className="text-[10px] uppercase tracking-wide text-text-muted">upstream error body</span>
            {captured.state === 'captured' && captured.truncated && (
              <span className="rounded-sm bg-status-cooling/15 px-1 text-[9px] uppercase text-status-cooling" data-testid="error-capture-truncated" title="truncated by the capture cap — a prefix of the full body">
                truncated
              </span>
            )}
          </div>
          {captured.state === 'captured' ? (
            // The captured upstream error body — shown to the operator (an auth-gated DIAGNOSTIC
            // surface; showing the upstream error body is the WHOLE POINT of capturing it).
            <pre className="max-h-32 overflow-auto whitespace-pre-wrap break-words rounded-sm bg-bg/60 p-1.5 font-mono text-[10px] text-text" data-testid="error-capture-body" title={captured.detail}>
              {fmtCapturedBody(captured.body)}
            </pre>
          ) : captureUnavailableHistorical ? (
            // SEEKING a historical flow: the body is LIVE-only, so it is UNAVAILABLE for this frozen cut
            // (NOT "capture disabled" — it may exist live). Explicit, never a blank implying "no error".
            <div className="rounded-sm border border-dashed border-line/70 px-1.5 py-1 text-[10px] italic text-text-muted" data-testid="error-capture-historical" title="the captured upstream error body is live-detail only and is unavailable on a historical (seeked) view — not a claim that no error occurred">
              capture unavailable on historical view — the upstream error body is live-only. Not "no error".
            </div>
          ) : (
            // LIVE error flow, capture DISABLED / no body / evicted — explicit (NOT a blank).
            <div className="rounded-sm border border-dashed border-line/70 px-1.5 py-1 text-[10px] italic text-text-muted" data-testid="error-capture-disabled" title={captured.detail}>
              capture disabled — no upstream error body captured (set LLMCONDUIT_DASHBOARD_CAPTURE_UPSTREAM_RESPONSE=1). Not "no error".
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/** Render a captured upstream error body (JSON value or string) as readable text for the tab. */
function fmtCapturedBody(body: unknown): string {
  if (typeof body === 'string') return body.length > 0 ? body : '(empty body)';
  try {
    return JSON.stringify(body, null, 2);
  } catch {
    return String(body);
  }
}
