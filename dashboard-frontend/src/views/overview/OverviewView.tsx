/**
 * OverviewView (gap 16) — the CONTROL-ROOM overview ⭐. The single screen an operator watches DURING
 * an incident to answer: what's failing, which provider/client, how bad, how much it's costing, is the
 * context window about to overflow. It COMPOSES the surfaces built in gaps 01–15 into one HONEST
 * overview — it does NOT invent new data or re-fetch.
 *
 * The 5th hash route (`#/overview`, alongside flows/topology/sankey/theater). Lands LAST — only honest
 * once the spine (02–07/12) exists.
 *
 * WIRE-SOURCE CORRECTNESS (the recurring trap this program hit — each datum read from the source that
 * actually carries it):
 *  - the per-provider LATENCY/ERROR tiles read the gap-12 `ProviderLatency` from the REST `/topology`
 *    (live) / `/snapshot`-node (seek) path via gap-13's `useTopologyQuery().perProviderById ??
 *    storeNode.per_provider` — NEVER the live WS `topology_update` frame (which carries `per_provider`
 *    ABSENT). Deriving provider latency from visible flow summaries would HIDE failed primaries
 *    (spec 16) — so it is NOT done here.
 *  - the volume/cost/failure/client/context/token-mix tiles read the FLOW-LIST population
 *    (`useFlowRows(filters)` — `/flows` + `/snapshot` + the live store union), the source that
 *    carries per-flow `cost`/`cost_confidence`/`usage`/`client_label` — NOT the live `flow_status`
 *    frame (which omits client attribution + the roll-up fields).
 *  - the headline + cost-over-time read the live `metrics` store (the unified gap-01 WS+REST tile).
 *
 * DATA-QUALITY (cross-cutting, where it matters MOST — operators trust this during incidents): EVERY
 * tile/figure is tagged `measured`/`derived`/`estimated`/`unavailable`; an aggregate that mixes
 * confident + estimated inputs surfaces the LOWER confidence (the cost leaderboards inherit the
 * weakest tag); a value that can't be measured renders `—`, NEVER `0`; an empty window renders the
 * `—` empty-state, not an all-`0` dashboard. The honesty lives in the pure models; this view is a
 * thin composer that surfaces their tags.
 */
import { useCallback, useMemo, useRef } from 'react';
import { useDashboard, useFlowFilter } from '../../store/hooks';
import { useFlowRows } from '../../components/FlowTable/useFlowRows';
import { useCatalog } from '../../components/FlowTable/useCatalog';
import { useTopologyQuery } from '../../store/useTopologyQuery';
import { useMetricStream } from '../../store/useMetricStream';
import { getConnection, queryKeys } from '../../api/connection';
import { useQuery } from '@tanstack/react-query';
import type { MetricsResponse, ProviderLatency } from '../../api/types';
import { Panel } from '../../components/ui/Panel';
import { Sparkline } from '../../viz/Sparkline';
import { cn } from '../../lib/cn';
import {
  topByVolume,
  topByCost,
  tokenMix,
  fmtMixShare,
  type Leaderboard,
  type LeaderboardRow,
  type TokenMix,
} from './overviewModel';
import { fmtCost, fmtTokens } from '../../components/FlowTable/format';
import { fmtTokenRate } from '../../components/StatsStrip/chips';
import { buildProviderLatency } from '../../components/viz/providerLatency';
import { ProviderLatencyTile } from '../../components/viz/ProviderLatencyTile';
import { failureTaxonomy, type FailureTaxonomy as FailureTaxonomyModel } from '../../components/FlowTable/failureTaxonomy';
import { clientRollup, fmtLatency, type ClientRollup as ClientRollupModel, type ClientRollupRow } from '../../components/FlowTable/clientAttribution';
import { aggregateContextPressure, type ContextPressureAggregate } from '../../components/FlowTable/contextUtilization';

/** The unavailable / no-data marker (a value that cannot be measured renders this, never `0`). */
const DASH = '—';

/** Error-rate threshold (%) above which a rate reads red (mirrors the stats strip / failure panel). */
const ERROR_RATE_THRESHOLD = 5;

export function OverviewView() {
  // FLOW-LIST source (the wire source that carries cost/usage/client). Filters apply so a topology/
  // sankey cross-link or a client filter re-scopes the WHOLE control room consistently with the table.
  const filters = useFlowFilter((s) => s.filters);
  const { rows } = useFlowRows(filters);
  const limits = useCatalog();
  const seeking = useDashboard((s) => s.connection === 'seeking');

  // PER-PROVIDER source (gap 12/13): REST `/topology` live, frozen snapshot node while seeking — NEVER
  // the live WS frame. Mirrors TopologyView's resolution exactly.
  const { perProviderById } = useTopologyQuery();
  const nodes = useDashboard((s) => s.topologyNodes);
  const perProviderByNode = useMemo<{ id: string; per: ProviderLatency }[]>(() => {
    const out: { id: string; per: ProviderLatency }[] = [];
    for (const n of nodes) {
      const per = perProviderById[n.id] ?? n.per_provider;
      if (per) out.push({ id: n.id, per });
    }
    // Worst (most-degraded) provider first: by error rate desc, then p99 desc.
    out.sort((a, b) => b.per.error_rate - a.per.error_rate || b.per.p99 - a.per.p99);
    return out;
  }, [nodes, perProviderById]);

  // HEADLINE source (gap 01): the unified live metrics tile (WS+REST). Seeded by `/metrics`; the live
  // store `metrics` supersedes it. While seeking it IS the frozen cut (read as-of the seeked moment).
  const { client } = getConnection();
  const metricsQuery = useQuery({ queryKey: queryKeys.metrics, queryFn: () => client.metrics() });
  const storeMetrics = useDashboard((s) => s.metrics);
  const metrics = storeMetrics ?? metricsQuery.data ?? null;

  // Cost-over-time trend (gap 01 $/min) — fold each distinct live tick's headline $/min into a ring.
  // `useMetricStream` dedups by `metrics_seq` + skips the frozen seek cut (LIVE-only trend, like the
  // stats strip). Seeded from the `/metrics` query so the spark has shape before the first tick.
  // DON'T-LIE-WITH-ZEROS (review HIGH 2): a `priced_samples === 0` tick is UNMEASURABLE $/min — push
  // `null` (a GAP uPlot breaks across AND excludes from the y-scale — only `null`, NOT `NaN`, is a
  // uPlot gap sentinel) rather than a fabricated `0` that would plot a flat zero-cost trend while the
  // headline correctly reads `—`. The latest tick's cost quality tags the trend so an estimated/
  // unavailable trend is labelled, never shown as a confident line.
  const costRingRef = useRef<(number | null)[]>([]);
  const trendQualityRef = useRef<'measured' | 'derived' | 'estimated' | 'unavailable'>('unavailable');
  const foldCost = useCallback((sample: MetricsResponse) => {
    const priced = sample.priced_samples > 0;
    const point = priced ? sample.cost_per_min : null; // null ⇒ a uPlot gap, never a fabricated 0
    const next = [...costRingRef.current, point];
    costRingRef.current = next.length > 60 ? next.slice(next.length - 60) : next;
    trendQualityRef.current = !priced || sample.cost_confidence === 'unavailable'
      ? 'unavailable'
      : sample.cost_confidence === 'confident' ? 'derived' : 'estimated';
  }, []);
  const { version } = useMetricStream(foldCost, metricsQuery.data);
  void version; // read so the body re-runs after each ring fold (the ref mutation is invisible to React)

  // The composed pure models (each over the FLOW-LIST rows — the correct wire source).
  const volumeByModel = useMemo(() => topByVolume(rows, 'model'), [rows]);
  const costByModel = useMemo(() => topByCost(rows, 'model'), [rows]);
  const volumeByProvider = useMemo(() => topByVolume(rows, 'provider'), [rows]);
  // Review HIGH 1: top providers by COST too (the spec requires top models/providers by volume AND
  // cost) — SAME weakest-confidence fold + unpriced ⇒ — rules as the model-cost board.
  const costByProvider = useMemo(() => topByCost(rows, 'provider'), [rows]);
  const failures = useMemo(() => failureTaxonomy(rows), [rows]);
  const clients = useMemo(() => clientRollup(rows), [rows]);
  const pressure = useMemo(() => aggregateContextPressure(rows, limits), [rows, limits]);
  const mix = useMemo(() => tokenMix(rows), [rows]);

  return (
    <div className="min-h-0 min-w-0 flex-1 overflow-auto p-4" data-testid="overview-view">
      <div className="mb-3 flex items-baseline gap-2">
        <h1 className="text-sm font-semibold uppercase tracking-[0.18em] text-text">control room</h1>
        <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">
          what's failing · which provider/client · how bad · cost · context
        </span>
        {seeking && (
          <span className="text-[10px] text-status-cooling" title="as-of the seeked moment (frozen cut)" data-testid="overview-frozen">
            · frozen cut
          </span>
        )}
      </div>

      {/* Row 1 — the HONEST headline (gap 01 metrics tile) + the cost-over-time trend (gap 01 $/min). */}
      <HeadlineStrip metrics={metrics} costSeries={costRingRef.current} trendQuality={trendQualityRef.current} />

      <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-2 2xl:grid-cols-3">
        {/* Per-provider latency/error (gap 12/13 — REST/snapshot DTO, NOT the WS frame). */}
        <ProviderTiles providers={perProviderByNode} />
        {/* Top models by volume (gap 16 roll-up over FlowRows — volume measured, cost weakest-tag). */}
        <LeaderboardTile
          testId="overview-top-models-volume"
          title="top models · volume"
          board={volumeByModel}
          mode="volume"
        />
        {/* Top models by cost (only priced groups; empty-state — when none priced). */}
        <LeaderboardTile
          testId="overview-top-models-cost"
          title="top models · cost"
          board={costByModel}
          mode="cost"
        />
        {/* Top providers by volume. */}
        <LeaderboardTile
          testId="overview-top-providers-volume"
          title="top providers · volume"
          board={volumeByProvider}
          mode="volume"
        />
        {/* Top providers by cost (review HIGH 1 — same weakest-tag/unpriced rules as the model board). */}
        <LeaderboardTile
          testId="overview-top-providers-cost"
          title="top providers · cost"
          board={costByProvider}
          mode="cost"
        />
        {/* Failure taxonomy (gap 14 — the SAME failureTaxonomy(rows) model). */}
        <FailureTile model={failures} />
        {/* Top clients (gap 15 — the SAME clientRollup(rows) model). */}
        <ClientTile model={clients} />
        {/* Context-window pressure (gap 09 — the SAME aggregateContextPressure model). */}
        <ContextTile agg={pressure} />
        {/* Token-mix (gap 16 roll-up — measured classes; unreported optional ⇒ —). */}
        <TokenMixTile mix={mix} />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Headline strip — echoes the gap-01 honest metrics tile + the cost-over-time trend.
// ---------------------------------------------------------------------------

/** One headline metric cell: a `tabular-nums` value + its DQ tag. `—` when unmeasurable (never 0). */
function HeadlineCell({
  testId,
  label,
  value,
  quality,
  accent,
}: {
  testId: string;
  label: string;
  value: string;
  quality: 'measured' | 'derived' | 'estimated' | 'unavailable';
  accent?: string;
}) {
  return (
    <div
      className="flex flex-col gap-0.5 border-l border-line/50 px-3 first:border-l-0"
      data-testid={testId}
      data-quality={quality}
      title={`${label}: ${quality}`}
    >
      <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">{label}</span>
      <span className={cn('font-mono text-lg font-semibold tabular-nums', quality === 'unavailable' ? 'text-text-muted' : accent ?? 'text-text')}>
        {value}
      </span>
    </div>
  );
}

/**
 * The headline strip — the honest gap-01 metrics (active streams / prefill + decode tok/s /
 * $/min / err% / samples) +
 * a cost-over-time sparkline. Every figure respects don't-lie-with-zeros: a window whose denominator
 * is `0` renders `—` (the `samples`/phase-sample/`priced_samples` gates), never a fabricated `0`.
 * Reads the headline (`m1`) window fields directly off the unified live tile.
 */
function HeadlineStrip({
  metrics,
  costSeries,
  trendQuality,
}: {
  metrics: MetricsResponse | null;
  costSeries: (number | null)[];
  trendQuality: 'measured' | 'derived' | 'estimated' | 'unavailable';
}) {
  // No tick yet ⇒ the whole strip is unavailable (`—`), never an all-`0` headline.
  const has = metrics !== null;
  const samples = metrics?.samples ?? 0;
  const prefillSamples = metrics?.prefill_samples ?? 0;
  const decodeSamples = metrics?.decode_samples ?? 0;
  const pricedSamples = metrics?.priced_samples ?? 0;

  // Latency/err% need a finalized flow (`samples`); each phase speed needs its own measured
  // duration+usage sample; $/min needs a priced one.
  // active_streams + req/s are never sample-gated (a genuine idle `0` is honest).
  const activeStreams = has ? String(metrics!.active_streams) : DASH;
  const reqs = has ? metrics!.reqs_per_sec.toFixed(1) : DASH;
  const errPct = has && samples > 0 ? `${metrics!.error_pct.toFixed(1)}%` : DASH;
  const prefillTokS = has && prefillSamples > 0 ? fmtTokenRate(metrics!.prefill_tokens_per_sec) : DASH;
  const decodeTokS = has && decodeSamples > 0 ? fmtTokenRate(metrics!.decode_tokens_per_sec) : DASH;
  const costPerMin = has && pricedSamples > 0 ? `$${metrics!.cost_per_min.toFixed(2)}` : DASH;
  // $/min DQ tag: the AGGREGATE cost_confidence (confident ⇒ derived, estimated ⇒ estimated, else —).
  const costConfidence = metrics?.cost_confidence ?? 'unavailable';
  const costQuality = pricedSamples > 0 && costConfidence !== 'unavailable'
    ? (costConfidence === 'confident' ? 'derived' : 'estimated')
    : 'unavailable';
  const errOver = has && samples > 0 && metrics!.error_pct > ERROR_RATE_THRESHOLD;

  return (
    <Panel className="grid grid-cols-2 gap-1 px-2 py-2 sm:grid-cols-4 xl:grid-cols-[repeat(7,minmax(0,1fr))_auto]" data-testid="overview-headline">
      <HeadlineCell testId="overview-hl-active" label="active streams" value={activeStreams} quality={has ? 'measured' : 'unavailable'} />
      <HeadlineCell testId="overview-hl-reqs" label="req/s" value={reqs} quality={has ? 'measured' : 'unavailable'} accent="text-accent" />
      <HeadlineCell
        testId="overview-hl-err"
        label="err %"
        value={errPct}
        quality={errPct === DASH ? 'unavailable' : 'derived'}
        accent={errOver ? 'text-status-down' : 'text-text'}
      />
      <HeadlineCell testId="overview-hl-prefill" label="prefill tok/s" value={prefillTokS} quality={prefillTokS === DASH ? 'unavailable' : 'derived'} accent="text-status-healthy" />
      <HeadlineCell testId="overview-hl-decode" label="decode tok/s" value={decodeTokS} quality={decodeTokS === DASH ? 'unavailable' : 'derived'} accent="text-status-cooling" />
      <HeadlineCell testId="overview-hl-cost" label="$/min" value={costPerMin} quality={costQuality} accent="text-meta" />
      <HeadlineCell testId="overview-hl-samples" label="samples" value={has ? String(samples) : DASH} quality={has ? 'measured' : 'unavailable'} />
      {/* Cost-over-time trend — LIVE-only $/min history (mirrors the strip sparkline discipline). An
          unpriced tick is a GAP (NaN) — the line breaks rather than dropping to a fabricated 0; the
          trend is tagged with the latest tick's cost quality (estimated/unavailable labelled). */}
      <div
        className="col-span-2 flex min-w-0 flex-col justify-center border-t border-border px-2 pt-2 sm:col-span-4 xl:col-span-1 xl:border-l xl:border-t-0 xl:pt-0"
        data-testid="overview-cost-trend"
        data-quality={trendQuality}
        title={`cost-over-time ($/min, live trend) — ${trendQuality}`}
      >
        <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">$/min trend</span>
        <Sparkline data={costSeries} label="cost per minute trend" />
      </div>
    </Panel>
  );
}

// ---------------------------------------------------------------------------
// Per-provider latency/error tiles (gap 12/13 — the REST/snapshot DTO).
// ---------------------------------------------------------------------------

/**
 * The per-provider latency/error tiles. CONSUMES the gap-12 per-provider DTO (resolved by the parent
 * from the REST/snapshot topology path, NEVER the WS frame) via gap-13's `buildProviderLatency` +
 * `ProviderLatencyTile`. A present tile is `derived` (real percentiles) + a `measured` error rate; a
 * window with NO per-provider samples renders an explicit `unavailable` state (`—`), never a
 * fabricated `0ms`/`0%`.
 */
function ProviderTiles({ providers }: { providers: { id: string; per: ProviderLatency }[] }) {
  const available = providers.length > 0;
  return (
    <Panel className="flex flex-col gap-2 p-3" data-testid="overview-providers" data-available={available ? 'true' : 'false'}>
      <div className="flex items-baseline justify-between">
        <span className="text-xs font-semibold uppercase tracking-[0.14em] text-text-muted">providers · latency / error</span>
        <span className="text-[9px] uppercase tracking-wide text-text-muted" title="per-provider metrics from the REST /topology + /snapshot node (the live WS frame carries them absent)">
          rest topology
        </span>
      </div>
      {!available ? (
        <p className="px-1 py-2 text-xs italic text-text-muted" data-testid="overview-providers-unavailable" data-quality="unavailable">
          No per-provider samples in this window — provider latency unavailable ({DASH}), not 0.
        </p>
      ) : (
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          {providers.map(({ id, per }) => (
            <div key={id} className="rounded-md border border-line/60 bg-panel-raised px-2 pb-1.5 pt-1" data-testid="overview-provider" data-provider={id}>
              <div className="truncate font-mono text-xs text-text" title={id}>{id}</div>
              <ProviderLatencyTile model={buildProviderLatency(per, id)} />
            </div>
          ))}
        </div>
      )}
    </Panel>
  );
}

// ---------------------------------------------------------------------------
// Leaderboard tiles (top models / providers by volume or cost).
// ---------------------------------------------------------------------------

/** Quality → the small DQ badge color (mirrors the inspector instrument badges). */
const QUALITY_DOT: Record<string, string> = {
  measured: 'text-status-healthy',
  derived: 'text-accent',
  estimated: 'text-status-cooling',
  unavailable: 'text-text-muted',
};

/**
 * A "top models/providers" leaderboard tile. Volume rows show the measured flow count + the cost
 * (weakest-tag, `—` when unpriced); cost rows are ordered by cost. The empty-state renders an
 * explicit `unavailable` `—` (never an all-`0` board). `estimated` costs are LABELLED (an `est`
 * badge), so a mixed-confidence total is never shown as confident.
 */
function LeaderboardTile({
  testId,
  title,
  board,
  mode,
}: {
  testId: string;
  title: string;
  board: Leaderboard;
  mode: 'volume' | 'cost';
}) {
  return (
    <Panel className="flex flex-col gap-2 p-3" data-testid={testId} data-available={board.available ? 'true' : 'false'}>
      <div className="flex items-baseline justify-between">
        <span className="text-xs font-semibold uppercase tracking-[0.14em] text-text-muted">{title}</span>
        {board.available && board.groupCount > board.rows.length && (
          <span className="text-[9px] tabular-nums text-text-muted">+{board.groupCount - board.rows.length} more</span>
        )}
      </div>
      {!board.available ? (
        <p className="px-1 py-2 text-xs italic text-text-muted" data-testid={`${testId}-unavailable`} data-quality="unavailable">
          {mode === 'cost'
            ? `No priced flows in this window — cost unavailable (${DASH}), not $0.00.`
            : `No flows in this window — unavailable (${DASH}), not 0.`}
        </p>
      ) : (
        <ul className="flex flex-col gap-1" role="list" data-testid={`${testId}-list`}>
          {board.rows.map((row, i) => (
            <LeaderboardRowView key={row.key} row={row} rank={i + 1} />
          ))}
        </ul>
      )}
    </Panel>
  );
}

/** One leaderboard row — rank · label · volume (measured) · cost (weakest-tag, est-labelled, `—` unpriced). */
function LeaderboardRowView({ row, rank }: { row: LeaderboardRow; rank: number }) {
  const costText = row.cost === null ? DASH : fmtCost(row.cost);
  return (
    <li
      className="flex items-center justify-between gap-2 rounded-md border border-line/50 bg-panel-raised px-2 py-1"
      role="listitem"
      data-testid="overview-leaderboard-row"
      data-key={row.key}
    >
      <div className="flex min-w-0 items-baseline gap-1.5">
        <span className="text-[10px] tabular-nums text-text-muted">{rank}.</span>
        <span className="truncate font-mono text-xs text-text" title={row.label}>{row.label}</span>
      </div>
      <div className="flex shrink-0 items-baseline gap-2">
        <span className="font-mono text-xs tabular-nums text-text" data-testid="overview-leaderboard-volume" data-quality="measured" title="flows observed (measured)">
          {row.volume}
        </span>
        <span
          className={cn('font-mono text-xs tabular-nums', row.costQuality === 'unavailable' ? 'text-text-muted' : 'text-meta')}
          data-testid="overview-leaderboard-cost"
          data-quality={row.costQuality}
          title={
            row.cost === null
              ? 'no priced flow in this group — cost unavailable (—), not $0.00'
              : `summed cost (${row.costConfidence})`
          }
        >
          {costText}
        </span>
        {row.costQuality === 'estimated' && (
          <span className="rounded-sm bg-status-cooling/15 px-1 text-[8px] uppercase tracking-wide text-status-cooling" data-testid="overview-leaderboard-est" title="estimated cost — a contributing flow is priced with an unconfigured cache rate (labelled, not silently confident)">
            est
          </span>
        )}
      </div>
    </li>
  );
}

// ---------------------------------------------------------------------------
// Failure tile (gap 14 failureTaxonomy model).
// ---------------------------------------------------------------------------

/**
 * The failure-taxonomy tile — the overall error-rate chip + the top failing groups, from the SAME
 * gap-14 `failureTaxonomy(rows)` model. A zero-sample window is an EXPLICIT `unavailable` `—` (NOT a
 * blank, NOT `0%`); an observed all-success window reads a measured-base `derived 0%`.
 */
function FailureTile({ model }: { model: FailureTaxonomyModel }) {
  const over = model.overallQuality === 'derived' && model.overallErrorRatePct > ERROR_RATE_THRESHOLD;
  return (
    <Panel className="flex flex-col gap-2 p-3" data-testid="overview-failures" data-available={model.available ? 'true' : 'false'}>
      <div className="flex items-baseline justify-between">
        <span className="text-xs font-semibold uppercase tracking-[0.14em] text-text-muted">failures · what & why</span>
        <span
          className="flex items-baseline gap-1"
          data-testid="overview-failures-rate"
          data-quality={model.overallQuality}
          title={model.overallQuality === 'unavailable' ? 'error rate unavailable — no flows observed (—, not 0%)' : `overall error rate (derived): ${model.totalFailed}/${model.totalFlows}`}
        >
          <span className="text-[9px] uppercase tracking-wide text-text-muted">err</span>
          <span className={cn('font-mono text-sm font-semibold tabular-nums', over ? 'text-status-down' : 'text-text')}>
            {model.overallErrorRateText}
          </span>
        </span>
      </div>
      {!model.available ? (
        <p className="px-1 py-2 text-xs italic text-text-muted" data-testid="overview-failures-unavailable" data-quality="unavailable">
          No flows observed — error rate unavailable ({DASH}), not 0%.
        </p>
      ) : model.groups.length === 0 ? (
        <p className="px-1 py-2 text-xs italic text-text-muted" data-testid="overview-failures-none">
          No failures in this window. ({model.totalFlows} observed)
        </p>
      ) : (
        <ul className="flex flex-col gap-1" role="list" data-testid="overview-failures-list">
          {model.groups.slice(0, 5).map((g) => (
            <li
              key={g.key}
              className="flex items-center justify-between gap-2 rounded-md border border-line/50 bg-panel-raised px-2 py-1"
              role="listitem"
              data-testid="overview-failure-group"
              data-group-key={g.key}
            >
              <div className="flex min-w-0 items-baseline gap-1.5">
                <span className="truncate font-mono text-xs text-text" title={`${g.provider} · ${g.model}`}>{g.provider}</span>
                <span className="text-line">·</span>
                <span className="truncate font-mono text-[11px] text-text-muted">{g.model}</span>
              </div>
              <div className="flex shrink-0 items-baseline gap-1.5">
                <span className="font-mono text-xs font-semibold tabular-nums text-status-down" data-testid="overview-failure-rate" data-quality="derived" title="error rate for this group (derived: failed/observed)">
                  {g.errorRateText}
                </span>
                <span className="text-[10px] tabular-nums text-text-muted">{g.failed}/{g.total}</span>
              </div>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}

// ---------------------------------------------------------------------------
// Client tile (gap 15 clientRollup model).
// ---------------------------------------------------------------------------

/**
 * The hover title for a client's summed cost — built from the roll-up's FOLDED `costConfidence`/
 * `costQuality` (review round-2 MEDIUM), NOT a hardcoded "measured": an `estimated` client spend (a
 * contributing flow priced via an unconfigured cache rate) is labelled an estimate, an unpriced
 * client reads unavailable, a confident-only client reads derived — consistently with the rest of the
 * overview's cost surfaces.
 */
function clientCostTitle(row: ClientRollupRow): string {
  if (row.cost === null || row.costQuality === 'unavailable') return 'no priced flow — cost unavailable (—)';
  if (row.costQuality === 'estimated') return 'summed cost (estimated — a contributing flow is priced via an unconfigured cache rate / unreported tokens)';
  return 'summed cost (derived — every contributing flow is a confident price)';
}

/**
 * The top-clients tile — cost/errors/latency BY non-secret client, from the SAME gap-15
 * `clientRollup(rows)` model. The WEAK User-Agent fallback is rendered visibly weaker (a `ua` badge,
 * `derived`); an unattributed flow is NEVER a fabricated client (it bumps the explicit unattributed
 * count). A window with NO attributed flow is an explicit `unavailable` `—`. The shown label is the
 * gap-04 one-way `key-<hex>` HASH (the auth-gated diagnostic purpose) — NEVER a raw key.
 */
function ClientTile({ model }: { model: ClientRollupModel }) {
  return (
    <Panel className="flex flex-col gap-2 p-3" data-testid="overview-clients" data-available={model.available ? 'true' : 'false'}>
      <div className="flex items-baseline justify-between">
        <span className="text-xs font-semibold uppercase tracking-[0.14em] text-text-muted">top clients · cost / err</span>
        {model.unattributedFlows > 0 && (
          <span className="text-[9px] tabular-nums text-text-muted" title="flows with no key / configured-id / UA (rendered — , not a fabricated client)">
            {model.unattributedFlows} unattributed
          </span>
        )}
      </div>
      {!model.available ? (
        <p className="px-1 py-2 text-xs italic text-text-muted" data-testid="overview-clients-unavailable" data-quality="unavailable">
          No attributed clients in this window — unavailable ({DASH}), not 0.
        </p>
      ) : (
        <ul className="flex flex-col gap-1" role="list" data-testid="overview-clients-list">
          {model.rows.slice(0, 5).map((row) => {
            const errOver = row.errorRatePct > ERROR_RATE_THRESHOLD;
            return (
              <li
                key={row.key}
                className="flex items-center justify-between gap-2 rounded-md border border-line/50 bg-panel-raised px-2 py-1"
                role="listitem"
                data-testid="overview-client-row"
                data-client={row.key}
                data-strength={row.strength}
              >
                <div className="flex min-w-0 items-center gap-1">
                  <span className={cn('truncate font-mono text-xs', row.weak ? 'italic text-text-muted' : 'text-text')} title={row.label}>{row.label}</span>
                  {row.weak && (
                    <span className="shrink-0 rounded-sm bg-status-cooling/15 px-1 text-[8px] uppercase tracking-wide text-status-cooling" data-testid="overview-client-ua" title="weak user-agent fallback — not a confirmed identity">
                      ua
                    </span>
                  )}
                </div>
                <div className="flex shrink-0 items-baseline gap-2">
                  <span className="font-mono text-[11px] tabular-nums" data-testid="overview-client-cost" data-quality={row.costQuality} title={clientCostTitle(row)}>
                    <span className={row.costQuality === 'unavailable' ? 'text-text-muted' : 'text-meta'}>{row.cost === null ? DASH : fmtCost(row.cost)}</span>
                  </span>
                  <span className={cn('font-mono text-[11px] tabular-nums', errOver ? 'text-status-down' : 'text-text-muted')} data-testid="overview-client-err" data-quality="derived" title="error rate (derived: failed/observed)">
                    {row.errorRateText}
                  </span>
                  <span className="font-mono text-[10px] tabular-nums text-text-muted" data-testid="overview-client-latency" data-quality={row.latencyQuality} title="mean latency (derived; — when none timed)">
                    {fmtLatency(row.avgLatencyMs)}
                  </span>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </Panel>
  );
}

// ---------------------------------------------------------------------------
// Context-pressure tile (gap 09 aggregateContextPressure model).
// ---------------------------------------------------------------------------

/** Risk band → the peak-percent accent (mirrors the gauge tokens). */
const RISK_TEXT: Record<string, string> = {
  ok: 'text-status-healthy',
  near: 'text-status-cooling',
  over: 'text-status-down',
};

/**
 * The context-window-pressure tile — the PEAK utilization + near/over counts across the filtered
 * flows, from the SAME gap-09 `aggregateContextPressure` model. A window with NO measurable flow
 * renders `—` (unavailable), never a fabricated `0%` peak; the measured/total coverage is shown.
 */
function ContextTile({ agg }: { agg: ContextPressureAggregate }) {
  const measurable = agg.measuredFlows > 0;
  const risk = agg.peakRisk === 'none' ? 'ok' : agg.peakRisk;
  return (
    <Panel className="flex flex-col gap-2 p-3" data-testid="overview-context" data-available={measurable ? 'true' : 'false'}>
      <div className="flex items-baseline justify-between">
        <span className="text-xs font-semibold uppercase tracking-[0.14em] text-text-muted">context pressure</span>
        <span className="font-mono text-[9px] tabular-nums text-text-muted" data-testid="overview-context-coverage" title="how much of the set is measurable (known limit + reported prompt usage)">
          {agg.measuredFlows}/{agg.totalFlows} measured
        </span>
      </div>
      <div className="flex items-end justify-between gap-3 px-1">
        <div className="flex flex-col">
          <span className="text-[10px] uppercase tracking-wide text-text-muted">peak util</span>
          <span
            className={cn('font-mono text-2xl font-semibold tabular-nums', measurable ? RISK_TEXT[risk] : 'text-text-muted')}
            data-testid="overview-context-peak"
            data-quality={measurable ? 'derived' : 'unavailable'}
            data-risk={agg.peakRisk}
            title={measurable ? 'peak context-window utilization across the flows (derived)' : 'no measurable flow — peak unavailable (—), not 0%'}
          >
            {agg.peakLabel}
          </span>
        </div>
        <div className="flex flex-col items-end gap-0.5">
          <span className="text-[10px] uppercase tracking-wide text-text-muted">near / over</span>
          {/* Review HIGH 4: with NO measurable flow the near/over counts are UNMEASURED — render
              `— / —` (unavailable), NOT `0 / 0`, which would read as a measured zero-risk. A genuine
              measured `0 / 0` (≥1 measurable flow, none near/over) stays a real measured `0 / 0`. */}
          <span
            className="font-mono text-sm tabular-nums"
            data-testid="overview-context-nearover"
            data-quality={measurable ? 'derived' : 'unavailable'}
          >
            {measurable ? (
              <>
                <span className="text-status-cooling" data-testid="overview-context-near">{agg.nearCount}</span>
                <span className="text-line"> / </span>
                <span className="text-status-down" data-testid="overview-context-over">{agg.overCount}</span>
              </>
            ) : (
              <span className="text-text-muted" data-testid="overview-context-near">{DASH} / {DASH}</span>
            )}
          </span>
        </div>
      </div>
    </Panel>
  );
}

// ---------------------------------------------------------------------------
// Token-mix tile (gap 16 tokenMix model).
// ---------------------------------------------------------------------------

/** Token class → bar color token. */
const MIX_COLOR: Record<string, string> = {
  prompt: 'bg-accent',
  completion: 'bg-status-healthy',
  cached: 'bg-meta',
  reasoning: 'bg-status-cooling',
};

/**
 * The token-mix tile — the prompt/completion/cached/reasoning split across usage-bearing flows. The
 * REQUIRED prompt/completion classes are `measured` (a real `0` is honest); an UNREPORTED optional
 * class (cached/reasoning absent across the window) is `unavailable` (`—`), never a fabricated `0`. A
 * window with no usage-bearing flow renders an explicit `unavailable` empty-state.
 */
function TokenMixTile({ mix }: { mix: TokenMix }) {
  return (
    <Panel className="flex flex-col gap-2 p-3" data-testid="overview-token-mix" data-available={mix.available ? 'true' : 'false'}>
      <div className="flex items-baseline justify-between">
        <span className="text-xs font-semibold uppercase tracking-[0.14em] text-text-muted">token mix</span>
        {mix.available && (
          <span className="font-mono text-[9px] tabular-nums text-text-muted" title="usage-bearing flows in the window (the measurability base)">
            {mix.usageFlows} usage flows
          </span>
        )}
      </div>
      {!mix.available ? (
        <p className="px-1 py-2 text-xs italic text-text-muted" data-testid="overview-token-mix-unavailable" data-quality="unavailable">
          No usage-bearing flows in this window — token mix unavailable ({DASH}), not 0.
        </p>
      ) : (
        <>
          {/* The composite share bar — EXCLUSIVE segments (cached carved out of prompt, reasoning out
              of completion) so the stack sums to ≤100%, never the >100% a raw cached+reasoning stack
              would give (cached ⊆ prompt, reasoning ⊆ completion). */}
          <div className="flex h-2 w-full overflow-hidden rounded-full bg-line/40" data-testid="overview-token-mix-bar" aria-hidden>
            {mix.barSegments.map((seg) => (
              <div key={seg.key} className={cn('h-full', MIX_COLOR[seg.colorKey])} style={{ width: `${Math.min(100, seg.fraction * 100)}%` }} />
            ))}
          </div>
          <dl className="grid grid-cols-2 gap-x-3 gap-y-0.5">
            {mix.classes.map((c) => (
              <div className="contents" key={c.key}>
                <dt className="flex items-center gap-1 text-[11px] text-text-muted">
                  <span className={cn('inline-block h-2 w-2 rounded-sm', MIX_COLOR[c.key])} aria-hidden />
                  {c.label}
                </dt>
                <dd
                  className={cn('flex items-baseline justify-end gap-1 text-right tabular-nums text-[11px]', c.quality === 'unavailable' ? 'text-text-muted' : 'text-text')}
                  data-testid={`overview-token-${c.key}`}
                  data-quality={c.quality}
                  title={c.quality === 'unavailable' ? `${c.label} unreported in this window — unavailable (—), not 0` : `${c.label} (measured)`}
                >
                  <span>{c.tokens === null ? DASH : fmtTokens(c.tokens)}</span>
                  <span className={cn('text-[9px]', QUALITY_DOT[c.quality])}>{fmtMixShare(c.fraction)}</span>
                </dd>
              </div>
            ))}
          </dl>
        </>
      )}
    </Panel>
  );
}
