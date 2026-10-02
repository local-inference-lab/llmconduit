/**
 * ThroughputView — requests, prefill and decode throughput, and TTFT per model and combined.
 *
 * Top: the GATEWAY-derived series (persisted requests; every backend). Bottom: the engines'
 * own counters where a `/metrics` endpoint was scraped (vLLM/SGLang), per backend × model —
 * running/waiting, KV usage, prefix-cache hit rate, prompt/generation tok/s. The two are never
 * mixed: each tile says which it is.
 */
import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import { Panel } from '../../components/ui/Panel';
import { ChartLegend, LineChart, type ChartSeries } from '../../viz/LineChart';
import { cn } from '../../lib/cn';
import { fmtTokens } from '../../components/FlowTable/format';
import {
  ALL_MODELS,
  engineRateSeries,
  engineStates,
  modelsByVolume,
  throughputSeries,
  throughputTotals,
} from './throughputModel';

const DASH = '—';
const WINDOWS: Array<{ label: string; ms: number; bucketSecs: number }> = [
  { label: '1h', ms: 60 * 60_000, bucketSecs: 60 },
  { label: '6h', ms: 6 * 60 * 60_000, bucketSecs: 300 },
  { label: '24h', ms: 24 * 60 * 60_000, bucketSecs: 900 },
];

function fmtRate(v: number | null, digits = 0): string {
  return v == null || !Number.isFinite(v) ? DASH : v >= 1000 ? `${(v / 1000).toFixed(1)}k` : v.toFixed(digits);
}

export function ThroughputView() {
  const { client } = getConnection();
  const [windowIdx, setWindowIdx] = useState(0);
  const [model, setModel] = useState<string>(ALL_MODELS);
  const win = WINDOWS[windowIdx]!;
  const sinceMs = Date.now() - win.ms;
  const throughput = useQuery({
    queryKey: [...queryKeys.throughput, win.label],
    queryFn: () => client.historyThroughput({ since_ms: sinceMs, bucket_secs: win.bucketSecs }),
    refetchInterval: 15_000,
  });
  const metrics = useQuery({
    queryKey: [...queryKeys.historyMetrics, win.label],
    queryFn: () => client.historyMetrics({ since_ms: sinceMs, limit: 5000 }),
    refetchInterval: 15_000,
  });
  const buckets = useMemo(() => throughput.data?.buckets ?? [], [throughput.data]);
  const bucketMs = throughput.data?.bucket_ms ?? win.bucketSecs * 1000;
  const models = useMemo(() => modelsByVolume(buckets), [buckets]);
  const points = useMemo(() => throughputSeries(buckets, model, bucketMs), [buckets, model, bucketMs]);
  const totals = useMemo(() => throughputTotals(points), [points]);
  const perModelSeries = useMemo(
    () => (model === ALL_MODELS ? models : [model]).slice(0, 6).map((m) => ({ m, points: throughputSeries(buckets, m, bucketMs) })),
    [buckets, models, model, bucketMs],
  );
  const chart = (pick: (p: (typeof points)[number]) => number | null, label: string): ChartSeries[] =>
    perModelSeries.map(({ m, points: ps }) => ({ key: `${label}-${m}`, label: m, points: ps.map((p) => [p.bucket_ms, pick(p)] as [number, number | null]) }));
  const samples = useMemo(() => metrics.data?.samples ?? [], [metrics.data]);
  const engines = useMemo(() => engineStates(samples), [samples]);

  return (
    <div className="min-h-0 min-w-0 flex-1 overflow-auto p-4" data-testid="throughput-view">
      <div className="mb-3 flex flex-wrap items-baseline gap-2">
        <h1 className="text-sm font-semibold uppercase tracking-[0.18em] text-text">throughput</h1>
        <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">requests · prefill · decode · ttft — per model and combined</span>
        <div className="ml-auto flex items-center gap-1" data-testid="throughput-window">
          {WINDOWS.map((w, i) => (
            <button
              key={w.label}
              type="button"
              onClick={() => setWindowIdx(i)}
              className={cn('rounded-full border px-2.5 py-0.5 text-xs', i === windowIdx ? 'border-accent/40 bg-accent/15 text-accent' : 'border-line bg-panel text-text-muted')}
            >
              {w.label}
            </button>
          ))}
        </div>
      </div>

      <div className="mb-3 flex flex-wrap items-center gap-1.5" data-testid="throughput-models">
        <span className="text-[10px] uppercase tracking-wide text-text-muted">model</span>
        <button type="button" onClick={() => setModel(ALL_MODELS)} className={cn('rounded-full border px-2.5 py-0.5 text-xs', model === ALL_MODELS ? 'border-accent/40 bg-accent/15 text-accent' : 'border-line bg-panel text-text-muted')}>
          all
        </button>
        {models.map((m) => (
          <button key={m} type="button" onClick={() => setModel(m)} className={cn('rounded-full border px-2.5 py-0.5 text-xs', model === m ? 'border-accent/40 bg-accent/15 text-accent' : 'border-line bg-panel text-text-muted')}>
            {m}
          </button>
        ))}
      </div>

      {throughput.isError && (
        <Panel className="mb-3 px-3 py-3 text-xs text-text-muted" data-testid="throughput-unavailable">
          {String(throughput.error).includes('503')
            ? 'Durable history is disabled: configure control_plane.storage (sqlite or postgres).'
            : `Could not load throughput: ${String(throughput.error)}`}
        </Panel>
      )}

      <Panel raised className="mb-3 flex flex-wrap gap-6 px-3 py-2" data-testid="throughput-headline" data-quality={totals.quality}>
        <Cell testId="tp-requests" label={`requests · ${win.label}`} value={totals.requests ? String(totals.requests) : DASH} quality={totals.requests ? 'measured' : 'unavailable'} />
        <Cell testId="tp-prefill" label="prefill tok/s · avg" value={fmtRate(totals.prefill_tps)} quality={totals.prefill_tps == null ? 'unavailable' : 'derived'} />
        <Cell testId="tp-decode" label="decode tok/s · avg" value={fmtRate(totals.decode_tps, 1)} quality={totals.decode_tps == null ? 'unavailable' : 'derived'} />
        <Cell testId="tp-ttft" label="ttft ms · avg" value={fmtRate(totals.ttft_ms)} quality={totals.ttft_ms == null ? 'unavailable' : 'derived'} />
        <span className="ml-auto self-center text-[10px] text-text-muted" title="from persisted requests (gateway-side); works for every backend">gateway · derived</span>
      </Panel>

      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        <ChartTile testId="chart-requests" title="requests / min" series={chart((p) => p.requests_per_min, 'rpm')} />
        <ChartTile testId="chart-prefill" title="prefill tok/s" series={chart((p) => p.prefill_tps, 'prefill')} />
        <ChartTile testId="chart-decode" title="decode tok/s" series={chart((p) => p.decode_tps, 'decode')} />
        <ChartTile testId="chart-ttft" title="ttft ms" series={chart((p) => p.ttft_ms, 'ttft')} />
      </div>

      <h2 className="mb-1 mt-4 text-[10px] uppercase tracking-[0.14em] text-text-muted">engines · scraped /metrics (vllm · sglang)</h2>
      {engines.length === 0 ? (
        <Panel className="px-3 py-3 text-xs text-text-muted" data-testid="engines-empty">
          No upstream metrics scraped yet. Backends need a Prometheus /metrics endpoint (vLLM, SGLang --enable-metrics) and control_plane.metrics enabled.
        </Panel>
      ) : (
        <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
          {engines.map((e) => (
            <Panel key={`${e.backend}/${e.model}`} className="px-3 py-2" data-testid="engine-tile" data-quality={e.quality}>
              <div className="mb-1 flex items-baseline gap-2">
                <span className="font-mono text-sm text-text">{e.backend}</span>
                <span className="text-xs text-text-muted">{e.model}</span>
                <span className="rounded-sm bg-accent/15 px-1 text-[9px] uppercase tracking-wide text-accent">{e.engine}</span>
                <span className="ml-auto text-[10px] text-text-muted" title="from the engine's own counters">measured</span>
              </div>
              <div className="flex flex-wrap gap-4">
                <Cell testId="engine-running" label="running · waiting" value={`${e.running ?? DASH} · ${e.waiting ?? DASH}`} quality={e.running == null ? 'unavailable' : 'measured'} />
                <Cell testId="engine-kv" label="kv usage" value={e.kv_usage == null ? DASH : `${(e.kv_usage * 100).toFixed(0)}%`} quality={e.kv_usage == null ? 'unavailable' : 'measured'} />
                <Cell testId="engine-hit" label="prefix hit" value={e.prefix_hit_rate == null ? DASH : `${(e.prefix_hit_rate * 100).toFixed(0)}%`} quality={e.prefix_hit_rate == null ? 'unavailable' : 'measured'} />
                <Cell testId="engine-prompt" label="prompt tok/s" value={fmtRate(e.prompt_tps)} quality={e.prompt_tps == null ? 'unavailable' : 'derived'} />
                <Cell testId="engine-gen" label="gen tok/s" value={fmtRate(e.generation_tps, 1)} quality={e.generation_tps == null ? 'unavailable' : 'derived'} />
                <Cell testId="engine-ttft" label="ttft ms · lifetime avg" value={fmtRate(e.ttft_ms)} quality={e.ttft_ms == null ? 'unavailable' : 'derived'} />
              </div>
              <div className="mt-2">
                <LineChart
                  height={110}
                  testId="engine-chart"
                  series={[
                    { key: 'prompt', label: 'prompt tok/s', points: engineRateSeries(samples, e.backend, e.model, 'prompt_tokens_total') },
                    { key: 'gen', label: 'generation tok/s', points: engineRateSeries(samples, e.backend, e.model, 'generation_tokens_total') },
                  ]}
                />
              </div>
            </Panel>
          ))}
        </div>
      )}
      <p className="mt-3 text-[10px] text-text-muted">
        tokens in window: in {fmtTokens(points.reduce((a, p) => a + p.input_tokens, 0))} · out {fmtTokens(points.reduce((a, p) => a + p.output_tokens, 0))} · cached {fmtTokens(points.reduce((a, p) => a + p.cached_tokens, 0))}
      </p>
    </div>
  );
}

function Cell({ testId, label, value, quality }: { testId: string; label: string; value: string; quality: 'measured' | 'derived' | 'unavailable' }) {
  return (
    <span className="flex flex-col" data-testid={testId} data-quality={quality} title={`${label}: ${quality}`}>
      <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">{label}</span>
      <span className={cn('font-mono text-lg font-semibold tabular-nums', quality === 'unavailable' ? 'text-text-muted' : 'text-text')}>{value}</span>
    </span>
  );
}

function ChartTile({ testId, title, series }: { testId: string; title: string; series: ChartSeries[] }) {
  return (
    <Panel className="px-3 py-2" data-testid={testId}>
      <div className="mb-1 flex items-baseline justify-between">
        <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">{title}</span>
        <ChartLegend series={series} />
      </div>
      <LineChart series={series} height={150} />
    </Panel>
  );
}
