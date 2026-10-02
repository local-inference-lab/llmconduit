/**
 * throughputModel — pure derivations for the Throughput view.
 *
 * Two sources, deliberately kept apart and labelled:
 *  - GATEWAY series (`/history/throughput`): per bucket × model × backend sums from persisted
 *    requests. Prefill tok/s = input tokens of requests with a first token ÷ their summed TTFT;
 *    decode tok/s = output tokens ÷ summed (completed − first token). Both are DERIVED from what
 *    the gateway itself measured, and available for every backend.
 *  - UPSTREAM samples (`/history/metrics` with `kind: "upstream"`): the engine's own counters
 *    (vLLM/SGLang). Rates are the delta between consecutive samples of the same backend+model
 *    divided by the elapsed time — MEASURED by the engine, but only where a `/metrics` endpoint
 *    exists.
 *
 * Every figure is tagged `derived` / `measured` / `unavailable`; a bucket or sample without the
 * needed inputs renders `—`, never 0.
 */
import type { ActivityBucket, MetricSample, ThroughputBucket, UpstreamMetricsSample } from '../../api/types';

export type Quality = 'measured' | 'derived' | 'unavailable';

export const ALL_MODELS = '*';

export interface ThroughputPoint {
  bucket_ms: number;
  requests: number;
  completed: number;
  /** requests per minute over the bucket length */
  requests_per_min: number;
  prefill_tps: number | null;
  decode_tps: number | null;
  ttft_ms: number | null;
  input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
}

/** Fold buckets (already per model×backend) into one series per model, or one combined series. */
export function throughputSeries(buckets: ThroughputBucket[], model: string, bucketMs: number): ThroughputPoint[] {
  const byBucket = new Map<number, ThroughputBucket[]>();
  for (const b of buckets) {
    if (model !== ALL_MODELS && b.model !== model) continue;
    const list = byBucket.get(b.bucket_ms) ?? [];
    list.push(b);
    byBucket.set(b.bucket_ms, list);
  }
  const minutes = Math.max(bucketMs, 1) / 60_000;
  return [...byBucket.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([bucket_ms, cells]) => {
      const sum = (pick: (c: ThroughputBucket) => number) => cells.reduce((acc, c) => acc + pick(c), 0);
      const ttftSum = sum((c) => c.ttft_ms_sum);
      const ttftCount = sum((c) => c.ttft_count);
      const prefillTokens = sum((c) => c.prefill_tokens);
      const decodeMs = sum((c) => c.decode_ms_sum);
      const decodeTokens = sum((c) => c.decode_tokens);
      return {
        bucket_ms,
        requests: sum((c) => c.requests),
        completed: sum((c) => c.completed),
        requests_per_min: sum((c) => c.requests) / minutes,
        prefill_tps: ttftSum > 0 ? (prefillTokens / ttftSum) * 1000 : null,
        decode_tps: decodeMs > 0 ? (decodeTokens / decodeMs) * 1000 : null,
        ttft_ms: ttftCount > 0 ? ttftSum / ttftCount : null,
        input_tokens: sum((c) => c.input_tokens),
        output_tokens: sum((c) => c.output_tokens),
        cached_tokens: sum((c) => c.cached_tokens),
      };
    });
}

/** Distinct models present in the buckets, by descending request volume. */
export function modelsByVolume(buckets: ThroughputBucket[]): string[] {
  const counts = new Map<string, number>();
  for (const b of buckets) counts.set(b.model, (counts.get(b.model) ?? 0) + b.requests);
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).map(([m]) => m);
}

/** Totals over a series for the headline cells. */
export function throughputTotals(points: ThroughputPoint[]): {
  requests: number;
  prefill_tps: number | null;
  decode_tps: number | null;
  ttft_ms: number | null;
  quality: Quality;
} {
  if (points.length === 0) return { requests: 0, prefill_tps: null, decode_tps: null, ttft_ms: null, quality: 'unavailable' };
  const requests = points.reduce((a, p) => a + p.requests, 0);
  const avg = (pick: (p: ThroughputPoint) => number | null): number | null => {
    const vals = points.map(pick).filter((v): v is number => v != null);
    return vals.length ? vals.reduce((a, v) => a + v, 0) / vals.length : null;
  };
  return { requests, prefill_tps: avg((p) => p.prefill_tps), decode_tps: avg((p) => p.decode_tps), ttft_ms: avg((p) => p.ttft_ms), quality: 'derived' };
}

// ---------------------------------------------------------------------------
// Upstream engine samples
// ---------------------------------------------------------------------------

/** Parse the stored sample JSON; non-upstream (health) samples yield null. */
export function parseUpstreamSample(sample: MetricSample): UpstreamMetricsSample | null {
  try {
    const parsed = JSON.parse(sample.data) as Partial<UpstreamMetricsSample>;
    if (parsed.kind !== 'upstream' || typeof parsed.models !== 'object' || parsed.models === null) return null;
    return {
      kind: 'upstream',
      engine: parsed.engine ?? 'unknown',
      backend: parsed.backend ?? sample.backend,
      scraped_at_ms: typeof parsed.scraped_at_ms === 'number' ? parsed.scraped_at_ms : sample.ts_ms,
      models: parsed.models,
      kept_lines: parsed.kept_lines ?? 0,
    };
  } catch {
    return null;
  }
}

export interface EngineModelState {
  backend: string;
  engine: string;
  model: string;
  scraped_at_ms: number;
  running: number | null;
  waiting: number | null;
  kv_usage: number | null;
  /** vLLM: hits/queries counters; SGLang: the reported rate gauge. */
  prefix_hit_rate: number | null;
  /** Rates between the two latest samples (null with a single sample). */
  prompt_tps: number | null;
  generation_tps: number | null;
  ttft_ms: number | null;
  quality: Quality;
}

function v(m: UpstreamMetricsSample, model: string, key: string): number | null {
  const value = m.models[model]?.values?.[key];
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

/** The latest engine state per backend×model, with rates from the previous sample. */
export function engineStates(samples: MetricSample[]): EngineModelState[] {
  const parsed = samples.map(parseUpstreamSample).filter((s): s is UpstreamMetricsSample => s !== null);
  const byBackend = new Map<string, UpstreamMetricsSample[]>();
  for (const s of parsed) {
    const list = byBackend.get(s.backend) ?? [];
    list.push(s);
    byBackend.set(s.backend, list);
  }
  const out: EngineModelState[] = [];
  for (const [backend, list] of byBackend) {
    list.sort((a, b) => a.scraped_at_ms - b.scraped_at_ms);
    const latest = list[list.length - 1]!;
    const previous = list.length > 1 ? list[list.length - 2]! : null;
    for (const model of Object.keys(latest.models)) {
      const rate = (key: string): number | null => {
        if (!previous || !(model in previous.models)) return null;
        const dt = (latest.scraped_at_ms - previous.scraped_at_ms) / 1000;
        const now = v(latest, model, key);
        const then = v(previous, model, key);
        if (dt <= 0 || now == null || then == null || now < then) return null;
        return (now - then) / dt;
      };
      const hits = v(latest, model, 'prefix_cache_hits_total');
      const queries = v(latest, model, 'prefix_cache_queries_total');
      const ttftSum = v(latest, model, 'ttft_seconds_sum');
      const ttftCount = v(latest, model, 'ttft_count');
      out.push({
        backend,
        engine: latest.engine,
        model,
        scraped_at_ms: latest.scraped_at_ms,
        running: v(latest, model, 'running'),
        waiting: v(latest, model, 'waiting'),
        kv_usage: v(latest, model, 'kv_usage'),
        prefix_hit_rate: v(latest, model, 'cache_hit_rate') ?? (hits != null && queries ? hits / queries : null),
        prompt_tps: rate('prompt_tokens_total'),
        generation_tps: v(latest, model, 'decode_tokens_per_sec') ?? rate('generation_tokens_total'),
        ttft_ms: ttftSum != null && ttftCount ? (ttftSum / ttftCount) * 1000 : null,
        quality: 'measured',
      });
    }
  }
  return out.sort((a, b) => a.backend.localeCompare(b.backend) || a.model.localeCompare(b.model));
}

/** Rate series (tok/s) for one backend×model across all samples, for the engine chart. */
export function engineRateSeries(samples: MetricSample[], backend: string, model: string, key: string): Array<[number, number | null]> {
  const list = samples
    .map(parseUpstreamSample)
    .filter((s): s is UpstreamMetricsSample => s !== null && s.backend === backend)
    .sort((a, b) => a.scraped_at_ms - b.scraped_at_ms);
  const points: Array<[number, number | null]> = [];
  for (let i = 1; i < list.length; i++) {
    const a = list[i - 1]!;
    const b = list[i]!;
    const dt = (b.scraped_at_ms - a.scraped_at_ms) / 1000;
    const now = v(b, model, key);
    const then = v(a, model, key);
    points.push([b.scraped_at_ms, dt > 0 && now != null && then != null && now >= then ? (now - then) / dt : null]);
  }
  return points;
}

// ---------------------------------------------------------------------------
// Activity (per user / key) — shares this module's quality vocabulary.
// ---------------------------------------------------------------------------

export interface ActivityRow {
  user_id: string | null;
  virtual_key_id: string | null;
  requests: number;
  failed: number;
  error_pct: number | null;
  input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
  /** requests per bucket, x ascending */
  series: Array<[number, number]>;
}

/** Fold activity buckets into one row per (user, key), keeping the per-bucket request series. */
export function activityRows(buckets: ActivityBucket[]): ActivityRow[] {
  const rows = new Map<string, ActivityRow>();
  for (const b of buckets) {
    const key = `${b.user_id ?? ''} ${b.virtual_key_id ?? ''}`;
    const row = rows.get(key) ?? {
      user_id: b.user_id,
      virtual_key_id: b.virtual_key_id,
      requests: 0,
      failed: 0,
      error_pct: null,
      input_tokens: 0,
      output_tokens: 0,
      cached_tokens: 0,
      series: [],
    };
    row.requests += b.requests;
    row.failed += b.failed;
    row.input_tokens += b.input_tokens;
    row.output_tokens += b.output_tokens;
    row.cached_tokens += b.cached_tokens;
    row.series.push([b.bucket_ms, b.requests]);
    rows.set(key, row);
  }
  return [...rows.values()]
    .map((row) => ({ ...row, error_pct: row.requests > 0 ? (row.failed / row.requests) * 100 : null, series: row.series.sort((a, b) => a[0] - b[0]) }))
    .sort((a, b) => b.requests - a.requests);
}
