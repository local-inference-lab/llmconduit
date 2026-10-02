/**
 * Per-window metric history — the ring buffers the StatsStrip sparklines read.
 *
 * Each `MetricTick` (and the seed `/metrics`) carries `windows.{m1,m5,h1}`, every one a full
 * `MetricWindow` (the 9 chip metrics). A sparkline for a chosen window+metric is the recent
 * history of that `windows[w][metric]` scalar. So we keep, PER window, a capped ring of the last
 * N `MetricWindow` samples; a chip extracts one field into a `number[]` for its Sparkline.
 *
 * Pure + DOM-free (a plain reducer over an immutable state object) so it is trivially testable
 * and can live in `useMemo`/`useRef` without React entanglement.
 */
import type { MetricWindow, MetricsResponse, MetricTickPayload } from '../../api/types';

/** The three sliding windows the selector switches between. */
export type WindowKey = 'm1' | 'm5' | 'h1';
export const WINDOW_KEYS: readonly WindowKey[] = ['m1', 'm5', 'h1'];
export const WINDOW_LABELS: Record<WindowKey, string> = { m1: '1m', m5: '5m', h1: '1h' };

/** The chip metrics, in strip display order. Each maps to a `MetricWindow` field. */
export type MetricKey =
  | 'reqs_per_sec'
  | 'active_streams'
  | 'error_pct'
  | 'p50'
  | 'p95'
  | 'p99'
  | 'prefill_tokens_per_sec'
  | 'decode_tokens_per_sec'
  | 'cost_per_min';

/**
 * Which per-window count is a metric's MEASURABILITY denominator (gap 01 findings 2/3 —
 * token and cost availability are SEPARATE from latency). `0` for the relevant count means
 * the metric is unmeasurable in that window → the chip renders `—` and the sparkline draws
 * a gap, never a fabricated `0`:
 *  - `always`  — not sample-gated (`req/s` idle-`0`, `active_streams` live count).
 *  - `samples` — needs a finalized flow (err%, p50/p95/p99).
 *  - `prefill` — needs prompt usage plus a measured non-zero prefill phase.
 *  - `decode`  — needs completion usage plus a measured non-zero decode phase.
 *  - `priced`  — needs a usage-bearing flow on a PRICED model (`cost_per_min`).
 * Lives here (the pure history module) so both the sparkline (`seriesFor`) and the chip
 * value (`deriveChips`) read one source — and so there is no chips↔history import cycle.
 */
export type Availability = 'always' | 'samples' | 'prefill' | 'decode' | 'priced';

/** Each metric's measurability denominator (gap 01 finding 3). */
export const METRIC_AVAILABILITY: Record<MetricKey, Availability> = {
  reqs_per_sec: 'always',
  active_streams: 'always',
  error_pct: 'samples',
  p50: 'samples',
  p95: 'samples',
  p99: 'samples',
  prefill_tokens_per_sec: 'prefill',
  decode_tokens_per_sec: 'decode',
  cost_per_min: 'priced',
};

/** Read a window's measurability denominator for an `Availability` tier. */
function denominatorFor(window: MetricWindow, availability: Availability): number {
  switch (availability) {
    case 'always':
      return Number.POSITIVE_INFINITY; // never gated
    case 'samples':
      return window.samples;
    case 'prefill':
      return window.prefill_samples;
    case 'decode':
      return window.decode_samples;
    case 'priced':
      return window.priced_samples;
  }
}

/**
 * Whether `metric` is UNMEASURABLE for `window` — its measurability denominator is `0`.
 * A `null` window (no tick yet) is unavailable for every metric. Shared by `seriesFor`
 * (sparkline → gap) AND `deriveChips` (value → `—`) so the trend and the value agree.
 */
export function metricUnavailable(window: MetricWindow | null, metric: MetricKey): boolean {
  if (!window) return true;
  return denominatorFor(window, METRIC_AVAILABILITY[metric]) === 0;
}

/** Sparkline depth (samples retained per window). Spec: 60-sample sparklines. */
export const HISTORY_DEPTH = 60;

/** Immutable history state: a capped ring of `MetricWindow` samples per window. */
export interface MetricHistory {
  m1: MetricWindow[];
  m5: MetricWindow[];
  h1: MetricWindow[];
}

export function emptyHistory(): MetricHistory {
  return { m1: [], m5: [], h1: [] };
}

/** Append `sample` to `ring`, dropping the oldest beyond `cap` (returns a NEW array). */
function pushCapped(ring: MetricWindow[], sample: MetricWindow, cap = HISTORY_DEPTH): MetricWindow[] {
  const next = ring.length >= cap ? ring.slice(ring.length - cap + 1) : ring.slice();
  next.push(sample);
  return next;
}

/**
 * Fold one tick's `windows` into the history (one sample per window). Accepts the `MetricTick`
 * WS payload OR the `/metrics` REST response — both expose the same `windows` shape — so the
 * seed read and the live stream share one accumulator. Returns a NEW `MetricHistory`.
 */
export function appendTick(
  history: MetricHistory,
  tick: Pick<MetricTickPayload | MetricsResponse, 'windows'>,
): MetricHistory {
  return {
    m1: pushCapped(history.m1, tick.windows.m1),
    m5: pushCapped(history.m5, tick.windows.m5),
    h1: pushCapped(history.h1, tick.windows.h1),
  };
}

/**
 * Extract the `metric` field across a window's ring into the Sparkline's `number[]`.
 *
 * Availability-aware (gap 01 finding 2): a sample-derived point that was UNMEASURABLE in
 * its sample (e.g. prefill/decode tok/s when its phase sample count is `0`, or
 * `cost_per_min` when `priced_samples === 0`) is emitted as `NaN` — uPlot renders a GAP
 * there rather than plotting a raw `0`, so an unavailable p50/tok-s/$/min never draws a
 * misleading zero trend. `metricUnavailable` (above) is the same predicate the chip value
 * uses, so the sparkline and the chip agree on what is real vs. a gap. `req/s`/
 * `active_streams` are never gated, so their series is the raw values.
 */
export function seriesFor(history: MetricHistory, window: WindowKey, metric: MetricKey): number[] {
  return history[window].map((w) => (metricUnavailable(w, metric) ? NaN : w[metric]));
}

/** The newest sample for a window (the live chip VALUE), or null before any tick. */
export function latest(history: MetricHistory, window: WindowKey): MetricWindow | null {
  const ring = history[window];
  return ring.length ? ring[ring.length - 1]! : null;
}

/** The previous sample for a window (drives the chip's delta arrow), or null. */
export function previous(history: MetricHistory, window: WindowKey): MetricWindow | null {
  const ring = history[window];
  return ring.length >= 2 ? ring[ring.length - 2]! : null;
}
