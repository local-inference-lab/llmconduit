/**
 * Chip descriptors — the pure mapping from a `MetricWindow` sample (+ its predecessor) to each
 * strip chip's display value, sparkline stroke, threshold accent, and delta direction.
 *
 * DOM-free so it is unit-testable and the component stays a thin renderer.
 */
import type { CostConfidence, MetricWindow } from '../../api/types';
import { colors } from '../../design/tokens';
import { metricUnavailable, type MetricKey } from './metricHistory';

/** Error-% threshold above which the err chip turns red (spec: "red above threshold"). */
export const ERROR_PCT_THRESHOLD = 5;

const TOKEN_RATE_NUMBER = new Intl.NumberFormat('en-US', {
  useGrouping: false,
  maximumFractionDigits: 2,
});

export type DeltaDir = 'up' | 'down' | 'flat';

/**
 * The data-quality provenance of a chip's value (IMPLEMENTATION_PLAN cross-cutting rule:
 * EVERY rendered metric is tagged measured / derived / estimated / unavailable). Rendered
 * via `data-quality` + an ARIA/title hint on the chip so operators can tell a directly
 * counted value from a derived/estimated one from an honest gap:
 *  - `measured`     — directly counted off the live gateway (`req/s`, `active_streams`).
 *  - `derived`      — computed from finalized-flow samples (err%, p50/p95/p99, tok/s).
 *  - `estimated`    — priced via the configured price table, i.e. a modelled estimate
 *                     ($/min). MUST be surfaced as such (the plan calls this out).
 *  - `unavailable`  — not measurable in this window; the value renders `—`, never `0`.
 */
export type MetricQuality = 'measured' | 'derived' | 'estimated' | 'unavailable';

export interface ChipDescriptor {
  key: MetricKey;
  label: string;
  /** Preformatted value string (tabular-nums applied by the chip component). */
  value: string;
  /** Sparkline stroke (hex token). */
  stroke: string;
  /** Token accent for the value text (threshold-driven for err%). */
  accent: 'accent' | 'healthy' | 'meta' | 'down' | 'text';
  /** Direction of change vs. the previous sample (drives the delta arrow). */
  delta: DeltaDir;
  /**
   * Data-quality provenance of the rendered value (finding 4). `unavailable` whenever
   * the value is `—`; otherwise the metric's intrinsic tier (measured/derived/estimated).
   */
  quality: MetricQuality;
  /** uPlot stroke as hex for the sparkline (mirrors `stroke`, kept explicit for clarity). */
  sparkStroke: string;
}

/** Round-trip-safe compact rate (`4.2`, `142`, `1.2k`). */
function fmtRate(n: number): string {
  if (!Number.isFinite(n)) return '—';
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  if (n >= 100) return String(Math.round(n));
  return n.toFixed(1);
}

/** Token throughput, compacted with at most two fractional digits. */
export function fmtTokenRate(n: number): string {
  if (!Number.isFinite(n)) return '—';
  const compact = (value: number, suffix: string) => `${TOKEN_RATE_NUMBER.format(value)}${suffix}`;
  if (n >= 1_000_000) return compact(n / 1_000_000, 'm');
  if (n >= 1_000) return compact(n / 1_000, 'k');
  return TOKEN_RATE_NUMBER.format(n);
}

/** Latency ms → integer ms (`920`). */
function fmtMs(n: number): string {
  return Number.isFinite(n) ? String(Math.round(n)) : '—';
}

/** Percent → 1dp (`1.1`). */
function fmtPct(n: number): string {
  return Number.isFinite(n) ? n.toFixed(1) : '—';
}

/** Dollars/min → 2dp (`0.21`). */
function fmtMoney(n: number): string {
  return Number.isFinite(n) ? n.toFixed(2) : '—';
}

/** Compare a metric field across two samples → delta direction (with a small epsilon). */
function deltaDir(cur: number, prev: number | undefined): DeltaDir {
  if (prev === undefined || !Number.isFinite(prev) || !Number.isFinite(cur)) return 'flat';
  const d = cur - prev;
  const eps = Math.max(1e-6, Math.abs(prev) * 1e-4);
  if (d > eps) return 'up';
  if (d < -eps) return 'down';
  return 'flat';
}

/**
 * The $/min chip's provenance tier from the backend's AGGREGATE `cost_confidence` (gap 07
 * review round 1, finding 5). Called ONLY when the cost is available (the `unavailable`
 * denominator branch wins otherwise):
 *  - `confident`   → `derived`   (a real cost computed from finalized-flow samples — every
 *                                 priced bucket's billed classes have known rates).
 *  - `estimated`   → `estimated` (some priced bucket bills cached at the default 0.0, or an
 *                                 unpriced bucket bears usage — a labelled best-effort estimate).
 *  - `unavailable` → `unavailable` (defensive; this normally coincides with `priced_samples ===
 *                                 0`, which the denominator branch already caught).
 */
function costQuality(confidence: CostConfidence): MetricQuality {
  switch (confidence) {
    case 'confident':
      return 'derived';
    case 'estimated':
      return 'estimated';
    case 'unavailable':
      return 'unavailable';
  }
}

/** Per-metric display config: label, formatter, stroke. Order = strip display order. */
interface MetricSpec {
  key: MetricKey;
  label: string;
  fmt: (n: number) => string;
  stroke: string;
  accent: ChipDescriptor['accent'];
  /**
   * Intrinsic data-quality tier when the value IS available (finding 4). `measured` for
   * directly-counted metrics, `derived` for sample-computed ones, `estimated` for the
   * price-modelled cost. Collapses to `unavailable` when the metric's denominator is `0`
   * (the denominator itself lives in `METRIC_AVAILABILITY` in metricHistory).
   */
  quality: Exclude<MetricQuality, 'unavailable'>;
}

const METRIC_SPECS: readonly MetricSpec[] = [
  { key: 'reqs_per_sec', label: 'req/s', fmt: fmtRate, stroke: colors.accent, accent: 'accent', quality: 'measured' },
  { key: 'active_streams', label: 'active', fmt: fmtRate, stroke: colors.accent, accent: 'text', quality: 'measured' },
  { key: 'error_pct', label: 'err %', fmt: fmtPct, stroke: colors.statusDown, accent: 'text', quality: 'derived' },
  { key: 'p50', label: 'p50 ms', fmt: fmtMs, stroke: colors.statusHealthy, accent: 'text', quality: 'derived' },
  { key: 'p95', label: 'p95 ms', fmt: fmtMs, stroke: colors.statusCooling, accent: 'text', quality: 'derived' },
  { key: 'p99', label: 'p99 ms', fmt: fmtMs, stroke: colors.statusDown, accent: 'text', quality: 'derived' },
  { key: 'prefill_tokens_per_sec', label: 'prefill tok/s', fmt: fmtTokenRate, stroke: colors.statusHealthy, accent: 'healthy', quality: 'derived' },
  { key: 'decode_tokens_per_sec', label: 'decode tok/s', fmt: fmtTokenRate, stroke: colors.statusCooling, accent: 'healthy', quality: 'derived' },
  // $/min: the static tier here is a FALLBACK only — its real quality is derived per-sample from
  // the backend `cost_confidence` (gap 07 finding 5, see `costQuality`), so a confident aggregate
  // reads `derived` and an estimated one reads `estimated` (no longer always `estimated`).
  { key: 'cost_per_min', label: '$/min', fmt: fmtMoney, stroke: colors.meta, accent: 'meta', quality: 'estimated' },
];

/** The unavailable / no-data marker (a value that cannot be measured renders this, never `0`). */
export const UNAVAILABLE = '—';

/** The metric keys, in strip order (handy for tests / iteration). */
export const CHIP_METRICS: readonly MetricKey[] = METRIC_SPECS.map((s) => s.key);

/**
 * Derive every chip descriptor for the current + previous window samples. `cur === null`
 * (no tick yet) renders every value as the unavailable marker with a flat delta.
 *
 * Don't-lie-with-zeros + per-metric availability (gap 01 findings 3/4): each metric has its
 * OWN measurability denominator — latency/err% need a finalized flow (`samples`), each phase
 * speed needs usage plus its measured phase duration (`prefill_samples`/`decode_samples`), and
 * $/min needs a usage-bearing flow on a PRICED model (`priced_samples`). A window can have
 * `samples > 0` yet no phase samples or no priced samples: those metrics render
 * `unavailable` (`—`), NEVER a fabricated `0`, with a flat delta and no threshold accent.
 * `req/s` (a genuine idle `0`) and `active_streams` (the live open count) are never gated.
 * Every chip also carries a `quality` provenance tag (measured/derived/estimated/unavailable).
 */
export function deriveChips(cur: MetricWindow | null, prev: MetricWindow | null): ChipDescriptor[] {
  return METRIC_SPECS.map((spec): ChipDescriptor => {
    // Unmeasurable when there is no window, or this metric's own denominator is 0.
    const unavailable = metricUnavailable(cur, spec.key);
    const value = unavailable || !cur ? UNAVAILABLE : spec.fmt(cur[spec.key]);
    // The err% chip turns red ABOVE the threshold — but only when it is actually MEASURED
    // (an unavailable err% carries no threshold accent); others keep their static accent.
    const accent: ChipDescriptor['accent'] =
      !unavailable && cur && spec.key === 'error_pct' && cur.error_pct > ERROR_PCT_THRESHOLD ? 'down' : spec.accent;
    // No trend direction for an unavailable value, nor across the genuine→unavailable boundary
    // (the previous sample being unavailable for THIS metric makes the delta meaningless).
    const prevUnavailable = metricUnavailable(prev, spec.key);
    const delta = !unavailable && cur && !prevUnavailable
      ? deltaDir(cur[spec.key], prev?.[spec.key])
      : 'flat';
    // Provenance (finding 4): `unavailable` when `—`, else the metric's intrinsic tier — EXCEPT
    // the cost chip, whose tier is the AGGREGATE `cost_confidence` the backend reports (gap 07
    // review round 1, finding 5), not a hard-coded `estimated`. A `confident` aggregate (every
    // priced bucket's billed classes have known rates) is a real DERIVED figure; an `estimated`
    // one (some priced bucket bills cached at the default 0.0, or an unpriced bucket bears usage)
    // is a labelled estimate. `unavailable` is already handled by the denominator branch above
    // (it coincides with `priced_samples === 0`), so operators can finally tell a confident
    // aggregate cost from an estimated one instead of every `$/min` always reading `estimated`.
    const quality: MetricQuality = unavailable
      ? 'unavailable'
      : spec.key === 'cost_per_min' && cur
        ? costQuality(cur.cost_confidence)
        : spec.quality;
    return {
      key: spec.key,
      label: spec.label,
      value,
      stroke: spec.stroke,
      accent,
      delta,
      quality,
      sparkStroke: spec.stroke,
    };
  });
}

/** Delta arrow glyph for a direction (UI affordance). */
export function deltaGlyph(dir: DeltaDir): string {
  return dir === 'up' ? '▲' : dir === 'down' ? '▼' : '·';
}
