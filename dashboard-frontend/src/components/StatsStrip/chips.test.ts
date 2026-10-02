import { describe, it, expect } from 'vitest';
import type { MetricWindow } from '../../api/types';
import { CHIP_METRICS, deriveChips, deltaGlyph, ERROR_PCT_THRESHOLD } from './chips';

function win(over: Partial<MetricWindow> = {}): MetricWindow {
  // Default to a fully-measured window: the three denominators mirror `samples` so a test
  // that sets `samples: 0` (no finalized flow) also zeroes the tok/s + $/min denominators
  // unless it overrides them — keeping the gap-01 "unavailable" semantics intact. A test
  // that needs to diverge them (samples > 0 but usage/priced = 0) passes them explicitly.
  const samples = over.samples ?? 252;
  return {
    reqs_per_sec: 4.2, active_streams: 3, error_pct: 1.1,
    p50: 180, p95: 920, p99: 1840, tokens_per_sec: 142,
    prefill_tokens_per_sec: 10_000, decode_tokens_per_sec: 78.34, cost_per_min: 0.21,
    samples,
    usage_samples: samples,
    prefill_samples: samples,
    decode_samples: samples,
    priced_samples: samples,
    cost_confidence: 'estimated',
    ...over,
  };
}

describe('chips', () => {
  it('emits one descriptor per chip metric in strip order', () => {
    const chips = deriveChips(win(), null);
    expect(chips.map((c) => c.key)).toEqual([...CHIP_METRICS]);
  });

  it('formats values (rate/ms/pct/tokens/money)', () => {
    const chips = deriveChips(win({ reqs_per_sec: 4.2, p95: 920, error_pct: 1.1, prefill_tokens_per_sec: 1500, decode_tokens_per_sec: 142.35, cost_per_min: 0.21 }), null);
    const byKey = Object.fromEntries(chips.map((c) => [c.key, c.value]));
    expect(byKey.reqs_per_sec).toBe('4.2');
    expect(byKey.p95).toBe('920');
    expect(byKey.error_pct).toBe('1.1');
    expect(byKey.prefill_tokens_per_sec).toBe('1.5k');
    expect(byKey.decode_tokens_per_sec).toBe('142.35');
    expect(byKey.cost_per_min).toBe('0.21');
  });

  it('caps tok/s at two decimal places without padding trailing zeroes', () => {
    const precise = deriveChips(win({ decode_tokens_per_sec: 142.345 }), null)
      .find((c) => c.key === 'decode_tokens_per_sec')!;
    const compact = deriveChips(win({ prefill_tokens_per_sec: 1_500.123 }), null)
      .find((c) => c.key === 'prefill_tokens_per_sec')!;

    expect(precise.value).toBe('142.35');
    expect(compact.value).toBe('1.5k');
  });

  it('renders "—" for every chip when there is no sample', () => {
    const chips = deriveChips(null, null);
    expect(chips.every((c) => c.value === '—')).toBe(true);
    expect(chips.every((c) => c.delta === 'flat')).toBe(true);
  });

  it('turns the err% chip red ONLY above the threshold', () => {
    const below = deriveChips(win({ error_pct: ERROR_PCT_THRESHOLD - 0.1 }), null).find((c) => c.key === 'error_pct')!;
    const above = deriveChips(win({ error_pct: ERROR_PCT_THRESHOLD + 0.1 }), null).find((c) => c.key === 'error_pct')!;
    expect(below.accent).not.toBe('down');
    expect(above.accent).toBe('down');
  });

  it('computes the delta direction vs. the previous sample', () => {
    const up = deriveChips(win({ reqs_per_sec: 5 }), win({ reqs_per_sec: 4 })).find((c) => c.key === 'reqs_per_sec')!;
    const down = deriveChips(win({ reqs_per_sec: 3 }), win({ reqs_per_sec: 4 })).find((c) => c.key === 'reqs_per_sec')!;
    const flat = deriveChips(win({ reqs_per_sec: 4 }), win({ reqs_per_sec: 4 })).find((c) => c.key === 'reqs_per_sec')!;
    expect(up.delta).toBe('up');
    expect(down.delta).toBe('down');
    expect(flat.delta).toBe('flat');
  });

  it('deltaGlyph maps direction → arrow', () => {
    expect(deltaGlyph('up')).toBe('▲');
    expect(deltaGlyph('down')).toBe('▼');
    expect(deltaGlyph('flat')).toBe('·');
  });

  // Gap 01 — don't lie with zeros.
  it('renders sample-derived metrics as UNAVAILABLE (—) when the window has zero samples', () => {
    // A window with traffic in flight but NOTHING finalized: samples 0, but req/s is a
    // genuine measured rate (and active_streams a live count). The numeric fields are 0
    // on the wire (no sample fed them) — they MUST render "—", never "0".
    const chips = deriveChips(win({ samples: 0, error_pct: 0, p50: 0, p95: 0, p99: 0, tokens_per_sec: 0, cost_per_min: 0, reqs_per_sec: 2.5, active_streams: 4 }), null);
    const byKey = Object.fromEntries(chips.map((c) => [c.key, c.value]));
    // Sample-derived metrics are unavailable (—), NOT a fabricated 0.
    expect(byKey.error_pct).toBe('—');
    expect(byKey.p50).toBe('—');
    expect(byKey.p95).toBe('—');
    expect(byKey.p99).toBe('—');
    expect(byKey.prefill_tokens_per_sec).toBe('—');
    expect(byKey.decode_tokens_per_sec).toBe('—');
    expect(byKey.cost_per_min).toBe('—');
    // req/s + active_streams are NOT sample-derived → they show real values.
    expect(byKey.reqs_per_sec).toBe('2.5');
    expect(byKey.active_streams).toBe('4.0');
  });

  it('distinguishes a GENUINE measured zero (samples > 0) from unavailable', () => {
    // Real traffic finalized (samples 12) that genuinely measured 0 latency/cost/err: those
    // are honest zeros and render numerically, NOT "—".
    const chips = deriveChips(win({ samples: 12, error_pct: 0, p50: 0, cost_per_min: 0, reqs_per_sec: 0 }), null);
    const byKey = Object.fromEntries(chips.map((c) => [c.key, c.value]));
    expect(byKey.error_pct).toBe('0.0');
    expect(byKey.p50).toBe('0');
    expect(byKey.cost_per_min).toBe('0.00');
    expect(byKey.reqs_per_sec).toBe('0.0'); // genuine idle zero, also numeric
  });

  it('an unavailable (zero-sample) window has a FLAT delta and no err% threshold accent', () => {
    const prev = win({ samples: 10, error_pct: 9.9 });
    const cur = win({ samples: 0, error_pct: 0, p50: 0 });
    const chips = deriveChips(cur, prev);
    const err = chips.find((c) => c.key === 'error_pct')!;
    const p50 = chips.find((c) => c.key === 'p50')!;
    expect(err.value).toBe('—');
    expect(err.accent).not.toBe('down'); // an unavailable err% carries no threshold red
    expect(p50.delta).toBe('flat'); // no trend for an unmeasurable value
  });

  // Gap 01 finding 3 — per-metric availability denominators diverge.
  it('renders phase tok/s + $/min as "—" without phase/usage samples, even though latency IS measured', () => {
    // samples 12 (latency measured) but usage_samples 0 (no flow reported tokens) and so
    // priced_samples 0 too. Latency/err% are real; tok/s + $/min are unmeasurable → "—".
    const chips = deriveChips(win({ samples: 12, usage_samples: 0, prefill_samples: 0, decode_samples: 0, priced_samples: 0, p50: 200, prefill_tokens_per_sec: 0, decode_tokens_per_sec: 0, cost_per_min: 0 }), null);
    const byKey = Object.fromEntries(chips.map((c) => [c.key, c.value]));
    expect(byKey.p50).toBe('200'); // latency measured (samples > 0)
    expect(byKey.error_pct).toBe('1.1');
    expect(byKey.prefill_tokens_per_sec).toBe('—');
    expect(byKey.decode_tokens_per_sec).toBe('—');
    expect(byKey.cost_per_min).toBe('—'); // no priced usage sample → unmeasurable
  });

  it('renders $/min as "—" for an unpriced model while phase speeds stay numeric', () => {
    // usage_samples 8 (tok/s measurable) but priced_samples 0 (only unpriced models) →
    // $/min is unmeasurable ("—"), distinct from a genuine $0.00. tok/s renders normally.
    const chips = deriveChips(win({ samples: 8, usage_samples: 8, prefill_samples: 8, decode_samples: 8, priced_samples: 0, cost_per_min: 0 }), null);
    const byKey = Object.fromEntries(chips.map((c) => [c.key, c.value]));
    expect(byKey.prefill_tokens_per_sec).toBe('10k');
    expect(byKey.decode_tokens_per_sec).toBe('78.34');
    expect(byKey.cost_per_min).toBe('—'); // no priced sample → unavailable, not $0.00
  });

  // Gap 01 finding 4 — provenance/quality on every chip state.
  it('tags each chip with measured/derived/estimated provenance when available', () => {
    const chips = deriveChips(win(), null);
    const byKey = Object.fromEntries(chips.map((c) => [c.key, c.quality]));
    expect(byKey.reqs_per_sec).toBe('measured');
    expect(byKey.active_streams).toBe('measured');
    expect(byKey.error_pct).toBe('derived');
    expect(byKey.p50).toBe('derived');
    expect(byKey.p95).toBe('derived');
    expect(byKey.p99).toBe('derived');
    expect(byKey.prefill_tokens_per_sec).toBe('derived');
    expect(byKey.decode_tokens_per_sec).toBe('derived');
    expect(byKey.cost_per_min).toBe('estimated'); // priced → estimated, surfaced as such
  });

  it('tags an unmeasurable metric as "unavailable" while measured ones keep their tier', () => {
    // samples 0 → latency/err%/tok-s/$/min all unavailable; req/s + active stay measured.
    const chips = deriveChips(win({ samples: 0 }), null);
    const byKey = Object.fromEntries(chips.map((c) => [c.key, c.quality]));
    expect(byKey.reqs_per_sec).toBe('measured');
    expect(byKey.active_streams).toBe('measured');
    expect(byKey.error_pct).toBe('unavailable');
    expect(byKey.p50).toBe('unavailable');
    expect(byKey.prefill_tokens_per_sec).toBe('unavailable');
    expect(byKey.decode_tokens_per_sec).toBe('unavailable');
    expect(byKey.cost_per_min).toBe('unavailable');
  });

  it('tags cost as unavailable but tok/s as derived when only pricing is missing', () => {
    const chips = deriveChips(win({ samples: 8, usage_samples: 8, priced_samples: 0 }), null);
    const byKey = Object.fromEntries(chips.map((c) => [c.key, c.quality]));
    expect(byKey.prefill_tokens_per_sec).toBe('derived');
    expect(byKey.decode_tokens_per_sec).toBe('derived');
    expect(byKey.cost_per_min).toBe('unavailable'); // unpriced → not an estimate, a gap
  });

  it('every chip carries a quality tag from the closed set (no chip is untagged)', () => {
    const valid = new Set(['measured', 'derived', 'estimated', 'unavailable']);
    for (const cur of [win(), win({ samples: 0 }), null]) {
      for (const chip of deriveChips(cur, null)) {
        expect(valid.has(chip.quality), `${chip.key} → ${chip.quality}`).toBe(true);
      }
    }
  });

  // Gap 07 review round 1, finding 5 — the $/min chip's quality/label is DERIVED from the
  // backend aggregate `cost_confidence`, not a hard-coded `estimated`. Operators can now tell a
  // confident aggregate cost from an estimated one.
  it('a CONFIDENT aggregate $/min reads as "derived", not "estimated"', () => {
    const chip = deriveChips(win({ cost_confidence: 'confident' }), null).find((c) => c.key === 'cost_per_min')!;
    expect(chip.value).toBe('0.21'); // a real, rendered number
    expect(chip.quality).toBe('derived'); // confident ⇒ a real computed cost, not a modelled estimate
  });

  it('an ESTIMATED aggregate $/min stays labelled "estimated"', () => {
    const chip = deriveChips(win({ cost_confidence: 'estimated' }), null).find((c) => c.key === 'cost_per_min')!;
    expect(chip.value).toBe('0.21');
    expect(chip.quality).toBe('estimated'); // a priced bucket bills cached at the default 0.0 (or an unpriced bucket bears usage)
  });

  it('an UNAVAILABLE cost ($/min) renders "—" and is tagged unavailable regardless of the cost_confidence value', () => {
    // priced_samples 0 ⇒ the denominator branch makes $/min unavailable; the cost_confidence the
    // backend pairs with that is `unavailable` too, but even a stray non-unavailable tag cannot
    // resurrect a value (the denominator wins).
    const chip = deriveChips(win({ samples: 8, usage_samples: 8, priced_samples: 0, cost_confidence: 'unavailable' }), null)
      .find((c) => c.key === 'cost_per_min')!;
    expect(chip.value).toBe('—');
    expect(chip.quality).toBe('unavailable');
  });

  it('the $/min cost_confidence override does NOT affect the tok/s chip (only the cost chip)', () => {
    // tok/s keeps its intrinsic `derived` tier even when the window cost is confident.
    const toks = deriveChips(win({ cost_confidence: 'confident' }), null).find((c) => c.key === 'decode_tokens_per_sec')!;
    expect(toks.quality).toBe('derived');
  });
});
