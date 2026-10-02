import { describe, it, expect } from 'vitest';
import type { MetricWindow, MetricsResponse } from '../../api/types';
import {
  appendTick,
  emptyHistory,
  HISTORY_DEPTH,
  latest,
  previous,
  seriesFor,
} from './metricHistory';

function win(over: Partial<MetricWindow> = {}): MetricWindow {
  // Fully-measured default: the three denominators mirror `samples` unless overridden.
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

function tick(over: { m1?: Partial<MetricWindow>; m5?: Partial<MetricWindow>; h1?: Partial<MetricWindow> } = {}) {
  return { windows: { m1: win(over.m1), m5: win(over.m5), h1: win(over.h1) } } as Pick<MetricsResponse, 'windows'>;
}

describe('metricHistory', () => {
  it('emptyHistory starts with empty rings', () => {
    const h = emptyHistory();
    expect(h.m1).toEqual([]);
    expect(seriesFor(h, 'm1', 'reqs_per_sec')).toEqual([]);
    expect(latest(h, 'm1')).toBeNull();
    expect(previous(h, 'm1')).toBeNull();
  });

  it('appendTick folds one sample per window, newest last', () => {
    let h = emptyHistory();
    h = appendTick(h, tick({ m1: { reqs_per_sec: 1 } }));
    h = appendTick(h, tick({ m1: { reqs_per_sec: 2 } }));
    expect(seriesFor(h, 'm1', 'reqs_per_sec')).toEqual([1, 2]);
    expect(latest(h, 'm1')?.reqs_per_sec).toBe(2);
    expect(previous(h, 'm1')?.reqs_per_sec).toBe(1);
  });

  it('extracts the per-metric series independently per window', () => {
    let h = emptyHistory();
    h = appendTick(h, tick({ m1: { decode_tokens_per_sec: 10 }, h1: { decode_tokens_per_sec: 99 } }));
    h = appendTick(h, tick({ m1: { decode_tokens_per_sec: 20 }, h1: { decode_tokens_per_sec: 88 } }));
    expect(seriesFor(h, 'm1', 'decode_tokens_per_sec')).toEqual([10, 20]);
    expect(seriesFor(h, 'h1', 'decode_tokens_per_sec')).toEqual([99, 88]);
  });

  it('caps each ring at HISTORY_DEPTH (oldest evicted)', () => {
    let h = emptyHistory();
    for (let i = 0; i < HISTORY_DEPTH + 25; i++) h = appendTick(h, tick({ m1: { p95: i } }));
    const series = seriesFor(h, 'm1', 'p95');
    expect(series).toHaveLength(HISTORY_DEPTH);
    // Last value is the most recent; first is exactly DEPTH back (oldest dropped).
    expect(series[series.length - 1]).toBe(HISTORY_DEPTH + 24);
    expect(series[0]).toBe(25);
  });

  it('append is immutable (returns a new history, does not mutate input)', () => {
    const h0 = emptyHistory();
    const h1 = appendTick(h0, tick());
    expect(h0.m1).toHaveLength(0);
    expect(h1.m1).toHaveLength(1);
    expect(h1).not.toBe(h0);
  });

  // Gap 01 finding 2 — sparklines must NOT plot a raw 0 for an unmeasurable sample.
  it('emits a NaN gap (not 0) for a sample-derived point that was unavailable in its sample', () => {
    let h = emptyHistory();
    // Sample A: usage reported → real tok/s (142). Sample B: NO usage (usage_samples 0)
    // with a raw 0 tok/s on the wire. Sample C: usage again → real tok/s (50).
    h = appendTick(h, tick({ m1: { decode_tokens_per_sec: 142, decode_samples: 5 } }));
    h = appendTick(h, tick({ m1: { decode_tokens_per_sec: 0, decode_samples: 0 } }));
    h = appendTick(h, tick({ m1: { decode_tokens_per_sec: 50, decode_samples: 5 } }));
    const series = seriesFor(h, 'm1', 'decode_tokens_per_sec');
    expect(series).toHaveLength(3);
    expect(series[0]).toBe(142);
    expect(Number.isNaN(series[1])).toBe(true); // GAP, not a fabricated 0
    expect(series[2]).toBe(50);
  });

  it('never gaps req/s — a genuine idle 0 is plotted (it is not sample-derived)', () => {
    let h = emptyHistory();
    // Even with zero samples (nothing finalized), req/s is a real measured rate.
    h = appendTick(h, tick({ m1: { reqs_per_sec: 0, samples: 0, usage_samples: 0, priced_samples: 0 } }));
    h = appendTick(h, tick({ m1: { reqs_per_sec: 2.5, samples: 0, usage_samples: 0, priced_samples: 0 } }));
    const series = seriesFor(h, 'm1', 'reqs_per_sec');
    expect(series).toEqual([0, 2.5]); // both plotted, no NaN gap
  });

  it('gaps cost_per_min when pricing is missing but plots phase speed when measured', () => {
    let h = emptyHistory();
    h = appendTick(h, tick({ m1: { decode_tokens_per_sec: 142, cost_per_min: 0, samples: 4, decode_samples: 4, priced_samples: 0 } }));
    expect(Number.isNaN(seriesFor(h, 'm1', 'cost_per_min')[0])).toBe(true); // unpriced → gap
    expect(seriesFor(h, 'm1', 'decode_tokens_per_sec')[0]).toBe(142);
  });
});
