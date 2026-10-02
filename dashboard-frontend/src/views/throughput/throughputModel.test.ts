import { describe, expect, it } from 'vitest';
import type { ActivityBucket, MetricSample, ThroughputBucket } from '../../api/types';
import { ALL_MODELS, activityRows, engineRateSeries, engineStates, modelsByVolume, parseUpstreamSample, throughputSeries, throughputTotals } from './throughputModel';

function bucket(over: Partial<ThroughputBucket>): ThroughputBucket {
  return { bucket_ms: 0, model: 'm1', backend: 'b', requests: 0, completed: 0, input_tokens: 0, output_tokens: 0, cached_tokens: 0, ttft_ms_sum: 0, ttft_count: 0, prefill_tokens: 0, decode_ms_sum: 0, decode_tokens: 0, decode_count: 0, ...over };
}

describe('throughputModel — gateway series', () => {
  it('derives prefill/decode tok/s and TTFT per bucket, combined or per model', () => {
    const buckets = [
      bucket({ bucket_ms: 0, model: 'm1', requests: 10, completed: 9, prefill_tokens: 20_000, ttft_ms_sum: 4_000, ttft_count: 10, decode_tokens: 3_000, decode_ms_sum: 30_000, decode_count: 9 }),
      bucket({ bucket_ms: 0, model: 'm2', requests: 2, completed: 2, prefill_tokens: 1_000, ttft_ms_sum: 1_000, ttft_count: 2, decode_tokens: 200, decode_ms_sum: 4_000, decode_count: 2 }),
      bucket({ bucket_ms: 60_000, model: 'm1', requests: 3, completed: 0 }), // still running: no latency inputs
    ];
    const all = throughputSeries(buckets, ALL_MODELS, 60_000);
    expect(all.map((p) => p.bucket_ms)).toEqual([0, 60_000]);
    expect(all[0]!.requests).toBe(12);
    expect(all[0]!.requests_per_min).toBe(12);
    expect(all[0]!.prefill_tps).toBeCloseTo((21_000 / 5_000) * 1000);
    expect(all[0]!.decode_tps).toBeCloseTo((3_200 / 34_000) * 1000);
    expect(all[0]!.ttft_ms).toBeCloseTo(5_000 / 12);
    expect(all[1]!.prefill_tps).toBeNull();
    expect(all[1]!.decode_tps).toBeNull();
    expect(all[1]!.ttft_ms).toBeNull();

    const m2 = throughputSeries(buckets, 'm2', 60_000);
    expect(m2.length).toBe(1);
    expect(m2[0]!.prefill_tps).toBeCloseTo(1_000);
    expect(modelsByVolume(buckets)).toEqual(['m1', 'm2']);

    const totals = throughputTotals(all);
    expect(totals.requests).toBe(15);
    expect(totals.quality).toBe('derived');
    expect(totals.ttft_ms).toBeCloseTo(5_000 / 12, 5);
    expect(throughputTotals([]).quality).toBe('unavailable');
  });
});

describe('throughputModel — upstream engine samples', () => {
  const sample = (ts: number, prompt: number, gen: number): MetricSample => ({
    backend: 'vllm-a', ts_ms: ts,
    data: JSON.stringify({ kind: 'upstream', engine: 'vllm', backend: 'vllm-a', scraped_at_ms: ts, kept_lines: 5, models: { glm: { values: { running: 3, waiting: 1, kv_usage: 0.5, prompt_tokens_total: prompt, generation_tokens_total: gen, prefix_cache_hits_total: 75, prefix_cache_queries_total: 100, ttft_seconds_sum: 10, ttft_count: 20 } } } }),
  });

  it('ignores health samples and derives rates between consecutive scrapes', () => {
    expect(parseUpstreamSample({ backend: 'x', ts_ms: 1, data: JSON.stringify({ kind: 'health' }) })).toBeNull();
    expect(parseUpstreamSample({ backend: 'x', ts_ms: 1, data: 'not json' })).toBeNull();
    const states = engineStates([sample(15_000, 1_000, 200), sample(0, 100, 20), { backend: 'vllm-a', ts_ms: 5, data: JSON.stringify({ kind: 'health' }) }]);
    expect(states.length).toBe(1);
    const glm = states[0]!;
    expect(glm.model).toBe('glm');
    expect(glm.engine).toBe('vllm');
    expect(glm.running).toBe(3);
    expect(glm.kv_usage).toBe(0.5);
    expect(glm.prefix_hit_rate).toBeCloseTo(0.75);
    expect(glm.prompt_tps).toBeCloseTo(900 / 15);
    expect(glm.generation_tps).toBeCloseTo(180 / 15);
    expect(glm.ttft_ms).toBeCloseTo(500);
    // A single sample has no rate.
    expect(engineStates([sample(0, 1, 1)])[0]!.prompt_tps).toBeNull();
    // A counter reset (now < then) is not a negative rate.
    expect(engineStates([sample(0, 1_000, 1), sample(15_000, 10, 1)])[0]!.prompt_tps).toBeNull();
    const series = engineRateSeries([sample(0, 100, 0), sample(15_000, 400, 0), sample(30_000, 400, 0)], 'vllm-a', 'glm', 'prompt_tokens_total');
    expect(series).toEqual([[15_000, 20], [30_000, 0]]);
  });
});

describe('throughputModel — activity rows', () => {
  it('folds buckets per user/key with error rate and a request series', () => {
    const buckets: ActivityBucket[] = [
      { bucket_ms: 0, user_id: 'u1', virtual_key_id: 'k1', requests: 10, failed: 1, input_tokens: 100, output_tokens: 10, cached_tokens: 50 },
      { bucket_ms: 60_000, user_id: 'u1', virtual_key_id: 'k1', requests: 30, failed: 0, input_tokens: 300, output_tokens: 30, cached_tokens: 0 },
      { bucket_ms: 0, user_id: null, virtual_key_id: null, requests: 2, failed: 2, input_tokens: 0, output_tokens: 0, cached_tokens: 0 },
    ];
    const rows = activityRows(buckets);
    expect(rows.map((r) => [r.user_id, r.requests])).toEqual([['u1', 40], [null, 2]]);
    expect(rows[0]!.error_pct).toBeCloseTo(2.5);
    expect(rows[0]!.series).toEqual([[0, 10], [60_000, 30]]);
    expect(rows[0]!.cached_tokens).toBe(50);
    expect(rows[1]!.error_pct).toBe(100);
    expect(activityRows([])).toEqual([]);
  });
});
