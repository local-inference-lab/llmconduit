import { describe, expect, it } from 'vitest';
import type { HistoryRequest, SessionRow } from '../../api/types';
import { divergenceLabel, fmtAge, kindBadge, rollupRequests, sessionLabel, sortByActivity } from './sessionsModel';

function node(over: Partial<SessionRow> = {}): SessionRow {
  return {
    id: 'a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d',
    parent_id: null,
    kind: 'declared',
    harness: 'claude-code',
    harness_version: '2.1.205',
    external_id: null,
    session_kind: null,
    client_label: null,
    virtual_key_id: null,
    user_id: null,
    depth: 0,
    root_request_id: null,
    spawned_by_request_id: null,
    first_seen_ms: 1_000,
    last_seen_ms: 2_000,
    request_count: 3,
    ...over,
  };
}

function request(over: Partial<HistoryRequest> = {}): HistoryRequest {
  return {
    id: 'api_1', response_id: null, client_protocol: 'responses', client_model: 'm', backend: null, resolved_model: null,
    status: 'completed', created_at_ms: 1, completed_at_ms: 2, first_token_at_ms: null,
    input_tokens: null, output_tokens: null, cached_tokens: null, error: null, client_label: null,
    harness: 'codex', harness_version: null, session_id: 's', chain_parent_request_id: null,
    item_count: null, shared_prefix_items: null, divergence_kind: null, divergence_index: null, cache_bust: null,
    ...over,
  };
}

describe('sessionsModel', () => {
  it('labels a node by its declared id, compacting long ids', () => {
    expect(sessionLabel(node({ external_id: 'codex-1' }))).toBe('codex-1');
    expect(sessionLabel(node({ external_id: '11111111-1111-4111-8111-111111111111' }))).toBe('11111111…111111');
    expect(sessionLabel(node())).toBe('a1b2c3d4…3b4c5d');
  });

  it('badges declared vs inferred nodes', () => {
    expect(kindBadge(node()).inferred).toBe(false);
    expect(kindBadge(node({ kind: 'inferred' }))).toMatchObject({ label: 'inferred', inferred: true });
  });

  it('formats ages in the coarsest sensible unit', () => {
    const now = 1_000_000_000;
    expect(fmtAge(now - 12_000, now)).toBe('12s');
    expect(fmtAge(now - 4 * 60_000, now)).toBe('4m');
    expect(fmtAge(now - 3 * 3_600_000, now)).toBe('3h');
    expect(fmtAge(now - 2 * 86_400_000, now)).toBe('2d');
    expect(fmtAge(now + 5_000, now)).toBe('0s');
  });

  it('rolls up busts, kinds and reported tokens; unreported tokens stay unavailable', () => {
    const empty = rollupRequests([]);
    expect(empty).toMatchObject({ count: 0, busts: 0, inputTokens: null, quality: 'unavailable' });

    const rollup = rollupRequests([
      request({ divergence_kind: 'new_chain', cache_bust: false }),
      request({ divergence_kind: 'append', cache_bust: false, input_tokens: 100, output_tokens: 10, cached_tokens: 80 }),
      request({ divergence_kind: 'tools_changed', cache_bust: true, input_tokens: 200, output_tokens: 20 }),
    ]);
    expect(rollup.count).toBe(3);
    expect(rollup.busts).toBe(1);
    expect(rollup.kinds).toEqual({ new_chain: 1, append: 1, tools_changed: 1 });
    expect(rollup.inputTokens).toBe(300);
    expect(rollup.outputTokens).toBe(30);
    expect(rollup.cachedTokens).toBe(80);
    expect(rollup.quality).toBe('derived');
    // No request reported usage ⇒ null, never 0.
    expect(rollupRequests([request()]).inputTokens).toBeNull();
  });

  it('classifies divergence kinds into bust / non-bust labels', () => {
    expect(divergenceLabel('append')).toMatchObject({ label: 'append', bust: false });
    expect(divergenceLabel('new_chain')).toMatchObject({ label: 'new', bust: false });
    expect(divergenceLabel('instructions_changed').bust).toBe(true);
    expect(divergenceLabel('tools_changed').bust).toBe(true);
    expect(divergenceLabel('history_rewritten').bust).toBe(true);
    expect(divergenceLabel(null)).toMatchObject({ label: '—', bust: false });
  });

  it('sorts by last activity, newest first, id as tiebreak', () => {
    const sorted = sortByActivity([node({ id: 'b', last_seen_ms: 5 }), node({ id: 'a', last_seen_ms: 5 }), node({ id: 'c', last_seen_ms: 9 })]);
    expect(sorted.map((n) => n.id)).toEqual(['c', 'a', 'b']);
  });
});
