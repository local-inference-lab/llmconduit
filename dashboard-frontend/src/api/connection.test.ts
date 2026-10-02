import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { getConnection, resetConnection, teardownSession, queryKeys } from './connection';
import { DashboardClient } from './client';
import { mockKillLog, buildMonitorFrame } from './mock';
import { authStore } from '../store/authStore';
import { dashboardStore } from '../store/dashboardStore';

function clearCsrfCookie(): void {
  document.cookie = 'llmconduit_csrf=; expires=Thu, 01 Jan 1970 00:00:00 GMT';
}

describe('connection — CSRF resolved dynamically (cookie-first) on kill (finding 2)', () => {
  beforeEach(() => {
    resetConnection();
    mockKillLog.length = 0;
    clearCsrfCookie();
    authStore.getState().setCsrfToken(null);
  });
  afterEach(() => {
    resetConnection();
    clearCsrfCookie();
  });

  it('a token issued AFTER boot (fresh login cookie) reaches the kill POST', async () => {
    const { client } = getConnection();
    // Simulate a fresh login setting the double-submit cookie AFTER the connection booted.
    document.cookie = 'llmconduit_csrf=fresh-login-token';
    const res = await client.kill('api_001');
    expect(res.killed).toBe(true);
    // The kill carried the COOKIE token, not a stale bootstrap value.
    expect(mockKillLog.at(-1)?.csrf).toBe('fresh-login-token');
  });

  it('falls back to the auth-store token when no cookie is present', async () => {
    const { client } = getConnection();
    authStore.getState().setCsrfToken('store-token');
    const res = await client.kill('api_002');
    expect(res.killed).toBe(true);
    expect(mockKillLog.at(-1)?.csrf).toBe('store-token');
  });
});

describe('connection — WS-driven REST invalidation (finding 10)', () => {
  beforeEach(() => {
    resetConnection();
    dashboardStore.getState().reset();
  });
  afterEach(() => resetConnection());

  it('an ACCEPTED flow frame invalidates the flows query', () => {
    const { socket, queryClient } = getConnection();
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    // Prime snapshot so live frames apply.
    socket.handleParsed({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [], metrics: null, topology: null,
    });
    socket.applyFrame({
      domain: 'flow', seq: 1,
      batch: [{ type: 'flow_status', api_call_id: 'api_r1', status: 'open', model_served: 'm', upstream_target: 'u', usage: null, started_ms: 1000, elapsed_ms: 5 }],
    });
    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.flows });
  });

  it('a metrics frame invalidates BOTH /metrics AND /topology (gap 13: per_provider joins the m1 window)', () => {
    const { socket, queryClient } = getConnection();
    socket.handleParsed({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [], metrics: null, topology: null,
    });
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    // A minimal valid `metric_tick` (the per-domain validator requires the full shape).
    const w = {
      reqs_per_sec: 1, active_streams: 0, error_pct: 0, p50: 10, p95: 20, p99: 30,
      tokens_per_sec: 5, prefill_tokens_per_sec: 1000, decode_tokens_per_sec: 50,
      cost_per_min: 0, samples: 1, usage_samples: 1, prefill_samples: 1,
      decode_samples: 1, priced_samples: 1,
      cost_confidence: 'estimated' as const,
    };
    socket.applyFrame({
      domain: 'metrics', seq: 1,
      batch: [{ type: 'metric_tick', ...w, windows: { m1: w, m5: w, h1: w } }],
    });
    // The metrics tick must refresh the metrics tiles AND the REST /topology per_provider join —
    // otherwise the per-provider tile/node emphasis stay STALE until an unrelated topology change.
    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.metrics });
    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.topology });
  });

  it('a DROPPED (duplicate) frame does NOT invalidate', () => {
    const { socket, queryClient } = getConnection();
    socket.handleParsed({
      type: 'snapshot',
      cursors: { flow_seq: 5, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [], metrics: null, topology: null,
    });
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    // seq 5 <= cursor 5 → dropped, no invalidation.
    socket.applyFrame({
      domain: 'flow', seq: 5,
      batch: [{ type: 'flow_status', api_call_id: 'api_r1', status: 'open', model_served: 'm', upstream_target: 'u', usage: null, started_ms: 1000, elapsed_ms: 5 }],
    });
    expect(spy).not.toHaveBeenCalled();
  });

  it('a monitor frame does NOT invalidate any REST query (no mirror)', () => {
    const { socket, queryClient } = getConnection();
    socket.handleParsed({
      type: 'snapshot',
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
      flows: [], metrics: null, topology: null,
    });
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    socket.applyFrame(buildMonitorFrame(6));
    expect(spy).not.toHaveBeenCalled();
  });
});

describe('teardownSession — clears cache + resets stores + disconnects WS (finding 1)', () => {
  beforeEach(() => resetConnection());
  afterEach(() => resetConnection());

  it('clears the query cache, resets both stores, and disconnects the socket', () => {
    const { socket, queryClient } = getConnection();
    // Seed session-scoped state: a cached query, live store data, and auth secrets.
    queryClient.setQueryData(queryKeys.flows, { flows: [], total: 0, flow_seq: 1 });
    socket.handleParsed({
      type: 'snapshot',
      cursors: { flow_seq: 1, metrics_seq: 0, topology_seq: 0, monitor_seq: 5 },
      flows: [], metrics: null, topology: null,
    });
    socket.applyFrame(buildMonitorFrame(6));
    authStore.getState().setAuthenticated(true);
    authStore.getState().setCsrfToken('secret-token');
    authStore.getState().setMutationsEnabled(true);
    const clearSpy = vi.spyOn(queryClient, 'clear');
    const disconnectSpy = vi.spyOn(socket, 'disconnect');

    teardownSession();

    // REST cache cleared (no leaked bodies/usage across sessions).
    expect(clearSpy).toHaveBeenCalledOnce();
    expect(queryClient.getQueryData(queryKeys.flows)).toBeUndefined();
    // WS disconnected.
    expect(disconnectSpy).toHaveBeenCalledOnce();
    // Live store reset.
    expect(dashboardStore.getState().monitor).toHaveLength(0);
    expect(dashboardStore.getState().flows.size).toBe(0);
    expect(dashboardStore.getState().cursors.monitor_seq).toBe(0);
    // Auth store fully reset (token + mutation flag cleared, not just `authenticated`).
    expect(authStore.getState().authenticated).toBe(false);
    expect(authStore.getState().csrfToken).toBeNull();
    expect(authStore.getState().mutationsEnabled).toBe(false);
  });

  it('a real 401 from a client read routes through teardownSession (wired onUnauthorized)', async () => {
    // Build a client wired with the SAME onUnauthorized the connection uses
    // (teardownSession), against a fetch that 401s — then assert teardown ran.
    const { queryClient } = getConnection();
    queryClient.setQueryData(queryKeys.metrics, { metrics_seq: 1 });
    authStore.getState().setAuthenticated(true);
    authStore.getState().setCsrfToken('secret');
    const fetch401: typeof globalThis.fetch = async () => new Response('no', { status: 401 });
    const client = new DashboardClient({ fetchImpl: fetch401, onUnauthorized: teardownSession });
    await expect(client.metrics()).rejects.toBeTruthy(); // 401 → UnauthorizedError
    // The 401 fired onUnauthorized === teardownSession: cache cleared, auth reset.
    expect(queryClient.getQueryData(queryKeys.metrics)).toBeUndefined();
    expect(authStore.getState().authenticated).toBe(false);
    expect(authStore.getState().csrfToken).toBeNull();
  });
});
