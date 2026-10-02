import { describe, it, expect, beforeEach, vi } from 'vitest';
import { DashboardClient, UnauthorizedError, readCsrfCookie } from './client';
import { mockFetch, mockKillLog } from './mock';

describe('DashboardClient — kill includes X-CSRF-Token', () => {
  beforeEach(() => {
    mockKillLog.length = 0;
  });

  it('attaches the CSRF token header on the kill POST (via the mock backend)', async () => {
    const client = new DashboardClient({
      fetchImpl: mockFetch,
      getCsrfToken: () => 'mock-csrf-token',
    });
    // `:id` == api_call_id (D13 contract).
    const res = await client.kill('api_001');
    expect(res.killed).toBe(true);
    expect(mockKillLog).toHaveLength(1);
    expect(mockKillLog[0]).toEqual({ id: 'api_001', csrf: 'mock-csrf-token' });
  });

  it('mock backend rejects a kill with no CSRF token (403)', async () => {
    const client = new DashboardClient({
      fetchImpl: mockFetch,
      getCsrfToken: () => null,
    });
    await expect(client.kill('api_001')).rejects.toThrow(/403/);
    expect(mockKillLog[0]?.csrf).toBeNull();
  });

  it('mock backend 404s a kill for an unknown api_call_id (finding 7)', async () => {
    const client = new DashboardClient({
      fetchImpl: mockFetch,
      getCsrfToken: () => 'mock-csrf-token',
    });
    // A response_id is NOT a valid kill key — `:id` must be api_call_id.
    await expect(client.kill('resp_001')).rejects.toThrow(/404/);
  });
});

describe('DashboardClient — 401 bounce-to-login', () => {
  it('fires onUnauthorized and throws UnauthorizedError on any 401', async () => {
    const onUnauthorized = vi.fn();
    const fetch401: typeof globalThis.fetch = async () =>
      new Response('nope', { status: 401 });
    const client = new DashboardClient({
      fetchImpl: fetch401,
      onUnauthorized,
    });
    await expect(client.flows()).rejects.toBeInstanceOf(UnauthorizedError);
    expect(onUnauthorized).toHaveBeenCalledOnce();
  });
});

describe('DashboardClient — typed reads against the D13 shapes (mock)', () => {
  it('sends the selected reasoning and sampling controls to the chat route', async () => {
    let sent: Record<string, unknown> = {};
    const fetchChat: typeof fetch = async (_input, init) => {
      sent = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response('data: [DONE]\n\n', { status: 200, headers: { 'Content-Type': 'text/event-stream' } });
    };
    const client = new DashboardClient({ fetchImpl: fetchChat, getCsrfToken: () => 'test-csrf' });

    await client.streamChat({
      model: 'reasoning-model',
      messages: [{ role: 'user', content: 'think' }],
      reasoning_effort: 'xhigh',
      top_p: 0.85,
      max_tokens: 8192,
    }, () => undefined);

    expect(sent).toMatchObject({ reasoning_effort: 'xhigh', top_p: 0.85, max_tokens: 8192, stream: true });
  });

  it('sends strict instance counts for Fleet and mesh load requests only when requested', async () => {
    const bodies: Array<Record<string, unknown> | undefined> = [];
    const fetchMutations: typeof fetch = async (input, init) => {
      const path = String(input);
      bodies.push(init?.body === undefined ? undefined : JSON.parse(String(init.body)) as Record<string, unknown>);
      if (path.includes('/fleet/')) return new Response(JSON.stringify({ changed: true }), { status: 200 });
      return new Response(JSON.stringify({ endpoint_id: 'vllm-a', model_id: 'qwen', accepted: true, changed: true }), { status: 200 });
    };
    const client = new DashboardClient({ fetchImpl: fetchMutations, getCsrfToken: () => 'test-csrf' });

    await client.loadFleetModel('qwen', 2);
    await client.loadMeshModel('vllm-a', 'qwen', 3);
    await client.loadFleetModel('qwen');

    expect(bodies).toEqual([{ instances: 2 }, { instances: 3 }, undefined]);
  });

  it('streams chat deltas, usage, and the terminal reason through the dashboard route', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch, getCsrfToken: () => 'test-csrf' });
    const deltas: Array<{ kind: 'content' | 'reasoning'; text: string }> = [];
    const result = await client.streamChat(
      { model: 'gpt-4o', messages: [{ role: 'user', content: 'ping' }] },
      (delta) => deltas.push(delta),
    );

    expect(deltas.filter((delta) => delta.kind === 'reasoning').map((delta) => delta.text).join('')).toBe('**Checking** the request.');
    expect(deltas.filter((delta) => delta.kind === 'content').map((delta) => delta.text).join('')).toBe('Mock response from gpt-4o: ping');
    expect(result).toMatchObject({ model: 'gpt-4o', finishReason: 'stop' });
    expect(result.usage?.total_tokens).toBe(20);
  });

  it('reports a stream that closes without the terminal marker', async () => {
    const fetchIncomplete: typeof fetch = async () => new Response(
      'data: {"choices":[{"delta":{"content":"partial"},"finish_reason":null}]}\n\n',
      { status: 200, headers: { 'Content-Type': 'text/event-stream' } },
    );
    const client = new DashboardClient({ fetchImpl: fetchIncomplete, getCsrfToken: () => 'test-csrf' });
    const deltas: Array<{ kind: 'content' | 'reasoning'; text: string }> = [];

    await expect(client.streamChat(
      { model: 'gpt-4o', messages: [{ role: 'user', content: 'ping' }] },
      (delta) => deltas.push(delta),
    )).rejects.toThrow(/terminal \[DONE\]/);
    expect(deltas.map((delta) => delta.text).join('')).toBe('partial');
  });

  it('flows() returns the cursor-bearing FlowsResponse', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch });
    const res = await client.flows();
    expect(typeof res.flow_seq).toBe('number');
    expect(Array.isArray(res.flows)).toBe(true);
  });

  it('catalog() returns a BARE array (no cursor) with a NULLABLE context_limit', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch });
    const cat = await client.catalog();
    expect(Array.isArray(cat)).toBe(true);
    const first = cat[0];
    expect(first).toBeDefined();
    expect(first).toHaveProperty('context_limit');
    // gap 06: a real window surfaces as a number...
    expect(typeof first?.context_limit).toBe('number');
    // ...and a model with no advertised window surfaces as `null` (unavailable),
    // NEVER a non-null `0` (the lie-with-zeros the gap removed).
    const unavailable = cat.find((e) => e.id === 'mystery-model');
    expect(unavailable).toBeDefined();
    expect(unavailable?.context_limit ?? null).toBeNull();
    expect(unavailable?.context_limit).not.toBe(0);
  });

  it('providers() and providerMetrics() return exact provider inventory and cache samples', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch });
    const providers = await client.providers();
    expect(providers.providers.find((provider) => provider.provider_id === 'vllm-a')?.models.map((model) => model.id)).toContain('llama-3.1-70b');
    expect(providers.providers.find((provider) => provider.provider_id === 'vllm-a')?.availability?.timezone).toBe('America/Chicago');

    const metrics = await client.providerMetrics();
    const vllm = metrics.providers.find((provider) => provider.provider === 'vllm-a');
    expect(vllm?.source).toBe('vllm');
    expect(vllm?.cache_hit_rate).toBeGreaterThan(0);
  });

  it('manages additional providers without returning their API keys', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch, getCsrfToken: () => 'test-csrf' });
    const initial = await client.configuredProviders();
    expect(initial.providers[0]).not.toHaveProperty('api_key');
    expect(initial.providers[0]).toMatchObject({ auto_discover: true, allowed_models: null, disabled_models: [] });

    const created = await client.createConfiguredProvider({
      name: 'Test provider',
      base_url: 'https://inference.example/v1',
      api_key: 'secret-value',
    });
    expect(created).toMatchObject({ name: 'Test provider', api_key_present: true, auto_discover: true, allowed_models: null, disabled_models: [] });
    expect(created).not.toHaveProperty('api_key');

    const disabledModel = created.models[0]!.id;
    const patched = await client.updateConfiguredProvider(created.id, {
      auto_discover: false,
      allowed_models: [disabledModel],
      disabled_models: [disabledModel],
    });
    expect(patched).toMatchObject({ id: created.id, auto_discover: false, allowed_models: [disabledModel], disabled_models: [disabledModel] });

    const unrestricted = await client.updateConfiguredProvider(created.id, { allowed_models: null });
    expect(unrestricted.allowed_models).toBeNull();

    await client.deleteConfiguredProvider(created.id);
    expect((await client.configuredProviders()).providers.some((provider) => provider.id === created.id)).toBe(false);
  });

  it('rejects configured-provider PATCHes without CSRF and validates model ids', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch, getCsrfToken: () => null });
    const provider = (await client.configuredProviders()).providers[0]!;

    await expect(client.updateConfiguredProvider(provider.id, { auto_discover: false })).rejects.toThrow(/403/);

    const authed = new DashboardClient({ fetchImpl: mockFetch, getCsrfToken: () => 'test-csrf' });
    await expect(authed.updateConfiguredProvider(provider.id, { disabled_models: ['not-advertised'] })).rejects.toThrow(/400/);
    await expect(authed.updateConfiguredProvider(provider.id, { allowed_models: ['not-advertised'] })).rejects.toThrow(/400/);
  });

  it('serializes configured-provider allowlist patches as omitted, null, or arrays', async () => {
    const bodies: unknown[] = [];
    const fetchMutations: typeof fetch = async (_input, init) => {
      bodies.push(init?.body === undefined ? undefined : JSON.parse(String(init.body)));
      return new Response(JSON.stringify({
        id: 'managed-local-lab',
        name: 'Local lab',
        base_url: 'http://127.0.0.1:8101/v1',
        api_key_present: true,
        auto_discover: true,
        allowed_models: null,
        disabled_models: [],
        models: [{ id: 'qwen3-8b-flash', context_limit: 32768 }],
      }), { status: 200 });
    };
    const client = new DashboardClient({ fetchImpl: fetchMutations, getCsrfToken: () => 'test-csrf' });

    await client.updateConfiguredProvider('managed-local-lab', { disabled_models: [] });
    await client.updateConfiguredProvider('managed-local-lab', { allowed_models: null });
    await client.updateConfiguredProvider('managed-local-lab', { allowed_models: ['qwen3-8b-flash'] });

    expect(bodies).toEqual([
      { disabled_models: [] },
      { allowed_models: null },
      { allowed_models: ['qwen3-8b-flash'] },
    ]);
  });

  it('mesh admin methods validate reads and send CSRF for mutations', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch, getCsrfToken: () => 'test-csrf' });
    const mesh = await client.mesh();
    expect(mesh.join_keys.length).toBeGreaterThan(0);
    expect(mesh.nodes.find((node) => node.endpoint_id === 'vllm-a')?.enabled).toBe(true);

    const created = await client.createMeshJoinKey({ label: 'fixture', max_uses: 1, expires_in_secs: 3600 });
    expect(created.token).toContain(created.join_key.id);

    const switched = await client.switchMeshModel('vllm-a', 'qwen3-32b');
    expect(switched).toMatchObject({ endpoint_id: 'vllm-a', model_id: 'qwen3-32b', accepted: true, changed: true });

    const unloaded = await client.unloadMeshModel('vllm-a', 'qwen3-8b-flash');
    expect(unloaded).toMatchObject({ endpoint_id: 'vllm-a', model_id: 'qwen3-8b-flash', accepted: true, changed: true });

    const loaded = await client.loadMeshModel('vllm-a', 'qwen3-8b-flash');
    expect(loaded).toMatchObject({ endpoint_id: 'vllm-a', model_id: 'qwen3-8b-flash', accepted: true, changed: true });

    const node = await client.setMeshNodeEnabled('vllm-a', false);
    expect(node).toMatchObject({ endpoint_id: 'vllm-a', enabled: false, evicted: true });

    const disabled = await client.setMeshModelDisabled({ endpoint_id: 'vllm-a', resource_id: 'gpu-a', model: 'llama-3.1-70b' }, true);
    expect(disabled.disabled).toBe(true);

    const revoked = await client.revokeMeshJoinKey(created.join_key.id);
    expect(revoked.updated).toBe(true);
  });
});

describe('DashboardClient — access management runtime validation', () => {
  it('validates management responses and sends CSRF for mutations', async () => {
    const client = new DashboardClient({ fetchImpl: mockFetch, getCsrfToken: () => 'test-csrf' });
    const summary = await client.authSummary();
    expect(summary.policy_epoch).toBeGreaterThan(0);
    const users = await client.authUsers();
    const created = await client.createAuthApiKey({ principal_id: users.users[0]!.id, name: 'test' });
    expect(created.raw_key).toMatch(/^llmc_/);
    const revoked = await client.revokeAuthApiKey(created.id);
    expect(revoked.api_keys.find((key) => key.id === created.id)?.enabled).toBe(false);
  });

  it('rejects an invalid management response before it reaches the UI', async () => {
    const fetchInvalid: typeof fetch = async () => new Response(JSON.stringify({ users: [{ id: 7 }] }), { status: 200 });
    const client = new DashboardClient({ fetchImpl: fetchInvalid });
    await expect(client.authUsers()).rejects.toThrow(/invalid response/);
  });

  it('accepts the backend audit outcome used for successful management events', async () => {
    const fetchAudit: typeof fetch = async () => new Response(JSON.stringify({
      events: [{
        id: '1',
        timestamp: '2026-09-20T12:06:19+00:00',
        actor: 'bootstrap',
        action: 'bootstrap.created',
        target: 'key_bootstrap',
        outcome: 'ok',
        metadata: {},
      }],
    }), { status: 200 });
    const client = new DashboardClient({ fetchImpl: fetchAudit });

    await expect(client.authAudit()).resolves.toMatchObject({
      events: [{ outcome: 'ok' }],
    });
  });
});

describe('readCsrfCookie', () => {
  it('reads the double-submit token from the non-HttpOnly cookie', () => {
    document.cookie = 'llmconduit_csrf=abc123';
    expect(readCsrfCookie()).toBe('abc123');
  });
});
