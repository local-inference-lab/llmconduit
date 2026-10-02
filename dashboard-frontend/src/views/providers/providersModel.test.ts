import { describe, expect, it } from 'vitest';
import type { AuthPolicy, FlowSummary, ProviderHealth, ProviderInventoryEntry, TopologyEdge } from '../../api/types';
import { buildProviderInventory, formatTimeWindow, groupProviderSlots } from './providersModel';

const provider = (id: string, status: ProviderHealth['status'] = 'healthy'): ProviderHealth => ({
  id,
  name: id,
  route: id === 'openai' ? 'cloud' : null,
  base_url: `https://${id}.example.test`,
  status,
  cooling_until_ms: null,
  last_error: null,
  served_count: 0,
  failover_count: 0,
  consecutive_failures: 0,
  catalog_fetched_ms: 1000,
  catalog_size: 2,
});

const inventoryProvider = (id: string, models = ['gpt-4.1']): ProviderInventoryEntry => ({
  provider_id: id,
  provider_name: id,
  resource_id: id,
  route: id === 'openai' ? 'cloud' : null,
  base_url: `https://${id}.example.test`,
  models: models.map((model) => ({ id: model, context_limit: model === 'gpt-4.1' ? 128000 : model === 'qwen' ? 32768 : null })),
  availability: null,
  capacity_limit: 4,
  active_requests: 1,
  accepting_requests: true,
  healthy: true,
});

const flow = (id: string, upstream: string, model: string, overrides: Partial<FlowSummary> = {}): FlowSummary => ({
  api_call_id: id,
  method: 'POST',
  uri: '/v1/chat/completions',
  model_requested: model,
  model_served: model,
  upstream_target: upstream,
  usage: { prompt: 100, completion: 40, total: 140, cached: 10 },
  status: 'completed',
  started_ms: 1000,
  cost: 0.02,
  cost_confidence: 'confident',
  ...overrides,
});

const policy = (overrides: Partial<AuthPolicy>): AuthPolicy => ({
  id: 'pol',
  name: 'Policy',
  effect: 'allow',
  enabled: true,
  subjects: ['grp_prod'],
  endpoints: ['chat'],
  models: [],
  requested_models: ['gpt-*'],
  served_models: ['gpt-4.1'],
  providers: ['openai'],
  routes: [],
  time_windows: [{ weekday_mask: 31, start_minute: 480, end_minute: 1080, absolute_start_ms: null, absolute_end_ms: null }],
  max_concurrent_sessions: 4,
  max_daily_session_starts: null,
  management_permissions: [],
  ...overrides,
});

describe('providersModel', () => {
  it('keeps an unhealthy slot down when its provider connection is healthy', () => {
    const inventory = buildProviderInventory({
      providers: [{ ...inventoryProvider('mesh:lab'), healthy: false, accepting_requests: false }],
      health: [provider('mesh:lab')], edges: [], flows: [], policies: [], cacheMetrics: [], priceTable: {},
    });
    expect(inventory.rows[0]!.status).toBe('down');
    expect(inventory.summary.down).toBe(1);
  });

  it('groups by display name while retaining distinct slot identities and order', () => {
    const inventory = buildProviderInventory({
      providers: [
        { ...inventoryProvider('node-c'), provider_name: 'South lab', resource_id: 'gpu-c' },
        { ...inventoryProvider('node-b'), provider_name: 'North lab', resource_id: 'gpu-b' },
        { ...inventoryProvider('node-a'), provider_name: ' North lab ', resource_id: 'gpu-a' },
        { ...inventoryProvider('unnamed'), provider_name: '  ', resource_id: null },
      ],
      health: [], edges: [], flows: [], policies: [], cacheMetrics: [], priceTable: {},
    });
    const original = [...inventory.rows];
    const groups = groupProviderSlots(inventory.rows);

    expect(groups.map((group) => group.name)).toEqual(['North lab', 'South lab', 'unnamed']);
    expect(groups[0]!.rows).toEqual(inventory.rows.filter((row) => row.name.trim() === 'North lab'));
    expect(groups[0]!.rows.map((row) => row.id)).toEqual(['node-a', 'node-b']);
    expect(inventory.rows).toEqual(original);
    expect(groupProviderSlots([])).toEqual([]);
  });

  it('joins provider health, observed usage, policy windows, limits, and pricing', () => {
    const edges: TopologyEdge[] = [{ from: 'gateway', to: 'openai', throughput: 0.4, tokens_per_sec: 12, cost_per_sec: 0.01 }];
    const inventory = buildProviderInventory({
      providers: [{
        ...inventoryProvider('openai', ['gpt-4.1']),
        availability: {
          timezone: 'UTC',
          default_capacity: 0,
          weekly: [{ days: ['monday', 'tuesday'], start_local: '08:00', end_local: '18:00', capacity: 4 }],
          exceptions: [],
        },
      }],
      health: [provider('openai')],
      edges,
      flows: [
        flow('a', 'openai', 'gpt-4.1'),
        flow('b', 'openai', 'gpt-4.1', { status: 'failed', usage: null, cost: null, cost_confidence: 'unavailable' }),
      ],
      policies: [policy({ id: 'pol_prod' })],
      cacheMetrics: [{ provider: 'openai', source: 'vllm', fetched_at_ms: 1000, cache_hits: 7, cache_queries: 10, cache_hit_rate: 0.7, kv_cache_usage: 0.25, data_quality: 'derived' }],
      priceTable: { 'gpt-4.1': { input_per_1k: 0.002, output_per_1k: 0.008, cached_per_1k: 0, cached_price_configured: false } },
      perProviderById: {
        openai: { provider: 'openai', data_quality: 'derived', samples: 2, served: 1, failed: 1, p50: 80, p95: 120, p99: 140, error_rate: 50, errors: { http_status: 1 } },
      },
    });

    expect(inventory.summary.providers).toBe(1);
    expect(inventory.summary.requests).toBe(2);
    expect(inventory.summary.cost).toBe(0.02);
    const row = inventory.rows[0]!;
    expect(row.advertisedModels).toEqual(['gpt-4.1']);
    expect(row.contextWindows).toContainEqual({ model: 'gpt-4.1', contextLimit: 128000 });
    expect(row.availability.summary).toEqual(['Monday, Tuesday 08:00-18:00 cap 4']);
    expect(row.policy.windows).toEqual(['Mon, Tue, Wed, Thu, Fri 08:00-18:00 UTC']);
    expect(row.policy.limits).toEqual(['4 concurrent sessions']);
    expect(row.usage.failures).toBe(1);
    expect(row.perProvider?.error_rate).toBe(50);
    expect(row.cacheMetrics?.cache_hit_rate).toBe(0.7);
    expect(row.edge).toBe(edges[0]);
    expect(row.priceCoverage).toEqual({ priced: 1, total: 1 });
  });

  it('uses exact advertised provider models even before any flow is observed', () => {
    const inventory = buildProviderInventory({
      providers: [inventoryProvider('fresh-local', ['llama', 'qwen'])],
      health: [provider('fresh-local')],
      edges: [],
      flows: [],
      policies: [],
      cacheMetrics: [],
      priceTable: {},
    });

    expect(inventory.rows[0]!.advertisedModels).toEqual(['llama', 'qwen']);
    expect(inventory.rows[0]!.contextWindows).toEqual([
      { model: 'llama', contextLimit: null },
      { model: 'qwen', contextLimit: 32768 },
    ]);
    expect(inventory.rows[0]!.usage.cost).toBeNull();
  });

  it('formats weekly UTC windows without implying local time', () => {
    expect(formatTimeWindow({ weekday_mask: 0x7f, start_minute: 0, end_minute: 1440, absolute_start_ms: null, absolute_end_ms: null })).toBe('Every day 00:00-24:00 UTC');
  });

  it('matches access policies by provider id, route, or resource id', () => {
    const inventory = buildProviderInventory({
      providers: [{ ...inventoryProvider('mesh:node-a', ['llama']), route: 'gpu-a', resource_id: 'res-a' }],
      health: [],
      edges: [],
      flows: [],
      policies: [
        policy({ id: 'by_route', providers: [], routes: ['gpu-a'], subjects: ['grp_route'] }),
        policy({ id: 'by_resource', providers: [], routes: ['res-a'], subjects: ['grp_resource'] }),
        policy({ id: 'other_route', providers: [], routes: ['gpu-b'], subjects: ['grp_other'] }),
      ],
      cacheMetrics: [],
      priceTable: {},
    });

    expect(inventory.rows[0]!.key).toBe('mesh:node-a/res-a');
    expect(inventory.rows[0]!.policy.policyIds).toEqual(['by_route', 'by_resource']);
    expect(inventory.rows[0]!.policy.subjects).toEqual(['grp_resource', 'grp_route']);
  });

  it('does not double-count provider-level flows across multiple resources', () => {
    const first = { ...inventoryProvider('mesh:node-a'), resource_id: 'gpu-a' };
    const second = { ...inventoryProvider('mesh:node-a'), resource_id: 'gpu-b' };
    const inventory = buildProviderInventory({
      providers: [first, second],
      health: [],
      edges: [],
      flows: [flow('call-1', first.provider_id, 'gpt-4.1', { cost: 0.25 })],
      policies: [],
      cacheMetrics: [],
      priceTable: {},
    });

    expect(inventory.rows).toHaveLength(2);
    expect(inventory.summary.requests).toBe(1);
    expect(inventory.summary.cost).toBe(0.25);
  });
});
