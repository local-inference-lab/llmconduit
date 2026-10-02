import type {
  AuthPolicy,
  AuthPolicyTimeWindow,
  CostConfidence,
  FlowSummary,
  ModelPrice,
  ProviderCacheMetrics,
  ProviderHealth,
  ProviderInventoryEntry,
  ProviderLatency,
  TopologyEdge,
} from '../../api/types';

export type ProviderQuality = 'measured' | 'derived' | 'estimated' | 'unavailable';

export interface ProviderUsageSummary {
  requests: number;
  failures: number;
  active: number;
  promptTokens: number | null;
  completionTokens: number | null;
  cachedTokens: number | null;
  reasoningTokens: number | null;
  cost: number | null;
  costConfidence: CostConfidence;
}

export interface ProviderPolicySummary {
  policyIds: string[];
  allowCount: number;
  denyCount: number;
  windows: string[];
  limits: string[];
  subjects: string[];
  requestedModels: string[];
  servedModels: string[];
  endpoints: string[];
}

export interface ProviderInventoryRow {
  key: string;
  id: string;
  name: string;
  resourceId: string | null;
  route: string | null;
  baseUrl: string;
  status: ProviderHealth['status'];
  lastError: string | null;
  cooldownUntilMs: number | null;
  catalogFetchedMs: number | null;
  catalogSize: number | null;
  advertisedModels: string[];
  observedModels: string[];
  contextWindows: Array<{ model: string; contextLimit: number | null }>;
  availability: {
    timezone: string | null;
    summary: string[];
    defaultCapacity: number | null;
  };
  capacity: {
    limit: number | null;
    active: number | null;
    accepting: boolean;
    healthy: boolean;
  };
  policy: ProviderPolicySummary;
  usage: ProviderUsageSummary;
  perProvider: ProviderLatency | null;
  cacheMetrics: ProviderCacheMetrics | null;
  edge: TopologyEdge | null;
  priceCoverage: {
    priced: number;
    total: number;
  };
}

export interface ProviderInventorySummary {
  providers: number;
  healthy: number;
  cooling: number;
  down: number;
  advertisedModels: number;
  requests: number;
  active: number;
  cost: number | null;
  costConfidence: CostConfidence;
}

export interface ProviderInventory {
  rows: ProviderInventoryRow[];
  summary: ProviderInventorySummary;
  unionCatalogModels: Array<{ id: string; contextLimit: number | null; priced: boolean }>;
}

export function groupProviderSlots(rows: ProviderInventoryRow[]): Array<{ name: string; rows: ProviderInventoryRow[] }> {
  const groups = new Map<string, ProviderInventoryRow[]>();
  for (const row of rows) {
    const name = row.name.trim() || row.id;
    const slots = groups.get(name);
    if (slots) slots.push(row);
    else groups.set(name, [row]);
  }
  return [...groups.entries()]
    .map(([name, slots]) => ({ name, rows: slots }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

const DASH = '—';
const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] as const;

export function buildProviderInventory(input: {
  providers: ProviderInventoryEntry[];
  health: ProviderHealth[];
  edges: TopologyEdge[];
  flows: FlowSummary[];
  policies: AuthPolicy[];
  cacheMetrics: ProviderCacheMetrics[];
  priceTable: Record<string, ModelPrice>;
  perProviderById?: Record<string, ProviderLatency>;
}): ProviderInventory {
  const union = new Map<string, number | null>();
  for (const provider of input.providers) {
    for (const model of provider.models) union.set(model.id, model.context_limit ?? null);
  }
  const unionCatalogModels = [...union.entries()]
    .map(([id, contextLimit]) => ({ id, contextLimit, priced: Boolean(input.priceTable[id]) }))
    .sort((a, b) => a.id.localeCompare(b.id));
  const healthById = new Map(input.health.map((node) => [node.id, node]));
  const cacheByProvider = new Map(input.cacheMetrics.map((metrics) => [metrics.provider, metrics]));
  const rows = input.providers.map((provider) => {
    const health = healthById.get(provider.provider_id);
    const providerFlows = input.flows.filter((flow) => flow.upstream_target === provider.provider_id || flow.upstream_target === provider.route);
    const policy = summarizePoliciesForProvider(provider.provider_id, provider.route, provider.resource_id, input.policies);
    const observedModels = sortedUnique(providerFlows.flatMap((flow) => [flow.model_served, flow.model_requested]));
    const advertisedModels = provider.models.map((model) => model.id).sort();
    const contextWindows = provider.models
      .slice()
      .sort((a, b) => a.id.localeCompare(b.id))
      .slice(0, 12)
      .map((model) => ({ model: model.id, contextLimit: model.context_limit ?? null }));
    const usage = summarizeUsage(providerFlows);
    return {
      key: providerKey(provider),
      id: provider.provider_id,
      name: provider.provider_name,
      resourceId: provider.resource_id,
      route: provider.route,
      baseUrl: provider.base_url,
      status: provider.healthy ? (health?.status ?? 'healthy') : 'down',
      lastError: health?.last_error ?? null,
      cooldownUntilMs: health?.cooling_until_ms ?? null,
      catalogFetchedMs: health?.catalog_fetched_ms ?? null,
      catalogSize: provider.models.length,
      advertisedModels,
      observedModels,
      contextWindows,
      availability: summarizeAvailability(provider.availability),
      capacity: {
        limit: provider.capacity_limit,
        active: provider.active_requests,
        accepting: provider.accepting_requests,
        healthy: provider.healthy,
      },
      policy,
      usage,
      perProvider: input.perProviderById?.[provider.provider_id] ?? health?.per_provider ?? null,
      cacheMetrics: cacheByProvider.get(provider.provider_id)
        ?? (provider.route ? cacheByProvider.get(provider.route) : undefined)
        ?? (provider.resource_id ? cacheByProvider.get(provider.resource_id) : undefined)
        ?? null,
      edge: input.edges.find((edge) => edge.to === provider.provider_id) ?? null,
      priceCoverage: {
        priced: advertisedModels.filter((model) => Boolean(input.priceTable[model])).length,
        total: advertisedModels.length,
      },
    } satisfies ProviderInventoryRow;
  }).sort((a, b) => providerStatusRank(a.status) - providerStatusRank(b.status) || b.usage.requests - a.usage.requests || a.id.localeCompare(b.id));

  // Provider-level telemetry is repeated for each advertised resource. Summarize the
  // underlying calls once so multi-resource providers do not inflate the headline.
  const providerTargets = new Set(input.providers.flatMap((provider) => [provider.provider_id, provider.route].filter((value): value is string => Boolean(value))));
  const summaryFlows = [...new Map(input.flows
    .filter((flow) => flow.upstream_target != null && providerTargets.has(flow.upstream_target))
    .map((flow) => [flow.api_call_id, flow])).values()];
  const summaryUsage = summarizeUsage(summaryFlows);
  const summary: ProviderInventorySummary = {
    providers: rows.length,
    healthy: rows.filter((row) => row.status === 'healthy').length,
    cooling: rows.filter((row) => row.status === 'cooling').length,
    down: rows.filter((row) => row.status === 'down').length,
    advertisedModels: unionCatalogModels.length,
    requests: summaryUsage.requests,
    active: summaryUsage.active,
    cost: summaryUsage.cost,
    costConfidence: summaryUsage.costConfidence,
  };
  return { rows, summary, unionCatalogModels };
}

function summarizeUsage(flows: FlowSummary[]): ProviderUsageSummary {
  const usageRows = flows.filter((flow) => flow.usage);
  const costRows = flows.filter((flow) => flow.cost != null);
  return {
    requests: flows.length,
    failures: flows.filter((flow) => flow.status === 'failed').length,
    active: flows.filter((flow) => flow.status === 'open').length,
    promptTokens: sumOptional(usageRows.map((flow) => flow.usage?.prompt)),
    completionTokens: sumOptional(usageRows.map((flow) => flow.usage?.completion)),
    cachedTokens: sumOptional(usageRows.map((flow) => flow.usage?.cached)),
    reasoningTokens: sumOptional(usageRows.map((flow) => flow.usage?.reasoning)),
    cost: costRows.length > 0 ? costRows.reduce((sum, flow) => sum + (flow.cost ?? 0), 0) : null,
    costConfidence: foldCostConfidence(costRows.map((flow) => flow.cost_confidence)),
  };
}

function summarizePoliciesForProvider(provider: string, route: string | null, resourceId: string | null, policies: AuthPolicy[]): ProviderPolicySummary {
  const matching = policies.filter((policy) => policy.management_permissions.length === 0
    && policy.enabled
    && matchesScope(policy.providers, provider)
    && matchesRoute(policy.routes, route, resourceId));
  const windows = sortedUnique(matching.flatMap((policy) => policy.time_windows.map(formatTimeWindow)));
  return {
    policyIds: matching.map((policy) => policy.id),
    allowCount: matching.filter((policy) => policy.effect === 'allow').length,
    denyCount: matching.filter((policy) => policy.effect === 'deny').length,
    windows,
    limits: sortedUnique(matching.flatMap(formatPolicyLimits)),
    subjects: sortedUnique(matching.flatMap((policy) => policy.subjects)),
    requestedModels: sortedUnique(matching.flatMap((policy) => policy.requested_models)),
    servedModels: sortedUnique(matching.flatMap((policy) => policy.served_models)),
    endpoints: sortedUnique(matching.flatMap((policy) => policy.endpoints)),
  };
}

function summarizeAvailability(availability: ProviderInventoryEntry['availability']): ProviderInventoryRow['availability'] {
  if (!availability) return { timezone: null, summary: [], defaultCapacity: null };
  const weekly = availability.weekly.map((window) => {
    const days = window.days.length > 0 ? window.days.map(titleCase).join(', ') : 'All days';
    return `${days} ${window.start_local}-${window.end_local} cap ${window.capacity}`;
  });
  const exceptions = availability.exceptions.map((exception) => `${exception.start} - ${exception.end} cap ${exception.capacity}`);
  return {
    timezone: availability.timezone,
    summary: [...weekly, ...exceptions],
    defaultCapacity: availability.default_capacity,
  };
}

function matchesScope(matchers: string[], value: string): boolean {
  return matchers.length === 0 || matchers.includes('*') || matchers.includes(value);
}

function matchesRoute(routes: string[], route: string | null, resourceId: string | null): boolean {
  if (routes.length === 0 || routes.includes('*')) return true;
  return (route !== null && routes.includes(route))
    || (resourceId !== null && routes.includes(resourceId));
}

export function formatTimeWindow(window: AuthPolicyTimeWindow): string {
  const days = WEEKDAYS.filter((_, index) => (window.weekday_mask & (1 << index)) !== 0);
  const dayText = days.length === WEEKDAYS.length ? 'Every day' : days.join(', ');
  return `${dayText} ${minuteClock(window.start_minute)}-${minuteClock(window.end_minute)} UTC`;
}

function formatPolicyLimits(policy: AuthPolicy): string[] {
  const limits: string[] = [];
  if (policy.max_concurrent_sessions !== null) limits.push(`${policy.max_concurrent_sessions} concurrent sessions`);
  if (policy.max_daily_session_starts !== null) limits.push(`${policy.max_daily_session_starts} daily starts`);
  return limits;
}

function minuteClock(minute: number): string {
  const h = Math.floor(minute / 60);
  const m = minute % 60;
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
}

function sumOptional(values: Array<number | null | undefined>): number | null {
  const present = values.filter((value): value is number => value !== null && value !== undefined && Number.isFinite(value));
  return present.length > 0 ? present.reduce((sum, value) => sum + value, 0) : null;
}

function sortedUnique(values: Array<string | null | undefined>): string[] {
  return [...new Set(values.filter((value): value is string => Boolean(value)))].sort();
}

function foldCostConfidence(confidences: CostConfidence[]): CostConfidence {
  if (confidences.length === 0) return 'unavailable';
  if (confidences.includes('estimated')) return 'estimated';
  if (confidences.includes('confident')) return 'confident';
  return 'unavailable';
}

function providerStatusRank(status: ProviderHealth['status']): number {
  if (status === 'down') return 0;
  if (status === 'cooling') return 1;
  return 2;
}

function titleCase(value: string): string {
  return value ? `${value.slice(0, 1).toUpperCase()}${value.slice(1)}` : value;
}

function providerKey(provider: ProviderInventoryEntry): string {
  return `${provider.provider_id}/${provider.resource_id ?? provider.route ?? provider.provider_name}`;
}

export function costQuality(confidence: CostConfidence): ProviderQuality {
  if (confidence === 'unavailable') return 'unavailable';
  if (confidence === 'estimated') return 'estimated';
  return 'measured';
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return DASH;
  if (value === 0) return '0%';
  return `${value < 10 ? value.toFixed(1) : Math.round(value)}%`;
}
