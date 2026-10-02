import type {
  AuthApiKey,
  AuthGroup,
  AuthPolicy,
  AuthRole,
  AuthSession,
  AuthUsageRow,
  AuthUser,
} from '../../api/types';

export type AccessSubjectKind = 'principal' | 'group' | 'role' | 'key';
export type AccessHealth = 'healthy' | 'warning' | 'critical' | 'unavailable';

export interface AccessOverviewInput {
  users: AuthUser[];
  groups: AuthGroup[];
  roles: AuthRole[];
  policies: AuthPolicy[];
  apiKeys: AuthApiKey[];
  sessions: AuthSession[];
  usage: AuthUsageRow[];
}

export interface AccessSummaryCard {
  id: 'identities' | 'keys' | 'sessions' | 'policies';
  label: string;
  value: number;
  caption: string;
  health: AccessHealth;
}

export interface AccessPrincipalRow {
  id: string;
  kind: AuthUser['kind'];
  name: string;
  enabled: boolean;
  keyCount: number;
  activeKeyCount: number;
  revokedKeyCount: number;
  activeSessionCount: number;
  policyCount: number;
  allowedModels: string[];
  deniedModels: string[];
  providers: string[];
  endpoints: string[];
  schedule: string;
  sessionLimit: string;
  dailyStarts: string;
  usageTokens: number | null;
  usageCost: number | null;
  usageConfidence: AuthUsageRow['cost_confidence'] | 'unavailable';
}

export interface AccessAttentionItem {
  id: string;
  severity: 'warning' | 'critical';
  title: string;
  detail: string;
}

export interface AccessOverviewModel {
  summaryCards: AccessSummaryCard[];
  principalRows: AccessPrincipalRow[];
  attentionItems: AccessAttentionItem[];
}

const weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

export function buildAccessOverview(input: AccessOverviewInput): AccessOverviewModel {
  const activeUsers = input.users.filter((user) => user.enabled);
  const activeKeys = input.apiKeys.filter((key) => key.enabled);
  const activePolicies = input.policies.filter((policy) => policy.enabled);
  const keysByPrincipal = groupBy(input.apiKeys, (key) => key.principal_id);
  const sessionsByPrincipal = groupBy(input.sessions, (session) => session.principal_id);
  const directPoliciesByPrincipal = policiesBySubject(activePolicies, 'principal');
  const usageByPrincipal = principalUsage(input);

  const principalRows = input.users.map((user) => {
    const keys = keysByPrincipal.get(user.id) ?? [];
    const directPolicies = directPoliciesByPrincipal.get(user.id) ?? [];
    const usage = usageByPrincipal.get(user.id);
    return {
      id: user.id,
      kind: user.kind,
      name: user.display_name,
      enabled: user.enabled,
      keyCount: keys.length,
      activeKeyCount: keys.filter((key) => key.enabled).length,
      revokedKeyCount: keys.filter((key) => !key.enabled).length,
      activeSessionCount: sessionsByPrincipal.get(user.id)?.length ?? 0,
      policyCount: directPolicies.length,
      allowedModels: unique(directPolicies.filter((policy) => policy.effect === 'allow').flatMap(policyModels)),
      deniedModels: unique(directPolicies.filter((policy) => policy.effect === 'deny').flatMap(policyModels)),
      providers: unique(directPolicies.flatMap((policy) => policy.providers)),
      endpoints: unique(directPolicies.flatMap((policy) => policy.endpoints)),
      schedule: scheduleLabel(directPolicies),
      sessionLimit: limitLabel(directPolicies.map((policy) => policy.max_concurrent_sessions)),
      dailyStarts: limitLabel(directPolicies.map((policy) => policy.max_daily_session_starts)),
      usageTokens: usage?.tokens ?? null,
      usageCost: usage?.cost ?? null,
      usageConfidence: usage?.confidence ?? 'unavailable',
    };
  });

  const attentionItems = buildAttentionItems(input, principalRows);
  const disabledKeys = input.apiKeys.length - activeKeys.length;

  return {
    summaryCards: [
      {
        id: 'identities',
        label: 'Active identities',
        value: activeUsers.length,
        caption: `${input.users.length - activeUsers.length} disabled`,
        health: input.users.length === activeUsers.length ? 'healthy' : 'warning',
      },
      {
        id: 'keys',
        label: 'Active API keys',
        value: activeKeys.length,
        caption: `${disabledKeys} disabled or revoked`,
        health: disabledKeys === 0 ? 'healthy' : 'warning',
      },
      {
        id: 'sessions',
        label: 'Live sessions',
        value: input.sessions.length,
        caption: sessionPressureCaption(input.sessions, activePolicies),
        health: input.sessions.length === 0 ? 'unavailable' : 'healthy',
      },
      {
        id: 'policies',
        label: 'Enabled policies',
        value: activePolicies.length,
        caption: `${input.policies.length - activePolicies.length} disabled`,
        health: activePolicies.length === 0 ? 'critical' : 'healthy',
      },
    ],
    principalRows,
    attentionItems,
  };
}

export function subjectLabel(subject: string): { kind: AccessSubjectKind; id: string } | null {
  const [kind, ...rest] = subject.includes(':') ? subject.split(':') : ['group', subject];
  const id = rest.join(':');
  if ((kind === 'principal' || kind === 'group' || kind === 'role' || kind === 'key') && id) {
    return { kind, id };
  }
  if (kind === 'group' && subject) return { kind: 'group', id: subject };
  return null;
}

export function formatAccessList(items: string[], fallback = 'Unspecified'): string {
  if (items.length === 0) return fallback;
  if (items.length <= 2) return items.join(', ');
  return `${items.slice(0, 2).join(', ')} +${items.length - 2}`;
}

function buildAttentionItems(input: AccessOverviewInput, rows: AccessPrincipalRow[]): AccessAttentionItem[] {
  const items: AccessAttentionItem[] = [];
  for (const key of input.apiKeys.filter((candidate) => !candidate.enabled).slice(0, 3)) {
    items.push({
      id: `key:${key.id}`,
      severity: 'warning',
      title: 'Disabled API key',
      detail: `${key.name} (${key.prefix}...) is not usable.`,
    });
  }
  for (const row of rows.filter((candidate) => candidate.enabled && candidate.activeKeyCount === 0).slice(0, 3)) {
    items.push({
      id: `nokey:${row.id}`,
      severity: 'critical',
      title: 'Identity has no active key',
      detail: `${row.name} cannot authenticate until a key is issued.`,
    });
  }
  for (const row of rows.filter((candidate) => candidate.enabled && candidate.policyCount === 0).slice(0, 3)) {
    items.push({
      id: `nopolicy:${row.id}`,
      severity: 'warning',
      title: 'Identity has no direct policy',
      detail: `${row.name} may rely on group policy or be denied by default.`,
    });
  }
  for (const row of rows.filter((candidate) => candidate.sessionLimit !== 'Unlimited' && candidate.activeSessionCount > 0).slice(0, 3)) {
    items.push({
      id: `sessions:${row.id}`,
      severity: row.activeSessionCount >= Number(row.sessionLimit) ? 'critical' : 'warning',
      title: 'Session limit in use',
      detail: `${row.name} has ${row.activeSessionCount} live session(s) against a limit of ${row.sessionLimit}.`,
    });
  }
  return items.slice(0, 6);
}

function policiesBySubject(policies: AuthPolicy[], kind: AccessSubjectKind): Map<string, AuthPolicy[]> {
  const result = new Map<string, AuthPolicy[]>();
  for (const policy of policies) {
    for (const subject of policy.subjects) {
      const parsed = subjectLabel(subject);
      if (!parsed || parsed.kind !== kind) continue;
      const existing = result.get(parsed.id) ?? [];
      existing.push(policy);
      result.set(parsed.id, existing);
    }
  }
  return result;
}

function principalUsage(input: AccessOverviewInput): Map<string, { tokens: number | null; cost: number | null; confidence: AuthUsageRow['cost_confidence'] }> {
  const keyToPrincipal = new Map(input.apiKeys.map((key) => [key.id, key.principal_id]));
  const usage = new Map<string, { tokens: number | null; cost: number | null; confidence: AuthUsageRow['cost_confidence'] }>();
  for (const row of input.usage) {
    const principalId = row.dimension === 'principal' ? row.value : row.dimension === 'key' ? keyToPrincipal.get(row.value) : null;
    if (!principalId) continue;
    const current = usage.get(principalId) ?? { tokens: 0, cost: 0, confidence: 'confident' as const };
    const tokens = row.prompt_tokens === null || row.completion_tokens === null ? null : row.prompt_tokens + row.completion_tokens;
    usage.set(principalId, {
      tokens: current.tokens === null || tokens === null ? null : current.tokens + tokens,
      cost: current.cost === null || row.cost === null ? null : current.cost + row.cost,
      confidence: weakerConfidence(current.confidence, row.cost_confidence),
    });
  }
  return usage;
}

function sessionPressureCaption(sessions: AuthSession[], policies: AuthPolicy[]): string {
  const limits = policies.map((policy) => policy.max_concurrent_sessions).filter((value): value is number => value !== null);
  if (sessions.length === 0) return 'No live sessions';
  if (limits.length === 0) return 'No session cap';
  return `Smallest cap ${Math.min(...limits)}`;
}

function scheduleLabel(policies: AuthPolicy[]): string {
  const windows = unique(policies.flatMap((policy) => policy.time_windows.map(formatWindow)));
  if (windows.length === 0) return 'Any time';
  return formatAccessList(windows);
}

function formatWindow(window: AuthPolicy['time_windows'][number]): string {
  const days = weekdays.filter((_, index) => (window.weekday_mask & (1 << index)) !== 0);
  return `${days.length === 7 ? 'Every day' : days.join('/')} ${minuteLabel(window.start_minute)}-${minuteLabel(window.end_minute)} UTC`;
}

function minuteLabel(value: number): string {
  const hour = Math.floor(value / 60).toString().padStart(2, '0');
  const minute = (value % 60).toString().padStart(2, '0');
  return `${hour}:${minute}`;
}

function policyModels(policy: AuthPolicy): string[] {
  return policy.requested_models.length > 0 ? policy.requested_models : policy.models;
}

function limitLabel(values: Array<number | null>): string {
  const limits = values.filter((value): value is number => value !== null);
  return limits.length === 0 ? 'Unlimited' : String(Math.min(...limits));
}

function weakerConfidence(a: AuthUsageRow['cost_confidence'], b: AuthUsageRow['cost_confidence']): AuthUsageRow['cost_confidence'] {
  const rank = { confident: 0, estimated: 1, unavailable: 2 };
  return rank[a] >= rank[b] ? a : b;
}

function groupBy<T>(items: T[], key: (item: T) => string): Map<string, T[]> {
  const result = new Map<string, T[]>();
  for (const item of items) {
    const id = key(item);
    result.set(id, [...(result.get(id) ?? []), item]);
  }
  return result;
}

function unique(items: string[]): string[] {
  return Array.from(new Set(items.filter(Boolean)));
}
