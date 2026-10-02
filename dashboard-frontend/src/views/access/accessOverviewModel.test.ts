import { describe, expect, it } from 'vitest';
import type { AccessOverviewInput } from './accessOverviewModel';
import { buildAccessOverview, subjectLabel } from './accessOverviewModel';

const baseInput: AccessOverviewInput = {
  users: [
    { id: 'usr_ops', kind: 'user', display_name: 'Operations', enabled: true, created_at: '2026-01-01T00:00:00Z' },
    { id: 'usr_batch', kind: 'service_account', display_name: 'Batch jobs', enabled: true, created_at: '2026-01-01T00:00:00Z' },
  ],
  groups: [],
  roles: [],
  policies: [
    {
      id: 'pol_ops',
      name: 'Ops allow',
      effect: 'allow',
      enabled: true,
      subjects: ['principal:usr_ops'],
      endpoints: ['responses'],
      models: [],
      requested_models: ['gpt-*'],
      served_models: ['gpt-4.1'],
      providers: ['openai'],
      routes: ['cloud'],
      time_windows: [{ weekday_mask: 31, start_minute: 480, end_minute: 1080, absolute_start_ms: null, absolute_end_ms: null }],
      max_concurrent_sessions: 2,
      max_daily_session_starts: 20,
      management_permissions: [],
    },
  ],
  apiKeys: [
    { id: 'key_ops', principal_id: 'usr_ops', name: 'laptop', prefix: 'llmc_abcd', enabled: true, created_at: '2026-01-01T00:00:00Z', expires_at: null, last_used_at: null, capture_payloads: false },
    { id: 'key_old', principal_id: 'usr_ops', name: 'old laptop', prefix: 'llmc_dead', enabled: false, created_at: '2026-01-01T00:00:00Z', expires_at: null, last_used_at: null, capture_payloads: false },
  ],
  sessions: [
    { id: 'sess_ops', kind: 'inference', principal_id: 'usr_ops', key_id: 'key_ops', endpoint: 'responses', requested_model: 'gpt-4.1', started_at: '2026-01-01T00:00:00Z', expires_at: null },
  ],
  usage: [
    { dimension: 'key', value: 'key_ops', requests: 3, prompt_tokens: 1000, completion_tokens: 500, cached_tokens: null, reasoning_tokens: null, cost: 0.25, cost_confidence: 'estimated' },
  ],
};

describe('accessOverviewModel', () => {
  it('parses policy subjects without treating legacy group ids as principals', () => {
    expect(subjectLabel('principal:usr_ops')).toEqual({ kind: 'principal', id: 'usr_ops' });
    expect(subjectLabel('grp_prod')).toEqual({ kind: 'group', id: 'grp_prod' });
  });

  it('builds operator-readable summary cards and principal rows', () => {
    const model = buildAccessOverview(baseInput);

    expect(model.summaryCards.map((card) => [card.id, card.value])).toEqual([
      ['identities', 2],
      ['keys', 1],
      ['sessions', 1],
      ['policies', 1],
    ]);
    expect(model.summaryCards.find((card) => card.id === 'keys')?.caption).toBe('1 disabled or revoked');

    const ops = model.principalRows.find((row) => row.id === 'usr_ops');
    expect(ops).toMatchObject({
      name: 'Operations',
      activeKeyCount: 1,
      revokedKeyCount: 1,
      activeSessionCount: 1,
      policyCount: 1,
      allowedModels: ['gpt-*'],
      providers: ['openai'],
      schedule: 'Mon/Tue/Wed/Thu/Fri 08:00-18:00 UTC',
      sessionLimit: '2',
      dailyStarts: '20',
      usageTokens: 1500,
      usageCost: 0.25,
      usageConfidence: 'estimated',
    });
  });

  it('surfaces bounded attention items without inventing unavailable usage values', () => {
    const model = buildAccessOverview(baseInput);
    const batch = model.principalRows.find((row) => row.id === 'usr_batch');

    expect(batch?.usageTokens).toBeNull();
    expect(batch?.usageCost).toBeNull();
    expect(batch?.usageConfidence).toBe('unavailable');
    expect(model.attentionItems.map((item) => item.title)).toEqual([
      'Disabled API key',
      'Identity has no active key',
      'Identity has no direct policy',
      'Session limit in use',
    ]);
  });
});
