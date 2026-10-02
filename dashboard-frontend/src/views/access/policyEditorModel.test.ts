import { describe, expect, it } from 'vitest';
import type { CreateAuthPolicyRequest } from '../../api/types';
import {
  inferenceEndpointOptions,
  summarizeEndpoints,
  summarizeManagementPermissions,
  summarizeMatcher,
  summarizePolicyIntent,
  validatePolicy,
} from './policyEditorModel';

const basePolicy: CreateAuthPolicyRequest = {
  name: 'Standard access',
  effect: 'allow',
  subjects: ['principal:usr_ops'],
  endpoints: ['responses'],
  models: [],
  requested_models: ['gpt-*'],
  served_models: ['gpt-4.1'],
  providers: ['openai'],
  routes: [],
  time_windows: [],
  max_concurrent_sessions: null,
  max_daily_session_starts: null,
  management_permissions: [],
};

describe('policyEditorModel', () => {
  it('exports fixed endpoint metadata that matches the backend policy enum', () => {
    expect(inferenceEndpointOptions.map((option) => option.value)).toEqual([
      'responses',
      'chat',
      'messages',
      'completions',
      'models',
      'count_tokens',
    ]);
    expect(inferenceEndpointOptions.every((option) => option.label && option.description)).toBe(true);
  });

  it('validates model-access policies with a name, subject, and capability', () => {
    expect(validatePolicy({
      ...basePolicy,
      name: '',
      subjects: [],
      endpoints: [],
      max_concurrent_sessions: 0,
      max_daily_session_starts: 1.5,
      time_windows: [{ weekday_mask: 0, start_minute: 1500, end_minute: 1500, absolute_start_ms: null, absolute_end_ms: null }],
    }, 'model')).toEqual([
      'Policy name is required.',
      'Select at least one real subject.',
      'Select at least one model API capability.',
      'Max concurrent sessions must be a positive integer.',
      'Daily session starts must be a positive integer.',
      'UTC window requires days and distinct HH:MM start/end times.',
    ]);
  });

  it('rejects partial UTC windows while allowing the window to be omitted entirely', () => {
    expect(validatePolicy({ ...basePolicy, time_windows: [] }, 'model')).toEqual([]);
    expect(validatePolicy({
      ...basePolicy,
      time_windows: [{ weekday_mask: 31, start_minute: Number.NaN, end_minute: Number.NaN, absolute_start_ms: null, absolute_end_ms: null }],
    }, 'model')).toEqual(['UTC window requires days and distinct HH:MM start/end times.']);
  });

  it('accepts administration policies without model endpoints when management permissions are selected', () => {
    expect(validatePolicy({
      ...basePolicy,
      endpoints: [],
      requested_models: [],
      served_models: [],
      providers: [],
      management_permissions: ['auth.policies.read'],
    }, 'administration')).toEqual([]);
  });

  it('exposes human-readable Fleet administration permissions', () => {
    expect(summarizeManagementPermissions([
      'fleet.models.read',
      'fleet.models.load',
      'fleet.models.unload',
    ])).toBe('View Fleet model state, Load Fleet models, Unload Fleet models');
  });

  it('rejects policies that grant neither model access nor administration access', () => {
    expect(validatePolicy({
      ...basePolicy,
      endpoints: [],
      management_permissions: [],
    }, 'administration')).toEqual([
      'Select at least one administration permission.',
    ]);
  });

  it('summarizes explicit and unrestricted capability/provider/model selections', () => {
    expect(summarizeEndpoints(['responses', 'chat'])).toBe('Responses API, Chat Completions');
    expect(summarizeEndpoints(['*'])).toBe('All model API capabilities');
    expect(summarizeMatcher([], 'All providers')).toBe('All providers');
    expect(summarizeMatcher(['vllm-a', 'openai'], 'All providers')).toBe('vllm-a, openai');

    expect(summarizePolicyIntent({ ...basePolicy, subjects: [] }, 'model')[0]).toBe('Select who this policy applies to.');

    expect(summarizePolicyIntent({
      ...basePolicy,
      endpoints: ['responses', 'chat'],
      providers: [],
      requested_models: ['*'],
      served_models: [],
    }, 'model')).toEqual([
      'Allow 1 subject.',
      'Capabilities: Responses API, Chat Completions.',
      'Providers: All providers.',
      'Requested models: All requested models.',
      'Served models: All served models.',
      'Available at any time.',
      'Unlimited concurrent sessions.',
      'Unlimited daily session starts.',
    ]);
  });
});
