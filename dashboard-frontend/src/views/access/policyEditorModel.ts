import type { CreateAuthPolicyRequest, ManagementPermission } from '../../api/types';

export type PolicyEditorKind = 'model' | 'administration';

export interface EndpointOption {
  value: string;
  label: string;
  description: string;
}

export const inferenceEndpointOptions: readonly EndpointOption[] = [
  { value: 'responses', label: 'Responses API', description: 'OpenAI Responses requests at /v1/responses.' },
  { value: 'chat', label: 'Chat Completions', description: 'OpenAI-compatible chat requests at /v1/chat/completions.' },
  { value: 'messages', label: 'Anthropic Messages', description: 'Anthropic-compatible requests at /v1/messages.' },
  { value: 'completions', label: 'Legacy Completions', description: 'Legacy OpenAI completions at /v1/completions.' },
  { value: 'models', label: 'Model List', description: 'List available models at /v1/models.' },
  { value: 'count_tokens', label: 'Token Counting', description: 'Anthropic-compatible token counting.' },
];

const endpointLabels = new Map(inferenceEndpointOptions.map((option) => [option.value, option.label]));

export const managementPermissionLabels: Record<ManagementPermission, string> = {
  'auth.keys.read': 'View keys',
  'auth.keys.create': 'Create keys',
  'auth.keys.revoke': 'Revoke keys',
  'auth.keys.rotate': 'Rotate keys',
  'auth.principals.read': 'View identities',
  'auth.principals.write': 'Manage identities',
  'auth.groups.read': 'View groups',
  'auth.groups.write': 'Manage groups',
  'auth.roles.read': 'View roles',
  'auth.roles.write': 'Manage roles',
  'auth.policies.read': 'View policies',
  'auth.policies.write': 'Manage policies',
  'auth.usage.read': 'View usage and costs',
  'auth.audit.read': 'View audit log',
  'auth.pricing.read': 'View pricing',
  'auth.pricing.sync': 'Sync pricing',
  'auth.pricing.write': 'Manage pricing',
  'auth.sessions.read': 'View sessions',
  'auth.sessions.terminate': 'Terminate sessions',
  'fleet.models.read': 'View Fleet model state',
  'fleet.models.load': 'Load Fleet models',
  'fleet.models.unload': 'Unload Fleet models',
};

export function validatePolicy(policy: CreateAuthPolicyRequest, kind: PolicyEditorKind = 'model'): string[] {
  const errors: string[] = [];
  if (!policy.name) errors.push('Policy name is required.');
  if (policy.subjects.length === 0) errors.push('Select at least one real subject.');
  if (kind === 'model' && policy.endpoints.length === 0) errors.push('Select at least one model API capability.');
  if (kind === 'administration' && policy.management_permissions.length === 0) errors.push('Select at least one administration permission.');
  for (const [label, value] of [
    ['Max concurrent sessions', policy.max_concurrent_sessions],
    ['Daily session starts', policy.max_daily_session_starts],
  ] as const) {
    if (value !== null && (!Number.isInteger(value) || value < 1)) {
      errors.push(`${label} must be a positive integer.`);
    }
  }
  for (const window of policy.time_windows) {
    if (!Number.isInteger(window.weekday_mask) || !Number.isInteger(window.start_minute) || !Number.isInteger(window.end_minute)
      || window.weekday_mask < 1 || window.weekday_mask > 0x7f
      || window.start_minute < 0 || window.end_minute < 0
      || window.start_minute >= 24 * 60 || window.end_minute > 24 * 60
      || window.start_minute === window.end_minute) {
      errors.push('UTC window requires days and distinct HH:MM start/end times.');
    }
  }
  return errors;
}

export function summarizeMatcher(values: readonly string[], unrestrictedLabel: string): string {
  if (values.length === 0 || values.includes('*')) return unrestrictedLabel;
  return values.join(', ');
}

export function summarizeEndpoints(endpoints: readonly string[]): string {
  if (endpoints.length === 0 || endpoints.includes('*')) return 'All model API capabilities';
  return endpoints.map((endpoint) => endpointLabels.get(endpoint) ?? endpoint).join(', ');
}

export function summarizeManagementPermissions(permissions: readonly ManagementPermission[]): string {
  if (permissions.length === 0) return 'No administration permissions';
  return permissions.map((permission) => managementPermissionLabels[permission] ?? permission).join(', ');
}

export function summarizePolicyIntent(policy: CreateAuthPolicyRequest, kind: PolicyEditorKind): string[] {
  const lines = [
    policy.subjects.length === 0
      ? 'Select who this policy applies to.'
      : `${policy.effect === 'deny' ? 'Deny' : 'Allow'} ${policy.subjects.length} ${policy.subjects.length === 1 ? 'subject' : 'subjects'}.`,
  ];
  if (kind === 'administration') {
    lines.push(`Administration: ${summarizeManagementPermissions(policy.management_permissions)}.`);
  } else {
    lines.push(`Capabilities: ${summarizeEndpoints(policy.endpoints)}.`);
    lines.push(`Providers: ${summarizeMatcher(policy.providers, 'All providers')}.`);
    lines.push(`Requested models: ${summarizeMatcher(policy.requested_models, 'All requested models')}.`);
    lines.push(`Served models: ${summarizeMatcher(policy.served_models, 'All served models')}.`);
  }
  lines.push(policy.time_windows.length > 0 ? `${policy.time_windows.length} UTC schedule window configured.` : 'Available at any time.');
  lines.push(policy.max_concurrent_sessions === null ? 'Unlimited concurrent sessions.' : `${policy.max_concurrent_sessions} concurrent sessions.`);
  lines.push(policy.max_daily_session_starts === null ? 'Unlimited daily session starts.' : `${policy.max_daily_session_starts} daily session starts.`);
  return lines;
}
