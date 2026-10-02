/**
 * Typed REST client for the D13 endpoints. Cross-cutting behavior:
 *  - All reads carry the session cookie automatically (`credentials: 'include'`).
 *  - The kill POST attaches `X-CSRF-Token` (D7 double-submit; value from bootstrap/cookie).
 *  - A 401 on ANY fetch fires the `onUnauthorized` signal so the shell bounces to login.
 *
 * The client is transport-agnostic: pass a `fetchImpl` (default `globalThis.fetch`) and
 * a `csrfToken` getter so the mock backend + tests can inject their own.
 */
import type {
  ActivityResponse,
  ActiveSessionsResponse,
  ApiKeyRecord,
  AuthApiKeysResponse,
  AuthAuditResponse,
  AuthGroupsResponse,
  AuthPoliciesResponse,
  AuthPricingResponse,
  AuthRolesResponse,
  AuthSessionsResponse,
  AuthSummary,
  AuthUsageResponse,
  AuthUsersResponse,
  CatalogEntry,
  ConfiguredProvider,
  ConfiguredProvidersResponse,
  CreateConfiguredProviderRequest,
  UpdateConfiguredProviderRequest,
  CreateAuthApiKeyRequest,
  CreateAuthGroupRequest,
  CreateAuthPolicyRequest,
  CreateAuthRoleRequest,
  CreateAuthUserRequest,
  CreatedAuthApiKey,
  CreatedKeyResponse,
  FleetModelsResponse,
  FleetOperationResponse,
  FlowDetail,
  FlowsQuery,
  FlowsResponse,
  HistoryBodyHop,
  HistoryMetricsResponse,
  KillResponse,
  LoginRequest,
  MeResponse,
  CreateMeshJoinKeyRequest,
  CreateMeshJoinKeyResponse,
  MeshAdminState,
  MeshModelOverrideRequest,
  MetricsResponse,
  ProviderMetricsResponse,
  ProvidersResponse,
  RevokeMeshJoinKeyResponse,
  SessionDetailResponse,
  SessionUser,
  SessionsResponse,
  SetMeshModelResponse,
  SetMeshNodeResponse,
  SwitchMeshModelResponse,
  SnapshotResponse,
  ThroughputResponse,
  TopologyResponse,
  UserRecord,
} from './types';
import {
  isAuthApiKeysResponse,
  isAuthAuditResponse,
  isAuthGroupsResponse,
  isAuthPoliciesResponse,
  isAuthPricingResponse,
  isAuthRolesResponse,
  isAuthSessionsResponse,
  isAuthSummary,
  isAuthUsageResponse,
  isAuthUsersResponse,
  isCreateMeshJoinKeyResponse,
  isConfiguredProviderResponse,
  isConfiguredProvidersResponse,
  isFleetModelsResponse,
  isFleetOperationResponse,
  isMeshAdminState,
  isCreatedAuthApiKey,
  isProviderMetricsResponse,
  isProvidersResponse,
  isRevokeMeshJoinKeyResponse,
  isSetMeshModelResponse,
  isSetMeshNodeResponse,
  isSwitchMeshModelResponse,
} from './types';

export type FetchImpl = typeof fetch;

export interface DashboardChatMessage {
  role: 'system' | 'user' | 'assistant';
  content: string;
}

export interface DashboardChatRequest {
  model: string;
  messages: DashboardChatMessage[];
  temperature?: number;
  top_p?: number;
  max_tokens?: number;
  reasoning_effort?: 'low' | 'medium' | 'high' | 'xhigh' | 'max';
  prompt_cache_key?: string;
}

export interface DashboardChatDelta {
  kind: 'content' | 'reasoning';
  text: string;
}

export interface DashboardChatUsage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
}

export interface DashboardChatResult {
  model: string | null;
  requestedModel: string | null;
  finishReason: string | null;
  usage: DashboardChatUsage | null;
}

/** Raised when a fetch returns 401; the shell listens for this to bounce to login. */
export class UnauthorizedError extends Error {
  constructor() {
    super('unauthorized');
    this.name = 'UnauthorizedError';
  }
}

export interface DashboardClientOptions {
  /** Base path the API is mounted under. Default `/dashboard/api`. */
  basePath?: string;
  /** Injected fetch (mock/test). Default `globalThis.fetch`. */
  fetchImpl?: FetchImpl;
  /** Returns the current double-submit CSRF token (read from cookie/bootstrap). */
  getCsrfToken?: () => string | null;
  /** Fired on ANY 401 so the app can bounce to the login shell. */
  onUnauthorized?: () => void;
}

export class DashboardClient {
  private readonly basePath: string;
  private readonly fetchImpl: FetchImpl;
  private readonly getCsrfToken: () => string | null;
  private readonly onUnauthorized: (() => void) | undefined;

  constructor(opts: DashboardClientOptions = {}) {
    this.basePath = opts.basePath ?? '/dashboard/api';
    // Bind to globalThis so the default impl isn't called with a `this` of the class.
    this.fetchImpl = opts.fetchImpl ?? ((...a: Parameters<FetchImpl>) => globalThis.fetch(...a));
    this.getCsrfToken = opts.getCsrfToken ?? (() => null);
    this.onUnauthorized = opts.onUnauthorized;
  }

  private async request<T>(path: string, init?: RequestInit, guard?: (value: unknown) => value is T): Promise<T> {
    const res = await this.fetchImpl(`${this.basePath}${path}`, {
      credentials: 'include',
      ...init,
    });
    if (res.status === 401) {
      // Bounce-to-login signal: notify, then throw so callers stop.
      this.onUnauthorized?.();
      throw new UnauthorizedError();
    }
    if (!res.ok) {
      throw new Error(`${init?.method ?? 'GET'} ${path} failed: ${res.status}`);
    }
    // 204/empty bodies decode to `undefined as T` at the call sites that allow it.
    const text = await res.text();
    const value: unknown = text ? JSON.parse(text) : undefined;
    if (guard && !guard(value)) throw new Error(`${path} returned an invalid response`);
    return value as T;
  }

  // -- Auth -----------------------------------------------------------------

  /**
   * `POST /dashboard/login` — note: login lives at /dashboard, NOT under /api. Returns the
   * signed-in user (null for a token login) as the server reports it.
   */
  async login(body: LoginRequest): Promise<{ user: SessionUser | null }> {
    const res = await this.fetchImpl('/dashboard/login', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`login failed: ${res.status}`);
    }
    try {
      const parsed = (await res.json()) as { user?: SessionUser | null };
      return { user: parsed.user ?? null };
    } catch {
      return { user: null };
    }
  }

  /** A CSRF-carrying mutation under /dashboard/api. */
  private mutate<T>(path: string, method: 'POST' | 'PATCH' | 'DELETE', body?: unknown): Promise<T> {
    const csrf = this.getCsrfToken();
    const headers: Record<string, string> = {};
    if (csrf) headers['X-CSRF-Token'] = csrf;
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    return this.request<T>(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  }

  // -- Accounts -------------------------------------------------------------

  me(): Promise<MeResponse> {
    return this.request<MeResponse>('/me');
  }

  listUsers(): Promise<{ users: UserRecord[] }> {
    return this.request<{ users: UserRecord[] }>('/users');
  }

  createUser(body: { username: string; password: string; is_admin: boolean }): Promise<UserRecord> {
    return this.mutate<UserRecord>('/users', 'POST', body);
  }

  updateUser(id: string, body: { password?: string; is_admin?: boolean }): Promise<{ id: string; updated: boolean }> {
    return this.mutate(`/users/${encodeURIComponent(id)}`, 'PATCH', body);
  }

  deleteUser(id: string): Promise<{ id: string; deleted: boolean; keys_revoked: number }> {
    return this.mutate(`/users/${encodeURIComponent(id)}`, 'DELETE');
  }

  /** `GET /keys[?user_id=]` — own keys by default; admins may pass a user id or `all`. */
  listKeys(userId?: string): Promise<{ keys: ApiKeyRecord[] }> {
    return this.request<{ keys: ApiKeyRecord[] }>(`/keys${userId ? `?user_id=${encodeURIComponent(userId)}` : ''}`);
  }

  createKey(body: { label?: string; allowed_models?: string[]; user_id?: string }): Promise<CreatedKeyResponse> {
    return this.mutate<CreatedKeyResponse>('/keys', 'POST', body);
  }

  deleteKey(id: string): Promise<{ id: string; revoked: boolean }> {
    return this.mutate(`/keys/${encodeURIComponent(id)}`, 'DELETE');
  }

  // -- History series -------------------------------------------------------

  historyThroughput(query: { since_ms?: number; bucket_secs?: number; limit?: number } = {}): Promise<ThroughputResponse> {
    return this.request<ThroughputResponse>(`/history/throughput${buildQuery(query)}`);
  }

  historyActivity(query: { since_ms?: number; bucket_secs?: number; limit?: number } = {}): Promise<ActivityResponse> {
    return this.request<ActivityResponse>(`/history/activity${buildQuery(query)}`);
  }

  historyMetrics(query: { since_ms?: number; limit?: number } = {}): Promise<HistoryMetricsResponse> {
    return this.request<HistoryMetricsResponse>(`/history/metrics${buildQuery(query)}`);
  }

  /** Delegated management login backed by an API key. */
  async keyLogin(apiKey: string): Promise<void> {
    const res = await this.fetchImpl('/dashboard/auth/key-login', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: apiKey }),
    });
    if (!res.ok) throw new Error(`key login failed: ${res.status}`);
  }

  /** `POST /dashboard/auth/logout` — revokes delegated sessions and clears cookies. */
  async logout(): Promise<void> {
    const csrf = this.getCsrfToken();
    await this.fetchImpl('/dashboard/auth/logout', {
      method: 'POST',
      credentials: 'include',
      headers: csrf ? { 'X-CSRF-Token': csrf } : undefined,
    });
  }

  /**
   * Lightweight protected-endpoint probe (finding 7): a cheap GET used by the WS layer
   * after a transient drop to decide reconnect-vs-logout. Returns `true` if the session is
   * still valid, `false` ONLY on a `401`. It does NOT fire `onUnauthorized` itself (the
   * caller decides); a non-401 error (network) resolves `true` so a blip reconnects rather
   * than logging the user out. Probes `/metrics` (a small, always-present read).
   */
  async probeAuth(): Promise<boolean> {
    try {
      const res = await this.fetchImpl(`${this.basePath}/metrics`, { credentials: 'include' });
      return res.status !== 401;
    } catch {
      // Network failure ≠ auth failure: stay logged in, let the socket reconnect.
      return true;
    }
  }

  // -- Reads (cursor-bearing) ----------------------------------------------

  flows(query: FlowsQuery = {}): Promise<FlowsResponse> {
    const qs = buildQuery(query);
    return this.request<FlowsResponse>(`/flows${qs}`);
  }

  flowDetail(id: string): Promise<FlowDetail> {
    return this.request<FlowDetail>(`/flows/${encodeURIComponent(id)}`);
  }

  metrics(): Promise<MetricsResponse> {
    return this.request<MetricsResponse>('/metrics');
  }

  topology(): Promise<TopologyResponse> {
    return this.request<TopologyResponse>('/topology');
  }

  providers(): Promise<ProvidersResponse> {
    return this.request('/providers', undefined, isProvidersResponse);
  }

  configuredProviders(): Promise<ConfiguredProvidersResponse> {
    return this.request('/configured-providers', undefined, isConfiguredProvidersResponse);
  }

  createConfiguredProvider(body: CreateConfiguredProviderRequest): Promise<ConfiguredProvider> {
    return this.mutate('/configured-providers', 'POST', body).then((value) => {
      if (!isConfiguredProviderResponse(value)) throw new Error('/configured-providers returned an invalid response');
      return value;
    });
  }

  updateConfiguredProvider(id: string, body: UpdateConfiguredProviderRequest): Promise<ConfiguredProvider> {
    return this.mutate(`/configured-providers/${encodeURIComponent(id)}`, 'PATCH', body).then((value) => {
      if (!isConfiguredProviderResponse(value)) throw new Error('/configured-providers/:id returned an invalid response');
      return value;
    });
  }

  deleteConfiguredProvider(id: string): Promise<void> {
    return this.mutate(`/configured-providers/${encodeURIComponent(id)}`, 'DELETE');
  }

  providerMetrics(): Promise<ProviderMetricsResponse> {
    return this.request('/provider-metrics', undefined, isProviderMetricsResponse);
  }

  fleet(): Promise<FleetModelsResponse> {
    return this.request('/fleet', undefined, isFleetModelsResponse);
  }

  loadFleetModel(id: string, instances?: number): Promise<FleetOperationResponse> {
    return this.mutate(`/fleet/models/${encodeURIComponent(id)}/load`, 'POST', instances === undefined ? undefined : { instances }).then((value) => {
      if (!isFleetOperationResponse(value)) throw new Error('/fleet/models/:id/load returned an invalid response');
      return value;
    });
  }

  unloadFleetModel(id: string): Promise<FleetOperationResponse> {
    return this.mutate(`/fleet/models/${encodeURIComponent(id)}/unload`, 'POST').then((value) => {
      if (!isFleetOperationResponse(value)) throw new Error('/fleet/models/:id/unload returned an invalid response');
      return value;
    });
  }

  mesh(): Promise<MeshAdminState> {
    return this.request('/mesh', undefined, isMeshAdminState);
  }

  createMeshJoinKey(body: CreateMeshJoinKeyRequest): Promise<CreateMeshJoinKeyResponse> {
    return this.mutate('/mesh/join-keys', 'POST', body).then((value) => {
      if (!isCreateMeshJoinKeyResponse(value)) throw new Error('/mesh/join-keys returned an invalid response');
      return value;
    });
  }

  revokeMeshJoinKey(id: string): Promise<RevokeMeshJoinKeyResponse> {
    return this.mutate(`/mesh/join-keys/${encodeURIComponent(id)}/revoke`, 'POST').then((value) => {
      if (!isRevokeMeshJoinKeyResponse(value)) throw new Error('/mesh/join-keys/:id/revoke returned an invalid response');
      return value;
    });
  }

  setMeshNodeEnabled(endpointId: string, enabled: boolean): Promise<SetMeshNodeResponse> {
    const action = enabled ? 'enable' : 'disable';
    return this.mutate(`/mesh/nodes/${encodeURIComponent(endpointId)}/${action}`, 'POST').then((value) => {
      if (!isSetMeshNodeResponse(value)) throw new Error(`/mesh/nodes/:endpoint_id/${action} returned an invalid response`);
      return value;
    });
  }

  setMeshModelDisabled(body: MeshModelOverrideRequest, disabled: boolean): Promise<SetMeshModelResponse> {
    const action = disabled ? 'disable' : 'enable';
    return this.mutate(`/mesh/models/${action}`, 'POST', body).then((value) => {
      if (!isSetMeshModelResponse(value)) throw new Error(`/mesh/models/${action} returned an invalid response`);
      return value;
    });
  }

  switchMeshModel(endpointId: string, modelId: string): Promise<SwitchMeshModelResponse> {
    return this.mutate(`/mesh/nodes/${encodeURIComponent(endpointId)}/models/${encodeURIComponent(modelId)}/switch`, 'POST').then((value) => {
      if (!isSwitchMeshModelResponse(value)) throw new Error('/mesh/nodes/:endpoint_id/models/:model_id/switch returned an invalid response');
      return value;
    });
  }

  loadMeshModel(endpointId: string, modelId: string, instances?: number): Promise<SwitchMeshModelResponse> {
    return this.mutate(`/mesh/nodes/${encodeURIComponent(endpointId)}/models/${encodeURIComponent(modelId)}/load`, 'POST', instances === undefined ? undefined : { instances }).then((value) => {
      if (!isSwitchMeshModelResponse(value)) throw new Error('/mesh/nodes/:endpoint_id/models/:model_id/load returned an invalid response');
      return value;
    });
  }

  unloadMeshModel(endpointId: string, modelId: string): Promise<SwitchMeshModelResponse> {
    return this.mutate(`/mesh/nodes/${encodeURIComponent(endpointId)}/models/${encodeURIComponent(modelId)}/unload`, 'POST').then((value) => {
      if (!isSwitchMeshModelResponse(value)) throw new Error('/mesh/nodes/:endpoint_id/models/:model_id/unload returned an invalid response');
      return value;
    });
  }

  /** Bare array — no cursor (D13: static-ish catalog read). */
  catalog(): Promise<CatalogEntry[]> {
    return this.request<CatalogEntry[]>('/catalog');
  }

  /** Stream a dashboard-authenticated turn through the real gateway path. */
  async streamChat(
    request: DashboardChatRequest,
    onDelta: (delta: DashboardChatDelta) => void,
    signal?: AbortSignal,
  ): Promise<DashboardChatResult> {
    const csrf = this.getCsrfToken();
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (csrf) headers['X-CSRF-Token'] = csrf;
    const response = await this.fetchImpl(`${this.basePath}/chat`, {
      method: 'POST',
      credentials: 'include',
      headers,
      signal,
      body: JSON.stringify({ ...request, stream: true, stream_options: { include_usage: true } }),
    });
    if (response.status === 401) {
      this.onUnauthorized?.();
      throw new UnauthorizedError();
    }
    if (!response.ok) {
      const text = await response.text();
      let detail = text.trim();
      try {
        const parsed = JSON.parse(text) as { error?: string | { message?: string } };
        detail = typeof parsed.error === 'string' ? parsed.error : parsed.error?.message ?? detail;
      } catch {
        // Preserve a plain-text upstream/dashboard error.
      }
      throw new Error(detail || `chat failed: ${response.status}`);
    }
    if (!response.body) throw new Error('chat response did not include a stream');

    const result: DashboardChatResult = {
      model: response.headers.get('x-llmconduit-model'),
      requestedModel: response.headers.get('x-llmconduit-requested'),
      finishReason: null,
      usage: null,
    };
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let doneSeen = false;

    const processLine = (rawLine: string) => {
      const line = rawLine.endsWith('\r') ? rawLine.slice(0, -1) : rawLine;
      if (!line.startsWith('data:')) return;
      const data = line.slice(5).trimStart();
      if (data === '[DONE]') {
        doneSeen = true;
        return;
      }
      if (!data) return;
      let event: {
        model?: string;
        choices?: Array<{ delta?: { content?: string; reasoning_content?: string }; finish_reason?: string | null }>;
        usage?: DashboardChatUsage | null;
        error?: { message?: string };
      };
      try {
        event = JSON.parse(data) as typeof event;
      } catch {
        throw new Error('chat stream returned malformed JSON');
      }
      if (event.error) throw new Error(event.error.message || 'model returned an error');
      if (event.model) result.model = event.model;
      if (event.usage) result.usage = event.usage;
      for (const choice of event.choices ?? []) {
        if (choice.delta?.reasoning_content) onDelta({ kind: 'reasoning', text: choice.delta.reasoning_content });
        if (choice.delta?.content) onDelta({ kind: 'content', text: choice.delta.content });
        if (choice.finish_reason) result.finishReason = choice.finish_reason;
      }
    };

    let streamDone = false;
    while (!streamDone) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value, { stream: !done });
      let newline = buffer.indexOf('\n');
      while (newline >= 0) {
        processLine(buffer.slice(0, newline));
        buffer = buffer.slice(newline + 1);
        newline = buffer.indexOf('\n');
      }
      streamDone = done;
    }
    if (buffer.trim()) processLine(buffer);
    if (!doneSeen) throw new Error('stream ended before the terminal [DONE] marker');
    return result;
  }

  snapshot(atMs: number): Promise<SnapshotResponse> {
    return this.request<SnapshotResponse>(`/snapshot?at=${encodeURIComponent(String(atMs))}`);
  }

  // -- Durable history (SQL-backed; 503 when no SQL store is configured) ----

  /** `GET /history/sessions` — recent session-tree nodes (roots only unless `roots: false`). */
  historySessions(query: { since_ms?: number; limit?: number; roots?: boolean } = {}): Promise<SessionsResponse> {
    return this.request<SessionsResponse>(`/history/sessions${buildQuery(query)}`);
  }

  /** `GET /sessions/active` — the live hub's active-session cut (last 15 minutes). */
  activeSessions(): Promise<ActiveSessionsResponse> {
    return this.request<ActiveSessionsResponse>('/sessions/active');
  }

  /** `GET /history/sessions/:id` — one node with its ancestors, children and newest requests. */
  historySession(id: string, query: { limit?: number } = {}): Promise<SessionDetailResponse> {
    return this.request<SessionDetailResponse>(`/history/sessions/${encodeURIComponent(id)}${buildQuery(query)}`);
  }

  /**
   * `GET /history/requests/:id/body?hop=` — the FULL reassembled request body of one hop (the
   * content store joins the skeleton with its items). The response IS the body, not an envelope.
   */
  historyRequestBody(id: string, hop: HistoryBodyHop = 'client_in'): Promise<unknown> {
    return this.request<unknown>(`/history/requests/${encodeURIComponent(id)}/body?hop=${hop}`);
  }

  // -- Access management ----------------------------------------------------

  authSummary(): Promise<AuthSummary> {
    return this.request('/auth/summary', undefined, isAuthSummary);
  }
  authUsers(): Promise<AuthUsersResponse> {
    return this.request('/auth/users', undefined, isAuthUsersResponse);
  }
  authGroups(): Promise<AuthGroupsResponse> {
    return this.request('/auth/groups', undefined, isAuthGroupsResponse);
  }
  authRoles(): Promise<AuthRolesResponse> {
    return this.request('/auth/roles', undefined, isAuthRolesResponse);
  }
  authPolicies(): Promise<AuthPoliciesResponse> {
    return this.request('/auth/policies', undefined, isAuthPoliciesResponse);
  }
  authApiKeys(): Promise<AuthApiKeysResponse> {
    return this.request('/auth/api-keys', undefined, isAuthApiKeysResponse);
  }
  authSessions(): Promise<AuthSessionsResponse> {
    return this.request('/auth/sessions', undefined, isAuthSessionsResponse);
  }
  authUsage(): Promise<AuthUsageResponse> {
    return this.request('/auth/usage', undefined, isAuthUsageResponse);
  }
  authAudit(): Promise<AuthAuditResponse> {
    return this.request('/auth/audit', undefined, isAuthAuditResponse);
  }
  authPricing(): Promise<AuthPricingResponse> {
    return this.request('/auth/pricing', undefined, isAuthPricingResponse);
  }

  createAuthUser(body: CreateAuthUserRequest): Promise<AuthUsersResponse> {
    return this.authMutation('/auth/users', body, isAuthUsersResponse);
  }
  createAuthGroup(body: CreateAuthGroupRequest): Promise<AuthGroupsResponse> {
    return this.authMutation('/auth/groups', body, isAuthGroupsResponse);
  }
  createAuthRole(body: CreateAuthRoleRequest): Promise<AuthRolesResponse> {
    return this.authMutation('/auth/roles', body, isAuthRolesResponse);
  }
  createAuthApiKey(body: CreateAuthApiKeyRequest): Promise<CreatedAuthApiKey> {
    return this.authMutation('/auth/api-keys', body, isCreatedAuthApiKey);
  }
  createAuthPolicy(body: CreateAuthPolicyRequest): Promise<AuthPoliciesResponse> {
    return this.authMutation('/auth/policies', body, isAuthPoliciesResponse);
  }
  revokeAuthApiKey(id: string): Promise<AuthApiKeysResponse> {
    return this.authMutation(`/auth/api-keys/${encodeURIComponent(id)}/revoke`, undefined, isAuthApiKeysResponse);
  }
  rotateAuthApiKey(id: string): Promise<CreatedAuthApiKey> {
    return this.authMutation(`/auth/api-keys/${encodeURIComponent(id)}/rotate`, undefined, isCreatedAuthApiKey);
  }
  updateAuthApiKeyPayloadCapture(id: string, capturePayloads: boolean): Promise<AuthApiKeysResponse> {
    return this.authMutation(`/auth/api-keys/${encodeURIComponent(id)}/payload-capture`, { capture_payloads: capturePayloads }, isAuthApiKeysResponse);
  }
  revokeAuthSession(id: string): Promise<AuthSessionsResponse> {
    return this.authMutation(`/auth/sessions/${encodeURIComponent(id)}/revoke`, undefined, isAuthSessionsResponse);
  }

  private authMutation<T>(
    path: string,
    body: unknown,
    guard: (value: unknown) => value is T,
  ): Promise<T> {
    const csrf = this.getCsrfToken();
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (csrf) headers['X-CSRF-Token'] = csrf;
    return this.request(path, {
      method: 'POST',
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    }, guard);
  }

  // -- Mutation (CSRF-gated) ------------------------------------------------

  /** `POST /flows/:id/kill` — attaches `X-CSRF-Token` (D7). */
  kill(id: string): Promise<KillResponse> {
    const csrf = this.getCsrfToken();
    const headers: Record<string, string> = {};
    if (csrf) headers['X-CSRF-Token'] = csrf;
    return this.request<KillResponse>(`/flows/${encodeURIComponent(id)}/kill`, {
      method: 'POST',
      headers,
    });
  }

}

/** Serializes a query object into a `?a=b&c=d` string, dropping undefined/null values. */
function buildQuery(query: object): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query as Record<string, unknown>)) {
    if (v !== undefined && v !== null) params.set(k, String(v));
  }
  const s = params.toString();
  return s ? `?${s}` : '';
}

/**
 * Reads the double-submit CSRF token from a non-HttpOnly cookie (D7). The Rust shell
 * sets `csrf_token` in both a cookie and the SPA bootstrap; this reads the cookie form.
 */
export function readCsrfCookie(cookieName = 'llmconduit_csrf'): string | null {
  if (typeof document === 'undefined') return null;
  const match = document.cookie.split('; ').find((c) => c.startsWith(`${cookieName}=`));
  return match ? decodeURIComponent(match.slice(cookieName.length + 1)) : null;
}
