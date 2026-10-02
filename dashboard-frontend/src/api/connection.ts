/**
 * Wires the REST client + DashboardSocket to the stores, selecting mock vs real
 * transport from the env flag. This is the single composition root the app boots from;
 * views consume the stores (live state) + a TanStack Query client (REST cache) that this
 * module configures with the mock/real client.
 *
 * WS-driven invalidation (finding 10): after an ACCEPTED (post-dedup) frame, the socket
 * fires `onFrameApplied(domain)`; we map the domain → the affected query keys and
 * invalidate them so the REST cache refetches the authoritative shape.
 */
import { QueryClient } from '@tanstack/react-query';
import { DashboardClient, readCsrfCookie } from './client';
import { DashboardSocket } from './ws';
import { mockFetch, mockWsFactory } from './mock';
import { isMockEnabled, readBootstrap } from '../config/env';
import { authStore } from '../store/authStore';
import { dashboardStore } from '../store/dashboardStore';
import { startSankeyFold, __resetSankeyFold } from '../store/useSankeyWindow';
import type { Domain } from './types';

/** Stable query keys; the WS invalidation + the views both reference these. */
export const queryKeys = {
  flows: ['flows'] as const,
  flowDetail: (id: string) => ['flows', id] as const,
  metrics: ['metrics'] as const,
  topology: ['topology'] as const,
  providers: ['providers'] as const,
  configuredProviders: ['configured-providers'] as const,
  fleet: ['fleet'] as const,
  mesh: ['mesh'] as const,
  providerMetrics: ['provider-metrics'] as const,
  catalog: ['catalog'] as const,
  // Durable history (sessions view + the inspector's chain diff).
  sessions: ['history', 'sessions'] as const,
  activeSessions: ['history', 'sessions', 'active'] as const,
  session: (id: string) => ['history', 'sessions', id] as const,
  requestBody: (id: string, hop: string) => ['history', 'requests', id, 'body', hop] as const,
  throughput: ['history', 'throughput'] as const,
  activity: ['history', 'activity'] as const,
  historyMetrics: ['history', 'metrics'] as const,
  me: ['me'] as const,
  users: ['users'] as const,
  keys: (scope: string) => ['keys', scope] as const,
  authUsers: ['auth', 'users'] as const,
  authApiKeys: ['auth', 'api-keys'] as const,
} as const;

export interface Connection {
  client: DashboardClient;
  socket: DashboardSocket;
  queryClient: QueryClient;
  mock: boolean;
}

let singleton: Connection | null = null;

/**
 * Resolves the double-submit CSRF token at CALL time (finding 2): a token issued during a
 * fresh login lands in the `llmconduit_csrf` cookie, so we prefer the live cookie and fall
 * back to the auth store (seeded from the SPA bootstrap). This way a kill after login
 * always carries the current token.
 */
function resolveCsrfToken(): string | null {
  return readCsrfCookie() ?? authStore.getState().csrfToken;
}

/** Builds (once) the client+socket+query-client, seeding auth state from the bootstrap. */
export function getConnection(): Connection {
  if (singleton) return singleton;

  const mock = isMockEnabled();
  const boot = readBootstrap();

  // Seed auth store from the bootstrap (D7 double-submit CSRF + auth state).
  authStore.getState().setAuthenticated(boot.authenticated);
  authStore.getState().setCsrfToken(boot.csrf_token);
  authStore.getState().setMutationsEnabled(boot.mutations_enabled);
  authStore.getState().setUser(boot.user);
  authStore.getState().setAuthMode(boot.auth_mode);

  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        // WS frames drive invalidation; keep REST reads cheap + non-chatty.
        staleTime: 5_000,
        refetchOnWindowFocus: false,
        retry: 1,
      },
    },
  });

  // Finding 1: BOTH the logout control and any 401/unauthorized handler route through the
  // ONE centralized teardown so the sensitive REST cache + live store never survive a
  // session boundary.
  const onUnauthorized = () => teardownSession();

  const client = new DashboardClient({
    fetchImpl: mock ? mockFetch : undefined,
    // Dynamic: cookie-first, bootstrap fallback — read fresh on every kill.
    getCsrfToken: resolveCsrfToken,
    onUnauthorized,
  });

  const socket = new DashboardSocket({
    factory: mock ? mockWsFactory : undefined,
    onUnauthorized,
    onFrameApplied: (domain) => invalidateForDomain(queryClient, domain),
    // Finding 7: after a transient WS drop, probe a protected endpoint — only a 401
    // bounces to login; otherwise the socket reconnects.
    probeAuth: () => client.probeAuth(),
  });

  // Start the APP-LIFETIME Sankey fold engine from the composition root (D12 R3): it subscribes to
  // the store directly and folds each flow's usage GROWTH into a timestamped delta as frames arrive,
  // stamping every delta at its REAL arrival time regardless of which view is mounted. The SankeyView
  // only READS the maintained ring — so usage that grew while the Sankey was unmounted is recorded at
  // its real arrival time and correctly ages out of the 30 s window, never lumped in at remount.
  startSankeyFold();

  singleton = { client, socket, queryClient, mock };
  return singleton;
}

/**
 * Centralized session teardown (finding 1). Disconnects the WS, CLEARS the TanStack Query
 * cache (which holds bodies/headers/usage), and resets BOTH zustand stores to their
 * initial state — so no session-scoped data (cached flow bodies, live frames, CSRF token,
 * mutation flag) survives a logout or a 401. Idempotent + safe before `getConnection()`.
 */
export function teardownSession(): void {
  if (singleton) {
    singleton.socket.disconnect();
    singleton.queryClient.clear();
  }
  dashboardStore.getState().reset();
  authStore.getState().reset();
}

/** Maps an accepted WS domain to the REST queries it invalidates. */
function invalidateForDomain(queryClient: QueryClient, domain: Domain): void {
  switch (domain) {
    case 'flow':
      // A flow frame changes the list AND whatever detail is open.
      void queryClient.invalidateQueries({ queryKey: queryKeys.flows });
      return;
    case 'metrics':
      void queryClient.invalidateQueries({ queryKey: queryKeys.metrics });
      // Gap 13: the REST `/topology` node's per_provider tile (p50/p95/p99 + error rate) is JOINED
      // from the SAME m1 metrics window (Rust `from_health_with_metrics`), but the live WS
      // `topology_update` frame carries `per_provider` ABSENT — so without this the per-provider
      // metrics would stay STALE until an UNRELATED topology change happened to refetch `/topology`
      // (e.g. old values lingering after a provider drops to zero in-window samples). A metrics frame
      // moves that window, so it must ALSO refresh the topology query that joins it.
      void queryClient.invalidateQueries({ queryKey: queryKeys.topology });
      return;
    case 'topology':
      void queryClient.invalidateQueries({ queryKey: queryKeys.topology });
      return;
    case 'monitor':
      // Monitor deltas feed the live store directly; no REST query mirrors them.
      return;
    case 'sessions':
      // The hub tick is a push notification: refetch the active-session board.
      void queryClient.invalidateQueries({ queryKey: queryKeys.activeSessions });
      return;
  }
}

/** Test/HMR reset of the singleton (also stops the app-lifetime Sankey fold engine it started). */
export function resetConnection(): void {
  singleton?.socket.disconnect();
  singleton = null;
  // Pair the engine teardown with the singleton reset so a fresh `getConnection()` re-starts it
  // (HMR) and tests don't leak a running fold subscription / accumulated ring across cases.
  __resetSankeyFold();
}
