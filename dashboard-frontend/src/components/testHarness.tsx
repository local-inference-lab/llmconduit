/**
 * Shared test scaffolding for the D10 component/integration tests: a `renderWithQuery` that wraps
 * a tree in a fresh `QueryClientProvider` (retries off, no refetch) and resets the live stores +
 * the connection singleton so each test starts clean. Mirrors the app's composition (main.tsx
 * wraps in QueryClientProvider; connection.ts is the singleton root).
 */
import type { ReactElement } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, type RenderResult } from '@testing-library/react';
import { getConnection, resetConnection } from '../api/connection';
import { dashboardStore } from '../store/dashboardStore';
import { authStore } from '../store/authStore';
import { flowFilterStore } from '../store/flowFilterStore';
import { resetMockAccounts } from '../api/mock';
import type { FlowSummary } from '../api/types';

/**
 * Reset all global singletons so a test starts from a known-empty state.
 *
 * By default it installs a real (authenticated) bootstrap so `isMockEnabled()` returns false —
 * this DISABLES the `/flows` REST poll in `useFlowRows`, letting tests drive the table purely
 * through the live store (deterministic, no async fetch churn). Pass `{ mock: true }` to exercise
 * the mock-backed path (e.g. the kill round-trip, which needs `mockFetch` + mutations enabled).
 */
export function resetWorld(opts: { mock?: boolean } = {}): void {
  resetConnection();
  dashboardStore.getState().reset();
  authStore.getState().reset();
  // The shared FlowTable filter (D12) is a global singleton — clear it so a filter set by one
  // test (or a Topology/Sankey cross-link) never carries into the next.
  flowFilterStore.getState().clear();
  resetMockAccounts();
  if (opts.mock) {
    delete window.__LLMCONDUIT_DASHBOARD__;
  } else {
    window.__LLMCONDUIT_DASHBOARD__ = { authenticated: true, csrf_token: 'test-csrf', mutations_enabled: true, user: null, auth_mode: 'token' };
  }
}

/** Render `ui` inside the connection's QueryClient (built fresh per `getConnection`). */
export function renderWithQuery(ui: ReactElement): RenderResult & { queryClient: QueryClient } {
  const { queryClient } = getConnection();
  const result = render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>);
  return Object.assign(result, { queryClient });
}

/** Push a batch of flows into the live store via the snapshot path (newest-on-top order). */
export function seedFlows(flows: FlowSummary[]): void {
  dashboardStore.getState().applySnapshot({
    cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 },
    flows,
    metrics: null,
    topology: null,
  });
}

/** A minimal valid `FlowSummary` for table tests. */
export function makeFlow(over: Partial<FlowSummary> = {}): FlowSummary {
  return {
    api_call_id: `api_${Math.random().toString(36).slice(2, 8)}`,
    method: 'POST',
    uri: '/v1/responses',
    status: 'completed',
    started_ms: 1_700_000_000_000,
    // Gap 07: every row carries a cost-confidence tag; default to `unavailable` (no price).
    cost_confidence: 'unavailable',
    ...over,
  };
}
