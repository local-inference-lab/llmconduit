/**
 * Runtime environment + the mock-vs-real selector.
 *
 * Mock is enabled when:
 *  - the Vite dev server is running (`import.meta.env.DEV`) and no real bootstrap is
 *    present, OR
 *  - `?mock=1` is in the URL (force on), unless `?mock=0` forces it off.
 *
 * The Rust shell embeds a real bootstrap (`window.__LLMCONDUIT_DASHBOARD__`) with the
 * CSRF token + auth state (D7). When that object is present we run against the real
 * backend; otherwise we fall back to the in-browser mock so views render in `npm run dev`.
 */
import type { DashboardBootstrap } from '../api/types';
import { mockBootstrapCsrf } from '../api/mock';

declare global {
  interface Window {
    __LLMCONDUIT_DASHBOARD__?: DashboardBootstrap;
  }
}

function urlFlag(name: string): string | null {
  if (typeof window === 'undefined') return null;
  return new URLSearchParams(window.location.search).get(name);
}

export function isMockEnabled(): boolean {
  const forced = urlFlag('mock');
  if (forced === '1') return true;
  if (forced === '0') return false;
  const hasRealBootstrap = typeof window !== 'undefined' && !!window.__LLMCONDUIT_DASHBOARD__;
  if (hasRealBootstrap) return false;
  // Vite injects `import.meta.env.DEV`; default to mock in dev.
  return import.meta.env.DEV;
}

/**
 * Validates + normalizes a raw `window.__LLMCONDUIT_DASHBOARD__` object into a
 * `DashboardBootstrap` (finding 6). The FROZEN field names D7 emits are exactly
 * `authenticated: boolean`, `csrf_token: string | null`, `mutations_enabled: boolean`.
 * Missing/ill-typed fields default safely (unauthenticated, no token, mutations off) so a
 * malformed embed can never crash boot or silently grant mutations.
 */
export function parseBootstrap(raw: unknown): DashboardBootstrap {
  const obj = (typeof raw === 'object' && raw !== null ? raw : {}) as Record<string, unknown>;
  const user = obj.user;
  const parsedUser =
    typeof user === 'object' && user !== null &&
    typeof (user as Record<string, unknown>).id === 'string' &&
    typeof (user as Record<string, unknown>).username === 'string'
      ? {
          id: (user as Record<string, unknown>).id as string,
          username: (user as Record<string, unknown>).username as string,
          is_admin: (user as Record<string, unknown>).is_admin === true,
        }
      : null;
  const mode = obj.auth_mode;
  return {
    authenticated: obj.authenticated === true,
    csrf_token: typeof obj.csrf_token === 'string' ? obj.csrf_token : null,
    mutations_enabled: obj.mutations_enabled === true,
    user: parsedUser,
    auth_mode: mode === 'users' || mode === 'open' || mode === 'github' ? mode : 'token',
  };
}

/**
 * Reads the SPA bootstrap. With a real Rust shell this is `window.__LLMCONDUIT_DASHBOARD__`
 * (validated via `parseBootstrap`). Under the mock we synthesize an UNAUTHENTICATED
 * bootstrap (the login shell renders; a successful mock login flips auth) carrying the
 * mock CSRF token + mutations enabled.
 */
export function readBootstrap(): DashboardBootstrap {
  if (typeof window !== 'undefined' && window.__LLMCONDUIT_DASHBOARD__) {
    return parseBootstrap(window.__LLMCONDUIT_DASHBOARD__);
  }
  return {
    authenticated: false,
    csrf_token: isMockEnabled() ? mockBootstrapCsrf : null,
    mutations_enabled: isMockEnabled(),
    user: null,
    auth_mode: 'token',
  };
}
