/**
 * Auth/session state (zustand vanilla, bridged via useSyncExternalStore). The shell
 * reads `authenticated` to decide login-shell vs dashboard. On any 401 / logout the app
 * calls `teardownSession()` (connection.ts), which invokes `reset()` here (full clear) —
 * so no session-scoped secret survives. `bounceToLogin()` remains as a narrow
 * "auth flag only" helper for callers that don't need the full teardown.
 */
import { createStore } from 'zustand/vanilla';
import type { AuthMode, SessionUser } from '../api/types';

export interface AuthState {
  authenticated: boolean;
  /** Double-submit CSRF token from the bootstrap/cookie (D7). */
  csrfToken: string | null;
  mutationsEnabled: boolean;
  /** The signed-in user; null for a token / dev-open session. */
  user: SessionUser | null;
  authMode: AuthMode;
  setUser: (user: SessionUser | null) => void;
  setAuthMode: (mode: AuthMode) => void;

  setAuthenticated: (v: boolean) => void;
  setCsrfToken: (t: string | null) => void;
  setMutationsEnabled: (v: boolean) => void;
  /** Any 401 → drop back to the login shell. */
  bounceToLogin: () => void;
  /**
   * Full reset to the initial UNAUTHENTICATED state — clears the CSRF token + mutation
   * flag too (not just `authenticated`). Called by `teardownSession()` on logout / 401 so
   * no session-scoped secret survives across sessions.
   */
  reset: () => void;
}

export const authStore = createStore<AuthState>((set) => ({
  authenticated: false,
  csrfToken: null,
  mutationsEnabled: false,
  user: null,
  authMode: 'token',

  setUser: (user) => set({ user }),
  setAuthMode: (authMode) => set({ authMode }),
  setAuthenticated: (authenticated) => set({ authenticated }),
  setCsrfToken: (csrfToken) => set({ csrfToken }),
  setMutationsEnabled: (mutationsEnabled) => set({ mutationsEnabled }),
  bounceToLogin: () => set({ authenticated: false }),
  reset: () => set({ authenticated: false, csrfToken: null, mutationsEnabled: false, user: null }),
}));

export type AuthStore = typeof authStore;
