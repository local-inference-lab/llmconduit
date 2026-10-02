import { useState, type FormEvent } from 'react';
import { Panel } from './ui/Panel';
import { Button } from './ui/Button';
import { authStore } from '../store/authStore';
import { useAuth } from '../store/hooks';
import type { DashboardClient } from '../api/client';
import type { SessionUser } from '../api/types';

const INPUT = 'rounded-md border border-line bg-panel-raised px-3 py-2 font-mono text-sm text-text outline-none focus:border-accent';

const ERROR_MESSAGES: Record<string, string> = {
  github_cancelled: 'GitHub sign-in was cancelled.',
  github_configuration: 'GitHub SSO is not configured on this server.',
  github_state: 'The GitHub sign-in request expired or could not be verified.',
  github_exchange: 'GitHub could not complete the sign-in exchange.',
  github_profile: 'The GitHub profile could not be loaded.',
  github_denied: 'This GitHub account is not allowed to use the dashboard.',
};

export function LoginShell({ client }: { client: DashboardClient }) {
  const authMode = useAuth((state) => state.authMode);
  if (authMode !== 'github') return <LegacyLogin client={client} authMode={authMode} />;

  const reason = typeof window === 'undefined' ? null : new URLSearchParams(window.location.search).get('login_error');
  const error = reason ? ERROR_MESSAGES[reason] ?? 'GitHub sign-in failed.' : null;
  return (
    <div className="flex h-full items-center justify-center bg-bg p-4">
      <Panel className="w-full max-w-sm p-6">
        <h1 className="mb-1 text-lg font-semibold text-text">llmconduit</h1>
        <p className="mb-5 text-sm text-text-muted" data-testid="login-subtitle">Sign in with an approved GitHub account.</p>
        {error && <p role="alert" className="mb-3 text-sm text-status-down">{error}</p>}
        <a href="/dashboard/auth/github/start" className="flex w-full items-center justify-center gap-2 rounded-md border border-line bg-panel-raised px-4 py-2 text-sm font-medium text-text transition-colors hover:border-accent hover:text-accent">
          <GithubMark />
          Continue with GitHub
        </a>
        <p className="mt-4 text-xs leading-relaxed text-text-muted">Access is controlled by the server-side GitHub allow-list.</p>
      </Panel>
    </div>
  );
}

function LegacyLogin({ client, authMode }: { client: DashboardClient; authMode: 'users' | 'token' | 'open' }) {
  const [tokenMode, setTokenMode] = useState(authMode !== 'users');
  const [token, setToken] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      let user: SessionUser | null = null;
      if (tokenMode) {
        if (token.startsWith('llmc_')) await client.keyLogin(token);
        else ({ user } = await client.login({ token }));
      } else {
        ({ user } = await client.login({ username, password }));
      }
      authStore.getState().setUser(user);
      authStore.getState().setAuthenticated(true);
    } catch {
      setError(tokenMode ? 'Invalid token.' : 'Invalid username or password.');
    } finally {
      setBusy(false);
    }
  }

  const canSubmit = tokenMode ? token.length > 0 : username.length > 0 && password.length > 0;
  return (
    <div className="flex h-full items-center justify-center bg-bg p-4">
      <Panel className="w-full max-w-sm p-6">
        <h1 className="mb-1 text-lg font-semibold text-text">llmconduit</h1>
        <p className="mb-4 text-sm text-text-muted" data-testid="login-subtitle">
          {tokenMode ? 'Dashboard token or management-enabled API key required.' : 'Sign in with your username and password.'}
        </p>
        <form onSubmit={onSubmit} className="flex flex-col gap-3" data-testid="login-form" data-mode={tokenMode ? 'token' : 'users'}>
          {tokenMode ? (
            <label className="flex flex-col gap-1 text-sm">
              <span className="text-text-muted">Token</span>
              <input type="password" autoFocus value={token} onChange={(event) => setToken(event.target.value)} className={INPUT} placeholder="Dashboard token or llmc_…" aria-label="Dashboard token" />
            </label>
          ) : (
            <>
              <label className="flex flex-col gap-1 text-sm">
                <span className="text-text-muted">Username</span>
                <input type="text" autoFocus autoComplete="username" value={username} onChange={(event) => setUsername(event.target.value)} className={INPUT} aria-label="Username" />
              </label>
              <label className="flex flex-col gap-1 text-sm">
                <span className="text-text-muted">Password</span>
                <input type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} className={INPUT} aria-label="Password" />
              </label>
            </>
          )}
          {error && <p role="alert" className="text-sm text-status-down">{error}</p>}
          <Button type="submit" disabled={busy || !canSubmit}>{busy ? 'Signing in…' : 'Sign in'}</Button>
          <button type="button" className="text-left text-xs text-text-muted hover:text-text" onClick={() => { setTokenMode((mode) => !mode); setError(null); }} data-testid="login-toggle">
            {tokenMode ? 'Use a username and password instead' : 'Use an access token instead'}
          </button>
        </form>
      </Panel>
    </div>
  );
}

function GithubMark() {
  return (
    <svg viewBox="0 0 24 24" className="h-4 w-4 fill-current" aria-hidden="true">
      <path d="M12 .7a11.5 11.5 0 0 0-3.64 22.41c.58.1.79-.25.79-.56v-2.23c-3.22.7-3.9-1.37-3.9-1.37-.52-1.34-1.28-1.7-1.28-1.7-1.05-.72.08-.7.08-.7 1.16.08 1.77 1.19 1.77 1.19 1.03 1.77 2.7 1.26 3.36.96.1-.75.4-1.26.73-1.55-2.57-.29-5.27-1.28-5.27-5.69 0-1.26.45-2.28 1.19-3.09-.12-.29-.52-1.47.11-3.05 0 0 .97-.31 3.16 1.18A10.98 10.98 0 0 1 12 6.11c.98 0 1.95.13 2.86.39 2.2-1.49 3.16-1.18 3.16-1.18.63 1.58.23 2.76.11 3.05.74.81 1.19 1.83 1.19 3.09 0 4.42-2.71 5.39-5.29 5.68.42.36.79 1.07.79 2.17v3.24c0 .31.21.67.8.56A11.5 11.5 0 0 0 12 .7Z" />
    </svg>
  );
}
