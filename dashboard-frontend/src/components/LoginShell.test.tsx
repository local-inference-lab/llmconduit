import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { cleanup, render, screen, waitFor, act } from '@testing-library/react';
import { LoginShell } from './LoginShell';
import { App } from '../App';
import { QueryClientProvider } from '@tanstack/react-query';
import { DashboardClient } from '../api/client';
import { authStore } from '../store/authStore';
import { getConnection, resetConnection } from '../api/connection';

describe('LoginShell', () => {
  beforeEach(() => {
    window.history.replaceState({}, '', '/dashboard');
    authStore.getState().setAuthenticated(false);
    authStore.getState().setAuthMode('github');
  });

  it('offers GitHub SSO without exposing token or password fields', () => {
    render(<LoginShell client={new DashboardClient()} />);
    expect(screen.getByRole('link', { name: /continue with github/i })).toHaveAttribute('href', '/dashboard/auth/github/start');
    expect(screen.queryByLabelText(/token/i)).toBeNull();
    expect(screen.queryByLabelText(/password/i)).toBeNull();
  });

  it('preserves the legacy token login when GitHub SSO is disabled', () => {
    authStore.getState().setAuthMode('token');
    render(<LoginShell client={new DashboardClient()} />);
    expect(screen.getByLabelText(/dashboard token/i)).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: /continue with github/i })).toBeNull();
  });

  it.each([
    ['github_cancelled', /cancelled/i],
    ['github_configuration', /not configured/i],
    ['github_state', /expired or could not be verified/i],
    ['github_exchange', /could not complete/i],
    ['github_profile', /profile could not be loaded/i],
    ['github_denied', /not allowed/i],
    ['<script>alert(1)</script>', /gitHub sign-in failed/i],
  ])('renders a safe callback failure message for %s', (reason, expected) => {
    window.history.replaceState({}, '', `/dashboard?login_error=${encodeURIComponent(reason)}`);
    render(<LoginShell client={new DashboardClient()} />);
    expect(screen.getByRole('alert')).toHaveTextContent(expected);
    expect(screen.getByRole('alert')).not.toHaveTextContent(reason);
    cleanup();
  });
});

describe('App auth gate — unauthed load renders login; 401 bounces back', () => {
  beforeEach(() => {
    window.history.replaceState({}, '', '/dashboard');
    resetConnection();
    window.__LLMCONDUIT_DASHBOARD__ = {
      authenticated: false,
      csrf_token: null,
      mutations_enabled: false,
      user: null,
      auth_mode: 'github',
    };
    authStore.getState().setAuthenticated(false);
  });
  afterEach(() => {
    cleanup();
    resetConnection();
    delete window.__LLMCONDUIT_DASHBOARD__;
  });

  it('renders the GitHub login shell on an unauthenticated load', () => {
    const { queryClient } = getConnection();
    render(<QueryClientProvider client={queryClient}><App /></QueryClientProvider>);
    expect(screen.getByRole('link', { name: /continue with github/i })).toBeInTheDocument();
  });

  it('a 401 drops an authenticated dashboard back to GitHub login', async () => {
    const { queryClient } = getConnection();
    authStore.getState().setAuthenticated(true);
    const { rerender } = render(<QueryClientProvider client={queryClient}><App /></QueryClientProvider>);
    await waitFor(() => expect(screen.queryByRole('link', { name: /continue with github/i })).not.toBeInTheDocument());
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 0)); });
    act(() => authStore.getState().bounceToLogin());
    rerender(<QueryClientProvider client={queryClient}><App /></QueryClientProvider>);
    await waitFor(() => expect(screen.getByRole('link', { name: /continue with github/i })).toBeInTheDocument());
  });
});

describe('CSRF read from bootstrap/cookie is sent on kill', () => {
  beforeEach(() => resetConnection());
  it('connection seeds csrfToken from bootstrap and sends it', async () => {
    const { client } = getConnection();
    expect(authStore.getState().csrfToken).toBeTruthy();
    expect((await client.kill('api_001')).killed).toBe(true);
  });
});
