import { describe, it, expect } from 'vitest';
import { parseBootstrap } from './env';
import { GOLDEN_BOOTSTRAP } from '../api/ws.fixtures';

describe('bootstrap parsing — frozen field names (finding 6)', () => {
  it('parses the GOLDEN bootstrap D7 embeds (authenticated/csrf_token/mutations_enabled)', () => {
    const boot = parseBootstrap(GOLDEN_BOOTSTRAP);
    expect(boot).toEqual({
      authenticated: true,
      csrf_token: 'csrf-abc123',
      mutations_enabled: true,
    user: null,
    auth_mode: 'token',
    });
  });

  it('defaults safely for an UNAUTHENTICATED bootstrap', () => {
    expect(parseBootstrap({ authenticated: false, csrf_token: null, mutations_enabled: false, user: null, auth_mode: 'token' })).toEqual({
      authenticated: false,
      csrf_token: null,
      mutations_enabled: false,
    user: null,
    auth_mode: 'token',
    });
  });

  it('coerces ill-typed / missing fields to safe defaults (no crash, no silent mutations)', () => {
    expect(parseBootstrap({})).toEqual({ authenticated: false, csrf_token: null, mutations_enabled: false, user: null, auth_mode: 'token' });
    expect(parseBootstrap(null)).toEqual({ authenticated: false, csrf_token: null, mutations_enabled: false, user: null, auth_mode: 'token' });
    // A truthy-but-not-true `authenticated` must NOT authenticate; a non-string token → null.
    expect(parseBootstrap({ authenticated: 'yes', csrf_token: 123, mutations_enabled: 1 })).toEqual({
      authenticated: false,
      csrf_token: null,
      mutations_enabled: false,
    user: null,
    auth_mode: 'token',
    });
  });

  it('preserves the GitHub authentication mode', () => {
    expect(parseBootstrap({ auth_mode: 'github' }).auth_mode).toBe('github');
  });
});
