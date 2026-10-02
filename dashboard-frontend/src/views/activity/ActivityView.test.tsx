import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { cleanup, waitFor } from '@testing-library/react';
import { ActivityView } from './ActivityView';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { authStore } from '../../store/authStore';
import { getConnection } from '../../api/connection';

describe('ActivityView (mock history API)', () => {
  beforeEach(() => {
    resetWorld({ mock: true });
    // The connection seeds the auth store from the bootstrap; build it first, then set the user.
    getConnection();
    authStore.getState().setAuthMode('users');
    authStore.getState().setUser({ id: 'user_admin', username: 'admin', is_admin: true });
  });
  afterEach(cleanup);

  it('lists activity per user and key with names resolved, unattributed traffic included', async () => {
    const { getByTestId, getAllByTestId } = renderWithQuery(<ActivityView />);
    await waitFor(() => expect(getAllByTestId('activity-row').length).toBe(4));
    const users = getAllByTestId('activity-row').map((r) => r.getAttribute('data-user'));
    expect(users).toEqual(['user_admin', 'user_dev', 'usr_ops', 'none']);
    await waitFor(() => expect(getAllByTestId('activity-user')[0]!.textContent).toBe('admin'));
    await waitFor(() => expect(getAllByTestId('activity-user')[2]!.textContent).toBe('Operations'));
    expect(getAllByTestId('activity-key')[2]!.textContent).toBe('operator laptop');
    expect(getAllByTestId('activity-user')[3]!.textContent).toBe('unattributed');
    expect(getByTestId('act-requests').getAttribute('data-quality')).toBe('measured');
    expect(getByTestId('act-principals').textContent).toContain('4 · 4');
    // Each row carries a request sparkline with data.
    expect(getAllByTestId('activity-row')[0]!.querySelector('svg')!.getAttribute('data-available')).toBe('true');
  });
});
