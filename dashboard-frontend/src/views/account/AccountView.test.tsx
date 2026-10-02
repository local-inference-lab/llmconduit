import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { cleanup, fireEvent, waitFor, within } from '@testing-library/react';
import { AccountView } from './AccountView';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { authStore } from '../../store/authStore';
import { getConnection } from '../../api/connection';

describe('AccountView (mock accounts API)', () => {
  beforeEach(() => {
    resetWorld({ mock: true });
    // The connection seeds the auth store from the bootstrap; build it first, then set the user.
    getConnection();
    authStore.getState().setCsrfToken('mock-csrf-token');
  });
  afterEach(cleanup);

  it('an admin sees everyone’s keys and the users panel; creating a key shows the secret once', async () => {
    authStore.getState().setAuthMode('users');
    authStore.getState().setUser({ id: 'user_admin', username: 'admin', is_admin: true });
    const { getByTestId, getAllByTestId, getByLabelText, getByRole, queryByTestId } = renderWithQuery(<AccountView />);
    expect(getByTestId('account-username').textContent).toBe('admin');
    expect(getByTestId('account-role').textContent).toBe('admin');
    await waitFor(() => expect(getAllByTestId('key-row').length).toBe(2));
    await waitFor(() => expect(getAllByTestId('user-row').length).toBe(2));

    fireEvent.change(getByLabelText('key label'), { target: { value: 'desk' } });
    fireEvent.change(getByLabelText('allowed models'), { target: { value: 'local, fast' } });
    fireEvent.click(getByRole('button', { name: 'create key' }));
    await waitFor(() => expect(queryByTestId('account-new-secret')).toBeTruthy());
    expect(getByTestId('account-secret').textContent).toMatch(/^llmc_/);
    await waitFor(() => expect(getAllByTestId('key-row').length).toBe(3));
    const created = getAllByTestId('key-row').find((row) => row.textContent?.includes('desk'))!;
    expect(created.textContent).toContain('local, fast');

    // Revoke it again.
    fireEvent.click(within(created).getByTestId('key-revoke'));
    await waitFor(() => expect(getAllByTestId('key-row').length).toBe(2));

    // Create a user; the admin cannot delete themselves.
    fireEvent.change(getByLabelText('new username'), { target: { value: 'ops' } });
    fireEvent.change(getByLabelText('new password'), { target: { value: 'ops-password' } });
    fireEvent.click(getByRole('button', { name: 'create user' }));
    await waitFor(() => expect(getAllByTestId('user-row').length).toBe(3));
    const self = getAllByTestId('user-row').find((row) => row.textContent?.includes('(you)'))!;
    expect(within(self).getByTestId('user-delete')).toBeDisabled();
  });

  it('a plain user sees only their own keys and no users panel', async () => {
    authStore.getState().setAuthMode('users');
    authStore.getState().setUser({ id: 'user_dev', username: 'dev', is_admin: false });
    const { getByTestId, getAllByTestId, queryByTestId } = renderWithQuery(<AccountView />);
    expect(getByTestId('account-role').textContent).toBe('user');
    await waitFor(() => expect(getAllByTestId('key-row').length).toBe(1));
    expect(getAllByTestId('key-row')[0]!.textContent).toContain('ci');
    expect(queryByTestId('account-users')).toBeNull();
  });
});
