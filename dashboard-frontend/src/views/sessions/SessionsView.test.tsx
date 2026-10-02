import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { cleanup, fireEvent, waitFor, within } from '@testing-library/react';
import { SessionsView } from './SessionsView';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { flowFilterStore } from '../../store/flowFilterStore';

/** The Sessions view: the ACTIVE board (live hub) + the durable 7-day tree. */
describe('SessionsView (mock APIs)', () => {
  beforeEach(() => {
    resetWorld({ mock: true });
    window.location.hash = '#/sessions';
  });
  afterEach(cleanup);

  // -- the active board (primary) --------------------------------------------

  it('lists active sessions with user/key attribution and window counts', async () => {
    const { getAllByTestId, getByTestId } = renderWithQuery(<SessionsView />);
    await waitFor(() => expect(getAllByTestId('active-session').length).toBe(3));
    const root = getAllByTestId('active-session')[0]!;
    // The mock seeds user_dev (dev) + key_dev_ci (ci) on the claude-code root.
    await waitFor(() => expect(within(root).getByTestId('active-session-user').textContent).toBe('dev'));
    expect(within(root).getByTestId('active-session-key').textContent).toBe('ci');
    expect(within(root).getByTestId('active-session-total').textContent).toBe('2');
    expect(getByTestId('sessions-count').textContent).toBe('3 active');
  });

  it('unfolds a session into its requests with status and tokens', async () => {
    const { getAllByTestId, getByTestId } = renderWithQuery(<SessionsView />);
    await waitFor(() => expect(getAllByTestId('active-session').length).toBe(3));
    const row = getAllByTestId('active-session')[0]!;
    fireEvent.click(within(row).getByTestId('active-session-row'));
    const requests = within(getByTestId('active-session-requests')).getAllByTestId('active-request');
    expect(requests.length).toBe(2);
    // Newest first; one running, one completed.
    expect(requests[0]!.getAttribute('data-status')).toBe('running');
    expect(requests[1]!.getAttribute('data-status')).toBe('completed');
  });

  it('cross-links an active session into the flows table', async () => {
    const { getAllByTestId, getByTestId } = renderWithQuery(<SessionsView />);
    await waitFor(() => expect(getAllByTestId('active-session').length).toBe(3));
    const row = getAllByTestId('active-session')[0]!;
    fireEvent.click(within(row).getByTestId('active-session-row'));
    fireEvent.click(getByTestId('active-session-show-in-flows'));
    expect(flowFilterStore.getState().filters.session).toBe('sess_root');
    expect(window.location.hash).toBe('#/flows');
  });

  it('a session_update WS frame refetches the board (real-time invalidation)', async () => {
    const { getAllByTestId } = renderWithQuery(<SessionsView />);
    await waitFor(() => expect(getAllByTestId('active-session').length).toBe(3));
    // The mock's staggered live frames include a sessions-domain tick; after it
    // lands the query is invalidated and re-resolved (same mock data → same
    // count, but the frame was ACCEPTED, not rejected by the domain guard).
    await waitFor(() => expect(getAllByTestId('active-session').length).toBe(3), { timeout: 3000 });
  });

  // -- the durable 7-day tree (behind the toggle) -----------------------------

  it('lists root sessions in the tree with harness + kind badges', async () => {
    const { getByTestId, getAllByTestId } = renderWithQuery(<SessionsView />);
    fireEvent.click(getByTestId('sessions-tree-toggle'));
    await waitFor(() => expect(getAllByTestId('session-row').length).toBe(2));
    const rows = getAllByTestId('session-row');
    const harnesses = rows.map((r) => within(r).getByTestId('session-harness').textContent);
    expect(harnesses.sort()).toEqual(['claude-code', 'codex']);
    expect(rows.every((r) => within(r).getByTestId('session-kind').getAttribute('data-kind') === 'declared')).toBe(true);
    expect(getByTestId('sessions-tree-detail-empty')).toBeTruthy();
  });

  it('selecting a tree session shows sub-sessions and requests with lineage chips', async () => {
    const { getAllByTestId, getByTestId, findByTestId } = renderWithQuery(<SessionsView />);
    fireEvent.click(getByTestId('sessions-tree-toggle'));
    await waitFor(() => expect(getAllByTestId('session-row').length).toBe(2));
    const claude = getAllByTestId('session-row').find((r) => within(r).getByTestId('session-harness').textContent === 'claude-code')!;
    fireEvent.click(claude);
    await findByTestId('session-detail');
    const children = within(getByTestId('session-children')).getAllByTestId('session-row');
    expect(children.length).toBe(1);
    expect(within(children[0]!).getByTestId('session-kind').getAttribute('data-kind')).toBe('inferred');
    const requests = within(getByTestId('session-requests')).getAllByTestId('session-request');
    expect(requests.length).toBe(2);
    const kinds = requests.map((r) => within(r).getByTestId('session-request-lineage').getAttribute('data-kind'));
    expect(kinds).toEqual(['new_chain', 'append']);
    expect(getByTestId('session-stat-busts').textContent).toContain('0');

    fireEvent.click(children[0]!);
    await waitFor(() => expect(within(getByTestId('session-breadcrumb')).getAllByRole('button').length).toBe(1));
    await waitFor(() => expect(within(getByTestId('session-requests')).getAllByTestId('session-request').length).toBe(1));
  });

  it('flags cache busts on the codex tree session and cross-links into flows', async () => {
    const { getAllByTestId, getByTestId, findByTestId } = renderWithQuery(<SessionsView />);
    fireEvent.click(getByTestId('sessions-tree-toggle'));
    await waitFor(() => expect(getAllByTestId('session-row').length).toBe(2));
    const codex = getAllByTestId('session-row').find((r) => within(r).getByTestId('session-harness').textContent === 'codex')!;
    fireEvent.click(codex);
    await findByTestId('session-detail');
    const busted = within(getByTestId('session-requests')).getAllByTestId('session-request').filter((r) => r.getAttribute('data-cache-bust') === 'true');
    expect(busted.length).toBe(1);
    expect(within(busted[0]!).getByTestId('session-request-lineage').textContent).toBe('bust · tools');

    fireEvent.click(getByTestId('session-show-in-flows'));
    expect(flowFilterStore.getState().filters.session).toBe('sess_codex');
    expect(window.location.hash).toBe('#/flows');
  });
});
