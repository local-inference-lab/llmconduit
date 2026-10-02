import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { cleanup, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { AccessView } from './AccessView';
import { renderWithQuery, resetWorld } from '../../components/testHarness';

beforeEach(() => resetWorld({ mock: true }));
afterEach(cleanup);

describe('AccessView management lifecycle', () => {
  it('keeps flow metadata while payload capture is explicitly toggled per key', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    fireEvent.click(screen.getByRole('button', { name: /close create access/i }));
    fireEvent.click(screen.getByRole('tab', { name: /^model api keys$/i }));
    expect(screen.getByText(/flow metadata, timings, usage, and status are always retained/i)).toBeInTheDocument();
    const toggle = screen.getByRole('switch', { name: 'metadata only' });
    expect(toggle).toHaveAttribute('aria-checked', 'false');
    fireEvent.click(toggle);

    await waitFor(() => {
      const enabled = screen.getByRole('switch', { name: 'captured' });
      expect(enabled).toHaveAttribute('aria-checked', 'true');
    });
  });

  it('rotates a key with copy-once disclosure and then forgets the raw secret', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    fireEvent.click(screen.getByRole('button', { name: /close create access/i }));
    fireEvent.click(screen.getByRole('tab', { name: /^model api keys$/i }));
    const rotate = screen.getAllByRole('button', { name: 'Rotate' })[0];
    expect(rotate).toBeDefined();
    fireEvent.click(rotate!);
    const dialog = await screen.findByRole('dialog', { name: /copy api key/i });
    const raw = within(dialog).getByTestId('raw-api-key').textContent;
    expect(raw).toMatch(/^llmc_/);
    fireEvent.click(within(dialog).getByRole('button', { name: /i stored it/i }));

    await waitFor(() => expect(screen.queryByRole('dialog', { name: /copy api key/i })).not.toBeInTheDocument());
    expect(screen.queryByText(raw!)).not.toBeInTheDocument();
  });

  it('revokes a key and disables further rotate/revoke actions', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    fireEvent.click(screen.getByRole('button', { name: /close create access/i }));
    fireEvent.click(screen.getByRole('tab', { name: /^model api keys$/i }));
    const revoke = screen.getAllByRole('button', { name: 'Revoke' })[0];
    expect(revoke).toBeDefined();
    const row = revoke!.closest('tr');
    expect(row).not.toBeNull();
    fireEvent.click(revoke!);

    await waitFor(() => expect(row).toHaveTextContent(/disabled/i));
    expect(within(row!).getByRole('button', { name: 'Rotate' })).toBeDisabled();
    expect(within(row!).getByRole('button', { name: 'Revoke' })).toBeDisabled();
    expect(within(row!).queryByText(/^llmc_.*copy_once$/)).not.toBeInTheDocument();
  });

  it('makes an explicit deny visible in the effective policy preview before save', async () => {
    renderWithQuery(<AccessView />);
    await screen.findByTestId('access-view');

    fireEvent.click(screen.getByRole('button', { name: /close create access/i }));
    fireEvent.click(screen.getByRole('tab', { name: /model policies/i }));
    fireEvent.change(screen.getByLabelText('Policy effect'), { target: { value: 'deny' } });
    fireEvent.click(screen.getByRole('checkbox', { name: /model list/i }));

    expect(screen.getAllByText('deny').length).toBeGreaterThan(0);
    expect(screen.getByText(/Capabilities: .*Model List/i)).toBeInTheDocument();
    expect(screen.getByText(/Providers: All providers/i)).toBeInTheDocument();
  });
});
