import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { cleanup, fireEvent, waitFor, within } from '@testing-library/react';
import { ThroughputView } from './ThroughputView';
import { renderWithQuery, resetWorld } from '../../components/testHarness';

describe('ThroughputView (mock history API)', () => {
  beforeEach(() => resetWorld({ mock: true }));
  afterEach(cleanup);

  it('renders derived gateway figures per model and combined, plus the scraped engine tile', async () => {
    const { getByTestId, getAllByTestId } = renderWithQuery(<ThroughputView />);
    await waitFor(() => expect(getByTestId('tp-requests').getAttribute('data-quality')).toBe('measured'));
    expect(getByTestId('tp-prefill').getAttribute('data-quality')).toBe('derived');
    expect(getByTestId('tp-decode').getAttribute('data-quality')).toBe('derived');
    expect(getByTestId('tp-prefill').textContent).not.toContain('—');
    // Charts carry one series per model when "all" is selected.
    const requests = getByTestId('chart-requests');
    expect(requests.querySelectorAll('path[data-series]').length).toBe(2);
    expect(within(requests).getByRole('img').getAttribute('data-available')).toBe('true');
    // Selecting one model narrows the charts to that series.
    fireEvent.click(within(getByTestId('throughput-models')).getByText('gpt-4o'));
    await waitFor(() => expect(getByTestId('chart-requests').querySelectorAll('path[data-series]').length).toBe(1));
    // Engine tile from the scraped vLLM samples, with rates from consecutive scrapes.
    await waitFor(() => expect(getAllByTestId('engine-tile').length).toBe(1));
    const tile = getAllByTestId('engine-tile')[0]!;
    expect(tile.textContent).toContain('vllm-a');
    expect(within(tile).getByTestId('engine-kv').textContent).toContain('62%');
    expect(within(tile).getByTestId('engine-hit').textContent).toContain('75%');
    expect(within(tile).getByTestId('engine-prompt').getAttribute('data-quality')).toBe('derived');
    expect(within(tile).getByTestId('engine-prompt').textContent).toContain('2.0k');
  });
});
