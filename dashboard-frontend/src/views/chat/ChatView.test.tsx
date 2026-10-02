import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, fireEvent, screen, waitFor } from '@testing-library/react';
import { getConnection, queryKeys } from '../../api/connection';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { authStore } from '../../store/authStore';
import { ChatView } from './ChatView';

beforeEach(() => resetWorld({ mock: true }));
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('ChatView', () => {
  it('selects a catalog model and renders the streamed response with terminal diagnostics', async () => {
    const streamChat = vi.spyOn(getConnection().client, 'streamChat');
    authStore.getState().setMutationsEnabled(false);
    renderWithQuery(<ChatView />);

    await waitFor(() => expect(screen.getByLabelText('Model')).toHaveValue('gpt-4o'));
    expect(screen.getByLabelText('Model').tagName).toBe('SELECT');
    expect(screen.getByLabelText('Thinking level')).toHaveValue('medium');
    expect(screen.getByLabelText('Temperature')).toHaveValue('1');
    expect(screen.getByLabelText('Top P')).toHaveValue('0.95');
    expect(screen.getByLabelText('Max context length')).toHaveValue('128000');
    expect(screen.getByRole('heading', { name: 'Chat' })).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('Message'), { target: { value: 'ping' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() => expect(screen.getByTestId('chat-message-assistant')).toHaveTextContent('Mock response from gpt-4o: ping'));
    expect(screen.getByTestId('chat-thinking')).toHaveTextContent('Checking the request.');
    expect(screen.getByTestId('chat-thinking').querySelector('strong')).toHaveTextContent('Checking');
    expect(screen.getByTestId('chat-run-status')).toHaveTextContent('finish: stop');
    expect(screen.getByTestId('chat-run-status')).toHaveTextContent('20 tokens');
    expect(screen.getByTestId('chat-run-status')).toHaveTextContent('tg/s');
    expect(screen.getByTestId('chat-run-status')).toHaveTextContent('pp/s');
    expect(streamChat).toHaveBeenCalledWith(expect.objectContaining({ model: 'gpt-4o', max_tokens: 128000 }), expect.any(Function), expect.any(AbortSignal));
  });

  it.each([
    { limit: 32768, expected: '32768' },
    { limit: 262144, expected: '262144' },
    { limit: 1048576, expected: '262144' },
    { limit: null, expected: '262144' },
    { limit: undefined, expected: '262144' },
    { limit: 0, expected: '262144' },
  ])('defaults max context to $expected for advertised limit $limit', async ({ limit, expected }) => {
    vi.spyOn(getConnection().client, 'catalog').mockResolvedValue([{ id: 'test-model', context_limit: limit }]);
    renderWithQuery(<ChatView />);

    await waitFor(() => expect(screen.getByLabelText('Model')).toHaveValue('test-model'));
    expect(screen.getByLabelText('Max context length')).toHaveValue(expected);
  });

  it('uses each selected model default and preserves manual edits until the model changes', async () => {
    vi.spyOn(getConnection().client, 'catalog').mockResolvedValue([
      { id: 'large-model', context_limit: 1048576 },
      { id: 'small-model', context_limit: 32768 },
    ]);
    renderWithQuery(<ChatView />);

    await waitFor(() => expect(screen.getByLabelText('Model')).toHaveValue('large-model'));
    expect(screen.getByLabelText('Max context length')).toHaveValue('262144');
    fireEvent.change(screen.getByLabelText('Max context length'), { target: { value: '8192' } });
    fireEvent.change(screen.getByLabelText('Temperature'), { target: { value: '0.7' } });
    expect(screen.getByLabelText('Max context length')).toHaveValue('8192');

    fireEvent.change(screen.getByLabelText('Model'), { target: { value: 'small-model' } });
    expect(screen.getByLabelText('Max context length')).toHaveValue('32768');
    expect(screen.getByTestId('chat-settings')).toHaveTextContent(`model window ${(32768).toLocaleString()}`);
    fireEvent.change(screen.getByLabelText('Model'), { target: { value: 'large-model' } });
    expect(screen.getByLabelText('Max context length')).toHaveValue('262144');
  });

  it('updates an automatic default when the catalog limit arrives without overwriting manual edits', async () => {
    vi.spyOn(getConnection().client, 'catalog').mockResolvedValue([{ id: 'test-model', context_limit: null }]);
    const { queryClient } = renderWithQuery(<ChatView />);
    await waitFor(() => expect(screen.getByLabelText('Model')).toHaveValue('test-model'));
    expect(screen.getByLabelText('Max context length')).toHaveValue('262144');

    await act(async () => { queryClient.setQueryData(queryKeys.catalog, [{ id: 'test-model', context_limit: 65536 }]); });
    await waitFor(() => expect(screen.getByLabelText('Max context length')).toHaveValue('65536'));
    fireEvent.change(screen.getByLabelText('Max context length'), { target: { value: '8192' } });
    await act(async () => { queryClient.setQueryData(queryKeys.catalog, [{ id: 'test-model', context_limit: 32768 }]); });
    await waitFor(() => expect(screen.getByTestId('chat-settings')).toHaveTextContent(`model window ${(32768).toLocaleString()}`));
    expect(screen.getByLabelText('Max context length')).toHaveValue('8192');
  });

  it('clears the local transcript without persisting it', async () => {
    renderWithQuery(<ChatView />);
    await waitFor(() => expect(screen.getByLabelText('Model')).toHaveValue('gpt-4o'));
    fireEvent.change(screen.getByLabelText('Message'), { target: { value: 'hello' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));
    await screen.findByTestId('chat-message-assistant');

    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(screen.queryByTestId('chat-message-user')).toBeNull();
    expect(screen.getByTestId('chat-empty')).toBeInTheDocument();
  });

  it('keeps one prompt cache key across turns and resets it when the chat is cleared', async () => {
    vi.spyOn(getConnection().client, 'catalog').mockResolvedValue([{ id: 'gpt-4o', context_limit: 128000 }]);
    const sent: string[] = [];
    vi.spyOn(getConnection().client, 'streamChat').mockImplementation(async (request, onDelta) => {
      sent.push(request.prompt_cache_key ?? '');
      onDelta({ kind: 'content', text: 'ok' });
      return {
        model: request.model,
        requestedModel: request.model,
        finishReason: 'stop',
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      };
    });
    renderWithQuery(<ChatView />);

    await waitFor(() => expect(screen.getByLabelText('Model')).toHaveValue('gpt-4o'));
    fireEvent.change(screen.getByLabelText('Message'), { target: { value: 'one' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));
    await waitFor(() => expect(sent).toHaveLength(1));
    fireEvent.change(screen.getByLabelText('Message'), { target: { value: 'two' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));
    await waitFor(() => expect(sent).toHaveLength(2));

    expect(sent[0]).toBeTruthy();
    expect(sent[1]).toBe(sent[0]);

    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    fireEvent.change(screen.getByLabelText('Message'), { target: { value: 'three' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));
    await waitFor(() => expect(sent).toHaveLength(3));
    expect(sent[2]).toBeTruthy();
    expect(sent[2]).not.toBe(sent[0]);
  });
});
