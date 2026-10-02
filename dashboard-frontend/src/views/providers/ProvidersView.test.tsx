import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { getConnection, queryKeys } from '../../api/connection';
import type { ConfiguredProvider, ConfiguredProvidersResponse, FleetModelsResponse, ProviderInventoryEntry } from '../../api/types';
import { renderWithQuery, resetWorld } from '../../components/testHarness';
import { authStore } from '../../store/authStore';
import { ProvidersView } from './ProvidersView';

beforeEach(() => resetWorld({ mock: true }));
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

const slot = (providerId: string, name: string, resourceId: string, healthy = true): ProviderInventoryEntry => ({
  provider_id: providerId,
  provider_name: name,
  resource_id: resourceId,
  route: null,
  base_url: 'https://inference.example/v1',
  models: [{ id: `${resourceId}-model`, context_limit: 32768 }],
  availability: null,
  capacity_limit: 4,
  active_requests: 0,
  accepting_requests: healthy,
  healthy,
});

function mockProviderSlots() {
  vi.spyOn(getConnection().client, 'providers').mockResolvedValue({ providers: [
    slot('mesh:south', 'South lab', 'gpu-c'),
    slot('mesh:north', 'North lab', 'gpu-a'),
    slot('mesh:north', 'North lab', 'gpu-b', false),
  ] });
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void; reject: (reason?: unknown) => void } {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const fleetStatusCases: FleetModelsResponse = {
  models: [
    {
      model: { id: 'fleet-ready', image: 'ready:image' },
      status: { model_id: 'fleet-ready', phase: 'ready', desired_state: 'ready', assigned_gpus: [0] },
    },
    {
      model: { id: 'fleet-loading', image: 'loading:image' },
      status: { model_id: 'fleet-loading', phase: 'loading', desired_state: 'ready', assigned_gpus: [1], container_status: 'running' },
    },
    {
      model: { id: 'fleet-stopping', image: 'stopping:image' },
      status: { model_id: 'fleet-stopping', phase: 'stopping', desired_state: 'unloaded', assigned_gpus: [2], container_status: 'running' },
    },
    {
      model: { id: 'fleet-failed', image: 'failed:image' },
      status: { model_id: 'fleet-failed', phase: 'failed', desired_state: 'ready', assigned_gpus: [], last_error: 'boot failed' },
    },
    {
      model: { id: 'fleet-unhealthy', image: 'unhealthy:image' },
      status: { model_id: 'fleet-unhealthy', phase: 'unhealthy', desired_state: 'ready', assigned_gpus: [], last_error: 'readiness failed' },
    },
    {
      model: { id: 'fleet-unloaded', image: 'unloaded:image' },
      status: { model_id: 'fleet-unloaded', phase: 'unloaded', desired_state: 'unloaded', assigned_gpus: [] },
    },
  ],
};

describe('ProvidersView', () => {
  it('defaults slots to healthy while allowing all statuses to be shown', async () => {
    mockProviderSlots();
    renderWithQuery(<ProvidersView />);

    const status = await screen.findByRole('group', { name: 'Provider status' });
    expect(within(status).getByRole('button', { name: 'healthy' })).toHaveAttribute('aria-pressed', 'true');
    expect(within(status).getByRole('button', { name: 'all' })).toHaveAttribute('aria-pressed', 'false');
    expect(screen.getAllByTestId('provider-row').map((row) => row.dataset.resource)).toEqual(['gpu-a', 'gpu-c']);
    expect(screen.getByTestId('providers-table')).not.toHaveTextContent('gpu-b-model');

    fireEvent.click(within(status).getByRole('button', { name: 'all' }));
    expect(screen.getAllByTestId('provider-row')).toHaveLength(3);
    expect(screen.getByTestId('providers-table')).toHaveTextContent('gpu-b-model');
  });

  it('shows an empty healthy view without hiding the option to inspect down slots', async () => {
    vi.spyOn(getConnection().client, 'providers').mockResolvedValue({ providers: [
      slot('mesh:north', 'North lab', 'gpu-down', false),
    ] });
    renderWithQuery(<ProvidersView />);

    await screen.findByTestId('providers-empty');
    expect(screen.queryByTestId('provider-row')).not.toBeInTheDocument();
    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'down' }));
    expect(screen.getByTestId('provider-row')).toHaveTextContent('gpu-down-model');
  });

  it('pages slots in stable provider/slot order and resets when filters or page size change', async () => {
    const providers = Array.from({ length: 12 }, (_, i) => slot(
      i < 8 ? 'mesh:north' : 'mesh:south', i < 8 ? 'North lab' : 'South lab',
      `gpu-${String(i).padStart(2, '0')}`, i !== 0,
    )).reverse();
    vi.spyOn(getConnection().client, 'providers').mockResolvedValue({ providers });
    renderWithQuery(<ProvidersView />);
    fireEvent.click(within(await screen.findByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'all' }));
    await waitFor(() => expect(screen.getAllByTestId('provider-row')).toHaveLength(5));
    const table = screen.getByTestId('providers-table');
    expect(table).toHaveTextContent('Showing 1–5 of 12 matching slots');
    expect(screen.getByRole('button', { name: 'Previous page' })).toBeDisabled();
    expect(screen.getAllByTestId('provider-row').map((row) => row.dataset.resource)).toEqual(['gpu-00', 'gpu-01', 'gpu-02', 'gpu-03', 'gpu-04']);
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    expect(screen.getAllByTestId('provider-group')).toHaveLength(2);
    expect(table).toHaveTextContent('Showing 6–10 of 12 matching slots');
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    expect(screen.getAllByTestId('provider-row')).toHaveLength(2);
    expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled();

    fireEvent.change(screen.getByRole('combobox', { name: 'Filter by provider' }), { target: { value: 'North lab' } });
    expect(table).toHaveTextContent('Showing 1–5 of 8 matching slots');
    expect(screen.getAllByTestId('provider-group')).toHaveLength(1);
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    fireEvent.change(screen.getByRole('combobox', { name: 'Slots per page' }), { target: { value: '10' } });
    expect(screen.getAllByTestId('provider-row')).toHaveLength(8);
    expect(table).toHaveTextContent('Page 1 of 1');

    fireEvent.change(screen.getByRole('textbox', { name: 'Search providers' }), { target: { value: ' GPU-00-MODEL ' } });
    expect(screen.getAllByTestId('provider-row')).toHaveLength(1);
    fireEvent.change(screen.getByRole('combobox', { name: 'Filter by availability' }), { target: { value: 'accepting' } });
    expect(screen.getByTestId('providers-empty')).toBeVisible();
    expect(table).toHaveTextContent('Showing 0 of 0 matching slots');
    fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }));
    expect(screen.getAllByTestId('provider-row')).toHaveLength(10);
  });

  it('clamps pagination when live slots disappear without restoring a stale page later', async () => {
    const providers = Array.from({ length: 11 }, (_, i) => slot('mesh:north', 'North lab', `gpu-${i}`));
    vi.spyOn(getConnection().client, 'providers').mockResolvedValue({ providers });
    const { queryClient } = renderWithQuery(<ProvidersView />);
    await waitFor(() => expect(screen.getAllByTestId('provider-row')).toHaveLength(5));
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    await act(async () => { queryClient.setQueryData(queryKeys.providers, { providers: providers.slice(0, 2) }); });
    await waitFor(() => expect(screen.getAllByTestId('provider-row')).toHaveLength(2));
    expect(screen.getByTestId('providers-table')).toHaveTextContent('Page 1 of 1');
    await act(async () => { queryClient.setQueryData(queryKeys.providers, { providers }); });
    await waitFor(() => expect(screen.getByTestId('providers-table')).toHaveTextContent('Page 1 of 3'));
  });

  it('groups resource slots under provider-name headings without losing slot details', async () => {
    mockProviderSlots();
    renderWithQuery(<ProvidersView />);

    await waitFor(() => expect(screen.getAllByTestId('provider-group')).toHaveLength(2));
    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'all' }));
    const [north, south] = screen.getAllByTestId('provider-group');
    expect(within(north!).getByRole('heading', { name: 'North lab' })).toBeVisible();
    expect(within(north!).getAllByTestId('provider-row')).toHaveLength(2);
    expect(north).toHaveTextContent('2 / 2 slots');
    expect(north).toHaveTextContent('gpu-a-model');
    expect(north).toHaveTextContent('gpu-b-model');
    expect(north).not.toHaveTextContent('gpu-c-model');
    expect(within(south!).getByRole('heading', { name: 'South lab' })).toBeVisible();
    expect(within(south!).getAllByTestId('provider-row')).toHaveLength(1);
    expect(south).toHaveTextContent('1 / 1 slot');
  });

  it('filters slots within provider groups and hides groups without matches', async () => {
    mockProviderSlots();
    renderWithQuery(<ProvidersView />);
    await waitFor(() => expect(screen.getAllByTestId('provider-group')).toHaveLength(2));

    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'down' }));
    expect(screen.getAllByTestId('provider-group')).toHaveLength(1);
    const north = screen.getByTestId('provider-group');
    expect(north).toHaveTextContent('North lab');
    expect(north).toHaveTextContent('1 / 2 slots');
    expect(north).toHaveTextContent('gpu-b-model');
    expect(north).not.toHaveTextContent('gpu-a-model');

    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'all' }));
    fireEvent.change(screen.getByRole('textbox', { name: 'Search providers' }), { target: { value: 'South lab' } });
    expect(screen.getAllByTestId('provider-group')).toHaveLength(1);
    expect(screen.getByTestId('provider-group')).toHaveTextContent('gpu-c-model');

    fireEvent.change(screen.getByRole('textbox', { name: 'Search providers' }), { target: { value: 'missing-slot' } });
    expect(screen.queryByTestId('provider-group')).not.toBeInTheDocument();
    expect(screen.getByTestId('providers-empty')).toBeVisible();
  });

  it('renders exact advertised models, operational capacity, access limits, and cache metrics', async () => {
    renderWithQuery(<ProvidersView />);

    await screen.findByTestId('providers-view');
    await waitFor(() => expect(screen.getAllByTestId('provider-row').length).toBeGreaterThan(0));
    fireEvent.click(within(screen.getByRole('group', { name: 'Provider status' })).getByRole('button', { name: 'all' }));

    const table = screen.getByTestId('providers-table');
    expect(table).toHaveTextContent('vllm-a');
    expect(table).toHaveTextContent('llama-3.1-70b');
    expect(table).toHaveTextContent('2/8 active');
    expect(table).toHaveTextContent('America/Chicago');
    expect(table).toHaveTextContent('4 concurrent sessions');
    expect(table).toHaveTextContent('hit%');
    expect(table).toHaveTextContent('kv');
    expect(screen.getByTestId('providers-catalog')).toHaveTextContent('Exact provider-scoped model advertisements');
  });

  it('renders Fleet state and sends CSRF-gated load/unload actions', async () => {
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('fleet-panel');
    await waitFor(() => expect(within(panel).getAllByTestId('fleet-model-card').length).toBeGreaterThan(0));
    expect(panel).toHaveTextContent('qwen3-8b-flash');
    expect(panel).toHaveTextContent('GPU 0');

    const idleCard = within(panel).getByText('qwen3-32b').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    fireEvent.click(within(idleCard).getByRole('button', { name: 'Load' }));
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Confirm load' }));

    await waitFor(() => expect(idleCard).toHaveTextContent('loading'));
  });

  it('shows Fleet model status lights for loaded, loading, failed, and unloaded states', async () => {
    vi.spyOn(getConnection().client, 'fleet').mockResolvedValue(fleetStatusCases);
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('fleet-panel');
    await waitFor(() => expect(within(panel).getAllByTestId('fleet-model-card')).toHaveLength(6));

    const ready = within(panel).getByText('fleet-ready').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const loading = within(panel).getByText('fleet-loading').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const stopping = within(panel).getByText('fleet-stopping').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const failed = within(panel).getByText('fleet-failed').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const unhealthy = within(panel).getByText('fleet-unhealthy').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const unloaded = within(panel).getByText('fleet-unloaded').closest('[data-testid="fleet-model-card"]') as HTMLElement;

    expect(within(ready).getByRole('status', { name: 'model status: loaded' })).toHaveAttribute('data-status', 'loaded');
    expect(within(loading).getByRole('status', { name: 'model status: loading (ready)' })).toHaveAttribute('data-status', 'loading');
    expect(within(stopping).getByRole('status', { name: 'model status: stopping (unloaded)' })).toHaveAttribute('data-status', 'loading');
    expect(within(failed).getByRole('status', { name: 'model status: failed (failed)' })).toHaveAttribute('data-status', 'failed');
    expect(within(unhealthy).getByRole('status', { name: 'model status: failed (unhealthy)' })).toHaveAttribute('data-status', 'failed');
    expect(within(unloaded).getByRole('status', { name: 'model status: unloaded' })).toHaveAttribute('data-status', 'unloaded');
    expect(within(loading).getByRole('button', { name: 'Unload' })).toBeDisabled();
    expect(within(stopping).getByRole('button', { name: 'Unload' })).toBeDisabled();
    expect(within(failed).getByRole('button', { name: 'Unload' })).toBeEnabled();
    expect(within(unhealthy).getByRole('button', { name: 'Unload' })).toBeEnabled();
  });

  it('polls local Fleet state into terminal ready and failed indicators without manual invalidation', async () => {
    vi.useFakeTimers();
    const terminalFleetState: FleetModelsResponse = {
      models: [
        {
          model: { id: 'fleet-poll-ready', image: 'ready:image' },
          status: { model_id: 'fleet-poll-ready', phase: 'ready', desired_state: 'ready', assigned_gpus: [0] },
        },
        {
          model: { id: 'fleet-poll-failed', image: 'failed:image' },
          status: { model_id: 'fleet-poll-failed', phase: 'failed', desired_state: 'ready', assigned_gpus: [1], last_error: 'boot failed' },
        },
      ],
    };
    const fleet = vi.spyOn(getConnection().client, 'fleet')
      .mockResolvedValueOnce({
        models: [
          {
            model: { id: 'fleet-poll-ready', image: 'ready:image' },
            status: { model_id: 'fleet-poll-ready', phase: 'loading', desired_state: 'ready', assigned_gpus: [0] },
          },
          {
            model: { id: 'fleet-poll-failed', image: 'failed:image' },
            status: { model_id: 'fleet-poll-failed', phase: 'loading', desired_state: 'ready', assigned_gpus: [1] },
          },
        ],
      })
      .mockResolvedValue(terminalFleetState);
    renderWithQuery(<ProvidersView />);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    const panel = screen.getByTestId('fleet-panel');
    const ready = within(panel).getByText('fleet-poll-ready');
    const failed = within(panel).getByText('fleet-poll-failed');
    const readyCard = ready.closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const failedCard = failed.closest('[data-testid="fleet-model-card"]') as HTMLElement;
    expect(within(readyCard).getByRole('status', { name: 'model status: loading (ready)' })).toHaveAttribute('data-status', 'loading');
    expect(within(failedCard).getByRole('status', { name: 'model status: loading (ready)' })).toHaveAttribute('data-status', 'loading');

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    await vi.waitFor(() => {
      expect(fleet).toHaveBeenCalledTimes(2);
      const updatedReadyCard = within(panel).getByText('fleet-poll-ready').closest('[data-testid="fleet-model-card"]') as HTMLElement;
      const updatedFailedCard = within(panel).getByText('fleet-poll-failed').closest('[data-testid="fleet-model-card"]') as HTMLElement;
      expect(within(updatedReadyCard).getByRole('status', { name: 'model status: loaded' })).toHaveAttribute('data-status', 'loaded');
      expect(within(updatedFailedCard).getByRole('status', { name: 'model status: failed (failed)' })).toHaveAttribute('data-status', 'failed');
    });
  });

  it('discovers and removes additional OpenAI-compatible providers', async () => {
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('configured-providers-panel');
    expect(panel).toHaveTextContent('Local lab');
    expect(panel).toHaveTextContent('auto-discover');
    expect(panel).toHaveTextContent('Refreshes about every 5 min');
    expect(panel).toHaveTextContent('Newly discovered models are enabled unless individually disabled');
    expect(panel).not.toHaveTextContent('secret-value');

    fireEvent.change(within(panel).getByLabelText('Provider name'), { target: { value: 'Remote lab' } });
    fireEvent.change(within(panel).getByLabelText('Provider URL'), { target: { value: 'https://inference.example/v1' } });
    fireEvent.change(within(panel).getByLabelText('Provider API key'), { target: { value: 'secret-value' } });
    fireEvent.click(within(panel).getByRole('button', { name: 'Discover & add' }));

    await waitFor(() => expect(panel).toHaveTextContent('Remote lab'));
    expect(panel).not.toHaveTextContent('secret-value');
  });

  it('toggles configured-provider autodiscovery and individual model availability', async () => {
    const client = getConnection().client;
    const updateProvider = vi.spyOn(client, 'updateConfiguredProvider');
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('configured-providers-panel');
    const card = within(panel).getByText('Local lab').closest('[data-testid="configured-provider-card"]') as HTMLElement;

    fireEvent.click(within(card).getByRole('checkbox', { name: 'auto-discover' }));
    await waitFor(() => expect(updateProvider).toHaveBeenCalledWith('managed-local-lab', { auto_discover: false }));
    await waitFor(() => expect(card).toHaveTextContent('Automatic catalog refresh is paused'));

    fireEvent.change(within(card).getByLabelText('Search models for Local lab'), { target: { value: '32b' } });
    expect(within(card).getByText('qwen3-32b')).toBeVisible();
    expect(within(card).queryByText('qwen3-8b-flash')).toBeNull();

    fireEvent.click(within(card).getByRole('checkbox', { name: 'Enable qwen3-32b' }));
    await waitFor(() => expect(updateProvider).toHaveBeenCalledWith('managed-local-lab', { disabled_models: ['qwen3-32b'] }));
    await waitFor(() => expect(within(card).getByRole('checkbox', { name: 'Enable qwen3-32b' })).not.toBeChecked());
    expect(card).toHaveTextContent('1 enabled');
  });

  it('switches configured providers into strict selected-model mode and back to unrestricted', async () => {
    const client = getConnection().client;
    const updateProvider = vi.spyOn(client, 'updateConfiguredProvider');
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('configured-providers-panel');
    const card = within(panel).getByText('Local lab').closest('[data-testid="configured-provider-card"]') as HTMLElement;

    fireEvent.click(within(card).getByRole('checkbox', { name: 'Only allow selected models' }));
    await waitFor(() => expect(updateProvider).toHaveBeenCalledWith('managed-local-lab', { allowed_models: ['qwen3-32b', 'qwen3-8b-flash'] }));
    await waitFor(() => expect(card).toHaveTextContent('Newly discovered models stay visible but disabled until selected'));

    fireEvent.click(within(card).getByRole('checkbox', { name: 'Only allow selected models' }));
    await waitFor(() => expect(updateProvider).toHaveBeenLastCalledWith('managed-local-lab', { allowed_models: null }));
  });

  it('keeps newly discovered models disabled in strict provider allowlist mode', async () => {
    const client = getConnection().client;
    vi.spyOn(client, 'configuredProviders').mockResolvedValue({
      providers: [{
        id: 'managed-strict',
        name: 'Strict lab',
        base_url: 'https://strict.example/v1',
        api_key_present: true,
        auto_discover: true,
        allowed_models: ['model-a'],
        disabled_models: [],
        models: [{ id: 'model-a', context_limit: 8192 }, { id: 'model-b', context_limit: 8192 }],
      }],
    });
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('configured-providers-panel');
    const card = within(panel).getByText('Strict lab').closest('[data-testid="configured-provider-card"]') as HTMLElement;

    expect(within(card).getByRole('checkbox', { name: 'Only allow selected models' })).toBeChecked();
    expect(within(card).getByRole('checkbox', { name: 'Enable model-a' })).toBeChecked();
    expect(within(card).getByRole('checkbox', { name: 'Enable model-b' })).not.toBeChecked();
    expect(card).toHaveTextContent('Newly discovered models stay visible but disabled until selected');
  });

  it('handles legacy configured providers that omit allowed_models', async () => {
    const client = getConnection().client;
    vi.spyOn(client, 'configuredProviders').mockResolvedValue({
      providers: [{
        id: 'managed-legacy',
        name: 'Legacy lab',
        base_url: 'https://legacy.example/v1',
        api_key_present: true,
        auto_discover: true,
        disabled_models: [],
        models: [{ id: 'model-a', context_limit: 8192 }],
      }],
    });
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('configured-providers-panel');
    const card = within(panel).getByText('Legacy lab').closest('[data-testid="configured-provider-card"]') as HTMLElement;

    expect(within(card).getByRole('checkbox', { name: 'Only allow selected models' })).not.toBeChecked();
    expect(within(card).getByRole('checkbox', { name: 'Enable model-a' })).toBeChecked();
  });

  it('selecting a blacklisted model in strict mode removes it from disabled models', async () => {
    const client = getConnection().client;
    vi.spyOn(client, 'configuredProviders').mockResolvedValue({
      providers: [{
        id: 'managed-strict-blacklist',
        name: 'Strict blacklist lab',
        base_url: 'https://strict-blacklist.example/v1',
        api_key_present: true,
        auto_discover: true,
        allowed_models: ['model-a'],
        disabled_models: ['model-b'],
        models: [{ id: 'model-a', context_limit: 8192 }, { id: 'model-b', context_limit: 8192 }],
      }],
    });
    const updateProvider = vi.spyOn(client, 'updateConfiguredProvider');
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('configured-providers-panel');
    const card = within(panel).getByText('Strict blacklist lab').closest('[data-testid="configured-provider-card"]') as HTMLElement;

    fireEvent.click(within(card).getByRole('checkbox', { name: 'Enable model-b' }));
    await waitFor(() => expect(updateProvider).toHaveBeenCalledWith('managed-strict-blacklist', {
      allowed_models: ['model-a', 'model-b'],
      disabled_models: [],
    }));
  });

  it('keeps configured-provider controls busy and uses returned state before delayed refetch completes', async () => {
    const client = getConnection().client;
    const provider: ConfiguredProvider = {
      id: 'managed-race',
      name: 'Race lab',
      base_url: 'https://race.example/v1',
      api_key_present: true,
      auto_discover: true,
      allowed_models: null,
      disabled_models: [],
      models: [{ id: 'model-a', context_limit: 8192 }, { id: 'model-b', context_limit: 8192 }],
    };
    let serverProvider = provider;
    const refetchGate = deferred<ConfiguredProvidersResponse>();
    const configuredProviders = vi.spyOn(client, 'configuredProviders')
      .mockResolvedValueOnce({ providers: [serverProvider] })
      .mockImplementation(() => refetchGate.promise);
    const updateProvider = vi.spyOn(client, 'updateConfiguredProvider').mockImplementation(async (_id, body) => {
      serverProvider = { ...serverProvider, ...body };
      return serverProvider;
    });
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('configured-providers-panel');
    const card = within(panel).getByText('Race lab').closest('[data-testid="configured-provider-card"]') as HTMLElement;

    fireEvent.click(within(card).getByRole('checkbox', { name: 'Enable model-a' }));
    await waitFor(() => expect(updateProvider).toHaveBeenCalledWith('managed-race', { disabled_models: ['model-a'] }));
    await waitFor(() => expect(within(card).getByRole('checkbox', { name: 'Enable model-a' })).toBeDisabled());
    expect(configuredProviders).toHaveBeenCalledTimes(2);

    refetchGate.resolve({ providers: [serverProvider] });
    await waitFor(() => expect(within(card).getByRole('checkbox', { name: 'Enable model-a' })).toBeEnabled());

    fireEvent.click(within(card).getByRole('checkbox', { name: 'Enable model-a' }));
    await waitFor(() => expect(updateProvider).toHaveBeenLastCalledWith('managed-race', { disabled_models: [] }));
  });

  it('renders configured-provider controls read-only when mutations are disabled', async () => {
    renderWithQuery(<ProvidersView />);
    act(() => authStore.getState().setMutationsEnabled(false));

    const panel = await screen.findByTestId('configured-providers-panel');
    const card = within(panel).getByText('Local lab').closest('[data-testid="configured-provider-card"]') as HTMLElement;

    expect(within(panel).getByText('mutations disabled')).toBeVisible();
    expect(within(card).getByRole('checkbox', { name: 'auto-discover' })).toBeDisabled();
    expect(within(card).getByRole('checkbox', { name: 'Only allow selected models' })).toBeDisabled();
    expect(within(card).getByRole('button', { name: 'Remove' })).toBeDisabled();
    expect(within(card).getByRole('checkbox', { name: 'Enable qwen3-8b-flash' })).toBeDisabled();
  });

  it('confirms and invokes model loading advertised by a downstream mesh worker', async () => {
    const client = getConnection().client;
    const mesh = await client.mesh();
    const endpoint = mesh.nodes.find((node) => node.model_switching)!.endpoint_id;
    vi.spyOn(client, 'providers').mockResolvedValue({ providers: [slot(`mesh:${endpoint}`, 'North lab', 'gpu-a')] });
    const loadModel = vi.spyOn(client, 'loadMeshModel');
    const unloadModel = vi.spyOn(client, 'unloadMeshModel');
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('mesh-admin');
    await waitFor(() => expect(panel).toHaveTextContent('remote model switching'));
    await waitFor(() => expect(within(screen.getByTestId('remote-model-switcher')).getByText('North lab')).toBeVisible());
    const model = within(panel).getByText('qwen3-32b').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    fireEvent.click(within(model).getByRole('button', { name: 'Load' }));
    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveTextContent('Every currently loaded model');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Confirm load' }));

    await waitFor(() => expect(model).toHaveTextContent('loading'));
    expect(unloadModel).toHaveBeenCalledWith(endpoint, 'qwen3-8b-flash');
    expect(loadModel).toHaveBeenCalledWith(endpoint, 'qwen3-32b', 1);
  });

  it('lets a loaded local Fleet profile scale by sending the selected desired count', async () => {
    const client = getConnection().client;
    const loadModel = vi.spyOn(client, 'loadFleetModel');
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('fleet-panel');
    const model = within(panel).getByText('qwen3-8b-flash').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    expect(within(model).getByRole('button', { name: 'Scale' })).toBeDisabled();

    fireEvent.change(within(model).getByLabelText('Instances'), { target: { value: '2' } });
    fireEvent.click(within(model).getByRole('button', { name: 'Scale' }));
    expect(screen.getByRole('dialog')).toHaveTextContent('desired Fleet instance count to 2');
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Confirm load' }));

    await waitFor(() => expect(loadModel).toHaveBeenCalledWith('qwen3-8b-flash', 2));
  });

  it('blocks invalid local instance counts before mutation', async () => {
    const client = getConnection().client;
    const loadModel = vi.spyOn(client, 'loadFleetModel');
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('fleet-panel');
    const model = within(panel).getByText('qwen3-32b').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    fireEvent.change(within(model).getByLabelText('Instances'), { target: { value: '0' } });

    expect(within(model).getByText('min 1')).toBeVisible();
    expect(within(model).getByRole('button', { name: 'Load' })).toBeDisabled();
    expect(loadModel).not.toHaveBeenCalled();
  });

  it('shows concrete Fleet instance ports and GPU assignments when reported', async () => {
    renderWithQuery(<ProvidersView />);

    const panel = await screen.findByTestId('fleet-panel');
    const model = within(panel).getByText('qwen3-8b-flash').closest('[data-testid="fleet-model-card"]') as HTMLElement;
    const instances = within(model).getByTestId('instance-list');
    expect(instances).toHaveTextContent('#0');
    expect(instances).toHaveTextContent(':8101');
    expect(instances).toHaveTextContent('GPU 0');
    expect(model).toHaveTextContent('1/1');
  });

  it('scales a remote TP4 profile to two instances and reports the 2x GPU total', async () => {
    const client = getConnection().client;
    const mesh = await client.mesh();
    const endpoint = mesh.nodes.find((node) => node.model_switching)!.endpoint_id;
    vi.spyOn(client, 'providers').mockResolvedValue({ providers: [slot(`mesh:${endpoint}`, 'North lab', 'gpu-a')] });
    const loadModel = vi.spyOn(client, 'loadMeshModel');
    renderWithQuery(<ProvidersView />);

    const switcher = await screen.findByTestId('remote-model-switcher');
    const model = within(switcher).getByText('qwen3-8b-flash').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    fireEvent.change(within(model).getByLabelText('Instances'), { target: { value: '2' } });
    fireEvent.click(within(model).getByRole('button', { name: 'Scale' }));
    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveTextContent('2 instances needs 8 GPUs total and 4 additional');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Confirm load' }));

    await waitFor(() => expect(loadModel).toHaveBeenCalledWith(endpoint, 'qwen3-8b-flash', 2));
  });

  it('can unload and account for GPUs when a remote profile has one ready replica and one failed replica', async () => {
    const client = getConnection().client;
    const mesh = await client.mesh();
    const endpoint = mesh.nodes.find((node) => node.model_switching)!.endpoint_id;
    vi.spyOn(client, 'mesh').mockResolvedValue({
      ...mesh,
      nodes: mesh.nodes.map((node) => node.endpoint_id === endpoint && node.model_switching ? {
        ...node,
        model_switching: {
          ...node.model_switching,
          models: [
            {
              id: 'qwen3-8b-flash',
              description: 'partial replica profile',
              phase: 'failed',
              desired_state: 'ready',
              gpu_count: 4,
              max_instances: 3,
              desired_instances: 2,
              ready_instances: 1,
              assigned_gpus: [0, 1, 2, 3, 4, 5, 6, 7],
              instances: [
                { instance_id: 'qwen3-8b-flash-0', index: 0, port: 8101, phase: 'ready', container_status: 'running', assigned_gpus: [0, 1, 2, 3] },
                { instance_id: 'qwen3-8b-flash-1', index: 1, port: 8102, phase: 'failed', container_status: 'exited', assigned_gpus: [4, 5, 6, 7], last_error: 'replica failed' },
              ],
              last_error: 'replica failed',
            },
            { id: 'qwen3-32b', description: 'larger local model', phase: 'unloaded', desired_state: 'unloaded', gpu_count: 8, max_instances: 1, desired_instances: 0, ready_instances: 0, assigned_gpus: [], instances: [] },
          ],
        },
      } : node),
    });
    vi.spyOn(client, 'providers').mockResolvedValue({ providers: [slot(`mesh:${endpoint}`, 'North lab', 'gpu-a')] });
    const unloadModel = vi.spyOn(client, 'unloadMeshModel');
    renderWithQuery(<ProvidersView />);

    const switcher = await screen.findByTestId('remote-model-switcher');
    const partial = within(switcher).getByText('qwen3-8b-flash').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    expect(within(partial).getByRole('status', { name: 'model status: failed (failed)' })).toHaveAttribute('data-status', 'failed');
    expect(partial).toHaveTextContent('1/2');
    expect(within(partial).getByRole('button', { name: 'Unload' })).toBeEnabled();
    fireEvent.click(within(partial).getByRole('button', { name: 'Unload' }));
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Confirm unload' }));
    await waitFor(() => expect(unloadModel).toHaveBeenCalledWith(endpoint, 'qwen3-8b-flash'));

    const large = within(switcher).getByText('qwen3-32b').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    fireEvent.click(within(large).getByRole('button', { name: 'Load' }));
    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveTextContent('Every currently loaded model');
    expect(dialog).toHaveTextContent('qwen3-8b-flash');
  });

  it('confirms remote model unloads before sending the mutation', async () => {
    const client = getConnection().client;
    const mesh = await client.mesh();
    const endpoint = mesh.nodes.find((node) => node.model_switching)!.endpoint_id;
    vi.spyOn(client, 'mesh').mockResolvedValue({
      ...mesh,
      nodes: mesh.nodes.map((node) => node.endpoint_id === endpoint && node.model_switching ? {
        ...node,
        model_switching: {
          ...node.model_switching,
          models: node.model_switching.models.map((model) => model.id === 'qwen3-8b-flash'
            ? { ...model, phase: 'ready', desired_state: 'ready', assigned_gpus: [0, 1, 6, 7] }
            : model),
        },
      } : node),
    });
    const unloadModel = vi.spyOn(client, 'unloadMeshModel');
    renderWithQuery(<ProvidersView />);

    const switcher = await screen.findByTestId('remote-model-switcher');
    const model = within(switcher).getByText('qwen3-8b-flash').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    fireEvent.click(within(model).getByRole('button', { name: 'Unload' }));
    expect(unloadModel).not.toHaveBeenCalled();
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Confirm unload' }));

    await waitFor(() => expect(unloadModel).toHaveBeenCalledWith(endpoint, 'qwen3-8b-flash'));
  });

  it('shows remote switchable model status lights from advertised Fleet phases', async () => {
    const client = getConnection().client;
    const mesh = await client.mesh();
    const endpoint = mesh.nodes.find((node) => node.model_switching)!.endpoint_id;
    vi.spyOn(client, 'mesh').mockResolvedValue({
      ...mesh,
      nodes: mesh.nodes.map((node) => node.endpoint_id === endpoint ? {
        ...node,
        model_switching: {
          provider: 'fleet',
          revision: 7,
          models: [
            { id: 'remote-ready', phase: 'ready', desired_state: 'ready' },
            { id: 'remote-loading', phase: 'loading', desired_state: 'ready' },
            { id: 'remote-stopping', phase: 'stopping', desired_state: 'unloaded' },
            { id: 'remote-failed', phase: 'failed', desired_state: 'ready' },
            { id: 'remote-unhealthy', phase: 'unhealthy', desired_state: 'ready' },
            { id: 'remote-unloaded', phase: 'unloaded', desired_state: 'unloaded' },
          ],
        },
      } : node),
    });
    vi.spyOn(client, 'providers').mockResolvedValue({ providers: [slot(`mesh:${endpoint}`, 'North lab', 'gpu-a')] });
    renderWithQuery(<ProvidersView />);

    const switcher = await screen.findByTestId('remote-model-switcher');
    await waitFor(() => expect(within(switcher).getAllByTestId('remote-switch-model')).toHaveLength(6));

    const ready = within(switcher).getByText('remote-ready').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const loading = within(switcher).getByText('remote-loading').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const stopping = within(switcher).getByText('remote-stopping').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const failed = within(switcher).getByText('remote-failed').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const unhealthy = within(switcher).getByText('remote-unhealthy').closest('[data-testid="remote-switch-model"]') as HTMLElement;
    const unloaded = within(switcher).getByText('remote-unloaded').closest('[data-testid="remote-switch-model"]') as HTMLElement;

    expect(within(ready).getByRole('status', { name: 'model status: loaded' })).toHaveAttribute('data-status', 'loaded');
    expect(within(loading).getByRole('status', { name: 'model status: loading (ready)' })).toHaveAttribute('data-status', 'loading');
    expect(within(stopping).getByRole('status', { name: 'model status: stopping (unloaded)' })).toHaveAttribute('data-status', 'loading');
    expect(within(failed).getByRole('status', { name: 'model status: failed (failed)' })).toHaveAttribute('data-status', 'failed');
    expect(within(unhealthy).getByRole('status', { name: 'model status: failed (unhealthy)' })).toHaveAttribute('data-status', 'failed');
    expect(within(unloaded).getByRole('status', { name: 'model status: unloaded' })).toHaveAttribute('data-status', 'unloaded');
  });
});
