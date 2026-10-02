import { useEffect, useMemo, useState, type FormEvent, type ReactNode } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import type { ConfiguredProvider, ConfiguredProvidersResponse, CreateConfiguredProviderRequest, FleetDeploymentStatus, FleetModelEntry, FleetModelsResponse, MeshAdminState, MeshDisabledModel, MeshJoinKey, MeshNode, MeshSwitchableModel, ProviderHealth, UpdateConfiguredProviderRequest } from '../../api/types';
import { EMPTY_FILTERS } from '../../components/FlowTable/filterTypes';
import { useFlowRows } from '../../components/FlowTable/useFlowRows';
import { fmtCost, fmtElapsed, fmtTokens } from '../../components/FlowTable/format';
import { buildProviderLatency, fmtProviderLatencyMs } from '../../components/viz/providerLatency';
import { Panel } from '../../components/ui/Panel';
import { Button } from '../../components/ui/Button';
import { useAuth, useDashboard } from '../../store/hooks';
import { useTopologyQuery } from '../../store/useTopologyQuery';
import { cn } from '../../lib/cn';
import {
  buildProviderInventory,
  costQuality,
  formatPercent,
  groupProviderSlots,
  type ProviderInventoryRow,
  type ProviderInventorySummary,
} from './providersModel';

const authPoliciesKey = ['auth', 'policies', 'providers-view'] as const;
const DASH = '—';
const INPUT = 'rounded-md border border-line bg-bg px-2 py-1.5 font-mono text-xs text-text outline-none focus:border-accent';
const INSTANCE_LIMIT = 64;
const CONFIGURED_PROVIDER_MODEL_LIMIT = 80;

export function ProvidersView() {
  const [createdToken, setCreatedToken] = useState<{ label: string | null; token: string } | null>(null);
  const { client } = getConnection();
  const queryClient = useQueryClient();
  const mutationsEnabled = useAuth((s) => s.mutationsEnabled);
  const nodes = useDashboard((s) => s.topologyNodes);
  const edges = useDashboard((s) => s.topologyEdges);
  const { rows: flows } = useFlowRows(EMPTY_FILTERS);
  const { perProviderById } = useTopologyQuery();

  const topologyQuery = useQuery({ queryKey: queryKeys.topology, queryFn: () => client.topology() });
  const providersQuery = useQuery({ queryKey: queryKeys.providers, queryFn: () => client.providers() });
  const configuredProvidersQuery = useQuery({ queryKey: queryKeys.configuredProviders, queryFn: () => client.configuredProviders(), retry: false });
  const meshQuery = useQuery({ queryKey: queryKeys.mesh, queryFn: () => client.mesh(), retry: false, refetchInterval: 5_000 });
  const fleetQuery = useQuery({ queryKey: queryKeys.fleet, queryFn: () => client.fleet(), retry: false, refetchInterval: 5_000 });
  const providerMetricsQuery = useQuery({ queryKey: queryKeys.providerMetrics, queryFn: () => client.providerMetrics() });
  const policiesQuery = useQuery({ queryKey: authPoliciesKey, queryFn: () => client.authPolicies(), retry: false });
  const invalidateMesh = () => {
    void queryClient.invalidateQueries({ queryKey: queryKeys.mesh });
    void queryClient.invalidateQueries({ queryKey: queryKeys.providers });
  };
  const createJoinKey = useMutation({
    mutationFn: (body: { label?: string; max_uses?: number; expires_in_secs?: number }) => client.createMeshJoinKey(body),
    onSuccess: (created) => {
      setCreatedToken({ label: created.join_key.label ?? null, token: created.token });
      invalidateMesh();
    },
  });
  const revokeJoinKey = useMutation({ mutationFn: (id: string) => client.revokeMeshJoinKey(id), onSuccess: invalidateMesh });
  const setNodeEnabled = useMutation({ mutationFn: ({ endpointId, enabled }: { endpointId: string; enabled: boolean }) => client.setMeshNodeEnabled(endpointId, enabled), onSuccess: invalidateMesh });
  const setModelDisabled = useMutation({
    mutationFn: ({ endpointId, resourceId, model, disabled }: { endpointId: string; resourceId: string; model: string; disabled: boolean }) =>
      client.setMeshModelDisabled({ endpoint_id: endpointId, resource_id: resourceId, model }, disabled),
    onSuccess: invalidateMesh,
  });
  const changeMeshModel = useMutation({
    mutationFn: async ({ endpointId, modelId, unloadIds, unloadOnly, instances }: { endpointId: string; modelId: string; unloadIds: string[]; unloadOnly?: boolean; instances?: number }) => {
      for (const unloadId of unloadIds) {
        await client.unloadMeshModel(endpointId, unloadId);
        if (!unloadOnly || unloadId !== unloadIds.at(-1)) {
          await waitForRemoteModelUnloaded(() => client.mesh(), endpointId, unloadId);
        }
      }
      if (!unloadOnly) return client.loadMeshModel(endpointId, modelId, instances);
      return null;
    },
    onSuccess: invalidateMesh,
  });
  const invalidateFleet = () => {
    void queryClient.invalidateQueries({ queryKey: queryKeys.fleet });
    void queryClient.invalidateQueries({ queryKey: queryKeys.providers });
    void queryClient.invalidateQueries({ queryKey: queryKeys.topology });
  };
  const loadFleetModel = useMutation({ mutationFn: ({ id, instances }: { id: string; instances?: number }) => client.loadFleetModel(id, instances), onSuccess: invalidateFleet });
  const unloadFleetModel = useMutation({ mutationFn: (id: string) => client.unloadFleetModel(id), onSuccess: invalidateFleet });
  const invalidateConfiguredProviders = () => {
    return Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.configuredProviders }),
      queryClient.invalidateQueries({ queryKey: queryKeys.providers }),
      queryClient.invalidateQueries({ queryKey: queryKeys.catalog }),
      queryClient.invalidateQueries({ queryKey: queryKeys.topology }),
    ]);
  };
  const createConfiguredProvider = useMutation({
    mutationFn: (body: CreateConfiguredProviderRequest) => client.createConfiguredProvider(body),
    onSuccess: invalidateConfiguredProviders,
  });
  const deleteConfiguredProvider = useMutation({
    mutationFn: (id: string) => client.deleteConfiguredProvider(id),
    onSuccess: invalidateConfiguredProviders,
  });
  const updateConfiguredProvider = useMutation({
    mutationFn: ({ id, body }: { id: string; body: UpdateConfiguredProviderRequest }) => client.updateConfiguredProvider(id, body),
    onSuccess: (updated) => {
      queryClient.setQueryData<ConfiguredProvidersResponse>(queryKeys.configuredProviders, (current) => ({
        providers: current?.providers.map((provider) => provider.id === updated.id ? updated : provider) ?? [updated],
      }));
      return invalidateConfiguredProviders();
    },
  });

  const inventory = useMemo(() => buildProviderInventory({
    providers: providersQuery.data?.providers ?? [],
    health: nodes,
    edges,
    flows,
    policies: policiesQuery.data?.policies ?? [],
    cacheMetrics: providerMetricsQuery.data?.providers ?? [],
    priceTable: topologyQuery.data?.price_table ?? {},
    perProviderById,
  }), [providersQuery.data, nodes, edges, flows, policiesQuery.data, providerMetricsQuery.data, topologyQuery.data, perProviderById]);

  if (providersQuery.isLoading && nodes.length === 0) {
    return <div className="p-5 text-sm text-text-muted">Loading provider inventory...</div>;
  }

  // Access policy inventory is optional when inference authorization is disabled. The backend
  // advertises that state as 404, so it must not make an otherwise healthy provider inventory
  // look partially broken.
  const policiesError = isOptionalFeatureUnavailable(policiesQuery.error) ? null : policiesQuery.error;
  const error = providersQuery.error || providerMetricsQuery.error || topologyQuery.error || policiesError;

  return (
    <div className="min-h-0 min-w-0 flex-1 overflow-auto p-4" data-testid="providers-view">
      <div className="mb-3 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-sm font-semibold uppercase tracking-[0.18em] text-text">provider inventory</h1>
          <p className="mt-1 text-xs text-text-muted">
            upstream health · advertised catalog · access windows · limits · usage
          </p>
        </div>
      </div>

      {error && (
        <Panel className="mb-3 border-status-cooling/40 bg-status-cooling/10 p-3 text-xs text-status-cooling" data-testid="providers-warning">
          Some provider data is unavailable: {(error as Error).message}
        </Panel>
      )}

      <SummaryStrip summary={inventory.summary} />

      <div className="mt-3 space-y-3">
        <ConfiguredProvidersPanel
          providers={configuredProvidersQuery.data?.providers ?? []}
          loading={configuredProvidersQuery.isLoading}
          error={configuredProvidersQuery.error ? String(configuredProvidersQuery.error) : null}
          mutationsEnabled={mutationsEnabled}
          busy={createConfiguredProvider.isPending || deleteConfiguredProvider.isPending || updateConfiguredProvider.isPending}
          mutationError={String(createConfiguredProvider.error ?? deleteConfiguredProvider.error ?? updateConfiguredProvider.error ?? '') || null}
          onCreate={(body) => createConfiguredProvider.mutate(body)}
          onDelete={(id) => deleteConfiguredProvider.mutate(id)}
          onPatch={(id, body) => updateConfiguredProvider.mutate({ id, body })}
        />
        <MeshAdminPanel
          mesh={meshQuery.data ?? null}
          rows={inventory.rows}
          loading={meshQuery.isLoading}
          error={meshQuery.error ? String(meshQuery.error) : null}
          mutationsEnabled={mutationsEnabled}
          createdToken={createdToken}
          busy={createJoinKey.isPending || revokeJoinKey.isPending || setNodeEnabled.isPending || setModelDisabled.isPending || changeMeshModel.isPending}
          mutationError={String(createJoinKey.error ?? revokeJoinKey.error ?? setNodeEnabled.error ?? setModelDisabled.error ?? changeMeshModel.error ?? '') || null}
          onCreate={(body) => createJoinKey.mutate(body)}
          onDismissToken={() => setCreatedToken(null)}
          onRevoke={(id) => revokeJoinKey.mutate(id)}
          onSetNode={(endpointId, enabled) => setNodeEnabled.mutate({ endpointId, enabled })}
          onSetModel={(endpointId, resourceId, model, disabled) => setModelDisabled.mutate({ endpointId, resourceId, model, disabled })}
          onLoadModel={(endpointId, modelId, unloadIds, instances) => changeMeshModel.mutate({ endpointId, modelId, unloadIds, instances })}
          onUnloadModel={(endpointId, modelId) => changeMeshModel.mutate({ endpointId, modelId, unloadIds: [modelId], unloadOnly: true })}
        />
        <FleetPanel
          fleet={fleetQuery.data ?? null}
          loading={fleetQuery.isLoading}
          error={fleetQuery.error ? String(fleetQuery.error) : null}
          mutationsEnabled={mutationsEnabled}
          busy={loadFleetModel.isPending || unloadFleetModel.isPending}
          mutationError={String(loadFleetModel.error ?? unloadFleetModel.error ?? '') || null}
          onLoad={(id, instances) => loadFleetModel.mutate({ id, instances })}
          onUnload={(id) => unloadFleetModel.mutate(id)}
        />
        <ProviderTable allRows={inventory.rows} />
        <ModelsPanel models={inventory.unionCatalogModels} />
      </div>
    </div>
  );
}

function ConfiguredProvidersPanel({
  providers,
  loading,
  error,
  mutationsEnabled,
  busy,
  mutationError,
  onCreate,
  onDelete,
  onPatch,
}: {
  providers: ConfiguredProvider[];
  loading: boolean;
  error: string | null;
  mutationsEnabled: boolean;
  busy: boolean;
  mutationError: string | null;
  onCreate: (body: CreateConfiguredProviderRequest) => void;
  onDelete: (id: string) => void;
  onPatch: (id: string, body: UpdateConfiguredProviderRequest) => void;
}) {
  const [name, setName] = useState('');
  const [baseUrl, setBaseUrl] = useState('');
  const [apiKey, setApiKey] = useState('');
  const submit = (event: FormEvent) => {
    event.preventDefault();
    onCreate({ name: name.trim(), base_url: baseUrl.trim(), api_key: apiKey });
  };

  return (
    <Panel className="p-4" data-testid="configured-providers-panel">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Additional providers</h2>
          <p className="mt-1 text-[10px] leading-relaxed text-text-muted">
            Add an OpenAI-compatible endpoint. LLMConduit discovers its models; API keys remain server-side.
          </p>
        </div>
        {!mutationsEnabled && (
          <span className="rounded-sm bg-status-cooling/15 px-2 py-1 text-[10px] uppercase tracking-wide text-status-cooling">
            mutations disabled
          </span>
        )}
      </div>

      <form className="mt-3 grid gap-2 lg:grid-cols-[minmax(10rem,0.7fr)_minmax(16rem,1.4fr)_minmax(12rem,1fr)_auto]" onSubmit={submit}>
        <input aria-label="Provider name" required value={name} onChange={(event) => setName(event.target.value)} placeholder="provider name" className={INPUT} />
        <input aria-label="Provider URL" required type="url" value={baseUrl} onChange={(event) => setBaseUrl(event.target.value)} placeholder="https://api.example.com/v1" className={INPUT} />
        <input aria-label="Provider API key" required type="password" autoComplete="new-password" value={apiKey} onChange={(event) => setApiKey(event.target.value)} placeholder="API key" className={INPUT} />
        <Button type="submit" disabled={!mutationsEnabled || busy || !name.trim() || !baseUrl.trim() || !apiKey}>Discover &amp; add</Button>
      </form>

      {loading && <p className="mt-3 text-xs text-text-muted">Loading configured providers...</p>}
      {error && <p className="mt-3 text-xs text-status-down">Could not load configured providers: {error}</p>}
      {mutationError && <p className="mt-3 text-xs text-status-down">Provider change failed: {mutationError}</p>}

      {!loading && providers.length === 0 && (
        <p className="mt-3 text-xs text-text-muted" data-quality="unavailable">No additional providers configured.</p>
      )}
      {providers.length > 0 && (
        <div className="mt-3 grid gap-2 lg:grid-cols-2">
          {providers.map((provider) => (
            <ConfiguredProviderCard
              key={provider.id}
              provider={provider}
              mutationsEnabled={mutationsEnabled}
              busy={busy}
              onDelete={onDelete}
              onPatch={onPatch}
            />
          ))}
        </div>
      )}
    </Panel>
  );
}

function ConfiguredProviderCard({
  provider,
  mutationsEnabled,
  busy,
  onDelete,
  onPatch,
}: {
  provider: ConfiguredProvider;
  mutationsEnabled: boolean;
  busy: boolean;
  onDelete: (id: string) => void;
  onPatch: (id: string, body: UpdateConfiguredProviderRequest) => void;
}) {
  const [modelQuery, setModelQuery] = useState('');
  const normalizedQuery = modelQuery.trim().toLowerCase();
  const allowedModels = Array.isArray(provider.allowed_models) ? provider.allowed_models : null;
  const allowlistMode = allowedModels !== null;
  const allowed = new Set(allowedModels ?? []);
  const disabled = new Set(provider.disabled_models);
  const models = provider.models
    .filter((model) => !normalizedQuery || model.id.toLowerCase().includes(normalizedQuery))
    .sort((a, b) => a.id.localeCompare(b.id));
  const visibleModels = models.slice(0, CONFIGURED_PROVIDER_MODEL_LIMIT);
  const modelIsEnabled = (modelId: string) => (!allowlistMode || allowed.has(modelId)) && !disabled.has(modelId);
  const enabledCount = provider.models.filter((model) => modelIsEnabled(model.id)).length;
  const setAllowlistMode = (enabled: boolean) => {
    if (!enabled) {
      onPatch(provider.id, { allowed_models: null });
      return;
    }
    onPatch(provider.id, {
      allowed_models: provider.models.filter((model) => !disabled.has(model.id)).map((model) => model.id).sort(),
    });
  };
  const setModelEnabled = (modelId: string, enabled: boolean) => {
    if (allowlistMode) {
      const nextAllowed = new Set(allowed);
      const nextDisabled = new Set(disabled);
      if (enabled) {
        nextAllowed.add(modelId);
        nextDisabled.delete(modelId);
      } else {
        nextAllowed.delete(modelId);
      }
      onPatch(provider.id, {
        allowed_models: [...nextAllowed].sort(),
        disabled_models: [...nextDisabled].sort(),
      });
      return;
    }
    const nextDisabled = new Set(disabled);
    if (enabled) nextDisabled.delete(modelId);
    else nextDisabled.add(modelId);
    onPatch(provider.id, { disabled_models: [...nextDisabled].sort() });
  };
  return (
    <div className="min-w-0 rounded border border-line/70 bg-bg p-3" data-testid="configured-provider-card">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-xs font-medium text-text">{provider.name}</div>
          <div className="mt-1 truncate font-mono text-[10px] text-text-muted" title={provider.base_url}>{provider.base_url}</div>
        </div>
        <Button type="button" variant="danger" className="px-2 py-1 text-[10px]" disabled={!mutationsEnabled || busy} onClick={() => onDelete(provider.id)}>Remove</Button>
      </div>
      <div className="mt-2 flex flex-wrap items-center gap-2 text-[10px] text-text-muted">
        <span>{provider.models.length} discovered · {enabledCount} enabled · key {provider.api_key_present ? 'configured' : 'missing'}</span>
        <label className={cn('ml-auto inline-flex items-center gap-1.5 rounded border border-line/70 px-2 py-1', !mutationsEnabled && 'opacity-70')} data-testid="configured-provider-autodiscover">
          <input
            type="checkbox"
            checked={provider.auto_discover}
            disabled={!mutationsEnabled || busy}
            onChange={(event) => onPatch(provider.id, { auto_discover: event.target.checked })}
          />
          <span>auto-discover</span>
        </label>
      </div>
      <p className="mt-2 text-[10px] leading-relaxed text-text-muted">
        {provider.auto_discover ? 'Refreshes about every 5 min and adds newly advertised models.' : 'Automatic catalog refresh is paused; existing model choices remain.'}
      </p>
      <label className={cn('mt-2 inline-flex items-center gap-1.5 rounded border border-line/70 px-2 py-1 text-[10px] text-text-muted', !mutationsEnabled && 'opacity-70')} data-testid="configured-provider-allowlist">
        <input
          type="checkbox"
          checked={allowlistMode}
          disabled={!mutationsEnabled || busy}
          onChange={(event) => setAllowlistMode(event.target.checked)}
        />
        <span>Only allow selected models</span>
      </label>
      <p className="mt-1 text-[10px] leading-relaxed text-text-muted">
        {allowlistMode ? 'Newly discovered models stay visible but disabled until selected.' : 'Newly discovered models are enabled unless individually disabled.'}
      </p>
      <div className="mt-2 flex items-center gap-2">
        <input
          aria-label={`Search models for ${provider.name}`}
          value={modelQuery}
          onChange={(event) => setModelQuery(event.target.value)}
          placeholder="search models"
          className={cn(INPUT, 'min-w-0 flex-1')}
        />
        <span className="shrink-0 text-[10px] text-text-muted">{models.length}/{provider.models.length}</span>
      </div>
      <div className="mt-2 max-h-56 overflow-auto rounded border border-line/60">
        {visibleModels.map((model) => {
          const isEnabled = modelIsEnabled(model.id);
          const isBlacklisted = disabled.has(model.id);
          return (
            <div key={model.id} className="flex min-w-0 items-center gap-2 border-b border-line/50 px-2 py-1.5 last:border-b-0" data-testid="configured-provider-model">
              <input
                aria-label={`Enable ${model.id}`}
                type="checkbox"
                checked={isEnabled}
                disabled={!mutationsEnabled || busy}
                onChange={(event) => setModelEnabled(model.id, event.target.checked)}
              />
              <div className="min-w-0 flex-1">
                <div className={cn('truncate font-mono text-[10px]', isEnabled ? 'text-text' : 'text-text-muted line-through')} title={model.id}>{model.id}</div>
                <div className="text-[9px] text-text-muted">
                  context {model.context_limit == null ? DASH : fmtTokens(model.context_limit)}
                  {!isEnabled && allowlistMode && !allowed.has(model.id) ? ' · not selected' : ''}
                  {isBlacklisted ? ' · disabled' : ''}
                </div>
              </div>
            </div>
          );
        })}
        {models.length === 0 && <div className="px-2 py-3 text-xs italic text-text-muted" data-quality="unavailable">No models match this search.</div>}
      </div>
      {models.length > visibleModels.length && (
        <p className="mt-1 text-[10px] text-text-muted">Showing first {CONFIGURED_PROVIDER_MODEL_LIMIT}; narrow the search to edit more.</p>
      )}
    </div>
  );
}

function FleetPanel({
  fleet,
  loading,
  error,
  mutationsEnabled,
  busy,
  mutationError,
  onLoad,
  onUnload,
}: {
  fleet: FleetModelsResponse | null;
  loading: boolean;
  error: string | null;
  mutationsEnabled: boolean;
  busy: boolean;
  mutationError: string | null;
  onLoad: (id: string, instances?: number) => void;
  onUnload: (id: string) => void;
}) {
  const unavailable = error && error.includes('404');
  const active = fleet?.models.filter((entry) => isFleetActive(entry)).length ?? 0;
  return (
    <Panel className="p-4" data-testid="fleet-panel" data-available={fleet ? 'true' : 'false'}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Fleet GPU switching</h2>
          <p className="mt-1 text-[10px] leading-relaxed text-text-muted">
            Local Fleet models · loaded {active}/{fleet?.models.length ?? 0} · actions use the dashboard mutation gate
          </p>
        </div>
        {!mutationsEnabled && (
          <span className="rounded-sm bg-status-cooling/15 px-2 py-1 text-[10px] uppercase tracking-wide text-status-cooling" data-testid="fleet-mutations-disabled">
            mutations disabled
          </span>
        )}
      </div>

      {loading && <p className="mt-3 text-xs text-text-muted">Loading Fleet state...</p>}
      {unavailable && <p className="mt-3 text-xs text-text-muted" data-quality="unavailable">Local Fleet is not configured on this gateway.</p>}
      {error && !unavailable && <p className="mt-3 text-xs text-status-down">Could not load Fleet state: {error}</p>}
      {mutationError && <p className="mt-2 text-xs text-status-down">{mutationError}</p>}

      {fleet && (
        <div className="mt-3 grid gap-2 lg:grid-cols-2">
          {fleet.models.map((entry) => (
            <FleetModelCard
              key={entry.model.id}
              entry={entry}
              busy={busy}
              mutationsEnabled={mutationsEnabled}
              onLoad={onLoad}
              onUnload={onUnload}
            />
          ))}
          {fleet.models.length === 0 && <EmptyMeshLine>No Fleet models configured.</EmptyMeshLine>}
        </div>
      )}
    </Panel>
  );
}

function FleetModelCard({
  entry,
  busy,
  mutationsEnabled,
  onLoad,
  onUnload,
}: {
  entry: FleetModelEntry;
  busy: boolean;
  mutationsEnabled: boolean;
  onLoad: (id: string, instances?: number) => void;
  onUnload: (id: string) => void;
}) {
  const [confirming, setConfirming] = useState<'load' | 'unload' | null>(null);
  const desiredInstanceCount = modelDesiredInstances(entry);
  const [instanceText, setInstanceText] = useState(String(desiredInstanceCount));
  useEffect(() => setInstanceText(String(desiredInstanceCount)), [desiredInstanceCount]);
  const status = modelRuntimeStatus(entry.status);
  const active = status.state === 'loaded';
  const transitioning = status.state === 'loading';
  const phase = entry.status.phase.toLowerCase();
  const canUnload = !transitioning && (active || phase === 'failed' || phase === 'unhealthy' || Boolean(entry.status.container_status));
  const instanceValidation = validateInstanceText(instanceText, entry.model.max_instances);
  const requestedInstances = instanceValidation.value ?? desiredInstanceCount;
  const scaling = active && requestedInstances !== desiredInstanceCount;
  const gpus = entry.status.assigned_gpus.length ? entry.status.assigned_gpus.map((gpu) => `GPU ${gpu}`).join(', ') : DASH;
  return (
    <div className="min-w-0 rounded border border-line/70 bg-bg p-3" data-testid="fleet-model-card">
      <div className="flex items-start justify-between gap-3">
        <div className="flex min-w-0 gap-2">
          <ModelStatusLight status={status} testId="fleet-model-status" />
          <div className="min-w-0">
            <div className="truncate font-mono text-xs text-text" title={entry.model.id}>{entry.model.id}</div>
            <div className="mt-1 truncate text-[10px] text-text-muted" title={entry.model.image}>{entry.model.image}</div>
          </div>
        </div>
        <span className={cn('rounded-sm px-2 py-1 text-[10px] uppercase tracking-wide', fleetPhaseClass(entry.status.phase))}>
          {entry.status.phase}
        </span>
      </div>
      {entry.model.description && <p className="mt-2 text-[10px] leading-relaxed text-text-muted">{entry.model.description}</p>}
      <div className="mt-3 grid grid-cols-2 gap-2 text-[10px]">
        <FleetFact label="desired" value={entry.status.desired_state} />
        <FleetFact label="instances" value={formatInstanceSummary(entry.status.ready_instances, entry.status.desired_instances, entry.status.instances?.length)} />
        <FleetFact label="gpus" value={gpus} />
        <FleetFact label="health" value={entry.status.health ?? entry.status.container_status ?? DASH} />
        <FleetFact label="checked" value={entry.status.last_checked ? fmtFleetTime(entry.status.last_checked) : DASH} />
      </div>
      <InstanceList instances={entry.status.instances} />
      {entry.status.last_error && (
        <p className={cn('mt-2 text-[10px]', transitioning ? 'text-status-cooling' : 'text-status-down')}>
          {transitioning ? 'Waiting for readiness: ' : ''}{entry.status.last_error}
        </p>
      )}
      <div className="mt-3 flex flex-wrap items-end justify-between gap-2">
        <InstanceCountField
          label="Instances"
          value={instanceText}
          onChange={setInstanceText}
          disabled={!mutationsEnabled || busy || transitioning}
          max={entry.model.max_instances}
          error={instanceValidation.error}
        />
        <div className="flex justify-end gap-2">
          <Button type="button" disabled={!mutationsEnabled || busy || transitioning || Boolean(instanceValidation.error) || (active && !scaling)} onClick={() => setConfirming('load')} className="px-2 py-1 text-[10px]">{active ? 'Scale' : 'Load'}</Button>
          <Button type="button" variant="danger" disabled={!mutationsEnabled || busy || !canUnload} onClick={() => setConfirming('unload')} className="px-2 py-1 text-[10px]">Unload</Button>
        </div>
      </div>
      {confirming && (
        <SimpleConfirmationDialog
          title={`Confirm ${confirming === 'load' && active ? 'scale' : confirming}: ${entry.model.id}`}
          message={confirming === 'load' ? `This will set the desired Fleet instance count to ${requestedInstances}.` : 'This will stop every instance and remove the model from routing until it is loaded again.'}
          confirmLabel={`Confirm ${confirming}`}
          danger={confirming === 'unload'}
          onCancel={() => setConfirming(null)}
          onConfirm={() => {
            if (confirming === 'load') onLoad(entry.model.id, requestedInstances);
            else onUnload(entry.model.id);
            setConfirming(null);
          }}
        />
      )}
    </div>
  );
}

function SimpleConfirmationDialog({ title, message, confirmLabel, danger, onCancel, onConfirm }: {
  title: string;
  message: string;
  confirmLabel: string;
  danger?: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onCancel(); }}>
      <div className="w-full max-w-md rounded-lg border border-line bg-panel p-4 shadow-2xl" role="dialog" aria-modal="true" aria-label={title} data-testid="confirmation-dialog">
        <h3 className="text-sm font-semibold text-text">{title}</h3>
        <p className="mt-2 text-xs leading-relaxed text-text-muted">{message}</p>
        <div className="mt-4 flex justify-end gap-2">
          <Button type="button" variant="ghost" onClick={onCancel}>Cancel</Button>
          <Button type="button" variant={danger ? 'danger' : 'default'} onClick={onConfirm}>{confirmLabel}</Button>
        </div>
      </div>
    </div>
  );
}

function FleetFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 rounded border border-line/60 bg-panel px-2 py-1.5">
      <div className="uppercase tracking-[0.14em] text-text-muted">{label}</div>
      <div className="mt-1 truncate font-mono text-text" title={value} data-quality={value === DASH ? 'unavailable' : 'measured'}>{value}</div>
    </div>
  );
}

function InstanceCountField({ label, value, onChange, disabled, max, error }: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  disabled: boolean;
  max?: number;
  error?: string;
}) {
  return (
    <label className="min-w-24 text-[10px] uppercase tracking-wide text-text-muted">
      {label}
      <input
        aria-label={label}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        disabled={disabled}
        inputMode="numeric"
        className={cn(INPUT, 'mt-1 w-24')}
      />
      <span className={cn('mt-1 block normal-case tracking-normal', error ? 'text-status-down' : 'text-text-muted')}>
        {error ?? `1-${Math.min(max ?? INSTANCE_LIMIT, INSTANCE_LIMIT)}`}
      </span>
    </label>
  );
}

function InstanceList({ instances }: { instances?: Array<{ instance_id?: string; index: number; port?: number; phase: string; container_status?: string; assigned_gpus?: number[]; last_error?: string }> }) {
  if (!instances?.length) return null;
  return (
    <div className="mt-2 space-y-1" data-testid="instance-list">
      {instances.map((instance) => (
        <div key={instance.instance_id ?? instance.index} className="grid grid-cols-[2.5rem_minmax(0,1fr)_minmax(0,1fr)] gap-2 rounded border border-line/60 bg-panel px-2 py-1 text-[10px]">
          <span className="font-mono text-text">#{instance.index}</span>
          <span className="truncate text-text-muted" title={instance.assigned_gpus?.map((gpu) => `GPU ${gpu}`).join(', ')}>
            {instance.assigned_gpus?.length ? instance.assigned_gpus.map((gpu) => `GPU ${gpu}`).join(', ') : DASH}
          </span>
          <span className="truncate text-text-muted" title={instance.last_error ?? instance.container_status ?? instance.phase}>
            {instance.port ? `:${instance.port} · ` : ''}{instance.phase}{instance.container_status ? ` · ${instance.container_status}` : ''}{instance.last_error ? ` · ${instance.last_error}` : ''}
          </span>
        </div>
      ))}
    </div>
  );
}

function validateInstanceText(value: string, max?: number): { value?: number; error?: string } {
  const trimmed = value.trim();
  const limit = Math.min(max ?? INSTANCE_LIMIT, INSTANCE_LIMIT);
  if (!/^\d+$/.test(trimmed)) return { error: 'whole number' };
  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed) || parsed < 1) return { error: 'min 1' };
  if (parsed > limit) return { error: `max ${limit}` };
  return { value: parsed };
}

function formatInstanceSummary(ready?: number, desired?: number, fallback?: number): string {
  const actualReady = ready ?? fallback;
  if (actualReady === undefined && desired === undefined) return DASH;
  if (desired === undefined) return String(actualReady ?? 0);
  return `${actualReady ?? 0}/${desired}`;
}

function modelDesiredInstances(entry: FleetModelEntry): number {
  return Math.max(1, entry.status.desired_instances ?? entry.status.ready_instances ?? entry.status.instances?.length ?? 1);
}

function meshDesiredInstances(model: MeshSwitchableModel): number {
  return Math.max(1, model.desired_instances ?? model.ready_instances ?? model.instances?.length ?? 1);
}

function isFleetActive(entry: FleetModelEntry): boolean {
  return modelRuntimeStatus(entry.status).state === 'loaded';
}

function isOptionalFeatureUnavailable(error: unknown): boolean {
  return error instanceof Error && /failed: 404$/.test(error.message);
}

type RuntimeStatusInput = Pick<FleetDeploymentStatus, 'phase' | 'desired_state'>;
type RuntimeStatusState = 'loaded' | 'loading' | 'failed' | 'unloaded';

interface RuntimeStatus {
  state: RuntimeStatusState;
  label: string;
}

function modelRuntimeStatus(status: RuntimeStatusInput): RuntimeStatus {
  const phase = status.phase.toLowerCase();
  const desired = status.desired_state.toLowerCase();
  if (phase === 'failed' || phase === 'unhealthy') return { state: 'failed', label: `failed (${status.phase})` };
  if (phase === 'loading' || phase === 'stopping') return { state: 'loading', label: `${status.phase} (${status.desired_state})` };
  if (phase === 'ready' && (desired === 'ready' || desired === 'loaded')) return { state: 'loaded', label: 'loaded' };
  return { state: 'unloaded', label: phase === 'unloaded' ? 'unloaded' : `${status.phase} (${status.desired_state})` };
}

function ModelStatusLight({ status, testId }: { status: RuntimeStatus; testId: string }) {
  const color = status.state === 'loaded' ? 'bg-status-healthy'
    : status.state === 'loading' ? 'bg-status-cooling'
      : status.state === 'failed' ? 'bg-status-down'
        : 'bg-text-muted/40';
  return (
    <span
      className="mt-0.5 inline-flex shrink-0 items-center gap-1.5 text-[9px] uppercase tracking-wide text-text-muted"
      role="status"
      aria-label={`model status: ${status.label}`}
      title={`model status: ${status.label}`}
      data-testid={testId}
      data-status={status.state}
    >
      <span className={cn('h-2 w-2 rounded-full', color)} aria-hidden />
      <span className="sr-only">{status.label}</span>
    </span>
  );
}

function fleetPhaseClass(phase: string): string {
  if (phase === 'ready') return 'bg-status-healthy/15 text-status-healthy';
  if (phase === 'loading' || phase === 'stopping') return 'bg-status-cooling/15 text-status-cooling';
  if (phase === 'failed' || phase === 'unhealthy') return 'bg-status-down/15 text-status-down';
  return 'bg-panel text-text-muted';
}

function MeshAdminPanel({
  mesh,
  rows,
  loading,
  error,
  mutationsEnabled,
  createdToken,
  busy,
  mutationError,
  onCreate,
  onDismissToken,
  onRevoke,
  onSetNode,
  onSetModel,
  onLoadModel,
  onUnloadModel,
}: {
  mesh: MeshAdminState | null;
  rows: ProviderInventoryRow[];
  loading: boolean;
  error: string | null;
  mutationsEnabled: boolean;
  createdToken: { label: string | null; token: string } | null;
  busy: boolean;
  mutationError: string | null;
  onCreate: (body: { label?: string; max_uses?: number; expires_in_secs?: number }) => void;
  onDismissToken: () => void;
  onRevoke: (id: string) => void;
  onSetNode: (endpointId: string, enabled: boolean) => void;
  onSetModel: (endpointId: string, resourceId: string, model: string, disabled: boolean) => void;
  onLoadModel: (endpointId: string, modelId: string, unloadIds: string[], instances?: number) => void;
  onUnloadModel: (endpointId: string, modelId: string) => void;
}) {
  const [label, setLabel] = useState('');
  const [maxUses, setMaxUses] = useState('1');
  const [expiresHours, setExpiresHours] = useState('24');
  const disabledModels = new Set((mesh?.disabled_models ?? []).map(disabledModelKey));
  // Enrollment labels may predate a worker rename; prefer its live advertised name.
  const namedNodes = (mesh?.nodes ?? []).map((node) => {
    const advertised = rows.find((row) => row.id === `mesh:${node.endpoint_id}` && row.name.trim() && row.name !== row.id);
    return advertised ? { ...node, label: advertised.name.trim() } : node;
  });
  const meshEndpointIds = new Set((mesh?.nodes ?? []).map((node) => node.endpoint_id));
  const meshRows = rows
    .map((row) => ({ row, endpointId: endpointFromProvider(row.id) }))
    .filter(({ row, endpointId }) => endpointId && meshEndpointIds.has(endpointId) && row.resourceId);
  const submit = (event: FormEvent) => {
    event.preventDefault();
    onCreate({
      label: label.trim() || undefined,
      max_uses: numberField(maxUses),
      expires_in_secs: numberField(expiresHours) == null ? undefined : numberField(expiresHours)! * 3600,
    });
  };
  const unavailable = error && error.includes('404');

  return (
    <Panel className="p-4" data-testid="mesh-admin" data-available={mesh ? 'true' : 'false'}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Mesh enrollment</h2>
          <p className="mt-1 text-[10px] leading-relaxed text-text-muted">
            Provision provider tokens, revoke future enrollment, disable nodes, or suppress one advertised model.
          </p>
        </div>
        {!mutationsEnabled && (
          <span className="rounded-sm bg-status-cooling/15 px-2 py-1 text-[10px] uppercase tracking-wide text-status-cooling" data-testid="mesh-mutations-disabled">
            mutations disabled
          </span>
        )}
      </div>

      {loading && <p className="mt-3 text-xs text-text-muted">Loading mesh state...</p>}
      {unavailable && <p className="mt-3 text-xs text-text-muted" data-quality="unavailable">Mesh controller is not enabled on this gateway.</p>}
      {error && !unavailable && <p className="mt-3 text-xs text-status-down">Could not load mesh admin state: {error}</p>}

      {createdToken && (
        <div className="mt-3 rounded border border-status-healthy/40 bg-status-healthy/10 p-3" data-testid="mesh-created-token">
          <div className="text-[10px] uppercase tracking-[0.14em] text-status-healthy">new enrollment token{createdToken.label ? ` · ${createdToken.label}` : ''} · shown once</div>
          <code className="mt-1 block select-all break-all font-mono text-xs text-text">{createdToken.token}</code>
          <button type="button" className="mt-1 text-[10px] text-text-muted hover:text-text" onClick={onDismissToken}>dismiss</button>
        </div>
      )}

      {mesh && (
        <>
          <form className="mt-3 grid gap-2 md:grid-cols-[minmax(0,1fr)_7rem_7rem_auto]" onSubmit={submit}>
            <input aria-label="Enrollment label" value={label} onChange={(event) => setLabel(event.target.value)} placeholder="label" className={INPUT} />
            <input aria-label="Max uses" value={maxUses} onChange={(event) => setMaxUses(event.target.value)} inputMode="numeric" className={INPUT} />
            <input aria-label="Expires hours" value={expiresHours} onChange={(event) => setExpiresHours(event.target.value)} inputMode="numeric" className={INPUT} />
            <Button type="submit" disabled={!mutationsEnabled || busy} className="text-xs">Create token</Button>
          </form>
          {mutationError && <p className="mt-2 text-xs text-status-down">{mutationError}</p>}
          <div className="mt-3 grid gap-3 xl:grid-cols-3">
            <JoinKeyList keys={mesh.join_keys} busy={busy} mutationsEnabled={mutationsEnabled} onRevoke={onRevoke} />
            <NodeList nodes={namedNodes} busy={busy} mutationsEnabled={mutationsEnabled} onSetNode={onSetNode} />
            <ModelOverrideList rows={meshRows} disabledModels={disabledModels} busy={busy} mutationsEnabled={mutationsEnabled} onSetModel={onSetModel} />
          </div>
          <SwitchableModelList nodes={namedNodes} busy={busy} mutationsEnabled={mutationsEnabled} onLoadModel={onLoadModel} onUnloadModel={onUnloadModel} />
        </>
      )}
    </Panel>
  );
}

function JoinKeyList({ keys, busy, mutationsEnabled, onRevoke }: { keys: MeshJoinKey[]; busy: boolean; mutationsEnabled: boolean; onRevoke: (id: string) => void }) {
  return (
    <MeshList title="tokens" count={keys.length}>
      {keys.map((key) => (
        <div key={key.id} className="border-b border-line/70 py-2 last:border-0">
          <div className="flex items-center justify-between gap-2">
            <span className="truncate font-mono text-[10px]" title={key.id}>{key.label || key.id}</span>
            <Button type="button" variant="danger" disabled={!mutationsEnabled || busy || !key.enabled} onClick={() => onRevoke(key.id)} className="px-2 py-1 text-[10px]">Revoke</Button>
          </div>
          <div className="mt-1 text-[10px] text-text-muted">
            {key.enabled ? 'enabled' : 'revoked'} · uses {key.use_count}/{key.max_uses ?? DASH} · expires {key.expires_at_ms ? fmtWhen(key.expires_at_ms) : DASH}
          </div>
        </div>
      ))}
      {keys.length === 0 && <EmptyMeshLine>No enrollment tokens.</EmptyMeshLine>}
    </MeshList>
  );
}

function NodeList({ nodes, busy, mutationsEnabled, onSetNode }: { nodes: MeshNode[]; busy: boolean; mutationsEnabled: boolean; onSetNode: (endpointId: string, enabled: boolean) => void }) {
  return (
    <MeshList title="nodes" count={nodes.length}>
      {nodes.map((node) => (
        <div key={node.endpoint_id} className="border-b border-line/70 py-2 last:border-0">
          <div className="flex items-center justify-between gap-2">
            <span className="truncate font-mono text-[10px]" title={node.endpoint_id}>{node.label || node.endpoint_id}</span>
            <Button type="button" variant={node.enabled ? 'danger' : 'default'} disabled={!mutationsEnabled || busy} onClick={() => onSetNode(node.endpoint_id, !node.enabled)} className="px-2 py-1 text-[10px]">
              {node.enabled ? 'Disable' : 'Enable'}
            </Button>
          </div>
          <div className="mt-1 text-[10px] text-text-muted">
            {node.enabled ? 'enabled' : 'disabled'} · last seen {node.last_seen_at_ms ? fmtElapsed(Date.now() - node.last_seen_at_ms) + ' ago' : DASH}
          </div>
        </div>
      ))}
      {nodes.length === 0 && <EmptyMeshLine>No enrolled nodes.</EmptyMeshLine>}
    </MeshList>
  );
}

function ModelOverrideList({
  rows,
  disabledModels,
  busy,
  mutationsEnabled,
  onSetModel,
}: {
  rows: Array<{ row: ProviderInventoryRow; endpointId: string | null }>;
  disabledModels: Set<string>;
  busy: boolean;
  mutationsEnabled: boolean;
  onSetModel: (endpointId: string, resourceId: string, model: string, disabled: boolean) => void;
}) {
  const entries = rows.flatMap(({ row, endpointId }) => row.advertisedModels.map((model) => ({ row, endpointId, model }))).filter((entry): entry is { row: ProviderInventoryRow; endpointId: string; model: string } => Boolean(entry.endpointId && entry.row.resourceId));
  return (
    <MeshList title="model overrides" count={disabledModels.size}>
      {entries.slice(0, 12).map(({ row, endpointId, model }) => {
        const resourceId = row.resourceId ?? '';
        const disabled = disabledModels.has(disabledModelKey({ endpoint_id: endpointId, resource_id: resourceId, model }));
        return (
          <div key={`${endpointId}/${resourceId}/${model}`} className="border-b border-line/70 py-2 last:border-0">
            <div className="flex items-center justify-between gap-2">
              <span className="truncate font-mono text-[10px]" title={`${endpointId}/${resourceId}/${model}`}>{model}</span>
              <Button type="button" variant={disabled ? 'default' : 'danger'} disabled={!mutationsEnabled || busy} onClick={() => onSetModel(endpointId, resourceId, model, !disabled)} className="px-2 py-1 text-[10px]">
                {disabled ? 'Enable' : 'Disable'}
              </Button>
            </div>
            <div className="mt-1 truncate text-[10px] text-text-muted" title={endpointId}>{row.name} · {resourceId} · {disabled ? 'disabled' : 'routable'}</div>
          </div>
        );
      })}
      {entries.length === 0 && <EmptyMeshLine>No mesh models advertised.</EmptyMeshLine>}
    </MeshList>
  );
}

function SwitchableModelList({
  nodes,
  busy,
  mutationsEnabled,
  onLoadModel,
  onUnloadModel,
}: {
  nodes: MeshNode[];
  busy: boolean;
  mutationsEnabled: boolean;
  onLoadModel: (endpointId: string, modelId: string, unloadIds: string[], instances?: number) => void;
  onUnloadModel: (endpointId: string, modelId: string) => void;
}) {
  const [pending, setPending] = useState<PendingRemoteAction | null>(null);
  const [instanceCounts, setInstanceCounts] = useState<Record<string, string>>({});
  const providers = useMemo(() => nodes.filter((node) => node.model_switching), [nodes]);
  const count = providers.reduce((total, node) => total + (node.model_switching?.models.length ?? 0), 0);
  useEffect(() => {
    setInstanceCounts((current) => {
      const next = { ...current };
      for (const node of providers) {
        for (const model of node.model_switching?.models ?? []) {
          const key = remoteModelKey(node.endpoint_id, model.id);
          if (next[key] === undefined) next[key] = String(meshDesiredInstances(model));
        }
      }
      return next;
    });
  }, [providers]);
  return (
    <div className="mt-3 overflow-hidden rounded border border-line/70 bg-bg" data-testid="remote-model-switcher">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-line/70 bg-panel/60 px-3 py-2">
        <div>
          <h3 className="text-[10px] uppercase tracking-[0.14em] text-text-muted">remote model switching</h3>
          <p className="mt-0.5 text-[10px] text-text-muted">Fleet-capable downstream providers</p>
        </div>
        <span className="rounded-full border border-line px-2 py-0.5 font-mono text-[10px] text-text-muted">{count} models</span>
      </div>
      <div className={cn('grid gap-3 p-3', providers.length > 1 && 'xl:grid-cols-2')}>
        {providers.map((node) => (
          <section key={node.endpoint_id} className="min-w-0 rounded-md border border-line/70 bg-panel/40 p-3">
            <div className="mb-2 flex min-w-0 items-center justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-xs font-medium text-text" title={node.endpoint_id}>{node.label || node.endpoint_id}</div>
                <div className="mt-0.5 truncate font-mono text-[9px] text-text-muted">{node.model_switching?.provider} · rev {node.model_switching?.revision}</div>
              </div>
              <span className="shrink-0 rounded-full bg-status-healthy/10 px-2 py-0.5 text-[9px] uppercase tracking-wide text-status-healthy">connected</span>
            </div>
            <div className="grid gap-2 sm:grid-cols-2">
              {node.model_switching?.models.map((model) => {
                const status = modelRuntimeStatus(model);
                const loaded = status.state === 'loaded';
                const transitioning = status.state === 'loading';
                const canUnload = !transitioning && meshModelHasResidentCapacity(model);
                const countKey = remoteModelKey(node.endpoint_id, model.id);
                const instanceText = instanceCounts[countKey] ?? String(meshDesiredInstances(model));
                const instanceValidation = validateInstanceText(instanceText, model.max_instances);
                const requestedInstances = instanceValidation.value ?? meshDesiredInstances(model);
                const scaling = loaded && requestedInstances !== meshDesiredInstances(model);
                return (
                  <div
                    key={model.id}
                    className="min-w-0 rounded border border-line/60 bg-bg/70 px-3 py-2"
                    data-testid="remote-switch-model"
                  >
                    <div className="flex min-w-0 items-center gap-3">
                      <ModelStatusLight status={status} testId="remote-model-status" />
                      <div className="min-w-0 flex-1">
                        <div className="truncate font-mono text-[11px] text-text" title={model.id}>{model.id}</div>
                        <div className="mt-0.5 flex min-w-0 items-center gap-1.5 text-[9px] text-text-muted">
                          <span className={cn(
                            'shrink-0 uppercase tracking-wide',
                            status.state === 'loaded' ? 'text-status-healthy' : status.state === 'loading' ? 'text-status-cooling' : status.state === 'failed' ? 'text-status-down' : 'text-text-muted',
                          )}>{model.phase}</span>
                          <span className="shrink-0">· {formatInstanceSummary(model.ready_instances, model.desired_instances, model.instances?.length)}</span>
                          {model.description && <span className="truncate" title={model.description}>· {model.description}</span>}
                        </div>
                      </div>
                      <Button
                        type="button"
                        disabled={!mutationsEnabled || busy || transitioning || Boolean(instanceValidation.error) || (loaded && !scaling)}
                        onClick={() => setPending({ kind: 'load', endpointId: node.endpoint_id, model, models: node.model_switching?.models ?? [], instances: requestedInstances })}
                        className="shrink-0 px-2.5 py-1 text-[10px]"
                      >
                        {loaded ? 'Scale' : transitioning ? model.phase : 'Load'}
                      </Button>
                      <Button
                        type="button"
                        variant="danger"
                        disabled={!mutationsEnabled || busy || !canUnload}
                        onClick={() => setPending({ kind: 'unload', endpointId: node.endpoint_id, model, models: node.model_switching?.models ?? [] })}
                        className="shrink-0 px-2.5 py-1 text-[10px]"
                      >
                        Unload
                      </Button>
                    </div>
                    <div className="mt-2">
                      <InstanceCountField
                        label="Instances"
                        value={instanceText}
                        onChange={(value) => setInstanceCounts((current) => ({ ...current, [countKey]: value }))}
                        disabled={!mutationsEnabled || busy || transitioning}
                        max={model.max_instances}
                        error={instanceValidation.error}
                      />
                    </div>
                    <InstanceList instances={model.instances} />
                  </div>
                );
              })}
            </div>
          </section>
        ))}
        {providers.length === 0 && <div className="xl:col-span-2"><EmptyMeshLine>No downstream provider advertises model switching.</EmptyMeshLine></div>}
      </div>
      {pending && (
        <ModelLifecycleDialog
          action={pending}
          busy={busy}
          onCancel={() => setPending(null)}
          onConfirm={(unloadIds) => {
            if (pending.kind === 'load') onLoadModel(pending.endpointId, pending.model.id, unloadIds, pending.instances);
            else onUnloadModel(pending.endpointId, pending.model.id);
            setPending(null);
          }}
        />
      )}
    </div>
  );
}

interface PendingRemoteAction {
  kind: 'load' | 'unload';
  endpointId: string;
  model: MeshSwitchableModel;
  models: MeshSwitchableModel[];
  instances?: number;
}

function ModelLifecycleDialog({ action, busy, onCancel, onConfirm }: {
  action: PendingRemoteAction;
  busy: boolean;
  onCancel: () => void;
  onConfirm: (unloadIds: string[]) => void;
}) {
  const plan = useMemo(() => capacityPlan(action.models, action.model, action.instances ?? 1), [action.models, action.model, action.instances]);
  const [selected, setSelected] = useState<string[]>(plan.defaultUnloadIds);
  const freed = plan.candidates.filter((model) => selected.includes(model.id)).reduce((sum, model) => sum + modelGPUCount(model), 0);
  const enoughCapacity = action.kind === 'unload' || freed >= plan.shortfall;
  const unloadIds = action.kind === 'load' ? (plan.unloadEverything ? plan.candidates.map((model) => model.id) : selected) : [];
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onCancel(); }}>
      <div className="w-full max-w-lg rounded-lg border border-line bg-panel p-4 shadow-2xl" role="dialog" aria-modal="true" aria-labelledby="model-action-title" data-testid="model-lifecycle-dialog">
        <h3 id="model-action-title" className="text-sm font-semibold text-text">
          Confirm {action.kind === 'load' && modelRuntimeStatus(action.model).state === 'loaded' ? 'scale' : action.kind}: <span className="font-mono">{action.model.id}</span>
        </h3>
        {action.kind === 'unload' ? (
          <p className="mt-2 text-xs leading-relaxed text-text-muted">This will stop the model and remove it from routing until it is loaded again.</p>
        ) : plan.unloadEverything ? (
          <p className="mt-2 text-xs leading-relaxed text-status-cooling" data-testid="unload-everything-warning">
            This model needs all {plan.totalGPUs} GPU slots. Every currently loaded model on this node will be unloaded before it starts.
          </p>
        ) : plan.shortfall > 0 ? (
          <>
            <p className="mt-2 text-xs leading-relaxed text-text-muted">
              Loading {action.instances ?? 1} instance{(action.instances ?? 1) === 1 ? '' : 's'} needs {plan.requiredGPUs} GPUs total and {plan.additionalGPUs} additional; choose models that free at least {plan.shortfall} more GPU slot{plan.shortfall === 1 ? '' : 's'}.
            </p>
            <div className="mt-3 space-y-2" data-testid="capacity-eviction-choices">
              {plan.candidates.map((model) => (
                <label key={model.id} className="flex items-center justify-between gap-3 rounded border border-line/70 bg-bg px-3 py-2 text-xs">
                  <span className="truncate font-mono">{model.id}</span>
                  <span className="flex shrink-0 items-center gap-2 text-text-muted">
                    {modelGPUCount(model)} GPUs
                    <input type="checkbox" checked={selected.includes(model.id)} onChange={(event) => setSelected((current) => event.target.checked ? [...current, model.id] : current.filter((id) => id !== model.id))} />
                  </span>
                </label>
              ))}
            </div>
          </>
        ) : (
          <p className="mt-2 text-xs leading-relaxed text-text-muted">
            Loading {action.instances ?? 1} instance{(action.instances ?? 1) === 1 ? '' : 's'} needs {plan.requiredGPUs} GPUs total and {plan.additionalGPUs} additional. The desired instance count will be set to {action.instances ?? 1}.
          </p>
        )}
        {action.kind === 'load' && unloadIds.length > 0 && (
          <p className="mt-3 text-[10px] text-text-muted">Unload first: {unloadIds.join(', ')}</p>
        )}
        <div className="mt-4 flex justify-end gap-2">
          <Button type="button" variant="ghost" disabled={busy} onClick={onCancel}>Cancel</Button>
          <Button type="button" variant={action.kind === 'unload' || unloadIds.length > 0 ? 'danger' : 'default'} disabled={busy || !enoughCapacity} onClick={() => onConfirm(unloadIds)}>
            Confirm {action.kind}
          </Button>
        </div>
      </div>
    </div>
  );
}

function modelGPUCount(model: MeshSwitchableModel): number {
  return model.gpu_count ?? model.assigned_gpus?.length ?? 0;
}

function capacityPlan(models: MeshSwitchableModel[], target: MeshSwitchableModel, instances: number) {
  const residentModels = models.filter(meshModelHasResidentCapacity);
  const candidates = residentModels.filter((model) => model.id !== target.id);
  const assigned = new Set(residentModels.flatMap((model) => model.assigned_gpus ?? model.instances?.flatMap((instance) => instance.assigned_gpus ?? []) ?? []));
  const totalGPUs = Math.max(0, ...models.map(modelGPUCount), ...Array.from(assigned, (gpu) => gpu + 1));
  const requiredGPUs = modelGPUCount(target) * instances;
  const currentTargetGPUs = meshModelHasResidentCapacity(target) ? liveModelGPUCount(target) : 0;
  const additionalGPUs = Math.max(0, requiredGPUs - currentTargetGPUs);
  const shortfall = Math.max(0, additionalGPUs - Math.max(0, totalGPUs - assigned.size));
  const unloadEverything = requiredGPUs > 0 && totalGPUs > 0 && requiredGPUs >= totalGPUs && candidates.length > 0;
  const defaultUnloadIds: string[] = [];
  let freed = 0;
  for (const model of candidates) {
    if (freed >= shortfall) break;
    defaultUnloadIds.push(model.id);
    freed += modelGPUCount(model);
  }
  return { candidates, totalGPUs, requiredGPUs, additionalGPUs, shortfall, unloadEverything, defaultUnloadIds };
}

function remoteModelKey(endpointId: string, modelId: string): string {
  return `${endpointId}\n${modelId}`;
}

function meshModelHasResidentCapacity(model: MeshSwitchableModel): boolean {
  const phase = model.phase.toLowerCase();
  return modelRuntimeStatus(model).state === 'loaded'
    || (model.ready_instances ?? 0) > 0
    || Boolean(model.instances?.some((instance) => instance.phase.toLowerCase() !== 'unloaded' || (instance.assigned_gpus?.length ?? 0) > 0))
    || (model.assigned_gpus?.length ?? 0) > 0
    || phase === 'failed'
    || phase === 'unhealthy'
    || Boolean(model.container_status)
    || Boolean(model.last_error);
}

function liveModelGPUCount(model: MeshSwitchableModel): number {
  const fromInstances = model.instances?.reduce((sum, instance) => sum + (instance.assigned_gpus?.length ?? 0), 0) ?? 0;
  return Math.max(model.assigned_gpus?.length ?? 0, fromInstances);
}

async function waitForRemoteModelUnloaded(fetchMesh: () => Promise<MeshAdminState>, endpointId: string, modelId: string): Promise<void> {
  for (let attempt = 0; attempt < 300; attempt += 1) {
    const mesh = await fetchMesh();
    const model = mesh.nodes.find((node) => node.endpoint_id === endpointId)?.model_switching?.models.find((entry) => entry.id === modelId);
    if (model && modelRuntimeStatus(model).state === 'unloaded') return;
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  throw new Error(`Timed out waiting for ${modelId} to unload`);
}

function MeshList({ title, count, children }: { title: string; count: number; children: ReactNode }) {
  return (
    <div className="min-w-0 rounded border border-line/70 bg-bg px-3 py-2">
      <div className="flex items-center justify-between border-b border-line/70 pb-1">
        <h3 className="text-[10px] uppercase tracking-[0.14em] text-text-muted">{title}</h3>
        <span className="font-mono text-[10px] text-text-muted">{count}</span>
      </div>
      <div>{children}</div>
    </div>
  );
}

function EmptyMeshLine({ children }: { children: ReactNode }) {
  return <p className="py-4 text-center text-xs italic text-text-muted" data-quality="unavailable">{children}</p>;
}

function endpointFromProvider(providerId: string): string | null {
  return providerId.startsWith('mesh:') ? providerId.slice('mesh:'.length) : providerId || null;
}

function disabledModelKey(value: Pick<MeshDisabledModel, 'endpoint_id' | 'resource_id' | 'model'>): string {
  return `${value.endpoint_id}\n${value.resource_id}\n${value.model.toLowerCase()}`;
}

function numberField(value: string): number | undefined {
  const parsed = Number(value.trim());
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : undefined;
}

function fmtWhen(ms: number): string {
  return new Date(ms).toISOString().slice(0, 16).replace('T', ' ');
}

function fmtFleetTime(value: string): string {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? fmtElapsed(Date.now() - parsed) + ' ago' : value;
}

function SummaryStrip({ summary }: { summary: ProviderInventorySummary }) {
  const cells = [
    { label: 'slots', value: String(summary.providers), quality: 'measured' },
    { label: 'healthy', value: String(summary.healthy), quality: 'measured', accent: 'text-status-healthy' },
    { label: 'cooling', value: String(summary.cooling), quality: 'measured', accent: 'text-status-cooling' },
    { label: 'down', value: String(summary.down), quality: 'measured', accent: 'text-status-down' },
    { label: 'models', value: summary.advertisedModels ? String(summary.advertisedModels) : DASH, quality: summary.advertisedModels ? 'derived' : 'unavailable' },
    { label: 'requests', value: String(summary.requests), quality: 'measured' },
    { label: 'active', value: String(summary.active), quality: 'measured', accent: 'text-accent' },
    { label: 'cost', value: fmtCost(summary.cost), quality: costQuality(summary.costConfidence), accent: 'text-meta' },
  ];
  return (
    <Panel className="grid grid-cols-2 gap-px overflow-hidden bg-line sm:grid-cols-4 xl:grid-cols-8" data-testid="providers-summary">
      {cells.map((cell) => (
        <div key={cell.label} className="bg-panel px-3 py-2">
          <div className="text-[9px] uppercase tracking-[0.14em] text-text-muted">{cell.label}</div>
          <div className={cn('mt-1 font-mono text-lg tabular-nums text-text', cell.accent)} data-quality={cell.quality}>
            {cell.value}
          </div>
        </div>
      ))}
    </Panel>
  );
}

function ProviderTable({ allRows }: { allRows: ProviderInventoryRow[] }) {
  const [status, setStatus] = useState<'all' | ProviderHealth['status']>('healthy');
  const [query, setQuery] = useState('');
  const [provider, setProvider] = useState('');
  const [availability, setAvailability] = useState('all');
  const [pageSize, setPageSize] = useState(5);
  const [page, setPage] = useState(1);
  const allGroups = groupProviderSlots(allRows);
  const needle = query.trim().toLowerCase();
  const groups = allGroups
    .filter((group) => !provider || group.name === provider)
    .map((group) => ({
      name: group.name,
      total: group.rows.length,
      rows: group.rows.filter((row) => {
        if (status !== 'all' && row.status !== status) return false;
        if (availability !== 'all' && row.capacity.accepting !== (availability === 'accepting')) return false;
        return !needle || [row.id, row.name, row.resourceId, row.route, row.baseUrl,
          ...row.advertisedModels, ...row.policy.subjects, ...row.policy.endpoints,
        ].filter(Boolean).join(' ').toLowerCase().includes(needle);
      // Live health and usage changes must not move slots between pages.
      }).sort((a, b) => (a.resourceId ?? a.id).localeCompare(b.resourceId ?? b.id) || a.key.localeCompare(b.key)),
    }))
    .filter((group) => group.rows.length > 0);
  const rows = groups.flatMap((group) => group.rows);
  const pageCount = Math.max(1, Math.ceil(rows.length / pageSize));
  const currentPage = Math.min(page, pageCount);
  if (page !== currentPage) setPage(currentPage);
  const start = (currentPage - 1) * pageSize;
  const visibleKeys = new Set(rows.slice(start, start + pageSize).map((row) => row.key));
  const visibleGroups = groups.map((group) => ({ ...group, visibleRows: group.rows.filter((row) => visibleKeys.has(row.key)) }))
    .filter((group) => group.visibleRows.length > 0);
  const hasFilters = Boolean(query || provider || status !== 'all' || availability !== 'all');
  return (
    <div className="space-y-3" data-testid="providers-table" data-available={rows.length > 0 ? 'true' : 'false'}>
      <div className="flex flex-wrap items-center justify-between gap-2 px-1">
        <h2 className="text-sm font-semibold">Provider slots</h2>
        <span className="font-mono text-[10px] text-text-muted">
          {groups.length} {groups.length === 1 ? 'provider' : 'providers'} · {rows.length} / {allRows.length} slots
        </span>
      </div>
      <Panel className="space-y-2 p-3">
        <div className="flex flex-wrap items-center gap-2">
          <input aria-label="Search providers" value={query}
            onChange={(event) => { setQuery(event.target.value); setPage(1); }}
            placeholder="Search provider, slot, model, subject..." className={cn(INPUT, 'min-w-0 flex-1 basis-64')} />
          <select aria-label="Filter by provider" value={provider}
            onChange={(event) => { setProvider(event.target.value); setPage(1); }} className={cn(INPUT, 'max-w-full')}>
            <option value="">All providers</option>
            {allGroups.map((group) => <option key={group.name} value={group.name}>{group.name}</option>)}
            {provider && !allGroups.some((group) => group.name === provider) && <option value={provider}>{provider} (unavailable)</option>}
          </select>
          <select aria-label="Filter by availability" value={availability}
            onChange={(event) => { setAvailability(event.target.value); setPage(1); }} className={INPUT}>
            <option value="all">Any availability</option>
            <option value="accepting">Accepting requests</option>
            <option value="closed">Closed to requests</option>
          </select>
        </div>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex overflow-hidden rounded-md border border-line text-xs" role="group" aria-label="Provider status">
            {(['all', 'healthy', 'cooling', 'down'] as const).map((item) => (
              <button key={item} type="button" aria-pressed={status === item}
                onClick={() => { setStatus(item); setPage(1); }}
                className={cn('px-3 py-1.5 uppercase tracking-[0.12em]', status === item ? 'bg-accent/15 text-accent' : 'bg-panel text-text-muted hover:text-text')}>
                {item}
              </button>
            ))}
          </div>
          {hasFilters && <Button type="button" variant="ghost" className="text-xs"
            onClick={() => { setQuery(''); setProvider(''); setStatus('all'); setAvailability('all'); setPage(1); }}>Clear filters</Button>}
        </div>
      </Panel>
      <nav aria-label="Provider slot pagination" className="flex flex-wrap items-center justify-between gap-2 px-1 text-xs text-text-muted">
        <span aria-live="polite">Showing {rows.length ? `${start + 1}–${Math.min(start + pageSize, rows.length)}` : '0'} of {rows.length} matching slots</span>
        <div className="flex flex-wrap items-center gap-2">
          <label className="flex items-center gap-2">Slots per page
            <select value={pageSize} onChange={(event) => { setPageSize(Number(event.target.value)); setPage(1); }} className={INPUT}>
              {[5, 10, 25].map((size) => <option key={size} value={size}>{size}</option>)}
            </select>
          </label>
          <Button type="button" aria-label="Previous page" variant="ghost" disabled={currentPage === 1} onClick={() => setPage(currentPage - 1)} className="text-xs">Previous</Button>
          <span className="font-mono text-[10px]">Page {currentPage} of {pageCount}</span>
          <Button type="button" aria-label="Next page" variant="ghost" disabled={currentPage === pageCount} onClick={() => setPage(currentPage + 1)} className="text-xs">Next</Button>
        </div>
      </nav>
      {visibleGroups.map((group) => (
        <Panel key={group.name} className="overflow-hidden" role="region" aria-label={group.name} data-testid="provider-group">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-line bg-accent/5 px-4 py-3">
            <h3 className="min-w-0 break-words text-sm font-semibold text-text">{group.name}</h3>
            <span className="shrink-0 font-mono text-[10px] text-text-muted">
              {group.rows.length} / {group.total} {group.total === 1 ? 'slot' : 'slots'}
              {group.visibleRows.length < group.rows.length ? ` · ${group.visibleRows.length} on this page` : ''}
            </span>
          </div>
          <div className="overflow-auto">
            <table className="w-full min-w-[1180px] text-left text-xs" aria-label={`${group.name} slots`}>
              <thead className="bg-panel text-[10px] uppercase tracking-[0.12em] text-text-muted">
                <tr>
                  <th className="px-4 py-2">Slot</th>
                  <th className="px-3 py-2">Advertised models</th>
                  <th className="px-3 py-2">Windows & limits</th>
                  <th className="px-3 py-2">Usage</th>
                  <th className="px-3 py-2">Latency</th>
                  <th className="px-3 py-2">Traffic</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-line">
                {group.visibleRows.map((row) => <ProviderRow key={row.key} row={row} />)}
              </tbody>
            </table>
          </div>
        </Panel>
      ))}
      {rows.length === 0 && (
        <Panel className="p-6 text-center text-xs italic text-text-muted" data-testid="providers-empty" data-quality="unavailable">
          No providers match this filter.
        </Panel>
      )}
    </div>
  );
}

function ProviderRow({ row }: { row: ProviderInventoryRow }) {
  const errorRate = row.usage.requests > 0 ? (row.usage.failures / row.usage.requests) * 100 : null;
  const latency = buildProviderLatency(row.perProvider, row.id);
  return (
    <tr data-testid="provider-row" data-provider={row.id} data-resource={row.resourceId ?? ''}>
      <td className="px-4 py-3 align-top">
        <div className="flex items-center gap-2">
          <span className={cn('h-2.5 w-2.5 rounded-full', statusDot(row.status))} aria-hidden />
          <div>
            <div className="font-medium text-text">{row.resourceId ?? row.route ?? row.id}</div>
            <div className="mt-0.5 font-mono text-[10px] text-text-muted">
              {row.id}{row.route ? ` · ${row.route}` : ''}
            </div>
          </div>
        </div>
        <div className="mt-2 max-w-[18rem] truncate font-mono text-[10px] text-text-muted" title={row.baseUrl}>{row.baseUrl}</div>
        <div className="mt-1 text-[10px] text-text-muted">
          catalog {row.catalogSize ?? DASH} · fetched {row.catalogFetchedMs ? fmtElapsed(Date.now() - row.catalogFetchedMs) + ' ago' : DASH}
        </div>
        {row.lastError && <div className="mt-1 max-w-[18rem] truncate text-[10px] text-status-down" title={row.lastError}>{row.lastError}</div>}
      </td>
      <td className="px-3 py-3 align-top">
        <div className="flex max-w-[19rem] flex-wrap gap-1">
          {row.advertisedModels.slice(0, 6).map((model) => (
            <span key={model} className="rounded-sm bg-line/60 px-1.5 py-0.5 font-mono text-[10px]" title={model}>{model}</span>
          ))}
          {row.advertisedModels.length > 6 && <span className="rounded-sm bg-line/40 px-1.5 py-0.5 text-[10px] text-text-muted">+{row.advertisedModels.length - 6}</span>}
          {row.advertisedModels.length === 0 && <span className="text-text-muted" data-quality="unavailable">{DASH}</span>}
        </div>
        <div className="mt-2 space-y-0.5">
          {row.contextWindows.slice(0, 3).map((entry) => (
            <div key={entry.model} className="flex max-w-[18rem] justify-between gap-3 font-mono text-[10px] text-text-muted">
              <span className="truncate">{entry.model}</span>
              <span data-quality={entry.contextLimit == null ? 'unavailable' : 'measured'}>{entry.contextLimit == null ? DASH : fmtTokens(entry.contextLimit)}</span>
            </div>
          ))}
        </div>
        <div className="mt-2 text-[10px] text-text-muted">
          priced {row.priceCoverage.priced}/{row.priceCoverage.total || 0}
        </div>
      </td>
      <td className="px-3 py-3 align-top">
        <div className="mb-2 border-l-2 border-accent/40 pl-2">
          <div className="text-[9px] uppercase tracking-[0.12em] text-text-muted">capacity / availability</div>
          <div className="mt-1 text-[10px] text-text-muted">
            {row.capacity.active ?? DASH}/{row.capacity.limit ?? DASH} active · {row.capacity.accepting ? 'accepting' : 'closed'}
          </div>
          <div className="mt-1 text-[10px] text-text-muted">
            {row.availability.summary.length > 0
              ? row.availability.summary.slice(0, 2).join(' · ')
              : row.availability.defaultCapacity == null
                ? 'Schedule unavailable'
                : `Default capacity ${row.availability.defaultCapacity}`}
            {row.availability.timezone ? ` · ${row.availability.timezone}` : ''}
          </div>
        </div>
        <div className="text-[9px] uppercase tracking-[0.12em] text-text-muted">access policy</div>
        {row.policy.policyIds.length === 0 ? (
          <div className="mt-1 text-[11px] text-text-muted" data-quality="unavailable">No model access policies</div>
        ) : (
          <>
            <div className="flex flex-wrap gap-1">
              <span className="rounded-sm bg-status-healthy/15 px-1.5 py-0.5 text-[10px] text-status-healthy">{row.policy.allowCount} allow</span>
              <span className="rounded-sm bg-status-down/15 px-1.5 py-0.5 text-[10px] text-status-down">{row.policy.denyCount} deny</span>
            </div>
            <div className="mt-2 text-[10px] text-text-muted">
              {row.policy.windows.length > 0 ? row.policy.windows.slice(0, 2).join(' · ') : 'Any time'}
            </div>
            <div className="mt-1 text-[10px] text-text-muted">
              {row.policy.limits.length > 0 ? row.policy.limits.join(' · ') : 'No session limits'}
            </div>
            <div className="mt-1 max-w-[18rem] truncate text-[10px] text-text-muted" title={row.policy.subjects.join(', ')}>
              subjects {row.policy.subjects.length ? row.policy.subjects.join(', ') : DASH}
            </div>
          </>
        )}
      </td>
      <td className="px-3 py-3 align-top font-mono text-[10px] tabular-nums">
        <MetricLine label="req" value={String(row.usage.requests)} quality="measured" />
        <MetricLine label="err" value={formatPercent(errorRate)} quality={errorRate == null ? 'unavailable' : 'derived'} danger={(errorRate ?? 0) > 0} />
        <MetricLine label="tok" value={fmtTokens(row.usage.promptTokens == null && row.usage.completionTokens == null ? null : (row.usage.promptTokens ?? 0) + (row.usage.completionTokens ?? 0))} quality={row.usage.promptTokens == null && row.usage.completionTokens == null ? 'unavailable' : 'measured'} />
        <MetricLine label="cache" value={fmtTokens(row.usage.cachedTokens)} quality={row.usage.cachedTokens == null ? 'unavailable' : 'measured'} />
        <MetricLine label="cost" value={fmtCost(row.usage.cost)} quality={costQuality(row.usage.costConfidence)} />
        <MetricLine label="hit%" value={formatPercent(row.cacheMetrics?.cache_hit_rate == null ? null : row.cacheMetrics.cache_hit_rate * 100)} quality={row.cacheMetrics?.cache_hit_rate == null ? 'unavailable' : 'derived'} />
        <MetricLine label="kv" value={formatPercent(row.cacheMetrics?.kv_cache_usage == null ? null : row.cacheMetrics.kv_cache_usage * 100)} quality={row.cacheMetrics?.kv_cache_usage == null ? 'unavailable' : 'derived'} />
      </td>
      <td className="px-3 py-3 align-top font-mono text-[10px] tabular-nums">
        <MetricLine label="p50" value={latency.p50.text} quality={latency.p50.quality} />
        <MetricLine label="p95" value={latency.p95.text} quality={latency.p95.quality} />
        <MetricLine label="p99" value={latency.p99.text} quality={latency.p99.quality} danger={row.perProvider ? row.perProvider.p99 >= 1000 : false} />
        <MetricLine label="fail" value={latency.errorRate.text} quality={latency.errorRate.quality} danger={(row.perProvider?.error_rate ?? 0) > 0} />
      </td>
      <td className="px-3 py-3 align-top font-mono text-[10px] tabular-nums">
        <MetricLine label="req/s" value={row.edge ? row.edge.throughput.toFixed(2) : DASH} quality={row.edge ? 'derived' : 'unavailable'} />
        <MetricLine label="tok/s" value={row.edge ? row.edge.tokens_per_sec.toFixed(row.edge.tokens_per_sec < 10 ? 1 : 0) : DASH} quality={row.edge ? 'derived' : 'unavailable'} />
        <MetricLine label="$/s" value={row.edge ? fmtCost(row.edge.cost_per_sec) : DASH} quality={row.edge && row.edge.cost_per_sec > 0 ? 'derived' : 'unavailable'} />
        <MetricLine label="attempt p99" value={row.perProvider ? fmtProviderLatencyMs(row.perProvider.p99) : DASH} quality={row.perProvider ? 'derived' : 'unavailable'} />
      </td>
    </tr>
  );
}

function MetricLine({ label, value, quality, danger }: { label: string; value: string; quality: string; danger?: boolean }) {
  return (
    <div className="flex justify-between gap-3">
      <span className="text-text-muted">{label}</span>
      <span data-quality={quality} className={cn(danger ? 'text-status-down' : 'text-text')}>{value}</span>
    </div>
  );
}

function ModelsPanel({ models }: { models: Array<{ id: string; contextLimit: number | null; priced: boolean }> }) {
  return (
    <div>
      <Panel className="p-4" data-testid="providers-catalog">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold">Global advertised model catalog</h2>
          <span className="font-mono text-[10px] text-text-muted">{models.length} models</span>
        </div>
        <p className="mt-1 text-[10px] leading-relaxed text-text-muted">
          Exact provider-scoped model advertisements from /dashboard/api/providers.
        </p>
        <div className="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
          {models.map((entry) => (
            <div key={entry.id} className="grid grid-cols-[minmax(0,1fr)_4rem_3rem] items-center gap-2 rounded border border-line/70 bg-bg px-2 py-1.5 text-[10px]">
              <span className="truncate font-mono" title={entry.id}>{entry.id}</span>
              <span className="text-right font-mono text-text-muted" data-quality={entry.contextLimit == null ? 'unavailable' : 'measured'}>{entry.contextLimit == null ? DASH : fmtTokens(entry.contextLimit)}</span>
              <span className={cn('text-right uppercase tracking-wide', entry.priced ? 'text-status-healthy' : 'text-text-muted')}>{entry.priced ? 'priced' : DASH}</span>
            </div>
          ))}
          {models.length === 0 && (
            <p className="py-4 text-center text-xs italic text-text-muted" data-quality="unavailable">
              No catalog models advertised yet.
            </p>
          )}
        </div>
      </Panel>
    </div>
  );
}

function statusDot(status: ProviderHealth['status']): string {
  if (status === 'healthy') return 'bg-status-healthy';
  if (status === 'cooling') return 'bg-status-cooling';
  return 'bg-status-down';
}
