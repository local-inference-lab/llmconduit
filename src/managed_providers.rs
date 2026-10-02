use crate::control_plane_store::PersistenceStore;
use crate::dashboard_flow::DashboardFlowStore;
use crate::error::{AppError, AppResult};
use crate::upstream::{
    BackendCandidate, BackendCandidatePlan, BackendChatRequest, DynUpstreamClient,
    InferenceEndpoint, ProviderInventoryEntry, ProxyCompletionsRequest, ReqwestUpstreamClient,
    UpstreamClient, UpstreamModelEntry, UpstreamModelsResponse, canonical_model_key,
};
use async_trait::async_trait;
use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode, header};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};
use utoipa::ToSchema;
use uuid::Uuid;

const STORE_KEY: &str = "configured_providers";
const MAX_CONFIGURED_PROVIDERS: usize = 32;
const MAX_PROVIDER_MODELS: usize = 4096;
const MODEL_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(20);
const MODEL_DISCOVERY_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct ManagedProviderOptions {
    pub http_client: reqwest::Client,
    pub flatten_content: bool,
    pub min_completion_tokens: i64,
    pub max_sse_frame_bytes: usize,
    pub finalization_policies: crate::upstream::BackendFinalizationPolicies,
    pub flow_store: DashboardFlowStore,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfiguredProvidersBody {
    pub providers: Vec<ConfiguredProviderView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfiguredProviderView {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub api_key_present: bool,
    pub models: Vec<UpstreamModelEntry>,
    pub auto_discover: bool,
    pub allowed_models: Option<Vec<String>>,
    pub disabled_models: Vec<String>,
}

#[derive(Clone, Deserialize, ToSchema)]
pub struct CreateConfiguredProviderRequest {
    pub name: String,
    pub base_url: String,
    pub api_key: String,
}

#[derive(Clone, Deserialize, ToSchema)]
pub struct UpdateConfiguredProviderRequest {
    pub auto_discover: Option<bool>,
    #[serde(default)]
    #[schema(value_type = Option<Vec<String>>, nullable = true)]
    pub allowed_models: ModelListUpdate,
    pub disabled_models: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum ModelListUpdate {
    #[default]
    Omitted,
    Clear,
    Replace(Vec<String>),
}

impl<'de> Deserialize<'de> for ModelListUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Option::<Vec<String>>::deserialize(deserializer).map(|value| match value {
            Some(models) => Self::Replace(models),
            None => Self::Clear,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredProviders {
    providers: Vec<StoredProvider>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredProvider {
    id: String,
    name: String,
    base_url: String,
    api_key: String,
    models: Vec<UpstreamModelEntry>,
    #[serde(default = "default_auto_discover")]
    auto_discover: bool,
    #[serde(default)]
    allowed_models: Option<Vec<String>>,
    #[serde(default)]
    disabled_models: Vec<String>,
}

#[derive(Clone)]
struct ManagedProvider {
    stored: StoredProvider,
    client: DynUpstreamClient,
    revision: u64,
}

#[derive(Default)]
struct ManagedProviderState {
    loaded: bool,
    providers: Vec<ManagedProvider>,
}

pub struct ManagedProviderRegistry {
    store: Arc<dyn PersistenceStore>,
    options: ManagedProviderOptions,
    state: RwLock<ManagedProviderState>,
    vision_changes: tokio::sync::watch::Sender<u64>,
}

impl ManagedProviderRegistry {
    pub fn new(store: Arc<dyn PersistenceStore>, options: ManagedProviderOptions) -> Arc<Self> {
        let registry = Arc::new(Self {
            store,
            options,
            state: RwLock::new(ManagedProviderState::default()),
            vision_changes: tokio::sync::watch::channel(0).0,
        });
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let registry = Arc::downgrade(&registry);
            handle.spawn(async move {
                if let Some(registry) = registry.upgrade() {
                    if let Err(err) = registry.ensure_loaded().await {
                        tracing::warn!(error = %err, "failed to load configured providers");
                    }
                    if let Err(err) = registry.refresh_configured_providers_once().await {
                        tracing::warn!(error = %err, "failed to refresh configured provider catalogs");
                    }
                }
                refresh_configured_providers_loop(registry).await;
            });
        }
        registry
    }

    pub async fn list(&self) -> AppResult<Vec<ConfiguredProviderView>> {
        self.ensure_loaded().await?;
        Ok(self
            .state
            .read()
            .await
            .providers
            .iter()
            .map(provider_view)
            .collect())
    }

    /// Only enabled catalog entries are candidates for automatic vision checks.
    pub async fn vision_probe_targets(&self) -> AppResult<Vec<crate::vision_probe::ProbeTarget>> {
        self.ensure_loaded().await?;
        let state = self.state.read().await;
        Ok(state
            .providers
            .iter()
            .flat_map(|provider| {
                provider
                    .stored
                    .models
                    .iter()
                    .filter(|model| model_enabled(&provider.stored, &model.id))
                    .map(|model| crate::vision_probe::ProbeTarget {
                        backend: provider.stored.id.clone(),
                        base_url: provider.stored.base_url.clone(),
                        api_key: Some(provider.stored.api_key.clone()),
                        model: model.id.clone(),
                    })
            })
            .collect())
    }

    pub fn vision_probe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.vision_changes.subscribe()
    }

    pub async fn add(
        &self,
        request: CreateConfiguredProviderRequest,
    ) -> AppResult<ConfiguredProviderView> {
        self.ensure_loaded().await?;
        let name = validate_provider_name(&request.name)?;
        let base_url = normalize_provider_url(&request.base_url)?;
        let api_key = request.api_key.trim().to_string();
        if api_key.is_empty() {
            return Err(AppError::bad_request("api_key is required"));
        }
        let client: DynUpstreamClient =
            Arc::new(self.make_client(base_url.clone(), Some(api_key.clone())));
        let models = discover_provider_models(&client).await?;

        let mut state = self.state.write().await;
        if state.providers.len() >= MAX_CONFIGURED_PROVIDERS {
            return Err(AppError::bad_request(format!(
                "too many configured providers; maximum is {MAX_CONFIGURED_PROVIDERS}"
            )));
        }
        if state
            .providers
            .iter()
            .any(|provider| provider.stored.name.eq_ignore_ascii_case(&name))
        {
            return Err(AppError::bad_request("provider name already exists"));
        }
        if state
            .providers
            .iter()
            .any(|provider| provider.stored.base_url == base_url.as_str())
        {
            return Err(AppError::bad_request("provider URL already exists"));
        }
        let stored = StoredProvider {
            id: format!("cfg_{}", Uuid::new_v4().simple()),
            name,
            base_url: base_url.to_string(),
            api_key,
            models,
            auto_discover: true,
            allowed_models: None,
            disabled_models: Vec::new(),
        };
        state.providers.push(ManagedProvider {
            stored,
            client,
            revision: 0,
        });
        match self.persist_locked(&state).await {
            Ok(()) => {
                let provider = state
                    .providers
                    .last()
                    .map(provider_view)
                    .expect("provider was just inserted");
                Ok(provider)
            }
            Err(err) => {
                state.providers.pop();
                Err(err)
            }
        }
    }

    pub async fn update(
        &self,
        id: &str,
        request: UpdateConfiguredProviderRequest,
    ) -> AppResult<Option<ConfiguredProviderView>> {
        self.ensure_loaded().await?;
        let mut state = self.state.write().await;
        let Some(index) = state
            .providers
            .iter()
            .position(|provider| provider.stored.id == id)
        else {
            return Ok(None);
        };
        let original = state.providers[index].clone();
        let allowed_models = match request.allowed_models {
            ModelListUpdate::Replace(allowed_models) => Some(Some(validate_model_list(
                &state.providers[index].stored.models,
                allowed_models,
                "allowed",
            )?)),
            ModelListUpdate::Clear => Some(None),
            ModelListUpdate::Omitted => None,
        };
        let disabled_models = match request.disabled_models {
            Some(disabled_models) => Some(validate_model_list(
                &state.providers[index].stored.models,
                disabled_models,
                "disabled",
            )?),
            None => None,
        };
        if let Some(auto_discover) = request.auto_discover {
            state.providers[index].stored.auto_discover = auto_discover;
        }
        if let Some(allowed_models) = allowed_models {
            state.providers[index].stored.allowed_models = allowed_models;
        }
        if let Some(disabled_models) = disabled_models {
            state.providers[index].stored.disabled_models = disabled_models;
        }
        state.providers[index].revision = state.providers[index].revision.saturating_add(1);
        match self.persist_locked(&state).await {
            Ok(()) => Ok(Some(provider_view(&state.providers[index]))),
            Err(err) => {
                state.providers[index] = original;
                Err(err)
            }
        }
    }

    pub async fn delete(&self, id: &str) -> AppResult<bool> {
        self.ensure_loaded().await?;
        let mut state = self.state.write().await;
        let Some(index) = state
            .providers
            .iter()
            .position(|provider| provider.stored.id == id)
        else {
            return Ok(false);
        };
        let removed = state.providers.remove(index);
        match self.persist_locked(&state).await {
            Ok(()) => Ok(true),
            Err(err) => {
                state.providers.insert(index, removed);
                Err(err)
            }
        }
    }

    async fn ensure_loaded(&self) -> AppResult<()> {
        if self.state.read().await.loaded {
            return Ok(());
        }
        let mut state = self.state.write().await;
        if state.loaded {
            return Ok(());
        }
        let stored = match self.store.get_setting(STORE_KEY).await {
            Ok(Some(value)) if !value.trim().is_empty() => {
                serde_json::from_str::<StoredProviders>(&value).map_err(|err| {
                    AppError::internal(format!("configured providers JSON is invalid: {err}"))
                })?
            }
            Ok(_) => StoredProviders {
                providers: Vec::new(),
            },
            Err(err) => {
                return Err(AppError::internal(format!(
                    "failed to load configured providers: {err}"
                )));
            }
        };
        let providers = stored
            .providers
            .into_iter()
            .take(MAX_CONFIGURED_PROVIDERS)
            .filter_map(|stored| match Url::parse(&stored.base_url) {
                Ok(base_url) => Some(ManagedProvider {
                    client: Arc::new(self.make_client(base_url, Some(stored.api_key.clone()))),
                    stored,
                    revision: 0,
                }),
                Err(err) => {
                    tracing::warn!(
                        provider_id = %stored.id,
                        error = %err,
                        "skipping configured provider with invalid stored URL"
                    );
                    None
                }
            })
            .collect();
        state.providers = providers;
        state.loaded = true;
        self.vision_changes
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn persist_locked(&self, state: &ManagedProviderState) -> AppResult<()> {
        let stored = StoredProviders {
            providers: state
                .providers
                .iter()
                .map(|provider| provider.stored.clone())
                .collect(),
        };
        let json = serde_json::to_string(&stored).map_err(|err| {
            AppError::internal(format!("failed to serialize configured providers: {err}"))
        })?;
        self.store
            .set_setting(STORE_KEY, &json)
            .await
            .map_err(|err| {
                AppError::internal(format!("failed to persist configured providers: {err}"))
            })?;
        // Notify only after a successful save; failed edits are rolled back.
        self.vision_changes
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn refresh_configured_providers_once(&self) -> AppResult<()> {
        self.ensure_loaded().await?;
        let snapshots = {
            let state = self.state.read().await;
            state
                .providers
                .iter()
                .filter(|provider| provider.stored.auto_discover)
                .map(|provider| ProviderRefreshSnapshot {
                    id: provider.stored.id.clone(),
                    revision: provider.revision,
                    client: Arc::clone(&provider.client),
                })
                .collect::<Vec<_>>()
        };

        for snapshot in snapshots {
            match discover_provider_models(&snapshot.client).await {
                Ok(models) => {
                    if let Err(err) = self.merge_refreshed_models(&snapshot, models).await {
                        tracing::warn!(
                            provider_id = %snapshot.id,
                            error = %err,
                            "failed to persist refreshed configured provider catalog"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        provider_id = %snapshot.id,
                        error = %err,
                        "configured provider model refresh failed; preserving existing catalog"
                    );
                }
            }
        }
        Ok(())
    }

    async fn merge_refreshed_models(
        &self,
        snapshot: &ProviderRefreshSnapshot,
        models: Vec<UpstreamModelEntry>,
    ) -> AppResult<()> {
        let mut state = self.state.write().await;
        let Some(index) = state
            .providers
            .iter()
            .position(|provider| provider.stored.id == snapshot.id)
        else {
            return Ok(());
        };
        let provider = &mut state.providers[index];
        if provider.revision != snapshot.revision || !provider.stored.auto_discover {
            return Ok(());
        }
        let mut seen = provider
            .stored
            .models
            .iter()
            .map(|entry| entry.id.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        let mut merged_models = provider.stored.models.clone();
        let mut changed = false;
        for model in models {
            if seen.insert(model.id.to_ascii_lowercase()) {
                if merged_models.len() >= MAX_PROVIDER_MODELS {
                    return Err(AppError::bad_request(format!(
                        "provider returned too many models; maximum is {MAX_PROVIDER_MODELS}"
                    )));
                }
                merged_models.push(model);
                changed = true;
            }
        }
        if !changed {
            return Ok(());
        }
        merged_models.sort_by_key(|a| a.id.to_ascii_lowercase());
        let original = provider.clone();
        provider.stored.models = merged_models;
        provider.revision = provider.revision.saturating_add(1);
        match self.persist_locked(&state).await {
            Ok(()) => Ok(()),
            Err(err) => {
                state.providers[index] = original;
                Err(err)
            }
        }
    }

    fn make_client(&self, base_url: Url, api_key: Option<String>) -> ReqwestUpstreamClient {
        ReqwestUpstreamClient::with_options(
            self.options.http_client.clone(),
            base_url,
            api_key,
            None::<PathBuf>,
            self.options.flatten_content,
            self.options.min_completion_tokens,
            self.options.max_sse_frame_bytes,
        )
        .with_finalization_policies(self.options.finalization_policies.clone())
        .with_flow_store(self.options.flow_store.clone())
    }

    async fn route_for_model(
        &self,
        model: &str,
        base_models: &[UpstreamModelEntry],
    ) -> Option<ManagedProvider> {
        self.ensure_loaded().await.ok()?;
        if base_models
            .iter()
            .any(|entry| entry.id.eq_ignore_ascii_case(model))
        {
            return None;
        }
        let state = self.state.read().await;
        let mut exact_matches = state
            .providers
            .iter()
            .filter(|provider| {
                provider.stored.models.iter().any(|entry| {
                    entry.id.eq_ignore_ascii_case(model)
                        && model_enabled(&provider.stored, &entry.id)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if exact_matches.len() == 1 {
            return exact_matches.pop();
        }
        if !exact_matches.is_empty() {
            return None;
        }
        if resolve_model_entry_in_catalog(base_models, model).is_some() {
            return None;
        }
        let mut matches = state
            .providers
            .iter()
            .filter(|provider| provider_model_enabled(&provider.stored, model))
            .cloned()
            .collect::<Vec<_>>();
        (matches.len() == 1).then(|| matches.remove(0))
    }

    async fn blocked_configured_model(
        &self,
        model: &str,
        base_models: &[UpstreamModelEntry],
    ) -> bool {
        if model.trim().is_empty() {
            return false;
        }
        if resolve_model_entry_in_catalog(base_models, model).is_some() {
            return false;
        }
        self.ensure_loaded().await.ok();
        let state = self.state.read().await;
        if enabled_configured_model_matches(&state.providers, model) {
            return false;
        }
        blocked_configured_model_matches(&state.providers, model)
    }

    pub(crate) async fn disabled_model_blocks_default(&self, model: &str) -> bool {
        if model.trim().is_empty() {
            return false;
        }
        self.ensure_loaded().await.ok();
        let state = self.state.read().await;
        !enabled_configured_model_matches(&state.providers, model)
            && blocked_configured_model_matches(&state.providers, model)
    }

    async fn inventory(&self) -> AppResult<Vec<ProviderInventoryEntry>> {
        self.ensure_loaded().await?;
        Ok(self
            .state
            .read()
            .await
            .providers
            .iter()
            .map(|provider| ProviderInventoryEntry {
                provider_id: provider.stored.id.clone(),
                provider_name: provider.stored.name.clone(),
                resource_id: None,
                route: Some("configured".to_string()),
                base_url: provider.stored.base_url.clone(),
                models: enabled_provider_models(&provider.stored),
                availability: None,
                capacity_limit: None,
                active_requests: None,
                accepting_requests: true,
                healthy: true,
            })
            .collect())
    }
}

#[derive(Clone)]
struct ProviderRefreshSnapshot {
    id: String,
    revision: u64,
    client: DynUpstreamClient,
}

#[derive(Clone)]
struct CachedBaseCatalog {
    fetched_at: Instant,
    catalog: Vec<UpstreamModelEntry>,
}

pub struct ManagedProviderUpstream {
    base: DynUpstreamClient,
    registry: Arc<ManagedProviderRegistry>,
    base_catalog: RwLock<Option<CachedBaseCatalog>>,
}

impl ManagedProviderUpstream {
    pub fn new(base: DynUpstreamClient, registry: Arc<ManagedProviderRegistry>) -> Self {
        Self {
            base,
            registry,
            base_catalog: RwLock::new(None),
        }
    }

    async fn base_catalog(&self) -> AppResult<Vec<UpstreamModelEntry>> {
        if let Some(cached) = self.base_catalog.read().await.as_ref()
            && cached.fetched_at.elapsed() < self.base.model_catalog_cache_ttl()
        {
            return Ok(cached.catalog.clone());
        }
        let catalog = self.base.supported_model_catalog().await?;
        *self.base_catalog.write().await = Some(CachedBaseCatalog {
            fetched_at: Instant::now(),
            catalog: catalog.clone(),
        });
        Ok(catalog)
    }

    async fn base_catalog_or_empty(&self) -> Vec<UpstreamModelEntry> {
        self.base_catalog().await.unwrap_or_default()
    }

    fn routed_request(
        &self,
        backend: &BackendChatRequest,
        provider: &ManagedProvider,
    ) -> BackendChatRequest {
        let mut request = backend.request.clone();
        let resolved = resolve_provider_model(&provider.stored, &request.model)
            .unwrap_or_else(|| request.model.clone());
        request.model = resolved;
        if let Some(serving) = &backend.serving {
            serving.set_route("configured");
            serving.set_provider(provider.stored.name.clone());
        }
        BackendChatRequest {
            request,
            client_chat_template_kwargs: backend.client_chat_template_kwargs.clone(),
            thinking_override: backend.thinking_override,
            response_id: backend.response_id.clone(),
            serving: backend.serving.clone(),
            capture: backend.capture.clone(),
            persistence_capture: backend.persistence_capture.clone(),
            capture_payloads: backend.capture_payloads,
            authorization: backend.authorization.clone(),
            affinity: backend.affinity.clone(),
            endpoint: backend.endpoint,
            authorization_route: Some("configured".to_string()),
            authorization_provider: Some(provider.stored.name.clone()),
        }
    }
}

#[async_trait]
impl UpstreamClient for ManagedProviderUpstream {
    async fn stream_chat_completion(
        &self,
        request: &BackendChatRequest,
    ) -> AppResult<crate::upstream::UpstreamStream> {
        self.stream_chat_completion_with_timeout(request, Duration::from_secs(60))
            .await
    }

    async fn stream_chat_completion_with_timeout(
        &self,
        request: &BackendChatRequest,
        request_timeout: Duration,
    ) -> AppResult<crate::upstream::UpstreamStream> {
        let base_catalog = self.base_catalog_or_empty().await;
        if let Some(provider) = self
            .registry
            .route_for_model(&request.request.model, &base_catalog)
            .await
        {
            let routed = self.routed_request(request, &provider);
            ensure_provider_authorized(
                &routed.authorization,
                &provider,
                &routed.request.model,
                routed.endpoint,
            )?;
            return provider
                .client
                .stream_chat_completion_with_timeout(&routed, request_timeout)
                .await;
        }
        if self
            .registry
            .blocked_configured_model(&request.request.model, &base_catalog)
            .await
        {
            return Err(AppError::bad_request(
                "configured provider model is not enabled",
            ));
        }
        self.base
            .stream_chat_completion_with_timeout(request, request_timeout)
            .await
    }

    async fn list_models(&self) -> AppResult<UpstreamModelsResponse> {
        let catalog = self.supported_model_catalog().await?;
        models_response(catalog)
    }

    async fn proxy_completions(
        &self,
        mut request: ProxyCompletionsRequest,
    ) -> AppResult<reqwest::Response> {
        let base_catalog = self.base_catalog_or_empty().await;
        let requested_model = proxy_body_model(&request.body).unwrap_or_default();
        if let Some(provider) = self
            .registry
            .route_for_model(&requested_model, &base_catalog)
            .await
        {
            ensure_provider_authorized(
                &request.authorization,
                &provider,
                &requested_model,
                InferenceEndpoint::Completions,
            )?;
            let resolved_model = resolve_provider_model(&provider.stored, &requested_model)
                .unwrap_or_else(|| requested_model.clone());
            request.body = proxy_body_with_model(request.body, &resolved_model)?;
            request.authorization_route = Some("configured".to_string());
            request.authorization_provider = Some(provider.stored.name.clone());
            return provider.client.proxy_completions(request).await;
        }
        if self
            .registry
            .blocked_configured_model(&requested_model, &base_catalog)
            .await
        {
            return Err(AppError::bad_request(
                "configured provider model is not enabled",
            ));
        }
        self.base.proxy_completions(request).await
    }

    async fn count_tokens(&self, request: &BackendChatRequest) -> AppResult<Option<u64>> {
        let base_catalog = self.base_catalog_or_empty().await;
        if let Some(provider) = self
            .registry
            .route_for_model(&request.request.model, &base_catalog)
            .await
        {
            let routed = self.routed_request(request, &provider);
            ensure_provider_authorized(
                &routed.authorization,
                &provider,
                &routed.request.model,
                routed.endpoint,
            )?;
            return provider.client.count_tokens(&routed).await;
        }
        if self
            .registry
            .blocked_configured_model(&request.request.model, &base_catalog)
            .await
        {
            return Err(AppError::bad_request(
                "configured provider model is not enabled",
            ));
        }
        self.base.count_tokens(request).await
    }

    async fn supported_model_catalog(&self) -> AppResult<Vec<UpstreamModelEntry>> {
        let mut catalog = self.base_catalog().await?;
        let mut seen = catalog
            .iter()
            .map(|entry| entry.id.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        let mut counts = HashMap::<String, usize>::new();
        self.registry.ensure_loaded().await?;
        let state = self.registry.state.read().await;
        for provider in &state.providers {
            for model in enabled_provider_models(&provider.stored) {
                *counts.entry(model.id.to_ascii_lowercase()).or_default() += 1;
            }
        }
        for provider in &state.providers {
            for model in enabled_provider_models(&provider.stored) {
                let key = model.id.to_ascii_lowercase();
                if counts.get(&key) == Some(&1) && seen.insert(key) {
                    catalog.push(model);
                }
            }
        }
        Ok(catalog)
    }

    async fn provider_inventory(&self) -> AppResult<Vec<ProviderInventoryEntry>> {
        let mut inventory = self.base.provider_inventory().await.unwrap_or_default();
        inventory.extend(self.registry.inventory().await?);
        Ok(inventory)
    }

    async fn backend_candidate_plan(&self, requested_model: &str) -> BackendCandidatePlan {
        let base_plan = self.base.backend_candidate_plan(requested_model).await;
        let base_catalog = self.base_catalog_or_empty().await;
        if let Some(provider) = self
            .registry
            .route_for_model(requested_model, &base_catalog)
            .await
            && let Some(model) = resolve_provider_model_entry(&provider.stored, requested_model)
                .filter(|entry| model_enabled(&provider.stored, &entry.id))
        {
            return BackendCandidatePlan {
                candidates: vec![BackendCandidate {
                    model: model.id.clone(),
                    context_limit: model.context_limit,
                }],
            };
        }
        if self
            .registry
            .blocked_configured_model(requested_model, &base_catalog)
            .await
        {
            return BackendCandidatePlan {
                candidates: Vec::new(),
            };
        }
        base_plan
    }

    fn provider_health(&self) -> Vec<crate::upstream::ProviderHealth> {
        self.base.provider_health()
    }

    fn provider_base_url(&self) -> String {
        self.base.provider_base_url()
    }

    fn set_reject_unknown_models(&self, reject: bool) {
        self.base.set_reject_unknown_models(reject);
    }

    fn model_catalog_cache_ttl(&self) -> Duration {
        Duration::from_secs(0)
    }
}

fn ensure_provider_authorized(
    authorization: &crate::upstream::AuthorizationScope,
    provider: &ManagedProvider,
    model: &str,
    endpoint: InferenceEndpoint,
) -> AppResult<()> {
    if authorization.allows_candidate(&provider.stored.name, Some("configured"), model, endpoint) {
        Ok(())
    } else {
        Err(AppError::forbidden(
            "the authenticated key is not authorized for this inference candidate",
        ))
    }
}

fn provider_view(provider: &ManagedProvider) -> ConfiguredProviderView {
    ConfiguredProviderView {
        id: provider.stored.id.clone(),
        name: provider.stored.name.clone(),
        base_url: provider.stored.base_url.clone(),
        api_key_present: !provider.stored.api_key.is_empty(),
        models: provider.stored.models.clone(),
        auto_discover: provider.stored.auto_discover,
        allowed_models: provider.stored.allowed_models.clone(),
        disabled_models: provider.stored.disabled_models.clone(),
    }
}

fn default_auto_discover() -> bool {
    true
}

fn provider_model_enabled(provider: &StoredProvider, model: &str) -> bool {
    resolve_provider_model_entry(provider, model)
        .is_some_and(|entry| model_enabled(provider, &entry.id))
}

fn enabled_configured_model_matches(providers: &[ManagedProvider], model: &str) -> bool {
    providers.iter().any(|provider| {
        resolve_provider_model_entry(&provider.stored, model)
            .is_some_and(|entry| model_enabled(&provider.stored, &entry.id))
    })
}

fn blocked_configured_model_matches(providers: &[ManagedProvider], model: &str) -> bool {
    providers.iter().any(|provider| {
        if let Some(entry) = resolve_provider_model_entry(&provider.stored, model) {
            return !model_enabled(&provider.stored, &entry.id);
        }
        provider
            .stored
            .models
            .iter()
            .any(|entry| canonical_model_key(&entry.id) == canonical_model_key(model))
    })
}

fn model_disabled(provider: &StoredProvider, model: &str) -> bool {
    provider
        .disabled_models
        .iter()
        .any(|disabled| disabled.eq_ignore_ascii_case(model))
}

fn model_allowed(provider: &StoredProvider, model: &str) -> bool {
    match &provider.allowed_models {
        Some(allowed) => allowed
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(model)),
        None => true,
    }
}

fn model_enabled(provider: &StoredProvider, model: &str) -> bool {
    model_allowed(provider, model) && !model_disabled(provider, model)
}

fn resolve_provider_model(provider: &StoredProvider, requested_model: &str) -> Option<String> {
    resolve_provider_model_entry(provider, requested_model)
        .filter(|entry| model_enabled(provider, &entry.id))
        .map(|entry| entry.id.clone())
}

fn resolve_provider_model_entry<'a>(
    provider: &'a StoredProvider,
    requested_model: &str,
) -> Option<&'a UpstreamModelEntry> {
    resolve_model_entry_in_catalog(&provider.models, requested_model)
}

fn resolve_model_entry_in_catalog<'a>(
    models: &'a [UpstreamModelEntry],
    requested_model: &str,
) -> Option<&'a UpstreamModelEntry> {
    if let Some(entry) = models
        .iter()
        .find(|entry| entry.id.eq_ignore_ascii_case(requested_model))
    {
        return Some(entry);
    }
    let key = canonical_model_key(requested_model);
    let mut matches = models
        .iter()
        .filter(|entry| canonical_model_key(&entry.id) == key);
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

fn enabled_provider_models(provider: &StoredProvider) -> Vec<UpstreamModelEntry> {
    provider
        .models
        .iter()
        .filter(|entry| model_enabled(provider, &entry.id))
        .cloned()
        .collect()
}

fn validate_model_list(
    models: &[UpstreamModelEntry],
    requested_models: Vec<String>,
    kind: &str,
) -> AppResult<Vec<String>> {
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    for raw in requested_models {
        let model = raw.trim();
        if model.is_empty() || model.chars().any(char::is_control) {
            return Err(AppError::bad_request(format!("{kind} model id is invalid")));
        }
        let canonical = canonical_model_in_catalog(models, model, kind)?;
        if seen.insert(canonical.to_ascii_lowercase()) {
            normalized.push(canonical);
        }
    }
    Ok(normalized)
}

fn canonical_model_in_catalog(
    models: &[UpstreamModelEntry],
    model: &str,
    kind: &str,
) -> AppResult<String> {
    if let Some(entry) = models
        .iter()
        .find(|entry| entry.id.eq_ignore_ascii_case(model))
    {
        return Ok(entry.id.clone());
    }
    let key = canonical_model_key(model);
    let mut matches = models
        .iter()
        .filter(|entry| canonical_model_key(&entry.id) == key);
    let Some(first) = matches.next() else {
        return Err(AppError::bad_request(format!(
            "{kind} model {model:?} is not in the configured provider catalog"
        )));
    };
    if matches.next().is_some() {
        return Err(AppError::bad_request(format!(
            "{kind} model {model:?} matches multiple configured provider catalog ids"
        )));
    }
    Ok(first.id.clone())
}

async fn discover_provider_models(
    client: &DynUpstreamClient,
) -> AppResult<Vec<UpstreamModelEntry>> {
    let mut models =
        tokio::time::timeout(MODEL_DISCOVERY_TIMEOUT, client.supported_model_catalog())
            .await
            .map_err(|_| AppError::upstream("provider model discovery timed out"))??;
    models.sort_by_key(|a| a.id.to_ascii_lowercase());
    models.dedup_by(|a, b| a.id.eq_ignore_ascii_case(&b.id));
    if models.is_empty() {
        return Err(AppError::bad_request(
            "provider returned no models from /v1/models",
        ));
    }
    if models.len() > MAX_PROVIDER_MODELS {
        return Err(AppError::bad_request(format!(
            "provider returned too many models; maximum is {MAX_PROVIDER_MODELS}"
        )));
    }
    Ok(models)
}

async fn refresh_configured_providers_loop(registry: Weak<ManagedProviderRegistry>) {
    refresh_configured_providers_loop_with_interval(
        registry,
        MODEL_DISCOVERY_REFRESH_INTERVAL,
        None,
    )
    .await;
}

async fn refresh_configured_providers_loop_with_interval(
    registry: Weak<ManagedProviderRegistry>,
    refresh_interval: Duration,
    started: Option<Arc<Notify>>,
) {
    let mut interval = tokio::time::interval(refresh_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    if let Some(started) = started {
        started.notify_waiters();
    }
    loop {
        interval.tick().await;
        let Some(registry) = registry.upgrade() else {
            break;
        };
        if let Err(err) = registry.refresh_configured_providers_once().await {
            tracing::warn!(error = %err, "failed to refresh configured provider catalogs");
        }
    }
}

fn validate_provider_name(name: &str) -> AppResult<String> {
    let name = name.trim();
    if name.is_empty() || name.len() > 64 {
        return Err(AppError::bad_request(
            "provider name must be 1-64 characters",
        ));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ' ' | '.'))
    {
        return Err(AppError::bad_request(
            "provider name may contain letters, numbers, spaces, '.', '_' and '-'",
        ));
    }
    Ok(name.to_string())
}

fn normalize_provider_url(input: &str) -> AppResult<Url> {
    let mut url = Url::parse(input.trim())
        .map_err(|err| AppError::bad_request(format!("invalid provider URL: {err}")))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(AppError::bad_request(
            "provider URL must not contain credentials",
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(AppError::bad_request(
            "provider URL must not contain query or fragment",
        ));
    }
    match url.scheme() {
        "https" => {}
        "http" if is_loopback_url(&url) => {}
        _ => {
            return Err(AppError::bad_request(
                "provider URL must use https, except http is allowed for loopback hosts",
            ));
        }
    }
    let mut segments = url
        .path_segments()
        .map(|parts| parts.filter(|part| !part.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();
    if segments.is_empty() {
        segments.push("v1");
    }
    let mut path = format!("/{}", segments.join("/"));
    if !path.ends_with('/') {
        path.push('/');
    }
    url.set_path(&path);
    Ok(url)
}

fn is_loopback_url(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>().is_ok_and(|addr| addr.is_loopback())
}

fn models_response(catalog: Vec<UpstreamModelEntry>) -> AppResult<UpstreamModelsResponse> {
    let data = catalog
        .into_iter()
        .map(|entry| {
            let mut value = serde_json::json!({
                "id": entry.id,
                "object": "model",
            });
            if let Some(context_limit) = entry.context_limit
                && let Some(object) = value.as_object_mut()
            {
                object.insert(
                    "context_length".to_string(),
                    Value::Number(context_limit.into()),
                );
            }
            value
        })
        .collect::<Vec<_>>();
    let body = serde_json::to_vec(&serde_json::json!({
        "object": "list",
        "data": data,
    }))
    .map_err(|err| AppError::internal(format!("failed to serialize configured models: {err}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/json".parse().expect("static header"),
    );
    Ok(UpstreamModelsResponse {
        status: StatusCode::OK,
        headers,
        body: Bytes::from(body),
    })
}

fn proxy_body_model(body: &Bytes) -> Option<String> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn proxy_body_with_model(body: Bytes, model: &str) -> AppResult<Bytes> {
    let mut value = serde_json::from_slice::<Value>(&body)
        .map_err(|err| AppError::bad_request(format!("invalid completions JSON body: {err}")))?;
    if let Some(object) = value.as_object_mut() {
        object.insert("model".to_string(), Value::String(model.to_string()));
    }
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|err| AppError::internal(format!("failed to serialize completions body: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_store::SqlStore;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Clone)]
    struct CatalogOnlyUpstream {
        models: Vec<UpstreamModelEntry>,
    }

    #[async_trait]
    impl UpstreamClient for CatalogOnlyUpstream {
        async fn stream_chat_completion(
            &self,
            _request: &BackendChatRequest,
        ) -> AppResult<crate::upstream::UpstreamStream> {
            Err(AppError::internal("not implemented"))
        }

        async fn list_models(&self) -> AppResult<UpstreamModelsResponse> {
            models_response(self.models.clone())
        }

        async fn supported_model_catalog(&self) -> AppResult<Vec<UpstreamModelEntry>> {
            Ok(self.models.clone())
        }
    }

    #[derive(Clone)]
    struct MutableCatalogUpstream {
        models: Arc<RwLock<Result<Vec<UpstreamModelEntry>, String>>>,
        queries: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MutableCatalogUpstream {
        fn new(models: Vec<&str>) -> Self {
            Self {
                models: Arc::new(RwLock::new(Ok(model_entries(models)))),
                queries: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        async fn set_models(&self, models: Vec<&str>) {
            *self.models.write().await = Ok(model_entries(models));
        }

        async fn fail(&self) {
            *self.models.write().await = Err("catalog unavailable".to_string());
        }

        fn queries(&self) -> usize {
            self.queries.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl UpstreamClient for MutableCatalogUpstream {
        async fn stream_chat_completion(
            &self,
            _request: &BackendChatRequest,
        ) -> AppResult<crate::upstream::UpstreamStream> {
            Err(AppError::internal("not implemented"))
        }

        async fn list_models(&self) -> AppResult<UpstreamModelsResponse> {
            models_response(self.supported_model_catalog().await?)
        }

        async fn supported_model_catalog(&self) -> AppResult<Vec<UpstreamModelEntry>> {
            self.queries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.models.read().await.clone().map_err(AppError::upstream)
        }
    }

    fn model_entries(models: Vec<&str>) -> Vec<UpstreamModelEntry> {
        models
            .into_iter()
            .map(|id| UpstreamModelEntry {
                id: id.to_string(),
                context_limit: None,
            })
            .collect()
    }

    fn test_options() -> ManagedProviderOptions {
        ManagedProviderOptions {
            http_client: reqwest::Client::new(),
            flatten_content: true,
            min_completion_tokens: 1,
            max_sse_frame_bytes: 1024,
            finalization_policies: crate::upstream::BackendFinalizationPolicies::default(),
            flow_store: DashboardFlowStore::disabled(),
        }
    }

    async fn test_registry() -> Arc<ManagedProviderRegistry> {
        let store = Arc::new(
            SqlStore::connect_sqlite("sqlite::memory:")
                .await
                .expect("sqlite store"),
        );
        test_registry_with_store(store).await
    }

    async fn test_registry_with_store(
        store: Arc<dyn PersistenceStore>,
    ) -> Arc<ManagedProviderRegistry> {
        ManagedProviderRegistry::new(store, test_options())
    }

    async fn bare_test_registry() -> Arc<ManagedProviderRegistry> {
        let store = Arc::new(
            SqlStore::connect_sqlite("sqlite::memory:")
                .await
                .expect("sqlite store"),
        );
        Arc::new(ManagedProviderRegistry {
            store,
            options: test_options(),
            state: RwLock::new(ManagedProviderState::default()),
            vision_changes: tokio::sync::watch::channel(0).0,
        })
    }

    #[test]
    fn normalizes_loopback_http_and_adds_v1() {
        let url = normalize_provider_url("http://127.0.0.1:8080").expect("valid");
        assert_eq!(url.as_str(), "http://127.0.0.1:8080/v1/");
    }

    #[test]
    fn rejects_non_loopback_http_and_url_credentials() {
        assert!(normalize_provider_url("http://example.com/v1").is_err());
        assert!(normalize_provider_url("https://user:pass@example.com/v1").is_err());
    }

    #[test]
    fn provider_views_redact_api_key() {
        let provider = ManagedProvider {
            stored: StoredProvider {
                id: "cfg_1".to_string(),
                name: "external".to_string(),
                base_url: "https://api.example.com/v1/".to_string(),
                api_key: "secret".to_string(),
                models: vec![UpstreamModelEntry {
                    id: "model-a".to_string(),
                    context_limit: Some(4096),
                }],
                auto_discover: true,
                allowed_models: None,
                disabled_models: Vec::new(),
            },
            client: Arc::new(ReqwestUpstreamClient::with_options(
                reqwest::Client::new(),
                Url::parse("https://api.example.com/v1/").unwrap(),
                Some("secret".to_string()),
                None,
                true,
                1,
                1024,
            )),
            revision: 0,
        };
        let json = serde_json::to_string(&provider_view(&provider)).expect("serialize");
        assert!(json.contains("\"api_key_present\":true"));
        assert!(json.contains("\"auto_discover\":true"));
        assert!(json.contains("\"disabled_models\":[]"));
        assert!(!json.contains("secret"));
    }

    #[test]
    fn old_stored_providers_default_to_no_allowlist() {
        let stored: StoredProviders = serde_json::from_str(
            r#"{"providers":[{"id":"cfg_old","name":"old","base_url":"https://old.example/v1/","api_key":"secret","models":[{"id":"model-a"}],"auto_discover":true,"disabled_models":[]}]}"#,
        )
        .expect("old stored provider JSON");
        assert_eq!(stored.providers.len(), 1);
        assert_eq!(stored.providers[0].allowed_models, None);
    }

    #[test]
    fn update_provider_allowlist_distinguishes_omitted_null_and_array() {
        let omitted: UpdateConfiguredProviderRequest =
            serde_json::from_str("{}").expect("omitted allowlist");
        assert_eq!(omitted.allowed_models, ModelListUpdate::Omitted);

        let clear: UpdateConfiguredProviderRequest =
            serde_json::from_str(r#"{"allowed_models":null}"#).expect("null allowlist");
        assert_eq!(clear.allowed_models, ModelListUpdate::Clear);

        let replace: UpdateConfiguredProviderRequest =
            serde_json::from_str(r#"{"allowed_models":["Model-A"]}"#).expect("array allowlist");
        assert_eq!(
            replace.allowed_models,
            ModelListUpdate::Replace(vec!["Model-A".to_string()])
        );
    }

    #[test]
    fn configured_provider_routing_honors_authorization_scope() {
        let provider = ManagedProvider {
            stored: StoredProvider {
                id: "cfg_1".to_string(),
                name: "external".to_string(),
                base_url: "https://api.example.com/v1/".to_string(),
                api_key: "secret".to_string(),
                models: vec![UpstreamModelEntry {
                    id: "model-a".to_string(),
                    context_limit: None,
                }],
                auto_discover: true,
                allowed_models: None,
                disabled_models: Vec::new(),
            },
            client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
            revision: 0,
        };
        let denied = crate::upstream::AuthorizationScope::restricted(
            |_provider, _route, _model, _endpoint| false,
        );
        let allowed =
            crate::upstream::AuthorizationScope::restricted(|provider, route, model, endpoint| {
                provider == "external"
                    && route == Some("configured")
                    && model == "model-a"
                    && endpoint == InferenceEndpoint::ChatCompletions
            });

        assert!(
            ensure_provider_authorized(
                &denied,
                &provider,
                "model-a",
                InferenceEndpoint::ChatCompletions,
            )
            .is_err()
        );
        assert!(
            ensure_provider_authorized(
                &allowed,
                &provider,
                "model-a",
                InferenceEndpoint::ChatCompletions,
            )
            .is_ok()
        );
    }

    #[test]
    fn provider_model_resolution_prefers_exact_ids_and_rejects_ambiguous_aliases() {
        let mut provider = StoredProvider {
            id: "cfg_collision".to_string(),
            name: "collision".to_string(),
            base_url: "https://collision.example/v1/".to_string(),
            api_key: "secret".to_string(),
            models: model_entries(vec!["foo-bar", "foo_bar", "unique-model"]),
            auto_discover: true,
            allowed_models: None,
            disabled_models: Vec::new(),
        };

        assert_eq!(
            resolve_provider_model(&provider, "FOO_BAR").as_deref(),
            Some("foo_bar"),
            "exact case-insensitive ids win before normalized aliases"
        );
        assert_eq!(
            resolve_provider_model(&provider, "unique.model").as_deref(),
            Some("unique-model"),
            "unique normalized aliases remain accepted"
        );
        assert_eq!(
            resolve_provider_model(&provider, "foobar"),
            None,
            "ambiguous normalized aliases are not routed to an arbitrary id"
        );
        provider.allowed_models = Some(vec!["foo-bar".to_string()]);
        assert!(
            !provider_model_enabled(&provider, "foo_bar"),
            "persisted allowlist membership is exact and does not enable a colliding id"
        );
        provider.allowed_models = None;
        provider.disabled_models = vec!["foo-bar".to_string()];
        assert!(
            provider_model_enabled(&provider, "foo_bar"),
            "persisted disabled-model membership is exact and does not disable a colliding id"
        );
        assert!(blocked_configured_model_matches(
            &[ManagedProvider {
                stored: provider,
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 0,
            }],
            "foobar",
        ));
    }

    #[tokio::test]
    async fn dashboard_vision_detection_tracks_add_filter_and_delete() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .and(header("authorization", "Bearer secret-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {"id": "vision-a", "architecture": {"input_modalities": ["text", "image"]}},
                    {"id": "text-b", "architecture": {"input_modalities": ["text"]}}
                ]
            })))
            .mount(&server)
            .await;
        let registry = bare_test_registry().await;
        let cache = crate::vision_probe::NativeVisionCache::default();
        let task = crate::vision_probe::spawn_with_managed(
            &crate::vision_probe::VisionProbeBootstrap::default(),
            Vec::new(),
            cache.clone(),
            Some(registry.clone()),
        )
        .expect("dynamic prober starts even with an empty registry");
        let added = registry
            .add(CreateConfiguredProviderRequest {
                name: "external".into(),
                base_url: server.uri(),
                api_key: "secret-key".into(),
            })
            .await
            .expect("add");
        async fn wait_for(
            cache: &crate::vision_probe::NativeVisionCache,
            model: &str,
            expected: Option<bool>,
        ) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while cache.lookup(model) != expected {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("vision cache updated after provider change");
        }
        wait_for(&cache, "vision-a", Some(true)).await;
        wait_for(&cache, "text-b", Some(false)).await;
        registry
            .update(
                &added.id,
                UpdateConfiguredProviderRequest {
                    auto_discover: None,
                    allowed_models: ModelListUpdate::Replace(vec!["vision-a".into()]),
                    disabled_models: None,
                },
            )
            .await
            .expect("filter");
        wait_for(&cache, "text-b", None).await;
        assert_eq!(
            registry
                .vision_probe_targets()
                .await
                .expect("targets")
                .len(),
            1
        );
        registry
            .update(
                &added.id,
                UpdateConfiguredProviderRequest {
                    auto_discover: None,
                    allowed_models: ModelListUpdate::Clear,
                    disabled_models: None,
                },
            )
            .await
            .expect("clear filter");
        wait_for(&cache, "text-b", Some(false)).await;
        registry.delete(&added.id).await.expect("delete");
        wait_for(&cache, "vision-a", None).await;
        wait_for(&cache, "text-b", None).await;
        task.abort();
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .iter()
                .all(|request| request.method == "GET"),
            "metadata prevents inference probes"
        );
    }

    #[tokio::test]
    async fn add_discovers_models_persists_and_redacts_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .and(header("authorization", "Bearer secret-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "object": "list",
                "data": [
                    {"id": "external-a", "object": "model", "context_length": 8192}
                ]
            })))
            .mount(&server)
            .await;
        let registry = test_registry().await;
        let added = registry
            .add(CreateConfiguredProviderRequest {
                name: "external".to_string(),
                base_url: server.uri(),
                api_key: "secret-key".to_string(),
            })
            .await
            .expect("provider added");
        assert_eq!(added.name, "external");
        assert_eq!(added.models[0].id, "external-a");
        let json = serde_json::to_string(&ConfiguredProvidersBody {
            providers: registry.list().await.expect("list"),
        })
        .expect("serialize");
        assert!(json.contains("external-a"));
        assert!(!json.contains("secret-key"));
    }

    #[tokio::test]
    async fn catalog_union_preserves_base_precedence_and_routes_unique_dynamic_models() {
        let registry = test_registry().await;
        {
            let dynamic = StoredProvider {
                id: "cfg_dynamic".to_string(),
                name: "dynamic".to_string(),
                base_url: "https://dynamic.example/v1/".to_string(),
                api_key: "secret".to_string(),
                models: vec![
                    UpstreamModelEntry {
                        id: "base-model".to_string(),
                        context_limit: Some(123),
                    },
                    UpstreamModelEntry {
                        id: "dynamic-model".to_string(),
                        context_limit: Some(456),
                    },
                ],
                auto_discover: true,
                allowed_models: None,
                disabled_models: Vec::new(),
            };
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: dynamic,
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }
        let upstream = ManagedProviderUpstream::new(
            Arc::new(CatalogOnlyUpstream {
                models: vec![UpstreamModelEntry {
                    id: "base-model".to_string(),
                    context_limit: Some(999),
                }],
            }),
            registry,
        );

        let catalog = upstream.supported_model_catalog().await.expect("catalog");
        assert_eq!(
            catalog
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            vec!["base-model", "dynamic-model"]
        );
        assert_eq!(
            catalog
                .iter()
                .find(|entry| entry.id == "base-model")
                .and_then(|entry| entry.context_limit),
            Some(999),
            "base catalog entry wins on collision"
        );

        let dynamic_plan = upstream.backend_candidate_plan("dynamic-model").await;
        assert_eq!(dynamic_plan.candidates.len(), 1);
        assert_eq!(dynamic_plan.candidates[0].model, "dynamic-model");
        assert_eq!(dynamic_plan.candidates[0].context_limit, Some(456));

        let base_plan = upstream.backend_candidate_plan("base-model").await;
        assert_eq!(base_plan.candidates[0].model, "base-model");
        assert_eq!(base_plan.candidates[0].context_limit, None);
    }

    #[tokio::test]
    async fn refresh_additively_merges_models_and_preserves_missing_disabled_models() {
        let registry = bare_test_registry().await;
        let upstream = MutableCatalogUpstream::new(vec!["model-a"]);
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_refresh".to_string(),
                    name: "refresh".to_string(),
                    base_url: "https://refresh.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["model-a", "stale-model"]),
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: vec!["stale-model".to_string()],
                },
                client: Arc::new(upstream.clone()),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }

        let changes = registry.vision_probe_changes();
        upstream.set_models(vec!["model-a", "model-b"]).await;
        registry
            .refresh_configured_providers_once()
            .await
            .expect("refresh");

        let provider = registry.list().await.expect("list").remove(0);
        assert_eq!(
            provider
                .models
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            vec!["model-a", "model-b", "stale-model"],
            "refresh adds new ids and retains models missing upstream"
        );
        assert_eq!(provider.disabled_models, vec!["stale-model"]);
        assert!(changes.has_changed().expect("watch open"));
        assert_eq!(
            registry
                .vision_probe_targets()
                .await
                .expect("targets")
                .iter()
                .map(|target| target.model.as_str())
                .collect::<Vec<_>>(),
            vec!["model-a", "model-b"],
            "newly discovered enabled models become probe targets"
        );
    }

    #[tokio::test]
    async fn refresh_failure_preserves_catalog_for_retry() {
        let registry = test_registry().await;
        let upstream = MutableCatalogUpstream::new(vec!["model-a"]);
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_failure".to_string(),
                    name: "failure".to_string(),
                    base_url: "https://failure.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["model-a"]),
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: Vec::new(),
                },
                client: Arc::new(upstream.clone()),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }

        upstream.fail().await;
        registry
            .refresh_configured_providers_once()
            .await
            .expect("provider failure is non-fatal");
        assert_eq!(registry.list().await.expect("list")[0].models.len(), 1);

        upstream.set_models(vec!["model-a", "model-b"]).await;
        registry
            .refresh_configured_providers_once()
            .await
            .expect("retry refresh");
        assert!(
            registry.list().await.expect("list")[0]
                .models
                .iter()
                .any(|entry| entry.id == "model-b"),
            "failed refresh did not poison the later additive retry"
        );
    }

    #[tokio::test]
    async fn model_selection_survives_refresh_and_restart_and_is_excluded_from_serving() {
        let store = Arc::new(
            SqlStore::connect_sqlite("sqlite::memory:")
                .await
                .expect("sqlite store"),
        );
        let registry = test_registry_with_store(store.clone()).await;
        let upstream = MutableCatalogUpstream::new(vec!["model-a", "model-b"]);
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_disabled".to_string(),
                    name: "disabled".to_string(),
                    base_url: "https://disabled.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["model-a", "model-b"]),
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: Vec::new(),
                },
                client: Arc::new(upstream.clone()),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }
        registry
            .update(
                "cfg_disabled",
                UpdateConfiguredProviderRequest {
                    auto_discover: None,
                    allowed_models: ModelListUpdate::Replace(vec!["MODEL-A".to_string()]),
                    disabled_models: Some(vec!["MODEL-B".to_string()]),
                },
            )
            .await
            .expect("update")
            .expect("found");
        upstream
            .set_models(vec!["model-a", "model-b", "model-c"])
            .await;
        registry
            .refresh_configured_providers_once()
            .await
            .expect("refresh");

        let restarted = test_registry_with_store(store).await;
        restarted.ensure_loaded().await.expect("loaded");
        let provider = restarted.list().await.expect("list").remove(0);
        assert_eq!(provider.allowed_models, Some(vec!["model-a".to_string()]));
        assert_eq!(provider.disabled_models, vec!["model-b"]);
        assert!(
            provider.models.iter().any(|entry| entry.id == "model-b"),
            "list view still includes disabled models"
        );

        let managed = ManagedProviderUpstream::new(
            Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
            restarted,
        );
        let catalog = managed.supported_model_catalog().await.expect("catalog");
        assert!(
            catalog.iter().all(|entry| entry.id != "model-b"),
            "serving catalog excludes disabled ids"
        );
        assert!(
            catalog.iter().all(|entry| entry.id != "model-c"),
            "serving catalog excludes ids outside the allowlist"
        );
        assert!(
            managed
                .backend_candidate_plan("model-b")
                .await
                .candidates
                .is_empty(),
            "candidate inventory excludes disabled ids"
        );
        assert!(
            managed
                .backend_candidate_plan("model-c")
                .await
                .candidates
                .is_empty(),
            "candidate inventory excludes non-allowlisted ids"
        );
    }

    #[tokio::test]
    async fn null_allowlist_clears_but_omitted_allowlist_preserves_previous_value() {
        let registry = test_registry().await;
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_allow".to_string(),
                    name: "allow".to_string(),
                    base_url: "https://allow.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["model-a", "model-b"]),
                    auto_discover: true,
                    allowed_models: Some(vec!["model-a".to_string()]),
                    disabled_models: Vec::new(),
                },
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }

        registry
            .update(
                "cfg_allow",
                UpdateConfiguredProviderRequest {
                    auto_discover: Some(false),
                    allowed_models: ModelListUpdate::Omitted,
                    disabled_models: None,
                },
            )
            .await
            .expect("omitted update")
            .expect("found");
        assert_eq!(
            registry.list().await.expect("list")[0].allowed_models,
            Some(vec!["model-a".to_string()])
        );

        registry
            .update(
                "cfg_allow",
                UpdateConfiguredProviderRequest {
                    auto_discover: None,
                    allowed_models: ModelListUpdate::Clear,
                    disabled_models: None,
                },
            )
            .await
            .expect("clear update")
            .expect("found");
        assert_eq!(registry.list().await.expect("list")[0].allowed_models, None);
    }

    #[tokio::test]
    async fn disabled_configured_only_model_does_not_fall_back_to_default() {
        let registry = test_registry().await;
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_disabled_route".to_string(),
                    name: "disabled-route".to_string(),
                    base_url: "https://disabled-route.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["disabled-model"]),
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: vec!["disabled-model".to_string()],
                },
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }
        let upstream = ManagedProviderUpstream::new(
            Arc::new(CatalogOnlyUpstream {
                models: model_entries(vec!["base-default"]),
            }),
            registry,
        );
        let request = BackendChatRequest::new(
            crate::models::chat::ChatCompletionRequest {
                model: "DISABLED-MODEL".to_string(),
                messages: Vec::new(),
                stream: true,
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                reasoning_effort: None,
                response_format: None,
                stream_options: None,
                temperature: None,
                top_p: None,
                max_output_tokens: None,
                frequency_penalty: None,
                presence_penalty: None,
                stop: None,
                extra_body: std::collections::BTreeMap::new(),
            },
            None,
            None,
            None,
        );
        let err = match upstream.stream_chat_completion(&request).await {
            Ok(_) => panic!("disabled configured model was not rejected"),
            Err(err) => err,
        };
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn disabled_configured_model_does_not_block_real_base_catalog_match() {
        let registry = test_registry().await;
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_disabled_base".to_string(),
                    name: "disabled-base".to_string(),
                    base_url: "https://disabled-base.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["model-a", "configured-only"]),
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: vec!["model-a".to_string(), "configured-only".to_string()],
                },
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }

        assert!(
            !registry
                .blocked_configured_model("model-a", &model_entries(vec!["model-a"]))
                .await,
            "a concrete base catalog match remains routable"
        );
        assert!(
            registry
                .blocked_configured_model("configured-only", &model_entries(vec!["model-a"]))
                .await,
            "configured-only disabled ids still cannot fall through"
        );
    }

    #[tokio::test]
    async fn exact_configured_id_is_not_stolen_by_canonical_base_collision() {
        let registry = test_registry().await;
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_collision_route".to_string(),
                    name: "collision-route".to_string(),
                    base_url: "https://collision-route.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["foo-bar"]),
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: Vec::new(),
                },
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }

        assert!(
            registry
                .route_for_model("foo-bar", &model_entries(vec!["foo_bar"]))
                .await
                .is_some(),
            "exact configured id wins over a base model that only collides canonically"
        );
        assert!(
            registry
                .route_for_model("foo_bar", &model_entries(vec!["foo_bar"]))
                .await
                .is_none(),
            "exact base id still keeps base precedence"
        );
    }

    #[tokio::test]
    async fn auto_discover_toggle_racing_refresh_prevents_stale_merge() {
        let registry = test_registry().await;
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_race".to_string(),
                    name: "race".to_string(),
                    base_url: "https://race.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["model-a"]),
                    auto_discover: false,
                    allowed_models: None,
                    disabled_models: Vec::new(),
                },
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 1,
            }];
            registry.state.write().await.loaded = true;
        }
        let stale_snapshot = ProviderRefreshSnapshot {
            id: "cfg_race".to_string(),
            revision: 0,
            client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
        };
        registry
            .merge_refreshed_models(&stale_snapshot, model_entries(vec!["model-b"]))
            .await
            .expect("stale merge ignored");
        assert_eq!(
            registry.list().await.expect("list")[0]
                .models
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            vec!["model-a"],
            "stale refresh cannot mutate after admin disabled discovery"
        );
    }

    #[tokio::test]
    async fn invalid_update_rolls_back_auto_discover_change() {
        let registry = test_registry().await;
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_update".to_string(),
                    name: "update".to_string(),
                    base_url: "https://update.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["known-model"]),
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: Vec::new(),
                },
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }

        let err = registry
            .update(
                "cfg_update",
                UpdateConfiguredProviderRequest {
                    auto_discover: Some(false),
                    allowed_models: ModelListUpdate::Omitted,
                    disabled_models: Some(vec!["unknown-model".to_string()]),
                },
            )
            .await
            .expect_err("unknown disabled model rejected");
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
        let provider = registry.list().await.expect("list").remove(0);
        assert!(
            provider.auto_discover,
            "invalid disabled-model replacement must not partially apply auto_discover"
        );
        assert!(provider.disabled_models.is_empty());
    }

    #[tokio::test]
    async fn refresh_capacity_failure_rolls_back_in_memory_catalog_additions() {
        let registry = test_registry().await;
        let near_full_catalog = (0..MAX_PROVIDER_MODELS - 1)
            .map(|index| UpstreamModelEntry {
                id: format!("model-{index}"),
                context_limit: None,
            })
            .collect::<Vec<_>>();
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_full".to_string(),
                    name: "full".to_string(),
                    base_url: "https://full.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: near_full_catalog,
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: Vec::new(),
                },
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }
        let snapshot = ProviderRefreshSnapshot {
            id: "cfg_full".to_string(),
            revision: 0,
            client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
        };

        let err = registry
            .merge_refreshed_models(
                &snapshot,
                model_entries(vec!["fits-before-overflow", "overflow"]),
            )
            .await
            .expect_err("aggregate catalog cap rejected");
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
        let provider = registry.list().await.expect("list").remove(0);
        assert_eq!(provider.models.len(), MAX_PROVIDER_MODELS - 1);
        assert!(
            provider
                .models
                .iter()
                .all(|entry| entry.id != "fits-before-overflow"),
            "near-capacity refresh is staged atomically"
        );
        assert!(provider.models.iter().all(|entry| entry.id != "overflow"));
    }

    #[tokio::test]
    async fn refresh_persist_failure_rolls_back_and_retry_persists_catalog() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-managed-refresh-{}.sqlite",
            Uuid::new_v4().simple()
        ));
        let url = format!("sqlite://{}", path.display());
        let store = Arc::new(SqlStore::connect_sqlite(&url).await.expect("sqlite store"));
        let registry = test_registry_with_store(store.clone()).await;
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_persist".to_string(),
                    name: "persist".to_string(),
                    base_url: "https://persist.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["model-a"]),
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: Vec::new(),
                },
                client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }

        let connection = rusqlite::Connection::open(&path).expect("open sqlite directly");
        connection
            .execute("DROP TABLE settings", [])
            .expect("drop settings");
        let snapshot = ProviderRefreshSnapshot {
            id: "cfg_persist".to_string(),
            revision: 0,
            client: Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
        };
        let err = registry
            .merge_refreshed_models(&snapshot, model_entries(vec!["model-b"]))
            .await
            .expect_err("persist failure returned");
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            registry.list().await.expect("list")[0]
                .models
                .iter()
                .all(|entry| entry.id != "model-b"),
            "failed persistence rolls in-memory merge back"
        );

        connection
            .execute(
                "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
                [],
            )
            .expect("restore settings table");
        registry
            .merge_refreshed_models(&snapshot, model_entries(vec!["model-b"]))
            .await
            .expect("retry persists");

        let restarted = test_registry_with_store(store).await;
        restarted.ensure_loaded().await.expect("reloaded");
        assert!(
            restarted.list().await.expect("list")[0]
                .models
                .iter()
                .any(|entry| entry.id == "model-b"),
            "retried refresh persisted the added model"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn periodic_refresh_waits_full_interval_and_skips_initial_burst() {
        let registry = bare_test_registry().await;
        let upstream = MutableCatalogUpstream::new(vec!["model-a", "model-b"]);
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_periodic".to_string(),
                    name: "periodic".to_string(),
                    base_url: "https://periodic.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["model-a"]),
                    auto_discover: true,
                    allowed_models: None,
                    disabled_models: Vec::new(),
                },
                client: Arc::new(upstream.clone()),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }

        tokio::time::pause();
        let refresh_interval = MODEL_DISCOVERY_REFRESH_INTERVAL;
        let mut changes = registry.vision_probe_changes();
        let started = Arc::new(Notify::new());
        let task = tokio::spawn(refresh_configured_providers_loop_with_interval(
            Arc::downgrade(&registry),
            refresh_interval,
            Some(Arc::clone(&started)),
        ));
        started.notified().await;
        assert_eq!(upstream.queries(), 0, "loop does not refresh immediately");

        tokio::time::advance(refresh_interval - Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            upstream.queries(),
            0,
            "loop waits the full refresh interval"
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(upstream.queries(), 1, "one refresh runs after the interval");
        // Query start is not commit completion: wait for the persisted-change
        // signal instead of racing the SQL worker with a fixed yield count.
        changes.changed().await.expect("refresh committed");
        assert!(
            registry.list().await.expect("list")[0]
                .models
                .iter()
                .any(|entry| entry.id == "model-b"),
            "refresh commit is visible after the interval tick"
        );

        drop(registry);
        tokio::time::advance(refresh_interval).await;
        task.await
            .expect("weak loop exits when registry is dropped");
    }
    #[tokio::test]
    async fn allowed_models_restrict_catalog_after_refresh_until_cleared() {
        let registry = test_registry().await;
        let upstream = MutableCatalogUpstream::new(vec!["model-a"]);
        {
            registry.state.write().await.providers = vec![ManagedProvider {
                stored: StoredProvider {
                    id: "cfg_allowed".to_string(),
                    name: "allowed".to_string(),
                    base_url: "https://allowed.example/v1/".to_string(),
                    api_key: "secret".to_string(),
                    models: model_entries(vec!["model-a"]),
                    auto_discover: true,
                    allowed_models: Some(vec!["model-a".to_string()]),
                    disabled_models: Vec::new(),
                },
                client: Arc::new(upstream.clone()),
                revision: 0,
            }];
            registry.state.write().await.loaded = true;
        }
        upstream.set_models(vec!["model-a", "model-b"]).await;
        registry
            .refresh_configured_providers_once()
            .await
            .expect("refresh");

        let managed = ManagedProviderUpstream::new(
            Arc::new(CatalogOnlyUpstream { models: Vec::new() }),
            registry.clone(),
        );
        let catalog = managed.supported_model_catalog().await.expect("catalog");
        assert_eq!(
            catalog
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            vec!["model-a"],
            "strict allowlists keep newly discovered models visible but unrouted"
        );
        assert!(
            managed
                .backend_candidate_plan("model-b")
                .await
                .candidates
                .is_empty()
        );

        registry
            .update(
                "cfg_allowed",
                UpdateConfiguredProviderRequest {
                    auto_discover: None,
                    allowed_models: ModelListUpdate::Clear,
                    disabled_models: None,
                },
            )
            .await
            .expect("update")
            .expect("found");
        let catalog = managed.supported_model_catalog().await.expect("catalog");
        assert!(
            catalog.iter().any(|entry| entry.id == "model-b"),
            "JSON null clears strict allowlist mode"
        );
    }
}
