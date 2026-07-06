use crate::error::AppError;
use crate::error::AppResult;
use crate::models::chat::ChatCompletionChunk;
use crate::models::chat::ChatCompletionRequest;
use async_trait::async_trait;
use axum::body::Bytes;
use eventsource_stream::Eventsource;
use futures::Stream;
use futures::StreamExt;
use http::HeaderMap;
use http::HeaderName;
use reqwest::RequestBuilder;
use reqwest::StatusCode;
use serde_json::Map as JsonMap;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex as AsyncMutex;
use url::Url;

pub type UpstreamStream =
    Pin<Box<dyn Stream<Item = Result<ChatCompletionChunk, AppError>> + Send + 'static>>;

const ROUTING_MODEL_CATALOG_TTL_SECS: u64 = 300;

#[async_trait]
pub trait UpstreamClient: Send + Sync {
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> AppResult<UpstreamStream>;
    async fn stream_chat_completion_with_timeout(
        &self,
        request: &ChatCompletionRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        let stream = self.stream_chat_completion(request).await?;
        Ok(timeout_upstream_stream(stream, request_timeout))
    }
    async fn list_models(&self) -> AppResult<reqwest::Response>;
    async fn proxy_completions(
        &self,
        _headers: HeaderMap,
        _body: Bytes,
    ) -> AppResult<reqwest::Response> {
        Err(AppError::internal(
            "upstream completions proxy is not implemented",
        ))
    }
    async fn count_tokens(&self, _body: &serde_json::Value) -> AppResult<Option<u64>> {
        Ok(None)
    }
    async fn supported_model_ids(&self) -> AppResult<Vec<String>>;

    /// Every backend model `requested_model` could ACTUALLY be served by
    /// pre-first-chunk, after any routing/route/exposed-alias + per-provider
    /// `upstream_model` rewrite (G4 review #2 + round-2 #1). This enumerates the
    /// full candidate set — the primary AND all eligible failover providers (and
    /// the routing target) — because failover happens before the first chunk, so
    /// the model that ultimately serves may be any of them. Native-vision gating
    /// uses the SAFE invariant over this set (passthrough only if EVERY candidate
    /// is native-vision), so it can never disagree with the provider that serves.
    ///
    /// The default impl (single `ReqwestUpstreamClient`) sends the request model
    /// unchanged, so the only candidate is `requested_model` itself.
    async fn candidate_backend_models(&self, requested_model: &str) -> Vec<String> {
        vec![requested_model.to_string()]
    }
}

#[derive(Debug, Clone)]
pub struct ReqwestUpstreamClient {
    client: reqwest::Client,
    base_url: Url,
    api_key: Option<String>,
    request_logger: Option<UpstreamRequestLogger>,
    flatten_content: bool,
}

#[derive(Debug, Clone)]
pub struct FailoverUpstreamProvider {
    name: String,
    client: ReqwestUpstreamClient,
    upstream_model: Option<String>,
    exposed_model: Option<String>,
    upstream_chat_kwargs: JsonMap<String, Value>,
}

impl FailoverUpstreamProvider {
    pub fn new(
        name: impl Into<String>,
        client: ReqwestUpstreamClient,
        upstream_model: Option<String>,
        exposed_model: Option<String>,
        upstream_chat_kwargs: JsonMap<String, Value>,
    ) -> Self {
        Self {
            name: name.into(),
            client,
            upstream_model,
            exposed_model,
            upstream_chat_kwargs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FailoverUpstreamClient {
    providers: Vec<FailoverUpstreamProvider>,
    cooldown: Duration,
    states: Arc<Mutex<Vec<ProviderCooldownState>>>,
}

#[derive(Debug, Clone)]
pub struct RoutingUpstreamProvider {
    name: String,
    primary_client: ReqwestUpstreamClient,
    primary_upstream_model: Option<String>,
    fallback_exposed_models: Vec<RoutingFallbackExposedModel>,
    client: FailoverUpstreamClient,
}

impl RoutingUpstreamProvider {
    pub fn new(
        name: impl Into<String>,
        primary_client: ReqwestUpstreamClient,
        primary_upstream_model: Option<String>,
        primary_upstream_chat_kwargs: JsonMap<String, Value>,
        fallback_providers: Vec<FailoverUpstreamProvider>,
        cooldown: Duration,
    ) -> Self {
        let name = name.into();
        let mut providers = vec![FailoverUpstreamProvider::new(
            name.clone(),
            primary_client.clone(),
            primary_upstream_model.clone(),
            None,
            primary_upstream_chat_kwargs,
        )];
        let fallback_exposed_models = fallback_providers
            .iter()
            .enumerate()
            .filter_map(|(index, provider)| {
                provider
                    .exposed_model
                    .clone()
                    .map(|model_id| RoutingFallbackExposedModel {
                        model_id,
                        failover_provider_index: index + 1,
                    })
            })
            .collect();
        providers.extend(fallback_providers);
        Self {
            name,
            primary_client,
            primary_upstream_model,
            fallback_exposed_models,
            client: FailoverUpstreamClient::new(providers, cooldown),
        }
    }

    /// The effective backend model of one of this routing provider's nested
    /// failover providers (its `upstream_model` rewrite, if any). Used by G4
    /// native-vision gating for an exposed-alias/fallback target that serves from
    /// exactly one provider. `None` when the index is out of range or the
    /// provider sends the request model through unchanged.
    fn failover_provider_model(&self, failover_provider_index: usize) -> Option<String> {
        self.client.provider_upstream_model(failover_provider_index)
    }
}

#[derive(Debug, Clone)]
pub struct RoutingUpstreamClient {
    providers: Vec<RoutingUpstreamProvider>,
    catalog: Arc<AsyncMutex<Option<CachedRoutingModelCatalog>>>,
}

#[derive(Debug, Clone)]
struct CachedRoutingModelCatalog {
    fetched_at: Instant,
    catalog: RoutingModelCatalog,
}

#[derive(Debug, Clone, Default)]
struct RoutingModelCatalog {
    provider_catalogs: Vec<RoutingProviderModelCatalog>,
    union_entries: Vec<Value>,
    union_ids: Vec<String>,
    ids_by_key: HashMap<String, Vec<RoutingModelCandidate>>,
}

#[derive(Debug, Clone, Default)]
struct RoutingProviderModelCatalog {
    candidates: Vec<RoutingModelCandidate>,
}

#[derive(Debug, Clone)]
struct RoutingModelCandidate {
    provider_index: usize,
    model_id: String,
    target: RoutingModelTarget,
}

#[derive(Debug, Clone)]
enum RoutingModelTarget {
    Primary,
    Fallback { failover_provider_index: usize },
}

#[derive(Debug, Clone)]
struct RoutingFallbackExposedModel {
    model_id: String,
    failover_provider_index: usize,
}

#[derive(Debug, Clone, Default)]
struct ProviderCooldownState {
    cooling_until: Option<Instant>,
    last_error: Option<String>,
}

/// Cheap case-insensitive scan for a potential image-URI marker (`data:` /
/// `http`) in serialized request bytes, so the log fast-path only pays the
/// redaction round-trip when an image URL might be present (G4 round-4 #3).
fn bytes_contain_image_uri(bytes: &[u8]) -> bool {
    bytes.windows(5).any(|w| w.eq_ignore_ascii_case(b"data:"))
        || bytes.windows(4).any(|w| w.eq_ignore_ascii_case(b"http"))
}

#[derive(Debug, Clone)]
struct UpstreamRequestLogger {
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl UpstreamRequestLogger {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    async fn log(&self, request: &ChatCompletionRequest) -> std::io::Result<()> {
        // G4 round-4 #3: the JSONL request log is written to DISK and would
        // otherwise serialize raw `data:` image bytes / signed `image_url`s for
        // native-vision-passthrough / disabled-agent / missing-url /
        // tool_choice:"none" image requests (any path that does NOT strip). The
        // common no-image request serializes directly (format unchanged); only
        // when the serialized bytes contain an image-URI marker do we re-redact
        // via a CLONED JSON value through the shared redactor, so the on-disk log
        // never carries request image content.
        let mut payload = serde_json::to_vec(request).map_err(std::io::Error::other)?;
        if bytes_contain_image_uri(&payload) {
            let mut value = serde_json::to_value(request).map_err(std::io::Error::other)?;
            crate::vision::redact_image_uris_in_value(&mut value);
            payload = serde_json::to_vec(&value).map_err(std::io::Error::other)?;
        }
        payload.push(b'\n');
        let path = self.path.clone();
        let write_lock = self.write_lock.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = write_lock.lock().map_err(|err| {
                std::io::Error::other(format!("request log lock poisoned: {err}"))
            })?;
            let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
            file.write_all(&payload)
        })
        .await
        .map_err(|err| std::io::Error::other(format!("spawn_blocking failed: {err}")))?
    }
}

/// vLLM/sglang serve `/tokenize` at the server root, not under `/v1`.
/// Strip a single trailing `v1` path segment and append `tokenize`.
fn derive_tokenize_url(base: &Url) -> Url {
    let mut segments: Vec<String> = base
        .path_segments()
        .map(|parts| {
            parts
                .filter(|segment| !segment.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if segments.last().map(String::as_str) == Some("v1") {
        segments.pop();
    }
    segments.push("tokenize".to_string());
    let mut url = base.clone();
    url.set_path(&format!("/{}", segments.join("/")));
    url
}

impl ReqwestUpstreamClient {
    pub fn new(
        client: reqwest::Client,
        base_url: Url,
        api_key: Option<String>,
        request_log_path: Option<PathBuf>,
        flatten_content: bool,
    ) -> Self {
        Self {
            client,
            base_url,
            api_key,
            request_logger: request_log_path.map(UpstreamRequestLogger::new),
            flatten_content,
        }
    }

    fn with_auth(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.api_key {
            Some(api_key) => request.bearer_auth(api_key),
            None => request,
        }
    }

    fn endpoint_url(&self, path: &str) -> AppResult<Url> {
        let mut url = self.base_url.clone();
        if !url.path().ends_with('/') {
            let new_path = format!("{}/", url.path());
            url.set_path(&new_path);
        }
        url.join(path)
            .map_err(|err| AppError::internal(format!("invalid upstream URL: {err}")))
    }
}

#[async_trait]
impl UpstreamClient for ReqwestUpstreamClient {
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> AppResult<UpstreamStream> {
        let url = self.endpoint_url("chat/completions")?;
        let request = sanitize_chat_request(request.clone(), self.flatten_content);
        if let Some(ref logger) = self.request_logger
            && let Err(err) = logger.log(&request).await
        {
            tracing::warn!(
                path = %logger.path.display(),
                error = %err,
                "failed to append upstream request log"
            );
        }
        let response = self
            .with_auth(self.client.post(url))
            .json(&request)
            .send()
            .await
            .map_err(|err| AppError::upstream(format!("upstream chat request failed: {err}")))?;
        ensure_success(response.status(), response).await
    }

    async fn list_models(&self) -> AppResult<reqwest::Response> {
        let url = self.endpoint_url("models")?;
        let response = self
            .with_auth(self.client.get(url))
            .send()
            .await
            .map_err(|err| AppError::upstream(format!("upstream models request failed: {err}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AppError::upstream(format!(
                "upstream /models failed with {status}: {}",
                redact_and_truncate_error_body(&body, 500)
            )));
        }
        Ok(response)
    }

    async fn proxy_completions(
        &self,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        let url = self.endpoint_url("completions")?;
        let request = copy_proxy_request_headers(self.client.post(url), &headers).body(body);
        self.with_auth(request).send().await.map_err(|err| {
            AppError::upstream(format!("upstream completions request failed: {err}"))
        })
    }

    async fn count_tokens(&self, body: &serde_json::Value) -> AppResult<Option<u64>> {
        let url = derive_tokenize_url(&self.base_url);
        let response = match self
            .with_auth(self.client.post(url))
            .json(body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                tracing::debug!(error = %err, "upstream /tokenize request failed");
                return Ok(None);
            }
        };
        if !response.status().is_success() {
            tracing::debug!(status = %response.status(), "upstream /tokenize non-2xx");
            return Ok(None);
        }
        let value: serde_json::Value = match response.json().await {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(error = %err, "upstream /tokenize body not json");
                return Ok(None);
            }
        };
        Ok(value.get("count").and_then(serde_json::Value::as_u64))
    }

    async fn supported_model_ids(&self) -> AppResult<Vec<String>> {
        let response = self.list_models().await?;
        collect_supported_model_ids(response).await
    }
}

impl FailoverUpstreamClient {
    pub fn new(providers: Vec<FailoverUpstreamProvider>, cooldown: Duration) -> Self {
        let states = vec![ProviderCooldownState::default(); providers.len()];
        Self {
            providers,
            cooldown,
            states: Arc::new(Mutex::new(states)),
        }
    }

    /// The configured `upstream_model` rewrite of provider `index`, if any.
    /// `None` when out of range or the provider sends the request model
    /// unchanged. Used by G4 native-vision gating (round-2 #1).
    fn provider_upstream_model(&self, index: usize) -> Option<String> {
        self.providers
            .get(index)
            .and_then(|provider| provider.upstream_model.clone())
    }

    fn available_provider_indices(&self) -> Vec<usize> {
        let now = Instant::now();
        let states = self
            .states
            .lock()
            .expect("upstream provider cooldown state lock poisoned");
        self.providers
            .iter()
            .enumerate()
            .filter_map(|(index, _)| {
                let cooling = states
                    .get(index)
                    .and_then(|state| state.cooling_until)
                    .is_some_and(|until| until > now);
                (!cooling).then_some(index)
            })
            .collect()
    }

    fn provider_is_available(&self, provider_index: usize) -> bool {
        let now = Instant::now();
        let states = self
            .states
            .lock()
            .expect("upstream provider cooldown state lock poisoned");
        !states
            .get(provider_index)
            .and_then(|state| state.cooling_until)
            .is_some_and(|until| until > now)
    }

    fn cooldown_error(&self) -> AppError {
        let now = Instant::now();
        let states = self
            .states
            .lock()
            .expect("upstream provider cooldown state lock poisoned");
        let next_retry_secs = states
            .iter()
            .filter_map(|state| state.cooling_until)
            .filter(|until| *until > now)
            .map(|until| until.duration_since(now).as_secs().max(1))
            .min()
            .unwrap_or(0);
        let last_error = states
            .iter()
            .rev()
            .find_map(|state| state.last_error.as_deref())
            .unwrap_or("no provider is currently available");
        AppError::upstream(format!(
            "all upstream providers are in cooldown; next retry in {next_retry_secs}s; last error: {last_error}"
        ))
    }

    fn request_for_provider(
        provider: &FailoverUpstreamProvider,
        request: &ChatCompletionRequest,
    ) -> ChatCompletionRequest {
        let mut request = request.clone();
        if let Some(model) = &provider.upstream_model {
            request.model = model.clone();
        }
        merge_fallback_chat_kwargs(&mut request, &provider.upstream_chat_kwargs);
        request
    }

    async fn prefetch_first_chunk(
        mut stream: UpstreamStream,
        request_timeout: Duration,
    ) -> AppResult<(ChatCompletionChunk, UpstreamStream)> {
        match tokio::time::timeout(request_timeout, stream.next()).await {
            Ok(Some(Ok(chunk))) => Ok((chunk, stream)),
            Ok(Some(Err(err))) => Err(err),
            Ok(None) => Err(AppError::upstream(
                "upstream stream ended before the first chunk",
            )),
            Err(_) => Err(AppError::upstream("upstream stream timed out".to_string())),
        }
    }

    fn stream_after_prefetch(
        &self,
        provider_index: usize,
        first_chunk: ChatCompletionChunk,
        mut stream: UpstreamStream,
        request_timeout: Duration,
    ) -> UpstreamStream {
        let states = Arc::clone(&self.states);
        let cooldown = self.cooldown;
        let provider_name = self.providers[provider_index].name.clone();
        Box::pin(async_stream::stream! {
            yield Ok(first_chunk);
            loop {
                match tokio::time::timeout(request_timeout, stream.next()).await {
                    Ok(Some(Ok(chunk))) => yield Ok(chunk),
                    Ok(Some(Err(err))) => {
                        Self::mark_provider_failure(
                            &states,
                            provider_index,
                            &provider_name,
                            cooldown,
                            err.to_string(),
                        );
                        yield Err(err);
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        let err = AppError::upstream("upstream stream timed out".to_string());
                        Self::mark_provider_failure(
                            &states,
                            provider_index,
                            &provider_name,
                            cooldown,
                            err.to_string(),
                        );
                        yield Err(err);
                        break;
                    }
                }
            }
        })
    }

    fn mark_provider_success(&self, provider_index: usize) {
        let mut states = self
            .states
            .lock()
            .expect("upstream provider cooldown state lock poisoned");
        if let Some(state) = states.get_mut(provider_index) {
            state.cooling_until = None;
            state.last_error = None;
        }
    }

    fn mark_provider_failure(
        states: &Arc<Mutex<Vec<ProviderCooldownState>>>,
        provider_index: usize,
        provider_name: &str,
        cooldown: Duration,
        error: String,
    ) {
        let cooling_until = (cooldown > Duration::ZERO).then(|| Instant::now() + cooldown);
        let mut states = states
            .lock()
            .expect("upstream provider cooldown state lock poisoned");
        if let Some(state) = states.get_mut(provider_index) {
            state.cooling_until = cooling_until;
            state.last_error = Some(error.clone());
        }
        if cooldown > Duration::ZERO {
            tracing::warn!(
                provider = provider_name,
                cooldown_secs = cooldown.as_secs(),
                error = %error,
                "upstream provider failed; entering cooldown"
            );
        } else {
            tracing::warn!(
                provider = provider_name,
                error = %error,
                "upstream provider failed"
            );
        }
    }

    fn mark_failure(&self, provider_index: usize, error: &AppError) {
        Self::mark_provider_failure(
            &self.states,
            provider_index,
            &self.providers[provider_index].name,
            self.cooldown,
            error.to_string(),
        );
    }

    async fn stream_chat_completion_with_timeout_from_provider(
        &self,
        provider_index: usize,
        request: &ChatCompletionRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        if provider_index >= self.providers.len() {
            return Err(AppError::internal(
                "resolved fallback provider index was out of range",
            ));
        }
        if !self.provider_is_available(provider_index) {
            return Err(self.cooldown_error());
        }
        self.stream_chat_completion_with_provider_indices(
            vec![provider_index],
            request,
            request_timeout,
        )
        .await
    }

    async fn stream_chat_completion_with_provider_indices(
        &self,
        provider_indices: Vec<usize>,
        request: &ChatCompletionRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        let mut last_error = None;
        for provider_index in provider_indices {
            let provider = &self.providers[provider_index];
            let provider_request = Self::request_for_provider(provider, request);
            let stream = match provider
                .client
                .stream_chat_completion(&provider_request)
                .await
            {
                Ok(stream) => stream,
                Err(err) => {
                    self.mark_failure(provider_index, &err);
                    last_error = Some(err);
                    continue;
                }
            };
            match Self::prefetch_first_chunk(stream, request_timeout).await {
                Ok((first_chunk, stream)) => {
                    self.mark_provider_success(provider_index);
                    if provider_index > 0 {
                        tracing::info!(
                            provider = %provider.name,
                            "using fallback upstream provider"
                        );
                    }
                    return Ok(self.stream_after_prefetch(
                        provider_index,
                        first_chunk,
                        stream,
                        request_timeout,
                    ));
                }
                Err(err) => {
                    self.mark_failure(provider_index, &err);
                    last_error = Some(err);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            AppError::upstream("all upstream providers failed before producing a response")
        }))
    }

    async fn proxy_completions_from_provider(
        &self,
        provider_index: usize,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        if provider_index >= self.providers.len() {
            return Err(AppError::internal(
                "resolved fallback provider index was out of range",
            ));
        }
        if !self.provider_is_available(provider_index) {
            return Err(self.cooldown_error());
        }
        self.proxy_completions_with_provider_indices(vec![provider_index], headers, body)
            .await
    }

    async fn proxy_completions_with_provider_indices(
        &self,
        provider_indices: Vec<usize>,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        let mut last_error = None;
        for provider_index in provider_indices {
            let provider = &self.providers[provider_index];
            let provider_body = proxy_body_for_provider(provider, &body);
            match self.providers[provider_index]
                .client
                .proxy_completions(headers.clone(), provider_body)
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    if !should_failover_proxy_status(status) {
                        return Ok(response);
                    }
                    let body = response.text().await.unwrap_or_default();
                    let err = AppError::upstream(format!(
                        "upstream completions failed with {status}: {}",
                        redact_and_truncate_error_body(&body, 500)
                    ));
                    self.mark_failure(provider_index, &err);
                    last_error = Some(err);
                }
                Err(err) => {
                    self.mark_failure(provider_index, &err);
                    last_error = Some(err);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            AppError::upstream("all upstream providers failed to proxy completions")
        }))
    }
}

impl RoutingUpstreamClient {
    pub fn new(providers: Vec<RoutingUpstreamProvider>) -> Self {
        Self {
            providers,
            catalog: Arc::new(AsyncMutex::new(None)),
        }
    }

    async fn load_catalog(&self) -> AppResult<RoutingModelCatalog> {
        let mut cache = self.catalog.lock().await;
        if let Some(cached) = cache.as_ref()
            && cached.fetched_at.elapsed().as_secs() < ROUTING_MODEL_CATALOG_TTL_SECS
        {
            return Ok(cached.catalog.clone());
        }
        let catalog = self.refresh_catalog().await?;
        *cache = Some(CachedRoutingModelCatalog {
            fetched_at: Instant::now(),
            catalog: catalog.clone(),
        });
        Ok(catalog)
    }

    async fn refresh_catalog(&self) -> AppResult<RoutingModelCatalog> {
        let mut provider_catalogs = Vec::with_capacity(self.providers.len());
        let mut union_entries = Vec::new();
        let mut union_ids = Vec::new();
        let mut ids_by_key: HashMap<String, Vec<RoutingModelCandidate>> = HashMap::new();
        let mut seen_union_ids = HashSet::new();
        let mut last_error = None;

        for (provider_index, provider) in self.providers.iter().enumerate() {
            let entries = match primary_provider_model_entries(provider).await {
                Ok(entries) => entries,
                Err(err) => {
                    tracing::warn!(
                        provider = %provider.name,
                        error = %err,
                        "failed to load upstream model catalog"
                    );
                    last_error = Some(err);
                    Vec::new()
                }
            };

            let mut provider_candidates = Vec::new();
            for entry in entries {
                let Some((model_id, entry)) = normalized_model_entry(entry) else {
                    continue;
                };
                register_routing_model(
                    provider_index,
                    model_id,
                    entry,
                    RoutingModelTarget::Primary,
                    &mut provider_candidates,
                    &mut union_entries,
                    &mut union_ids,
                    &mut ids_by_key,
                    &mut seen_union_ids,
                );
            }
            for model in &provider.fallback_exposed_models {
                register_routing_model(
                    provider_index,
                    model.model_id.clone(),
                    single_model_entry(&model.model_id),
                    RoutingModelTarget::Fallback {
                        failover_provider_index: model.failover_provider_index,
                    },
                    &mut provider_candidates,
                    &mut union_entries,
                    &mut union_ids,
                    &mut ids_by_key,
                    &mut seen_union_ids,
                );
            }
            provider_catalogs.push(RoutingProviderModelCatalog {
                candidates: provider_candidates,
            });
        }

        if union_ids.is_empty() {
            return Err(last_error.unwrap_or_else(|| {
                AppError::upstream("no models are currently available from configured upstreams")
            }));
        }

        Ok(RoutingModelCatalog {
            provider_catalogs,
            union_entries,
            union_ids,
            ids_by_key,
        })
    }
}

impl RoutingModelCatalog {
    fn resolve(&self, requested_model: &str) -> Option<RoutingModelCandidate> {
        let trimmed = requested_model.trim();
        if !trimmed.is_empty() {
            for (provider_index, provider) in self.provider_catalogs.iter().enumerate() {
                if let Some(candidate) = provider
                    .candidates
                    .iter()
                    .find(|candidate| candidate.model_id == trimmed)
                {
                    debug_assert_eq!(candidate.provider_index, provider_index);
                    return Some(candidate.clone());
                }
            }

            let key = canonical_model_key(trimmed);
            if let Some(candidates) = self.ids_by_key.get(&key)
                && let Some(model_id) = unique_candidate_model_id(candidates)
            {
                return candidates
                    .iter()
                    .find(|candidate| candidate.model_id == model_id)
                    .cloned();
            }
        }

        self.default_candidate()
    }

    fn default_candidate(&self) -> Option<RoutingModelCandidate> {
        self.provider_catalogs
            .iter()
            .enumerate()
            .find_map(|(provider_index, provider)| {
                provider.candidates.first().map(|candidate| {
                    debug_assert_eq!(candidate.provider_index, provider_index);
                    candidate.clone()
                })
            })
    }

    fn union_body(&self) -> Value {
        serde_json::json!({
            "object": "list",
            "data": self.union_entries,
        })
    }
}

fn unique_candidate_model_id(candidates: &[RoutingModelCandidate]) -> Option<String> {
    let mut unique = candidates
        .iter()
        .map(|candidate| candidate.model_id.as_str())
        .collect::<HashSet<_>>();
    if unique.len() == 1 {
        unique.drain().next().map(ToString::to_string)
    } else {
        None
    }
}

async fn primary_provider_model_entries(
    provider: &RoutingUpstreamProvider,
) -> AppResult<Vec<Value>> {
    let Some(model) = provider.primary_upstream_model.as_deref() else {
        let response = provider.primary_client.list_models().await?;
        let (_, body, _) = collect_models_response(response).await?;
        return Ok(model_entries_from_body(&body));
    };

    match provider.primary_client.list_models().await {
        Ok(response) => {
            let (_, body, _) = collect_models_response(response).await?;
            Ok(filter_model_entries(&model_entries_from_body(&body), model))
        }
        Err(err) => {
            tracing::warn!(
                provider = %provider.name,
                model,
                error = %err,
                "failed to load model metadata for configured upstream model; using synthetic entry"
            );
            Ok(vec![single_model_entry(model)])
        }
    }
}

fn normalized_model_entry(entry: Value) -> Option<(String, Value)> {
    match entry {
        Value::String(id) => Some((id.clone(), single_model_entry(&id))),
        Value::Object(map) => {
            let id = map.get("id").and_then(Value::as_str)?.to_string();
            Some((id, Value::Object(map)))
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn register_routing_model(
    provider_index: usize,
    model_id: String,
    entry: Value,
    target: RoutingModelTarget,
    provider_candidates: &mut Vec<RoutingModelCandidate>,
    union_entries: &mut Vec<Value>,
    union_ids: &mut Vec<String>,
    ids_by_key: &mut HashMap<String, Vec<RoutingModelCandidate>>,
    seen_union_ids: &mut HashSet<String>,
) {
    let key = canonical_model_key(&model_id);
    if key.is_empty() {
        return;
    }
    let candidate = RoutingModelCandidate {
        provider_index,
        model_id: model_id.clone(),
        target,
    };
    if !provider_candidates
        .iter()
        .any(|candidate| candidate.model_id == model_id)
    {
        provider_candidates.push(candidate.clone());
    }
    ids_by_key.entry(key).or_default().push(candidate);
    if seen_union_ids.insert(model_id.clone()) {
        union_ids.push(model_id);
        union_entries.push(entry);
    }
}

#[async_trait]
impl UpstreamClient for FailoverUpstreamClient {
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> AppResult<UpstreamStream> {
        self.stream_chat_completion_with_timeout(request, Duration::from_secs(60))
            .await
    }

    async fn stream_chat_completion_with_timeout(
        &self,
        request: &ChatCompletionRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        let provider_indices = self.available_provider_indices();
        if provider_indices.is_empty() {
            return Err(self.cooldown_error());
        }
        self.stream_chat_completion_with_provider_indices(
            provider_indices,
            request,
            request_timeout,
        )
        .await
    }

    async fn list_models(&self) -> AppResult<reqwest::Response> {
        let mut last_error = None;
        let provider_indices = self.available_provider_indices();
        if provider_indices.is_empty() {
            return Err(self.cooldown_error());
        }
        for provider_index in provider_indices {
            let provider = &self.providers[provider_index];
            match provider.client.list_models().await {
                Ok(response) => {
                    if let Some(model) = &provider.upstream_model {
                        return filter_models_response(response, model).await;
                    }
                    return Ok(response);
                }
                Err(err) => last_error = Some(err),
            }
        }
        Err(last_error
            .unwrap_or_else(|| AppError::upstream("all upstream providers failed to list models")))
    }

    async fn proxy_completions(
        &self,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        let provider_indices = self.available_provider_indices();
        if provider_indices.is_empty() {
            return Err(self.cooldown_error());
        }
        self.proxy_completions_with_provider_indices(provider_indices, headers, body)
            .await
    }

    async fn supported_model_ids(&self) -> AppResult<Vec<String>> {
        let response = self.list_models().await?;
        collect_supported_model_ids(response).await
    }

    /// Every configured provider is a pre-first-chunk failover candidate, so the
    /// candidate model set is each provider's effective model — its
    /// `upstream_model` rewrite (matching `request_for_provider`) or the request
    /// model when it sends through unchanged (G4 round-2 #1). We enumerate ALL
    /// providers (not just currently-available ones): cooldown is transient, so
    /// a fallback that is non-native must still force strip+offload.
    async fn candidate_backend_models(&self, requested_model: &str) -> Vec<String> {
        self.providers
            .iter()
            .map(|provider| {
                provider
                    .upstream_model
                    .clone()
                    .unwrap_or_else(|| requested_model.to_string())
            })
            .collect()
    }
}

#[async_trait]
impl UpstreamClient for RoutingUpstreamClient {
    async fn stream_chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> AppResult<UpstreamStream> {
        self.stream_chat_completion_with_timeout(request, Duration::from_secs(60))
            .await
    }

    /// Candidate backend models for `requested_model` using the SAME route/
    /// catalog resolution as `stream_chat_completion` (G4 review #2 + round-2
    /// #1). A route resolves to one backend model. A catalog match dispatches to
    /// a routing provider: the PRIMARY target may fail over across that
    /// provider's whole failover chain (so all of its candidate models count),
    /// while a fallback/exposed-alias target serves only that single provider's
    /// model. A catalog-load failure yields an empty set, which the safe
    /// invariant treats as "unknown → strip+offload".
    async fn candidate_backend_models(&self, requested_model: &str) -> Vec<String> {
        let Ok(catalog) = self.load_catalog().await else {
            return Vec::new();
        };
        let Some(candidate) = catalog.resolve(requested_model) else {
            return Vec::new();
        };
        let Some(provider) = self.providers.get(candidate.provider_index) else {
            return Vec::new();
        };
        match candidate.target {
            // Primary: the whole nested failover chain may serve.
            RoutingModelTarget::Primary => {
                provider
                    .client
                    .candidate_backend_models(&candidate.model_id)
                    .await
            }
            // Fallback/exposed-alias: only this one provider serves.
            RoutingModelTarget::Fallback {
                failover_provider_index,
            } => provider
                .failover_provider_model(failover_provider_index)
                .map(|model| vec![model])
                .unwrap_or_else(|| vec![candidate.model_id.clone()]),
        }
    }

    async fn stream_chat_completion_with_timeout(
        &self,
        request: &ChatCompletionRequest,
        request_timeout: Duration,
    ) -> AppResult<UpstreamStream> {
        let catalog = self.load_catalog().await?;
        let resolution = catalog.resolve(&request.model).ok_or_else(|| {
            AppError::upstream("no models are currently available from configured upstreams")
        })?;
        let provider = self
            .providers
            .get(resolution.provider_index)
            .ok_or_else(|| {
                AppError::internal("resolved upstream provider index was out of range")
            })?;
        let mut routed_request = request.clone();
        if routed_request.model != resolution.model_id {
            tracing::info!(
                requested_model = %routed_request.model,
                routed_model = %resolution.model_id,
                provider = %provider.name,
                "routed request model to upstream catalog model"
            );
            routed_request.model = resolution.model_id;
        }
        match resolution.target {
            RoutingModelTarget::Primary => {
                provider
                    .client
                    .stream_chat_completion_with_timeout(&routed_request, request_timeout)
                    .await
            }
            RoutingModelTarget::Fallback {
                failover_provider_index,
            } => {
                provider
                    .client
                    .stream_chat_completion_with_timeout_from_provider(
                        failover_provider_index,
                        &routed_request,
                        request_timeout,
                    )
                    .await
            }
        }
    }

    async fn list_models(&self) -> AppResult<reqwest::Response> {
        let catalog = self.load_catalog().await?;
        json_response(catalog.union_body())
    }

    async fn proxy_completions(
        &self,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<reqwest::Response> {
        let catalog = self.load_catalog().await?;
        let requested_model = proxy_body_model(&body).unwrap_or_default();
        let resolution = catalog.resolve(&requested_model).ok_or_else(|| {
            AppError::upstream("no models are currently available from configured upstreams")
        })?;
        let provider = self
            .providers
            .get(resolution.provider_index)
            .ok_or_else(|| {
                AppError::internal("resolved upstream provider index was out of range")
            })?;
        let body = proxy_body_with_model(body, &resolution.model_id);
        match resolution.target {
            RoutingModelTarget::Primary => provider.client.proxy_completions(headers, body).await,
            RoutingModelTarget::Fallback {
                failover_provider_index,
            } => {
                provider
                    .client
                    .proxy_completions_from_provider(failover_provider_index, headers, body)
                    .await
            }
        }
    }

    async fn supported_model_ids(&self) -> AppResult<Vec<String>> {
        let catalog = self.load_catalog().await?;
        Ok(catalog.union_ids)
    }
}

fn timeout_upstream_stream(
    mut stream: UpstreamStream,
    request_timeout: Duration,
) -> UpstreamStream {
    Box::pin(async_stream::stream! {
        loop {
            match tokio::time::timeout(request_timeout, stream.next()).await {
                Ok(Some(chunk)) => yield chunk,
                Ok(None) => break,
                Err(_) => {
                    yield Err(AppError::upstream("upstream stream timed out".to_string()));
                    break;
                }
            }
        }
    })
}

fn should_failover_proxy_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

fn merge_fallback_chat_kwargs(
    request: &mut ChatCompletionRequest,
    defaults: &JsonMap<String, Value>,
) {
    for (key, value) in defaults {
        if chat_request_field_is_set(request, key) {
            continue;
        }
        match request.extra_body.get_mut(key) {
            Some(existing) => merge_json_value_preserve_destination(existing, value),
            None => {
                request.extra_body.insert(key.clone(), value.clone());
            }
        }
    }
}

fn chat_request_field_is_set(request: &ChatCompletionRequest, key: &str) -> bool {
    match key {
        "model" | "messages" | "stream" | "parallel_tool_calls" => true,
        "tools" => request.tools.is_some(),
        "tool_choice" => request.tool_choice.is_some(),
        "reasoning_effort" => request.reasoning_effort.is_some(),
        "response_format" => request.response_format.is_some(),
        "stream_options" => request.stream_options.is_some(),
        "temperature" => request.temperature.is_some(),
        "top_p" => request.top_p.is_some(),
        "max_tokens" | "max_output_tokens" | "max_completion_tokens" => {
            request.max_output_tokens.is_some()
        }
        "frequency_penalty" => request.frequency_penalty.is_some(),
        "presence_penalty" => request.presence_penalty.is_some(),
        _ => false,
    }
}

fn merge_json_value_preserve_destination(destination: &mut Value, source: &Value) {
    if let Value::Object(destination_object) = destination
        && let Value::Object(source_object) = source
    {
        for (key, source_value) in source_object {
            match destination_object.get_mut(key) {
                Some(destination_value) => {
                    merge_json_value_preserve_destination(destination_value, source_value);
                }
                None => {
                    destination_object.insert(key.clone(), source_value.clone());
                }
            }
        }
    }
}

fn proxy_body_for_provider(provider: &FailoverUpstreamProvider, body: &Bytes) -> Bytes {
    match provider.upstream_model.as_deref() {
        Some(model) => proxy_body_with_model(body.clone(), model),
        None => body.clone(),
    }
}

fn proxy_body_model(body: &Bytes) -> Option<String> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("model")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|model| !model.is_empty())
                .map(ToString::to_string)
        })
}

fn proxy_body_with_model(body: Bytes, model: &str) -> Bytes {
    let Ok(mut value) = serde_json::from_slice::<Value>(&body) else {
        return body;
    };
    let Some(object) = value.as_object_mut() else {
        return body;
    };
    object.insert("model".to_string(), Value::String(model.to_string()));
    serde_json::to_vec(&value).map(Bytes::from).unwrap_or(body)
}

pub(crate) fn canonical_model_key(model: &str) -> String {
    model
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
}

fn copy_proxy_request_headers(mut request: RequestBuilder, headers: &HeaderMap) -> RequestBuilder {
    for (name, value) in headers {
        if should_proxy_request_header(name) {
            request = request.header(name.clone(), value.clone());
        }
    }
    request
}

fn should_proxy_request_header(name: &HeaderName) -> bool {
    !is_hop_by_hop_header(name)
        && !header_name_eq(name, "authorization")
        && !header_name_eq(name, "host")
        && !header_name_eq(name, "content-length")
}

fn is_hop_by_hop_header(name: &HeaderName) -> bool {
    [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ]
    .iter()
    .any(|header| header_name_eq(name, header))
}

fn header_name_eq(name: &HeaderName, other: &str) -> bool {
    name.as_str().eq_ignore_ascii_case(other)
}

async fn ensure_success(
    status: StatusCode,
    response: reqwest::Response,
) -> AppResult<UpstreamStream> {
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::upstream(format!(
            "upstream chat failed with {status}: {}",
            truncate_for_error(&body, 500)
        )));
    }
    let stream = response
        .bytes_stream()
        .eventsource()
        .filter_map(|result| async move {
            match result {
                Ok(event) if event.data == "[DONE]" => None,
                Ok(event) => Some(parse_chat_completion_chunk(&event.data).map_err(|err| {
                    AppError::upstream(format!(
                        "failed to parse upstream chat chunk: {err}; payload={}",
                        truncate_for_error(&event.data, 500)
                    ))
                })),
                Err(err) => Some(Err(AppError::upstream(format!(
                    "failed to read upstream SSE: {err}"
                )))),
            }
        });
    Ok(Box::pin(stream))
}

fn parse_chat_completion_chunk(data: &str) -> Result<ChatCompletionChunk, serde_json::Error> {
    let first_error = match serde_json::from_str::<ChatCompletionChunk>(data) {
        Ok(chunk) => return Ok(chunk),
        Err(err) => err,
    };
    let Ok(mut value) = serde_json::from_str::<Value>(data) else {
        return Err(first_error);
    };
    if !normalize_sparse_tool_call_types(&mut value) {
        return Err(first_error);
    }
    serde_json::from_value(value)
}

fn normalize_sparse_tool_call_types(value: &mut Value) -> bool {
    let Some(choices) = value.get_mut("choices").and_then(Value::as_array_mut) else {
        return false;
    };
    let mut changed = false;
    for choice in choices {
        let Some(tool_calls) = choice
            .get_mut("delta")
            .and_then(|delta| delta.get_mut("tool_calls"))
            .and_then(Value::as_array_mut)
        else {
            continue;
        };
        for tool_call in tool_calls {
            let Some(object) = tool_call.as_object_mut() else {
                continue;
            };
            if !object.contains_key("type") {
                object.insert("type".to_string(), Value::String("function".to_string()));
                changed = true;
            }
        }
    }
    changed
}

pub async fn collect_models_response(
    response: reqwest::Response,
) -> AppResult<(StatusCode, Value, Option<String>)> {
    let status = response.status();
    let etag = response
        .headers()
        .get(http::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    let body = response
        .json::<Value>()
        .await
        .map_err(|err| AppError::upstream(format!("invalid upstream /models JSON: {err}")))?;
    Ok((status, body, etag))
}

pub async fn collect_supported_model_ids(response: reqwest::Response) -> AppResult<Vec<String>> {
    let (_, body, _) = collect_models_response(response).await?;
    Ok(extract_supported_model_ids(&body))
}

async fn filter_models_response(
    response: reqwest::Response,
    model: &str,
) -> AppResult<reqwest::Response> {
    let status = response.status();
    let body = response
        .json::<Value>()
        .await
        .map_err(|err| AppError::upstream(format!("invalid upstream /models JSON: {err}")))?;
    let body = filter_models_body(body, model);
    let body = serde_json::to_string(&body).map_err(|err| {
        AppError::internal(format!("failed to serialize /models response: {err}"))
    })?;
    let response = http::Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .map_err(|err| AppError::internal(format!("failed to build /models response: {err}")))?;
    Ok(reqwest::Response::from(response))
}

fn json_response(body: Value) -> AppResult<reqwest::Response> {
    let body = serde_json::to_string(&body)
        .map_err(|err| AppError::internal(format!("failed to serialize JSON response: {err}")))?;
    let response = http::Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .map_err(|err| AppError::internal(format!("failed to build JSON response: {err}")))?;
    Ok(reqwest::Response::from(response))
}

fn filter_models_body(body: Value, model: &str) -> Value {
    match body {
        Value::Object(mut map) => {
            if let Some(entries) = map.get("data").and_then(Value::as_array) {
                map.insert(
                    "data".to_string(),
                    Value::Array(filter_model_entries(entries, model)),
                );
                Value::Object(map)
            } else if let Some(entries) = map.get("models").and_then(Value::as_array) {
                map.insert(
                    "models".to_string(),
                    Value::Array(filter_model_entries(entries, model)),
                );
                Value::Object(map)
            } else {
                single_model_list_body(model)
            }
        }
        Value::Array(entries) => Value::Array(filter_model_entries(&entries, model)),
        _ => single_model_list_body(model),
    }
}

fn filter_model_entries(entries: &[Value], model: &str) -> Vec<Value> {
    match entries
        .iter()
        .find(|entry| model_entry_id(entry).is_some_and(|id| id == model))
    {
        Some(entry) => vec![entry.clone()],
        None => vec![single_model_entry(model)],
    }
}

fn model_entry_id(entry: &Value) -> Option<&str> {
    match entry {
        Value::String(id) => Some(id.as_str()),
        Value::Object(map) => map.get("id").and_then(Value::as_str),
        _ => None,
    }
}

fn single_model_list_body(model: &str) -> Value {
    serde_json::json!({
        "object": "list",
        "data": [single_model_entry(model)]
    })
}

fn single_model_entry(model: &str) -> Value {
    serde_json::json!({
        "id": model,
        "object": "model",
    })
}

fn extract_supported_model_ids(body: &Value) -> Vec<String> {
    extract_model_ids_from_array(&model_entries_from_body(body))
}

fn model_entries_from_body(body: &Value) -> Vec<Value> {
    match body {
        Value::Array(entries) => entries.clone(),
        Value::Object(map) => map
            .get("data")
            .or_else(|| map.get("models"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn extract_model_ids_from_array(entries: &[Value]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|entry| match entry {
            Value::String(id) => Some(id.clone()),
            Value::Object(map) => map
                .get("id")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            _ => None,
        })
        .collect()
}

pub(crate) fn sanitize_chat_request(
    mut request: ChatCompletionRequest,
    flatten_content: bool,
) -> ChatCompletionRequest {
    if request.tools.as_ref().is_none_or(Vec::is_empty)
        && request.tool_choice.as_ref().is_none_or(|v| v == "auto")
    {
        request.tool_choice = None;
    }
    for message in &mut request.messages {
        if let Some(content) = message.content.take() {
            message.content = sanitize_message_content(content, flatten_content);
        }
        if let Some(tool_calls) = message.tool_calls.as_mut() {
            for tool_call in tool_calls {
                if let Some(arguments) = tool_call.function.arguments.take() {
                    tool_call.function.arguments = Some(stringify_json_value(arguments));
                }
            }
        }
    }
    canonicalize_chat_request_json_values(&mut request);
    request
}

fn canonicalize_chat_request_json_values(request: &mut ChatCompletionRequest) {
    for message in &mut request.messages {
        if let Some(content) = &mut message.content {
            canonicalize_json_value(content);
        }
        if let Some(tool_calls) = &mut message.tool_calls {
            for tool_call in tool_calls {
                if let Some(arguments) = &mut tool_call.function.arguments {
                    canonicalize_json_value(arguments);
                }
            }
        }
    }
    if let Some(tools) = &mut request.tools {
        for tool in tools {
            if let Some(parameters) = &mut tool.function.parameters {
                canonicalize_json_value(parameters);
            }
        }
    }
    if let Some(tool_choice) = &mut request.tool_choice {
        canonicalize_json_value(tool_choice);
    }
    if let Some(response_format) = &mut request.response_format {
        canonicalize_json_value(response_format);
    }
    for value in request.extra_body.values_mut() {
        canonicalize_json_value(value);
    }
}

fn canonicalize_json_value(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                canonicalize_json_value(item);
            }
        }
        Value::Object(map) => {
            let mut entries: Vec<_> = std::mem::take(map).into_iter().collect();
            for (_, entry_value) in &mut entries {
                canonicalize_json_value(entry_value);
            }
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            map.extend(entries);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn sanitize_message_content(content: Value, flatten_content: bool) -> Option<Value> {
    match content {
        Value::Null => None,
        Value::String(text) => Some(Value::String(text)),
        Value::Array(parts) => {
            if flatten_content && content_parts_are_text_only(&parts) {
                Some(Value::String(flatten_content_parts(&parts)))
            } else {
                Some(Value::Array(parts))
            }
        }
        other => Some(stringify_json_value(other)),
    }
}

fn content_parts_are_text_only(parts: &[Value]) -> bool {
    parts.iter().all(|part| {
        let has_text = part.get("text").and_then(Value::as_str).is_some();
        let text_kind = matches!(
            part.get("type").and_then(Value::as_str),
            None | Some("text") | Some("input_text") | Some("output_text")
        );
        has_text && text_kind
    })
}

fn flatten_content_parts(parts: &[Value]) -> String {
    let mut text_parts = Vec::new();
    for part in parts {
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            text_parts.push(text.to_string());
        } else {
            text_parts.push(serde_json::to_string(part).unwrap_or_else(|_| "null".to_string()));
        }
    }
    text_parts.join("\n")
}

fn truncate_for_error(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    match s.char_indices().nth(max) {
        Some((byte_idx, _)) => format!("{}...[truncated]", &s[..byte_idx]),
        None => s.to_string(),
    }
}

/// Redact image `data:`/signed URLs from an upstream RESPONSE error body, then
/// truncate it for an `AppError`/log message (G4 round-9 #2). A provider that
/// echoes a native-vision-passthrough / disabled-agent image request can mirror
/// the submitted `data:` bytes or a signed image URL back in its 4xx/5xx body;
/// without this they would leak through `response.failed` and failover logs
/// (AGENTS.md redact rule). Redaction runs BEFORE truncation so a split image
/// URI cannot survive at the truncation boundary.
fn redact_and_truncate_error_body(body: &str, max: usize) -> String {
    truncate_for_error(&crate::vision::redact_image_uris(body), max)
}

fn stringify_json_value(mut value: Value) -> Value {
    canonicalize_json_value(&mut value);
    Value::String(serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string()))
}

#[cfg(test)]
mod tests {
    use super::ReqwestUpstreamClient;
    use super::UpstreamClient;
    use super::UpstreamRequestLogger;
    use super::extract_supported_model_ids;
    use super::sanitize_chat_request;
    use crate::models::chat::ChatCompletionRequest;
    use crate::models::chat::ChatFunctionCall;
    use crate::models::chat::ChatMessage;
    use crate::models::chat::ChatTool;
    use crate::models::chat::ChatToolCall;
    use crate::models::chat::ChatToolDefinition;
    use serde_json::Value;
    use std::collections::BTreeMap;

    #[test]
    fn endpoint_url_preserves_v1_without_trailing_slash() {
        let client = ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            url::Url::parse("https://api.x.ai/v1").expect("url"),
            None,
            None,
            true,
        );

        assert_eq!(
            client
                .endpoint_url("chat/completions")
                .expect("endpoint")
                .as_str(),
            "https://api.x.ai/v1/chat/completions"
        );
    }

    #[test]
    fn endpoint_url_preserves_v1_with_trailing_slash() {
        let client = ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            url::Url::parse("https://api.x.ai/v1/").expect("url"),
            None,
            None,
            true,
        );

        assert_eq!(
            client.endpoint_url("models").expect("endpoint").as_str(),
            "https://api.x.ai/v1/models"
        );
    }

    #[test]
    fn normalize_sparse_tool_call_types_fills_chat_function_type() {
        let mut value = serde_json::json!({
            "id": "gen-1778509925-7119UkUjPTix9sGQ4vZf",
            "object": "chat.completion.chunk",
            "created": 1778509925,
            "model": "xiaomi/mimo-v2.5-pro-20260422",
            "provider": "Xiaomi",
            "choices": [{
                "index": 0,
                "delta": {
                    "content": null,
                    "role": "assistant",
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": "{\"file_path\": \"/home/luke/.claude/projects/-home-luke-projects-demo/memory/smb_clone.md\"}"
                        }
                    }]
                },
                "finish_reason": null,
                "native_finish_reason": null
            }]
        });

        assert!(super::normalize_sparse_tool_call_types(&mut value));
        assert_eq!(
            value["choices"][0]["delta"]["tool_calls"][0]["type"],
            Value::String("function".to_string())
        );
    }

    #[test]
    fn parse_chat_completion_chunk_accepts_openrouter_sparse_tool_call() {
        let payload = serde_json::json!({
            "id": "gen-1778509925-7119UkUjPTix9sGQ4vZf",
            "object": "chat.completion.chunk",
            "created": 1778509925,
            "model": "xiaomi/mimo-v2.5-pro-20260422",
            "provider": "Xiaomi",
            "choices": [{
                "index": 0,
                "delta": {
                    "content": null,
                    "role": "assistant",
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": "{\"file_path\": \"/home/luke/.claude/projects/-home-luke-projects-demo/memory/smb_clone.md\"}"
                        }
                    }]
                },
                "finish_reason": null,
                "native_finish_reason": null
            }]
        })
        .to_string();

        let chunk = super::parse_chat_completion_chunk(&payload).expect("parse chunk");
        let tool_call = &chunk.choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tool_call.kind, "function");
        assert_eq!(tool_call.index, Some(0));
        assert_eq!(
            tool_call
                .function
                .arguments
                .as_ref()
                .and_then(Value::as_str),
            Some(
                "{\"file_path\": \"/home/luke/.claude/projects/-home-luke-projects-demo/memory/smb_clone.md\"}"
            )
        );
    }

    fn ordered_object(entries: Vec<(&str, Value)>) -> Value {
        let mut map = serde_json::Map::new();
        for (key, value) in entries {
            map.insert(key.to_string(), value);
        }
        Value::Object(map)
    }

    #[test]
    fn sanitize_chat_request_canonicalizes_preserve_order_sensitive_json_values() {
        let make_request = |reverse: bool| {
            let prop_a = ordered_object(vec![("type", Value::String("string".to_string()))]);
            let prop_z = ordered_object(vec![("type", Value::String("number".to_string()))]);
            let properties = if reverse {
                ordered_object(vec![("z", prop_z.clone()), ("a", prop_a.clone())])
            } else {
                ordered_object(vec![("a", prop_a.clone()), ("z", prop_z.clone())])
            };
            let parameters = if reverse {
                ordered_object(vec![
                    ("required", serde_json::json!(["a", "z"])),
                    ("type", Value::String("object".to_string())),
                    ("properties", properties),
                ])
            } else {
                ordered_object(vec![
                    ("type", Value::String("object".to_string())),
                    ("properties", properties),
                    ("required", serde_json::json!(["a", "z"])),
                ])
            };
            let tool_choice = if reverse {
                ordered_object(vec![
                    (
                        "function",
                        ordered_object(vec![("name", Value::String("lookup".to_string()))]),
                    ),
                    ("type", Value::String("function".to_string())),
                ])
            } else {
                ordered_object(vec![
                    ("type", Value::String("function".to_string())),
                    (
                        "function",
                        ordered_object(vec![("name", Value::String("lookup".to_string()))]),
                    ),
                ])
            };
            let message_content = if reverse {
                ordered_object(vec![
                    ("z", Value::String("last".to_string())),
                    ("a", Value::String("first".to_string())),
                ])
            } else {
                ordered_object(vec![
                    ("a", Value::String("first".to_string())),
                    ("z", Value::String("last".to_string())),
                ])
            };
            let tool_arguments = if reverse {
                ordered_object(vec![
                    ("z", Value::String("last".to_string())),
                    ("a", Value::String("first".to_string())),
                ])
            } else {
                ordered_object(vec![
                    ("a", Value::String("first".to_string())),
                    ("z", Value::String("last".to_string())),
                ])
            };
            let mut extra_body = BTreeMap::new();
            extra_body.insert(
                "chat_template_kwargs".to_string(),
                if reverse {
                    ordered_object(vec![
                        ("enable_thinking", Value::Bool(true)),
                        ("clear_thinking", Value::Bool(false)),
                    ])
                } else {
                    ordered_object(vec![
                        ("clear_thinking", Value::Bool(false)),
                        ("enable_thinking", Value::Bool(true)),
                    ])
                },
            );
            ChatCompletionRequest {
                model: "grok-4".to_string(),
                messages: vec![
                    ChatMessage {
                        role: "user".to_string(),
                        content: Some(message_content),
                        tool_call_id: None,
                        name: None,
                        reasoning_content: None,
                        thinking: None,
                        tool_calls: None,
                    },
                    ChatMessage {
                        role: "assistant".to_string(),
                        content: None,
                        tool_call_id: None,
                        name: None,
                        reasoning_content: None,
                        thinking: None,
                        tool_calls: Some(vec![ChatToolCall {
                            id: Some("call_1".to_string()),
                            index: Some(0),
                            kind: "function".to_string(),
                            function: ChatFunctionCall {
                                name: Some("lookup".to_string()),
                                arguments: Some(tool_arguments),
                            },
                        }]),
                    },
                ],
                stream: true,
                tools: Some(vec![ChatTool {
                    kind: "function".to_string(),
                    function: ChatToolDefinition {
                        name: "lookup".to_string(),
                        description: String::new(),
                        parameters: Some(parameters),
                        strict: false,
                    },
                }]),
                tool_choice: Some(tool_choice),
                parallel_tool_calls: false,
                reasoning_effort: None,
                response_format: None,
                stream_options: None,
                temperature: None,
                top_p: None,
                max_output_tokens: None,
                frequency_penalty: None,
                presence_penalty: None,
                stop: None,
                extra_body,
            }
        };

        let canonical_a = sanitize_chat_request(make_request(false), true);
        let canonical_b = sanitize_chat_request(make_request(true), true);

        assert_eq!(
            serde_json::to_string(&canonical_a).expect("serialize canonical request A"),
            serde_json::to_string(&canonical_b).expect("serialize canonical request B"),
            "preserve_order-sensitive Value insertion order must not leak to upstream bytes"
        );
        let body = serde_json::to_string(&canonical_a).expect("serialize body");
        assert!(body.contains(r#""parameters":{"properties":{"a":"#));
        assert!(body.contains(r#""chat_template_kwargs":{"clear_thinking":false,"enable_thinking":true}"#));
        assert!(body.contains(r#""arguments":"{\"a\":\"first\",\"z\":\"last\"}""#));
    }

    #[test]
    fn sanitize_chat_request_clears_auto_tool_choice_and_preserves_reasoning() {
        let request = ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Some(Value::String("hello".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            stream: true,
            tools: None,
            tool_choice: Some(Value::String("auto".to_string())),
            parallel_tool_calls: false,
            reasoning_effort: Some("high".to_string()),
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized = sanitize_chat_request(request, true);

        assert_eq!(sanitized.reasoning_effort, Some("high".to_string()));
        assert_eq!(sanitized.tool_choice, None);
    }

    #[test]
    fn test_sanitize_clears_auto_when_no_tools() {
        let request = ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Some(Value::String("hi".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            stream: true,
            tools: Some(Vec::new()),
            tool_choice: Some(Value::String("auto".to_string())),
            parallel_tool_calls: false,
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized = sanitize_chat_request(request, true);
        assert_eq!(sanitized.tool_choice, None);
    }

    #[test]
    fn test_sanitize_preserves_none_and_required_without_tools() {
        let make = |tc: &str| ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Some(Value::String("hi".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            stream: true,
            tools: None,
            tool_choice: Some(Value::String(tc.to_string())),
            parallel_tool_calls: false,
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized_none = sanitize_chat_request(make("none"), true);
        assert_eq!(
            sanitized_none.tool_choice,
            Some(Value::String("none".to_string()))
        );

        let sanitized_required = sanitize_chat_request(make("required"), true);
        assert_eq!(
            sanitized_required.tool_choice,
            Some(Value::String("required".to_string()))
        );
    }

    #[test]
    fn test_sanitize_preserves_reasoning_effort() {
        let request = ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Some(Value::String("hi".to_string())),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            }],
            stream: true,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: false,
            reasoning_effort: Some("high".to_string()),
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized = sanitize_chat_request(request, true);
        assert_eq!(sanitized.reasoning_effort, Some("high".to_string()));
    }

    #[test]
    fn sanitize_chat_request_stringifies_structured_message_content_and_tool_args() {
        let request = ChatCompletionRequest {
            model: "grok-4".to_string(),
            messages: vec![ChatMessage {
                role: "assistant".to_string(),
                content: Some(serde_json::json!([
                    { "type": "text", "text": "hello" },
                    { "type": "text", "text": "world" }
                ])),
                tool_call_id: None,
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: Some(vec![crate::models::chat::ChatToolCall {
                    id: Some("call_1".to_string()),
                    index: Some(0),
                    kind: "function".to_string(),
                    function: crate::models::chat::ChatFunctionCall {
                        name: Some("echo".to_string()),
                        arguments: Some(serde_json::json!({ "value": "hi" })),
                    },
                }]),
            }],
            stream: true,
            tools: Some(Vec::new()),
            tool_choice: Some(Value::String("auto".to_string())),
            parallel_tool_calls: false,
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        };

        let sanitized = sanitize_chat_request(request, true);

        assert_eq!(
            sanitized.messages[0].content,
            Some(Value::String("hello\nworld".to_string()))
        );
        assert_eq!(
            sanitized.messages[0]
                .tool_calls
                .as_ref()
                .expect("tool calls")[0]
                .function
                .arguments,
            Some(Value::String("{\"value\":\"hi\"}".to_string()))
        );
    }

    #[test]
    fn sanitize_null_content() {
        assert_eq!(super::sanitize_message_content(Value::Null, true), None);
    }

    #[test]
    fn sanitize_non_string_content() {
        let result = super::sanitize_message_content(Value::Bool(true), true);
        assert_eq!(result, Some(Value::String("true".to_string())));
    }

    #[test]
    fn flatten_content_parts_non_text() {
        let parts = vec![serde_json::json!({"image": "data"})];
        let result = super::flatten_content_parts(&parts);
        assert!(result.contains("image"));
        assert!(result.contains("data"));
    }

    #[test]
    fn endpoint_url_bare_domain() {
        let client = ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            url::Url::parse("https://api.example.com").expect("url"),
            None,
            None,
            true,
        );
        let url = client.endpoint_url("chat/completions").expect("endpoint");
        assert_eq!(url.as_str(), "https://api.example.com/chat/completions");
    }

    #[tokio::test]
    async fn upstream_request_logger_writes_jsonl() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-upstream-log-{}.jsonl",
            uuid::Uuid::new_v4().simple()
        ));
        let logger = UpstreamRequestLogger::new(path.clone());
        let request = sanitize_chat_request(
            ChatCompletionRequest {
                model: "grok-4".to_string(),
                messages: vec![ChatMessage {
                    role: "user".to_string(),
                    content: Some(serde_json::json!([
                        { "type": "text", "text": "hello" },
                        { "type": "text", "text": "world" }
                    ])),
                    tool_call_id: None,
                    name: None,
                    reasoning_content: None,
                    thinking: None,
                    tool_calls: None,
                }],
                stream: true,
                tools: None,
                tool_choice: Some(Value::String("auto".to_string())),
                parallel_tool_calls: false,
                reasoning_effort: Some("high".to_string()),
                response_format: None,
                stream_options: None,
                temperature: None,
                top_p: None,
                max_output_tokens: None,
                frequency_penalty: None,
                presence_penalty: None,
                stop: None,
                extra_body: BTreeMap::new(),
            },
            true,
        );

        logger.log(&request).await.expect("write request log");

        let contents = std::fs::read_to_string(&path).expect("read request log");
        assert_eq!(
            contents,
            format!(
                "{}\n",
                serde_json::to_string(&request).expect("serialize request")
            )
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_truncate_short_body_unchanged() {
        assert_eq!(super::truncate_for_error("hello", 500), "hello");
    }

    #[test]
    fn test_truncate_long_body() {
        let long = "x".repeat(1000);
        let result = super::truncate_for_error(&long, 500);
        assert!(result.ends_with("...[truncated]"));
        assert_eq!(result.len(), 500 + "...[truncated]".len());
    }

    #[test]
    fn test_truncate_unicode_safe() {
        let base = "héllo wörld ";
        let repeated: String = base.repeat(100);
        let result = super::truncate_for_error(&repeated, 50);
        assert!(result.ends_with("...[truncated]"));
        // Verify truncation happened at a char boundary by checking it's valid UTF-8
        assert_eq!(result, result.to_string());
        let prefix = result.trim_end_matches("...[truncated]");
        assert_eq!(prefix.chars().count(), 50);
    }

    #[test]
    fn test_truncate_exact_boundary() {
        let exact = "a".repeat(500);
        assert_eq!(super::truncate_for_error(&exact, 500), exact);
    }

    #[test]
    fn test_sanitize_preserves_array_when_flatten_disabled() {
        let array = serde_json::json!([
            { "type": "text", "text": "hello" },
            { "type": "image_url", "image_url": { "url": "data:image/png;base64,abc" } }
        ]);
        let result = super::sanitize_message_content(array.clone(), false);
        assert_eq!(result, Some(array));
    }

    #[test]
    fn test_sanitize_flattens_array_when_flatten_enabled() {
        let array = serde_json::json!([
            { "type": "text", "text": "hello" },
            { "type": "text", "text": "world" }
        ]);
        let result = super::sanitize_message_content(array, true);
        assert_eq!(result, Some(Value::String("hello\nworld".to_string())));
    }

    #[test]
    fn test_sanitize_preserves_multimodal_array_when_flatten_enabled() {
        let array = serde_json::json!([
            { "type": "text", "text": "look" },
            { "type": "image_url", "image_url": { "url": "data:image/png;base64,abc" } }
        ]);
        let result = super::sanitize_message_content(array.clone(), true);
        assert_eq!(result, Some(array));
    }

    #[test]
    fn test_sanitize_non_array_unchanged_regardless() {
        let text = Value::String("hello".to_string());
        assert_eq!(
            super::sanitize_message_content(text.clone(), true),
            Some(text.clone())
        );
        assert_eq!(
            super::sanitize_message_content(text.clone(), false),
            Some(text)
        );
    }

    #[test]
    fn extract_supported_model_ids_reads_standard_models_list() {
        let body = serde_json::json!({
            "object": "list",
            "data": [
                {"id": "glm-5.1"},
                {"id": "Qwen3.5"},
                "grok-4"
            ]
        });

        assert_eq!(
            extract_supported_model_ids(&body),
            vec!["glm-5.1", "Qwen3.5", "grok-4"]
        );
    }

    #[test]
    fn extract_supported_model_ids_reads_models_array() {
        let body = serde_json::json!({"models": ["glm-5.1"]});
        assert_eq!(extract_supported_model_ids(&body), vec!["glm-5.1"]);
    }

    #[test]
    fn derive_tokenize_url_strips_trailing_v1() {
        let cases = [
            ("http://host:8000/v1", "http://host:8000/tokenize"),
            ("http://host:8000/v1/", "http://host:8000/tokenize"),
            ("http://host:8000", "http://host:8000/tokenize"),
            ("http://host:8000/", "http://host:8000/tokenize"),
            (
                "http://host:8000/openai/v1",
                "http://host:8000/openai/tokenize",
            ),
        ];
        for (base, expected) in cases {
            let url = super::derive_tokenize_url(&base.parse().expect("base url"));
            assert_eq!(url.as_str(), expected, "base {base}");
        }
    }

    #[tokio::test]
    async fn reqwest_count_tokens_reads_count_from_root_tokenize() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/tokenize"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "count": 123, "max_model_len": 8192 })),
            )
            .mount(&server)
            .await;

        let client = ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            format!("{}/v1", server.uri()).parse().expect("url"),
            None,
            None,
            true,
        );

        let body = serde_json::json!({ "model": "m", "messages": [] });
        let count = client.count_tokens(&body).await.expect("ok");
        assert_eq!(count, Some(123));
    }

    #[tokio::test]
    async fn reqwest_count_tokens_returns_none_on_non_2xx() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/tokenize"))
            .respond_with(wiremock::ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = ReqwestUpstreamClient::new(
            reqwest::Client::new(),
            format!("{}/v1", server.uri()).parse().expect("url"),
            None,
            None,
            true,
        );

        let count = client
            .count_tokens(&serde_json::json!({ "model": "m", "messages": [] }))
            .await
            .expect("ok");
        assert_eq!(count, None);
    }
}
