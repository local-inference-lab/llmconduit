mod access;
pub mod keys;
pub mod policy;
pub mod session;
pub mod store;

use crate::config::{AuthConfig, AuthMode};
use axum::http::HeaderMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use keys::{AuthPepper, digest_matches, generate_api_key, key_prefix};
pub use policy::{
    AuthError, AuthRequestId, Endpoint, LimitSet, ManagementPermission, PolicyBinding,
    PolicyDecision, PolicyEffect, PolicyIdentity, PolicyMatcher, PolicyRule, PolicyScope,
    PolicySnapshot, PolicySubject, UtcWindow,
};
pub use session::{SessionLease, SessionLimiter};
pub use store::{ApiKeySummary, CreatedApiKey};

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub auth_request_id: String,
    pub key_id: String,
    pub principal_id: String,
    identity: PolicyIdentity,
    policy: Arc<PolicySnapshot>,
    usage_admission: Option<UsageAdmission>,
    capture_payloads: bool,
}

impl AuthContext {
    pub fn management_actor(&self) -> crate::dashboard_access::ManagementActor {
        if self.key_id == "key_bootstrap" {
            return crate::dashboard_access::ManagementActor::Bootstrap;
        }
        let permissions = self
            .policy
            .management_permissions(&self.identity, chrono::Utc::now())
            .into_iter()
            .filter_map(access::wire_permission)
            .collect::<Vec<_>>();
        crate::dashboard_access::ManagementActor::Delegated {
            session_id: self.auth_request_id.clone(),
            principal_id: self.principal_id.clone(),
            key_id: self.key_id.clone(),
            permissions: permissions.into(),
        }
    }

    pub fn allows_endpoint(&self, endpoint: &str) -> bool {
        Endpoint::parse(endpoint).is_some_and(|endpoint| {
            self.policy
                .permits_endpoint(&self.identity, endpoint, chrono::Utc::now())
        })
    }

    pub fn allows_model(&self, endpoint: &str, model: &str) -> bool {
        Endpoint::parse(endpoint).is_some_and(|endpoint| {
            self.policy
                .authorize(&self.identity, endpoint, Some(model), chrono::Utc::now())
                .is_ok()
        })
    }

    pub fn authorization_scope(
        &self,
        endpoint: crate::upstream::InferenceEndpoint,
        requested_model: &str,
    ) -> Result<crate::upstream::AuthorizationScope, AuthError> {
        let policy_endpoint = inference_endpoint(endpoint);
        let scope = self.policy.authorize(
            &self.identity,
            policy_endpoint,
            Some(requested_model),
            chrono::Utc::now(),
        )?;
        Ok(crate::upstream::AuthorizationScope::restricted(
            move |provider_id, route_id, served_model, candidate_endpoint| {
                inference_endpoint(candidate_endpoint) == policy_endpoint
                    && scope.allows_candidate(Some(provider_id), route_id, Some(served_model))
            },
        ))
    }

    pub fn effective_limits(&self) -> LimitSet {
        self.policy
            .effective_limits(&self.identity, chrono::Utc::now())
    }

    pub fn capture_payloads(&self) -> bool {
        self.capture_payloads
    }

    pub(crate) fn record_usage(&self, event: crate::usage_accounting::UsageEvent) {
        if let Some(admission) = &self.usage_admission {
            admission.submit(event);
        }
    }
}

struct Inner {
    pepper: AuthPepper,
    store: Arc<std::sync::Mutex<store::AuthStore>>,
    authority: RwLock<Arc<store::StoredAuthority>>,
    effective_prices: RwLock<HashMap<String, crate::config::ModelPrice>>,
    session_limiter: SessionLimiter,
    session_persistence: SessionPersistence,
    usage_persistence: UsagePersistence,
}

const SESSION_PERSISTENCE_QUEUE_CAPACITY: usize = 1024;
const USAGE_PERSISTENCE_CAPACITY: usize = 1024;

enum SessionPersistenceCommand {
    Insert {
        session_id: String,
        key_id: String,
        result: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    Delete(String),
}

#[derive(Clone)]
struct SessionPersistence {
    sender: std::sync::mpsc::SyncSender<SessionPersistenceCommand>,
}

impl SessionPersistence {
    fn start(store: Arc<std::sync::Mutex<store::AuthStore>>) -> Result<Self, String> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(SESSION_PERSISTENCE_QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("llmconduit-auth-sessions".into())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    match command {
                        SessionPersistenceCommand::Insert {
                            session_id,
                            key_id,
                            result,
                        } => {
                            let inserted = store
                                .lock()
                                .map_err(|_| "auth store lock poisoned".to_string())
                                .and_then(|mut store| {
                                    store.insert_session(&session_id, &key_id)
                                });
                            let _ = result.send(inserted);
                        }
                        SessionPersistenceCommand::Delete(session_id) => {
                            let deleted = store
                                .lock()
                                .map_err(|_| "auth store lock poisoned".to_string())
                                .and_then(|mut store| store.delete_session(&session_id));
                            if let Err(error) = deleted {
                                tracing::warn!(%session_id, %error, "failed to persist session release");
                            }
                        }
                    }
                }
            })
            .map_err(|error| format!("failed to start auth session persistence worker: {error}"))?;
        Ok(Self { sender })
    }

    async fn insert(&self, session_id: String, key_id: String) -> Result<(), AuthError> {
        let (result, result_rx) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(SessionPersistenceCommand::Insert {
                session_id,
                key_id,
                result,
            })
            .map_err(|_| AuthError::PolicyUnavailable)?;
        result_rx
            .await
            .map_err(|_| AuthError::PolicyUnavailable)?
            .map_err(|_| AuthError::PolicyUnavailable)
    }

    fn delete(&self, session_id: &str) {
        if self
            .sender
            .try_send(SessionPersistenceCommand::Delete(session_id.to_owned()))
            .is_err()
        {
            tracing::warn!(%session_id, "auth session release queue is unavailable or full");
        }
    }
}

struct UsagePersistenceCommand {
    event: crate::usage_accounting::UsageEvent,
    _capacity: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Clone)]
struct UsagePersistence {
    sender: std::sync::mpsc::Sender<UsagePersistenceCommand>,
    capacity: Arc<tokio::sync::Semaphore>,
}

impl UsagePersistence {
    fn start(store: Arc<std::sync::Mutex<store::AuthStore>>) -> Result<Self, String> {
        let (sender, receiver) = std::sync::mpsc::channel::<UsagePersistenceCommand>();
        std::thread::Builder::new()
            .name("llmconduit-auth-usage".into())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    let result = store
                        .lock()
                        .map_err(|_| "auth store lock poisoned".to_string())
                        .and_then(|store| store.record_usage_once(&command.event));
                    if let Err(error) = result {
                        tracing::error!(%error, "failed to persist authenticated usage event");
                    }
                }
            })
            .map_err(|error| format!("failed to start auth usage persistence worker: {error}"))?;
        Ok(Self {
            sender,
            capacity: Arc::new(tokio::sync::Semaphore::new(USAGE_PERSISTENCE_CAPACITY)),
        })
    }

    fn admit(&self) -> Result<UsageAdmission, AuthFailure> {
        let capacity = Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| AuthFailure::Unavailable)?;
        Ok(UsageAdmission {
            inner: Arc::new(UsageAdmissionInner {
                sender: self.sender.clone(),
                capacity: std::sync::Mutex::new(Some(capacity)),
                submitted: AtomicBool::new(false),
            }),
        })
    }
}

#[derive(Clone)]
struct UsageAdmission {
    inner: Arc<UsageAdmissionInner>,
}

impl std::fmt::Debug for UsageAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UsageAdmission")
            .field("submitted", &self.inner.submitted.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

struct UsageAdmissionInner {
    sender: std::sync::mpsc::Sender<UsagePersistenceCommand>,
    capacity: std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    submitted: AtomicBool,
}

impl UsageAdmission {
    fn submit(&self, event: crate::usage_accounting::UsageEvent) {
        if self
            .inner
            .submitted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let capacity = self
            .inner
            .capacity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(capacity) = capacity else {
            tracing::error!("authenticated usage admission lost its capacity permit");
            return;
        };
        if self
            .inner
            .sender
            .send(UsagePersistenceCommand {
                event,
                _capacity: capacity,
            })
            .is_err()
        {
            tracing::error!("authenticated usage persistence worker is unavailable");
        }
    }
}

#[derive(Clone, Default)]
pub struct AuthzService {
    inner: Option<Arc<Inner>>,
}

impl std::fmt::Debug for AuthzService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthzService")
            .field("enabled", &self.inner.is_some())
            .finish()
    }
}

impl AuthzService {
    pub fn from_config(config: &AuthConfig) -> Result<Self, String> {
        if config.mode == AuthMode::Disabled {
            return Ok(Self::default());
        }
        let pepper = std::env::var("LLMCONDUIT_AUTH_PEPPER")
            .map_err(|_| "auth.mode=enforce requires LLMCONDUIT_AUTH_PEPPER".to_string())?;
        let bootstrap = std::env::var("LLMCONDUIT_AUTH_BOOTSTRAP_KEY").ok();
        Self::open_enforced(config, pepper.into_bytes(), bootstrap.as_deref())
    }

    fn open_enforced(
        config: &AuthConfig,
        pepper: Vec<u8>,
        bootstrap: Option<&str>,
    ) -> Result<Self, String> {
        if config.store_path.as_os_str().is_empty() {
            return Err("auth.store_path must not be blank in enforce mode".into());
        }
        let pepper = AuthPepper::new(pepper).map_err(|err| err.to_string())?;
        let mut store = store::AuthStore::open(&config.store_path)?;
        if store.key_count()? == 0 {
            let raw = bootstrap
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    "auth store has no keys; set LLMCONDUIT_AUTH_BOOTSTRAP_KEY for first startup"
                        .to_string()
                })?;
            key_prefix(raw).map_err(|_| {
                "LLMCONDUIT_AUTH_BOOTSTRAP_KEY must be a valid llmc_ API key".to_string()
            })?;
            store.insert_bootstrap_key(raw, &pepper.digest(raw))?;
        }
        let authority = Arc::new(store.load_authority()?);
        let effective_prices = store.effective_prices()?;
        let store = Arc::new(std::sync::Mutex::new(store));
        let session_persistence = SessionPersistence::start(Arc::clone(&store))?;
        let usage_persistence = UsagePersistence::start(Arc::clone(&store))?;
        Ok(Self {
            inner: Some(Arc::new(Inner {
                pepper,
                store,
                authority: RwLock::new(authority),
                effective_prices: RwLock::new(effective_prices),
                session_limiter: SessionLimiter::default(),
                session_persistence,
                usage_persistence,
            })),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn enabled(&self) -> bool {
        self.is_enabled()
    }

    pub fn access_backend(self: &Arc<Self>) -> Arc<dyn crate::dashboard_access::AccessBackend> {
        Arc::clone(self) as Arc<dyn crate::dashboard_access::AccessBackend>
    }

    pub fn authenticate(&self, headers: &HeaderMap) -> Result<Option<AuthContext>, AuthFailure> {
        let Some(inner) = &self.inner else {
            return Ok(None);
        };
        let raw = presented_key(headers).ok_or(AuthFailure::Missing)?;
        key_prefix(raw).map_err(|_| AuthFailure::Invalid)?;
        let digest = inner.pepper.digest(raw);
        let authority = inner
            .authority
            .read()
            .map_err(|_| AuthFailure::Unavailable)?
            .clone();
        let mut matched = None;
        for credential in &authority.credentials {
            if digest_matches(&credential.digest, &digest) {
                matched = Some(credential);
            }
        }
        let credential = matched.ok_or(AuthFailure::Invalid)?;
        let usage_admission = inner.usage_persistence.admit()?;
        let request_id = AuthRequestId::new();
        let identity = PolicyIdentity {
            request_id: request_id.clone(),
            key_id: credential.id.clone(),
            key_prefix: credential.prefix.clone(),
            principal_id: credential.principal_id.clone(),
            policy_epoch: authority.epoch,
        };
        Ok(Some(AuthContext {
            auth_request_id: request_id.as_str().to_string(),
            key_id: identity.key_id.clone(),
            principal_id: identity.principal_id.clone(),
            identity,
            policy: Arc::clone(&authority.policy),
            usage_admission: Some(usage_admission),
            capture_payloads: credential.capture_payloads,
        }))
    }

    pub async fn acquire_session(
        &self,
        context: &AuthContext,
    ) -> Result<Option<SessionLease>, AuthError> {
        let Some(inner) = &self.inner else {
            return Ok(None);
        };
        let session_id = format!("ses_{}", uuid::Uuid::new_v4().simple());
        let session_persistence = inner.session_persistence.clone();
        let on_release: Arc<dyn Fn(&str) + Send + Sync> = Arc::new(move |session_id| {
            session_persistence.delete(session_id);
        });
        let lease = inner.session_limiter.acquire_for_key_with_release(
            &context.key_id,
            context.effective_limits(),
            chrono::Utc::now(),
            session_id.clone(),
            Some(on_release),
        )?;
        if inner
            .session_persistence
            .insert(session_id, context.key_id.clone())
            .await
            .is_err()
        {
            drop(lease);
            return Err(AuthError::PolicyUnavailable);
        }
        Ok(Some(lease))
    }

    pub async fn create_delegated_session(
        &self,
        context: &AuthContext,
        csrf_digest: &[u8],
        expires_at: i64,
    ) -> Result<crate::dashboard_access::ManagementActor, AuthError> {
        let service = self.clone();
        let context = context.clone();
        let csrf_digest = csrf_digest.to_vec();
        tokio::task::spawn_blocking(move || {
            service.create_delegated_session_blocking(&context, &csrf_digest, expires_at)
        })
        .await
        .map_err(|_| AuthError::PolicyUnavailable)?
    }

    fn create_delegated_session_blocking(
        &self,
        context: &AuthContext,
        csrf_digest: &[u8],
        expires_at: i64,
    ) -> Result<crate::dashboard_access::ManagementActor, AuthError> {
        let inner = self.inner.as_ref().ok_or(AuthError::PolicyUnavailable)?;
        let permissions = context
            .policy
            .management_permissions(&context.identity, chrono::Utc::now())
            .into_iter()
            .filter_map(access::wire_permission)
            .collect::<Vec<_>>();
        if permissions.is_empty() {
            return Err(AuthError::Forbidden);
        }
        let session = inner
            .store
            .lock()
            .map_err(|_| AuthError::PolicyUnavailable)?
            .create_dashboard_session(
                &context.principal_id,
                &context.key_id,
                csrf_digest,
                expires_at,
                context.identity.policy_epoch,
            )
            .map_err(|_| AuthError::PolicyUnavailable)?;
        Ok(crate::dashboard_access::ManagementActor::Delegated {
            session_id: session.session_id,
            principal_id: session.principal_id,
            key_id: session.key_id,
            permissions: permissions.into(),
        })
    }

    pub async fn authenticate_delegated_session(
        &self,
        session_id: &str,
    ) -> Result<Option<crate::dashboard_access::ManagementActor>, AuthError> {
        let service = self.clone();
        let session_id = session_id.to_owned();
        tokio::task::spawn_blocking(move || {
            service.authenticate_delegated_session_blocking(&session_id)
        })
        .await
        .map_err(|_| AuthError::PolicyUnavailable)?
    }

    fn authenticate_delegated_session_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<crate::dashboard_access::ManagementActor>, AuthError> {
        let Some(inner) = &self.inner else {
            return Ok(None);
        };
        let session = inner
            .store
            .lock()
            .map_err(|_| AuthError::PolicyUnavailable)?
            .load_dashboard_session(session_id)
            .map_err(|_| AuthError::PolicyUnavailable)?;
        let Some(session) = session else {
            return Ok(None);
        };
        let authority = inner
            .authority
            .read()
            .map_err(|_| AuthError::PolicyUnavailable)?
            .clone();
        let identity = PolicyIdentity {
            request_id: AuthRequestId::new(),
            key_id: session.key_id.clone(),
            key_prefix: session.key_prefix,
            principal_id: session.principal_id.clone(),
            policy_epoch: authority.epoch,
        };
        let permissions = authority
            .policy
            .management_permissions(&identity, chrono::Utc::now())
            .into_iter()
            .filter_map(access::wire_permission)
            .collect::<Vec<_>>();
        if permissions.is_empty() {
            let _ = inner
                .store
                .lock()
                .map_err(|_| AuthError::PolicyUnavailable)?
                .revoke_dashboard_session(session_id, "permission_removed");
            return Ok(None);
        }
        Ok(Some(crate::dashboard_access::ManagementActor::Delegated {
            session_id: session.session_id,
            principal_id: session.principal_id,
            key_id: session.key_id,
            permissions: permissions.into(),
        }))
    }

    /// Recover the inference identity represented by a live delegated dashboard
    /// session. Management authorization and inference authorization remain separate:
    /// callers still run the normal endpoint/model policy check on this context.
    pub async fn delegated_inference_context(
        &self,
        session_id: &str,
    ) -> Result<Option<AuthContext>, AuthError> {
        let service = self.clone();
        let session_id = session_id.to_owned();
        tokio::task::spawn_blocking(move || {
            service.delegated_inference_context_blocking(&session_id)
        })
        .await
        .map_err(|_| AuthError::PolicyUnavailable)?
    }

    fn delegated_inference_context_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<AuthContext>, AuthError> {
        let Some(inner) = &self.inner else {
            return Ok(None);
        };
        let session = inner
            .store
            .lock()
            .map_err(|_| AuthError::PolicyUnavailable)?
            .load_dashboard_session(session_id)
            .map_err(|_| AuthError::PolicyUnavailable)?;
        let Some(session) = session else {
            return Ok(None);
        };
        let authority = inner
            .authority
            .read()
            .map_err(|_| AuthError::PolicyUnavailable)?
            .clone();
        let capture_payloads = authority
            .credentials
            .iter()
            .find(|credential| credential.id == session.key_id)
            .is_some_and(|credential| credential.capture_payloads);
        let identity = PolicyIdentity {
            request_id: AuthRequestId::new(),
            key_id: session.key_id,
            key_prefix: session.key_prefix,
            principal_id: session.principal_id,
            policy_epoch: authority.epoch,
        };
        let usage_admission = inner
            .usage_persistence
            .admit()
            .map_err(|_| AuthError::PolicyUnavailable)?;
        Ok(Some(AuthContext {
            auth_request_id: identity.request_id.as_str().to_string(),
            key_id: identity.key_id.clone(),
            principal_id: identity.principal_id.clone(),
            identity,
            policy: Arc::clone(&authority.policy),
            usage_admission: Some(usage_admission),
            capture_payloads,
        }))
    }

    pub async fn revoke_delegated_session(&self, session_id: &str) -> Result<bool, String> {
        let service = self.clone();
        let session_id = session_id.to_owned();
        tokio::task::spawn_blocking(move || service.revoke_delegated_session_blocking(&session_id))
            .await
            .map_err(|error| format!("auth session worker failed: {error}"))?
    }

    fn revoke_delegated_session_blocking(&self, session_id: &str) -> Result<bool, String> {
        let inner = self.inner()?;
        inner
            .store
            .lock()
            .map_err(|_| "auth store lock poisoned".to_string())?
            .revoke_dashboard_session(session_id, "logout")
    }

    /// Verify the caller-provided digest of the delegated session's CSRF
    /// secret. The raw CSRF token never enters the authorization store.
    pub async fn verify_delegated_csrf_digest(
        &self,
        session_id: &str,
        presented_digest: &[u8],
    ) -> Result<bool, String> {
        let service = self.clone();
        let session_id = session_id.to_owned();
        let presented_digest = presented_digest.to_vec();
        tokio::task::spawn_blocking(move || {
            service.verify_delegated_csrf_digest_blocking(&session_id, &presented_digest)
        })
        .await
        .map_err(|error| format!("auth session worker failed: {error}"))?
    }

    fn verify_delegated_csrf_digest_blocking(
        &self,
        session_id: &str,
        presented_digest: &[u8],
    ) -> Result<bool, String> {
        let inner = self.inner()?;
        inner
            .store
            .lock()
            .map_err(|_| "auth store lock poisoned".to_string())?
            .verify_dashboard_csrf_digest(session_id, presented_digest)
    }

    pub fn create_key(
        &self,
        principal_name: &str,
        key_name: &str,
        endpoints: &[String],
        models: &[String],
    ) -> Result<CreatedApiKey, String> {
        let inner = self.inner()?;
        let generated = generate_api_key(&inner.pepper);
        let raw = generated.expose_once();
        let digest = inner.pepper.digest(&raw);
        let mut store = inner.store.lock().map_err(|_| "auth store lock poisoned")?;
        let created =
            store.create_key(principal_name, key_name, &raw, &digest, endpoints, models)?;
        self.reload_locked(inner, &store)?;
        Ok(created.with_raw(raw))
    }

    pub fn revoke_key(&self, key_id: &str) -> Result<bool, String> {
        let inner = self.inner()?;
        let mut store = inner.store.lock().map_err(|_| "auth store lock poisoned")?;
        let changed = store.revoke_key(key_id)?;
        if changed {
            self.reload_locked(inner, &store)?;
        }
        Ok(changed)
    }

    pub fn list_keys(&self) -> Result<Vec<ApiKeySummary>, String> {
        let inner = self.inner()?;
        inner
            .store
            .lock()
            .map_err(|_| "auth store lock poisoned".to_string())?
            .list_keys()
    }

    /// Persist one terminal inference event. Disabled auth is a no-op; enabled
    /// auth uses the same store mutex as key mutations so SQLite is never used
    /// concurrently from multiple runtime workers.
    pub fn record_usage_once(
        &self,
        event: &crate::usage_accounting::UsageEvent,
    ) -> Result<bool, String> {
        let Some(inner) = &self.inner else {
            return Ok(false);
        };
        inner
            .store
            .lock()
            .map_err(|_| "auth store lock poisoned".to_string())?
            .record_usage_once(event)
    }

    pub fn effective_price(&self, model: &str) -> Option<crate::config::ModelPrice> {
        let inner = self.inner.as_ref()?;
        let prices = inner.effective_prices.read().ok()?;
        prices.get(model).copied().or_else(|| {
            prices
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(model))
                .map(|(_, price)| *price)
        })
    }

    pub fn effective_price_table(&self) -> HashMap<String, crate::config::ModelPrice> {
        self.inner
            .as_ref()
            .and_then(|inner| {
                inner
                    .effective_prices
                    .read()
                    .ok()
                    .map(|prices| prices.clone())
            })
            .unwrap_or_default()
    }

    pub async fn sync_openrouter_pricing_models(
        &self,
        models: Vec<String>,
    ) -> Result<Vec<crate::dashboard_access::AccessPricingRow>, crate::dashboard_access::AccessError>
    {
        match self
            .sync_openrouter_pricing(
                crate::dashboard_access::ManagementActor::Bootstrap,
                crate::dashboard_access::SyncPricingRequest { models },
            )
            .await?
        {
            crate::dashboard_access::AccessResult::Pricing(pricing) => Ok(pricing),
            _ => unreachable!("pricing sync always returns the pricing projection"),
        }
    }

    fn refresh_effective_prices_locked(
        &self,
        inner: &Inner,
        store: &store::AuthStore,
    ) -> Result<(), String> {
        *inner
            .effective_prices
            .write()
            .map_err(|_| "effective pricing lock poisoned")? = store.effective_prices()?;
        Ok(())
    }

    async fn sync_openrouter_pricing(
        &self,
        actor: crate::dashboard_access::ManagementActor,
        request: crate::dashboard_access::SyncPricingRequest,
    ) -> Result<crate::dashboard_access::AccessResult, crate::dashboard_access::AccessError> {
        use axum::http::StatusCode;
        use futures::StreamExt;
        use std::collections::HashSet;

        self.inner().map_err(|message| {
            crate::dashboard_access::AccessError::new(StatusCode::SERVICE_UNAVAILABLE, message)
        })?;
        let management_key = std::env::var("OPENROUTER_API_KEY")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                crate::dashboard_access::AccessError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "OpenRouter pricing sync is not configured",
                )
            })?;
        if request.models.len() > 128 {
            return Err(crate::dashboard_access::AccessError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "pricing sync is limited to 128 models",
            ));
        }
        let mut seen = HashSet::new();
        let models = request
            .models
            .into_iter()
            .map(|model| model.trim().to_string())
            .filter(|model| !model.is_empty() && seen.insert(model.clone()))
            .collect::<Vec<_>>();
        if models.is_empty() {
            return Err(crate::dashboard_access::AccessError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "at least one OpenRouter model id is required",
            ));
        }
        let client = crate::openrouter_pricing::OpenRouterPricingClient::default();
        let mut snapshots = Vec::with_capacity(models.len());
        let management_key: Arc<str> = management_key.into();
        let mut fetches = futures::stream::iter(models.into_iter().map(|model| {
            let client = client.clone();
            let management_key = Arc::clone(&management_key);
            async move {
                let result = client.fetch_model(&model, &management_key).await;
                (model, result)
            }
        }))
        .buffer_unordered(8);
        while let Some((model, result)) = fetches.next().await {
            match result {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(err) => {
                    let service = self.clone();
                    let audit_actor = actor.clone();
                    let audit_model = model.clone();
                    let audit_error = err.to_string();
                    if let Err(join_error) = tokio::task::spawn_blocking(move || {
                        service.audit_pricing_sync_failure(&audit_actor, &audit_model, &audit_error)
                    })
                    .await
                    {
                        tracing::warn!(error = %join_error, "pricing sync failure audit worker failed");
                    }
                    let status = match &err {
                        crate::openrouter_pricing::PricingError::InvalidModelId
                        | crate::openrouter_pricing::PricingError::InvalidPrice(_)
                        | crate::openrouter_pricing::PricingError::PrecisionLoss(_)
                        | crate::openrouter_pricing::PricingError::InvalidResponse(_)
                        | crate::openrouter_pricing::PricingError::NoUsableEndpoints => {
                            StatusCode::UNPROCESSABLE_ENTITY
                        }
                        _ => StatusCode::BAD_GATEWAY,
                    };
                    return Err(crate::dashboard_access::AccessError::new(
                        status,
                        err.to_string(),
                    ));
                }
            }
        }
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.persist_synced_pricing(&actor, &snapshots))
            .await
            .map_err(|error| {
                crate::dashboard_access::AccessError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("pricing persistence worker failed: {error}"),
                )
            })?
    }

    fn audit_pricing_sync_failure(
        &self,
        actor: &crate::dashboard_access::ManagementActor,
        model: &str,
        error: &str,
    ) -> Result<(), crate::dashboard_access::AccessError> {
        use axum::http::StatusCode;

        let inner = self.inner().map_err(|message| {
            crate::dashboard_access::AccessError::new(StatusCode::SERVICE_UNAVAILABLE, message)
        })?;
        inner
            .store
            .lock()
            .map_err(|_| {
                crate::dashboard_access::AccessError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "auth store lock poisoned",
                )
            })?
            .audit_pricing_sync_failure(actor, model, error)
    }

    fn persist_synced_pricing(
        &self,
        actor: &crate::dashboard_access::ManagementActor,
        snapshots: &[crate::openrouter_pricing::OpenRouterPriceSnapshot],
    ) -> Result<crate::dashboard_access::AccessResult, crate::dashboard_access::AccessError> {
        use axum::http::StatusCode;

        let inner = self.inner().map_err(|message| {
            crate::dashboard_access::AccessError::new(StatusCode::SERVICE_UNAVAILABLE, message)
        })?;
        let mut store = inner.store.lock().map_err(|_| {
            crate::dashboard_access::AccessError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "auth store lock poisoned",
            )
        })?;
        for snapshot in snapshots {
            store.persist_imported_price(actor, snapshot)?;
        }
        self.refresh_effective_prices_locked(inner, &store)
            .map_err(|message| {
                crate::dashboard_access::AccessError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    message,
                )
            })?;
        store.dispatch_access(actor, crate::dashboard_access::AccessOperation::Pricing)
    }

    fn inner(&self) -> Result<&Arc<Inner>, String> {
        self.inner
            .as_ref()
            .ok_or_else(|| "inference auth is disabled".to_string())
    }

    fn reload_locked(&self, inner: &Inner, store: &store::AuthStore) -> Result<(), String> {
        let fresh = Arc::new(store.load_authority()?);
        *inner
            .authority
            .write()
            .map_err(|_| "auth snapshot lock poisoned")? = fresh;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthFailure {
    Missing,
    Invalid,
    Forbidden,
    Unavailable,
}

fn presented_key(headers: &HeaderMap) -> Option<&str> {
    let authorization = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(value) = authorization {
        if value.len() >= 7 && value[..7].eq_ignore_ascii_case("bearer ") {
            return Some(value[7..].trim()).filter(|value| !value.is_empty());
        }
        return Some(value);
    }
    headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn inference_endpoint(endpoint: crate::upstream::InferenceEndpoint) -> Endpoint {
    match endpoint {
        crate::upstream::InferenceEndpoint::Responses => Endpoint::Responses,
        crate::upstream::InferenceEndpoint::ChatCompletions => Endpoint::ChatCompletions,
        crate::upstream::InferenceEndpoint::Messages => Endpoint::Messages,
        crate::upstream::InferenceEndpoint::CountTokens => Endpoint::CountTokens,
        crate::upstream::InferenceEndpoint::Completions => Endpoint::Completions,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuthContext, AuthFailure, AuthRequestId, AuthzService, Endpoint, LimitSet, PolicyBinding,
        PolicyEffect, PolicyIdentity, PolicyMatcher, PolicyRule, PolicySnapshot, PolicySubject,
        USAGE_PERSISTENCE_CAPACITY,
    };
    use crate::config::{AuthConfig, AuthMode};
    use crate::dashboard_access::{AccessBackend, AccessOperation, AccessResult, ManagementActor};
    use axum::http::{HeaderMap, HeaderValue};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    #[tokio::test]
    async fn key_lifecycle_uses_rich_snapshot_and_revokes_immediately() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-rich-auth-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let config = AuthConfig {
            mode: AuthMode::Enforce,
            store_path: path.clone(),
        };
        let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
        let service =
            AuthzService::open_enforced(&config, b"unit-test-pepper".to_vec(), Some(&bootstrap))
                .unwrap();
        let created = service
            .create_key(
                "reporting",
                "reporting",
                &["chat".into()],
                &["public-*".into()],
            )
            .unwrap();
        let raw = created.raw_key.as_deref().unwrap().to_string();
        assert!(!format!("{created:?}").contains(&raw));
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(&raw).unwrap());
        let context = service.authenticate(&headers).unwrap().unwrap();
        assert!(context.allows_model("chat", "public-v1"));
        assert!(!context.allows_model("chat", "secret-v1"));
        assert!(!context.allows_endpoint("responses"));
        assert!(service.acquire_session(&context).await.unwrap().is_some());
        assert!(service.revoke_key(&created.summary.id).unwrap());
        assert!(service.authenticate(&headers).is_err());
        drop(service);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn delegated_dashboard_sessions_verify_csrf_and_revoke_immediately() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-dashboard-session-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
        let service = AuthzService::open_enforced(
            &AuthConfig {
                mode: AuthMode::Enforce,
                store_path: path.clone(),
            },
            b"dashboard-session-pepper".to_vec(),
            Some(&bootstrap),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(&bootstrap).unwrap());
        let context = service.authenticate(&headers).unwrap().unwrap();
        let csrf_digest = [7u8; 32];
        let actor = service
            .create_delegated_session(&context, &csrf_digest, chrono::Utc::now().timestamp() + 300)
            .await
            .unwrap();
        let session_id = match actor {
            ManagementActor::Delegated { session_id, .. } => session_id,
            ManagementActor::Bootstrap => panic!("delegated session returned bootstrap actor"),
        };
        assert!(
            service
                .verify_delegated_csrf_digest(&session_id, &csrf_digest)
                .await
                .unwrap()
        );
        assert!(
            !service
                .verify_delegated_csrf_digest(&session_id, &[8u8; 32])
                .await
                .unwrap()
        );
        assert!(
            service
                .authenticate_delegated_session(&session_id)
                .await
                .unwrap()
                .is_some()
        );
        let delegated_context = service
            .delegated_inference_context(&session_id)
            .await
            .unwrap()
            .expect("live delegated session retains its inference identity");
        assert!(delegated_context.allows_model("chat", "any-model"));
        assert!(service.revoke_delegated_session(&session_id).await.unwrap());
        assert!(
            service
                .authenticate_delegated_session(&session_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            service
                .delegated_inference_context(&session_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !service
                .verify_delegated_csrf_digest(&session_id, &csrf_digest)
                .await
                .unwrap()
        );
        drop(service);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn requested_alias_and_served_backend_are_authorized_in_separate_dimensions() {
        let identity = PolicyIdentity {
            request_id: AuthRequestId::new(),
            key_id: "key_alias".into(),
            key_prefix: "llmc_alias".into(),
            principal_id: "usr_alias".into(),
            policy_epoch: 1,
        };
        let rule = PolicyRule {
            id: "allow-alias-remap".into(),
            effect: PolicyEffect::Allow,
            binding: PolicyBinding {
                subject: PolicySubject::Principal(identity.principal_id.clone()),
            },
            matcher: PolicyMatcher::new(
                [Endpoint::ChatCompletions],
                ["public-alias".into()],
                ["provider/backend-model".into()],
                ["provider-a".into()],
                Vec::<String>::new(),
            )
            .unwrap(),
            windows: Vec::new(),
            limits: LimitSet::default(),
            management_permissions: HashSet::new(),
        };
        let policy = Arc::new(PolicySnapshot::new(
            1,
            vec![rule],
            HashMap::new(),
            HashMap::new(),
        ));
        let context = AuthContext {
            auth_request_id: identity.request_id.as_str().to_string(),
            key_id: identity.key_id.clone(),
            principal_id: identity.principal_id.clone(),
            identity,
            policy,
            usage_admission: None,
            capture_payloads: false,
        };

        let scope = context
            .authorization_scope(
                crate::upstream::InferenceEndpoint::ChatCompletions,
                "public-alias",
            )
            .expect("the original requested alias is authorized");

        assert!(scope.allows_candidate(
            "provider-a",
            None,
            "provider/backend-model",
            crate::upstream::InferenceEndpoint::ChatCompletions,
        ));
        assert!(!scope.allows_candidate(
            "provider-a",
            None,
            "public-alias",
            crate::upstream::InferenceEndpoint::ChatCompletions,
        ));
        assert!(!scope.allows_candidate(
            "provider-b",
            None,
            "provider/backend-model",
            crate::upstream::InferenceEndpoint::ChatCompletions,
        ));
    }

    #[tokio::test]
    async fn cancelled_session_load_releases_persistence_without_blocking_runtime() {
        const SESSION_COUNT: usize = 128;

        let path = std::env::temp_dir().join(format!(
            "llmconduit-session-load-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
        let service = AuthzService::open_enforced(
            &AuthConfig {
                mode: AuthMode::Enforce,
                store_path: path.clone(),
            },
            b"session-load-pepper".to_vec(),
            Some(&bootstrap),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(&bootstrap).unwrap());
        let context = service.authenticate(&headers).unwrap().unwrap();
        let ready = Arc::new(tokio::sync::Barrier::new(SESSION_COUNT + 1));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..SESSION_COUNT {
            let service = service.clone();
            let context = context.clone();
            let ready = Arc::clone(&ready);
            tasks.spawn(async move {
                let _lease = service.acquire_session(&context).await.unwrap().unwrap();
                ready.wait().await;
                std::future::pending::<()>().await;
            });
        }
        ready.wait().await;

        assert_eq!(session_count(&service).await, SESSION_COUNT);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}

        for _ in 0..100 {
            if session_count(&service).await == 0 {
                drop(service);
                let _ = std::fs::remove_file(path);
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("cancelled leases were not drained by bounded persistence worker");
    }

    #[test]
    fn usage_capacity_fails_closed_before_admitting_another_request() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-usage-capacity-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
        let service = AuthzService::open_enforced(
            &AuthConfig {
                mode: AuthMode::Enforce,
                store_path: path.clone(),
            },
            b"usage-capacity-pepper".to_vec(),
            Some(&bootstrap),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(&bootstrap).unwrap());

        let admitted = (0..USAGE_PERSISTENCE_CAPACITY)
            .map(|_| service.authenticate(&headers).unwrap().unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(
            service.authenticate(&headers),
            Err(AuthFailure::Unavailable)
        ));
        drop(admitted);
        assert!(service.authenticate(&headers).unwrap().is_some());
        drop(service);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn admitted_usage_is_queued_once_without_blocking_tokio() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-usage-runtime-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
        let service = AuthzService::open_enforced(
            &AuthConfig {
                mode: AuthMode::Enforce,
                store_path: path.clone(),
            },
            b"usage-runtime-pepper".to_vec(),
            Some(&bootstrap),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(&bootstrap).unwrap());
        let context = service.authenticate(&headers).unwrap().unwrap();
        let event = crate::usage_accounting::UsageEvent {
            auth_request_id: context.auth_request_id.clone(),
            api_call_id: None,
            key_id: context.key_id.clone(),
            principal_id: context.principal_id.clone(),
            endpoint: "completions".into(),
            requested_model: Some("requested".into()),
            served_model: Some("served".into()),
            provider: Some("primary".into()),
            route: None,
            status: "cancelled".into(),
            usage: None,
            charge: None,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let store = Arc::clone(&service.inner().unwrap().store);
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let blocker = std::thread::spawn(move || {
            let _guard = store.lock().unwrap();
            locked_tx.send(()).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(150));
        });
        locked_rx.recv().unwrap();

        let started = std::time::Instant::now();
        context.record_usage(event.clone());
        context.clone().record_usage(event);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            started.elapsed() < std::time::Duration::from_millis(75),
            "usage persistence blocked the current-thread runtime"
        );
        blocker.join().unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let count = rusqlite::Connection::open(&path)
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM auth_usage_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap();
                if count == 1 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("admitted usage persisted exactly once");
        drop(context);
        drop(service);
        let _ = std::fs::remove_file(path);
    }

    async fn session_count(service: &AuthzService) -> usize {
        match AccessBackend::dispatch(
            service,
            &ManagementActor::Bootstrap,
            AccessOperation::ListSessions,
        )
        .await
        .unwrap()
        {
            AccessResult::Sessions(sessions) => sessions.len(),
            _ => panic!("unexpected session-list result"),
        }
    }
}
