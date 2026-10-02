#![allow(dead_code)]

use crate::config::MeshControllerConfig;
use crate::error::{AppError, AppResult};
use crate::mesh::auth::MeshAuthorizer;
use crate::mesh::identity::load_or_create;
use crate::mesh::protocol::{
    ENROLL_ALPN, EnrollRequest, EnrollResponse, HubToWorker, PROTOCOL_VERSION, WORKER_ALPN,
    WorkerToHub, read_control, sanitize_context_limit, validate_enroll_request, validate_heartbeat,
    validate_model_catalog, validate_model_switching, validate_resource_advertisement,
    validate_worker_advertisement, write_control,
};
use crate::mesh::registry::MeshRegistry;
use crate::mesh::store::{DisabledMeshModelRecord, JoinKeyDecision, MeshStore, StoreError};
use iroh::endpoint::{IncomingAddr, presets};
use iroh::{Endpoint, RelayMode};
use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

/// Connections still in the QUIC/TLS handshake. Nothing about the peer is
/// known yet, so this pool is small and short-lived.
const MAX_PENDING_HANDSHAKES: usize = 64;
/// Enrollment is unauthenticated (the join key arrives inside it), so it gets
/// its own small pool and can never consume worker capacity.
const MAX_ENROLL_SESSIONS: usize = 16;
/// Long-lived control sessions of already-authorized workers.
const MAX_WORKER_SESSIONS: usize = 256;
/// Unauthenticated connections (handshaking or enrolling) per source address
/// (IPv6 grouped by /64). Address validation via QUIC Retry makes the source
/// address trustworthy enough to key on.
const MAX_UNAUTHENTICATED_PER_SOURCE: usize = 4;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const AUTHORIZATION_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);
const ENROLL_STEP_TIMEOUT: Duration = Duration::from_secs(3);
const ENROLL_TOTAL_TIMEOUT: Duration = Duration::from_secs(8);
/// After the enrollment verdict is written, give the peer a moment to read
/// it and close before the connection is dropped (which would discard
/// unacknowledged stream data).
const ENROLL_RESPONSE_LINGER: Duration = Duration::from_secs(1);
const CONTROL_STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(10);
const INITIAL_CONTROL_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

/// A live session re-reads its authorization at most this often. Dashboard
/// revocation evicts live sessions immediately through the registry; this
/// bound only covers out-of-band revocation (e.g. the `mesh` CLI).
const AUTHORIZATION_RECHECK_INTERVAL: Duration = Duration::from_secs(5);
/// A transient store failure keeps the cached decision and retries soon
/// instead of tearing down a healthy worker session.
const AUTHORIZATION_RETRY_AFTER_ERROR: Duration = Duration::from_secs(1);

/// Control frames are heartbeats (one per ~10 s) plus occasional inventory
/// updates; a reconnect burst is at most a hello and one update per resource.
const CONTROL_FRAME_BURST: f64 = 64.0;
const CONTROL_FRAMES_PER_SEC: f64 = 4.0;

const DEFAULT_CONTROLLER_IDENTITY_FILE: &str = "llmconduit-controller.key";

type MeshModelAllowlist = BTreeMap<String, BTreeMap<String, Vec<String>>>;

#[derive(Debug, Clone)]
pub(crate) struct MeshAdmin {
    store: MeshStore,
    registry: Arc<MeshRegistry>,
    initialized: Arc<tokio::sync::OnceCell<()>>,
}

impl MeshAdmin {
    pub(crate) fn store(&self) -> &MeshStore {
        &self.store
    }

    pub(crate) fn registry(&self) -> Arc<MeshRegistry> {
        Arc::clone(&self.registry)
    }

    pub(crate) async fn initialize(&self) -> Result<(), StoreError> {
        self.initialized
            .get_or_try_init(|| async {
                self.store.initialize().await?;
                self.reload_disabled_models().await
            })
            .await
            .map(|_| ())
    }

    pub(crate) async fn reload_disabled_models(&self) -> Result<(), StoreError> {
        let disabled = self.store.list_disabled_models().await?;
        self.apply_disabled_models(disabled);
        Ok(())
    }

    pub(crate) fn disable_live_node(&self, endpoint_id: iroh::EndpointId) -> bool {
        self.registry
            .remove_endpoint(endpoint_id, b"mesh node was disabled")
    }

    pub(crate) fn apply_model_disabled(
        &self,
        endpoint_id: iroh::EndpointId,
        resource_id: &str,
        model: &str,
        disabled: bool,
    ) {
        self.registry
            .set_model_disabled(endpoint_id, resource_id, model, disabled);
    }

    fn apply_disabled_models(&self, records: Vec<DisabledMeshModelRecord>) {
        self.registry
            .replace_disabled_models(records.into_iter().filter_map(|record| {
                record
                    .endpoint_id
                    .parse::<iroh::EndpointId>()
                    .ok()
                    .map(|endpoint_id| (endpoint_id, record.resource_id, record.model))
            }));
    }
}

pub(crate) fn spawn_controller(config: &MeshControllerConfig) -> AppResult<Arc<MeshAdmin>> {
    let registry = Arc::new(
        MeshRegistry::new(Duration::from_secs(config.heartbeat_timeout_secs))
            .with_canonical_model_ids(allowlisted_model_ids(&config.model_allowlist)),
    );
    let state_path = config
        .state_path
        .clone()
        .ok_or_else(|| AppError::bad_request("mesh.controller.state_path is required"))?;
    let store = MeshStore::at_path(&state_path);
    let admin = Arc::new(MeshAdmin {
        store: store.clone(),
        registry: Arc::clone(&registry),
        initialized: Arc::new(tokio::sync::OnceCell::new()),
    });
    let config = config.clone();
    let registry_for_task = Arc::clone(&registry);
    let admin_for_task = Arc::clone(&admin);
    tokio::spawn(async move {
        if let Err(err) = run_controller(config, registry_for_task, admin_for_task).await {
            tracing::error!(error = %err, "mesh controller stopped");
        }
    });
    Ok(admin)
}

fn allowlisted_model_ids(allowlist: &MeshModelAllowlist) -> impl Iterator<Item = &str> {
    allowlist
        .values()
        .flat_map(|resources| resources.values())
        .flatten()
        .map(String::as_str)
}

async fn run_controller(
    config: MeshControllerConfig,
    registry: Arc<MeshRegistry>,
    admin: Arc<MeshAdmin>,
) -> AppResult<()> {
    let identity_path = resolve_controller_identity_path(&config);
    let identity = load_or_create(&identity_path)
        .await
        .map_err(|err| AppError::internal(format!("failed to load mesh identity: {err}")))?;
    let secret = identity.secret_key();
    let endpoint_id = secret.public();
    admin
        .initialize()
        .await
        .map_err(|err| AppError::internal(format!("failed to open mesh store: {err}")))?;
    let authorizer = Arc::new(MeshAuthorizer::new(admin.store.clone()));
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(secret)
        .relay_mode(RelayMode::Disabled)
        .alpns(vec![ENROLL_ALPN.to_vec(), WORKER_ALPN.to_vec()])
        .bind_addr(config.bind_addr)
        .map_err(|err| AppError::internal(format!("invalid mesh bind address: {err}")))?
        .bind()
        .await
        .map_err(|err| AppError::internal(format!("failed to bind mesh endpoint: {err}")))?;
    tracing::info!(
        endpoint_id = %endpoint_id,
        bind_addr = %config.bind_addr,
        identity_path = %identity_path.display(),
        "mesh controller listening"
    );
    let model_allowlist = Arc::new(config.model_allowlist.clone());
    if model_allowlist.is_empty() {
        tracing::warn!(
            "mesh controller model_allowlist is empty; workers may enroll but no mesh models will be routable"
        );
    }
    accept_loop(
        endpoint,
        ControllerContext {
            registry,
            authorizer,
            model_allowlist,
            limits: ControllerLimits::default(),
        },
    )
    .await;
    Ok(())
}

#[derive(Clone)]
struct ControllerContext {
    registry: Arc<MeshRegistry>,
    authorizer: Arc<MeshAuthorizer>,
    model_allowlist: Arc<MeshModelAllowlist>,
    limits: ControllerLimits,
}

#[derive(Clone)]
struct ControllerLimits {
    handshakes: Arc<Semaphore>,
    enrollments: Arc<Semaphore>,
    workers: Arc<Semaphore>,
    per_source: SourceLimiter,
}

impl Default for ControllerLimits {
    fn default() -> Self {
        Self {
            handshakes: Arc::new(Semaphore::new(MAX_PENDING_HANDSHAKES)),
            enrollments: Arc::new(Semaphore::new(MAX_ENROLL_SESSIONS)),
            workers: Arc::new(Semaphore::new(MAX_WORKER_SESSIONS)),
            per_source: SourceLimiter::new(MAX_UNAUTHENTICATED_PER_SOURCE),
        }
    }
}

async fn accept_loop(endpoint: Endpoint, ctx: ControllerContext) {
    while let Some(mut incoming) = endpoint.accept().await {
        // A QUIC Retry round trip proves the peer owns its source address
        // before we spend a handshake slot on it, so spoofed floods cannot
        // pin slots and the per-source cap below keys on a real address.
        if !incoming.remote_addr_validated() {
            match incoming.retry() {
                Ok(()) => continue,
                Err(err) => incoming = err.into_incoming(),
            }
        }
        let source = match incoming.remote_addr() {
            IncomingAddr::Ip(addr) => Some(source_key(addr.ip())),
            _ => None,
        };
        let Some(source_guard) = ctx.limits.per_source.try_acquire(source) else {
            tracing::warn!(
                source = ?source,
                limit = MAX_UNAUTHENTICATED_PER_SOURCE,
                "refusing mesh connection: too many unauthenticated connections from source"
            );
            incoming.refuse();
            continue;
        };
        let Ok(handshake_slot) = Arc::clone(&ctx.limits.handshakes).try_acquire_owned() else {
            tracing::warn!(
                limit = MAX_PENDING_HANDSHAKES,
                "refusing mesh connection: too many pending handshakes"
            );
            incoming.refuse();
            continue;
        };
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let connecting = match incoming.accept() {
                Ok(connecting) => connecting,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to accept mesh connection");
                    return;
                }
            };
            let handshake = timeout(HANDSHAKE_TIMEOUT, async move {
                let mut connecting = connecting;
                let alpn = connecting
                    .alpn()
                    .await
                    .map_err(|err| format!("ALPN negotiation failed: {err}"))?;
                let connection = connecting
                    .await
                    .map_err(|err| format!("handshake failed: {err}"))?;
                Ok::<_, String>((alpn, connection))
            })
            .await;
            drop(handshake_slot);
            let (alpn, connection) = match handshake {
                Ok(Ok(established)) => established,
                Ok(Err(err)) => {
                    tracing::warn!(error = %err, "mesh connection setup failed");
                    return;
                }
                Err(_) => {
                    tracing::warn!("mesh connection handshake timed out");
                    return;
                }
            };
            if let Err(err) = handle_connection(ctx, connection, &alpn, source_guard).await {
                tracing::warn!(error = %err, "mesh connection ended");
            }
        });
    }
}

async fn handle_connection(
    ctx: ControllerContext,
    connection: iroh::endpoint::Connection,
    alpn: &[u8],
    source_guard: SourceGuard,
) -> AppResult<()> {
    let endpoint_id = connection.remote_id();
    if alpn == WORKER_ALPN {
        // Decide authorization before granting a long-lived worker slot so
        // unenrolled identities never occupy worker capacity.
        let node = match timeout(
            AUTHORIZATION_LOOKUP_TIMEOUT,
            ctx.authorizer.worker_authorization(endpoint_id),
        )
        .await
        {
            Ok(Ok(node)) => node,
            Ok(Err(err)) => {
                connection.close(503u32.into(), b"mesh authorization unavailable");
                return Err(AppError::internal(format!(
                    "mesh authorization failed: {err}"
                )));
            }
            Err(_) => {
                connection.close(503u32.into(), b"mesh authorization unavailable");
                return Err(AppError::internal("mesh authorization lookup timed out"));
            }
        };
        let Some(node) = node.filter(|node| node.enabled) else {
            connection.close(403u32.into(), b"mesh node is not authorized");
            return Err(AppError::upstream("mesh node is not authorized"));
        };
        // Authenticated: stop counting against the per-source budget, which
        // exists for unauthenticated peers (many workers may share a NAT).
        drop(source_guard);
        let Ok(worker_slot) = Arc::clone(&ctx.limits.workers).try_acquire_owned() else {
            tracing::warn!(
                endpoint_id = %endpoint_id,
                limit = MAX_WORKER_SESSIONS,
                "refusing mesh worker: controller is at worker session capacity"
            );
            connection.close(503u32.into(), b"mesh controller is at capacity");
            return Err(AppError::upstream("mesh controller is at worker capacity"));
        };
        return handle_worker_connection(ctx, connection, endpoint_id, node.label, worker_slot)
            .await;
    }
    if alpn != ENROLL_ALPN {
        connection.close(400u32.into(), b"unsupported mesh ALPN");
        return Err(AppError::bad_request("unsupported mesh ALPN"));
    }
    let Ok(_enroll_slot) = Arc::clone(&ctx.limits.enrollments).try_acquire_owned() else {
        tracing::warn!(
            endpoint_id = %endpoint_id,
            limit = MAX_ENROLL_SESSIONS,
            "refusing mesh enrollment: too many concurrent enrollments"
        );
        connection.close(503u32.into(), b"mesh controller is busy");
        return Err(AppError::upstream("mesh enrollment capacity exhausted"));
    };
    let result = timeout(
        ENROLL_TOTAL_TIMEOUT,
        handle_enrollment(&ctx, &connection, endpoint_id),
    )
    .await
    .unwrap_or_else(|_| Err(AppError::upstream("mesh enrollment timed out")));
    drop(source_guard);
    result
}

async fn handle_enrollment(
    ctx: &ControllerContext,
    connection: &iroh::endpoint::Connection,
    endpoint_id: iroh::EndpointId,
) -> AppResult<()> {
    let (mut send, mut recv) = timeout(ENROLL_STEP_TIMEOUT, connection.accept_bi())
        .await
        .map_err(|_| AppError::upstream("timed out waiting for mesh enrollment stream"))?
        .map_err(|err| AppError::upstream(format!("failed to accept enrollment stream: {err}")))?;
    let request: EnrollRequest = timeout(ENROLL_STEP_TIMEOUT, read_control(&mut recv))
        .await
        .map_err(|_| AppError::upstream("timed out waiting for mesh enrollment request"))??;
    let verdict = match validate_enroll_request(&request) {
        Err(err) => Err(format!("invalid enrollment request: {err}")),
        Ok(()) => match ctx
            .authorizer
            .enroll(&request.join_key, endpoint_id, request.node_name)
            .await
        {
            Ok(JoinKeyDecision::Accepted | JoinKeyDecision::AlreadyEnrolled) => Ok(()),
            Ok(other) => Err(format!("{other:?}").to_ascii_lowercase()),
            Err(err) => Err(format!("store error: {err}")),
        },
    };
    let response = match &verdict {
        Ok(()) => EnrollResponse::Accepted {
            protocol_version: PROTOCOL_VERSION,
        },
        // The exact reason (unknown vs expired vs exhausted key, revoked
        // node) would let a prober map our key/node state; it stays in the
        // server log only.
        Err(_) => EnrollResponse::Rejected {
            reason: "rejected".to_string(),
        },
    };
    write_control(&mut send, &response).await?;
    let _ = send.finish();
    let _ = timeout(ENROLL_RESPONSE_LINGER, connection.closed()).await;
    match verdict {
        Ok(()) => {
            tracing::info!(endpoint_id = %endpoint_id, "mesh enrollment accepted");
            Ok(())
        }
        Err(detail) => {
            tracing::warn!(endpoint_id = %endpoint_id, reason = %detail, "mesh enrollment rejected");
            Err(AppError::bad_request("mesh enrollment rejected"))
        }
    }
}

async fn handle_worker_connection(
    ctx: ControllerContext,
    connection: iroh::endpoint::Connection,
    endpoint_id: iroh::EndpointId,
    enrollment_label: Option<String>,
    _worker_slot: OwnedSemaphorePermit,
) -> AppResult<()> {
    let (send, mut recv) = timeout(CONTROL_STREAM_OPEN_TIMEOUT, connection.accept_bi())
        .await
        .map_err(|_| AppError::upstream("timed out waiting for mesh control stream"))?
        .map_err(|err| {
            AppError::upstream(format!("failed to accept mesh control stream: {err}"))
        })?;
    let first: WorkerToHub = timeout(INITIAL_CONTROL_FRAME_TIMEOUT, read_control(&mut recv))
        .await
        .map_err(|_| AppError::upstream("timed out waiting for mesh worker hello"))??;
    let WorkerToHub::Hello(advertisement) = first else {
        return Err(AppError::bad_request(
            "mesh worker did not start with hello",
        ));
    };
    validate_worker_advertisement(&advertisement)
        .map_err(|err| AppError::bad_request(format!("invalid mesh worker hello: {err}")))?;
    let advertisement =
        filter_worker_advertisement(endpoint_id, ctx.model_allowlist.as_ref(), advertisement);
    validate_worker_advertisement(&advertisement)
        .map_err(|err| AppError::bad_request(format!("invalid mesh worker hello: {err}")))?;
    handle_worker_control(
        ctx,
        connection,
        endpoint_id,
        MeshControlStreams { send, recv },
        advertisement,
        enrollment_label,
    )
    .await
}

async fn handle_worker_control(
    ctx: ControllerContext,
    connection: iroh::endpoint::Connection,
    endpoint_id: iroh::EndpointId,
    mut streams: MeshControlStreams,
    advertisement: crate::mesh::protocol::WorkerAdvertisement,
    enrollment_label: Option<String>,
) -> AppResult<()> {
    let registry = Arc::clone(&ctx.registry);
    let model_allowlist = Arc::clone(&ctx.model_allowlist);
    let session = registry.register_enrolled(
        endpoint_id,
        connection.clone(),
        advertisement,
        enrollment_label,
    );
    write_control(
        &mut streams.send,
        &HubToWorker::HelloAck {
            protocol_version: crate::mesh::protocol::PROTOCOL_VERSION,
        },
    )
    .await?;
    tracing::info!(
        endpoint_id = %endpoint_id,
        generation = session.generation,
        "mesh worker connected"
    );
    let mut authorization = SessionAuthorization::new(Instant::now());
    let mut frame_budget = ControlFrameBudget::new(Instant::now());
    let result: AppResult<()> = async {
        loop {
            let frame = read_control::<_, WorkerToHub>(&mut streams.recv).await?;
            if !frame_budget.try_take(Instant::now()) {
                connection.close(429u32.into(), b"mesh control frame rate exceeded");
                return Err(AppError::bad_request(
                    "mesh worker exceeded the control frame rate limit",
                ));
            }
            authorization
                .refresh(&ctx.authorizer, &connection, endpoint_id, Instant::now())
                .await?;
            match frame {
                WorkerToHub::Heartbeat(heartbeat) => {
                    validate_heartbeat(&heartbeat).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh worker heartbeat: {err}"))
                    })?;
                    session.apply_heartbeat(heartbeat);
                }
                WorkerToHub::ResourceUpdate(update) => {
                    validate_resource_advertisement(&update).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh resource update: {err}"))
                    })?;
                    let update =
                        filter_resource_update(endpoint_id, model_allowlist.as_ref(), update);
                    validate_resource_advertisement(&update).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh resource update: {err}"))
                    })?;
                    let resource_id = update.resource_id.clone();
                    if !session.update_resource(update) {
                        tracing::warn!(
                            endpoint_id = %endpoint_id,
                            resource_id = %resource_id,
                            "mesh worker attempted to add an unapproved resource"
                        );
                    }
                }
                WorkerToHub::ModelCatalogUpdate {
                    resource_id,
                    models,
                    revision,
                } => {
                    validate_model_catalog(&resource_id, &models).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh model catalog update: {err}"))
                    })?;
                    let models = filter_model_catalog(
                        endpoint_id,
                        model_allowlist.as_ref(),
                        &resource_id,
                        models,
                    );
                    validate_model_catalog(&resource_id, &models).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh model catalog update: {err}"))
                    })?;
                    session.update_model_catalog(&resource_id, models, revision);
                }
                WorkerToHub::ModelSwitchingUpdate(update) => {
                    validate_model_switching(&update).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh switching update: {err}"))
                    })?;
                    if let Some(update) =
                        filter_model_switching(endpoint_id, model_allowlist.as_ref(), update)
                    {
                        session.update_model_switching(update);
                    } else {
                        tracing::warn!(
                            endpoint_id = %endpoint_id,
                            "mesh worker attempted to advertise switching without an approved endpoint"
                        );
                    }
                }
                WorkerToHub::Hello(advertisement) => {
                    validate_worker_advertisement(&advertisement).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh worker hello: {err}"))
                    })?;
                    let advertisement = filter_worker_advertisement(
                        endpoint_id,
                        model_allowlist.as_ref(),
                        advertisement,
                    );
                    validate_worker_advertisement(&advertisement).map_err(|err| {
                        AppError::bad_request(format!("invalid mesh worker hello: {err}"))
                    })?;
                    for resource in advertisement.resources {
                        let resource_id = resource.resource_id.clone();
                        if !session.update_resource(resource) {
                            tracing::warn!(
                                endpoint_id = %endpoint_id,
                                resource_id = %resource_id,
                                "mesh worker hello attempted to add an unapproved resource"
                            );
                        }
                    }
                }
            }
        }
    }
    .await;
    registry.remove_generation(endpoint_id, session.generation);
    connection.close(0u32.into(), b"mesh control stream ended");
    result
}

struct MeshControlStreams {
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
}

/// Cached per-session authorization: a worker sends a control frame every
/// few seconds, and re-reading SQLite for each one is avoidable load.
struct SessionAuthorization {
    next_check: Instant,
}

impl SessionAuthorization {
    /// The session was authorized at `authorized_at`.
    fn new(authorized_at: Instant) -> Self {
        Self {
            next_check: authorized_at + AUTHORIZATION_RECHECK_INTERVAL,
        }
    }

    async fn refresh(
        &mut self,
        authorizer: &MeshAuthorizer,
        connection: &iroh::endpoint::Connection,
        endpoint_id: iroh::EndpointId,
        now: Instant,
    ) -> AppResult<()> {
        match self.check(authorizer, endpoint_id, now).await {
            AuthorizationCheck::Revoked => {
                connection.close(403u32.into(), b"mesh node was revoked");
                Err(AppError::upstream("mesh node was revoked"))
            }
            AuthorizationCheck::Authorized => Ok(()),
        }
    }

    async fn check(
        &mut self,
        authorizer: &MeshAuthorizer,
        endpoint_id: iroh::EndpointId,
        now: Instant,
    ) -> AuthorizationCheck {
        if now < self.next_check {
            return AuthorizationCheck::Authorized;
        }
        match authorizer.authorize_worker(endpoint_id).await {
            Ok(true) => {
                self.next_check = now + AUTHORIZATION_RECHECK_INTERVAL;
                AuthorizationCheck::Authorized
            }
            Ok(false) => AuthorizationCheck::Revoked,
            Err(err) => {
                tracing::warn!(
                    endpoint_id = %endpoint_id,
                    error = %err,
                    "mesh authorization refresh failed; keeping the session and retrying"
                );
                self.next_check = now + AUTHORIZATION_RETRY_AFTER_ERROR;
                AuthorizationCheck::Authorized
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum AuthorizationCheck {
    Authorized,
    Revoked,
}

/// Token bucket over inbound control frames. Each frame costs a JSON parse,
/// validation, and registry locking on a 2-vCPU hub, so a compromised or
/// buggy worker spamming frames is disconnected rather than absorbed.
struct ControlFrameBudget {
    tokens: f64,
    last_refill: Instant,
}

impl ControlFrameBudget {
    fn new(now: Instant) -> Self {
        Self {
            tokens: CONTROL_FRAME_BURST,
            last_refill: now,
        }
    }

    fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        self.tokens = (self.tokens + elapsed * CONTROL_FRAMES_PER_SEC).min(CONTROL_FRAME_BURST);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Source bucket for per-address limits. IPv6 hosts typically control a
/// whole /64, so that is the unit an attacker can cheaply rotate within.
fn source_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return IpAddr::V4(v4);
            }
            let mut octets = v6.octets();
            octets[8..].fill(0);
            IpAddr::V6(std::net::Ipv6Addr::from(octets))
        }
    }
}

#[derive(Clone)]
struct SourceLimiter {
    limit: usize,
    counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
}

struct SourceGuard {
    source: Option<IpAddr>,
    counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
}

impl SourceLimiter {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// `None` sources (non-IP transports; relays are disabled) are not
    /// keyed and only bounded by the global pools.
    fn try_acquire(&self, source: Option<IpAddr>) -> Option<SourceGuard> {
        if let Some(ip) = source {
            let mut counts = self
                .counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let count = counts.entry(ip).or_insert(0);
            if *count >= self.limit {
                return None;
            }
            *count += 1;
        }
        Some(SourceGuard {
            source,
            counts: Arc::clone(&self.counts),
        })
    }

    #[cfg(test)]
    fn tracked_sources(&self) -> usize {
        self.counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

impl Drop for SourceGuard {
    fn drop(&mut self) {
        let Some(ip) = self.source else {
            return;
        };
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = counts.get_mut(&ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&ip);
            }
        }
    }
}

fn filter_worker_advertisement(
    endpoint_id: iroh::EndpointId,
    allowlist: &MeshModelAllowlist,
    mut advertisement: crate::mesh::protocol::WorkerAdvertisement,
) -> crate::mesh::protocol::WorkerAdvertisement {
    let endpoint_key = endpoint_id.to_string();
    let Some(resources) = allowlist.get(&endpoint_key) else {
        advertisement.resources.clear();
        advertisement.model_switching = None;
        return advertisement;
    };
    advertisement.resources.retain_mut(|resource| {
        if !resources.contains_key(&resource.resource_id) {
            return false;
        }
        filter_resource_models(resources, resource);
        true
    });
    advertisement.model_switching = advertisement
        .model_switching
        .and_then(|switching| filter_model_switching(endpoint_id, allowlist, switching));
    advertisement
}

fn filter_model_switching(
    endpoint_id: iroh::EndpointId,
    allowlist: &MeshModelAllowlist,
    mut switching: crate::mesh::protocol::ModelSwitchingAdvertisement,
) -> Option<crate::mesh::protocol::ModelSwitchingAdvertisement> {
    let endpoint_id = endpoint_id.to_string();
    let resources = allowlist.get(&endpoint_id)?;
    switching.models.retain(|model| {
        resources.values().any(|allowed_models| {
            allowed_models
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(&model.id))
        })
    });
    Some(switching)
}

fn filter_resource_update(
    endpoint_id: iroh::EndpointId,
    allowlist: &MeshModelAllowlist,
    mut update: crate::mesh::protocol::ResourceAdvertisement,
) -> crate::mesh::protocol::ResourceAdvertisement {
    let endpoint_id = endpoint_id.to_string();
    if let Some(resources) = allowlist.get(&endpoint_id) {
        filter_resource_models(resources, &mut update);
    } else {
        update.models.clear();
    }
    update
}

fn filter_model_catalog(
    endpoint_id: iroh::EndpointId,
    allowlist: &MeshModelAllowlist,
    resource_id: &str,
    models: Vec<crate::mesh::protocol::ModelAdvertisement>,
) -> Vec<crate::mesh::protocol::ModelAdvertisement> {
    let endpoint_id = endpoint_id.to_string();
    let Some(resources) = allowlist.get(&endpoint_id) else {
        return Vec::new();
    };
    let Some(allowed) = resources.get(resource_id) else {
        return Vec::new();
    };
    models
        .into_iter()
        .filter(|model| {
            allowed
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(&model.id))
        })
        .map(sanitize_model_advertisement)
        .collect()
}

fn filter_resource_models(
    resources: &BTreeMap<String, Vec<String>>,
    resource: &mut crate::mesh::protocol::ResourceAdvertisement,
) {
    let Some(allowed) = resources.get(&resource.resource_id) else {
        resource.models.clear();
        return;
    };
    resource.models = std::mem::take(&mut resource.models)
        .into_iter()
        .filter(|model| {
            allowed
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(&model.id))
        })
        .map(sanitize_model_advertisement)
        .collect();
}

fn sanitize_model_advertisement(
    mut model: crate::mesh::protocol::ModelAdvertisement,
) -> crate::mesh::protocol::ModelAdvertisement {
    let sanitized = sanitize_context_limit(model.context_limit);
    if sanitized != model.context_limit {
        tracing::warn!(
            model = %model.id,
            context_limit = ?model.context_limit,
            "ignoring out-of-range mesh model context limit"
        );
        model.context_limit = sanitized;
    }
    model
}

/// Without an explicit `identity_path` the old default was a file in the
/// process cwd, so a restart from another directory silently minted a new
/// hub id and orphaned every worker's pinned `controller_endpoint_id`. Keep
/// using an existing cwd key (rotating it would be worse) but otherwise put
/// the key beside the state database, which is a required, stable path.
fn resolve_controller_identity_path(config: &MeshControllerConfig) -> PathBuf {
    if let Some(path) = &config.identity_path {
        return path.clone();
    }
    let legacy = PathBuf::from(DEFAULT_CONTROLLER_IDENTITY_FILE);
    let state_dir = config
        .state_path
        .as_deref()
        .and_then(Path::parent)
        .filter(|parent| !parent.as_os_str().is_empty());
    let resolved = choose_controller_identity_path(&legacy, state_dir);
    if resolved == legacy {
        tracing::warn!(
            path = %std::env::current_dir()
                .map(|cwd| cwd.join(&legacy))
                .unwrap_or_else(|_| legacy.clone())
                .display(),
            "mesh.controller.identity_path is unset; using the controller key in the current \
             directory. Set identity_path explicitly so a restart from another directory does \
             not change the hub endpoint id"
        );
    } else {
        tracing::info!(
            path = %resolved.display(),
            "mesh.controller.identity_path is unset; storing the controller key beside the mesh state"
        );
    }
    resolved
}

fn choose_controller_identity_path(legacy: &Path, state_dir: Option<&Path>) -> PathBuf {
    match state_dir {
        Some(dir) if !legacy.exists() => dir.join(DEFAULT_CONTROLLER_IDENTITY_FILE),
        _ => legacy.to_path_buf(),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AvailabilitySchedule;
    use crate::mesh::protocol::{
        ModelAdvertisement, ModelSwitchingAdvertisement, ResourceAdvertisement,
        SwitchableModelAdvertisement, WorkerAdvertisement,
    };
    use crate::mesh::store::MeshStore;
    use iroh::SecretKey;
    use uuid::Uuid;

    fn advertisement() -> WorkerAdvertisement {
        WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: None,
            agent_version: "test".to_string(),
            resources: vec![
                ResourceAdvertisement {
                    resource_id: "gpu-a".to_string(),
                    models: vec![
                        ModelAdvertisement {
                            id: "allowed-model".to_string(),
                            context_limit: Some(4096),
                        },
                        ModelAdvertisement {
                            id: "surprise-model".to_string(),
                            context_limit: None,
                        },
                    ],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                },
                ResourceAdvertisement {
                    resource_id: "gpu-b".to_string(),
                    models: vec![ModelAdvertisement {
                        id: "other-model".to_string(),
                        context_limit: None,
                    }],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                },
            ],
            model_switching: Some(ModelSwitchingAdvertisement {
                provider: "lil-fleet".to_string(),
                models: vec![
                    SwitchableModelAdvertisement::legacy(
                        "allowed-model",
                        None,
                        "ready",
                        "loaded",
                        1,
                        vec![0],
                    ),
                    SwitchableModelAdvertisement::legacy(
                        "surprise-model",
                        None,
                        "unloaded",
                        "unloaded",
                        1,
                        Vec::new(),
                    ),
                ],
                revision: 1,
            }),
            request_encodings: Vec::new(),
        }
    }

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("llmconduit-{name}-{}.sqlite", Uuid::new_v4()))
    }

    #[test]
    fn controller_model_allowlist_filters_initial_advertisement() {
        let endpoint = SecretKey::generate().public();
        let mut resources = BTreeMap::new();
        resources.insert("gpu-a".to_string(), vec!["ALLOWED-MODEL".to_string()]);
        let allowlist = BTreeMap::from([(endpoint.to_string(), resources)]);

        let filtered = filter_worker_advertisement(endpoint, &allowlist, advertisement());

        assert_eq!(filtered.resources.len(), 1);
        assert_eq!(filtered.resources[0].resource_id, "gpu-a");
        assert_eq!(filtered.resources[0].models.len(), 1);
        assert_eq!(filtered.resources[0].models[0].id, "allowed-model");
        let switching = filtered.model_switching.expect("filtered switching");
        assert_eq!(switching.models.len(), 1);
        assert_eq!(switching.models[0].id, "allowed-model");
    }

    #[test]
    fn controller_model_allowlist_denies_unlisted_endpoint() {
        let endpoint = SecretKey::generate().public();
        let allowlist = BTreeMap::from([(
            SecretKey::generate().public().to_string(),
            BTreeMap::from([("gpu-a".to_string(), vec!["allowed-model".to_string()])]),
        )]);

        let filtered = filter_worker_advertisement(endpoint, &allowlist, advertisement());

        assert!(filtered.resources.is_empty());
        assert!(filtered.model_switching.is_none());
    }

    #[test]
    fn controller_model_allowlist_fails_closed_when_empty() {
        let endpoint = SecretKey::generate().public();

        let filtered = filter_worker_advertisement(endpoint, &BTreeMap::new(), advertisement());

        assert!(filtered.resources.is_empty());
        assert!(filtered.model_switching.is_none());
    }

    #[test]
    fn controller_model_allowlist_filters_later_switching_updates() {
        let endpoint = SecretKey::generate().public();
        let allowlist = BTreeMap::from([(
            endpoint.to_string(),
            BTreeMap::from([("gpu-a".to_string(), vec!["allowed-model".to_string()])]),
        )]);
        let switching = advertisement()
            .model_switching
            .expect("switching inventory");

        let filtered = filter_model_switching(endpoint, &allowlist, switching)
            .expect("approved endpoint retains an inventory");

        assert_eq!(filtered.models.len(), 1);
        assert_eq!(filtered.models[0].id, "allowed-model");
    }

    #[test]
    fn controller_model_allowlist_filters_later_catalog_updates() {
        let endpoint = SecretKey::generate().public();
        let allowlist = BTreeMap::from([(
            endpoint.to_string(),
            BTreeMap::from([("gpu-a".to_string(), vec!["allowed-model".to_string()])]),
        )]);
        let models = vec![
            ModelAdvertisement {
                id: "allowed-model".to_string(),
                context_limit: Some(4096),
            },
            ModelAdvertisement {
                id: "surprise-model".to_string(),
                context_limit: None,
            },
        ];

        let filtered = filter_model_catalog(endpoint, &allowlist, "gpu-a", models);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "allowed-model");
    }

    #[test]
    fn control_frame_budget_allows_bursts_and_refills_but_stops_floods() {
        let start = Instant::now();
        let mut budget = ControlFrameBudget::new(start);
        for _ in 0..CONTROL_FRAME_BURST as usize {
            assert!(budget.try_take(start));
        }
        assert!(!budget.try_take(start), "burst exhausted");
        assert!(budget.try_take(start + Duration::from_millis(300)));
        // Steady heartbeats never trip the limiter.
        let mut budget = ControlFrameBudget::new(start);
        for second in 0..600 {
            assert!(budget.try_take(start + Duration::from_secs(second)));
        }
    }

    #[test]
    fn per_source_limiter_caps_unauthenticated_peers_and_releases_on_drop() {
        let limiter = SourceLimiter::new(2);
        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        let first = limiter.try_acquire(Some(ip)).expect("first");
        let _second = limiter.try_acquire(Some(ip)).expect("second");
        assert!(limiter.try_acquire(Some(ip)).is_none());
        assert!(
            limiter
                .try_acquire(Some("203.0.113.8".parse().unwrap()))
                .is_some(),
            "other sources are unaffected"
        );
        assert!(limiter.try_acquire(None).is_some(), "unkeyed sources pass");
        drop(first);
        assert!(limiter.try_acquire(Some(ip)).is_some());
        drop(_second);
        assert_eq!(limiter.tracked_sources(), 0, "idle sources are forgotten");
    }

    #[test]
    fn ipv6_sources_are_grouped_by_prefix() {
        let a: IpAddr = "2001:db8:1:2:aaaa::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:bbbb::2".parse().unwrap();
        let other: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_eq!(source_key(a), source_key(b));
        assert_ne!(source_key(a), source_key(other));
        let mapped: IpAddr = "::ffff:192.0.2.1".parse().unwrap();
        assert_eq!(source_key(mapped), "192.0.2.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn default_identity_path_prefers_existing_legacy_key_then_state_dir() {
        let dir = std::env::temp_dir().join(format!("llmconduit-idpath-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing_legacy = dir.join("absent.key");
        assert_eq!(
            choose_controller_identity_path(&missing_legacy, Some(&dir)),
            dir.join(DEFAULT_CONTROLLER_IDENTITY_FILE)
        );
        let present_legacy = dir.join("present.key");
        std::fs::write(&present_legacy, b"x").unwrap();
        assert_eq!(
            choose_controller_identity_path(&present_legacy, Some(&dir)),
            present_legacy
        );
        assert_eq!(
            choose_controller_identity_path(&missing_legacy, None),
            missing_legacy
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn allowlist_filter_drops_out_of_range_context_limits() {
        let endpoint = SecretKey::generate().public();
        let allowlist = BTreeMap::from([(
            endpoint.to_string(),
            BTreeMap::from([("gpu-a".to_string(), vec!["allowed-model".to_string()])]),
        )]);
        for (limit, expected) in [
            (Some(0), None),
            (Some(-5), None),
            (Some(i64::MAX), None),
            (Some(8192), Some(8192)),
        ] {
            let filtered = filter_model_catalog(
                endpoint,
                &allowlist,
                "gpu-a",
                vec![ModelAdvertisement {
                    id: "allowed-model".to_string(),
                    context_limit: limit,
                }],
            );
            assert_eq!(filtered[0].context_limit, expected);
            let mut ad = advertisement();
            ad.resources[0].models[0].context_limit = limit;
            let filtered = filter_worker_advertisement(endpoint, &allowlist, ad);
            assert_eq!(filtered.resources[0].models[0].context_limit, expected);
        }
    }

    async fn enrolled_authorizer(
        name: &str,
    ) -> (PathBuf, MeshStore, MeshAuthorizer, iroh::EndpointId) {
        let path = temp_db(name);
        let store = MeshStore::open(&path).await.expect("open store");
        let created = store
            .create_join_key(None, None, Some(1))
            .await
            .expect("create key");
        let endpoint = SecretKey::generate().public();
        let authorizer = MeshAuthorizer::new(store.clone());
        assert_eq!(
            authorizer
                .enroll(&created.token, endpoint, None)
                .await
                .expect("enroll"),
            JoinKeyDecision::Accepted
        );
        (path, store, authorizer, endpoint)
    }

    #[tokio::test]
    async fn session_authorization_is_cached_but_revocation_lands_on_recheck() {
        let (path, store, authorizer, endpoint) = enrolled_authorizer("auth-cache").await;
        let start = Instant::now();
        let mut session = SessionAuthorization::new(start);
        store
            .set_node_enabled(&endpoint.to_string(), false)
            .await
            .expect("revoke");

        // Within the cache window no store read happens.
        assert_eq!(
            session
                .check(&authorizer, endpoint, start + Duration::from_secs(1))
                .await,
            AuthorizationCheck::Authorized
        );
        // The first frame after the window sees the revocation.
        assert_eq!(
            session
                .check(
                    &authorizer,
                    endpoint,
                    start + AUTHORIZATION_RECHECK_INTERVAL
                )
                .await,
            AuthorizationCheck::Revoked
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn transient_store_errors_do_not_end_a_worker_session() {
        // A directory cannot be opened as a database: every lookup errors.
        let dir = std::env::temp_dir().join(format!("llmconduit-auth-err-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let authorizer = MeshAuthorizer::new(MeshStore::at_path(&dir));
        let endpoint = SecretKey::generate().public();
        let start = Instant::now();
        let mut session = SessionAuthorization::new(start);
        let due = start + AUTHORIZATION_RECHECK_INTERVAL;
        assert_eq!(
            session.check(&authorizer, endpoint, due).await,
            AuthorizationCheck::Authorized
        );
        assert_eq!(session.next_check, due + AUTHORIZATION_RETRY_AFTER_ERROR);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn quic_enrollment_and_worker_sessions_end_to_end() {
        use iroh::endpoint::ConnectionError;
        use iroh::{EndpointAddr, TransportAddr};
        use std::net::{Ipv4Addr, SocketAddr};

        let path = temp_db("e2e");
        let store = MeshStore::open(&path).await.expect("open store");
        let join_key = store
            .create_join_key(None, None, Some(1))
            .await
            .expect("create key");
        let controller_key = SecretKey::generate();
        let controller_id = controller_key.public();
        let controller = Endpoint::builder(presets::Minimal)
            .secret_key(controller_key)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![ENROLL_ALPN.to_vec(), WORKER_ALPN.to_vec()])
            .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let controller_addr = EndpointAddr::from_parts(
            controller_id,
            controller
                .bound_sockets()
                .into_iter()
                .map(TransportAddr::Ip),
        );
        let worker_key = SecretKey::generate();
        let worker_id = worker_key.public();
        let worker = Endpoint::builder(presets::Minimal)
            .secret_key(worker_key)
            .relay_mode(RelayMode::Disabled)
            .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        let ctx = ControllerContext {
            registry: Arc::clone(&registry),
            authorizer: Arc::new(MeshAuthorizer::new(store.clone())),
            model_allowlist: Arc::new(BTreeMap::from([(
                worker_id.to_string(),
                BTreeMap::from([("gpu-a".to_string(), vec!["allowed-model".to_string()])]),
            )])),
            limits: ControllerLimits::default(),
        };
        let server = tokio::spawn(accept_loop(controller.clone(), ctx));

        // An unenrolled identity is refused before it gets a session.
        let connection = worker
            .connect(controller_addr.clone(), WORKER_ALPN)
            .await
            .expect("handshake completes");
        let closed = timeout(Duration::from_secs(5), connection.closed())
            .await
            .expect("unauthorized worker is closed promptly");
        assert!(
            matches!(&closed, ConnectionError::ApplicationClosed(close) if close.error_code == 403u32.into()),
            "{closed:?}"
        );

        let config = crate::config::MeshWorkerConfig {
            node_name: Some("lab-box".into()),
            ..Default::default()
        };
        // A bad key gets a generic rejection with no key-state detail.
        let rejected = crate::mesh::worker::enroll(
            &worker,
            controller_addr.clone(),
            &config,
            "llmc_join_not-a-real-key".into(),
        )
        .await
        .expect_err("unknown key rejected")
        .to_string();
        assert!(rejected.contains("rejected"), "{rejected}");
        assert!(
            !rejected.to_ascii_lowercase().contains("notfound"),
            "{rejected}"
        );

        crate::mesh::worker::enroll(&worker, controller_addr.clone(), &config, join_key.token)
            .await
            .expect("valid key enrolls");

        let connection = worker
            .connect(controller_addr.clone(), WORKER_ALPN)
            .await
            .expect("enrolled worker connects");
        let (mut send, mut recv) = connection.open_bi().await.expect("control stream");
        let mut hello = advertisement();
        hello.node_name = Some("pretend-to-be-prod".into());
        write_control(&mut send, &WorkerToHub::Hello(hello))
            .await
            .expect("hello");
        let ack: HubToWorker = timeout(Duration::from_secs(5), read_control(&mut recv))
            .await
            .expect("ack in time")
            .expect("ack");
        assert_eq!(
            ack,
            HubToWorker::HelloAck {
                protocol_version: PROTOCOL_VERSION
            }
        );
        let inventory = registry.provider_inventory();
        assert_eq!(inventory.len(), 1);
        assert_eq!(
            inventory[0].provider_name,
            format!("lab-box ({})", worker_id.fmt_short()),
            "the enrollment label, not the self-reported name, is displayed"
        );

        // Dashboard revocation evicts the live session immediately.
        assert!(registry.remove_endpoint(worker_id, b"mesh node was disabled"));
        timeout(Duration::from_secs(5), connection.closed())
            .await
            .expect("revoked worker is disconnected");

        server.abort();
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn mesh_admin_initialize_does_not_reload_over_live_model_overrides() {
        let path = temp_db("mesh-admin-once");
        let store = MeshStore::open(&path).await.expect("open store");
        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: None,
                agent_version: "test".into(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "gpu".into(),
                    models: vec![ModelAdvertisement {
                        id: "qwen".into(),
                        context_limit: None,
                    }],
                    availability: AvailabilitySchedule::default(),
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
                model_switching: None,
                request_encodings: Vec::new(),
            },
        );
        let admin = MeshAdmin {
            store: store.clone(),
            registry: Arc::clone(&registry),
            initialized: Arc::new(tokio::sync::OnceCell::new()),
        };
        admin.initialize().await.expect("initial load");
        store
            .set_model_disabled(&endpoint.to_string(), "gpu", "qwen", true, 1)
            .await
            .expect("persist disable");
        admin.apply_model_disabled(endpoint, "gpu", "qwen", true);
        assert!(registry.reserve("qwen").is_none());

        admin
            .initialize()
            .await
            .expect("second initialize is no-op");

        assert!(registry.reserve("qwen").is_none());
        let _ = std::fs::remove_file(path);
    }
}
