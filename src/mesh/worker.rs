use crate::config::{
    AvailabilitySchedule, MeshWeekday, MeshWorkerCapacitySource, MeshWorkerConfig,
    MeshWorkerResourceConfig,
};
use crate::error::{AppError, AppResult};
use crate::mesh::capacity::CapacityGate;
use crate::mesh::identity::load_or_create;
use crate::mesh::io::{Admission, read_stream_open, write_admission, write_switch_response};
use crate::mesh::protocol::{
    AdmissionRejectCode, ENROLL_ALPN, EnrollRequest, EnrollResponse, Heartbeat, HubToWorker,
    MAX_CAPACITY_PER_RESOURCE, ModelAdvertisement, ModelLifecycleAction,
    ModelSwitchingAdvertisement, PROTOCOL_VERSION, REQUEST_ENCODING_ZSTD, ResourceAdvertisement,
    ResourceRuntimeState, StreamOpen, SwitchModelRequest, SwitchModelResponse,
    SwitchableModelAdvertisement, SwitchableModelInstanceAdvertisement, WORKER_ALPN,
    WorkerAdvertisement, WorkerToHub, read_control, write_control,
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};

#[derive(Debug)]
pub(super) struct WorkerRuntime {
    config: MeshWorkerConfig,
    resources: HashMap<String, Arc<LocalResource>>,
    fleet: Option<Arc<crate::dashboard_fleet::FleetClient>>,
    model_switching: Mutex<Option<ModelSwitchingAdvertisement>>,
    /// After a load/unload, poll Fleet quickly until this instant (or until
    /// no model is mid-transition) so the hub sees the phase change promptly.
    fast_refresh_until: std::sync::Mutex<Option<TokioInstant>>,
    refresh_wakeup: tokio::sync::Notify,
}

/// Poll cadence while a requested model transition is in flight.
const FAST_MODEL_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
/// Upper bound on fast polling after one lifecycle request; large model
/// loads that take longer fall back to the normal cadence.
const FAST_MODEL_REFRESH_WINDOW: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
struct LocalResource {
    config: MeshWorkerResourceConfig,
    gate: CapacityGate,
    models: Mutex<Vec<ModelAdvertisement>>,
    healthy: Mutex<bool>,
    revision: Mutex<u64>,
    detected_capacity: Mutex<Option<u32>>,
}

pub async fn run_worker(config: MeshWorkerConfig, join_key: Option<String>) -> AppResult<()> {
    let identity_path = config
        .identity_path
        .clone()
        .ok_or_else(|| AppError::bad_request("mesh.worker.identity_path is required"))?;
    let identity = load_or_create(&identity_path)
        .await
        .map_err(|err| AppError::internal(format!("failed to load mesh worker identity: {err}")))?;
    let controller = controller_addr(&config).await?;
    let endpoint = identity
        .bind_direct_endpoint(Vec::new())
        .await
        .map_err(|err| AppError::internal(format!("failed to bind mesh worker endpoint: {err}")))?;
    let runtime = Arc::new(WorkerRuntime::new(config).await);

    if let Some(join_key) = join_key.filter(|key| !key.trim().is_empty()) {
        enroll(&endpoint, controller.clone(), &runtime.config, join_key).await?;
    }

    reconnect_loop(endpoint, controller, runtime).await
}

impl WorkerRuntime {
    pub(super) async fn new(config: MeshWorkerConfig) -> Self {
        let mut resources = HashMap::new();
        for resource in &config.resources {
            resources.insert(
                resource.id.clone(),
                Arc::new(LocalResource::new(resource.clone())),
            );
        }
        let fleet = match crate::dashboard_fleet::FleetClient::from_env(reqwest::Client::new()) {
            Ok(fleet) => fleet.map(Arc::new),
            Err(err) => {
                tracing::warn!(error = %err, "mesh worker Fleet capability is misconfigured");
                None
            }
        };
        let runtime = Self {
            config,
            resources,
            fleet,
            model_switching: Mutex::new(None),
            fast_refresh_until: std::sync::Mutex::new(None),
            refresh_wakeup: tokio::sync::Notify::new(),
        };
        runtime.refresh_model_switching().await;
        runtime
    }

    async fn advertisement(&self) -> WorkerAdvertisement {
        let mut resources = Vec::with_capacity(self.resources.len());
        for resource in self.resources.values() {
            resources.push(resource.advertisement().await);
        }
        WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: self.config.node_name.clone(),
            agent_version: crate::VERSION.to_string(),
            resources,
            model_switching: self.model_switching.lock().await.clone(),
            request_encodings: vec![REQUEST_ENCODING_ZSTD.to_string()],
        }
    }

    async fn refresh_model_switching(&self) -> Option<ModelSwitchingAdvertisement> {
        let fleet = self.fleet.as_ref()?;
        let models = match fleet.list_models().await {
            Ok(response) => response
                .models
                .into_iter()
                .map(|entry| SwitchableModelAdvertisement {
                    id: entry.model.id,
                    description: entry.model.description,
                    phase: entry.status.phase,
                    desired_state: entry.status.desired_state,
                    gpu_count: entry.model.gpu_count.unwrap_or(0),
                    max_instances: entry.model.max_instances,
                    desired_instances: entry.status.desired_instances,
                    ready_instances: entry.status.ready_instances,
                    instances: entry
                        .status
                        .instances
                        .into_iter()
                        .map(SwitchableModelInstanceAdvertisement::from_fleet)
                        .collect(),
                    assigned_gpus: entry
                        .status
                        .assigned_gpus
                        .into_iter()
                        .filter_map(|gpu| u32::try_from(gpu).ok())
                        .collect(),
                })
                .collect::<Vec<_>>(),
            Err(err) => {
                tracing::warn!(error = %err, "mesh worker failed to refresh Fleet inventory");
                return None;
            }
        };
        let mut current = self.model_switching.lock().await;
        if current.as_ref().is_some_and(|value| value.models == models) {
            return None;
        }
        let revision = current
            .as_ref()
            .map_or(1, |value| value.revision.saturating_add(1));
        let update = ModelSwitchingAdvertisement {
            provider: "lil-fleet".to_string(),
            models,
            revision,
        };
        *current = Some(update.clone());
        Some(update)
    }

    async fn heartbeat(&self, sequence: u64) -> Heartbeat {
        let mut resources = Vec::with_capacity(self.resources.len());
        for resource in self.resources.values() {
            let snapshot = resource.gate.snapshot();
            let (limit, active) = (snapshot.limit, snapshot.active);
            let healthy = *resource.healthy.lock().await;
            resources.push(ResourceRuntimeState {
                resource_id: resource.config.id.clone(),
                effective_capacity: limit,
                worker_active_requests: active,
                accepting_requests: healthy && limit > active,
                healthy,
            });
        }
        Heartbeat {
            sequence,
            resources,
        }
    }

    async fn apply_schedule_updates(&self) -> Vec<ResourceAdvertisement> {
        let now = Utc::now();
        let mut updates = Vec::new();
        for resource in self.resources.values() {
            if let Some(update) = resource.apply_schedule(now).await {
                updates.push(update);
            }
        }
        updates
    }

    fn next_schedule_delay(&self) -> Duration {
        let now = Utc::now();
        self.resources
            .values()
            .filter_map(|resource| next_schedule_boundary(&resource.config.availability, now))
            .filter_map(|boundary| (boundary - now).to_std().ok())
            .min()
            .unwrap_or_else(|| Duration::from_secs(300))
            .clamp(Duration::from_secs(1), Duration::from_secs(300))
    }
}

impl LocalResource {
    fn new(config: MeshWorkerResourceConfig) -> Self {
        let configured = config.capacity_source == MeshWorkerCapacitySource::Configured;
        let limit = if configured {
            effective_capacity_at(&config.availability, Utc::now())
        } else {
            0
        };
        Self {
            config,
            gate: CapacityGate::new(limit),
            models: Mutex::new(Vec::new()),
            healthy: Mutex::new(configured),
            revision: Mutex::new(0),
            detected_capacity: Mutex::new(None),
        }
    }

    fn capacity_at(&self, now: DateTime<Utc>, detected: Option<u32>) -> u32 {
        match self.config.capacity_source {
            MeshWorkerCapacitySource::Configured => {
                effective_capacity_at(&self.config.availability, now)
            }
            MeshWorkerCapacitySource::Vllm => detected.map_or(0, |capacity| {
                effective_capacity_with_default(&self.config.availability, now, capacity)
                    .min(capacity)
            }),
        }
    }

    async fn advertisement(&self) -> ResourceAdvertisement {
        let snapshot = self.gate.snapshot();
        let (limit, active) = (snapshot.limit, snapshot.active);
        let healthy = *self.healthy.lock().await;
        ResourceAdvertisement {
            resource_id: self.config.id.clone(),
            models: self.models.lock().await.clone(),
            availability: self.config.availability.clone(),
            effective_capacity: limit,
            accepting_requests: healthy && limit > active,
            healthy,
            revision: *self.revision.lock().await,
        }
    }

    async fn apply_schedule(&self, now: DateTime<Utc>) -> Option<ResourceAdvertisement> {
        // Share this lock with discovery so a schedule tick cannot restore a
        // previous engine's capacity after the port has been reused.
        let detected = self.detected_capacity.lock().await;
        let changed = self
            .gate
            .set_limit_if_changed(self.capacity_at(now, *detected));
        drop(detected);
        if changed {
            *self.revision.lock().await += 1;
            Some(self.advertisement().await)
        } else {
            None
        }
    }
}

pub(super) async fn enroll(
    endpoint: &Endpoint,
    controller: EndpointAddr,
    config: &MeshWorkerConfig,
    join_key: String,
) -> AppResult<()> {
    let connection = endpoint
        .connect(controller, ENROLL_ALPN)
        .await
        .map_err(|err| AppError::upstream(format!("mesh enrollment connect failed: {err}")))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| AppError::upstream(format!("mesh enrollment stream failed: {err}")))?;
    let request = enroll_request(config, join_key);
    write_control(&mut send, &request).await?;
    let response: EnrollResponse = read_control(&mut recv).await?;
    match response {
        EnrollResponse::Accepted { .. } => Ok(()),
        EnrollResponse::Rejected { reason } => Err(AppError::upstream(format!(
            "mesh enrollment rejected: {reason}"
        ))),
    }
}

fn enroll_request(config: &MeshWorkerConfig, join_key: String) -> EnrollRequest {
    EnrollRequest {
        protocol_version: PROTOCOL_VERSION,
        join_key,
        node_name: config.node_name.clone(),
        agent_version: crate::VERSION.to_string(),
    }
}

async fn reconnect_loop(
    endpoint: Endpoint,
    controller: EndpointAddr,
    runtime: Arc<WorkerRuntime>,
) -> AppResult<()> {
    let mut backoff = Duration::from_millis(250);
    loop {
        match endpoint.connect(controller.clone(), WORKER_ALPN).await {
            Ok(connection) => {
                let connected_at = TokioInstant::now();
                if let Err(err) = run_connected(connection, Arc::clone(&runtime)).await {
                    tracing::warn!(error = %err, "mesh worker connection ended");
                }
                if connected_at.elapsed()
                    >= Duration::from_secs(
                        runtime
                            .config
                            .heartbeat_interval_secs
                            .max(1)
                            .saturating_mul(2),
                    )
                {
                    backoff = Duration::from_millis(250);
                }
            }
            Err(err) => tracing::warn!(error = %err, "mesh worker connect failed"),
        }
        tokio::time::sleep(jittered_backoff(backoff)).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn jittered_backoff(backoff: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.subsec_nanos());
    let permille = 800 + nanos % 401;
    backoff.mul_f64(f64::from(permille) / 1000.0)
}

async fn run_connected(
    connection: iroh::endpoint::Connection,
    runtime: Arc<WorkerRuntime>,
) -> AppResult<()> {
    refresh_models(&runtime).await;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (update_tx, update_rx) = mpsc::channel(32);
    let mut tasks = JoinSet::new();
    tasks.spawn(control_task(
        connection.clone(),
        Arc::clone(&runtime),
        update_rx,
        shutdown_rx.clone(),
    ));
    tasks.spawn(request_task(
        connection.clone(),
        Arc::clone(&runtime),
        shutdown_rx.clone(),
    ));
    tasks.spawn(model_refresh_task(
        Arc::clone(&runtime),
        update_tx,
        shutdown_rx,
    ));
    let result = tokio::select! {
        reason = connection.closed() => Err(AppError::upstream(format!("mesh worker connection closed: {reason}"))),
        Some(joined) = tasks.join_next() => match joined {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(AppError::internal(format!("mesh worker task panicked: {err}"))),
        },
    };
    let _ = shutdown_tx.send(true);
    tasks.abort_all();
    result
}

async fn control_task(
    connection: iroh::endpoint::Connection,
    runtime: Arc<WorkerRuntime>,
    mut updates: mpsc::Receiver<WorkerToHub>,
    mut shutdown: watch::Receiver<bool>,
) -> AppResult<()> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| AppError::upstream(format!("failed to open mesh control stream: {err}")))?;
    write_control(
        &mut send,
        &WorkerToHub::Hello(runtime.advertisement().await),
    )
    .await?;
    let mut sequence = 0;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(
        runtime.config.heartbeat_interval_secs.max(1),
    ));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let schedule_sleep = tokio::time::sleep(runtime.next_schedule_delay());
    tokio::pin!(schedule_sleep);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            frame = read_control::<_, HubToWorker>(&mut recv) => {
                match frame? {
                    HubToWorker::HelloAck { protocol_version } if protocol_version == PROTOCOL_VERSION => {}
                    HubToWorker::HelloAck { .. } => return Err(AppError::upstream("mesh controller protocol version mismatch")),
                    HubToWorker::Ping { nonce: _ } => {}
                    HubToWorker::Close { reason } => return Err(AppError::upstream(format!("mesh controller closed worker: {reason}"))),
                }
            }
            _ = heartbeat.tick() => {
                sequence += 1;
                write_control(&mut send, &WorkerToHub::Heartbeat(runtime.heartbeat(sequence).await)).await?;
            }
            _ = &mut schedule_sleep => {
                for update in runtime.apply_schedule_updates().await {
                    write_control(&mut send, &WorkerToHub::ResourceUpdate(update)).await?;
                }
                schedule_sleep.as_mut().reset(TokioInstant::now() + runtime.next_schedule_delay());
            }
            Some(update) = updates.recv() => {
                write_control(&mut send, &update).await?;
            }
        }
    }
    Ok(())
}

async fn model_refresh_task(
    runtime: Arc<WorkerRuntime>,
    updates: mpsc::Sender<WorkerToHub>,
    mut shutdown: watch::Receiver<bool>,
) -> AppResult<()> {
    let refresh_secs = runtime
        .config
        .resources
        .iter()
        .map(|resource| resource.model_refresh_secs)
        .min()
        .unwrap_or(60)
        .max(1);
    let normal = Duration::from_secs(refresh_secs);
    let refresh = tokio::time::sleep(normal);
    tokio::pin!(refresh);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            // A lifecycle request just ran (and already refreshed): only
            // re-plan the next poll at the fast cadence.
            _ = runtime.refresh_wakeup.notified() => {}
            _ = &mut refresh => {
                for update in refresh_models(&runtime).await {
                    if updates.send(WorkerToHub::ResourceUpdate(update)).await.is_err() {
                        return Ok(());
                    }
                }
                if let Some(update) = runtime.refresh_model_switching().await
                    && updates.send(WorkerToHub::ModelSwitchingUpdate(update)).await.is_err()
                {
                    return Ok(());
                }
            }
        }
        let delay = runtime
            .next_model_refresh_delay(normal, TokioInstant::now())
            .await;
        refresh.as_mut().reset(TokioInstant::now() + delay);
    }
    Ok(())
}

/// Whether Fleet still reports this model moving toward its desired state.
/// Unknown phases count as in flight; the fast window bounds the cost.
fn model_is_settling(model: &SwitchableModelAdvertisement) -> bool {
    let wants_loaded = model.desired_state.eq_ignore_ascii_case("loaded")
        || model.desired_state.eq_ignore_ascii_case("ready");
    match model.phase.to_ascii_lowercase().as_str() {
        "ready" | "running" | "loaded" => model.ready_instances < model.desired_instances,
        "unloaded" | "stopped" => wants_loaded,
        "failed" | "error" => false,
        _ => true,
    }
}

impl WorkerRuntime {
    fn begin_fast_model_refresh(&self) {
        *self
            .fast_refresh_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(TokioInstant::now() + FAST_MODEL_REFRESH_WINDOW);
        self.refresh_wakeup.notify_one();
    }

    async fn next_model_refresh_delay(&self, normal: Duration, now: TokioInstant) -> Duration {
        let fast_until = *self
            .fast_refresh_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(fast_until) = fast_until.filter(|until| now < *until) else {
            return normal;
        };
        let settling = self
            .model_switching
            .lock()
            .await
            .as_ref()
            .is_some_and(|inventory| inventory.models.iter().any(model_is_settling));
        if settling {
            return FAST_MODEL_REFRESH_INTERVAL.min(normal).min(
                fast_until
                    .saturating_duration_since(now)
                    .max(Duration::from_millis(1)),
            );
        }
        *self
            .fast_refresh_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        normal
    }
}

async fn request_task(
    connection: iroh::endpoint::Connection,
    runtime: Arc<WorkerRuntime>,
    mut shutdown: watch::Receiver<bool>,
) -> AppResult<()> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            stream = connection.accept_bi() => {
                let (send, recv) = stream.map_err(|err| {
                    AppError::upstream(format!("failed to accept mesh request stream: {err}"))
                })?;
                let runtime = Arc::clone(&runtime);
                tokio::spawn(async move {
                    if let Err(err) = handle_stream(send, recv, runtime).await {
                        tracing::warn!(error = %err, "mesh worker request failed");
                    }
                });
            }
        }
    }
    Ok(())
}

pub(super) async fn handle_stream(
    send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    runtime: Arc<WorkerRuntime>,
) -> AppResult<()> {
    match read_stream_open(&mut recv).await? {
        StreamOpen::Inference(open) => handle_request(send, recv, runtime, open).await,
        StreamOpen::SwitchModel(request) => {
            handle_switch_model(send, runtime, request, ModelLifecycleAction::Load).await
        }
        StreamOpen::UnloadModel(request) => {
            handle_switch_model(send, runtime, request, ModelLifecycleAction::Unload).await
        }
    }
}

pub(super) async fn handle_request(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    runtime: Arc<WorkerRuntime>,
    open: crate::mesh::protocol::RequestOpen,
) -> AppResult<()> {
    if let Err(err) = crate::mesh::protocol::validate_request_open(&open) {
        write_admission(
            &mut send,
            &Admission::Rejected {
                code: AdmissionRejectCode::ProtocolError,
            },
        )
        .await?;
        return Err(AppError::bad_request(format!(
            "invalid mesh request preface: {err}"
        )));
    }
    let Some(resource) = runtime.resources.get(&open.resource_id).cloned() else {
        write_admission(
            &mut send,
            &Admission::Rejected {
                code: AdmissionRejectCode::UnknownResource,
            },
        )
        .await?;
        return Ok(());
    };
    if !*resource.healthy.lock().await {
        write_admission(
            &mut send,
            &Admission::Rejected {
                code: AdmissionRejectCode::ResourceUnhealthy,
            },
        )
        .await?;
        return Ok(());
    }
    // The hub can release its permit before the preceding worker stream has
    // finished teardown. Keep that brief overlap on the selected runner.
    let _permit = tokio::select! {
        biased;
        _ = send.stopped() => return Ok(()),
        permit = tokio::time::timeout(Duration::from_secs(30), resource.gate.acquire()) => {
            match permit {
                Ok(permit) => permit,
                Err(_) => {
                    write_admission(
                        &mut send,
                        &Admission::Rejected {
                            code: AdmissionRejectCode::CapacityExhausted,
                        },
                    ).await?;
                    return Ok(());
                }
            }
        }
    };
    if !*resource.healthy.lock().await {
        write_admission(
            &mut send,
            &Admission::Rejected {
                code: AdmissionRejectCode::ResourceUnhealthy,
            },
        )
        .await?;
        return Ok(());
    }
    tracing::debug!(
        request_id = %open.request_id,
        resource_id = %open.resource_id,
        active = resource.gate.snapshot().active,
        "mesh worker capacity acquired"
    );
    let local = match TcpStream::connect(resource.config.target).await {
        Ok(local) => local,
        Err(err) => {
            *resource.healthy.lock().await = false;
            write_admission(
                &mut send,
                &Admission::Rejected {
                    code: AdmissionRejectCode::LocalConnectFailed,
                },
            )
            .await?;
            return Err(AppError::upstream(format!(
                "failed to connect local mesh resource: {err}"
            )));
        }
    };
    write_admission(&mut send, &Admission::Accepted).await?;
    let (mut local_read, mut local_write) = local.into_split();
    // The local engine only ever sees a request this worker re-serialized
    // from a validated head, never the controller's raw bytes.
    let upload = tokio::time::timeout(
        REQUEST_UPLOAD_TIMEOUT,
        forward_request_upload(&mut recv, &mut local_write, resource.config.target),
    )
    .await
    .unwrap_or(Err(UploadError::Rejected {
        status: http::StatusCode::REQUEST_TIMEOUT,
        message: "mesh request upload timed out",
    }));
    if let Err(err) = upload {
        drop(_permit);
        return match err {
            UploadError::Rejected { status, message } => {
                write_http_error(&mut send, status, message).await;
                Err(AppError::bad_request(format!(
                    "rejected mesh request from controller: {message}"
                )))
            }
            UploadError::Io(err) => Err(AppError::upstream(format!(
                "mesh request forwarding failed: {err}"
            ))),
        };
    }
    let response_stopped = send.stopped();
    let forward_response = async {
        tokio::select! {
            result = async {
                tokio::io::copy(&mut local_read, &mut send).await?;
                tokio::io::AsyncWriteExt::shutdown(&mut send).await
            } => result,
            _ = response_stopped => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "mesh response receiver closed",
            )),
        }
    };
    // Content-Length delimits the request, and `local_write` stays open until
    // the response is done: uvicorn interprets a client EOF as cancellation.
    let result = forward_response
        .await
        .map_err(|err| AppError::upstream(format!("mesh request forwarding failed: {err}")));
    drop(local_write);
    drop(_permit);
    tracing::debug!(
        request_id = %open.request_id,
        resource_id = %open.resource_id,
        active = resource.gate.snapshot().active,
        "mesh worker capacity released"
    );
    result?;
    Ok(())
}

/// The only request the hub sends over an inference stream.
const FORWARDED_METHOD: &str = "POST";
const FORWARDED_PATH: &str = "/v1/chat/completions";
const MAX_FORWARDED_HEAD_BYTES: usize = 16 * 1024;
/// Matches the gateway's inbound body buffer; a larger body cannot have
/// come from a well-behaved hub.
const MAX_FORWARDED_BODY_BYTES: usize = 256 * 1024 * 1024;
/// Bounds the whole upload (head and body) so a stalled controller stream
/// cannot hold a capacity permit and a local engine connection forever.
const REQUEST_UPLOAD_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug)]
enum UploadError {
    /// Answered to the hub as an HTTP error before any engine response.
    Rejected {
        status: http::StatusCode,
        message: &'static str,
    },
    Io(std::io::Error),
}

impl From<std::io::Error> for UploadError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

fn reject(status: http::StatusCode, message: &'static str) -> UploadError {
    UploadError::Rejected { status, message }
}

#[derive(Debug, PartialEq, Eq)]
struct ForwardedRequestHead {
    content_length: usize,
    zstd: bool,
}

/// Reads the controller's request, validates it against the one shape the
/// hub emits, and writes a freshly built request to the local engine.
async fn forward_request_upload<W>(
    recv: &mut iroh::endpoint::RecvStream,
    local: &mut W,
    target: SocketAddr,
) -> Result<(), UploadError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buffered = tokio_util::bytes::BytesMut::new();
    let head_end = loop {
        if let Some(end) = find_head_end(&buffered) {
            break end;
        }
        if buffered.len() > MAX_FORWARDED_HEAD_BYTES {
            return Err(reject(
                http::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                "mesh request head is too large",
            ));
        }
        let chunk = recv
            .read_chunk(MAX_FORWARDED_HEAD_BYTES)
            .await
            .map_err(|err| UploadError::Io(std::io::Error::other(err)))?;
        let Some(chunk) = chunk else {
            return Err(reject(
                http::StatusCode::BAD_REQUEST,
                "mesh request ended before its head",
            ));
        };
        buffered.extend_from_slice(&chunk);
    };
    let head = parse_forwarded_head(&buffered[..head_end])?;
    let prefix = buffered.split_off(head_end).freeze();
    if prefix.len() > head.content_length {
        return Err(reject(
            http::StatusCode::BAD_REQUEST,
            "mesh request body exceeds its content-length",
        ));
    }
    let remaining = (head.content_length - prefix.len()) as u64;

    if head.zstd {
        let mut compressed = Vec::with_capacity(head.content_length);
        compressed.extend_from_slice(&prefix);
        recv.take(remaining).read_to_end(&mut compressed).await?;
        if compressed.len() != head.content_length {
            return Err(reject(
                http::StatusCode::BAD_REQUEST,
                "mesh request body is shorter than its content-length",
            ));
        }
        let body = tokio::task::spawn_blocking(move || {
            decompress_zstd_capped(&compressed, MAX_FORWARDED_BODY_BYTES)
        })
        .await
        .map_err(|err| UploadError::Io(std::io::Error::other(err)))??;
        local
            .write_all(&local_request_head(target, body.len()))
            .await?;
        local.write_all(&body).await?;
    } else {
        local
            .write_all(&local_request_head(target, head.content_length))
            .await?;
        local.write_all(&prefix).await?;
        let copied = tokio::io::copy(&mut recv.take(remaining), local).await?;
        if copied != remaining {
            return Err(UploadError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "mesh request body ended before its content-length",
            )));
        }
    }
    local.flush().await?;
    Ok(())
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn parse_forwarded_head(head: &[u8]) -> Result<ForwardedRequestHead, UploadError> {
    let bad = |message| reject(http::StatusCode::BAD_REQUEST, message);
    let text = std::str::from_utf8(head).map_err(|_| bad("mesh request head is not UTF-8"))?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    let (Some(method), Some(path), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad("malformed mesh request line"));
    };
    if method != FORWARDED_METHOD || path != FORWARDED_PATH || version != "HTTP/1.1" {
        return Err(reject(
            http::StatusCode::FORBIDDEN,
            "mesh worker only forwards POST /v1/chat/completions",
        ));
    }
    let mut content_length = None;
    let mut zstd = false;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| bad("malformed mesh request header"))?;
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| bad("invalid mesh request content-length"))?;
                if content_length.is_some_and(|existing| existing != parsed) {
                    return Err(bad("conflicting mesh request content-length"));
                }
                content_length = Some(parsed);
            }
            "transfer-encoding" => {
                return Err(bad("mesh requests must use content-length framing"));
            }
            "content-encoding" => {
                if value.eq_ignore_ascii_case(crate::mesh::protocol::REQUEST_ENCODING_ZSTD) {
                    zstd = true;
                } else if !value.eq_ignore_ascii_case("identity") {
                    return Err(reject(
                        http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        "unsupported mesh request content-encoding",
                    ));
                }
            }
            "content-type" => {
                let media_type = value.split(';').next().unwrap_or_default().trim();
                if !media_type.eq_ignore_ascii_case("application/json") {
                    return Err(reject(
                        http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        "mesh request body must be JSON",
                    ));
                }
            }
            // Everything else is dropped: the local request is rebuilt from
            // a fixed header set below.
            _ => {}
        }
    }
    let content_length =
        content_length.ok_or_else(|| bad("mesh request is missing content-length"))?;
    if content_length > MAX_FORWARDED_BODY_BYTES {
        return Err(reject(
            http::StatusCode::PAYLOAD_TOO_LARGE,
            "mesh request body is too large",
        ));
    }
    Ok(ForwardedRequestHead {
        content_length,
        zstd,
    })
}

fn local_request_head(target: SocketAddr, content_length: usize) -> Vec<u8> {
    format!(
        "{FORWARDED_METHOD} {FORWARDED_PATH} HTTP/1.1\r\n\
         host: {target}\r\n\
         content-type: application/json\r\n\
         content-length: {content_length}\r\n\
         accept: text/event-stream\r\n\
         connection: close\r\n\r\n"
    )
    .into_bytes()
}

/// Streams the decode so a small "zstd bomb" cannot allocate past `cap`.
fn decompress_zstd_capped(compressed: &[u8], cap: usize) -> Result<Vec<u8>, UploadError> {
    use std::io::Read;

    let invalid = || {
        reject(
            http::StatusCode::BAD_REQUEST,
            "mesh request body is not valid zstd",
        )
    };
    let decoder = zstd::stream::read::Decoder::new(compressed).map_err(|_| invalid())?;
    let mut body = Vec::new();
    decoder
        .take(cap as u64 + 1)
        .read_to_end(&mut body)
        .map_err(|_| invalid())?;
    if body.len() > cap {
        return Err(reject(
            http::StatusCode::PAYLOAD_TOO_LARGE,
            "mesh request body is too large",
        ));
    }
    Ok(body)
}

async fn write_http_error(
    send: &mut iroh::endpoint::SendStream,
    status: http::StatusCode,
    message: &str,
) {
    use tokio::io::AsyncWriteExt;

    let body = serde_json::json!({"error": {"message": message}}).to_string();
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    if send.write_all(response.as_bytes()).await.is_ok() {
        let _ = send.shutdown().await;
    }
}

async fn handle_switch_model(
    mut send: iroh::endpoint::SendStream,
    runtime: Arc<WorkerRuntime>,
    request: SwitchModelRequest,
    action: ModelLifecycleAction,
) -> AppResult<()> {
    crate::mesh::protocol::validate_switch_model_request(&request)
        .map_err(|err| AppError::bad_request(format!("invalid mesh switch request: {err}")))?;
    let mut response = SwitchModelResponse {
        request_id: request.request_id,
        model_id: request.model_id.clone(),
        accepted: false,
        changed: false,
        error: None,
        model_switching: None,
        error_status: None,
    };
    let Some(fleet) = runtime.fleet.as_ref() else {
        response.error = Some("model switching is not configured".to_string());
        write_switch_response(&mut send, &response).await?;
        return Ok(());
    };
    let advertised = runtime.model_switching.lock().await.clone();
    if !advertised.as_ref().is_some_and(|capability| {
        capability
            .models
            .iter()
            .any(|model| model.id == request.model_id)
    }) {
        response.error = Some("model is not advertised as switchable".to_string());
        write_switch_response(&mut send, &response).await?;
        return Ok(());
    }
    let operation = match action {
        ModelLifecycleAction::Load => {
            fleet
                .load_model_instances(&request.model_id, request.instances)
                .await
        }
        ModelLifecycleAction::Unload => fleet.unload_model(&request.model_id).await,
    };
    match operation {
        Ok(operation) => {
            response.accepted = true;
            response.changed = operation.changed;
            runtime.refresh_model_switching().await;
            response.model_switching = runtime.model_switching.lock().await.clone();
            if operation.changed {
                runtime.begin_fast_model_refresh();
            }
        }
        Err(err) => {
            response.error = Some(err.to_string());
            response.error_status = Some(err.status().as_u16());
        }
    }
    write_switch_response(&mut send, &response).await
}

async fn refresh_models(runtime: &WorkerRuntime) -> Vec<ResourceAdvertisement> {
    let mut updates = Vec::new();
    for resource in runtime.resources.values() {
        let discovery = match resource.config.capacity_source {
            MeshWorkerCapacitySource::Configured => fetch_models(resource.config.target)
                .await
                .map(|models| (models, None)),
            MeshWorkerCapacitySource::Vllm => tokio::try_join!(
                fetch_models(resource.config.target),
                fetch_vllm_capacity(resource.config.target)
            )
            .map(|(models, capacity)| (models, Some(capacity))),
        };
        let (models, healthy, detected_capacity) = match discovery {
            Ok((models, capacity)) => (
                filter_advertised_models(models, &resource.config.models),
                true,
                capacity,
            ),
            Err(err) => {
                tracing::warn!(
                    resource_id = %resource.config.id,
                    error = %err,
                    "mesh worker local model discovery failed"
                );
                (Vec::new(), false, None)
            }
        };
        let models_changed = {
            let mut current = resource.models.lock().await;
            let changed = *current != models;
            *current = models;
            changed
        };
        let health_changed = {
            let mut current = resource.healthy.lock().await;
            let changed = *current != healthy;
            *current = healthy;
            changed
        };
        let capacity_changed = {
            let mut detected = resource.detected_capacity.lock().await;
            *detected = detected_capacity;
            resource
                .gate
                .set_limit_if_changed(resource.capacity_at(Utc::now(), *detected))
        };
        if models_changed || health_changed || capacity_changed {
            *resource.revision.lock().await += 1;
            updates.push(resource.advertisement().await);
        }
    }
    updates
}

fn filter_advertised_models(
    discovered: Vec<ModelAdvertisement>,
    configured: &[String],
) -> Vec<ModelAdvertisement> {
    if configured.is_empty() {
        return discovered;
    }
    let discovered_by_lowercase = discovered
        .into_iter()
        .map(|model| (model.id.to_ascii_lowercase(), model))
        .collect::<HashMap<_, _>>();
    let mut seen = HashSet::new();
    configured
        .iter()
        .filter_map(|id| {
            let normalized = id.to_ascii_lowercase();
            if !seen.insert(normalized.clone()) {
                return None;
            }
            discovered_by_lowercase
                .get(&normalized)
                .map(|model| ModelAdvertisement {
                    id: id.clone(),
                    context_limit: model.context_limit,
                })
        })
        .collect()
}

async fn fetch_models(target: SocketAddr) -> AppResult<Vec<ModelAdvertisement>> {
    let url = format!("http://{target}/v1/models");
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| {
            AppError::internal(format!("failed to build model discovery client: {err}"))
        })?
        .get(url)
        .send()
        .await
        .map_err(|err| AppError::upstream(format!("local model discovery failed: {err}")))?;
    let value = response
        .error_for_status()
        .map_err(|err| AppError::upstream(format!("local model discovery failed: {err}")))?
        .json::<serde_json::Value>()
        .await
        .map_err(|err| AppError::upstream(format!("invalid local model catalog: {err}")))?;
    let entries = value
        .get("data")
        .or_else(|| value.get("models"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(entries
        .into_iter()
        .filter_map(|entry| {
            let id = entry.get("id").and_then(serde_json::Value::as_str)?;
            let context_limit = entry
                .get("context_length")
                .or_else(|| entry.get("max_model_len"))
                .or_else(|| entry.get("max_context_length"))
                .and_then(serde_json::Value::as_i64)
                .filter(|value| *value > 0);
            Some(ModelAdvertisement {
                id: id.to_string(),
                context_limit,
            })
        })
        .collect())
}

async fn fetch_vllm_capacity(target: SocketAddr) -> AppResult<u32> {
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| {
            AppError::internal(format!("failed to build capacity discovery client: {err}"))
        })?
        .get(format!("http://{target}/server_info?config_format=json"))
        .send()
        .await
        .map_err(|err| AppError::upstream(format!("local vLLM capacity discovery failed: {err}")))?
        .error_for_status()
        .map_err(|err| {
            AppError::upstream(format!("local vLLM capacity discovery failed: {err}"))
        })?;
    let value = response
        .json::<serde_json::Value>()
        .await
        .map_err(|err| AppError::upstream(format!("invalid local vLLM server info: {err}")))?;
    vllm_capacity(&value)
}

fn vllm_capacity(value: &serde_json::Value) -> AppResult<u32> {
    value
        .pointer("/vllm_config/scheduler_config/max_num_seqs")
        .and_then(serde_json::Value::as_u64)
        .filter(|capacity| *capacity > 0 && *capacity <= u64::from(MAX_CAPACITY_PER_RESOURCE))
        .map(|capacity| capacity as u32)
        .ok_or_else(|| {
            AppError::upstream("local vLLM server info has no valid scheduler max_num_seqs")
        })
}

async fn controller_addr(config: &MeshWorkerConfig) -> AppResult<EndpointAddr> {
    let endpoint_id = config
        .controller_endpoint_id
        .as_ref()
        .ok_or_else(|| AppError::bad_request("mesh.worker.controller_endpoint_id is required"))?;
    let endpoint_id = EndpointId::from_str(endpoint_id)
        .map_err(|err| AppError::bad_request(format!("invalid controller endpoint id: {err}")))?;
    let controller_addr = config
        .controller_addr
        .as_ref()
        .ok_or_else(|| AppError::bad_request("mesh.worker.controller_addr is required"))?;
    let addrs: Vec<TransportAddr> = tokio::net::lookup_host(controller_addr)
        .await
        .map_err(|err| AppError::bad_request(format!("invalid controller address: {err}")))?
        .map(TransportAddr::Ip)
        .collect();
    if addrs.is_empty() {
        return Err(AppError::bad_request(
            "mesh.worker.controller_addr resolved to no addresses",
        ));
    }
    Ok(EndpointAddr::from_parts(endpoint_id, addrs))
}

fn effective_capacity_at(schedule: &AvailabilitySchedule, now: DateTime<Utc>) -> u32 {
    effective_capacity_with_default(schedule, now, schedule.default_capacity)
}

fn effective_capacity_with_default(
    schedule: &AvailabilitySchedule,
    now: DateTime<Utc>,
    default_capacity: u32,
) -> u32 {
    for exception in schedule.exceptions.iter().rev() {
        let start = exception.start.with_timezone(&Utc);
        let end = exception.end.with_timezone(&Utc);
        if now >= start && now < end {
            return exception.capacity;
        }
    }

    let tz = schedule.timezone.parse::<Tz>().unwrap_or(chrono_tz::UTC);
    let local = now.with_timezone(&tz);
    let minute = local.hour() as u16 * 60 + local.minute() as u16;
    let weekday = mesh_weekday_from_chrono(local.weekday());
    let previous_weekday = previous_weekday(weekday);

    for window in &schedule.weekly {
        let Some(start) = local_time_minutes(window.start_local) else {
            continue;
        };
        let Some(end) = local_time_minutes(window.end_local) else {
            continue;
        };
        let today_matches = window.days.contains(&weekday);
        if start == end {
            if today_matches {
                return window.capacity;
            }
        } else if start < end {
            if today_matches && minute >= start && minute < end {
                return window.capacity;
            }
        } else if (today_matches && minute >= start)
            || (window.days.contains(&previous_weekday) && minute < end)
        {
            return window.capacity;
        }
    }

    default_capacity
}

fn next_schedule_boundary(
    schedule: &AvailabilitySchedule,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let tz = schedule.timezone.parse::<Tz>().unwrap_or(chrono_tz::UTC);
    let local_now = now.with_timezone(&tz);
    let mut best: Option<DateTime<Utc>> = None;

    for exception in &schedule.exceptions {
        for boundary in [
            exception.start.with_timezone(&Utc),
            exception.end.with_timezone(&Utc),
        ] {
            if boundary > now {
                best = Some(best.map_or(boundary, |current| current.min(boundary)));
            }
        }
    }

    let Some(base_date) =
        NaiveDate::from_ymd_opt(local_now.year(), local_now.month(), local_now.day())
    else {
        return best;
    };

    for day_offset in 0..=8 {
        let Some(date) = base_date.checked_add_days(chrono::Days::new(day_offset)) else {
            continue;
        };
        let weekday = mesh_weekday_from_chrono(date.weekday());
        for window in &schedule.weekly {
            if !window.days.contains(&weekday) {
                continue;
            }
            for (boundary_date, minutes) in [
                (Some(date), local_time_minutes(window.start_local)),
                (
                    if local_time_minutes(window.start_local) > local_time_minutes(window.end_local)
                    {
                        date.checked_add_days(chrono::Days::new(1))
                    } else {
                        Some(date)
                    },
                    local_time_minutes(window.end_local),
                ),
            ] {
                let (Some(boundary_date), Some(minutes)) = (boundary_date, minutes) else {
                    continue;
                };
                let Some(boundary) = local_boundary_to_utc(tz, boundary_date, minutes) else {
                    continue;
                };
                if boundary > now {
                    best = Some(best.map_or(boundary, |current| current.min(boundary)));
                }
            }
        }
    }

    best
}

fn local_boundary_to_utc(tz: Tz, date: NaiveDate, minutes: u16) -> Option<DateTime<Utc>> {
    let time = NaiveTime::from_hms_opt(u32::from(minutes / 60), u32::from(minutes % 60), 0)?;
    let local = date.and_time(time);
    tz.from_local_datetime(&local)
        .earliest()
        .or_else(|| tz.from_local_datetime(&local).latest())
        .map(|value| value.with_timezone(&Utc))
}

fn local_time_minutes(time: crate::config::LocalTime) -> Option<u16> {
    let value = serde_json::to_value(time).ok()?;
    let text = value.as_str()?;
    let (hour, minute) = text.split_once(':')?;
    let hour = hour.parse::<u16>().ok()?;
    let minute = minute.parse::<u16>().ok()?;
    (hour < 24 && minute < 60).then_some(hour * 60 + minute)
}

fn mesh_weekday_from_chrono(day: chrono::Weekday) -> MeshWeekday {
    match day {
        chrono::Weekday::Mon => MeshWeekday::Mon,
        chrono::Weekday::Tue => MeshWeekday::Tue,
        chrono::Weekday::Wed => MeshWeekday::Wed,
        chrono::Weekday::Thu => MeshWeekday::Thu,
        chrono::Weekday::Fri => MeshWeekday::Fri,
        chrono::Weekday::Sat => MeshWeekday::Sat,
        chrono::Weekday::Sun => MeshWeekday::Sun,
    }
}

fn previous_weekday(day: MeshWeekday) -> MeshWeekday {
    match day {
        MeshWeekday::Mon => MeshWeekday::Sun,
        MeshWeekday::Tue => MeshWeekday::Mon,
        MeshWeekday::Wed => MeshWeekday::Tue,
        MeshWeekday::Thu => MeshWeekday::Wed,
        MeshWeekday::Fri => MeshWeekday::Thu,
        MeshWeekday::Sat => MeshWeekday::Fri,
        MeshWeekday::Sun => MeshWeekday::Sat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_resource(
        target: SocketAddr,
        source: MeshWorkerCapacitySource,
    ) -> MeshWorkerResourceConfig {
        MeshWorkerResourceConfig {
            id: "shared-port".to_string(),
            target,
            models: Vec::new(),
            model_refresh_secs: 60,
            capacity_source: source,
            availability: AvailabilitySchedule {
                default_capacity: 6,
                ..Default::default()
            },
        }
    }

    fn test_runtime(config: MeshWorkerResourceConfig) -> WorkerRuntime {
        WorkerRuntime {
            config: MeshWorkerConfig {
                resources: vec![config.clone()],
                ..Default::default()
            },
            resources: HashMap::from([(config.id.clone(), Arc::new(LocalResource::new(config)))]),
            fleet: None,
            model_switching: Mutex::new(None),
            fast_refresh_until: std::sync::Mutex::new(None),
            refresh_wakeup: tokio::sync::Notify::new(),
        }
    }

    async fn mount_discovery(server: &MockServer, model: &str, info: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [{"id": model, "max_model_len": 16384}]
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/server_info"))
            .and(query_param("config_format", "json"))
            .respond_with(info)
            .mount(server)
            .await;
    }

    fn capacity_response(capacity: u32) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "vllm_config": {"scheduler_config": {"max_num_seqs": capacity}}
        }))
    }

    fn rejection_status(result: Result<ForwardedRequestHead, UploadError>) -> http::StatusCode {
        match result {
            Err(UploadError::Rejected { status, .. }) => status,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn forwarded_head_accepts_only_the_hub_chat_request_shape() {
        let hub = b"POST /v1/chat/completions HTTP/1.1\r\nhost: llmconduit-mesh-worker\r\ncontent-type: application/json\r\ncontent-length: 42\r\naccept: text/event-stream\r\nconnection: close\r\n\r\n";
        assert_eq!(
            parse_forwarded_head(hub).unwrap(),
            ForwardedRequestHead {
                content_length: 42,
                zstd: false
            }
        );
        let compressed = b"POST /v1/chat/completions HTTP/1.1\r\ncontent-type: application/json; charset=utf-8\r\ncontent-encoding: zstd\r\ncontent-length: 7\r\n\r\n";
        assert!(parse_forwarded_head(compressed).unwrap().zstd);

        for (head, expected) in [
            (
                &b"GET /v1/chat/completions HTTP/1.1\r\ncontent-length: 0\r\n\r\n"[..],
                http::StatusCode::FORBIDDEN,
            ),
            (
                b"POST /admin/shutdown HTTP/1.1\r\ncontent-length: 0\r\n\r\n",
                http::StatusCode::FORBIDDEN,
            ),
            (
                b"POST /v1/chat/completions?x=1 HTTP/1.1\r\ncontent-length: 0\r\n\r\n",
                http::StatusCode::FORBIDDEN,
            ),
            (
                b"POST /v1/chat/completions HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n",
                http::StatusCode::BAD_REQUEST,
            ),
            (
                b"POST /v1/chat/completions HTTP/1.1\r\n\r\n",
                http::StatusCode::BAD_REQUEST,
            ),
            (
                b"POST /v1/chat/completions HTTP/1.1\r\ncontent-length: 1\r\ncontent-length: 2\r\n\r\n",
                http::StatusCode::BAD_REQUEST,
            ),
            (
                b"POST /v1/chat/completions HTTP/1.1\r\ncontent-length: 1\r\ncontent-encoding: br\r\n\r\n",
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (
                b"POST /v1/chat/completions HTTP/1.1\r\ncontent-length: 1\r\ncontent-type: text/plain\r\n\r\n",
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
        ] {
            assert_eq!(
                rejection_status(parse_forwarded_head(head)),
                expected,
                "{}",
                String::from_utf8_lossy(head)
            );
        }
        let oversized = format!(
            "POST /v1/chat/completions HTTP/1.1\r\ncontent-length: {}\r\n\r\n",
            MAX_FORWARDED_BODY_BYTES + 1
        );
        assert_eq!(
            rejection_status(parse_forwarded_head(oversized.as_bytes())),
            http::StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn rebuilt_local_request_carries_only_allowlisted_headers() {
        let head =
            String::from_utf8(local_request_head("127.0.0.1:8000".parse().unwrap(), 12)).unwrap();
        assert_eq!(
            head,
            "POST /v1/chat/completions HTTP/1.1\r\nhost: 127.0.0.1:8000\r\ncontent-type: application/json\r\ncontent-length: 12\r\naccept: text/event-stream\r\nconnection: close\r\n\r\n"
        );
    }

    #[test]
    fn zstd_decode_is_capped_and_rejects_garbage() {
        let body = vec![b'a'; 64 * 1024];
        let compressed = zstd::bulk::compress(&body, 3).unwrap();
        assert_eq!(
            decompress_zstd_capped(&compressed, body.len()).unwrap(),
            body
        );
        assert!(matches!(
            decompress_zstd_capped(&compressed, body.len() - 1),
            Err(UploadError::Rejected {
                status: http::StatusCode::PAYLOAD_TOO_LARGE,
                ..
            })
        ));
        assert!(matches!(
            decompress_zstd_capped(b"not zstd at all", 1024),
            Err(UploadError::Rejected {
                status: http::StatusCode::BAD_REQUEST,
                ..
            })
        ));
    }

    #[test]
    fn model_settling_tracks_fleet_transitions() {
        let model = |phase: &str, desired: &str, ready: u32, wanted: u32| {
            let mut model =
                SwitchableModelAdvertisement::legacy("qwen", None, phase, desired, 1, Vec::new());
            model.ready_instances = ready;
            model.desired_instances = wanted;
            model
        };
        assert!(model_is_settling(&model("loading", "loaded", 0, 1)));
        assert!(model_is_settling(&model("starting", "loaded", 0, 1)));
        assert!(model_is_settling(&model("unloaded", "loaded", 0, 1)));
        assert!(model_is_settling(&model("ready", "loaded", 1, 2)));
        assert!(!model_is_settling(&model("ready", "loaded", 1, 1)));
        assert!(!model_is_settling(&model("unloaded", "unloaded", 0, 0)));
        assert!(!model_is_settling(&model("failed", "loaded", 0, 1)));
    }

    #[tokio::test]
    async fn lifecycle_requests_poll_fast_only_while_a_model_is_settling() {
        let runtime = test_runtime(test_resource(
            "127.0.0.1:9".parse().unwrap(),
            MeshWorkerCapacitySource::Configured,
        ));
        let normal = Duration::from_secs(60);
        let now = TokioInstant::now();
        assert_eq!(runtime.next_model_refresh_delay(normal, now).await, normal);

        *runtime.model_switching.lock().await = Some(ModelSwitchingAdvertisement {
            provider: "lil-fleet".into(),
            models: vec![SwitchableModelAdvertisement::legacy(
                "qwen",
                None,
                "loading",
                "loaded",
                1,
                Vec::new(),
            )],
            revision: 1,
        });
        runtime.begin_fast_model_refresh();
        let now = TokioInstant::now();
        assert_eq!(
            runtime.next_model_refresh_delay(normal, now).await,
            FAST_MODEL_REFRESH_INTERVAL
        );
        // The fast window is bounded even if Fleet never settles.
        assert_eq!(
            runtime
                .next_model_refresh_delay(normal, now + FAST_MODEL_REFRESH_WINDOW)
                .await,
            normal
        );

        runtime
            .model_switching
            .lock()
            .await
            .as_mut()
            .unwrap()
            .models[0]
            .phase = "ready".into();
        assert_eq!(
            runtime
                .next_model_refresh_delay(normal, TokioInstant::now())
                .await,
            normal
        );
        assert!(
            runtime.fast_refresh_until.lock().unwrap().is_none(),
            "a settled inventory ends fast polling"
        );
    }

    #[tokio::test]
    async fn worker_advertises_zstd_request_support() {
        let runtime = test_runtime(test_resource(
            "127.0.0.1:9".parse().unwrap(),
            MeshWorkerCapacitySource::Configured,
        ));
        assert!(
            runtime
                .advertisement()
                .await
                .accepts_request_encoding(REQUEST_ENCODING_ZSTD)
        );
    }

    #[test]
    fn vllm_capacity_rejects_missing_invalid_and_unbounded_values() {
        assert_eq!(
            vllm_capacity(&serde_json::json!({
                "vllm_config": {"scheduler_config": {"max_num_seqs": 64}}
            }))
            .unwrap(),
            64
        );
        for invalid in [
            serde_json::Value::Null,
            serde_json::json!(0),
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!("64"),
            serde_json::json!(65_536),
        ] {
            assert!(
                vllm_capacity(&serde_json::json!({
                    "vllm_config": {"scheduler_config": {"max_num_seqs": invalid}}
                }))
                .is_err()
            );
        }
        assert!(vllm_capacity(&serde_json::json!({})).is_err());
        assert!(vllm_capacity(&serde_json::json!({"vllm_config": "text config"})).is_err());
    }

    #[tokio::test]
    async fn vllm_discovery_tracks_shared_port_and_same_model_capacity_changes() {
        let server = MockServer::start().await;
        let runtime = test_runtime(test_resource(
            *server.address(),
            MeshWorkerCapacitySource::Vllm,
        ));
        let resource = &runtime.resources["shared-port"];
        assert_eq!(resource.gate.snapshot().limit, 0);
        assert!(!resource.advertisement().await.healthy);
        for (model, capacity) in [
            ("DeepSeek-V4.1-Flash", 32),
            ("Qwen-27B", 64),
            ("DeepSeek-V4.1-Flash", 32),
            ("DeepSeek-V4.1-Flash", 64),
        ] {
            server.reset().await;
            mount_discovery(&server, model, capacity_response(capacity)).await;
            let updates = refresh_models(&runtime).await;
            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].effective_capacity, capacity);
            assert_eq!(updates[0].models[0].id, model);
            assert_eq!(updates[0].models[0].context_limit, Some(16384));
            assert!(updates[0].healthy);
            assert!(resource.apply_schedule(Utc::now()).await.is_none());
            assert_eq!(
                runtime.heartbeat(1).await.resources[0].effective_capacity,
                capacity
            );
        }
        assert!(refresh_models(&runtime).await.is_empty());
    }

    #[tokio::test]
    async fn failed_vllm_discovery_clears_capacity_until_valid_recovery() {
        let server = MockServer::start().await;
        let runtime = test_runtime(test_resource(
            *server.address(),
            MeshWorkerCapacitySource::Vllm,
        ));
        mount_discovery(&server, "Qwen-27B", capacity_response(64)).await;
        refresh_models(&runtime).await;
        for info in [
            ResponseTemplate::new(404),
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"vllm_config": {"scheduler_config": {}}})),
        ] {
            server.reset().await;
            mount_discovery(&server, "DeepSeek-V4.1-Flash", info).await;
            refresh_models(&runtime).await;
            let resource = &runtime.resources["shared-port"];
            let advertisement = resource.advertisement().await;
            assert!(!advertisement.healthy);
            assert!(!advertisement.accepting_requests);
            assert!(advertisement.models.is_empty());
            assert_eq!(advertisement.effective_capacity, 0);
            assert!(resource.apply_schedule(Utc::now()).await.is_none());
            assert_eq!(resource.gate.snapshot().limit, 0);
        }
        server.reset().await;
        mount_discovery(&server, "DeepSeek-V4.1-Flash", capacity_response(32)).await;
        let updates = refresh_models(&runtime).await;
        assert_eq!(updates[0].effective_capacity, 32);
        assert!(updates[0].healthy);
    }

    #[tokio::test]
    async fn vllm_capacity_preserves_explicit_schedule_limits() {
        let mut config = test_resource(
            "127.0.0.1:8115".parse().unwrap(),
            MeshWorkerCapacitySource::Vllm,
        );
        config.availability = schedule(serde_json::json!({
            "timezone": "UTC", "default_capacity": 6,
            "weekly": [{"days": ["mon"], "start_local": "12:00", "end_local": "15:00", "capacity": 4}],
            "exceptions": [{"start": "2026-09-28T13:00:00Z", "end": "2026-09-28T14:00:00Z", "capacity": 0}]
        }));
        let resource = LocalResource::new(config);
        *resource.detected_capacity.lock().await = Some(64);
        for (time, expected) in [
            ("2026-09-29T12:00:00Z", 64),
            ("2026-09-28T12:30:00Z", 4),
            ("2026-09-28T13:30:00Z", 0),
            ("2026-09-29T12:00:00Z", 64),
        ] {
            resource.apply_schedule(time.parse().unwrap()).await;
            assert_eq!(resource.gate.snapshot().limit, expected);
        }
        *resource.detected_capacity.lock().await = Some(2);
        resource
            .apply_schedule("2026-09-28T12:30:00Z".parse().unwrap())
            .await;
        assert_eq!(resource.gate.snapshot().limit, 2);
    }

    #[tokio::test]
    async fn configured_capacity_does_not_query_vllm_server_info() {
        let server = MockServer::start().await;
        mount_discovery(&server, "generic-model", ResponseTemplate::new(404)).await;
        let runtime = test_runtime(test_resource(
            *server.address(),
            MeshWorkerCapacitySource::Configured,
        ));
        let updates = refresh_models(&runtime).await;
        assert_eq!(updates[0].effective_capacity, 6);
        assert!(updates[0].healthy);
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url.path(), "/v1/models");
    }

    #[test]
    fn worker_capacity_gate_drains_after_shrink() {
        let gate = CapacityGate::new(2);
        let a = gate.try_acquire().expect("a");
        let b = gate.try_acquire().expect("b");
        assert!(gate.try_acquire().is_none());
        gate.set_limit(1);
        drop(a);
        assert!(gate.try_acquire().is_none());
        drop(b);
        assert!(gate.try_acquire().is_some());
    }

    #[test]
    fn configured_models_filter_discovered_catalog() {
        let discovered = vec![
            ModelAdvertisement {
                id: "safe-model".to_string(),
                context_limit: Some(4096),
            },
            ModelAdvertisement {
                id: "surprise-model".to_string(),
                context_limit: Some(8192),
            },
        ];

        let filtered = filter_advertised_models(discovered, &["SAFE-MODEL".to_string()]);

        assert_eq!(
            filtered,
            vec![ModelAdvertisement {
                id: "SAFE-MODEL".to_string(),
                context_limit: Some(4096),
            }]
        );
    }

    #[tokio::test]
    async fn worker_advertisement_uses_configured_node_name() {
        let runtime = WorkerRuntime {
            config: MeshWorkerConfig {
                node_name: Some("lab-worker".to_string()),
                ..Default::default()
            },
            resources: HashMap::new(),
            fleet: None,
            model_switching: Mutex::new(None),
            fast_refresh_until: std::sync::Mutex::new(None),
            refresh_wakeup: tokio::sync::Notify::new(),
        };

        let advertisement = runtime.advertisement().await;

        assert_eq!(advertisement.node_name.as_deref(), Some("lab-worker"));
        assert!(advertisement.resources.is_empty());
    }

    #[test]
    fn enrollment_request_uses_configured_node_name() {
        let config = MeshWorkerConfig {
            node_name: Some("lab-worker".to_string()),
            ..Default::default()
        };

        let request = enroll_request(&config, "join-token".to_string());

        assert_eq!(request.node_name.as_deref(), Some("lab-worker"));
        assert_eq!(request.join_key, "join-token");
    }

    fn schedule(json: serde_json::Value) -> AvailabilitySchedule {
        serde_json::from_value(json).expect("schedule")
    }

    #[test]
    fn schedule_handles_weekly_cross_midnight_and_exceptions() {
        let schedule = schedule(serde_json::json!({
            "timezone": "America/Chicago",
            "default_capacity": 1,
            "weekly": [
                {"days":["mon"], "start_local":"22:00", "end_local":"02:00", "capacity": 4}
            ],
            "exceptions": [
                {"start":"2026-09-22T01:00:00-05:00", "end":"2026-09-22T01:30:00-05:00", "capacity": 0}
            ]
        }));
        let active = DateTime::parse_from_rfc3339("2026-09-22T04:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let inactive = DateTime::parse_from_rfc3339("2026-09-23T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let exception = DateTime::parse_from_rfc3339("2026-09-22T06:15:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(effective_capacity_at(&schedule, active), 4);
        assert_eq!(effective_capacity_at(&schedule, inactive), 1);
        assert_eq!(effective_capacity_at(&schedule, exception), 0);
    }

    #[test]
    fn schedule_uses_iana_timezone_across_dst_transition() {
        let schedule = schedule(serde_json::json!({
            "timezone": "America/Chicago",
            "default_capacity": 1,
            "weekly": [
                {"days":["sun"], "start_local":"01:00", "end_local":"04:00", "capacity": 8}
            ],
            "exceptions": []
        }));
        let before_jump = DateTime::parse_from_rfc3339("2026-03-08T07:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let after_jump = DateTime::parse_from_rfc3339("2026-03-08T08:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let after_window = DateTime::parse_from_rfc3339("2026-03-08T09:30:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(effective_capacity_at(&schedule, before_jump), 8);
        assert_eq!(effective_capacity_at(&schedule, after_jump), 8);
        assert_eq!(effective_capacity_at(&schedule, after_window), 1);
    }
}
