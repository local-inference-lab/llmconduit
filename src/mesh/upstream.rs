use crate::error::{AppError, AppResult, FailoverDisposition};
use crate::mesh::io::{
    Admission, MAX_HTTP_CHUNK_BYTES, MAX_HTTP_CHUNK_LINE_BYTES, RequestOpen, content_length,
    drain_http_chunk, is_chunked, read_admission, read_http_response_head,
    split_http_response_head, write_request_open,
};
use crate::mesh::protocol::{AdmissionRejectCode, ModelAdvertisement, REQUEST_PROTOCOL_VERSION};
use crate::mesh::registry::{MeshRegistry, MeshReservationError};
use crate::models::chat::{ChatCompletionChunk, ChatCompletionRequest};
use crate::upstream::{
    BackendCandidate, BackendCandidatePlan, BackendChatRequest, BackendFinalizationPolicies,
    ProviderHealth, ProviderInventoryEntry, ProviderStatus, UpstreamClient, UpstreamModelEntry,
    UpstreamModelsResponse, UpstreamStream, finalize_request_for_backend,
    offload_redacted_upstream_request_bytes, sanitize_chat_request, stamp_header_byte,
};
use async_stream::try_stream;
use async_trait::async_trait;
use axum::body::Bytes;
use futures::StreamExt;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio_util::bytes::BytesMut;

const MESH_ERROR_BODY_READ_LIMIT: usize = 16 * 1024;
const MESH_ERROR_BODY_DISPLAY_LIMIT: usize = 500;
const DEFAULT_MESH_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Largest slice taken from the QUIC receive buffer per read. Chunks are
/// handed through without copying, so this only bounds per-read latency.
const MESH_BODY_READ_CHUNK_BYTES: usize = 64 * 1024;
/// Small chat bodies are not worth a compression pass on the hub's 2 vCPUs.
const MESH_COMPRESSION_MIN_BYTES: usize = 8 * 1024;
/// Fast levels only: the hub pays this CPU on every large mesh request.
const MESH_ZSTD_LEVEL: i32 = 2;

#[derive(Debug, Clone)]
pub struct MeshUpstreamClient {
    registry: Arc<MeshRegistry>,
    finalization_policies: BackendFinalizationPolicies,
    flatten_content: bool,
    max_sse_frame_bytes: usize,
    flow_store: crate::dashboard_flow::DashboardFlowStore,
    capacity_wait_timeout: Duration,
    first_byte_timeout: Duration,
    idle_timeout: Duration,
}

impl MeshUpstreamClient {
    pub(crate) fn new(
        registry: Arc<MeshRegistry>,
        finalization_policies: BackendFinalizationPolicies,
        flatten_content: bool,
        max_sse_frame_bytes: usize,
        flow_store: crate::dashboard_flow::DashboardFlowStore,
    ) -> Self {
        Self {
            registry,
            finalization_policies,
            flatten_content,
            max_sse_frame_bytes,
            flow_store,
            capacity_wait_timeout: DEFAULT_MESH_REQUEST_TIMEOUT,
            first_byte_timeout: DEFAULT_MESH_REQUEST_TIMEOUT,
            idle_timeout: DEFAULT_MESH_REQUEST_TIMEOUT,
        }
    }

    /// The gateway passes its `request_timeout` here. It bounds the wait for
    /// worker capacity and, once a worker admits the request, the wait for
    /// its first response chunk and every later gap between body reads, so
    /// a worker that accepts and then hangs cannot pin the request forever.
    pub(crate) fn with_capacity_wait_timeout(mut self, timeout: Duration) -> Self {
        self.capacity_wait_timeout = timeout;
        self.first_byte_timeout = timeout;
        self.idle_timeout = timeout;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_response_timeouts(mut self, first_byte: Duration, idle: Duration) -> Self {
        self.first_byte_timeout = first_byte;
        self.idle_timeout = idle;
        self
    }
}

#[async_trait]
impl UpstreamClient for MeshUpstreamClient {
    async fn stream_chat_completion(
        &self,
        backend: &BackendChatRequest,
    ) -> AppResult<UpstreamStream> {
        let mut backend = backend.clone();
        finalize_request_for_backend(&mut backend, &self.finalization_policies);
        let model = backend.request.model.clone();
        let request = sanitize_chat_request(backend.request, self.flatten_content);
        if self.flow_store.is_enabled()
            && let Some(response_id) = backend.response_id.as_deref()
        {
            self.flow_store.set_upstream(
                response_id,
                Some("mesh://workers".to_string()),
                Some(model.clone()),
                Some(crate::dashboard_flow::capture_body_from_value(&request)),
            );
        }
        if let Some(capture) = backend.capture.as_ref() {
            match offload_redacted_upstream_request_bytes(&request).await {
                Some(redacted) => capture.write_upstream_request(&redacted),
                None => capture.mark_upstream_request_redaction_failed(),
            }
        }
        let body = MeshRequestBody::encode(&request)?;
        let mut excluded = HashSet::new();
        let mut last_error = None;
        let capacity_deadline = tokio::time::Instant::now() + self.capacity_wait_timeout;
        loop {
            let authorization = backend.authorization.clone();
            let route = backend.authorization_route.clone();
            let reservation = match self
                .registry
                .reserve_excluding_where_with_affinity_wait(
                    &model,
                    &excluded,
                    backend.affinity.as_ref(),
                    |endpoint_id, resource_id| {
                        authorization.allows_candidate(
                            &format!("mesh:{endpoint_id}"),
                            route.as_deref().or(Some(resource_id)),
                            &model,
                            backend.endpoint,
                        )
                    },
                    capacity_deadline.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await
            {
                Ok(Some(reservation)) => reservation,
                Ok(None) => {
                    let any_authorized =
                        self.registry
                            .has_candidate_where(&model, |endpoint_id, resource_id| {
                                authorization.allows_candidate(
                                    &format!("mesh:{endpoint_id}"),
                                    route.as_deref().or(Some(resource_id)),
                                    &model,
                                    backend.endpoint,
                                )
                            });
                    if !any_authorized && self.registry.has_candidate_where(&model, |_, _| true) {
                        return Err(AppError::forbidden(
                            "no authorized mesh resource is available for this request",
                        ));
                    }
                    return Err(last_error.unwrap_or_else(|| {
                        AppError::upstream_with_disposition(
                            "mesh capacity exhausted",
                            FailoverDisposition::FailoverNoCooldown,
                        )
                    }));
                }
                Err(MeshReservationError::CapacityWaitTimedOut { pinned }) => {
                    return Err(AppError::upstream_with_disposition(
                        if pinned {
                            "mesh session capacity wait timed out"
                        } else {
                            "mesh capacity wait timed out"
                        },
                        FailoverDisposition::FailoverNoCooldown,
                    ));
                }
                Err(MeshReservationError::PinnedCapacityExhausted) => {
                    unreachable!("async mesh admission waits for occupied session capacity")
                }
            };
            let candidate = (
                reservation.resource.endpoint_id,
                reservation.resource.resource_id.clone(),
            );
            let affinity_commit = reservation.affinity_commit();
            if let Some(capture) = backend.capture.as_ref() {
                capture.reset_upstream_response();
            }
            let wire = if reservation.resource.local_model_id == request.model {
                body.wire(reservation.resource.accepts_zstd).await?
            } else {
                // The catalog shows the allowlist spelling; this worker's
                // engine matches its own spelling case-sensitively.
                let mut local = request.clone();
                local.model = reservation.resource.local_model_id.clone();
                MeshRequestBody::encode(&local)?
                    .wire(reservation.resource.accepts_zstd)
                    .await?
            };
            match open_mesh_http_stream(
                reservation,
                wire,
                MeshStreamLimits {
                    max_chunk_bytes: self.max_sse_frame_bytes,
                    first_byte_timeout: self.first_byte_timeout,
                    idle_timeout: self.idle_timeout,
                },
                capacity_deadline,
            )
            .await
            {
                Ok((stream, first_byte_deadline)) => {
                    stamp_header_byte(backend.serving.as_ref());
                    let mut stream =
                        parse_sse_stream(stream, self.max_sse_frame_bytes, backend.capture.clone());
                    // Still pre-first-chunk, so a stall here is safe to fail
                    // over: nothing has reached the client yet.
                    let first = tokio::time::timeout_at(first_byte_deadline, stream.next())
                        .await
                        .unwrap_or_else(|_| {
                            Some(Err(first_byte_timeout_error(self.first_byte_timeout)))
                        });
                    match first {
                        Some(Ok(first)) => {
                            if let Some(commit) = affinity_commit {
                                self.registry.commit_affinity(commit);
                            }
                            if let Some(serving) = &backend.serving {
                                serving.set_provider(format!("mesh:{}", candidate.0));
                                serving.set_model_served_final(model.clone());
                            }
                            return Ok(Box::pin(
                                futures::stream::once(async move { Ok(first) }).chain(stream),
                            ));
                        }
                        Some(Err(err))
                            if err.failover_disposition() != FailoverDisposition::Terminal =>
                        {
                            tracing::warn!(
                                endpoint_id = %candidate.0,
                                resource_id = %candidate.1,
                                error = %err,
                                "mesh attempt failed before its first SSE chunk; trying another resource"
                            );
                            excluded.insert(candidate);
                            last_error = Some(err);
                        }
                        Some(Err(err)) => return Err(err),
                        None => {
                            excluded.insert(candidate);
                            last_error = Some(AppError::upstream(
                                "mesh worker response ended before its first SSE chunk",
                            ));
                        }
                    }
                }
                Err(MeshOpenError::CapacityWaitTimedOut) => {
                    return Err(AppError::upstream_with_disposition(
                        "mesh worker capacity wait timed out",
                        FailoverDisposition::FailoverNoCooldown,
                    ));
                }
                Err(MeshOpenError::CapacityExhausted) => {
                    // Worker teardown can lag the hub permit release. Busy is
                    // not a failed runner, so preserve an existing session pin.
                    if tokio::time::Instant::now() >= capacity_deadline {
                        return Err(AppError::upstream_with_disposition(
                            "mesh worker capacity wait timed out",
                            FailoverDisposition::FailoverNoCooldown,
                        ));
                    }
                    tracing::debug!(
                        endpoint_id = %candidate.0,
                        resource_id = %candidate.1,
                        "mesh worker capacity still occupied; retaining session affinity"
                    );
                    tokio::time::sleep_until(
                        (tokio::time::Instant::now() + Duration::from_millis(100))
                            .min(capacity_deadline),
                    )
                    .await;
                }
                Err(MeshOpenError::Other(err))
                    if err.failover_disposition() != FailoverDisposition::Terminal =>
                {
                    tracing::warn!(
                        endpoint_id = %candidate.0,
                        resource_id = %candidate.1,
                        error = %err,
                        "mesh attempt failed before the first response body; trying another resource"
                    );
                    excluded.insert(candidate);
                    last_error = Some(err);
                }
                Err(MeshOpenError::Other(err)) => return Err(err),
            }
        }
    }

    async fn list_models(&self) -> AppResult<UpstreamModelsResponse> {
        let body = mesh_models_list_body(self.registry.model_catalog());
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        Ok(UpstreamModelsResponse {
            status: http::StatusCode::OK,
            headers,
            body: Bytes::from(serde_json::to_vec(&body).map_err(|err| {
                AppError::internal(format!("failed to encode mesh model catalog: {err}"))
            })?),
        })
    }

    async fn supported_model_catalog(&self) -> AppResult<Vec<UpstreamModelEntry>> {
        Ok(self
            .registry
            .model_catalog()
            .into_iter()
            .map(|model| UpstreamModelEntry {
                id: model.id,
                context_limit: model.context_limit,
            })
            .collect())
    }

    async fn provider_inventory(&self) -> AppResult<Vec<ProviderInventoryEntry>> {
        Ok(self.registry.provider_inventory())
    }

    async fn backend_candidate_plan(&self, requested_model: &str) -> BackendCandidatePlan {
        let candidates = self
            .registry
            .model_catalog()
            .into_iter()
            .filter(|model| model.id.eq_ignore_ascii_case(requested_model))
            .map(|model| BackendCandidate {
                model: model.id,
                context_limit: model.context_limit,
            })
            .collect();
        BackendCandidatePlan { candidates }
    }

    fn provider_base_url(&self) -> String {
        "mesh://workers".to_string()
    }

    fn provider_health(&self) -> Vec<ProviderHealth> {
        let mut by_provider = BTreeMap::<String, (bool, HashSet<String>)>::new();
        for entry in self.registry.provider_inventory() {
            let aggregate = by_provider
                .entry(entry.provider_id)
                .or_insert_with(|| (false, HashSet::new()));
            aggregate.0 |= entry.healthy;
            aggregate
                .1
                .extend(entry.models.into_iter().map(|model| model.id));
        }
        by_provider
            .into_iter()
            .map(|(id, (healthy, models))| ProviderHealth {
                name: id.clone(),
                id: id.clone(),
                route: None,
                base_url: id.replacen("mesh:", "mesh://", 1),
                status: if healthy {
                    ProviderStatus::Healthy
                } else {
                    ProviderStatus::Down
                },
                cooling_until_ms: None,
                last_error: (!healthy).then(|| "mesh worker is unavailable".to_string()),
                served_count: 0,
                failover_count: 0,
                consecutive_failures: 0,
                catalog_fetched_ms: None,
                catalog_size: Some(models.len() as u64),
            })
            .collect()
    }

    fn model_catalog_cache_ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
}

struct MeshHttpStream {
    _reservation: crate::mesh::registry::MeshReservation,
    recv: iroh::endpoint::RecvStream,
    write_task: Option<tokio::task::JoinHandle<AppResult<()>>>,
    remaining_content_length: Option<usize>,
    chunked: bool,
    chunk_buf: BytesMut,
    max_chunk_bytes: usize,
    idle_timeout: Duration,
}

#[derive(Debug, Clone, Copy)]
struct MeshStreamLimits {
    max_chunk_bytes: usize,
    first_byte_timeout: Duration,
    idle_timeout: Duration,
}

enum MeshOpenError {
    CapacityExhausted,
    CapacityWaitTimedOut,
    Other(AppError),
}

impl From<AppError> for MeshOpenError {
    fn from(error: AppError) -> Self {
        Self::Other(error)
    }
}

/// On success also returns the deadline for the first SSE chunk, measured
/// from worker admission.
async fn open_mesh_http_stream(
    reservation: crate::mesh::registry::MeshReservation,
    http_request: MeshWireRequest,
    limits: MeshStreamLimits,
    capacity_deadline: tokio::time::Instant,
) -> Result<(MeshHttpStream, tokio::time::Instant), MeshOpenError> {
    let connection = reservation
        .resource
        .connection
        .clone()
        .ok_or_else(|| AppError::upstream("mesh reservation has no live worker connection"))?;
    let (mut send, recv) = connection
        .open_bi()
        .await
        .map_err(|err| AppError::upstream(format!("failed to open mesh request stream: {err}")))?;
    let open = RequestOpen {
        protocol_version: REQUEST_PROTOCOL_VERSION,
        request_id: reservation.request_id,
        resource_id: reservation.resource.resource_id.clone(),
    };
    write_request_open(&mut send, &open).await?;

    let request_id = open.request_id;
    let resource_id = open.resource_id.clone();
    let write_task = tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        let request_bytes = http_request.head.len() + http_request.body.len();
        for part in [&http_request.head, &http_request.body] {
            send.write_all(part).await.map_err(|err| {
                AppError::upstream(format!("failed to write mesh HTTP request: {err}"))
            })?;
        }
        let write_ms = started.elapsed().as_millis();
        send.shutdown().await.map_err(|err| {
            AppError::upstream(format!("failed to finish mesh HTTP request: {err}"))
        })?;
        let elapsed_ms = started.elapsed().as_millis();
        if elapsed_ms >= 1000 {
            tracing::warn!(
                %request_id,
                %resource_id,
                request_bytes,
                write_ms,
                elapsed_ms,
                "slow mesh request upload"
            );
        }
        Ok(())
    });

    // Own the writer before awaiting admission so cancellation/rejection
    // aborts it together with the reservation instead of detaching the task.
    let mut stream = MeshHttpStream {
        _reservation: reservation,
        recv,
        write_task: Some(write_task),
        remaining_content_length: None,
        chunked: false,
        chunk_buf: BytesMut::new(),
        max_chunk_bytes: limits.max_chunk_bytes,
        idle_timeout: limits.idle_timeout,
    };
    let admission = tokio::time::timeout_at(capacity_deadline, read_admission(&mut stream.recv))
        .await
        .map_err(|_| MeshOpenError::CapacityWaitTimedOut)??;
    match admission {
        Admission::Accepted => {}
        Admission::Rejected {
            code: AdmissionRejectCode::CapacityExhausted,
        } => {
            return Err(MeshOpenError::CapacityExhausted);
        }
        Admission::Rejected { code } => return Err(admission_error(code).into()),
    }

    // Admission only proves the worker reached its local engine. Bound the
    // wait for the engine's answer too: an accepted-then-silent worker would
    // otherwise hold this request (and its capacity permit) indefinitely.
    let first_byte_deadline = tokio::time::Instant::now() + limits.first_byte_timeout;
    let head = tokio::time::timeout_at(
        first_byte_deadline,
        read_http_response_head(&mut stream.recv, 64 * 1024),
    )
    .await
    .map_err(|_| first_byte_timeout_error(limits.first_byte_timeout))??;
    let (status, headers) = split_http_response_head(&head)?;
    stream.remaining_content_length = content_length(&headers);
    stream.chunked = is_chunked(&headers);
    if !status.is_success() {
        let body = stream.read_body_prefix(MESH_ERROR_BODY_READ_LIMIT).await?;
        stream.finish_writer().await?;
        let message = mesh_upstream_error_message(status, &body);
        if matches!(
            status,
            http::StatusCode::BAD_REQUEST
                | http::StatusCode::PAYLOAD_TOO_LARGE
                | http::StatusCode::UNSUPPORTED_MEDIA_TYPE
                | http::StatusCode::UNPROCESSABLE_ENTITY
        ) {
            return Err(AppError::upstream_with_disposition(
                message,
                FailoverDisposition::Terminal,
            )
            .into());
        }
        return Err(AppError::upstream(message).into());
    }

    Ok((stream, first_byte_deadline))
}

fn mesh_upstream_error_message(status: http::StatusCode, body: &[u8]) -> String {
    let prefix = format!("mesh worker local upstream returned {status}");
    if body.is_empty() {
        return prefix;
    }
    let body = String::from_utf8_lossy(body);
    format!(
        "{prefix}: {}",
        crate::upstream::redact_and_truncate_error_body(body.trim(), MESH_ERROR_BODY_DISPLAY_LIMIT,)
    )
}

fn admission_error(code: AdmissionRejectCode) -> AppError {
    match code {
        AdmissionRejectCode::CapacityExhausted
        | AdmissionRejectCode::ResourceUnhealthy
        | AdmissionRejectCode::LocalConnectFailed => AppError::upstream_with_disposition(
            format!("mesh worker rejected request: {code:?}"),
            FailoverDisposition::FailoverNoCooldown,
        ),
        other => AppError::upstream(format!("mesh worker rejected request: {other:?}")),
    }
}

/// The JSON chat body for one mesh request, encoded once and shared by every
/// attempt. The zstd variant is computed at most once, and only if some
/// attempt lands on a worker that advertised it can decode zstd.
struct MeshRequestBody {
    json: Bytes,
    zstd: tokio::sync::OnceCell<Option<Bytes>>,
}

/// One attempt's request as written to the worker stream: the head and
/// body stay separate so a multi-MiB body is never copied into a combined
/// buffer.
struct MeshWireRequest {
    head: Bytes,
    body: Bytes,
}

impl MeshRequestBody {
    fn encode(request: &ChatCompletionRequest) -> AppResult<Self> {
        let body_len = crate::replay::serialized_len(request).map_err(|err| {
            AppError::internal(format!("failed to encode mesh chat request: {err}"))
        })?;
        // Sized exactly up front: a native-vision request may carry tens of
        // MiB of image data, so growth reallocations would double peak memory.
        let mut json = Vec::with_capacity(body_len);
        serde_json::to_writer(&mut json, request).map_err(|err| {
            AppError::internal(format!("failed to encode mesh chat request: {err}"))
        })?;
        Ok(Self {
            json: Bytes::from(json),
            zstd: tokio::sync::OnceCell::new(),
        })
    }

    async fn wire(&self, worker_accepts_zstd: bool) -> AppResult<MeshWireRequest> {
        if worker_accepts_zstd && self.json.len() >= MESH_COMPRESSION_MIN_BYTES {
            let compressed = self
                .zstd
                .get_or_init(|| compress_mesh_body(self.json.clone()))
                .await;
            if let Some(compressed) = compressed {
                return Ok(MeshWireRequest {
                    head: chat_http_request_head(
                        compressed.len(),
                        Some(crate::mesh::protocol::REQUEST_ENCODING_ZSTD),
                    ),
                    body: compressed.clone(),
                });
            }
        }
        Ok(MeshWireRequest {
            head: chat_http_request_head(self.json.len(), None),
            body: self.json.clone(),
        })
    }
}

/// `None` when compression fails or saves too little to be worth the
/// worker's decode (base64 image payloads barely shrink).
async fn compress_mesh_body(json: Bytes) -> Option<Bytes> {
    // Multi-MiB bodies take tens of milliseconds to compress; keep that off
    // the async workers of a 2-vCPU hub.
    let compressed = tokio::task::spawn_blocking(move || {
        zstd::bulk::compress(&json, MESH_ZSTD_LEVEL)
            .ok()
            .filter(|compressed| compressed.len() < json.len() - json.len() / 10)
    })
    .await
    .ok()
    .flatten()?;
    Some(Bytes::from(compressed))
}

fn chat_http_request_head(content_length: usize, content_encoding: Option<&str>) -> Bytes {
    let mut head = Vec::with_capacity(256);
    head.extend_from_slice(b"POST /v1/chat/completions HTTP/1.1\r\n");
    head.extend_from_slice(b"host: llmconduit-mesh-worker\r\n");
    head.extend_from_slice(b"content-type: application/json\r\n");
    if let Some(encoding) = content_encoding {
        head.extend_from_slice(format!("content-encoding: {encoding}\r\n").as_bytes());
    }
    head.extend_from_slice(format!("content-length: {content_length}\r\n").as_bytes());
    head.extend_from_slice(b"accept: text/event-stream\r\n");
    head.extend_from_slice(b"connection: close\r\n\r\n");
    Bytes::from(head)
}

fn first_byte_timeout_error(timeout: Duration) -> AppError {
    AppError::upstream(format!(
        "mesh worker produced no response within {}s of admitting the request",
        timeout.as_secs_f64()
    ))
}

fn parse_sse_stream(
    mut stream: MeshHttpStream,
    max_frame_bytes: usize,
    capture: Option<Arc<crate::turn_capture::TurnCaptureState>>,
) -> UpstreamStream {
    let s = try_stream! {
        let mut capture_guard = capture.clone().map(MeshCaptureGuard::new);
        let mut capture_sink = capture.as_ref().and_then(|capture| capture.upstream_response_sink());
        let mut capture_started = false;
        let mut frame_guard = crate::sse_guard::SseFrameGuard::new(max_frame_bytes);
        let mut event = String::new();
        let mut line_buf = Vec::new();
        let mut saw_done = false;
        let mut saw_finish_reason = false;
        while let Some(bytes) = stream.next_body_bytes().await? {
            if let Some(capture) = capture.as_ref() {
                if !capture_started {
                    capture.mark_upstream_response_streamed();
                    capture_started = true;
                }
                if let Some(sink) = capture_sink.as_mut() {
                    match std::future::poll_fn(|cx| sink.poll_reserve(cx)).await {
                        Ok(()) => {
                            if sink.send(bytes.to_vec()).is_err() {
                                capture.mark_upstream_response_degraded();
                                capture_sink = None;
                            }
                        }
                        Err(_) => {
                            capture.mark_upstream_response_degraded();
                            capture_sink = None;
                        }
                    }
                }
            }
            frame_guard.accept(&bytes)?;
            line_buf.extend_from_slice(&bytes);
            // Walk complete lines in place and compact once per read instead
            // of allocating and shifting the buffer for every line.
            let mut consumed = 0;
            while let Some(offset) = line_buf[consumed..].iter().position(|byte| *byte == b'\n') {
                let line_end = consumed + offset;
                let mut line = &line_buf[consumed..line_end];
                consumed = line_end + 1;
                if line.last() == Some(&b'\r') { line = &line[..line.len() - 1]; }
                let line = std::str::from_utf8(line)
                    .map_err(|err| AppError::upstream(format!("invalid UTF-8 in mesh SSE event: {err}")))?;
                if line.is_empty() {
                    if !event.is_empty() {
                        let data = std::mem::take(&mut event);
                        if data == "[DONE]" {
                            saw_done = true;
                        } else {
                            let chunk = parse_chat_completion_chunk(&data)?;
                            saw_finish_reason |= chunk
                                .choices
                                .iter()
                                .any(|choice| choice.finish_reason.is_some());
                            yield chunk;
                        }
                    }
                } else if let Some(data) = line.strip_prefix("data:") {
                    if !event.is_empty() {
                        event.push('\n');
                    }
                    event.push_str(data.trim_start());
                }
            }
            line_buf.drain(..consumed);
        }
        if !line_buf.is_empty() {
            if line_buf.last() == Some(&b'\r') { line_buf.pop(); }
            let line = std::str::from_utf8(&line_buf)
                .map_err(|err| AppError::upstream(format!("invalid UTF-8 in mesh SSE event: {err}")))?;
            if let Some(data) = line.strip_prefix("data:") {
                if !event.is_empty() {
                    event.push('\n');
                }
                event.push_str(data.trim_start());
            }
        }
        if event == "[DONE]" {
            saw_done = true;
        } else if !event.is_empty() {
            let chunk = parse_chat_completion_chunk(&event)?;
            saw_finish_reason |= chunk
                .choices
                .iter()
                .any(|choice| choice.finish_reason.is_some());
            yield chunk;
        }
        frame_guard.finish()?;
        stream.finish_writer().await?;
        if !saw_done {
            Err(AppError::upstream(
                "mesh upstream SSE ended before the [DONE] marker",
            ))?;
        }
        if !saw_finish_reason {
            // HTTP framing and [DONE] were both validated above. An omitted
            // finish reason is unknown metadata, not a truncated response.
            tracing::warn!(
                "mesh upstream SSE completed with [DONE] but without a terminal finish_reason"
            );
        }
        if let Some(capture) = capture.as_ref() {
            capture.mark_upstream_response_streamed();
        }
        drop(capture_sink);
        if let Some(guard) = capture_guard.as_mut() {
            guard.clean = true;
        }
    };
    Box::pin(s)
}

struct MeshCaptureGuard {
    capture: Arc<crate::turn_capture::TurnCaptureState>,
    clean: bool,
}

impl MeshCaptureGuard {
    fn new(capture: Arc<crate::turn_capture::TurnCaptureState>) -> Self {
        Self {
            capture,
            clean: false,
        }
    }
}

impl Drop for MeshCaptureGuard {
    fn drop(&mut self) {
        self.capture.upstream_response_done(!self.clean);
    }
}

impl MeshHttpStream {
    async fn finish_writer(&mut self) -> AppResult<()> {
        let Some(write_task) = self.write_task.take() else {
            return Ok(());
        };
        write_task
            .await
            .map_err(|err| AppError::internal(format!("mesh writer task failed: {err}")))?
    }

    /// One zero-copy slice of the QUIC receive buffer, or `None` at EOF.
    /// Every read is bounded by the idle timeout: once the first chunk has
    /// gone to the client the stream cannot fail over, but a silent worker
    /// must still surface as an error instead of a hung response.
    async fn read_chunk(&mut self) -> AppResult<Option<Bytes>> {
        match tokio::time::timeout(
            self.idle_timeout,
            self.recv.read_chunk(MESH_BODY_READ_CHUNK_BYTES),
        )
        .await
        {
            Ok(Ok(chunk)) => Ok(chunk),
            Ok(Err(err)) => Err(AppError::upstream(format!(
                "failed to read mesh response body: {err}"
            ))),
            Err(_) => Err(AppError::upstream(format!(
                "mesh response body stalled for {}s",
                self.idle_timeout.as_secs_f64()
            ))),
        }
    }

    async fn next_body_bytes(&mut self) -> AppResult<Option<Bytes>> {
        if self.chunked {
            loop {
                if let Some(chunk) = drain_http_chunk(&mut self.chunk_buf, self.max_chunk_bytes)? {
                    if chunk.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(chunk));
                }
                let Some(read) = self.read_chunk().await? else {
                    return Err(AppError::upstream(
                        "mesh chunked response ended before its terminator",
                    ));
                };
                let read_len = read.len();
                let max_buffered = MAX_HTTP_CHUNK_LINE_BYTES
                    .checked_add(2)
                    .and_then(|value| {
                        value.checked_add(self.max_chunk_bytes.min(MAX_HTTP_CHUNK_BYTES))
                    })
                    .and_then(|value| value.checked_add(2))
                    .ok_or_else(|| AppError::upstream("mesh HTTP chunk buffer limit overflow"))?;
                if self.chunk_buf.len().saturating_add(read_len) > max_buffered {
                    return Err(AppError::upstream(format!(
                        "mesh HTTP chunk buffer exceeds {max_buffered} byte limit"
                    )));
                }
                self.chunk_buf.extend_from_slice(&read);
            }
        }

        if matches!(self.remaining_content_length, Some(0)) {
            return Ok(None);
        }
        let Some(read) = self.read_chunk().await? else {
            if let Some(remaining) = self.remaining_content_length
                && remaining > 0
            {
                return Err(AppError::upstream(format!(
                    "mesh response body ended with {remaining} Content-Length bytes remaining"
                )));
            }
            return Ok(None);
        };
        if let Some(remaining) = self.remaining_content_length.as_mut() {
            if read.len() > *remaining {
                return Err(AppError::upstream(format!(
                    "mesh response body exceeded Content-Length by {} bytes",
                    read.len() - *remaining
                )));
            }
            *remaining -= read.len();
        }
        Ok(Some(read))
    }

    async fn read_body_prefix(&mut self, limit: usize) -> AppResult<Vec<u8>> {
        let mut body = Vec::new();
        while body.len() < limit {
            let Some(bytes) = self.next_body_bytes().await? else {
                break;
            };
            let remaining = limit - body.len();
            body.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
        }
        Ok(body)
    }
}

impl Drop for MeshHttpStream {
    fn drop(&mut self) {
        if let Some(write_task) = self.write_task.take() {
            write_task.abort();
        }
    }
}

fn parse_chat_completion_chunk(data: &str) -> AppResult<ChatCompletionChunk> {
    match serde_json::from_str::<ChatCompletionChunk>(data) {
        Ok(chunk) => Ok(chunk),
        Err(first_error) => {
            let mut value = serde_json::from_str::<Value>(data).map_err(|_| {
                AppError::upstream(format!(
                    "failed to parse upstream chat chunk: {first_error}"
                ))
            })?;
            if !normalize_sparse_tool_call_types(&mut value) {
                return Err(AppError::upstream(format!(
                    "failed to parse upstream chat chunk: {first_error}"
                )));
            }
            serde_json::from_value(value).map_err(|err| {
                AppError::upstream(format!("failed to parse upstream chat chunk: {err}"))
            })
        }
    }
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

fn mesh_models_list_body(catalog: Vec<ModelAdvertisement>) -> Value {
    let mut by_id = BTreeMap::<String, Option<i64>>::new();
    for model in catalog {
        by_id.entry(model.id).or_insert(model.context_limit);
    }
    let data: Vec<_> = by_id
        .into_iter()
        .map(|(id, context_limit)| {
            let mut object = serde_json::Map::new();
            object.insert("id".to_string(), Value::String(id));
            object.insert("object".to_string(), Value::String("model".to_string()));
            if let Some(limit) = context_limit {
                object.insert("context_length".to_string(), Value::from(limit));
            }
            Value::Object(object)
        })
        .collect();
    serde_json::json!({ "object": "list", "data": data })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AvailabilitySchedule, MeshWorkerConfig, MeshWorkerResourceConfig};
    use crate::mesh::protocol::{
        PROTOCOL_VERSION, ResourceAdvertisement, WORKER_ALPN, WorkerAdvertisement,
    };
    use crate::mesh::worker::{WorkerRuntime, handle_stream};
    use crate::upstream::{AuthorizationScope, InferenceEndpoint, RequestAffinity};
    use futures::poll;
    use iroh::endpoint::Connection;
    use iroh::endpoint::presets;
    use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey, TransportAddr};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::task::Poll;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::{JoinHandle, JoinSet};

    fn large_image_request() -> ChatCompletionRequest {
        let mut request = chat_request("describe π and \"quotes\"");
        request.messages[0].content = Some(serde_json::json!([
            {"type": "text", "text": "describe π and \"quotes\""},
            {"type": "image_url", "image_url": {"url": format!("data:image/png;base64,{}", "A".repeat(2 * 1024 * 1024))}}
        ]));
        request
    }

    #[tokio::test]
    async fn mesh_wire_request_preserves_json_and_content_length_for_large_images() {
        let request = large_image_request();
        let wire = MeshRequestBody::encode(&request)
            .expect("wire body")
            .wire(false)
            .await
            .expect("wire");
        let head = std::str::from_utf8(&wire.head).unwrap();
        assert!(head.ends_with("\r\n\r\n"));
        assert_eq!(
            &wire.body[..],
            serde_json::to_vec(&request).expect("reference JSON")
        );
        assert!(head.contains(&format!("content-length: {}\r\n", wire.body.len())));
        assert!(!head.contains("content-encoding"));
    }

    #[tokio::test]
    async fn zstd_is_used_only_for_capable_workers_and_large_bodies() {
        let request = large_image_request();
        let body = MeshRequestBody::encode(&request).expect("encode");
        let wire = body.wire(true).await.expect("wire");
        let head = std::str::from_utf8(&wire.head).unwrap();
        assert!(head.contains("content-encoding: zstd\r\n"), "{head}");
        assert!(head.contains(&format!("content-length: {}\r\n", wire.body.len())));
        assert!(wire.body.len() < body.json.len());
        let decoded = zstd::decode_all(&wire.body[..]).expect("zstd body");
        assert_eq!(decoded, &body.json[..]);
        // The compressed body is cached for later attempts.
        let again = body.wire(true).await.expect("wire again");
        assert_eq!(again.body.as_ptr(), wire.body.as_ptr());

        // Legacy workers always receive identity bodies.
        let legacy = body.wire(false).await.expect("legacy wire");
        assert!(
            !std::str::from_utf8(&legacy.head)
                .unwrap()
                .contains("content-encoding")
        );

        // Small bodies skip the compression pass.
        let small = MeshRequestBody::encode(&chat_request("hi")).expect("encode");
        let wire = small.wire(true).await.expect("wire");
        assert!(
            !std::str::from_utf8(&wire.head)
                .unwrap()
                .contains("content-encoding")
        );
    }

    async fn read_test_request(socket: &mut TcpStream, buf: &mut [u8]) {
        let mut request = Vec::new();
        let header_end = loop {
            let read = socket.read(buf).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buf[..read]);
            if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .map(str::to_owned)
            })
            .unwrap()
            .trim()
            .parse::<usize>()
            .unwrap();
        while request.len() < header_end + content_length {
            let read = socket.read(buf).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buf[..read]);
        }
    }

    fn chat_request(prompt: &str) -> ChatCompletionRequest {
        serde_json::from_value(serde_json::json!({
            "model": "mesh-model",
            "messages": [{"role": "user", "content": prompt}],
            "stream": true
        }))
        .unwrap()
    }

    fn resource_advertisement(resource_id: &str, capacity: u32) -> ResourceAdvertisement {
        ResourceAdvertisement {
            resource_id: resource_id.to_string(),
            models: vec![ModelAdvertisement {
                id: "mesh-model".to_string(),
                context_limit: Some(4096),
            }],
            availability: AvailabilitySchedule {
                default_capacity: capacity,
                ..Default::default()
            },
            effective_capacity: capacity,
            accepting_requests: true,
            healthy: true,
            revision: 1,
        }
    }

    fn worker_advertisement(resources: Vec<ResourceAdvertisement>) -> WorkerAdvertisement {
        WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: None,
            agent_version: "test".to_string(),
            resources,
            model_switching: None,
            request_encodings: Vec::new(),
        }
    }

    fn mesh_client(registry: Arc<MeshRegistry>) -> MeshUpstreamClient {
        MeshUpstreamClient::new(
            registry,
            BackendFinalizationPolicies::default(),
            true,
            1024 * 1024,
            crate::dashboard_flow::DashboardFlowStore::disabled(),
        )
    }

    struct MeshHarness {
        client: MeshUpstreamClient,
        worker_task: JoinHandle<()>,
        _worker_connection_keepalive: Connection,
        _controller: Endpoint,
        _worker: Endpoint,
    }

    async fn start_mesh_harness(
        resources: Vec<(&str, SocketAddr, u32)>,
        accept_count: usize,
    ) -> MeshHarness {
        start_mesh_harness_with_worker_capacity(
            resources
                .into_iter()
                .map(|(id, target, capacity)| (id, target, capacity, capacity))
                .collect(),
            accept_count,
        )
        .await
    }

    async fn start_mesh_harness_with_worker_capacity(
        resources: Vec<(&str, SocketAddr, u32, u32)>,
        accept_count: usize,
    ) -> MeshHarness {
        start_mesh_harness_with_encodings(resources, accept_count, Vec::new(), "mesh-model").await
    }

    async fn start_mesh_harness_with_encodings(
        resources: Vec<(&str, SocketAddr, u32, u32)>,
        accept_count: usize,
        request_encodings: Vec<String>,
        advertised_model: &str,
    ) -> MeshHarness {
        let controller_key = SecretKey::generate();
        let controller_id = controller_key.public();
        let controller = Endpoint::builder(presets::Minimal)
            .secret_key(controller_key)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![WORKER_ALPN.to_vec()])
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
        let worker_connect = worker.connect(controller_addr, WORKER_ALPN);
        let controller_accept = async {
            controller
                .accept()
                .await
                .unwrap()
                .accept()
                .unwrap()
                .await
                .unwrap()
        };
        let (worker_connection, controller_connection) =
            tokio::join!(worker_connect, controller_accept);
        let worker_connection = worker_connection.unwrap();
        let worker_connection_keepalive = worker_connection.clone();

        let resource_configs = resources
            .iter()
            .map(
                |(id, target, _, worker_capacity)| MeshWorkerResourceConfig {
                    id: (*id).to_string(),
                    target: *target,
                    models: Vec::new(),
                    model_refresh_secs: 60,
                    capacity_source: crate::config::MeshWorkerCapacitySource::Configured,
                    availability: AvailabilitySchedule {
                        default_capacity: *worker_capacity,
                        ..Default::default()
                    },
                },
            )
            .collect();
        let resource_ads = resources
            .iter()
            .map(|(id, _, advertised_capacity, _)| {
                let mut ad = resource_advertisement(id, *advertised_capacity);
                ad.models[0].id = advertised_model.to_string();
                ad
            })
            .collect();
        let runtime = Arc::new(
            WorkerRuntime::new(MeshWorkerConfig {
                resources: resource_configs,
                ..Default::default()
            })
            .await,
        );
        let worker_task = tokio::spawn(async move {
            let mut streams = JoinSet::new();
            for _ in 0..accept_count {
                let (send, recv) = worker_connection.accept_bi().await.unwrap();
                let runtime = Arc::clone(&runtime);
                streams.spawn(async move {
                    let _ = handle_stream(send, recv, runtime).await;
                });
            }
            while streams.join_next().await.is_some() {}
        });

        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        registry.register(
            worker_id,
            controller_connection,
            WorkerAdvertisement {
                request_encodings,
                ..worker_advertisement(resource_ads)
            },
        );

        MeshHarness {
            client: mesh_client(registry),
            worker_task,
            _worker_connection_keepalive: worker_connection_keepalive,
            _controller: controller,
            _worker: worker,
        }
    }

    #[test]
    fn sparse_tool_calls_are_normalized() {
        let chunk = r#"{"id":"x","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}}]}"#;
        let parsed = parse_chat_completion_chunk(chunk).expect("chunk");
        assert_eq!(
            parsed.choices[0].delta.tool_calls.as_ref().unwrap()[0].kind,
            "function"
        );
    }

    #[test]
    fn mesh_upstream_error_includes_redacted_body() {
        let message = mesh_upstream_error_message(
            http::StatusCode::BAD_REQUEST,
            br#"{"error":{"message":"Unexpected reasoning effort high","image_url":"data:image/png;base64,secret"}}"#,
        );

        assert!(message.contains("Unexpected reasoning effort high"));
        assert!(message.contains("400 Bad Request"));
        assert!(!message.contains("base64,secret"));
    }

    #[test]
    fn mesh_upstream_error_without_body_keeps_status_message() {
        assert_eq!(
            mesh_upstream_error_message(http::StatusCode::BAD_GATEWAY, b""),
            "mesh worker local upstream returned 502 Bad Gateway"
        );
    }

    #[test]
    fn mesh_catalog_dedupes_models() {
        let body = mesh_models_list_body(vec![
            ModelAdvertisement {
                id: "a".to_string(),
                context_limit: Some(42),
            },
            ModelAdvertisement {
                id: "a".to_string(),
                context_limit: None,
            },
        ]);
        assert_eq!(body["data"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn failover_wrapper_preserves_mesh_resource_inventory() {
        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        let endpoint = SecretKey::generate().public();
        registry.register_test(
            endpoint,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: Some("workstation".into()),
                agent_version: "test".into(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "local-vllm".into(),
                    models: vec![ModelAdvertisement {
                        id: "local-model".into(),
                        context_limit: Some(32_768),
                    }],
                    availability: AvailabilitySchedule {
                        timezone: "America/Chicago".into(),
                        default_capacity: 3,
                        weekly: Vec::new(),
                        exceptions: Vec::new(),
                    },
                    effective_capacity: 3,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
                model_switching: None,
                request_encodings: Vec::new(),
            },
        );
        let mesh = MeshUpstreamClient::new(
            registry,
            BackendFinalizationPolicies::default(),
            true,
            1024 * 1024,
            crate::dashboard_flow::DashboardFlowStore::disabled(),
        );
        let wrapped = crate::upstream::FailoverUpstreamClient::new(
            vec![crate::upstream::FailoverUpstreamProvider::new(
                "mesh",
                mesh,
                None,
                None,
                serde_json::Map::new(),
            )],
            Duration::from_secs(30),
        );

        let inventory = wrapped.provider_inventory().await.expect("inventory");
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].provider_id, format!("mesh:{endpoint}"));
        assert_eq!(
            inventory[0].provider_name,
            format!("workstation ({})", endpoint.fmt_short())
        );
        assert_eq!(inventory[0].resource_id.as_deref(), Some("local-vllm"));
        assert_eq!(inventory[0].capacity_limit, Some(3));
        assert_eq!(
            inventory[0].availability.as_ref().unwrap().default_capacity,
            3
        );
    }

    #[tokio::test]
    async fn pinned_session_waits_for_original_resource_even_when_spare_capacity_exists() {
        let primary_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let primary_target = primary_listener.local_addr().unwrap();
        let spare_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let spare_target = spare_listener.local_addr().unwrap();
        let (first_request_tx, first_request_rx) = tokio::sync::oneshot::channel();
        let (release_first_tx, release_first_rx) = tokio::sync::oneshot::channel();
        let (second_request_tx, second_request_rx) = tokio::sync::oneshot::channel();
        let primary_server = tokio::spawn(async move {
            let mut buf = [0_u8; 2048];
            let (mut socket, _) = primary_listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            first_request_tx.send(()).unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"first\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"held\"}}]}\n\n")
                .await
                .unwrap();
            socket.flush().await.unwrap();
            release_first_rx.await.unwrap();
            socket
                .write_all(b"data: {\"id\":\"first\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
                .await
                .unwrap();
            drop(socket);

            let (mut socket, _) = primary_listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            second_request_tx.send(()).unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"second\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"same primary\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
                .await
                .unwrap();
        });
        let (spare_hit_tx, mut spare_hit_rx) = tokio::sync::oneshot::channel();
        let spare_server = tokio::spawn(async move {
            let _ = spare_listener.accept().await;
            let _ = spare_hit_tx.send(());
        });
        let harness = start_mesh_harness(
            vec![("primary", primary_target, 1), ("spare", spare_target, 1)],
            4,
        )
        .await;
        let affinity = RequestAffinity("session-a".to_string());
        let primary_only =
            AuthorizationScope::restricted(|_, route, _, _| route == Some("primary"));
        let first_backend = BackendChatRequest::new(chat_request("first"), None, None, None)
            .with_authorization(primary_only, InferenceEndpoint::ChatCompletions)
            .with_affinity(Some(affinity.clone()));
        let mut first_stream = harness
            .client
            .stream_chat_completion(&first_backend)
            .await
            .unwrap();
        assert_eq!(
            first_stream.next().await.unwrap().unwrap().choices[0]
                .delta
                .content
                .as_deref(),
            Some("held")
        );
        first_request_rx.await.unwrap();

        let second_backend = BackendChatRequest::new(chat_request("second"), None, None, None)
            .with_affinity(Some(affinity));
        let mut second = Box::pin(harness.client.stream_chat_completion(&second_backend));
        assert!(
            matches!(poll!(&mut second), Poll::Pending),
            "same-session request must wait for its pinned resource instead of migrating"
        );
        assert!(
            spare_hit_rx.try_recv().is_err(),
            "spare resource should remain unused while the pinned resource is busy"
        );

        release_first_tx.send(()).unwrap();
        assert_eq!(
            first_stream.next().await.unwrap().unwrap().choices[0]
                .delta
                .content
                .as_deref(),
            Some(" done")
        );
        assert!(first_stream.next().await.is_none());
        drop(first_stream);

        let mut second_stream = second.await.unwrap();
        assert_eq!(
            second_stream.next().await.unwrap().unwrap().choices[0]
                .delta
                .content
                .as_deref(),
            Some("same primary")
        );
        assert!(second_stream.next().await.is_none());
        drop(second_stream);
        second_request_rx.await.unwrap();
        primary_server.await.unwrap();
        harness.worker_task.abort();
        spare_server.abort();
    }

    #[tokio::test]
    async fn denied_busy_mesh_resource_returns_forbidden_without_waiting() {
        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        registry.register_test(
            SecretKey::generate().public(),
            worker_advertisement(vec![resource_advertisement("denied", 1)]),
        );
        let held = registry.reserve("mesh-model").expect("initial reservation");
        let client =
            mesh_client(Arc::clone(&registry)).with_capacity_wait_timeout(Duration::from_secs(30));
        let backend = BackendChatRequest::new(chat_request("denied"), None, None, None)
            .with_authorization(
                AuthorizationScope::restricted(|_, _, _, _| false),
                InferenceEndpoint::ChatCompletions,
            );
        let mut attempt = Box::pin(client.stream_chat_completion(&backend));

        let error = match poll!(&mut attempt) {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("denied mesh resource should not be served"),
            Poll::Pending => panic!("authorization denial must not wait for busy capacity"),
        };
        assert_eq!(error.status_code(), http::StatusCode::FORBIDDEN);
        assert_eq!(error.failover_disposition(), FailoverDisposition::Terminal);
        drop(held);
    }

    #[tokio::test]
    async fn pinned_capacity_deadline_reports_session_wait_without_cooldown() {
        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        registry.register_test(
            SecretKey::generate().public(),
            worker_advertisement(vec![
                resource_advertisement("primary", 1),
                resource_advertisement("spare", 1),
            ]),
        );
        let affinity = RequestAffinity("session-a".to_string());
        let first = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, resource_id| resource_id == "primary",
            )
            .unwrap()
            .expect("primary reservation");
        registry.commit_affinity(first.affinity_commit().expect("affinity commit"));
        drop(first);
        let held = registry
            .reserve_excluding_where_with_affinity(
                "mesh-model",
                &HashSet::new(),
                Some(&affinity),
                |_, _| true,
            )
            .unwrap()
            .expect("pinned reservation");
        let client = mesh_client(Arc::clone(&registry)).with_capacity_wait_timeout(Duration::ZERO);
        let backend = BackendChatRequest::new(chat_request("pinned timeout"), None, None, None)
            .with_affinity(Some(affinity));

        let error = match client.stream_chat_completion(&backend).await {
            Ok(_) => panic!("pinned capacity wait should time out"),
            Err(error) => error,
        };
        assert_eq!(
            error.failover_disposition(),
            FailoverDisposition::FailoverNoCooldown
        );
        assert!(
            error
                .to_string()
                .contains("mesh session capacity wait timed out")
        );
        drop(held);
    }

    async fn read_full_test_request(socket: &mut TcpStream) -> (String, Vec<u8>) {
        let mut buf = [0_u8; 64 * 1024];
        let mut request = Vec::new();
        let header_end = loop {
            let read = socket.read(&mut buf).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buf[..read]);
            if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let head = String::from_utf8(request[..header_end].to_vec()).unwrap();
        let content_length = head
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .map(str::to_owned)
            })
            .unwrap()
            .trim()
            .parse::<usize>()
            .unwrap();
        while request.len() < header_end + content_length {
            let read = socket.read(&mut buf).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buf[..read]);
        }
        (head, request[header_end..].to_vec())
    }

    #[tokio::test]
    async fn zstd_request_bodies_are_decoded_by_the_worker_before_the_local_engine() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_full_test_request(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"z\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
                .await
                .unwrap();
            request_tx.send(request).unwrap();
        });
        // The worker spells the model differently from the request; its
        // engine must receive its own spelling.
        let harness = start_mesh_harness_with_encodings(
            vec![("primary", target, 1, 1)],
            1,
            vec!["zstd".to_string()],
            "Mesh-Model",
        )
        .await;
        let request = large_image_request();
        let mut stream = harness
            .client
            .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
            .await
            .expect("stream");
        assert_eq!(
            stream.next().await.unwrap().unwrap().choices[0]
                .delta
                .content
                .as_deref(),
            Some("ok")
        );
        let (head, body) = request_rx.await.unwrap();
        let head = head.to_ascii_lowercase();
        assert!(!head.contains("content-encoding"), "{head}");
        assert!(head.contains(&format!("content-length: {}\r\n", body.len())));
        assert!(head.contains(&format!("host: {target}\r\n")), "{head}");
        let forwarded: Value = serde_json::from_slice(&body).expect("identity JSON body");
        assert_eq!(forwarded["model"], "Mesh-Model");
        assert!(
            forwarded["messages"][0]["content"][1]["image_url"]["url"]
                .as_str()
                .unwrap()
                .len()
                > 2 * 1024 * 1024
        );
        drop(stream);
        server.await.unwrap();
        harness.worker_task.await.unwrap();
    }

    #[tokio::test]
    async fn admitted_but_silent_worker_times_out_before_the_first_chunk() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target = listener.local_addr().unwrap();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_full_test_request(&mut socket).await;
            // Accept the request and then say nothing.
            let _ = release_rx.await;
        });
        let harness = start_mesh_harness(vec![("primary", target, 1)], 1).await;
        let client = harness
            .client
            .clone()
            .with_response_timeouts(Duration::from_millis(300), Duration::from_secs(30));
        let started = tokio::time::Instant::now();
        let error = match client
            .stream_chat_completion(&BackendChatRequest::new(
                chat_request("hang"),
                None,
                None,
                None,
            ))
            .await
        {
            Ok(_) => panic!("silent worker must not produce a stream"),
            Err(error) => error,
        };
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(error.to_string().contains("no response"), "{error}");
        // Pre-first-chunk: the failover layer may still try another provider.
        assert_ne!(error.failover_disposition(), FailoverDisposition::Terminal);
        let reservation = client
            .registry
            .reserve("mesh-model")
            .expect("timed-out attempt releases its reservation");
        drop(reservation);
        let _ = release_tx.send(());
        server.await.unwrap();
        harness.worker_task.abort();
    }

    #[tokio::test]
    async fn stalled_stream_after_the_first_chunk_surfaces_an_error() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target = listener.local_addr().unwrap();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_full_test_request(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"s\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n")
                .await
                .unwrap();
            socket.flush().await.unwrap();
            let _ = release_rx.await;
        });
        let harness = start_mesh_harness(vec![("primary", target, 1)], 1).await;
        let client = harness
            .client
            .clone()
            .with_response_timeouts(Duration::from_secs(30), Duration::from_millis(300));
        let mut stream = client
            .stream_chat_completion(&BackendChatRequest::new(
                chat_request("stall"),
                None,
                None,
                None,
            ))
            .await
            .expect("first chunk arrives");
        assert_eq!(
            stream.next().await.unwrap().unwrap().choices[0]
                .delta
                .content
                .as_deref(),
            Some("first")
        );
        let error = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("idle timeout fires")
            .expect("stream yields an error")
            .expect_err("stalled body is an error");
        assert!(error.to_string().contains("stalled"), "{error}");
        drop(stream);
        let _ = release_tx.send(());
        server.await.unwrap();
        harness.worker_task.abort();
    }

    #[tokio::test]
    async fn unpinned_capacity_wait_resumes_after_busy_resource_releases() {
        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        registry.register_test(
            SecretKey::generate().public(),
            worker_advertisement(vec![resource_advertisement("primary", 1)]),
        );
        let held = registry.reserve("mesh-model").expect("initial reservation");
        let excluded = HashSet::new();
        let mut waiting = Box::pin(registry.reserve_excluding_where_with_affinity_wait(
            "mesh-model",
            &excluded,
            None,
            |_, _| true,
            Duration::from_secs(30),
        ));
        assert!(
            matches!(poll!(&mut waiting), Poll::Pending),
            "global capacity exhaustion should wait while an authorized resource is busy"
        );

        drop(held);
        let reservation = waiting
            .await
            .unwrap()
            .expect("released resource should be reserved");
        assert_eq!(reservation.resource.resource_id, "primary");
    }

    #[tokio::test]
    async fn cancelling_pending_worker_admission_releases_hub_reservation() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target = listener.local_addr().unwrap();
        let harness =
            start_mesh_harness_with_worker_capacity(vec![("primary", target, 1, 0)], 1).await;
        let backend = BackendChatRequest::new(chat_request("wait at worker"), None, None, None);
        let mut pending = Box::pin(harness.client.stream_chat_completion(&backend));
        assert!(
            matches!(poll!(&mut pending), Poll::Pending),
            "worker-side capacity exhaustion should leave admission pending"
        );
        assert!(
            harness.client.registry.reserve("mesh-model").is_none(),
            "pending worker admission should hold the hub reservation while alive"
        );

        drop(pending);
        let reservation = harness
            .client
            .registry
            .reserve("mesh-model")
            .expect("dropping pending admission should release the hub reservation");
        assert_eq!(reservation.resource.resource_id, "primary");
        drop(reservation);
        harness.worker_task.await.unwrap();
    }

    #[tokio::test]
    async fn configured_worker_capacity_deadline_times_out_and_releases_hub_reservation() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target = listener.local_addr().unwrap();
        let harness =
            start_mesh_harness_with_worker_capacity(vec![("primary", target, 1, 0)], 1).await;
        let client = harness
            .client
            .clone()
            .with_capacity_wait_timeout(Duration::ZERO);
        let backend = BackendChatRequest::new(chat_request("worker timeout"), None, None, None);

        let error = match client.stream_chat_completion(&backend).await {
            Ok(_) => panic!("worker-side capacity wait should time out"),
            Err(error) => error,
        };
        assert_eq!(
            error.failover_disposition(),
            FailoverDisposition::FailoverNoCooldown
        );
        assert!(
            error
                .to_string()
                .contains("mesh worker capacity wait timed out")
        );
        let reservation = harness
            .client
            .registry
            .reserve("mesh-model")
            .expect("worker admission timeout should release the hub reservation");
        assert_eq!(reservation.resource.resource_id, "primary");
        drop(reservation);
        harness.worker_task.await.unwrap();
    }

    #[tokio::test]
    async fn streams_chat_completion_over_one_quic_request_stream() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let local_server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0_u8; 2048];
            let header_end = loop {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(str::to_owned)
                })
                .unwrap()
                .trim()
                .parse::<usize>()
                .unwrap();
            while request.len() < header_end + content_length {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
            }
            request_tx
                .send(request[header_end..header_end + content_length].to_vec())
                .unwrap();

            match tokio::time::timeout(Duration::from_millis(30), socket.read(&mut buf)).await {
                Err(_) => {}
                Ok(Ok(0)) => panic!("worker half-closed local TCP before the response"),
                Ok(Ok(read)) => panic!("unexpected {read} request bytes after Content-Length"),
                Ok(Err(err)) => panic!("local TCP read failed before response: {err}"),
            }

            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n").await.unwrap();
            socket.write_all(b"data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n").await.unwrap();
            socket.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            socket.write_all(b"data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n").await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let header_end = loop {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(str::to_owned)
                })
                .unwrap()
                .trim()
                .parse::<usize>()
                .unwrap();
            while request.len() < header_end + content_length {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
            }
            let error_body = br#"{"error":{"message":"Unexpected reasoning effort high"}}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        error_body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(error_body).await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            let event = b"data: {\"id\":\"chunked-cut\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n";
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            socket
                .write_all(format!("{:x}\r\n", event.len()).as_bytes())
                .await
                .unwrap();
            socket.write_all(event).await.unwrap();
            socket.write_all(b"\r\n").await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            let event = b"data: {\"id\":\"length-cut\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n";
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        event.len() + 32
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(event).await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"missing-done\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"stop\"}]}\n\n").await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            read_test_request(&mut socket, &mut buf).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"missing-finish\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\ndata: [DONE]\n\n").await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let header_end = loop {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(str::to_owned)
                })
                .unwrap()
                .trim()
                .parse::<usize>()
                .unwrap();
            while request.len() < header_end + content_length {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buf[..read]);
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"id\":\"cancel\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"started\"}}]}\n\n").await.unwrap();
            socket.flush().await.unwrap();
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            let _ = cancel_tx.send(());
        });

        let controller_key = SecretKey::generate();
        let controller_id = controller_key.public();
        let controller = Endpoint::builder(presets::Minimal)
            .secret_key(controller_key)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![WORKER_ALPN.to_vec()])
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
        let worker_connect = worker.connect(controller_addr, WORKER_ALPN);
        let controller_accept = async {
            controller
                .accept()
                .await
                .unwrap()
                .accept()
                .unwrap()
                .await
                .unwrap()
        };
        let (worker_connection, controller_connection) =
            tokio::join!(worker_connect, controller_accept);
        let worker_connection = worker_connection.unwrap();
        let _worker_keepalive = worker_connection.clone();

        let availability = AvailabilitySchedule {
            default_capacity: 1,
            ..Default::default()
        };
        let resource_config = MeshWorkerResourceConfig {
            id: "primary".to_string(),
            target,
            models: Vec::new(),
            model_refresh_secs: 60,
            capacity_source: crate::config::MeshWorkerCapacitySource::Configured,
            availability: availability.clone(),
        };
        let runtime = Arc::new(
            WorkerRuntime::new(MeshWorkerConfig {
                resources: vec![resource_config],
                ..Default::default()
            })
            .await,
        );
        let worker_task = tokio::spawn(async move {
            for _ in 0..7 {
                let (send, recv) = worker_connection.accept_bi().await.unwrap();
                let _ = handle_stream(send, recv, Arc::clone(&runtime)).await;
            }
        });

        let registry = Arc::new(MeshRegistry::new(Duration::from_secs(30)));
        registry.register(
            worker_id,
            controller_connection,
            WorkerAdvertisement {
                protocol_version: PROTOCOL_VERSION,
                node_name: None,
                agent_version: "test".to_string(),
                resources: vec![ResourceAdvertisement {
                    resource_id: "primary".to_string(),
                    models: vec![ModelAdvertisement {
                        id: "mesh-model".to_string(),
                        context_limit: Some(4096),
                    }],
                    availability,
                    effective_capacity: 1,
                    accepting_requests: true,
                    healthy: true,
                    revision: 1,
                }],
                model_switching: None,
                request_encodings: Vec::new(),
            },
        );
        let client = MeshUpstreamClient::new(
            registry,
            BackendFinalizationPolicies::default(),
            true,
            1024 * 1024,
            crate::dashboard_flow::DashboardFlowStore::disabled(),
        );
        let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "mesh-model",
            "messages": [{"role": "user", "content": "ping"}],
            "stream": true
        }))
        .unwrap();
        let mut stream = client
            .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
            .await
            .unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.choices[0].delta.content.as_deref(), Some("hello"));
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.choices[0].delta.content.as_deref(), Some(" world"));
        assert!(stream.next().await.is_none());

        let forwarded: Value = serde_json::from_slice(&request_rx.await.unwrap()).unwrap();
        assert_eq!(forwarded["model"], "mesh-model");
        assert_eq!(forwarded["messages"][0]["content"], "ping");

        let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "mesh-model",
            "messages": [{"role": "user", "content": "bad effort"}],
            "stream": true
        }))
        .unwrap();
        let error = match client
            .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
            .await
        {
            Ok(_) => panic!("400 response should fail before returning a stream"),
            Err(error) => error,
        };
        assert_eq!(error.failover_disposition(), FailoverDisposition::Terminal);
        assert!(
            error
                .to_string()
                .contains("Unexpected reasoning effort high")
        );

        for (prompt, expected_error) in [
            (
                "truncate chunked",
                "chunked response ended before its terminator",
            ),
            ("truncate content length", "response body ended with"),
        ] {
            let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
                "model": "mesh-model",
                "messages": [{"role": "user", "content": prompt}],
                "stream": true
            }))
            .unwrap();
            let mut stream = client
                .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
                .await
                .unwrap();
            assert_eq!(
                stream.next().await.unwrap().unwrap().choices[0]
                    .delta
                    .content
                    .as_deref(),
                Some("partial")
            );
            let error = stream
                .next()
                .await
                .expect("truncation error")
                .expect_err("truncated body must fail");
            assert!(error.to_string().contains(expected_error), "{error}");
        }

        for (prompt, expected_error) in [("missing done", Some("[DONE]")), ("missing finish", None)]
        {
            let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
                "model": "mesh-model",
                "messages": [{"role": "user", "content": prompt}],
                "stream": true
            }))
            .unwrap();
            let mut stream = client
                .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
                .await
                .unwrap();
            assert_eq!(
                stream.next().await.unwrap().unwrap().choices[0]
                    .delta
                    .content
                    .as_deref(),
                Some("partial")
            );
            if let Some(expected_error) = expected_error {
                let error = stream
                    .next()
                    .await
                    .expect("terminal protocol error")
                    .expect_err("incomplete terminal protocol must fail");
                assert!(error.to_string().contains(expected_error), "{error}");
            } else {
                assert!(stream.next().await.is_none(), "clean [DONE] must complete");
            }
        }

        let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "mesh-model",
            "messages": [{"role": "user", "content": "cancel me"}],
            "stream": true
        }))
        .unwrap();
        let mut cancelled_stream = client
            .stream_chat_completion(&BackendChatRequest::new(request, None, None, None))
            .await
            .unwrap();
        assert_eq!(
            cancelled_stream.next().await.unwrap().unwrap().choices[0]
                .delta
                .content
                .as_deref(),
            Some("started")
        );
        drop(cancelled_stream);
        tokio::time::timeout(Duration::from_secs(2), cancel_rx)
            .await
            .expect("worker should close local vLLM connection after hub cancellation")
            .unwrap();
        local_server.await.unwrap();
        worker_task.await.unwrap();
    }
}
