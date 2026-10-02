use crate::config::AvailabilitySchedule;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub const ENROLL_ALPN: &[u8] = b"llmconduit-mesh-enroll/2";
pub const WORKER_ALPN: &[u8] = b"llmconduit-mesh-worker/2";

pub const PROTOCOL_VERSION: u16 = 2;
pub const CONTROL_PROTOCOL_VERSION: u16 = 2;
pub const REQUEST_PROTOCOL_VERSION: u16 = 2;
pub const MAX_CONTROL_FRAME_BYTES: usize = 64 * 1024;
pub const MAX_REQUEST_PREFACE_BYTES: usize = 4 * 1024;
pub const MAX_NODE_NAME_BYTES: usize = 128;
pub const MAX_AGENT_VERSION_BYTES: usize = 128;
pub const MAX_RESOURCE_ID_BYTES: usize = 128;
pub const MAX_MODEL_ID_BYTES: usize = 512;
pub const MAX_RESOURCES_PER_NODE: usize = 32;
pub const MAX_MODELS_PER_RESOURCE: usize = 1024;
pub const MAX_SWITCHABLE_MODELS: usize = 256;
pub const MAX_SWITCHING_PROVIDER_BYTES: usize = 64;
pub const MAX_SWITCHING_DESCRIPTION_BYTES: usize = 1024;
pub const MAX_SWITCHING_STATE_BYTES: usize = 64;
pub const MAX_CAPACITY_PER_RESOURCE: u32 = 65_535;
pub const MAX_SWITCH_MODEL_INSTANCES: u32 = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollRequest {
    pub protocol_version: u16,
    pub join_key: String,
    pub node_name: Option<String>,
    pub agent_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollAccepted {
    pub protocol_version: u16,
    pub endpoint_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum EnrollResponse {
    Accepted { protocol_version: u16 },
    Rejected { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerAdvertisement {
    pub protocol_version: u16,
    pub node_name: Option<String>,
    pub agent_version: String,
    pub resources: Vec<ResourceAdvertisement>,
    pub model_switching: Option<ModelSwitchingAdvertisement>,
    /// `Content-Encoding` values this worker can decode on inference request
    /// bodies. Optional on the wire in both directions: an older hub ignores
    /// the unknown field, and an older worker omits it so a newer hub keeps
    /// sending identity-encoded bodies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_encodings: Vec<String>,
}

/// Request-body encoding negotiated through
/// [`WorkerAdvertisement::request_encodings`].
pub const REQUEST_ENCODING_ZSTD: &str = "zstd";
pub const MAX_REQUEST_ENCODINGS: usize = 8;
pub const MAX_REQUEST_ENCODING_BYTES: usize = 32;

/// Largest context window a worker may advertise. Anything above this is
/// either a bug or hostile and would overflow downstream token budgeting.
pub const MAX_CONTEXT_LIMIT: i64 = 10_000_000;

impl WorkerAdvertisement {
    pub fn accepts_request_encoding(&self, encoding: &str) -> bool {
        self.request_encodings
            .iter()
            .any(|value| value.eq_ignore_ascii_case(encoding))
    }
}

/// Drops a context limit outside `1..=MAX_CONTEXT_LIMIT` instead of
/// rejecting the whole advertisement: an unknown window only disables
/// pre-flight budgeting for that model, while a bogus one corrupts it.
pub fn sanitize_context_limit(limit: Option<i64>) -> Option<i64> {
    limit.filter(|limit| (1..=MAX_CONTEXT_LIMIT).contains(limit))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelSwitchingAdvertisement {
    pub provider: String,
    pub models: Vec<SwitchableModelAdvertisement>,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchableModelAdvertisement {
    pub id: String,
    pub description: Option<String>,
    pub phase: String,
    pub desired_state: String,
    #[serde(default)]
    pub gpu_count: u32,
    #[serde(default)]
    pub assigned_gpus: Vec<u32>,
    #[serde(default = "default_max_instances")]
    pub max_instances: u32,
    #[serde(default)]
    pub desired_instances: u32,
    #[serde(default)]
    pub ready_instances: u32,
    #[serde(default)]
    pub instances: Vec<SwitchableModelInstanceAdvertisement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchableModelInstanceAdvertisement {
    pub instance_id: String,
    pub index: u32,
    pub port: u16,
    pub phase: String,
    #[serde(default)]
    pub assigned_gpus: Vec<u32>,
    pub container_status: Option<String>,
    pub last_error: Option<String>,
    pub health: Option<String>,
}

impl SwitchableModelAdvertisement {
    #[cfg(test)]
    pub(crate) fn legacy(
        id: impl Into<String>,
        description: Option<String>,
        phase: impl Into<String>,
        desired_state: impl Into<String>,
        gpu_count: u32,
        assigned_gpus: Vec<u32>,
    ) -> Self {
        Self {
            id: id.into(),
            description,
            phase: phase.into(),
            desired_state: desired_state.into(),
            gpu_count,
            assigned_gpus,
            max_instances: 1,
            desired_instances: 0,
            ready_instances: 0,
            instances: Vec::new(),
        }
    }
}

impl SwitchableModelInstanceAdvertisement {
    pub(crate) fn from_fleet(value: crate::dashboard_fleet::FleetInstanceStatus) -> Self {
        Self {
            instance_id: value.instance_id,
            index: value.index,
            port: value.port,
            phase: value.phase,
            assigned_gpus: value
                .assigned_gpus
                .into_iter()
                .filter_map(|gpu| u32::try_from(gpu).ok())
                .collect(),
            container_status: fleet_diagnostic_text(value.container_status),
            last_error: fleet_diagnostic_text(value.last_error),
            health: fleet_diagnostic_text(value.health),
        }
    }
}

fn fleet_diagnostic_text(value: Option<String>) -> Option<String> {
    // Docker errors contain newlines and can be arbitrarily long; they must not
    // invalidate the worker's entire advertisement or bypass its wire bounds.
    let value = value?;
    let mut normalized = String::new();
    for ch in value.trim().chars() {
        let ch = if ch.is_control() { ' ' } else { ch };
        if normalized.len() + ch.len_utf8() > MAX_SWITCHING_DESCRIPTION_BYTES {
            break;
        }
        normalized.push(ch);
    }
    let normalized = normalized.trim().to_owned();
    (!normalized.is_empty()).then_some(normalized)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceAdvertisement {
    pub resource_id: String,
    pub models: Vec<ModelAdvertisement>,
    pub availability: AvailabilitySchedule,
    pub effective_capacity: u32,
    pub accepting_requests: bool,
    pub healthy: bool,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAdvertisement {
    pub id: String,
    pub context_limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heartbeat {
    pub sequence: u64,
    pub resources: Vec<ResourceRuntimeState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRuntimeState {
    pub resource_id: String,
    pub effective_capacity: u32,
    pub worker_active_requests: u32,
    pub accepting_requests: bool,
    pub healthy: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum WorkerToHub {
    Hello(WorkerAdvertisement),
    Heartbeat(Heartbeat),
    ResourceUpdate(ResourceAdvertisement),
    ModelCatalogUpdate {
        resource_id: String,
        models: Vec<ModelAdvertisement>,
        revision: u64,
    },
    ModelSwitchingUpdate(ModelSwitchingAdvertisement),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum HubToWorker {
    HelloAck { protocol_version: u16 },
    Ping { nonce: u64 },
    Close { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestOpen {
    pub protocol_version: u16,
    pub request_id: Uuid,
    pub resource_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum StreamOpen {
    Inference(RequestOpen),
    SwitchModel(SwitchModelRequest),
    UnloadModel(SwitchModelRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchModelRequest {
    pub protocol_version: u16,
    pub request_id: Uuid,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instances: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelLifecycleAction {
    Load,
    Unload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchModelResponse {
    pub request_id: Uuid,
    pub model_id: String,
    pub accepted: bool,
    pub changed: bool,
    pub error: Option<String>,
    pub model_switching: Option<ModelSwitchingAdvertisement>,
    /// HTTP status the worker's Fleet answered with when it refused the
    /// operation, so the hub can report e.g. a 400 instead of a blanket 409.
    /// Optional both ways: older peers omit or ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_status: Option<u16>,
}

impl SwitchModelResponse {
    /// Status for the hub's dashboard response when the worker refused:
    /// Fleet's request-level 4xx (and gateway 502/504) pass through,
    /// anything else keeps the historical 409.
    pub fn rejection_status(&self) -> http::StatusCode {
        self.error_status
            .filter(|status| (400..=499).contains(status) || matches!(status, 502 | 504))
            .and_then(|status| http::StatusCode::from_u16(status).ok())
            .unwrap_or(http::StatusCode::CONFLICT)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum Admission {
    Accepted,
    Rejected { code: AdmissionRejectCode },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionRejectCode {
    CapacityExhausted,
    UnknownResource,
    ResourceUnhealthy,
    LocalConnectFailed,
    ProtocolError,
}

pub fn validate_enroll_request(request: &EnrollRequest) -> Result<(), ProtocolError> {
    validate_protocol_version(request.protocol_version)?;
    validate_optional_name(request.node_name.as_deref())?;
    validate_agent_version(&request.agent_version)?;
    if request.join_key.trim().is_empty() {
        return Err(ProtocolError::BlankJoinKey);
    }
    Ok(())
}

pub fn validate_worker_advertisement(
    advertisement: &WorkerAdvertisement,
) -> Result<(), ProtocolError> {
    validate_protocol_version(advertisement.protocol_version)?;
    validate_optional_name(advertisement.node_name.as_deref())?;
    validate_agent_version(&advertisement.agent_version)?;
    if advertisement.resources.len() > MAX_RESOURCES_PER_NODE {
        return Err(ProtocolError::TooManyResources {
            count: advertisement.resources.len(),
            max: MAX_RESOURCES_PER_NODE,
        });
    }
    for resource in &advertisement.resources {
        validate_resource_advertisement(resource)?;
    }
    if let Some(switching) = &advertisement.model_switching {
        validate_model_switching(switching)?;
    }
    if advertisement.request_encodings.len() > MAX_REQUEST_ENCODINGS {
        return Err(ProtocolError::TooManyRequestEncodings {
            count: advertisement.request_encodings.len(),
            max: MAX_REQUEST_ENCODINGS,
        });
    }
    for encoding in &advertisement.request_encodings {
        validate_bounded_string(
            encoding,
            MAX_REQUEST_ENCODING_BYTES,
            ProtocolError::BlankSwitchingState {
                field: "request encoding",
            },
            |len, max| ProtocolError::SwitchingStateTooLong {
                field: "request encoding",
                len,
                max,
            },
        )?;
        reject_control_characters(encoding, "request encoding")?;
    }
    Ok(())
}

pub fn validate_model_switching(
    switching: &ModelSwitchingAdvertisement,
) -> Result<(), ProtocolError> {
    validate_bounded_string(
        &switching.provider,
        MAX_SWITCHING_PROVIDER_BYTES,
        ProtocolError::BlankSwitchingProvider,
        |len, max| ProtocolError::SwitchingProviderTooLong { len, max },
    )?;
    reject_control_characters(&switching.provider, "switching provider")?;
    if switching.models.len() > MAX_SWITCHABLE_MODELS {
        return Err(ProtocolError::TooManyModels {
            count: switching.models.len(),
            max: MAX_SWITCHABLE_MODELS,
        });
    }
    for model in &switching.models {
        validate_model_id(&model.id)?;
        if model.gpu_count > 64 || model.assigned_gpus.len() > 64 {
            return Err(ProtocolError::CapacityTooLarge {
                capacity: model.gpu_count.max(model.assigned_gpus.len() as u32),
                max: 64,
            });
        }
        for (field, count) in [
            ("switching model max_instances", model.max_instances),
            ("switching model desired_instances", model.desired_instances),
            ("switching model ready_instances", model.ready_instances),
        ] {
            if count > MAX_SWITCH_MODEL_INSTANCES {
                return Err(ProtocolError::InstanceCountOutOfRange {
                    field,
                    count,
                    max: MAX_SWITCH_MODEL_INSTANCES,
                });
            }
        }
        if model.instances.len() > MAX_SWITCH_MODEL_INSTANCES as usize {
            return Err(ProtocolError::TooManyModelInstances {
                count: model.instances.len(),
                max: MAX_SWITCH_MODEL_INSTANCES as usize,
            });
        }
        if let Some(description) = &model.description {
            if description.len() > MAX_SWITCHING_DESCRIPTION_BYTES {
                return Err(ProtocolError::SwitchingDescriptionTooLong {
                    len: description.len(),
                    max: MAX_SWITCHING_DESCRIPTION_BYTES,
                });
            }
            reject_control_characters(description, "switching model description")?;
        }
        for (field, value) in [
            ("switching model phase", model.phase.as_str()),
            (
                "switching model desired state",
                model.desired_state.as_str(),
            ),
        ] {
            if value.trim().is_empty() {
                return Err(ProtocolError::BlankSwitchingState { field });
            }
            if value.len() > MAX_SWITCHING_STATE_BYTES {
                return Err(ProtocolError::SwitchingStateTooLong {
                    field,
                    len: value.len(),
                    max: MAX_SWITCHING_STATE_BYTES,
                });
            }
            reject_control_characters(value, field)?;
        }
        for instance in &model.instances {
            validate_bounded_string(
                &instance.instance_id,
                MAX_RESOURCE_ID_BYTES,
                ProtocolError::BlankSwitchingState {
                    field: "switching model instance id",
                },
                |len, max| ProtocolError::SwitchingStateTooLong {
                    field: "switching model instance id",
                    len,
                    max,
                },
            )?;
            reject_control_characters(&instance.instance_id, "switching model instance id")?;
            if instance.assigned_gpus.len() > 64 {
                return Err(ProtocolError::CapacityTooLarge {
                    capacity: instance.assigned_gpus.len() as u32,
                    max: 64,
                });
            }
            for (field, value) in [
                (
                    "switching model instance phase",
                    Some(instance.phase.as_str()),
                ),
                (
                    "switching model instance container status",
                    instance.container_status.as_deref(),
                ),
                (
                    "switching model instance last error",
                    instance.last_error.as_deref(),
                ),
                (
                    "switching model instance health",
                    instance.health.as_deref(),
                ),
            ] {
                if let Some(value) = value {
                    if value.trim().is_empty() {
                        return Err(ProtocolError::BlankSwitchingState { field });
                    }
                    if value.len() > MAX_SWITCHING_DESCRIPTION_BYTES {
                        return Err(ProtocolError::SwitchingDescriptionTooLong {
                            len: value.len(),
                            max: MAX_SWITCHING_DESCRIPTION_BYTES,
                        });
                    }
                    reject_control_characters(value, field)?;
                }
            }
        }
    }
    Ok(())
}

pub fn validate_heartbeat(heartbeat: &Heartbeat) -> Result<(), ProtocolError> {
    if heartbeat.resources.len() > MAX_RESOURCES_PER_NODE {
        return Err(ProtocolError::TooManyResources {
            count: heartbeat.resources.len(),
            max: MAX_RESOURCES_PER_NODE,
        });
    }
    for resource in &heartbeat.resources {
        validate_resource_id(&resource.resource_id)?;
        if resource.effective_capacity > MAX_CAPACITY_PER_RESOURCE {
            return Err(ProtocolError::CapacityTooLarge {
                capacity: resource.effective_capacity,
                max: MAX_CAPACITY_PER_RESOURCE,
            });
        }
    }
    Ok(())
}

pub fn validate_model_catalog(
    resource_id: &str,
    models: &[ModelAdvertisement],
) -> Result<(), ProtocolError> {
    validate_resource_id(resource_id)?;
    if models.len() > MAX_MODELS_PER_RESOURCE {
        return Err(ProtocolError::TooManyModels {
            count: models.len(),
            max: MAX_MODELS_PER_RESOURCE,
        });
    }
    for model in models {
        validate_model_id(&model.id)?;
    }
    Ok(())
}

pub fn validate_resource_advertisement(
    resource: &ResourceAdvertisement,
) -> Result<(), ProtocolError> {
    validate_resource_id(&resource.resource_id)?;
    if resource.effective_capacity > MAX_CAPACITY_PER_RESOURCE {
        return Err(ProtocolError::CapacityTooLarge {
            capacity: resource.effective_capacity,
            max: MAX_CAPACITY_PER_RESOURCE,
        });
    }
    if resource.models.len() > MAX_MODELS_PER_RESOURCE {
        return Err(ProtocolError::TooManyModels {
            count: resource.models.len(),
            max: MAX_MODELS_PER_RESOURCE,
        });
    }
    for model in &resource.models {
        validate_model_id(&model.id)?;
    }
    Ok(())
}

pub fn validate_request_open(request: &RequestOpen) -> Result<(), ProtocolError> {
    validate_protocol_version(request.protocol_version)?;
    validate_resource_id(&request.resource_id)
}

pub fn validate_switch_model_request(request: &SwitchModelRequest) -> Result<(), ProtocolError> {
    validate_protocol_version(request.protocol_version)?;
    validate_model_id(&request.model_id)?;
    if let Some(instances) = request.instances {
        validate_instance_count("instances", instances)?;
    }
    Ok(())
}

pub fn validate_protocol_version(version: u16) -> Result<(), ProtocolError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedProtocolVersion(version))
    }
}

pub fn validate_resource_id(resource_id: &str) -> Result<(), ProtocolError> {
    validate_bounded_string(
        resource_id,
        MAX_RESOURCE_ID_BYTES,
        ProtocolError::BlankResourceId,
        |len, max| ProtocolError::ResourceIdTooLong { len, max },
    )?;
    reject_control_characters(resource_id, "resource id")
}

pub(crate) fn validate_model_id(model_id: &str) -> Result<(), ProtocolError> {
    validate_bounded_string(
        model_id,
        MAX_MODEL_ID_BYTES,
        ProtocolError::BlankModelId,
        |len, max| ProtocolError::ModelIdTooLong { len, max },
    )?;
    reject_control_characters(model_id, "model id")
}

fn validate_agent_version(agent_version: &str) -> Result<(), ProtocolError> {
    validate_bounded_string(
        agent_version,
        MAX_AGENT_VERSION_BYTES,
        ProtocolError::BlankAgentVersion,
        |len, max| ProtocolError::AgentVersionTooLong { len, max },
    )?;
    reject_control_characters(agent_version, "agent version")
}

fn validate_optional_name(name: Option<&str>) -> Result<(), ProtocolError> {
    if let Some(name) = name {
        validate_bounded_string(
            name,
            MAX_NODE_NAME_BYTES,
            ProtocolError::BlankNodeName,
            |len, max| ProtocolError::NodeNameTooLong { len, max },
        )?;
        reject_control_characters(name, "node name")?;
    }
    Ok(())
}

fn reject_control_characters(value: &str, field: &'static str) -> Result<(), ProtocolError> {
    if value.chars().any(char::is_control) {
        return Err(ProtocolError::ControlCharacter { field });
    }
    Ok(())
}

fn validate_instance_count(field: &'static str, count: u32) -> Result<(), ProtocolError> {
    if !(1..=MAX_SWITCH_MODEL_INSTANCES).contains(&count) {
        return Err(ProtocolError::InstanceCountOutOfRange {
            field,
            count,
            max: MAX_SWITCH_MODEL_INSTANCES,
        });
    }
    Ok(())
}

fn default_max_instances() -> u32 {
    1
}

fn validate_bounded_string<F>(
    value: &str,
    max: usize,
    blank_error: ProtocolError,
    too_long: F,
) -> Result<(), ProtocolError>
where
    F: FnOnce(usize, usize) -> ProtocolError,
{
    if value.trim().is_empty() {
        return Err(blank_error);
    }
    let len = value.len();
    if len > max {
        return Err(too_long(len, max));
    }
    Ok(())
}

pub async fn read_control<R, T>(reader: &mut R) -> crate::error::AppResult<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    read_json_frame(reader, MAX_CONTROL_FRAME_BYTES)
        .await
        .map_err(|err| crate::error::AppError::upstream(format!("mesh control frame error: {err}")))
}

pub async fn write_control<W, T>(writer: &mut W, message: &T) -> crate::error::AppResult<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    write_json_frame(writer, message, MAX_CONTROL_FRAME_BYTES)
        .await
        .map_err(|err| crate::error::AppError::upstream(format!("mesh control frame error: {err}")))
}

pub fn encode_json_frame<T: Serialize>(message: &T, max_len: usize) -> Result<Vec<u8>, FrameError> {
    let payload = serde_json::to_vec(message)?;
    if payload.len() > max_len {
        return Err(FrameError::FrameTooLarge {
            len: payload.len(),
            max: max_len,
        });
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub fn decode_json_frame<T: DeserializeOwned>(
    frame: &[u8],
    max_len: usize,
) -> Result<T, FrameError> {
    if frame.len() < 4 {
        return Err(FrameError::ShortLengthPrefix);
    }
    let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    if len > max_len {
        return Err(FrameError::FrameTooLarge { len, max: max_len });
    }
    if frame.len() - 4 != len {
        return Err(FrameError::LengthMismatch {
            declared: len,
            actual: frame.len() - 4,
        });
    }
    Ok(serde_json::from_slice(&frame[4..])?)
}

pub async fn read_json_frame<R, T>(reader: &mut R, max_len: usize) -> Result<T, FrameError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len_buf = [0; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_len {
        return Err(FrameError::FrameTooLarge { len, max: max_len });
    }
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await?;
    Ok(serde_json::from_slice(&payload)?)
}

pub async fn write_json_frame<W, T>(
    writer: &mut W,
    message: &T,
    max_len: usize,
) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let frame = encode_json_frame(message, max_len)?;
    writer.write_all(&frame).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("unsupported mesh protocol version {0}")]
    UnsupportedProtocolVersion(u16),
    #[error("node name must not be blank")]
    BlankNodeName,
    #[error("node name is {len} bytes, maximum is {max}")]
    NodeNameTooLong { len: usize, max: usize },
    #[error("agent version must not be blank")]
    BlankAgentVersion,
    #[error("agent version is {len} bytes, maximum is {max}")]
    AgentVersionTooLong { len: usize, max: usize },
    #[error("resource id must not be blank")]
    BlankResourceId,
    #[error("resource id is {len} bytes, maximum is {max}")]
    ResourceIdTooLong { len: usize, max: usize },
    #[error("capacity {capacity} exceeds maximum {max}")]
    CapacityTooLarge { capacity: u32, max: u32 },
    #[error("worker advertised {count} resources, maximum is {max}")]
    TooManyResources { count: usize, max: usize },
    #[error("resource advertised {count} models, maximum is {max}")]
    TooManyModels { count: usize, max: usize },
    #[error("model advertised {count} instances, maximum is {max}")]
    TooManyModelInstances { count: usize, max: usize },
    #[error("model id must not be blank")]
    BlankModelId,
    #[error("model id is {len} bytes, maximum is {max}")]
    ModelIdTooLong { len: usize, max: usize },
    #[error("switching provider must not be blank")]
    BlankSwitchingProvider,
    #[error("switching provider is {len} bytes, maximum is {max}")]
    SwitchingProviderTooLong { len: usize, max: usize },
    #[error("switching model description is {len} bytes, maximum is {max}")]
    SwitchingDescriptionTooLong { len: usize, max: usize },
    #[error("{field} must not be blank")]
    BlankSwitchingState { field: &'static str },
    #[error("{field} is {len} bytes, maximum is {max}")]
    SwitchingStateTooLong {
        field: &'static str,
        len: usize,
        max: usize,
    },
    #[error("{field} must not contain control characters")]
    ControlCharacter { field: &'static str },
    #[error("{field} instance count {count} must be between 1 and {max}")]
    InstanceCountOutOfRange {
        field: &'static str,
        count: u32,
        max: u32,
    },
    #[error("join key must not be blank")]
    BlankJoinKey,
    #[error("worker advertised {count} request encodings, maximum is {max}")]
    TooManyRequestEncodings { count: usize, max: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame length prefix was incomplete")]
    ShortLengthPrefix,
    #[error("frame is {len} bytes, maximum is {max}")]
    FrameTooLarge { len: usize, max: usize },
    #[error("frame declared {declared} bytes but had {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn framed_json_round_trips_and_enforces_limit() {
        let request = RequestOpen {
            protocol_version: PROTOCOL_VERSION,
            request_id: Uuid::nil(),
            resource_id: "primary".to_string(),
        };
        let frame = encode_json_frame(&request, MAX_REQUEST_PREFACE_BYTES).unwrap();
        let decoded: RequestOpen = decode_json_frame(&frame, MAX_REQUEST_PREFACE_BYTES).unwrap();
        assert_eq!(decoded, request);
        assert!(matches!(
            encode_json_frame(&request, 4),
            Err(FrameError::FrameTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn async_frame_helpers_round_trip() {
        let (mut client, mut server) = duplex(1024);
        let writer = tokio::spawn(async move {
            write_control(&mut client, &HubToWorker::Ping { nonce: 42 })
                .await
                .unwrap();
        });
        let decoded: HubToWorker = read_control(&mut server).await.unwrap();
        writer.await.unwrap();
        assert_eq!(decoded, HubToWorker::Ping { nonce: 42 });
    }

    #[test]
    fn request_open_validation_rejects_unknown_version_and_blank_resource() {
        let request = RequestOpen {
            protocol_version: PROTOCOL_VERSION + 1,
            request_id: Uuid::nil(),
            resource_id: "primary".to_string(),
        };
        assert_eq!(
            validate_request_open(&request),
            Err(ProtocolError::UnsupportedProtocolVersion(
                PROTOCOL_VERSION + 1
            ))
        );

        let request = RequestOpen {
            protocol_version: PROTOCOL_VERSION,
            request_id: Uuid::nil(),
            resource_id: " ".to_string(),
        };
        assert_eq!(
            validate_request_open(&request),
            Err(ProtocolError::BlankResourceId)
        );
    }

    #[test]
    fn advertisement_validation_enforces_limits() {
        let mut advertisement = WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: Some("node-a".to_string()),
            agent_version: "test".to_string(),
            resources: vec![ResourceAdvertisement {
                resource_id: "primary".to_string(),
                models: vec![ModelAdvertisement {
                    id: "qwen".to_string(),
                    context_limit: Some(32_768),
                }],
                availability: AvailabilitySchedule::default(),
                effective_capacity: 4,
                accepting_requests: true,
                healthy: true,
                revision: 1,
            }],
            model_switching: None,
            request_encodings: Vec::new(),
        };
        validate_worker_advertisement(&advertisement).unwrap();
        advertisement.resources[0].models = vec![
            ModelAdvertisement {
                id: "m".to_string(),
                context_limit: None,
            };
            MAX_MODELS_PER_RESOURCE + 1
        ];
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::TooManyModels { .. })
        ));
    }

    #[test]
    fn model_switching_validation_bounds_all_operator_visible_fields() {
        let mut switching = ModelSwitchingAdvertisement {
            provider: "lil-fleet".to_string(),
            models: vec![SwitchableModelAdvertisement {
                id: "qwen3-flash".to_string(),
                description: Some("fast lane".to_string()),
                phase: "ready".to_string(),
                desired_state: "loaded".to_string(),
                gpu_count: 1,
                assigned_gpus: vec![0],
                max_instances: 4,
                desired_instances: 2,
                ready_instances: 1,
                instances: vec![SwitchableModelInstanceAdvertisement {
                    instance_id: "qwen3-flash-0".to_string(),
                    index: 0,
                    port: 8114,
                    phase: "ready".to_string(),
                    assigned_gpus: vec![0],
                    container_status: Some("running".to_string()),
                    last_error: None,
                    health: Some("healthy".to_string()),
                }],
            }],
            revision: 1,
        };
        validate_model_switching(&switching).unwrap();
        switching.models[0].description = Some("x".repeat(MAX_SWITCHING_DESCRIPTION_BYTES + 1));
        assert!(matches!(
            validate_model_switching(&switching),
            Err(ProtocolError::SwitchingDescriptionTooLong { .. })
        ));
        switching.models[0].description = None;
        switching.models[0].phase = "ready\nforged".to_string();
        assert!(matches!(
            validate_model_switching(&switching),
            Err(ProtocolError::ControlCharacter { .. })
        ));
        switching.models[0].phase = "ready".to_string();
        switching.models[0].instances[0].phase = "ready\nforged".to_string();
        assert!(matches!(
            validate_model_switching(&switching),
            Err(ProtocolError::ControlCharacter { .. })
        ));
        switching.models[0].instances[0].phase = "ready".to_string();
        switching.models[0].desired_instances = MAX_SWITCH_MODEL_INSTANCES + 1;
        assert!(matches!(
            validate_model_switching(&switching),
            Err(ProtocolError::InstanceCountOutOfRange { .. })
        ));
    }

    #[test]
    fn fleet_instance_diagnostics_do_not_invalidate_worker_inventory() {
        for (input, expected) in [
            (
                "container failed\nGPU unavailable\t\0".to_string(),
                Some("container failed GPU unavailable".to_string()),
            ),
            ("\n\t\0".to_string(), None),
            (
                "界".repeat(MAX_SWITCHING_DESCRIPTION_BYTES),
                Some("界".repeat(MAX_SWITCHING_DESCRIPTION_BYTES / 3)),
            ),
        ] {
            let instance = SwitchableModelInstanceAdvertisement::from_fleet(
                serde_json::from_value(serde_json::json!({
                    "instance_id": "qwen--2",
                    "index": 2,
                    "port": 8115,
                    "phase": "failed",
                    "last_error": input,
                    "container_status": "\n",
                    "health": "unhealthy\n"
                }))
                .unwrap(),
            );
            assert_eq!(instance.last_error, expected);
            assert_eq!(instance.container_status, None);
            assert_eq!(instance.health.as_deref(), Some("unhealthy"));
            let mut model =
                SwitchableModelAdvertisement::legacy("qwen", None, "failed", "ready", 1, vec![]);
            model.instances = vec![instance];
            validate_model_switching(&ModelSwitchingAdvertisement {
                provider: "lil-fleet".to_string(),
                models: vec![model],
                revision: 1,
            })
            .unwrap();
        }
    }

    #[test]
    fn unload_model_uses_a_distinct_stream_variant() {
        let request = SwitchModelRequest {
            protocol_version: REQUEST_PROTOCOL_VERSION,
            request_id: Uuid::nil(),
            model_id: "qwen3-flash".to_string(),
            instances: Some(2),
        };
        let value = serde_json::to_value(StreamOpen::UnloadModel(request.clone()))
            .expect("serialize unload request");
        assert_eq!(value["type"], "unload_model");
        assert_eq!(value["payload"]["instances"], serde_json::json!(2));
        assert_eq!(
            serde_json::from_value::<StreamOpen>(value).expect("deserialize unload request"),
            StreamOpen::UnloadModel(request)
        );
    }

    #[test]
    fn switch_model_request_is_backward_compatible_and_bounds_instances() {
        let request: SwitchModelRequest = serde_json::from_value(serde_json::json!({
            "protocol_version": REQUEST_PROTOCOL_VERSION,
            "request_id": Uuid::nil(),
            "model_id": "qwen3-flash"
        }))
        .expect("legacy request");
        assert_eq!(request.instances, None);
        validate_switch_model_request(&request).unwrap();

        let mut request = request;
        request.instances = Some(0);
        assert!(matches!(
            validate_switch_model_request(&request),
            Err(ProtocolError::InstanceCountOutOfRange { .. })
        ));
        request.instances = Some(MAX_SWITCH_MODEL_INSTANCES);
        validate_switch_model_request(&request).unwrap();
    }

    #[test]
    fn request_encodings_round_trip_and_stay_backward_compatible() {
        let advertisement = WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: None,
            agent_version: "test".to_string(),
            resources: Vec::new(),
            model_switching: None,
            request_encodings: vec![REQUEST_ENCODING_ZSTD.to_string()],
        };
        let hello = WorkerToHub::Hello(advertisement.clone());
        let value = serde_json::to_value(&hello).unwrap();
        assert_eq!(
            value["payload"]["request_encodings"],
            serde_json::json!(["zstd"])
        );
        let decoded: WorkerToHub = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(decoded, hello);
        let WorkerToHub::Hello(decoded) = decoded else {
            unreachable!()
        };
        assert!(decoded.accepts_request_encoding("ZSTD"));

        // An older worker omits the field: a newer hub must parse it and not
        // compress.
        let mut legacy = value["payload"].clone();
        legacy.as_object_mut().unwrap().remove("request_encodings");
        let legacy: WorkerAdvertisement = serde_json::from_value(legacy).unwrap();
        assert!(legacy.request_encodings.is_empty());
        assert!(!legacy.accepts_request_encoding(REQUEST_ENCODING_ZSTD));
        // ...and it serializes without the field so an older peer sees the
        // exact legacy shape.
        assert!(
            serde_json::to_value(&legacy).unwrap()["request_encodings"].is_null(),
            "empty encodings must be omitted on the wire"
        );

        // An older hub's struct (no field) tolerates a newer worker's hello.
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct LegacyAdvertisement {
            protocol_version: u16,
            node_name: Option<String>,
            agent_version: String,
            resources: Vec<ResourceAdvertisement>,
            model_switching: Option<ModelSwitchingAdvertisement>,
        }
        serde_json::from_value::<LegacyAdvertisement>(value["payload"].clone())
            .expect("older hub ignores the new field");
    }

    #[test]
    fn switch_response_error_status_round_trips_and_is_optional() {
        let response = SwitchModelResponse {
            request_id: Uuid::nil(),
            model_id: "qwen3-flash".to_string(),
            accepted: false,
            changed: false,
            error: Some("Fleet rejected the request (overrides_not_allowed)".to_string()),
            model_switching: None,
            error_status: Some(400),
        };
        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value["error_status"], serde_json::json!(400));
        assert_eq!(
            serde_json::from_value::<SwitchModelResponse>(value.clone()).unwrap(),
            response
        );
        assert_eq!(response.rejection_status(), http::StatusCode::BAD_REQUEST);

        let mut legacy = value;
        legacy.as_object_mut().unwrap().remove("error_status");
        let legacy: SwitchModelResponse = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy.error_status, None);
        assert_eq!(legacy.rejection_status(), http::StatusCode::CONFLICT);
        assert!(
            serde_json::to_value(&legacy).unwrap()["error_status"].is_null(),
            "absent status stays off the wire"
        );
        for (status, expected) in [
            (404, http::StatusCode::NOT_FOUND),
            (502, http::StatusCode::BAD_GATEWAY),
            (500, http::StatusCode::CONFLICT),
            (200, http::StatusCode::CONFLICT),
        ] {
            let response = SwitchModelResponse {
                error_status: Some(status),
                ..legacy.clone()
            };
            assert_eq!(response.rejection_status(), expected, "{status}");
        }
    }

    #[test]
    fn request_encodings_are_bounded() {
        let mut advertisement = WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: None,
            agent_version: "test".to_string(),
            resources: Vec::new(),
            model_switching: None,
            request_encodings: vec!["zstd".to_string(); MAX_REQUEST_ENCODINGS + 1],
        };
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::TooManyRequestEncodings { .. })
        ));
        advertisement.request_encodings = vec!["zstd\nforged".to_string()];
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::ControlCharacter { .. })
        ));
        advertisement.request_encodings = vec!["zstd".to_string()];
        validate_worker_advertisement(&advertisement).unwrap();
    }

    #[test]
    fn context_limits_outside_the_sane_range_are_dropped() {
        assert_eq!(sanitize_context_limit(Some(32_768)), Some(32_768));
        assert_eq!(sanitize_context_limit(Some(1)), Some(1));
        assert_eq!(
            sanitize_context_limit(Some(MAX_CONTEXT_LIMIT)),
            Some(MAX_CONTEXT_LIMIT)
        );
        for invalid in [0, -1, i64::MIN, MAX_CONTEXT_LIMIT + 1, i64::MAX] {
            assert_eq!(sanitize_context_limit(Some(invalid)), None, "{invalid}");
        }
        assert_eq!(sanitize_context_limit(None), None);
    }

    #[test]
    fn advertisement_validation_rejects_log_injection_and_oversized_model_ids() {
        let mut advertisement = WorkerAdvertisement {
            protocol_version: PROTOCOL_VERSION,
            node_name: Some("node-a\nforged-log".to_string()),
            agent_version: "test".to_string(),
            resources: Vec::new(),
            model_switching: None,
            request_encodings: Vec::new(),
        };
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::ControlCharacter { field: "node name" })
        ));

        advertisement.node_name = None;
        advertisement.resources.push(ResourceAdvertisement {
            resource_id: "primary".to_string(),
            models: vec![ModelAdvertisement {
                id: "m".repeat(MAX_MODEL_ID_BYTES + 1),
                context_limit: None,
            }],
            availability: AvailabilitySchedule::default(),
            effective_capacity: 1,
            accepting_requests: true,
            healthy: true,
            revision: 1,
        });
        assert!(matches!(
            validate_worker_advertisement(&advertisement),
            Err(ProtocolError::ModelIdTooLong { .. })
        ));
    }
}
