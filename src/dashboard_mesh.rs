use crate::dashboard_access::{ManagementActor, ManagementPermission};
use crate::dashboard_api::json_no_store;
use crate::dashboard_auth::AuthSession;
use crate::dashboard_auth::DashboardAuth;
use crate::dashboard_auth::MutationPolicy;
use crate::engine::Gateway;
use crate::mesh::protocol::{
    MAX_SWITCH_MODEL_INSTANCES, ModelSwitchingAdvertisement, SwitchableModelInstanceAdvertisement,
    validate_model_id, validate_resource_id,
};
use crate::mesh::store::CreatedJoinKey;
use crate::mesh::store::DisabledMeshModelRecord;
use crate::mesh::store::JoinKeyRecord;
use crate::mesh::store::MeshNodeRecord;
use crate::mesh::store::now_ms;
use axum::Extension;
use axum::Json;
use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use utoipa::ToSchema;

const MAX_JOIN_KEY_LABEL_BYTES: usize = 128;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshAdminState {
    pub join_keys: Vec<MeshJoinKey>,
    pub nodes: Vec<MeshNode>,
    pub disabled_models: Vec<MeshDisabledModel>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshJoinKey {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub enabled: bool,
    pub created_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<i64>,
    pub use_count: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshNode {
    pub endpoint_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub enabled: bool,
    pub joined_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_switching: Option<MeshModelSwitching>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshModelSwitching {
    pub provider: String,
    pub models: Vec<MeshSwitchableModel>,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshSwitchableModel {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub phase: String,
    pub desired_state: String,
    pub gpu_count: u32,
    pub assigned_gpus: Vec<u32>,
    pub max_instances: u32,
    pub desired_instances: u32,
    pub ready_instances: u32,
    pub instances: Vec<MeshSwitchableModelInstance>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshSwitchableModelInstance {
    pub instance_id: String,
    pub index: u32,
    pub port: u16,
    pub phase: String,
    pub assigned_gpus: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MeshDisabledModel {
    pub endpoint_id: String,
    pub resource_id: String,
    pub model: String,
    pub disabled_at_ms: i64,
}

/// Omitted `max_uses` / `expires_in_secs` default to a single-use key valid
/// for 24 hours: a forgotten join key is a standing credential to add GPU
/// workers. An explicit JSON `null` still requests no limit.
#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct CreateMeshJoinKeyRequest {
    pub label: Option<String>,
    #[serde(default, deserialize_with = "present_option")]
    #[schema(value_type = Option<i64>, nullable = true)]
    pub max_uses: Option<Option<i64>>,
    #[serde(default, deserialize_with = "present_option")]
    #[schema(value_type = Option<i64>, nullable = true)]
    pub expires_in_secs: Option<Option<i64>>,
}

const DEFAULT_JOIN_KEY_MAX_USES: i64 = 1;
const DEFAULT_JOIN_KEY_TTL_SECS: i64 = 24 * 60 * 60;

/// Distinguishes an absent field (`None`) from an explicit `null`
/// (`Some(None)`).
fn present_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CreateMeshJoinKeyResponse {
    pub join_key: MeshJoinKey,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RevokeMeshJoinKeyResponse {
    pub updated: bool,
    pub disabled_endpoint_ids: Vec<String>,
    pub evicted_endpoint_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SetMeshNodeResponse {
    pub updated: bool,
    pub endpoint_id: String,
    pub enabled: bool,
    pub evicted: bool,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct MeshModelOverrideRequest {
    pub endpoint_id: String,
    pub resource_id: String,
    pub model: String,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MeshModelLoadRequest {
    pub instances: u32,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SetMeshModelResponse {
    pub updated: bool,
    pub endpoint_id: String,
    pub resource_id: String,
    pub model: String,
    pub disabled: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SwitchMeshModelResponse {
    pub endpoint_id: String,
    pub model_id: String,
    pub accepted: bool,
    pub changed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[utoipa::path(
    get,
    path = "/dashboard/api/mesh",
    tag = "dashboard",
    responses(
        (status = 200, body = MeshAdminState),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn mesh_state(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
) -> Response {
    if !actor.allows(ManagementPermission::FleetModelsRead) {
        return mesh_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    let store = admin.store();
    let (join_keys, nodes, disabled_models) = match futures::try_join!(
        store.list_join_keys(),
        store.list_nodes(),
        store.list_disabled_models()
    ) {
        Ok(records) => records,
        Err(err) => {
            tracing::error!(error = %err, "failed to read mesh admin state");
            return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
        }
    };
    let registry = admin.registry();
    let nodes = nodes
        .into_iter()
        .map(|record| {
            let switching = record
                .endpoint_id
                .parse()
                .ok()
                .and_then(|endpoint| registry.model_switching(endpoint))
                .map(MeshModelSwitching::from);
            let mut node = MeshNode::from(record);
            node.model_switching = switching;
            node
        })
        .collect();
    json_no_store(
        StatusCode::OK,
        &MeshAdminState {
            join_keys: join_keys.into_iter().map(MeshJoinKey::from).collect(),
            nodes,
            disabled_models: disabled_models
                .into_iter()
                .map(MeshDisabledModel::from)
                .collect(),
        },
    )
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/join-keys",
    tag = "dashboard",
    request_body = CreateMeshJoinKeyRequest,
    responses(
        (status = 200, body = CreateMeshJoinKeyResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn create_join_key(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    headers: HeaderMap,
    Json(body): Json<CreateMeshJoinKeyRequest>,
) -> Response {
    if let Some(response) = authorize_mesh_mutation(auth.as_ref(), &session, &headers).await {
        return response;
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let Some((expires_at_ms, max_uses)) = validate_join_key_request(&body) else {
        return mesh_error(
            StatusCode::BAD_REQUEST,
            "max_uses and expires_in_secs must be greater than zero when provided",
        );
    };
    if body
        .label
        .as_deref()
        .is_some_and(|label| label.len() > MAX_JOIN_KEY_LABEL_BYTES)
    {
        return mesh_error(StatusCode::BAD_REQUEST, "label must be at most 128 bytes");
    }
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    match admin
        .store()
        .create_join_key(body.label, expires_at_ms, max_uses)
        .await
    {
        Ok(created) => json_no_store(StatusCode::OK, &CreateMeshJoinKeyResponse::from(created)),
        Err(err) => {
            tracing::error!(error = %err, "failed to create mesh join key");
            mesh_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create join key",
            )
        }
    }
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/join-keys/{id}/revoke",
    tag = "dashboard",
    params(("id" = String, Path, description = "Join key id.")),
    responses(
        (status = 200, body = RevokeMeshJoinKeyResponse),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn revoke_join_key(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = authorize_mesh_mutation(auth.as_ref(), &session, &headers).await {
        return response;
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    match admin.store().revoke_join_key(&id).await {
        Ok(revoked) => {
            let mut evicted_endpoint_ids = Vec::new();
            for endpoint_id in &revoked.disabled_endpoint_ids {
                if let Ok(endpoint) = endpoint_id.parse()
                    && admin.disable_live_node(endpoint)
                {
                    evicted_endpoint_ids.push(endpoint_id.clone());
                }
            }
            json_no_store(
                StatusCode::OK,
                &RevokeMeshJoinKeyResponse {
                    updated: revoked.updated,
                    disabled_endpoint_ids: revoked.disabled_endpoint_ids,
                    evicted_endpoint_ids,
                },
            )
        }
        Err(err) => {
            tracing::error!(error = %err, "failed to revoke mesh join key");
            mesh_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to revoke join key",
            )
        }
    }
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/disable",
    tag = "dashboard",
    params(("endpoint_id" = String, Path, description = "Iroh endpoint id.")),
    responses(
        (status = 200, body = SetMeshNodeResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn disable_node(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    Path(endpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    set_node_enabled(gateway, auth, session, headers, endpoint_id, false).await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/enable",
    tag = "dashboard",
    params(("endpoint_id" = String, Path, description = "Iroh endpoint id.")),
    responses(
        (status = 200, body = SetMeshNodeResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn enable_node(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    Path(endpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    set_node_enabled(gateway, auth, session, headers, endpoint_id, true).await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/switch",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Advertised Fleet model id.")
    ),
    request_body(content = Option<MeshModelLoadRequest>),
    responses(
        (status = 200, body = SwitchMeshModelResponse),
        (status = 202, body = SwitchMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 409, body = crate::openapi::DashboardError),
        (status = 502, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn switch_node_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    Path((endpoint_id, model_id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let instances = match parse_mesh_model_load_body(&body) {
        Ok(instances) => instances,
        Err(error) => return error.into_response(),
    };
    set_node_model_state(gateway, actor, endpoint_id, model_id, false, instances).await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/load",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Advertised Fleet model id.")
    ),
    request_body(content = Option<MeshModelLoadRequest>),
    responses(
        (status = 200, body = SwitchMeshModelResponse),
        (status = 202, body = SwitchMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 409, body = crate::openapi::DashboardError),
        (status = 502, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn load_node_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    Path((endpoint_id, model_id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let instances = match parse_mesh_model_load_body(&body) {
        Ok(instances) => instances,
        Err(error) => return error.into_response(),
    };
    set_node_model_state(gateway, actor, endpoint_id, model_id, false, instances).await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/nodes/{endpoint_id}/models/{model_id}/unload",
    tag = "dashboard",
    params(
        ("endpoint_id" = String, Path, description = "Iroh endpoint id."),
        ("model_id" = String, Path, description = "Advertised Fleet model id.")
    ),
    responses(
        (status = 200, body = SwitchMeshModelResponse),
        (status = 202, body = SwitchMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError),
        (status = 409, body = crate::openapi::DashboardError),
        (status = 502, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn unload_node_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(actor): Extension<ManagementActor>,
    Path((endpoint_id, model_id)): Path<(String, String)>,
) -> Response {
    set_node_model_state(gateway, actor, endpoint_id, model_id, true, None).await
}

async fn set_node_model_state(
    gateway: Arc<Gateway>,
    actor: ManagementActor,
    endpoint_id: String,
    model_id: String,
    unload: bool,
    instances: Option<u32>,
) -> Response {
    let permission = fleet_model_permission(unload);
    if !actor.allows(permission) {
        return mesh_error(StatusCode::FORBIDDEN, "management permission denied");
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let endpoint_id = endpoint_id.trim().to_string();
    let model_id = model_id.trim().to_string();
    let endpoint = match endpoint_id.parse() {
        Ok(endpoint) => endpoint,
        Err(_) => return mesh_error(StatusCode::BAD_REQUEST, "invalid mesh endpoint id"),
    };
    if validate_model_id(&model_id).is_err() {
        return mesh_error(StatusCode::BAD_REQUEST, "invalid model id");
    }
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    let result = if unload {
        admin
            .registry()
            .unload_model(endpoint, model_id.clone())
            .await
    } else {
        admin
            .registry()
            .switch_model_instances(endpoint, model_id.clone(), instances)
            .await
    };
    match result {
        Ok(result) if result.accepted => json_no_store(
            if result.changed {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            },
            &SwitchMeshModelResponse {
                endpoint_id,
                model_id,
                accepted: true,
                changed: result.changed,
                error: None,
            },
        ),
        Ok(result) => mesh_error(
            result.rejection_status(),
            result
                .error
                .as_deref()
                .unwrap_or("downstream provider rejected the model switch"),
        ),
        Err(err) => {
            tracing::warn!(error = %err, endpoint_id, model_id, "mesh model switch failed");
            mesh_error(err.status_code(), &err.client_message)
        }
    }
}

fn fleet_model_permission(unload: bool) -> ManagementPermission {
    if unload {
        ManagementPermission::FleetModelsUnload
    } else {
        ManagementPermission::FleetModelsLoad
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MeshLoadBodyError {
    InvalidShape,
    InvalidInstances,
}

impl MeshLoadBodyError {
    fn into_response(self) -> Response {
        match self {
            Self::InvalidShape => mesh_error(
                StatusCode::BAD_REQUEST,
                "load body must be empty or JSON with only instances",
            ),
            Self::InvalidInstances => mesh_error(
                StatusCode::BAD_REQUEST,
                "instances must be between 1 and 64",
            ),
        }
    }
}

fn parse_mesh_model_load_body(body: &[u8]) -> Result<Option<u32>, MeshLoadBodyError> {
    if body.is_empty() {
        return Ok(None);
    }
    let request: MeshModelLoadRequest =
        serde_json::from_slice(body).map_err(|_| MeshLoadBodyError::InvalidShape)?;
    if !(1..=MAX_SWITCH_MODEL_INSTANCES).contains(&request.instances) {
        return Err(MeshLoadBodyError::InvalidInstances);
    }
    Ok(Some(request.instances))
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/models/disable",
    tag = "dashboard",
    request_body = MeshModelOverrideRequest,
    responses(
        (status = 200, body = SetMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn disable_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    headers: HeaderMap,
    Json(body): Json<MeshModelOverrideRequest>,
) -> Response {
    set_model_disabled(gateway, auth, session, headers, body, true).await
}

#[utoipa::path(
    post,
    path = "/dashboard/api/mesh/models/enable",
    tag = "dashboard",
    request_body = MeshModelOverrideRequest,
    responses(
        (status = 200, body = SetMeshModelResponse),
        (status = 400, body = crate::openapi::DashboardError),
        (status = 401, body = crate::openapi::DashboardError),
        (status = 403, body = crate::openapi::DashboardError),
        (status = 404, body = crate::openapi::DashboardError)
    ),
    security(("session" = []))
)]
pub async fn enable_model(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Extension(session): Extension<AuthSession>,
    headers: HeaderMap,
    Json(body): Json<MeshModelOverrideRequest>,
) -> Response {
    set_model_disabled(gateway, auth, session, headers, body, false).await
}

async fn set_node_enabled(
    gateway: Arc<Gateway>,
    auth: Arc<DashboardAuth>,
    session: AuthSession,
    headers: HeaderMap,
    endpoint_id: String,
    enabled: bool,
) -> Response {
    if let Some(response) = authorize_mesh_mutation(auth.as_ref(), &session, &headers).await {
        return response;
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let endpoint_id = endpoint_id.trim().to_string();
    let endpoint = match endpoint_id.parse() {
        Ok(endpoint) => endpoint,
        Err(_) => return mesh_error(StatusCode::BAD_REQUEST, "invalid mesh endpoint id"),
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    match admin.store().set_node_enabled(&endpoint_id, enabled).await {
        Ok(updated) => {
            if !updated {
                return mesh_error(StatusCode::NOT_FOUND, "mesh node not found");
            }
            let evicted = if updated && !enabled {
                admin.disable_live_node(endpoint)
            } else {
                false
            };
            json_no_store(
                StatusCode::OK,
                &SetMeshNodeResponse {
                    updated,
                    endpoint_id,
                    enabled,
                    evicted,
                },
            )
        }
        Err(err) => {
            tracing::error!(error = %err, "failed to update mesh node");
            mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update node")
        }
    }
}

async fn set_model_disabled(
    gateway: Arc<Gateway>,
    auth: Arc<DashboardAuth>,
    session: AuthSession,
    headers: HeaderMap,
    body: MeshModelOverrideRequest,
    disabled: bool,
) -> Response {
    if let Some(response) = authorize_mesh_mutation(auth.as_ref(), &session, &headers).await {
        return response;
    }
    let Some(admin) = gateway.mesh_admin() else {
        return mesh_error(StatusCode::NOT_FOUND, "mesh controller is disabled");
    };
    let target = match ModelOverrideTarget::try_from(body) {
        Ok(target) => target,
        Err(message) => return mesh_error(StatusCode::BAD_REQUEST, message),
    };
    if let Err(err) = admin.initialize().await {
        tracing::error!(error = %err, "failed to initialize mesh admin store");
        return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
    }
    if disabled
        && !admin
            .registry()
            .has_resource_model(target.endpoint, &target.resource_id, &target.model)
    {
        return mesh_error(
            StatusCode::NOT_FOUND,
            "mesh endpoint/resource/model is not currently advertised",
        );
    }
    if !disabled {
        let exists = match admin.store().list_disabled_models().await {
            Ok(records) => records.into_iter().any(|record| {
                record.endpoint_id == target.endpoint_id
                    && record.resource_id == target.resource_id
                    && record.model.eq_ignore_ascii_case(&target.model)
            }),
            Err(err) => {
                tracing::error!(error = %err, "failed to read mesh model overrides");
                return mesh_error(StatusCode::INTERNAL_SERVER_ERROR, "mesh state unavailable");
            }
        };
        if !exists {
            return mesh_error(StatusCode::NOT_FOUND, "mesh model override not found");
        }
    }
    let changed = admin
        .store()
        .set_model_disabled(
            &target.endpoint_id,
            &target.resource_id,
            &target.model,
            disabled,
            now_ms(),
        )
        .await;
    match changed {
        Ok(updated) => {
            admin.apply_model_disabled(
                target.endpoint,
                &target.resource_id,
                &target.model,
                disabled,
            );
            json_no_store(
                StatusCode::OK,
                &SetMeshModelResponse {
                    updated,
                    endpoint_id: target.endpoint_id,
                    resource_id: target.resource_id,
                    model: target.model,
                    disabled,
                },
            )
        }
        Err(err) => {
            tracing::error!(error = %err, "failed to update mesh model override");
            mesh_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to update model override",
            )
        }
    }
}

pub(crate) async fn authorize_mesh_mutation(
    auth: &DashboardAuth,
    session: &AuthSession,
    headers: &HeaderMap,
) -> Option<Response> {
    if let Some(response) = authorize_mesh_admin_read(auth, session, headers) {
        return Some(response);
    }
    if let Err(denied) = auth.authorize_mutation(headers) {
        return Some(mesh_error(denied.status(), denied.message()));
    }
    None
}

pub(crate) fn authorize_mesh_admin_read(
    auth: &DashboardAuth,
    session: &AuthSession,
    headers: &HeaderMap,
) -> Option<Response> {
    if auth.delegated_session(headers).is_some() {
        return Some(mesh_error(
            StatusCode::FORBIDDEN,
            "administrator role required",
        ));
    }
    if session
        .user
        .as_ref()
        .map_or_else(|| session.bootstrap_admin(), |user| user.is_admin)
    {
        None
    } else {
        Some(mesh_error(
            StatusCode::FORBIDDEN,
            "administrator role required",
        ))
    }
}

fn validate_join_key_request(
    body: &CreateMeshJoinKeyRequest,
) -> Option<(Option<i64>, Option<i64>)> {
    let max_uses = body.max_uses.unwrap_or(Some(DEFAULT_JOIN_KEY_MAX_USES));
    let expires_in_secs = body
        .expires_in_secs
        .unwrap_or(Some(DEFAULT_JOIN_KEY_TTL_SECS));
    if max_uses.is_some_and(|uses| uses <= 0) || expires_in_secs.is_some_and(|seconds| seconds <= 0)
    {
        return None;
    }
    let expires_at_ms =
        expires_in_secs.map(|seconds| now_ms().saturating_add(seconds.saturating_mul(1000)));
    Some((expires_at_ms, max_uses))
}

#[derive(Debug)]
struct ModelOverrideTarget {
    endpoint: iroh::EndpointId,
    endpoint_id: String,
    resource_id: String,
    model: String,
}

impl TryFrom<MeshModelOverrideRequest> for ModelOverrideTarget {
    type Error = &'static str;

    fn try_from(body: MeshModelOverrideRequest) -> Result<Self, Self::Error> {
        let endpoint_id = body.endpoint_id.trim().to_string();
        let resource_id = body.resource_id.trim().to_string();
        let model = body.model.trim().to_ascii_lowercase();
        if endpoint_id.is_empty() || resource_id.is_empty() || model.is_empty() {
            return Err("endpoint_id, resource_id, and model are required");
        }
        validate_resource_id(&resource_id).map_err(|_| "invalid resource_id")?;
        validate_model_id(&model).map_err(|_| "invalid model")?;
        let endpoint = endpoint_id
            .parse()
            .map_err(|_| "invalid mesh endpoint id")?;
        Ok(Self {
            endpoint,
            endpoint_id,
            resource_id,
            model,
        })
    }
}

fn mesh_error(status: StatusCode, message: &str) -> Response {
    json_no_store(status, &serde_json::json!({ "error": message }))
}

impl From<JoinKeyRecord> for MeshJoinKey {
    fn from(value: JoinKeyRecord) -> Self {
        Self {
            id: value.id,
            label: value.label,
            enabled: value.enabled,
            created_at_ms: value.created_at_ms,
            expires_at_ms: value.expires_at_ms,
            max_uses: value.max_uses,
            use_count: value.use_count,
        }
    }
}

impl From<CreatedJoinKey> for CreateMeshJoinKeyResponse {
    fn from(value: CreatedJoinKey) -> Self {
        let token = value.token;
        Self {
            join_key: MeshJoinKey {
                id: value.id,
                label: value.label,
                enabled: true,
                created_at_ms: value.created_at_ms,
                expires_at_ms: value.expires_at_ms,
                max_uses: value.max_uses,
                use_count: 0,
            },
            token,
        }
    }
}

impl From<MeshNodeRecord> for MeshNode {
    fn from(value: MeshNodeRecord) -> Self {
        Self {
            endpoint_id: value.endpoint_id,
            label: value.label,
            enabled: value.enabled,
            joined_at_ms: value.joined_at_ms,
            join_key_id: value.join_key_id,
            last_seen_at_ms: value.last_seen_at_ms,
            model_switching: None,
        }
    }
}

impl From<ModelSwitchingAdvertisement> for MeshModelSwitching {
    fn from(value: ModelSwitchingAdvertisement) -> Self {
        Self {
            provider: value.provider,
            models: value
                .models
                .into_iter()
                .map(|model| MeshSwitchableModel {
                    id: model.id,
                    description: model.description,
                    phase: model.phase,
                    desired_state: model.desired_state,
                    gpu_count: model.gpu_count,
                    assigned_gpus: model.assigned_gpus,
                    max_instances: model.max_instances,
                    desired_instances: model.desired_instances,
                    ready_instances: model.ready_instances,
                    instances: model
                        .instances
                        .into_iter()
                        .map(MeshSwitchableModelInstance::from)
                        .collect(),
                })
                .collect(),
            revision: value.revision,
        }
    }
}

impl From<SwitchableModelInstanceAdvertisement> for MeshSwitchableModelInstance {
    fn from(value: SwitchableModelInstanceAdvertisement) -> Self {
        Self {
            instance_id: value.instance_id,
            index: value.index,
            port: value.port,
            phase: value.phase,
            assigned_gpus: value.assigned_gpus,
            container_status: value.container_status,
            last_error: value.last_error,
            health: value.health,
        }
    }
}

impl From<DisabledMeshModelRecord> for MeshDisabledModel {
    fn from(value: DisabledMeshModelRecord) -> Self {
        Self {
            endpoint_id: value.endpoint_id,
            resource_id: value.resource_id,
            model: value.model,
            disabled_at_ms: value.disabled_at_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard_auth::CSRF_COOKIE;
    use crate::dashboard_auth::CSRF_HEADER;
    use crate::dashboard_auth::DashboardEnv;
    use crate::dashboard_auth::SESSION_COOKIE;
    use axum::http::HeaderValue;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use iroh::SecretKey;
    use std::net::SocketAddr;

    fn test_auth() -> Arc<DashboardAuth> {
        DashboardAuth::from_env(
            "127.0.0.1:0".parse::<SocketAddr>().expect("socket addr"),
            &DashboardEnv {
                token: Some("dashboard-token".to_string()),
                session_key_b64: Some(STANDARD.encode([7u8; 32])),
                public_origin: None,
                allow_insecure: false,
                allow_mutations: true,
                ..Default::default()
            },
        )
        .expect("auth")
        .auth
    }

    fn user_session(is_admin: bool) -> AuthSession {
        AuthSession {
            exp: u64::MAX,
            user: Some(crate::accounts::SessionUser {
                id: "user-id".to_string(),
                username: "operator".to_string(),
                is_admin,
            }),
            kind: crate::dashboard_auth::AuthSessionKind::User,
        }
    }

    fn bootstrap_session() -> AuthSession {
        AuthSession {
            exp: u64::MAX,
            user: None,
            kind: crate::dashboard_auth::AuthSessionKind::DashboardToken,
        }
    }

    fn csrf_headers(auth: &DashboardAuth) -> HeaderMap {
        let csrf = auth.issue_csrf_token();
        let mut headers = HeaderMap::new();
        headers.insert(CSRF_HEADER, HeaderValue::from_str(&csrf).expect("csrf"));
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&format!("{CSRF_COOKIE}={csrf}")).expect("cookie"),
        );
        headers
    }

    #[test]
    fn remote_fleet_actions_require_distinct_management_permissions() {
        assert_eq!(
            fleet_model_permission(false),
            ManagementPermission::FleetModelsLoad
        );
        assert_eq!(
            fleet_model_permission(true),
            ManagementPermission::FleetModelsUnload
        );

        let read_only = ManagementActor::Delegated {
            session_id: "sess_mesh_fleet_read".into(),
            principal_id: "usr_mesh_fleet_read".into(),
            key_id: "key_mesh_fleet_read".into(),
            permissions: Arc::from([ManagementPermission::FleetModelsRead]),
        };
        assert!(read_only.allows(ManagementPermission::FleetModelsRead));
        assert!(!read_only.allows(ManagementPermission::FleetModelsLoad));
        assert!(!read_only.allows(ManagementPermission::FleetModelsUnload));
    }

    #[test]
    fn mesh_load_body_is_empty_legacy_or_strict_instances() {
        assert_eq!(parse_mesh_model_load_body(b"").unwrap(), None);
        assert_eq!(
            parse_mesh_model_load_body(br#"{"instances":2}"#).unwrap(),
            Some(2)
        );
        assert!(parse_mesh_model_load_body(br#"{"instances":0}"#).is_err());
        assert!(parse_mesh_model_load_body(br#"{"instances":65}"#).is_err());
        assert!(parse_mesh_model_load_body(br#"{"instances":1,"extra":true}"#).is_err());
        assert!(parse_mesh_model_load_body(br#"{}"#).is_err());
    }

    #[test]
    fn model_override_target_trims_and_canonicalizes_model() {
        let endpoint_id = SecretKey::generate().public().to_string();

        let target = ModelOverrideTarget::try_from(MeshModelOverrideRequest {
            endpoint_id: format!(" {endpoint_id} "),
            resource_id: " gpu-a ".to_string(),
            model: " QWEN-Flash ".to_string(),
        })
        .expect("valid target");

        assert_eq!(target.endpoint_id, endpoint_id);
        assert_eq!(target.resource_id, "gpu-a");
        assert_eq!(target.model, "qwen-flash");
    }

    #[test]
    fn model_override_target_rejects_blank_and_invalid_endpoint() {
        assert_eq!(
            ModelOverrideTarget::try_from(MeshModelOverrideRequest {
                endpoint_id: " ".to_string(),
                resource_id: "gpu".to_string(),
                model: "qwen".to_string(),
            })
            .expect_err("blank endpoint rejected"),
            "endpoint_id, resource_id, and model are required"
        );
        assert_eq!(
            ModelOverrideTarget::try_from(MeshModelOverrideRequest {
                endpoint_id: "not-an-endpoint".to_string(),
                resource_id: "gpu".to_string(),
                model: "qwen".to_string(),
            })
            .expect_err("invalid endpoint rejected"),
            "invalid mesh endpoint id"
        );
    }

    #[test]
    fn join_key_request_rejects_non_positive_limits() {
        assert!(
            validate_join_key_request(&CreateMeshJoinKeyRequest {
                label: None,
                max_uses: Some(Some(0)),
                expires_in_secs: None,
            })
            .is_none()
        );
        assert!(
            validate_join_key_request(&CreateMeshJoinKeyRequest {
                label: None,
                max_uses: None,
                expires_in_secs: Some(Some(-1)),
            })
            .is_none()
        );
    }

    #[test]
    fn join_keys_default_to_single_use_and_a_day_but_allow_explicit_unlimited() {
        let parse = |json: serde_json::Value| -> CreateMeshJoinKeyRequest {
            serde_json::from_value(json).expect("request")
        };

        let before = now_ms();
        let (expires_at_ms, max_uses) =
            validate_join_key_request(&parse(serde_json::json!({}))).expect("defaults");
        assert_eq!(max_uses, Some(DEFAULT_JOIN_KEY_MAX_USES));
        let expires_at_ms = expires_at_ms.expect("default expiry");
        assert!(expires_at_ms >= before + DEFAULT_JOIN_KEY_TTL_SECS * 1000);
        assert!(expires_at_ms <= now_ms() + DEFAULT_JOIN_KEY_TTL_SECS * 1000);

        let explicit = parse(serde_json::json!({"max_uses": 5, "expires_in_secs": 60}));
        let (expires_at_ms, max_uses) = validate_join_key_request(&explicit).expect("explicit");
        assert_eq!(max_uses, Some(5));
        assert!(expires_at_ms.unwrap() <= now_ms() + 60_000);

        let unlimited = parse(serde_json::json!({"max_uses": null, "expires_in_secs": null}));
        assert_eq!(unlimited.max_uses, Some(None));
        assert_eq!(
            validate_join_key_request(&unlimited).expect("unlimited"),
            (None, None)
        );
    }

    #[test]
    fn model_override_target_rejects_oversized_identifiers() {
        let endpoint_id = SecretKey::generate().public().to_string();
        assert_eq!(
            ModelOverrideTarget::try_from(MeshModelOverrideRequest {
                endpoint_id: endpoint_id.clone(),
                resource_id: "r".repeat(crate::mesh::protocol::MAX_RESOURCE_ID_BYTES + 1),
                model: "qwen".to_string(),
            })
            .expect_err("oversized resource id rejected"),
            "invalid resource_id"
        );
        assert_eq!(
            ModelOverrideTarget::try_from(MeshModelOverrideRequest {
                endpoint_id,
                resource_id: "gpu".to_string(),
                model: "m".repeat(crate::mesh::protocol::MAX_MODEL_ID_BYTES + 1),
            })
            .expect_err("oversized model id rejected"),
            "invalid model"
        );
    }

    #[test]
    fn mesh_admin_read_rejects_non_admin_user() {
        let auth = test_auth();
        let headers = HeaderMap::new();

        let response = authorize_mesh_admin_read(&auth, &user_session(false), &headers)
            .expect("non-admin rejected");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn mesh_admin_read_allows_admin_user_and_bootstrap_session() {
        let auth = test_auth();
        let headers = HeaderMap::new();

        assert!(authorize_mesh_admin_read(&auth, &user_session(true), &headers).is_none());
        assert!(authorize_mesh_admin_read(&auth, &bootstrap_session(), &headers).is_none());
    }

    #[test]
    fn mesh_admin_read_rejects_delegated_management_session() {
        let auth = test_auth();
        let cookie = auth.issue_delegated_session("delegated-session", u64::MAX - 1);
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&format!("{SESSION_COOKIE}={cookie}")).expect("cookie"),
        );

        let response = authorize_mesh_admin_read(&auth, &bootstrap_session(), &headers)
            .expect("delegated session rejected");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn mesh_mutation_keeps_csrf_gate_for_bootstrap_session() {
        let auth = test_auth();

        assert!(
            authorize_mesh_mutation(&auth, &bootstrap_session(), &csrf_headers(&auth))
                .await
                .is_none()
        );
        assert_eq!(
            authorize_mesh_mutation(&auth, &bootstrap_session(), &HeaderMap::new())
                .await
                .expect("missing csrf rejected")
                .status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn mesh_mutation_rejects_delegated_session_even_with_csrf() {
        let auth = test_auth();
        let mut headers = csrf_headers(&auth);
        let cookie = auth.issue_delegated_session("delegated-session", u64::MAX - 1);
        let csrf_cookie = headers
            .get(axum::http::header::COOKIE)
            .and_then(|value| value.to_str().ok())
            .expect("csrf cookie");
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&format!("{SESSION_COOKIE}={cookie}; {csrf_cookie}"))
                .expect("cookie"),
        );

        let response = authorize_mesh_mutation(&auth, &bootstrap_session(), &headers)
            .await
            .expect("delegated rejected");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
