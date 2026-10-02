//! OpenAPI 3.1 description of every route the gateway serves, generated at
//! compile time from the handler annotations (`#[utoipa::path]`) and the
//! request/response types (`ToSchema` / `IntoParams`) so it cannot drift from
//! the code. Served at `GET /openapi.json`.
//!
//! Adding a route: annotate the handler, list it under `paths(...)` below, and
//! the drift tests in `tests/openapi.rs` hold you to it (every registered
//! route must be described, every described route must be registered).

use serde::Serialize;
use utoipa::{OpenApi, ToSchema};

/// OpenAI-style error envelope returned by the inference routes and by every
/// `AppError` (`{"error": {"message": "..."}}`).
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiError {
    pub error: ApiErrorPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorPayload {
    /// Client-safe message; internal detail is never included.
    pub message: String,
}

/// Anthropic-style error envelope returned by `/v1/messages` and
/// `/v1/messages/count_tokens` (`{"type":"error","error":{"type","message"}}`).
#[derive(Debug, Serialize, ToSchema)]
pub struct AnthropicError {
    /// Always `"error"`.
    #[schema(example = "error")]
    pub r#type: String,
    pub error: AnthropicErrorPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AnthropicErrorPayload {
    /// Anthropic error type, e.g. `invalid_request_error`, `not_found_error`,
    /// `api_error`.
    pub r#type: String,
    pub message: String,
}

/// Dashboard-side error envelope (`{"error": "message"}`), used by the
/// dashboard, history and accounts routes.
#[derive(Debug, Serialize, ToSchema)]
pub struct DashboardError {
    pub error: String,
}

/// `GET /health` body.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    /// Always `"healthy"`.
    #[schema(example = "healthy")]
    pub status: String,
}

/// `GET /` body for non-browser clients.
#[derive(Debug, Serialize, ToSchema)]
pub struct RootStatus {
    /// Always `"ok"`.
    #[schema(example = "ok")]
    pub status: String,
}

/// `POST /dashboard/api/flows/{id}/kill` success body (frozen shape; the SPA
/// decodes both fields).
#[derive(Debug, Serialize, ToSchema)]
pub struct FlowKillResponse {
    pub api_call_id: String,
    /// Always `true` on a 200.
    pub killed: bool,
}

/// `POST /v1/messages/count_tokens` body.
#[derive(Debug, Serialize, ToSchema)]
pub struct CountTokensResponse {
    pub input_tokens: u64,
}

/// Registers the security schemes the operations refer to.
struct SecuritySchemes;

impl utoipa::Modify for SecuritySchemes {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{
            ApiKey, ApiKeyValue, Http, HttpAuthScheme, SecurityScheme,
        };
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer",
            SecurityScheme::Http(
                Http::builder()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some("Virtual API key (`llmc_…`, or a YAML-configured key) as `Authorization: Bearer <key>`."))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "api_key",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "x-api-key",
                "Virtual API key in the Anthropic-style `x-api-key` header.",
            ))),
        );
        components.add_security_scheme(
            "session",
            SecurityScheme::ApiKey(ApiKey::Cookie(ApiKeyValue::with_description(
                "llmconduit_session",
                "Dashboard session cookie set by GitHub SSO (or the legacy programmatic login endpoint). Mutations also need the `x-csrf-token` header equal to the `llmconduit_csrf` cookie.",
            ))),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "llmconduit",
        description = "LLM gateway: OpenAI- and Anthropic-compatible inference routes in front of \
                       vLLM/SGLang/LiteLLM-style upstreams, plus the dashboard API that exposes \
                       live flows, persisted request history, sessions, upstream metrics and \
                       user/key accounts. Dashboard routes exist only when the server runs with \
                       `--with-debug-ui`."
    ),
    servers((url = "/", description = "The server that serves this document.")),
    tags(
        (name = "inference", description = "OpenAI/Anthropic-compatible client routes; authenticated with a virtual API key (`Authorization: Bearer llmc_…` or `x-api-key`) when client auth is required."),
        (name = "system", description = "Health and root."),
        (name = "dashboard-auth", description = "Dashboard session login/logout (cookie session; mutations need the `x-csrf-token` double-submit header)."),
        (name = "dashboard", description = "Live in-memory state: flows, metrics, topology, catalog, snapshots."),
        (name = "history", description = "Durable history from the SQL store: requests, bodies, sessions, throughput, activity, usage, metrics. 503 when no SQL storage is configured."),
        (name = "accounts", description = "Dashboard users and their API keys."),
        (name = "ui", description = "Dashboard SPA and debug UI assets, plus their WebSocket feeds."),
    ),
    paths(
        crate::http::get_root,
        crate::http::get_health,
        crate::http::get_openapi,
        crate::http::post_chat_completions,
        crate::http::post_completions,
        crate::http::post_responses,
        crate::http::get_responses,
        crate::http::post_messages,
        crate::http::probe_messages,
        crate::http::post_count_tokens,
        crate::http::get_models,
        crate::http::dashboard_flow_kill,
        crate::http::dashboard_chat_completions,
        crate::http::dashboard_key_login,
        crate::http::dashboard_auth_logout,
        crate::persistent_history_api::history_requests,
        crate::persistent_history_api::history_request_detail,
        crate::persistent_history_api::history_request_body,
        crate::persistent_history_api::history_throughput,
        crate::persistent_history_api::history_sessions,
        crate::persistent_history_api::history_session_detail,
        crate::persistent_history_api::history_usage,
        crate::persistent_history_api::history_metrics,
        crate::persistent_history_api::history_activity,
        crate::dashboard_api::dashboard_flows,
        crate::dashboard_api::dashboard_sessions_active,
        crate::dashboard_api::dashboard_flow_detail,
        crate::dashboard_api::dashboard_metrics,
        crate::dashboard_api::dashboard_topology,
        crate::dashboard_api::dashboard_catalog,
        crate::dashboard_api::dashboard_snapshot,
        crate::dashboard_api::dashboard_providers,
        crate::dashboard_api::dashboard_configured_providers,
        crate::dashboard_api::create_configured_provider,
        crate::dashboard_api::update_configured_provider,
        crate::dashboard_api::delete_configured_provider,
        crate::dashboard_fleet::fleet_models,
        crate::dashboard_fleet::fleet_load_model,
        crate::dashboard_fleet::fleet_unload_model,
        crate::dashboard_mesh::mesh_state,
        crate::dashboard_mesh::create_join_key,
        crate::dashboard_mesh::revoke_join_key,
        crate::dashboard_mesh::disable_node,
        crate::dashboard_mesh::enable_node,
        crate::dashboard_mesh::switch_node_model,
        crate::dashboard_mesh::load_node_model,
        crate::dashboard_mesh::unload_node_model,
        crate::dashboard_mesh::disable_model,
        crate::dashboard_mesh::enable_model,
        crate::accounts_api::me,
        crate::accounts_api::list_users,
        crate::accounts_api::create_user,
        crate::accounts_api::update_user,
        crate::accounts_api::delete_user,
        crate::accounts_api::list_keys,
        crate::accounts_api::create_key,
        crate::accounts_api::delete_key,
        crate::dashboard_auth::dashboard_login,
        crate::dashboard_auth::dashboard_logout,
        crate::dashboard_auth::dashboard_github_start,
        crate::dashboard_auth::dashboard_github_callback,
        crate::dashboard_ui::dashboard_index,
        crate::dashboard_ui::dashboard_asset,
        crate::debug_ui::debug_index,
        crate::debug_ui::debug_app_js,
        crate::debug_ui::debug_ws,
        crate::dashboard_ws::dashboard_ws,
    ),
    components(schemas(
        ApiError,
        ApiErrorPayload,
        AnthropicError,
        AnthropicErrorPayload,
        DashboardError,
        HealthResponse,
        RootStatus,
        FlowKillResponse,
        CountTokensResponse,
        crate::persistent_history_api::RequestsBody,
        crate::persistent_history_api::RequestDetailBody,
        crate::persistent_history_api::UsageBody,
        crate::persistent_history_api::MetricsBody,
        crate::persistent_history_api::ThroughputBody,
        crate::persistent_history_api::ActivityBody,
        crate::persistent_history_api::SessionsBody,
        crate::persistent_history_api::SessionDetailBody,
        crate::persistent_history_api::HistoryReadFailed,
        crate::control_plane_store::RequestSummary,
        crate::control_plane_store::EventRow,
        crate::control_plane_store::UsageBucket,
        crate::control_plane_store::MetricSample,
        crate::control_plane_store::ThroughputBucket,
        crate::control_plane_store::ActivityBucket,
        crate::sessions::SessionRow,
        crate::dashboard_api::FlowRow,
        crate::dashboard_api::FlowsResponse,
        crate::dashboard_api::FlowDelta,
        crate::dashboard_api::FlowUpstreamResponse,
        crate::dashboard_api::FlowDetailBody,
        crate::dashboard_api::CatalogEntry,
        crate::dashboard_api::SnapshotResponse,
        crate::dashboard_api::CostConfidence,
        crate::dashboard_fleet::FleetModelsResponse,
        crate::dashboard_fleet::FleetModelEntry,
        crate::dashboard_fleet::FleetModel,
        crate::dashboard_fleet::FleetDeploymentStatus,
        crate::dashboard_fleet::FleetInstanceStatus,
        crate::dashboard_fleet::FleetOperationResponse,
        crate::dashboard_fleet::FleetOperation,
        crate::dashboard_fleet::FleetLoadModelRequest,
        crate::dashboard_flow::FlowStatus,
        crate::dashboard_flow::FlowUsage,
        crate::dashboard_flow::ClientSource,
        crate::dashboard_flow::Attempt,
        crate::dashboard_flow::AttemptStatus,
        crate::dashboard_flow::AttemptErrorClass,
        crate::dashboard_flow::AttemptFailoverReason,
        crate::dashboard_flow::FlowSessionFacts,
        crate::dashboard_flow::PhaseTimings,
        crate::dashboard_ws::SeqCursors,
        crate::dashboard_ws::MetricsSnapshot,
        crate::dashboard_ws::TopologySnapshot,
        crate::dashboard_ws::MetricWindows,
        crate::dashboard_ws::MetricWindow,
        crate::dashboard_ws::TopologyNode,
        crate::dashboard_ws::TopologyEdge,
        crate::config::ModelPrice,
        crate::upstream::ProviderStatus,
        crate::metrics::ProviderLatency,
        crate::metrics::ProviderMetricQuality,
        crate::metrics::ProviderErrorDistribution,
        crate::accounts::SessionUser,
        crate::control_plane_store::UserRecord,
        crate::control_plane_store::ApiKeyRecord,
        crate::accounts_api::CreateUserRequest,
        crate::accounts_api::UpdateUserRequest,
        crate::accounts_api::CreateKeyRequest,
        crate::accounts_api::MeBody,
        crate::accounts_api::UsersBody,
        crate::accounts_api::KeysBody,
        crate::accounts_api::CreatedKeyBody,
        crate::accounts_api::UserUpdatedBody,
        crate::accounts_api::UserDeletedBody,
        crate::accounts_api::KeyRevokedBody,
        crate::dashboard_auth::LoginRequest,
        crate::dashboard_auth::LoginResponse,
        crate::dashboard_mesh::MeshAdminState,
        crate::dashboard_mesh::MeshJoinKey,
        crate::dashboard_mesh::MeshNode,
        crate::dashboard_mesh::MeshModelSwitching,
        crate::dashboard_mesh::MeshSwitchableModel,
        crate::dashboard_mesh::MeshSwitchableModelInstance,
        crate::dashboard_mesh::MeshDisabledModel,
        crate::dashboard_mesh::CreateMeshJoinKeyRequest,
        crate::dashboard_mesh::MeshModelLoadRequest,
        crate::dashboard_mesh::CreateMeshJoinKeyResponse,
        crate::dashboard_mesh::RevokeMeshJoinKeyResponse,
        crate::dashboard_mesh::SetMeshNodeResponse,
        crate::dashboard_mesh::MeshModelOverrideRequest,
        crate::dashboard_mesh::SetMeshModelResponse,
        crate::dashboard_mesh::SwitchMeshModelResponse,
    )),
    modifiers(&SecuritySchemes)
)]
pub struct ApiDoc;

/// The complete document, as served at `/openapi.json`.
pub fn document() -> utoipa::openapi::OpenApi {
    let mut openapi = ApiDoc::openapi();
    // Cargo.toml declares no license; utoipa would otherwise emit an empty one.
    openapi.info.license = None;
    apply_session_security(&mut openapi);
    mark_open_operations(&mut openapi);
    disambiguate_operation_ids(&mut openapi);
    openapi
}

/// The serialized OpenAPI document. The spec is static for the life of the
/// process, so it is built and serialized once instead of on every
/// `/openapi.json` hit (a few hundred KiB of allocation per request).
pub fn document_json() -> axum::body::Bytes {
    static DOCUMENT: std::sync::OnceLock<axum::body::Bytes> = std::sync::OnceLock::new();
    DOCUMENT
        .get_or_init(|| {
            axum::body::Bytes::from(
                serde_json::to_vec(&document()).expect("OpenAPI document serializes"),
            )
        })
        .clone()
}

fn operations(
    item: &mut utoipa::openapi::path::PathItem,
) -> Vec<(&'static str, &mut utoipa::openapi::path::Operation)> {
    let mut out = Vec::new();
    if let Some(op) = item.get.as_mut() {
        out.push(("get", op));
    }
    if let Some(op) = item.post.as_mut() {
        out.push(("post", op));
    }
    if let Some(op) = item.put.as_mut() {
        out.push(("put", op));
    }
    if let Some(op) = item.patch.as_mut() {
        out.push(("patch", op));
    }
    if let Some(op) = item.delete.as_mut() {
        out.push(("delete", op));
    }
    if let Some(op) = item.head.as_mut() {
        out.push(("head", op));
    }
    if let Some(op) = item.options.as_mut() {
        out.push(("options", op));
    }
    out
}

/// An operation with no security requirement is open on purpose (health, the
/// document itself, the login route, the SPA shell, the HEAD/OPTIONS probes,
/// and `/v1/*` when client auth is not required is described per route).
/// Say so explicitly (`security: []`) rather than leaving it undefined.
fn mark_open_operations(openapi: &mut utoipa::openapi::OpenApi) {
    for item in openapi.paths.paths.values_mut() {
        for (_, operation) in operations(item) {
            if operation.security.is_none() {
                operation.security = Some(Vec::new());
            }
        }
    }
}

/// One handler can serve several methods (`HEAD`/`OPTIONS /v1/messages`); each
/// operation still needs its own id, so suffix the method when an id repeats.
fn disambiguate_operation_ids(openapi: &mut utoipa::openapi::OpenApi) {
    let mut seen = std::collections::HashSet::new();
    for item in openapi.paths.paths.values_mut() {
        for (method, operation) in operations(item) {
            if let Some(id) = operation.operation_id.clone()
                && !seen.insert(id.clone())
            {
                operation.operation_id = Some(format!("{id}_{method}"));
            }
        }
    }
}

/// Mirrors the router in `http.rs`: `/dashboard/api/*`, `/debug` and
/// `/debug/app.js` sit behind the `require_session` middleware, and the two
/// WebSocket routes check the same session cookie (plus `Origin`) in-handler.
/// `/dashboard` (the shell), `/dashboard/assets/*`, login and logout are open.
/// Stamp the requirement on each gated operation so the document says what the
/// server enforces, without every handler repeating it.
fn apply_session_security(openapi: &mut utoipa::openapi::OpenApi) {
    use utoipa::openapi::security::SecurityRequirement;
    for (path, item) in openapi.paths.paths.iter_mut() {
        let gated = path.starts_with("/dashboard/api/")
            || path == "/dashboard/ws"
            || path.starts_with("/debug");
        if !gated {
            continue;
        }
        for (_, operation) in operations(item) {
            if operation.security.is_none() {
                operation.security = Some(vec![SecurityRequirement::new(
                    "session",
                    Vec::<String>::new(),
                )]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_serializes_with_shared_error_schemas() {
        let json = serde_json::to_value(document()).expect("serializable");
        let schemas = &json["components"]["schemas"];
        for name in ["ApiError", "AnthropicError", "DashboardError"] {
            assert!(schemas.get(name).is_some(), "missing schema {name}");
        }
    }
}
