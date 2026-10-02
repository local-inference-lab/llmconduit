//! Wire contract and isolated router for the dashboard Access console.
//!
//! Authentication/session lookup and CSRF/origin enforcement stay in `http.rs` and
//! `dashboard_auth`; those layers insert a [`ManagementActor`] before these handlers run.
//! The authz implementation owns [`AccessBackend`] and therefore remains free to transact,
//! rebuild its immutable snapshot, and revoke delegated sessions atomically.

use axum::Router;
use axum::extract::{Extension, Json, Path};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagementPermission {
    #[serde(rename = "auth.keys.read")]
    KeysRead,
    #[serde(rename = "auth.keys.create")]
    KeysCreate,
    #[serde(rename = "auth.keys.revoke")]
    KeysRevoke,
    #[serde(rename = "auth.keys.rotate")]
    KeysRotate,
    #[serde(rename = "auth.principals.read")]
    PrincipalsRead,
    #[serde(rename = "auth.principals.write")]
    PrincipalsWrite,
    #[serde(rename = "auth.groups.read")]
    GroupsRead,
    #[serde(rename = "auth.groups.write")]
    GroupsWrite,
    #[serde(rename = "auth.roles.read")]
    RolesRead,
    #[serde(rename = "auth.roles.write")]
    RolesWrite,
    #[serde(rename = "auth.policies.read")]
    PoliciesRead,
    #[serde(rename = "auth.policies.write")]
    PoliciesWrite,
    #[serde(rename = "auth.usage.read")]
    UsageRead,
    #[serde(rename = "auth.audit.read")]
    AuditRead,
    #[serde(rename = "auth.pricing.read")]
    PricingRead,
    #[serde(rename = "auth.pricing.sync")]
    PricingSync,
    #[serde(rename = "auth.pricing.write")]
    PricingWrite,
    #[serde(rename = "auth.sessions.read")]
    SessionsRead,
    #[serde(rename = "auth.sessions.terminate")]
    SessionsTerminate,
    #[serde(rename = "fleet.models.read")]
    FleetModelsRead,
    #[serde(rename = "fleet.models.load")]
    FleetModelsLoad,
    #[serde(rename = "fleet.models.unload")]
    FleetModelsUnload,
}

#[derive(Debug, Clone)]
pub enum ManagementActor {
    Bootstrap,
    Delegated {
        session_id: String,
        principal_id: String,
        key_id: String,
        permissions: Arc<[ManagementPermission]>,
    },
}

impl ManagementActor {
    pub(crate) fn allows(&self, permission: ManagementPermission) -> bool {
        matches!(self, Self::Bootstrap)
            || matches!(self, Self::Delegated { permissions, .. } if permissions.contains(&permission))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessSummary {
    /// Whether policy-backed inference authorization is enabled. When false,
    /// read collections are empty and mutations are unavailable.
    pub enabled: bool,
    pub policy_epoch: u64,
    pub actor: ActorSummary,
    pub counts: AccessCounts,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActorSummary {
    pub kind: String,
    pub principal_id: Option<String>,
    pub display_name: String,
    pub permissions: Vec<ManagementPermission>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AccessCounts {
    pub users: usize,
    pub groups: usize,
    pub roles: usize,
    pub policies: usize,
    pub api_keys: usize,
    pub active_sessions: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessUser {
    pub id: String,
    pub kind: String,
    pub display_name: String,
    pub enabled: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessGroup {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub member_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessRole {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub permissions: Vec<ManagementPermission>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessPolicy {
    pub id: String,
    pub name: String,
    pub effect: String,
    pub enabled: bool,
    pub subjects: Vec<String>,
    pub endpoints: Vec<String>,
    /// Compatibility projection for older dashboard clients. New callers should
    /// use the two explicit model dimensions below.
    pub models: Vec<String>,
    pub requested_models: Vec<String>,
    pub served_models: Vec<String>,
    pub providers: Vec<String>,
    pub routes: Vec<String>,
    pub time_windows: Vec<AccessTimeWindow>,
    pub max_concurrent_sessions: Option<u32>,
    pub max_daily_session_starts: Option<u32>,
    pub management_permissions: Vec<ManagementPermission>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessTimeWindow {
    /// Bit 0 is Monday and bit 6 is Sunday. Zero means every weekday.
    #[serde(default)]
    pub weekday_mask: u8,
    #[serde(default)]
    pub start_minute: u16,
    #[serde(default)]
    pub end_minute: u16,
    pub absolute_start_ms: Option<i64>,
    pub absolute_end_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessApiKey {
    pub id: String,
    pub principal_id: String,
    pub name: String,
    pub prefix: String,
    pub enabled: bool,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub last_used_at: Option<String>,
    /// Opt-in retention of request/response bodies. Flow metadata is retained
    /// independently of this setting.
    pub capture_payloads: bool,
}

/// Only the create/rotate response carries `raw_key`; list responses cannot recover it.
#[derive(Serialize)]
pub struct CreatedAccessApiKey {
    #[serde(flatten)]
    pub api_key: AccessApiKey,
    pub raw_key: String,
}

impl std::fmt::Debug for CreatedAccessApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreatedAccessApiKey")
            .field("api_key", &self.api_key)
            .field("raw_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessSession {
    pub id: String,
    pub kind: String,
    pub principal_id: String,
    pub key_id: String,
    pub endpoint: Option<String>,
    pub requested_model: Option<String>,
    pub started_at: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessUsageRow {
    pub dimension: String,
    pub value: String,
    pub requests: u64,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub cost: Option<f64>,
    pub cost_confidence: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessAuditEvent {
    pub id: String,
    pub timestamp: String,
    pub actor: String,
    pub action: String,
    pub target: String,
    pub outcome: String,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessPricingRow {
    pub model: String,
    pub provider: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_url: Option<String>,
    pub fetched_at: String,
    pub input_per_1k: String,
    pub output_per_1k: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_min_per_1k: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_max_per_1k: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_min_per_1k: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_max_per_1k: Option<String>,
    pub confidence: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub display_name: String,
    pub kind: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub principal_id: String,
    pub name: String,
    pub expires_at: Option<String>,
    #[serde(default)]
    pub capture_payloads: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePayloadCaptureRequest {
    pub capture_payloads: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreatePolicyRequest {
    pub name: String,
    pub effect: String,
    #[serde(default)]
    pub subjects: Vec<String>,
    #[serde(default)]
    pub endpoints: Vec<String>,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub requested_models: Vec<String>,
    #[serde(default)]
    pub served_models: Vec<String>,
    #[serde(default)]
    pub providers: Vec<String>,
    #[serde(default)]
    pub routes: Vec<String>,
    #[serde(default)]
    pub time_windows: Vec<AccessTimeWindow>,
    pub max_concurrent_sessions: Option<u32>,
    pub max_daily_session_starts: Option<u32>,
    #[serde(default)]
    pub management_permissions: Vec<ManagementPermission>,
}

#[derive(Debug, Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
    #[serde(default)]
    pub members: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateRoleRequest {
    pub name: String,
    #[serde(default)]
    pub permissions: Vec<ManagementPermission>,
}

#[derive(Debug, Deserialize)]
pub struct WritePricingRequest {
    pub pricing: Vec<AccessPricingInput>,
}

#[derive(Debug, Deserialize)]
pub struct SyncPricingRequest {
    pub models: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct AccessPricingInput {
    pub model: String,
    pub provider: String,
    pub input_per_1k: String,
    pub output_per_1k: String,
}

#[derive(Debug)]
pub enum AccessOperation {
    Summary,
    ListUsers,
    CreateUser(CreateUserRequest),
    ListGroups,
    CreateGroup(CreateGroupRequest),
    ListRoles,
    CreateRole(CreateRoleRequest),
    ListPolicies,
    CreatePolicy(Box<CreatePolicyRequest>),
    ListApiKeys,
    CreateApiKey(CreateApiKeyRequest),
    RevokeApiKey(String),
    RotateApiKey(String),
    UpdateApiKeyPayloadCapture(String, bool),
    ListSessions,
    RevokeSession(String),
    Usage,
    Audit,
    Pricing,
    SyncPricing(SyncPricingRequest),
    WritePricing(WritePricingRequest),
}

#[derive(Debug)]
pub enum AccessResult {
    Summary(AccessSummary),
    Users(Vec<AccessUser>),
    Groups(Vec<AccessGroup>),
    Roles(Vec<AccessRole>),
    Policies(Vec<AccessPolicy>),
    ApiKeys(Vec<AccessApiKey>),
    CreatedApiKey(CreatedAccessApiKey),
    Sessions(Vec<AccessSession>),
    Usage(Vec<AccessUsageRow>),
    Audit(Vec<AccessAuditEvent>),
    Pricing(Vec<AccessPricingRow>),
}

pub type AccessFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AccessResult, AccessError>> + Send + 'a>>;

/// Authz/store adapter. Mutations must enforce anti-escalation and atomically publish a fresh
/// policy snapshot before their future resolves successfully.
pub trait AccessBackend: Send + Sync + 'static {
    fn dispatch<'a>(
        &'a self,
        actor: &'a ManagementActor,
        operation: AccessOperation,
    ) -> AccessFuture<'a>;
}

#[derive(Debug)]
pub struct AccessError {
    status: StatusCode,
    message: String,
}

impl AccessError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn forbidden() -> Self {
        Self::new(StatusCode::FORBIDDEN, "management permission denied")
    }

    fn contract() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "access backend contract mismatch",
        )
    }
}

impl std::fmt::Display for AccessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AccessError {}

impl IntoResponse for AccessError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

/// Routes are generic over the application's missing axum state and carry the backend as an
/// extension, so the main router only needs a narrow merge plus its existing session/CSRF layers.
pub fn routes<S>(backend: Arc<dyn AccessBackend>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/dashboard/api/auth/summary", get(summary))
        .route("/dashboard/api/auth/users", get(users).post(create_user))
        .route("/dashboard/api/auth/groups", get(groups).post(create_group))
        .route("/dashboard/api/auth/roles", get(roles).post(create_role))
        .route(
            "/dashboard/api/auth/policies",
            get(policies).post(create_policy),
        )
        .route(
            "/dashboard/api/auth/api-keys",
            get(api_keys).post(create_api_key),
        )
        .route(
            "/dashboard/api/auth/api-keys/{id}/revoke",
            post(revoke_api_key),
        )
        .route(
            "/dashboard/api/auth/api-keys/{id}/rotate",
            post(rotate_api_key),
        )
        .route(
            "/dashboard/api/auth/api-keys/{id}/payload-capture",
            post(update_api_key_payload_capture),
        )
        .route("/dashboard/api/auth/sessions", get(sessions))
        .route(
            "/dashboard/api/auth/sessions/{id}/revoke",
            post(revoke_session),
        )
        .route("/dashboard/api/auth/usage", get(usage))
        .route("/dashboard/api/auth/audit", get(audit))
        .route(
            "/dashboard/api/auth/pricing",
            get(pricing).post(write_pricing),
        )
        .route("/dashboard/api/auth/pricing/sync", post(sync_pricing))
        .layer(Extension(backend))
}

type Ctx = (
    Extension<Arc<dyn AccessBackend>>,
    Extension<ManagementActor>,
);

async fn execute(
    ctx: Ctx,
    permission: ManagementPermission,
    operation: AccessOperation,
) -> Result<AccessResult, AccessError> {
    let (Extension(backend), Extension(actor)) = ctx;
    if !actor.allows(permission) {
        return Err(AccessError::forbidden());
    }
    backend.dispatch(&actor, operation).await
}

macro_rules! read_handler {
    ($name:ident, $permission:expr, $operation:expr, $variant:ident, $key:literal) => {
        async fn $name(backend: Extension<Arc<dyn AccessBackend>>, actor: Extension<ManagementActor>) -> Result<Json<Value>, AccessError> {
            match execute((backend, actor), $permission, $operation).await? {
                AccessResult::$variant(value) => Ok(Json(json!({ $key: value }))),
                _ => Err(AccessError::contract()),
            }
        }
    };
}

async fn summary(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
) -> Result<Json<AccessSummary>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::PrincipalsRead,
        AccessOperation::Summary,
    )
    .await?
    {
        AccessResult::Summary(value) => Ok(Json(value)),
        _ => Err(AccessError::contract()),
    }
}

async fn create_group(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreateGroupRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::GroupsWrite,
        AccessOperation::CreateGroup(body),
    )
    .await?
    {
        AccessResult::Groups(value) => Ok(Json(json!({ "groups": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn create_role(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreateRoleRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::RolesWrite,
        AccessOperation::CreateRole(body),
    )
    .await?
    {
        AccessResult::Roles(value) => Ok(Json(json!({ "roles": value }))),
        _ => Err(AccessError::contract()),
    }
}

read_handler!(
    users,
    ManagementPermission::PrincipalsRead,
    AccessOperation::ListUsers,
    Users,
    "users"
);
read_handler!(
    groups,
    ManagementPermission::GroupsRead,
    AccessOperation::ListGroups,
    Groups,
    "groups"
);
read_handler!(
    roles,
    ManagementPermission::RolesRead,
    AccessOperation::ListRoles,
    Roles,
    "roles"
);
read_handler!(
    policies,
    ManagementPermission::PoliciesRead,
    AccessOperation::ListPolicies,
    Policies,
    "policies"
);
read_handler!(
    api_keys,
    ManagementPermission::KeysRead,
    AccessOperation::ListApiKeys,
    ApiKeys,
    "api_keys"
);
read_handler!(
    sessions,
    ManagementPermission::SessionsRead,
    AccessOperation::ListSessions,
    Sessions,
    "sessions"
);
read_handler!(
    usage,
    ManagementPermission::UsageRead,
    AccessOperation::Usage,
    Usage,
    "usage"
);
read_handler!(
    audit,
    ManagementPermission::AuditRead,
    AccessOperation::Audit,
    Audit,
    "events"
);
read_handler!(
    pricing,
    ManagementPermission::PricingRead,
    AccessOperation::Pricing,
    Pricing,
    "pricing"
);

async fn create_user(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreateUserRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::PrincipalsWrite,
        AccessOperation::CreateUser(body),
    )
    .await?
    {
        AccessResult::Users(value) => Ok(Json(json!({ "users": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn create_policy(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreatePolicyRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::PoliciesWrite,
        AccessOperation::CreatePolicy(Box::new(body)),
    )
    .await?
    {
        AccessResult::Policies(value) => Ok(Json(json!({ "policies": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn create_api_key(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<CreateApiKeyRequest>,
) -> Result<Json<CreatedAccessApiKey>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::KeysCreate,
        AccessOperation::CreateApiKey(body),
    )
    .await?
    {
        AccessResult::CreatedApiKey(value) => Ok(Json(value)),
        _ => Err(AccessError::contract()),
    }
}

async fn revoke_api_key(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::KeysRevoke,
        AccessOperation::RevokeApiKey(id),
    )
    .await?
    {
        AccessResult::ApiKeys(value) => Ok(Json(json!({ "api_keys": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn rotate_api_key(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Path(id): Path<String>,
) -> Result<Json<CreatedAccessApiKey>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::KeysRotate,
        AccessOperation::RotateApiKey(id),
    )
    .await?
    {
        AccessResult::CreatedApiKey(value) => Ok(Json(value)),
        _ => Err(AccessError::contract()),
    }
}

async fn update_api_key_payload_capture(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Path(id): Path<String>,
    Json(body): Json<UpdatePayloadCaptureRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::KeysCreate,
        AccessOperation::UpdateApiKeyPayloadCapture(id, body.capture_payloads),
    )
    .await?
    {
        AccessResult::ApiKeys(value) => Ok(Json(json!({ "api_keys": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn revoke_session(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::SessionsTerminate,
        AccessOperation::RevokeSession(id),
    )
    .await?
    {
        AccessResult::Sessions(value) => Ok(Json(json!({ "sessions": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn write_pricing(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<WritePricingRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::PricingWrite,
        AccessOperation::WritePricing(body),
    )
    .await?
    {
        AccessResult::Pricing(value) => Ok(Json(json!({ "pricing": value }))),
        _ => Err(AccessError::contract()),
    }
}

async fn sync_pricing(
    backend: Extension<Arc<dyn AccessBackend>>,
    actor: Extension<ManagementActor>,
    Json(body): Json<SyncPricingRequest>,
) -> Result<Json<Value>, AccessError> {
    match execute(
        (backend, actor),
        ManagementPermission::PricingSync,
        AccessOperation::SyncPricing(body),
    )
    .await?
    {
        AccessResult::Pricing(value) => Ok(Json(json!({ "pricing": value }))),
        _ => Err(AccessError::contract()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    #[derive(Default)]
    struct RouteBackend {
        dispatches: AtomicUsize,
    }

    impl AccessBackend for RouteBackend {
        fn dispatch<'a>(
            &'a self,
            actor: &'a ManagementActor,
            operation: AccessOperation,
        ) -> AccessFuture<'a> {
            self.dispatches.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move {
                match operation {
                    AccessOperation::ListApiKeys => Ok(AccessResult::ApiKeys(Vec::new())),
                    AccessOperation::Summary => Ok(AccessResult::Summary(AccessSummary {
                        enabled: true,
                        policy_epoch: 7,
                        actor: ActorSummary {
                            kind: "delegated".into(),
                            principal_id: match actor {
                                ManagementActor::Delegated { principal_id, .. } => {
                                    Some(principal_id.clone())
                                }
                                ManagementActor::Bootstrap => None,
                            },
                            display_name: "route test".into(),
                            permissions: Vec::new(),
                        },
                        counts: AccessCounts::default(),
                    })),
                    AccessOperation::CreateApiKey(body) => {
                        Ok(AccessResult::CreatedApiKey(CreatedAccessApiKey {
                            api_key: AccessApiKey {
                                id: "key_created".into(),
                                principal_id: body.principal_id,
                                name: body.name,
                                prefix: "llmc_test".into(),
                                enabled: true,
                                created_at: "now".into(),
                                expires_at: body.expires_at,
                                last_used_at: None,
                                capture_payloads: body.capture_payloads,
                            },
                            raw_key: "llmc_secret_once".into(),
                        }))
                    }
                    AccessOperation::SyncPricing(_) => Ok(AccessResult::Pricing(Vec::new())),
                    _ => Err(AccessError::contract()),
                }
            })
        }
    }

    fn delegated(permissions: impl IntoIterator<Item = ManagementPermission>) -> ManagementActor {
        ManagementActor::Delegated {
            session_id: "sess_1".into(),
            principal_id: "usr_1".into(),
            key_id: "key_1".into(),
            permissions: permissions.into_iter().collect::<Vec<_>>().into(),
        }
    }

    async fn json_body(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn created_key_debug_never_contains_secret() {
        let created = CreatedAccessApiKey {
            api_key: AccessApiKey {
                id: "key_1".into(),
                principal_id: "usr_1".into(),
                name: "test".into(),
                prefix: "llmc_abcd".into(),
                enabled: true,
                created_at: "now".into(),
                expires_at: None,
                last_used_at: None,
                capture_payloads: false,
            },
            raw_key: "llmc_extremely_secret".into(),
        };
        let debug = format!("{created:?}");
        assert!(!debug.contains("extremely_secret"));
        assert!(debug.contains("[REDACTED]"));

        let wire = serde_json::to_value(created).unwrap();
        assert_eq!(wire["id"], "key_1");
        assert_eq!(wire["raw_key"], "llmc_extremely_secret");
        assert!(wire.get("api_key").is_none());
    }

    #[test]
    fn delegated_permissions_are_exact() {
        let actor = ManagementActor::Delegated {
            session_id: "sess_1".into(),
            principal_id: "usr_1".into(),
            key_id: "key_1".into(),
            permissions: Arc::from([ManagementPermission::KeysRead]),
        };
        assert!(actor.allows(ManagementPermission::KeysRead));
        assert!(!actor.allows(ManagementPermission::KeysRevoke));
        assert!(ManagementActor::Bootstrap.allows(ManagementPermission::KeysRevoke));
    }

    #[tokio::test]
    async fn route_denies_wrong_named_permission_without_dispatch() {
        let backend = Arc::new(RouteBackend::default());
        let app = routes::<()>(backend.clone())
            .layer(Extension(delegated([ManagementPermission::UsageRead])));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard/api/auth/api-keys")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(backend.dispatches.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn route_allows_matching_named_permission() {
        let backend = Arc::new(RouteBackend::default());
        let app = routes::<()>(backend.clone())
            .layer(Extension(delegated([ManagementPermission::KeysRead])));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard/api/auth/api-keys")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(backend.dispatches.load(Ordering::Relaxed), 1);
        assert_eq!(json_body(response).await, json!({ "api_keys": [] }));
    }

    #[tokio::test]
    async fn pricing_sync_requires_exact_named_permission() {
        let request = || {
            Request::builder()
                .method(Method::POST)
                .uri("/dashboard/api/auth/pricing/sync")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"models":["openai/gpt-4"]}"#))
                .unwrap()
        };
        let denied = Arc::new(RouteBackend::default());
        let response = routes::<()>(denied.clone())
            .layer(Extension(delegated([ManagementPermission::PricingWrite])))
            .oneshot(request())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(denied.dispatches.load(Ordering::Relaxed), 0);

        let allowed = Arc::new(RouteBackend::default());
        let response = routes::<()>(allowed.clone())
            .layer(Extension(delegated([ManagementPermission::PricingSync])))
            .oneshot(request())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(allowed.dispatches.load(Ordering::Relaxed), 1);
        assert_eq!(json_body(response).await, json!({ "pricing": [] }));
    }

    #[tokio::test]
    async fn summary_requires_principals_read_permission() {
        for permissions in [Vec::new(), vec![ManagementPermission::AuditRead]] {
            let backend = Arc::new(RouteBackend::default());
            let response = routes::<()>(backend.clone())
                .layer(Extension(delegated(permissions)))
                .oneshot(
                    Request::builder()
                        .uri("/dashboard/api/auth/summary")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert_eq!(backend.dispatches.load(Ordering::Relaxed), 0);
        }

        let backend = Arc::new(RouteBackend::default());
        let response = routes::<()>(backend.clone())
            .layer(Extension(delegated([ManagementPermission::PrincipalsRead])))
            .oneshot(
                Request::builder()
                    .uri("/dashboard/api/auth/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(backend.dispatches.load(Ordering::Relaxed), 1);
        assert_eq!(json_body(response).await["policy_epoch"], 7);
    }

    #[tokio::test]
    async fn create_key_route_returns_flattened_copy_once_secret() {
        let app = routes::<()>(Arc::new(RouteBackend::default()))
            .layer(Extension(delegated([ManagementPermission::KeysCreate])));
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/dashboard/api/auth/api-keys")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"principal_id":"usr_target","name":"automation","expires_at":null}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            json_body(response).await,
            json!({
                "id": "key_created",
                "principal_id": "usr_target",
                "name": "automation",
                "prefix": "llmc_test",
                "enabled": true,
                "created_at": "now",
                "expires_at": null,
                "last_used_at": null,
                "capture_payloads": false,
                "raw_key": "llmc_secret_once"
            })
        );
    }

    #[tokio::test]
    async fn route_without_management_actor_is_rejected_before_dispatch() {
        let backend = Arc::new(RouteBackend::default());
        let response = routes::<()>(backend.clone())
            .oneshot(
                Request::builder()
                    .uri("/dashboard/api/auth/api-keys")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(backend.dispatches.load(Ordering::Relaxed), 0);
    }
}
