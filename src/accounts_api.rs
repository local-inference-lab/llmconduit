//! Dashboard account and key management routes (`/dashboard/api/me`,
//! `/dashboard/api/users*`, `/dashboard/api/keys*`).
//!
//! All routes sit behind `require_session`. Mutations additionally require the
//! double-submit CSRF token. Authorization: a session without a user (token
//! login / dev-open) is treated as admin; a user session may manage its own
//! keys; user management and other users' keys need `is_admin`.

// Helpers return the ready-to-send axum `Response` as their error so handlers
// can `return denied;` directly; boxing it would only add noise.
#![allow(clippy::result_large_err)]

use crate::accounts::{self, SessionUser};
use crate::control_plane_store::{ApiKeyRecord, PersistenceStore, UserRecord};
use crate::dashboard_auth::{AuthSession, DashboardAuth, no_store};
use crate::engine::Gateway;
use crate::openapi::DashboardError;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};

const MAX_LABEL_LEN: usize = 128;
const MAX_ALLOWED_MODELS: usize = 64;

fn json<T: Serialize>(status: StatusCode, body: &T) -> Response {
    no_store((status, Json(body)).into_response())
}

fn error(status: StatusCode, message: &str) -> Response {
    json(status, &serde_json::json!({ "error": message }))
}

/// 400 for a body the extractor refused: wrong content type, malformed JSON,
/// a missing or mistyped field, or an unknown field (the request types are
/// `deny_unknown_fields`). The extractor's text names the offending field.
fn invalid_body(expected: &str, rejection: &axum::extract::rejection::JsonRejection) -> Response {
    error(
        StatusCode::BAD_REQUEST,
        &format!(
            "invalid JSON body, expected {expected}: {}",
            rejection.body_text()
        ),
    )
}

fn store_or_503(gateway: &Gateway) -> Result<Arc<dyn PersistenceStore>, Response> {
    gateway.persistence_store().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "user and key management need a SQL store (control_plane.storage sqlite or postgres)",
        )
    })
}

fn is_admin(session: &AuthSession) -> bool {
    session
        .user
        .as_ref()
        .map_or_else(|| session.bootstrap_admin(), |user| user.is_admin)
}

fn actor(session: &AuthSession) -> String {
    if let Some(user) = session.user.as_ref() {
        user.username.clone()
    } else if session.bootstrap_admin() {
        "dashboard-token".to_string()
    } else {
        "delegated-dashboard-session".to_string()
    }
}

fn require_admin(session: &AuthSession) -> Result<(), Response> {
    if is_admin(session) {
        Ok(())
    } else {
        Err(error(StatusCode::FORBIDDEN, "administrator role required"))
    }
}

fn require_csrf(auth: &DashboardAuth, headers: &HeaderMap) -> Result<(), Response> {
    if auth.verify_csrf(headers) {
        Ok(())
    } else {
        Err(error(
            StatusCode::FORBIDDEN,
            "missing or invalid CSRF token",
        ))
    }
}

// ---------------------------------------------------------------------------
// /me
// ---------------------------------------------------------------------------

/// `GET /dashboard/api/me` body.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct MeBody {
    /// `null` for a token/dev-open session.
    user: Option<SessionUser>,
    /// `true` for an admin user, or for an explicit bootstrap session
    /// (token login / dev-open). Delegated key sessions are not bootstrap admins.
    is_admin: bool,
    /// `users` when at least one account exists, `token` when only the env
    /// token gates the dashboard, `open` when nothing does (loopback dev).
    auth_mode: &'static str,
    /// Whether users/keys can be managed (a SQL store is configured).
    accounts_enabled: bool,
}

/// `GET /dashboard/api/me`
#[utoipa::path(
    get,
    path = "/dashboard/api/me",
    tag = "accounts",
    operation_id = "me",
    responses(
        (status = 200, description = "The calling session: its user (`null` for a token/dev-open session), whether it counts as admin, the dashboard auth mode, and whether accounts can be managed.", body = MeBody),
        (status = 401, description = "No valid session (missing/expired/invalid `llmconduit_session` cookie and no matching bearer token). Plain text `unauthorized`, `Cache-Control: no-store`.", content_type = "text/plain", body = String),
    )
)]
pub async fn me(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
) -> Response {
    let body = MeBody {
        is_admin: is_admin(&session),
        user: session.user,
        auth_mode: auth_mode(&gateway, &auth).await,
        accounts_enabled: gateway.persistence_store().is_some(),
    };
    json(StatusCode::OK, &body)
}

/// The dashboard's authentication mode, for the login shell and `/me`. Users
/// created out of band (the CLI, another replica) are noticed here: while the
/// in-memory flag is off, a cheap `count_users` refreshes it.
pub async fn auth_mode(gateway: &Gateway, auth: &DashboardAuth) -> &'static str {
    if auth.github_sso_enabled() {
        return "github";
    }
    if !gateway.users_configured()
        && let Some(store) = gateway.persistence_store()
        && let Ok(count) = store.count_users().await
        && count > 0
    {
        gateway.set_users_configured(true);
    }
    if gateway.users_configured() {
        "users"
    } else if auth.dev_open() {
        "open"
    } else {
        "token"
    }
}

// ---------------------------------------------------------------------------
// /users
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateUserRequest {
    /// Validated before use; an invalid username is rejected with 400.
    pub username: String,
    /// Stored as an Argon2id hash; a password the hasher rejects is a 400.
    pub password: String,
    /// Grant the administrator role (default `false`).
    #[serde(default)]
    pub is_admin: bool,
}

/// Both fields optional; omit a field to leave it unchanged.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateUserRequest {
    /// New password (stored as an Argon2id hash).
    #[serde(default)]
    pub password: Option<String>,
    /// New administrator flag. An admin cannot set `false` on themselves.
    #[serde(default)]
    pub is_admin: Option<bool>,
}

/// `GET /dashboard/api/users` body.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct UsersBody {
    users: Vec<UserRecord>,
}

/// `PATCH /dashboard/api/users/{id}` success body (documentation of the
/// handler's inline JSON; the handler itself builds it with `json!`).
#[derive(ToSchema)]
#[allow(dead_code)]
pub(crate) struct UserUpdatedBody {
    /// The `{id}` path parameter, echoed.
    id: String,
    /// Always `true`.
    updated: bool,
}

/// `DELETE /dashboard/api/users/{id}` success body (documentation of the
/// handler's inline JSON; the handler itself builds it with `json!`).
#[derive(ToSchema)]
#[allow(dead_code)]
pub(crate) struct UserDeletedBody {
    /// The `{id}` path parameter, echoed.
    id: String,
    /// Always `true`.
    deleted: bool,
    /// Number of the user's API keys that were revoked along with the account.
    keys_revoked: usize,
}

/// `GET /dashboard/api/users` (admin)
#[utoipa::path(
    get,
    path = "/dashboard/api/users",
    tag = "accounts",
    operation_id = "list_users",
    responses(
        (status = 200, description = "All (non-deleted) user accounts.", body = UsersBody),
        (status = 401, description = "No valid session. Plain text `unauthorized`, `Cache-Control: no-store`.", content_type = "text/plain", body = String),
        (status = 403, description = "The session user is not an administrator (`administrator role required`). Sessions without a user (token login / dev-open) count as admin.", body = DashboardError),
        (status = 500, description = "SQL store failure (`list users: …`).", body = DashboardError),
        (status = 503, description = "No SQL store configured (`control_plane.storage` sqlite or postgres); user and key management is unavailable.", body = DashboardError),
    )
)]
pub async fn list_users(State(gateway): State<Arc<Gateway>>, session: AuthSession) -> Response {
    if let Err(denied) = require_admin(&session) {
        return denied;
    }
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    match store.list_users().await {
        Ok(users) => json(StatusCode::OK, &UsersBody { users }),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("list users: {err}"),
        ),
    }
}

/// `POST /dashboard/api/users` (admin, CSRF)
#[utoipa::path(
    post,
    path = "/dashboard/api/users",
    tag = "accounts",
    operation_id = "create_user",
    params(
        ("x-csrf-token" = String, Header, description = "Double-submit CSRF token: must equal the `llmconduit_csrf` cookie (issued by `POST /dashboard/login`, refreshed by `GET /dashboard`). Missing, empty, or mismatched → 403."),
    ),
    request_body(content = CreateUserRequest, description = "`{username, password, is_admin?}`"),
    responses(
        (status = 201, description = "The created user.", body = UserRecord),
        (status = 400, description = "Body is not `{username, password, is_admin?}` (`invalid JSON body, expected …: <extractor text>`, which names an unknown or mistyped field; the body is strict), the username fails validation, or the password cannot be hashed (message from the validator).", body = DashboardError),
        (status = 401, description = "No valid session. Plain text `unauthorized`, `Cache-Control: no-store`.", content_type = "text/plain", body = String),
        (status = 403, description = "`administrator role required` (checked first), or `missing or invalid CSRF token`.", body = DashboardError),
        (status = 409, description = "`username already exists`.", body = DashboardError),
        (status = 500, description = "SQL store failure (`lookup user: …` / `create user: …`).", body = DashboardError),
        (status = 503, description = "No SQL store configured; user and key management is unavailable.", body = DashboardError),
    )
)]
pub async fn create_user(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    headers: HeaderMap,
    payload: Result<Json<CreateUserRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(denied) = require_admin(&session).and_then(|()| require_csrf(&auth, &headers)) {
        return denied;
    }
    let body = match payload {
        Ok(Json(body)) => body,
        Err(rejection) => return invalid_body("{username, password, is_admin?}", &rejection),
    };
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    let username = match accounts::validate_username(&body.username) {
        Ok(username) => username.to_string(),
        Err(err) => return error(StatusCode::BAD_REQUEST, &err),
    };
    let hash = match accounts::hash_password(&body.password) {
        Ok(hash) => hash,
        Err(err) => return error(StatusCode::BAD_REQUEST, &err),
    };
    match store.get_user_auth(&username).await {
        Ok(Some(_)) => return error(StatusCode::CONFLICT, "username already exists"),
        Ok(None) => {}
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("lookup user: {err}"),
            );
        }
    }
    match store
        .create_user(&username, &hash, body.is_admin, &actor(&session))
        .await
    {
        Ok(user) => {
            gateway.set_users_configured(true);
            json(StatusCode::CREATED, &user)
        }
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("create user: {err}"),
        ),
    }
}

/// `PATCH /dashboard/api/users/{id}` (admin, CSRF): reset password and/or role.
#[utoipa::path(
    patch,
    path = "/dashboard/api/users/{id}",
    tag = "accounts",
    operation_id = "update_user",
    params(
        ("id" = String, Path, description = "User id (`UserRecord.id`)."),
        ("x-csrf-token" = String, Header, description = "Double-submit CSRF token: must equal the `llmconduit_csrf` cookie. Missing, empty, or mismatched → 403."),
    ),
    request_body(content = UpdateUserRequest, description = "`{password?, is_admin?}` — reset the password and/or change the role."),
    responses(
        (status = 200, description = "Updated.", body = UserUpdatedBody),
        (status = 400, description = "Body is not `{password?, is_admin?}` (`invalid JSON body, expected …: <extractor text>`, which names an unknown or mistyped field; the body is strict), `you cannot remove your own administrator role` (`is_admin: false` on the caller's own account), or the new password cannot be hashed.", body = DashboardError),
        (status = 401, description = "No valid session. Plain text `unauthorized`, `Cache-Control: no-store`.", content_type = "text/plain", body = String),
        (status = 403, description = "`administrator role required` (checked first), or `missing or invalid CSRF token`.", body = DashboardError),
        (status = 404, description = "`user not found`.", body = DashboardError),
        (status = 500, description = "SQL store failure (`lookup user: …` / `update user: …`).", body = DashboardError),
        (status = 503, description = "No SQL store configured; user and key management is unavailable.", body = DashboardError),
    )
)]
pub async fn update_user(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<UpdateUserRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(denied) = require_admin(&session).and_then(|()| require_csrf(&auth, &headers)) {
        return denied;
    }
    let body = match payload {
        Ok(Json(body)) => body,
        Err(rejection) => return invalid_body("{password?, is_admin?}", &rejection),
    };
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    match store.get_user(&id).await {
        Ok(Some(_)) => {}
        Ok(None) => return error(StatusCode::NOT_FOUND, "user not found"),
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("lookup user: {err}"),
            );
        }
    }
    // An admin cannot demote themselves (no lock-out through the UI).
    if body.is_admin == Some(false) && session.user.as_ref().is_some_and(|user| user.id == id) {
        return error(
            StatusCode::BAD_REQUEST,
            "you cannot remove your own administrator role",
        );
    }
    let hash = match body.password.as_deref() {
        Some(password) => match accounts::hash_password(password) {
            Ok(hash) => Some(hash),
            Err(err) => return error(StatusCode::BAD_REQUEST, &err),
        },
        None => None,
    };
    match store
        .update_user(&id, hash.as_deref(), body.is_admin, &actor(&session))
        .await
    {
        Ok(()) => json(
            StatusCode::OK,
            &serde_json::json!({ "id": id, "updated": true }),
        ),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("update user: {err}"),
        ),
    }
}

/// `DELETE /dashboard/api/users/{id}` (admin, CSRF): soft-deletes the user and
/// revokes their keys (the registry reloads immediately).
#[utoipa::path(
    delete,
    path = "/dashboard/api/users/{id}",
    tag = "accounts",
    operation_id = "delete_user",
    params(
        ("id" = String, Path, description = "User id (`UserRecord.id`)."),
        ("x-csrf-token" = String, Header, description = "Double-submit CSRF token: must equal the `llmconduit_csrf` cookie. Missing, empty, or mismatched → 403."),
    ),
    responses(
        (status = 200, description = "The user is soft-deleted and every one of their API keys revoked; the live key registry is reloaded (a reload failure is only logged).", body = UserDeletedBody),
        (status = 400, description = "`you cannot delete your own account`.", body = DashboardError),
        (status = 401, description = "No valid session. Plain text `unauthorized`, `Cache-Control: no-store`.", content_type = "text/plain", body = String),
        (status = 403, description = "`administrator role required` (checked first), or `missing or invalid CSRF token`.", body = DashboardError),
        (status = 404, description = "`user not found`.", body = DashboardError),
        (status = 500, description = "SQL store failure (`lookup user: …` / `list keys: …` / `revoke key: …` / `delete user: …`).", body = DashboardError),
        (status = 503, description = "No SQL store configured; user and key management is unavailable.", body = DashboardError),
    )
)]
pub async fn delete_user(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(denied) = require_admin(&session).and_then(|()| require_csrf(&auth, &headers)) {
        return denied;
    }
    if session.user.as_ref().is_some_and(|user| user.id == id) {
        return error(
            StatusCode::BAD_REQUEST,
            "you cannot delete your own account",
        );
    }
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    match store.get_user(&id).await {
        Ok(Some(_)) => {}
        Ok(None) => return error(StatusCode::NOT_FOUND, "user not found"),
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("lookup user: {err}"),
            );
        }
    }
    let who = actor(&session);
    let keys = match store.list_api_keys_for_user(&id).await {
        Ok(keys) => keys,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("list keys: {err}"),
            );
        }
    };
    for key in &keys {
        if let Err(err) = store.delete_api_key(&key.id, &who).await {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("revoke key: {err}"),
            );
        }
    }
    if let Err(err) = store.delete_user(&id, &who).await {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("delete user: {err}"),
        );
    }
    if let Err(err) = accounts::reload_client_keys(&gateway).await {
        tracing::warn!(error = %err, "key registry reload after user delete failed");
    }
    json(
        StatusCode::OK,
        &serde_json::json!({ "id": id, "deleted": true, "keys_revoked": keys.len() }),
    )
}

// ---------------------------------------------------------------------------
// /keys
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct KeysQuery {
    /// Admin only: another user's id, or `all` for every key. A non-admin user
    /// may only pass their own id.
    pub user_id: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateKeyRequest {
    /// Human label; trimmed, blank → none, truncated to 128 characters.
    #[serde(default)]
    pub label: Option<String>,
    /// Client-facing model/alias names this key may request; empty = any.
    /// Entries are trimmed, blanks dropped, at most 64 kept.
    #[serde(default)]
    pub allowed_models: Vec<String>,
    /// Admin only: create the key for another user (default: the caller).
    #[serde(default)]
    pub user_id: Option<String>,
}

/// `GET /dashboard/api/keys` body.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct KeysBody {
    keys: Vec<ApiKeyRecord>,
}

/// `POST /dashboard/api/keys` body.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CreatedKeyBody {
    key: ApiKeyRecord,
    /// The plaintext key (`llmc_…`), shown exactly once; only its digest is stored.
    secret: String,
    /// Number of keys in the live client-auth registry after the reload.
    registered_keys: usize,
}

/// `DELETE /dashboard/api/keys/{id}` success body (documentation of the
/// handler's inline JSON; the handler itself builds it with `json!`).
#[derive(ToSchema)]
#[allow(dead_code)]
pub(crate) struct KeyRevokedBody {
    /// The `{id}` path parameter, echoed.
    id: String,
    /// Always `true`.
    revoked: bool,
    /// Number of keys in the live client-auth registry after the reload
    /// (`0` when the reload failed; the failure is only logged).
    registered_keys: usize,
}

/// `GET /dashboard/api/keys[?user_id=<id>|all]`
#[utoipa::path(
    get,
    path = "/dashboard/api/keys",
    tag = "accounts",
    operation_id = "list_keys",
    params(KeysQuery),
    responses(
        (status = 200, description = "Key metadata (never the secret). Scope: a user session without `user_id` → its own keys; an admin user or a token/dev-open session with `user_id=<id>` → that user's keys, with `user_id=all` → every key; a token/dev-open session without `user_id` → every key.", body = KeysBody),
        (status = 400, description = "Unknown or mistyped query parameter (the query string is strict; the text names the field). Plain text from the extractor.", content_type = "text/plain", body = String),
        (status = 401, description = "No valid session. Plain text `unauthorized`, `Cache-Control: no-store`.", content_type = "text/plain", body = String),
        (status = 403, description = "`administrator role required`: a non-admin user passed a `user_id` other than their own (including `all`).", body = DashboardError),
        (status = 500, description = "SQL store failure (`list keys: …`).", body = DashboardError),
        (status = 503, description = "No SQL store configured; user and key management is unavailable.", body = DashboardError),
    )
)]
pub async fn list_keys(
    State(gateway): State<Arc<Gateway>>,
    session: AuthSession,
    Query(query): Query<KeysQuery>,
) -> Response {
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    let scope = match (&session.user, query.user_id.as_deref()) {
        (Some(user), None) => Some(user.id.clone()),
        (Some(user), Some(requested)) if !user.is_admin => {
            if requested != user.id {
                return error(StatusCode::FORBIDDEN, "administrator role required");
            }
            Some(user.id.clone())
        }
        (_, Some("all")) | (None, None) => None,
        (_, Some(requested)) => Some(requested.to_string()),
    };
    let result = match scope {
        Some(user_id) => store.list_api_keys_for_user(&user_id).await,
        None => store.list_api_keys().await,
    };
    match result {
        Ok(keys) => json(StatusCode::OK, &KeysBody { keys }),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("list keys: {err}"),
        ),
    }
}

/// `POST /dashboard/api/keys` (CSRF): mints a key, stores its digest, reloads
/// the live registry, and returns the plaintext once.
#[utoipa::path(
    post,
    path = "/dashboard/api/keys",
    tag = "accounts",
    operation_id = "create_key",
    params(
        ("x-csrf-token" = String, Header, description = "Double-submit CSRF token: must equal the `llmconduit_csrf` cookie. Missing, empty, or mismatched → 403."),
    ),
    request_body(content = Option<CreateKeyRequest>, description = "`{label?, allowed_models?, user_id?}`. Optional: a missing or unparsable JSON body is treated as `{}` (no label, any model, owned by the caller)."),
    responses(
        (status = 201, description = "The key was stored and the live registry reloaded; `secret` is the only time the plaintext is shown. Owner: the session user (an admin may name another user via `user_id`); a token/dev-open session owns nothing unless it passes `user_id`.", body = CreatedKeyBody),
        (status = 400, description = "Body is not `{label?, allowed_models?, user_id?}`: wrong content type, malformed JSON, a mistyped field or an unknown field (the body is strict); `invalid JSON body, expected …: <extractor text naming the field>`.", body = DashboardError),
        (status = 401, description = "No valid session. Plain text `unauthorized`, `Cache-Control: no-store`.", content_type = "text/plain", body = String),
        (status = 403, description = "`missing or invalid CSRF token` (checked first), or `administrator role required` (a non-admin user passed another user's `user_id`).", body = DashboardError),
        (status = 404, description = "`user not found` (the `user_id` does not exist).", body = DashboardError),
        (status = 500, description = "SQL store failure (`lookup user: …` / `store key: …`), or `key stored but not activated: …` when the registry reload failed after storing the digest (the key becomes live on the next successful reload).", body = DashboardError),
        (status = 503, description = "No SQL store configured; user and key management is unavailable.", body = DashboardError),
    )
)]
pub async fn create_key(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    headers: HeaderMap,
    payload: Result<Json<CreateKeyRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(denied) = require_csrf(&auth, &headers) {
        return denied;
    }
    let body = match payload {
        Ok(Json(body)) => body,
        Err(rejection) => {
            return invalid_body("{label?, allowed_models?, user_id?}", &rejection);
        }
    };
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    let owner = match (&session.user, body.user_id.as_deref()) {
        (Some(user), Some(requested)) if requested != user.id => {
            if !user.is_admin {
                return error(StatusCode::FORBIDDEN, "administrator role required");
            }
            Some(requested.to_string())
        }
        (Some(user), _) => Some(user.id.clone()),
        (None, requested) => requested.map(str::to_string),
    };
    if let Some(owner) = owner.as_deref() {
        match store.get_user(owner).await {
            Ok(Some(_)) => {}
            Ok(None) => return error(StatusCode::NOT_FOUND, "user not found"),
            Err(err) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("lookup user: {err}"),
                );
            }
        }
    }
    let label = body
        .label
        .as_deref()
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .map(|label| label.chars().take(MAX_LABEL_LEN).collect::<String>());
    let allowed: Vec<String> = body
        .allowed_models
        .iter()
        .map(|model| model.trim().to_string())
        .filter(|model| !model.is_empty())
        .take(MAX_ALLOWED_MODELS)
        .collect();
    let secret = accounts::generate_api_key();
    let id = uuid::Uuid::new_v4().to_string();
    let record = match store
        .put_api_key(
            &id,
            &secret,
            label.as_deref(),
            owner.as_deref(),
            &allowed,
            &actor(&session),
        )
        .await
    {
        Ok(record) => record,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("store key: {err}"),
            );
        }
    };
    let registered_keys = match accounts::reload_client_keys(&gateway).await {
        Ok(count) => count,
        Err(err) => {
            // The digest is stored; the registry will pick it up on the next
            // successful reload. Report it rather than hand out a dead key.
            tracing::warn!(error = %err, "key registry reload after key create failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("key stored but not activated: {err}"),
            );
        }
    };
    json(
        StatusCode::CREATED,
        &CreatedKeyBody {
            key: record,
            secret,
            registered_keys,
        },
    )
}

/// `DELETE /dashboard/api/keys/{id}` (CSRF): revokes a key (own, or any as admin).
#[utoipa::path(
    delete,
    path = "/dashboard/api/keys/{id}",
    tag = "accounts",
    operation_id = "delete_key",
    params(
        ("id" = String, Path, description = "Key id (`ApiKeyRecord.id`)."),
        ("x-csrf-token" = String, Header, description = "Double-submit CSRF token: must equal the `llmconduit_csrf` cookie. Missing, empty, or mismatched → 403."),
    ),
    responses(
        (status = 200, description = "Revoked; the live registry was reloaded (a reload failure is only logged and reports `registered_keys: 0`).", body = KeyRevokedBody),
        (status = 401, description = "No valid session. Plain text `unauthorized`, `Cache-Control: no-store`.", content_type = "text/plain", body = String),
        (status = 403, description = "`missing or invalid CSRF token` (checked first), or `not your key` (a non-admin user revoking a key they do not own).", body = DashboardError),
        (status = 404, description = "`key not found`.", body = DashboardError),
        (status = 500, description = "SQL store failure (`list keys: …` / `revoke key: …`).", body = DashboardError),
        (status = 503, description = "No SQL store configured; user and key management is unavailable.", body = DashboardError),
    )
)]
pub async fn delete_key(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: AuthSession,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(denied) = require_csrf(&auth, &headers) {
        return denied;
    }
    let store = match store_or_503(&gateway) {
        Ok(store) => store,
        Err(response) => return response,
    };
    let keys = match store.list_api_keys().await {
        Ok(keys) => keys,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("list keys: {err}"),
            );
        }
    };
    let Some(key) = keys.into_iter().find(|key| key.id == id) else {
        return error(StatusCode::NOT_FOUND, "key not found");
    };
    if let Some(user) = &session.user
        && !user.is_admin
        && key.user_id.as_deref() != Some(user.id.as_str())
    {
        return error(StatusCode::FORBIDDEN, "not your key");
    }
    if let Err(err) = store.delete_api_key(&id, &actor(&session)).await {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("revoke key: {err}"),
        );
    }
    let registered_keys = accounts::reload_client_keys(&gateway)
        .await
        .unwrap_or_else(|err| {
            tracing::warn!(error = %err, "key registry reload after key revoke failed");
            0
        });
    json(
        StatusCode::OK,
        &serde_json::json!({ "id": id, "revoked": true, "registered_keys": registered_keys }),
    )
}
