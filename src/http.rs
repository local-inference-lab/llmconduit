use crate::adapters::anthropic_to_responses;
use crate::adapters::chat_completions;
use crate::adapters::chat_completions::ChatCompletionCollector;
use crate::adapters::chat_completions::ChatCompletionStreamConverter;
use crate::adapters::responses_to_anthropic::AnthropicStreamCollector;
use crate::adapters::responses_to_anthropic::AnthropicStreamConverter;
use crate::adapters::responses_to_chat;
use crate::dashboard_api::dashboard_catalog;
use crate::dashboard_api::dashboard_flow_detail;
use crate::dashboard_api::dashboard_flows;
use crate::dashboard_api::dashboard_metrics;
use crate::dashboard_api::dashboard_snapshot;
use crate::dashboard_api::dashboard_topology;
use crate::dashboard_auth::DashboardAuth;
use crate::dashboard_auth::MutationDenied;
use crate::dashboard_auth::MutationPolicy;
use crate::dashboard_auth::dashboard_login;
use crate::dashboard_auth::dashboard_logout;
use crate::dashboard_auth::require_session;
use crate::dashboard_ui::dashboard_asset;
use crate::dashboard_ui::dashboard_index;
use crate::dashboard_ws::dashboard_ws;
use crate::debug_ui::debug_app_js;
use crate::debug_ui::debug_index;
use crate::debug_ui::debug_ws;
use crate::engine::Gateway;
use crate::error::AppError;
use crate::error::AppResult;
use crate::models::anthropic::AnthropicRequest;
use crate::models::anthropic::AnthropicThinking;
use crate::models::chat::ChatCompletionRequest;
use crate::models::chat::normalize_stop;
use crate::models::responses::ResponsesRequest;
use crate::proxy_headers::header_name_eq;
use crate::proxy_headers::is_hop_by_hop_header;
use crate::upstream::BackendChatRequest;
use crate::upstream::collect_models_response;
use axum::Extension;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderName;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::Sse;
use axum::routing::MethodFilter;
use axum::routing::get;
use axum::routing::on;
use axum::routing::post;
use futures::StreamExt;
use http_body::Frame;
use http_body::SizeHint;
use serde::Deserialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

const API_LOG_PAYLOAD_DUMP_LIMIT_BYTES: usize = 16 * 1024;
const API_LOG_PREVIEW_CHARS: usize = 160;
/// Fable review (Finding 1): inbound bodies at or below this size have their
/// turn-capture redaction done INLINE; a larger body moves the parse+redact+
/// re-serialize onto the blocking pool so a multi-MB Claude Code (1M-context)
/// request never stalls the tokio worker (which the synchronous path did — 100ms+
/// plus a 3–6× transient allocation spike per concurrent request). Set to the SAME
/// 16 KiB as the journal dump gate ([`API_LOG_PAYLOAD_DUMP_LIMIT_BYTES`]).
const TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES: usize = API_LOG_PAYLOAD_DUMP_LIMIT_BYTES;
const UNKNOWN_MODEL_CREATED_AT: &str = "1970-01-01T00:00:00Z";

#[derive(Debug, Clone, Copy, Default)]
pub struct RouterOptions {
    pub with_debug_ui: bool,
    /// D7 startup gate: whether the protected `/debug` + `/dashboard` routes may
    /// be registered. `false` when the bind/secret configuration refuses them
    /// (e.g. non-loopback without a token + https origin). Independent of
    /// `with_debug_ui` so an operator sees a clear "refused to register" startup
    /// log rather than a silent 404. Only consulted when `with_debug_ui` is set.
    pub register_protected_routes: bool,
}

pub fn build_router(gateway: Arc<Gateway>, options: RouterOptions) -> Router {
    // Read before `gateway` is moved into `.with_state(...)` below. Replaces
    // axum's stock 2 MiB `DefaultBodyLimit` with the configured cap (default
    // 10 MiB) so oversized inbound bodies are the operator's choice, not a
    // silent framework default.
    let max_request_body_bytes = gateway.config().max_request_body_bytes;
    let router = Router::new()
        .route("/v1/responses", post(post_responses))
        .route("/v1/messages", post(post_messages))
        .route("/v1/messages/count_tokens", post(post_count_tokens))
        .route("/v1/messages", on(MethodFilter::HEAD, probe_messages))
        .route("/v1/messages", on(MethodFilter::OPTIONS, probe_messages))
        .route("/v1/chat/completions", post(post_chat_completions))
        .route("/v1/completions", post(post_completions))
        .route("/v1/models", get(get_models))
        .route("/health", get(get_health))
        .route("/", get(get_root));

    // D7: the debug UI + dashboard routes register only when `--with-debug-ui`
    // is set AND the startup decision permits it AND the env-built auth context
    // exists. All three hold or none of the protected routes appear (production
    // untouched; a misconfigured non-loopback server refuses rather than serving
    // transcripts/credentials in the clear).
    let router = match (
        options.with_debug_ui && options.register_protected_routes,
        gateway.dashboard_auth(),
    ) {
        (true, Some(auth)) => router.merge(protected_routes(auth)),
        _ => router,
    };

    router
        .fallback(api_not_found)
        // `log_api_call` enforces the inbound body cap as a HARD memory bound for
        // EVERY route (Content-Length precheck + capped buffered read), so an
        // oversized upload is rejected with 413 before it can be buffered. The
        // `DefaultBodyLimit` makes the POST handlers' JSON/Bytes extractors agree
        // on that same ceiling instead of axum's stock 2 MiB default. Both read
        // the single configured value (`max_request_body_bytes`, which the
        // middleware re-reads from the same gateway config) — there is no second,
        // larger hidden limit. The middleware state stays `Arc<Gateway>` because
        // `log_api_call` also opens the dashboard flow record from it.
        .layer(middleware::from_fn_with_state(
            Arc::clone(&gateway),
            log_api_call,
        ))
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .with_state(gateway)
}

/// The D7-gated `/debug` + `/dashboard` sub-router (state `Arc<Gateway>`, merged
/// into the main router so it shares the outer `.with_state`).
///
/// Auth topology:
/// - `/dashboard/login` + `/dashboard/logout` — read the auth `Extension` (so
///   the handlers can sign/clear cookies) but are NOT behind `require_session`
///   (login is how you authenticate; logout must work for any state).
/// - `/dashboard` — auth `Extension` only; the shell handler serves the login
///   page vs. the SPA from `Option<AuthSession>` itself (no 401).
/// - `/dashboard/assets/{*path}` — public sub-resources (hashed, immutable); the
///   SPA shell behind them is already gated, and the asset bytes carry no
///   secrets.
/// - `/debug` + `/debug/app.js` — behind `require_session` (401 when unauthed).
/// - `/debug/ws` — self-gated inside the handler (cookie + `Origin` + `exp`); it
///   needs to OWN the rejection so the WS `Origin` check is authoritative.
///
/// The shared `Arc<DashboardAuth>` is attached as a request `Extension` scoped to
/// this sub-router so the middleware/handlers/extractors can read it
/// (`/debug/ws` reads it via `gateway.dashboard_auth()` instead).
fn protected_routes(auth: Arc<DashboardAuth>) -> Router<Arc<Gateway>> {
    // D13 `/dashboard/api/*` REST surface. `no-store` + the dashboard security
    // headers are applied as ROUTE-LEVEL response middleware on the WHOLE api router
    // (D13 R1 MED), so EVERY response carries them — including an axum EXTRACTOR
    // rejection (an invalid `page`/`limit`/`at` query → a bare `400` produced before
    // any handler runs), which previously escaped the per-handler stamping. The
    // handlers' own `json_no_store` re-stamp is idempotent (same static header
    // values), so double-application is harmless. `require_session` is layered
    // OUTSIDE the response map (added last → outermost) so it 401's an unauthed
    // caller BEFORE any handler/extractor work AND its 401 also flows back through
    // the `no_store` map.
    let api_routes = Router::new()
        .route("/dashboard/api/flows", get(dashboard_flows))
        .route("/dashboard/api/flows/{id}", get(dashboard_flow_detail))
        .route("/dashboard/api/flows/{id}/kill", post(dashboard_flow_kill))
        .route("/dashboard/api/metrics", get(dashboard_metrics))
        .route("/dashboard/api/topology", get(dashboard_topology))
        .route("/dashboard/api/catalog", get(dashboard_catalog))
        .route("/dashboard/api/snapshot", get(dashboard_snapshot))
        .route_layer(middleware::map_response(dashboard_api_no_store));

    // The `/debug` HTML/JS endpoints share the same session gate but stamp their own
    // headers in-handler (they serve HTML, not the JSON `no-store` set), so they are
    // NOT under the api router's `no_store` response map.
    let debug_routes = Router::new()
        .route("/debug", get(debug_index))
        .route("/debug/app.js", get(debug_app_js));

    // Both groups require a valid session (401 when missing/expired/invalid): every
    // dashboard read AND the kill mutation is 401'd for an unauthenticated caller
    // BEFORE any handler work (the kill's CSRF/mutation gate runs only for an
    // authenticated request).
    let session_gated = api_routes
        .merge(debug_routes)
        .route_layer(middleware::from_fn(require_session));

    // Routes that read the auth context but manage their own access decision,
    // plus the self-gated WS and the public hashed assets.
    //
    // D7b: the dashboard data socket is a separate `/dashboard/ws` route carrying
    // the batched `DashboardFrame` envelope (Monitor/Usage/FlowStatus/MetricTick/
    // TopologyUpdate). Like `/debug/ws` it is SELF-gated inside the handler (cookie
    // + `Origin` allow-list + cookie-`exp` close, via D7a's `authenticate_ws`), so
    // it OWNS its rejection and the WS `Origin` check stays authoritative.
    let open = Router::new()
        .route("/dashboard", get(dashboard_index))
        .route("/dashboard/login", post(dashboard_login))
        .route("/dashboard/logout", post(dashboard_logout))
        .route("/debug/ws", get(debug_ws))
        .route("/dashboard/ws", get(dashboard_ws))
        .route("/dashboard/assets/{*path}", get(dashboard_asset));

    session_gated
        .merge(open)
        // Scope the auth context to ONLY the protected routes (not `/v1/*`).
        .layer(Extension(auth))
}

/// Route-level response middleware (D13 R1 MED): stamp `no-store` + the dashboard
/// security header set on EVERY `/dashboard/api/*` response, including an axum
/// extractor-rejection `400` produced before any handler runs (an invalid
/// `page`/`limit`/`at` query). Delegates to the single header authority
/// [`crate::dashboard_auth::no_store`]; the handlers' own `json_no_store` re-stamps
/// the same static values, so applying this on top is idempotent.
async fn dashboard_api_no_store(response: Response) -> Response {
    crate::dashboard_auth::no_store(response)
}

/// D6 — the outcome of a `POST /dashboard/api/flows/:id/kill` attempt, decoupled from
/// axum so the policy + abort logic is unit-testable against a MOCK [`MutationPolicy`]
/// (the spec's "compiles + tests against a mocked auth/CSRF gate"). The axum handler is
/// the only place that maps this to an HTTP status.
#[derive(Debug, PartialEq, Eq)]
pub enum FlowKillOutcome {
    /// A live token was found and cancelled → `200 OK`.
    Killed,
    /// No live flow for that `api_call_id` (unknown OR already finished) → `404`.
    NotFound,
    /// The mutation policy refused (mutations disabled, or CSRF missing/invalid) → the
    /// `MutationDenied` status (`403`). Carries the reason so the body can be precise.
    Denied(MutationDenied),
}

/// D6 — the pure kill core: authorize the mutation, then cancel the flow. Separated
/// from the axum handler so tests drive it with a mock `MutationPolicy` + a real
/// `Gateway` (no HTTP stack). CSRF/mutation gating runs FIRST (a refused mutation must
/// not even probe whether the id is live — no existence oracle for an unauthorized
/// caller); only an authorized request consults the AbortHub. `gateway.abort` is
/// idempotent, so a double-kill of a still-live flow simply re-cancels an
/// already-cancelled token (`true` both times until the guard removes it), and a kill
/// of a finished/unknown flow is `false` → 404.
pub fn flow_kill_outcome(
    policy: &dyn MutationPolicy,
    headers: &HeaderMap,
    gateway: &Gateway,
    api_call_id: &str,
) -> FlowKillOutcome {
    if let Err(denied) = policy.authorize_mutation(headers) {
        return FlowKillOutcome::Denied(denied);
    }
    if gateway.abort(api_call_id) {
        FlowKillOutcome::Killed
    } else {
        FlowKillOutcome::NotFound
    }
}

/// D6 — the `POST /dashboard/api/flows/:id/kill` handler. `:id` IS the flow's
/// `api_call_id` (the AbortHub key == the route param, no rekeying — spec decision), so
/// it cancels the live server-side stream: a `200` flips the flow's `CancellationToken`
/// and the engine's compose-with-`tx.closed()` sites surface `AppError::cancelled()`
/// (499) to the client while the L1 guard finalizes the record `Cancelled`; a `404`
/// means no live flow. The mutation+CSRF gate (`LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS` +
/// a double-submit CSRF token) is enforced via the shared `DashboardAuth`
/// [`MutationPolicy`] BEFORE any abort. Behind `require_session` when registered (D13),
/// so an unauthenticated caller is already 401'd before reaching here.
///
/// REGISTRATION is D13's job (this is the only mutation route in the phase; replay is
/// deferred). The handler is provided here so D6 ships the kill behavior + tests
/// independent of D13's route table (breaking the D6↔D13 cycle).
pub async fn dashboard_flow_kill(
    State(gateway): State<Arc<Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    Path(api_call_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let response = match flow_kill_outcome(auth.as_ref(), &headers, gateway.as_ref(), &api_call_id)
    {
        // The 200 body MUST match the frozen `KillResponse {api_call_id, killed}`
        // (dashboard-frontend/src/api/types.ts) — the SPA decodes both fields.
        FlowKillOutcome::Killed => (
            StatusCode::OK,
            Json(serde_json::json!({"api_call_id": api_call_id, "killed": true})),
        )
            .into_response(),
        FlowKillOutcome::NotFound => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no live flow for that id"})),
        )
            .into_response(),
        FlowKillOutcome::Denied(denied) => (
            denied.status(),
            Json(serde_json::json!({"error": denied.message()})),
        )
            .into_response(),
    };
    // Dashboard API responses are never cached (mutation result, auth-scoped).
    crate::dashboard_auth::no_store(response)
}

/// Whether a request is an instrumented inference flow (D1, incl. R1 #1): the
/// METHOD must be `POST` AND the path one of the three canonical inference entry
/// points. The method check matters because `/v1/messages` also serves HEAD/OPTIONS
/// probes — those (and any non-POST) must NOT open an orphan flow record.
/// `/v1/completions` is a raw upstream passthrough that bypasses the engine (never
/// instrumented); `/dashboard*`/`/debug*`/`/health`/`/`/`/v1/models` carry no flow.
fn is_flow_capture_request(method: &axum::http::Method, path: &str) -> bool {
    method == axum::http::Method::POST
        && matches!(
            path,
            "/v1/responses" | "/v1/messages" | "/v1/chat/completions"
        )
}

/// Whether `path` is a dashboard auth endpoint whose request body carries the
/// session secret (the login `{"token": ...}`; logout is bodyless but symmetric).
/// D7a R2 #1: the bare JSON key `token` is NOT in the global sensitive-key set
/// (too many legitimate `token` fields elsewhere), so the access token would leak
/// through the small-body `body_payload` dump. D7a R3 #1: a `body_sha256` + a
/// `body_bytes` length on a login body form an OFFLINE verification oracle — an
/// attacker with the logs can brute-force the token against the known digest and
/// length. So for these endpoints we suppress ALL body-derived fields (digest,
/// length, summary, AND payload), logging only non-body metadata.
fn is_dashboard_auth_path(path: &str) -> bool {
    matches!(path, "/dashboard/login" | "/dashboard/logout")
}

/// Body-derived tracing fields for the inbound-request log line. `None` for a
/// dashboard auth endpoint (D7a R3 #1): emitting the body length or its SHA-256
/// for a login body leaks an offline token-verification oracle, so an auth-path
/// request logs NO body-derived field at all (not the digest, length, summary,
/// nor — separately — the payload dump). `Some` for every other path carries the
/// length, hex digest, and the (already-redacted) summary.
struct BodyLogFields {
    bytes: usize,
    sha256: String,
    summary: String,
}

/// Compute the body-derived log fields for `path`/`body`, returning `None` for a
/// dashboard auth endpoint so the caller emits no body-derived field (D7a R3 #1
/// — the digest + length are a token-verification oracle).
fn body_log_fields(path: &str, body: &Bytes) -> Option<BodyLogFields> {
    if is_dashboard_auth_path(path) {
        return None;
    }
    Some(BodyLogFields {
        bytes: body.len(),
        sha256: hex::encode(Sha256::digest(body)),
        summary: summarize_api_body(path, body),
    })
}

/// Parse the inbound `Content-Length` as a byte count, if present and valid.
fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

/// 413 response for an inbound body that exceeds `limit_bytes`.
fn payload_too_large(limit_bytes: usize) -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        format!("request body exceeds the {limit_bytes}-byte limit"),
    )
        .into_response()
}

/// True when an `axum::body::to_bytes` error is an over-cap length-limit
/// rejection (the inbound body exceeded the configured byte cap), as opposed to
/// a truncated or otherwise broken stream. `to_bytes` collects the body through
/// `http_body_util::Limited`, which surfaces a `LengthLimitError` in the error
/// source chain on overflow — so the classification is exact and does not depend
/// on whether a `Content-Length` was sent.
fn is_length_limit_error(err: &axum::Error) -> bool {
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        if cause.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        source = cause.source();
    }
    false
}

async fn log_api_call(
    State(gateway): State<Arc<Gateway>>,
    request: Request,
    next: Next,
) -> Response {
    let api_call_id = format!("api_{}", Uuid::new_v4().simple());
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let started_at = Instant::now();

    // The configurable inbound body cap (default 10 MiB), read from the gateway
    // config — the SAME value `build_router` hands `DefaultBodyLimit::max`, so the
    // middleware's buffered-read cap and the POST extractors' ceiling never diverge.
    let max_request_body_bytes = gateway.config().max_request_body_bytes;

    // Reject before buffering when the declared Content-Length already exceeds
    // the inbound cap: a hostile multi-hundred-MiB upload is refused with 413
    // without reading a byte. `DefaultBodyLimit` (checked later by the JSON
    // extractor) cannot bound memory here because this middleware buffers the
    // whole body for logging FIRST — so the cap is also enforced at the read
    // below for bodies that arrive without a (trustworthy) Content-Length.
    //
    // F1b turn-capture scope (review #3): these PRE-body-read rejections (the 413
    // here, and the read-failure 413/400 just below) return BEFORE the capture
    // gate runs and BEFORE any `api_call_id` turn/`inbound_request` section is
    // minted — so they are intentionally NOT captured: there is no turn to attach
    // a `served_response` to, and we do not mint one just to record a 413.
    // Every POST-gate response (an engine error, a `Reject`, any 4xx/5xx produced
    // AFTER the gate) IS teed, because the tee wraps the WHOLE `next.run` result
    // below regardless of status.
    let declared_length = content_length(&headers);
    if let Some(declared) = declared_length
        && declared > max_request_body_bytes as u64
    {
        tracing::warn!(
            api_call_id = %api_call_id,
            method = %method,
            path = %uri.path(),
            content_length = declared,
            limit_bytes = max_request_body_bytes,
            "rejected oversized inbound API request: Content-Length over limit"
        );
        return payload_too_large(max_request_body_bytes);
    }

    let (mut parts, body) = request.into_parts();
    // Cap the buffered read at the CONFIGURED limit (not a fixed ceiling) so a
    // chunked / length-less body cannot grow memory past the cap. An over-cap
    // body surfaces a `LengthLimitError` -> 413 oversize; any other read failure
    // (truncated / broken stream) -> 400. The classification is exact, so it is
    // correct even for length-less bodies that lacked the Content-Length precheck.
    let body_bytes = match to_bytes(body, max_request_body_bytes).await {
        Ok(bytes) => bytes,
        Err(err) => {
            if is_length_limit_error(&err) {
                tracing::warn!(
                    api_call_id = %api_call_id,
                    method = %method,
                    path = %uri.path(),
                    limit_bytes = max_request_body_bytes,
                    error = %err,
                    "rejected inbound API request: body exceeded limit"
                );
                return payload_too_large(max_request_body_bytes);
            }
            tracing::warn!(
                api_call_id = %api_call_id,
                method = %method,
                path = %uri.path(),
                error = %err,
                "failed to read inbound API request body"
            );
            return (
                StatusCode::BAD_REQUEST,
                format!("failed to read request body: {err}"),
            )
                .into_response();
        }
    };

    // D7a R3 #1: for a dashboard auth endpoint (login/logout) NO body-derived
    // field may be logged — a `body_sha256` + `body_bytes` length on the login
    // body is an offline token-verification oracle. `body_log_fields` returns
    // `None` there so we emit only non-body metadata; every other path logs the
    // length, hex digest, and the redacted summary.
    let is_auth_path = is_dashboard_auth_path(uri.path());
    match body_log_fields(uri.path(), &body_bytes) {
        Some(fields) => tracing::info!(
            api_call_id = %api_call_id,
            method = %method,
            path = %uri.path(),
            query = uri.query().unwrap_or(""),
            content_type = %header_for_log(&headers, header::CONTENT_TYPE.as_str()),
            user_agent = %header_for_log(&headers, header::USER_AGENT.as_str()),
            anthropic_version = %header_for_log(&headers, "anthropic-version"),
            anthropic_beta = %header_for_log(&headers, "anthropic-beta"),
            openai_beta = %header_for_log(&headers, "openai-beta"),
            request_id = %header_for_log(&headers, "x-request-id"),
            authorization_present = headers.contains_key(header::AUTHORIZATION),
            x_api_key_present = headers.contains_key("x-api-key"),
            body_bytes = fields.bytes,
            body_sha256 = %fields.sha256,
            body_summary = %fields.summary,
            "inbound API request"
        ),
        // Auth endpoint: log only non-body metadata (no length, digest, summary).
        None => tracing::info!(
            api_call_id = %api_call_id,
            method = %method,
            path = %uri.path(),
            query = uri.query().unwrap_or(""),
            content_type = %header_for_log(&headers, header::CONTENT_TYPE.as_str()),
            user_agent = %header_for_log(&headers, header::USER_AGENT.as_str()),
            anthropic_version = %header_for_log(&headers, "anthropic-version"),
            anthropic_beta = %header_for_log(&headers, "anthropic-beta"),
            openai_beta = %header_for_log(&headers, "openai-beta"),
            request_id = %header_for_log(&headers, "x-request-id"),
            authorization_present = headers.contains_key(header::AUTHORIZATION),
            x_api_key_present = headers.contains_key("x-api-key"),
            "inbound API request"
        ),
    }
    // Never dump the auth-endpoint body (it carries the token, and even its
    // length/digest are an oracle — handled above).
    if !is_auth_path && body_bytes.len() <= API_LOG_PAYLOAD_DUMP_LIMIT_BYTES {
        tracing::info!(
            api_call_id = %api_call_id,
            method = %method,
            path = %uri.path(),
            body_payload = %payload_for_log(&body_bytes),
            "inbound API request payload"
        );
    }

    // Shared "instrument this request?" predicate (POST + whitelisted inference
    // path). Both the dashboard FlowStore gate and the F1 turn-capture gate hang
    // off it, so a HEAD/OPTIONS probe or a non-whitelisted path opens neither.
    let instrument = is_flow_capture_request(&method, uri.path());
    // D1: dashboard FlowStore capture is gated on the debug UI (`flow_store()` is
    // `disabled()` off `--with-debug-ui`).
    let flow_gate = instrument && gateway.flow_store().is_enabled();
    // F1b (spec Design #1): turn capture has its OWN gate on the SAME paths but
    // keyed on `turn_capture().is_enabled()` INDEPENDENT of the flow store / debug
    // UI — so `api_call_id` reaches the engine and the artifact is written with the
    // dashboard OFF.
    let capture_gate = instrument && gateway.turn_capture().is_enabled();

    // The `api_call_id` extension the engine reads to link `response_id →
    // api_call_id` (D1) and to reach the per-turn capture state (F1c) is inserted
    // ONCE if EITHER gate wants it — never double-inserted when both fire.
    if flow_gate || capture_gate {
        parts
            .extensions
            .insert(crate::dashboard_flow::ApiCallId(api_call_id.clone()));
    }

    // D1 (incl. R1 #1/#6): capture the inbound body + headers and open the record.
    // Secrets (auth headers, `api_key`, image URIs) are redacted INLINE by the
    // serializer/header redactor — none persist here. D3 L0: the RAII middleware
    // guard. `None` for disabled-store / non-whitelisted requests (zero overhead).
    // When `Some`, it is held across `next.run`: if the request never reaches the
    // engine (an extractor/`Json` rejection, a layer panic above the handler) the
    // record is still `OpenL0` at the guard's `Drop`, which CASes it to `Finalized`
    // + `Failed("unhandled")` — no orphan stuck `Open`. If the engine claimed it
    // (`ClaimedL1`), the L0 `Drop` is inert and L1 owns finalization.
    let _l0_guard = if flow_gate {
        let inbound_body = Some(crate::dashboard_flow::capture_body(&body_bytes));
        // Gap 04: derive the client attribution from the RAW headers BEFORE they are
        // redacted — this is the only point the raw API key is still readable, and
        // `derive` hashes it in-place (a one-way SHA-256 prefix becomes the label; the
        // raw key is dropped, never stored/logged). The optional configured caller-id
        // header NAME is read env-only (`LLMCONDUIT_DASHBOARD_CLIENT_HEADER`) so no
        // secret/identity config lands in the `Debug`/`Clone` persisted `Config`
        // struct — mirroring the dashboard auth env-only posture. The header name is
        // non-secret; only the api-key VALUE is, and it is never persisted.
        let client = crate::dashboard_flow::ClientAttribution::derive(
            &headers,
            dashboard_client_header().as_deref(),
        );
        let headers_redacted = crate::dashboard_flow::redact_headers(&headers);
        gateway.flow_store().open(
            api_call_id.clone(),
            method.to_string(),
            uri.path().to_string(),
            headers_redacted,
            inbound_body,
            client,
        );
        gateway.flow_store().middleware_guard(&api_call_id)
    } else {
        None
    };

    // F1b: start the per-turn artifact and write the redacted inbound-request
    // section. `redacted_inbound_section` COPIES + redacts the body (secret keys +
    // image URIs, the SAME path `payload_for_log` uses — AGENTS.md line 137/144),
    // never retaining a slice of the 256 MiB buffer.
    let turn_capture_state = if capture_gate {
        // Finding 1: redact OFF the tokio worker for large bodies (spawn_blocking),
        // AWAITED here before `write_inbound_request` so the section is written +
        // closed before the finalize barrier can read it. `body_bytes.clone()` is a
        // cheap Arc-backed `Bytes` clone; the offload copies it into an OWNED `Vec` for
        // the blocking task (F1 — so nothing pins the 256 MiB backing) and this clone
        // is dropped before `body_bytes` is moved into the rebuilt request below.
        let (model_requested, inbound_section, inbound_partial) =
            offload_redacted_inbound_section(body_bytes.clone()).await;
        let state = gateway
            .turn_capture()
            .start(&api_call_id, model_requested, epoch_millis());
        if let Some(state) = &state {
            state.write_inbound_request(&inbound_section);
            if inbound_partial {
                // F3 (Fable-fix): the redaction offload could not capture the body (a
                // spawn_blocking join failure); mark the section partial so it never
                // reads as a complete inbound body (don't-lie-with-zeros).
                state.mark_inbound_request_degraded();
            }
        }
        state
    } else {
        None
    };

    // F1c: the turn-capture MIDDLEWARE backstop, held across `next.run`. If the
    // request NEVER reaches the engine (a `Json`/extractor rejection, a
    // `convert_request` error — so no engine `CaptureGuard` is ever built), the turn
    // is UNCLAIMED at this guard's `Drop`, which then finalizes the engine side
    // `failed`/`"unhandled"`. That closes the both-`done` barrier (the served tee
    // below always fires `served_done`), so the registry entry + `.work` dir are
    // evicted and a useful `status:"failed"` artifact is still written — no hang, no
    // leak. A turn that reached the engine is CLAIMED synchronously (before
    // `next.run` returns), so this backstop is inert for it. Mirrors the dashboard's
    // L0 `MiddlewareGuard`.
    let _capture_backstop = turn_capture_state
        .as_ref()
        .map(|state| crate::turn_capture::MiddlewareCaptureGuard::new(Arc::clone(state)));

    let request = Request::from_parts(parts, Body::from(body_bytes));
    let response = next.run(request).await;
    // F1b served-body tee (spec Design #4): wrap the outbound response `Body` so
    // every served byte — streaming SSE, non-streaming JSON, or a handler error
    // body — is copied to the `served_response` section; its `Drop` marks
    // `served_done(partial)` when the stream did not reach a clean end (a client
    // disconnect drops the body mid-stream). One wrapper covers all served shapes.
    // Review #3: this wraps the WHOLE `next.run` result unconditionally, so every
    // POST-gate error response (an engine error, a `Reject`, any 4xx/5xx minted
    // after the gate inserted the `ApiCallId` extension) is teed too — only the
    // PRE-body-read 413/400 rejections above (no turn minted) are out of scope.
    let response = match turn_capture_state {
        Some(state) => tee_served_body(response, state),
        None => response,
    };
    // Per-request model-resolution audit: the handler tags the response with the
    // served model (and the requested model when it differs) via
    // `with_model_headers`; echo both here so every response record shows whether
    // a model fell back — un-throttled, unlike the engine WARN. `requested_model`
    // is empty when the requested model was served as-is.
    let served_model = header_for_log(response.headers(), "x-llmconduit-model").to_string();
    let requested_model = header_for_log(response.headers(), "x-llmconduit-requested").to_string();
    tracing::info!(
        api_call_id = %api_call_id,
        method = %method,
        path = %uri.path(),
        status = response.status().as_u16(),
        served_model = %served_model,
        requested_model = %requested_model,
        elapsed_ms = started_at.elapsed().as_millis(),
        "inbound API response prepared"
    );
    response
}

async fn api_not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}

fn probe_response(allow: &str) -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::ALLOW,
        HeaderValue::try_from(allow).expect("valid header value"),
    );
    response
}

async fn probe_messages() -> Response {
    probe_response("POST, HEAD, OPTIONS")
}

async fn get_health() -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({"status": "healthy"})),
    )
        .into_response()
}

async fn get_root() -> Response {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
}

fn header_for_log(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(compact_for_log)
        .unwrap_or_default()
}

/// Env var naming the OPTIONAL non-secret request header that carries an explicit
/// caller id (e.g. `x-client-id`) for the dashboard's client attribution (gap 04).
const ENV_DASHBOARD_CLIENT_HEADER: &str = "LLMCONDUIT_DASHBOARD_CLIENT_HEADER";

/// The operator-configured caller-id header NAME, read ENV-ONLY (never from the
/// persisted `Config`, which is `Debug`/`Clone` — keeping attribution config out of
/// it mirrors the dashboard auth env-only posture; AGENTS.md). The header name itself
/// is non-secret — only the api-key VALUE is sensitive, and that is never persisted.
/// `None`/blank ⇒ the configured-header attribution source is simply skipped (the
/// derivation falls through to the User-Agent fallback). Trimmed; blank ⇒ `None`.
fn dashboard_client_header() -> Option<String> {
    std::env::var(ENV_DASHBOARD_CLIENT_HEADER)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn summarize_api_body(path: &str, body: &Bytes) -> String {
    if body.is_empty() {
        return "empty".to_string();
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(value) => summarize_json_api_body(path, &value),
        Err(err) => {
            // Redact image URIs from the raw preview before logging (round-4 #2):
            // a non-JSON body could still embed a `data:`/signed image URL.
            let preview = crate::redaction::redact_image_uris(&String::from_utf8_lossy(body));
            format!(
                "non_json parse_error={} preview={}",
                compact_for_log(&err.to_string()),
                compact_for_log(&preview)
            )
        }
    }
}

fn payload_for_log(body: &Bytes) -> String {
    match serde_json::from_slice::<Value>(body) {
        Ok(mut value) => {
            redact_payload_secrets(&mut value);
            // G4 round-4 #2: an inbound body under the dump limit would otherwise
            // log raw `data:` image bytes / signed `image_url`s. Strip image URIs
            // from every remaining string via the shared redactor BEFORE
            // serializing, so no logged surface carries request image content.
            crate::redaction::redact_image_uris_in_value(&mut value);
            serde_json::to_string(&value)
                .unwrap_or_else(|_| "<failed to serialize json>".to_string())
        }
        Err(_) => {
            // Non-JSON body: still strip image URIs from the raw text so a
            // `data:`/signed URL in a malformed/odd payload is not logged raw.
            crate::redaction::redact_image_uris(&String::from_utf8_lossy(body))
        }
    }
}

fn redact_payload_secrets(value: &mut Value) {
    // Single sensitive-key authority AND walker now live in `crate::redaction`
    // (D1 R1 #10; F1d extended the shared authority from just the key-list to the
    // walk itself, so `upstream.rs`'s turn-capture `upstream_request` section can
    // reuse the EXACT same redaction without duplicating the tree-walk here).
    // This name stays as the documented call-through (AGENTS.md line 137, the F1
    // spec) — only its body changed.
    crate::redaction::redact_payload_secrets_in_value(value);
}

/// F1b: the redacted bytes for the turn-capture `inbound_request` section, plus
/// the requested `model` (outcome metadata). Redaction MIRRORS `payload_for_log`
/// EXACTLY — secret keys via [`redact_payload_secrets`], image/data URIs via
/// [`crate::redaction::redact_image_uris_in_value`] — so the on-disk artifact is a
/// NEW logged surface that does NOT bypass `redact_payload_secrets` (AGENTS.md
/// line 137) and never carries raw image bytes. Parses/serializes a fresh owned
/// `Value`, so it COPIES out of `body` and never retains a slice of the 256 MiB
/// middleware buffer (AGENTS.md line 144).
fn redacted_inbound_section(body: &[u8]) -> (Option<String>, Vec<u8>) {
    match serde_json::from_slice::<Value>(body) {
        Ok(mut value) => {
            let model = value
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_string);
            redact_payload_secrets(&mut value);
            crate::redaction::redact_image_uris_in_value(&mut value);
            let bytes = serde_json::to_vec(&value)
                .unwrap_or_else(|_| b"<failed to serialize json>".to_vec());
            (model, bytes)
        }
        Err(_) => {
            // Non-JSON body: still strip image URIs from the raw text so a
            // `data:`/signed URL in a malformed/odd payload is not captured raw.
            let redacted = crate::redaction::redact_image_uris(&String::from_utf8_lossy(body));
            (None, redacted.into_bytes())
        }
    }
}

/// Fable review (Finding 1): produce the redacted `inbound_request` bytes, moving the
/// CPU-bound parse+redact+re-serialize OFF the tokio worker for a LARGE body via
/// `spawn_blocking` (mirroring `upstream::UpstreamRequestLogger`). Small bodies
/// (`<= TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES`) stay inline — the blocking-pool hop
/// isn't worth it. Redaction is IDENTICAL on both paths (it is the SAME
/// [`redacted_inbound_section`]). The caller AWAITS this INLINE, before
/// `write_inbound_request` (append + close), so the section is fully written and
/// closed before the both-`done` finalize barrier can read it (no section race, no
/// hang). Returns `(model_requested, redacted_bytes, partial)`; `partial` is `true`
/// ONLY on a join failure (the body could not be captured -- the caller then marks
/// the section degraded rather than reporting a false "complete", don't-lie-with-zeros).
///
/// F1 (Fable-fix): the blocking task is handed an OWNED `Vec<u8>` copy of the body,
/// NOT the Arc-backed `Bytes` — moving a `Bytes` clone into `spawn_blocking` would PIN
/// the whole 256 MiB inbound middleware backing allocation for the task's lifetime,
/// and a DETACHED task (outer future cancelled) would keep it pinned (AGENTS.md line
/// 144 — no retained slice of that buffer). The owned right-sized copy is the intended
/// cost; the redacted output is likewise a fresh owned `Vec`.
async fn offload_redacted_inbound_section(body: Bytes) -> (Option<String>, Vec<u8>, bool) {
    if body.len() <= TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES {
        let (model, bytes) = redacted_inbound_section(&body);
        return (model, bytes, false);
    }
    // Copy to an OWNED, right-sized `Vec` and DROP the Arc-backed `Bytes` BEFORE the
    // blocking hop, so nothing pins the 256 MiB backing across the task (or after, on
    // cancellation when the task detaches).
    let owned: Vec<u8> = body.to_vec();
    drop(body);
    match tokio::task::spawn_blocking(move || redacted_inbound_section(&owned)).await {
        Ok((model, bytes)) => (model, bytes, false),
        // `redacted_inbound_section` is panic-free (serde failures fall back to a
        // marker), so a `JoinError` here means the runtime is shutting down. Record an
        // honest, NON-EMPTY marker AND signal `partial` so the section still closes
        // (never a hang) and never reads as a fabricated empty/complete body
        // (don't-lie-with-zeros; F3).
        Err(err) => {
            tracing::warn!(error = %err, "turn-capture: inbound redaction task failed");
            (
                None,
                b"<turn-capture: inbound redaction task failed>".to_vec(),
                true,
            )
        }
    }
}

/// Current wall-clock time as epoch milliseconds (the `started_ms` clock the
/// dashboard FlowStore's `started_ms` also uses, for a consistent turn timestamp).
fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or(0)
}

fn summarize_json_api_body(path: &str, value: &Value) -> String {
    let Some(map) = value.as_object() else {
        return format!("json_type={}", json_type(value));
    };

    let mut parts = Vec::new();
    parts.push(format!(
        "keys={}",
        summarized_list(map.keys().cloned().collect(), 24)
    ));
    append_common_json_fields(&mut parts, map);

    if path.contains("/messages") {
        append_anthropic_json_summary(&mut parts, map);
    } else if path.contains("/responses") {
        append_responses_json_summary(&mut parts, map);
    } else if path.contains("/chat/completions") || path.ends_with("/completions") {
        append_chat_json_summary(&mut parts, map);
    } else {
        append_generic_json_summary(&mut parts, map);
    }

    parts.join(" ")
}

fn append_common_json_fields(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    for key in [
        "model",
        "stream",
        "max_tokens",
        "max_output_tokens",
        "max_completion_tokens",
        "store",
        "parallel_tool_calls",
        "temperature",
        "top_p",
    ] {
        append_scalar_field(parts, map, key);
    }
    append_typed_field(parts, map, "tool_choice");
    append_typed_field(parts, map, "thinking");
    append_typed_field(parts, map, "reasoning");
}

fn append_anthropic_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    append_anthropic_system_summary(parts, map.get("system"));
    if let Some(messages) = map.get("messages").and_then(Value::as_array) {
        parts.push(format!("messages={}", messages.len()));
        append_anthropic_message_summary(parts, messages);
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
    append_metadata_summary(parts, map.get("metadata"));
    append_array_len(parts, map, "stop_sequences");
}

fn append_responses_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    if let Some(instructions) = map.get("instructions").and_then(Value::as_str) {
        parts.push(format!(
            "instructions_chars={}",
            instructions.chars().count()
        ));
    }
    match map.get("input") {
        Some(Value::String(text)) => {
            parts.push("input=string".to_string());
            parts.push(format!("input_chars={}", text.chars().count()));
        }
        Some(Value::Array(items)) => {
            parts.push(format!("input_items={}", items.len()));
            append_responses_input_summary(parts, items);
        }
        Some(other) => {
            parts.push(format!("input_type={}", json_type(other)));
        }
        None => {}
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
    append_array_len(parts, map, "include");
    append_metadata_summary(parts, map.get("metadata"));
}

fn append_chat_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    if let Some(messages) = map.get("messages").and_then(Value::as_array) {
        parts.push(format!("messages={}", messages.len()));
        append_chat_message_summary(parts, messages);
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
    append_typed_field(parts, map, "response_format");
    append_typed_field(parts, map, "stream_options");
}

fn append_generic_json_summary(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>) {
    if let Some(messages) = map.get("messages").and_then(Value::as_array) {
        parts.push(format!("messages={}", messages.len()));
        append_chat_message_summary(parts, messages);
    }
    match map.get("input") {
        Some(Value::String(text)) => {
            parts.push("input=string".to_string());
            parts.push(format!("input_chars={}", text.chars().count()));
        }
        Some(Value::Array(items)) => {
            parts.push(format!("input_items={}", items.len()));
        }
        _ => {}
    }
    if let Some(tools) = map.get("tools").and_then(Value::as_array) {
        append_tool_summary(parts, "tools", tools);
    }
}

fn append_anthropic_system_summary(parts: &mut Vec<String>, system: Option<&Value>) {
    match system {
        Some(Value::String(text)) => {
            parts.push("system=string".to_string());
            parts.push(format!("system_chars={}", text.chars().count()));
        }
        Some(Value::Array(blocks)) => {
            let mut text_chars = 0usize;
            let mut counts = BTreeMap::new();
            for block in blocks {
                let kind = typed_json_value(block);
                increment_count(&mut counts, kind);
                if let Some(text) = block.get("text").and_then(Value::as_str) {
                    text_chars += text.chars().count();
                }
            }
            parts.push(format!("system_blocks={}", blocks.len()));
            parts.push(format!("system_chars={text_chars}"));
            push_counts(parts, "system_block_types", &counts);
        }
        Some(other) => {
            parts.push(format!("system_type={}", json_type(other)));
        }
        None => {}
    }
}

fn append_anthropic_message_summary(parts: &mut Vec<String>, messages: &[Value]) {
    let mut roles = Vec::new();
    let mut content_counts = BTreeMap::new();
    let mut text_chars = 0usize;

    for message in messages {
        roles.push(
            message
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        );
        if let Some(content) = message.get("content") {
            accumulate_anthropic_content(content, &mut text_chars, &mut content_counts);
        }
    }

    parts.push(format!("message_roles={}", summarized_list(roles, 16)));
    parts.push(format!("message_text_chars={text_chars}"));
    push_counts(parts, "message_content", &content_counts);
}

fn append_chat_message_summary(parts: &mut Vec<String>, messages: &[Value]) {
    let mut roles = Vec::new();
    let mut content_counts = BTreeMap::new();
    let mut text_chars = 0usize;
    let mut tool_calls = 0usize;

    for message in messages {
        roles.push(
            message
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        );
        if let Some(content) = message.get("content") {
            accumulate_chat_content(content, &mut text_chars, &mut content_counts);
        }
        tool_calls += message
            .get("tool_calls")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
    }

    parts.push(format!("message_roles={}", summarized_list(roles, 16)));
    parts.push(format!("message_text_chars={text_chars}"));
    parts.push(format!("message_tool_calls={tool_calls}"));
    push_counts(parts, "message_content", &content_counts);
}

fn append_responses_input_summary(parts: &mut Vec<String>, items: &[Value]) {
    let mut roles = Vec::new();
    let mut item_counts = BTreeMap::new();
    let mut text_chars = 0usize;

    for item in items {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_else(|| {
            if item.get("role").is_some() {
                "message"
            } else {
                "unknown"
            }
        });
        increment_count(&mut item_counts, item_type.to_string());
        if let Some(role) = item.get("role").and_then(Value::as_str) {
            roles.push(role.to_string());
        }
        if let Some(content) = item.get("content") {
            accumulate_responses_content(content, &mut text_chars);
        }
    }

    parts.push(format!("input_roles={}", summarized_list(roles, 16)));
    parts.push(format!("input_text_chars={text_chars}"));
    push_counts(parts, "input_item_types", &item_counts);
}

fn accumulate_anthropic_content(
    content: &Value,
    text_chars: &mut usize,
    counts: &mut BTreeMap<String, usize>,
) {
    match content {
        Value::String(text) => {
            *text_chars += text.chars().count();
            increment_count(counts, "string".to_string());
        }
        Value::Array(blocks) => {
            for block in blocks {
                let kind = typed_json_value(block);
                increment_count(counts, kind.clone());
                match kind.as_str() {
                    "text" => {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            *text_chars += text.chars().count();
                        }
                    }
                    "thinking" => {
                        if let Some(text) = block.get("thinking").and_then(Value::as_str) {
                            *text_chars += text.chars().count();
                        }
                    }
                    "tool_result" => {
                        if let Some(nested) = block.get("content") {
                            accumulate_anthropic_content(nested, text_chars, counts);
                        }
                    }
                    _ => {}
                }
            }
        }
        other => {
            increment_count(counts, json_type(other).to_string());
        }
    }
}

fn accumulate_chat_content(
    content: &Value,
    text_chars: &mut usize,
    counts: &mut BTreeMap<String, usize>,
) {
    match content {
        Value::String(text) => {
            *text_chars += text.chars().count();
            increment_count(counts, "string".to_string());
        }
        Value::Array(parts) => {
            for part in parts {
                let kind = typed_json_value(part);
                increment_count(counts, kind.clone());
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    *text_chars += text.chars().count();
                }
            }
        }
        other => {
            increment_count(counts, json_type(other).to_string());
        }
    }
}

fn accumulate_responses_content(content: &Value, text_chars: &mut usize) {
    match content {
        Value::String(text) => {
            *text_chars += text.chars().count();
        }
        Value::Array(parts) => {
            for part in parts {
                for key in ["text", "input_text", "output_text"] {
                    if let Some(text) = part.get(key).and_then(Value::as_str) {
                        *text_chars += text.chars().count();
                    }
                }
            }
        }
        _ => {}
    }
}

fn append_tool_summary(parts: &mut Vec<String>, label: &str, tools: &[Value]) {
    let names = tools
        .iter()
        .filter_map(tool_name_for_summary)
        .collect::<Vec<_>>();
    parts.push(format!("{label}={}", tools.len()));
    if !names.is_empty() {
        parts.push(format!("{label}_names={}", summarized_list(names, 12)));
    }
}

fn tool_name_for_summary(tool: &Value) -> Option<String> {
    tool.get("name")
        .and_then(Value::as_str)
        .or_else(|| {
            tool.get("function")
                .and_then(|function| function.get("name"))
                .and_then(Value::as_str)
        })
        .map(ToString::to_string)
}

fn append_metadata_summary(parts: &mut Vec<String>, metadata: Option<&Value>) {
    if let Some(Value::Object(map)) = metadata {
        parts.push(format!(
            "metadata_keys={}",
            summarized_list(map.keys().cloned().collect(), 16)
        ));
    }
}

fn append_array_len(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>, key: &str) {
    if let Some(values) = map.get(key).and_then(Value::as_array) {
        parts.push(format!("{key}={}", values.len()));
    }
}

fn append_scalar_field(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>, key: &str) {
    if let Some(value) = map.get(key).and_then(scalar_for_log) {
        parts.push(format!("{key}={value}"));
    }
}

fn append_typed_field(parts: &mut Vec<String>, map: &serde_json::Map<String, Value>, key: &str) {
    if let Some(value) = map.get(key) {
        parts.push(format!("{key}={}", typed_json_value(value)));
    }
}

fn typed_json_value(value: &Value) -> String {
    match value {
        Value::Object(map) => map
            .get("type")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .unwrap_or_else(|| "object".to_string()),
        Value::Array(_) => "array".to_string(),
        Value::String(text) => compact_for_log(text),
        other => json_type(other).to_string(),
    }
}

fn scalar_for_log(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(compact_for_log(text)),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Null => Some("null".to_string()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn push_counts(parts: &mut Vec<String>, label: &str, counts: &BTreeMap<String, usize>) {
    if counts.is_empty() {
        return;
    }
    let values = counts
        .iter()
        .map(|(key, count)| format!("{key}:{count}"))
        .collect::<Vec<_>>();
    parts.push(format!("{label}={}", summarized_list(values, 16)));
}

fn increment_count(counts: &mut BTreeMap<String, usize>, key: String) {
    *counts.entry(key).or_default() += 1;
}

fn summarized_list(mut values: Vec<String>, max: usize) -> String {
    let total = values.len();
    values.truncate(max);
    if total > max {
        values.push(format!("+{}", total - max));
    }
    format!("[{}]", values.join(","))
}

fn compact_for_log(value: &str) -> String {
    let mut compact = String::new();
    for ch in value.chars().take(API_LOG_PREVIEW_CHARS) {
        if ch.is_control() {
            compact.push(' ');
        } else {
            compact.push(ch);
        }
    }
    if value.chars().count() > API_LOG_PREVIEW_CHARS {
        compact.push_str("...");
    }
    compact
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

async fn post_responses(
    State(gateway): State<Arc<Gateway>>,
    api_call_id: Option<axum::Extension<crate::dashboard_flow::ApiCallId>>,
    Json(request): Json<ResponsesRequest>,
) -> AppResult<Response> {
    let requested = request.model.clone();
    let served = gateway.resolve_request_model(&request.model).await.0;
    let wants_stream = request.stream;
    let stream = gateway
        .stream_responses_with_api_call_id(request, api_call_id.map(|extension| extension.0.0))
        .await?;
    let response = if wants_stream {
        stream_responses_response(stream)
    } else {
        collect_responses_response(stream).await?
    };
    Ok(with_model_headers(response, &requested, &served))
}

async fn post_messages(
    State(gateway): State<Arc<Gateway>>,
    api_call_id: Option<axum::Extension<crate::dashboard_flow::ApiCallId>>,
    Json(request): Json<AnthropicRequest>,
) -> Response {
    let api_call_id = api_call_id.map(|extension| extension.0.0);
    match handle_post_messages(gateway, request, api_call_id).await {
        Ok(response) => response,
        Err(err) => anthropic_error_response(err),
    }
}

async fn post_count_tokens(State(gateway): State<Arc<Gateway>>, body: Bytes) -> Response {
    let request: AnthropicRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(err) => {
            return anthropic_error_response(AppError::bad_request(format!(
                "invalid request body: {err}"
            )));
        }
    };
    match handle_count_tokens(gateway, request).await {
        Ok(response) => response,
        Err(err) => anthropic_error_response(err),
    }
}

async fn handle_count_tokens(
    gateway: Arc<Gateway>,
    request: AnthropicRequest,
) -> AppResult<Response> {
    use crate::engine::TokenizeCapability;

    if gateway.tokenize_capability() == TokenizeCapability::Unsupported {
        return Err(AppError::not_found("upstream does not support /tokenize"));
    }

    let original_model = request.model.clone();
    let responses_request = anthropic_to_responses::convert_request(request)?;
    let resolved_model = gateway.resolve_request_model(&original_model).await.0;
    let responses_request = gateway.apply_system_prompt_prefix(responses_request, &resolved_model);
    let roles = gateway
        .config()
        .resolve_roles_config_for_resolved_model(&original_model, &resolved_model);
    let lowered = responses_to_chat::lower_request_with_image_agent_and_roles(
        &responses_request,
        Vec::new(),
        false,
        roles,
    )?;
    let client_chat_template_kwargs = responses_request
        .extra_body
        .get("chat_template_kwargs")
        .and_then(Value::as_object)
        .cloned();
    let thinking_override = responses_request.thinking;
    let backend = BackendChatRequest::new(
        ChatCompletionRequest {
            model: resolved_model,
            messages: lowered.messages,
            stream: false,
            tools: (!lowered.tools.is_empty()).then_some(lowered.tools),
            tool_choice: Some(responses_request.tool_choice),
            parallel_tool_calls: responses_request.parallel_tool_calls,
            reasoning_effort: lowered.reasoning_effort,
            response_format: lowered.response_format,
            stream_options: None,
            temperature: responses_request.temperature,
            top_p: responses_request.top_p,
            max_output_tokens: None,
            frequency_penalty: lowered.frequency_penalty,
            presence_penalty: lowered.presence_penalty,
            stop: normalize_stop(responses_request.stop)?,
            extra_body: responses_request.extra_body,
        },
        client_chat_template_kwargs,
        None,
        None,
    )
    .with_thinking_override(thinking_override);

    match gateway.upstream_client().count_tokens(&backend).await {
        Ok(Some(count)) => {
            gateway.set_tokenize_capability(TokenizeCapability::Supported);
            Ok((
                StatusCode::OK,
                Json(serde_json::json!({ "input_tokens": count })),
            )
                .into_response())
        }
        Ok(None) | Err(_) => {
            gateway.set_tokenize_capability(TokenizeCapability::Unsupported);
            Err(AppError::not_found("upstream does not support /tokenize"))
        }
    }
}

async fn post_chat_completions(
    State(gateway): State<Arc<Gateway>>,
    api_call_id: Option<axum::Extension<crate::dashboard_flow::ApiCallId>>,
    Json(request): Json<ChatCompletionRequest>,
) -> AppResult<Response> {
    let requested = request.model.clone();
    let model = gateway.resolve_request_model(&request.model).await.0;
    let wants_stream = request.stream;
    let include_usage = request
        .stream_options
        .as_ref()
        .is_some_and(|options| options.include_usage);
    // Decide BEFORE converting (which consumes the inbound request) whether the
    // Chat output converter must suppress `reasoning_content`. Suppression is
    // family-independent: a Chat client that did not request reasoning never
    // receives server-side chain-of-thought from ANY model (G2, Finding 1).
    let suppress_reasoning = gateway.chat_reasoning_suppressed(&request);
    let responses_request = chat_completions::convert_request(request)?;
    let stream = gateway
        .stream_responses_with_api_call_id(
            responses_request,
            api_call_id.map(|extension| extension.0.0),
        )
        .await?;

    let response = if wants_stream {
        stream_chat_completions_response(model.clone(), include_usage, suppress_reasoning, stream)
    } else {
        collect_chat_completions_response(model.clone(), suppress_reasoning, stream).await?
    };
    Ok(with_model_headers(response, &requested, &model))
}

async fn post_completions(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Response> {
    let response = gateway
        .upstream_client()
        .proxy_completions(headers, body)
        .await?;
    Ok(proxy_upstream_response(response))
}

async fn handle_post_messages(
    gateway: Arc<Gateway>,
    request: AnthropicRequest,
    api_call_id: Option<String>,
) -> AppResult<Response> {
    let requested = request.model.clone();
    let model = gateway.resolve_request_model(&request.model).await.0;
    let wants_stream = request.stream;
    let suppress_reasoning = !matches!(
        request.thinking.as_ref(),
        Some(AnthropicThinking::Enabled { .. } | AnthropicThinking::Adaptive { .. })
    );
    let responses_request = anthropic_to_responses::convert_request(request)?;
    let stream = gateway
        .stream_responses_with_api_call_id(responses_request, api_call_id)
        .await?;

    let response = if wants_stream {
        stream_anthropic_response(model.clone(), suppress_reasoning, stream)?
    } else {
        collect_anthropic_response(model.clone(), suppress_reasoning, stream).await?
    };
    Ok(with_model_headers(response, &requested, &model))
}

/// Tag a response with the model that actually served it, so a model mismatch
/// (requested model not served → fell back to the loaded model) is visible
/// PER-REQUEST to anyone inspecting the response (`curl -v`, a proxy, or
/// response logging) without tailing the throttled engine WARN. The
/// `x-llmconduit-requested` header is added ONLY when the requested model
/// differs from the served one (the mismatch signal); an exact/canonical match
/// omits it to keep the common case quiet. The `log_api_call` middleware echoes
/// both headers into the per-request "response prepared" log line.
fn with_model_headers(mut response: Response, requested: &str, served: &str) -> Response {
    let headers = response.headers_mut();
    if !served.is_empty()
        && let Ok(value) = HeaderValue::from_str(served)
    {
        headers.insert(HeaderName::from_static("x-llmconduit-model"), value);
    }
    if !requested.is_empty()
        && !requested.eq_ignore_ascii_case(served)
        && let Ok(value) = HeaderValue::from_str(requested)
    {
        headers.insert(HeaderName::from_static("x-llmconduit-requested"), value);
    }
    response
}

/// F1b served-response tee: an `http_body::Body` that mirrors `inner` frame for
/// frame, COPYING each DATA frame's bytes into the turn-capture `served_response`
/// section (never retaining the frame's backing allocation) and passing every
/// frame through UNCHANGED — so SSE framing, keep-alive comments, and trailers are
/// preserved byte-for-byte. Capture is BACK-PRESSURED (F1b review #1): before
/// pulling each frame it reserves a slot in the section's BOUNDED writer channel
/// and, when the writer is behind, returns `Poll::Pending` — throttling the served
/// stream to disk pace rather than buffering the whole body in RAM (bounded
/// memory, AGENTS.md). Its `Drop` reports `served_done`: `partial` unless the
/// stream reached a clean end (a `Ready(None)` poll, or delivering its full
/// promised length). A client disconnect drops the body mid-stream → partial.
/// F1b review r2: if `served_sink` ever closes early (writer gone mid-stream —
/// see the field doc), that alone permanently marks the section `partial` via
/// `mark_served_degraded`, REGARDLESS of how `Drop`'s own clean-end check comes
/// out — a truncated capture must never be reported complete just because the
/// client-facing stream itself still ended cleanly.
struct TeeBody {
    inner: Body,
    state: Arc<crate::turn_capture::TurnCaptureState>,
    /// Back-pressured sink into the `served_response` section's BOUNDED writer
    /// channel (F1b review #1). `None` once the writer is gone (a section write
    /// error closed the channel) — capture then stops but the served stream
    /// continues byte-for-byte (a diagnostic failure must never break the served
    /// bytes). The transition to `None` also permanently marks the section
    /// `partial` (F1b review r2) — see `poll_frame`.
    served_sink: Option<crate::turn_capture::ServedSink>,
    /// Set once `inner` yields `Poll::Ready(None)` (clean end-of-stream).
    clean_eos: bool,
    /// Total DATA bytes forwarded so far (for the exact-length clean check).
    forwarded: u64,
    /// The inner body's exact promised length at construction (a non-streaming JSON
    /// body has `Some`; a chunked SSE stream has `None`). When `Some`, delivering
    /// that many bytes is a clean end even if hyper stops polling the
    /// Content-Length body before it yields `Ready(None)`.
    exact_len: Option<u64>,
}

impl http_body::Body for TeeBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        // `axum::body::Body` is `Unpin`, so the fields can be reached by `&mut`.
        let this = self.get_mut();

        // Bounded-memory back-pressure (F1b review #1): reserve a slot in the
        // served-section writer channel BEFORE pulling the next frame. If the disk
        // writer is behind, the BOUNDED channel is full → `poll_reserve` returns
        // `Pending` and we propagate it, throttling the served stream to the
        // writer's pace instead of piling the whole body into RAM. A closed channel
        // (writer gone) drops the sink; we then forward WITHOUT capture — a
        // diagnostic failure must never break or stall the served stream (AGENTS.md).
        // F1b review r2 (don't-lie-with-zeros): that also means every byte from
        // here on is missing the section, so mark the section degraded RIGHT NOW —
        // sticky, so a later clean end-of-stream can never report this capture
        // complete (`TurnCaptureState::mark_served_degraded`).
        if let Some(sink) = this.served_sink.as_mut() {
            match sink.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(_)) => {
                    this.served_sink = None;
                    this.state.mark_served_degraded();
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref()
                    && !data.is_empty()
                {
                    this.forwarded = this.forwarded.saturating_add(data.len() as u64);
                    // Send the COPY into the slot reserved above; the frame passes
                    // through to the client UNCHANGED (never a retained slice —
                    // AGENTS.md). A failed send just means the writer went away —
                    // mark the section degraded (F1b review r2; see above).
                    if let Some(sink) = this.served_sink.as_mut()
                        && sink.send(data.to_vec()).is_err()
                    {
                        this.served_sink = None;
                        this.state.mark_served_degraded();
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                this.clean_eos = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for TeeBody {
    fn drop(&mut self) {
        // Clean when we observed end-of-stream, OR forwarded the full promised
        // length (hyper can stop polling a Content-Length body before it ever
        // yields `Ready(None)`, so a complete non-streaming response would else be
        // mis-flagged partial). Otherwise the served stream was cut short (client
        // disconnect / mid-stream error) → partial. F1b review r2: this `clean`
        // check is about the CLIENT-facing stream only — if the section itself
        // was marked degraded mid-stream (`poll_frame`'s `mark_served_degraded`
        // calls, section write errors), `served_done`'s `close` cannot unset that
        // sticky mark no matter what we pass here, so a `served_sink` failure can
        // never be reported as a complete capture just because the client still
        // saw a clean end.
        let clean = self.clean_eos || self.exact_len.is_some_and(|len| self.forwarded >= len);
        self.state.served_done(!clean);
    }
}

/// Wrap `response`'s body in a [`TeeBody`] so its served bytes are captured to the
/// turn's `served_response` section. Status/headers are preserved; only the body
/// is wrapped, and `size_hint`/`is_end_stream` are forwarded so a non-streaming
/// response keeps its `Content-Length` and framing.
fn tee_served_body(
    response: Response,
    state: Arc<crate::turn_capture::TurnCaptureState>,
) -> Response {
    let (parts, body) = response.into_parts();
    let exact_len = http_body::Body::size_hint(&body).exact();
    // F1c (finding #2): record that the served tee is now installed BEFORE the
    // `MiddlewareCaptureGuard` served backstop can drop (it drops when `log_api_call`
    // returns, AFTER this runs). With the tee installed, the tee's own `Drop` owns
    // `served_done`; the backstop stays inert. Only a pre-tee unwind (this never
    // runs) leaves the flag false, so the backstop fires `served_done` to resolve the
    // barrier instead of leaking the turn.
    state.mark_served_tee_installed();
    // Take the back-pressured served sink BEFORE moving `state` into the tee.
    let served_sink = state.served_sink();
    let tee = TeeBody {
        inner: body,
        state,
        served_sink,
        clean_eos: false,
        forwarded: 0,
        exact_len,
    };
    Response::from_parts(parts, Body::new(tee))
}

fn stream_chat_completions_response(
    model: String,
    include_usage: bool,
    suppress_reasoning: bool,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> Response {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        let mut converter = ChatCompletionStreamConverter::with_reasoning_suppression(
            model,
            include_usage,
            suppress_reasoning,
        );
        let mut stream = std::pin::pin!(stream);
        'streaming: while let Some(event) = stream.next().await {
            let chat_events = converter.convert(&event);
            for chat_event in chat_events {
                if tx.send(chat_event).await.is_err() {
                    break 'streaming;
                }
            }
        }
    });

    let mapped = ReceiverStream::new(rx).map(|event| {
        Ok::<_, Infallible>(axum::response::sse::Event::default().data(event.to_sse_data()))
    });

    let mut response = Sse::new(mapped)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    response
}

fn stream_anthropic_response(
    model: String,
    suppress_reasoning: bool,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        let mut converter =
            AnthropicStreamConverter::with_reasoning_suppression(model, suppress_reasoning);
        let mut stream = std::pin::pin!(stream);
        while let Some(event) = stream.next().await {
            let anthropic_events = converter.convert(&event);
            for anthropic_event in anthropic_events {
                if tx.send(anthropic_event).await.is_err() {
                    return;
                }
            }
        }
        // The upstream event stream ended. If it never produced a
        // `response.completed` (engine error, dropped/stalled turn, aborted
        // web-search round-trip), emit a terminal `message_delta` +
        // `message_stop` so the client is not left hanging behind the SSE
        // keep-alive forever.
        for anthropic_event in converter.finalize() {
            if tx.send(anthropic_event).await.is_err() {
                return;
            }
        }
    });

    let mapped = ReceiverStream::new(rx).map(|event| {
        Ok::<_, Infallible>(
            axum::response::sse::Event::default()
                .event(event.sse_event_type())
                .data(event.to_json()),
        )
    });

    let mut response = Sse::new(mapped)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    Ok(response)
}

fn proxy_upstream_response(response: reqwest::Response) -> Response {
    let status = response.status();
    let upstream_headers = response.headers().clone();
    let mut builder = Response::builder().status(status);
    if let Some(headers) = builder.headers_mut() {
        copy_proxy_response_headers(&upstream_headers, headers);
    }
    builder
        .body(Body::from_stream(response.bytes_stream()))
        .expect("valid upstream proxy response")
}

fn copy_proxy_response_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for (name, value) in source {
        if should_proxy_response_header(name) {
            target.append(name.clone(), value.clone());
        }
    }
}

fn should_proxy_response_header(name: &HeaderName) -> bool {
    !is_hop_by_hop_header(name) && !header_name_eq(name, "content-length")
}

/// CR1.1: `engine.rs::created_event` stamps `estimated_input_tokens` onto the
/// canonical `response.created` event as an INTERNAL transport hint -- its
/// only reader is `AnthropicStreamConverter::handle_created`, which seeds
/// `message_start.usage.input_tokens` from it (the real upstream count isn't
/// known until `response.completed`, much later). Every OTHER egress
/// CONVERTS `response.created` into its own wire shape and never copies the
/// field across (Chat's `ChatCompletionStreamConverter` reads only `id`); but
/// this fn is a raw byte-forward of `event.data`, so without this strip a
/// `/v1/responses` streaming client would see a non-standard field OpenAI's
/// Responses API has no concept of, breaking the "Responses wire shape
/// unchanged" contract (a `deny_unknown_fields` consumer or exact-bytes
/// snapshot). Scoped to `response.created` only -- the only event that can
/// ever carry the field (`response.in_progress` reuses the same `ResponseStub`
/// struct but always passes `None`, which `skip_serializing_if` already
/// omits) -- so every other event is serialized untouched with no clone.
fn responses_wire_event_data(event: &crate::engine::SseEvent) -> String {
    if event.event != "response.created" {
        return event.data.to_string();
    }
    let mut data = event.data.clone();
    if let Some(response) = data.get_mut("response").and_then(Value::as_object_mut) {
        response.remove("estimated_input_tokens");
    }
    data.to_string()
}

fn stream_responses_response(stream: ReceiverStream<crate::engine::SseEvent>) -> Response {
    let mapped = stream.map(|event| {
        let data = responses_wire_event_data(&event);
        Ok::<_, Infallible>(
            axum::response::sse::Event::default()
                .event(event.event)
                .data(data),
        )
    });
    let mut response = Sse::new(mapped)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    response
}

async fn collect_responses_response(
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let mut final_payload: Option<Value> = None;
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        match event.event.as_str() {
            "response.completed" | "response.incomplete" => {
                final_payload = event.data.get("response").cloned();
            }
            "response.failed" => {
                let message = event
                    .data
                    .get("response")
                    .and_then(|response| response.get("error"))
                    .and_then(|error| error.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("upstream request failed");
                return Err(AppError::upstream(message));
            }
            _ => {}
        }
    }

    match final_payload {
        Some(payload) => Ok(Json(payload).into_response()),
        None => Err(AppError::upstream(
            "stream ended before a final response resource was emitted",
        )),
    }
}

async fn collect_chat_completions_response(
    model: String,
    suppress_reasoning: bool,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let mut collector =
        ChatCompletionCollector::with_reasoning_suppression(model, suppress_reasoning);
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        collector.process(&event);
    }
    Ok(Json(collector.into_response()?).into_response())
}

async fn collect_anthropic_response(
    model: String,
    suppress_reasoning: bool,
    stream: ReceiverStream<crate::engine::SseEvent>,
) -> AppResult<Response> {
    let mut collector =
        AnthropicStreamCollector::with_reasoning_suppression(model, suppress_reasoning);
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        collector.process(&event);
    }
    match collector.into_response() {
        Ok(msg) => Ok(Json(msg).into_response()),
        Err(err) => Ok(anthropic_error_response(AppError::upstream(err.message))),
    }
}

fn anthropic_error_response(err: AppError) -> Response {
    let status = err.status_code();
    let error_type = match err.status_code() {
        axum::http::StatusCode::BAD_REQUEST => "invalid_request_error",
        axum::http::StatusCode::CONFLICT => "invalid_request_error",
        axum::http::StatusCode::NOT_FOUND => "not_found_error",
        _ => "api_error",
    };
    let body = serde_json::json!({
        "type": "error",
        "error": {
            "type": error_type,
            "message": err.to_string(),
        }
    });
    (status, Json(body)).into_response()
}

#[derive(Debug, Default, Deserialize)]
struct ModelsListQuery {
    after_id: Option<String>,
    before_id: Option<String>,
    limit: Option<String>,
}

async fn get_models(
    headers: HeaderMap,
    Query(query): Query<ModelsListQuery>,
    State(gateway): State<Arc<Gateway>>,
) -> AppResult<Response> {
    let anthropic_models = is_anthropic_models_request(&headers);
    let response = gateway.upstream_client().list_models().await?;
    let (status, body, etag) = collect_models_response(response).await?;
    let body = if anthropic_models {
        transform_models_response_for_anthropic(body, &query, gateway.config())?
    } else {
        body
    };
    let mut headers = HeaderMap::new();
    if !anthropic_models && let Some(etag) = etag {
        headers.insert(
            http::header::ETAG,
            HeaderValue::from_str(&etag)
                .map_err(|err| AppError::internal(format!("invalid ETag header: {err}")))?,
        );
    }
    Ok((status, headers, Json(body)).into_response())
}

fn is_anthropic_models_request(headers: &HeaderMap) -> bool {
    headers.contains_key("anthropic-version") || headers.contains_key("anthropic-beta")
}

fn transform_models_response_for_anthropic(
    body: Value,
    query: &ModelsListQuery,
    config: &crate::config::Config,
) -> AppResult<Value> {
    if query.after_id.is_some() && query.before_id.is_some() {
        return Err(AppError::bad_request(
            "after_id and before_id cannot both be specified",
        ));
    }

    let limit = parse_anthropic_models_limit(query.limit.as_deref())?;
    let models = extract_model_entries(&body)
        .into_iter()
        .filter_map(|entry| anthropic_model_entry(&entry, config))
        .collect::<Vec<_>>();

    let (page, has_more) = page_anthropic_models(&models, query, limit)?;
    let first_id = page
        .first()
        .and_then(model_id_from_value)
        .map(Value::String)
        .unwrap_or(Value::Null);
    let last_id = page
        .last()
        .and_then(model_id_from_value)
        .map(Value::String)
        .unwrap_or(Value::Null);

    Ok(serde_json::json!({
        "data": page,
        "first_id": first_id,
        "has_more": has_more,
        "last_id": last_id,
    }))
}

fn parse_anthropic_models_limit(limit: Option<&str>) -> AppResult<usize> {
    match limit {
        Some(raw) => {
            let parsed = raw
                .parse::<usize>()
                .map_err(|_| AppError::bad_request("limit must be an integer from 1 to 1000"))?;
            if !(1..=1000).contains(&parsed) {
                return Err(AppError::bad_request("limit must be between 1 and 1000"));
            }
            Ok(parsed)
        }
        None => Ok(20),
    }
}

fn extract_model_entries(body: &Value) -> Vec<Value> {
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

fn anthropic_model_entry(entry: &Value, config: &crate::config::Config) -> Option<Value> {
    match entry {
        Value::String(id) => {
            let caps =
                merge_configured_capabilities(config, id, infer_capabilities_from_model_id(id));
            Some(build_anthropic_model_entry(
                id,
                id,
                UNKNOWN_MODEL_CREATED_AT,
                None,
                None,
                Some(&caps),
            ))
        }
        Value::Object(map) => {
            let id = map.get("id").and_then(Value::as_str)?;
            let display_name = map
                .get("display_name")
                .and_then(Value::as_str)
                .or_else(|| map.get("id").and_then(Value::as_str))
                .unwrap_or(id);
            let created_at =
                parse_created_at(map).unwrap_or_else(|| UNKNOWN_MODEL_CREATED_AT.to_string());

            let max_input_tokens = map
                .get("max_input_tokens")
                .or_else(|| map.get("context_length"))
                .or_else(|| map.get("context_window"))
                .or_else(|| map.get("max_context_length"))
                .or_else(|| map.get("max_model_len"));
            let max_tokens = map
                .get("max_tokens")
                .or_else(|| map.get("max_output_tokens"));
            let capabilities = map
                .get("capabilities")
                .filter(|value| value.is_object())
                .cloned()
                .unwrap_or_else(|| infer_capabilities_from_model_id(id));
            let capabilities = merge_configured_capabilities(config, id, capabilities);

            Some(build_anthropic_model_entry(
                id,
                display_name,
                &created_at,
                max_input_tokens,
                max_tokens,
                Some(&capabilities),
            ))
        }
        _ => None,
    }
}

/// Parse a creation timestamp from common upstream formats.
///
/// - `created_at` → ISO 8601 string (passed through)
/// - `created`    → Unix epoch integer / float (⇒ ISO 8601 string)
fn parse_created_at(map: &serde_json::Map<String, Value>) -> Option<String> {
    match map.get("created_at").and_then(Value::as_str) {
        Some(iso) => Some(iso.to_string()),
        None => {
            let epoch = map.get("created").and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_f64().and_then(|f| (f as u64).checked_add(0)))
            })?;
            chrono::DateTime::from_timestamp(epoch as i64, 0)
                .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        }
    }
}

fn infer_capabilities_from_model_id(_id: &str) -> Value {
    default_anthropic_model_capabilities()
}

fn merge_configured_capabilities(config: &crate::config::Config, id: &str, base: Value) -> Value {
    config
        .resolve_capabilities_for_upstream(id)
        .map_or(base.clone(), |capabilities| capabilities.merge_into(base))
}

fn build_anthropic_model_entry(
    id: &str,
    display_name: &str,
    created_at: &str,
    max_input_tokens: Option<&Value>,
    max_tokens: Option<&Value>,
    capabilities: Option<&Value>,
) -> Value {
    serde_json::json!({
        "id": id,
        "capabilities": capabilities
            .filter(|value| value.is_object())
            .cloned()
            .unwrap_or_else(default_anthropic_model_capabilities),
        "created_at": created_at,
        "display_name": display_name,
        "max_input_tokens": numeric_field_or_zero(max_input_tokens),
        "max_tokens": numeric_field_or_zero(max_tokens),
        "type": "model",
    })
}

fn numeric_field_or_zero(value: Option<&Value>) -> Value {
    value
        .and_then(Value::as_u64)
        .map(|number| serde_json::json!(number))
        .unwrap_or_else(|| serde_json::json!(0))
}

fn default_anthropic_model_capabilities() -> Value {
    let unsupported = || serde_json::json!({ "supported": false });
    serde_json::json!({
        "batch": unsupported(),
        "citations": unsupported(),
        "code_execution": unsupported(),
        "context_management": {
            "clear_thinking_20251015": unsupported(),
            "clear_tool_uses_20250919": unsupported(),
            "compact_20260112": unsupported(),
            "supported": false
        },
        "effort": {
            "high": unsupported(),
            "low": unsupported(),
            "max": unsupported(),
            "medium": unsupported(),
            "supported": false
        },
        "image_input": unsupported(),
        "pdf_input": unsupported(),
        "structured_outputs": unsupported(),
        "thinking": {
            "supported": false,
            "types": {
                "adaptive": unsupported(),
                "enabled": unsupported()
            }
        }
    })
}

fn page_anthropic_models(
    models: &[Value],
    query: &ModelsListQuery,
    limit: usize,
) -> AppResult<(Vec<Value>, bool)> {
    if let Some(before_id) = query.before_id.as_deref() {
        let end = model_index(models, before_id)?;
        let start = end.saturating_sub(limit);
        return Ok((models[start..end].to_vec(), start > 0));
    }

    let start = match query.after_id.as_deref() {
        Some(after_id) => model_index(models, after_id)? + 1,
        None => 0,
    };
    let end = (start + limit).min(models.len());
    Ok((models[start..end].to_vec(), end < models.len()))
}

fn model_index(models: &[Value], id: &str) -> AppResult<usize> {
    models
        .iter()
        .position(|model| model_id_from_value(model).as_deref() == Some(id))
        .ok_or_else(|| AppError::bad_request(format!("model cursor not found: {id}")))
}

fn model_id_from_value(model: &Value) -> Option<String> {
    model
        .as_object()
        .and_then(|map| map.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::body_log_fields;
    use super::responses_wire_event_data;
    use super::should_proxy_response_header;
    use axum::body::Bytes;
    use axum::http::HeaderName;
    use sha2::Digest as _;

    /// Finding 1: the inbound redaction produces IDENTICAL redacted output on the
    /// inline (small) and `spawn_blocking` (large) paths — a secret-bearing field and
    /// an image `data:` URI are BOTH redacted, with no raw secret/image bytes
    /// surviving, and the requested `model` is still extracted. The large body forces
    /// the off-worker path (`> TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES`); the small one
    /// stays inline. Both must round-trip to valid JSON with the same redaction.
    #[tokio::test]
    async fn offload_inbound_redacts_small_inline_and_large_spawn_blocking_identically() {
        use serde_json::Value;
        let body_with_filler = |filler: &str| {
            Bytes::from(format!(
                r#"{{"model":"claude-x","api_key":"sk-LEAK-INBOUND-9999","messages":[{{"role":"user","content":[{{"type":"image_url","image_url":{{"url":"data:image/png;base64,RAWINBOUNDIMG7777"}}}},{{"type":"text","text":"{filler}"}}]}}]}}"#
            ))
        };

        let small = body_with_filler("hi");
        assert!(
            small.len() <= super::TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES,
            "small body takes the inline path"
        );
        let (model_small, red_small, partial_small) =
            super::offload_redacted_inbound_section(small.clone()).await;
        assert!(!partial_small, "a clean inline redaction is not partial");

        let large = body_with_filler(&"x".repeat(32 * 1024));
        assert!(
            large.len() > super::TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES,
            "large body takes the spawn_blocking path"
        );
        let (model_large, red_large, partial_large) =
            super::offload_redacted_inbound_section(large).await;
        assert!(
            !partial_large,
            "a clean spawn_blocking redaction is not partial"
        );

        for (label, model, redacted) in [
            ("small/inline", model_small, red_small),
            ("large/spawn_blocking", model_large, red_large),
        ] {
            assert_eq!(
                model.as_deref(),
                Some("claude-x"),
                "{label}: model extracted"
            );
            let text = String::from_utf8(redacted).expect("redacted section is UTF-8");
            assert!(
                !text.contains("sk-LEAK-INBOUND-9999"),
                "{label}: secret value redacted"
            );
            assert!(
                text.contains("[redacted]"),
                "{label}: secret marker present"
            );
            assert!(
                !text.contains("RAWINBOUNDIMG7777"),
                "{label}: raw image bytes redacted"
            );
            assert!(
                text.contains("<redacted uri>"),
                "{label}: image URI marker present"
            );
            let value: Value = serde_json::from_str(&text).expect("redacted section is valid JSON");
            assert_eq!(value["model"], "claude-x", "{label}: structure intact");
        }
    }

    /// F1 (Fable-fix, BLOCKING): the large-body `spawn_blocking` offload must be
    /// handed an OWNED `Vec` copy, NOT the Arc-backed `Bytes` (which would PIN the
    /// 256 MiB inbound middleware backing for the task's lifetime — AGENTS.md line
    /// 144). A large body forces the off-worker path; the capture completes correctly
    /// AND the offload retains NO clone of the inbound `Bytes` backing after it returns
    /// (the observable half of the no-pin contract): a second handle to the same shared
    /// backing is uniquely reclaimable once the offload dropped its `Bytes` in favor of
    /// the owned copy.
    #[tokio::test]
    async fn offload_large_inbound_hands_owned_copy_not_pinned_bytes() {
        let large = Bytes::from(format!(
            r#"{{"model":"claude-x","api_key":"sk-LEAK-PIN-1234","messages":[{{"role":"user","content":"{}"}}]}}"#,
            "x".repeat(64 * 1024)
        ));
        assert!(
            large.len() > super::TURN_CAPTURE_INLINE_REDACT_LIMIT_BYTES,
            "body forces the spawn_blocking path"
        );
        // A SECOND handle to the SAME shared backing; if the offload retained a clone
        // (old bug: moved the `Bytes` into `spawn_blocking`) this could not reclaim it.
        let observer = large.clone();

        let (model, redacted, partial) = super::offload_redacted_inbound_section(large).await;
        assert_eq!(model.as_deref(), Some("claude-x"), "model extracted");
        assert!(!partial, "a clean spawn_blocking redaction is not partial");
        let text = String::from_utf8(redacted).expect("redacted section is UTF-8");
        assert!(
            !text.contains("sk-LEAK-PIN-1234"),
            "secret redacted on the offloaded path"
        );

        // The offload converted the body to an OWNED `Vec` for the blocking task and
        // dropped its `Bytes`, so nothing pins the shared backing: `observer` is now the
        // SOLE owner and reclaims uniquely (no retained slice of the inbound buffer).
        assert!(
            observer.try_into_mut().is_ok(),
            "offload retained no clone of the inbound Bytes backing"
        );
    }

    /// D7a R3 #1 (REGRESSION): a `/dashboard/login` body must yield NO
    /// body-derived log field. Emitting `body_sha256` + `body_bytes` (length) for
    /// the login body is an offline token-verification oracle — an attacker with
    /// the logs can brute-force the token against the digest and the known length.
    /// So `body_log_fields` returns `None`, and the request line that follows it
    /// carries NEITHER the token NOR its SHA-256 NOR the body length.
    #[test]
    fn auth_path_body_emits_no_body_derived_log_fields() {
        let token = "s3cret-login-token";
        let body = Bytes::from(format!(r#"{{"token":"{token}"}}"#));
        let token_sha = hex::encode(sha2::Sha256::digest(token.as_bytes()));
        let body_sha = hex::encode(sha2::Sha256::digest(&body));

        // The login endpoint suppresses every body-derived field.
        assert!(
            body_log_fields("/dashboard/login", &body).is_none(),
            "login body must produce no body-derived log fields (token oracle)"
        );
        // Logout is symmetric (bodyless, but the same path class).
        assert!(body_log_fields("/dashboard/logout", &Bytes::new()).is_none());

        // A normal inference path still logs the length + digest + summary, and
        // that digest is over the body (never resembles the bare-token digest).
        let normal =
            body_log_fields("/v1/messages", &body).expect("non-auth path logs body-derived fields");
        assert_eq!(normal.bytes, body.len());
        assert_eq!(normal.sha256, body_sha);
        // Sanity: the body digest is not the standalone token digest, so even the
        // normal path never logs a digest of the bare token.
        assert_ne!(normal.sha256, token_sha);
    }

    /// The full RFC 7230 §6.1 hop-by-hop set; must match the canonical list and
    /// the request-direction parity test in `upstream.rs`.
    const HOP_BY_HOP: [&str; 8] = [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ];

    #[test]
    fn response_direction_strips_full_hop_by_hop_set() {
        for header in HOP_BY_HOP {
            let name = HeaderName::from_bytes(header.as_bytes()).unwrap();
            assert!(
                !should_proxy_response_header(&name),
                "response proxy must strip hop-by-hop header {header}",
            );
        }
    }

    #[test]
    fn response_direction_strips_content_length() {
        let name = HeaderName::from_static("content-length");
        assert!(!should_proxy_response_header(&name));
    }

    #[test]
    fn response_direction_passes_representative_passthrough_header() {
        let name = HeaderName::from_static("content-type");
        assert!(should_proxy_response_header(&name));
    }

    /// CR1.1: `response.created` is the only event that can carry the
    /// internal `estimated_input_tokens` hint (`engine.rs::created_event`
    /// stamps it there for the Anthropic egress's `handle_created` to read);
    /// the raw-forward `/v1/responses` egress must strip it before it reaches
    /// the wire. The rest of the `response` object survives untouched, and a
    /// different event carrying an incidentally-named field is passed through
    /// byte-identical -- the strip is scoped to `response.created` only, not
    /// a blanket key filter.
    #[test]
    fn responses_wire_event_data_strips_estimate_from_created_only() {
        let created = crate::engine::SseEvent {
            event: "response.created".to_string(),
            data: serde_json::json!({
                "type": "response.created",
                "response": { "id": "resp_1", "estimated_input_tokens": 42 }
            }),
        };
        let stripped: serde_json::Value =
            serde_json::from_str(&responses_wire_event_data(&created)).expect("valid json");
        assert_eq!(stripped["response"]["id"], "resp_1");
        assert!(
            stripped["response"].get("estimated_input_tokens").is_none(),
            "estimated_input_tokens must be stripped from response.created: {stripped}"
        );

        let other = crate::engine::SseEvent {
            event: "response.in_progress".to_string(),
            data: serde_json::json!({
                "type": "response.in_progress",
                "response": { "id": "resp_1" }
            }),
        };
        assert_eq!(
            responses_wire_event_data(&other),
            other.data.to_string(),
            "non-created events must pass through untouched"
        );
    }

    /// Defensive: a `response.created` event with no `response` object at all
    /// (malformed/unexpected shape) must not panic -- it just serializes
    /// through unchanged.
    #[test]
    fn responses_wire_event_data_tolerates_missing_response_object() {
        let event = crate::engine::SseEvent {
            event: "response.created".to_string(),
            data: serde_json::json!({ "type": "response.created" }),
        };
        assert_eq!(responses_wire_event_data(&event), event.data.to_string());
    }
}
