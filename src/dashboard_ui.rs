//! Serve the React+TS+Vite dashboard SPA embedded into the binary at compile
//! time (D8), with the D7 auth gate.
//!
//! `build.rs` guarantees `$OUT_DIR/dashboard_dist/` exists (a node-less stub by
//! default, the real Vite `dist/` when built with `LLMCONDUIT_BUILD_DASHBOARD=1`),
//! so `include_dir!` always compiles. The `static DASHBOARD_DIST: Dir<'static>`
//! binding is REQUIRED — a bare `include_dir!(concat!(env!("OUT_DIR"), …))` does
//! not type-check.
//!
//! Routes (registered by `http.rs` only when `--with-debug-ui` is set AND the
//! D7 startup decision permits it):
//! - `GET /dashboard` → the SPA shell (`index.html`) when authenticated, with an
//!   injected bootstrap `<script>` (carrying the CSRF token + mutation flag) and
//!   a `llmconduit_csrf` cookie; a small **login shell** (token form) when not.
//!   The SPA is a hash router, so deep links live in the fragment and need no
//!   server-side rewrite.
//! - `GET /dashboard/assets/{*path}` → a static asset under `dist/assets/`, with
//!   `Content-Type` inferred from the extension; `404` for a missing path.
//!
//! ## CSP-safe bootstrap injection
//! The dashboard CSP is `script-src 'self'` (no `'unsafe-inline'`). The SPA's
//! own `<script src=…>` tags are covered by `'self'`; the ONLY inline script is
//! the server-injected bootstrap, which we authorize with a per-response
//! `'nonce-<n>'` added to `script-src`. The frontend reads
//! `window.__LLMCONDUIT_DASHBOARD__` for its CSRF token + mutation flag.

use crate::dashboard_auth::AuthSession;
use crate::dashboard_auth::CSRF_COOKIE;
use crate::dashboard_auth::DashboardAuth;
use crate::dashboard_auth::SESSION_TTL_SECS;
use axum::Extension;
use axum::extract::Path;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use include_dir::Dir;
use include_dir::include_dir;
use std::sync::Arc;
use uuid::Uuid;

/// The embedded dashboard build. Backed by `$OUT_DIR/dashboard_dist/`, which
/// `build.rs` always materializes (stub or real). The `Dir<'static>` type on
/// this `static` is what makes `include_dir!` type-check.
static DASHBOARD_DIST: Dir<'static> = include_dir!("$OUT_DIR/dashboard_dist");

/// Base CSP for `/dashboard` (the `script-src` gets a per-response nonce appended
/// for the bootstrap inline script). Matches the D7 spec exactly.
const DASHBOARD_CSP_BASE: &str = "default-src 'self'; script-src 'self'{NONCE}; \
     connect-src 'self' ws: wss:; style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
     object-src 'none'; base-uri 'self'; frame-ancestors 'none'";

/// The minimal login shell served to an UNauthenticated `/dashboard` client: a
/// token-entry form POSTing JSON to `/dashboard/login`, then reloading. All
/// scripting is via a nonce'd inline script (no external asset needed, so the
/// login page works even before the SPA assets load). Styling is inline
/// (`style-src 'unsafe-inline'`).
const LOGIN_SHELL_TEMPLATE: &str = include_str!("dashboard_login.html");

const GITHUB_LOGIN_SHELL_TEMPLATE: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>llmconduit dashboard - sign in</title>
  <style>
    :root { color-scheme: dark; --bg:#101214; --panel:#171a1f; --line:#303741; --text:#edf1f5; --muted:#9aa6b2; --blue:#6bb6ff; --red:#ff6b6b; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    * { box-sizing: border-box; }
    html, body { height: 100%; margin: 0; }
    body { background: var(--bg); color: var(--text); display: grid; place-items: center; }
    .card { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 28px 26px; width: min(360px, 90vw); box-shadow: 0 16px 40px rgba(0,0,0,.4); }
    h1 { font-size: 17px; margin: 0 0 4px; }
    p { color: var(--muted); font-size: 13px; margin: 0 0 20px; }
    a { display: flex; align-items: center; justify-content: center; width: 100%; background: var(--blue); color: #06121f; border-radius: 8px; padding: 11px 12px; font-size: 14px; font-weight: 650; text-decoration: none; }
    .error { margin-bottom: 14px; color: var(--red); font-size: 13px; min-height: 16px; }
  </style>
</head>
<body>
  <main class="card">
    <h1>llmconduit dashboard</h1>
    <p>Sign in with an approved GitHub account.</p>
    <div class="error" id="error" role="alert"></div>
    <a href="/dashboard/auth/github/start">Continue with GitHub</a>
  </main>
  <script nonce="{NONCE}">
    const reason = new URLSearchParams(window.location.search).get("login_error");
    const messages = {
      github_cancelled: "GitHub sign-in was cancelled.",
      github_configuration: "GitHub SSO is not configured on this server.",
      github_state: "The GitHub sign-in request expired or could not be verified.",
      github_exchange: "GitHub could not complete the sign-in exchange.",
      github_profile: "The GitHub profile could not be loaded.",
      github_denied: "This GitHub account is not allowed to use the dashboard."
    };
    if (reason) document.getElementById("error").textContent = messages[reason] || "GitHub sign-in failed.";
  </script>
</body>
</html>"#;

/// `GET /dashboard` — auth-aware shell. Authenticated → the embedded SPA with an
/// injected bootstrap script + a refreshed CSRF cookie. Unauthenticated → the
/// login shell. Always carries the dashboard CSP + security headers + `no-store`
/// (transcripts/credentials must not be cached).
#[utoipa::path(
    get,
    path = "/dashboard",
    tag = "ui",
    operation_id = "dashboard_index",
    responses(
        (status = 200, description = "HTML. With a valid session (`llmconduit_session` cookie, bearer token, or dev-open): the embedded SPA `index.html` with a nonce-bearing bootstrap script element setting `window.__LLMCONDUIT_DASHBOARD__ = {authenticated: true, csrf_token, mutations_enabled, user, auth_mode}` and a freshly issued `llmconduit_csrf` cookie. Without one: the login shell (token/username form posting to `/dashboard/login`) — never a 401. Both carry the dashboard CSP (per-response `script-src` nonce), `X-Frame-Options: DENY`, `nosniff`, `no-referrer` and `Cache-Control: no-store`.", content_type = "text/html", body = String),
        (status = 500, description = "Authenticated, but the embedded dashboard build has no `index.html`. Plain text `dashboard index.html missing from embedded build`.", content_type = "text/plain", body = String),
    )
)]
pub async fn dashboard_index(
    axum::extract::State(gateway): axum::extract::State<Arc<crate::engine::Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    session: Option<AuthSession>,
) -> Response {
    let nonce = new_nonce();
    let auth_mode = crate::accounts_api::auth_mode(&gateway, &auth).await;
    match session {
        Some(session) => serve_authenticated_shell(&auth, &nonce, session.user.as_ref(), auth_mode),
        None => serve_login_shell(&auth, &nonce, auth_mode),
    }
}

/// Build the authenticated SPA response: inject the bootstrap script into
/// `index.html`, set a fresh `llmconduit_csrf` cookie, and stamp the CSP +
/// headers.
fn serve_authenticated_shell(
    auth: &DashboardAuth,
    nonce: &str,
    user: Option<&crate::accounts::SessionUser>,
    auth_mode: &str,
) -> Response {
    let Some(file) = DASHBOARD_DIST.get_file("index.html") else {
        return security_headers(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "dashboard index.html missing from embedded build",
            )
                .into_response(),
            None,
        );
    };
    let csrf = auth.issue_csrf_token();
    let user_json = serde_json::to_string(&user).unwrap_or_else(|_| "null".to_string());
    let bootstrap = format!(
        "<script nonce=\"{nonce}\">window.__LLMCONDUIT_DASHBOARD__={{\"authenticated\":true,\
         \"csrf_token\":{csrf},\"mutations_enabled\":{mutations},\"user\":{user_json},\
         \"auth_mode\":{mode}}};</script>",
        csrf = json_string(&csrf),
        mutations = auth.mutations_enabled(),
        mode = json_string(auth_mode),
    );
    let html = inject_before_head_close(&String::from_utf8_lossy(file.contents()), &bootstrap);

    let mut response = ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response();
    let secure = auth.secure_cookies();
    if let Ok(cookie) = HeaderValue::from_str(&csrf_cookie(&csrf, secure)) {
        response.headers_mut().append(header::SET_COOKIE, cookie);
    }
    security_headers(response, Some(nonce))
}

/// Build the login-shell response (unauthenticated `/dashboard`).
fn serve_login_shell(auth: &DashboardAuth, nonce: &str, auth_mode: &str) -> Response {
    let template = if auth.github_sso_enabled() {
        GITHUB_LOGIN_SHELL_TEMPLATE
    } else {
        LOGIN_SHELL_TEMPLATE
    };
    let html = template
        .replace("{NONCE}", nonce)
        .replace("{AUTH_MODE}", auth_mode);
    let response = ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response();
    security_headers(response, Some(nonce))
}

/// `GET /dashboard/assets/{*path}` — serve a static asset from `dist/assets/`.
/// The captured `path` is the portion AFTER `assets/`; we look it up under
/// `assets/` in the embedded tree and 404 if absent. Carries the security
/// headers (no CSP needed on a sub-resource, but `nosniff`/`no-referrer`/
/// frame-deny still apply) but NOT `no-store` — hashed Vite assets are
/// immutable and may be cached.
#[utoipa::path(
    get,
    path = "/dashboard/assets/{path}",
    tag = "ui",
    operation_id = "dashboard_asset",
    params(
        ("path" = String, Path, description = "Wildcard (axum `{*path}`): the file path below the embedded `assets/` directory, e.g. `index-DEADBEEF.js`; may contain `/`."),
    ),
    responses(
        (status = 200, description = "The embedded file bytes. `Content-Type` is derived from the extension (html, js/mjs, css, json/map, svg, png, jpg/jpeg, gif, webp, ico, woff2, woff, ttf, txt, wasm; anything else `application/octet-stream`). No session required; `X-Frame-Options: DENY`, `nosniff`, `no-referrer`, but no `Cache-Control: no-store` (hashed assets are immutable)."),
        (status = 404, description = "No embedded file at that path. Plain text `asset not found`.", content_type = "text/plain", body = String),
    )
)]
pub async fn dashboard_asset(Path(path): Path<String>) -> Response {
    let asset_path = format!("assets/{path}");
    match DASHBOARD_DIST.get_file(&asset_path) {
        Some(file) => asset_security_headers(serve_file(&asset_path, file.contents())),
        None => asset_security_headers((StatusCode::NOT_FOUND, "asset not found").into_response()),
    }
}

/// Apply the dashboard CSP (with the bootstrap nonce when `nonce` is `Some`) plus
/// `X-Frame-Options: DENY`, `nosniff`, `no-referrer`, and `Cache-Control: no-store`.
fn security_headers(mut response: Response, nonce: Option<&str>) -> Response {
    let headers = response.headers_mut();
    let csp = match nonce {
        Some(nonce) => DASHBOARD_CSP_BASE.replace("{NONCE}", &format!(" 'nonce-{nonce}'")),
        None => DASHBOARD_CSP_BASE.replace("{NONCE}", ""),
    };
    if let Ok(value) = HeaderValue::from_str(&csp) {
        headers.insert(header::CONTENT_SECURITY_POLICY, value);
    }
    apply_common_security_headers(headers);
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// Static-asset variant: the common hardening headers, no CSP, no `no-store`.
fn asset_security_headers(mut response: Response) -> Response {
    apply_common_security_headers(response.headers_mut());
    response
}

fn apply_common_security_headers(headers: &mut HeaderMap) {
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
}

/// Build the non-`HttpOnly` double-submit CSRF cookie (mirrors
/// `dashboard_auth`'s policy; duplicated here only because the shell sets a
/// FRESH token per page-load while `dashboard_auth` owns the login-time one).
fn csrf_cookie(value: &str, secure: bool) -> String {
    let mut cookie =
        format!("{CSRF_COOKIE}={value}; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}");
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

/// Insert `snippet` immediately before the first `</head>` (case-insensitive),
/// falling back to prepending it if the document has no head close tag (the
/// node-less stub `index.html` may be minimal).
fn inject_before_head_close(html: &str, snippet: &str) -> String {
    if let Some(idx) = find_ci(html, "</head>") {
        let mut out = String::with_capacity(html.len() + snippet.len());
        out.push_str(&html[..idx]);
        out.push_str(snippet);
        out.push_str(&html[idx..]);
        out
    } else {
        format!("{snippet}{html}")
    }
}

/// Case-insensitive search for `needle` in `haystack`, returning the byte index.
fn find_ci(haystack: &str, needle: &str) -> Option<usize> {
    let hay = haystack.to_ascii_lowercase();
    let need = needle.to_ascii_lowercase();
    hay.find(&need)
}

/// Serialize a string as a JSON string literal (quotes + escaping) so the
/// bootstrap object is valid JS even if the token ever contained a quote.
fn json_string(value: &str) -> String {
    serde_json::Value::String(value.to_string()).to_string()
}

/// A fresh random nonce for the per-response CSP `script-src`.
fn new_nonce() -> String {
    Uuid::new_v4().simple().to_string()
}

/// Path (relative to `DASHBOARD_DIST`, e.g. `assets/index-DEADBEEF.js`) of the
/// first file embedded under `assets/`, or `None` if that directory is empty.
///
/// Test-support: lets the `tests/` integration suite exercise the
/// `/dashboard/assets/{*path}` route against an asset that is REALLY embedded in
/// the current build, instead of hard-coding a name. The node-less stub embeds
/// `assets/stub.txt`, while a real `LLMCONDUIT_BUILD_DASHBOARD=1` build embeds
/// content-hashed Vite assets whose names are unknowable at source-edit time, so
/// the same test stays green under BOTH build modes. Not `#[cfg(test)]` because
/// integration tests link the library compiled WITHOUT `cfg(test)`; `doc(hidden)`
/// keeps it out of the public API surface. The captured `{*path}` is the portion
/// after `assets/`, so callers strip that prefix before requesting.
#[doc(hidden)]
pub fn first_embedded_asset_path() -> Option<String> {
    DASHBOARD_DIST
        .get_dir("assets")
        .and_then(|assets| assets.files().next())
        .map(|file| file.path().to_string_lossy().into_owned())
}

/// Build a `200 OK` body for an embedded file, tagging `Content-Type` from the
/// path's extension (falling back to `application/octet-stream`).
fn serve_file(path: &str, contents: &'static [u8]) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static(content_type_for(path)),
        )],
        contents,
    )
        .into_response()
}

/// Map a file extension to a `Content-Type`. Covers the asset kinds Vite emits
/// for this SPA (JS/CSS/HTML, source maps, fonts, images); anything else is
/// served as `application/octet-stream`.
fn content_type_for(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().filter(|ext| *ext != path);
    match ext {
        Some("html") => "text/html; charset=utf-8",
        Some("js") | Some("mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") | Some("map") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("ttf") => "font/ttf",
        Some("txt") => "text/plain; charset=utf-8",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::content_type_for;

    #[test]
    fn maps_known_vite_asset_extensions() {
        assert_eq!(content_type_for("index.html"), "text/html; charset=utf-8");
        assert_eq!(
            content_type_for("assets/index-DEADBEEF.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(
            content_type_for("assets/index-DEADBEEF.css"),
            "text/css; charset=utf-8"
        );
        assert_eq!(content_type_for("assets/logo.svg"), "image/svg+xml");
        assert_eq!(content_type_for("assets/font.woff2"), "font/woff2");
    }

    #[test]
    fn unknown_and_extensionless_paths_are_octet_stream() {
        assert_eq!(
            content_type_for("assets/data.bin"),
            "application/octet-stream"
        );
        // No extension: `rsplit('.')` yields the whole string, which we reject.
        assert_eq!(content_type_for("noext"), "application/octet-stream");
    }
}
