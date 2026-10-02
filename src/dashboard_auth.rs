//! Dashboard + `/debug` access control (D7, stage D7a).
//!
//! This module owns the security foundation for the optional debug UI and the
//! embedded dashboard SPA: env-only secrets, a stateless HMAC-SHA256 signed
//! session cookie, the login/logout handlers, the auth middleware (applied to
//! `/debug`, `/debug/ws`, `/dashboard`, and — later — `/dashboard/api/*`), a
//! WebSocket auth helper (signed cookie + `Origin` allow-list + cookie-`exp`
//! used to close the socket at expiry), and the `MutationPolicy` + CSRF
//! double-submit primitives that D6/D13 consume when wiring the kill route.
//!
//! ## Why secrets are env-only
//! The persisted [`crate::config::Config`] derives `Debug + Clone` and is
//! cloned into every [`crate::engine::Gateway`]. Putting the dashboard token /
//! session-signing key on it would risk leaking them through a `Debug` dump or
//! a config round-trip. Instead [`DashboardAuth`] is built from the process
//! environment ONCE at startup, stored behind an `Arc` in the router's
//! extension layer (NOT on `Config`), and given a hand-written [`std::fmt::Debug`]
//! that never prints the signing key or the token.
//!
//! ## Stateless signed cookie (rotation caveat)
//! The session cookie is `base64url(HMAC-SHA256(key, "{exp}:{nonce}")) +
//! "." + "{exp}:{nonce}"`. Verification recomputes the MAC and compares it in
//! constant time, then checks `exp` has not passed. There is no server-side
//! session table, so: (a) a leaked cookie is valid until its `exp` (≤ 24 h);
//! (b) rotating `LLMCONDUIT_DASHBOARD_SESSION_KEY` invalidates ALL live
//! sessions. Both are documented trade-offs; revocable sessions are future
//! work.
//!
//! ## D7b (deferred)
//! The batched `DashboardFrame` WS envelope, its `DashboardPayload` arms, the
//! `/dashboard/ws` route, and per-domain dedup are NOT in this stage — they
//! depend on the D3/D4/D5 payload types that do not exist yet. See the `// D7b:`
//! markers in `http.rs`/`debug_ui.rs` for the plug-in points.

use axum::Extension;
use axum::Json;
use axum::extract::FromRequestParts;
use axum::extract::OptionalFromRequestParts;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::Html;
use axum::response::IntoResponse;
use axum::response::Redirect;
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::Hmac;
use hmac::Mac;
use serde::Deserialize;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use subtle::ConstantTimeEq;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

/// Session lifetime: the cookie `Max-Age` AND the signed-payload `exp` window.
/// 24 hours bound how long a copied/leaked cookie stays valid (stateless — no
/// server-side revocation).
pub const SESSION_TTL_SECS: u64 = 86_400;

/// Session cookie name (signed, `HttpOnly`).
pub const SESSION_COOKIE: &str = "llmconduit_session";
/// Double-submit CSRF cookie name (NON-`HttpOnly` so the SPA can echo it in the
/// `X-CSRF-Token` header).
pub const CSRF_COOKIE: &str = "llmconduit_csrf";
/// Header carrying the double-submit CSRF token on a mutation request.
pub const CSRF_HEADER: &str = "x-csrf-token";
const GITHUB_OAUTH_COOKIE: &str = "llmconduit_github_oauth";
const GITHUB_OAUTH_TTL_SECS: u64 = 600;

/// Minimum decoded length (bytes) for `LLMCONDUIT_DASHBOARD_SESSION_KEY`.
const MIN_SESSION_KEY_BYTES: usize = 32;

// ---------------------------------------------------------------------------
// Env var names (single authority)
// ---------------------------------------------------------------------------

const ENV_TOKEN: &str = "LLMCONDUIT_DASHBOARD_TOKEN";
const ENV_SESSION_KEY: &str = "LLMCONDUIT_DASHBOARD_SESSION_KEY";
const ENV_PUBLIC_ORIGIN: &str = "LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN";
const ENV_ALLOW_INSECURE: &str = "LLMCONDUIT_ALLOW_INSECURE_DASHBOARD";
const ENV_ALLOW_MUTATIONS: &str = "LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS";
const ENV_GITHUB_CLIENT_ID: &str = "LLMCONDUIT_GITHUB_CLIENT_ID";
const ENV_GITHUB_CLIENT_SECRET: &str = "LLMCONDUIT_GITHUB_CLIENT_SECRET";
const ENV_GITHUB_CALLBACK_URL: &str = "LLMCONDUIT_GITHUB_CALLBACK_URL";
const ENV_GITHUB_ALLOWED_USERS: &str = "LLMCONDUIT_GITHUB_ALLOWED_USERS";
const ENV_GITHUB_ADMIN_USERS: &str = "LLMCONDUIT_GITHUB_ADMIN_USERS";

// ---------------------------------------------------------------------------
// Environment snapshot (so loading + the startup decision are unit-testable
// without touching the real process environment)
// ---------------------------------------------------------------------------

/// A read-only snapshot of the dashboard-relevant environment. Taking the env
/// as data (rather than reading `std::env` inline) lets every loading/startup
/// decision be exercised deterministically in tests.
#[derive(Clone, Default)]
pub struct DashboardEnv {
    pub token: Option<String>,
    pub session_key_b64: Option<String>,
    pub public_origin: Option<String>,
    pub allow_insecure: bool,
    pub allow_mutations: bool,
    pub github_client_id: Option<String>,
    pub github_client_secret: Option<String>,
    pub github_callback_url: Option<String>,
    pub github_allowed_users: Option<String>,
    pub github_admin_users: Option<String>,
}

impl std::fmt::Debug for DashboardEnv {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DashboardEnv")
            .field("token", &self.token.as_ref().map(|_| "[redacted]"))
            .field(
                "session_key_b64",
                &self.session_key_b64.as_ref().map(|_| "[redacted]"),
            )
            .field("public_origin", &self.public_origin)
            .field("allow_insecure", &self.allow_insecure)
            .field("allow_mutations", &self.allow_mutations)
            .field("github_sso_configured", &self.github_sso_configured())
            .finish()
    }
}

impl DashboardEnv {
    /// Read the dashboard env vars from the live process environment.
    pub fn from_process_env() -> Self {
        let read = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        Self {
            token: read(ENV_TOKEN),
            session_key_b64: read(ENV_SESSION_KEY),
            public_origin: read(ENV_PUBLIC_ORIGIN),
            allow_insecure: env_flag(ENV_ALLOW_INSECURE),
            allow_mutations: env_flag(ENV_ALLOW_MUTATIONS),
            github_client_id: read(ENV_GITHUB_CLIENT_ID),
            github_client_secret: read(ENV_GITHUB_CLIENT_SECRET),
            github_callback_url: read(ENV_GITHUB_CALLBACK_URL),
            github_allowed_users: read(ENV_GITHUB_ALLOWED_USERS),
            github_admin_users: read(ENV_GITHUB_ADMIN_USERS),
        }
    }

    pub fn github_sso_configured(&self) -> bool {
        self.github_client_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .github_client_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .github_allowed_users
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn github_sso_partially_configured(&self) -> bool {
        [
            self.github_client_id.as_deref(),
            self.github_client_secret.as_deref(),
            self.github_callback_url.as_deref(),
            self.github_allowed_users.as_deref(),
            self.github_admin_users.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|value| !value.trim().is_empty())
    }
}

/// A boolean env flag is true only for the explicit affirmative values `1`/
/// `true`/`yes` (case-insensitive). Anything else — including unset — is false,
/// so the secure default (mutations off, TLS required) holds unless explicitly
/// overridden.
fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

// ---------------------------------------------------------------------------
// Origin validation
// ---------------------------------------------------------------------------

/// A validated public origin (`https://host[:port]`, no path/query/fragment).
/// Stored as the exact normalized string we match `Origin` headers against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicOrigin(String);

impl PublicOrigin {
    /// Parse and validate a configured public origin. It MUST be an absolute URL
    /// with a host and no path (`scheme://host[:port]`). The scheme MUST be
    /// `https`, EXCEPT that `http` is accepted when `allow_insecure` is set (the
    /// `LLMCONDUIT_ALLOW_INSECURE_DASHBOARD` override — D7a R2 #2: an air-gapped
    /// non-loopback LAN running plaintext still needs an EXACT origin to match
    /// against, rather than falling back to the attacker-controllable `Host`).
    /// Returns the normalized `scheme://host[:port]` string on success.
    pub fn parse(raw: &str, allow_insecure: bool) -> Result<Self, String> {
        // D7a R4: NEVER interpolate the raw configured value into an error. A
        // malformed origin may embed credentials or a query token (e.g.
        // `https://user:pw@h` or `https://h?token=...`); echoing it into a logged
        // / returned error would leak that secret. Every message below is generic
        // (the env-var name + what was wrong), with no part of `raw` and no
        // `url::ParseError` rendering (which can include the offending substring).
        let url = url::Url::parse(raw.trim())
            .map_err(|_| format!("{ENV_PUBLIC_ORIGIN} is not a valid origin URL"))?;
        let scheme = url.scheme();
        let scheme_ok = scheme == "https" || (allow_insecure && scheme == "http");
        if !scheme_ok {
            return Err(if allow_insecure {
                format!("{ENV_PUBLIC_ORIGIN} must use http:// or https://")
            } else {
                format!("{ENV_PUBLIC_ORIGIN} must use https://")
            });
        }
        let host = url
            .host_str()
            .ok_or_else(|| format!("{ENV_PUBLIC_ORIGIN} has no host"))?;
        if url.path() != "/" && !url.path().is_empty() {
            return Err(format!(
                "{ENV_PUBLIC_ORIGIN} must be an origin only (no path)"
            ));
        }
        // D7a R3 #5: an origin is scheme + host(+port) ONLY. Reject (rather than
        // silently discard) any query, fragment, or userinfo/credentials — these
        // never appear in an `Origin` header, so accepting them would let a
        // configured value that looks meaningful (e.g. `https://h?x` or
        // `https://user:pw@h`) normalize away its extra parts and match an origin
        // the operator did not intend. The error omits the raw value (D7a R4): a
        // query/userinfo origin is exactly where a token/credential would hide.
        if url.query().is_some() {
            return Err(format!(
                "{ENV_PUBLIC_ORIGIN} must be an origin only (no query)"
            ));
        }
        if url.fragment().is_some() {
            return Err(format!(
                "{ENV_PUBLIC_ORIGIN} must be an origin only (no fragment)"
            ));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(format!(
                "{ENV_PUBLIC_ORIGIN} must be an origin only (no userinfo/credentials)"
            ));
        }
        // Re-serialize as a bare origin so an `Origin` header (which never has a
        // trailing slash) compares byte-for-byte. The scheme is preserved so an
        // `http` override origin matches an `http` `Origin` exactly.
        let normalized = match url.port() {
            Some(port) => format!("{scheme}://{host}:{port}"),
            None => format!("{scheme}://{host}"),
        };
        Ok(Self(normalized))
    }

    /// The normalized `https://host[:port]` string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this origin's scheme is `https`. Drives the `Secure` cookie
    /// attribute: a `Secure` cookie is silently DISCARDED by the browser over a
    /// plaintext `http` connection, so an `http` insecure-override origin (D7a R2
    /// #2) must set its session/CSRF cookies WITHOUT `Secure` or login can never
    /// complete in insecure-LAN mode.
    pub fn is_https(&self) -> bool {
        self.0.starts_with("https://")
    }
}

// ---------------------------------------------------------------------------
// DashboardAuth — built from env at startup, shared as Arc state
// ---------------------------------------------------------------------------

/// The dashboard/`/debug` auth context: the bearer token, the HMAC signing key,
/// the public-origin allow-list entry, and the mutation toggle. Built once from
/// [`DashboardEnv`] and shared (behind `Arc`) as an Axum extension on the
/// protected routes. NEVER stored on the persisted `Config`.
pub struct DashboardAuth {
    /// Whether a bearer/login token is configured. `false` in explicit dev-open
    /// mode: either a loopback listener, or a non-loopback listener whose operator
    /// deliberately set `LLMCONDUIT_ALLOW_INSECURE_DASHBOARD=1`. The token itself
    /// is NOT retained in cleartext; only its digest ([`Self::token_digest`]) is
    /// kept for the comparison.
    /// Whether requests require authentication. This is true with either the
    /// legacy bearer token or a complete GitHub SSO configuration.
    has_token: bool,
    /// Precomputed SHA-256 digest of the configured token (D7a R3 #4). Hashing the
    /// configured token ONCE at construction — rather than on every
    /// `verify_token` call — makes the per-request comparison run over two
    /// fixed-width digests with NO secret-length-dependent work: only the
    /// *presented* token is hashed per call, then the two 32-byte digests are
    /// compared in constant time. `None` in dev-open (no token) mode.
    token_digest: Option<[u8; 32]>,
    /// HMAC-SHA256 signing key for the session cookie (≥ 32 bytes). Decoded from
    /// base64 once; never logged, never `Debug`-printed.
    session_key: Vec<u8>,
    /// The validated public origin, when configured. Drives the `Secure` cookie
    /// attribute and the WS `Origin` allow-list.
    public_origin: Option<PublicOrigin>,
    /// Whether the server is bound to a loopback address.
    loopback: bool,
    /// Explicit operator opt-in to plaintext/dev-open behavior off loopback. In
    /// tokenless mode this also permits a same-origin `Origin`/`Host` WS fallback,
    /// so the dashboard works when reached directly by IP or through a proxy.
    allow_insecure: bool,
    /// Whether mutating dashboard routes (the D6 kill route) may proceed.
    /// Default off → mutations are 403.
    allow_mutations: bool,
    /// Validated GitHub OAuth configuration, when SSO is enabled. Secrets stay
    /// env-only and are redacted from Debug.
    github_oauth: Option<GithubOAuthConfig>,
}

impl std::fmt::Debug for DashboardAuth {
    /// Redacts the signing key and token so a `{:?}` dump never leaks secrets.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DashboardAuth")
            .field("token", &self.has_token.then_some("[redacted]"))
            .field("session_key", &"[redacted]")
            .field(
                "public_origin",
                &self.public_origin.as_ref().map(PublicOrigin::as_str),
            )
            .field("loopback", &self.loopback)
            .field("allow_insecure", &self.allow_insecure)
            .field("allow_mutations", &self.allow_mutations)
            .field(
                "github_oauth",
                &self.github_oauth.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

/// Outcome of building [`DashboardAuth`] from the environment, distinguishing a
/// hard configuration error from a dev-mode concession that should be logged.
/// `Debug` is safe: the inner [`DashboardAuth`] redacts its secrets.
#[derive(Debug)]
pub struct DashboardAuthBuild {
    pub auth: Arc<DashboardAuth>,
    /// Human-readable warnings to log at startup (e.g. an auto-generated key, or
    /// a tokenless loopback dev server). Never contains secret material.
    pub warnings: Vec<String>,
}

impl DashboardAuth {
    /// Build the auth context from an env snapshot for a server binding to
    /// `bind_addr`.
    ///
    /// Rules (mirrors the spec):
    /// - `LLMCONDUIT_DASHBOARD_SESSION_KEY`: must decode to ≥ 32 bytes when set.
    ///   It may be auto-generated in tokenless dev-open mode; authenticated
    ///   non-loopback deployments require an explicit stable key.
    /// - `LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN`: must be a valid `https://` origin
    ///   when set; required on a non-loopback bind (unless insecure override).
    /// - `LLMCONDUIT_DASHBOARD_TOKEN`: required on a non-loopback bind unless the
    ///   explicit insecure override is set. With no token, `dev_open`
    ///   authenticates every request.
    ///
    /// Returns `Err` for malformed values or for a non-loopback configuration
    /// that is neither fully authenticated nor explicitly opted into insecure
    /// dev-open mode.
    pub fn from_env(
        bind_addr: SocketAddr,
        env: &DashboardEnv,
    ) -> Result<DashboardAuthBuild, String> {
        let loopback = bind_addr.ip().is_loopback();
        let mut warnings = Vec::new();

        // D7a R3 #6: normalize the token here (trim; empty/whitespace-only → None)
        // so a directly-constructed `DashboardEnv { token: Some("") }` /
        // `Some("   ")` is treated as tokenless and therefore rejected on a
        // non-loopback bind unless the explicit insecure override is present,
        // rather than silently becoming a blank-token dashboard.
        // (`from_process_env` already trims/empties, but the constructor must not
        // rely on that for a hand-built env.)
        let token = env
            .token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let github_sso = env.github_sso_configured();
        let authentication_required = token.is_some() || github_sso;

        // Tokenless non-loopback access must always be an explicit operator
        // decision. The insecure override is intentionally loud and is the only
        // way to construct this fully unauthenticated state off loopback.
        if !loopback && !authentication_required && !env.allow_insecure {
            return Err(format!(
                "GitHub SSO or {ENV_TOKEN} is required on a non-loopback bind; an \
                 unauthenticated dashboard requires {ENV_ALLOW_INSECURE}=1"
            ));
        }

        let session_key = match env.session_key_b64.as_deref() {
            Some(encoded) => decode_session_key(encoded)?,
            None if loopback || (env.allow_insecure && !authentication_required) => {
                // Dev-open mode does not use login sessions, but the auth context
                // still needs a key for its uniform cookie/CSRF implementation.
                warnings.push(format!(
                    "{ENV_SESSION_KEY} not set; generated a temporary dev-open signing key \
                     (sessions reset on restart; the key is never logged)"
                ));
                generate_session_key()
            }
            None => {
                // An authenticated non-loopback bind needs a stable operator key.
                // Tokenless explicit dev-open mode was handled by the prior arm.
                return Err(format!(
                    "{ENV_SESSION_KEY} is required on a non-loopback bind (>= {MIN_SESSION_KEY_BYTES} \
                     decoded bytes of base64); authenticated deployments need a stable key"
                ));
            }
        };

        // An `http://` origin is accepted ONLY under the insecure override.
        // Tokenless explicit dev-open mode may omit it and uses Origin == Host;
        // authenticated off-loopback mode still requires an exact origin.
        let public_origin = match env.public_origin.as_deref() {
            Some(raw) => Some(PublicOrigin::parse(raw, env.allow_insecure)?),
            None => None,
        };
        let github_oauth =
            GithubOAuthConfig::from_env(env, public_origin.as_ref(), env.allow_insecure, loopback)?;

        // An authenticated non-loopback override still requires an exact origin:
        // unlike dev-open mode, it carries session cookies that a cross-site WS
        // must never be able to ride.
        if !loopback && env.allow_insecure && authentication_required && public_origin.is_none() {
            return Err(format!(
                "{ENV_ALLOW_INSECURE}=1 on a non-loopback bind requires an explicit \
                 {ENV_PUBLIC_ORIGIN} (http:// is allowed under this override): the Host header is \
                 attacker-controllable off loopback, so without an exact origin the dashboard \
                 WebSocket cannot validate cross-site upgrades and the exact-origin requirement \
                 cannot be met"
            ));
        }

        // D7a R3 #4: precompute the (normalized) configured token's SHA-256 digest
        // once so the per-request `verify_token` hashes only the *presented*
        // token, never the secret — its runtime no longer depends on the
        // configured token length.
        let token_digest = token.map(|token| <[u8; 32]>::from(Sha256::digest(token.as_bytes())));

        Ok(DashboardAuthBuild {
            auth: Arc::new(Self {
                has_token: authentication_required,
                token_digest,
                session_key,
                public_origin,
                loopback,
                allow_insecure: env.allow_insecure,
                allow_mutations: env.allow_mutations,
                github_oauth,
            }),
            warnings,
        })
    }

    /// Whether the server should send the `Secure` cookie attribute. Derived
    /// from the VALIDATED public-origin SCHEME, not merely its presence: an
    /// `https` origin → `Secure`; an `http` origin (only reachable under the
    /// `LLMCONDUIT_ALLOW_INSECURE_DASHBOARD` override — D7a R4) → NOT `Secure`,
    /// because a browser silently DISCARDS a `Secure` cookie received over a
    /// plaintext `http` connection, so a `Secure` login cookie could never be
    /// stored and login would always fail in insecure-LAN mode. With no
    /// configured origin (loopback dev over plain HTTP) there is likewise no
    /// `Secure`.
    pub fn secure_cookies(&self) -> bool {
        self.public_origin
            .as_ref()
            .is_some_and(PublicOrigin::is_https)
    }

    /// The configured public origin, if any.
    pub fn public_origin(&self) -> Option<&PublicOrigin> {
        self.public_origin.as_ref()
    }

    pub fn mutations_enabled(&self) -> bool {
        self.allow_mutations
    }

    pub fn github_sso_enabled(&self) -> bool {
        self.github_oauth.is_some()
    }

    fn github_oauth_config(&self) -> Result<&GithubOAuthConfig, &'static str> {
        self.github_oauth
            .as_ref()
            .ok_or("GitHub SSO is not configured")
    }

    /// Constant-time check of a presented login/bearer token against the
    /// configured token. When no token is configured (loopback dev), every
    /// presented token is accepted — the server is only reachable from
    /// localhost in that mode.
    ///
    /// D7a R3 #4: the configured token's digest is precomputed at construction, so
    /// here we hash ONLY the presented token and compare the two fixed-width
    /// SHA-256 digests in constant time. The comparison work is independent of the
    /// configured secret's length (and collision resistance subsumes the
    /// length-equality bit: a differently sized presented token yields a different
    /// digest).
    pub fn verify_token(&self, presented: &str) -> bool {
        match self.token_digest.as_ref() {
            None => !self.has_token,
            Some(expected) => {
                let presented_digest = Sha256::digest(presented.as_bytes());
                bool::from(presented_digest.as_slice().ct_eq(expected.as_slice()))
            }
        }
    }

    // -- session cookie sign/verify ---------------------------------------

    /// Mint a signed session-cookie value with `exp = now + SESSION_TTL_SECS`
    /// for a token/dev-open login (no user). Returns `(cookie_value, exp)`.
    pub fn issue_session(&self) -> (String, u64) {
        let exp = now_unix().saturating_add(SESSION_TTL_SECS);
        (self.sign_session(exp, None), exp)
    }

    /// Mint a signed session for a user login. The user is embedded in the
    /// signed payload so every request knows who acts without a lookup.
    pub fn issue_session_for(&self, user: &crate::accounts::SessionUser) -> (String, u64) {
        let exp = now_unix().saturating_add(SESSION_TTL_SECS);
        (self.sign_session(exp, Some(user)), exp)
    }

    /// Mint a signed cookie referencing a durable delegated session. The policy
    /// state stays server-side; only the opaque session id and expiry are signed.
    pub fn issue_delegated_session(&self, session_id: &str, exp: u64) -> String {
        let payload = format!("{exp}:delegated:{session_id}");
        let mac = self.mac(payload.as_bytes());
        format!("{}.{payload}", URL_SAFE_NO_PAD.encode(mac))
    }

    /// Sign a `{exp}:{nonce}[:{base64url(user json)}]` payload, returning the
    /// full cookie value `base64url(mac).{payload}`.
    fn sign_session(&self, exp: u64, user: Option<&crate::accounts::SessionUser>) -> String {
        let nonce = Uuid::new_v4().simple().to_string();
        let mut payload = format!("{exp}:{nonce}");
        if let Some(user) = user
            && let Ok(encoded) = serde_json::to_vec(user)
        {
            payload.push(':');
            payload.push_str(&URL_SAFE_NO_PAD.encode(encoded));
        }
        let mac = self.mac(payload.as_bytes());
        format!("{}.{payload}", URL_SAFE_NO_PAD.encode(mac))
    }

    /// Verify a session-cookie value: split on the FIRST `.`, recompute the MAC
    /// over the payload, compare in constant time, then confirm `exp` is in the
    /// future. Returns the cookie `exp` (unix secs) and the embedded user.
    pub fn verify_session(
        &self,
        cookie_value: &str,
    ) -> Option<(u64, Option<crate::accounts::SessionUser>)> {
        let payload = self.verify_signed_payload(cookie_value)?;
        let mut parts = payload.splitn(3, ':');
        let exp: u64 = parts.next()?.parse().ok()?;
        let nonce = parts.next()?;
        if nonce == "delegated" || exp <= now_unix() {
            return None;
        }
        let user = parts
            .next()
            .and_then(|encoded| URL_SAFE_NO_PAD.decode(encoded).ok())
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        Some((exp, user))
    }

    /// Resolve a signed delegated cookie to its server-side session id + expiry.
    pub fn delegated_session(&self, headers: &HeaderMap) -> Option<(String, u64)> {
        let value = cookie_value(headers, SESSION_COOKIE)?;
        let payload = self.verify_signed_payload(&value)?;
        let mut parts = payload.split(':');
        let exp: u64 = parts.next()?.parse().ok()?;
        if parts.next()? != "delegated" || exp <= now_unix() {
            return None;
        }
        let session_id = parts.next()?.to_string();
        if session_id.is_empty() || parts.next().is_some() {
            return None;
        }
        Some((session_id, exp))
    }

    fn verify_signed_payload<'a>(&self, cookie_value: &'a str) -> Option<&'a str> {
        let (mac_b64, payload) = cookie_value.split_once('.')?;
        let presented_mac = URL_SAFE_NO_PAD.decode(mac_b64).ok()?;
        let expected_mac = self.mac(payload.as_bytes());
        // Constant-time MAC comparison (length-independent: unequal lengths
        // compare false without early return).
        if !bool::from(presented_mac.ct_eq_padded(&expected_mac)) {
            return None;
        }
        Some(payload)
    }

    fn mac(&self, message: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(&self.session_key)
            .expect("HMAC accepts a key of any length");
        mac.update(message);
        mac.finalize().into_bytes().to_vec()
    }

    fn issue_github_oauth_state(&self) -> (String, String, String) {
        let state = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let verifier = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let exp = now_unix().saturating_add(GITHUB_OAUTH_TTL_SECS);
        let payload = format!("{exp}:{state}:{verifier}");
        let cookie = format!(
            "{}.{payload}",
            URL_SAFE_NO_PAD.encode(self.mac(payload.as_bytes()))
        );
        (state, verifier, cookie)
    }

    fn verify_github_oauth_state(&self, cookie: &str, presented_state: &str) -> Option<String> {
        let payload = self.verify_signed_payload(cookie)?;
        let mut parts = payload.split(':');
        let exp: u64 = parts.next()?.parse().ok()?;
        let expected_state = parts.next()?;
        let verifier = parts.next()?;
        if exp <= now_unix()
            || parts.next().is_some()
            || !bool::from(
                expected_state
                    .as_bytes()
                    .ct_eq_padded(presented_state.as_bytes()),
            )
        {
            return None;
        }
        Some(verifier.to_string())
    }

    // -- request authentication -------------------------------------------

    /// Dev-open mode: NO token is configured. This is automatic on loopback and
    /// requires the explicit insecure override off loopback. Every request is
    /// treated as authenticated; startup emits a warning before routes register.
    pub fn dev_open(&self) -> bool {
        !self.has_token
    }

    /// Authenticate an HTTP request from its headers: a valid signed session
    /// cookie (user or token login), dev-open mode (no token configured →
    /// always authenticated), OR (non-browser fallback) a constant-time
    /// `Authorization: Bearer` token match. The cookie is checked FIRST so a
    /// user login is attributed even on a dev-open listener. Returns the
    /// session with its `exp` (`u64::MAX` for the dev-open / bearer paths).
    pub fn authenticate(&self, headers: &HeaderMap) -> Option<AuthSession> {
        if let Some(value) = cookie_value(headers, SESSION_COOKIE)
            && let Some((exp, user)) = self.verify_session(&value)
        {
            let kind = if user.is_some() {
                AuthSessionKind::User
            } else {
                AuthSessionKind::DashboardToken
            };
            return Some(AuthSession { exp, user, kind });
        }
        if self.dev_open() {
            return Some(AuthSession {
                exp: u64::MAX,
                user: None,
                kind: AuthSessionKind::DevOpen,
            });
        }
        if let Some(token) = bearer_token(headers)
            && self.verify_token(&token)
        {
            return Some(AuthSession {
                exp: u64::MAX,
                user: None,
                kind: AuthSessionKind::DashboardToken,
            });
        }
        None
    }

    /// Validate a WebSocket upgrade: (1) a valid signed session cookie and
    /// (2) an `Origin` header on the allow-list (the served origin or the exact
    /// configured `PUBLIC_ORIGIN`). Returns the cookie `exp` so the socket can
    /// be closed when it passes. The bearer fallback is intentionally NOT
    /// honored for WS — browsers cannot set `Authorization` on a `WebSocket`,
    /// so a cookie is the only legitimate browser path and skipping bearer
    /// keeps the CSWSH `Origin` check meaningful.
    pub fn authenticate_ws(&self, headers: &HeaderMap) -> Option<u64> {
        // The Origin allow-list (CSWSH defense) applies even in dev-open mode —
        // a cross-site page must not open the socket regardless of the cookie.
        if !self.origin_allowed(headers) {
            return None;
        }
        if self.dev_open() {
            // No cookie to expire → no per-connection close timer.
            return Some(u64::MAX);
        }
        cookie_value(headers, SESSION_COOKIE)
            .and_then(|value| self.verify_session(&value))
            .map(|(exp, _user)| exp)
    }

    /// Whether the request's `Origin` is allowed for a WS upgrade. A request
    /// with NO `Origin` (a non-browser client) is allowed — the CSWSH risk is
    /// browser-only, and such a client already passed the signed-cookie check.
    ///
    /// When an `Origin` IS present:
    /// - If a `PUBLIC_ORIGIN` is configured, the origin must equal it EXACTLY. We
    ///   deliberately do NOT fall back to the request's `Host` here: `Host` is
    ///   attacker-controllable, so trusting it would let a page on `https://evil`
    ///   with a forged `Host: evil` ride a stolen cookie.
    /// - If NO `PUBLIC_ORIGIN` is configured, the request's `Origin` must match
    ///   its `Host`. This fallback is enabled on loopback and in explicitly
    ///   insecure tokenless mode, preserving a same-origin CSWSH boundary.
    pub(crate) fn origin_allowed(&self, headers: &HeaderMap) -> bool {
        let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else {
            return true;
        };
        match self.public_origin.as_ref() {
            Some(public) => origin == public.as_str(),
            None => {
                (self.loopback || (self.allow_insecure && self.dev_open()))
                    && same_origin_as_host(
                        headers,
                        origin,
                        !self.loopback && self.allow_insecure && self.dev_open(),
                    )
            }
        }
    }

    // -- CSRF double-submit -----------------------------------------------

    /// Mint a fresh CSRF token for the double-submit cookie + SPA bootstrap.
    pub fn issue_csrf_token(&self) -> String {
        Uuid::new_v4().simple().to_string()
    }

    /// Verify the double-submit CSRF token: the `X-CSRF-Token` header must
    /// constant-time-equal the `llmconduit_csrf` cookie, and neither may be
    /// empty. (The token's unforgeability comes from the `SameSite=Strict`
    /// session cookie gating the request; the double-submit defends the
    /// non-`HttpOnly` cookie against a same-site script that cannot read it.)
    pub fn verify_csrf(&self, headers: &HeaderMap) -> bool {
        let header_token = headers
            .get(CSRF_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let cookie_token = cookie_value(headers, CSRF_COOKIE).unwrap_or_default();
        if header_token.is_empty() || cookie_token.is_empty() {
            return false;
        }
        bool::from(
            header_token
                .as_bytes()
                .ct_eq_padded(cookie_token.as_bytes()),
        )
    }
}

// ---------------------------------------------------------------------------
// MutationPolicy — pluggable gate D6/D13 consume for the kill route
// ---------------------------------------------------------------------------

/// Decides whether a mutating dashboard action (the D6 flow-kill route) may
/// proceed. D6 compiles/tests against this trait with a mock impl, so D6 has no
/// build dependency on D7 (one-way edge D7→D6, no cycle).
pub trait MutationPolicy: Send + Sync {
    /// Whether mutations are enabled at all (the `ALLOW_MUTATIONS` gate). When
    /// `false`, a mutation route returns 403 before any CSRF work.
    fn mutations_enabled(&self) -> bool;

    /// Authorize a specific mutation request: mutations must be enabled AND the
    /// request must carry a valid double-submit CSRF token.
    fn authorize_mutation(&self, headers: &HeaderMap) -> Result<(), MutationDenied>;
}

/// Why a mutation was refused. Maps to an HTTP status at the route layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationDenied {
    /// Mutations are globally disabled (`ALLOW_MUTATIONS` not set) → 403.
    Disabled,
    /// CSRF double-submit token missing or mismatched → 403.
    CsrfInvalid,
}

impl MutationDenied {
    pub fn status(self) -> StatusCode {
        StatusCode::FORBIDDEN
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::Disabled => "dashboard mutations are disabled",
            Self::CsrfInvalid => "missing or invalid CSRF token",
        }
    }
}

impl MutationPolicy for DashboardAuth {
    fn mutations_enabled(&self) -> bool {
        self.allow_mutations
    }

    fn authorize_mutation(&self, headers: &HeaderMap) -> Result<(), MutationDenied> {
        if !self.allow_mutations {
            return Err(MutationDenied::Disabled);
        }
        if !self.verify_csrf(headers) {
            return Err(MutationDenied::CsrfInvalid);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Startup route-registration decision (pure + testable)
// ---------------------------------------------------------------------------

/// Why the protected routes were refused (for a precise startup log/test).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteRefusal {
    /// No `LLMCONDUIT_DASHBOARD_TOKEN` on a non-loopback bind without the
    /// explicit insecure override.
    MissingToken,
    /// No valid `LLMCONDUIT_DASHBOARD_SESSION_KEY` (missing or < 32 decoded
    /// bytes). Fatal for authenticated non-loopback mode; tokenless explicit
    /// dev-open mode does not use login sessions and may generate one.
    MissingSessionKey,
    /// No valid `LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN`. Fatal on a non-loopback
    /// bind: a validated `https://` origin is required, OR — when
    /// authenticated `ALLOW_INSECURE_DASHBOARD=1` mode accepts a validated
    /// explicit `http://`-or-`https://` origin. Tokenless dev-open mode does not
    /// reach this refusal because it uses same-origin Host validation.
    MissingHttpsOrigin,
}

impl RouteRefusal {
    pub fn reason(self) -> &'static str {
        match self {
            Self::MissingToken => {
                "LLMCONDUIT_DASHBOARD_TOKEN is required on a non-loopback bind unless \
                 LLMCONDUIT_ALLOW_INSECURE_DASHBOARD=1 explicitly enables tokenless access"
            }
            Self::MissingSessionKey => {
                "a valid LLMCONDUIT_DASHBOARD_SESSION_KEY (>= 32 decoded bytes of base64) is \
                 required on a non-loopback bind"
            }
            Self::MissingHttpsOrigin => {
                "a valid https:// LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN is required on a non-loopback \
                 bind (or set LLMCONDUIT_ALLOW_INSECURE_DASHBOARD=1 to relax ONLY the TLS scheme \
                 and supply an explicit http:// or https:// origin — the override does NOT waive \
                 the exact-origin requirement)"
            }
        }
    }
}

/// The startup decision for whether the protected routes (`/dashboard`,
/// `/debug`, and their sub-routes) may be registered, given the bind address
/// and the env snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// Register the routes. `warnings` are logged (e.g. a tokenless loopback dev
    /// server, or an insecure-override that bypassed the TLS requirement).
    Register { warnings: Vec<String> },
    /// Refuse to register the protected routes (production misconfiguration).
    Refuse(RouteRefusal),
}

impl RouteDecision {
    pub fn should_register(&self) -> bool {
        matches!(self, Self::Register { .. })
    }
}

/// Decide whether the protected routes may register.
///
/// - **Loopback bind:** always register. A missing token is a dev concession
///   (warned, and `dev_open` authenticates every request — reachable ONLY here);
///   a missing session key is auto-generated; a missing https origin is fine
///   (localhost is not TLS).
/// - **Non-loopback bind:** normally require a token, valid session key, and
///   `https://` public origin. `LLMCONDUIT_ALLOW_INSECURE_DASHBOARD=1` has two
///   explicit modes: with a token it relaxes only the origin scheme; without a
///   token it enables fully unauthenticated dev-open access and permits an
///   ephemeral signing key plus same-origin Host fallback.
///
/// A *malformed* public origin (bad URL / not https) counts as "missing" here
/// so the routes refuse rather than register with an unusable origin; the
/// precise parse error is surfaced when [`DashboardAuth::from_env`] runs.
pub fn startup_route_decision(bind_addr: SocketAddr, env: &DashboardEnv) -> RouteDecision {
    let github_sso = env.github_sso_configured();
    let authentication_required = env.token.is_some() || github_sso;
    if bind_addr.ip().is_loopback() {
        let mut warnings = Vec::new();
        if !authentication_required {
            warnings.push(format!(
                "{ENV_TOKEN} not set on a loopback dev bind; /dashboard and /debug are served \
                 WITHOUT a login token (localhost only)"
            ));
        }
        return RouteDecision::Register { warnings };
    }

    // Explicit tokenless insecure mode. This is intentionally checked before
    // the authenticated requirements because neither a session key nor a public
    // origin is needed for login; WS still enforces Origin == Host.
    if !authentication_required {
        if env.allow_insecure {
            return RouteDecision::Register {
                warnings: vec![format!(
                    "{ENV_ALLOW_INSECURE}=1 with no {ENV_TOKEN} on a non-loopback bind: \
                     /dashboard and /debug are FULLY UNAUTHENTICATED on every listening \
                     interface; use only behind a trusted network or access proxy"
                )],
            };
        }
        return RouteDecision::Refuse(RouteRefusal::MissingToken);
    }
    if !has_valid_session_key(env) {
        return RouteDecision::Refuse(RouteRefusal::MissingSessionKey);
    }

    // Strictly https here (pass `allow_insecure: false`): this is the SECURE-path
    // gate. A plaintext `http://` origin does not satisfy it; the override branch
    // below handles the "no validated https origin" case.
    let has_https_origin = env
        .public_origin
        .as_deref()
        .is_some_and(|raw| PublicOrigin::parse(raw, false).is_ok());
    if has_https_origin {
        return RouteDecision::Register {
            warnings: Vec::new(),
        };
    }

    // Token + key are present, but there is no validated *https* origin. The
    // override relaxes ONLY the TLS (scheme) requirement — it still REQUIRES an
    // explicit, validated origin (D7a R3 #2). Without one, the WS `Origin`
    // allow-list has nothing to match against off loopback (the `Host` header is
    // attacker-controllable and never trusted there), so the dashboard socket is
    // unusable AND the exact-origin requirement is violated. Accept the override
    // ONLY when an explicit `http`-or-`https` origin parses; otherwise refuse.
    if env.allow_insecure {
        let has_insecure_origin = env
            .public_origin
            .as_deref()
            .is_some_and(|raw| PublicOrigin::parse(raw, true).is_ok());
        if has_insecure_origin {
            return RouteDecision::Register {
                warnings: vec![format!(
                    "{ENV_ALLOW_INSECURE}=1 on a non-loopback bind: serving /dashboard and \
                     /debug over plaintext with an explicit {ENV_PUBLIC_ORIGIN} but WITHOUT \
                     https — credentials and transcripts may be exposed in transit (token + \
                     session auth and the exact-origin check are STILL enforced)"
                )],
            };
        }
        // Override set but no validated explicit origin → refuse: an off-loopback
        // bind with no exact origin cannot validate cross-site WS upgrades.
        return RouteDecision::Refuse(RouteRefusal::MissingHttpsOrigin);
    }
    RouteDecision::Refuse(RouteRefusal::MissingHttpsOrigin)
}

/// Whether `env` carries a session key that decodes to a valid (≥ 32 byte) HMAC
/// key. Mirrors [`decode_session_key`] so the startup decision refuses a
/// non-loopback bind whose key is missing or too short BEFORE
/// [`DashboardAuth::from_env`] would silently auto-generate an ephemeral one.
fn has_valid_session_key(env: &DashboardEnv) -> bool {
    env.session_key_b64
        .as_deref()
        .is_some_and(|encoded| decode_session_key(encoded).is_ok())
}

// ---------------------------------------------------------------------------
// Login / logout handlers
// ---------------------------------------------------------------------------

/// Either a dashboard token or a username + password.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    /// The dashboard token (`LLMCONDUIT_DASHBOARD_TOKEN`); used only when
    /// `username` + `password` are not both present.
    #[serde(default)]
    pub token: Option<String>,
    /// Username of a SQL `users` account; takes the password path together
    /// with `password`.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

/// `POST /dashboard/login` success body (documentation of the handler's
/// inline JSON; the handler itself builds it with `json!`).
#[derive(utoipa::ToSchema)]
#[allow(dead_code)]
pub(crate) struct LoginResponse {
    /// Always `true`.
    authenticated: bool,
    /// The signed-in user for a username/password login; `null` for a token login.
    user: Option<crate::accounts::SessionUser>,
}

/// `POST /dashboard/login` — a username/password login is verified against
/// the SQL `users` table (Argon2id); otherwise the constant-time token check
/// applies. On success set the signed `HttpOnly; SameSite=Strict[; Secure];
/// Path=/; Max-Age=86400` session cookie plus the non-`HttpOnly` double-submit
/// CSRF cookie. Response is always `no-store`.
#[utoipa::path(
    post,
    path = "/dashboard/login",
    tag = "dashboard-auth",
    operation_id = "dashboard_login",
    request_body(content = Option<LoginRequest>, description = "`{username, password}` (verified against the SQL `users` table, Argon2id) when both are present; otherwise `{token}` (constant-time match against the dashboard token). A missing or unparsable JSON body is treated as an empty token login."),
    responses(
        (status = 200, description = "Signed in. Sets two cookies: `llmconduit_session` (HMAC-signed, `HttpOnly; SameSite=Strict; Path=/; Max-Age=86400`) and the double-submit CSRF cookie `llmconduit_csrf` (same attributes but NOT `HttpOnly`, so the SPA can echo it in `x-csrf-token`); both add `Secure` when a public https origin is configured. `Cache-Control: no-store`.", body = LoginResponse),
        (status = 400, description = "Malformed body, wrong content type, or an unknown field (the body is strict); the message names the field.", body = crate::openapi::DashboardError),
        (status = 401, description = "Rejected, `Cache-Control: no-store`. Messages: `invalid username or password`; `user login needs a SQL store` (username/password given but no SQL store is configured); `sign in with username and password` (a token login once user accounts exist, unless the listener is dev-open); `invalid token`.", body = crate::openapi::DashboardError),
    )
)]
pub async fn dashboard_login(
    axum::extract::State(gateway): axum::extract::State<Arc<crate::engine::Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    payload: Result<Json<LoginRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let body = match payload {
        Ok(Json(body)) => body,
        Err(rejection) => {
            // Strict: a malformed body or an unknown field is a 400 that names
            // the problem, never a silent fall-through to a failed token login.
            return no_store(
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!(
                            "invalid JSON body, expected {{token}} or {{username, password}}: {}",
                            rejection.body_text()
                        )
                    })),
                )
                    .into_response(),
            );
        }
    };
    login_response(Some(gateway.as_ref()), &auth, body).await
}

/// The login decision, separated from the axum extractors for tests. `gateway`
/// supplies the SQL store for user logins; `None` means token-only.
pub async fn login_response(
    gateway: Option<&crate::engine::Gateway>,
    auth: &DashboardAuth,
    body: LoginRequest,
) -> Response {
    let unauthorized = |message: &str| {
        no_store(
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": message})),
            )
                .into_response(),
        )
    };
    let session_user = match (body.username.as_deref(), body.password.as_deref()) {
        (Some(username), Some(password)) => {
            let Some(store) = gateway.and_then(|gateway| gateway.persistence_store()) else {
                return unauthorized("user login needs a SQL store");
            };
            let Ok(username) = crate::accounts::validate_username(username) else {
                return unauthorized("invalid username or password");
            };
            match store.get_user_auth(username).await {
                Ok(Some(user))
                    if crate::accounts::verify_password(&user.password_hash, password) =>
                {
                    Some(crate::accounts::SessionUser {
                        id: user.id,
                        username: user.username,
                        is_admin: user.is_admin,
                    })
                }
                Ok(_) => return unauthorized("invalid username or password"),
                Err(error) => {
                    tracing::warn!(error = %error, "user login lookup failed");
                    return unauthorized("invalid username or password");
                }
            }
        }
        _ => {
            let token = body.token.unwrap_or_default();
            // Once users exist the shared token no longer opens the dashboard
            // (dev-open loopback listeners keep accepting anything).
            let users_configured = match gateway {
                Some(gateway) => crate::accounts_api::auth_mode(gateway, auth).await == "users",
                None => false,
            };
            if users_configured && !auth.dev_open() {
                return unauthorized("sign in with username and password");
            }
            if !auth.verify_token(&token) {
                return unauthorized("invalid token");
            }
            None
        }
    };

    let (session_value, _exp) = match &session_user {
        Some(user) => auth.issue_session_for(user),
        None => auth.issue_session(),
    };
    let csrf = auth.issue_csrf_token();
    let secure = auth.secure_cookies();

    let mut response = no_store(
        (
            StatusCode::OK,
            Json(serde_json::json!({"authenticated": true, "user": session_user})),
        )
            .into_response(),
    );
    let headers = response.headers_mut();
    append_set_cookie(headers, &session_cookie(&session_value, secure));
    append_set_cookie(headers, &csrf_cookie(&csrf, secure));
    response
}

/// `POST /dashboard/logout` — clear both cookies (stateless; a copied session
/// cookie remains valid until its `exp`).
#[utoipa::path(
    post,
    path = "/dashboard/logout",
    tag = "dashboard-auth",
    operation_id = "dashboard_logout",
    responses(
        (status = 204, description = "Always. Expires both cookies (`llmconduit_session=; Max-Age=0; HttpOnly` and `llmconduit_csrf=; Max-Age=0`, `SameSite=Strict; Path=/`, plus `Secure` when configured). No session is required; sessions are stateless, so a copied session cookie stays valid until its `exp`. `Cache-Control: no-store`."),
    )
)]
pub async fn dashboard_logout(Extension(auth): Extension<Arc<DashboardAuth>>) -> Response {
    let secure = auth.secure_cookies();
    let mut response = no_store(StatusCode::NO_CONTENT.into_response());
    let headers = response.headers_mut();
    append_set_cookie(headers, &expire_cookie(SESSION_COOKIE, secure, true));
    append_set_cookie(headers, &expire_cookie(CSRF_COOKIE, secure, false));
    response
}

#[derive(Clone)]
struct GithubOAuthConfig {
    client_id: String,
    client_secret: String,
    callback_url: String,
    allowed_users: HashSet<String>,
    admin_users: HashSet<String>,
}

impl GithubOAuthConfig {
    fn from_env(
        env: &DashboardEnv,
        public_origin: Option<&PublicOrigin>,
        allow_insecure: bool,
        loopback: bool,
    ) -> Result<Option<Self>, String> {
        if !env.github_sso_partially_configured() {
            return Ok(None);
        }
        if !env.github_sso_configured() {
            return Err(format!(
                "GitHub SSO requires {ENV_GITHUB_CLIENT_ID}, {ENV_GITHUB_CLIENT_SECRET}, and {ENV_GITHUB_ALLOWED_USERS}"
            ));
        }
        Self::from_values(env, public_origin, allow_insecure, loopback).map(Some)
    }

    fn from_values(
        env: &DashboardEnv,
        public_origin: Option<&PublicOrigin>,
        allow_insecure: bool,
        loopback: bool,
    ) -> Result<Self, String> {
        let callback_url = env.github_callback_url.clone().or_else(|| {
            public_origin
                .map(|origin| format!("{}/dashboard/auth/github/callback", origin.as_str()))
        });
        let users = |raw: Option<String>| -> HashSet<String> {
            raw.unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_ascii_lowercase)
                .collect()
        };
        let config = Self {
            client_id: env
                .github_client_id
                .clone()
                .ok_or_else(|| "GitHub SSO is not configured".to_string())?,
            client_secret: env
                .github_client_secret
                .clone()
                .ok_or_else(|| "GitHub SSO is not configured".to_string())?,
            callback_url: validate_github_callback_url(
                public_origin,
                allow_insecure,
                loopback,
                callback_url
                    .ok_or_else(|| "GitHub SSO callback URL is not configured".to_string())?,
            )?,
            allowed_users: users(env.github_allowed_users.clone()),
            admin_users: users(env.github_admin_users.clone()),
        };
        if config.allowed_users.is_empty() {
            return Err("GitHub SSO has no allowed users".to_string());
        }
        for admin in &config.admin_users {
            if !config.allowed_users.contains(admin) {
                return Err(format!(
                    "GitHub SSO administrator '{admin}' is not present in the allowed-users list"
                ));
            }
        }
        Ok(config)
    }
}

fn validate_github_callback_url(
    public_origin: Option<&PublicOrigin>,
    allow_insecure: bool,
    loopback: bool,
    raw: String,
) -> Result<String, String> {
    let url = url::Url::parse(raw.trim())
        .map_err(|_| "GitHub SSO callback URL is invalid".to_string())?;
    if url.username() != "" || url.password().is_some() {
        return Err("GitHub SSO callback URL must not contain credentials".to_string());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("GitHub SSO callback URL must not contain query or fragment".to_string());
    }
    if url.path() != "/dashboard/auth/github/callback" {
        return Err(
            "GitHub SSO callback URL must end with /dashboard/auth/github/callback".to_string(),
        );
    }
    let scheme = url.scheme();
    let host = url
        .host_str()
        .ok_or_else(|| "GitHub SSO callback URL is invalid".to_string())?;
    let loopback_callback = matches!(host, "localhost" | "127.0.0.1" | "::1");
    if scheme != "https"
        && !(allow_insecure && scheme == "http")
        && !(loopback && loopback_callback && scheme == "http")
    {
        return Err("GitHub SSO callback URL must use https".to_string());
    }
    let origin = url.origin().ascii_serialization();
    if let Some(public_origin) = public_origin
        && origin != public_origin.as_str()
    {
        return Err(
            "GitHub SSO callback URL origin must match LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN"
                .to_string(),
        );
    }
    Ok(format!("{origin}/dashboard/auth/github/callback"))
}

#[derive(Debug, Deserialize)]
pub struct GithubCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GithubTokenResponse {
    access_token: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GithubUserResponse {
    id: u64,
    login: String,
}

/// Begin the GitHub OAuth web flow. The short-lived state cookie is signed and
/// uses SameSite=Lax so it is returned on GitHub's cross-site callback.
#[utoipa::path(
    get,
    path = "/dashboard/auth/github/start",
    tag = "dashboard-auth",
    operation_id = "dashboard_github_start",
    responses(
        (status = 303, description = "Redirects to GitHub OAuth authorization and sets a short-lived signed OAuth state cookie. `Cache-Control: no-store`."),
        (status = 503, description = "GitHub SSO is not configured or is invalid. Plain text error, `Cache-Control: no-store`.", content_type = "text/plain", body = String),
    )
)]
pub async fn dashboard_github_start(Extension(auth): Extension<Arc<DashboardAuth>>) -> Response {
    let config = match auth.github_oauth_config() {
        Ok(config) => config,
        Err(message) => {
            return no_store((StatusCode::SERVICE_UNAVAILABLE, message).into_response());
        }
    };
    let (state, verifier, oauth_cookie) = auth.issue_github_oauth_state();
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut authorize =
        url::Url::parse("https://github.com/login/oauth/authorize").expect("static GitHub URL");
    authorize
        .query_pairs_mut()
        .append_pair("client_id", &config.client_id)
        .append_pair("redirect_uri", &config.callback_url)
        .append_pair("state", &state)
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256");
    let mut response = no_store(Redirect::to(authorize.as_str()).into_response());
    append_set_cookie(
        response.headers_mut(),
        &github_oauth_cookie(&oauth_cookie, auth.secure_cookies()),
    );
    response
}

/// Complete GitHub OAuth, enforce the explicit user allow-list, and mint the
/// same hardened dashboard session/CSRF cookies used by other login methods.
#[utoipa::path(
    get,
    path = "/dashboard/auth/github/callback",
    tag = "dashboard-auth",
    operation_id = "dashboard_github_callback",
    params(
        ("code" = Option<String>, Query, description = "GitHub OAuth authorization code."),
        ("state" = Option<String>, Query, description = "Signed OAuth state returned by GitHub."),
        ("error" = Option<String>, Query, description = "GitHub OAuth error marker; redirects to a safe login-error code."),
    ),
    responses(
        (status = 200, description = "Successful sign-in completion page. Sets dashboard session and CSRF cookies, clears the OAuth state cookie, and starts a fresh same-origin navigation to `/dashboard`. `Cache-Control: no-store`."),
        (status = 303, description = "On denial/error, redirects to `/dashboard?login_error=github_*`. `Cache-Control: no-store`."),
    )
)]
pub async fn dashboard_github_callback(
    axum::extract::State(gateway): axum::extract::State<Arc<crate::engine::Gateway>>,
    Extension(auth): Extension<Arc<DashboardAuth>>,
    headers: HeaderMap,
    Query(query): Query<GithubCallbackQuery>,
) -> Response {
    if query.error.is_some() {
        return github_login_error(&auth, "cancelled");
    }
    let config = match auth.github_oauth_config() {
        Ok(config) => config,
        Err(_) => return github_login_error(&auth, "configuration"),
    };
    let (Some(code), Some(state), Some(cookie)) = (
        query.code.as_deref(),
        query.state.as_deref(),
        cookie_value(&headers, GITHUB_OAUTH_COOKIE),
    ) else {
        return github_login_error(&auth, "state");
    };
    let Some(verifier) = auth.verify_github_oauth_state(&cookie, state) else {
        return github_login_error(&auth, "state");
    };

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
    {
        Ok(client) => client,
        Err(_) => return github_login_error(&auth, "exchange"),
    };
    let token_response = client
        .post("https://github.com/login/oauth/access_token")
        .header(reqwest::header::ACCEPT, "application/json")
        .json(&serde_json::json!({
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "code": code,
            "redirect_uri": config.callback_url,
            "code_verifier": verifier,
        }))
        .send()
        .await;
    let Ok(token_response) = token_response else {
        return github_login_error(&auth, "exchange");
    };
    if !token_response.status().is_success() {
        return github_login_error(&auth, "exchange");
    }
    let Ok(token) = token_response.json::<GithubTokenResponse>().await else {
        return github_login_error(&auth, "exchange");
    };
    let Some(access_token) = token.access_token.filter(|_| token.error.is_none()) else {
        return github_login_error(&auth, "exchange");
    };
    let user_response = client
        .get("https://api.github.com/user")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header(reqwest::header::USER_AGENT, "llmconduit-dashboard")
        .bearer_auth(access_token)
        .send()
        .await;
    let Ok(user_response) = user_response else {
        return github_login_error(&auth, "profile");
    };
    if !user_response.status().is_success() {
        return github_login_error(&auth, "profile");
    }
    let Ok(github_user) = user_response.json::<GithubUserResponse>().await else {
        return github_login_error(&auth, "profile");
    };
    let login = github_user.login.to_ascii_lowercase();
    if !config.allowed_users.contains(&login) {
        return github_login_error(&auth, "denied");
    }
    let user = match provision_github_user(&gateway, config, &github_user).await {
        Ok(user) => user,
        Err(reason) => return github_login_error(&auth, reason),
    };
    let (session, _) = auth.issue_session_for(&user);
    let csrf = auth.issue_csrf_token();
    github_login_complete(&auth, &session, &csrf)
}

/// Finish OAuth on a committed same-origin document before entering the SPA.
///
/// A `SameSite=Strict` cookie set on GitHub's cross-site callback is stored, but
/// browsers do not send it on an immediate HTTP redirect because that request
/// still belongs to the cross-site redirect chain. A document-level refresh is
/// a new navigation initiated by our own origin, so the Strict session cookie
/// is present on the first `/dashboard` request without weakening its policy.
fn github_login_complete(auth: &DashboardAuth, session: &str, csrf: &str) -> Response {
    const COMPLETE_HTML: &str = "<!doctype html><html><head><meta charset=\"utf-8\"><meta http-equiv=\"refresh\" content=\"0;url=/dashboard\"><title>Sign-in complete</title></head><body><p>Sign-in complete. <a href=\"/dashboard\">Continue to the dashboard</a>.</p></body></html>";

    let secure = auth.secure_cookies();
    let mut response = no_store(Html(COMPLETE_HTML).into_response());
    append_set_cookie(response.headers_mut(), &session_cookie(session, secure));
    append_set_cookie(response.headers_mut(), &csrf_cookie(csrf, secure));
    append_set_cookie(
        response.headers_mut(),
        &expire_cookie_lax(GITHUB_OAUTH_COOKIE, secure),
    );
    response
}

async fn provision_github_user(
    gateway: &crate::engine::Gateway,
    config: &GithubOAuthConfig,
    github_user: &GithubUserResponse,
) -> Result<crate::accounts::SessionUser, &'static str> {
    let Some(store) = gateway.persistence_store() else {
        tracing::error!("GitHub SSO login requires configured SQL storage");
        return Err("configuration");
    };
    let login = github_user.login.to_ascii_lowercase();
    let id = format!("github:{}", github_user.id);
    let username = format!("github:{}", github_user.login);
    let is_admin = config.admin_users.contains(&login);
    let user = store
        .upsert_external_user(&id, &username, is_admin, "github-sso")
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to provision GitHub SSO user");
            "configuration"
        })?;
    gateway.set_users_configured(true);
    Ok(crate::accounts::SessionUser::from(&user))
}

fn github_login_error(auth: &DashboardAuth, reason: &str) -> Response {
    let target = format!("/dashboard?login_error=github_{reason}");
    let mut response = no_store(Redirect::to(&target).into_response());
    append_set_cookie(
        response.headers_mut(),
        &expire_cookie_lax(GITHUB_OAUTH_COOKIE, auth.secure_cookies()),
    );
    response
}

/// Build the successful key-login response without exposing session or CSRF
/// material in the body. Both values are carried only by hardened cookies.
pub(crate) fn delegated_login_response(
    auth: &DashboardAuth,
    session_id: &str,
    csrf: &str,
    exp: u64,
) -> Response {
    let secure = auth.secure_cookies();
    let session_value = auth.issue_delegated_session(session_id, exp);
    let max_age = exp.saturating_sub(now_unix());
    let mut response = no_store(
        (
            StatusCode::OK,
            Json(serde_json::json!({"authenticated": true})),
        )
            .into_response(),
    );
    append_set_cookie(
        response.headers_mut(),
        &session_cookie_with_max_age(&session_value, secure, max_age),
    );
    append_set_cookie(response.headers_mut(), &csrf_cookie(csrf, secure));
    response
}

// ---------------------------------------------------------------------------
// Auth middleware + extractor for the protected HTTP routes
// ---------------------------------------------------------------------------

/// A successful authentication, attached to the request so handlers can read
/// the session `exp` and user without re-validating.
#[derive(Debug, Clone)]
pub struct AuthSession {
    /// Session expiry (unix secs); `u64::MAX` for a bearer-authenticated
    /// (non-browser) request.
    pub exp: u64,
    /// The user behind a username/password login; `None` for token/dev-open
    /// and delegated dashboard sessions.
    pub user: Option<crate::accounts::SessionUser>,
    /// The credential class that authenticated this dashboard session.
    pub kind: AuthSessionKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthSessionKind {
    User,
    DashboardToken,
    DevOpen,
    Delegated,
}

impl AuthSession {
    pub fn delegated(exp: u64) -> Self {
        Self {
            exp,
            user: None,
            kind: AuthSessionKind::Delegated,
        }
    }

    pub fn bootstrap_admin(&self) -> bool {
        matches!(
            self.kind,
            AuthSessionKind::DashboardToken | AuthSessionKind::DevOpen
        )
    }
}

/// `axum` middleware enforcing a valid session on the protected HTTP routes
/// (`/debug`, `/debug/app.js`, and — registered by D13 — `/dashboard/api/*`).
/// Reads the shared [`DashboardAuth`] from the request extension installed by
/// the auth layer, validates, and either inserts an [`AuthSession`] extension
/// and continues, or returns `401 no-store`. The auth Extension is layered onto
/// the same routes (see `http.rs`), so it is always present here; a missing
/// extension fails closed.
pub async fn require_session(mut request: axum::extract::Request, next: Next) -> Response {
    let Some(auth) = request.extensions().get::<Arc<DashboardAuth>>().cloned() else {
        return unauthorized();
    };
    match auth.authenticate(request.headers()) {
        Some(session) => {
            request.extensions_mut().insert(session);
            next.run(request).await
        }
        None => unauthorized(),
    }
}

/// Extractor form of the same check, for handlers that want the session
/// directly (and for the `/dashboard` shell decision). Reads the shared
/// [`DashboardAuth`] from the request extension installed by the auth layer.
impl<S> FromRequestParts<S> for AuthSession
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Prefer an extension inserted by `require_session` (avoids re-verifying
        // the HMAC); fall back to validating the headers directly.
        if let Some(session) = parts.extensions.get::<AuthSession>() {
            return Ok(session.clone());
        }
        let auth = parts
            .extensions
            .get::<Arc<DashboardAuth>>()
            .cloned()
            .ok_or_else(unauthorized)?;
        auth.authenticate(&parts.headers).ok_or_else(unauthorized)
    }
}

/// Optional variant: `Option<AuthSession>` is `None` for an unauthenticated
/// request instead of short-circuiting with 401. Used by the `/dashboard` route
/// to choose the login shell vs. the SPA without rejecting the request.
impl<S> OptionalFromRequestParts<S> for AuthSession
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> Result<Option<Self>, Self::Rejection> {
        if let Some(session) = parts.extensions.get::<AuthSession>() {
            return Ok(Some(session.clone()));
        }
        let Some(auth) = parts.extensions.get::<Arc<DashboardAuth>>().cloned() else {
            return Ok(None);
        };
        Ok(auth.authenticate(&parts.headers))
    }
}

// ---------------------------------------------------------------------------
// Cookie + header helpers
// ---------------------------------------------------------------------------

/// Build the `Set-Cookie` value for the signed session cookie. `HttpOnly`
/// (no JS access), `SameSite=Strict` (no cross-site send), `Path=/` (so the
/// SAME cookie authorizes `/dashboard` AND `/debug`), `Max-Age=86400`, and
/// `Secure` only when a public https origin is configured.
fn session_cookie(value: &str, secure: bool) -> String {
    session_cookie_with_max_age(value, secure, SESSION_TTL_SECS)
}

fn session_cookie_with_max_age(value: &str, secure: bool, max_age: u64) -> String {
    let mut cookie =
        format!("{SESSION_COOKIE}={value}; HttpOnly; SameSite=Strict; Path=/; Max-Age={max_age}");
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

/// Build the `Set-Cookie` value for the double-submit CSRF cookie. NON-`HttpOnly`
/// so the SPA can read it and echo it in `X-CSRF-Token`; same `SameSite=Strict`,
/// `Path=/`, `Max-Age`, and `Secure` policy as the session cookie.
fn csrf_cookie(value: &str, secure: bool) -> String {
    let mut cookie =
        format!("{CSRF_COOKIE}={value}; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}");
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

fn github_oauth_cookie(value: &str, secure: bool) -> String {
    let mut cookie = format!(
        "{GITHUB_OAUTH_COOKIE}={value}; HttpOnly; SameSite=Lax; Path=/dashboard/auth/github; Max-Age={GITHUB_OAUTH_TTL_SECS}"
    );
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

fn expire_cookie_lax(name: &str, secure: bool) -> String {
    let mut cookie =
        format!("{name}=; HttpOnly; SameSite=Lax; Path=/dashboard/auth/github; Max-Age=0");
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

/// Build a cookie-clearing `Set-Cookie` (empty value, `Max-Age=0`). `http_only`
/// matches the original cookie's flag so the attributes line up.
fn expire_cookie(name: &str, secure: bool, http_only: bool) -> String {
    let mut cookie = format!("{name}=; SameSite=Strict; Path=/; Max-Age=0");
    if http_only {
        cookie.push_str("; HttpOnly");
    }
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

/// Append a `Set-Cookie` header (multiple cookies → multiple headers).
fn append_set_cookie(headers: &mut HeaderMap, cookie: &str) {
    if let Ok(value) = HeaderValue::from_str(cookie) {
        headers.append(header::SET_COOKIE, value);
    }
}

/// A `401 Unauthorized` carrying the full auth-response security header set
/// (`no-store` + CSP + nosniff/no-referrer/X-Frame-Options) — see [`no_store`].
fn unauthorized() -> Response {
    no_store((StatusCode::UNAUTHORIZED, "unauthorized").into_response())
}

/// Stamp the common security headers on an auth response (D7a R4): a restrictive
/// `Content-Security-Policy`, `X-Content-Type-Options: nosniff`,
/// `Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`, and
/// `Cache-Control: no-store`. EVERY auth-layer response (the 401, the login
/// success, and the logout) flows through here so none is missing the hardening
/// the `/dashboard` shell and `/debug` already carry. The CSP is the locked-down
/// `default-src 'none'` form: these endpoints return JSON or a bare status, never
/// HTML/script, so nothing legitimate needs to load and `frame-ancestors 'none'`
/// reinforces the `X-Frame-Options: DENY` clickjacking defense.
pub fn no_store(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    response
}

/// Extract a single cookie value by name from the `Cookie` header. Splits on
/// `;`, trims, and matches the `name=` prefix exactly.
fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let header = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in header.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix(name)
            && let Some(value) = rest.strip_prefix('=')
        {
            return Some(value.to_string());
        }
    }
    None
}

/// Extract a bearer token from `Authorization: Bearer <token>` (case-insensitive
/// scheme).
fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let header = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let rest = header.strip_prefix("Bearer ").or_else(|| {
        header
            .get(..7)
            .filter(|prefix| prefix.eq_ignore_ascii_case("bearer "))
            .map(|_| &header[7..])
    })?;
    let token = rest.trim();
    (!token.is_empty()).then(|| token.to_string())
}

/// Whether `origin` equals the request's own `scheme://host[:port]`, derived
/// from `Host`. We can't see the TLS state of the inbound connection from the
/// handler, so we accept either the http or https form of the request's `Host`
/// as "same origin". Normally the Host must also be a literal loopback address.
/// The explicit tokenless non-loopback insecure mode passes
/// `allow_nonloop_host=true`; it has no authentication cookie to steal, but still
/// requires `Origin == Host` to stop an unrelated browser origin opening the WS.
///
/// D7a R4: even on a loopback bind the `Host` is only trusted when it is a
/// LITERAL loopback host name (`localhost`, `127.0.0.1`, `[::1]`/`::1`, with any
/// port). A DNS-rebinding attacker (e.g. `evil.com` resolving to `127.0.0.1`)
/// otherwise reaches `/debug/ws` with `Origin: http://evil.com` + `Host:
/// evil.com` and would ride a stolen cookie; rejecting any non-loopback `Host`
/// here closes that path while preserving real localhost development.
fn same_origin_as_host(headers: &HeaderMap, origin: &str, allow_nonloop_host: bool) -> bool {
    let Some(host) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    if !allow_nonloop_host && !is_loopback_host(host) {
        return false;
    }
    origin == format!("http://{host}") || origin == format!("https://{host}")
}

/// Whether a `Host` header value is a LITERAL loopback host (anti-DNS-rebinding,
/// D7a R4). Accepts `localhost`, `127.0.0.1`, and the IPv6 loopback in both the
/// bracketed (`[::1]`) and bare (`::1`) forms, each with an optional `:port`.
/// Any other name — including one that merely RESOLVES to a loopback address — is
/// rejected, so the loopback dev-open `Host` fallback cannot be reached via a
/// rebound DNS name.
fn is_loopback_host(host: &str) -> bool {
    // A bare (unbracketed) IPv6 loopback never appears in a real `Host` header
    // (RFC 7230 requires the bracketed form), but accept the exact `::1` token
    // for the constructed-headers path before the `:port` split below would
    // mis-parse its embedded colons.
    if host == "::1" {
        return true;
    }
    // IPv6 hosts are bracketed in a `Host` header (`[::1]` or `[::1]:port`);
    // strip the brackets (and any port) before matching the literal address.
    let bare = if let Some(rest) = host.strip_prefix('[') {
        match rest.split_once(']') {
            // `[addr]` or `[addr]:port` — the part after `]` must be empty or a
            // `:port`, never arbitrary trailing data.
            Some((addr, suffix)) if suffix.is_empty() || suffix.starts_with(':') => addr,
            _ => return false,
        }
    } else {
        // Non-bracketed: a single trailing `:port` may follow the name/IPv4
        // literal. An empty port (`localhost:`) leaves the host unchanged so it
        // still fails the literal match below.
        host.rsplit_once(':').map_or(
            host,
            |(name, port)| if port.is_empty() { host } else { name },
        )
    };
    matches!(bare, "localhost" | "127.0.0.1" | "::1")
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

/// Seconds since the Unix epoch (monotonic enough for an expiry check; a clock
/// that jumps backwards only shortens a session).
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Decode + length-check the configured base64 session key.
fn decode_session_key(encoded: &str) -> Result<Vec<u8>, String> {
    // Accept both standard and URL-safe base64 (with or without padding) so an
    // operator pasting either form works.
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(encoded))
        .or_else(|_| URL_SAFE_NO_PAD.decode(encoded))
        .map_err(|err| format!("{ENV_SESSION_KEY} is not valid base64: {err}"))?;
    if decoded.len() < MIN_SESSION_KEY_BYTES {
        return Err(format!(
            "{ENV_SESSION_KEY} must decode to at least {MIN_SESSION_KEY_BYTES} bytes (got {})",
            decoded.len()
        ));
    }
    Ok(decoded)
}

/// Generate a 32-byte signing key from UUID entropy (no extra RNG dependency).
fn generate_session_key() -> Vec<u8> {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(Uuid::new_v4().as_bytes());
    key.extend_from_slice(Uuid::new_v4().as_bytes());
    key
}

/// Constant-time, **length-independent** byte-slice equality. Used for the
/// session MAC and CSRF comparisons — both of which may compare slices of
/// differing length (a presented cookie/header is attacker-sized). (The token
/// path no longer uses this: its configured digest is precomputed at
/// construction, so it compares two fixed-width digests directly — D7a R3 #4.)
///
/// `subtle::ConstantTimeEq` on `[u8]` requires equal lengths and would itself
/// short-circuit (leaking length via timing) on a mismatch. Instead we hash
/// BOTH sides to a fixed 32-byte SHA-256 digest and compare the digests in
/// constant time, then fold in a constant-time length-equality bit. Because the
/// comparison always runs over two fixed-width digests, the work is independent
/// of either input's length, and there is NO early return / branch on the
/// secret-dependent comparison: the length bit is combined with a bitwise `&`
/// on the `subtle::Choice`, which is constant-time. Hashing also means an
/// attacker observing timing learns nothing about the secret's contents
/// (digests of unequal inputs differ pseudo-randomly).
trait CtEqPadded {
    fn ct_eq_padded(&self, other: &[u8]) -> subtle::Choice;
}

impl CtEqPadded for [u8] {
    fn ct_eq_padded(&self, other: &[u8]) -> subtle::Choice {
        // Fixed-width digests → the constant-time compare runs over 32 bytes
        // regardless of input length (no length-dependent work, no early
        // return). SHA-256 is a fixed pre-comparison transform on both sides.
        let lhs = Sha256::digest(self);
        let rhs = Sha256::digest(other);
        // Length-equality is folded in (constant-time `&`) so distinct-length
        // inputs that happen to collide in digest still compare not-equal,
        // WITHOUT branching on `len()` before the compare.
        let len_eq = (self.len() as u64).ct_eq(&(other.len() as u64));
        lhs.ct_eq(rhs.as_slice()) & len_eq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderName;

    fn loopback() -> SocketAddr {
        "127.0.0.1:4000".parse().unwrap()
    }

    fn public_bind() -> SocketAddr {
        "0.0.0.0:4000".parse().unwrap()
    }

    fn key_b64() -> String {
        base64::engine::general_purpose::STANDARD.encode([7u8; 32])
    }

    fn env_with_token() -> DashboardEnv {
        DashboardEnv {
            token: Some("s3cret-token".to_string()),
            session_key_b64: Some(key_b64()),
            public_origin: Some("https://dash.example.com".to_string()),
            allow_insecure: false,
            allow_mutations: false,
            ..Default::default()
        }
    }

    fn build(bind: SocketAddr, env: &DashboardEnv) -> Arc<DashboardAuth> {
        DashboardAuth::from_env(bind, env).unwrap().auth
    }

    #[test]
    fn github_oauth_state_is_signed_bound_and_single_purpose() {
        let auth = build(loopback(), &env_with_token());
        let (state, verifier, cookie) = auth.issue_github_oauth_state();
        assert_eq!(
            auth.verify_github_oauth_state(&cookie, &state),
            Some(verifier)
        );
        assert!(
            auth.verify_github_oauth_state(&cookie, "wrong-state")
                .is_none()
        );
        assert!(auth.verify_github_oauth_state("tampered", &state).is_none());
    }

    #[test]
    fn github_sso_env_counts_as_authenticated_startup() {
        let mut env = env_with_token();
        env.token = None;
        env.github_client_id = Some("client-id".to_string());
        env.github_client_secret = Some("client-secret".to_string());
        env.github_allowed_users = Some("octocat".to_string());
        let decision = startup_route_decision(public_bind(), &env);
        assert!(
            decision.should_register(),
            "complete GitHub SSO config should satisfy non-loopback auth"
        );
        let auth = build(public_bind(), &env);
        assert!(auth.github_sso_enabled());
    }

    #[test]
    fn github_admin_users_are_explicit_not_allowed_by_default() {
        let auth = build(public_bind(), &env_with_token());
        let env = DashboardEnv {
            github_client_id: Some("client-id".to_string()),
            github_client_secret: Some("client-secret".to_string()),
            github_allowed_users: Some("octocat".to_string()),
            ..Default::default()
        };
        let config = GithubOAuthConfig::from_values(&env, auth.public_origin(), false, false)
            .expect("config");
        assert!(config.admin_users.is_empty());
        assert!(
            !config.admin_users.contains("octocat"),
            "allowed users are not admins unless listed in LLMCONDUIT_GITHUB_ADMIN_USERS"
        );
    }

    #[test]
    fn github_callback_url_is_strictly_validated() {
        let auth = build(public_bind(), &env_with_token());
        let mut env = DashboardEnv {
            github_client_id: Some("client-id".to_string()),
            github_client_secret: Some("client-secret".to_string()),
            github_callback_url: Some(
                "https://dash.example.com/dashboard/auth/github/callback".to_string(),
            ),
            github_allowed_users: Some("octocat".to_string()),
            github_admin_users: Some("octocat".to_string()),
            ..Default::default()
        };
        let ok = GithubOAuthConfig::from_values(&env, auth.public_origin(), false, false)
            .expect("valid callback");
        assert_eq!(
            ok.callback_url,
            "https://dash.example.com/dashboard/auth/github/callback"
        );

        for bad in [
            "https://dash.example.com/dashboard/auth/github/callback?code=secret",
            "https://dash.example.com/dashboard/auth/github/callback#frag",
            "https://user:pw@dash.example.com/dashboard/auth/github/callback",
            "https://dash.example.com/other/callback",
            "https://attacker.example.com/dashboard/auth/github/callback",
            "http://dash.example.com/dashboard/auth/github/callback",
        ] {
            env.github_callback_url = Some(bad.to_string());
            assert!(
                GithubOAuthConfig::from_values(&env, auth.public_origin(), false, false,).is_err(),
                "bad callback should be rejected: {bad}"
            );
        }
    }

    /// Construct a `DashboardAuth` directly with an explicit `loopback`/
    /// `public_origin`. `from_env` deliberately REFUSES some of these states
    /// (e.g. a non-loopback bind with no origin under the insecure override — D7a
    /// R3 #2), so the raw constructor lets us still exercise the lower-level
    /// `origin_allowed`/`authenticate_ws` invariants for those configurations.
    fn raw_auth(loopback: bool, public_origin: Option<&str>) -> Arc<DashboardAuth> {
        Arc::new(DashboardAuth {
            has_token: true,
            token_digest: Some(Sha256::digest(b"t").into()),
            session_key: [7u8; 32].to_vec(),
            public_origin: public_origin
                .map(|raw| PublicOrigin::parse(raw, true).expect("valid test origin")),
            loopback,
            allow_insecure: false,
            allow_mutations: false,
            github_oauth: None,
        })
    }

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    // -- startup route decision -------------------------------------------

    #[test]
    fn non_loopback_without_token_refuses_routes() {
        let env = DashboardEnv {
            token: None,
            public_origin: Some("https://dash.example.com".to_string()),
            ..Default::default()
        };
        let decision = startup_route_decision(public_bind(), &env);
        assert_eq!(decision, RouteDecision::Refuse(RouteRefusal::MissingToken));
        assert!(!decision.should_register());
    }

    #[test]
    fn non_loopback_without_https_origin_refuses_routes() {
        // Token + key present so the https-origin check is the one that fires.
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: Some(key_b64()),
            public_origin: None,
            ..Default::default()
        };
        let decision = startup_route_decision(public_bind(), &env);
        assert_eq!(
            decision,
            RouteDecision::Refuse(RouteRefusal::MissingHttpsOrigin)
        );
    }

    #[test]
    fn non_loopback_with_http_origin_is_treated_as_missing() {
        // A non-https origin is not a valid PUBLIC_ORIGIN → refuse (token + key
        // present so the origin check is reached).
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: Some(key_b64()),
            public_origin: Some("http://dash.example.com".to_string()),
            ..Default::default()
        };
        let decision = startup_route_decision(public_bind(), &env);
        assert_eq!(
            decision,
            RouteDecision::Refuse(RouteRefusal::MissingHttpsOrigin)
        );
    }

    #[test]
    fn non_loopback_bare_env_refuses_on_token_first() {
        // With nothing configured, the token is the first (and unrelaxable)
        // requirement checked → MissingToken (not origin).
        let env = DashboardEnv::default();
        let decision = startup_route_decision(public_bind(), &env);
        assert_eq!(decision, RouteDecision::Refuse(RouteRefusal::MissingToken));
    }

    #[test]
    fn non_loopback_with_token_but_no_session_key_refuses() {
        // Token + https origin present but NO session key → refuse (ephemeral
        // key generation is loopback-only; the spec requires the key here).
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: None,
            public_origin: Some("https://dash.example.com".to_string()),
            ..Default::default()
        };
        let decision = startup_route_decision(public_bind(), &env);
        assert_eq!(
            decision,
            RouteDecision::Refuse(RouteRefusal::MissingSessionKey)
        );
    }

    #[test]
    fn non_loopback_with_short_session_key_refuses() {
        // A present-but-too-short key is as good as missing for the predicate.
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: Some(base64::engine::general_purpose::STANDARD.encode([1u8; 16])),
            public_origin: Some("https://dash.example.com".to_string()),
            ..Default::default()
        };
        let decision = startup_route_decision(public_bind(), &env);
        assert_eq!(
            decision,
            RouteDecision::Refuse(RouteRefusal::MissingSessionKey)
        );
    }

    // -- insecure override ---------------------------------------------------

    #[test]
    fn insecure_override_explicitly_allows_tokenless_nonloopback() {
        let env = DashboardEnv {
            token: None,
            session_key_b64: None,
            public_origin: None,
            allow_insecure: true,
            ..Default::default()
        };
        let decision = startup_route_decision(public_bind(), &env);
        let RouteDecision::Register { warnings } = decision else {
            panic!("explicit tokenless insecure mode must register")
        };
        assert!(!warnings.is_empty(), "open non-loopback mode must warn");

        let auth = build(public_bind(), &env);
        assert!(auth.dev_open());
        assert_eq!(
            auth.authenticate(&HeaderMap::new()).map(|s| s.exp),
            Some(u64::MAX)
        );

        let same_origin = headers_with(&[
            ("origin", "http://192.168.1.10:4000"),
            ("host", "192.168.1.10:4000"),
        ]);
        assert_eq!(auth.authenticate_ws(&same_origin), Some(u64::MAX));

        let cross_origin = headers_with(&[
            ("origin", "http://attacker.example.com"),
            ("host", "192.168.1.10:4000"),
        ]);
        assert_eq!(auth.authenticate_ws(&cross_origin), None);
    }

    #[test]
    fn insecure_override_does_not_relax_missing_session_key() {
        // ALLOW_INSECURE=1 + token but NO session key → still refuse (key is not
        // a TLS concern; only the https origin is relaxed).
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: None,
            public_origin: None,
            allow_insecure: true,
            ..Default::default()
        };
        let decision = startup_route_decision(public_bind(), &env);
        assert_eq!(
            decision,
            RouteDecision::Refuse(RouteRefusal::MissingSessionKey)
        );
    }

    #[test]
    fn insecure_override_relaxes_scheme_but_still_requires_explicit_origin() {
        // D7a R3 #2: ALLOW_INSECURE=1 + token + valid key but NO origin → REFUSE.
        // The override relaxes only the origin scheme (TLS); it does NOT waive the
        // exact-origin requirement, and an origin-less off-loopback bind cannot
        // validate cross-site WS upgrades (Host is attacker-controllable).
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: Some(key_b64()),
            public_origin: None,
            allow_insecure: true,
            ..Default::default()
        };
        let decision = startup_route_decision(public_bind(), &env);
        assert_eq!(
            decision,
            RouteDecision::Refuse(RouteRefusal::MissingHttpsOrigin),
            "insecure override without an explicit origin must refuse"
        );
        assert!(!decision.should_register());
        // The constructor fails closed on the same condition (no origin-less
        // insecure non-loopback context can be built).
        assert!(
            DashboardAuth::from_env(public_bind(), &env).is_err(),
            "from_env must fail closed without an explicit origin under the override"
        );

        // WITH an explicit http origin, the override registers (scheme relaxed)
        // and the built context enforces real auth (token set → not dev-open).
        let env_with_origin = DashboardEnv {
            public_origin: Some("http://dash.lan:8080".to_string()),
            ..env
        };
        let decision = startup_route_decision(public_bind(), &env_with_origin);
        match &decision {
            RouteDecision::Register { warnings } => {
                assert!(!warnings.is_empty(), "insecure override must warn");
            }
            RouteDecision::Refuse(r) => panic!("expected register, got refuse: {r:?}"),
        }
        let auth = build(public_bind(), &env_with_origin);
        assert!(!auth.dev_open(), "a configured token must disable dev-open");
        assert!(
            auth.authenticate(&HeaderMap::new()).is_none(),
            "no cookie/bearer → unauthenticated even under the insecure override"
        );
    }

    #[test]
    fn non_loopback_with_token_and_https_origin_registers() {
        let decision = startup_route_decision(public_bind(), &env_with_token());
        assert!(decision.should_register());
    }

    #[test]
    fn insecure_override_registers_with_warning_when_token_key_and_origin_present() {
        // The override's legitimate use: token + valid key + an EXPLICIT (http)
        // origin present, only the origin *scheme* (TLS) relaxed (air-gapped LAN).
        // It registers WITH a warning. (A tokenless/keyless override is covered by
        // the dedicated `insecure_override_does_not_relax_*` tests; an origin-less
        // override is covered by `insecure_override_relaxes_scheme_but_still_*` —
        // those must REFUSE.)
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: Some(key_b64()),
            public_origin: Some("http://dash.lan:8080".to_string()),
            allow_insecure: true,
            ..Default::default()
        };
        let decision = startup_route_decision(public_bind(), &env);
        match decision {
            RouteDecision::Register { warnings } => {
                assert!(!warnings.is_empty(), "insecure override must warn");
            }
            RouteDecision::Refuse(_) => panic!("insecure override should register"),
        }
    }

    #[test]
    fn loopback_without_token_registers_with_warning() {
        let env = DashboardEnv::default();
        let decision = startup_route_decision(loopback(), &env);
        match decision {
            RouteDecision::Register { warnings } => {
                assert!(!warnings.is_empty(), "tokenless loopback must warn");
            }
            RouteDecision::Refuse(_) => panic!("loopback dev should register"),
        }
    }

    #[test]
    fn loopback_with_token_registers_without_warning() {
        let env = env_with_token();
        let decision = startup_route_decision(loopback(), &env);
        assert_eq!(decision, RouteDecision::Register { warnings: vec![] });
    }

    // -- key + origin validation ------------------------------------------

    #[test]
    fn short_session_key_is_rejected() {
        let env = DashboardEnv {
            session_key_b64: Some(base64::engine::general_purpose::STANDARD.encode([1u8; 16])),
            ..env_with_token()
        };
        let err = DashboardAuth::from_env(loopback(), &env).unwrap_err();
        assert!(err.contains("at least 32 bytes"), "got: {err}");
    }

    #[test]
    fn loopback_auto_generates_key_and_warns() {
        let env = DashboardEnv {
            session_key_b64: None,
            ..Default::default()
        };
        let build = DashboardAuth::from_env(loopback(), &env).unwrap();
        assert!(
            build.warnings.iter().any(|w| w.contains("temporary")),
            "expected a temporary-key warning"
        );
        // The auto-generated key still signs/verifies.
        let (cookie, _) = build.auth.issue_session();
        assert!(build.auth.verify_session(&cookie).is_some());
    }

    #[test]
    fn bad_public_origin_is_rejected() {
        let env = DashboardEnv {
            public_origin: Some("not a url".to_string()),
            ..env_with_token()
        };
        assert!(DashboardAuth::from_env(loopback(), &env).is_err());
    }

    #[test]
    fn public_origin_normalizes_to_bare_origin() {
        assert_eq!(
            PublicOrigin::parse("https://dash.example.com/", false)
                .unwrap()
                .as_str(),
            "https://dash.example.com"
        );
        assert_eq!(
            PublicOrigin::parse("https://dash.example.com:8443", false)
                .unwrap()
                .as_str(),
            "https://dash.example.com:8443"
        );
        assert!(PublicOrigin::parse("https://dash.example.com/path", false).is_err());
        // D7a R3 #5: query, fragment, and userinfo/credentials are rejected
        // (never silently discarded) — an origin is scheme+host(+port) only.
        assert!(PublicOrigin::parse("https://dash.example.com?x=1", false).is_err());
        assert!(PublicOrigin::parse("https://dash.example.com/?x=1", false).is_err());
        assert!(PublicOrigin::parse("https://dash.example.com#frag", false).is_err());
        assert!(PublicOrigin::parse("https://user@dash.example.com", false).is_err());
        assert!(PublicOrigin::parse("https://user:pw@dash.example.com", false).is_err());
        // Without the insecure override, `http://` is rejected.
        assert!(PublicOrigin::parse("http://dash.example.com", false).is_err());
        // D7a R2 #2: WITH the insecure override, an `http://` origin is accepted
        // and its scheme is preserved so it can be exact-matched against an
        // `http` `Origin` header (an air-gapped plaintext LAN still gets an exact
        // origin instead of a Host fallback).
        assert_eq!(
            PublicOrigin::parse("http://dash.lan:8080", true)
                .unwrap()
                .as_str(),
            "http://dash.lan:8080"
        );
        // A bogus scheme is still rejected even under the override.
        assert!(PublicOrigin::parse("ftp://dash.lan", true).is_err());
    }

    #[test]
    fn parse_error_never_leaks_token_bearing_origin() {
        // D7a R4: a malformed origin may embed a credential or query token; the
        // returned/logged error must NOT echo any part of it.
        let secret = "sup3r-s3cret-token-value";
        let inputs = [
            format!("https://user:{secret}@dash.example.com"),
            format!("https://dash.example.com?access={secret}"),
            format!("https://dash.example.com/#{secret}"),
            format!("ht!tp://{secret}.example.com"), // bad scheme/parse path
        ];
        for raw in inputs {
            let err =
                PublicOrigin::parse(&raw, true).expect_err("token-bearing origin must be rejected");
            assert!(
                !err.contains(secret),
                "error leaked the secret: input={raw:?} err={err:?}"
            );
            // And the env-var name is still present so the operator knows what to fix.
            assert!(err.contains(ENV_PUBLIC_ORIGIN), "err={err:?}");
        }
    }

    #[test]
    fn debug_redacts_secrets() {
        let mut env = env_with_token();
        env.github_client_id = Some("client-id".to_string());
        env.github_client_secret = Some("github-client-secret".to_string());
        env.github_allowed_users = Some("octocat".to_string());
        let rendered_env = format!("{env:?}");
        assert!(!rendered_env.contains("s3cret-token"));
        assert!(!rendered_env.contains("github-client-secret"));
        assert!(!rendered_env.contains(&key_b64()));

        let auth = build(loopback(), &env);
        let rendered = format!("{auth:?}");
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("s3cret-token"));
        assert!(!rendered.contains(&key_b64()));
    }

    // -- token compare -----------------------------------------------------

    #[test]
    fn token_compare_is_exact() {
        let auth = build(public_bind(), &env_with_token());
        assert!(auth.verify_token("s3cret-token"));
        assert!(!auth.verify_token("s3cret-toke"));
        assert!(!auth.verify_token("s3cret-tokenX"));
        assert!(!auth.verify_token("wrong"));
        assert!(!auth.verify_token(""));
    }

    #[test]
    fn tokenless_loopback_accepts_any_token() {
        let env = DashboardEnv {
            token: None,
            ..Default::default()
        };
        let auth = build(loopback(), &env);
        assert!(auth.verify_token("anything"));
    }

    // -- length-independent constant-time compare -------------------------

    #[test]
    fn ct_eq_padded_equal_is_true() {
        assert!(bool::from(b"abcdef".ct_eq_padded(b"abcdef")));
        // Empty == empty.
        assert!(bool::from(b"".ct_eq_padded(b"")));
        // Works at the HMAC width we compare in practice.
        let a = [7u8; 32];
        assert!(bool::from(a.ct_eq_padded(&[7u8; 32])));
    }

    #[test]
    fn ct_eq_padded_unequal_same_length_is_false() {
        assert!(!bool::from(b"abcdef".ct_eq_padded(b"abcdeg")));
        let mut b = [7u8; 32];
        b[31] = 8;
        assert!(!bool::from([7u8; 32].ct_eq_padded(&b)));
    }

    #[test]
    fn ct_eq_padded_different_length_is_false() {
        // A prefix/suffix relationship must NOT compare equal, and the length
        // bit is folded in even when one side is empty.
        assert!(!bool::from(b"abc".ct_eq_padded(b"abcdef")));
        assert!(!bool::from(b"abcdef".ct_eq_padded(b"abc")));
        assert!(!bool::from(b"".ct_eq_padded(b"x")));
        assert!(!bool::from(b"x".ct_eq_padded(b"")));
    }

    #[test]
    fn token_compare_rejects_length_mismatch_via_ct() {
        // Drives the token path (D7a R3 #4: presented token hashed per call, then
        // its digest compared against the precomputed configured digest): a
        // shorter and a longer presentation both produce a different digest and
        // are rejected without an early length branch.
        let auth = build(public_bind(), &env_with_token());
        assert!(!auth.verify_token("s3cret-tok")); // shorter
        assert!(!auth.verify_token("s3cret-token-extra")); // longer
    }

    // -- session cookie sign/verify ---------------------------------------

    #[test]
    fn session_roundtrips() {
        let auth = build(public_bind(), &env_with_token());
        let (cookie, exp) = auth.issue_session();
        let verified = auth.verify_session(&cookie).expect("valid cookie verifies");
        assert_eq!(verified, (exp, None));
    }

    #[test]
    fn user_session_roundtrips_the_embedded_user() {
        let auth = build(public_bind(), &env_with_token());
        let user = crate::accounts::SessionUser {
            id: "u-1".to_string(),
            username: "koen".to_string(),
            is_admin: true,
        };
        let (cookie, exp) = auth.issue_session_for(&user);
        let (verified_exp, verified_user) = auth.verify_session(&cookie).expect("valid");
        assert_eq!(verified_exp, exp);
        assert_eq!(verified_user, Some(user));
        // Tampering with the embedded user (swapping the signed segment for an
        // admin "root") breaks the MAC.
        let (mac, payload) = cookie.split_once('.').unwrap();
        let mut parts = payload.splitn(3, ':');
        let (exp_text, nonce) = (parts.next().unwrap(), parts.next().unwrap());
        let root = crate::accounts::SessionUser {
            id: "u-0".to_string(),
            username: "root".to_string(),
            is_admin: true,
        };
        let forged_user = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&root).unwrap());
        let forged = format!("{mac}.{exp_text}:{nonce}:{forged_user}");
        assert!(auth.verify_session(&forged).is_none());
        let headers = headers_with(&[("cookie", &format!("{SESSION_COOKIE}={cookie}"))]);
        assert_eq!(
            auth.authenticate(&headers).unwrap().user.unwrap().username,
            "koen"
        );
    }

    #[test]
    fn delegated_cookie_is_typed_and_never_accepted_as_bootstrap() {
        let auth = build(public_bind(), &env_with_token());
        let exp = now_unix() + 300;
        let cookie = auth.issue_delegated_session("dsh_example", exp);
        assert!(auth.verify_session(&cookie).is_none());
        let headers = headers_with(&[("cookie", &format!("{SESSION_COOKIE}={cookie}"))]);
        assert_eq!(
            auth.delegated_session(&headers),
            Some(("dsh_example".to_string(), exp))
        );
        assert!(auth.authenticate(&headers).is_none());
    }

    #[test]
    fn tampered_session_mac_is_rejected() {
        let auth = build(public_bind(), &env_with_token());
        let (cookie, _) = auth.issue_session();
        let (_, payload) = cookie.split_once('.').unwrap();
        // Forge a MAC over the same payload.
        let forged = format!("{}.{payload}", URL_SAFE_NO_PAD.encode([0u8; 32]));
        assert!(auth.verify_session(&forged).is_none());
    }

    #[test]
    fn cross_key_session_is_rejected() {
        let auth_a = build(public_bind(), &env_with_token());
        let env_b = DashboardEnv {
            session_key_b64: Some(base64::engine::general_purpose::STANDARD.encode([9u8; 32])),
            ..env_with_token()
        };
        let auth_b = build(public_bind(), &env_b);
        let (cookie, _) = auth_a.issue_session();
        assert!(auth_b.verify_session(&cookie).is_none());
    }

    #[test]
    fn expired_session_is_rejected() {
        let auth = build(public_bind(), &env_with_token());
        // Sign with an exp in the past.
        let cookie = auth.sign_session(now_unix().saturating_sub(10), None);
        assert!(auth.verify_session(&cookie).is_none());
    }

    #[test]
    fn malformed_session_values_are_rejected() {
        let auth = build(public_bind(), &env_with_token());
        assert!(auth.verify_session("").is_none());
        assert!(auth.verify_session("no-dot").is_none());
        assert!(auth.verify_session(".onlypayload").is_none());
        assert!(auth.verify_session("notbase64!.123:abc").is_none());
    }

    // -- request authentication -------------------------------------------

    #[test]
    fn valid_cookie_authenticates() {
        let auth = build(public_bind(), &env_with_token());
        let (cookie, exp) = auth.issue_session();
        let headers = headers_with(&[("cookie", &format!("{SESSION_COOKIE}={cookie}"))]);
        assert_eq!(auth.authenticate(&headers).map(|s| s.exp), Some(exp));
    }

    #[test]
    fn missing_cookie_does_not_authenticate() {
        let auth = build(public_bind(), &env_with_token());
        assert!(auth.authenticate(&HeaderMap::new()).is_none());
    }

    #[test]
    fn bearer_fallback_authenticates_with_configured_token() {
        let auth = build(public_bind(), &env_with_token());
        let headers = headers_with(&[("authorization", "Bearer s3cret-token")]);
        assert_eq!(auth.authenticate(&headers).map(|s| s.exp), Some(u64::MAX));
        let bad = headers_with(&[("authorization", "Bearer nope")]);
        assert!(auth.authenticate(&bad).is_none());
    }

    #[test]
    fn cookie_is_selected_among_multiple_cookies() {
        let auth = build(public_bind(), &env_with_token());
        let (cookie, _) = auth.issue_session();
        let headers = headers_with(&[(
            "cookie",
            &format!("other=1; {SESSION_COOKIE}={cookie}; {CSRF_COOKIE}=abc"),
        )]);
        assert!(auth.authenticate(&headers).is_some());
    }

    // -- WS auth (cookie + origin) ----------------------------------------

    #[test]
    fn ws_accepts_valid_cookie_and_public_origin() {
        let auth = build(public_bind(), &env_with_token());
        let (cookie, exp) = auth.issue_session();
        let headers = headers_with(&[
            ("cookie", &format!("{SESSION_COOKIE}={cookie}")),
            ("origin", "https://dash.example.com"),
        ]);
        assert_eq!(auth.authenticate_ws(&headers), Some(exp));
    }

    #[test]
    fn ws_accepts_valid_cookie_and_same_host_origin() {
        let env = DashboardEnv {
            public_origin: None,
            ..env_with_token()
        };
        let auth = build(loopback(), &env);
        let (cookie, _) = auth.issue_session();
        let headers = headers_with(&[
            ("cookie", &format!("{SESSION_COOKIE}={cookie}")),
            ("origin", "http://127.0.0.1:4000"),
            ("host", "127.0.0.1:4000"),
        ]);
        assert!(auth.authenticate_ws(&headers).is_some());
    }

    #[test]
    fn ws_loopback_rejects_non_loopback_host_anti_rebinding() {
        // D7a R4 (DNS-rebinding): on a loopback bind with NO configured
        // PUBLIC_ORIGIN, the Host-derived fallback must accept ONLY literal
        // loopback host names. A rebound name (`evil.com` → 127.0.0.1) supplying a
        // matching Origin+Host must be REJECTED so it cannot reach `/debug/ws`.
        let env = DashboardEnv {
            public_origin: None,
            ..env_with_token()
        };
        let auth = build(loopback(), &env);
        let (cookie, _) = auth.issue_session();
        let rebound = headers_with(&[
            ("cookie", &format!("{SESSION_COOKIE}={cookie}")),
            ("origin", "http://evil.com"),
            ("host", "evil.com"),
        ]);
        assert!(
            auth.authenticate_ws(&rebound).is_none(),
            "a non-loopback (rebound) Host must be rejected even in loopback dev"
        );
        // Real localhost development still works (literal loopback names).
        for (origin, host) in [
            ("http://localhost:4000", "localhost:4000"),
            ("http://127.0.0.1:4000", "127.0.0.1:4000"),
            ("http://[::1]:4000", "[::1]:4000"),
        ] {
            let ok = headers_with(&[
                ("cookie", &format!("{SESSION_COOKIE}={cookie}")),
                ("origin", origin),
                ("host", host),
            ]);
            assert!(
                auth.authenticate_ws(&ok).is_some(),
                "literal loopback host {host} must still pass"
            );
        }
    }

    #[test]
    fn is_loopback_host_accepts_only_literal_loopback() {
        // Literal loopback names (with/without port, bracketed IPv6) pass.
        for host in [
            "localhost",
            "localhost:4000",
            "127.0.0.1",
            "127.0.0.1:8080",
            "[::1]",
            "[::1]:443",
            "::1",
        ] {
            assert!(is_loopback_host(host), "{host} should be loopback");
        }
        // Anything else — including names that merely resolve to loopback, or a
        // loopback substring — is rejected.
        for host in [
            "evil.com",
            "evil.com:4000",
            "127.0.0.1.evil.com",
            "notlocalhost",
            "localhost.evil.com",
            "10.0.0.1",
            "dash.lan:8080",
            "",
        ] {
            assert!(!is_loopback_host(host), "{host} should NOT be loopback");
        }
    }

    #[test]
    fn ws_rejects_cross_origin() {
        let auth = build(public_bind(), &env_with_token());
        let (cookie, _) = auth.issue_session();
        let headers = headers_with(&[
            ("cookie", &format!("{SESSION_COOKIE}={cookie}")),
            ("origin", "https://evil.example.com"),
            ("host", "dash.example.com"),
        ]);
        assert!(auth.authenticate_ws(&headers).is_none());
    }

    #[test]
    fn ws_with_public_origin_ignores_attacker_host_header() {
        // With a configured PUBLIC_ORIGIN, a forged `Host` matching a malicious
        // `Origin` must NOT be accepted (Host is attacker-controllable).
        let auth = build(public_bind(), &env_with_token());
        let (cookie, _) = auth.issue_session();
        let headers = headers_with(&[
            ("cookie", &format!("{SESSION_COOKIE}={cookie}")),
            ("origin", "https://evil.example.com"),
            ("host", "evil.example.com"),
        ]);
        assert!(auth.authenticate_ws(&headers).is_none());
    }

    #[test]
    fn ws_rejects_bad_cookie_even_with_good_origin() {
        let auth = build(public_bind(), &env_with_token());
        let headers = headers_with(&[
            ("cookie", &format!("{SESSION_COOKIE}=forged.0:0")),
            ("origin", "https://dash.example.com"),
        ]);
        assert!(auth.authenticate_ws(&headers).is_none());
    }

    #[test]
    fn ws_does_not_honor_bearer_fallback() {
        // A browser WebSocket can't set Authorization; bearer must NOT bypass
        // the Origin check on the WS path.
        let auth = build(public_bind(), &env_with_token());
        let headers = headers_with(&[
            ("authorization", "Bearer s3cret-token"),
            ("origin", "https://evil.example.com"),
        ]);
        assert!(auth.authenticate_ws(&headers).is_none());
    }

    #[test]
    fn ws_nonloopback_without_origin_rejects_host_fallback() {
        // D7a R2 #2 (CSWSH): on a NON-loopback bind with NO configured
        // PUBLIC_ORIGIN, a browser WS upgrade carrying an `Origin` must be
        // REJECTED — the `Host`-derived same-origin fallback is loopback-only, so
        // an attacker page on `https://evil` with a forged `Host` must NOT ride a
        // stolen cookie. (D7a R3 #2: `from_env` now refuses to build this state
        // via the insecure override, so we construct the raw context directly to
        // exercise the `origin_allowed` invariant it still must uphold.)
        let auth = raw_auth(false, None);
        let (cookie, _) = auth.issue_session();
        // Attacker supplies a matching Origin+Host (the loopback fallback would
        // have accepted this); off loopback it must be rejected.
        let forged = headers_with(&[
            ("cookie", &format!("{SESSION_COOKIE}={cookie}")),
            ("origin", "http://attacker.example.com"),
            ("host", "attacker.example.com"),
        ]);
        assert!(
            auth.authenticate_ws(&forged).is_none(),
            "Host-derived origin must NOT be trusted off loopback"
        );
        // A non-browser client (no Origin) still passes on the cookie alone.
        let no_origin = headers_with(&[("cookie", &format!("{SESSION_COOKIE}={cookie}"))]);
        assert!(auth.authenticate_ws(&no_origin).is_some());
    }

    #[test]
    fn ws_insecure_nonloopback_with_explicit_http_origin_exact_matches() {
        // D7a R2 #2: with an EXPLICIT http PUBLIC_ORIGIN under the insecure
        // override, the WS upgrade is accepted ONLY for that exact origin and
        // rejected for any other — never a Host fallback.
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: Some(key_b64()),
            public_origin: Some("http://dash.lan:8080".to_string()),
            allow_insecure: true,
            ..Default::default()
        };
        let auth = build(public_bind(), &env);
        let (cookie, exp) = auth.issue_session();
        let exact = headers_with(&[
            ("cookie", &format!("{SESSION_COOKIE}={cookie}")),
            ("origin", "http://dash.lan:8080"),
            // A forged Host must be irrelevant when an origin is configured.
            ("host", "attacker.example.com"),
        ]);
        assert_eq!(auth.authenticate_ws(&exact), Some(exp));
        let other = headers_with(&[
            ("cookie", &format!("{SESSION_COOKIE}={cookie}")),
            ("origin", "http://attacker.example.com"),
            ("host", "dash.lan:8080"),
        ]);
        assert!(
            auth.authenticate_ws(&other).is_none(),
            "only the exact configured origin is accepted"
        );
    }

    #[test]
    fn from_env_insecure_nonloopback_without_origin_fails_closed() {
        // D7a R3 #2: the insecure-override non-loopback bind with NO origin now
        // FAILS CLOSED (token + key present, but the override does not waive the
        // exact-origin requirement — an origin-less off-loopback bind cannot
        // validate cross-site WS upgrades).
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: Some(key_b64()),
            public_origin: None,
            allow_insecure: true,
            ..Default::default()
        };
        let err = DashboardAuth::from_env(public_bind(), &env).unwrap_err();
        assert!(
            err.contains(ENV_PUBLIC_ORIGIN),
            "expected an explicit-origin requirement error: {err}"
        );
        // WITH an explicit http origin under the override it builds.
        let env_ok = DashboardEnv {
            public_origin: Some("http://dash.lan:8080".to_string()),
            ..env
        };
        assert!(DashboardAuth::from_env(public_bind(), &env_ok).is_ok());
    }

    // -- fail-closed construction (D7a R2 #4) -----------------------------

    #[test]
    fn from_env_requires_override_for_tokenless_nonloopback() {
        let env = DashboardEnv {
            token: None,
            session_key_b64: Some(key_b64()),
            public_origin: Some("https://dash.example.com".to_string()),
            ..Default::default()
        };
        let err = DashboardAuth::from_env(public_bind(), &env).unwrap_err();
        assert!(
            err.contains(ENV_TOKEN),
            "tokenless non-loopback must fail closed: {err}"
        );
        // The explicit override permits the otherwise-rejected dev-open state.
        let env_insecure = DashboardEnv {
            allow_insecure: true,
            session_key_b64: None,
            public_origin: None,
            ..env
        };
        let build = DashboardAuth::from_env(public_bind(), &env_insecure)
            .expect("explicit insecure override should allow tokenless non-loopback");
        assert!(build.auth.dev_open());
        assert!(!build.warnings.is_empty());
        // The loopback dev concession still allows a tokenless build.
        let env_loopback = DashboardEnv {
            token: None,
            ..Default::default()
        };
        assert!(DashboardAuth::from_env(loopback(), &env_loopback).is_ok());
    }

    #[test]
    fn from_env_rejects_blank_token_nonloopback() {
        // D7a R3 #6: a directly-constructed env with an empty or whitespace-only
        // token must be treated as tokenless (not a blank-token dashboard) and
        // rejected on a non-loopback bind.
        for blank in ["", "   ", "\t\n"] {
            let env = DashboardEnv {
                token: Some(blank.to_string()),
                session_key_b64: Some(key_b64()),
                public_origin: Some("https://dash.example.com".to_string()),
                ..Default::default()
            };
            let err = DashboardAuth::from_env(public_bind(), &env).unwrap_err();
            assert!(
                err.contains(ENV_TOKEN),
                "blank token {blank:?} must fail closed on non-loopback: {err}"
            );
        }
        // On loopback a blank token is the tokenless dev concession → dev-open.
        let env_loopback = DashboardEnv {
            token: Some("   ".to_string()),
            ..Default::default()
        };
        let auth = build(loopback(), &env_loopback);
        assert!(
            auth.dev_open(),
            "a blank token on loopback is tokenless dev-open, not a blank-secret"
        );
    }

    // -- CSRF + mutation policy -------------------------------------------

    #[test]
    fn mutations_disabled_by_default() {
        let auth = build(public_bind(), &env_with_token());
        assert!(!auth.mutations_enabled());
        let csrf = auth.issue_csrf_token();
        let headers = headers_with(&[
            ("x-csrf-token", &csrf),
            ("cookie", &format!("{CSRF_COOKIE}={csrf}")),
        ]);
        assert_eq!(
            auth.authorize_mutation(&headers),
            Err(MutationDenied::Disabled)
        );
    }

    #[test]
    fn mutation_requires_matching_csrf_when_enabled() {
        let env = DashboardEnv {
            allow_mutations: true,
            ..env_with_token()
        };
        let auth = build(public_bind(), &env);
        assert!(auth.mutations_enabled());

        let csrf = auth.issue_csrf_token();
        let ok = headers_with(&[
            ("x-csrf-token", &csrf),
            ("cookie", &format!("{CSRF_COOKIE}={csrf}")),
        ]);
        assert_eq!(auth.authorize_mutation(&ok), Ok(()));

        // Mismatched header vs cookie.
        let mismatched = headers_with(&[
            ("x-csrf-token", "different"),
            ("cookie", &format!("{CSRF_COOKIE}={csrf}")),
        ]);
        assert_eq!(
            auth.authorize_mutation(&mismatched),
            Err(MutationDenied::CsrfInvalid)
        );

        // Missing header.
        let no_header = headers_with(&[("cookie", &format!("{CSRF_COOKIE}={csrf}"))]);
        assert_eq!(
            auth.authorize_mutation(&no_header),
            Err(MutationDenied::CsrfInvalid)
        );

        // Empty tokens never validate.
        let empty = headers_with(&[("x-csrf-token", ""), ("cookie", &format!("{CSRF_COOKIE}="))]);
        assert_eq!(
            auth.authorize_mutation(&empty),
            Err(MutationDenied::CsrfInvalid)
        );
    }

    // -- cookie attributes -------------------------------------------------

    #[test]
    fn session_cookie_has_required_attributes_with_secure() {
        let cookie = session_cookie("abc.def", true);
        assert!(cookie.starts_with(&format!("{SESSION_COOKIE}=abc.def")));
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Strict"));
        assert!(cookie.contains("Path=/"));
        assert!(cookie.contains(&format!("Max-Age={SESSION_TTL_SECS}")));
        assert!(cookie.contains("Secure"));
    }

    #[test]
    fn session_cookie_omits_secure_without_public_origin() {
        let cookie = session_cookie("abc.def", false);
        assert!(!cookie.contains("Secure"));
        assert!(cookie.contains("HttpOnly"));
    }

    #[test]
    fn csrf_cookie_is_not_http_only() {
        let cookie = csrf_cookie("tok", true);
        assert!(!cookie.contains("HttpOnly"));
        assert!(cookie.contains("Secure"));
        assert!(cookie.contains("SameSite=Strict"));
        assert!(cookie.contains("Path=/"));
    }

    #[tokio::test]
    async fn github_login_completion_breaks_the_cross_site_redirect_chain() {
        let auth = build(public_bind(), &env_with_token());
        let (session, _) = auth.issue_session();
        let response = github_login_complete(&auth, &session, "csrf-token");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::LOCATION).is_none());
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        let cookies: Vec<_> = response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect();
        assert!(cookies.iter().any(|cookie| {
            cookie.starts_with(&format!("{SESSION_COOKIE}="))
                && cookie.contains("SameSite=Strict")
                && cookie.contains("Secure")
        }));
        assert!(cookies.iter().any(|cookie| {
            cookie.starts_with(&format!("{CSRF_COOKIE}=csrf-token"))
                && cookie.contains("SameSite=Strict")
        }));
        assert!(cookies.iter().any(|cookie| {
            cookie.starts_with(&format!("{GITHUB_OAUTH_COOKIE}=")) && cookie.contains("Max-Age=0")
        }));

        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("http-equiv=\"refresh\""));
        assert!(body.contains("url=/dashboard"));
        assert!(!body.contains(&session));
        assert!(!body.contains("csrf-token"));
    }

    // -- Secure cookie derives from the origin SCHEME (D7a R4) -------------

    #[test]
    fn secure_cookies_true_for_https_origin() {
        // An https public origin → Secure cookies (the secure-transport default).
        let auth = build(public_bind(), &env_with_token());
        assert!(
            auth.secure_cookies(),
            "https origin must set Secure on the session/CSRF cookies"
        );
    }

    #[test]
    fn secure_cookies_false_for_http_insecure_origin() {
        // D7a R4 (FUNCTIONAL BUG): under the insecure override an http origin must
        // NOT set Secure — a browser DISCARDS a Secure cookie over plaintext http,
        // so a Secure login cookie could never be stored and login would always
        // fail in insecure-LAN mode. Derive Secure from the validated SCHEME.
        let env = DashboardEnv {
            token: Some("t".to_string()),
            session_key_b64: Some(key_b64()),
            public_origin: Some("http://dash.lan:8080".to_string()),
            allow_insecure: true,
            ..Default::default()
        };
        let auth = build(public_bind(), &env);
        assert!(
            !auth.secure_cookies(),
            "http insecure-override origin must NOT set Secure (browser would discard it)"
        );
        // The actual login response must carry the session cookie WITHOUT Secure.
        let (value, _) = auth.issue_session();
        let cookie = session_cookie(&value, auth.secure_cookies());
        assert!(
            !cookie.contains("Secure"),
            "insecure-LAN login cookie must omit Secure: {cookie}"
        );
    }

    #[test]
    fn secure_cookies_false_without_origin() {
        // Loopback dev over plain http (no configured origin) → no Secure.
        let env = DashboardEnv {
            public_origin: None,
            ..env_with_token()
        };
        let auth = build(loopback(), &env);
        assert!(!auth.secure_cookies());
    }

    // -- auth-response security headers (D7a R4) --------------------------

    #[test]
    fn no_store_stamps_full_security_header_set() {
        // EVERY auth response (401 / login / logout) must carry the hardening
        // headers, not just no-store.
        let resp = no_store((StatusCode::OK, "x").into_response());
        let h = resp.headers();
        assert_eq!(h.get(header::CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(h.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
        assert_eq!(h.get(header::REFERRER_POLICY).unwrap(), "no-referrer");
        assert_eq!(h.get(header::X_FRAME_OPTIONS).unwrap(), "DENY");
        assert!(
            h.get(header::CONTENT_SECURITY_POLICY)
                .unwrap()
                .to_str()
                .unwrap()
                .contains("frame-ancestors 'none'"),
            "CSP must lock down framing/loading on auth responses"
        );
    }

    #[test]
    fn unauthorized_401_carries_security_headers() {
        let resp = unauthorized();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let h = resp.headers();
        assert_eq!(h.get(header::CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(h.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
        assert_eq!(h.get(header::REFERRER_POLICY).unwrap(), "no-referrer");
        assert_eq!(h.get(header::X_FRAME_OPTIONS).unwrap(), "DENY");
        assert!(h.get(header::CONTENT_SECURITY_POLICY).is_some());
    }

    #[tokio::test]
    async fn login_response_carries_security_headers() {
        // The successful login response must carry the headers AND its cookies.
        let auth = build(public_bind(), &env_with_token());
        let resp = login_response(
            None,
            &auth,
            LoginRequest {
                token: Some("s3cret-token".to_string()),
                username: None,
                password: None,
            },
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let h = resp.headers();
        assert_eq!(h.get(header::CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(h.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
        assert_eq!(h.get(header::REFERRER_POLICY).unwrap(), "no-referrer");
        assert_eq!(h.get(header::X_FRAME_OPTIONS).unwrap(), "DENY");
        assert!(h.get(header::CONTENT_SECURITY_POLICY).is_some());
        // The security headers did not clobber the Set-Cookie pair.
        assert_eq!(h.get_all(header::SET_COOKIE).iter().count(), 2);
    }
}
