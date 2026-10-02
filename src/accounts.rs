//! User accounts and per-user API keys.
//!
//! Users live in the `users` table (Argon2id password hashes); API keys in
//! `api_keys` (SHA-256 digests, an optional owning user, an optional list of
//! client-facing model/alias names). The gateway's live key registry
//! ([`crate::client_auth::ClientAuth`]) is the union of the YAML-configured
//! keys and the SQL keys; [`reload_client_keys`] rebuilds it after every key
//! change so a new key works without a restart.
//!
//! Dashboard sessions carry a [`SessionUser`] when they were opened with a
//! username/password; token/dev-open sessions carry none and act as admin
//! (there is no one to attribute to).

use crate::client_auth::VirtualKeySpec;
use crate::control_plane_store::{ApiKeyAuthSpec, PersistenceStore, UserRecord};
use crate::engine::Gateway;
use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Prefix of generated API keys, so an operator can recognise one in a log.
pub const API_KEY_PREFIX: &str = "llmc_";
/// Minimum accepted password length.
pub const MIN_PASSWORD_LEN: usize = 8;
/// Bootstrap admin environment variables (used only when no user exists).
pub const ENV_ADMIN_USERNAME: &str = "LLMCONDUIT_ADMIN_USERNAME";
pub const ENV_ADMIN_PASSWORD: &str = "LLMCONDUIT_ADMIN_PASSWORD";

/// The user behind a dashboard session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SessionUser {
    /// Opaque user id (UUID string).
    pub id: String,
    pub username: String,
    /// Whether the user may manage users and other users' keys.
    pub is_admin: bool,
}

impl From<&UserRecord> for SessionUser {
    fn from(user: &UserRecord) -> Self {
        Self {
            id: user.id.clone(),
            username: user.username.clone(),
            is_admin: user.is_admin,
        }
    }
}

/// Argon2id hash of a password (PHC string).
pub fn hash_password(password: &str) -> Result<String, String> {
    if password.len() < MIN_PASSWORD_LEN {
        return Err(format!(
            "password must be at least {MIN_PASSWORD_LEN} characters"
        ));
    }
    let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| format!("password hashing failed: {error}"))
}

/// Constant-time-by-construction verification against a PHC hash. A malformed
/// stored hash verifies false rather than erroring, so a corrupt row cannot
/// become a login bypass.
pub fn verify_password(stored_hash: &str, password: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(stored_hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// A fresh API key: `llmc_` + 64 hex characters from two random UUIDs.
pub fn generate_api_key() -> String {
    format!(
        "{API_KEY_PREFIX}{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Validate a username: 1..=64 chars of `[A-Za-z0-9._@-]`.
pub fn validate_username(username: &str) -> Result<&str, String> {
    let username = username.trim();
    if username.is_empty() || username.len() > 64 {
        return Err("username must be 1-64 characters".to_string());
    }
    if !username
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '@' | '-'))
    {
        return Err("username may contain letters, digits, '.', '_', '@' and '-'".to_string());
    }
    Ok(username)
}

/// SQL key rows as live registry specs.
pub fn specs_from_sql(specs: Vec<ApiKeyAuthSpec>) -> Vec<VirtualKeySpec> {
    specs
        .into_iter()
        .map(|spec| VirtualKeySpec {
            id: spec.id,
            label: spec.label,
            owner_id: spec.user_id,
            secret_hash: spec.secret_hash,
            allowed_models: spec.allowed_models,
        })
        .collect()
}

/// Union of the configured (YAML, or legacy relational) key specs and the SQL
/// keys, keyed by id. A legacy relational database contributes its `api_keys`
/// rows through the config side as well, so the SQL copy replaces a
/// same-id config entry instead of registering it twice (which the registry
/// rejects as a duplicate id and would crash startup).
pub fn merge_key_specs(
    configured: Vec<VirtualKeySpec>,
    sql: Vec<VirtualKeySpec>,
) -> Vec<VirtualKeySpec> {
    let mut merged: Vec<VirtualKeySpec> = Vec::with_capacity(configured.len() + sql.len());
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for spec in configured {
        if seen.insert(spec.id.clone()) {
            merged.push(spec);
        }
    }
    for spec in sql {
        if let Some(existing) = merged.iter_mut().find(|m| m.id == spec.id) {
            *existing = spec;
        } else {
            merged.push(spec);
        }
    }
    merged
}

/// Rebuild the gateway's live key registry from the YAML keys plus every
/// active SQL key. Returns the number of keys now registered. A failure leaves
/// the previous registry in place.
pub async fn reload_client_keys(gateway: &Gateway) -> Result<usize, String> {
    let mut specs = gateway.yaml_key_specs().to_vec();
    if let Some(store) = gateway.persistence_store() {
        specs = merge_key_specs(specs, specs_from_sql(store.api_key_auth_specs().await?));
    }
    let count = specs.len();
    gateway
        .client_auth()
        .replace(gateway.client_auth_required(), specs)?;
    Ok(count)
}

/// Create the first admin from the environment when the store has no users.
/// Returns the created user, or `None` when nothing was done.
pub async fn bootstrap_admin_from_env(
    store: &Arc<dyn PersistenceStore>,
) -> Result<Option<UserRecord>, String> {
    let (Some(username), Some(password)) = (
        std::env::var(ENV_ADMIN_USERNAME)
            .ok()
            .filter(|v| !v.trim().is_empty()),
        std::env::var(ENV_ADMIN_PASSWORD)
            .ok()
            .filter(|v| !v.is_empty()),
    ) else {
        return Ok(None);
    };
    if store.count_users().await? > 0 {
        return Ok(None);
    }
    let username = validate_username(&username)?;
    let hash = hash_password(&password)?;
    let user = store
        .create_user(username, &hash, true, "bootstrap")
        .await?;
    Ok(Some(user))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(id: &str, label: &str) -> VirtualKeySpec {
        VirtualKeySpec {
            id: id.to_string(),
            label: Some(label.to_string()),
            owner_id: None,
            secret_hash: format!("sha256:{}", "0".repeat(64)),
            allowed_models: Vec::new(),
        }
    }

    #[test]
    fn merge_key_specs_dedupes_by_id_with_sql_precedence() {
        // A legacy relational database contributes its keys through the YAML
        // (config) side as well; the SQL side must win exactly once.
        let merged = merge_key_specs(
            vec![spec("yaml-1", "yaml"), spec("shared", "from-config")],
            vec![spec("shared", "from-sql"), spec("sql-1", "sql")],
        );
        let ids: Vec<&str> = merged.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, vec!["yaml-1", "shared", "sql-1"]);
        assert_eq!(merged[1].label.as_deref(), Some("from-sql"));
    }

    #[test]
    fn passwords_hash_and_verify_with_argon2id() {
        let hash = hash_password("correct horse battery").unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(verify_password(&hash, "correct horse battery"));
        assert!(!verify_password(&hash, "wrong"));
        assert!(!verify_password("not-a-hash", "correct horse battery"));
        assert!(hash_password("short").is_err());
        assert_ne!(
            hash,
            hash_password("correct horse battery").unwrap(),
            "salted"
        );
    }

    #[test]
    fn generated_keys_are_prefixed_unique_and_long() {
        let a = generate_api_key();
        let b = generate_api_key();
        assert!(a.starts_with(API_KEY_PREFIX));
        assert_eq!(a.len(), API_KEY_PREFIX.len() + 64);
        assert_ne!(a, b);
    }

    #[test]
    fn usernames_are_validated() {
        assert_eq!(validate_username("  koen  ").unwrap(), "koen");
        assert!(validate_username("koen@ambicero.com").is_ok());
        assert!(validate_username("").is_err());
        assert!(validate_username("has space").is_err());
        assert!(validate_username(&"x".repeat(65)).is_err());
    }

    #[test]
    fn sql_specs_map_to_registry_specs() {
        let specs = specs_from_sql(vec![ApiKeyAuthSpec {
            id: "k1".into(),
            label: Some("team".into()),
            user_id: Some("u1".into()),
            secret_hash: "sha256:abc".into(),
            allowed_models: vec!["local".into()],
        }]);
        assert_eq!(specs[0].owner_id.as_deref(), Some("u1"));
        assert_eq!(specs[0].allowed_models, ["local"]);
    }
}
