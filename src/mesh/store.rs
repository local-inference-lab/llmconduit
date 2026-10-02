use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::TransactionBehavior;
use rusqlite::params;
use sha2::Digest;
use sha2::Sha256;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::task;
use uuid::Uuid;

/// SQLite-backed mesh enrollment state.
///
/// One long-lived connection is shared by every clone (behind a blocking
/// mutex touched only from `spawn_blocking`): reopening the database per call
/// re-parsed the schema and re-negotiated locks on every worker control
/// frame, which is pure overhead on the small hub VM.
#[derive(Debug, Clone)]
pub struct MeshStore {
    path: PathBuf,
    conn: Arc<std::sync::Mutex<Option<Connection>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedJoinKey {
    pub id: String,
    pub token: String,
    pub label: Option<String>,
    pub created_at_ms: i64,
    pub expires_at_ms: Option<i64>,
    pub max_uses: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinKeyRecord {
    pub id: String,
    pub label: Option<String>,
    pub enabled: bool,
    pub created_at_ms: i64,
    pub expires_at_ms: Option<i64>,
    pub max_uses: Option<i64>,
    pub use_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshNodeRecord {
    pub endpoint_id: String,
    pub label: Option<String>,
    pub enabled: bool,
    pub joined_at_ms: i64,
    pub join_key_id: Option<String>,
    pub last_seen_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokedJoinKey {
    pub updated: bool,
    pub disabled_endpoint_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisabledMeshModelRecord {
    pub endpoint_id: String,
    pub resource_id: String,
    pub model: String,
    pub disabled_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKeyDecision {
    Accepted,
    AlreadyEnrolled,
    NodeRevoked,
    NotFound,
    Disabled,
    Expired,
    Exhausted,
}

impl MeshStore {
    pub fn at_path(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            conn: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let store = Self::at_path(path);
        store.initialize().await?;
        Ok(store)
    }

    pub async fn initialize(&self) -> Result<(), StoreError> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        self.with_conn(|conn| {
            conn.execute_batch(SCHEMA)?;
            Ok(())
        })
        .await
    }

    pub async fn create_join_key(
        &self,
        label: Option<String>,
        expires_at_ms: Option<i64>,
        max_uses: Option<i64>,
    ) -> Result<CreatedJoinKey, StoreError> {
        let token = generate_join_token();
        let token_hash = hash_join_token(&token);
        let id = format!("jk_{}", Uuid::new_v4().simple());
        let created_at_ms = now_ms();
        let clean_label = label.and_then(nonempty);
        self.with_conn({
            let id = id.clone();
            let label = clean_label.clone();
            move |conn| {
                conn.execute(
                    "INSERT INTO mesh_join_keys \
                     (id, label, token_hash, enabled, created_at_ms, expires_at_ms, max_uses, use_count) \
                     VALUES (?1, ?2, ?3, 1, ?4, ?5, ?6, 0)",
                    params![id, label, token_hash.as_slice(), created_at_ms, expires_at_ms, max_uses],
                )?;
                Ok(())
            }
        })
        .await?;

        Ok(CreatedJoinKey {
            id,
            token,
            label: clean_label,
            created_at_ms,
            expires_at_ms,
            max_uses,
        })
    }

    pub async fn list_join_keys(&self) -> Result<Vec<JoinKeyRecord>, StoreError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, label, enabled, created_at_ms, expires_at_ms, max_uses, use_count \
                 FROM mesh_join_keys ORDER BY created_at_ms DESC, id DESC",
            )?;
            let rows = stmt.query_map([], map_join_key)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
        .await
    }

    pub async fn revoke_join_key(&self, id: &str) -> Result<RevokedJoinKey, StoreError> {
        let id = id.to_string();
        self.with_conn(move |conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = tx.execute(
                "UPDATE mesh_join_keys SET enabled = 0 WHERE id = ?1 AND enabled != 0",
                params![id],
            )?;
            let disabled_endpoint_ids = if changed > 0 {
                let endpoints = {
                    let mut stmt = tx.prepare(
                        "SELECT endpoint_id FROM mesh_nodes \
                         WHERE join_key_id = ?1 AND enabled != 0 ORDER BY endpoint_id ASC",
                    )?;
                    let rows = stmt.query_map(params![id], |row| row.get::<_, String>(0))?;
                    rows.collect::<Result<Vec<_>, _>>()?
                };
                tx.execute(
                    "UPDATE mesh_nodes SET enabled = 0 WHERE join_key_id = ?1 AND enabled != 0",
                    params![id],
                )?;
                endpoints
            } else {
                Vec::new()
            };
            tx.commit()?;
            Ok(RevokedJoinKey {
                updated: changed > 0,
                disabled_endpoint_ids,
            })
        })
        .await
    }

    pub async fn validate_join_key_for_endpoint(
        &self,
        token: &str,
        endpoint_id: &str,
        node_label: Option<String>,
        now_ms: i64,
    ) -> Result<JoinKeyDecision, StoreError> {
        let token_hash = hash_join_token(token);
        let endpoint_id = endpoint_id.to_string();
        let node_label = node_label.and_then(nonempty);
        self.with_conn(move |conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existing = tx
                .query_row(
                    "SELECT enabled FROM mesh_nodes WHERE endpoint_id = ?1",
                    params![endpoint_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            match existing {
                Some(1) => return Ok(JoinKeyDecision::AlreadyEnrolled),
                Some(_) => return Ok(JoinKeyDecision::NodeRevoked),
                None => {}
            }
            let record = tx
                .query_row(
                    "SELECT id, label, enabled, created_at_ms, expires_at_ms, max_uses, use_count \
                     FROM mesh_join_keys WHERE token_hash = ?1",
                    params![token_hash.as_slice()],
                    map_join_key,
                )
                .optional()?;
            let Some(record) = record else {
                return Ok(JoinKeyDecision::NotFound);
            };
            if !record.enabled {
                return Ok(JoinKeyDecision::Disabled);
            }
            if record
                .expires_at_ms
                .is_some_and(|expires| expires <= now_ms)
            {
                return Ok(JoinKeyDecision::Expired);
            }
            if record
                .max_uses
                .is_some_and(|max_uses| record.use_count >= max_uses)
            {
                return Ok(JoinKeyDecision::Exhausted);
            }

            tx.execute(
                "UPDATE mesh_join_keys SET use_count = use_count + 1 WHERE id = ?1",
                params![record.id],
            )?;
            tx.execute(
                "INSERT INTO mesh_nodes \
                 (endpoint_id, label, enabled, joined_at_ms, join_key_id, last_seen_at_ms) \
                 VALUES (?1, ?2, 1, ?3, ?4, ?3) \
                 ON CONFLICT(endpoint_id) DO UPDATE SET \
                 label = COALESCE(excluded.label, mesh_nodes.label), enabled = 1, \
                 join_key_id = excluded.join_key_id, last_seen_at_ms = excluded.last_seen_at_ms",
                params![endpoint_id, node_label, now_ms, record.id],
            )?;
            tx.commit()?;
            Ok(JoinKeyDecision::Accepted)
        })
        .await
    }

    pub async fn list_nodes(&self) -> Result<Vec<MeshNodeRecord>, StoreError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT endpoint_id, label, enabled, joined_at_ms, join_key_id, last_seen_at_ms \
                 FROM mesh_nodes ORDER BY joined_at_ms DESC, endpoint_id ASC",
            )?;
            let rows = stmt.query_map([], map_node)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
        .await
    }

    pub async fn set_node_enabled(
        &self,
        endpoint_id: &str,
        enabled: bool,
    ) -> Result<bool, StoreError> {
        let endpoint_id = endpoint_id.to_string();
        self.with_conn(move |conn| {
            let changed = conn.execute(
                "UPDATE mesh_nodes SET enabled = ?2 WHERE endpoint_id = ?1",
                params![endpoint_id, enabled as i64],
            )?;
            Ok(changed > 0)
        })
        .await
    }

    pub async fn is_node_authorized(&self, endpoint_id: &str) -> Result<bool, StoreError> {
        Ok(self
            .node_authorization(endpoint_id)
            .await?
            .is_some_and(|node| node.enabled))
    }

    /// Enabled flag plus the operator-visible label recorded at enrollment,
    /// or `None` when the endpoint never enrolled.
    pub async fn node_authorization(
        &self,
        endpoint_id: &str,
    ) -> Result<Option<NodeAuthorization>, StoreError> {
        let endpoint_id = endpoint_id.to_string();
        self.with_conn(move |conn| {
            conn.query_row(
                "SELECT enabled, label FROM mesh_nodes WHERE endpoint_id = ?1",
                params![endpoint_id],
                |row| {
                    Ok(NodeAuthorization {
                        enabled: row.get::<_, i64>(0)? == 1,
                        label: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
        })
        .await
    }

    pub async fn touch_node_seen(
        &self,
        endpoint_id: &str,
        seen_at_ms: i64,
    ) -> Result<(), StoreError> {
        let endpoint_id = endpoint_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE mesh_nodes SET last_seen_at_ms = ?2 WHERE endpoint_id = ?1",
                params![endpoint_id, seen_at_ms],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_disabled_models(&self) -> Result<Vec<DisabledMeshModelRecord>, StoreError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT endpoint_id, resource_id, model, disabled_at_ms \
                 FROM mesh_disabled_models ORDER BY disabled_at_ms DESC, endpoint_id ASC, resource_id ASC, model ASC",
            )?;
            let rows = stmt.query_map([], map_disabled_model)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
        .await
    }

    pub async fn set_model_disabled(
        &self,
        endpoint_id: &str,
        resource_id: &str,
        model: &str,
        disabled: bool,
        disabled_at_ms: i64,
    ) -> Result<bool, StoreError> {
        let endpoint_id = endpoint_id.trim().to_string();
        let resource_id = resource_id.trim().to_string();
        let model = model.trim().to_ascii_lowercase();
        self.with_conn(move |conn| {
            if disabled {
                let changed = conn.execute(
                    "INSERT INTO mesh_disabled_models \
                     (endpoint_id, resource_id, model, disabled_at_ms) VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(endpoint_id, resource_id, model) DO UPDATE SET \
                     disabled_at_ms = excluded.disabled_at_ms",
                    params![endpoint_id, resource_id, model, disabled_at_ms],
                )?;
                Ok(changed > 0)
            } else {
                let changed = conn.execute(
                    "DELETE FROM mesh_disabled_models \
                     WHERE endpoint_id = ?1 AND resource_id = ?2 AND model = ?3",
                    params![endpoint_id, resource_id, model],
                )?;
                Ok(changed > 0)
            }
        })
        .await
    }

    async fn with_conn<F, T>(&self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
        T: Send + 'static,
    {
        let path = self.path.clone();
        let shared = Arc::clone(&self.conn);
        task::spawn_blocking(move || {
            let mut guard = shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if guard.is_none() {
                *guard = Some(open_connection(&path)?);
            }
            let conn = guard
                .as_mut()
                .expect("mesh store connection was just opened");
            f(conn)
        })
        .await
        .map_err(StoreError::Join)?
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAuthorization {
    pub enabled: bool,
    pub label: Option<String>,
}

fn open_connection(path: &Path) -> Result<Connection, StoreError> {
    ensure_private_db_file(path)?;
    let conn = Connection::open(path)?;
    // The dashboard, the controller, and the `mesh` CLI can all touch the
    // same file; WAL lets readers proceed during a write and busy_timeout
    // turns a brief lock overlap into a wait instead of SQLITE_BUSY.
    conn.busy_timeout(Duration::from_secs(5))?;
    let _: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(conn)
}

/// The store holds join-token hashes and the node allowlist, so it must not
/// be world-readable. SQLite creates its `-wal`/`-shm` siblings with the main
/// file's mode, so fixing the main file covers them too.
#[cfg(unix)]
fn ensure_private_db_file(path: &Path) -> Result<(), StoreError> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::PermissionsExt;

    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(_) => return Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(err) => return Err(err.into()),
    }
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        tracing::warn!(
            path = %path.display(),
            mode = format!("{mode:o}"),
            "mesh state database was group/world accessible; restricting it to 0600"
        );
        // Best-effort like the identity key: an unchangeable mode (read-only
        // mount, foreign owner) must not keep the controller from starting.
        if let Err(error) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            tracing::warn!(
                path = %path.display(),
                %error,
                "could not restrict mesh state database permissions; fix them on the host"
            );
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_private_db_file(_path: &Path) -> Result<(), StoreError> {
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS mesh_join_keys (
    id TEXT PRIMARY KEY,
    label TEXT,
    token_hash BLOB NOT NULL UNIQUE,
    enabled INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER,
    max_uses INTEGER,
    use_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mesh_nodes (
    endpoint_id TEXT PRIMARY KEY,
    label TEXT,
    enabled INTEGER NOT NULL,
    joined_at_ms INTEGER NOT NULL,
    join_key_id TEXT,
    last_seen_at_ms INTEGER,
    FOREIGN KEY(join_key_id) REFERENCES mesh_join_keys(id)
);

CREATE TABLE IF NOT EXISTS mesh_disabled_models (
    endpoint_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    model TEXT NOT NULL,
    disabled_at_ms INTEGER NOT NULL,
    PRIMARY KEY(endpoint_id, resource_id, model)
);
"#;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

pub fn hash_join_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn generate_join_token() -> String {
    let bytes = iroh::SecretKey::generate().to_bytes();
    format!("llmc_join_{}", URL_SAFE_NO_PAD.encode(bytes))
}

fn nonempty(value: String) -> Option<String> {
    let value = value.trim().to_string();
    (!value.is_empty()).then_some(value)
}

fn map_join_key(row: &rusqlite::Row<'_>) -> rusqlite::Result<JoinKeyRecord> {
    Ok(JoinKeyRecord {
        id: row.get(0)?,
        label: row.get(1)?,
        enabled: row.get::<_, i64>(2)? != 0,
        created_at_ms: row.get(3)?,
        expires_at_ms: row.get(4)?,
        max_uses: row.get(5)?,
        use_count: row.get(6)?,
    })
}

fn map_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<MeshNodeRecord> {
    Ok(MeshNodeRecord {
        endpoint_id: row.get(0)?,
        label: row.get(1)?,
        enabled: row.get::<_, i64>(2)? != 0,
        joined_at_ms: row.get(3)?,
        join_key_id: row.get(4)?,
        last_seen_at_ms: row.get(5)?,
    })
}

fn map_disabled_model(row: &rusqlite::Row<'_>) -> rusqlite::Result<DisabledMeshModelRecord> {
    Ok(DisabledMeshModelRecord {
        endpoint_id: row.get(0)?,
        resource_id: row.get(1)?,
        model: row.get(2)?,
        disabled_at_ms: row.get(3)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("llmconduit-{name}-{}.sqlite", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn join_key_plaintext_is_returned_once_and_hash_is_stored() {
        let path = temp_db("join-key");
        let store = MeshStore::open(&path).await.expect("open store");
        let created = store
            .create_join_key(Some("community".to_string()), None, Some(1))
            .await
            .expect("create key");

        assert!(created.token.starts_with("llmc_join_"));
        assert_eq!(store.list_join_keys().await.expect("list")[0].use_count, 0);
        let decision = store
            .validate_join_key_for_endpoint(&created.token, "node-a", Some("node".to_string()), 1)
            .await
            .expect("validate");
        assert_eq!(decision, JoinKeyDecision::Accepted);
        assert!(store.is_node_authorized("node-a").await.expect("auth"));
        assert_eq!(
            store
                .validate_join_key_for_endpoint(&created.token, "node-b", None, 2)
                .await
                .expect("validate exhausted"),
            JoinKeyDecision::Exhausted
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn revoked_and_expired_join_keys_are_rejected() {
        let path = temp_db("revoked-expired");
        let store = MeshStore::open(&path).await.expect("open store");
        let revoked = store
            .create_join_key(None, None, None)
            .await
            .expect("create key");
        assert!(
            store
                .revoke_join_key(&revoked.id)
                .await
                .expect("revoke")
                .updated
        );
        assert_eq!(
            store
                .validate_join_key_for_endpoint(&revoked.token, "node-a", None, 1)
                .await
                .expect("validate revoked"),
            JoinKeyDecision::Disabled
        );

        let expired = store
            .create_join_key(None, Some(10), None)
            .await
            .expect("create expired");
        assert_eq!(
            store
                .validate_join_key_for_endpoint(&expired.token, "node-b", None, 10)
                .await
                .expect("validate expired"),
            JoinKeyDecision::Expired
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn enrolled_identity_does_not_consume_again_and_revocation_is_sticky() {
        let path = temp_db("node-reenroll");
        let store = MeshStore::open(&path).await.expect("open store");
        let created = store
            .create_join_key(None, None, Some(2))
            .await
            .expect("create key");
        assert_eq!(
            store
                .validate_join_key_for_endpoint(&created.token, "node-a", None, 1)
                .await
                .expect("first enrollment"),
            JoinKeyDecision::Accepted
        );
        assert_eq!(
            store
                .validate_join_key_for_endpoint(&created.token, "node-a", None, 2)
                .await
                .expect("repeat enrollment"),
            JoinKeyDecision::AlreadyEnrolled
        );
        assert_eq!(store.list_join_keys().await.expect("list")[0].use_count, 1);
        assert!(
            store
                .set_node_enabled("node-a", false)
                .await
                .expect("revoke")
        );
        assert_eq!(
            store
                .validate_join_key_for_endpoint(&created.token, "node-a", None, 3)
                .await
                .expect("revoked enrollment"),
            JoinKeyDecision::NodeRevoked
        );
        assert_eq!(store.list_join_keys().await.expect("list")[0].use_count, 1);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn concurrent_one_use_key_is_consumed_once() {
        let path = temp_db("concurrent");
        let store = MeshStore::open(&path).await.expect("open store");
        let created = store
            .create_join_key(None, None, Some(1))
            .await
            .expect("create key");
        let attempts = (0..16).map(|index| {
            let store = store.clone();
            let token = created.token.clone();
            tokio::spawn(async move {
                store
                    .validate_join_key_for_endpoint(&token, &format!("node-{index}"), None, 1)
                    .await
                    .expect("validate")
            })
        });
        let accepted = futures::future::join_all(attempts)
            .await
            .into_iter()
            .map(Result::unwrap)
            .filter(|decision| *decision == JoinKeyDecision::Accepted)
            .count();

        assert_eq!(accepted, 1);
        assert_eq!(store.list_nodes().await.expect("nodes").len(), 1);
        assert_eq!(store.list_join_keys().await.expect("keys")[0].use_count, 1);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn revoking_join_key_disables_enrolled_nodes_atomically() {
        let path = temp_db("revoke-disables-nodes");
        let store = MeshStore::open(&path).await.expect("open store");
        let created = store
            .create_join_key(None, None, Some(2))
            .await
            .expect("create key");
        assert_eq!(
            store
                .validate_join_key_for_endpoint(&created.token, "node-a", None, 1)
                .await
                .expect("enroll a"),
            JoinKeyDecision::Accepted
        );
        assert_eq!(
            store
                .validate_join_key_for_endpoint(&created.token, "node-b", None, 2)
                .await
                .expect("enroll b"),
            JoinKeyDecision::Accepted
        );

        let revoked = store.revoke_join_key(&created.id).await.expect("revoke");

        assert!(revoked.updated);
        assert_eq!(revoked.disabled_endpoint_ids, vec!["node-a", "node-b"]);
        assert!(!store.is_node_authorized("node-a").await.expect("auth a"));
        assert!(!store.is_node_authorized("node-b").await.expect("auth b"));
        assert_eq!(
            store
                .validate_join_key_for_endpoint(&created.token, "node-c", None, 3)
                .await
                .expect("validate revoked"),
            JoinKeyDecision::Disabled
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn store_uses_wal_and_reuses_one_connection() {
        let path = temp_db("wal");
        let store = MeshStore::open(&path).await.expect("open store");
        let mode = store
            .with_conn(|conn| {
                conn.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                    .map_err(StoreError::from)
            })
            .await
            .expect("journal mode");
        assert_eq!(mode.to_ascii_lowercase(), "wal");
        // A connection-scoped temp table survives only if the clone reuses
        // the very same connection.
        store
            .with_conn(|conn| {
                conn.execute_batch("CREATE TEMP TABLE reuse_probe(x INTEGER)")?;
                Ok(())
            })
            .await
            .expect("create temp table");
        store
            .clone()
            .with_conn(|conn| {
                conn.execute("INSERT INTO reuse_probe(x) VALUES (1)", [])?;
                Ok(())
            })
            .await
            .expect("temp table is visible on the shared connection");
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn store_file_is_owner_only_and_loose_modes_are_repaired() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_db("mode");
        let store = MeshStore::open(&path).await.expect("open store");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        drop(store);

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let reopened = MeshStore::open(&path).await.expect("reopen store");
        reopened.list_nodes().await.expect("query");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn node_authorization_returns_enrollment_label() {
        let path = temp_db("node-label");
        let store = MeshStore::open(&path).await.expect("open store");
        let created = store
            .create_join_key(None, None, Some(1))
            .await
            .expect("create key");
        store
            .validate_join_key_for_endpoint(&created.token, "node-a", Some("gpu-box".into()), 1)
            .await
            .expect("enroll");
        assert_eq!(
            store.node_authorization("node-a").await.expect("lookup"),
            Some(NodeAuthorization {
                enabled: true,
                label: Some("gpu-box".into()),
            })
        );
        assert_eq!(
            store.node_authorization("node-b").await.expect("lookup"),
            None
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn disabled_models_round_trip() {
        let path = temp_db("disabled-models");
        let store = MeshStore::open(&path).await.expect("open store");

        assert!(
            store
                .set_model_disabled("endpoint", "gpu", "QWEN", true, 123)
                .await
                .expect("disable")
        );
        assert_eq!(
            store.list_disabled_models().await.expect("list"),
            vec![DisabledMeshModelRecord {
                endpoint_id: "endpoint".to_string(),
                resource_id: "gpu".to_string(),
                model: "qwen".to_string(),
                disabled_at_ms: 123,
            }]
        );
        assert!(
            store
                .set_model_disabled("endpoint", "gpu", "qWeN", false, 124)
                .await
                .expect("enable")
        );
        assert!(store.list_disabled_models().await.expect("list").is_empty());

        let _ = std::fs::remove_file(path);
    }
}
