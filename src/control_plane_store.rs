//! Durable control-plane persistence, deliberately independent from the gateway's
//! runtime configuration and request engine.
//!
//! The SQL schema is compatible with the historical `store.rs` migrations. Hot
//! request-path writes can be handed to [`PersistenceQueue`], whose `try_*`
//! methods never wait for the database or for queue capacity. Read/admin calls
//! use [`PersistenceStore`] directly.

use async_trait::async_trait;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::IpAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot};
use utoipa::ToSchema;

pub type StoreResult<T> = Result<T, String>;

/// Shared wire/storage form used by `client_auth`: SHA-256 is appropriate for
/// high-entropy generated API keys and lets authentication remain deterministic.
const API_KEY_HASH_PREFIX: &str = "sha256:";
const DEFAULT_PERSISTENCE_QUEUE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const PERSISTENCE_QUEUE_MAX_BYTES_ENV: &str = "LLMCONDUIT_PERSISTENCE_QUEUE_MAX_BYTES";
/// SQLite pool ceiling: one writer plus a few concurrent dashboard readers.
const SQLITE_MAX_CONNECTIONS: u32 = 4;
/// Per-connection SQLite page cache in KiB (`PRAGMA cache_size` negative form).
const SQLITE_CACHE_SIZE_PRAGMA: &str = "-8192";

/// Lifecycle row created at the HTTP ingress seam. `id` is the stable
/// `api_call_id`; the response id and actual winning backend are unknown here.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RequestRow {
    pub id: String,
    pub response_id: Option<String>,
    pub conversation_id: Option<String>,
    pub virtual_key_id: Option<String>,
    pub client_protocol: String,
    pub client_model: String,
    pub alias: Option<String>,
    pub backend: Option<String>,
    pub resolved_model: Option<String>,
    pub status: String,
    pub created_at_ms: i64,
    /// Detected harness identity (see `crate::harness`), known at ingress.
    pub harness: Option<String>,
    pub harness_version: Option<String>,
    pub harness_session_id: Option<String>,
    pub harness_sub_session_id: Option<String>,
    pub harness_parent_session_id: Option<String>,
    pub session_kind: Option<String>,
    /// Session-tree linkage (see `crate::sessions`), known at ingress.
    pub session_id: Option<String>,
    pub chain_parent_request_id: Option<String>,
    pub item_count: Option<i64>,
    pub shared_prefix_items: Option<i64>,
    pub divergence_kind: Option<String>,
    pub divergence_index: Option<i64>,
    pub cache_bust: Option<bool>,
    /// Client attribution, also known at ingress (the terminal upsert keeps it).
    pub client_label: Option<String>,
    pub client_source: Option<String>,
    /// Owner of the authenticating virtual key, when known.
    pub user_id: Option<String>,
}

/// One bounded/redacted hop event. Callers must use the existing turn-capture
/// redactor before populating `payload`; this module never accepts raw headers.
/// SQL persistence treats `(request_id, seq)` as last-writer-wins. JSONL is
/// append-only, so readers of that format must likewise keep the last duplicate.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct EventRow {
    pub request_id: String,
    /// Event order within the request; the four body skeletons use 1..=4 (see `PayloadSection::seq`).
    pub seq: i64,
    /// Epoch milliseconds.
    pub ts_ms: i64,
    /// Hop label, e.g. `client_in`, `upstream_out`, `upstream_in`, `client_out`.
    pub hop: String,
    /// Event kind, e.g. `request`, `response`, `attempt`, `terminal_attempt`.
    pub kind: String,
    /// Bounded, secret-redacted JSON envelope text, when stored.
    pub payload: Option<String>,
    pub bytes: Option<i64>,
}

/// One content-addressed item body. `hash` is the lowercase hex SHA-256 of
/// `content`, which is canonical, secret-redacted JSON text.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BlobRow {
    pub hash: String,
    pub media: String,
    pub size: i64,
    pub content: String,
    pub created_at_ms: i64,
}

/// One position of a request body on one hop, referencing a blob by hash.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ItemRow {
    pub request_id: String,
    pub hop: String,
    pub ordinal: i64,
    pub section: String,
    pub kind: Option<String>,
    pub blob_hash: String,
    /// Lineage identity of the item (see `SplitItem::identity`); `None` for
    /// rows written before the column existed, which then fall back to the
    /// storage hash.
    pub identity_hash: Option<String>,
}

/// A request-side hop body: the skeleton event plus its items and the blobs
/// they reference. Written as one unit so a body is never half-stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyWrite {
    pub event: EventRow,
    pub items: Vec<ItemRow>,
    pub blobs: Vec<BlobRow>,
}

/// Authoritative terminal aggregate. `backend` and `resolved_model` are the
/// winner from the final served attempt, not the configured primary. They stay
/// `None` for pre-dispatch/all-failed requests.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RequestFinish {
    pub response_id: Option<String>,
    pub status: String,
    pub completed_at_ms: i64,
    /// Epoch-ms of the first content delta, not merely the first upstream byte.
    pub first_token_at_ms: Option<i64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub error: Option<String>,
    pub terminal_reason: Option<String>,
    pub backend: Option<String>,
    pub resolved_model: Option<String>,
    pub attempts_json: Option<String>,
    pub timings_json: Option<String>,
    pub client_label: Option<String>,
    pub client_source: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageFilter {
    pub virtual_key_id: Option<String>,
    pub user_id: Option<String>,
    pub since_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct UsageBucket {
    pub user_id: Option<String>,
    pub virtual_key_id: Option<String>,
    pub alias: Option<String>,
    pub resolved_model: Option<String>,
    pub backend: Option<String>,
    pub requests: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub reasoning_tokens: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct RequestSummary {
    pub id: String,
    pub response_id: Option<String>,
    pub conversation_id: Option<String>,
    pub virtual_key_id: Option<String>,
    pub client_protocol: String,
    pub client_model: String,
    pub alias: Option<String>,
    pub backend: Option<String>,
    pub resolved_model: Option<String>,
    /// `running`, `completed` or `failed`.
    pub status: String,
    /// Epoch milliseconds.
    pub created_at_ms: i64,
    /// Epoch milliseconds; `None` while still running.
    pub completed_at_ms: Option<i64>,
    /// Epoch milliseconds of the first content delta, when one was observed.
    pub first_token_at_ms: Option<i64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub error: Option<String>,
    pub terminal_reason: Option<String>,
    pub attempts_json: Option<String>,
    pub timings_json: Option<String>,
    pub client_label: Option<String>,
    pub client_source: Option<String>,
    pub harness: Option<String>,
    pub harness_version: Option<String>,
    pub harness_session_id: Option<String>,
    pub harness_sub_session_id: Option<String>,
    pub harness_parent_session_id: Option<String>,
    pub session_kind: Option<String>,
    pub session_id: Option<String>,
    pub chain_parent_request_id: Option<String>,
    pub item_count: Option<i64>,
    pub shared_prefix_items: Option<i64>,
    pub divergence_kind: Option<String>,
    pub divergence_index: Option<i64>,
    pub cache_bust: Option<bool>,
    pub user_id: Option<String>,
}

/// One (bucket, user, key) cell of the activity series.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct ActivityBucket {
    pub bucket_ms: i64,
    pub user_id: Option<String>,
    pub virtual_key_id: Option<String>,
    pub requests: i64,
    pub failed: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
}

/// One (bucket, model, backend) cell of the gateway-side throughput series.
/// Sums cover only rows that reported the class; the `*_count` companions say
/// how many rows contributed so a consumer can derive means without lying.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct ThroughputBucket {
    pub bucket_ms: i64,
    pub model: String,
    pub backend: Option<String>,
    pub requests: i64,
    pub completed: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    /// Sum of (first token − request start) over rows with a first token.
    pub ttft_ms_sum: i64,
    pub ttft_count: i64,
    /// Input tokens of rows with a first token (the prefill throughput numerator).
    pub prefill_tokens: i64,
    /// Sum of (completed − first token) over rows with both and reported output.
    pub decode_ms_sum: i64,
    /// Output tokens of those rows (the decode throughput numerator).
    pub decode_tokens: i64,
    pub decode_count: i64,
}

/// Lifetime token totals over one session node's requests. Each class sums
/// only the rows that REPORTED it; `None` ⇒ no row reported the class.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct SessionAggregate {
    pub session_id: String,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct MetricSample {
    pub backend: String,
    /// Epoch milliseconds at which the sample was taken.
    pub ts_ms: i64,
    /// The sample as JSON text (backend health or scraped upstream metrics).
    pub data: String,
}

/// A dashboard user account (never carries the password hash).
#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
pub struct UserRecord {
    /// Opaque user id (UUID string); the `{id}` in `/dashboard/api/users/{id}`.
    pub id: String,
    pub username: String,
    /// Whether the user may manage users and other users' keys.
    pub is_admin: bool,
    /// Creation time, unix milliseconds.
    pub created_at_ms: i64,
    /// Last password/role change, unix milliseconds.
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAuth {
    pub id: String,
    pub username: String,
    pub password_hash: String,
    pub is_admin: bool,
}

/// API-key metadata. The secret (or its digest) is intentionally never exposed.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
pub struct ApiKeyRecord {
    /// Opaque key id (UUID string); the `{id}` in `/dashboard/api/keys/{id}`.
    pub id: String,
    /// Optional human label (trimmed, at most 128 characters).
    pub label: Option<String>,
    /// Owning user id; `null` for a key created by a token/dev-open session without `user_id`.
    pub user_id: Option<String>,
    /// Client-facing model/alias names the key may request; empty = any.
    /// Always present on the wire (`serde(default)` only covers reads).
    #[serde(default)]
    #[schema(required)]
    pub allowed_models: Vec<String>,
    /// Creation time, unix milliseconds.
    pub created_at_ms: i64,
    /// Last change, unix milliseconds.
    pub updated_at_ms: i64,
}

/// Authentication snapshot carrying only one-way digests. This is the safe
/// bridge to `ClientAuth::from_specs`; the database's raw `secret` column never
/// crosses the store boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKeyAuthSpec {
    pub id: String,
    pub label: Option<String>,
    pub user_id: Option<String>,
    pub secret_hash: String,
    pub allowed_models: Vec<String>,
}

/// Read-only recovery result for databases written by the pre-upstream
/// control plane. A nonblank `settings.operational` document is authoritative;
/// otherwise the legacy relational tables are projected into the current type.
///
/// The custom `Debug` intentionally exposes only provenance and cardinality:
/// both representations may contain upstream or client authentication material.
#[derive(Clone, PartialEq)]
pub enum LegacyOperationalRead {
    SettingsDocument(String),
    Relational(crate::control_plane::OperationalConfig),
}

impl fmt::Debug for LegacyOperationalRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SettingsDocument(document) => formatter
                .debug_struct("LegacyOperationalRead")
                .field("source", &"settings.operational")
                .field("document_bytes", &document.len())
                .finish(),
            Self::Relational(operational) => formatter
                .debug_struct("LegacyOperationalRead")
                .field("source", &"relational")
                .field("backends", &operational.backends.len())
                .field("model_profiles", &operational.model_profiles.len())
                .field("aliases", &operational.aliases.len())
                .field("keys", &operational.keys.len())
                .field("unknown_model_policy", &operational.unknown_model_policy)
                .finish(),
        }
    }
}

impl fmt::Debug for ApiKeyAuthSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiKeyAuthSpec")
            .field("id", &self.id)
            .field("label", &self.label)
            .field("user_id", &self.user_id)
            .field("secret_hash", &"[redacted]")
            .finish()
    }
}

#[async_trait]
pub trait PersistenceWriter: Send + Sync {
    async fn begin_request(&self, row: RequestRow) -> StoreResult<()>;
    async fn append_event(&self, event: EventRow) -> StoreResult<()>;
    async fn finish_request(&self, id: &str, finish: RequestFinish) -> StoreResult<()>;
    /// Store a split request body: blobs first, then item references, then the
    /// skeleton event. The default keeps only the skeleton event, for writers
    /// that have no item storage; the SQL and JSONL writers override it.
    async fn store_body(&self, body: BodyWrite) -> StoreResult<()> {
        self.append_event(body.event).await
    }
    /// Create or refresh a session node. Writers without session storage may
    /// keep the default no-op.
    async fn upsert_session(&self, _row: crate::sessions::SessionRow) -> StoreResult<()> {
        Ok(())
    }
}

#[async_trait]
pub trait PersistenceStore: PersistenceWriter {
    async fn get_request(&self, id: &str) -> StoreResult<Option<RequestSummary>>;
    async fn request_events(&self, request_id: &str) -> StoreResult<Vec<EventRow>>;
    /// Newest `limit` request events, returned in sequence order.
    async fn request_events_limited(
        &self,
        request_id: &str,
        limit: usize,
    ) -> StoreResult<Vec<EventRow>>;
    async fn list_requests(&self, limit: i64) -> StoreResult<Vec<RequestSummary>>;
    async fn usage_summary(&self, filter: &UsageFilter) -> StoreResult<Vec<UsageBucket>>;
    async fn usage_summary_limited(
        &self,
        filter: &UsageFilter,
        limit: usize,
    ) -> StoreResult<Vec<UsageBucket>>;
    /// Atomically remove requests older than `before_ms` and their event and
    /// item rows. A request exactly at the cutoff is retained.
    async fn prune_request_history(&self, before_ms: i64) -> StoreResult<()>;
    /// Items of one request hop in body order.
    async fn request_items(&self, request_id: &str, hop: &str) -> StoreResult<Vec<ItemRow>>;
    /// Blobs for the given hashes; missing hashes are simply absent.
    async fn get_blobs(&self, hashes: &[String]) -> StoreResult<Vec<BlobRow>>;
    /// Remove blobs no `request_items` row references. Returns the count.
    async fn prune_orphan_blobs(&self) -> StoreResult<u64>;

    async fn get_session(&self, id: &str) -> StoreResult<Option<crate::sessions::SessionRow>>;
    /// A declared node by `(harness, external_id)`, or the anonymous bucket
    /// for `(harness, client_label)` when `external_id` is `None`.
    async fn find_session(
        &self,
        harness: &str,
        external_id: Option<&str>,
        client_label: Option<&str>,
    ) -> StoreResult<Option<crate::sessions::SessionRow>>;
    /// Most recently active nodes at/after `since_ms`; `roots_only` limits to
    /// nodes without a parent.
    async fn list_sessions(
        &self,
        since_ms: i64,
        roots_only: bool,
        limit: usize,
    ) -> StoreResult<Vec<crate::sessions::SessionRow>>;
    /// Direct children of a node, oldest first.
    async fn session_children(
        &self,
        parent_id: &str,
    ) -> StoreResult<Vec<crate::sessions::SessionRow>>;
    /// All descendants of a node, breadth-first, at most `limit`.
    async fn session_descendants(
        &self,
        id: &str,
        limit: usize,
    ) -> StoreResult<Vec<crate::sessions::SessionRow>>;
    /// Newest `limit` requests of one node, oldest first.
    async fn session_requests(
        &self,
        session_id: &str,
        limit: usize,
    ) -> StoreResult<Vec<RequestSummary>>;
    /// Lifetime token totals of one session node's requests: the sums over the
    /// rows that REPORTED each class (`Option` ⇒ no row reported it — a
    /// distinct "unavailable", never a fabricated zero).
    async fn session_aggregate(&self, session_id: &str) -> StoreResult<Option<SessionAggregate>>;
    /// The latest request of a node's chain with its inbound items.
    async fn chain_head(&self, session_id: &str)
    -> StoreResult<Option<crate::sessions::ChainHead>>;
    /// Gateway-side per-model throughput: requests bucketed by `bucket_ms`
    /// since `since_ms`, grouped by served model and backend. Oldest first.
    async fn throughput_series(
        &self,
        since_ms: i64,
        bucket_ms: i64,
        limit: usize,
    ) -> StoreResult<Vec<ThroughputBucket>>;

    async fn get_setting(&self, key: &str) -> StoreResult<Option<String>>;
    async fn set_setting(&self, key: &str, value: &str) -> StoreResult<()>;
    async fn delete_setting(&self, key: &str) -> StoreResult<()>;

    async fn record_backend_metrics(&self, sample: MetricSample) -> StoreResult<()>;
    async fn backend_metrics_history(&self, since_ms: i64) -> StoreResult<Vec<MetricSample>>;
    /// Newest `limit` samples at/after `since_ms`, returned oldest-first. The
    /// database applies the limit before rows enter process memory.
    async fn backend_metrics_history_limited(
        &self,
        since_ms: i64,
        limit: usize,
    ) -> StoreResult<Vec<MetricSample>>;
    async fn prune_backend_metrics(&self, before_ms: i64) -> StoreResult<()>;

    async fn list_users(&self) -> StoreResult<Vec<UserRecord>>;
    async fn count_users(&self) -> StoreResult<i64>;
    async fn get_user_auth(&self, username: &str) -> StoreResult<Option<UserAuth>>;
    async fn create_user(
        &self,
        username: &str,
        password_hash: &str,
        is_admin: bool,
        actor: &str,
    ) -> StoreResult<UserRecord>;
    /// Create or refresh an externally-authenticated dashboard user with a
    /// caller-stable id such as `github:<numeric-id>`. These rows deliberately
    /// carry a non-PHC password sentinel, so password login cannot authenticate
    /// them through `verify_password`.
    async fn upsert_external_user(
        &self,
        id: &str,
        username: &str,
        is_admin: bool,
        actor: &str,
    ) -> StoreResult<UserRecord>;
    async fn update_user(
        &self,
        id: &str,
        password_hash: Option<&str>,
        is_admin: Option<bool>,
        actor: &str,
    ) -> StoreResult<()>;
    async fn delete_user(&self, id: &str, actor: &str) -> StoreResult<()>;

    async fn list_api_keys(&self) -> StoreResult<Vec<ApiKeyRecord>>;
    async fn api_key_auth_specs(&self) -> StoreResult<Vec<ApiKeyAuthSpec>>;
    async fn put_api_key(
        &self,
        id: &str,
        plaintext: &str,
        label: Option<&str>,
        user_id: Option<&str>,
        allowed_models: &[String],
        actor: &str,
    ) -> StoreResult<ApiKeyRecord>;
    async fn verify_api_key(&self, plaintext: &str) -> StoreResult<Option<ApiKeyRecord>>;
    async fn delete_api_key(&self, id: &str, actor: &str) -> StoreResult<()>;
    async fn get_user(&self, id: &str) -> StoreResult<Option<UserRecord>>;
    async fn list_api_keys_for_user(&self, user_id: &str) -> StoreResult<Vec<ApiKeyRecord>>;
    /// Per-user/key activity: requests bucketed by `bucket_ms` since `since_ms`.
    async fn activity_series(
        &self,
        since_ms: i64,
        bucket_ms: i64,
        limit: usize,
    ) -> StoreResult<Vec<ActivityBucket>>;
}

/// Append-only, log-only persistence for installations that do not need the
/// queryable history API. File IO is always moved to `spawn_blocking`; the
/// inference path still talks only to [`PersistenceQueue`].
#[derive(Clone)]
pub struct JsonlWriter {
    dir: PathBuf,
    lock: Arc<Mutex<()>>,
    retention_days: Option<NonZeroU64>,
    last_prune_day: Arc<AtomicI64>,
}

impl JsonlWriter {
    /// Create an append-only writer. On Unix the target directory is tightened
    /// to `0700`; each file is tightened to `0600` whenever it is opened.
    pub fn new(dir: impl AsRef<Path>) -> StoreResult<Self> {
        Self::new_inner(dir, None)
    }

    /// Create a daily-rotated writer that removes only its own expired JSONL
    /// files. Unrelated files in the directory are never considered.
    pub fn new_with_retention(
        dir: impl AsRef<Path>,
        retention_days: NonZeroU64,
    ) -> StoreResult<Self> {
        Self::new_inner(dir, Some(retention_days))
    }

    fn new_inner(dir: impl AsRef<Path>, retention_days: Option<NonZeroU64>) -> StoreResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)
            .map_err(|error| format!("failed to create {}: {error}", dir.display()))?;
        secure_jsonl_directory(&dir)?;
        Ok(Self {
            dir,
            lock: Arc::new(Mutex::new(())),
            retention_days,
            last_prune_day: Arc::new(AtomicI64::new(i64::MIN)),
        })
    }

    async fn append(&self, stem: &'static str, value: serde_json::Value) -> StoreResult<()> {
        let dir = self.dir.clone();
        let lock = Arc::clone(&self.lock);
        let retention_days = self.retention_days;
        let last_prune_day = Arc::clone(&self.last_prune_day);
        tokio::task::spawn_blocking(move || {
            let _guard = lock
                .lock()
                .map_err(|error| format!("jsonl lock poisoned: {error}"))?;
            if let Some(retention_days) = retention_days
                && let Err(error) = maybe_prune_jsonl(&dir, retention_days, &last_prune_day)
            {
                tracing::warn!(
                    directory = %dir.display(),
                    %error,
                    "failed to prune expired JSONL persistence files; continuing append"
                );
            }
            let filename = retention_days.map_or_else(
                || format!("{stem}.jsonl"),
                |_| format!("{stem}-{}.jsonl", chrono::Utc::now().format("%Y-%m-%d")),
            );
            let path = dir.join(filename);
            let mut line = serde_json::to_vec(&value)
                .map_err(|error| format!("failed to serialize JSONL row: {error}"))?;
            line.push(b'\n');
            open_private_append_file(&path)
                .and_then(|mut file| file.write_all(&line))
                .map_err(|error| format!("failed to append {}: {error}", path.display()))
        })
        .await
        .map_err(|error| format!("JSONL writer task failed: {error}"))?
    }
}

fn maybe_prune_jsonl(
    dir: &Path,
    retention_days: NonZeroU64,
    last_prune_day: &AtomicI64,
) -> StoreResult<()> {
    let now = SystemTime::now();
    let epoch_day = chrono::Utc::now().timestamp().div_euclid(86_400);
    // Pruning is maintenance, not part of the durability contract for the row
    // being appended. Attempt it at most once per process/day so a transient
    // filesystem error cannot turn every persistence write into another failed
    // directory scan.
    if last_prune_day.swap(epoch_day, Ordering::Relaxed) == epoch_day {
        return Ok(());
    }
    let max_age = Duration::from_secs(retention_days.get().saturating_mul(86_400));
    let cutoff = now.checked_sub(max_age).unwrap_or(SystemTime::UNIX_EPOCH);
    let entries = std::fs::read_dir(dir)
        .map_err(|error| format!("failed to inspect {}: {error}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("failed to inspect JSONL entry: {error}"))?;
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();
        let owned = filename == "requests.jsonl"
            || filename == "events.jsonl"
            || (filename.starts_with("requests-") && filename.ends_with(".jsonl"))
            || (filename.starts_with("events-") && filename.ends_with(".jsonl"));
        if !owned {
            continue;
        }
        let metadata = entry
            .metadata()
            .map_err(|error| format!("failed to inspect {}: {error}", entry.path().display()))?;
        if metadata.is_file() && metadata.modified().is_ok_and(|modified| modified < cutoff) {
            std::fs::remove_file(entry.path())
                .map_err(|error| format!("failed to prune {}: {error}", entry.path().display()))?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn secure_jsonl_directory(path: &Path) -> StoreResult<()> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
    if !metadata.is_dir() {
        return Err(format!("JSONL path is not a directory: {}", path.display()));
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .map_err(|error| format!("failed to secure {}: {error}", path.display()))
}

#[cfg(not(unix))]
fn secure_jsonl_directory(_path: &Path) -> StoreResult<()> {
    Ok(())
}

#[cfg(unix)]
fn open_private_append_file(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?;
    // `mode` applies only at creation; explicitly tighten a pre-existing file.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

#[cfg(not(unix))]
fn open_private_append_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

#[async_trait]
impl PersistenceWriter for JsonlWriter {
    async fn begin_request(&self, row: RequestRow) -> StoreResult<()> {
        let mut value = serde_json::to_value(row).map_err(store_error)?;
        if let Some(object) = value.as_object_mut() {
            object.insert("event".to_string(), serde_json::json!("begin"));
        }
        self.append("requests", value).await
    }

    async fn append_event(&self, event: EventRow) -> StoreResult<()> {
        let value = serde_json::to_value(event).map_err(store_error)?;
        self.append("events", value).await
    }

    async fn finish_request(&self, id: &str, finish: RequestFinish) -> StoreResult<()> {
        let mut value = serde_json::to_value(finish).map_err(store_error)?;
        if let Some(object) = value.as_object_mut() {
            object.insert("event".to_string(), serde_json::json!("finish"));
            object.insert("id".to_string(), serde_json::json!(id));
        }
        self.append("requests", value).await
    }

    /// JSONL has no cross-record addressing, so the body record carries every
    /// item inline (no deduplication) alongside the skeleton event.
    async fn store_body(&self, body: BodyWrite) -> StoreResult<()> {
        let blobs: std::collections::HashMap<&str, &str> = body
            .blobs
            .iter()
            .map(|blob| (blob.hash.as_str(), blob.content.as_str()))
            .collect();
        let items = body
            .items
            .iter()
            .map(|item| {
                serde_json::json!({
                    "ordinal": item.ordinal,
                    "section": item.section,
                    "kind": item.kind,
                    "hash": item.blob_hash,
                    "content": blobs.get(item.blob_hash.as_str()),
                })
            })
            .collect::<Vec<_>>();
        let mut value = serde_json::to_value(&body.event).map_err(store_error)?;
        if let Some(object) = value.as_object_mut() {
            object.insert("items".to_string(), serde_json::Value::Array(items));
        }
        self.append("events", value).await
    }

    async fn upsert_session(&self, row: crate::sessions::SessionRow) -> StoreResult<()> {
        let mut value = serde_json::to_value(row).map_err(store_error)?;
        if let Some(object) = value.as_object_mut() {
            object.insert("event".to_string(), serde_json::json!("session"));
        }
        self.append("requests", value).await
    }
}

#[derive(Clone)]
enum SqlPool {
    Sqlite(sqlx::SqlitePool),
    Postgres(sqlx::PgPool),
}

#[derive(Clone)]
pub struct SqlStore {
    pool: SqlPool,
}

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

macro_rules! execute {
    ($self:expr, $sql:expr $(, $bind:expr )* $(,)?) => {{
        match &$self.pool {
            SqlPool::Sqlite(pool) => {
                #[allow(unused_mut)]
                let mut query = sqlx::query($sql);
                $(query = query.bind($bind);)*
                query.execute(pool).await.map_err(store_error)?;
            }
            SqlPool::Postgres(pool) => {
                #[allow(unused_mut)]
                let mut query = sqlx::query($sql);
                $(query = query.bind($bind);)*
                query.execute(pool).await.map_err(store_error)?;
            }
        }
    }};
}

impl SqlStore {
    /// Connect and migrate SQLite storage. On Unix a file-backed database is
    /// tightened to `0600`, including when it already exists. Its parent is not
    /// chmodded because it may be shared or operator-owned; deployments must
    /// place the database in a private directory when pathname privacy matters.
    pub async fn connect_sqlite(url: &str) -> StoreResult<Self> {
        let in_memory = sqlite_url_is_memory(url);
        let mut options = sqlx::sqlite::SqliteConnectOptions::from_str(url)
            // The connection string can carry URI parameters; never repeat it.
            .map_err(|_| "invalid SQLite connection URL".to_string())?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(if in_memory {
                sqlx::sqlite::SqliteJournalMode::Memory
            } else {
                sqlx::sqlite::SqliteJournalMode::Wal
            })
            .busy_timeout(Duration::from_secs(5))
            // Bound the per-connection page cache (negative = KiB) so the pool's
            // worst case stays a few tens of MiB on a small host.
            .pragma("cache_size", SQLITE_CACHE_SIZE_PRAGMA);
        if !in_memory {
            // WAL + NORMAL is crash-safe (no corruption); a power loss can only
            // drop the last few committed transactions, which is acceptable for
            // observability/control-plane rows and saves an fsync per commit.
            options = options.synchronous(sqlx::sqlite::SqliteSynchronous::Normal);
        }
        let database_path = (!in_memory).then(|| options.get_filename().to_owned());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            // SQLite serializes writers anyway; a small pool bounds the per-
            // connection caches and file handles without costing throughput.
            .max_connections(SQLITE_MAX_CONNECTIONS)
            .connect_with(options)
            .await
            .map_err(|_| "SQLite storage connection failed".to_string())?;
        if let Some(path) = database_path {
            // The database itself is always owner-only. We deliberately do not
            // chmod its parent: it may be a shared directory selected by the
            // operator, who remains responsible for making that directory private.
            secure_sqlite_database_file(&path)?;
        }
        let store = Self {
            pool: SqlPool::Sqlite(pool),
        };
        store.migrate().await?;
        Ok(store)
    }

    /// Connect and migrate PostgreSQL storage. Remote hosts must select a TLS
    /// mode that cannot silently fall back to plaintext; local developer
    /// endpoints retain libpq/SQLx's default-mode flexibility.
    pub async fn connect_postgres(url: &str) -> StoreResult<Self> {
        let options = PgConnectOptions::from_str(url)
            // Never repeat a potentially credential-bearing URL in an error.
            .map_err(|_| "invalid PostgreSQL connection URL".to_string())?;
        validate_postgres_transport(&options)?;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_with(options)
            .await
            .map_err(|_| "PostgreSQL storage connection failed".to_string())?;
        let store = Self {
            pool: SqlPool::Postgres(pool),
        };
        store.migrate().await?;
        Ok(store)
    }

    pub async fn close(&self) {
        match &self.pool {
            SqlPool::Sqlite(pool) => pool.close().await,
            SqlPool::Postgres(pool) => pool.close().await,
        }
    }

    fn postgres(&self) -> bool {
        matches!(self.pool, SqlPool::Postgres(_))
    }

    async fn migrate(&self) -> StoreResult<()> {
        match &self.pool {
            SqlPool::Sqlite(pool) => MIGRATOR.run(pool).await.map_err(store_error),
            SqlPool::Postgres(pool) => MIGRATOR.run(pool).await.map_err(store_error),
        }
    }
}

fn sqlite_url_is_memory(url: &str) -> bool {
    let stripped = url
        .trim_start_matches("sqlite://")
        .trim_start_matches("sqlite:");
    let (database, query) = stripped
        .split_once('?')
        .map_or((stripped, None), |(database, query)| {
            (database, Some(query))
        });
    database == ":memory:"
        || query.is_some_and(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .any(|(key, value)| key == "mode" && value == "memory")
        })
}

#[cfg(unix)]
fn secure_sqlite_database_file(path: &Path) -> StoreResult<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|error| {
        format!(
            "failed to secure SQLite database {}: {error}",
            path.display()
        )
    })
}

#[cfg(not(unix))]
fn secure_sqlite_database_file(_path: &Path) -> StoreResult<()> {
    Ok(())
}

fn validate_postgres_transport(options: &PgConnectOptions) -> StoreResult<()> {
    let host = options.get_host();
    let local = options.get_socket().is_some()
        || host.starts_with('/')
        || host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    let encrypted_only = matches!(
        options.get_ssl_mode(),
        PgSslMode::Require | PgSslMode::VerifyCa | PgSslMode::VerifyFull
    );
    if !local && !encrypted_only {
        return Err(
            "remote PostgreSQL storage requires sslmode=require, verify-ca, or verify-full (prefer verify-full)"
                .to_string(),
        );
    }
    Ok(())
}

fn store_error(error: impl fmt::Display) -> String {
    error.to_string()
}

fn placeholder(postgres: bool, index: usize) -> String {
    if postgres {
        format!("${index}")
    } else {
        "?".to_string()
    }
}

fn placeholders(postgres: bool, count: usize) -> String {
    (1..=count)
        .map(|index| placeholder(postgres, index))
        .collect::<Vec<_>>()
        .join(", ")
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn api_key_digest(plaintext: &str) -> String {
    let digest = Sha256::digest(plaintext.as_bytes());
    format!("{API_KEY_HASH_PREFIX}{}", hex::encode(digest))
}

fn is_canonical_api_key_digest(value: &str) -> bool {
    let Some(encoded) = value.strip_prefix(API_KEY_HASH_PREFIX) else {
        return false;
    };
    encoded.len() == 64 && hex::decode(encoded).is_ok()
}

fn canonical_api_key_digest(stored: &str) -> String {
    if is_canonical_api_key_digest(stored) {
        stored.to_ascii_lowercase()
    } else {
        api_key_digest(stored)
    }
}

fn digest_matches(stored: &str, candidate: &str) -> bool {
    let stored_bytes: [u8; 32] = if is_canonical_api_key_digest(stored) {
        let stored_hex = stored
            .strip_prefix(API_KEY_HASH_PREFIX)
            .expect("canonical digest has prefix");
        hex::decode(stored_hex)
            .expect("canonical digest is hexadecimal")
            .try_into()
            .expect("canonical digest has 32 bytes")
    } else {
        // Pre-integration binaries need plaintext rows to remain on disk for
        // rollback compatibility. Hash in memory without rewriting the row.
        Sha256::digest(stored.as_bytes()).into()
    };
    let candidate_bytes: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
    stored_bytes.ct_eq(&candidate_bytes).into()
}

const REQUEST_COLUMNS: &str = "SELECT id, response_id, conversation_id, virtual_key_id, \
    client_protocol, client_model, alias, backend, resolved_model, status, created_at_ms, \
    completed_at_ms, first_token_at_ms, input_tokens, output_tokens, cached_tokens, \
    reasoning_tokens, error, terminal_reason, attempts_json, timings_json, client_label, \
    client_source, harness, harness_version, harness_session_id, harness_sub_session_id, \
    harness_parent_session_id, session_kind, session_id, chain_parent_request_id, item_count, \
    shared_prefix_items, divergence_kind, divergence_index, cache_bust, user_id FROM requests";

fn decode_request<R>(row: &R) -> StoreResult<RequestSummary>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<i64>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(RequestSummary {
        id: row.try_get(0).map_err(store_error)?,
        response_id: row.try_get(1).map_err(store_error)?,
        conversation_id: row.try_get(2).map_err(store_error)?,
        virtual_key_id: row.try_get(3).map_err(store_error)?,
        client_protocol: row.try_get(4).map_err(store_error)?,
        client_model: row.try_get(5).map_err(store_error)?,
        alias: row.try_get(6).map_err(store_error)?,
        backend: row.try_get(7).map_err(store_error)?,
        resolved_model: row.try_get(8).map_err(store_error)?,
        status: row.try_get(9).map_err(store_error)?,
        created_at_ms: row.try_get(10).map_err(store_error)?,
        completed_at_ms: row.try_get(11).map_err(store_error)?,
        first_token_at_ms: row.try_get(12).map_err(store_error)?,
        input_tokens: row.try_get(13).map_err(store_error)?,
        output_tokens: row.try_get(14).map_err(store_error)?,
        cached_tokens: row.try_get(15).map_err(store_error)?,
        reasoning_tokens: row.try_get(16).map_err(store_error)?,
        error: row.try_get(17).map_err(store_error)?,
        terminal_reason: row.try_get(18).map_err(store_error)?,
        attempts_json: row.try_get(19).map_err(store_error)?,
        timings_json: row.try_get(20).map_err(store_error)?,
        client_label: row.try_get(21).map_err(store_error)?,
        client_source: row.try_get(22).map_err(store_error)?,
        harness: row.try_get(23).map_err(store_error)?,
        harness_version: row.try_get(24).map_err(store_error)?,
        harness_session_id: row.try_get(25).map_err(store_error)?,
        harness_sub_session_id: row.try_get(26).map_err(store_error)?,
        harness_parent_session_id: row.try_get(27).map_err(store_error)?,
        session_kind: row.try_get(28).map_err(store_error)?,
        session_id: row.try_get(29).map_err(store_error)?,
        chain_parent_request_id: row.try_get(30).map_err(store_error)?,
        item_count: row.try_get(31).map_err(store_error)?,
        shared_prefix_items: row.try_get(32).map_err(store_error)?,
        divergence_kind: row.try_get(33).map_err(store_error)?,
        divergence_index: row.try_get(34).map_err(store_error)?,
        cache_bust: row
            .try_get::<Option<i64>, _>(35)
            .map_err(store_error)?
            .map(|flag| flag != 0),
        user_id: row.try_get(36).map_err(store_error)?,
    })
}

const SESSION_COLUMNS: &str = "SELECT id, parent_id, kind, harness, harness_version, external_id, \
    session_kind, client_label, virtual_key_id, user_id, depth, root_request_id, \
    spawned_by_request_id, first_seen_ms, last_seen_ms, request_count FROM sessions";

fn decode_session<R>(row: &R) -> StoreResult<crate::sessions::SessionRow>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(crate::sessions::SessionRow {
        id: row.try_get(0).map_err(store_error)?,
        parent_id: row.try_get(1).map_err(store_error)?,
        kind: row.try_get(2).map_err(store_error)?,
        harness: row.try_get(3).map_err(store_error)?,
        harness_version: row.try_get(4).map_err(store_error)?,
        external_id: row.try_get(5).map_err(store_error)?,
        session_kind: row.try_get(6).map_err(store_error)?,
        client_label: row.try_get(7).map_err(store_error)?,
        virtual_key_id: row.try_get(8).map_err(store_error)?,
        user_id: row.try_get(9).map_err(store_error)?,
        depth: row.try_get(10).map_err(store_error)?,
        root_request_id: row.try_get(11).map_err(store_error)?,
        spawned_by_request_id: row.try_get(12).map_err(store_error)?,
        first_seen_ms: row.try_get(13).map_err(store_error)?,
        last_seen_ms: row.try_get(14).map_err(store_error)?,
        request_count: row.try_get(15).map_err(store_error)?,
    })
}

fn decode_event<R>(row: &R) -> StoreResult<EventRow>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<i64>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(EventRow {
        request_id: row.try_get(0).map_err(store_error)?,
        seq: row.try_get(1).map_err(store_error)?,
        ts_ms: row.try_get(2).map_err(store_error)?,
        hop: row.try_get(3).map_err(store_error)?,
        kind: row.try_get(4).map_err(store_error)?,
        payload: row.try_get(5).map_err(store_error)?,
        bytes: row.try_get(6).map_err(store_error)?,
    })
}

fn decode_item<R>(row: &R) -> StoreResult<ItemRow>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(ItemRow {
        request_id: row.try_get(0).map_err(store_error)?,
        hop: row.try_get(1).map_err(store_error)?,
        ordinal: row.try_get(2).map_err(store_error)?,
        section: row.try_get(3).map_err(store_error)?,
        kind: row.try_get(4).map_err(store_error)?,
        blob_hash: row.try_get(5).map_err(store_error)?,
        identity_hash: row.try_get(6).map_err(store_error)?,
    })
}

fn decode_blob<R>(row: &R) -> StoreResult<BlobRow>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(BlobRow {
        hash: row.try_get(0).map_err(store_error)?,
        media: row.try_get(1).map_err(store_error)?,
        size: row.try_get(2).map_err(store_error)?,
        content: row.try_get(3).map_err(store_error)?,
        created_at_ms: row.try_get(4).map_err(store_error)?,
    })
}

fn decode_throughput<R>(row: &R) -> StoreResult<ThroughputBucket>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(ThroughputBucket {
        bucket_ms: row.try_get(0).map_err(store_error)?,
        model: row.try_get(1).map_err(store_error)?,
        backend: row.try_get(2).map_err(store_error)?,
        requests: row.try_get(3).map_err(store_error)?,
        completed: row.try_get(4).map_err(store_error)?,
        input_tokens: row.try_get(5).map_err(store_error)?,
        output_tokens: row.try_get(6).map_err(store_error)?,
        cached_tokens: row.try_get(7).map_err(store_error)?,
        ttft_ms_sum: row.try_get(8).map_err(store_error)?,
        ttft_count: row.try_get(9).map_err(store_error)?,
        prefill_tokens: row.try_get(10).map_err(store_error)?,
        decode_ms_sum: row.try_get(11).map_err(store_error)?,
        decode_tokens: row.try_get(12).map_err(store_error)?,
        decode_count: row.try_get(13).map_err(store_error)?,
    })
}

fn decode_activity<R>(row: &R) -> StoreResult<ActivityBucket>
where
    R: Row,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(ActivityBucket {
        bucket_ms: row.try_get(0).map_err(store_error)?,
        user_id: row.try_get(1).map_err(store_error)?,
        virtual_key_id: row.try_get(2).map_err(store_error)?,
        requests: row.try_get(3).map_err(store_error)?,
        failed: row.try_get(4).map_err(store_error)?,
        input_tokens: row.try_get(5).map_err(store_error)?,
        output_tokens: row.try_get(6).map_err(store_error)?,
        cached_tokens: row.try_get(7).map_err(store_error)?,
    })
}

fn decode_metric<R>(row: &R) -> StoreResult<MetricSample>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(MetricSample {
        backend: row.try_get(0).map_err(store_error)?,
        ts_ms: row.try_get(1).map_err(store_error)?,
        data: row.try_get(2).map_err(store_error)?,
    })
}

fn decode_user<R>(row: &R) -> StoreResult<UserRecord>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(UserRecord {
        id: row.try_get(0).map_err(store_error)?,
        username: row.try_get(1).map_err(store_error)?,
        is_admin: row.try_get::<i64, _>(2).map_err(store_error)? != 0,
        created_at_ms: row.try_get(3).map_err(store_error)?,
        updated_at_ms: row.try_get(4).map_err(store_error)?,
    })
}

fn decode_key<R>(row: &R) -> StoreResult<ApiKeyRecord>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(ApiKeyRecord {
        id: row.try_get(0).map_err(store_error)?,
        label: row.try_get(1).map_err(store_error)?,
        user_id: row.try_get(2).map_err(store_error)?,
        created_at_ms: row.try_get(3).map_err(store_error)?,
        updated_at_ms: row.try_get(4).map_err(store_error)?,
        allowed_models: parse_allowed_models(
            row.try_get::<Option<String>, _>(5).map_err(store_error)?,
        ),
    })
}

/// `allowed_models_json` is a JSON array of names; anything else reads as "any".
fn parse_allowed_models(raw: Option<String>) -> Vec<String> {
    raw.and_then(|text| serde_json::from_str::<Vec<String>>(&text).ok())
        .unwrap_or_default()
}

fn encode_allowed_models(models: &[String]) -> Option<String> {
    if models.is_empty() {
        None
    } else {
        serde_json::to_string(models).ok()
    }
}

fn decode_key_auth<R>(row: &R) -> StoreResult<ApiKeyAuthSpec>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    let stored: String = row.try_get(3).map_err(store_error)?;
    Ok(ApiKeyAuthSpec {
        id: row.try_get(0).map_err(store_error)?,
        label: row.try_get(1).map_err(store_error)?,
        user_id: row.try_get(2).map_err(store_error)?,
        allowed_models: parse_allowed_models(
            row.try_get::<Option<String>, _>(4).map_err(store_error)?,
        ),
        // Legacy plaintext remains untouched on disk, but never crosses this
        // authentication boundary in plaintext.
        secret_hash: canonical_api_key_digest(&stored),
    })
}

struct LegacyBackendRow {
    id: String,
    name: String,
    base_url: String,
    api_key: Option<String>,
    request_log_path: Option<String>,
    enabled: i64,
    metrics_json: Option<String>,
    deleted_at_ms: Option<i64>,
}

struct LegacyProfileRow {
    id: String,
    name: String,
    upstream_model: Option<String>,
    system_prompt_prefix: Option<String>,
    upstream_chat_kwargs_json: Option<String>,
    deleted_at_ms: Option<i64>,
}

struct LegacyNamedRow {
    id: String,
    name: String,
    deleted_at_ms: Option<i64>,
}

struct LegacyKeyRow {
    id: String,
    secret: String,
    label: Option<String>,
    user_id: Option<String>,
    deleted_at_ms: Option<i64>,
}

fn decode_legacy_backend<R>(row: &R) -> StoreResult<LegacyBackendRow>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<i64>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(LegacyBackendRow {
        id: row.try_get(0).map_err(store_error)?,
        name: row.try_get(1).map_err(store_error)?,
        base_url: row.try_get(2).map_err(store_error)?,
        api_key: row.try_get(3).map_err(store_error)?,
        request_log_path: row.try_get(4).map_err(store_error)?,
        enabled: row.try_get(5).map_err(store_error)?,
        metrics_json: row.try_get(6).map_err(store_error)?,
        deleted_at_ms: row.try_get(7).map_err(store_error)?,
    })
}

fn decode_legacy_profile<R>(row: &R) -> StoreResult<LegacyProfileRow>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<i64>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(LegacyProfileRow {
        id: row.try_get(0).map_err(store_error)?,
        name: row.try_get(1).map_err(store_error)?,
        upstream_model: row.try_get(2).map_err(store_error)?,
        system_prompt_prefix: row.try_get(3).map_err(store_error)?,
        upstream_chat_kwargs_json: row.try_get(4).map_err(store_error)?,
        deleted_at_ms: row.try_get(5).map_err(store_error)?,
    })
}

fn decode_legacy_named<R>(row: &R) -> StoreResult<LegacyNamedRow>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<i64>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(LegacyNamedRow {
        id: row.try_get(0).map_err(store_error)?,
        name: row.try_get(1).map_err(store_error)?,
        deleted_at_ms: row.try_get(2).map_err(store_error)?,
    })
}

fn decode_legacy_key<R>(row: &R) -> StoreResult<LegacyKeyRow>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<i64>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok(LegacyKeyRow {
        id: row.try_get(0).map_err(store_error)?,
        secret: row.try_get(1).map_err(store_error)?,
        label: row.try_get(2).map_err(store_error)?,
        user_id: row.try_get(3).map_err(store_error)?,
        deleted_at_ms: row.try_get(4).map_err(store_error)?,
    })
}

fn decode_legacy_edge<R>(row: &R) -> StoreResult<(String, String)>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    Ok((
        row.try_get(0).map_err(store_error)?,
        row.try_get(1).map_err(store_error)?,
    ))
}

macro_rules! fetch_all_decoded {
    ($self:expr, $sql:expr, [$($bind:expr),* $(,)?], $decoder:path) => {{
        match &$self.pool {
            SqlPool::Sqlite(pool) => {
                #[allow(unused_mut)]
                let mut query = sqlx::query($sql);
                $(query = query.bind($bind);)*
                query.fetch_all(pool).await.map_err(store_error)?
                    .iter().map($decoder).collect::<StoreResult<Vec<_>>>()?
            }
            SqlPool::Postgres(pool) => {
                #[allow(unused_mut)]
                let mut query = sqlx::query($sql);
                $(query = query.bind($bind);)*
                query.fetch_all(pool).await.map_err(store_error)?
                    .iter().map($decoder).collect::<StoreResult<Vec<_>>>()?
            }
        }
    }};
}

impl SqlStore {
    /// Recover a legacy SQL operational configuration without mutating it.
    ///
    /// A nonblank name-map/JSON document in `settings.operational` wins and is
    /// returned byte-for-byte so the current parser can migrate or reject it at
    /// startup. Otherwise the relational representation is assembled using
    /// only live entities and live edge targets. `None` means genuinely
    /// unseeded; tombstones still count as seeded and produce `Some(empty)`.
    pub async fn load_legacy_operational(&self) -> StoreResult<Option<LegacyOperationalRead>> {
        if let Some(document) = self.get_setting("operational").await?
            && !document.trim().is_empty()
        {
            return Ok(Some(LegacyOperationalRead::SettingsDocument(document)));
        }

        let backends: Vec<LegacyBackendRow> = fetch_all_decoded!(
            self,
            "SELECT id, name, base_url, api_key, request_log_path, enabled, metrics_json, \
             deleted_at_ms FROM backends ORDER BY created_at_ms, id",
            [],
            decode_legacy_backend
        );
        let profiles: Vec<LegacyProfileRow> = fetch_all_decoded!(
            self,
            "SELECT id, name, upstream_model, system_prompt_prefix, upstream_chat_kwargs_json, \
             deleted_at_ms FROM model_profiles ORDER BY created_at_ms, id",
            [],
            decode_legacy_profile
        );
        let aliases: Vec<LegacyNamedRow> = fetch_all_decoded!(
            self,
            "SELECT id, name, deleted_at_ms FROM aliases ORDER BY created_at_ms, id",
            [],
            decode_legacy_named
        );
        let keys: Vec<LegacyKeyRow> = fetch_all_decoded!(
            self,
            "SELECT id, secret, label, user_id, deleted_at_ms FROM api_keys \
             ORDER BY created_at_ms, id",
            [],
            decode_legacy_key
        );
        let policy = self.get_setting("unknown_model_policy").await?;

        // API keys alone are not a legacy signal: the accounts feature stores
        // its keys in `api_keys` and the startup path loads them into the live
        // registry separately (YAML ∪ SQL). Only routing state (or a stored
        // policy) marks a database as an already-seeded operational source.
        if backends.is_empty() && profiles.is_empty() && aliases.is_empty() && policy.is_none() {
            return Ok(None);
        }

        let backend_ids = strict_legacy_ids("backend", backends.iter().map(|row| &row.id))?;
        let profile_ids = strict_legacy_ids("model profile", profiles.iter().map(|row| &row.id))?;
        let alias_ids = strict_legacy_ids("alias", aliases.iter().map(|row| &row.id))?;
        let key_ids = strict_legacy_ids("API key", keys.iter().map(|row| &row.id))?;

        let live_backends: HashSet<&str> = backends
            .iter()
            .filter(|row| row.deleted_at_ms.is_none())
            .map(|row| row.id.as_str())
            .collect();
        let live_profiles: HashSet<&str> = profiles
            .iter()
            .filter(|row| row.deleted_at_ms.is_none())
            .map(|row| row.id.as_str())
            .collect();
        let live_aliases: HashSet<&str> = aliases
            .iter()
            .filter(|row| row.deleted_at_ms.is_none())
            .map(|row| row.id.as_str())
            .collect();

        let profile_backends = self
            .load_legacy_edges(
                "SELECT profile_id, backend_id FROM profile_backends \
                 ORDER BY profile_id, ordinal, backend_id",
            )
            .await?;
        let alias_profiles = self
            .load_legacy_edges(
                "SELECT alias_id, profile_id FROM alias_profiles \
                 ORDER BY alias_id, ordinal, profile_id",
            )
            .await?;
        let key_aliases = self
            .load_legacy_edges("SELECT key_id, alias_id FROM key_aliases ORDER BY key_id, alias_id")
            .await?;

        let operational = crate::control_plane::OperationalConfig {
            backends: backends
                .iter()
                .filter(|row| row.deleted_at_ms.is_none())
                .map(|row| {
                    Ok(crate::control_plane::OpBackend {
                        id: backend_ids[&row.id],
                        name: row.name.clone(),
                        base_url: row.base_url.clone(),
                        api_key: row.api_key.clone(),
                        api_key_env: None,
                        request_log_path: row.request_log_path.clone(),
                        enabled: row.enabled != 0,
                        extra: legacy_backend_extra(row.metrics_json.as_deref())?,
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?,
            model_profiles: profiles
                .iter()
                .filter(|row| row.deleted_at_ms.is_none())
                .map(|row| {
                    Ok(crate::control_plane::OpProfile {
                        id: profile_ids[&row.id],
                        name: row.name.clone(),
                        backends: live_legacy_edges(
                            &profile_backends,
                            &row.id,
                            &live_backends,
                            &backend_ids,
                        ),
                        profile: crate::config::PersistedModelProfile {
                            upstream_model: row.upstream_model.clone(),
                            system_prompt_prefix: row.system_prompt_prefix.clone(),
                            upstream_chat_kwargs: legacy_kwargs(
                                row.upstream_chat_kwargs_json.as_deref(),
                            )?,
                            ..crate::config::PersistedModelProfile::default()
                        },
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?,
            aliases: aliases
                .iter()
                .filter(|row| row.deleted_at_ms.is_none())
                .map(|row| crate::control_plane::OpAlias {
                    id: alias_ids[&row.id],
                    name: row.name.clone(),
                    profiles: live_legacy_edges(
                        &alias_profiles,
                        &row.id,
                        &live_profiles,
                        &profile_ids,
                    ),
                    extra: BTreeMap::new(),
                })
                .collect(),
            keys: keys
                .iter()
                .filter(|row| row.deleted_at_ms.is_none())
                .map(|row| {
                    let stored_scopes = key_aliases.get(&row.id).map_or(0, Vec::len);
                    let allowed_aliases = live_legacy_edges(
                        &key_aliases,
                        &row.id,
                        &live_aliases,
                        &alias_ids,
                    );
                    if stored_scopes > 0 && allowed_aliases.is_empty() {
                        return Err(format!(
                            "legacy API key '{}' has scopes but no live aliases; refusing to widen it to unrestricted access",
                            row.id
                        ));
                    }
                    Ok(crate::control_plane::OpKey {
                        id: key_ids[&row.id],
                        // Never surface a legacy plaintext secret. The database
                        // remains unchanged so an old binary can still roll back.
                        key: canonical_api_key_digest(&row.secret),
                        label: row.label.clone(),
                        user_id: row
                            .user_id
                            .as_deref()
                            .map(|id| strict_legacy_uuid("API key owner", id))
                            .transpose()?,
                        allowed_aliases,
                        extra: BTreeMap::new(),
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?,
            unknown_model_policy: match policy.as_deref().map(str::trim) {
                Some(value) if value.eq_ignore_ascii_case("reject") => {
                    crate::control_plane::UnknownModelPolicy::Reject
                }
                _ => crate::control_plane::UnknownModelPolicy::Passthrough,
            },
            extra: BTreeMap::new(),
        };
        operational.validate()?;
        Ok(Some(LegacyOperationalRead::Relational(operational)))
    }

    async fn load_legacy_edges(&self, sql: &str) -> StoreResult<HashMap<String, Vec<String>>> {
        let pairs: Vec<(String, String)> = fetch_all_decoded!(self, sql, [], decode_legacy_edge);
        let mut edges = HashMap::<String, Vec<String>>::new();
        for (parent, child) in pairs {
            edges.entry(parent).or_default().push(child);
        }
        Ok(edges)
    }
}

fn strict_legacy_uuid(kind: &str, value: &str) -> StoreResult<uuid::Uuid> {
    uuid::Uuid::parse_str(value)
        .map_err(|_| format!("legacy {kind} id is not a valid UUID: {value:?}"))
}

fn strict_legacy_ids<'a>(
    kind: &str,
    values: impl Iterator<Item = &'a String>,
) -> StoreResult<HashMap<String, uuid::Uuid>> {
    values
        .map(|value| Ok((value.clone(), strict_legacy_uuid(kind, value)?)))
        .collect()
}

fn live_legacy_edges(
    edges: &HashMap<String, Vec<String>>,
    parent: &str,
    live_children: &HashSet<&str>,
    ids: &HashMap<String, uuid::Uuid>,
) -> Vec<uuid::Uuid> {
    edges
        .get(parent)
        .into_iter()
        .flatten()
        .filter(|child| live_children.contains(child.as_str()))
        .filter_map(|child| ids.get(child).copied())
        .collect()
}

fn legacy_backend_extra(raw: Option<&str>) -> StoreResult<BTreeMap<String, serde_json::Value>> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(BTreeMap::new());
    };
    let metrics = serde_json::from_str(raw)
        .map_err(|error| format!("invalid legacy backend metrics JSON: {error}"))?;
    Ok(BTreeMap::from([("metrics".to_string(), metrics)]))
}

fn legacy_kwargs(raw: Option<&str>) -> StoreResult<serde_json::Map<String, serde_json::Value>> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(serde_json::Map::new());
    };
    serde_json::from_str(raw)
        .map_err(|error| format!("invalid legacy profile kwargs JSON: {error}"))
}

#[async_trait]
impl PersistenceWriter for SqlStore {
    async fn begin_request(&self, row: RequestRow) -> StoreResult<()> {
        let sql = format!(
            "INSERT INTO requests (id, response_id, conversation_id, virtual_key_id, \
             client_protocol, client_model, alias, backend, resolved_model, status, created_at_ms, \
             harness, harness_version, harness_session_id, harness_sub_session_id, \
             harness_parent_session_id, session_kind, session_id, chain_parent_request_id, \
             item_count, shared_prefix_items, divergence_kind, divergence_index, cache_bust, \
             client_label, client_source, user_id) VALUES ({})",
            placeholders(self.postgres(), 27)
        );
        execute!(
            self,
            &sql,
            row.id,
            row.response_id,
            row.conversation_id,
            row.virtual_key_id,
            row.client_protocol,
            row.client_model,
            row.alias,
            row.backend,
            row.resolved_model,
            row.status,
            row.created_at_ms,
            row.harness,
            row.harness_version,
            row.harness_session_id,
            row.harness_sub_session_id,
            row.harness_parent_session_id,
            row.session_kind,
            row.session_id,
            row.chain_parent_request_id,
            row.item_count,
            row.shared_prefix_items,
            row.divergence_kind,
            row.divergence_index,
            row.cache_bust.map(i64::from),
            row.client_label,
            row.client_source,
            row.user_id,
        );
        Ok(())
    }

    async fn append_event(&self, event: EventRow) -> StoreResult<()> {
        let sql = format!(
            "INSERT INTO request_events (request_id, seq, ts_ms, hop, kind, payload, bytes) \
             VALUES ({}) ON CONFLICT (request_id, seq) DO UPDATE SET \
             ts_ms = excluded.ts_ms, hop = excluded.hop, kind = excluded.kind, \
             payload = excluded.payload, bytes = excluded.bytes",
            placeholders(self.postgres(), 7)
        );
        execute!(
            self,
            &sql,
            event.request_id,
            event.seq,
            event.ts_ms,
            event.hop,
            event.kind,
            event.payload,
            event.bytes,
        );
        Ok(())
    }

    async fn store_body(&self, body: BodyWrite) -> StoreResult<()> {
        let pg = self.postgres();
        let insert_blob = format!(
            "INSERT INTO content_blobs (hash, media, size, content, created_at_ms) \
             VALUES ({}) ON CONFLICT (hash) DO NOTHING",
            placeholders(pg, 5)
        );
        let insert_item = format!(
            "INSERT INTO request_items (request_id, hop, ordinal, section, kind, blob_hash, \
             identity_hash) \
             VALUES ({}) ON CONFLICT (request_id, hop, ordinal) DO UPDATE SET \
             section = excluded.section, kind = excluded.kind, blob_hash = excluded.blob_hash, \
             identity_hash = excluded.identity_hash",
            placeholders(pg, 7)
        );
        let insert_event = format!(
            "INSERT INTO request_events (request_id, seq, ts_ms, hop, kind, payload, bytes) \
             VALUES ({}) ON CONFLICT (request_id, seq) DO UPDATE SET \
             ts_ms = excluded.ts_ms, hop = excluded.hop, kind = excluded.kind, \
             payload = excluded.payload, bytes = excluded.bytes",
            placeholders(pg, 7)
        );
        let BodyWrite {
            event,
            items,
            blobs,
        } = body;

        macro_rules! store_in_transaction {
            ($pool:expr) => {{
                let mut transaction = $pool.begin().await.map_err(store_error)?;
                for blob in &blobs {
                    sqlx::query(&insert_blob)
                        .bind(&blob.hash)
                        .bind(&blob.media)
                        .bind(blob.size)
                        .bind(&blob.content)
                        .bind(blob.created_at_ms)
                        .execute(&mut *transaction)
                        .await
                        .map_err(store_error)?;
                }
                for item in &items {
                    sqlx::query(&insert_item)
                        .bind(&item.request_id)
                        .bind(&item.hop)
                        .bind(item.ordinal)
                        .bind(&item.section)
                        .bind(&item.kind)
                        .bind(&item.blob_hash)
                        .bind(&item.identity_hash)
                        .execute(&mut *transaction)
                        .await
                        .map_err(store_error)?;
                }
                sqlx::query(&insert_event)
                    .bind(&event.request_id)
                    .bind(event.seq)
                    .bind(event.ts_ms)
                    .bind(&event.hop)
                    .bind(&event.kind)
                    .bind(&event.payload)
                    .bind(event.bytes)
                    .execute(&mut *transaction)
                    .await
                    .map_err(store_error)?;
                transaction.commit().await.map_err(store_error)?;
            }};
        }

        match &self.pool {
            SqlPool::Sqlite(pool) => store_in_transaction!(pool),
            SqlPool::Postgres(pool) => store_in_transaction!(pool),
        }
        Ok(())
    }

    async fn upsert_session(&self, row: crate::sessions::SessionRow) -> StoreResult<()> {
        let pg = self.postgres();
        // SQLite's multi-argument MAX is Postgres' GREATEST.
        let greatest = if pg { "GREATEST" } else { "MAX" };
        let sql = format!(
            "INSERT INTO sessions (id, parent_id, kind, harness, harness_version, external_id, \
             session_kind, client_label, virtual_key_id, user_id, depth, root_request_id, \
             spawned_by_request_id, first_seen_ms, last_seen_ms, request_count) VALUES ({}) \
             ON CONFLICT (id) DO UPDATE SET \
             parent_id = COALESCE(excluded.parent_id, sessions.parent_id), \
             harness_version = COALESCE(excluded.harness_version, sessions.harness_version), \
             session_kind = COALESCE(excluded.session_kind, sessions.session_kind), \
             client_label = COALESCE(sessions.client_label, excluded.client_label), \
             virtual_key_id = COALESCE(sessions.virtual_key_id, excluded.virtual_key_id), \
             user_id = COALESCE(sessions.user_id, excluded.user_id), \
             depth = excluded.depth, \
             root_request_id = COALESCE(sessions.root_request_id, excluded.root_request_id), \
             spawned_by_request_id = COALESCE(sessions.spawned_by_request_id, \
                 excluded.spawned_by_request_id), \
             last_seen_ms = {greatest}(sessions.last_seen_ms, excluded.last_seen_ms), \
             request_count = {greatest}(sessions.request_count, excluded.request_count)",
            placeholders(pg, 16)
        );
        execute!(
            self,
            &sql,
            row.id,
            row.parent_id,
            row.kind,
            row.harness,
            row.harness_version,
            row.external_id,
            row.session_kind,
            row.client_label,
            row.virtual_key_id,
            row.user_id,
            row.depth,
            row.root_request_id,
            row.spawned_by_request_id,
            row.first_seen_ms,
            row.last_seen_ms,
            row.request_count,
        );
        Ok(())
    }

    async fn finish_request(&self, id: &str, finish: RequestFinish) -> StoreResult<()> {
        let pg = self.postgres();
        let sql = format!(
            "INSERT INTO requests (id, client_protocol, client_model, status, created_at_ms, \
             response_id, completed_at_ms, first_token_at_ms, input_tokens, output_tokens, \
             cached_tokens, reasoning_tokens, error, terminal_reason, backend, resolved_model, \
             attempts_json, timings_json, client_label, client_source) VALUES ({}, 'unknown', '', \
             {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) \
             ON CONFLICT (id) DO UPDATE SET response_id = excluded.response_id, \
             status = excluded.status, completed_at_ms = excluded.completed_at_ms, \
             first_token_at_ms = excluded.first_token_at_ms, input_tokens = excluded.input_tokens, \
             output_tokens = excluded.output_tokens, cached_tokens = excluded.cached_tokens, \
             reasoning_tokens = excluded.reasoning_tokens, error = excluded.error, \
             terminal_reason = excluded.terminal_reason, backend = excluded.backend, \
             resolved_model = excluded.resolved_model, attempts_json = excluded.attempts_json, \
             timings_json = excluded.timings_json, \
             client_label = COALESCE(excluded.client_label, requests.client_label), \
             client_source = COALESCE(excluded.client_source, requests.client_source)",
            placeholder(pg, 1),
            placeholder(pg, 2),
            placeholder(pg, 3),
            placeholder(pg, 4),
            placeholder(pg, 5),
            placeholder(pg, 6),
            placeholder(pg, 7),
            placeholder(pg, 8),
            placeholder(pg, 9),
            placeholder(pg, 10),
            placeholder(pg, 11),
            placeholder(pg, 12),
            placeholder(pg, 13),
            placeholder(pg, 14),
            placeholder(pg, 15),
            placeholder(pg, 16),
            placeholder(pg, 17),
            placeholder(pg, 18),
        );
        execute!(
            self,
            &sql,
            id,
            finish.status,
            finish.completed_at_ms,
            finish.response_id,
            finish.completed_at_ms,
            finish.first_token_at_ms,
            finish.input_tokens,
            finish.output_tokens,
            finish.cached_tokens,
            finish.reasoning_tokens,
            finish.error,
            finish.terminal_reason,
            finish.backend,
            finish.resolved_model,
            finish.attempts_json,
            finish.timings_json,
            finish.client_label,
            finish.client_source,
        );
        Ok(())
    }
}

#[async_trait]
impl PersistenceStore for SqlStore {
    async fn get_request(&self, id: &str) -> StoreResult<Option<RequestSummary>> {
        let sql = format!(
            "{REQUEST_COLUMNS} WHERE id = {}",
            placeholder(self.postgres(), 1)
        );
        match &self.pool {
            SqlPool::Sqlite(pool) => sqlx::query(&sql)
                .bind(id)
                .fetch_optional(pool)
                .await
                .map_err(store_error)?
                .map(|row| decode_request(&row))
                .transpose(),
            SqlPool::Postgres(pool) => sqlx::query(&sql)
                .bind(id)
                .fetch_optional(pool)
                .await
                .map_err(store_error)?
                .map(|row| decode_request(&row))
                .transpose(),
        }
    }

    async fn request_events(&self, request_id: &str) -> StoreResult<Vec<EventRow>> {
        let sql = format!(
            "SELECT request_id, seq, ts_ms, hop, kind, payload, bytes FROM request_events \
             WHERE request_id = {} ORDER BY seq",
            placeholder(self.postgres(), 1)
        );
        Ok(fetch_all_decoded!(self, &sql, [request_id], decode_event))
    }

    async fn request_events_limited(
        &self,
        request_id: &str,
        limit: usize,
    ) -> StoreResult<Vec<EventRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let pg = self.postgres();
        let sql = format!(
            "SELECT request_id, seq, ts_ms, hop, kind, payload, bytes FROM (\
                 SELECT request_id, seq, ts_ms, hop, kind, payload, bytes \
                 FROM request_events WHERE request_id = {} \
                 ORDER BY seq DESC LIMIT {}\
             ) AS recent ORDER BY seq",
            placeholder(pg, 1),
            placeholder(pg, 2),
        );
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        Ok(fetch_all_decoded!(
            self,
            &sql,
            [request_id, limit],
            decode_event
        ))
    }

    async fn list_requests(&self, limit: i64) -> StoreResult<Vec<RequestSummary>> {
        let sql = format!(
            "{REQUEST_COLUMNS} ORDER BY created_at_ms DESC LIMIT {}",
            placeholder(self.postgres(), 1)
        );
        Ok(fetch_all_decoded!(self, &sql, [limit], decode_request))
    }

    async fn usage_summary(&self, filter: &UsageFilter) -> StoreResult<Vec<UsageBucket>> {
        let pg = self.postgres();
        let mut sql = String::from(
            "SELECT user_id, virtual_key_id, alias, resolved_model, backend, \
             CAST(COUNT(*) AS BIGINT), CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(cached_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(reasoning_tokens), 0) AS BIGINT) FROM requests",
        );
        let mut clauses = Vec::new();
        if filter.virtual_key_id.is_some() {
            clauses.push(format!(
                "virtual_key_id = {}",
                placeholder(pg, clauses.len() + 1)
            ));
        }
        if filter.user_id.is_some() {
            clauses.push(format!("user_id = {}", placeholder(pg, clauses.len() + 1)));
        }
        if filter.since_ms.is_some() {
            clauses.push(format!(
                "created_at_ms >= {}",
                placeholder(pg, clauses.len() + 1)
            ));
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" GROUP BY user_id, virtual_key_id, alias, resolved_model, backend");

        macro_rules! query_usage {
            ($pool:expr) => {{
                let mut query = sqlx::query(&sql);
                if let Some(key) = filter.virtual_key_id.as_deref() {
                    query = query.bind(key);
                }
                if let Some(user) = filter.user_id.as_deref() {
                    query = query.bind(user);
                }
                if let Some(since) = filter.since_ms {
                    query = query.bind(since);
                }
                query
                    .fetch_all($pool)
                    .await
                    .map_err(store_error)?
                    .into_iter()
                    .map(|row| {
                        Ok(UsageBucket {
                            user_id: row.try_get(0).map_err(store_error)?,
                            virtual_key_id: row.try_get(1).map_err(store_error)?,
                            alias: row.try_get(2).map_err(store_error)?,
                            resolved_model: row.try_get(3).map_err(store_error)?,
                            backend: row.try_get(4).map_err(store_error)?,
                            requests: row.try_get(5).map_err(store_error)?,
                            input_tokens: row.try_get(6).map_err(store_error)?,
                            output_tokens: row.try_get(7).map_err(store_error)?,
                            cached_tokens: row.try_get(8).map_err(store_error)?,
                            reasoning_tokens: row.try_get(9).map_err(store_error)?,
                        })
                    })
                    .collect::<StoreResult<Vec<_>>>()
            }};
        }
        match &self.pool {
            SqlPool::Sqlite(pool) => query_usage!(pool),
            SqlPool::Postgres(pool) => query_usage!(pool),
        }
    }

    async fn usage_summary_limited(
        &self,
        filter: &UsageFilter,
        limit: usize,
    ) -> StoreResult<Vec<UsageBucket>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let pg = self.postgres();
        let mut sql = String::from(
            "SELECT user_id, virtual_key_id, alias, resolved_model, backend, \
             CAST(COUNT(*) AS BIGINT) AS request_count, \
             CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(cached_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(reasoning_tokens), 0) AS BIGINT) FROM requests",
        );
        let mut clauses = Vec::new();
        if filter.virtual_key_id.is_some() {
            clauses.push(format!(
                "virtual_key_id = {}",
                placeholder(pg, clauses.len() + 1)
            ));
        }
        if filter.user_id.is_some() {
            clauses.push(format!("user_id = {}", placeholder(pg, clauses.len() + 1)));
        }
        if filter.since_ms.is_some() {
            clauses.push(format!(
                "created_at_ms >= {}",
                placeholder(pg, clauses.len() + 1)
            ));
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" GROUP BY user_id, virtual_key_id, alias, resolved_model, backend");
        sql.push_str(" ORDER BY request_count DESC LIMIT ");
        sql.push_str(&placeholder(pg, clauses.len() + 1));
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);

        macro_rules! query_usage_limited {
            ($pool:expr) => {{
                let mut query = sqlx::query(&sql);
                if let Some(key) = filter.virtual_key_id.as_deref() {
                    query = query.bind(key);
                }
                if let Some(user) = filter.user_id.as_deref() {
                    query = query.bind(user);
                }
                if let Some(since) = filter.since_ms {
                    query = query.bind(since);
                }
                query = query.bind(limit);
                query
                    .fetch_all($pool)
                    .await
                    .map_err(store_error)?
                    .into_iter()
                    .map(|row| {
                        Ok(UsageBucket {
                            user_id: row.try_get(0).map_err(store_error)?,
                            virtual_key_id: row.try_get(1).map_err(store_error)?,
                            alias: row.try_get(2).map_err(store_error)?,
                            resolved_model: row.try_get(3).map_err(store_error)?,
                            backend: row.try_get(4).map_err(store_error)?,
                            requests: row.try_get(5).map_err(store_error)?,
                            input_tokens: row.try_get(6).map_err(store_error)?,
                            output_tokens: row.try_get(7).map_err(store_error)?,
                            cached_tokens: row.try_get(8).map_err(store_error)?,
                            reasoning_tokens: row.try_get(9).map_err(store_error)?,
                        })
                    })
                    .collect::<StoreResult<Vec<_>>>()
            }};
        }
        match &self.pool {
            SqlPool::Sqlite(pool) => query_usage_limited!(pool),
            SqlPool::Postgres(pool) => query_usage_limited!(pool),
        }
    }

    async fn prune_request_history(&self, before_ms: i64) -> StoreResult<()> {
        let pg = self.postgres();
        let delete_events = format!(
            "DELETE FROM request_events WHERE request_id IN (\
                 SELECT id FROM requests WHERE created_at_ms < {}\
             )",
            placeholder(pg, 1)
        );
        let delete_items = format!(
            "DELETE FROM request_items WHERE request_id IN (\
                 SELECT id FROM requests WHERE created_at_ms < {}\
             )",
            placeholder(pg, 1)
        );
        let delete_requests = format!(
            "DELETE FROM requests WHERE created_at_ms < {}",
            placeholder(pg, 1)
        );
        let delete_orphan_events = format!(
            "DELETE FROM request_events WHERE ts_ms < {} AND NOT EXISTS (\
                 SELECT 1 FROM requests WHERE requests.id = request_events.request_id\
             )",
            placeholder(pg, 1)
        );

        macro_rules! prune_in_transaction {
            ($pool:expr) => {{
                let mut transaction = $pool.begin().await.map_err(store_error)?;
                sqlx::query(&delete_events)
                    .bind(before_ms)
                    .execute(&mut *transaction)
                    .await
                    .map_err(store_error)?;
                sqlx::query(&delete_items)
                    .bind(before_ms)
                    .execute(&mut *transaction)
                    .await
                    .map_err(store_error)?;
                sqlx::query(&delete_requests)
                    .bind(before_ms)
                    .execute(&mut *transaction)
                    .await
                    .map_err(store_error)?;
                sqlx::query(&delete_orphan_events)
                    .bind(before_ms)
                    .execute(&mut *transaction)
                    .await
                    .map_err(store_error)?;
                transaction.commit().await.map_err(store_error)
            }};
        }

        match &self.pool {
            SqlPool::Sqlite(pool) => prune_in_transaction!(pool),
            SqlPool::Postgres(pool) => prune_in_transaction!(pool),
        }
    }

    async fn request_items(&self, request_id: &str, hop: &str) -> StoreResult<Vec<ItemRow>> {
        let pg = self.postgres();
        let sql = format!(
            "SELECT request_id, hop, ordinal, section, kind, blob_hash, identity_hash \
             FROM request_items \
             WHERE request_id = {} AND hop = {} ORDER BY ordinal",
            placeholder(pg, 1),
            placeholder(pg, 2)
        );
        Ok(fetch_all_decoded!(
            self,
            &sql,
            [request_id, hop],
            decode_item
        ))
    }

    async fn get_blobs(&self, hashes: &[String]) -> StoreResult<Vec<BlobRow>> {
        // Stay well under SQLite's bound-parameter limit per statement.
        const CHUNK: usize = 256;
        let pg = self.postgres();
        let mut blobs = Vec::with_capacity(hashes.len());
        for chunk in hashes.chunks(CHUNK) {
            let sql = format!(
                "SELECT hash, media, size, content, created_at_ms FROM content_blobs \
                 WHERE hash IN ({})",
                placeholders(pg, chunk.len())
            );
            macro_rules! fetch_chunk {
                ($pool:expr) => {{
                    let mut query = sqlx::query(&sql);
                    for hash in chunk {
                        query = query.bind(hash);
                    }
                    query
                        .fetch_all($pool)
                        .await
                        .map_err(store_error)?
                        .iter()
                        .map(decode_blob)
                        .collect::<StoreResult<Vec<BlobRow>>>()?
                }};
            }
            let rows = match &self.pool {
                SqlPool::Sqlite(pool) => fetch_chunk!(pool),
                SqlPool::Postgres(pool) => fetch_chunk!(pool),
            };
            blobs.extend(rows);
        }
        Ok(blobs)
    }

    async fn prune_orphan_blobs(&self) -> StoreResult<u64> {
        let sql = "DELETE FROM content_blobs WHERE NOT EXISTS (\
                       SELECT 1 FROM request_items WHERE request_items.blob_hash = content_blobs.hash\
                   )";
        let affected = match &self.pool {
            SqlPool::Sqlite(pool) => sqlx::query(sql)
                .execute(pool)
                .await
                .map_err(store_error)?
                .rows_affected(),
            SqlPool::Postgres(pool) => sqlx::query(sql)
                .execute(pool)
                .await
                .map_err(store_error)?
                .rows_affected(),
        };
        Ok(affected)
    }

    async fn get_session(&self, id: &str) -> StoreResult<Option<crate::sessions::SessionRow>> {
        let sql = format!(
            "{SESSION_COLUMNS} WHERE id = {}",
            placeholder(self.postgres(), 1)
        );
        let rows: Vec<crate::sessions::SessionRow> =
            fetch_all_decoded!(self, &sql, [id], decode_session);
        Ok(rows.into_iter().next())
    }

    async fn session_aggregate(&self, session_id: &str) -> StoreResult<Option<SessionAggregate>> {
        // Select the bound id as a constant rather than the filtered column.
        // PostgreSQL rejects an ungrouped column beside aggregates, while a
        // constant preserves the useful all-null aggregate row for sessions
        // whose requests have not reported usage.
        let sql = "SELECT CAST(?1 AS TEXT), \
             CAST(COALESCE(SUM(CASE WHEN input_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN output_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN cached_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(cached_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN reasoning_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(reasoning_tokens), 0) AS BIGINT) \
             FROM requests WHERE session_id = ?1";
        let rows: Vec<(String, i64, i64, i64, i64, i64, i64, i64, i64)> = match &self.pool {
            SqlPool::Sqlite(pool) => {
                sqlx::query_as::<_, (String, i64, i64, i64, i64, i64, i64, i64, i64)>(sql)
                    .bind(session_id)
                    .fetch_all(pool)
                    .await
                    .map_err(store_error)?
            }
            SqlPool::Postgres(pool) => sqlx::query_as::<
                _,
                (String, i64, i64, i64, i64, i64, i64, i64, i64),
            >(sql.replace("?1", "$1").as_str())
            .bind(session_id)
            .fetch_all(pool)
            .await
            .map_err(store_error)?,
        };
        // An aggregate with zero reporting rows in every class is a session
        // whose requests never reported usage — keep the row (all `None`) so
        // the caller renders "unavailable", not "missing".
        Ok(rows.into_iter().next().map(
            |(
                session_id,
                in_n,
                in_sum,
                out_n,
                out_sum,
                cached_n,
                cached_sum,
                reasoning_n,
                reasoning_sum,
            )| {
                SessionAggregate {
                    session_id: session_id.clone(),
                    input_tokens: (in_n > 0).then_some(in_sum),
                    output_tokens: (out_n > 0).then_some(out_sum),
                    cached_tokens: (cached_n > 0).then_some(cached_sum),
                    reasoning_tokens: (reasoning_n > 0).then_some(reasoning_sum),
                }
            },
        ))
    }

    async fn find_session(
        &self,
        harness: &str,
        external_id: Option<&str>,
        client_label: Option<&str>,
    ) -> StoreResult<Option<crate::sessions::SessionRow>> {
        let pg = self.postgres();
        let rows: Vec<crate::sessions::SessionRow> = match external_id {
            Some(external_id) => {
                let sql = format!(
                    "{SESSION_COLUMNS} WHERE harness = {} AND external_id = {} \
                     ORDER BY last_seen_ms DESC LIMIT 1",
                    placeholder(pg, 1),
                    placeholder(pg, 2)
                );
                fetch_all_decoded!(self, &sql, [harness, external_id], decode_session)
            }
            None => {
                let sql = format!(
                    "{SESSION_COLUMNS} WHERE harness = {} AND external_id IS NULL \
                     AND parent_id IS NULL AND kind = 'inferred' \
                     AND COALESCE(client_label, '') = {} ORDER BY last_seen_ms DESC LIMIT 1",
                    placeholder(pg, 1),
                    placeholder(pg, 2)
                );
                let label = client_label.unwrap_or_default();
                fetch_all_decoded!(self, &sql, [harness, label], decode_session)
            }
        };
        Ok(rows.into_iter().next())
    }

    async fn list_sessions(
        &self,
        since_ms: i64,
        roots_only: bool,
        limit: usize,
    ) -> StoreResult<Vec<crate::sessions::SessionRow>> {
        let pg = self.postgres();
        let roots = if roots_only {
            " AND parent_id IS NULL"
        } else {
            ""
        };
        let sql = format!(
            "{SESSION_COLUMNS} WHERE last_seen_ms >= {}{roots} \
             ORDER BY last_seen_ms DESC, id LIMIT {}",
            placeholder(pg, 1),
            placeholder(pg, 2)
        );
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        Ok(fetch_all_decoded!(
            self,
            &sql,
            [since_ms, limit],
            decode_session
        ))
    }

    async fn session_children(
        &self,
        parent_id: &str,
    ) -> StoreResult<Vec<crate::sessions::SessionRow>> {
        let sql = format!(
            "{SESSION_COLUMNS} WHERE parent_id = {} ORDER BY first_seen_ms, id",
            placeholder(self.postgres(), 1)
        );
        Ok(fetch_all_decoded!(self, &sql, [parent_id], decode_session))
    }

    async fn session_descendants(
        &self,
        id: &str,
        limit: usize,
    ) -> StoreResult<Vec<crate::sessions::SessionRow>> {
        let mut out = Vec::new();
        let mut frontier = vec![id.to_string()];
        while let Some(parent) = frontier.first().cloned() {
            frontier.remove(0);
            for child in self.session_children(&parent).await? {
                if out.len() >= limit {
                    return Ok(out);
                }
                frontier.push(child.id.clone());
                out.push(child);
            }
        }
        Ok(out)
    }

    async fn session_requests(
        &self,
        session_id: &str,
        limit: usize,
    ) -> StoreResult<Vec<RequestSummary>> {
        let pg = self.postgres();
        let sql = format!(
            "{REQUEST_COLUMNS} WHERE session_id = {} ORDER BY created_at_ms DESC, id LIMIT {}",
            placeholder(pg, 1),
            placeholder(pg, 2)
        );
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut rows: Vec<RequestSummary> =
            fetch_all_decoded!(self, &sql, [session_id, limit], decode_request);
        rows.reverse();
        Ok(rows)
    }

    async fn chain_head(
        &self,
        session_id: &str,
    ) -> StoreResult<Option<crate::sessions::ChainHead>> {
        let latest = self.session_requests(session_id, 1).await?;
        let Some(request) = latest.into_iter().next() else {
            return Ok(None);
        };
        let items = self
            .request_items(&request.id, "client_in")
            .await?
            .into_iter()
            .map(|item| crate::sessions::ItemFingerprint {
                hash: item.identity_hash.unwrap_or(item.blob_hash),
                section: crate::content_store::ItemSection::parse(&item.section)
                    .unwrap_or(crate::content_store::ItemSection::Message),
                kind: item.kind,
            })
            .collect();
        Ok(Some(crate::sessions::ChainHead {
            request_id: request.id,
            items,
        }))
    }

    async fn throughput_series(
        &self,
        since_ms: i64,
        bucket_ms: i64,
        limit: usize,
    ) -> StoreResult<Vec<ThroughputBucket>> {
        let pg = self.postgres();
        let bucket_ms = bucket_ms.max(1);
        let sql = format!(
            "SELECT (created_at_ms / {b1}) * {b2} AS bucket_ms, \
             COALESCE(resolved_model, client_model) AS model, backend, \
             CAST(COUNT(*) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(cached_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN first_token_at_ms IS NOT NULL \
                 THEN first_token_at_ms - created_at_ms END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN first_token_at_ms IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN first_token_at_ms IS NOT NULL THEN input_tokens END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN first_token_at_ms IS NOT NULL AND completed_at_ms IS NOT NULL \
                 AND output_tokens IS NOT NULL THEN completed_at_ms - first_token_at_ms END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN first_token_at_ms IS NOT NULL AND completed_at_ms IS NOT NULL \
                 AND output_tokens IS NOT NULL THEN output_tokens END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN first_token_at_ms IS NOT NULL AND completed_at_ms IS NOT NULL \
                 AND output_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT) \
             FROM requests WHERE created_at_ms >= {since} \
             GROUP BY 1, 2, 3 ORDER BY 1, 2, 3 LIMIT {limit}",
            b1 = placeholder(pg, 1),
            b2 = placeholder(pg, 2),
            since = placeholder(pg, 3),
            limit = placeholder(pg, 4)
        );
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        Ok(fetch_all_decoded!(
            self,
            &sql,
            [bucket_ms, bucket_ms, since_ms, limit],
            decode_throughput
        ))
    }

    async fn get_setting(&self, key: &str) -> StoreResult<Option<String>> {
        let sql = format!(
            "SELECT value FROM settings WHERE key = {}",
            placeholder(self.postgres(), 1)
        );
        match &self.pool {
            SqlPool::Sqlite(pool) => sqlx::query(&sql)
                .bind(key)
                .fetch_optional(pool)
                .await
                .map_err(store_error)?
                .map(|row| row.try_get(0).map_err(store_error))
                .transpose(),
            SqlPool::Postgres(pool) => sqlx::query(&sql)
                .bind(key)
                .fetch_optional(pool)
                .await
                .map_err(store_error)?
                .map(|row| row.try_get(0).map_err(store_error))
                .transpose(),
        }
    }

    async fn set_setting(&self, key: &str, value: &str) -> StoreResult<()> {
        let pg = self.postgres();
        let sql = format!(
            "INSERT INTO settings (key, value) VALUES ({}, {}) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            placeholder(pg, 1),
            placeholder(pg, 2)
        );
        execute!(self, &sql, key, value);
        Ok(())
    }

    async fn delete_setting(&self, key: &str) -> StoreResult<()> {
        let sql = format!(
            "DELETE FROM settings WHERE key = {}",
            placeholder(self.postgres(), 1)
        );
        execute!(self, &sql, key);
        Ok(())
    }

    async fn record_backend_metrics(&self, sample: MetricSample) -> StoreResult<()> {
        let sql = format!(
            "INSERT INTO backend_metrics (id, backend, ts_ms, data) VALUES ({})",
            placeholders(self.postgres(), 4)
        );
        execute!(
            self,
            &sql,
            new_id(),
            sample.backend,
            sample.ts_ms,
            sample.data
        );
        Ok(())
    }

    async fn backend_metrics_history(&self, since_ms: i64) -> StoreResult<Vec<MetricSample>> {
        let sql = format!(
            "SELECT backend, ts_ms, data FROM backend_metrics WHERE ts_ms >= {} ORDER BY ts_ms",
            placeholder(self.postgres(), 1)
        );
        Ok(fetch_all_decoded!(self, &sql, [since_ms], decode_metric))
    }

    async fn backend_metrics_history_limited(
        &self,
        since_ms: i64,
        limit: usize,
    ) -> StoreResult<Vec<MetricSample>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let pg = self.postgres();
        let sql = format!(
            "SELECT backend, ts_ms, data FROM (\
                 SELECT id, backend, ts_ms, data FROM backend_metrics \
                 WHERE ts_ms >= {} ORDER BY ts_ms DESC, id DESC LIMIT {}\
             ) AS recent ORDER BY ts_ms, id",
            placeholder(pg, 1),
            placeholder(pg, 2),
        );
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        Ok(fetch_all_decoded!(
            self,
            &sql,
            [since_ms, limit],
            decode_metric
        ))
    }

    async fn prune_backend_metrics(&self, before_ms: i64) -> StoreResult<()> {
        let sql = format!(
            "DELETE FROM backend_metrics WHERE ts_ms < {}",
            placeholder(self.postgres(), 1)
        );
        execute!(self, &sql, before_ms);
        Ok(())
    }

    async fn list_users(&self) -> StoreResult<Vec<UserRecord>> {
        Ok(fetch_all_decoded!(
            self,
            "SELECT id, username, is_admin, created_at_ms, updated_at_ms FROM users \
             WHERE deleted_at_ms IS NULL ORDER BY username",
            [],
            decode_user
        ))
    }

    async fn count_users(&self) -> StoreResult<i64> {
        let sql = "SELECT CAST(COUNT(*) AS BIGINT) FROM users WHERE deleted_at_ms IS NULL";
        match &self.pool {
            SqlPool::Sqlite(pool) => sqlx::query(sql)
                .fetch_one(pool)
                .await
                .map_err(store_error)?
                .try_get(0)
                .map_err(store_error),
            SqlPool::Postgres(pool) => sqlx::query(sql)
                .fetch_one(pool)
                .await
                .map_err(store_error)?
                .try_get(0)
                .map_err(store_error),
        }
    }

    async fn get_user_auth(&self, username: &str) -> StoreResult<Option<UserAuth>> {
        let sql = format!(
            "SELECT id, username, password_hash, is_admin FROM users \
             WHERE username = {} AND deleted_at_ms IS NULL",
            placeholder(self.postgres(), 1)
        );
        macro_rules! get_auth {
            ($pool:expr) => {{
                sqlx::query(&sql)
                    .bind(username)
                    .fetch_optional($pool)
                    .await
                    .map_err(store_error)?
                    .map(|row| {
                        Ok(UserAuth {
                            id: row.try_get(0).map_err(store_error)?,
                            username: row.try_get(1).map_err(store_error)?,
                            password_hash: row.try_get(2).map_err(store_error)?,
                            is_admin: row.try_get::<i64, _>(3).map_err(store_error)? != 0,
                        })
                    })
                    .transpose()
            }};
        }
        match &self.pool {
            SqlPool::Sqlite(pool) => get_auth!(pool),
            SqlPool::Postgres(pool) => get_auth!(pool),
        }
    }

    async fn create_user(
        &self,
        username: &str,
        password_hash: &str,
        is_admin: bool,
        actor: &str,
    ) -> StoreResult<UserRecord> {
        let id = new_id();
        let now = now_ms();
        let sql = format!(
            "INSERT INTO users (id, username, password_hash, is_admin, created_at_ms, created_by, \
             updated_at_ms, updated_by) VALUES ({})",
            placeholders(self.postgres(), 8)
        );
        execute!(
            self,
            &sql,
            &id,
            username,
            password_hash,
            is_admin as i64,
            now,
            actor,
            now,
            actor
        );
        Ok(UserRecord {
            id,
            username: username.to_string(),
            is_admin,
            created_at_ms: now,
            updated_at_ms: now,
        })
    }

    async fn upsert_external_user(
        &self,
        id: &str,
        username: &str,
        is_admin: bool,
        actor: &str,
    ) -> StoreResult<UserRecord> {
        let now = now_ms();
        let password_hash = "external-oauth-only";
        let pg = self.postgres();
        let sql = if pg {
            "INSERT INTO users (id, username, password_hash, is_admin, created_at_ms, created_by, \
             updated_at_ms, updated_by, deleted_at_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $5, $6, NULL) \
             ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, \
             is_admin = EXCLUDED.is_admin, updated_at_ms = EXCLUDED.updated_at_ms, \
             updated_by = EXCLUDED.updated_by, deleted_at_ms = NULL"
                .to_string()
        } else {
            "INSERT INTO users (id, username, password_hash, is_admin, created_at_ms, created_by, \
             updated_at_ms, updated_by, deleted_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?5, ?6, NULL) \
             ON CONFLICT(id) DO UPDATE SET username = excluded.username, \
             is_admin = excluded.is_admin, updated_at_ms = excluded.updated_at_ms, \
             updated_by = excluded.updated_by, deleted_at_ms = NULL"
                .to_string()
        };
        execute!(
            self,
            &sql,
            id,
            username,
            password_hash,
            is_admin as i64,
            now,
            actor
        );
        Ok(UserRecord {
            id: id.to_string(),
            username: username.to_string(),
            is_admin,
            created_at_ms: now,
            updated_at_ms: now,
        })
    }

    async fn update_user(
        &self,
        id: &str,
        password_hash: Option<&str>,
        is_admin: Option<bool>,
        actor: &str,
    ) -> StoreResult<()> {
        let pg = self.postgres();
        let now = now_ms();
        match (password_hash, is_admin) {
            (None, None) => return Ok(()),
            (Some(hash), None) => {
                let sql = format!(
                    "UPDATE users SET password_hash = {}, updated_at_ms = {}, updated_by = {}, \
                     deleted_at_ms = NULL WHERE id = {}",
                    placeholder(pg, 1),
                    placeholder(pg, 2),
                    placeholder(pg, 3),
                    placeholder(pg, 4)
                );
                execute!(self, &sql, hash, now, actor, id);
            }
            (None, Some(admin)) => {
                let sql = format!(
                    "UPDATE users SET is_admin = {}, updated_at_ms = {}, updated_by = {}, \
                     deleted_at_ms = NULL WHERE id = {}",
                    placeholder(pg, 1),
                    placeholder(pg, 2),
                    placeholder(pg, 3),
                    placeholder(pg, 4)
                );
                execute!(self, &sql, admin as i64, now, actor, id);
            }
            (Some(hash), Some(admin)) => {
                let sql = format!(
                    "UPDATE users SET password_hash = {}, is_admin = {}, updated_at_ms = {}, \
                     updated_by = {}, deleted_at_ms = NULL WHERE id = {}",
                    placeholder(pg, 1),
                    placeholder(pg, 2),
                    placeholder(pg, 3),
                    placeholder(pg, 4),
                    placeholder(pg, 5)
                );
                execute!(self, &sql, hash, admin as i64, now, actor, id);
            }
        }
        Ok(())
    }

    async fn delete_user(&self, id: &str, actor: &str) -> StoreResult<()> {
        let pg = self.postgres();
        let now = now_ms();
        let sql = format!(
            "UPDATE users SET deleted_at_ms = {}, updated_at_ms = {}, updated_by = {} WHERE id = {}",
            placeholder(pg, 1),
            placeholder(pg, 2),
            placeholder(pg, 3),
            placeholder(pg, 4)
        );
        execute!(self, &sql, now, now, actor, id);
        Ok(())
    }

    async fn list_api_keys(&self) -> StoreResult<Vec<ApiKeyRecord>> {
        Ok(fetch_all_decoded!(
            self,
            "SELECT id, label, user_id, created_at_ms, updated_at_ms, allowed_models_json \
             FROM api_keys WHERE deleted_at_ms IS NULL ORDER BY created_at_ms",
            [],
            decode_key
        ))
    }

    async fn list_api_keys_for_user(&self, user_id: &str) -> StoreResult<Vec<ApiKeyRecord>> {
        let sql = format!(
            "SELECT id, label, user_id, created_at_ms, updated_at_ms, allowed_models_json \
             FROM api_keys WHERE deleted_at_ms IS NULL AND user_id = {} ORDER BY created_at_ms",
            placeholder(self.postgres(), 1)
        );
        Ok(fetch_all_decoded!(self, &sql, [user_id], decode_key))
    }

    async fn get_user(&self, id: &str) -> StoreResult<Option<UserRecord>> {
        let sql = format!(
            "SELECT id, username, is_admin, created_at_ms, updated_at_ms FROM users \
             WHERE id = {} AND deleted_at_ms IS NULL",
            placeholder(self.postgres(), 1)
        );
        let rows: Vec<UserRecord> = fetch_all_decoded!(self, &sql, [id], decode_user);
        Ok(rows.into_iter().next())
    }

    async fn activity_series(
        &self,
        since_ms: i64,
        bucket_ms: i64,
        limit: usize,
    ) -> StoreResult<Vec<ActivityBucket>> {
        let pg = self.postgres();
        let bucket_ms = bucket_ms.max(1);
        let sql = format!(
            "SELECT (created_at_ms / {b1}) * {b2} AS bucket_ms, user_id, virtual_key_id, \
             CAST(COUNT(*) AS BIGINT), \
             CAST(COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS BIGINT), \
             CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT), \
             CAST(COALESCE(SUM(cached_tokens), 0) AS BIGINT) \
             FROM requests WHERE created_at_ms >= {since} \
             GROUP BY 1, 2, 3 ORDER BY 1, 2, 3 LIMIT {limit}",
            b1 = placeholder(pg, 1),
            b2 = placeholder(pg, 2),
            since = placeholder(pg, 3),
            limit = placeholder(pg, 4)
        );
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        Ok(fetch_all_decoded!(
            self,
            &sql,
            [bucket_ms, bucket_ms, since_ms, limit],
            decode_activity
        ))
    }

    async fn api_key_auth_specs(&self) -> StoreResult<Vec<ApiKeyAuthSpec>> {
        Ok(fetch_all_decoded!(
            self,
            "SELECT id, label, user_id, secret, allowed_models_json FROM api_keys \
             WHERE deleted_at_ms IS NULL ORDER BY created_at_ms",
            [],
            decode_key_auth
        ))
    }

    async fn put_api_key(
        &self,
        id: &str,
        plaintext: &str,
        label: Option<&str>,
        user_id: Option<&str>,
        allowed_models: &[String],
        actor: &str,
    ) -> StoreResult<ApiKeyRecord> {
        if plaintext.is_empty() {
            return Err("API key must not be empty".to_string());
        }
        let now = now_ms();
        let digest = api_key_digest(plaintext);
        let allowed_json = encode_allowed_models(allowed_models);
        let pg = self.postgres();
        let sql = format!(
            "INSERT INTO api_keys (id, secret, label, user_id, created_at_ms, created_by, \
             updated_at_ms, updated_by, deleted_at_ms, allowed_models_json) VALUES ({}) \
             ON CONFLICT (id) DO UPDATE SET secret = excluded.secret, label = excluded.label, \
             user_id = excluded.user_id, updated_at_ms = excluded.updated_at_ms, \
             updated_by = excluded.updated_by, deleted_at_ms = NULL, \
             allowed_models_json = excluded.allowed_models_json",
            placeholders(pg, 10)
        );
        execute!(
            self,
            &sql,
            id,
            digest,
            label,
            user_id,
            now,
            actor,
            now,
            actor,
            Option::<i64>::None,
            allowed_json
        );
        let created_at_ms = match &self.pool {
            SqlPool::Sqlite(pool) => sqlx::query("SELECT created_at_ms FROM api_keys WHERE id = ?")
                .bind(id)
                .fetch_one(pool)
                .await
                .map_err(store_error)?
                .try_get(0)
                .map_err(store_error)?,
            SqlPool::Postgres(pool) => {
                sqlx::query("SELECT created_at_ms FROM api_keys WHERE id = $1")
                    .bind(id)
                    .fetch_one(pool)
                    .await
                    .map_err(store_error)?
                    .try_get(0)
                    .map_err(store_error)?
            }
        };
        Ok(ApiKeyRecord {
            id: id.to_string(),
            label: label.map(str::to_string),
            user_id: user_id.map(str::to_string),
            allowed_models: allowed_models.to_vec(),
            created_at_ms,
            updated_at_ms: now,
        })
    }

    async fn verify_api_key(&self, plaintext: &str) -> StoreResult<Option<ApiKeyRecord>> {
        if plaintext.is_empty() {
            return Ok(None);
        }
        macro_rules! scan {
            ($pool:expr) => {{
                let rows = sqlx::query(
                    "SELECT id, secret, label, user_id, created_at_ms, updated_at_ms, \
                     allowed_models_json FROM api_keys WHERE deleted_at_ms IS NULL",
                )
                .fetch_all($pool)
                .await
                .map_err(store_error)?;
                let mut found = None;
                for row in rows {
                    let secret: String = row.try_get(1).map_err(store_error)?;
                    if digest_matches(&secret, plaintext) {
                        found = Some(ApiKeyRecord {
                            id: row.try_get(0).map_err(store_error)?,
                            label: row.try_get(2).map_err(store_error)?,
                            user_id: row.try_get(3).map_err(store_error)?,
                            created_at_ms: row.try_get(4).map_err(store_error)?,
                            updated_at_ms: row.try_get(5).map_err(store_error)?,
                            allowed_models: parse_allowed_models(
                                row.try_get::<Option<String>, _>(6).map_err(store_error)?,
                            ),
                        });
                    }
                }
                Ok(found)
            }};
        }
        match &self.pool {
            SqlPool::Sqlite(pool) => scan!(pool),
            SqlPool::Postgres(pool) => scan!(pool),
        }
    }

    async fn delete_api_key(&self, id: &str, actor: &str) -> StoreResult<()> {
        let pg = self.postgres();
        let now = now_ms();
        let sql = format!(
            "UPDATE api_keys SET deleted_at_ms = {}, updated_at_ms = {}, updated_by = {} WHERE id = {}",
            placeholder(pg, 1),
            placeholder(pg, 2),
            placeholder(pg, 3),
            placeholder(pg, 4)
        );
        execute!(self, &sql, now, now, actor, id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Bounded, nonblocking hot-path writer
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum WriteCommand {
    Begin(Box<RequestRow>),
    Event(EventRow),
    Body(Box<BodyWrite>),
    Session(Box<crate::sessions::SessionRow>),
    Finish {
        request_id: String,
        finish: Box<RequestFinish>,
    },
    Flush(oneshot::Sender<()>),
}

impl WriteCommand {
    fn estimated_bytes(&self) -> u64 {
        match self {
            Self::Begin(row) => request_row_bytes(row),
            Self::Event(event) => event_row_bytes(event),
            Self::Body(body) => body_write_bytes(body),
            Self::Session(row) => session_row_bytes(row),
            Self::Finish { request_id, finish } => {
                string_bytes(request_id) + request_finish_bytes(finish)
            }
            Self::Flush(_) => 0,
        }
    }
}

struct QueuedCommand {
    command: WriteCommand,
    _reservation: BudgetReservation,
}

#[derive(Debug)]
struct QueueBudget {
    max_bytes: u64,
    reserved_bytes: AtomicU64,
}

impl QueueBudget {
    fn new(max_bytes: NonZeroU64) -> Self {
        Self {
            max_bytes: max_bytes.get(),
            reserved_bytes: AtomicU64::new(0),
        }
    }

    fn reserve(self: &Arc<Self>, bytes: u64) -> Result<BudgetReservation, EnqueueError> {
        if bytes == 0 {
            return Ok(BudgetReservation {
                budget: Arc::clone(self),
                bytes,
            });
        }
        if bytes > self.max_bytes {
            return Err(EnqueueError::Full);
        }
        let mut current = self.reserved_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return Err(EnqueueError::Full);
            };
            if next > self.max_bytes {
                return Err(EnqueueError::Full);
            }
            match self.reserved_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(BudgetReservation {
                        budget: Arc::clone(self),
                        bytes,
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

#[derive(Debug)]
struct BudgetReservation {
    budget: Arc<QueueBudget>,
    bytes: u64,
}

impl Drop for BudgetReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget
                .reserved_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

fn string_bytes(value: &String) -> u64 {
    value.capacity() as u64
}

fn optional_string_bytes(value: &Option<String>) -> u64 {
    value.as_ref().map_or(0, string_bytes)
}

fn request_row_bytes(row: &RequestRow) -> u64 {
    std::mem::size_of::<RequestRow>() as u64
        + string_bytes(&row.id)
        + optional_string_bytes(&row.response_id)
        + optional_string_bytes(&row.conversation_id)
        + optional_string_bytes(&row.virtual_key_id)
        + string_bytes(&row.client_protocol)
        + string_bytes(&row.client_model)
        + optional_string_bytes(&row.alias)
        + optional_string_bytes(&row.backend)
        + optional_string_bytes(&row.resolved_model)
        + string_bytes(&row.status)
        + optional_string_bytes(&row.harness)
        + optional_string_bytes(&row.harness_version)
        + optional_string_bytes(&row.harness_session_id)
        + optional_string_bytes(&row.harness_sub_session_id)
        + optional_string_bytes(&row.harness_parent_session_id)
        + optional_string_bytes(&row.session_kind)
        + optional_string_bytes(&row.session_id)
        + optional_string_bytes(&row.chain_parent_request_id)
        + optional_string_bytes(&row.divergence_kind)
        + optional_string_bytes(&row.client_label)
        + optional_string_bytes(&row.client_source)
        + optional_string_bytes(&row.user_id)
}

fn event_row_bytes(event: &EventRow) -> u64 {
    std::mem::size_of::<EventRow>() as u64 + event_row_dynamic_bytes(event)
}

fn event_row_dynamic_bytes(event: &EventRow) -> u64 {
    string_bytes(&event.request_id)
        + string_bytes(&event.hop)
        + string_bytes(&event.kind)
        + optional_string_bytes(&event.payload)
}

fn blob_row_dynamic_bytes(row: &BlobRow) -> u64 {
    string_bytes(&row.hash) + string_bytes(&row.media) + string_bytes(&row.content)
}

fn item_row_dynamic_bytes(row: &ItemRow) -> u64 {
    string_bytes(&row.request_id)
        + string_bytes(&row.hop)
        + string_bytes(&row.section)
        + optional_string_bytes(&row.kind)
        + string_bytes(&row.blob_hash)
        + optional_string_bytes(&row.identity_hash)
}

fn body_write_bytes(body: &BodyWrite) -> u64 {
    std::mem::size_of::<BodyWrite>() as u64
        + event_row_dynamic_bytes(&body.event)
        + (body.items.capacity() * std::mem::size_of::<ItemRow>()) as u64
        + body.items.iter().map(item_row_dynamic_bytes).sum::<u64>()
        + (body.blobs.capacity() * std::mem::size_of::<BlobRow>()) as u64
        + body.blobs.iter().map(blob_row_dynamic_bytes).sum::<u64>()
}

fn request_finish_bytes(finish: &RequestFinish) -> u64 {
    std::mem::size_of::<RequestFinish>() as u64
        + optional_string_bytes(&finish.response_id)
        + string_bytes(&finish.status)
        + optional_string_bytes(&finish.error)
        + optional_string_bytes(&finish.terminal_reason)
        + optional_string_bytes(&finish.backend)
        + optional_string_bytes(&finish.resolved_model)
        + optional_string_bytes(&finish.attempts_json)
        + optional_string_bytes(&finish.timings_json)
        + optional_string_bytes(&finish.client_label)
        + optional_string_bytes(&finish.client_source)
}

fn session_row_bytes(row: &crate::sessions::SessionRow) -> u64 {
    std::mem::size_of::<crate::sessions::SessionRow>() as u64
        + string_bytes(&row.id)
        + optional_string_bytes(&row.parent_id)
        + string_bytes(&row.kind)
        + string_bytes(&row.harness)
        + optional_string_bytes(&row.harness_version)
        + optional_string_bytes(&row.external_id)
        + optional_string_bytes(&row.session_kind)
        + optional_string_bytes(&row.client_label)
        + optional_string_bytes(&row.virtual_key_id)
        + optional_string_bytes(&row.user_id)
        + optional_string_bytes(&row.root_request_id)
        + optional_string_bytes(&row.spawned_by_request_id)
}

fn persistence_queue_max_bytes_from_env() -> NonZeroU64 {
    let parsed = std::env::var(PERSISTENCE_QUEUE_MAX_BYTES_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(NonZeroU64::new);
    parsed.unwrap_or_else(|| NonZeroU64::new(DEFAULT_PERSISTENCE_QUEUE_MAX_BYTES).unwrap())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueError {
    Full,
    Closed,
}

impl fmt::Display for EnqueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("persistence queue is full"),
            Self::Closed => formatter.write_str("persistence queue is closed"),
        }
    }
}

impl std::error::Error for EnqueueError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistenceQueueStats {
    pub accepted: u64,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    pub write_failures: u64,
    pub reserved_bytes: u64,
    pub max_bytes: u64,
}

#[derive(Default)]
struct QueueCounters {
    accepted: AtomicU64,
    dropped_full: AtomicU64,
    dropped_closed: AtomicU64,
    write_failures: AtomicU64,
}

/// Cloneable producer for a single FIFO writer task. The request path calls only
/// `try_*`; a full or stopped queue is reported immediately and never applies
/// backpressure to an SSE/body stream.
#[derive(Clone)]
pub struct PersistenceQueue {
    sender: mpsc::Sender<QueuedCommand>,
    counters: Arc<QueueCounters>,
    budget: Arc<QueueBudget>,
}

impl PersistenceQueue {
    pub fn spawn(store: Arc<dyn PersistenceWriter>, capacity: NonZeroUsize) -> Self {
        Self::spawn_with_max_bytes(store, capacity, persistence_queue_max_bytes_from_env())
    }

    pub(crate) fn spawn_with_max_bytes(
        store: Arc<dyn PersistenceWriter>,
        capacity: NonZeroUsize,
        max_bytes: NonZeroU64,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel(capacity.get());
        let counters = Arc::new(QueueCounters::default());
        let budget = Arc::new(QueueBudget::new(max_bytes));
        let worker_counters = Arc::clone(&counters);
        tokio::spawn(async move {
            const FAILURE_WARN_INTERVAL: Duration = Duration::from_secs(30);
            let mut last_failure_warning: Option<std::time::Instant> = None;
            let mut suppressed_failures = 0_u64;
            while let Some(queued) = receiver.recv().await {
                let QueuedCommand {
                    command,
                    _reservation,
                } = queued;
                let result = match command {
                    WriteCommand::Begin(row) => store.begin_request(*row).await,
                    WriteCommand::Event(event) => store.append_event(event).await,
                    WriteCommand::Body(body) => store.store_body(*body).await,
                    WriteCommand::Session(row) => store.upsert_session(*row).await,
                    WriteCommand::Finish { request_id, finish } => {
                        store.finish_request(&request_id, *finish).await
                    }
                    WriteCommand::Flush(done) => {
                        let _ = done.send(());
                        continue;
                    }
                };
                if let Err(error) = result {
                    worker_counters
                        .write_failures
                        .fetch_add(1, Ordering::Relaxed);
                    let should_warn = last_failure_warning
                        .is_none_or(|last| last.elapsed() >= FAILURE_WARN_INTERVAL);
                    if should_warn {
                        tracing::warn!(
                            error = %error,
                            suppressed_failures,
                            "asynchronous persistence write failed"
                        );
                        last_failure_warning = Some(std::time::Instant::now());
                        suppressed_failures = 0;
                    } else {
                        suppressed_failures = suppressed_failures.saturating_add(1);
                    }
                }
            }
            if suppressed_failures > 0 {
                tracing::warn!(
                    suppressed_failures,
                    "persistence writer stopped after additional coalesced failures"
                );
            }
        });
        Self {
            sender,
            counters,
            budget,
        }
    }

    pub fn try_begin(&self, row: RequestRow) -> Result<(), EnqueueError> {
        self.try_enqueue(WriteCommand::Begin(Box::new(row)))
    }

    pub fn try_event(&self, event: EventRow) -> Result<(), EnqueueError> {
        self.try_enqueue(WriteCommand::Event(event))
    }

    pub fn try_body(&self, body: BodyWrite) -> Result<(), EnqueueError> {
        self.try_enqueue(WriteCommand::Body(Box::new(body)))
    }

    pub fn try_session(&self, row: crate::sessions::SessionRow) -> Result<(), EnqueueError> {
        self.try_enqueue(WriteCommand::Session(Box::new(row)))
    }

    pub fn try_finish(
        &self,
        request_id: String,
        finish: RequestFinish,
    ) -> Result<(), EnqueueError> {
        self.try_enqueue(WriteCommand::Finish {
            request_id,
            finish: Box::new(finish),
        })
    }

    fn try_enqueue(&self, command: WriteCommand) -> Result<(), EnqueueError> {
        let reservation = match self.budget.reserve(command.estimated_bytes()) {
            Ok(reservation) => reservation,
            Err(error) => {
                self.counters.dropped_full.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        let queued = QueuedCommand {
            command,
            _reservation: reservation,
        };
        match self.sender.try_send(queued) {
            Ok(()) => {
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.counters.dropped_full.fetch_add(1, Ordering::Relaxed);
                Err(EnqueueError::Full)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.counters.dropped_closed.fetch_add(1, Ordering::Relaxed);
                Err(EnqueueError::Closed)
            }
        }
    }

    /// Wait until every command accepted before this call has been processed.
    /// This is a shutdown/test seam and must not be used on a streaming path.
    pub async fn flush(&self) -> StoreResult<()> {
        let (done_tx, done_rx) = oneshot::channel();
        let reservation = self
            .budget
            .reserve(0)
            .map_err(|_| "persistence queue memory budget is exhausted".to_string())?;
        self.sender
            .send(QueuedCommand {
                command: WriteCommand::Flush(done_tx),
                _reservation: reservation,
            })
            .await
            .map_err(|_| "persistence queue is closed".to_string())?;
        done_rx
            .await
            .map_err(|_| "persistence writer stopped before flush".to_string())
    }

    pub fn stats(&self) -> PersistenceQueueStats {
        PersistenceQueueStats {
            accepted: self.counters.accepted.load(Ordering::Relaxed),
            dropped_full: self.counters.dropped_full.load(Ordering::Relaxed),
            dropped_closed: self.counters.dropped_closed.load(Ordering::Relaxed),
            write_failures: self.counters.write_failures.load(Ordering::Relaxed),
            reserved_bytes: self.budget.reserved_bytes.load(Ordering::Relaxed),
            max_bytes: self.budget.max_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::sync::Notify;

    fn request(id: &str) -> RequestRow {
        RequestRow {
            id: id.to_string(),
            response_id: None,
            conversation_id: Some("conversation-1".to_string()),
            virtual_key_id: Some("key-1".to_string()),
            client_protocol: "responses".to_string(),
            client_model: "public-model".to_string(),
            alias: Some("public-model".to_string()),
            backend: None,
            resolved_model: None,
            status: "running".to_string(),
            created_at_ms: 100,
            ..RequestRow::default()
        }
    }

    fn finish() -> RequestFinish {
        RequestFinish {
            response_id: Some("resp-1".to_string()),
            status: "completed".to_string(),
            completed_at_ms: 200,
            first_token_at_ms: Some(130),
            input_tokens: Some(10),
            output_tokens: Some(5),
            cached_tokens: Some(2),
            reasoning_tokens: Some(3),
            terminal_reason: Some("stop".to_string()),
            backend: Some("winner".to_string()),
            resolved_model: Some("served-model".to_string()),
            attempts_json: Some(r#"[{"status":"served"}]"#.to_string()),
            timings_json: Some(r#"{"total_ms":100}"#.to_string()),
            client_label: Some("key-abcd".to_string()),
            client_source: Some("key_hash".to_string()),
            ..RequestFinish::default()
        }
    }

    fn event_with_payload(request_id: &str, payload_len: usize) -> EventRow {
        EventRow {
            request_id: request_id.to_string(),
            seq: 1,
            ts_ms: 1,
            hop: "client_in".to_string(),
            kind: "body".to_string(),
            payload: Some("x".repeat(payload_len)),
            bytes: Some(payload_len as i64),
        }
    }

    #[tokio::test]
    async fn sqlite_migrations_and_terminal_round_trip() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        store
            .begin_request(request("api-call-1"))
            .await
            .expect("begin");
        store
            .append_event(EventRow {
                request_id: "api-call-1".to_string(),
                seq: 2,
                ts_ms: 150,
                hop: "upstream_in".to_string(),
                kind: "body".to_string(),
                payload: Some(r#"{"safe":true}"#.to_string()),
                bytes: Some(13),
            })
            .await
            .expect("event");
        store
            .finish_request("api-call-1", finish())
            .await
            .expect("finish");

        let got = store
            .get_request("api-call-1")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.id, "api-call-1");
        assert_eq!(got.response_id.as_deref(), Some("resp-1"));
        assert_eq!(got.backend.as_deref(), Some("winner"));
        assert_eq!(got.resolved_model.as_deref(), Some("served-model"));
        assert_eq!(got.reasoning_tokens, Some(3));
        assert_eq!(got.client_label.as_deref(), Some("key-abcd"));
        assert_eq!(
            got.attempts_json.as_deref(),
            Some(r#"[{"status":"served"}]"#)
        );
        assert_eq!(store.request_events("api-call-1").await.unwrap().len(), 1);

        let usage = store
            .usage_summary(&UsageFilter::default())
            .await
            .expect("usage");
        assert_eq!(usage[0].reasoning_tokens, 3);
        assert_eq!(usage[0].backend.as_deref(), Some("winner"));

        let migration_versions: Vec<i64> = match &store.pool {
            SqlPool::Sqlite(pool) => sqlx::query(
                "SELECT version FROM _sqlx_migrations WHERE success = 1 ORDER BY version",
            )
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get(0).unwrap())
            .collect(),
            SqlPool::Postgres(_) => unreachable!(),
        };
        assert_eq!(
            migration_versions,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        let request_indexes: Vec<String> = match &store.pool {
            SqlPool::Sqlite(pool) => sqlx::query(
                "SELECT name FROM sqlite_master WHERE type = 'index' \
                 AND name IN ('idx_requests_created_at_ms', \
                              'idx_requests_virtual_key_created_at_ms') ORDER BY name",
            )
            .fetch_all(pool)
            .await
            .expect("request indexes")
            .into_iter()
            .map(|row| row.try_get(0).expect("index name"))
            .collect(),
            SqlPool::Postgres(_) => unreachable!(),
        };
        assert_eq!(
            request_indexes,
            [
                "idx_requests_created_at_ms".to_string(),
                "idx_requests_virtual_key_created_at_ms".to_string(),
            ]
        );
    }

    fn blob(hash: &str, content: &str) -> BlobRow {
        BlobRow {
            hash: hash.to_string(),
            media: "json".to_string(),
            size: content.len() as i64,
            content: content.to_string(),
            created_at_ms: 100,
        }
    }

    fn item(request_id: &str, hop: &str, ordinal: i64, hash: &str) -> ItemRow {
        ItemRow {
            identity_hash: None,
            request_id: request_id.to_string(),
            hop: hop.to_string(),
            ordinal,
            section: "message".to_string(),
            kind: Some("user".to_string()),
            blob_hash: hash.to_string(),
        }
    }

    fn skeleton_event(request_id: &str, seq: i64, ts_ms: i64) -> EventRow {
        EventRow {
            request_id: request_id.to_string(),
            seq,
            ts_ms,
            hop: if seq == 1 {
                "client_in"
            } else {
                "upstream_out"
            }
            .to_string(),
            kind: "request".to_string(),
            payload: Some(r#"{"content":"{}"}"#.to_string()),
            bytes: Some(2),
        }
    }

    fn session(
        id: &str,
        parent: Option<&str>,
        external: Option<&str>,
        seen: i64,
    ) -> crate::sessions::SessionRow {
        crate::sessions::SessionRow {
            id: id.to_string(),
            parent_id: parent.map(str::to_string),
            kind: if external.is_some() {
                "declared"
            } else {
                "inferred"
            }
            .to_string(),
            harness: "claude-code".to_string(),
            harness_version: None,
            external_id: external.map(str::to_string),
            session_kind: None,
            client_label: Some("key-abc".to_string()),
            virtual_key_id: None,
            user_id: None,
            depth: i64::from(parent.is_some()),
            root_request_id: None,
            spawned_by_request_id: None,
            first_seen_ms: seen,
            last_seen_ms: seen,
            request_count: 1,
        }
    }

    #[tokio::test]
    async fn sqlite_sessions_upsert_list_children_requests_and_chain_head() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        store
            .upsert_session(session("root", None, Some("s-1"), 100))
            .await
            .unwrap();
        store
            .upsert_session(session("child", Some("root"), None, 110))
            .await
            .unwrap();
        store
            .upsert_session(session("grand", Some("child"), Some("t-3"), 120))
            .await
            .unwrap();
        // Re-upserting keeps the first-seen root request and takes the greater counters.
        let mut again = session("root", None, Some("s-1"), 90);
        again.request_count = 3;
        again.root_request_id = Some("r-first".to_string());
        store.upsert_session(again).await.unwrap();
        let mut later = session("root", None, Some("s-1"), 200);
        later.request_count = 2;
        later.root_request_id = Some("r-other".to_string());
        store.upsert_session(later).await.unwrap();
        let root = store.get_session("root").await.unwrap().unwrap();
        assert_eq!(root.request_count, 3);
        assert_eq!(root.last_seen_ms, 200);
        assert_eq!(root.root_request_id.as_deref(), Some("r-first"));
        // The owner column round-trips and the first writer wins on conflict.
        assert_eq!(root.user_id.as_deref(), None);
        let mut owned = session("root", None, Some("s-1"), 210);
        owned.user_id = Some("user-7".to_string());
        store.upsert_session(owned).await.unwrap();
        assert_eq!(
            store
                .get_session("root")
                .await
                .unwrap()
                .unwrap()
                .user_id
                .as_deref(),
            Some("user-7")
        );

        assert_eq!(
            store
                .find_session("claude-code", Some("s-1"), None)
                .await
                .unwrap()
                .map(|row| row.id),
            Some("root".to_string())
        );
        assert!(
            store
                .find_session("claude-code", Some("nope"), None)
                .await
                .unwrap()
                .is_none()
        );
        store
            .upsert_session(session("bucket", None, None, 130))
            .await
            .unwrap();
        assert_eq!(
            store
                .find_session("claude-code", None, Some("key-abc"))
                .await
                .unwrap()
                .map(|row| row.id),
            Some("bucket".to_string())
        );

        let roots = store.list_sessions(0, true, 10).await.unwrap();
        assert_eq!(
            roots.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["root", "bucket"]
        );
        let all = store.list_sessions(115, false, 10).await.unwrap();
        assert_eq!(
            all.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["root", "bucket", "grand"]
        );
        let children = store.session_children("root").await.unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].id, "child");
        let descendants = store.session_descendants("root", 10).await.unwrap();
        assert_eq!(
            descendants
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["child", "grand"]
        );

        // Requests of a node, and the chain head built from the newest one.
        let mut first = request("req-1");
        first.session_id = Some("root".to_string());
        first.created_at_ms = 100;
        let mut second = request("req-2");
        second.session_id = Some("root".to_string());
        second.created_at_ms = 200;
        second.chain_parent_request_id = Some("req-1".to_string());
        second.divergence_kind = Some("append".to_string());
        second.cache_bust = Some(false);
        second.item_count = Some(2);
        first.user_id = Some("user-7".to_string());
        store.begin_request(first).await.unwrap();
        store.begin_request(second).await.unwrap();
        // Terminal usage of the second request feeds the session aggregate.
        let mut done = finish();
        done.input_tokens = Some(100);
        done.output_tokens = Some(40);
        done.cached_tokens = Some(60);
        done.reasoning_tokens = None; // unreported class stays `None`, never 0
        store.finish_request("req-2", done).await.unwrap();
        let aggregate = store.session_aggregate("root").await.unwrap().unwrap();
        assert_eq!(aggregate.input_tokens, Some(100));
        assert_eq!(aggregate.output_tokens, Some(40));
        assert_eq!(aggregate.cached_tokens, Some(60));
        assert_eq!(aggregate.reasoning_tokens, None);
        store
            .store_body(BodyWrite {
                event: skeleton_event("req-2", 1, 200),
                items: vec![
                    item("req-2", "client_in", 0, "aa"),
                    item("req-2", "client_in", 1, "bb"),
                ],
                blobs: vec![blob("aa", "1"), blob("bb", "2")],
            })
            .await
            .unwrap();
        let requests = store.session_requests("root", 10).await.unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["req-1", "req-2"]
        );
        assert_eq!(
            requests[1].chain_parent_request_id.as_deref(),
            Some("req-1")
        );
        assert_eq!(requests[1].divergence_kind.as_deref(), Some("append"));
        assert_eq!(requests[1].cache_bust, Some(false));
        let head = store.chain_head("root").await.unwrap().unwrap();
        assert_eq!(head.request_id, "req-2");
        assert_eq!(
            head.items
                .iter()
                .map(|item| item.hash.as_str())
                .collect::<Vec<_>>(),
            ["aa", "bb"]
        );
        assert!(store.chain_head("child").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn throughput_series_buckets_per_model_and_only_counts_reported_classes() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        let mut rows = Vec::new();
        for (id, model, created, first, done, input, output, cached) in [
            (
                "a",
                "m1",
                1_000,
                Some(1_400),
                Some(3_400),
                Some(1_000),
                Some(200),
                Some(800),
            ),
            (
                "b",
                "m1",
                2_000,
                Some(2_200),
                Some(2_700),
                Some(500),
                Some(50),
                None,
            ),
            ("c", "m1", 61_000, None, None, Some(100), None, None), // running, no first token
            (
                "d",
                "m2",
                61_500,
                Some(61_600),
                Some(62_600),
                None,
                Some(400),
                None,
            ),
        ] {
            let mut row = request(id);
            row.client_model = model.to_string();
            row.created_at_ms = created;
            rows.push((row, first, done, input, output, cached));
        }
        for (row, first, done, input, output, cached) in rows {
            let id = row.id.clone();
            store.begin_request(row).await.unwrap();
            if let Some(done) = done {
                let mut done_row = finish();
                done_row.first_token_at_ms = first;
                done_row.completed_at_ms = done;
                done_row.input_tokens = input;
                done_row.output_tokens = output;
                done_row.cached_tokens = cached;
                done_row.resolved_model = None;
                done_row.backend = Some("vllm-a".to_string());
                store.finish_request(&id, done_row).await.unwrap();
            }
        }
        let series = store.throughput_series(0, 60_000, 100).await.unwrap();
        assert_eq!(
            series
                .iter()
                .map(|b| (b.bucket_ms, b.model.as_str(), b.backend.as_deref()))
                .collect::<Vec<_>>(),
            [
                (0, "m1", Some("vllm-a")),
                (60_000, "m1", None),
                (60_000, "m2", Some("vllm-a"))
            ]
        );
        let first = &series[0];
        assert_eq!(first.requests, 2);
        assert_eq!(first.completed, 2);
        assert_eq!(first.input_tokens, 1_500);
        assert_eq!(first.output_tokens, 250);
        assert_eq!(first.cached_tokens, 800);
        assert_eq!(first.ttft_ms_sum, 400 + 200);
        assert_eq!(first.ttft_count, 2);
        assert_eq!(first.prefill_tokens, 1_500);
        assert_eq!(first.decode_ms_sum, 2_000 + 500);
        assert_eq!(first.decode_tokens, 250);
        assert_eq!(first.decode_count, 2);
        // The running request contributes no latency figures.
        let running = &series[1];
        assert_eq!(running.requests, 1);
        assert_eq!(running.ttft_count, 0);
        assert_eq!(running.decode_count, 0);
        // A row without input tokens adds nothing to prefill even with a first token.
        let m2 = &series[2];
        assert_eq!(m2.prefill_tokens, 0);
        assert_eq!(m2.decode_tokens, 400);
        assert!(
            store
                .throughput_series(100_000, 60_000, 100)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.throughput_series(0, 60_000, 1).await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn keys_carry_allowed_models_and_owner_and_list_per_user() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        let user = store
            .create_user("koen", "$argon2id$hash", true, "test")
            .await
            .unwrap();
        assert_eq!(
            store.get_user(&user.id).await.unwrap().unwrap().username,
            "koen"
        );
        assert!(store.get_user("nope").await.unwrap().is_none());
        store
            .put_api_key(
                "k-scoped",
                "llmc_a",
                Some("laptop"),
                Some(&user.id),
                &["local".to_string(), "fast".to_string()],
                "test",
            )
            .await
            .unwrap();
        store
            .put_api_key("k-any", "llmc_b", None, None, &[], "test")
            .await
            .unwrap();
        let mine = store.list_api_keys_for_user(&user.id).await.unwrap();
        assert_eq!(mine.len(), 1);
        assert_eq!(mine[0].allowed_models, ["local", "fast"]);
        let specs = store.api_key_auth_specs().await.unwrap();
        let scoped = specs.iter().find(|s| s.id == "k-scoped").unwrap();
        assert_eq!(scoped.allowed_models, ["local", "fast"]);
        assert_eq!(scoped.user_id.as_deref(), Some(user.id.as_str()));
        assert!(
            specs
                .iter()
                .find(|s| s.id == "k-any")
                .unwrap()
                .allowed_models
                .is_empty()
        );
        let verified = store.verify_api_key("llmc_a").await.unwrap().unwrap();
        assert_eq!(verified.allowed_models, ["local", "fast"]);
        store.delete_api_key("k-scoped", "test").await.unwrap();
        assert!(
            store
                .list_api_keys_for_user(&user.id)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn external_users_are_upserted_by_stable_id_for_key_ownership() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        let first = store
            .upsert_external_user("github:123", "github:octocat", false, "github-sso")
            .await
            .expect("upsert github user");
        assert_eq!(first.id, "github:123");
        assert_eq!(first.username, "github:octocat");
        assert!(!first.is_admin);
        assert!(
            store
                .get_user_auth("github:octocat")
                .await
                .unwrap()
                .is_some_and(|auth| !crate::accounts::verify_password(
                    &auth.password_hash,
                    "any-password"
                )),
            "external users must not authenticate through password login"
        );

        store
            .put_api_key(
                "github-key",
                "llmc_github",
                Some("scoped"),
                Some(&first.id),
                &["qwen".to_string()],
                "test",
            )
            .await
            .expect("key");
        let second = store
            .upsert_external_user("github:123", "github:octo-renamed", true, "github-sso")
            .await
            .expect("refresh github user");
        assert_eq!(second.id, first.id);
        assert_eq!(second.username, "github:octo-renamed");
        assert!(second.is_admin);
        let keys = store.list_api_keys_for_user("github:123").await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].id, "github-key");
    }

    #[tokio::test]
    async fn requests_carry_user_id_and_activity_buckets_per_user_and_key() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        for (id, user, key, created, status, input) in [
            ("a", Some("u1"), Some("k1"), 1_000, "completed", Some(100)),
            ("b", Some("u1"), Some("k1"), 2_000, "failed", Some(50)),
            ("c", Some("u2"), Some("k2"), 61_000, "completed", None),
            ("d", None, None, 61_500, "completed", Some(10)),
        ] {
            let mut row = request(id);
            row.user_id = user.map(str::to_string);
            row.virtual_key_id = key.map(str::to_string);
            row.created_at_ms = created;
            row.status = status.to_string();
            store.begin_request(row).await.unwrap();
            if let Some(input) = input {
                let mut done = finish();
                done.status = status.to_string();
                done.input_tokens = Some(input);
                done.output_tokens = Some(5);
                done.cached_tokens = None;
                store.finish_request(id, done).await.unwrap();
            }
        }
        assert_eq!(
            store
                .get_request("a")
                .await
                .unwrap()
                .unwrap()
                .user_id
                .as_deref(),
            Some("u1")
        );
        let usage = store
            .usage_summary(&UsageFilter {
                user_id: Some("u1".to_string()),
                ..UsageFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].user_id.as_deref(), Some("u1"));
        assert_eq!(usage[0].requests, 2);
        let series = store.activity_series(0, 60_000, 100).await.unwrap();
        assert_eq!(
            series
                .iter()
                .map(|b| (
                    b.bucket_ms,
                    b.user_id.as_deref(),
                    b.virtual_key_id.as_deref()
                ))
                .collect::<Vec<_>>(),
            [
                (0, Some("u1"), Some("k1")),
                (60_000, None, None),
                (60_000, Some("u2"), Some("k2"))
            ]
        );
        assert_eq!(series[0].requests, 2);
        assert_eq!(series[0].failed, 1);
        assert_eq!(series[0].input_tokens, 150);
        assert_eq!(
            series[2].input_tokens, 0,
            "unreported input sums to 0 with 1 request"
        );
    }

    #[tokio::test]
    async fn finish_keeps_ingress_client_attribution() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        let mut row = request("req-1");
        row.client_label = Some("key-ingress".to_string());
        row.client_source = Some("key_hash".to_string());
        store.begin_request(row).await.unwrap();
        let mut done = finish();
        done.client_label = None;
        done.client_source = None;
        store.finish_request("req-1", done).await.unwrap();
        let summary = store.get_request("req-1").await.unwrap().unwrap();
        assert_eq!(summary.client_label.as_deref(), Some("key-ingress"));
        assert_eq!(summary.client_source.as_deref(), Some("key_hash"));
        assert_eq!(summary.status, "completed");
    }

    #[tokio::test]
    async fn sqlite_body_store_dedups_blobs_and_reads_items_in_order() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        store.begin_request(request("req-1")).await.expect("begin");
        store
            .store_body(BodyWrite {
                event: skeleton_event("req-1", 1, 150),
                items: vec![
                    item("req-1", "client_in", 0, "aa"),
                    item("req-1", "client_in", 1, "bb"),
                    item("req-1", "client_in", 2, "aa"),
                ],
                blobs: vec![blob("aa", "\"same\""), blob("bb", "\"other\"")],
            })
            .await
            .expect("store body");
        // A second request sharing blob `aa` must not fail on the existing row.
        store.begin_request(request("req-2")).await.expect("begin");
        store
            .store_body(BodyWrite {
                event: skeleton_event("req-2", 1, 160),
                items: vec![item("req-2", "client_in", 0, "aa")],
                blobs: vec![blob("aa", "\"same\"")],
            })
            .await
            .expect("store body again");

        let items = store
            .request_items("req-1", "client_in")
            .await
            .expect("items");
        assert_eq!(
            items.iter().map(|item| item.ordinal).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(items[2].blob_hash, "aa");
        assert!(
            store
                .request_items("req-1", "upstream_out")
                .await
                .unwrap()
                .is_empty()
        );
        let mut blobs = store
            .get_blobs(&["aa".to_string(), "bb".to_string(), "zz".to_string()])
            .await
            .expect("blobs");
        blobs.sort_by(|left, right| left.hash.cmp(&right.hash));
        assert_eq!(
            blobs
                .iter()
                .map(|blob| (blob.hash.as_str(), blob.content.as_str()))
                .collect::<Vec<_>>(),
            [("aa", "\"same\""), ("bb", "\"other\"")]
        );
        // The skeleton event landed in request_events with the same seq contract.
        let events = store.request_events("req-1").await.expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 1);
        assert_eq!(events[0].hop, "client_in");
    }

    #[tokio::test]
    async fn sqlite_body_store_replaces_a_re_staged_hop_and_prunes_orphans() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        store.begin_request(request("req-1")).await.expect("begin");
        store
            .store_body(BodyWrite {
                event: skeleton_event("req-1", 2, 150),
                items: vec![item("req-1", "upstream_out", 0, "old")],
                blobs: vec![blob("old", "1")],
            })
            .await
            .unwrap();
        // Final-attempt-wins: the same (request, hop, ordinal) is overwritten.
        store
            .store_body(BodyWrite {
                event: skeleton_event("req-1", 2, 151),
                items: vec![item("req-1", "upstream_out", 0, "new")],
                blobs: vec![blob("new", "2")],
            })
            .await
            .unwrap();
        let items = store.request_items("req-1", "upstream_out").await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].blob_hash, "new");
        // `old` is now unreferenced and pruned; `new` survives.
        assert_eq!(store.prune_orphan_blobs().await.unwrap(), 1);
        let remaining = store
            .get_blobs(&["old".to_string(), "new".to_string()])
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].hash, "new");
        assert_eq!(store.prune_orphan_blobs().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn request_history_pruning_removes_items_and_frees_their_blobs() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        let mut old = request("old");
        old.created_at_ms = 100;
        let mut recent = request("recent");
        recent.created_at_ms = 500;
        store.begin_request(old).await.unwrap();
        store.begin_request(recent).await.unwrap();
        store
            .store_body(BodyWrite {
                event: skeleton_event("old", 1, 100),
                items: vec![
                    item("old", "client_in", 0, "shared"),
                    item("old", "client_in", 1, "only-old"),
                ],
                blobs: vec![blob("shared", "s"), blob("only-old", "o")],
            })
            .await
            .unwrap();
        store
            .store_body(BodyWrite {
                event: skeleton_event("recent", 1, 500),
                items: vec![item("recent", "client_in", 0, "shared")],
                blobs: vec![blob("shared", "s")],
            })
            .await
            .unwrap();
        store.prune_request_history(300).await.unwrap();
        assert!(
            store
                .request_items("old", "client_in")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .request_items("recent", "client_in")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store.prune_orphan_blobs().await.unwrap(),
            1,
            "only-old is freed"
        );
        let blobs = store
            .get_blobs(&["shared".to_string(), "only-old".to_string()])
            .await
            .unwrap();
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].hash, "shared");
    }

    #[tokio::test]
    async fn terminal_upsert_recovers_when_begin_was_dropped() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        store
            .finish_request("missing-begin", finish())
            .await
            .expect("terminal upsert");
        let request = store
            .get_request("missing-begin")
            .await
            .expect("lookup")
            .expect("terminal row");
        assert_eq!(request.status, "completed");
        assert_eq!(request.created_at_ms, 200);
        assert_eq!(request.completed_at_ms, Some(200));
        assert_eq!(request.client_protocol, "unknown");
        assert_eq!(request.client_model, "");
    }

    #[tokio::test]
    async fn request_history_pruning_removes_old_events_and_retains_boundary() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        for (id, created_at_ms) in [("old", 99_i64), ("boundary", 100), ("new", 101)] {
            let mut row = request(id);
            row.created_at_ms = created_at_ms;
            store.begin_request(row).await.expect("request");
            store
                .append_event(EventRow {
                    request_id: id.to_string(),
                    seq: 1,
                    ts_ms: created_at_ms,
                    hop: "client_in".to_string(),
                    kind: "body".to_string(),
                    payload: None,
                    bytes: None,
                })
                .await
                .expect("event");
        }

        store
            .prune_request_history(100)
            .await
            .expect("prune old history");
        assert!(
            store
                .get_request("old")
                .await
                .expect("old lookup")
                .is_none()
        );
        assert!(
            store
                .request_events("old")
                .await
                .expect("old events")
                .is_empty()
        );
        assert!(
            store
                .get_request("boundary")
                .await
                .expect("boundary lookup")
                .is_some(),
            "the cutoff is exclusive"
        );
        assert_eq!(
            store
                .request_events("boundary")
                .await
                .expect("boundary events")
                .len(),
            1
        );
        assert!(
            store
                .get_request("new")
                .await
                .expect("new lookup")
                .is_some()
        );

        let orphan_events: i64 = match &store.pool {
            SqlPool::Sqlite(pool) => sqlx::query(
                "SELECT COUNT(*) FROM request_events AS event \
                 LEFT JOIN requests AS request ON request.id = event.request_id \
                 WHERE request.id IS NULL",
            )
            .fetch_one(pool)
            .await
            .expect("orphan count")
            .try_get(0)
            .expect("count"),
            SqlPool::Postgres(_) => unreachable!(),
        };
        assert_eq!(orphan_events, 0);

        store
            .prune_request_history(101)
            .await
            .expect("prune boundary row");
        assert!(
            store
                .get_request("boundary")
                .await
                .expect("boundary after later cutoff")
                .is_none()
        );
        assert!(
            store
                .get_request("new")
                .await
                .expect("new retained")
                .is_some()
        );
    }

    #[tokio::test]
    async fn repeated_event_sequence_is_last_writer_wins() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        store
            .append_event(EventRow {
                request_id: "retry".to_string(),
                seq: 2,
                ts_ms: 10,
                hop: "upstream_out".to_string(),
                kind: "attempt".to_string(),
                payload: Some(r#"{"attempt":1}"#.to_string()),
                bytes: Some(13),
            })
            .await
            .expect("first event");
        store
            .append_event(EventRow {
                request_id: "retry".to_string(),
                seq: 2,
                ts_ms: 20,
                hop: "upstream_out".to_string(),
                kind: "terminal_attempt".to_string(),
                payload: Some(r#"{"attempt":2}"#.to_string()),
                bytes: Some(14),
            })
            .await
            .expect("replacement event");

        let events = store.request_events("retry").await.expect("read events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].ts_ms, 20);
        assert_eq!(events[0].kind, "terminal_attempt");
        assert_eq!(events[0].payload.as_deref(), Some(r#"{"attempt":2}"#));
        assert_eq!(events[0].bytes, Some(14));
    }

    #[test]
    fn legacy_migration_bytes_remain_compatible() {
        let fixtures = [
            (
                include_bytes!("../migrations/0001_baseline.sql").as_slice(),
                "4a31c23a430907f123abfef6fd9eaa0dba182cdcdf011dfd245a73677cbbe1f2",
            ),
            (
                include_bytes!("../migrations/0002_relational_config.sql").as_slice(),
                "045ddec0689ebcb61d94134d08ff62b436356071db816fb82738bde02bea9c7b",
            ),
            (
                include_bytes!("../migrations/0003_users.sql").as_slice(),
                "da9c0ae00eba35e81a0a3d5bde9452c0f48d4b8102c2997f9e8e5253a2707d4a",
            ),
            (
                include_bytes!("../migrations/0004_api_key_owner.sql").as_slice(),
                "a53c96f937157966e74b4438acdee2b4ef89eb6e49876d791105428c31b46aef",
            ),
        ];
        for (bytes, expected) in fixtures {
            assert_eq!(hex::encode(Sha256::digest(bytes)), expected);
        }
    }

    #[tokio::test]
    async fn api_keys_are_hashed_and_verifiable_without_plaintext_exposure() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        store
            .put_api_key("key-1", "sk-super-secret", Some("team"), None, &[], "test")
            .await
            .expect("put");

        assert_eq!(
            store
                .verify_api_key("sk-super-secret")
                .await
                .expect("verify")
                .expect("match")
                .id,
            "key-1"
        );
        assert!(
            store
                .verify_api_key("wrong")
                .await
                .expect("verify")
                .is_none()
        );

        let specs = store.api_key_auth_specs().await.expect("specs");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].secret_hash, api_key_digest("sk-super-secret"));
        assert!(!specs[0].secret_hash.contains("super-secret"));
        assert!(!format!("{:?}", specs[0]).contains(&specs[0].secret_hash));

        let stored: String = match &store.pool {
            SqlPool::Sqlite(pool) => sqlx::query("SELECT secret FROM api_keys WHERE id = ?")
                .bind("key-1")
                .fetch_one(pool)
                .await
                .unwrap()
                .try_get(0)
                .unwrap(),
            SqlPool::Postgres(_) => unreachable!(),
        };
        assert!(stored.starts_with(API_KEY_HASH_PREFIX));
        assert_ne!(stored, "sk-super-secret");

        store.delete_api_key("key-1", "test").await.expect("delete");
        assert!(
            store
                .verify_api_key("sk-super-secret")
                .await
                .expect("verify deleted")
                .is_none()
        );
    }

    #[tokio::test]
    async fn sqlite_settings_metrics_and_users_crud() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");

        store.set_setting("mode", "one").await.expect("set");
        store.set_setting("mode", "two").await.expect("upsert");
        assert_eq!(
            store.get_setting("mode").await.unwrap().as_deref(),
            Some("two")
        );
        store.delete_setting("mode").await.expect("delete setting");
        assert_eq!(store.get_setting("mode").await.unwrap(), None);

        for (backend, ts_ms) in [("a", 100), ("b", 200)] {
            store
                .record_backend_metrics(MetricSample {
                    backend: backend.to_string(),
                    ts_ms,
                    data: format!(r#"{{"backend":"{backend}"}}"#),
                })
                .await
                .expect("metric");
        }
        let recent = store.backend_metrics_history(150).await.expect("history");
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].backend, "b");
        let bounded = store
            .backend_metrics_history_limited(0, 1)
            .await
            .expect("bounded history");
        assert_eq!(bounded.len(), 1);
        assert_eq!(bounded[0].backend, "b", "the newest sample is retained");
        assert!(
            store
                .backend_metrics_history_limited(0, 0)
                .await
                .expect("zero limit")
                .is_empty()
        );
        store.prune_backend_metrics(201).await.expect("prune");
        assert!(store.backend_metrics_history(0).await.unwrap().is_empty());

        let user = store
            .create_user("alice", "argon2-hash", true, "test")
            .await
            .expect("create user");
        assert_eq!(store.count_users().await.unwrap(), 1);
        assert_eq!(
            store
                .get_user_auth("alice")
                .await
                .unwrap()
                .unwrap()
                .password_hash,
            "argon2-hash"
        );
        store
            .update_user(&user.id, Some("new-hash"), Some(false), "test")
            .await
            .expect("update user");
        let auth = store.get_user_auth("alice").await.unwrap().unwrap();
        assert_eq!(auth.password_hash, "new-hash");
        assert!(!auth.is_admin);
        assert_eq!(store.list_users().await.unwrap().len(), 1);
        store
            .delete_user(&user.id, "test")
            .await
            .expect("delete user");
        assert_eq!(store.count_users().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn legacy_plaintext_api_key_is_hashed_in_memory_without_disk_rewrite() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-store-{}.sqlite",
            uuid::Uuid::new_v4().simple()
        ));
        let url = format!("sqlite://{}", path.display());
        let store = SqlStore::connect_sqlite(&url).await.expect("first connect");
        let now = now_ms();
        if let SqlPool::Sqlite(pool) = &store.pool {
            sqlx::query(
                "INSERT INTO api_keys (id, secret, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?)",
            )
            .bind("legacy")
            .bind("legacy-plaintext")
            .bind(now)
            .bind(now)
            .execute(pool)
            .await
            .expect("legacy insert");
        }
        store.close().await;

        let reopened = SqlStore::connect_sqlite(&url).await.expect("reconnect");
        assert_eq!(
            reopened
                .verify_api_key("legacy-plaintext")
                .await
                .expect("verify")
                .expect("match")
                .id,
            "legacy"
        );
        let spec = reopened.api_key_auth_specs().await.unwrap().pop().unwrap();
        assert_eq!(spec.secret_hash, api_key_digest("legacy-plaintext"));
        let stored: String = match &reopened.pool {
            SqlPool::Sqlite(pool) => sqlx::query("SELECT secret FROM api_keys WHERE id = ?")
                .bind("legacy")
                .fetch_one(pool)
                .await
                .expect("stored legacy row")
                .try_get(0)
                .expect("legacy secret"),
            SqlPool::Postgres(_) => unreachable!(),
        };
        assert_eq!(
            stored, "legacy-plaintext",
            "startup must remain rollback-safe"
        );
        reopened.close().await;
        let _ = std::fs::remove_file(path);
    }

    fn sqlite_pool(store: &SqlStore) -> &sqlx::SqlitePool {
        match &store.pool {
            SqlPool::Sqlite(pool) => pool,
            SqlPool::Postgres(_) => unreachable!(),
        }
    }

    #[tokio::test]
    async fn legacy_operational_document_is_exact_and_takes_database_precedence() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        let document = concat!(
            "  {\n",
            "    \"backends\": {\"legacy\": {\"base_url\": ",
            "\"https://legacy.example/v1\", \"api_key\": \"blob-secret\"}}\n",
            "  }  \n"
        );
        store
            .set_setting("operational", document)
            .await
            .expect("store document");

        // This invalid relational id proves the document wins before relational
        // decoding; startup will parse/fail the returned document itself.
        sqlx::query(
            "INSERT INTO backends \
             (id, name, base_url, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?)",
        )
        .bind("not-a-uuid")
        .bind("shadowed")
        .bind("https://shadowed.example/v1")
        .bind(1_i64)
        .bind(1_i64)
        .execute(sqlite_pool(&store))
        .await
        .expect("relational shadow");

        let loaded = store
            .load_legacy_operational()
            .await
            .expect("load")
            .expect("source");
        let LegacyOperationalRead::SettingsDocument(loaded_document) = &loaded else {
            panic!("settings document must take precedence");
        };
        assert_eq!(loaded_document, document);
        assert_eq!(
            store.get_setting("operational").await.expect("setting"),
            Some(document.to_string()),
            "compatibility reads never delete the authoritative blob"
        );
        let debug = format!("{loaded:?}");
        assert!(debug.contains("settings.operational"));
        assert!(!debug.contains("blob-secret"));
    }

    #[tokio::test]
    async fn legacy_relational_operational_preserves_order_scopes_and_safe_secrets() {
        const B1: &str = "10000000-0000-0000-0000-000000000001";
        const B2: &str = "10000000-0000-0000-0000-000000000002";
        const B_DEAD: &str = "10000000-0000-0000-0000-000000000003";
        const P1: &str = "20000000-0000-0000-0000-000000000001";
        const P2: &str = "20000000-0000-0000-0000-000000000002";
        const P_DEAD: &str = "20000000-0000-0000-0000-000000000003";
        const A1: &str = "30000000-0000-0000-0000-000000000001";
        const A_DEAD: &str = "30000000-0000-0000-0000-000000000002";
        const A2: &str = "30000000-0000-0000-0000-000000000003";
        const K1: &str = "40000000-0000-0000-0000-000000000001";
        const U1: &str = "50000000-0000-0000-0000-000000000001";

        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        let pool = sqlite_pool(&store);

        for (id, name, api_key, metrics, created_at, deleted_at) in [
            (
                B1,
                "primary",
                Some("upstream-secret"),
                Some(r#"{"preset":"vllm","url":"https://metrics.example"}"#),
                1_i64,
                None,
            ),
            (B2, "secondary", None, None, 2, None),
            (B_DEAD, "retired", None, None, 3, Some(99_i64)),
        ] {
            sqlx::query(
                "INSERT INTO backends (id, name, base_url, api_key, enabled, metrics_json, \
                 created_at_ms, updated_at_ms, deleted_at_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(name)
            .bind(format!("https://{name}.example/v1"))
            .bind(api_key)
            .bind(1_i64)
            .bind(metrics)
            .bind(created_at)
            .bind(created_at)
            .bind(deleted_at)
            .execute(pool)
            .await
            .expect("backend");
        }

        for (id, name, kwargs, created_at, deleted_at) in [
            (
                P1,
                "smart",
                Some(r#"{"temperature":0.2,"chat_template_kwargs":{"x":true}}"#),
                1_i64,
                None,
            ),
            (P2, "fast", None, 2, None),
            (P_DEAD, "retired-profile", None, 3, Some(99_i64)),
        ] {
            sqlx::query(
                "INSERT INTO model_profiles (id, name, upstream_model, system_prompt_prefix, \
                 upstream_chat_kwargs_json, created_at_ms, updated_at_ms, deleted_at_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(name)
            .bind(Some(format!("upstream-{name}")))
            .bind(Some(format!("prefix-{name}")))
            .bind(kwargs)
            .bind(created_at)
            .bind(created_at)
            .bind(deleted_at)
            .execute(pool)
            .await
            .expect("profile");
        }

        for (id, name, created_at, deleted_at) in [
            (A1, "public", 1_i64, None),
            (A_DEAD, "retired-alias", 2, Some(99_i64)),
            (A2, "private", 3, None),
        ] {
            sqlx::query(
                "INSERT INTO aliases (id, name, created_at_ms, updated_at_ms, deleted_at_ms) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(name)
            .bind(created_at)
            .bind(created_at)
            .bind(deleted_at)
            .execute(pool)
            .await
            .expect("alias");
        }

        sqlx::query(
            "INSERT INTO users \
             (id, username, password_hash, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(U1)
        .bind("owner")
        .bind("argon2")
        .bind(1_i64)
        .bind(1_i64)
        .execute(pool)
        .await
        .expect("user");
        sqlx::query(
            "INSERT INTO api_keys \
             (id, secret, label, user_id, created_at_ms, updated_at_ms) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(K1)
        .bind("legacy-client-secret")
        .bind("team")
        .bind(U1)
        .bind(1_i64)
        .bind(1_i64)
        .execute(pool)
        .await
        .expect("key");

        for (profile, backend, ordinal) in
            [(P1, B2, 0_i64), (P1, B_DEAD, 1), (P1, B1, 2), (P2, B1, 0)]
        {
            sqlx::query(
                "INSERT INTO profile_backends (profile_id, backend_id, ordinal) \
                 VALUES (?, ?, ?)",
            )
            .bind(profile)
            .bind(backend)
            .bind(ordinal)
            .execute(pool)
            .await
            .expect("profile backend");
        }
        for (alias, profile, ordinal) in
            [(A1, P2, 0_i64), (A1, P_DEAD, 1), (A1, P1, 2), (A2, P1, 0)]
        {
            sqlx::query(
                "INSERT INTO alias_profiles (alias_id, profile_id, ordinal) VALUES (?, ?, ?)",
            )
            .bind(alias)
            .bind(profile)
            .bind(ordinal)
            .execute(pool)
            .await
            .expect("alias profile");
        }
        for alias in [A1, A_DEAD, A2] {
            sqlx::query("INSERT INTO key_aliases (key_id, alias_id) VALUES (?, ?)")
                .bind(K1)
                .bind(alias)
                .execute(pool)
                .await
                .expect("key alias");
        }
        store
            .set_setting("unknown_model_policy", "reject")
            .await
            .expect("policy");

        let loaded = store
            .load_legacy_operational()
            .await
            .expect("load")
            .expect("seeded");
        let debug = format!("{loaded:?}");
        assert!(!debug.contains("upstream-secret"));
        assert!(!debug.contains("legacy-client-secret"));
        let LegacyOperationalRead::Relational(operational) = loaded else {
            panic!("expected relational source");
        };

        assert_eq!(operational.backends.len(), 2);
        let primary = operational
            .backends
            .iter()
            .find(|backend| backend.id == uuid::Uuid::parse_str(B1).unwrap())
            .expect("primary backend");
        assert_eq!(
            primary.extra.get("metrics"),
            Some(&serde_json::json!({
                "preset": "vllm",
                "url": "https://metrics.example"
            }))
        );

        let smart = operational
            .model_profiles
            .iter()
            .find(|profile| profile.id == uuid::Uuid::parse_str(P1).unwrap())
            .expect("smart profile");
        assert_eq!(
            smart.backends,
            [B2, B1]
                .map(|id| uuid::Uuid::parse_str(id).unwrap())
                .to_vec()
        );
        assert_eq!(
            smart.profile.upstream_model.as_deref(),
            Some("upstream-smart")
        );
        assert_eq!(
            smart.profile.system_prompt_prefix.as_deref(),
            Some("prefix-smart")
        );
        assert_eq!(smart.profile.upstream_chat_kwargs["temperature"], 0.2);
        assert!(smart.profile.extends.is_empty());
        assert!(smart.profile.roles.is_none());

        let public = operational
            .aliases
            .iter()
            .find(|alias| alias.id == uuid::Uuid::parse_str(A1).unwrap())
            .expect("public alias");
        assert_eq!(
            public.profiles,
            [P2, P1]
                .map(|id| uuid::Uuid::parse_str(id).unwrap())
                .to_vec()
        );
        assert_eq!(operational.keys.len(), 1);
        assert_eq!(
            operational.keys[0].key,
            api_key_digest("legacy-client-secret")
        );
        assert_eq!(
            operational.keys[0].allowed_aliases,
            [A1, A2]
                .map(|id| uuid::Uuid::parse_str(id).unwrap())
                .to_vec()
        );
        assert_eq!(
            operational.keys[0].user_id,
            Some(uuid::Uuid::parse_str(U1).unwrap())
        );
        assert_eq!(
            operational.unknown_model_policy,
            crate::control_plane::UnknownModelPolicy::Reject
        );

        let stored: String = sqlx::query("SELECT secret FROM api_keys WHERE id = ?")
            .bind(K1)
            .fetch_one(pool)
            .await
            .expect("stored key")
            .try_get(0)
            .expect("secret");
        assert_eq!(stored, "legacy-client-secret");
    }

    #[tokio::test]
    async fn legacy_relational_tombstones_signal_seeded_empty() {
        const TOMBSTONE: &str = "60000000-0000-0000-0000-000000000001";
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        assert_eq!(store.load_legacy_operational().await.expect("fresh"), None);
        store
            .set_setting("operational", "  \n")
            .await
            .expect("blank document");
        assert_eq!(
            store.load_legacy_operational().await.expect("blank"),
            None,
            "a blank legacy document is not an operational source"
        );

        sqlx::query(
            "INSERT INTO aliases \
             (id, name, created_at_ms, updated_at_ms, deleted_at_ms) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(TOMBSTONE)
        .bind("removed")
        .bind(1_i64)
        .bind(2_i64)
        .bind(2_i64)
        .execute(sqlite_pool(&store))
        .await
        .expect("tombstone");

        let Some(LegacyOperationalRead::Relational(operational)) =
            store.load_legacy_operational().await.expect("load")
        else {
            panic!("tombstones must signal an already-seeded database");
        };
        assert_eq!(
            operational,
            crate::control_plane::OperationalConfig::default()
        );
    }

    #[tokio::test]
    async fn account_keys_alone_do_not_signal_legacy_operational_state() {
        // Regression: the accounts feature stores its keys in `api_keys`. A
        // database holding only such keys (no backends/profiles/aliases/policy)
        // is NOT a legacy operational source; treating it as one replaced the
        // YAML routing with an empty relational config and then registered the
        // same key twice ("duplicate virtual API key id") on every restart.
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        store
            .put_api_key(
                "80000000-0000-4000-8000-000000000001",
                "llmc_secret",
                Some("koen-main"),
                None,
                &[],
                "test",
            )
            .await
            .expect("key");
        assert_eq!(
            store.load_legacy_operational().await.expect("load"),
            None,
            "account-managed keys are loaded by the accounts registry, not as legacy state"
        );
    }

    #[tokio::test]
    async fn legacy_key_scoped_only_to_deleted_alias_fails_closed() {
        const ALIAS: &str = "70000000-0000-0000-0000-000000000001";
        const KEY: &str = "70000000-0000-0000-0000-000000000002";
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        let pool = sqlite_pool(&store);
        sqlx::query(
            "INSERT INTO aliases (id, name, created_at_ms, updated_at_ms, deleted_at_ms) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(ALIAS)
        .bind("retired")
        .bind(1_i64)
        .bind(2_i64)
        .bind(2_i64)
        .execute(pool)
        .await
        .expect("alias");
        sqlx::query(
            "INSERT INTO api_keys (id, secret, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?)",
        )
        .bind(KEY)
        .bind("secret")
        .bind(1_i64)
        .bind(1_i64)
        .execute(pool)
        .await
        .expect("key");
        sqlx::query("INSERT INTO key_aliases (key_id, alias_id) VALUES (?, ?)")
            .bind(KEY)
            .bind(ALIAS)
            .execute(pool)
            .await
            .expect("scope");

        let error = store.load_legacy_operational().await.unwrap_err();
        assert!(error.contains("refusing to widen"));
        assert!(error.contains(KEY));
        assert!(!error.contains("secret"));
    }

    #[tokio::test]
    async fn legacy_relational_ids_are_strict_uuids() {
        let store = SqlStore::connect_sqlite("sqlite::memory:")
            .await
            .expect("connect");
        sqlx::query(
            "INSERT INTO aliases \
             (id, name, created_at_ms, updated_at_ms, deleted_at_ms) VALUES (?, ?, ?, ?, ?)",
        )
        .bind("legacy-string-id")
        .bind("removed")
        .bind(1_i64)
        .bind(2_i64)
        .bind(2_i64)
        .execute(sqlite_pool(&store))
        .await
        .expect("legacy row");
        let error = store
            .load_legacy_operational()
            .await
            .expect_err("invalid id must fail closed");
        assert!(error.contains("not a valid UUID"));
    }

    #[test]
    fn remote_postgres_rejects_downgrade_capable_default_mode() {
        let options = PgConnectOptions::new_without_pgpass()
            .host("db.example.test")
            .ssl_mode(PgSslMode::Prefer);
        let error = validate_postgres_transport(&options).expect_err("remote prefer must fail");
        assert!(error.contains("sslmode=require"));
        assert!(!error.contains("db.example.test"));
    }

    #[test]
    fn remote_postgres_accepts_encrypted_only_modes() {
        for mode in [
            PgSslMode::Require,
            PgSslMode::VerifyCa,
            PgSslMode::VerifyFull,
        ] {
            let options = PgConnectOptions::new_without_pgpass()
                .host("db.example.test")
                .ssl_mode(mode);
            validate_postgres_transport(&options).expect("encrypted-only remote mode");
        }
    }

    #[test]
    fn local_postgres_accepts_developer_default_mode() {
        for host in ["localhost", "127.0.0.1", "::1"] {
            let options = PgConnectOptions::new_without_pgpass()
                .host(host)
                .ssl_mode(PgSslMode::Prefer);
            validate_postgres_transport(&options).expect("local PostgreSQL");
        }
        let socket = PgConnectOptions::new_without_pgpass()
            .socket("/tmp")
            .ssl_mode(PgSslMode::Disable);
        validate_postgres_transport(&socket).expect("local Unix socket");
    }

    #[tokio::test]
    async fn invalid_database_urls_never_echo_credentials() {
        let postgres = match SqlStore::connect_postgres("postgres://user:super-secret@[").await {
            Ok(_) => panic!("invalid PostgreSQL URL unexpectedly connected"),
            Err(error) => error,
        };
        assert_eq!(postgres, "invalid PostgreSQL connection URL");
        assert!(!postgres.contains("super-secret"));

        let sqlite = match SqlStore::connect_sqlite("sqlite://safe.db?password=super-secret").await
        {
            Ok(_) => panic!("invalid SQLite URL unexpectedly connected"),
            Err(error) => error,
        };
        assert_eq!(sqlite, "invalid SQLite connection URL");
        assert!(!sqlite.contains("super-secret"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn jsonl_writer_tightens_existing_directory_and_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!(
            "llmconduit-jsonl-permissions-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir(&dir).expect("create JSONL directory");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777))
            .expect("loosen directory");
        for filename in ["requests.jsonl", "events.jsonl"] {
            let path = dir.join(filename);
            std::fs::write(&path, b"existing\n").expect("precreate JSONL file");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666))
                .expect("loosen file");
        }

        let writer = JsonlWriter::new(&dir).expect("writer");
        writer
            .begin_request(request("private"))
            .await
            .expect("request write");
        writer
            .append_event(EventRow {
                request_id: "private".to_string(),
                seq: 1,
                ts_ms: 1,
                hop: "client_in".to_string(),
                kind: "body".to_string(),
                payload: None,
                bytes: None,
            })
            .await
            .expect("event write");

        assert_eq!(
            std::fs::metadata(&dir)
                .expect("directory metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        for filename in ["requests.jsonl", "events.jsonl"] {
            let path = dir.join(filename);
            assert_eq!(
                std::fs::metadata(&path)
                    .expect("file metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            std::fs::remove_file(path).expect("remove JSONL file");
        }
        std::fs::remove_dir(dir).expect("remove JSONL directory");
    }

    #[tokio::test]
    async fn retained_jsonl_writer_uses_gateway_owned_daily_files() {
        let dir = std::env::temp_dir().join(format!(
            "llmconduit-jsonl-daily-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let writer =
            JsonlWriter::new_with_retention(&dir, NonZeroU64::new(30).unwrap()).expect("writer");
        writer
            .begin_request(request("daily"))
            .await
            .expect("request write");
        let expected = dir.join(format!(
            "requests-{}.jsonl",
            chrono::Utc::now().format("%Y-%m-%d")
        ));
        assert!(expected.is_file());
        std::fs::remove_file(expected).expect("remove daily file");
        std::fs::remove_dir(dir).expect("remove JSONL directory");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sqlite_file_is_tightened_to_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!(
            "llmconduit-sqlite-permissions-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir(&dir).expect("create SQLite directory");
        let path = dir.join("control-plane.sqlite");
        std::fs::write(&path, b"").expect("precreate SQLite database");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666))
            .expect("loosen database");

        let store = SqlStore::connect_sqlite(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect");
        assert_eq!(
            std::fs::metadata(&path)
                .expect("database metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let journal_mode: String = sqlx::query("PRAGMA journal_mode")
            .fetch_one(sqlite_pool(&store))
            .await
            .expect("journal mode")
            .try_get(0)
            .expect("journal mode value");
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        let synchronous: i64 = sqlx::query("PRAGMA synchronous")
            .fetch_one(sqlite_pool(&store))
            .await
            .expect("synchronous")
            .try_get(0)
            .expect("synchronous value");
        assert_eq!(synchronous, 1, "WAL databases run synchronous=NORMAL");
        let cache_size: i64 = sqlx::query("PRAGMA cache_size")
            .fetch_one(sqlite_pool(&store))
            .await
            .expect("cache_size")
            .try_get(0)
            .expect("cache_size value");
        assert_eq!(cache_size, -8192);
        assert_eq!(sqlite_pool(&store).options().get_max_connections(), 4);
        store.close().await;
        std::fs::remove_file(path).expect("remove database");
        for suffix in ["-wal", "-shm"] {
            let sidecar = dir.join(format!("control-plane.sqlite{suffix}"));
            if let Err(error) = std::fs::remove_file(&sidecar)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                panic!("remove SQLite sidecar {}: {error}", sidecar.display());
            }
        }
        std::fs::remove_dir(dir).expect("remove SQLite directory");
    }

    #[derive(Default)]
    struct BlockingWriter {
        entered: Notify,
        release: Notify,
        writes: Mutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl PersistenceWriter for BlockingWriter {
        async fn begin_request(&self, _row: RequestRow) -> StoreResult<()> {
            self.entered.notify_one();
            self.release.notified().await;
            self.writes.lock().unwrap().push("begin");
            Ok(())
        }

        async fn append_event(&self, _event: EventRow) -> StoreResult<()> {
            self.writes.lock().unwrap().push("event");
            Ok(())
        }

        async fn finish_request(&self, _id: &str, _finish: RequestFinish) -> StoreResult<()> {
            self.writes.lock().unwrap().push("finish");
            Ok(())
        }
    }

    #[tokio::test]
    async fn bounded_queue_reports_overflow_without_waiting() {
        let writer = Arc::new(BlockingWriter::default());
        let queue = PersistenceQueue::spawn_with_max_bytes(
            Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
            NonZeroUsize::new(1).unwrap(),
            NonZeroU64::new(64 * 1024).unwrap(),
        );
        queue.try_begin(request("one")).expect("first accepted");
        writer.entered.notified().await;

        queue
            .try_event(EventRow {
                request_id: "one".to_string(),
                seq: 1,
                ts_ms: 1,
                hop: "client_in".to_string(),
                kind: "body".to_string(),
                payload: None,
                bytes: None,
            })
            .expect("fills queue");
        assert_eq!(
            queue.try_finish("one".to_string(), finish()),
            Err(EnqueueError::Full)
        );
        assert_eq!(queue.stats().dropped_full, 1);

        writer.release.notify_waiters();
        queue.flush().await.expect("flush");
        assert_eq!(*writer.writes.lock().unwrap(), vec!["begin", "event"]);
    }

    #[tokio::test]
    async fn persistence_queue_rejects_oversized_command_by_byte_budget() {
        let writer = Arc::new(BlockingWriter::default());
        let queue = PersistenceQueue::spawn_with_max_bytes(
            Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(512).unwrap(),
        );

        assert_eq!(
            queue.try_event(event_with_payload("one", 2048)),
            Err(EnqueueError::Full)
        );
        assert_eq!(queue.stats().accepted, 0);
        assert_eq!(queue.stats().dropped_full, 1);
        assert_eq!(queue.stats().reserved_bytes, 0);
        assert_eq!(queue.stats().max_bytes, 512);
    }

    #[tokio::test]
    async fn persistence_queue_byte_budget_rejects_even_with_queue_space() {
        let writer = Arc::new(BlockingWriter::default());
        let queue = PersistenceQueue::spawn_with_max_bytes(
            Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(4096).unwrap(),
        );
        let mut large_request = request("one");
        large_request.id = "one".repeat(1000);

        queue
            .try_begin(large_request)
            .expect("first request accepted");
        writer.entered.notified().await;
        assert_eq!(
            queue.try_event(event_with_payload("two", 1000)),
            Err(EnqueueError::Full)
        );
        assert_eq!(queue.stats().accepted, 1);
        assert_eq!(queue.stats().dropped_full, 1);
        assert!(queue.stats().reserved_bytes > 0);

        writer.release.notify_waiters();
        queue.flush().await.expect("flush");
        assert_eq!(queue.stats().reserved_bytes, 0);
    }

    #[tokio::test]
    async fn persistence_queue_releases_budget_after_completed_write() {
        let writer = Arc::new(BlockingWriter::default());
        let queue = PersistenceQueue::spawn_with_max_bytes(
            Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(64 * 1024).unwrap(),
        );

        queue.try_begin(request("one")).expect("accepted");
        writer.entered.notified().await;
        assert!(queue.stats().reserved_bytes > 0);

        writer.release.notify_waiters();
        queue.flush().await.expect("flush");
        assert_eq!(queue.stats().reserved_bytes, 0);
    }

    #[derive(Default)]
    struct FailingWriter {
        writes: Mutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl PersistenceWriter for FailingWriter {
        async fn begin_request(&self, _row: RequestRow) -> StoreResult<()> {
            self.writes.lock().unwrap().push("begin");
            Err("boom".to_string())
        }

        async fn append_event(&self, _event: EventRow) -> StoreResult<()> {
            self.writes.lock().unwrap().push("event");
            Err("boom".to_string())
        }

        async fn finish_request(&self, _id: &str, _finish: RequestFinish) -> StoreResult<()> {
            self.writes.lock().unwrap().push("finish");
            Err("boom".to_string())
        }
    }

    #[tokio::test]
    async fn persistence_queue_releases_budget_after_failed_write() {
        let writer = Arc::new(FailingWriter::default());
        let queue = PersistenceQueue::spawn_with_max_bytes(
            Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(64 * 1024).unwrap(),
        );

        queue
            .try_event(event_with_payload("one", 4096))
            .expect("accepted");
        assert!(queue.stats().reserved_bytes > 0);

        queue.flush().await.expect("flush");
        assert_eq!(queue.stats().write_failures, 1);
        assert_eq!(queue.stats().reserved_bytes, 0);
        assert_eq!(*writer.writes.lock().unwrap(), vec!["event"]);
    }

    #[tokio::test]
    async fn persistence_queue_count_limit_rejected_enqueue_releases_budget() {
        let writer = Arc::new(BlockingWriter::default());
        let queue = PersistenceQueue::spawn_with_max_bytes(
            Arc::clone(&writer) as Arc<dyn PersistenceWriter>,
            NonZeroUsize::new(1).unwrap(),
            NonZeroU64::new(64 * 1024).unwrap(),
        );
        queue.try_begin(request("one")).expect("first accepted");
        writer.entered.notified().await;
        queue
            .try_event(event_with_payload("one", 2048))
            .expect("queued event accepted");
        let reserved_before_rejected_enqueue = queue.stats().reserved_bytes;

        assert_eq!(
            queue.try_finish("one".to_string(), finish()),
            Err(EnqueueError::Full)
        );
        assert_eq!(
            queue.stats().reserved_bytes,
            reserved_before_rejected_enqueue
        );
        assert_eq!(queue.stats().dropped_full, 1);

        writer.release.notify_waiters();
        queue.flush().await.expect("flush");
        assert_eq!(queue.stats().reserved_bytes, 0);
    }
}
