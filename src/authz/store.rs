use super::policy::{
    ALL_MANAGEMENT_PERMISSIONS, Endpoint, LimitSet, ManagementPermission, PolicyBinding,
    PolicyEffect, PolicyMatcher, PolicyRule, PolicySnapshot, PolicySubject, UtcWindow,
};
use crate::dashboard_access::{
    AccessApiKey, AccessAuditEvent, AccessCounts, AccessError, AccessGroup, AccessOperation,
    AccessPolicy, AccessPricingRow, AccessResult, AccessRole, AccessSession, AccessSummary,
    AccessTimeWindow, AccessUsageRow, AccessUser, ActorSummary, ManagementActor,
    ManagementPermission as WirePermission,
};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use uuid::Uuid;

pub(crate) struct AuthStore {
    connection: Connection,
}

#[derive(Debug, Clone)]
pub(crate) struct StoredCredential {
    pub id: String,
    pub principal_id: String,
    pub prefix: String,
    pub digest: [u8; 32],
    pub capture_payloads: bool,
}

pub(crate) struct StoredAuthority {
    pub epoch: u64,
    pub credentials: Vec<StoredCredential>,
    pub policy: Arc<PolicySnapshot>,
}

pub(crate) struct StoredDashboardSession {
    pub session_id: String,
    pub principal_id: String,
    pub key_id: String,
    pub key_prefix: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiKeySummary {
    pub id: String,
    pub principal_id: String,
    pub name: String,
    pub prefix: String,
    pub enabled: bool,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub last_used_at: Option<i64>,
    pub capture_payloads: bool,
}

#[derive(Clone, Serialize)]
pub struct CreatedApiKey {
    #[serde(flatten)]
    pub summary: ApiKeySummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_key: Option<String>,
}

impl fmt::Debug for CreatedApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreatedApiKey")
            .field("summary", &self.summary)
            .field("raw_key", &self.raw_key.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

impl CreatedApiKey {
    pub(crate) fn with_raw(mut self, raw: String) -> Self {
        self.raw_key = Some(raw);
        self
    }
}

impl AuthStore {
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path)
            .map_err(|err| format!("failed to open auth store {}: {err}", path.display()))?;
        connection
            .execute_batch(SCHEMA)
            .map_err(|err| format!("failed to migrate auth store: {err}"))?;
        migrate_api_key_payload_capture(&connection)
            .map_err(|err| format!("failed to migrate auth key payload capture: {err}"))?;
        crate::usage_accounting::migrate_usage_schema(&connection)
            .map_err(|err| format!("failed to migrate auth usage store: {err}"))?;
        crate::openrouter_pricing::migrate_pricing_schema(&connection)
            .map_err(|err| format!("failed to migrate imported pricing store: {err}"))?;
        Ok(Self { connection })
    }

    pub(crate) fn persist_imported_price(
        &mut self,
        actor: &ManagementActor,
        snapshot: &crate::openrouter_pricing::OpenRouterPriceSnapshot,
    ) -> Result<(), AccessError> {
        crate::openrouter_pricing::persist_imported_price(&mut self.connection, snapshot)
            .map_err(|err| AccessError::new(StatusCode::BAD_GATEWAY, err.to_string()))?;
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "model".into(),
            Value::String(snapshot.price.model_id.clone()),
        );
        metadata.insert(
            "source_url".into(),
            Value::String(snapshot.source_url.clone()),
        );
        let tx = self.connection.transaction().map_err(access_db)?;
        audit(
            &tx,
            &actor_name(actor),
            "pricing.synced",
            &snapshot.price.model_id,
            "ok",
            metadata,
        )
        .map_err(access_internal)?;
        tx.commit().map_err(access_db)
    }

    pub(crate) fn audit_pricing_sync_failure(
        &mut self,
        actor: &ManagementActor,
        model: &str,
        error: &str,
    ) -> Result<(), AccessError> {
        let tx = self.connection.transaction().map_err(access_db)?;
        let mut metadata = BTreeMap::new();
        metadata.insert("model".into(), Value::String(model.to_string()));
        metadata.insert("error".into(), Value::String(error.to_string()));
        audit(
            &tx,
            &actor_name(actor),
            "pricing.sync_failed",
            model,
            "error",
            metadata,
        )
        .map_err(access_internal)?;
        tx.commit().map_err(access_db)
    }

    pub(crate) fn effective_prices(
        &self,
    ) -> Result<HashMap<String, crate::config::ModelPrice>, String> {
        let mut prices = HashMap::new();
        let mut imported = self
            .connection
            .prepare(
                "SELECT model_id,prompt_mean_nano_usd,completion_mean_nano_usd,cached_mean_nano_usd
                 FROM auth_imported_price_snapshots p
                 WHERE fetched_at_ms=(SELECT MAX(fetched_at_ms) FROM auth_imported_price_snapshots WHERE model_id=p.model_id)
                 GROUP BY model_id,fetched_at_ms
                 ORDER BY model_id",
            )
            .map_err(db)?;
        let rows = imported
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })
            .map_err(db)?;
        for row in rows {
            let (model, input, output, cached) = row.map_err(db)?;
            let input = input as f64 / 1_000_000.0;
            let output = output as f64 / 1_000_000.0;
            let price = cached.map_or_else(
                || crate::config::ModelPrice::without_cached(input, output),
                |cached| crate::config::ModelPrice::new(input, output, cached as f64 / 1_000_000.0),
            );
            prices.insert(model, price);
        }

        let mut overrides = self
            .connection
            .prepare("SELECT model,input_per_1k,output_per_1k FROM auth_price_snapshots WHERE source='operator'")
            .map_err(db)?;
        let rows = overrides
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(db)?;
        for row in rows {
            let (model, input, output) = row.map_err(db)?;
            let input = input.parse::<f64>().map_err(|err| err.to_string())?;
            let output = output.parse::<f64>().map_err(|err| err.to_string())?;
            prices.insert(
                model,
                crate::config::ModelPrice::without_cached(input, output),
            );
        }
        Ok(prices)
    }

    pub fn record_usage_once(
        &self,
        event: &crate::usage_accounting::UsageEvent,
    ) -> Result<bool, String> {
        crate::usage_accounting::record_usage_once(&self.connection, event)
            .map_err(|err| format!("failed to record auth usage: {err}"))
    }

    pub fn key_count(&self) -> Result<u64, String> {
        self.connection
            .query_row("SELECT COUNT(*) FROM auth_api_keys", [], |row| row.get(0))
            .map_err(db)
    }

    pub fn insert_bootstrap_key(&mut self, raw: &str, digest: &[u8; 32]) -> Result<(), String> {
        let now = Utc::now().timestamp();
        let tx = self.connection.transaction().map_err(db)?;
        tx.execute(
            "INSERT INTO auth_principals(id,kind,display_name,enabled,created_at) VALUES('usr_bootstrap','user','Bootstrap administrator',1,?1)",
            [now],
        ).map_err(db)?;
        tx.execute(
            "INSERT INTO auth_api_keys(id,principal_id,name,prefix,hmac_sha256_digest,enabled,created_at) VALUES('key_bootstrap','usr_bootstrap','Bootstrap key',?1,?2,1,?3)",
            params![display_prefix(raw), digest.as_slice(), now],
        ).map_err(db)?;
        tx.execute(
            "INSERT INTO auth_policies(id,name,effect,enabled,created_at,updated_at) VALUES('pol_bootstrap','Bootstrap full access','allow',1,?1,?1)",
            [now],
        ).map_err(db)?;
        tx.execute(
            "INSERT INTO auth_policy_subjects(policy_id,subject_kind,subject_id) VALUES('pol_bootstrap','key','key_bootstrap')",
            [],
        ).map_err(db)?;
        for permission in ALL_MANAGEMENT_PERMISSIONS {
            tx.execute(
                "INSERT INTO auth_management_permissions(policy_id,permission) VALUES('pol_bootstrap',?1)",
                [permission.as_str()],
            ).map_err(db)?;
        }
        bump_epoch(&tx)?;
        audit(
            &tx,
            "bootstrap",
            "bootstrap.created",
            "key_bootstrap",
            "ok",
            BTreeMap::new(),
        )?;
        tx.commit().map_err(db)
    }

    pub fn create_key(
        &mut self,
        principal_name: &str,
        key_name: &str,
        raw: &str,
        digest: &[u8; 32],
        endpoints: &[String],
        models: &[String],
    ) -> Result<CreatedApiKey, String> {
        let now = Utc::now().timestamp();
        let tx = self.connection.transaction().map_err(db)?;
        let principal_id = format!("usr_{}", Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO auth_principals(id,kind,display_name,enabled,created_at) VALUES(?1,'service_account',?2,1,?3)",
            params![principal_id, required(principal_name, "principal_name")?, now],
        ).map_err(db)?;
        let created = insert_key(&tx, &principal_id, key_name, raw, digest, None, false, now)?;
        let policy_id = format!("pol_{}", Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO auth_policies(id,name,effect,enabled,created_at,updated_at) VALUES(?1,?2,'allow',1,?3,?3)",
            params![policy_id, format!("{} inference access", key_name.trim()), now],
        ).map_err(db)?;
        tx.execute(
            "INSERT INTO auth_policy_subjects(policy_id,subject_kind,subject_id) VALUES(?1,'key',?2)",
            params![policy_id, created.summary.id],
        ).map_err(db)?;
        for endpoint in nonempty_or_wildcard(endpoints) {
            tx.execute(
                "INSERT INTO auth_policy_scopes(policy_id,dimension,matcher) VALUES(?1,'endpoint',?2)",
                params![policy_id, endpoint],
            ).map_err(db)?;
        }
        for model in nonempty_or_wildcard(models) {
            for dimension in ["requested_model", "served_model"] {
                tx.execute(
                    "INSERT INTO auth_policy_scopes(policy_id,dimension,matcher) VALUES(?1,?2,?3)",
                    params![policy_id, dimension, model],
                )
                .map_err(db)?;
            }
        }
        bump_epoch(&tx)?;
        audit(
            &tx,
            "bootstrap",
            "key.created",
            &created.summary.id,
            "ok",
            BTreeMap::new(),
        )?;
        tx.commit().map_err(db)?;
        Ok(created)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_key_for_principal(
        &mut self,
        principal_id: &str,
        name: &str,
        raw: &str,
        digest: &[u8; 32],
        expires_at: Option<i64>,
        capture_payloads: bool,
        actor: &ManagementActor,
    ) -> Result<CreatedApiKey, String> {
        let now = Utc::now().timestamp();
        if expires_at.is_some_and(|expiry| expiry <= now) {
            return Err("API key expiry must be in the future".into());
        }
        let tx = self.connection.transaction().map_err(db)?;
        let exists = tx
            .query_row(
                "SELECT enabled FROM auth_principals WHERE id=?1",
                [principal_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(db)?;
        if exists != Some(1) {
            return Err("principal does not exist or is disabled".into());
        }
        let created = insert_key(
            &tx,
            principal_id,
            name,
            raw,
            digest,
            expires_at,
            capture_payloads,
            now,
        )?;
        bump_epoch(&tx)?;
        audit(
            &tx,
            &actor_name(actor),
            "key.created",
            &created.summary.id,
            "ok",
            BTreeMap::new(),
        )?;
        tx.commit().map_err(db)?;
        Ok(created)
    }

    pub fn rotate_key(
        &mut self,
        old_id: &str,
        raw: &str,
        digest: &[u8; 32],
        actor: &ManagementActor,
    ) -> Result<CreatedApiKey, String> {
        let now = Utc::now().timestamp();
        let tx = self.connection.transaction().map_err(db)?;
        let old = query_key(&tx, old_id)?.ok_or_else(|| "API key not found".to_string())?;
        if !old.enabled {
            return Err("API key is revoked".into());
        }
        let created = insert_key(
            &tx,
            &old.principal_id,
            &format!("{} (rotated)", old.name),
            raw,
            digest,
            old.expires_at,
            old.capture_payloads,
            now,
        )?;
        tx.execute(
            "UPDATE auth_policy_subjects SET subject_id=?2 WHERE subject_kind='key' AND subject_id=?1",
            params![old_id, created.summary.id],
        ).map_err(db)?;
        tx.execute(
            "UPDATE auth_api_keys SET enabled=0,revoked_at=?2 WHERE id=?1",
            params![old_id, now],
        )
        .map_err(db)?;
        tx.execute("UPDATE auth_dashboard_sessions SET revoked_at=?2,revoked_reason='key_rotated' WHERE key_id=?1 AND revoked_at IS NULL", params![old_id, now]).map_err(db)?;
        bump_epoch(&tx)?;
        audit(
            &tx,
            &actor_name(actor),
            "key.rotated",
            old_id,
            "ok",
            BTreeMap::new(),
        )?;
        tx.commit().map_err(db)?;
        Ok(created)
    }

    pub fn revoke_key(&mut self, key_id: &str) -> Result<bool, String> {
        self.revoke_key_as(key_id, &ManagementActor::Bootstrap)
    }

    fn revoke_key_as(&mut self, key_id: &str, actor: &ManagementActor) -> Result<bool, String> {
        let now = Utc::now().timestamp();
        let tx = self.connection.transaction().map_err(db)?;
        let changed = tx
            .execute(
                "UPDATE auth_api_keys SET enabled=0,revoked_at=?2 WHERE id=?1 AND enabled=1",
                params![key_id, now],
            )
            .map_err(db)?
            > 0;
        if changed {
            tx.execute("UPDATE auth_dashboard_sessions SET revoked_at=?2,revoked_reason='key_revoked' WHERE key_id=?1 AND revoked_at IS NULL", params![key_id, now]).map_err(db)?;
            tx.execute("DELETE FROM auth_sessions WHERE key_id=?1", [key_id])
                .map_err(db)?;
            bump_epoch(&tx)?;
            audit(
                &tx,
                &actor_name(actor),
                "key.revoked",
                key_id,
                "ok",
                BTreeMap::new(),
            )?;
        }
        tx.commit().map_err(db)?;
        Ok(changed)
    }

    pub(crate) fn set_key_payload_capture(
        &mut self,
        key_id: &str,
        capture_payloads: bool,
        actor: &ManagementActor,
    ) -> Result<bool, String> {
        let tx = self.connection.transaction().map_err(db)?;
        let changed = tx
            .execute(
                "UPDATE auth_api_keys SET capture_payloads=?2 WHERE id=?1",
                params![key_id, i64::from(capture_payloads)],
            )
            .map_err(db)?
            > 0;
        if changed {
            bump_epoch(&tx)?;
            let mut metadata = BTreeMap::new();
            metadata.insert("capture_payloads".into(), Value::Bool(capture_payloads));
            audit(
                &tx,
                &actor_name(actor),
                "key.payload_capture_updated",
                key_id,
                "ok",
                metadata,
            )?;
        }
        tx.commit().map_err(db)?;
        Ok(changed)
    }

    pub fn list_keys(&self) -> Result<Vec<ApiKeySummary>, String> {
        let mut statement = self.connection.prepare(
            "SELECT id,principal_id,name,prefix,enabled,created_at,expires_at,last_used_at,capture_payloads FROM auth_api_keys ORDER BY created_at DESC,id"
        ).map_err(db)?;
        statement
            .query_map([], map_key)
            .map_err(db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db)
    }

    pub fn load_authority(&self) -> Result<StoredAuthority, String> {
        let now = Utc::now().timestamp();
        let epoch = current_epoch(&self.connection)?;
        let mut stmt = self.connection.prepare(
            "SELECT id,principal_id,prefix,hmac_sha256_digest,capture_payloads FROM auth_api_keys WHERE enabled=1 AND (expires_at IS NULL OR expires_at>?1)"
        ).map_err(db)?;
        let credentials = stmt
            .query_map([now], |row| {
                let bytes: Vec<u8> = row.get(3)?;
                let digest: [u8; 32] = bytes.try_into().map_err(|_| {
                    rusqlite::Error::InvalidColumnType(
                        3,
                        "hmac_sha256_digest".into(),
                        rusqlite::types::Type::Blob,
                    )
                })?;
                Ok(StoredCredential {
                    id: row.get(0)?,
                    principal_id: row.get(1)?,
                    prefix: row.get(2)?,
                    digest,
                    capture_payloads: row.get::<_, i64>(4)? != 0,
                })
            })
            .map_err(db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db)?;

        let mut principal_groups: HashMap<String, HashSet<String>> = HashMap::new();
        let mut stmt = self
            .connection
            .prepare("SELECT principal_id,group_id FROM auth_group_members")
            .map_err(db)?;
        for row in stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .map_err(db)?
        {
            let (principal, group) = row.map_err(db)?;
            principal_groups.entry(principal).or_default().insert(group);
        }
        let mut principal_roles: HashMap<String, HashSet<String>> = HashMap::new();
        let mut stmt = self.connection.prepare(
            "SELECT subject_id,role_id FROM auth_role_bindings WHERE subject_kind='principal' UNION SELECT gm.principal_id,rb.role_id FROM auth_role_bindings rb JOIN auth_group_members gm ON rb.subject_kind='group' AND rb.subject_id=gm.group_id"
        ).map_err(db)?;
        for row in stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .map_err(db)?
        {
            let (principal, role) = row.map_err(db)?;
            principal_roles.entry(principal).or_default().insert(role);
        }
        let rules = self.load_rules()?;
        Ok(StoredAuthority {
            epoch,
            credentials,
            policy: Arc::new(PolicySnapshot::new(
                epoch,
                rules,
                principal_groups,
                principal_roles,
            )),
        })
    }

    fn load_rules(&self) -> Result<Vec<PolicyRule>, String> {
        let mut stmt = self
            .connection
            .prepare("SELECT id,effect FROM auth_policies WHERE enabled=1")
            .map_err(db)?;
        let policies = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .map_err(db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db)?;
        let mut rules = Vec::new();
        for (policy_id, effect) in policies {
            let subjects = query_pairs(
                &self.connection,
                "SELECT subject_kind,subject_id FROM auth_policy_subjects WHERE policy_id=?1",
                &policy_id,
            )?;
            let scopes = query_pairs(
                &self.connection,
                "SELECT dimension,matcher FROM auth_policy_scopes WHERE policy_id=?1",
                &policy_id,
            )?;
            let mut endpoints = Vec::new();
            let mut requested = Vec::new();
            let mut served = Vec::new();
            let mut providers = Vec::new();
            let mut routes = Vec::new();
            for (dimension, matcher) in scopes {
                match dimension.as_str() {
                    "endpoint" if matcher != "*" => endpoints.push(
                        Endpoint::parse(&matcher)
                            .ok_or_else(|| format!("invalid endpoint matcher {matcher}"))?,
                    ),
                    "requested_model" if matcher != "*" => requested.push(matcher),
                    "served_model" if matcher != "*" => served.push(matcher),
                    "provider" if matcher != "*" => providers.push(matcher),
                    "route" if matcher != "*" => routes.push(matcher),
                    _ => {}
                }
            }
            let windows = self.query_windows(&policy_id)?;
            let limits = self.query_limits(&policy_id)?;
            let permissions = self.query_permissions(&policy_id)?;
            for (kind, id) in subjects {
                let mut subject_permissions = permissions.clone();
                if kind == "role" {
                    subject_permissions.extend(self.query_role_permissions(&id)?);
                }
                let subject = parse_subject(&kind, id)?;
                rules.push(PolicyRule {
                    id: format!("{policy_id}:{kind}"),
                    effect: if effect == "deny" {
                        PolicyEffect::Deny
                    } else {
                        PolicyEffect::Allow
                    },
                    binding: PolicyBinding { subject },
                    matcher: PolicyMatcher::new(
                        endpoints.clone(),
                        requested.clone(),
                        served.clone(),
                        providers.clone(),
                        routes.clone(),
                    )
                    .map_err(|err| err.to_string())?,
                    windows: windows.clone(),
                    limits,
                    management_permissions: subject_permissions,
                });
            }
        }
        Ok(rules)
    }

    fn query_windows(&self, policy_id: &str) -> Result<Vec<UtcWindow>, String> {
        let mut stmt = self.connection.prepare("SELECT weekday_mask,start_minute,end_minute,absolute_start,absolute_end FROM auth_time_windows WHERE policy_id=?1").map_err(db)?;
        stmt.query_map([policy_id], |r| {
            Ok(UtcWindow {
                weekday_mask: r.get(0)?,
                start_minute: r.get(1)?,
                end_minute: r.get(2)?,
                absolute_start_ms: r.get(3)?,
                absolute_end_ms: r.get(4)?,
            })
        })
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)
    }

    fn query_limits(&self, policy_id: &str) -> Result<LimitSet, String> {
        self.connection.query_row("SELECT max_concurrent_sessions,max_daily_session_starts FROM auth_limits WHERE policy_id=?1", [policy_id], |r| Ok(LimitSet { max_concurrent_sessions: r.get(0)?, max_daily_session_starts: r.get(1)? })).optional().map_err(db).map(|v| v.unwrap_or_default())
    }

    fn query_permissions(&self, policy_id: &str) -> Result<HashSet<ManagementPermission>, String> {
        let mut stmt = self
            .connection
            .prepare("SELECT permission FROM auth_management_permissions WHERE policy_id=?1")
            .map_err(db)?;
        let values = stmt
            .query_map([policy_id], |r| r.get::<_, String>(0))
            .map_err(db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db)?;
        Ok(values
            .into_iter()
            .filter_map(|value| ManagementPermission::parse(&value))
            .collect())
    }

    fn query_role_permissions(
        &self,
        role_id: &str,
    ) -> Result<HashSet<ManagementPermission>, String> {
        let mut stmt = self
            .connection
            .prepare("SELECT permission FROM auth_role_permissions WHERE role_id=?1")
            .map_err(db)?;
        let values = stmt
            .query_map([role_id], |row| row.get::<_, String>(0))
            .map_err(db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db)?;
        Ok(values
            .into_iter()
            .filter_map(|value| ManagementPermission::parse(&value))
            .collect())
    }

    pub fn insert_session(&mut self, id: &str, key_id: &str) -> Result<(), String> {
        self.connection.execute("INSERT INTO auth_sessions(id,key_id,started_at,endpoint) VALUES(?1,?2,?3,'inference')", params![id,key_id,Utc::now().timestamp()]).map_err(db)?;
        Ok(())
    }

    pub fn delete_session(&mut self, id: &str) -> Result<(), String> {
        self.connection
            .execute("DELETE FROM auth_sessions WHERE id=?1", [id])
            .map_err(db)?;
        Ok(())
    }

    pub fn create_dashboard_session(
        &mut self,
        principal_id: &str,
        key_id: &str,
        csrf_digest: &[u8],
        expires_at: i64,
        policy_epoch: u64,
    ) -> Result<StoredDashboardSession, String> {
        let now = Utc::now().timestamp();
        if expires_at <= now {
            return Err("dashboard session expiry must be in the future".into());
        }
        let session_id = format!("dsh_{}", Uuid::new_v4().simple());
        self.connection.execute(
            "INSERT INTO auth_dashboard_sessions(session_id,principal_id,key_id,csrf_secret_digest,created_at,expires_at,policy_epoch_at_login,last_seen_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?5)",
            params![session_id,principal_id,key_id,csrf_digest,now,expires_at,i64::try_from(policy_epoch).map_err(|_|"policy epoch overflow")?],
        ).map_err(db)?;
        let key_prefix = self
            .connection
            .query_row(
                "SELECT prefix FROM auth_api_keys WHERE id=?1",
                [key_id],
                |row| row.get(0),
            )
            .map_err(db)?;
        Ok(StoredDashboardSession {
            session_id,
            principal_id: principal_id.into(),
            key_id: key_id.into(),
            key_prefix,
        })
    }

    pub fn load_dashboard_session(
        &mut self,
        session_id: &str,
    ) -> Result<Option<StoredDashboardSession>, String> {
        let now = Utc::now().timestamp();
        let session = self.connection.query_row(
            "SELECT s.session_id,s.principal_id,s.key_id,k.prefix,s.expires_at FROM auth_dashboard_sessions s JOIN auth_api_keys k ON k.id=s.key_id JOIN auth_principals p ON p.id=s.principal_id WHERE s.session_id=?1 AND s.revoked_at IS NULL AND s.expires_at>?2 AND k.enabled=1 AND p.enabled=1",
            params![session_id,now],
            |row|Ok(StoredDashboardSession{session_id:row.get(0)?,principal_id:row.get(1)?,key_id:row.get(2)?,key_prefix:row.get(3)?}),
        ).optional().map_err(db)?;
        if session.is_some() {
            self.connection
                .execute(
                    "UPDATE auth_dashboard_sessions SET last_seen_at=?2 WHERE session_id=?1",
                    params![session_id, now],
                )
                .map_err(db)?;
        }
        Ok(session)
    }

    pub fn revoke_dashboard_session(
        &mut self,
        session_id: &str,
        reason: &str,
    ) -> Result<bool, String> {
        self.connection.execute("UPDATE auth_dashboard_sessions SET revoked_at=?2,revoked_reason=?3 WHERE session_id=?1 AND revoked_at IS NULL",params![session_id,Utc::now().timestamp(),reason]).map(|changed|changed>0).map_err(db)
    }

    pub fn verify_dashboard_csrf_digest(
        &self,
        session_id: &str,
        presented_digest: &[u8],
    ) -> Result<bool, String> {
        let stored = self
            .connection
            .query_row(
                "SELECT csrf_secret_digest FROM auth_dashboard_sessions WHERE session_id=?1 AND revoked_at IS NULL AND expires_at>?2",
                params![session_id, Utc::now().timestamp()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(db)?;
        let Some(stored) = stored else {
            return Ok(false);
        };
        // Compare equal-sized buffers without data-dependent early exit. A
        // length mismatch is still rejected after doing fixed work over the
        // stored digest length.
        let mut candidate = vec![0u8; stored.len()];
        let copy_len = candidate.len().min(presented_digest.len());
        candidate[..copy_len].copy_from_slice(&presented_digest[..copy_len]);
        Ok(stored.ct_eq(&candidate).into() && stored.len() == presented_digest.len())
    }

    pub fn dispatch_access(
        &mut self,
        actor: &ManagementActor,
        operation: AccessOperation,
    ) -> Result<AccessResult, AccessError> {
        match operation {
            AccessOperation::Summary => self.access_summary(actor),
            AccessOperation::ListUsers => self.list_users().map(AccessResult::Users),
            AccessOperation::CreateUser(body) => {
                self.create_user(actor, &body.kind, &body.display_name)?;
                self.list_users().map(AccessResult::Users)
            }
            AccessOperation::ListGroups => self.list_groups().map(AccessResult::Groups),
            AccessOperation::CreateGroup(body) => {
                self.create_group(actor, &body.name, &body.members)?;
                self.list_groups().map(AccessResult::Groups)
            }
            AccessOperation::ListRoles => self.list_roles().map(AccessResult::Roles),
            AccessOperation::CreateRole(body) => {
                self.create_role(actor, &body.name, &body.permissions)?;
                self.list_roles().map(AccessResult::Roles)
            }
            AccessOperation::ListPolicies => self.list_policies().map(AccessResult::Policies),
            AccessOperation::CreatePolicy(body) => {
                self.create_policy(actor, *body)?;
                self.list_policies().map(AccessResult::Policies)
            }
            AccessOperation::ListApiKeys => self.list_access_keys().map(AccessResult::ApiKeys),
            AccessOperation::RevokeApiKey(id) => {
                if !self.revoke_key_as(&id, actor).map_err(access_internal)? {
                    return Err(AccessError::new(
                        StatusCode::NOT_FOUND,
                        "API key not found or already revoked",
                    ));
                }
                self.list_access_keys().map(AccessResult::ApiKeys)
            }
            AccessOperation::ListSessions => self.list_sessions().map(AccessResult::Sessions),
            AccessOperation::RevokeSession(id) => {
                self.revoke_session(actor, &id)?;
                self.list_sessions().map(AccessResult::Sessions)
            }
            AccessOperation::Usage => self.usage().map(AccessResult::Usage),
            AccessOperation::Audit => self.audit_events().map(AccessResult::Audit),
            AccessOperation::Pricing => self.pricing().map(AccessResult::Pricing),
            AccessOperation::SyncPricing(_) => Err(AccessError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "pricing sync must be handled asynchronously by AuthzService",
            )),
            AccessOperation::WritePricing(body) => {
                self.write_pricing(actor, body)?;
                self.pricing().map(AccessResult::Pricing)
            }
            AccessOperation::CreateApiKey(_)
            | AccessOperation::RotateApiKey(_)
            | AccessOperation::UpdateApiKeyPayloadCapture(_, _) => Err(AccessError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "key secret operation must be handled by AuthzService",
            )),
        }
    }

    fn access_summary(&self, actor: &ManagementActor) -> Result<AccessResult, AccessError> {
        let count = |table: &str| -> Result<usize, AccessError> {
            let sql = format!("SELECT COUNT(*) FROM {table}");
            self.connection
                .query_row(&sql, [], |r| r.get::<_, i64>(0))
                .map(|v| v as usize)
                .map_err(access_db)
        };
        let (kind, principal_id, display_name, permissions) = match actor {
            ManagementActor::Bootstrap => (
                "bootstrap",
                None,
                "Bootstrap administrator".into(),
                WirePermission::all(),
            ),
            ManagementActor::Delegated {
                principal_id,
                permissions,
                ..
            } => (
                "delegated",
                Some(principal_id.clone()),
                principal_id.clone(),
                permissions.to_vec(),
            ),
        };
        Ok(AccessResult::Summary(AccessSummary {
            enabled: true,
            policy_epoch: current_epoch(&self.connection).map_err(access_internal)?,
            actor: ActorSummary {
                kind: kind.into(),
                principal_id,
                display_name,
                permissions,
            },
            counts: AccessCounts {
                users: count("auth_principals")?,
                groups: count("auth_groups")?,
                roles: count("auth_roles")?,
                policies: count("auth_policies")?,
                api_keys: count("auth_api_keys")?,
                active_sessions: count("auth_sessions")?,
            },
        }))
    }

    fn list_users(&self) -> Result<Vec<AccessUser>, AccessError> {
        let mut stmt=self.connection.prepare("SELECT id,kind,display_name,enabled,created_at FROM auth_principals ORDER BY created_at,id").map_err(access_db)?;
        stmt.query_map([], |r| {
            Ok(AccessUser {
                id: r.get(0)?,
                kind: r.get(1)?,
                display_name: r.get(2)?,
                enabled: r.get::<_, i64>(3)? != 0,
                created_at: timestamp(r.get(4)?),
            })
        })
        .map_err(access_db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(access_db)
    }

    fn create_user(
        &mut self,
        actor: &ManagementActor,
        kind: &str,
        name: &str,
    ) -> Result<(), AccessError> {
        if !matches!(kind, "user" | "service_account") {
            return Err(AccessError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "kind must be user or service_account",
            ));
        }
        let now = Utc::now().timestamp();
        let id = format!("usr_{}", Uuid::new_v4().simple());
        let tx = self.connection.transaction().map_err(access_db)?;
        tx.execute("INSERT INTO auth_principals(id,kind,display_name,enabled,created_at) VALUES(?1,?2,?3,1,?4)",params![id,kind,required(name,"display_name").map_err(access_internal)?,now]).map_err(access_db)?;
        bump_epoch(&tx).map_err(access_internal)?;
        audit(
            &tx,
            &actor_name(actor),
            "principal.created",
            &id,
            "ok",
            BTreeMap::new(),
        )
        .map_err(access_internal)?;
        tx.commit().map_err(access_db)
    }

    fn list_groups(&self) -> Result<Vec<AccessGroup>, AccessError> {
        let mut stmt=self.connection.prepare("SELECT g.id,g.name,g.enabled,COUNT(m.principal_id) FROM auth_groups g LEFT JOIN auth_group_members m ON m.group_id=g.id GROUP BY g.id ORDER BY g.name").map_err(access_db)?;
        stmt.query_map([], |r| {
            Ok(AccessGroup {
                id: r.get(0)?,
                name: r.get(1)?,
                enabled: r.get::<_, i64>(2)? != 0,
                member_count: r.get::<_, i64>(3)? as usize,
            })
        })
        .map_err(access_db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(access_db)
    }

    fn create_group(
        &mut self,
        actor: &ManagementActor,
        name: &str,
        members: &[String],
    ) -> Result<(), AccessError> {
        let id = format!("grp_{}", Uuid::new_v4().simple());
        let tx = self.connection.transaction().map_err(access_db)?;
        tx.execute(
            "INSERT INTO auth_groups(id,name,enabled,created_at) VALUES(?1,?2,1,?3)",
            params![
                id,
                required(name, "name").map_err(access_internal)?,
                Utc::now().timestamp()
            ],
        )
        .map_err(access_db)?;
        for member in members {
            tx.execute(
                "INSERT INTO auth_group_members(group_id,principal_id) VALUES(?1,?2)",
                params![id, member],
            )
            .map_err(access_db)?;
        }
        bump_epoch(&tx).map_err(access_internal)?;
        audit(
            &tx,
            &actor_name(actor),
            "group.created",
            &id,
            "ok",
            BTreeMap::new(),
        )
        .map_err(access_internal)?;
        tx.commit().map_err(access_db)
    }

    fn list_roles(&self) -> Result<Vec<AccessRole>, AccessError> {
        let mut stmt = self
            .connection
            .prepare("SELECT id,name,enabled FROM auth_roles ORDER BY name")
            .map_err(access_db)?;
        let base = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)? != 0,
                ))
            })
            .map_err(access_db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(access_db)?;
        base.into_iter()
            .map(|(id, name, enabled)| {
                let mut p = self
                    .connection
                    .prepare("SELECT permission FROM auth_role_permissions WHERE role_id=?1")
                    .map_err(access_db)?;
                let permissions = p
                    .query_map([&id], |r| r.get::<_, String>(0))
                    .map_err(access_db)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(access_db)?
                    .into_iter()
                    .filter_map(|v| wire_permission(&v))
                    .collect();
                Ok(AccessRole {
                    id: id.clone(),
                    name,
                    enabled,
                    permissions,
                })
            })
            .collect()
    }

    fn create_role(
        &mut self,
        actor: &ManagementActor,
        name: &str,
        permissions: &[WirePermission],
    ) -> Result<(), AccessError> {
        if let ManagementActor::Delegated {
            permissions: actor_permissions,
            ..
        } = actor
            && permissions
                .iter()
                .any(|permission| !actor_permissions.contains(permission))
        {
            self.audit_denied(actor, "role.create", "requested permissions exceed actor")?;
            return Err(AccessError::new(
                StatusCode::FORBIDDEN,
                "cannot grant management permissions the actor does not possess",
            ));
        }
        let id = format!("rol_{}", Uuid::new_v4().simple());
        let tx = self.connection.transaction().map_err(access_db)?;
        tx.execute(
            "INSERT INTO auth_roles(id,name,enabled,created_at) VALUES(?1,?2,1,?3)",
            params![
                id,
                required(name, "name").map_err(access_internal)?,
                Utc::now().timestamp()
            ],
        )
        .map_err(access_db)?;
        for permission in permissions {
            tx.execute(
                "INSERT INTO auth_role_permissions(role_id,permission) VALUES(?1,?2)",
                params![id, wire_permission_name(*permission)],
            )
            .map_err(access_db)?;
        }
        bump_epoch(&tx).map_err(access_internal)?;
        audit(
            &tx,
            &actor_name(actor),
            "role.created",
            &id,
            "ok",
            BTreeMap::new(),
        )
        .map_err(access_internal)?;
        tx.commit().map_err(access_db)
    }

    fn list_policies(&self) -> Result<Vec<AccessPolicy>, AccessError> {
        let mut stmt = self
            .connection
            .prepare("SELECT id,name,effect,enabled FROM auth_policies ORDER BY created_at,id")
            .map_err(access_db)?;
        let base = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)? != 0,
                ))
            })
            .map_err(access_db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(access_db)?;
        base.into_iter()
            .map(|(id, name, effect, enabled)| {
                let subjects = query_pairs(
                    &self.connection,
                    "SELECT subject_kind,subject_id FROM auth_policy_subjects WHERE policy_id=?1",
                    &id,
                )
                .map_err(access_internal)?
                .into_iter()
                .map(|(k, v)| format!("{k}:{v}"))
                .collect();
                let scopes = query_pairs(
                    &self.connection,
                    "SELECT dimension,matcher FROM auth_policy_scopes WHERE policy_id=?1",
                    &id,
                )
                .map_err(access_internal)?;
                let dim = |d: &str| {
                    scopes
                        .iter()
                        .filter(|(kind, _)| kind == d)
                        .map(|(_, v)| v.clone())
                        .collect()
                };
                let limits = self.query_limits(&id).map_err(access_internal)?;
                Ok(AccessPolicy {
                    id: id.clone(),
                    name,
                    effect,
                    enabled,
                    subjects,
                    endpoints: dim("endpoint"),
                    models: dim("requested_model"),
                    requested_models: dim("requested_model"),
                    served_models: dim("served_model"),
                    providers: dim("provider"),
                    routes: dim("route"),
                    time_windows: self
                        .query_windows(&id)
                        .map_err(access_internal)?
                        .into_iter()
                        .map(|window| AccessTimeWindow {
                            weekday_mask: window.weekday_mask,
                            start_minute: window.start_minute,
                            end_minute: window.end_minute,
                            absolute_start_ms: window.absolute_start_ms,
                            absolute_end_ms: window.absolute_end_ms,
                        })
                        .collect(),
                    max_concurrent_sessions: limits.max_concurrent_sessions,
                    max_daily_session_starts: limits.max_daily_session_starts,
                    management_permissions: self
                        .query_permissions(&id)
                        .map_err(access_internal)?
                        .into_iter()
                        .filter_map(crate::authz::access::wire_permission)
                        .collect(),
                })
            })
            .collect()
    }

    fn create_policy(
        &mut self,
        actor: &ManagementActor,
        body: crate::dashboard_access::CreatePolicyRequest,
    ) -> Result<(), AccessError> {
        if !matches!(body.effect.as_str(), "allow" | "deny") {
            return Err(AccessError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "effect must be allow or deny",
            ));
        }
        if body.subjects.is_empty() {
            return Err(AccessError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "at least one subject is required",
            ));
        }
        if let ManagementActor::Delegated {
            principal_id,
            key_id,
            permissions: actor_permissions,
            ..
        } = actor
        {
            let owns_every_subject = body.subjects.iter().all(|subject| {
                subject == &format!("principal:{principal_id}")
                    || subject == &format!("key:{key_id}")
            });
            let permissions_are_subset = body
                .management_permissions
                .iter()
                .all(|permission| actor_permissions.contains(permission));
            let grants_inference_scope = body.effect == "allow"
                && (!body.endpoints.is_empty()
                    || !body.models.is_empty()
                    || !body.requested_models.is_empty()
                    || !body.served_models.is_empty()
                    || !body.providers.is_empty()
                    || !body.routes.is_empty());
            if !owns_every_subject || !permissions_are_subset || grants_inference_scope {
                self.audit_denied(actor, "policy.create", "requested grant exceeds actor")?;
                return Err(AccessError::new(
                    StatusCode::FORBIDDEN,
                    "cannot create a policy that expands the actor's privileges",
                ));
            }
        }
        for window in &body.time_windows {
            if window.weekday_mask > 0x7f
                || window.start_minute > 1439
                || window.end_minute > 1439
                || matches!((window.absolute_start_ms, window.absolute_end_ms), (Some(start), Some(end)) if start >= end)
            {
                return Err(AccessError::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid UTC policy window",
                ));
            }
        }
        let requested_models = if body.requested_models.is_empty() {
            body.models.clone()
        } else {
            body.requested_models.clone()
        };
        let served_models = if body.served_models.is_empty() {
            body.models.clone()
        } else {
            body.served_models.clone()
        };
        let id = format!("pol_{}", Uuid::new_v4().simple());
        let now = Utc::now().timestamp();
        let tx = self.connection.transaction().map_err(access_db)?;
        tx.execute("INSERT INTO auth_policies(id,name,effect,enabled,created_at,updated_at) VALUES(?1,?2,?3,1,?4,?4)",params![id,required(&body.name,"name").map_err(access_internal)?,body.effect,now]).map_err(access_db)?;
        for subject in body.subjects {
            let (kind, value) = subject.split_once(':').ok_or_else(|| {
                AccessError::new(StatusCode::UNPROCESSABLE_ENTITY, "subject must be kind:id")
            })?;
            if !matches!(kind, "key" | "principal" | "group" | "role") {
                return Err(AccessError::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid subject kind",
                ));
            }
            tx.execute("INSERT INTO auth_policy_subjects(policy_id,subject_kind,subject_id) VALUES(?1,?2,?3)",params![id,kind,value]).map_err(access_db)?;
        }
        for (endpoint, dimension) in body
            .endpoints
            .into_iter()
            .map(|v| (v, "endpoint"))
            .chain(requested_models.into_iter().map(|v| (v, "requested_model")))
            .chain(served_models.into_iter().map(|v| (v, "served_model")))
            .chain(body.providers.into_iter().map(|v| (v, "provider")))
            .chain(body.routes.into_iter().map(|v| (v, "route")))
        {
            tx.execute(
                "INSERT INTO auth_policy_scopes(policy_id,dimension,matcher) VALUES(?1,?2,?3)",
                params![id, dimension, endpoint],
            )
            .map_err(access_db)?;
        }
        for window in body.time_windows {
            tx.execute(
                "INSERT INTO auth_time_windows(id,policy_id,weekday_mask,start_minute,end_minute,absolute_start,absolute_end) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![format!("win_{}", Uuid::new_v4().simple()), id, window.weekday_mask, window.start_minute, window.end_minute, window.absolute_start_ms, window.absolute_end_ms],
            )
            .map_err(access_db)?;
        }
        if body.max_concurrent_sessions.is_some() || body.max_daily_session_starts.is_some() {
            tx.execute(
                "INSERT INTO auth_limits(policy_id,max_concurrent_sessions,max_daily_session_starts) VALUES(?1,?2,?3)",
                params![id, body.max_concurrent_sessions, body.max_daily_session_starts],
            )
            .map_err(access_db)?;
        }
        for permission in body.management_permissions {
            tx.execute(
                "INSERT INTO auth_management_permissions(policy_id,permission) VALUES(?1,?2)",
                params![id, wire_permission_name(permission)],
            )
            .map_err(access_db)?;
        }
        bump_epoch(&tx).map_err(access_internal)?;
        audit(
            &tx,
            &actor_name(actor),
            "policy.created",
            &id,
            "ok",
            BTreeMap::new(),
        )
        .map_err(access_internal)?;
        tx.commit().map_err(access_db)
    }

    fn audit_denied(
        &mut self,
        actor: &ManagementActor,
        action: &str,
        reason: &str,
    ) -> Result<(), AccessError> {
        let tx = self.connection.transaction().map_err(access_db)?;
        let mut metadata = BTreeMap::new();
        metadata.insert("reason".into(), Value::String(reason.into()));
        audit(&tx, &actor_name(actor), action, "", "denied", metadata).map_err(access_internal)?;
        tx.commit().map_err(access_db)
    }

    pub(crate) fn list_access_keys(&self) -> Result<Vec<AccessApiKey>, AccessError> {
        Ok(self
            .list_keys()
            .map_err(access_internal)?
            .into_iter()
            .map(access_key)
            .collect())
    }

    fn list_sessions(&self) -> Result<Vec<AccessSession>, AccessError> {
        let mut stmt=self.connection.prepare("SELECT s.id,'inference',k.principal_id,s.key_id,s.endpoint,s.requested_model,s.started_at,s.expires_at FROM auth_sessions s JOIN auth_api_keys k ON k.id=s.key_id UNION ALL SELECT d.session_id,'dashboard',d.principal_id,d.key_id,NULL,NULL,d.created_at,d.expires_at FROM auth_dashboard_sessions d WHERE d.revoked_at IS NULL ORDER BY 7 DESC").map_err(access_db)?;
        stmt.query_map([], |r| {
            Ok(AccessSession {
                id: r.get(0)?,
                kind: r.get(1)?,
                principal_id: r.get(2)?,
                key_id: r.get(3)?,
                endpoint: r.get(4)?,
                requested_model: r.get(5)?,
                started_at: timestamp(r.get(6)?),
                expires_at: r.get::<_, Option<i64>>(7)?.map(timestamp),
            })
        })
        .map_err(access_db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(access_db)
    }
    fn revoke_session(&mut self, actor: &ManagementActor, id: &str) -> Result<(), AccessError> {
        let deleted = self
            .connection
            .execute("DELETE FROM auth_sessions WHERE id=?1", [id])
            .map_err(access_db)?;
        let revoked = self.connection.execute("UPDATE auth_dashboard_sessions SET revoked_at=?2,revoked_reason='admin_revoked' WHERE session_id=?1 AND revoked_at IS NULL",params![id,Utc::now().timestamp()]).map_err(access_db)?;
        if deleted + revoked == 0 {
            return Err(AccessError::new(StatusCode::NOT_FOUND, "session not found"));
        }
        let tx = self.connection.transaction().map_err(access_db)?;
        audit(
            &tx,
            &actor_name(actor),
            "session.revoked",
            id,
            "ok",
            BTreeMap::new(),
        )
        .map_err(access_internal)?;
        tx.commit().map_err(access_db)
    }

    fn usage(&self) -> Result<Vec<AccessUsageRow>, AccessError> {
        let mut stmt=self.connection.prepare("SELECT 'key',key_id,COUNT(*),SUM(prompt_tokens),SUM(completion_tokens),SUM(cached_tokens),SUM(reasoning_tokens),SUM(cost_nano_usd),CASE WHEN SUM(CASE WHEN cost_confidence='estimated' THEN 1 ELSE 0 END)>0 THEN 'estimated' WHEN SUM(CASE WHEN cost_confidence='confident' THEN 1 ELSE 0 END)>0 THEN 'confident' ELSE 'unavailable' END FROM auth_usage_events GROUP BY key_id ORDER BY COUNT(*) DESC").map_err(access_db)?;
        stmt.query_map([], |r| {
            Ok(AccessUsageRow {
                dimension: r.get(0)?,
                value: r.get(1)?,
                requests: r.get::<_, i64>(2)? as u64,
                prompt_tokens: r.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                completion_tokens: r.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                cached_tokens: r.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                reasoning_tokens: r.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                cost: r
                    .get::<_, Option<i64>>(7)?
                    .map(|v| v as f64 / 1_000_000_000.0),
                cost_confidence: r.get(8)?,
            })
        })
        .map_err(access_db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(access_db)
    }

    fn audit_events(&self) -> Result<Vec<AccessAuditEvent>, AccessError> {
        let mut stmt=self.connection.prepare("SELECT id,created_at,actor,action,target,outcome,metadata_json FROM auth_audit_events ORDER BY created_at DESC,id DESC LIMIT 500").map_err(access_db)?;
        stmt.query_map([], |r| {
            let raw: String = r.get(6)?;
            let metadata = serde_json::from_str(&raw).unwrap_or_default();
            Ok(AccessAuditEvent {
                id: r.get::<_, i64>(0)?.to_string(),
                timestamp: timestamp(r.get(1)?),
                actor: r.get(2)?,
                action: r.get(3)?,
                target: r.get(4)?,
                outcome: r.get(5)?,
                metadata,
            })
        })
        .map_err(access_db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(access_db)
    }
    fn pricing(&self) -> Result<Vec<AccessPricingRow>, AccessError> {
        let mut stmt=self.connection.prepare("SELECT model,provider,source,fetched_at,input_per_1k,output_per_1k,confidence FROM auth_price_snapshots ORDER BY model,provider").map_err(access_db)?;
        let mut rows = stmt
            .query_map([], |r| {
                Ok(AccessPricingRow {
                    model: r.get(0)?,
                    provider: r.get(1)?,
                    source: r.get(2)?,
                    source_url: None,
                    fetched_at: timestamp(r.get(3)?),
                    input_per_1k: r.get(4)?,
                    output_per_1k: r.get(5)?,
                    input_min_per_1k: None,
                    input_max_per_1k: None,
                    output_min_per_1k: None,
                    output_max_per_1k: None,
                    confidence: r.get(6)?,
                })
            })
            .map_err(access_db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(access_db)?;
        let mut imported = self
            .connection
            .prepare(
                "SELECT model_id,endpoint_id,source,source_url,fetched_at_ms,
                    prompt_mean_nano_usd,completion_mean_nano_usd,
                    prompt_min_nano_usd,prompt_max_nano_usd,
                    completion_min_nano_usd,completion_max_nano_usd,confidence
             FROM auth_imported_price_snapshots p
             WHERE fetched_at_ms=(SELECT MAX(fetched_at_ms) FROM auth_imported_price_snapshots WHERE model_id=p.model_id)
             ORDER BY model_id,endpoint_id LIMIT 2000",
            )
            .map_err(access_db)?;
        let imported_rows = imported
            .query_map([], |r| {
                let price = |column| -> rusqlite::Result<String> {
                    Ok(format!("{}", r.get::<_, i64>(column)? as f64 / 1_000_000.0))
                };
                Ok(AccessPricingRow {
                    model: r.get(0)?,
                    provider: r.get(1)?,
                    source: r.get(2)?,
                    source_url: Some(r.get(3)?),
                    fetched_at: timestamp(r.get::<_, i64>(4)? / 1_000),
                    input_per_1k: price(5)?,
                    output_per_1k: price(6)?,
                    input_min_per_1k: Some(price(7)?),
                    input_max_per_1k: Some(price(8)?),
                    output_min_per_1k: Some(price(9)?),
                    output_max_per_1k: Some(price(10)?),
                    confidence: r.get(11)?,
                })
            })
            .map_err(access_db)?;
        rows.extend(
            imported_rows
                .collect::<Result<Vec<_>, _>>()
                .map_err(access_db)?,
        );
        Ok(rows)
    }
    fn write_pricing(
        &mut self,
        actor: &ManagementActor,
        body: crate::dashboard_access::WritePricingRequest,
    ) -> Result<(), AccessError> {
        let now = Utc::now().timestamp();
        let tx = self.connection.transaction().map_err(access_db)?;
        for row in body.pricing {
            validate_decimal(&row.input_per_1k)?;
            validate_decimal(&row.output_per_1k)?;
            tx.execute("INSERT INTO auth_price_snapshots(model,provider,source,fetched_at,input_per_1k,output_per_1k,confidence) VALUES(?1,?2,'operator',?3,?4,?5,'confident') ON CONFLICT(model,provider) DO UPDATE SET source='operator',fetched_at=excluded.fetched_at,input_per_1k=excluded.input_per_1k,output_per_1k=excluded.output_per_1k,confidence='confident'",params![row.model,row.provider,now,row.input_per_1k,row.output_per_1k]).map_err(access_db)?;
        }
        audit(
            &tx,
            &actor_name(actor),
            "pricing.written",
            "pricing",
            "ok",
            BTreeMap::new(),
        )
        .map_err(access_internal)?;
        tx.commit().map_err(access_db)
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_key(
    tx: &Transaction<'_>,
    principal_id: &str,
    name: &str,
    raw: &str,
    digest: &[u8; 32],
    expires_at: Option<i64>,
    capture_payloads: bool,
    now: i64,
) -> Result<CreatedApiKey, String> {
    let id = format!("key_{}", Uuid::new_v4().simple());
    let prefix = display_prefix(raw);
    tx.execute("INSERT INTO auth_api_keys(id,principal_id,name,prefix,hmac_sha256_digest,enabled,created_at,expires_at,capture_payloads) VALUES(?1,?2,?3,?4,?5,1,?6,?7,?8)",params![id,principal_id,required(name,"name")?,prefix,digest.as_slice(),now,expires_at,i64::from(capture_payloads)]).map_err(db)?;
    Ok(CreatedApiKey {
        summary: ApiKeySummary {
            id,
            principal_id: principal_id.into(),
            name: name.trim().into(),
            prefix,
            enabled: true,
            created_at: now,
            expires_at,
            last_used_at: None,
            capture_payloads,
        },
        raw_key: None,
    })
}
fn query_key(tx: &Transaction<'_>, id: &str) -> Result<Option<ApiKeySummary>, String> {
    tx.query_row("SELECT id,principal_id,name,prefix,enabled,created_at,expires_at,last_used_at,capture_payloads FROM auth_api_keys WHERE id=?1",[id],map_key).optional().map_err(db)
}
fn map_key(r: &rusqlite::Row<'_>) -> rusqlite::Result<ApiKeySummary> {
    Ok(ApiKeySummary {
        id: r.get(0)?,
        principal_id: r.get(1)?,
        name: r.get(2)?,
        prefix: r.get(3)?,
        enabled: r.get::<_, i64>(4)? != 0,
        created_at: r.get(5)?,
        expires_at: r.get(6)?,
        last_used_at: r.get(7)?,
        capture_payloads: r.get::<_, i64>(8)? != 0,
    })
}
fn access_key(k: ApiKeySummary) -> AccessApiKey {
    AccessApiKey {
        id: k.id,
        principal_id: k.principal_id,
        name: k.name,
        prefix: k.prefix,
        enabled: k.enabled,
        created_at: timestamp(k.created_at),
        expires_at: k.expires_at.map(timestamp),
        last_used_at: k.last_used_at.map(timestamp),
        capture_payloads: k.capture_payloads,
    }
}

fn migrate_api_key_payload_capture(connection: &Connection) -> rusqlite::Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(auth_api_keys)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<HashSet<_>, _>>()?;
    if !columns.contains("capture_payloads") {
        connection.execute(
            "ALTER TABLE auth_api_keys ADD COLUMN capture_payloads INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}
fn parse_subject(kind: &str, id: String) -> Result<PolicySubject, String> {
    match kind {
        "key" => Ok(PolicySubject::Key(id)),
        "principal" => Ok(PolicySubject::Principal(id)),
        "group" => Ok(PolicySubject::Group(id)),
        "role" => Ok(PolicySubject::Role(id)),
        _ => Err(format!("invalid subject kind {kind}")),
    }
}
fn query_pairs(conn: &Connection, sql: &str, id: &str) -> Result<Vec<(String, String)>, String> {
    let mut stmt = conn.prepare(sql).map_err(db)?;
    stmt.query_map([id], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)
}
fn current_epoch(conn: &Connection) -> Result<u64, String> {
    let v: i64 = conn
        .query_row(
            "SELECT epoch FROM auth_policy_epoch WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .map_err(db)?;
    u64::try_from(v).map_err(|_| "invalid policy epoch".into())
}
fn bump_epoch(tx: &Transaction<'_>) -> Result<(), String> {
    tx.execute(
        "UPDATE auth_policy_epoch SET epoch=epoch+1 WHERE singleton=1",
        [],
    )
    .map(|_| ())
    .map_err(db)
}
fn audit(
    tx: &Transaction<'_>,
    actor: &str,
    action: &str,
    target: &str,
    outcome: &str,
    metadata: BTreeMap<String, Value>,
) -> Result<(), String> {
    tx.execute("INSERT INTO auth_audit_events(actor,action,target,outcome,metadata_json,created_at) VALUES(?1,?2,?3,?4,?5,?6)",params![actor,action,target,outcome,serde_json::to_string(&metadata).map_err(|e|e.to_string())?,Utc::now().timestamp()]).map(|_|()).map_err(db)
}
fn actor_name(actor: &ManagementActor) -> String {
    match actor {
        ManagementActor::Bootstrap => "bootstrap".into(),
        ManagementActor::Delegated {
            principal_id,
            key_id,
            ..
        } => format!("{principal_id}:{key_id}"),
    }
}
fn timestamp(seconds: i64) -> String {
    DateTime::from_timestamp(seconds, 0)
        .map(|v| v.to_rfc3339())
        .unwrap_or_else(|| seconds.to_string())
}
fn display_prefix(raw: &str) -> String {
    raw.chars().take(12).collect()
}
fn required<'a>(v: &'a str, field: &str) -> Result<&'a str, String> {
    let v = v.trim();
    if v.is_empty() {
        Err(format!("{field} must not be blank"))
    } else {
        Ok(v)
    }
}
fn nonempty_or_wildcard(values: &[String]) -> Vec<String> {
    if values.is_empty() {
        vec!["*".into()]
    } else {
        values.to_vec()
    }
}
fn db(e: rusqlite::Error) -> String {
    e.to_string()
}
fn access_db(e: rusqlite::Error) -> AccessError {
    tracing::error!(error = %e, "authorization database operation failed");
    AccessError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
}
fn access_internal(e: String) -> AccessError {
    tracing::error!(error = %e, "authorization management operation failed");
    AccessError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
}

fn validate_decimal(v: &str) -> Result<(), AccessError> {
    if v.parse::<f64>()
        .ok()
        .is_some_and(|n| n.is_finite() && n >= 0.0)
    {
        Ok(())
    } else {
        Err(AccessError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "price must be a non-negative decimal",
        ))
    }
}

fn wire_permission_name(permission: WirePermission) -> &'static str {
    match permission {
        WirePermission::KeysRead => "auth.keys.read",
        WirePermission::KeysCreate => "auth.keys.create",
        WirePermission::KeysRevoke => "auth.keys.revoke",
        WirePermission::KeysRotate => "auth.keys.rotate",
        WirePermission::PrincipalsRead => "auth.principals.read",
        WirePermission::PrincipalsWrite => "auth.principals.write",
        WirePermission::GroupsRead => "auth.groups.read",
        WirePermission::GroupsWrite => "auth.groups.write",
        WirePermission::RolesRead => "auth.roles.read",
        WirePermission::RolesWrite => "auth.roles.write",
        WirePermission::PoliciesRead => "auth.policies.read",
        WirePermission::PoliciesWrite => "auth.policies.write",
        WirePermission::UsageRead => "auth.usage.read",
        WirePermission::AuditRead => "auth.audit.read",
        WirePermission::PricingRead => "auth.pricing.read",
        WirePermission::PricingSync => "auth.pricing.sync",
        WirePermission::PricingWrite => "auth.pricing.write",
        WirePermission::SessionsRead => "auth.sessions.read",
        WirePermission::SessionsTerminate => "auth.sessions.terminate",
        WirePermission::FleetModelsRead => "fleet.models.read",
        WirePermission::FleetModelsLoad => "fleet.models.load",
        WirePermission::FleetModelsUnload => "fleet.models.unload",
    }
}
fn wire_permission(v: &str) -> Option<WirePermission> {
    WirePermission::all()
        .into_iter()
        .find(|p| wire_permission_name(*p) == v)
}

const SCHEMA: &str = r#"
PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS auth_principals(id TEXT PRIMARY KEY,kind TEXT NOT NULL CHECK(kind IN('user','service_account')),display_name TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_api_keys(id TEXT PRIMARY KEY,principal_id TEXT NOT NULL REFERENCES auth_principals(id),name TEXT NOT NULL,prefix TEXT NOT NULL,hmac_sha256_digest BLOB NOT NULL UNIQUE,enabled INTEGER NOT NULL DEFAULT 1,created_at INTEGER NOT NULL,expires_at INTEGER,last_used_at INTEGER,revoked_at INTEGER,capture_payloads INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS auth_api_keys_prefix_idx ON auth_api_keys(prefix);
CREATE TABLE IF NOT EXISTS auth_groups(id TEXT PRIMARY KEY,name TEXT NOT NULL UNIQUE,enabled INTEGER NOT NULL DEFAULT 1,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_group_members(group_id TEXT NOT NULL REFERENCES auth_groups(id) ON DELETE CASCADE,principal_id TEXT NOT NULL REFERENCES auth_principals(id) ON DELETE CASCADE,PRIMARY KEY(group_id,principal_id));
CREATE TABLE IF NOT EXISTS auth_roles(id TEXT PRIMARY KEY,name TEXT NOT NULL UNIQUE,enabled INTEGER NOT NULL DEFAULT 1,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_role_bindings(role_id TEXT NOT NULL REFERENCES auth_roles(id) ON DELETE CASCADE,subject_kind TEXT NOT NULL CHECK(subject_kind IN('principal','group')),subject_id TEXT NOT NULL,PRIMARY KEY(role_id,subject_kind,subject_id));
CREATE TABLE IF NOT EXISTS auth_role_permissions(role_id TEXT NOT NULL REFERENCES auth_roles(id) ON DELETE CASCADE,permission TEXT NOT NULL,PRIMARY KEY(role_id,permission));
CREATE TABLE IF NOT EXISTS auth_policies(id TEXT PRIMARY KEY,name TEXT NOT NULL,effect TEXT NOT NULL CHECK(effect IN('allow','deny')),enabled INTEGER NOT NULL DEFAULT 1,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_policy_subjects(policy_id TEXT NOT NULL REFERENCES auth_policies(id) ON DELETE CASCADE,subject_kind TEXT NOT NULL CHECK(subject_kind IN('key','principal','group','role')),subject_id TEXT NOT NULL,PRIMARY KEY(policy_id,subject_kind,subject_id));
CREATE TABLE IF NOT EXISTS auth_policy_scopes(policy_id TEXT NOT NULL REFERENCES auth_policies(id) ON DELETE CASCADE,dimension TEXT NOT NULL CHECK(dimension IN('endpoint','requested_model','served_model','provider','route')),matcher TEXT NOT NULL,PRIMARY KEY(policy_id,dimension,matcher));
CREATE TABLE IF NOT EXISTS auth_management_permissions(policy_id TEXT NOT NULL REFERENCES auth_policies(id) ON DELETE CASCADE,permission TEXT NOT NULL,PRIMARY KEY(policy_id,permission));
CREATE TABLE IF NOT EXISTS auth_time_windows(id TEXT PRIMARY KEY,policy_id TEXT NOT NULL REFERENCES auth_policies(id) ON DELETE CASCADE,weekday_mask INTEGER NOT NULL DEFAULT 0,start_minute INTEGER NOT NULL DEFAULT 0,end_minute INTEGER NOT NULL DEFAULT 0,absolute_start INTEGER,absolute_end INTEGER);
CREATE TABLE IF NOT EXISTS auth_limits(policy_id TEXT PRIMARY KEY REFERENCES auth_policies(id) ON DELETE CASCADE,max_concurrent_sessions INTEGER,max_daily_session_starts INTEGER,max_tokens_per_day INTEGER,max_cost_nanos_per_day INTEGER);
CREATE TABLE IF NOT EXISTS auth_sessions(id TEXT PRIMARY KEY,key_id TEXT NOT NULL REFERENCES auth_api_keys(id),started_at INTEGER NOT NULL,expires_at INTEGER,endpoint TEXT NOT NULL,requested_model TEXT);
CREATE TABLE IF NOT EXISTS auth_dashboard_sessions(session_id TEXT PRIMARY KEY,principal_id TEXT NOT NULL REFERENCES auth_principals(id),key_id TEXT NOT NULL REFERENCES auth_api_keys(id),csrf_secret_digest BLOB NOT NULL,created_at INTEGER NOT NULL,expires_at INTEGER NOT NULL,policy_epoch_at_login INTEGER NOT NULL,revoked_at INTEGER,revoked_reason TEXT,last_seen_at INTEGER);
CREATE TABLE IF NOT EXISTS auth_usage_events(auth_request_id TEXT PRIMARY KEY,api_call_id TEXT,key_id TEXT NOT NULL,principal_id TEXT NOT NULL,endpoint TEXT NOT NULL,requested_model TEXT,served_model TEXT,provider TEXT,route TEXT,status TEXT NOT NULL,prompt_tokens INTEGER,completion_tokens INTEGER,total_tokens INTEGER,cached_tokens INTEGER,reasoning_tokens INTEGER,cost_nano_usd INTEGER,cost_confidence TEXT NOT NULL DEFAULT 'unavailable',created_at_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_audit_events(id INTEGER PRIMARY KEY AUTOINCREMENT,actor TEXT NOT NULL,action TEXT NOT NULL,target TEXT NOT NULL,outcome TEXT NOT NULL,metadata_json TEXT NOT NULL DEFAULT '{}',created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS auth_price_snapshots(model TEXT NOT NULL,provider TEXT NOT NULL,source TEXT NOT NULL,fetched_at INTEGER NOT NULL,input_per_1k TEXT NOT NULL,output_per_1k TEXT NOT NULL,confidence TEXT NOT NULL,PRIMARY KEY(model,provider));
CREATE TABLE IF NOT EXISTS auth_policy_epoch(singleton INTEGER PRIMARY KEY CHECK(singleton=1),epoch INTEGER NOT NULL);
INSERT OR IGNORE INTO auth_policy_epoch(singleton,epoch) VALUES(1,1);
"#;

trait WirePermissionSet {
    fn all() -> Vec<WirePermission>;
}
impl WirePermissionSet for WirePermission {
    fn all() -> Vec<WirePermission> {
        vec![
            WirePermission::KeysRead,
            WirePermission::KeysCreate,
            WirePermission::KeysRevoke,
            WirePermission::KeysRotate,
            WirePermission::PrincipalsRead,
            WirePermission::PrincipalsWrite,
            WirePermission::GroupsRead,
            WirePermission::GroupsWrite,
            WirePermission::RolesRead,
            WirePermission::RolesWrite,
            WirePermission::PoliciesRead,
            WirePermission::PoliciesWrite,
            WirePermission::UsageRead,
            WirePermission::AuditRead,
            WirePermission::PricingRead,
            WirePermission::PricingSync,
            WirePermission::PricingWrite,
            WirePermission::SessionsRead,
            WirePermission::SessionsTerminate,
            WirePermission::FleetModelsRead,
            WirePermission::FleetModelsLoad,
            WirePermission::FleetModelsUnload,
        ]
    }
}
