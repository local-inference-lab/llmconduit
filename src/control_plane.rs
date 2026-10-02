//! Lossless control-plane configuration layered over the upstream gateway config.
//!
//! The gateway's [`PersistedConfig`] remains the owner of inference settings.  This
//! module deliberately does not mirror those fields: the original YAML mapping is
//! retained and operational saves replace only the namespaced `control_plane` key.
//! That keeps both newly-added upstream fields and as-yet unknown top-level keys
//! intact across a control-plane edit.

use crate::config::{Config, PersistedConfig, PersistedModelProfile};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use url::Url;
use uuid::Uuid;

pub const CONTROL_PLANE_KEY: &str = "control_plane";
pub const DEFAULT_CONVERSATION_ID_HEADER: &str = "x-conversation-id";

/// Whether a configured conversation header is a known credential carrier.
/// Persisting one of these values as a conversation id would leak a client key
/// into history even though request-body/header capture is otherwise redacted.
pub fn is_sensitive_conversation_header(name: &str) -> bool {
    crate::redaction::is_sensitive_payload_key(name)
}

/// A parsed YAML document with an immutable, typed view of the upstream config.
///
/// `gateway_yaml` is the authority when serializing. `gateway` is intentionally
/// exposed only by shared reference: reconstructing the YAML from the typed value
/// would discard fields unknown to this build. Code that needs to change gateway
/// settings should use the upstream config writer; control-plane code changes only
/// [`ControlPlaneSection`].
#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    gateway: PersistedConfig,
    gateway_yaml: YamlMapping,
    control_plane: ControlPlaneSection,
    had_control_plane: bool,
    migrated_legacy_root: bool,
}

impl ControlPlaneConfig {
    pub fn from_yaml_str(source: &str) -> Result<Self, String> {
        let value: YamlValue = serde_yaml::from_str(source)
            .map_err(|err| format!("failed to parse control-plane config: {err}"))?;
        let (value, migrated_legacy_root) = migrate_legacy_root_control_plane(value)?;
        let mut config = Self::from_yaml_value(value)?;
        config.migrated_legacy_root = migrated_legacy_root;
        Ok(config)
    }

    pub fn from_gateway(
        gateway: PersistedConfig,
        control_plane: ControlPlaneSection,
    ) -> Result<Self, String> {
        if let Some(operational) = &control_plane.operational {
            operational.validate()?;
        }
        let value = serde_yaml::to_value(&gateway)
            .map_err(|err| format!("failed to serialize gateway config: {err}"))?;
        let YamlValue::Mapping(gateway_yaml) = value else {
            return Err("serialized gateway config was not a YAML mapping".to_string());
        };
        Ok(Self {
            gateway,
            gateway_yaml,
            had_control_plane: control_plane != ControlPlaneSection::default(),
            migrated_legacy_root: false,
            control_plane,
        })
    }

    fn from_yaml_value(value: YamlValue) -> Result<Self, String> {
        let YamlValue::Mapping(mut gateway_yaml) = value else {
            return Err("control-plane config root must be a YAML mapping".to_string());
        };
        let key = YamlValue::String(CONTROL_PLANE_KEY.to_string());
        let section_value = gateway_yaml.remove(&key);
        let had_control_plane = section_value.is_some();
        let control_plane = match section_value {
            Some(YamlValue::Null) | None => ControlPlaneSection::default(),
            Some(value) => serde_yaml::from_value(value)
                .map_err(|err| format!("failed to parse `{CONTROL_PLANE_KEY}`: {err}"))?,
        };
        if let Some(operational) = &control_plane.operational {
            operational.validate()?;
        }
        let gateway = serde_yaml::from_value(YamlValue::Mapping(gateway_yaml.clone()))
            .map_err(|err| format!("failed to parse gateway config: {err}"))?;
        Ok(Self {
            gateway,
            gateway_yaml,
            control_plane,
            had_control_plane,
            migrated_legacy_root: false,
        })
    }

    pub fn gateway(&self) -> &PersistedConfig {
        &self.gateway
    }

    /// Replace all upstream-known gateway fields while retaining raw top-level
    /// keys unknown to this build and the namespaced control-plane section.
    pub fn replace_gateway(&mut self, gateway: PersistedConfig) -> Result<(), String> {
        let old_known = serde_yaml::to_value(&self.gateway)
            .map_err(|err| format!("failed to serialize existing gateway config: {err}"))?;
        let new_known = serde_yaml::to_value(&gateway)
            .map_err(|err| format!("failed to serialize gateway config: {err}"))?;
        let (YamlValue::Mapping(old_known), YamlValue::Mapping(new_known)) = (old_known, new_known)
        else {
            return Err("serialized gateway config was not a YAML mapping".to_string());
        };
        for key in old_known.keys() {
            self.gateway_yaml.remove(key);
        }
        for (key, value) in new_known {
            self.gateway_yaml.insert(key, value);
        }
        self.gateway = gateway;
        Ok(())
    }

    pub fn control_plane(&self) -> &ControlPlaneSection {
        &self.control_plane
    }

    /// True when this in-memory document was upgraded from the pre-namespace
    /// root schema. Callers can warn until an atomic rewrite persists it.
    pub fn migrated_legacy_root(&self) -> bool {
        self.migrated_legacy_root
    }

    pub fn control_plane_mut(&mut self) -> &mut ControlPlaneSection {
        &mut self.control_plane
    }

    pub fn replace_operational(&mut self, operational: OperationalConfig) -> Result<(), String> {
        operational.validate()?;
        self.control_plane.operational = Some(operational);
        Ok(())
    }

    /// Convert legacy plaintext virtual keys in an already-namespaced document
    /// into the canonical digest representation. This is intentionally an
    /// explicit-write migration seam: normal startup remains fail-closed and
    /// rejects plaintext keys through `validate_persisted_key_digests`.
    ///
    /// Returns whether the serialized document changed. Valid digests are also
    /// normalized to lowercase so a successful migration always leaves the
    /// portable `sha256:<lowercase hex>` form on disk.
    pub fn migrate_plaintext_key_digests(&mut self) -> Result<bool, String> {
        let Some(operational) = self.control_plane.operational.as_mut() else {
            return Ok(false);
        };
        operational.validate()?;

        let mut migrated = false;
        for key in &mut operational.keys {
            let stored = key.key.trim();
            match crate::client_auth::parse_secret_hash(stored) {
                Ok(_) => {
                    let canonical = stored.to_ascii_lowercase();
                    if key.key != canonical {
                        key.key = canonical;
                        migrated = true;
                    }
                }
                Err(_) => {
                    key.key = crate::client_auth::hash_secret(stored);
                    migrated = true;
                }
            }
        }
        Ok(migrated)
    }

    /// Materialize the typed gateway profile overlay and the exact alias route
    /// plans consumed by the existing routing/failover stack.
    pub fn materialize(&self) -> Result<MaterializedControlPlane, String> {
        match &self.control_plane.operational {
            Some(operational) => {
                let gateway = operational.apply_profiles_to(&self.gateway)?;
                let routes = operational.route_plans(&gateway)?;
                Ok(MaterializedControlPlane { gateway, routes })
            }
            None => Ok(MaterializedControlPlane {
                gateway: self.gateway.clone(),
                routes: Vec::new(),
            }),
        }
    }

    pub fn to_yaml_value(&self) -> Result<YamlValue, String> {
        let mut root = self.gateway_yaml.clone();
        if self.had_control_plane || self.control_plane != ControlPlaneSection::default() {
            let section = serde_yaml::to_value(&self.control_plane)
                .map_err(|err| format!("failed to serialize `{CONTROL_PLANE_KEY}`: {err}"))?;
            root.insert(YamlValue::String(CONTROL_PLANE_KEY.to_string()), section);
        }
        Ok(YamlValue::Mapping(root))
    }

    pub fn to_yaml_string(&self) -> Result<String, String> {
        serde_yaml::to_string(&self.to_yaml_value()?)
            .map_err(|err| format!("failed to serialize control-plane config: {err}"))
    }

    /// Persist the lossless YAML document with owner-only permissions on Unix.
    /// TOML remains read-only, matching the upstream config writer contract.
    pub fn write_to_path(&self, path: &Path) -> Result<(), String> {
        if crate::config::path_is_toml(path) {
            return Err(format!(
                "cannot write control-plane config to read-only TOML path {}",
                path.display()
            ));
        }
        let parent = path
            .parent()
            .ok_or_else(|| format!("config path has no parent: {}", path.display()))?;
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create {}: {err}", parent.display()))?;
        let yaml = self.to_yaml_string()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            let filename = path
                .file_name()
                .ok_or_else(|| format!("config path has no filename: {}", path.display()))?
                .to_string_lossy();
            let temporary = parent.join(format!(".{filename}.{}.tmp", Uuid::new_v4().as_simple()));
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true).mode(0o600);
            let mut file = options
                .open(&temporary)
                .map_err(|err| format!("failed to create temporary config: {err}"))?;
            let write_result = (|| -> std::io::Result<()> {
                file.write_all(yaml.as_bytes())?;
                file.set_permissions(fs::Permissions::from_mode(0o600))?;
                file.sync_all()?;
                drop(file);
                fs::rename(&temporary, path)?;
                fs::File::open(parent)?.sync_all()?;
                Ok(())
            })();
            if let Err(error) = write_result {
                let _ = fs::remove_file(&temporary);
                return Err(format!(
                    "failed to atomically write {}: {error}",
                    path.display()
                ));
            }
        }
        #[cfg(not(unix))]
        fs::write(path, yaml)
            .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
        Ok(())
    }
}

/// Upgrade the old fork's root-level control-plane YAML before upstream serde can
/// silently ignore it (or mistake `model_profiles.*.backends` for a wire kwarg).
/// Stable UUIDs are derived from entity kind/name so this startup projection is
/// deterministic until the next `configure` write persists the namespaced form.
fn migrate_legacy_root_control_plane(value: YamlValue) -> Result<(YamlValue, bool), String> {
    let YamlValue::Mapping(mut root) = value else {
        return Ok((value, false));
    };
    let control_key = YamlValue::String(CONTROL_PLANE_KEY.to_string());
    let has_legacy_profile_chain = root
        .get(YamlValue::String("model_profiles".to_string()))
        .and_then(YamlValue::as_mapping)
        .is_some_and(|profiles| {
            profiles.values().any(|profile| {
                profile.as_mapping().is_some_and(|profile| {
                    profile.contains_key(YamlValue::String("backends".to_string()))
                })
            })
        });
    let root_auth = root
        .get(YamlValue::String("auth".to_string()))
        .and_then(YamlValue::as_mapping);
    let gateway_auth = root_auth.is_some_and(|auth| {
        auth.contains_key(YamlValue::String("mode".to_string()))
            || auth.contains_key(YamlValue::String("store_path".to_string()))
    });
    let legacy_auth = root_auth.is_some_and(|auth| {
        !gateway_auth || {
            auth.contains_key(YamlValue::String("require".to_string()))
                || auth.contains_key(YamlValue::String("conversation_id_header".to_string()))
                || auth.contains_key(YamlValue::String("keys".to_string()))
        }
    });
    if gateway_auth && legacy_auth {
        return Err(
            "root `auth` mixes gateway authentication fields with legacy control-plane fields"
                .to_string(),
        );
    }
    let legacy_keys = [
        "storage",
        "backends",
        "aliases",
        "unknown_model_policy",
        "admin_password",
        "admin_username",
    ];
    let has_legacy_root = legacy_auth
        || has_legacy_profile_chain
        || legacy_keys
            .iter()
            .any(|key| root.contains_key(YamlValue::String((*key).to_string())));
    if root.contains_key(&control_key) {
        if has_legacy_root {
            return Err(
                "configuration mixes legacy root control-plane fields with `control_plane`; migrate to one namespaced source before startup"
                    .to_string(),
            );
        }
        return Ok((YamlValue::Mapping(root), false));
    }
    if !has_legacy_root {
        return Ok((YamlValue::Mapping(root), false));
    }

    let backends =
        take_yaml(&mut root, "backends").unwrap_or(YamlValue::Mapping(Default::default()));
    let aliases = take_yaml(&mut root, "aliases").unwrap_or(YamlValue::Mapping(Default::default()));
    let profiles =
        take_yaml(&mut root, "model_profiles").unwrap_or(YamlValue::Mapping(Default::default()));
    let legacy_operational = serde_json::json!({
        "backends": yaml_to_json(backends)?,
        "model_profiles": yaml_to_json(profiles)?,
        "aliases": yaml_to_json(aliases)?,
        "keys": serde_json::Value::Array(Vec::new()),
        "unknown_model_policy": take_yaml(&mut root, "unknown_model_policy")
            .map(yaml_to_json)
            .transpose()?
            .unwrap_or_else(|| serde_json::json!("passthrough")),
    });
    let mut operational = OperationalConfig::from_stored(&legacy_operational.to_string())?;

    let auth_value = legacy_auth.then(|| take_yaml(&mut root, "auth")).flatten();
    let storage_value = take_yaml(&mut root, "storage");
    let mut auth = AuthBootstrap::default();
    if let Some(value) = auth_value {
        #[derive(Deserialize)]
        struct LegacyRootAuth {
            #[serde(default)]
            require: bool,
            #[serde(default)]
            conversation_id_header: Option<String>,
            #[serde(default)]
            keys: Vec<LegacyRootKey>,
            #[serde(default, flatten)]
            extra: BTreeMap<String, YamlValue>,
        }
        #[derive(Deserialize)]
        struct LegacyRootKey {
            key: String,
            #[serde(default)]
            label: Option<String>,
            #[serde(default)]
            allowed_aliases: Vec<String>,
            #[serde(default, flatten)]
            extra: BTreeMap<String, JsonValue>,
        }
        let legacy: LegacyRootAuth = serde_yaml::from_value(value)
            .map_err(|error| format!("invalid legacy root auth config: {error}"))?;
        auth.require = legacy.require;
        auth.conversation_id_header = legacy.conversation_id_header;
        auth.extra = legacy.extra;
        let aliases_by_name: HashMap<_, _> = operational
            .aliases
            .iter()
            .map(|alias| (alias.name.clone(), alias.id))
            .collect();
        for key in legacy.keys {
            let allowed_aliases =
                key.allowed_aliases
                    .iter()
                    .map(|name| {
                        aliases_by_name.get(name).copied().ok_or_else(|| {
                            format!("legacy auth key references unknown alias '{name}'")
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
            operational.keys.push(OpKey {
                id: stable_legacy_uuid("key", key.label.as_deref().unwrap_or(&key.key)),
                key: crate::client_auth::hash_secret(key.key.trim()),
                label: key.label,
                user_id: None,
                allowed_aliases,
                extra: key.extra,
            });
        }
    }
    let storage = match storage_value {
        Some(value) => serde_yaml::from_value(value)
            .map_err(|error| format!("invalid legacy root storage config: {error}"))?,
        None => StorageBootstrap::default(),
    };
    // Password/admin fields are deliberately dropped: dashboard authentication
    // is env-only upstream, and persisting them would reintroduce a secret leak.
    let _ = take_yaml(&mut root, "admin_password");
    let _ = take_yaml(&mut root, "admin_username");
    operational.validate()?;
    let section = ControlPlaneSection {
        storage,
        auth,
        operational: Some(operational),
        extra: BTreeMap::new(),
        sessions: crate::harness::SessionsBootstrap::default(),
        metrics: crate::upstream_metrics::MetricsBootstrap::default(),
        vision_probe: crate::vision_probe::VisionProbeBootstrap::default(),
    };
    root.insert(
        control_key,
        serde_yaml::to_value(section)
            .map_err(|error| format!("failed to migrate legacy control plane: {error}"))?,
    );
    Ok((YamlValue::Mapping(root), true))
}

fn take_yaml(root: &mut YamlMapping, key: &str) -> Option<YamlValue> {
    root.remove(YamlValue::String(key.to_string()))
}

fn yaml_to_json(value: YamlValue) -> Result<JsonValue, String> {
    serde_yaml::from_value(value)
        .map_err(|error| format!("legacy control-plane value is not JSON-compatible: {error}"))
}

fn stable_legacy_uuid(kind: &str, name: &str) -> Uuid {
    let digest = Sha256::digest(format!("llmconduit:{kind}:{}", name.trim()).as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ControlPlaneSection {
    #[serde(default, skip_serializing_if = "StorageBootstrap::is_default")]
    pub storage: StorageBootstrap,
    #[serde(default, skip_serializing_if = "AuthBootstrap::is_default")]
    pub auth: AuthBootstrap,
    /// Harness/session detection profiles (see `crate::harness`).
    #[serde(
        default,
        skip_serializing_if = "crate::harness::SessionsBootstrap::is_default"
    )]
    pub sessions: crate::harness::SessionsBootstrap,
    /// Upstream engine metrics scraping (see `crate::upstream_metrics`).
    #[serde(
        default,
        skip_serializing_if = "crate::upstream_metrics::MetricsBootstrap::is_default"
    )]
    pub metrics: crate::upstream_metrics::MetricsBootstrap,
    /// Native-vision probing of routed backend models (see `crate::vision_probe`).
    #[serde(
        default,
        skip_serializing_if = "crate::vision_probe::VisionProbeBootstrap::is_default"
    )]
    pub vision_probe: crate::vision_probe::VisionProbeBootstrap,
    /// `None` means "use the upstream YAML unchanged". `Some(empty)` is an
    /// explicit operational configuration and may intentionally clear profiles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational: Option<OperationalConfig>,
    /// Preserve future control-plane settings owned by a newer build.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, YamlValue>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackend {
    #[default]
    None,
    Jsonl,
    Sqlite,
    #[serde(alias = "postgresql")]
    Postgres,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageBootstrap {
    #[serde(default)]
    pub backend: StorageBackend,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonl_dir: Option<String>,
    /// Maximum number of pending persistence writes. Inference never waits for
    /// this queue; a saturated queue drops the write and increments its
    /// observable overflow counter.
    #[serde(default = "default_persistence_queue_capacity")]
    pub queue_capacity: usize,
    /// Durable history retention. SQL rows are pruned in the background and
    /// JSONL output is rotated daily; zero is rejected at startup.
    #[serde(default = "default_persistence_retention_days")]
    pub retention_days: u64,
    /// Whether persisted request items keep image/data URIs. Secrets are
    /// always redacted; this only controls media payloads, which dedupe like
    /// any other item in the content store.
    #[serde(default = "default_persistence_keep_media")]
    pub keep_media: bool,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, YamlValue>,
}

impl fmt::Debug for StorageBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageBootstrap")
            .field("backend", &self.backend)
            .field("url", &self.url.as_ref().map(|_| "[redacted]"))
            .field("jsonl_dir", &self.jsonl_dir)
            .field("queue_capacity", &self.queue_capacity)
            .field("retention_days", &self.retention_days)
            .field("keep_media", &self.keep_media)
            .field("extra", &self.extra)
            .finish()
    }
}

impl Default for StorageBootstrap {
    fn default() -> Self {
        Self {
            backend: StorageBackend::None,
            url: None,
            jsonl_dir: None,
            queue_capacity: default_persistence_queue_capacity(),
            retention_days: default_persistence_retention_days(),
            keep_media: default_persistence_keep_media(),
            extra: BTreeMap::new(),
        }
    }
}

impl StorageBootstrap {
    fn is_default(value: &Self) -> bool {
        *value == Self::default()
    }
}

const fn default_persistence_queue_capacity() -> usize {
    1024
}

const fn default_persistence_keep_media() -> bool {
    true
}

const fn default_persistence_retention_days() -> u64 {
    30
}

/// Startup-only authentication policy. API keys themselves are operational
/// entities so they can retain stable ids and ownership metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct AuthBootstrap {
    #[serde(default)]
    pub require: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_id_header: Option<String>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, YamlValue>,
}

impl AuthBootstrap {
    fn is_default(value: &Self) -> bool {
        *value == Self::default()
    }

    pub fn conversation_id_header(&self) -> &str {
        self.conversation_id_header
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_CONVERSATION_ID_HEADER)
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UnknownModelPolicy {
    #[default]
    Passthrough,
    Reject,
}

/// A named endpoint. Model-specific behavior remains on [`OpProfile`].
#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct OpBackend {
    pub id: Uuid,
    pub name: String,
    #[serde(alias = "upstream_base_url")]
    pub base_url: String,
    #[serde(
        default,
        alias = "upstream_api_key",
        skip_serializing_if = "Option::is_none"
    )]
    pub api_key: Option<String>,
    /// Name of an environment variable containing this backend's API key.
    /// The resolved value is runtime-only and is never written back to YAML.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    #[serde(
        default,
        alias = "upstream_request_log_path",
        skip_serializing_if = "Option::is_none"
    )]
    pub request_log_path: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Retain backend metadata (for example metrics-scrape settings) that this
    /// routing projection does not interpret.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

impl fmt::Debug for OpBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpBackend")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("api_key_env", &self.api_key_env)
            .field("request_log_path", &self.request_log_path)
            .field("enabled", &self.enabled)
            .field("extra", &self.extra)
            .finish()
    }
}

/// An operational profile embeds the complete upstream profile type.
///
/// This is intentionally a flattened value rather than a hand-maintained subset.
/// When upstream adds roles/reasoning/capability fields, an operational rename or
/// backend edit continues to carry those fields through unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpProfile {
    pub id: Uuid,
    pub name: String,
    /// Ordered failover chain of backend ids.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backends: Vec<Uuid>,
    #[serde(flatten)]
    pub profile: PersistedModelProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpAlias {
    pub id: Uuid,
    pub name: String,
    /// Ordered chain of profile ids. Each profile contributes its ordered
    /// backend chain to the alias's single failover route.
    #[serde(default)]
    pub profiles: Vec<Uuid>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct OpKey {
    pub id: Uuid,
    #[serde(alias = "secret")]
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_aliases: Vec<Uuid>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

impl fmt::Debug for OpKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpKey")
            .field("id", &self.id)
            .field("key", &"[redacted]")
            .field("label", &self.label)
            .field("user_id", &self.user_id)
            .field("allowed_aliases", &self.allowed_aliases)
            .field("extra", &self.extra)
            .finish()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct OperationalConfig {
    #[serde(default)]
    pub backends: Vec<OpBackend>,
    #[serde(default)]
    pub model_profiles: Vec<OpProfile>,
    #[serde(default)]
    pub aliases: Vec<OpAlias>,
    #[serde(default)]
    pub keys: Vec<OpKey>,
    #[serde(default)]
    pub unknown_model_policy: UnknownModelPolicy,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

impl OperationalConfig {
    /// Seed the editable profile set once. The returned ids become stable after
    /// this document is persisted; repeated seeding intentionally creates a new
    /// operational universe and therefore new ids.
    pub fn seed_from_gateway(gateway: &PersistedConfig) -> Self {
        Self {
            model_profiles: gateway
                .model_profiles
                .iter()
                .map(|(name, profile)| OpProfile {
                    id: Uuid::new_v4(),
                    name: name.clone(),
                    backends: Vec::new(),
                    profile: profile.clone(),
                })
                .collect(),
            ..Self::default()
        }
    }

    /// Load the current UUID-array format or migrate the pre-UUID, name-keyed
    /// JSON format used by the original control-plane branch.
    pub fn from_stored(document: &str) -> Result<Self, String> {
        let value: JsonValue = serde_json::from_str(document)
            .map_err(|err| format!("failed to parse stored operational config: {err}"))?;
        let legacy = ["backends", "model_profiles", "aliases"]
            .iter()
            .any(|key| value.get(*key).is_some_and(JsonValue::is_object));
        let operational = if legacy {
            let legacy: LegacyOperationalConfig = serde_json::from_value(value)
                .map_err(|err| format!("failed to parse legacy operational config: {err}"))?;
            legacy.migrate()?
        } else {
            serde_json::from_value(value)
                .map_err(|err| format!("failed to parse operational config: {err}"))?
        };
        operational.validate()?;
        Ok(operational)
    }

    pub fn validate(&self) -> Result<(), String> {
        let mut all_ids = HashSet::new();
        let mut backend_names = HashSet::new();
        let mut profile_names = HashSet::new();
        let mut alias_names = HashSet::new();

        for backend in &self.backends {
            validate_entity(
                "backend",
                backend.id,
                &backend.name,
                &mut all_ids,
                &mut backend_names,
            )?;
            Url::parse(backend.base_url.trim()).map_err(|err| {
                format!("invalid backend '{}'.base_url: {err}", backend.name.trim())
            })?;
            if backend.api_key.is_some() && backend.api_key_env.is_some() {
                return Err(format!(
                    "backend '{}' must set only one of api_key or api_key_env",
                    backend.name.trim()
                ));
            }
            if let Some(name) = trim_option(backend.api_key_env.as_deref()) {
                validate_env_name(&name).map_err(|error| {
                    format!(
                        "invalid backend '{}'.api_key_env: {error}",
                        backend.name.trim()
                    )
                })?;
            }
        }
        for profile in &self.model_profiles {
            validate_entity(
                "model profile",
                profile.id,
                &profile.name,
                &mut all_ids,
                &mut profile_names,
            )?;
        }
        for alias in &self.aliases {
            validate_entity(
                "alias",
                alias.id,
                &alias.name,
                &mut all_ids,
                &mut alias_names,
            )?;
            if alias.profiles.is_empty() {
                return Err(format!(
                    "alias '{}' must reference at least one model profile",
                    alias.name.trim()
                ));
            }
        }
        for alias_name in &alias_names {
            if profile_names.contains(alias_name) {
                return Err(format!(
                    "operational route name '{alias_name}' is used by both a model profile and an alias"
                ));
            }
        }
        for key in &self.keys {
            if !all_ids.insert(key.id) {
                return Err(format!("duplicate operational entity id '{}'", key.id));
            }
            if key.key.trim().is_empty() {
                return Err(format!("api key '{}' must not be blank", key.id));
            }
        }

        let backend_ids: HashSet<_> = self.backends.iter().map(|item| item.id).collect();
        let profile_ids: HashSet<_> = self.model_profiles.iter().map(|item| item.id).collect();
        let alias_ids: HashSet<_> = self.aliases.iter().map(|item| item.id).collect();
        for profile in &self.model_profiles {
            validate_refs(
                &format!("model profile '{}'.backends", profile.name.trim()),
                &profile.backends,
                &backend_ids,
            )?;
        }
        for alias in &self.aliases {
            validate_refs(
                &format!("alias '{}'.profiles", alias.name.trim()),
                &alias.profiles,
                &profile_ids,
            )?;
        }
        for key in &self.keys {
            validate_refs(
                &format!("api key '{}'.allowed_aliases", key.id),
                &key.allowed_aliases,
                &alias_ids,
            )?;
        }
        Ok(())
    }

    /// Require deployed YAML keys to contain one-way digests. Legacy SQL rows
    /// are handled separately at the compatibility boundary because old
    /// releases stored plaintext in the database; accepting plaintext in a
    /// mounted/bootstrap config would leave a live credential on disk forever.
    pub fn validate_persisted_key_digests(&self) -> Result<(), String> {
        for key in &self.keys {
            crate::client_auth::parse_secret_hash(key.key.trim()).map_err(|error| {
                format!(
                    "api key '{}' must use canonical sha256:<hex> storage: {error}",
                    key.id
                )
            })?;
        }
        Ok(())
    }

    /// Build the in-memory virtual-key registry without retaining plaintext.
    /// Operational documents may contain a bootstrap plaintext key or an
    /// already-hashed `sha256:<hex>` value; both converge to the same digest at
    /// this boundary. The registry only receives alias names, never UUIDs.
    pub fn client_auth_specs(&self) -> Result<Vec<crate::client_auth::VirtualKeySpec>, String> {
        self.validate()?;
        let aliases: HashMap<_, _> = self
            .aliases
            .iter()
            .map(|alias| (alias.id, alias.name.trim().to_string()))
            .collect();
        self.keys
            .iter()
            .map(
                |key| -> Result<crate::client_auth::VirtualKeySpec, String> {
                    let stored = key.key.trim();
                    let secret_hash = if stored.starts_with(crate::client_auth::KEY_HASH_PREFIX) {
                        // Validate canonical hashes before handing them to the atomic
                        // registry; malformed values fail the entire replacement.
                        crate::client_auth::parse_secret_hash(stored)?;
                        stored.to_ascii_lowercase()
                    } else {
                        crate::client_auth::hash_secret(stored)
                    };
                    let allowed_models: Vec<String> = key
                        .allowed_aliases
                        .iter()
                        .filter_map(|id| aliases.get(id).cloned())
                        .collect();
                    Ok(crate::client_auth::VirtualKeySpec {
                        id: key.id.to_string(),
                        label: trim_option(key.label.as_deref()),
                        owner_id: key.user_id.map(|id| id.to_string()),
                        secret_hash,
                        allowed_models,
                    })
                },
            )
            .collect()
    }

    pub fn client_auth(&self, require: bool) -> Result<crate::client_auth::ClientAuth, String> {
        crate::client_auth::ClientAuth::from_specs(require, self.client_auth_specs()?)
    }

    /// Clone-and-overlay is the lossless boundary: all non-profile upstream
    /// settings and the profile-template map stay owned by `gateway`; profiles
    /// are inserted as complete [`PersistedModelProfile`] values.
    pub fn apply_profiles_to(&self, gateway: &PersistedConfig) -> Result<PersistedConfig, String> {
        self.validate()?;
        let mut result = gateway.clone();
        result.model_profiles = self
            .model_profiles
            .iter()
            .map(|profile| (profile.name.trim().to_string(), profile.profile.clone()))
            .collect();
        Ok(result)
    }

    /// Resolve aliases into exact route plans. The DI integration should feed
    /// each plan to the existing `RoutingUpstreamClient::with_routes`: build one
    /// `FailoverUpstreamClient` from `providers`, wrap it as one route provider,
    /// and add an exact `ModelRouteSpec` for `name`. No second routing layer is
    /// needed, and the existing pre-first-chunk-only failover rule is retained.
    pub fn route_plans(
        &self,
        gateway: &PersistedConfig,
    ) -> Result<Vec<OperationalRoutePlan>, String> {
        self.validate()?;
        let resolved = Config::from_persisted(gateway)?;
        let backends: HashMap<_, _> = self
            .backends
            .iter()
            .map(|backend| (backend.id, backend))
            .collect();
        let profiles: HashMap<_, _> = self
            .model_profiles
            .iter()
            .map(|profile| (profile.id, profile))
            .collect();

        let build_route = |route_id: Uuid,
                           route_name: &str,
                           profile_ids: &[Uuid]|
         -> Result<OperationalRoutePlan, String> {
            let primary_profile_id = profile_ids[0];
            let primary_profile_name = profiles[&primary_profile_id].name.trim().to_string();
            let mut provider_plans = Vec::new();
            for profile_id in profile_ids {
                let profile = profiles[profile_id];
                let profile_name = profile.name.trim();
                let resolved_profile =
                    resolved.model_profiles.get(profile_name).ok_or_else(|| {
                        format!("resolved model profile '{profile_name}' was not found")
                    })?;
                for backend_id in &profile.backends {
                    let backend = backends[backend_id];
                    if !backend.enabled {
                        continue;
                    }
                    provider_plans.push(OperationalProviderPlan {
                        backend_id: backend.id,
                        backend_name: backend.name.trim().to_string(),
                        profile_id: profile.id,
                        profile_name: profile_name.to_string(),
                        base_url: Url::parse(backend.base_url.trim()).map_err(|err| {
                            format!("invalid backend '{}'.base_url: {err}", backend.name.trim())
                        })?,
                        api_key: resolve_backend_api_key(backend)?,
                        request_log_path: trim_option(backend.request_log_path.as_deref())
                            .map(PathBuf::from),
                        upstream_model: resolved_profile.upstream_model.clone(),
                        upstream_chat_kwargs: resolved_profile.upstream_chat_kwargs.clone(),
                    });
                }
            }
            if provider_plans.is_empty() {
                return Err(format!(
                    "operational route '{}' has no enabled backend provider",
                    route_name.trim()
                ));
            }
            Ok(OperationalRoutePlan {
                route_id,
                name: route_name.trim().to_string(),
                primary_profile_id,
                primary_profile_name,
                providers: provider_plans,
            })
        };

        let mut routes = Vec::new();
        for profile in &self.model_profiles {
            if !profile.backends.is_empty() {
                routes.push(build_route(profile.id, &profile.name, &[profile.id])?);
            }
        }
        for alias in &self.aliases {
            routes.push(build_route(alias.id, &alias.name, &alias.profiles)?);
        }
        Ok(routes)
    }
}

#[derive(Debug, Clone)]
pub struct MaterializedControlPlane {
    pub gateway: PersistedConfig,
    pub routes: Vec<OperationalRoutePlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperationalRoutePlan {
    pub route_id: Uuid,
    pub name: String,
    /// The engine uses this profile for request-level prefix/role policy while
    /// the leaf applies each provider's final-model policy.
    pub primary_profile_id: Uuid,
    pub primary_profile_name: String,
    pub providers: Vec<OperationalProviderPlan>,
}

#[derive(Clone, PartialEq)]
pub struct OperationalProviderPlan {
    pub backend_id: Uuid,
    pub backend_name: String,
    pub profile_id: Uuid,
    pub profile_name: String,
    pub base_url: Url,
    pub api_key: Option<String>,
    pub request_log_path: Option<PathBuf>,
    pub upstream_model: Option<String>,
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
}

impl fmt::Debug for OperationalProviderPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OperationalProviderPlan")
            .field("backend_id", &self.backend_id)
            .field("backend_name", &self.backend_name)
            .field("profile_id", &self.profile_id)
            .field("profile_name", &self.profile_name)
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("request_log_path", &self.request_log_path)
            .field("upstream_model", &self.upstream_model)
            .field("upstream_chat_kwargs", &self.upstream_chat_kwargs)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct LegacyOperationalConfig {
    #[serde(default)]
    backends: BTreeMap<String, LegacyBackend>,
    #[serde(default)]
    model_profiles: BTreeMap<String, LegacyProfile>,
    #[serde(default)]
    aliases: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    keys: Vec<LegacyKey>,
    #[serde(default)]
    unknown_model_policy: UnknownModelPolicy,
    #[serde(default, flatten)]
    extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Deserialize)]
struct LegacyBackend {
    #[serde(alias = "upstream_base_url")]
    base_url: String,
    #[serde(default, alias = "upstream_api_key")]
    api_key: Option<String>,
    #[serde(default, alias = "upstream_request_log_path")]
    request_log_path: Option<String>,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default, flatten)]
    extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Deserialize)]
struct LegacyProfile {
    #[serde(default)]
    backends: Vec<String>,
    #[serde(flatten)]
    profile: PersistedModelProfile,
}

#[derive(Debug, Deserialize)]
struct LegacyKey {
    #[serde(alias = "secret")]
    key: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    user_id: Option<Uuid>,
    #[serde(default)]
    allowed_aliases: Vec<String>,
    #[serde(default, flatten)]
    extra: BTreeMap<String, JsonValue>,
}

impl LegacyOperationalConfig {
    fn migrate(self) -> Result<OperationalConfig, String> {
        let backend_ids: BTreeMap<_, _> = self
            .backends
            .keys()
            .map(|name| (name.clone(), stable_legacy_uuid("backend", name)))
            .collect();
        let profile_ids: BTreeMap<_, _> = self
            .model_profiles
            .keys()
            .map(|name| (name.clone(), stable_legacy_uuid("profile", name)))
            .collect();
        let alias_ids: BTreeMap<_, _> = self
            .aliases
            .keys()
            .map(|name| (name.clone(), stable_legacy_uuid("alias", name)))
            .collect();

        let backends = self
            .backends
            .into_iter()
            .map(|(name, backend)| OpBackend {
                id: backend_ids[&name],
                name,
                base_url: backend.base_url,
                api_key: backend.api_key,
                api_key_env: None,
                request_log_path: backend.request_log_path,
                enabled: backend.enabled,
                extra: backend.extra,
            })
            .collect();
        let model_profiles = self
            .model_profiles
            .into_iter()
            .map(|(name, profile)| {
                let backends = resolve_legacy_refs(
                    &format!("model_profiles[{name}].backends"),
                    &profile.backends,
                    &backend_ids,
                )?;
                Ok(OpProfile {
                    id: profile_ids[&name],
                    name,
                    backends,
                    profile: profile.profile,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let aliases = self
            .aliases
            .into_iter()
            .map(|(name, profiles)| {
                let profiles = resolve_legacy_refs(
                    &format!("aliases[{name}].profiles"),
                    &profiles,
                    &profile_ids,
                )?;
                Ok(OpAlias {
                    id: alias_ids[&name],
                    name,
                    profiles,
                    extra: BTreeMap::new(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let keys = self
            .keys
            .into_iter()
            .map(|key| {
                let allowed_aliases = resolve_legacy_refs(
                    "keys[].allowed_aliases",
                    &key.allowed_aliases,
                    &alias_ids,
                )?;
                Ok(OpKey {
                    id: stable_legacy_uuid("key", key.label.as_deref().unwrap_or(key.key.as_str())),
                    key: key.key,
                    label: key.label,
                    user_id: key.user_id,
                    allowed_aliases,
                    extra: key.extra,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(OperationalConfig {
            backends,
            model_profiles,
            aliases,
            keys,
            unknown_model_policy: self.unknown_model_policy,
            extra: self.extra,
        })
    }
}

fn default_true() -> bool {
    true
}

fn trim_option(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn validate_env_name(name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    if !chars
        .next()
        .is_some_and(|value| value == '_' || value.is_ascii_alphabetic())
        || !chars.all(|value| value == '_' || value.is_ascii_alphanumeric())
    {
        return Err("must be an ASCII environment variable name".to_string());
    }
    Ok(())
}

fn resolve_backend_api_key(backend: &OpBackend) -> Result<Option<String>, String> {
    let Some(name) = trim_option(backend.api_key_env.as_deref()) else {
        return Ok(trim_option(backend.api_key.as_deref()));
    };
    validate_env_name(&name)?;
    let value = std::env::var(&name).map_err(|_| {
        format!(
            "backend '{}' requires non-empty environment variable '{name}'",
            backend.name.trim()
        )
    })?;
    trim_option(Some(&value)).map(Some).ok_or_else(|| {
        format!(
            "backend '{}' requires non-empty environment variable '{name}'",
            backend.name.trim()
        )
    })
}

fn validate_entity(
    kind: &str,
    id: Uuid,
    name: &str,
    ids: &mut HashSet<Uuid>,
    names: &mut HashSet<String>,
) -> Result<(), String> {
    if !ids.insert(id) {
        return Err(format!("duplicate operational entity id '{id}'"));
    }
    let name = name.trim();
    if name.is_empty() {
        return Err(format!("{kind} name must not be empty"));
    }
    if !names.insert(name.to_ascii_lowercase()) {
        return Err(format!("duplicate {kind} name '{name}'"));
    }
    Ok(())
}

fn validate_refs(location: &str, refs: &[Uuid], known: &HashSet<Uuid>) -> Result<(), String> {
    let mut seen = HashSet::new();
    for id in refs {
        if !known.contains(id) {
            return Err(format!("{location} references unknown id '{id}'"));
        }
        if !seen.insert(*id) {
            return Err(format!("{location} contains duplicate id '{id}'"));
        }
    }
    Ok(())
}

fn resolve_legacy_refs(
    location: &str,
    refs: &[String],
    ids: &BTreeMap<String, Uuid>,
) -> Result<Vec<Uuid>, String> {
    refs.iter()
        .map(|name| {
            ids.get(name)
                .copied()
                .ok_or_else(|| format!("{location} references unknown legacy name '{name}'"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn modern_profile_yaml() -> &'static str {
        r#"
extends: [reasoning-base]
upstream_model: glm-5
system_prompt_prefix: preserve me
roles:
  merge_adjacent: [system, user]
  developer:
    when: always
    action: rewrite
    target_role: user
    tag: developer
    tag_attributes: {source: control-plane}
template_family: deepseek
native_vision: false
upstream_chat_kwargs:
  chat_template_kwargs:
    enable_thinking: true
reasoning_effort_map:
  high:
    chat_template_kwargs:
      reasoning_effort: high
reasoning_effort_default: high
capabilities:
  image_input: false
  thinking:
    supported: true
    types: [adaptive, enabled]
reasoning_effort:
  default: high
  map: {low: low, high: high}
"#
    }

    fn gateway_with_modern_profile() -> PersistedConfig {
        let profile: PersistedModelProfile =
            serde_yaml::from_str(modern_profile_yaml()).expect("modern profile parses");
        let mut gateway = PersistedConfig::default();
        gateway.model_profile_templates.insert(
            "reasoning-base".to_string(),
            serde_yaml::from_str("temperature: 0.2\n").expect("template parses"),
        );
        gateway
            .model_profiles
            .insert("glm-profile".to_string(), profile);
        gateway
    }

    #[test]
    fn unknown_top_level_gateway_keys_survive_control_plane_edit() {
        let source = r#"
bind_addr: 127.0.0.1:4111
future_upstream_knob:
  nested: [one, two]
model_profiles:
  model-a:
    roles:
      developer: {action: drop}
control_plane:
  future_control_plane_knob: retained
"#;
        let mut document = ControlPlaneConfig::from_yaml_str(source).expect("parse");
        document.control_plane_mut().auth.require = true;
        let written = document.to_yaml_string().expect("serialize");
        let raw: YamlValue = serde_yaml::from_str(&written).expect("reparse raw");
        assert_eq!(
            raw["future_upstream_knob"]["nested"],
            serde_yaml::from_str::<YamlValue>("[one, two]").unwrap()
        );
        assert_eq!(
            raw[CONTROL_PLANE_KEY]["future_control_plane_knob"],
            YamlValue::String("retained".to_string())
        );
        assert_eq!(raw[CONTROL_PLANE_KEY]["auth"]["require"], true);
    }

    #[test]
    fn legacy_root_control_plane_migrates_without_opening_auth_or_leaking_keys() {
        let source = r#"
upstream_base_url: http://localhost:8000/v1
storage: {backend: sqlite, url: "sqlite:///tmp/legacy.sqlite3"}
backends:
  local: {base_url: "http://localhost:9000/v1"}
model_profiles:
  public:
    upstream_model: model-a
    backends: [local]
aliases: {team: [public]}
unknown_model_policy: reject
auth:
  require: true
  keys:
    - key: legacy-secret
      label: team-key
      allowed_aliases: [team]
"#;
        let first = ControlPlaneConfig::from_yaml_str(source).expect("migrate");
        let section = first.control_plane();
        assert!(section.auth.require);
        assert_eq!(section.storage.backend, StorageBackend::Sqlite);
        let operational = section.operational.as_ref().expect("operational");
        assert_eq!(operational.unknown_model_policy, UnknownModelPolicy::Reject);
        assert_eq!(operational.backends.len(), 1);
        assert_eq!(
            operational.model_profiles[0].backends[0],
            operational.backends[0].id
        );
        assert_eq!(
            operational.keys[0].allowed_aliases[0],
            operational.aliases[0].id
        );
        assert!(operational.keys[0].key.starts_with("sha256:"));
        assert!(!first.to_yaml_string().unwrap().contains("legacy-secret"));

        let second = ControlPlaneConfig::from_yaml_str(source).expect("migrate again");
        assert_eq!(
            second.control_plane().operational,
            first.control_plane().operational,
            "legacy migration ids are deterministic"
        );
    }

    #[test]
    fn mixed_legacy_and_namespaced_control_plane_is_rejected() {
        let error =
            ControlPlaneConfig::from_yaml_str("control_plane: {}\nauth:\n  require: true\n")
                .unwrap_err();
        assert!(error.contains("mixes legacy root control-plane fields"));
    }

    #[test]
    fn gateway_auth_coexists_with_namespaced_control_plane() {
        let source = r#"
auth:
  mode: enforce
  store_path: /var/lib/llmconduit/auth.sqlite3
control_plane:
  auth:
    require: false
"#;
        let document = ControlPlaneConfig::from_yaml_str(source).expect("parse");

        assert_eq!(
            document.gateway().auth.mode,
            crate::config::AuthMode::Enforce
        );
        assert_eq!(
            document.gateway().auth.store_path,
            "/var/lib/llmconduit/auth.sqlite3"
        );
        assert!(!document.control_plane().auth.require);
        assert!(!document.migrated_legacy_root());
    }

    #[test]
    fn gateway_auth_without_control_plane_is_not_migrated() {
        let source = r#"
auth:
  mode: enforce
  store_path: auth.sqlite3
"#;
        let document = ControlPlaneConfig::from_yaml_str(source).expect("parse");

        assert_eq!(
            document.gateway().auth.mode,
            crate::config::AuthMode::Enforce
        );
        assert_eq!(document.gateway().auth.store_path, "auth.sqlite3");
        assert!(!document.migrated_legacy_root());
    }

    #[test]
    fn gateway_auth_survives_migration_of_other_legacy_fields() {
        let source = r#"
auth:
  mode: enforce
  store_path: auth.sqlite3
storage:
  backend: jsonl
  jsonl_dir: history
"#;
        let document = ControlPlaneConfig::from_yaml_str(source).expect("migrate");

        assert_eq!(
            document.gateway().auth.mode,
            crate::config::AuthMode::Enforce
        );
        assert_eq!(document.gateway().auth.store_path, "auth.sqlite3");
        assert_eq!(
            document.control_plane().storage.backend,
            StorageBackend::Jsonl
        );
        assert!(document.migrated_legacy_root());
    }

    #[test]
    fn mixed_gateway_and_legacy_root_auth_is_rejected() {
        let error = ControlPlaneConfig::from_yaml_str(
            "auth:\n  mode: enforce\n  store_path: auth.sqlite3\n  require: true\n",
        )
        .unwrap_err();

        assert!(error.contains("mixes gateway authentication fields"));
    }

    #[test]
    fn legacy_auth_unknown_fields_survive_migration() {
        let source = r#"
auth:
  future_auth: retained
  keys:
    - key: secret
      future_key: retained-too
"#;
        let document = ControlPlaneConfig::from_yaml_str(source).expect("migrate");
        let raw = document.to_yaml_value().expect("serialize");
        assert_eq!(raw[CONTROL_PLANE_KEY]["auth"]["future_auth"], "retained");
        assert_eq!(
            raw[CONTROL_PLANE_KEY]["operational"]["keys"][0]["future_key"],
            "retained-too"
        );
    }

    #[test]
    fn unknown_operational_and_alias_fields_survive_control_plane_edit() {
        let profile_id = Uuid::new_v4();
        let alias_id = Uuid::new_v4();
        let source = format!(
            r#"
control_plane:
  operational:
    future_operational_knob: retained
    model_profiles:
      - id: "{profile_id}"
        name: profile
    aliases:
      - id: "{alias_id}"
        name: public
        profiles: ["{profile_id}"]
        future_alias_knob: retained-too
"#
        );
        let mut document = ControlPlaneConfig::from_yaml_str(&source).expect("parse");
        document.control_plane_mut().auth.require = true;
        let raw: YamlValue =
            serde_yaml::from_str(&document.to_yaml_string().expect("serialize")).expect("yaml");
        assert_eq!(
            raw[CONTROL_PLANE_KEY]["operational"]["future_operational_knob"],
            "retained"
        );
        assert_eq!(
            raw[CONTROL_PLANE_KEY]["operational"]["aliases"][0]["future_alias_knob"],
            "retained-too"
        );
    }

    #[test]
    fn known_upstream_config_round_trips_semantically() {
        let gateway = gateway_with_modern_profile();
        let mut document =
            ControlPlaneConfig::from_gateway(gateway.clone(), ControlPlaneSection::default())
                .expect("document");
        document.control_plane_mut().storage = StorageBootstrap {
            backend: StorageBackend::Postgres,
            url: Some("postgres://db/llmconduit".to_string()),
            ..StorageBootstrap::default()
        };
        let written = document.to_yaml_string().expect("serialize");
        let reparsed = ControlPlaneConfig::from_yaml_str(&written).expect("reparse");
        assert_eq!(reparsed.gateway(), &gateway);
    }

    #[test]
    fn profile_overlay_preserves_every_modern_profile_field() {
        let gateway = gateway_with_modern_profile();
        let original = gateway.model_profiles["glm-profile"].clone();
        let mut operational = OperationalConfig::seed_from_gateway(&gateway);
        let stable_id = operational.model_profiles[0].id;
        operational.model_profiles[0].name = "renamed-profile".to_string();

        let overlaid = operational.apply_profiles_to(&gateway).expect("overlay");
        assert_eq!(overlaid.model_profiles["renamed-profile"], original);
        assert_eq!(
            overlaid.model_profile_templates,
            gateway.model_profile_templates
        );

        let encoded = serde_json::to_string(&operational).expect("encode");
        let decoded = OperationalConfig::from_stored(&encoded).expect("decode");
        assert_eq!(decoded.model_profiles[0].id, stable_id);
        assert_eq!(decoded.model_profiles[0].profile, original);
    }

    #[test]
    fn rename_keeps_uuid_references_and_builds_one_failover_route() {
        let gateway = gateway_with_modern_profile();
        let backend_a = Uuid::new_v4();
        let backend_b = Uuid::new_v4();
        let profile_id = Uuid::new_v4();
        let alias_id = Uuid::new_v4();
        let mut profile = gateway.model_profiles["glm-profile"].clone();
        // The fixture above deliberately carries both supported reasoning wire
        // shapes to prove lossless serialization. Runtime config correctly
        // rejects using both at once, so this routing fixture selects the
        // fragment-based form.
        profile.reasoning_effort = None;
        let mut operational = OperationalConfig {
            backends: vec![
                OpBackend {
                    id: backend_a,
                    name: "primary".to_string(),
                    base_url: "http://127.0.0.1:8001/v1".to_string(),
                    api_key: None,
                    api_key_env: None,
                    request_log_path: None,
                    enabled: true,
                    extra: BTreeMap::new(),
                },
                OpBackend {
                    id: backend_b,
                    name: "backup".to_string(),
                    base_url: "http://127.0.0.1:8002/v1".to_string(),
                    api_key: None,
                    api_key_env: None,
                    request_log_path: None,
                    enabled: true,
                    extra: BTreeMap::new(),
                },
            ],
            model_profiles: vec![OpProfile {
                id: profile_id,
                name: "glm-profile".to_string(),
                backends: vec![backend_a, backend_b],
                profile,
            }],
            aliases: vec![OpAlias {
                id: alias_id,
                name: "smart".to_string(),
                profiles: vec![profile_id],
                extra: BTreeMap::new(),
            }],
            ..OperationalConfig::default()
        };
        operational.backends[0].name = "primary-renamed".to_string();

        let overlaid = operational.apply_profiles_to(&gateway).expect("overlay");
        let routes = operational.route_plans(&overlaid).expect("routes");
        assert_eq!(routes.len(), 2, "profile and alias are both routable");
        let route = routes
            .iter()
            .find(|route| route.name == "smart")
            .expect("alias route");
        assert_eq!(route.primary_profile_id, profile_id);
        assert_eq!(route.providers.len(), 2);
        assert_eq!(route.providers[0].backend_id, backend_a);
        assert_eq!(route.providers[0].backend_name, "primary-renamed");
        assert_eq!(route.providers[1].backend_id, backend_b);
    }

    #[test]
    fn provider_plan_debug_redacts_resolved_api_key() {
        let provider = OperationalProviderPlan {
            backend_id: Uuid::new_v4(),
            backend_name: "private".to_string(),
            profile_id: Uuid::new_v4(),
            profile_name: "profile".to_string(),
            base_url: Url::parse("https://example.invalid/v1").unwrap(),
            api_key: Some("super-secret-provider-key".to_string()),
            request_log_path: None,
            upstream_model: None,
            upstream_chat_kwargs: JsonMap::new(),
        };
        let rendered = format!("{provider:?}");
        assert!(!rendered.contains("super-secret-provider-key"));
        assert!(rendered.contains("[redacted]"));
    }

    #[test]
    fn control_plane_debug_redacts_key_and_storage_url() {
        let secret = "client-secret-that-must-not-render";
        let database = "postgres://user:database-password@example.invalid/db";
        let section = ControlPlaneSection {
            storage: StorageBootstrap {
                backend: StorageBackend::Postgres,
                url: Some(database.to_string()),
                ..StorageBootstrap::default()
            },
            operational: Some(OperationalConfig {
                keys: vec![OpKey {
                    id: Uuid::new_v4(),
                    key: secret.to_string(),
                    label: None,
                    user_id: None,
                    allowed_aliases: Vec::new(),
                    extra: BTreeMap::new(),
                }],
                ..OperationalConfig::default()
            }),
            ..ControlPlaneSection::default()
        };
        let rendered = format!("{section:?}");
        assert!(!rendered.contains(secret));
        assert!(!rendered.contains("database-password"));
        assert!(rendered.contains("[redacted]"));
    }

    #[test]
    fn validation_rejects_dangling_and_case_folded_duplicate_names() {
        let profile_id = Uuid::new_v4();
        let mut operational = OperationalConfig {
            model_profiles: vec![OpProfile {
                id: profile_id,
                name: "Model".to_string(),
                backends: vec![Uuid::new_v4()],
                profile: PersistedModelProfile::default(),
            }],
            ..OperationalConfig::default()
        };
        assert!(operational.validate().unwrap_err().contains("unknown id"));
        operational.model_profiles[0].backends.clear();
        operational.model_profiles.push(OpProfile {
            id: Uuid::new_v4(),
            name: "model".to_string(),
            backends: Vec::new(),
            profile: PersistedModelProfile::default(),
        });
        assert!(
            operational
                .validate()
                .unwrap_err()
                .contains("duplicate model profile name")
        );
    }

    #[test]
    fn legacy_name_keyed_document_migrates_without_dropping_profile_fields() {
        let profile_value: JsonValue = serde_yaml::from_str::<YamlValue>(modern_profile_yaml())
            .and_then(serde_yaml::from_value)
            .expect("profile JSON-compatible");
        let legacy = json!({
            "backends": {
                "local": {"base_url": "http://127.0.0.1:8000/v1"}
            },
            "model_profiles": {
                "glm-profile": {
                    "backends": ["local"],
                    "extends": profile_value["extends"],
                    "upstream_model": profile_value["upstream_model"],
                    "system_prompt_prefix": profile_value["system_prompt_prefix"],
                    "roles": profile_value["roles"],
                    "template_family": profile_value["template_family"],
                    "native_vision": profile_value["native_vision"],
                    "upstream_chat_kwargs": profile_value["upstream_chat_kwargs"],
                    "reasoning_effort_map": profile_value["reasoning_effort_map"],
                    "reasoning_effort_default": profile_value["reasoning_effort_default"],
                    "capabilities": profile_value["capabilities"],
                    "reasoning_effort": profile_value["reasoning_effort"]
                }
            },
            "aliases": {"smart": ["glm-profile"]},
            "keys": [{"key": "sk-team", "allowed_aliases": ["smart"]}]
        });
        let migrated = OperationalConfig::from_stored(&legacy.to_string()).expect("migrate");
        let expected: PersistedModelProfile =
            serde_yaml::from_str(modern_profile_yaml()).expect("expected profile");
        assert_eq!(migrated.model_profiles[0].profile, expected);
        assert_eq!(
            migrated.model_profiles[0].backends[0],
            migrated.backends[0].id
        );
        assert_eq!(
            migrated.aliases[0].profiles[0],
            migrated.model_profiles[0].id
        );
        assert_eq!(migrated.keys[0].allowed_aliases[0], migrated.aliases[0].id);
    }

    #[test]
    fn client_auth_resolves_alias_ids_and_never_exposes_plaintext() {
        let alias_id = Uuid::new_v4();
        let key_id = Uuid::new_v4();
        let profile_id = Uuid::new_v4();
        let operational = OperationalConfig {
            model_profiles: vec![OpProfile {
                id: profile_id,
                name: "profile".to_string(),
                backends: Vec::new(),
                profile: PersistedModelProfile::default(),
            }],
            aliases: vec![OpAlias {
                id: alias_id,
                name: "smart".to_string(),
                profiles: vec![profile_id],
                extra: BTreeMap::new(),
            }],
            keys: vec![OpKey {
                id: key_id,
                key: "sk-bootstrap-secret".to_string(),
                label: Some("team".to_string()),
                user_id: None,
                allowed_aliases: vec![alias_id],
                extra: BTreeMap::new(),
            }],
            ..OperationalConfig::default()
        };
        let auth = operational.client_auth(true).expect("auth registry");
        let rendered = format!("{auth:?}");
        assert!(!rendered.contains("sk-bootstrap-secret"));
        let crate::client_auth::ClientAuthOutcome::Authenticated(identity) =
            auth.authenticate(Some("sk-bootstrap-secret"))
        else {
            panic!("key authenticates");
        };
        assert_eq!(identity.key_id, key_id.to_string());
        assert!(identity.allows_model("SMART"));
        assert!(!identity.allows_model("other"));
    }

    #[cfg(unix)]
    #[test]
    fn rewriting_config_tightens_existing_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path = std::env::temp_dir().join(format!(
            "llmconduit-control-plane-permissions-{}.yaml",
            Uuid::new_v4()
        ));
        std::fs::write(&path, "bind_addr: 127.0.0.1:4111\n").expect("seed config");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("make config permissive");

        let document =
            ControlPlaneConfig::from_gateway(PersistedConfig::default(), Default::default())
                .expect("document");
        document.write_to_path(&path).expect("rewrite config");

        let mode = std::fs::metadata(&path)
            .expect("config metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        std::fs::remove_file(path).expect("remove test config");
    }

    #[test]
    fn conversation_header_rejects_credential_carriers() {
        assert!(is_sensitive_conversation_header("Authorization"));
        assert!(is_sensitive_conversation_header("x_api_key"));
        assert!(!is_sensitive_conversation_header("x-conversation-id"));
    }

    #[test]
    fn deployed_operational_keys_require_one_way_digests() {
        let mut operational = OperationalConfig::default();
        operational.keys.push(OpKey {
            id: Uuid::new_v4(),
            key: "plaintext-is-not-safe-at-rest".to_string(),
            label: None,
            user_id: None,
            allowed_aliases: Vec::new(),
            extra: BTreeMap::new(),
        });
        assert!(
            operational
                .validate_persisted_key_digests()
                .unwrap_err()
                .contains("must use canonical")
        );
        operational.keys[0].key = crate::client_auth::hash_secret("secret");
        operational
            .validate_persisted_key_digests()
            .expect("digest is accepted");
    }
}
