use chrono::DateTime;
use chrono::FixedOffset;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use serde::de::{Deserializer, Error as DeError};
use serde::ser::{SerializeMap, Serializer};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

/// Upstream-compatible per-profile reasoning shaping. The fork's older
/// `reasoning_effort_map` accepts arbitrary request fragments and remains the
/// more expressive form; this typed form is retained as a convenient shorthand
/// for top-level effort remapping plus a dynamic thinking template kwarg.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReasoningConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub map: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "is_default_thinking_param_name")]
    pub thinking_param_name: String,
    #[serde(skip_serializing_if = "is_default_thinking_param_value_on")]
    pub thinking_param_value_on: JsonValue,
    #[serde(skip_serializing_if = "is_default_thinking_param_value_off")]
    pub thinking_param_value_off: JsonValue,
}

const DEFAULT_THINKING_PARAM_NAME: &str = "enable_thinking";

fn default_thinking_param_name() -> String {
    DEFAULT_THINKING_PARAM_NAME.to_string()
}

fn default_thinking_param_value_on() -> JsonValue {
    JsonValue::Bool(true)
}

fn default_thinking_param_value_off() -> JsonValue {
    JsonValue::Bool(false)
}

fn is_default_thinking_param_name(name: &str) -> bool {
    name == DEFAULT_THINKING_PARAM_NAME
}

fn is_default_thinking_param_value_on(value: &JsonValue) -> bool {
    matches!(value, JsonValue::Bool(true))
}

fn is_default_thinking_param_value_off(value: &JsonValue) -> bool {
    matches!(value, JsonValue::Bool(false))
}

impl Default for ReasoningConfig {
    fn default() -> Self {
        Self {
            default: None,
            map: BTreeMap::new(),
            thinking_param_name: default_thinking_param_name(),
            thinking_param_value_on: default_thinking_param_value_on(),
            thinking_param_value_off: default_thinking_param_value_off(),
        }
    }
}

impl ReasoningConfig {
    pub fn thinking_param_value(&self, on: bool) -> &JsonValue {
        if on {
            &self.thinking_param_value_on
        } else {
            &self.thinking_param_value_off
        }
    }
}

impl<'de> Deserialize<'de> for ReasoningConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            #[serde(default)]
            default: Option<String>,
            #[serde(default)]
            map: BTreeMap<String, String>,
            #[serde(default = "default_thinking_param_name")]
            thinking_param_name: String,
            #[serde(default = "default_thinking_param_value_on")]
            thinking_param_value_on: JsonValue,
            #[serde(default = "default_thinking_param_value_off")]
            thinking_param_value_off: JsonValue,
        }

        let raw = Raw::deserialize(deserializer)?;
        let map = raw
            .map
            .into_iter()
            .filter_map(|(key, value)| {
                let key = key.trim().to_ascii_lowercase();
                let value = value.trim().to_string();
                (!key.is_empty() && !value.is_empty()).then_some((key, value))
            })
            .collect();
        let default = raw.default.and_then(|value| {
            let value = value.trim().to_string();
            (!value.is_empty()).then_some(value)
        });
        let thinking_param_name = if raw.thinking_param_name.trim().is_empty() {
            default_thinking_param_name()
        } else {
            raw.thinking_param_name.trim().to_string()
        };
        Ok(Self {
            default,
            map,
            thinking_param_name,
            thinking_param_value_on: raw.thinking_param_value_on,
            thinking_param_value_off: raw.thinking_param_value_off,
        })
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThinkingType {
    Adaptive,
    Enabled,
}

impl ThinkingType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Adaptive => "adaptive",
            Self::Enabled => "enabled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffortLevel {
    Max,
    Xhigh,
    High,
    Medium,
    Low,
    Minimal,
    #[serde(rename = "none")]
    Disabled,
}

impl EffortLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Max => "max",
            Self::Xhigh => "xhigh",
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
            Self::Minimal => "minimal",
            Self::Disabled => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextFeature {
    #[serde(rename = "clear_thinking_20251015")]
    ClearThinking20251015,
    #[serde(rename = "clear_tool_uses_20250919")]
    ClearToolUses20250919,
    #[serde(rename = "compact_20260112")]
    Compact20260112,
}

impl ContextFeature {
    fn as_str(self) -> &'static str {
        match self {
            Self::ClearThinking20251015 => "clear_thinking_20251015",
            Self::ClearToolUses20250919 => "clear_tool_uses_20250919",
            Self::Compact20260112 => "compact_20260112",
        }
    }
}

fn supported_obj(supported: bool) -> JsonValue {
    JsonValue::Object(JsonMap::from_iter([(
        "supported".to_string(),
        JsonValue::Bool(supported),
    )]))
}

/// Anthropic capability with a single support flag. A bare boolean is accepted
/// as shorthand for `{ supported: ... }`.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct SimpleCap {
    pub supported: bool,
}

impl SimpleCap {
    fn to_wire(&self) -> JsonValue {
        supported_obj(self.supported)
    }
}

impl<'de> Deserialize<'de> for SimpleCap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        if let Some(supported) = value.as_bool() {
            return Ok(Self { supported });
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            #[serde(default = "default_true")]
            supported: bool,
        }
        let raw = serde_json::from_value::<Raw>(value)
            .map_err(|err| <D::Error as serde::de::Error>::custom(err.to_string()))?;
        Ok(Self {
            supported: raw.supported,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThinkingCap {
    #[serde(default = "default_true")]
    pub supported: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub types: Vec<ThinkingType>,
}

impl ThinkingCap {
    fn to_wire(&self) -> JsonValue {
        let types = self
            .types
            .iter()
            .map(|kind| (kind.as_str().to_string(), supported_obj(self.supported)))
            .collect();
        JsonValue::Object(JsonMap::from_iter([
            ("supported".to_string(), JsonValue::Bool(self.supported)),
            ("types".to_string(), JsonValue::Object(types)),
        ]))
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffortCap {
    #[serde(default = "default_true")]
    pub supported: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub levels: Vec<EffortLevel>,
}

impl EffortCap {
    fn to_wire(&self) -> JsonValue {
        let mut map = JsonMap::new();
        map.insert("supported".to_string(), JsonValue::Bool(self.supported));
        for level in &self.levels {
            map.insert(level.as_str().to_string(), supported_obj(self.supported));
        }
        JsonValue::Object(map)
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextManagementCap {
    #[serde(default = "default_true")]
    pub supported: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<ContextFeature>,
}

impl ContextManagementCap {
    fn to_wire(&self) -> JsonValue {
        let mut map = JsonMap::new();
        map.insert("supported".to_string(), JsonValue::Bool(self.supported));
        for feature in &self.features {
            map.insert(feature.as_str().to_string(), supported_obj(self.supported));
        }
        JsonValue::Object(map)
    }
}

/// Per-profile capability overrides used by Anthropic-compatible `/v1/models`.
/// Each configured capability replaces that one key while unconfigured keys
/// retain the upstream/default advertisement.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesConfig {
    pub batch: Option<SimpleCap>,
    pub citations: Option<SimpleCap>,
    pub code_execution: Option<SimpleCap>,
    pub image_input: Option<SimpleCap>,
    pub pdf_input: Option<SimpleCap>,
    pub structured_outputs: Option<SimpleCap>,
    pub thinking: Option<ThinkingCap>,
    pub effort: Option<EffortCap>,
    pub context_management: Option<ContextManagementCap>,
}

impl CapabilitiesConfig {
    pub fn merge_into(&self, mut base: JsonValue) -> JsonValue {
        let Some(map) = base.as_object_mut() else {
            return base;
        };
        macro_rules! replace {
            ($field:ident) => {
                if let Some(cap) = &self.$field {
                    map.insert(stringify!($field).to_string(), cap.to_wire());
                }
            };
        }
        replace!(batch);
        replace!(citations);
        replace!(code_execution);
        replace!(image_input);
        replace!(pdf_input);
        replace!(structured_outputs);
        replace!(thinking);
        replace!(effort);
        replace!(context_management);
        base
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    #[default]
    Accept,
    Reject,
    Drop,
    Rewrite,
}

impl Action {
    fn is_accept(&self) -> bool {
        *self == Self::Accept
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum When {
    Leading,
    Inline,
    Always,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RoleRule {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when: Option<When>,
    #[serde(skip_serializing_if = "Action::is_accept")]
    pub action: Action,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub tag_attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RoleRuleSet {
    pub rules: Vec<RoleRule>,
}

impl<'de> Deserialize<'de> for RoleRuleSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        let rules = if value.is_array() {
            serde_json::from_value(value).map_err(DeError::custom)?
        } else {
            vec![serde_json::from_value(value).map_err(DeError::custom)?]
        };
        Ok(Self { rules })
    }
}

impl Serialize for RoleRuleSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.rules.len() == 1 {
            self.rules[0].serialize(serializer)
        } else {
            self.rules.serialize(serializer)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RolesConfig {
    pub merge_adjacent: Vec<String>,
    pub rules: BTreeMap<String, RoleRuleSet>,
}

impl<'de> Deserialize<'de> for RolesConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        let JsonValue::Object(map) = value else {
            return Err(DeError::custom(
                "roles must be a map of role names to rules",
            ));
        };
        let mut merge_adjacent = Vec::new();
        let mut rules = BTreeMap::new();
        for (key, value) in map {
            if key == "merge_adjacent" {
                merge_adjacent = serde_json::from_value(value).map_err(DeError::custom)?;
            } else {
                rules.insert(key, serde_json::from_value(value).map_err(DeError::custom)?);
            }
        }
        Ok(Self {
            merge_adjacent,
            rules,
        })
    }
}

impl Serialize for RolesConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(
            self.rules.len() + usize::from(!self.merge_adjacent.is_empty()),
        ))?;
        if !self.merge_adjacent.is_empty() {
            map.serialize_entry("merge_adjacent", &self.merge_adjacent)?;
        }
        for (key, rules) in &self.rules {
            map.serialize_entry(key, rules)?;
        }
        map.end()
    }
}

impl RolesConfig {
    pub fn rules_for(&self, role: &str) -> Option<&[RoleRule]> {
        self.rules
            .get(role)
            .or_else(|| self.rules.get("*"))
            .map(|set| set.rules.as_slice())
    }

    fn validate(&self) -> Result<(), String> {
        for (role, set) in &self.rules {
            for rule in &set.rules {
                let has_target = rule
                    .target_role
                    .as_deref()
                    .is_some_and(|target| !target.trim().is_empty());
                if rule.action == Action::Rewrite && !has_target {
                    return Err(format!(
                        "roles[{role}]: action `rewrite` requires a non-empty `target_role`"
                    ));
                }
                if rule.action != Action::Rewrite && rule.target_role.is_some() {
                    return Err(format!(
                        "roles[{role}]: `target_role` is only valid with action `rewrite`"
                    ));
                }
                if rule.tag.as_deref().is_some_and(|tag| !valid_tag_name(tag)) {
                    return Err(format!("roles[{role}]: invalid tag name"));
                }
                if !rule.tag_attributes.is_empty() && rule.tag.is_none() {
                    return Err(format!(
                        "roles[{role}]: `tag_attributes` requires a non-empty `tag`"
                    ));
                }
                if rule.tag_attributes.keys().any(|key| !valid_tag_name(key)) {
                    return Err(format!("roles[{role}]: invalid tag attribute name"));
                }
            }
        }
        // `merge_adjacent` collapses a run of same-role messages into ONE,
        // discarding every non-content field (tool_call_id/name/reasoning/
        // tool_calls). That is lossy for roles that carry those — merging
        // `assistant` drops its tool_calls, merging `tool` orphans a
        // tool_call_id — so restrict merging to content-only roles. This also
        // keeps the merge safe to re-run before every upstream send.
        const MERGE_SAFE_ROLES: [&str; 3] = ["system", "developer", "user"];
        for role in &self.merge_adjacent {
            if !MERGE_SAFE_ROLES.contains(&role.as_str()) {
                return Err(format!(
                    "merge_adjacent[{role}]: only content-only roles {MERGE_SAFE_ROLES:?} may be merged (merging `tool`/`assistant` discards tool_call_id/tool_calls)"
                ));
            }
        }
        Ok(())
    }
}

fn valid_tag_name(name: &str) -> bool {
    let name = name.trim();
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.'))
}

/// E2b: policy for a residual `InputImage` (one the G4 image agent did not
/// strip — agent inactive/disabled, `tool_choice=="none"`, a `file_id` image,
/// a non-`user`-role image, or old history) that would otherwise reach a
/// non-native-vision backend. `Placeholder` (the default) replaces it with an
/// instructive text placeholder so the model self-corrects instead of the
/// upstream 400ing on raw image bytes (the field incident this closes);
/// `Reject` fails the turn before dispatch with a 4xx instead. No `Drop`
/// variant — silently discarding image content with no signal to the model or
/// client is a defect, not a policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedImagePolicy {
    #[default]
    Placeholder,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    #[default]
    Disabled,
    Enforce,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub store_path: PathBuf,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Disabled,
            store_path: PathBuf::from("llmconduit-auth.sqlite3"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct PersistedAuthConfig {
    pub mode: AuthMode,
    pub store_path: String,
}

impl Default for PersistedAuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Disabled,
            store_path: "llmconduit-auth.sqlite3".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub upstream_base_url: Url,
    pub upstream_api_key: Option<String>,
    pub upstream_model: Option<String>,
    pub system_prompt_prefix: Option<String>,
    pub upstream_request_log_path: Option<PathBuf>,
    /// F1 (Topic F): opt-in durable per-turn capture directory. When set,
    /// every instrumented inference turn writes `<dir>/<api_call_id>.json`
    /// with the full inbound request, on-wire upstream request, raw upstream
    /// response, and served response bytes (redacted). `None` disables the
    /// capture sink entirely (zero-op: no thread, no alloc, no fs).
    pub turn_capture_dir: Option<PathBuf>,
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    pub provider_metrics_targets: Vec<crate::provider_metrics::ProviderMetricsTarget>,
    pub upstreams: Vec<UpstreamConfig>,
    pub fallback_upstreams: Vec<FallbackUpstreamConfig>,
    pub upstream_failure_cooldown_secs: u64,
    pub model_profiles: BTreeMap<String, ModelProfile>,
    /// Ad-hoc model routes (G7). Each route maps a request-model *name* (which
    /// may be a glob pattern such as `claude-opus-*`) to a synthetic upstream
    /// (base URL + optional upstream model). Routes turn the gateway into
    /// routing mode and are matched in `RoutingModelCatalog::resolve` strictly
    /// between an exact catalog model id and the canonical-key/default
    /// fallbacks, so an exact upstream model id always beats a glob route.
    /// DECLARATION order is preserved (file order, then CLI `--model-route`
    /// merged in) so the FIRST matching glob wins when two globs overlap.
    pub model_routes: Vec<ModelRoute>,
    /// Forces the backend chat-template contract (`kimi`/`deepseek`) regardless
    /// of the model name, when family auto-detection from the model id is wrong
    /// (G2). Profile-level `template_family` overrides this global value.
    pub template_family: Option<String>,
    pub brave_base_url: Url,
    pub brave_api_key: Option<String>,
    pub brave_max_results: usize,
    pub request_timeout: Duration,
    pub connect_timeout_secs: u64,
    pub max_web_search_rounds: usize,
    pub flatten_content: bool,
    pub max_replay_entries: usize,
    pub debug_log_max_age_hours: Option<u64>,
    /// Floor for the reduced completion budget when retrying a context-window
    /// overflow (G1). A shrink-and-retry never pushes `max_completion_tokens`
    /// below this value, so a near-full prompt still gets a usable (if small)
    /// completion budget instead of being clamped to zero/negative.
    pub min_completion_tokens: i64,
    /// Per-frame byte ceiling for the UPSTREAM SSE read path (G6, DoS guard).
    /// A hostile/buggy upstream that streams an oversized or never-terminated
    /// SSE frame (no `\n\n` event boundary) would otherwise grow the parser
    /// buffer without bound. When the bytes accumulated since the last event
    /// boundary exceed this value, the stream is rejected as an `AppError`
    /// before unbounded accumulation. The inbound-request-body cap in `http.rs`
    /// does NOT cover this response-read path.
    pub max_sse_frame_bytes: usize,
    /// Maximum inbound HTTP request body size in bytes, applied as axum's
    /// `DefaultBodyLimit`. A request whose JSON body exceeds this is rejected
    /// with HTTP 413 before model resolution or any upstream call. Defaults to
    /// 10 MiB (raise for very large prompts). Distinct from `max_sse_frame_bytes`,
    /// which bounds the UPSTREAM response read path, not the inbound request.
    pub max_request_body_bytes: usize,
    /// Master switch for the G4 image agent (vision offload). When `false` the
    /// strip/cache seam and `analyzeImage` tool injection are skipped entirely
    /// and images flow to the upstream unchanged.
    pub image_agent_enabled: bool,
    /// OpenAI-compatible chat-completions endpoint of the vision backend the
    /// image agent forwards stripped images to. `None` disables the agent even
    /// when `image_agent_enabled` is true (no endpoint to call), matching
    /// claude-relay's "skip without `vision_url`" gate.
    pub vision_url: Option<Url>,
    /// Model id sent to the vision backend.
    pub vision_model: Option<String>,
    /// Per-session LRU image-cache capacity.
    pub image_cache_max_size: usize,
    /// Per-session image-cache TTL (seconds).
    pub image_cache_ttl_secs: u64,
    /// E2b: policy applied to a residual image reaching a non-native-vision
    /// backend after G4 gating. See [`UnsupportedImagePolicy`].
    pub unsupported_image_policy: UnsupportedImagePolicy,
    /// Per-model price table (T13/D13), keyed by SERVED model id. Drives the
    /// dashboard's flow `cost` roll-up + the Sankey cost coloring + the
    /// `cost_per_min`/`cost_per_sec` rates. Loaded from the YAML `price_table:`
    /// map and overridable wholesale by `LLMCONDUIT_PRICE_TABLE_JSON`. Empty by
    /// default — an absent model simply has no price (cost stays `None`/0), which
    /// is contract-valid (the frontend only requires finite rates when present).
    pub price_table: HashMap<String, ModelPrice>,
    /// Inference authorization configuration. The key-verification pepper and
    /// optional bootstrap key remain environment-only and are never retained in
    /// this `Debug + Clone` structure.
    pub auth: AuthConfig,
    pub mesh: MeshConfig,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MeshConfig {
    pub controller: MeshControllerConfig,
    pub worker: MeshWorkerConfig,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeshControllerConfig {
    pub enabled: bool,
    pub bind_addr: SocketAddr,
    pub identity_path: Option<PathBuf>,
    pub state_path: Option<PathBuf>,
    pub heartbeat_timeout_secs: u64,
    pub model_allowlist: BTreeMap<String, BTreeMap<String, Vec<String>>>,
}

impl Default for MeshControllerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_addr: "0.0.0.0:4433"
                .parse()
                .expect("default mesh controller bind address is valid"),
            identity_path: None,
            state_path: None,
            heartbeat_timeout_secs: 30,
            model_allowlist: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeshWorkerConfig {
    pub controller_addr: Option<String>,
    pub controller_endpoint_id: Option<String>,
    pub identity_path: Option<PathBuf>,
    pub node_name: Option<String>,
    pub heartbeat_interval_secs: u64,
    pub resources: Vec<MeshWorkerResourceConfig>,
}

impl Default for MeshWorkerConfig {
    fn default() -> Self {
        Self {
            controller_addr: None,
            controller_endpoint_id: None,
            identity_path: None,
            node_name: None,
            heartbeat_interval_secs: 10,
            resources: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeshWorkerResourceConfig {
    pub id: String,
    pub target: SocketAddr,
    pub models: Vec<String>,
    pub model_refresh_secs: u64,
    pub capacity_source: MeshWorkerCapacitySource,
    pub availability: AvailabilitySchedule,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeshWorkerCapacitySource {
    #[default]
    Configured,
    Vllm,
}

impl MeshWorkerCapacitySource {
    fn is_configured(&self) -> bool {
        *self == Self::Configured
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AvailabilitySchedule {
    pub timezone: String,
    pub default_capacity: u32,
    pub weekly: Vec<WeeklyCapacityWindow>,
    pub exceptions: Vec<CapacityException>,
}

impl Default for AvailabilitySchedule {
    fn default() -> Self {
        Self {
            timezone: "UTC".to_string(),
            default_capacity: 0,
            weekly: Vec::new(),
            exceptions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeeklyCapacityWindow {
    pub days: Vec<MeshWeekday>,
    pub start_local: LocalTime,
    pub end_local: LocalTime,
    pub capacity: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacityException {
    pub start: DateTime<FixedOffset>,
    pub end: DateTime<FixedOffset>,
    pub capacity: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MeshWeekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

impl MeshWeekday {
    fn index(self) -> usize {
        match self {
            Self::Mon => 0,
            Self::Tue => 1,
            Self::Wed => 2,
            Self::Thu => 3,
            Self::Fri => 4,
            Self::Sat => 5,
            Self::Sun => 6,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalTime {
    hour: u8,
    minute: u8,
}

impl LocalTime {
    fn minutes_since_midnight(self) -> u16 {
        u16::from(self.hour) * 60 + u16::from(self.minute)
    }
}

impl<'de> Deserialize<'de> for LocalTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_local_time(&value).map_err(DeError::custom)
    }
}

impl Serialize for LocalTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{:02}:{:02}", self.hour, self.minute))
    }
}

/// One model's billing rates (T13/D13), per 1k tokens. Field names mirror the
/// FROZEN frontend `ModelPrice` contract (`dashboard-frontend/src/api/types.ts`)
/// byte-for-byte so the `/dashboard/api/topology` `price_table` validates. All
/// three rates are finite (the frontend `isModelPrice` guard rejects NaN/Inf);
/// `cached_per_1k` defaults to `0.0` when a config entry omits it. This is the
/// SINGLE `ModelPrice` definition for the crate — the dashboard WS topology
/// snapshot re-exports it so REST + WS agree on the wire shape.
///
/// Gap 07 — cached-price PRESENCE seam. `cached_per_1k` keeps its existing numeric
/// type (`f64`, default `0.0`), but a `0.0` is AMBIGUOUS: "the provider charges 0
/// for cache reads" is indistinguishable from "the config entry omitted the rate".
/// The ADDITIVE `cached_price_configured` boolean records which it is — set `true`
/// only when the source actually carried a `cached_per_1k` key (decided in the
/// custom [`Deserialize`] below). Downstream cost-CONFIDENCE (`dashboard_api`)
/// consumes THIS flag, NOT the numeric `0.0`: a flow that billed cached tokens at a
/// CONFIGURED rate is `confident`, one that fell back to the default `0.0` is
/// `estimated`. The flag is serialized additively (the frontend `isModelPrice`
/// accepts it); `cached_per_1k` stays `number` so the topology/Sankey price table is
/// NOT a second contract migration (spec 07 item 3).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, utoipa::ToSchema)]
pub struct ModelPrice {
    /// USD per 1k PROMPT (input) tokens.
    pub input_per_1k: f64,
    /// USD per 1k COMPLETION (output) tokens.
    pub output_per_1k: f64,
    /// USD per 1k CACHED (cache-read) prompt tokens. Defaults to `0.0` when the
    /// config entry omits it (a provider with no cache discount). PRESERVES its
    /// numeric type (gap 07) — presence is carried by `cached_price_configured`, not
    /// by nulling this field.
    pub cached_per_1k: f64,
    /// Gap 07 — whether `cached_per_1k` was EXPLICITLY configured (a `cached_per_1k`
    /// key was present in the source), distinguishing a real configured `0.0`
    /// cache-read rate from an OMITTED one (which also defaults to `0.0`). The
    /// cost-confidence seam reads this so a default-`0.0` cached charge is flagged
    /// `estimated`, never silently `confident`. Additive on the wire.
    #[serde(default)]
    pub cached_price_configured: bool,
}

impl ModelPrice {
    /// Construct a price with the cached rate EXPLICITLY configured (presence `true`).
    /// The constructor for callers that genuinely set a cache-read rate (tests,
    /// programmatic config). Use [`ModelPrice::without_cached`] when there is no
    /// configured cache rate (presence `false`, rate defaults to `0.0`).
    pub fn new(input_per_1k: f64, output_per_1k: f64, cached_per_1k: f64) -> Self {
        Self {
            input_per_1k,
            output_per_1k,
            cached_per_1k,
            cached_price_configured: true,
        }
    }

    /// Construct a price with NO configured cache-read rate (presence `false`): the
    /// `cached_per_1k` numeric is the default `0.0` but `cached_price_configured` is
    /// `false`, so the cost-confidence seam treats a cached charge as `estimated`.
    pub fn without_cached(input_per_1k: f64, output_per_1k: f64) -> Self {
        Self {
            input_per_1k,
            output_per_1k,
            cached_per_1k: 0.0,
            cached_price_configured: false,
        }
    }

    /// Whether ALL three rates are finite (no NaN/±∞). The frozen `ModelPrice`
    /// contract (and the frontend `isModelPrice` guard) requires finite rates;
    /// `serde_json` serializes NaN/±∞ as `null`, which would silently corrupt the
    /// `/dashboard/api/topology` price table on the wire. Config load uses this to
    /// REJECT a malformed entry so the in-memory table only ever holds finite prices
    /// (D13 R1 MED).
    fn is_finite(&self) -> bool {
        self.input_per_1k.is_finite()
            && self.output_per_1k.is_finite()
            && self.cached_per_1k.is_finite()
    }
}

/// Custom `Deserialize` for [`ModelPrice`] that captures cached-price PRESENCE (gap
/// 07). `cached_per_1k` is read as an `Option<f64>`: `Some` ⇒ the source carried the
/// key, `None` ⇒ it was omitted (rate then defaults to `0.0`). This is how a real
/// configured `0.0` cache-read rate is distinguished from an absent one — both have a
/// `0.0` numeric, but only the configured one drives a `confident` cost.
///
/// PRESENCE round-trip (gap 07 review round 1, finding 3): `ModelPrice` SERIALIZES the
/// additive `cached_price_configured` boolean (via the derived `Serialize`), so a
/// re-parse MUST honor an EXPLICIT flag rather than re-deriving presence solely from
/// `cached_per_1k`. Otherwise a [`ModelPrice::without_cached`] price (numeric
/// `cached_per_1k: 0.0`, presence `false`) round-trips as CONFIGURED — its serialized
/// `cached_per_1k: 0.0` is `Some`, so the old `is_some()` heuristic flipped presence to
/// `true` and a default-`0.0` cached charge was silently re-tagged `confident`. So:
/// PREFER the explicit `cached_price_configured` when present; fall back to the
/// presence-of-`cached_per_1k` heuristic ONLY when the flag is absent (a hand-written
/// YAML entry that gives a rate but no flag is still treated as configured). YAML and the
/// `LLMCONDUIT_PRICE_TABLE_JSON` env override both feed through here.
impl<'de> Deserialize<'de> for ModelPrice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            input_per_1k: f64,
            output_per_1k: f64,
            #[serde(default)]
            cached_per_1k: Option<f64>,
            // An EXPLICIT presence flag (re-read on a round-trip of a serialized price).
            // `None` ⇒ the source omitted it (hand-written config) ⇒ fall back to the
            // cached_per_1k-presence heuristic below.
            #[serde(default)]
            cached_price_configured: Option<bool>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(ModelPrice {
            input_per_1k: raw.input_per_1k,
            output_per_1k: raw.output_per_1k,
            cached_per_1k: raw.cached_per_1k.unwrap_or(0.0),
            // Prefer the explicit flag (round-trips a serialized `false` faithfully);
            // else derive from whether a `cached_per_1k` key was present.
            cached_price_configured: raw
                .cached_price_configured
                .unwrap_or_else(|| raw.cached_per_1k.is_some()),
        })
    }
}

/// Drop any price-table entry carrying a non-finite rate (NaN/±∞), logging the key
/// so a misconfiguration is visible. YAML and the `LLMCONDUIT_PRICE_TABLE_JSON` env
/// override both feed through here, so the in-memory `price_table` only ever holds
/// finite `ModelPrice`s and the topology price serialization is always finite (D13
/// R1 MED — `serde_json` would otherwise emit `null` for a NaN/Inf rate, violating
/// the frozen finite-number contract the frontend `isModelPrice` guard rejects).
fn retain_finite_prices(table: &mut HashMap<String, ModelPrice>) {
    table.retain(|model, price| {
        let finite = price.is_finite();
        if !finite {
            tracing::warn!(
                model = %model,
                "dropping price_table entry with non-finite rate (NaN/Inf)"
            );
        }
        finite
    });
}

#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    pub name: String,
    pub upstream_base_url: Url,
    pub upstream_api_key: Option<String>,
    pub upstream_model: Option<String>,
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    pub upstream_request_log_path: Option<PathBuf>,
    pub fallback_upstreams: Vec<FallbackUpstreamConfig>,
}

/// A resolved ad-hoc model route (G7): request-model name → synthetic upstream.
#[derive(Debug, Clone)]
pub struct ModelRoute {
    /// The request-model name this route matches. May be a literal id or a glob
    /// pattern (`*`, `?`, `[...]`) such as `claude-opus-*`.
    pub name: String,
    /// Compiled, case-insensitive matcher when `name` is a glob pattern; `None`
    /// for a literal name (matched with `eq_ignore_ascii_case`). Compiled once
    /// here so an invalid pattern is a clean startup error, not a later panic.
    pub glob: Option<Regex>,
    /// Base URL of the synthetic upstream this route forwards to.
    pub upstream_base_url: Url,
    /// Optional upstream model id to send to that upstream. When `None`, the
    /// request model flows through unchanged.
    pub upstream_model: Option<String>,
}

impl PartialEq for ModelRoute {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.glob.as_ref().map(Regex::as_str) == other.glob.as_ref().map(Regex::as_str)
            && self.upstream_base_url == other.upstream_base_url
            && self.upstream_model == other.upstream_model
    }
}

/// Whether `model` matches ANY of `routes` by exact name (case-insensitive) or
/// glob. The single route-matching primitive shared by config-side route checks
/// and the routing catalog's dispatch (`RoutingModelCatalog::match_route` returns
/// the SPECIFIC matching route for dispatch; this is the boolean projection).
/// Trims + rejects empty input.
pub fn route_matches(routes: &[ModelRoute], model: &str) -> bool {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return false;
    }
    routes.iter().any(|route| match &route.glob {
        Some(glob) => glob.is_match(trimmed),
        None => route.name.eq_ignore_ascii_case(trimmed),
    })
}

/// Persisted form of a model route. Accepts either a bare URL string
/// (`name = "http://host:8000"`) or a table with `upstream_base_url`/`url` and
/// `upstream_model`/`model`, mirroring claude-relay's str-or-table coercion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(from = "RawPersistedModelRoute")]
pub struct PersistedModelRoute {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_model: Option<String>,
}

/// Untagged input shape for `PersistedModelRoute`: either a bare string URL or a
/// table with URL/model aliases.
#[derive(Deserialize)]
#[serde(untagged)]
enum RawPersistedModelRoute {
    Url(String),
    Table {
        #[serde(default)]
        upstream_base_url: Option<String>,
        #[serde(default)]
        url: Option<String>,
        #[serde(default)]
        upstream_model: Option<String>,
        #[serde(default)]
        model: Option<String>,
    },
}

impl From<RawPersistedModelRoute> for PersistedModelRoute {
    fn from(raw: RawPersistedModelRoute) -> Self {
        match raw {
            RawPersistedModelRoute::Url(url) => Self {
                upstream_base_url: Some(url),
                upstream_model: None,
            },
            RawPersistedModelRoute::Table {
                upstream_base_url,
                url,
                upstream_model,
                model,
            } => Self {
                upstream_base_url: upstream_base_url.or(url),
                upstream_model: upstream_model.or(model),
            },
        }
    }
}

/// Ordered set of persisted model routes, keyed by request-model name.
///
/// A `Vec` of pairs rather than a `BTreeMap` so glob routes keep their
/// DECLARATION order: overlapping globs are scanned first-match-wins, and a
/// `BTreeMap` would silently re-sort keys alphabetically (e.g. `claude-*`
/// sorting before `claude-opus-*`) and mis-route. Both serde_yaml and the
/// `toml` crate (with `preserve_order`) yield map entries in document order, so
/// the file order is the routing order. CLI `--model-route` specs are merged in
/// declaration order too (replace-in-place on a name match, else append).
///
/// It (de)serializes as a MAP (`name: route`), not a sequence, so a config
/// written by `write_persisted_config` round-trips back through the map
/// deserializer; declaration order is preserved on write.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrderedModelRoutes(pub Vec<(String, PersistedModelRoute)>);

impl OrderedModelRoutes {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Insert a route by name, replacing an existing entry IN PLACE (preserving
    /// its position) or appending when the name is new. Used to layer CLI routes
    /// over file routes — and to collapse duplicate keys to last-wins — without
    /// disturbing declaration order.
    ///
    /// Name comparison is TRIMMED + ASCII-case-insensitive, identical to route
    /// DISPATCH (`Config::matches_model_route` / `RoutingModelCatalog::match_route`),
    /// so e.g. a CLI `claude-*` route overrides a file `Claude-*` route in place
    /// rather than adding a shadowed duplicate. The replacing entry adopts the
    /// new name (CLI/last value wins) but keeps the original position.
    pub fn upsert(&mut self, name: String, route: PersistedModelRoute) {
        let key = name.trim();
        if let Some(existing) = self
            .0
            .iter_mut()
            .find(|(existing, _)| existing.trim().eq_ignore_ascii_case(key))
        {
            existing.0 = name;
            existing.1 = route;
        } else {
            self.0.push((name, route));
        }
    }
}

impl Serialize for OrderedModelRoutes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        // Emit as a MAP in declaration order so the written config reloads
        // through `visit_map` (a sequence would not round-trip).
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, route) in &self.0 {
            map.serialize_entry(name, route)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for OrderedModelRoutes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RoutesVisitor;

        impl<'de> serde::de::Visitor<'de> for RoutesVisitor {
            type Value = OrderedModelRoutes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map of model-route name to route definition")
            }

            fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let mut routes =
                    OrderedModelRoutes(Vec::with_capacity(access.size_hint().unwrap_or(0)));
                while let Some((name, route)) =
                    access.next_entry::<String, PersistedModelRoute>()?
                {
                    // Collapse duplicate keys to last-wins (replace in place),
                    // matching CLI-override and claude-relay dict semantics,
                    // rather than keeping a shadowed first entry.
                    routes.upsert(name, route);
                }
                Ok(routes)
            }
        }

        deserializer.deserialize_map(RoutesVisitor)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct PersistedModelProfile {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extends: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roles: Option<RolesConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_family: Option<String>,
    /// Per-profile override for whether the resolved backend can natively see
    /// images (G4). `Some(true)` forces the image agent OFF for this profile
    /// (the backend is multimodal); `Some(false)` forces text-only handling
    /// even for a name the family sniff would treat as native-vision. `None`
    /// defers to the name-based default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_vision: Option<bool>,
    /// Opt-in repairs for malformed upstream tool calls (`crate::tool_repair`).
    /// A non-empty list REPLACES an inherited one; omitted keeps it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_call_repairs: Vec<crate::tool_repair::ToolCallRepair>,
    #[serde(default, skip_serializing_if = "JsonMap::is_empty")]
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    /// Per-model map from a canonical reasoning-effort level (`none`/`low`/
    /// `medium`/`high`/`xhigh`/`max`) to a request fragment merged into the
    /// upstream chat request — e.g. `{chat_template_kwargs: {reasoning_effort:
    /// high}}` for GLM, or `{chat_template_kwargs: {enable_thinking: false}}` to
    /// turn reasoning off. When a level matches, the fragment is merged (as a
    /// default; an explicit client/configured value wins) and the top-level
    /// `reasoning_effort` is cleared, so the fragment alone decides placement.
    /// This lets a backend with its own effort vocabulary receive the right knob
    /// instead of the model-agnostic low/high clamp. Empty = use the default.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub reasoning_effort_map: BTreeMap<String, JsonValue>,
    /// Canonical level used when the client requested no effort but this model
    /// has a `reasoning_effort_map` (e.g. GLM's template default is `max`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort_default: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<CapabilitiesConfig>,
    /// Upstream-compatible shorthand for effort remapping and thinking-kwarg
    /// control. Mutually exclusive with the fragment-based effort fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningConfig>,
}

impl<'de> Deserialize<'de> for PersistedModelProfile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawPersistedModelProfile {
            #[serde(default)]
            extends: Vec<String>,
            #[serde(default)]
            upstream_model: Option<String>,
            #[serde(default)]
            system_prompt_prefix: Option<String>,
            #[serde(default)]
            roles: Option<RolesConfig>,
            #[serde(default)]
            template_family: Option<String>,
            #[serde(default)]
            native_vision: Option<bool>,
            #[serde(default)]
            tool_call_repairs: Vec<crate::tool_repair::ToolCallRepair>,
            #[serde(default)]
            upstream_chat_kwargs: JsonMap<String, JsonValue>,
            #[serde(default)]
            reasoning_effort_map: BTreeMap<String, JsonValue>,
            #[serde(default)]
            reasoning_effort_default: Option<String>,
            #[serde(default)]
            capabilities: Option<CapabilitiesConfig>,
            #[serde(default)]
            reasoning_effort: Option<ReasoningConfig>,
            #[serde(default, flatten)]
            shorthand_upstream_chat_kwargs: JsonMap<String, JsonValue>,
        }

        let raw = RawPersistedModelProfile::deserialize(deserializer)?;
        let mut upstream_chat_kwargs = raw.shorthand_upstream_chat_kwargs;
        // `template_family`/`native_vision`/effort knobs are recognized profile
        // fields, not chat-template shorthand kwargs, so drop any copy the
        // `flatten` swept into the shorthand bucket (they live in typed fields).
        upstream_chat_kwargs.remove("template_family");
        upstream_chat_kwargs.remove("native_vision");
        upstream_chat_kwargs.remove("tool_call_repairs");
        upstream_chat_kwargs.remove("reasoning_effort_map");
        upstream_chat_kwargs.remove("reasoning_effort_default");
        upstream_chat_kwargs.remove("reasoning_effort");
        upstream_chat_kwargs.remove("roles");
        upstream_chat_kwargs.remove("capabilities");
        merge_json_maps(&mut upstream_chat_kwargs, &raw.upstream_chat_kwargs);
        Ok(Self {
            extends: raw.extends,
            upstream_model: raw.upstream_model,
            system_prompt_prefix: raw.system_prompt_prefix,
            roles: raw.roles,
            template_family: raw.template_family,
            native_vision: raw.native_vision,
            tool_call_repairs: raw.tool_call_repairs,
            upstream_chat_kwargs,
            reasoning_effort_map: raw.reasoning_effort_map,
            reasoning_effort_default: raw.reasoning_effort_default,
            capabilities: raw.capabilities,
            reasoning_effort: raw.reasoning_effort,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModelProfile {
    pub upstream_model: Option<String>,
    pub system_prompt_prefix: Option<String>,
    pub roles: Option<RolesConfig>,
    pub template_family: Option<String>,
    /// Per-profile native-vision override (G4); see `PersistedModelProfile`.
    pub native_vision: Option<bool>,
    /// Opt-in repairs for malformed upstream tool calls (see
    /// `crate::tool_repair`). Empty = the upstream's tool calls are forwarded
    /// exactly as they arrive.
    pub tool_call_repairs: Vec<crate::tool_repair::ToolCallRepair>,
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    /// Per-model reasoning-effort map + default; see `PersistedModelProfile`.
    pub reasoning_effort_map: BTreeMap<String, JsonValue>,
    pub reasoning_effort_default: Option<String>,
    pub capabilities: Option<CapabilitiesConfig>,
    pub reasoning_effort: Option<ReasoningConfig>,
}

/// Resolved reasoning-effort policy for a backend model: canonical effort level
/// → request fragment, plus the default level for an effort-less request.
#[derive(Debug, Clone)]
pub struct ReasoningEffortPolicy {
    pub map: BTreeMap<String, JsonValue>,
    pub default: Option<String>,
    /// Present when the policy came from the upstream-compatible typed syntax.
    /// Unmapped levels pass through verbatim and the dynamic thinking kwarg is
    /// taken from this config.
    pub upstream_reasoning: Option<ReasoningConfig>,
}

#[derive(Debug, Clone)]
pub struct FallbackUpstreamConfig {
    pub name: String,
    pub upstream_base_url: Url,
    pub upstream_api_key: Option<String>,
    pub upstream_model: Option<String>,
    pub exposed_model: Option<String>,
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    pub upstream_request_log_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct PersistedFallbackUpstream {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub upstream_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exposed_model: Option<String>,
    #[serde(default, skip_serializing_if = "JsonMap::is_empty")]
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_request_log_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_source: Option<crate::provider_metrics::MetricsSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct PersistedUpstream {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub upstream_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_model: Option<String>,
    #[serde(default, skip_serializing_if = "JsonMap::is_empty")]
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_request_log_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_source: Option<crate::provider_metrics::MetricsSource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fallback_upstreams: Vec<PersistedFallbackUpstream>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_upstream_base_url")]
    pub upstream_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_source: Option<crate::provider_metrics::MetricsSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_request_log_path: Option<String>,
    /// F1: opt-in durable per-turn capture directory (see
    /// `Config::turn_capture_dir`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_capture_dir: Option<String>,
    #[serde(default, skip_serializing_if = "JsonMap::is_empty")]
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upstreams: Vec<PersistedUpstream>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fallback_upstreams: Vec<PersistedFallbackUpstream>,
    #[serde(default = "default_upstream_failure_cooldown_secs")]
    pub upstream_failure_cooldown_secs: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub model_profile_templates: BTreeMap<String, PersistedModelProfile>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub model_profiles: BTreeMap<String, PersistedModelProfile>,
    /// Ad-hoc model routes (G7): request-model name (possibly a glob) →
    /// synthetic upstream, in DECLARATION order (see `OrderedModelRoutes`). CLI
    /// `--model-route` specs are merged in after these.
    #[serde(default, skip_serializing_if = "OrderedModelRoutes::is_empty")]
    pub model_routes: OrderedModelRoutes,
    /// Global override for the backend chat-template family (`kimi`/`deepseek`).
    /// A matched model profile's `template_family` takes precedence (G2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_family: Option<String>,
    #[serde(default = "default_brave_base_url")]
    pub brave_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brave_api_key: Option<String>,
    #[serde(default = "default_brave_max_results")]
    pub brave_max_results: usize,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_max_web_search_rounds")]
    pub max_web_search_rounds: usize,
    #[serde(default = "default_flatten_content")]
    pub flatten_content: bool,
    #[serde(default = "default_max_replay_entries")]
    pub max_replay_entries: usize,
    /// Opt-in age-based cleanup of debug/request-log dump files. `None` (the
    /// default) disables rotation entirely so behavior is opt-in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub debug_log_max_age_hours: Option<u64>,
    #[serde(default = "default_min_completion_tokens")]
    pub min_completion_tokens: i64,
    /// Per-frame byte ceiling for the upstream SSE read path (G6 DoS guard).
    /// Bounds the bytes accumulated between event boundaries; an oversized or
    /// unterminated frame is rejected before unbounded buffer growth.
    #[serde(default = "default_max_sse_frame_bytes")]
    pub max_sse_frame_bytes: usize,
    /// Maximum inbound HTTP request body size in bytes (axum `DefaultBodyLimit`).
    /// Bodies larger than this are rejected with HTTP 413. Defaults to 10 MiB.
    #[serde(default = "default_max_request_body_bytes")]
    pub max_request_body_bytes: usize,
    /// Master switch for the G4 image agent (vision offload). Off by default so
    /// the gateway's text-first design is preserved unless explicitly opted in.
    #[serde(default)]
    pub image_agent_enabled: bool,
    /// OpenAI-compatible chat-completions endpoint of the vision backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_url: Option<String>,
    /// Model id sent to the vision backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    /// Per-session LRU image-cache capacity.
    #[serde(default = "default_image_cache_max_size")]
    pub image_cache_max_size: usize,
    /// Per-session image-cache TTL (seconds).
    #[serde(default = "default_image_cache_ttl_secs")]
    pub image_cache_ttl_secs: u64,
    /// E2b: policy for a residual image reaching a non-native-vision backend.
    /// Defaults to `placeholder` (never a silent `drop`) — see
    /// [`UnsupportedImagePolicy`].
    #[serde(default = "default_unsupported_image_policy")]
    pub unsupported_image_policy: UnsupportedImagePolicy,
    /// Per-model price table (T13/D13), keyed by served model id. A YAML map of
    /// `model: { input_per_1k, output_per_1k, cached_per_1k? }`. Wholesale-
    /// overridable by `LLMCONDUIT_PRICE_TABLE_JSON` (mirrors the
    /// `upstream_chat_kwargs` env-JSON pattern). Empty by default.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub price_table: HashMap<String, ModelPrice>,
    #[serde(default, skip_serializing_if = "is_default_auth_config")]
    pub auth: PersistedAuthConfig,
    #[serde(default, skip_serializing_if = "PersistedMeshConfig::is_default")]
    pub mesh: PersistedMeshConfig,
}

fn is_default_auth_config(config: &PersistedAuthConfig) -> bool {
    config == &PersistedAuthConfig::default()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct PersistedMeshConfig {
    #[serde(
        default,
        skip_serializing_if = "PersistedMeshControllerConfig::is_default"
    )]
    pub controller: PersistedMeshControllerConfig,
    #[serde(default, skip_serializing_if = "PersistedMeshWorkerConfig::is_default")]
    pub worker: PersistedMeshWorkerConfig,
}

impl PersistedMeshConfig {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedMeshControllerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_mesh_controller_bind_addr")]
    pub bind_addr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_path: Option<String>,
    #[serde(default = "default_mesh_heartbeat_timeout_secs")]
    pub heartbeat_timeout_secs: u64,
    /// Controller-side routing approval map:
    /// endpoint_id -> resource_id -> allowed model ids. An empty map is
    /// fail-closed: enrolled workers remain connected but cannot receive work.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub model_allowlist: BTreeMap<String, BTreeMap<String, Vec<String>>>,
}

impl Default for PersistedMeshControllerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_addr: default_mesh_controller_bind_addr(),
            identity_path: None,
            state_path: None,
            heartbeat_timeout_secs: default_mesh_heartbeat_timeout_secs(),
            model_allowlist: BTreeMap::new(),
        }
    }
}

impl PersistedMeshControllerConfig {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedMeshWorkerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_endpoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    #[serde(default = "default_mesh_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<PersistedMeshWorkerResourceConfig>,
}

impl Default for PersistedMeshWorkerConfig {
    fn default() -> Self {
        Self {
            controller_addr: None,
            controller_endpoint_id: None,
            identity_path: None,
            node_name: None,
            heartbeat_interval_secs: default_mesh_heartbeat_interval_secs(),
            resources: Vec::new(),
        }
    }
}

impl PersistedMeshWorkerConfig {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedMeshWorkerResourceConfig {
    pub id: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
    #[serde(default = "default_mesh_model_refresh_secs")]
    pub model_refresh_secs: u64,
    #[serde(
        default,
        skip_serializing_if = "MeshWorkerCapacitySource::is_configured"
    )]
    pub capacity_source: MeshWorkerCapacitySource,
    #[serde(default)]
    pub availability: PersistedAvailabilitySchedule,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedAvailabilitySchedule {
    #[serde(default = "default_mesh_timezone")]
    pub timezone: String,
    #[serde(default)]
    pub default_capacity: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub weekly: Vec<PersistedWeeklyCapacityWindow>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exceptions: Vec<PersistedCapacityException>,
}

impl Default for PersistedAvailabilitySchedule {
    fn default() -> Self {
        Self {
            timezone: default_mesh_timezone(),
            default_capacity: 0,
            weekly: Vec::new(),
            exceptions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedWeeklyCapacityWindow {
    pub days: Vec<MeshWeekday>,
    pub start: LocalTime,
    pub end: LocalTime,
    pub capacity: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedCapacityException {
    pub start: DateTime<FixedOffset>,
    pub end: DateTime<FixedOffset>,
    pub capacity: u32,
}

fn default_bind_addr() -> String {
    "127.0.0.1:4000".to_string()
}

fn default_upstream_base_url() -> String {
    "http://127.0.0.1:8000/v1".to_string()
}

fn default_mesh_controller_bind_addr() -> String {
    "0.0.0.0:4433".to_string()
}

fn default_mesh_heartbeat_timeout_secs() -> u64 {
    30
}

fn default_mesh_heartbeat_interval_secs() -> u64 {
    10
}

fn default_mesh_model_refresh_secs() -> u64 {
    60
}

fn default_mesh_timezone() -> String {
    "UTC".to_string()
}

fn default_brave_base_url() -> String {
    "https://api.search.brave.com/res/v1".to_string()
}

fn default_brave_max_results() -> usize {
    5
}

fn default_request_timeout_secs() -> u64 {
    60
}

fn default_connect_timeout_secs() -> u64 {
    10
}

fn default_max_web_search_rounds() -> usize {
    5
}

fn default_flatten_content() -> bool {
    true
}

fn default_max_replay_entries() -> usize {
    1000
}

fn default_upstream_failure_cooldown_secs() -> u64 {
    30
}

fn default_min_completion_tokens() -> i64 {
    4096
}

/// Default upstream SSE per-frame cap: 8 MiB. Comfortably above any sane single
/// model-output SSE event (typical chunks are well under 1 MiB) so normal
/// streaming is never affected, while still bounding a hostile/unterminated
/// frame far below the memory a single oversized accumulation could exhaust.
/// Returns [`crate::sse_guard::DEFAULT_MAX_SSE_FRAME_BYTES`], the single source
/// of truth shared with the direct-client default.
fn default_max_sse_frame_bytes() -> usize {
    crate::sse_guard::DEFAULT_MAX_SSE_FRAME_BYTES
}

/// Default inbound request-body cap: 10 MiB. Generous enough for very long
/// prompts and multi-message conversations while bounding per-request memory.
fn default_max_request_body_bytes() -> usize {
    10 * 1024 * 1024
}

/// Default per-session image-cache capacity (G4). Generous enough for a normal
/// multi-image turn while bounding memory.
fn default_image_cache_max_size() -> usize {
    100
}

/// Default per-session image-cache TTL in seconds (G4), matching claude-relay's
/// 300s default.
fn default_image_cache_ttl_secs() -> u64 {
    300
}

/// Default E2b residual-image policy: replace with an instructive placeholder
/// rather than reject the turn, so an existing deployment that upgrades keeps
/// serving `200`s (matching the pre-E2b common case) instead of newly 4xx-ing
/// every image-bearing turn against a non-native-vision backend.
fn default_unsupported_image_policy() -> UnsupportedImagePolicy {
    UnsupportedImagePolicy::Placeholder
}

impl Default for PersistedConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            upstream_base_url: default_upstream_base_url(),
            upstream_api_key: None,
            upstream_model: None,
            metrics_url: None,
            metrics_source: None,
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: default_upstream_failure_cooldown_secs(),
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::new(),
            model_routes: OrderedModelRoutes::default(),
            template_family: None,
            brave_base_url: default_brave_base_url(),
            brave_api_key: None,
            brave_max_results: default_brave_max_results(),
            request_timeout_secs: default_request_timeout_secs(),
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            debug_log_max_age_hours: None,
            min_completion_tokens: default_min_completion_tokens(),
            max_sse_frame_bytes: default_max_sse_frame_bytes(),
            max_request_body_bytes: default_max_request_body_bytes(),
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: default_image_cache_max_size(),
            image_cache_ttl_secs: default_image_cache_ttl_secs(),
            unsupported_image_policy: default_unsupported_image_policy(),
            price_table: HashMap::new(),
            auth: PersistedAuthConfig::default(),
            mesh: PersistedMeshConfig::default(),
        }
    }
}

impl Config {
    pub fn from_env_and_file(path: Option<&Path>) -> Result<Self, String> {
        Self::from_env_file_and_routes(path, &[])
    }

    /// Like `from_env_and_file`, but additionally merges `--model-route` CLI
    /// specs (G7) after the file and env overrides, so a CLI route wins over a
    /// file route with the same name. A malformed spec is a clean `Err`.
    pub fn from_env_file_and_routes(
        path: Option<&Path>,
        route_specs: &[String],
    ) -> Result<Self, String> {
        let persisted = if let Some(path) = path {
            load_persisted_config(path)?
        } else {
            load_default_persisted_config()?
        };
        Self::from_persisted_env_and_routes(persisted, route_specs)
    }

    /// Resolve an already-materialized persisted document through the same env
    /// and CLI route precedence as the normal file loader. Used by the
    /// namespaced control-plane wrapper after its lossless profile overlay.
    pub fn from_persisted_env_and_routes(
        mut persisted: PersistedConfig,
        route_specs: &[String],
    ) -> Result<Self, String> {
        apply_env_overrides(&mut persisted);
        for spec in route_specs {
            let (name, route) = parse_model_route_spec(spec)?;
            // Replace a same-named file route in place (preserve its position),
            // else append — keeps glob declaration order intact while letting
            // CLI win on a name conflict.
            persisted.model_routes.upsert(name, route);
        }
        Self::from_persisted(&persisted)
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }

    /// Deduplicated parent directories of the request-log paths the running
    /// gateway *actually writes to* for the configured mode. Used by debug-log
    /// rotation so it only cleans directories that receive dump files, without
    /// inventing a second log-path concept.
    ///
    /// This mirrors how `build_app_with_gateway_and_options` wires upstreams:
    /// - Routing mode (`upstreams` non-empty): only the per-routing-upstream
    ///   paths and their nested fallbacks are active. The top-level
    ///   `upstream_request_log_path` and the global `fallback_upstreams` are
    ///   ignored by the gateway, so they are excluded here too.
    /// - Single/failover mode (`upstreams` empty): the top-level path plus the
    ///   global `fallback_upstreams` paths are active.
    /// - Explicit-upstream routing (`upstreams` non-empty, no `model_routes`):
    ///   per-routing-upstream primaries and their nested fallbacks.
    /// - Routes-only (`model_routes` non-empty, `upstreams` empty): route
    ///   providers all write the top-level `upstream_request_log_path`, so the
    ///   single/failover branch covers them.
    /// - Mixed (`upstreams` + `model_routes`): per-routing-upstream paths PLUS
    ///   the top-level path (route providers always use the top-level path).
    ///
    /// F1: `turn_capture_dir`, when set, is ALWAYS included (unconditionally,
    /// last) regardless of routing/failover mode -- it is a single
    /// engine-level directory, not a per-provider request-log path, and it IS
    /// the directory (not a file whose parent must be extracted).
    pub fn debug_log_dirs(&self) -> Vec<PathBuf> {
        let mut dirs: Vec<PathBuf> = Vec::new();
        let mut push_dir = |path: Option<&PathBuf>| {
            if let Some(dir) = path.and_then(|path| path.parent()) {
                // Treat a bare filename (no parent component) as the current dir.
                let dir = if dir.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    dir.to_path_buf()
                };
                if !dirs.contains(&dir) {
                    dirs.push(dir);
                }
            }
        };

        if self.upstreams.is_empty() {
            // Single/failover OR routes-only mode: top-level primary + global
            // fallbacks. Route providers (G7) all write the top-level path, so
            // this branch covers routes-only configs too.
            push_dir(self.upstream_request_log_path.as_ref());
            for fallback in &self.fallback_upstreams {
                push_dir(fallback.upstream_request_log_path.as_ref());
            }
        } else {
            // Explicit-upstream routing mode: per-routing-upstream primaries and
            // their nested fallbacks.
            for upstream in &self.upstreams {
                push_dir(upstream.upstream_request_log_path.as_ref());
                for fallback in &upstream.fallback_upstreams {
                    push_dir(fallback.upstream_request_log_path.as_ref());
                }
            }
            // In mixed mode (`upstreams` + `model_routes`), route providers
            // still write the top-level `upstream_request_log_path`, which the
            // loop above does not include. Add it so route-log dirs are cleaned
            // too (no-op dedup if it coincides with a routing-upstream path).
            if !self.model_routes.is_empty() {
                push_dir(self.upstream_request_log_path.as_ref());
            }
        }
        // F1: `turn_capture_dir` IS the directory artifacts land in directly
        // (`<dir>/<api_call_id>.json`), unlike the request-log paths above
        // (which are FILE paths whose parent `push_dir` extracts). It is also
        // engine-level, not per-provider, so -- unlike the branches above --
        // it is collected unconditionally, independent of routing/failover
        // mode.
        if let Some(dir) = self.turn_capture_dir.as_ref()
            && !dirs.contains(dir)
        {
            dirs.push(dir.clone());
        }
        dirs
    }

    pub fn from_persisted(config: &PersistedConfig) -> Result<Self, String> {
        let bind_addr = config
            .bind_addr
            .parse()
            .map_err(|err| format!("invalid bind_addr: {err}"))?;
        let upstream_base_url = Url::parse(&config.upstream_base_url)
            .map_err(|err| format!("invalid upstream_base_url: {err}"))?;
        let brave_base_url = Url::parse(&config.brave_base_url)
            .map_err(|err| format!("invalid brave_base_url: {err}"))?;
        let fallback_upstreams = config
            .fallback_upstreams
            .iter()
            .enumerate()
            .map(|(index, provider)| parse_fallback_upstream(provider, index, "fallback_upstreams"))
            .collect::<Result<Vec<_>, String>>()?;
        let upstreams = config
            .upstreams
            .iter()
            .enumerate()
            .map(parse_upstream)
            .collect::<Result<Vec<_>, String>>()?;
        let mut provider_metrics_targets = Vec::new();
        if let Some(target) = parse_provider_metrics_target(
            "primary",
            config.metrics_url.as_deref(),
            config.metrics_source,
            "root",
        )? {
            provider_metrics_targets.push(target);
        }
        for (index, (persisted, resolved)) in config
            .fallback_upstreams
            .iter()
            .zip(&fallback_upstreams)
            .enumerate()
        {
            if let Some(target) = parse_provider_metrics_target(
                &resolved.name,
                persisted.metrics_url.as_deref(),
                persisted.metrics_source,
                &format!("fallback_upstreams[{index}]"),
            )? {
                provider_metrics_targets.push(target);
            }
        }
        for (index, (persisted, resolved)) in config.upstreams.iter().zip(&upstreams).enumerate() {
            if let Some(target) = parse_provider_metrics_target(
                &resolved.name,
                persisted.metrics_url.as_deref(),
                persisted.metrics_source,
                &format!("upstreams[{index}]"),
            )? {
                provider_metrics_targets.push(target);
            }
            for (fallback_index, (persisted_fallback, resolved_fallback)) in persisted
                .fallback_upstreams
                .iter()
                .zip(&resolved.fallback_upstreams)
                .enumerate()
            {
                if let Some(target) = parse_provider_metrics_target(
                    &resolved_fallback.name,
                    persisted_fallback.metrics_url.as_deref(),
                    persisted_fallback.metrics_source,
                    &format!("upstreams[{index}].fallback_upstreams[{fallback_index}]"),
                )? {
                    provider_metrics_targets.push(target);
                }
            }
        }
        let model_profiles =
            resolve_model_profiles(&config.model_profiles, &config.model_profile_templates)?;
        let model_routes = resolve_model_routes(&config.model_routes)?;
        let mesh = parse_mesh_config(&config.mesh)?;
        let vision_url = match trim_nonempty(config.vision_url.as_deref()) {
            Some(url) => {
                Some(Url::parse(&url).map_err(|err| format!("invalid vision_url: {err}"))?)
            }
            None => None,
        };
        Ok(Self {
            bind_addr,
            upstream_base_url,
            upstream_api_key: config
                .upstream_api_key
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            upstream_model: config
                .upstream_model
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            system_prompt_prefix: config
                .system_prompt_prefix
                .as_ref()
                .filter(|value| !value.trim().is_empty())
                .cloned(),
            upstream_request_log_path: config
                .upstream_request_log_path
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            turn_capture_dir: config
                .turn_capture_dir
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            upstream_chat_kwargs: config.upstream_chat_kwargs.clone(),
            provider_metrics_targets,
            upstreams,
            fallback_upstreams,
            upstream_failure_cooldown_secs: config.upstream_failure_cooldown_secs,
            model_profiles,
            model_routes,
            template_family: normalize_template_family(config.template_family.as_deref()),
            brave_base_url,
            brave_api_key: config
                .brave_api_key
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            brave_max_results: config.brave_max_results,
            request_timeout: Duration::from_secs(config.request_timeout_secs),
            connect_timeout_secs: config.connect_timeout_secs,
            max_web_search_rounds: config.max_web_search_rounds,
            flatten_content: config.flatten_content,
            max_replay_entries: config.max_replay_entries,
            debug_log_max_age_hours: config.debug_log_max_age_hours,
            min_completion_tokens: config.min_completion_tokens.max(1),
            // Floor at 1 KiB so a misconfigured tiny/zero cap cannot reject every
            // normal frame; the default is far larger.
            max_sse_frame_bytes: config.max_sse_frame_bytes.max(1024),
            // Floor at 1 KiB so a misconfigured tiny/zero cap cannot reject every
            // request; the default (10 MiB) is far larger.
            max_request_body_bytes: config.max_request_body_bytes.max(1024),
            image_agent_enabled: config.image_agent_enabled,
            vision_url,
            vision_model: trim_nonempty(config.vision_model.as_deref()),
            // Floor the capacity at 1 so a misconfigured zero does not make the
            // cache evict every image immediately and silently disable the agent.
            image_cache_max_size: config.image_cache_max_size.max(1),
            image_cache_ttl_secs: config.image_cache_ttl_secs,
            unsupported_image_policy: config.unsupported_image_policy,
            price_table: {
                // D13 R1 MED: reject any YAML price entry with a non-finite rate so
                // the resolved table only holds finite prices (the topology price
                // serialization is then always finite).
                let mut table = config.price_table.clone();
                retain_finite_prices(&mut table);
                table
            },
            auth: AuthConfig {
                mode: config.auth.mode,
                store_path: PathBuf::from(config.auth.store_path.trim()),
            },
            mesh,
        })
    }

    pub fn resolve_upstream_model(&self, request_model: &str) -> String {
        self.model_profile(request_model)
            .and_then(|profile| profile.upstream_model.clone())
            .or_else(|| self.upstream_model.clone())
            .unwrap_or_else(|| request_model.to_string())
    }

    /// The configured [`ModelPrice`] for `model` (T13/D13). Exact key match first,
    /// then an ASCII-case-insensitive fallback (mirrors `model_profile`'s lookup),
    /// so a price keyed `GLM-4.6` still matches a served `glm-4.6`. `None` when no
    /// price is configured for the model — the dashboard then reports no `cost`
    /// for that flow (contract: `cost` is `Option`), never a fabricated zero.
    pub fn price_for(&self, model: &str) -> Option<ModelPrice> {
        self.price_table.get(model).copied().or_else(|| {
            self.price_table
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(model))
                .map(|(_, price)| *price)
        })
    }

    /// Whether `model` matches an ad-hoc `model_routes` entry by exact name
    /// (case-insensitive) or glob, via the shared [`route_matches`] primitive
    /// (the SAME boolean projection `RoutingModelCatalog::match_route` uses for
    /// dispatch, G7). The engine consults this so a route-bound request model is
    /// NOT pre-collapsed to a catalog/default model before the routing client can
    /// dispatch it. Slots between an exact catalog id and the canonical-key/
    /// default fallbacks: callers must check an exact catalog id FIRST so a
    /// catalog id still beats a route.
    pub fn matches_model_route(&self, model: &str) -> bool {
        route_matches(&self.model_routes, model)
    }

    /// Plain single-provider mode: no `upstreams` (routing), no `model_routes`
    /// (ad-hoc routes), no top-level `fallback_upstreams` (non-routing failover).
    /// In this mode the engine's own `/v1/models` catalog IS the single served
    /// provider, so G3 budgeting may fall back to it when the candidate plan has
    /// no known window. In any other mode the candidate plan is the authoritative
    /// resolver (routing/failover rewrites the model pre-first-chunk), so the
    /// engine union catalog must NOT be used as a budgeting fallback — it could
    /// mask a failover target's smaller window or budget a routed model against
    /// the wrong window (T9).
    pub fn is_plain_single_provider(&self) -> bool {
        self.upstreams.is_empty()
            && self.model_routes.is_empty()
            && self.fallback_upstreams.is_empty()
    }

    /// Per-BACKEND-MODEL reasoning-effort policies, keyed by the resolved model
    /// id (profile name). Applied at the upstream LEAF — the single point that
    /// knows the FINAL provider model after routing/failover/exposed-alias remap
    /// — so a route/failover target gets its OWN model's effort vocabulary rather
    /// than the request alias's. Both the fork's fragment-based map and the
    /// upstream-compatible typed shorthand are compiled into the same leaf
    /// policy, after profile inheritance has resolved.
    pub fn reasoning_effort_policies(&self) -> BTreeMap<String, ReasoningEffortPolicy> {
        self.model_profiles
            .iter()
            .filter(|(_, profile)| {
                !profile.reasoning_effort_map.is_empty() || profile.reasoning_effort.is_some()
            })
            .map(|(name, profile)| {
                let (map, default, upstream_reasoning) =
                    if let Some(reasoning) = &profile.reasoning_effort {
                        let mut map: BTreeMap<String, JsonValue> = reasoning
                            .map
                            .iter()
                            .map(|(level, mapped)| {
                                (
                                    level.trim().to_ascii_lowercase(),
                                    serde_json::json!({"reasoning_effort": mapped}),
                                )
                            })
                            .collect();
                        let default = trim_nonempty(reasoning.default.as_deref())
                            .map(|level| level.to_ascii_lowercase());
                        // Upstream semantics do not run the configured default
                        // back through `map`: the default is emitted verbatim.
                        if let Some(level) = &default {
                            map.insert(
                                level.clone(),
                                serde_json::json!({"reasoning_effort": level}),
                            );
                        }
                        (map, default, Some(reasoning.clone()))
                    } else {
                        (
                            profile
                                .reasoning_effort_map
                                .iter()
                                .map(|(level, fragment)| {
                                    (level.trim().to_ascii_lowercase(), fragment.clone())
                                })
                                .collect(),
                            trim_nonempty(profile.reasoning_effort_default.as_deref())
                                .map(|level| level.to_ascii_lowercase()),
                            None,
                        )
                    };
                (
                    name.clone(),
                    ReasoningEffortPolicy {
                        map,
                        default,
                        upstream_reasoning,
                    },
                )
            })
            .collect()
    }

    /// Per-BACKEND-MODEL `template_family` override policies, keyed by the
    /// resolved model id (profile name). Applied at the upstream LEAF — the
    /// single point that knows the FINAL provider model after routing/failover/
    /// exposed-alias remap — so a route/failover target gets its OWN model's
    /// family override rather than the request alias's (T1). Each profile's
    /// `template_family` is already normalized to `kimi`/`deepseek` at
    /// construction. Only profiles that set a family are included; the GLOBAL
    /// `template_family` is exposed separately by [`global_template_family`]
    /// and the leaf folds it in as the fallback when no per-model policy matches.
    ///
    /// [`global_template_family`]: Config::global_template_family
    pub fn template_family_policies(&self) -> BTreeMap<String, String> {
        self.model_profiles
            .iter()
            .filter_map(|(name, profile)| {
                profile
                    .template_family
                    .clone()
                    .map(|family| (name.clone(), family))
            })
            .collect()
    }

    /// The GLOBAL `template_family` override (already normalized), applied by
    /// the leaf as the fallback when no per-model policy matches the FINAL
    /// model. Exposed so the upstream leaf can resolve family against the
    /// post-routing model without consulting the engine (T1).
    pub fn global_template_family(&self) -> Option<String> {
        self.template_family.clone()
    }

    /// Per-BACKEND-MODEL `upstream_chat_kwargs` policies, keyed by the resolved
    /// model id (profile name). Applied at the upstream LEAF — the single point
    /// that knows the FINAL provider model after routing/failover/exposed-alias
    /// remap — so a route/failover target gets its OWN model's kwargs rather
    /// than the request alias's (T1). Each profile's `upstream_chat_kwargs` is
    /// already extends-merged at construction. Only non-empty profiles are
    /// included; the GLOBAL `upstream_chat_kwargs` is exposed separately by
    /// [`global_upstream_chat_kwargs`] and the leaf merges it as the base layer
    /// under the per-profile policy.
    ///
    /// [`global_upstream_chat_kwargs`]: Config::global_upstream_chat_kwargs
    pub fn upstream_chat_kwargs_policies(&self) -> BTreeMap<String, JsonMap<String, JsonValue>> {
        self.model_profiles
            .iter()
            .filter_map(|(name, profile)| {
                if profile.upstream_chat_kwargs.is_empty() {
                    None
                } else {
                    Some((name.clone(), profile.upstream_chat_kwargs.clone()))
                }
            })
            .collect()
    }

    /// The GLOBAL `upstream_chat_kwargs` (base layer), merged by the leaf under
    /// the per-profile policy for the FINAL model. Exposed for the upstream leaf
    /// (T1); the engine no longer pre-merges profile kwargs.
    pub fn global_upstream_chat_kwargs(&self) -> &JsonMap<String, JsonValue> {
        &self.upstream_chat_kwargs
    }

    /// Direct, PROFILE-ONLY `native_vision` lookup for EXACTLY `model` (G4
    /// round-9 #1). Looks up the profile keyed on `model` and returns its
    /// (already template-resolved) `native_vision`, with NO `upstream_model`
    /// remap. This is the ONLY native_vision accessor G4 gating uses: each input
    /// is already a final backend model (a candidate) or the literal request
    /// model, so re-applying the `upstream_model` remap would judge a DIFFERENT
    /// model's profile than the one the provider receives / than the request
    /// actually carries.
    pub fn profile_native_vision(&self, model: &str) -> Option<bool> {
        self.model_profile(model)
            .and_then(|profile| profile.native_vision)
    }

    /// The tool-call repairs configured for EXACTLY `model` (the final backend
    /// model, matching `profile_native_vision`'s lookup). Empty unless a profile
    /// opts in: a repair rewrites what the upstream sent, so it is never implied.
    pub fn profile_tool_call_repairs(&self, model: &str) -> &[crate::tool_repair::ToolCallRepair] {
        self.model_profile(model)
            .map(|profile| profile.tool_call_repairs.as_slice())
            .unwrap_or_default()
    }

    pub fn resolve_system_prompt_prefix(&self, request_model: &str) -> Option<String> {
        let upstream_model = self.resolve_upstream_model(request_model);
        self.resolve_system_prompt_prefix_for_resolved_model(request_model, &upstream_model)
    }

    pub fn resolve_system_prompt_prefix_for_resolved_model(
        &self,
        request_model: &str,
        resolved_model: &str,
    ) -> Option<String> {
        let profile_prefix = self
            .model_profiles_for_resolved_model(request_model, resolved_model)
            .into_iter()
            .rev()
            .find_map(|profile| profile.system_prompt_prefix.clone());
        join_prompt_prefixes(
            [self.system_prompt_prefix.clone(), profile_prefix]
                .into_iter()
                .flatten(),
        )
    }

    /// Resolve the capability overrides advertised for an upstream model id.
    /// An id-keyed profile wins, followed by the first alias targeting that id;
    /// the reserved `*` profile is the final fallback.
    pub fn resolve_capabilities_for_upstream(&self, id: &str) -> Option<&CapabilitiesConfig> {
        if let Some(profile) = self.model_profile(id) {
            return profile.capabilities.as_ref();
        }
        for profile in self.model_profiles.values() {
            if profile
                .upstream_model
                .as_deref()
                .is_some_and(|model| model.eq_ignore_ascii_case(id))
            {
                return profile.capabilities.as_ref();
            }
        }
        self.model_profile("*")
            .and_then(|profile| profile.capabilities.as_ref())
    }

    pub fn resolve_roles_config_for_resolved_model(
        &self,
        request_model: &str,
        resolved_model: &str,
    ) -> Option<&RolesConfig> {
        self.model_profiles_for_resolved_model(request_model, resolved_model)
            .into_iter()
            .rev()
            .find_map(|profile| profile.roles.as_ref())
    }

    fn model_profiles_for_resolved_model(
        &self,
        request_model: &str,
        resolved_model: &str,
    ) -> Vec<&ModelProfile> {
        let mut profiles: Vec<&ModelProfile> = Vec::new();
        let configured_model = self.resolve_upstream_model(request_model);
        for model in [resolved_model, configured_model.as_str(), request_model] {
            if let Some(profile) = self.model_profile(model)
                && !profiles
                    .iter()
                    .any(|existing| std::ptr::eq(*existing, profile))
            {
                profiles.push(profile);
            }
        }
        if profiles.is_empty()
            && let Some(profile) = self.model_profile("*")
        {
            profiles.push(profile);
        }
        profiles
    }

    fn model_profile(&self, request_model: &str) -> Option<&ModelProfile> {
        self.model_profiles.get(request_model).or_else(|| {
            self.model_profiles
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(request_model))
                .map(|(_, profile)| profile)
        })
    }
}

#[derive(Debug, Clone, Default)]
struct ResolvedModelProfile {
    upstream_model: Option<String>,
    system_prompt_prefixes: Vec<String>,
    roles: Option<RolesConfig>,
    template_family: Option<String>,
    native_vision: Option<bool>,
    tool_call_repairs: Vec<crate::tool_repair::ToolCallRepair>,
    upstream_chat_kwargs: JsonMap<String, JsonValue>,
    reasoning_effort_map: BTreeMap<String, JsonValue>,
    reasoning_effort_default: Option<String>,
    capabilities: Option<CapabilitiesConfig>,
    reasoning_effort: Option<ReasoningConfig>,
}

impl ResolvedModelProfile {
    fn into_model_profile(self) -> ModelProfile {
        ModelProfile {
            upstream_model: self.upstream_model,
            system_prompt_prefix: join_prompt_prefixes(self.system_prompt_prefixes),
            roles: self.roles,
            template_family: normalize_template_family(self.template_family.as_deref()),
            native_vision: self.native_vision,
            tool_call_repairs: self.tool_call_repairs,
            upstream_chat_kwargs: self.upstream_chat_kwargs,
            reasoning_effort_map: self.reasoning_effort_map,
            reasoning_effort_default: self.reasoning_effort_default,
            capabilities: self.capabilities,
            reasoning_effort: self.reasoning_effort,
        }
    }
}

/// True when a route key contains glob metacharacters and should be matched as
/// a pattern. Mirrors claude-relay's `_is_glob_pattern` (`*`, `?`, `[`).
pub(crate) fn is_glob_pattern(value: &str) -> bool {
    value.contains(['*', '?', '['])
}

const MAX_MESH_CONFIGURED_CAPACITY: u32 = 65_535;
const MAX_MESH_RESOURCE_ID_BYTES: usize = 128;
const MAX_MESH_RESOURCE_MODELS: usize = 1024;
const MAX_MESH_MODEL_ID_BYTES: usize = 512;

fn parse_mesh_config(config: &PersistedMeshConfig) -> Result<MeshConfig, String> {
    let controller_bind_addr = config
        .controller
        .bind_addr
        .parse()
        .map_err(|err| format!("invalid mesh.controller.bind_addr: {err}"))?;
    let controller_identity_path =
        trim_nonempty(config.controller.identity_path.as_deref()).map(PathBuf::from);
    let controller_state_path =
        trim_nonempty(config.controller.state_path.as_deref()).map(PathBuf::from);
    if config.controller.enabled {
        if controller_identity_path.is_none() {
            return Err(
                "mesh.controller.identity_path is required when controller is enabled".to_string(),
            );
        }
        if controller_state_path.is_none() {
            return Err(
                "mesh.controller.state_path is required when controller is enabled".to_string(),
            );
        }
        if config.controller.heartbeat_timeout_secs == 0 {
            return Err(
                "mesh.controller.heartbeat_timeout_secs must be greater than zero".to_string(),
            );
        }
    }
    validate_mesh_model_allowlist(&config.controller.model_allowlist)?;

    let worker_configured = config.worker.controller_addr.is_some()
        || config.worker.controller_endpoint_id.is_some()
        || config.worker.identity_path.is_some()
        || config.worker.node_name.is_some()
        || !config.worker.resources.is_empty()
        || config.worker.heartbeat_interval_secs != default_mesh_heartbeat_interval_secs();
    let worker_controller_addr = trim_nonempty(config.worker.controller_addr.as_deref());
    let worker_controller_endpoint_id =
        trim_nonempty(config.worker.controller_endpoint_id.as_deref());
    let worker_identity_path =
        trim_nonempty(config.worker.identity_path.as_deref()).map(PathBuf::from);
    let worker_node_name =
        parse_optional_mesh_worker_node_name(config.worker.node_name.as_deref())?;
    if worker_configured {
        if worker_controller_addr.is_none() {
            return Err("mesh.worker.controller_addr is required for worker mode".to_string());
        }
        if worker_controller_endpoint_id.is_none() {
            return Err(
                "mesh.worker.controller_endpoint_id is required for worker mode".to_string(),
            );
        }
        if worker_identity_path.is_none() {
            return Err("mesh.worker.identity_path is required for worker mode".to_string());
        }
        if config.worker.resources.is_empty() {
            return Err(
                "mesh.worker.resources must contain at least one resource for worker mode"
                    .to_string(),
            );
        }
        if config.worker.heartbeat_interval_secs == 0 {
            return Err(
                "mesh.worker.heartbeat_interval_secs must be greater than zero".to_string(),
            );
        }
    }

    let resources = config
        .worker
        .resources
        .iter()
        .enumerate()
        .map(parse_mesh_worker_resource)
        .collect::<Result<Vec<_>, String>>()?;

    Ok(MeshConfig {
        controller: MeshControllerConfig {
            enabled: config.controller.enabled,
            bind_addr: controller_bind_addr,
            identity_path: controller_identity_path,
            state_path: controller_state_path,
            heartbeat_timeout_secs: config.controller.heartbeat_timeout_secs,
            model_allowlist: config.controller.model_allowlist.clone(),
        },
        worker: MeshWorkerConfig {
            controller_addr: worker_controller_addr,
            controller_endpoint_id: worker_controller_endpoint_id,
            identity_path: worker_identity_path,
            node_name: worker_node_name,
            heartbeat_interval_secs: config.worker.heartbeat_interval_secs,
            resources,
        },
    })
}

fn parse_optional_mesh_worker_node_name(value: Option<&str>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("mesh.worker.node_name must not be blank".to_string());
    }
    let max = crate::mesh::protocol::MAX_NODE_NAME_BYTES;
    if trimmed.len() > max {
        return Err(format!("mesh.worker.node_name must be at most {max} bytes"));
    }
    if trimmed.chars().any(char::is_control) {
        return Err("mesh.worker.node_name must not contain control characters".to_string());
    }
    Ok(Some(trimmed.to_string()))
}

fn validate_mesh_model_allowlist(
    allowlist: &BTreeMap<String, BTreeMap<String, Vec<String>>>,
) -> Result<(), String> {
    for (endpoint_id, resources) in allowlist {
        if endpoint_id.trim().is_empty() {
            return Err(
                "mesh.controller.model_allowlist endpoint id must not be blank".to_string(),
            );
        }
        if endpoint_id.chars().any(char::is_control) {
            return Err(
                "mesh.controller.model_allowlist endpoint id must not contain control characters"
                    .to_string(),
            );
        }
        if resources.is_empty() {
            return Err(format!(
                "mesh.controller.model_allowlist[{endpoint_id}] must approve at least one resource"
            ));
        }
        for (resource_id, models) in resources {
            if resource_id.trim().is_empty() {
                return Err(format!(
                    "mesh.controller.model_allowlist[{endpoint_id}] resource id must not be blank"
                ));
            }
            if resource_id.len() > MAX_MESH_RESOURCE_ID_BYTES {
                return Err(format!(
                    "mesh.controller.model_allowlist[{endpoint_id}].{resource_id} must be at most {MAX_MESH_RESOURCE_ID_BYTES} bytes"
                ));
            }
            if resource_id.chars().any(char::is_control) {
                return Err(format!(
                    "mesh.controller.model_allowlist[{endpoint_id}] resource id must not contain control characters"
                ));
            }
            if models.is_empty() {
                return Err(format!(
                    "mesh.controller.model_allowlist[{endpoint_id}].{resource_id} must approve at least one model"
                ));
            }
            if models.len() > MAX_MESH_RESOURCE_MODELS {
                return Err(format!(
                    "mesh.controller.model_allowlist[{endpoint_id}].{resource_id} must contain at most {MAX_MESH_RESOURCE_MODELS} models"
                ));
            }
            for model in models {
                if model.trim().is_empty() {
                    return Err(format!(
                        "mesh.controller.model_allowlist[{endpoint_id}].{resource_id} must not contain blank model ids"
                    ));
                }
                if model.len() > MAX_MESH_MODEL_ID_BYTES || model.chars().any(char::is_control) {
                    return Err(format!(
                        "mesh.controller.model_allowlist[{endpoint_id}].{resource_id} contains an invalid model id"
                    ));
                }
            }
        }
    }
    Ok(())
}

fn parse_mesh_worker_resource(
    (index, resource): (usize, &PersistedMeshWorkerResourceConfig),
) -> Result<MeshWorkerResourceConfig, String> {
    let id = resource.id.trim();
    if id.is_empty() {
        return Err(format!(
            "mesh.worker.resources[{index}].id must not be blank"
        ));
    }
    if id.len() > MAX_MESH_RESOURCE_ID_BYTES {
        return Err(format!(
            "mesh.worker.resources[{index}].id must be at most {MAX_MESH_RESOURCE_ID_BYTES} bytes"
        ));
    }
    if resource.model_refresh_secs == 0 {
        return Err(format!(
            "mesh.worker.resources[{index}].model_refresh_secs must be greater than zero"
        ));
    }
    if resource.models.len() > MAX_MESH_RESOURCE_MODELS {
        return Err(format!(
            "mesh.worker.resources[{index}].models must contain at most {MAX_MESH_RESOURCE_MODELS} entries"
        ));
    }
    let models = resource
        .models
        .iter()
        .enumerate()
        .map(|(model_index, model)| {
            let model = model.trim();
            if model.is_empty() {
                return Err(format!(
                    "mesh.worker.resources[{index}].models[{model_index}] must not be blank"
                ));
            }
            if model.len() > MAX_MESH_MODEL_ID_BYTES || model.chars().any(char::is_control) {
                return Err(format!(
                    "mesh.worker.resources[{index}].models[{model_index}] is invalid"
                ));
            }
            Ok(model.to_string())
        })
        .collect::<Result<Vec<_>, String>>()?;
    let target = resource
        .target
        .trim()
        .parse()
        .map_err(|err| format!("invalid mesh.worker.resources[{index}].target: {err}"))?;
    let availability = parse_availability_schedule(
        &resource.availability,
        &format!("mesh.worker.resources[{index}].availability"),
    )?;
    Ok(MeshWorkerResourceConfig {
        id: id.to_string(),
        target,
        models,
        model_refresh_secs: resource.model_refresh_secs,
        capacity_source: resource.capacity_source,
        availability,
    })
}

fn parse_availability_schedule(
    schedule: &PersistedAvailabilitySchedule,
    path: &str,
) -> Result<AvailabilitySchedule, String> {
    let timezone = trim_nonempty(Some(&schedule.timezone))
        .ok_or_else(|| format!("{path}.timezone must not be blank"))?;
    timezone
        .parse::<chrono_tz::Tz>()
        .map_err(|err| format!("{path}.timezone is not a valid IANA timezone: {err}"))?;
    validate_mesh_capacity(
        schedule.default_capacity,
        &format!("{path}.default_capacity"),
    )?;

    let weekly = schedule
        .weekly
        .iter()
        .enumerate()
        .map(|(index, window)| {
            if window.days.is_empty() {
                return Err(format!("{path}.weekly[{index}].days must not be empty"));
            }
            if window.start == window.end {
                return Err(format!(
                    "{path}.weekly[{index}] must not have a zero-length window"
                ));
            }
            validate_mesh_capacity(window.capacity, &format!("{path}.weekly[{index}].capacity"))?;
            Ok(WeeklyCapacityWindow {
                days: window.days.clone(),
                start_local: window.start,
                end_local: window.end,
                capacity: window.capacity,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    validate_weekly_capacity_windows(&weekly, path)?;

    let exceptions = schedule
        .exceptions
        .iter()
        .enumerate()
        .map(|(index, exception)| {
            if exception.end <= exception.start {
                return Err(format!(
                    "{path}.exceptions[{index}].end must be after start"
                ));
            }
            validate_mesh_capacity(
                exception.capacity,
                &format!("{path}.exceptions[{index}].capacity"),
            )?;
            Ok(CapacityException {
                start: exception.start,
                end: exception.end,
                capacity: exception.capacity,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    validate_capacity_exceptions(&exceptions, path)?;

    Ok(AvailabilitySchedule {
        timezone,
        default_capacity: schedule.default_capacity,
        weekly,
        exceptions,
    })
}

fn validate_mesh_capacity(capacity: u32, path: &str) -> Result<(), String> {
    if capacity > MAX_MESH_CONFIGURED_CAPACITY {
        return Err(format!("{path} must be <= {MAX_MESH_CONFIGURED_CAPACITY}"));
    }
    Ok(())
}

fn validate_weekly_capacity_windows(
    weekly: &[WeeklyCapacityWindow],
    path: &str,
) -> Result<(), String> {
    let mut by_day: [Vec<(u16, u16, usize)>; 7] = Default::default();
    for (index, window) in weekly.iter().enumerate() {
        let start = window.start_local.minutes_since_midnight();
        let end = window.end_local.minutes_since_midnight();
        for day in &window.days {
            let day_index = day.index();
            if end > start {
                by_day[day_index].push((start, end, index));
            } else {
                by_day[day_index].push((start, 24 * 60, index));
                by_day[(day_index + 1) % 7].push((0, end, index));
            }
        }
    }
    for windows in &mut by_day {
        windows.sort_by_key(|(start, end, _)| (*start, *end));
        for pair in windows.windows(2) {
            let (_, prev_end, prev_index) = pair[0];
            let (next_start, _, next_index) = pair[1];
            if prev_end > next_start {
                return Err(format!(
                    "{path}.weekly[{prev_index}] overlaps mesh availability window weekly[{next_index}]"
                ));
            }
        }
    }
    Ok(())
}

fn validate_capacity_exceptions(
    exceptions: &[CapacityException],
    path: &str,
) -> Result<(), String> {
    let mut ranges = exceptions
        .iter()
        .enumerate()
        .map(|(index, exception)| (exception.start, exception.end, index))
        .collect::<Vec<_>>();
    ranges.sort_by_key(|(start, end, _)| (*start, *end));
    for pair in ranges.windows(2) {
        let (_, prev_end, prev_index) = pair[0];
        let (next_start, _, next_index) = pair[1];
        if prev_end > next_start {
            return Err(format!(
                "{path}.exceptions[{prev_index}] overlaps mesh availability exception exceptions[{next_index}]"
            ));
        }
    }
    Ok(())
}

fn parse_local_time(value: &str) -> Result<LocalTime, String> {
    let (hour, minute) = value
        .split_once(':')
        .ok_or_else(|| format!("local time {value:?} must use HH:MM"))?;
    let hour = hour
        .parse::<u8>()
        .map_err(|_| format!("local time {value:?} has invalid hour"))?;
    let minute = minute
        .parse::<u8>()
        .map_err(|_| format!("local time {value:?} has invalid minute"))?;
    if hour > 23 || minute > 59 {
        return Err(format!("local time {value:?} must be within 00:00..23:59"));
    }
    Ok(LocalTime { hour, minute })
}

/// Translate a glob pattern into an anchored, case-insensitive `Regex`,
/// approximating Python `fnmatch` semantics: `*` → any run, `?` → one char,
/// `[...]` → a character class (with `[!...]` negation), and every other
/// character matched literally. Returns a clean `Err` for an unparseable class
/// so a bad route is rejected at startup instead of panicking later.
pub(crate) fn glob_to_regex(pattern: &str) -> Result<Regex, String> {
    let mut regex = String::with_capacity(pattern.len() + 8);
    regex.push_str("(?i)^");
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '[' => {
                let mut class = String::from("[");
                if matches!(chars.peek(), Some('!')) {
                    chars.next();
                    class.push('^');
                }
                // A `]` immediately after `[` / `[!` is a literal member.
                if matches!(chars.peek(), Some(']')) {
                    chars.next();
                    class.push_str("\\]");
                }
                let mut closed = false;
                for class_ch in chars.by_ref() {
                    if class_ch == ']' {
                        closed = true;
                        break;
                    }
                    if matches!(class_ch, '\\' | '^') {
                        class.push('\\');
                    }
                    class.push(class_ch);
                }
                if !closed {
                    return Err(format!("unterminated character class in glob {pattern:?}"));
                }
                class.push(']');
                regex.push_str(&class);
            }
            other => regex.push_str(&regex::escape(&other.to_string())),
        }
    }
    regex.push('$');
    Regex::new(&regex).map_err(|err| format!("invalid glob {pattern:?}: {err}"))
}

/// Resolve persisted model routes into ordered `ModelRoute`s. A route with a
/// blank key, a missing/invalid `upstream_base_url`, or an uncompilable glob is
/// a clean startup error, never a panic. Order follows DECLARATION order (file
/// order, then CLI specs merged in before this runs), so an earlier route wins
/// when two globs overlap.
fn resolve_model_routes(routes: &OrderedModelRoutes) -> Result<Vec<ModelRoute>, String> {
    let mut resolved = Vec::with_capacity(routes.0.len());
    for (name, route) in &routes.0 {
        let name = name.trim();
        if name.is_empty() {
            return Err("model_routes: route name must not be blank".to_string());
        }
        let base_url = trim_nonempty(route.upstream_base_url.as_deref())
            .ok_or_else(|| format!("model_routes[{name}]: missing upstream_base_url"))?;
        let upstream_base_url = Url::parse(&base_url)
            .map_err(|err| format!("model_routes[{name}]: invalid upstream_base_url: {err}"))?;
        let glob = if is_glob_pattern(name) {
            Some(glob_to_regex(name).map_err(|err| format!("model_routes[{name}]: {err}"))?)
        } else {
            None
        };
        resolved.push(ModelRoute {
            name: name.to_string(),
            glob,
            upstream_base_url,
            upstream_model: trim_nonempty(route.upstream_model.as_deref()),
        });
    }
    Ok(resolved)
}

/// Parse a `--model-route "name=url[,upstream]"` CLI spec into a persisted route
/// (G7). Malformed specs return an `Err` so startup fails cleanly instead of
/// panicking. Mirrors claude-relay's `parse_model_route`.
pub fn parse_model_route_spec(spec: &str) -> Result<(String, PersistedModelRoute), String> {
    let (name, value) = spec
        .split_once('=')
        .ok_or_else(|| format!("--model-route {spec:?} must use NAME=URL[,UPSTREAM_MODEL]"))?;
    let name = name.trim();
    if name.is_empty() {
        return Err(format!("--model-route {spec:?} is missing NAME"));
    }
    let (url, upstream_model) = match value.split_once(',') {
        Some((url, upstream_model)) => (url.trim(), upstream_model.trim()),
        None => (value.trim(), ""),
    };
    if url.is_empty() {
        return Err(format!("--model-route {spec:?} is missing URL"));
    }
    // Validate the URL eagerly so a malformed spec is rejected here rather than
    // surfacing later from `from_persisted`.
    Url::parse(url).map_err(|err| format!("--model-route {spec:?}: invalid URL: {err}"))?;
    Ok((
        name.to_string(),
        PersistedModelRoute {
            upstream_base_url: Some(url.to_string()),
            upstream_model: (!upstream_model.is_empty()).then(|| upstream_model.to_string()),
        },
    ))
}

fn resolve_model_profiles(
    profiles: &BTreeMap<String, PersistedModelProfile>,
    templates: &BTreeMap<String, PersistedModelProfile>,
) -> Result<BTreeMap<String, ModelProfile>, String> {
    let mut resolved = BTreeMap::new();
    for (name, profile) in profiles {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let profile = resolve_persisted_model_profile(profile, templates, &mut Vec::new())
            .map_err(|err| format!("model_profiles[{name}]: {err}"))?;
        let profile = profile.into_model_profile();
        if let Some(roles) = &profile.roles {
            roles
                .validate()
                .map_err(|err| format!("model_profiles[{name}]: {err}"))?;
        }
        if profile.reasoning_effort.is_some()
            && (!profile.reasoning_effort_map.is_empty()
                || profile.reasoning_effort_default.is_some())
        {
            return Err(format!(
                "model_profiles[{name}]: `reasoning_effort` cannot be combined with `reasoning_effort_map` or `reasoning_effort_default`"
            ));
        }
        resolved.insert(name.to_string(), profile);
    }
    Ok(resolved)
}

fn resolve_persisted_model_profile(
    profile: &PersistedModelProfile,
    templates: &BTreeMap<String, PersistedModelProfile>,
    stack: &mut Vec<String>,
) -> Result<ResolvedModelProfile, String> {
    let mut resolved = ResolvedModelProfile::default();
    for template_name in &profile.extends {
        let template_name = template_name.trim();
        if template_name.is_empty() {
            continue;
        }
        if stack.iter().any(|name| name == template_name) {
            let mut cycle = stack.clone();
            cycle.push(template_name.to_string());
            return Err(format!("template cycle: {}", cycle.join(" -> ")));
        }
        let template = templates
            .get(template_name)
            .or_else(|| {
                templates
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(template_name))
                    .map(|(_, template)| template)
            })
            .ok_or_else(|| format!("unknown template {template_name:?}"))?;
        stack.push(template_name.to_string());
        let template = resolve_persisted_model_profile(template, templates, stack)?;
        stack.pop();
        merge_resolved_model_profile(&mut resolved, template);
    }
    merge_persisted_model_profile(&mut resolved, profile);
    Ok(resolved)
}

fn merge_resolved_model_profile(
    destination: &mut ResolvedModelProfile,
    source: ResolvedModelProfile,
) {
    if source.upstream_model.is_some() {
        destination.upstream_model = source.upstream_model;
    }
    if source.template_family.is_some() {
        destination.template_family = source.template_family;
    }
    if source.native_vision.is_some() {
        destination.native_vision = source.native_vision;
    }
    if !source.tool_call_repairs.is_empty() {
        destination.tool_call_repairs = source.tool_call_repairs.clone();
    }
    if source.roles.is_some() {
        destination.roles = source.roles;
    }
    if source.capabilities.is_some() {
        destination.capabilities = source.capabilities;
    }
    if source.reasoning_effort.is_some() {
        destination.reasoning_effort = source.reasoning_effort;
        destination.reasoning_effort_map.clear();
        destination.reasoning_effort_default = None;
    } else if !source.reasoning_effort_map.is_empty() || source.reasoning_effort_default.is_some() {
        destination.reasoning_effort = None;
    }
    destination
        .system_prompt_prefixes
        .extend(source.system_prompt_prefixes);
    merge_json_maps(
        &mut destination.upstream_chat_kwargs,
        &source.upstream_chat_kwargs,
    );
    // Effort map merges per-level (child level overrides parent level); default
    // is set-if-some (child wins).
    for (level, fragment) in source.reasoning_effort_map {
        destination.reasoning_effort_map.insert(level, fragment);
    }
    if source.reasoning_effort_default.is_some() {
        destination.reasoning_effort_default = source.reasoning_effort_default;
    }
}

fn merge_persisted_model_profile(
    destination: &mut ResolvedModelProfile,
    source: &PersistedModelProfile,
) {
    if let Some(upstream_model) = trim_nonempty(source.upstream_model.as_deref()) {
        destination.upstream_model = Some(upstream_model);
    }
    if let Some(template_family) = trim_nonempty(source.template_family.as_deref()) {
        destination.template_family = Some(template_family);
    }
    if source.native_vision.is_some() {
        destination.native_vision = source.native_vision;
    }
    if !source.tool_call_repairs.is_empty() {
        destination.tool_call_repairs = source.tool_call_repairs.clone();
    }
    if source.roles.is_some() {
        destination.roles.clone_from(&source.roles);
    }
    if source.capabilities.is_some() {
        destination.capabilities.clone_from(&source.capabilities);
    }
    if source.reasoning_effort.is_some() {
        destination
            .reasoning_effort
            .clone_from(&source.reasoning_effort);
        destination.reasoning_effort_map.clear();
        destination.reasoning_effort_default = None;
    } else if !source.reasoning_effort_map.is_empty() || source.reasoning_effort_default.is_some() {
        destination.reasoning_effort = None;
    }
    if let Some(system_prompt_prefix) = trim_nonempty(source.system_prompt_prefix.as_deref()) {
        destination
            .system_prompt_prefixes
            .push(system_prompt_prefix);
    }
    merge_json_maps(
        &mut destination.upstream_chat_kwargs,
        &source.upstream_chat_kwargs,
    );
    for (level, fragment) in &source.reasoning_effort_map {
        destination
            .reasoning_effort_map
            .insert(level.clone(), fragment.clone());
    }
    if source.reasoning_effort_default.is_some() {
        destination
            .reasoning_effort_default
            .clone_from(&source.reasoning_effort_default);
    }
}

fn join_prompt_prefixes(prefixes: impl IntoIterator<Item = String>) -> Option<String> {
    let prefixes = prefixes
        .into_iter()
        .map(|prefix| prefix.trim().to_string())
        .filter(|prefix| !prefix.is_empty())
        .collect::<Vec<_>>();
    if prefixes.is_empty() {
        None
    } else {
        Some(prefixes.join("\n\n"))
    }
}

fn parse_upstream(
    (index, provider): (usize, &PersistedUpstream),
) -> Result<UpstreamConfig, String> {
    let upstream_base_url = Url::parse(provider.upstream_base_url.trim())
        .map_err(|err| format!("invalid upstreams[{index}].upstream_base_url: {err}"))?;
    let fallback_upstreams = provider
        .fallback_upstreams
        .iter()
        .enumerate()
        .map(|(fallback_index, fallback)| {
            parse_fallback_upstream(
                fallback,
                fallback_index,
                &format!("upstreams[{index}].fallback_upstreams"),
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(UpstreamConfig {
        name: provider
            .name
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("upstream-{}", index + 1)),
        upstream_base_url,
        upstream_api_key: trim_nonempty(provider.upstream_api_key.as_deref()),
        upstream_model: trim_nonempty(provider.upstream_model.as_deref()),
        upstream_chat_kwargs: provider.upstream_chat_kwargs.clone(),
        upstream_request_log_path: trim_nonempty(provider.upstream_request_log_path.as_deref())
            .map(PathBuf::from),
        fallback_upstreams,
    })
}

fn parse_provider_metrics_target(
    provider: &str,
    metrics_url: Option<&str>,
    metrics_source: Option<crate::provider_metrics::MetricsSource>,
    path: &str,
) -> Result<Option<crate::provider_metrics::ProviderMetricsTarget>, String> {
    let metrics_url = trim_nonempty(metrics_url);
    match (metrics_url, metrics_source) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(format!(
            "{path}.metrics_source is required with metrics_url"
        )),
        (None, Some(_)) => Err(format!(
            "{path}.metrics_url is required with metrics_source"
        )),
        (Some(raw_url), Some(source)) => {
            let url =
                Url::parse(&raw_url).map_err(|err| format!("invalid {path}.metrics_url: {err}"))?;
            crate::provider_metrics::ProviderMetricsTarget::from_operator_config(
                provider.to_string(),
                url,
                source,
            )
            .map(Some)
            .map_err(|err| format!("invalid {path}.metrics_url: {err}"))
        }
    }
}

fn parse_fallback_upstream(
    provider: &PersistedFallbackUpstream,
    index: usize,
    path: &str,
) -> Result<FallbackUpstreamConfig, String> {
    let upstream_base_url = Url::parse(provider.upstream_base_url.trim())
        .map_err(|err| format!("invalid {path}[{index}].upstream_base_url: {err}"))?;
    Ok(FallbackUpstreamConfig {
        name: provider
            .name
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("fallback-{}", index + 1)),
        upstream_base_url,
        upstream_api_key: trim_nonempty(provider.upstream_api_key.as_deref()),
        upstream_model: trim_nonempty(provider.upstream_model.as_deref()),
        exposed_model: trim_nonempty(provider.exposed_model.as_deref()),
        upstream_chat_kwargs: provider.upstream_chat_kwargs.clone(),
        upstream_request_log_path: trim_nonempty(provider.upstream_request_log_path.as_deref())
            .map(PathBuf::from),
    })
}

fn trim_nonempty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

/// Canonicalize a configured `template_family` override to the lowercase forms
/// the family detector understands. Unrecognized / blank values resolve to
/// `None` so a typo silently falls back to name-based auto-detection rather
/// than forcing the wrong contract.
fn normalize_template_family(value: Option<&str>) -> Option<String> {
    match trim_nonempty(value)?.to_ascii_lowercase().as_str() {
        "kimi" => Some("kimi".to_string()),
        "deepseek" => Some("deepseek".to_string()),
        _ => None,
    }
}

pub fn default_config_path() -> Result<PathBuf, String> {
    let config_dir = dirs::config_dir()
        .ok_or_else(|| "unable to determine configuration directory".to_string())?;
    Ok(config_dir.join("llmconduit").join("config.yaml"))
}

pub fn load_default_persisted_config() -> Result<PersistedConfig, String> {
    let path = default_config_path()?;
    load_persisted_config(&path)
}

/// Whether a config path is TOML (by `.toml` extension, case-insensitive). TOML
/// configs are READ-ONLY (G7): they load via the `toml` crate but are never
/// written (see `write_persisted_config`).
pub fn path_is_toml(path: &Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
}

pub fn load_persisted_config(path: &Path) -> Result<PersistedConfig, String> {
    if !path.exists() {
        return Ok(PersistedConfig::default());
    }
    let contents = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    // Detect format by extension (G7). `.toml` parses via the `toml` crate;
    // everything else (including `.yaml`/`.yml` and extensionless paths) keeps
    // the existing YAML path byte-identical.
    if path_is_toml(path) {
        toml::from_str(&contents)
            .map_err(|err| format!("failed to parse {}: {err}", path.display()))
    } else {
        serde_yaml::from_str(&contents)
            .map_err(|err| format!("failed to parse {}: {err}", path.display()))
    }
}

pub fn write_persisted_config(path: &Path, config: &PersistedConfig) -> Result<(), String> {
    // `configure` only writes YAML. `.toml` configs are read-only: writing YAML
    // bytes into a `.toml` file would produce a config that `load_persisted_config`
    // then tries (and fails) to parse as TOML. Reject cleanly BEFORE touching the
    // filesystem so no unreadable file is ever created (G7).
    if path_is_toml(path) {
        return Err(format!(
            "cannot write config to {}: `configure` writes YAML; `.toml` config files are read-only \u{2014} use a `.yaml`/`.yml` path",
            path.display()
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| format!("config path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)
        .map_err(|err| format!("failed to create {}: {err}", parent.display()))?;
    let yaml = serde_yaml::to_string(config)
        .map_err(|err| format!("failed to serialize config: {err}"))?;
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true).mode(0o600);
        let mut file = opts
            .open(path)
            .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
        file.write_all(yaml.as_bytes())
            .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        fs::write(path, yaml)
            .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
    }
    Ok(())
}

fn apply_env_overrides(config: &mut PersistedConfig) {
    if let Ok(value) = env::var("LLMCONDUIT_BIND_ADDR")
        && !value.trim().is_empty()
    {
        config.bind_addr = value;
    }
    if let Ok(value) = env::var("LLMCONDUIT_UPSTREAM_BASE_URL")
        && !value.trim().is_empty()
    {
        config.upstream_base_url = value;
    }
    if let Ok(value) = env::var("LLMCONDUIT_UPSTREAM_API_KEY")
        && !value.trim().is_empty()
    {
        config.upstream_api_key = Some(value);
    } else if config.upstream_api_key.is_none()
        && let Ok(value) = env::var("OPENAI_API_KEY")
        && !value.trim().is_empty()
    {
        config.upstream_api_key = Some(value);
    }
    if let Ok(value) = env::var("LLMCONDUIT_UPSTREAM_MODEL")
        && !value.trim().is_empty()
    {
        config.upstream_model = Some(value);
    }
    if let Ok(value) = env::var("LLMCONDUIT_TEMPLATE_FAMILY")
        && !value.trim().is_empty()
    {
        config.template_family = Some(value);
    }
    if let Ok(value) = env::var("LLMCONDUIT_SYSTEM_PROMPT_PREFIX")
        && !value.trim().is_empty()
    {
        config.system_prompt_prefix = Some(value);
    }
    if let Ok(value) = env::var("LLMCONDUIT_UPSTREAM_REQUEST_LOG_PATH")
        && !value.trim().is_empty()
    {
        config.upstream_request_log_path = Some(value);
    }
    if let Ok(value) = env::var("LLMCONDUIT_TURN_CAPTURE_DIR")
        && !value.trim().is_empty()
    {
        config.turn_capture_dir = Some(value);
    }
    if let Ok(value) = env::var("LLMCONDUIT_UPSTREAM_CHAT_KWARGS_JSON")
        && !value.trim().is_empty()
        && let Ok(parsed) = serde_json::from_str::<JsonMap<String, JsonValue>>(&value)
    {
        config.upstream_chat_kwargs = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_UPSTREAM_FAILURE_COOLDOWN_SECS")
        && let Ok(parsed) = value.parse()
    {
        config.upstream_failure_cooldown_secs = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_BRAVE_BASE_URL")
        && !value.trim().is_empty()
    {
        config.brave_base_url = value;
    }
    if let Ok(value) = env::var("BRAVE_SEARCH_API_KEY")
        && !value.trim().is_empty()
    {
        config.brave_api_key = Some(value);
    }
    if let Ok(value) = env::var("LLMCONDUIT_BRAVE_MAX_RESULTS")
        && let Ok(parsed) = value.parse()
    {
        config.brave_max_results = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_REQUEST_TIMEOUT_SECS")
        && let Ok(parsed) = value.parse()
    {
        config.request_timeout_secs = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_CONNECT_TIMEOUT_SECS")
        && let Ok(parsed) = value.parse()
    {
        config.connect_timeout_secs = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_MAX_WEB_SEARCH_ROUNDS")
        && let Ok(parsed) = value.parse()
    {
        config.max_web_search_rounds = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_FLATTEN_CONTENT")
        && let Ok(parsed) = value.parse()
    {
        config.flatten_content = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_MAX_REPLAY_ENTRIES")
        && let Ok(parsed) = value.parse()
    {
        config.max_replay_entries = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_DEBUG_LOG_MAX_AGE_HOURS")
        && let Ok(parsed) = value.trim().parse()
    {
        config.debug_log_max_age_hours = Some(parsed);
    }
    if let Ok(value) = env::var("LLMCONDUIT_MIN_COMPLETION_TOKENS")
        && let Ok(parsed) = value.trim().parse::<i64>()
        && parsed >= 1
    {
        config.min_completion_tokens = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_MAX_SSE_FRAME_BYTES")
        && let Ok(parsed) = value.trim().parse::<usize>()
        && parsed >= 1
    {
        config.max_sse_frame_bytes = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_MAX_REQUEST_BODY_BYTES")
        && let Ok(parsed) = value.trim().parse::<usize>()
        && parsed >= 1
    {
        config.max_request_body_bytes = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_IMAGE_AGENT_ENABLED")
        && let Ok(parsed) = value.trim().parse::<bool>()
    {
        config.image_agent_enabled = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_VISION_URL")
        && !value.trim().is_empty()
    {
        config.vision_url = Some(value);
    }
    if let Ok(value) = env::var("LLMCONDUIT_VISION_MODEL")
        && !value.trim().is_empty()
    {
        config.vision_model = Some(value);
    }
    if let Ok(value) = env::var("LLMCONDUIT_IMAGE_CACHE_MAX_SIZE")
        && let Ok(parsed) = value.trim().parse::<usize>()
        && parsed >= 1
    {
        config.image_cache_max_size = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_IMAGE_CACHE_TTL_SECS")
        && let Ok(parsed) = value.trim().parse::<u64>()
    {
        config.image_cache_ttl_secs = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_UNSUPPORTED_IMAGE_POLICY") {
        match value.trim().to_ascii_lowercase().as_str() {
            "placeholder" => config.unsupported_image_policy = UnsupportedImagePolicy::Placeholder,
            "reject" => config.unsupported_image_policy = UnsupportedImagePolicy::Reject,
            // Unrecognized value: ignore rather than silently defaulting to a
            // DIFFERENT policy than a typo'd env var probably intended.
            _ => {}
        }
    }
    // T13/D13: the per-model price table can be supplied wholesale as a JSON map
    // via the environment (mirrors `LLMCONDUIT_UPSTREAM_CHAT_KWARGS_JSON`). The
    // env value REPLACES the YAML `price_table:` map when it parses; a malformed
    // value is ignored so a typo cannot wipe a configured table silently.
    if let Ok(value) = env::var("LLMCONDUIT_PRICE_TABLE_JSON")
        && !value.trim().is_empty()
        && let Ok(mut parsed) = serde_json::from_str::<HashMap<String, ModelPrice>>(&value)
    {
        // D13 R1 MED: drop any non-finite (NaN/Inf) env-supplied price so the table
        // serializes finite numbers only (the frozen `ModelPrice` contract).
        retain_finite_prices(&mut parsed);
        config.price_table = parsed;
    }
}

pub fn merge_json_maps(
    destination: &mut JsonMap<String, JsonValue>,
    source: &JsonMap<String, JsonValue>,
) {
    for (key, source_value) in source {
        match (destination.get_mut(key), source_value) {
            (Some(JsonValue::Object(destination_object)), JsonValue::Object(source_object)) => {
                merge_json_maps(destination_object, source_object);
            }
            _ => {
                destination.insert(key.clone(), source_value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use super::JsonMap;
    use super::JsonValue;
    use super::MeshWorkerCapacitySource;
    use super::ModelPrice;
    use super::OrderedModelRoutes;
    use super::PersistedConfig;
    use super::PersistedFallbackUpstream;
    use super::PersistedMeshConfig;
    use super::PersistedMeshWorkerConfig;
    use super::PersistedModelProfile;
    use super::PersistedUpstream;
    use super::RolesConfig;
    use super::UnsupportedImagePolicy;
    use super::apply_env_overrides;
    use super::default_config_path;
    use super::load_persisted_config;
    use super::merge_json_maps;
    use super::retain_finite_prices;
    use super::write_persisted_config;
    use crate::models::chat::ChatCompletionRequest;
    use crate::upstream::BackendChatRequest;
    use crate::upstream::BackendFinalizationPolicies;
    use crate::upstream::finalize_request_for_backend;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::collections::BTreeMap as StdBTreeMap;
    use std::collections::HashMap;

    #[test]
    fn merge_adjacent_rejects_non_content_role() {
        // `merge_adjacent` discards non-content fields, so merging a role that
        // carries tool_calls/tool_call_id (assistant/tool) would corrupt history.
        let roles: RolesConfig = serde_json::from_value(json!({
            "merge_adjacent": ["tool"],
            "tool": {},
        }))
        .expect("parse");
        let err = roles
            .validate()
            .expect_err("tool is not a content-only role");
        assert!(
            err.contains("merge_adjacent"),
            "expected a merge_adjacent guard error, got: {err}"
        );
    }

    #[test]
    fn merge_adjacent_accepts_content_only_roles() {
        let roles: RolesConfig = serde_json::from_value(json!({
            "merge_adjacent": ["system", "developer", "user"],
            "system": {},
            "developer": {},
            "user": {},
        }))
        .expect("parse");
        roles.validate().expect("content-only roles may be merged");
    }

    /// Minimal wire request for leaf-finalization tests. `backend_model` is the
    /// FINAL provider model the leaf sees (after any routing/failover/alias
    /// remap); the leaf resolves per-model policies against THIS id.
    fn leaf_request(backend_model: &str) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: backend_model.to_string(),
            messages: Vec::new(),
            stream: true,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: Some(false),
            reasoning_effort: None,
            response_format: None,
            stream_options: None,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: StdBTreeMap::new(),
        }
    }

    /// Resolve the FINAL `chat_template_kwargs`-bearing `extra_body` the upstream
    /// LEAF produces for `backend_model`, exercising the REAL production path: the
    /// policies are built via [`BackendFinalizationPolicies::from_config`] (the
    /// single way production builds leaf policies, T1) and applied through the
    /// public [`finalize_request_for_backend`] seam. The request carries an empty
    /// `extra_body` and no `reasoning_effort`, so the resulting `extra_body` is
    /// exactly the leaf's per-model `upstream_chat_kwargs` resolution: the
    /// at-most-one per-model policy (`policy_for_model`, exact-then-canonical-
    /// unique) layered over `global_upstream_chat_kwargs` (per-model wins on
    /// conflict). Pick a `backend_model` that does NOT name-sniff to a family
    /// (kimi/deepseek) so no family `chat_template_kwargs` are injected and the
    /// returned map isolates kwargs precedence.
    fn leaf_chat_kwargs(config: &Config, backend_model: &str) -> JsonMap<String, JsonValue> {
        let policies = BackendFinalizationPolicies::from_config(config);
        let mut backend = BackendChatRequest::new(leaf_request(backend_model), None, None, None);
        finalize_request_for_backend(&mut backend, &policies);
        backend.request.extra_body.into_iter().collect()
    }

    /// The family `chat_template_kwargs` the LEAF injects for `backend_model`,
    /// exercising the REAL path: policies via `from_config`, applied through the
    /// public `finalize_request_for_backend` seam. The resolved `template_family`
    /// override (per-model policy else global) selects the family; the injected
    /// `chat_template_kwargs` object is returned.
    fn leaf_family_kwargs(config: &Config, backend_model: &str) -> JsonValue {
        leaf_chat_kwargs(config, backend_model)
            .get("chat_template_kwargs")
            .cloned()
            .unwrap_or(JsonValue::Null)
    }

    /// Whether the LEAF injected any `chat_template_kwargs` for `backend_model`
    /// (i.e. a family was resolved). `false` means no per-model/global override
    /// matched and the name did not sniff to a family.
    fn leaf_request_has_family_kwargs(config: &Config, backend_model: &str) -> bool {
        leaf_chat_kwargs(config, backend_model).contains_key("chat_template_kwargs")
    }

    #[test]
    fn resolves_reasoning_effort_policy_from_profile_chain() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
model_profile_templates:
  glm-effort:
    reasoning_effort_default: max
    reasoning_effort_map:
      high: { chat_template_kwargs: { reasoning_effort: high } }
      max: { chat_template_kwargs: { reasoning_effort: max } }
      none: { chat_template_kwargs: { enable_thinking: false } }
model_profiles:
  GLM-5.2-NVFP4-MTP:
    extends: [glm-effort]
    reasoning_effort_map:
      xhigh: { chat_template_kwargs: { reasoning_effort: max } }
"#,
        )
        .expect("yaml");
        let config = Config::from_persisted(&persisted).expect("config");
        let policies = config.reasoning_effort_policies();
        let policy = policies
            .get("GLM-5.2-NVFP4-MTP")
            .expect("GLM policy present");
        // Default + template levels resolve; the profile ADDS xhigh on top.
        assert_eq!(policy.default.as_deref(), Some("max"));
        assert_eq!(
            policy.map["high"]["chat_template_kwargs"]["reasoning_effort"],
            json!("high")
        );
        assert_eq!(
            policy.map["xhigh"]["chat_template_kwargs"]["reasoning_effort"],
            json!("max")
        );
        assert_eq!(
            policy.map["none"]["chat_template_kwargs"]["enable_thinking"],
            json!(false)
        );
        // A profile with no effort map is not included.
        assert!(!policies.contains_key("other"));
    }

    #[test]
    fn upstream_reasoning_syntax_uses_leaf_resolution_and_dynamic_thinking_kwarg() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
model_profiles:
  served-model:
    reasoning_effort:
      default: medium
      map:
        low: high
        "*": max
      thinking_param_name: thinking_mode
      thinking_param_value_on: enabled
      thinking_param_value_off: disabled
"#,
        )
        .expect("yaml");
        let config = Config::from_persisted(&persisted).expect("config");
        let policies = BackendFinalizationPolicies::from_config(&config);

        let mut request = leaf_request("served-model");
        request.reasoning_effort = Some("low".to_string());
        let mut backend =
            BackendChatRequest::new(request, None, None, None).with_thinking_override(Some(true));
        finalize_request_for_backend(&mut backend, &policies);
        let wire = serde_json::to_value(&backend.request).expect("wire json");
        assert_eq!(wire["reasoning_effort"], json!("high"));
        assert_eq!(
            wire["chat_template_kwargs"]["thinking_mode"],
            json!("enabled")
        );

        let mut request = leaf_request("served-model");
        request.reasoning_effort = Some("unlisted".to_string());
        let mut backend =
            BackendChatRequest::new(request, None, None, None).with_thinking_override(Some(false));
        finalize_request_for_backend(&mut backend, &policies);
        let wire = serde_json::to_value(&backend.request).expect("wire json");
        assert_eq!(wire["reasoning_effort"], json!("max"));
        assert_eq!(
            wire["chat_template_kwargs"]["thinking_mode"],
            json!("disabled")
        );
    }

    #[test]
    fn typed_and_fragment_reasoning_syntaxes_are_mutually_exclusive() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
model_profiles:
  broken:
    reasoning_effort: {default: high}
    reasoning_effort_map:
      high: {reasoning_effort: high}
"#,
        )
        .expect("yaml");
        let error = Config::from_persisted(&persisted).expect_err("ambiguous config must fail");
        assert!(
            error.contains("cannot be combined"),
            "unexpected error: {error}"
        );
    }
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn default_config_path_uses_llmconduit_config_dir() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let config_home = std::env::temp_dir().join(format!(
            "llmconduit-xdg-config-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let previous_xdg_config_home = std::env::var_os("XDG_CONFIG_HOME");
        unsafe {
            std::env::set_var("XDG_CONFIG_HOME", &config_home);
        }

        let path = default_config_path().expect("default config path");
        assert_eq!(path, config_home.join("llmconduit").join("config.yaml"));

        unsafe {
            match previous_xdg_config_home {
                Some(value) => std::env::set_var("XDG_CONFIG_HOME", value),
                None => std::env::remove_var("XDG_CONFIG_HOME"),
            }
        }
    }

    #[test]
    fn from_persisted_invalid_base_url() {
        let config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            upstream_base_url: "not a url".to_string(),
            ..PersistedConfig::default()
        };
        assert!(Config::from_persisted(&config).is_err());
    }

    #[test]
    fn whitespace_api_key_trimmed() {
        let config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            upstream_api_key: Some("  secret  ".to_string()),
            ..PersistedConfig::default()
        };
        let result = Config::from_persisted(&config).unwrap();
        assert_eq!(result.upstream_api_key, Some("secret".to_string()));

        let config2 = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            upstream_api_key: Some("   ".to_string()),
            ..PersistedConfig::default()
        };
        let result2 = Config::from_persisted(&config2).unwrap();
        assert_eq!(result2.upstream_api_key, None);
    }

    /// AC-1 (F1a): `turn_capture_dir` trims like `upstream_request_log_path`
    /// (mirrors `whitespace_api_key_trimmed`'s trim-to-`None` shape) --
    /// surrounding whitespace is trimmed, and a blank/whitespace-only value
    /// resolves to `None` (capture disabled), never `Some("")`.
    #[test]
    fn turn_capture_dir_trims_blank_to_none() {
        let config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            turn_capture_dir: Some("  /tmp/llmconduit-turns  ".to_string()),
            ..PersistedConfig::default()
        };
        let result = Config::from_persisted(&config).unwrap();
        assert_eq!(
            result.turn_capture_dir,
            Some(PathBuf::from("/tmp/llmconduit-turns"))
        );

        let blank = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            turn_capture_dir: Some("   ".to_string()),
            ..PersistedConfig::default()
        };
        let blank_result = Config::from_persisted(&blank).unwrap();
        assert_eq!(blank_result.turn_capture_dir, None);

        let unset = PersistedConfig::default();
        let unset_result = Config::from_persisted(&unset).unwrap();
        assert_eq!(unset_result.turn_capture_dir, None);
    }

    /// AC-1 (F1a): `turn_capture_dir` round-trips through both file formats
    /// `load_persisted_config` supports (mirrors `toml_file_extension_is_detected_on_load`
    /// in `tests/port_config.rs`, but exercised here alongside the other
    /// config-parsing unit tests). Byte-identical resolution from a `.yaml`
    /// and an equivalent `.toml` file proves the field is "just another
    /// Option field" on `PersistedConfig` -- no format-specific plumbing
    /// needed.
    #[test]
    fn turn_capture_dir_parses_from_yaml_and_toml_files() {
        // `Config::from_env_and_file` layers env overrides on top of the file
        // (`apply_env_overrides`), so hold `ENV_LOCK` and start from a clean
        // slate -- otherwise this could race `apply_env_overrides_turn_capture_dir`
        // (below), which mutates the same `LLMCONDUIT_TURN_CAPTURE_DIR` var.
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::remove_var("LLMCONDUIT_TURN_CAPTURE_DIR");
        }
        let yaml_path = std::env::temp_dir().join(format!(
            "llmconduit-turn-capture-{}.yaml",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::write(
            &yaml_path,
            "upstream_base_url: http://127.0.0.1:8000/v1\nturn_capture_dir: /tmp/llmconduit-turns\n",
        )
        .expect("write yaml");
        let yaml_config = Config::from_env_and_file(Some(&yaml_path)).expect("load yaml config");
        let _ = std::fs::remove_file(&yaml_path);
        assert_eq!(
            yaml_config.turn_capture_dir,
            Some(PathBuf::from("/tmp/llmconduit-turns"))
        );

        let toml_path = std::env::temp_dir().join(format!(
            "llmconduit-turn-capture-{}.toml",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::write(
            &toml_path,
            "upstream_base_url = \"http://127.0.0.1:8000/v1\"\nturn_capture_dir = \"/tmp/llmconduit-turns\"\n",
        )
        .expect("write toml");
        let toml_config = Config::from_env_and_file(Some(&toml_path)).expect("load toml config");
        let _ = std::fs::remove_file(&toml_path);
        assert_eq!(
            toml_config.turn_capture_dir,
            Some(PathBuf::from("/tmp/llmconduit-turns"))
        );
    }

    #[test]
    fn from_persisted_parses_fallback_upstreams() {
        let config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            fallback_upstreams: vec![
                PersistedFallbackUpstream {
                    metrics_url: None,
                    metrics_source: None,

                    name: Some(" backup ".to_string()),
                    upstream_base_url: "  http://127.0.0.1:8001/v1  ".to_string(),
                    upstream_api_key: Some(" backup-secret ".to_string()),
                    upstream_model: Some(" fallback-model ".to_string()),
                    exposed_model: Some(" fallback-alias ".to_string()),
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "provider".to_string(),
                        json!({
                            "order": ["z-ai"],
                            "allow_fallbacks": true
                        }),
                    )]),
                    upstream_request_log_path: Some(" /tmp/llmconduit-fallback.jsonl ".to_string()),
                },
                PersistedFallbackUpstream {
                    metrics_url: None,
                    metrics_source: None,

                    name: Some("   ".to_string()),
                    upstream_base_url: "http://127.0.0.1:8002/v1".to_string(),
                    upstream_api_key: Some("   ".to_string()),
                    upstream_model: None,
                    exposed_model: None,
                    upstream_chat_kwargs: JsonMap::new(),
                    upstream_request_log_path: None,
                },
            ],
            upstream_failure_cooldown_secs: 12,
            ..PersistedConfig::default()
        };

        let result = Config::from_persisted(&config).expect("config");

        assert_eq!(result.upstream_failure_cooldown_secs, 12);
        assert_eq!(result.fallback_upstreams.len(), 2);
        assert_eq!(result.fallback_upstreams[0].name, "backup");
        assert_eq!(
            result.fallback_upstreams[0].upstream_base_url.as_str(),
            "http://127.0.0.1:8001/v1"
        );
        assert_eq!(
            result.fallback_upstreams[0].upstream_api_key.as_deref(),
            Some("backup-secret")
        );
        assert_eq!(
            result.fallback_upstreams[0].upstream_model.as_deref(),
            Some("fallback-model")
        );
        assert_eq!(
            result.fallback_upstreams[0].exposed_model.as_deref(),
            Some("fallback-alias")
        );
        assert_eq!(
            result.fallback_upstreams[0]
                .upstream_chat_kwargs
                .get("provider"),
            Some(&json!({
                "order": ["z-ai"],
                "allow_fallbacks": true
            }))
        );
        assert_eq!(
            result.fallback_upstreams[0]
                .upstream_request_log_path
                .as_deref(),
            Some(std::path::Path::new("/tmp/llmconduit-fallback.jsonl"))
        );
        assert_eq!(result.fallback_upstreams[1].name, "fallback-2");
        assert_eq!(result.fallback_upstreams[1].upstream_api_key, None);
    }

    #[test]
    fn from_persisted_parses_explicit_upstreams_with_nested_fallbacks() {
        let config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            upstreams: vec![PersistedUpstream {
                metrics_url: None,
                metrics_source: None,

                name: Some(" local ".to_string()),
                upstream_base_url: " http://127.0.0.1:8000/v1 ".to_string(),
                upstream_api_key: Some(" local-secret ".to_string()),
                upstream_model: Some(" local-model ".to_string()),
                upstream_chat_kwargs: JsonMap::from_iter([(
                    "chat_template_kwargs".to_string(),
                    json!({"thinking": true}),
                )]),
                upstream_request_log_path: Some(" /tmp/llmconduit-local.jsonl ".to_string()),
                fallback_upstreams: vec![PersistedFallbackUpstream {
                    metrics_url: None,
                    metrics_source: None,

                    name: Some(" backup ".to_string()),
                    upstream_base_url: " https://openrouter.ai/api/v1 ".to_string(),
                    upstream_api_key: Some(" backup-secret ".to_string()),
                    upstream_model: Some(" backup-model ".to_string()),
                    exposed_model: Some(" backup-alias ".to_string()),
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "provider".to_string(),
                        json!({"order": ["openai"]}),
                    )]),
                    upstream_request_log_path: Some(" /tmp/llmconduit-backup.jsonl ".to_string()),
                }],
            }],
            ..PersistedConfig::default()
        };

        let result = Config::from_persisted(&config).expect("config");

        assert_eq!(result.upstreams.len(), 1);
        let upstream = &result.upstreams[0];
        assert_eq!(upstream.name, "local");
        assert_eq!(
            upstream.upstream_base_url.as_str(),
            "http://127.0.0.1:8000/v1"
        );
        assert_eq!(upstream.upstream_api_key.as_deref(), Some("local-secret"));
        assert_eq!(upstream.upstream_model.as_deref(), Some("local-model"));
        assert_eq!(
            upstream.upstream_chat_kwargs.get("chat_template_kwargs"),
            Some(&json!({"thinking": true}))
        );
        assert_eq!(
            upstream.upstream_request_log_path.as_deref(),
            Some(std::path::Path::new("/tmp/llmconduit-local.jsonl"))
        );
        assert_eq!(upstream.fallback_upstreams.len(), 1);
        assert_eq!(upstream.fallback_upstreams[0].name, "backup");
        assert_eq!(
            upstream.fallback_upstreams[0].upstream_model.as_deref(),
            Some("backup-model")
        );
        assert_eq!(
            upstream.fallback_upstreams[0].exposed_model.as_deref(),
            Some("backup-alias")
        );
    }

    #[test]
    fn from_persisted_rejects_invalid_fallback_upstream_url() {
        let config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            fallback_upstreams: vec![PersistedFallbackUpstream {
                metrics_url: None,
                metrics_source: None,

                upstream_base_url: "not a url".to_string(),
                ..PersistedFallbackUpstream::default()
            }],
            ..PersistedConfig::default()
        };

        let error = Config::from_persisted(&config).expect_err("invalid fallback URL");

        assert!(error.contains("invalid fallback_upstreams[0].upstream_base_url"));
    }

    #[test]
    fn load_persisted_config_missing_file_returns_default() {
        let result = load_persisted_config(std::path::Path::new(
            "/tmp/nonexistent-llmconduit-config-test.yaml",
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PersistedConfig::default());
    }

    #[test]
    fn apply_env_overrides_upstream_api_key() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::remove_var("OPENAI_API_KEY");
            std::env::remove_var("LLMCONDUIT_UPSTREAM_API_KEY");
            std::env::set_var("LLMCONDUIT_UPSTREAM_API_KEY", "test-key-12345");
        }
        let mut config = PersistedConfig::default();
        apply_env_overrides(&mut config);
        assert_eq!(config.upstream_api_key, Some("test-key-12345".to_string()));
        unsafe {
            std::env::remove_var("LLMCONDUIT_UPSTREAM_API_KEY");
            std::env::remove_var("OPENAI_API_KEY");
        };
    }

    #[test]
    fn apply_env_overrides_openai_fallback() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::remove_var("LLMCONDUIT_UPSTREAM_API_KEY");
            std::env::remove_var("OPENAI_API_KEY");
            std::env::set_var("OPENAI_API_KEY", "fallback-key-67890");
        }
        let mut config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            upstream_api_key: None,
            ..Default::default()
        };
        apply_env_overrides(&mut config);
        assert_eq!(
            config.upstream_api_key,
            Some("fallback-key-67890".to_string())
        );
        unsafe {
            std::env::remove_var("LLMCONDUIT_UPSTREAM_API_KEY");
            std::env::remove_var("OPENAI_API_KEY");
        };
    }

    #[test]
    fn apply_env_overrides_system_prompt_prefix() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::set_var("LLMCONDUIT_SYSTEM_PROMPT_PREFIX", "Global prefix.");
        }
        let mut config = PersistedConfig::default();
        apply_env_overrides(&mut config);
        assert_eq!(
            config.system_prompt_prefix,
            Some("Global prefix.".to_string())
        );
        unsafe {
            std::env::remove_var("LLMCONDUIT_SYSTEM_PROMPT_PREFIX");
        };
    }

    /// AC-1 (F1a): `LLMCONDUIT_TURN_CAPTURE_DIR` overrides the persisted
    /// `turn_capture_dir` (mirrors `apply_env_overrides_system_prompt_prefix`).
    #[test]
    fn apply_env_overrides_turn_capture_dir() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::set_var("LLMCONDUIT_TURN_CAPTURE_DIR", "/tmp/llmconduit-turns");
        }
        let mut config = PersistedConfig::default();
        apply_env_overrides(&mut config);
        assert_eq!(
            config.turn_capture_dir,
            Some("/tmp/llmconduit-turns".to_string())
        );
        unsafe {
            std::env::remove_var("LLMCONDUIT_TURN_CAPTURE_DIR");
        };
    }

    /// AC-1 (F1a): a blank `LLMCONDUIT_TURN_CAPTURE_DIR` is ignored (same
    /// blank-guard as every other string env override), leaving the
    /// persisted value untouched.
    #[test]
    fn apply_env_overrides_turn_capture_dir_ignores_blank() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::set_var("LLMCONDUIT_TURN_CAPTURE_DIR", "   ");
        }
        let mut config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            turn_capture_dir: Some("/tmp/llmconduit-existing".to_string()),
            ..PersistedConfig::default()
        };
        apply_env_overrides(&mut config);
        assert_eq!(
            config.turn_capture_dir,
            Some("/tmp/llmconduit-existing".to_string()),
            "a blank env override must not clobber the persisted value"
        );
        unsafe {
            std::env::remove_var("LLMCONDUIT_TURN_CAPTURE_DIR");
        };
    }

    #[test]
    fn apply_env_overrides_template_family() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::set_var("LLMCONDUIT_TEMPLATE_FAMILY", "deepseek");
        }
        let mut config = PersistedConfig::default();
        apply_env_overrides(&mut config);
        // Env sets the raw persisted value; normalization happens in
        // `from_persisted`.
        assert_eq!(config.template_family, Some("deepseek".to_string()));
        let resolved = Config::from_persisted(&config).expect("config");
        assert_eq!(resolved.template_family, Some("deepseek".to_string()));
        unsafe {
            std::env::remove_var("LLMCONDUIT_TEMPLATE_FAMILY");
        };
    }

    #[test]
    fn unsupported_image_policy_defaults_to_placeholder_when_omitted() {
        // `#[serde(default = "default_unsupported_image_policy")]` must fill in
        // `Placeholder` for a config file written before E2b existed.
        let parsed: PersistedConfig =
            serde_yaml::from_str("upstream_base_url: http://127.0.0.1:8000/v1\n").expect("yaml");
        assert_eq!(
            parsed.unsupported_image_policy,
            UnsupportedImagePolicy::Placeholder
        );
        let resolved = Config::from_persisted(&parsed).expect("config");
        assert_eq!(
            resolved.unsupported_image_policy,
            UnsupportedImagePolicy::Placeholder
        );
    }

    #[test]
    fn unsupported_image_policy_parses_and_serializes_snake_case() {
        let parsed: PersistedConfig =
            serde_yaml::from_str("unsupported_image_policy: reject\n").expect("yaml");
        assert_eq!(
            parsed.unsupported_image_policy,
            UnsupportedImagePolicy::Reject
        );
        let yaml = serde_yaml::to_string(&parsed).expect("serialize");
        assert!(
            yaml.contains("unsupported_image_policy: reject"),
            "expected snake_case `reject`, got: {yaml}"
        );
    }

    #[test]
    fn apply_env_overrides_unsupported_image_policy_reject() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::set_var("LLMCONDUIT_UNSUPPORTED_IMAGE_POLICY", "Reject");
        }
        let mut config = PersistedConfig::default();
        apply_env_overrides(&mut config);
        assert_eq!(
            config.unsupported_image_policy,
            UnsupportedImagePolicy::Reject
        );
        unsafe {
            std::env::remove_var("LLMCONDUIT_UNSUPPORTED_IMAGE_POLICY");
        };
    }

    #[test]
    fn apply_env_overrides_unsupported_image_policy_ignores_unrecognized_value() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::set_var("LLMCONDUIT_UNSUPPORTED_IMAGE_POLICY", "bogus");
        }
        let mut config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            unsupported_image_policy: UnsupportedImagePolicy::Reject,
            ..PersistedConfig::default()
        };
        apply_env_overrides(&mut config);
        // A typo'd value must not silently coerce to the OTHER policy — left
        // unchanged rather than guessing which one was intended.
        assert_eq!(
            config.unsupported_image_policy,
            UnsupportedImagePolicy::Reject
        );
        unsafe {
            std::env::remove_var("LLMCONDUIT_UNSUPPORTED_IMAGE_POLICY");
        };
    }

    #[test]
    fn apply_env_overrides_upstream_failure_cooldown() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::set_var("LLMCONDUIT_UPSTREAM_FAILURE_COOLDOWN_SECS", "7");
        }
        let mut config = PersistedConfig::default();
        apply_env_overrides(&mut config);
        assert_eq!(config.upstream_failure_cooldown_secs, 7);
        unsafe {
            std::env::remove_var("LLMCONDUIT_UPSTREAM_FAILURE_COOLDOWN_SECS");
        };
    }

    #[test]
    fn persisted_config_roundtrips() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-config-{}.yaml",
            uuid::Uuid::new_v4().simple()
        ));
        let config = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: Some("upstream-secret".to_string()),
            upstream_model: Some("grok-4".to_string()),
            system_prompt_prefix: Some("Global prefix.".to_string()),
            upstream_request_log_path: Some("/tmp/llmconduit-upstream.jsonl".to_string()),
            turn_capture_dir: Some("/tmp/llmconduit-turns".to_string()),
            upstream_chat_kwargs: JsonMap::from_iter([(
                "clear_thinking".to_string(),
                JsonValue::Bool(false),
            )]),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::from_iter([(
                "streaming-reasoning".to_string(),
                PersistedModelProfile {
                    extends: Vec::new(),
                    upstream_model: None,
                    system_prompt_prefix: None,
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "stream_reasoning".to_string(),
                        JsonValue::Bool(true),
                    )]),
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            model_profiles: BTreeMap::from_iter([(
                "Kimi-K2.6".to_string(),
                PersistedModelProfile {
                    extends: vec!["streaming-reasoning".to_string()],
                    upstream_model: None,
                    system_prompt_prefix: Some("Use Kimi-compatible behavior.".to_string()),
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "chat_template_kwargs".to_string(),
                        json!({
                            "thinking": true,
                            "preserve_thinking": true
                        }),
                    )]),
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: Some("secret".to_string()),
            brave_max_results: 7,
            request_timeout_secs: 45,
            connect_timeout_secs: 10,
            max_web_search_rounds: 10,
            flatten_content: false,
            max_replay_entries: 1000,
            debug_log_max_age_hours: Some(48),
            min_completion_tokens: 4096,
            max_sse_frame_bytes: 8 * 1024 * 1024,
            max_request_body_bytes: 10 * 1024 * 1024,
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            model_routes: OrderedModelRoutes::default(),
            template_family: None,
            price_table: std::collections::HashMap::new(),
            auth: Default::default(),
            mesh: PersistedMeshConfig::default(),
        };
        write_persisted_config(&path, &config).expect("write config");
        let loaded = load_persisted_config(&path).expect("load config");
        assert_eq!(loaded, config);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn resolves_profile_specific_upstream_chat_kwargs() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            // Non-empty GLOBAL base: a global-only sibling key plus a nested key
            // (`thinking`) that CONFLICTS with the per-model policy below, so the
            // leaf precedence (per-model wins on conflict, global-only survives)
            // is exercised, not just a per-model-over-empty-base lookup.
            upstream_chat_kwargs: JsonMap::from_iter([(
                "chat_template_kwargs".to_string(),
                json!({
                    "thinking": false,
                    "global_only": true
                }),
            )]),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([(
                "Reasoner-A26".to_string(),
                PersistedModelProfile {
                    extends: Vec::new(),
                    upstream_model: None,
                    system_prompt_prefix: None,
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "chat_template_kwargs".to_string(),
                        json!({
                            "thinking": true,
                            "preserve_thinking": true
                        }),
                    )]),
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            debug_log_max_age_hours: None,
            min_completion_tokens: 4096,
            max_sse_frame_bytes: 8 * 1024 * 1024,
            max_request_body_bytes: 10 * 1024 * 1024,
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            model_routes: OrderedModelRoutes::default(),
            template_family: None,
            price_table: std::collections::HashMap::new(),
            auth: Default::default(),
            mesh: PersistedMeshConfig::default(),
        })
        .expect("config");

        // The LEAF layers the at-most-one per-model policy OVER the global base
        // for the FINAL backend model (the profile name): the global-only key
        // (`global_only`) survives, while the per-model value wins on the
        // conflicting `thinking` key (per-model `true` beats global `false`).
        assert_eq!(
            config.resolve_upstream_model("Reasoner-A26"),
            "Reasoner-A26".to_string()
        );
        assert_eq!(
            leaf_chat_kwargs(&config, "Reasoner-A26"),
            JsonMap::from_iter([(
                "chat_template_kwargs".to_string(),
                json!({
                    "thinking": true,
                    "preserve_thinking": true,
                    "global_only": true
                }),
            )])
        );
        // An unprofiled (non-family-sniffing) backend model gets ONLY the global
        // base — no per-model policy is layered.
        assert_eq!(
            leaf_chat_kwargs(&config, "unprofiled-plain"),
            JsonMap::from_iter([(
                "chat_template_kwargs".to_string(),
                json!({
                    "thinking": false,
                    "global_only": true
                }),
            )])
        );
    }

    /// Build a `PersistedModelProfile` with only a `template_family` set.
    fn profile_with_family(family: &str) -> PersistedModelProfile {
        PersistedModelProfile {
            template_family: Some(family.to_string()),
            native_vision: None,
            ..PersistedModelProfile::default()
        }
    }

    #[test]
    fn resolves_template_family_override_from_profile_and_global() {
        // Profile key carries an explicit `template_family` but a name that does
        // NOT sniff to any family, so the per-model override (not name sniffing)
        // is what drives injection at the leaf.
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            template_family: Some("deepseek".to_string()),
            model_profiles: BTreeMap::from_iter([(
                "Router-X".to_string(),
                profile_with_family("KIMI"),
            )]),
            ..PersistedConfig::default()
        })
        .expect("config");

        // The LEAF builds per-model + global `template_family` policies via
        // `from_config`. Per-model wins over global; the value is normalized.
        assert_eq!(
            config.template_family_policies(),
            BTreeMap::from_iter([("Router-X".to_string(), "kimi".to_string())])
        );
        assert_eq!(
            config.global_template_family(),
            Some("deepseek".to_string())
        );

        // Proven on the wire through the public leaf seam: the per-model `kimi`
        // override forces Kimi injection (`thinking: true`) for `Router-X`
        // despite its non-Kimi name; an unmatched model falls back to the global
        // `deepseek` override (`enable_thinking: true`).
        assert_eq!(
            leaf_family_kwargs(&config, "Router-X"),
            json!({"thinking": true, "preserve_thinking": true})
        );
        assert_eq!(
            leaf_family_kwargs(&config, "plain-model"),
            json!({"enable_thinking": true})
        );
    }

    #[test]
    fn template_family_normalizes_and_rejects_unknown_values() {
        // Unknown/blank override values normalize to None (fall back to name
        // sniffing) rather than forcing a wrong contract.
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            template_family: Some("  Bogus ".to_string()),
            ..PersistedConfig::default()
        })
        .expect("config");
        assert_eq!(config.template_family, None);
        // The leaf sees neither a per-model policy nor a global override, so no
        // family is forced for an unrecognized backend model.
        assert!(config.template_family_policies().is_empty());
        assert_eq!(config.global_template_family(), None);
        assert!(!leaf_request_has_family_kwargs(&config, "m"));

        // A recognized value is canonicalized to lowercase.
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            template_family: Some("KIMI".to_string()),
            ..PersistedConfig::default()
        })
        .expect("config");
        assert_eq!(config.template_family, Some("kimi".to_string()));
        assert_eq!(config.global_template_family(), Some("kimi".to_string()));
    }

    #[test]
    fn profile_template_family_shorthand_does_not_leak_into_chat_kwargs() {
        // `template_family` provided as a YAML shorthand key must land in the
        // typed field, not the flattened chat-template kwargs bucket.
        let profile: PersistedModelProfile =
            serde_yaml::from_str("template_family: kimi\nthinking: true\n").expect("profile");
        assert_eq!(profile.template_family, Some("kimi".to_string()));
        assert!(!profile.upstream_chat_kwargs.contains_key("template_family"));
        assert_eq!(profile.upstream_chat_kwargs["thinking"], json!(true));
    }

    #[test]
    fn resolves_model_profiles_case_insensitively() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([(
                "MiMo-V2.5".to_string(),
                PersistedModelProfile {
                    extends: Vec::new(),
                    upstream_model: Some("mimo-v2.5".to_string()),
                    system_prompt_prefix: Some("Prefer concise answers.".to_string()),
                    upstream_chat_kwargs: JsonMap::from_iter([
                        ("separate_reasoning".to_string(), JsonValue::Bool(true)),
                        (
                            "chat_template_kwargs".to_string(),
                            json!({
                                "enable_thinking": true,
                                "keep_all_reasoning": true
                            }),
                        ),
                    ]),
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            debug_log_max_age_hours: None,
            min_completion_tokens: 4096,
            max_sse_frame_bytes: 8 * 1024 * 1024,
            max_request_body_bytes: 10 * 1024 * 1024,
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            model_routes: OrderedModelRoutes::default(),
            template_family: None,
            price_table: std::collections::HashMap::new(),
            auth: Default::default(),
            mesh: PersistedMeshConfig::default(),
        })
        .expect("config");

        // The LEAF's `policy_for_model` matches the profile keyed `MiMo-V2.5`
        // against the FINAL backend model `mimo-v2.5` via canonical key (case-
        // insensitive), surfacing that profile's `upstream_chat_kwargs`.
        assert_eq!(config.resolve_upstream_model("mimo-v2.5"), "mimo-v2.5");
        assert_eq!(
            leaf_chat_kwargs(&config, "mimo-v2.5"),
            JsonMap::from_iter([
                ("separate_reasoning".to_string(), JsonValue::Bool(true)),
                (
                    "chat_template_kwargs".to_string(),
                    json!({
                        "enable_thinking": true,
                        "keep_all_reasoning": true
                    }),
                ),
            ])
        );
    }

    #[test]
    fn resolves_upstream_model_profile_after_global_model_remap() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "https://openrouter.ai/api/v1".to_string(),
            upstream_api_key: None,
            upstream_model: Some("xiaomi/mimo-v2.5-pro".to_string()),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([(
                "xiaomi/mimo-v2.5-pro".to_string(),
                PersistedModelProfile {
                    extends: Vec::new(),
                    upstream_model: None,
                    system_prompt_prefix: Some("Use MiMo-compatible behavior.".to_string()),
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "reasoning".to_string(),
                        json!({
                            "enabled": true
                        }),
                    )]),
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            debug_log_max_age_hours: None,
            min_completion_tokens: 4096,
            max_sse_frame_bytes: 8 * 1024 * 1024,
            max_request_body_bytes: 10 * 1024 * 1024,
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            model_routes: OrderedModelRoutes::default(),
            template_family: None,
            price_table: std::collections::HashMap::new(),
            auth: Default::default(),
            mesh: PersistedMeshConfig::default(),
        })
        .expect("config");

        assert_eq!(
            config.resolve_upstream_model("client-default-model"),
            "xiaomi/mimo-v2.5-pro"
        );
        // The engine remaps the request model to the configured upstream model,
        // and the LEAF resolves kwargs against that FINAL backend id — the
        // profile keyed on the remap target supplies the kwargs.
        assert_eq!(
            leaf_chat_kwargs(&config, "xiaomi/mimo-v2.5-pro"),
            JsonMap::from_iter([(
                "reasoning".to_string(),
                json!({
                    "enabled": true
                }),
            )])
        );
        assert_eq!(
            config
                .resolve_system_prompt_prefix("client-default-model")
                .as_deref(),
            Some("Use MiMo-compatible behavior.")
        );
    }

    #[test]
    fn leaf_resolves_only_final_model_profile_not_request_alias() {
        // The OLD config merge layered the request-alias profile over the
        // backend profile and produced `{enabled:true, effort:high}` — a merge
        // the gateway NEVER runs. The REAL leaf keys kwargs by the FINAL backend
        // model ONLY (at-most-one per-model policy over the global base), so the
        // request-alias profile's kwargs do NOT bleed into the backend request.
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "https://openrouter.ai/api/v1".to_string(),
            upstream_api_key: None,
            upstream_model: Some("xiaomi/mimo-v2.5-pro".to_string()),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([
                (
                    "xiaomi/mimo-v2.5-pro".to_string(),
                    PersistedModelProfile {
                        extends: Vec::new(),
                        upstream_model: None,
                        system_prompt_prefix: Some("Backend prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "reasoning".to_string(),
                            json!({
                                "enabled": true,
                                "effort": "medium"
                            }),
                        )]),
                        template_family: None,
                        native_vision: None,
                        ..Default::default()
                    },
                ),
                (
                    "client-default-model".to_string(),
                    PersistedModelProfile {
                        extends: Vec::new(),
                        upstream_model: None,
                        system_prompt_prefix: Some("Client prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "reasoning".to_string(),
                            json!({
                                "effort": "high"
                            }),
                        )]),
                        template_family: None,
                        native_vision: None,
                        ..Default::default()
                    },
                ),
            ]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            debug_log_max_age_hours: None,
            min_completion_tokens: 4096,
            max_sse_frame_bytes: 8 * 1024 * 1024,
            max_request_body_bytes: 10 * 1024 * 1024,
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            model_routes: OrderedModelRoutes::default(),
            template_family: None,
            price_table: std::collections::HashMap::new(),
            auth: Default::default(),
            mesh: PersistedMeshConfig::default(),
        })
        .expect("config");

        // FINAL backend model is the remap target: only ITS profile's kwargs
        // apply. The request-alias `client-default-model` profile (`effort:high`)
        // is NOT layered, so the leaf keeps `effort: medium`.
        assert_eq!(
            leaf_chat_kwargs(&config, "xiaomi/mimo-v2.5-pro"),
            JsonMap::from_iter([(
                "reasoning".to_string(),
                json!({
                    "enabled": true,
                    "effort": "medium"
                }),
            )])
        );
        // The request-alias profile contributes ZERO kwargs at the leaf when it
        // is not the final backend model.
        assert_eq!(
            leaf_chat_kwargs(&config, "client-default-model"),
            JsonMap::from_iter([(
                "reasoning".to_string(),
                json!({
                    "effort": "high"
                }),
            )])
        );
        // The system-prompt-prefix path is UNCHANGED (still engine-side,
        // multi-profile): the request-model profile prefix still applies.
        assert_eq!(
            config
                .resolve_system_prompt_prefix("client-default-model")
                .as_deref(),
            Some("Client prefix.")
        );
    }

    #[test]
    fn resolves_exact_model_profile_before_case_insensitive_fallback() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([
                (
                    "MiMo-V2.5".to_string(),
                    PersistedModelProfile {
                        extends: Vec::new(),
                        upstream_model: Some("upper-profile".to_string()),
                        system_prompt_prefix: Some("Upper prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "stream_reasoning".to_string(),
                            JsonValue::Bool(true),
                        )]),
                        template_family: None,
                        native_vision: None,
                        ..Default::default()
                    },
                ),
                (
                    "mimo-v2.5".to_string(),
                    PersistedModelProfile {
                        extends: Vec::new(),
                        upstream_model: Some("lower-profile".to_string()),
                        system_prompt_prefix: Some("Lower prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "stream_reasoning".to_string(),
                            JsonValue::Bool(false),
                        )]),
                        template_family: None,
                        native_vision: None,
                        ..Default::default()
                    },
                ),
            ]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            debug_log_max_age_hours: None,
            min_completion_tokens: 4096,
            max_sse_frame_bytes: 8 * 1024 * 1024,
            max_request_body_bytes: 10 * 1024 * 1024,
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            model_routes: OrderedModelRoutes::default(),
            template_family: None,
            price_table: std::collections::HashMap::new(),
            auth: Default::default(),
            mesh: PersistedMeshConfig::default(),
        })
        .expect("config");

        assert_eq!(config.resolve_upstream_model("mimo-v2.5"), "lower-profile");
        // The LEAF's `policy_for_model` prefers the EXACT-id profile over the
        // case-insensitive (canonical-key) sibling: for FINAL backend `mimo-v2.5`
        // the lowercase profile (`false`) wins over `MiMo-V2.5` (`true`), even
        // though both share a canonical key.
        assert_eq!(
            leaf_chat_kwargs(&config, "mimo-v2.5"),
            JsonMap::from_iter([("stream_reasoning".to_string(), JsonValue::Bool(false))])
        );
        assert_eq!(
            config.resolve_system_prompt_prefix("mimo-v2.5").as_deref(),
            Some("Lower prefix.")
        );
    }

    #[test]
    fn resolves_global_system_prompt_prefix_with_profile_prefix() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            system_prompt_prefix: Some("Global prefix.".to_string()),
            model_profiles: BTreeMap::from_iter([(
                "GLM-5.1".to_string(),
                PersistedModelProfile {
                    extends: Vec::new(),
                    upstream_model: None,
                    system_prompt_prefix: Some("Profile prefix.".to_string()),
                    upstream_chat_kwargs: JsonMap::new(),
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            ..PersistedConfig::default()
        })
        .expect("config");

        assert_eq!(
            config.resolve_system_prompt_prefix("GLM-5.1").as_deref(),
            Some("Global prefix.\n\nProfile prefix.")
        );
        assert_eq!(
            config
                .resolve_system_prompt_prefix("unprofiled-model")
                .as_deref(),
            Some("Global prefix.")
        );
    }

    #[test]
    fn model_profiles_extend_templates_in_order() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            model_profile_templates: BTreeMap::from_iter([
                (
                    "reasoning".to_string(),
                    PersistedModelProfile {
                        extends: Vec::new(),
                        upstream_model: None,
                        system_prompt_prefix: Some("Reasoning prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([
                            (
                                "reasoning".to_string(),
                                json!({
                                    "enabled": true,
                                    "effort": "medium"
                                }),
                            ),
                            (
                                "chat_template_kwargs".to_string(),
                                json!({
                                    "nested": {
                                        "shared": "reasoning",
                                        "template_only": true
                                    }
                                }),
                            ),
                        ]),
                        template_family: None,
                        native_vision: None,
                        ..Default::default()
                    },
                ),
                (
                    "streaming".to_string(),
                    PersistedModelProfile {
                        extends: vec!["reasoning".to_string()],
                        upstream_model: None,
                        system_prompt_prefix: None,
                        upstream_chat_kwargs: JsonMap::from_iter([
                            ("stream_reasoning".to_string(), JsonValue::Bool(true)),
                            (
                                "reasoning".to_string(),
                                json!({
                                    "effort": "high"
                                }),
                            ),
                            (
                                "chat_template_kwargs".to_string(),
                                json!({
                                    "nested": {
                                        "shared": "streaming"
                                    }
                                }),
                            ),
                        ]),
                        template_family: None,
                        native_vision: None,
                        ..Default::default()
                    },
                ),
            ]),
            model_profiles: BTreeMap::from_iter([(
                "GLM-5.1".to_string(),
                PersistedModelProfile {
                    extends: vec!["streaming".to_string()],
                    upstream_model: None,
                    system_prompt_prefix: Some("Model prefix.".to_string()),
                    upstream_chat_kwargs: JsonMap::from_iter([
                        (
                            "reasoning".to_string(),
                            json!({
                                "max_tokens": 512
                            }),
                        ),
                        (
                            "chat_template_kwargs".to_string(),
                            json!({
                                "clear_thinking": false,
                                "nested": {
                                    "profile_only": true
                                }
                            }),
                        ),
                    ]),
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            ..PersistedConfig::default()
        })
        .expect("config");

        assert_eq!(
            config.resolve_system_prompt_prefix("GLM-5.1").as_deref(),
            Some("Reasoning prefix.\n\nModel prefix.")
        );
        // The per-model `upstream_chat_kwargs` are extends-merged at construction
        // (template order: reasoning -> streaming -> profile). The LEAF surfaces
        // that already-merged map for the FINAL backend model unchanged.
        assert_eq!(
            leaf_chat_kwargs(&config, "GLM-5.1"),
            JsonMap::from_iter([
                (
                    "reasoning".to_string(),
                    json!({
                        "enabled": true,
                        "effort": "high",
                        "max_tokens": 512
                    }),
                ),
                (
                    "chat_template_kwargs".to_string(),
                    json!({
                        "clear_thinking": false,
                        "nested": {
                            "shared": "streaming",
                            "template_only": true,
                            "profile_only": true
                        }
                    }),
                ),
                ("stream_reasoning".to_string(), JsonValue::Bool(true)),
            ])
        );
    }

    #[test]
    fn model_profile_shorthand_kwargs_merge_with_explicit_wrapper() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
model_profile_templates:
  reasoning:
    separate_reasoning: true
    chat_template_kwargs:
      thinking: true

model_profiles:
  Reasoner-D4:
    extends:
      - reasoning
    stream_reasoning: true
    chat_template_kwargs:
      separate_reasoning: true
      thinking: false
    upstream_chat_kwargs:
      reasoning_effort: high
      chat_template_kwargs:
        thinking: true
"#,
        )
        .expect("yaml");
        let config = Config::from_persisted(&persisted).expect("config");

        // The profile-root shorthand keys and the explicit `upstream_chat_kwargs`
        // wrapper are extends-merged into ONE per-model map at construction. The
        // LEAF surfaces that map for the FINAL backend model unchanged.
        assert_eq!(
            leaf_chat_kwargs(&config, "Reasoner-D4"),
            JsonMap::from_iter([
                ("separate_reasoning".to_string(), JsonValue::Bool(true)),
                ("stream_reasoning".to_string(), JsonValue::Bool(true)),
                ("reasoning_effort".to_string(), json!("high")),
                (
                    "chat_template_kwargs".to_string(),
                    json!({
                        "thinking": true,
                        "separate_reasoning": true
                    }),
                ),
            ])
        );
    }

    #[test]
    fn model_profiles_reject_unknown_template() {
        let error = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            model_profiles: BTreeMap::from_iter([(
                "GLM-5.1".to_string(),
                PersistedModelProfile {
                    extends: vec!["missing".to_string()],
                    upstream_model: None,
                    system_prompt_prefix: None,
                    upstream_chat_kwargs: JsonMap::new(),
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            ..PersistedConfig::default()
        })
        .expect_err("unknown template should fail");

        assert!(error.contains("model_profiles[GLM-5.1]: unknown template \"missing\""));
    }

    #[test]
    fn model_profiles_reject_template_cycles() {
        let error = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            model_profile_templates: BTreeMap::from_iter([
                (
                    "a".to_string(),
                    PersistedModelProfile {
                        extends: vec!["b".to_string()],
                        upstream_model: None,
                        system_prompt_prefix: None,
                        upstream_chat_kwargs: JsonMap::new(),
                        template_family: None,
                        native_vision: None,
                        ..Default::default()
                    },
                ),
                (
                    "b".to_string(),
                    PersistedModelProfile {
                        extends: vec!["a".to_string()],
                        upstream_model: None,
                        system_prompt_prefix: None,
                        upstream_chat_kwargs: JsonMap::new(),
                        template_family: None,
                        native_vision: None,
                        ..Default::default()
                    },
                ),
            ]),
            model_profiles: BTreeMap::from_iter([(
                "GLM-5.1".to_string(),
                PersistedModelProfile {
                    extends: vec!["a".to_string()],
                    upstream_model: None,
                    system_prompt_prefix: None,
                    upstream_chat_kwargs: JsonMap::new(),
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            ..PersistedConfig::default()
        })
        .expect_err("template cycle should fail");

        assert!(error.contains("model_profiles[GLM-5.1]: template cycle: a -> b -> a"));
    }

    #[test]
    fn merges_nested_profile_chat_kwargs() {
        let mut destination = JsonMap::from_iter([
            (
                "chat_template_kwargs".to_string(),
                json!({
                    "enable_thinking": true,
                    "clear_thinking": false
                }),
            ),
            ("stream_reasoning".to_string(), JsonValue::Bool(true)),
        ]);
        let source = JsonMap::from_iter([(
            "chat_template_kwargs".to_string(),
            json!({
                "thinking": true,
                "preserve_thinking": true
            }),
        )]);

        merge_json_maps(&mut destination, &source);

        assert_eq!(
            destination,
            JsonMap::from_iter([
                (
                    "chat_template_kwargs".to_string(),
                    json!({
                        "enable_thinking": true,
                        "clear_thinking": false,
                        "thinking": true,
                        "preserve_thinking": true
                    }),
                ),
                ("stream_reasoning".to_string(), JsonValue::Bool(true)),
            ])
        );
    }

    #[test]
    fn test_connect_timeout_default_is_10() {
        let persisted = PersistedConfig::default();
        assert_eq!(persisted.connect_timeout_secs, 10);
        let config = Config::from_persisted(&persisted).unwrap();
        assert_eq!(config.connect_timeout_secs, 10);
        assert_eq!(config.connect_timeout(), std::time::Duration::from_secs(10));
    }

    #[cfg(unix)]
    #[test]
    fn config_file_has_restricted_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!(
            "llmconduit-perms-{}.yaml",
            uuid::Uuid::new_v4().simple()
        ));
        let config = PersistedConfig::default();
        write_persisted_config(&path, &config).expect("write config");
        let metadata = std::fs::metadata(&path).expect("metadata");
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "config file should have 0600 permissions");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn passes_prefixed_model_name_unmodified_when_no_profile() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::new(),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            debug_log_max_age_hours: None,
            min_completion_tokens: 4096,
            max_sse_frame_bytes: 8 * 1024 * 1024,
            max_request_body_bytes: 10 * 1024 * 1024,
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            model_routes: OrderedModelRoutes::default(),
            template_family: None,
            price_table: std::collections::HashMap::new(),
            auth: Default::default(),
            mesh: PersistedMeshConfig::default(),
        })
        .expect("config");

        assert_eq!(
            config.resolve_upstream_model("anthropic/Kimi-K2.6"),
            "anthropic/Kimi-K2.6"
        );
        // With no profile and no global base, the LEAF resolves NO per-model
        // `upstream_chat_kwargs` for an unprofiled (non-family-sniffing) backend
        // model — the request passes through with an empty `extra_body`.
        assert_eq!(leaf_chat_kwargs(&config, "vendor/plain-v1"), JsonMap::new());
    }

    #[test]
    fn resolves_exact_prefix_model_profile_when_present() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            turn_capture_dir: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([(
                "anthropic/Kimi-K2.6".to_string(),
                PersistedModelProfile {
                    extends: Vec::new(),
                    upstream_model: Some("anthropic-custom".to_string()),
                    upstream_chat_kwargs: JsonMap::new(),
                    system_prompt_prefix: None,
                    template_family: None,
                    native_vision: None,
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            debug_log_max_age_hours: None,
            min_completion_tokens: 4096,
            max_sse_frame_bytes: 8 * 1024 * 1024,
            max_request_body_bytes: 10 * 1024 * 1024,
            image_agent_enabled: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
            unsupported_image_policy: UnsupportedImagePolicy::Placeholder,
            model_routes: OrderedModelRoutes::default(),
            template_family: None,
            price_table: std::collections::HashMap::new(),
            auth: Default::default(),
            mesh: PersistedMeshConfig::default(),
        })
        .expect("config");

        assert_eq!(
            config.resolve_upstream_model("anthropic/Kimi-K2.6"),
            "anthropic-custom"
        );
    }

    #[test]
    fn debug_log_dirs_single_failover_mode_includes_top_level_and_global_fallbacks() {
        // No `upstreams` => single/failover mode, matching the `upstreams`
        // empty branch in `build_app_with_gateway_and_options`. The top-level
        // primary path and the global fallback paths are the active log paths.
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            upstream_request_log_path: Some("/tmp/llmconduit-top/primary.jsonl".to_string()),
            fallback_upstreams: vec![PersistedFallbackUpstream {
                metrics_url: None,
                metrics_source: None,

                upstream_base_url: "http://127.0.0.1:8001/v1".to_string(),
                upstream_request_log_path: Some("/tmp/llmconduit-global/backup.jsonl".to_string()),
                ..PersistedFallbackUpstream::default()
            }],
            ..PersistedConfig::default()
        })
        .expect("config");

        let dirs = config.debug_log_dirs();
        assert_eq!(
            dirs,
            vec![
                PathBuf::from("/tmp/llmconduit-top"),
                PathBuf::from("/tmp/llmconduit-global"),
            ],
            "single/failover mode must include the top-level and global fallback log dirs"
        );
    }

    #[test]
    fn debug_log_dirs_routing_mode_excludes_inactive_top_level_and_global_fallbacks() {
        // Non-empty `upstreams` => routing mode. The gateway uses only the
        // per-routing-upstream clients and their nested fallbacks; the
        // top-level `upstream_request_log_path` and global `fallback_upstreams`
        // are never written to, so they must NOT be collected for cleanup.
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            upstream_request_log_path: Some(
                "/tmp/llmconduit-inactive-top/primary.jsonl".to_string(),
            ),
            fallback_upstreams: vec![PersistedFallbackUpstream {
                metrics_url: None,
                metrics_source: None,

                upstream_base_url: "http://127.0.0.1:9001/v1".to_string(),
                upstream_request_log_path: Some(
                    "/tmp/llmconduit-inactive-global/backup.jsonl".to_string(),
                ),
                ..PersistedFallbackUpstream::default()
            }],
            upstreams: vec![PersistedUpstream {
                metrics_url: None,
                metrics_source: None,

                upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
                upstream_request_log_path: Some(
                    "/tmp/llmconduit-routing/primary.jsonl".to_string(),
                ),
                fallback_upstreams: vec![PersistedFallbackUpstream {
                    metrics_url: None,
                    metrics_source: None,

                    upstream_base_url: "https://openrouter.ai/api/v1".to_string(),
                    upstream_request_log_path: Some(
                        "/tmp/llmconduit-routing-fallback/backup.jsonl".to_string(),
                    ),
                    ..PersistedFallbackUpstream::default()
                }],
                ..PersistedUpstream::default()
            }],
            ..PersistedConfig::default()
        })
        .expect("config");

        let dirs = config.debug_log_dirs();
        assert_eq!(
            dirs,
            vec![
                PathBuf::from("/tmp/llmconduit-routing"),
                PathBuf::from("/tmp/llmconduit-routing-fallback"),
            ],
            "routing mode must collect only routing-upstream + nested-fallback log dirs"
        );
        assert!(
            !dirs.contains(&PathBuf::from("/tmp/llmconduit-inactive-top")),
            "routing mode must exclude the inactive top-level log dir"
        );
        assert!(
            !dirs.contains(&PathBuf::from("/tmp/llmconduit-inactive-global")),
            "routing mode must exclude the inactive global fallback log dir"
        );
    }

    /// AC-2 (F1a): `debug_log_dirs()` includes the configured
    /// `turn_capture_dir` so the existing `debug_log_max_age_hours` rotation
    /// covers turn-capture artifacts too. Pushed AS-IS (it already IS the
    /// directory `<api_call_id>.json` artifacts land in, unlike the
    /// request-log FILE paths above whose *parent* directory is extracted).
    #[test]
    fn debug_log_dirs_includes_turn_capture_dir() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            upstream_request_log_path: Some("/tmp/llmconduit-top/primary.jsonl".to_string()),
            turn_capture_dir: Some("/tmp/llmconduit-turns".to_string()),
            ..PersistedConfig::default()
        })
        .expect("config");

        let dirs = config.debug_log_dirs();
        assert_eq!(
            dirs,
            vec![
                PathBuf::from("/tmp/llmconduit-top"),
                PathBuf::from("/tmp/llmconduit-turns"),
            ],
            "turn_capture_dir must be collected alongside the request-log dirs"
        );
    }

    /// AC-2 (F1a): unlike the per-provider request-log dirs, `turn_capture_dir`
    /// is engine-level -- it must be collected even in ROUTING mode (where the
    /// top-level `upstream_request_log_path` itself is inactive/excluded).
    #[test]
    fn debug_log_dirs_includes_turn_capture_dir_in_routing_mode() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            turn_capture_dir: Some("/tmp/llmconduit-turns".to_string()),
            upstreams: vec![PersistedUpstream {
                metrics_url: None,
                metrics_source: None,

                upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
                ..PersistedUpstream::default()
            }],
            ..PersistedConfig::default()
        })
        .expect("config");

        let dirs = config.debug_log_dirs();
        assert_eq!(
            dirs,
            vec![PathBuf::from("/tmp/llmconduit-turns")],
            "turn_capture_dir must be collected in routing mode too"
        );
    }

    /// `debug_log_dirs()` dedups when `turn_capture_dir` coincides with a
    /// directory already collected from a request-log path.
    #[test]
    fn debug_log_dirs_dedups_turn_capture_dir_against_request_log_dir() {
        let config = Config::from_persisted(&PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            upstream_request_log_path: Some("/tmp/llmconduit-shared/requests.jsonl".to_string()),
            turn_capture_dir: Some("/tmp/llmconduit-shared".to_string()),
            ..PersistedConfig::default()
        })
        .expect("config");

        let dirs = config.debug_log_dirs();
        assert_eq!(dirs, vec![PathBuf::from("/tmp/llmconduit-shared")]);
    }

    // -- D13 price table -------------------------------------------------------

    /// The YAML `price_table:` map deserializes into `Config.price_table` and
    /// `price_for` resolves an exact key. A missing `cached_per_1k` defaults to
    /// `0.0` (a provider with no cache discount), so a two-field entry is valid.
    #[test]
    fn price_table_loads_from_yaml_and_price_for_resolves() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            "price_table:\n  glm-5.1:\n    input_per_1k: 2.0\n    output_per_1k: 6.0\n    \
             cached_per_1k: 0.5\n  cheap-model:\n    input_per_1k: 0.1\n    output_per_1k: 0.2\n",
        )
        .expect("yaml parses");
        let config = Config::from_persisted(&persisted).expect("config");

        let glm = config.price_for("glm-5.1").expect("glm priced");
        assert_eq!(glm.input_per_1k, 2.0);
        assert_eq!(glm.output_per_1k, 6.0);
        assert_eq!(glm.cached_per_1k, 0.5);
        // The two-field entry defaults cached to 0.0.
        let cheap = config.price_for("cheap-model").expect("cheap priced");
        assert_eq!(cheap.cached_per_1k, 0.0);
        // An unknown model has no price (cost will report null, never a fake zero).
        assert!(config.price_for("unknown-model").is_none());
    }

    /// `price_for` falls back to an ASCII-case-insensitive match so a price keyed
    /// `GLM-5.1` still resolves a served `glm-5.1` (mirrors `model_profile`).
    #[test]
    fn price_for_is_case_insensitive() {
        let mut table = HashMap::new();
        table.insert("GLM-5.1".to_string(), ModelPrice::without_cached(1.0, 2.0));
        let persisted = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            price_table: table,
            ..Default::default()
        };
        let config = Config::from_persisted(&persisted).expect("config");
        assert!(
            config.price_for("glm-5.1").is_some(),
            "a differently-cased served model still resolves its configured price"
        );
    }

    /// `LLMCONDUIT_PRICE_TABLE_JSON` REPLACES the YAML price table wholesale (the
    /// env-JSON override pattern), and a malformed value is ignored (a typo cannot
    /// silently wipe a configured table).
    #[test]
    fn apply_env_overrides_price_table_json() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            std::env::set_var(
                "LLMCONDUIT_PRICE_TABLE_JSON",
                r#"{"env-model":{"input_per_1k":3.0,"output_per_1k":9.0,"cached_per_1k":1.0}}"#,
            );
        }
        let mut config = PersistedConfig::default();
        // Seed a YAML table the env override must REPLACE.
        config.price_table.insert(
            "yaml-model".to_string(),
            ModelPrice::without_cached(1.0, 1.0),
        );
        apply_env_overrides(&mut config);
        assert!(
            config.price_table.contains_key("env-model"),
            "the env JSON installs its model"
        );
        assert!(
            !config.price_table.contains_key("yaml-model"),
            "the env JSON REPLACES (not merges) the YAML table"
        );

        // A malformed value is ignored (the seeded table survives).
        unsafe {
            std::env::set_var("LLMCONDUIT_PRICE_TABLE_JSON", "{not valid json");
        }
        let mut config2 = PersistedConfig::default();
        config2
            .price_table
            .insert("kept".to_string(), ModelPrice::without_cached(1.0, 1.0));
        apply_env_overrides(&mut config2);
        assert!(
            config2.price_table.contains_key("kept"),
            "a malformed env JSON is ignored, leaving the configured table intact"
        );

        unsafe {
            std::env::remove_var("LLMCONDUIT_PRICE_TABLE_JSON");
        }
    }

    /// D13 R1 MED: a price entry carrying a NON-finite rate (NaN / ±∞) is REJECTED at
    /// config load so the in-memory table only ever holds finite prices — `serde_json`
    /// serializes NaN/Inf as `null`, which would silently corrupt the
    /// `/dashboard/api/topology` price table and violate the frozen finite-number
    /// `ModelPrice` contract. Both the YAML load and the env override drop the bad
    /// entry; a sibling finite entry survives.
    #[test]
    fn price_table_rejects_non_finite_rates_on_load() {
        // YAML `.inf` parses to an infinite f64; the entry must be dropped, the finite
        // sibling kept.
        let persisted: PersistedConfig = serde_yaml::from_str(
            "price_table:\n  good:\n    input_per_1k: 2.0\n    output_per_1k: 6.0\n  bad-inf:\n    \
             input_per_1k: .inf\n    output_per_1k: 1.0\n",
        )
        .expect("yaml parses");
        let config = Config::from_persisted(&persisted).expect("config");
        assert!(
            config.price_for("good").is_some(),
            "a finite YAML price survives the finite filter"
        );
        assert!(
            config.price_for("bad-inf").is_none(),
            "a non-finite (∞) YAML price entry is dropped at load"
        );

        // The env-override path applies the SAME `retain_finite_prices` filter before
        // installing the parsed table, so a NaN/±∞ in any of the three rate fields is
        // dropped while finite siblings survive. (`serde_json` rejects bare
        // `NaN`/overflow literals at parse time, so the filter is exercised directly on
        // a parsed table — the exact post-parse value the env path feeds it.)
        let mut table = HashMap::new();
        table.insert("finite".to_string(), ModelPrice::without_cached(1.0, 2.0));
        table.insert(
            "nan-input".to_string(),
            ModelPrice {
                input_per_1k: f64::NAN,
                output_per_1k: 2.0,
                cached_per_1k: 0.0,
                cached_price_configured: false,
            },
        );
        table.insert(
            "inf-cached".to_string(),
            ModelPrice {
                input_per_1k: 1.0,
                output_per_1k: 2.0,
                cached_per_1k: f64::INFINITY,
                cached_price_configured: true,
            },
        );
        retain_finite_prices(&mut table);
        assert!(table.contains_key("finite"), "a finite price survives");
        assert!(
            !table.contains_key("nan-input"),
            "a NaN rate in any field drops the entry"
        );
        assert!(
            !table.contains_key("inf-cached"),
            "an ∞ rate in any field drops the entry"
        );
    }

    /// Gap 07 — cached-price PRESENCE seam. A `ModelPrice` deserialized from a source
    /// that OMITS `cached_per_1k` has `cached_price_configured == false` (and the
    /// numeric defaults to `0.0`); one that EXPLICITLY sets `cached_per_1k: 0.0` has
    /// `cached_price_configured == true` — distinguishing a configured `0.0`
    /// cache-read rate from an absent one even though BOTH carry the same `0.0`
    /// numeric. This is the presence bit the cost-confidence seam reads (a default
    /// `0.0` cached charge ⇒ `estimated`; a configured one ⇒ `confident`). Round-trips
    /// through serialize → deserialize (AGENTS.md: no new wire field without a proof).
    #[test]
    fn model_price_cached_presence_distinguishes_configured_zero_from_omitted() {
        // OMITTED cached_per_1k ⇒ presence false, numeric defaults to 0.0.
        let omitted: ModelPrice =
            serde_yaml::from_str("input_per_1k: 2.0\noutput_per_1k: 6.0\n").expect("yaml");
        assert_eq!(omitted.cached_per_1k, 0.0);
        assert!(
            !omitted.cached_price_configured,
            "an omitted cached rate is NOT configured"
        );

        // EXPLICIT cached_per_1k: 0.0 ⇒ presence true (a configured zero cache rate),
        // distinct from the omitted case above despite the identical numeric.
        let configured_zero: ModelPrice =
            serde_yaml::from_str("input_per_1k: 2.0\noutput_per_1k: 6.0\ncached_per_1k: 0.0\n")
                .expect("yaml");
        assert_eq!(configured_zero.cached_per_1k, 0.0);
        assert!(
            configured_zero.cached_price_configured,
            "an explicit cached_per_1k: 0.0 IS configured (distinct from omitted)"
        );

        // A configured non-zero rate is likewise present.
        let configured: ModelPrice =
            serde_yaml::from_str("input_per_1k: 2.0\noutput_per_1k: 6.0\ncached_per_1k: 0.5\n")
                .expect("yaml");
        assert_eq!(configured.cached_per_1k, 0.5);
        assert!(configured.cached_price_configured);

        // Round-trip the presence flag through JSON: serialize emits the additive
        // `cached_price_configured` boolean; it survives a re-parse intact.
        let json = serde_json::to_string(&configured).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("re-parse");
        assert_eq!(value["cached_per_1k"], serde_json::json!(0.5));
        assert_eq!(value["cached_price_configured"], serde_json::json!(true));
        let back: ModelPrice = serde_json::from_value(value).expect("deserialize");
        assert!(back.cached_price_configured);
        assert_eq!(back.cached_per_1k, 0.5);

        // The `ModelPrice::new` constructor marks the cache rate configured; `without_cached`
        // does not — the two programmatic seams matching the YAML semantics.
        assert!(ModelPrice::new(1.0, 2.0, 0.0).cached_price_configured);
        assert!(!ModelPrice::without_cached(1.0, 2.0).cached_price_configured);
    }

    #[test]
    fn provider_metrics_targets_round_trip_and_validate_at_resolution() {
        let yaml = r#"
upstream_base_url: http://127.0.0.1:8000/v1
metrics_url: http://127.0.0.1:8000/metrics
metrics_source: vllm
upstreams:
  - name: sglang-a
    upstream_base_url: http://127.0.0.1:30000/v1
    metrics_url: http://127.0.0.1:30000/metrics
    metrics_source: sglang
    fallback_upstreams:
      - name: backup
        upstream_base_url: http://127.0.0.1:30001/v1
        metrics_url: http://127.0.0.1:30001/metrics
        metrics_source: vllm
"#;
        let persisted: PersistedConfig = serde_yaml::from_str(yaml).unwrap();
        let encoded = serde_yaml::to_string(&persisted).unwrap();
        let reparsed: PersistedConfig = serde_yaml::from_str(&encoded).unwrap();
        assert_eq!(persisted, reparsed);
        let resolved = Config::from_persisted(&persisted).unwrap();
        assert_eq!(resolved.provider_metrics_targets.len(), 3);

        let mut missing_source = persisted.clone();
        missing_source.metrics_source = None;
        assert!(
            Config::from_persisted(&missing_source)
                .unwrap_err()
                .contains("metrics_source is required")
        );

        let mut wrong_path = persisted;
        wrong_path.metrics_url = Some("http://127.0.0.1:8000/private".to_string());
        assert!(
            Config::from_persisted(&wrong_path)
                .unwrap_err()
                .contains("invalid root.metrics_url")
        );
    }

    /// Gap 07 review round 1, finding 3 — a NO-cache-rate price (`without_cached`,
    /// numeric `cached_per_1k: 0.0`, presence `false`) MUST round-trip as STILL NOT
    /// configured. The serialized form carries BOTH `cached_per_1k: 0.0` AND
    /// `cached_price_configured: false`; the re-parse must PREFER the explicit `false`
    /// flag over the presence-of-`cached_per_1k` heuristic (which would otherwise see
    /// `Some(0.0)` and flip presence to `true`, silently re-tagging a default-`0.0`
    /// cached charge as `confident`). This is the round-trip the prior presence test
    /// only proved for the `true` case.
    #[test]
    fn model_price_configured_false_round_trips_as_not_configured() {
        let unconfigured = ModelPrice::without_cached(2.0, 6.0);
        assert!(!unconfigured.cached_price_configured);

        // The serialized form carries the explicit flag alongside the numeric 0.0.
        let json = serde_json::to_string(&unconfigured).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("re-parse");
        assert_eq!(value["cached_per_1k"], serde_json::json!(0.0));
        assert_eq!(
            value["cached_price_configured"],
            serde_json::json!(false),
            "serialize emits the explicit presence flag"
        );

        // The re-parse honors the explicit `false` — NOT re-derived to `true` from the
        // present `cached_per_1k: 0.0`.
        let back: ModelPrice = serde_json::from_value(value).expect("deserialize");
        assert!(
            !back.cached_price_configured,
            "a serialized no-cache-rate price stays NOT configured on re-parse \
             (explicit flag beats the cached_per_1k-presence heuristic)"
        );
        assert_eq!(back.cached_per_1k, 0.0);
        assert_eq!(back, unconfigured, "full round-trip equality");

        // A whole `Config` round-trip through YAML (the persisted path) preserves it too:
        // the price serializes with its `cached_price_configured: false` and reloads NOT
        // configured, so the cost-confidence seam keeps tagging its cached charge
        // `estimated` rather than silently `confident`.
        let mut table = HashMap::new();
        table.insert("no-cache".to_string(), ModelPrice::without_cached(2.0, 6.0));
        let persisted = PersistedConfig {
            metrics_url: None,
            metrics_source: None,

            price_table: table,
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&persisted).expect("serialize config");
        let reloaded: PersistedConfig = serde_yaml::from_str(&yaml).expect("reload config");
        let config = Config::from_persisted(&reloaded).expect("config");
        let price = config.price_for("no-cache").expect("priced");
        assert!(
            !price.cached_price_configured,
            "the no-cache-rate price survives a full Config YAML round-trip as NOT configured"
        );

        // An EXPLICIT `cached_price_configured: true` is honored even when
        // `cached_per_1k` is OMITTED (the flag is authoritative when present).
        let flag_only: ModelPrice = serde_yaml::from_str(
            "input_per_1k: 2.0\noutput_per_1k: 6.0\ncached_price_configured: true\n",
        )
        .expect("yaml");
        assert!(
            flag_only.cached_price_configured,
            "an explicit flag wins even with cached_per_1k omitted"
        );
        assert_eq!(flag_only.cached_per_1k, 0.0);
    }

    #[test]
    fn mesh_absent_defaults_without_changing_existing_configs() {
        let config = Config::from_persisted(&PersistedConfig::default()).expect("config");

        assert!(!config.mesh.controller.enabled);
        assert_eq!(
            config.mesh.controller.bind_addr,
            "0.0.0.0:4433".parse().expect("addr")
        );
        assert!(config.mesh.controller.identity_path.is_none());
        assert!(config.mesh.controller.state_path.is_none());
        assert!(config.mesh.worker.resources.is_empty());

        let yaml = serde_yaml::to_string(&PersistedConfig::default()).expect("serialize");
        assert!(
            !yaml.contains("mesh:"),
            "default persisted config omits mesh so old configs round-trip unchanged"
        );
    }

    #[test]
    fn mesh_worker_schedule_loads_and_round_trips() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  worker:
    controller_addr: "mesh.example.com:4433"
    controller_endpoint_id: "controller-id"
    identity_path: "/var/lib/llmconduit/worker.key"
    node_name: "  lab-worker  "
    resources:
      - id: "primary"
        target: "127.0.0.1:8000"
        models: ["qwen3", "llama-vision"]
        availability:
          timezone: "America/Chicago"
          default_capacity: 0
          weekly:
            - days: [mon, tue, wed, thu, fri]
              start: "00:00"
              end: "08:00"
              capacity: 32
            - days: [mon, tue, wed, thu, fri]
              start: "18:00"
              end: "00:00"
              capacity: 16
          exceptions:
            - start: "2026-12-24T00:00:00-06:00"
              end: "2026-12-26T00:00:00-06:00"
              capacity: 0
"#,
        )
        .expect("yaml");
        let yaml = serde_yaml::to_string(&persisted).expect("serialize");
        let reparsed: PersistedConfig = serde_yaml::from_str(&yaml).expect("reparse");
        assert_eq!(reparsed.mesh, persisted.mesh);

        let config = Config::from_persisted(&persisted).expect("config");
        assert_eq!(config.mesh.worker.node_name.as_deref(), Some("lab-worker"));
        let resource = &config.mesh.worker.resources[0];
        assert_eq!(resource.id, "primary");
        assert_eq!(resource.models, ["qwen3", "llama-vision"]);
        assert_eq!(resource.model_refresh_secs, 60);
        assert_eq!(
            resource.capacity_source,
            MeshWorkerCapacitySource::Configured
        );
        assert_eq!(resource.availability.timezone, "America/Chicago");
        assert_eq!(resource.availability.weekly.len(), 2);
        assert_eq!(resource.availability.exceptions.len(), 1);
    }

    #[test]
    fn mesh_worker_vllm_capacity_source_loads_and_round_trips() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  worker:
    controller_addr: "mesh.example.com:4433"
    controller_endpoint_id: "controller-id"
    identity_path: "/var/lib/llmconduit/worker.key"
    resources:
      - id: "replica"
        target: "127.0.0.1:8115"
        capacity_source: vllm
        availability:
          default_capacity: 6
"#,
        )
        .expect("yaml");
        let yaml = serde_yaml::to_string(&persisted).expect("serialize");
        assert!(yaml.contains("capacity_source: vllm"));
        let reparsed: PersistedConfig = serde_yaml::from_str(&yaml).expect("reparse");
        assert_eq!(persisted.mesh, reparsed.mesh);
        let config = Config::from_persisted(&reparsed).expect("config");
        assert_eq!(
            config.mesh.worker.resources[0].capacity_source,
            MeshWorkerCapacitySource::Vllm
        );
    }

    #[test]
    fn mesh_worker_node_name_rejects_blank_control_and_oversized_values() {
        let blank: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  worker:
    node_name: "  "
"#,
        )
        .expect("yaml");
        let err = Config::from_persisted(&blank).expect_err("blank node name rejected");
        assert!(err.contains("mesh.worker.node_name must not be blank"));

        let control: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  worker:
    node_name: "node\nname"
"#,
        )
        .expect("yaml");
        let err = Config::from_persisted(&control).expect_err("control character rejected");
        assert!(err.contains("mesh.worker.node_name must not contain control characters"));

        let oversized = PersistedConfig {
            mesh: PersistedMeshConfig {
                worker: PersistedMeshWorkerConfig {
                    node_name: Some("x".repeat(crate::mesh::protocol::MAX_NODE_NAME_BYTES + 1)),
                    ..PersistedMeshWorkerConfig::default()
                },
                ..PersistedMeshConfig::default()
            },
            ..PersistedConfig::default()
        };
        let err = Config::from_persisted(&oversized).expect_err("oversized node name rejected");
        assert!(err.contains("mesh.worker.node_name must be at most"));
    }

    #[test]
    fn mesh_worker_models_reject_blank_entries() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  worker:
    controller_addr: "mesh.example.com:4433"
    controller_endpoint_id: "controller-id"
    identity_path: "/var/lib/llmconduit/worker.key"
    resources:
      - id: "primary"
        target: "127.0.0.1:8000"
        models: ["qwen3", " "]
"#,
        )
        .expect("yaml");

        let err = Config::from_persisted(&persisted).expect_err("blank model rejected");
        assert!(err.contains("models[1] must not be blank"));
    }

    #[test]
    fn mesh_controller_enabled_requires_identity_and_state_paths() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  controller:
    enabled: true
"#,
        )
        .expect("yaml");
        let err = Config::from_persisted(&persisted).expect_err("missing paths rejected");
        assert!(err.contains("mesh.controller.identity_path"));

        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  controller:
    enabled: true
    identity_path: "/var/lib/llmconduit/controller.key"
"#,
        )
        .expect("yaml");
        let err = Config::from_persisted(&persisted).expect_err("missing state rejected");
        assert!(err.contains("mesh.controller.state_path"));
    }

    #[test]
    fn mesh_controller_model_allowlist_loads() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  controller:
    enabled: true
    identity_path: "/var/lib/llmconduit/controller.key"
    state_path: "/var/lib/llmconduit/mesh.sqlite"
    model_allowlist:
      endpoint-a:
        primary: ["approved-model"]
"#,
        )
        .expect("yaml");

        let config = Config::from_persisted(&persisted).expect("config");

        assert_eq!(
            config.mesh.controller.model_allowlist["endpoint-a"]["primary"],
            ["approved-model"]
        );
    }

    #[test]
    fn mesh_weekly_overlap_is_rejected_including_cross_midnight_spillover() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  worker:
    controller_addr: "mesh.example.com:4433"
    controller_endpoint_id: "controller-id"
    identity_path: "/var/lib/llmconduit/worker.key"
    resources:
      - id: "primary"
        target: "127.0.0.1:8000"
        availability:
          timezone: "UTC"
          weekly:
            - days: [mon]
              start: "20:00"
              end: "02:00"
              capacity: 8
            - days: [tue]
              start: "01:00"
              end: "03:00"
              capacity: 4
"#,
        )
        .expect("yaml");
        let err = Config::from_persisted(&persisted).expect_err("overlap rejected");
        assert!(err.contains("overlaps mesh availability window"));
    }

    #[test]
    fn mesh_zero_length_weekly_window_is_rejected() {
        let persisted: PersistedConfig = serde_yaml::from_str(
            r#"
mesh:
  worker:
    controller_addr: "mesh.example.com:4433"
    controller_endpoint_id: "controller-id"
    identity_path: "/var/lib/llmconduit/worker.key"
    resources:
      - id: "primary"
        target: "127.0.0.1:8000"
        availability:
          timezone: "UTC"
          weekly:
            - days: [mon]
              start: "08:00"
              end: "08:00"
              capacity: 4
"#,
        )
        .expect("yaml");
        let err = Config::from_persisted(&persisted).expect_err("zero-length window rejected");
        assert!(err.contains("zero-length window"));
    }
}
