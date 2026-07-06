use serde::Deserialize;
use serde::Serialize;
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

fn default_true() -> bool {
    true
}

/// Thinking modes a profile can advertise for the `thinking` capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThinkingType {
    Adaptive,
    Enabled,
}

impl ThinkingType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ThinkingType::Adaptive => "adaptive",
            ThinkingType::Enabled => "enabled",
        }
    }
}

/// Reasoning-effort levels a profile can advertise for the `effort` capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffortLevel {
    Max,
    Xhigh,
    High,
    Medium,
    Low,
    Minimal,
    // Named `Disabled` (not `None`) so the variant does not shadow `Option::None` lexically;
    // `rename` keeps the wire format as "none" over the container's `rename_all="lowercase"`.
    #[serde(rename = "none")]
    Disabled,
}

impl EffortLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            EffortLevel::Max => "max",
            EffortLevel::Xhigh => "xhigh",
            EffortLevel::High => "high",
            EffortLevel::Medium => "medium",
            EffortLevel::Low => "low",
            EffortLevel::Minimal => "minimal",
            EffortLevel::Disabled => "none",
        }
    }
}

/// Context-management features a profile can advertise.
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
    pub fn as_str(&self) -> &'static str {
        match self {
            ContextFeature::ClearThinking20251015 => "clear_thinking_20251015",
            ContextFeature::ClearToolUses20250919 => "clear_tool_uses_20250919",
            ContextFeature::Compact20260112 => "compact_20260112",
        }
    }
}

fn supported_obj(supported: bool) -> JsonValue {
    let mut map = JsonMap::new();
    map.insert("supported".to_string(), JsonValue::Bool(supported));
    JsonValue::Object(map)
}

/// A capability with only a `supported` flag. A bare bool is shorthand for
/// `{supported: <bool>}`; an object may omit `supported` (defaults to true).
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct SimpleCap {
    pub supported: bool,
}

impl SimpleCap {
    pub fn to_wire(&self) -> JsonValue {
        supported_obj(self.supported)
    }
}

impl<'de> Deserialize<'de> for SimpleCap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if let Some(supported) = value.as_bool() {
            return Ok(SimpleCap { supported });
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            #[serde(default = "default_true")]
            supported: bool,
        }
        let raw = serde_json::from_value::<Raw>(value)
            .map_err(|err| <D::Error as serde::de::Error>::custom(err.to_string()))?;
        Ok(SimpleCap {
            supported: raw.supported,
        })
    }
}

/// The `thinking` capability. `types` lists advertised thinking modes; each emitted
/// type inherits the cap's `supported` flag (one knob, per the capabilities design).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThinkingCap {
    #[serde(default = "default_true")]
    pub supported: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub types: Vec<ThinkingType>,
}

impl ThinkingCap {
    pub fn to_wire(&self) -> JsonValue {
        let mut types = JsonMap::new();
        for thinking_type in &self.types {
            types.insert(
                thinking_type.as_str().to_string(),
                supported_obj(self.supported),
            );
        }
        let mut map = JsonMap::new();
        map.insert("supported".to_string(), JsonValue::Bool(self.supported));
        map.insert("types".to_string(), JsonValue::Object(types));
        JsonValue::Object(map)
    }
}

/// The `effort` capability. `levels` are emitted as siblings of `supported` on the
/// wire, each inheriting the cap's `supported` flag.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffortCap {
    #[serde(default = "default_true")]
    pub supported: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub levels: Vec<EffortLevel>,
}

impl EffortCap {
    pub fn to_wire(&self) -> JsonValue {
        let mut map = JsonMap::new();
        map.insert("supported".to_string(), JsonValue::Bool(self.supported));
        for level in &self.levels {
            map.insert(level.as_str().to_string(), supported_obj(self.supported));
        }
        JsonValue::Object(map)
    }
}

/// The `context_management` capability. `features` are emitted as siblings of
/// `supported`, each inheriting the cap's `supported` flag.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextManagementCap {
    #[serde(default = "default_true")]
    pub supported: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<ContextFeature>,
}

impl ContextManagementCap {
    pub fn to_wire(&self) -> JsonValue {
        let mut map = JsonMap::new();
        map.insert("supported".to_string(), JsonValue::Bool(self.supported));
        for feature in &self.features {
            map.insert(feature.as_str().to_string(), supported_obj(self.supported));
        }
        JsonValue::Object(map)
    }
}

/// Per-profile Anthropic model capabilities. Only `supported` is a knob (defaulting
/// to true); configured caps override the base capabilities wholesale per cap key.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<SimpleCap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub citations: Option<SimpleCap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_execution: Option<SimpleCap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_input: Option<SimpleCap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pdf_input: Option<SimpleCap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_outputs: Option<SimpleCap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingCap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<EffortCap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_management: Option<ContextManagementCap>,
}

impl CapabilitiesConfig {
    /// Override configured caps in `base` per cap key, wholesale. Unconfigured caps
    /// keep their base value.
    pub fn merge_into(&self, mut base: JsonValue) -> JsonValue {
        if let Some(map) = base.as_object_mut() {
            if let Some(cap) = &self.batch {
                map.insert("batch".to_string(), cap.to_wire());
            }
            if let Some(cap) = &self.citations {
                map.insert("citations".to_string(), cap.to_wire());
            }
            if let Some(cap) = &self.code_execution {
                map.insert("code_execution".to_string(), cap.to_wire());
            }
            if let Some(cap) = &self.image_input {
                map.insert("image_input".to_string(), cap.to_wire());
            }
            if let Some(cap) = &self.pdf_input {
                map.insert("pdf_input".to_string(), cap.to_wire());
            }
            if let Some(cap) = &self.structured_outputs {
                map.insert("structured_outputs".to_string(), cap.to_wire());
            }
            if let Some(cap) = &self.thinking {
                map.insert("thinking".to_string(), cap.to_wire());
            }
            if let Some(cap) = &self.effort {
                map.insert("effort".to_string(), cap.to_wire());
            }
            if let Some(cap) = &self.context_management {
                map.insert("context_management".to_string(), cap.to_wire());
            }
        }
        base
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub upstream_base_url: Url,
    pub upstream_api_key: Option<String>,
    pub upstream_model: Option<String>,
    pub default_reasoning_effort: String,
    pub system_prompt_prefix: Option<String>,
    pub upstream_request_log_path: Option<PathBuf>,
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    pub upstreams: Vec<UpstreamConfig>,
    pub fallback_upstreams: Vec<FallbackUpstreamConfig>,
    pub upstream_failure_cooldown_secs: u64,
    /// Per-model profiles keyed by name (case-insensitive lookup). The name `"*"` is
    /// reserved as the fallback profile for per-model settings that no specific profile covers.
    pub model_profiles: BTreeMap<String, ModelProfile>,
    pub brave_base_url: Url,
    pub brave_api_key: Option<String>,
    pub brave_max_results: usize,
    /// Kagi Search API base URL.
    pub kagi_base_url: Url,
    /// Kagi Search API key (Bearer token).
    pub kagi_api_key: Option<String>,
    /// Maximum number of Kagi search results.
    pub kagi_max_results: usize,
    /// Search backend selection: "brave" (Brave Search API) or "crawl4ai"
    /// (SearXNG + crawl4ai content extraction). Defaults to "brave" for
    /// backward compatibility; set to "crawl4ai" for a free self-hosted stack.
    pub search_backend: String,
    /// SearXNG instance base URL for the crawl4ai search backend.
    pub searxng_base_url: Url,
    /// crawl4ai Docker API server base URL.
    pub crawl4ai_base_url: Url,
    /// Bearer token for the crawl4ai API server (optional, local-only servers
    /// generate one at startup if unset).
    pub crawl4ai_api_token: Option<String>,
    /// Number of SearXNG results whose URLs are sent to crawl4ai for full-page
    /// extraction. Higher = richer context but slower.
    pub crawl4ai_max_crawl_urls: usize,
    /// Maximum characters of crawled Markdown included per result.
    pub crawl4ai_content_max_chars: usize,
    pub request_timeout: Duration,
    pub connect_timeout_secs: u64,
    pub max_web_search_rounds: usize,
    pub flatten_content: bool,
    pub max_replay_entries: usize,
    /// Master switch for the G4 image agent (vision offload). When `false` the
    /// strip/cache seam and `analyzeImage` tool injection are skipped entirely
    /// and images flow to the upstream unchanged.
    pub image_agent_enabled: bool,
    /// Always inject the `analyzeImage` tool + image-handling system prefix on
    /// every eligible turn (not just when the latest user turn has images), so
    /// the prompt head stays byte-identical across a session and an image
    /// arriving mid-session cannot invalidate the whole prefix cache. Also
    /// re-strips images in older history turns.
    pub image_agent_always_active: bool,
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

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct PersistedModelProfile {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extends: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "JsonMap::is_empty")]
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<CapabilitiesConfig>,
    /// Per-profile override for whether the resolved backend can natively see
    /// images (G4). `Some(true)` forces the image agent OFF for this profile
    /// (the backend is multimodal); `Some(false)` forces text-only handling
    /// even for a name the family sniff would treat as native-vision. `None`
    /// defers to the name-based default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_vision: Option<bool>,
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
            upstream_chat_kwargs: JsonMap<String, JsonValue>,
            #[serde(default, flatten)]
            shorthand_upstream_chat_kwargs: JsonMap<String, JsonValue>,
            #[serde(default)]
            capabilities: Option<CapabilitiesConfig>,
            #[serde(default)]
            native_vision: Option<bool>,
        }

        let raw = RawPersistedModelProfile::deserialize(deserializer)?;
        let mut upstream_chat_kwargs = raw.shorthand_upstream_chat_kwargs;
        // `native_vision` is a recognized profile knob, not a chat-template
        // shorthand kwarg, so drop any copy the `flatten` swept into the
        // shorthand bucket (it lives in its own typed field).
        upstream_chat_kwargs.remove("native_vision");
        merge_json_maps(&mut upstream_chat_kwargs, &raw.upstream_chat_kwargs);
        Ok(Self {
            extends: raw.extends,
            upstream_model: raw.upstream_model,
            system_prompt_prefix: raw.system_prompt_prefix,
            upstream_chat_kwargs,
            capabilities: raw.capabilities,
            native_vision: raw.native_vision,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModelProfile {
    pub upstream_model: Option<String>,
    pub system_prompt_prefix: Option<String>,
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    pub capabilities: Option<CapabilitiesConfig>,
    /// Per-profile native-vision override (G4); see `PersistedModelProfile`.
    pub native_vision: Option<bool>,
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
    #[serde(default = "default_reasoning_effort")]
    pub default_reasoning_effort: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_request_log_path: Option<String>,
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
    #[serde(default = "default_brave_base_url")]
    pub brave_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brave_api_key: Option<String>,
    #[serde(default = "default_brave_max_results")]
    pub brave_max_results: usize,
    /// Kagi Search API base URL.
    #[serde(default = "default_kagi_base_url")]
    pub kagi_base_url: String,
    /// Kagi Search API key (Bearer token).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kagi_api_key: Option<String>,
    /// Maximum number of Kagi search results.
    #[serde(default = "default_kagi_max_results")]
    pub kagi_max_results: usize,
    /// Search backend: "brave" or "crawl4ai". Defaults to "brave" for
    /// backward compatibility.
    #[serde(default = "default_search_backend")]
    pub search_backend: String,
    /// SearXNG base URL (crawl4ai backend search step).
    #[serde(default = "default_searxng_base_url")]
    pub searxng_base_url: String,
    /// crawl4ai Docker API server base URL.
    #[serde(default = "default_crawl4ai_base_url")]
    pub crawl4ai_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crawl4ai_api_token: Option<String>,
    #[serde(default = "default_crawl4ai_max_crawl_urls")]
    pub crawl4ai_max_crawl_urls: usize,
    #[serde(default = "default_crawl4ai_content_max_chars")]
    pub crawl4ai_content_max_chars: usize,
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
    /// Master switch for the G4 image agent (vision offload). Off by default so
    /// the gateway's text-first design is preserved unless explicitly opted in.
    #[serde(default)]
    pub image_agent_enabled: bool,
    /// Always-on tool injection for prompt-head/prefix-cache stability; see
    /// `Config::image_agent_always_active`.
    #[serde(default)]
    pub image_agent_always_active: bool,
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
}

fn default_bind_addr() -> String {
    "127.0.0.1:4000".to_string()
}

fn default_upstream_base_url() -> String {
    "http://127.0.0.1:8000/v1".to_string()
}

pub fn default_reasoning_effort() -> String {
    "max".to_string()
}

fn default_brave_base_url() -> String {
    "https://api.search.brave.com/res/v1".to_string()
}

fn default_brave_max_results() -> usize {
    5
}

fn default_kagi_base_url() -> String {
    "https://kagi.com/api/v1".to_string()
}

fn default_kagi_max_results() -> usize {
    5
}

fn default_search_backend() -> String {
    "brave".to_string()
}

fn default_searxng_base_url() -> String {
    "http://localhost:4040".to_string()
}

fn default_crawl4ai_base_url() -> String {
    "http://localhost:11235".to_string()
}

fn default_crawl4ai_max_crawl_urls() -> usize {
    3
}

fn default_crawl4ai_content_max_chars() -> usize {
    8000
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

impl Default for PersistedConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            upstream_base_url: default_upstream_base_url(),
            upstream_api_key: None,
            upstream_model: None,
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: default_upstream_failure_cooldown_secs(),
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::new(),
            brave_base_url: default_brave_base_url(),
            brave_api_key: None,
            brave_max_results: default_brave_max_results(),
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: default_search_backend(),
            searxng_base_url: default_searxng_base_url(),
            crawl4ai_base_url: default_crawl4ai_base_url(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: default_crawl4ai_max_crawl_urls(),
            crawl4ai_content_max_chars: default_crawl4ai_content_max_chars(),
            request_timeout_secs: default_request_timeout_secs(),
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: default_image_cache_max_size(),
            image_cache_ttl_secs: default_image_cache_ttl_secs(),
        }
    }
}

impl Config {
    pub fn from_env_and_file(path: Option<&Path>) -> Result<Self, String> {
        let mut persisted = if let Some(path) = path {
            load_persisted_config(path)?
        } else {
            load_default_persisted_config()?
        };
        apply_env_overrides(&mut persisted);
        Self::from_persisted(&persisted)
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
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
        let kagi_base_url = Url::parse(&config.kagi_base_url)
            .map_err(|err| format!("invalid kagi_base_url: {err}"))?;
        let searxng_base_url = Url::parse(&config.searxng_base_url)
            .map_err(|err| format!("invalid searxng_base_url: {err}"))?;
        let crawl4ai_base_url = Url::parse(&config.crawl4ai_base_url)
            .map_err(|err| format!("invalid crawl4ai_base_url: {err}"))?;
        let search_backend = match config.search_backend.trim().to_lowercase().as_str() {
            "brave" | "crawl4ai" | "kagi" => config.search_backend.trim().to_lowercase(),
            other => return Err(format!("invalid search_backend: '{other}' (expected 'brave', 'crawl4ai', or 'kagi')")),
        };
        let default_reasoning_effort =
            normalize_default_reasoning_effort(&config.default_reasoning_effort);
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
        let model_profiles =
            resolve_model_profiles(&config.model_profiles, &config.model_profile_templates)?;
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
            default_reasoning_effort,
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
            upstream_chat_kwargs: config.upstream_chat_kwargs.clone(),
            upstreams,
            fallback_upstreams,
            upstream_failure_cooldown_secs: config.upstream_failure_cooldown_secs,
            model_profiles,
            brave_base_url,
            brave_api_key: config
                .brave_api_key
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            brave_max_results: config.brave_max_results,
            kagi_base_url,
            kagi_api_key: config
                .kagi_api_key
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            kagi_max_results: config.kagi_max_results,
            search_backend,
            searxng_base_url,
            crawl4ai_base_url,
            crawl4ai_api_token: config
                .crawl4ai_api_token
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            crawl4ai_max_crawl_urls: config.crawl4ai_max_crawl_urls,
            crawl4ai_content_max_chars: config.crawl4ai_content_max_chars,
            request_timeout: Duration::from_secs(config.request_timeout_secs),
            connect_timeout_secs: config.connect_timeout_secs,
            max_web_search_rounds: config.max_web_search_rounds,
            flatten_content: config.flatten_content,
            max_replay_entries: config.max_replay_entries,
            image_agent_enabled: config.image_agent_enabled,
            image_agent_always_active: config.image_agent_always_active,
            vision_url,
            vision_model: trim_nonempty(config.vision_model.as_deref()),
            // Floor the capacity at 1 so a misconfigured zero does not make the
            // cache evict every image immediately and silently disable the agent.
            image_cache_max_size: config.image_cache_max_size.max(1),
            image_cache_ttl_secs: config.image_cache_ttl_secs,
        })
    }

    pub fn resolve_upstream_model(&self, request_model: &str) -> String {
        self.model_profile(request_model)
            .and_then(|profile| profile.upstream_model.clone())
            .or_else(|| self.upstream_model.clone())
            .unwrap_or_else(|| request_model.to_string())
    }

    pub fn resolve_upstream_chat_kwargs(&self, request_model: &str) -> JsonMap<String, JsonValue> {
        let upstream_model = self.resolve_upstream_model(request_model);
        self.resolve_upstream_chat_kwargs_for_resolved_model(request_model, &upstream_model)
    }

    pub fn resolve_upstream_chat_kwargs_for_resolved_model(
        &self,
        request_model: &str,
        resolved_model: &str,
    ) -> JsonMap<String, JsonValue> {
        let mut kwargs = self.upstream_chat_kwargs.clone();
        for profile in self.model_profiles_for_resolved_model(request_model, resolved_model) {
            merge_json_maps(&mut kwargs, &profile.upstream_chat_kwargs);
        }
        kwargs
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

    /// Resolve the capabilities config advertised for an upstream model id. A profile
    /// keyed by the id wins; otherwise the first alias (BTreeMap key order, i.e.
    /// lexicographically smallest profile key) whose `upstream_model` targets the id
    /// case-insensitively wins - so among several profiles aliasing the same upstream id
    /// the lexicographically-smallest profile key is selected, and the reserved `*`
    /// profile participates in this same tie-break. A matching profile without a
    /// `capabilities` block yields `None` (no fill-in). If no profile matches, the
    /// reserved `*` profile's `capabilities` is used.
    pub fn resolve_capabilities_for_upstream(&self, id: &str) -> Option<&CapabilitiesConfig> {
        if let Some(profile) = self.model_profile(id) {
            return profile.capabilities.as_ref();
        }
        for profile in self.model_profiles.values() {
            if profile
                .upstream_model
                .as_deref()
                .map(|upstream| upstream.eq_ignore_ascii_case(id))
                .unwrap_or(false)
            {
                return profile.capabilities.as_ref();
            }
        }
        self.model_profile("*")
            .and_then(|p| p.capabilities.as_ref())
    }

    /// Collect profiles matching the request model chain. `resolved_model` must be
    /// `resolve_upstream_model(request_model)` (callers pass the already-resolved upstream
    /// id); it is not re-resolved here. The resolved/upstream model is tried first, then the
    /// request model itself, with pointer-dedup to keep the precedence order stable. The
    /// reserved `*` profile is a pure fallback: it is included only when no specific profile
    /// matches, so an explicit match never inherits unset fields from `*` (use profile
    /// templates to share fields between explicit profiles).
    fn model_profiles_for_resolved_model(
        &self,
        request_model: &str,
        resolved_model: &str,
    ) -> Vec<&ModelProfile> {
        let mut profiles: Vec<&ModelProfile> = Vec::new();
        for model in [resolved_model, request_model] {
            if let Some(profile) = self.model_profile(model)
                && !profiles
                    .iter()
                    .any(|existing| std::ptr::eq(*existing, profile))
            {
                profiles.push(profile);
            }
        }
        if profiles.is_empty() {
            if let Some(profile) = self.model_profile("*") {
                profiles.push(profile);
            }
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

    /// Direct, PROFILE-ONLY `native_vision` lookup for EXACTLY `model` (G4
    /// round-9 #1). Looks up the profile keyed on `model` and returns its
    /// `native_vision`, with NO `upstream_model` remap. This is the ONLY
    /// native_vision accessor G4 gating uses: each input is already a final
    /// backend model (a candidate) or the literal request model, so re-applying
    /// the `upstream_model` remap would judge a DIFFERENT model's profile than
    /// the one the provider receives / than the request actually carries.
    pub fn profile_native_vision(&self, model: &str) -> Option<bool> {
        self.model_profile(model)
            .and_then(|profile| profile.native_vision)
    }
}

fn normalize_default_reasoning_effort(effort: &str) -> String {
    match effort.trim().to_ascii_lowercase().as_str() {
        "max" | "xhigh" => "max".to_string(),
        _ => "high".to_string(),
    }
}

#[derive(Debug, Clone, Default)]
struct ResolvedModelProfile {
    upstream_model: Option<String>,
    system_prompt_prefixes: Vec<String>,
    upstream_chat_kwargs: JsonMap<String, JsonValue>,
    capabilities: Option<CapabilitiesConfig>,
    native_vision: Option<bool>,
}

impl ResolvedModelProfile {
    fn into_model_profile(self) -> ModelProfile {
        ModelProfile {
            upstream_model: self.upstream_model,
            system_prompt_prefix: join_prompt_prefixes(self.system_prompt_prefixes),
            upstream_chat_kwargs: self.upstream_chat_kwargs,
            capabilities: self.capabilities,
            native_vision: self.native_vision,
        }
    }
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
        resolved.insert(name.to_string(), profile.into_model_profile());
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

/// Merge a resolved template (`source`) into the accumulating `destination`. The
/// `capabilities` block overrides wholesale: a child re-specifies a block to replace
/// the inherited one; omitting it (or writing `null`) does NOT clear an inherited
/// block - there is no opt-out, only override. Applies to `merge_persisted_model_profile`
/// below as well.
fn merge_resolved_model_profile(
    destination: &mut ResolvedModelProfile,
    source: ResolvedModelProfile,
) {
    if source.upstream_model.is_some() {
        destination.upstream_model = source.upstream_model;
    }
    if source.capabilities.is_some() {
        destination.capabilities = source.capabilities;
    }
    if source.native_vision.is_some() {
        destination.native_vision = source.native_vision;
    }
    destination
        .system_prompt_prefixes
        .extend(source.system_prompt_prefixes);
    merge_json_maps(
        &mut destination.upstream_chat_kwargs,
        &source.upstream_chat_kwargs,
    );
}

fn merge_persisted_model_profile(
    destination: &mut ResolvedModelProfile,
    source: &PersistedModelProfile,
) {
    if let Some(upstream_model) = trim_nonempty(source.upstream_model.as_deref()) {
        destination.upstream_model = Some(upstream_model);
    }
    if let Some(system_prompt_prefix) = trim_nonempty(source.system_prompt_prefix.as_deref()) {
        destination
            .system_prompt_prefixes
            .push(system_prompt_prefix);
    }
    if source.capabilities.is_some() {
        destination.capabilities = source.capabilities.clone();
    }
    if source.native_vision.is_some() {
        destination.native_vision = source.native_vision;
    }
    merge_json_maps(
        &mut destination.upstream_chat_kwargs,
        &source.upstream_chat_kwargs,
    );
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

pub fn default_config_path() -> Result<PathBuf, String> {
    let config_dir = dirs::config_dir()
        .ok_or_else(|| "unable to determine configuration directory".to_string())?;
    Ok(config_dir.join("llmconduit").join("config.yaml"))
}

pub fn load_default_persisted_config() -> Result<PersistedConfig, String> {
    let path = default_config_path()?;
    load_persisted_config(&path)
}

pub fn load_persisted_config(path: &Path) -> Result<PersistedConfig, String> {
    if !path.exists() {
        return Ok(PersistedConfig::default());
    }
    let contents = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    serde_yaml::from_str(&contents)
        .map_err(|err| format!("failed to parse {}: {err}", path.display()))
}

pub fn write_persisted_config(path: &Path, config: &PersistedConfig) -> Result<(), String> {
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
    if let Ok(value) = env::var("LLMCONDUIT_DEFAULT_REASONING_EFFORT")
        && !value.trim().is_empty()
    {
        config.default_reasoning_effort = value;
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
    if let Ok(value) = env::var("LLMCONDUIT_IMAGE_AGENT_ENABLED")
        && let Ok(parsed) = value.trim().parse::<bool>()
    {
        config.image_agent_enabled = parsed;
    }
    if let Ok(value) = env::var("LLMCONDUIT_IMAGE_AGENT_ALWAYS_ACTIVE")
        && let Ok(parsed) = value.trim().parse::<bool>()
    {
        config.image_agent_always_active = parsed;
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
    use super::CapabilitiesConfig;
    use super::Config;
    use super::ContextFeature;
    use super::EffortLevel;
    use super::JsonMap;
    use super::JsonValue;
    use super::PersistedConfig;
    use super::PersistedFallbackUpstream;
    use super::PersistedModelProfile;
    use super::PersistedUpstream;
    use super::SimpleCap;
    use super::ThinkingCap;
    use super::apply_env_overrides;
    use super::default_config_path;
    use super::default_crawl4ai_base_url;
    use super::default_crawl4ai_content_max_chars;
    use super::default_crawl4ai_max_crawl_urls;
    use super::default_kagi_base_url;
    use super::default_kagi_max_results;
    use super::default_reasoning_effort;
    use super::default_search_backend;
    use super::default_searxng_base_url;
    use super::load_persisted_config;
    use super::merge_json_maps;
    use super::write_persisted_config;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::collections::BTreeMap;
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
            upstream_base_url: "not a url".to_string(),
            ..PersistedConfig::default()
        };
        assert!(Config::from_persisted(&config).is_err());
    }

    #[test]
    fn whitespace_api_key_trimmed() {
        let config = PersistedConfig {
            upstream_api_key: Some("  secret  ".to_string()),
            ..PersistedConfig::default()
        };
        let result = Config::from_persisted(&config).unwrap();
        assert_eq!(result.upstream_api_key, Some("secret".to_string()));

        let config2 = PersistedConfig {
            upstream_api_key: Some("   ".to_string()),
            ..PersistedConfig::default()
        };
        let result2 = Config::from_persisted(&config2).unwrap();
        assert_eq!(result2.upstream_api_key, None);
    }

    #[test]
    fn default_reasoning_effort_defaults_to_max_and_normalizes_to_two_levels() {
        let result = Config::from_persisted(&PersistedConfig::default()).unwrap();
        assert_eq!(result.default_reasoning_effort, "max");

        let high_config = PersistedConfig {
            default_reasoning_effort: " low ".to_string(),
            ..PersistedConfig::default()
        };
        let result = Config::from_persisted(&high_config).unwrap();
        assert_eq!(result.default_reasoning_effort, "high");

        let max_config = PersistedConfig {
            default_reasoning_effort: " xhigh ".to_string(),
            ..PersistedConfig::default()
        };
        let result = Config::from_persisted(&max_config).unwrap();
        assert_eq!(result.default_reasoning_effort, "max");
    }

    #[test]
    fn from_persisted_parses_fallback_upstreams() {
        let config = PersistedConfig {
            fallback_upstreams: vec![
                PersistedFallbackUpstream {
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
            upstreams: vec![PersistedUpstream {
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
            fallback_upstreams: vec![PersistedFallbackUpstream {
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
        let mut config = PersistedConfig::default();
        config.upstream_api_key = None;
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
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: Some("upstream-secret".to_string()),
            upstream_model: Some("grok-4".to_string()),
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: Some("Global prefix.".to_string()),
            upstream_request_log_path: Some("/tmp/llmconduit-upstream.jsonl".to_string()),
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
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "stream_reasoning".to_string(),
                        JsonValue::Bool(true),
                    )]),
                    ..Default::default()
                },
            )]),
            model_profiles: BTreeMap::from_iter([(
                "Kimi-K2.6".to_string(),
                PersistedModelProfile {
                    extends: vec!["streaming-reasoning".to_string()],
                    system_prompt_prefix: Some("Use Kimi-compatible behavior.".to_string()),
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "chat_template_kwargs".to_string(),
                        json!({
                            "thinking": true,
                            "preserve_thinking": true
                        }),
                    )]),
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: Some("secret".to_string()),
            brave_max_results: 7,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: default_search_backend(),
            searxng_base_url: default_searxng_base_url(),
            crawl4ai_base_url: default_crawl4ai_base_url(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: default_crawl4ai_max_crawl_urls(),
            crawl4ai_content_max_chars: default_crawl4ai_content_max_chars(),
            request_timeout_secs: 45,
            connect_timeout_secs: 10,
            max_web_search_rounds: 10,
            flatten_content: false,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        };
        write_persisted_config(&path, &config).expect("write config");
        let loaded = load_persisted_config(&path).expect("load config");
        assert_eq!(loaded, config);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn resolves_profile_specific_upstream_chat_kwargs() {
        let config = Config::from_persisted(&PersistedConfig {
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([(
                "Kimi-K2.6".to_string(),
                PersistedModelProfile {
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "chat_template_kwargs".to_string(),
                        json!({
                            "thinking": true,
                            "preserve_thinking": true
                        }),
                    )]),
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: "brave".to_string(),
            searxng_base_url: "http://localhost:4040".to_string(),
            crawl4ai_base_url: "http://localhost:11235".to_string(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: 3,
            crawl4ai_content_max_chars: 8000,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        })
        .expect("config");

        assert_eq!(
            config.resolve_upstream_model("Kimi-K2.6"),
            "Kimi-K2.6".to_string()
        );
        assert_eq!(
            config.resolve_upstream_chat_kwargs("Kimi-K2.6"),
            JsonMap::from_iter([(
                "chat_template_kwargs".to_string(),
                json!({
                    "thinking": true,
                    "preserve_thinking": true
                }),
            )])
        );
    }

    #[test]
    fn star_profile_provides_upstream_chat_kwargs_for_unmatched_model() {
        let config = Config::from_persisted(&PersistedConfig {
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([(
                "*".to_string(),
                PersistedModelProfile {
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "chat_template_kwargs".to_string(),
                        json!({ "enable_thinking": true }),
                    )]),
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: "brave".to_string(),
            searxng_base_url: "http://localhost:4040".to_string(),
            crawl4ai_base_url: "http://localhost:11235".to_string(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: 3,
            crawl4ai_content_max_chars: 8000,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        })
        .expect("config");

        // No specific profile matches; the reserved `*` profile supplies the kwargs.
        assert_eq!(
            config.resolve_upstream_chat_kwargs("unmatched-model"),
            JsonMap::from_iter([(
                "chat_template_kwargs".to_string(),
                json!({ "enable_thinking": true }),
            )])
        );
    }

    #[test]
    fn explicit_profile_match_does_not_inherit_star_upstream_chat_kwargs() {
        let config = Config::from_persisted(&PersistedConfig {
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([
                (
                    "*".to_string(),
                    PersistedModelProfile {
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "chat_template_kwargs".to_string(),
                            json!({ "enable_thinking": true, "keep_all_reasoning": true }),
                        )]),
                        ..Default::default()
                    },
                ),
                (
                    "glm-5.2".to_string(),
                    PersistedModelProfile {
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "chat_template_kwargs".to_string(),
                            json!({ "enable_thinking": false }),
                        )]),
                        ..Default::default()
                    },
                ),
            ]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: "brave".to_string(),
            searxng_base_url: "http://localhost:4040".to_string(),
            crawl4ai_base_url: "http://localhost:11235".to_string(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: 3,
            crawl4ai_content_max_chars: 8000,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        })
        .expect("config");

        // An explicit match uses only that profile; the `*` profile does not fill in unset
        // fields (keep_all_reasoning is absent even though `*` sets it).
        assert_eq!(
            config.resolve_upstream_chat_kwargs("glm-5.2"),
            JsonMap::from_iter([(
                "chat_template_kwargs".to_string(),
                json!({ "enable_thinking": false }),
            )])
        );
    }

    #[test]
    fn resolves_model_profiles_case_insensitively() {
        let config = Config::from_persisted(&PersistedConfig {
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([(
                "MiMo-V2.5".to_string(),
                PersistedModelProfile {
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
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: "brave".to_string(),
            searxng_base_url: "http://localhost:4040".to_string(),
            crawl4ai_base_url: "http://localhost:11235".to_string(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: 3,
            crawl4ai_content_max_chars: 8000,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        })
        .expect("config");

        assert_eq!(config.resolve_upstream_model("mimo-v2.5"), "mimo-v2.5");
        assert_eq!(
            config.resolve_upstream_chat_kwargs("mimo-v2.5"),
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
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "https://openrouter.ai/api/v1".to_string(),
            upstream_api_key: None,
            upstream_model: Some("xiaomi/mimo-v2.5-pro".to_string()),
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([(
                "xiaomi/mimo-v2.5-pro".to_string(),
                PersistedModelProfile {
                    system_prompt_prefix: Some("Use MiMo-compatible behavior.".to_string()),
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "reasoning".to_string(),
                        json!({
                            "enabled": true
                        }),
                    )]),
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: "brave".to_string(),
            searxng_base_url: "http://localhost:4040".to_string(),
            crawl4ai_base_url: "http://localhost:11235".to_string(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: 3,
            crawl4ai_content_max_chars: 8000,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        })
        .expect("config");

        assert_eq!(
            config.resolve_upstream_model("client-default-model"),
            "xiaomi/mimo-v2.5-pro"
        );
        assert_eq!(
            config.resolve_upstream_chat_kwargs("client-default-model"),
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
    fn request_model_profile_overrides_upstream_model_profile_kwargs() {
        let config = Config::from_persisted(&PersistedConfig {
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "https://openrouter.ai/api/v1".to_string(),
            upstream_api_key: None,
            upstream_model: Some("xiaomi/mimo-v2.5-pro".to_string()),
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([
                (
                    "xiaomi/mimo-v2.5-pro".to_string(),
                    PersistedModelProfile {
                        system_prompt_prefix: Some("Backend prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "reasoning".to_string(),
                            json!({
                                "enabled": true,
                                "effort": "medium"
                            }),
                        )]),
                        ..Default::default()
                    },
                ),
                (
                    "client-default-model".to_string(),
                    PersistedModelProfile {
                        system_prompt_prefix: Some("Client prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "reasoning".to_string(),
                            json!({
                                "effort": "high"
                            }),
                        )]),
                        ..Default::default()
                    },
                ),
            ]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: "brave".to_string(),
            searxng_base_url: "http://localhost:4040".to_string(),
            crawl4ai_base_url: "http://localhost:11235".to_string(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: 3,
            crawl4ai_content_max_chars: 8000,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        })
        .expect("config");

        assert_eq!(
            config.resolve_upstream_chat_kwargs("client-default-model"),
            JsonMap::from_iter([(
                "reasoning".to_string(),
                json!({
                    "enabled": true,
                    "effort": "high"
                }),
            )])
        );
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
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([
                (
                    "MiMo-V2.5".to_string(),
                    PersistedModelProfile {
                        upstream_model: Some("upper-profile".to_string()),
                        system_prompt_prefix: Some("Upper prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "stream_reasoning".to_string(),
                            JsonValue::Bool(true),
                        )]),
                        ..Default::default()
                    },
                ),
                (
                    "mimo-v2.5".to_string(),
                    PersistedModelProfile {
                        upstream_model: Some("lower-profile".to_string()),
                        system_prompt_prefix: Some("Lower prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "stream_reasoning".to_string(),
                            JsonValue::Bool(false),
                        )]),
                        ..Default::default()
                    },
                ),
            ]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: "brave".to_string(),
            searxng_base_url: "http://localhost:4040".to_string(),
            crawl4ai_base_url: "http://localhost:11235".to_string(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: 3,
            crawl4ai_content_max_chars: 8000,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        })
        .expect("config");

        assert_eq!(config.resolve_upstream_model("mimo-v2.5"), "lower-profile");
        assert_eq!(
            config.resolve_upstream_chat_kwargs("mimo-v2.5"),
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
            system_prompt_prefix: Some("Global prefix.".to_string()),
            model_profiles: BTreeMap::from_iter([(
                "GLM-5.1".to_string(),
                PersistedModelProfile {
                    system_prompt_prefix: Some("Profile prefix.".to_string()),
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
            model_profile_templates: BTreeMap::from_iter([
                (
                    "reasoning".to_string(),
                    PersistedModelProfile {
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
                        ..Default::default()
                    },
                ),
                (
                    "streaming".to_string(),
                    PersistedModelProfile {
                        extends: vec!["reasoning".to_string()],
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
                        ..Default::default()
                    },
                ),
            ]),
            model_profiles: BTreeMap::from_iter([(
                "GLM-5.1".to_string(),
                PersistedModelProfile {
                    extends: vec!["streaming".to_string()],
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
        assert_eq!(
            config.resolve_upstream_chat_kwargs("GLM-5.1"),
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
  DeepSeek-V4-Pro:
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

        assert_eq!(
            config.resolve_upstream_chat_kwargs("DeepSeek-V4-Pro"),
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
            model_profiles: BTreeMap::from_iter([(
                "GLM-5.1".to_string(),
                PersistedModelProfile {
                    extends: vec!["missing".to_string()],
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
            model_profile_templates: BTreeMap::from_iter([
                (
                    "a".to_string(),
                    PersistedModelProfile {
                        extends: vec!["b".to_string()],
                        ..Default::default()
                    },
                ),
                (
                    "b".to_string(),
                    PersistedModelProfile {
                        extends: vec!["a".to_string()],
                        ..Default::default()
                    },
                ),
            ]),
            model_profiles: BTreeMap::from_iter([(
                "GLM-5.1".to_string(),
                PersistedModelProfile {
                    extends: vec!["a".to_string()],
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
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::new(),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: "brave".to_string(),
            searxng_base_url: "http://localhost:4040".to_string(),
            crawl4ai_base_url: "http://localhost:11235".to_string(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: 3,
            crawl4ai_content_max_chars: 8000,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        })
        .expect("config");

        assert_eq!(
            config.resolve_upstream_model("anthropic/Kimi-K2.6"),
            "anthropic/Kimi-K2.6"
        );
        assert_eq!(
            config.resolve_upstream_chat_kwargs("anthropic/Kimi-K2.6"),
            JsonMap::new()
        );
    }

    #[test]
    fn resolves_exact_prefix_model_profile_when_present() {
        let config = Config::from_persisted(&PersistedConfig {
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
            default_reasoning_effort: default_reasoning_effort(),
            system_prompt_prefix: None,
            upstream_request_log_path: None,
            upstream_chat_kwargs: JsonMap::new(),
            upstreams: Vec::new(),
            fallback_upstreams: Vec::new(),
            upstream_failure_cooldown_secs: 30,
            model_profile_templates: BTreeMap::new(),
            model_profiles: BTreeMap::from_iter([(
                "anthropic/Kimi-K2.6".to_string(),
                PersistedModelProfile {
                    upstream_model: Some("anthropic-custom".to_string()),
                    ..Default::default()
                },
            )]),
            brave_base_url: "https://api.search.brave.com/res/v1".to_string(),
            brave_api_key: None,
            brave_max_results: 5,
            kagi_base_url: default_kagi_base_url(),
            kagi_api_key: None,
            kagi_max_results: default_kagi_max_results(),
            search_backend: "brave".to_string(),
            searxng_base_url: "http://localhost:4040".to_string(),
            crawl4ai_base_url: "http://localhost:11235".to_string(),
            crawl4ai_api_token: None,
            crawl4ai_max_crawl_urls: 3,
            crawl4ai_content_max_chars: 8000,
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            image_agent_enabled: false,
            image_agent_always_active: false,
            vision_url: None,
            vision_model: None,
            image_cache_max_size: 100,
            image_cache_ttl_secs: 300,
        })
        .expect("config");

        assert_eq!(
            config.resolve_upstream_model("anthropic/Kimi-K2.6"),
            "anthropic-custom"
        );
    }

    #[test]
    fn simple_cap_accepts_bool_shorthand() {
        assert_eq!(
            serde_json::from_value::<SimpleCap>(json!(true)).unwrap(),
            SimpleCap { supported: true }
        );
        assert_eq!(
            serde_json::from_value::<SimpleCap>(json!(false)).unwrap(),
            SimpleCap { supported: false }
        );
    }

    #[test]
    fn simple_cap_object_supported_defaults_true() {
        assert_eq!(
            serde_json::from_value::<SimpleCap>(json!({})).unwrap(),
            SimpleCap { supported: true }
        );
        assert_eq!(
            serde_json::from_value::<SimpleCap>(json!({"supported": false})).unwrap(),
            SimpleCap { supported: false }
        );
    }

    #[test]
    fn simple_cap_rejects_unknown_keys() {
        assert!(serde_json::from_value::<SimpleCap>(json!({"supported": true, "x": 1})).is_err());
    }

    #[test]
    fn capabilities_reject_unknown_cap_key() {
        assert!(serde_json::from_value::<CapabilitiesConfig>(json!({"bogus": {}})).is_err());
    }

    #[test]
    fn effort_cap_rejects_unknown_level() {
        assert!(
            serde_json::from_value::<CapabilitiesConfig>(json!({"effort": {"levels": ["turbo"]}}))
                .is_err()
        );
    }

    #[test]
    fn thinking_cap_rejects_unknown_type() {
        assert!(
            serde_json::from_value::<CapabilitiesConfig>(json!({"thinking": {"types": ["bogus"]}}))
                .is_err()
        );
    }

    #[test]
    fn context_management_rejects_unknown_feature() {
        assert!(
            serde_json::from_value::<CapabilitiesConfig>(
                json!({"context_management": {"features": ["nope"]}})
            )
            .is_err()
        );
    }

    #[test]
    fn effort_cap_supported_defaults_true() {
        let caps: CapabilitiesConfig =
            serde_json::from_value(json!({"effort": {"levels": ["max"]}})).expect("parse");
        let effort = caps.effort.unwrap();
        assert!(effort.supported);
        assert_eq!(effort.levels, vec![EffortLevel::Max]);
    }

    #[test]
    fn capabilities_to_wire_thinking() {
        let caps: CapabilitiesConfig =
            serde_json::from_value(json!({"thinking": {"types": ["adaptive", "enabled"]}}))
                .expect("parse");
        assert_eq!(
            caps.thinking.unwrap().to_wire(),
            json!({
                "supported": true,
                "types": {
                    "adaptive": {"supported": true},
                    "enabled": {"supported": true}
                }
            })
        );
    }

    #[test]
    fn thinking_cap_to_wire_empty_types() {
        let cap = ThinkingCap {
            supported: true,
            types: vec![],
        };
        assert_eq!(cap.to_wire(), json!({"supported": true, "types": {}}));
    }

    #[test]
    fn capabilities_to_wire_effort_levels_are_siblings_of_supported() {
        let caps: CapabilitiesConfig =
            serde_json::from_value(json!({"effort": {"levels": ["max", "medium", "none"]}}))
                .expect("parse");
        assert_eq!(
            caps.effort.unwrap().to_wire(),
            json!({
                "supported": true,
                "max": {"supported": true},
                "medium": {"supported": true},
                "none": {"supported": true}
            })
        );
    }

    #[test]
    fn capabilities_to_wire_context_management() {
        let caps: CapabilitiesConfig = serde_json::from_value(json!({
            "context_management": {"features": ["clear_thinking_20251015", "compact_20260112"]}
        }))
        .expect("parse");
        assert_eq!(
            caps.context_management.unwrap().to_wire(),
            json!({
                "supported": true,
                "clear_thinking_20251015": {"supported": true},
                "compact_20260112": {"supported": true}
            })
        );
    }

    #[test]
    fn capabilities_to_wire_simple_caps() {
        let caps: CapabilitiesConfig = serde_json::from_value(json!({
            "batch": true,
            "image_input": {"supported": false}
        }))
        .expect("parse");
        assert_eq!(caps.batch.unwrap().to_wire(), json!({"supported": true}));
        assert_eq!(
            caps.image_input.unwrap().to_wire(),
            json!({"supported": false})
        );
    }

    #[test]
    fn capabilities_to_wire_supported_false_propagates_to_children() {
        let caps: CapabilitiesConfig =
            serde_json::from_value(json!({"effort": {"supported": false, "levels": ["max"]}}))
                .expect("parse");
        assert_eq!(
            caps.effort.unwrap().to_wire(),
            json!({
                "supported": false,
                "max": {"supported": false}
            })
        );
    }

    #[test]
    fn capabilities_merge_into_overrides_configured_caps_only() {
        let base = json!({
            "thinking": {"supported": false, "types": {"adaptive": {"supported": false}}},
            "effort": {"supported": false, "max": {"supported": false}},
            "image_input": {"supported": false}
        });
        let caps: CapabilitiesConfig =
            serde_json::from_value(json!({"thinking": {"types": ["enabled"]}})).expect("parse");
        let merged = caps.merge_into(base);
        assert_eq!(
            merged["thinking"],
            json!({
                "supported": true,
                "types": {"enabled": {"supported": true}}
            })
        );
        // Unconfigured caps keep the base value (wholesale, no fill-in).
        assert_eq!(merged["effort"]["supported"], false);
        assert_eq!(merged["image_input"]["supported"], false);
    }

    #[test]
    fn context_feature_roundtrips_through_serde() {
        let feature: ContextFeature =
            serde_json::from_value(json!("clear_tool_uses_20250919")).expect("parse");
        assert_eq!(feature.as_str(), "clear_tool_uses_20250919");
    }

    fn persisted_with_profiles(profiles: serde_json::Value) -> Config {
        let mut root = serde_json::Map::new();
        root.insert(
            "upstream_base_url".to_string(),
            json!("http://127.0.0.1:8000/v1"),
        );
        if let serde_json::Value::Object(map) = profiles {
            for (key, value) in map {
                root.insert(key, value);
            }
        }
        let persisted: PersistedConfig =
            serde_json::from_value(serde_json::Value::Object(root)).expect("parse config");
        Config::from_persisted(&persisted).expect("config")
    }

    #[test]
    fn resolve_capabilities_id_keyed_wins() {
        let config = persisted_with_profiles(json!({
            "model_profiles": {"glm-5.2": {"capabilities": {"thinking": {"types": ["adaptive"]}}}}
        }));
        let caps = config
            .resolve_capabilities_for_upstream("glm-5.2")
            .expect("caps");
        assert!(caps.thinking.is_some());
    }

    #[test]
    fn resolve_capabilities_alias_target_matches() {
        let config = persisted_with_profiles(json!({
            "model_profiles": {"glm-alias": {"upstream_model": "glm-5.2", "capabilities": {"effort": {"levels": ["max"]}}}}
        }));
        let caps = config
            .resolve_capabilities_for_upstream("glm-5.2")
            .expect("caps");
        assert!(caps.effort.is_some());
    }

    #[test]
    fn resolve_capabilities_unprofiled_uses_default() {
        let config = persisted_with_profiles(json!({
            "model_profiles": {"*": {"capabilities": {"thinking": {"types": ["adaptive"]}}}}
        }));
        let caps = config
            .resolve_capabilities_for_upstream("unknown-id")
            .expect("caps");
        assert!(caps.thinking.is_some());
    }

    #[test]
    fn resolve_capabilities_profiled_without_block_gets_none_no_fillin() {
        let config = persisted_with_profiles(json!({
            "model_profiles": {
                "*": {"capabilities": {"thinking": {"types": ["adaptive"]}}},
                "glm-5.2": {"upstream_model": "glm-5.2-upstream"}
            }
        }));
        assert!(
            config
                .resolve_capabilities_for_upstream("glm-5.2")
                .is_none()
        );
    }
}
