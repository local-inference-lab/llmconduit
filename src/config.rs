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

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub upstream_base_url: Url,
    pub upstream_api_key: Option<String>,
    pub upstream_model: Option<String>,
    pub system_prompt_prefix: Option<String>,
    pub upstream_request_log_path: Option<PathBuf>,
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
    pub upstreams: Vec<UpstreamConfig>,
    pub fallback_upstreams: Vec<FallbackUpstreamConfig>,
    pub upstream_failure_cooldown_secs: u64,
    pub model_profiles: BTreeMap<String, ModelProfile>,
    pub brave_base_url: Url,
    pub brave_api_key: Option<String>,
    pub brave_max_results: usize,
    pub request_timeout: Duration,
    pub connect_timeout_secs: u64,
    pub max_web_search_rounds: usize,
    pub flatten_content: bool,
    pub max_replay_entries: usize,
    pub compaction: crate::compaction::CompactionConfig,
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
        }

        let raw = RawPersistedModelProfile::deserialize(deserializer)?;
        let mut upstream_chat_kwargs = raw.shorthand_upstream_chat_kwargs;
        merge_json_maps(&mut upstream_chat_kwargs, &raw.upstream_chat_kwargs);
        Ok(Self {
            extends: raw.extends,
            upstream_model: raw.upstream_model,
            system_prompt_prefix: raw.system_prompt_prefix,
            upstream_chat_kwargs,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ModelProfile {
    pub upstream_model: Option<String>,
    pub system_prompt_prefix: Option<String>,
    pub upstream_chat_kwargs: JsonMap<String, JsonValue>,
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
    #[serde(default)]
    pub compaction: crate::compaction::CompactionConfig,
}

fn default_bind_addr() -> String {
    "127.0.0.1:4000".to_string()
}

fn default_upstream_base_url() -> String {
    "http://127.0.0.1:8000/v1".to_string()
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

impl Default for PersistedConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            upstream_base_url: default_upstream_base_url(),
            upstream_api_key: None,
            upstream_model: None,
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
            request_timeout_secs: default_request_timeout_secs(),
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            compaction: crate::compaction::CompactionConfig::default(),
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
            request_timeout: Duration::from_secs(config.request_timeout_secs),
            connect_timeout_secs: config.connect_timeout_secs,
            max_web_search_rounds: config.max_web_search_rounds,
            flatten_content: config.flatten_content,
            max_replay_entries: config.max_replay_entries,
            compaction: config.compaction.clone(),
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
    upstream_chat_kwargs: JsonMap<String, JsonValue>,
}

impl ResolvedModelProfile {
    fn into_model_profile(self) -> ModelProfile {
        ModelProfile {
            upstream_model: self.upstream_model,
            system_prompt_prefix: join_prompt_prefixes(self.system_prompt_prefixes),
            upstream_chat_kwargs: self.upstream_chat_kwargs,
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

fn merge_resolved_model_profile(
    destination: &mut ResolvedModelProfile,
    source: ResolvedModelProfile,
) {
    if source.upstream_model.is_some() {
        destination.upstream_model = source.upstream_model;
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
    use super::PersistedConfig;
    use super::PersistedFallbackUpstream;
    use super::PersistedModelProfile;
    use super::PersistedUpstream;
    use super::apply_env_overrides;
    use super::default_config_path;
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
                    extends: Vec::new(),
                    upstream_model: None,
                    system_prompt_prefix: None,
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "stream_reasoning".to_string(),
                        JsonValue::Bool(true),
                    )]),
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
            compaction: crate::compaction::CompactionConfig::default(),
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
            compaction: crate::compaction::CompactionConfig::default(),
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
    fn resolves_model_profiles_case_insensitively() {
        let config = Config::from_persisted(&PersistedConfig {
            bind_addr: "127.0.0.1:4010".to_string(),
            upstream_base_url: "http://127.0.0.1:8000/v1".to_string(),
            upstream_api_key: None,
            upstream_model: None,
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
            compaction: crate::compaction::CompactionConfig::default(),
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
                    extends: Vec::new(),
                    upstream_model: None,
                    system_prompt_prefix: Some("Use MiMo-compatible behavior.".to_string()),
                    upstream_chat_kwargs: JsonMap::from_iter([(
                        "reasoning".to_string(),
                        json!({
                            "enabled": true
                        }),
                    )]),
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
            compaction: crate::compaction::CompactionConfig::default(),
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
            compaction: crate::compaction::CompactionConfig::default(),
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
                        extends: Vec::new(),
                        upstream_model: Some("upper-profile".to_string()),
                        system_prompt_prefix: Some("Upper prefix.".to_string()),
                        upstream_chat_kwargs: JsonMap::from_iter([(
                            "stream_reasoning".to_string(),
                            JsonValue::Bool(true),
                        )]),
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
            compaction: crate::compaction::CompactionConfig::default(),
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
                    extends: Vec::new(),
                    upstream_model: None,
                    system_prompt_prefix: Some("Profile prefix.".to_string()),
                    upstream_chat_kwargs: JsonMap::new(),
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
                    upstream_model: None,
                    system_prompt_prefix: None,
                    upstream_chat_kwargs: JsonMap::new(),
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
                        upstream_model: None,
                        system_prompt_prefix: None,
                        upstream_chat_kwargs: JsonMap::new(),
                    },
                ),
                (
                    "b".to_string(),
                    PersistedModelProfile {
                        extends: vec!["a".to_string()],
                        upstream_model: None,
                        system_prompt_prefix: None,
                        upstream_chat_kwargs: JsonMap::new(),
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
            request_timeout_secs: 60,
            connect_timeout_secs: 10,
            max_web_search_rounds: 5,
            flatten_content: true,
            max_replay_entries: 1000,
            compaction: crate::compaction::CompactionConfig::default(),
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
                    extends: Vec::new(),
                    upstream_model: Some("anthropic-custom".to_string()),
                    upstream_chat_kwargs: JsonMap::new(),
                    system_prompt_prefix: None,
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
            compaction: crate::compaction::CompactionConfig::default(),
        })
        .expect("config");

        assert_eq!(
            config.resolve_upstream_model("anthropic/Kimi-K2.6"),
            "anthropic-custom"
        );
    }
}
