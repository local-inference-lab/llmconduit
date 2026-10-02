//! Native-vision auto-detection: does a backend model accept image input?
//!
//! The gateway degrades images to a text placeholder when the resolved backend
//! is not known to be multimodal. Knowing that from names alone is guesswork,
//! so the gateway first reads provider model metadata, then asks the engine only
//! when metadata is missing or inconclusive. Static routes and dashboard-managed
//! providers are checked at startup, periodically, and whenever the managed
//! provider registry changes. The fallback probe sends a one-pixel PNG with
//! `max_tokens: 1`: a 2xx means images are accepted; a 4xx whose error text
//! blames the image means text-only; anything else leaves the answer unknown.
//! An explicit profile `native_vision` override still wins, and an unknown model
//! keeps the name-based default.
//!
//! Results live in [`NativeVisionCache`], keyed by the exact model id the
//! provider receives — the same string the vision gate sees on a candidate —
//! so aliases, profiles and failover chains need no extra bookkeeping.

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// `control_plane.vision_probe` section.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VisionProbeBootstrap {
    /// Probe every routed (backend, model) pair for image support.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Seconds between probe rounds (the first round runs at startup).
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// Per-request timeout for one probe.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, serde_yaml::Value>,
}

impl Default for VisionProbeBootstrap {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: default_interval_secs(),
            timeout_secs: default_timeout_secs(),
            extra: BTreeMap::new(),
        }
    }
}

impl VisionProbeBootstrap {
    pub fn is_default(value: &Self) -> bool {
        *value == Self::default()
    }
}

const fn default_true() -> bool {
    true
}
const fn default_interval_secs() -> u64 {
    3_600
}
const fn default_timeout_secs() -> u64 {
    20
}

/// One (backend, model) pair to probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeTarget {
    pub backend: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub model: String,
}

/// What one probe established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// The engine accepted an image part.
    Native,
    /// The engine rejected the request because of the image.
    TextOnly,
    /// No conclusion (transport error, auth, 5xx, an unrelated 4xx).
    Unknown(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataVision {
    Native,
    TextOnly,
    Unknown,
}

const MAX_MODELS_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_PROBE_BODY_BYTES: usize = 64 * 1024;

/// A 1×1 red PNG, the smallest image an OpenAI-compatible endpoint will decode.
pub const PROBE_IMAGE_DATA_URL: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";

/// The chat-completions body the probe sends: one user turn with the image and
/// a one-word instruction, one output token, no streaming.
pub fn probe_body(model: &str) -> serde_json::Value {
    serde_json::json!({
        "model": model,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": PROBE_IMAGE_DATA_URL}},
                {"type": "text", "text": "Reply with OK."}
            ]
        }],
        "max_tokens": 1,
        "stream": false
    })
}

/// Classify a probe response. Only an image-related 4xx counts as text-only;
/// everything else that is not a success stays unknown so a flaky backend
/// never demotes a multimodal model.
pub fn classify_response(status: u16, body: &str) -> ProbeOutcome {
    if (200..300).contains(&status) {
        return ProbeOutcome::Native;
    }
    if (400..500).contains(&status)
        && status != 401
        && status != 403
        && status != 404
        && status != 429
    {
        let lower = body.to_ascii_lowercase();
        let blames_image = [
            "image",
            "multimodal",
            "multi-modal",
            "vision",
            "image_url",
            "content type",
            "does not support",
            "not supported",
        ]
        .iter()
        .any(|needle| lower.contains(needle));
        if blames_image {
            return ProbeOutcome::TextOnly;
        }
    }
    ProbeOutcome::Unknown(format!("status {status}"))
}

/// Send one probe.
pub async fn probe_target(client: &reqwest::Client, target: &ProbeTarget) -> ProbeOutcome {
    let url = format!("{}/chat/completions", target.base_url.trim_end_matches('/'));
    let mut request = client.post(&url).json(&probe_body(&target.model));
    if let Some(key) = &target.api_key {
        request = request.bearer_auth(key);
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            let body = read_limited_body(response, MAX_PROBE_BODY_BYTES)
                .await
                .ok()
                .and_then(|body| String::from_utf8(body).ok())
                .unwrap_or_default();
            classify_response(status, &body)
        }
        Err(error) => ProbeOutcome::Unknown(error.without_url().to_string()),
    }
}

/// The (backend, model) pairs the gateway can route to: every provider of every
/// operational route with an explicit upstream model, plus the legacy primary
/// upstream when it names a model. Deduplicated by (base_url, model).
pub fn targets_from_routes(
    routes: &[crate::control_plane::OperationalRoutePlan],
    primary: Option<ProbeTarget>,
) -> Vec<ProbeTarget> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    let mut push = |target: ProbeTarget| {
        if seen.insert((target.base_url.clone(), target.model.clone())) {
            out.push(target);
        }
    };
    for route in routes {
        for provider in &route.providers {
            let Some(model) = provider.upstream_model.as_deref() else {
                continue;
            };
            push(ProbeTarget {
                backend: provider.backend_name.clone(),
                base_url: provider.base_url.to_string(),
                api_key: provider.api_key.clone(),
                model: model.to_string(),
            });
        }
    }
    if let Some(primary) = primary {
        push(primary);
    }
    out
}

/// Probe results keyed by the exact backend model id, then by backend. The
/// lookup is conservative: a model counts as native only when every backend
/// that was probed for it accepted the image.
#[derive(Debug, Clone, Default)]
pub struct NativeVisionCache {
    inner: Arc<RwLock<HashMap<String, HashMap<String, bool>>>>,
}

impl NativeVisionCache {
    pub fn record(&self, backend: &str, model: &str, native: bool) {
        let mut map = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.entry(model.to_string())
            .or_default()
            .insert(backend.to_string(), native);
    }

    /// `Some(true)` when every probed backend serving `model` accepts images,
    /// `Some(false)` when any rejected them, `None` when nothing is known.
    pub fn lookup(&self, model: &str) -> Option<bool> {
        let map = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let backends = map.get(model)?;
        if backends.is_empty() {
            return None;
        }
        Some(backends.values().all(|native| *native))
    }

    /// Remove stale entries that no current probe target can serve. Static and
    /// dynamic targets are passed together by the background prober so configured
    /// routes survive dashboard-provider churn.
    pub fn retain_targets(&self, targets: &HashSet<(String, String)>) {
        let mut map = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.retain(|model, backends| {
            backends.retain(|backend, _| targets.contains(&(backend.clone(), model.clone())));
            !backends.is_empty()
        });
    }

    /// Every recorded (model, backend, native) triple, sorted, for logs and the
    /// dashboard.
    pub fn snapshot(&self) -> Vec<(String, String, bool)> {
        let map = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out: Vec<(String, String, bool)> = map
            .iter()
            .flat_map(|(model, backends)| {
                backends
                    .iter()
                    .map(move |(backend, native)| (model.clone(), backend.clone(), *native))
            })
            .collect();
        out.sort();
        out
    }
}

fn parse_model_metadata(body: &[u8]) -> HashMap<String, MetadataVision> {
    let Ok(root) = serde_json::from_slice::<serde_json::Value>(body) else {
        return HashMap::new();
    };
    let mut out = HashMap::new();
    let Some(data) = root.get("data").and_then(|value| value.as_array()) else {
        return out;
    };
    for item in data {
        let Some(id) = item.get("id").and_then(|value| value.as_str()) else {
            continue;
        };
        let modalities = item
            .get("architecture")
            .and_then(|architecture| architecture.get("input_modalities"))
            .and_then(|modalities| modalities.as_array());
        let Some(modalities) = modalities else {
            out.insert(id.to_string(), MetadataVision::Unknown);
            continue;
        };
        if modalities.is_empty() {
            out.insert(id.to_string(), MetadataVision::Unknown);
            continue;
        }
        let mut saw_image = false;
        let mut valid = true;
        for modality in modalities {
            let Some(modality) = modality.as_str() else {
                valid = false;
                break;
            };
            if modality.eq_ignore_ascii_case("image") {
                saw_image = true;
            }
        }
        let metadata = if !valid {
            MetadataVision::Unknown
        } else if saw_image {
            MetadataVision::Native
        } else {
            MetadataVision::TextOnly
        };
        out.insert(id.to_string(), metadata);
    }
    out
}

async fn read_limited_body(
    response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, &'static str> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err("response body is too large");
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "failed to read response body")?;
        if body.len().saturating_add(chunk.len()) > limit {
            return Err("response body is too large");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn fetch_model_metadata(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
) -> Result<HashMap<String, MetadataVision>, String> {
    let url = format!("{}/models", base_url.trim_end_matches('/'));
    let mut request = client.get(&url);
    if let Some(key) = api_key {
        request = request.bearer_auth(key);
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("catalog metadata request failed: {}", error.without_url()))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("catalog metadata status {}", status.as_u16()));
    }
    let body = read_limited_body(response, MAX_MODELS_BODY_BYTES)
        .await
        .map_err(str::to_string)?;
    Ok(parse_model_metadata(&body))
}

/// One probe round over `targets`, recording conclusive answers.
pub async fn probe_round(
    client: &reqwest::Client,
    targets: &[ProbeTarget],
    cache: &NativeVisionCache,
) {
    let mut groups: HashMap<(String, Option<String>), Vec<&ProbeTarget>> = HashMap::new();
    for target in targets {
        groups
            .entry((target.base_url.clone(), target.api_key.clone()))
            .or_default()
            .push(target);
    }
    for ((base_url, api_key), targets) in groups {
        let metadata = match fetch_model_metadata(client, &base_url, api_key.as_deref()).await {
            Ok(metadata) => metadata,
            Err(reason) => {
                tracing::warn!(base_url = %base_url, reason = %reason, "vision probe: model metadata unavailable");
                HashMap::new()
            }
        };
        for target in targets {
            match metadata.get(&target.model).copied() {
                Some(MetadataVision::Native) => {
                    cache.record(&target.backend, &target.model, true);
                    tracing::info!(backend = %target.backend, model = %target.model, "vision probe: metadata advertises image input");
                    continue;
                }
                Some(MetadataVision::TextOnly) => {
                    cache.record(&target.backend, &target.model, false);
                    tracing::info!(backend = %target.backend, model = %target.model, "vision probe: metadata advertises text-only input");
                    continue;
                }
                Some(MetadataVision::Unknown) | None => {}
            }
            probe_one_target(client, target, cache).await;
        }
    }
}

async fn probe_one_target(
    client: &reqwest::Client,
    target: &ProbeTarget,
    cache: &NativeVisionCache,
) {
    match probe_target(client, target).await {
        ProbeOutcome::Native => {
            cache.record(&target.backend, &target.model, true);
            tracing::info!(backend = %target.backend, model = %target.model, "vision probe: images accepted");
        }
        ProbeOutcome::TextOnly => {
            cache.record(&target.backend, &target.model, false);
            tracing::info!(backend = %target.backend, model = %target.model, "vision probe: text-only");
        }
        ProbeOutcome::Unknown(reason) => {
            tracing::warn!(backend = %target.backend, model = %target.model, reason = %reason, "vision probe: no conclusion");
        }
    }
}

/// Spawn the background prober: one round now, then every `interval_secs`.
pub fn spawn(
    config: &VisionProbeBootstrap,
    targets: Vec<ProbeTarget>,
    cache: NativeVisionCache,
) -> Option<tokio::task::JoinHandle<()>> {
    spawn_with_managed(config, targets, cache, None)
}

/// Spawn the background prober for static routes and dashboard-managed
/// providers. Managed targets are refreshed when the dashboard provider
/// registry changes as well as on the periodic interval.
pub fn spawn_with_managed(
    config: &VisionProbeBootstrap,
    static_targets: Vec<ProbeTarget>,
    cache: NativeVisionCache,
    managed: Option<Arc<crate::managed_providers::ManagedProviderRegistry>>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !config.enabled {
        tracing::info!("native-vision probing disabled by configuration");
        return None;
    }
    if static_targets.is_empty() && managed.is_none() {
        tracing::info!("native-vision probing: no routed backend models to probe");
        return None;
    }
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs.max(1)))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(error = %error, "native-vision prober could not build an HTTP client");
            return None;
        }
    };
    let interval_secs = config.interval_secs.max(30);
    tracing::info!(
        static_targets = static_targets.len(),
        managed_targets = managed.is_some(),
        interval_secs,
        "native-vision probing enabled"
    );
    Some(tokio::spawn(async move {
        let mut managed_changes = managed
            .as_ref()
            .map(|registry| registry.vision_probe_changes());
        let mut managed_targets = Vec::new();
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(interval_secs),
            Duration::from_secs(interval_secs),
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if let Some(registry) = managed.as_ref() {
                match registry.vision_probe_targets().await {
                    Ok(targets) => {
                        managed_targets = targets;
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "native-vision prober could not load dashboard provider targets; preserving previous targets");
                    }
                }
            }
            let targets = merge_targets(&static_targets, &managed_targets);
            let active: HashSet<(String, String)> = targets
                .iter()
                .map(|target| (target.backend.clone(), target.model.clone()))
                .collect();
            cache.retain_targets(&active);
            match managed_changes.as_mut() {
                Some(changes) => {
                    let mut changed = false;
                    tokio::select! {
                        _ = probe_round(&client, &targets, &cache) => {}
                        result = changes.changed() => {
                            if result.is_err() {
                                managed_changes = None;
                            }
                            changed = true;
                        }
                    }
                    if changed {
                        continue;
                    }
                }
                None => {
                    probe_round(&client, &targets, &cache).await;
                }
            }
            match managed_changes.as_mut() {
                Some(changes) => {
                    tokio::select! {
                        _ = interval.tick() => {}
                        result = changes.changed() => {
                            if result.is_err() {
                                managed_changes = None;
                            }
                        }
                    }
                }
                None => {
                    interval.tick().await;
                }
            }
        }
    }))
}

fn merge_targets(
    static_targets: &[ProbeTarget],
    managed_targets: &[ProbeTarget],
) -> Vec<ProbeTarget> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for target in static_targets.iter().chain(managed_targets.iter()) {
        if seen.insert((
            target.backend.clone(),
            target.base_url.clone(),
            target.api_key.clone(),
            target.model.clone(),
        )) {
            out.push(target.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_and_yaml() {
        let config = VisionProbeBootstrap::default();
        assert!(config.enabled);
        assert_eq!(config.interval_secs, 3_600);
        assert!(VisionProbeBootstrap::is_default(&config));
        let parsed: VisionProbeBootstrap =
            serde_yaml::from_str("enabled: false\ninterval_secs: 60\n").unwrap();
        assert!(!parsed.enabled);
        assert_eq!(parsed.interval_secs, 60);
        assert_eq!(parsed.timeout_secs, 20);
    }

    #[test]
    fn classification_is_conservative() {
        assert_eq!(
            classify_response(200, r#"{"choices":[]}"#),
            ProbeOutcome::Native
        );
        assert_eq!(
            classify_response(
                400,
                r#"{"error":{"message":"This model does not support image input"}}"#
            ),
            ProbeOutcome::TextOnly
        );
        assert_eq!(
            classify_response(
                400,
                r#"{"error":{"message":"Invalid 'messages[0].content': multimodal content is not accepted"}}"#
            ),
            ProbeOutcome::TextOnly
        );
        assert!(matches!(
            classify_response(400, "max_tokens must be > 1"),
            ProbeOutcome::Unknown(_)
        ));
        assert!(matches!(
            classify_response(401, "image not allowed"),
            ProbeOutcome::Unknown(_)
        ));
        assert!(matches!(
            classify_response(404, "model not found"),
            ProbeOutcome::Unknown(_)
        ));
        assert!(matches!(
            classify_response(503, "image backend down"),
            ProbeOutcome::Unknown(_)
        ));
    }

    #[test]
    fn probe_body_is_one_image_one_token() {
        let body = probe_body("m");
        assert_eq!(body["model"], "m");
        assert_eq!(body["max_tokens"], 1);
        assert_eq!(body["messages"][0]["content"][0]["type"], "image_url");
        assert!(
            body["messages"][0]["content"][0]["image_url"]["url"]
                .as_str()
                .unwrap()
                .starts_with("data:image/png;base64,")
        );
    }

    #[test]
    fn cache_requires_every_backend_to_agree() {
        let cache = NativeVisionCache::default();
        assert_eq!(cache.lookup("m"), None);
        cache.record("vllm", "m", true);
        assert_eq!(cache.lookup("m"), Some(true));
        cache.record("litellm", "m", false);
        assert_eq!(cache.lookup("m"), Some(false));
        cache.record("litellm", "m", true);
        assert_eq!(cache.lookup("m"), Some(true));
        assert_eq!(cache.snapshot().len(), 2);
    }

    #[test]
    fn metadata_parser_uses_only_explicit_valid_modalities() {
        let parsed = parse_model_metadata(
            br#"{
                "data": [
                    {"id": "vision", "architecture": {"input_modalities": ["text", "image"]}},
                    {"id": "text", "architecture": {"input_modalities": ["text"]}},
                    {"id": "missing", "architecture": {}},
                    {"id": "empty", "architecture": {"input_modalities": []}},
                    {"id": "malformed", "architecture": {"input_modalities": ["text", 1]}}
                ]
            }"#,
        );
        assert_eq!(parsed.get("vision"), Some(&MetadataVision::Native));
        assert_eq!(parsed.get("text"), Some(&MetadataVision::TextOnly));
        assert_eq!(parsed.get("missing"), Some(&MetadataVision::Unknown));
        assert_eq!(parsed.get("empty"), Some(&MetadataVision::Unknown));
        assert_eq!(parsed.get("malformed"), Some(&MetadataVision::Unknown));
        assert!(parse_model_metadata(br#"{"data":"bad"}"#).is_empty());
    }

    #[tokio::test]
    async fn probe_round_uses_metadata_before_image_probe() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {"id": "vision", "architecture": {"input_modalities": ["text", "image"]}},
                    {"id": "text", "architecture": {"input_modalities": ["text"]}}
                ]
            })))
            .mount(&server)
            .await;
        let targets = vec![
            ProbeTarget {
                backend: "openrouter".into(),
                base_url: format!("{}/v1", server.uri()),
                api_key: Some("k".into()),
                model: "vision".into(),
            },
            ProbeTarget {
                backend: "openrouter".into(),
                base_url: format!("{}/v1", server.uri()),
                api_key: Some("k".into()),
                model: "text".into(),
            },
        ];
        let cache = NativeVisionCache::default();
        probe_round(&reqwest::Client::new(), &targets, &cache).await;
        assert_eq!(cache.lookup("vision"), Some(true));
        assert_eq!(cache.lookup("text"), Some(false));
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1, "metadata avoids image probes");
        assert_eq!(requests[0].method.as_str(), "GET");
        assert_eq!(
            requests[0].headers.get("authorization").unwrap(),
            "Bearer k"
        );
    }

    #[tokio::test]
    async fn probe_round_falls_back_when_metadata_is_unknown() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {"id": "unknown", "architecture": {"input_modalities": []}}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"choices":[]}"#))
            .mount(&server)
            .await;
        let targets = vec![ProbeTarget {
            backend: "vllm".into(),
            base_url: format!("{}/v1", server.uri()),
            api_key: None,
            model: "unknown".into(),
        }];
        let cache = NativeVisionCache::default();
        probe_round(&reqwest::Client::new(), &targets, &cache).await;
        assert_eq!(cache.lookup("unknown"), Some(true));
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2, "unknown metadata falls back to probe");
        assert_eq!(requests[0].method.as_str(), "GET");
        assert_eq!(requests[1].method.as_str(), "POST");
    }

    #[tokio::test]
    async fn probe_round_records_what_the_engines_answer() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let vision = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"choices":[]}"#))
            .mount(&vision)
            .await;
        let text = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(400).set_body_string(
                r#"{"error":{"message":"Multimodal input is not supported by this model"}}"#,
            ))
            .mount(&text)
            .await;
        let targets = vec![
            ProbeTarget {
                backend: "vllm-vision".into(),
                base_url: format!("{}/v1", vision.uri()),
                api_key: Some("k".into()),
                model: "V".into(),
            },
            ProbeTarget {
                backend: "vllm-text".into(),
                base_url: format!("{}/v1", text.uri()),
                api_key: None,
                model: "T".into(),
            },
            ProbeTarget {
                backend: "down".into(),
                base_url: "http://127.0.0.1:1/v1".into(),
                api_key: None,
                model: "D".into(),
            },
        ];
        let cache = NativeVisionCache::default();
        probe_round(&reqwest::Client::new(), &targets, &cache).await;
        assert_eq!(cache.lookup("V"), Some(true));
        assert_eq!(cache.lookup("T"), Some(false));
        assert_eq!(
            cache.lookup("D"),
            None,
            "an unreachable backend proves nothing"
        );
        let received = vision.received_requests().await.unwrap();
        assert_eq!(
            received[0].headers.get("authorization").unwrap(),
            "Bearer k"
        );
    }
}
