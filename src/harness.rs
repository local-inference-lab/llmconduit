//! Harness detection: which client program sent a request, and which session
//! and sub-session it belongs to.
//!
//! Detection is data-driven. A [`HarnessProfile`] pairs a matcher over the
//! request headers/body with extractors for the session identifiers. Profiles
//! are evaluated in order; the first whose matcher succeeds wins. A built-in
//! profile set (Claude Code, Codex, pi, oh-my-pi, pi-agent, generic) ships
//! embedded in the binary as YAML; operators extend, replace, or disable it
//! from `control_plane.sessions` without touching Rust. New extraction
//! primitives are enum variants here.
//!
//! This module is pure: it reads headers and a parsed body and returns a
//! [`HarnessIdentity`]. Persisting and linking sessions is the caller's job.

use axum::http::HeaderMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;

/// Longest identifier value retained from a header or body field.
const VALUE_CAP: usize = 256;

/// Placeholder in a header name that resolves to the configured
/// `auth.conversation_id_header`.
pub const CONVERSATION_HEADER_PLACEHOLDER: &str = "${conversation_id_header}";

/// Built-in profiles. Kept as YAML so the shipped rules read exactly like an
/// operator's own; `builtin_profiles_parse` asserts they always load.
pub const BUILTIN_PROFILES_YAML: &str = include_str!("harness_profiles.yaml");

// ---------------------------------------------------------------------------
// Configuration (serde)
// ---------------------------------------------------------------------------

/// `control_plane.sessions` section.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionsBootstrap {
    /// Infer sub-sessions from prefix lineage when a harness does not declare
    /// them. Profiles with `sub_sessions: none` opt out individually.
    #[serde(default = "default_true")]
    pub infer_sub_sessions: bool,
    /// How the built-in profile set combines with `harnesses`.
    #[serde(default)]
    pub builtin: BuiltinPolicy,
    /// Operator profiles. Evaluated before the built-ins under `extend`; a
    /// profile named like a built-in replaces it in place.
    #[serde(default)]
    pub harnesses: Vec<HarnessProfileConfig>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, serde_yaml::Value>,
}

impl Default for SessionsBootstrap {
    fn default() -> Self {
        Self {
            infer_sub_sessions: true,
            builtin: BuiltinPolicy::Extend,
            harnesses: Vec::new(),
            extra: BTreeMap::new(),
        }
    }
}

impl SessionsBootstrap {
    pub fn is_default(value: &Self) -> bool {
        *value == Self::default()
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinPolicy {
    /// Operator profiles first, then the built-ins (same-name overrides in place).
    #[default]
    Extend,
    /// Only operator profiles.
    Replace,
    /// Built-ins off and no operator profiles evaluated either: every request
    /// is `unknown` with no session identifiers.
    Disable,
}

/// How a profile's sub-sessions are established.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubSessionPolicy {
    /// Honor the declared `sub_session_id`/`parent_session_id`; requests
    /// without them still get lineage inference when globally enabled.
    Declared,
    /// Ignore any declared identifiers; rely on lineage inference only.
    #[default]
    Infer,
    /// No sub-sessions for this harness.
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HarnessProfileConfig {
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(rename = "match")]
    pub matcher: MatchConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<ExtractConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<ExtractConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_session_id: Option<ExtractConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<ExtractConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_kind: Option<ExtractConfig>,
    #[serde(default)]
    pub sub_sessions: SubSessionPolicy,
}

/// A header reference: either a bare name or `{name, regex}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum HeaderSpec {
    Name(String),
    Full {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        regex: Option<String>,
    },
}

impl HeaderSpec {
    fn parts(&self) -> (&str, Option<&str>) {
        match self {
            Self::Name(name) => (name, None),
            Self::Full { name, regex } => (name, regex.as_deref()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BodySpec {
    /// JSON pointer into the parsed inbound body (`/metadata/user_id`).
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
}

/// Parse the string at `path` as JSON, then read `pointer` inside it. Claude
/// Code's `metadata.user_id` is a JSON document encoded as a string.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonStringSpec {
    pub path: String,
    pub pointer: String,
}

/// One matcher: exactly one of the fields is set (`{ header: ... }`,
/// `{ all: [...] }`, ...). Modelled as a struct of options rather than an
/// enum so the same `{kind: value}` spelling works in YAML and JSON without
/// tags; `compile_matcher` enforces the exactly-one rule.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MatchConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<HeaderSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<BodySpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all: Option<Vec<MatchConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub any: Option<Vec<MatchConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not: Option<Box<MatchConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub always: Option<bool>,
}

/// One extractor: exactly one of the fields is set. See [`MatchConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtractConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<HeaderSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<BodySpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_string: Option<JsonStringSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_of: Option<Vec<ExtractConfig>>,
    #[serde(default, rename = "const", skip_serializing_if = "Option::is_none")]
    pub constant: Option<String>,
}

fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Compiled detector
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessConfigError(pub String);

impl fmt::Display for HarnessConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for HarnessConfigError {}

#[derive(Debug)]
enum Matcher {
    Header { name: String, regex: Option<Regex> },
    Body { path: String, regex: Option<Regex> },
    All(Vec<Matcher>),
    Any(Vec<Matcher>),
    Not(Box<Matcher>),
    Always(bool),
}

#[derive(Debug)]
enum Extractor {
    Header { name: String, regex: Option<Regex> },
    Body { path: String, regex: Option<Regex> },
    JsonString { path: String, pointer: String },
    FirstOf(Vec<Extractor>),
    Const(String),
}

#[derive(Debug)]
pub struct HarnessProfile {
    name: String,
    matcher: Matcher,
    version: Option<Extractor>,
    session_id: Option<Extractor>,
    sub_session_id: Option<Extractor>,
    parent_session_id: Option<Extractor>,
    session_kind: Option<Extractor>,
    sub_sessions: SubSessionPolicy,
}

impl HarnessProfile {
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// The compiled, ordered profile list.
#[derive(Debug)]
pub struct HarnessDetector {
    profiles: Vec<HarnessProfile>,
    infer_sub_sessions: bool,
}

/// What detection found for one request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct HarnessIdentity {
    /// Profile name, or `unknown` when no profile matched.
    pub harness: String,
    pub version: Option<String>,
    /// The harness's own session id, when declared on the wire.
    pub session_id: Option<String>,
    /// A declared sub-session id (an agent/thread id); absent or equal to
    /// `session_id` means this request belongs to the session itself.
    pub sub_session_id: Option<String>,
    /// The declared parent of `sub_session_id`, when nested.
    pub parent_session_id: Option<String>,
    pub session_kind: Option<String>,
    pub sub_sessions: SubSessionPolicy,
}

impl HarnessIdentity {
    pub fn unknown() -> Self {
        Self {
            harness: "unknown".to_string(),
            sub_sessions: SubSessionPolicy::Infer,
            ..Self::default()
        }
    }
}

impl HarnessDetector {
    /// Compile the effective profile list: operator profiles combined with
    /// the built-ins per `builtin`, with `${conversation_id_header}` resolved.
    pub fn from_config(
        config: &SessionsBootstrap,
        conversation_id_header: &str,
    ) -> Result<Self, HarnessConfigError> {
        let mut configs: Vec<HarnessProfileConfig> = match config.builtin {
            BuiltinPolicy::Disable => Vec::new(),
            BuiltinPolicy::Replace => config.harnesses.clone(),
            BuiltinPolicy::Extend => {
                let mut merged = builtin_profile_configs()?;
                for profile in config.harnesses.iter().rev() {
                    if let Some(slot) = merged.iter_mut().find(|p| p.name == profile.name) {
                        *slot = profile.clone();
                    } else {
                        merged.insert(0, profile.clone());
                    }
                }
                merged
            }
        };
        configs.retain(|profile| profile.enabled);
        let mut seen = std::collections::HashSet::new();
        for profile in &configs {
            if profile.name.trim().is_empty() {
                return Err(HarnessConfigError(
                    "control_plane.sessions.harnesses: a profile needs a name".to_string(),
                ));
            }
            if !seen.insert(profile.name.as_str()) {
                return Err(HarnessConfigError(format!(
                    "control_plane.sessions.harnesses: duplicate profile '{}'",
                    profile.name
                )));
            }
        }
        let profiles = configs
            .iter()
            .map(|profile| compile_profile(profile, conversation_id_header))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            profiles,
            infer_sub_sessions: config.infer_sub_sessions,
        })
    }

    /// The built-in set with default settings; never fails.
    pub fn builtin() -> Self {
        Self::from_config(
            &SessionsBootstrap::default(),
            crate::control_plane::DEFAULT_CONVERSATION_ID_HEADER,
        )
        .expect("built-in harness profiles compile")
    }

    pub fn profile_names(&self) -> Vec<&str> {
        self.profiles.iter().map(|p| p.name.as_str()).collect()
    }

    pub fn infer_sub_sessions(&self) -> bool {
        self.infer_sub_sessions
    }

    /// Run detection. `body` is the parsed inbound JSON when available.
    pub fn detect(&self, headers: &HeaderMap, body: Option<&Value>) -> HarnessIdentity {
        let input = Input { headers, body };
        for profile in &self.profiles {
            if !profile.matcher.matches(&input) {
                continue;
            }
            let extract = |extractor: &Option<Extractor>| {
                extractor
                    .as_ref()
                    .and_then(|extractor| extractor.extract(&input))
            };
            let session_id = extract(&profile.session_id);
            let mut sub_session_id = extract(&profile.sub_session_id);
            if sub_session_id.is_some() && sub_session_id == session_id {
                // The root thread of a session names itself; not a sub-session.
                sub_session_id = None;
            }
            let mut parent_session_id = extract(&profile.parent_session_id);
            if parent_session_id.is_some()
                && (parent_session_id == session_id || parent_session_id == sub_session_id)
            {
                parent_session_id = None;
            }
            return HarnessIdentity {
                harness: profile.name.clone(),
                version: extract(&profile.version),
                session_id,
                sub_session_id,
                parent_session_id,
                session_kind: extract(&profile.session_kind),
                sub_sessions: profile.sub_sessions,
            };
        }
        HarnessIdentity::unknown()
    }
}

fn builtin_profile_configs() -> Result<Vec<HarnessProfileConfig>, HarnessConfigError> {
    #[derive(Deserialize)]
    struct Document {
        harnesses: Vec<HarnessProfileConfig>,
    }
    serde_yaml::from_str::<Document>(BUILTIN_PROFILES_YAML)
        .map(|document| document.harnesses)
        .map_err(|error| HarnessConfigError(format!("built-in harness profiles: {error}")))
}

fn compile_profile(
    config: &HarnessProfileConfig,
    conversation_id_header: &str,
) -> Result<HarnessProfile, HarnessConfigError> {
    let context = format!("harness profile '{}'", config.name);
    let compile_extract = |extractor: &Option<ExtractConfig>| {
        extractor
            .as_ref()
            .map(|extractor| compile_extractor(extractor, conversation_id_header, &context))
            .transpose()
    };
    Ok(HarnessProfile {
        name: config.name.clone(),
        matcher: compile_matcher(&config.matcher, conversation_id_header, &context)?,
        version: compile_extract(&config.version)?,
        session_id: compile_extract(&config.session_id)?,
        sub_session_id: compile_extract(&config.sub_session_id)?,
        parent_session_id: compile_extract(&config.parent_session_id)?,
        session_kind: compile_extract(&config.session_kind)?,
        sub_sessions: config.sub_sessions,
    })
}

fn compile_regex(regex: Option<&str>, context: &str) -> Result<Option<Regex>, HarnessConfigError> {
    regex
        .map(|pattern| {
            Regex::new(pattern)
                .map_err(|error| HarnessConfigError(format!("{context}: bad regex: {error}")))
        })
        .transpose()
}

fn header_name(name: &str, conversation_id_header: &str) -> String {
    if name == CONVERSATION_HEADER_PLACEHOLDER {
        conversation_id_header.to_ascii_lowercase()
    } else {
        name.trim().to_ascii_lowercase()
    }
}

fn exactly_one(set: &[(&str, bool)], what: &str, context: &str) -> Result<(), HarnessConfigError> {
    let chosen: Vec<&str> = set
        .iter()
        .filter(|(_, present)| *present)
        .map(|(name, _)| *name)
        .collect();
    match chosen.len() {
        1 => Ok(()),
        0 => Err(HarnessConfigError(format!(
            "{context}: a {what} needs exactly one of {}",
            set.iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>()
                .join(", ")
        ))),
        _ => Err(HarnessConfigError(format!(
            "{context}: a {what} has several kinds set ({})",
            chosen.join(", ")
        ))),
    }
}

fn compile_matcher(
    config: &MatchConfig,
    conversation_id_header: &str,
    context: &str,
) -> Result<Matcher, HarnessConfigError> {
    exactly_one(
        &[
            ("header", config.header.is_some()),
            ("body", config.body.is_some()),
            ("all", config.all.is_some()),
            ("any", config.any.is_some()),
            ("not", config.not.is_some()),
            ("always", config.always.is_some()),
        ],
        "matcher",
        context,
    )?;
    let compile_list = |items: &[MatchConfig]| {
        items
            .iter()
            .map(|item| compile_matcher(item, conversation_id_header, context))
            .collect::<Result<Vec<_>, _>>()
    };
    Ok(if let Some(spec) = &config.header {
        let (name, regex) = spec.parts();
        Matcher::Header {
            name: header_name(name, conversation_id_header),
            regex: compile_regex(regex, context)?,
        }
    } else if let Some(spec) = &config.body {
        Matcher::Body {
            path: spec.path.clone(),
            regex: compile_regex(spec.regex.as_deref(), context)?,
        }
    } else if let Some(items) = &config.all {
        Matcher::All(compile_list(items)?)
    } else if let Some(items) = &config.any {
        Matcher::Any(compile_list(items)?)
    } else if let Some(inner) = &config.not {
        Matcher::Not(Box::new(compile_matcher(
            inner,
            conversation_id_header,
            context,
        )?))
    } else {
        Matcher::Always(config.always.unwrap_or(false))
    })
}

fn compile_extractor(
    config: &ExtractConfig,
    conversation_id_header: &str,
    context: &str,
) -> Result<Extractor, HarnessConfigError> {
    exactly_one(
        &[
            ("header", config.header.is_some()),
            ("body", config.body.is_some()),
            ("json_string", config.json_string.is_some()),
            ("first_of", config.first_of.is_some()),
            ("const", config.constant.is_some()),
        ],
        "extractor",
        context,
    )?;
    Ok(if let Some(spec) = &config.header {
        let (name, regex) = spec.parts();
        Extractor::Header {
            name: header_name(name, conversation_id_header),
            regex: compile_regex(regex, context)?,
        }
    } else if let Some(spec) = &config.body {
        Extractor::Body {
            path: spec.path.clone(),
            regex: compile_regex(spec.regex.as_deref(), context)?,
        }
    } else if let Some(spec) = &config.json_string {
        Extractor::JsonString {
            path: spec.path.clone(),
            pointer: spec.pointer.clone(),
        }
    } else if let Some(items) = &config.first_of {
        Extractor::FirstOf(
            items
                .iter()
                .map(|item| compile_extractor(item, conversation_id_header, context))
                .collect::<Result<_, _>>()?,
        )
    } else {
        Extractor::Const(config.constant.clone().unwrap_or_default())
    })
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

struct Input<'a> {
    headers: &'a HeaderMap,
    body: Option<&'a Value>,
}

impl Input<'_> {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    fn body_scalar(&self, path: &str) -> Option<String> {
        let value = self.body?.pointer(path)?;
        match value {
            Value::String(text) => {
                let text = text.trim();
                (!text.is_empty()).then(|| text.to_string())
            }
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(flag) => Some(flag.to_string()),
            _ => None,
        }
    }
}

/// Apply an optional regex: the first capture group when present, else the
/// whole match. `None` when the regex does not match.
fn apply_regex(regex: Option<&Regex>, value: &str) -> Option<String> {
    match regex {
        None => Some(value.to_string()),
        Some(regex) => {
            let captures = regex.captures(value)?;
            let matched = captures
                .get(1)
                .or_else(|| captures.get(0))
                .map(|m| m.as_str())?;
            Some(matched.to_string())
        }
    }
}

fn bounded(value: String) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.len() <= VALUE_CAP {
        return Some(value.to_string());
    }
    let mut end = VALUE_CAP;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    Some(value[..end].to_string())
}

impl Matcher {
    fn matches(&self, input: &Input<'_>) -> bool {
        match self {
            Self::Header { name, regex } => input
                .header(name)
                .is_some_and(|value| apply_regex(regex.as_ref(), value).is_some()),
            Self::Body { path, regex } => input
                .body_scalar(path)
                .is_some_and(|value| apply_regex(regex.as_ref(), &value).is_some()),
            Self::All(items) => items.iter().all(|item| item.matches(input)),
            Self::Any(items) => items.iter().any(|item| item.matches(input)),
            Self::Not(inner) => !inner.matches(input),
            Self::Always(value) => *value,
        }
    }
}

impl Extractor {
    fn extract(&self, input: &Input<'_>) -> Option<String> {
        match self {
            Self::Header { name, regex } => input
                .header(name)
                .and_then(|value| apply_regex(regex.as_ref(), value))
                .and_then(bounded),
            Self::Body { path, regex } => input
                .body_scalar(path)
                .and_then(|value| apply_regex(regex.as_ref(), &value))
                .and_then(bounded),
            Self::JsonString { path, pointer } => {
                let text = input.body_scalar(path)?;
                let parsed = serde_json::from_str::<Value>(&text).ok()?;
                let inner = Input {
                    headers: input.headers,
                    body: Some(&parsed),
                };
                inner.body_scalar(pointer).and_then(bounded)
            }
            Self::FirstOf(items) => items.iter().find_map(|item| item.extract(input)),
            Self::Const(value) => bounded(value.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use serde_json::json;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn builtin_profiles_parse_in_documented_order() {
        let detector = HarnessDetector::builtin();
        assert_eq!(
            detector.profile_names(),
            [
                "pi-agent",
                "oh-my-pi",
                "claude-code",
                "codex",
                "opencode",
                "pi",
                "generic"
            ]
        );
        assert!(detector.infer_sub_sessions());
    }

    #[test]
    fn claude_code_current_wire_format() {
        let detector = HarnessDetector::builtin();
        let user_id = json!({
            "device_id": "d3adb33f",
            "account_uuid": "0f0f0f0f-0000-4000-8000-000000000001",
            "session_id": "11111111-1111-4111-8111-111111111111"
        })
        .to_string();
        let body = json!({"model": "claude", "metadata": {"user_id": user_id}});
        // Main thread: session header + body agree, no agent headers.
        let identity = detector.detect(
            &headers(&[
                ("user-agent", "claude-cli/2.1.205 (external, cli)"),
                ("x-app", "cli"),
                (
                    "x-claude-code-session-id",
                    "11111111-1111-4111-8111-111111111111",
                ),
            ]),
            Some(&body),
        );
        assert_eq!(identity.harness, "claude-code");
        assert_eq!(identity.version.as_deref(), Some("2.1.205"));
        assert_eq!(
            identity.session_id.as_deref(),
            Some("11111111-1111-4111-8111-111111111111")
        );
        assert_eq!(identity.sub_session_id, None);
        assert_eq!(identity.parent_session_id, None);
        assert_eq!(identity.sub_sessions, SubSessionPolicy::Declared);

        // Spawned agent: agent id on top of the same session id; nested agent
        // also names its parent agent.
        let identity = detector.detect(
            &headers(&[
                ("user-agent", "claude-cli/2.1.205 (external, cli)"),
                (
                    "x-claude-code-session-id",
                    "11111111-1111-4111-8111-111111111111",
                ),
                ("x-claude-code-agent-id", "agent-b"),
                ("x-claude-code-parent-agent-id", "agent-a"),
            ]),
            Some(&body),
        );
        assert_eq!(identity.sub_session_id.as_deref(), Some("agent-b"));
        assert_eq!(identity.parent_session_id.as_deref(), Some("agent-a"));
    }

    #[test]
    fn claude_code_without_session_header_falls_back_to_metadata() {
        let detector = HarnessDetector::builtin();
        let json_user_id =
            json!({"device_id": "x", "session_id": "22222222-2222-4222-8222-222222222222"})
                .to_string();
        let identity = detector.detect(
            &headers(&[("user-agent", "claude-cli/2.0.1 (external, cli)")]),
            Some(&json!({"metadata": {"user_id": json_user_id}})),
        );
        assert_eq!(identity.harness, "claude-code");
        assert_eq!(
            identity.session_id.as_deref(),
            Some("22222222-2222-4222-8222-222222222222")
        );

        // Legacy underscore format, and no User-Agent at all: the body alone matches.
        let legacy = format!(
            "user_{}_account_{}_session_{}",
            "a".repeat(64),
            "33333333-3333-4333-8333-333333333333",
            "44444444-4444-4444-8444-444444444444"
        );
        let identity = detector.detect(
            &HeaderMap::new(),
            Some(&json!({"metadata": {"user_id": legacy}})),
        );
        assert_eq!(identity.harness, "claude-code");
        assert_eq!(
            identity.session_id.as_deref(),
            Some("44444444-4444-4444-8444-444444444444")
        );
    }

    #[test]
    fn opencode_is_recognised_by_its_user_agent() {
        // Seen live: `opencode/1.2.15 ai-sdk/provider-utils/3.0.20 runtime/bun/1.3.10`,
        // no session headers, no prompt_cache_key.
        let detector = HarnessDetector::builtin();
        let identity = detector.detect(
            &headers(&[(
                "user-agent",
                "opencode/1.2.15 ai-sdk/provider-utils/3.0.20 runtime/bun/1.3.10",
            )]),
            None,
        );
        assert_eq!(identity.harness, "opencode");
        assert_eq!(identity.version.as_deref(), Some("1.2.15"));
        assert_eq!(identity.session_id, None);
        assert_eq!(identity.sub_sessions, SubSessionPolicy::Infer);
    }

    #[test]
    fn codex_exec_user_agent_carries_the_version() {
        // `codex exec` (0.154.0) identifies as `codex_exec/<version>`, not
        // `codex_cli_rs/…`; seen live through the gateway.
        let detector = HarnessDetector::builtin();
        let identity = detector.detect(
            &headers(&[
                (
                    "user-agent",
                    "codex_exec/0.154.0 (Debian 13.0.0; x86_64) unknown",
                ),
                ("originator", "codex_exec"),
                ("session-id", "s-9"),
            ]),
            None,
        );
        assert_eq!(identity.harness, "codex");
        assert_eq!(identity.version.as_deref(), Some("0.154.0"));
        assert_eq!(identity.session_id.as_deref(), Some("s-9"));
    }

    #[test]
    fn codex_root_thread_and_spawned_agent() {
        let detector = HarnessDetector::builtin();
        let root = headers(&[
            (
                "user-agent",
                "codex_cli_rs/0.104.0 (Ubuntu 24.04; x86_64) WezTerm",
            ),
            ("originator", "codex_cli_rs"),
            ("session-id", "s-1"),
            ("thread-id", "s-1"),
            ("x-client-request-id", "s-1"),
        ]);
        let identity = detector.detect(&root, None);
        assert_eq!(identity.harness, "codex");
        assert_eq!(identity.version.as_deref(), Some("0.104.0"));
        assert_eq!(identity.session_id.as_deref(), Some("s-1"));
        assert_eq!(identity.sub_session_id, None, "root thread names itself");
        assert_eq!(identity.session_kind, None);

        let spawned = headers(&[
            ("originator", "codex_cli_rs"),
            ("session-id", "s-1"),
            ("thread-id", "t-2"),
            ("x-codex-parent-thread-id", "s-1"),
            ("x-openai-subagent", "collab_spawn"),
        ]);
        let identity = detector.detect(&spawned, None);
        assert_eq!(identity.session_id.as_deref(), Some("s-1"));
        assert_eq!(identity.sub_session_id.as_deref(), Some("t-2"));
        assert_eq!(
            identity.parent_session_id, None,
            "a parent equal to the session collapses to the session"
        );
        assert_eq!(identity.session_kind.as_deref(), Some("collab_spawn"));

        // Body-only fallback (client_metadata) when headers are stripped.
        let identity = detector.detect(
            &headers(&[("originator", "codex_exec")]),
            Some(
                &json!({"client_metadata": {"session_id": "s-9", "thread_id": "t-9",
                "x-codex-parent-thread-id": "t-8"}}),
            ),
        );
        assert_eq!(identity.session_id.as_deref(), Some("s-9"));
        assert_eq!(identity.sub_session_id.as_deref(), Some("t-9"));
        assert_eq!(identity.parent_session_id.as_deref(), Some("t-8"));
    }

    #[test]
    fn pi_agent_convention_wins_over_everything() {
        let detector = HarnessDetector::builtin();
        let identity = detector.detect(
            &headers(&[
                ("user-agent", "OpenAI/JS 6.26.0"),
                ("x-llm-harness", "pi-agent/1.0.0"),
                ("x-llm-session-id", "0192-child"),
                ("x-llm-parent-session-id", "0192-parent"),
                ("x-llm-session-kind", "subagent"),
                ("x-session-affinity", "0192-child"),
            ]),
            None,
        );
        assert_eq!(identity.harness, "pi-agent");
        assert_eq!(identity.version.as_deref(), Some("1.0.0"));
        assert_eq!(identity.session_id.as_deref(), Some("0192-child"));
        assert_eq!(identity.sub_session_id, None, "session is its own node");
        assert_eq!(identity.parent_session_id.as_deref(), Some("0192-parent"));
        assert_eq!(identity.session_kind.as_deref(), Some("subagent"));
        assert_eq!(identity.sub_sessions, SubSessionPolicy::Declared);
    }

    #[test]
    fn pi_and_oh_my_pi_are_told_apart_by_user_agent() {
        let detector = HarnessDetector::builtin();
        let identity = detector.detect(
            &headers(&[
                ("user-agent", "OpenAI/JS 6.26.0"),
                ("x-session-affinity", "0192-pi"),
                ("x-client-request-id", "0192-pi"),
                ("session_id", "0192-pi"),
            ]),
            None,
        );
        assert_eq!(identity.harness, "pi");
        assert_eq!(identity.session_id.as_deref(), Some("0192-pi"));
        assert_eq!(identity.sub_sessions, SubSessionPolicy::Infer);

        let identity = detector.detect(
            &headers(&[
                ("user-agent", "pi (linux 6.12; x64)"),
                ("x-client-request-id", "0192-x"),
            ]),
            None,
        );
        assert_eq!(identity.harness, "pi");
        assert_eq!(identity.session_id.as_deref(), Some("0192-x"));

        let identity = detector.detect(
            &headers(&[
                ("user-agent", "omp/0.9.1"),
                ("x-claude-code-session-id", "omp-s"),
            ]),
            None,
        );
        assert_eq!(identity.harness, "oh-my-pi");
        assert_eq!(identity.version.as_deref(), Some("0.9.1"));
        assert_eq!(identity.session_id.as_deref(), Some("omp-s"));

        let identity = detector.detect(
            &headers(&[("user-agent", "omp/0.9.1")]),
            Some(&json!({"prompt_cache_key": "omp-cache"})),
        );
        assert_eq!(identity.session_id.as_deref(), Some("omp-cache"));
    }

    #[test]
    fn generic_profile_uses_common_headers_and_the_conversation_header() {
        let detector =
            HarnessDetector::from_config(&SessionsBootstrap::default(), "X-Conv-Id").unwrap();
        let identity = detector.detect(
            &headers(&[("user-agent", "curl/8"), ("x-conv-id", "conv-1")]),
            Some(&json!({"user": "body-user"})),
        );
        assert_eq!(identity.harness, "generic");
        assert_eq!(identity.session_id.as_deref(), Some("conv-1"));
        let identity = detector.detect(&HeaderMap::new(), Some(&json!({"user": "body-user"})));
        assert_eq!(identity.session_id.as_deref(), Some("body-user"));
        let identity = detector.detect(&HeaderMap::new(), None);
        assert_eq!(identity.harness, "generic");
        assert_eq!(identity.session_id, None);
        // The open convention is honored without a dedicated profile.
        let identity = detector.detect(
            &headers(&[
                ("x-llm-session-id", "s"),
                ("x-llm-parent-session-id", "p"),
                ("x-llm-session-kind", "oracle"),
            ]),
            None,
        );
        assert_eq!(identity.session_id.as_deref(), Some("s"));
        assert_eq!(identity.parent_session_id.as_deref(), Some("p"));
        assert_eq!(identity.session_kind.as_deref(), Some("oracle"));
    }

    fn profile_yaml(yaml: &str) -> SessionsBootstrap {
        serde_yaml::from_str(yaml).expect("sessions yaml")
    }

    #[test]
    fn operator_profiles_extend_override_replace_and_disable() {
        let config = profile_yaml(
            r#"
harnesses:
  - name: my-tool
    match: { header: { name: user-agent, regex: '^my-tool/' } }
    version: { header: { name: user-agent, regex: '^my-tool/(\S+)' } }
    session_id: { first_of: [ { header: x-my-session }, { const: fallback } ] }
    sub_sessions: none
  - name: codex
    match: { header: { name: originator, regex: '^codex' } }
    session_id: { header: x-custom-codex-session }
    sub_sessions: infer
"#,
        );
        let detector = HarnessDetector::from_config(&config, "x-conversation-id").unwrap();
        assert_eq!(
            detector.profile_names(),
            [
                "my-tool",
                "pi-agent",
                "oh-my-pi",
                "claude-code",
                "codex",
                "opencode",
                "pi",
                "generic"
            ]
        );
        let identity = detector.detect(&headers(&[("user-agent", "my-tool/3")]), None);
        assert_eq!(identity.harness, "my-tool");
        assert_eq!(identity.version.as_deref(), Some("3"));
        assert_eq!(identity.session_id.as_deref(), Some("fallback"));
        assert_eq!(identity.sub_sessions, SubSessionPolicy::None);
        // The codex override replaced the built-in in place.
        let identity = detector.detect(
            &headers(&[
                ("originator", "codex_cli_rs"),
                ("x-custom-codex-session", "c"),
            ]),
            None,
        );
        assert_eq!(identity.harness, "codex");
        assert_eq!(identity.session_id.as_deref(), Some("c"));
        assert_eq!(identity.sub_sessions, SubSessionPolicy::Infer);

        let mut replaced = config.clone();
        replaced.builtin = BuiltinPolicy::Replace;
        let detector = HarnessDetector::from_config(&replaced, "x-conversation-id").unwrap();
        assert_eq!(detector.profile_names(), ["my-tool", "codex"]);
        let identity = detector.detect(&headers(&[("user-agent", "claude-cli/2")]), None);
        assert_eq!(identity.harness, "unknown");
        assert_eq!(identity.session_id, None);

        let disabled = profile_yaml("builtin: disable\ninfer_sub_sessions: false\n");
        let detector = HarnessDetector::from_config(&disabled, "x-conversation-id").unwrap();
        assert!(detector.profile_names().is_empty());
        assert!(!detector.infer_sub_sessions());

        let off = profile_yaml(
            "harnesses:\n  - name: claude-code\n    enabled: false\n    match: { always: true }\n",
        );
        let detector = HarnessDetector::from_config(&off, "x-conversation-id").unwrap();
        assert!(!detector.profile_names().contains(&"claude-code"));
    }

    #[test]
    fn config_errors_are_reported_with_the_profile_name() {
        let bad = profile_yaml(
            "harnesses:\n  - name: broken\n    match: { header: { name: ua, regex: '(' } }\n",
        );
        let error = HarnessDetector::from_config(&bad, "x").unwrap_err();
        assert!(error.0.contains("harness profile 'broken'"), "{error}");
        let duplicate = profile_yaml(
            "builtin: replace\nharnesses:\n  - name: a\n    match: { always: true }\n  - name: a\n    match: { always: true }\n",
        );
        let error = HarnessDetector::from_config(&duplicate, "x").unwrap_err();
        assert!(error.0.contains("duplicate profile 'a'"));
        let empty = profile_yaml("builtin: replace\nharnesses:\n  - name: e\n    match: {}\n");
        let error = HarnessDetector::from_config(&empty, "x").unwrap_err();
        assert!(error.0.contains("exactly one of"), "{error}");
        let two = profile_yaml(
            "builtin: replace\nharnesses:\n  - name: t\n    match: { always: true }\n    session_id: { header: a, const: b }\n",
        );
        let error = HarnessDetector::from_config(&two, "x").unwrap_err();
        assert!(error.0.contains("several kinds"), "{error}");
    }

    #[test]
    fn values_are_trimmed_bounded_and_combinators_compose() {
        let config = profile_yaml(
            r#"
builtin: replace
harnesses:
  - name: t
    match:
      all:
        - any: [ { header: a }, { header: b } ]
        - not: { body: { path: /skip } }
    session_id: { header: a }
"#,
        );
        let detector = HarnessDetector::from_config(&config, "x").unwrap();
        let long = "z".repeat(600);
        let identity = detector.detect(&headers(&[("a", &format!("  {long}  "))]), None);
        assert_eq!(identity.harness, "t");
        assert_eq!(
            identity.session_id.as_deref().map(str::len),
            Some(VALUE_CAP)
        );
        let identity = detector.detect(&headers(&[("b", "1")]), Some(&json!({"skip": true})));
        assert_eq!(identity.harness, "unknown");
        let identity = detector.detect(&headers(&[("b", "1")]), None);
        assert_eq!(identity.harness, "t");
        assert_eq!(identity.session_id, None);
    }

    #[test]
    fn sessions_bootstrap_default_round_trips_and_is_default() {
        let config = SessionsBootstrap::default();
        assert!(SessionsBootstrap::is_default(&config));
        let yaml = serde_yaml::to_string(&config).unwrap();
        let parsed: SessionsBootstrap = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed, config);
    }
}
