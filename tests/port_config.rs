//! Ported surface: config-loading (gap G7, claude-relay `test_config.py`).
//!
//! Routing is now expressed through `upstreams` (named endpoints) plus
//! `model_profiles` (request model -> upstream, possibly a glob key), so the
//! config surface these tests cover is:
//!
//! - The removed legacy top-level knobs (`upstream_base_url`, `model_routes`,
//!   the vision knobs, ...) are HARD startup errors naming their replacement.
//! - `.toml` config loads with identical semantics to the equivalent YAML
//!   (`PersistedConfig` round-trips through both deserializers).
//! - `model_profiles` preserve declaration order and round-trip as a map.
//! - `template_family` (G2) still resolves through the profile chain — a
//!   regression guard, since this touches the same config-resolution code.
//!
//! These are the PURE config-resolution tests. The gateway-driving HTTP routing
//! tests - which mount wiremock upstreams and assert which one received the
//! request - live in the sibling `tests/port_config_routing.rs`.

mod common;

use common::config_from_yaml;
use llmconduit::config::Config;
use llmconduit::config::OrderedModelProfiles;
use llmconduit::config::PersistedConfig;
use llmconduit::config::PersistedModelProfile;
use llmconduit::config::compile_model_glob;
use llmconduit::config::load_persisted_config;
use llmconduit::config::write_persisted_config;

// ---------------------------------------------------------------------------
// Legacy top-level knobs are hard startup errors with a migration hint
// ---------------------------------------------------------------------------

/// Every removed top-level knob is a startup error naming its replacement and
/// pointing at the README "Migrating" section, so an upgraded config fails loud
/// instead of silently ignoring a key that no longer does anything.
#[test]
fn removed_global_knobs_fail_startup_with_migration_hint() {
    for (yaml, key) in [
        ("upstream_base_url: \"http://h/v1\"", "upstream_base_url"),
        ("upstream_api_key: \"k\"", "upstream_api_key"),
        ("upstream_model: \"m\"", "upstream_model"),
        ("fallback_upstreams: []", "fallback_upstreams"),
        ("model_routes: {}", "model_routes"),
        ("image_agent_enabled: true", "image_agent_enabled"),
        ("vision_url: \"http://v/v1\"", "vision_url"),
        ("vision_model: \"m\"", "vision_model"),
        (
            "unsupported_image_policy: reject",
            "unsupported_image_policy",
        ),
    ] {
        let full = format!(
            "{yaml}\nupstreams: [{{ name: l, url: \"http://h/v1\" }}]\nmodel_profiles:\n  \"*\": {{ upstream: l }}\n"
        );
        let err = Config::from_persisted(&serde_yaml::from_str(&full).unwrap()).unwrap_err();
        assert!(
            err.contains(key) && err.contains("Migrating"),
            "{key}: {err}"
        );
    }
}

/// A config that declares `upstreams:` but no `model_profiles:` at all cannot
/// route anything, so it fails startup naming the catch-all fix instead of
/// leaving every request a silent 404.
#[test]
fn empty_model_profiles_fail_startup_with_migration_hint() {
    let yaml = "upstreams: [{ name: l, url: \"http://h/v1\" }]\n";
    let err = Config::from_persisted(&serde_yaml::from_str(yaml).unwrap()).unwrap_err();
    assert!(
        err.contains("model_profiles") && err.contains("Migrating"),
        "{err}"
    );
}

/// A missing config file resolves to the minimal profile config: one `default`
/// upstream plus a `"*"` catch-all profile, preserving out-of-the-box behavior.
#[test]
fn default_persisted_config_is_the_minimal_profile_config() {
    let config = Config::from_persisted(&PersistedConfig::default()).unwrap();
    assert_eq!(config.upstreams[0].name, "default");
    assert!(config.model_profile("*").is_some());
}

// ---------------------------------------------------------------------------
// Unit tests: TOML loading + template_family guard
// ---------------------------------------------------------------------------

/// A `.toml` config deserializes into the SAME `PersistedConfig` as the
/// equivalent YAML (claude-relay `test_load_proxy_config_reads_toml_routes`,
/// adapted to llmconduit's schema).
#[test]
fn toml_config_loads_identically_to_yaml() {
    let yaml = r#"
bind_addr: "127.0.0.1:4010"
request_timeout_secs: 120
flatten_content: false
upstreams:
  - name: sonnet
    url: "http://sonnet:8000/v1"
model_profiles:
  "claude-3-5-sonnet":
    upstream: sonnet
    upstream_model: "Qwen3.5"
"#;
    let toml = r#"
bind_addr = "127.0.0.1:4010"
request_timeout_secs = 120
flatten_content = false

[[upstreams]]
name = "sonnet"
url = "http://sonnet:8000/v1"

[model_profiles."claude-3-5-sonnet"]
upstream = "sonnet"
upstream_model = "Qwen3.5"
"#;

    let from_yaml: PersistedConfig = serde_yaml::from_str(yaml).expect("yaml");
    let from_toml: PersistedConfig = toml::from_str(toml).expect("toml");
    assert_eq!(
        from_yaml, from_toml,
        "TOML and YAML must deserialize to an identical PersistedConfig"
    );
}

/// The `.toml` extension is detected by `load_persisted_config` (byte-identical
/// to loading the equivalent YAML through the file path).
#[test]
fn toml_file_extension_is_detected_on_load() {
    let toml = r#"
[[upstreams]]
name = "opus"
url = "http://opus:8000/v1"

[model_profiles."claude-opus-*"]
upstream = "opus"
upstream_model = "Kimi-K2.6"
"#;
    let path = std::env::temp_dir().join(format!(
        "llmconduit-g7-{}.toml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(&path, toml).expect("write toml");
    let config = Config::from_env_and_file(Some(&path)).expect("load toml config");
    let _ = std::fs::remove_file(&path);

    assert_eq!(config.upstreams.len(), 1);
    assert_eq!(config.upstreams[0].name, "opus");
    assert_eq!(config.upstreams[0].url.as_str(), "http://opus:8000/v1");
    let profile = config.model_profile("claude-opus-*").expect("profile");
    assert_eq!(profile.upstream, "opus");
    assert_eq!(profile.upstream_model.as_deref(), Some("Kimi-K2.6"));
}

/// Regression guard for G2: `template_family` still resolves through the profile
/// chain after the G7 config changes. Exercised through the PUBLIC upstream-leaf
/// seam (`finalize_request_for_backend`) — the path production runs — since the
/// per-request resolution lives at the leaf, not on `Config`. The per-model
/// `template_family` policy wins over the global override; an unmatched model
/// falls back to the global value. The profiled model carries a NON-family name
/// so the per-model `kimi` override (not name sniffing) is what drives injection.
#[test]
fn template_family_still_resolves_through_profile_chain() {
    let config = config_from_yaml(
        r#"
template_family: deepseek
upstreams:
  - name: backend
    url: "http://h/v1"
model_profiles:
  "Router-X":
    upstream: backend
    template_family: kimi
"#,
    );
    // Per-model `kimi` override -> Kimi `chat_template_kwargs` injected on the
    // wire for `Router-X`, despite its non-Kimi name.
    assert_eq!(
        leaf_family_chat_template_kwargs(&config, "Router-X"),
        serde_json::json!({"thinking": true, "preserve_thinking": true})
    );
    // No per-model policy -> global `deepseek` override applies (`enable_thinking`).
    assert_eq!(
        leaf_family_chat_template_kwargs(&config, "plain-model"),
        serde_json::json!({"enable_thinking": true})
    );
}

/// Resolve the family `chat_template_kwargs` the upstream LEAF injects for
/// `backend_model`, via the PUBLIC seam: build the SAME finalization policies
/// production builds (`BackendFinalizationPolicies::from_config`) and apply them
/// through `finalize_request_for_backend` to an empty wire request. Returns the
/// injected `chat_template_kwargs` object.
fn leaf_family_chat_template_kwargs(config: &Config, backend_model: &str) -> serde_json::Value {
    use llmconduit::models::chat::ChatCompletionRequest;
    use llmconduit::upstream::BackendChatRequest;
    use llmconduit::upstream::BackendFinalizationPolicies;
    use llmconduit::upstream::finalize_request_for_backend;

    let request = ChatCompletionRequest {
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
        extra_body: std::collections::BTreeMap::new(),
    };
    let policies = BackendFinalizationPolicies::from_config(config);
    let mut backend = BackendChatRequest::new(request, None, None, None);
    finalize_request_for_backend(&mut backend, &policies);
    backend
        .request
        .extra_body
        .get("chat_template_kwargs")
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

// ---------------------------------------------------------------------------
// `.toml` is read-only: `configure` writes YAML, `.toml` is never written
// ---------------------------------------------------------------------------

/// Writing a config to a `.toml` path is rejected cleanly (not a panic) and
/// never produces a file - `configure` writes YAML, `.toml` is read-only.
#[test]
fn writing_config_to_toml_path_errors_and_creates_no_file() {
    let path = std::env::temp_dir().join(format!(
        "llmconduit-g7-write-{}.toml",
        uuid::Uuid::new_v4().simple()
    ));
    let config = PersistedConfig::default();
    let result = write_persisted_config(&path, &config);

    assert!(
        result.is_err(),
        "writing to a .toml path must be a clean Err"
    );
    let message = result.unwrap_err();
    assert!(
        message.contains("read-only") || message.contains(".toml"),
        "error must explain .toml is read-only: {message}"
    );
    assert!(
        !path.exists(),
        "a rejected .toml write must not leave a file behind"
    );
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// `upstreams:` entries are pure named endpoints
// ---------------------------------------------------------------------------

#[test]
fn upstream_entries_parse_short_and_aliased_names() {
    let yaml = r#"
upstreams:
  - name: local
    url: "http://127.0.0.1:8000/v1"
  - name: aliased
    upstream_base_url: "http://127.0.0.1:8001/v1"
    upstream_api_key: "k"
model_profiles:
  "*": { upstream: local }
"#;
    let persisted: PersistedConfig = serde_yaml::from_str(yaml).expect("yaml");
    let config = Config::from_persisted(&persisted).expect("config");
    assert_eq!(config.upstreams[0].name, "local");
    assert_eq!(config.upstreams[1].url.as_str(), "http://127.0.0.1:8001/v1");
    assert_eq!(config.upstreams[1].api_key.as_deref(), Some("k"));
}

#[test]
fn upstream_entry_rejects_removed_keys_and_duplicates() {
    for (yaml_fragment, needle) in [
        ("upstream_model: \"m\"", "upstream_model"),
        ("fallback_upstreams: []", "fallback_upstreams"),
    ] {
        let yaml =
            format!("upstreams:\n  - name: local\n    url: \"http://h/v1\"\n    {yaml_fragment}\n");
        let persisted: PersistedConfig = serde_yaml::from_str(&yaml).expect("yaml");
        let err = Config::from_persisted(&persisted).expect_err("must reject");
        assert!(err.contains(needle) && err.contains("Migrating"), "{err}");
    }
    let dup = "upstreams:\n  - name: local\n    url: \"http://h/v1\"\n  - name: LOCAL\n    url: \"http://h2/v1\"\n";
    let persisted: PersistedConfig = serde_yaml::from_str(dup).expect("yaml");
    assert!(
        Config::from_persisted(&persisted)
            .expect_err("dup")
            .contains("duplicate upstream name")
    );
    let blank = "upstreams:\n  - name: \"   \"\n    url: \"http://h/v1\"\n";
    let persisted: PersistedConfig = serde_yaml::from_str(blank).expect("yaml");
    assert!(
        Config::from_persisted(&persisted)
            .expect_err("blank name")
            .contains("must not be blank")
    );
}

#[test]
fn profile_parses_upstream_fallbacks_and_image_analysis() {
    let yaml = r#"
upstreams:
  - { name: local, url: "http://h/v1" }
  - { name: or, url: "http://o/v1" }
  - { name: vl, url: "http://v/v1" }
model_profiles:
  GLM-5.2:
    upstream: local
    fallbacks:
      - { upstream: or, model: "z-ai/glm-5.2" }
      - vl
    image_analysis: { model: Qwen3-VL }
  Qwen3-VL:
    upstream: vl
"#;
    // The runtime `ModelProfile` carries a required `upstream: String` plus its
    // resolved fallbacks and `image_analysis`; route resolution is covered by
    // `resolve_route` in `tests/port_config_routing.rs`.
    let config = Config::from_persisted(&serde_yaml::from_str(yaml).unwrap()).unwrap();
    let profile = config.model_profile("GLM-5.2").expect("profile");
    assert_eq!(profile.upstream, "local");
    assert_eq!(profile.fallbacks[0].model.as_deref(), Some("z-ai/glm-5.2"));
    assert_eq!(profile.fallbacks[1].upstream, "vl");
    assert!(profile.fallbacks[1].model.is_none());
    assert_eq!(profile.image_analysis.as_ref().unwrap().model, "Qwen3-VL");
    // The shorthand-kwargs sweep must not let the new typed keys leak in.
    assert!(profile.upstream_chat_kwargs.is_empty());
}

#[test]
fn profile_native_vision_is_a_startup_error() {
    let yaml = "upstreams: [{ name: l, url: \"http://h/v1\" }]\nmodel_profiles:\n  M: { upstream: l, native_vision: true }\n";
    let err = Config::from_persisted(&serde_yaml::from_str(yaml).unwrap()).unwrap_err();
    assert!(
        err.contains("native_vision") && err.contains("image_analysis"),
        "{err}"
    );
}

#[test]
fn extends_template_supplies_upstream_and_fallbacks_whole_value() {
    let yaml = r#"
upstreams: [{ name: l, url: "http://h/v1" }, { name: o, url: "http://o/v1" }]
model_profile_templates:
  routed: { upstream: l, fallbacks: [o] }
model_profiles:
  A: { extends: [routed] }
  B: { extends: [routed], fallbacks: [] }
"#;
    let config = Config::from_persisted(&serde_yaml::from_str(yaml).unwrap()).unwrap();
    assert_eq!(config.model_profile("A").unwrap().fallbacks.len(), 1);
    assert!(config.model_profile("B").unwrap().fallbacks.is_empty());
}

// ---------------------------------------------------------------------------
// `OrderedModelProfiles`: declaration order, duplicate-key collapse, round
// trip, and the shared glob compiler. These preserve the persisted shape;
// glob-aware route resolution is covered by `resolve_route`.
// ---------------------------------------------------------------------------

/// Persisted `model_profiles` preserve DECLARATION order, not alphabetical
/// order. `Zeta` sorts AFTER `Alpha` alphabetically, so a `BTreeMap` would
/// reorder them; the ordered structure keeps file order. Mirrors
/// `overlapping_glob_routes_preserve_declaration_order_not_alphabetical`.
#[test]
fn model_profiles_preserve_declaration_order_not_alphabetical() {
    let zeta_first: PersistedConfig = serde_yaml::from_str(
        r#"
model_profiles:
  Zeta:
    upstream: z
  Alpha:
    upstream: a
"#,
    )
    .expect("yaml");
    let names: Vec<&str> = zeta_first
        .model_profiles
        .0
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["Zeta", "Alpha"],
        "declaration order must be preserved (a BTreeMap would sort Alpha first)"
    );

    // Reversed declaration => reversed resolved order.
    let alpha_first: PersistedConfig = serde_yaml::from_str(
        r#"
model_profiles:
  Alpha:
    upstream: a
  Zeta:
    upstream: z
"#,
    )
    .expect("yaml");
    let names: Vec<&str> = alpha_first
        .model_profiles
        .0
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(names, vec!["Alpha", "Zeta"]);
}

/// Duplicate profile keys collapse to last-wins (later value replaces the
/// first, preserving the first position), matching `OrderedModelRoutes` and
/// claude-relay dict semantics rather than keeping a shadowed first entry.
#[test]
fn duplicate_model_profile_keys_collapse_to_last_wins() {
    let persisted: PersistedConfig = serde_yaml::from_str(
        r#"
model_profiles:
  GLM-5.2:
    upstream: first
  GLM-5.2:
    upstream: second
"#,
    )
    .expect("yaml");

    assert_eq!(
        persisted.model_profiles.0.len(),
        1,
        "duplicate keys must collapse to a single profile"
    );
    let (name, profile) = &persisted.model_profiles.0[0];
    assert_eq!(name, "GLM-5.2");
    assert_eq!(
        profile.upstream.as_deref(),
        Some("second"),
        "the later duplicate value must win"
    );
}

/// `model_profiles` written by `write_persisted_config` reload through
/// `load_persisted_config` as a MAP, preserving values AND declaration order.
/// Mirrors `model_routes_round_trip_through_write_and_reload_as_map`.
#[test]
fn model_profiles_round_trip_through_write_and_reload_as_map() {
    let config = PersistedConfig {
        // Declared in non-alphabetical order to also lock order across the trip.
        model_profiles: OrderedModelProfiles(vec![
            (
                "Zeta-Model".to_string(),
                PersistedModelProfile {
                    upstream: Some("local".to_string()),
                    ..Default::default()
                },
            ),
            (
                "Alpha-Model".to_string(),
                PersistedModelProfile {
                    upstream: Some("vl".to_string()),
                    ..Default::default()
                },
            ),
        ]),
        ..PersistedConfig::default()
    };

    // The serialized form must be a MAP (`name: profile`), not a sequence.
    let yaml = serde_yaml::to_string(&config).expect("serialize config");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("parse yaml");
    assert!(
        parsed["model_profiles"].is_mapping(),
        "model_profiles must serialize as a YAML map, not a sequence:\n{yaml}"
    );

    let path = std::env::temp_dir().join(format!(
        "llmconduit-t3-rt-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    write_persisted_config(&path, &config).expect("write config");
    let reloaded = load_persisted_config(&path).expect("reload config");
    let _ = std::fs::remove_file(&path);

    assert_eq!(
        reloaded, config,
        "profiles must round-trip through write + reload unchanged"
    );
    let names: Vec<&str> = reloaded
        .model_profiles
        .0
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["Zeta-Model", "Alpha-Model"],
        "declaration order must survive the round trip"
    );
}

/// `compile_model_glob` compiles a case-insensitive matcher for a key holding
/// any of `*?[`.
#[test]
fn compile_model_glob_matches_case_insensitively() {
    let regex = compile_model_glob("claude-*")
        .expect("valid glob")
        .expect("glob pattern must compile a matcher");
    assert!(regex.is_match("Claude-Opus-4"));
}

/// A literal key (no glob metacharacters) compiles to `None`, matched by exact
/// comparison instead of a regex.
#[test]
fn compile_model_glob_returns_none_for_literal_key() {
    assert!(
        compile_model_glob("GLM-5.2")
            .expect("literal key")
            .is_none(),
        "a literal key must not compile a glob matcher"
    );
}
