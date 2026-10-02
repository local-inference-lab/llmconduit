use crate::config::PersistedConfig;
use crate::config::default_config_path;
use crate::config::load_persisted_config;
use crate::config::write_persisted_config;
use crate::control_plane::ControlPlaneConfig;
use crate::control_plane::ControlPlaneSection;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use dialoguer::Confirm;
use dialoguer::Input;
use dialoguer::Password;
use dialoguer::theme::ColorfulTheme;
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "llmconduit",
    version = crate::VERSION,
    about = "LLM API gateway for translating, normalizing, and extending model traffic"
)]
pub struct Cli {
    /// Enable the embedded request debug UI at /debug.
    #[arg(long, global = true, default_value_t = false)]
    pub with_debug_ui: bool,
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Start the gateway server.
    Start {
        /// Path to the config file. Defaults to ~/.config/llmconduit/config.yaml
        #[arg(long)]
        config: Option<PathBuf>,
        /// Dump raw model delta text to the terminal while the gateway is running.
        #[arg(long, default_value_t = false)]
        raw: bool,
        /// Ad-hoc model route `NAME=URL[,UPSTREAM_MODEL]`, repeatable. NAME may be
        /// a glob (e.g. `claude-opus-*`). Merged after the config file and env
        /// (CLI wins); a malformed spec is a clean startup error.
        #[arg(long = "model-route", value_name = "NAME=URL[,UPSTREAM_MODEL]")]
        model_route: Vec<String>,
    },
    /// Run the interactive configuration flow and write a config file.
    Configure {
        /// Path to the config file. Defaults to ~/.config/llmconduit/config.yaml
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Atomically migrate legacy YAML control-plane layout and plaintext keys
    /// without running the interactive editor.
    MigrateConfig {
        /// Path to the YAML config file. Defaults to ~/.config/llmconduit/config.yaml
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Manage dashboard user accounts (needs a SQL store in the config).
    User {
        /// Path to the config file. Defaults to ~/.config/llmconduit/config.yaml
        #[arg(long)]
        config: Option<PathBuf>,
        #[command(subcommand)]
        action: UserCommand,
    },
    /// Diff consecutive upstream request log entries and highlight unstable prefixes.
    AnalyzeLog {
        /// Path to the config file. Defaults to ~/.config/llmconduit/config.yaml
        #[arg(long)]
        config: Option<PathBuf>,
        /// Path to the JSONL request log. Defaults to upstream_request_log_path from config.
        #[arg(long)]
        path: Option<PathBuf>,
        /// Maximum number of consecutive pairs to report.
        #[arg(long, default_value_t = 10)]
        pairs: usize,
    },
    /// Run a mesh worker sidecar.
    Worker {
        /// Path to the worker config file. Defaults to ~/.config/llmconduit/config.yaml
        #[arg(long)]
        config: Option<PathBuf>,
        /// DEPRECATED: one-shot enrollment key. Prefer LLMCONDUIT_MESH_JOIN_KEY, --join-key-stdin, or --join-key-file so the token is not exposed in process listings.
        #[arg(long = "join-key", conflicts_with_all = ["join_key_stdin", "join_key_file"])]
        join_key: Option<String>,
        /// Read the one-shot enrollment key from stdin.
        #[arg(long = "join-key-stdin", default_value_t = false, conflicts_with_all = ["join_key", "join_key_file"])]
        join_key_stdin: bool,
        /// Read the one-shot enrollment key from a file.
        #[arg(long = "join-key-file", conflicts_with_all = ["join_key", "join_key_stdin"])]
        join_key_file: Option<PathBuf>,
    },
    /// Manage mesh controller state.
    Mesh {
        /// Path to the controller config file. Defaults to ~/.config/llmconduit/config.yaml
        #[arg(long)]
        config: Option<PathBuf>,
        #[command(subcommand)]
        command: MeshCommands,
    },
    /// Import model pricing into the authorization store.
    Pricing {
        /// Path to the config file. Defaults to ~/.config/llmconduit/config.yaml
        #[arg(long)]
        config: Option<PathBuf>,
        #[command(subcommand)]
        command: PricingCommands,
    },
}

#[derive(Debug, Subcommand)]
pub enum PricingCommands {
    /// Fetch official endpoint pricing and persist normalized snapshots.
    Sync {
        source: PricingSource,
        /// OpenRouter model id to import, repeatable (for example openai/gpt-4.1).
        #[arg(long = "model", required = true)]
        models: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PricingSource {
    Openrouter,
}

#[derive(Debug, Subcommand)]
pub enum MeshCommands {
    /// Show controller mesh identity and state information.
    Info,
    /// Manage enrollment join keys.
    JoinKey {
        #[command(subcommand)]
        command: JoinKeyCommands,
    },
    /// Manage enrolled worker nodes.
    Node {
        #[command(subcommand)]
        command: NodeCommands,
    },
}

#[derive(Debug, Subcommand)]
pub enum JoinKeyCommands {
    /// Create a join token. The plaintext token is printed exactly once.
    Create {
        /// Optional operator-facing label.
        #[arg(long)]
        label: Option<String>,
        /// Maximum number of successful enrollments allowed.
        #[arg(long = "max-uses")]
        max_uses: Option<i64>,
        /// Relative expiry such as 30m, 12h, or 7d.
        #[arg(long = "expires-in")]
        expires_in: Option<String>,
    },
    /// List join keys without exposing plaintext tokens.
    List,
    /// Disable a join key by id.
    Revoke {
        /// Join-key id, for example jk_<uuid>.
        key_id: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum NodeCommands {
    /// List enrolled worker nodes.
    List,
    /// Disable a worker node by endpoint id.
    Revoke {
        /// Worker Iroh endpoint id.
        endpoint_id: String,
    },
    /// Re-enable a worker node by endpoint id.
    Enable {
        /// Worker Iroh endpoint id.
        endpoint_id: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum UserCommand {
    /// Create a user. The password is read from `--password-env` (an
    /// environment variable name) or, when absent, from stdin.
    Create {
        #[arg(long)]
        username: String,
        /// Grant the administrator role.
        #[arg(long, default_value_t = false)]
        admin: bool,
        /// Name of an environment variable holding the password.
        #[arg(long)]
        password_env: Option<String>,
    },
    /// List users.
    List,
    /// Reset a user's password (same password sources as `create`).
    SetPassword {
        #[arg(long)]
        username: String,
        #[arg(long)]
        password_env: Option<String>,
    },
    /// Delete a user and revoke their keys.
    Delete {
        #[arg(long)]
        username: String,
    },
}

pub fn migrate_config_file(path: &std::path::Path) -> Result<bool, String> {
    if crate::config::path_is_toml(path) {
        return Err(
            "TOML configuration is read-only and has no legacy control-plane schema".into(),
        );
    }
    let source = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let mut document = ControlPlaneConfig::from_yaml_str(&source)?;
    let migrated = document.migrated_legacy_root() || document.migrate_plaintext_key_digests()?;
    document.write_to_path(path)?;
    Ok(migrated)
}

pub fn resolve_config_path(path: Option<PathBuf>) -> Result<PathBuf, String> {
    path.map(Ok).unwrap_or_else(default_config_path)
}

pub fn run_configure_flow(path: PathBuf) -> Result<PersistedConfig, String> {
    let mut control_plane_document = if !crate::config::path_is_toml(&path) {
        if path.exists() {
            let source = std::fs::read_to_string(&path)
                .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
            Some(ControlPlaneConfig::from_yaml_str(&source)?)
        } else {
            Some(ControlPlaneConfig::from_gateway(
                PersistedConfig::default(),
                ControlPlaneSection::default(),
            )?)
        }
    } else {
        None
    };
    let existing = match &control_plane_document {
        Some(document) => document.gateway().clone(),
        None => load_persisted_config(&path)?,
    };
    let theme = ColorfulTheme::default();

    println!("Configuring llmconduit");
    println!("Config file: {}", path.display());

    let bind_addr = Input::with_theme(&theme)
        .with_prompt("Bind address")
        .default(existing.bind_addr.clone())
        .interact_text()
        .map_err(|err| format!("failed to read bind address: {err}"))?;
    let upstream_base_url = Input::with_theme(&theme)
        .with_prompt("Upstream chat-completions base URL")
        .default(existing.upstream_base_url.clone())
        .interact_text()
        .map_err(|err| format!("failed to read upstream URL: {err}"))?;
    let upstream_api_key = match existing.upstream_api_key.clone() {
        Some(existing_api_key) => {
            let keep_existing = Confirm::with_theme(&theme)
                .with_prompt("Keep existing upstream API key?")
                .default(true)
                .interact()
                .map_err(|err| format!("failed to confirm upstream API key: {err}"))?;
            if keep_existing {
                Some(existing_api_key)
            } else {
                let value = Password::with_theme(&theme)
                    .with_prompt("Upstream API key (leave blank for local/no auth)")
                    .allow_empty_password(true)
                    .interact()
                    .map_err(|err| format!("failed to read upstream API key: {err}"))?;
                (!value.trim().is_empty()).then_some(value)
            }
        }
        None => {
            let value = Password::with_theme(&theme)
                .with_prompt("Upstream API key (leave blank for local/no auth)")
                .allow_empty_password(true)
                .interact()
                .map_err(|err| format!("failed to read upstream API key: {err}"))?;
            (!value.trim().is_empty()).then_some(value)
        }
    };
    let upstream_model = Input::with_theme(&theme)
        .with_prompt("Upstream model override (leave blank to pass through request model)")
        .allow_empty(true)
        .default(existing.upstream_model.clone().unwrap_or_default())
        .interact_text()
        .map_err(|err| format!("failed to read upstream model override: {err}"))?;
    let upstream_request_log_path = Input::with_theme(&theme)
        .with_prompt("Upstream request JSONL log path (leave blank to disable)")
        .allow_empty(true)
        .default(
            existing
                .upstream_request_log_path
                .clone()
                .unwrap_or_default(),
        )
        .interact_text()
        .map_err(|err| format!("failed to read upstream request log path: {err}"))?;
    let upstream_chat_kwargs = Input::with_theme(&theme)
        .with_prompt("Extra upstream chat kwargs as JSON object (leave blank for none)")
        .allow_empty(true)
        .default(if existing.upstream_chat_kwargs.is_empty() {
            String::new()
        } else {
            serde_json::to_string(&existing.upstream_chat_kwargs)
                .map_err(|err| format!("failed to encode upstream chat kwargs: {err}"))?
        })
        .interact_text()
        .map_err(|err| format!("failed to read upstream chat kwargs: {err}"))?;
    let brave_base_url = Input::with_theme(&theme)
        .with_prompt("Brave Search base URL")
        .default(existing.brave_base_url.clone())
        .interact_text()
        .map_err(|err| format!("failed to read Brave URL: {err}"))?;
    let brave_api_key = Password::with_theme(&theme)
        .with_prompt("Brave Search API key (leave blank to disable provider-side web_search)")
        .allow_empty_password(true)
        .interact()
        .map_err(|err| format!("failed to read Brave API key: {err}"))?;
    let brave_max_results = Input::with_theme(&theme)
        .with_prompt("Brave max results")
        .default(existing.brave_max_results)
        .interact_text()
        .map_err(|err| format!("failed to read Brave max results: {err}"))?;
    let request_timeout_secs = Input::with_theme(&theme)
        .with_prompt("Request timeout (seconds)")
        .default(existing.request_timeout_secs)
        .interact_text()
        .map_err(|err| format!("failed to read timeout: {err}"))?;

    let upstream_chat_kwargs = if upstream_chat_kwargs.trim().is_empty() {
        JsonMap::new()
    } else {
        serde_json::from_str::<JsonMap<String, JsonValue>>(&upstream_chat_kwargs)
            .map_err(|err| format!("invalid upstream chat kwargs JSON: {err}"))?
    };

    let config = PersistedConfig {
        metrics_url: None,
        metrics_source: None,

        bind_addr,
        upstream_base_url,
        upstream_api_key,
        upstream_model: (!upstream_model.trim().is_empty()).then_some(upstream_model),
        system_prompt_prefix: existing.system_prompt_prefix.clone(),
        upstream_request_log_path: (!upstream_request_log_path.trim().is_empty())
            .then_some(upstream_request_log_path),
        // F1: not interactively prompted (an advanced, opt-in knob, like
        // `debug_log_max_age_hours` below) -- just carried through unchanged.
        turn_capture_dir: existing.turn_capture_dir.clone(),
        upstream_chat_kwargs,
        upstreams: existing.upstreams.clone(),
        fallback_upstreams: existing.fallback_upstreams.clone(),
        upstream_failure_cooldown_secs: existing.upstream_failure_cooldown_secs,
        model_profile_templates: existing.model_profile_templates.clone(),
        model_profiles: existing.model_profiles.clone(),
        model_routes: existing.model_routes.clone(),
        template_family: existing.template_family.clone(),
        brave_base_url,
        brave_api_key: (!brave_api_key.trim().is_empty()).then_some(brave_api_key),
        brave_max_results,
        request_timeout_secs,
        connect_timeout_secs: existing.connect_timeout_secs,
        max_web_search_rounds: existing.max_web_search_rounds,
        flatten_content: existing.flatten_content,
        max_replay_entries: existing.max_replay_entries,
        debug_log_max_age_hours: existing.debug_log_max_age_hours,
        min_completion_tokens: existing.min_completion_tokens,
        max_sse_frame_bytes: existing.max_sse_frame_bytes,
        max_request_body_bytes: existing.max_request_body_bytes,
        image_agent_enabled: existing.image_agent_enabled,
        vision_url: existing.vision_url.clone(),
        vision_model: existing.vision_model.clone(),
        image_cache_max_size: existing.image_cache_max_size,
        image_cache_ttl_secs: existing.image_cache_ttl_secs,
        unsupported_image_policy: existing.unsupported_image_policy,
        price_table: existing.price_table.clone(),
        auth: existing.auth.clone(),
        mesh: existing.mesh.clone(),
    };

    let should_write = Confirm::with_theme(&theme)
        .with_prompt(format!("Write configuration to {}?", path.display()))
        .default(true)
        .interact()
        .map_err(|err| format!("failed to confirm config write: {err}"))?;
    if !should_write {
        return Err("configuration cancelled".to_string());
    }

    if let Some(document) = control_plane_document.as_mut() {
        document.replace_gateway(config.clone())?;
        document.write_to_path(&path)?;
    } else {
        write_persisted_config(&path, &config)?;
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrate_config_file_atomically_persists_namespaced_digest() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-legacy-config-{}.yaml",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::write(
            &path,
            "auth:\n  require: true\n  keys:\n    - key: legacy-plaintext\n",
        )
        .expect("legacy config");
        assert!(migrate_config_file(&path).expect("migrate"));
        let migrated = std::fs::read_to_string(&path).expect("rewritten config");
        assert!(migrated.contains("control_plane:"));
        assert!(migrated.contains("sha256:"));
        assert!(!migrated.contains("legacy-plaintext"));
        assert!(!migrate_config_file(&path).expect("idempotent rewrite"));
        std::fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn migrate_config_file_hashes_plaintext_key_in_namespaced_operational_config() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-namespaced-legacy-key-{}.yaml",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::write(
            &path,
            "control_plane:\n  operational:\n    keys:\n      - id: 10000000-0000-4000-8000-000000000001\n        key: plaintext-secret\n",
        )
        .expect("config");

        assert!(migrate_config_file(&path).expect("migrate"));
        let migrated = std::fs::read_to_string(&path).expect("rewritten config");
        assert!(!migrated.contains("plaintext-secret"));
        assert!(migrated.contains(&crate::client_auth::hash_secret("plaintext-secret")));

        let document = ControlPlaneConfig::from_yaml_str(&migrated).expect("parse migrated");
        document
            .control_plane()
            .operational
            .as_ref()
            .expect("operational config")
            .validate_persisted_key_digests()
            .expect("migrated key is accepted by fail-closed startup validation");
        assert!(!migrate_config_file(&path).expect("idempotent rewrite"));
        std::fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn parses_worker_command_with_join_key() {
        let cli = Cli::parse_from([
            "llmconduit",
            "worker",
            "--config",
            "/etc/llmconduit/worker.yaml",
            "--join-key",
            "llmc_join_secret",
        ]);

        assert!(matches!(
            cli.command,
            Some(Commands::Worker {
                config: Some(_),
                join_key: Some(_),
                join_key_stdin: false,
                join_key_file: None,
            })
        ));
    }

    #[test]
    fn parses_worker_command_with_join_key_stdin() {
        let cli = Cli::parse_from([
            "llmconduit",
            "worker",
            "--config",
            "/etc/llmconduit/worker.yaml",
            "--join-key-stdin",
        ]);

        assert!(matches!(
            cli.command,
            Some(Commands::Worker {
                config: Some(_),
                join_key: None,
                join_key_stdin: true,
                join_key_file: None,
            })
        ));
    }

    #[test]
    fn parses_worker_command_with_join_key_file() {
        let cli = Cli::parse_from([
            "llmconduit",
            "worker",
            "--config",
            "/etc/llmconduit/worker.yaml",
            "--join-key-file",
            "/run/secrets/mesh-join-key",
        ]);

        assert!(matches!(
            cli.command,
            Some(Commands::Worker {
                config: Some(_),
                join_key: None,
                join_key_stdin: false,
                join_key_file: Some(_),
            })
        ));
    }

    #[test]
    fn parses_mesh_join_key_create_command() {
        let cli = Cli::parse_from([
            "llmconduit",
            "mesh",
            "--config",
            "/etc/llmconduit/config.yaml",
            "join-key",
            "create",
            "--label",
            "community",
            "--max-uses",
            "20",
            "--expires-in",
            "7d",
        ]);

        assert!(matches!(
            cli.command,
            Some(Commands::Mesh {
                command: MeshCommands::JoinKey {
                    command: JoinKeyCommands::Create {
                        label: Some(_),
                        max_uses: Some(20),
                        expires_in: Some(_),
                    }
                },
                ..
            })
        ));
    }

    #[test]
    fn parses_mesh_node_enable_command() {
        let cli = Cli::parse_from(["llmconduit", "mesh", "node", "enable", "endpoint-id"]);

        assert!(matches!(
            cli.command,
            Some(Commands::Mesh {
                command: MeshCommands::Node {
                    command: NodeCommands::Enable { endpoint_id },
                },
                ..
            }) if endpoint_id == "endpoint-id"
        ));
    }

    #[test]
    fn parses_openrouter_pricing_sync_models() {
        let cli = Cli::parse_from([
            "llmconduit",
            "pricing",
            "sync",
            "openrouter",
            "--model",
            "openai/gpt-4.1",
            "--model",
            "anthropic/claude-sonnet-4",
        ]);

        assert!(matches!(
            cli.command,
            Some(Commands::Pricing {
                command: PricingCommands::Sync {
                    source: PricingSource::Openrouter,
                    models,
                },
                ..
            }) if models == ["openai/gpt-4.1", "anthropic/claude-sonnet-4"]
        ));
    }
}
