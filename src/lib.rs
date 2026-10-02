pub mod accounts;
pub mod accounts_api;
pub mod adapters;
pub mod authz;
pub mod cli;
pub mod client_auth;
pub mod config;
pub mod content_store;
pub mod control_plane;
pub mod control_plane_store;
pub mod dashboard_access;
pub mod dashboard_api;
pub mod dashboard_auth;
pub mod dashboard_fleet;
pub mod dashboard_flow;
pub mod dashboard_mesh;
pub mod dashboard_ui;
pub mod dashboard_ws;
pub mod debug_ui;
pub mod engine;
pub mod error;
pub mod flow_persistence;
pub mod harness;
pub mod http;
pub mod log_rotation;
pub mod managed_providers;
pub mod mesh;
pub mod metrics;
pub mod models;
pub mod monitor;
pub mod openapi;
pub mod openrouter_pricing;
pub mod persistent_history_api;
pub mod provider_metrics;
pub(crate) mod proxy_headers;
pub mod raw;
pub(crate) mod redaction;
pub mod replay;
pub mod request_log;
pub mod search;
pub mod session_hub;
pub mod sessions;
pub(crate) mod sse_guard;
/// Crate-internal, test-only peak-allocation probe (the crate's single
/// `#[global_allocator]`), shared by the `sse_guard` reject-path and
/// `dashboard_flow::capture_body` heap-bound tests.
#[cfg(test)]
pub(crate) mod test_alloc_probe;
pub(crate) mod tool_delta_gate;
pub mod tool_repair;
pub mod turn_capture;
pub mod upstream;
pub mod upstream_metrics;
pub mod usage_accounting;
pub mod vision;
pub mod vision_probe;

/// Build provenance, embedded at compile time by `build.rs`: git short commit,
/// working-tree dirty flag, and UTC build timestamp. Surfaced in `--version`
/// (see [`VERSION`]) and the startup log so a running process is traceable to
/// the exact source commit it was built from.
pub const GIT_HASH: &str = env!("LLMCONDUIT_GIT_HASH");
/// `"true"` if the working tree had uncommitted changes at build time.
pub const GIT_DIRTY: &str = env!("LLMCONDUIT_GIT_DIRTY");
/// UTC build timestamp (`YYYY-MM-DDTHH:MM:SSZ`), or `unknown`.
pub const BUILD_TIME: &str = env!("LLMCONDUIT_BUILD_TIME");
/// `--version` string: `"<semver> (<short-hash>, <build-time>)"`.
pub const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("LLMCONDUIT_GIT_HASH"),
    ", ",
    env!("LLMCONDUIT_BUILD_TIME"),
    ")"
);

use crate::config::Config;
use crate::engine::Gateway;
use crate::http::RouterOptions;
use crate::http::build_router;
use crate::mesh::MeshUpstreamClient;
use crate::monitor::MonitorHub;
use crate::raw::RawOutput;
use crate::replay::ReplayStore;
use crate::search::BraveSearchClient;
use crate::upstream::FailoverUpstreamClient;
use crate::upstream::FailoverUpstreamProvider;
use crate::upstream::ModelRouteSpec;
use crate::upstream::ReqwestUpstreamClient;
use crate::upstream::RouteUpstreamProvider;
use crate::upstream::RoutingUpstreamClient;
use crate::upstream::RoutingUpstreamProvider;
use crate::vision::ImageCache;
use crate::vision::ReqwestVisionClient;
use std::sync::Arc;
use std::time::Duration;

const PROVIDER_METRICS_INTERVAL_ENV: &str = "LLMCONDUIT_PROVIDER_METRICS_INTERVAL_SECS";
/// Dashboard-only cache telemetry; two minutes keeps idle hosts quiet. The
/// env override still allows anything in `5..=3600`.
const DEFAULT_PROVIDER_METRICS_INTERVAL_SECS: u64 = 120;

fn provider_metrics_interval_from_env() -> Duration {
    let seconds = std::env::var(PROVIDER_METRICS_INTERVAL_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_PROVIDER_METRICS_INTERVAL_SECS)
        .clamp(5, 3_600);
    Duration::from_secs(seconds)
}

fn spawn_provider_metrics_refresh(
    registry: crate::provider_metrics::ProviderMetricsRegistry,
    targets: Vec<crate::provider_metrics::ProviderMetricsTarget>,
    interval: Duration,
) {
    if targets.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let scraper = crate::provider_metrics::ProviderMetricsScraper::default();
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let report = registry.refresh(&scraper, &targets).await;
            if report.failed > 0 || report.skipped > 0 {
                tracing::warn!(
                    attempted = report.attempted,
                    updated = report.updated,
                    failed = report.failed,
                    skipped = report.skipped,
                    "provider metrics refresh completed with unavailable targets"
                );
            }
        }
    });
}

pub fn build_app(config: Config) -> axum::Router {
    build_app_with_gateway(config).0
}

pub fn build_app_with_options(config: Config, options: AppOptions) -> axum::Router {
    build_app_with_gateway_and_options(config, None, options).0
}

pub fn build_app_with_gateway(config: Config) -> (axum::Router, Arc<Gateway>) {
    build_app_with_gateway_and_raw_output(config, None)
}

pub fn build_app_with_gateway_and_raw_output(
    config: Config,
    raw_output: Option<RawOutput>,
) -> (axum::Router, Arc<Gateway>) {
    build_app_with_gateway_and_options(config, raw_output, AppOptions::default())
}

pub fn build_app_with_gateway_and_options(
    config: Config,
    raw_output: Option<RawOutput>,
    options: AppOptions,
) -> (axum::Router, Arc<Gateway>) {
    build_app_with_gateway_control_plane(
        config,
        raw_output,
        options,
        Vec::new(),
        crate::control_plane::UnknownModelPolicy::Passthrough,
        None,
    )
    .expect("empty control-plane route set is valid")
}

/// Build with materialized operational routes and client authentication. The
/// public legacy builders delegate here with empty/open control-plane state.
pub fn build_app_with_gateway_control_plane(
    config: Config,
    raw_output: Option<RawOutput>,
    options: AppOptions,
    operational_routes: Vec<crate::control_plane::OperationalRoutePlan>,
    unknown_model_policy: crate::control_plane::UnknownModelPolicy,
    client_auth: Option<crate::client_auth::ClientAuth>,
) -> Result<(axum::Router, Arc<Gateway>), String> {
    build_app_with_gateway_control_plane_runtime(
        config,
        raw_output,
        options,
        operational_routes,
        unknown_model_policy,
        client_auth,
        ControlPlaneRuntime::default(),
    )
}

/// Runtime-only control-plane handles. Secrets and database connections never
/// enter the persisted upstream [`Config`].
#[derive(Clone)]
pub struct ControlPlaneRuntime {
    pub persistence_store: Option<Arc<dyn crate::control_plane_store::PersistenceStore>>,
    pub persistence_queue: Option<crate::control_plane_store::PersistenceQueue>,
    /// Whether persisted request items keep image/data URIs.
    pub persistence_keep_media: bool,
    pub conversation_id_header: String,
    /// Compiled harness/session detection profiles.
    pub harness_detector: Arc<crate::harness::HarnessDetector>,
    /// In-memory session tree used to link requests into chains.
    pub session_linker: Arc<crate::sessions::SessionLinker>,
    /// YAML-configured virtual keys, retained so the live registry can be
    /// rebuilt (YAML ∪ SQL) after a key change.
    pub yaml_key_specs: Vec<crate::client_auth::VirtualKeySpec>,
    pub client_auth_required: bool,
    /// Whether at least one user account exists at startup.
    pub users_configured: bool,
}

impl Default for ControlPlaneRuntime {
    fn default() -> Self {
        Self {
            persistence_store: None,
            persistence_queue: None,
            persistence_keep_media: true,
            conversation_id_header: crate::control_plane::DEFAULT_CONVERSATION_ID_HEADER
                .to_string(),
            harness_detector: Arc::new(crate::harness::HarnessDetector::builtin()),
            session_linker: Arc::new(crate::sessions::SessionLinker::new(true)),
            yaml_key_specs: Vec::new(),
            client_auth_required: false,
            users_configured: false,
        }
    }
}

/// Full dependency-injection entry point used by the server after it has
/// connected the configured persistence backend.
#[allow(clippy::too_many_arguments)]
pub fn build_app_with_gateway_control_plane_runtime(
    config: Config,
    raw_output: Option<RawOutput>,
    options: AppOptions,
    operational_routes: Vec<crate::control_plane::OperationalRoutePlan>,
    unknown_model_policy: crate::control_plane::UnknownModelPolicy,
    client_auth: Option<crate::client_auth::ClientAuth>,
    runtime: ControlPlaneRuntime,
) -> Result<(axum::Router, Arc<Gateway>), String> {
    let conversation_id_header = runtime.conversation_id_header.trim();
    let conversation_id_header = if conversation_id_header.is_empty() {
        crate::control_plane::DEFAULT_CONVERSATION_ID_HEADER
    } else {
        conversation_id_header
    };
    axum::http::HeaderName::from_bytes(conversation_id_header.as_bytes())
        .map_err(|_| format!("invalid conversation id header name '{conversation_id_header}'"))?;
    if crate::control_plane::is_sensitive_conversation_header(conversation_id_header) {
        return Err(format!(
            "conversation id header '{conversation_id_header}' is a sensitive credential carrier"
        ));
    }
    let conversation_id_header = conversation_id_header.to_string();
    let operational_models = operational_routes
        .iter()
        .map(|route| (route.name.clone(), route.primary_profile_name.clone()))
        .collect::<Vec<_>>();
    for operational in &operational_routes {
        if let Some(route) = config.model_routes.iter().find(|route| {
            route.glob.is_none()
                && route
                    .name
                    .trim()
                    .eq_ignore_ascii_case(operational.name.trim())
        }) {
            return Err(format!(
                "operational route '{}' conflicts with configured model route '{}'",
                operational.name, route.name
            ));
        }
    }
    let http_client = reqwest::Client::builder()
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .build()
        .expect("reqwest client");
    let replay_store = ReplayStore::new(config.max_replay_entries);
    let monitor = if options.with_debug_ui {
        MonitorHub::new(512)
    } else {
        MonitorHub::disabled()
    };
    // D1 dashboard FlowStore: enabled only when the debug UI is on, mirroring the
    // monitor's zero-overhead `disabled()` split.
    let flow_store = if options.with_debug_ui {
        crate::dashboard_flow::DashboardFlowStore::new()
    } else {
        crate::dashboard_flow::DashboardFlowStore::disabled()
    };
    // D5 MetricsLayer: enabled only when the debug UI is on (same zero-overhead
    // `disabled()` split). Attached to the Gateway via `with_metrics`; the 5 s
    // coordinated snapshot task is spawned below (on a live runtime) under the same
    // gate, so production runs no ring/histogram/snapshot work.
    let metrics = if options.with_debug_ui {
        crate::metrics::MetricsLayer::new()
    } else {
        crate::metrics::MetricsLayer::disabled()
    };
    // Live active-session hub (dashboard "sessions now" view): enabled only
    // when the debug UI is on, mirroring the FlowStore's zero-overhead
    // `disabled()` split. Fed at the persistence link/terminal seams.
    let session_hub = if options.with_debug_ui {
        crate::session_hub::SessionHub::new()
    } else {
        crate::session_hub::SessionHub::disabled()
    };
    let provider_metrics = crate::provider_metrics::ProviderMetricsRegistry::default();
    let provider_metrics_targets = if options.with_debug_ui {
        config.provider_metrics_targets.clone()
    } else {
        Vec::new()
    };
    // F1 (Topic F) durable per-turn capture: opt-in, config-only gate --
    // constructed regardless of `--with-debug-ui` (works even when the debug
    // UI/dashboard is off). `disabled()` is a zero-op sink (no thread, no
    // alloc, no fs) when `turn_capture_dir` is unset.
    let turn_capture = match config.turn_capture_dir.clone() {
        Some(dir) => crate::turn_capture::TurnCapture::enabled(dir),
        None => crate::turn_capture::TurnCapture::disabled(),
    };
    let mesh_admin = if config.mesh.controller.enabled {
        Some(
            crate::mesh::controller::spawn_controller(&config.mesh.controller)
                .expect("validated mesh controller configuration"),
        )
    } else {
        None
    };
    let mesh_enabled = mesh_admin.is_some();
    // Routing mode is engaged by explicit `upstreams` OR ad-hoc `model_routes`
    // (G7); routes alone are enough to switch the gateway into the routing
    // client so route-name/glob matching applies.
    let routing_mode = !config.upstreams.is_empty()
        || !config.model_routes.is_empty()
        || !operational_routes.is_empty()
        || mesh_enabled;
    // Per-backend-model finalization policies (effort map, `template_family`
    // override, `upstream_chat_kwargs`), shared (cheap clone) across all leaf
    // clients so each resolves against the FINAL provider model (T1). Built once
    // from config; the leaf (`finalize_request_for_backend`) looks up the policy
    // for the model it actually POSTs to. Copy the scalar config knobs out so
    // the builder closure doesn't borrow `config` (moved into the Gateway below).
    let finalization_policies = crate::upstream::BackendFinalizationPolicies::from_config(&config);
    let flatten_content = config.flatten_content;
    let min_completion_tokens = config.min_completion_tokens;
    let max_sse_frame_bytes = config.max_sse_frame_bytes;
    let make_upstream_client =
        |base_url: url::Url, api_key: Option<String>, log_path: Option<std::path::PathBuf>| {
            ReqwestUpstreamClient::with_options(
                http_client.clone(),
                base_url,
                api_key,
                log_path,
                flatten_content,
                min_completion_tokens,
                max_sse_frame_bytes,
            )
            .with_finalization_policies(finalization_policies.clone())
            // D2: every leaf shares the dashboard FlowStore handle (a cheap `Clone`
            // of the inner `Arc<Mutex>`; `disabled()` no-ops when the debug UI is
            // off) so the single point that sees the on-wire body can capture it.
            .with_flow_store(flow_store.clone())
        };
    let base_upstream: Arc<dyn crate::upstream::UpstreamClient> = if routing_mode {
        let mut providers = Vec::new();
        if let Some(admin) = mesh_admin.clone() {
            providers.push(RoutingUpstreamProvider::new(
                "mesh",
                MeshUpstreamClient::new(
                    admin.registry(),
                    finalization_policies.clone(),
                    flatten_content,
                    max_sse_frame_bytes,
                    flow_store.clone(),
                )
                .with_capacity_wait_timeout(config.request_timeout),
                None,
                serde_json::Map::new(),
                Vec::new(),
                Duration::from_secs(config.upstream_failure_cooldown_secs),
            ));
        }
        providers.extend(config.upstreams.iter().map(|provider| {
            let primary_client = make_upstream_client(
                provider.upstream_base_url.clone(),
                provider.upstream_api_key.clone(),
                provider.upstream_request_log_path.clone(),
            );
            let fallback_providers = provider
                .fallback_upstreams
                .iter()
                .map(|fallback| {
                    FailoverUpstreamProvider::new(
                        fallback.name.clone(),
                        make_upstream_client(
                            fallback.upstream_base_url.clone(),
                            fallback.upstream_api_key.clone(),
                            fallback.upstream_request_log_path.clone(),
                        ),
                        fallback.upstream_model.clone(),
                        fallback.exposed_model.clone(),
                        fallback.upstream_chat_kwargs.clone(),
                    )
                })
                .collect();
            RoutingUpstreamProvider::new(
                provider.name.clone(),
                primary_client,
                provider.upstream_model.clone(),
                provider.upstream_chat_kwargs.clone(),
                fallback_providers,
                Duration::from_secs(config.upstream_failure_cooldown_secs),
            )
        }));
        // Operational/ad-hoc routes still need the ordinary top-level provider
        // as their passthrough/default catalog when no explicit `upstreams` are
        // configured. Otherwise merely adding one alias makes every unknown
        // model unroutable despite `unknown_model_policy: passthrough`.
        if config.upstreams.is_empty()
            && !mesh_enabled
            && (!operational_routes.is_empty() || !config.model_routes.is_empty())
        {
            let primary_client = make_upstream_client(
                config.upstream_base_url.clone(),
                config.upstream_api_key.clone(),
                config.upstream_request_log_path.clone(),
            );
            let fallback_providers = config
                .fallback_upstreams
                .iter()
                .map(|fallback| {
                    FailoverUpstreamProvider::new(
                        fallback.name.clone(),
                        make_upstream_client(
                            fallback.upstream_base_url.clone(),
                            fallback.upstream_api_key.clone(),
                            fallback.upstream_request_log_path.clone(),
                        ),
                        fallback.upstream_model.clone(),
                        fallback.exposed_model.clone(),
                        fallback.upstream_chat_kwargs.clone(),
                    )
                })
                .collect();
            providers.push(RoutingUpstreamProvider::new(
                "primary",
                primary_client,
                config.upstream_model.clone(),
                config.upstream_chat_kwargs.clone(),
                fallback_providers,
                Duration::from_secs(config.upstream_failure_cooldown_secs),
            ));
        }
        // Build a synthetic provider + spec per ad-hoc route (G7). Each route is
        // a single-upstream client keyed by request-model name/glob; the glob
        // matcher was compiled at config time.
        let cooldown = Duration::from_secs(config.upstream_failure_cooldown_secs);
        let mut route_providers =
            Vec::with_capacity(config.model_routes.len() + operational_routes.len());
        let mut route_specs =
            Vec::with_capacity(config.model_routes.len() + operational_routes.len());
        for (index, route) in config.model_routes.iter().enumerate() {
            let client = make_upstream_client(
                route.upstream_base_url.clone(),
                config.upstream_api_key.clone(),
                config.upstream_request_log_path.clone(),
            );
            route_providers.push(RouteUpstreamProvider::new(
                format!("route-{}", route.name),
                client,
                cooldown,
            ));
            route_specs.push(ModelRouteSpec::new(
                route.name.clone(),
                route.glob.clone(),
                index,
                route.upstream_model.clone(),
            ));
        }
        for route in &operational_routes {
            let index = route_providers.len();
            let legs = route
                .providers
                .iter()
                .map(|provider| {
                    FailoverUpstreamProvider::new(
                        provider.backend_name.clone(),
                        make_upstream_client(
                            provider.base_url.clone(),
                            provider.api_key.clone(),
                            provider.request_log_path.clone(),
                        ),
                        provider.upstream_model.clone(),
                        None,
                        provider.upstream_chat_kwargs.clone(),
                    )
                })
                .collect();
            route_providers.push(RouteUpstreamProvider::from_failover(
                format!("alias-{}", route.name),
                legs,
                cooldown,
            )?);
            route_specs.push(ModelRouteSpec::advertised_exact(route.name.clone(), index));
        }
        Arc::new(RoutingUpstreamClient::with_routes(
            providers,
            route_providers,
            route_specs,
        ))
    } else {
        let primary_upstream = make_upstream_client(
            config.upstream_base_url.clone(),
            config.upstream_api_key.clone(),
            config.upstream_request_log_path.clone(),
        );
        if config.fallback_upstreams.is_empty() {
            // D2: the BARE leaf is the engine's upstream directly — no routing/
            // failover layer owns the `provider` serving field, so mark this leaf to
            // synthesize `provider = "primary"`.
            Arc::new(primary_upstream.into_bare_primary())
        } else {
            let mut providers = vec![FailoverUpstreamProvider::new(
                "primary",
                primary_upstream,
                None,
                None,
                serde_json::Map::new(),
            )];
            providers.extend(config.fallback_upstreams.iter().map(|provider| {
                FailoverUpstreamProvider::new(
                    provider.name.clone(),
                    make_upstream_client(
                        provider.upstream_base_url.clone(),
                        provider.upstream_api_key.clone(),
                        provider.upstream_request_log_path.clone(),
                    ),
                    provider.upstream_model.clone(),
                    provider.exposed_model.clone(),
                    provider.upstream_chat_kwargs.clone(),
                )
            }));
            Arc::new(FailoverUpstreamClient::new(
                providers,
                Duration::from_secs(config.upstream_failure_cooldown_secs),
            ))
        }
    };
    let managed_providers = runtime.persistence_store.as_ref().map(|store| {
        crate::managed_providers::ManagedProviderRegistry::new(
            Arc::clone(store),
            crate::managed_providers::ManagedProviderOptions {
                http_client: http_client.clone(),
                flatten_content,
                min_completion_tokens,
                max_sse_frame_bytes,
                finalization_policies: finalization_policies.clone(),
                flow_store: flow_store.clone(),
            },
        )
    });
    let upstream: Arc<dyn crate::upstream::UpstreamClient> =
        if let Some(registry) = managed_providers.clone() {
            Arc::new(crate::managed_providers::ManagedProviderUpstream::new(
                Arc::clone(&base_upstream),
                registry,
            ))
        } else {
            base_upstream
        };
    let search = Arc::new(BraveSearchClient::new(http_client.clone(), config.clone()));
    // G4 image agent: a vision client + a shared per-session image cache. The
    // cache is constructed once and shared so the strip seam (in
    // `stream_responses`) and the executor (`run_image_analysis`) see the same
    // store. Construction is unconditional and cheap; gating happens per-turn.
    let fleet = if options.with_debug_ui {
        match crate::dashboard_fleet::FleetClient::from_env(http_client.clone()) {
            Ok(fleet) => fleet.map(Arc::new),
            Err(err) => {
                tracing::warn!("Fleet dashboard integration disabled: {err}");
                None
            }
        }
    } else {
        None
    };
    let vision: Arc<dyn crate::vision::VisionClient> =
        Arc::new(ReqwestVisionClient::new(http_client, &config));
    let image_cache = Arc::new(ImageCache::from_config(&config));

    // D7 dashboard/`/debug` auth, built from the ENVIRONMENT (never from the
    // persisted `Config`). Only constructed when the debug UI is enabled. The
    // env snapshot + bind address also drive the route-registration decision:
    // a non-loopback bind without a token + validated https origin REFUSES to
    // register the protected routes (logged), unless `ALLOW_INSECURE=1`.
    let bind_addr = config.bind_addr;
    let authz = crate::authz::AuthzService::from_config(&config.auth)
        .unwrap_or_else(|err| panic!("inference auth startup validation failed: {err}"));
    match config.auth.mode {
        crate::config::AuthMode::Disabled => {
            tracing::info!("inference authentication disabled");
        }
        crate::config::AuthMode::Enforce => {
            tracing::info!(
                store_path = %config.auth.store_path.display(),
                "inference authentication enabled"
            );
        }
    }
    let (dashboard_auth, register_protected_routes) = if options.with_debug_ui {
        build_dashboard_auth(bind_addr)
    } else {
        (None, false)
    };

    // D5: capture cheap `Clone` handles BEFORE the originals move into `Gateway::new`
    // so the 5 s coordinated snapshot task can own them (each is an `Arc`-backed
    // handle; `disabled()` ones no-op). The snapshot task reads the FlowStore THEN
    // the MetricsLayer (the fixed lock order) + one topology `Arc` + the monitor seq.
    let snapshot_flow_store = flow_store.clone();
    let snapshot_metrics = metrics.clone();
    let snapshot_monitor = monitor.clone();
    let mut gateway = Gateway::new(
        config,
        replay_store,
        upstream,
        search,
        vision,
        image_cache,
        monitor,
        raw_output,
        flow_store,
    )
    .with_dashboard_auth(dashboard_auth)
    .with_authz(authz)
    .with_metrics(metrics)
    .with_session_hub(session_hub)
    .with_provider_metrics(provider_metrics.clone())
    .with_turn_capture(turn_capture)
    .with_fleet(fleet)
    .with_managed_providers(managed_providers)
    .with_mesh_admin(mesh_admin)
    .with_operational_models(operational_models, unknown_model_policy);
    if let Some(client_auth) = client_auth {
        gateway = gateway.with_client_auth(client_auth);
    }
    if let Some(queue) = runtime.persistence_queue {
        gateway = gateway.with_persistence_queue(queue);
    }
    gateway = gateway
        .with_persistence_keep_media(runtime.persistence_keep_media)
        .with_harness_detector(runtime.harness_detector)
        .with_session_linker(runtime.session_linker)
        .with_key_registry_source(runtime.yaml_key_specs, runtime.client_auth_required);
    gateway.set_users_configured(runtime.users_configured);
    if let Some(store) = runtime.persistence_store {
        gateway = gateway.with_persistence_store(store);
    }
    gateway = gateway.with_conversation_id_header(conversation_id_header);
    let gateway = Arc::new(gateway);
    // D4: spawn the topology-health publication task ONLY when the debug UI is on,
    // so production keeps the zero-overhead path (no 1 s tick). Guard on a live
    // tokio runtime so a non-async embedder that enables the debug UI does not
    // panic in `tokio::spawn` (the `main.rs` server path always has one).
    if options.with_debug_ui && tokio::runtime::Handle::try_current().is_ok() {
        gateway.spawn_provider_health_publisher();
        // D5: spawn the 5 s coordinated body-free snapshot task (same gate + live-
        // runtime guard). It takes the single FlowStore→Metrics critical section,
        // captures one topology `Arc` (D4's publisher), and pushes a body-free cut
        // onto the bounded ring every 5 s.
        crate::metrics::spawn_snapshot_task(
            snapshot_metrics,
            snapshot_flow_store,
            gateway.provider_health_publisher(),
            snapshot_monitor,
        );
        spawn_provider_metrics_refresh(
            provider_metrics,
            provider_metrics_targets,
            provider_metrics_interval_from_env(),
        );
    }
    let router_options = RouterOptions {
        with_debug_ui: options.with_debug_ui,
        register_protected_routes,
    };
    let app = build_router(Arc::clone(&gateway), router_options);
    Ok((app, gateway))
}

/// Build the D7 dashboard auth context + the route-registration decision for a
/// server binding to `bind_addr`, reading secrets from the process environment.
/// Returns `(Some(auth), true)` when the protected routes may register,
/// `(None, false)` when the startup decision refuses them (logged) or the auth
/// context fails to build (e.g. a malformed secret — logged). Warnings (a
/// tokenless loopback dev server, an auto-generated key, an insecure override)
/// are logged here so a running process is auditable.
fn build_dashboard_auth(
    bind_addr: std::net::SocketAddr,
) -> (Option<Arc<crate::dashboard_auth::DashboardAuth>>, bool) {
    use crate::dashboard_auth::DashboardEnv;
    use crate::dashboard_auth::RouteDecision;
    use crate::dashboard_auth::startup_route_decision;

    let env = DashboardEnv::from_process_env();
    let decision = startup_route_decision(bind_addr, &env);
    match decision {
        RouteDecision::Refuse(refusal) => {
            tracing::warn!(
                "dashboard/debug routes NOT registered: {} (set the required env vars, or \
                 LLMCONDUIT_ALLOW_INSECURE_DASHBOARD=1 to override)",
                refusal.reason()
            );
            (None, false)
        }
        RouteDecision::Register { warnings } => {
            for warning in &warnings {
                tracing::warn!("dashboard auth: {warning}");
            }
            match crate::dashboard_auth::DashboardAuth::from_env(bind_addr, &env) {
                Ok(build) => {
                    for warning in &build.warnings {
                        tracing::warn!("dashboard auth: {warning}");
                    }
                    (Some(build.auth), true)
                }
                Err(err) => {
                    tracing::error!(
                        "dashboard/debug routes NOT registered: failed to build auth context: \
                         {err}"
                    );
                    (None, false)
                }
            }
        }
    }
}

pub fn build_app_from_gateway(gateway: Arc<Gateway>) -> axum::Router {
    build_app_from_gateway_with_options(gateway, AppOptions::default())
}

pub fn build_app_from_gateway_with_options(
    gateway: Arc<Gateway>,
    options: AppOptions,
) -> axum::Router {
    build_router(gateway, options.into())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AppOptions {
    pub with_debug_ui: bool,
}

impl From<AppOptions> for RouterOptions {
    fn from(options: AppOptions) -> Self {
        // The `build_app_from_gateway*` path (tests, embedders) has no bind
        // address / env snapshot to run the D7 startup decision against, so it
        // does NOT register the protected routes — `build_app_with_gateway_and_options`
        // is the path that computes the decision and attaches the auth context.
        Self {
            with_debug_ui: options.with_debug_ui,
            register_protected_routes: false,
        }
    }
}
