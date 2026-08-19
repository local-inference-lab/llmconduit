pub mod adapters;
pub mod cli;
pub mod config;
pub mod dashboard_api;
pub mod dashboard_auth;
pub mod dashboard_flow;
pub mod dashboard_ui;
pub mod dashboard_ws;
pub mod debug_ui;
pub mod engine;
pub mod error;
pub mod http;
pub mod log_rotation;
pub mod metrics;
pub mod models;
pub mod monitor;
pub(crate) mod proxy_headers;
pub mod raw;
pub(crate) mod redaction;
pub mod replay;
pub mod request_log;
pub mod search;
pub(crate) mod sse_guard;
/// Crate-internal, test-only peak-allocation probe (the crate's single
/// `#[global_allocator]`), shared by the `sse_guard` reject-path and
/// `dashboard_flow::capture_body` heap-bound tests.
#[cfg(test)]
pub(crate) mod test_alloc_probe;
pub(crate) mod tool_delta_gate;
pub mod turn_capture;
pub mod upstream;
pub mod vision;

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
use crate::monitor::MonitorHub;
use crate::raw::RawOutput;
use crate::replay::ReplayStore;
use crate::search::BraveSearchClient;
use crate::upstream::FailoverUpstreamProvider;
use crate::upstream::ProfileRoutingClient;
use crate::upstream::ReqwestUpstreamClient;
use crate::vision::ImageCache;
use std::sync::Arc;
use std::time::Duration;

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
    // F1 (Topic F) durable per-turn capture: opt-in, config-only gate --
    // constructed regardless of `--with-debug-ui` (works even when the debug
    // UI/dashboard is off). `disabled()` is a zero-op sink (no thread, no
    // alloc, no fs) when `turn_capture_dir` is unset.
    let turn_capture = match config.turn_capture_dir.clone() {
        Some(dir) => crate::turn_capture::TurnCapture::enabled(dir),
        None => crate::turn_capture::TurnCapture::disabled(),
    };
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
    // One provider per named upstream (pure endpoints: no per-provider model
    // rewrite; a profile chain supplies the model per step). The
    // `ProfileRoutingClient` resolves each request model to its profile's chain
    // and dispatches over these shared providers, so cooldown is keyed by
    // upstream name across every profile that routes to it.
    let providers: Vec<FailoverUpstreamProvider> = config
        .upstreams
        .iter()
        .map(|entry| {
            FailoverUpstreamProvider::new(
                entry.name.clone(),
                make_upstream_client(
                    entry.url.clone(),
                    entry.api_key.clone(),
                    entry.request_log_path.clone(),
                ),
                None,
                entry.chat_kwargs.clone(),
            )
        })
        .collect();
    let config_arc = Arc::new(config.clone());
    let upstream: Arc<dyn crate::upstream::UpstreamClient> =
        Arc::new(ProfileRoutingClient::new(config_arc, providers));
    let search = Arc::new(BraveSearchClient::new(http_client.clone(), config.clone()));
    // G4 image agent: a shared per-session image cache. Constructed once and
    // shared so the strip seam (in `stream_responses`) and the executor
    // (`run_image_analysis`) see the same store. The analyzer is dispatched
    // through the gateway's own upstreams by model name, so no separate vision
    // client is wired here. Construction is unconditional and cheap; activation
    // is per-turn on the resolved profile's `image_analysis`.
    let image_cache = Arc::new(ImageCache::from_config(&config));

    // D7 dashboard/`/debug` auth, built from the ENVIRONMENT (never from the
    // persisted `Config`). Only constructed when the debug UI is enabled. The
    // env snapshot + bind address also drive the route-registration decision:
    // a non-loopback bind without a token + validated https origin REFUSES to
    // register the protected routes (logged), unless `ALLOW_INSECURE=1`.
    let bind_addr = config.bind_addr;
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
    let gateway = Arc::new(
        Gateway::new(
            config,
            replay_store,
            upstream,
            search,
            image_cache,
            monitor,
            raw_output,
            flow_store,
        )
        .with_dashboard_auth(dashboard_auth)
        .with_metrics(metrics)
        .with_turn_capture(turn_capture),
    );
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
    }
    let router_options = RouterOptions {
        with_debug_ui: options.with_debug_ui,
        register_protected_routes,
    };
    let app = build_router(Arc::clone(&gateway), router_options);
    (app, gateway)
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
