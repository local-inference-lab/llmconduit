pub mod adapters;
pub mod cli;
pub mod config;
pub mod debug_ui;
pub mod engine;
pub mod error;
pub mod http;
pub mod models;
pub mod monitor;
pub mod raw;
pub mod replay;
pub mod request_log;
pub mod search;
pub mod upstream;
pub mod vision;

use crate::config::Config;
use crate::engine::Gateway;
use crate::http::RouterOptions;
use crate::http::build_router;
use crate::monitor::MonitorHub;
use crate::raw::RawOutput;
use crate::replay::ReplayStore;
use crate::search::BraveSearchClient;
use crate::search::Crawl4AISearchClient;
use crate::search::KagiSearchClient;
use crate::search::SearchClient;
use crate::upstream::FailoverUpstreamClient;
use crate::upstream::FailoverUpstreamProvider;
use crate::upstream::ReqwestUpstreamClient;
use crate::upstream::RoutingUpstreamClient;
use crate::upstream::RoutingUpstreamProvider;
use crate::vision::ImageCache;
use crate::vision::ReqwestVisionClient;
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
    let upstream: Arc<dyn crate::upstream::UpstreamClient> = if !config.upstreams.is_empty() {
        let providers = config
            .upstreams
            .iter()
            .map(|provider| {
                let primary_client = ReqwestUpstreamClient::new(
                    http_client.clone(),
                    provider.upstream_base_url.clone(),
                    provider.upstream_api_key.clone(),
                    provider.upstream_request_log_path.clone(),
                    config.flatten_content,
                );
                let fallback_providers = provider
                    .fallback_upstreams
                    .iter()
                    .map(|fallback| {
                        FailoverUpstreamProvider::new(
                            fallback.name.clone(),
                            ReqwestUpstreamClient::new(
                                http_client.clone(),
                                fallback.upstream_base_url.clone(),
                                fallback.upstream_api_key.clone(),
                                fallback.upstream_request_log_path.clone(),
                                config.flatten_content,
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
            })
            .collect();
        Arc::new(RoutingUpstreamClient::new(providers))
    } else {
        let primary_upstream = ReqwestUpstreamClient::new(
            http_client.clone(),
            config.upstream_base_url.clone(),
            config.upstream_api_key.clone(),
            config.upstream_request_log_path.clone(),
            config.flatten_content,
        );
        if config.fallback_upstreams.is_empty() {
            Arc::new(primary_upstream)
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
                    ReqwestUpstreamClient::new(
                        http_client.clone(),
                        provider.upstream_base_url.clone(),
                        provider.upstream_api_key.clone(),
                        provider.upstream_request_log_path.clone(),
                        config.flatten_content,
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
    let search: Arc<dyn SearchClient> = if config.search_backend == "crawl4ai" {
        Arc::new(Crawl4AISearchClient::new(http_client.clone(), config.clone()))
    } else if config.search_backend == "kagi" {
        Arc::new(KagiSearchClient::new(http_client.clone(), config.clone()))
    } else {
        Arc::new(BraveSearchClient::new(http_client.clone(), config.clone()))
    };
    // G4 image agent: a vision client + a shared per-session image cache. The
    // cache is constructed once and shared so the strip seam (in
    // `stream_responses`) and the executor (`run_image_analysis`) see the same
    // store. Construction is unconditional and cheap; gating happens per-turn.
    let vision: Arc<dyn crate::vision::VisionClient> =
        Arc::new(ReqwestVisionClient::new(http_client, &config));
    let image_cache = Arc::new(ImageCache::from_config(&config));
    let gateway = Arc::new(Gateway::new(
        config,
        replay_store,
        upstream,
        search,
        vision,
        image_cache,
        monitor,
        raw_output,
    ));
    let app = build_router(Arc::clone(&gateway), options.into());
    (app, gateway)
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
        Self {
            with_debug_ui: options.with_debug_ui,
        }
    }
}
