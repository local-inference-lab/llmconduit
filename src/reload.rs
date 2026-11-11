//! File-backed configuration reload with one immutable gateway per request.

use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use tokio_util::task::AbortOnDropHandle;
use tower::ServiceExt;

use crate::AppOptions;
use crate::build_app_generation;
use crate::config::{Config, PersistedConfig, parse_persisted_config};
use crate::engine::Gateway;
use crate::raw::RawOutput;

const POLL_INTERVAL: Duration = Duration::from_millis(500);

struct Generation {
    router: Router,
    gateway: Arc<Gateway>,
}

/// Selects a complete router/config/client generation at the start of each request.
#[derive(Clone)]
pub struct ReloadableApp {
    current: Arc<RwLock<Generation>>,
    _watcher: Arc<AbortOnDropHandle<()>>,
}

/// Dashboard sockets read live prices while inference keeps its original config.
#[derive(Clone)]
pub struct GatewaySource(Weak<RwLock<Generation>>);

impl GatewaySource {
    pub(crate) fn gateway(&self) -> Option<Arc<Gateway>> {
        self.0.upgrade().map(|current| {
            current
                .read()
                .expect("config generation lock poisoned")
                .gateway
                .clone()
        })
    }
}

impl ReloadableApp {
    /// Load the file with environment and CLI overrides, then watch its contents.
    /// A missing file uses defaults at startup; a missing file during reload is
    /// rejected. Two identical reads debounce writes and support atomic renames.
    pub async fn load(
        path: PathBuf,
        route_specs: Vec<String>,
        raw_output: Option<RawOutput>,
        options: AppOptions,
    ) -> Result<Self, String> {
        let source_path = path.clone();
        let specs = route_specs.clone();
        let raw = raw_output.clone();
        let (generation, contents) = tokio::task::spawn_blocking(move || {
            let contents = match std::fs::read_to_string(&source_path) {
                Ok(contents) => Some(contents),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Err(format!("failed to read {}: {error}", source_path.display()));
                }
            };
            let persisted = match &contents {
                Some(contents) => {
                    parse_persisted_config(&source_path, contents).map_err(|error| {
                        format!("failed to parse {}: {error}", source_path.display())
                    })?
                }
                None => PersistedConfig::default(),
            };
            let config = Config::from_persisted_and_overrides(persisted, &specs)?;
            let (router, gateway) = build_app_generation(config, raw, options, None);
            Ok((Generation { router, gateway }, contents))
        })
        .await
        .map_err(|error| format!("config initialization failed: {error}"))??;
        let current = Arc::new(RwLock::new(generation));
        let watcher = tokio::spawn(watch_config(
            GatewaySource(Arc::downgrade(&current)),
            path,
            route_specs,
            raw_output,
            options,
            contents,
        ));
        Ok(Self {
            current,
            _watcher: Arc::new(AbortOnDropHandle::new(watcher)),
        })
    }

    pub fn gateway(&self) -> Arc<Gateway> {
        self.current
            .read()
            .expect("config generation lock poisoned")
            .gateway
            .clone()
    }

    pub fn router(&self) -> Router {
        Router::new().fallback(dispatch).with_state(self.clone())
    }
}

async fn dispatch(State(app): State<ReloadableApp>, mut request: Request) -> Response {
    let router = app
        .current
        .read()
        .expect("config generation lock poisoned")
        .router
        .clone();
    request
        .extensions_mut()
        .insert(GatewaySource(Arc::downgrade(&app.current)));
    // Release the generation lock before polling the body or awaiting upstream IO.
    router
        .oneshot(request)
        .await
        .unwrap_or_else(|never| match never {})
}

async fn watch_config(
    source: GatewaySource,
    path: PathBuf,
    route_specs: Vec<String>,
    raw_output: Option<RawOutput>,
    options: AppOptions,
    mut attempted: Option<String>,
) {
    let Some(initial) = source.gateway() else {
        return;
    };
    let mut health_task = options
        .with_debug_ui
        .then(|| AbortOnDropHandle::new(initial.spawn_provider_health_publisher()));
    let _snapshot_task = options.with_debug_ui.then(|| {
        AbortOnDropHandle::new(crate::metrics::spawn_snapshot_task(
            initial.metrics().clone(),
            initial.flow_store().clone(),
            initial.provider_health_publisher(),
            initial.monitor_hub(),
        ))
    });
    drop(initial);
    let mut pending = None;
    let mut read_error = None;
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if source.0.strong_count() == 0 {
            return;
        }
        let source_path = path.clone();
        let contents = tokio::task::spawn_blocking(move || std::fs::read_to_string(source_path))
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
        let contents = match contents {
            Ok(contents) => {
                read_error = None;
                contents
            }
            Err(error) => {
                if read_error.as_ref() != Some(&error) {
                    tracing::warn!(path = %path.display(), %error, "config reload rejected; keeping active configuration");
                    read_error = Some(error);
                }
                pending = None;
                continue;
            }
        };
        if attempted.as_ref() == Some(&contents) {
            pending = None;
            continue;
        }
        if pending.as_ref() != Some(&contents) {
            pending = Some(contents);
            continue;
        }
        pending = None;
        attempted = Some(contents.clone());
        let Some(previous) = source.gateway() else {
            return;
        };
        let source_path = path.clone();
        let specs = route_specs.clone();
        let raw = raw_output.clone();
        let candidate = tokio::task::spawn_blocking(move || {
            if contents.trim().is_empty() {
                return Err("config file is empty".to_string());
            }
            let persisted = parse_persisted_config(&source_path, &contents)
                .map_err(|error| error.reload_message(&contents))?;
            let config = Config::from_persisted_and_overrides(persisted, &specs)?;
            if config.bind_addr != previous.config().bind_addr {
                return Err("bind_addr changes require a restart".to_string());
            }
            if config.debug_log_max_age_hours != previous.config().debug_log_max_age_hours {
                return Err("debug_log_max_age_hours changes require a restart".to_string());
            }
            let (router, gateway) = build_app_generation(config, raw, options, Some(&previous));
            Ok(Generation { router, gateway })
        })
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result);
        let candidate = match candidate {
            Ok(candidate) => candidate,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "config reload rejected; keeping active configuration");
                continue;
            }
        };
        // Join the stopped publisher before starting its replacement: both use
        // the same sequence source and only the serving providers may publish.
        if let Some(task) = health_task.take() {
            task.abort();
            let _ = task.await;
        }
        let Some(current) = source.0.upgrade() else {
            return;
        };
        let gateway = candidate.gateway.clone();
        *current.write().expect("config generation lock poisoned") = candidate;
        health_task = options
            .with_debug_ui
            .then(|| AbortOnDropHandle::new(gateway.spawn_provider_health_publisher()));
        tracing::info!(path = %path.display(), "configuration reloaded");
    }
}
