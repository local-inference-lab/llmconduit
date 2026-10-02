//! Bounded, best-effort provider cache metrics for operator-configured targets.
//!
//! These values are aggregate provider observability. They must never be copied
//! into a request/key usage record because Prometheus samples have no trustworthy
//! per-request correlation.

use axum::extract::Extension;
use axum::routing::get;
use axum::{Json, Router};
use futures::StreamExt;
use futures::stream;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

pub const DEFAULT_METRICS_BODY_LIMIT: usize = 512 * 1024;
/// The configured target list is operator-controlled but still bounded so a bad
/// configuration cannot turn one refresh into unbounded work or retained state.
pub const MAX_PROVIDER_METRICS_TARGETS: usize = 64;
const MAX_CONCURRENT_SCRAPES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsSource {
    Vllm,
    Sglang,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProviderCacheMetrics {
    pub provider: String,
    pub source: MetricsSource,
    pub fetched_at_ms: u64,
    pub cache_hits: Option<f64>,
    pub cache_queries: Option<f64>,
    pub cache_hit_rate: Option<f64>,
    pub kv_cache_usage: Option<f64>,
    pub data_quality: &'static str,
}

#[derive(Debug, Clone)]
pub struct ProviderMetricsTarget {
    provider: String,
    url: Url,
    source: MetricsSource,
}

impl ProviderMetricsTarget {
    /// Construct only from operator configuration. Dashboard/API request payloads
    /// must not be allowed to call this constructor with caller-supplied URLs.
    pub fn from_operator_config(
        provider: impl Into<String>,
        url: Url,
        source: MetricsSource,
    ) -> Result<Self, ProviderMetricsError> {
        let provider = provider.into();
        if provider.trim().is_empty()
            || !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/metrics"
        {
            return Err(ProviderMetricsError::InvalidTarget);
        }
        Ok(Self {
            provider,
            url,
            source,
        })
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }
}

/// A body-only dashboard projection. Samples are sorted by provider and carry their
/// own fetch timestamps; an empty list means metrics are unavailable, never zero.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProviderMetricsSnapshot {
    pub generated_at_ms: u64,
    pub providers: Vec<ProviderCacheMetrics>,
}

/// Outcome of one best-effort refresh pass. Individual scrape errors are counted but
/// intentionally do not fail the pass or evict the provider's last good sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderMetricsRefresh {
    pub attempted: usize,
    pub updated: usize,
    pub failed: usize,
    pub skipped: usize,
}

/// Last-good provider cache samples shared by the optional refresh task and dashboard.
/// The registry has no inference dependency: scrape failure only preserves stale data.
#[derive(Clone, Default)]
pub struct ProviderMetricsRegistry {
    samples: Arc<RwLock<BTreeMap<String, ProviderCacheMetrics>>>,
}

impl ProviderMetricsRegistry {
    pub fn snapshot(&self) -> ProviderMetricsSnapshot {
        let providers = self
            .samples
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect();
        ProviderMetricsSnapshot {
            generated_at_ms: now_ms(),
            providers,
        }
    }

    /// Refresh configured targets concurrently with a fixed fan-out. Targets after the
    /// hard cap are skipped; providers removed from configuration are pruned, while a
    /// configured provider whose scrape fails keeps its last good sample.
    pub async fn refresh(
        &self,
        scraper: &ProviderMetricsScraper,
        targets: &[ProviderMetricsTarget],
    ) -> ProviderMetricsRefresh {
        let bounded: Vec<_> = targets
            .iter()
            .take(MAX_PROVIDER_METRICS_TARGETS)
            .cloned()
            .collect();
        let configured: BTreeSet<_> = bounded
            .iter()
            .map(|target| target.provider.clone())
            .collect();
        self.samples
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|provider, _| configured.contains(provider));

        let results = stream::iter(bounded.into_iter().map(|target| {
            let scraper = scraper.clone();
            async move {
                let provider = target.provider.clone();
                (provider, scraper.scrape(&target).await)
            }
        }))
        .buffer_unordered(MAX_CONCURRENT_SCRAPES)
        .collect::<Vec<_>>()
        .await;

        let attempted = results.len();
        let mut updated = 0usize;
        for (provider, result) in results {
            if let Ok(sample) = result {
                self.samples
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(provider, sample);
                updated += 1;
            }
        }
        ProviderMetricsRefresh {
            attempted,
            updated,
            failed: attempted.saturating_sub(updated),
            skipped: targets.len().saturating_sub(attempted),
        }
    }

    #[cfg(test)]
    fn record_sample(&self, sample: ProviderCacheMetrics) {
        self.samples
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(sample.provider.clone(), sample);
    }
}

/// Isolated dashboard route; the main dashboard router remains responsible for wrapping
/// it in the existing authentication and no-store middleware.
pub fn dashboard_routes<S>(registry: ProviderMetricsRegistry) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/dashboard/api/provider-metrics", get(dashboard_snapshot))
        .layer(Extension(registry))
}

async fn dashboard_snapshot(
    Extension(registry): Extension<ProviderMetricsRegistry>,
) -> Json<ProviderMetricsSnapshot> {
    Json(registry.snapshot())
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderMetricsError {
    #[error("invalid operator-configured metrics target")]
    InvalidTarget,
    #[error("metrics request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("metrics endpoint returned HTTP {0}")]
    Http(reqwest::StatusCode),
    #[error("metrics endpoint did not return text")]
    NonTextResponse,
    #[error("metrics response exceeds configured limit")]
    BodyTooLarge,
    #[error("metrics response is not UTF-8 text")]
    InvalidText,
}

#[derive(Clone)]
pub struct ProviderMetricsScraper {
    client: reqwest::Client,
    total_timeout: Duration,
    body_limit: usize,
}

impl ProviderMetricsScraper {
    pub fn new(
        connect_timeout: Duration,
        total_timeout: Duration,
        body_limit: usize,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            // A configured endpoint redirecting elsewhere would bypass the
            // operator-only target boundary.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            total_timeout,
            body_limit,
        })
    }

    /// Scrape one target. Callers should log errors and retain the previous sample;
    /// inference must never wait on or fail because of this best-effort path.
    pub async fn scrape(
        &self,
        target: &ProviderMetricsTarget,
    ) -> Result<ProviderCacheMetrics, ProviderMetricsError> {
        let response = self
            .client
            .get(target.url.clone())
            .timeout(self.total_timeout)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(ProviderMetricsError::Http(response.status()));
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if !(content_type.starts_with("text/plain")
            || content_type.starts_with("application/openmetrics-text"))
        {
            return Err(ProviderMetricsError::NonTextResponse);
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.body_limit as u64)
        {
            return Err(ProviderMetricsError::BodyTooLarge);
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if body.len().saturating_add(chunk.len()) > self.body_limit {
                return Err(ProviderMetricsError::BodyTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        let text = std::str::from_utf8(&body).map_err(|_| ProviderMetricsError::InvalidText)?;
        Ok(parse_provider_metrics(
            &target.provider,
            target.source,
            text,
            now_ms(),
        ))
    }
}

impl Default for ProviderMetricsScraper {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(2),
            Duration::from_secs(5),
            DEFAULT_METRICS_BODY_LIMIT,
        )
        .expect("static provider metrics client configuration")
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Parse only the explicitly supported cache metrics. Unknown Prometheus names,
/// malformed samples, negative values, and NaN/Inf are ignored.
pub fn parse_provider_metrics(
    provider: &str,
    source: MetricsSource,
    body: &str,
    fetched_at_ms: u64,
) -> ProviderCacheMetrics {
    let whitelist = match source {
        MetricsSource::Vllm => &[
            "vllm:prefix_cache_hits",
            "vllm:prefix_cache_queries",
            "vllm:external_prefix_cache_hits",
            "vllm:external_prefix_cache_queries",
            "vllm:kv_cache_usage_perc",
        ][..],
        MetricsSource::Sglang => &["sglang:cache_hit_rate"][..],
    };
    let mut samples: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((series, raw_value)) = line.rsplit_once(char::is_whitespace) else {
            continue;
        };
        let name = series.split_once('{').map_or(series, |(name, _)| name);
        if !whitelist.contains(&name) {
            continue;
        }
        let Ok(value) = raw_value.trim().parse::<f64>() else {
            continue;
        };
        if !value.is_finite() || value < 0.0 {
            continue;
        }
        samples.entry(name).or_default().push(value);
    }

    let hits = sum(&samples, "vllm:prefix_cache_hits")
        .zip_or(sum(&samples, "vllm:external_prefix_cache_hits"), |a, b| {
            a + b
        });
    let queries = sum(&samples, "vllm:prefix_cache_queries").zip_or(
        sum(&samples, "vllm:external_prefix_cache_queries"),
        |a, b| a + b,
    );
    let vllm_rate = hits
        .zip(queries)
        .and_then(|(hits, queries)| (queries > 0.0).then_some((hits / queries).clamp(0.0, 1.0)));
    let sglang_rate =
        mean(&samples, "sglang:cache_hit_rate").filter(|value| (0.0..=1.0).contains(value));
    let kv_cache_usage =
        mean(&samples, "vllm:kv_cache_usage_perc").filter(|value| (0.0..=1.0).contains(value));

    ProviderCacheMetrics {
        provider: provider.to_string(),
        source,
        fetched_at_ms,
        cache_hits: hits,
        cache_queries: queries,
        cache_hit_rate: vllm_rate.or(sglang_rate),
        kv_cache_usage,
        data_quality: "derived",
    }
}

trait ZipOr<T> {
    fn zip_or(self, other: Option<T>, both: impl FnOnce(T, T) -> T) -> Option<T>;
}

impl<T> ZipOr<T> for Option<T> {
    fn zip_or(self, other: Option<T>, both: impl FnOnce(T, T) -> T) -> Option<T> {
        match (self, other) {
            (Some(a), Some(b)) => Some(both(a, b)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        }
    }
}

fn sum(samples: &BTreeMap<&str, Vec<f64>>, name: &str) -> Option<f64> {
    samples.get(name).map(|values| values.iter().sum())
}

fn mean(samples: &BTreeMap<&str, Vec<f64>>, name: &str) -> Option<f64> {
    let values = samples.get(name)?;
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[test]
    fn vllm_parser_whitelists_and_aggregates_cache_counters() {
        let body = r#"
          # TYPE vllm:prefix_cache_hits counter
          vllm:prefix_cache_hits{model_name="a"} 20
          vllm:prefix_cache_hits{model_name="b"} 10
          vllm:prefix_cache_queries{model_name="a"} 50
          vllm:prefix_cache_queries{model_name="b"} 50
          vllm:kv_cache_usage_perc{model_name="a"} 0.4
          process_resident_memory_bytes 999999999
          vllm:prompt_tokens_total 12345
        "#;
        let metrics = parse_provider_metrics("vllm-a", MetricsSource::Vllm, body, 7);
        assert_eq!(metrics.cache_hits, Some(30.0));
        assert_eq!(metrics.cache_queries, Some(100.0));
        assert_eq!(metrics.cache_hit_rate, Some(0.3));
        assert_eq!(metrics.kv_cache_usage, Some(0.4));
        assert_eq!(metrics.data_quality, "derived");
    }

    #[test]
    fn sglang_parser_accepts_only_bounded_finite_hit_rate() {
        let valid = parse_provider_metrics(
            "sglang-a",
            MetricsSource::Sglang,
            "sglang:cache_hit_rate{model_name=\"a\"} 0.25\nignored 1\n",
            1,
        );
        assert_eq!(valid.cache_hit_rate, Some(0.25));
        for value in ["NaN", "+Inf", "-1", "1.1"] {
            let body = format!("sglang:cache_hit_rate {value}\n");
            assert_eq!(
                parse_provider_metrics("s", MetricsSource::Sglang, &body, 1).cache_hit_rate,
                None
            );
        }
    }

    #[test]
    fn missing_metrics_are_unavailable_not_zero() {
        let metrics = parse_provider_metrics("v", MetricsSource::Vllm, "", 1);
        assert_eq!(metrics.cache_hits, None);
        assert_eq!(metrics.cache_queries, None);
        assert_eq!(metrics.cache_hit_rate, None);
        assert_eq!(metrics.kv_cache_usage, None);
    }

    #[test]
    fn target_rejects_credentials_redirectable_paths_and_caller_parameters() {
        for raw in [
            "ftp://localhost/metrics",
            "http://user@localhost/metrics",
            "http://localhost/not-metrics",
            "http://localhost/metrics?url=http://evil",
        ] {
            assert!(
                ProviderMetricsTarget::from_operator_config(
                    "p",
                    Url::parse(raw).unwrap(),
                    MetricsSource::Vllm
                )
                .is_err(),
                "accepted {raw}"
            );
        }
        assert!(
            ProviderMetricsTarget::from_operator_config(
                "p",
                Url::parse("http://127.0.0.1:8000/metrics").unwrap(),
                MetricsSource::Vllm
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn failed_refresh_retains_last_good_sample_and_prunes_removed_providers() {
        let registry = ProviderMetricsRegistry::default();
        registry.record_sample(ProviderCacheMetrics {
            provider: "kept".into(),
            source: MetricsSource::Vllm,
            fetched_at_ms: 1,
            cache_hits: Some(2.0),
            cache_queries: Some(4.0),
            cache_hit_rate: Some(0.5),
            kv_cache_usage: None,
            data_quality: "derived",
        });
        registry.record_sample(ProviderCacheMetrics {
            provider: "removed".into(),
            source: MetricsSource::Sglang,
            fetched_at_ms: 1,
            cache_hits: None,
            cache_queries: None,
            cache_hit_rate: Some(0.2),
            kv_cache_usage: None,
            data_quality: "derived",
        });
        let target = ProviderMetricsTarget::from_operator_config(
            "kept",
            Url::parse("http://127.0.0.1:1/metrics").unwrap(),
            MetricsSource::Vllm,
        )
        .unwrap();
        let scraper = ProviderMetricsScraper::new(
            Duration::from_millis(50),
            Duration::from_millis(100),
            1024,
        )
        .unwrap();

        let report = registry.refresh(&scraper, &[target]).await;

        assert_eq!(
            report,
            ProviderMetricsRefresh {
                attempted: 1,
                updated: 0,
                failed: 1,
                skipped: 0,
            }
        );
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.providers.len(), 1);
        assert_eq!(snapshot.providers[0].provider, "kept");
        assert_eq!(snapshot.providers[0].cache_hit_rate, Some(0.5));
    }

    #[tokio::test]
    async fn dashboard_route_exposes_sorted_derived_samples_without_fabricated_zeros() {
        let registry = ProviderMetricsRegistry::default();
        registry.record_sample(parse_provider_metrics(
            "z-provider",
            MetricsSource::Sglang,
            "sglang:cache_hit_rate 0.25\n",
            8,
        ));
        registry.record_sample(parse_provider_metrics(
            "a-provider",
            MetricsSource::Vllm,
            "",
            7,
        ));
        let response = dashboard_routes::<()>(registry)
            .oneshot(
                Request::builder()
                    .uri("/dashboard/api/provider-metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["providers"][0]["provider"], "a-provider");
        assert!(value["providers"][0]["cache_hit_rate"].is_null());
        assert_eq!(value["providers"][0]["data_quality"], "derived");
        assert_eq!(value["providers"][1]["provider"], "z-provider");
        assert_eq!(value["providers"][1]["cache_hit_rate"], 0.25);
    }
}
