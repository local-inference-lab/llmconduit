//! OpenRouter endpoint-pricing import.
//!
//! OpenRouter documents endpoint prices as decimal USD-per-token strings. They
//! are converted directly to integer nanodollars; binary floating point is never
//! persisted and malformed/non-finite values cannot enter the snapshot table.

use crate::config::ModelPrice;
use futures::StreamExt;
use rusqlite::{Connection, params};
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

pub const OPENROUTER_SOURCE: &str = "openrouter";
pub const OPENROUTER_SOURCE_URL: &str = "https://openrouter.ai/api/v1";
pub const OPENROUTER_PRICE_BODY_LIMIT: usize = 2 * 1024 * 1024;
pub const IMPORTED_PRICE_TABLE: &str = "auth_imported_price_snapshots";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NanoUsd(i64);

impl NanoUsd {
    pub const ZERO: Self = Self(0);

    pub fn new(value: i64) -> Option<Self> {
        (value >= 0).then_some(Self(value))
    }

    pub fn get(self) -> i64 {
        self.0
    }

    /// Parse a non-negative plain decimal USD value at nanodollar precision.
    /// Scientific notation, signs, NaN/Inf, and excess non-zero precision are
    /// rejected instead of being rounded through an `f64`.
    pub fn parse_usd(value: &str) -> Result<Self, PricingError> {
        let value = value.trim();
        if value.is_empty() || value.starts_with('+') || value.starts_with('-') {
            return Err(PricingError::InvalidPrice(value.to_string()));
        }
        let mut parts = value.split('.');
        let whole = parts.next().unwrap_or_default();
        let fraction = parts.next().unwrap_or_default();
        if parts.next().is_some()
            || whole.is_empty()
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(PricingError::InvalidPrice(value.to_string()));
        }
        let (kept, excess) = fraction.split_at(fraction.len().min(9));
        if excess.bytes().any(|byte| byte != b'0') {
            return Err(PricingError::PrecisionLoss(value.to_string()));
        }
        let whole: i64 = whole
            .parse()
            .map_err(|_| PricingError::InvalidPrice(value.to_string()))?;
        let mut fractional = kept.to_string();
        fractional.extend(std::iter::repeat_n('0', 9 - kept.len()));
        let fractional: i64 = if fractional.is_empty() {
            0
        } else {
            fractional
                .parse()
                .map_err(|_| PricingError::InvalidPrice(value.to_string()))?
        };
        let nanos = whole
            .checked_mul(1_000_000_000)
            .and_then(|base| base.checked_add(fractional))
            .ok_or_else(|| PricingError::InvalidPrice(value.to_string()))?;
        Ok(Self(nanos))
    }

    pub fn dollars_per_1k(self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PricingError {
    #[error("invalid OpenRouter price: {0}")]
    InvalidPrice(String),
    #[error("OpenRouter price has precision below one nanodollar: {0}")]
    PrecisionLoss(String),
    #[error("OpenRouter pricing response has no usable endpoints")]
    NoUsableEndpoints,
    #[error("invalid OpenRouter pricing response: {0}")]
    InvalidResponse(#[from] serde_json::Error),
    #[error("pricing database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("OpenRouter request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("OpenRouter returned HTTP {0}")]
    Http(reqwest::StatusCode),
    #[error("OpenRouter response exceeds the configured size limit")]
    BodyTooLarge,
    #[error("model id must contain an author and slug")]
    InvalidModelId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointPrice {
    pub endpoint_id: String,
    pub prompt: NanoUsd,
    pub completion: NanoUsd,
    pub cached: Option<NanoUsd>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceRange {
    pub min: NanoUsd,
    pub mean: NanoUsd,
    pub max: NanoUsd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedModelPrice {
    pub model_id: String,
    pub endpoints: Vec<EndpointPrice>,
    pub prompt: PriceRange,
    pub completion: PriceRange,
    pub cached: Option<PriceRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenRouterPriceSnapshot {
    pub source_url: String,
    pub fetched_at_ms: i64,
    pub price: ImportedModelPrice,
}

#[derive(Clone)]
pub struct OpenRouterPricingClient {
    client: reqwest::Client,
    base_url: Url,
    response_limit: usize,
    total_timeout: Duration,
}

impl OpenRouterPricingClient {
    pub fn new(total_timeout: Duration, response_limit: usize) -> Result<Self, PricingError> {
        Self::with_base_url(
            total_timeout,
            response_limit,
            Url::parse(OPENROUTER_SOURCE_URL).expect("static OpenRouter URL"),
        )
    }

    fn with_base_url(
        total_timeout: Duration,
        response_limit: usize,
        base_url: Url,
    ) -> Result<Self, PricingError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            base_url,
            response_limit,
            total_timeout,
        })
    }

    /// Fetch the official per-endpoint pricing for one model. This is intended for
    /// explicit operator/admin sync actions, never an inference hot-path request.
    pub async fn fetch_model(
        &self,
        model_id: &str,
        management_key: &str,
    ) -> Result<OpenRouterPriceSnapshot, PricingError> {
        let (author, slug) = model_id
            .split_once('/')
            .filter(|(author, slug)| !author.is_empty() && !slug.is_empty())
            .ok_or(PricingError::InvalidModelId)?;
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .expect("OpenRouter base URL can accept path segments")
            .extend(["models", author, slug, "endpoints"]);
        let response = self
            .client
            .get(url.clone())
            .bearer_auth(management_key)
            .timeout(self.total_timeout)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(PricingError::Http(response.status()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.response_limit as u64)
        {
            return Err(PricingError::BodyTooLarge);
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if body.len().saturating_add(chunk.len()) > self.response_limit {
                return Err(PricingError::BodyTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(OpenRouterPriceSnapshot {
            source_url: url.into(),
            fetched_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX),
            price: parse_endpoint_prices(&body)?,
        })
    }
}

impl Default for OpenRouterPricingClient {
    fn default() -> Self {
        Self::new(Duration::from_secs(15), OPENROUTER_PRICE_BODY_LIMIT)
            .expect("static OpenRouter pricing client configuration")
    }
}

impl ImportedModelPrice {
    /// The documented imported-average price. Operator overrides must be checked
    /// before calling this conversion.
    pub fn mean_model_price(&self) -> ModelPrice {
        match self.cached {
            Some(cached) => ModelPrice::new(
                self.prompt.mean.dollars_per_1k(),
                self.completion.mean.dollars_per_1k(),
                cached.mean.dollars_per_1k(),
            ),
            None => ModelPrice::without_cached(
                self.prompt.mean.dollars_per_1k(),
                self.completion.mean.dollars_per_1k(),
            ),
        }
    }
}

pub fn resolve_price(
    operator_override: Option<ModelPrice>,
    imported: Option<&ImportedModelPrice>,
) -> Option<ModelPrice> {
    operator_override.or_else(|| imported.map(ImportedModelPrice::mean_model_price))
}

#[derive(Debug, Deserialize)]
struct EndpointResponse {
    data: EndpointData,
}

#[derive(Debug, Deserialize)]
struct EndpointData {
    id: String,
    #[serde(default)]
    endpoints: Vec<RawEndpoint>,
}

#[derive(Debug, Deserialize)]
struct RawEndpoint {
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    provider_name: Option<String>,
    #[serde(default)]
    name: Option<String>,
    pricing: RawPricing,
}

#[derive(Debug, Deserialize)]
struct RawPricing {
    prompt: String,
    completion: String,
    #[serde(default, alias = "cache_read")]
    input_cache_read: Option<String>,
}

pub fn parse_endpoint_prices(body: &[u8]) -> Result<ImportedModelPrice, PricingError> {
    let response: EndpointResponse = serde_json::from_slice(body)?;
    if response.data.id.trim().is_empty() {
        return Err(PricingError::InvalidPrice("blank model id".to_string()));
    }
    let mut endpoints = Vec::with_capacity(response.data.endpoints.len());
    for (index, raw) in response.data.endpoints.into_iter().enumerate() {
        let endpoint_id = raw
            .tag
            .or(raw.provider_name)
            .or(raw.name)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("endpoint-{index}"));
        endpoints.push(EndpointPrice {
            endpoint_id,
            prompt: NanoUsd::parse_usd(&raw.pricing.prompt)?,
            completion: NanoUsd::parse_usd(&raw.pricing.completion)?,
            cached: raw
                .pricing
                .input_cache_read
                .as_deref()
                .map(NanoUsd::parse_usd)
                .transpose()?,
        });
    }
    if endpoints.is_empty() {
        return Err(PricingError::NoUsableEndpoints);
    }
    let prompt = range(endpoints.iter().map(|price| price.prompt)).unwrap();
    let completion = range(endpoints.iter().map(|price| price.completion)).unwrap();
    // A partial cache-price set is not a trustworthy model-wide average. Keep it
    // unavailable rather than treating missing endpoints as free cache reads.
    let cached = endpoints
        .iter()
        .map(|price| price.cached)
        .collect::<Option<Vec<_>>>()
        .and_then(|values| range(values.into_iter()));
    Ok(ImportedModelPrice {
        model_id: response.data.id,
        endpoints,
        prompt,
        completion,
        cached,
    })
}

fn range(values: impl Iterator<Item = NanoUsd>) -> Option<PriceRange> {
    let values: Vec<_> = values.collect();
    let min = values.iter().copied().min()?;
    let max = values.iter().copied().max()?;
    let sum: i128 = values.iter().map(|value| i128::from(value.get())).sum();
    let count = i128::try_from(values.len()).ok()?;
    let mean = i64::try_from((sum + count / 2) / count).ok()?;
    Some(PriceRange {
        min,
        mean: NanoUsd(mean),
        max,
    })
}

pub fn migrate_pricing_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS auth_imported_price_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_url TEXT NOT NULL,
            fetched_at_ms INTEGER NOT NULL,
            model_id TEXT NOT NULL,
            endpoint_id TEXT NOT NULL,
            prompt_nano_usd_per_token INTEGER NOT NULL,
            completion_nano_usd_per_token INTEGER NOT NULL,
            cached_nano_usd_per_token INTEGER,
            prompt_min_nano_usd INTEGER NOT NULL,
            prompt_mean_nano_usd INTEGER NOT NULL,
            prompt_max_nano_usd INTEGER NOT NULL,
            completion_min_nano_usd INTEGER NOT NULL,
            completion_mean_nano_usd INTEGER NOT NULL,
            completion_max_nano_usd INTEGER NOT NULL,
            cached_min_nano_usd INTEGER,
            cached_mean_nano_usd INTEGER,
            cached_max_nano_usd INTEGER,
            confidence TEXT NOT NULL CHECK(confidence IN ('imported', 'operator')),
            UNIQUE(source, fetched_at_ms, model_id, endpoint_id)
        );
        CREATE INDEX IF NOT EXISTS auth_price_model_fetched_idx
            ON auth_imported_price_snapshots(model_id, fetched_at_ms DESC);",
    )
}

pub fn persist_imported_price(
    conn: &mut Connection,
    snapshot: &OpenRouterPriceSnapshot,
) -> Result<usize, PricingError> {
    let price = &snapshot.price;
    let tx = conn.transaction()?;
    let mut inserted = 0;
    for endpoint in &price.endpoints {
        inserted += tx.execute(
            "INSERT OR IGNORE INTO auth_imported_price_snapshots (
                source, source_url, fetched_at_ms, model_id, endpoint_id,
                prompt_nano_usd_per_token, completion_nano_usd_per_token,
                cached_nano_usd_per_token, prompt_min_nano_usd,
                prompt_mean_nano_usd, prompt_max_nano_usd,
                completion_min_nano_usd, completion_mean_nano_usd,
                completion_max_nano_usd, cached_min_nano_usd,
                cached_mean_nano_usd, cached_max_nano_usd, confidence
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, 'imported')",
            params![
                OPENROUTER_SOURCE,
                snapshot.source_url,
                snapshot.fetched_at_ms,
                price.model_id,
                endpoint.endpoint_id,
                endpoint.prompt.get(),
                endpoint.completion.get(),
                endpoint.cached.map(NanoUsd::get),
                price.prompt.min.get(),
                price.prompt.mean.get(),
                price.prompt.max.get(),
                price.completion.min.get(),
                price.completion.mean.get(),
                price.completion.max.get(),
                price.cached.map(|range| range.min.get()),
                price.cached.map(|range| range.mean.get()),
                price.cached.map(|range| range.max.get()),
            ],
        )?;
    }
    tx.commit()?;
    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const RESPONSE: &[u8] = br#"{
      "data": {"id":"openai/gpt-4", "endpoints":[
        {"tag":"a", "pricing":{"prompt":"0.000000030", "completion":"0.000000060", "input_cache_read":"0.000000010"}},
        {"tag":"b", "pricing":{"prompt":"0.000000050", "completion":"0.000000100", "input_cache_read":"0.000000020"}}
      ]}
    }"#;

    #[test]
    fn parses_exact_nanos_and_derives_ranges() {
        let price = parse_endpoint_prices(RESPONSE).unwrap();
        assert_eq!(price.prompt.min.get(), 30);
        assert_eq!(price.prompt.mean.get(), 40);
        assert_eq!(price.prompt.max.get(), 50);
        assert_eq!(price.completion.mean.get(), 80);
        assert_eq!(price.cached.unwrap().mean.get(), 15);
    }

    #[test]
    fn rejects_malformed_nonfinite_negative_and_precision_loss() {
        for value in ["NaN", "inf", "-0.1", "1e-6", "0.0000000001"] {
            assert!(NanoUsd::parse_usd(value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn operator_override_wins_and_unknown_is_unavailable() {
        let imported = parse_endpoint_prices(RESPONSE).unwrap();
        let operator = ModelPrice::new(1.0, 2.0, 0.5);
        assert_eq!(
            resolve_price(Some(operator), Some(&imported)),
            Some(operator)
        );
        assert_eq!(resolve_price(None, None), None);
    }

    #[test]
    fn snapshot_persistence_is_idempotent_and_decimal_free() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate_pricing_schema(&conn).unwrap();
        let price = parse_endpoint_prices(RESPONSE).unwrap();
        let snapshot = OpenRouterPriceSnapshot {
            source_url: "https://openrouter.ai/api/v1/models/openai/gpt-4/endpoints".to_string(),
            fetched_at_ms: 123,
            price,
        };
        assert_eq!(persist_imported_price(&mut conn, &snapshot).unwrap(), 2);
        assert_eq!(persist_imported_price(&mut conn, &snapshot).unwrap(), 0);
        let row: (i64, i64, String) = conn
            .query_row(
                "SELECT prompt_nano_usd_per_token, prompt_mean_nano_usd, confidence FROM auth_imported_price_snapshots WHERE endpoint_id='a'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(row, (30, 40, "imported".to_string()));
    }

    #[test]
    fn partial_cache_prices_remain_unavailable() {
        let body = br#"{"data":{"id":"m","endpoints":[
          {"tag":"a","pricing":{"prompt":"0.1","completion":"0.2","input_cache_read":"0.01"}},
          {"tag":"b","pricing":{"prompt":"0.1","completion":"0.2"}}
        ]}}"#;
        assert!(parse_endpoint_prices(body).unwrap().cached.is_none());
    }

    async fn test_client(
        server: &MockServer,
        timeout: Duration,
        limit: usize,
    ) -> OpenRouterPricingClient {
        OpenRouterPricingClient::with_base_url(
            timeout,
            limit,
            Url::parse(&format!("{}/api/v1", server.uri())).unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn fetch_model_enforces_auth_status_body_limit_and_timeout() {
        let status_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/models/openai/gpt-4/endpoints"))
            .and(header("authorization", "Bearer management-secret"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&status_server)
            .await;
        let error = test_client(&status_server, Duration::from_secs(1), 1024)
            .await
            .fetch_model("openai/gpt-4", "management-secret")
            .await
            .unwrap_err();
        assert!(
            matches!(error, PricingError::Http(status) if status == reqwest::StatusCode::SERVICE_UNAVAILABLE)
        );

        let body_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 65]))
            .mount(&body_server)
            .await;
        let error = test_client(&body_server, Duration::from_secs(1), 64)
            .await
            .fetch_model("openai/gpt-4", "management-secret")
            .await
            .unwrap_err();
        assert!(matches!(error, PricingError::BodyTooLarge));

        let timeout_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(100))
                    .set_body_bytes(RESPONSE),
            )
            .mount(&timeout_server)
            .await;
        let error = test_client(&timeout_server, Duration::from_millis(10), 1024)
            .await
            .fetch_model("openai/gpt-4", "management-secret")
            .await
            .unwrap_err();
        assert!(matches!(error, PricingError::Request(ref request) if request.is_timeout()));
    }
}
