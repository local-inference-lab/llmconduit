//! Upstream engine metrics: scrape each backend's Prometheus `/metrics` and
//! keep a compact per-model sample.
//!
//! vLLM (V1 engine) and SGLang expose the families below (verified against
//! their sources on 2026-09-12; see `docs/design/sessions-and-content-store.md`
//! for the fact sheet). The parser is engine-agnostic: it reads the text
//! exposition format, keeps the families listed in [`FAMILIES`], and folds every
//! label except `model_name` (and the few we keep as sub-keys) into one value
//! per model. Counters and histogram `_sum`/`_count` are stored raw so a
//! consumer derives rates from consecutive samples; gauges are stored as-is.
//!
//! Samples are persisted through the same `backend_metrics` table as the
//! gateway-side health samples, tagged `"kind": "upstream"`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// `control_plane.metrics` section.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricsBootstrap {
    /// Scrape every backend that answers `/metrics` (derived from its base
    /// URL by stripping a trailing `/v1`). Backends without a metrics endpoint
    /// are skipped after the first failure until the next retry window.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Seconds between scrapes of one backend.
    #[serde(default = "default_scrape_interval_secs")]
    pub scrape_interval_secs: u64,
    /// Per-backend overrides keyed by backend/provider name.
    #[serde(default)]
    pub backends: BTreeMap<String, BackendMetricsConfig>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, serde_yaml::Value>,
}

impl Default for MetricsBootstrap {
    fn default() -> Self {
        Self {
            enabled: true,
            scrape_interval_secs: default_scrape_interval_secs(),
            backends: BTreeMap::new(),
            extra: BTreeMap::new(),
        }
    }
}

impl MetricsBootstrap {
    pub fn is_default(value: &Self) -> bool {
        *value == Self::default()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct BackendMetricsConfig {
    /// Explicit metrics URL; overrides the base-URL derivation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// `false` disables scraping this backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
}

const fn default_true() -> bool {
    true
}

// Dashboard trends need minute-level resolution at most; a faster default
// spends burst CPU and SQL writes on small hubs for no visible gain.
const fn default_scrape_interval_secs() -> u64 {
    60
}

/// Derive the Prometheus endpoint from an OpenAI-compatible base URL:
/// `http://host:8000/v1` → `http://host:8000/metrics`.
pub fn derive_metrics_url(base_url: &str) -> Option<String> {
    let trimmed = base_url.trim().trim_end_matches('/');
    let root = trimmed
        .strip_suffix("/v1")
        .or_else(|| trimmed.strip_suffix("/openai/v1"))
        .unwrap_or(trimmed);
    if !(root.starts_with("http://") || root.starts_with("https://")) {
        return None;
    }
    Some(format!("{root}/metrics"))
}

/// Which engine a sample came from, detected from the metric prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Engine {
    Vllm,
    Sglang,
    Unknown,
}

impl Engine {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vllm => "vllm",
            Self::Sglang => "sglang",
            Self::Unknown => "unknown",
        }
    }
}

/// How the values of one family fold across the labels we do not keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fold {
    /// Counters and histogram sums/counts: add.
    Sum,
    /// Fractions/rates that are per-engine or per-rank: take the largest.
    Max,
}

/// One family we keep, with the neutral key it is stored under.
struct Family {
    /// Exposition name (after `prometheus_client` appends `_total` to counters).
    name: &'static str,
    key: &'static str,
    fold: Fold,
    /// A label whose value becomes a sub-key (`realtime_tokens_total{mode}`).
    sub_label: Option<&'static str>,
}

/// The verified families, mapped to engine-neutral keys.
const FAMILIES: &[Family] = &[
    // --- vLLM V1 ---
    Family {
        name: "vllm:num_requests_running",
        key: "running",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:num_requests_waiting",
        key: "waiting",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:kv_cache_usage_perc",
        key: "kv_usage",
        fold: Fold::Max,
        sub_label: None,
    },
    Family {
        name: "vllm:prompt_tokens_total",
        key: "prompt_tokens_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:generation_tokens_total",
        key: "generation_tokens_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:prompt_tokens_cached_total",
        key: "cached_prompt_tokens_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:prefix_cache_queries_total",
        key: "prefix_cache_queries_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:prefix_cache_hits_total",
        key: "prefix_cache_hits_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:time_to_first_token_seconds_sum",
        key: "ttft_seconds_sum",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:time_to_first_token_seconds_count",
        key: "ttft_count",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:inter_token_latency_seconds_sum",
        key: "itl_seconds_sum",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:inter_token_latency_seconds_count",
        key: "itl_count",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:e2e_request_latency_seconds_sum",
        key: "e2e_seconds_sum",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:e2e_request_latency_seconds_count",
        key: "e2e_count",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:request_prefill_time_seconds_sum",
        key: "prefill_seconds_sum",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:request_prefill_time_seconds_count",
        key: "prefill_count",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:request_decode_time_seconds_sum",
        key: "decode_seconds_sum",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:request_decode_time_seconds_count",
        key: "decode_count",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "vllm:request_success_total",
        key: "requests_finished_total",
        fold: Fold::Sum,
        sub_label: Some("finished_reason"),
    },
    Family {
        name: "vllm:num_preemptions_total",
        key: "preemptions_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    // --- SGLang ---
    Family {
        name: "sglang:num_running_reqs",
        key: "running",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:num_queue_reqs",
        key: "waiting",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:token_usage",
        key: "kv_usage",
        fold: Fold::Max,
        sub_label: None,
    },
    Family {
        name: "sglang:prompt_tokens_total",
        key: "prompt_tokens_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:generation_tokens_total",
        key: "generation_tokens_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:cached_tokens_total",
        key: "cached_prompt_tokens_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:cache_hit_rate",
        key: "cache_hit_rate",
        fold: Fold::Max,
        sub_label: None,
    },
    Family {
        name: "sglang:time_to_first_token_seconds_sum",
        key: "ttft_seconds_sum",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:time_to_first_token_seconds_count",
        key: "ttft_count",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:inter_token_latency_seconds_sum",
        key: "itl_seconds_sum",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:inter_token_latency_seconds_count",
        key: "itl_count",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:e2e_request_latency_seconds_sum",
        key: "e2e_seconds_sum",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:e2e_request_latency_seconds_count",
        key: "e2e_count",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:num_requests_total",
        key: "requests_finished_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:num_aborted_requests_total",
        key: "requests_aborted_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:num_retracted_requests_total",
        key: "preemptions_total",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:gen_throughput",
        key: "decode_tokens_per_sec",
        fold: Fold::Sum,
        sub_label: None,
    },
    Family {
        name: "sglang:realtime_tokens_total",
        key: "realtime_tokens_total",
        fold: Fold::Sum,
        sub_label: Some("mode"),
    },
    Family {
        name: "sglang:prefill_effective_tokens_total",
        key: "prefill_effective_tokens_total",
        fold: Fold::Sum,
        sub_label: Some("mode"),
    },
];

/// Per-model values: neutral key → value, or key → {sub-label value → value}.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelMetrics {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub values: BTreeMap<String, f64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub by_label: BTreeMap<String, BTreeMap<String, f64>>,
}

/// One scrape of one backend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpstreamMetricsSample {
    /// Discriminator against the gateway-side health samples in the same table.
    pub kind: String,
    pub engine: Engine,
    pub backend: String,
    pub scraped_at_ms: i64,
    /// Per `model_name`; `""` when the exposition carries no model label.
    pub models: BTreeMap<String, ModelMetrics>,
    /// Number of exposition lines that belonged to a kept family.
    pub kept_lines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// No known family found: not a vLLM/SGLang exposition (or metrics disabled).
    NoKnownFamilies,
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoKnownFamilies => formatter.write_str("no vLLM/SGLang metric families found"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse a Prometheus text exposition into a per-model sample.
pub fn parse_exposition(
    backend: &str,
    text: &str,
    scraped_at_ms: i64,
) -> Result<UpstreamMetricsSample, ParseError> {
    let mut models: BTreeMap<String, ModelMetrics> = BTreeMap::new();
    let mut engine = Engine::Unknown;
    let mut kept = 0usize;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, labels, value)) = split_sample(line) else {
            continue;
        };
        let Some(family) = FAMILIES.iter().find(|family| family.name == name) else {
            continue;
        };
        if engine == Engine::Unknown {
            engine = if name.starts_with("vllm:") {
                Engine::Vllm
            } else if name.starts_with("sglang:") {
                Engine::Sglang
            } else {
                Engine::Unknown
            };
        }
        let model = labels.get("model_name").cloned().unwrap_or_default();
        let entry = models.entry(model).or_default();
        match family.sub_label {
            Some(label) => {
                let sub = labels.get(label).cloned().unwrap_or_default();
                let slot = entry
                    .by_label
                    .entry(family.key.to_string())
                    .or_default()
                    .entry(sub)
                    .or_insert(0.0);
                fold_into(slot, value, family.fold);
            }
            None => {
                let slot =
                    entry
                        .values
                        .entry(family.key.to_string())
                        .or_insert(match family.fold {
                            Fold::Sum => 0.0,
                            Fold::Max => f64::MIN,
                        });
                fold_into(slot, value, family.fold);
            }
        }
        kept += 1;
    }
    if kept == 0 {
        return Err(ParseError::NoKnownFamilies);
    }
    for metrics in models.values_mut() {
        for value in metrics.values.values_mut() {
            if *value == f64::MIN {
                *value = 0.0;
            }
        }
    }
    Ok(UpstreamMetricsSample {
        kind: "upstream".to_string(),
        engine,
        backend: backend.to_string(),
        scraped_at_ms,
        models,
        kept_lines: kept,
    })
}

fn fold_into(slot: &mut f64, value: f64, fold: Fold) {
    match fold {
        Fold::Sum => *slot += value,
        Fold::Max => {
            if value > *slot {
                *slot = value;
            }
        }
    }
}

/// Split `name{labels} value [timestamp]` into its parts. Returns `None` for
/// malformed lines (never panics on operator-controlled input).
fn split_sample(line: &str) -> Option<(&str, BTreeMap<String, String>, f64)> {
    let (name_and_labels, rest) = match line.find(['{', ' ']) {
        Some(idx) if line.as_bytes()[idx] == b'{' => {
            let close = find_label_close(line, idx)?;
            (&line[..close + 1], line[close + 1..].trim())
        }
        Some(idx) => (&line[..idx], line[idx..].trim()),
        None => return None,
    };
    let (name, labels) = match name_and_labels.find('{') {
        Some(idx) => (
            &name_and_labels[..idx],
            parse_labels(&name_and_labels[idx + 1..name_and_labels.len() - 1]),
        ),
        None => (name_and_labels, BTreeMap::new()),
    };
    let value_text = rest.split_whitespace().next()?;
    let value = match value_text {
        "NaN" | "+Inf" | "-Inf" => return None,
        other => other.parse::<f64>().ok()?,
    };
    Some((name, labels, value))
}

/// Index of the `}` closing the label block that opens at `open`, honouring
/// quoted values (which may contain `}` or escaped quotes).
fn find_label_close(line: &str, open: usize) -> Option<usize> {
    let bytes = line.as_bytes();
    let mut in_quote = false;
    let mut escaped = false;
    for (offset, &byte) in bytes.iter().enumerate().skip(open + 1) {
        if in_quote {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_quote = false;
            }
            continue;
        }
        match byte {
            b'"' => in_quote = true,
            b'}' => return Some(offset),
            _ => {}
        }
    }
    None
}

fn parse_labels(text: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    let mut rest = text;
    while !rest.is_empty() {
        let Some(eq) = rest.find('=') else { break };
        let key = rest[..eq].trim();
        let after = &rest[eq + 1..];
        let Some(value_start) = after.find('"') else {
            break;
        };
        let value_body = &after[value_start + 1..];
        // Find the closing quote, skipping escaped ones.
        let mut end = None;
        let mut escaped = false;
        for (offset, ch) in value_body.char_indices() {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                end = Some(offset);
                break;
            }
        }
        let Some(end) = end else { break };
        let value = value_body[..end]
            .replace("\\\"", "\"")
            .replace("\\\\", "\\");
        labels.insert(key.to_string(), value);
        rest = value_body[end + 1..].trim_start_matches(',').trim_start();
    }
    labels
}

#[cfg(test)]
mod tests {
    use super::*;

    const VLLM: &str = r#"
# HELP vllm:num_requests_running Number of requests in model execution batches.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="Qwen3.5"} 3.0
vllm:num_requests_running{engine="1",model_name="Qwen3.5"} 2.0
vllm:num_requests_waiting{engine="0",model_name="Qwen3.5"} 1.0
vllm:kv_cache_usage_perc{engine="0",model_name="Qwen3.5"} 0.42
vllm:kv_cache_usage_perc{engine="1",model_name="Qwen3.5"} 0.61
# TYPE vllm:prompt_tokens counter
vllm:prompt_tokens_total{engine="0",model_name="Qwen3.5"} 120000.0
vllm:prompt_tokens_total{engine="1",model_name="Qwen3.5"} 80000.0
vllm:generation_tokens_total{engine="0",model_name="Qwen3.5"} 30000.0
vllm:generation_tokens_total{engine="1",model_name="Qwen3.5"} 15000.0
vllm:prompt_tokens_cached_total{engine="0",model_name="Qwen3.5"} 90000.0
vllm:prefix_cache_queries_total{engine="0",model_name="Qwen3.5"} 100000.0
vllm:prefix_cache_hits_total{engine="0",model_name="Qwen3.5"} 75000.0
# TYPE vllm:time_to_first_token_seconds histogram
vllm:time_to_first_token_seconds_bucket{engine="0",le="0.001",model_name="Qwen3.5"} 0.0
vllm:time_to_first_token_seconds_bucket{engine="0",le="+Inf",model_name="Qwen3.5"} 500.0
vllm:time_to_first_token_seconds_sum{engine="0",model_name="Qwen3.5"} 250.0
vllm:time_to_first_token_seconds_count{engine="0",model_name="Qwen3.5"} 500.0
vllm:inter_token_latency_seconds_sum{engine="0",model_name="Qwen3.5"} 600.0
vllm:inter_token_latency_seconds_count{engine="0",model_name="Qwen3.5"} 30000.0
vllm:e2e_request_latency_seconds_sum{engine="0",model_name="Qwen3.5"} 2000.0
vllm:e2e_request_latency_seconds_count{engine="0",model_name="Qwen3.5"} 500.0
vllm:request_prefill_time_seconds_sum{engine="0",model_name="Qwen3.5"} 100.0
vllm:request_prefill_time_seconds_count{engine="0",model_name="Qwen3.5"} 500.0
vllm:request_decode_time_seconds_sum{engine="0",model_name="Qwen3.5"} 1800.0
vllm:request_decode_time_seconds_count{engine="0",model_name="Qwen3.5"} 500.0
vllm:request_success_total{engine="0",finished_reason="stop",model_name="Qwen3.5"} 480.0
vllm:request_success_total{engine="0",finished_reason="length",model_name="Qwen3.5"} 20.0
vllm:num_preemptions_total{engine="0",model_name="Qwen3.5"} 2.0
vllm:cache_config_info{cache_dtype="auto"} 1.0
process_cpu_seconds_total 12.5
"#;

    const SGLANG: &str = r#"
sglang:num_running_reqs{engine_type="unified",model_name="glm",tp_rank="0",pp_rank="0",moe_ep_rank="0"} 4.0
sglang:num_queue_reqs{engine_type="unified",model_name="glm",tp_rank="0",pp_rank="0",moe_ep_rank="0"} 7.0
sglang:token_usage{engine_type="unified",model_name="glm",tp_rank="0",pp_rank="0",moe_ep_rank="0"} 0.83
sglang:cache_hit_rate{engine_type="unified",model_name="glm",tp_rank="0",pp_rank="0",moe_ep_rank="0"} 0.71
sglang:gen_throughput{engine_type="unified",model_name="glm",tp_rank="0",pp_rank="0",moe_ep_rank="0"} 1830.5
sglang:prompt_tokens_total{engine_type="unified",model_name="glm",is_streaming="true"} 50000.0
sglang:prompt_tokens_total{engine_type="unified",model_name="glm",is_streaming="false"} 5000.0
sglang:generation_tokens_total{engine_type="unified",model_name="glm",is_streaming="true"} 20000.0
sglang:cached_tokens_total{engine_type="unified",model_name="glm",cache_source="device"} 30000.0
sglang:cached_tokens_total{engine_type="unified",model_name="glm",cache_source="host"} 1000.0
sglang:time_to_first_token_seconds_sum{engine_type="unified",model_name="glm",is_streaming="true"} 40.0
sglang:time_to_first_token_seconds_count{engine_type="unified",model_name="glm",is_streaming="true"} 200.0
sglang:inter_token_latency_seconds_sum{engine_type="unified",model_name="glm"} 300.0
sglang:inter_token_latency_seconds_count{engine_type="unified",model_name="glm"} 20000.0
sglang:e2e_request_latency_seconds_sum{engine_type="unified",model_name="glm",is_streaming="true"} 900.0
sglang:e2e_request_latency_seconds_count{engine_type="unified",model_name="glm",is_streaming="true"} 200.0
sglang:num_requests_total{engine_type="unified",model_name="glm",is_streaming="true"} 200.0
sglang:num_aborted_requests_total{engine_type="unified",model_name="glm"} 3.0
sglang:realtime_tokens_total{engine_type="unified",model_name="glm",mode="prefill_compute"} 12000.0
sglang:realtime_tokens_total{engine_type="unified",model_name="glm",mode="prefill_cache"} 38000.0
sglang:realtime_tokens_total{engine_type="unified",model_name="glm",mode="decode"} 20000.0
sglang:prefill_effective_tokens_total{engine_type="unified",model_name="glm",mode="input"} 50000.0
sglang:prefill_effective_tokens_total{engine_type="unified",model_name="glm",mode="device_hit"} 30000.0
"#;

    #[test]
    fn derives_the_metrics_url_from_a_base_url() {
        assert_eq!(
            derive_metrics_url("http://127.0.0.1:8000/v1").as_deref(),
            Some("http://127.0.0.1:8000/metrics")
        );
        assert_eq!(
            derive_metrics_url("https://llm.example/v1/").as_deref(),
            Some("https://llm.example/metrics")
        );
        assert_eq!(
            derive_metrics_url("http://host:30000").as_deref(),
            Some("http://host:30000/metrics")
        );
        assert_eq!(derive_metrics_url("ftp://nope"), None);
    }

    #[test]
    fn vllm_exposition_folds_engines_and_keeps_finish_reasons() {
        let sample = parse_exposition("vllm-a", VLLM, 1_000).expect("parse");
        assert_eq!(sample.engine, Engine::Vllm);
        assert_eq!(sample.kind, "upstream");
        assert_eq!(sample.backend, "vllm-a");
        let qwen = &sample.models["Qwen3.5"];
        // Gauges summed across engines; fractions take the max.
        assert_eq!(qwen.values["running"], 5.0);
        assert_eq!(qwen.values["waiting"], 1.0);
        assert_eq!(qwen.values["kv_usage"], 0.61);
        // Counters summed across engines.
        assert_eq!(qwen.values["prompt_tokens_total"], 200_000.0);
        assert_eq!(qwen.values["generation_tokens_total"], 45_000.0);
        assert_eq!(qwen.values["cached_prompt_tokens_total"], 90_000.0);
        assert_eq!(qwen.values["prefix_cache_hits_total"], 75_000.0);
        assert_eq!(qwen.values["prefix_cache_queries_total"], 100_000.0);
        // Histogram sum/count kept raw (mean = 0.5 s TTFT here).
        assert_eq!(qwen.values["ttft_seconds_sum"], 250.0);
        assert_eq!(qwen.values["ttft_count"], 500.0);
        assert_eq!(qwen.values["prefill_seconds_sum"], 100.0);
        assert_eq!(qwen.values["decode_seconds_sum"], 1_800.0);
        assert_eq!(qwen.values["preemptions_total"], 2.0);
        // Buckets are ignored; the finish reasons are kept as sub-keys.
        assert!(!qwen.values.contains_key("ttft_bucket"));
        assert_eq!(qwen.by_label["requests_finished_total"]["stop"], 480.0);
        assert_eq!(qwen.by_label["requests_finished_total"]["length"], 20.0);
        // Unrelated families are ignored, and nothing lands under an empty model.
        assert!(!sample.models.contains_key(""));
        assert_eq!(sample.kept_lines, 25);
    }

    #[test]
    fn sglang_exposition_folds_streaming_and_rank_labels() {
        let sample = parse_exposition("sgl", SGLANG, 2_000).expect("parse");
        assert_eq!(sample.engine, Engine::Sglang);
        let glm = &sample.models["glm"];
        assert_eq!(glm.values["running"], 4.0);
        assert_eq!(glm.values["waiting"], 7.0);
        assert_eq!(glm.values["kv_usage"], 0.83);
        assert_eq!(glm.values["cache_hit_rate"], 0.71);
        assert_eq!(glm.values["decode_tokens_per_sec"], 1_830.5);
        assert_eq!(
            glm.values["prompt_tokens_total"], 55_000.0,
            "is_streaming folded"
        );
        assert_eq!(
            glm.values["cached_prompt_tokens_total"], 31_000.0,
            "cache_source folded"
        );
        assert_eq!(glm.values["requests_aborted_total"], 3.0);
        assert_eq!(
            glm.by_label["realtime_tokens_total"]["prefill_compute"],
            12_000.0
        );
        assert_eq!(glm.by_label["realtime_tokens_total"]["decode"], 20_000.0);
        assert_eq!(
            glm.by_label["prefill_effective_tokens_total"]["device_hit"],
            30_000.0
        );
    }

    #[test]
    fn unrelated_or_empty_expositions_are_rejected_and_malformed_lines_skipped() {
        assert_eq!(
            parse_exposition("x", "process_cpu_seconds_total 1\n", 0),
            Err(ParseError::NoKnownFamilies)
        );
        assert_eq!(
            parse_exposition("x", "", 0),
            Err(ParseError::NoKnownFamilies)
        );
        let messy = "vllm:num_requests_running{model_name=\"m\"} NaN\nvllm:num_requests_running{model_name=\"m\"\nvllm:num_requests_running{model_name=\"a\\\"b}\"} 2 1700000000\n";
        let sample = parse_exposition("x", messy, 0).expect("one good line");
        assert_eq!(sample.kept_lines, 1);
        assert_eq!(sample.models["a\"b}"].values["running"], 2.0);
    }

    #[test]
    fn sample_round_trips_through_json() {
        let sample = parse_exposition("vllm-a", VLLM, 1_000).unwrap();
        let encoded = serde_json::to_string(&sample).unwrap();
        let decoded: UpstreamMetricsSample = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, sample);
        assert!(encoded.contains("\"kind\":\"upstream\""));
        assert!(encoded.contains("\"engine\":\"vllm\""));
    }

    #[test]
    fn metrics_bootstrap_defaults_and_parses_overrides() {
        let config = MetricsBootstrap::default();
        assert!(config.enabled);
        assert_eq!(config.scrape_interval_secs, 60);
        assert!(MetricsBootstrap::is_default(&config));
        let parsed: MetricsBootstrap = serde_yaml::from_str(
            "scrape_interval_secs: 30\nbackends:\n  local: { url: 'http://x:9/metrics' }\n  litellm: { enabled: false }\n",
        )
        .unwrap();
        assert_eq!(parsed.scrape_interval_secs, 30);
        assert_eq!(
            parsed.backends["local"].url.as_deref(),
            Some("http://x:9/metrics")
        );
        assert_eq!(parsed.backends["litellm"].enabled, Some(false));
    }
}
