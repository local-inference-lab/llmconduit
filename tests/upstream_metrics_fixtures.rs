//! The scraper's family table against real vLLM expositions captured on
//! 2026-09-12 from two production engines (vLLM 0.11.2 serving GLM-5.2-NVFP4
//! and vLLM 0.26.1 serving GLM-5.3-Flash-NVFP4). Guards the names the
//! Throughput view depends on across vLLM versions.

use llmconduit::upstream_metrics::{Engine, parse_exposition};

fn assert_real_exposition(path: &str, model: &str) {
    let text = std::fs::read_to_string(path).expect("fixture");
    let sample = parse_exposition("engine", &text, 1).expect("parses");
    assert_eq!(sample.engine, Engine::Vllm);
    let metrics = sample
        .models
        .get(model)
        .unwrap_or_else(|| panic!("model {model} present"));
    for key in [
        "running",
        "waiting",
        "kv_usage",
        "prompt_tokens_total",
        "generation_tokens_total",
        "cached_prompt_tokens_total",
        "prefix_cache_queries_total",
        "prefix_cache_hits_total",
        "ttft_seconds_sum",
        "ttft_count",
        "itl_seconds_sum",
        "itl_count",
        "e2e_seconds_sum",
        "e2e_count",
        "prefill_seconds_sum",
        "prefill_count",
        "decode_seconds_sum",
        "decode_count",
        "preemptions_total",
    ] {
        assert!(metrics.values.contains_key(key), "{path}: missing {key}");
    }
    assert!(
        metrics.by_label.contains_key("requests_finished_total"),
        "{path}: finish reasons"
    );
    assert!(
        sample.kept_lines >= 19,
        "{path}: kept {}",
        sample.kept_lines
    );
    let kv = metrics.values["kv_usage"];
    assert!((0.0..=1.0).contains(&kv), "{path}: kv usage fraction {kv}");
}

#[test]
fn vllm_0_11_glm_5_2_exposition_parses() {
    assert_real_exposition(
        "tests/fixtures/vllm-0.11.2-glm-5.2-metrics.txt",
        "GLM-5.2-NVFP4",
    );
}

#[test]
fn vllm_0_26_glm_5_3_flash_exposition_parses() {
    assert_real_exposition(
        "tests/fixtures/vllm-0.26.1-glm-5.3-flash-metrics.txt",
        "GLM-5.3-Flash-NVFP4",
    );
}
