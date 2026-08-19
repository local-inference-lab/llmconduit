//! D4 — `provider_health` accessor + versioned topology publication, observed
//! through REAL upstream clients + the `Gateway` publication task.
//!
//! The in-crate unit tests (`upstream.rs` `d4_provider_health_tests`) cover the
//! DTO shape, status logic, `consecutive_failures` reset, the no-torn catalog
//! meta pair, and the publisher's monotonic versioning by driving the private
//! `mark_failure`/`mark_provider_success` paths directly. This file closes the
//! gap those cannot reach from outside the crate: the IDLE cooling→Healthy flip
//! published by the `Gateway`'s real publication task (1 s tick + cooldown-
//! deadline wake) with ZERO traffic after the provider entered cooldown - a
//! provider is driven into cooldown by a real failing upstream POST, then
//! recovers purely on the deadline wake.

mod common;

use common::test_config;
use llmconduit::engine::Gateway;
use llmconduit::models::chat::ChatCompletionRequest;
use llmconduit::models::chat::ChatMessage;
use llmconduit::replay::ReplayStore;
use llmconduit::upstream::BackendChatRequest;
use llmconduit::upstream::FailoverUpstreamProvider;
use llmconduit::upstream::ProviderStatus;
use llmconduit::upstream::ReqwestUpstreamClient;
use llmconduit::upstream::UpstreamClient;
use serde_json::Map as JsonMap;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

fn leaf(base: &str) -> ReqwestUpstreamClient {
    ReqwestUpstreamClient::new(
        reqwest::Client::new(),
        base.parse().expect("url"),
        None,
        None,
        true,
        4096,
    )
}

fn empty_request() -> ChatCompletionRequest {
    ChatCompletionRequest {
        model: "m".to_string(),
        messages: vec![ChatMessage {
            role: "user".to_string(),
            content: Some(Value::String("hi".to_string())),
            tool_call_id: None,
            name: None,
            reasoning_content: None,
            thinking: None,
            tool_calls: None,
        }],
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
    }
}

/// Build a `Gateway` whose upstream is `upstream`, with everything else a cheap
/// default (the publication task only touches `upstream` + the publisher).
fn gateway_with_upstream(upstream: Arc<dyn UpstreamClient>) -> Arc<Gateway> {
    let config = test_config();
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    let search = Arc::new(llmconduit::search::BraveSearchClient::new(
        reqwest::Client::new(),
        config.clone(),
    ));
    Arc::new(Gateway::new(
        config,
        ReplayStore::new(16),
        upstream,
        search,
        image_cache,
        llmconduit::monitor::MonitorHub::disabled(),
        None,
        llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
    ))
}

/// `Gateway::upstream_health()` surfaces the underlying client's health; a bare
/// single-leaf gateway has no provider layer and reports an empty vector.
#[tokio::test]
async fn gateway_upstream_health_delegates_and_bare_is_empty() {
    let bare = gateway_with_upstream(Arc::new(leaf("https://example.invalid/v1")));
    assert!(
        bare.upstream_health().is_empty(),
        "a bare leaf has no provider health"
    );

    let failover = llmconduit::upstream::FailoverUpstreamClient::new(
        vec![
            FailoverUpstreamProvider::new(
                "primary",
                leaf("https://a.invalid/v1"),
                None,
                JsonMap::new(),
            ),
            FailoverUpstreamProvider::new(
                "backup",
                leaf("https://b.invalid/v1"),
                None,
                JsonMap::new(),
            ),
        ],
        Duration::from_secs(30),
    );
    let gateway = gateway_with_upstream(Arc::new(failover));
    let health = gateway.upstream_health();
    assert_eq!(health.len(), 2);
    assert_eq!(health[0].id, "primary");
    assert_eq!(health[1].id, "backup");
}

/// THE idle-flip acceptance test: a provider is driven into cooldown by a real
/// failing upstream POST, then — with ZERO further traffic — the Gateway's
/// publication task flips it Cooling→Healthy at the cooldown deadline via the
/// deadline-wake path (not a per-request republish). A SHORT real cooldown
/// exercises the sub-second deadline wake. (The cooldown clock is a monotonic
/// `std::time::Instant`, which a tokio paused clock cannot advance, so this uses
/// real time on a multi-thread runtime — the faithful end-to-end proof.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_cooling_provider_flips_to_healthy_via_deadline_wake() {
    let server = MockServer::start().await;
    // The upstream 503s, so the first (and only) request fails over off this
    // single provider and marks it cooling.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .mount(&server)
        .await;

    let cooldown = Duration::from_millis(120);
    let failover = llmconduit::upstream::FailoverUpstreamClient::new(
        vec![FailoverUpstreamProvider::new(
            "primary",
            leaf(&format!("{}/v1/", server.uri())),
            None,
            JsonMap::new(),
        )],
        cooldown,
    );
    let gateway = gateway_with_upstream(Arc::new(failover));

    // Drive ONE real failing request so the provider enters cooldown. The whole
    // chain fails (single provider), which is expected.
    let backend = BackendChatRequest::new(empty_request(), None, None, None);
    let _ = gateway
        .upstream_client()
        .stream_chat_completion(&backend)
        .await;

    // The provider is now cooling (Down/Cooling — one failure → Cooling).
    let cooling = gateway.upstream_health();
    assert_eq!(cooling.len(), 1);
    assert_eq!(
        cooling[0].status,
        ProviderStatus::Cooling,
        "a single failure puts the provider in cooldown"
    );
    assert!(cooling[0].cooling_until_ms.is_some());

    // Start the publication task. From here we make NO further requests — the
    // flip must come purely from the cooldown-deadline wake.
    let handle = gateway.spawn_provider_health_publisher();
    let publisher = gateway.provider_health_publisher();

    // Poll the PUBLISHED snapshot (not a fresh on-demand read) until it reports
    // Healthy, proving the task republished the idle transition. Bound the wait
    // generously; the deadline is ~120 ms out.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut flipped = None;
    while Instant::now() < deadline {
        let snapshot = publisher.latest();
        if let Some(provider) = snapshot.providers.first()
            && provider.status == ProviderStatus::Healthy
        {
            flipped = Some(snapshot.version);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.abort();

    let version = flipped.expect("publisher flips the idle provider to Healthy with no traffic");
    assert!(
        version >= 1,
        "the flip was published as a versioned snapshot"
    );
    // The published Healthy entry has no cooldown deadline.
    let final_snapshot = publisher.latest();
    let provider = &final_snapshot.providers[0];
    assert_eq!(provider.status, ProviderStatus::Healthy);
    assert_eq!(provider.cooling_until_ms, None);
    assert_eq!(
        provider.served_count, 0,
        "the flip required NO served traffic — purely the deadline wake"
    );
    // A failure WAS recorded (cumulative), proving the entry is the same provider.
    assert!(provider.failover_count >= 1);
}
