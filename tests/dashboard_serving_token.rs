//! D2 — per-flow serving-token identity, observed through a REAL gateway flow.
//!
//! The in-crate unit test (`upstream.rs` `concurrent_flows_have_independent_…`)
//! hand-mints two `ServingToken`s, so it proves the failover/routing REBUILD clones
//! the `Arc` forward — but it can NOT catch a regression where the ENGINE mints a
//! single CLIENT-WIDE token (the rev2 race), because the test never goes through
//! `stream_responses` (where the engine allocates the token).
//!
//! This file closes that gap: it drives TWO CONCURRENT real `Gateway::stream_responses`
//! flows over a recording upstream that captures the production `BackendChatRequest`'s
//! `Arc<ServingToken>` and tags its `{route, provider}` (mirroring the routing +
//! failover layers). A client-wide token would make the two captured `Arc`s
//! POINTER-EQUAL and let one flow's `{route, provider}` bleed into the other; the
//! assertions below (`!Arc::ptr_eq` + independent snapshots) fail in that case.

mod common;

use common::MockSearch;
use common::base_request;
use common::collect_stream;
use common::content_chunk;
use common::usage_chunk;
use common::user_message;

use async_trait::async_trait;
use llmconduit::engine::Gateway;
use llmconduit::error::AppError;
use llmconduit::replay::ReplayStore;
use llmconduit::upstream::BackendChatRequest;
use llmconduit::upstream::ServingToken;
use llmconduit::upstream::UpstreamClient;
use llmconduit::upstream::UpstreamModelEntry;
use llmconduit::upstream::UpstreamStream;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Barrier;

/// A recording upstream that, for every production `BackendChatRequest`, captures
/// the flow's `Arc<ServingToken>` and tags its `route`/`provider` (as the routing
/// and failover layers do in production). A `Barrier` holds BOTH flows' upstream
/// calls in-flight simultaneously BEFORE either yields chunks, so the tokens are
/// genuinely concurrent — a client-wide token would be observed as the SAME `Arc`.
#[derive(Clone)]
struct RecordingServingUpstream {
    captured: Arc<Mutex<Vec<Arc<ServingToken>>>>,
    /// Released once both concurrent flows have reached the upstream call.
    barrier: Arc<Barrier>,
    /// Distinct `{route, provider}` each call tags, keyed by call order.
    tags: Arc<Mutex<Vec<(&'static str, &'static str)>>>,
}

impl RecordingServingUpstream {
    fn new(tags: Vec<(&'static str, &'static str)>) -> Self {
        Self {
            captured: Arc::new(Mutex::new(Vec::new())),
            barrier: Arc::new(Barrier::new(tags.len())),
            tags: Arc::new(Mutex::new(tags)),
        }
    }
}

#[async_trait]
impl UpstreamClient for RecordingServingUpstream {
    async fn stream_chat_completion(
        &self,
        backend: &BackendChatRequest,
    ) -> Result<UpstreamStream, AppError> {
        // The production engine ALWAYS threads a serving token; capture this flow's.
        let serving = backend
            .serving
            .clone()
            .expect("production BackendChatRequest carries a serving token");
        let (route, provider) = self
            .tags
            .lock()
            .expect("tags lock")
            .pop()
            .expect("a tag per concurrent flow");
        // Tag THIS flow's token exactly as the routing (route) + failover (provider)
        // layers would. First-writer-wins, so this is the serving identity.
        serving.set_route(route);
        serving.set_provider(provider);
        self.captured.lock().expect("captured lock").push(serving);
        // Hold every concurrent flow here until ALL have captured + tagged, so the
        // tokens are live simultaneously (a shared client-wide token would have been
        // overwritten by the second flow by now).
        self.barrier.wait().await;
        let chunks = vec![
            Ok(content_chunk("chat-1", "hi")),
            Ok(usage_chunk("chat-1", 5, 1, 6)),
        ];
        Ok(Box::pin(futures::stream::iter(chunks)))
    }

    async fn list_models(&self) -> Result<reqwest::Response, AppError> {
        Err(AppError::internal("unused in this test"))
    }

    async fn supported_model_catalog(&self) -> Result<Vec<UpstreamModelEntry>, AppError> {
        Ok(vec![UpstreamModelEntry {
            id: "glm-5.1".to_string(),
            context_limit: None,
        }])
    }
}

fn gateway_with_recording_upstream(upstream: RecordingServingUpstream) -> Arc<Gateway> {
    let config = common::test_config();
    let image_cache = Arc::new(llmconduit::vision::ImageCache::from_config(&config));
    Arc::new(Gateway::new(
        config,
        ReplayStore::new(1000),
        Arc::new(upstream),
        Arc::new(MockSearch::default()),
        image_cache,
        llmconduit::monitor::MonitorHub::disabled(),
        None,
        llmconduit::dashboard_flow::DashboardFlowStore::disabled(),
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_gateway_flows_get_distinct_serving_tokens() {
    // Two REAL gateway flows, run concurrently. Each tags its OWN token; a
    // client-wide token (the rev2 race) would make the captured Arcs identical and
    // cross-contaminate `{route, provider}`.
    let upstream = RecordingServingUpstream::new(vec![
        // popped in reverse, so flow A (first started) takes ("route-b", …) is
        // irrelevant — the assertion is on DISTINCTNESS + independence, not order.
        ("route-b", "sglang-b"),
        ("route-a", "vllm-a"),
    ]);
    let gateway = gateway_with_recording_upstream(upstream.clone());

    let flow_a = {
        let gateway = Arc::clone(&gateway);
        tokio::spawn(async move {
            let request = base_request(vec![user_message("hello A")]);
            let stream = gateway.stream_responses(request).await.expect("stream A");
            collect_stream(stream).await;
        })
    };
    let flow_b = {
        let gateway = Arc::clone(&gateway);
        tokio::spawn(async move {
            let request = base_request(vec![user_message("hello B")]);
            let stream = gateway.stream_responses(request).await.expect("stream B");
            collect_stream(stream).await;
        })
    };
    flow_a.await.expect("flow A joins");
    flow_b.await.expect("flow B joins");

    let captured = upstream.captured.lock().expect("captured lock");
    assert_eq!(captured.len(), 2, "one serving token captured per flow");

    // Distinct Arc identities — the crux: a client-wide token would be pointer-equal.
    assert!(
        !Arc::ptr_eq(&captured[0], &captured[1]),
        "each concurrent flow must mint its OWN Arc<ServingToken> (a shared \
         client-wide token is the rev2 cross-flow race)"
    );

    // Each token holds ONLY its own {route, provider} — no cross-flow bleed.
    let mut snapshots: Vec<(Option<String>, Option<String>)> =
        captured.iter().map(|token| token.snapshot()).collect();
    snapshots.sort();
    assert_eq!(
        snapshots,
        vec![
            (Some("route-a".to_string()), Some("vllm-a".to_string())),
            (Some("route-b".to_string()), Some("sglang-b".to_string())),
        ],
        "the two flows carry independent serving identities (no overwrite)"
    );
}
