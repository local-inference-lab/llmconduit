use crate::mesh::store::JoinKeyDecision;
use crate::mesh::store::MeshNodeRecord;
use crate::mesh::store::MeshStore;
use crate::mesh::store::NodeAuthorization;
use crate::mesh::store::StoreError;
use crate::mesh::store::now_ms;
use iroh::EndpointId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

/// `last_seen_at_ms` is an operator hint ("when did this node last talk to
/// us"), not a liveness signal — the registry tracks liveness in memory. A
/// coarse write cadence keeps heartbeats from turning into a SQLite write
/// per frame per worker.
pub(crate) const NODE_SEEN_WRITE_INTERVAL: Duration = Duration::from_secs(30);
/// Bounds the touch-throttle map even if many distinct endpoints connect.
const MAX_TRACKED_SEEN_ENDPOINTS: usize = 4096;

#[derive(Debug, Clone)]
pub struct MeshAuthorizer {
    store: MeshStore,
    last_seen_written: Arc<Mutex<HashMap<EndpointId, Instant>>>,
}

impl MeshAuthorizer {
    pub fn new(store: MeshStore) -> Self {
        Self {
            store,
            last_seen_written: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn enroll(
        &self,
        join_key: &str,
        endpoint_id: EndpointId,
        node_label: Option<String>,
    ) -> Result<JoinKeyDecision, StoreError> {
        self.store
            .validate_join_key_for_endpoint(
                join_key,
                &endpoint_id.to_string(),
                node_label,
                now_ms(),
            )
            .await
    }

    pub async fn authorize_worker(&self, endpoint_id: EndpointId) -> Result<bool, StoreError> {
        Ok(self
            .worker_authorization(endpoint_id)
            .await?
            .is_some_and(|node| node.enabled))
    }

    /// Authorization decision plus the enrollment label, recording the node
    /// as seen at most once per [`NODE_SEEN_WRITE_INTERVAL`].
    pub async fn worker_authorization(
        &self,
        endpoint_id: EndpointId,
    ) -> Result<Option<NodeAuthorization>, StoreError> {
        let endpoint_key = endpoint_id.to_string();
        let node = self.store.node_authorization(&endpoint_key).await?;
        if node.as_ref().is_some_and(|node| node.enabled) && self.should_write_seen(endpoint_id) {
            self.store.touch_node_seen(&endpoint_key, now_ms()).await?;
        }
        Ok(node)
    }

    fn should_write_seen(&self, endpoint_id: EndpointId) -> bool {
        let now = Instant::now();
        let mut written = self
            .last_seen_written
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if written
            .get(&endpoint_id)
            .is_some_and(|last| now.duration_since(*last) < NODE_SEEN_WRITE_INTERVAL)
        {
            return false;
        }
        if written.len() >= MAX_TRACKED_SEEN_ENDPOINTS {
            written.retain(|_, last| now.duration_since(*last) < NODE_SEEN_WRITE_INTERVAL);
        }
        if written.len() < MAX_TRACKED_SEEN_ENDPOINTS {
            written.insert(endpoint_id, now);
        }
        true
    }

    pub async fn revoke_node(&self, endpoint_id: EndpointId) -> Result<bool, StoreError> {
        self.store
            .set_node_enabled(&endpoint_id.to_string(), false)
            .await
    }

    pub async fn enable_node(&self, endpoint_id: EndpointId) -> Result<bool, StoreError> {
        self.store
            .set_node_enabled(&endpoint_id.to_string(), true)
            .await
    }

    pub async fn nodes(&self) -> Result<Vec<MeshNodeRecord>, StoreError> {
        self.store.list_nodes().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    #[tokio::test]
    async fn repeated_authorization_writes_last_seen_at_most_once_per_interval() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-auth-touch-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let store = MeshStore::open(&path).await.expect("open store");
        let created = store
            .create_join_key(None, None, Some(1))
            .await
            .expect("create key");
        let endpoint = SecretKey::generate().public();
        let authorizer = MeshAuthorizer::new(store.clone());
        authorizer
            .enroll(&created.token, endpoint, None)
            .await
            .expect("enroll");
        // Enrollment stamps last_seen; reset it so the first touch is visible.
        store
            .touch_node_seen(&endpoint.to_string(), 1)
            .await
            .expect("reset");

        assert!(authorizer.authorize_worker(endpoint).await.expect("auth"));
        let first = store.list_nodes().await.expect("nodes")[0].last_seen_at_ms;
        assert!(first.is_some_and(|seen| seen > 1));
        store
            .touch_node_seen(&endpoint.to_string(), 2)
            .await
            .expect("sentinel");
        for _ in 0..5 {
            assert!(authorizer.authorize_worker(endpoint).await.expect("auth"));
        }
        assert_eq!(
            store.list_nodes().await.expect("nodes")[0].last_seen_at_ms,
            Some(2),
            "throttled authorizations must not rewrite last_seen"
        );
        let _ = std::fs::remove_file(path);
    }
}
