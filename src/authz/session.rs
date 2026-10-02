use super::{AuthError, PolicyScope};
use chrono::{DateTime, Datelike, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

type ReleaseHook = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Debug, Clone, Default)]
pub struct SessionLimiter {
    state: Arc<Mutex<LimiterState>>,
}

#[derive(Debug, Default)]
struct LimiterState {
    by_key: HashMap<String, KeySessions>,
}

#[derive(Debug, Default)]
struct KeySessions {
    active: u32,
    day: i32,
    starts: u32,
}

impl SessionLimiter {
    pub fn acquire(
        &self,
        scope: &PolicyScope,
        now: DateTime<Utc>,
    ) -> Result<SessionLease, AuthError> {
        self.acquire_for_key(&scope.context().key_id, scope.effective_limits(), now)
    }

    /// Acquires a request lease from already-evaluated identity and limits.
    /// This keeps ingress independent of the richer policy representation while
    /// preserving one limiter and one RAII release path.
    pub fn acquire_for_key(
        &self,
        key_id: &str,
        limits: super::LimitSet,
        now: DateTime<Utc>,
    ) -> Result<SessionLease, AuthError> {
        let session_id = format!("ses_{}", uuid::Uuid::new_v4().simple());
        self.acquire_for_key_with_release(key_id, limits, now, session_id, None)
    }

    pub fn acquire_for_key_with_release(
        &self,
        key_id: &str,
        limits: super::LimitSet,
        now: DateTime<Utc>,
        session_id: String,
        on_release: Option<ReleaseHook>,
    ) -> Result<SessionLease, AuthError> {
        let key_id = key_id.to_owned();
        let day = now.date_naive().num_days_from_ce();
        let mut state = self
            .state
            .lock()
            .map_err(|_| AuthError::PolicyUnavailable)?;
        let sessions = state.by_key.entry(key_id.clone()).or_default();
        if sessions.day != day {
            sessions.day = day;
            sessions.starts = 0;
        }
        if limits
            .max_concurrent_sessions
            .is_some_and(|maximum| sessions.active >= maximum)
            || limits
                .max_daily_session_starts
                .is_some_and(|maximum| sessions.starts >= maximum)
        {
            return Err(AuthError::QuotaExceeded);
        }
        sessions.active = sessions.active.saturating_add(1);
        sessions.starts = sessions.starts.saturating_add(1);
        drop(state);
        Ok(SessionLease {
            inner: Arc::new(LeaseInner {
                state: Arc::downgrade(&self.state),
                key_id,
                session_id,
                on_release,
            }),
        })
    }

    pub fn active_for_key(&self, key_id: &str) -> u32 {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.by_key.get(key_id).map(|sessions| sessions.active))
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct SessionLease {
    inner: Arc<LeaseInner>,
}

struct LeaseInner {
    state: Weak<Mutex<LimiterState>>,
    key_id: String,
    session_id: String,
    on_release: Option<ReleaseHook>,
}

impl std::fmt::Debug for LeaseInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseInner")
            .field("key_id", &self.key_id)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl Drop for LeaseInner {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        if let Some(sessions) = state.by_key.get_mut(&self.key_id) {
            sessions.active = sessions.active.saturating_sub(1);
        }
        drop(state);
        if let Some(on_release) = &self.on_release {
            on_release(&self.session_id);
        }
    }
}

impl SessionLease {
    pub fn key_id(&self) -> &str {
        &self.inner.key_id
    }

    pub fn session_id(&self) -> &str {
        &self.inner.session_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authz::{
        AuthRequestId, Endpoint, LimitSet, PolicyBinding, PolicyEffect, PolicyIdentity,
        PolicyMatcher, PolicyRule, PolicySnapshot, PolicySubject,
    };
    use std::collections::{HashMap, HashSet};

    fn scope(max_concurrent: u32, max_daily: u32) -> PolicyScope {
        let rule = PolicyRule {
            id: "pol_limit".into(),
            effect: PolicyEffect::Allow,
            binding: PolicyBinding {
                subject: PolicySubject::Key("key_a".into()),
            },
            matcher: PolicyMatcher::all(),
            windows: Vec::new(),
            limits: LimitSet {
                max_concurrent_sessions: Some(max_concurrent),
                max_daily_session_starts: Some(max_daily),
            },
            management_permissions: HashSet::new(),
        };
        let snapshot = Arc::new(PolicySnapshot::new(
            1,
            vec![rule],
            HashMap::new(),
            HashMap::new(),
        ));
        snapshot
            .authorize(
                &PolicyIdentity {
                    request_id: AuthRequestId::new(),
                    key_id: "key_a".into(),
                    key_prefix: "llmc_example".into(),
                    principal_id: "usr_a".into(),
                    policy_epoch: 1,
                },
                Endpoint::Responses,
                Some("model"),
                Utc::now(),
            )
            .unwrap()
    }

    #[test]
    fn cloned_lease_releases_once_after_last_owner() {
        let limiter = SessionLimiter::default();
        let scope = scope(1, 2);
        let now = Utc::now();
        let lease = limiter.acquire(&scope, now).unwrap();
        let clone = lease.clone();
        drop(lease);
        assert_eq!(limiter.active_for_key("key_a"), 1);
        assert!(matches!(
            limiter.acquire(&scope, now),
            Err(AuthError::QuotaExceeded)
        ));
        drop(clone);
        assert_eq!(limiter.active_for_key("key_a"), 0);
        assert!(limiter.acquire(&scope, now).is_ok());
    }

    #[tokio::test]
    async fn aborted_stream_task_releases_lease() {
        let limiter = SessionLimiter::default();
        let scope = scope(1, 2);
        let lease = limiter.acquire(&scope, Utc::now()).unwrap();
        let task = tokio::spawn(async move {
            let _lease = lease;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        assert_eq!(limiter.active_for_key("key_a"), 1);
        task.abort();
        let _ = task.await;
        assert_eq!(limiter.active_for_key("key_a"), 0);
    }
}
