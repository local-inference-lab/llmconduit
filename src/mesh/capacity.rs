use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct CapacityGate {
    inner: Arc<Mutex<CapacityGateState>>,
    notify: Arc<Notify>,
    shared_notify: Option<Arc<Notify>>,
}

#[derive(Debug)]
struct CapacityGateState {
    limit: u32,
    active: u32,
}

#[derive(Debug)]
pub struct CapacityPermit {
    inner: Arc<Mutex<CapacityGateState>>,
    notify: Arc<Notify>,
    shared_notify: Option<Arc<Notify>>,
}

impl CapacityGate {
    pub fn new(limit: u32) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CapacityGateState { limit, active: 0 })),
            notify: Arc::new(Notify::new()),
            shared_notify: None,
        }
    }

    pub(crate) fn with_shared_notify(limit: u32, shared_notify: Arc<Notify>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CapacityGateState { limit, active: 0 })),
            notify: Arc::new(Notify::new()),
            shared_notify: Some(shared_notify),
        }
    }

    pub fn set_limit(&self, limit: u32) {
        self.set_limit_if_changed(limit);
    }

    pub fn set_limit_if_changed(&self, limit: u32) -> bool {
        let mut state = self.inner.lock().expect("mesh capacity lock poisoned");
        if state.limit == limit {
            return false;
        }
        state.limit = limit;
        drop(state);
        self.notify_waiters();
        true
    }

    pub fn snapshot(&self) -> CapacitySnapshot {
        let state = self.inner.lock().expect("mesh capacity lock poisoned");
        CapacitySnapshot {
            limit: state.limit,
            active: state.active,
            available: state.limit.saturating_sub(state.active),
        }
    }

    pub fn try_acquire(&self) -> Option<CapacityPermit> {
        let mut state = self.inner.lock().expect("mesh capacity lock poisoned");
        if state.active >= state.limit {
            return None;
        }
        state.active += 1;
        Some(CapacityPermit {
            inner: Arc::clone(&self.inner),
            notify: Arc::clone(&self.notify),
            shared_notify: self.shared_notify.clone(),
        })
    }

    pub async fn acquire(&self) -> CapacityPermit {
        loop {
            let mut notified = Box::pin(self.notify.notified());
            notified.as_mut().enable();
            if let Some(permit) = self.try_acquire() {
                return permit;
            }
            notified.await;
        }
    }

    fn notify_waiters(&self) {
        self.notify.notify_waiters();
        if let Some(shared_notify) = &self.shared_notify {
            shared_notify.notify_waiters();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacitySnapshot {
    pub limit: u32,
    pub active: u32,
    pub available: u32,
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        let mut state = self.inner.lock().expect("mesh capacity lock poisoned");
        state.active = state.active.saturating_sub(1);
        drop(state);
        self.notify.notify_waiters();
        if let Some(shared_notify) = &self.shared_notify {
            shared_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn capacity_shrinks_without_revoking_active_permits() {
        let gate = CapacityGate::new(3);
        let a = gate.try_acquire().expect("slot a");
        let b = gate.try_acquire().expect("slot b");
        gate.set_limit(1);

        assert!(gate.try_acquire().is_none());
        assert_eq!(
            gate.snapshot(),
            CapacitySnapshot {
                limit: 1,
                active: 2,
                available: 0,
            }
        );

        drop(a);
        assert!(gate.try_acquire().is_none());
        drop(b);
        let permit = gate.try_acquire();
        assert!(permit.is_some());
        drop(permit);
    }

    #[test]
    fn concurrent_acquire_never_exceeds_limit() {
        let gate = Arc::new(CapacityGate::new(8));
        let handles = (0..64)
            .map(|_| {
                let gate = Arc::clone(&gate);
                thread::spawn(move || gate.try_acquire())
            })
            .collect::<Vec<_>>();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("join"))
            .collect::<Vec<_>>();
        let permits = results.into_iter().flatten().collect::<Vec<_>>();

        assert_eq!(permits.len(), 8);
        assert_eq!(
            gate.snapshot(),
            CapacitySnapshot {
                limit: 8,
                active: 8,
                available: 0,
            }
        );
        drop(permits);
        assert_eq!(
            gate.snapshot(),
            CapacitySnapshot {
                limit: 8,
                active: 0,
                available: 8,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn async_acquire_waits_until_permit_is_released() {
        let gate = CapacityGate::new(1);
        let first = gate.try_acquire().expect("first permit");
        let waiting = gate.acquire();
        tokio::pin!(waiting);

        tokio::select! {
            _ = &mut waiting => panic!("acquired while full"),
            _ = tokio::task::yield_now() => {}
        }

        drop(first);
        let second = waiting.await;
        assert_eq!(
            gate.snapshot(),
            CapacitySnapshot {
                limit: 1,
                active: 1,
                available: 0,
            }
        );
        drop(second);
        assert_eq!(
            gate.snapshot(),
            CapacitySnapshot {
                limit: 1,
                active: 0,
                available: 1,
            }
        );
    }
}
