use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct Load {
    outstanding: usize,
    last_selected: u64,
}

#[derive(Debug)]
struct State {
    providers: Vec<Load>,
    sequence: u64,
}

#[derive(Debug, Clone)]
pub(super) struct LoadBalancer(Arc<Mutex<State>>);

impl LoadBalancer {
    pub(super) fn new(providers: usize) -> Self {
        Self(Arc::new(Mutex::new(State {
            providers: (0..providers).map(|_| Load::default()).collect(),
            sequence: 0,
        })))
    }

    pub(super) fn reserve(&self, candidates: &[usize]) -> Option<Reservation> {
        // Selection and reservation share a lock so simultaneous arrivals see each other.
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let provider = candidates.iter().copied().min_by_key(|index| {
            let load = &state.providers[*index];
            (load.outstanding, load.last_selected, *index)
        })?;
        state.sequence += 1;
        let sequence = state.sequence;
        let load = &mut state.providers[provider];
        load.outstanding += 1;
        load.last_selected = sequence;
        Some(Reservation {
            state: self.0.clone(),
            provider,
        })
    }
}

pub(super) struct Reservation {
    state: Arc<Mutex<State>>,
    pub(super) provider: usize,
}

impl Reservation {
    pub(super) fn stream<S>(self, stream: S) -> Pin<Box<dyn Stream<Item = S::Item> + Send>>
    where
        S: Stream + Send + 'static,
        S::Item: Send,
    {
        Box::pin(async_stream::stream! {
            let _reservation = self;
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                yield item;
            }
        })
    }

    pub(super) fn response(self, response: reqwest::Response) -> reqwest::Response {
        use http_body_util::BodyExt;
        let response: http::Response<reqwest::Body> = response.into();
        let (parts, body) = response.into_parts();
        http::Response::from_parts(
            parts,
            reqwest::Body::wrap_stream(self.stream(body.into_data_stream())),
        )
        .into()
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.providers[self.provider].outstanding -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[tokio::test]
    async fn reservations_release_before_first_poll_after_eof_and_on_drop() {
        let balance = LoadBalancer::new(1);
        let outstanding = || balance.0.lock().unwrap().providers[0].outstanding;
        let unpolled = balance
            .reserve(&[0])
            .unwrap()
            .stream(stream::pending::<()>());
        assert_eq!(outstanding(), 1);
        drop(unpolled);
        assert_eq!(outstanding(), 0);

        let mut complete = balance.reserve(&[0]).unwrap().stream(stream::iter([1]));
        assert_eq!(complete.next().await, Some(1));
        assert_eq!(outstanding(), 1);
        assert_eq!(complete.next().await, None);
        assert_eq!(outstanding(), 0);

        let mut partial = balance.reserve(&[0]).unwrap().stream(stream::iter([1, 2]));
        assert_eq!(partial.next().await, Some(1));
        drop(partial);
        assert_eq!(outstanding(), 0);
    }

    #[test]
    fn simultaneous_arrivals_reserve_evenly() {
        let balance = LoadBalancer::new(4);
        let reservations: Vec<_> = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..32)
                .map(|_| {
                    let balance = balance.clone();
                    scope.spawn(move || balance.reserve(&[0, 1, 2, 3]).unwrap())
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect()
        });
        assert!(
            balance
                .0
                .lock()
                .unwrap()
                .providers
                .iter()
                .all(|load| load.outstanding == 8)
        );
        drop(reservations);
        assert!(
            balance
                .0
                .lock()
                .unwrap()
                .providers
                .iter()
                .all(|load| load.outstanding == 0)
        );
    }
}
