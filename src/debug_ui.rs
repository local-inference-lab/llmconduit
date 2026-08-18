//! `/debug` HTML + `/debug/app.js` + `/debug/ws` handlers.
//!
//! D7: the inline module script that used to live in `debug.html` is now served
//! from `/debug/app.js` (see [`debug_app_js`]) so `/debug` can ship a strict
//! Content-Security-Policy (`script-src 'self'`, no `'unsafe-inline'`). The
//! `/debug` + `/debug/ws` routes are gated behind the dashboard session cookie
//! by the auth layer wired in `http.rs`; the per-connection `Origin` check and
//! cookie-`exp` socket close live in [`debug_ws`]/[`debug_socket`]. The
//! `/debug/ws` WIRE CONTRACT is unchanged — it still streams bare
//! [`DebugWsMessage`] frames (D7b adds the dashboard-only batched envelope on a
//! separate `/dashboard/ws` route).

use crate::engine::Gateway;
use crate::monitor::DebugWsMessage;
use axum::extract::State;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::broadcast;

const DEBUG_HTML: &str = include_str!("debug.html");
/// The dashboard/debug client logic, externalized from `debug.html` so `/debug`
/// can use `script-src 'self'` (D7). Served verbatim at `/debug/app.js`.
const DEBUG_APP_JS: &str = include_str!("debug_app.js");

/// Strict CSP for `/debug`: same shape as the `/dashboard` policy. With the
/// script externalized to `/debug/app.js`, `script-src 'self'` needs NO
/// `'unsafe-inline'`. Styles remain inline (`<style>` in `debug.html`), so
/// `style-src` keeps `'unsafe-inline'`. `connect-src` allows the WS upgrade.
pub const DEBUG_CSP: &str = "default-src 'self'; script-src 'self'; connect-src 'self' ws: wss:; \
     style-src 'self' 'unsafe-inline'; img-src 'self' data:; object-src 'none'; \
     base-uri 'self'; frame-ancestors 'none'";

/// `GET /debug` — the debug UI shell. Carries the strict CSP + the standard
/// security headers + `no-store` (transcripts must not be cached).
pub async fn debug_index() -> Response {
    let mut response = (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        DEBUG_HTML,
    )
        .into_response();
    apply_debug_security_headers(response.headers_mut());
    response
}

/// `GET /debug/app.js` — the externalized client module. Same security headers
/// so the JS is served under the same hardened policy.
pub async fn debug_app_js() -> Response {
    let mut response = (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        DEBUG_APP_JS,
    )
        .into_response();
    apply_debug_security_headers(response.headers_mut());
    response
}

/// Apply `Content-Security-Policy`, `X-Frame-Options: DENY`, `nosniff`,
/// `no-referrer`, and `Cache-Control: no-store` to a `/debug` response.
fn apply_debug_security_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(DEBUG_CSP),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
}

/// `GET /debug/ws` — the monitor WebSocket. The HTTP-layer auth middleware has
/// already validated the session cookie for this path; here we additionally
/// enforce the WS `Origin` allow-list (CSWSH defense) and capture the cookie
/// `exp` so the socket is closed when the session expires. A request that fails
/// the cookie+Origin check is rejected with `401 no-store` BEFORE the upgrade.
pub async fn debug_ws(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    // The auth layer attaches the shared `DashboardAuth` so the WS handler can
    // re-validate cookie+Origin (the cookie was already checked by the layer,
    // but the Origin allow-list is WS-specific and the `exp` drives the close).
    let auth = match gateway.dashboard_auth() {
        Some(auth) => auth,
        // No auth context (debug UI disabled / not registered) — should be
        // unreachable because the route only registers when auth exists, but
        // fail closed rather than serving an unauthenticated socket.
        None => {
            return crate::dashboard_auth::no_store(
                (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
            );
        }
    };
    let Some(exp) = auth.authenticate_ws(&headers) else {
        return crate::dashboard_auth::no_store(
            (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
        );
    };
    upgrade
        .on_upgrade(move |socket| debug_socket(socket, gateway, exp))
        .into_response()
}

async fn debug_socket(mut socket: WebSocket, gateway: Arc<Gateway>, session_exp: u64) {
    let mut receiver = gateway.subscribe_monitor();
    let snapshot = gateway.debug_snapshot();
    let mut last_sequence = snapshot.last_sequence;

    // Arm the expiry timer BEFORE replaying the retained snapshot. The socket
    // must close at the cookie `exp` even mid-snapshot — under backpressure a
    // near-expired cookie could otherwise keep receiving snapshot frames after
    // `exp`. `session_exp == u64::MAX` (dev-open: no token configured) yields an
    // effectively-infinite timer that never fires; a real cookie carries a
    // bounded future `exp`.
    let expiry = wait_for_session_expiry(session_exp);
    tokio::pin!(expiry);

    // Snapshot replay, racing the expiry timer: a cookie that expires during
    // replay closes the socket instead of finishing the (possibly large)
    // backlog. The race is factored into `replay_snapshot` over a `FrameSink` so
    // it is unit-testable with a paused clock + a mock sink (the socket itself
    // can't be constructed off a real upgrade).
    match replay_snapshot(&snapshot.messages, expiry.as_mut(), &mut socket).await {
        ReplayOutcome::Completed => {}
        ReplayOutcome::Expired => {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
        ReplayOutcome::SendFailed => return,
    }

    loop {
        tokio::select! {
            // Session expired mid-connection: close the socket.
            _ = &mut expiry => {
                let _ = socket.send(Message::Close(None)).await;
                return;
            }
            received = receiver.recv() => {
                match received {
                    Ok(update) if update.sequence <= last_sequence => {}
                    Ok(update) => {
                        // Race the expiry future around EVERY send in the live
                        // batch (not just at the top of the loop): a backpressured
                        // send must not deliver frames after the cookie `exp`. The
                        // same expiry-raced sender used for snapshot replay closes
                        // the socket at `exp` even mid-batch (finding D7a R2 #3).
                        match replay_snapshot(&update.messages, expiry.as_mut(), &mut socket).await {
                            ReplayOutcome::Completed => {}
                            ReplayOutcome::Expired => {
                                let _ = socket.send(Message::Close(None)).await;
                                return;
                            }
                            ReplayOutcome::SendFailed => return,
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Rebuild the client from authoritative retained state on
                        // the SAME socket. Closing here made the browser reconnect,
                        // clear a more complete live transcript, and provide no
                        // indication that the replacement snapshot had retention
                        // omissions. Protocol-v2 RequestUpsert/EventAppend frames
                        // carry explicit omission counters and payload metadata.
                        let replacement = gateway.debug_snapshot();
                        match replay_snapshot(
                            &replacement.messages,
                            expiry.as_mut(),
                            &mut socket,
                        )
                        .await
                        {
                            ReplayOutcome::Completed => {
                                last_sequence = replacement.last_sequence;
                            }
                            ReplayOutcome::Expired => {
                                let _ = socket.send(Message::Close(None)).await;
                                return;
                            }
                            ReplayOutcome::SendFailed => return,
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

/// A sink for one monitor frame. Abstracts the WS socket so the snapshot-replay
/// race ([`replay_snapshot`]) is unit-testable with a mock sink — an
/// `axum` `WebSocket` can't be constructed off a real upgrade in a unit test.
trait FrameSink {
    /// Send one frame; `false` means the peer is gone (replay should stop).
    fn send_frame(&mut self, message: &DebugWsMessage) -> impl Future<Output = bool>;
}

impl FrameSink for WebSocket {
    fn send_frame(&mut self, message: &DebugWsMessage) -> impl Future<Output = bool> {
        send_message(self, message)
    }
}

/// Outcome of [`replay_snapshot`]: the retained backlog drained fully, the
/// session expired mid-replay (caller must send the WS `Close`), or a send
/// failed (peer gone — caller returns).
#[derive(Debug, PartialEq, Eq)]
enum ReplayOutcome {
    Completed,
    Expired,
    SendFailed,
}

/// Send a batch of `messages` into `sink`, racing each send against the
/// already-armed `expiry` future. Used for BOTH the retained-snapshot replay and
/// each LIVE monitor batch (D7a R2 #3) so neither can deliver a frame past the
/// cookie `exp`. If `expiry` resolves first — even between frames, under
/// backpressure — sending stops with [`ReplayOutcome::Expired`] so the socket
/// closes at `exp` instead of finishing a large backlog/batch past expiry. A
/// failed send yields [`ReplayOutcome::SendFailed`]. The race is `biased` so a
/// ready expiry wins deterministically over a ready send (the connection must not
/// outlive `exp`).
async fn replay_snapshot(
    messages: &[DebugWsMessage],
    mut expiry: std::pin::Pin<&mut (impl Future<Output = ()> + ?Sized)>,
    sink: &mut impl FrameSink,
) -> ReplayOutcome {
    for message in messages {
        tokio::select! {
            biased;
            _ = expiry.as_mut() => return ReplayOutcome::Expired,
            sent = sink.send_frame(message) => {
                if !sent {
                    return ReplayOutcome::SendFailed;
                }
            }
        }
    }
    ReplayOutcome::Completed
}

/// Sleep until the session `exp` (unix secs) is reached, then return. A
/// non-positive remaining duration (already expired / clock skew) returns after
/// a zero-duration sleep (effectively immediate). The duration is derived from
/// the wall clock (`SystemTime`); the wait itself is a `tokio::time` sleep, so a
/// paused-clock test can drive it deterministically with `tokio::time::advance`.
async fn wait_for_session_expiry(session_exp: u64) {
    tokio::time::sleep(session_remaining(session_exp)).await;
}

/// A far-future cap for the expiry timer. The dev-open path (no token) passes
/// `session_exp == u64::MAX`; capping the computed remaining time keeps
/// `tokio::time::sleep` from overflowing the underlying `Instant` arithmetic on
/// an absurd duration (the socket has no real cookie expiry to honor there and
/// closes on disconnect/lag regardless). A bounded real cookie `exp` (≤ 24 h) is
/// always far below this cap.
const MAX_EXPIRY_WAIT: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Remaining time until `session_exp` (unix secs), saturating at zero and capped
/// at [`MAX_EXPIRY_WAIT`].
///
/// D7a R3 #3: the deadline is computed from the FULL sub-second wall clock, not a
/// whole-second truncation. Truncating `now` to whole seconds rounds the
/// remaining time UP by up to ~1s (e.g. at a true `99.9s` vs `exp=100`, an
/// `as_secs()` now of `99` yields `1s` remaining instead of `0.1s`), so frames
/// could be delivered for nearly a second past the signed `exp`. Subtracting the
/// full `Duration` since epoch (seconds + nanos) closes the socket within the
/// `exp` second.
fn session_remaining(session_exp: u64) -> Duration {
    let exp = Duration::from_secs(session_exp);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    exp.saturating_sub(now).min(MAX_EXPIRY_WAIT)
}

async fn send_message(socket: &mut WebSocket, message: &DebugWsMessage) -> bool {
    let Ok(payload) = serde_json::to_string(message) else {
        return true;
    };
    socket.send(Message::Text(payload.into())).await.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    fn now_unix() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    #[test]
    fn debug_app_turn_reconstruction_regressions() {
        if Command::new("node").arg("--version").output().is_err() {
            eprintln!("node is unavailable; skipping debug_app.js behavior test");
            return;
        }
        let output = Command::new("node")
            .arg("tests/debug_app.test.cjs")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .expect("run debug_app.js behavior test");
        assert!(
            output.status.success(),
            "debug_app.js behavior test failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[test]
    fn expired_session_has_zero_remaining() {
        assert_eq!(
            session_remaining(now_unix().saturating_sub(60)),
            Duration::ZERO
        );
        assert_eq!(session_remaining(0), Duration::ZERO);
    }

    #[test]
    fn future_session_has_positive_remaining() {
        let remaining = session_remaining(now_unix() + 120);
        assert!(
            remaining > Duration::from_secs(60),
            "remaining: {remaining:?}"
        );
        assert!(remaining <= Duration::from_secs(120));
    }

    /// REGRESSION (D7a R3 #3): the remaining time to a WHOLE-SECOND `exp` must be
    /// the true sub-second residual, NOT rounded UP to a full second. The old
    /// `as_secs()` truncation of `now` made the remaining to the *next* whole
    /// second always a full `1s` (so frames could ship ~1s past `exp`). Here we
    /// capture the full-precision now, take the next whole second as `exp`, and
    /// assert the computed remaining is below a full second (it equals
    /// `1s - now.subsec`, always `< 1s` unless we land exactly on a tick) and
    /// close to the full-precision expectation.
    #[test]
    fn remaining_uses_subsecond_precision_not_whole_second_round_up() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch");
        let exp_secs = now.as_secs() + 1;
        let expected = Duration::from_secs(exp_secs).saturating_sub(now); // < 1s
        let remaining = session_remaining(exp_secs);
        // The deadline must be the sub-second residual, strictly under a full
        // second (the truncating implementation returned exactly 1s here).
        assert!(
            remaining < Duration::from_secs(1),
            "remaining to the next whole second must be sub-second, got {remaining:?}"
        );
        // And it must track the full-precision expectation (both sampled the
        // clock microseconds apart, so allow a small skew).
        let skew = remaining.abs_diff(expected);
        assert!(
            skew < Duration::from_millis(50),
            "remaining {remaining:?} should match sub-second expected {expected:?} (skew {skew:?})"
        );
    }

    /// The per-connection expiry timer fires once the cookie `exp` passes. We
    /// arm it for a 2s-future `exp`, then advance the paused clock past it and
    /// assert the wait completes — the same future the socket `select!`s on to
    /// send a `Close`.
    #[tokio::test(start_paused = true)]
    async fn expiry_wait_completes_after_exp_passes() {
        let exp = now_unix() + 2;
        let waiter = tokio::spawn(wait_for_session_expiry(exp));
        // Before the deadline the waiter is still pending.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!waiter.is_finished(), "must not close before exp");
        // Past the deadline it resolves.
        tokio::time::advance(Duration::from_secs(2)).await;
        waiter.await.expect("expiry wait completes");
    }

    /// An already-expired cookie closes promptly (zero-duration wait).
    #[tokio::test(start_paused = true)]
    async fn expiry_wait_completes_immediately_when_already_expired() {
        let exp = now_unix().saturating_sub(10);
        // A zero-duration sleep still needs the timer to be polled once; yield
        // by advancing zero so the runtime drives it.
        let waiter = tokio::spawn(wait_for_session_expiry(exp));
        tokio::time::advance(Duration::from_millis(1)).await;
        waiter
            .await
            .expect("already-expired wait completes immediately");
    }

    fn snapshot_msgs(n: usize) -> Vec<DebugWsMessage> {
        (0..n).map(|_| DebugWsMessage::SnapshotDone).collect()
    }

    /// A mock [`FrameSink`]: counts sends, optionally sleeps per send to model
    /// backpressure, and optionally "fails" (peer gone) at a given send index.
    struct MockSink {
        sent: usize,
        per_send: Duration,
        fail_at: Option<usize>,
    }

    impl MockSink {
        fn instant() -> Self {
            Self {
                sent: 0,
                per_send: Duration::ZERO,
                fail_at: None,
            }
        }
    }

    impl FrameSink for MockSink {
        async fn send_frame(&mut self, _message: &DebugWsMessage) -> bool {
            // Count BEFORE the await so a frame whose send is cancelled by the
            // expiry race does not count as sent (the select drops this future).
            // We increment only once we've committed to delivering it.
            if self.per_send > Duration::ZERO {
                tokio::time::sleep(self.per_send).await;
            }
            self.sent += 1;
            !matches!(self.fail_at, Some(at) if self.sent == at)
        }
    }

    /// A non-expiring session replays the whole backlog.
    #[tokio::test(start_paused = true)]
    async fn replay_completes_and_sends_all_when_not_expired() {
        let expiry = wait_for_session_expiry(now_unix() + 3600);
        tokio::pin!(expiry);
        let mut sink = MockSink::instant();
        let outcome = replay_snapshot(&snapshot_msgs(5), expiry.as_mut(), &mut sink).await;
        assert_eq!(outcome, ReplayOutcome::Completed);
        assert_eq!(sink.sent, 5, "every retained frame replayed");
    }

    /// REGRESSION (finding 4): the expiry timer is armed BEFORE snapshot replay.
    /// An already-expired cookie must close the socket WITHOUT replaying any
    /// retained frame — previously the snapshot was flushed before the timer was
    /// armed, so a near-/already-expired cookie still received snapshot frames.
    #[tokio::test(start_paused = true)]
    async fn replay_with_expired_cookie_sends_nothing_and_closes() {
        let expiry = wait_for_session_expiry(now_unix().saturating_sub(10));
        tokio::pin!(expiry);
        let mut sink = MockSink::instant();
        let outcome = replay_snapshot(&snapshot_msgs(5), expiry.as_mut(), &mut sink).await;
        assert_eq!(
            outcome,
            ReplayOutcome::Expired,
            "an already-expired cookie must yield Expired (socket gets a Close)"
        );
        assert_eq!(sink.sent, 0, "no snapshot frame may be sent after exp");
    }

    /// A cookie that expires PART-WAY through a slow (backpressured) replay stops
    /// mid-snapshot with `Expired` rather than draining the whole backlog past
    /// `exp`. Each send blocks (simulating backpressure); under the paused clock
    /// tokio auto-advances to the earliest timer, so the expiry deadline wins.
    #[tokio::test(start_paused = true)]
    async fn replay_expiring_mid_backlog_stops_early() {
        // exp 5s out; each frame takes 2s to flush → a later send straddles exp.
        let expiry = wait_for_session_expiry(now_unix() + 5);
        tokio::pin!(expiry);
        let mut sink = MockSink {
            sent: 0,
            per_send: Duration::from_secs(2),
            fail_at: None,
        };
        let outcome = replay_snapshot(&snapshot_msgs(100), expiry.as_mut(), &mut sink).await;
        assert_eq!(outcome, ReplayOutcome::Expired);
        assert!(
            (1..100).contains(&sink.sent),
            "replay stopped mid-backlog at exp (sent {} of 100)",
            sink.sent
        );
    }

    /// A peer that drops mid-replay surfaces `SendFailed` (caller returns).
    #[tokio::test(start_paused = true)]
    async fn replay_send_failure_short_circuits() {
        let expiry = wait_for_session_expiry(now_unix() + 3600);
        tokio::pin!(expiry);
        let mut sink = MockSink {
            sent: 0,
            per_send: Duration::ZERO,
            fail_at: Some(2), // the 2nd send "fails"
        };
        let outcome = replay_snapshot(&snapshot_msgs(5), expiry.as_mut(), &mut sink).await;
        assert_eq!(outcome, ReplayOutcome::SendFailed);
        assert_eq!(sink.sent, 2, "stopped at the failing send");
    }

    /// REGRESSION (finding D7a R2 #3): the LIVE loop reuses the SAME armed expiry
    /// future for every monitor batch, so a backpressured live send after the
    /// cookie `exp` stops with `Expired` (the socket gets a `Close`) instead of
    /// delivering frames past expiry. This models the live path: the snapshot
    /// replay completes first (cheap), then a slow live batch straddles `exp`
    /// using the same `expiry` the live `select!` arms.
    #[tokio::test(start_paused = true)]
    async fn live_batch_after_exp_closes_via_shared_expiry() {
        // exp 5s out. The snapshot drains instantly; the live batch then sends
        // slowly (2s/frame) so a later frame crosses exp.
        let expiry = wait_for_session_expiry(now_unix() + 5);
        tokio::pin!(expiry);

        // 1) Snapshot replay finishes before exp (instant sink).
        let mut snap_sink = MockSink::instant();
        assert_eq!(
            replay_snapshot(&snapshot_msgs(3), expiry.as_mut(), &mut snap_sink).await,
            ReplayOutcome::Completed,
        );

        // 2) A backpressured LIVE batch reusing the SAME expiry stops at exp.
        let mut live_sink = MockSink {
            sent: 0,
            per_send: Duration::from_secs(2),
            fail_at: None,
        };
        let outcome = replay_snapshot(&snapshot_msgs(100), expiry.as_mut(), &mut live_sink).await;
        assert_eq!(
            outcome,
            ReplayOutcome::Expired,
            "a live send after exp must close, not deliver past expiry"
        );
        assert!(
            (1..100).contains(&live_sink.sent),
            "live batch stopped mid-stream at exp (sent {} of 100)",
            live_sink.sent
        );
    }
}
