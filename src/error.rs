use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde::Serialize;
use std::fmt;

pub type AppResult<T> = Result<T, AppError>;

/// How the multi-provider `FailoverUpstreamClient`/routing layer should treat a
/// failed upstream attempt. This is a property of the upstream-attempt OUTCOME,
/// not a generic error policy: only the leaf upstream client decides it, and
/// only the failover loop reads it.
///
/// `Failover` (the default) retries and cools a provider. `FailoverNoCooldown`
/// retries elsewhere without penalizing a healthy provider for a request it
/// rejected. `Terminal` surfaces the error without retrying or cooling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FailoverDisposition {
    /// Provider-failure-shaped: failover may retry on the next provider.
    #[default]
    Failover,
    /// Request-shaped failure: another provider may accept it, but this
    /// provider remains healthy and must not enter cooldown.
    FailoverNoCooldown,
    /// Same-provider terminal: surface as-is, do not fail over.
    Terminal,
}

#[derive(Debug)]
pub struct AppError {
    pub status: StatusCode,
    pub message: String,
    pub client_message: String,
    /// Optional STRUCTURED error code carried on the canonical Responses
    /// `response.failed` event (its `error.code`). `None` keeps the historical
    /// `"gateway_error"` default; a constructor like
    /// [`AppError::unknown_tool_repair_exhausted`] sets a specific machine code
    /// (e.g. `"invalid_tool_call"`) so a terminal failure is a structured event,
    /// not a raw message. The Responses converter renders it as `error.code`, and
    /// the Chat converter renders it as the OpenAI error object's `code` field;
    /// the Anthropic error shape has no `code` slot (it carries an error `type`),
    /// so there the `client_message` stays informative on its own.
    pub code: Option<String>,
    /// The failover disposition of the upstream attempt that produced this
    /// error. Generic errors carry the default (`Failover`); only the leaf
    /// upstream client promotes an error to `Terminal`.
    failover: FailoverDisposition,
}

impl AppError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        let msg = message.into();
        Self {
            status: StatusCode::BAD_REQUEST,
            client_message: msg.clone(),
            message: msg,
            code: None,
            failover: FailoverDisposition::default(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        let msg = message.into();
        Self {
            status: StatusCode::NOT_FOUND,
            client_message: msg.clone(),
            message: msg,
            code: None,
            failover: FailoverDisposition::default(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        let msg = message.into();
        Self {
            status: StatusCode::CONFLICT,
            client_message: msg.clone(),
            message: msg,
            code: None,
            failover: FailoverDisposition::default(),
        }
    }

    /// An authenticated request is not permitted to use the selected resource.
    /// Authorization failures are terminal request outcomes: routing/failover
    /// must not retry another candidate or cool a healthy provider.
    pub fn forbidden(message: impl Into<String>) -> Self {
        let msg = message.into();
        Self {
            status: StatusCode::FORBIDDEN,
            client_message: msg.clone(),
            message: msg,
            code: Some("authorization_denied".to_string()),
            failover: FailoverDisposition::Terminal,
        }
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        let msg = message.into();
        Self {
            status: StatusCode::BAD_GATEWAY,
            client_message: msg.clone(),
            message: msg,
            code: None,
            failover: FailoverDisposition::default(),
        }
    }

    /// E1: the upstream model returned a tool call whose name was NOT in the
    /// offered tool set and could not self-correct within the bounded in-gateway
    /// repair ceiling. Surfaces as a STRUCTURED terminal `response.failed`
    /// (code `invalid_tool_call`) — NOT a raw mid-stream `?` abort — so all three
    /// inbound converters render a clean terminal frame. The client message is
    /// deliberately generic (it does not echo the hallucinated tool name; the
    /// operator gets that via the `tracing::warn!` + monitor phase).
    pub fn unknown_tool_repair_exhausted() -> Self {
        Self::upstream(
            "the model requested a tool that is not available and could not recover; \
             the request was ended without completing the tool call",
        )
        .with_code("invalid_tool_call")
    }

    /// Attach a structured [`code`](Self::code) for the canonical `response.failed`
    /// event. Builder form so a constructor can tag a specific machine code
    /// without widening every call site.
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    /// An upstream error tagged with an explicit failover disposition. The leaf
    /// upstream client uses this to mark a context-window overflow that survived
    /// its shrink-and-retry as `Terminal` so failover/routing surfaces it
    /// instead of retrying the same oversized prompt on another provider.
    pub(crate) fn upstream_with_disposition(
        message: impl Into<String>,
        disposition: FailoverDisposition,
    ) -> Self {
        Self {
            failover: disposition,
            ..Self::upstream(message)
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
            client_message: "internal server error".to_string(),
            code: None,
            failover: FailoverDisposition::default(),
        }
    }

    pub fn cancelled() -> Self {
        Self {
            status: StatusCode::from_u16(499).expect("valid status code"),
            message: "client disconnected".to_string(),
            client_message: "client disconnected".to_string(),
            code: None,
            failover: FailoverDisposition::default(),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.status == StatusCode::from_u16(499).expect("valid status code")
    }

    pub fn status_code(&self) -> StatusCode {
        self.status
    }

    /// The failover disposition of the upstream attempt that produced this error.
    /// The failover loop matches on this to decide whether to retry the next
    /// provider (`Failover`) or surface the error terminally (`Terminal`).
    pub(crate) fn failover_disposition(&self) -> FailoverDisposition {
        self.failover
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for AppError {}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    error: ErrorPayload<'a>,
}

#[derive(Debug, Serialize)]
struct ErrorPayload<'a> {
    message: &'a str,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!(status = %self.status, detail = %self.message, "request error");
        let status = self.status_code();
        let body = ErrorBody {
            error: ErrorPayload {
                message: &self.client_message,
            },
        };
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn response_body_string(error: AppError) -> String {
        let response = error.into_response();
        let body = response.into_body();
        let bytes = body.collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn test_internal_error_hides_detail() {
        let body = response_body_string(AppError::internal("secret detail")).await;
        assert!(body.contains("internal server error"));
        assert!(!body.contains("secret detail"));
    }

    #[tokio::test]
    async fn test_bad_request_shows_detail() {
        let body = response_body_string(AppError::bad_request("invalid field X")).await;
        assert!(body.contains("invalid field X"));
    }

    #[tokio::test]
    async fn test_upstream_error_shows_detail() {
        let body = response_body_string(AppError::upstream("provider returned 500: oops")).await;
        assert!(body.contains("provider returned 500: oops"));
    }

    // Disposition equivalence vs the old `failover_eligible: bool`. The previous
    // representation had EVERY constructor failover-eligible (`true`) except the
    // terminal one (`false`). The typed disposition must reproduce that exact
    // truth table: every generic constructor defaults to `Failover`, and only
    // the explicit-disposition constructor with `Terminal` is non-failover.
    #[test]
    fn generic_constructors_default_to_failover_disposition() {
        let cases = [
            AppError::bad_request("x"),
            AppError::conflict("x"),
            AppError::upstream("x"),
            AppError::internal("x"),
            AppError::cancelled(),
            // An upstream error explicitly tagged `Failover` stays eligible.
            AppError::upstream_with_disposition("x", FailoverDisposition::Failover),
        ];
        for error in cases {
            assert_eq!(
                error.failover_disposition(),
                FailoverDisposition::Failover,
                "generic/explicit-failover errors must remain failover-eligible \
                 (status {})",
                error.status
            );
        }
    }

    #[test]
    fn unknown_tool_repair_exhausted_carries_structured_code() {
        // E1: the bounded-repair terminal is a 502 upstream error tagged with a
        // structured `invalid_tool_call` code (rendered on the canonical
        // `response.failed`), and its client message must NOT echo any tool name.
        let error = AppError::unknown_tool_repair_exhausted();
        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert_eq!(error.code.as_deref(), Some("invalid_tool_call"));
        assert!(error.failover_disposition() == FailoverDisposition::Failover);
        assert!(!error.client_message.is_empty());
    }

    #[test]
    fn with_code_sets_structured_code_and_generic_constructors_have_none() {
        assert_eq!(AppError::upstream("x").code, None);
        assert_eq!(
            AppError::upstream("x").with_code("my_code").code.as_deref(),
            Some("my_code")
        );
    }

    #[test]
    fn upstream_terminal_disposition_is_not_failover() {
        let error = AppError::upstream_with_disposition("overflow", FailoverDisposition::Terminal);
        assert_eq!(error.failover_disposition(), FailoverDisposition::Terminal);
        // It is still a 502 upstream error in every other respect; only the
        // disposition differs from a plain `upstream(...)`.
        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    }
}
