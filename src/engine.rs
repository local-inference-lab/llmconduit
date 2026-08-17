use crate::adapters::chat_to_responses::FinalizedAssistantTurn;
use crate::adapters::chat_to_responses::ResolvedToolCall;
use crate::adapters::chat_to_responses::StreamEmission;
use crate::adapters::chat_to_responses::StreamState;
use crate::adapters::responses_to_chat::LoweredTurn;
use crate::adapters::responses_to_chat::ToolKind;
use crate::adapters::responses_to_chat::{
    lower_request_with_image_agent_and_roles, merge_adjacent_if_configured, shape_tail_message,
};
use crate::config::Config;
use crate::config::UnsupportedImagePolicy;
use crate::error::AppError;
use crate::error::AppResult;
use crate::models::chat::ChatCompletionChunk;
use crate::models::chat::ChatCompletionRequest;
use crate::models::chat::ChatMessage;
use crate::models::chat::ChunkUsage;
use crate::models::chat::StreamOptions;
use crate::models::responses::DeltaPayload;
use crate::models::responses::FailedError;
use crate::models::responses::FailedPayload;
use crate::models::responses::FailedResponse;
use crate::models::responses::OutputItemPayload;
use crate::models::responses::ReasoningDeltaPayload;
use crate::models::responses::ReasoningSignatureDeltaPayload;
use crate::models::responses::ResponseCompletedPayload;
use crate::models::responses::ResponseCreatedPayload;
use crate::models::responses::ResponseInputTokensDetails;
use crate::models::responses::ResponseItem;
use crate::models::responses::ResponseOutputTokensDetails;
use crate::models::responses::ResponseResource;
use crate::models::responses::ResponseStub;
use crate::models::responses::ResponseUsage;
use crate::models::responses::ResponsesEnvelope;
use crate::models::responses::ResponsesRequest;
use crate::models::responses::WebSearchAction;
use crate::monitor::DebugEventImage;
use crate::monitor::MonitorEventKind;
use crate::monitor::MonitorHub;
use crate::raw::RawOutput;
use crate::replay::ReplayRecord;
use crate::replay::ReplayStore;
use crate::search::SearchClient;
use crate::search::SearchOutcome;
use crate::tool_delta_gate::DeltaDecision;
use crate::tool_delta_gate::DeltaEmission;
use crate::tool_delta_gate::ToolDeltaGate;
use crate::upstream::ProviderHealth;
use crate::upstream::ProviderHealthPublisher;
use crate::upstream::UpstreamClient;
use crate::upstream::UpstreamModelEntry;
use crate::upstream::canonical_model_key;
use crate::upstream::sanitize_chat_request;
use crate::vision::ImageCache;
use crate::vision::VisionClient;
use crate::vision::VisionRequest;
use futures::StreamExt;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

const UPSTREAM_MODEL_CATALOG_TTL_SECS: u64 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenizeCapability {
    Unknown,
    Supported,
    Unsupported,
}

/// E1: absolute ceiling on in-gateway repair rounds for hallucinated (unoffered)
/// tool calls — mirrors `WEB_SEARCH_ROUNDS_HARD_CEILING`. Default 1 (one
/// self-correction attempt); 2 is the practical maximum (a model that cannot
/// recover in two rounds will not in ten). MUST NOT exceed 2.
const UNKNOWN_TOOL_REPAIR_CEILING: usize = 1;

/// E1: HARD cap on the distinct `{provider, served_model}` keys tracked by the
/// always-on unknown-tool-call counter. The labels can be attacker-influenced
/// (a request model name flows into `served_model`), so once the map is full a
/// fresh key folds into the bounded [`UNKNOWN_TOOL_COUNTER_OVERFLOW_KEY`] catch-all
/// instead of growing without bound (AGENTS.md: bounded structures only). The
/// outcome dimension is already bounded ({repaired, exhausted}).
const MAX_UNKNOWN_TOOL_COUNTER_KEYS: usize = 256;

/// E1: the bounded catch-all `{provider, served_model}` a counter key folds into
/// once [`MAX_UNKNOWN_TOOL_COUNTER_KEYS`] distinct keys are tracked.
const UNKNOWN_TOOL_COUNTER_OVERFLOW_KEY: &str = "__other__";

/// E1: synthetic tool result injected for a VALID call that was tainted (NOT run)
/// because a sibling call in the same batch referenced an unoffered tool.
const TAINTED_TOOL_RESULT: &str = "not_executed: another tool call in this turn referenced a tool that is not available, \
     so no tool in this batch was executed. Re-issue only valid tool calls.";

/// E1: closed-tool-set prevention note (option C) injected as a system message in
/// the repair round so the model stops inventing tool names.
const CLOSED_TOOL_SET_NOTE: &str = "You may only call tools that are explicitly provided in this request. Do not invent or \
     guess tool names. If a `ToolSearch` tool is provided, call it to request any additional \
     tools you need before using them.";

/// E1: outcome of an unknown-tool soft-reject turn, for the always-on
/// `unknown_tool_call_total{provider,served_model,outcome}` counter. Bounded by
/// construction — exactly two variants, never labeled by raw tool name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnknownToolOutcome {
    /// The model self-corrected within the repair ceiling (no rejected calls in a
    /// subsequent round).
    Repaired,
    /// The repair ceiling was reached with rejected calls still present; the turn
    /// ended in a structured terminal failure.
    Exhausted,
}

impl UnknownToolOutcome {
    /// Stable label for the counter key / observability.
    pub fn as_str(self) -> &'static str {
        match self {
            UnknownToolOutcome::Repaired => "repaired",
            UnknownToolOutcome::Exhausted => "exhausted",
        }
    }
}

/// E1: key for the bounded unknown-tool-call counter.
type UnknownToolCounterKey = (String, String, UnknownToolOutcome);

#[derive(Clone)]
pub struct Gateway {
    config: Config,
    replay_store: ReplayStore,
    upstream: Arc<dyn UpstreamClient>,
    search: Arc<dyn SearchClient>,
    vision: Arc<dyn VisionClient>,
    image_cache: Arc<ImageCache>,
    monitor: MonitorHub,
    raw_output: Option<RawOutput>,
    /// D1 dashboard FlowStore: authoritative per-flow records + capture seam.
    /// `DashboardFlowStore::disabled()` when `--with-debug-ui` is off, so every
    /// store op is a no-op and the production hot path is unchanged.
    flow_store: crate::dashboard_flow::DashboardFlowStore,
    /// D7 dashboard/`/debug` auth context (env-only secrets), shared as the
    /// router's auth-layer state. `None` when the debug UI is off OR when the
    /// startup decision refused to register the protected routes — in either
    /// case the auth-gated routes are simply not registered. NEVER built from
    /// the persisted `Config` (secrets are read from the environment).
    dashboard_auth: Option<Arc<crate::dashboard_auth::DashboardAuth>>,
    upstream_model_catalog: Arc<Mutex<Option<CachedUpstreamModelCatalog>>>,
    /// D4 published topology health: the latest versioned
    /// `Arc<ProviderHealthSnapshot>`, swapped by the publication task (1 s tick +
    /// cooldown-deadline wake) when `--with-debug-ui` is on. Always present (a
    /// cheap shared handle) so `upstream_health()` and the snapshot readers do not
    /// branch on the debug flag; the TASK that refreshes it is gated. Cloning the
    /// Gateway shares the inner `Arc`, keeping the derived `Clone`.
    provider_health: ProviderHealthPublisher,
    /// D5 aggregated-metrics layer: per-window rings + histograms + the coordinated
    /// body-free snapshot ring. `MetricsLayer::disabled()` when `--with-debug-ui` is
    /// off (every op a no-op, zero lock), built `new()` + attached via
    /// `with_metrics` in the `with_debug_ui` DI branch — mirroring how the FlowStore/
    /// monitor are gated. The engine records into it at the D3 TERMINAL finalize seam
    /// (NOT the middleware); the 5 s snapshot task reads it under the fixed
    /// FlowStore→Metrics lock order.
    metrics: crate::metrics::MetricsLayer,
    /// F1 (Topic F) durable per-turn capture handle (see `turn_capture.rs`).
    /// `TurnCapture::disabled()` when `turn_capture_dir` is unset -- every op
    /// a no-op (no thread, no alloc, no fs). Unlike the FlowStore/metrics/
    /// monitor triad above, this is NOT gated on `--with-debug-ui`: the DI
    /// root (`lib.rs`) attaches it unconditionally via `with_turn_capture`,
    /// keyed only on `config.turn_capture_dir` (spec Design overview #1 --
    /// its own instrumentation gate, independent of the debug UI).
    turn_capture: crate::turn_capture::TurnCapture,
    /// D6 AbortHub: the live-cancellation registry keyed by `api_call_id`, so the
    /// dashboard kill route can cancel a stuck server-side stream. Gated identically to
    /// the FlowStore (enabled iff `flow_store.is_enabled()`), because the D3 L1 guard —
    /// which registers/removes the kill token — only exists on the enabled FlowStore
    /// path. `disabled()` makes every op a no-op (no map, no lock), so production keeps
    /// zero overhead. Cloning the Gateway shares the inner `Arc`, keeping the derived
    /// `Clone`.
    abort_hub: crate::dashboard_flow::AbortHub,
    /// Throttle state for the "requested model not served → fell back to the
    /// default catalog model" WARN. Every request resolves the model TWICE (the
    /// HTTP layer to label the response, then the engine to drive the upstream
    /// call), so without throttling a persistent mismatch logs the same WARN
    /// twice per request forever. Keyed by requested model; fires once per
    /// catalog-TTL window, mirroring claude-relay's once-per-detection logging.
    model_fallback_warned: Arc<std::sync::Mutex<HashMap<String, std::time::Instant>>>,
    /// E1: always-on (NOT dashboard-gated) bounded counter for hallucinated
    /// unoffered tool calls, keyed `{provider, served_model, outcome}`. The
    /// incident this guards was INVISIBLE in production logs, so unlike the
    /// dashboard `MetricsLayer` (which is `disabled()` without `--with-debug-ui`)
    /// this aggregate is always live; the `tracing::warn!` at the reject site is
    /// the primary operator signal and this is the bounded count. Raw tool names
    /// are NEVER labels (cardinality). `Arc<Mutex<..>>` so a cloned `Gateway`
    /// shares one count, mirroring `model_fallback_warned`.
    unknown_tool_call_counts: Arc<std::sync::Mutex<BTreeMap<UnknownToolCounterKey, u64>>>,
    /// Process-wide negative capability cache for the optional backend
    /// `/tokenize` endpoint. The routing implementation probes all eligible
    /// candidates before returning unsupported.
    tokenize_capability: Arc<std::sync::Mutex<TokenizeCapability>>,
}

#[derive(Debug, Clone)]
pub struct SseEvent {
    pub event: String,
    pub data: Value,
}

#[derive(Clone)]
struct CachedUpstreamModelCatalog {
    fetched_at: std::time::Instant,
    catalog: UpstreamModelCatalog,
}

#[derive(Clone, Default)]
struct UpstreamModelCatalog {
    ids: Vec<String>,
    ids_by_key: HashMap<String, Vec<String>>,
    /// Per-model context-window length (keyed by upstream catalog id) parsed
    /// from `/v1/models`. T9 moved ROUTING-mode budgeting to the routing
    /// layer's `BackendCandidatePlan` (conservative MIN over per-provider
    /// limits); this engine catalog is the NON-ROUTING resolver (the single
    /// provider's catalog, which IS the served model's limit) and the fallback
    /// when the candidate plan has no known limits (all-unknown / catalog-load
    /// failure). `normalize_upstream_model`'s ladder uses only the id fields.
    context_limit_by_id: HashMap<String, i64>,
}

impl UpstreamModelCatalog {
    /// Build the catalog from a single `/v1/models` snapshot: the id list,
    /// `canonical_model_key` index, and per-model context limit all derive from
    /// the same entries, so normalization and (non-routing) budgeting describe
    /// one consistent provider state.
    fn from_entries(entries: Vec<UpstreamModelEntry>) -> Self {
        let mut ids = Vec::with_capacity(entries.len());
        let mut ids_by_key: HashMap<String, Vec<String>> = HashMap::new();
        let mut context_limit_by_id: HashMap<String, i64> = HashMap::new();
        for entry in entries {
            let key = canonical_model_key(&entry.id);
            if !key.is_empty() {
                ids_by_key.entry(key).or_default().push(entry.id.clone());
            }
            if let Some(limit) = entry.context_limit {
                context_limit_by_id.insert(entry.id.clone(), limit);
            }
            ids.push(entry.id);
        }
        Self {
            ids,
            ids_by_key,
            context_limit_by_id,
        }
    }

    /// Exact catalog id match (highest precedence). `None` when the model is
    /// blank or not an exact id.
    fn exact_id(&self, model: &str) -> Option<String> {
        let trimmed = model.trim();
        if trimmed.is_empty() {
            return None;
        }
        self.ids.iter().find(|id| id.as_str() == trimmed).cloned()
    }

    /// Unique canonical-key match (`canonical_model_key`). `None` when blank,
    /// unmatched, or ambiguous (maps to more than one id).
    fn canonical_unique(&self, model: &str) -> Option<String> {
        let trimmed = model.trim();
        if trimmed.is_empty() {
            return None;
        }
        let key = canonical_model_key(trimmed);
        let matches = self.ids_by_key.get(&key)?;
        let unique_ids = matches
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        (unique_ids.len() == 1)
            .then(|| matches.first().cloned())
            .flatten()
    }

    /// Default catalog id: first model of the catalog (blank/missing/ambiguous
    /// fallback). `None` only when the catalog is empty.
    fn default_id(&self) -> Option<String> {
        self.ids.first().cloned()
    }
}

/// Fixed token reserve subtracted from a model's context window when capping an
/// explicitly-requested output budget (G3 pre-flight budgeting). Mirrors
/// claude-relay's `_completion_token_margin = 128`: a deliberately
/// model-independent constant reserve, NOT a per-tokenizer computation.
const CONTEXT_BUDGET_MARGIN_TOKENS: i64 = 128;

/// The local size heuristic found no remaining context budget. This is a
/// budgeting signal, not proof of tokenizer overflow; the call site defers to
/// the upstream provider.
#[derive(Debug)]
struct ContextBudgetError;

/// Build the chat request whose serialized bytes the G3 estimate counts: the
/// LOWERED payload (`messages`/`tools`/`response_format`) passed through the SAME
/// `sanitize_chat_request` the upstream leaf applies before POSTing
/// (`flatten_content` from config). This is the terminal layer — nothing
/// transforms the body below `sanitize_chat_request`, so this is the closest
/// stable byte-size proxy for the actual wire body (e.g. multi-part text content
/// is flattened to a bare string here exactly as on the wire).
///
/// The ADDITIVE fields the leaf merges later (`extra_body`/`upstream_chat_kwargs`,
/// G2 family `chat_template_kwargs`, `temperature`/`stop`/penalties) are
/// deliberately OMITTED — they only ever GROW the real payload, so leaving them
/// out keeps the serialized-size proxy conservative and keeps G3 out of the
/// kwargs-merge seam (whose entanglement caused the original G3 thrash). This
/// byte-size relationship is not a lower-bound proof for every tokenizer.
///
/// `reasoning_effort` is ALSO omitted: a per-model `reasoning_effort_map` clears
/// the top-level field at the leaf and relays effort through the additive
/// `chat_template_kwargs` instead, so the field is not guaranteed on the wire.
/// Omitting it can only shrink the serialized-size proxy in both the mapped
/// (cleared) and unmapped (kept) cases.
/// Additive upstream-request fields that the G3 estimate and the dispatch loop
/// parameterize differently (T9). The COMMON base (`messages`/`tools`/
/// `response_format`/`tool_choice`/`stream`/`stream_options`/`parallel_tool_calls`)
/// is shared via [`build_upstream_chat_request`]; these additives are the seam
/// where the estimate deliberately uses conservative empty values while dispatch
/// uses the real values.
///
/// Why the estimate omits what it omits:
/// - `reasoning_effort`: a per-model `reasoning_effort_map` CLEARS the top-level
///   field at the leaf for mapped models, so it is not guaranteed on the wire.
///   Including it would inflate the proxy for mapped models; omitting can only
///   shrink it.
/// - `max_output_tokens`: budgeting CAPS this down, so the real payload carries
///   the (smaller) capped value. Including the uncapped request value could
///   over-count. Omitting is safe.
/// - `stop` / `temperature` / `top_p` / `frequency_penalty` / `presence_penalty`
///   / `extra_body`: the additive leaf merges (`upstream_chat_kwargs`,
///   `chat_template_kwargs`) happen at `finalize_request_for_backend`, AFTER the
///   `run_turn` build, so `extra_body` here is pre-leaf-merge and does NOT
///   include the kwargs that grow the payload. The proxy omits these scalars;
///   they only ever grow the real payload.
/// - `model`: the real model id is always on the wire, so the estimate uses the
///   real id (safe — it can only make the estimate LARGER, never over-count vs.
///   the wire since the wire carries the same id).
#[derive(Clone)]
struct UpstreamRequestAdditives {
    model: String,
    parallel_tool_calls: Option<bool>,
    reasoning_effort: Option<String>,
    max_output_tokens: Option<i64>,
    temperature: Option<f64>,
    top_p: Option<f64>,
    frequency_penalty: Option<f64>,
    presence_penalty: Option<f64>,
    stop: Option<Vec<String>>,
    extra_body: BTreeMap<String, Value>,
}

impl UpstreamRequestAdditives {
    /// Lower-bound-safe additives for the G3 estimate: real `model` (on the
    /// wire), everything else empty/None (see [`UpstreamRequestAdditives`]).
    fn for_estimate(model: String) -> Self {
        Self {
            model,
            parallel_tool_calls: None,
            reasoning_effort: None,
            max_output_tokens: None,
            temperature: None,
            top_p: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            extra_body: BTreeMap::new(),
        }
    }
}

/// The ONE first-upstream-request builder (T9): shared by the G3 estimate and
/// the `run_turn` dispatch loop so the request shape has a single source of
/// truth. The COMMON base (`messages`/`tools`/`response_format`/`tool_choice`)
/// is identical for both callers; the [`UpstreamRequestAdditives`] parameter is
/// the seam where the estimate uses lower-bound-safe empties and dispatch uses
/// the real values. `tool_choice` is passed in because the dispatch loop mutates
/// it across turns (forced `tool_choice` on turn 1 only), while the estimate
/// always uses the run_turn seam's default (`"auto"`, cleared by
/// `sanitize_chat_request` when there are no tools).
fn build_upstream_chat_request(
    messages: Vec<ChatMessage>,
    tools: Option<Vec<crate::models::chat::ChatTool>>,
    response_format: Option<Value>,
    tool_choice: Value,
    additives: UpstreamRequestAdditives,
) -> ChatCompletionRequest {
    ChatCompletionRequest {
        model: additives.model,
        messages,
        stream: true,
        tools,
        tool_choice: Some(tool_choice),
        parallel_tool_calls: additives.parallel_tool_calls,
        reasoning_effort: additives.reasoning_effort,
        response_format,
        stream_options: Some(StreamOptions {
            include_usage: true,
        }),
        temperature: additives.temperature,
        top_p: additives.top_p,
        max_output_tokens: additives.max_output_tokens,
        frequency_penalty: additives.frequency_penalty,
        presence_penalty: additives.presence_penalty,
        stop: additives.stop,
        extra_body: additives.extra_body,
    }
}

/// Build the G3 estimate request from the LOWERED payload: the COMMON base via
/// [`build_upstream_chat_request`] with lower-bound-safe additives
/// ([`UpstreamRequestAdditives::for_estimate`]), then `sanitize_chat_request`
/// (the terminal leaf transform — nothing transforms the body below it, so
/// it is the closest stable proxy for the wire body). Private: only
/// `estimate_input_tokens` calls it. The G3 test oracle (T9) builds its OWN
/// independent normalization of the recorded request — it does NOT call this
/// fn, so estimator-vs-oracle drift is detectable. `resolved_model` is the
/// backend model id the leaf POSTs, so the proxy includes that real id.
fn estimate_request_from_lowered(
    messages: &[ChatMessage],
    tools: &[crate::models::chat::ChatTool],
    response_format: &Option<Value>,
    flatten_content: bool,
    resolved_model: &str,
) -> ChatCompletionRequest {
    let request = build_upstream_chat_request(
        messages.to_vec(),
        (!tools.is_empty()).then(|| tools.to_vec()),
        response_format.clone(),
        // Mirror the run_turn seam's default; `sanitize_chat_request` clears it
        // when there are no tools, matching the wire body.
        Value::String("auto".to_string()),
        UpstreamRequestAdditives::for_estimate(resolved_model.to_string()),
    );
    sanitize_chat_request(request, flatten_content)
}

/// Coarse, deterministic estimate of the input tokens
/// the FIRST upstream turn will consume. Originally G3 pre-flight budgeting
/// only; C3 additionally rides this value onto `response.created`
/// (`created_event`) so the Anthropic streaming converter can seed
/// `message_start.usage.input_tokens` with a plausible non-zero number instead
/// of a hardcoded `0` (the real upstream tokenizer count is not known until
/// `response.completed`'s usage arrives, well after `message_start` is sent).
///
/// Option B, terminal layer: the estimate counts the EXACT serialized bytes the
/// leaf POSTs — the lowered payload after `sanitize_chat_request`
/// (`estimate_request_from_lowered`) — NOT the canonical `ResponsesRequest` and
/// NOT the pre-sanitize lowered messages. Because no transform exists below
/// `sanitize_chat_request`, no field (dropped `Message` subfields,
/// `text.verbosity`, `reasoning.summary`, raw `ToolSpec`, `ImageGenerationCall`,
/// or leaf content-flattening of multi-part text) can inflate the estimate.
/// `ceil(serialized_bytes / 4)` is an intentional coarse heuristic, not a
/// tokenizer. Tokenizers can compress some inputs below that ratio or expand
/// others above it, so this value may cap an explicit output budget
/// conservatively but must never be the sole reason to reject a request.
///
/// The serialized request omits only additive per-provider config/family kwargs
/// merged at the leaf (G2), but byte length is not a proof about tokenizer
/// output. Any under-count is absorbed by G1's reactive shrink-and-retry; a
/// would-be local overflow is deferred to that exact provider path. The estimate
/// covers the first upstream turn only; later tool-loop turns rely on G1.
fn estimate_input_tokens(
    lowered: &LoweredTurn,
    flatten_content: bool,
    resolved_model: &str,
) -> i64 {
    let request = estimate_request_from_lowered(
        &lowered.messages,
        &lowered.tools,
        &lowered.response_format,
        flatten_content,
        resolved_model,
    );
    // `serde_json` serialization is deterministic for this type, so the byte
    // count (and thus the estimate) is stable. The G3 test oracle (T9) builds
    // an INDEPENDENT normalization of the recorded request — it does not call
    // this fn — so estimator-vs-oracle drift surfaces as a test failure.
    let bytes = serde_json::to_vec(&request).map(|v| v.len()).unwrap_or(0);
    // ceil(bytes / 4): ~4 bytes per token is the standard coarse approximation.
    bytes.div_ceil(4) as i64
}

/// Cap an explicitly-requested output-token budget down to what the model's
/// context window can still fit after the estimated input and the fixed margin.
///
/// Pure and unit-testable; `Err` means the heuristic found no remaining budget,
/// and the call site defers to the provider tokenizer rather than returning a
/// false-positive 400. Rules (mirroring claude-relay
/// `_cap_max_completion_tokens`):
/// - `available = context_limit - estimated_input_tokens - margin`.
/// - `available <= 0` ⇒ `Err` (input + margin already exhausts the context).
/// - an explicit positive request is capped to `min(requested, available)`;
///   it is NEVER raised, and an absent/non-positive request is left untouched
///   (G3 never synthesizes a cap — G1 stays the reactive net).
fn budget_explicit_max_output_tokens(
    requested: Option<i64>,
    context_limit: i64,
    estimated_input_tokens: i64,
) -> Result<Option<i64>, ContextBudgetError> {
    let available = context_limit - estimated_input_tokens - CONTEXT_BUDGET_MARGIN_TOKENS;
    if available <= 0 {
        return Err(ContextBudgetError);
    }
    Ok(match requested {
        Some(n) if n > 0 => Some(n.min(available)),
        other => other,
    })
}

/// Conservative floor for G3 pre-flight budgeting (T9): the STRICTEST context
/// window across the candidate set's KNOWN per-model limits (the min). A
/// failover to a smaller-window model then constrains the output-budget cap to
/// the tightest backend that could serve.
/// Candidates with no reported window (`None`) are skipped (unknown ⇒ no-op,
/// matching pre-T9). Returns `None` when the set is empty OR no candidate
/// reports a window ⇒ budgeting no-ops entirely.
fn candidate_context_floor(plan: &crate::upstream::BackendCandidatePlan) -> Option<i64> {
    plan.candidates
        .iter()
        .filter_map(|candidate| candidate.context_limit)
        .min()
}

/// F1c (review r1, finding #3): the clean-completion shape `run_turn` reports up to
/// the terminal seam so the turn-capture artifact status can distinguish a genuine
/// stop from a max-token truncation. A `finish_reason: length` turn completes
/// cleanly (`Ok`) at the HTTP/serving layer -- the dashboard `FlowStore` still counts
/// it `Completed` -- but its response was CUT SHORT, so the capture artifact must not
/// claim `completed` (don't-lie-with-zeros); it maps to `incomplete`. Failed /
/// cancelled turns are the `Err` arm and never reach this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TurnCompletion {
    /// The turn ended on a genuine stop (upstream `response.completed`).
    Completed,
    /// The turn was truncated by the upstream output-token cap (`finish_reason:
    /// length` ⇒ `response.incomplete`).
    Incomplete,
}

/// F1c: map the engine's terminal `FlowStatus` to the turn-capture artifact status
/// string. `Open` never reaches a terminal seam; treat it as `failed` defensively
/// rather than emit a non-terminal status into the artifact.
fn flow_status_artifact_str(status: crate::dashboard_flow::FlowStatus) -> &'static str {
    match status {
        crate::dashboard_flow::FlowStatus::Completed => "completed",
        crate::dashboard_flow::FlowStatus::Cancelled => "cancelled",
        crate::dashboard_flow::FlowStatus::Failed | crate::dashboard_flow::FlowStatus::Open => {
            "failed"
        }
    }
}

/// Whether a Chat-Completions inbound request asked to receive reasoning.
/// `reasoning_effort: none` keeps the backend reasoning parser active but
/// suppresses its reasoning channel at the client boundary. Other explicit
/// effort values and explicit thinking knobs request the reasoning channel.
fn chat_request_requested_reasoning(request: &ChatCompletionRequest) -> bool {
    if let Some(effort) = request.reasoning_effort.as_deref() {
        return !effort.trim().eq_ignore_ascii_case("none");
    }
    request
        .extra_body
        .get("chat_template_kwargs")
        .and_then(Value::as_object)
        .is_some_and(|kwargs| {
            let reasoning_effort_requested = kwargs
                .get("reasoning_effort")
                .and_then(Value::as_str)
                .is_some_and(|effort| !effort.trim().eq_ignore_ascii_case("none"));
            kwargs.contains_key("thinking")
                || kwargs.contains_key("enable_thinking")
                || reasoning_effort_requested
        })
}

fn build_upstream_extra_body(
    defaults: serde_json::Map<String, Value>,
    request: &ResponsesRequest,
    response_format: &Option<Value>,
    reasoning_effort: &Option<String>,
) -> BTreeMap<String, Value> {
    let mut extra_body = defaults.into_iter().collect();
    remove_defaults_for_explicit_request_fields(
        &mut extra_body,
        request,
        response_format,
        reasoning_effort,
    );
    remove_defaults_shadowed_by_request_extra(&mut extra_body, &request.extra_body);
    for (key, value) in &request.extra_body {
        merge_request_extra_value(&mut extra_body, key, value);
    }
    extra_body
}

fn remove_defaults_for_explicit_request_fields(
    extra_body: &mut BTreeMap<String, Value>,
    request: &ResponsesRequest,
    response_format: &Option<Value>,
    reasoning_effort: &Option<String>,
) {
    if request.temperature.is_some() {
        remove_keys(extra_body, &["temperature"]);
    }
    if request.top_p.is_some() {
        remove_keys(extra_body, &["top_p"]);
    }
    if request.max_output_tokens.is_some() {
        remove_keys(
            extra_body,
            &["max_tokens", "max_output_tokens", "max_completion_tokens"],
        );
    }
    if request.frequency_penalty.is_some() {
        remove_keys(extra_body, &["frequency_penalty"]);
    }
    if request.presence_penalty.is_some() {
        remove_keys(extra_body, &["presence_penalty"]);
    }
    if response_format.is_some() {
        remove_keys(extra_body, &["response_format"]);
    }
    if reasoning_effort.is_some() {
        remove_keys(extra_body, &["reasoning_effort"]);
    }
}

fn remove_defaults_shadowed_by_request_extra(
    extra_body: &mut BTreeMap<String, Value>,
    request_extra: &BTreeMap<String, Value>,
) {
    for aliases in [&["max_tokens", "max_output_tokens", "max_completion_tokens"][..]] {
        if aliases.iter().any(|key| request_extra.contains_key(*key)) {
            remove_keys(extra_body, aliases);
        }
    }
}

fn remove_keys(extra_body: &mut BTreeMap<String, Value>, keys: &[&str]) {
    for key in keys {
        extra_body.remove(*key);
    }
}

fn merge_request_extra_value(extra_body: &mut BTreeMap<String, Value>, key: &str, value: &Value) {
    if key == "chat_template_kwargs"
        && let Some(existing) = extra_body.get_mut(key)
    {
        merge_json_value_prefer_source(existing, value);
        return;
    }
    extra_body.insert(key.to_string(), value.clone());
}

fn merge_json_value_prefer_source(destination: &mut Value, source: &Value) {
    if let Value::Object(destination_object) = destination
        && let Value::Object(source_object) = source
    {
        for (key, source_value) in source_object {
            match destination_object.get_mut(key) {
                Some(destination_value) => {
                    merge_json_value_prefer_source(destination_value, source_value);
                }
                None => {
                    destination_object.insert(key.clone(), source_value.clone());
                }
            }
        }
        return;
    }
    *destination = source.clone();
}

/// The nearest FUTURE cooldown deadline in a health vector, as a duration from
/// now (D4 publication-task wake). Each `cooling_until_ms` is wall-clock epoch-ms;
/// we subtract the current epoch-ms to get the remaining time. `None` when no
/// provider is cooling (the task then just waits for the next 1 s tick). A
/// deadline already at/behind now yields `Duration::ZERO` (wake immediately), so
/// the elapsed window is observed on the very next recompute.
fn next_cooldown_wake(health: &[ProviderHealth]) -> Option<std::time::Duration> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    health
        .iter()
        .filter_map(|provider| provider.cooling_until_ms)
        .map(|until_ms| std::time::Duration::from_millis(until_ms.saturating_sub(now_ms)))
        .min()
}

impl Gateway {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        replay_store: ReplayStore,
        upstream: Arc<dyn UpstreamClient>,
        search: Arc<dyn SearchClient>,
        vision: Arc<dyn VisionClient>,
        image_cache: Arc<ImageCache>,
        monitor: MonitorHub,
        raw_output: Option<RawOutput>,
        flow_store: crate::dashboard_flow::DashboardFlowStore,
    ) -> Self {
        // D6: gate the AbortHub on the SAME flag as the FlowStore — the L1 guard that
        // owns the kill token's lifecycle only exists when the store is enabled, so a
        // disabled store ⇒ a disabled hub (no map, no lock) keeps production overhead
        // at zero without widening this constructor or touching the DI root.
        let abort_hub = if flow_store.is_enabled() {
            crate::dashboard_flow::AbortHub::new()
        } else {
            crate::dashboard_flow::AbortHub::disabled()
        };
        Self {
            config,
            replay_store,
            upstream,
            search,
            vision,
            image_cache,
            monitor,
            raw_output,
            flow_store,
            abort_hub,
            dashboard_auth: None,
            upstream_model_catalog: Arc::new(Mutex::new(None)),
            provider_health: ProviderHealthPublisher::default(),
            // D5: disabled by default (zero overhead); the DI root attaches an
            // enabled layer via `with_metrics` in the `--with-debug-ui` branch.
            metrics: crate::metrics::MetricsLayer::disabled(),
            // F1: disabled by default (zero overhead); the DI root attaches an
            // enabled sink via `with_turn_capture` when `turn_capture_dir` is
            // configured -- independent of `--with-debug-ui`.
            turn_capture: crate::turn_capture::TurnCapture::disabled(),
            model_fallback_warned: Arc::new(std::sync::Mutex::new(HashMap::new())),
            unknown_tool_call_counts: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            tokenize_capability: Arc::new(std::sync::Mutex::new(TokenizeCapability::Unknown)),
        }
    }

    /// E1: record one unknown-tool-call outcome into the bounded always-on
    /// counter. Folds a fresh `{provider, served_model}` into the bounded
    /// overflow key once the map is full, so the key space cannot grow without
    /// bound under attacker-influenced labels.
    fn record_unknown_tool_outcome(
        &self,
        provider: &str,
        served_model: &str,
        outcome: UnknownToolOutcome,
    ) {
        let mut counts = self
            .unknown_tool_call_counts
            .lock()
            .expect("unknown tool counter lock poisoned");
        let key = (provider.to_string(), served_model.to_string(), outcome);
        if !counts.contains_key(&key) && counts.len() >= MAX_UNKNOWN_TOOL_COUNTER_KEYS {
            let overflow = (
                UNKNOWN_TOOL_COUNTER_OVERFLOW_KEY.to_string(),
                UNKNOWN_TOOL_COUNTER_OVERFLOW_KEY.to_string(),
                outcome,
            );
            *counts.entry(overflow).or_insert(0) += 1;
            return;
        }
        *counts.entry(key).or_insert(0) += 1;
    }

    /// E1: snapshot of the unknown-tool-call counter (`{provider, served_model,
    /// outcome} -> count`). Exposed for tests / introspection.
    pub fn unknown_tool_call_counts(&self) -> BTreeMap<UnknownToolCounterKey, u64> {
        self.unknown_tool_call_counts
            .lock()
            .expect("unknown tool counter lock poisoned")
            .clone()
    }

    /// Attach the D5 enabled [`MetricsLayer`](crate::metrics::MetricsLayer) (built in
    /// the `--with-debug-ui` DI branch). Consuming builder so it threads through the
    /// `Gateway::new(...)` → `Arc::new` construction WITHOUT widening the constructor
    /// signature (every test that builds a bare `Gateway` keeps the default
    /// `disabled()` layer — zero overhead). Mirrors `with_dashboard_auth`.
    pub fn with_metrics(mut self, metrics: crate::metrics::MetricsLayer) -> Self {
        self.metrics = metrics;
        self
    }

    /// Access the D5 metrics layer. `is_enabled()` is `false` when the debug UI is
    /// off, in which case every metrics op is a no-op (zero lock, zero work).
    pub fn metrics(&self) -> &crate::metrics::MetricsLayer {
        &self.metrics
    }

    /// Attach the F1 [`TurnCapture`](crate::turn_capture::TurnCapture) sink (built in
    /// the DI root from `config.turn_capture_dir`). Consuming builder so it threads
    /// through the `Gateway::new(...)` → `Arc::new` construction WITHOUT widening the
    /// constructor signature (every test that builds a bare `Gateway` keeps the
    /// default `disabled()` sink -- zero overhead). Mirrors `with_metrics`.
    pub fn with_turn_capture(mut self, turn_capture: crate::turn_capture::TurnCapture) -> Self {
        self.turn_capture = turn_capture;
        self
    }

    /// Access the F1 durable per-turn capture handle. `is_enabled()` is `false`
    /// when `turn_capture_dir` is unset, in which case every capture op is a
    /// no-op (no thread, no alloc, no fs). Independent of the debug UI.
    pub fn turn_capture(&self) -> &crate::turn_capture::TurnCapture {
        &self.turn_capture
    }

    /// D5: record a TERMINAL response into the metrics rings at the engine's D3
    /// terminal finalize seam — co-located with `guard.finalize(...)` so it fires at
    /// the SAME single CAS-guarded choke point (exactly once per flow, NOT from the
    /// middleware, NOT per chunk). Sources the served model + endpoint + upstream + the
    /// flow's FINAL cumulative usage from the GUARD's own evict-safe copy
    /// ([`TerminalMetricsInputs`](crate::dashboard_flow::TerminalMetricsInputs)) — the
    /// endpoint captured at claim, the model/upstream/usage from the shared
    /// `ServingToken` the guard holds — NOT by re-reading the record via `detail()`.
    /// D5 R3 (MEDIUM): the FlowStore can prune (TTL) or evict (cap) a long-running /
    /// high-concurrency flow BEFORE this runs, so a `detail()` re-read would `None`-out
    /// and make the authoritative metrics layer UNDERCOUNT completed requests; recording
    /// from the guard's own copy makes metrics independent of FlowStore retention. The
    /// latency is the guard's monotonic `elapsed`. No-op when the metrics layer is
    /// disabled OR the guard never finalized a live record (bare/non-instrumented
    /// paths), so it is zero-overhead off the dashboard path. MUST be called AFTER
    /// `guard.finalize(...)` (which assembles the inputs).
    fn record_terminal_metrics(
        &self,
        guard: &crate::dashboard_flow::TelemetryGuard,
        status: crate::dashboard_flow::FlowStatus,
        elapsed_ms: u128,
    ) {
        if !self.metrics.is_enabled() {
            return;
        }
        // The guard assembled the authoritative served-model / endpoint / upstream
        // attribution + final cumulative usage at finalize, from its claim-captured
        // endpoint + the shared ServingToken — so this is evict-safe (no `detail()`
        // re-read of a possibly-pruned record).
        let Some(inputs) = guard.terminal_metrics() else {
            return;
        };
        // D5 R1 #2: record the terminal response AND the flow's FINAL cumulative token
        // usage in ONE atomic metrics call (single lock, single epoch/slot), into the
        // SAME `{status, model, endpoint, upstream}` bucket — so a concurrent 5 s
        // snapshot can never split the count and the tokens across two different 1 s
        // slots.
        self.metrics.record_terminal(
            status,
            inputs.model_served.as_deref(),
            &inputs.endpoint,
            inputs.upstream.as_deref(),
            elapsed_ms,
            inputs.usage,
            // Gap 12: the evict-safe per-attempt trace (spec 03) feeds the per-provider
            // latency/error rings off the SAME terminal payload as `usage` — NOT a
            // re-read of the evictable FlowStore record, so a failed primary on a flow
            // whose record was pruned/evicted before finalize is still counted.
            &inputs.attempts,
        );
    }

    /// Attach the D7 dashboard auth context (built from the environment in the
    /// DI root). Consuming builder so it threads through the `Gateway::new(...)`
    /// → `Arc::new` construction without widening the constructor signature
    /// (every test that builds a bare `Gateway` keeps `dashboard_auth: None`).
    pub fn with_dashboard_auth(
        mut self,
        auth: Option<Arc<crate::dashboard_auth::DashboardAuth>>,
    ) -> Self {
        self.dashboard_auth = auth;
        self
    }

    /// The dashboard/`/debug` auth context, when the protected routes are
    /// registered. `None` when the debug UI is off or the startup decision
    /// refused registration.
    pub fn dashboard_auth(&self) -> Option<Arc<crate::dashboard_auth::DashboardAuth>> {
        self.dashboard_auth.clone()
    }

    /// Access the dashboard FlowStore (D1). `is_enabled()` is `false` when the
    /// debug UI is off, in which case every store op is a no-op.
    pub fn flow_store(&self) -> &crate::dashboard_flow::DashboardFlowStore {
        &self.flow_store
    }

    /// D6: access the AbortHub (the live-cancellation registry keyed by `api_call_id`).
    /// Disabled (every op a no-op) when the debug UI is off.
    pub fn abort_hub(&self) -> &crate::dashboard_flow::AbortHub {
        &self.abort_hub
    }

    /// D6: cancel the live server-side stream for `api_call_id`, returning whether a
    /// live token was found (`true` ⇒ a stream was cancelled; `false` ⇒ unknown or
    /// already-finished flow — the kill route maps this to `200`/`404`). The kill flips
    /// the flow's shared `CancellationToken`; the engine's cancel sites (composed with
    /// the existing `tx.closed()` client-hangup checks) then surface
    /// `AppError::cancelled()` (HTTP 499) and the L1 guard's `Drop` finalizes the record
    /// `Cancelled` (which also removes the token from the hub). No tokens are duplicated
    /// or replayed — a mid-stream kill is a cancel, not a retry (AGENTS.md "Failover
    /// only pre-first-chunk"). The mutation+CSRF gate is applied by the route layer
    /// (D7/D13 via [`MutationPolicy`](crate::dashboard_auth::MutationPolicy)), NOT here.
    pub fn abort(&self, api_call_id: &str) -> bool {
        self.abort_hub.abort(api_call_id)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// D13: the configured [`ModelPrice`](crate::config::ModelPrice) for `model`,
    /// keyed by the SERVED model id (exact, then case-insensitive). `None` when no
    /// price is configured — the dashboard then reports no `cost` for that flow.
    /// A thin pass-through to [`Config::price_for`] so the dashboard REST handlers
    /// + the flow-cost roll-up resolve prices without reaching into `config()`.
    pub fn price_for(&self, model: &str) -> Option<crate::config::ModelPrice> {
        self.config.price_for(model)
    }

    /// D13: the whole per-model price table (`/dashboard/api/topology` returns it,
    /// the Sankey colors edges from it). A borrow of the `Config`-owned map; empty
    /// when none is configured (contract-valid — an empty `price_table` validates).
    pub fn price_table(&self) -> &std::collections::HashMap<String, crate::config::ModelPrice> {
        &self.config.price_table
    }

    pub fn upstream_client(&self) -> Arc<dyn UpstreamClient> {
        Arc::clone(&self.upstream)
    }

    pub fn tokenize_capability(&self) -> TokenizeCapability {
        *self
            .tokenize_capability
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn set_tokenize_capability(&self, capability: TokenizeCapability) {
        *self
            .tokenize_capability
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = capability;
    }

    /// D4: the current per-upstream health + counters (lock-free read through
    /// the `UpstreamClient` trait). This is the LIVE view computed on demand; the
    /// publication task calls it on each tick to refresh the versioned snapshot.
    /// Bare/single-upstream gateways return an empty vector (no provider layer
    /// owns health), matching the trait default.
    pub fn upstream_health(&self) -> Vec<ProviderHealth> {
        self.upstream.provider_health()
    }

    /// D4: the published topology-health handle. D5's 5 s snapshot task captures
    /// `latest()` (one `Arc`); D7b broadcasts it as a `TopologyUpdate`. Always
    /// present — the refreshing TASK is what `--with-debug-ui` gates, not this
    /// accessor.
    pub fn provider_health_publisher(&self) -> ProviderHealthPublisher {
        self.provider_health.clone()
    }

    /// Spawn the D4 topology-health publication task (gated by `--with-debug-ui`
    /// at the DI root — production must NOT run this, keeping the disabled path
    /// zero-overhead). The task COALESCES publications on a 1 s tick AND wakes
    /// early at the nearest provider cooldown deadline, so an IDLE cooling→Healthy
    /// transition is published with no traffic. It does NOT republish per served
    /// flow (the atomics update continuously; the snapshot reads them at tick
    /// time), so it allocates O(providers) at most once per second, never per
    /// request.
    ///
    /// The task holds only `Arc` clones (the upstream client + the publisher), so
    /// it is `Send + 'static` and outlives this call; it runs for the process
    /// lifetime (the gateway is an `Arc` held by the server). Returns the
    /// `JoinHandle` so a caller/test can abort it deterministically.
    pub fn spawn_provider_health_publisher(&self) -> tokio::task::JoinHandle<()> {
        let upstream = Arc::clone(&self.upstream);
        let publisher = self.provider_health.clone();
        // Publish an initial snapshot immediately so a consumer that reads before
        // the first tick still sees the current health (version 1), not the empty
        // default.
        publisher.publish(upstream.provider_health());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let health = upstream.provider_health();
                // Wake at the nearest future cooldown deadline if one is sooner
                // than the next 1 s tick, so an idle cooling→Healthy flip is
                // published right at the deadline (not up to a second late).
                let next_deadline = next_cooldown_wake(&health);
                publisher.publish(health);
                match next_deadline {
                    Some(wake) if wake < std::time::Duration::from_secs(1) => {
                        // A tiny epsilon past the deadline so the recomputed status
                        // observes the window as elapsed.
                        tokio::time::sleep(wake + std::time::Duration::from_millis(1)).await;
                    }
                    _ => {
                        tick.tick().await;
                    }
                }
            }
        })
    }

    /// Resolve `request_model` to the served upstream model and whether the
    /// resolution was GENUINE (the request truly maps to the served backend, not
    /// a catalog-default fallback). `genuine` is a byproduct of the one
    /// normalization ladder — NOT a re-derived side-channel (T2 deleted
    /// `request_model_genuinely_resolves`, which walked the ladder a second
    /// time). G4 native-vision gating consumes it so a request-model
    /// `native_vision` override attaches ONLY when the request genuinely maps to
    /// the served backend (G4 round-8 #1).
    pub async fn resolve_request_model(&self, request_model: &str) -> (String, bool) {
        let configured_model = self.config.resolve_upstream_model(request_model);
        self.normalize_upstream_model(&configured_model).await
    }

    /// Decide whether the Chat output converter must suppress
    /// `reasoning_content` for this inbound request. We suppress whenever the
    /// inbound Chat client did NOT request reasoning, for ALL models and
    /// independent of the backend family (G2, Finding 1). Cross-family
    /// routing/failover means the engine-resolved family is not a reliable proxy
    /// for what the backend will actually emit, so the decision is computed
    /// purely from the inbound request at the HTTP boundary: a Chat client that
    /// never asked for reasoning must never receive server-side chain-of-thought
    /// (AGENTS.md: do not leak server-side internals to Chat).
    ///
    /// The client is considered to have requested reasoning if it sent a
    /// non-`none` `reasoning_effort` value OR explicitly set a thinking knob
    /// (`thinking` / `enable_thinking`) in its `chat_template_kwargs`. A
    /// `reasoning_effort` value of `none` keeps forced backend reasoning out of
    /// `reasoning_content`.
    pub fn chat_reasoning_suppressed(&self, request: &ChatCompletionRequest) -> bool {
        !chat_request_requested_reasoning(request)
    }

    pub fn subscribe_monitor(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::monitor::DebugUpdate> {
        self.monitor.subscribe()
    }

    pub fn debug_snapshot(&self) -> crate::monitor::DebugSnapshot {
        self.monitor.snapshot()
    }

    /// Forward one SSE event to the client and (in `--raw` mode) mirror it to
    /// stdout.
    ///
    /// D3 (R1 #1): a `tx.send` failure has exactly ONE cause here — the SSE
    /// receiver was dropped, i.e. the client hung up MID-SEND (not parked in
    /// `next_upstream_chunk`'s `tx.closed()` select). `mpsc::Sender::send`
    /// awaits capacity and only errors when every receiver is gone, so this is
    /// unambiguously a cancellation, NOT an internal error. Returning
    /// `AppError::cancelled()` (HTTP 499) makes this the SINGLE source of truth
    /// so the spawned finalize choke classifies the flow `Cancelled`, honoring
    /// the AGENTS.md hard rule that a client hang-up surfaces as cancellation.
    /// (The `raw_output` write below is a genuine local IO fault and stays
    /// `internal`.) The old per-call `failure_message` argument is gone because a
    /// closed receiver is no longer a generic internal error needing a detail.
    ///
    /// D6 (R1): the SEND itself is CANCELLABLE. A full-but-OPEN 128-slot channel
    /// (client connected but not draining) parks `tx.send().await` on capacity —
    /// `send` only resolves on capacity OR a fully-closed receiver, so without
    /// composing the kill token a dashboard `abort()` was MISSED: the task blocked
    /// here forever, upstream work was never torn down, and the AbortHub entry
    /// stayed live until the client finally drained/disconnected. Racing the send
    /// against `abort_token.cancelled()` makes BOTH a closed channel (hang-up, the
    /// D3 path above) AND the kill token (dashboard kill) cancel the send; the kill
    /// branch surfaces `cancelled()` (499), so `run_turn` returns Cancelled at the
    /// spawn terminal-match → upstream dropped, the finalizing guard removes the
    /// AbortHub entry. `biased` checks the (rare) kill before the send each poll.
    async fn send_event(
        &self,
        tx: &mpsc::Sender<SseEvent>,
        event: SseEvent,
        abort_token: &tokio_util::sync::CancellationToken,
    ) -> AppResult<()> {
        let raw_event = event.clone();
        tokio::select! {
            biased;
            _ = abort_token.cancelled() => return Err(AppError::cancelled()),
            result = tx.send(event) => result.map_err(|_| AppError::cancelled())?,
        }
        if let Some(raw_output) = &self.raw_output {
            raw_output
                .write_sse_event(&raw_event)
                .map_err(|err| AppError::internal(format!("failed to write raw output: {err}")))?;
        }
        Ok(())
    }

    /// Forward one gated `function_call_arguments` delta: mirror it to the
    /// monitor hub and stream it as an SSE event. This is the single emission
    /// path the [`ToolDeltaGate`] feeds — previously inlined (and duplicated)
    /// across the fast path, the `Emit`/flush branches, and the turn-end flush.
    async fn emit_function_call_delta(
        &self,
        response_id: &str,
        tx: &mpsc::Sender<SseEvent>,
        emission: DeltaEmission,
        abort_token: &tokio_util::sync::CancellationToken,
    ) -> AppResult<()> {
        let DeltaEmission {
            call_id,
            name,
            delta,
        } = emission;
        // The monitor and the SSE event each consume an owned `call_id`/`delta`
        // for this one fragment, so one clone of each is inherent here — the
        // pre-T3 inline code cloned identically at every emission site. `call_id`
        // and `delta` are then MOVED into the SSE event (their last use).
        self.monitor.emit_with(response_id, || {
            MonitorEventKind::FunctionCallArgumentsDelta {
                call_id: call_id.clone(),
                delta: delta.clone(),
            }
        });
        self.send_event(
            tx,
            function_call_args_delta_event(call_id, name, delta),
            abort_token,
        )
        .await
    }

    /// Drive a [`ToolDeltaGate`] decision to the wire, in order. `None` emits
    /// nothing; `One` forwards the single delta; `Flush` emits the gate's
    /// moved-out buffered fragments (iterated in place, no copy) then the
    /// optional trailing delta. Allocation-free beyond the `String`s the gate
    /// already owns.
    async fn drive_delta_decision(
        &self,
        response_id: &str,
        tx: &mpsc::Sender<SseEvent>,
        decision: DeltaDecision,
        abort_token: &tokio_util::sync::CancellationToken,
    ) -> AppResult<()> {
        match decision {
            DeltaDecision::None => {}
            DeltaDecision::One(emission) => {
                self.emit_function_call_delta(response_id, tx, emission, abort_token)
                    .await?;
            }
            DeltaDecision::Flush {
                call_id,
                buffered,
                trailing,
            } => {
                // One `call_id.clone()` per buffered fragment: each fragment is a
                // distinct emission that needs its own owned id, exactly as the
                // pre-T3 inline loop did (parity, no regression). The trailing
                // delta is the id's last use, so it MOVES `call_id` (no clone).
                for (name, delta) in buffered {
                    self.emit_function_call_delta(
                        response_id,
                        tx,
                        DeltaEmission {
                            call_id: call_id.clone(),
                            name,
                            delta,
                        },
                        abort_token,
                    )
                    .await?;
                }
                if let Some((name, delta)) = trailing {
                    self.emit_function_call_delta(
                        response_id,
                        tx,
                        DeltaEmission {
                            call_id,
                            name,
                            delta,
                        },
                        abort_token,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Public entry point: stream a canonical Responses request. Thin wrapper that
    /// delegates to [`stream_responses_with_api_call_id`](Self::stream_responses_with_api_call_id)
    /// with no `api_call_id`, so existing callers (tests, non-instrumented paths)
    /// keep this exact signature. The HTTP handlers pass the `api_call_id` from the
    /// request extension via the `_with_api_call_id` variant so the engine can
    /// `flow_store.link(response_id, api_call_id)` (D1).
    pub async fn stream_responses(
        self: Arc<Self>,
        request: ResponsesRequest,
    ) -> AppResult<ReceiverStream<SseEvent>> {
        self.stream_responses_with_api_call_id(request, None).await
    }

    pub async fn stream_responses_with_api_call_id(
        self: Arc<Self>,
        request: ResponsesRequest,
        api_call_id: Option<String>,
    ) -> AppResult<ReceiverStream<SseEvent>> {
        // D2/D3: ONE serving token per flow, allocated here (not per turn) so the L1
        // telemetry guard built BELOW and every per-turn `BackendChatRequest` in
        // `run_turn` share the SAME `Arc` — the failover/routing layers tag
        // `{route, provider}` on it, and the guard reads that pair at finalize.
        let serving_token = Arc::new(crate::upstream::ServingToken::default());
        // D3 L1: claim the flow record (`OpenL0 → ClaimedL1`) BEFORE any pre-spawn
        // early return. A lowering/budget failure below finalizes the record
        // explicitly `Failed` (via `finalize_pre_spawn_err`) so it carries the right
        // terminal — and even if a future pre-spawn path forgot to, the guard's
        // `Drop` is the backstop (never an orphan stuck `Open`). `None` when the
        // store is disabled or no `api_call_id` was threaded (the public
        // `stream_responses` wrapper / non-instrumented paths), so this is
        // zero-overhead off the dashboard path. On the success path the guard moves
        // into the `tokio::spawn` below (it holds only `Arc`s + an `Instant`, so it
        // is `Send`) and the spawned body finalizes it from the typed `Result`.
        let telemetry_guard = api_call_id.as_deref().and_then(|id| {
            self.flow_store()
                .engine_guard(id, Arc::clone(&serving_token), self.abort_hub())
        });
        // D6: the flow's cancellation token. The L1 guard registered it in the AbortHub
        // under `api_call_id` (so the kill route can flip it) and exposes a clone here;
        // the engine composes it with every `tx.closed()` client-hangup check below so a
        // kill surfaces `AppError::cancelled()` (499) exactly like a hang-up. Off the
        // dashboard path (no guard — disabled store or no `api_call_id`) this is a fresh
        // never-cancelled token, so the compose sites are uniform and zero-overhead (an
        // atomic-load `false` + a `cancelled()` future that never fires) with no per-site
        // `Option` branching.
        let abort_token = telemetry_guard
            .as_ref()
            .map(|guard| guard.abort_token())
            .unwrap_or_default();
        // F1c: the turn-capture ENGINE guard. `state(id)` reaches the SAME per-turn
        // state the HTTP layer's `start()` registered (F1b), so the engine terminal
        // and the served-body tee share one state. `None` when capture is disabled or
        // no `api_call_id` was threaded (the public wrapper / non-instrumented paths).
        // Built HERE, BEFORE the pre-spawn `?` paths (so a lowering/budget failure
        // finalizes it via `finalize_pre_spawn_err`), and moved into the terminal
        // `tokio::spawn` below — mirroring `telemetry_guard`. `new` CLAIMS the turn so
        // the middleware backstop stays inert; the guard's `Drop` is the
        // abandoned/panicked-turn fallback (`failed`, or `cancelled` if the abort
        // token fired).
        let capture_guard = api_call_id.as_deref().and_then(|id| {
            self.turn_capture()
                .state(id)
                .map(|state| crate::turn_capture::CaptureGuard::new(state, abort_token.clone()))
        });
        // D2 (D13 R1 HIGH): the ORIGINAL request model, captured BEFORE resolution so
        // the flow record's `model_requested` reflects what the CLIENT asked for (an
        // alias / ad-hoc route / profile name), distinct from the resolved/served
        // model the leaf records as `model_served`. Stamped onto the record via
        // `set_normalized` below alongside the normalized canonical body.
        let model_requested = request.model.clone();
        let (resolved_model, request_genuine) = self.resolve_request_model(&request.model).await;
        // F1c: stamp the resolved/served model onto the capture outcome metadata now
        // that resolution has settled (absent for a turn that fails before here).
        if let Some(guard) = &capture_guard {
            guard.set_model_served(&resolved_model);
        }
        let mut request = self.apply_system_prompt_prefix(request, &resolved_model);

        // D1/E2b: minted here rather than just before the `tokio::spawn` below
        // (its only prior use) so the E2b residual-image-degrade monitor
        // emission further down can key off the SAME id every other event for
        // this turn uses. Pure reordering: nothing between here and the spawn
        // reads or depends on this value.
        let response_id = format!("resp_{}", Uuid::new_v4().simple());

        // G4/E2b: the resolved-backend native-vision decision, computed ONCE
        // and shared between G4 gating (`activate_image_agent`, which used to
        // recompute this itself) and the E2b residual-image pass below, so the
        // two can never disagree about whether this backend is safe to receive
        // raw images. Must be computed unconditionally (not just when G4's own
        // earlier gates pass) because E2b runs regardless of whether the G4
        // agent activated.
        let native_vision = self
            .backend_is_native_vision(&request.model, &resolved_model, request_genuine)
            .await;

        // G4 image-agent strip/cache seam. This runs AFTER model/profile
        // resolution + system-prompt prefix but BEFORE replay lookup/lowering so
        // that (a) gating sees the resolved/profiled backend, not the raw request
        // model, and (b) replay hashes and the lowered upstream payload only ever
        // see `[Image #N]` placeholder TEXT, never image bytes. When gating
        // activates, `strip_and_cache_images` mutates `request` in place
        // (images → placeholders, inject one `analyzeImage` tool + system
        // instruction, dedup) and returns a per-turn session id; we then lower
        // with the image agent active so `analyzeImage` classifies as the
        // server-side tool and thread the session id to the executor.
        let vision_session = self.activate_image_agent(&mut request, native_vision).await;

        // D3: a pre-spawn early return (replay baseline lookup, lowering/validation,
        // or budgeting below) must finalize the record `Failed` with the correct
        // reason — NOT fall through to the `Drop` fallback's `Cancelled`. Helper to
        // finalize-then-return on the `?` paths without duplicating the guard plumbing.
        let finalize_pre_spawn_err = |err: AppError| -> AppError {
            if let Some(guard) = &telemetry_guard {
                guard.finalize(
                    crate::dashboard_flow::FlowStatus::Failed,
                    Some(err.to_string()),
                );
                // D5: record the terminal into the metrics rings at the SAME seam as
                // the finalize (co-located so a pre-spawn failure is counted exactly
                // once, with the guard's monotonic latency, from the guard's own
                // evict-safe inputs). No-op when metrics off. Runs AFTER the finalize
                // above, which assembled those inputs.
                self.record_terminal_metrics(
                    guard,
                    crate::dashboard_flow::FlowStatus::Failed,
                    guard.elapsed().as_millis(),
                );
            }
            // F1c: a pre-spawn failure (bad request / lowering / budget) is always
            // `failed`. Record it on the capture guard so the artifact carries the
            // terminal reason; the served side is the error body the handler returns
            // (teed by the HTTP layer), so the both-`done` barrier still resolves and
            // writes a `status:"failed"` artifact — never a hang (AC-7). Idempotent.
            if let Some(guard) = &capture_guard {
                guard.finalize("failed", Some(&err.to_string()));
            }
            err
        };

        // E2b residual-image safety pass: the TRUE choke point for "no raw
        // image reaches a non-native-vision backend". `activate_image_agent`
        // above only ever strips `role=="user"` + `image_url` in an ACTIVE
        // turn (`vision/strip.rs:strip_and_cache_images`); this sweep is
        // role-agnostic and runs regardless of whether that agent activated,
        // so it also catches `file_id` images, images in a non-`user` message,
        // `tool_choice=="none"` residuals, and old-history images the active
        // strip never sees. It is a no-op — never even inspected — on
        // native-vision passthrough, and never double-transforms what the
        // active strip already rewrote to `InputText` (there is nothing left
        // of type `InputImage` for it to find there).
        if !native_vision {
            if self.config.unsupported_image_policy == UnsupportedImagePolicy::Reject
                && crate::vision::has_residual_images(&request.input)
            {
                // Reject BEFORE dispatch: a bad-request 4xx via `AppError::
                // bad_request` (400), never `AppError::upstream` (502) — the
                // provider is not contacted at all, so it is never cooled.
                return Err(finalize_pre_spawn_err(AppError::bad_request(
                    "upstream model is text-only; images are not supported",
                )));
            }
            let degraded_images = crate::vision::degrade_residual_images(&mut request.input);
            if degraded_images > 0 {
                // MVP observability (AC-6): a WARN log plus a monitor phase via
                // `emit_with` (no-op under `MonitorHub::disabled()`). The
                // response header/dashboard flag are a documented follow-up —
                // the engine returns a bare `ReceiverStream`, so surfacing this
                // there needs a metadata wrapper, out of scope here.
                tracing::warn!(
                    count = degraded_images,
                    model = %resolved_model,
                    "residual image(s) degraded to text placeholders for a non-vision backend"
                );
                self.monitor
                    .emit_with(response_id.as_str(), || MonitorEventKind::ToolPhase {
                        phase: "residual_image_degraded".to_string(),
                        detail: format!(
                            "{degraded_images} image(s) replaced with a text placeholder"
                        ),
                    });
                // Replay-safety (AC-5): bypass the cache entirely for this
                // degraded turn, both the lookup just below AND the store
                // insert in `run_turn` — reusing the SAME `request.store` flag
                // both sides already gate on, rather than adding new plumbing.
                // Required because `hash_visible_history` (`replay.rs`) hashes
                // POST-transform items: two DIFFERENT images collapsing to
                // byte-identical placeholder text at the same position would
                // otherwise collide and serve the wrong cached response.
                request.store = false;
            }
        }

        let (baseline_record, prefix_len) = self
            .find_replay_baseline(&request)
            .await
            .map_err(&finalize_pre_spawn_err)?;
        let mut tail_request = request.clone();
        tail_request.input = request.input[prefix_len..].to_vec();
        if self.config.brave_api_key.is_none() {
            let original_tool_count = tail_request.tools.len();
            tail_request
                .tools
                .retain(|t| !matches!(t, crate::models::responses::ToolSpec::WebSearch { .. }));
            if tail_request.tools.len() != original_tool_count {
                relax_tool_choice_after_stripping_tool(
                    &mut tail_request.tool_choice,
                    "web_search",
                    tail_request.tools.is_empty(),
                );
                relax_tool_choice_after_stripping_tool(
                    &mut request.tool_choice,
                    "web_search",
                    tail_request.tools.is_empty(),
                );
            }
        }
        // D2 (D13 R1 HIGH): capture the NORMALIZED canonical body — the
        // `ResponsesRequest` the engine operates on AFTER the inbound→canonical
        // adapter, system-prompt prefixing, image-agent strip, and tool stripping,
        // i.e. the settled internal Responses protocol, just BEFORE lowering to the
        // upstream chat payload. This feeds the 3-pane inspector's MIDDLE pane
        // (inbound → NORMALIZED → upstream). Keyed by `api_call_id` (the store key),
        // captured via the O(CAP) redacting mint so a multi-MiB prompt is never
        // serialized in full. No-op when the store is disabled or no `api_call_id`
        // was threaded (the public wrapper / non-instrumented paths). Done after the
        // pre-spawn `?` paths above could have early-returned, but BEFORE lowering so
        // it reflects exactly what the engine hands to `lower_request_with_image_agent`.
        if self.flow_store().is_enabled()
            && let Some(api_call_id) = api_call_id.as_deref()
        {
            let normalized = crate::dashboard_flow::capture_body_from_value(&request);
            self.flow_store()
                .set_normalized(api_call_id, Some(model_requested), Some(normalized));
        }
        // Lower the canonical request to the upstream chat payload BEFORE
        // budgeting. The `?` surfaces any lowering/validation error (invalid
        // tool_choice, unsupported `previous_response_id`, duplicate tools, …)
        // exactly as before, so the client sees the same canonical error;
        // budgeting only runs on a successful lowering (never a new error path).
        // `lower_request` is a pure transform and `find_replay_baseline` above is
        // a side-effect-free read, so computing them here is safe. Pass the image
        // agent flag so an injected/caller `analyzeImage` tool lowers as the
        // server-side ImageAnalysis kind (run by the gateway) on active turns.
        let roles = self
            .config
            .resolve_roles_config_for_resolved_model(&request.model, &resolved_model);
        let lowered = lower_request_with_image_agent_and_roles(
            &tail_request,
            baseline_record
                .as_ref()
                .map(|record| record.internal_messages.clone())
                .unwrap_or_default(),
            vision_session.is_some(),
            roles,
        )
        .map_err(&finalize_pre_spawn_err)?;

        // G3 pre-flight context budgeting (T9: candidate-set seam). Estimate
        // over the LOWERED upstream payload (`lowered.messages`/`tools`/scalars)
        // so no canonical field can inflate the estimate, then budget against
        // the CONSERVATIVE MIN of the per-candidate context windows the
        // routing/failover layer reports for the resolved model's pre-first-chunk
        // candidate set (the primary + its failover chain + the routing target).
        // The MIN is the strictest window across the chain, so a failover to a
        // smaller-window model cannot overflow; candidates with no reported
        // window (`None`) are skipped (unknown ⇒ no-op, matching pre-T9). If NO
        // candidate reports a window (non-routing single upstream / all-unknown
        // / empty set), fall back to the engine's non-routing catalog limit for
        // `resolved_model` (the single provider's window — the served model's
        // limit, correct in non-routing mode). If that is also unknown,
        // budgeting no-ops. Cap an explicitly requested `max_output_tokens` down
        // to what still fits after the estimated input + fixed margin. If the
        // byte heuristic says no budget remains, defer instead of rejecting:
        // only the provider tokenizer can decide true overflow. We mutate ONLY
        // the typed field, which flows through to the chat request build and
        // wins over conflicting default max-token aliases.
        let candidate_plan = self.upstream.backend_candidate_plan(&resolved_model).await;
        // T9: the candidate plan is the authoritative resolver in routing/
        // failover mode. The engine's own `/v1/models` catalog is a budgeting
        // fallback ONLY in plain single-provider mode (where it IS the single
        // served provider); in any other mode an all-unknown candidate plan
        // must NO-OP rather than budget against the engine union, which could
        // mask a failover target's smaller window or budget a routed model
        // against the wrong window.
        let mut limit = candidate_context_floor(&candidate_plan);
        if limit.is_none() && self.config.is_plain_single_provider() {
            limit = self.upstream_model_context_limit(&resolved_model).await;
        }
        // C3: compute the estimate UNCONDITIONALLY now, not only when a context
        // `limit` is known -- it also rides the `response.created` SSE event
        // (`run_turn` below) so the Anthropic streaming converter can seed
        // `message_start.usage.input_tokens` with a plausible non-zero value
        // instead of a hardcoded `0` (the real upstream tokenizer count isn't
        // known this early).
        //
        // CR1.2 (reviewed, accepted as-is): this is NOT free -- it's a
        // `messages.to_vec()` + `tools.to_vec()` clone of the lowered payload
        // (incl. any multimodal content) plus a `serde_json` serialize, and it
        // now runs even for requests where `limit` is `None` (routing/
        // multi-provider configs, or a single provider whose context window
        // could not be resolved) and whose egress never reads the field
        // (Chat/Responses egress convert/strip it away -- see
        // `http.rs::responses_wire_event_data`, CR1.1). We cannot skip the
        // compute for that combination: `stream_responses_with_api_call_id` is
        // the single funnel for all three egress surfaces (Anthropic/Chat/
        // Responses) and has no visibility into which one the caller
        // (`http.rs`, one layer up) will use to drain the returned stream --
        // that would require threading an egress hint down through this fn,
        // `run_turn`, and `created_event`, which is a bigger, riskier change
        // than a LOW-priority finding justifies.
        // The common case (plain single-provider, `limit` resolved) already
        // paid this cost pre-C3 for budgeting, so the incremental cost is
        // bounded to the `limit.is_none()` minority, once per top-level turn
        // (not per streamed chunk) -- accepted rather than churned for a LOW
        // finding.
        let estimated_input_tokens =
            estimate_input_tokens(&lowered, self.config.flatten_content, &resolved_model);
        if let Some(limit) = limit {
            match budget_explicit_max_output_tokens(
                request.max_output_tokens,
                limit,
                estimated_input_tokens,
            ) {
                Ok(capped) => request.max_output_tokens = capped,
                Err(ContextBudgetError) => {
                    // The local estimator is intentionally approximate. Never
                    // reject a request solely from the byte/character heuristic:
                    // let the provider's tokenizer decide, then use the exact
                    // context-overflow shrink-and-retry path if necessary.
                    tracing::debug!(
                        model = %resolved_model,
                        estimated_input_tokens,
                        context_limit = limit,
                        "estimated prompt exceeds context window; deferring to upstream tokenizer"
                    );
                }
            }
        }

        // Gap 02: the routing/lowering decision is now SETTLED — the served model is
        // resolved, the candidate plan was fetched, and the canonical request lowered
        // to the upstream chat payload without error (every pre-spawn `?` above passed).
        // Stamp the `routing_decision` phase HERE, at the engine seam, so it fires for
        // every upstream client (mock or real) the instant the engine commits to a
        // backend — distinct from the leaf's later on-the-wire body capture. First
        // write-wins keeps the FIRST decision on a multi-turn flow. Gated on
        // `api_call_id` so the production hot path skips the call.
        if let Some(api_call_id) = api_call_id.as_deref() {
            self.flow_store().stamp_routing_decision(api_call_id);
        }
        // Chat-message length of the replayed prefix (the baseline handed to
        // lowering above). Threaded into `run_turn` so its pre-send adjacency
        // merge is tail-scoped and never rewrites the cache-stable prefix.
        let replay_prefix_len = baseline_record
            .as_ref()
            .map(|record| record.internal_messages.len())
            .unwrap_or(0);
        let (tx, rx) = mpsc::channel(128);
        let gateway = Arc::clone(&self);
        tokio::spawn(async move {
            let result = gateway
                .run_turn(
                    response_id.clone(),
                    request,
                    lowered.messages,
                    replay_prefix_len,
                    lowered.tools,
                    lowered.tool_registry,
                    lowered.response_format,
                    lowered.reasoning_effort,
                    // C3: the early G3 estimate, threaded through so `run_turn` can
                    // stamp it onto `response.created` (see `created_event` below).
                    estimated_input_tokens,
                    resolved_model,
                    vision_session,
                    // D1 (R1 #9): the engine binds `response_id → api_call_id` at
                    // the RequestStarted emission seam inside `run_turn`, not here.
                    api_call_id,
                    // D2/D3: the shared serving token (tagged by routing/failover,
                    // read by the guard at finalize) threaded onto every per-turn
                    // `BackendChatRequest`.
                    serving_token,
                    tx.clone(),
                    // D6: the flow's kill token, composed with every `tx.closed()`
                    // client-hangup check inside `run_turn` + its helpers.
                    abort_token,
                )
                .await;
            // D3 L1: finalize the flow record at THIS single choke point (the spawned
            // body) from the typed `result`, then let the guard drop. `is_cancelled()`
            // (HTTP 499 — client hung up) ⇒ `Cancelled`; any other error ⇒ `Failed`;
            // `Ok` ⇒ `Completed`. The guard's own `Drop` (below, when this closure
            // returns) is the LAST-resort fallback for a path that never reached here
            // — a panic INSIDE `run_turn` unwinds through this closure and drops the
            // guard, finalizing `Cancelled`. The CAS makes the explicit finalize win
            // and the drop then no-op (idempotent).
            // Resolve the terminal status ONCE so the same value drives the FlowStore
            // finalize, the D5 metrics record, AND the F1c capture terminal (all
            // co-located at this single choke point → recorded exactly once).
            // `is_cancelled()` (499 — client hung up) ⇒ Cancelled; any other error ⇒
            // Failed; `Ok` ⇒ Completed.
            let (status, reason) = match &result {
                // Both a genuine stop and a `length` truncation are `Ok` and count as
                // `Completed` for the dashboard/metrics (a successful HTTP serve); the
                // capture artifact splits them below (finding #3).
                Ok(_) => (
                    crate::dashboard_flow::FlowStatus::Completed,
                    "response.completed".to_string(),
                ),
                Err(err) if err.is_cancelled() => (
                    crate::dashboard_flow::FlowStatus::Cancelled,
                    "client_disconnected".to_string(),
                ),
                Err(err) => (crate::dashboard_flow::FlowStatus::Failed, err.to_string()),
            };
            if let Some(guard) = &telemetry_guard {
                guard.finalize(status, Some(reason.clone()));
                // D5: record the terminal into the metrics rings (sources served
                // model + endpoint + upstream + final usage from the guard's own
                // evict-safe inputs — no `detail()` re-read — + the guard's monotonic
                // latency). No-op when the metrics layer is off. Runs AFTER
                // `guard.finalize`, which assembled those inputs.
                gateway.record_terminal_metrics(guard, status, guard.elapsed().as_millis());
            }
            // F1c: report the SAME engine terminal to the capture guard (status +
            // reason come from the engine seam ONLY, never the served tee). Idempotent
            // first-writer-wins; the both-`done` barrier assembles + evicts once the
            // served tee's `served_done` has also fired.
            // Finding #3 (don't-lie-with-zeros): a `length`-truncated turn is `Ok` but
            // its response was cut short, so the ARTIFACT status is `incomplete`
            // (`response.incomplete`) even though the dashboard `status` above stays
            // `Completed`. Genuine stop / failed / cancelled map from the `FlowStatus`.
            if let Some(guard) = &capture_guard {
                let (capture_status, capture_reason) = match &result {
                    Ok(TurnCompletion::Incomplete) => ("incomplete", "response.incomplete"),
                    _ => (flow_status_artifact_str(status), reason.as_str()),
                };
                guard.finalize(capture_status, Some(capture_reason));
            }
            if let Err(err) = &result {
                if tx.is_closed() {
                    gateway
                        .monitor
                        .emit_with(response_id.as_str(), || MonitorEventKind::Failed {
                            message: "client disconnected".to_string(),
                        });
                    return;
                }
                gateway
                    .monitor
                    .emit_with(response_id.as_str(), || MonitorEventKind::Failed {
                        message: err.to_string(),
                    });
                // The terminal `response.failed` is the client's ONLY signal that the
                // turn failed/was killed, so it must DELIVER, not cancel: pass a fresh
                // never-cancelled token. (The flow's own token is already flipped on a
                // kill — composing it here would suppress the terminal event.) Teardown
                // is already done by this point — `run_turn` returned (upstream dropped)
                // and `guard.finalize` above removed the AbortHub entry — so this last
                // best-effort send blocks on nothing but client backpressure, and the
                // `tx.is_closed()` early-return above already covers a hung-up client.
                let _ = gateway
                    .send_event(
                        &tx,
                        failure_event(err),
                        &tokio_util::sync::CancellationToken::new(),
                    )
                    .await;
            }
        });
        Ok(ReceiverStream::new(rx))
    }

    pub(crate) fn apply_system_prompt_prefix(
        &self,
        mut request: ResponsesRequest,
        resolved_model: &str,
    ) -> ResponsesRequest {
        let Some(prefix) = self
            .config
            .resolve_system_prompt_prefix_for_resolved_model(&request.model, resolved_model)
        else {
            return request;
        };
        request.instructions = if request.instructions.is_empty() {
            prefix
        } else {
            format!("{prefix}\n\n{}", request.instructions)
        };
        request
    }

    /// G4 gating + strip. Decide whether the image agent runs for this turn and,
    /// if so, strip images to placeholders, cache them, inject the
    /// `analyzeImage` tool + system instruction, and return the per-turn cache
    /// session id. Returns `None` (no mutation) when ANY gate fails.
    ///
    /// All gates (claude-relay + the canonical-Responses adaptation):
    /// - `image_agent_enabled` is true and a `vision_url` is configured (no
    ///   endpoint ⇒ nothing to offload to),
    /// - the LATEST user message carries ≥1 image (old images in history must
    ///   not re-trigger the agent),
    /// - `native_vision` is `false` — the resolved/profiled backend is NOT
    ///   native-vision (Kimi by name, or a profile `native_vision` override).
    ///   Precomputed by the caller (`backend_is_native_vision`, see its
    ///   decision table) and shared with the E2b residual-image pass that runs
    ///   right after this returns, so the two never disagree,
    /// - `tool_choice` is not `"none"` (the caller forbade tools, so injecting a
    ///   mandatory tool would be a contradiction).
    async fn activate_image_agent(
        &self,
        request: &mut ResponsesRequest,
        native_vision: bool,
    ) -> Option<String> {
        if !self.config.image_agent_enabled || self.config.vision_url.is_none() {
            return None;
        }
        if request.tool_choice == Value::String("none".to_string()) {
            return None;
        }
        if !crate::vision::latest_user_message_has_images(&request.input) {
            return None;
        }
        if native_vision {
            return None;
        }
        // A per-turn session id keys the shared cache. It need only be unique for
        // the lifetime of this turn: `strip_and_cache_images` clears+repopulates
        // this session, the executor reads it, and a later turn gets a fresh id —
        // so multi-turn placeholder numbering resets exactly like claude-relay.
        let session_id = format!("vis_{}", Uuid::new_v4().simple());
        self.image_cache
            .strip_and_cache_images(request, &session_id);
        Some(session_id)
    }

    /// Native-vision gating decision (G4). Decides whether to pass raw images
    /// through (return `true` → skip strip/offload) or strip+offload (`false`).
    ///
    /// DECISION TABLE (the single source of truth — the code below matches it
    /// exactly; do not special-case index 0):
    ///
    /// ```text
    /// (1) Candidate set = every pre-first-chunk serving backend (selected
    ///     primary + its failover chain + routing target), enumerated from
    ///     `resolved_model` via `backend_candidate_plan`.
    ///     EMPTY/unknown  =>  STRIP (return false) — never fall back to a
    ///     name-looks-native model.
    ///
    /// (2) For EACH candidate c (c is ALREADY the final backend model the
    ///     provider receives — lookups are PROFILE-ONLY, NO further upstream_model
    ///     remap, round-9 #1), native(c) =
    ///     (2a) IF request_model GENUINELY resolves/maps to c (exact id / route /
    ///          unique canonical key — NOT the blank/unmatched/ambiguous default
    ///          fallback; `request_genuine`, a byproduct of the ONE
    ///          `normalize_upstream_model` walk threaded from `stream_responses`
    ///          since T2) AND the LITERAL request model's profile sets
    ///          `native_vision` => that value
    ///     (2b) ELSE c's OWN profile `native_vision` (keyed on c exactly) if set
    ///     (2c) ELSE name-based native detection (Kimi etc.)
    ///
    /// (3) PASSTHROUGH iff the candidate set is non-empty AND native(c)==true for
    ///     ALL c. Otherwise STRIP. So native_vision:false anywhere it legitimately
    ///     applies => STRIP; any non-native/unknown candidate => STRIP.
    /// ```
    ///
    /// The request override attaches to the candidate it GENUINELY maps to (the
    /// selected primary, when the request truly resolves there), never blindly to
    /// index 0 — a stale alias normalized to a different default backend must NOT
    /// borrow the request's `native_vision` (round-8 #1). Fallback candidates are
    /// always per-candidate, so the override never leaks onto a non-native
    /// fallback (round-2/3). All native_vision lookups are profile-only on the
    /// exact model, so a candidate's (or the request's) `upstream_model` remap
    /// cannot make the gate judge a different model than runs (round-9 #1).
    async fn backend_is_native_vision(
        &self,
        request_model: &str,
        resolved_model: &str,
        request_genuine: bool,
    ) -> bool {
        // T2: the routing/failover layer owns the candidate set (typed
        // `BackendCandidatePlan`), and `request_genuine` — a byproduct of the
        // ONE `normalize_upstream_model` walk, threaded from `stream_responses`
        // — owns the genuine-vs-default signal. The gate no longer re-derives
        // the resolution ladder in the engine.
        let candidates = self
            .upstream
            .backend_candidate_plan(resolved_model)
            .await
            .candidates;
        if candidates.is_empty() {
            // Cell 1: unknown candidate set ⇒ strip (works for every backend).
            return false;
        }
        // The request override (cell 2a) may attach to the SELECTED primary
        // candidate (index 0 — the model the resolved request lands on) only when
        // the request model GENUINELY resolves there. On a default-fallback
        // (`request_genuine == false`) it does not map to that candidate, so the
        // override is dropped entirely and every candidate uses per-candidate
        // detection. This is a PROFILE-ONLY lookup on the LITERAL request model
        // (round-9 #1): no `upstream_model` remap, so the remap TARGET's profile
        // cannot displace the request's.
        let request_override = if request_genuine {
            self.config.profile_native_vision(request_model)
        } else {
            None
        };
        candidates.iter().enumerate().all(|(index, candidate)| {
            // Cell 2a: request override applies ONLY to the genuinely-mapped
            // primary candidate; cells 2b/2c for everything else.
            if index == 0
                && let Some(native) = request_override
            {
                return native;
            }
            self.candidate_is_native_vision(&candidate.model)
        })
    }

    /// Per-candidate native-vision (decision-table cells 2b/2c). `candidate_model`
    /// is ALREADY the final backend model the provider will receive, so this is a
    /// PROFILE-ONLY lookup on that exact model (round-9 #1): its own profile
    /// `native_vision` with NO further `upstream_model` remap (re-remapping would
    /// judge the remap target, a DIFFERENT model than the provider gets), else the
    /// name sniff (Kimi). The request model's profile is NOT consulted (round-3
    /// #2). Unknown ⇒ not native.
    fn candidate_is_native_vision(&self, candidate_model: &str) -> bool {
        if let Some(native) = self.config.profile_native_vision(candidate_model) {
            return native;
        }
        candidate_model.to_ascii_lowercase().contains("kimi")
    }

    async fn find_replay_baseline(
        &self,
        request: &ResponsesRequest,
    ) -> AppResult<(Option<ReplayRecord>, usize)> {
        if !request.store {
            return Ok((None, 0));
        }
        let record = self
            .replay_store
            .longest_prefix_match(&request.model, &request.instructions, &request.input)
            .await;
        if let Some(record) = record {
            let prefix_len = record.visible_history.len();
            return Ok((Some(record), prefix_len));
        }
        Ok((None, 0))
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_turn(
        &self,
        response_id: String,
        request: ResponsesRequest,
        mut current_messages: Vec<ChatMessage>,
        // Length of the replayed prefix carried over from a prior turn. Role
        // shaping + adjacency merges apply ONLY to the tail
        // `current_messages[replay_prefix_len..]`, never the replayed prefix, so
        // the vLLM prefix cache stays byte-identical (mirrors the tail split in
        // `lower_request_with_image_agent_and_roles`).
        replay_prefix_len: usize,
        tools: Vec<crate::models::chat::ChatTool>,
        tool_registry: crate::adapters::responses_to_chat::ToolRegistry,
        response_format: Option<Value>,
        reasoning_effort: Option<String>,
        // C3: the pre-spawn G3 estimate (`estimate_input_tokens`) for THIS turn's
        // lowered payload, computed once by the caller (`stream_responses_with_api_call_id`).
        // Stamped onto `response.created` (`created_event`, below) so the Anthropic
        // streaming converter can seed a non-zero `message_start.usage.input_tokens`
        // instead of a hardcoded `0` — see that fn's doc comment for why the REAL
        // upstream count can't be used this early.
        estimated_input_tokens: i64,
        upstream_model: String,
        // G4: `Some(session_id)` when the image agent is active for this turn —
        // the key into `self.image_cache` the `analyzeImage` executor resolves
        // images against, and the signal to suppress `analyzeImage` streamed
        // deltas from the client.
        vision_session: Option<String>,
        // D1 (R1 #9): the inbound `api_call_id` for this flow (when the request was
        // captured by the dashboard FlowStore), so `link(response_id, api_call_id)`
        // fires at the RequestStarted seam below.
        api_call_id: Option<String>,
        // D2/D3: the flow's shared serving token (allocated once in
        // `stream_responses_with_api_call_id`, tagged by routing/failover) threaded
        // onto every per-turn `BackendChatRequest`. The D3 L1 telemetry guard stays
        // in the spawn closure (the single finalize choke point) rather than here, so
        // the terminal status is classified from `run_turn`'s typed `Result`
        // (cancelled vs failed vs completed) and the guard's `Drop` still covers a
        // panic inside this function (it unwinds through the closure).
        serving_token: Arc<crate::upstream::ServingToken>,
        tx: mpsc::Sender<SseEvent>,
        // D6: the flow's cancellation token (registered in the AbortHub by the L1 guard
        // under `api_call_id`). COMPOSED with — never a replacement for — every existing
        // `tx.closed()` client-hangup check: a kill flips this token and each cancel site
        // surfaces `AppError::cancelled()` (499) just like a hang-up, with no token
        // duplication. A fresh never-cancelled token off the dashboard path.
        abort_token: tokio_util::sync::CancellationToken,
    ) -> AppResult<TurnCompletion> {
        // D5 R3 (MEDIUM): record the resolved served model onto the shared serving
        // token so the L1 telemetry guard (which holds the token) can attribute the
        // metrics bucket's model at finalize WITHOUT re-reading the FlowStore record —
        // which may be pruned/evicted by the time a long-running flow finalizes. The
        // route/provider + final usage are also carried on the token (set by the
        // failover/routing layers + the usage upsert below), making the guard's
        // terminal metrics fully independent of FlowStore retention.
        //
        // D5 R4 (MEDIUM): this `upstream_model` is the engine's PRE-routing model; the
        // leaf rewrites `request.model` on failover/routing and then finalizes the ACTUAL
        // on-wire model onto the same token (`set_model_served_final`, which overwrites
        // this guess). So this write is the FALLBACK for the no-leaf / error-before-
        // dispatch path; the leaf's value wins when a flow reaches the wire.
        serving_token.set_model_served(upstream_model.clone());
        // D1 (R1 #9): bind this flow's `response_id` to its inbound `api_call_id`
        // exactly ONCE, at the RequestStarted emission seam (not pre-spawn). No-op
        // when the FlowStore is disabled or no `api_call_id` was threaded (the
        // public `stream_responses` wrapper). `response_id` stays the `resp_{uuid}`
        // API contract — never collapsed to the `api_call_id`.
        if let Some(api_call_id) = &api_call_id {
            self.flow_store()
                .link(response_id.clone(), api_call_id.clone());
        }
        self.monitor.emit_with(response_id.as_str(), || {
            MonitorEventKind::RequestStarted {
                model: request.model.clone(),
                input_items: request.input.len(),
                tool_count: request.tools.len(),
                turn_count: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "user"
                        )
                    })
                    .count(),
                user_messages: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "user"
                        )
                    })
                    .count(),
                assistant_messages: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "assistant"
                        )
                    })
                    .count(),
                system_messages: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "system"
                        )
                    })
                    .count(),
                developer_messages: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::Message {
                                role,
                                ..
                            } if role == "developer"
                        )
                    })
                    .count(),
                reasoning_items: request
                    .input
                    .iter()
                    .filter(|item| matches!(item, ResponseItem::Reasoning { .. }))
                    .count(),
                function_calls: request
                    .input
                    .iter()
                    .filter(|item| matches!(item, ResponseItem::FunctionCall { .. }))
                    .count(),
                function_outputs: request
                    .input
                    .iter()
                    .filter(|item| matches!(item, ResponseItem::FunctionCallOutput { .. }))
                    .count(),
                tool_items: request
                    .input
                    .iter()
                    .filter(|item| {
                        matches!(
                            item,
                            ResponseItem::FunctionCall { .. }
                                | ResponseItem::FunctionCallOutput { .. }
                                | ResponseItem::CustomToolCall { .. }
                                | ResponseItem::CustomToolCallOutput { .. }
                                | ResponseItem::ToolSearchCall { .. }
                                | ResponseItem::ToolSearchOutput { .. }
                                | ResponseItem::LocalShellCall { .. }
                                | ResponseItem::WebSearchCall { .. }
                                | ResponseItem::ImageGenerationCall { .. }
                        )
                    })
                    .count(),
                input_chars: request
                    .input
                    .iter()
                    .map(|item| match item {
                        ResponseItem::Message { content, .. } => content
                            .iter()
                            .map(|content| match content {
                                crate::models::responses::ContentItem::InputText { text }
                                | crate::models::responses::ContentItem::OutputText { text } => {
                                    text.chars().count()
                                }
                                crate::models::responses::ContentItem::InputImage {
                                    image_url,
                                    file_id,
                                    detail,
                                } => image_url
                                    .iter()
                                    .chain(file_id.iter())
                                    .chain(detail.iter())
                                    .map(|value| value.chars().count())
                                    .sum(),
                                crate::models::responses::ContentItem::InputFile {
                                    file_id,
                                    file_url,
                                    filename,
                                    file_data,
                                } => file_id
                                    .iter()
                                    .chain(file_url.iter())
                                    .chain(filename.iter())
                                    .chain(file_data.iter())
                                    .map(|value| value.chars().count())
                                    .sum(),
                                crate::models::responses::ContentItem::Other(value) => {
                                    value.to_string().chars().count()
                                }
                            })
                            .sum::<usize>(),
                        ResponseItem::Reasoning { content, .. } => content
                            .as_ref()
                            .map(|items| {
                                items.iter()
                                    .map(|item| match item {
                                        crate::models::responses::ReasoningContentItem::ReasoningText {
                                            text,
                                        }
                                        | crate::models::responses::ReasoningContentItem::Text {
                                            text,
                                        } => text.chars().count(),
                                    })
                                    .sum()
                            })
                            .unwrap_or(0),
                        ResponseItem::FunctionCall {
                            name, arguments, ..
                        } => name.chars().count() + arguments.chars().count(),
                        ResponseItem::FunctionCallOutput { call_id, output } => {
                            call_id.chars().count() + output.to_string().chars().count()
                        }
                        ResponseItem::CustomToolCall { name, input, .. } => {
                            name.chars().count() + input.chars().count()
                        }
                        ResponseItem::CustomToolCallOutput {
                            call_id,
                            name,
                            output,
                        } => {
                            call_id.chars().count()
                                + name.as_ref().map(|name| name.chars().count()).unwrap_or(0)
                                + output.to_string().chars().count()
                        }
                        ResponseItem::ToolSearchCall { arguments, .. } => {
                            arguments.to_string().chars().count()
                        }
                        ResponseItem::ToolSearchOutput { tools, .. } => tools
                            .iter()
                            .map(|tool| tool.to_string().chars().count())
                            .sum(),
                        ResponseItem::LocalShellCall { action, .. } => match action {
                            crate::models::responses::LocalShellAction::Exec(exec) => exec
                                .command
                                .iter()
                                .map(|part| part.chars().count())
                                .sum(),
                        },
                        ResponseItem::WebSearchCall { action, .. } => action
                            .as_ref()
                            .map(|action| match action {
                                crate::models::responses::WebSearchAction::Search {
                                    query,
                                    queries,
                                } => {
                                    query.as_ref().map(|q| q.chars().count()).unwrap_or(0)
                                        + queries
                                            .as_ref()
                                            .map(|queries| {
                                                queries
                                                    .iter()
                                                    .map(|query| query.chars().count())
                                                    .sum()
                                            })
                                            .unwrap_or(0)
                                }
                                crate::models::responses::WebSearchAction::OpenPage {
                                    url,
                                } => url.as_ref().map(|url| url.chars().count()).unwrap_or(0),
                                crate::models::responses::WebSearchAction::FindInPage {
                                    url,
                                    pattern,
                                } => {
                                    url.as_ref().map(|url| url.chars().count()).unwrap_or(0)
                                        + pattern
                                            .as_ref()
                                            .map(|pattern| pattern.chars().count())
                                            .unwrap_or(0)
                                }
                                crate::models::responses::WebSearchAction::Other => 0,
                            })
                            .unwrap_or(0),
                        ResponseItem::ImageGenerationCall {
                            revised_prompt,
                            result,
                            ..
                        } => {
                            revised_prompt
                                .as_ref()
                                .map(|text| text.chars().count())
                                .unwrap_or(0)
                                + result.chars().count()
                        }
                    })
                    .sum(),
                instructions_chars: request.instructions.chars().count(),
            }
        });
        self.monitor.emit_with(response_id.as_str(), || {
            let request_preview = preview_json_limited_with_images(&request, 128 * 1024);
            MonitorEventKind::RequestPayload {
                payload_preview: request_preview.text,
                images: request_preview.images,
            }
        });
        // `trailing_tool_output_items` reverse-walks the request tail and
        // allocates a `Vec`, so gate the whole loop on `is_enabled()` to keep the
        // disabled (`MonitorHub::disabled()`) path zero-overhead — the inner
        // `emit_with` closures defer `summarize_response_item` past the same check.
        if self.monitor.is_enabled() {
            for item in trailing_tool_output_items(&request.input) {
                self.monitor
                    .emit_with(response_id.as_str(), || MonitorEventKind::ToolPhase {
                        phase: "client_tool_result".to_string(),
                        detail: summarize_response_item(item),
                    });
            }
        }
        self.send_event(
            &tx,
            created_event(&response_id, estimated_input_tokens),
            &abort_token,
        )
        .await?;
        self.send_event(&tx, in_progress_event(&response_id), &abort_token)
            .await?;

        let mut public_history = request.input.clone();
        let mut response_output = Vec::new();
        let mut event_state = ResponseEventState::default();

        let mut accumulated_usage = AccumulatedUsage::default();
        let mut upstream_request_index = 0usize;
        let mut web_search_rounds = 0usize;
        // G4: independent round counter for `analyzeImage` server-tool loops, so
        // a model that keeps calling the vision tool cannot hang the turn. This
        // is SEPARATE from `web_search_rounds` and the web-search hard ceiling
        // (AGENTS.md: do not change `WEB_SEARCH_ROUNDS_HARD_CEILING`).
        let mut image_analysis_rounds = 0usize;
        // E1: independent counter for in-gateway repair rounds triggered by a
        // hallucinated (unoffered) tool call, bounded by
        // `UNKNOWN_TOOL_REPAIR_CEILING`. SEPARATE from the web-search / image
        // ceilings (a model can mix all three). `pending_repair` is set after a
        // repair round is injected so a SUBSEQUENT clean round counts as a
        // `Repaired` outcome exactly once.
        let mut unknown_tool_repair_rounds = 0usize;
        let mut pending_repair = false;
        // A forced `tool_choice` (e.g. an Anthropic `web_search` server tool,
        // which Claude Code always forces) must apply only to the first
        // upstream request. After a provider-side web search runs and its
        // results are injected, the model has to be free to answer in prose.
        // Re-sending the forced tool_choice makes vLLM/Kimi emit the final
        // answer text into `function.arguments`, which then fails to parse.
        let mut current_tool_choice = request.tool_choice.clone();
        #[allow(unused_assignments)]
        let mut last_finish_reason: Option<String> = None;
        let mut last_stop_sequence: Option<String>;
        // T1: `template_family` + `upstream_chat_kwargs` profile resolution moved
        // to the upstream LEAF (`finalize_request_for_backend`), where the FINAL
        // per-provider model is known after routing/failover/exposed-alias remap.
        // The engine no longer pre-resolves these against the pre-routing
        // `upstream_model`, so a routed/failover cross-family target gets its OWN
        // family/kwargs rather than the alias's. The engine still captures the
        // client's EXPLICIT `chat_template_kwargs` here (PRE-MERGE) and threads it
        // on the `BackendChatRequest` wrapper: the leaf cannot re-derive it from
        // the merged `extra_body`, and re-asserting it (not the provider/global
        // blend) over the forced family default preserves client-wins precedence.
        let client_chat_template_kwargs = request
            .extra_body
            .get("chat_template_kwargs")
            .and_then(Value::as_object)
            .cloned();
        // D2: the FRESH per-flow serving token is now allocated in
        // `stream_responses_with_api_call_id` (so the D3 L1 guard built pre-spawn
        // shares the SAME `Arc`) and threaded in. The routing layer fills `route`,
        // the failover layer fills `provider`; because it is per-flow, concurrent
        // flows never overwrite each other's `{route, provider}` (the rev2
        // cross-flow race). Cloned (the `Arc`, sharing the token) onto every
        // per-turn `BackendChatRequest` below so all upstream turns of THIS flow tag
        // the same token.
        // `build_upstream_extra_body` now runs with EMPTY defaults: the
        // profile/global `upstream_chat_kwargs` merge at the leaf. It still
        // performs the request-extra normalization (remove typed-field defaults
        // shadowed by explicit request fields, deep-merge `request.extra_body`).
        let upstream_extra_body = build_upstream_extra_body(
            serde_json::Map::new(),
            &request,
            &response_format,
            &reasoning_effort,
        );
        // `reasoning_effort` here is the RAW canonical level (lowering no longer
        // clamps it). It flows onto the upstream request as-is; the leaf — the
        // single point that knows the FINAL provider model after routing/failover
        // — either maps it (`reasoning_effort_map`) or clamps it to the backend's
        // vocabulary in `finalize_request_for_backend`.
        let normalized_stop = crate::models::chat::normalize_stop(request.stop.clone())?;
        // F1d: the turn's durable-capture handle (Topic F), looked up ONCE by
        // `api_call_id` from the SAME registry the `CaptureGuard` built above
        // reads — `None` when capture is disabled or this request was never
        // instrumented (the public non-`api_call_id` wrapper). Cloned onto every
        // per-round `BackendChatRequest` below (a turn's tool-call loop can dispatch
        // multiple upstream rounds; the failover/routing rebuilds clone it further
        // — AC-11), so the leaf writes each attempt's on-wire request into the SAME
        // turn's `upstream_request` section (last-writer-wins).
        let capture = api_call_id
            .as_deref()
            .and_then(|id| self.turn_capture().state(id));
        // Gateway-owned server tools run a sequential request/tool/result loop.
        // Preserve the upstream typed/client setting for ordinary client tools,
        // but never allow parallel calls to interleave the internal loop.
        let parallel_tool_calls = if tool_registry.has_active_server_tool(
            self.config.brave_api_key.is_some(),
            vision_session.is_some(),
        ) {
            Some(false)
        } else {
            request.parallel_tool_calls
        };
        // The profile role policy for this resolved model — the SAME config the
        // initial lowering used (`resolve_roles_config_for_resolved_model`,
        // pre-spawn). Drives the pre-send adjacency merge + repair-note shaping
        // below. `None` for a model with no `roles` profile (legacy passthrough).
        let roles = self
            .config
            .resolve_roles_config_for_resolved_model(&request.model, &upstream_model);
        loop {
            // D6: compose the kill token with the client-hangup check — a dashboard
            // `abort()` flips `abort_token`, surfacing `cancelled()` (499) like a hang-up.
            if tx.is_closed() || abort_token.is_cancelled() {
                return Err(AppError::cancelled());
            }
            upstream_request_index += 1;
            // Idempotent, tail-scoped role-adjacency normalization before EVERY
            // upstream send: fold any repair-injected same-role run (e.g. a
            // rewritten `developer` note) into its neighbor. Scoped to the tail
            // (`replay_prefix_len..`) so the replayed prefix stays byte-identical
            // and the vLLM prefix cache is preserved. A no-op on the first round
            // (the lowering already merged the tail); merge is idempotent.
            if roles.is_some() && replay_prefix_len < current_messages.len() {
                let mut tail = current_messages.split_off(replay_prefix_len);
                merge_adjacent_if_configured(&mut tail, roles);
                current_messages.append(&mut tail);
            }
            let taken_messages = std::mem::take(&mut current_messages);
            // T9: the ONE first-upstream-request builder, shared with the G3
            // estimate. The common base is identical; the additives carry the
            // real dispatch values (vs. the estimate's lower-bound-safe
            // empties). `current_tool_choice` mutates across turns (forced
            // `tool_choice` on turn 1 only).
            let upstream_request = build_upstream_chat_request(
                taken_messages,
                (!tools.is_empty()).then_some(tools.clone()),
                response_format.clone(),
                current_tool_choice.clone(),
                UpstreamRequestAdditives {
                    model: upstream_model.clone(),
                    parallel_tool_calls,
                    reasoning_effort: reasoning_effort.clone(),
                    max_output_tokens: request.max_output_tokens,
                    temperature: request.temperature,
                    top_p: request.top_p,
                    frequency_penalty: request.frequency_penalty,
                    presence_penalty: request.presence_penalty,
                    stop: normalized_stop.clone(),
                    extra_body: upstream_extra_body.clone(),
                },
            );
            self.monitor.emit_with(response_id.as_str(), || {
                let upstream_debug_request =
                    sanitize_chat_request(upstream_request.clone(), self.config.flatten_content);
                let upstream_preview =
                    preview_json_limited_with_images(&upstream_debug_request, 128 * 1024);
                MonitorEventKind::UpstreamRequest {
                    request_index: upstream_request_index,
                    message_count: upstream_debug_request.messages.len(),
                    prompt_chars: upstream_debug_request
                        .messages
                        .iter()
                        .map(|message| {
                            message.role.chars().count()
                                + message
                                    .name
                                    .as_ref()
                                    .map(|name| name.chars().count())
                                    .unwrap_or(0)
                                + message
                                    .tool_call_id
                                    .as_ref()
                                    .map(|call_id| call_id.chars().count())
                                    .unwrap_or(0)
                                + message
                                    .reasoning_content
                                    .as_ref()
                                    .map(|text| text.chars().count())
                                    .unwrap_or(0)
                                + message
                                    .content
                                    .as_ref()
                                    .map(|content| content.to_string().chars().count())
                                    .unwrap_or(0)
                                + message
                                    .tool_calls
                                    .as_ref()
                                    .map(|tool_calls| {
                                        tool_calls
                                            .iter()
                                            .map(|tool_call| {
                                                serde_json::to_string(tool_call)
                                                    .unwrap_or_default()
                                                    .chars()
                                                    .count()
                                            })
                                            .sum::<usize>()
                                    })
                                    .unwrap_or(0)
                        })
                        .sum::<usize>()
                        + upstream_debug_request
                            .tools
                            .as_ref()
                            .map(|tools| {
                                tools
                                    .iter()
                                    .map(|tool| {
                                        serde_json::to_string(tool)
                                            .unwrap_or_default()
                                            .chars()
                                            .count()
                                    })
                                    .sum::<usize>()
                            })
                            .unwrap_or(0)
                        + upstream_debug_request
                            .extra_body
                            .values()
                            .map(|value| value.to_string().chars().count())
                            .sum::<usize>(),
                    payload_preview: upstream_preview.text,
                    images: upstream_preview.images,
                }
            });
            // D6: compose kill with hangup before opening the upstream stream.
            if tx.is_closed() || abort_token.is_cancelled() {
                return Err(AppError::cancelled());
            }
            let backend_request = crate::upstream::BackendChatRequest::new(
                upstream_request.clone(),
                client_chat_template_kwargs.clone(),
                // D2: thread the flow's `resp_{uuid}` so the leaf can key its on-wire
                // capture to this flow's FlowStore record, and share the per-flow
                // serving token so routing/failover can tag `{route, provider}`.
                Some(response_id.clone()),
                Some(Arc::clone(&serving_token)),
            )
            .with_thinking_override(request.thinking)
            // F1d: attach the turn-capture handle (see above) so the leaf's
            // `upstream_request` write can reach this turn's artifact.
            .with_capture(capture.clone());
            let mut stream = tokio::select! {
                biased;
                _ = tx.closed() => return Err(AppError::cancelled()),
                // D6: a kill while awaiting the upstream connect cancels it (499), same
                // as a hang-up. `biased` keeps the cancel branches highest-priority.
                _ = abort_token.cancelled() => return Err(AppError::cancelled()),
                result = self.upstream.stream_chat_completion_with_timeout(
                    &backend_request,
                    self.config.request_timeout,
                ) => result?,
            };
            let mut state = StreamState::default();
            let mut turn_usage: Option<ChunkUsage> = None;
            // D3: the flow's cumulative usage BEFORE this turn. OpenAI usage chunks
            // are cumulative WITHIN a turn, so the record's running total is
            // `turn_base + <this turn's latest chunk>` — adding the chunk to the base
            // of PRIOR turns, never chunk-over-chunk (which would double-count). A
            // single authoritative `accumulated_usage.add(turn_usage)` AFTER the inner
            // loop advances the base for the NEXT turn of a multi-turn tool loop.
            let turn_base = accumulated_usage.snapshot();
            // G4 + E1: per-upstream-turn gate over streamed tool-call argument
            // deltas. It buffers leading deltas (keyed by call_id) until the
            // engine classifies the resolved tool name (`hidden` below), then
            // DROPS hidden ones (internal `analyzeImage`, OR a hallucinated tool
            // not in the offered set) or FLUSHES client-visible ones in order — so
            // neither an `analyzeImage` arg fragment nor a hallucinated tool's
            // arguments can ever leak even when a sparse upstream streams
            // arguments before the name. The gate is a pure decision machine; the
            // engine forwards its emissions via `emit_function_call_delta`.
            let mut tool_delta_gate = ToolDeltaGate::new();
            loop {
                let Some(chunk) = Self::next_upstream_chunk(&mut stream, &tx, &abort_token).await?
                else {
                    break;
                };
                if let Some(usage) = chunk.usage.clone() {
                    // D3 (R1 #2): the cumulative-aware dashboard/monitor UPSERT is the
                    // ONLY consumer of `total`, and both its sinks are dashboard-only
                    // (the FlowStore `record_usage` is gated on `api_call_id`, the
                    // monitor `emit_with` no-ops when disabled). So skip the
                    // `flow_usage_from_base_and_chunk` construction entirely on the
                    // production hot path — no `api_call_id` threaded AND the monitor
                    // disabled — keeping `MonitorHub::disabled()` truly zero-overhead.
                    // Borrow `usage` here; it is MOVED into `turn_usage` below.
                    if api_call_id.is_some() || self.monitor.is_enabled() {
                        // `total` is the flow's running cumulative (turn_base + this
                        // cumulative chunk), NOT an increment — so a multi-chunk turn
                        // does not double-count and a midstream cancel keeps this LAST
                        // upserted total (never zero).
                        let total = flow_usage_from_base_and_chunk(turn_base, &usage);
                        if let Some(api_call_id) = &api_call_id {
                            self.flow_store().record_usage(api_call_id, total);
                            // D5 R3 (MEDIUM): mirror the cumulative total onto the serving
                            // token (last-write-wins) so the L1 guard records the flow's
                            // final usage into the metrics layer even if the FlowStore
                            // record is pruned/evicted before finalize. Same dashboard-only
                            // path as `record_usage`, so the disabled path stays zero-cost.
                            serving_token.set_usage(total);
                        }
                        // D3: emit the usage event to the monitor hub. The `/debug/ws`
                        // + dashboard surfaces store it on the record + replay it in
                        // `snapshot()`. D5: the `MetricsLayer` token sum is recorded
                        // ONCE at the TERMINAL seam (`record_terminal_metrics`, from
                        // the record's final cumulative `usage`), NOT per chunk — so
                        // the per-window token sum is the true throughput without
                        // over-counting cumulative chunks here.
                        self.monitor
                            .emit_with(response_id.as_str(), || MonitorEventKind::Usage {
                                prompt: total.prompt,
                                completion: total.completion,
                                total: total.total,
                                // The BARE `/debug/ws` `DebugWsMessage::Usage` contract is
                                // integer-only (AGENTS.md — untouched). Gap 07's UNAVAILABLE
                                // cached/reasoning is a DASHBOARD-surface distinction
                                // (`FlowUsage`/REST/WS-envelope), so project the optional
                                // counts to `0` for the legacy debug-UI echo only.
                                cached: total.cached.unwrap_or(0),
                                reasoning: total.reasoning.unwrap_or(0),
                            });
                    }
                    // D3: the engine's own accumulated_usage advance (post-loop `add`)
                    // and the bare `ResponseUsage` for the CLIENT response need only
                    // the raw chunk — this assignment is ALWAYS required regardless of
                    // the dashboard gate above.
                    turn_usage = Some(usage);
                }
                let emissions = state.apply_chunk(&chunk);
                for emission in emissions {
                    match emission {
                        StreamEmission::OutputItemAdded(item) => {
                            let target = event_state.register_item(&item);
                            self.monitor.emit_with(response_id.as_str(), || {
                                MonitorEventKind::ResponseItem {
                                    event: "response.output_item.added".to_string(),
                                    summary: summarize_response_item(&item),
                                    payload_preview: preview_json(&item),
                                }
                            });
                            self.send_event(
                                &tx,
                                output_item_added_event(item, target.output_index),
                                &abort_token,
                            )
                            .await?;
                        }
                        StreamEmission::OutputTextDelta(delta) => {
                            let target = event_state.active_message_target()?;
                            self.monitor.emit_with(response_id.as_str(), || {
                                MonitorEventKind::OutputTextDelta {
                                    delta: delta.clone(),
                                }
                            });
                            self.send_event(
                                &tx,
                                output_text_delta_event(target.item_id, target.output_index, delta),
                                &abort_token,
                            )
                            .await?;
                            // Gap 02 (true TTFT): stamp `first_content_delta` ONLY AFTER the
                            // FIRST canonical CONTENT delta's `send_event` returns Ok — i.e.
                            // the delta was actually handed off to the client. Stamping
                            // before the `.await?` would record TTFT even when the send fails
                            // (client hung up / kill token fired) and the first token never
                            // reached the client (review round 1, HIGH). This arm is
                            // content-only (reasoning/tool-argument/signature deltas have
                            // their own arms below), so a stream that emits reasoning or tool
                            // deltas first does NOT stamp TTFT early. First-write-wins in the
                            // store makes only the first delivered content delta stamp. Gated
                            // on an `api_call_id` so the production hot path (no dashboard)
                            // skips even the disabled-store early-return's call overhead.
                            if let Some(api_call_id) = &api_call_id {
                                self.flow_store().stamp_first_content_delta(api_call_id);
                            }
                        }
                        StreamEmission::ReasoningItemAdded(item) => {
                            let target = event_state.register_item(&item);
                            self.monitor.emit_with(response_id.as_str(), || {
                                MonitorEventKind::ResponseItem {
                                    event: "response.output_item.added".to_string(),
                                    summary: summarize_response_item(&item),
                                    payload_preview: preview_json(&item),
                                }
                            });
                            self.send_event(
                                &tx,
                                output_item_added_event(item, target.output_index),
                                &abort_token,
                            )
                            .await?;
                        }
                        StreamEmission::ReasoningTextDelta(delta) => {
                            let target = event_state.active_reasoning_target()?;
                            self.monitor.emit_with(response_id.as_str(), || {
                                MonitorEventKind::ReasoningTextDelta {
                                    delta: delta.clone(),
                                }
                            });
                            self.send_event(
                                &tx,
                                reasoning_text_delta_event(
                                    target.item_id,
                                    target.output_index,
                                    delta,
                                ),
                                &abort_token,
                            )
                            .await?;
                        }
                        StreamEmission::ReasoningSignatureDelta(signature) => {
                            let target = event_state.active_reasoning_target()?;
                            self.send_event(
                                &tx,
                                reasoning_signature_delta_event(
                                    target.item_id,
                                    target.output_index,
                                    signature,
                                ),
                                &abort_token,
                            )
                            .await?;
                        }
                        StreamEmission::FunctionCallArgumentsDelta {
                            call_id,
                            name,
                            delta,
                        } => {
                            // Classify the (possibly still-`None`) resolved tool
                            // name against the offered registry: `None` while the
                            // name is unknown (gate buffers), `Some(true)` for a
                            // HIDDEN tool — internal `analyzeImage` OR a
                            // hallucinated name not in the offered set (gate drops
                            // its buffered + later deltas, so the client never sees
                            // it), `Some(false)` for a client-visible tool (gate
                            // flushes + forwards). The gate returns the
                            // allocation-free decision; an overflow of the
                            // pending-byte cap fails the turn cleanly.
                            let hidden = name
                                .as_deref()
                                .map(|n| is_hidden_tool_name(n, &tool_registry));
                            let decision = tool_delta_gate
                                .on_delta(call_id, name, delta, hidden)
                                .map_err(|_| {
                                    AppError::upstream(
                                        "upstream streamed too many tool-call argument bytes before a tool name",
                                    )
                                })?;
                            self.drive_delta_decision(&response_id, &tx, decision, &abort_token)
                                .await?;
                        }
                        StreamEmission::ContentPartAdded => {
                            let target = event_state.active_message_target()?;
                            self.send_event(
                                &tx,
                                content_part_added_event(target.item_id, target.output_index),
                                &abort_token,
                            )
                            .await?;
                        }
                        StreamEmission::ContentPartDone { text } => {
                            let target = event_state.active_message_target()?;
                            self.send_event(
                                &tx,
                                content_part_done_event(target.item_id, target.output_index, text),
                                &abort_token,
                            )
                            .await?;
                        }
                        StreamEmission::ReasoningSummaryPartAdded => {
                            let target = event_state.active_reasoning_target()?;
                            self.send_event(
                                &tx,
                                reasoning_summary_part_added_event(
                                    target.item_id,
                                    target.output_index,
                                ),
                                &abort_token,
                            )
                            .await?;
                        }
                        StreamEmission::ReasoningSummaryPartDone { text } => {
                            let target = event_state.active_reasoning_target()?;
                            self.send_event(
                                &tx,
                                reasoning_summary_part_done_event(
                                    target.item_id,
                                    target.output_index,
                                    text,
                                ),
                                &abort_token,
                            )
                            .await?;
                        }
                        StreamEmission::RefusalDelta(delta) => {
                            self.monitor.emit_with(response_id.as_str(), || {
                                MonitorEventKind::RefusalDelta {
                                    delta: delta.clone(),
                                }
                            });
                            self.send_event(&tx, refusal_delta_event(delta), &abort_token)
                                .await?;
                        }
                    }
                }
            }
            if let Some(usage) = turn_usage {
                accumulated_usage.add(usage);
            }
            let finalized = state.finalize(&tool_registry)?;
            last_finish_reason = finalized.finish_reason.clone();
            last_stop_sequence = finalized.stop_sequence.clone();
            current_messages = upstream_request.messages;
            // G4 round-2 #5: a CLIENT tool whose arguments streamed entirely
            // before its name (name arrived name-only, so no delta ever
            // triggered the flush) still has its leading deltas buffered as
            // `Pending`. Flush them now — in order, before the public items and
            // the `function_call_arguments.done` emitted by `handle_tool_calls`
            // — so the client receives all of its tool-arg deltas. ONLY
            // `analyzeImage`/`ImageAnalysis` deltas are dropped; every other
            // (client) tool's buffer is forwarded.
            //
            // E1: SKIP this flush entirely when the batch is TAINTED (any
            // rejected/hallucinated call present). The whole batch is discarded —
            // no tool is handed off — so a name-only valid tool's buffered deltas
            // must be dropped too, not streamed to the client (the gate drops them
            // when it is discarded at turn end). The repair round re-issues them.
            if finalized.rejected_tool_calls.is_empty() {
                for tool_call in &finalized.tool_calls {
                    if matches!(tool_call.kind, ToolKind::ImageAnalysis) {
                        continue;
                    }
                    // Borrow the id (no clone): the gate takes `&str` and mints the
                    // single owned id it needs for the `Flush` decision.
                    let Some(call_id) = tool_call.internal_call.id.as_deref() else {
                        continue;
                    };
                    let decision = tool_delta_gate.flush_pending_client_tool(call_id);
                    self.drive_delta_decision(&response_id, &tx, decision, &abort_token)
                        .await?;
                }
            }
            self.emit_completed_public_items(
                &response_id,
                &tx,
                &abort_token,
                &finalized,
                &mut public_history,
                &mut response_output,
                &mut event_state,
            )
            .await?;
            // D6: compose kill with hangup after emitting the completed items, before
            // deciding whether to loop for another turn.
            if tx.is_closed() || abort_token.is_cancelled() {
                return Err(AppError::cancelled());
            }
            if let Some(message) = finalized.internal_assistant_message.clone() {
                push_shaped(&mut current_messages, message, roles)?;
            }
            // E1: bounded soft-reject repair for hallucinated (unoffered) tool
            // calls. A TAINTED batch (any rejected call) executes NO server tool
            // and hands off NO client tool; instead we inject a synthetic tool
            // result per call + a closed-tool-set note and run ONE bounded
            // in-gateway repair round (same loop, same provider — NOT a failover,
            // NOT a token-duplicating retry) so the model can self-correct. Past
            // the ceiling we end the turn with a STRUCTURED terminal failure (NOT
            // a raw `?` abort); any already-streamed text was emitted by
            // `emit_completed_public_items` above and is never retracted.
            if !finalized.rejected_tool_calls.is_empty() {
                // Provider attribution for the always-on observability (read
                // lazily — this is a rare path). `served_model` is the resolved
                // upstream model.
                let provider = serving_token
                    .snapshot()
                    .1
                    .unwrap_or_else(|| "unknown".to_string());
                let unknown_tools: Vec<&str> = finalized
                    .rejected_tool_calls
                    .iter()
                    .map(|rejected| rejected.name.as_str())
                    .collect();
                // Always-on WARN — the incident this guards was invisible in
                // journalctl (the old hard-error surfaced only to the client).
                tracing::warn!(
                    response_id = %response_id,
                    provider = %provider,
                    served_model = %upstream_model,
                    unknown_tools = ?unknown_tools,
                    offered_tools = tools.len(),
                    repair_round = unknown_tool_repair_rounds,
                    "upstream returned tool call(s) not in the offered tool set; soft-rejecting (bounded repair)"
                );
                self.monitor
                    .emit_with(response_id.as_str(), || MonitorEventKind::ToolPhase {
                        phase: "unknown_tool_rejected".to_string(),
                        detail: format!(
                            "{} unoffered tool call(s) rejected; repair round {}",
                            finalized.rejected_tool_calls.len(),
                            unknown_tool_repair_rounds
                        ),
                    });

                if unknown_tool_repair_rounds >= UNKNOWN_TOOL_REPAIR_CEILING {
                    // Exhausted: emit a STRUCTURED terminal failure. The
                    // spawn-closure renders the returned error as the canonical
                    // `response.failed` (code `invalid_tool_call`), which the three
                    // inbound converters render in their own format.
                    self.record_unknown_tool_outcome(
                        &provider,
                        &upstream_model,
                        UnknownToolOutcome::Exhausted,
                    );
                    self.monitor
                        .emit_with(response_id.as_str(), || MonitorEventKind::ToolPhase {
                            phase: "unknown_tool_repair_exhausted".to_string(),
                            detail: format!(
                                "unoffered tool calls persisted after {} repair round(s)",
                                unknown_tool_repair_rounds
                            ),
                        });
                    tracing::warn!(
                        response_id = %response_id,
                        provider = %provider,
                        served_model = %upstream_model,
                        repair_round = unknown_tool_repair_rounds,
                        "unknown-tool repair exhausted; ending turn with a structured terminal failure"
                    );
                    return Err(AppError::unknown_tool_repair_exhausted());
                }

                // Under the ceiling: inject a synthetic tool result for EVERY call
                // in the tainted batch (so the chat history stays well-formed —
                // each replayed tool_call gets a matching tool result) + the
                // closed-tool-set note, then run another round. The internal
                // assistant message (attempted valid + rejected calls) was pushed
                // above.
                unknown_tool_repair_rounds += 1;
                pending_repair = true;
                for tool_call in &finalized.tool_calls {
                    if let Some(call_id) = tool_call.internal_call.id.clone() {
                        push_shaped(
                            &mut current_messages,
                            synthetic_tool_result(call_id, TAINTED_TOOL_RESULT.to_string()),
                            roles,
                        )?;
                    }
                }
                for rejected in &finalized.rejected_tool_calls {
                    push_shaped(
                        &mut current_messages,
                        synthetic_tool_result(
                            rejected.call_id.clone(),
                            format!(
                                "tool_unavailable: the tool \"{}\" is not one of the tools provided in this request.",
                                rejected.name
                            ),
                        ),
                        roles,
                    )?;
                }
                // Author the closed-tool-set note as a `system` message, then run
                // it through the SAME profile role mapping an interleaved system
                // message would get (tail ⇒ inline). For the DeepSeek profile this
                // rewrites it to `developer`, preserving the "exactly one leading
                // system message" invariant the backend template expects; the
                // pre-send merge above folds it into any adjacent developer run.
                push_shaped(&mut current_messages, closed_tool_set_note(), roles)?;
                // Relax any forced `tool_choice` so the model can answer in prose
                // or re-issue a VALID tool call rather than be forced back into a
                // tool it may not actually have.
                current_tool_choice = Value::String("auto".to_string());
                continue;
            }
            // Reached here ⇒ this round had NO rejected tool calls. If a prior
            // round triggered a repair, the model has now self-corrected — count
            // the `Repaired` outcome exactly once.
            if pending_repair {
                pending_repair = false;
                let provider = serving_token
                    .snapshot()
                    .1
                    .unwrap_or_else(|| "unknown".to_string());
                self.record_unknown_tool_outcome(
                    &provider,
                    &upstream_model,
                    UnknownToolOutcome::Repaired,
                );
            }
            if finalized.tool_calls.is_empty() {
                break;
            }
            self.handle_tool_calls(
                &response_id,
                &finalized,
                &tx,
                &abort_token,
                vision_session.as_deref(),
                &mut current_messages,
                roles,
                &mut public_history,
                &mut response_output,
                &mut event_state,
            )
            .await?;
            // Decide whether to continue the tool loop. `handle_tool_calls`
            // already handed off any CLIENT-tool batch (and a mixed batch is
            // rejected before reaching here), so a batch that ran at all and
            // contains no client tool is a pure SERVER-tool batch (web_search
            // and/or analyzeImage). Its results are now in the chat history, so
            // relax any forced `tool_choice` to `auto` (let the model answer or
            // call again) and bump each present server tool's INDEPENDENT round
            // ceiling so a tool-only loop cannot run forever.
            let can_search = self.config.brave_api_key.is_some();
            let had_web_search = can_search
                && finalized
                    .tool_calls
                    .iter()
                    .any(|call| matches!(call.kind, ToolKind::WebSearch));
            let had_image_analysis = vision_session.is_some()
                && finalized
                    .tool_calls
                    .iter()
                    .any(|call| matches!(call.kind, ToolKind::ImageAnalysis));
            let had_client_tool = finalized.tool_calls.iter().any(|call| {
                !(matches!(call.kind, ToolKind::WebSearch) && can_search
                    || matches!(call.kind, ToolKind::ImageAnalysis) && vision_session.is_some())
            });
            if !finalized.tool_calls.is_empty() && !had_client_tool {
                if had_web_search {
                    web_search_rounds += 1;
                    // `max_web_search_rounds == 0` is treated as "unlimited" by
                    // config, but an unbounded loop lets a model that keeps
                    // choosing web_search hang the turn. Always enforce an
                    // absolute ceiling so the turn is guaranteed to end.
                    const WEB_SEARCH_ROUNDS_HARD_CEILING: usize = 25;
                    let configured_limit = if self.config.max_web_search_rounds > 0 {
                        self.config.max_web_search_rounds
                    } else {
                        WEB_SEARCH_ROUNDS_HARD_CEILING
                    };
                    let effective_limit = configured_limit.min(WEB_SEARCH_ROUNDS_HARD_CEILING);
                    if web_search_rounds >= effective_limit {
                        return Err(AppError::upstream("web search round limit exceeded"));
                    }
                }
                if had_image_analysis {
                    image_analysis_rounds += 1;
                    // Absolute ceiling on `analyzeImage` rounds — INDEPENDENT of
                    // the web-search ceiling (AGENTS.md: do not touch
                    // `WEB_SEARCH_ROUNDS_HARD_CEILING`). A model that re-requests
                    // image analysis every round must still terminate.
                    const IMAGE_ANALYSIS_ROUNDS_HARD_CEILING: usize = 8;
                    if image_analysis_rounds >= IMAGE_ANALYSIS_ROUNDS_HARD_CEILING {
                        return Err(AppError::upstream("image analysis round limit exceeded"));
                    }
                }
                // Results are now in the message history; let the model answer
                // (or call a server tool again) instead of being forced.
                current_tool_choice = Value::String("auto".to_string());
                continue;
            }
            break;
        }

        let model_name = upstream_model.clone();
        let completed_output = response_output.clone();
        let metadata = request.metadata.clone();
        if request.store {
            self.replay_store
                .insert(ReplayRecord {
                    model: model_name.clone(),
                    instructions: request.instructions,
                    visible_history: public_history,
                    internal_messages: current_messages,
                })
                .await;
        }

        let usage = accumulated_usage.into_response_usage();
        // T7: typed terminal reason from the upstream finish_reason. `length` ⇒
        // incomplete; everything else ⇒ completed. The typed reason is carried
        // on the resource so the Anthropic converter gates reasoning-promotion
        // on `reason.is_clean_stop()` (stop only), not on the event-type string
        // — a future non-stop terminal reason arriving as `response.completed`
        // can no longer wrongly promote.
        let terminal_reason = crate::models::responses::TerminalReason::from_finish_reason(
            last_finish_reason.as_deref(),
        );
        let is_incomplete = matches!(
            terminal_reason,
            crate::models::responses::TerminalReason::Length
        );
        let resource = ResponseResource {
            id: response_id.clone(),
            object: "response".to_string(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            status: if is_incomplete {
                "incomplete".to_string()
            } else {
                "completed".to_string()
            },
            output: completed_output,
            model: model_name,
            usage,
            metadata,
            incomplete_details: if is_incomplete {
                Some(crate::models::responses::IncompleteDetails {
                    reason: "max_output_tokens".to_string(),
                })
            } else {
                None
            },
            stop_sequence: last_stop_sequence,
            terminal_reason: Some(terminal_reason),
        };
        self.monitor.emit_with(response_id.as_str(), || {
            let final_preview = preview_json_limited_with_images(&resource, 128 * 1024);
            MonitorEventKind::FinalResponse {
                status: resource.status.clone(),
                payload_preview: final_preview.text,
                images: final_preview.images,
            }
        });
        if is_incomplete {
            self.send_event(&tx, incomplete_event(resource), &abort_token)
                .await?;
        } else {
            self.send_event(&tx, completed_event(resource), &abort_token)
                .await?;
        }
        // Gap 02: the terminal `response.completed`/`response.incomplete` has been
        // emitted — stamp the `stream_end` phase. This is the clean-completion edge
        // (the spawned closure's `guard.finalize` stamps `finalize` immediately after);
        // a flow that errored/cancelled before here never reaches this `?`-guarded path,
        // so `stream_end` stays `None` for it. Gated on `api_call_id` so the production
        // hot path skips the call. The `?`s above mean we only reach here on a clean
        // emit, which is exactly the semantics we want.
        if let Some(api_call_id) = &api_call_id {
            self.flow_store().stamp_stream_end(api_call_id);
        }
        self.monitor.emit(response_id, MonitorEventKind::Completed);
        // F1c (finding #3): carry the terminal shape to the seam so the capture
        // artifact records `incomplete` for a max-token truncation. `is_incomplete`
        // was already derived above from the upstream `finish_reason` (the same bit
        // that chose `response.incomplete` vs `response.completed`).
        Ok(if is_incomplete {
            TurnCompletion::Incomplete
        } else {
            TurnCompletion::Completed
        })
    }

    /// Resolve `model` against the upstream catalog, returning the served model
    /// AND whether the resolution was GENUINE (true = the request truly maps to
    /// the served backend; false = collapsed to a real, differing catalog
    /// default because the model was blank/unmatched/ambiguous). The `genuine`
    /// flag is a byproduct of this ONE ladder walk — not a re-derived
    /// side-channel — so G4 gating (the only `genuine` consumer) keeps a single
    /// resolution truth (T2 deleted `request_model_genuinely_resolves`).
    async fn normalize_upstream_model(&self, model: &str) -> (String, bool) {
        let catalog = match self.load_upstream_model_catalog().await {
            Ok(catalog) => catalog,
            Err(err) => {
                tracing::warn!(model, error = %err, "failed to refresh upstream model catalog");
                // Catalog unavailable ⇒ model flows through unchanged ⇒ genuine.
                return (model.to_string(), true);
            }
        };
        // Precedence (mirrors `RoutingModelCatalog::resolve`, G7; route-match
        // uses the shared `config::route_matches` primitive):
        //   1. exact catalog id (an exact id always wins),
        //   2. ad-hoc route match (exact name or glob) -> pass the model through
        //      UNCHANGED so the routing client dispatches the route instead of
        //      collapsing an unknown route name to the catalog default,
        //   3. unique canonical-key catalog match,
        //   4. default catalog id.
        // The ladder is duplicated here vs `RoutingModelCatalog::resolve` because
        // the engine normalizes against its own `UpstreamModelCatalog` (which also
        // carries G3 context limits for G3 budgeting) rather than the routing
        // client's catalog. T2 collapsed the GATING side-channel
        // (`request_model_genuinely_resolves` deleted; `genuine` is now a
        // byproduct of this walk, and the gate's candidates come from a typed
        // `BackendCandidatePlan` on the routing layer). The ladder DEDUP here
        // remains because `UpstreamModelCatalog::context_limit_by_id` feeds G3
        // budgeting, which T9 moves behind route/provider resolution — at which
        // point this fn delegates to the routing catalog and the ladder
        // collapses. Without step 2, a mixed `upstreams` + `model_routes` config
        // would pre-normalize a route-only model to the catalog default here and
        // the route would never fire.
        if let Some(exact) = catalog.exact_id(model) {
            if exact != model {
                tracing::info!(
                    requested_model = %model,
                    normalized_model = %exact,
                    "normalized upstream model name from backend catalog"
                );
            }
            return (exact, true);
        }
        if self.config.matches_model_route(model) {
            // Leave the model as-is; `RoutingUpstreamClient::resolve` performs
            // the route match + upstream-model rewrite. A route match is genuine.
            return (model.to_string(), true);
        }
        if let Some(canonical) = catalog.canonical_unique(model) {
            if canonical != model {
                tracing::info!(
                    requested_model = %model,
                    normalized_model = %canonical,
                    "normalized upstream model name from backend catalog"
                );
            }
            return (canonical, true);
        }
        // No exact id, ad-hoc route, or canonical-key match: fall back to the
        // first catalog model (claude-relay parity). A NON-BLANK requested model
        // that lands here is a genuine mismatch — the loaded backend model
        // differs from what the client asked for — so surface it at WARN. A
        // blank/absent model defaulting to the first catalog id is expected and
        // stays at INFO. Both blank and non-blank default-fallbacks are
        // NON-genuine: the served model is the catalog default, not the request
        // model (and a blank request has no model identity to attach an override
        // to), so a `native_vision` override on the request model must NOT
        // attach to the (different) default backend (G4 round-8 #1).
        match catalog.default_id() {
            Some(default) if model.trim().is_empty() => {
                tracing::info!(
                    fallback_model = %default,
                    "no model requested; using the default catalog model"
                );
                (default, false)
            }
            Some(default) => {
                if self.should_warn_model_fallback(model) {
                    tracing::warn!(
                        requested_model = %model,
                        fallback_model = %default,
                        "requested model is not served by any configured upstream; falling back to the default catalog model"
                    );
                }
                (default, false)
            }
            None => {
                // No default to collapse to (empty catalog) ⇒ the model passes
                // through unchanged, so the request model IS the served model ⇒
                // genuine (mirrors `RoutingModelCatalog::resolve` returning None
                // for an empty catalog).
                (model.to_string(), true)
            }
        }
    }

    /// Rate-limit the model-fallback WARN to once per catalog-TTL window per
    /// requested model. A request resolves its model twice (HTTP label + engine
    /// dispatch), and a mismatch usually persists across many requests, so an
    /// un-throttled WARN would flood the log. Stale entries are pruned on access
    /// so the map stays bounded even under random/hostile model names.
    fn should_warn_model_fallback(&self, requested_model: &str) -> bool {
        let now = std::time::Instant::now();
        let window = std::time::Duration::from_secs(UPSTREAM_MODEL_CATALOG_TTL_SECS);
        let mut warned = self
            .model_fallback_warned
            .lock()
            .expect("model fallback warn lock poisoned");
        warned.retain(|_, last| now.duration_since(*last) < window);
        if warned.contains_key(requested_model) {
            return false;
        }
        warned.insert(requested_model.to_string(), now);
        true
    }

    async fn load_upstream_model_catalog(&self) -> AppResult<UpstreamModelCatalog> {
        let mut cache = self.upstream_model_catalog.lock().await;
        if let Some(cached) = cache.as_ref()
            && cached.fetched_at.elapsed().as_secs() < UPSTREAM_MODEL_CATALOG_TTL_SECS
        {
            return Ok(cached.catalog.clone());
        }
        // Single `/v1/models` snapshot feeds BOTH model normalization and G3
        // context budgeting, so ids and context limits can never describe
        // different provider states.
        let entries = self.upstream.supported_model_catalog().await?;
        let catalog = UpstreamModelCatalog::from_entries(entries);
        *cache = Some(CachedUpstreamModelCatalog {
            fetched_at: std::time::Instant::now(),
            catalog: catalog.clone(),
        });
        Ok(catalog)
    }

    /// Context-window length the upstream reports for the resolved catalog
    /// model id, for G3 pre-flight budgeting's NON-ROUTING fallback (T9). The
    /// primary budgeting path is `candidate_context_floor` over the routing
    /// layer's `BackendCandidatePlan` (conservative MIN across the failover
    /// chain); this is the fallback when the plan has no known limits
    /// (non-routing single upstream / all-unknown / catalog-load failure). A
    /// catalog-load failure is non-fatal (logged) and yields `None` so budgeting
    /// no-ops.
    async fn upstream_model_context_limit(&self, resolved_model: &str) -> Option<i64> {
        match self.load_upstream_model_catalog().await {
            Ok(catalog) => catalog.context_limit_by_id.get(resolved_model).copied(),
            Err(err) => {
                tracing::warn!(error = %err, "failed to load catalog for context budgeting");
                None
            }
        }
    }

    async fn next_upstream_chunk(
        stream: &mut crate::upstream::UpstreamStream,
        tx: &mpsc::Sender<SseEvent>,
        // D6: the flow's kill token, composed with the `tx.closed()` hangup branch so a
        // dashboard kill cancels the in-flight chunk read (499) just like a hang-up.
        abort_token: &tokio_util::sync::CancellationToken,
    ) -> AppResult<Option<ChatCompletionChunk>> {
        tokio::select! {
            biased;
            _ = tx.closed() => Err(AppError::cancelled()),
            _ = abort_token.cancelled() => Err(AppError::cancelled()),
            result = stream.next() => match result {
                Some(chunk) => chunk.map(Some),
                None => Ok(None),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_completed_public_items(
        &self,
        response_id: &str,
        tx: &mpsc::Sender<SseEvent>,
        // D6: the flow's kill token, composed with `tx.closed()` inside `send_event`
        // so a dashboard kill cancels these terminal-item sends even under full-channel
        // backpressure (no poll/select site here — the SEND is the only block point).
        abort_token: &tokio_util::sync::CancellationToken,
        finalized: &FinalizedAssistantTurn,
        public_history: &mut Vec<ResponseItem>,
        response_output: &mut Vec<ResponseItem>,
        event_state: &mut ResponseEventState,
    ) -> AppResult<()> {
        if let Some(reasoning) = finalized.reasoning_item.clone() {
            let target = event_state.target_for_item(&reasoning);
            public_history.push(reasoning.clone());
            response_output.push(reasoning.clone());
            if finalized.reasoning_part_emitted
                && let ResponseItem::Reasoning { ref content, .. } = reasoning
            {
                let reasoning_text = content
                    .as_ref()
                    .and_then(|items| items.first())
                    .map(|item| match item {
                        crate::models::responses::ReasoningContentItem::ReasoningText { text }
                        | crate::models::responses::ReasoningContentItem::Text { text } => {
                            text.clone()
                        }
                    })
                    .unwrap_or_default();
                self.send_event(
                    tx,
                    reasoning_summary_part_done_event(
                        target.item_id.clone(),
                        target.output_index,
                        reasoning_text,
                    ),
                    abort_token,
                )
                .await?;
            }
            self.monitor
                .emit_with(response_id, || MonitorEventKind::ResponseItem {
                    event: "response.output_item.done".to_string(),
                    summary: summarize_response_item(&reasoning),
                    payload_preview: preview_json(&reasoning),
                });
            self.send_event(
                tx,
                output_item_done_event(reasoning, target.output_index),
                abort_token,
            )
            .await?;
        }
        if let Some(message) = finalized.message_item.clone() {
            let target = event_state.target_for_item(&message);
            if let ResponseItem::Message { ref content, .. } = message {
                let full_text: String = content
                    .iter()
                    .filter_map(|c| match c {
                        crate::models::responses::ContentItem::OutputText { text } => {
                            Some(text.as_str())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("");
                if !full_text.is_empty() {
                    self.send_event(
                        tx,
                        output_text_done_event(
                            target.item_id.clone(),
                            target.output_index,
                            full_text.clone(),
                        ),
                        abort_token,
                    )
                    .await?;
                    if finalized.content_part_emitted {
                        self.send_event(
                            tx,
                            content_part_done_event(
                                target.item_id.clone(),
                                target.output_index,
                                full_text,
                            ),
                            abort_token,
                        )
                        .await?;
                    }
                }
            }
            public_history.push(message.clone());
            response_output.push(message.clone());
            self.monitor
                .emit_with(response_id, || MonitorEventKind::ResponseItem {
                    event: "response.output_item.done".to_string(),
                    summary: summarize_response_item(&message),
                    payload_preview: preview_json(&message),
                });
            self.send_event(
                tx,
                output_item_done_event(message, target.output_index),
                abort_token,
            )
            .await?;
        }
        if !finalized.refusal_text.is_empty() {
            self.send_event(
                tx,
                refusal_done_event(finalized.refusal_text.clone()),
                abort_token,
            )
            .await?;
        }
        Ok(())
    }

    /// Generic server-tool dispatcher. Classifies EVERY tool call as
    /// server-runnable or client-handed-off, rejects a mixed batch up front,
    /// then either hands all calls to the client or runs all server tools
    /// SEQUENTIALLY (`parallel_tool_calls: false` stays forced upstream).
    ///
    /// A call is server-runnable when it is `web_search` and Brave is configured,
    /// OR `analyzeImage` (`ToolKind::ImageAnalysis`) and the image agent is
    /// active for this turn (`vision_session` is `Some`). Centralizing the
    /// classification here keeps the mixed-tools rule a single decision and lets
    /// new server tools slot in without scattering predicates.
    #[allow(clippy::too_many_arguments)]
    async fn handle_tool_calls(
        &self,
        response_id: &str,
        finalized: &FinalizedAssistantTurn,
        tx: &mpsc::Sender<SseEvent>,
        // D6: the flow's kill token, composed with `tx.closed()` here and forwarded to
        // the server-tool executors (web search / vision) so a dashboard kill cancels a
        // stuck tool call (499) the same as a hang-up.
        abort_token: &tokio_util::sync::CancellationToken,
        vision_session: Option<&str>,
        current_messages: &mut Vec<ChatMessage>,
        roles: Option<&crate::config::RolesConfig>,
        public_history: &mut Vec<ResponseItem>,
        response_output: &mut Vec<ResponseItem>,
        event_state: &mut ResponseEventState,
    ) -> AppResult<()> {
        if tx.is_closed() || abort_token.is_cancelled() {
            return Err(AppError::cancelled());
        }
        let can_search = self.config.brave_api_key.is_some();
        let image_agent_active = vision_session.is_some();
        // A single classification pass over the batch: every call is either a
        // server tool this gateway runs, or a client tool handed off. This is
        // the ONE place the server/client split is decided (review risk #1).
        let is_server_tool = |call: &ResolvedToolCall| match call.kind {
            ToolKind::WebSearch => can_search,
            ToolKind::ImageAnalysis => image_agent_active,
            _ => false,
        };
        let has_server_tool = finalized.tool_calls.iter().any(is_server_tool);
        let has_client_tool = finalized
            .tool_calls
            .iter()
            .any(|call| !is_server_tool(call));
        if has_server_tool && has_client_tool {
            return Err(AppError::upstream(
                "mixed provider-side and client-side tool calls are not supported in v1",
            ));
        }
        if has_client_tool {
            for tool_call in &finalized.tool_calls {
                if let ResponseItem::FunctionCall {
                    ref call_id,
                    ref name,
                    ref arguments,
                    ..
                } = tool_call.public_item
                {
                    self.send_event(
                        tx,
                        function_call_args_done_event(
                            call_id.clone(),
                            name.clone(),
                            arguments.clone(),
                        ),
                        abort_token,
                    )
                    .await?;
                }
                self.monitor
                    .emit_with(response_id, || MonitorEventKind::ToolPhase {
                        phase: "client_tool_handoff".to_string(),
                        detail: summarize_response_item(&tool_call.public_item),
                    });
                let target = event_state.target_for_item(&tool_call.public_item);
                public_history.push(tool_call.public_item.clone());
                response_output.push(tool_call.public_item.clone());
                self.monitor
                    .emit_with(response_id, || MonitorEventKind::ResponseItem {
                        event: "response.output_item.done".to_string(),
                        summary: summarize_response_item(&tool_call.public_item),
                        payload_preview: preview_json(&tool_call.public_item),
                    });
                self.send_event(
                    tx,
                    output_item_done_event(tool_call.public_item.clone(), target.output_index),
                    abort_token,
                )
                .await?;
            }
            return Ok(());
        }
        // Server tools only: execute SEQUENTIALLY. A batch may mix `web_search`
        // and `analyzeImage` (both server-runnable); each dispatches to its own
        // executor in order, never in parallel.
        for tool_call in &finalized.tool_calls {
            match tool_call.kind {
                ToolKind::ImageAnalysis => {
                    self.run_image_analysis(
                        response_id,
                        tool_call,
                        vision_session,
                        tx,
                        abort_token,
                        current_messages,
                        roles,
                    )
                    .await?;
                }
                _ => {
                    self.run_web_search(
                        response_id,
                        tool_call,
                        tx,
                        abort_token,
                        current_messages,
                        roles,
                        public_history,
                        response_output,
                        event_state,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // distinct mutable tool-loop state threaded per turn
    async fn run_web_search(
        &self,
        response_id: &str,
        tool_call: &ResolvedToolCall,
        tx: &mpsc::Sender<SseEvent>,
        // D6: the flow's kill token, composed with this executor's `tx.closed()` checks
        // so a dashboard kill cancels a stuck/slow Brave search (499) like a hang-up.
        abort_token: &tokio_util::sync::CancellationToken,
        current_messages: &mut Vec<ChatMessage>,
        roles: Option<&crate::config::RolesConfig>,
        public_history: &mut Vec<ResponseItem>,
        response_output: &mut Vec<ResponseItem>,
        event_state: &mut ResponseEventState,
    ) -> AppResult<()> {
        let ResponseItem::WebSearchCall {
            id,
            status: _,
            action,
        } = &tool_call.public_item
        else {
            return Err(AppError::internal("expected web_search_call item"));
        };
        let partial = ResponseItem::WebSearchCall {
            id: id.clone(),
            status: Some("in_progress".to_string()),
            action: None,
        };
        self.monitor
            .emit_with(response_id, || MonitorEventKind::ToolPhase {
                phase: "provider_tool_detected".to_string(),
                detail: summarize_response_item(&tool_call.public_item),
            });
        self.monitor
            .emit_with(response_id, || MonitorEventKind::ResponseItem {
                event: "response.output_item.added".to_string(),
                summary: summarize_response_item(&partial),
                payload_preview: preview_json(&partial),
            });
        let partial_target = event_state.register_item(&partial);
        self.send_event(
            tx,
            output_item_added_event(partial, partial_target.output_index),
            abort_token,
        )
        .await?;

        let query = extract_web_search_query(action, &tool_call.arguments)?;
        self.monitor
            .emit_with(response_id, || MonitorEventKind::ToolPhase {
                phase: "provider_tool_running".to_string(),
                detail: format!("web_search {query}"),
            });
        if tx.is_closed() || abort_token.is_cancelled() {
            return Err(AppError::cancelled());
        }
        // The search backend (Brave) has no internal timeout; without this
        // bound a slow or stalled search request would block the turn forever
        // and the client would hang behind the SSE keep-alive. Degrade
        // gracefully so the model can still produce a final answer.
        let outcome: SearchOutcome = tokio::select! {
            biased;
            _ = tx.closed() => return Err(AppError::cancelled()),
            // D6: a kill during the search cancels it (499), same as a hang-up.
            _ = abort_token.cancelled() => return Err(AppError::cancelled()),
            result = timeout(self.config.request_timeout, self.search.search(&query)) => match result {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(err)) => SearchOutcome {
                    formatted: format!("web_search failed: {err}"),
                    sources: Vec::new(),
                },
                Err(_) => SearchOutcome {
                    formatted: "web_search timed out before returning results.".to_string(),
                    sources: Vec::new(),
                },
            },
        };

        let completed = ResponseItem::WebSearchCall {
            id: id.clone(),
            status: Some("completed".to_string()),
            action: action.clone(),
        };
        let completed_target = event_state.target_for_item(&completed);
        public_history.push(completed.clone());
        response_output.push(completed.clone());
        self.monitor
            .emit_with(response_id, || MonitorEventKind::ResponseItem {
                event: "response.output_item.done".to_string(),
                summary: summarize_response_item(&completed),
                payload_preview: preview_json(&completed),
            });
        self.send_event(
            tx,
            output_item_done_event(completed, completed_target.output_index),
            abort_token,
        )
        .await?;

        // Surface the search to Anthropic clients. The OpenAI `web_search_call`
        // item above carries no results (matching OpenAI's schema), so this
        // additive event hands the structured sources to the Anthropic
        // converter, which renders them as `server_tool_use` +
        // `web_search_tool_result` blocks. Non-Anthropic clients ignore the
        // unknown SSE event, keeping the Responses stream OpenAI-compatible.
        let tool_use_id = id
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("srvtoolu_{}", Uuid::new_v4().simple()));
        let result_items: Vec<Value> = outcome
            .sources
            .iter()
            .map(|source| {
                serde_json::json!({
                    "type": "web_search_result",
                    "url": source.url,
                    "title": source.title,
                })
            })
            .collect();
        self.send_event(
            tx,
            SseEvent {
                event: "response.web_search_results".to_string(),
                data: serde_json::json!({
                    "type": "response.web_search_results",
                    "tool_use_id": tool_use_id,
                    "query": query,
                    "results": result_items,
                }),
            },
            abort_token,
        )
        .await?;
        self.monitor
            .emit_with(response_id, || MonitorEventKind::ToolPhase {
                phase: "provider_tool_completed".to_string(),
                detail: format!("web_search result {}", preview_text(&outcome.formatted)),
            });

        push_shaped(
            current_messages,
            ChatMessage {
                role: "tool".to_string(),
                content: Some(Value::String(outcome.formatted.clone())),
                tool_call_id: tool_call.internal_call.id.clone(),
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            },
            roles,
        )?;
        Ok(())
    }

    /// Run the server-side `analyzeImage` tool (G4). Resolves the requested
    /// cached images, calls the vision backend bounded by `request_timeout` and
    /// cancellable via `tx.closed()`, and injects the description (or a
    /// model-visible failure/timeout message) back into `current_messages` as
    /// the tool result so the text model can answer.
    ///
    /// Unlike `run_web_search`, this emits NO public `output_item` events and
    /// pushes NOTHING to `public_history`/`response_output`: `analyzeImage` is an
    /// internal server tool that must never surface to any client (review risk
    /// #3). A backend failure/timeout degrades to model-visible tool text so the
    /// turn still completes (matching the Brave contract); an `AppError::internal`
    /// is reserved for an impossible state (e.g. the dispatcher routed a
    /// non-FunctionCall item here).
    #[allow(clippy::too_many_arguments)] // distinct mutable tool-loop state threaded per turn
    async fn run_image_analysis(
        &self,
        response_id: &str,
        tool_call: &ResolvedToolCall,
        vision_session: Option<&str>,
        tx: &mpsc::Sender<SseEvent>,
        // D6: the flow's kill token, composed with this executor's `tx.closed()` checks
        // so a dashboard kill cancels a stuck/slow vision call (499) like a hang-up.
        abort_token: &tokio_util::sync::CancellationToken,
        current_messages: &mut Vec<ChatMessage>,
        roles: Option<&crate::config::RolesConfig>,
    ) -> AppResult<()> {
        let ResponseItem::FunctionCall { .. } = &tool_call.public_item else {
            return Err(AppError::internal(
                "expected analyzeImage function call item",
            ));
        };
        // The session is always present here: the dispatcher only routes to this
        // executor when `vision_session.is_some()`. Treat its absence as an
        // impossible state rather than silently degrading.
        let session_id = vision_session.ok_or_else(|| {
            AppError::internal("analyzeImage dispatched without a vision session")
        })?;
        if tx.is_closed() || abort_token.is_cancelled() {
            return Err(AppError::cancelled());
        }
        let vision_request =
            VisionRequest::from_arguments(&tool_call.arguments, session_id, &self.image_cache);
        self.monitor
            .emit_with(response_id, || MonitorEventKind::ToolPhase {
                phase: "image_analysis_running".to_string(),
                detail: format!(
                    "analyzeImage ids={:?} images={}",
                    vision_request.image_ids,
                    vision_request.images.len()
                ),
            });

        let result_text = if vision_request.images.is_empty() {
            // No requested id resolved to a cached image. Surface a model-visible
            // message (not an error) so the model can recover (e.g. re-ask or
            // answer without the image) instead of hanging the turn.
            format!(
                "[Vision analysis unavailable: no cached image found for ids {:?}. The image may have expired or the id is wrong.]",
                vision_request.image_ids
            )
        } else {
            // Bounded + cancellable, mirroring run_web_search: a stalled vision
            // backend must not hang the turn, and a client hang-up cancels it.
            tokio::select! {
                biased;
                _ = tx.closed() => return Err(AppError::cancelled()),
                // D6: a kill during the vision call cancels it (499), same as a hang-up.
                _ = abort_token.cancelled() => return Err(AppError::cancelled()),
                result = timeout(self.config.request_timeout, self.vision.analyze(&vision_request)) => match result {
                    // Round-3 #3: redact the SUCCESS description before it is
                    // logged (monitor preview below) or injected as a tool
                    // result, so an echoing vision backend cannot leak a
                    // submitted `data:`/signed image URL. Defense-in-depth even
                    // though `ReqwestVisionClient` already redacts at the source,
                    // so any `VisionClient` impl is covered. The error message is
                    // already redacted inside the client.
                    Ok(Ok(outcome)) => crate::redaction::redact_vision_text(&outcome.text),
                    Ok(Err(err)) => format!("[Vision analysis failed: {err}]"),
                    Err(_) => "[Vision analysis timed out before returning a result.]".to_string(),
                },
            }
        };
        self.monitor
            .emit_with(response_id, || MonitorEventKind::ToolPhase {
                phase: "image_analysis_completed".to_string(),
                detail: format!("analyzeImage result {}", preview_text(&result_text)),
            });

        // Inject the description as the tool result keyed to the model's
        // `analyzeImage` call id, so the follow-up upstream turn sees it. Nothing
        // is added to public history/output.
        push_shaped(
            current_messages,
            ChatMessage {
                role: "tool".to_string(),
                content: Some(Value::String(result_text)),
                tool_call_id: tool_call.internal_call.id.clone(),
                name: None,
                reasoning_content: None,
                thinking: None,
                tool_calls: None,
            },
            roles,
        )?;
        Ok(())
    }
}

/// E1: classify a streamed tool name as HIDDEN from the client (the
/// [`ToolDeltaGate`] drops its argument deltas) vs client-visible. A name NOT in
/// the offered registry is hidden (a hallucinated/unoffered tool — the client
/// must not see it before the engine soft-rejects it); the server-side
/// `analyzeImage` tool is hidden (G4); every other offered tool is visible
/// (forwarded — `web_search` rendering is decided downstream by the converters).
fn is_hidden_tool_name(
    name: &str,
    registry: &crate::adapters::responses_to_chat::ToolRegistry,
) -> bool {
    let name_lc = name.to_ascii_lowercase();
    match registry.get(&name_lc) {
        None => true,
        Some(ToolKind::ImageAnalysis) => true,
        Some(_) => false,
    }
}

/// E1: a synthetic `tool`-role chat message keyed to `call_id`, injected into the
/// repair round so the upstream history stays well-formed (every replayed
/// tool_call has a matching tool result). Mirrors the web-search/vision tool-result
/// shape.
fn synthetic_tool_result(call_id: String, content: String) -> ChatMessage {
    ChatMessage {
        role: "tool".to_string(),
        content: Some(Value::String(content)),
        tool_call_id: Some(call_id),
        name: None,
        reasoning_content: None,
        thinking: None,
        tool_calls: None,
    }
}

/// E1: closed-tool-set prevention note (option C) injected as a `system` message
/// in the repair round.
fn closed_tool_set_note() -> ChatMessage {
    ChatMessage {
        role: "system".to_string(),
        content: Some(Value::String(CLOSED_TOOL_SET_NOTE.to_string())),
        tool_call_id: None,
        name: None,
        reasoning_content: None,
        thinking: None,
        tool_calls: None,
    }
}

/// Shape an internally-appended tail message through the profile role policy
/// (always inline — never the conversation-leading message) and push it, so
/// gateway-injected history (the repair note, synthetic/real tool results, the
/// internal assistant turn) honors the SAME role mapping the lowering pass
/// applied to the request tail. `Action::Drop` skips the message. This keeps
/// injected roles consistent with the request for arbitrary role policies (e.g.
/// a `tool`->`user` rewrite), not just always-permitted roles; the pre-send
/// `merge_adjacent_if_configured` pass then folds any adjacent same-role run.
fn push_shaped(
    messages: &mut Vec<ChatMessage>,
    message: ChatMessage,
    roles: Option<&crate::config::RolesConfig>,
) -> AppResult<()> {
    if let Some(shaped) = shape_tail_message(message, roles)? {
        messages.push(shaped);
    }
    Ok(())
}

fn relax_tool_choice_after_stripping_tool(
    tool_choice: &mut Value,
    stripped_name: &str,
    no_tools_remaining: bool,
) {
    match tool_choice {
        Value::String(choice) if choice == "required" && no_tools_remaining => {
            *tool_choice = Value::String("auto".to_string());
        }
        Value::Object(map)
            if map.get("type").and_then(Value::as_str) == Some("function")
                && map
                    .get("function")
                    .and_then(Value::as_object)
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str)
                    == Some(stripped_name) =>
        {
            *tool_choice = Value::String("auto".to_string());
        }
        _ => {}
    }
}

fn preview_json<T>(value: &T) -> String
where
    T: Serialize,
{
    preview_json_limited(value, 4_000)
}

fn preview_json_limited<T>(value: &T, limit: usize) -> String
where
    T: Serialize,
{
    preview_json_limited_with_images(value, limit).text
}

#[derive(Debug)]
struct JsonPreview {
    text: String,
    images: Vec<DebugEventImage>,
}

fn preview_json_limited_with_images<T>(value: &T, limit: usize) -> JsonPreview
where
    T: Serialize,
{
    let mut images = Vec::new();
    let rendered = match serde_json::to_value(value) {
        Ok(mut value) => {
            // First collect image METADATA cards (mime/size/path) for the debug
            // UI — without the raw bytes. Then redact ALL image URIs (data: and
            // raw/escaped http(s), case-insensitive) in the preview TEXT via the
            // shared redactor, so the broadcast preview never carries image
            // content (G4 round-4 #4 — the weaker bespoke redactor missed
            // remote/signed URLs and uppercase DATA:).
            collect_data_image_cards(&value, "$", &mut images);
            crate::redaction::redact_image_uris_in_value(&mut value);
            serde_json::to_string_pretty(&value)
        }
        Err(err) => Err(err),
    }
    .unwrap_or_else(|err| format!("{{\"serialization_error\":\"{err}\"}}"));
    if rendered.chars().count() <= limit {
        JsonPreview {
            text: rendered,
            images,
        }
    } else {
        let end = rendered
            .char_indices()
            .nth(limit)
            .map(|(index, _)| index)
            .unwrap_or(rendered.len());
        JsonPreview {
            text: format!("{}...\n[truncated]", &rendered[..end]),
            images,
        }
    }
}

/// Collect debug-UI image metadata cards (mime/size/path) from a JSON value,
/// WITHOUT copying the raw image bytes/URL (G4 round-4 #4). Read-only: the
/// preview text redaction happens separately via `redact_image_uris_in_value`.
fn collect_data_image_cards(value: &Value, path: &str, images: &mut Vec<DebugEventImage>) {
    match value {
        Value::String(text) => {
            if let Some(image) = extract_data_image(text, path, images.len() + 1) {
                images.push(image);
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                collect_data_image_cards(item, &format!("{path}[{index}]"), images);
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter() {
                collect_data_image_cards(item, &json_path_child(path, key), images);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn extract_data_image(value: &str, path: &str, index: usize) -> Option<DebugEventImage> {
    // Case-insensitive `data:image/` prefix (the previous version missed
    // uppercase `DATA:`). The card carries only descriptors — never the bytes.
    // UTF-8-SAFE prefix check (round-5): `value` is untrusted request/response
    // JSON, so a byte slice (`value[..11]`) could land mid-codepoint and panic;
    // `as_bytes().get(..)` never panics and the prefix is pure ASCII.
    if !value
        .as_bytes()
        .get(.."data:image/".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"data:image/"))
    {
        return None;
    }
    let comma_index = value.find(',')?;
    let header = &value["data:".len()..comma_index];
    if !header
        .split(';')
        .any(|part| part.eq_ignore_ascii_case("base64"))
    {
        return None;
    }
    let mime_type = header
        .split(';')
        .next()
        .filter(|part| part.to_ascii_lowercase().starts_with("image/"))?
        .to_string();
    Some(DebugEventImage {
        id: format!("image-{index}"),
        label: format!("image {index}"),
        path: path.to_string(),
        mime_type,
        size_bytes: estimate_base64_payload_bytes(&value[comma_index + 1..]),
    })
}

fn estimate_base64_payload_bytes(encoded: &str) -> Option<usize> {
    let base64_len = encoded.chars().filter(|ch| !ch.is_whitespace()).count();
    if base64_len == 0 {
        return Some(0);
    }
    let padding = encoded
        .chars()
        .rev()
        .filter(|ch| !ch.is_whitespace())
        .take_while(|ch| *ch == '=')
        .count()
        .min(2);
    Some((base64_len.saturating_mul(3) / 4).saturating_sub(padding))
}

fn json_path_child(parent: &str, key: &str) -> String {
    if key
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        format!("{parent}.{key}")
    } else {
        format!(
            "{parent}[{}]",
            serde_json::to_string(key).unwrap_or_default()
        )
    }
}

fn summarize_response_item(item: &ResponseItem) -> String {
    match item {
        ResponseItem::Message { role, content, .. } => {
            format!("{role}: {}", summarize_content(content))
        }
        ResponseItem::Reasoning { content, .. } => content
            .as_ref()
            .and_then(|items| items.first())
            .map(|item| match item {
                crate::models::responses::ReasoningContentItem::ReasoningText { text }
                | crate::models::responses::ReasoningContentItem::Text { text } => {
                    format!("reasoning: {}", preview_text(text))
                }
            })
            .unwrap_or_else(|| "reasoning".to_string()),
        ResponseItem::FunctionCall {
            name, arguments, ..
        } => {
            format!("function_call {name} {}", preview_text(arguments))
        }
        ResponseItem::FunctionCallOutput { call_id, output } => {
            format!(
                "function_call_output {call_id} {}",
                preview_text(&output.to_string())
            )
        }
        ResponseItem::CustomToolCall { name, input, .. } => {
            format!("custom_tool_call {name} {}", preview_text(input))
        }
        ResponseItem::CustomToolCallOutput {
            call_id, output, ..
        } => {
            format!(
                "custom_tool_call_output {call_id} {}",
                preview_text(&output.to_string())
            )
        }
        ResponseItem::ToolSearchCall { arguments, .. } => {
            format!("tool_search_call {}", preview_text(&arguments.to_string()))
        }
        ResponseItem::ToolSearchOutput { tools, .. } => {
            format!("tool_search_output {} tools", tools.len())
        }
        ResponseItem::LocalShellCall { action, .. } => match action {
            crate::models::responses::LocalShellAction::Exec(exec) => {
                format!("local_shell {}", exec.command.join(" "))
            }
        },
        ResponseItem::WebSearchCall { action, .. } => match action {
            Some(crate::models::responses::WebSearchAction::Search { query, .. }) => {
                format!("web_search {}", query.clone().unwrap_or_default())
            }
            Some(_) => "web_search".to_string(),
            None => "web_search in_progress".to_string(),
        },
        ResponseItem::ImageGenerationCall { id, .. } => format!("image_generation_call {id}"),
    }
}

fn summarize_content(content: &[crate::models::responses::ContentItem]) -> String {
    let mut text = String::new();
    for item in content {
        match item {
            crate::models::responses::ContentItem::InputText { text: item_text }
            | crate::models::responses::ContentItem::OutputText { text: item_text } => {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str(item_text);
            }
            crate::models::responses::ContentItem::InputImage { .. } => {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str("[image]");
            }
            crate::models::responses::ContentItem::InputFile { .. } => {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str("[file]");
            }
            crate::models::responses::ContentItem::Other(_) => {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str("[input]");
            }
        }
    }
    preview_text(&text)
}

fn trailing_tool_output_items(input: &[ResponseItem]) -> Vec<&ResponseItem> {
    let mut items = input
        .iter()
        .rev()
        .take_while(|item| is_tool_output_item(item))
        .collect::<Vec<_>>();
    items.reverse();
    items
}

fn is_tool_output_item(item: &ResponseItem) -> bool {
    matches!(
        item,
        ResponseItem::FunctionCallOutput { .. }
            | ResponseItem::CustomToolCallOutput { .. }
            | ResponseItem::ToolSearchOutput { .. }
    )
}

fn preview_text(text: &str) -> String {
    const LIMIT: usize = 1024;
    if text.chars().count() <= LIMIT {
        text.to_string()
    } else {
        let end = text
            .char_indices()
            .nth(LIMIT)
            .map(|(index, _)| index)
            .unwrap_or(text.len());
        format!("{}...", &text[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::extract_data_image;
    use super::preview_json;
    use super::preview_json_limited_with_images;
    use super::preview_text;
    use super::trailing_tool_output_items;
    use crate::models::responses::ResponseItem;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn preview_text_truncates_on_char_boundary() {
        let text = format!("{}é", "a".repeat(1023));
        assert_eq!(preview_text(&text), format!("{}é", "a".repeat(1023)));

        let text = format!("{}éβ", "a".repeat(1023));
        assert_eq!(preview_text(&text), format!("{}é...", "a".repeat(1023)));
    }

    #[test]
    fn preview_json_truncates_on_char_boundary() {
        let value = json!({ "text": format!("{}éβ", "a".repeat(4_100)) });
        let preview = preview_json(&value);
        assert!(preview.ends_with("...\n[truncated]"));
        assert!(preview.is_char_boundary(preview.len()));
    }

    #[test]
    fn preview_json_redacts_data_image_urls_and_collects_images() {
        let data_url = "data:image/jpeg;base64,/9j/AA==";
        let value = json!({
            "type": "input_image",
            "image_url": data_url,
            "text": "keep me visible"
        });

        let preview = preview_json_limited_with_images(&value, 4_000);

        // Non-image text survives; the data URL (incl. payload) is fully redacted
        // via the shared redactor and the raw bytes never appear in the preview.
        assert!(preview.text.contains("keep me visible"));
        assert!(preview.text.contains("<redacted uri>"));
        assert!(!preview.text.contains("/9j/AA=="));
        // An image metadata card is still surfaced for the UI, but with NO raw
        // `src` (round-4 #4): only mime/size/path descriptors.
        assert_eq!(preview.images.len(), 1);
        assert_eq!(preview.images[0].mime_type, "image/jpeg");
        assert_eq!(preview.images[0].path, "$.image_url");
    }

    #[test]
    fn preview_json_redacts_remote_and_uppercase_image_urls() {
        // Round-4 #4: the monitor preview must also redact remote/signed image
        // URLs and uppercase DATA: that the previous bespoke redactor missed.
        let value = json!({
            "image_url": "https://signed.example.com/i.png?sig=PREVIEWSECRET",
            "other": "DATA:IMAGE/PNG;BASE64,UPPERLEAK",
            "keep": "ordinary text"
        });
        let preview = preview_json_limited_with_images(&value, 4_000);
        assert!(
            !preview.text.contains("PREVIEWSECRET"),
            "signed-url token redacted"
        );
        assert!(
            !preview.text.contains("signed.example.com"),
            "remote host redacted"
        );
        assert!(
            !preview.text.contains("UPPERLEAK"),
            "uppercase data: redacted"
        );
        assert!(preview.text.contains("ordinary text"));
    }

    #[test]
    fn preview_handles_multibyte_strings_straddling_data_image_prefix() {
        // Round-5: `extract_data_image` walks UNTRUSTED request/response JSON. A
        // non-ASCII string whose byte at the `data:image/` prefix boundary
        // (index 11) is mid-codepoint must NOT panic (the old byte slice did).
        // `"data:imageé..."`: `data:image` is 10 bytes, `é` is 2 bytes, so byte
        // 11 is the SECOND byte of `é` — not a char boundary.
        let straddling = "data:imageé;base64,SHOULDNOTMATCH";
        assert!(
            !straddling.is_char_boundary(11),
            "test premise: byte 11 mid-char"
        );
        // Direct call: must return None (prefix is `data:imageé`, not
        // `data:image/`) without panicking.
        assert!(extract_data_image(straddling, "$", 1).is_none());

        // A short multibyte string (< 11 bytes) must also not panic.
        assert!(extract_data_image("dáta", "$", 1).is_none());

        // Through the full preview path (redaction + card collection) with a
        // multibyte value at an image-bearing key: must not panic.
        let value = json!({
            "image_url": straddling,
            "note": "café ☕ data:imagé/png oops",
            "keep": "ünïcödé text"
        });
        let preview = preview_json_limited_with_images(&value, 4_000);
        assert!(preview.text.contains("ünïcödé text"));
        // No valid data:image/ match here, so no card; the point is no panic.
        assert!(preview.images.is_empty());

        // A VALID data:image/ URL with multibyte content after the comma is
        // matched, redacted in the text, carded, and never panics.
        let value2 = json!({
            "image_url": "data:image/png;base64,QUJDé/+=",
            "tag": "déjà vu"
        });
        let preview2 = preview_json_limited_with_images(&value2, 4_000);
        assert_eq!(preview2.images.len(), 1);
        assert_eq!(preview2.images[0].mime_type, "image/png");
        assert!(preview2.text.contains("<redacted uri>"));
        assert!(preview2.text.contains("déjà vu"));
        assert!(!preview2.text.contains("QUJD"));
    }

    #[test]
    fn trailing_tool_output_items_returns_only_tail_outputs() {
        let input = vec![
            ResponseItem::FunctionCallOutput {
                call_id: "old".to_string(),
                output: json!("old"),
            },
            ResponseItem::message_text("assistant", "done"),
            ResponseItem::FunctionCallOutput {
                call_id: "fn".to_string(),
                output: json!("fn out"),
            },
            ResponseItem::CustomToolCallOutput {
                call_id: "custom".to_string(),
                name: Some("tool".to_string()),
                output: json!("custom out"),
            },
            ResponseItem::ToolSearchOutput {
                call_id: Some("search".to_string()),
                status: "completed".to_string(),
                execution: "search".to_string(),
                tools: vec![json!({ "name": "tool" })],
            },
        ];

        let result = trailing_tool_output_items(&input);
        assert_eq!(result.len(), 3);
        assert!(matches!(
            result[0],
            ResponseItem::FunctionCallOutput { call_id, .. } if call_id == "fn"
        ));
        assert!(matches!(
            result[1],
            ResponseItem::CustomToolCallOutput { call_id, .. } if call_id == "custom"
        ));
        assert!(matches!(
            result[2],
            ResponseItem::ToolSearchOutput {
                call_id: Some(call_id),
                ..
            } if call_id == "search"
        ));
    }

    use super::AccumulatedUsage;
    use super::failure_event;
    use crate::models::chat::ChunkUsage;

    #[test]
    fn accumulated_usage_cached_tokens() {
        let mut usage = AccumulatedUsage::default();
        usage.add(ChunkUsage {
            prompt_tokens: 100,
            completion_tokens: 25,
            total_tokens: 125,
            reasoning_tokens: None,
            prompt_tokens_details: Some(crate::models::chat::PromptTokensDetails {
                cached_tokens: 50,
            }),
            completion_tokens_details: None,
        });
        let result = usage.into_response_usage().unwrap();
        assert_eq!(result.input_tokens, 100);
        assert_eq!(result.input_tokens_details.unwrap().cached_tokens, 50);
    }

    #[test]
    fn accumulated_usage_reasoning_tokens() {
        let mut usage = AccumulatedUsage::default();
        usage.add(ChunkUsage {
            prompt_tokens: 100,
            completion_tokens: 25,
            total_tokens: 125,
            reasoning_tokens: None,
            prompt_tokens_details: None,
            completion_tokens_details: Some(crate::models::chat::CompletionTokensDetails {
                reasoning_tokens: 30,
            }),
        });
        let result = usage.into_response_usage().unwrap();
        assert_eq!(result.output_tokens, 25);
        assert_eq!(result.output_tokens_details.unwrap().reasoning_tokens, 30);
    }

    #[test]
    fn accumulated_usage_top_level_reasoning_tokens() {
        let mut usage = AccumulatedUsage::default();
        usage.add(ChunkUsage {
            prompt_tokens: 100,
            completion_tokens: 25,
            total_tokens: 125,
            reasoning_tokens: Some(30),
            prompt_tokens_details: None,
            completion_tokens_details: None,
        });
        let result = usage.into_response_usage().unwrap();
        assert_eq!(result.output_tokens, 25);
        assert_eq!(result.output_tokens_details.unwrap().reasoning_tokens, 30);
    }

    #[test]
    fn accumulated_usage_prefers_nested_reasoning_tokens() {
        let mut usage = AccumulatedUsage::default();
        usage.add(ChunkUsage {
            prompt_tokens: 100,
            completion_tokens: 25,
            total_tokens: 125,
            reasoning_tokens: Some(10),
            prompt_tokens_details: None,
            completion_tokens_details: Some(crate::models::chat::CompletionTokensDetails {
                reasoning_tokens: 30,
            }),
        });
        let result = usage.into_response_usage().unwrap();
        assert_eq!(result.output_tokens_details.unwrap().reasoning_tokens, 30);
    }

    #[test]
    fn accumulated_usage_zero_returns_none() {
        let usage = AccumulatedUsage::default();
        assert!(usage.into_response_usage().is_none());
    }

    use super::extract_web_search_query;
    use crate::models::responses::WebSearchAction;

    #[test]
    fn test_run_web_search_rejects_open_page() {
        let action = Some(WebSearchAction::OpenPage {
            url: Some("https://example.com".to_string()),
        });
        let args = json!({"query": "test"});
        let result = extract_web_search_query(&action, &args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported web_search action")
        );
    }

    #[test]
    fn test_run_web_search_rejects_find_in_page() {
        let action = Some(WebSearchAction::FindInPage {
            url: Some("https://example.com".to_string()),
            pattern: Some("test".to_string()),
        });
        let args = json!({"query": "test"});
        let result = extract_web_search_query(&action, &args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported web_search action")
        );
    }

    #[test]
    fn test_run_web_search_rejects_other_action() {
        let action = Some(WebSearchAction::Other);
        let args = json!({"query": "test"});
        let result = extract_web_search_query(&action, &args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported web_search action")
        );
    }

    #[test]
    fn test_extract_web_search_query_from_action() {
        let action = Some(WebSearchAction::Search {
            query: Some("rust async".to_string()),
            queries: None,
        });
        let args = json!({});
        let result = extract_web_search_query(&action, &args).unwrap();
        assert_eq!(result, "rust async");
    }

    #[test]
    fn test_extract_web_search_query_fallback_to_arguments() {
        let action = None;
        let args = json!({"query": "fallback query"});
        let result = extract_web_search_query(&action, &args).unwrap();
        assert_eq!(result, "fallback query");
    }

    #[test]
    fn test_extract_web_search_query_search_action_none_query_falls_back() {
        let action = Some(WebSearchAction::Search {
            query: None,
            queries: None,
        });
        let args = json!({"query": "from args"});
        let result = extract_web_search_query(&action, &args).unwrap();
        assert_eq!(result, "from args");
    }

    #[test]
    fn test_max_web_search_rounds_default() {
        let config =
            crate::config::Config::from_persisted(&crate::config::PersistedConfig::default())
                .unwrap();
        assert_eq!(config.max_web_search_rounds, 5);
    }

    #[test]
    fn failure_event_shape() {
        let error = crate::error::AppError::internal("test error");
        let event = failure_event(&error);
        assert_eq!(event.event, "response.failed");
        assert_eq!(event.data["type"], "response.failed");
        assert_eq!(event.data["response"]["error"]["code"], "gateway_error");
        assert_eq!(
            event.data["response"]["error"]["message"].as_str().unwrap(),
            "internal server error"
        );
    }

    // G2 model-family detection + `chat_template_kwargs` injection now lives in
    // the upstream client (it must run against the FINAL per-provider model,
    // which routing/failover only know there). See `src/upstream.rs` tests.
}

fn extract_web_search_query(
    action: &Option<WebSearchAction>,
    arguments: &Value,
) -> AppResult<String> {
    match action {
        Some(WebSearchAction::Search { query, .. }) => {
            if let Some(q) = query {
                Ok(q.clone())
            } else {
                arguments
                    .get("query")
                    .and_then(Value::as_str)
                    .map(String::from)
                    .ok_or_else(|| AppError::upstream("web_search call missing query"))
            }
        }
        Some(WebSearchAction::OpenPage { .. })
        | Some(WebSearchAction::FindInPage { .. })
        | Some(WebSearchAction::Other) => Err(AppError::upstream("unsupported web_search action")),
        None => arguments
            .get("query")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| AppError::upstream("web_search call missing query")),
    }
}

fn created_event(response_id: &str, estimated_input_tokens: i64) -> SseEvent {
    json_event(
        "response.created",
        ResponsesEnvelope {
            kind: "response.created".to_string(),
            payload: ResponseCreatedPayload {
                response: ResponseStub {
                    id: response_id.to_string(),
                    // C3: ride the early G3 estimate onto `response.created` so the
                    // Anthropic streaming converter can seed `message_start` with a
                    // non-zero `input_tokens` instead of a hardcoded `0` -- the real
                    // upstream tokenizer count arrives LATE (`response.completed`'s
                    // `usage`, after `message_start` has already gone out) and
                    // always overrides this estimate at the terminal event. `try_into`
                    // never actually fails (`estimate_input_tokens` is a non-negative
                    // byte count) but stays total rather than panicking.
                    estimated_input_tokens: estimated_input_tokens.try_into().ok(),
                },
            },
        },
    )
}

fn completed_event(response: ResponseResource) -> SseEvent {
    json_event(
        "response.completed",
        ResponsesEnvelope {
            kind: "response.completed".to_string(),
            payload: ResponseCompletedPayload { response },
        },
    )
}

fn incomplete_event(response: ResponseResource) -> SseEvent {
    json_event(
        "response.incomplete",
        ResponsesEnvelope {
            kind: "response.incomplete".to_string(),
            payload: ResponseCompletedPayload { response },
        },
    )
}

fn content_part_added_event(item_id: String, output_index: usize) -> SseEvent {
    json_event(
        "response.content_part.added",
        ResponsesEnvelope {
            kind: "response.content_part.added".to_string(),
            payload: crate::models::responses::ContentPartPayload {
                item_id,
                output_index,
                content_index: 0,
                part: crate::models::responses::ContentPartRef {
                    kind: "output_text".to_string(),
                    text: String::new(),
                    annotations: Vec::new(),
                },
            },
        },
    )
}

fn content_part_done_event(item_id: String, output_index: usize, text: String) -> SseEvent {
    json_event(
        "response.content_part.done",
        ResponsesEnvelope {
            kind: "response.content_part.done".to_string(),
            payload: crate::models::responses::ContentPartPayload {
                item_id,
                output_index,
                content_index: 0,
                part: crate::models::responses::ContentPartRef {
                    kind: "output_text".to_string(),
                    text,
                    annotations: Vec::new(),
                },
            },
        },
    )
}

fn reasoning_summary_part_added_event(item_id: String, output_index: usize) -> SseEvent {
    json_event(
        "response.reasoning_summary_part.added",
        ResponsesEnvelope {
            kind: "response.reasoning_summary_part.added".to_string(),
            payload: crate::models::responses::ReasoningSummaryPartPayload {
                item_id,
                output_index,
                summary_index: 0,
                part: crate::models::responses::ReasoningSummaryPartRef {
                    kind: "summary_text".to_string(),
                    text: String::new(),
                },
            },
        },
    )
}

fn reasoning_summary_part_done_event(
    item_id: String,
    output_index: usize,
    text: String,
) -> SseEvent {
    json_event(
        "response.reasoning_summary_part.done",
        ResponsesEnvelope {
            kind: "response.reasoning_summary_part.done".to_string(),
            payload: crate::models::responses::ReasoningSummaryPartPayload {
                item_id,
                output_index,
                summary_index: 0,
                part: crate::models::responses::ReasoningSummaryPartRef {
                    kind: "summary_text".to_string(),
                    text,
                },
            },
        },
    )
}

fn refusal_delta_event(delta: String) -> SseEvent {
    json_event(
        "response.refusal.delta",
        ResponsesEnvelope {
            kind: "response.refusal.delta".to_string(),
            payload: crate::models::responses::RefusalDeltaPayload { delta },
        },
    )
}

fn refusal_done_event(refusal: String) -> SseEvent {
    json_event(
        "response.refusal.done",
        ResponsesEnvelope {
            kind: "response.refusal.done".to_string(),
            payload: crate::models::responses::RefusalDonePayload { refusal },
        },
    )
}

#[derive(Debug, Clone)]
struct OutputTarget {
    item_id: String,
    output_index: usize,
}

#[derive(Default)]
struct ResponseEventState {
    next_output_index: usize,
    output_indices: HashMap<String, usize>,
    active_message: Option<OutputTarget>,
    active_reasoning: Option<OutputTarget>,
}

impl ResponseEventState {
    fn register_item(&mut self, item: &ResponseItem) -> OutputTarget {
        let item_id = response_item_event_id(item)
            .unwrap_or_else(|| format!("item_{}", self.next_output_index));
        let output_index = match self.output_indices.get(&item_id) {
            Some(index) => *index,
            None => {
                let index = self.next_output_index;
                self.next_output_index += 1;
                self.output_indices.insert(item_id.clone(), index);
                index
            }
        };
        let target = OutputTarget {
            item_id,
            output_index,
        };
        match item {
            ResponseItem::Message { .. } => self.active_message = Some(target.clone()),
            ResponseItem::Reasoning { .. } => self.active_reasoning = Some(target.clone()),
            _ => {}
        }
        target
    }

    fn target_for_item(&mut self, item: &ResponseItem) -> OutputTarget {
        self.register_item(item)
    }

    fn active_message_target(&self) -> AppResult<OutputTarget> {
        self.active_message
            .clone()
            .ok_or_else(|| AppError::internal("missing active message output item"))
    }

    fn active_reasoning_target(&self) -> AppResult<OutputTarget> {
        self.active_reasoning
            .clone()
            .ok_or_else(|| AppError::internal("missing active reasoning output item"))
    }
}

fn response_item_event_id(item: &ResponseItem) -> Option<String> {
    match item {
        ResponseItem::Message { id, .. } => id.clone(),
        ResponseItem::Reasoning { id, .. } => Some(id.clone()),
        ResponseItem::FunctionCall { id, call_id, .. } => {
            id.clone().or_else(|| Some(call_id.clone()))
        }
        ResponseItem::FunctionCallOutput { call_id, .. } => Some(call_id.clone()),
        ResponseItem::CustomToolCall { call_id, .. } => Some(call_id.clone()),
        ResponseItem::CustomToolCallOutput { call_id, .. } => Some(call_id.clone()),
        ResponseItem::ToolSearchCall { call_id, .. } => call_id.clone(),
        ResponseItem::ToolSearchOutput { call_id, .. } => call_id.clone(),
        ResponseItem::LocalShellCall { id, call_id, .. } => id.clone().or_else(|| call_id.clone()),
        ResponseItem::WebSearchCall { id, .. } => id.clone(),
        ResponseItem::ImageGenerationCall { id, .. } => Some(id.clone()),
    }
}

#[derive(Default)]
struct AccumulatedUsage {
    input_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
    /// Cache-read prompt tokens, or `None` until a chunk REPORTS a cached breakdown
    /// (gap 07): an upstream that never sends `prompt_tokens_details` leaves this
    /// `None` (UNAVAILABLE), distinct from a reported `Some(0)`. Once any chunk
    /// reports it, subsequent chunks accumulate into the `Some`.
    cached_input_tokens: Option<i64>,
    /// Reasoning tokens, or `None` until a chunk reports reasoning details / a
    /// top-level `reasoning_tokens` (gap 07) — UNAVAILABLE vs a reported `Some(0)`.
    reasoning_output_tokens: Option<i64>,
}

/// Add an OPTIONAL reported sub-count into an accumulator slot that starts `None`
/// (gap 07): `None + None = None` (still unreported), but ANY reported chunk promotes
/// the slot to `Some` and sums subsequent reports. So the flow's cached/reasoning is
/// UNAVAILABLE only when NO chunk ever reported it; a single reported `0` makes it a
/// measured `Some(0)`, never collapsing back to unreported.
fn accumulate_optional(slot: &mut Option<i64>, reported: Option<i64>) {
    if let Some(value) = reported {
        *slot = Some(slot.unwrap_or(0) + value);
    }
}

impl AccumulatedUsage {
    fn add(&mut self, usage: ChunkUsage) {
        self.input_tokens += usage.prompt_tokens;
        self.output_tokens += usage.completion_tokens;
        self.total_tokens += usage.total_tokens;
        accumulate_optional(
            &mut self.cached_input_tokens,
            usage.prompt_tokens_details.map(|d| d.cached_tokens),
        );
        let reasoning_tokens = usage
            .completion_tokens_details
            .map(|d| d.reasoning_tokens)
            .or(usage.reasoning_tokens);
        accumulate_optional(&mut self.reasoning_output_tokens, reasoning_tokens);
    }

    /// D3: the running cumulative-so-far as a dashboard [`FlowUsage`] — the
    /// `turn_base` captured at the START of each turn. Adding a within-turn chunk's
    /// (already-cumulative) usage to this base gives the flow's CURRENT total
    /// without double-counting prior turns. The OPTIONAL `cached`/`reasoning` carry
    /// the gap-07 UNAVAILABLE-vs-reported distinction straight through.
    fn snapshot(&self) -> crate::dashboard_flow::FlowUsage {
        crate::dashboard_flow::FlowUsage {
            prompt: self.input_tokens,
            completion: self.output_tokens,
            total: self.total_tokens,
            cached: self.cached_input_tokens,
            reasoning: self.reasoning_output_tokens,
        }
    }

    fn into_response_usage(self) -> Option<ResponseUsage> {
        if self.total_tokens == 0 {
            return None;
        }
        Some(ResponseUsage {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            total_tokens: self.total_tokens,
            // The CLIENT-facing canonical `ResponseUsage` keeps its integer-only shape
            // (gap 07's UNAVAILABLE distinction is a dashboard concern, not a client
            // contract change): an unreported cached/reasoning class projects to `0` here.
            input_tokens_details: Some(ResponseInputTokensDetails {
                cached_tokens: self.cached_input_tokens.unwrap_or(0),
            }),
            output_tokens_details: Some(ResponseOutputTokensDetails {
                reasoning_tokens: self.reasoning_output_tokens.unwrap_or(0),
            }),
        })
    }
}

/// D3: combine the turn's CUMULATIVE base (`accumulated_usage.snapshot()` captured
/// at turn start) with a within-turn usage chunk to get the flow's CURRENT total.
/// OpenAI usage chunks are cumulative WITHIN a turn, so we add the chunk to the base
/// of PRIOR turns — never accumulate chunk-over-chunk (that double-counts). Maps the
/// `ChunkUsage` fields exactly as [`AccumulatedUsage::add`] does (nested cached /
/// reasoning details, with the top-level `reasoning_tokens` fallback).
fn flow_usage_from_base_and_chunk(
    base: crate::dashboard_flow::FlowUsage,
    chunk: &ChunkUsage,
) -> crate::dashboard_flow::FlowUsage {
    // Gap 07: a chunk reports cached/reasoning only when the upstream sent the detail
    // block. `None` here means UNAVAILABLE for THIS chunk — it folds into the base via
    // `accumulate_optional`, so the running total stays `None` until SOME chunk reports
    // the class (then it is a measured `Some`, incl. a reported `0`).
    let cached = chunk
        .prompt_tokens_details
        .as_ref()
        .map(|d| d.cached_tokens);
    let reasoning = chunk
        .completion_tokens_details
        .as_ref()
        .map(|d| d.reasoning_tokens)
        .or(chunk.reasoning_tokens);
    let mut cached_total = base.cached;
    accumulate_optional(&mut cached_total, cached);
    let mut reasoning_total = base.reasoning;
    accumulate_optional(&mut reasoning_total, reasoning);
    crate::dashboard_flow::FlowUsage {
        prompt: base.prompt + chunk.prompt_tokens,
        completion: base.completion + chunk.completion_tokens,
        total: base.total + chunk.total_tokens,
        cached: cached_total,
        reasoning: reasoning_total,
    }
}

fn output_item_added_event(item: ResponseItem, output_index: usize) -> SseEvent {
    json_event(
        "response.output_item.added",
        ResponsesEnvelope {
            kind: "response.output_item.added".to_string(),
            payload: OutputItemPayload { output_index, item },
        },
    )
}

fn output_item_done_event(item: ResponseItem, output_index: usize) -> SseEvent {
    json_event(
        "response.output_item.done",
        ResponsesEnvelope {
            kind: "response.output_item.done".to_string(),
            payload: OutputItemPayload { output_index, item },
        },
    )
}

fn output_text_delta_event(item_id: String, output_index: usize, delta: String) -> SseEvent {
    json_event(
        "response.output_text.delta",
        ResponsesEnvelope {
            kind: "response.output_text.delta".to_string(),
            payload: DeltaPayload {
                item_id,
                output_index,
                content_index: 0,
                delta,
            },
        },
    )
}

fn reasoning_text_delta_event(item_id: String, output_index: usize, delta: String) -> SseEvent {
    json_event(
        "response.reasoning_summary_text.delta",
        ResponsesEnvelope {
            kind: "response.reasoning_summary_text.delta".to_string(),
            payload: ReasoningDeltaPayload {
                item_id,
                output_index,
                summary_index: 0,
                delta,
            },
        },
    )
}

fn reasoning_signature_delta_event(
    item_id: String,
    output_index: usize,
    signature: String,
) -> SseEvent {
    json_event(
        "response.reasoning_summary_text.signature_delta",
        ResponsesEnvelope {
            kind: "response.reasoning_summary_text.signature_delta".to_string(),
            payload: ReasoningSignatureDeltaPayload {
                item_id,
                output_index,
                summary_index: 0,
                signature,
            },
        },
    )
}

fn failure_event(error: &AppError) -> SseEvent {
    json_event(
        "response.failed",
        ResponsesEnvelope {
            kind: "response.failed".to_string(),
            payload: FailedPayload {
                response: FailedResponse {
                    error: FailedError {
                        // E1: surface a structured machine code when the error
                        // carries one (e.g. `invalid_tool_call` for an exhausted
                        // unknown-tool repair); otherwise the historical default.
                        code: error
                            .code
                            .clone()
                            .unwrap_or_else(|| "gateway_error".to_string()),
                        message: error.client_message.clone(),
                    },
                },
            },
        },
    )
}

fn in_progress_event(response_id: &str) -> SseEvent {
    json_event(
        "response.in_progress",
        ResponsesEnvelope {
            kind: "response.in_progress".to_string(),
            payload: ResponseCreatedPayload {
                response: ResponseStub {
                    id: response_id.to_string(),
                    // Not carried on `response.in_progress` -- the Anthropic converter
                    // only reads this field off `response.created` (`handle_created`).
                    estimated_input_tokens: None,
                },
            },
        },
    )
}

fn output_text_done_event(item_id: String, output_index: usize, text: String) -> SseEvent {
    json_event(
        "response.output_text.done",
        ResponsesEnvelope {
            kind: "response.output_text.done".to_string(),
            payload: crate::models::responses::TextDonePayload {
                item_id,
                output_index,
                content_index: 0,
                text,
            },
        },
    )
}

fn function_call_args_delta_event(
    call_id: String,
    name: Option<String>,
    delta: String,
) -> SseEvent {
    json_event(
        "response.function_call_arguments.delta",
        ResponsesEnvelope {
            kind: "response.function_call_arguments.delta".to_string(),
            payload: crate::models::responses::FunctionCallArgsDeltaPayload {
                call_id,
                name,
                delta,
            },
        },
    )
}

fn function_call_args_done_event(call_id: String, name: String, arguments: String) -> SseEvent {
    json_event(
        "response.function_call_arguments.done",
        ResponsesEnvelope {
            kind: "response.function_call_arguments.done".to_string(),
            payload: crate::models::responses::FunctionCallArgsDonePayload {
                call_id,
                name,
                arguments,
            },
        },
    )
}

fn json_event<T>(event: &str, payload: T) -> SseEvent
where
    T: Serialize,
{
    SseEvent {
        event: event.to_string(),
        data: serde_json::to_value(payload).unwrap_or(Value::Null),
    }
}
