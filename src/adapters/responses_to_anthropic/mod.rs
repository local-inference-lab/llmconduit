use crate::engine::SseEvent;
use crate::models::anthropic::AnthropicContentBlockStart;
use crate::models::anthropic::AnthropicDelta;
use crate::models::anthropic::AnthropicErrorBody;
use crate::models::anthropic::AnthropicMessageDeltaBody;
use crate::models::anthropic::AnthropicMessageStart;
use crate::models::anthropic::AnthropicMessageUsage;
use crate::models::anthropic::AnthropicOutputTokensDetails;
use crate::models::anthropic::AnthropicServerToolUse;
use crate::models::anthropic::AnthropicStreamEvent;
use crate::models::anthropic::AnthropicUsage;
use crate::models::anthropic::SYNTHETIC_SIGNATURE_PREFIX;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashSet;
use uuid::Uuid;

mod reasoning;

mod collector;

pub mod conformance;

#[cfg(test)]
mod tests;

pub use collector::AnthropicStreamCollector;

use reasoning::ReasoningEgressState;

const ESTIMATED_OUTPUT_TOKEN_BYTES: usize = 4;

/// Name of the server-side web-search tool. Brave runs server-side, so the
/// model's own `web_search` call is NOT surfaced to the Anthropic client as a
/// regular `tool_use` block -- the search is rendered via the additive
/// `response.web_search_results` event (`server_tool_use` +
/// `web_search_tool_result`). The streamed `function_call_arguments` for this
/// tool are therefore swallowed: they must not open a client tool_use block,
/// must not flip `has_tool_calls` (the turn ends `end_turn`, not `tool_use`),
/// and must not trip the late-reasoning drop gate (`content_started`) -- a
/// web-search turn legitimately continues with reasoning after the results.
const WEB_SEARCH_TOOL_NAME: &str = "web_search";

/// Whether a tool name is a server-side tool the converter must swallow from the
/// canonical Responses stream so it never becomes an Anthropic `tool_use` block.
/// This is ONLY Brave `web_search`: the engine still streams its
/// `function_call_arguments` (the search is surfaced separately via
/// `response.web_search_results`), so the converter has to drop them, and the
/// turn ends `end_turn` so post-search reasoning is not gated as "late".
///
/// NOTE (round-7 #2): `analyzeImage` is deliberately NOT hidden by name here. On
/// an active image-agent turn the engine classifies it as the server-side
/// ImageAnalysis tool and NEVER emits its delta/done/item events into the stream
/// at all, so the converter never sees them. On an INACTIVE turn `analyzeImage`
/// is a legitimate CLIENT tool that must surface normally — name-hiding it would
/// wrongly swallow the client's tool.
fn is_hidden_server_tool(name: &str) -> bool {
    name == WEB_SEARCH_TOOL_NAME
}

enum ContentBlockState {
    Text { index: usize },
    ToolUse { index: usize, call_id: String },
}

pub struct AnthropicStreamConverter {
    model: String,
    message_id: String,
    next_block_index: usize,
    open_block: Option<ContentBlockState>,
    started: bool,
    completed: bool,
    // C3: seeded from the EARLY `response.created` estimate (`handle_created`)
    // so `message_start` carries a plausible non-zero value, then OVERWRITTEN
    // with the REAL upstream count once `response.completed`/`response.incomplete`
    // arrives (`handle_completed`). Also the fallback terminal usage `finalize`
    // sends when no clean terminal event ever arrives.
    pending_input_tokens: Option<u64>,
    estimated_output_bytes: usize,
    last_output_tokens: u64,
    web_search_count: u64,
    emitted_tool_call_ids: HashSet<String>,
    closed_tool_call_ids: HashSet<String>,
    // Deferred reasoning (G8) state machine (T8): owns the buffer, signature,
    // late-reasoning gate, tool-call flag, and the promote/hold decisions. See
    // `reasoning::ReasoningEgressState`. Block EMISSION stays on the converter
    // (it owns block indices + `open_block`); the converter delegates the
    // buffer/hold/promote DECISIONS to `self.reasoning`.
    reasoning: ReasoningEgressState,
    /// Keep backend reasoning available to the parser while omitting it from an
    /// Anthropic response whose request did not enable thinking.
    suppress_reasoning: bool,
}

impl AnthropicStreamConverter {
    pub fn new(model: String) -> Self {
        Self::with_reasoning_suppression(model, false)
    }

    pub fn with_reasoning_suppression(model: String, suppress_reasoning: bool) -> Self {
        Self {
            model,
            message_id: format!("msg_{}", Uuid::new_v4().simple()),
            next_block_index: 0,
            open_block: None,
            started: false,
            completed: false,
            pending_input_tokens: None,
            estimated_output_bytes: 0,
            last_output_tokens: 0,
            web_search_count: 0,
            emitted_tool_call_ids: HashSet::new(),
            closed_tool_call_ids: HashSet::new(),
            reasoning: ReasoningEgressState::default(),
            suppress_reasoning,
        }
    }

    /// `usage.server_tool_use` for terminal events: `Some` once a server-side
    /// web search has run this turn, otherwise `None` so token-only turns stay
    /// byte-identical.
    fn server_tool_use_usage(&self) -> Option<AnthropicServerToolUse> {
        (self.web_search_count > 0).then_some(AnthropicServerToolUse {
            web_search_requests: self.web_search_count,
        })
    }

    pub fn convert(&mut self, event: &SseEvent) -> Vec<AnthropicStreamEvent> {
        let mut output = Vec::new();
        match event.event.as_str() {
            "response.created" => self.handle_created(&event.data, &mut output),
            "response.output_item.added" => self.handle_item_added(&event.data, &mut output),
            "response.output_text.delta" => self.handle_text_delta(&event.data, &mut output),
            "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                if self.suppress_reasoning {
                    self.ensure_started(&mut output);
                    if let Some(delta) = event.data.get("delta").and_then(Value::as_str) {
                        self.record_output_delta(delta);
                    }
                } else {
                    self.handle_reasoning_delta(&event.data, &mut output);
                }
            }
            "response.reasoning_summary_text.signature_delta" if !self.suppress_reasoning => {
                self.handle_reasoning_signature_delta(&event.data, &mut output);
            }
            "response.function_call_arguments.delta" => {
                self.handle_function_call_arguments_delta(&event.data, &mut output);
            }
            "response.function_call_arguments.done" => {
                self.handle_function_call_arguments_done(&event.data, &mut output);
            }
            "response.output_item.done" => self.handle_item_done(&event.data, &mut output),
            "response.completed" | "response.incomplete" => {
                self.handle_completed(event.event.as_str(), &event.data, &mut output)
            }
            "response.failed" => self.handle_failed(&event.data, &mut output),
            "response.web_search_results" => {
                self.handle_web_search_results(&event.data, &mut output)
            }
            _ => {}
        }
        output
    }

    fn ensure_started(&mut self, output: &mut Vec<AnthropicStreamEvent>) {
        if !self.started {
            self.started = true;
            // C4 (deviation #4): vLLM native emits NO `ping` before
            // `message_start`. Do not push
            // `AnthropicStreamEvent::Ping` here to byte-match. This is safe for
            // SSE keep-alive: `http.rs::stream_anthropic_response` (and the
            // sibling Chat/Responses streamers) already wrap the response in
            // `axum::response::sse::KeepAlive::new()`, which sends its own
            // transport-level `: keep-alive` comment frames independent of the
            // Anthropic event vocabulary -- so dropping this event does not
            // remove keep-alive coverage. The `Ping` variant itself is kept
            // (not deleted) as a still-valid representable wire shape.
            output.push(AnthropicStreamEvent::MessageStart {
                message: AnthropicMessageStart {
                    id: self.message_id.clone(),
                    kind: "message".to_string(),
                    role: "assistant".to_string(),
                    content: Vec::new(),
                    model: self.model.clone(),
                    stop_reason: None,
                    stop_sequence: None,
                    usage: AnthropicUsage {
                        input_tokens: Some(self.pending_input_tokens.unwrap_or(0)),
                        output_tokens: Some(0),
                        output_tokens_details: None,
                        server_tool_use: None,
                    },
                },
            });
        }
    }

    fn handle_created(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        // C3: seed `pending_input_tokens` from the engine's EARLY estimate
        // (`response.created.response.estimated_input_tokens`, threaded by
        // `engine.rs::created_event`) BEFORE `ensure_started` reads it, so
        // `message_start.usage.input_tokens` is a plausible non-zero value
        // instead of the previous hardcoded `0`. Guarded on `is_none()` so a
        // real upstream count is never clobbered by a stale estimate --
        // `response.created` fires once, at the very start of the turn, so
        // this guard is defensive rather than load-bearing today. This is an
        // ESTIMATE (~4 bytes/token, see `estimate_input_tokens`), NOT the exact
        // tokenizer count -- the terminal `message_delta` / non-stream usage
        // always overrides it with the REAL upstream count once
        // `response.completed` arrives (`handle_completed`, below).
        if self.pending_input_tokens.is_none() {
            self.pending_input_tokens = response_estimated_input_tokens(data);
        }
        self.ensure_started(output);
    }

    fn handle_item_added(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.ensure_started(output);
        let Some(item) = data.get("item") else { return };
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
        match item_type {
            "message" => {
                self.flush_reasoning_as_thinking(output);
                self.close_open_block(output);
                self.reasoning.note_content_started();
                self.start_text_block(output);
            }
            "reasoning" => {
                // Deferred: do not open a thinking block here. Reasoning is
                // buffered until the stream shape is known (see struct docs).
            }
            _ => {}
        }
    }

    fn handle_text_delta(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.ensure_started(output);
        let Some(delta) = data.get("delta").and_then(Value::as_str) else {
            return;
        };
        match self.open_block {
            Some(ContentBlockState::Text { index }) => {
                output.push(AnthropicStreamEvent::ContentBlockDelta {
                    index,
                    delta: AnthropicDelta::TextDelta {
                        text: delta.to_string(),
                    },
                });
                self.record_output_delta(delta);
            }
            _ => {
                self.flush_reasoning_as_thinking(output);
                self.close_open_block(output);
                self.reasoning.note_content_started();
                self.start_text_block(output);
                if let Some(ContentBlockState::Text { index }) = self.open_block {
                    output.push(AnthropicStreamEvent::ContentBlockDelta {
                        index,
                        delta: AnthropicDelta::TextDelta {
                            text: delta.to_string(),
                        },
                    });
                    self.record_output_delta(delta);
                }
            }
        }
    }

    fn handle_reasoning_delta(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.ensure_started(output);
        let Some(delta) = data.get("delta").and_then(Value::as_str) else {
            return;
        };
        if delta.is_empty() {
            return;
        }
        // Late reasoning: anything that arrives after text/tool output has
        // already begun is abnormal and dropped (it cannot legally precede the
        // content that was already streamed).
        if self.reasoning.is_late_reasoning() {
            return;
        }
        // Defer: buffer the reasoning. Output-token bookkeeping still
        // accumulates (record_output_delta), but it no longer pushes a
        // progressive `message_delta` -- see that function's doc comment.
        self.record_output_delta(delta);
        self.reasoning.push_reasoning(delta);
    }

    fn handle_reasoning_signature_delta(
        &mut self,
        data: &Value,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        self.ensure_started(output);
        let Some(signature) = data.get("signature").and_then(Value::as_str) else {
            return;
        };
        if signature.is_empty() || self.reasoning.is_late_reasoning() {
            return;
        }
        // Buffered alongside the reasoning text; flushed with it. A signature is
        // a marker of genuine chain-of-thought, so its presence later pins the
        // buffer to a `thinking` block (never promoted to text). A signature can
        // arrive in multiple `signature_delta` chunks, so accumulate them in
        // order (concatenate) rather than overwriting -- otherwise only the last
        // fragment would survive.
        self.reasoning.push_signature(signature);
    }

    fn handle_function_call_arguments_delta(
        &mut self,
        data: &Value,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        self.ensure_started(output);
        let Some(call_id) = data.get("call_id").and_then(Value::as_str) else {
            return;
        };
        if self.emitted_tool_call_ids.contains(call_id)
            || self.closed_tool_call_ids.contains(call_id)
        {
            return;
        }
        let name = data.get("name").and_then(Value::as_str).unwrap_or_default();
        // Server-side tools (web_search, analyzeImage): swallow streamed
        // arguments. web_search is surfaced via `response.web_search_results`;
        // analyzeImage is fully internal (G4). Opening a client tool_use block
        // here would leak it, flip the turn to `tool_use`, and trip the
        // late-reasoning gate against the post-tool reasoning.
        if is_hidden_server_tool(name) {
            return;
        }
        let Some(delta) = data.get("delta").and_then(Value::as_str) else {
            return;
        };
        if delta.is_empty() {
            return;
        }
        self.reasoning.note_tool_calls();
        self.ensure_tool_block(call_id, name, output);
        if let Some(ContentBlockState::ToolUse { index, .. }) = self.open_block {
            output.push(AnthropicStreamEvent::ContentBlockDelta {
                index,
                delta: AnthropicDelta::InputJsonDelta {
                    partial_json: delta.to_string(),
                },
            });
            self.record_output_delta(delta);
        }
    }

    fn handle_function_call_arguments_done(
        &mut self,
        data: &Value,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        self.ensure_started(output);
        let Some(call_id) = data.get("call_id").and_then(Value::as_str) else {
            return;
        };
        // Server-side tools (web_search, analyzeImage): swallow. Must precede the
        // `has_tool_calls` flip so the turn ends `end_turn` and post-tool
        // reasoning is not gated as "late".
        if data
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(is_hidden_server_tool)
        {
            return;
        }
        self.reasoning.note_tool_calls();
        if self.emitted_tool_call_ids.contains(call_id) {
            return;
        }
        if self.closed_tool_call_ids.contains(call_id) {
            self.emitted_tool_call_ids.insert(call_id.to_string());
            return;
        }
        if matches!(
            &self.open_block,
            Some(ContentBlockState::ToolUse {
                call_id: open_call_id,
                ..
            }) if open_call_id == call_id
        ) {
            self.close_open_block(output);
            self.emitted_tool_call_ids.insert(call_id.to_string());
            return;
        }

        let name = data.get("name").and_then(Value::as_str).unwrap_or_default();
        let arguments = data
            .get("arguments")
            .and_then(Value::as_str)
            .unwrap_or("{}");
        self.ensure_tool_block(call_id, name, output);
        if !arguments.is_empty()
            && let Some(ContentBlockState::ToolUse { index, .. }) = self.open_block
        {
            output.push(AnthropicStreamEvent::ContentBlockDelta {
                index,
                delta: AnthropicDelta::InputJsonDelta {
                    partial_json: arguments.to_string(),
                },
            });
            self.record_output_delta(arguments);
        }
        self.close_open_block(output);
        self.emitted_tool_call_ids.insert(call_id.to_string());
    }

    fn handle_item_done(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        let Some(item) = data.get("item") else { return };
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
        match item_type {
            "message" => {
                if matches!(self.open_block, Some(ContentBlockState::Text { .. })) {
                    self.close_open_block(output);
                }
            }
            "reasoning" => {
                // Deferred: the reasoning item completing does not flush the
                // buffer. The buffer is held until content arrives (-> thinking)
                // or the turn ends (-> promote to text / keep as thinking).
            }
            "function_call" | "custom_tool_call" => {
                // G4: a server-side `analyzeImage` function_call is hidden like
                // `web_search_call` (review #3) — never surfaced as a tool_use
                // block. The engine already keeps it out of the public stream.
                if item
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(is_hidden_server_tool)
                {
                    return;
                }
                self.close_open_block(output);
                self.emit_tool_use_block(item, output);
            }
            "web_search_call" => {
                // Server-side tool: deliberately not surfaced. Brave Search runs
                // server-side, so the Anthropic client must not see a tool_use
                // block for it; the final answer (generated after results are
                // injected) is emitted as regular text. Same effect as the `_`
                // arm — kept explicit to document the intentional omission.
            }
            _ => {}
        }
    }

    /// Guarantees the Anthropic stream is terminated.
    ///
    /// The converter only emits `message_delta` + `message_stop` when it sees a
    /// `response.completed` event. If the upstream turn ends any other way with
    /// NO explicit terminal signal at all (a dropped/stalled engine task, an
    /// aborted web-search round-trip), the client would otherwise be left
    /// waiting on a connection that eventually just closes with no parseable
    /// terminal event. Callers MUST invoke this once the upstream event stream
    /// is exhausted so every connection ends with a terminal event.
    ///
    /// C4: an explicit `response.failed` is NOT one of those "no signal" cases
    /// any more -- `handle_failed` now marks the turn `completed` itself, so
    /// this becomes a no-op after a failure and the stream ends at `error`
    /// (matching Anthropic's real error-stream shape: a mid-stream error is
    /// terminal on its own, never followed by a synthetic `message_delta` +
    /// `message_stop`). See `handle_failed` for the rationale.
    pub fn finalize(&mut self) -> Vec<AnthropicStreamEvent> {
        let mut output = Vec::new();
        if self.completed {
            return output;
        }
        self.ensure_started(&mut output);
        // No clean terminal event arrived (engine error / stalled turn). The
        // reference treats a missing finish_reason as "do not promote": flush any
        // buffered reasoning as a thinking block rather than guessing it was the
        // answer. Promotion is reserved for a clean `response.completed`.
        self.flush_reasoning_as_thinking(&mut output);
        self.close_open_block(&mut output);
        output.push(AnthropicStreamEvent::MessageDelta {
            delta: AnthropicMessageDeltaBody {
                stop_reason: Some(if self.reasoning.has_tool_calls {
                    "tool_use".to_string()
                } else {
                    "end_turn".to_string()
                }),
                stop_sequence: None,
            },
            usage: AnthropicUsage {
                input_tokens: self.pending_input_tokens,
                output_tokens: Some(self.last_output_tokens),
                output_tokens_details: None,
                server_tool_use: self.server_tool_use_usage(),
            },
        });
        output.push(AnthropicStreamEvent::MessageStop);
        self.completed = true;
        output
    }

    fn handle_completed(
        &mut self,
        event_type: &str,
        data: &Value,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        self.completed = true;
        let mapped_stop_reason = response_stop_reason(data);
        // T7: resolve any buffered reasoning before closing out, promoting a
        // reasoning-only turn to text ONLY on a CLEAN STOP (`terminal_reason:
        // stop`). The gate reads the typed terminal reason the engine carries
        // on the resource (not the event-type string), so a future non-stop
        // terminal reason arriving as `response.completed` can no longer
        // wrongly promote — `response.incomplete` is never a clean stop, and a
        // non-`stop` `response.completed` (e.g. `content_filter`) is not
        // either. Fallback: if the typed reason is absent (older event / a
        // non-terminal resource), only a literal `response.completed` is
        // treated as a clean stop (preserves the pre-T7 behavior for any path
        // the engine did not tag).
        let clean_stop = response_terminal_reason(data)
            .map(|reason| reason.is_clean_stop())
            .unwrap_or_else(|| event_type == "response.completed");
        self.flush_reasoning_terminal(clean_stop, output);
        self.close_open_block(output);
        let usage = response_usage(data);
        if let Some(usage) = usage.as_ref() {
            self.pending_input_tokens = Some(usage.input_tokens);
        }
        let output_tokens = usage
            .as_ref()
            .map(|usage| usage.output_tokens)
            .unwrap_or(self.last_output_tokens)
            .max(self.last_output_tokens);
        self.last_output_tokens = output_tokens;
        let (stop_reason, stop_sequence) = if let Some(reason) = mapped_stop_reason {
            (reason, None)
        } else if self.reasoning.has_tool_calls {
            // Anthropic precedence is tool_use > stop_sequence > end_turn.
            ("tool_use".to_string(), None)
        } else if let Some(stop) = response_stop_sequence(data) {
            ("stop_sequence".to_string(), Some(Value::String(stop)))
        } else {
            ("end_turn".to_string(), None)
        };
        output.push(AnthropicStreamEvent::MessageDelta {
            delta: AnthropicMessageDeltaBody {
                stop_reason: Some(stop_reason),
                stop_sequence,
            },
            usage: AnthropicUsage {
                input_tokens: usage.as_ref().map(|usage| usage.input_tokens),
                output_tokens: Some(output_tokens),
                output_tokens_details: usage
                    .as_ref()
                    .and_then(|usage| usage.output_tokens_details.clone()),
                server_tool_use: self.server_tool_use_usage(),
            },
        });
        output.push(AnthropicStreamEvent::MessageStop);
    }

    // C4 (deviation "error-terminal shape"): a real Anthropic stream ends AT
    // the `error` event on a mid-stream failure -- there is no following
    // `message_delta` / `message_stop` (see the SDKs' streaming error
    // handling, and the standalone `event: error` example in Anthropic's
    // streaming docs, which is never followed by a stop pair). Mark the turn
    // `completed` here so the caller's unconditional trailing `finalize()`
    // call (`http.rs::stream_anthropic_response`) becomes a no-op instead of
    // appending a synthetic terminal delta + stop after the error. This does
    // NOT reopen a hang risk: the SSE task in `http.rs` always finishes (and
    // drops its `mpsc::Sender`) once the upstream event loop + `finalize()`
    // both return, regardless of how many events `finalize()` emits, which is
    // what actually closes the HTTP stream -- the trailing pair was never
    // load-bearing for that. Also reconciles the conformance harness's
    // `Surface::Error` contract (`conformance.rs` invariant 6: the stream must
    // end WITH the `error` event), which this now satisfies for real.
    fn handle_failed(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        let message = data
            .get("response")
            .and_then(|r| r.get("error"))
            .and_then(|e| e.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("gateway error")
            .to_string();
        output.push(AnthropicStreamEvent::Error {
            error: AnthropicErrorBody {
                kind: "api_error".to_string(),
                message,
            },
        });
        self.completed = true;
    }

    /// Surface a server-side web search to the Anthropic client.
    ///
    /// resp2chat runs Brave server-side, so the client never sees the model's
    /// own tool call. Without explicit `server_tool_use` + `web_search_tool_result`
    /// blocks, Claude Code reports "Did 0 searches" and renders no source
    /// citations. This mirrors Anthropic's native web-search streaming shape:
    /// a `server_tool_use` block (query streamed via `input_json_delta`) followed
    /// by a `web_search_tool_result` block carrying the structured sources. The
    /// turn still ends with `end_turn` (the search is resolved server-side), so
    /// `has_tool_calls` is intentionally left untouched.
    ///
    /// A web-search round is additive, NOT real text/tool output: a normal turn
    /// is reasoning -> web_search_call -> `web_search_results` -> CONTINUATION
    /// reasoning -> text answer. So this block must NOT trip the late-reasoning
    /// gate (`content_started`) -- doing so would drop the legitimate
    /// post-search continuation reasoning. It still flushes any buffered
    /// PRE-search reasoning as a `thinking` block (genuine CoT that preceded the
    /// search) and records the search via `web_search_count` (tracked separately
    /// from `content_started`, which keys only on real text/tool content). The
    /// continuation reasoning that follows is buffered and handled normally
    /// (flushed as thinking when the answer starts, or resolved at the terminal).
    fn handle_web_search_results(&mut self, data: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.ensure_started(output);
        self.flush_reasoning_as_thinking(output);
        self.close_open_block(output);
        self.web_search_count += 1;

        let tool_use_id = data
            .get("tool_use_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let query = data.get("query").and_then(Value::as_str).unwrap_or("");
        let results = data
            .get("results")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));

        let stu_index = self.next_block_index;
        self.next_block_index += 1;
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index: stu_index,
            content_block: AnthropicContentBlockStart::ServerToolUse {
                id: tool_use_id.clone(),
                name: "web_search".to_string(),
            },
        });
        output.push(AnthropicStreamEvent::ContentBlockDelta {
            index: stu_index,
            delta: AnthropicDelta::InputJsonDelta {
                partial_json: serde_json::json!({ "query": query }).to_string(),
            },
        });
        output.push(AnthropicStreamEvent::ContentBlockStop { index: stu_index });

        let result_index = self.next_block_index;
        self.next_block_index += 1;
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index: result_index,
            content_block: AnthropicContentBlockStart::WebSearchToolResult {
                tool_use_id,
                content: results,
            },
        });
        output.push(AnthropicStreamEvent::ContentBlockStop {
            index: result_index,
        });
    }

    /// Flush buffered reasoning as a genuine `thinking` block.
    ///
    /// Used when text/tool content follows the reasoning (the reasoning was a
    /// real chain-of-thought preface) and at the terminal event for truncated or
    /// signed reasoning. No-op when the buffer is empty. The buffer (text and
    /// signature) is consumed.
    fn flush_reasoning_as_thinking(&mut self, output: &mut Vec<AnthropicStreamEvent>) {
        if !self.reasoning.has_buffered() {
            return;
        }
        self.close_open_block(output);
        let index = self.next_block_index;
        self.next_block_index += 1;
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index,
            content_block: AnthropicContentBlockStart::Thinking {
                thinking: String::new(),
            },
        });
        // `take_buffer` returns the concatenated text; emit each ORIGINAL chunk
        // as its own delta to preserve the exact stream shape. Re-split by the
        // recorded chunks: take_buffer concatenates, so instead iterate the
        // buffer's chunks directly via take_signature + per-chunk emit. Also
        // accumulate the concatenation as we go (one pass, no extra clone) so
        // an unsigned block can derive a synthetic signature below.
        let buffer = std::mem::take(&mut self.reasoning.reasoning_buffer);
        let mut thinking_text = String::new();
        for chunk in buffer {
            thinking_text.push_str(&chunk);
            output.push(AnthropicStreamEvent::ContentBlockDelta {
                index,
                delta: AnthropicDelta::ThinkingDelta { thinking: chunk },
            });
        }
        // Every emitted thinking block must close with a non-empty signature_delta.
        // A real upstream signature (vLLM native, an Anthropic-history replay)
        // forwards unchanged. When the reasoning channel carried none (e.g.
        // DeepSeek's `reasoning_content`), synthesize a deterministic
        // stand-in -- see `synthetic_signature` for why it must NOT depend on
        // `message_id`. Stripped back out on Anthropic ingress
        // (`anthropic_to_responses.rs`) so a client echo-back is never
        // re-forwarded upstream as a genuine signature.
        let signature = self
            .reasoning
            .take_signature()
            .unwrap_or_else(|| synthetic_signature(&thinking_text));
        output.push(AnthropicStreamEvent::ContentBlockDelta {
            index,
            delta: AnthropicDelta::SignatureDelta { signature },
        });
        output.push(AnthropicStreamEvent::ContentBlockStop { index });
    }

    /// Resolve the reasoning buffer at the terminal event.
    ///
    /// If reasoning was the *only* output this turn (no text or tool blocks),
    /// the backend put the answer in the reasoning channel: promote it to a
    /// `text` block so the client renders it -- but ONLY on a CLEAN STOP (a
    /// true `finish_reason:stop`, surfaced as `terminal_reason: stop` since
    /// T7) and only when it is not genuine chain-of-thought (no signature).
    /// EVERY other terminal reason -- `length` (`response.incomplete`), a
    /// future `content_filter` arriving as `response.completed`, or any
    /// non-`stop` reason -- is not a clean stop, so `clean_stop` is false and
    /// the buffer stays a `thinking` block. Once any content has started,
    /// leftover reasoning is just a normal preface and is flushed as a
    /// `thinking` block.
    fn flush_reasoning_terminal(
        &mut self,
        clean_stop: bool,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        if !self.reasoning.has_buffered() {
            return;
        }
        let promote = self.reasoning.should_promote(clean_stop);
        if !promote {
            self.flush_reasoning_as_thinking(output);
            return;
        }
        let promoted = self.reasoning.take_buffer();
        self.reasoning.reasoning_signature = None;
        self.reasoning.note_content_started();
        self.close_open_block(output);
        let index = self.next_block_index;
        self.next_block_index += 1;
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index,
            content_block: AnthropicContentBlockStart::Text {
                text: String::new(),
            },
        });
        output.push(AnthropicStreamEvent::ContentBlockDelta {
            index,
            delta: AnthropicDelta::TextDelta { text: promoted },
        });
        output.push(AnthropicStreamEvent::ContentBlockStop { index });
    }

    fn close_open_block(&mut self, output: &mut Vec<AnthropicStreamEvent>) {
        if let Some(block) = self.open_block.take() {
            let index = match block {
                ContentBlockState::Text { index } => index,
                ContentBlockState::ToolUse { index, call_id } => {
                    self.closed_tool_call_ids.insert(call_id);
                    index
                }
            };
            output.push(AnthropicStreamEvent::ContentBlockStop { index });
        }
    }

    /// Update output-token bookkeeping for a streamed delta chunk.
    ///
    /// Bookkeeping-ONLY (deviation #1 fix): this used to also push a progressive
    /// `message_delta` onto the wire for every chunk, which produced a
    /// `message_delta` storm (10+ events in a typical reasoning+text turn) --
    /// real Anthropic streams (and strict client SDKs) expect exactly ONE
    /// terminal `message_delta`. The byte-count estimate is still worth keeping,
    /// though: `last_output_tokens` is the only source for the terminal delta's
    /// `output_tokens` when upstream usage is absent, read by both the streaming
    /// terminal paths (`handle_completed` / `finalize`) and the non-stream
    /// collector (`collector.rs:150-156`). Dropping the bookkeeping alongside the
    /// removed push would silently zero out `output_tokens` on every upstream
    /// that doesn't report usage.
    fn record_output_delta(&mut self, delta: &str) {
        if delta.is_empty() {
            return;
        }
        self.estimated_output_bytes = self.estimated_output_bytes.saturating_add(delta.len());
        let estimated_tokens = self
            .estimated_output_bytes
            .div_ceil(ESTIMATED_OUTPUT_TOKEN_BYTES) as u64;
        self.last_output_tokens = self.last_output_tokens.max(estimated_tokens);
    }

    fn start_text_block(&mut self, output: &mut Vec<AnthropicStreamEvent>) {
        let index = self.next_block_index;
        self.next_block_index += 1;
        self.open_block = Some(ContentBlockState::Text { index });
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index,
            content_block: AnthropicContentBlockStart::Text {
                text: String::new(),
            },
        });
    }

    fn ensure_tool_block(
        &mut self,
        call_id: &str,
        name: &str,
        output: &mut Vec<AnthropicStreamEvent>,
    ) {
        if matches!(
            &self.open_block,
            Some(ContentBlockState::ToolUse {
                call_id: open_call_id,
                ..
            }) if open_call_id == call_id
        ) {
            return;
        }
        self.flush_reasoning_as_thinking(output);
        self.reasoning.note_content_started();
        self.close_open_block(output);
        let index = self.next_block_index;
        self.next_block_index += 1;
        self.open_block = Some(ContentBlockState::ToolUse {
            index,
            call_id: call_id.to_string(),
        });
        output.push(AnthropicStreamEvent::ContentBlockStart {
            index,
            content_block: AnthropicContentBlockStart::ToolUse {
                id: call_id.to_string(),
                name: name.to_string(),
                input: Value::Object(Default::default()),
            },
        });
    }

    fn emit_tool_use_block(&mut self, item: &Value, output: &mut Vec<AnthropicStreamEvent>) {
        self.reasoning.note_tool_calls();
        let call_id = item
            .get("call_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if self.emitted_tool_call_ids.contains(call_id) {
            return;
        }
        if self.closed_tool_call_ids.contains(call_id) {
            self.emitted_tool_call_ids.insert(call_id.to_string());
            return;
        }
        let name = item.get("name").and_then(Value::as_str).unwrap_or_default();
        let arguments = item
            .get("arguments")
            .and_then(Value::as_str)
            .unwrap_or("{}");

        self.ensure_tool_block(call_id, name, output);
        if let Some(ContentBlockState::ToolUse { index, .. }) = self.open_block {
            output.push(AnthropicStreamEvent::ContentBlockDelta {
                index,
                delta: AnthropicDelta::InputJsonDelta {
                    partial_json: arguments.to_string(),
                },
            });
            self.record_output_delta(arguments);
        }
        self.close_open_block(output);
        self.emitted_tool_call_ids.insert(call_id.to_string());
    }
}

/// Deterministic synthetic signature for a `thinking` block whose upstream
/// reasoning channel carried none (C2, deviation #2). Hashes ONLY the
/// buffered thinking text -- deliberately NOT `self.message_id`, which is a
/// fresh random UUID per `AnthropicStreamConverter::new` call and would make
/// the identical canonical input produce a different signature on every
/// converter instance. Hashing just the text keeps the "same input -> same
/// synthetic signature" contract true across independent converter runs
/// (what tests assert on), while still never touching wall-clock time or an
/// RNG. This is a SHAPE-only marker, not a real Anthropic signature -- see
/// `SYNTHETIC_SIGNATURE_PREFIX`.
fn synthetic_signature(thinking_text: &str) -> String {
    let digest = Sha256::digest(thinking_text.as_bytes());
    format!("{SYNTHETIC_SIGNATURE_PREFIX}{}", hex::encode(digest))
}

/// C3: the EARLY estimate the engine threads onto `response.created`
/// (`ResponseStub::estimated_input_tokens`) -- `None` when the event doesn't
/// carry one (e.g. the bare `created_event()` fixture most converter tests
/// use), in which case `message_start.usage.input_tokens` stays the prior
/// hardcoded `Some(0)` fallback via `ensure_started`.
fn response_estimated_input_tokens(data: &Value) -> Option<u64> {
    data.get("response")?
        .get("estimated_input_tokens")?
        .as_u64()
}

pub(super) fn response_usage(data: &Value) -> Option<AnthropicMessageUsage> {
    let usage = data.get("response")?.get("usage")?;
    Some(AnthropicMessageUsage {
        input_tokens: usage.get("input_tokens")?.as_u64()?,
        output_tokens: usage.get("output_tokens")?.as_u64()?,
        output_tokens_details: usage
            .get("output_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .or_else(|| {
                usage
                    .get("output_tokens_details")
                    .and_then(|details| details.get("thinking_tokens"))
            })
            .and_then(Value::as_u64)
            .map(|thinking_tokens| AnthropicOutputTokensDetails { thinking_tokens }),
    })
}

fn response_stop_sequence(data: &Value) -> Option<String> {
    data.get("response")
        .and_then(|response| response.get("stop_sequence"))
        .and_then(Value::as_str)
        .filter(|stop| !stop.is_empty())
        .map(str::to_string)
}

fn response_stop_reason(data: &Value) -> Option<String> {
    if let Some(reason) = data
        .get("response")
        .and_then(|response| response.get("incomplete_details"))
        .and_then(|details| details.get("reason"))
        .and_then(Value::as_str)
    {
        return Some(match reason {
            "max_output_tokens" => "max_tokens".to_string(),
            other => other.to_string(),
        });
    }
    None
}

/// Typed terminal reason the engine carries on the terminal resource (T7).
/// `None` only when the field is ABSENT (a non-terminal resource, or an older
/// event that the engine did not tag — the caller falls back to the event-type
/// string). A PRESENT-but-unrecognized reason maps to `Other` (non-clean), NOT
/// `None` — so a future reason the converter doesn't know still gates as
/// non-clean rather than falling back to the event-type string (T7 R1 fix).
/// Reads `data.response.terminal_reason`.
fn response_terminal_reason(data: &Value) -> Option<crate::models::responses::TerminalReason> {
    use crate::models::responses::TerminalReason;
    data.get("response")
        .and_then(|response| response.get("terminal_reason"))
        .and_then(Value::as_str)
        // Delegate to the canonical string→variant map (the sole authoritative
        // mapping). PRESENT-but-unrecognized ⇒ `Other` (non-clean, never falls
        // back to the event-type string); ABSENT ⇒ `None` (T7 R1 invariant).
        .map(|reason| TerminalReason::from_finish_reason(Some(reason)))
}
