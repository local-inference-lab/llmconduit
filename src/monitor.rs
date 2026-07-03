use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::broadcast;

const DEBUG_PROTOCOL_VERSION: u32 = 1;
const DEBUG_HISTORY_RETENTION_MS: u128 = 30 * 60 * 1000;
const REQUEST_TEXT_CHAR_LIMIT: usize = 128 * 1024;
const REQUEST_EVENT_LIMIT: usize = 512;

#[derive(Debug, Clone, Serialize)]
pub enum MonitorEventKind {
    RequestStarted {
        model: String,
        input_items: usize,
        tool_count: usize,
        turn_count: usize,
        user_messages: usize,
        assistant_messages: usize,
        system_messages: usize,
        developer_messages: usize,
        reasoning_items: usize,
        function_calls: usize,
        function_outputs: usize,
        tool_items: usize,
        input_chars: usize,
        instructions_chars: usize,
    },
    RequestPayload {
        payload_preview: String,
        images: Vec<DebugEventImage>,
    },
    UpstreamRequest {
        request_index: usize,
        message_count: usize,
        prompt_chars: usize,
        payload_preview: String,
        images: Vec<DebugEventImage>,
    },
    FinalResponse {
        status: String,
        payload_preview: String,
        images: Vec<DebugEventImage>,
    },
    ResponseItem {
        event: String,
        summary: String,
        payload_preview: String,
    },
    OutputTextDelta {
        delta: String,
    },
    ReasoningTextDelta {
        delta: String,
    },
    FunctionCallArgumentsDelta {
        call_id: String,
        delta: String,
    },
    RefusalDelta {
        delta: String,
    },
    ToolPhase {
        phase: String,
        detail: String,
    },
    Completed,
    Failed {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct MonitorEvent {
    pub sequence: u64,
    pub response_id: String,
    pub timestamp_ms: u128,
    pub kind: MonitorEventKind,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugUpdate {
    pub sequence: u64,
    pub messages: Vec<DebugWsMessage>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugSnapshot {
    pub last_sequence: u64,
    pub messages: Vec<DebugWsMessage>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DebugWsMessage {
    Hello {
        protocol_version: u32,
        history_limit: usize,
        history_retention_ms: u128,
    },
    RequestUpsert {
        request: DebugRequest,
    },
    SegmentAppend {
        response_id: String,
        segment: DebugSegment,
    },
    EventAppend {
        response_id: String,
        event: DebugTimelineEvent,
    },
    RequestStatus {
        response_id: String,
        status: DebugRequestStatus,
        completed_at_ms: Option<u128>,
        error: Option<String>,
    },
    RequestRemove {
        response_id: String,
        reason: String,
    },
    SnapshotDone,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugRequest {
    pub response_id: String,
    pub model: String,
    pub started_at_ms: u128,
    pub updated_at_ms: u128,
    pub completed_at_ms: Option<u128>,
    pub status: DebugRequestStatus,
    pub stats: DebugRequestStats,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugRequestStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct DebugRequestStats {
    pub input_items: usize,
    pub tool_count: usize,
    pub turn_count: usize,
    pub user_messages: usize,
    pub assistant_messages: usize,
    pub system_messages: usize,
    pub developer_messages: usize,
    pub reasoning_items: usize,
    pub function_calls: usize,
    pub function_outputs: usize,
    pub tool_items: usize,
    pub input_chars: usize,
    pub instructions_chars: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugSegment {
    pub timestamp_ms: u128,
    pub kind: DebugSegmentKind,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugSegmentKind {
    Output,
    Reasoning,
    Tool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugTimelineEvent {
    pub timestamp_ms: u128,
    pub kind: String,
    pub summary: String,
    pub payload_preview: Option<String>,
    pub images: Vec<DebugEventImage>,
}

/// Metadata about an image found in a request/response preview, surfaced to the
/// debug UI over `/debug/ws`. Carries ONLY non-sensitive descriptors — never the
/// raw image bytes or URL (G4 round-4 #4): `data:`/signed URLs must not leave the
/// process via the monitor broadcast. The UI renders a redacted placeholder card
/// from this metadata, not the image itself.
#[derive(Debug, Clone, Serialize)]
pub struct DebugEventImage {
    pub id: String,
    pub label: String,
    pub path: String,
    pub mime_type: String,
    pub size_bytes: Option<usize>,
}

#[derive(Clone)]
pub struct MonitorHub {
    enabled: bool,
    tx: broadcast::Sender<DebugUpdate>,
    state: Arc<Mutex<MonitorState>>,
}

#[derive(Debug)]
struct MonitorState {
    history_limit: usize,
    last_sequence: u64,
    records: VecDeque<DebugRequestRecord>,
}

#[derive(Debug)]
struct DebugRequestRecord {
    request: DebugRequest,
    segments: VecDeque<DebugSegment>,
    events: VecDeque<DebugTimelineEvent>,
    active_function_call_id: Option<String>,
    pending_output_backslash: bool,
}

impl MonitorHub {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            enabled: true,
            tx,
            state: Arc::new(Mutex::new(MonitorState {
                history_limit: capacity.max(1),
                last_sequence: 0,
                records: VecDeque::new(),
            })),
        }
    }

    pub fn disabled() -> Self {
        let (tx, _) = broadcast::channel(1);
        Self {
            enabled: false,
            tx,
            state: Arc::new(Mutex::new(MonitorState {
                history_limit: 1,
                last_sequence: 0,
                records: VecDeque::new(),
            })),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn emit(&self, response_id: impl Into<String>, kind: MonitorEventKind) {
        if !self.enabled {
            return;
        }
        let mut state = self.state.lock().expect("monitor state lock poisoned");
        state.last_sequence = state.last_sequence.saturating_add(1);
        let event = MonitorEvent {
            sequence: state.last_sequence,
            response_id: response_id.into(),
            timestamp_ms: now_ms(),
            kind,
        };
        let mut messages = state.apply_event(&event);
        messages.extend(state.prune_expired(event.timestamp_ms));
        drop(state);
        // Round-6 #1: redact image `data:`/signed URLs from EVERY outgoing
        // `/debug/ws` message at this single broadcast choke point, so no raw
        // image data/URL ever leaves the process via the monitor. This is on the
        // active-debug path only (`emit` early-returns when disabled), so the
        // production `MonitorHub::disabled()` path keeps zero overhead.
        for message in &mut messages {
            redact_ws_message_image_uris(message);
        }
        let update = DebugUpdate {
            sequence: event.sequence,
            messages,
        };
        let _ = self.tx.send(update);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DebugUpdate> {
        self.tx.subscribe()
    }

    pub fn snapshot(&self) -> DebugSnapshot {
        let mut state = self.state.lock().expect("monitor state lock poisoned");
        state.prune_expired(now_ms());
        let mut messages = vec![DebugWsMessage::Hello {
            protocol_version: DEBUG_PROTOCOL_VERSION,
            history_limit: state.history_limit,
            history_retention_ms: DEBUG_HISTORY_RETENTION_MS,
        }];
        for record in &state.records {
            messages.push(DebugWsMessage::RequestUpsert {
                request: record.request.clone(),
            });
            for segment in &record.segments {
                messages.push(DebugWsMessage::SegmentAppend {
                    response_id: record.request.response_id.clone(),
                    segment: segment.clone(),
                });
            }
            for event in &record.events {
                messages.push(DebugWsMessage::EventAppend {
                    response_id: record.request.response_id.clone(),
                    event: event.clone(),
                });
            }
            messages.push(DebugWsMessage::RequestStatus {
                response_id: record.request.response_id.clone(),
                status: record.request.status,
                completed_at_ms: record.request.completed_at_ms,
                error: record.request.error.clone(),
            });
        }
        messages.push(DebugWsMessage::SnapshotDone);
        // Round-6 #1: the snapshot replays accumulated segments/events over
        // `/debug/ws`; redact image URIs from every message just like the live
        // `emit` path. Snapshot segments hold the FULL accumulated text, so this
        // also catches any image URL split across live delta chunks.
        for message in &mut messages {
            redact_ws_message_image_uris(message);
        }
        DebugSnapshot {
            last_sequence: state.last_sequence,
            messages,
        }
    }
}

/// Redact image `data:`/signed URLs from a single outgoing `/debug/ws` message
/// (round-6 #1), via the shared [`crate::vision::redact_image_uris`] utility, so
/// the whole monitor/debug class is covered at one boundary. Covers every
/// text-bearing field: segment text, timeline summary/preview, model id, and
/// error strings. (`DebugEventImage` already carries no raw bytes.)
///
/// Residual edge: a `SegmentAppend` carries a single LIVE delta chunk, so an
/// image URL split ACROSS delta-chunk boundaries is only partially matched in
/// the per-chunk live message; the SNAPSHOT (full accumulated segment text)
/// redacts it completely, and the engine already redacts the request/response
/// PAYLOAD previews at their source — model output rarely emits raw image URLs.
fn redact_ws_message_image_uris(message: &mut DebugWsMessage) {
    let redact = |text: &mut String| {
        let redacted = crate::vision::redact_image_uris(text);
        if redacted != *text {
            *text = redacted;
        }
    };
    match message {
        DebugWsMessage::SegmentAppend { segment, .. } => redact(&mut segment.text),
        DebugWsMessage::EventAppend { event, .. } => {
            redact(&mut event.summary);
            if let Some(preview) = event.payload_preview.as_mut() {
                redact(preview);
            }
        }
        DebugWsMessage::RequestUpsert { request } => {
            redact(&mut request.model);
            if let Some(error) = request.error.as_mut() {
                redact(error);
            }
        }
        DebugWsMessage::RequestStatus { error, .. } => {
            if let Some(error) = error.as_mut() {
                redact(error);
            }
        }
        DebugWsMessage::Hello { .. }
        | DebugWsMessage::RequestRemove { .. }
        | DebugWsMessage::SnapshotDone => {}
    }
}

impl MonitorState {
    fn apply_event(&mut self, event: &MonitorEvent) -> Vec<DebugWsMessage> {
        match &event.kind {
            MonitorEventKind::RequestStarted {
                model,
                input_items,
                tool_count,
                turn_count,
                user_messages,
                assistant_messages,
                system_messages,
                developer_messages,
                reasoning_items,
                function_calls,
                function_outputs,
                tool_items,
                input_chars,
                instructions_chars,
            } => {
                let stats = DebugRequestStats {
                    input_items: *input_items,
                    tool_count: *tool_count,
                    turn_count: *turn_count,
                    user_messages: *user_messages,
                    assistant_messages: *assistant_messages,
                    system_messages: *system_messages,
                    developer_messages: *developer_messages,
                    reasoning_items: *reasoning_items,
                    function_calls: *function_calls,
                    function_outputs: *function_outputs,
                    tool_items: *tool_items,
                    input_chars: *input_chars,
                    instructions_chars: *instructions_chars,
                };
                self.start_record(event, model.clone(), stats);
                let mut messages = vec![DebugWsMessage::RequestUpsert {
                    request: self
                        .records
                        .front()
                        .expect("record inserted")
                        .request
                        .clone(),
                }];
                self.push_timeline_event(
                    &event.response_id,
                    event.timestamp_ms,
                    "request_started",
                    format!("model={model} input_items={input_items} tools={tool_count}"),
                    None,
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::RequestPayload {
                payload_preview,
                images,
            } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                self.push_timeline_event_with_images(
                    &event.response_id,
                    event.timestamp_ms,
                    "request_payload",
                    "Responses request".to_string(),
                    Some(payload_preview.clone()),
                    images.clone(),
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::UpstreamRequest {
                request_index,
                message_count,
                prompt_chars,
                payload_preview,
                images,
            } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                self.push_timeline_event_with_images(
                    &event.response_id,
                    event.timestamp_ms,
                    "upstream_request",
                    format!(
                        "round={request_index} messages={message_count} prompt_chars={prompt_chars}"
                    ),
                    Some(payload_preview.clone()),
                    images.clone(),
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::FinalResponse {
                status,
                payload_preview,
                images,
            } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                self.push_timeline_event_with_images(
                    &event.response_id,
                    event.timestamp_ms,
                    "final_response",
                    format!("status={status}"),
                    Some(payload_preview.clone()),
                    images.clone(),
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::ResponseItem {
                event: response_event,
                summary,
                payload_preview,
            } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                self.push_timeline_event(
                    &event.response_id,
                    event.timestamp_ms,
                    response_event,
                    summary.clone(),
                    Some(payload_preview.clone()),
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::OutputTextDelta { delta } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                let record = self
                    .record_mut(&event.response_id)
                    .expect("record exists after ensure");
                append_output_text_delta(
                    record,
                    event.timestamp_ms,
                    &event.response_id,
                    delta,
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::ReasoningTextDelta { delta } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                let record = self
                    .record_mut(&event.response_id)
                    .expect("record exists after ensure");
                append_styled_text_delta(
                    record,
                    event.timestamp_ms,
                    &event.response_id,
                    DebugSegmentKind::Reasoning,
                    delta,
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::RefusalDelta { delta } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                let record = self
                    .record_mut(&event.response_id)
                    .expect("record exists after ensure");
                append_styled_text_delta(
                    record,
                    event.timestamp_ms,
                    &event.response_id,
                    DebugSegmentKind::Output,
                    delta,
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::FunctionCallArgumentsDelta { call_id, delta } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                let record = self
                    .record_mut(&event.response_id)
                    .expect("record exists after ensure");
                append_function_call_delta(
                    record,
                    event.timestamp_ms,
                    &event.response_id,
                    call_id,
                    delta,
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::ToolPhase { phase, detail } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                if let Some(line) = tool_phase_line(phase, detail) {
                    let record = self
                        .record_mut(&event.response_id)
                        .expect("record exists after ensure");
                    append_segment_line(
                        record,
                        event.timestamp_ms,
                        &event.response_id,
                        DebugSegmentKind::Tool,
                        &line,
                        &mut messages,
                    );
                }
                self.push_timeline_event(
                    &event.response_id,
                    event.timestamp_ms,
                    phase,
                    detail.clone(),
                    None,
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::Completed => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                let record = self
                    .record_mut(&event.response_id)
                    .expect("record exists after ensure");
                flush_pending_output_backslash(
                    record,
                    event.timestamp_ms,
                    &event.response_id,
                    &mut messages,
                );
                record.active_function_call_id = None;
                record.request.status = DebugRequestStatus::Completed;
                record.request.completed_at_ms = Some(event.timestamp_ms);
                record.request.updated_at_ms = event.timestamp_ms;
                messages.push(DebugWsMessage::RequestStatus {
                    response_id: event.response_id.clone(),
                    status: DebugRequestStatus::Completed,
                    completed_at_ms: Some(event.timestamp_ms),
                    error: None,
                });
                self.push_timeline_event(
                    &event.response_id,
                    event.timestamp_ms,
                    "completed",
                    "completed".to_string(),
                    None,
                    &mut messages,
                );
                messages
            }
            MonitorEventKind::Failed { message } => {
                let mut messages = Vec::new();
                self.ensure_record_message(&event.response_id, event.timestamp_ms, &mut messages);
                let record = self
                    .record_mut(&event.response_id)
                    .expect("record exists after ensure");
                flush_pending_output_backslash(
                    record,
                    event.timestamp_ms,
                    &event.response_id,
                    &mut messages,
                );
                record.active_function_call_id = None;
                record.request.status = DebugRequestStatus::Failed;
                record.request.completed_at_ms = Some(event.timestamp_ms);
                record.request.updated_at_ms = event.timestamp_ms;
                record.request.error = Some(message.clone());
                append_segment_line(
                    record,
                    event.timestamp_ms,
                    &event.response_id,
                    DebugSegmentKind::Tool,
                    &format!("failed: {message}"),
                    &mut messages,
                );
                messages.push(DebugWsMessage::RequestStatus {
                    response_id: event.response_id.clone(),
                    status: DebugRequestStatus::Failed,
                    completed_at_ms: Some(event.timestamp_ms),
                    error: Some(message.clone()),
                });
                self.push_timeline_event(
                    &event.response_id,
                    event.timestamp_ms,
                    "failed",
                    message.clone(),
                    None,
                    &mut messages,
                );
                messages
            }
        }
    }

    fn prune_expired(&mut self, now_ms: u128) -> Vec<DebugWsMessage> {
        let cutoff = now_ms.saturating_sub(DEBUG_HISTORY_RETENTION_MS);
        let mut messages = Vec::new();
        let mut index = 0;
        while index < self.records.len() {
            let should_remove = self
                .records
                .get(index)
                .is_some_and(|record| record.request.updated_at_ms < cutoff);
            if should_remove {
                if let Some(record) = self.records.remove(index) {
                    messages.push(DebugWsMessage::RequestRemove {
                        response_id: record.request.response_id,
                        reason: "expired".to_string(),
                    });
                }
            } else {
                index += 1;
            }
        }
        messages
    }

    fn start_record(&mut self, event: &MonitorEvent, model: String, stats: DebugRequestStats) {
        if let Some(index) = self
            .records
            .iter()
            .position(|record| record.request.response_id == event.response_id)
        {
            let _ = self.records.remove(index);
        }
        while self.records.len() >= self.history_limit {
            let _ = self.records.pop_back();
        }
        self.records.push_front(DebugRequestRecord {
            request: DebugRequest {
                response_id: event.response_id.clone(),
                model,
                started_at_ms: event.timestamp_ms,
                updated_at_ms: event.timestamp_ms,
                completed_at_ms: None,
                status: DebugRequestStatus::Running,
                stats,
                error: None,
            },
            segments: VecDeque::new(),
            events: VecDeque::new(),
            active_function_call_id: None,
            pending_output_backslash: false,
        });
    }

    fn ensure_record_message(
        &mut self,
        response_id: &str,
        timestamp_ms: u128,
        messages: &mut Vec<DebugWsMessage>,
    ) {
        if self
            .records
            .iter()
            .any(|record| record.request.response_id == response_id)
        {
            if let Some(record) = self.record_mut(response_id) {
                record.request.updated_at_ms = timestamp_ms;
            }
            return;
        }
        while self.records.len() >= self.history_limit {
            let _ = self.records.pop_back();
        }
        self.records.push_front(DebugRequestRecord {
            request: DebugRequest {
                response_id: response_id.to_string(),
                model: String::new(),
                started_at_ms: timestamp_ms,
                updated_at_ms: timestamp_ms,
                completed_at_ms: None,
                status: DebugRequestStatus::Running,
                stats: DebugRequestStats::default(),
                error: None,
            },
            segments: VecDeque::new(),
            events: VecDeque::new(),
            active_function_call_id: None,
            pending_output_backslash: false,
        });
        messages.push(DebugWsMessage::RequestUpsert {
            request: self
                .records
                .front()
                .expect("record inserted")
                .request
                .clone(),
        });
    }

    fn record_mut(&mut self, response_id: &str) -> Option<&mut DebugRequestRecord> {
        self.records
            .iter_mut()
            .find(|record| record.request.response_id == response_id)
    }

    fn push_timeline_event(
        &mut self,
        response_id: &str,
        timestamp_ms: u128,
        kind: &str,
        summary: String,
        payload_preview: Option<String>,
        messages: &mut Vec<DebugWsMessage>,
    ) {
        self.push_timeline_event_with_images(
            response_id,
            timestamp_ms,
            kind,
            summary,
            payload_preview,
            Vec::new(),
            messages,
        );
    }

    fn push_timeline_event_with_images(
        &mut self,
        response_id: &str,
        timestamp_ms: u128,
        kind: &str,
        summary: String,
        payload_preview: Option<String>,
        images: Vec<DebugEventImage>,
        messages: &mut Vec<DebugWsMessage>,
    ) {
        let event = DebugTimelineEvent {
            timestamp_ms,
            kind: kind.to_string(),
            summary,
            payload_preview,
            images,
        };
        if let Some(record) = self.record_mut(response_id) {
            record.request.updated_at_ms = timestamp_ms;
            record.events.push_back(event.clone());
            while record.events.len() > REQUEST_EVENT_LIMIT {
                let _ = record.events.pop_front();
            }
        }
        messages.push(DebugWsMessage::EventAppend {
            response_id: response_id.to_string(),
            event,
        });
    }
}

fn append_output_text_delta(
    record: &mut DebugRequestRecord,
    timestamp_ms: u128,
    response_id: &str,
    delta: &str,
    messages: &mut Vec<DebugWsMessage>,
) {
    prepare_for_styled_delta(
        record,
        timestamp_ms,
        response_id,
        DebugSegmentKind::Output,
        messages,
    );
    let normalized = normalize_output_newlines(record, delta);
    append_segment_text(
        record,
        timestamp_ms,
        response_id,
        DebugSegmentKind::Output,
        &normalized,
        messages,
    );
}

fn append_styled_text_delta(
    record: &mut DebugRequestRecord,
    timestamp_ms: u128,
    response_id: &str,
    kind: DebugSegmentKind,
    delta: &str,
    messages: &mut Vec<DebugWsMessage>,
) {
    if kind != DebugSegmentKind::Output {
        flush_pending_output_backslash(record, timestamp_ms, response_id, messages);
    }
    prepare_for_styled_delta(record, timestamp_ms, response_id, kind, messages);
    append_segment_text(record, timestamp_ms, response_id, kind, delta, messages);
}

fn prepare_for_styled_delta(
    record: &mut DebugRequestRecord,
    timestamp_ms: u128,
    response_id: &str,
    kind: DebugSegmentKind,
    messages: &mut Vec<DebugWsMessage>,
) {
    if record.active_function_call_id.take().is_some() {
        ensure_newline(record, timestamp_ms, response_id, kind, messages);
    } else {
        ensure_newline_after_kind_change(record, timestamp_ms, response_id, kind, messages);
    }
}

fn append_function_call_delta(
    record: &mut DebugRequestRecord,
    timestamp_ms: u128,
    response_id: &str,
    call_id: &str,
    delta: &str,
    messages: &mut Vec<DebugWsMessage>,
) {
    flush_pending_output_backslash(record, timestamp_ms, response_id, messages);
    if record.active_function_call_id.as_deref() != Some(call_id) {
        ensure_newline(
            record,
            timestamp_ms,
            response_id,
            DebugSegmentKind::Tool,
            messages,
        );
        append_segment_text(
            record,
            timestamp_ms,
            response_id,
            DebugSegmentKind::Tool,
            &format!("tool arguments {}:\n", short_id(call_id)),
            messages,
        );
        record.active_function_call_id = Some(call_id.to_string());
    }
    append_segment_text(
        record,
        timestamp_ms,
        response_id,
        DebugSegmentKind::Tool,
        delta,
        messages,
    );
}

fn append_segment_line(
    record: &mut DebugRequestRecord,
    timestamp_ms: u128,
    response_id: &str,
    kind: DebugSegmentKind,
    line: &str,
    messages: &mut Vec<DebugWsMessage>,
) {
    flush_pending_output_backslash(record, timestamp_ms, response_id, messages);
    record.active_function_call_id = None;
    ensure_newline(record, timestamp_ms, response_id, kind, messages);
    append_segment_text(record, timestamp_ms, response_id, kind, line, messages);
    append_segment_text(record, timestamp_ms, response_id, kind, "\n", messages);
}

fn ensure_newline(
    record: &mut DebugRequestRecord,
    timestamp_ms: u128,
    response_id: &str,
    kind: DebugSegmentKind,
    messages: &mut Vec<DebugWsMessage>,
) {
    if !segments_text_ends_with_newline(record) && !record.segments.is_empty() {
        append_segment_text(record, timestamp_ms, response_id, kind, "\n", messages);
    }
}

fn ensure_newline_after_kind_change(
    record: &mut DebugRequestRecord,
    timestamp_ms: u128,
    response_id: &str,
    kind: DebugSegmentKind,
    messages: &mut Vec<DebugWsMessage>,
) {
    if !record.segments.is_empty()
        && !segments_text_ends_with_newline(record)
        && last_segment_kind(record) != Some(kind)
    {
        append_segment_text(record, timestamp_ms, response_id, kind, "\n", messages);
    }
}

fn append_segment_text(
    record: &mut DebugRequestRecord,
    timestamp_ms: u128,
    response_id: &str,
    kind: DebugSegmentKind,
    text: &str,
    messages: &mut Vec<DebugWsMessage>,
) {
    if text.is_empty() {
        return;
    }

    record.request.updated_at_ms = timestamp_ms;
    match record.segments.back_mut() {
        Some(segment) if segment.kind == kind => segment.text.push_str(text),
        _ => record.segments.push_back(DebugSegment {
            timestamp_ms,
            kind,
            text: text.to_string(),
        }),
    }
    trim_segment_prefix(record);
    messages.push(DebugWsMessage::SegmentAppend {
        response_id: response_id.to_string(),
        segment: DebugSegment {
            timestamp_ms,
            kind,
            text: text.to_string(),
        },
    });
}

fn trim_segment_prefix(record: &mut DebugRequestRecord) {
    let char_count: usize = record
        .segments
        .iter()
        .map(|segment| segment.text.chars().count())
        .sum();
    if char_count <= REQUEST_TEXT_CHAR_LIMIT {
        return;
    }
    let mut drain_chars = char_count - REQUEST_TEXT_CHAR_LIMIT;
    while drain_chars > 0 {
        let Some(front) = record.segments.front_mut() else {
            return;
        };
        let segment_chars = front.text.chars().count();
        if drain_chars >= segment_chars {
            drain_chars -= segment_chars;
            let _ = record.segments.pop_front();
            continue;
        }

        drain_string_prefix_chars(&mut front.text, drain_chars);
        if front.text.is_empty() {
            let _ = record.segments.pop_front();
        }
        return;
    }
}

fn drain_string_prefix_chars(buffer: &mut String, drain_chars: usize) {
    if drain_chars == 0 {
        return;
    }
    let drain_len = buffer
        .char_indices()
        .nth(drain_chars)
        .map(|(index, _)| index)
        .unwrap_or(buffer.len());
    buffer.drain(..drain_len);
}

fn normalize_output_newlines(record: &mut DebugRequestRecord, delta: &str) -> String {
    let mut output = String::new();
    let mut chars = delta.chars();

    if record.pending_output_backslash {
        record.pending_output_backslash = false;
        match chars.next() {
            Some('n') => output.push('\n'),
            Some(ch) => {
                output.push('\\');
                output.push(ch);
            }
            None => {
                record.pending_output_backslash = true;
                return output;
            }
        }
    }

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => output.push('\n'),
                Some(next) => {
                    output.push('\\');
                    output.push(next);
                }
                None => record.pending_output_backslash = true,
            }
        } else {
            output.push(ch);
        }
    }

    output
}

fn flush_pending_output_backslash(
    record: &mut DebugRequestRecord,
    timestamp_ms: u128,
    response_id: &str,
    messages: &mut Vec<DebugWsMessage>,
) {
    if record.pending_output_backslash {
        record.pending_output_backslash = false;
        append_segment_text(
            record,
            timestamp_ms,
            response_id,
            DebugSegmentKind::Output,
            "\\",
            messages,
        );
    }
}

fn segments_text_ends_with_newline(record: &DebugRequestRecord) -> bool {
    record
        .segments
        .iter()
        .rev()
        .find(|segment| !segment.text.is_empty())
        .is_some_and(|segment| segment.text.ends_with('\n'))
}

fn last_segment_kind(record: &DebugRequestRecord) -> Option<DebugSegmentKind> {
    record
        .segments
        .iter()
        .rev()
        .find(|segment| !segment.text.is_empty())
        .map(|segment| segment.kind)
}

fn tool_phase_line(phase: &str, detail: &str) -> Option<String> {
    match phase {
        "provider_tool_detected" => None,
        "client_tool_handoff"
        | "client_tool_result"
        | "provider_tool_running"
        | "provider_tool_completed" => Some(detail.to_string()),
        _ => Some(format!("{phase}: {detail}")),
    }
}

fn short_id(response_id: &str) -> String {
    const LIMIT: usize = 18;
    // `response_id` is an upstream-controlled tool-call id; truncate on a CHAR
    // boundary so a non-ASCII id >18 bytes cannot panic the debug monitor
    // (round-6 #2). `char_indices().nth(LIMIT)` gives the byte offset of the
    // LIMIT-th char (a valid boundary); `None` means the id has <= LIMIT chars.
    match response_id.char_indices().nth(LIMIT) {
        Some((byte_idx, _)) => format!("{}...", &response_id[..byte_idx]),
        None => response_id.to_string(),
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::DEBUG_HISTORY_RETENTION_MS;
    use super::DebugRequestStatus;
    use super::DebugSegmentKind;
    use super::DebugWsMessage;
    use super::MonitorEvent;
    use super::MonitorEventKind;
    use super::MonitorHub;
    use super::MonitorState;
    use std::collections::VecDeque;

    #[test]
    fn snapshot_replays_history_as_websocket_messages() {
        let hub = MonitorHub::new(8);
        hub.emit("resp_1", started("model-a"));
        hub.emit(
            "resp_1",
            MonitorEventKind::OutputTextDelta {
                delta: "hello".to_string(),
            },
        );
        hub.emit("resp_1", MonitorEventKind::Completed);

        let snapshot = hub.snapshot();
        assert!(snapshot.last_sequence >= 3);
        assert!(matches!(
            snapshot.messages.first(),
            Some(DebugWsMessage::Hello { .. })
        ));
        assert!(matches!(
            snapshot.messages.last(),
            Some(DebugWsMessage::SnapshotDone)
        ));
        assert!(snapshot.messages.iter().any(|message| matches!(
            message,
            DebugWsMessage::RequestUpsert { request }
                if request.response_id == "resp_1" && request.status == DebugRequestStatus::Completed
        )));
        assert!(snapshot.messages.iter().any(|message| matches!(
            message,
            DebugWsMessage::SegmentAppend { response_id, segment }
                if response_id == "resp_1"
                    && segment.kind == DebugSegmentKind::Output
                    && segment.text == "hello"
        )));
    }

    #[test]
    fn output_text_decodes_literal_newline_sequences() {
        let hub = MonitorHub::new(8);
        hub.emit("resp_1", started("model-a"));
        hub.emit(
            "resp_1",
            MonitorEventKind::OutputTextDelta {
                delta: "first\\nsecond".to_string(),
            },
        );
        hub.emit(
            "resp_1",
            MonitorEventKind::OutputTextDelta {
                delta: "\\".to_string(),
            },
        );
        hub.emit(
            "resp_1",
            MonitorEventKind::OutputTextDelta {
                delta: "nthird".to_string(),
            },
        );
        hub.emit("resp_1", MonitorEventKind::Completed);

        let text = hub
            .snapshot()
            .messages
            .into_iter()
            .filter_map(|message| match message {
                DebugWsMessage::SegmentAppend { segment, .. } => Some(segment.text),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(text, "first\nsecond\nthird");
    }

    #[test]
    fn debug_ws_segments_redact_image_uris_in_snapshot() {
        // Round-6 #1: output/tool segment text echoing a data:/signed image URL
        // must be redacted in the /debug/ws snapshot (and broadcast).
        let hub = MonitorHub::new(8);
        hub.emit("resp_1", started("model-a"));
        hub.emit(
            "resp_1",
            MonitorEventKind::OutputTextDelta {
                delta:
                    "see data:image/png;base64,SEGLEAKDATA and https://signed.x/i?sig=SEGSIGLEAK"
                        .to_string(),
            },
        );
        hub.emit(
            "resp_1",
            MonitorEventKind::ToolPhase {
                phase: "provider_tool_running".to_string(),
                detail: "fetched data:image/jpeg;base64,TOOLLEAK".to_string(),
            },
        );
        hub.emit("resp_1", MonitorEventKind::Completed);

        let snapshot = hub.snapshot();
        let dumped = serde_json::to_string(&snapshot.messages).expect("serialize");
        assert!(
            !dumped.contains("SEGLEAKDATA"),
            "output data: payload redacted"
        );
        assert!(
            !dumped.contains("SEGSIGLEAK"),
            "output signed-url token redacted"
        );
        assert!(!dumped.contains("TOOLLEAK"), "tool data: payload redacted");
        assert!(dumped.contains("<redacted uri>"));
        // Non-image text survives.
        assert!(dumped.contains("see "));
    }

    #[test]
    fn debug_ws_broadcast_redacts_image_uris_live() {
        // Round-6 #1: the LIVE broadcast (not just snapshot) is redacted too.
        let hub = MonitorHub::new(8);
        let mut rx = hub.subscribe();
        hub.emit("resp_live", started("model-a"));
        hub.emit(
            "resp_live",
            MonitorEventKind::OutputTextDelta {
                delta: "x data:image/png;base64,LIVELEAK y".to_string(),
            },
        );
        let mut saw_redaction = false;
        // Drain the queued updates and assert no raw payload appears anywhere.
        while let Ok(update) = rx.try_recv() {
            let dumped = serde_json::to_string(&update.messages).expect("serialize");
            assert!(
                !dumped.contains("LIVELEAK"),
                "live broadcast must redact data:"
            );
            if dumped.contains("<redacted uri>") {
                saw_redaction = true;
            }
        }
        assert!(
            saw_redaction,
            "expected a redacted segment in the live broadcast"
        );
    }

    #[test]
    fn short_id_truncates_multibyte_ids_without_panic() {
        // Round-6 #2: an upstream tool-call id with multibyte chars >18 bytes
        // must truncate on a CHAR boundary, not panic on a byte slice.
        let multibyte = "café_☕_tool_call_überlang_id_0123456789"; // many bytes, >18 chars
        let short = super::short_id(multibyte);
        assert!(short.ends_with("..."));
        assert!(short.is_char_boundary(short.len()));
        // First 18 CHARS preserved.
        let expected: String = multibyte.chars().take(18).collect();
        assert_eq!(short, format!("{expected}..."));

        // A short ascii id is returned unchanged.
        assert_eq!(super::short_id("call_123"), "call_123");

        // An id with EXACTLY 18 multibyte chars is returned unchanged (no panic).
        let exactly_18: String = "é".repeat(18);
        assert_eq!(super::short_id(&exactly_18), exactly_18);
    }

    #[test]
    fn function_call_delta_with_multibyte_id_does_not_panic() {
        // End-to-end: a function-call delta whose call_id is a long multibyte id
        // flows through append_function_call_delta -> short_id without panic.
        let hub = MonitorHub::new(8);
        hub.emit("resp_1", started("model-a"));
        hub.emit(
            "resp_1",
            MonitorEventKind::FunctionCallArgumentsDelta {
                call_id: "café_☕_überlang_call_id_9876543210".to_string(),
                delta: "{\"q\":1}".to_string(),
            },
        );
        hub.emit("resp_1", MonitorEventKind::Completed);
        // Snapshot must build without panicking.
        let _ = hub.snapshot();
    }

    #[test]
    fn stale_records_expire_after_retention_window() {
        let mut state = MonitorState {
            history_limit: 8,
            last_sequence: 0,
            records: VecDeque::new(),
        };
        state.apply_event(&event_at(1, "old", 0, started("old-model")));
        state.apply_event(&event_at(
            2,
            "recent",
            DEBUG_HISTORY_RETENTION_MS,
            started("recent-model"),
        ));

        let removals = state.prune_expired(DEBUG_HISTORY_RETENTION_MS + 2);

        assert_eq!(state.records.len(), 1);
        assert_eq!(state.records[0].request.response_id, "recent");
        assert!(matches!(
            removals.as_slice(),
            [DebugWsMessage::RequestRemove {
                response_id,
                reason
            }] if response_id == "old" && reason == "expired"
        ));
    }

    fn started(model: &str) -> MonitorEventKind {
        MonitorEventKind::RequestStarted {
            model: model.to_string(),
            input_items: 1,
            tool_count: 0,
            turn_count: 1,
            user_messages: 1,
            assistant_messages: 0,
            system_messages: 0,
            developer_messages: 0,
            reasoning_items: 0,
            function_calls: 0,
            function_outputs: 0,
            tool_items: 0,
            input_chars: 5,
            instructions_chars: 0,
        }
    }

    fn event_at(
        sequence: u64,
        response_id: &str,
        timestamp_ms: u128,
        kind: MonitorEventKind,
    ) -> MonitorEvent {
        MonitorEvent {
            sequence,
            response_id: response_id.to_string(),
            timestamp_ms,
            kind,
        }
    }
}
