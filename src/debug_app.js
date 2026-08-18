  // ------------------------------------------------------------------------
  // Constants & utilities
  // ------------------------------------------------------------------------

  const DASHBOARD_KEY = "__dashboard__";
  const TILE_LINGER_MS = 30_000; // must match CSS opacity transition on request-tile

  const escapeHtml = (value) => String(value ?? "")
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#039;");

  const shortId = (id) => (id && id.length > 22) ? `${id.slice(0, 22)}...` : (id ?? "");

  const formatCount = (value) => Number(value || 0).toLocaleString();

  const formatTime = (ms) => ms ? new Date(Number(ms)).toLocaleTimeString() : "";

  const formatDuration = (request) => {
    const start = Number(request.started_at_ms || 0);
    if (!start) return "";
    const end = Number(request.completed_at_ms || Date.now());
    const seconds = Math.max(0, (end - start) / 1000);
    return seconds < 10 ? `${seconds.toFixed(1)}s` : `${Math.round(seconds)}s`;
  };

  const formatBytes = (value) => {
    const bytes = Number(value || 0);
    if (!bytes) return "unknown size";
    if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
    if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
    return `${bytes} B`;
  };

  const formatCompact = (value) => {
    const n = Number(value || 0);
    if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
    if (n >= 10e3) return `${Math.round(n / 1e3)}k`;
    if (n >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
    return String(n);
  };

  // Segments index where the current round's live stream begins, derived from
  // timestamps: the first run starting at/after the newest payload event.
  // Used after snapshot replay, where event-vs-segment ordering is lost.
  function computeLiveSegmentsFrom(request) {
    let lastPayloadTs = 0;
    for (const event of request.events || []) {
      if (!event || !event.payload_preview) continue;
      if (event.kind !== "upstream_request" && event.kind !== "request_payload") continue;
      const ts = Number(event.timestamp_ms || 0);
      if (ts > lastPayloadTs) lastPayloadTs = ts;
    }
    const segments = request.segments || [];
    if (!lastPayloadTs) return 0;
    for (let i = 0; i < segments.length; i++) {
      if (Number(segments[i].timestamp_ms || 0) >= lastPayloadTs) return i;
    }
    return segments.length;
  }

  // ------------------------------------------------------------------------
  // Tolerant payload parsing. Server previews elide long string leaves but can
  // still be hard-truncated (`...\n[truncated]` suffix). A strict JSON.parse
  // fails on those, which used to silently drop the ENTIRE conversation from
  // the Messages tab. The repair pass closes an unterminated string, drops a
  // dangling partial token, and closes open brackets, then parses again.
  // ------------------------------------------------------------------------

  const TRUNCATION_MARKER = "...\n[truncated]";

  function parseJsonTolerant(raw) {
    if (!raw) return { value: null, truncated: false };
    try { return { value: JSON.parse(raw), truncated: false }; } catch (_) { /* repair below */ }
    let text = raw;
    if (text.endsWith(TRUNCATION_MARKER)) text = text.slice(0, -TRUNCATION_MARKER.length);
    for (let attempt = 0; attempt < 4; attempt++) {
      const repaired = repairTruncatedJson(text);
      if (repaired !== null) {
        try { return { value: JSON.parse(repaired), truncated: true }; } catch (_) { /* chop */ }
      }
      // The cut landed somewhere the repair pass couldn't classify: chop back
      // to before the last comma and retry. Previews are pretty-printed, so
      // commas at value boundaries dominate.
      const cut = text.lastIndexOf(",");
      if (cut <= 0) break;
      text = text.slice(0, cut);
    }
    return { value: null, truncated: true };
  }

  function repairTruncatedJson(text) {
    // One scan tracking string/escape state and the open-bracket stack.
    const stack = [];
    let inString = false;
    let escape = false;
    for (let i = 0; i < text.length; i++) {
      const ch = text[i];
      if (inString) {
        if (escape) { escape = false; continue; }
        if (ch === "\\") { escape = true; continue; }
        if (ch === '"') inString = false;
        continue;
      }
      if (ch === '"') inString = true;
      else if (ch === "{" || ch === "[") stack.push(ch);
      else if (ch === "}" || ch === "]") stack.pop();
    }
    if (!stack.length && !inString) return null; // complete JSON; not a truncation problem
    let out = text;
    if (inString) {
      if (escape) out = out.slice(0, -1); // dangling backslash
      out = out.replace(/\\u[0-9a-fA-F]{0,3}$/, ""); // partial \uXXXX escape
      out += '"';
    }
    // Normalize the tail to a clean value boundary.
    for (let guard = 0; guard < 16; guard++) {
      const trimmed = out.replace(/\s+$/, "");
      if (trimmed.endsWith(",")) { out = trimmed.slice(0, -1); continue; }
      if (trimmed.endsWith(":")) { out = trimmed + " null"; break; }
      if (trimmed.endsWith('"')) {
        // A complete string at the tail. In KEY position (preceded by `{` or
        // `,`) it needs a value for the closed object to parse.
        const start = findTailStringStart(trimmed);
        if (start > 0) {
          const before = trimmed.slice(0, start).replace(/\s+$/, "");
          if (before.endsWith("{") || before.endsWith(",")) { out = trimmed + ": null"; break; }
        }
        out = trimmed;
        break;
      }
      // A partial bare token (`tru`, `nul`, `-12.`, `1e`): drop it, re-check.
      const token = trimmed.match(/[A-Za-z0-9+\-.eE]+$/);
      if (token
              && !/^(true|false|null)$/.test(token[0])
              && !/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(token[0])) {
        out = trimmed.slice(0, trimmed.length - token[0].length);
        continue;
      }
      out = trimmed;
      break;
    }
    for (let i = stack.length - 1; i >= 0; i--) out += stack[i] === "{" ? "}" : "]";
    return out;
  }

  // Index of the opening quote of the string literal that ENDS `text` (which
  // must end with `"`), or -1. Walks back skipping escaped quotes.
  function findTailStringStart(text) {
    for (let i = text.length - 2; i >= 0; i--) {
      if (text[i] !== '"') continue;
      let backslashes = 0;
      for (let j = i - 1; j >= 0 && text[j] === "\\"; j--) backslashes++;
      if (backslashes % 2 === 0) return i;
    }
    return -1;
  }

  // ------------------------------------------------------------------------
  // Conversation extraction: turns a request/upstream payload preview into a
  // uniform list of turn items (message / tool_call / tool_result), covering
  // OpenAI Chat Completions, Anthropic Messages, and OpenAI Responses shapes —
  // including reasoning carried as Responses `type:"reasoning"` input items,
  // chat-shape assistant `reasoning_content`, and Anthropic `thinking` blocks.
  // ------------------------------------------------------------------------

  function payloadEventCandidates(request) {
    const events = request.events || [];
    const upstream = [];
    const client = [];
    for (let i = events.length - 1; i >= 0; i--) {
      const event = events[i];
      if (!event || !event.payload_preview) continue;
      if (event.kind === "upstream_request") upstream.push(event);
      else if (event.kind === "request_payload") client.push(event);
    }
    // Newest upstream payloads first (they carry the full lowered history),
    // then client payloads as fallback.
    return [...upstream, ...client];
  }

  function parseConversationEvent(event) {
    if (!event) return null;
    const { value, truncated } = parseJsonTolerant(event.payload_preview);
    if (!value || typeof value !== "object") return null;
    const convo = conversationFromPayload(value);
    if (!convo) return null;
    convo.truncated = truncated;
    convo.event = event;
    return convo;
  }

  function flattenTextDeep(value) {
    if (value === null || value === undefined) return "";
    if (typeof value === "string") return value;
    if (!Array.isArray(value)) {
      if (typeof value === "object" && typeof value.text === "string") return value.text;
      return "";
    }
    let out = "";
    const push = (s) => {
      if (!s) return;
      if (out && !out.endsWith("\n")) out += "\n";
      out += s;
    };
    for (const item of value) {
      if (typeof item === "string") push(item);
      else if (item && typeof item === "object" && typeof item.text === "string") push(item.text);
    }
    return out;
  }

  // Reasoning text from a Responses `type:"reasoning"` item: summary parts
  // and/or content parts, whichever the provider populated.
  function reasoningItemText(item) {
    const parts = [];
    for (const list of [item.summary, item.content]) {
      if (!Array.isArray(list)) continue;
      for (const block of list) {
        if (block && typeof block === "object" && typeof block.text === "string" && block.text) {
          parts.push(block.text);
        } else if (typeof block === "string" && block) {
          parts.push(block);
        }
      }
    }
    return parts.join("\n");
  }

  function conversationFromPayload(parsed) {
    let system = "";
    if (typeof parsed.system === "string") {
      system = parsed.system;
    } else if (Array.isArray(parsed.system)) {
      system = flattenTextDeep(parsed.system);
    }

    const items = [];
    const pushReasoning = (text) => {
      items.push({ kind: "message", role: "reasoning", content: text || "[encrypted reasoning]" });
    };
    const pushFromContent = (role, content) => {
      if (content === null || content === undefined) return;
      if (typeof content === "string") {
        if (content) items.push({ kind: "message", role, content });
        return;
      }
      const blocks = Array.isArray(content) ? content : [content];
      let textBuf = "";
      const appendText = (text) => {
        if (!text) return;
        if (textBuf) textBuf += "\n";
        textBuf += text;
      };
      const flushText = () => {
        if (textBuf) {
          items.push({ kind: "message", role, content: textBuf });
          textBuf = "";
        }
      };
      for (const block of blocks) {
        if (typeof block === "string") { appendText(block); continue; }
        if (!block || typeof block !== "object") continue;
        const t = block.type;
        if (t === "text" || t === "input_text" || t === "output_text" || typeof block.text === "string") {
          appendText(block.text || "");
        } else if (t === "thinking" || t === "redacted_thinking") {
          flushText();
          const thinking = typeof block.thinking === "string" && block.thinking
                  ? block.thinking
                  : "[redacted thinking]";
          pushReasoning(thinking);
        } else if (t === "tool_use") {
          flushText();
          items.push({
            kind: "tool_call",
            name: block.name || "",
            call_id: block.id || "",
            arguments: block.input,
          });
        } else if (t === "tool_result") {
          flushText();
          items.push({
            kind: "tool_result",
            call_id: block.tool_use_id || "",
            output: block.content !== undefined ? block.content : block.output,
          });
        } else if (t === "refusal") {
          appendText(`[refusal] ${block.refusal || ""}`);
        } else if (t === "image" || t === "input_image") {
          appendText("[image]");
        } else {
          appendText(`[${t || "block"}]`);
        }
      }
      flushText();
    };

    if (Array.isArray(parsed.messages)) {
      // Anthropic and OpenAI Chat Completions: top-level messages[].
      for (const msg of parsed.messages) {
        if (!msg) continue;
        const role = typeof msg.role === "string" ? msg.role : "user";
        if (role === "system" || role === "developer") {
          if (!system) {
            const text = typeof msg.content === "string" ? msg.content : flattenTextDeep(msg.content);
            if (text) system = text;
          }
          continue;
        }
        if (role === "tool") {
          // Chat Completions tool result message.
          items.push({
            kind: "tool_result",
            call_id: msg.tool_call_id || "",
            output: msg.content !== undefined ? msg.content : msg.output,
          });
          continue;
        }
        if (role === "assistant") {
          // Reasoning lowered onto the chat assistant message (DeepSeek-style
          // `reasoning_content`, or a bare `reasoning`/`thinking` string).
          const reasoning = [msg.reasoning_content, msg.reasoning, msg.thinking]
                  .find(v => typeof v === "string" && v);
          if (reasoning) pushReasoning(reasoning);
        }
        pushFromContent(role, msg.content);
        if (Array.isArray(msg.tool_calls)) {
          for (const tc of msg.tool_calls) {
            if (!tc) continue;
            const fn = tc.function || {};
            let args = fn.arguments;
            if (typeof args === "string") {
              try { args = JSON.parse(args); } catch (_) { /* keep string */ }
            }
            items.push({
              kind: "tool_call",
              name: fn.name || tc.name || "",
              call_id: tc.id || "",
              arguments: args,
            });
          }
        }
      }
    } else if (parsed.input !== undefined) {
      // OpenAI Responses: `input` is a string or an array of typed items.
      if (Array.isArray(parsed.input)) {
        for (const item of parsed.input) {
          if (!item) continue;
          if (typeof item === "string") {
            items.push({ kind: "message", role: "user", content: item });
            continue;
          }
          if (typeof item !== "object") continue;
          const t = item.type;
          if (t === "reasoning") {
            pushReasoning(reasoningItemText(item));
          } else if (t === "function_call") {
            let args = item.arguments;
            if (typeof args === "string") {
              try { args = JSON.parse(args); } catch (_) { /* keep string */ }
            }
            items.push({
              kind: "tool_call",
              name: item.name || "",
              call_id: item.call_id || item.id || "",
              arguments: args,
            });
          } else if (t === "function_call_output") {
            let out = item.output;
            if (typeof out === "string") {
              try { out = JSON.parse(out); } catch (_) { /* keep string */ }
            }
            items.push({
              kind: "tool_result",
              call_id: item.call_id || "",
              output: out,
            });
          } else if (t === "web_search_call") {
            items.push({
              kind: "tool_call",
              name: "web_search",
              call_id: item.id || "",
              arguments: item.action !== undefined ? item.action : item.query,
            });
          } else if (typeof item.role === "string") {
            if (item.role === "system" || item.role === "developer") {
              if (!system) {
                const text = typeof item.content === "string" ? item.content : flattenTextDeep(item.content);
                if (text) system = text;
              }
            } else {
              pushFromContent(item.role, item.content);
            }
          } else if (typeof item.text === "string") {
            items.push({ kind: "message", role: "user", content: item.text });
          } else {
            // Unknown typed item: surface it rather than silently dropping a
            // turn — this is a debugging tool.
            items.push({ kind: "message", role: "meta", content: `[${t || "item"}]` });
          }
        }
      } else if (typeof parsed.input === "string") {
        if (parsed.input) items.push({ kind: "message", role: "user", content: parsed.input });
      }
      if (typeof parsed.instructions === "string" && parsed.instructions && !system) {
        system = parsed.instructions;
      }
    } else {
      return null;
    }

    return { system, items };
  }

  // ------------------------------------------------------------------------
  // Span buffer: flattens monitor events into a kinded text stream that the
  // tile body renders. Mirrors the TUI's Tile/span model. Reasoning is kept
  // as its own kind so reasoning-block can group consecutive chunks.
  // ------------------------------------------------------------------------

  const tileBuffer = {
    appendSegment(request, segmentKind, text) {
      const kind = segmentKind === "reasoning" ? "reasoning"
              : segmentKind === "tool" ? "tool"
                      : "output";
      this._append(request, kind, text);
    },

    applyEvent(request, event) {
      const k = event.kind;
      if (k === "request_started" || k === "final_response") {
        this._ensureNewlineBefore(request, "meta");
        this._append(request, "meta", `[${event.summary || k}]\n`);
      } else if ((k === "request_payload" || k === "upstream_request") && event.payload_preview) {
        this._appendRequestPayload(request, event.payload_preview);
      }
    },

    _append(request, kind, text) {
      if (!text) return;
      const last = request.tileSpans[request.tileSpans.length - 1];
      if (last && last.kind === kind) {
        last.text += text;
      } else {
        request.tileSpans.push({ kind, text });
      }
      request.tileLastKind = kind;
    },

    _ensureNewlineBefore(request, kind) {
      if (request.tileLastKind === kind) return;
      const last = request.tileSpans[request.tileSpans.length - 1];
      if (last && !last.text.endsWith("\n")) last.text += "\n";
    },

    _appendRequestPayload(request, preview) {
      let parsed = null;
      try { parsed = JSON.parse(preview); } catch (_) { /* may be truncated */ }
      if (!parsed) {
        this._ensureNewlineBefore(request, "request");
        this._append(request, "request", preview);
        this._append(request, "request", "\n");
        return;
      }

      if (typeof parsed.temperature === "number") request.temperature = parsed.temperature;

      this._ensureNewlineBefore(request, "request");

      if (Array.isArray(parsed.messages)) {
        for (const msg of parsed.messages) {
          const role = typeof msg.role === "string" ? msg.role : "?";
          const content = this._extractContent(msg.content);
          if (!content) continue;
          this._append(request, "request", `${role}: ${content}`);
          if (!content.endsWith("\n")) this._append(request, "request", "\n");
        }
      } else if (parsed.input !== undefined) {
        const text = this._extractContent(parsed.input);
        if (text) {
          this._append(request, "request", text);
          if (!text.endsWith("\n")) this._append(request, "request", "\n");
        }
      }

      if (parsed.tools) {
        try {
          const pretty = JSON.stringify(parsed.tools, null, 2);
          if (pretty && pretty !== "[]" && pretty !== "null") {
            this._append(request, "request", "tools: ");
            this._append(request, "request", pretty);
            this._append(request, "request", "\n");
          }
        } catch (_) { /* ignore */ }
      }
    },

    _extractContent(value) {
      if (value === null || value === undefined) return "";
      if (typeof value === "string") return value;
      if (!Array.isArray(value)) return "";
      let out = "";
      const push = (s) => {
        if (!s) return;
        if (out && !out.endsWith("\n")) out += "\n";
        out += s;
      };
      for (const item of value) {
        if (typeof item === "string") push(item);
        else if (item && typeof item === "object") {
          if (typeof item.text === "string") push(item.text);
          else if (item.content !== undefined) push(this._extractContent(item.content));
          else if (typeof item.type === "string") push(`[${item.type}]`);
        }
      }
      return out;
    }
  };

  // ------------------------------------------------------------------------
  // Store: holds all request records and broadcasts change events. Custom
  // elements subscribe via the document-level event bus on `store`.
  // ------------------------------------------------------------------------

  class Store extends EventTarget {
    constructor() {
      super();
      this.requests = new Map();
      this.order = []; // newest first
      this.selectedId = DASHBOARD_KEY;
      this.retentionMs = 30 * 60 * 1000;
      this.searchQuery = "";
    }

    setSearchQuery(q) {
      const next = (q || "").trim();
      if (next === this.searchQuery) return;
      this.searchQuery = next;
      this._emit("search-changed");
    }

    matchesSearch(request) {
      const q = this.searchQuery.toLowerCase();
      if (!q) return true;
      if (request.response_id && request.response_id.toLowerCase().includes(q)) return true;
      if (request.model && request.model.toLowerCase().includes(q)) return true;
      for (const seg of request.segments || []) {
        if (seg.text && seg.text.toLowerCase().includes(q)) return true;
      }
      for (const ev of request.events || []) {
        if (ev.summary && ev.summary.toLowerCase().includes(q)) return true;
        if (ev.payload_preview && ev.payload_preview.toLowerCase().includes(q)) return true;
      }
      return false;
    }

    get(id) { return this.requests.get(id); }

    ensureRequest(id) {
      if (!this.requests.has(id)) {
        this.requests.set(id, {
          response_id: id,
          model: "",
          status: "running",
          stats: {},
          segments: [],
          events: [],
          tileSpans: [],
          tileLastKind: null,
          temperature: null,
          reasoningUserToggles: new Map(),
          // Detail-view local state: accordion open/close choices the user made
          // (keyed by section key) and the segments index where the CURRENT
          // upstream round's live stream begins (everything before it is
          // represented structurally in the latest payload event).
          detailToggles: new Map(),
          liveSegmentsFrom: 0,
          breakNextSegmentMerge: false
        });
        this.order.unshift(id);
      }
      return this.requests.get(id);
    }

    removeRequest(id) {
      this.requests.delete(id);
      this.order = this.order.filter(x => x !== id);
      if (this.selectedId === id) this.selectedId = DASHBOARD_KEY;
    }

    selectRequest(id) {
      if (this.selectedId === id) return;
      if (id !== DASHBOARD_KEY && !this.requests.has(id)) return;
      this.selectedId = id;
      this._emit("selection-changed");
      this._emit("list-changed");
    }

    pruneExpired() {
      const cutoff = Date.now() - this.retentionMs;
      let changed = false;
      for (const id of [...this.order]) {
        const request = this.requests.get(id);
        const lastUpdate = Number(request?.updated_at_ms || request?.completed_at_ms || request?.started_at_ms || 0);
        if (lastUpdate && lastUpdate < cutoff) {
          this.removeRequest(id);
          changed = true;
        }
      }
      return changed;
    }

    applyMessage(message) {
      switch (message.type) {
        case "hello":
          if (message.history_retention_ms) this.retentionMs = Number(message.history_retention_ms);
          this.requests.clear();
          this.order = [];
          // Suppress per-message emits until snapshot_done so an initial
          // burst of hundreds of upserts triggers a single render pass
          // instead of one per request.
          this._snapshotLoading = true;
          this._emit("list-changed");
          this._emit("selection-changed");
          return;

        case "snapshot_done":
          this._snapshotLoading = false;
          // Server may replay history in either direction; sort explicitly
          // so the sidebar always shows newest first.
          this.order.sort((a, b) => {
            const ra = this.requests.get(a);
            const rb = this.requests.get(b);
            return Number(rb?.started_at_ms || 0) - Number(ra?.started_at_ms || 0);
          });
          // The snapshot replays ALL segments before ALL events, so the live
          // event-order watermark couldn't be tracked during replay; recompute
          // it from timestamps (server-side merged segments keep their FIRST
          // chunk's timestamp, so run starts are comparable to event times).
          for (const request of this.requests.values()) {
            request.liveSegmentsFrom = computeLiveSegmentsFrom(request);
          }
          this._emit("list-changed");
          // A mounted detail view suppressed per-request emits during the
          // replay; wake it so it re-renders against the rebuilt record.
          if (this.selectedId !== DASHBOARD_KEY && this.requests.has(this.selectedId)) {
            this._emit("request-changed", { id: this.selectedId });
          }
          return;

        case "request_upsert": {
          const id = message.request.response_id;
          const exists = this.requests.has(id);
          const request = this.ensureRequest(id);
          // Don't clobber locally-tracked tile fields.
          const { tileSpans, tileLastKind, temperature, reasoningUserToggles } = request;
          Object.assign(request, message.request);
          request.tileSpans = tileSpans;
          request.tileLastKind = tileLastKind;
          request.temperature = temperature;
          request.reasoningUserToggles = reasoningUserToggles;
          request.segments ||= [];
          request.events ||= [];
          if (!exists) {
            this.order = [id, ...this.order.filter(x => x !== id)];
          }
          if (this._snapshotLoading) return;
          this._emit("list-changed");
          this._emit("request-changed", { id });
          return;
        }

        case "request_remove":
          this.removeRequest(message.response_id);
          this._emit("list-changed");
          this._emit("selection-changed");
          return;

        case "segment_append": {
          const id = message.response_id;
          const request = this.ensureRequest(id);
          request.updated_at_ms = message.segment.timestamp_ms || Date.now();
          const last = request.segments[request.segments.length - 1];
          if (last && last.kind === message.segment.kind && !request.breakNextSegmentMerge) {
            // Merge into the run but KEEP the run's first-chunk timestamp —
            // the detail view compares run starts against payload-event times.
            last.text += message.segment.text;
          } else {
            request.breakNextSegmentMerge = false;
            request.segments.push(message.segment);
          }
          tileBuffer.appendSegment(request, message.segment.kind, message.segment.text);
          if (this._snapshotLoading) return;
          this._emit("request-changed", { id });
          return;
        }

        case "event_append": {
          const id = message.response_id;
          const request = this.ensureRequest(id);
          request.updated_at_ms = message.event.timestamp_ms || Date.now();
          request.events.push(message.event);
          const kind = message.event.kind;
          if (kind === "upstream_request" || kind === "request_payload") {
            // Payload boundary: stream content after this point belongs to the
            // new round; everything before it is represented structurally in
            // this payload. Snapshot replay sends segments before events, so
            // only trust live ordering here (snapshot_done recomputes from
            // timestamps). Break the tail merge so the boundary is exact.
            if (!this._snapshotLoading) {
              request.liveSegmentsFrom = request.segments.length;
              request.breakNextSegmentMerge = true;
            }
          }
          tileBuffer.applyEvent(request, message.event);
          if (this._snapshotLoading) return;
          this._emit("request-changed", { id });
          return;
        }

        case "usage": {
          const id = message.response_id;
          const request = this.ensureRequest(id);
          request.usage = {
            prompt: Number(message.prompt || 0),
            completion: Number(message.completion || 0),
            total: Number(message.total || 0),
            cached: Number(message.cached || 0),
            reasoning: Number(message.reasoning || 0)
          };
          request.updated_at_ms = Date.now();
          if (this._snapshotLoading) return;
          this._emit("request-changed", { id });
          return;
        }

        case "request_status": {
          const id = message.response_id;
          const request = this.ensureRequest(id);
          request.status = message.status;
          request.completed_at_ms = message.completed_at_ms;
          request.updated_at_ms = message.completed_at_ms || Date.now();
          request.error = message.error;
          if (this._snapshotLoading) return;
          this._emit("list-changed");
          this._emit("request-changed", { id });
          return;
        }
      }
    }

    _emit(type, detail) {
      // While the tab is hidden, the WebSocket keeps streaming messages
      // into the store, but firing change events causes custom elements
      // to mutate the DOM and queue requestAnimationFrame callbacks
      // (FLIP animations, stick-to-bottom scrolling). Browsers throttle
      // rAF in hidden tabs, so those callbacks pile up and the
      // mutations are never painted. When the tab returns, everything
      // floods the main thread at once and the page appears frozen.
      //
      // Solution: coalesce changes while hidden, then dispatch a single
      // batch on visibilitychange. Data still flows into the store --
      // only the rendering is paused.
      if (document.hidden) {
        this._markDirty(type, detail);
        return;
      }
      this.dispatchEvent(new CustomEvent(type, { detail }));
    }

    _markDirty(type, detail) {
      const d = this._dirty ||= { list: false, selection: false, search: false, requests: new Set() };
      if (type === "list-changed") d.list = true;
      else if (type === "selection-changed") d.selection = true;
      else if (type === "search-changed") d.search = true;
      else if (type === "request-changed" && detail?.id) d.requests.add(detail.id);
    }

    flushDirty() {
      const d = this._dirty;
      if (!d) return;
      this._dirty = null;
      // Selection-changed first so the right-pane element is the
      // current one before list/request work runs against it. Search
      // re-filters the sidebar; list reconciles sidebar + dashboard
      // membership; per-request events then update surviving tiles
      // and the detail view.
      if (d.selection) this.dispatchEvent(new CustomEvent("selection-changed"));
      if (d.search) this.dispatchEvent(new CustomEvent("search-changed"));
      if (d.list) this.dispatchEvent(new CustomEvent("list-changed"));
      for (const id of d.requests) {
        this.dispatchEvent(new CustomEvent("request-changed", { detail: { id } }));
      }
    }
  }

  const store = new Store();

  // ------------------------------------------------------------------------
  // WebSocket connection (auto-reconnect, exposes status to <socket-indicator>)
  // ------------------------------------------------------------------------

  class DebugSocket extends EventTarget {
    constructor(store) {
      super();
      this.store = store;
    }
    start() { this._connect(); }
    _connect() {
      const scheme = location.protocol === "https:" ? "wss" : "ws";
      const ws = new WebSocket(`${scheme}://${location.host}/debug/ws`);
      this._setStatus("connecting", "connecting");
      ws.addEventListener("open", () => this._setStatus("open", "live"));
      ws.addEventListener("message", e => this.store.applyMessage(JSON.parse(e.data)));
      ws.addEventListener("close", () => {
        this._setStatus("closed", "closed");
        setTimeout(() => this._connect(), 1000);
      });
      ws.addEventListener("error", () => this._setStatus("error", "error"));
    }
    _setStatus(state, label) {
      this.dispatchEvent(new CustomEvent("status", { detail: { state, label } }));
    }
  }

  const socket = new DebugSocket(store);

  // ------------------------------------------------------------------------
  // <socket-indicator>: dot + label reflecting WS state.
  // ------------------------------------------------------------------------

  class SocketIndicator extends HTMLElement {
    static get observedAttributes() { return ["state"]; }
    connectedCallback() {
      if (!this._built) {
        this.innerHTML = '<span class="dot"></span>';
        this._built = true;
      }
      this._onStatus = (e) => {
        this.setAttribute("state", e.detail.state);
        this.title = `WebSocket: ${e.detail.label}`;
      };
      socket.addEventListener("status", this._onStatus);
    }
    disconnectedCallback() {
      socket.removeEventListener("status", this._onStatus);
    }
  }
  customElements.define("socket-indicator", SocketIndicator);

  // ------------------------------------------------------------------------
  // <request-list-item>: a single sidebar row. The dashboard pseudo-entry
  // uses the same element; it just has request-id="__dashboard__".
  // ------------------------------------------------------------------------

  class RequestListItem extends HTMLElement {
    static get observedAttributes() { return ["request-id", "selected"]; }
    connectedCallback() {
      if (!this._built) {
        const id = this.getAttribute("request-id");
        if (id === DASHBOARD_KEY) {
          this.innerHTML = `
              <button class="request-button dashboard" type="button">
                <div class="request-head">
                  <svg class="dashboard-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                    <path d="M3 12h4l2 -7 4 14 2 -7h6"></path>
                  </svg>
                  <span class="dashboard-label">Live Requests</span>
                  <span class="badge dashboard-count" data-role="count">0</span>
                </div>
              </button>
            `;
        } else {
          this.innerHTML = `
              <button class="request-button" type="button">
                <div class="request-head">
                  <span class="head-time" data-role="started"></span>
                  <span class="request-id" data-role="id"></span>
                  <span class="head-time" data-role="duration"></span>
                  <span class="badge" data-role="status"></span>
                </div>
                <div class="msg-preview user" data-role="user-preview"></div>
                <div class="msg-preview assistant" data-role="assistant-preview"></div>
                <div class="metrics">
                  <div class="metric" data-role="items"></div>
                  <div class="metric" data-role="tools"></div>
                  <div class="metric" data-role="chars"></div>
                  <div class="metric" data-role="events"></div>
                </div>
              </button>
            `;
        }
        this._built = true;
        this.querySelector("button").addEventListener("click", () => {
          store.selectRequest(this.getAttribute("request-id"));
        });
      }
      this._render();
    }
    attributeChangedCallback() { if (this._built) this._render(); }

    _setText(role, text) {
      const el = this.querySelector(`[data-role="${role}"]`);
      if (el && el.textContent !== text) el.textContent = text;
    }

    _setPreview(role, text, prefix) {
      const el = this.querySelector(`[data-role="${role}"]`);
      if (!el) return;
      // Collapse runs of whitespace so the snippet stays on a single line
      // and the ellipsis kicks in cleanly.
      const clean = (text || "").replace(/\s+/g, " ").trim();
      if (!clean) {
        if (el.textContent !== "") el.textContent = "";
        el.dataset.empty = "";
        return;
      }
      delete el.dataset.empty;
      const wanted = `${prefix} ${clean}`;
      if (el.textContent !== wanted) el.textContent = wanted;
    }

    _render() {
      const id = this.getAttribute("request-id");
      const selected = this.hasAttribute("selected");
      const button = this.querySelector("button");
      if (!button) return;
      const isDashboard = id === DASHBOARD_KEY;
      const wantClass = `request-button${isDashboard ? " dashboard" : ""}${selected ? " selected" : ""}`;
      if (button.className !== wantClass) button.className = wantClass;

      if (isDashboard) {
        this._setText("count", String(countRunningRequests()));
        return;
      }
      const request = store.get(id);
      if (!request) return;
      const status = request.status || "running";
      const badge = this.querySelector('[data-role="status"]');
      if (badge) {
        const wantBadge = `badge status-icon ${status}`;
        if (badge.className !== wantBadge) badge.className = wantBadge;
        if (badge.dataset.status !== status) {
          badge.dataset.status = status;
          badge.innerHTML = statusIcon(status);
        }
        badge.title = status;
      }
      this._setText("id", shortId(id));

      const userText = lastUserMessageFrom(latestPayloadPreview(request));
      const assistantText = responseStart(request);
      this._setPreview("user-preview", userText, "🡪");
      this._setPreview("assistant-preview", assistantText, "🡨");

      this._setText("items", `${formatCount(request.stats?.input_items)} items`);
      this._setText("tools", `${formatCount(request.stats?.tool_count)} tools`);
      // Real token usage (D3) beats the input-chars heuristic once it arrives.
      this._setText("chars", request.usage
              ? `${formatCompact(request.usage.total)} tok`
              : `${formatCount(request.stats?.input_chars)} chars`);
      this._setText("started", formatTime(request.started_at_ms));
      this._setText("duration", formatDuration(request));
      this._setText("events", `${formatCount(request.events?.length)} events`);
    }

    refresh() { this._render(); }
  }
  customElements.define("request-list-item", RequestListItem);

  // ------------------------------------------------------------------------
  // <request-list>: diff-based sidebar. Reuses item nodes across renders so
  // hover/focus/scroll position survive.
  // ------------------------------------------------------------------------

  class RequestList extends HTMLElement {
    connectedCallback() {
      this._onListChanged = () => this._render();
      this._onSelectionChanged = () => this._render();
      this._onRequestChanged = (e) => {
        // When search is active, streaming changes can flip a row's match
        // state -- run the full filter pass so it appears/disappears.
        if (store.searchQuery) {
          this._render();
          return;
        }
        // Streaming updates: re-render just that item. The dashboard count
        // only changes with set-membership changes (request added/removed/
        // status flip), which arrive via list-changed -- skip the
        // O(requests) countDashboardTiles() walk on every token.
        const item = this.querySelector(`request-list-item[request-id="${cssAttrEscape(e.detail.id)}"]`);
        if (item) item.refresh();
      };
      this._onSearchChanged = () => this._render();
      store.addEventListener("list-changed", this._onListChanged);
      store.addEventListener("selection-changed", this._onSelectionChanged);
      store.addEventListener("request-changed", this._onRequestChanged);
      store.addEventListener("search-changed", this._onSearchChanged);
      this._render();
    }
    disconnectedCallback() {
      store.removeEventListener("list-changed", this._onListChanged);
      store.removeEventListener("selection-changed", this._onSelectionChanged);
      store.removeEventListener("request-changed", this._onRequestChanged);
      store.removeEventListener("search-changed", this._onSearchChanged);
    }
    _render() {
      // The dashboard pseudo-entry is hidden while search is active --
      // search-mode is about finding a specific historical request, and
      // the dashboard isn't a request.
      const wantDashboard = !store.searchQuery;
      let dash = this.querySelector(`:scope > request-list-item[request-id="${DASHBOARD_KEY}"]`);
      if (wantDashboard && !dash) {
        dash = document.createElement("request-list-item");
        dash.setAttribute("request-id", DASHBOARD_KEY);
        this.prepend(dash);
      } else if (!wantDashboard && dash) {
        dash.remove();
        dash = null;
      }
      if (dash) toggleAttr(dash, "selected", store.selectedId === DASHBOARD_KEY);

      // Helper: insert `node` after `prev`, or prepend if there is no prev.
      const insertAt = (node, prev) => prev ? prev.after(node) : this.prepend(node);
      const currentAfter = (prev) => prev ? prev.nextElementSibling : this.firstElementChild;

      // Index existing per-request items (skipping the dashboard).
      const existing = new Map();
      for (let n = currentAfter(dash); n; n = n.nextElementSibling) {
        const id = n.getAttribute("request-id");
        if (id && id !== DASHBOARD_KEY) existing.set(id, n);
      }

      // FLIP step 1: snapshot existing item positions so we can animate
      // them sliding down when a new request arrives at the top. Skip
      // during the initial snapshot load -- there's nothing to animate.
      const animate = !store._snapshotLoading && this._everRendered;
      const oldRects = animate ? new Map() : null;
      if (animate) {
        for (const [id, node] of existing) {
          oldRects.set(id, node.getBoundingClientRect());
        }
      }

      // Filter when search is active.
      const desiredOrder = store.searchQuery
              ? store.order.filter(id => {
                const r = store.get(id);
                return r && store.matchesSearch(r);
              })
              : store.order;

      // Walk desired order, inserting / moving / updating.
      const newlyInserted = [];
      let prev = dash;
      for (const id of desiredOrder) {
        let node = existing.get(id);
        if (!node) {
          node = document.createElement("request-list-item");
          node.setAttribute("request-id", id);
          insertAt(node, prev);
          if (animate) newlyInserted.push(node);
        } else if (currentAfter(prev) !== node) {
          insertAt(node, prev);
        }
        toggleAttr(node, "selected", id === store.selectedId);
        node.refresh();
        existing.delete(id);
        prev = node;
      }

      for (const stale of existing.values()) stale.remove();

      if (animate) this._playFlip(oldRects, newlyInserted);
      this._everRendered = true;
    }

    _playFlip(oldRects, newlyInserted) {
      // Run after the browser has applied the new layout so
      // getBoundingClientRect reflects the post-insert geometry.
      requestAnimationFrame(() => {
        // Existing rows: translate from their previous Y to current Y.
        for (const [id, prevRect] of oldRects) {
          const node = this.querySelector(`:scope > request-list-item[request-id="${cssAttrEscape(id)}"]`);
          if (!node) continue;
          const curr = node.getBoundingClientRect();
          const dy = prevRect.top - curr.top;
          if (Math.abs(dy) < 0.5) continue;
          node.animate(
                  [{ transform: `translateY(${dy}px)` }, { transform: "none" }],
                  { duration: 260, easing: "cubic-bezier(0.2, 0, 0.2, 1)" }
          );
        }
        // Newly-arrived rows: slide in from above with a fade.
        for (const node of newlyInserted) {
          const rect = node.getBoundingClientRect();
          const startY = -Math.max(rect.height, 24);
          node.animate(
                  [
                    { transform: `translateY(${startY}px)`, opacity: 0 },
                    { transform: "none", opacity: 1 }
                  ],
                  { duration: 260, easing: "cubic-bezier(0.2, 0, 0.2, 1)" }
          );
        }
      });
    }
  }
  customElements.define("request-list", RequestList);

  // ------------------------------------------------------------------------
  // <request-search>: collapsible search input in the topbar. Drives
  // store.searchQuery; <request-list> reads from it.
  // ------------------------------------------------------------------------

  class RequestSearch extends HTMLElement {
    connectedCallback() {
      if (!this._built) {
        this.innerHTML = `
            <button type="button" class="search-icon-btn" title="Search requests" aria-label="Search requests">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="11" cy="11" r="7"></circle>
                <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
              </svg>
            </button>
            <div class="search-form">
              <input type="text" placeholder="Search in requests…" autocomplete="off" spellcheck="false">
              <button type="button" class="search-clear" title="Close search" aria-label="Close search">×</button>
            </div>
          `;
        this._iconBtn = this.querySelector(".search-icon-btn");
        this._input = this.querySelector("input");
        this._clear = this.querySelector(".search-clear");

        this._iconBtn.addEventListener("click", () => this._open());
        this._clear.addEventListener("click", () => this._close());
        this._input.addEventListener("input", () => {
          // Debounced: the filter pass scans every request's payload previews
          // (now full-fidelity, possibly megabytes), so run it once per typing
          // pause instead of per keystroke.
          clearTimeout(this._debounce);
          this._debounce = setTimeout(() => store.setSearchQuery(this._input.value), 150);
        });
        this._input.addEventListener("keydown", (e) => {
          if (e.key === "Escape") {
            clearTimeout(this._debounce);
            if (this._input.value) {
              this._input.value = "";
              store.setSearchQuery("");
            } else {
              this._close();
            }
          }
        });
        this._built = true;
      }
    }
    _open() {
      this.setAttribute("open", "");
      requestAnimationFrame(() => this._input.focus());
    }
    _close() {
      this.removeAttribute("open");
      clearTimeout(this._debounce);
      if (this._input.value) {
        this._input.value = "";
        store.setSearchQuery("");
      }
    }
  }
  customElements.define("request-search", RequestSearch);

  // ------------------------------------------------------------------------
  // Dashboard helpers
  // ------------------------------------------------------------------------

  function dashboardVisibleRequests() {
    const now = Date.now();
    // Sidebar order is newest-first; tile grid should be stable (oldest-
    // first) so new arrivals append at the end rather than shifting all
    // existing tiles.
    const out = [];
    for (const id of [...store.order].reverse()) {
      const request = store.get(id);
      if (!request) continue;
      if (request.status !== "running") {
        const completedAt = Number(request.completed_at_ms || 0);
        if (completedAt && (now - completedAt) > TILE_LINGER_MS) continue;
      }
      out.push(request);
    }
    return out;
  }

  function countDashboardTiles() { return dashboardVisibleRequests().length; }

  function countRunningRequests() {
    let n = 0;
    for (const id of store.order) {
      const r = store.get(id);
      if (r && r.status === "running") n++;
    }
    return n;
  }

  function dashboardLayoutClass(count) {
    if (count <= 4) return `n-${count}`;
    return "n-many";
  }

  // Function declarations (not const arrows) so they hoist above custom-
  // element class definitions. The browser upgrades existing tags the
  // moment customElements.define() runs, which fires connectedCallback ->
  // _render synchronously -- before later top-level const declarations
  // would otherwise have executed.
  function cssAttrEscape(v) { return String(v).replace(/(["\\])/g, "\\$1"); }
  function toggleAttr(el, name, on) { on ? el.setAttribute(name, "") : el.removeAttribute(name); }

  // Batched scroll-anchoring. Tiles call scheduleStickToBottom(tile) from
  // their refresh; we flush all pending anchors in one rAF callback. Reading
  // scrollHeight per tile in the streaming hot path used to force a layout
  // each call (N tiles -> N layouts per frame). Doing the reads inside a
  // single rAF, after the browser has already laid out for the frame, lets
  // the engine satisfy them from cached layout state.
  const pendingStickTiles = new Set();
  let stickFlushScheduled = false;
  function scheduleStickToBottom(tile) {
    pendingStickTiles.add(tile);
    if (stickFlushScheduled) return;
    stickFlushScheduled = true;
    requestAnimationFrame(() => {
      stickFlushScheduled = false;
      const tiles = [...pendingStickTiles];
      pendingStickTiles.clear();
      for (const t of tiles) t._flushStickToBottom();
    });
  }

  // Streaming-friendly text setter. Setting textContent on a node throws
  // away its existing text node and creates a new one, forcing the browser
  // to relay out the entire string on every token. Instead, find the
  // trailing Text node and grow it in place via appendData (which only
  // dirties the appended characters), creating it if missing. Falls back
  // to a wholesale rewrite when the new text doesn't extend the old one.
  function setNodeText(node, text) {
    const last = node.lastChild;
    if (last && last.nodeType === Node.TEXT_NODE) {
      const have = last.data;
      if (have === text) return;
      if (text.length > have.length && text.startsWith(have)) {
        last.appendData(text.slice(have.length));
        // Drop any other text node siblings so the tail Text remains the
        // single source of truth on the next call.
        while (node.firstChild !== last) node.removeChild(node.firstChild);
        return;
      }
      // Text shrank or diverged: replace in place without recreating.
      last.data = text;
      while (node.firstChild !== last) node.removeChild(node.firstChild);
      return;
    }
    node.textContent = text;
  }

  // ------------------------------------------------------------------------
  // Preview extraction: pulls the last user message and the start of the
  // model's response out of a request for the sidebar snippet rows.
  //
  // Live (in-flight) requests stream their upstream payload incrementally,
  // so the JSON preview is often truncated mid-string. We try a strict
  // JSON.parse first and fall back to a deliberately lax regex that hunts
  // for the relevant fields without requiring balanced quotes.
  // ------------------------------------------------------------------------

  function flattenContent(value) {
    if (value === null || value === undefined) return "";
    if (typeof value === "string") return value;
    if (!Array.isArray(value)) return "";
    const parts = [];
    for (const item of value) {
      if (typeof item === "string") parts.push(item);
      else if (item && typeof item === "object") {
        if (typeof item.text === "string") parts.push(item.text);
        else if (item.content !== undefined) parts.push(flattenContent(item.content));
      }
    }
    return parts.filter(Boolean).join(" ");
  }

  // Match a JSON string literal value AFTER a "key":" marker, tolerant of
  // truncation. The non-greedy capture stops at the first closing quote OR
  // end-of-input, whichever comes first.
  const JSON_STRING_TAIL = /((?:[^"\\]|\\.)*?)(?:"|$)/;

  function unescapeJsonish(s) {
    try { return JSON.parse(`"${s}"`); } catch (_) { return s; }
  }

  function lastUserMessageFrom(preview) {
    if (!preview) return "";
    // Strict path first.
    try {
      const parsed = JSON.parse(preview);
      if (Array.isArray(parsed.messages)) {
        for (let i = parsed.messages.length - 1; i >= 0; i--) {
          const m = parsed.messages[i];
          if (m && m.role === "user") return flattenContent(m.content);
        }
      }
      if (parsed.input !== undefined) {
        // responses-api shape: items may carry role/content.
        if (Array.isArray(parsed.input)) {
          for (let i = parsed.input.length - 1; i >= 0; i--) {
            const it = parsed.input[i];
            if (it && it.role === "user") return flattenContent(it.content ?? it);
          }
        }
        return flattenContent(parsed.input);
      }
    } catch (_) { /* fall through to regex */ }

    // Find the LAST "role":"user" occurrence, then look for the nearest
    // content payload after it.
    const roleRe = /"role"\s*:\s*"user"/g;
    let lastIdx = -1, m;
    while ((m = roleRe.exec(preview)) !== null) lastIdx = m.index;
    if (lastIdx === -1) return "";
    const after = preview.slice(lastIdx);

    const stringContent = after.match(new RegExp(`"content"\\s*:\\s*"${JSON_STRING_TAIL.source}`));
    if (stringContent) return unescapeJsonish(stringContent[1]);

    // Content is an array of typed items: pull the first "text":"…".
    const textInside = after.match(new RegExp(`"text"\\s*:\\s*"${JSON_STRING_TAIL.source}`));
    if (textInside) return unescapeJsonish(textInside[1]);

    return "";
  }

  function responseStart(request) {
    for (const seg of request.segments || []) {
      if (seg.kind === "output" && seg.text) return seg.text;
    }
    return "";
  }

  function latestPayloadPreview(request) {
    const events = request.events || [];
    for (let i = events.length - 1; i >= 0; i--) {
      const e = events[i];
      if (!e || !e.payload_preview) continue;
      if (e.kind === "upstream_request" || e.kind === "request_payload") return e.payload_preview;
    }
    return "";
  }

  // Inline SVG glyphs for status badges. Thin stroke, currentColor so the
  // existing .badge.{running,completed,failed} color rules tint them.
  const STATUS_ICONS = {
    running: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="8"></circle><path d="M12 8v4l3 2"></path></svg>`,
    completed: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="5 12 10 17 19 7"></polyline></svg>`,
    failed: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="6" y1="6" x2="18" y2="18"></line><line x1="6" y1="18" x2="18" y2="6"></line></svg>`
  };
  function statusIcon(status) { return STATUS_ICONS[status] || STATUS_ICONS.running; }

  // ------------------------------------------------------------------------
  // <reasoning-block>: streaming-while-open, auto-close once non-reasoning
  // span arrives, and remember user toggles. Hosts a <details> internally.
  // ------------------------------------------------------------------------

  class ReasoningBlock extends HTMLElement {
    static get observedAttributes() { return ["streaming"]; }
    connectedCallback() {
      if (!this._built) {
        this.innerHTML = `
            <details>
              <summary></summary>
              <div class="reasoning-body"></div>
            </details>
          `;
        this._details = this.querySelector("details");
        this._summary = this.querySelector("summary");
        this._body = this.querySelector(".reasoning-body");
        this._details.addEventListener("toggle", () => {
          // Programmatic opens/closes via setForcedOpen aren't user actions
          // -- skip recording them so they don't masquerade as a preference.
          if (this._suppressNextToggle) {
            this._suppressNextToggle = false;
            return;
          }
          const tile = this.closest("request-tile");
          const groupKey = this.dataset.groupKey;
          if (tile && groupKey !== undefined) tile.recordReasoningToggle(groupKey, this._details.open);
        });
        this._built = true;
      }
      this._sync();
    }
    attributeChangedCallback() { if (this._built) this._sync(); }
    setText(text) {
      // Each _renderBody call creates a fresh reasoning-block, so an
      // inequality check would always pass anyway -- but keep it explicit
      // so that if we ever reuse blocks, an unchanged text doesn't trigger
      // a redundant DOM mutation. Note: textContent preserves all
      // whitespace exactly, so trailing characters (incl. italic glyphs)
      // are written verbatim.
      if (!this._body) return;
      setNodeText(this._body, text);
    }
    setForcedOpen(open) {
      // Forcefully set initial state from the parent tile (after consulting
      // user-toggle history). The toggle listener distinguishes this from a
      // real user click via the _suppressNextToggle flag.
      if (!this._details) return;
      if (this._details.open !== open) {
        this._suppressNextToggle = true;
        this._details.open = open;
      }
    }
    _sync() {
      const streaming = this.hasAttribute("streaming");
      if (this._summary) this._summary.textContent = streaming ? "reasoning…" : "reasoning";
    }
  }
  customElements.define("reasoning-block", ReasoningBlock);

  // ------------------------------------------------------------------------
  // <request-tile>: single live tile. Subscribes to its own request.
  // ------------------------------------------------------------------------

  class RequestTile extends HTMLElement {
    static get observedAttributes() { return ["request-id"]; }

    connectedCallback() {
      if (!this._built) {
        this.innerHTML = `
            <div class="tile-title" data-role="title">
              <span class="badge status-icon" data-role="title-status"></span>
              <span class="title-main" data-role="title-main"></span>
              <span class="title-linger" data-role="title-linger"></span>
            </div>
            <div class="tile-body" data-role="body"></div>
          `;
        this._titleEl = this.querySelector('[data-role="title"]');
        this._titleEl.title = "Double-click to open this request";
        this._titleStatusEl = this.querySelector('[data-role="title-status"]');
        this._titleMainEl = this.querySelector('[data-role="title-main"]');
        this._titleLingerEl = this.querySelector('[data-role="title-linger"]');
        this._bodyEl = this.querySelector('[data-role="body"]');
        // Tiles start anchored to the bottom. The flag flips off only when
        // the user actively scrolls up, and back on when they return to the
        // bottom. Comparing wasAtBottom snapshots before/after a re-render
        // is fragile -- height changes mid-render (reasoning details
        // opening/closing, FLIP animations, child-list churn) can move
        // scrollTop out from under the measurement -- so we track stick
        // state explicitly instead.
        this._stickToBottom = true;
        this._suppressNextScroll = false;
        this._bodyEl.addEventListener("scroll", () => {
          if (this._suppressNextScroll) {
            this._suppressNextScroll = false;
            return;
          }
          const b = this._bodyEl;
          this._stickToBottom = (b.scrollTop + b.clientHeight + 32) >= b.scrollHeight;
        });
        // Double-click jumps to the request's detail view in the sidebar.
        // Use dblclick so single-clicks inside the body (text selection,
        // reasoning toggles) still work normally.
        this.addEventListener("dblclick", () => {
          const id = this.getAttribute("request-id");
          if (id) store.selectRequest(id);
        });
        this._built = true;
      }
      this._onRequestChanged = (e) => {
        if (e.detail.id === this.getAttribute("request-id")) this.refresh();
      };
      store.addEventListener("request-changed", this._onRequestChanged);
      this.refresh();
    }
    disconnectedCallback() {
      store.removeEventListener("request-changed", this._onRequestChanged);
      pendingStickTiles.delete(this);
    }
    attributeChangedCallback() { if (this._built) this.refresh(); }

    recordReasoningToggle(groupKey, open) {
      const request = store.get(this.getAttribute("request-id"));
      if (request) request.reasoningUserToggles.set(groupKey, open);
    }

    refresh() {
      const id = this.getAttribute("request-id");
      const request = store.get(id);
      if (!request) return;
      const status = request.status || "running";
      if (this.getAttribute("status") !== status) this.setAttribute("status", status);
      this._renderTitle(request);
      this._renderBody(request);
    }

    _renderTitle(request) {
      const id = shortId(request.response_id);
      const model = request.model || "?";
      const temp = (typeof request.temperature === "number") ? ` · T=${request.temperature.toFixed(2)}` : "";
      const status = request.status || "running";
      const start = Number(request.started_at_ms || 0);
      const end = Number(request.completed_at_ms || Date.now());
      const seconds = start ? Math.max(0, (end - start) / 1000) : 0;
      const tokens = request.usage ? ` · ${formatCompact(request.usage.total)} tok` : "";
      const mainText = `${id} · ${model}${temp} · ${seconds.toFixed(1)}s${tokens}`;

      let lingerText = "";
      if (status !== "running" && request.completed_at_ms) {
        const remainingMs = Math.max(0, TILE_LINGER_MS - (Date.now() - Number(request.completed_at_ms)));
        lingerText = `Hiding in ${Math.ceil(remainingMs / 1000)}s`;
      }

      const badge = this._titleStatusEl;
      const wantClass = `badge status-icon ${status}`;
      if (badge.className !== wantClass) badge.className = wantClass;
      if (badge.dataset.status !== status) {
        badge.dataset.status = status;
        badge.innerHTML = statusIcon(status);
      }
      badge.title = status;

      if (this._titleMainEl.textContent !== mainText) this._titleMainEl.textContent = mainText;
      if (this._titleLingerEl.textContent !== lingerText) this._titleLingerEl.textContent = lingerText;
    }

    _renderBody(request) {
      const body = this._bodyEl;
      const spans = request.tileSpans;

      // Identify the last reasoning group's start index. If there's a
      // non-reasoning span after the last reasoning span, no group is
      // currently streaming.
      let lastReasoningGroupStart = -1;
      if (spans.length) {
        const lastIdx = spans.length - 1;
        if (spans[lastIdx].kind === "reasoning") {
          let g = lastIdx;
          while (g > 0 && spans[g - 1].kind === "reasoning") g--;
          lastReasoningGroupStart = g;
        }
      }

      // Incremental render: everything before the trailing item is
      // immutable (reasoning groups close when a non-reasoning span
      // arrives; non-reasoning spans freeze when any new span follows).
      // We keep `_renderedTailStart` pointing at the tileSpans index of
      // the still-mutable trailing item. On refresh we only touch that
      // item (and the error suffix). Items past it are appended.
      const rendered = this._renderedItems ||= [];
      let tailStart = this._renderedTailStart ?? 0;

      // Sanity check: if the store reset (snapshot reload) or replaced the
      // tileSpans array, our incremental state is stale -- rebuild.
      if (rendered.length && rendered[0].spans !== spans) {
        rendered.length = 0;
        tailStart = 0;
        body.replaceChildren();
        if (this._errorNode) { this._errorNode = null; }
      }

      // Walk spans from tailStart. The previous trailing entry (if any) is
      // the only rendered item whose range can still mutate; we update its
      // text in place when its kind/range still match, otherwise drop and
      // rebuild. All later items are appended fresh.
      let i = tailStart;

      const buildGroup = () => {
        if (spans[i].kind !== "reasoning") return null;
        const groupStart = i;
        let text = "";
        let j = i;
        while (j < spans.length && spans[j].kind === "reasoning") {
          text += spans[j].text;
          j++;
        }
        return { groupStart, endIdx: j, text };
      };

      // Try to reuse the existing tail entry instead of recreating it.
      // Only treat it as "still the tail" if no spans have arrived past it;
      // once newer spans exist, the previous tail is finalized and we just
      // append to it. For reasoning the endIdx grows as the group extends,
      // so we compare against the live group extent.
      let tailEntry = rendered.length && rendered[rendered.length - 1].startIdx === tailStart
              ? rendered[rendered.length - 1]
              : null;

      if (tailEntry) {
        // Compute current extent of the trailing logical item to see if
        // it's still the tail.
        let liveEnd = tailStart + 1;
        if (tailEntry.kind === "reasoning" && spans[tailStart] && spans[tailStart].kind === "reasoning") {
          let j = tailStart;
          while (j < spans.length && spans[j].kind === "reasoning") j++;
          liveEnd = j;
        }
        // If anything comes after the live extent, the prior tail is now
        // finalized. Update it once (text may have grown to its final form)
        // and stop treating it as the tail.
        if (liveEnd < spans.length) {
          if (tailEntry.kind === "reasoning" && spans[tailStart].kind === "reasoning") {
            const grp = buildGroup();
            const block = tailEntry.node;
            toggleAttr(block, "streaming", false);
            block.setText(grp.text);
            const groupKey = String(grp.groupStart);
            const autoClosed = (request.reasoningAutoClosed ??= new Set());
            if (!autoClosed.has(groupKey)) {
              autoClosed.add(groupKey);
              request.reasoningUserToggles.delete(groupKey);
            }
            const userPref = request.reasoningUserToggles.get(groupKey);
            block.setForcedOpen(userPref !== undefined ? userPref : false);
            tailEntry.endIdx = grp.endIdx;
            i = grp.endIdx;
          } else if (tailEntry.kind !== "reasoning" && spans[tailStart] && spans[tailStart].kind === tailEntry.kind) {
            setNodeText(tailEntry.node, spans[tailStart].text);
            tailEntry.endIdx = tailStart + 1;
            i = tailStart + 1;
          } else {
            // Kind mismatch: rebuild.
            if (tailEntry.node.parentNode === body) body.removeChild(tailEntry.node);
            rendered.pop();
          }
          tailEntry = null; // fall through to append loop
        }
      }

      if (tailEntry && i < spans.length) {
        if (spans[i].kind === "reasoning" && tailEntry.kind === "reasoning") {
          const grp = buildGroup();
          const block = tailEntry.node;
          const isStreaming = (grp.groupStart === lastReasoningGroupStart);
          toggleAttr(block, "streaming", isStreaming);
          block.setText(grp.text);
          const groupKey = String(grp.groupStart);
          const autoClosed = (request.reasoningAutoClosed ??= new Set());
          if (!isStreaming && !autoClosed.has(groupKey)) {
            autoClosed.add(groupKey);
            request.reasoningUserToggles.delete(groupKey);
          }
          const userPref = request.reasoningUserToggles.get(groupKey);
          block.setForcedOpen(userPref !== undefined ? userPref : isStreaming);
          tailEntry.endIdx = grp.endIdx;
          i = grp.endIdx;
        } else if (spans[i].kind === tailEntry.kind && spans[i].kind !== "reasoning") {
          // Same-kind non-reasoning span: just update text. (Same-kind spans
          // are merged in tileBuffer, so this is the only span we represent.)
          setNodeText(tailEntry.node, spans[i].text);
          tailEntry.endIdx = i + 1;
          i += 1;
        } else {
          // Tail kind no longer matches -- drop and rebuild.
          if (tailEntry.node.parentNode === body) body.removeChild(tailEntry.node);
          rendered.pop();
        }
      } else if (tailEntry && i >= spans.length) {
        // Spans shrank (shouldn't normally happen) -- drop trailing entry.
        if (tailEntry.node.parentNode === body) body.removeChild(tailEntry.node);
        rendered.pop();
      }

      // Append any remaining new items.
      while (i < spans.length) {
        const span = spans[i];
        if (span.kind === "reasoning") {
          const grp = buildGroup();
          const block = document.createElement("reasoning-block");
          const groupKey = String(grp.groupStart);
          block.dataset.groupKey = groupKey;
          const isStreaming = (grp.groupStart === lastReasoningGroupStart);
          toggleAttr(block, "streaming", isStreaming);
          if (this._errorNode && this._errorNode.parentNode === body) {
            body.insertBefore(block, this._errorNode);
          } else {
            body.appendChild(block);
          }
          block.setText(grp.text);

          const autoClosed = (request.reasoningAutoClosed ??= new Set());
          if (!isStreaming && !autoClosed.has(groupKey)) {
            autoClosed.add(groupKey);
            request.reasoningUserToggles.delete(groupKey);
          }
          const userPref = request.reasoningUserToggles.get(groupKey);
          block.setForcedOpen(userPref !== undefined ? userPref : isStreaming);

          rendered.push({ kind: "reasoning", startIdx: grp.groupStart, endIdx: grp.endIdx, node: block, spans });
          i = grp.endIdx;
          continue;
        }
        const node = document.createElement("span");
        node.className = `sk-${span.kind}`;
        node.textContent = span.text;
        if (this._errorNode && this._errorNode.parentNode === body) {
          body.insertBefore(node, this._errorNode);
        } else {
          body.appendChild(node);
        }
        rendered.push({ kind: span.kind, startIdx: i, endIdx: i + 1, node, spans });
        i++;
      }

      // The next refresh only needs to touch the final rendered item.
      this._renderedTailStart = rendered.length ? rendered[rendered.length - 1].startIdx : 0;

      // Error suffix: keep a single node and update its text.
      if (request.error) {
        if (!this._errorNode) {
          this._errorNode = document.createElement("span");
          this._errorNode.className = "sk-error";
          body.appendChild(this._errorNode);
        }
        const wantText = `\nerror: ${request.error}`;
        setNodeText(this._errorNode, wantText);
      } else if (this._errorNode) {
        if (this._errorNode.parentNode === body) body.removeChild(this._errorNode);
        this._errorNode = null;
      }
      if (this._stickToBottom) scheduleStickToBottom(this);
    }

    _flushStickToBottom() {
      if (!this._stickToBottom || !this._bodyEl) return;
      const b = this._bodyEl;
      if (b.scrollTop + b.clientHeight + 1 < b.scrollHeight) {
        this._suppressNextScroll = true;
        b.scrollTop = b.scrollHeight;
      }
    }
  }
  customElements.define("request-tile", RequestTile);

  // ------------------------------------------------------------------------
  // <live-dashboard>: the right pane's "all in-flight" view.
  // ------------------------------------------------------------------------

  class LiveDashboard extends HTMLElement {
    connectedCallback() {
      this.innerHTML = `<div class="dashboard-grid" data-role="grid"></div>`;
      this._grid = this.querySelector('[data-role="grid"]');
      this._onListChanged = () => this.refresh();
      store.addEventListener("list-changed", this._onListChanged);
      // Tiles already subscribe to request-changed individually for their own
      // id. The dashboard's only job on streaming events is set-membership,
      // and membership changes are signaled via list-changed (insert / status
      // flip / prune). The 1Hz tick handles linger-driven removal.
      this.refresh();
    }
    disconnectedCallback() {
      store.removeEventListener("list-changed", this._onListChanged);
    }
    refresh() {
      const visible = dashboardVisibleRequests();
      this._reconcileGrid(visible);
    }
    _reconcileGrid(visible) {
      const wantGridClass = `dashboard-grid ${dashboardLayoutClass(visible.length)}`;
      const layoutWillChange = this._grid.className !== wantGridClass;

      // FLIP: snapshot the existing tiles' positions before any DOM/class
      // mutations so we can animate from the old layout to the new one.
      // Skip when nothing is changing (steady-state streaming updates run
      // through this path on every event).
      const oldRects = layoutWillChange ? this._snapshotTileRects() : null;

      if (layoutWillChange) this._grid.className = wantGridClass;

      // Remove empty placeholder if present.
      const empty = this._grid.querySelector(".dashboard-empty");

      if (!visible.length) {
        if (!empty) {
          this._grid.replaceChildren(Object.assign(document.createElement("div"), {
            className: "dashboard-empty",
            textContent: "No active requests."
          }));
        }
        return;
      }
      if (empty) empty.remove();

      // Diff tiles by request-id.
      const existing = new Map();
      for (const node of [...this._grid.children]) {
        if (node.tagName.toLowerCase() === "request-tile") {
          existing.set(node.getAttribute("request-id"), node);
        }
      }
      let prev = null;
      for (const request of visible) {
        let tile = existing.get(request.response_id);
        if (!tile) {
          tile = document.createElement("request-tile");
          tile.setAttribute("request-id", request.response_id);
          if (prev) prev.after(tile);
          else this._grid.prepend(tile);
        } else if (prev ? prev.nextElementSibling !== tile : this._grid.firstChild !== tile) {
          if (prev) prev.after(tile);
          else this._grid.prepend(tile);
        }
        tile.refresh();
        existing.delete(request.response_id);
        prev = tile;
      }
      for (const stale of existing.values()) stale.remove();

      // FLIP step 2: after layout settles, animate each surviving tile from
      // its old rect to its new one. New tiles get a brief fade-in instead.
      if (oldRects) this._playFlip(oldRects);
    }

    _snapshotTileRects() {
      const rects = new Map();
      for (const tile of this._grid.querySelectorAll("request-tile")) {
        const id = tile.getAttribute("request-id");
        if (id) rects.set(id, tile.getBoundingClientRect());
      }
      return rects;
    }

    _playFlip(oldRects) {
      // requestAnimationFrame so the browser has applied the new grid
      // tracks; getBoundingClientRect inside will reflect the post-layout
      // geometry.
      requestAnimationFrame(() => {
        for (const tile of this._grid.querySelectorAll("request-tile")) {
          const id = tile.getAttribute("request-id");
          const prev = oldRects.get(id);
          const curr = tile.getBoundingClientRect();
          if (curr.width === 0 || curr.height === 0) continue;

          if (!prev) {
            // Newly-added tile: small fade-in. Skip if the tile already
            // has an in-flight animation (e.g. CSS opacity fade-out).
            tile.animate(
                    [{ opacity: 0, transform: "scale(0.96)" }, { opacity: 1, transform: "none" }],
                    { duration: 220, easing: "cubic-bezier(0.2, 0, 0.2, 1)" }
            );
            continue;
          }

          const dx = prev.left - curr.left;
          const dy = prev.top - curr.top;
          const sx = prev.width / curr.width;
          const sy = prev.height / curr.height;
          // Skip work when the change is below perceptible threshold.
          if (Math.abs(dx) < 0.5 && Math.abs(dy) < 0.5 &&
                  Math.abs(sx - 1) < 0.005 && Math.abs(sy - 1) < 0.005) continue;

          tile.animate(
                  [
                    {
                      transform: `translate(${dx}px, ${dy}px) scale(${sx}, ${sy})`,
                      transformOrigin: "top left"
                    },
                    { transform: "none", transformOrigin: "top left" }
                  ],
                  { duration: 320, easing: "cubic-bezier(0.2, 0, 0.2, 1)" }
          );
        }
      });
    }
  }
  customElements.define("live-dashboard", LiveDashboard);

  // ------------------------------------------------------------------------
  // <request-detail>: tabbed view for a single request (Messages / Raw /
  // Events). Fully incremental: the head is built once and patched in place,
  // and each tab reconciles its pane against keyed section state instead of
  // rebuilding, so accordion open/close choices, text selection, scroll
  // position, and in-flight clicks all survive streaming updates. Refreshes
  // are coalesced to one per animation frame.
  // ------------------------------------------------------------------------

  function prettyJson(value) {
    if (value === null || value === undefined) return "";
    if (typeof value === "string") {
      const trimmed = value.trim();
      if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
              || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        try { return JSON.stringify(JSON.parse(trimmed), null, 2); } catch (_) { /* fallthrough */ }
      }
      return value;
    }
    try { return JSON.stringify(value, null, 2); } catch (_) { return String(value); }
  }

  function makeCopyButton(getText) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "copy-btn";
    btn.textContent = "copy";
    btn.title = "Copy to clipboard";
    const flash = (label, cls) => {
      btn.textContent = label;
      if (cls) btn.classList.add(cls);
      setTimeout(() => {
        btn.textContent = "copy";
        if (cls) btn.classList.remove(cls);
      }, 1200);
    };
    btn.addEventListener("click", (e) => {
      // Inside a <summary>: don't toggle the enclosing <details>.
      e.preventDefault();
      e.stopPropagation();
      try {
        navigator.clipboard.writeText(getText()).then(
                () => flash("copied", "done"),
                () => flash("failed")
        );
      } catch (_) {
        flash("failed");
      }
    });
    return btn;
  }

  function eventDotClass(kind) {
    if (kind === "failed") return "k-red";
    if (kind === "completed") return "k-green";
    if (kind === "request_started" || kind === "request_payload"
            || kind === "upstream_request" || kind === "final_response") return "k-blue";
    if ((kind || "").includes("tool")) return "k-amber";
    return "";
  }

  const ROLE_LABELS = {
    reasoning: "Reasoning",
    meta: "Item",
    user: "User",
    assistant: "Assistant",
  };
  function roleLabel(role) {
    return ROLE_LABELS[role] || (role ? role.charAt(0).toUpperCase() + role.slice(1) : "Message");
  }

  class RequestDetail extends HTMLElement {
    static get observedAttributes() { return ["request-id"]; }

    connectedCallback() {
      if (!this._built) {
        this.innerHTML = `
            <div class="detail-head" data-role="head">
              <div class="title-row">
                <span class="badge status-icon" data-role="status"></span>
                <span class="title" data-role="title"></span>
                <span class="title-model" data-role="model"></span>
              </div>
              <div class="head-meta">
                <span class="meta-item" data-role="started"></span>
                <span class="meta-item" data-role="duration"></span>
                <span class="chip-list" data-role="chips"></span>
              </div>
              <div class="error-banner" data-role="error" hidden></div>
              <div class="tabs-row">
                <div class="tabs" data-role="tabs">
                  <button type="button" class="tab" data-tab="messages">Messages<span class="tab-count" data-role="count-messages"></span></button>
                  <button type="button" class="tab" data-tab="raw">Raw<span class="tab-count" data-role="count-raw"></span></button>
                  <button type="button" class="tab" data-tab="events">Events<span class="tab-count" data-role="count-events"></span></button>
                </div>
                <div class="tab-actions" data-role="tab-actions">
                  <button type="button" class="tab-action" data-role="expand-all">Expand all</button>
                  <button type="button" class="tab-action" data-role="collapse-all">Collapse all</button>
                </div>
              </div>
            </div>
            <div class="pane" data-role="pane"></div>
            <button type="button" class="jump-latest" data-role="jump" hidden>↓ Follow stream</button>
          `;
        this._headEl = this.querySelector('[data-role="head"]');
        this._statusEl = this.querySelector('[data-role="status"]');
        this._titleEl = this.querySelector('[data-role="title"]');
        this._modelEl = this.querySelector('[data-role="model"]');
        this._startedEl = this.querySelector('[data-role="started"]');
        this._durationEl = this.querySelector('[data-role="duration"]');
        this._chipsEl = this.querySelector('[data-role="chips"]');
        this._errorEl = this.querySelector('[data-role="error"]');
        this._actionsEl = this.querySelector('[data-role="tab-actions"]');
        this._pane = this.querySelector('[data-role="pane"]');
        this._jumpEl = this.querySelector('[data-role="jump"]');
        this._tab = "messages";
        this._stick = false;
        this._suppressScroll = false;

        for (const button of this.querySelectorAll(".tab")) {
          button.addEventListener("click", () => this._setTab(button.dataset.tab));
        }
        this.querySelector('[data-role="expand-all"]').addEventListener("click", () => this._setAllOpen(true));
        this.querySelector('[data-role="collapse-all"]').addEventListener("click", () => this._setAllOpen(false));
        this._pane.addEventListener("scroll", () => {
          if (this._suppressScroll) {
            this._suppressScroll = false;
            return;
          }
          const p = this._pane;
          this._stick = (p.scrollTop + p.clientHeight + 48) >= p.scrollHeight;
          this._renderJump();
        });
        this._jumpEl.addEventListener("click", () => {
          this._stick = true;
          this._scrollToBottom();
          this._renderJump();
        });
        this._built = true;
      }
      this._onRequestChanged = (e) => {
        if (e.detail.id === this.getAttribute("request-id")) this._scheduleRefresh();
      };
      store.addEventListener("request-changed", this._onRequestChanged);
      this._syncTabButtons();
      this.refresh();
    }
    disconnectedCallback() {
      store.removeEventListener("request-changed", this._onRequestChanged);
    }
    attributeChangedCallback() {
      if (!this._built) return;
      this._resetPane();
      this.refresh();
    }

    _scheduleRefresh() {
      if (this._refreshQueued) return;
      this._refreshQueued = true;
      requestAnimationFrame(() => {
        this._refreshQueued = false;
        if (this.isConnected) this.refresh();
      });
    }

    _setTab(tab) {
      if (this._tab === tab) return;
      this._tab = tab;
      this._syncTabButtons();
      this._resetPane();
      this.refresh();
    }

    _syncTabButtons() {
      for (const button of this.querySelectorAll(".tab")) {
        const active = button.dataset.tab === this._tab;
        button.classList.toggle("active", active);
      }
      this._actionsEl.hidden = this._tab !== "messages";
    }

    _resetPane() {
      this._paneState = null;
      this._convoCacheKey = null;
      this._convoCache = null;
      this._stick = false;
      if (this._pane) {
        this._pane.className = "pane";
        this._pane.replaceChildren();
      }
    }

    refresh() {
      const request = store.get(this.getAttribute("request-id"));
      if (!request) {
        if (!this._waiting) {
          this._waiting = true;
          this._headEl.hidden = true;
          this._jumpEl.hidden = true;
          this._pane.className = "pane empty";
          this._pane.textContent = "Waiting for requests";
          this._paneState = null;
        }
        return;
      }
      if (this._waiting) {
        this._waiting = false;
        this._headEl.hidden = false;
        this._resetPane();
      }
      // Pane first: it derives the parsed conversation, whose truncation flag
      // the head chips display.
      switch (this._tab) {
        case "raw": this._renderRaw(request); break;
        case "events": this._renderEvents(request); break;
        default: this._renderMessages(request); break;
      }
      this._renderHead(request);
      this._maybeStick();
    }

    // Called by the global 1Hz tick so the elapsed clock moves between
    // streaming events.
    tick() {
      if (this._waiting || !this._built) return;
      const request = store.get(this.getAttribute("request-id"));
      if (!request || request.status !== "running") return;
      this._setText(this._durationEl, formatDuration(request));
    }

    // ---- head ------------------------------------------------------------

    _setText(el, text) {
      if (el.textContent !== text) el.textContent = text;
    }

    _renderHead(request) {
      const status = request.status || "running";
      const badge = this._statusEl;
      const wantClass = `badge status-icon ${status}`;
      if (badge.className !== wantClass) badge.className = wantClass;
      if (badge.dataset.status !== status) {
        badge.dataset.status = status;
        badge.innerHTML = statusIcon(status);
        badge.title = status;
      }
      this._setText(this._titleEl, request.response_id || "");
      this._setText(this._modelEl, request.model || "unknown model");
      this._setText(this._startedEl, formatTime(request.started_at_ms));
      this._setText(this._durationEl, formatDuration(request));

      const error = request.error || "";
      this._errorEl.hidden = !error;
      if (error) this._setText(this._errorEl, error);

      // Tab counts (all cheap: conversation parse is cached per payload
      // event). Parsed before the chips so the truncation flag is current.
      const convo = this._conversationFor(request);
      this._renderChips(request);
      const events = request.events || [];
      let payloadCount = 0;
      for (const event of events) {
        if (event.payload_preview
                && (event.kind === "upstream_request" || event.kind === "request_payload")) {
          payloadCount++;
        }
      }
      let liveRuns = 0;
      const from = Math.min(request.liveSegmentsFrom ?? 0, request.segments.length);
      for (let i = from; i < request.segments.length; i++) {
        if (request.segments[i].text && request.segments[i].text.trim()) liveRuns++;
      }
      const messageCount = (convo ? convo.items.length + (convo.system ? 1 : 0) : 0) + liveRuns;
      this._setText(this.querySelector('[data-role="count-messages"]'), messageCount ? String(messageCount) : "");
      this._setText(this.querySelector('[data-role="count-raw"]'), payloadCount ? String(payloadCount) : "");
      this._setText(this.querySelector('[data-role="count-events"]'), events.length ? String(events.length) : "");
      this._renderJump();
    }

    _renderChips(request) {
      const parts = [];
      const usage = request.usage;
      if (usage) {
        const cached = usage.cached
                ? ` <span class="chip-sub">(${escapeHtml(formatCount(usage.cached))} cached)</span>` : "";
        parts.push(`<span class="chip" title="Prompt tokens (cumulative)">↑ ${escapeHtml(formatCount(usage.prompt))}${cached}</span>`);
        parts.push(`<span class="chip" title="Completion tokens (cumulative)">↓ ${escapeHtml(formatCount(usage.completion))}</span>`);
        if (usage.reasoning) {
          parts.push(`<span class="chip" title="Reasoning tokens (cumulative)">∴ ${escapeHtml(formatCount(usage.reasoning))}</span>`);
        }
      }
      if (typeof request.temperature === "number") {
        parts.push(`<span class="chip">T=${request.temperature.toFixed(2)}</span>`);
      }
      if (this._truncatedPreview) {
        parts.push(`<span class="chip warn" title="The server truncated this payload preview; the earliest turns may be missing.">truncated preview</span>`);
      }
      const html = parts.join("");
      if (this._chipsHtml !== html) {
        this._chipsHtml = html;
        this._chipsEl.innerHTML = html;
      }
    }

    _renderJump() {
      const request = store.get(this.getAttribute("request-id"));
      const show = !!request && (request.status || "running") === "running" && !this._stick;
      if (this._jumpEl.hidden !== !show) this._jumpEl.hidden = !show;
    }

    _maybeStick() {
      if (!this._stick) return;
      const p = this._pane;
      // Only suppress when the assignment will actually move the pane (and
      // thus fire a scroll event) — otherwise the flag would swallow the
      // user's next real scroll.
      if (p.scrollTop + p.clientHeight + 1 < p.scrollHeight) {
        this._suppressScroll = true;
        this._scrollToBottom();
      }
    }

    _scrollToBottom() {
      this._pane.scrollTop = this._pane.scrollHeight;
    }

    _setAllOpen(open) {
      const request = store.get(this.getAttribute("request-id"));
      for (const details of this._pane.querySelectorAll("details.accordion")) {
        // The per-node toggle listener records the choice as a user pref.
        details.open = open;
      }
      if (request && this._paneState?.nodes) {
        for (const key of this._paneState.nodes.keys()) request.detailToggles.set(key, open);
      }
    }

    // ---- conversation cache ------------------------------------------------

    // Parse (and cache) the conversation from the best available payload
    // event. Re-parses only when a NEW payload event arrives — never on
    // streaming deltas.
    _conversationFor(request) {
      const candidates = payloadEventCandidates(request);
      const cacheKey = `${request.response_id}:${candidates.length}`;
      if (this._convoCacheKey === cacheKey) return this._convoCache;
      let convo = null;
      let usedIndex = -1;
      for (let i = 0; i < candidates.length; i++) {
        const parsed = parseConversationEvent(candidates[i]);
        if (parsed && (parsed.items.length || parsed.system)) {
          convo = parsed;
          usedIndex = i;
          break;
        }
      }
      // Item count of the PREVIOUS same-kind payload: items below it are
      // carry-over from the prior round and default to collapsed.
      let prevCount = 0;
      if (convo) {
        for (let i = usedIndex + 1; i < candidates.length; i++) {
          if (candidates[i].kind !== convo.event.kind) continue;
          const prev = parseConversationEvent(candidates[i]);
          if (prev) {
            prevCount = prev.items.length;
            break;
          }
        }
      }
      this._convoCacheKey = cacheKey;
      this._convoCache = convo;
      this._prevItemCount = prevCount;
      this._truncatedPreview = !!convo?.truncated;
      return convo;
    }

    // ---- messages tab ------------------------------------------------------

    _messageSections(request, convo) {
      const sections = [];
      if (convo) {
        if (convo.system) {
          sections.push({
            key: "sys", type: "message", label: "System", role: "system",
            body: convo.system, stale: true, defaultOpen: false,
          });
        }
        const prevCount = this._prevItemCount || 0;
        convo.items.forEach((item, idx) => {
          const stale = idx < prevCount;
          const key = `c${idx}`;
          if (item.kind === "tool_call") {
            sections.push({
              key, type: "tool_call", label: `Tool call → ${item.name || "unknown"}`,
              role: "tool", item, stale, defaultOpen: !stale,
            });
          } else if (item.kind === "tool_result") {
            sections.push({
              key, type: "tool_result", label: "Tool result",
              role: "tool", item, stale, defaultOpen: !stale,
            });
          } else {
            sections.push({
              key, type: "message", label: roleLabel(item.role), role: item.role,
              body: item.content, stale,
              defaultOpen: !stale && item.role !== "reasoning" && item.role !== "meta",
            });
          }
        });
      }

      // Live stream of the current round, in arrival order — one section per
      // consecutive same-kind run, NOT merged across the whole request.
      // Earlier rounds already appear structurally above (they're in the
      // latest upstream payload), so start at the live watermark.
      const segs = request.segments || [];
      const from = Math.min(request.liveSegmentsFrom ?? 0, segs.length);
      const running = (request.status || "running") === "running";
      const live = [];
      for (let i = from; i < segs.length; i++) {
        const seg = segs[i];
        if (!seg.text || !seg.text.trim()) continue;
        const role = seg.kind === "reasoning" ? "reasoning" : seg.kind === "tool" ? "tool" : "assistant";
        const label = seg.kind === "reasoning" ? "Reasoning" : seg.kind === "tool" ? "Tool activity" : "Assistant";
        live.push({
          key: `live${i}`, type: "live", label, role, body: seg.text,
          defaultOpen: true, streaming: running && i === segs.length - 1,
        });
      }
      if (live.length) {
        sections.push({
          key: "live-divider", type: "divider",
          label: running ? "Streaming" : "Response", streaming: running,
        });
        sections.push(...live);
      }
      return sections;
    }

    _renderMessages(request) {
      const convo = this._conversationFor(request);
      const state = (this._paneState ||= { kind: "messages", convoKey: null, nodes: new Map(), emptyNode: null });
      const sections = this._messageSections(request, convo);

      // The parsed conversation changed (a new payload round arrived):
      // rebuild wholesale. Section keys are stable across rounds, so the
      // user's recorded open/close choices re-apply.
      if (state.convoKey !== this._convoCacheKey) {
        state.convoKey = this._convoCacheKey;
        state.nodes = new Map();
        state.emptyNode = null;
        this._pane.replaceChildren();
      }

      if (!sections.length) {
        if (!state.emptyNode) {
          state.emptyNode = document.createElement("div");
          state.emptyNode.className = "pane-empty";
          state.emptyNode.textContent = "No messages yet";
          this._pane.appendChild(state.emptyNode);
        }
        return;
      }
      if (state.emptyNode) {
        state.emptyNode.remove();
        state.emptyNode = null;
      }

      const nodes = state.nodes;
      const seen = new Set();
      let prev = null;
      for (const section of sections) {
        seen.add(section.key);
        let entry = nodes.get(section.key);
        if (!entry) {
          entry = this._buildSection(request, section);
          nodes.set(section.key, entry);
          if (prev) prev.after(entry.node);
          else this._pane.prepend(entry.node);
        } else if (entry.mutable) {
          this._updateSection(entry, section);
        }
        prev = entry.node;
      }
      for (const [key, entry] of nodes) {
        if (!seen.has(key)) {
          entry.node.remove();
          nodes.delete(key);
        }
      }
    }

    _buildSection(request, section) {
      if (section.type === "divider") {
        const div = document.createElement("div");
        div.className = `live-divider${section.streaming ? " streaming" : ""}`;
        const label = document.createElement("span");
        label.className = "live-divider-label";
        label.textContent = section.label;
        div.appendChild(label);
        return { node: div, labelEl: label, type: "divider", mutable: true };
      }

      const details = document.createElement("details");
      const variant = section.type === "tool_call" ? " tool-call"
              : section.type === "tool_result" ? " tool-result" : "";
      details.className = `accordion role-${section.role || "tool"}${variant}`
              + `${section.stale ? " stale" : ""}${section.streaming ? " streaming" : ""}`;

      const summary = document.createElement("summary");
      summary.className = "accordion-summary";
      const roleEl = document.createElement("span");
      roleEl.className = "accordion-role";
      roleEl.textContent = section.label;
      summary.appendChild(roleEl);
      if (section.item?.call_id) {
        const callId = document.createElement("span");
        callId.className = "tool-call-id";
        callId.textContent = section.item.call_id;
        summary.appendChild(callId);
      }

      const body = document.createElement("pre");
      let mutable = false;
      if (section.type === "tool_call" || section.type === "tool_result") {
        body.className = "segment tool-payload";
        body.textContent = prettyJson(section.type === "tool_call" ? section.item.arguments : section.item.output);
      } else {
        body.className = `segment ${section.role === "reasoning" ? "reasoning" : section.role === "tool" ? "tool" : "output"}`;
        body.textContent = section.body || "";
        mutable = section.type === "live";
      }
      summary.appendChild(makeCopyButton(() => body.textContent));
      details.appendChild(summary);
      details.appendChild(body);

      // Initial open state: the user's recorded choice wins over the default.
      const pref = request.detailToggles.get(section.key);
      details.open = pref !== undefined ? pref : !!section.defaultOpen;
      // `toggle` also fires (async) for the programmatic assignment above;
      // compare against the last known state so only real changes record.
      let lastOpen = details.open;
      details.addEventListener("toggle", () => {
        if (details.open === lastOpen) return;
        lastOpen = details.open;
        request.detailToggles.set(section.key, details.open);
      });

      return { node: details, bodyEl: body, labelEl: roleEl, type: section.type, mutable };
    }

    _updateSection(entry, section) {
      if (entry.type === "divider") {
        if (entry.labelEl.textContent !== section.label) entry.labelEl.textContent = section.label;
        entry.node.classList.toggle("streaming", !!section.streaming);
        return;
      }
      // Only live sections mutate: grow the text in place.
      setNodeText(entry.bodyEl, section.body || "");
      entry.node.classList.toggle("streaming", !!section.streaming);
    }

    // ---- raw tab -----------------------------------------------------------

    _renderRaw(request) {
      const state = (this._paneState ||= { kind: "raw" });
      if (!state.built) {
        state.built = true;
        this._pane.replaceChildren();
        const section = (label) => {
          const head = document.createElement("div");
          head.className = "raw-section-label";
          head.textContent = label;
          this._pane.appendChild(head);
          const wrap = document.createElement("div");
          this._pane.appendChild(wrap);
          return wrap;
        };
        state.clientWrap = section("Client request");
        state.upstreamWrap = section("Upstream requests");
        state.receivedWrap = section("Received stream");
        state.clientCount = 0;
        state.upstreamCount = 0;
        state.receivedPre = null;
        state.receivedLen = -1;
      }
      const events = request.events || [];
      const client = events.filter(e => e.kind === "request_payload" && e.payload_preview);
      const upstream = events.filter(e => e.kind === "upstream_request" && e.payload_preview);
      state.clientCount = this._appendPayloadBlocks(state.clientWrap, client, state.clientCount, "No client payload captured");
      state.upstreamCount = this._appendPayloadBlocks(state.upstreamWrap, upstream, state.upstreamCount, "No upstream request yet");

      const segs = request.segments || [];
      if (!segs.length) {
        if (!state.receivedWrap.firstChild) {
          const empty = document.createElement("div");
          empty.className = "raw-empty";
          empty.textContent = "No response yet";
          state.receivedWrap.appendChild(empty);
        }
        return;
      }
      if (!state.receivedPre) {
        state.receivedWrap.replaceChildren();
        state.receivedPre = document.createElement("pre");
        state.receivedPre.className = "payload";
        state.receivedWrap.appendChild(state.receivedPre);
      }
      // The join is O(total text); only rebuild when the length moved.
      let total = 0;
      for (const seg of segs) total += seg.text.length;
      if (total !== state.receivedLen) {
        state.receivedLen = total;
        setNodeText(state.receivedPre, segs.map(s => s.text).join(""));
      }
    }

    _appendPayloadBlocks(wrap, events, renderedCount, emptyText) {
      if (!events.length) {
        if (!wrap.firstChild) {
          const empty = document.createElement("div");
          empty.className = "raw-empty";
          empty.textContent = emptyText;
          wrap.appendChild(empty);
        }
        return 0;
      }
      if (renderedCount === 0) wrap.replaceChildren();
      for (let i = renderedCount; i < events.length; i++) {
        const event = events[i];
        const block = document.createElement("div");
        block.className = "raw-block";
        const head = document.createElement("div");
        head.className = "raw-block-head";
        const meta = document.createElement("span");
        meta.className = "raw-subhead";
        meta.textContent = `#${i + 1} · ${formatTime(event.timestamp_ms)} · ${formatCount(event.payload_preview.length)} chars`;
        head.appendChild(meta);
        head.appendChild(makeCopyButton(() => event.payload_preview));
        block.appendChild(head);
        const pre = document.createElement("pre");
        pre.className = "payload";
        pre.textContent = event.payload_preview;
        block.appendChild(pre);
        wrap.appendChild(block);
      }
      return events.length;
    }

    // ---- events tab ----------------------------------------------------------

    _renderEvents(request) {
      const state = (this._paneState ||= { kind: "events", eventCount: 0, emptyNode: null });
      const events = request.events || [];
      if (!events.length) {
        if (!state.emptyNode) {
          state.emptyNode = document.createElement("div");
          state.emptyNode.className = "pane-empty";
          state.emptyNode.textContent = "No events";
          this._pane.appendChild(state.emptyNode);
        }
        return;
      }
      if (state.emptyNode) {
        state.emptyNode.remove();
        state.emptyNode = null;
      }
      for (let i = state.eventCount; i < events.length; i++) {
        this._pane.appendChild(this._buildEventNode(events[i]));
      }
      state.eventCount = events.length;
    }

    _buildEventNode(event) {
      const details = document.createElement("details");
      details.className = `event${this._hasDetail(event) ? "" : " no-detail"}`;
      const summary = document.createElement("summary");
      const meta = document.createElement("div");
      meta.className = "event-meta";
      const dot = document.createElement("span");
      dot.className = `event-dot ${eventDotClass(event.kind)}`;
      const time = document.createElement("span");
      time.textContent = formatTime(event.timestamp_ms);
      const kind = document.createElement("span");
      kind.className = "event-kind";
      kind.textContent = event.kind;
      meta.append(dot, time, kind);
      summary.appendChild(meta);
      const text = document.createElement("div");
      text.className = "event-summary";
      text.textContent = event.summary || "";
      summary.appendChild(text);
      details.appendChild(summary);
      this._appendImages(details, event);
      this._appendPayload(details, event);
      return details;
    }

    _hasDetail(event) { return Boolean(event.payload_preview || event.images?.length); }

    _appendPayload(parent, event) {
      if (!event.payload_preview) return;
      const pre = document.createElement("pre");
      pre.className = "payload";
      pre.textContent = event.payload_preview;
      parent.appendChild(pre);
    }

    _appendImages(parent, event) {
      if (!event.images?.length) return;
      const list = document.createElement("div");
      list.className = "image-list";
      for (const image of event.images) {
        const item = document.createElement("figure");
        item.className = "event-image";
        // Raw image bytes/URLs are NEVER broadcast over /debug/ws (server-side
        // redaction); render a metadata-only placeholder card, not the image.
        const placeholder = document.createElement("div");
        placeholder.className = "image-redacted";
        placeholder.textContent = "[image redacted]";
        item.appendChild(placeholder);
        const caption = document.createElement("figcaption");
        caption.className = "image-caption";
        caption.textContent = `${image.label || "image"} | ${image.mime_type || "image"} | ${formatBytes(image.size_bytes)} | ${image.path || ""}`;
        item.appendChild(caption);
        list.appendChild(item);
      }
      parent.appendChild(list);
    }
  }
  customElements.define("request-detail", RequestDetail);

  // ------------------------------------------------------------------------
  // View controller: swaps the right-pane element on selection change.
  // ------------------------------------------------------------------------

  class ContentArea {
    constructor(host) {
      this.host = host;
      this._currentKey = null;
      store.addEventListener("selection-changed", () => this._sync());
      this._sync();
    }
    _sync() {
      const key = store.selectedId || DASHBOARD_KEY;
      if (key === this._currentKey) return;
      this._currentKey = key;
      if (key === DASHBOARD_KEY) {
        this.host.replaceChildren(document.createElement("live-dashboard"));
      } else {
        const detail = document.createElement("request-detail");
        detail.setAttribute("request-id", key);
        this.host.replaceChildren(detail);
      }
    }
  }
  new ContentArea(document.getElementById("content"));

  // ------------------------------------------------------------------------
  // 1Hz tick: prune expired requests and let tiles update their elapsed
  // counters / linger countdowns / fade-out state without waiting for a
  // streaming event.
  // ------------------------------------------------------------------------

  setInterval(() => {
    const pruned = store.pruneExpired();
    if (pruned) {
      // Route through _emit so a hidden tab coalesces the change instead of
      // queueing FLIP/rAF work it can't paint.
      store._emit("list-changed");
      return;
    }
    // Skip the per-tile DOM tick when the tab is hidden -- the dashboard
    // isn't visible and the rAF callbacks scheduled inside tile.refresh()
    // would just queue up to flood the main thread on return. Tiles will
    // re-render through flushDirty() on visibilitychange.
    if (document.hidden) return;
    // Detail view: advance the elapsed clock for a running request.
    const detail = document.querySelector("request-detail");
    if (detail && typeof detail.tick === "function") detail.tick();
    // Per-tile refresh (only when dashboard is mounted). We tick tiles for
    // the elapsed clock + linger countdown, but only call dashboard.refresh()
    // -- which walks every request via dashboardVisibleRequests() -- when at
    // least one tile is past its linger expiry and actually needs removal.
    const dashboard = document.querySelector("live-dashboard");
    if (!dashboard) return;
    const now = Date.now();
    let needsReconcile = false;
    for (const tile of dashboard.querySelectorAll("request-tile")) {
      tile.refresh();
      if (needsReconcile) continue;
      const id = tile.getAttribute("request-id");
      const request = store.get(id);
      if (!request) { needsReconcile = true; continue; }
      if (request.status !== "running") {
        const completedAt = Number(request.completed_at_ms || 0);
        if (completedAt && (now - completedAt) > TILE_LINGER_MS) needsReconcile = true;
      }
    }
    if (needsReconcile) dashboard.refresh();
  }, 1000);

  // ------------------------------------------------------------------------
  // Brand reflects the model of the most recent request (store.order[0]).
  // Falls back to the default text when there are no requests. Writes the
  // DOM only when the displayed string actually changes.
  // ------------------------------------------------------------------------
  {
    const DEFAULT_BRAND = "llmconduit debug";
    const brandEl = document.querySelector(".brand");
    const updateBrand = () => {
      const id = store.order[0];
      const request = id ? store.get(id) : null;
      const next = (request && request.model) ? request.model : DEFAULT_BRAND;
      if (brandEl.textContent !== next) brandEl.textContent = next;
    };
    store.addEventListener("list-changed", updateBrand);
    store.addEventListener("request-changed", (e) => {
      // Only the newest request can change the brand; ignore other ids.
      if (e.detail && e.detail.id === store.order[0]) updateBrand();
    });
    updateBrand();
  }

  socket.start();

  // Flush any changes that accumulated while the tab was hidden. The
  // store buffers dirty state during background time so the DOM isn't
  // mutated and rAF callbacks aren't queued for events the user can't
  // see; this fires one batched re-render on return.
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) store.flushDirty();
  });
