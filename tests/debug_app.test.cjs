"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync("src/debug_app.js", "utf8");
const start = source.indexOf("  const formatCompact");
const end = source.indexOf("  // ------------------------------------------------------------------------\n  // Span buffer");
assert.ok(start >= 0 && end > start, "debug helper section found");

const helpers = vm.runInNewContext(`(function () {
${source.slice(start, end)}
return { computeLiveSegmentsFrom, payloadEventCandidates, parseConversationEvent, conversationFromPayload };
})()`);

{
  const conversation = helpers.conversationFromPayload({
    messages: [
      { role: "system", content: "system-1" },
      { role: "user", content: "user-1" },
      { role: "developer", content: "developer-2" },
      { role: "user", content: "user-2" },
      { role: "system", content: "system-3" },
    ],
  });
  assert.deepEqual(
    Array.from(conversation.items, item => `${item.role}:${item.content}`),
    ["system:system-1", "user:user-1", "developer:developer-2", "user:user-2", "system:system-3"],
  );
  assert.equal(conversation.sourceEntryCount, 5);
}

{
  const conversation = helpers.conversationFromPayload({
    input: [
      { type: "custom_tool_call", call_id: "c1", name: "exec", input: "pwd" },
      { type: "custom_tool_call_output", call_id: "c1", output: "ok" },
      { type: "tool_search_call", call_id: "s1", execution: "client", arguments: { q: "x" } },
      { type: "tool_search_output", call_id: "s1", status: "completed", execution: "client", tools: [{ name: "x" }] },
      { type: "local_shell_call", call_id: "l1", status: "completed", action: { type: "exec", command: ["pwd"] } },
    ],
  });
  assert.deepEqual(
    Array.from(conversation.items, item => item.kind),
    ["tool_call", "tool_result", "tool_call", "tool_result", "tool_call"],
  );
  assert.equal(conversation.items[0].arguments, "pwd");
  assert.equal(conversation.items[4].arguments.command[0], "pwd");
}

{
  const messages = Array.from({ length: 30 }, (_, index) => ({
    role: index % 2 ? "assistant" : "user",
    content: `turn-${index}-` + "x".repeat(1000),
  }));
  const complete = JSON.stringify({ model: "m", messages }, null, 2);
  const cut = complete.indexOf("turn-12-") + 440;
  const request = {
    events: [
      { kind: "request_payload", sequence: 1, payload_entry_count: 30, payload_preview: complete },
      { kind: "upstream_request", sequence: 2, payload_entry_count: 30, payload_preview: complete.slice(0, cut) + "...\n[truncated]" },
    ],
  };
  let selected = null;
  let partial = null;
  for (const event of helpers.payloadEventCandidates(request)) {
    const parsed = helpers.parseConversationEvent(event);
    if (!parsed?.items.length) continue;
    if (parsed.structurallyComplete) {
      selected = parsed;
      break;
    }
    partial ||= parsed;
  }
  selected ||= partial;
  assert.equal(selected.event.kind, "request_payload");
  assert.equal(selected.items.length, 30);
  assert.equal(selected.structurallyComplete, true);
}

{
  const watermark = helpers.computeLiveSegmentsFrom({
    events: [{ kind: "upstream_request", sequence: 4, timestamp_ms: 200, payload_preview: "{}" }],
    segments: [
      { kind: "output", payload_sequence: 2, timestamp_ms: 100, text: "old" },
      { kind: "output", payload_sequence: 4, timestamp_ms: 200, text: "new" },
    ],
  });
  assert.equal(watermark, 1);
}

{
  const candidates = helpers.payloadEventCandidates({
    events: [
      { kind: "upstream_request", sequence: 2, payload_preview: "{}" },
      { kind: "upstream_request", sequence: 5, payload_retention_omitted: true },
    ],
  });
  assert.deepEqual(Array.from(candidates, event => event.sequence), [5, 2]);
}

{
  const watermark = helpers.computeLiveSegmentsFrom({
    events: [
      { kind: "request_payload", sequence: 2, payload_preview: "{}" },
      { kind: "upstream_request", sequence: 5, payload_retention_omitted: true },
    ],
    segments: [
      { kind: "output", payload_sequence: 2, text: "old" },
      { kind: "output", payload_sequence: 5, text: "new" },
    ],
  });
  assert.equal(watermark, 1);
}

{
  const parsed = helpers.parseConversationEvent({
    kind: "upstream_request",
    payload_entry_count: 2,
    payload_truncated: true,
    payload_preview: JSON.stringify({
      messages: [
        { role: "user", content: "… [+100000 chars]" },
        { role: "assistant", content: "ok" },
      ],
    }),
  });
  assert.equal(parsed.structurallyComplete, true);
  assert.equal(parsed.truncated, true);
  assert.equal(parsed.items.length, 2);
}
