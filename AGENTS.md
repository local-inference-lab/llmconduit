# AGENTS.md

Guidance for AI coding agents working in this repo.

## What this is

Rust LLM API gateway. Accepts OpenAI Responses, OpenAI Chat Completions, and Anthropic Messages on the front; forwards to OpenAI-compatible `/v1/chat/completions` upstream, and proxies legacy `/v1/completions`. Adds server-side Brave web search, per-model defaults, nested failover/model routing across upstreams, replay caching, request-log analysis, and an optional debug UI.

Use the code layout below as the architecture map when touching unfamiliar areas.

## Commands

```bash
cargo build --release          # build
cargo test                     # run all tests
cargo test <name>              # run single test by substring
cargo clippy --all-targets     # lint
cargo fmt                      # format
```

Run locally:

```bash
./target/release/llmconduit configure     # interactive YAML config
./target/release/llmconduit start         # serve
./target/release/llmconduit start --raw   # also write delta text to stdout
./target/release/llmconduit start --with-debug-ui   # exposes /debug + /debug/ws (and, when built, /dashboard)
./target/release/llmconduit analyze-log   # prefix-stability diff of upstream JSONL
```

Dashboard (Topic 13 — optional, opt-in embed):

```bash
LLMCONDUIT_BUILD_DASHBOARD=1 cargo build --release   # build the React SPA + embed via include_dir
cd dashboard-frontend && npm install && npm run dev  # frontend dev against an in-browser mock
cargo build --release                                 # node-less host: embeds a stub, still compiles
```
`/dashboard` + `/dashboard/api/*` + `/dashboard/ws` are registered only when `--with-debug-ui` is on.
Dashboard auth (env-only, never a persisted `Config` field): `LLMCONDUIT_DASHBOARD_TOKEN`,
`LLMCONDUIT_DASHBOARD_SESSION_KEY`, `LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN` (must be `https://` on
non-loopback by default). `LLMCONDUIT_ALLOW_INSECURE_DASHBOARD=1` explicitly permits plaintext;
when no token is configured it also permits fully unauthenticated non-loopback dev-open access and
uses same-origin `Origin`/`Host` validation for dashboard WebSockets. Kill requires `LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS=1`
+ a CSRF token.

## Code layout

| Path | Role |
|-|-|
| `src/main.rs` | Binary entry, tokio + tracing |
| `src/lib.rs` | DI root: `build_app_with_gateway_and_options` |
| `src/cli.rs` | clap CLI + interactive configure |
| `src/config.rs` | `Config`/`PersistedConfig`, profile resolution, env overrides |
| `src/http.rs` | axum router, body-logging middleware, secret redaction, `/v1/models` transform |
| `src/engine.rs` | `Gateway`, `run_turn` -- streaming + replay + tool-loop orchestration |
| `src/upstream.rs` | `Reqwest`/`Failover`/`Routing` upstream clients, `/v1/completions` proxy |
| `src/replay.rs` | SHA256-keyed LRU replay cache, longest-prefix match |
| `src/search.rs` | Brave Search client, `SearchClient` trait |
| `src/monitor.rs` | Debug-UI broadcast hub |
| `src/debug_ui.rs` | `/debug` HTML + WS handler |
| `src/dashboard_flow.rs` | (T13) DashboardFlowStore — authoritative per-flow records + capture seams |
| `src/metrics.rs` | (T13) MetricsLayer — ring buffers, histograms, 5 s body-free snapshots |
| `src/dashboard_api.rs` | (T13) `/dashboard/api/*` REST handlers |
| `src/dashboard_auth.rs` / `src/dashboard_ws.rs` | (T13) dashboard session-cookie auth + batched WS envelope |
| `src/dashboard_ui.rs` | (T13) `include_dir!`-embedded SPA shell + static assets |
| `dashboard-frontend/` | (T13) React + TS + Vite SPA (Vite build → `dist`, embedded when `LLMCONDUIT_BUILD_DASHBOARD=1`) |
| `src/raw.rs` | `--raw` stdout delta writer |
| `src/request_log.rs` | `analyze-log` impl |
| `src/error.rs` | `AppError` (client vs internal message split) |
| `src/adapters/` | Pure conversion layer between wire formats |
| `src/models/` | `responses.rs`, `chat.rs`, `anthropic.rs` wire types |
| `tests/gateway.rs` | Integration tests with `MockUpstream`, `MockSearch`, `PendingChunkUpstream` + wiremock |

## Canonical protocol

OpenAI Responses is the **single canonical internal protocol**. All inbound shapes convert in via adapters; all outbound shapes convert out via streaming converters. Do not add direct adapters between non-canonical shapes — go through Responses.

Adapter direction map:

| Module | Direction |
|-|-|
| `adapters/anthropic_to_responses.rs` | Anthropic request → canonical |
| `adapters/chat_completions.rs` | Chat request → canonical; canonical SSE → Chat SSE/non-stream; hides server-side `web_search` |
| `adapters/responses_to_chat.rs` | Canonical → `LoweredTurn` (chat messages + `ToolRegistry`) |
| `adapters/chat_to_responses.rs` | Upstream chunk stream → canonical SSE |
| `adapters/responses_to_anthropic.rs` | Canonical SSE → Anthropic SSE/non-stream |

## Conventions

- **`extra_body: BTreeMap<String, Value>` flattened** on `ResponsesRequest` and `ChatCompletionRequest`. Vendor-specific kwargs round-trip without schema changes. Prefer this over adding typed fields for provider-specific knobs.
- **Explicit request fields win over configured upstream defaults.** When building upstream chat requests, typed fields remove conflicting default keys (`temperature`, `top_p`, max-token aliases, penalties, `response_format`, `reasoning_effort`); request `extra_body.chat_template_kwargs` deep-merges over configured defaults.
- **No new wire fields without round-trip tests.** If you add a field, add a deserialize-then-serialize test that proves it survives.
- **`#[serde(deny_unknown_fields)]` is NOT used** on request types so unknown fields can flow into `extra_body`. Be careful adding it.
- **`tracing` for server logs, not `println!`.** CLI/reporting stdout is allowed for `configure`, `analyze-log`, and `RawOutput`.
- **Errors via `AppError`.** Use `AppError::internal(...)` when the detail must not reach the client — internal logs full message, client gets `"internal server error"`. Use `AppError::cancelled()` (HTTP 499) when the client hung up mid-stream.
- **Trait objects (`Arc<dyn UpstreamClient>`, `Arc<dyn SearchClient>`) on seams.** Tests inject mocks; don't reach for concrete types in `Gateway`.
- **Comments explain WHY**, not what. See clusters around `engine.rs:684`, `engine.rs:1027`, `engine.rs:1480` for examples.

## Hard rules in the engine

These are intentional and load-bearing. Do not change without strong reason + matching test.

- **`parallel_tool_calls: false` for gateway-owned server tools.** Ordinary client tools honor the typed request/profile default, but Brave Search and image-analysis loops force sequential calls. Multi-tool server turns interleave badly with replay and internal tool/result loops.
- **`WEB_SEARCH_ROUNDS_HARD_CEILING = 25`** in `engine.rs:1032`, enforced regardless of config. Defense against infinite tool loops.
- **`OPENAI_MAX_STOP_SEQUENCES = 4`** in `chat.rs:81`. Returns 400 — do not silently truncate.
- **`API_LOG_BODY_LIMIT_BYTES` / `API_LOG_PAYLOAD_DUMP_LIMIT_BYTES`** in `http.rs:51-52`. Don't bypass.
- **Failover only pre-first-chunk** (`upstream.rs:407-419`). Mid-stream provider failure surfaces as error — never retry, never duplicate tokens.
- **Routing providers are not failure fallbacks.** With explicit `upstreams`, only the selected upstream's nested `fallback_upstreams` are failover candidates. Never fail over to the next routing upstream just because the selected provider failed.
- **`web_search` tool stripped from request when `brave_api_key` is unset.** Engine also relaxes `tool_choice` to `"auto"` when the only tool was stripped (`engine.rs:1536-1558`).
- **Provider-side `web_search` is single-purpose.** Runtime execution supports search/query actions only; `open_page`, `find_in_page`, and unknown actions are rejected. Failed/timed-out Brave calls are injected as model-visible text so the turn can complete.
- **Mixed provider-side and client-side tool calls are rejected.** A turn cannot hand off client tools and run Brave search in the same upstream tool-call batch (`engine.rs:1290-1357`).
- **`response.web_search_results`** is a non-standard additive SSE event consumed only by the Anthropic converter. OpenAI clients ignore unknown events, so this stays compatible. See `engine.rs:1480-1485`.
- **`previous_response_id` is unsupported** and must continue to return 400 from canonical lowering. Replay is internal SHA256-prefix state, not OpenAI hosted response retrieval.
- **Inbound `Content-Encoding` is decoded in `log_api_call`.** `zstd`/`gzip`/`deflate`/`br` request bodies are decompressed BEFORE the `Json`/`Bytes` extractors run (codex-tui 0.145+ ships `Content-Encoding: zstd` via `enable_request_compression`). The `Content-Encoding` header is stripped and `Content-Length` is corrected on the rebuilt request so the upstream (which builds its own request from the parsed body) never sees a stale encoding. Unknown encodings return `415`, not a misleading 400 JSON-parse error. Bodies are fully buffered, so decode is synchronous in place (`zstd`/`flate2`/`brotli` crates) — do not add `async-compression`.
- **Image generation tools are stripped before upstream.** They remain accepted in Responses wire types but are not sent as chat tools.
- **Request-intrinsic 4xx `{400,413,415,422}` never cools/fails over a provider.** The leaf tags these `FailoverDisposition::Terminal` (`upstream.rs` `dispatch_chat_stream`); `401/403/404/408/429`/5xx/connect/timeout keep failover + cooldown unchanged. Limitation: a request-intrinsic 400 is therefore NOT retried on a differently-capable provider — acceptable because E2b removes images before dispatch, and a non-image request-intrinsic 400 would reject identically elsewhere.
- **No raw image reaches a non-native-vision backend.** A role-agnostic residual-image pass (`engine.rs`, after `activate_image_agent`, gated on `!backend_is_native_vision`) sweeps every `ResponseItem` (not just `role=="user"`) for `ContentItem::InputImage` (`image_url` or `file_id`) the active G4 agent left behind and either degrades it to a text placeholder (`unsupported_image_policy: placeholder`, default) or rejects the turn pre-dispatch with a 4xx (`reject`) — never forwarded raw. A degraded turn forces `request.store = false` to bypass the replay cache both ways (lookup + store), since two different images collapse to byte-identical placeholder text at the same position.

## Config resolution order

Global → matched model profile templates (`extends:` in order) → matched model profile → explicit request fields. Profile-root shorthand keys merge into `upstream_chat_kwargs` via custom `Deserialize` (`config.rs:60-89`); explicit `upstream_chat_kwargs:` wrapper overrides shorthand on conflict.

Profiles are considered against the resolved catalog model, the configured upstream-model remap target, and the original request model, de-duplicated in that order. For kwargs, later matches override earlier matches, so request-model profile settings beat backend-model profile settings on conflict. For `system_prompt_prefix`, global prefix is prepended and the most specific matched profile prefix is appended before request `instructions`.

`upstreams: [...]` switches the app to model-routing mode. `/v1/models` exposes the ordered union of primary upstream model catalogs plus fallback `exposed_model` aliases. Exact model id wins; normalized alias routing uses `canonical_model_key` and only succeeds when it maps to one unique id. Blank/missing/unavailable/ambiguous models default to the first model in the first non-empty provider catalog.

## Testing

- Integration tests: `tests/gateway.rs` (one file, ~5700 lines, 79 `#[tokio::test]` functions). Use `MockUpstream` (`tests/gateway.rs:51-99`) for in-process gateway tests, `MockSearch` for Brave behavior, `PendingChunkUpstream` for cancellation, or wiremock for HTTP-level routing/failover/proxy behavior.
- Prefer adding to `tests/gateway.rs` over creating new test files unless the new file is a focused topic suite.
- Streaming tests: collect SSE events into a `Vec<SseEvent>` and assert on the sequence, not on timing.
- Replay tests must hash the same `(model, instructions, items)` tuple as `replay::hash_visible_history` — keep them in sync.
- Adapter tests should include both streaming and non-streaming collectors when converter behavior changes.

## Don'ts

- Don't add direct converter between two non-canonical shapes — go through Responses.
- Don't add a typed field for a provider-specific knob if `extra_body` works.
- Don't bypass `redact_payload_secrets` in `http.rs` when adding new logged surfaces.
- Don't introduce blocking IO on the tokio runtime. Upstream request log uses `spawn_blocking` for a reason.
- Don't silence cancellation. Every long-running task in `run_turn` selects on `tx.closed()` so client hang-up cancels upstream work — preserve that pattern.
- Don't lower the hard ceilings listed above.
- Don't leak server-side Brave search internals into Chat Completions output. Chat hides `web_search_call`; Anthropic gets `server_tool_use` + `web_search_tool_result` from `response.web_search_results`.
- Don't add CI/CD or new top-level files without checking with the user first.
- Don't store the dashboard auth TOKEN/SESSION_KEY in the persisted `Config` struct (it's `#[derive(Debug, Clone)]` — secrets would leak) — read them env-only in the dashboard auth layer.
- Don't retain `Bytes` slices of the 256 MiB middleware body buffer in the dashboard FlowStore — copy via the capped/redacting streaming serializer (a slice keeps the whole backing allocation alive).
- Don't put dashboard snapshot bodies on historical snapshots — snapshots hold body-free `SnapshotFlowSummary` only (body retention on snapshots recreates a 135 GiB worst case).
- Don't drive a single global `seq` watermark across monitor/flow/metrics/topology — use per-domain `{domain, seq}` cursors (a global watermark discards valid lower-numbered sibling frames).

## Quick gotchas

- `flatten_content` defaults to `true` — multimodal text-only content gets flattened to bare string before going upstream. Some providers expect arrays; the option is configurable.
- `OPENAI_API_KEY` is a fallback upstream key when `upstream_api_key` is unset.
- Chat and Anthropic ingress set canonical `store=false`; raw Responses defaults to `store=true`, enabling replay unless the caller disables it.
- `/v1/messages` has HEAD/OPTIONS probe routes returning `204` with `Allow: POST, HEAD, OPTIONS`.
- `/v1/models` is reshaped to Anthropic-style pagination when `anthropic-version` or `anthropic-beta` is present; OpenAI-style responses can preserve upstream `ETag`, Anthropic-shaped responses do not.
- `/health` returns `{"status":"healthy"}` and `/` returns `{"status":"ok"}`. There is no `/healthz` route.
- `/v1/completions` is a raw upstream proxy with header filtering. In routing mode it resolves the request body `model`, including exposed fallback aliases.
- `MonitorHub::disabled()` is a no-op used when `--with-debug-ui` is off — production has zero monitor overhead, don't add unconditional broadcast sends.
- `UpstreamModelCatalog` caches `/v1/models` for 300s (`engine.rs:56`). Tests that depend on catalog changes need to construct a fresh `Gateway`.
- `RoutingUpstreamClient` also caches the union model catalog for 300s (`upstream.rs:32`). Tests that change provider catalogs need a fresh router/client or must account for the cache.
- `RawOutput` writes every SSE event whose name ends with `.delta` and has a string `delta`, including reasoning/refusal/tool-argument deltas; raw mode suppresses tracing output to keep stdout clean.
- Anthropic `/v1/messages` stream-shape invariants (one terminal `message_delta`, signed `thinking` blocks, etc.) are asserted by `src/adapters/responses_to_anthropic/conformance.rs` — `pub`, not `#[cfg(test)]`, so integration crates reach it at `llmconduit::adapters::responses_to_anthropic::conformance::{assert_stream_conformant, assert_sse_conformant, Surface}`. A `thinking` block's `signature_delta` is the real upstream one when present, else a synthetic `llmconduit-synthetic-v1:<sha256>` marker (`SYNTHETIC_SIGNATURE_PREFIX`, `models/anthropic.rs`) — stripped on Anthropic ingress (`anthropic_to_responses.rs`) so a client echo-back is never replayed upstream as a genuine signature.
- Dashboard (T13): `/v1/completions` is a raw passthrough and is NOT instrumented by the FlowStore on purpose (it bypasses the engine); the FlowStore whitelist is `/v1/responses`,`/v1/messages`,`/v1/chat/completions` only.
- Dashboard (gap 04): the flow `client_label`/`client_source` (key-hash `key-<hex>` / configured-id / UA fallback) is derived in `log_api_call` at the PRE-redaction seam — the only point the raw key is readable; it is SHA-256-hashed in-place and never stored. The optional caller-id header NAME is read env-only via `LLMCONDUIT_DASHBOARD_CLIENT_HEADER` (e.g. `x-client-id`), never on the persisted `Config` (the header name is non-secret; only the key VALUE is). If that configured header NAME is itself a sensitive key carrier (per `redaction::is_sensitive_payload_key`, e.g. `api-key`/`authorization`), its value is HASHED like any key — never emitted verbatim — and suppressed if it carries no usable key; the key-candidate order is bearer → `x-api-key` → sensitive-configured-header, each trimmed/skipped-if-blank before fallback.
- `LLMCONDUIT_BUILD_DASHBOARD` is a BUILD-time env var (build.rs runs `npm run build` + embeds via `include_dir!`); without it `cargo build` still succeeds on a node-less host (a stub shell is embedded). Docker builds the frontend in a separate Node stage and passes its output through the mutually exclusive `LLMCONDUIT_DASHBOARD_DIST` build-time path, keeping Node out of the final image. Runtime gating is the existing `--with-debug-ui`.
- The dashboard WS envelope is BATCHED `DashboardFrame{domain,seq,batch:Vec<payload>}` (one per `DebugUpdate`, seq = `DebugUpdate.sequence`) — `/debug/ws` keeps the BARE `DebugWsMessage` contract untouched. Don't add per-frame seq to `DebugWsMessage`.
- Durable turn capture (F1): `turn_capture_dir` arms an opt-in, on-disk, per-turn diagnostic on its OWN gate — INDEPENDENT of `--with-debug-ui` — over the SAME `/v1/responses`,`/v1/messages`,`/v1/chat/completions` whitelist. It is a REDACTED, BOUNDED-MEMORY surface: request bodies route through the SAME secret + image-URI redactors as the `body_payload` log (a new logged surface must not bypass `redact_payload_secrets` — line 137), and each section (`inbound_request`/`upstream_request`/`upstream_response`/`served_response`) STREAMS to a per-turn temp file under `<turn_capture_dir>/.work/<api_call_id>/` (never a full body in RAM, never a `Bytes` slice of the 256 MiB middleware buffer — line 144). At the both-`done` barrier (engine terminal + served-body tee) the sections assemble by streaming-escape into an ATOMIC single JSON `<turn_capture_dir>/<api_call_id>.json` (tmp → fsync → rename), keyed by `api_call_id`, then the `.work/<id>/` dir is best-effort deleted. Age-rotated via `debug_log_max_age_hours` — `debug_log_dirs()` includes `turn_capture_dir`, so `log_rotation::cleanup_dump_files` prunes the `<id>.json` and `cleanup_orphan_work_dirs` sweeps crash-orphaned `.work/<id>/` in the same startup pass. `served_response`/`upstream_response` are raw model OUTPUT (streamed, not secret-scanned beyond image-URI redaction on the failed-body path). The disabled sink (no `turn_capture_dir`) is a zero-op — no thread, no allocation, no fs.
- Dashboard (gap 05): upstream RESPONSE/ERROR-body capture is a SEPARATE opt-in gate from the debug UI — `LLMCONDUIT_DASHBOARD_CAPTURE_UPSTREAM_RESPONSE=1/true/yes` (env-only, never on `Config`); `--with-debug-ui` alone arms only REQUEST capture. The body lands on the LIVE `FlowRecord.upstream_response` only (`UpstreamResponseBody{bytes,truncated}`), NEVER on `SnapshotFlowSummary` (body-free invariant). It reflects the TURN's FINAL ATTEMPT's outcome: the failover loop (and the bare-leaf dispatch) CLEARS the staged body at the START of EACH attempt (`clear_pending_response_body`, the SAME per-attempt reset seam as `arm_attempt_header_byte`); an HTTP-status failure then STAGES its body on the shared `ServingToken` (`set_pending_response_body`, last-writer-wins within one attempt) at `dispatch_chat_stream`'s terminal-error sites via `capture_response_body` (the SAME capped/redacting `capture_capped_redacted` path — copies the already-read error `String`, never slices the 256 MiB buffer; `truncated = raw.len() > BODY_CAP`); a provider that SERVES the turn also clears it (serve-success seam); and the L1 `TelemetryGuard` commits the token's pending body to the record at finalize. So whatever sits on the token at finalize is the FINAL attempt's: A 500 → B 200 ⇒ `None`; A 500 → B 503 ⇒ B's body; A 500 → B connect/timeout/no-first-chunk (a body-less failure that never re-stages) ⇒ `None`. An empty/non-JSON upstream body is recorded as the `[redacted: unparseable body N bytes]` marker (`Some`), distinct from `None` (capture off / no body / a final body-less failure / a later provider served). Projected onto the LIVE `/dashboard/api/flows/:id` detail as the optional `FlowDetailBody.upstream_response = {body, truncated}` (parsed; absent when the record field is `None`) — kept OFF `FlowRow` list rows + snapshot summaries. `/v1/completions` still uninstrumented.
- Dashboard metrics tiles are built by ONE function: the live WS `metric_tick`/initial-snapshot AND the REST `/metrics` all go through `dashboard_api::metrics_body` (pass the live `active_stream_count` + `gateway.price_table()`). Don't add a second tile builder. Each `MetricWindow` carries `samples` (terminal-flow count) — the don't-lie-with-zeros signal: `samples == 0` ⇒ frontend renders latency/tok-s/cost/err% as `—` (unavailable), while `req/s`/`active_streams` stay numeric.
- Dashboard (gap 10b): the gap-02/03 spine fields (`PhaseTimings` flattened siblings + `attempts[]` + `first_upstream_byte_ms`) are projected onto the frontend-CONSUMED wire DTOs — `FlowRow` (`/flows` + `/snapshot`, via `from_record`/`from_summary`), `FlowDetailBody` (`/flows/:id`), and the live WS `DashboardPayload::FlowStatus` (`flow_status_payload`) — additive, `skip_serializing_if` so an unmeasured phase/attempt is ABSENT (never `0`). When adding a field to the internal `FlowRecord`/`SnapshotFlowSummary`, remember it does NOT reach the React app until you also project it onto these three consumed DTOs.
- Dashboard (gap 12): per-provider latency (p50/p95/p99) + error distribution is aggregated in `MetricsLayer` off the EVICT-SAFE terminal `attempts[]` (`TerminalMetricsInputs.attempts`, fed at `MetricsLayer::record_terminal`'s trailing `attempts` arg) — NOT a `FlowStore` read — so a FAILED PRIMARY is counted toward its provider even if the record was evicted before finalize. It REUSES the global 30-bucket `Histogram` + window-ring/slot/eviction (no second percentile impl); per-provider memory is bounded BOTH at `MAX_TRACKED_PROVIDERS=64` per slot AND at the aggregate-union (rotating-alias defense), overflow folding into the fixed `__other__` key; error keys are the bounded gap-03 `AttemptErrorClass` taxonomy. Exposed additively as `TopologyNode.per_provider: Option<ProviderLatency>` on the REST `/topology` + `/snapshot` node ONLY (built by `from_health_with_metrics` off the m1 window) — the LIVE WS `topology_frame`/`topology_snapshot` carry `None` (they don't join metrics, like their `0.0` edges), so a per-provider tile that must refresh live reads the REST/snapshot path, not the WS topology frame. Absent ⇒ zero in-window samples (don't-lie-with-zeros, renders `—`); a present tile is always `data_quality:"derived"` with `samples>=1`.
- Dashboard (gap 14): the FRONTEND failure taxonomy is PURE (`components/FlowTable/failureTaxonomy.ts` DOM-free model + `FailureTaxonomy.tsx` aggregate panel mounted above `FlowTable` in `FlowsView`; the enriched `FlowDetail` `ErrorTab`). `failureTaxonomy(rows)` groups OBSERVED flows by `<provider>|<model>` × failure REASON — ALWAYS a BOUNDED key (the LAST failed attempt's gap-03 `error_class` FIRST → a WHITELISTED `TerminalReason` code (`stop`/`length`/`tool_calls`/`content_filter`/`other`) → `__unclassified__`; a FREE-FORM `terminal_reason` is NEVER a group key — the `finalize` seam stamps arbitrary capped-but-unbounded strings, so keying on them blows up cardinality + breaks gap-03's bounded taxonomy) — with a DERIVED error rate. It reads the SAME `useFlowRows(filters)` population as the table (no extra fetch — reuse it for gap-16 control-room failure tiles directly). The panel ALWAYS renders: a zero-sample window is an EXPLICIT `unavailable` `—` (NOT hidden, NOT `0%`). The captured upstream error BODY shown in the `ErrorTab` is the gap-05 `FlowDetailBody.upstream_response` (LIVE `/flows/:id` ONLY — read via `frozenDetail`, live-detail only). The ErrorTab's "No error." empty state depends ONLY on `!isError` (`isError = status==='failed' || monitor joinError`): a GENUINE error/failed flow NEVER reads "No error." — independent of `seeking`/capture suppression — it ALWAYS renders an explicit capture state (captured body `measured`; live capture-disabled `unavailable`; or, while SEEKING a historical flow where the live body is suppressed, a DISTINCT historical `unavailable` state "capture unavailable on historical view — live-only"). A benign completed `terminal_reason` shows the reason but no capture block; a clean success reads "No error.". The TS `FlowDetail.upstream_response` + `isFlowUpstreamResponse` guard mirror the existing Rust DTO — no backend change was needed (the projection already existed). `ui/Panel.tsx` now forwards arbitrary `<div>` props (`data-testid`/`data-*`/`aria-*`) to its root (were silently dropped) — tag panel surfaces directly, don't wrap.
- Dashboard (gap 13): the FRONTEND per-provider tile (`components/viz/providerLatency.ts` pure model + `ProviderLatencyTile.tsx`, mounted in `CooldownTooltip` replacing the old global p99; `RadialTopology` node sizing/error-ring via `providerNodeEmphasis`) reads the gap-12 `TopologyNode.per_provider` from the REST `/topology` (live) / `/snapshot`-node (seek) path — NEVER the live WS `topology_update` frame (which carries `per_provider` ABSENT). `useTopologyQuery` returns a `perProviderById` map derived off the LIVE REST `data` (the stable source — a WS `topology_update` re-seeds `topologyNodes` via `setTopology` with `per_provider` ABSENT, clobbering the store node's copy); `TopologyView` resolves a node's metrics as `perProviderById[id] ?? storeNode.per_provider` (REST live → frozen-snapshot node while seeking, which the `/snapshot` reshape populates). When a future surface (e.g. gap 16 control-room provider tiles) needs per-provider data, reuse THIS read pattern — adding `per_provider` to the WS frame would be a NEW backend gap. The mock attaches `per_provider` on the REST/snapshot nodes but STRIPS it on the WS `topologyFrame` (mirrors Rust `from_health`), so e2e/tests prove the REST-path read.
- Dashboard (gap 16): the control-room OVERVIEW is the 5th hash route `#/overview` (`views/overview/OverviewView.tsx`) — a THIN composer that COMPOSES the gap-01–15 surfaces; it adds NO backend seam and re-fetches nothing. Routing slots in via `router/useHashRoute.ts` (`RouteName`/`ROUTES`), `views/registry.ts`, `NavTabs.tsx` LABELS, and the e2e `harness.ts` `VIEWS` (so the per-view screenshot loop covers it — its `overview-chromium-linux.png` baseline is a real view baseline). NEW pure model `views/overview/overviewModel.ts` (DOM-free, sibling of `failureTaxonomy.ts`): `topByVolume`/`topByCost(rows, 'model'|'provider')` (cost inherits the WEAKEST `cost_confidence` of contributing PRICED flows — mixed confident+estimated ⇒ estimated, never upgraded; no priced flow ⇒ `—`, never `$0.00`; `topByCost` ranks only priced groups) + `tokenMix(rows)` (unreported optional cached/reasoning ⇒ `—`, never a fabricated `0`). WIRE SOURCE (the recurring trap — honored): per-provider latency/error tiles read the gap-12 `ProviderLatency` from the REST `/topology`+`/snapshot` node via the gap-13 pattern `useTopologyQuery().perProviderById ?? storeNode.per_provider` (NEVER the WS `topology_update` frame, which carries `per_provider` absent — deriving from flows would hide failed primaries); all flow-row tiles (volume/cost/failures via gap-14 `failureTaxonomy`, clients via gap-15 `clientRollup`, context via gap-09 `aggregateContextPressure`, token-mix) read the `useFlowRows(filters)` FLOW-LIST population (NOT the live `flow_status` frame); the headline echoes the live `metrics` store (gap-01 tile) + a LIVE-only `$/min` `useMetricStream` trend. The flow filter applies, so a topology/sankey/client cross-link re-scopes the WHOLE overview. Every tile is DQ-tagged + don't-lie-with-zeros (empty window ⇒ `—` empty-state, not an all-`0` board). When a future overview tile needs per-provider data, REUSE the gap-13 REST-path read — adding `per_provider` to the WS frame would be a NEW backend gap.
- Dashboard (gap 15): the FRONTEND client/key attribution is PURE (`components/FlowTable/clientAttribution.ts` DOM-free model — `clientCell(flow)` + `clientRollup(rows)`; `ClientRollup.tsx` collapsible "by client" panel under `FlowTable`; the CLIENT column cell in `FlowTable.tsx`). It consumes the gap-04 `client_label`/`client_source` which are on the wire-facing `FlowRow` (`/flows` + `/snapshot`, projected by `dashboard_api::from_record`/`from_summary`) ONLY — NOT on the live WS `FlowStatus` (`flow_status_payload`) or `FlowDetailBody`. So a surface needing client attribution must read the FLOW-LIST population (the store seeds it from the WS snapshot + the REST `/flows` query via `useFlowRows`), NOT the live `flow_status` frame; the gap-16 control-room "top clients" tiles can reuse `clientRollup(rows)` directly off `useFlowRows(filters)` (no fetch). Source-STRENGTH is the DQ rule: `key_hash`/`configured_header` ⇒ strong/`measured`; `user_agent` ⇒ weak/`derived` (rendered visibly weaker + an amber `ua` badge — NEVER a confirmed identity); ABSENT ⇒ `—`/`unavailable` (don't-lie-with-zeros). The shown label is the gap-04 one-way `key-<hex>` HASH (or configured-id / UA) — NEVER a raw key (gap 04 hashes it in place pre-redaction); showing the hash to the auth-gated operator is the INTENDED diagnostic purpose, not a leak. `patchFlowStatus` (dashboardStore) now CARRIES `client_label`/`client_source` from the prior row — the live `flow_status` frame omits them (gate-F field), and rebuilding the row fresh would otherwise DROP the snapshot-seeded attribution. A `client` facet was added to `FlowFilters`/`flowFilterStore` (`setClient`) + the `FilterBar` (a client chip group); `useFlowRows` exposes distinct `clients[]`. The mock seeds attribution on the flows (api_006 deliberately ABSENT ⇒ the `—`/unattributed path).
