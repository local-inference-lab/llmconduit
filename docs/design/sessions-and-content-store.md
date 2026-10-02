# Sessions, harness detection, and the content store

Status: design for review. Target branch: `integration/upstream-control-plane`.

This document covers three of the gaps identified on 2026-09-12: attaching
requests to sessions and nested sub-sessions, storing full request bodies
without duplicating shared context, and detecting requests that break the
upstream KV/prefix cache. It also fixes the pi-agent header convention.

## Goals

- Every persisted request is attributed to a **harness** (Claude Code, Codex,
  pi, oh-my-pi, pi-agent, or unknown) and to a **session tree node**.
- Session trees nest arbitrarily: session, sub-session, sub-sub-session.
- Detection is **data-driven**. Shipped profiles cover the common harnesses out
  of the box; operators can add, reorder, override, or disable profiles in
  YAML without touching Rust. New extraction primitives are Rust enum variants.
- Request bodies are stored **in full**, split per item, content-addressed, so
  each distinct item (system prompt, tool definition, message) is stored once.
- For each request we know the **divergence point** from its predecessor in
  the same chain, classified, so KV-cache busting is a column you can filter
  on.

## Vocabulary

| Term | Meaning |
|---|---|
| harness | The client program driving the model: `claude-code`, `codex`, `pi`, `oh-my-pi`, `pi-agent`, `unknown`. |
| declared session | A session id the harness put on the wire (header or body field). |
| inferred session | A session node the gateway created because a request did not extend any live chain in its parent. |
| chain | The sequence of requests that each extend the previous one's item list. Every session node owns one chain. |
| item | One addressable unit of a request body: the instructions/system block, one tool definition, one message or input item. |
| divergence | The first item index at which a request's item hashes differ from its chain predecessor. |

A session node is either declared or inferred, and both kinds nest. Examples:

- Claude Code: one declared session per CLI process. A Task/Agent sub-agent
  reuses the same session id but starts a new conversation. It appears as an
  inferred sub-session whose parent is the declared session, and whose
  `spawned_by_request_id` is the request that was in flight when it started.
- pi-agent: every sub-agent is its own process with its own session id. With
  the header convention below it declares its parent, so the tree is exact.
- Upstream pi and Codex: declared session per process; sub-agents, if any,
  are inferred as for Claude Code.

## Harness detection

### Config shape

```yaml
control_plane:
  sessions:
    # Whether to infer sub-sessions from prefix lineage when the harness did
    # not declare them. Per-profile override below.
    infer_sub_sessions: true
    # Shipped profiles are evaluated after `harnesses` unless `builtin: replace`.
    builtin: extend            # extend | replace | disable
    harnesses:
      - name: my-internal-tool
        match:
          all:
            - header: { name: x-llm-harness, regex: '^my-internal-tool' }
        version:            { header: x-llm-harness, regex: '/(.+)$' }
        session_id:         { header: x-llm-session-id }
        parent_session_id:  { header: x-llm-parent-session-id }
        session_kind:       { header: x-llm-session-kind }
        sub_sessions: declared     # declared | infer | none
```

Profiles are evaluated in order; the first whose `match` succeeds wins. A
profile named the same as a built-in replaces that built-in in place.

### Extractor primitives

| Primitive | Meaning |
|---|---|
| `header: { name, regex? }` | Header value, optionally reduced to the first capture group. Presence check when no regex. |
| `body: { path, regex? }` | JSON pointer into the parsed inbound body (`/metadata/user_id`), optionally reduced by regex. |
| `first_of: [ ... ]` | First primitive that yields a non-empty value. |
| `const: "value"` | Literal. |
| `all: [...]`, `any: [...]`, `not: {...}` | Match combinators. |

The special value `${conversation_id_header}` in a header name resolves to
the configured `auth.conversation_id_header`.

### Shipped profiles

Fingerprints below were verified against the harness sources and official
docs on 2026-09-12 (Claude Code gateway-protocol doc; codex-rs; pi and
oh-my-pi repositories). Where a fact is only observed by third parties it is
marked as such.

| name | match | session_id | sub-session id | parent | sub-sessions |
|---|---|---|---|---|---|
| `pi-agent` | header `x-llm-harness` starts with `pi-agent` | `x-llm-session-id` | `x-llm-session-id` | `x-llm-parent-session-id` | declared |
| `claude-code` | header `x-claude-code-session-id` present, or UA `^claude-cli/` (third-party observed), or body `/metadata/user_id` parses as JSON with `session_id` | `x-claude-code-session-id`, else body `/metadata/user_id` JSON `session_id`, else legacy `_session_<uuid>` capture | `x-claude-code-agent-id` (present only on sub-agent requests; fresh per spawn) | `x-claude-code-parent-agent-id` (nested agents), else the session | declared, infer as fallback |
| `codex` | header `originator` starts with `codex` or UA `^codex_cli_rs/` | header `session-id` (legacy `session_id`), else body `/client_metadata/session_id` | header `thread-id`, else `x-client-request-id`, else body `/client_metadata/thread_id` | header `x-codex-parent-thread-id`, else body `/client_metadata/x-codex-parent-thread-id` | declared, infer as fallback |
| `oh-my-pi` | UA `^omp/` | `x-claude-code-session-id` (Anthropic protocol), else `session_id`, else `x-client-request-id`, else body `/prompt_cache_key` | same | none on the wire | infer |
| `pi` | UA `^pi (` or (`x-session-affinity` present and no other profile matched) | `x-session-affinity`, else `x-client-request-id`, else `session_id` | same | none on the wire | infer |
| `generic` | always | first of `x-llm-session-id`, conversation header, `session-id`, `session_id`, `x-session-id`, `x-session-affinity`, `x-client-request-id`, body `/prompt_cache_key`, body `/user` | same | `x-llm-parent-session-id` | infer |

Notes from the verification:

- Claude Code always speaks Anthropic Messages, also behind a gateway. Its
  `x-codex`-style fine identifiers are the two agent headers above; the
  sub-agent header is added on top of the unchanged session header, so a
  Claude Code sub-agent maps to a declared sub-session whose parent is the
  declared session (or the parent agent when nested).
- Codex sub-agents (`x-openai-subagent: collab_spawn|review|compact|...`)
  share the parent's session id and get their own thread id plus a parent
  thread id, so Codex trees are exact. `x-openai-subagent` is stored as
  `session_kind`.
- Upstream pi only sends session headers on Chat Completions when
  `sendSessionAffinityHeaders` is on (default off for generic endpoints); on
  Responses it always sends `x-client-request-id` and `session_id`. Its
  sub-agent example spawns a separate process with a fresh session id and no
  parent pointer, so pi sub-agents are inferred.
- oh-my-pi's task tool runs sub-agents in-process with a fresh session id and
  no parent header; inferred as well.
- Session kind values (`x-llm-session-kind`, `x-openai-subagent`) are stored
  verbatim on the session node.

### Open header convention

Any harness can opt in to exact attribution by sending:

| Header | Value |
|---|---|
| `x-llm-harness` | `<name>/<version>` |
| `x-llm-session-id` | the harness's own session id |
| `x-llm-parent-session-id` | the parent's session id, when this is a sub-agent |
| `x-llm-session-kind` | free text: `main`, `subagent`, `compaction`, `oracle`, ... |

The `generic` profile honors these, so an unknown harness that sends them is
attributed correctly without a profile.

## pi-agent changes

Our vendored pi (0.83.0) still sends the OpenAI SDK default User-Agent and,
with `sendSessionAffinityHeaders: true` in `models.json`, the three affinity
headers. The extension below makes attribution exact regardless of that flag.

- New extension `extensions/llm-session-headers`, registered on
  `before_provider_headers`. It sets the four headers above from
  `ctx.sessionManager.getSessionId()`, the package version, and the new CLI
  flag `--parent-session-id` that the subagent manager passes when it spawns
  or restarts a child (persisted in `subagent-meta.json`).
- The five `completeSimple` bypass sites (oracle, image-analyzer,
  custom-compaction x2, playwright) pass the same headers explicitly, with
  `x-llm-session-kind` naming the caller. Built-in compaction keeps its
  throwaway session id but sends the real session as parent and kind
  `compaction`.

## Content store

### Item splitting

The inbound body is split per protocol on the raw JSON, before any
normalization, so the stored bytes reproduce what the client sent:

| Protocol | Items |
|---|---|
| Responses | `instructions`, each `tools[i]`, each `input[i]` |
| Chat Completions | each `tools[i]`, each `messages[i]` |
| Anthropic Messages | `system` (string or block list), each `tools[i]`, each `messages[i]` |

Each item is canonicalized (sorted keys, no whitespace), secret keys redacted
with the existing `redaction` rules, then hashed with SHA-256. Data URIs are
kept, not stripped, when `content_store.keep_media` is true (default true);
they dedupe like any other item.

The remainder of the body, with every item replaced by `{"$ref":"<hash>"}`, is
the **skeleton** and is stored as the seq-1 request event payload. The
upstream request (seq 2) is split the same way; its items mostly hash
identically to the inbound ones, so the upstream hop costs almost nothing.
Response events keep their current handling but lose the 128 KiB cap in
favor of a per-blob cap.

### Schema (migration 0007)

```sql
CREATE TABLE content_blobs (
    hash TEXT PRIMARY KEY,           -- sha256 hex of the canonical bytes
    media TEXT NOT NULL,             -- 'json' | 'text'
    size BIGINT NOT NULL,
    content BLOB NOT NULL,
    created_at_ms BIGINT NOT NULL
);
CREATE TABLE request_items (
    request_id TEXT NOT NULL,
    hop TEXT NOT NULL,               -- 'client_in' | 'upstream_out'
    ordinal BIGINT NOT NULL,
    section TEXT NOT NULL,           -- 'instructions' | 'tool' | 'message'
    kind TEXT,                       -- role or item type
    blob_hash TEXT NOT NULL REFERENCES content_blobs(hash),
    PRIMARY KEY (request_id, hop, ordinal)
);
CREATE INDEX idx_request_items_blob ON request_items(blob_hash);

CREATE TABLE sessions (
    id TEXT PRIMARY KEY,             -- gateway uuid
    parent_id TEXT REFERENCES sessions(id),
    kind TEXT NOT NULL,              -- 'declared' | 'inferred'
    harness TEXT NOT NULL,
    harness_version TEXT,
    external_id TEXT,                -- the harness's own id when declared
    session_kind TEXT,               -- from x-llm-session-kind
    client_label TEXT,
    virtual_key_id TEXT,
    depth BIGINT NOT NULL,
    root_request_id TEXT,
    spawned_by_request_id TEXT,
    first_seen_ms BIGINT NOT NULL,
    last_seen_ms BIGINT NOT NULL,
    request_count BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX idx_sessions_parent ON sessions(parent_id);
CREATE INDEX idx_sessions_external ON sessions(harness, external_id);

ALTER TABLE requests ADD COLUMN session_id TEXT;
ALTER TABLE requests ADD COLUMN harness TEXT;
ALTER TABLE requests ADD COLUMN harness_version TEXT;
ALTER TABLE requests ADD COLUMN chain_parent_request_id TEXT;
ALTER TABLE requests ADD COLUMN item_count BIGINT;
ALTER TABLE requests ADD COLUMN shared_prefix_items BIGINT;
ALTER TABLE requests ADD COLUMN divergence_kind TEXT;
ALTER TABLE requests ADD COLUMN divergence_index BIGINT;
ALTER TABLE requests ADD COLUMN cache_bust BIGINT;      -- 0/1
CREATE INDEX idx_requests_session ON requests(session_id, created_at_ms);
CREATE INDEX idx_requests_cache_bust ON requests(cache_bust, created_at_ms);
```

Retention: after the hourly request prune, delete `content_blobs` rows with no
remaining `request_items` reference.

### Reconstruction

`GET /dashboard/api/history/requests/{id}/body?hop=client_in|upstream_out`
joins the skeleton with its items and returns the full body. The inspector
uses this for the three-layer diff and for the chain diff below.

## Lineage and divergence

For each new request the gateway holds the item-hash sequence. It looks up
the **chain predecessor**: the most recent request in the same declared
session (or, without one, the same client label) whose hash sequence is the
longest prefix of this one. The lookup is served from an in-memory index of
recent sequences per session, warmed from SQL on first touch.

| divergence_kind | Condition |
|---|---|
| `append` | predecessor is a full prefix; only new items were added |
| `instructions_changed` | index 0 differs and the section is instructions/system |
| `tools_changed` | first difference is inside the tool list |
| `history_rewritten` | first difference is inside the message list before the predecessor's length |
| `new_chain` | no predecessor shares any prefix |

`cache_bust` is true for every kind except `append` and `new_chain`. The
upstream's reported `cached_tokens`, when present, is stored alongside so an
operator can confirm the prediction against what the server actually reused.

A `new_chain` request inside a declared session with `sub_sessions: infer`
creates an inferred sub-session. Its `spawned_by_request_id` is the request
in the same session that was still streaming when it arrived, if any.

## Dashboard

- Sessions view: tree of session nodes with harness badge, timings, tokens,
  and cache-bust count; click through to the chain's requests.
- Flows table: harness column, session facet, cache-bust chip showing the
  divergence kind.
- Inspector: a "vs previous in chain" tab that renders the structural diff of
  the two reconstructed bodies, anchored at the divergence index.

## Status

- Phase 1 (content store) landed in commit a9bd689.
- Phase 2 (harness detection) landed in commit bf53cae: `crate::harness`,
  the shipped profiles in `src/harness_profiles.yaml`,
  `control_plane.sessions`, and the six `requests` columns from migration
  0008 written at request begin.
- Phase 3 (session tree and lineage) is implemented: `crate::sessions`
  (classifier + bounded in-memory linker with durable warm-up), migration
  0009 (`sessions` table, lineage columns on `requests`), linking at the
  HTTP persistence seam, and the `/dashboard/api/history/sessions*` reads.
  Deviations from the design above: one chain per node (an inferred
  sub-session *is* the chain), `spawned_by_request_id` is the parent chain's
  latest request rather than an in-flight lookup, and ingress now also
  writes `client_label`/`client_source` on the request row.
- Phase 4 (pi-agent) landed in pi-agent on branch `codex-improvements`:
  `extensions/llm-session-headers` plus `extensions/shared/llm-session.ts`;
  the subagent manager passes `--parent-session-id` (a CLI flag, since
  environment variables do not reliably cross the tmux boundary) and the
  five direct `completeSimple` call sites merge the headers with their own
  session kind. Extensions load from source at runtime, so no bundle rebuild
  is needed.
- Phase 5 (dashboard) is implemented: `FlowSessionFacts` on every live flow
  surface (row, snapshot, detail, `flow_status` frame); a harness column,
  cache-bust marker and harness/cache/session facets on the flows table; a
  Sessions view (root list, ancestors/children, requests with lineage chips,
  "show in flows" cross-link); and a Chain tab in the inspector that diffs a
  request's full inbound body against its chain predecessor via the content
  store's body endpoint.

- Phase 6 is implemented: `crate::accounts` + `accounts_api` (users with
  Argon2id passwords, per-user keys with a live YAML ∪ SQL registry, CLI
  `user` commands, user-aware sessions and password login, `requests.user_id`,
  migration 0010); `crate::upstream_metrics` (vLLM/SGLang `/metrics` scraper
  per backend, `control_plane.metrics`); `history/throughput` and
  `history/activity` series; dashboard Account, Throughput and Activity views
  and the username/password login.

## Phases

1. Content store: item splitting, blob store, skeleton events, reconstruction
   endpoint, retention. Replaces the 128 KiB cap.
2. Harness detection engine, YAML config, shipped profiles, fixture tests
   with one recorded request per harness.
3. Session tree and lineage: sessions table, chain predecessor lookup,
   divergence classification, new request columns, history endpoints.
4. pi-agent extension and spawn changes.
5. Dashboard surfaces.
6. Remaining items from the gap list: users and keys, upstream metrics
   scraper, per-model throughput and activity dashboards.
