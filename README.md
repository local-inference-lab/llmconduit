# llmconduit

LLM API gateway for local and OpenAI-compatible chat-completions backends.

It accepts OpenAI Responses, OpenAI Chat Completions, and Anthropic Messages
requests, normalizes them, and forwards them to an upstream
`/v1/chat/completions` server. It can also run server-side tools such as Brave
Search.

![Architecture: clients (Claude Code via Anthropic Messages, Codex/OpenAI via Responses, OpenAI chat clients) route through the HTTP router and adapters into the gateway engine, which applies per-profile shaping (roles, reasoning_effort, capabilities, parallel_tool_calls) and runs server-side tools, then forwards to OpenAI-compatible upstreams (vLLM, OpenRouter) via the upstream client with routing, failover, and cooldown; config.yaml supplies profiles and upstreams.](architecture.svg)

## Build

```bash
cargo build --release
```

## Configure

```bash
./target/release/llmconduit configure
```

The default config path is:

```text
~/.config/llmconduit/config.yaml
```

Configuration is loaded at startup. Restart llmconduit after editing the file.
Older control-plane configs with root-level `auth`, `storage`, `backends`, or
`aliases` are accepted through a deterministic in-memory compatibility migration.
Persist that conversion—and replace legacy plaintext client keys with digests—using
`llmconduit migrate-config --config /path/to/config.yaml`; the rewrite is atomic
and owner-only (`0600` on Unix).

Minimal config:

```yaml
bind_addr: "127.0.0.1:4000"
upstream_base_url: "http://127.0.0.1:8000/v1"
upstream_model: "Qwen3.5"
```

### Inference API keys

Inference authentication is opt-in and defaults to the legacy-compatible
`disabled` mode. To enable fail-closed API-key enforcement, configure a
dedicated SQLite store and supply the verifier pepper through the environment:

```yaml
auth:
  mode: enforce
  store_path: "/var/lib/llmconduit/auth.sqlite3"
```

```bash
export LLMCONDUIT_AUTH_PEPPER='a-long-random-environment-only-secret'
# Required only on the first startup of an empty auth store.
export LLMCONDUIT_AUTH_BOOTSTRAP_KEY='llmc_<at-least-27-more-random-characters>'
```

The bootstrap key receives wildcard inference access. Subsequent keys are
created from the dashboard **Access** view and are displayed exactly once;
only an HMAC-SHA256 verifier and short non-secret prefix are stored. Clients may
send either `Authorization: Bearer llmc_...`, a raw `Authorization` value, or
`x-api-key`. Missing/invalid credentials receive `401`; valid keys without an
endpoint/model grant receive `403`. Authenticated `/v1/models` responses are
filtered to the caller's grants and upstream ETags are stripped.

Key creation and revocation use the existing dashboard session and mutation
gate, so start with `--with-debug-ui` and set
`LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS=1`; browser writes also require the
dashboard CSRF token. The pepper and bootstrap key are never persisted in the
YAML config or returned by read APIs.

Signing in with a management API key creates a delegated session, not a
dashboard administrator. Delegated sessions use the permission-checked
`/dashboard/api/auth/*` routes; catalog and chat access also enforce the key's
inference grants. Legacy account administration, provider inventory and global
telemetry are not available to delegated sessions.

`max_request_body_bytes` limits both received and decompressed request bodies,
including Responses WebSocket messages. Oversized decoded HTTP bodies receive
`413`; decompression runs with bounded concurrency and a bounded zstd window.

To import current OpenRouter endpoint pricing into the authorization store,
set the management credential in the environment and name each model to sync:

```bash
export OPENROUTER_API_KEY='your-openrouter-management-key'
llmconduit pricing sync openrouter \
  --model openai/gpt-4.1 \
  --model anthropic/claude-sonnet-4
```

The command uses the same bounded importer and SQLite snapshots as the
dashboard pricing action. Operator-configured price overrides retain
precedence; unavailable token/cache price components remain unavailable rather
than being reported as zero.

Multi-upstream model routing:

```yaml
upstreams:
  - name: "local"
    upstream_base_url: "http://127.0.0.1:8000/v1"
  - name: "openrouter"
    upstream_base_url: "https://openrouter.ai/api/v1"
    upstream_api_key: "..."
```

When `upstreams` is configured, llmconduit exposes the ordered union of the
primary upstream model catalogs. If a request omits `model`, passes a blank
model, or requests a model that is not currently available, llmconduit uses the
first model from the first upstream with a catalog entry. Requested model names
are normalized against the catalogs, so aliases such as different case or
punctuation route to the exact model id exposed by the backend. If multiple
upstreams expose the same model id, the first upstream wins.

Optional nested fallback providers:

```yaml
upstreams:
  - name: "local"
    upstream_base_url: "http://127.0.0.1:8000/v1"
    fallback_upstreams:
      - name: "backup"
        upstream_base_url: "https://openrouter.ai/api/v1"
        upstream_api_key: "..."
        upstream_model: "openai/gpt-4.1-mini"
        exposed_model: "GPT-4.1-mini"
        upstream_chat_kwargs:
          provider:
            order:
              - z-ai
            allow_fallbacks: true
```

If a selected upstream fails before producing the first chat chunk, only that
upstream's nested `fallback_upstreams` are tried. llmconduit does not treat the
next model-routing upstream as a failure fallback. Fallback models are not shown
in `/v1/models` unless `exposed_model` is set. A fallback `upstream_model` is
optional; when set, fallback requests use that model, otherwise they keep the
routed primary model id. `exposed_model` advertises a fallback model under a
client-facing alias and routes requests for that alias to the declaring fallback
provider.
Fallback `upstream_chat_kwargs` are merged only when that fallback is selected,
with per-model kwargs and explicit request values taking precedence.

The legacy top-level `upstream_*` and `fallback_upstreams` settings still work
when `upstreams` is not configured.

### Control-plane overlay

YAML configurations may add a `control_plane:` namespace without moving or
duplicating the ordinary gateway settings above. The loader retains the original
top-level YAML mapping, so operational edits do not erase newer upstream fields it
does not understand. TOML remains an upstream-compatible, read-only configuration
format and cannot contain this overlay. Environment storage/auth overrides and a
selected legacy SQL database can still activate their corresponding runtime
control-plane behavior with TOML. The current integration reads this state at
process startup; it does not expose control-plane CRUD or hot reload.

Operational backends, profiles, aliases, and virtual keys use stable UUIDs for
references. Names remain the human- and client-facing handles, but renaming an
entity does not break its references. An operational profile contains the full
upstream model-profile shape—including `extends`, `roles`, capability and
reasoning settings—plus its ordered backend UUID chain. An alias expands its
ordered profile UUIDs into one ordered, pre-first-chunk failover route.

See [`config.example.yaml`](config.example.yaml) for a complete Docker-ready
configuration. The essential shape is:

```yaml
control_plane:
  storage:
    backend: sqlite
    url: "sqlite:///data/llmconduit.sqlite3"
    queue_capacity: 1024
    retention_days: 30
  auth:
    require: false
    conversation_id_header: x-conversation-id
  operational:
    backends:
      - id: "10000000-0000-4000-8000-000000000001"
        name: local
        base_url: "http://127.0.0.1:8000/v1"
        # api_key_env: OPENAI_API_KEY
    model_profiles:
      - id: "20000000-0000-4000-8000-000000000001"
        name: qwen-local
        backends: ["10000000-0000-4000-8000-000000000001"]
        upstream_model: Qwen3.5
    aliases:
      - id: "30000000-0000-4000-8000-000000000001"
        name: local
        profiles: ["20000000-0000-4000-8000-000000000001"]
    keys: []
    unknown_model_policy: passthrough
```

`unknown_model_policy: passthrough` preserves the normal catalog/default route
for unknown names; `reject` returns 404 before contacting an upstream, including
when a configured profile has no available backend and would select a different
catalog-default model. This also applies to dashboard chat and Responses
WebSocket turns. Disabled
backends are omitted from operational route plans. Empty provider chains and
dangling or duplicate UUID references fail startup rather than silently changing
routing.

Operational backend credentials may be supplied with `api_key_env`; the named
environment variable is resolved once at startup and its value is never written
back to configuration or shown in debug output. Setting both `api_key` and
`api_key_env` is an error. Containers receive only explicitly forwarded
variables: Compose forwards `OPENAI_API_KEY` for the checked-in example; add an
environment mapping if you choose another variable name.

Client inference authentication is independent of dashboard authentication.
Virtual keys accept `Authorization: Bearer ...` or `x-api-key`. Store deployed
secrets as `sha256:<64 lowercase hex characters>` and keep the plaintext outside
the config; `allowed_aliases` is a list of alias UUIDs and an empty list permits
any model. With `require: true`, a valid configured key is mandatory; starting
with no keys deliberately leaves every protected route inaccessible. The gate
covers `/v1` data routes, including models, token counting, and raw completions
(Anthropic `HEAD`/`OPTIONS` probes remain open). Adding any keys also activates
key verification so requests can be attributed; leave `keys: []` with
`require: false` for an open development gateway.

### Configured provider model discovery

Providers added through the dashboard discover new models automatically at startup
and every five minutes. Discovery is additive: models missing from a later provider
response remain in the saved catalog, and a failed refresh leaves the catalog intact.
Existing providers also default to automatic discovery after upgrading.

Admins can turn off automatic discovery for a provider, allow only selected saved
models, or disable individual models in the Providers page. Disabling discovery
freezes the saved catalog; it does not turn off inference for enabled models.
`allowed_models: null` means all saved models are allowed, `allowed_models: []`
means none are allowed, and a non-empty list permits only those model ids.
`disabled_models` wins over the allowlist. Models remain visible in the admin list
but non-enabled models are excluded from the public model catalog and that
provider's routing. Refreshes and gateway restarts preserve these settings.

The same controls are available through the authenticated, CSRF-protected
`PATCH /dashboard/api/configured-providers/{id}` endpoint:

```json
{"auto_discover": false, "allowed_models": ["vendor/model-id"], "disabled_models": []}
```

All fields are optional. Omit `allowed_models` to leave the current allowlist
unchanged, pass `null` to allow all saved models, or pass an array to replace the
allowlist. `disabled_models` replaces that provider's disabled-model list; pass an
empty array to enable all allowlisted saved models again. Disabling or omitting a
model on one provider does not disable another provider that serves the same model.

### Harness and session detection

Every persisted request records which harness sent it and the session
identifiers it declared: `harness`, `harness_version`, `harness_session_id`,
`harness_sub_session_id`, `harness_parent_session_id`, and `session_kind`.
Detection is data-driven. Built-in profiles cover Claude Code (session and
agent headers, `metadata.user_id`), Codex (`session-id`, `thread-id`,
`x-codex-parent-thread-id`, `x-openai-subagent`), opencode (user-agent only;
sessions inferred), pi, oh-my-pi, our own `x-llm-*` header convention (used by
pi-agent), and a generic fallback over the common affinity headers. Every
stored request keeps its redacted client headers on the `client_in` event, so
a detection result can be checked against what the harness actually sent. Profiles are evaluated in order; the first match
wins. The shipped list is `src/harness_profiles.yaml`; operators add, override
(same name), reorder, or disable profiles under `control_plane.sessions`:

```yaml
control_plane:
  sessions:
    infer_sub_sessions: true     # lineage inference when nothing is declared
    builtin: extend              # extend | replace | disable
    harnesses:
      - name: my-tool
        match: { header: { name: user-agent, regex: '^my-tool/' } }
        version: { header: { name: user-agent, regex: '^my-tool/(\S+)' } }
        session_id: { first_of: [ { header: x-my-session }, { body: { path: /user } } ] }
        parent_session_id: { header: x-my-parent-session }
        sub_sessions: declared   # declared | infer | none
```

#### Session tree and cache-bust detection

Requests are linked into a session tree. A **declared** node is a session or
sub-session id the harness put on the wire; an **inferred** node is created
when a request starts a new conversation inside its parent (a Claude Code
sub-agent that reuses the session id, a pi sub-process with no parent header,
or any client without session ids, which gets one bucket per client key).
Each node owns a chain: the requests that each extend the previous one's item
list (system block, tools, messages, as stored by the content store). Per
request the gateway records `session_id`, `chain_parent_request_id`,
`item_count`, `shared_prefix_items`, `divergence_kind` (`append`,
`instructions_changed`, `tools_changed`, `history_rewritten`, `new_chain`),
`divergence_index`, and `cache_bust`, which is true whenever the difference
falls inside the predecessor's items, the condition that invalidates an
upstream prefix cache. `GET /dashboard/api/history/sessions` lists recent
root nodes (`roots=false` for all), and `GET
/dashboard/api/history/sessions/{id}` returns a node with its ancestors,
children, and newest requests. The index is in memory and bounded; sessions
not seen since startup are warmed from SQL on first touch, with a short
timeout after which the request links cold. `infer_sub_sessions: false`
keeps every request of a declared session on that session's chain and reports
divergences there instead of opening inferred sub-sessions.

The dashboard's Throughput tab charts requests/min, prefill and decode tok/s
and TTFT per model and combined from the gateway series, and shows the scraped
engine state (running/waiting, KV usage, prefix-cache hit rate, prompt and
generation tok/s) per backend and model. The Activity tab rolls requests,
failures and tokens up per user and key; the Account tab manages keys and
users.

The dashboard surfaces all of this: the flows table has a harness column, a
`bust` marker on cache-busting rows, and harness / cache / session facets; the
Sessions tab lists root sessions with their sub-sessions and requests; and the
inspector's Chain tab diffs a request's full inbound body against its chain
predecessor.

Matchers are `header`, `body` (JSON pointer), `all`, `any`, `not`, and
`always`. Extractors are `header`, `body`, `json_string` (parse a string
field as JSON, then read a pointer), `first_of`, and `const`; a `regex` on
`header`/`body` reduces the value to its first capture group. The header name
`${conversation_id_header}` resolves to `auth.conversation_id_header`. Any
harness can opt in to exact attribution by sending `x-llm-harness`
(`name/version`), `x-llm-session-id`, `x-llm-parent-session-id`, and
`x-llm-session-kind`.

### Users and API keys

With a SQL store, the dashboard has user accounts and per-user API keys.
Create the first administrator either from the environment at startup
(`LLMCONDUIT_ADMIN_USERNAME` + `LLMCONDUIT_ADMIN_PASSWORD`, used only while no
user exists) or with the CLI:

```bash
LLMCONDUIT_ADMIN_PW=... ./llmconduit user --config config.yaml create \
    --username koen --admin --password-env LLMCONDUIT_ADMIN_PW
./llmconduit user --config config.yaml list
```

Once a user exists the dashboard login form asks for a username and password
(Argon2id hashes in the `users` table); the shared `LLMCONDUIT_DASHBOARD_TOKEN`
no longer opens the dashboard except on a dev-open loopback listener. The
session cookie carries the signed user identity.

Keys are managed in the dashboard (Account tab) or through the API:
`GET/POST /dashboard/api/keys`, `DELETE /dashboard/api/keys/{id}`,
`GET/POST /dashboard/api/users`, `PATCH/DELETE /dashboard/api/users/{id}`,
`GET /dashboard/api/me`. Every user can create and revoke their own keys;
administrators manage users and everyone's keys. Mutations need the
double-submit CSRF token like the kill route, but not
`LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS`. A created key's plaintext
(`llmc_…`) is returned exactly once; only its SHA-256 digest is stored. The
live key registry is the union of the YAML `keys` and the SQL keys and is
rebuilt on every change, so a new or revoked key takes effect without a
restart. Requests record the key's owner in `requests.user_id`, and
`GET /dashboard/api/history/usage?user_id=` and
`GET /dashboard/api/history/activity?since_ms=&bucket_secs=` roll usage up
per user and key.

### Mesh enrollment administration

When mesh controller mode is enabled, dashboard administrators can manage
provider enrollment from the Providers view. Creating an enrollment key returns
its token exactly once; only its hash is retained. Keys may be time-limited or
usage-limited. Revoking a key immediately disables and disconnects every node
that enrolled with it, as well as preventing future enrollment.

An administrator can also disable or re-enable a single node, or disable an
exact advertised `(endpoint, resource, model)` tuple without restarting the
gateway. Model-level disables are an operational routing control, not a security
quarantine: if a provider is untrusted or may advertise another model alias,
disable its node or revoke its enrollment key instead. Mesh enrollment and
routing administration is restricted to bootstrap dashboard sessions and
administrator user sessions. The mesh-state read and remote Fleet model
lifecycle endpoints additionally accept delegated management credentials with
the corresponding `fleet.models.*` permission. Mutations retain the dashboard's
existing CSRF and mutation-policy checks.

A downstream mesh worker that has local Fleet configured also advertises its
current switchable model inventory. The controller can request a load only for
an exact model id in that live inventory. The command travels over the worker's
authenticated Iroh connection; Fleet remains bound to loopback on the worker,
and its bearer token never leaves that machine. Set `LLMCONDUIT_FLEET_URL` and
either `LLMCONDUIT_FLEET_TOKEN_FILE` or `LLMCONDUIT_FLEET_TOKEN` in the
`llmconduit mesh-worker` environment to enable this capability. The controller
and worker must both use mesh protocol v2.

For a vLLM resource, set `mesh.worker.resources[].capacity_source: vllm` to
derive its slots from `/server_info?config_format=json` (`max_num_seqs`). The
worker refreshes this limit alongside the model catalog, so a port reused by
another model adopts that engine's limit. Explicit availability windows and
exceptions can reduce capacity, including to zero; they cannot exceed the
engine limit. Failed capacity discovery makes the resource unavailable until
discovery succeeds. Other resources retain `capacity_source: configured`
(the default) and their existing availability schedules.

Set `mesh.worker.node_name` to a short friendly worker label when one physical
provider exposes multiple resource slots. The worker sends that label during
enrollment and in every live advertisement; routing ids remain the Iroh endpoint
id plus resource id.

Mesh enrollment and disabled-model state remains in the mesh controller's
dedicated SQLite database. `control_plane.storage: postgres` stores dashboard
accounts, API keys, request history, and metrics; it intentionally does not move
mesh identity state or Access RBAC state into PostgreSQL.

### Upstream engine metrics and throughput

With a SQL store, llmconduit scrapes every backend's Prometheus `/metrics`
endpoint (derived from its base URL by stripping `/v1`; override or disable per
backend under `control_plane.metrics.backends`) every
`control_plane.metrics.scrape_interval_secs` seconds. Derived URLs are only
scraped for self-hosted backends (loopback, private/CGNAT IPs, single-label or
`.local`/`.internal`/`.lan`/`.ts.net` hosts); public hosted APIs are skipped
unless given an explicit `url` or `enabled: true`. Each body is capped at 2 MiB. vLLM V1 and SGLang
families are recognised (running/waiting requests, KV-cache usage, prompt,
generation and cached prompt tokens, prefix-cache hits/queries, TTFT,
inter-token and end-to-end latency sums and counts, prefill/decode time,
finished requests by reason, preemptions, SGLang decode throughput and
realtime prefill/decode token counters) and folded per `model_name`. Samples
are stored in `backend_metrics` tagged `"kind": "upstream"` next to the
gateway health samples (`"kind": "health"`), and served by
`GET /dashboard/api/history/metrics`. Backends without a metrics endpoint are
retried with backoff and never affect the request path.

Independently of upstream metrics, `GET
/dashboard/api/history/throughput?since_ms=&bucket_secs=` returns the
gateway-side per-model/backend series computed from persisted requests:
requests, completed, input/output/cached tokens, and the TTFT and decode-time
sums with their counts, from which prefill and decode throughput are derived.
This works for every backend, including ones that expose no metrics.

Storage backends are `none` (disabled), `jsonl` (write-only diagnostic records;
requires `jsonl_dir`), `sqlite`, and `postgres` (both require `url`).
Remote PostgreSQL URLs must select encrypted transport with `sslmode=require`,
`verify-ca`, or preferably `verify-full`; local and Unix-socket connections may
explicitly use `sslmode=disable`.
`queue_capacity` defaults to `1024` and must be nonzero. `retention_days`
defaults to `30`: SQL request/event/metric rows are pruned hourly, while JSONL
uses daily gateway-owned files and attempts to remove expired files on the first
subsequent append each process-day. Request-path writes use non-blocking enqueue:
saturation drops persistence records instead of delaying inference, and
accepted/drop/failure counters are logged during graceful shutdown.
Queued and in-flight commands also share a 64 MiB allocation budget, configurable
with `LLMCONDUIT_PERSISTENCE_QUEUE_MAX_BYTES`. Exhausting either limit drops the
write immediately; the byte budget is released after the writer finishes it.

The SQL migration set is compiled into the binary and is applied when a SQLite or
Postgres store is opened; migration files do not need to be mounted into the
runtime container. SQLite needs a writable parent directory for the database plus
its WAL/SHM files. The Docker image and Compose file therefore reserve the
nonroot-writable `/data` volume. SQL storage exposes durable request summaries,
event history, retention-windowed usage rollups, and minute-level
provider health/counter samples through the
session-authenticated `/dashboard/api/history/*` reads.

Request bodies are stored in full through a content-addressed store. Each
inbound and upstream request is split into items (the instructions/system
block, every tool definition, every message), each item is secret-redacted,
canonicalized, and hashed, and identical items are stored once in
`content_blobs`. The hop's event row keeps the skeleton: the body with each
item replaced by a hash reference. `GET
/dashboard/api/history/requests/{id}/body?hop=client_in|upstream_out`
reassembles the full body. Image and `data:` URIs are kept in stored items
by default; set `control_plane.storage.keep_media: false` to strip them.
Response events (upstream and served) are retained up to 16 MiB each.
Unreferenced blobs are removed by the hourly retention pass. `none` and JSONL are not
queryable and those endpoints return 503. SQL-backed API-key rows are snapshotted
at startup; there is no live database-key reload. For compatibility with the old
control plane, a seeded legacy `settings.operational` document or relational
operational state is authoritative over YAML operational routes, profiles, keys,
and unknown-model policy. That SQL import is read-only and startup-only;
`migrate-config` rewrites YAML and does not mutate legacy SQL rows.

The first four migrations are byte-identical to the pre-upstream control-plane
history so existing sqlx ledgers remain valid. Some comments in those immutable
historical migrations describe the retired `/ui` user-management implementation;
they are historical text, not current behavior. This build keeps dashboard auth
env-only and exposes no user/config mutation API.

Global and per-model request defaults:

```yaml
system_prompt_prefix: |
  Shared instructions prepended to every request.

upstream_chat_kwargs:
  stream_reasoning: true

model_profile_templates:
  thinking:
    separate_reasoning: true
    chat_template_kwargs:
      enable_thinking: true

model_profiles:
  Kimi-K2.7:
    extends:
      - thinking
    system_prompt_prefix: |
      Extra Kimi-specific instructions.
    chat_template_kwargs:
      preserve_thinking: true

  GLM-5.2:
    extends:
      - thinking
    chat_template_kwargs:
      clear_thinking: false
    upstream_chat_kwargs:
      parallel_tool_calls: true
```

`system_prompt_prefix` is prepended to all Responses, Chat Completions, and
Anthropic Messages requests. A profile-specific prefix is appended after the
global prefix. `upstream_chat_kwargs` merge in this order: top-level defaults,
matched model profile templates, matched model profile, then explicit request
values. In model profiles and templates, extra profile-level keys are shorthand
for upstream chat kwargs; the explicit `upstream_chat_kwargs` wrapper still
works and overrides the shorthand when both set the same key. When a profile
`extends` multiple templates, the `extends` list is applied in declaration
order: later entries override earlier ones, and the profile's own fields
override all templates.

### Reserved `*` profile

A profile keyed `*` is a pure fallback for per-model settings. When a request
names a model that no specific `model_profiles` entry matches, the `*` profile
stands in as that model's profile: its `upstream_chat_kwargs` and
`system_prompt_prefix` apply. When a specific profile DOES match, the `*`
profile is not consulted at all - an explicit match never inherits unset fields
from `*`. The `*` profile can itself `extend` templates, so extending a shared
template is the way to give `*` and explicit profiles common defaults. Use
`model_profile_templates` (`extends`) to share fields between explicit
profiles, not `*`.

Per-model profile matching precedence, highest to lowest:

- The request model - matched by name (case-insensitive) against `model_profiles`.
- The resolved/upstream model (after `upstream_model` rewriting) - matched by name.
- The reserved `*` profile - used only when neither of the above matches.

Top-level config is the base below all profiles: `upstream_chat_kwargs` is the
deep-merge base, and `system_prompt_prefix` is always prepended. Client request
values still override profile settings, as described above.

```yaml
model_profiles:
  # Fallback for any model without an explicit profile.
  "*":
    upstream_chat_kwargs:
      chat_template_kwargs:
        enable_thinking: false

  GLM-5.2:
    upstream_chat_kwargs:
      chat_template_kwargs:
        enable_thinking: true
```

With this config, a request for `GLM-5.2` uses only the `GLM-5.2` profile
(`enable_thinking: true`); the `*` profile contributes nothing. A request for
any other model (e.g. `Qwen-3`) falls back to `*` (`enable_thinking: false`).

### Model capabilities

A profile's `capabilities` block overrides the Anthropic model capabilities
advertised on `/v1/models` for Anthropic clients.

```yaml
model_profiles:
  GLM-5.2:
    capabilities:
      thinking:
        types: [adaptive, enabled]
      effort:
        levels: [max, xhigh, high, medium, low, minimal, none]
      structured_outputs: true
      image_input: false
      pdf_input: false
```

- `supported` is the only knob and defaults to `true`. The simple caps (`batch`,
  `citations`, `code_execution`, `image_input`, `pdf_input`,
  `structured_outputs`) accept a bare bool as shorthand for `{supported: <bool>}`.
- `thinking.types`, `effort.levels`, and `context_management.features` list the
  advertised sub-entries; each inherits the cap's `supported` flag.
- Unknown cap keys, effort levels, thinking types, and context-management features
  are rejected at load.
- A configured cap replaces the base (upstream-supplied, else the default
  capabilities) for that cap key, wholesale; unconfigured caps keep the base.

### Tool-call repairs

Some models mix tool-call encodings: they open the arguments as JSON and then
switch into the markup their own chat template trained them on. GLM does this
with `<arg_key>`/`<arg_value>`, mid-argument, so the result is still valid JSON
and the first argument silently swallows the rest:

```json
{"action": "edit<arg_key>appendContent</arg_key><arg_value>…the real value…",
 "scope": "local"}
```

The harness then rejects the call (`action` is not a valid action) and the agent
retries — intermittently, because the drift happens on long values. A repair
rewrites what an upstream sent, so it is opt-in per model profile and off by
default:

```yaml
model_profiles:
  - id: "20000000-0000-4000-8000-000000000001"
    name: "large/vllm"
    upstream_model: "GLM-5.2-NVFP4"
    tool_call_repairs: [glm_arg_markup]
```

`glm_arg_markup` splits the swallowed key/value back out into real arguments; a
call that does not carry the markup is never touched, and a partial block is
left alone rather than guessed at. With a repair enabled the model's tool-call
argument FRAGMENTS are not streamed (the markup only becomes visible once the
text is complete, and a sent fragment cannot be recalled) — the client receives
the whole, repaired call instead. Text content still streams normally.

### Reasoning effort

A profile's `reasoning_effort` block shapes the upstream `reasoning_effort` field
(the value Claude Code sends as `output_config.effort`, and the value OpenAI
clients send as `reasoning_effort`) and controls the thinking template kwarg the
gateway injects on the Anthropic route. On that route an absent `output_config.effort`
means thinking is disabled, and the upstream chat template would otherwise infer
on/off from the effort field or default it on when the kwarg is absent, so the
gateway injects an explicit `enable_thinking` template kwarg to state the intent
rather than leave it implicit. Effort shaping applies on every converting route
(`/v1/messages`, `/v1/responses`, `/v1/chat/completions`, and
`/v1/messages/count_tokens`); the thinking-template-kwarg injection applies only
on the Anthropic routes (`/v1/messages` and `/v1/messages/count_tokens`).

```yaml
model_profiles:
  GLM-5.2:
    reasoning_effort:
      default: high
      map:
        none: none
        minimal: none
        low: high
        medium: high
        high: high
        "*": high
        xhigh: max
        max: max
      thinking_param_name: enable_thinking
      thinking_param_value_on: true
      thinking_param_value_off: false
```

- `map` translates a client effort level to an upstream effort string. Keys match
  case-insensitively. A level that is not listed passes through verbatim, unless
  the reserved `*` entry is set, which rewrites every otherwise-unlisted level. An
  explicit level always wins over `*`.
- `default` is the effort emitted when the client sends no effort string. `default:
  null` (or omitting it) sends no `reasoning_effort` field. `*` does not apply to
  this case.
- Anthropic clients expect thinking to be **off** unless the request explicitly
  enables it, but some upstreams treat an absent `enable_thinking` kwarg as thinking
  *on*. So on the Anthropic route the gateway always injects a
  thinking template kwarg into `chat_template_kwargs`, stating on/off explicitly
  rather than leaving it to the upstream default or inferring it from the effort
  value. `thinking_param_name` is the kwarg name (default `enable_thinking`);
  `thinking_param_value_on` / `_off` are the values for thinking-on and thinking-off
  (defaults `true` / `false`, but any JSON value is allowed). The injected value
  overrides any static `chat_template_kwargs` default for that key, and a profile
  with no `reasoning_effort` block still injects the built-in `enable_thinking:
  true`/`false`.
- A resolved effort of `none` also forces the off-value on the Anthropic route, even
  when the request enabled thinking. This is what makes a `map` that clamps low levels
  to `none` (e.g. z.ai's `minimal`/`none` -> `none`) actually skip thinking.
- Chat Completions and native Responses clients control the thinking kwarg
  themselves via `chat_template_kwargs` in the request; the gateway never injects
  one for them.
- A profile with no `reasoning_effort` block applies no effort shaping: the client
  effort is forwarded if present, otherwise omitted (no clamp).

### Roles

A per-profile `roles` block maps whole-message roles before the conversation is
sent upstream. It is fail-closed: a role with no matching rule is rejected with
HTTP 400. With no `roles` block configured, messages pass through **verbatim** - all
role shaping is opt-in.

`roles` holds an optional `merge_adjacent` list plus a map of role name to a
rule, or an ordered list of rules. `*` is the wildcard role: it matches any role
that has no explicit key. A single rule is shorthand for a one-element list. In
a list, the first rule whose `when` matches wins; a rule with no `when` always
matches, so put it last as the catch-all.

Per-rule keys:

- `when` (`leading` / `inline` / `always`, default `always`): `leading` matches
  index 0, `inline` matches index > 0, `always` matches any position. Omitting
  `when` is equivalent to `always`; spell it out only to be explicit.
- `action` (`accept` / `reject` / `drop` / `rewrite`, default `accept`):
  `accept` keeps the message in place; `reject` returns HTTP 400; `drop` removes
  the message; `rewrite` renames the role, staying its own turn in place.
- `target_role` (string, required with `action: rewrite`): the new role name.
- `tag` (string, optional): wrap the message content in `<tag>...</tag>`.
- `tag_attributes` (map<string,string>, requires `tag`): render attributes on
  the opening tag, alphabetical by key, XML-escaped (`&` `"` `<`).

Tagging gives the model extra context about a block. For example, rewriting a
`developer` message to `system` with `tag: system-instruction` and
`tag_attributes: {description: "IMPORTANT system message. You MUST follow this with high priority!"}`
wraps the content as
`<system-instruction description="IMPORTANT system message. You MUST follow this with high priority!">...</system-instruction>`.

`merge_adjacent` is a post-pass keyed on the **final** role (after rewrites). It
coalesces each maximal run of consecutive messages that share a final role in
the list into one content-only message joined with `\n\n`. There is no
inline/leading distinction at this level - it only looks at the role messages
end up as and whether they are adjacent. Folding system and tool into `user` is
`rewrite` to `user` plus `merge_adjacent: [user]`, which preserves order.

Resolution order for a message: the explicit role key, then the `*` wildcard,
then fail-closed `reject`.

```yaml
model_profiles:
  # Full-role, system inline ANYWHERE; tool role supported (GLM-5.2, Kimi K2.7).
  # Both group tool runs in-template, so do NOT set merge_adjacent on `tool`.
  GLM-5.2:
    roles:
      "*":       { action: reject }
      user:      {}
      assistant: {}
      tool:      {}
      system:    {}
      developer: { action: rewrite, target_role: system }

  # System-FIRST only (Qwen3.5 raises on a non-first system message). An INLINE
  # system/developer message is rewritten to `user` in place; the index-0
  # message stays system, so Qwen never sees a non-first system.
  Qwen3.5:
    roles:
      "*":       { action: reject }
      user:      {}
      assistant: {}
      tool:      {}
      system:
        - { when: inline, action: rewrite, target_role: user }
        - {}
      developer:
        - { when: inline, action: rewrite, target_role: user }
        - { action: rewrite, target_role: system }

  # System-less model (Gemma): only `user`/`assistant` exist. Fold system and
  # tool into `user` and coalesce the adjacent user runs.
  Gemma:
    roles:
      merge_adjacent: [user]
      "*":       { action: reject }
      user:      {}
      assistant: {}
      system:    { action: rewrite, target_role: user }
      tool:      { action: rewrite, target_role: user, tag: tool_result }
```

### Brave Search

Setting `brave_api_key` enables a server-side `web_search` tool: when a request
asks for the built-in `web_search` tool and the model calls it, the gateway runs
the Brave Search API itself and feeds the results back into the conversation so
the model can answer (or search again) without its own internet access. With no
key set, the gateway strips `web_search` from the tool list so the upstream
never sees it. Related knobs: `brave_max_results` caps results per query
(default `5`); `max_web_search_rounds` caps how many search rounds a single
request may run (default `5`; `0` means unlimited, with a hard ceiling of `25`);
`brave_base_url` is the Brave API endpoint (default
`https://api.search.brave.com/res/v1`).

```yaml
brave_api_key: "..."
brave_max_results: 5
max_web_search_rounds: 5
brave_base_url: "https://api.search.brave.com/res/v1"
```

Optional vision offload — forward images to a separate vision-capable model instead
of the primary upstream:

```yaml
image_agent_enabled: true
vision_url: "http://127.0.0.1:8001/v1"
vision_model: "Qwen3-VL"
```

Whether a backend has native vision support is detected, not guessed: at startup
and every ten minutes the gateway checks each routed (backend, model) pair.
Dashboard-added providers are checked too, including newly discovered models;
provider additions, removals, and model allow/block list changes trigger a new check.
The gateway first reads `/models` metadata (`architecture.input_modalities`):
an explicit `image` input marks the model multimodal, while an explicit non-image
input list marks it text-only. If the metadata is absent or inconclusive, the
gateway sends a one-pixel PNG with `max_tokens: 1`. Metadata avoids inference
requests for providers such as OpenRouter. A success marks the model multimodal; a 4xx
that blames the image marks it text-only; anything else (auth, 5xx, unreachable)
leaves the answer unknown. A profile's explicit `native_vision` still overrides
the probe, and an unknown model keeps the name-based default. Configure or
disable it under `control_plane`:

```yaml
control_plane:
  vision_probe:
    enabled: true
    interval_secs: 3600   # default; each round sends real image requests
    timeout_secs: 20
```

Any image that still reaches a backend without native vision support (whether or not
the agent above is active, e.g. no `vision_url` configured) is degraded instead of
forwarded raw:

```yaml
# placeholder (default): replace the image in place with an instructive text note so
#   the model asks the user to describe it / requests text, instead of guessing.
# reject: fail the turn before dispatch with an HTTP 400 (the provider is never
#   contacted, so it is never cooled down or failed over).
unsupported_image_policy: placeholder
```

## Run

```bash
./target/release/llmconduit start
```

Useful flags:

```bash
./target/release/llmconduit start --raw
./target/release/llmconduit start --with-debug-ui
```

The gateway listens on `http://127.0.0.1:4000` by default.

## Codex

```toml
[model_providers.llmconduit]
name = "llmconduit"
base_url = "http://127.0.0.1:4000/v1"
wire_api = "responses"
requires_openai_auth = false

[profiles.llmconduit]
model_provider = "llmconduit"
model = "Qwen3.5"
```

```bash
codex -p llmconduit "what files are in this directory?"
```

## Docker

The Docker build compiles and embeds the complete dashboard in a separate Node
stage; Node, frontend sources, and SQL migration source files are not present in
the final image. Migrations are copied into the Rust builder because
`sqlx::migrate!` embeds them at compile time. The final image runs as distroless
`nonroot`; `/data` is prepared with matching ownership for SQLite.

Local dashboard builds and tests use Vite 7 and Vitest 4. Use Node 22.12+
(22.x) or Node 24+; the Docker frontend stage uses Node 22.

The quickest local deployment uses the checked-in Docker-ready config and a
named data volume:

```bash
docker compose up --build
curl http://127.0.0.1:4000/health
```

For a private configuration, copy the example and select it without adding it to
the image build context:

```bash
cp config.example.yaml config.yaml
# Edit config.yaml, then:
LLMCONDUIT_CONFIG_FILE=./config.yaml docker compose up --build
```

The config is mounted read-only as bootstrap input; SQLite and its sidecars live
on the `llmconduit-data` volume mounted at `/data`. To remove that durable state,
stop the service and explicitly remove its volume with `docker compose down -v`.

A direct `docker run` equivalent is:

```bash
docker build -t llmconduit .
docker run --rm -p 127.0.0.1:4000:4000 \
  -e LLMCONDUIT_BIND_ADDR=0.0.0.0:4000 \
  -e OPENAI_API_KEY \
  --add-host=host.docker.internal:host-gateway \
  -v llmconduit-data:/data \
  -v "$PWD/config.example.yaml:/etc/llmconduit/config.yaml:ro" \
  llmconduit start --config /etc/llmconduit/config.yaml
```

`/debug` and `/dashboard` exist only when the global `--with-debug-ui` flag is
passed. When GitHub SSO is configured, it replaces the browser login while the
legacy programmatic token endpoint remains available. Create a GitHub OAuth App
whose callback is `https://your-host/dashboard/auth/github/callback`, then set:

```bash
LLMCONDUIT_GITHUB_CLIENT_ID=...
LLMCONDUIT_GITHUB_CLIENT_SECRET=...
LLMCONDUIT_GITHUB_ALLOWED_USERS=octocat,another-login
# Required for admin privileges; omitted means no GitHub user is an admin.
LLMCONDUIT_GITHUB_ADMIN_USERS=octocat
```

On a non-loopback bind, authenticated dashboard exposure also requires a stable
base64 session key decoding to at least 32 bytes
(`LLMCONDUIT_DASHBOARD_SESSION_KEY`) and an exact HTTPS
`LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN`. The callback URL is derived from that
origin and must be `https://<origin>/dashboard/auth/github/callback` with no
credentials, query, or fragment; `LLMCONDUIT_GITHUB_CALLBACK_URL` can override it
only when it keeps the same public origin and callback path.
The legacy `LLMCONDUIT_DASHBOARD_TOKEN` remains a non-browser break-glass API
credential, but is no longer exposed by the dashboard login UI. These env-only
secrets must not be placed in `control_plane.auth` or persisted config. To deliberately run tokenless over
plaintext on a trusted development network, set
`LLMCONDUIT_ALLOW_INSECURE_DASHBOARD=1`; startup logs a prominent warning because
the debug surfaces are then fully unauthenticated.

GitHub SSO requires SQL storage (`control_plane.storage` with SQLite or
Postgres). Successful GitHub logins are provisioned as dashboard users so
existing per-user API keys and model grants apply to both dashboard chat and key
management.

The production helper deliberately refuses to install on an unacknowledged
machine. Validate it first with
`scripts/install-github-sso-production.sh --confirm-production-host "$(hostname)" --dry-run`,
then omit `--dry-run` on the intended production host (see "Small production
hosts" below for installing a prebuilt binary). The generated unit reads
ordinary gateway settings from `LLMCONDUIT_CONFIG_PATH` and env-only OAuth
credentials from the mode-`0600` `LLMCONDUIT_ENV_FILE`.

Dashboard mutations are disabled unless
`LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS=1`; the kill endpoint additionally enforces
the authenticated session and double-submit CSRF token. Raw upstream response
capture is independently opt-in with
`LLMCONDUIT_DASHBOARD_CAPTURE_UPSTREAM_RESPONSE=1`. A non-secret caller-id header
may be selected with `LLMCONDUIT_DASHBOARD_CLIENT_HEADER` for dashboard
attribution; values from credential-bearing header names are retained only as a
one-way short hash, never verbatim.

For small hosts, tune retained dashboard data with
`LLMCONDUIT_MONITOR_PAYLOAD_PREVIEW_BYTES` (default 128 MiB) and
`LLMCONDUIT_DASHBOARD_SNAPSHOT_BYTES` (default 400 MiB). Both take positive byte
counts. Exceeding the preview budget omits older bodies while keeping flow
summaries; exceeding the snapshot budget evicts older time-travel cuts. Completed
WebSocket replays release their temporary transcript copies before serving live
updates.

Replay history is bounded by both `max_replay_entries` and a conservative
64 MiB retained-memory budget. Oversized records are skipped; setting
`max_replay_entries: 0` disables replay retention.

Large inference uploads share a 128 MiB in-flight body budget, configurable
with `LLMCONDUIT_INFLIGHT_REQUEST_BODY_BYTES`. This budget queues uploads before
buffering and stays charged until the response stream finishes or disconnects.
It is separate from the per-request `max_request_body_bytes` limit, so a
100 MiB request remains supported. Compressed uploads reserve their maximum
decoded size. Smaller requests consume proportionally less of the budget.
Dashboard chat uploads use the same budget.

On small glibc hosts, large temporary image strings can leave allocator arenas
resident after requests finish. Set these service environment variables to keep
large allocations outside those arenas and limit arena count:

```ini
Environment=MALLOC_ARENA_MAX=2
Environment=MALLOC_MMAP_THRESHOLD_=131072
Environment=MALLOC_TRIM_THRESHOLD_=131072
```

These are [glibc allocator settings](https://sourceware.org/glibc/manual/latest/html_node/Memory-Allocation-Tunables.html).
The fixed mmap threshold disables glibc's automatic threshold increases; the
arena cap trades allocation concurrency for lower memory retention. The
`llmconduit` binary itself allocates through mimalloc, so these only affect
glibc-internal allocations and are harmless to keep.

Large request bodies are parsed on Tokio's blocking pool, which is capped at
16 threads (`LLMCONDUIT_MAX_BLOCKING_THREADS` overrides).

#### Small production hosts (2 GB / 2 vCPU)

The production helper writes these defaults into the systemd unit, with
`MemoryHigh=1200M` / `MemoryMax=1536M` guardrails (`LLMCONDUIT_MEMORY_HIGH` /
`LLMCONDUIT_MEMORY_MAX` at install time):

```ini
Environment=MALLOC_ARENA_MAX=2
Environment=LLMCONDUIT_DASHBOARD_SNAPSHOT_BYTES=33554432
Environment=LLMCONDUIT_MONITOR_PAYLOAD_PREVIEW_BYTES=16777216
Environment=LLMCONDUIT_INFLIGHT_REQUEST_BODY_BYTES=67108864
Environment=LLMCONDUIT_PERSISTENCE_QUEUE_MAX_BYTES=16777216
Environment=LLMCONDUIT_PROVIDER_METRICS_INTERVAL_SECS=120
```

Override any of them in `~/.config/llmconduit/tuning.env` (created commented-out
on first install, never overwritten). Do not compile the release binary on such
a host — thin LTO with one codegen unit can run it out of memory. Build on a
larger machine with the same architecture and an equal-or-older glibc, then
install the result:

```bash
LLMCONDUIT_BUILD_DASHBOARD=1 cargo build --locked --release   # build host
scripts/install-github-sso-production.sh --confirm-production-host "$(hostname)" \
  --binary /path/to/llmconduit          # or --binary-url URL --binary-sha256 HEX
```

Without `--binary`, the helper compiles locally with
`CARGO_PROFILE_RELEASE_LTO=false`, 16 codegen units and one build job. That
still peaks around 2.1 GB, so it refuses on hosts with under 3 GiB of RAM+swap
unless `--build-on-host` is passed (for example after adding swap). Also
consider `control_plane.storage.keep_media: false` and a shorter `retention_days`.

Streaming responses are Server-Sent Events. A reverse proxy in front of the
gateway must not buffer or compress them, or clients see tokens in bursts and
the proxy holds whole responses in memory. Streaming responses already carry
`X-Accel-Buffering: no`, which nginx honours; still, on the nginx location
that proxies llmconduit:

```nginx
proxy_http_version 1.1;
proxy_buffering off;
proxy_cache off;
gzip off;
proxy_read_timeout 1h;
```

Caddy streams `text/event-stream` responses without buffering by default; do not
add an `encode` directive that covers `/v1/*`. Dashboard WebSockets (`/dashboard/ws`,
`/debug/ws`) also need the usual `Upgrade`/`Connection` forwarding.

The Providers view can also control a local [lil-fleet](https://github.com/local-inference-lab/lil-fleet)
instance. Set `LLMCONDUIT_FLEET_URL` to its loopback origin and provide either
`LLMCONDUIT_FLEET_TOKEN_FILE` (preferred, so the bearer token can remain in a
mode-`0600` file) or `LLMCONDUIT_FLEET_TOKEN`. This integration intentionally
rejects non-loopback URLs. A mesh worker may advertise the same capability to
its controller over authenticated Iroh, without exposing Fleet itself.
Inventory reads require a dashboard administrator, while load/unload
also require the global mutation gate and CSRF token. Fleet credentials remain
server-side and are never returned to the browser.

## Endpoints

| Endpoint | Description |
|-|-|
| `POST /v1/responses` | OpenAI Responses API |
| `POST /v1/chat/completions` | OpenAI Chat Completions API |
| `POST /v1/messages` | Anthropic Messages API |
| `POST /v1/messages/count_tokens` | Anthropic token counting |
| `POST /v1/completions` | Legacy completions passthrough |
| `GET /v1/models` | Proxied model list |
| `GET /health` | Health check (`{"status":"healthy"}`) |
| `GET /` | Process check (`{"status":"ok"}`); browsers are redirected to `/dashboard` when it is registered |
| `GET /openapi.json` | OpenAPI 3.1 document describing every route above and below, generated from the handler annotations at compile time (`src/openapi.rs`); `tests/openapi.rs` fails the build when a registered route is missing from it or a described route is not served |
| `GET /debug` | Debug UI when started with `--with-debug-ui` |
| `GET /dashboard` | Embedded Argus dashboard when started with `--with-debug-ui` and its env-only auth gate permits registration |
| `GET /dashboard/api/*` | Authenticated Argus flow, metrics, topology, catalog, and snapshot APIs |
| `GET /dashboard/api/history/requests` | Authenticated durable request summaries (SQL storage) |
| `GET /dashboard/api/history/requests/:id` | Authenticated durable request and event detail (SQL storage) |
| `GET /dashboard/api/history/requests/:id/body` | Authenticated full request body of one hop (`client_in`/`upstream_out`), reassembled from the content store |
| `GET /dashboard/api/history/sessions[/:id]` | Authenticated session tree (harness sessions, sub-sessions, chain divergence, cache busts) |
| `GET /dashboard/api/history/throughput` | Authenticated per-model/backend throughput buckets (tokens, TTFT, decode) |
| `GET /dashboard/api/history/activity` | Authenticated per-user/key request, failure and token buckets |
| `GET /dashboard/api/history/usage` | Authenticated durable usage rollups (SQL storage) |
| `GET /dashboard/api/history/metrics` | Authenticated durable minute-level provider health/counter history (SQL storage) |
| `POST /dashboard/api/flows/:id/kill` | Abort a live flow when dashboard mutations are enabled; requires a session and CSRF token |
| `GET /dashboard/api/fleet` | Local Fleet model/deployment inventory when configured; requires administrator access or `fleet.models.read` |
| `POST /dashboard/api/fleet/models/:id/load` | Mutation- and CSRF-gated local Fleet load control; requires administrator access or `fleet.models.load` |
| `POST /dashboard/api/fleet/models/:id/unload` | Mutation- and CSRF-gated local Fleet unload control; requires administrator access or `fleet.models.unload` |
| `GET /dashboard/api/mesh` | Mesh state, including remote Fleet model load state; requires administrator access or `fleet.models.read` |
| `POST /dashboard/api/mesh/nodes/:endpoint_id/models/:model_id/switch`, `/load` | Load an exactly advertised model on a connected Fleet-capable mesh worker; requires administrator access or `fleet.models.load` |
| `POST /dashboard/api/mesh/nodes/:endpoint_id/models/:model_id/unload` | Unload an exactly advertised model on a connected Fleet-capable mesh worker; requires administrator access or `fleet.models.unload` |
| `GET /dashboard/api/me`, `/users`, `/keys` (+ `POST`/`PATCH`/`DELETE`) | Authenticated accounts API: the current user, user administration, API keys |
| `GET /dashboard/auth/github/start`, `GET /dashboard/auth/github/callback`, `POST /dashboard/logout` | GitHub SSO dashboard session (signed state + PKCE; session and CSRF cookies) |
| `GET /dashboard/ws`, `GET /debug/ws` | Authenticated WebSocket feeds behind the dashboard and debug UI |

## Environment

Common overrides:

```text
LLMCONDUIT_BIND_ADDR
LLMCONDUIT_UPSTREAM_BASE_URL
LLMCONDUIT_UPSTREAM_API_KEY
LLMCONDUIT_UPSTREAM_MODEL
LLMCONDUIT_SYSTEM_PROMPT_PREFIX
LLMCONDUIT_UPSTREAM_CHAT_KWARGS_JSON
LLMCONDUIT_UPSTREAM_FAILURE_COOLDOWN_SECS
LLMCONDUIT_BRAVE_MAX_RESULTS
LLMCONDUIT_REQUEST_TIMEOUT_SECS
LLMCONDUIT_CONNECT_TIMEOUT_SECS
LLMCONDUIT_MAX_WEB_SEARCH_ROUNDS
LLMCONDUIT_MAX_REPLAY_ENTRIES
LLMCONDUIT_FLATTEN_CONTENT
LLMCONDUIT_TURN_CAPTURE_DIR
LLMCONDUIT_STORAGE_BACKEND
LLMCONDUIT_DATABASE_URL
LLMCONDUIT_STORAGE_JSONL_DIR
LLMCONDUIT_PERSISTENCE_QUEUE_CAPACITY
LLMCONDUIT_PERSISTENCE_QUEUE_MAX_BYTES
LLMCONDUIT_PERSISTENCE_RETENTION_DAYS
LLMCONDUIT_REQUIRE_AUTH
LLMCONDUIT_CONVERSATION_ID_HEADER
LLMCONDUIT_PROVIDER_METRICS_INTERVAL_SECS
LLMCONDUIT_DASHBOARD_TOKEN
LLMCONDUIT_DASHBOARD_SESSION_KEY
LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN
LLMCONDUIT_ALLOW_INSECURE_DASHBOARD
LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS
LLMCONDUIT_GITHUB_CLIENT_ID
LLMCONDUIT_GITHUB_CLIENT_SECRET
LLMCONDUIT_GITHUB_CALLBACK_URL
LLMCONDUIT_GITHUB_ALLOWED_USERS
LLMCONDUIT_GITHUB_ADMIN_USERS
LLMCONDUIT_DASHBOARD_CAPTURE_UPSTREAM_RESPONSE
LLMCONDUIT_DASHBOARD_CLIENT_HEADER
LLMCONDUIT_MONITOR_PAYLOAD_PREVIEW_BYTES
LLMCONDUIT_DASHBOARD_SNAPSHOT_BYTES
LLMCONDUIT_FLEET_URL
LLMCONDUIT_FLEET_TOKEN_FILE
LLMCONDUIT_FLEET_TOKEN
BRAVE_SEARCH_API_KEY
OPENAI_API_KEY
OPENROUTER_API_KEY
```

`OPENAI_API_KEY` is used as a fallback upstream API key.

Provider cache metrics are optional, dashboard-only aggregate observability.
Targets are accepted only from operator configuration, never from dashboard
request parameters, and each URL must point exactly at an HTTP(S) `/metrics`
path. They may be attached to the primary or any configured/fallback upstream:

```yaml
upstreams:
  - name: local-vllm
    upstream_base_url: http://127.0.0.1:8000/v1
    metrics_url: http://127.0.0.1:8000/metrics
    metrics_source: vllm
```

```bash
export LLMCONDUIT_PROVIDER_METRICS_INTERVAL_SECS=120   # default; 5..3600
```

Scrapes run only with `--with-debug-ui`; failures retain the last good sample
and never affect inference. These provider-level counters are not attributed to
individual API keys or requests.

## Request Logs

Set this in config to write upstream chat requests as JSONL:

```yaml
upstream_request_log_path: "/tmp/llmconduit-upstream.jsonl"
```

Then inspect prefix stability:

```bash
llmconduit analyze-log
```

## Durable turn capture

Set `turn_capture_dir` to persist ONE self-contained JSON artifact per inference
turn — the full request+response chain, for debugging output that returned a plain
`200 OK` (e.g. a stray `<think>` tag that leaked into text, a dropped tool call).
It is opt-in and works independently of the `--with-debug-ui` dashboard:

```yaml
turn_capture_dir: "/tmp/llmconduit-turns"
# Optional: age-rotate artifacts (and sweep crash-orphaned work dirs) after N hours.
debug_log_max_age_hours: 48
```

Each instrumented turn writes `<turn_capture_dir>/<api_call_id>.json` with four
sections — `inbound_request`, `upstream_request` (translated, on-wire),
`upstream_response` (raw upstream bytes — the pre-parse ground truth), and
`served_response` (the exact bytes returned to the client) — plus outcome metadata
(`status`, `terminal_reason`, timings, per-section `{bytes, partial, encoding}`).
Diff `upstream_response` against `served_response` to localize a `<think>` leak as
upstream-emitted vs converter-introduced. Request sections are redacted (secret keys
+ image URIs); memory stays bounded (sections stream to per-turn temp files under
`<dir>/.work/<id>/`, assembled atomically via tmp→fsync→rename). Leave
`turn_capture_dir` unset to disable (zero overhead — no thread, no allocation).

## Test

```bash
cargo test
```

## License

MIT
