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

All routing goes through named upstreams plus model profiles. The minimal
config is one upstream and one profile that passes every requested model
through unchanged:

```yaml
upstreams:
  - name: default
    url: "http://127.0.0.1:8000/v1"
model_profiles:
  "*": { upstream: default }    # pass any requested model through unchanged
```

### Upstreams

`upstreams:` is a list of named endpoints. `name` and `url` are required;
`name` must be unique, case-insensitive. Everything else is optional:

```yaml
upstreams:
  - name: local
    url: "http://127.0.0.1:8000/v1"
  - name: openrouter
    url: "https://openrouter.ai/api/v1"
    api_key: "sk-or-..."
    chat_kwargs:
      provider:
        order: [z-ai]
    request_log_path: "/tmp/openrouter-upstream.jsonl"
```

Each entry also accepts `upstream_base_url`/`upstream_api_key`/
`upstream_chat_kwargs`/`upstream_request_log_path` as aliases for
`url`/`api_key`/`chat_kwargs`/`request_log_path`. An entry that omits
`api_key` sends no credentials; every entry that needs auth declares its own,
either inline or as `api_key_env: SOME_VAR`, which reads the key from that
environment variable at startup. Declaring `api_key_env` for a variable that
is unset or blank is a startup error, and one entry may not set both
`api_key` and `api_key_env`.

An upstream is a pure endpoint: it names no model and carries no fallback
chain of its own. Routing, model overrides, and failover all live on the
model profile that points at it.

### Model profiles

`model_profiles:` is an ordered map from a served model id (or a glob using
`*`, `?`, or `[...]`, matched case-insensitively) to a profile. Every profile
requires `upstream:`, naming the `upstreams:` entry it routes to. Assuming
`openrouter`, `backup`, and `vision` are also declared under `upstreams:`:

```yaml
model_profiles:
  GLM-5.2:
    upstream: local
    upstream_model: "z-ai/glm-5.2"        # backend id to POST; defaults to the profile key
    fallbacks:
      - openrouter                         # bare upstream name
      - { upstream: backup, model: "z-ai/glm-5.2-backup" }  # upstream + model override
    image_analysis:
      model: Qwen3-VL                      # must name another exact-key profile
      residual_images: reject              # placeholder (default) or reject

  Qwen3-VL:
    upstream: vision

  "claude-*":
    upstream: openrouter                   # glob: passes the request model through
```

A request resolves to exactly one profile. An exact key (trimmed,
case-insensitive) always wins over a glob, regardless of declaration order -
an exact key is never shadowed. Among globs, the first declared match wins,
so declare a narrower glob before a broader one like `"*"`, or the broader
pattern shadows it. Duplicate keys collapse to the last one declared. `upstream_model` is the id
actually POSTed to the backend: it defaults to the profile key for an exact
key, or to the (trimmed) request model for a glob. A request for a model with
no matching profile is a 404; a blank/missing model is a 400 unless a glob
matches the empty string and also sets `upstream_model`.

`fallbacks:` lists upstreams to retry, in order, if the primary fails before
its first response chunk (never mid-stream). Each entry is either a bare
upstream name or `{upstream, model}` to also remap the model for that hop.
Cooldown after a failure is tracked per upstream name and shared across every
profile that references it.

`image_analysis` opts a profile into stripping images out and routing them to
another profile for description; see "Image analysis" below. A profile with
no `image_analysis` passes images to its upstream untouched.

`extends:` shares fields across profiles via `model_profile_templates`,
including `upstream` and `fallbacks`:

```yaml
model_profile_templates:
  routed:
    upstream: local
    fallbacks: [openrouter]

model_profiles:
  Qwen3.5:
    extends: [routed]
    upstream_model: "Qwen/Qwen3.5"
```

When a profile `extends` multiple templates, later entries in the list
override earlier ones, and the profile's own fields override every template.

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
    upstream: local
    extends:
      - thinking
    system_prompt_prefix: |
      Extra Kimi-specific instructions.
    chat_template_kwargs:
      preserve_thinking: true

  GLM-5.2:
    upstream: local
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

### The `*` catch-all profile

`*` is not special syntax; it is a glob pattern like any other, and it
happens to match every model. It requires `upstream:` like any profile, and
it follows the same declaration-order rule as every other glob: declare it
last among your glob keys, or it shadows the more specific globs beneath it.
An exact key always wins over `*` regardless of where `*` is declared. `*`
can itself `extend` a template to share defaults with explicit profiles.

```yaml
model_profiles:
  GLM-5.2:
    upstream: local
    upstream_chat_kwargs:
      chat_template_kwargs:
        enable_thinking: true

  # Matches any model without a more specific profile. Declared last so more
  # specific keys and globs are tried first.
  "*":
    upstream: local
    upstream_chat_kwargs:
      chat_template_kwargs:
        enable_thinking: false
```

With this config, a request for `GLM-5.2` uses only the `GLM-5.2` profile
(`enable_thinking: true`) - `*` is a separate profile, not a source of
inherited defaults, so an explicit match never picks up its fields. A request
for any other model (e.g. `Qwen-3`) falls through to `*`
(`enable_thinking: false`).

### Model capabilities

A profile's `capabilities` block overrides the Anthropic model capabilities
advertised on `/v1/models` for Anthropic clients.

```yaml
model_profiles:
  GLM-5.2:
    upstream: local
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
    upstream: local
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
    upstream: local
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
    upstream: local
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
    upstream: local
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

### Image analysis

Images pass through to a profile's upstream untouched by default. Setting
`image_analysis` on a profile opts it into stripping images out of the latest
turn, sending them to another profile for description, and feeding that
description back into the conversation instead:

```yaml
model_profiles:
  GLM-5.2:
    upstream: local
    image_analysis:
      model: Qwen3-VL          # must be an existing exact-key profile
      residual_images: reject  # default: placeholder

  Qwen3-VL:
    upstream: vision
```

`image_analysis.model` must name another EXACT-key profile (not a glob) that
itself has no `image_analysis` set - redirect chains are rejected at startup,
as is a profile redirecting to itself. `residual_images` decides what happens
to an image the redirect could not strip - a `file_id` image, one outside the
latest user turn, or an image left over in older history: `placeholder` (the
default) replaces it with an instructive text note so the model asks for a
description instead of guessing; `reject` fails the turn before dispatch with
an HTTP 400 instead of contacting the upstream.

## Migrating

Every removed key below now maps to a named `upstreams:` entry, a
`model_profiles:` field, or both. Shown together for illustration - a real
config normally uses only the pieces it needs:

Before:

```yaml
bind_addr: "127.0.0.1:4000"
upstream_base_url: "http://127.0.0.1:8000/v1"
upstream_api_key: "sk-local"
upstream_model: "Qwen3.5"

model_routes:
  "claude-opus-*": "https://openrouter.ai/api/v1"

image_agent_enabled: true
vision_url: "http://127.0.0.1:8001/v1"
vision_model: "Qwen3-VL"

upstreams:
  - name: "local"
    upstream_base_url: "http://127.0.0.1:8000/v1"
    fallback_upstreams:
      - name: "backup"
        upstream_base_url: "https://openrouter.ai/api/v1"
        upstream_api_key: "sk-or-..."
        upstream_model: "openai/gpt-4.1-mini"
        exposed_model: "GPT-4.1-mini"
```

After:

```yaml
bind_addr: "127.0.0.1:4000"

upstreams:
  - name: local
    url: "http://127.0.0.1:8000/v1"
    api_key: "sk-local"
  - name: openrouter
    url: "https://openrouter.ai/api/v1"
    api_key: "sk-or-..."
  - name: vision
    url: "http://127.0.0.1:8001/v1"

model_profiles:
  "claude-opus-*":
    upstream: openrouter

  GPT-4.1-mini:
    upstream: openrouter
    upstream_model: "openai/gpt-4.1-mini"

  Qwen3-VL:
    upstream: vision

  "*":
    upstream: local
    upstream_model: "Qwen3.5"
    fallbacks:
      - { upstream: openrouter, model: "openai/gpt-4.1-mini" }
    image_analysis:
      model: Qwen3-VL
```

The top-level `upstream_base_url`/`upstream_api_key`/`upstream_model` become
an `upstreams:` entry plus that entry named on a profile's `upstream:` (and
`upstream_model:` when the served id differs from the profile key). A
`model_routes` glob becomes a glob `model_profiles` key pointing at a named
upstream; the removed `--model-route` CLI flag has no replacement flag for
the same reason - express it in the config instead. `image_agent_enabled` /
`vision_url` / `vision_model` become `image_analysis` on whichever profiles
need it, redirecting to a profile for the vision-capable backend.
`unsupported_image_policy` becomes `image_analysis.residual_images` on that
same profile. A nested `fallback_upstreams` entry becomes a real `upstreams:`
entry plus a `fallbacks:` reference on the profile; an `exposed_model` alias
becomes its own exact-key profile pointing at that upstream, since `/v1/models`
now only lists exact profile keys, not fallback aliases.

**Environment overrides.** `LLMCONDUIT_UPSTREAM_BASE_URL`,
`LLMCONDUIT_UPSTREAM_API_KEY`, and `LLMCONDUIT_UPSTREAM_MODEL` are no longer
read; set those values on an `upstreams:` entry and a profile instead.
`OPENAI_API_KEY` is no longer read implicitly: an ambient key must not leak
to endpoints that never asked for one, so every `upstreams:` entry that needs
auth declares its own `api_key`. To keep sourcing the key from the
environment, opt in per entry with `api_key_env: OPENAI_API_KEY` (or any
other variable name).

**`configure` and existing multi-upstream configs.** `llmconduit configure`
always writes the minimal shape: one `default` upstream plus a `"*"` profile.
Running it against a config that already has several `upstreams:` entries or
several `model_profiles` collapses them down to that single pair (other
settings, like `model_profile_templates` and `price_table`, are preserved).
Back up a hand-edited config before running `configure` again.

**Behavioral changes.** A request for a model with no matching profile now
returns 404 instead of silently falling back to the first catalog entry seen.
A blank/missing model returns 400 unless a glob profile matches the empty
string and also sets `upstream_model`. `/v1/models` lists the exact
(non-glob) profile keys in declaration order instead of the union of upstream
catalogs. An image reaches the upstream untouched unless the matched profile
sets `image_analysis` - there is no more implicit backend-capability
sniffing or global `unsupported_image_policy`.

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
stage; Node and the frontend sources are not present in the final image.

```bash
docker build -t llmconduit .
docker run --rm -p 4000:4000 \
  --add-host=host.docker.internal:host-gateway \
  -v "$(pwd)/config.yaml:/home/nonroot/.config/llmconduit/config.yaml:ro" \
  llmconduit
```

`config.yaml` is the same `upstreams:`/`model_profiles:` file described above
(point `url` at `http://host.docker.internal:8000/v1` to reach a backend
running on the host).

To expose `/debug` and `/dashboard`, replace the final line with
`llmconduit start --with-debug-ui`.

Non-loopback dashboard access requires authentication by default. To deliberately
run tokenless on a trusted network, set `LLMCONDUIT_ALLOW_INSECURE_DASHBOARD=1`;
startup logs a prominent warning because `/debug` and `/dashboard` will be open.

## Endpoints

| Endpoint | Description |
|-|-|
| `POST /v1/responses` | OpenAI Responses API |
| `POST /v1/chat/completions` | OpenAI Chat Completions API |
| `POST /v1/messages` | Anthropic Messages API |
| `GET /v1/models` | Model list, built from `model_profiles` |
| `GET /healthz` | Health check |
| `GET /debug` | Debug UI when started with `--with-debug-ui` |

## Environment

Common overrides:

```text
LLMCONDUIT_BIND_ADDR
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
BRAVE_SEARCH_API_KEY
```

`LLMCONDUIT_UPSTREAM_BASE_URL`/`LLMCONDUIT_UPSTREAM_API_KEY`/
`LLMCONDUIT_UPSTREAM_MODEL`/`OPENAI_API_KEY` are gone; set the URL, key, and
model on an `upstreams:` entry and a profile instead (see "Migrating"). An
entry can still source its key from any environment variable by naming it in
`api_key_env`.

## Request Logs

Set `request_log_path` on an `upstreams:` entry to write that upstream's
chat requests as JSONL:

```yaml
upstreams:
  - name: local
    url: "http://127.0.0.1:8000/v1"
    request_log_path: "/tmp/llmconduit-upstream.jsonl"
```

Then inspect prefix stability:

```bash
llmconduit analyze-log --path /tmp/llmconduit-upstream.jsonl
```

`analyze-log` also accepts a top-level `upstream_request_log_path:` as the
default `--path` when the flag is omitted, but that top-level key has no
effect on what gets logged while serving requests; only each `upstreams:`
entry's own `request_log_path` does that.

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
