# llmconduit

LLM API gateway for local and OpenAI-compatible chat-completions backends.

It accepts OpenAI Responses, OpenAI Chat Completions, and Anthropic Messages
requests, normalizes them, and forwards them to an upstream
`/v1/chat/completions` server. It can also run server-side tools such as Brave
Search.

Chat Completions function tools are executed by the client, including a function
named `web_search`. Hosted Brave Search uses the Responses `web_search` tool or
Anthropic's typed web-search tool.

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

Minimal config:

```yaml
bind_addr: "127.0.0.1:4000"
upstream_base_url: "http://127.0.0.1:8000/v1"
upstream_model: "Qwen3.5"
```

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

`parallel_tool_calls` follows the same precedence for client tools, Brave Search,
and image analysis. The gateway does not force it to `false` for server tools;
it executes returned server-tool batches sequentially in model output order.

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
stage; Node and the frontend sources are not present in the final image.

```bash
docker build -t llmconduit .
docker run --rm -p 4000:4000 \
  --add-host=host.docker.internal:host-gateway \
  -e LLMCONDUIT_UPSTREAM_BASE_URL=http://host.docker.internal:8000/v1 \
  llmconduit
```

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
| `GET /v1/models` | Proxied model list |
| `GET /healthz` | Health check |
| `GET /debug` | Debug UI when started with `--with-debug-ui` |

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
BRAVE_SEARCH_API_KEY
OPENAI_API_KEY
```

`OPENAI_API_KEY` is used as a fallback upstream API key.

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
