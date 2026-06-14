# llmconduit

LLM API gateway for local and OpenAI-compatible chat-completions backends.

It accepts OpenAI Responses, OpenAI Chat Completions, and Anthropic Messages
requests, normalizes them, and forwards them to an upstream
`/v1/chat/completions` server. It can also run server-side tools such as Brave
Search.

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
  GLM-5.1:
    extends:
      - thinking
    chat_template_kwargs:
      clear_thinking: false

  Kimi-K2.6:
    extends:
      - thinking
    system_prompt_prefix: |
      Extra Kimi-specific instructions.
    chat_template_kwargs:
      preserve_thinking: true
```

`system_prompt_prefix` is prepended to all Responses, Chat Completions, and
Anthropic Messages requests. A profile-specific prefix is appended after the
global prefix. `upstream_chat_kwargs` merge in this order: top-level defaults,
matched model profile templates, matched model profile, then explicit request
values. In model profiles and templates, extra profile-level keys are shorthand
for upstream chat kwargs; the explicit `upstream_chat_kwargs` wrapper still
works and overrides the shorthand when both set the same key.

Optional Brave Search:

```yaml
brave_api_key: "..."
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

```bash
docker build -t llmconduit .
docker run --rm -p 4000:4000 \
  --add-host=host.docker.internal:host-gateway \
  -e LLMCONDUIT_UPSTREAM_BASE_URL=http://host.docker.internal:8000/v1 \
  llmconduit
```

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

## Context Compaction (optional, off by default)

llmconduit can keep a long conversation under a model's effective context window
by compacting older turns before forwarding upstream. This is useful for closed
harnesses (which can't compact themselves) and for enforcing a local model's real
window. It is **off by default** and **best-effort**: if compaction is disabled,
under budget, or fails for any reason, the original request is forwarded
unchanged — a user request never fails because of compaction.

Compaction is delegated to an **external** HTTP compactor, which keeps the
gateway a single static binary and lets the compaction strategy (plain summary,
structured state ledger, etc.) live in any language. Enable it in config:

```yaml
compaction:
  enabled: true
  mode: "external"                 # only "external" today; "builtin" reserved
  endpoint: "http://127.0.0.1:4100" # your compactor service
  max_input_tokens: 96000          # compact when the estimated request exceeds this
  keep_recent_turns: 6             # most-recent turns kept verbatim
  timeout_ms: 60000
```

When the lowered chat request is estimated (≈4 chars/token) to exceed
`max_input_tokens`, llmconduit POSTs it to `{endpoint}/compact` and forwards the
compacted messages it returns. The contract:

```jsonc
// POST {endpoint}/compact
// request
{
  "session_id": "sess-…",          // stable per conversation (system + first user)
  "messages": [ /* OpenAI chat messages */ ],
  "max_input_tokens": 96000,
  "keep_recent_turns": 6,
  "model": "your-model"
}
// response
{
  "compacted": true,               // false => gateway forwards the original messages
  "messages": [ /* replacement chat messages */ ]
}
```

A correct compactor must keep tool-call/tool-result pairs intact (never start the
forwarded tail with an orphan `tool` message). A reference implementation
(event-sourced state-ledger compactor, tuned for coding agents and documents) is
available at
[llmconduit-compactor](https://github.com/brandonmmusic-max/llmconduit-compactor).

## Test

```bash
cargo test
```

## License

MIT
