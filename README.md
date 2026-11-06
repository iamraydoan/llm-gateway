# llm-gateway

A minimal, fast, OpenAI-compatible LLM gateway in Go. One endpoint in front
of a pool of API keys, each with its own proxy, concurrency cap and
multi-window rate limits.

## What it does

- **Key pool, priority order** — keys are tried by ascending `priority`.
  A key is skipped when it is disabled, blacklisted, out of quota, over any
  rate limit, or at max concurrency. The first usable key wins.
- **Per-key proxy** — each key can have its own HTTP proxy
  (`http://user:pass@host:port`); empty means direct.
- **Per-key concurrency** — `max_concurrency` caps in-flight requests per
  key (`0` = unlimited). A busy key is skipped, the next key takes the call.
- **Multi-window rate limits** — sliding windows per key, e.g. `5 req / 1m`
  **and** `100 req / 1h` at the same time.
- **Blacklist with backoff** — on HTTP 429 the key is blacklisted with
  exponential backoff (5s, 10s, 20s … capped at 10m). A `Retry-After`
  header overrides the backoff when it is longer.
- **Quota exhaustion + reset time** — error bodies mentioning
  `insufficient_quota` / `quota_exceeded` (and OpenAI's
  `x-ratelimit-reset-requests` / `x-ratelimit-reset-tokens` headers) mark
  the key out until its reset time. No calls are sent to it before then.
- **Failover** — on 429 / any 4xx / 5xx / network error the key goes into
  cooldown backoff and the request jumps to the next key immediately
  (no delay-and-retry). `server.max_failovers` (default 10) caps how many
  keys one request may jump across. Every upstream attempt is logged as
  its own line, so a request that fails on key 1 and succeeds on key 2
  leaves two log lines.
- **Model mapping** — `model_map` rewrites requested model names to
  upstream names (`cheap` → `gpt-4o-mini`).
- **Request log + dashboard** — every upstream attempt is logged to
  SQLite (`gateway.db` next to the config) with key, models, status,
  latency and token usage (including streaming usage when the upstream
  sends it). Token usage is split into input (`prompt_tokens`), cached
  (`prompt_tokens_details.cached_tokens`) and output (`completion_tokens`),
  stored per request and summed in the dashboard.
  The dashboard at `http://localhost:8080/` has four pages, all **realtime**
  over SSE (`GET /api/events`): new requests appear instantly, counters
  tick up live, and key status badges change the moment a key is
  blacklisted or recovers (sidebar shows ● LIVE; falls back to 5s
  polling if the stream drops, plus a 30s full-stats safety refetch):
  - **Dashboard** — request/error cards plus token cards split into
    Input / Cached / Output tokens, token usage per key (by key name,
    with a Cached column), recent requests. Date filter with presets
    (1h / 24h / 7d / 30d) or a custom From–To range; card labels follow
    the selected range.
  - **Keys** — key pool with names, live status, rate-limit bars
    (read-only; keys come from `config.yaml` — edit the file and
    restart to change them). The **Last error** column shows the last
    upstream response body attached to each key, with consecutive
    failure count and retry countdown from the exponential backoff
    (delay = max of backoff, Retry-After, quota reset wait); it clears
    when the key succeeds again. Requests / Errors, rate-limit usage
    and last error are rebuilt from the SQLite request log, so they
    survive a gateway restart (the live cooldown / quota throttle
    itself is in-memory and resets on restart).
  - **Logs** — request log with date-range filter, key / model / status
    filters, errors-only toggle, and pagination (page size 25/50/100).
    Each row shows Prompt / Cached / Completion / Total tokens.
    New matching rows prepend live on page 1.
  - **Model Map** — model mappings (read-only, from `config.yaml`).

Keys and model mappings live in `config.yaml` only. The SQLite database
stores request logs only. API keys are never shown in full in the UI (masked).

## Endpoints (OpenAI-compatible)

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/chat/completions` | streaming + non-streaming |
| POST | `/v1/embeddings` | |
| POST | `/v1/completions` | |
| GET | `/v1/models` | lists mapped model names |
| GET | `/` | dashboard |
| GET | `/api/keys` | list keys with live status (read-only) |
| GET | `/api/model-map` | list mappings (read-only) |
| GET | `/api/logs?from=&to=&key_id=&model=&status=&errors=&limit=&offset=` | filtered, paginated logs + total |
| GET | `/api/events` | SSE stream: `log` / `keys` / `stats` realtime events |
| GET | `/api/stats?hours=24` or `?from=&to=` | aggregate counters + `tokens_by_key` for the range |
| GET | `/api/config` | upstream + model map |
| GET | `/healthz` | |

Point any OpenAI-compatible client at the gateway, e.g.
`base_url = "http://localhost:8080/v1"`. If `server.master_key` is set,
clients must send it as the Bearer token; upstream keys are never exposed
to clients.

## Quick start

```bash
# Go 1.26+ required
cp config.example.yaml config.yaml   # fill in your keys
go build -o llm-gateway .
./llm-gateway -config config.yaml
# dashboard: http://localhost:8080/
```

### Docker Compose

```bash
cp config.example.yaml config.yaml  # fill in your keys first
docker compose up -d --build
```

- The container runs the gateway on port 8080 with a healthcheck on
  `/healthz`.
- `./config.yaml` is mounted read-only; the SQLite database (keys, model
  map, request logs) lives in the `gateway-data` volume, so keys and logs
  added from the dashboard survive container recreates and rebuilds.
- `TZ=Asia/Ho_Chi_Minh` keeps log timestamps in local time.
- Stop: `docker compose down` (the volume is kept; `docker compose down -v`
  deletes the database too).

The image is a multi-stage build (`golang:1.26-alpine` → `alpine:3.21`,
pure-Go SQLite, no CGO). The database path can be overridden with
`server.db_path` in the config or the `LLM_GATEWAY_DB` environment
variable (the compose file uses the env var to point at the volume).

## Config

```yaml
server:
  listen: ":8080"
  master_key: ""          # optional client auth, empty = none
  max_failovers: 10       # max key jumps per request, default 10

upstream:
  base_url: "https://api.openai.com"
  timeout_seconds: 120

model_map:
  cheap: gpt-4o-mini
  smart: gpt-4o

keys:
  - id: key-1
    name: "Main account"   # display name in the dashboard
    api_key: "sk-..."
    proxy: ""             # per-key proxy, empty = direct
    priority: 1           # lower = tried first
    max_concurrency: 5    # 0 = unlimited
    enabled: true
    rate_limits:
      - max: 5
        window: 1m        # 30s, 1m, 1h, 1d, 7d ...
      - max: 100
        window: 1h
```

## Tests

```bash
go test ./...          # 32 tests: pool selection, rate limits, backoff, gateway
                       # passthrough (failover, quota, auth, streaming) and a
                       # 50-request integration test that audits the request log
                       # for concurrency and rate-limit compliance
```

`e2e/` holds a mock upstream + test config for an end-to-end smoke test:

```bash
go build -o /tmp/mock_upstream ./e2e/mock_upstream.go
/tmp/mock_upstream &                 # :18081
./llm-gateway -config e2e/test-config.yaml   # :18080
curl -X POST localhost:18080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"cheap","messages":[{"role":"user","content":"hi"}]}'
```

The mock makes key `key-429` always 429 (failover test), `key-quota`
quota-exhausted with a 5s reset, and serves streaming + usage normally.

## Layout

```
main.go                    entry: config -> store -> pool -> server
internal/config/           YAML config + validation (keys and model map live here)
internal/pool/             key pool: priority, semaphore, sliding windows,
                           cooldown/backoff, quota reset, per-key client+proxy,
                           live Sync() for dashboard edits
internal/gateway/          reverse proxy (streaming), dashboard API + UI
                           (Dashboard / Keys / Logs / Model Map pages),
                           SSE hub for realtime pushes (events.go)
internal/store/            SQLite: keys + model_map tables, request log,
                           log filters/pagination, tokens-by-key stats
e2e/                       mock upstream + test config
```
