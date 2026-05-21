# Floodgate — API Gateway & Rate Limiter

An **API gateway** that meters traffic per API key with three real
**rate-limiting algorithms** — token bucket, sliding-window log, and
GCRA — each enforced **atomically in Postgres** so it stays correct
across any number of concurrent gateway instances. Written in **Go**.

**[▶ Live demo](https://floodgate-ten.vercel.app)** · **[Repo](https://github.com/naumanAhmed3/floodgate)**

---

## Why this exists

Rate limiting looks trivial until you make it *distributed*. An
in-memory counter is wrong the moment you run two instances — they each
let the limit through. Correct rate limiting needs a **shared store**
and an **atomic check-and-update**, and the algorithm you pick changes
the burst behaviour your users feel. Floodgate implements three, side by
side, so the differences are visible.

## The algorithms

All three live in `internal/ratelimit`, each run inside a transaction
guarded by a **per-key Postgres advisory lock** (`pg_advisory_xact_lock`)
— that lock is what makes the check-and-update atomic under concurrency.

| Algorithm | State | Behaviour |
|---|---|---|
| **Token bucket** | `tokens`, `last_refill` | Tokens refill continuously; allows bursts up to the bucket capacity, then a steady drip |
| **Sliding-window log** | one timestamp per request | Exact — counts requests in the trailing window; no boundary spikes, more storage |
| **GCRA** | a single timestamp (`tat`) | The Generic Cell Rate Algorithm — a leaky-bucket meter as *one* timestamp. Smoothest output; the approach Cloudflare uses at the edge |

## Architecture

```
 client ──X-API-Key──▶ /api/gateway ──▶ ratelimit.Check()
                                            │
                          BEGIN; advisory-lock(key); run algorithm; COMMIT
                                            │
                                   Postgres (keys, rl_state, rl_log, usage)
                                            │
        dashboard ◀── /api/keys · /api/usage ┘
```

- **Go serverless functions** on Vercel (`api/*.go`) — gateway, key
  management, usage, demo seeding.
- **Postgres** (`pgx`) is the shared store. Every limiter decision is a
  transaction; the advisory lock serialises concurrent requests *for the
  same key* without blocking other keys.
- A **vanilla-JS dashboard** with a load-test tool — fire a burst at a
  key and watch its limiter respond, request by request.

Standard `X-RateLimit-Limit / -Remaining / -Reset` headers on every
response; `Retry-After` and `429` when limited.

## Tech

- **Go** — `net/http` handlers, `pgx/v5`
- **Postgres** — advisory locks, atomic transactions
- **Vercel** — Go serverless functions + static dashboard

## API

```http
GET    /api/gateway        rate-limited endpoint  (X-API-Key header, or ?key=)
GET    /api/keys           list keys
POST   /api/keys           { name, algorithm, rate_limit, window_sec, burst }
DELETE /api/keys?id=…      remove a key
GET    /api/usage          per-key allowed/limited totals (last hour)
POST   /api/seed           reset to one demo key per algorithm
```

## Run it

```bash
# Go 1.23+
go build ./...

# Point DATABASE_URL at Postgres, then deploy to Vercel (it builds the
# api/*.go functions automatically). Hit /api/seed once to create the
# schema and demo keys.
```

## Project layout

```
api/
  gateway.go     the rate-limited endpoint
  keys.go        key CRUD
  usage.go       usage analytics
  seed.go        schema bootstrap + demo keys
pkg/
  ratelimit/     token bucket · sliding window · GCRA
  store/         Postgres layer (keys, state, usage)
  web/           JSON helpers
index.html       dashboard (load-test tool)
```
