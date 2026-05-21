// Package ratelimit implements three distributed rate-limiting
// algorithms over Postgres. Each runs inside a transaction guarded by
// a per-key advisory lock, so the check-and-update is atomic even with
// many concurrent gateway instances.
package ratelimit

import (
	"context"
	"fmt"
	"math"
	"time"

	"floodgate/pkg/store"

	"github.com/jackc/pgx/v5"
)

type Result struct {
	Allowed    bool          `json:"allowed"`
	Limit      int           `json:"limit"`
	Remaining  int           `json:"remaining"`
	RetryAfter time.Duration `json:"-"`
	ResetAt    time.Time     `json:"reset_at"`
	Algorithm  string        `json:"algorithm"`
}

// Check runs the key's algorithm atomically and returns the decision.
func Check(ctx context.Context, conn *pgx.Conn, k store.Key) (Result, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback(ctx)

	// Serialize concurrent checks for this key — makes check+update atomic.
	if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtext($1))`, k.ID); err != nil {
		return Result{}, err
	}

	var res Result
	switch k.Algorithm {
	case "token_bucket":
		res, err = tokenBucket(ctx, tx, k)
	case "sliding_window":
		res, err = slidingWindow(ctx, tx, k)
	case "gcra":
		res, err = gcra(ctx, tx, k)
	default:
		return Result{}, fmt.Errorf("unknown algorithm %q", k.Algorithm)
	}
	if err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	res.Algorithm = k.Algorithm
	return res, nil
}

// ── Token bucket ─────────────────────────────────────────────
// Tokens refill continuously at limit/window per second up to a
// `burst` ceiling; each request costs one token.
func tokenBucket(ctx context.Context, tx pgx.Tx, k store.Key) (Result, error) {
	// A fresh bucket starts full, so a client can burst immediately.
	if _, err := tx.Exec(ctx,
		`insert into rl_state (key_id, tokens, last_refill) values ($1, $2, now())
		 on conflict do nothing`, k.ID, float64(k.Burst)); err != nil {
		return Result{}, err
	}
	var tokens float64
	var lastRefill time.Time
	if err := tx.QueryRow(ctx,
		`select tokens, last_refill from rl_state where key_id=$1`, k.ID,
	).Scan(&tokens, &lastRefill); err != nil {
		return Result{}, err
	}

	now := time.Now()
	ratePerSec := float64(k.Limit) / float64(k.WindowSec)
	capacity := float64(k.Burst)
	tokens = math.Min(capacity, tokens+now.Sub(lastRefill).Seconds()*ratePerSec)

	allowed := tokens >= 1
	if allowed {
		tokens--
	}
	if _, err := tx.Exec(ctx,
		`update rl_state set tokens=$2, last_refill=$3 where key_id=$1`,
		k.ID, tokens, now); err != nil {
		return Result{}, err
	}

	var retry time.Duration
	if !allowed {
		retry = time.Duration((1 - tokens) / ratePerSec * float64(time.Second))
	}
	return Result{
		Allowed:    allowed,
		Limit:      k.Burst,
		Remaining:  int(math.Floor(tokens)),
		RetryAfter: retry,
		ResetAt:    now.Add(retry),
	}, nil
}

// ── Sliding window log ───────────────────────────────────────
// Keeps a timestamp per accepted request; a request is allowed while
// fewer than `limit` timestamps fall inside the trailing window.
func slidingWindow(ctx context.Context, tx pgx.Tx, k store.Key) (Result, error) {
	now := time.Now()
	window := time.Duration(k.WindowSec) * time.Second

	if _, err := tx.Exec(ctx,
		`delete from rl_log where key_id=$1 and at < $2`, k.ID, now.Add(-window)); err != nil {
		return Result{}, err
	}
	var count int
	if err := tx.QueryRow(ctx,
		`select count(*) from rl_log where key_id=$1`, k.ID).Scan(&count); err != nil {
		return Result{}, err
	}

	allowed := count < k.Limit
	if allowed {
		if _, err := tx.Exec(ctx,
			`insert into rl_log (key_id, at) values ($1, $2)`, k.ID, now); err != nil {
			return Result{}, err
		}
	}
	remaining := k.Limit - count
	if allowed {
		remaining--
	}
	if remaining < 0 {
		remaining = 0
	}

	var retry time.Duration
	if !allowed {
		var oldest time.Time
		if err := tx.QueryRow(ctx,
			`select min(at) from rl_log where key_id=$1`, k.ID).Scan(&oldest); err == nil {
			retry = oldest.Add(window).Sub(now)
			if retry < 0 {
				retry = 0
			}
		}
	}
	return Result{
		Allowed:    allowed,
		Limit:      k.Limit,
		Remaining:  remaining,
		RetryAfter: retry,
		ResetAt:    now.Add(retry),
	}, nil
}

// ── GCRA ─────────────────────────────────────────────────────
// The Generic Cell Rate Algorithm — a leaky-bucket meter held as a
// single timestamp (the "theoretical arrival time"). Elegant and the
// approach Cloudflare uses for edge rate limiting.
func gcra(ctx context.Context, tx pgx.Tx, k store.Key) (Result, error) {
	if _, err := tx.Exec(ctx,
		`insert into rl_state (key_id) values ($1) on conflict do nothing`, k.ID); err != nil {
		return Result{}, err
	}
	var tat time.Time
	if err := tx.QueryRow(ctx,
		`select tat from rl_state where key_id=$1`, k.ID).Scan(&tat); err != nil {
		return Result{}, err
	}

	now := time.Now()
	emission := time.Duration(float64(k.WindowSec) / float64(k.Limit) * float64(time.Second))
	tolerance := time.Duration(k.Burst-1) * emission // burst allowance

	if tat.Before(now) {
		tat = now
	}
	// Conforming iff the TAT is within `tolerance` of now.
	allowed := tat.Sub(now) <= tolerance
	if allowed {
		newTat := tat.Add(emission)
		if _, err := tx.Exec(ctx,
			`update rl_state set tat=$2 where key_id=$1`, k.ID, newTat); err != nil {
			return Result{}, err
		}
		tat = newTat
	}

	var retry time.Duration
	if !allowed {
		retry = tat.Sub(now) - tolerance
		if retry < 0 {
			retry = 0
		}
	}
	remaining := int((tolerance - max0(tat.Sub(now)-emission)) / emission)
	if remaining < 0 {
		remaining = 0
	}
	return Result{
		Allowed:    allowed,
		Limit:      k.Burst,
		Remaining:  remaining,
		RetryAfter: retry,
		ResetAt:    now.Add(retry),
	}, nil
}

func max0(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}
