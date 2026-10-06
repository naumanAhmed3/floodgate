package ratelimit

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"floodgate/pkg/store"

	"github.com/jackc/pgx/v5"
)

// openTestDB connects to an existing test database and creates the tables each
// test needs. TEMP tables are private to this connection and disappear when it
// closes. Restricting search_path to pg_temp keeps queries off application tables.
func openTestDB(t *testing.T, tracer pgx.QueryTracer) (context.Context, *pgx.Conn) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Fatal("TEST_DATABASE_URL is required for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	config, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.Tracer = tracer
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	_, err = conn.Exec(ctx, `
		CREATE TEMP TABLE rl_state (
			key_id text PRIMARY KEY,
			tokens double precision NOT NULL DEFAULT 0,
			last_refill timestamptz NOT NULL DEFAULT now(),
			tat timestamptz NOT NULL DEFAULT now()
		);
		CREATE TEMP TABLE rl_log (
			key_id text NOT NULL,
			at timestamptz NOT NULL DEFAULT now()
		);
		SET search_path TO pg_temp
	`)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, conn
}

// beginTestTx gives the real algorithm a transaction and rolls back its changes
// during cleanup, even if the test fails.
func beginTestTx(t *testing.T, ctx context.Context, conn *pgx.Conn) pgx.Tx {
	t.Helper()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback(context.Background()) })
	return tx
}

// TestTokenBucketUsesProvidedTime checks that the real tokenBucket function in
// ratelimit.go uses its now parameter instead of reading the machine's clock.
// In the application, Check obtains now from PostgreSQL. Here, we supply a fixed
// time so the expected result is predictable; we do not change either clock.
//
// Each run creates a temporary rl_state table in the test database and inserts
// an empty bucket. Its last_refill (the last refill calculation time) equals the
// supplied now, so zero seconds have passed and no tokens can be added. The
// request must be denied because the bucket is empty. The transaction rollback
// removes the temporary table and its data after the test.
//
// Both timestamps are in 2000 deliberately: if tokenBucket accidentally uses
// time.Now(), it sees years of elapsed time, refills the bucket, and allows the
// request, causing this test to fail. This tests how the algorithm uses time;
// obtaining time from PostgreSQL is covered separately.
// Removing the now parameter would cause a compile error; keeping the parameter
// but ignoring or overwriting it can still compile, which is why we test behavior.
func TestTokenBucketUsesProvidedTime(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Fatal("TEST_DATABASE_URL is required for PostgreSQL tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())

	// Temporary tables belong only to this database connection.
	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE rl_state (
			key_id text PRIMARY KEY,
			tokens double precision NOT NULL,
			last_refill timestamptz NOT NULL
		) ON COMMIT DROP
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Restrict table lookups to this connection's temporary tables.
	if _, err := tx.Exec(ctx, `SET LOCAL search_path TO pg_temp`); err != nil {
		t.Fatal(err)
	}

	// Arrange: an empty bucket refilling at one token per second.
	now := time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)
	key := store.Key{
		ID:        "clock-test",
		Limit:     1,
		WindowSec: 1,
		Burst:     1,
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO rl_state (key_id, tokens, last_refill) VALUES ($1, $2, $3)`,
		key.ID, 0, now,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Act: check at exactly the last refill time.
	result, err := tokenBucket(ctx, tx, key, now)
	if err != nil {
		t.Fatal(err)
	}

	// Assert: no elapsed time means no new tokens.
	if result.Allowed {
		t.Fatal("request allowed despite an empty bucket and no elapsed time")
	}
	if result.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", result.Remaining)
	}
	if result.RetryAfter != time.Second {
		t.Fatalf("retry = %v, want 1s", result.RetryAfter)
	}
}

// TestSlidingWindowUsesProvidedTime checks that recent requests still count
// according to the supplied now. With a limit of one request per 60 seconds, a
// request recorded 10 seconds ago must block another request with a 50s retry.
// We call the real slidingWindow function and verify that it preserves the log
// entry. If it uses the machine's current time, the entry from 2000 looks expired:
// it gets deleted and the new request is incorrectly allowed.
func TestSlidingWindowUsesProvidedTime(t *testing.T) {
	ctx, conn := openTestDB(t, nil)
	tx := beginTestTx(t, ctx, conn)
	now := time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)
	k := store.Key{ID: "window-clock", Limit: 1, WindowSec: 60, Burst: 1}
	if _, err := tx.Exec(ctx, `INSERT INTO rl_log (key_id, at) VALUES ($1, $2)`,
		k.ID, now.Add(-10*time.Second)); err != nil {
		t.Fatal(err)
	}
	res, err := slidingWindow(ctx, tx, k, now)
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed || res.Remaining != 0 || res.RetryAfter != 50*time.Second {
		t.Fatalf("got %+v; want denied, 0 remaining, 50s retry", res)
	}
	var at time.Time
	if err := tx.QueryRow(ctx, `SELECT at FROM rl_log WHERE key_id=$1`, k.ID).Scan(&at); err != nil {
		t.Fatal(err)
	}
	if !at.Equal(now.Add(-10 * time.Second)) {
		t.Fatal("recent request was removed or replaced")
	}
}

// TestGCRAUsesProvidedTime checks that a request arriving before its scheduled
// time is denied. With burst=1, tat is the earliest allowed time for the next
// request. Setting it five seconds after the supplied now must produce a 5s
// retry and leave tat unchanged: a denied request must not extend the wait.
// Using the machine's current time would make the schedule from 2000 look past
// and incorrectly allow the request. The real gcra function makes the decision.
func TestGCRAUsesProvidedTime(t *testing.T) {
	ctx, conn := openTestDB(t, nil)
	tx := beginTestTx(t, ctx, conn)
	now := time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)
	k := store.Key{ID: "gcra-clock", Limit: 1, WindowSec: 5, Burst: 1}
	tat := now.Add(5 * time.Second)
	if _, err := tx.Exec(ctx, `INSERT INTO rl_state (key_id, tat) VALUES ($1, $2)`, k.ID, tat); err != nil {
		t.Fatal(err)
	}
	res, err := gcra(ctx, tx, k, now)
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed || res.Remaining != 0 || res.RetryAfter != 5*time.Second {
		t.Fatalf("got %+v; want denied, 0 remaining, 5s retry", res)
	}
	var stored time.Time
	if err := tx.QueryRow(ctx, `SELECT tat FROM rl_state WHERE key_id=$1`, k.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Equal(tat) {
		t.Fatal("denied request changed the next allowed time")
	}
}

// TestFreshStateUsesProvidedTime leaves the state table empty so the real
// algorithm must initialize it, unlike tests that insert existing state first.
// Correct calculations are not enough if newly created timestamps use a
// different time: SQL now() during initialization could disagree with the
// supplied now and cause even a fresh key's first request to be denied.
//
// At one request per second with burst=1, the first request must be allowed.
// Token bucket starts with one token, spends it, and saves last_refill=now.
// GCRA starts at now and advances tat to now+1s. We inspect the saved state as
// well as the decision, since incorrect state could break the next request.
// This tests initialization, not locking; the algorithms are called directly.
func TestFreshStateUsesProvidedTime(t *testing.T) {
	for _, algo := range []string{"token_bucket", "gcra"} {
		t.Run(algo, func(t *testing.T) {
			ctx, conn := openTestDB(t, nil)
			tx := beginTestTx(t, ctx, conn)
			now := time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)
			k := store.Key{ID: "fresh-clock", Limit: 1, WindowSec: 1, Burst: 1}
			var res Result
			var err error
			if algo == "token_bucket" {
				res, err = tokenBucket(ctx, tx, k, now)
			} else {
				res, err = gcra(ctx, tx, k, now)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !res.Allowed {
				t.Fatal("fresh state denied its first request")
			}
			var tokens float64
			var lastRefill, tat time.Time
			if err := tx.QueryRow(ctx, `SELECT tokens, last_refill, tat FROM rl_state WHERE key_id=$1`,
				k.ID).Scan(&tokens, &lastRefill, &tat); err != nil {
				t.Fatal(err)
			}
			if algo == "token_bucket" && (tokens != 0 || !lastRefill.Equal(now)) {
				t.Fatalf("tokens=%v last_refill=%v; want 0 and %v", tokens, lastRefill, now)
			}
			if algo == "gcra" && !tat.Equal(now.Add(time.Second)) {
				t.Fatalf("tat=%v; want %v", tat, now.Add(time.Second))
			}
		})
	}
}

// clockQueryTracer records real SQL calls without replacing the algorithms or
// database. When failClock is set, it returns a canceled child context only for
// the clock query. The original context remains usable for transaction rollback.
type clockQueryTracer struct {
	queries   []string
	failClock bool
}

func (tr *clockQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	sql := strings.ToLower(strings.TrimSpace(data.SQL))
	tr.queries = append(tr.queries, sql)
	if tr.failClock && sql == "select clock_timestamp()" {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		return ctx
	}
	return ctx
}

func (*clockQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// TestCheckReadsClockAfterLock exercises Check with real PostgreSQL time for
// each algorithm. Its tracer verifies one clock_timestamp() query after the
// advisory lock: reading time before a lock wait could give a stale timestamp.
// The lock query inside Check is what can wait; the test does not deliberately
// create contention or change either machine's clock.
//
// The test-only before/after readings bound the expected decision timestamp.
// Check itself still reads the clock just once. We also verify that the saved
// state agrees with that decision's time. The fixed-time algorithm tests above
// separately detect accidental use of the machine's clock.
func TestCheckReadsClockAfterLock(t *testing.T) {
	for _, algo := range []string{"token_bucket", "sliding_window", "gcra"} {
		t.Run(algo, func(t *testing.T) {
			tr := &clockQueryTracer{}
			ctx, conn := openTestDB(t, tr)
			var before, after time.Time
			// Establish a lower time bound; this query does not acquire a lock.
			if err := conn.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			// Count only Check's queries, excluding setup and the before reading.
			tr.queries = nil
			k := store.Key{ID: "check-clock", Algorithm: algo, Limit: 1, WindowSec: 1, Burst: 1}
			res, err := Check(ctx, conn, k)
			if err != nil {
				t.Fatal(err)
			}
			// Snapshot Check's queries before the test adds its after reading.
			queries := append([]string(nil), tr.queries...)
			if err := conn.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if !res.Allowed || res.Algorithm != algo || res.ResetAt.Before(before) || res.ResetAt.After(after) {
				t.Fatalf("unexpected decision: %+v; database interval [%v, %v]", res, before, after)
			}
			lockIndex, clockIndex, clockCount := -1, -1, 0
			for i, sql := range queries {
				if strings.Contains(sql, "pg_advisory_xact_lock") {
					lockIndex = i
				}
				if sql == "select clock_timestamp()" {
					clockIndex = i
					clockCount++
				}
			}
			if lockIndex < 0 || clockIndex <= lockIndex || clockCount != 1 {
				t.Fatalf("want one clock read after lock; queries: %v", queries)
			}
			var stored time.Time
			sql := `SELECT last_refill FROM rl_state WHERE key_id=$1`
			want := res.ResetAt
			if algo == "sliding_window" {
				sql = `SELECT at FROM rl_log WHERE key_id=$1`
			} else if algo == "gcra" {
				sql = `SELECT tat FROM rl_state WHERE key_id=$1`
				want = want.Add(time.Second)
			}
			if err := conn.QueryRow(ctx, sql, k.ID).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			if !stored.Equal(want) {
				t.Fatalf("stored time=%v; want %v", stored, want)
			}
		})
	}
}

// TestCheckClockFailureRollsBack deliberately makes the clock query fail. Without
// a valid shared time, Check must return an error before accessing limiter state
// or committing. Its deferred rollback must run to release the transaction lock.
// We check both the observed queries and the empty state table, so ignoring the
// error and continuing with an invalid timestamp cannot silently pass this test.
func TestCheckClockFailureRollsBack(t *testing.T) {
	tr := &clockQueryTracer{failClock: true}
	ctx, conn := openTestDB(t, tr)
	tr.queries = nil
	k := store.Key{ID: "clock-failure", Algorithm: "token_bucket", Limit: 1, WindowSec: 1, Burst: 1}
	if _, err := Check(ctx, conn, k); err == nil {
		t.Fatal("expected a clock-query error")
	}
	if len(tr.queries) == 0 || tr.queries[len(tr.queries)-1] != "rollback" {
		t.Fatalf("expected rollback; queries: %v", tr.queries)
	}
	for _, sql := range tr.queries {
		if strings.Contains(sql, "rl_state") || strings.Contains(sql, "rl_log") || sql == "commit" {
			t.Fatalf("clock failure continued to state access or commit: %s", sql)
		}
	}
	var count int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM rl_state`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("state rows=%d; want 0", count)
	}
}
