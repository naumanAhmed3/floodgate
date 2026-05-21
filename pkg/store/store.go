// Package store is Floodgate's Postgres layer: API keys, rate-limit
// state, and usage counters.
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

const Schema = `
create table if not exists keys (
  id          text primary key,
  name        text not null,
  secret      text not null unique,
  algorithm   text not null,            -- token_bucket | sliding_window | gcra
  rate_limit  int  not null,            -- requests allowed per window
  window_sec  int  not null,            -- window length, seconds
  burst       int  not null,            -- burst capacity
  created_at  timestamptz not null default now()
);

-- One row per key holding token-bucket + GCRA state.
create table if not exists rl_state (
  key_id      text primary key references keys(id) on delete cascade,
  tokens      double precision not null default 0,
  last_refill timestamptz not null default now(),
  tat         timestamptz not null default now()  -- GCRA theoretical arrival time
);

-- Sliding-window-log: one row per accepted request.
create table if not exists rl_log (
  id     bigserial primary key,
  key_id text not null references keys(id) on delete cascade,
  at     timestamptz not null default now()
);
create index if not exists rl_log_idx on rl_log (key_id, at);

-- Per-minute usage counters, for the dashboard.
create table if not exists usage (
  key_id  text not null references keys(id) on delete cascade,
  minute  timestamptz not null,
  allowed int not null default 0,
  denied  int not null default 0,
  primary key (key_id, minute)
);
`

type Key struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Secret    string    `json:"secret"`
	Algorithm string    `json:"algorithm"`
	Limit     int       `json:"rate_limit"`
	WindowSec int       `json:"window_sec"`
	Burst     int       `json:"burst"`
	CreatedAt time.Time `json:"created_at"`
}

type UsageTotal struct {
	KeyID   string `json:"key_id"`
	Allowed int    `json:"allowed"`
	Denied  int    `json:"denied"`
}

// Connect opens a single connection from DATABASE_URL.
func Connect(ctx context.Context) (*pgx.Conn, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, errors.New("DATABASE_URL is not set")
	}
	return pgx.Connect(ctx, url)
}

func EnsureSchema(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, Schema)
	return err
}

func randID(prefix string, n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

func CreateKey(ctx context.Context, conn *pgx.Conn, name, algo string, limit, windowSec, burst int) (Key, error) {
	k := Key{
		ID:        randID("key_", 8),
		Name:      name,
		Secret:    randID("fg_", 16),
		Algorithm: algo,
		Limit:     limit,
		WindowSec: windowSec,
		Burst:     burst,
		CreatedAt: time.Now(),
	}
	_, err := conn.Exec(ctx,
		`insert into keys (id, name, secret, algorithm, rate_limit, window_sec, burst)
		 values ($1,$2,$3,$4,$5,$6,$7)`,
		k.ID, k.Name, k.Secret, k.Algorithm, k.Limit, k.WindowSec, k.Burst)
	return k, err
}

func scanKeys(rows pgx.Rows) ([]Key, error) {
	defer rows.Close()
	var out []Key
	for rows.Next() {
		var k Key
		if err := rows.Scan(&k.ID, &k.Name, &k.Secret, &k.Algorithm,
			&k.Limit, &k.WindowSec, &k.Burst, &k.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

const keyCols = `id, name, secret, algorithm, rate_limit, window_sec, burst, created_at`

func ListKeys(ctx context.Context, conn *pgx.Conn) ([]Key, error) {
	rows, err := conn.Query(ctx, `select `+keyCols+` from keys order by created_at desc`)
	if err != nil {
		return nil, err
	}
	return scanKeys(rows)
}

func KeyBySecret(ctx context.Context, conn *pgx.Conn, secret string) (*Key, error) {
	rows, err := conn.Query(ctx, `select `+keyCols+` from keys where secret=$1`, secret)
	if err != nil {
		return nil, err
	}
	keys, err := scanKeys(rows)
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	return &keys[0], nil
}

func DeleteKey(ctx context.Context, conn *pgx.Conn, id string) error {
	_, err := conn.Exec(ctx, `delete from keys where id=$1`, id)
	return err
}

// RecordUsage bumps the current-minute counter for a key.
func RecordUsage(ctx context.Context, conn *pgx.Conn, keyID string, allowed bool) {
	a, d := 0, 1
	if allowed {
		a, d = 1, 0
	}
	_, _ = conn.Exec(ctx,
		`insert into usage (key_id, minute, allowed, denied)
		 values ($1, date_trunc('minute', now()), $2, $3)
		 on conflict (key_id, minute) do update
		 set allowed = usage.allowed + excluded.allowed,
		     denied  = usage.denied  + excluded.denied`,
		keyID, a, d)
}

// UsageTotals sums the last hour of usage per key.
func UsageTotals(ctx context.Context, conn *pgx.Conn) ([]UsageTotal, error) {
	rows, err := conn.Query(ctx,
		`select key_id, coalesce(sum(allowed),0)::int, coalesce(sum(denied),0)::int
		 from usage where minute > now() - interval '1 hour'
		 group by key_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UsageTotal
	for rows.Next() {
		var u UsageTotal
		if err := rows.Scan(&u.KeyID, &u.Allowed, &u.Denied); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
