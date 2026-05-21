package handler

import (
	"net/http"
	"strconv"
	"time"

	"floodgate/pkg/ratelimit"
	"floodgate/pkg/store"
	"floodgate/pkg/web"
)

// Handler is the rate-limited gateway endpoint. A client presents an
// API key; Floodgate runs that key's algorithm and returns the
// decision plus standard X-RateLimit-* headers (429 when limited).
func Handler(w http.ResponseWriter, r *http.Request) {
	secret := r.Header.Get("X-API-Key")
	if secret == "" {
		secret = r.URL.Query().Get("key") // convenience for the browser demo
	}
	if secret == "" {
		web.Err(w, http.StatusUnauthorized, "missing X-API-Key")
		return
	}

	ctx := r.Context()
	conn, err := store.Connect(ctx)
	if err != nil {
		web.Err(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer conn.Close(ctx)

	key, err := store.KeyBySecret(ctx, conn, secret)
	if err != nil {
		web.Err(w, http.StatusInternalServerError, err.Error())
		return
	}
	if key == nil {
		web.Err(w, http.StatusUnauthorized, "invalid API key")
		return
	}

	res, err := ratelimit.Check(ctx, conn, *key)
	if err != nil {
		web.Err(w, http.StatusInternalServerError, err.Error())
		return
	}
	store.RecordUsage(ctx, conn, key.ID, res.Allowed)

	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(res.ResetAt.Unix(), 10))

	if !res.Allowed {
		w.Header().Set("Retry-After", strconv.Itoa(int(res.RetryAfter/time.Second)+1))
		web.JSON(w, http.StatusTooManyRequests, map[string]any{
			"allowed":        false,
			"algorithm":      res.Algorithm,
			"remaining":      res.Remaining,
			"retry_after_ms": res.RetryAfter.Milliseconds(),
		})
		return
	}

	web.JSON(w, http.StatusOK, map[string]any{
		"allowed":   true,
		"algorithm": res.Algorithm,
		"limit":     res.Limit,
		"remaining": res.Remaining,
	})
}
