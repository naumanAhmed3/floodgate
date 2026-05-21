package handler

import (
	"net/http"

	"floodgate/pkg/store"
	"floodgate/pkg/web"
)

// Handler bootstraps the demo: ensures the schema, clears existing
// keys, and creates one key per algorithm so the dashboard has
// something to compare immediately.
func Handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	conn, err := store.Connect(ctx)
	if err != nil {
		web.Err(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer conn.Close(ctx)

	if err := store.EnsureSchema(ctx, conn); err != nil {
		web.Err(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Reset to a clean demo set.
	if _, err := conn.Exec(ctx, `delete from keys`); err != nil {
		web.Err(w, http.StatusInternalServerError, err.Error())
		return
	}

	type spec struct {
		name      string
		algo      string
		limit     int
		windowSec int
		burst     int
	}
	specs := []spec{
		{"Token bucket — 20/10s", "token_bucket", 20, 10, 20},
		{"Sliding window — 15/10s", "sliding_window", 15, 10, 15},
		{"GCRA — 15/10s, burst 8", "gcra", 15, 10, 8},
	}

	keys := make([]store.Key, 0, len(specs))
	for _, s := range specs {
		k, err := store.CreateKey(ctx, conn, s.name, s.algo, s.limit, s.windowSec, s.burst)
		if err != nil {
			web.Err(w, http.StatusInternalServerError, err.Error())
			return
		}
		keys = append(keys, k)
	}
	web.JSON(w, http.StatusOK, map[string]any{"seeded": len(keys), "keys": keys})
}
