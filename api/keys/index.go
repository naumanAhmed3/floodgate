package handler

import (
	"encoding/json"
	"net/http"

	"floodgate/pkg/store"
	"floodgate/pkg/web"
)

var validAlgorithms = map[string]bool{
	"token_bucket":   true,
	"sliding_window": true,
	"gcra":           true,
}

// Handler manages API keys: GET list, POST create, DELETE ?id=.
func Handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	conn, err := store.Connect(ctx)
	if err != nil {
		web.Err(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer conn.Close(ctx)

	switch r.Method {
	case http.MethodGet:
		keys, err := store.ListKeys(ctx, conn)
		if err != nil {
			web.Err(w, http.StatusInternalServerError, err.Error())
			return
		}
		if keys == nil {
			keys = []store.Key{}
		}
		web.JSON(w, http.StatusOK, keys)

	case http.MethodPost:
		var body struct {
			Name      string `json:"name"`
			Algorithm string `json:"algorithm"`
			Limit     int    `json:"rate_limit"`
			WindowSec int    `json:"window_sec"`
			Burst     int    `json:"burst"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			web.Err(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if body.Name == "" || !validAlgorithms[body.Algorithm] {
			web.Err(w, http.StatusBadRequest, "name and a valid algorithm are required")
			return
		}
		if body.Limit < 1 || body.WindowSec < 1 {
			web.Err(w, http.StatusBadRequest, "rate_limit and window_sec must be >= 1")
			return
		}
		if body.Burst < 1 {
			body.Burst = body.Limit
		}
		key, err := store.CreateKey(ctx, conn, body.Name, body.Algorithm,
			body.Limit, body.WindowSec, body.Burst)
		if err != nil {
			web.Err(w, http.StatusInternalServerError, err.Error())
			return
		}
		web.JSON(w, http.StatusCreated, key)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			web.Err(w, http.StatusBadRequest, "missing id")
			return
		}
		if err := store.DeleteKey(ctx, conn, id); err != nil {
			web.Err(w, http.StatusInternalServerError, err.Error())
			return
		}
		web.JSON(w, http.StatusOK, map[string]string{"deleted": id})

	default:
		web.Err(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
