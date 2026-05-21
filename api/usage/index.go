package handler

import (
	"net/http"

	"floodgate/pkg/store"
	"floodgate/pkg/web"
)

// Handler returns per-key allowed/denied totals over the last hour.
func Handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	conn, err := store.Connect(ctx)
	if err != nil {
		web.Err(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer conn.Close(ctx)

	totals, err := store.UsageTotals(ctx, conn)
	if err != nil {
		web.Err(w, http.StatusInternalServerError, err.Error())
		return
	}
	if totals == nil {
		totals = []store.UsageTotal{}
	}
	web.JSON(w, http.StatusOK, totals)
}
