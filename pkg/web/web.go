// Package web holds tiny HTTP helpers shared by the api/ functions.
package web

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"os"
	"strings"
)

func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func Err(w http.ResponseWriter, status int, msg string) {
	JSON(w, status, map[string]string{"error": msg})
}

// RequireAdmin protects Floodgate's control-plane handlers. It fails closed
// when the deployment has not configured an administrator token.
func RequireAdmin(w http.ResponseWriter, r *http.Request) bool {
	expected := os.Getenv("FLOODGATE_ADMIN_TOKEN")
	if expected == "" {
		Err(w, http.StatusServiceUnavailable, "administrative authentication is not configured")
		return false
	}
	supplied := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	a := sha256.Sum256([]byte(supplied))
	b := sha256.Sum256([]byte(expected))
	if supplied == "" || subtle.ConstantTimeCompare(a[:], b[:]) != 1 {
		Err(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	return true
}
