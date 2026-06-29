// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// authed wraps a handler with bearer-token authentication.
func (s *Server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.checkAuth(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

// checkAuth accepts the token via the Authorization: Bearer header, or (for
// WebSocket clients that cannot set headers) via a ?token= query parameter.
func (s *Server) checkAuth(r *http.Request) bool {
	// Open mode: an empty api_token means the relay runs without auth, so app
	// clients can connect to a public node without a token. The relay only ever
	// sees opaque, end-to-end-encrypted payloads, so this exposes no user data.
	if s.cfg.APIToken == "" {
		return true
	}
	want := []byte(s.cfg.APIToken)

	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		got := []byte(strings.TrimPrefix(h, "Bearer "))
		if subtle.ConstantTimeCompare(got, want) == 1 {
			return true
		}
	}
	if q := r.URL.Query().Get("token"); q != "" {
		if subtle.ConstantTimeCompare([]byte(q), want) == 1 {
			return true
		}
	}
	return false
}
