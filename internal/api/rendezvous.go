// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Rendezvous lets two app clients that share a short code find each other. The
// app publishes its signed identity under namespace = hash(code); the other app
// looks it up by the same namespace. The node never sees the code, never
// inspects the record, and stores nothing on disk — entries live in memory and
// expire. This is purely a discovery aid; all authentication and encryption
// happen end-to-end in the app.
//
// Scope note: register and lookup must hit the same node. App clients on the
// default public node resolve fine; cross-node propagation is a future addition.

const (
	rendezvousMaxEntries   = 20000     // cap to bound memory
	rendezvousMaxRecordLen = 8 * 1024  // opaque record byte cap
	rendezvousMaxTTLSecs   = 3600       // 1h ceiling (app uses 30m)
)

type rendezvousEntry struct {
	record    string
	expiresAt time.Time
}

type rendezvousStore struct {
	mu      sync.Mutex
	entries map[string]rendezvousEntry
}

func newRendezvousStore() *rendezvousStore {
	rs := &rendezvousStore{entries: make(map[string]rendezvousEntry)}
	go rs.janitor()
	return rs
}

// janitor periodically purges expired entries.
func (rs *rendezvousStore) janitor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		rs.mu.Lock()
		for k, e := range rs.entries {
			if now.After(e.expiresAt) {
				delete(rs.entries, k)
			}
		}
		rs.mu.Unlock()
	}
}

func (rs *rendezvousStore) put(namespace, record string, ttl time.Duration) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, exists := rs.entries[namespace]; !exists && len(rs.entries) >= rendezvousMaxEntries {
		return false
	}
	rs.entries[namespace] = rendezvousEntry{record: record, expiresAt: time.Now().Add(ttl)}
	return true
}

func (rs *rendezvousStore) get(namespace string) (string, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	e, ok := rs.entries[namespace]
	if !ok {
		return "", false
	}
	if time.Now().After(e.expiresAt) {
		delete(rs.entries, namespace)
		return "", false
	}
	return e.record, true
}

func (s *Server) handleRendezvousRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Namespace string `json:"namespace"`
		Record    string `json:"record"`
		TTLSecs   int    `json:"ttl_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.Namespace == "" || req.Record == "" {
		writeError(w, http.StatusBadRequest, "namespace and record required")
		return
	}
	if len(req.Record) > rendezvousMaxRecordLen {
		writeError(w, http.StatusBadRequest, "record too large")
		return
	}
	ttl := req.TTLSecs
	if ttl <= 0 || ttl > rendezvousMaxTTLSecs {
		ttl = rendezvousMaxTTLSecs
	}
	if !s.rv.put(req.Namespace, req.Record, time.Duration(ttl)*time.Second) {
		writeError(w, http.StatusServiceUnavailable, "rendezvous full")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleRendezvousLookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		writeError(w, http.StatusBadRequest, "namespace required")
		return
	}
	rec, ok := s.rv.get(ns)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"record": rec})
}
