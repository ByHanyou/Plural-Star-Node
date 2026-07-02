// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Rendezvous lets two app clients that share a short code find each other. The
// app publishes its signed identity under namespace = hash(code); the other app
// looks it up by the same namespace. The node never sees the code and never
// inspects the record. This is purely a discovery aid; all authentication and
// encryption happen end-to-end in the app.
//
// Entries are snapshotted to a small JSON file next to the config (namespace ->
// opaque record + expiry) so a node restart doesn't invalidate every active
// friend/sync code mid-pairing — that produced user-facing "code wasn't found"
// for codes that were still well within their 30-minute window. Expired entries
// are never loaded. The records were already public-by-design (they exist to be
// looked up), so persistence adds no new exposure.
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

// persistedEntry is the on-disk form of one rendezvous entry.
type persistedEntry struct {
	Record    string `json:"record"`
	ExpiresAt int64  `json:"expires_at"` // unix seconds
}

type rendezvousStore struct {
	mu      sync.Mutex
	entries map[string]rendezvousEntry
	path    string // snapshot file; "" = memory-only (no persistence)
}

// rendezvousPathFor derives the snapshot path from the node's config path so
// the store lives alongside the rest of the node's state.
func rendezvousPathFor(configPath string) string {
	if configPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(configPath), "rendezvous.json")
}

func newRendezvousStore(path string) *rendezvousStore {
	rs := &rendezvousStore{entries: make(map[string]rendezvousEntry), path: path}
	rs.load()
	go rs.janitor()
	return rs
}

// load restores unexpired entries from a previous run's snapshot.
func (rs *rendezvousStore) load() {
	if rs.path == "" {
		return
	}
	b, err := os.ReadFile(rs.path)
	if err != nil {
		return // first run or unreadable snapshot — start empty
	}
	var m map[string]persistedEntry
	if json.Unmarshal(b, &m) != nil {
		return
	}
	now := time.Now()
	rs.mu.Lock()
	for ns, e := range m {
		exp := time.Unix(e.ExpiresAt, 0)
		if e.Record != "" && exp.After(now) {
			rs.entries[ns] = rendezvousEntry{record: e.Record, expiresAt: exp}
		}
	}
	rs.mu.Unlock()
}

// saveLocked snapshots the store to disk (atomic tmp+rename). Caller holds mu.
// Best-effort: a failed write only costs restart durability, never a request.
func (rs *rendezvousStore) saveLocked() {
	if rs.path == "" {
		return
	}
	m := make(map[string]persistedEntry, len(rs.entries))
	for ns, e := range rs.entries {
		m[ns] = persistedEntry{Record: e.record, ExpiresAt: e.expiresAt.Unix()}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return
	}
	tmp := rs.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) != nil {
		return
	}
	_ = os.Rename(tmp, rs.path)
}

// janitor periodically purges expired entries.
func (rs *rendezvousStore) janitor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		rs.mu.Lock()
		removed := 0
		for k, e := range rs.entries {
			if now.After(e.expiresAt) {
				delete(rs.entries, k)
				removed++
			}
		}
		if removed > 0 {
			rs.saveLocked()
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
	rs.saveLocked()
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
