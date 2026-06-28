// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerRoute is a live routing-table entry: an app peer and the node it is
// reachable through.
type PeerRoute struct {
	Peer peer.ID
	Via  peer.ID
}

// Snapshot returns all live (non-expired) routing entries, for the API's
// /peers endpoint.
func (r *Router) Snapshot() []PeerRoute {
	now := time.Now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]PeerRoute, 0, len(r.table))
	for id, e := range r.table {
		if now.Before(e.ExpiresAt) {
			out = append(out, PeerRoute{Peer: id, Via: e.ViaNode})
		}
	}
	return out
}
