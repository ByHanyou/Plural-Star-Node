// SPDX-License-Identifier: AGPL-3.0-or-later

// Package relay implements the node-to-node packet relay: the wire packet,
// dedup cache, routing table, presence gossip, and the libp2p stream handler.
package relay

import (
	"crypto/rand"
	"fmt"
	"time"
)

// Protocol constants and tunables (see spec "Hardcoded Constants").
const (
	// RelayProtocol is the libp2p stream protocol for packet forwarding.
	RelayProtocol = "/plural-star/relay/1.0.0"

	PresenceTTL             = 60 * time.Second
	PresenceRefreshInterval = 45 * time.Second
	DedupCacheTTL           = 10 * time.Second
	DedupCacheEvictInterval = 5 * time.Second
	RoutingTablePruneTicker = 30 * time.Second
)

// Packet is the unit relayed between nodes. Nodes inspect only RecipientID for
// routing and ID for dedup; Payload is an opaque E2E-encrypted blob.
type Packet struct {
	ID          [16]byte `msgpack:"id"`
	SenderID    []byte   `msgpack:"sender"`
	RecipientID []byte   `msgpack:"recipient"`
	Payload     []byte   `msgpack:"payload"`
	Timestamp   int64    `msgpack:"ts"`
}

// NewPacketID returns a random 16-byte packet ID for dedup.
func NewPacketID() ([16]byte, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return id, fmt.Errorf("generate packet id: %w", err)
	}
	return id, nil
}
