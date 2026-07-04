// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"encoding/json"
	"log"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// rendezvousMsg is the JSON gossiped on the rendezvous topic when an app
// registers (or a node refreshes) a pairing record. Records are opaque,
// self-certifying blobs — signed by the app and verified client-side — so
// nodes can propagate them without trusting each other. Expiry is TTL-only;
// there are no tombstones.
type rendezvousMsg struct {
	Namespace string `json:"namespace"`
	Record    string `json:"record"`
	ExpiresAt int64  `json:"expires_at"` // unix milliseconds
}

// RecordEvent is invoked for every valid rendezvous record received from a
// remote node, so the API layer can merge it into the local store.
type RecordEvent func(namespace, record string, ttl time.Duration)

const (
	rendezvousMaxNamespaceLen = 128
	rendezvousMaxRecordLen    = 8 * 1024
	rendezvousMaxTTL          = time.Hour
)

// RendezvousGossip publishes and consumes pairing records on a GossipSub
// topic, mirroring Presence: same scoped-topic scheme, same TTL'd upsert
// model, so every node in the network serves the same pairing directory.
type RendezvousGossip struct {
	ctx      context.Context
	self     peer.ID
	topic    *pubsub.Topic
	sub      *pubsub.Subscription
	onRecord RecordEvent
}

// NewRendezvousGossip joins topicName and starts consuming records. onRecord
// may be nil (a node that only publishes).
func NewRendezvousGossip(ctx context.Context, ps *pubsub.PubSub, self peer.ID, topicName string, onRecord RecordEvent) (*RendezvousGossip, error) {
	topic, err := ps.Join(topicName)
	if err != nil {
		return nil, err
	}
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, err
	}
	g := &RendezvousGossip{
		ctx:      ctx,
		self:     self,
		topic:    topic,
		sub:      sub,
		onRecord: onRecord,
	}
	go g.readLoop()
	return g, nil
}

// Announce publishes (or refreshes) a locally registered record to the network.
func (g *RendezvousGossip) Announce(namespace, record string, expiresAt time.Time) error {
	m := rendezvousMsg{
		Namespace: namespace,
		Record:    record,
		ExpiresAt: expiresAt.UnixMilli(),
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return g.topic.Publish(g.ctx, b)
}

func (g *RendezvousGossip) readLoop() {
	for {
		msg, err := g.sub.Next(g.ctx)
		if err != nil {
			return // ctx cancelled or subscription closed
		}
		// Skip our own announcements; our local store is authoritative for them.
		if msg.ReceivedFrom == g.self {
			continue
		}
		var m rendezvousMsg
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			continue
		}
		if m.Namespace == "" || len(m.Namespace) > rendezvousMaxNamespaceLen {
			continue
		}
		if m.Record == "" || len(m.Record) > rendezvousMaxRecordLen {
			continue
		}
		ttl := time.Until(time.UnixMilli(m.ExpiresAt))
		if ttl <= 0 {
			continue // already expired
		}
		if ttl > rendezvousMaxTTL {
			ttl = rendezvousMaxTTL
		}
		if g.onRecord != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("rendezvous: onRecord callback panicked: %v", r)
					}
				}()
				g.onRecord(m.Namespace, m.Record, ttl)
			}()
		}
	}
}
