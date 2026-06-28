// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	corenet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	msgio "github.com/libp2p/go-msgio"
)

// ErrNoRoute is returned when a packet's recipient is neither connected locally
// nor present in the routing table.
var ErrNoRoute = errors.New("recipient not found in routing table")

// DeliverFunc hands a packet to a locally connected app.
type DeliverFunc func(*Packet)

// Manager wires together the routing table, dedup cache, presence gossip, and
// the /plural-star/relay/1.0.0 stream handler.
type Manager struct {
	ctx      context.Context
	h        host.Host
	self     peer.ID
	router   *Router
	dedup    *DedupCache
	presence *Presence

	mu         sync.RWMutex
	localApps  map[peer.ID]DeliverFunc
	refreshers map[peer.ID]context.CancelFunc
}

// NewManager builds the relay manager, registers the stream handler, and starts
// presence gossip on "<gossipPrefix>presence". onPeer (may be nil) is invoked on
// remote peer online/offline transitions.
func NewManager(ctx context.Context, h host.Host, ps *pubsub.PubSub, gossipPrefix string, onPeer PeerEvent) (*Manager, error) {
	router := NewRouter(ctx, RoutingTablePruneTicker)
	dedup := NewDedupCache(ctx, DedupCacheTTL, DedupCacheEvictInterval)
	presence, err := NewPresence(ctx, ps, h.ID(), gossipPrefix+"presence", router, PresenceTTL, onPeer)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		ctx:        ctx,
		h:          h,
		self:       h.ID(),
		router:     router,
		dedup:      dedup,
		presence:   presence,
		localApps:  make(map[peer.ID]DeliverFunc),
		refreshers: make(map[peer.ID]context.CancelFunc),
	}
	h.SetStreamHandler(protocol.ID(RelayProtocol), m.handleStream)
	return m, nil
}

// Router exposes the routing table (read-only use by the API layer).
func (m *Manager) Router() *Router { return m.router }

// AppConnected registers a locally connected app peer, announces its presence to
// the network, and begins refreshing that presence before TTL expiry. deliver is
// called when a packet arrives for this app.
func (m *Manager) AppConnected(appPeer peer.ID, deliver DeliverFunc) {
	rctx, cancel := context.WithCancel(m.ctx)
	m.mu.Lock()
	if old, ok := m.refreshers[appPeer]; ok {
		old() // replace any prior refresher
	}
	m.localApps[appPeer] = deliver
	m.refreshers[appPeer] = cancel
	m.mu.Unlock()

	if err := m.presence.Announce(appPeer); err != nil {
		log.Printf("relay: announce presence for %s: %v", appPeer, err)
	}
	go m.refreshLoop(rctx, appPeer)
}

// AppDisconnected unregisters a local app and publishes a presence tombstone.
func (m *Manager) AppDisconnected(appPeer peer.ID) {
	m.mu.Lock()
	delete(m.localApps, appPeer)
	if cancel, ok := m.refreshers[appPeer]; ok {
		cancel()
		delete(m.refreshers, appPeer)
	}
	m.mu.Unlock()

	if err := m.presence.Tombstone(appPeer); err != nil {
		log.Printf("relay: tombstone presence for %s: %v", appPeer, err)
	}
}

func (m *Manager) refreshLoop(ctx context.Context, appPeer peer.ID) {
	ticker := time.NewTicker(PresenceRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.presence.Announce(appPeer); err != nil {
				log.Printf("relay: refresh presence for %s: %v", appPeer, err)
			}
		}
	}
}

// Route processes a packet from any source (a local app or another node):
// dedup, then deliver locally or forward toward the recipient.
func (m *Manager) Route(p *Packet) error {
	if m.dedup.SeenOrAdd(p.ID) {
		return nil // duplicate; drop silently
	}
	return m.forwardOrDeliver(p)
}

func (m *Manager) forwardOrDeliver(p *Packet) error {
	recipient, err := peer.IDFromBytes(p.RecipientID)
	if err != nil {
		return fmt.Errorf("invalid recipient id: %w", err)
	}

	m.mu.RLock()
	deliver, isLocal := m.localApps[recipient]
	m.mu.RUnlock()
	if isLocal {
		deliver(p)
		return nil
	}

	via, ok := m.router.Lookup(recipient)
	if !ok || via == m.self {
		// No live route, or a stale entry pointing back at this node for an app
		// that is no longer connected locally — don't dial ourselves.
		return ErrNoRoute
	}
	return m.forwardTo(via, p)
}

func (m *Manager) forwardTo(via peer.ID, p *Packet) error {
	b, err := p.Marshal()
	if err != nil {
		return fmt.Errorf("marshal packet: %w", err)
	}
	streamCtx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()
	s, err := m.h.NewStream(streamCtx, via, protocol.ID(RelayProtocol))
	if err != nil {
		return fmt.Errorf("open relay stream to %s: %w", via, err)
	}
	defer s.Close()
	w := msgio.NewWriter(s)
	if err := w.WriteMsg(b); err != nil {
		_ = s.Reset()
		return fmt.Errorf("write relay packet to %s: %w", via, err)
	}
	return nil
}

func (m *Manager) handleStream(s corenet.Stream) {
	defer s.Close()
	r := msgio.NewReader(s)
	b, err := r.ReadMsg()
	if err != nil {
		_ = s.Reset()
		return
	}
	p, err := UnmarshalPacket(b)
	r.ReleaseMsg(b)
	if err != nil {
		_ = s.Reset()
		return
	}
	if err := m.Route(p); err != nil && !errors.Is(err, ErrNoRoute) {
		log.Printf("relay: route packet: %v", err)
	}
}
