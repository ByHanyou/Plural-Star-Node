// SPDX-License-Identifier: AGPL-3.0-or-later

// Package ping tracks the node's currently connected libp2p peers (other relay
// nodes) and their round-trip times, for the app-facing /nodes endpoint.
package ping

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	corenet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	pingsvc "github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

// NodeInfo describes a connected libp2p node.
type NodeInfo struct {
	PeerID    string `json:"peer_id"`
	Multiaddr string `json:"multiaddr"`
	RTTms     int64  `json:"rtt_ms"`
}

// EventFunc is invoked when a node connects (connected=true, with measured RTT)
// or disconnects (connected=false).
type EventFunc func(peerID peer.ID, rttMs int64, connected bool)

// Manager runs the libp2p ping responder, measures RTT to peers as they
// connect, and answers /nodes queries.
type Manager struct {
	ctx     context.Context
	h       host.Host
	svc     *pingsvc.PingService
	onEvent EventFunc

	mu   sync.RWMutex
	rtts map[peer.ID]time.Duration
}

// NewManager starts the ping service and subscribes to connection events.
// onEvent may be nil.
func NewManager(ctx context.Context, h host.Host, onEvent EventFunc) *Manager {
	m := &Manager{
		ctx:     ctx,
		h:       h,
		svc:     pingsvc.NewPingService(h),
		onEvent: onEvent,
		rtts:    make(map[peer.ID]time.Duration),
	}
	h.Network().Notify(&corenet.NotifyBundle{
		ConnectedF: func(_ corenet.Network, c corenet.Conn) {
			go m.measure(c.RemotePeer())
		},
		DisconnectedF: func(n corenet.Network, c corenet.Conn) {
			p := c.RemotePeer()
			// Only treat as a disconnect once no connections remain.
			if len(n.ConnsToPeer(p)) > 0 {
				return
			}
			m.mu.Lock()
			delete(m.rtts, p)
			m.mu.Unlock()
			if m.onEvent != nil {
				m.onEvent(p, 0, false)
			}
		},
	})
	return m
}

func (m *Manager) measure(p peer.ID) {
	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()
	select {
	case res := <-m.svc.Ping(ctx, p):
		if res.Error != nil {
			return
		}
		m.mu.Lock()
		m.rtts[p] = res.RTT
		m.mu.Unlock()
		if m.onEvent != nil {
			m.onEvent(p, res.RTT.Milliseconds(), true)
		}
	case <-ctx.Done():
	}
}

// Nodes returns all currently connected libp2p nodes with their last-measured
// RTT (0 if not yet measured).
func (m *Manager) Nodes() []NodeInfo {
	peers := m.h.Network().Peers()
	out := make([]NodeInfo, 0, len(peers))
	for _, p := range peers {
		conns := m.h.Network().ConnsToPeer(p)
		if len(conns) == 0 {
			continue
		}
		m.mu.RLock()
		rtt := m.rtts[p]
		m.mu.RUnlock()
		out = append(out, NodeInfo{
			PeerID:    p.String(),
			Multiaddr: conns[0].RemoteMultiaddr().String(),
			RTTms:     rtt.Milliseconds(),
		})
	}
	return out
}

// Count returns the number of connected libp2p nodes.
func (m *Manager) Count() int {
	return len(m.h.Network().Peers())
}
