// SPDX-License-Identifier: AGPL-3.0-or-later

// Package api implements the local app<->node API: a REST + WebSocket server
// authenticated with a bearer token.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Node/internal/network"
	"github.com/ByHanyou/Plural-Star-Node/internal/ping"
	"github.com/ByHanyou/Plural-Star-Node/internal/relay"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Server is the app-facing HTTP/WebSocket API.
type Server struct {
	cfg        *config.Config
	configPath string
	h          host.Host
	ping       *ping.Manager
	relay      *relay.Manager
	networks   *network.Store
	scope      string
	startedAt  time.Time

	httpSrv  *http.Server
	upgrader websocket.Upgrader

	mu      sync.RWMutex
	clients map[*wsClient]struct{}
	appPeer peer.ID // most recently registered local app, used as /send sender

	rv *rendezvousStore // in-memory code -> identity discovery (no disk)
}

// NewServer constructs the API server. Call SetPing and SetRelay before Start.
func NewServer(cfg *config.Config, configPath string, h host.Host, scope string) *Server {
	return &Server{
		cfg:        cfg,
		configPath: configPath,
		h:          h,
		scope:      scope,
		startedAt:  time.Now(),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		clients: make(map[*wsClient]struct{}),
		rv:      newRendezvousStore(),
	}
}

// SetPing injects the ping manager (created after the server so its node-event
// callback can target this server's WebSocket clients).
func (s *Server) SetPing(m *ping.Manager) { s.ping = m }

// SetRelay injects the relay manager (created after the server so its peer-event
// callback can target this server's WebSocket clients).
func (s *Server) SetRelay(m *relay.Manager) { s.relay = m }

// SetNetworks injects the known-networks store (nil on private networks).
func (s *Server) SetNetworks(store *network.Store) { s.networks = store }

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.authed(s.handleHealth))
	mux.HandleFunc("/nodes", s.authed(s.handleNodes))
	mux.HandleFunc("/peers", s.authed(s.handlePeers))
	mux.HandleFunc("/networks", s.authed(s.handleNetworks))
	mux.HandleFunc("/send", s.authed(s.handleSend))
	mux.HandleFunc("/invite/generate", s.authed(s.handleInviteGenerate))
	mux.HandleFunc("/invite/accept", s.authed(s.handleInviteAccept))
	mux.HandleFunc("/config", s.authed(s.handleConfig))
	mux.HandleFunc("/rendezvous/register", s.authed(s.handleRendezvousRegister))
	mux.HandleFunc("/rendezvous/lookup", s.authed(s.handleRendezvousLookup))
	mux.HandleFunc("/ws", s.authed(s.handleWS))
	return mux
}

// Start binds the API port and serves until Shutdown. It returns once the
// listener stops.
func (s *Server) Start() error {
	host := s.cfg.APIHost
	if host == "" {
		host = "127.0.0.1"
	}
	s.httpSrv = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", host, s.cfg.APIPort),
		Handler: s.routes(),
	}
	log.Printf("API server listening on http://%s:%d", host, s.cfg.APIPort)
	if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully stops the HTTP server and closes WebSocket clients.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	for c := range s.clients {
		c.close()
	}
	s.mu.Unlock()
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

// ----- event hooks (wired into relay + ping) -----

// OnAppPeerEvent is the relay.PeerEvent callback: broadcast app online/offline.
func (s *Server) OnAppPeerEvent(peerID, viaNode peer.ID, online bool) {
	if online {
		s.broadcast(peerOnlineEvent{Type: "peer_online", PeerID: peerID.String(), ViaNode: viaNode.String()})
	} else {
		s.broadcast(peerOfflineEvent{Type: "peer_offline", PeerID: peerID.String()})
	}
}

// OnNodeEvent is the ping.EventFunc callback: broadcast node connect/disconnect.
func (s *Server) OnNodeEvent(peerID peer.ID, rttMs int64, connected bool) {
	if connected {
		s.broadcast(nodeConnectedEvent{Type: "node_connected", NodePeerID: peerID.String(), RTTms: rttMs})
	} else {
		s.broadcast(nodeDisconnectedEvent{Type: "node_disconnected", NodePeerID: peerID.String()})
	}
}

// ----- broadcast helpers -----

func (s *Server) broadcast(ev any) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	s.mu.RLock()
	for c := range s.clients {
		c.trySend(b)
	}
	s.mu.RUnlock()
}

func (s *Server) sendToApp(appPeer peer.ID, ev any) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	s.mu.RLock()
	for c := range s.clients {
		if c.appPeer == appPeer {
			c.trySend(b)
		}
	}
	s.mu.RUnlock()
}

func (s *Server) addClient(c *wsClient) {
	s.mu.Lock()
	s.clients[c] = struct{}{}
	s.appPeer = c.appPeer
	s.mu.Unlock()
}

func (s *Server) removeClient(c *wsClient) {
	s.mu.Lock()
	delete(s.clients, c)
	if s.appPeer == c.appPeer {
		s.appPeer = ""
	}
	s.mu.Unlock()
}

func (s *Server) currentAppPeer() peer.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appPeer
}

// ----- response helpers -----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
