// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/ByHanyou/Plural-Star-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Node/internal/network"
	"github.com/ByHanyou/Plural-Star-Node/internal/relay"

	"github.com/libp2p/go-libp2p/core/peer"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	networkID := s.cfg.NetworkID
	if networkID == "" && s.cfg.NetworkMode == config.ModePublic {
		networkID = "plural-star-global"
	}
	s.mu.RLock()
	apps := len(s.clients)
	s.mu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"status":          "ok",
		"peer_id":         s.h.ID().String(),
		"network_mode":    s.cfg.NetworkMode,
		"network_id":      networkID,
		"connected_nodes": s.ping.Count(),
		"connected_apps":  apps,
		"uptime_seconds":  int64(time.Since(s.startedAt).Seconds()),
	})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.ping.Nodes())
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	seen := make(map[string]struct{})
	out := make([]map[string]string, 0)

	// Locally connected apps first: they are NOT in the routing table (local
	// delivery uses a separate map, and the node skips its own presence gossip),
	// yet they're exactly the peers an app on this node most needs to see.
	self := s.h.ID().String()
	s.mu.RLock()
	for c := range s.clients {
		id := c.appPeer.String()
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, map[string]string{
			"peer_id":  id,
			"via_node": self,
		})
	}
	s.mu.RUnlock()

	for _, pr := range s.relay.Router().Snapshot() {
		id := pr.Peer.String()
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, map[string]string{
			"peer_id":  id,
			"via_node": pr.Via.String(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleNetworks(w http.ResponseWriter, r *http.Request) {
	if s.networks == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	cards, err := s.networks.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read networks: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, cards)
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		SenderID  string `json:"sender_peer_id,omitempty"`
		Recipient string `json:"recipient_peer_id"`
		Payload   string `json:"payload"`
		PacketID  string `json:"packet_id,omitempty"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	recipient, err := peer.Decode(req.Recipient)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid recipient_peer_id")
		return
	}
	payload, err := base64.StdEncoding.DecodeString(req.Payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, "payload must be base64")
		return
	}

	var id [16]byte
	if req.PacketID != "" {
		raw, derr := hex.DecodeString(req.PacketID)
		if derr != nil || len(raw) != 16 {
			writeError(w, http.StatusBadRequest, "packet_id must be 32 hex characters (16 bytes)")
			return
		}
		copy(id[:], raw)
	} else {
		gid, gerr := relay.NewPacketID()
		if gerr != nil {
			writeError(w, http.StatusInternalServerError, "could not generate packet id")
			return
		}
		id = gid
	}
	var sender peer.ID
	if req.SenderID != "" {
		parsed, pErr := peer.Decode(req.SenderID)
		if pErr != nil {
			writeError(w, http.StatusBadRequest, "invalid sender_peer_id")
			return
		}
		sender = parsed
	} else {
		sender = s.currentAppPeer()
	}
	pkt := &relay.Packet{
		ID:          id,
		SenderID:    []byte(sender),
		RecipientID: []byte(recipient),
		Payload:     payload,
		Timestamp:   time.Now().UnixMilli(),
	}
	s.trafficf("app->node sender=%s recipient=%s packet_id=%s payload_bytes=%d", sender, recipient, hex.EncodeToString(id[:]), len(payload))

	if rErr := s.relay.Route(pkt); rErr != nil {
		if errors.Is(rErr, relay.ErrNoRoute) && sender != "" {
			s.sendToApp(sender, errorEvent{
				Type:    "error",
				Code:    "SEND_FAILED",
				Message: "Recipient not found in routing table",
			})
		}
	}
	// Delivery is best-effort; the node does not confirm receipt. The packet_id
	// is returned so the app can reuse it when sending the same packet to its
	// other connected nodes (multi-path redundancy + dedup).
	writeJSON(w, http.StatusOK, map[string]string{
		"status":    "queued",
		"packet_id": hex.EncodeToString(id[:]),
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	redacted := *s.cfg
	redacted.APIToken = "***redacted***"
	writeJSON(w, http.StatusOK, redacted)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	appPeer, err := peer.Decode(r.URL.Query().Get("peer_id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing or invalid peer_id query parameter")
		return
	}
	s.mu.RLock()
	atCapacity := s.cfg.MaxAppConnections > 0 && len(s.clients) >= s.cfg.MaxAppConnections
	s.mu.RUnlock()
	if atCapacity {
		writeError(w, http.StatusServiceUnavailable, "max_app_connections reached")
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote an error response
	}

	client := newWSClient(conn, appPeer)
	s.addClient(client)
	s.trafficf("ws connected app_peer=%s remote=%s", appPeer, r.RemoteAddr)

	// Tell every connected app this peer is online. The presence gossip only
	// fires events for REMOTE transitions (the read loop skips this node's own
	// announcements), so without this, two apps connected to the SAME node —
	// the common case on the public node — never see each other come online.
	s.broadcast(peerOnlineEvent{Type: "peer_online", PeerID: appPeer.String(), ViaNode: s.h.ID().String()})

	// Register with the relay so packets for this app are delivered over the WS.
	s.relay.AppConnected(appPeer, func(p *relay.Packet) {
		sender := ""
		if sid, e := peer.IDFromBytes(p.SenderID); e == nil {
			sender = sid.String()
		}
		s.trafficf("node->app recipient=%s sender=%s packet_id=%s payload_bytes=%d", appPeer, sender, hex.EncodeToString(p.ID[:]), len(p.Payload))
		ev := packetReceivedEvent{
			Type:         "packet_received",
			SenderPeerID: sender,
			Payload:      base64.StdEncoding.EncodeToString(p.Payload),
			Timestamp:    p.Timestamp,
		}
		if b, e := json.Marshal(ev); e == nil {
			client.trySend(b)
		}
	})

	go client.writePump()
	client.readPump(func() {
		s.removeClient(client)
		client.close()
		s.trafficf("ws disconnected app_peer=%s remote=%s", appPeer, r.RemoteAddr)
		// Mirror of the local peer_online above — announce the local offline
		// transition to the remaining connected apps.
		if !s.hasClientFor(appPeer) {
			s.relay.AppDisconnected(appPeer)
			s.broadcast(peerOfflineEvent{Type: "peer_offline", PeerID: appPeer.String()})
		}
	})
}

func (s *Server) handleInviteGenerate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	if s.cfg.NetworkMode != config.ModePrivate {
		writeError(w, http.StatusBadRequest, "invites are only available in private network mode")
		return
	}
	psk, err := network.LoadPSK(s.cfg.PSKPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load psk: "+err.Error())
		return
	}
	p2pAddrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{ID: s.h.ID(), Addrs: s.h.Addrs()})
	if err != nil || len(p2pAddrs) == 0 {
		writeError(w, http.StatusInternalServerError, "node has no advertisable addresses")
		return
	}
	invite, err := network.EncodeInvite(network.Invite{
		Multiaddrs: p2pAddrs,
		PSK:        psk,
		Label:      s.cfg.NetworkID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"invite": invite})
}

func (s *Server) handleInviteAccept(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		Invite string `json:"invite"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	inv, err := network.DecodeInvite(req.Invite)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid invite: "+err.Error())
		return
	}
	if err := network.ApplyInvite(s.cfg, inv, s.cfg.PSKPath); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := config.Save(s.cfg, s.configPath); err != nil {
		writeError(w, http.StatusInternalServerError, "could not persist config: "+err.Error())
		return
	}
	// Joining a PSK-protected network requires rebuilding the libp2p host with
	// the new key, which happens on restart.
	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "accepted",
		"restart_required": true,
	})
}
