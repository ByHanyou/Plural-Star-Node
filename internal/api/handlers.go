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
		"status":         "ok",
		"peer_id":        s.h.ID().String(),
		"network_mode":   s.cfg.NetworkMode,
		"network_id":     networkID,
		"connected_nodes": s.ping.Count(),
		"connected_apps": apps,
		"uptime_seconds": int64(time.Since(s.startedAt).Seconds()),
	})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.ping.Nodes())
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	routes := s.relay.Router().Snapshot()
	out := make([]map[string]string, 0, len(routes))
	for _, pr := range routes {
		out = append(out, map[string]string{
			"peer_id":  pr.Peer.String(),
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
		Recipient string `json:"recipient_peer_id"`
		Payload   string `json:"payload"`
		// PacketID is optional. For multi-path redundancy the app sends the same
		// packet to each connected node with the SAME id, so duplicates are
		// suppressed by the dedup cache. If omitted, the node generates one.
		PacketID string `json:"packet_id,omitempty"`
	}
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
	sender := s.currentAppPeer()
	pkt := &relay.Packet{
		ID:          id,
		SenderID:    []byte(sender),
		RecipientID: []byte(recipient),
		Payload:     payload,
		Timestamp:   time.Now().UnixMilli(),
	}

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
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote an error response
	}

	client := newWSClient(conn, appPeer)
	s.addClient(client)

	// Register with the relay so packets for this app are delivered over the WS.
	s.relay.AppConnected(appPeer, func(p *relay.Packet) {
		sender := ""
		if sid, e := peer.IDFromBytes(p.SenderID); e == nil {
			sender = sid.String()
		}
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
		s.relay.AppDisconnected(appPeer)
		s.removeClient(client)
		client.close()
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
