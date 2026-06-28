// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	wsWriteWait  = 10 * time.Second
	wsPongWait   = 60 * time.Second
	wsPingPeriod = 50 * time.Second
	wsSendBuffer = 64
)

// wsClient is one connected app's WebSocket session.
type wsClient struct {
	conn      *websocket.Conn
	appPeer   peer.ID
	send      chan []byte
	closeOnce sync.Once
}

func newWSClient(conn *websocket.Conn, appPeer peer.ID) *wsClient {
	return &wsClient{
		conn:    conn,
		appPeer: appPeer,
		send:    make(chan []byte, wsSendBuffer),
	}
}

// trySend queues a message, dropping it if the client's buffer is full (a slow
// app must not block the relay or other clients).
func (c *wsClient) trySend(b []byte) {
	select {
	case c.send <- b:
	default:
	}
}

func (c *wsClient) close() {
	c.closeOnce.Do(func() { close(c.send) })
}

// writePump serializes all writes to the connection and sends periodic pings.
func (c *wsClient) writePump() {
	ticker := time.NewTicker(wsPingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump drains inbound frames (the app sends via REST, not the socket) and
// detects disconnection. It calls onClose exactly once when the socket ends.
func (c *wsClient) readPump(onClose func()) {
	defer onClose()
	_ = c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}
