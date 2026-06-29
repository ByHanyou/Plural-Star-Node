// SPDX-License-Identifier: AGPL-3.0-or-later

// Package host builds the go-libp2p host and the node's persistent identity.
package host

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ByHanyou/Plural-Star-Node/internal/config"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	tcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
)

// LoadOrCreateIdentity returns the Ed25519 private key at path, generating and
// persisting a new one (0600) if the file is absent.
func LoadOrCreateIdentity(path string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		priv, uErr := crypto.UnmarshalPrivateKey(data)
		if uErr != nil {
			return nil, fmt.Errorf("parse keypair %q: %w", path, uErr)
		}
		return priv, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read keypair %q: %w", path, err)
	}

	priv, _, gErr := crypto.GenerateEd25519Key(rand.Reader)
	if gErr != nil {
		return nil, fmt.Errorf("generate keypair: %w", gErr)
	}
	out, mErr := crypto.MarshalPrivateKey(priv)
	if mErr != nil {
		return nil, fmt.Errorf("marshal keypair: %w", mErr)
	}
	if wErr := os.WriteFile(path, out, 0o600); wErr != nil {
		return nil, fmt.Errorf("write keypair %q: %w", path, wErr)
	}
	return priv, nil
}

// New builds a go-libp2p host from the config and identity. psk is the optional
// 32-byte private-network key; pass nil for public/custom-public networks.
func New(cfg *config.Config, priv crypto.PrivKey, psk []byte) (host.Host, error) {
	listen, err := parseMultiaddrs(cfg.ListenAddrs)
	if err != nil {
		return nil, fmt.Errorf("listen_addrs: %w", err)
	}

	cm, err := connmgr.NewConnManager(
		cfg.MaxPeers/2, // low watermark
		cfg.MaxPeers,   // high watermark
		connmgr.WithGracePeriod(time.Minute),
	)
	if err != nil {
		return nil, fmt.Errorf("connection manager: %w", err)
	}

	// QUIC is incompatible with PSK private networks — go-libp2p returns
	// "QUIC doesn't support private networks yet" — so private nodes run
	// TCP-only and drop any QUIC listen addresses.
	if psk != nil {
		listen = dropQUIC(listen)
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrs(listen...),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.ConnectionManager(cm),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
		libp2p.EnableRelay(),
	}
	if psk == nil {
		// QUIC only on public / custom-public networks.
		opts = append(opts, libp2p.Transport(libp2pquic.NewTransport))
	}

	if cfg.RelayEnabled {
		opts = append(opts, libp2p.EnableRelayService())
	}

	if len(cfg.AnnounceAddrs) > 0 {
		announce, aErr := parseMultiaddrs(cfg.AnnounceAddrs)
		if aErr != nil {
			return nil, fmt.Errorf("announce_addrs: %w", aErr)
		}
		opts = append(opts, libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr {
			return announce
		}))
	}

	if psk != nil {
		opts = append(opts, libp2p.PrivateNetwork(psk))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("build libp2p host: %w", err)
	}
	return h, nil
}

// NewDHT starts a Kademlia DHT in server mode under the given protocol prefix
// (e.g. "/plural-star/global") and kicks off its bootstrap routine.
func NewDHT(ctx context.Context, h host.Host, prefix string) (*dht.IpfsDHT, error) {
	kdht, err := dht.New(ctx, h,
		dht.Mode(dht.ModeServer),
		dht.ProtocolPrefix(protocol.ID(prefix)),
	)
	if err != nil {
		return nil, fmt.Errorf("create dht: %w", err)
	}
	if err := kdht.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap dht: %w", err)
	}
	return kdht, nil
}

// ConnectBootstrap dials each bootstrap multiaddr. It returns the number of
// peers successfully connected and a joined error for any failures; a partial
// failure is not fatal to the caller.
func ConnectBootstrap(ctx context.Context, h host.Host, addrs []string) (int, error) {
	infos, err := ParsePeerAddrs(addrs)
	if err != nil {
		return 0, err
	}
	connected := 0
	var firstErr error
	for _, ai := range infos {
		if ai.ID == h.ID() {
			continue // never bootstrap to ourselves (this node may be in the default list)
		}
		dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if cErr := h.Connect(dialCtx, ai); cErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("connect %s: %w", ai.ID, cErr)
			}
		} else {
			connected++
		}
		cancel()
	}
	return connected, firstErr
}

// ParsePeerAddrs converts /p2p/-terminated multiaddr strings into AddrInfos.
func ParsePeerAddrs(addrs []string) ([]peer.AddrInfo, error) {
	out := make([]peer.AddrInfo, 0, len(addrs))
	for _, s := range addrs {
		maddr, err := ma.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("parse bootstrap addr %q: %w", s, err)
		}
		ai, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("bootstrap addr %q missing /p2p component: %w", s, err)
		}
		out = append(out, *ai)
	}
	return out, nil
}

// dropQUIC removes QUIC listen addresses (used for PSK private networks, where
// QUIC is unsupported).
func dropQUIC(addrs []ma.Multiaddr) []ma.Multiaddr {
	out := make([]ma.Multiaddr, 0, len(addrs))
	for _, a := range addrs {
		if strings.Contains(a.String(), "quic") {
			continue
		}
		out = append(out, a)
	}
	return out
}

func parseMultiaddrs(addrs []string) ([]ma.Multiaddr, error) {
	out := make([]ma.Multiaddr, 0, len(addrs))
	for _, s := range addrs {
		maddr, err := ma.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("parse multiaddr %q: %w", s, err)
		}
		out = append(out, maddr)
	}
	return out, nil
}
