// SPDX-License-Identifier: AGPL-3.0-or-later

// Package network holds network-mode helpers: DHT/gossip namespacing, the
// hardcoded public bootstrap fallback, and PSK loading for private networks.
package network

import (
	"fmt"
	"os"

	"github.com/ByHanyou/Plural-Star-Node/internal/config"
)

// GlobalScope is the scope label used by the public (global) network.
const GlobalScope = "global"

// DefaultBootstrapPeers are the well-known Plural Star public bootstrap nodes,
// used as a fallback when a public node has no bootstrap_peers configured.
//
// TODO: populate with the real deployed bootstrap node multiaddrs before public
// release, e.g. "/ip4/<ip>/tcp/4001/p2p/<peerid>".
var DefaultBootstrapPeers = []string{
	"/dns4/pluralstar.dedyn.io/tcp/4001/p2p/12D3KooWL4A25M2sWt1HoFY3r8hWn2idbc4yVdsdBqpFitRTNyfz",
}

// Scope returns the network scope label ("global" or the custom network_id).
func Scope(cfg *config.Config) string {
	if cfg.NetworkMode == config.ModeCustomPublic {
		return cfg.NetworkID
	}
	return GlobalScope
}

// DHTPrefix returns the DHT protocol prefix for the configured mode,
// e.g. "/plural-star/global" or "/plural-star/<network_id>".
func DHTPrefix(cfg *config.Config) string {
	return "/plural-star/" + Scope(cfg)
}

// GossipPrefix returns the GossipSub topic prefix for the configured mode,
// e.g. "plural-star:global:" or "plural-star:<network_id>:".
func GossipPrefix(cfg *config.Config) string {
	return "plural-star:" + Scope(cfg) + ":"
}

// BootstrapPeers returns the effective bootstrap list: the configured peers,
// or the hardcoded fallback when a public node has none configured.
func BootstrapPeers(cfg *config.Config) []string {
	if len(cfg.BootstrapPeers) > 0 {
		return cfg.BootstrapPeers
	}
	if cfg.NetworkMode == config.ModePublic {
		return DefaultBootstrapPeers
	}
	return nil
}

// LoadPSK reads a raw 32-byte pre-shared key from path for private networks.
func LoadPSK(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read psk %q: %w", path, err)
	}
	if len(data) != 32 {
		return nil, fmt.Errorf("psk %q must be exactly 32 bytes, got %d", path, len(data))
	}
	return data, nil
}
