// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"fmt"
	"os"

	"github.com/ByHanyou/Plural-Star-Node/internal/config"
)

// ApplyInvite writes the invite's PSK to pskPath and mutates cfg to join the
// private network: sets private mode, the PSK path, the label as network ID, and
// adds the inviting node(s) as bootstrap peers. The caller is responsible for
// persisting cfg (config.Save) and restarting the node.
func ApplyInvite(cfg *config.Config, inv Invite, pskPath string) error {
	if pskPath == "" {
		pskPath = "./network.psk"
	}
	if len(inv.PSK) != pskLen {
		return fmt.Errorf("invite psk must be %d bytes", pskLen)
	}
	if err := os.WriteFile(pskPath, inv.PSK, 0o600); err != nil {
		return fmt.Errorf("write psk %q: %w", pskPath, err)
	}

	cfg.NetworkMode = config.ModePrivate
	cfg.PSKPath = pskPath
	if inv.Label != "" {
		cfg.NetworkID = inv.Label
	}
	for _, a := range inv.Multiaddrs {
		s := a.String()
		if !containsString(cfg.BootstrapPeers, s) {
			cfg.BootstrapPeers = append(cfg.BootstrapPeers, s)
		}
	}
	return nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
