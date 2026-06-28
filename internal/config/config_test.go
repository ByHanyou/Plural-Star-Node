// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"path/filepath"
	"testing"
)

func TestValidateNetworkModes(t *testing.T) {
	base := func() *Config {
		c := Default()
		c.APIToken = "x" // Validate requires a token
		return c
	}

	t.Run("public ok", func(t *testing.T) {
		if err := base().Validate(); err != nil {
			t.Fatalf("public should be valid: %v", err)
		}
	})

	t.Run("private requires psk and bootstrap", func(t *testing.T) {
		c := base()
		c.NetworkMode = ModePrivate
		c.PSKPath = ""
		if err := c.Validate(); err == nil {
			t.Fatal("private without psk_path should fail")
		}
		c.PSKPath = "./network.psk"
		c.BootstrapPeers = nil
		if err := c.Validate(); err == nil {
			t.Fatal("private without bootstrap_peers should fail")
		}
		c.BootstrapPeers = []string{"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWGFEV2PobB8q33b9MW5sCeKtpfTdWHyPmoV6sAYqZHcCU"}
		if err := c.Validate(); err != nil {
			t.Fatalf("private with psk+bootstrap should be valid: %v", err)
		}
	})

	t.Run("custom_public requires network_id", func(t *testing.T) {
		c := base()
		c.NetworkMode = ModeCustomPublic
		if err := c.Validate(); err == nil {
			t.Fatal("custom_public without network_id should fail")
		}
		c.NetworkID = "my-net"
		if err := c.Validate(); err != nil {
			t.Fatalf("custom_public with network_id should be valid: %v", err)
		}
	})

	t.Run("rejects bad fields", func(t *testing.T) {
		c := base()
		c.NetworkMode = "bogus"
		if err := c.Validate(); err == nil {
			t.Fatal("invalid network_mode should fail")
		}
		c = base()
		c.APIPort = 0
		if err := c.Validate(); err == nil {
			t.Fatal("api_port 0 should fail")
		}
		c = base()
		c.APIToken = ""
		if err := c.Validate(); err == nil {
			t.Fatal("empty api_token should fail")
		}
		c = base()
		c.ListenAddrs = nil
		if err := c.Validate(); err == nil {
			t.Fatal("no listen_addrs should fail")
		}
	})
}

func TestLoadFirstRunGeneratesTokenAndFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg, firstRun, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !firstRun {
		t.Fatal("expected firstRun=true on a fresh path")
	}
	if cfg.APIToken == "" {
		t.Fatal("expected a generated api_token")
	}

	// A second load reads the persisted file and is no longer first-run.
	cfg2, firstRun2, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if firstRun2 {
		t.Fatal("expected firstRun=false on reload")
	}
	if cfg2.APIToken != cfg.APIToken {
		t.Fatal("api_token should persist across loads")
	}
}
