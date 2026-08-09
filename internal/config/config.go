// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	ModePublic       = "public"
	ModePrivate      = "private"
	ModeCustomPublic = "custom_public"
)

const DefaultAPIPort = 7523

type Config struct {
	KeypairPath string `yaml:"keypair_path" json:"keypair_path"`

	NetworkMode string `yaml:"network_mode" json:"network_mode"`

	PSKPath string `yaml:"psk_path" json:"psk_path"`

	NetworkID string `yaml:"network_id" json:"network_id"`

	BootstrapPeers []string `yaml:"bootstrap_peers" json:"bootstrap_peers"`

	DirectoryURL string `yaml:"directory_url" json:"directory_url"`

	APIHost  string `yaml:"api_host" json:"api_host"`
	APIPort  int    `yaml:"api_port" json:"api_port"`
	APIToken string `yaml:"api_token" json:"api_token"`

	ListenAddrs   []string `yaml:"listen_addrs" json:"listen_addrs"`
	AnnounceAddrs []string `yaml:"announce_addrs" json:"announce_addrs"`

	RelayEnabled bool `yaml:"relay_enabled" json:"relay_enabled"`

	MaxPeers          int `yaml:"max_peers" json:"max_peers"`
	MaxAppConnections int `yaml:"max_app_connections" json:"max_app_connections"`
}

func Default() *Config {
	return &Config{
		KeypairPath:    "./node.key",
		NetworkMode:    ModePublic,
		PSKPath:        "./network.psk",
		NetworkID:      "",
		BootstrapPeers: []string{},
		DirectoryURL:   "",
		APIHost:        "127.0.0.1",
		APIPort:        DefaultAPIPort,
		APIToken:       "",
		ListenAddrs: []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
		},
		AnnounceAddrs:     []string{},
		RelayEnabled:      true,
		MaxPeers:          200,
		MaxAppConnections: 5000,
	}
}

func Load(path string) (cfg *Config, firstRun bool, err error) {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if !os.IsNotExist(readErr) {
			return nil, false, fmt.Errorf("read config %q: %w", path, readErr)
		}
		cfg = Default()
		if err := Save(cfg, path); err != nil {
			return nil, false, fmt.Errorf("write initial config %q: %w", path, err)
		}
		if err := cfg.Validate(); err != nil {
			return nil, false, err
		}
		return cfg, true, nil
	}

	cfg = Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, false, fmt.Errorf("parse config %q: %w", path, err)
	}

	// A node on the default public network exists to serve app users who have
	// no token and never will, so every endpoint an app touches is behind the
	// same auth gate. An api_token here therefore does not secure the node, it
	// shuts the whole network out, and because nothing reads the file until the
	// next start the damage stays invisible until a reboot. Strip it, say so,
	// and write the file back so it cannot come back a third time. Private and
	// custom_public networks are operator-run and still honour a token.
	if cfg.NetworkMode == ModePublic && cfg.APIToken != "" {
		cfg.APIToken = ""
		log.Printf("config: api_token ignored and removed from %s. A public node must stay open or it locks out every app on the default network.", path)
		if err := Save(cfg, path); err != nil {
			return nil, false, fmt.Errorf("clear api_token in %q: %w", path, err)
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, false, err
	}
	return cfg, firstRun, nil
}

func Save(cfg *Config, path string) error {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(path, out, 0o600)
}

func (c *Config) Validate() error {
	switch c.NetworkMode {
	case ModePublic:
	case ModePrivate:
		if c.PSKPath == "" {
			return fmt.Errorf("network_mode=private requires psk_path")
		}
		if len(c.BootstrapPeers) == 0 {
			return fmt.Errorf("network_mode=private requires at least one bootstrap_peers entry")
		}
	case ModeCustomPublic:
		if c.NetworkID == "" {
			return fmt.Errorf("network_mode=custom_public requires network_id")
		}
	default:
		return fmt.Errorf("invalid network_mode %q (want public, private, or custom_public)", c.NetworkMode)
	}

	if c.APIPort <= 0 || c.APIPort > 65535 {
		return fmt.Errorf("api_port %d out of range", c.APIPort)
	}
	if len(c.ListenAddrs) == 0 {
		return fmt.Errorf("at least one listen_addr is required")
	}
	if c.MaxPeers <= 0 {
		return fmt.Errorf("max_peers must be positive")
	}
	if c.MaxAppConnections <= 0 {
		return fmt.Errorf("max_app_connections must be positive")
	}
	return nil
}
