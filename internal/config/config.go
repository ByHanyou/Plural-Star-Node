// SPDX-License-Identifier: AGPL-3.0-or-later

// Package config loads, validates, and (on first run) generates the node's
// YAML configuration.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Network modes.
const (
	ModePublic       = "public"
	ModePrivate      = "private"
	ModeCustomPublic = "custom_public"
)

// DefaultAPIPort is the default port the app<->node API server listens on.
const DefaultAPIPort = 7523

// Config mirrors config.yaml. Field order matches the documented file. JSON tags
// keep the /config API response consistent with the YAML field names.
type Config struct {
	KeypairPath string `yaml:"keypair_path" json:"keypair_path"`

	NetworkMode string `yaml:"network_mode" json:"network_mode"`

	// Private network only.
	PSKPath string `yaml:"psk_path" json:"psk_path"`

	// Custom public network only.
	NetworkID string `yaml:"network_id" json:"network_id"`

	BootstrapPeers []string `yaml:"bootstrap_peers" json:"bootstrap_peers"`

	// Optional override for the hosted public-network directory URL (public /
	// custom_public modes). Empty falls back to the compiled-in default.
	DirectoryURL string `yaml:"directory_url" json:"directory_url"`

	// API server bind address. Defaults to 127.0.0.1 (local only); set to
	// 0.0.0.0 (or a specific IP) to let the app reach this node over the network.
	APIHost  string `yaml:"api_host" json:"api_host"`
	APIPort  int    `yaml:"api_port" json:"api_port"`
	APIToken string `yaml:"api_token" json:"api_token"`

	ListenAddrs   []string `yaml:"listen_addrs" json:"listen_addrs"`
	AnnounceAddrs []string `yaml:"announce_addrs" json:"announce_addrs"`

	RelayEnabled bool `yaml:"relay_enabled" json:"relay_enabled"`

	MaxPeers          int `yaml:"max_peers" json:"max_peers"`
	MaxAppConnections int `yaml:"max_app_connections" json:"max_app_connections"`
}

// Default returns a Config populated with the documented defaults. The API
// token is left empty; callers should fill it (Load does so on first run).
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

// Load reads the config from path. If the file does not exist, it generates a
// default config with a fresh random API token, writes it to path, and returns
// it with firstRun=true so the caller can surface the token to the user.
func Load(path string) (cfg *Config, firstRun bool, err error) {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if !os.IsNotExist(readErr) {
			return nil, false, fmt.Errorf("read config %q: %w", path, readErr)
		}
		// First run: write defaults and run open (no token) by default. An
		// operator who wants auth can set api_token in the generated config.
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

	// An empty api_token is allowed and means open mode (no auth); it is not
	// auto-filled, so the operator's choice to run open is respected.
	if err := cfg.Validate(); err != nil {
		return nil, false, err
	}
	return cfg, firstRun, nil
}

// Save writes the config to path as YAML with 0600 permissions (it contains the
// API token).
func Save(cfg *Config, path string) error {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(path, out, 0o600)
}

// Validate checks invariants that depend on the chosen network mode.
func (c *Config) Validate() error {
	switch c.NetworkMode {
	case ModePublic:
		// No extra requirements; bootstrap peers fall back to hardcoded defaults.
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
	// api_token is optional: empty means the API runs in open mode (no auth).
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

// generateToken returns a 32-byte random token, hex-encoded.
func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate api token: %w", err)
	}
	return hex.EncodeToString(b), nil
}
