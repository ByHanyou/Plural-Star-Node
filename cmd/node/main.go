// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/ByHanyou/Plural-Star-Node/internal/api"
	"github.com/ByHanyou/Plural-Star-Node/internal/config"
	psnhost "github.com/ByHanyou/Plural-Star-Node/internal/host"
	"github.com/ByHanyou/Plural-Star-Node/internal/network"
	"github.com/ByHanyou/Plural-Star-Node/internal/ping"
	"github.com/ByHanyou/Plural-Star-Node/internal/relay"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

type node struct {
	cfg   *config.Config
	h     host.Host
	dht   *dht.IpfsDHT
	ps    *pubsub.PubSub
	rd    *drouting.RoutingDiscovery
	relay *relay.Manager
	ping  *ping.Manager
	api   *api.Server
	networks *network.Store
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	flag.Parse()
	if err := run(*configPath); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func run(configPath string) error {
	cfg, firstRun, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if firstRun {
		printFirstRun(configPath, cfg.APIToken)
	}

	priv, err := psnhost.LoadOrCreateIdentity(cfg.KeypairPath)
	if err != nil {
		return err
	}

	var psk []byte
	if cfg.NetworkMode == config.ModePrivate {
		psk, err = network.LoadPSK(cfg.PSKPath)
		if err != nil {
			return err
		}
	}

	h, err := psnhost.New(cfg, priv, psk)
	if err != nil {
		return err
	}
	defer h.Close()

	n := &node{cfg: cfg, h: h}

	log.Printf("node peer ID: %s", h.ID())
	log.Printf("network mode: %s (scope %q)", cfg.NetworkMode, network.Scope(cfg))
	for _, a := range h.Addrs() {
		log.Printf("listening on: %s/p2p/%s", a, h.ID())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.NetworkMode != config.ModePrivate {
		kdht, dErr := psnhost.NewDHT(ctx, h, network.DHTPrefix(cfg))
		if dErr != nil {
			return dErr
		}
		defer kdht.Close()
		n.dht = kdht
		log.Printf("DHT started (prefix %s)", network.DHTPrefix(cfg))
	}

	ps, rd, gErr := network.NewGossipSub(ctx, h, n.dht)
	if gErr != nil {
		return fmt.Errorf("gossipsub: %w", gErr)
	}
	n.ps, n.rd = ps, rd
	log.Printf("gossipsub ready")

	if n.rd != nil {
		network.AdvertiseAndDiscover(ctx, h, n.rd, network.DHTPrefix(cfg))
		log.Printf("DHT peer discovery advertising %q", network.DHTPrefix(cfg))
	}

	mdnsSvc, mErr := network.SetupMDNS(ctx, h)
	if mErr != nil {
		log.Printf("warning: mDNS unavailable: %v", mErr)
	} else {
		defer mdnsSvc.Close()
		log.Printf("mDNS LAN discovery started")
	}

	srv := api.NewServer(cfg, configPath, h, network.Scope(cfg))
	n.api = srv

	pingMgr := ping.NewManager(ctx, h, srv.OnNodeEvent)
	n.ping = pingMgr
	srv.SetPing(pingMgr)

	mgr, rErr := relay.NewManager(ctx, h, n.ps, network.GossipPrefix(cfg), srv.OnAppPeerEvent)
	if rErr != nil {
		return fmt.Errorf("relay manager: %w", rErr)
	}
	n.relay = mgr
	srv.SetRelay(mgr)
	log.Printf("relay protocol %s registered", relay.RelayProtocol)

	if cfg.NetworkMode != config.ModePrivate {
		store, sErr := network.OpenStore(network.NetworkDBDefault)
		if sErr != nil {
			return fmt.Errorf("network store: %w", sErr)
		}
		defer store.Close()
		n.networks = store
		srv.SetNetworks(store)

		if _, ndErr := network.NewNetworkDiscovery(ctx, n.ps, h.ID(), store); ndErr != nil {
			return fmt.Errorf("network discovery: %w", ndErr)
		}
		log.Printf("network discovery joined %q", network.DiscoveryTopic)

		directoryURL := cfg.DirectoryURL
		if directoryURL == "" {
			directoryURL = network.DefaultDirectoryURL
		}
		go func() {
			if count, fErr := network.FetchDirectory(ctx, directoryURL, store); fErr != nil {
				log.Printf("directory fetch skipped: %v", fErr)
			} else if count > 0 {
				log.Printf("directory: cached %d network card(s)", count)
			}
		}()
	}

	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("API server error: %v", err)
		}
	}()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	bootstrapPeers := network.BootstrapPeers(cfg)
	if len(bootstrapPeers) > 0 {
		connected, bErr := psnhost.ConnectBootstrap(ctx, h, bootstrapPeers)
		log.Printf("initial bootstrap: connected to %d/%d peers", connected, len(bootstrapPeers))
		if bErr != nil {
			log.Printf("initial bootstrap partial: %v", bErr)
		}
	} else if cfg.NetworkMode == config.ModePublic {
		log.Printf("warning: no bootstrap peers")
	}

	if len(bootstrapPeers) > 0 {
		go persistentReconnectLoop(ctx, h, bootstrapPeers)
	}

	go monitorConnections(ctx, h)

	// Single-node self-advertise for testing
	if n.rd != nil {
		go singleNodeAdvertise(ctx, n.rd, network.DHTPrefix(cfg))
	}

	log.Printf("node running; press Ctrl-C to stop")
	<-ctx.Done()
	log.Printf("shutting down")
	return nil
}

func persistentReconnectLoop(ctx context.Context, h host.Host, bootstrapPeers []string) {
	ticker := time.NewTicker(90 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			connected, err := psnhost.ConnectBootstrap(ctx, h, bootstrapPeers)
			if connected > 0 {
				log.Printf("reconnect success: %d bootstrap peers", connected)
			} else if err != nil {
				log.Printf("reconnect failed: %v", err)
			}
		}
	}
}

func monitorConnections(ctx context.Context, h host.Host) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conns := h.Network().Conns()
			log.Printf("active libp2p connections: %d", len(conns))
		}
	}
}

func singleNodeAdvertise(ctx context.Context, rd *drouting.RoutingDiscovery, rendezvous string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dutil.Advertise(ctx, rd, rendezvous)
			log.Printf("single-node: re-advertised on %s", rendezvous)
		}
	}
}

func printFirstRun(configPath, token string) {
	fmt.Println("=====================================================")
	fmt.Println(" Plural Star Node — first run")
	fmt.Printf(" Wrote default config to: %s\n", configPath)
	fmt.Printf(" API token (configure this in your Plural Star app):\n   %s\n", token)
	fmt.Println("=====================================================")
}