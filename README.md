# Plural Star Node Network

A standalone, decentralized, relay-only peer-to-peer node for Plural Star's Friend & Syncing System. It runs on PCs and Raspberry Pis as a background service and forms the relay infrastructure that Plural Star app clients connect to.

This is a separate program from the Plural Star app. It is built directly on [go-libp2p](https://github.com/libp2p/go-libp2p) and treats relay nodes as hostile infrastructure by design: a node sees only peer IDs and opaque, end-to-end-encrypted blobs. It never stores user data, messages, relationships, or sync content.

## Related projects & links

<p align="center">
  <a href="https://www.buymeacoffee.com/PluralStar">
    <img src="https://img.buymeacoffee.com/button-api/?text=Support+PS&amp;emoji=%E2%98%95&amp;slug=PluralStar&amp;button_colour=151929&amp;font_colour=ffffff&amp;font_family=Cookie&amp;outline_colour=ffffff&amp;coffee_colour=FFDD00" alt="Support Plural Star on Buy Me a Coffee" />
  </a>
  &nbsp;
  <a href="https://discord.gg/FFQw33cu8m">
    <img src="https://img.shields.io/badge/Discord-Join%20Us-5865F2?style=for-the-badge&logo=discord&logoColor=white" alt="Join our Discord" />
  </a>
</p>

- **Plural Star** (the app this node serves) — [Website](https://byhanyou.github.io/Plural-Star/) · [GitHub](https://github.com/ByHanyou/Plural-Star) · [App Store](https://apps.apple.com/in/app/plural-star/id6763964266) · [Google Play](https://play.google.com/store/apps/details?id=com.pluralspace.app)
- **Plural Star Desktop** - [GitHub](https://github.com/ByHanyou/Plural-Star-Desktop) · [Latest release](https://github.com/ByHanyou/Plural-Star-Desktop/releases/latest)
- **Community** - [Reddit](https://www.reddit.com/r/PluralStar/)

## What it does

- Joins a libp2p mesh (public, private, or custom-public).
- Relays encrypted packets between connected Plural Star app clients.
- Exposes a local WebSocket + REST API (default `127.0.0.1:7523`) that the app connects to.
- Keeps an in-memory routing table from gossiped presence, with packet dedup for multi-path redundancy.

## Requirements

- Go 1.21+ — the module declares `go 1.25.7`, so on an older (but toolchain-aware) Go, `go build` downloads the `go1.25.7` toolchain automatically.

## Build and run

Windows (`cmd` or PowerShell):

```bat
go build -o plural-star-node.exe ./cmd/node
plural-star-node.exe --config config.yaml
```

Linux / macOS:

```sh
go build -o plural-star-node ./cmd/node
./plural-star-node --config config.yaml
```

On first run with no config, the node writes a default `config.yaml`, generates an Ed25519 identity (`node.key`), generates a random API token, and prints the token to stdout — configure that token in your Plural Star app.

### Cross-compile

`make dist` builds fully static binaries for all release targets into `dist/`: linux amd64, linux arm64 (Pi 4/5), linux armv7 (Pi 3), darwin arm64, windows amd64. Without `make`, set the target and build directly.

Linux / macOS:

```sh
GOOS=linux GOARCH=arm64 go build -o plural-star-node-arm64 ./cmd/node
```

Windows (`cmd`):

```bat
set GOOS=linux
set GOARCH=arm64
go build -o plural-star-node-arm64 ./cmd/node
```

## Configuration

See `config.yaml.example`. Key fields:

| Field | Meaning |
|---|---|
| `network_mode` | `public`, `private`, or `custom_public` |
| `psk_path` | 32-byte pre-shared key file (required for `private`) |
| `network_id` | namespacing string (required for `custom_public`) |
| `bootstrap_peers` | multiaddr list; required for `private`/`custom_public` |
| `api_host` | API bind address; `127.0.0.1` (default, local only) or `0.0.0.0` to allow the app to connect over the network |
| `api_port` / `api_token` | API port and bearer token |
| `listen_addrs` | libp2p listen multiaddrs |
| `relay_enabled` | whether this node forwards traffic for others |
| `max_peers` / `max_app_connections` | connection limits |

### Network modes

- **Public** — open join via Kad-DHT + GossipSub under namespace `plural-star/global`. If no bootstrap peers are set in config, the built-in defaults are used.
- **Private** — PSK-enforced via libp2p `pnet`. Nodes without the matching key cannot complete the handshake. No DHT or public discovery; bootstrap peers must be specified. **Runs TCP-only:** go-libp2p's QUIC transport does not support private networks, so QUIC is disabled automatically when a PSK is set.
- **Custom public** — open join scoped under `plural-star/<network_id>`, separate from the global network.

## API

All endpoints require `Authorization: Bearer <api_token>` (WebSocket clients may instead pass `?token=<api_token>`).

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Node status, mode, connected node/app counts, uptime |
| GET | `/nodes` | Connected libp2p nodes with RTT |
| GET | `/peers` | Known online app peers (from routing table) |
| GET | `/networks` | Known public networks (from discovery cache) |
| POST | `/send` | Send a packet: `{recipient_peer_id, payload(base64), packet_id?}` → `{status:"queued", packet_id}` (best-effort). For multi-path redundancy, reuse the returned `packet_id` when sending the same packet to your other connected nodes, so duplicates are deduped end-to-end. |
| POST | `/invite/generate` | Generate a private-network invite (private mode only) |
| POST | `/invite/accept` | Accept an invite; writes PSK + bootstrap, requires restart |
| GET | `/config` | Current config (API token redacted) |
| GET | `/ws` | WebSocket; pass `?peer_id=<app peer id>` to register |

WebSocket events (node → app), each a JSON object with a `type`: `packet_received`, `peer_online`, `peer_offline`, `node_connected`, `node_disconnected`, `error`.

## Running as a service

### Linux (systemd)

A hardened systemd unit template is in `scripts/plural-star-node.service`:

```sh
sudo useradd --system --home /var/lib/plural-star-node plural-star
sudo install -Dm755 plural-star-node /usr/local/bin/plural-star-node
sudo install -Dm644 scripts/plural-star-node.service /etc/systemd/system/plural-star-node.service
sudo mkdir -p /etc/plural-star-node /var/lib/plural-star-node
sudo systemctl enable --now plural-star-node
```

### Windows

Run it at startup with [NSSM](https://nssm.cc/), from an Administrator `cmd`:

```bat
nssm install PluralStarNode "C:\path\to\plural-star-node.exe" --config "C:\path\to\config.yaml"
nssm start PluralStarNode
```

Or create a Task Scheduler task that runs `plural-star-node.exe --config config.yaml` with the trigger "At startup."

## Exposing a public node (port forwarding & dynamic DNS)

A public bootstrap node must be reachable from the internet at a stable address.

**1. Forward the port.** On your router, forward **TCP and UDP 4001** to the node's LAN IP.

**2. Announce your public address.** Set `announce_addrs` in `config.yaml` to your public IP — this is what other nodes use to reach you, and it also stops the node advertising local-only addresses:

```yaml
announce_addrs:
  - "/ip4/<your-public-ip>/tcp/4001"
  - "/ip4/<your-public-ip>/udp/4001/quic-v1"
```

Find your public IPv4 with `curl -4 ifconfig.me` (Windows: `curl.exe -4 ifconfig.me`). If your router's WAN IP differs from that result, your ISP uses CGNAT and port forwarding won't expose you.

**3. Use a hostname so the address survives IP changes.** Most home IPs are dynamic. Register a free dynamic-DNS hostname with [deSEC](https://desec.io) (e.g. `yourname.dedyn.io`) and copy the token shown at sign-up. Point it at your IP once:

```sh
curl --user yourname.dedyn.io:<your-token> "https://update.dedyn.io/?myipv4=<your-ip>&myipv6=preserve"
```

Keep it updated automatically every 10 minutes — Linux/macOS (cron):

```sh
*/10 * * * * curl -4 --user yourname.dedyn.io:<your-token> "https://update.dedyn.io/?myipv6=preserve"
```

Windows (Task Scheduler):

```bat
schtasks /create /tn "deSEC DDNS" /tr "curl.exe -4 --user yourname.dedyn.io:<your-token> \"https://update.dedyn.io/?myipv6=preserve\"" /sc minute /mo 10 /f
```

Then announce the hostname instead of the raw IP:

```yaml
announce_addrs:
  - "/dns4/yourname.dedyn.io/tcp/4001"
  - "/dns4/yourname.dedyn.io/udp/4001/quic-v1"
```

Restart the node. Your stable bootstrap address — for other nodes' `bootstrap_peers`, a directory card, or `DefaultBootstrapPeers` — is then `/dns4/yourname.dedyn.io/tcp/4001/p2p/<your-peer-id>`.

## Hosting the public directory

The public-network directory is a static JSON file — an array of signed network cards — so it can be hosted anywhere static, including **GitHub Pages**. Nodes fetch it on startup (set `directory_url` in config), verify each card's signature, cache valid ones, and serve them on `/networks`. Networks also propagate node-to-node over gossip.

Create and sign a card with the bundled tool (signs with an Ed25519 key; `created_by` is that key's peer ID):

```sh
go run ./cmd/gencard -key ./directory.key -id plural-star-global -name "Plural Star Global" -desc "The main open network" -bootstrap "/ip4/<ip>/tcp/4001/p2p/<peerid>" > card.json
```

Assemble the directory as a JSON array and publish it (e.g. commit `directory.json` to a `gh-pages` branch), then point nodes at it:

```yaml
directory_url: "https://<user>.github.io/<repo>/directory.json"
```

Keep the signing key (`directory.key`) safe — it is the authority for any card published under its `created_by` identity.

## Architecture

```
cmd/node            entry point, subsystem wiring, signal handling
internal/config     config load/validate + first-run generation
internal/host       go-libp2p host, identity, DHT
internal/network    network modes, mDNS + DHT discovery, invites,
                    signed network cards, bbolt cache
internal/relay      packet, dedup cache, routing table, presence gossip,
                    /plural-star/relay/1.0.0 stream handler
internal/ping       connected-node tracking + RTT (libp2p ping)
internal/api        REST + WebSocket server, bearer auth
```

## Security model

Relay nodes are untrusted. They route on `RecipientID` and never inspect `Payload`. End-to-end encryption is the app's responsibility — the node treats payloads as opaque bytes. Node-to-node transport is encrypted by libp2p (Noise). The local API is bound to `127.0.0.1`; if you expose it remotely you are responsible for TLS via a reverse proxy.

## Privacy & data

The node is relay-only and **stores no personal data**. It handles:

- **libp2p peer IDs** — of other nodes and of connected app clients, kept in memory (routing table, presence) and never written to disk. A peer ID is a random public-key identifier, not account or contact information.
- **Encrypted payloads** — passed through opaquely; never inspected, logged, or stored. End-to-end encryption is performed by the app.

The only files a node writes are operational and contain no user data: its own keypair (`node.key`), config (`config.yaml`), an optional private-network PSK (`network.psk`), and a cache of public *network cards* (`networks.db`). The local API binds to `127.0.0.1` and is gated by a bearer token.

Because the node neither collects nor stores personal data, it does not by itself require a formal privacy policy. Operators who run **public bootstrap nodes** may still want to publish a short transparency statement (what their node logs, retention, jurisdiction). A full privacy policy belongs with the **Plural Star app**, which handles user content and is subject to app-store requirements.
