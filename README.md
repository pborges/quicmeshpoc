# quickMeshPOC — learn QUIC + NAT hole punching

A deliberately tiny, heavily-commented proof of concept for understanding how two
peers behind home routers (NATs) establish a **direct** peer-to-peer connection,
with a central **lighthouse** only there to introduce them. Once introduced,
peers form a **self-healing full mesh** and chat directly — the lighthouse never
relays a single byte of chat traffic.

Everything runs on QUIC (UDP). There are two programs:

| program          | what it is                                                                                                                                        |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `cmd/lighthouse` | the rendezvous / signaling server. Speaks a tiny JSON API over **HTTP/3**, plus a live **web dashboard** (htmx + SSE). Never relays chat traffic. |
| `cmd/quicchat`   | the peer. A **charm/Bubble Tea TUI** chat client. Every peer is identical.                                                                        |

> Read the code top-to-bottom — it's written to be read. Start with
> `cmd/quicchat/mesh.go` (the hole punch + mesh) and `cmd/lighthouse/main.go`
> (the signaling).

---

## The big idea in 60 seconds

A home NAT drops unsolicited inbound UDP. But the moment *you* send a packet
**out** to some `ip:port`, your NAT opens a temporary return path for packets
coming back from that `ip:port`. Hole punching = **both peers send to each other
at the same time**, so both NATs open their holes and traffic flows directly.

The lighthouse does three things, nothing more:

1. **Observe** each peer's *public* UDP address (the peer can't know its own).
2. **List** who's online so peers can find each other.
3. **Introduce**: when A wants B, tell B to start punching back toward A.

### Three tricks that make it work

- **One socket for everything.** Each `quicchat` opens a *single* UDP socket
  (`quic.Transport`) and uses it for HTTP/3 to the lighthouse **and** for dialing
  **and** accepting peers. So the NAT mapping the lighthouse sees is the *same*
  one peers punch through. (See `mesh.go`.)
- **Address reflection (home-grown STUN).** The lighthouse reads the UDP source
  address of your HTTP/3 connection (`http3.Server.ConnContext`) and tells you
  what it saw. That's the address it hands to your peers.
- **Host candidates (a sliver of ICE).** Each peer *also* advertises its own LAN
  addresses. When connecting, a peer races **every** candidate — public + LAN —
  in parallel and keeps the first handshake that completes. Two peers on the same
  network connect directly over the LAN instead of relying on NAT *hairpinning*
  (bouncing off the shared public IP), which many home routers don't support.

### The handoff, step by step

```
  alice                         lighthouse                 bob
  | --POST /register------------> |  sees alice@1.2.3.4:5678 (+ LAN)
  |                               | <--------- POST /register  (sees bob@9.8.7.6:4321)
  | --GET /peers----------------> |  returns [bob @ 9.8.7.6:4321 (+ LAN)]
  | --POST /connect{from,to}----> |  queues a punch-back for bob
  | --dial QUIC to bob----------> X  (bob's NAT drops it — no hole yet)
  |                               |  bob holds an open GET /signals/stream (SSE)
  |                               | ===push===> "punch toward alice (public + LAN)"
  |                               |   bob fires punch packets at alice -->
  | <===== QUIC handshake completes through both holes — alice <-> bob =====>
  | --POST /handoff-------------> |  dashboard shows "handed off ✔"
  | <===== direct peer-to-peer chat over a QUIC stream — alice <-> bob =====>
```

`alice` is the **initiator** (dials). `bob` is the **responder** (fires throwaway
"punch" packets to open his NAT, then `Accept`s alice's connection). The first
chat-stream line is `HELLO <nodeid>` so the responder learns who connected.

---

## The full mesh (auto-connect)

You don't have to wire peers together by hand. Each `quicchat` continuously dials
every peer it discovers and isn't already linked to, so the graph fills in to a
**complete mesh** — a broadcast from *any* node reaches everyone directly.

- **Glare avoidance.** For each pair, only **one** side initiates (the node with
  the lexicographically smaller id); the other just accepts the inbound. So every
  pair gets exactly one connection — no duplicate/"glare" links.
- **Idempotent + self-healing.** Dials are de-duplicated while in flight, and a
  dropped link is automatically re-dialed on the next poll (~3s). Bring a node
  back and it rejoins the mesh on its own.

> A full mesh is N² connections — perfect for a handful of nodes (this POC), not
> for hundreds. That's a deliberate simplification.

### The 📢 broadcast pseudo-peer

The top sidebar entry is **broadcast** — a fake "peer" that represents everyone:

- Sending a message while it's selected fans the message out to **every connected
  peer** (over each peer's existing direct QUIC stream — the lighthouse is not
  involved).
- Its chat view aggregates **incoming messages from all peers**, so it reads like
  a group room. The `(N)` next to it is the connected-peer count.
- Selecting it and pressing **Enter** does a one-shot "connect all" (rarely needed
  now that the mesh forms itself).

It's purely a client-side convenience: there's no "broadcast" on the wire, just
the same 1:1 peer streams written to in a loop.

---

## How fast does a peer notice another is gone?

| how the peer leaves                         | detected in                                               |
|---------------------------------------------|-----------------------------------------------------------|
| clean quit (`q` / ctrl-c / `kill`)          | **< 1s** — it sends a QUIC `CONNECTION_CLOSE` on shutdown |
| silent death (`kill -9`, Wi-Fi drop, sleep) | **~20s** — QUIC `MaxIdleTimeout` (keepalive every 7s)     |
| drops off the sidebar list                  | **~30s** — lighthouse `peerTTL`, polled every 3s          |

A dropped link is then re-dialed automatically, so transient blips self-recover.

---

## Run it locally (one machine)

```bash
# terminal 1 — lighthouse (self-signed cert; HTTP/3 on :4433, dashboard on :8080)
go run ./cmd/lighthouse

# terminal 2 & 3 — peers (give each a UNIQUE name)
go run ./cmd/quicchat alice
go run ./cmd/quicchat bob
```

In a peer: **Tab** switches panels, **↑/↓** pick a peer, **Enter** connects (or
just wait — the mesh auto-connects), then Tab to the input box and chat. Open
<http://localhost:8080> to watch the live dashboard. (Loopback has no NAT, so this
exercises the full code path but not a *real* punch — for that, see below.)

> ⚠️ **One process per name.** Each `quicchat` name must be unique and run once.
> Two processes sharing a name both register under it, so the lighthouse's record
> ping-pongs between their two sockets and connections won't form reliably.

## Run it on your LAN (real two-machine test)

Let's Encrypt **cannot** issue certs for a private LAN (no public DNS / port 80),
so on a LAN you use the default **self-signed** lighthouse and let clients skip
verification (`-insecure`, the default).

```bash
# on the "server" box, find its LAN IP (e.g. 192.168.1.50)
go run ./cmd/lighthouse                       # binds 0.0.0.0:4433 (UDP) + :8080 (web)

# on each other box on the same Wi-Fi/LAN:
go run ./cmd/quicchat alice -lighthouse https://192.168.1.50:4433
go run ./cmd/quicchat bob   -lighthouse https://192.168.1.50:4433
```

Same-LAN peers connect over their **host candidates** (the `192.168.x` addresses),
so this works even if your router can't hairpin.

## Run it on a public VPS (real NAT punch)

To see hole punching against **real, different NATs**, run the lighthouse on a
cheap public box (directly reachable, no proxy) and run the two peers from two
*different* home networks:

```bash
# on the VPS — automatic Let's Encrypt, HTTP/3 on UDP :443, browsable dashboard on TCP :443
sudo ./lighthouse \
  -domain mesh.example.com -email you@example.com \
  -addr :443 -http01-addr :80 -web-tls :443

# on each peer's home network (verify the real cert):
go run ./cmd/quicchat alice -lighthouse https://mesh.example.com -insecure=false
go run ./cmd/quicchat bob   -lighthouse https://mesh.example.com -insecure=false
```

> ℹ️ Two peers on the **same** network is *not* a real test: they'll connect over
> the LAN candidate, and the public path needs NAT hairpinning. Use two genuinely
> separate networks to exercise an actual hole punch.

A `systemd` unit makes a good long-running deployment (set `AmbientCapabilities=
CAP_NET_BIND_SERVICE` so it can bind 80/443 unprivileged, and point
`-acme-cache` at a writable `StateDirectory`).

---

## TLS / certificate options (lighthouse)

QUIC is always encrypted, so the lighthouse always needs a cert. Pick a mode:

| mode                        | flags                                             | when                                                            |
|-----------------------------|---------------------------------------------------|-----------------------------------------------------------------|
| **self-signed** (default)   | *(none)*                                          | local / LAN testing. Clients use `-insecure` (default `true`).  |
| **provide your own**        | `-cert fullchain.pem -key privkey.pem`            | you got a cert elsewhere (certbot DNS-01, internal CA, …).      |
| **automatic Let's Encrypt** | `-domain mesh.example.com -email you@example.com` | public deployment. The lighthouse issues & renews its own cert. |

Automatic issuance uses the ACME **HTTP-01** challenge, which needs:

- DNS for `mesh.example.com` pointing at the box.
- **TCP** port 80 reachable from the internet (QUIC/UDP can't answer the challenge,
  so the lighthouse runs a tiny TCP listener just for it).
- **UDP** port 443 (your `-addr`) reachable for the actual HTTP/3 traffic.

Add `-acme-staging` first to avoid rate limits while you test the setup.

### Two dashboards: plain HTTP vs HTTPS

The web dashboard is served over TCP (browsers can't load an HTTP/3-only site
directly):

- `-web :8080` — plain HTTP. Handy for `curl`/internal use.
- `-web-tls :443` — HTTPS, reusing the same cert, with an `Alt-Svc` header that
  advertises the HTTP/3 endpoint. **Required to view the dashboard in a browser on
  an HSTS-preloaded TLD** (e.g. any `.dev` domain), where browsers force `https://`.
  TCP :443 coexists fine with the UDP :443 QUIC listener — same number, different
  transport.

---

## The dashboard

`GET /` is a live, auto-updating board (htmx swaps in HTML snapshots pushed over
SSE — no page reload, no hand-written JS). It shows:

- **🟢 Online nodes** — who's registered, their public address, and last-seen age.
- **🤝 Handoffs** — one row per `from→to` pair, moving through its lifecycle in
  place: `waiting…` → `punching…` → `handed off ✔`. A row that never completes
  within 30s turns red `timed out ✕`; a retry re-arms it back to `waiting…`.
- **📜 Activity log** — a human-readable stream of online/offline/punch events.

All board state is in-memory, so restarting the lighthouse simply clears it.

---

## "Can it run behind a reverse proxy?"

Short answer: **not a normal QUIC-terminating one.** The whole scheme depends on
the lighthouse seeing each client's *real* public UDP address. A reverse proxy
that terminates QUIC (e.g. a standard Traefik HTTP/3 setup) makes the lighthouse
observe the **proxy's** address, so the addresses it hands out are useless and
punching fails. Your realistic options:

1. **Directly reachable (recommended for this POC).** Run the lighthouse on a
   public UDP port with no proxy. Use the built-in Let's Encrypt mode above for
   TLS. This is what Nebula/Tailscale-style lighthouses do.
2. **Pure L4 UDP port-forward (DNAT), not a proxy.** If your edge box just
   `iptables`-forwards UDP/443 to the lighthouse's internal IP, the client's
   source address is *preserved* (it's NAT, not proxying), so everything works.
   This is the easiest way to put it "behind" something.
3. **Split address-reflection from the API (the "proper" way).** Keep a normal
   reverse proxy in front of the `/register`, `/peers`, `/connect`,
   `/signals/stream` API, **and** add a tiny directly-reachable STUN-style
   reflector that tells
   each client its public address. This is exactly how WebRTC/Tailscale separate
   STUN servers from the control plane.

---

## Logs & debugging

Every process writes its own log file under `./logs/` (created automatically, and
git-ignored):

| process          | file                                          |
|------------------|-----------------------------------------------|
| `lighthouse`     | `logs/lighthouse.log` (also echoed to stderr) |
| `quicchat alice` | `logs/alice.log`                              |
| `quicchat bob`   | `logs/bob.log`                                |

The clients log to a **file** (not the screen) on purpose: they're full-screen
TUIs, so stray stdout/stderr would corrupt the display. Tail a node's log in a
second terminal while you use the app:

```bash
tail -f logs/alice.log
```

Logging uses `slog` (text handler). Clients log at debug level (peer state
changes, dial/punch/accept, stream errors); the lighthouse at info. Every
background goroutine has a panic guard (`logutil.Recover`) that logs the panic +
stack instead of taking down the process — so a single bad peer can't freeze the
UI or wedge your terminal.

> Note on flags: the node id is positional and flags work in any position, e.g.
> both `quicchat alice -lighthouse https://host:4433` and
> `quicchat -lighthouse https://host:4433 alice` work.

---

## Flag reference

**lighthouse**

| flag             | default        | meaning                                               |
|------------------|----------------|-------------------------------------------------------|
| `-addr`          | `:4433`        | UDP address for the HTTP/3 signaling API              |
| `-web`           | `:8080`        | TCP address for the plain-HTTP dashboard              |
| `-web-tls`       | *(off)*        | TCP address for the HTTPS dashboard (reuses the cert) |
| `-domain`        | *(off)*        | enable automatic Let's Encrypt for this domain        |
| `-email`         |                | ACME account contact email                            |
| `-acme-cache`    | `./acme-cache` | directory to cache issued certs                       |
| `-acme-staging`  | `false`        | use Let's Encrypt staging (no rate limits)            |
| `-http01-addr`   | `:80`          | TCP address for the ACME HTTP-01 challenge            |
| `-cert` / `-key` |                | serve a certificate you supply instead                |

**quicchat**

| arg / flag              | default                  | meaning                                                                     |
|-------------------------|--------------------------|-----------------------------------------------------------------------------|
| `<nodeid>` (positional) | *(required)*             | your unique peer name                                                       |
| `-lighthouse`           | `https://127.0.0.1:4433` | lighthouse base URL                                                         |
| `-insecure`             | `true`                   | skip TLS verification of the lighthouse cert (set `=false` for a real cert) |

---

## Layout

```
cmd/lighthouse/main.go       signaling API, TLS modes, address reflection
cmd/lighthouse/dashboard.go  htmx + SSE live web dashboard
cmd/quicchat/main.go         CLI wiring + graceful shutdown
cmd/quicchat/mesh.go         the QUIC transport, hole punch, and auto-mesh  ← start here
cmd/quicchat/tui.go          the Bubble Tea UI
internal/signal/             the JSON wire protocol (shared types)
internal/quicutil/           tiny TLS helpers
internal/logutil/            per-process slog file logging + panic recovery
cmd/quicchat/integration_test.go  end-to-end test (lighthouse + 2 peers + chat + dashboard)
```

Run the tests: `go test ./...`
