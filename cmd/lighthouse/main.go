// Command lighthouse is the rendezvous / signaling server for the quickmesh
// POC. It speaks a tiny JSON API over HTTP/3 (i.e. HTTP over QUIC over UDP).
//
// Why HTTP/3 specifically? Two reasons, one practical and one pedagogical:
//
//   - Practical: the clients ALREADY have a QUIC/UDP socket open (they use it
//     to talk to each other). Talking to the lighthouse over that SAME socket
//     means the public address the lighthouse observes is the EXACT same NAT
//     mapping the peers will later punch through. If the client used TCP/HTTP
//     for signaling and UDP for peers, those would be two different NAT
//     mappings and the address we hand out would be useless.
//
//   - Pedagogical: it's a neat demonstration that "HTTP/3" is just HTTP framing
//     riding on the same QUIC machinery we use for the chat.
//
// The lighthouse stores everything in memory. Restarting it forgets all nodes;
// that's fine, clients re-register every few seconds.
//
// It must run on a directly reachable public UDP port so that it observes each
// client's real public address. (A QUIC-terminating reverse proxy in front
// would make us observe the proxy's address instead, which breaks the address
// reflection that hole punching depends on.)
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pborges/quicmeshpoc/internal/logutil"
	"github.com/pborges/quicmeshpoc/internal/quicutil"
	"github.com/pborges/quicmeshpoc/internal/signal"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

// peerTTL is how long a registration stays valid without a refresh. Clients
// re-register well within this window; anything older is treated as gone.
const peerTTL = 30 * time.Second

// registration is what we remember about one node.
type registration struct {
	addr       string    // public UDP ip:port we observed
	localAddrs []string  // LAN candidates the client reported (we can't observe these)
	lastSeen   time.Time // updated on every /register
}

// lighthouse is the whole server state: who is registered, what punch requests
// are queued for each node, and the data behind the live web dashboard. One big
// mutex protects it all — simplest possible thing that is correct.
type lighthouse struct {
	mu      sync.Mutex
	nodes   map[string]registration    // nodeID -> registration
	signals map[string][]signal.Signal // nodeID -> queued punch requests for it

	// Dashboard state (see dashboard.go):
	handoffs []*handoff               // recent handoff attempts, newest last
	events   []logEntry               // recent human-readable log, newest last
	subs     map[chan string]struct{} // connected SSE clients (each gets HTML snapshots)
}

func newLighthouse() *lighthouse {
	return &lighthouse{
		nodes:   make(map[string]registration),
		signals: make(map[string][]signal.Signal),
		subs:    make(map[chan string]struct{}),
	}
}

// remoteAddrKey is the context key under which we stash the observed UDP source
// address of the current request. We can't read it from the HTTP request fields
// reliably, so we capture it at the QUIC-connection level via Server.ConnContext
// (see main) and thread it through the request context.
type remoteAddrKey struct{}

// observedAddr pulls the address we captured for this request.
func observedAddr(r *http.Request) string {
	if a, ok := r.Context().Value(remoteAddrKey{}).(net.Addr); ok {
		return a.String()
	}
	return r.RemoteAddr // fallback
}

// handleRegister records "nodeID lives at <the address these packets came
// from>" and tells the client what address we saw.
func (l *lighthouse) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req signal.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.NodeID == "" {
		http.Error(w, "bad register request", http.StatusBadRequest)
		return
	}

	addr := observedAddr(r)

	l.mu.Lock()
	_, existed := l.nodes[req.NodeID]
	l.nodes[req.NodeID] = registration{addr: addr, localAddrs: req.LocalAddrs, lastSeen: time.Now()}
	l.mu.Unlock()

	// Only log the FIRST time we see a node (registrations repeat every ~10s as
	// a keepalive; we don't want to flood the dashboard log with those).
	if !existed {
		l.addEvent("online", fmt.Sprintf("%s came online (public addr %s)", req.NodeID, addr))
	} else {
		l.broadcast() // refresh "last seen" without a new log line
	}

	slog.Info("register", "node", req.NodeID, "addr", addr)
	writeJSON(w, signal.RegisterResponse{YourAddr: addr})
}

// handlePeers returns every fresh node EXCEPT the caller (identified by the
// ?self= query param so a client doesn't list itself).
func (l *lighthouse) handlePeers(w http.ResponseWriter, r *http.Request) {
	self := r.URL.Query().Get("self")

	l.mu.Lock()
	now := time.Now()
	var expired []string
	resp := signal.PeersResponse{}
	for id, reg := range l.nodes {
		if now.Sub(reg.lastSeen) > peerTTL {
			delete(l.nodes, id) // garbage-collect stale registrations lazily
			expired = append(expired, id)
			continue
		}
		if id == self {
			continue
		}
		resp.Peers = append(resp.Peers, signal.Peer{NodeID: id, Addr: reg.addr, LocalAddrs: reg.localAddrs})
	}
	l.mu.Unlock()

	// addEvent locks internally, so emit offline notices after releasing the lock.
	for _, id := range expired {
		l.addEvent("offline", fmt.Sprintf("%s went offline (registration expired)", id))
	}
	writeJSON(w, resp)
}

// handleConnect queues a punch request: "From wants to talk to To". We attach
// From's currently-known public address so that To knows where to aim its punch
// packets.
func (l *lighthouse) handleConnect(w http.ResponseWriter, r *http.Request) {
	var req signal.ConnectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.From == "" || req.To == "" {
		http.Error(w, "bad connect request", http.StatusBadRequest)
		return
	}

	l.mu.Lock()
	fromReg, ok := l.nodes[req.From]
	if ok {
		l.signals[req.To] = append(l.signals[req.To], signal.Signal{
			FromNodeID:     req.From,
			FromAddr:       fromReg.addr,
			FromLocalAddrs: fromReg.localAddrs,
		})
		// Record the handoff for the dashboard. Reuse the existing row for this
		// from→to pair if one exists (so retries don't pile up duplicate rows —
		// one row per pair); otherwise add a new one. Either way we (re-)arm it to
		// a fresh "waiting" state, which also resets its timed-out window.
		h := l.findHandoff(req.From, req.To)
		if h == nil {
			h = &handoff{from: req.From, to: req.To}
			l.handoffs = appendCapped(l.handoffs, h, maxHandoffs)
		}
		h.started = time.Now()
		h.pickedUp = false
		h.completed = false
		h.completedAt = time.Time{}
	}
	l.mu.Unlock()

	if !ok {
		http.Error(w, "unknown 'from' node", http.StatusBadRequest)
		return
	}
	l.addEvent("handoff-requested", fmt.Sprintf("%s wants to reach %s — waiting for handoff", req.From, req.To))
	slog.Info("connect requested", "from", req.From, "to", req.To)
	w.WriteHeader(http.StatusNoContent)
}

// handleSignals drains and returns the punch requests queued for the polling
// node. Draining (delete after read) keeps it simple: each signal is delivered
// once.
func (l *lighthouse) handleSignals(w http.ResponseWriter, r *http.Request) {
	self := r.URL.Query().Get("self")

	l.mu.Lock()
	pending := l.signals[self]
	delete(l.signals, self)
	// Mark any matching pending handoffs as "picked up": the responder has now
	// learned it should punch back, so the punch is in progress.
	var pickedUp []string
	for _, s := range pending {
		for _, h := range l.handoffs {
			if h.from == s.FromNodeID && h.to == self && !h.pickedUp && !h.completed {
				h.pickedUp = true
				pickedUp = append(pickedUp, s.FromNodeID)
			}
		}
	}
	l.mu.Unlock()

	for _, from := range pickedUp {
		l.addEvent("punching", fmt.Sprintf("%s picked up punch request from %s — punching…", self, from))
	}
	writeJSON(w, signal.SignalsResponse{Signals: pending})
}

// handleHandoff is reported by the INITIATOR once a peer connection is fully
// established. The lighthouse can't observe the P2P handshake itself (that's the
// whole point — it's direct), so the client tells it, purely so the dashboard
// can show "handed off ✔".
func (l *lighthouse) handleHandoff(w http.ResponseWriter, r *http.Request) {
	var req signal.ConnectRequest // reuse {from,to}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.From == "" || req.To == "" {
		http.Error(w, "bad handoff report", http.StatusBadRequest)
		return
	}

	l.mu.Lock()
	// Complete the most recent matching handoff.
	for i := len(l.handoffs) - 1; i >= 0; i-- {
		h := l.handoffs[i]
		if h.from == req.From && h.to == req.To && !h.completed {
			h.completed = true
			h.completedAt = time.Now()
			break
		}
	}
	l.mu.Unlock()

	l.addEvent("handed-off", fmt.Sprintf("%s ↔ %s connected directly — handed off ✔", req.From, req.To))
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// letsEncryptStaging is the ACME directory for Let's Encrypt's staging
// environment. Staging has no rate limits but issues certificates from an
// untrusted root, so use it while testing your setup, then drop -acme-staging.
const letsEncryptStaging = "https://acme-staging-v02.api.letsencrypt.org/directory"

// serverTLSConfig builds the TLS config for the HTTP/3 server in one of three
// modes, in priority order:
//
//  1. Automatic Let's Encrypt (when -domain is set). The lighthouse obtains and
//     renews its own certificate via the ACME "HTTP-01" challenge. Requirements:
//     - DNS for <domain> points at this machine.
//     - TCP port 80 is reachable from the internet (that's where the ACME
//     challenge is answered — see the goroutine below).
//     - UDP port 443 (your -addr) is reachable for the actual HTTP/3 traffic.
//     The obtained certificate is a normal cert and is served over QUIC just
//     fine; only the one-time challenge needs that TCP/80 listener.
//
//  2. A provided certificate (when -cert/-key are set), e.g. one you obtained
//     out of band (certbot, DNS-01, an internal CA, ...).
//
//  3. A throwaway self-signed cert (default) for local hacking. Clients must
//     run with -insecure to accept it.
func serverTLSConfig(domain, email, cacheDir, http01Addr string, staging bool, certFile, keyFile string) *tls.Config {
	tlsConf := quicutil.GenerateTLSConfig("h3")

	switch {
	case domain != "":
		// Mode 1: automatic Let's Encrypt.
		m := &autocert.Manager{
			Prompt:     autocert.AcceptTOS,             // agree to the Let's Encrypt TOS
			Cache:      autocert.DirCache(cacheDir),    // persist certs so we don't re-issue on restart
			HostPolicy: autocert.HostWhitelist(domain), // only issue for our domain
			Email:      email,                          // account contact (renewal warnings)
		}
		if staging {
			m.Client = &acme.Client{DirectoryURL: letsEncryptStaging}
		}

		// The ACME HTTP-01 challenge is answered over plain TCP on port 80.
		// QUIC can't serve the challenge itself (Let's Encrypt validates over
		// TCP), so we run this tiny side server. It also redirects any normal
		// http:// visitor to https://.
		go func() {
			slog.Info("ACME HTTP-01 challenge listener (TCP)", "addr", http01Addr)
			if err := http.ListenAndServe(http01Addr, m.HTTPHandler(nil)); err != nil {
				slog.Error("http-01 listener failed", "err", err)
				os.Exit(1)
			}
		}()

		// GetCertificate makes the QUIC server fetch/renew the cert on demand.
		// Note: we clear Certificates so only GetCertificate is used.
		tlsConf.Certificates = nil
		tlsConf.GetCertificate = m.GetCertificate
		slog.Info("automatic Let's Encrypt enabled", "domain", domain, "staging", staging)

	case certFile != "":
		// Mode 2: a certificate you supply.
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			slog.Error("load cert/key failed", "err", err)
			os.Exit(1)
		}
		tlsConf.Certificates = []tls.Certificate{cert}
		slog.Info("serving provided certificate", "cert", certFile)

	default:
		// Mode 3: self-signed (clients need -insecure).
		slog.Info("serving a self-signed certificate (clients must use -insecure)")
	}

	return tlsConf
}

func main() {
	addr := flag.String("addr", ":4433", "UDP address to serve HTTP/3 on")
	certFile := flag.String("cert", "", "TLS certificate (PEM); if empty, a self-signed cert is generated")
	keyFile := flag.String("key", "", "TLS private key (PEM); required when -cert is set")
	// Let's Encrypt (autocert) flags. Set -domain to enable automatic certs.
	domain := flag.String("domain", "", "public domain name; enables automatic Let's Encrypt certificates")
	email := flag.String("email", "", "contact email for the ACME (Let's Encrypt) account")
	acmeCache := flag.String("acme-cache", "./acme-cache", "directory to cache Let's Encrypt certificates")
	acmeStaging := flag.Bool("acme-staging", false, "use the Let's Encrypt staging environment (no rate limits, untrusted certs)")
	http01Addr := flag.String("http01-addr", ":80", "TCP address for the ACME HTTP-01 challenge (must be reachable on port 80)")
	webAddr := flag.String("web", ":8080", "TCP address for the human-facing web dashboard (plain HTTP)")
	webTLSAddr := flag.String("web-tls", "", "TCP address for the HTTPS web dashboard (reuses the -domain/-cert certificate); empty to disable")
	flag.Parse()

	// Log to logs/lighthouse.log, teed to stderr (the lighthouse isn't a TUI, so
	// seeing logs on the console is handy). slog.SetDefault makes the package-level
	// slog.Info/Error calls throughout this file use it.
	logger, closeLog, err := logutil.New("lighthouse", slog.LevelInfo, true)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to open log:", err)
		os.Exit(1)
	}
	defer closeLog()
	slog.SetDefault(logger)

	lh := newLighthouse()

	mux := http.NewServeMux()
	// Signaling API (clients reach these over HTTP/3).
	mux.HandleFunc("POST /register", lh.handleRegister)
	mux.HandleFunc("GET /peers", lh.handlePeers)
	mux.HandleFunc("POST /connect", lh.handleConnect)
	mux.HandleFunc("GET /signals", lh.handleSignals)
	mux.HandleFunc("POST /handoff", lh.handleHandoff)
	// Human dashboard (browsers reach these over plain TCP HTTP — see below).
	mux.HandleFunc("GET /", lh.handleIndex)
	mux.HandleFunc("GET /events/stream", lh.handleStream)

	// Pick a TLS config (see serverTLSConfig for the three modes).
	tlsConf := serverTLSConfig(*domain, *email, *acmeCache, *http01Addr, *acmeStaging, *certFile, *keyFile)

	// The HTTP/3 server.
	server := &http3.Server{
		Addr:      *addr,
		Handler:   mux,
		TLSConfig: tlsConf,

		// THE IMPORTANT BIT: ConnContext runs once per QUIC connection and lets
		// us capture the client's real UDP source address. We stuff it into the
		// context so every handler for that connection can read it. This is our
		// home-grown STUN: "I'll tell you what address I see you coming from."
		ConnContext: func(ctx context.Context, c *quic.Conn) context.Context {
			return context.WithValue(ctx, remoteAddrKey{}, c.RemoteAddr())
		},
	}

	// The human dashboard runs over TCP, because browsers can't load an
	// HTTP/3-only (UDP) site directly — they must first reach the site over TCP,
	// then learn about h3 via an Alt-Svc header. We serve the same mux on TCP for
	// humans. It's informational only (this is a public-log POC); put it behind
	// your own auth/proxy if you expose it.
	//
	// Two TCP variants:
	//
	//   - Plain HTTP (-web, default :8080). Handy for curl/internal use. NOTE:
	//     this is useless in a browser for an HSTS-preloaded TLD like ".dev",
	//     because the browser force-upgrades http:// to https:// and there's no
	//     TLS on this port. Use -web-tls for a browsable URL on such domains.
	//
	//   - HTTPS (-web-tls, e.g. :443). Reuses the same certificate as the HTTP/3
	//     server (autocert in -domain mode, or -cert/-key). TCP :443 coexists
	//     fine with the UDP :443 QUIC listener — different transport, same number.
	//     We also stamp an Alt-Svc header so browsers can discover and upgrade to
	//     the HTTP/3 endpoint.
	go func() {
		slog.Info("web dashboard (plain HTTP)", "addr", *webAddr)
		if err := http.ListenAndServe(*webAddr, mux); err != nil {
			slog.Error("web dashboard stopped", "err", err)
			os.Exit(1)
		}
	}()

	if *webTLSAddr != "" {
		// Advertise the HTTP/3 endpoint so browsers can upgrade to QUIC. The
		// port is taken from -addr (the UDP listener).
		altSvc := "h3=\"" + *addr + "\"; ma=86400"
		dashHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Alt-Svc", altSvc)
			mux.ServeHTTP(w, r)
		})
		// Reuse the server cert (GetCertificate/Certificates already set by
		// serverTLSConfig); only the ALPN list differs for TCP.
		tcpTLS := tlsConf.Clone()
		tcpTLS.NextProtos = []string{"h2", "http/1.1"}
		tlsSrv := &http.Server{Addr: *webTLSAddr, Handler: dashHandler, TLSConfig: tcpTLS}
		go func() {
			slog.Info("web dashboard (HTTPS/TCP)", "addr", *webTLSAddr)
			if err := tlsSrv.ListenAndServeTLS("", ""); err != nil {
				slog.Error("https dashboard stopped", "err", err)
				os.Exit(1)
			}
		}()
	}

	slog.Info("lighthouse signaling API (HTTP/3 / QUIC / UDP)", "addr", *addr)
	if err := server.ListenAndServe(); err != nil {
		slog.Error("lighthouse stopped", "err", err)
		os.Exit(1)
	}
}
