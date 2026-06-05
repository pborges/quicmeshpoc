package main

// mesh.go is the networking heart of the client. This is where the actual NAT
// hole punching lives. Read this file top-to-bottom to understand the dance.
//
// ── The single-socket trick (most important idea in the whole POC) ──────────
// We open exactly ONE UDP socket and wrap it in a quic.Transport. That one
// transport is used for THREE things at once:
//
//   1. HTTP/3 requests to the lighthouse        (outbound, ALPN "h3")
//   2. Dialing a peer to start a chat            (outbound, ALPN quickmesh-chat)
//   3. Accepting a peer dialing us               (inbound,  ALPN quickmesh-chat)
//
// Because all three share one socket, they all share ONE NAT mapping (one
// public ip:port as far as the internet is concerned). So the public address
// the lighthouse observes when we register is the very same address a peer can
// punch through to. If we used separate sockets this would all fall apart.
//
// ── What "hole punching" actually is ───────────────────────────────────────
// A home NAT blocks unsolicited inbound UDP. But once YOU send a packet OUT to
// some ip:port, the NAT creates a temporary mapping and will let return packets
// from that ip:port back in. Hole punching = both peers send packets toward
// each other at roughly the same time, so both NATs open their holes, and then
// traffic flows. The lighthouse is only there to introduce the two peers and
// trigger them to start sending simultaneously.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pborges/quicmeshpoc/internal/logutil"
	"github.com/pborges/quicmeshpoc/internal/quicutil"
	"github.com/pborges/quicmeshpoc/internal/signal"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// connState is the per-peer connection status shown as a coloured dot in the
// sidebar.
type connState int

const (
	stateDisconnected connState = iota
	stateConnecting             // we are dialing / punching
	stateConnected              // QUIC handshake done, chat stream open
	stateFailed
)

func (s connState) String() string {
	switch s {
	case stateConnecting:
		return "connecting"
	case stateConnected:
		return "connected"
	case stateFailed:
		return "failed"
	default:
		return "disconnected"
	}
}

// peerConn holds an established chat connection to one peer.
type peerConn struct {
	nodeID  string
	conn    *quic.Conn
	stream  *quic.Stream // single bidirectional stream we read/write chat lines on
	writeMu sync.Mutex   // serializes writes to stream (so concurrent sends don't interleave)
}

// Mesh owns the shared socket and all peer connections. Its goroutines push
// updates to the TUI by calling the events callback (wired to program.Send).
type Mesh struct {
	nodeID      string
	lhURL       string // lighthouse base URL, e.g. https://127.0.0.1:4433
	transport   *quic.Transport
	listener    *quic.Listener
	http3       *http.Client // HTTP/3 client for short request/response calls (10s timeout)
	http3Stream *http.Client // same transport, NO timeout — for the long-lived /signals/stream SSE
	log         *slog.Logger // writes to logs/<nodeid>.log

	// events delivers tea.Msg values to the bubbletea program. Set by main
	// before Start. We store it as a plain func so this file has no TUI deps.
	events func(any)

	mu         sync.Mutex
	conns      map[string]*peerConn // nodeID -> live connection
	connecting map[string]struct{}  // nodeID -> a dial is currently in flight
}

// NewMesh opens the one UDP socket, wraps it in a quic.Transport, and wires up
// an HTTP/3 client that dials over that same transport.
//
// insecure controls verification of the LIGHTHOUSE certificate only: true for a
// self-signed lighthouse (LAN testing), false to verify a real cert. Peer
// connections always skip verification because peers use self-signed certs.
func NewMesh(nodeID, lighthouseURL string, insecure bool, logger *slog.Logger) (*Mesh, error) {
	// The lighthouse's certificate (when verified) is checked against this host
	// name, parsed from the URL.
	lhHost := lighthouseURL
	if u, err := url.Parse(lighthouseURL); err == nil {
		lhHost = u.Hostname()
	}

	// 1. One UDP socket, OS-assigned local port (:0).
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return nil, fmt.Errorf("listen udp: %w", err)
	}

	// 2. The shared transport. From here on we must NOT read/write udpConn
	//    directly except via the transport (transport.WriteTo for punching).
	tr := &quic.Transport{Conn: udpConn}

	m := &Mesh{
		nodeID:     nodeID,
		lhURL:      lighthouseURL,
		transport:  tr,
		log:        logger,
		conns:      make(map[string]*peerConn),
		connecting: make(map[string]struct{}),
		events:     func(any) {}, // no-op until main wires the real one
	}

	// 3. HTTP/3 client whose Dial hook reuses our shared transport. This is the
	//    line that makes signaling traffic share the peer NAT mapping.
	rt := &http3.Transport{
		TLSClientConfig: quicutil.ClientTLSConfig(lhHost, insecure, "h3"),
		Dial: func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
			// addr is "host:port"; resolve to a UDP address and dial it on the
			// shared transport.
			udpAddr, err := net.ResolveUDPAddr("udp", addr)
			if err != nil {
				return nil, err
			}
			return tr.DialEarly(ctx, udpAddr, tlsCfg, cfg)
		},
	}
	m.http3 = &http.Client{Transport: rt, Timeout: 10 * time.Second}
	// A second client over the SAME transport with NO timeout: the signals SSE
	// stream is held open indefinitely, so the 10s timeout above would kill it.
	// Sharing rt means this rides the same QUIC connection / NAT mapping.
	m.http3Stream = &http.Client{Transport: rt}

	// 4. Listen on the SAME transport for inbound peer connections. Peers dial
	//    us with ALPN quickmesh-chat; the lighthouse traffic (ALPN h3) is
	//    outbound only, so there's no ambiguity.
	ln, err := tr.Listen(
		quicutil.GenerateTLSConfig(signal.ALPNPeer),
		&quic.Config{
			// Detect a silently-gone peer (crash, Wi-Fi drop, sleep) in ~20s
			// rather than 60s. KeepAlivePeriod < ½·MaxIdleTimeout keeps a healthy
			// link alive AND keeps the NAT hole open.
			MaxIdleTimeout:  20 * time.Second,
			KeepAlivePeriod: 7 * time.Second,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("listen quic: %w", err)
	}
	m.listener = ln

	return m, nil
}

// SetEvents wires the callback used to push messages into the TUI.
func (m *Mesh) SetEvents(fn func(any)) { m.events = fn }

// Close gracefully tears the mesh down. It sends a QUIC CONNECTION_CLOSE to
// every connected peer so they learn we're gone IMMEDIATELY, instead of each
// waiting out its ~MaxIdleTimeout before noticing the silence. Then it stops the
// listener and closes the shared transport (and its UDP socket). Safe to call
// once on shutdown.
func (m *Mesh) Close() error {
	m.mu.Lock()
	conns := make([]*peerConn, 0, len(m.conns))
	for _, pc := range m.conns {
		conns = append(conns, pc)
	}
	m.conns = make(map[string]*peerConn)
	m.mu.Unlock()

	for _, pc := range conns {
		_ = pc.conn.CloseWithError(0, "peer shutting down")
	}
	if m.listener != nil {
		_ = m.listener.Close()
	}
	return m.transport.Close()
}

// statusf logs a line AND shows it in the TUI's status bar.
func (m *Mesh) statusf(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	m.log.Info(msg)
	m.events(statusMsg{text: msg})
}

// Start launches all the background loops. None of them block.
func (m *Mesh) Start(ctx context.Context) {
	go m.registerLoop(ctx)      // keep ourselves advertised + refresh NAT mapping
	go m.peersLoop(ctx)         // refresh the sidebar list
	go m.signalsStreamLoop(ctx) // react to "someone wants to punch to you"
	go m.acceptLoop(ctx)        // accept inbound peer connections
}

// ─────────────────────────── lighthouse helpers ────────────────────────────

// post sends a JSON POST to the lighthouse and optionally decodes the response.
func (m *Mesh) post(ctx context.Context, path string, body, out any) error {
	buf, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.lhURL+path, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.http3.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", path, resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (m *Mesh) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.lhURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := m.http3.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// registerLoop re-registers every few seconds. Re-registering does double duty:
// it keeps our entry fresh on the lighthouse AND keeps refreshing the NAT
// mapping toward the lighthouse.
func (m *Mesh) registerLoop(ctx context.Context) {
	defer logutil.Recover(m.log, "registerLoop")
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		var resp signal.RegisterResponse
		err := m.post(ctx, "/register", signal.RegisterRequest{NodeID: m.nodeID, LocalAddrs: m.localCandidates()}, &resp)
		if err != nil {
			m.statusf("register error: %v", err)
		} else {
			m.statusf("registered; lighthouse sees me at %s", resp.YourAddr)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// peersLoop refreshes the sidebar list of advertised peers.
func (m *Mesh) peersLoop(ctx context.Context) {
	defer logutil.Recover(m.log, "peersLoop")
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for {
		var resp signal.PeersResponse
		if err := m.get(ctx, "/peers?self="+m.nodeID, &resp); err != nil {
			m.log.Debug("peers poll failed", "err", err)
		} else {
			m.events(peersMsg{peers: resp.Peers})
			m.autoConnect(resp.Peers)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// autoConnect keeps the mesh fully connected: it dials any peer we don't yet
// have a link to, so a broadcast from ANY node reaches everyone directly (not
// just from whoever happened to dial out). Without this the graph is a star
// centered on whoever pressed "connect" and the spokes can't reach each other.
//
// To avoid "glare" — both peers dialing each other at once and ending up with
// two redundant connections — only ONE side of each pair initiates: the node
// with the lexicographically smaller id. The other side simply accepts the
// inbound connection (its NAT hole is opened by the punch-back it already does
// in signalsStreamLoop). Connect itself is idempotent, so calling this every poll is
// cheap and also serves as automatic retry for links that haven't formed yet.
func (m *Mesh) autoConnect(peers []signal.Peer) {
	for _, p := range peers {
		if m.nodeID >= p.NodeID {
			continue // the peer with the smaller id is the initiator
		}
		m.Connect(p) // no-op if already connected or a dial is in flight
	}
}

// signalsStreamLoop holds open an SSE stream to the lighthouse and reacts to
// punch requests aimed at us the instant they're pushed. When another node asks
// the lighthouse to reach us, a Signal arrives here and we play the RESPONDER
// role: fire punch packets toward them so our NAT opens, then let acceptLoop
// pick up the QUIC connection they dial.
//
// The stream can drop (lighthouse restart, idle timeout, transient loss), so
// this loops: (re)connect, consume until it ends, brief backoff, repeat — until
// our context is cancelled on shutdown.
func (m *Mesh) signalsStreamLoop(ctx context.Context) {
	defer logutil.Recover(m.log, "signalsStreamLoop")
	for {
		if err := m.streamSignals(ctx); err != nil && ctx.Err() == nil {
			m.log.Debug("signals stream ended; reconnecting", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second): // backoff before reconnecting
		}
	}
}

// streamSignals opens the /signals/stream SSE endpoint and dispatches each
// "signal" event to respondToPunch. It returns when the stream ends (error,
// EOF, or context cancel) so signalsStreamLoop can reconnect.
func (m *Mesh) streamSignals(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.lhURL+"/signals/stream?self="+m.nodeID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	// Use the no-timeout client: this response body is read for the lifetime of
	// the stream, not a single round-trip.
	resp, err := m.http3Stream.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("/signals/stream: %s", resp.Status)
	}
	m.log.Debug("signals stream connected")

	// Minimal SSE parser: accumulate "event:"/"data:" fields until a blank line
	// dispatches the event. Comment lines (": …", our heartbeat) are ignored.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var event, data string
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "": // blank line — dispatch the accumulated event
			if data != "" && (event == "" || event == "signal") {
				var s signal.Signal
				if err := json.Unmarshal([]byte(data), &s); err != nil {
					m.log.Warn("bad signal event", "data", data, "err", err)
				} else {
					m.respondToPunch(s)
				}
			}
			event, data = "", ""
		case strings.HasPrefix(line, ":"): // comment / heartbeat
			continue
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(line[len("event:"):])
		case strings.HasPrefix(line, "data:"):
			d := strings.TrimPrefix(line[len("data:"):], " ")
			if data == "" {
				data = d
			} else {
				data += "\n" + d // SSE joins multiple data: lines with newlines
			}
		}
	}
	return scanner.Err()
}

// ─────────────────────────── candidate addresses ───────────────────────────

// localCandidates returns this client's own LAN addresses as ip:port strings,
// all sharing the one UDP port of our single shared socket. We advertise these
// to the lighthouse so a peer behind the SAME NAT can reach us directly over the
// LAN instead of bouncing off our public address (which would require NAT
// hairpinning that many home routers don't do). IPv4-only, loopback and
// link-local skipped — keeping the POC simple.
func (m *Mesh) localCandidates() []string {
	ua, ok := m.transport.Conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil
	}
	port := strconv.Itoa(ua.Port)
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var out []string
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipnet.IP
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			continue
		}
		ip4 := ip.To4()
		if ip4 == nil {
			continue // IPv4 only for the POC
		}
		out = append(out, net.JoinHostPort(ip4.String(), port))
	}
	return out
}

// dedupeCandidates joins the public address with any LAN candidates, dropping
// duplicates and empties while preserving order (public first).
func dedupeCandidates(public string, local []string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, c := range append([]string{public}, local...) {
		if c == "" {
			continue
		}
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}

// ─────────────────────────── the hole punch ────────────────────────────────

// respondToPunch is the RESPONDER side. We don't dial; we just blast a few
// throwaway UDP packets at the initiator's public address. Each packet causes
// our NAT to open a hole for return traffic from that address. Once the hole is
// open, the initiator's QUIC handshake packets get through and acceptLoop
// accepts the connection.
func (m *Mesh) respondToPunch(s signal.Signal) {
	// If we're already connected to this peer, the signal is stale/duplicate —
	// ignore it. (Without this, a late signal would fire a pointless punch and
	// flip the indicator back to "connecting".)
	m.mu.Lock()
	_, already := m.conns[s.FromNodeID]
	m.mu.Unlock()
	if already {
		m.log.Debug("ignoring punch signal; already connected", "peer", s.FromNodeID)
		return
	}

	// Punch toward every candidate the initiator advertised: its public address
	// AND any LAN addresses. On the same network the LAN candidate is the one
	// that actually carries traffic (no hairpin); across networks the public one
	// does. Punching all of them is cheap and we don't know in advance which
	// applies.
	cands := dedupeCandidates(s.FromAddr, s.FromLocalAddrs)
	m.statusf("punching back toward %s (%s)", s.FromNodeID, strings.Join(cands, ", "))
	m.setState(s.FromNodeID, stateConnecting)

	for _, c := range cands {
		addr, err := net.ResolveUDPAddr("udp", c)
		if err != nil {
			m.log.Error("punch: bad peer address", "addr", c, "err", err)
			continue
		}
		// Send a handful of punch packets over a short window. They are not valid
		// QUIC packets; the peer's quic-go will just ignore them. Their ONLY
		// purpose is to make OUR NAT create an outbound mapping toward the peer.
		go func(addr *net.UDPAddr) {
			defer logutil.Recover(m.log, "respondToPunch")
			for i := 0; i < 5; i++ {
				_, _ = m.transport.WriteTo([]byte("punch"), addr)
				time.Sleep(200 * time.Millisecond)
			}
		}(addr)
	}
}

// Connect is the INITIATOR side, triggered when the user hits Enter on a peer.
// We (1) tell the lighthouse to nudge the peer into punching back, then
// (2) dial the peer directly. Our dial packets open our NAT; the peer's punch
// packets open theirs; the handshake completes through both holes.
func (m *Mesh) Connect(peer signal.Peer) {
	// Guard against redundant dials: if we're already connected to this peer, or
	// a dial is already in flight, do nothing. This makes Connect safe to call
	// repeatedly — both from the UI and from the auto-mesh loop every few seconds.
	m.mu.Lock()
	_, connected := m.conns[peer.NodeID]
	_, dialing := m.connecting[peer.NodeID]
	if connected || dialing {
		m.mu.Unlock()
		return
	}
	m.connecting[peer.NodeID] = struct{}{}
	m.mu.Unlock()

	go func() {
		defer logutil.Recover(m.log, "Connect")
		defer func() {
			m.mu.Lock()
			delete(m.connecting, peer.NodeID)
			m.mu.Unlock()
		}()
		ctx := context.Background()

		m.setState(peer.NodeID, stateConnecting)
		m.statusf("asking lighthouse to nudge %s", peer.NodeID)

		// 1. Leave a punch-back request for the peer.
		if err := m.post(ctx, "/connect", signal.ConnectRequest{From: m.nodeID, To: peer.NodeID}, nil); err != nil {
			m.statusf("connect signal failed: %v", err)
			m.setState(peer.NodeID, stateFailed)
			return
		}

		// 2. Dial the peer over the shared transport. We race every candidate
		//    (public + the peer's LAN addresses) in parallel and take the first
		//    handshake that completes — that's the path that actually works
		//    (LAN when we're co-located, public otherwise). We give it a generous
		//    handshake window because the peer might take a moment to punch back.
		cands := dedupeCandidates(peer.Addr, peer.LocalAddrs)
		m.statusf("dialing %s at %s", peer.NodeID, strings.Join(cands, ", "))

		dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		conn, err := m.dialFirst(dialCtx, cands)
		if err != nil {
			m.statusf("dial failed: %v", err)
			m.setState(peer.NodeID, stateFailed)
			return
		}

		// 3. Open the chat stream and send a HELLO so the responder learns who
		//    we are (the accept side only knows our address, not our node id).
		stream, err := conn.OpenStreamSync(ctx)
		if err != nil {
			m.log.Error("open stream failed", "peer", peer.NodeID, "err", err)
			m.setState(peer.NodeID, stateFailed)
			return
		}
		pc := &peerConn{nodeID: peer.NodeID, conn: conn, stream: stream}
		if err := m.sendLine(pc, "HELLO "+m.nodeID); err != nil {
			m.log.Error("hello write failed", "peer", peer.NodeID, "err", err)
			m.setState(peer.NodeID, stateFailed)
			return
		}

		m.addConn(pc)
		m.setState(peer.NodeID, stateConnected)
		m.statusf("connected to %s 🎉", peer.NodeID)

		// Tell the lighthouse the handoff succeeded, purely so its public
		// dashboard can show "handed off ✔" (the lighthouse can't observe our
		// direct P2P connection itself). Best-effort; ignore errors.
		_ = m.post(ctx, "/handoff", signal.ConnectRequest{From: m.nodeID, To: peer.NodeID}, nil)

		go m.readLoop(peer.NodeID, stream)
	}()
}

// dialFirst races a QUIC handshake against every candidate address at once and
// returns the first connection that succeeds, cancelling the rest. All dials
// ride the one shared transport (so they all leave from our single NAT mapping);
// the losers fail fast once we cancel the context. If none connect, it returns
// the first error seen.
func (m *Mesh) dialFirst(ctx context.Context, cands []string) (*quic.Conn, error) {
	if len(cands) == 0 {
		return nil, fmt.Errorf("no candidate addresses")
	}
	dctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tlsCfg := quicutil.ClientTLSConfig("quickmesh-peer", true, signal.ALPNPeer)
	qcfg := &quic.Config{
		HandshakeIdleTimeout: 15 * time.Second, // generous: the peer may take a moment to punch back
		MaxIdleTimeout:       20 * time.Second, // match the listen side (see NewMesh)
		KeepAlivePeriod:      7 * time.Second,
	}

	type result struct {
		conn *quic.Conn
		err  error
	}
	ch := make(chan result, len(cands))
	started := 0
	for _, c := range cands {
		addr, err := net.ResolveUDPAddr("udp", c)
		if err != nil {
			m.log.Error("dial: bad candidate", "addr", c, "err", err)
			continue
		}
		started++
		go func(addr *net.UDPAddr, label string) {
			defer logutil.Recover(m.log, "dialFirst")
			conn, err := m.transport.Dial(dctx, addr, tlsCfg, qcfg)
			if err == nil {
				m.log.Debug("candidate connected", "addr", label)
			}
			ch <- result{conn: conn, err: err}
		}(addr, c)
	}

	var winner *quic.Conn
	var firstErr error
	for i := 0; i < started; i++ {
		r := <-ch
		switch {
		case r.err == nil && winner == nil:
			winner = r.conn
			cancel() // abort the slower candidates
		case r.err == nil:
			_ = r.conn.CloseWithError(0, "redundant candidate") // a loser that still connected
		case firstErr == nil:
			firstErr = r.err
		}
	}
	if winner == nil {
		if firstErr == nil {
			firstErr = fmt.Errorf("no candidate connected")
		}
		return nil, firstErr
	}
	return winner, nil
}

// acceptLoop accepts inbound peer connections (RESPONDER side handshake). The
// initiator sends "HELLO <nodeID>" as the first line so we can identify them.
func (m *Mesh) acceptLoop(ctx context.Context) {
	defer logutil.Recover(m.log, "acceptLoop")
	for {
		conn, err := m.listener.Accept(ctx)
		if err != nil {
			m.log.Debug("accept loop ending", "err", err)
			return // transport closed / ctx done
		}
		go m.handleInbound(ctx, conn)
	}
}

func (m *Mesh) handleInbound(ctx context.Context, conn *quic.Conn) {
	defer logutil.Recover(m.log, "handleInbound")

	// Accept the bidirectional chat stream the initiator opens.
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		m.log.Debug("accept stream failed", "remote", conn.RemoteAddr().String(), "err", err)
		return
	}
	reader := bufio.NewReader(stream)

	// First line identifies the peer: "HELLO <nodeID>".
	hello, err := reader.ReadString('\n')
	if err != nil {
		m.log.Debug("read HELLO failed", "remote", conn.RemoteAddr().String(), "err", err)
		return
	}
	var peerID string
	if _, err := fmt.Sscanf(hello, "HELLO %s", &peerID); err != nil || peerID == "" {
		m.log.Warn("malformed HELLO", "remote", conn.RemoteAddr().String(), "line", trimNewline(hello))
		return
	}

	m.addConn(&peerConn{nodeID: peerID, conn: conn, stream: stream})
	m.setState(peerID, stateConnected)
	m.statusf("accepted connection from %s (%s)", peerID, conn.RemoteAddr())

	// Continue reading chat lines using the same buffered reader (it may already
	// hold buffered bytes past the HELLO line).
	m.readLoopReader(peerID, reader)
}

// ─────────────────────────── chat I/O ──────────────────────────────────────

// readLoop reads newline-delimited chat lines from a freshly-opened stream
// (initiator side, no buffered reader yet).
func (m *Mesh) readLoop(peerID string, stream *quic.Stream) {
	m.readLoopReader(peerID, bufio.NewReader(stream))
}

func (m *Mesh) readLoopReader(peerID string, reader *bufio.Reader) {
	defer logutil.Recover(m.log, "readLoop:"+peerID)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			m.setState(peerID, stateDisconnected)
			m.removeConn(peerID)
			m.statusf("%s disconnected (%v)", peerID, err)
			return
		}
		text := trimNewline(line)
		if text == "" {
			continue
		}
		m.events(chatMsg{peerID: peerID, from: peerID, text: text})
	}
}

// sendLine writes one newline-terminated line to a peer's stream. It serializes
// writes per-connection (writeMu) and sets a write DEADLINE so a stuck or gone
// peer can never block forever — critical, because the TUI must never wait on
// the network.
func (m *Mesh) sendLine(pc *peerConn, line string) error {
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()
	_ = pc.stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := fmt.Fprintf(pc.stream, "%s\n", line)
	return err
}

// SendChat writes a chat line to a peer. It is called from the Bubble Tea Update
// loop, so it MUST return instantly and MUST NOT call m.events (program.Send) on
// the calling goroutine — Bubble Tea's msg channel is unbuffered, and a
// synchronous Send from Update deadlocks the whole UI. So we do absolutely
// everything (lookup, write, and any status/event reporting) inside a goroutine.
// The local echo is handled by the TUI itself (see tui.go).
func (m *Mesh) SendChat(peerID, text string) {
	go func() {
		defer logutil.Recover(m.log, "SendChat:"+peerID)

		m.mu.Lock()
		pc := m.conns[peerID]
		m.mu.Unlock()
		if pc == nil {
			m.statusf("not connected to %s", peerID)
			return
		}

		if err := m.sendLine(pc, text); err != nil {
			m.log.Error("send failed", "peer", peerID, "err", err)
			m.statusf("send to %s failed: %v", peerID, err)
			m.setState(peerID, stateDisconnected)
			m.removeConn(peerID)
		}
	}()
}

// SendBroadcast sends text to EVERY currently-connected peer (this powers the
// "broadcast" pseudo-peer in the UI). Like SendChat it must be safe to call from
// the Bubble Tea Update loop, so the whole thing runs off the UI goroutine: the
// outer goroutine snapshots the peer list and reports status, and each peer is
// written to in its own goroutine so one slow peer can't hold up the rest.
func (m *Mesh) SendBroadcast(text string) {
	go func() {
		defer logutil.Recover(m.log, "SendBroadcast")

		m.mu.Lock()
		peers := make([]*peerConn, 0, len(m.conns))
		for _, pc := range m.conns {
			peers = append(peers, pc)
		}
		m.mu.Unlock()

		if len(peers) == 0 {
			m.statusf("no connected peers to broadcast to")
			return
		}

		for _, pc := range peers {
			go func(pc *peerConn) {
				defer logutil.Recover(m.log, "SendBroadcast:"+pc.nodeID)
				if err := m.sendLine(pc, text); err != nil {
					m.log.Error("broadcast send failed", "peer", pc.nodeID, "err", err)
					m.setState(pc.nodeID, stateDisconnected)
					m.removeConn(pc.nodeID)
				}
			}(pc)
		}
		m.statusf("broadcast to %d peer(s)", len(peers))
	}()
}

// ─────────────────────────── small helpers ─────────────────────────────────

func (m *Mesh) addConn(pc *peerConn) {
	m.mu.Lock()
	m.conns[pc.nodeID] = pc
	m.mu.Unlock()
}

func (m *Mesh) removeConn(peerID string) {
	m.mu.Lock()
	delete(m.conns, peerID)
	m.mu.Unlock()
}

func (m *Mesh) setState(peerID string, st connState) {
	m.log.Debug("peer state", "peer", peerID, "state", st.String())
	m.events(stateMsg{peerID: peerID, state: st})
}

func trimNewline(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}
