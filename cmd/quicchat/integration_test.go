package main

// An end-to-end smoke test that exercises the real networking path WITHOUT the
// TUI: it boots the actual lighthouse binary, then drives two Mesh instances
// through register -> peer discovery -> connect (dial/accept) -> chat.
//
// On loopback there is no NAT, so this doesn't test real hole punching, but it
// does exercise every line of the signaling + QUIC dial/accept/stream code,
// which is what usually breaks.
//
// Run with: go test ./cmd/quicchat -run TestEndToEnd -v

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pborges/quicmeshpoc/internal/signal"
)

// testLogger discards logs during the test.
var testLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// collector captures the tea.Msg values a Mesh pushes, so a test can assert on
// peer connection states and received chat lines.
type collector struct {
	mu       sync.Mutex
	states   map[string]connState
	messages []chatMsg
}

func newCollector() *collector { return &collector{states: map[string]connState{}} }

// wire returns the events callback to hand to Mesh.SetEvents.
func (c *collector) wire() func(any) {
	return func(msg any) {
		c.mu.Lock()
		defer c.mu.Unlock()
		switch m := msg.(type) {
		case stateMsg:
			c.states[m.peerID] = m.state
		case chatMsg:
			c.messages = append(c.messages, m)
		}
	}
}

func (c *collector) state(peer string) connState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.states[peer]
}

func (c *collector) hasMessage(peer, text string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.messages {
		if m.peerID == peer && m.text == text {
			return true
		}
	}
	return false
}

// startLighthouse builds and starts the lighthouse binary on the given loopback
// API (UDP) and web (TCP) addresses, returning their base URLs. The process is
// killed via t.Cleanup when the test ends.
func startLighthouse(t *testing.T, apiAddr, webAddr string) (lhURL, webURL string) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "lighthouse")
	build := exec.Command("go", "build", "-o", bin, "../lighthouse")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("build lighthouse: %v", err)
	}
	lh := exec.Command(bin, "-addr", apiAddr, "-web", webAddr)
	lh.Stderr = os.Stderr
	if err := lh.Start(); err != nil {
		t.Fatalf("start lighthouse: %v", err)
	}
	t.Cleanup(func() { lh.Process.Kill() })
	time.Sleep(500 * time.Millisecond) // let it bind the socket
	return "https://" + apiAddr, "http://" + webAddr
}

// newMesh builds a Mesh wired to a fresh collector and starts it.
func newMesh(t *testing.T, ctx context.Context, id, lhURL string) (*Mesh, *collector) {
	t.Helper()
	m, err := NewMesh(id, lhURL, true, testLogger)
	if err != nil {
		t.Fatalf("%s mesh: %v", id, err)
	}
	c := newCollector()
	m.SetEvents(c.wire())
	m.Start(ctx)
	return m, c
}

func TestEndToEnd(t *testing.T) {
	lhURL, webURL := startLighthouse(t, "127.0.0.1:14433", "127.0.0.1:18080")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alice, ac := newMesh(t, ctx, "alice", lhURL)
	bob, bc := newMesh(t, ctx, "bob", lhURL)

	// 4. Wait until alice can see bob in the peer list (proves register + peers).
	var bobPeer signal.Peer
	if !waitFor(t, 15*time.Second, func() bool {
		var resp signal.PeersResponse
		if err := alice.get(ctx, "/peers?self=alice", &resp); err != nil {
			return false
		}
		for _, p := range resp.Peers {
			if p.NodeID == "bob" {
				bobPeer = p
				return true
			}
		}
		return false
	}) {
		t.Fatal("alice never discovered bob via lighthouse")
	}

	// 5. Alice connects to bob (dial + punch-back + accept).
	alice.Connect(bobPeer)

	if !waitFor(t, 20*time.Second, func() bool {
		return ac.state("bob") == stateConnected && bc.state("alice") == stateConnected
	}) {
		t.Fatal("alice<->bob never reached connected state")
	}

	// 6. Alice sends a chat line; bob must receive it.
	alice.SendChat("bob", "hello bob")
	if !waitFor(t, 10*time.Second, func() bool {
		return bc.hasMessage("alice", "hello bob")
	}) {
		t.Fatal("bob never received alice's message")
	}

	// 7. And the reverse direction.
	bob.SendChat("alice", "hi alice")
	if !waitFor(t, 10*time.Second, func() bool {
		return ac.hasMessage("bob", "hi alice")
	}) {
		t.Fatal("alice never received bob's message")
	}

	// 8. The web dashboard must reflect the handoff. Read the SSE stream (it
	//    pushes a full HTML snapshot immediately) and confirm it shows both
	//    nodes online and the completed handoff.
	snap := readSSESnapshot(t, webURL+"/events/stream")
	for _, want := range []string{"alice", "bob", "handed off"} {
		if !strings.Contains(snap, want) {
			t.Fatalf("dashboard snapshot missing %q; got:\n%s", want, snap)
		}
	}
}

// readSSESnapshot opens an SSE stream, reads ~1s of it, and returns the body.
func readSSESnapshot(t *testing.T, url string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("dashboard SSE request: %v", err)
	}
	defer resp.Body.Close()
	// The first "update" event carries the current snapshot, flushed as one
	// write, so a single Read returns it. Guard with a timeout so the test can't
	// hang on the long-lived stream.
	buf := make([]byte, 16*1024)
	done := make(chan int, 1)
	go func() { n, _ := resp.Body.Read(buf); done <- n }()
	select {
	case n := <-done:
		return string(buf[:n])
	case <-time.After(2 * time.Second):
		return ""
	}
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
