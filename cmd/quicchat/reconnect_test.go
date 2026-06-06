package main

// Tests for the "snappy reconnect" behaviour: detecting a peer restart via its
// incarnation id and re-forming the link to the NEW process, rather than
// holding a dead connection until the QUIC idle timeout.

import (
	"context"
	"testing"
	"time"
)

// TestIncarnationChanged covers the guard logic that decides whether a peer has
// restarted. The important cases are the "mixed fleet" ones (one side has no
// incarnation): those must NOT be treated as a restart, or we'd reap healthy
// links to/through older peers and lighthouses.
func TestIncarnationChanged(t *testing.T) {
	cases := []struct {
		name         string
		have, latest string
		want         bool
	}{
		{"same incarnation", "abc", "abc", false},
		{"restarted", "abc", "xyz", true},
		{"we recorded none (old peer)", "", "xyz", false},
		{"roster has none (old lighthouse)", "abc", "", false},
		{"both none", "", "", false},
	}
	for _, tc := range cases {
		if got := incarnationChanged(tc.have, tc.latest); got != tc.want {
			t.Errorf("%s: incarnationChanged(%q, %q) = %v, want %v", tc.name, tc.have, tc.latest, got, tc.want)
		}
	}
}

// TestReconnectAfterPeerRestart connects alice<->bob, then restarts bob as a
// brand-new process (new socket/port AND new incarnation) and asserts the mesh
// re-forms the link to the new bob on its own — no manual reconnect — and that
// the link alice ends up holding is to the new incarnation, not a ghost of the
// old one.
func TestReconnectAfterPeerRestart(t *testing.T) {
	lhURL, _ := startLighthouse(t, "127.0.0.1:14455", "127.0.0.1:18081")

	// alice runs for the whole test; only bob is restarted.
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	alice, ac := newMesh(t, ctxA, "alice", lhURL)
	defer alice.Close()

	ctxB1, cancelB1 := context.WithCancel(context.Background())
	bob1, _ := newMesh(t, ctxB1, "bob", lhURL)

	// 1. Wait for the initial link (alice is the initiator since "alice" < "bob").
	if !waitFor(t, 25*time.Second, func() bool {
		return ac.state("bob") == stateConnected
	}) {
		t.Fatal("alice never connected to bob")
	}

	// Capture bob's incarnation as alice recorded it, to prove it changes later.
	oldInc := peerIncarnation(t, alice, "bob")
	if oldInc == "" {
		t.Fatal("alice recorded no incarnation for bob (incarnation not wired through?)")
	}

	// 2. Restart bob: stop its loops, close its socket (alice sees the drop), then
	//    bring up a fresh bob — new OS-assigned port and a new incarnation.
	cancelB1()
	bob1.Close()

	ctxB2, cancelB2 := context.WithCancel(context.Background())
	defer cancelB2()
	bob2, bc2 := newMesh(t, ctxB2, "bob", lhURL)
	defer bob2.Close()

	// 3. The mesh must re-form alice<->bob to the new process on its own.
	if !waitFor(t, 30*time.Second, func() bool {
		return ac.state("bob") == stateConnected && bc2.state("alice") == stateConnected
	}) {
		t.Fatal("alice never reconnected to the restarted bob")
	}

	// 4. The connection alice now holds must be to the NEW incarnation.
	if newInc := peerIncarnation(t, alice, "bob"); newInc == oldInc {
		t.Fatalf("alice still holds the old incarnation %q after restart", oldInc)
	}

	// 5. And chat must flow over the re-formed link.
	alice.SendChat("bob", "welcome back")
	if !waitFor(t, 10*time.Second, func() bool {
		return bc2.hasMessage("alice", "welcome back")
	}) {
		t.Fatal("restarted bob never received alice's message")
	}
}

// peerIncarnation reads the incarnation recorded on the live connection to peer,
// or "" if there is none. Safe for concurrent mesh goroutines (takes m.mu).
func peerIncarnation(t *testing.T, m *Mesh, peer string) string {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if pc, ok := m.conns[peer]; ok {
		return pc.incarnation
	}
	return ""
}
