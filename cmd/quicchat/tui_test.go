package main

// Regression test for the "UI hangs on Enter" bug.
//
// Bubble Tea runs Model.Update on its event-loop goroutine, and its message
// channel is UNBUFFERED. So if Update ever calls program.Send (our m.events)
// synchronously, it blocks forever (the only reader is the very goroutine stuck
// in Update) and the whole UI — including ctrl+c — freezes.
//
// This test wires m.events to block forever, then drives the key presses that go
// through the Update -> Mesh path. Each Update call must return promptly; if any
// of them calls m.events synchronously, Update blocks and the test times out.

import (
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// A late "connecting" signal (e.g. the responder's punch poll firing after the
// connection was already accepted) must not downgrade a connected peer back to
// amber.
func TestConnectedNotDowngradedByLatePunch(t *testing.T) {
	m, err := NewMesh("tester", "https://127.0.0.1:1", true, testLogger)
	if err != nil {
		t.Fatalf("NewMesh: %v", err)
	}
	var mdl tea.Model = newModel(m)

	mdl, _ = mdl.Update(stateMsg{peerID: "bob", state: stateConnected})
	mdl, _ = mdl.Update(stateMsg{peerID: "bob", state: stateConnecting}) // late punch

	if got := mdl.(model).states["bob"]; got != stateConnected {
		t.Fatalf("peer state downgraded to %v; want connected", got)
	}

	// But a real disconnect must still take effect, and a later reconnect can go
	// back to connecting.
	mdl, _ = mdl.Update(stateMsg{peerID: "bob", state: stateDisconnected})
	mdl, _ = mdl.Update(stateMsg{peerID: "bob", state: stateConnecting})
	if got := mdl.(model).states["bob"]; got != stateConnecting {
		t.Fatalf("reconnect state = %v; want connecting", got)
	}
}

func TestUpdateNeverSendsSynchronously(t *testing.T) {
	m, err := NewMesh("tester", "https://127.0.0.1:1", true, testLogger)
	if err != nil {
		t.Fatalf("NewMesh: %v", err)
	}

	// Any synchronous call to events from Update will park here forever.
	block := make(chan struct{})
	m.SetEvents(func(any) { <-block })

	enter := tea.KeyMsg{Type: tea.KeyEnter}

	cases := []struct {
		name  string
		setup func(mdl *model)
	}{
		{
			name: "enter in input box with a connected peer (SendChat path)",
			setup: func(mdl *model) {
				mdl.focus = focusInput
				mdl.peers = []peerView{{nodeID: "peer", state: stateConnected}}
				mdl.states["peer"] = stateConnected
				mdl.input.SetValue("hello")
			},
		},
		{
			name: "enter in sidebar starting a connection (Connect path)",
			setup: func(mdl *model) {
				mdl.focus = focusSidebar
				mdl.peers = []peerView{{nodeID: "peer", addr: "127.0.0.1:9", state: stateDisconnected}}
			},
		},
		{
			name: "enter in input box on the broadcast peer (SendBroadcast path)",
			setup: func(mdl *model) {
				mdl.focus = focusInput
				mdl.peers = []peerView{{nodeID: broadcastID}, {nodeID: "peer", state: stateConnected}}
				mdl.cursor = 0 // broadcast row
				mdl.states["peer"] = stateConnected
				mdl.input.SetValue("hi everyone")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mdl := newModel(m)
			tc.setup(&mdl)

			done := make(chan struct{})
			go func() {
				mdl.Update(enter)
				close(done)
			}()

			select {
			case <-done:
				// Update returned without blocking on a synchronous Send. Good.
			case <-time.After(2 * time.Second):
				t.Fatal("Update blocked — it called program.Send synchronously (UI deadlock)")
			}
		})
	}
}
