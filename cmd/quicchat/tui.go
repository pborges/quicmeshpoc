package main

// tui.go is the Bubble Tea (charm) terminal UI. It is deliberately dumb: it
// owns no networking. The Mesh (mesh.go) runs all the QUIC/hole-punch logic in
// background goroutines and pushes events in as tea.Msg values via program.Send.
// The UI reacts to those messages and, on key presses, calls back into the Mesh
// (Connect / SendChat).
//
// Layout:
//
//   ┌────────────┬─────────────────────────────┐
//   │ sidebar    │ chat viewport               │  <- peers list | messages
//   │ ● peerA    │ ...                         │
//   │ ○ peerB    │                             │
//   │            ├─────────────────────────────┤
//   │            │ > input box                 │  <- text input
//   └────────────┴─────────────────────────────┘
//   status line
//
// Tab cycles focus between the sidebar and the input box. In the sidebar,
// up/down move the selection and Enter starts a connection to the highlighted
// peer.

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/pborges/quicmeshpoc/internal/signal"
)

// ── messages pushed in from the Mesh (see mesh.go) ──────────────────────────

type peersMsg struct{ peers []signal.Peer } // refreshed sidebar list
type stateMsg struct {                      // a peer's conn state changed
	peerID string
	state  connState
}
type chatMsg struct { // an incoming or locally-echoed chat line
	peerID string
	from   string
	text   string
}
type statusMsg struct{ text string } // one-line status / log message

// ── focus areas ─────────────────────────────────────────────────────────────

type focusArea int

const (
	focusSidebar focusArea = iota
	focusInput
)

// broadcastID is the sentinel nodeID of the "broadcast" pseudo-peer that's
// always pinned at the top of the sidebar. Sending to it fans the message out to
// every connected peer; its chat view aggregates everyone's incoming messages.
// The asterisks make it extremely unlikely to collide with a real node name.
const broadcastID = "*broadcast*"

// peerView is the sidebar's view of one peer: identity + live connection state.
type peerView struct {
	nodeID     string
	addr       string
	localAddrs []string // LAN candidates advertised by the peer (used when connecting)
	state      connState
}

type model struct {
	mesh *Mesh

	peers  []peerView           // ordered sidebar entries
	states map[string]connState // nodeID -> state (survives peer-list refreshes)
	chats  map[string][]string  // nodeID -> rendered chat lines

	cursor int       // highlighted sidebar index
	focus  focusArea // which panel has focus
	input  textinput.Model
	status string

	width, height int
}

func newModel(m *Mesh) model {
	ti := textinput.New()
	ti.Placeholder = "type a message and press Enter…"
	ti.Prompt = "> "

	return model{
		mesh:   m,
		states: make(map[string]connState),
		chats:  make(map[string][]string),
		// The broadcast pseudo-peer is always present, even before any real
		// peers show up.
		peers:  []peerView{{nodeID: broadcastID}},
		focus:  focusSidebar,
		input:  ti,
		status: "starting up… (Tab switches panels, ↑/↓ pick a peer, Enter connects)",
	}
}

// connectedCount returns how many real peers are currently connected.
func (m model) connectedCount() int {
	n := 0
	for id, st := range m.states {
		if id != broadcastID && st == stateConnected {
			n++
		}
	}
	return n
}

// broadcastState gives the broadcast row an aggregate indicator: green if any
// peer is connected, amber if any is mid-connect, else grey.
func (m model) broadcastState() connState {
	best := stateDisconnected
	for id, st := range m.states {
		if id == broadcastID {
			continue
		}
		if st == stateConnected {
			return stateConnected
		}
		if st == stateConnecting {
			best = stateConnecting
		}
	}
	return best
}

func (m model) Init() tea.Cmd { return textinput.Blink }

// activePeer returns the nodeID of the currently highlighted sidebar entry.
func (m model) activePeer() string {
	if m.cursor >= 0 && m.cursor < len(m.peers) {
		return m.peers[m.cursor].nodeID
	}
	return ""
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		// Input width = right panel width minus borders/prompt padding.
		m.input.Width = m.rightWidth() - 6
		return m, nil

	case peersMsg:
		m.mergePeers(msg.peers)
		return m, nil

	case stateMsg:
		// Guard against a late punch signal downgrading an already-established
		// connection: on a fast link the inbound connection is accepted (green)
		// BEFORE the responder's 1s signal poll fires "connecting" (amber). The
		// model is the authority, so we simply refuse connected -> connecting.
		if !(msg.state == stateConnecting && m.states[msg.peerID] == stateConnected) {
			m.states[msg.peerID] = msg.state
		}
		m.applyStates()
		return m, nil

	case chatMsg:
		prefix := msg.from
		if msg.from == m.mesh.nodeID {
			prefix = "me"
		}
		m.chats[msg.peerID] = append(m.chats[msg.peerID], fmt.Sprintf("%s: %s", prefix, msg.text))
		// Mirror incoming peer messages into the aggregate broadcast feed so it
		// reads like a group chat. (Our own echoes are added by the input handler.)
		if msg.peerID != broadcastID && msg.from != m.mesh.nodeID {
			m.chats[broadcastID] = append(m.chats[broadcastID], fmt.Sprintf("%s: %s", msg.from, msg.text))
		}
		return m, nil

	case statusMsg:
		m.status = msg.text
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)
	}

	// Forward anything else (e.g. cursor blink) to the text input.
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit

	case "tab":
		// Toggle focus between the two panels.
		if m.focus == focusSidebar {
			m.focus = focusInput
			m.input.Focus()
		} else {
			m.focus = focusSidebar
			m.input.Blur()
		}
		return m, nil
	}

	if m.focus == focusSidebar {
		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.peers)-1 {
				m.cursor++
			}
		case "enter":
			p := m.activePeer()
			if p == broadcastID {
				// "Connect all": hole-punch to every peer that isn't already
				// connected/connecting. Each Connect spawns its own goroutine.
				for _, pv := range m.peers {
					if pv.nodeID == broadcastID {
						continue
					}
					if st := m.states[pv.nodeID]; st != stateConnected && st != stateConnecting {
						m.mesh.Connect(signal.Peer{NodeID: pv.nodeID, Addr: pv.addr, LocalAddrs: pv.localAddrs})
					}
				}
			} else if p != "" {
				// Start a connection (hole punch) to the highlighted peer.
				pv := m.peers[m.cursor]
				m.mesh.Connect(signal.Peer{NodeID: pv.nodeID, Addr: pv.addr, LocalAddrs: pv.localAddrs})
			}
		}
		return m, nil
	}

	// focusInput
	if msg.String() == "enter" {
		text := strings.TrimSpace(m.input.Value())
		peer := m.activePeer()
		if text == "" {
			return m, nil
		}
		// IMPORTANT for every branch below: give feedback by mutating the model
		// directly and NEVER call program.Send (m.events) from here — Update runs
		// on Bubble Tea's event-loop goroutine and its msg channel is unbuffered,
		// so a synchronous Send from Update would deadlock the whole UI.
		if peer == broadcastID {
			if m.connectedCount() == 0 {
				m.status = "no peers connected — broadcast has nobody to reach"
				return m, nil
			}
			m.chats[broadcastID] = append(m.chats[broadcastID], "me: "+text)
			m.mesh.SendBroadcast(text) // non-blocking
			m.input.SetValue("")
			return m, nil
		}
		if peer == "" || m.states[peer] != stateConnected {
			m.status = "not connected to " + peer + " — pick it in the sidebar and press Enter to connect"
			return m, nil
		}
		// Local echo straight into the model (no program.Send), then hand the
		// network write to a goroutine. SendChat returns immediately.
		m.chats[peer] = append(m.chats[peer], "me: "+text)
		m.mesh.SendChat(peer, text)
		m.input.SetValue("")
		return m, nil
	}

	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

// mergePeers updates the sidebar list from a fresh lighthouse response while
// preserving connection states we already know about.
func (m *model) mergePeers(peers []signal.Peer) {
	real := make([]peerView, 0, len(peers))
	for _, p := range peers {
		real = append(real, peerView{nodeID: p.NodeID, addr: p.Addr, localAddrs: p.LocalAddrs, state: m.states[p.NodeID]})
	}
	// Stable alphabetical order so the list doesn't jump around between polls.
	sort.Slice(real, func(i, j int) bool { return real[i].nodeID < real[j].nodeID })
	// The broadcast pseudo-peer is always pinned at index 0.
	m.peers = append([]peerView{{nodeID: broadcastID}}, real...)
	if m.cursor >= len(m.peers) {
		m.cursor = max(0, len(m.peers)-1)
	}
}

// applyStates re-stamps the live connection state onto the current peer views.
func (m *model) applyStates() {
	for i := range m.peers {
		if m.peers[i].nodeID == broadcastID {
			continue // broadcast uses an aggregate indicator, not a single state
		}
		m.peers[i].state = m.states[m.peers[i].nodeID]
	}
}

// ─────────────────────────────── rendering ─────────────────────────────────

var (
	sidebarBorder = lipgloss.NewStyle().Border(lipgloss.NormalBorder()).Padding(0, 1)
	mainBorder    = lipgloss.NewStyle().Border(lipgloss.NormalBorder()).Padding(0, 1)
	focusColor    = lipgloss.Color("205")
	dimColor      = lipgloss.Color("240")
	selectedStyle = lipgloss.NewStyle().Foreground(focusColor).Bold(true)
)

// stateDot maps a connection state to a coloured indicator for the sidebar.
func stateDot(s connState) string {
	switch s {
	case stateConnected:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Render("●") // green
	case stateConnecting:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Render("◐") // amber
	case stateFailed:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Render("✗") // red
	default:
		return lipgloss.NewStyle().Foreground(dimColor).Render("○") // grey
	}
}

func (m model) sidebarWidth() int { return max(24, m.width/4) }
func (m model) rightWidth() int   { return max(20, m.width-m.sidebarWidth()-4) }

func (m model) View() string {
	if m.width == 0 {
		return "loading…"
	}

	innerHeight := m.height - 4 // leave room for borders + status line

	// ── sidebar ──
	var sb strings.Builder
	sb.WriteString("PEERS\n\n")
	for i, p := range m.peers {
		var line string
		if p.nodeID == broadcastID {
			// Aggregate indicator + label + connected-peer count.
			line = fmt.Sprintf("%s 📢 broadcast (%d)", stateDot(m.broadcastState()), m.connectedCount())
		} else {
			line = fmt.Sprintf("%s %s", stateDot(p.state), p.nodeID)
		}
		if i == m.cursor {
			line = selectedStyle.Render("▸ " + line)
		} else {
			line = "  " + line
		}
		sb.WriteString(line + "\n")
	}
	if len(m.peers) <= 1 { // only the broadcast row, no real peers yet
		sb.WriteString(lipgloss.NewStyle().Foreground(dimColor).Render("\n(waiting for peers…)"))
	}
	sidebar := sidebarBorder.
		Width(m.sidebarWidth()).
		Height(innerHeight).
		BorderForeground(borderColor(m.focus == focusSidebar)).
		Render(sb.String())

	// ── chat viewport ──
	peer := m.activePeer()
	var chat strings.Builder
	if peer == "" {
		chat.WriteString(lipgloss.NewStyle().Foreground(dimColor).Render("no peer selected"))
	} else {
		header := "chat with " + peer
		if peer == broadcastID {
			header = fmt.Sprintf("📢 broadcast — %d connected peer(s)", m.connectedCount())
		}
		chat.WriteString(lipgloss.NewStyle().Bold(true).Render(header) + "\n\n")
		lines := m.chats[peer]
		// Show only the lines that fit in the viewport.
		if visible := innerHeight - 4; len(lines) > visible && visible > 0 {
			lines = lines[len(lines)-visible:]
		}
		chat.WriteString(strings.Join(lines, "\n"))
	}
	chatView := mainBorder.
		Width(m.rightWidth()).
		Height(innerHeight - 3).
		BorderForeground(borderColor(false)).
		Render(chat.String())

	inputView := mainBorder.
		Width(m.rightWidth()).
		BorderForeground(borderColor(m.focus == focusInput)).
		Render(m.input.View())

	right := lipgloss.JoinVertical(lipgloss.Left, chatView, inputView)
	body := lipgloss.JoinHorizontal(lipgloss.Top, sidebar, right)

	status := lipgloss.NewStyle().Foreground(dimColor).Render(m.status)
	return lipgloss.JoinVertical(lipgloss.Left, body, status)
}

func borderColor(focused bool) lipgloss.Color {
	if focused {
		return focusColor
	}
	return dimColor
}
