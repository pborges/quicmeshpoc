// Package signal defines the tiny JSON "wire protocol" spoken between the
// quicchat clients and the lighthouse.
//
// The lighthouse is a *signaling* / *rendezvous* server. It never relays chat
// traffic. Its only jobs are:
//
//  1. Observe each client's PUBLIC UDP address (the address its NAT presents to
//     the internet) and remember it under a node id.  -> Register
//  2. Hand out the list of everyone it currently knows about.                -> Peers
//  3. When client A wants to reach client B, leave a note for B telling it to
//     "punch" a hole back toward A.                                          -> Connect / Signals
//
// That is the entire control plane. Everything else (the actual hole punch and
// the chat) happens directly between the two clients.
package signal

// ALPNPeer is the ALPN protocol id used for the peer-to-peer chat QUIC
// connections. (The lighthouse traffic uses the standard "h3" ALPN instead.)
const ALPNPeer = "quickmesh-chat/1"

// RegisterRequest is the body a client POSTs to /register to advertise itself.
// Note that the client does NOT send its own address — it can't know its own
// public address. The lighthouse fills that in by looking at where the packets
// actually came from.
type RegisterRequest struct {
	NodeID string `json:"node_id"`
	// LocalAddrs are the client's own LAN candidate addresses (ip:port on the
	// SAME shared socket). The lighthouse can't observe these — only the public
	// reflexive address — so the client reports them. They let two peers behind
	// the SAME NAT connect directly over the LAN instead of relying on NAT
	// hairpinning of the public address. This is a tiny slice of the idea ICE
	// formalises as "host candidates".
	LocalAddrs []string `json:"local_addrs,omitempty"`
}

// RegisterResponse echoes back the public address the lighthouse observed. This
// is the "reflexive" address in STUN terminology: "here is how the outside
// world sees you." It's useful for the client to display and to understand what
// its NAT is doing.
type RegisterResponse struct {
	YourAddr string `json:"your_addr"`
}

// Peer is one advertised node as seen by the lighthouse.
type Peer struct {
	NodeID     string   `json:"node_id"`
	Addr       string   `json:"addr"`                  // public UDP ip:port observed by the lighthouse
	LocalAddrs []string `json:"local_addrs,omitempty"` // LAN candidates the peer reported (see RegisterRequest)
}

// PeersResponse is the answer to GET /peers.
type PeersResponse struct {
	Peers []Peer `json:"peers"`
}

// ConnectRequest is POSTed to /connect when node From wants to start a
// connection to node To. The lighthouse turns this into a pending Signal for To.
type ConnectRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Signal is a pending "please punch back toward this peer" note that a node
// picks up when it polls GET /signals. FromAddr is the public address the
// responder should fire punch packets at.
type Signal struct {
	FromNodeID     string   `json:"from_node_id"`
	FromAddr       string   `json:"from_addr"`
	FromLocalAddrs []string `json:"from_local_addrs,omitempty"` // LAN candidates to also punch toward
}

// SignalsResponse is the answer to GET /signals: every punch request queued for
// the polling node since it last asked.
type SignalsResponse struct {
	Signals []Signal `json:"signals"`
}
