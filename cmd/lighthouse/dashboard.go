package main

// dashboard.go implements the lighthouse's live web dashboard: a public log of
// which nodes are online, which are waiting for a handoff, and which have been
// handed off (connected directly, peer-to-peer).
//
// How the "live" part works, end to end:
//
//	browser ──GET /──────────────▶ HTML shell (htmx + htmx-sse from a CDN)
//	browser ──GET /events/stream─▶ Server-Sent Events (SSE) connection, kept open
//	          ◀── event: update ── the server pushes a fresh HTML snapshot of the
//	                               whole board every time state changes
//	htmx swaps that HTML into <div sse-swap="update">  ── no page reload, no JS
//
// SSE is just a long-lived HTTP response whose body is a stream of
// "event:"/"data:" lines. htmx's SSE extension connects to it and, for each
// named event, replaces an element's contents. We push the entire board as one
// fragment to keep the server dead simple.

import (
	"fmt"
	"html"
	"io"
	"net/http"
	"strings"
	"time"
)

// Caps so the in-memory log/handoff slices don't grow forever.
const (
	maxEvents   = 50
	maxHandoffs = 50
)

// logEntry is one line in the public activity log.
type logEntry struct {
	at   time.Time
	kind string // "online" / "offline" / "handoff-requested" / "punching" / "handed-off"
	msg  string
}

// handoff tracks one connection attempt through its lifecycle:
//
//	requested (started)  ->  pickedUp (responder is punching)  ->  completed
type handoff struct {
	from, to    string
	started     time.Time
	pickedUp    bool // the responder drained its punch request
	completed   bool // the initiator reported a successful direct connection
	completedAt time.Time
}

// handoffTimeout is how long a handoff may sit un-completed before the dashboard
// shows it as "timed out". A real connection completes in a few seconds (the
// client's dial deadline is ~15s); anything still pending well past that never
// connected.
const handoffTimeout = 30 * time.Second

// findHandoff returns the existing handoff row for a from→to pair, or nil. We
// keep at most one row per pair, so the newest match is the only match. The
// caller must hold l.mu.
func (l *lighthouse) findHandoff(from, to string) *handoff {
	for i := len(l.handoffs) - 1; i >= 0; i-- {
		if l.handoffs[i].from == from && l.handoffs[i].to == to {
			return l.handoffs[i]
		}
	}
	return nil
}

// appendCapped appends v to s and trims the slice to the newest max entries.
func appendCapped[T any](s []T, v T, max int) []T {
	s = append(s, v)
	if len(s) > max {
		s = s[len(s)-max:]
	}
	return s
}

// addEvent records a log line and pushes a fresh snapshot to every SSE client.
func (l *lighthouse) addEvent(kind, msg string) {
	l.mu.Lock()
	l.events = appendCapped(l.events, logEntry{at: time.Now(), kind: kind, msg: msg}, maxEvents)
	l.mu.Unlock()
	l.broadcast()
}

// ─────────────────────────── SSE plumbing ──────────────────────────────────

// broadcast renders the current board and sends it to all subscribers. Slow or
// dead clients are skipped (non-blocking send) rather than blocking everyone.
func (l *lighthouse) broadcast() {
	html := l.snapshotHTML()
	l.mu.Lock()
	defer l.mu.Unlock()
	for ch := range l.subs {
		select {
		case ch <- html:
		default: // subscriber's buffer is full; drop this update for them
		}
	}
}

func (l *lighthouse) subscribe() chan string {
	ch := make(chan string, 4)
	l.mu.Lock()
	l.subs[ch] = struct{}{}
	l.mu.Unlock()
	return ch
}

func (l *lighthouse) unsubscribe(ch chan string) {
	l.mu.Lock()
	delete(l.subs, ch)
	l.mu.Unlock()
}

// handleStream is the SSE endpoint. It holds the HTTP response open and writes
// an "update" event whenever the board changes.
func (l *lighthouse) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := l.subscribe()
	defer l.unsubscribe(ch)

	// Send the current state immediately so a fresh page isn't blank.
	writeSSE(w, "update", l.snapshotHTML())
	flusher.Flush()

	// A heartbeat keeps proxies from closing an idle connection.
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return // browser navigated away / closed the tab
		case htmlSnapshot := <-ch:
			writeSSE(w, "update", htmlSnapshot)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprint(w, ": keep-alive\n\n") // an SSE comment line
			flusher.Flush()
		}
	}
}

// writeSSE writes one SSE event. Each line of the payload must be sent as its
// own "data:" field, so we split on newlines.
func writeSSE(w io.Writer, event, payload string) {
	fmt.Fprintf(w, "event: %s\n", event)
	for _, line := range strings.Split(payload, "\n") {
		fmt.Fprintf(w, "data: %s\n", line)
	}
	fmt.Fprint(w, "\n") // blank line terminates the event
}

// ─────────────────────────── HTML rendering ────────────────────────────────

// handleIndex serves the page shell. The shell never changes; all live data
// arrives via the SSE connection and is swapped into the marked div by htmx.
func (l *lighthouse) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, pageShell)
}

// snapshotHTML renders the whole board (online nodes, handoffs, log) as one HTML
// fragment. Called under no lock by addEvent/broadcast callers? It locks itself.
func (l *lighthouse) snapshotHTML() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	var b strings.Builder

	// ── Online nodes ──
	b.WriteString(`<section><h2>🟢 Online nodes</h2>`)
	if len(l.nodes) == 0 {
		b.WriteString(`<p class="muted">none</p>`)
	} else {
		b.WriteString(`<table><tr><th>node</th><th>public address</th><th>last seen</th></tr>`)
		for id, reg := range l.nodes {
			fmt.Fprintf(&b, `<tr><td><b>%s</b></td><td><code>%s</code></td><td>%s ago</td></tr>`,
				esc(id), esc(reg.addr), since(now, reg.lastSeen))
		}
		b.WriteString(`</table>`)
	}
	b.WriteString(`</section>`)

	// ── Handoffs (newest first) ──
	b.WriteString(`<section><h2>🤝 Handoffs</h2>`)
	if len(l.handoffs) == 0 {
		b.WriteString(`<p class="muted">none yet</p>`)
	} else {
		b.WriteString(`<table><tr><th>from</th><th>to</th><th>state</th><th>when</th></tr>`)
		for i := len(l.handoffs) - 1; i >= 0; i-- {
			h := l.handoffs[i]
			state, when := handoffStatus(now, h)
			fmt.Fprintf(&b, `<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>`,
				esc(h.from), esc(h.to), state, when)
		}
		b.WriteString(`</table>`)
	}
	b.WriteString(`</section>`)

	// ── Activity log (newest first) ──
	b.WriteString(`<section><h2>📜 Activity log</h2>`)
	if len(l.events) == 0 {
		b.WriteString(`<p class="muted">quiet…</p>`)
	} else {
		b.WriteString(`<ul class="log">`)
		for i := len(l.events) - 1; i >= 0; i-- {
			e := l.events[i]
			fmt.Fprintf(&b, `<li><span class="ts">%s</span> %s</li>`,
				e.at.Format("15:04:05"), esc(e.msg))
		}
		b.WriteString(`</ul>`)
	}
	b.WriteString(`</section>`)

	return b.String()
}

// handoffStatus maps a handoff to a coloured badge + a timestamp string.
func handoffStatus(now time.Time, h *handoff) (badge, when string) {
	switch {
	case h.completed:
		return `<span class="badge ok">handed off ✔</span>`, since(now, h.completedAt) + " ago"
	case now.Sub(h.started) > handoffTimeout:
		// Never completed within the window — the punch failed (or both peers are
		// behind a NAT that won't cooperate). A retry re-arms this row to waiting.
		return `<span class="badge fail">timed out ✕</span>`, since(now, h.started) + " ago"
	case h.pickedUp:
		return `<span class="badge go">punching…</span>`, since(now, h.started) + " ago"
	default:
		return `<span class="badge wait">waiting…</span>`, since(now, h.started) + " ago"
	}
}

// since renders a short "Ns" / "Nm" age.
func since(now, t time.Time) string {
	d := now.Sub(t).Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
}

// esc HTML-escapes untrusted text (node ids come from clients).
func esc(s string) string { return html.EscapeString(s) }

// pageShell is the static HTML page. htmx + the SSE extension are loaded from a
// CDN; the single div connects to /events/stream and swaps in each "update".
const pageShell = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>quickmesh lighthouse</title>
<script src="https://unpkg.com/htmx.org@2.0.4"></script>
<script src="https://unpkg.com/htmx-ext-sse@2.2.2/sse.js"></script>
<style>
  body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; background:#0d1117; color:#c9d1d9; margin:0; padding:2rem; }
  h1 { color:#58a6ff; margin-top:0; }
  h2 { color:#79c0ff; font-size:1rem; border-bottom:1px solid #30363d; padding-bottom:.3rem; }
  section { margin-bottom:2rem; }
  table { border-collapse:collapse; width:100%; }
  th, td { text-align:left; padding:.35rem .6rem; border-bottom:1px solid #21262d; }
  th { color:#8b949e; font-weight:normal; }
  code { color:#a5d6ff; }
  .muted { color:#6e7681; }
  .log { list-style:none; padding:0; margin:0; }
  .log li { padding:.2rem 0; border-bottom:1px solid #161b22; }
  .ts { color:#6e7681; margin-right:.6rem; }
  .badge { padding:.1rem .5rem; border-radius:999px; font-size:.8rem; }
  .badge.ok   { background:#1a7f37; color:#fff; }
  .badge.go   { background:#9e6a03; color:#fff; }
  .badge.wait { background:#30363d; color:#c9d1d9; }
  .badge.fail { background:#da3633; color:#fff; }
</style>
</head>
<body>
  <h1>🔦 quickmesh lighthouse</h1>
  <p class="muted">Live public log of NAT hole-punching handoffs. Updates push automatically over SSE.</p>
  <!-- htmx opens the SSE connection and replaces this div's contents on every "update" event -->
  <div hx-ext="sse" sse-connect="/events/stream" sse-swap="update">
    <p class="muted">connecting…</p>
  </div>
</body>
</html>`
