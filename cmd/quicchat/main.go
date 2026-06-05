// Command quicchat is the homogeneous peer in the quickmesh POC: every client
// is identical. It advertises itself to the lighthouse, downloads the list of
// other peers, and can hole-punch a direct QUIC connection to any of them for a
// simple terminal chat.
//
// Usage:
//
//	quicchat <nodeid> [-lighthouse https://host:port]
//
// The <nodeid> is just a human-friendly name you pick (e.g. "alice"). It's how
// other peers find and address you.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/pborges/quicmeshpoc/internal/logutil"
)

func main() {
	lighthouse := flag.String("lighthouse", "https://127.0.0.1:4433", "lighthouse base URL")
	insecure := flag.Bool("insecure", true, "skip TLS verification of the lighthouse cert (true for a self-signed lighthouse / LAN testing; set false to verify a real Let's Encrypt cert)")

	// The node id is a positional argument: `quicchat alice`. We want flags to
	// work in ANY position (e.g. `quicchat alice -lighthouse X`), but Go's flag
	// package stops at the first positional. So we parse in stages: parse leading
	// flags, take the next positional as the node id, then parse the rest.
	nodeID := ""
	rest := os.Args[1:]
	for {
		if err := flag.CommandLine.Parse(rest); err != nil {
			os.Exit(2)
		}
		rest = flag.Args()
		if len(rest) == 0 {
			break
		}
		if nodeID == "" {
			nodeID = rest[0] // first bare word is the node id
		}
		rest = rest[1:] // continue parsing any flags that followed it
	}

	if nodeID == "" {
		fmt.Fprintln(os.Stderr, "usage: quicchat <nodeid> [-lighthouse https://host:port] [-insecure=false]")
		os.Exit(1)
	}

	// All logging goes to logs/<nodeid>.log. We must NOT log to stdout/stderr:
	// this is a full-screen TUI and stray output would corrupt the display.
	logger, closeLog, err := logutil.New(nodeID, slog.LevelDebug, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to open log:", err)
		os.Exit(1)
	}
	defer closeLog()
	logger.Info("quicchat starting", "node", nodeID, "lighthouse", *lighthouse, "insecure", *insecure)

	// 1. Build the networking layer (opens the shared UDP socket + transport).
	mesh, err := NewMesh(nodeID, *lighthouse, *insecure, logger)
	if err != nil {
		logger.Error("failed to start mesh", "err", err)
		fmt.Fprintln(os.Stderr, "failed to start mesh:", err)
		os.Exit(1)
	}

	// Gracefully say goodbye to peers on exit: Close sends each peer a QUIC
	// CONNECTION_CLOSE so they drop us instantly instead of waiting out the idle
	// timeout. This runs on a normal quit (Run returns) because we don't os.Exit
	// on the happy path.
	defer mesh.Close()

	// 2. Build the Bubble Tea program around a fresh model.
	program := tea.NewProgram(newModel(mesh), tea.WithAltScreen())

	// Catch SIGINT/SIGTERM and ask the UI to quit, so program.Run returns and the
	// deferred mesh.Close above fires. (Bubble Tea already turns ctrl+c into a
	// quit; this also covers `kill` and ensures the terminal is restored.)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		program.Quit()
	}()

	// 3. Wire the Mesh's event callback to push tea.Msg values into the UI.
	//    program.Send is goroutine-safe, which is exactly what the Mesh's
	//    background loops need.
	mesh.SetEvents(func(msg any) { program.Send(msg) })

	// 4. Start the background networking loops, then run the UI (blocks).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mesh.Start(ctx)

	// A top-level guard: if the UI goroutine itself panics, restore the terminal
	// (tea.Program does this on its own panic, but we double-protect here) and
	// log it, so we never leave the user with a wedged, un-ctrl+c-able terminal.
	defer func() {
		if r := recover(); r != nil {
			program.Kill() // restores the terminal out of alt-screen/raw mode
			logger.Error("UI panic", "panic", r)
			fmt.Fprintln(os.Stderr, "quicchat crashed; see logs/"+nodeID+".log")
			os.Exit(1)
		}
	}()

	if _, err := program.Run(); err != nil {
		logger.Error("ui error", "err", err)
		fmt.Fprintln(os.Stderr, "ui error:", err)
		os.Exit(1)
	}
	logger.Info("quicchat exited cleanly")
}
