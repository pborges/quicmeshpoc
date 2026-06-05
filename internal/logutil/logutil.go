// Package logutil sets up per-process file logging with slog.
//
// Each program writes to its own file under ./logs:
//
//	lighthouse        -> logs/lighthouse.log
//	quicchat <nodeid> -> logs/<nodeid>.log
//
// This matters a lot for quicchat: it's a full-screen TUI, so anything printed
// to stdout/stderr would scribble over the interface. Routing logs to a file
// keeps the screen clean and gives us something to read after a crash.
package logutil

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
)

// New opens logs/<name>.log (creating the logs dir) and returns an slog.Logger
// plus a close func to defer. If alsoStderr is true, output is teed to stderr as
// well — handy for the lighthouse, which isn't a TUI. level sets the minimum
// level (use slog.LevelDebug while debugging).
func New(name string, level slog.Level, alsoStderr bool) (*slog.Logger, func() error, error) {
	if err := os.MkdirAll("logs", 0o755); err != nil {
		return nil, nil, fmt.Errorf("create logs dir: %w", err)
	}

	path := filepath.Join("logs", name+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", path, err)
	}

	var w io.Writer = f
	if alsoStderr {
		w = io.MultiWriter(f, os.Stderr)
	}

	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}))
	logger.Info("log opened", "file", path, "pid", os.Getpid())
	return logger, f.Close, nil
}

// Recover logs a panic (with stack trace) and swallows it so one misbehaving
// goroutine doesn't take down the whole process. Defer it at the top of every
// background goroutine:
//
//	go func() {
//	    defer logutil.Recover(logger, "registerLoop")
//	    ...
//	}()
//
// Swallowing (rather than re-panicking) is deliberate here: in a chat app, a bad
// peer stream shouldn't kill every other connection or wreck the terminal.
func Recover(logger *slog.Logger, where string) {
	if r := recover(); r != nil {
		logger.Error("panic recovered",
			"where", where,
			"panic", fmt.Sprint(r),
			"stack", string(debug.Stack()),
		)
	}
}
