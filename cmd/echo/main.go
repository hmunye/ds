package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/hmunye/ds/maelstrom"
)

type EchoRequest struct {
	Echo string `json:"echo"`
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()

	maelstrom.Handle(n, "echo", func(incoming maelstrom.Message[EchoRequest]) error {
		return maelstrom.Reply(n, incoming, "echo_ok", incoming.Body.Payload)
	})

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
