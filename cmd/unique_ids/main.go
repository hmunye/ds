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

type GenerateResponse struct {
	ID string `json:"id"`
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()

	maelstrom.Handle(n, "generate", func(incoming maelstrom.Message[maelstrom.EmptyPayload]) error {
		payload := GenerateResponse{ID: n.GUID()}

		return maelstrom.Reply(n, incoming, "generate_ok", payload)
	})

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
