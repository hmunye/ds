package main

import (
	"log/slog"
	"os"

	"github.com/hmunye/ds/messenger"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(
		os.Stderr,
		&slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	))
	slog.SetDefault(logger)

	n := messenger.NewNode()

	messenger.RegisterHandler(
		n, "init",
		func(incoming messenger.Message[messenger.InitRequest]) error {
			n.Bootstrap(&incoming)

			body := messenger.InitResponse{
				Meta: messenger.Meta{
					Type:      "init_ok",
					InReplyTo: incoming.Body.MsgID,
				},
			}

			return messenger.Reply(n, incoming, body)
		})

	messenger.RegisterHandler(
		n, "echo",
		func(incoming messenger.Message[messenger.EchoMessage]) error {
			body := incoming.Body

			body.Type = "echo_ok"
			body.InReplyTo = incoming.Body.MsgID

			return messenger.Reply(n, incoming, body)
		})

	if err := n.Run(); err != nil {
		slog.Error("node failed", "error", err)
		os.Exit(1)
	}
}
