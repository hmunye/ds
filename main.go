package main

import (
	"log/slog"
	"os"

	"github.com/hmunye/ds/messenger"
)

type EchoMessage struct {
	messenger.PayloadMetadata
	Echo string `json:"echo"`
}

func main() {
	n := messenger.NewNode()

	messenger.Handle(n, "echo", func(incoming messenger.Message[EchoMessage]) error {
		outgoing := incoming.Body

		outgoing.Type = "echo_ok"
		outgoing.MsgID = n.MsgID()
		outgoing.InReplyTo = incoming.Body.MsgID

		return messenger.Reply(n, incoming, outgoing)
	})

	if err := n.Run(); err != nil {
		slog.Error("node execution failed", slog.Any("error", err))
		os.Exit(1)
	}
}
