package main

import (
	"log/slog"
	"os"

	"github.com/hmunye/ds/maelstrom"
)

type EchoMessage struct {
	maelstrom.RPCMetadata
	Echo string `json:"echo"`
}

func main() {
	n := maelstrom.NewNode()

	maelstrom.Handle(n, "echo", func(incoming maelstrom.Message[EchoMessage]) error {
		payload := incoming.Body

		payload.Type = "echo_ok"
		payload.MsgID = n.NextMsgID()
		payload.InReplyTo = incoming.Body.MsgID

		return maelstrom.Reply(n, incoming, payload)
	})

	if err := n.Run(); err != nil {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
