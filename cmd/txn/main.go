package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hmunye/ds/maelstrom"
)

type TXNMessage struct {
	TXN [][3]any `json:"txn"`
}

type Store struct {
	kv map[int]int
	mu sync.Mutex
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()
	store := Store{kv: make(map[int]int)}

	maelstrom.Handle(n, "txn", func(incoming maelstrom.Message[TXNMessage]) error {
		txn_out := make([][3]any, 0, len(incoming.Body.Payload.TXN))

		store.mu.Lock()

		for _, operation := range incoming.Body.Payload.TXN {
			op := operation[0].(string)
			key := int(operation[1].(float64))
			val := operation[2]

			switch op {
			case "r":
				txn_out = append(txn_out, [3]any{"r", key, store.kv[key]})
			case "w":
				store.kv[key] = int(val.(float64))
				txn_out = append(txn_out, operation)

				for _, nodeID := range n.NodeIDs {
					if nodeID != n.NodeID {
						payload := TXNMessage{
							TXN: [][3]any{operation},
						}
						_, err := maelstrom.RPC[TXNMessage, TXNMessage](n, nodeID, "txn", payload)
						if err != nil {
							store.mu.Unlock()
							return err
						}
					}
				}
			}

		}

		store.mu.Unlock()

		payload := TXNMessage{TXN: txn_out}
		return maelstrom.Reply(n, incoming, "txn_ok", payload)
	})

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
