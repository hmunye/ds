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
	entries map[int]int
	mu      sync.Mutex
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()
	kv := Store{entries: make(map[int]int)}

	// Executes a transaction consisting of a sequence of read and write
	// operations against a local in-memory key-value store. All operations are
	// applied under a mutex, providing single-node atomicity and ensuring no
	// concurrent transaction can observe intermediate state (serializable
	// execution at the node level).
	//
	// This guarantees read committed behavior locally: uncommitted writes are
	// never visible to other operations on the same node.
	//
	// After local execution, all writes are asynchronously replicated to other
	// nodes using best-effort, fire-and-forget propagation. Replicating the
	// full write set ensures write atomicity across replicas and prevents G1b
	// (intermediate read) anomalies, where only a subset of a transaction's
	// writes would be observed.
	//
	// This replication model provides eventual consistency across the cluster,
	// but does not provide global serializability due to the absence of
	// coordination or consensus.
	maelstrom.Handle(n, "txn", func(incoming maelstrom.Message[TXNMessage]) error {
		txn_out := make([][3]any, 0, len(incoming.Body.Payload.TXN))
		writes := make([][3]any, 0)

		kv.mu.Lock()

		for _, operation := range incoming.Body.Payload.TXN {
			op := operation[0].(string)
			key := int(operation[1].(float64))
			val := operation[2]

			switch op {
			case "r":
				txn_out = append(txn_out, [3]any{"r", key, kv.entries[key]})
			case "w":
				kv.entries[key] = int(val.(float64))
				txn_out = append(txn_out, operation)
				writes = append(writes, operation)
			}
		}

		kv.mu.Unlock()

		payload := TXNMessage{TXN: txn_out}

		err := maelstrom.Reply(n, incoming, "txn_ok", payload)
		if err != nil {
			return err
		}

		for _, nodeID := range n.NodeIDs {
			if nodeID != n.NodeID {
				payload := TXNMessage{
					TXN: writes,
				}
				_, err := maelstrom.RPC[TXNMessage, TXNMessage](n, nodeID, "txn", payload)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
