package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hmunye/ds/maelstrom"
)

type AddRequest struct {
	Delta int `json:"delta"`
}

type ReadResponse struct {
	Value int `json:"value"`
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	var mu sync.Mutex

	// Increments the counter by the requested delta. Rather than writing to a
	// single shared key, which would cause the entire cluster to contend on the
	// same CAS operation, each node writes exclusively to a key derived from
	// its assigned ID.
	//
	// This partitions ownership of the counter across nodes, ensuring that no
	// node ever writes to another node's key and making all increments
	// conflict-free. Together, these per-node counters form a G-Counter CRDT
	// (Conflict-free Replicated Data Type), where the global count is obtained
	// by summing the counters from all nodes.
	maelstrom.Handle(n, "add", func(incoming maelstrom.Message[AddRequest]) error {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		delta := incoming.Body.Payload.Delta

		mu.Lock()

		val, err := maelstrom.KVRead[int](kv, ctx, n.NodeID)

		mu.Unlock()

		if err != nil {
			if code, ok := err.(*maelstrom.ErrorCode); !ok || *code != maelstrom.ErrKeyDoesNotExist {
				return err
			}
		}

		mu.Lock()

		err = maelstrom.KVWrite[int](kv, ctx, n.NodeID, val+delta)

		mu.Unlock()

		if err != nil {
			return err
		}

		return maelstrom.Reply(n, incoming, "add_ok", maelstrom.EmptyPayload{})
	})

	// Reads the total value of the G-Counter by summing all per-node counters.
	//
	// Each node maintains its own counter under its assigned key. To compute
	// the global total, we read each node's counter and sum them together. The
	// CAS loop ensures that we read a consistent value under sequentially
	// consistent semantics: if another operation updates a node's counter
	// concurrently, the CAS may fail, and we retry until we successfully read
	// the latest value.
	//
	// This operation does not modify any other node's counters, and together
	// with the "add" handler, it allows the distributed G-Counter to maintain
	// conflict-free, eventually consistent counts across the cluster.
	maelstrom.Handle(n, "read", func(incoming maelstrom.Message[maelstrom.EmptyPayload]) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		total := 0

		for _, id := range n.NodeIDs {
			var node_val int

			for {
				val, err := maelstrom.KVRead[int](kv, ctx, id)
				if err != nil {
					if code, ok := err.(*maelstrom.ErrorCode); !ok || *code != maelstrom.ErrKeyDoesNotExist {
						return err
					}
				}

				err = maelstrom.KVCAS(kv, ctx, id, val, val, false)
				if err != nil {
					if code, ok := err.(*maelstrom.ErrorCode); ok && *code == maelstrom.ErrPreconditionFailed {
						continue
					}

					if code, ok := err.(*maelstrom.ErrorCode); !ok || *code != maelstrom.ErrKeyDoesNotExist {
						return err
					}
				}

				node_val = val
				break
			}

			total += node_val
		}

		payload := ReadResponse{Value: total}
		return maelstrom.Reply(n, incoming, "read_ok", payload)
	})

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
