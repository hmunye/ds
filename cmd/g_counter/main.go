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

	maelstrom.Handle(n, "read", func(incoming maelstrom.Message[maelstrom.EmptyPayload]) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		total := 0

		// Includes this node's ID.
		for _, id := range n.NodeIDs {
			var node_val int

			for {
				val, err := maelstrom.KVRead[int](kv, ctx, id)
				if err != nil {
					if code, ok := err.(*maelstrom.ErrorCode); !ok || *code != maelstrom.ErrKeyDoesNotExist {
						return err
					}
				}

				err = maelstrom.KVCompareAndSwap(kv, ctx, id, val, val, false)
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
