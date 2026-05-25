package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/hmunye/ds/broadcast"
	"github.com/hmunye/ds/maelstrom"
)

type EchoRequest struct {
	Echo string `json:"echo"`
}

type GenerateResponse struct {
	ID string `json:"id"`
}

type AddRequest struct {
	Delta int `json:"delta"`
}

type ReadResponse struct {
	Value int `json:"value"`
}

func GCounter(n *maelstrom.Node) {
	const counterKey = "count"
	kv := maelstrom.NewSeqKV(n)

	maelstrom.Handle(n, "add", func(incoming maelstrom.Message[AddRequest]) error {
		delta := incoming.Body.Payload.Delta

		for {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)

			val, err := maelstrom.KVRead[int](kv, ctx, counterKey)
			cancel()

			if err != nil {
				if e, ok := err.(*maelstrom.ErrorCode); !ok || *e != maelstrom.ErrKeyDoesNotExist {
					return err
				}
			}

			ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)

			err = maelstrom.KVCompareAndSwap[int](kv, ctx, counterKey, val, val+delta, true)
			cancel()

			if err != nil {
				if e, ok := err.(*maelstrom.ErrorCode); !ok || *e != maelstrom.ErrPreconditionFailed {
					return err
				} else {
					continue
				}
			}

			break
		}

		return maelstrom.Reply(n, incoming, "add_ok", maelstrom.EmptyPayload{})
	})

	maelstrom.Handle(n, "read", func(incoming maelstrom.Message[maelstrom.EmptyPayload]) error {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		val, err := maelstrom.KVRead[int](kv, ctx, counterKey)
		if err != nil {
			if e, ok := err.(*maelstrom.ErrorCode); !ok || *e != maelstrom.ErrKeyDoesNotExist {
				return err
			}
		}

		payload := ReadResponse{Value: val}

		return maelstrom.Reply(n, incoming, "read_ok", payload)
	})

}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()

	maelstrom.Handle(n, "echo", func(incoming maelstrom.Message[EchoRequest]) error {
		return maelstrom.Reply(n, incoming, "echo_ok", incoming.Body.Payload)
	})

	maelstrom.Handle(n, "generate", func(incoming maelstrom.Message[maelstrom.EmptyPayload]) error {
		payload := GenerateResponse{ID: n.GUID()}

		return maelstrom.Reply(n, incoming, "generate_ok", payload)
	})

	GCounter(n)

	//	broadcast.New(n).
	//		WithFanout(4).
	//		WithInterval(120 * time.Millisecond).
	//		Start()

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
