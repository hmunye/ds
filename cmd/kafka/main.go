package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hmunye/ds/maelstrom"
)

type SendRequest struct {
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type SendResponse struct {
	Offset int `json:"offset"`
}

type PollRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	Msgs map[string][][2]int `json:"msgs"`
}

type CommitRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type ListCommitsRequest struct {
	Keys []string `json:"keys"`
}

type ListCommitsResponse struct {
	Offsets map[string]int `json:"offsets"`
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	maelstrom.Handle(n, "send", func(incoming maelstrom.Message[SendRequest]) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		key := incoming.Body.Payload.Key
		msg := incoming.Body.Payload.Msg

		var offset int

		for {
			logs, err := maelstrom.KVRead[[]int](kv, ctx, key)
			if err != nil {
				if code, ok := err.(*maelstrom.ErrorCode); !ok || *code != maelstrom.ErrKeyDoesNotExist {
					return err
				}
			}

			err = maelstrom.KVCompareAndSwap(kv, ctx, key, logs, append(logs, msg), true)
			if err != nil {
				if code, ok := err.(*maelstrom.ErrorCode); ok && *code == maelstrom.ErrPreconditionFailed {
					continue
				}

				if code, ok := err.(*maelstrom.ErrorCode); !ok || *code != maelstrom.ErrKeyDoesNotExist {
					return err
				}
			}

			offset = len(logs)
			break
		}

		payload := SendResponse{Offset: offset}
		return maelstrom.Reply(n, incoming, "send_ok", payload)
	})

	maelstrom.Handle(n, "poll", func(incoming maelstrom.Message[PollRequest]) error {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		offsets := incoming.Body.Payload.Offsets
		msgs := make(map[string][][2]int, len(offsets))

		for key, offset := range offsets {
			logs, err := maelstrom.KVRead[[]int](kv, ctx, key)
			if err != nil {
				return err
			}

			key_msgs := make([][2]int, 0)

			l_len := min(offset+10, len(logs))

			for i, msg := range logs[offset:l_len] {
				key_msgs = append(key_msgs, [2]int{offset + i, msg})
			}

			msgs[key] = key_msgs
		}

		payload := PollResponse{Msgs: msgs}
		return maelstrom.Reply(n, incoming, "poll_ok", payload)
	})

	maelstrom.Handle(n, "commit_offsets", func(incoming maelstrom.Message[CommitRequest]) error {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		offsets := incoming.Body.Payload.Offsets

		for key, offset := range offsets {
			err := maelstrom.KVWrite(kv, ctx, fmt.Sprintf("commit-%s", key), offset)
			if err != nil {
				return err
			}
		}

		return maelstrom.Reply(n, incoming, "commit_offsets_ok", maelstrom.EmptyPayload{})
	})

	maelstrom.Handle(n, "list_committed_offsets", func(incoming maelstrom.Message[ListCommitsRequest]) error {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		keys := incoming.Body.Payload.Keys
		offsets := make(map[string]int)

		for _, key := range keys {
			offset, err := maelstrom.KVRead[int](kv, ctx, fmt.Sprintf("commit-%s", key))
			if err != nil {
				return err
			}

			offsets[key] = offset
		}

		payload := ListCommitsResponse{Offsets: offsets}
		return maelstrom.Reply(n, incoming, "list_committed_offsets_ok", payload)
	})

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
