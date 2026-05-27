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

type KafkaLog struct {
	logs map[string][]int
	lMu  sync.Mutex

	commits map[string]int
	cMu     sync.Mutex
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()

	kafka := KafkaLog{logs: make(map[string][]int), commits: make(map[string]int)}

	maelstrom.Handle(n, "send", func(incoming maelstrom.Message[SendRequest]) error {
		key := incoming.Body.Payload.Key
		msg := incoming.Body.Payload.Msg

		kafka.lMu.Lock()

		kafka.logs[key] = append(kafka.logs[key], msg)
		offset := len(kafka.logs[key]) - 1

		kafka.lMu.Unlock()

		payload := SendResponse{Offset: offset}
		return maelstrom.Reply(n, incoming, "send_ok", payload)
	})

	maelstrom.Handle(n, "poll", func(incoming maelstrom.Message[PollRequest]) error {
		offsets := incoming.Body.Payload.Offsets
		msgs := make(map[string][][2]int, len(offsets))

		kafka.lMu.Lock()

		for key, offset := range offsets {
			logs := kafka.logs[key]
			key_msgs := make([][2]int, 0)

			l_len := min(offset+10, len(logs))

			for i, msg := range logs[offset:l_len] {
				key_msgs = append(key_msgs, [2]int{offset + i, msg})
			}

			msgs[key] = key_msgs
		}

		kafka.lMu.Unlock()

		payload := PollResponse{Msgs: msgs}
		return maelstrom.Reply(n, incoming, "poll_ok", payload)
	})

	maelstrom.Handle(n, "commit_offsets", func(incoming maelstrom.Message[CommitRequest]) error {
		offsets := incoming.Body.Payload.Offsets

		kafka.cMu.Lock()

		for key, offset := range offsets {
			kafka.commits[key] = offset
		}

		kafka.cMu.Unlock()

		return maelstrom.Reply(n, incoming, "commit_offsets_ok", maelstrom.EmptyPayload{})
	})

	maelstrom.Handle(n, "list_committed_offsets", func(incoming maelstrom.Message[ListCommitsRequest]) error {
		keys := incoming.Body.Payload.Keys
		offsets := make(map[string]int)

		kafka.cMu.Lock()

		for _, key := range keys {
			offset, ok := kafka.commits[key]
			if ok {
				offsets[key] = offset
			}
		}

		kafka.cMu.Unlock()

		payload := ListCommitsResponse{Offsets: offsets}

		return maelstrom.Reply(n, incoming, "commit_offsets_ok", payload)
	})

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
