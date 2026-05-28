package main

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
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

type Broker struct {
	logs map[string][]int
	lMu  sync.Mutex

	commits map[string]int
	cMu     sync.Mutex
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))

	return h.Sum64()
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()
	broker := Broker{logs: make(map[string][]int), commits: make(map[string]int)}

	maelstrom.Handle(n, "send", func(incoming maelstrom.Message[SendRequest]) error {
		key := incoming.Body.Payload.Key

		var offset int
		idx := hashKey(key) % uint64(len(n.NodeIDs))
		nodeID := n.NodeIDs[idx]

		if nodeID != n.NodeID {
			ch, err := maelstrom.RPC[SendResponse, SendRequest](n, nodeID, "send", incoming.Body.Payload)
			if err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			select {
			case snd_ok := <-ch:
				offset = snd_ok.Body.Payload.Offset
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			msg := incoming.Body.Payload.Msg

			broker.lMu.Lock()

			broker.logs[key] = append(broker.logs[key], msg)
			offset = len(broker.logs[key]) - 1

			broker.lMu.Unlock()
		}

		payload := SendResponse{Offset: offset}
		return maelstrom.Reply(n, incoming, "send_ok", payload)
	})

	maelstrom.Handle(n, "poll", func(incoming maelstrom.Message[PollRequest]) error {
		offsets := incoming.Body.Payload.Offsets
		msgs := make(map[string][][2]int, len(offsets))

		for key, offset := range offsets {
			idx := hashKey(key) % uint64(len(n.NodeIDs))
			nodeID := n.NodeIDs[idx]

			if nodeID != n.NodeID {
				payload := PollRequest{Offsets: map[string]int{key: offset}}

				ch, err := maelstrom.RPC[PollResponse, PollRequest](n, nodeID, "poll", payload)
				if err != nil {
					return err
				}

				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				select {
				case poll_ok := <-ch:
					msgs[key] = poll_ok.Body.Payload.Msgs[key]
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				broker.lMu.Lock()

				logs := broker.logs[key]
				key_msgs := make([][2]int, 0)

				l_len := min(offset+10, len(logs))

				for i, msg := range logs[offset:l_len] {
					key_msgs = append(key_msgs, [2]int{offset + i, msg})
				}

				msgs[key] = key_msgs

				broker.lMu.Unlock()
			}
		}

		payload := PollResponse{Msgs: msgs}
		return maelstrom.Reply(n, incoming, "poll_ok", payload)
	})

	maelstrom.Handle(n, "commit_offsets", func(incoming maelstrom.Message[CommitRequest]) error {
		offsets := incoming.Body.Payload.Offsets

		for key, offset := range offsets {
			idx := hashKey(key) % uint64(len(n.NodeIDs))
			nodeID := n.NodeIDs[idx]

			if nodeID != n.NodeID {
				payload := CommitRequest{Offsets: map[string]int{key: offset}}

				ch, err := maelstrom.RPC[maelstrom.EmptyPayload, CommitRequest](n, nodeID, "commit_offsets", payload)
				if err != nil {
					return err
				}

				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				select {
				case <-ch:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				broker.cMu.Lock()

				broker.commits[key] = offset

				broker.cMu.Unlock()
			}
		}

		return maelstrom.Reply(n, incoming, "commit_offsets_ok", maelstrom.EmptyPayload{})
	})

	maelstrom.Handle(n, "list_committed_offsets", func(incoming maelstrom.Message[ListCommitsRequest]) error {
		keys := incoming.Body.Payload.Keys
		offsets := make(map[string]int)

		for _, key := range keys {
			idx := hashKey(key) % uint64(len(n.NodeIDs))
			nodeID := n.NodeIDs[idx]

			if nodeID != n.NodeID {
				payload := ListCommitsRequest{Keys: []string{key}}

				ch, err := maelstrom.RPC[ListCommitsResponse, ListCommitsRequest](n, nodeID, "list_committed_offsets", payload)
				if err != nil {
					return err
				}

				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				select {
				case list_commit_ok := <-ch:
					offset, ok := list_commit_ok.Body.Payload.Offsets[key]
					if ok {
						offsets[key] = offset
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				broker.cMu.Lock()

				offset, ok := broker.commits[key]
				if ok {
					offsets[key] = offset
				}

				broker.cMu.Unlock()
			}
		}

		payload := ListCommitsResponse{Offsets: offsets}
		return maelstrom.Reply(n, incoming, "list_committed_offsets_ok", payload)
	})

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
