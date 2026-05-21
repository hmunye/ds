package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/hmunye/ds/maelstrom"
)

type EchoMessage struct {
	maelstrom.RPCMetadata
	Echo string `json:"echo"`
}

type GenerateRequest struct {
	maelstrom.RPCMetadata
}

type GenerateResponse struct {
	maelstrom.RPCMetadata
	ID string `json:"id"`
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := maelstrom.NewNode()

	maelstrom.Handle(n, "echo", func(incoming maelstrom.Message[EchoMessage]) error {
		payload := incoming.Body

		payload.Type = "echo_ok"

		// In a synchronous system, the relationship between a request and its
		// response is implicit in the call-stack, a function call blocks until
		// it returns. In an asynchronous distributed system, there is no call
		// stack to rely on. Requests and responses are independent messages
		// that may arrive out of order, be delayed, or never arrive at all.
		//
		// `msg_id` and `in_reply_to` solve this: the sending node assigns a
		// unique ID to each outgoing message, and the receiving node echoes it
		// back as `in_reply_to`. This allows the sending node to correlate each
		// response with its original request. Without this, a node receiving
		// messages concurrently would have no way of determining which response
		// belongs to which request.
		payload.MsgID = n.NextMsgID()
		payload.InReplyTo = incoming.Body.MsgID

		return maelstrom.Reply(n, incoming, payload)
	})

	// In a single-process system, generating unique IDs is trivial: a single
	// monotonic counter or `UUID` can suffice. In a distributed system, the
	// problem becomes harder due to multiple nodes independently generating IDs
	// concurrently, with the network also introducing uncertainty.
	//
	// A naive solution using wall-clock time fails for two reasons. First,
	// clocks across machines are not synchronized, two nodes can generate an
	// ID at the same millisecond. Second, clock skew and drift mean that time
	// can appear to move backwards, breaking any monotonicity assumptions.
	//
	// Coordination-based approaches, where nodes agree on the next ID through
	// consensus or a centralized counter, work but are expensive. They require
	// a round-trip between nodes for each generated ID, and become unavailable
	// under network partition when nodes cannot reach each other to agree.
	//
	// This implementation avoids both problems by combining two values that are
	// already guaranteed unique within different scopes:
	//
	// - NodeID: unique across the entire cluster, assigned by `Maelstrom`
	// - MsgID:  monotonically increasing, unique to a single node
	//
	// Combining them produces an ID that is globally unique across the cluster
	// with no coordination, shared state, or clock dependency. Network
	// partitions become irrelevant, as each node generates IDs independently.
	//
	// This approach would not be appropriate when:
	//
	// - IDs must be time-ordered or sortable by generation time
	// - IDs must be unpredictable (e.g., security tokens, session IDs)
	// - IDs must be compact (e.g., packed into a fixed-width integer)
	// - NodeID uniqueness is not guaranteed
	//
	// For those cases, approaches like Snowflake (timestamp + ID + sequence),
	// UUID v4 (random), or server-assigned IDs with consensus would be more
	// appropriate, each with their own tradeoffs around coordination, clock
	// dependency, and collision probability.
	maelstrom.Handle(n, "generate", func(incoming maelstrom.Message[GenerateRequest]) error {
		msgID := n.NextMsgID()
		id := fmt.Sprintf("%s-%d", n.NodeID, msgID)

		payload := GenerateResponse{
			maelstrom.RPCMetadata{
				Type:      "generate_ok",
				MsgID:     msgID,
				InReplyTo: incoming.Body.MsgID,
			},
			id,
		}

		return maelstrom.Reply(n, incoming, payload)
	})

	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node failed", slog.Any("error", err))
		os.Exit(1)
	}
}
