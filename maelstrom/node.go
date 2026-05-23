// Package maelstrom provides utilities for building message-based nodes
// (distributed processes) on top of `Maelstrom`.
//
// Within a single process, threads share the same virtual-address space and can
// communicate through shared data structures synchronized with primitives such
// as mutexes. Communication between processes on the same machine typically
// relies on IPC (Inter-Process Communication) mechanisms such as sockets,
// pipes, or memory-mapped pages (`mmap`).
//
// In a distributed system, nodes communicate by exchanging messages over a
// network. Unlike threads, nodes cannot share memory or directly inspect each
// other's state, which introduces additional challenges:
//
//   - Messages may be lost
//   - Messages may be delayed
//   - Messages may be received out of order
//
// Since nodes cannot rely on shared state, every message must contain enough
// information for the receiver to interpret it independently. `Maelstrom`
// models this using a RPC (Remote Procedure Call) protocol, where nodes
// exchange self-contained JSON messages over STDIN and STDOUT. Each message
// includes a source (`src`), destination (`dest`), and `body` containing the
// payload of that request or response.
//
// Requests and responses are correlated using message identifiers such as
// `msg_id` and `in_reply_to`. A sender attaches a unique identifier to each
// request, and the receiver includes that identifier in its reply, allowing
// asynchronous communication without shared state.
//
// `Maelstrom` also defines structured error codes rather than human-readable
// messages. This allows nodes to distinguish between different classes of
// failure and make programmatic decisions about retries, recovery, or crashes.
package maelstrom

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

type handlerFunc func(json.RawMessage) error

// Node represents a single `Maelstrom` node.
type Node struct {
	// NodeID is the identifier of this node.
	NodeID string
	// NodeIDs lists all node IDs in the cluster.
	NodeIDs []string

	msgID    atomic.Uint32
	logger   *slog.Logger
	handlers map[string]handlerFunc
	mu       sync.Mutex
	out      io.Writer
}

// NewNode returns a new `Maelstrom` node which writes messages to STDOUT.
func NewNode() *Node {
	return newNode(os.Stdout)
}

func newNode(out io.Writer) *Node {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	n := &Node{
		out:      out,
		logger:   logger,
		handlers: make(map[string]handlerFunc),
	}

	n.handleInit()
	n.handleError()

	return n
}

// Run continuously reads `Maelstrom` messages from STDIN, dispatching each to
// its registered handler.
//
// An error is returned if an incoming message type has no registered handler.
func (n *Node) Run(ctx context.Context) error {
	return n.run(ctx, os.Stdin)
}

func (n *Node) run(ctx context.Context, reader io.Reader) (err error) {
	var wg sync.WaitGroup
	scanner := bufio.NewScanner(reader)
	lines := make(chan []byte)

	go func() {
		defer close(lines)

		for scanner.Scan() {
			lines <- []byte(scanner.Text())
		}
	}()

outer:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break outer
		case line, ok := <-lines:
			if !ok {
				err = scanner.Err()
				break outer
			}

			var meta struct {
				Body struct {
					Type string `json:"type"`
				} `json:"body"`
			}
			if err := json.Unmarshal(line, &meta); err != nil {
				slog.Error("failed to decode incoming message type", slog.Any("error", err))
				continue
			}

			ty := meta.Body.Type
			if ty == "" {
				slog.Error("failed to decode incoming message type", slog.String("error", "missing `type` field"))
				continue
			}

			callback, exists := n.handlers[ty]
			if !exists {
				err = fmt.Errorf("unregistered type for incoming message: %q", ty)
				break outer
			}

			wg.Go(func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("failed to process incoming message",
							slog.String("error", fmt.Sprint(r)),
							slog.String("type", ty),
						)
					}
				}()

				if err := callback(line); err != nil {
					slog.Error("failed to process incoming message",
						slog.Any("error", err),
						slog.String("type", ty),
					)
				}
			})
		}
	}

	wg.Wait()

	return
}

// GenerateID returns a globally unique identifier, provided calls to this
// function are interleaved with [Node.Reply].
func (n *Node) GenerateID() string {
	// Generating unique IDs within a program is straightforward. A monotonic
	// counter or a random `UUID` is usually sufficient. In distributed systems,
	// however, nodes must generate identifiers independently, without any
	// shared state.
	//
	// Using wall-clock time alone appears to work but fails in practice. Clocks
	// on different machines are not perfectly synchronized, so two nodes may
	// generate the same timestamp simultaneously. Also, clock skew and drift
	// can cause time to move backwards, breaking any monotonicity assumptions.
	//
	// Coordination-based approaches, where nodes agree on the next ID through
	// consensus or a centralized counter, work but introduce additional latency
	// and complexity. A round-trip between nodes is required for each generated
	// ID, and the system becomes unavailable under network partitions when
	// nodes cannot reach a quorum (i.e., the minimum number of nodes required
	// to make a decision).
	//
	// This implementation avoids both problems by combining two values that are
	// already guaranteed unique within different scopes:
	//
	// 	 - NodeID: unique across the entire cluster, assigned by `Maelstrom`
	// 	 - msgID:  monotonically increasing, unique to a single node
	//
	// Together, these values form a globally unique identifier across the
	// cluster without requiring coordination, shared state, or reliance on
	// wall-clock time. As a result, ID generation remains available during
	// network partitions, since each node can continue producing identifiers
	// independently.
	//
	// This approach would not be appropriate when:
	//
	//	 - IDs must be time-ordered or sortable by generation time
	//	 - IDs must be unpredictable (e.g., security tokens, session IDs)
	//	 - IDs must be compact (e.g., packed into a fixed-width integer)
	//
	// In those cases, alternative schemes such as Snowflake (timestamp + node
	// ID + sequence), `UUID` v4 (random), or consensus-based server-assigned
	// identifiers are more appropriate, each with different tradeoffs in
	// coordination, reliance on clocks, and probability of collision.
	return fmt.Sprintf("%s-%d", n.NodeID, n.msgID.Load())
}

func (n *Node) handleInit() {
	Handle(n, "init", func(incoming Message[initRequest]) error {
		n.init(incoming.Body.Payload.NodeID, incoming.Body.Payload.NodeIDs)

		return Reply(n, incoming, "init_ok", EmptyPayload{})
	})
}

func (n *Node) handleError() {
	Handle(n, "error", func(incoming Message[EmptyPayload]) error {
		var msg string
		if incoming.Body.Text != "" {
			msg = incoming.Body.Text
		} else {
			msg = incoming.Body.Code.Error()
		}

		slog.Error(
			"\"error\" message received",
			slog.String("error", msg),
			slog.Int("code", int(incoming.Body.Code)),
		)

		return nil
	})
}

func (n *Node) init(nodeID string, nodeIDs []string) {
	// Safe for concurrent use without locks: `Maelstrom` ensures no other
	// messages are delivered until the node responds to the "init" message.
	n.NodeID = nodeID
	n.NodeIDs = nodeIDs

	n.logger = n.logger.With(slog.String("node_id", n.NodeID))
	slog.SetDefault(n.logger)
}

// Handle registers a callback for processing incoming `Maelstrom` messages of
// the given type, which All handlers must be registered before calling
// [Node.Run].
func Handle[T any](n *Node, ty string, callback func(Message[T]) error) {
	n.handlers[ty] = func(line json.RawMessage) error {
		var msg Message[T]
		if err := json.Unmarshal(line, &msg); err != nil {
			return fmt.Errorf("failed to decode incoming message: %w", err)
		}

		return callback(msg)
	}
}

// Reply transmits a response for the given incoming message to STDOUT.
func Reply[T, U any](n *Node, incoming Message[T], ty string, payload U) error {
	msg := Message[U]{
		Src: n.NodeID,
		Dst: incoming.Src,
	}

	msg.Body.Type = ty
	msg.Body.MsgID = uint(n.msgID.Add(1))
	msg.Body.InReplyTo = incoming.Body.MsgID
	msg.Body.Payload = payload

	return Send(n, msg)
}

// Send transmits the given message to STDOUT.
func Send[T any](n *Node, msg Message[T]) error {
	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("failed to encode outgoing message",
			slog.Any("error", err),
			slog.String("type", msg.Body.Type),
		)
		return err
	}

	data = append(data, '\n')

	n.mu.Lock()
	defer n.mu.Unlock()

	_, err = n.out.Write(data)
	return err
}
