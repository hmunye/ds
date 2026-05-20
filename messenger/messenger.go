// Package messenger provides utilities for building message-based distributed
// systems on top of `Maelstrom`.
//
// Distributed systems communicate by exchanging messages. Unlike threads in a
// single process, distributed processes (nodes) cannot directly share memory,
// fundamentally shaping how distributed algorithms are designed.
//
// In a traditional single-machine program, threads operate within the same
// virtual address space and can communicate through shared memory. Separate
// processes can also communicate locally using mechanisms such as sockets,
// pipes, or memory-mapped pages (`mmap`).
//
// In a distributed system, however, each program runs on a different machine
// with its own memory and execution environment. Communication must happen
// over a network, which introduces additional challenges:
//
//   - messages may be lost
//   - messages may be delayed
//   - messages may be transmitted out of order
//
// Because nodes cannot directly inspect each other's state, every message
// must contain enough information for the receiving node to process it
// independently.
package messenger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

// Message represents a `Maelstrom` message.
type Message[T hasMetadata] struct {
	Src  string `json:"src"`
	Dst  string `json:"dest"`
	Body T      `json:"body"`
}

// PayloadMetadata contains metadata fields shared by all `Maelstrom` message
// payloads.
type PayloadMetadata struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id,omitempty"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
}

func (p PayloadMetadata) payload() PayloadMetadata { return p }

type initRequest struct {
	PayloadMetadata
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

type initResponse struct {
	PayloadMetadata
}

// hasMetadata acts as a compile-time constraint ensuring each instantiated
// [Message.Body] embeds [PayloadMetadata].
type hasMetadata interface {
	payload() PayloadMetadata
}

// Node represents a `Maelstrom` node.
type Node struct {
	NodeID  string
	NodeIDs []string

	nextMsgID atomic.Int32
	out       io.Writer
	logger    *slog.Logger
	handlers  map[string]func(json.RawMessage) error
	mu        sync.Mutex
}

// MsgID returns the node's current message counter.
func (n *Node) MsgID() int {
	return int(n.nextMsgID.Load())
}

// NewNode returns a new `Maelstrom` node, whose state is logically finalized
// upon receiving a valid "init" message.
func NewNode() *Node {
	return newNode(os.Stdout)
}

func newNode(out io.Writer) *Node {
	logger := slog.New(slog.NewJSONHandler(
		os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
	slog.SetDefault(logger)

	n := &Node{
		out:      out,
		logger:   logger,
		handlers: make(map[string]func(json.RawMessage) error),
	}
	n.nextMsgID.Store(1)

	// Register default "init" handler.
	Handle(n, "init", func(incoming Message[initRequest]) error {
		n.configure(&incoming)

		body := initResponse{
			PayloadMetadata{
				Type:      "init_ok",
				InReplyTo: incoming.Body.MsgID,
			},
		}

		return Reply(n, incoming, body)
	})

	return n
}

// Reply transmits a message payload in response to an incoming request via
// STDOUT.
func Reply[In hasMetadata, Out hasMetadata](n *Node, incoming Message[In], payload Out) error {
	return Send(n, incoming.Src, payload)
}

// Send transmits a message payload to the specified destination via STDOUT.
func Send[Out hasMetadata](n *Node, dst string, payload Out) error {
	msg := Message[Out]{Src: n.NodeID, Dst: dst, Body: payload}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	n.nextMsgID.Add(1)

	n.mu.Lock()
	defer n.mu.Unlock()

	_, err = n.out.Write(append(data, '\n'))
	return err
}

// Handle registers a handler for messages of the given type, invoked whenever a
// `Maelstrom` message of that type is received. Handlers must be registered
// before calling [Node.Run].
func Handle[In hasMetadata](n *Node, msgType string, callback func(Message[In]) error) {
	n.handlers[msgType] = func(data json.RawMessage) error {
		var msg Message[In]
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}

		return callback(msg)
	}
}

// Run continuously reads `Maelstrom` messages from STDIN, dispatching each to
// its registered handler. An error is returned if a message is received with no
// corresponding handler.
func (n *Node) Run() error {
	return n.run(os.Stdin)
}

func (n *Node) run(reader io.Reader) error {
	var wg sync.WaitGroup
	var meta struct {
		Body struct {
			Type string `json:"type"`
		} `json:"body"`
	}

	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := []byte(scanner.Text())

		if err := json.Unmarshal(line, &meta); err != nil {
			slog.Error("failed to parse message", slog.Any("error", err))
			continue
		}

		ty := meta.Body.Type
		if ty == "" {
			slog.Error("failed to parse message", slog.String("error", "missing `type` field"))
			continue
		}

		callback, ok := n.handlers[ty]
		if !ok {
			return fmt.Errorf("no handler registered for message type: %q", ty)
		}

		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("handler panic",
						slog.String("panic", fmt.Sprint(r)),
						slog.String("type", ty),
					)
				}
			}()

			if err := callback(line); err != nil {
				slog.Error("failed to process message",
					slog.Any("error", err),
					slog.String("type", ty),
				)
			}
		})
	}

	wg.Wait()

	return scanner.Err()
}

func (n *Node) configure(incoming *Message[initRequest]) {
	// NOTE: Safe for concurrent use without a Mutex; Maelstrom guarantees no
	// other messages are delivered until the node responds to the "init"
	// message.

	n.NodeID = incoming.Body.NodeID
	n.NodeIDs = incoming.Body.NodeIDs
	n.logger = n.logger.With(slog.String("node_id", n.NodeID))
}
