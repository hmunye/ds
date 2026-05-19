// Package messenger provides utilities for building message-based distributed
// systems on top of `Maelstrom`.
//
// Distributed systems communicate by exchanging messages. Unlike threads in a
// single process, distributed processes (nodes) cannot directly share memory,
// and this constraint fundamentally shapes how distributed algorithms are
// designed.
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
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

// Message represents a `Maelstrom` message.
type Message[T any] struct {
	Src  string `json:"src"`
	Dst  string `json:"dest"`
	Body T      `json:"body"`
}

// Meta contains common metadata fields shared by all `Maelstrom` payloads.
type Meta struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id,omitempty"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
}

// InitRequest represents the incoming payload for a `Maelstrom` "init" message
// sent at startup.
type InitRequest struct {
	Meta
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

// InitResponse represents the outgoing payload to a `Maelstrom` "init" message.
type InitResponse struct {
	Meta
}

// EchoMessage represents the incoming/outgoing message payload for a
// `Maelstrom` "echo" message.
type EchoMessage struct {
	Meta
	Echo string `json:"echo"`
}

type MessageHandler func(json.RawMessage) error

// Node represents a `Maelstrom` node.
type Node struct {
	NodeID  string
	NodeIDs []string

	nextMsgID atomic.Int32
	handlers  map[string]MessageHandler
	mu        sync.Mutex
}

// NewNode returns a new `Maelstrom` node. It must be initialized by handling
// an "init" message via [Node.Bootstrap].
func NewNode() *Node {
	node := &Node{
		handlers: make(map[string]MessageHandler),
	}

	node.nextMsgID.Store(1)
	return node
}

// Bootstrap initializes the node from an incoming [InitRequest], setting the
// node's identity and cluster membership.
func (n *Node) Bootstrap(incoming *Message[InitRequest]) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.NodeID = incoming.Body.NodeID
	n.NodeIDs = incoming.Body.NodeIDs
}

// Send sends a message with the given typed payload to the specified
// destination node via stdout.
func SendMessage[T any](n *Node, dst string, payload T) error {
	outgoing := Message[T]{Src: n.NodeID, Dst: dst, Body: payload}

	bytes, err := json.Marshal(outgoing)
	if err != nil {
		return err
	}

	n.nextMsgID.Store(n.nextMsgID.Load() + 1)

	_, err = fmt.Println(string(bytes))
	return err
}

// Reply sends a response given an incoming request and outgoing payload via
// stdout.
func Reply[I any, O any](n *Node, incoming Message[I], payload O) error {
	outgoing := Message[O]{
		Src:  incoming.Dst,
		Dst:  incoming.Src,
		Body: payload,
	}

	bytes, err := json.Marshal(outgoing)
	if err != nil {
		return err
	}

	n.nextMsgID.Store(n.nextMsgID.Load() + 1)

	_, err = fmt.Println(string(bytes))
	return err
}

// Handle registers a handler for the given message type.
//
// When a `Maelstrom` message with the message type is received, the registered
// handler will be invoked.
func RegisterHandler[T any](n *Node, msgType string, callback func(Message[T]) error) {
	n.handlers[msgType] = func(data json.RawMessage) error {
		var msg Message[T]
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}

		return callback(msg)
	}
}

// Run continuously reads messages from stdin, dispatching incoming messages
// to registered handlers.
//
// If no handler is registered for the "type" of the incoming message, an error
// is returned.
func (n *Node) Run() error {
	wg := sync.WaitGroup{}
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := []byte(scanner.Text())

		ty, err := parseMessageType(line)
		if err != nil {
			slog.Error("error parsing message", "error", err)
			continue
		}

		callback, ok := n.handlers[ty]
		if !ok {
			slog.Error("no handler for message type", slog.String("type", ty))
			continue
		}

		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("handler panic",
						slog.String("type", ty),
						slog.String("panic", fmt.Sprint(r)),
					)
				}
			}()

			if err := callback(line); err != nil {
				slog.Error("error processing message",
					"error", err,
					slog.String("type", ty),
				)
			}
		})
	}

	wg.Wait()

	return scanner.Err()
}

func parseMessageType(line []byte) (string, error) {
	var raw struct {
		Body json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(line, &raw); err != nil {
		return "", fmt.Errorf("failed to parse message: %w", err)
	}

	var meta struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw.Body, &meta); err != nil {
		return "", fmt.Errorf("failed to parse message type: %w", err)
	}

	if meta.Type == "" {
		return "", fmt.Errorf("message missing 'type' field")
	}

	return meta.Type, nil
}
