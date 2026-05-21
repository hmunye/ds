// Package maelstrom provides utilities for building message-based nodes
// (distributed processes) on top of `Maelstrom`.
//
// Nodes in a distributed system communicate by exchanging messages. Unlike
// threads in a single process, nodes cannot directly share memory, which
// fundamentally shapes how distributed algorithms are designed.
//
// In a traditional single-machine program, threads operate within the same
// memory space and can communicate through shared memory. Separate processes
// on the same host can communicate using mechanisms such as sockets, pipes,
// or memory-mapped pages (`mmap`).
//
// In a distributed system, each node runs on a different machine with its own
// memory and execution environment. Communication must happen over a network,
// which introduces additional challenges:
//
//   - messages may be lost
//   - messages may be delayed
//   - messages may be received out of order
//
// Because nodes cannot directly inspect each other's state, every message must
// contain enough information for the receiver to process it independently. The
// `Maelstrom` RPC (Remote Procedure Call) protocol facilitates this by using
// self-contained JSON objects sent over STDIN and STDOUT. Each message includes
// `src`, `dest`, and a `body` containing a `type` field and optional payload,
// allowing nodes to identify the sender, destination, and purpose of the
// communication without shared state.
//
// To handle asynchronous communication and match responses to requests,
// messages use IDs (e.g., `msg_id`, `in_reply_to`). When a node sends a
// request, it includes a unique identifier; when the receiver responds, it
// echoes that identifier back, allowing the sender to correlate the response
// with its original request.
//
// Additionally, `Maelstrom` defines a standard error format using structured
// error codes (similar to HTTP status codes) rather than human-readable text,
// enabling nodes to make programmatic decisions about retrying or failing
// requests based on the nature of the failure (e.g., indefinite or definite).
package maelstrom

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

type HandlerFunc func(json.RawMessage) error

// Node represents a `Maelstrom` node.
type Node struct {
	NodeID  string
	NodeIDs []string

	nextMsgID atomic.Uint64
	out       io.Writer
	logger    *slog.Logger
	handlers  map[string]HandlerFunc
	mu        sync.Mutex
}

// NextMsgID atomically increments and returns the next message ID.
func (n *Node) NextMsgID() uint64 {
	return uint64(n.nextMsgID.Add(1))
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
		handlers: make(map[string]HandlerFunc),
	}

	// Register default "init" handler.
	n.handleInit()

	// Register default "error" handler.
	n.handleError()

	return n
}

// Run continuously reads `Maelstrom` formatted messages from STDIN, dispatching
// each to its registered handler. An error is returned if an incoming message
// type has no corresponding handler registered.
func (n *Node) Run() error {
	return n.run(os.Stdin)
}

func (n *Node) run(reader io.Reader) (err error) {
	var wg sync.WaitGroup
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := []byte(scanner.Text())

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

		callback, ok := n.handlers[ty]
		if !ok {
			err = fmt.Errorf("unregistered type for incoming message: %q", ty)
			break
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
				slog.Error("failed to process incoming message",
					slog.Any("error", err),
					slog.String("type", ty),
				)
			}
		})
	}

	wg.Wait()

	if err == nil {
		err = scanner.Err()
	}

	return
}

func (n *Node) handleInit() {
	Handle(n, "init", func(incoming Message[initRequest]) error {
		n.configure(&incoming)

		body := initResponse{
			RPCMetadata{
				Type:      "init_ok",
				InReplyTo: incoming.Body.MsgID,
			},
		}

		return Reply(n, incoming, body)
	})
}

func (n *Node) handleError() {
	Handle(n, "error", func(incoming Message[ErrorMessage]) error {
		var msg string
		if incoming.Body.Text != "" {
			msg = incoming.Body.Text
		} else {
			msg = incoming.Body.Code.Error()
		}

		slog.Error(
			"incoming \"error\" message",
			slog.Int("code", int(incoming.Body.Code)),
			slog.String("error", msg),
		)

		return nil
	})
}

func (n *Node) configure(incoming *Message[initRequest]) {
	// NOTE: Safe for concurrent use without locks; `Maelstrom` ensures no other
	// messages are delivered until the node responds to the "init" message.
	n.NodeID = incoming.Body.NodeID
	n.NodeIDs = incoming.Body.NodeIDs

	n.logger = n.logger.With(slog.String("node_id", n.NodeID))
	slog.SetDefault(n.logger)
}

// Send transmits a payload to the specified destination node via STDOUT.
func Send[Out hasMetadata](n *Node, dst string, payload Out) error {
	msg := Message[Out]{Src: n.NodeID, Dst: dst, Body: payload}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	_, err = n.out.Write(append(data, '\n'))
	return err
}

// Reply transmits a payload in response to an incoming request via STDOUT.
func Reply[In hasMetadata, Out hasMetadata](n *Node, incoming Message[In], payload Out) error {
	return Send(n, incoming.Src, payload)
}

// Handle registers a handler for `Maelstrom` messages of the given type,
// invoked whenever a message of that type is received. Handlers must be
// registered before calling [Node.Run].
func Handle[In hasMetadata](n *Node, msgType string, callback func(Message[In]) error) {
	n.handlers[msgType] = func(line json.RawMessage) error {
		var msg Message[In]
		if err := json.Unmarshal(line, &msg); err != nil {
			slog.Error("failed to decode incoming message", slog.Any("error", err))
			return err
		}

		return callback(msg)
	}
}
