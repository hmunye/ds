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

type ErrorCode int

const (
	// Indicates the requested operation could not be completed within a timeout
	// (indefinite).
	ErrTimeout ErrorCode = 0
	// Indicates a client sent an RPC request to a node which does not exist
	// (definite).
	ErrNodeNotFound = 1

	// Indicates a requested operation is not supported by the current
	// implementation. Helpful for stubbing out APIs during development
	// (definite).
	ErrNotSupported = 10
	// Indicates the operation definitely cannot be performed at this
	// time--perhaps because the server is in a read-only state, has not yet
	// been initialized, believes its peers to be down, and so on. Do not use
	// this error for indeterminate cases, when the operation may actually have
	// taken place (definite).
	ErrTemporarilyUnavailable = 11
	// Indicates the client's request did not conform to the server's
	// expectations, and could not possibly have been processed (definite).
	ErrMalformedRequest = 12
	// Indicates some kind of general, indefinite error occurred. Use this as a
	// catch-all for errors you can't otherwise categorize, or as a starting
	// point for your error handler: it's safe to return crash for every problem
	// by default, then add special cases for more specific errors later
	// (indefinite).
	ErrCrash = 13
	// Indicates some kind of general, definite error occurred. Use this as a
	// catch-all for errors you can't otherwise categorize, when you
	// specifically know that the requested operation has not taken place. For
	// instance, you might encounter an indefinite failure during the prepare
	// phase of a transaction: since you haven't started the commit process yet,
	// the transaction can't have taken place. It's therefore safe to return a
	// definite abort to the client (definite).
	ErrAbort = 14

	// Indicates an operation on a key which does not exist (assuming the
	// operation should not automatically create missing keys) (definite).
	ErrKeyDoesNotExist = 20
	// Indicates the creation of a key which already exists, and the server will
	// not overwrite it (definite).
	ErrKeyAlreadyExists = 21
	// Indicates the requested operation expected some conditions to hold, and
	// those conditions were not met. For instance, a compare-and-set operation
	// might assert that the value of a key is currently 5; if the value is 3,
	// the server would return precondition-failed (definite).
	ErrPreconditionFailed = 22

	// Indicates the requested transaction has been aborted because of a
	// conflict with another transaction. Servers need not return this error on
	// every conflict: they may choose to retry automatically instead
	// (definite).
	ErrTxnConflict = 30
)

func (e ErrorCode) Error() string {
	switch e {
	case ErrTimeout:
		return "timeout"
	case ErrNodeNotFound:
		return "node not found"

	case ErrNotSupported:
		return "not supported"
	case ErrTemporarilyUnavailable:
		return "temporarily unavailable"
	case ErrMalformedRequest:
		return "malformed request"

	case ErrCrash:
		return "crash"
	case ErrAbort:
		return "abort"

	case ErrKeyDoesNotExist:
		return "key does not exist"
	case ErrKeyAlreadyExists:
		return "key already exists"
	case ErrPreconditionFailed:
		return "precondition failed"

	case ErrTxnConflict:
		return "txn conflict"

	default:
		return "unknown error"
	}
}

// Message represents a `Maelstrom` message.
type Message[T hasMetadata] struct {
	Src  string `json:"src"`
	Dst  string `json:"dest"`
	Body T      `json:"body"`
}

// RPCMetadata contains metadata fields shared by all `Maelstrom` message
// payloads.
type RPCMetadata struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id,omitempty"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
}

func (meta RPCMetadata) MetaType() string   { return meta.Type }
func (meta RPCMetadata) MetaMsgID() int     { return meta.MsgID }
func (meta RPCMetadata) MetaInReplyTo() int { return meta.InReplyTo }

// ErrorMessage represents an "error" message exchanged between `Maelstrom`
// nodes.
type ErrorMessage struct {
	RPCMetadata
	Code ErrorCode `json:"code"`
	Text string    `json:"text,omitempty"`
}

type initRequest struct {
	RPCMetadata
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

type initResponse struct {
	RPCMetadata
}

// hasMetadata is used as a compile-time constraint ensuring each instantiated
// [Message.Body] embeds [RPCMetadata].
type hasMetadata interface {
	MetaType() string
	MetaMsgID() int
	MetaInReplyTo() int
}

type HandlerFunc func(json.RawMessage) error

// Node represents a `Maelstrom` node.
type Node struct {
	NodeID  string
	NodeIDs []string

	nextMsgID atomic.Int32
	out       io.Writer
	logger    *slog.Logger
	handlers  map[string]HandlerFunc
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
		handlers: make(map[string]HandlerFunc),
	}
	n.nextMsgID.Store(1)

	// Register default "init" handler.
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

	// Register default "error" handler.
	Handle(n, "error", func(incoming Message[ErrorMessage]) error {
		var msg string
		if incoming.Body.Text != "" {
			msg = incoming.Body.Text
		} else {
			msg = incoming.Body.Code.Error()
		}

		slog.Error(
			"received client error",
			slog.Int("code", int(incoming.Body.Code)),
			slog.String("error", msg),
		)

		return nil
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
	n.handlers[msgType] = func(line json.RawMessage) error {
		var msg Message[In]
		if err := json.Unmarshal(line, &msg); err != nil {
			slog.Error("failed to decode message", slog.Any("error", err))
			return err
		}

		return callback(msg)
	}
}

// Run continuously reads `Maelstrom` messages from STDIN, dispatching each to
// its registered handler. An error is returned if an incoming message type has
// no corresponding handler.
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
			slog.Error("failed to decode message type", slog.Any("error", err))
			continue
		}

		ty := meta.Body.Type
		if ty == "" {
			slog.Error("failed to decode message type", slog.String("error", "missing `type` field"))
			continue
		}

		callback, ok := n.handlers[ty]
		if !ok {
			err = fmt.Errorf("unregistered message type: %q", ty)
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
				slog.Error("failed to process message",
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

func (n *Node) configure(incoming *Message[initRequest]) {
	// NOTE: Safe for concurrent use without synchronization; Maelstrom
	// guarantees no other messages are delivered until the node responds
	// to the initial "init" message.
	n.NodeID = incoming.Body.NodeID
	n.NodeIDs = incoming.Body.NodeIDs

	n.logger = n.logger.With(slog.String("node_id", n.NodeID))
	slog.SetDefault(n.logger)
}
