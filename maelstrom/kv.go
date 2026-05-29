package maelstrom

import (
	"context"
	"log/slog"
)

const (
	seqKV = "seq-kv"
	linKV = "lin-kv"
)

// KV represents a key/value store service provided by `Maelstrom`.
//
// In distributed systems, a key/value store is the simplest form of shared
// state: a distributed map that nodes in the cluster can read from and write
// to concurrently. Each value is associated with a unique key, allowing clients
// to retrieve, update, or atomically modify data shared across the system.
//
// Distributed key/value stores are commonly used to coordinate state between
// nodes, replicate data across machines, and provide different consistency
// guarantees such as sequential consistency or linearizability.
type KV struct {
	ty string
	n  *Node
}

// NewSeqKV returns an initialized KV for the "seq-kv" service.
//
// With "seq-kv" (Sequentially Consistent), operations appear to execute in a
// single global order. Nodes may temporarily observe older values, but all
// nodes move forward through the same "timeline" of writes.
//
// For example, if node A writes 2 and later writes 4 to key "a", another node
// may still temporarily read 2 even after 4 has been written. However, once a
// node observes 4, it will never later observe 2 again.
//
// Because reads can be stale, operations like compare-and-swap (CAS) are needed
// when clients must atomically operate on the latest value.
func NewSeqKV(n *Node) *KV {
	return &KV{seqKV, n}
}

// NewLinKV returns an initialized KV for the "lin-kv" service.
//
// With "lin-kv" (Linearizable), operations appear to execute in a single global
// order, and once a write completes, all future reads observe that value or a
// newer one. Nodes never observe stale values after a more recent write has
// become visible.
//
// For example, if node A writes 2 and later writes 4 to key "a", once the write
// of 4 completes, no node will ever read 2 again. All reads immediately reflect
// the most recent completed write.
func NewLinKV(n *Node) *KV {
	return &KV{linKV, n}
}

// KVRead returns the value for a given key in the key/value store. Returns
// [ErrKeyDoesNotExist] if the key does not exist.
func KVRead[T any](kv *KV, ctx context.Context, key string) (T, error) {
	payload := kvReadRequest{key}

	ch, err := RPC[
		kvReadResponse[T],
		kvReadRequest,
	](kv.n, kv.ty, "read", payload)
	if err != nil {
		slog.Error("failed to send \"read\" request",
			slog.String("service", kv.ty),
			slog.Any("error", err))

		var zero T
		return zero, err
	}

	select {
	case msg := <-ch:
		if msg.Body.Code != nil {
			var zero T
			return zero, msg.Body.Code
		}

		return msg.Body.Payload.Value, nil
	case <-ctx.Done():
		slog.Warn("stopped waiting for \"read\" response",
			slog.String("service", kv.ty),
			slog.Any("error", ctx.Err()),
		)

		var zero T
		return zero, ctx.Err()
	}
}

// KVWrite overwrites the value for a given key in the key/value store.
func KVWrite[T any](kv *KV, ctx context.Context, key string, value T) (err error) {
	payload := kvWriteRequest[T]{key, value}

	ch, writeErr := RPC[EmptyPayload, kvWriteRequest[T]](kv.n, kv.ty, "write", payload)
	if err != nil {
		slog.Error("failed to send \"write\" request",
			slog.String("service", kv.ty),
			slog.Any("error", err))

		err = writeErr
		return
	}

	select {
	case msg := <-ch:
		if msg.Body.Code != nil {
			err = msg.Body.Code
		}
	case <-ctx.Done():
		slog.Warn("stopped waiting for \"write\" response",
			slog.String("service", kv.ty),
			slog.Any("error", ctx.Err()),
		)
		err = ctx.Err()
	}

	return
}

// KVCAS updates the value for a key if its current value matches the previous
// value. Creates the key if `createIfNotExists` is true.
//
// Returns [ErrPreconditionFailed] if the previous value does not match or
// [ErrKeyDoesNotExist] if the key does not exist.
func KVCAS[T any](kv *KV, ctx context.Context, key string, from, to T, createIfNotExists bool) (err error) {
	payload := kvCASRequest[T]{
		Key:               key,
		From:              from,
		To:                to,
		CreateIfNotExists: createIfNotExists,
	}

	ch, casErr := RPC[EmptyPayload, kvCASRequest[T]](kv.n, kv.ty, "cas", payload)
	if err != nil {
		slog.Error("failed to send \"cas\" request",
			slog.String("service", kv.ty),
			slog.Any("error", err))

		err = casErr
		return
	}

	select {
	case msg := <-ch:
		if msg.Body.Code != nil {
			err = msg.Body.Code
		}
	case <-ctx.Done():
		slog.Warn("stopped waiting for \"cas\" response",
			slog.String("service", kv.ty),
			slog.Any("error", ctx.Err()),
		)
		err = ctx.Err()
	}

	return
}
