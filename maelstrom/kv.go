package maelstrom

import (
	"context"
	"log/slog"
)

const (
	seqKV = "seq-kv"
)

// KV represents a key/value store service provided by `Maelstrom`.
type KV struct {
	ty string
	n  *Node
}

// NewSeqKV returns an initialized KV for the "seq-kv" service.
func NewSeqKV(n *Node) *KV {
	return &KV{seqKV, n}
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
		slog.Warn("timeout on waiting for \"read\" response", slog.String("service", kv.ty))

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
		slog.Warn("timeout on waiting for \"write\" response", slog.String("service", kv.ty))
	}

	return
}

// KVCompareAndSwap updates the value for a key if its current value matches the
// previous value. Creates the key if `createIfNotExists` is true.
//
// Returns [ErrPreconditionFailed] if the previous value does not match or
// [ErrKeyDoesNotExist] if the key does not exist.
func KVCompareAndSwap[T any](kv *KV, ctx context.Context, key string, from, to T, createIfNotExists bool) (err error) {
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
		slog.Warn("timeout on waiting for \"cas\" response", slog.String("service", kv.ty))
	}

	return
}
