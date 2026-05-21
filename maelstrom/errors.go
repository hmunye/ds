package maelstrom

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
