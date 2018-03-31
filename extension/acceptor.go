package extension

import (
	"context"

	"github.com/peterbourgon/caspaxos/protocol"
)

// Acceptor extends the core protocol acceptor.
type Acceptor interface {
	protocol.Addresser
	protocol.Preparer
	protocol.Accepter
	protocol.RejectRemover
	ValueWatcher
}

// ValueWatcher wraps protocol.StateWatcher. Implementations must perform
// (Version, Value) tuple deserialization of the received states to convert
// them to values before sending them on.
type ValueWatcher interface {
	Watch(ctx context.Context, key string, values chan<- []byte) error
}
