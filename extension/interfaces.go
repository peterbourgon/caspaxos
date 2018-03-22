package extension

import (
	"context"

	"github.com/peterbourgon/caspaxos/protocol"
)

// Proposer models serializable methods of a proposer.
// Notably, Propose is swapped for Read and CAS.
type Proposer interface {
	protocol.Addresser
	Read(ctx context.Context, key string) (version uint64, value []byte, err error)
	CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error)
	protocol.ConfigurationChanger
	protocol.FastForwarder
	protocol.AcceptorLister
}

// Acceptor is a type alias for a core protocol acceptor. It's here for symmetry
// with proposer. We may also want to add methods to it for cluster ops, in the
// future.
type Acceptor protocol.Acceptor

// UserNode models the user API supported by the cluster.
type UserNode interface {
	protocol.Addresser
	Read(ctx context.Context, key string) (version uint64, value []byte, err error)
	CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error)
	Watch(ctx context.Context, key string, values chan<- []byte) error // TODO(pb): correct API?
}
