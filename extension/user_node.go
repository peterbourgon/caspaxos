package extension

import (
	"context"

	"github.com/peterbourgon/caspaxos/protocol"
)

// UserNode models the user API supported by the cluster.
type UserNode interface {
	protocol.Addresser
	Read(ctx context.Context, key string) (version uint64, value []byte, err error)
	CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error)
	Watch(ctx context.Context, key string, values chan<- []byte) error // TODO(pb): correct API?
}
