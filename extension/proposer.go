package extension

import (
	"context"

	"github.com/peterbourgon/caspaxos/protocol"
)

// Proposer models serializable methods of a proposer.
// Notably, Propose is swapped for Read and CAS.
// CAS protocol is taken from the paper, with a (Version, Value) tuple.
type Proposer interface {
	protocol.Addresser
	Read(ctx context.Context, key string) (version uint64, value []byte, err error)
	CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error)
	protocol.ConfigurationChanger
	protocol.FastForwarder
	protocol.AcceptorLister
}
