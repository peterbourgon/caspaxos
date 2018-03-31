package extension

import (
	"context"
	"fmt"

	"github.com/peterbourgon/caspaxos/protocol"
)

// Proposer models serializable methods of a proposer. Notably, the Watch method
// has the same signature but returns values instead of states; and the Propose
// method is dropped in favor of Read and CAS. The CAS protocol is taken from
// the paper, with a (Version, Value) tuple.
type Proposer interface {
	protocol.Addresser
	Read(ctx context.Context, key string) (version uint64, value []byte, err error)
	CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error)
	protocol.ConfigurationChanger
	protocol.FastForwarder
	protocol.AcceptorLister
}

// CASError indicates a conflict during CAS, likely a version conflict.
type CASError struct{ Err error }

// Error implements the error interface.
func (e CASError) Error() string {
	return fmt.Sprintf("CAS error: %v", e.Err)
}
