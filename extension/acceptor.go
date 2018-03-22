package extension

import (
	"github.com/peterbourgon/caspaxos/protocol"
)

// Acceptor is a type alias for a core protocol acceptor. It's here for symmetry
// with proposer. We may also want to add methods to it for cluster ops, in the
// future.
type Acceptor protocol.Acceptor
