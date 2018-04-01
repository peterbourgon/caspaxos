package extension

import (
	"github.com/peterbourgon/caspaxos/protocol"
)

// Acceptor models serializable methods of the protocol.Acceptor. Since all
// Acceptor methods are trivially serializable, this means it's essentially a
// type alias for protocol.Acceptor.
type Acceptor interface {
	protocol.Acceptor
}

//
//
//

// SimpleAcceptor wraps a protocol.Acceptor and implements extension.Acceptor.
// Since protocol.Acceptor and extension.Acceptor are identical, this is
// trivially accomplished by embedding.
type SimpleAcceptor struct {
	protocol.Acceptor
}

var _ Acceptor = (*SimpleAcceptor)(nil)

// NewSimpleAcceptor returns a usable SimpleAcceptor wrapping the given
// protocol.Acceptor.
func NewSimpleAcceptor(target protocol.Acceptor) *SimpleAcceptor {
	return &SimpleAcceptor{target}
}
