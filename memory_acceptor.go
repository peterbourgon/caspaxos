package caspaxos

import (
	"context"
	"fmt"
	"sync"
)

// MemoryAcceptor persists data in-memory.
type MemoryAcceptor struct {
	mtx     sync.Mutex
	addr    string
	promise Ballot
	ballot  Ballot
	value   []byte
}

// NewMemoryAcceptor returns a usable in-memory acceptor.
// Useful primarily for testing.
func NewMemoryAcceptor(addr string) *MemoryAcceptor {
	return &MemoryAcceptor{addr: addr}
}

// Address implements Addresser.
func (a *MemoryAcceptor) Address() string {
	return a.addr
}

// Prepare implements the first-phase responsibilities of an acceptor.
func (a *MemoryAcceptor) Prepare(ctx context.Context, b Ballot) (value []byte, current Ballot, err error) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	// rystsov: "If a promise isn't empty during the prepare phase, we should
	// compare the proposed ballot number against the promise, and update the
	// promise if the promise is less."
	//
	// Here, we exploit the fact that a zero-value ballot number is less than
	// any non-zero-value ballot number.
	if a.promise.greaterThan(b) {
		return a.value, a.promise, ConflictError{Proposed: b, Existing: a.promise}
	}

	// Similarly, return a conflict if we already saw a greater ballot number.
	if a.ballot.greaterThan(b) {
		return a.value, a.ballot, ConflictError{Proposed: b, Existing: a.ballot}
	}

	// If everything is satisfied, from the paper: "persist the ballot number as
	// a promise."
	a.promise = b

	// From the paper: "and return a confirmation either with an empty value (if
	// it hasn't accepted any value yet) or with a tuple of an accepted value
	// and its ballot number."
	//
	// Note: if the acceptor hasn't accepted any value yet, a.value is nil,
	// which we take to mean "an empty value". The receiver must interpret
	// a.value == nil as an empty value and ignore the returned ballot, which
	// will be zero.
	return a.value, a.ballot, nil
}

// Accept models the second-phase responsibilities of an acceptor.
func (a *MemoryAcceptor) Accept(ctx context.Context, b Ballot, value []byte) error {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	// Return a conflict if it already saw a greater ballot number, either in
	// the promise or in the actual ballot number.
	//
	// rystsov: "During the accept phase, it's not necessary for the promise to
	// be equal to the passed ballot number. The promise simply cannot be
	// larger. The promise may even be empty; in this case, the request's ballot
	// number should be greater than the accepted ballot number."
	if a.promise.greaterThan(b) {
		return ConflictError{Proposed: b, Existing: a.promise}
	}

	// Similarly.
	if a.ballot.greaterThan(b) {
		return ConflictError{Proposed: b, Existing: a.ballot}
	}

	// If everything is satisfied, from the paper: "Erase the promise, mark the
	// received tuple as the accepted value."
	a.promise, a.ballot, a.value = Ballot{}, b, value

	// From the paper: "Return a confirmation."
	return nil
}

func (a *MemoryAcceptor) dumpValue() []byte {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	dst := make([]byte, len(a.value))
	copy(dst, a.value)
	return dst
}

// ConflictError is returned by acceptors when there's a ballot conflict.
type ConflictError struct {
	Proposed Ballot
	Existing Ballot
}

func (ce ConflictError) Error() string {
	return fmt.Sprintf("conflict: proposed ballot %+v isn't greater than existing ballot %+v", ce.Proposed, ce.Existing)
}
