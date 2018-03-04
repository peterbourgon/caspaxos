package caspaxos

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// MemoryAcceptor persists data in-memory.
type MemoryAcceptor struct {
	mtx    sync.Mutex
	addr   string
	values map[string]acceptedValue
	logger log.Logger
}

// An accepted value is associated with a key in an acceptor.
// In this way, one acceptor can manage many key-value pairs.
type acceptedValue struct {
	promise  Ballot
	accepted Ballot
	value    []byte
}

// The zero ballot can be used to clear promises.
var zeroballot Ballot

// NewMemoryAcceptor returns a usable in-memory acceptor.
// Useful primarily for testing.
func NewMemoryAcceptor(addr string, logger log.Logger) *MemoryAcceptor {
	return &MemoryAcceptor{
		addr:   addr,
		values: map[string]acceptedValue{},
		logger: logger,
	}
}

// Address implements Addresser.
func (a *MemoryAcceptor) Address() string {
	return a.addr
}

// Prepare implements the first-phase responsibilities of an acceptor.
func (a *MemoryAcceptor) Prepare(ctx context.Context, key string, b Ballot) (value []byte, current Ballot, err error) {
	defer func() {
		level.Debug(a.logger).Log(
			"method", "Prepare", "key", key, "B", b,
			"success", err == nil, "return_ballot", current, "err", err,
		)
	}()

	a.mtx.Lock()
	defer a.mtx.Unlock()

	// Select the promise/accepted/value tuple for this key.
	// A zero value is useful.
	av := a.values[key]

	// rystsov: "If a promise isn't empty during the prepare phase, we should
	// compare the proposed ballot number against the promise, and update the
	// promise if the promise is less."
	//
	// Here, we exploit the fact that a zero-value ballot number is less than
	// any non-zero-value ballot number.
	if av.promise.greaterThan(b) {
		return av.value, av.promise, ConflictError{Proposed: b, Existing: av.promise}
	}

	// Similarly, return a conflict if we already saw a greater ballot number.
	if av.accepted.greaterThan(b) {
		return av.value, av.accepted, ConflictError{Proposed: b, Existing: av.accepted}
	}

	// If everything is satisfied, from the paper: "persist the ballot number as
	// a promise."
	av.promise = b
	a.values[key] = av

	// From the paper: "and return a confirmation either with an empty value (if
	// it hasn't accepted any value yet) or with a tuple of an accepted value
	// and its ballot number."
	//
	// Note: if the acceptor hasn't accepted any value yet, the value is nil,
	// which we take to mean "an empty value". The receiver should interpret
	// value == nil as an empty value and ignore the returned ballot, which will
	// be zero.
	return av.value, av.accepted, nil
}

// Accept implements the second-phase responsibilities of an acceptor.
func (a *MemoryAcceptor) Accept(ctx context.Context, key string, b Ballot, value []byte) (err error) {
	defer func() {
		level.Debug(a.logger).Log(
			"method", "Accept", "key", key, "B", b,
			"success", err == nil, "err", err,
		)
	}()

	a.mtx.Lock()
	defer a.mtx.Unlock()

	// Select the promise/accepted/value tuple for this key.
	// A zero value is useful.
	av := a.values[key]

	// Return a conflict if it already saw a greater ballot number, either in
	// the promise or in the actual ballot number.
	//
	// rystsov: "During the accept phase, it's not necessary for the promise to
	// be equal to the passed ballot number. The promise simply cannot be
	// larger. The promise may even be empty; in this case, the request's ballot
	// number should be greater than the accepted ballot number."
	if av.promise.greaterThan(b) {
		return ConflictError{Proposed: b, Existing: av.promise}
	}

	// Similarly.
	if av.accepted.greaterThan(b) {
		return ConflictError{Proposed: b, Existing: av.accepted}
	}

	// If everything is satisfied, from the paper: "Erase the promise, mark the
	// received tuple as the accepted value."
	av.promise, av.accepted, av.value = zeroballot, b, value
	a.values[key] = av

	// From the paper: "Return a confirmation."
	return nil
}

func (a *MemoryAcceptor) dumpValue(key string) []byte {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	av := a.values[key]
	dst := make([]byte, len(av.value))
	copy(dst, av.value)
	return dst
}

// ConflictError is returned by acceptors when there's a ballot conflict.
type ConflictError struct {
	Proposed Ballot
	Existing Ballot
}

func (ce ConflictError) Error() string {
	return fmt.Sprintf("conflict: proposed ballot %s isn't greater than existing ballot %s", ce.Proposed, ce.Existing)
}
