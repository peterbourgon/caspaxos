package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// ErrNotEqual indicates the tombstone value sent as part of a delete request
// doesn't correspond to the stored value we have for that key.
var ErrNotEqual = errors.New("not equal")

// MemoryAcceptor persists data in-memory.
type MemoryAcceptor struct {
	mtx    sync.Mutex
	addr   string
	ages   map[string]Age
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
		ages:   map[string]Age{},
		values: map[string]acceptedValue{},
		logger: logger,
	}
}

// Address implements Addresser.
func (a *MemoryAcceptor) Address() string {
	return a.addr
}

// Prepare implements the first-phase responsibilities of an acceptor.
func (a *MemoryAcceptor) Prepare(ctx context.Context, key string, age Age, b Ballot) (value []byte, current Ballot, err error) {
	defer func() {
		level.Debug(a.logger).Log(
			"method", "Prepare", "key", key, "age", age, "B", b,
			"success", err == nil, "return_ballot", current, "err", err,
		)
	}()

	a.mtx.Lock()
	defer a.mtx.Unlock()

	// From the GC section of the paper: "Acceptors [should] reject messages
	// from proposers if [the incoming age] is younger than the corresponding
	// age [that was previously accepted]."
	if incoming, existing := age, a.ages[age.ID]; incoming.youngerThan(existing) {
		return nil, zeroballot, AgeError{Incoming: incoming, Existing: existing}
	}

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
		return av.value, av.promise, ConflictError{Proposed: b, Existing: av.promise, ExistingKind: "promise"}
	}

	// Similarly, return a conflict if we already saw a greater ballot number.
	if av.accepted.greaterThan(b) {
		return av.value, av.accepted, ConflictError{Proposed: b, Existing: av.accepted, ExistingKind: "accepted"}
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
func (a *MemoryAcceptor) Accept(ctx context.Context, key string, age Age, b Ballot, value []byte) (err error) {
	defer func() {
		level.Debug(a.logger).Log(
			"method", "Accept", "key", key, "age", age, "B", b,
			"success", err == nil, "err", err,
		)
	}()

	a.mtx.Lock()
	defer a.mtx.Unlock()

	// From the GC section of the paper: "Acceptors [should] reject messages
	// from proposers if [the incoming age] is younger than the corresponding
	// age [that was previously accepted]."
	if incoming, existing := age, a.ages[age.ID]; incoming.youngerThan(existing) {
		return AgeError{Incoming: incoming, Existing: existing}
	}

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
		return ConflictError{Proposed: b, Existing: av.promise, ExistingKind: "promise"}
	}

	// Similarly.
	if av.accepted.greaterThan(b) {
		return ConflictError{Proposed: b, Existing: av.accepted, ExistingKind: "accepted"}
	}

	// If everything is satisfied, from the paper: "Erase the promise, mark the
	// received tuple as the accepted value."
	av.promise, av.accepted, av.value = zeroballot, b, value
	a.values[key] = av

	// From the paper: "Return a confirmation."
	return nil
}

// RejectByAge implements part of the garbage collection responsibilities of an
// acceptor. It updates the minimum age expected for the provided set of
// proposers.
func (a *MemoryAcceptor) RejectByAge(ctx context.Context, ages []Age) (err error) {
	defer func() {
		level.Debug(a.logger).Log(
			"method", "RejectByAge", "n", len(ages),
			"success", err == nil, "err", err,
		)
	}()

	a.mtx.Lock()
	defer a.mtx.Unlock()

	for _, age := range ages {
		target := a.ages[age.ID]
		target.Counter = age.Counter
		a.ages[age.ID] = target
	}

	return nil
}

// RemoveIfEqual implements part of the garbage collection responsibilities of
// an acceptor. It removes the key/value pair identified by key, if the stored
// value is equal to the tombstone's value.
func (a *MemoryAcceptor) RemoveIfEqual(ctx context.Context, key string, state []byte) (err error) {
	defer func() {
		level.Debug(a.logger).Log(
			"method", "RemoveIfEqual", "key", key,
			"success", err == nil, "err", err,
		)
	}()

	a.mtx.Lock()
	defer a.mtx.Unlock()

	// If the key is already deleted, we don't need to do anything.
	av, ok := a.values[key]
	if !ok {
		return nil // great, no work to do
	}

	if !bytes.Equal(av.value, state) {
		return ErrNotEqual
	}

	delete(a.values, key)
	return nil
}

func (a *MemoryAcceptor) dumpValue(key string) []byte {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	av, ok := a.values[key]
	if !ok {
		return nil
	}
	dst := make([]byte, len(av.value))
	copy(dst, av.value)
	return dst
}

// ConflictError is returned by acceptors when there's a ballot conflict.
type ConflictError struct {
	Proposed     Ballot
	Existing     Ballot
	ExistingKind string // e.g. promise or accepted
}

func (ce ConflictError) Error() string {
	return fmt.Sprintf("conflict: proposed ballot %s isn't greater than existing '%s' ballot %s", ce.Proposed, ce.ExistingKind, ce.Existing)
}

// AgeError is returned by acceptors when there's an age conflict.
type AgeError struct {
	Incoming Age
	Existing Age
}

func (ae AgeError) Error() string {
	return fmt.Sprintf("conflict: incoming age %s is younger than existing age %s", ae.Incoming, ae.Existing)
}
