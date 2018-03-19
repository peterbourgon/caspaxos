package protocol

import (
	"context"
	"errors"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// ChangeFunc models client change proposals.
type ChangeFunc func(current []byte) (new []byte)

var (
	// ErrPrepareFailed indicates a failure during the first "prepare" phase.
	ErrPrepareFailed = errors.New("not enough confirmations during prepare phase; proposer ballot was fast-forwarded")

	// ErrAcceptFailed indicates a failure during the first "accept" phase.
	ErrAcceptFailed = errors.New("not enough confirmations during accept phase")

	// ErrDuplicate indicates the same acceptor was added twice.
	ErrDuplicate = errors.New("duplicate")

	// ErrNotFound indicates an attempt to remove a non-present acceptor.
	ErrNotFound = errors.New("not found")
)

// MemoryProposer (from the paper) "performs the initialization by communicating
// with acceptors, and keep minimal state needed to generate unique increasing
// update IDs (ballot numbers)."
type MemoryProposer struct {
	mtx       sync.Mutex
	age       Age
	ballot    Ballot
	preparers map[string]Preparer
	accepters map[string]Accepter
	logger    log.Logger
}

// NewMemoryProposer returns a usable Proposer uniquely identified by id.
// It communicates with the initial set of acceptors.
func NewMemoryProposer(id string, logger log.Logger, initial ...Acceptor) *MemoryProposer {
	p := &MemoryProposer{
		age:       Age{Counter: 0, ID: id},
		ballot:    Ballot{Counter: 0, ID: id},
		preparers: map[string]Preparer{},
		accepters: map[string]Accepter{},
		logger:    logger,
	}
	for _, target := range initial {
		p.preparers[target.Address()] = target
		p.accepters[target.Address()] = target
	}
	return p
}

// Propose a change from a client into the cluster.
func (p *MemoryProposer) Propose(ctx context.Context, key string, f ChangeFunc) (state []byte, b Ballot, err error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	state, b, err = p.propose(ctx, key, f, regularQuorum)
	if err == ErrPrepareFailed {
		// allow a single retry, to hide fast-forwards
		state, b, err = p.propose(ctx, key, f, regularQuorum)
	}

	return state, b, err
}

// quorumFunc declares how many of n nodes are required.
type quorumFunc func(n int) int

var (
	regularQuorum = func(n int) int { return (n / 2) + 1 } // i.e. F+1
	fullQuorum    = func(n int) int { return n }           // i.e. 2F+1
)

func (p *MemoryProposer) propose(ctx context.Context, key string, f ChangeFunc, qf quorumFunc) (state []byte, b Ballot, err error) {
	// From the paper: "A client submits the change function to a proposer. The
	// proposer generates a ballot number B, by incrementing the current ballot
	// number's counter."
	//
	// Note that this ballot number increment must be tracked in our state.
	// rystsov: "I proved correctness for the case when each *attempt* has a
	// unique ballot number. [Otherwise] I would bet that linearizability may be
	// violated."
	b = p.ballot.inc()

	// Set up a logger, for debugging.
	logger := level.Debug(log.With(p.logger, "method", "Propose", "key", key, "B", b))

	// If prepare is successful, we'll have an accepted current state.
	var currentState []byte

	// Prepare phase.
	{
		// Set up a sub-logger for this phase.
		logger := log.With(logger, "phase", "prepare")

		// We collect prepare results into this channel.
		type result struct {
			addr   string
			value  []byte
			ballot Ballot
			err    error
		}
		results := make(chan result, len(p.preparers))

		// Broadcast the prepare requests to the preparers.
		// (Preparers are just acceptors, serving their first role.)
		logger.Log("broadcast_to", len(p.preparers))
		for addr, target := range p.preparers {
			go func(addr string, target Preparer) {
				value, ballot, err := target.Prepare(ctx, key, p.age, b)
				results <- result{addr, value, ballot, err}
			}(addr, target)
		}

		// From the paper: "The proposer waits for F+1 confirmations. If they
		// all contain the empty value, then the proposer defines the current
		// state as nil; otherwise, it picks the value of the tuple with the
		// highest ballot number."
		var (
			quorum          = qf(len(p.preparers))
			biggestConfirm  Ballot
			biggestConflict Ballot
		)

		// Broadcast the prepare request to the preparers. Observe that once
		// we've got confirmation from a quorum of preparers, we ignore any
		// subsequent messages.
		for i := 0; i < cap(results) && quorum > 0; i++ {
			result := <-results
			if result.err != nil {
				// A conflict indicates that the proposed ballot is too old and
				// will be rejected; the largest conflicting ballot number
				// should be used to fast-forward the proposer's ballot number
				// counter in the case of total (quorum) failure.
				logger.Log("addr", result.addr, "result", "conflict", "ballot", result.ballot, "err", result.err)
				if result.ballot.greaterThan(biggestConflict) {
					biggestConflict = result.ballot
				}
			} else {
				// A confirmation indicates the proposed ballot will succeed,
				// and the preparer has accepted it as a promise; the largest
				// confirmed ballot number is used to select which returned
				// value will be chosen as the current value.
				logger.Log("addr", result.addr, "result", "confirm", "ballot", result.ballot)
				if result.ballot.greaterThan(biggestConfirm) {
					biggestConfirm, currentState = result.ballot, result.value
				}
				quorum--
			}
		}

		// If we finish collecting results and haven't achieved quorum, the
		// proposal fails. We should fast-forward our ballot number's counter to
		// the highest number we saw from the conflicted preparers, so a
		// subsequent proposal might succeed. We could try to re-submit the same
		// request with our updated ballot number, but for now let's leave that
		// responsibility to the caller.
		if quorum > 0 {
			logger.Log("result", "failed", "fast_forward_to", biggestConflict.Counter)
			p.ballot.Counter = biggestConflict.Counter // fast-forward
			return nil, b, ErrPrepareFailed
		}

		logger.Log("result", "success")
	}

	// We've successfully completed the prepare phase. From the paper: "The
	// proposer applies the change function to the current state. It will send
	// that new state along with the generated ballot number B (together, known
	// as an "accept" message) to the acceptors."
	newState := f(currentState)

	// Accept phase.
	{
		// Set up a sub-logger for this phase.
		logger := log.With(logger, "phase", "accept")

		// We collect accept results into this channel.
		type result struct {
			addr string
			err  error
		}
		results := make(chan result, len(p.accepters))

		// Broadcast accept messages to the accepters.
		logger.Log("broadcast_to", len(p.accepters))
		for addr, target := range p.accepters {
			go func(addr string, target Accepter) {
				err := target.Accept(ctx, key, p.age, b, newState)
				results <- result{addr, err}
			}(addr, target)
		}

		// From the paper: "The proposer waits for the F+1 confirmations."
		// Observe that once we've got confirmation from a quorum of accepters,
		// we ignore any subsequent messages.
		quorum := qf(len(p.accepters))
		for i := 0; i < cap(results) && quorum > 0; i++ {
			result := <-results
			if result.err != nil {
				logger.Log("addr", result.addr, "result", "conflict", "err", err)
			} else {
				logger.Log("addr", result.addr, "result", "confirm")
				quorum--
			}
		}

		// If we don't get quorum, I guess we must fail the proposal.
		if quorum > 0 {
			logger.Log("result", "failed", "err", "not enough confirmations")
			return nil, b, ErrAcceptFailed
		}

		// Log the success.
		logger.Log("result", "success")
	}

	// Return the new state to the caller.
	return newState, b, nil
}

// AddAccepter adds the target acceptor to the pool of accepters used in the
// second phase of proposals. It's the first step in growing the cluster, which
// is a global process that needs to be orchestrated by an operator.
func (p *MemoryProposer) AddAccepter(target Acceptor) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if _, ok := p.accepters[target.Address()]; ok {
		return ErrDuplicate
	}
	p.accepters[target.Address()] = target
	return nil
}

// AddPreparer adds the target acceptor to the pool of preparers used in the
// first phase of proposals. It's the third step in growing the cluster, which
// is a global process that needs to be orchestrated by an operator.
func (p *MemoryProposer) AddPreparer(target Acceptor) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if _, ok := p.preparers[target.Address()]; ok {
		return ErrDuplicate
	}
	p.preparers[target.Address()] = target
	return nil
}

// RemovePreparer removes the target acceptor from the pool of preparers used in
// the first phase of proposals. It's the first step in shrinking the cluster,
// which is a global process that needs to be orchestrated by an operator.
func (p *MemoryProposer) RemovePreparer(target Acceptor) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if _, ok := p.preparers[target.Address()]; !ok {
		return ErrNotFound
	}
	delete(p.preparers, target.Address())
	return nil
}

// RemoveAccepter removes the target acceptor from the pool of accepters used in
// the second phase of proposals. It's the third step in shrinking the cluster,
// which is a global process that needs to be orchestrated by an operator.
func (p *MemoryProposer) RemoveAccepter(target Acceptor) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if _, ok := p.accepters[target.Address()]; !ok {
		return ErrNotFound
	}
	delete(p.accepters, target.Address())
	return nil
}

// FullIdentityRead performs an identity read on the given key with a quorum
// size of 100%. It's used as part of the garbage collection process, to delete
// keys.
func (p *MemoryProposer) FullIdentityRead(ctx context.Context, key string) (state []byte, b Ballot, err error) {
	identity := func(x []byte) []byte { return x }
	state, b, err = p.propose(ctx, key, identity, fullQuorum)
	if err == ErrPrepareFailed {
		// allow a single retry, to hide fast-forwards
		state, b, err = p.propose(ctx, key, identity, fullQuorum)
	}
	return state, b, err
}

// FastForwardIncrement performs part (2b) responsibilities of the GC process.
func (p *MemoryProposer) FastForwardIncrement(ctx context.Context, key string, b Ballot) (Age, error) {
	// From the paper, this method should: "invalidate [the] cache associated
	// with the key ... fast-forward [the ballot number] counter to guarantee
	// that new ballot numbers are greater than the tombstone's ballot, and
	// increments the proposer's age."

	p.mtx.Lock()
	defer p.mtx.Unlock()

	// We have no cache associated with the key, because we don't implement the
	// One-round trip optimization from 2.2.1. So all we have to do is update
	// our counters. First, fast-forward the ballot.
	if p.ballot.Counter < (b.Counter + 1) {
		p.ballot.Counter = (b.Counter + 1)
	}

	// Then, increment our age.
	return p.age.inc(), nil
}
