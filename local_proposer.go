package caspaxos

import (
	"context"
	"errors"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// Acceptor models a complete, uniquely-addressable acceptor.
//
// Here we have a little fun with names: use Acceptor (-or) as a noun, to model
// the whole composite acceptor, and Accepter (-er) as a verb, to model the
// second-phase "accept" responsibilities only.
type Acceptor interface {
	Addresser
	Preparer
	Accepter
}

// Addresser models something with a unique address.
type Addresser interface {
	Address() string // typically "protocol://host:port"
}

// Preparer models the first-phase responsibilities of an acceptor.
type Preparer interface {
	Prepare(ctx context.Context, key string, b Ballot) (value []byte, current Ballot, err error)
}

// Accepter models the second-phase responsibilities of an acceptor.
type Accepter interface {
	Accept(ctx context.Context, key string, b Ballot, value []byte) error
}

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

// LocalProposer performs the initialization by communicating with acceptors,
// and keep minimal state needed to generate unique increasing update IDs
// (ballot numbers).
type LocalProposer struct {
	mtx       sync.Mutex
	ballot    Ballot
	preparers map[string]Preparer
	accepters map[string]Accepter
	logger    log.Logger
}

// NewLocalProposer returns a usable Proposer uniquely identified by id.
// It communicates with the initial set of acceptors.
func NewLocalProposer(id uint64, logger log.Logger, initial ...Acceptor) *LocalProposer {
	p := &LocalProposer{
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
func (p *LocalProposer) Propose(ctx context.Context, key string, f ChangeFunc) (newState []byte, err error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	newState, err = p.propose(ctx, key, f)
	if err == ErrPrepareFailed {
		newState, err = p.propose(ctx, key, f) // allow a single retry, to hide fast-forwards
	}

	return newState, err
}

func (p *LocalProposer) propose(ctx context.Context, key string, f ChangeFunc) (newState []byte, err error) {
	// From the paper: "A client submits the change function to a proposer. The
	// proposer generates a ballot number B, by incrementing the current ballot
	// number's counter."
	//
	// Note that this ballot number increment must be tracked in our state.
	// rystsov: "I proved correctness for the case when each *attempt* has a
	// unique ballot number. [Otherwise] I would bet that linearizability may be
	// violated."
	b := p.ballot.inc()

	// Set up a logger, for debugging.
	logger := level.Debug(log.With(p.logger, "method", "Propose", "B", b))

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
				value, ballot, err := target.Prepare(ctx, key, b)
				results <- result{addr, value, ballot, err}
			}(addr, target)
		}

		// From the paper: "The proposer waits for F+1 confirmations. If they
		// all contain the empty value, then the proposer defines the current
		// state as nil; otherwise, it picks the value of the tuple with the
		// highest ballot number."
		var (
			quorum          = (len(p.preparers) / 2) + 1
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
				logger.Log("addr", result.addr, "result", "confirm", "ballot", result.ballot, "value", prettyPrint(result.value))
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
			return nil, ErrPrepareFailed
		}

		logger.Log("result", "success", "current_state", prettyPrint(currentState))
	}

	// We've successfully completed the prepare phase. From the paper: "The
	// proposer applies the change function to the current state. It will send
	// that new state along with the generated ballot number B (together, known
	// as an "accept" message) to the acceptors."
	newState = f(currentState)

	// Accept phase.
	{
		// Set up a sub-logger for this phase.
		logger := log.With(logger, "phase", "accept")
		logger.Log("current_state", prettyPrint(currentState), "new_state", prettyPrint(newState))

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
				err := target.Accept(ctx, key, b, newState)
				results <- result{addr, err}
			}(addr, target)
		}

		// From the paper: "The proposer waits for the F+1 confirmations."
		// Observe that once we've got confirmation from a quorum of accepters,
		// we ignore any subsequent messages.
		quorum := (len(p.accepters) / 2) + 1
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
			return nil, ErrAcceptFailed
		}

		// Log the success.
		logger.Log("result", "success", "new_state", prettyPrint(newState))
	}

	// Return the new state to the caller.
	return newState, nil
}

// AddAccepter adds the target acceptor to the pool of accepters used in the
// second phase of proposals. It's the first step in growing the cluster, which
// is a global process that needs to be orchestrated by an operator.
func (p *LocalProposer) AddAccepter(target Acceptor) error {
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
func (p *LocalProposer) AddPreparer(target Acceptor) error {
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
func (p *LocalProposer) RemovePreparer(target Acceptor) error {
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
func (p *LocalProposer) RemoveAccepter(target Acceptor) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if _, ok := p.accepters[target.Address()]; !ok {
		return ErrNotFound
	}
	delete(p.accepters, target.Address())
	return nil
}

type prettyPrint []byte

func (pp prettyPrint) String() string {
	if []byte(pp) == nil {
		return "Ø"
	}
	return string(pp)
}
