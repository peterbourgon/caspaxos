package protocol

import (
	"context"
	"math/rand"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// Proposer models a concrete proposer.
type Proposer interface {
	// Propose is the primary API for clients. All changes including reads are
	// sent this way.
	Propose(ctx context.Context, key string, f ChangeFunc) (state []byte, ballot Ballot, err error)

	// These methods are for configuration changes.
	AddAccepter(target Acceptor) error
	AddPreparer(target Acceptor) error
	RemovePreparer(target Acceptor) error
	RemoveAccepter(target Acceptor) error

	// This method is for garbage collection, for deletes.
	FastForward(tombstone uint64) error
}

// Acceptor models a complete, uniquely-addressable acceptor.
//
// Here we have a little fun with names: use Acceptor (-or) as a noun, to model
// the whole composite acceptor, and Accepter (-er) as a verb, to model the
// second-phase "accept" responsibilities only.
type Acceptor interface {
	Addresser
	Preparer
	Accepter
	Remover
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

// Remover models the garbage collection responsibilities of an acceptor.
type Remover interface {
	RemoveIfEmpty(ctx context.Context, key string) error
}

// Assign special meaning to the zero/empty key "", which we use to increment
// ballot numbers for operations like changing cluster configuration.
const zerokey = ""

// Note: When growing (or shrinking) a cluster from an odd number of acceptors
// to an even number of acceptors, the implemented process is required. But when
// growing (or shrinking) a cluster from an even number of acceptors to an odd
// number of acceptors, an optimization is possible: we can first change the
// accept and prepare lists of all proposers, and then turn the acceptor on, and
// avoid the cost of a read.
//
// This is what's meant in this section of the paper: "The protocol for changing
// the set of acceptors from A_1...A_2F+2 to A_1...A_2F+3 [from even to odd] is
// more straightforward because we can treat a 2F+2 nodes cluster as a 2F+3
// nodes cluster where one node had been down from the beginning: [that process
// is] (1) Connect to each proposer and update its configuration to send the
// prepare and accept messages to the [second] A_1...A_2F+3 set of acceptors;
// (2) Turn on the A_2F+3 acceptor."
//
// I've chosen not to implement this for several reasons. First, cluster
// membership changes are rare and operator-driven, and so don't really benefit
// from the lower latency as much as reads or writes would. Second, the number
// of acceptors in the cluster is not known a priori, and can in theory drift
// between different proposers; calculating the correct value is difficult in
// itself, probably requiring asking some other source of authority. Third, in
// production environments, there's great value in having a consistent process
// for any cluster change; turning a node on at different points in that process
// depending on the cardinality of the node-set is fraught with peril.

// GrowCluster adds the target acceptor to the cluster of proposers.
func GrowCluster(ctx context.Context, target Acceptor, proposers []Proposer) error {
	// If we fail, try to leave the cluster in its original state.
	var undo []func()
	defer func() {
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
	}()

	// From the paper: "Connect to each proposer and update its configuration to
	// send the 'accept' messages to the [new] set of acceptors, and to require
	// F+2 confirmations during the 'accept' phase."
	for _, proposer := range proposers {
		if err := proposer.AddAccepter(target); err != nil {
			return errors.Wrap(err, "during grow step 1 (add accepter)")
		}
		undo = append(undo, func() { proposer.RemoveAccepter(target) })
	}

	// From the paper: "Pick any proposer and execute the identity state
	// transaction x -> x."
	var (
		identity = func(x []byte) []byte { return x }
		proposer = proposers[rand.Intn(len(proposers))]
	)
	if _, _, err := proposer.Propose(ctx, zerokey, identity); err != nil {
		return errors.Wrap(err, "during grow step 2 (identity read)")
	}

	// From the paper: "Connect to each proposer and update its configuration to
	// send 'prepare' messages to the [new] set of acceptors, and to require F+2
	// confirmations [during the 'prepare' phase]."
	for _, proposer := range proposers {
		if err := proposer.AddPreparer(target); err != nil {
			return errors.Wrap(err, "during grow step 3 (add preparer)")
		}
		undo = append(undo, func() { proposer.RemovePreparer(target) })
	}

	// Success! Kill the undo stack, and return.
	undo = []func(){}
	return nil
}

// ShrinkCluster removes the target acceptor from the cluster of proposers.
func ShrinkCluster(ctx context.Context, target Acceptor, proposers []Proposer) error {
	// If we fail, try to leave the cluster in its original state.
	var undo []func()
	defer func() {
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
	}()

	// From the paper: "The same steps [for growing the cluster] should be
	// executed in the reverse order to reduce the size of the cluster."

	// So, remove it as a preparer.
	for _, proposer := range proposers {
		if err := proposer.RemovePreparer(target); err != nil {
			return errors.Wrap(err, "during shrink step 1 (remove preparer)")
		}
		undo = append(undo, func() { proposer.AddPreparer(target) })
	}

	// Execute a no-op read.
	var (
		identity = func(x []byte) []byte { return x }
		proposer = proposers[rand.Intn(len(proposers))]
	)
	if _, _, err := proposer.Propose(ctx, zerokey, identity); err != nil {
		return errors.Wrap(err, "during shrink step 2 (identity read)")
	}

	// And then remove it as an accepter.
	for _, proposer := range proposers {
		if err := proposer.RemoveAccepter(target); err != nil {
			return errors.Wrap(err, "during shrink step 3 (remove accepter)")
		}
		undo = append(undo, func() { proposer.AddAccepter(target) })
	}

	// Done.
	undo = []func(){}
	return nil
}

// ErrStateUpdated indicates a key was re-written to a nonempty value before the
// garbage collection process could completely remove it. The new value has
// "won" and the client should take some corrective action, likely re-issuing a
// delete.
var ErrStateUpdated = errors.New("state updated before garbage collection could complete")

// GarbageCollect removes an empty key as described in section 3.1 "How to
// delete a record" in the paper. It will continue until the key is successfully
// garbage collected, or the context is canceled.
func GarbageCollect(ctx context.Context, key string, delay time.Duration, proposers []Proposer, acceptors []Acceptor, logger log.Logger) error {
	// From the paper, this process: "(a) Replicates an empty value to all nodes
	// by executing the identity transform with 2F+1 quorum size. Reschedules
	// itself if at least one node is down."
	for {
		tombstone, err := gcBroadcastIdentity(ctx, key, proposers)
		if err == context.Canceled {
			return err // fatal
		}
		if err != nil {
			retry := time.Second // nonfatal
			level.Debug(logger).Log("broadcast", "failed", "err", err, "retry_in", retry.String())
			select {
			case <-time.After(retry):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// From the paper: "(b) For each proposer, fast-forwards its counter to
		// generate ballot numbers greater than the tombstone's number."
		if err := gcFastForward(ctx, tombstone, proposers); err != nil {
			return err
		}

		// From the paper: "(c) Wait some time to make sure that all in-channel
		// messages with lesser ballots were delivered."
		time.Sleep(delay)

		// From the paper: "(d) [Each] acceptor [should] remove the register if
		// its value is empty."
		if err := gcRemoveIfEmpty(ctx, key, acceptors); err != nil {
			return err
		}

		// Done!
		return nil
	}
}

func gcBroadcastIdentity(ctx context.Context, key string, proposers []Proposer) (tombstone uint64, err error) {
	// The identity read change function.
	identity := func(x []byte) []byte { return x }

	// Collect results in the results chan.
	type result struct {
		state  []byte
		ballot Ballot
		err    error
	}
	results := make(chan result, len(proposers))

	// Broadcast to all proposers.
	for _, p := range proposers {
		go func(p Proposer) {
			state, ballot, err := p.Propose(ctx, key, identity)
			results <- result{state, ballot, err}
		}(p)
	}

	// Wait for every result. Any error is a failure.
	for i := 0; i < cap(results); i++ {
		result := <-results
		if result.err != nil {
			return 0, result.err
		}
		if len(result.state) != 0 {
			return 0, ErrStateUpdated
		}
		if result.ballot.Counter > tombstone {
			tombstone = result.ballot.Counter
		}
	}

	// The biggest of all the counters becomes the tombstone.
	return tombstone, nil
}

func gcFastForward(ctx context.Context, tombstone uint64, proposers []Proposer) error {
	// Collect results in the results chan.
	results := make(chan error, len(proposers))

	// Broadcast the fast-forward request.
	for _, p := range proposers {
		go func(p Proposer) {
			results <- p.FastForward(tombstone)
		}(p)
	}

	// Verify results.
	for i := 0; i < cap(results); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	// Good.
	return nil
}

func gcRemoveIfEmpty(ctx context.Context, key string, acceptors []Acceptor) error {
	// Broadcast the remove-if-empty request.
	results := make(chan error, len(acceptors))
	for _, a := range acceptors {
		go func(a Acceptor) {
			results <- a.RemoveIfEmpty(ctx, key)
		}(a)
	}

	// We need a quorum of 100%.
	for i := 0; i < cap(results); i++ {
		if err := <-results; err != nil {
			return err // fatal
		}
	}

	// Good.
	return nil
}
