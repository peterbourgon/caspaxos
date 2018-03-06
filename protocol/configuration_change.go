package protocol

import (
	"context"
	"math/rand"

	"github.com/pkg/errors"
)

// Proposer models a concrete proposer.
type Proposer interface {
	Propose(ctx context.Context, key string, f ChangeFunc) (newState []byte, err error)

	AddAccepter(target Acceptor) error
	AddPreparer(target Acceptor) error
	RemovePreparer(target Acceptor) error
	RemoveAccepter(target Acceptor) error
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
func GrowCluster(ctx context.Context, target Acceptor, proposers ...Proposer) error {
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
	if _, err := proposer.Propose(ctx, zerokey, identity); err != nil {
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
func ShrinkCluster(ctx context.Context, target Acceptor, proposers ...Proposer) error {
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
	if _, err := proposer.Propose(ctx, zerokey, identity); err != nil {
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
