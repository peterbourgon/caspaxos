package caspaxos

import (
	"context"
	"testing"

	"github.com/go-kit/kit/log"
)

func TestConfigurationChange(t *testing.T) {
	// Build the cluster.
	var (
		logger = log.NewLogfmtLogger(testWriter{t})
		a1     = NewMemoryAcceptor("1")
		a2     = NewMemoryAcceptor("2")
		a3     = NewMemoryAcceptor("3")
		p1     = NewLocalProposer(1, log.With(logger, "p", 1), a1, a2, a3)
		p2     = NewLocalProposer(2, log.With(logger, "p", 2), a1, a2, a3)
		p3     = NewLocalProposer(3, log.With(logger, "p", 3), a1, a2, a3)
		ctx    = context.Background()
		val0   = "xxx"
	)

	// Declare some verification functions.
	growClusterWith := func(a Acceptor) {
		if err := GrowCluster(ctx, a, p1, p2, p3); err != nil {
			t.Fatalf("grow cluster with %q: %v", a.Address(), err)
		}
	}

	shrinkClusterWith := func(a Acceptor) {
		if err := ShrinkCluster(ctx, a, p1, p2, p3); err != nil {
			t.Fatalf("shrink cluster with %q: %v", a.Address(), err)
		}
	}

	verifyValue := func(a *MemoryAcceptor) {
		if want, have := val0, string(a.dumpValue()); want != have {
			t.Errorf("acceptor %s value: want %q, have %q", a.Address(), want, have)
		}
	}

	verifyReads := func() {
		for name, p := range map[string]Proposer{
			"p1": p1, "p2": p2, "p3": p3,
		} {
			if state, err := p.Propose(ctx, changeFuncRead); err != nil {
				t.Errorf("read via %s after shrink: %v", name, err)
			} else if want, have := val0, string(state); want != have {
				t.Errorf("read via %s after shrink: want %q, have %q", name, want, have)
			}
		}
	}

	// Set up an initial value.
	p2.Propose(ctx, changeFuncInitializeOnlyOnce(val0))

	// Add a new acceptor; it should have the correct value.
	a4 := NewMemoryAcceptor("4")
	growClusterWith(a4)
	verifyValue(a4)
	verifyReads()

	// Add another acceptor, same deal.
	a5 := NewMemoryAcceptor("5")
	growClusterWith(a5)
	verifyValue(a5)
	verifyReads()

	// Remove one of the initial acceptors; reads should still work.
	shrinkClusterWith(a1)
	verifyReads()

	// Remove one of the newly-added acceptors, same deal.
	shrinkClusterWith(a4)
	verifyReads()
}
