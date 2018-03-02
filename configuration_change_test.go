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
	)

	// Set up and confirm.
	const val0 = "xxx"
	p2.Propose(ctx, changeFuncInitializeOnlyOnce(val0))
	for _, p := range []Proposer{p1, p2, p3} {
		state, err := p.Propose(ctx, changeFuncRead)
		if err != nil {
			t.Fatal(err)
		}
		if want, have := val0, string(state); want != have {
			t.Fatalf("want %q, have %q", want, have)
		}
	}

	// Start the new acceptor and add it in.
	a4 := NewMemoryAcceptor("4")
	if err := GrowCluster(ctx, a4, p1, p2, p3); err != nil {
		t.Fatalf("grow cluster: %v", err)
	}

	// It should have the correct value.
	if want, have := val0, string(a4.dumpValue()); want != have {
		t.Errorf("first new acceptor value: want %q, have %q", want, have)
	}

	// Start another new acceptor and add it in.
	a5 := NewMemoryAcceptor("5")
	if err := GrowCluster(ctx, a5, p1, p2, p3); err != nil {
		t.Fatalf("grow cluster again: %v", err)
	}

	// It should have the correct value.
	if want, have := val0, string(a5.dumpValue()); want != have {
		t.Errorf("second new acceptor value: want %q, have %q", want, have)
	}

	// Remove one of the initial acceptors.
	if err := ShrinkCluster(ctx, a1, p1, p2, p3); err != nil {
		t.Fatalf("shrink cluster: %v", err)
	}

	// Reads should still work.
	for name, p := range map[string]Proposer{
		"p1": p1, "p2": p2, "p3": p3,
	} {
		if state, err := p.Propose(ctx, changeFuncRead); err != nil {
			t.Fatalf("read %s after first shrink: %v", name, err)
		} else if want, have := val0, string(state); want != have {
			t.Fatalf("read %s after first shrink: want %q, have %q", name, want, have)
		}
	}

	// Remove one of the newly-added acceptors.
	if err := ShrinkCluster(ctx, a4, p1, p2, p3); err != nil {
		t.Fatalf("shrink cluster again: %v", err)
	}

	// Reads should still work.
	for name, p := range map[string]Proposer{
		"p1": p1, "p2": p2, "p3": p3,
	} {
		if state, err := p.Propose(ctx, changeFuncRead); err != nil {
			t.Fatalf("read %s after second shrink: %v", name, err)
		} else if want, have := val0, string(state); want != have {
			t.Fatalf("read %s after second shrink: want %q, have %q", name, want, have)
		}
	}
}
