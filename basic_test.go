package caspaxos

import (
	"context"
	"testing"

	"github.com/go-kit/kit/log"
)

func TestInitializeOnlyOnce(t *testing.T) {
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

	// Model an Initialize-Only-Once distributed register
	// (i.e. Synod) with an idempotent change function.
	const val0 = "val0"

	// The first proposal should work.
	have, err := p1.Propose(ctx, changeFuncInitializeOnlyOnce(val0))
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "val0", string(have); want != have {
		t.Errorf("want %q, have %q", want, have)
	}

	// If we make a read from anywhere, we should see the right thing.
	have, err = p2.Propose(ctx, changeFuncRead)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "val0", string(have); want != have {
		t.Errorf("want %q, have %q", want, have)
	}
	have, err = p3.Propose(ctx, changeFuncRead)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "val0", string(have); want != have {
		t.Errorf("want %q, have %q", want, have)
	}

	// Subsequent proposals should succeed but leave the value un-altered.
	have, err = p2.Propose(ctx, changeFuncInitializeOnlyOnce("alternative value"))
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "val0", string(have); want != have {
		t.Errorf("want %q, have %q", want, have)
	}
	have, err = p3.Propose(ctx, changeFuncInitializeOnlyOnce("must not succeed"))
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "val0", string(have); want != have {
		t.Errorf("want %q, have %q", want, have)
	}
}

func TestFastForward(t *testing.T) {
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

	// Set an initial value, and increment the ballot a few times.
	const val0 = "asdf"
	p1.Propose(ctx, changeFuncInitializeOnlyOnce(val0)) // ballot is incremented
	p1.Propose(ctx, changeFuncRead)                     // incremented again
	p1.Propose(ctx, changeFuncRead)                     // and again

	// Make sure reads thru other proposers succeed.
	for name, p := range map[string]*LocalProposer{
		"p2": p2, "p3": p3,
	} {
		if val, err := p.Propose(ctx, changeFuncRead); err != nil {
			t.Errorf("%s second read: got unexpected error: %v", name, err)
		} else if want, have := val0, string(val); want != have {
			t.Errorf("%s second read: want %q, have %q", name, want, have)
		}
	}
}
