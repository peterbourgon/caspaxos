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
	var (
		key, val0          = "k", "val0"
		initialize         = changeFuncInitializeOnlyOnce(val0)
		differentInit      = changeFuncInitializeOnlyOnce("alternate value")
		stillDifferentInit = changeFuncInitializeOnlyOnce("abcdefghijklmno")
	)

	// The first proposal should work.
	have, err := p1.Propose(ctx, key, initialize)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "val0", string(have); want != have {
		t.Errorf("want %q, have %q", want, have)
	}

	// If we make a read from anywhere, we should see the right thing.
	have, err = p2.Propose(ctx, key, changeFuncRead)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "val0", string(have); want != have {
		t.Errorf("want %q, have %q", want, have)
	}
	have, err = p3.Propose(ctx, key, changeFuncRead)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "val0", string(have); want != have {
		t.Errorf("want %q, have %q", want, have)
	}

	// Subsequent proposals should succeed but leave the value un-altered.
	have, err = p2.Propose(ctx, key, differentInit)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "val0", string(have); want != have {
		t.Errorf("want %q, have %q", want, have)
	}
	have, err = p3.Propose(ctx, key, stillDifferentInit)
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
	const key, val0 = "k", "asdf"
	p1.Propose(ctx, key, changeFuncInitializeOnlyOnce(val0)) // ballot is incremented
	p1.Propose(ctx, key, changeFuncRead)                     // incremented again
	p1.Propose(ctx, key, changeFuncRead)                     // and again

	// Make sure reads thru other proposers succeed.
	for name, p := range map[string]*LocalProposer{
		"p2": p2, "p3": p3,
	} {
		if val, err := p.Propose(ctx, key, changeFuncRead); err != nil {
			t.Errorf("%s second read: got unexpected error: %v", name, err)
		} else if want, have := val0, string(val); want != have {
			t.Errorf("%s second read: want %q, have %q", name, want, have)
		}
	}
}

func TestMultiKeyReads(t *testing.T) {
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

	// If we write a key through one proposer...
	p1.Propose(ctx, "k1", changeFuncInitializeOnlyOnce("v1"))

	// And a different key through another proposer...
	p2.Propose(ctx, "k2", changeFuncInitializeOnlyOnce("v2"))

	// Reads should work through a still-different proposer.
	if val, err := p3.Propose(ctx, "k1", changeFuncRead); err != nil {
		t.Errorf("read k1 via p3: %v", err)
	} else if want, have := "v1", string(val); want != have {
		t.Errorf("read k1 via p3: want %q, have %q", want, have)
	}
	if val, err := p3.Propose(ctx, "k2", changeFuncRead); err != nil {
		t.Errorf("read k2 via p3: %v", err)
	} else if want, have := "v2", string(val); want != have {
		t.Errorf("read k2 via p3: want %q, have %q", want, have)
	}

}
