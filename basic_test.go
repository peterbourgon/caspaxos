package caspaxos

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
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

func TestConcurrentCASWrites(t *testing.T) {
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

	// Define a function to pick a random proposer.
	randomProposer := func() Proposer {
		return []Proposer{p1, p2, p3}[rand.Intn(3)]
	}

	// Define a function to generate a bunch of values for a key.
	valuesFor := func(key string, n int) [][]byte {
		values := make([][]byte, n)
		for i := 0; i < n; i++ {
			values[i] = []byte(fmt.Sprintf("%s%d", key, i+1))
		}
		return values
	}

	// Set up some keys, and a sequence of values that we want for each.
	mutations := map[string][][]byte{
		"a": valuesFor("a", 997),
		"b": valuesFor("b", 998),
		"c": valuesFor("c", 999),
		"d": valuesFor("d", 1000),
	}

	// The compare-and-swap change function.
	cas := func(prev, next []byte) ChangeFunc {
		return func(current []byte) []byte {
			if (current == nil && prev == nil) || (bytes.Compare(current, prev) == 0) {
				return next // good
			}
			return current // bad
		}
	}

	// Define a worker function to CAS-write all the values in order.
	// Each proposal will go to a random proposer, to keep us honest.
	worker := func(key string, values [][]byte) {
		var prev []byte // initially no value is set
		for i, value := range values {
			var (
				p = randomProposer()
				f = cas(prev, value)
			)
			have, err := p.Propose(ctx, key, f)
			if err != nil {
				t.Errorf("%s worker: step %d (%s -> %s): %v", key, i+1, prettyPrint(prev), value, err)
				return
			}
			if want, have := string(value), string(have); want != have {
				t.Errorf("%s worker: step %d (%s -> %s): want %s, have %s", key, i+1, prettyPrint(prev), value, want, have)
				return
			}
			prev = have
		}
	}

	// Launch a CAS writer per key.
	// They'll do all their writes concurrently.
	var wg sync.WaitGroup
	for key, values := range mutations {
		wg.Add(1)
		go func(key string, values [][]byte) {
			defer wg.Done()
			worker(key, values)
		}(key, values)
	}
	wg.Wait()

	// This is kind of needless, but verify the final state with a read.
	for key, values := range mutations {
		var (
			final   = values[len(values)-1]
			have, _ = randomProposer().Propose(ctx, key, changeFuncRead)
		)
		if want, have := string(final), string(have); want != have {
			t.Errorf("%s: final state: want %s, have %s", key, want, have)
		}
	}
}
