package httpapi

import (
	"context"
	"math/rand"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-kit/kit/log"

	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/protocol"
)

// TODO(pb): these tests are redundant with cmd/caspaxos-http tests; remove

func TestProposersAndAcceptors(t *testing.T) {
	// Build the cluster.
	var (
		logger = log.NewLogfmtLogger(testWriter{t})
		a1     = protocol.NewMemoryAcceptor("1", log.With(logger, "a", 1))
		a2     = protocol.NewMemoryAcceptor("2", log.With(logger, "a", 2))
		a3     = protocol.NewMemoryAcceptor("3", log.With(logger, "a", 3))
		p1     = protocol.NewMemoryProposer("1", log.With(logger, "p", 1))
		p2     = protocol.NewMemoryProposer("2", log.With(logger, "p", 2))
		p3     = protocol.NewMemoryProposer("3", log.With(logger, "p", 3))
	)

	// Wrap with HTTP adapters.
	var (
		ha1 = NewAcceptorServer(a1)
		ha2 = NewAcceptorServer(a2)
		ha3 = NewAcceptorServer(a3)
		hp1 = NewProposerServer(p1)
		hp2 = NewProposerServer(p2)
		hp3 = NewProposerServer(p3)
	)

	// Mount the HTTP adapters in servers.
	var (
		as1 = httptest.NewServer(ha1)
		as2 = httptest.NewServer(ha2)
		as3 = httptest.NewServer(ha3)
		ps1 = httptest.NewServer(hp1)
		ps2 = httptest.NewServer(hp2)
		ps3 = httptest.NewServer(hp3)
	)
	defer func() {
		as1.Close()
		as2.Close()
		as3.Close()
		ps1.Close()
		ps2.Close()
		ps3.Close()
	}()

	// Wrap with HTTP clients.
	var (
		ac1 = AcceptorClient{URL: mustParseURL(t, as1.URL)}
		ac2 = AcceptorClient{URL: mustParseURL(t, as2.URL)}
		ac3 = AcceptorClient{URL: mustParseURL(t, as3.URL)}
		pc1 = ProposerClient{URL: mustParseURL(t, ps1.URL)}
		pc2 = ProposerClient{URL: mustParseURL(t, ps2.URL)}
		pc3 = ProposerClient{URL: mustParseURL(t, ps3.URL)}
	)

	// HTTP client interface to HTTP server wrappers for proposers.
	var (
		confChangers = []protocol.ConfigurationChanger{pc1, pc2, pc3}
		extProposers = []extension.Proposer{pc1, pc2, pc3}
	)

	// Initialize.
	ctx := context.Background()
	if err := protocol.GrowCluster(ctx, ac1, confChangers); err != nil {
		t.Fatalf("first GrowCluster: %v", err)
	}
	if err := protocol.GrowCluster(ctx, ac2, confChangers); err != nil {
		t.Fatalf("second GrowCluster: %v", err)
	}
	if err := protocol.GrowCluster(ctx, ac3, confChangers); err != nil {
		t.Fatalf("third GrowCluster: %v", err)
	}

	// Do a CAS write.
	key := "foo"
	p := extProposers[rand.Intn(len(extProposers))]
	version, value, err := p.CAS(ctx, key, 0, []byte("val0"))
	if want, have := error(nil), err; want != have {
		t.Fatalf("CAS(%s): want error %v, have %v", key, want, have)
	}
	if want, have := uint64(1), version; want != have {
		t.Fatalf("CAS(%s): want version %d, have %d", key, want, have)
	}
	if want, have := "val0", string(value); want != have {
		t.Fatalf("CAS(%s): want value %q, have %q", key, want, have)
	}

	// Do some reads.
	for i, p := range extProposers {
		version, value, err := p.Read(ctx, key)
		if want, have := error(nil), err; want != have {
			t.Fatalf("Read(%s) %d: want error %v, have %v", key, i+1, want, have)
		}
		if want, have := uint64(1), version; want != have {
			t.Fatalf("Read(%s) %d: want version %d, have %d", key, i+1, want, have)
		}
		if want, have := "val0", string(value); want != have {
			t.Fatalf("Read(%s) %d: want value %q, have %q", key, i+1, want, have)
		}
	}

	// Make another write.
	p = extProposers[rand.Intn(len(extProposers))]
	version, value, err = p.CAS(ctx, key, 1, []byte("val1"))
	if want, have := error(nil), err; want != have {
		t.Fatalf("CAS(%s): want error %v, have %v", key, want, have)
	}
	if want, have := uint64(2), version; want != have {
		t.Fatalf("CAS(%s): want version %d, have %d", key, want, have)
	}
	if want, have := "val1", string(value); want != have {
		t.Fatalf("CAS(%s): want value %q, have %q", key, want, have)
	}
}

type testWriter struct{ t *testing.T }

func (tw testWriter) Write(p []byte) (int, error) {
	tw.t.Logf("%s", string(p))
	return len(p), nil
}

func mustParseURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
