package mockcluster

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/protocol"
)

func TestMockClusterUX(t *testing.T) {
	// Create all the components, disconnected at first.
	var (
		logger = log.NewLogfmtLogger(testWriter{t})
		ctx    = context.Background()
		c      = NewCluster()
		a1     = extension.NewSimpleAcceptor(protocol.NewMemoryAcceptor("a1", log.With(logger, "a", 1)))
		a2     = extension.NewSimpleAcceptor(protocol.NewMemoryAcceptor("a2", log.With(logger, "a", 2)))
		a3     = extension.NewSimpleAcceptor(protocol.NewMemoryAcceptor("a3", log.With(logger, "a", 3)))
		p1     = extension.NewSimpleProposer(protocol.NewMemoryProposer("p1", log.With(logger, "p", 1)))
		p2     = extension.NewSimpleProposer(protocol.NewMemoryProposer("p2", log.With(logger, "p", 2)))
		p3     = extension.NewSimpleProposer(protocol.NewMemoryProposer("p3", log.With(logger, "p", 3)))
		o1     = extension.NewClusterOperator2("o1", c, log.With(logger, "o", 1))
		o2     = extension.NewClusterOperator2("o2", c, log.With(logger, "o", 2))
	)

	// Have each node join the cluster.
	for _, target := range []interface{}{a1, a2, a3, p1, p2, p3, o1, o2} {
		c.Join(target)
	}

	// The operator should see everything.
	for i, o := range []extension.Operator{o1, o2} {
		a, _ := o.ListAcceptors(ctx)
		sort.Strings(a)
		if want, have := []string{"a1", "a2", "a3"}, a; !reflect.DeepEqual(want, have) {
			t.Errorf("Operator %d: ListAcceptors: want %v, have %v", i+1, want, have)
		}
		p, _ := o.ListProposers(ctx)
		sort.Strings(p)
		if want, have := []string{"p1", "p2", "p3"}, p; !reflect.DeepEqual(want, have) {
			t.Errorf("Operator %d: ListProposers: want %v, have %v", i+1, want, have)
		}
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { w.t.Logf("%s", string(p)); return len(p), nil }
