package examples

import (
	"context"
	"testing"

	"github.com/go-kit/kit/log"

	"github.com/peterbourgon/caspaxos/cluster/mockcluster"
	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/protocol"
)

func TestCompleteConfigurationChange(t *testing.T) {
	var (
		logger = log.NewLogfmtLogger(testWriter{t})
		ctx    = context.Background()
		c      = mockcluster.NewCluster()
		a1     = extension.NewSimpleAcceptor(protocol.NewMemoryAcceptor("a1", log.With(logger, "a", 1)))
		a2     = extension.NewSimpleAcceptor(protocol.NewMemoryAcceptor("a2", log.With(logger, "a", 2)))
		a3     = extension.NewSimpleAcceptor(protocol.NewMemoryAcceptor("a3", log.With(logger, "a", 3)))
		p1     = extension.NewSimpleProposer(protocol.NewMemoryProposer("p1", log.With(logger, "p", 1)))
		p2     = extension.NewSimpleProposer(protocol.NewMemoryProposer("p2", log.With(logger, "p", 2)))
		p3     = extension.NewSimpleProposer(protocol.NewMemoryProposer("p3", log.With(logger, "p", 3)))
		o1     = extension.NewClusterOperator("o1", c, log.With(logger, "o", 1))
		o2     = extension.NewClusterOperator("o2", c, log.With(logger, "o", 2))
	)
	for _, node := range []interface{}{a1, a2, a3, p1, p2, p3, o1, o2} {
		c.Join(node)
	}

	t.Logf("Add the first three Acceptors.")
	{
		for _, target := range []protocol.Acceptor{a1, a2, a3} {
			if err := o1.AddAcceptor(ctx, target); err != nil {
				t.Fatalf("Add acceptor %s: %v", target.Address(), err)
			}
		}
	}

	t.Logf("Make a write.")
	{
		version, value, err := o2.CAS(ctx, "foo", 0, []byte("val0"))
		checkOp(t, "first CAS", version, value, err, 1, "val0", nil)
	}

	t.Logf("Create and add a fourth acceptor.")
	var a4 *extension.SimpleAcceptor
	{
		a4 = extension.NewSimpleAcceptor(protocol.NewMemoryAcceptor("a4", log.With(logger, "a", 4)))
		c.Join(a4)
		if err := o2.AddAcceptor(ctx, a4); err != nil {
			t.Fatalf("Add acceptor %s: %v", a4.Address(), err)
		}
	}

	t.Logf("Remove the first acceptor.")
	{
		if err := o1.RemoveAcceptor(ctx, a1); err != nil {
			t.Fatalf("Remove acceptor %s: %v", a1.Address(), err)
		}
	}

	t.Logf("Make a read.")
	t.Logf("The read is a consistent read, meaning it goes through the Propose mechanism.")
	t.Logf("That means the ballot is incremented, and the new acceptor receives the current value.")
	{
		version, value, err := o2.Read(ctx, "foo")
		checkOp(t, "first read", version, value, err, 1, "val0", nil)
	}

	t.Logf("Create and add a fifth acceptor.")
	var a5 *extension.SimpleAcceptor
	{
		a5 = extension.NewSimpleAcceptor(protocol.NewMemoryAcceptor("a5", log.With(logger, "a", 5)))
		c.Join(a5)
		if err := o2.AddAcceptor(ctx, a5); err != nil {
			t.Fatalf("Add acceptor %s: %v", a5.Address(), err)
		}
	}

	t.Logf("Remove the second acceptor.")
	{
		if err := o1.RemoveAcceptor(ctx, a2); err != nil {
			t.Fatalf("Remove acceptor %s: %v", a2.Address(), err)
		}
	}

	t.Logf("Make a read.")
	{
		version, value, err := o1.Read(ctx, "foo")
		checkOp(t, "second read", version, value, err, 1, "val0", nil)
	}

	t.Logf("Create and add a sixth acceptor.")
	var a6 *extension.SimpleAcceptor
	{
		a6 = extension.NewSimpleAcceptor(protocol.NewMemoryAcceptor("a6", log.With(logger, "a", 6)))
		c.Join(a6)
		if err := o2.AddAcceptor(ctx, a6); err != nil {
			t.Fatalf("Add acceptor %s: %v", a6.Address(), err)
		}
	}

	t.Logf("Remove the third acceptor.")
	t.Logf("After this, all acceptors of the initial write are gone.")
	{
		if err := o1.RemoveAcceptor(ctx, a3); err != nil {
			t.Fatalf("Remove acceptor %s: %v", a3.Address(), err)
		}
	}

	t.Logf("Make another CAS.")
	{
		version, value, err := o2.CAS(ctx, "foo", 1, []byte("val1"))
		checkOp(t, "second CAS", version, value, err, 2, "val1", nil)
	}
}

func checkOp(t *testing.T, ctx string, version uint64, value []byte, err error, wantVersion uint64, wantValue string, wantErr error) {
	t.Helper()
	if want, have := wantErr, err; want != have {
		t.Fatalf("%s: error: want %v, have %v", ctx, want, have)
	}
	if want, have := wantVersion, version; want != have {
		t.Fatalf("%s: version: want %d, have %d", ctx, want, have)
	}
	if want, have := wantValue, string(value); want != have {
		t.Fatalf("%s: value: want %q, have %q", ctx, want, have)
	}
}
