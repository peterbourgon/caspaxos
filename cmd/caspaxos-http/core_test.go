package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/kit/log"

	"github.com/peterbourgon/caspaxos/cluster"
	"github.com/peterbourgon/caspaxos/cluster/httpcluster"
	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/httpapi"
	"github.com/peterbourgon/caspaxos/protocol"
)

func TestOperations(t *testing.T) {
	ctx := context.Background()
	c, teardown := makeCluster(ctx, t)
	defer teardown()

	t.Logf("Fetching cluster state")
	{
		s, err := c.O1.ClusterState(context.Background())
		if err != nil {
			t.Fatalf("ClusterState: %v", err)
		}
		t.Logf("%d Acceptors: %+v", len(s.Acceptors), s.Acceptors)
		t.Logf("%d Proposers: %+v", len(s.Proposers), s.Proposers)
	}

	t.Logf("Growing cluster with each acceptor")
	for i, a := range []extension.Acceptor{c.A1, c.A2, c.A3} {
		if err := c.O1.GrowCluster(ctx, a); err != nil {
			t.Fatalf("GrowCluster(a%d): %v", i+1, err)
		}
	}

	t.Logf("Listing preparers and accepters for each proposer")
	for i, p := range []extension.Proposer{c.P1, c.P2} {
		s, err := p.ListPreparers()
		if err != nil {
			t.Fatalf("p%d ListPreparers: %v", i+1, err)
		}
		t.Logf("p%d ListPreparers: %v", i+1, s)
		s, err = p.ListAccepters()
		if err != nil {
			t.Fatalf("p%d ListAccepters: %v", i+1, err)
		}
		t.Logf("p%d ListAccepters: %v", i+1, s)
	}

	t.Logf("Dumping cluster state")
	{
		s, _ := c.O1.ClusterState(context.Background())
		buf, _ := json.MarshalIndent(s, "", "    ")
		fmt.Printf("%s\n", buf)
	}

	t.Logf("Proposing a change")
	{
		version, value, err := c.U1.CAS(ctx, "foo", 0, []byte("val0"))
		if want, have := error(nil), err; want != have {
			t.Fatalf("first CAS: want error %v, have %v", want, have)
		}
		if want, have := uint64(1), version; want != have {
			t.Fatalf("first CAS: want version %d, have %d", want, have)
		}
		if want, have := "val0", string(value); want != have {
			t.Fatalf("first CAS: want value %q, have %q", want, have)
		}
	}

	t.Logf("Doing a read")
	{
		version, value, err := c.U2.Read(ctx, "foo")
		if want, have := error(nil), err; want != have {
			t.Fatalf("first Read: want error %v, have %v", want, have)
		}
		if want, have := uint64(1), version; want != have {
			t.Fatalf("first Read: want version %d, have %d", want, have)
		}
		if want, have := "val0", string(value); want != have {
			t.Fatalf("first Read: want value %q, have %q", want, have)
		}
	}

	t.Logf("Proposing another change")
	{
		version, value, err := c.U2.CAS(ctx, "foo", 1, []byte("val1"))
		if want, have := error(nil), err; want != have {
			t.Fatalf("second CAS: want error %v, have %v", want, have)
		}
		if want, have := uint64(2), version; want != have {
			t.Fatalf("second CAS: want version %d, have %d", want, have)
		}
		if want, have := "val1", string(value); want != have {
			t.Fatalf("second CAS: want value %q, have %q", want, have)
		}
	}

	t.Logf("Proposing a change with a bad version")
	{
		version, value, err := c.U1.CAS(ctx, "foo", 1, []byte("BAD_VALUE"))
		if want, have := error(nil), err; want != have {
			t.Fatalf("bad CAS: want error %v, have %v", want, have)
		}
		if want, have := uint64(2), version; want != have {
			t.Fatalf("bad CAS: want version %d, have %d", want, have)
		}
		if want, have := "val1", string(value); want != have {
			t.Fatalf("bad CAS: want value %q, have %q", want, have)
		}
	}

	t.Logf("Doing a CAS delete")
	{
		version, value, err := c.U2.CAS(ctx, "foo", 2, []byte{})
		if want, have := error(nil), err; want != have {
			t.Fatalf("CAS delete: want error %v, have %v", want, have)
		}
		if want, have := uint64(3), version; want != have {
			t.Fatalf("CAS delete: want version %d, have %d", want, have)
		}
		if want, have := "", string(value); want != have {
			t.Fatalf("CAS delete: want value %q, have %q", want, have)
		}
	}

	t.Logf("Verifying CAS delete performed GC")
	{
		for i, reader := range []interface {
			Read(context.Context, string) (uint64, []byte, error)
		}{
			c.P1, c.P2, c.U1, c.U2,
		} {
			version, value, err := reader.Read(ctx, "foo")
			if want, have := error(nil), err; want != have {
				t.Fatalf("verify GC %d: want error %v, have %v", i+1, want, have)
			}
			if want, have := uint64(0), version; want != have {
				t.Fatalf("verify GC %d: want version %d, have %d", i+1, want, have)
			}
			if want, have := []byte(nil), value; !bytes.Equal(want, have) {
				t.Fatalf("verify GC %d: want value %v, have %v", i+1, want, have)
			}
		}
	}

	t.Logf("Making another write for Watch")
	{
		version, value, err := c.U2.CAS(ctx, "foo", 0, []byte("hello"))
		if err != nil {
			t.Fatalf("watch CAS: %v", err)
		}
		if want, have := uint64(1), version; want != have {
			t.Fatalf("watch CAS: want version %d, have %d", want, have)
		}
		if want, have := "hello", string(value); want != have {
			t.Fatalf("watch CAS: want value %q, have %q", want, have)
		}
	}

	t.Logf("Starting Watch on user node")
	var (
		values        = make(chan []byte, 100)
		wctx, wcancel = context.WithCancel(ctx)
		done          = make(chan error)
	)
	{
		go func() {
			done <- c.U1.Watch(wctx, "foo", values)
		}()
	}

	t.Logf("Checking initial Watch value")
	{
		select {
		case err := <-done:
			t.Fatalf("first recv: Watch error: %v", err)
		case value := <-values:
			if want, have := "hello", string(value); want != have {
				t.Fatalf("first recv: want %q, have %q", want, have)
			}
		case <-time.After(time.Second):
			t.Fatal("first recv: timeout")
		}
	}

	t.Logf("Making write for Watch")
	{
		version, value, err := c.U2.CAS(ctx, "foo", 1, []byte("world"))
		if err != nil {
			t.Fatalf("second CAS: %v", err)
		}
		if want, have := uint64(2), version; want != have {
			t.Fatalf("second CAS: want version %d, have %d", want, have)
		}
		if want, have := "world", string(value); want != have {
			t.Fatalf("second CAS: want value %q, have %q", want, have)
		}
	}

	t.Logf("Checking second Watch value")
	{
		select {
		case err := <-done:
			t.Fatalf("second recv: Watch error: %v", err)
		case value := <-values:
			if want, have := "world", string(value); want != have {
				t.Fatalf("second recv: want %q, have %q", want, have)
			}
		case <-time.After(time.Second):
			t.Fatal("second recv: timeout")
		}
	}

	t.Logf("Canceling the Watch and waiting for it to finish")
	{
		wcancel()
		<-done
	}
}

type testWriter struct{ t *testing.T }

func (tw testWriter) Write(p []byte) (int, error) {
	tw.t.Logf("%s", string(p))
	return len(p), nil
}

type testCluster struct {
	A1 extension.Acceptor
	A2 extension.Acceptor
	A3 extension.Acceptor
	P1 extension.Proposer
	P2 extension.Proposer
	O1 extension.OperatorNode
	U1 extension.UserNode
	U2 extension.UserNode
}

func makeCluster(ctx context.Context, t *testing.T) (result testCluster, teardown func()) {
	var (
		logger = log.NewLogfmtLogger(testWriter{t})
		stack  = []func(){} // for teardown
	)

	{
		a1Acceptor := protocol.NewMemoryAcceptor("a1", log.With(logger, "acceptor_core", 1))
		a1Handler := httpapi.NewAcceptorServer(a1Acceptor)
		a1Server := http.Server{Addr: "127.0.0.1:10011", Handler: a1Handler}
		a1Done := make(chan struct{})
		go func() { a1Server.ListenAndServe(); close(a1Done) }()
		stack = append(stack, func() { a1Server.Close(); <-a1Done })
		a1Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10011,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10012,
			Type:         httpcluster.PeerTypeAcceptor,
			InitialPeers: []string{},
			Logger:       log.With(logger, "acceptor_peer", 1),
		})
		stack = append(stack, func() { a1Peer.Leave(time.Second) })
		result.A1 = httpapi.AcceptorClient{URL: mustParseURL(t, "http://127.0.0.1:10011")}
		waitForListener(t, "127.0.0.1:10011")
	}

	{
		a2Acceptor := protocol.NewMemoryAcceptor("a2", log.With(logger, "acceptor_core", 2))
		a2Handler := httpapi.NewAcceptorServer(a2Acceptor)
		a2Server := http.Server{Addr: "127.0.0.1:10021", Handler: a2Handler}
		a2Done := make(chan struct{})
		go func() { a2Server.ListenAndServe(); close(a2Done) }()
		stack = append(stack, func() { a2Server.Close(); <-a2Done })
		a2Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10021,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10022,
			Type:         httpcluster.PeerTypeAcceptor,
			InitialPeers: []string{"127.0.0.1:10012"},
			Logger:       log.With(logger, "acceptor_peer", 2),
		})
		stack = append(stack, func() { a2Peer.Leave(time.Second) })
		result.A2 = httpapi.AcceptorClient{URL: mustParseURL(t, "http://127.0.0.1:10021")}
		waitForListener(t, "127.0.0.1:10021")
	}

	{
		a3Acceptor := protocol.NewMemoryAcceptor("a3", log.With(logger, "acceptor_core", 3))
		a3Handler := httpapi.NewAcceptorServer(a3Acceptor)
		a3Server := http.Server{Addr: "127.0.0.1:10031", Handler: a3Handler}
		a3Done := make(chan struct{})
		go func() { a3Server.ListenAndServe(); close(a3Done) }()
		stack = append(stack, func() { a3Server.Close(); <-a3Done })
		a3Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10031,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10032,
			Type:         httpcluster.PeerTypeAcceptor,
			InitialPeers: []string{"127.0.0.1:10012", "127.0.0.1:10022"},
			Logger:       log.With(logger, "acceptor_peer", 3),
		})
		stack = append(stack, func() { a3Peer.Leave(time.Second) })
		result.A3 = httpapi.AcceptorClient{URL: mustParseURL(t, "http://127.0.0.1:10031")}
		waitForListener(t, "127.0.0.1:10031")
	}

	{
		p1Proposer := protocol.NewMemoryProposer("p1", log.With(logger, "proposer_core", 1))
		p1Handler := httpapi.NewProposerServer(p1Proposer)
		p1Server := http.Server{Addr: "127.0.0.1:10041", Handler: p1Handler}
		p1Done := make(chan struct{})
		go func() { p1Server.ListenAndServe(); close(p1Done) }()
		stack = append(stack, func() { p1Server.Close(); <-p1Done })
		p1Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10041,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10042,
			Type:         httpcluster.PeerTypeProposer,
			InitialPeers: []string{"127.0.0.1:10012", "127.0.0.1:10022", "127.0.0.1:10032"},
			Logger:       log.With(logger, "proposer_peer", 1),
		})
		stack = append(stack, func() { p1Peer.Leave(time.Second) })
		result.P1 = httpapi.ProposerClient{URL: mustParseURL(t, "http://127.0.0.1:10041")}
		waitForListener(t, "127.0.0.1:10041")
	}

	{
		p2Proposer := protocol.NewMemoryProposer("p2", log.With(logger, "proposer_core", 2))
		p2Handler := httpapi.NewProposerServer(p2Proposer)
		p2Server := http.Server{Addr: "127.0.0.1:10051", Handler: p2Handler}
		p2Done := make(chan struct{})
		go func() { p2Server.ListenAndServe(); close(p2Done) }()
		stack = append(stack, func() { p2Server.Close(); <-p2Done })
		p2Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10051,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10052,
			Type:         httpcluster.PeerTypeProposer,
			InitialPeers: []string{"127.0.0.1:10012", "127.0.0.1:10022", "127.0.0.1:10032", "127.0.0.1:10042"},
			Logger:       log.With(logger, "proposer_peer", 2),
		})
		stack = append(stack, func() { p2Peer.Leave(time.Second) })
		result.P2 = httpapi.ProposerClient{URL: mustParseURL(t, "http://127.0.0.1:10051")}
		waitForListener(t, "127.0.0.1:10051")
	}

	{
		// Construct the peer first, because the ClusterOperator needs it to
		// have access to extension.Cluster functionality. We have a little race
		// where we're advertising ourself in the cluster before our API is
		// listening. That's not really a problem.
		o1Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10061,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10062,
			Type:         httpcluster.PeerTypeOperatorNode,
			InitialPeers: []string{"127.0.0.1:10012", "127.0.0.1:10022", "127.0.0.1:10032", "127.0.0.1:10042", "127.0.0.1:10052"},
			Logger:       log.With(logger, "operator_peer", 1),
		})
		o1Cluster := httpcluster.Peer{Peer: o1Peer}
		o1OperatorNode := extension.NewClusterOperator("o1", o1Cluster, log.With(logger, "operator_core", 1))
		o1Handler := httpapi.NewOperatorNodeServer(o1OperatorNode)
		o1Server := http.Server{Addr: "127.0.0.1:10061", Handler: o1Handler}
		o1Done := make(chan struct{})
		go func() { o1Server.ListenAndServe(); close(o1Done) }()
		stack = append(stack, func() { o1Server.Close(); <-o1Done })
		result.O1 = httpapi.OperatorNodeClient{URL: mustParseURL(t, "http://127.0.0.1:10061")}
		waitForListener(t, "127.0.0.1:10061")
	}

	{
		// Similarly, peer first, for access to the cluster.
		u1Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10071,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10072,
			Type:         httpcluster.PeerTypeUserNode,
			InitialPeers: []string{"127.0.0.1:10012", "127.0.0.1:10022", "127.0.0.1:10032", "127.0.0.1:10042", "127.0.0.1:10052", "127.0.0.1:10062"},
			Logger:       log.With(logger, "user_peer", 1),
		})
		u1Cluster := httpcluster.Peer{Peer: u1Peer}
		u1UserNode := extension.NewClusterUser("u1", u1Cluster, log.With(logger, "user_core", 1))
		u1Handler := httpapi.NewUserNodeServer(u1UserNode)
		u1Server := http.Server{Addr: "127.0.0.1:10071", Handler: u1Handler}
		u1Done := make(chan struct{})
		go func() { u1Server.ListenAndServe(); close(u1Done) }()
		stack = append(stack, func() { u1Server.Close(); <-u1Done })
		result.U1 = httpapi.UserNodeClient{URL: mustParseURL(t, "http://127.0.0.1:10071")}
		waitForListener(t, "127.0.0.1:10071")
	}

	{
		u2Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10081,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10082,
			Type:         httpcluster.PeerTypeUserNode,
			InitialPeers: []string{"127.0.0.1:10012", "127.0.0.1:10022", "127.0.0.1:10032", "127.0.0.1:10042", "127.0.0.1:10052", "127.0.0.1:10062", "127.0.0.1:10072"},
			Logger:       log.With(logger, "user_peer", 2),
		})
		u2Cluster := httpcluster.Peer{Peer: u2Peer}
		u2UserNode := extension.NewClusterUser("u2", u2Cluster, log.With(logger, "user_core", 2))
		u2Handler := httpapi.NewUserNodeServer(u2UserNode)
		u2Server := http.Server{Addr: "127.0.0.1:10081", Handler: u2Handler}
		u2Done := make(chan struct{})
		go func() { u2Server.ListenAndServe(); close(u2Done) }()
		stack = append(stack, func() { u2Server.Close(); <-u2Done })
		result.U2 = httpapi.UserNodeClient{URL: mustParseURL(t, "http://127.0.0.1:10081")}
		waitForListener(t, "127.0.0.1:10071")
	}

	teardown = func() {
		for i := len(stack) - 1; i >= 0; i-- {
			stack[i]()
		}
	}

	return result, teardown
}

func mustParseURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func waitForListener(t *testing.T, addr string) {
	var (
		deadline = time.Now().Add(10 * time.Second)
		backoff  = 25 * time.Millisecond
	)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return
		}
		t.Logf("waitForListener(%s): %v", addr, err)
		backoff *= 2
		if backoff > time.Second {
			backoff = time.Second
		}
		time.Sleep(backoff)
	}
	t.Fatalf("%s never came up", addr)
}
