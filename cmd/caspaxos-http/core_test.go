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

func TestCluster(t *testing.T) {
	var (
		logger = log.NewLogfmtLogger(testWriter{t})
		ctx    = context.Background()
	)

	var a1 extension.Acceptor
	{
		a1Acceptor := protocol.NewMemoryAcceptor("a1", log.With(logger, "acceptor_core", 1))
		a1Handler := httpapi.NewAcceptorServer(a1Acceptor)
		a1Server := http.Server{Addr: "127.0.0.1:10011", Handler: a1Handler}
		a1Done := make(chan struct{})
		go func() { a1Server.ListenAndServe(); close(a1Done) }()
		defer func() { a1Server.Close(); <-a1Done }()
		a1Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10011,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10012,
			Type:         httpcluster.PeerTypeAcceptor,
			InitialPeers: []string{},
			Logger:       log.With(logger, "acceptor_peer", 1),
		})
		defer a1Peer.Leave(time.Second)
		a1 = httpapi.AcceptorClient{URL: mustParseURL(t, "http://127.0.0.1:10011")}
		waitForListener(t, "127.0.0.1:10011")
	}

	var a2 extension.Acceptor
	{
		a2Acceptor := protocol.NewMemoryAcceptor("a2", log.With(logger, "acceptor_core", 2))
		a2Handler := httpapi.NewAcceptorServer(a2Acceptor)
		a2Server := http.Server{Addr: "127.0.0.1:10021", Handler: a2Handler}
		a2Done := make(chan struct{})
		go func() { a2Server.ListenAndServe(); close(a2Done) }()
		defer func() { a2Server.Close(); <-a2Done }()
		a2Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10021,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10022,
			Type:         httpcluster.PeerTypeAcceptor,
			InitialPeers: []string{"127.0.0.1:10012"},
			Logger:       log.With(logger, "acceptor_peer", 2),
		})
		defer a2Peer.Leave(time.Second)
		a2 = httpapi.AcceptorClient{URL: mustParseURL(t, "http://127.0.0.1:10021")}
		waitForListener(t, "127.0.0.1:10021")
	}

	var a3 extension.Acceptor
	{
		a3Acceptor := protocol.NewMemoryAcceptor("a3", log.With(logger, "acceptor_core", 3))
		a3Handler := httpapi.NewAcceptorServer(a3Acceptor)
		a3Server := http.Server{Addr: "127.0.0.1:10031", Handler: a3Handler}
		a3Done := make(chan struct{})
		go func() { a3Server.ListenAndServe(); close(a3Done) }()
		defer func() { a3Server.Close(); <-a3Done }()
		a3Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10031,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10032,
			Type:         httpcluster.PeerTypeAcceptor,
			InitialPeers: []string{"127.0.0.1:10012", "127.0.0.1:10022"},
			Logger:       log.With(logger, "acceptor_peer", 3),
		})
		defer a3Peer.Leave(time.Second)
		a3 = httpapi.AcceptorClient{URL: mustParseURL(t, "http://127.0.0.1:10031")}
		waitForListener(t, "127.0.0.1:10031")
	}

	var p1 extension.Proposer
	{
		p1Proposer := protocol.NewMemoryProposer("p1", log.With(logger, "proposer_core", 1))
		p1Handler := httpapi.NewProposerServer(p1Proposer)
		p1Server := http.Server{Addr: "127.0.0.1:10041", Handler: p1Handler}
		p1Done := make(chan struct{})
		go func() { p1Server.ListenAndServe(); close(p1Done) }()
		defer func() { p1Server.Close(); <-p1Done }()
		p1Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10041,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10042,
			Type:         httpcluster.PeerTypeProposer,
			InitialPeers: []string{"127.0.0.1:10012", "127.0.0.1:10022", "127.0.0.1:10032"},
			Logger:       log.With(logger, "proposer_peer", 1),
		})
		defer p1Peer.Leave(time.Second)
		p1 = httpapi.ProposerClient{URL: mustParseURL(t, "http://127.0.0.1:10041")}
		waitForListener(t, "127.0.0.1:10041")
	}

	var p2 extension.Proposer
	{
		p2Proposer := protocol.NewMemoryProposer("p2", log.With(logger, "proposer_core", 2))
		p2Handler := httpapi.NewProposerServer(p2Proposer)
		p2Server := http.Server{Addr: "127.0.0.1:10051", Handler: p2Handler}
		p2Done := make(chan struct{})
		go func() { p2Server.ListenAndServe(); close(p2Done) }()
		defer func() { p2Server.Close(); <-p2Done }()
		p2Peer, _ := cluster.NewPeer(cluster.Config{
			APIHost:      "127.0.0.1",
			APIPort:      10051,
			ClusterHost:  "127.0.0.1",
			ClusterPort:  10052,
			Type:         httpcluster.PeerTypeProposer,
			InitialPeers: []string{"127.0.0.1:10012", "127.0.0.1:10022", "127.0.0.1:10032", "127.0.0.1:10042"},
			Logger:       log.With(logger, "proposer_peer", 2),
		})
		defer p2Peer.Leave(time.Second)
		p2 = httpapi.ProposerClient{URL: mustParseURL(t, "http://127.0.0.1:10051")}
		waitForListener(t, "127.0.0.1:10051")
	}

	var o1 extension.OperatorNode
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
		defer func() { o1Server.Close(); <-o1Done }()
		o1 = httpapi.OperatorNodeClient{URL: mustParseURL(t, "http://127.0.0.1:10061")}
		waitForListener(t, "127.0.0.1:10061")
	}

	{
		s, err := o1.ClusterState(context.Background())
		if err != nil {
			t.Fatalf("ClusterState: %v", err)
		}
		t.Logf("%d Acceptors: %+v", len(s.Acceptors), s.Acceptors)
		t.Logf("%d Proposers: %+v", len(s.Proposers), s.Proposers)
	}

	t.Logf("Growing cluster with each acceptor")
	for i, a := range []extension.Acceptor{a1, a2, a3} {
		if err := o1.GrowCluster(ctx, a); err != nil {
			t.Fatalf("GrowCluster(a%d): %v", i+1, err)
		}
	}

	t.Logf("Listing preparers and accepters for each proposer")
	for i, p := range []extension.Proposer{p1, p2} {
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
		s, _ := o1.ClusterState(context.Background())
		buf, _ := json.MarshalIndent(s, "", "    ")
		fmt.Printf("%s\n", buf)
	}

	t.Logf("Proposing a change")
	{
		version, value, err := p2.CAS(ctx, "foo", 0, []byte("val0"))
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
		version, value, err := p1.Read(ctx, "foo")
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

	t.Logf("Doing a GC and verifying")
	{
		if err := o1.GarbageCollect(ctx, "foo"); err != nil {
			t.Fatalf("GC: %v", err)
		}
		version, value, err := p1.Read(ctx, "foo")
		if want, have := error(nil), err; want != have {
			t.Fatalf("verify GC: want error %v, have %v", want, have)
		}
		if want, have := uint64(0), version; want != have {
			t.Fatalf("first CAS: want version %d, have %d", want, have)
		}
		if want, have := []byte(nil), value; !bytes.Equal(want, have) {
			t.Fatalf("verify GC: want value %v, have %v", want, have)
		}
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

func waitForListener(t *testing.T, addr string) {
	var (
		deadline = time.Now().Add(10 * time.Second)
		backoff  = 25 * time.Millisecond
	)
	for time.Now().Before(deadline) {
		_, err := net.Dial("tcp", addr)
		if err == nil {
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
