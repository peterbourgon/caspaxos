package httpcluster

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/peterbourgon/caspaxos/cluster"
	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/httpapi"
)

// The types of peers in the cluster.
const (
	PeerTypeAcceptor     = "acceptor"
	PeerTypeProposer     = "proposer"
	PeerTypeOperator     = "operator"
	PeerTypeOperatorNode = "operator-node"
	PeerTypeUserNode     = "user-node"
)

// Peer wraps a plain cluster.Peer and implements extension.Cluster.
// We assume API host:ports in the cluster map to httpapi servers.
type Peer struct {
	*cluster.Peer
}

var _ extension.Cluster = (*Peer)(nil)

// Acceptors implements extension.Cluster.
func (p Peer) Acceptors(ctx context.Context) ([]extension.Acceptor, error) {
	var (
		hostports = p.Query(func(peerType string) bool { return strings.Contains(peerType, PeerTypeAcceptor) })
		acceptors = make([]extension.Acceptor, len(hostports))
	)
	for i := range hostports {
		u, _ := url.Parse(fmt.Sprintf("http://%s", hostports[i])) // TODO(pb): scheme
		acceptors[i] = httpapi.AcceptorClient{URL: u}             // TODO(pb): HTTP client
	}
	return acceptors, nil
}

// Proposers implements extension.Cluster.
func (p Peer) Proposers(ctx context.Context) ([]extension.Proposer, error) {
	var (
		hostports = p.Query(func(peerType string) bool { return strings.Contains(peerType, PeerTypeProposer) })
		proposers = make([]extension.Proposer, len(hostports))
	)
	for i := range hostports {
		u, _ := url.Parse(fmt.Sprintf("http://%s", hostports[i])) // TODO(pb): scheme
		proposers[i] = httpapi.ProposerClient{URL: u}             // TODO(pb): HTTP client
	}
	return proposers, nil
}

// Operators implements extension.Cluster.
func (p Peer) Operators(ctx context.Context) ([]extension.Operator, error) {
	var (
		hostports = p.Query(func(peerType string) bool { return strings.Contains(peerType, PeerTypeOperator) })
		operators = make([]extension.Operator, len(hostports))
	)
	for i := range hostports {
		u, _ := url.Parse(fmt.Sprintf("http://%s", hostports[i])) // TODO(pb): scheme
		operators[i] = httpapi.OperatorClient{URL: u}             // TODO(pb): HTTP client
	}
	return operators, nil
}
