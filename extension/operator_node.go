package extension

import (
	"context"

	"github.com/go-kit/kit/log"

	"github.com/peterbourgon/caspaxos/protocol"
)

// OperatorNode models the functionality that a cluster admin needs.
type OperatorNode interface {
	protocol.Addresser
	ClusterState(ctx context.Context) (ClusterState, error)
	GrowCluster(ctx context.Context, target protocol.Acceptor) error
	ShrinkCluster(ctx context.Context, target protocol.Acceptor) error
	GarbageCollect(ctx context.Context, key string) error
}

// ClusterState captures the current state of the cluster.
type ClusterState struct {
	Acceptors     []string        `json:"acceptors"`
	Proposers     []ProposerState `json:"proposers"`
	OperatorNodes []string        `json:"operator_nodes"`
	UserNodes     []string        `json:"user_nodes"`
}

// ProposerState captures the current state of a proposer in the cluster.
type ProposerState struct {
	Address   string   `json:"address"`
	Preparers []string `json:"preparers"`
	Accepters []string `json:"accepters"`
}

//
//
//

// ClusterOperator provides OperatorNode methods over a cluster.
type ClusterOperator struct {
	address string
	cluster Cluster
	logger  log.Logger
}

var _ OperatorNode = (*ClusterOperator)(nil)

// NewClusterOperator returns a usable ClusterOperator, which is an
// implementation of the operator node interface that wraps the passed cluster.
// It should be uniquely identified by address.
func NewClusterOperator(address string, c Cluster, logger log.Logger) *ClusterOperator {
	return &ClusterOperator{
		address: address,
		cluster: c,
		logger:  logger,
	}
}

// Address implements OperatorNode.
func (op ClusterOperator) Address() string {
	return op.address
}

// GrowCluster implements OperatorNode.
func (op ClusterOperator) GrowCluster(ctx context.Context, target protocol.Acceptor) error {
	proposers, err := op.cluster.Proposers(ctx)
	if err != nil {
		return err
	}
	confChangers := make([]protocol.ConfigurationChanger, len(proposers))
	for i := range proposers {
		confChangers[i] = proposers[i]
	}
	return protocol.GrowCluster(ctx, target, confChangers)
}

// ShrinkCluster implements OperatorNode.
func (op ClusterOperator) ShrinkCluster(ctx context.Context, target protocol.Acceptor) error {
	proposers, err := op.cluster.Proposers(ctx)
	if err != nil {
		return err
	}
	confChangers := make([]protocol.ConfigurationChanger, len(proposers))
	for i := range proposers {
		confChangers[i] = proposers[i]
	}
	return protocol.ShrinkCluster(ctx, target, confChangers)
}

// GarbageCollect implements OperatorNode.
func (op ClusterOperator) GarbageCollect(ctx context.Context, key string) error {
	proposers, err := op.cluster.Proposers(ctx)
	if err != nil {
		return err
	}
	acceptors, err := op.cluster.Acceptors(ctx)
	if err != nil {
		return err
	}
	fastForwarders := make([]protocol.FastForwarder, len(proposers))
	for i := range proposers {
		fastForwarders[i] = proposers[i]
	}
	rejectRemovers := make([]protocol.RejectRemover, len(acceptors))
	for i := range acceptors {
		rejectRemovers[i] = acceptors[i]
	}
	return protocol.GarbageCollect(ctx, key, fastForwarders, rejectRemovers, op.logger)
}

// ClusterState implements OperatorNode.
func (op ClusterOperator) ClusterState(ctx context.Context) (s ClusterState, err error) {
	acceptors, err := op.cluster.Acceptors(ctx)
	if err != nil {
		return s, err
	}
	for _, a := range acceptors {
		s.Acceptors = append(s.Acceptors, a.Address())
	}
	s.Acceptors = denil(s.Acceptors)

	proposers, err := op.cluster.Proposers(ctx)
	if err != nil {
		return s, err
	}
	for _, p := range proposers {
		preparers, err := p.ListPreparers()
		if err != nil {
			return s, err
		}
		accepters, err := p.ListAccepters()
		if err != nil {
			return s, err
		}
		s.Proposers = append(s.Proposers, ProposerState{
			Address:   p.Address(),
			Preparers: denil(preparers),
			Accepters: denil(accepters),
		})
	}

	operatorNodes, err := op.cluster.OperatorNodes(ctx)
	if err != nil {
		return s, err
	}
	for _, operatorNode := range operatorNodes {
		s.OperatorNodes = append(s.OperatorNodes, operatorNode.Address())
	}
	s.OperatorNodes = denil(s.OperatorNodes)

	userNodes, err := op.cluster.UserNodes(ctx)
	if err != nil {
		return s, err
	}
	for _, userNode := range userNodes {
		s.UserNodes = append(s.UserNodes, userNode.Address())
	}
	s.UserNodes = denil(s.UserNodes)

	return s, nil
}

func denil(s []string) []string {
	if s == nil {
		s = []string{}
	}
	return s
}
