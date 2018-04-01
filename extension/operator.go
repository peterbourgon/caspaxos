package extension

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/peterbourgon/caspaxos/protocol"
	"github.com/pkg/errors"
)

// Operator models the functionality that a user or admin needs.
type Operator interface {
	// Generally important.
	protocol.Addresser

	// User API
	Read(ctx context.Context, key string) (version uint64, value []byte, err error)
	CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error)
	Watch(ctx context.Context, key string, values chan<- []byte) error

	// Admin API
	ClusterState(ctx context.Context) (ClusterState2, error)
	ListAcceptors(ctx context.Context) ([]string, error)
	AddAcceptor(ctx context.Context, target protocol.Acceptor) error
	RemoveAcceptor(ctx context.Context, target protocol.Acceptor) error
	ListProposers(ctx context.Context) ([]string, error)
	AddProposer(ctx context.Context, target protocol.Proposer) error
	RemoveProposer(ctx context.Context, target protocol.Proposer) error
}

// ClusterState2 captures the current state of the cluster.
type ClusterState2 struct {
	Acceptors []string        `json:"acceptors"`
	Proposers []ProposerState `json:"proposers"`
	Operators []OperatorState `json:"operators"`
}

// OperatorState captures the current state of an operator in the cluster.
type OperatorState struct {
	Address   string   `json:"address"`
	Proposers []string `json:"proposers"`
	Acceptors []string `json:"acceptors"`
}

//
//
//

// ClusterOperator2 provides Operator methods over a cluster.
type ClusterOperator2 struct {
	address string
	cluster Cluster
	logger  log.Logger
}

var _ Operator = (*ClusterOperator2)(nil)

// NewClusterOperator2 returns a usable ClusterOperator, which is an
// implementation of the operator node interface that wraps the passed cluster.
// It should be uniquely identified by address.
func NewClusterOperator2(address string, c Cluster, logger log.Logger) *ClusterOperator2 {
	return &ClusterOperator2{
		address: address,
		cluster: c,
		logger:  logger,
	}
}

// Address implements Operator.
func (op ClusterOperator2) Address() string {
	return op.address
}

// Read implements Operator.
func (op ClusterOperator2) Read(ctx context.Context, key string) (version uint64, value []byte, err error) {
	return version, value, errors.New("ClusterOperator Read not yet implemented")
}

// CAS implements Operator.
func (op ClusterOperator2) CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error) {
	return version, value, errors.New("ClusterOperator CAS not yet implemented")
}

// Watch implements Operator.
func (op ClusterOperator2) Watch(ctx context.Context, key string, values chan<- []byte) error {
	return errors.New("ClusterOperator Watch not yet implemented")
}

// ClusterState implements Operator.
func (op ClusterOperator2) ClusterState(ctx context.Context) (s ClusterState2, err error) {
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

	operators, err := op.cluster.Operators(ctx)
	if err != nil {
		return s, err
	}
	for _, op := range operators {
		proposers, err := op.ListProposers(ctx)
		if err != nil {
			return s, err
		}
		acceptors, err := op.ListAcceptors(ctx)
		if err != nil {
			return s, err
		}
		s.Operators = append(s.Operators, OperatorState{
			Address:   op.Address(),
			Proposers: denil(proposers),
			Acceptors: denil(acceptors),
		})
	}

	return s, nil
}

// ListAcceptors implements Operator.
func (op ClusterOperator2) ListAcceptors(ctx context.Context) (results []string, err error) {
	a, err := op.cluster.Acceptors(ctx)
	if err != nil {
		return results, err
	}
	results = make([]string, len(a))
	for i := range a {
		results[i] = a[i].Address()
	}
	return results, nil
}

// AddAcceptor implements Operator.
func (op ClusterOperator2) AddAcceptor(ctx context.Context, target protocol.Acceptor) error {
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

// RemoveAcceptor implements Operator.
func (op ClusterOperator2) RemoveAcceptor(ctx context.Context, target protocol.Acceptor) error {
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

// ListProposers implements Operator.
func (op ClusterOperator2) ListProposers(ctx context.Context) (results []string, err error) {
	a, err := op.cluster.Proposers(ctx)
	if err != nil {
		return results, err
	}
	results = make([]string, len(a))
	for i := range a {
		results[i] = a[i].Address()
	}
	return results, nil
}

// AddProposer implements Operator.
func (op ClusterOperator2) AddProposer(ctx context.Context, target protocol.Proposer) error {
	return errors.New("ClusterOperator AddProposer not yet implemented")
}

// RemoveProposer implements Operator.
func (op ClusterOperator2) RemoveProposer(ctx context.Context, target protocol.Proposer) error {
	return errors.New("ClusterOperator RemoveProposer not yet implemented")
}
