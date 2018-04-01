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
	ClusterState(ctx context.Context) (ClusterState, error)
	ListAcceptors(ctx context.Context) ([]string, error)
	AddAcceptor(ctx context.Context, target protocol.Acceptor) error
	RemoveAcceptor(ctx context.Context, target protocol.Acceptor) error
	ListProposers(ctx context.Context) ([]string, error)
	AddProposer(ctx context.Context, target protocol.Proposer) error
	RemoveProposer(ctx context.Context, target protocol.Proposer) error
}

// ClusterState captures the current state of the cluster.
type ClusterState struct {
	Acceptors []string        `json:"acceptors"`
	Proposers []ProposerState `json:"proposers"`
	Operators []OperatorState `json:"operators"`
}

// ProposerState captures the current state of a proposer in the cluster.
type ProposerState struct {
	Address   string   `json:"address"`
	Preparers []string `json:"preparers"`
	Accepters []string `json:"accepters"`
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

// ClusterOperator provides Operator methods over a cluster.
type ClusterOperator struct {
	address string
	cluster Cluster
	logger  log.Logger
}

var _ Operator = (*ClusterOperator)(nil)

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

// Address implements Operator.
func (op ClusterOperator) Address() string {
	return op.address
}

// Read implements Operator.
func (op ClusterOperator) Read(ctx context.Context, key string) (version uint64, value []byte, err error) {
	return version, value, errors.New("ClusterOperator Read not yet implemented")
}

// CAS implements Operator.
func (op ClusterOperator) CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error) {
	return version, value, errors.New("ClusterOperator CAS not yet implemented")
}

// Watch implements Operator.
func (op ClusterOperator) Watch(ctx context.Context, key string, values chan<- []byte) error {
	return errors.New("ClusterOperator Watch not yet implemented")
}

// ClusterState implements Operator.
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
func (op ClusterOperator) ListAcceptors(ctx context.Context) (results []string, err error) {
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
func (op ClusterOperator) AddAcceptor(ctx context.Context, target protocol.Acceptor) error {
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
func (op ClusterOperator) RemoveAcceptor(ctx context.Context, target protocol.Acceptor) error {
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
func (op ClusterOperator) ListProposers(ctx context.Context) (results []string, err error) {
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
func (op ClusterOperator) AddProposer(ctx context.Context, target protocol.Proposer) error {
	return errors.New("ClusterOperator AddProposer not yet implemented")
}

// RemoveProposer implements Operator.
func (op ClusterOperator) RemoveProposer(ctx context.Context, target protocol.Proposer) error {
	return errors.New("ClusterOperator RemoveProposer not yet implemented")
}

func denil(s []string) []string {
	if s == nil {
		s = []string{}
	}
	return s
}
