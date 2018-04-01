package extension

import (
	"context"
	"math/rand"

	"github.com/go-kit/kit/log"
	"github.com/peterbourgon/caspaxos/protocol"
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
	ListProposers(ctx context.Context) ([]string, error)
	ListAcceptors(ctx context.Context) ([]string, error)
	AddAcceptor(ctx context.Context, target protocol.Acceptor) error
	RemoveAcceptor(ctx context.Context, target protocol.Acceptor) error
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
	a, err := op.cluster.Proposers(ctx)
	if err != nil {
		return version, value, err
	}
	target := a[rand.Intn(len(a))]
	return target.Read(ctx, key)
}

// CAS implements Operator.
func (op ClusterOperator) CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error) {
	a, err := op.cluster.Proposers(ctx)
	if err != nil {
		return version, value, err
	}
	target := a[rand.Intn(len(a))]
	return target.CAS(ctx, key, currentVersion, nextValue)
}

// Watch implements Operator. Since the user API wants values, and the acceptors
// emits states, this method performs (Version, Value) parsing.
func (op ClusterOperator) Watch(ctx context.Context, key string, values chan<- []byte) error {
	a, err := op.cluster.Acceptors(ctx)
	if err != nil {
		return err
	}

	var (
		target         = a[rand.Intn(len(a))]
		states         = make(chan []byte)
		errs           = make(chan error, 1)
		subctx, cancel = context.WithCancel(ctx)
	)

	go func() {
		errs <- target.Watch(subctx, key, states)
	}()

	for {
		select {
		case state := <-states:
			_, value, err := parseVersionValue(state)
			if err != nil {
				cancel()
				<-errs
				return err
			}
			values <- value

		case err := <-errs:
			cancel() // no-op for linter
			return err
		}
	}
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

func denil(s []string) []string {
	if s == nil {
		s = []string{}
	}
	return s
}
