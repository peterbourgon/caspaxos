package mockcluster

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/peterbourgon/caspaxos/extension"
)

// Cluster is an in-memory collection of nodes.
type Cluster struct {
	mtx       sync.Mutex
	acceptors map[string]extension.Acceptor
	proposers map[string]extension.Proposer
	operators map[string]extension.Operator
}

var _ extension.Cluster = (*Cluster)(nil)

// NewCluster returns an empty in-memory cluster.
func NewCluster() *Cluster {
	return &Cluster{
		acceptors: map[string]extension.Acceptor{},
		proposers: map[string]extension.Proposer{},
		operators: map[string]extension.Operator{},
	}
}

// Join the target into the cluster. Returns the cluster as a convenience,
// because it can be used as an extension.Cluster.
func (c *Cluster) Join(target interface{}) (*Cluster, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	switch x := target.(type) {
	case extension.Acceptor:
		if _, ok := c.acceptors[x.Address()]; ok {
			return nil, errors.Errorf("acceptor %s already in cluster", x.Address())
		}
		c.acceptors[x.Address()] = x

	case extension.Proposer:
		if _, ok := c.proposers[x.Address()]; ok {
			return nil, errors.Errorf("proposer %s already in cluster", x.Address())
		}
		c.proposers[x.Address()] = x

	case extension.Operator:
		if _, ok := c.operators[x.Address()]; ok {
			return nil, errors.Errorf("operator %s already in cluster", x.Address())
		}
		c.operators[x.Address()] = x

	default:
		return nil, errors.Errorf("invalid target type")
	}

	return c, nil
}

// Acceptors implements extension.Cluster.
func (c *Cluster) Acceptors(context.Context) (result []extension.Acceptor, err error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	result = make([]extension.Acceptor, 0, len(c.acceptors))
	for _, target := range c.acceptors {
		result = append(result, target)
	}
	return result, nil
}

// Proposers implements extension.Cluster.
func (c *Cluster) Proposers(context.Context) (result []extension.Proposer, err error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	result = make([]extension.Proposer, 0, len(c.proposers))
	for _, target := range c.proposers {
		result = append(result, target)
	}
	return result, nil
}

// Operators implements extension.Cluster.
func (c *Cluster) Operators(context.Context) (result []extension.Operator, err error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	result = make([]extension.Operator, 0, len(c.operators))
	for _, target := range c.operators {
		result = append(result, target)
	}
	return result, nil
}
