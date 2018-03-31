package extension

import (
	"bytes"
	"context"
	"math/rand"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"

	"github.com/peterbourgon/caspaxos/protocol"
)

// UserNode models the user API supported by the cluster.
type UserNode interface {
	protocol.Addresser
	Read(ctx context.Context, key string) (version uint64, value []byte, err error)
	CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error)
	Watch(ctx context.Context, key string, values chan<- []byte) error // TODO(pb): correct API?
}

// ClusterUser provides UserNode methods over a cluster.
type ClusterUser struct {
	address string
	cluster Cluster
	logger  log.Logger
}

var _ UserNode = (*ClusterUser)(nil)

// NewClusterUser returns a usable ClusterUser, which is an implementation of
// the user node interface that wraps the passed cluster. It should be uniquely
// identified by address.
func NewClusterUser(address string, c Cluster, logger log.Logger) *ClusterUser {
	return &ClusterUser{
		address: address,
		cluster: c,
		logger:  logger,
	}
}

// Address implements UserNode.
func (u ClusterUser) Address() string {
	return u.address
}

// Read implements UserNode.
func (u ClusterUser) Read(ctx context.Context, key string) (version uint64, value []byte, err error) {
	proposers, err := u.cluster.Proposers(ctx)
	if err != nil {
		return version, value, err
	}
	if len(proposers) <= 0 {
		return version, value, errors.New("no proposers in cluster")
	}

	proposer := proposers[rand.Intn(len(proposers))]
	return proposer.Read(ctx, key)
}

// CAS implements UserNode.
func (u ClusterUser) CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error) {
	proposers, err := u.cluster.Proposers(ctx)
	if err != nil {
		return version, value, err
	}
	if len(proposers) <= 0 {
		return version, value, errors.New("no proposers in cluster")
	}

	proposer := proposers[rand.Intn(len(proposers))]
	version, value, err = proposer.CAS(ctx, key, currentVersion, nextValue)
	if err != nil {
		return version, value, err
	}

	var (
		worked = bytes.Equal(value, nextValue)
		delete = worked && len(value) <= 0
	)
	if !worked || !delete {
		return version, value, err // regular exit
	}

	// The CAS worked, and it was a delete.
	// Assume we want to trigger a GC. TODO(pb): maybe move to explicit method?
	// Do the GC synchronously. TODO(pb): probably do this in the background?
	operators, err := u.cluster.OperatorNodes(ctx)
	if err != nil {
		return version, value, errors.Wrap(err, "GC failed")
	}
	if len(operators) <= 0 {
		return version, value, errors.New("GC failed: no operators in cluster")
	}
	operator := operators[rand.Intn(len(operators))]
	if err := operator.GarbageCollect(ctx, key); err != nil {
		return version, value, errors.Wrap(err, "GC failed")
	}

	// All good.
	return version, value, nil
}

// Watch implements UserNode.
func (u ClusterUser) Watch(ctx context.Context, key string, values chan<- []byte) error {
	acceptors, err := u.cluster.Acceptors(ctx)
	if err != nil {
		return err
	}
	if len(acceptors) <= 0 {
		return errors.New("no acceptors in cluster")
	}
	acceptor := acceptors[rand.Intn(len(acceptors))]
	return acceptor.Watch(ctx, key, values)
}
