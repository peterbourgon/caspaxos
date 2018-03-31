package extension

import "context"

// Cluster models the methods that operator and user nodes need from a cluster.
type Cluster interface {
	Acceptors(context.Context) ([]Acceptor, error)
	Proposers(context.Context) ([]Proposer, error)
	OperatorNodes(context.Context) ([]OperatorNode, error)
	UserNodes(context.Context) ([]UserNode, error)
}
