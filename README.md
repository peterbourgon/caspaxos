# CASPaxos [![GoDoc](https://godoc.org/github.com/peterbourgon/caspaxos?status.svg)](https://godoc.org/github.com/peterbourgon/caspaxos) [![Travis CI](https://travis-ci.org/peterbourgon/caspaxos.svg?branch=master)](https://travis-ci.org/peterbourgon/caspaxos) [![Go Report Card](https://goreportcard.com/badge/peterbourgon/caspaxos)](https://goreportcard.com/report/peterbourgon/caspaxos)

- [CASPaxos: Replicated State Machines without logs](https://github.com/rystsov/caspaxos/blob/master/latex/caspaxos.pdf)

This repo provides a Go implementation of the CASPaxos consensus protocol,
including (eventually, soon) a batteries-included, deployable system
with configurable transports and persistence layers.

# Protocol implementation guide

This section has some notes regarding an implementation of the core protocol.

## Node types

The paper describes three node types: clients, proposers, and acceptors. Clients
are the users interacting with your system, so we only have to worry about
proposers and acceptors. Proposals, modeled as change functions, go from clients
to proposers. Proposers take the change function and perform the algorithm
against acceptors, in two phases: prepare, and then accept. 

A useful way to think of these two node types might be: proposers handle most of
the _behavior_ related to the protocol, but don't handle much _state_; acceptors
handle most of the _state_ related to the protocol, but don't handle much
_behavior_.

Proposers do have one important bit of long-lived state, which is a ballot
number. This is a two-tuple, consisting of a unique proposer ID and a counter
that increases monotonically with each proposal. Proposers also need to manage
transactional state for each proposal. 

Strictly speaking, proposers don't need to persist any of this state, as long as
new instances have unique IDs. But there are advantages to doing so, related to
key affinity, discussed in optimization, below.

One interesting detail revealed by the configuration change section is that, in
some circumstances, we need to add acceptors to the pool of "prepare" targets in
a proposer separately from the pool of "accept" targets. So proposers need to
distinguish between "prepare" acceptors and "accept" acceptors. 

Acceptors are a bit easier to model. They're responsible for storing the current
(accepted) state, as well as two ballot numbers: the accepted ballot number,
associated with the state; and a second so-called promise, which is set and
cleared as part of the proposal process.

From all of this, we can start to sketch out some definitions.
First, let's start with the core types, and some behavioral contracts.

```go
type Ballot struct { 
    Counter uint64
    ID      uint64
}

type State []byte // could be anything

type ChangeFunc func(current State) (next State)

// Proposer is the interface called by clients and implemented by proposers.
type Proposer interface {
    Propose(f ChangeFunc) (new State, err error)
}

// Preparer is the first-phase protocol behavior, 
// called by proposers and implemented by acceptors.
type Preparer interface {
    Prepare(b Ballot) (s State, b Ballot, err error)
}

// Accepter is the second-phase protocol behavior,
// called by proposers and implemented by acceptors.
type Accepter interface {
    Accept(b Ballot, s State) (err error)
}
```

With those, we can model basic implementations of our node types.

```go
type myProposer struct {
    ballot    Ballot
    preparers []Preparer
    accepters []Accepter
}

func (p Proposer) Propose(f ChangeFunc) (new State, err error) { /* ... */ }

type myAcceptor struct {
    promise  Ballot // sometimes empty
    accepted Ballot
    current  State
}

func (a myAcceptor) Prepare(b Ballot) (s State, b Ballot, err error) { /* ... */ }
func (a myAcceptor) Accept(b Ballot, s State) (err error)            { /* ... */ }
```

Note: it would also be possible to model the distinct sets of preparers and
accepters as a single set of acceptors, with some kind of flag to distinguish
the two.

Note: any serious implementation of an acceptor will probably want to persist
the state to disk. There are lots of ways to do that: [Gryadka][gryadka] writes
to Redis, for example. We'll deal with that in a bit.

[gryadka]: https://github.com/gryadka/js

## Proposal algorithm

Now let's talk about what needs to happen in Propose, Prepare, and Accept.

TODO

## Configuration changes

TODO

## Deletes

TODO

## Optimizations

TODO

# Cluster implementation guide

Production systems live and die on the basis of their operability. In the ideal
case, bootstrapping a CASPaxos system should involve nothing more than starting
enough proposer and acceptor nodes, telling them about a few of each other so
that they can discover the rest, and letting them self-cluster. Adding a node
should involve nothing more than starting it up and pointing it at one other
node in the cluster: the configuration change should be triggered automatically
and succeed. Removing a node should involve nothing more than stopping the node:
the system should detect it, trigger the configuration change, and continue
normally, assuming there are still enough acceptors to satisfy failure
requirements.

All of these properties should be possible to achieve if we consider the
strongly-consistent CASPaxos protocol as the data plane, and combine it with
with an eventually-consistent gossip membership layer as the control plane. In
this section, we'll sketch out what such a system would look like.