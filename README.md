# CASPaxos [![GoDoc](https://godoc.org/github.com/peterbourgon/caspaxos?status.svg)](https://godoc.org/github.com/peterbourgon/caspaxos) [![Travis CI](https://travis-ci.org/peterbourgon/caspaxos.svg?branch=master)](https://travis-ci.org/peterbourgon/caspaxos) [![Go Report Card](https://goreportcard.com/badge/peterbourgon/caspaxos)](https://goreportcard.com/report/peterbourgon/caspaxos)

- [CASPaxos: Replicated State Machines without logs](https://github.com/rystsov/caspaxos/blob/master/latex/caspaxos.pdf) (PDF)

This repo provides a Go implementation of the CASPaxos consensus protocol,
including (eventually, soon) a batteries-included, deployable system
with configurable transports and persistence layers.

**DISCLAIMER**: this is a **work in progress** and is **incomplete**.
This disclaimer will be removed when the repo is more usable.

—

1. [Building and running](#building-and-running)
1. [Protocol implementation notes](#protocol-implementation-notes)
1. [System implementation notes](#system-implementation-notes)

## Building and running

TODO

## Protocol implementation notes

This section has some notes regarding an implementation of the core protocol.

### Node types

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
distinguish between "prepare" and "accept" acceptors. 

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

func (p myProposer) Propose(f ChangeFunc) (new State, err error) { /* ... */ }

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

### Proposal algorithm

Now let's talk about what needs to happen in Propose, Prepare, and Accept.
Luckily, the protocol is pretty explicitly laid-out in the paper. I'll
transcribe each step, and provide my implementation interpretation.

> **1.** A client submits the _f_ change to a proposer.

Nothing to note here.

> **2.** The proposer generates a ballot number, B, and sends "prepare" messages
> containing that number to the acceptors.

Ballot numbers must be monotonically increasing, so one important observation is
that each proposal must have the effect of incrementing the proposer's ballot
number.

```go
func (p myProposer) Propose(f ChangeFunc) (new State, err error) {
    p.ballot.Counter++ // increment
    b := p.ballot      // make a copy
```

We can make this a little easier with a helper method.

```go
func (b *Ballot) inc() Ballot {
    b.Counter++
    return Ballot{Counter: b.Counter, ID: b.ID}
}

func (p myProposer) Propose(f ChangeFunc) (new State, err error) {
    b := p.ballot.inc()
```

We then broadcast a prepare to _every_ acceptor.
In pseudocode,

```
for each preparer-stage acceptor a:
    go a.Prepare(b)
```

We'll convert this to Go in a moment.

> **3.** An acceptor [r]eturns a conflict if it already saw a greater ballot
> number. [Otherwise, it p]ersists the ballot number as a promise, and returns a
> confirmation either with an empty value (if it hasn't accepted any value yet)
> or with a tuple of an accepted value and its ballot number.

We turn our attention to the acceptor. 

One important observation is that the acceptor returns a conflict if the
submitted ballot number isn't greater than (i.e. doesn't beat) _either_ the
stored promise (if one exists) _or_ the accepted ballot number.

We can make these comparisons a little easier with a helper method.

```go
func (b Ballot) greaterThan(other Ballot) bool {
    if b.Counter == other.Counter {
        return b.ID > other.ID
    }
    return b.Counter > other.Counter
}
```

Observe that a zero-value Ballot will fail greaterThan checks against any
non-zero-value Ballot. This property is useful as we write our Prepare method.
Essentially, we need to ensure that the submitted ballot is greater than (i.e.
beats) any ballot we already know about. We test for that via the inverse, by
seeing if any of our stored ballots are greater than (i.e. beat) the submitted
ballot.

```go
func (a myAcceptor) Prepare(b Ballot) (current State, accepted Ballot, err error) {
    if a.promise.greaterThan(b) {
        return nil, zeroballot, ErrConflict
    }
    if a.accepted.greaterThan(b) {
        return nil, zeroballot, ErrConflict
    }
```

With those checks done, we should confirm the prepare statement. We'll persist
the ballot number as a promise, and return our state and accepted ballot number.
The client can easily check for a nil state to determine if we haven't accepted
a value yet.

```go
    a.promise = b
    return a.state, a.accepted, nil
}
```

> **4.** The proposer waits for F+1 confirmations. If they all contain the empty
> value, then the proposer defines the current state as Ø; otherwise, it picks
> the value of the tuple with the highest ballot number. 
>
> [The proposer] applies the _f_ function to the current state and sends the
> result[ing] new state, along with the generated ballot number B (i.e. an
> "accept" message) to the acceptors.

F+1 is our quorum size, in the general case it's also calculable as (N/2)+1. 
So we can complete our scatter/gather implementation in the proposer.
First, we'll set up a few types, and initiate the broadcast.

```go
func (p myProposer) Propose(f ChangeFunc) (new State, err error) {
    // Generate a ballot number B.
    b := p.ballot.inc()

    // Define a type for the 3-tuple returned by each acceptor.
    type result struct { 
        state  State
        ballot Ballot
        err    error
    }

    // We'll collect results into this channel.
    results := make(chan result, len(p.preparers))

    // Scatter.
    for _, preparer := range p.preparers {
        go func(preparer Preparer) {
            new, accepted, err := preparer.Prepare(b)
            results <- result{new, accepted, err}
        }(preparer)
    }
```

Observe that we only need to wait for the first F+1 confirmations. So we can
define an integer counter of whatever that number should be, and decrement it
whenever we receive a confirmation. Once it's to zero, or we've run out of
responses, we're done.

Observe that in order to "pick the value of the tuple with the highest ballot
number" we need to track the highest ballot number.

```go
    // Gather.
    var (
        quorum  = (len(p.preparers) / 2) + 1
        highest Ballot // initially zero
        winning State  // initially nil
    )
    for i := 0; i < cap(results) && quorum > 0; i++ {
        result := <-results
        if result.err != nil {
            continue // conflict
        }
        quorum-- // confirmation
        if result.state != nil && result.ballot.greaterThan(highest) {
            winning = result.state
            highest = result.ballot
        }
    }
    if quorum > 0 {
        return nil, errors.New("didn't get enough confirmations")
    }
```

When this code is done, the winning state corresponds to the highest returned
ballot number. It may be nil, but that's fine: we can still apply our change
function.

```go
    // Derive new state.
    newState := f(winning)
```

Now we need to do _another_ broadcast to all the acceptors, this time with an
"accept" message. It looks basically the same, with different types.

```go
    // The result type for accept messages is just an error.
    results := make(chan error, len(p.accepters))

    // Scatter.
    for _, accepter := range p.accepters {
        go func(accepter Accepter) {
            err := accepter.Accept(b, newState)
            results <- err
        }(accepter)
    }
```

Now we turn our attention back to the acceptor.

> **5.** An acceptor returns a conflict if it already saw a greater ballot
> number. [Otherwise, it] erases the promise, marks the received tuple (ballot
> number, value) as the accepted value, and returns a confirmation.

Same as in the prepare phase, a conflict occurs if the submitted ballot number
isn't greater than _either_ of the stored ballot numbers. Beyond that, the
process is straightforward.

```go
func (a myAcceptor) Accept(b Ballot, new State) error {
    if a.promise.greaterThan(b) {
        return ErrConflict
    }
    if a.accepted.greaterThan(b) {
        return ErrConflict
    }

    a.promise = zeroballot
    a.accepted = b
    a.current = new
    return nil
}
```

> **6.** The proposer waits for the F+1 confirmations.
> [It then] returns the new state to the client.

This maps to another gather step, almost identical to the first phase.
Upon successful completion, we can return.

```go
    // Gather.
    quorum := (len(p.accepters) / 2) + 1
    for i := 0; i < cap(results) && quorum > 0; i++ {
        if err := <-results; err != nil {
            continue // conflict
        }
        quorum-- // confirmation
    }
    if quorum > 0 {
        return nil, errors.New("didn't get enough confirmations")
    }

    // Success!
    return newState, nil
}
```

### Multiple keys

So far we've described a process for agreeing upon a single state, which
corresponds to a single, implicit key. But any real system will certainly not
want to have independent proposers and acceptors for each key/value pair! We'll
need to make the key explicit in all of the communication, and multiplex
key/value pairs onto the same set of proposers and acceptors. Luckily, this is
possible without too much work.

> "If a proposer accepts a key and a function, instead of just a function, and
> attaches the key label to every accept and prepare message it sends, then it
> can process multiple keys." —[@rystsov](https://twitter.com/rystsov)

Observe that the proposal semantics require only that new ballot numbers are
greater-than existing ballot numbers, _not_ that they're greater-than by
precisely one. Therefore, the proposer can use the same incrementing ballot
number counter for all keys.

We only need to make one change to the proposer.

```diff
type myProposer struct {
    ballot    Ballot
    preparers []Preparer
    accepters []Accepter
}

-func (p myProposer) Propose(f ChangeFunc) (new State, err error) { /* ... */ }
+func (p myProposer) Propose(key string, f ChangeFunc) (new State, err error) { /* ... */ }
```

And a few changes to the acceptor, to keep unique accepted states per key.

```diff
type myAcceptor struct {
-    promise  Ballot // sometimes empty
-    accepted Ballot
-    current  State
+    states map[string]acceptedState
}

+type acceptedState struct {
+    promise  Ballot // sometimes empty
+    accepted Ballot
+    current  State
+}

-func (a myAcceptor) Prepare(b Ballot) (s State, b Ballot, err error) { /* ... */ }
-func (a myAcceptor) Accept(b Ballot, s State) (err error)            { /* ... */ }
+func (a myAcceptor) Prepare(key string, b Ballot) (s State, b Ballot, err error) { /* ... */ }
+func (a myAcceptor) Accept(key string, b Ballot, s State) (err error)            { /* ... */ }
```

Changing the method bodies is left as an exercise for the reader. 
(The reader is free to cheat by inspecting this repo.)

### Configuration changes

TODO

### Deletes

TODO

### Optimizations

TODO

## System implementation notes

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

This is an opinionated approach. Other, radically different approaches are also
viable. As always, carefully consider your operational requirements to determine
which approach is best for your use-case.

### Safety

TODO

### Cluster membership

TODO

### Client interfaces

TODO
