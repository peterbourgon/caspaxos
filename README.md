# CASPaxos [![GoDoc](https://godoc.org/github.com/peterbourgon/caspaxos?status.svg)](https://godoc.org/github.com/peterbourgon/caspaxos) [![Travis CI](https://travis-ci.org/peterbourgon/caspaxos.svg?branch=master)](https://travis-ci.org/peterbourgon/caspaxos) [![Go Report Card](https://goreportcard.com/badge/peterbourgon/caspaxos)](https://goreportcard.com/report/peterbourgon/caspaxos)

[CASPaxos: Replicated State Machines without logs](https://github.com/rystsov/caspaxos/blob/master/latex/caspaxos.pdf) (PDF)

This repo provides a Go implementation of the CASPaxos consensus protocol,
including (eventually, soon) a batteries-included, deployable system with
configurable transports and persistence layers. 
**Please be aware that this is a work in progress and is incomplete**. 
I'll remove this disclaimer when the repo is more usable.

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
    ID      string
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

#### Propose step 1

> A client submits the _f_ change to a proposer.

Nothing to note here.

Well, maybe one thing: sending functions over the wire is not easy in languages
like Go. It may be a good initial compromise to present a more opinionated API
to clients, where API methods map to specific, pre-defined change functions.
Making this work in the general case is an exercise for the reader.

#### Propose step 2

> The proposer generates a ballot number, B, and sends "prepare" messages
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

#### Propose step 3

> An acceptor [r]eturns a conflict if it already saw a greater ballot number.
> [Otherwise, it p]ersists the ballot number as a promise, and returns a
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

#### Propose step 4

> The proposer waits for F+1 confirmations. If they all contain the empty value,
> then the proposer defines the current state as Ø; otherwise, it picks the
> value of the tuple with the highest ballot number. 
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

We can actually elide the result.state check, because if the value isn't set
yet, then all result.states will be nil, and the assignment makes no difference.

```diff
-         if result.state != nil && result.ballot.greaterThan(highest) {
+         if result.ballot.greaterThan(highest) {
             winning = result.state
             highest = result.ballot
         }
```

When this code is done, the winning state corresponds to the highest returned
ballot number. It may be nil, but that's fine: we can still apply our change
function.

One interesting thing that isn't explicitly mentioned at this point in the paper
is the situation when a change is proposed through a proposer that has an old
ballot number, and therefore receives nothing but conflicts from acceptors. This
happens frequently: whenever a proposer successfully executes a change, all
other proposers necessarily have too-old ballot numbers, and won't find out
about it until they try to make a change and fail. Whenever this happens, we can
and should fast-forward the proposer's ballot number to the highest observed
conflicting ballot number, so that subsequent proposals have a chance at
success.

```diff
     var (
         quorum      = (len(p.preparers) / 2) + 1
+        conflicting Ballot // initially zero
         highest     Ballot // initially zero
         winning     State  // initially nil
     )
     for i := 0; i < cap(results) && quorum > 0; i++ {
         result := <-results
         if result.err != nil {
+            if result.ballot.greaterThan(conflicting) {
+                conflicting = result.ballot
+            }
             continue // conflict
         }
         quorum-- // confirmation
         if result.ballot.greaterThan(highest) {
             winning = result.state
             highest = result.ballot
         }
     }
     if quorum > 0 {
+        p.ballot.Counter = conflicting.Counter // fast-forward
         return nil, errors.New("didn't get enough confirmations")
     }
```

If this code succeeds, we have a valid current (winning) state, which we can
feed to our change function to derive the user's intended next state.

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

#### Propose step 5

> An acceptor returns a conflict if it already saw a greater ballot number.
> [Otherwise, it] erases the promise, marks the received tuple (ballot number,
> value) as the accepted value, and returns a confirmation.

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

#### Propose step 6 

> The proposer waits for the F+1 confirmations.
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

### Configuration changes

Throughout this section we assume there is some _a priori_ set of addressable
proposers and acceptors, somehow just known to the cluster operator. That
assumption is fraught with peril, which we'll address in the system
implementation notes. For now, we take it as a given.

The paper describes a process for adding acceptors to the cluster, including an
optimization in one case, and a way to also remove acceptors from the cluster.
Implicit in this is that adding or removing _proposers_ requires no special
work. That makes sense: proposers are basically stateless, so just starting one
up with a unique ID and configuring it with the current set of acceptors should
be sufficient.

Adding an acceptor in the general case is a four-step process.

#### Grow step 1

> Turn the new acceptor on.

The acceptor will be initially empty.

#### Grow step 2

> Connect to each proposer and update its configuration to send the
> [second-phase] "accept" messages to the A_1 .. A_2F+2 set of acceptors and to
> require F+2 confirmations during the "accept" phase.

Note that "the A_1 .. A_2F+2 set of acceptors" means the new, larger set of
acceptors, including the new acceptor. So, in effect, the first clause of this
step means to add the new acceptor to each proposer's pool of accepters _only_,
i.e. without adding it to the corresponding pool of preparers. This is why we
need to distinguish the two roles of acceptors in preparers!

The second clause, "require F+2 confirmations", means that when we compute
desired quorum during the accept phase, we should be sure to include the new
acceptor. But since we take quorum as `(len(p.accepters) / 2) + 1`, this happens
automatically.

Observe that this step requires connecting to each proposer. This means it has
to be done at some conceptual or operational level "above" proposers, rather
than as an e.g. method on a proposer. In Go I think we can model this as a
function.

```go
func GrowCluster(target Acceptor, proposers []Proposer) error { /* ... */ }
```

This also implies that proposers will need a new set of methods, allowing
operators to modify their sets of preparers and accepters independently.
I'll enumerate them all here.

```diff
 type Proposer interface {
     Propose(key string, f ChangeFunc) (new State, err error)
+
+    AddAccepter(target Accepter) error
+    AddPreparer(target Preparer) error
+    RemovePreparer(target Preparer) error
+    RemoveAccepter(target Accepter) error
 }
```

Observe that we don't have a key parameter in any of these methods. That's
because each acceptor (each accepter and preparer) can service all keys. There's
no need to have per-key pools of acceptors (accepters and preparers).

Now the implementation of GrowCluster starts taking shape.

```go
func GrowCluster(target Acceptor, proposers []Proposer) error {
    for _, p := range proposers {
        if err := p.AddAccepter(target); err != nil {
            return errors.Wrap(err, "adding accepter")
        }
    }
```

#### Grow step 3

> Pick any proposer and execute the identity state transition function x -> x.

This also seems straightforward.

```go
    var (
        proposer = proposers[rand.Intn(len(proposers))]
        zerokey  = "" // special key we don't allow users to modify
        identity = func(x State) State { return x }
    )
    if _, err := proposer.Propose(zerokey, identity); err != nil {
        return errors.Wrap(err, "executing identity function")
    }
```

We'll use a special _zerokey_ to push the transaction through. But I don't think
it matters which key we use, really, as long as the transaction succeeds. I
believe the goal of this step is just to get the target acceptor to receive
_any_ ballot number for _any_ key.

#### Grow step 4

> Connect to each proposer and update its configuration to send "prepare"
> messages to the A_1 .. A_2F+2 set of acceptors and to require F+2
> confirmations.

The final step has the same form as step two, but now dealing with the
first-phase set of preparers. So we can model it very similarly.

```go
    for _, p := range proposers {
        if err := p.AddPreparer(target); err != nil {
            return errors.Wrap(err, "adding preparer")
        }
    }
}
```

The inverse ShrinkCluster function is left as an exercise for the reader. 
(The reader is free to cheat by inspecting this repo.)

The paper, however, presents a slight caveat, which hinges on the difference
between a cluster of 2F+2 nodes (an even count) and a cluster of 2F+3 nodes (an
odd count).

> The protocol for changing the set of acceptors from A_1 .. A_2F+2 to A_1 ..
> A_2F+3 [from even to odd] is more straightforward because we can treat a 2F+2
> nodes cluster as a 2F+3 nodes cluster where one node had been down from the
> beginning: 
> 
>  1. Connect to each proposer and update its configuration to send the prepare
>     and accept messages to the [second] A_1...A_2F+3 set of acceptors.
>  2. Turn on the A_2F+3 acceptor.

Put another way, when growing (or shrinking) a cluster from an odd number of
acceptors to an even number of acceptors, the enumerated process is required.
But when growing (or shrinking) a cluster from an even number of acceptors to an
odd number of acceptors, an optimization is possible: we can first change the
accept and prepare lists of all proposers, and then turn the acceptor on, and
avoid the cost of an identity read.

I think this is probably not worth implementing, for several reasons. First,
cluster membership changes are rare and operator-driven, and so don't really
benefit from the lower latency as much as reads or writes would. Second, the
number of acceptors in the cluster is not necessarily known a priori, and can in
theory drift between different proposers; calculating the correct value is
difficult in itself, probably requiring asking some other source of authority.
Third, in production environments, there's great value in having a consistent
process for any cluster change; turning a node on at different points in that
process depending on the cardinality of the node-set is fraught with peril.

With that said, it's definitely possible I'm overlooking something, or my set of
assumptions may not be valid in your environment. Please think critically about
your use case, and provide me feedback if you think I've got something wrong.
I'll appreciate it, honest!

### One-round trip optimization

> Since the prepare phase doesn't depend on the change function, it's possible
> to piggyback the next prepare message on the current accept message to reduce
> the number of trips from two to one.
>
> In this case, a proposer caches the last written value, and the clients should
> use that proposer to initiate the state transition to benefit from the
> optimization.

This optimization has two assumptions: that there is a steady stream of change
requests for a single key, and that we will implement some kind of
client/proposer key affinity. I think both of these assumptions are probably not
valid for many use cases, and certainly not interesting to us at this stage in
the implementation. Therefore I won't make this optimization.

### Deletes

Here's the delete process outlined by the paper.

#### Delete step 1

> On a delete request, a system writes tombstone with regular F+1 "accept"
> quorum, schedules a garbage collection, and confirms the request to a client.

From this we learn that a delete begins by writing a tombstone, which is an
empty value. Of course, this still occupies space in the acceptors. So now we go
to reclaim that space with a garbage collection.

#### Delete step 2

> The garbage collection operation (in the background)

The paper stipulates the GC should occur in the background, presumably outside
of the client request path. To my mind it implies that some kind of resilience
is necessary, to prevent GC request from being lost. I guess that means either
persisting GC requests and re/loading them on process start; or, perhaps better,
implementing them by some kind of continuous stateless process which is able to
scan the keyspace and identify deleted keys that should be deleted _a priori_. 

For now, let's punt on these interesting questions, and just implement GC as a
synchronous function taking the key and tombstone. Later, we can figure out how
to call this function in the right context or circumstance.

#### Delete step 2a

> Replicates an empty value to all nodes by executing the identity transform
> with max quorum size (2F+1).

Put another way: make a read proposal, except require a quorum of 100%. I think
we can model this in one or two ways: either change the function signature of
Propose to accept some parameter that dictates the desired quorum, or add a
second method to the API for this step exclusively. I'll actually do a hybrid
approach: make an unexported (private) propose method that parameterizes the
quorum, change the existing propose method to call it, and add a new
FullIdentityRead method for this use-case specifically.

```diff
 type Proposer interface {
     Propose(key string, f ChangeFunc) (new State, err error)
 
     AddAccepter(target Accepter) error
     AddPreparer(target Preparer) error
     RemovePreparer(target Preparer) error
     RemoveAccepter(target Accepter) error

+    FullIdentityRead(key string) (current State, err error)
 }
```

```go
type quorumFunc func(n int) int

var (
	regularQuorum = func(n int) int { return (n / 2) + 1 } // i.e. F+1
	fullQuorum    = func(n int) int { return n }           // i.e. 2F+1
)

func (p myProposer) propose(key string, f ChangeFunc, qf quorumFunc) (new State, err error) {
    // ...
}

func (p myProposer) Propose(key string, f ChangeFunc) (new State, err error) {
    return p.propose(key, f, regularQuorum)
}

func (p myProposer) FullIdentityRead(key string) (current State, err error) {
    return p.propose(key, identityRead, fullQuorum)
}
```

Now we can start to write our GC function.

```go
func GarbageCollect(key string, proposers []Proposer) error {
    // 2a
    proposer := proposers[rand.Intn(len(proposers))]
    if _, err := proposer.FullIdentityRead(key); err != nil {
        return err
    }
```

#### Delete step 2b

> Connects to each proposer, invalidates its cache associated with the removing
> key (see one-round-trip optimization 2.2.1), fast-forwards its counter to be
> greater than the tombstone's ballot number, and increments the proposer's age.

There's a lot happening here. First, we'll ignore the bit about the cache,
because [the referenced optimization][rto] isn't implemented. We see that we
need to fast-forward the ballot number counter on proposers beyond the ballot
number of the tombstone, which means we need to know what that number is. And
we're introduced to a brand new concept, called the proposer's _age_. 

[rto]: #one-round-trip-optimization

In short, the age is essentially just another counter, like a ballot number,
kept by the proposers and forwarded with each request to the acceptors. The
acceptors check it, just like a ballot number, in order to potentially reject
requests if they are too old.

We start by adding a new method.

```diff
 type Proposer interface {
     Propose(key string, f ChangeFunc) (new State, err error)
 
     AddAccepter(target Accepter) error
     AddPreparer(target Preparer) error
     RemovePreparer(target Preparer) error
     RemoveAccepter(target Accepter) error

     FullIdentityRead(key string) (current State, err error)
+    FastForwardIncrement(key string, tombstone Ballot) error
 }
```

This method should fast-forward the ballot number counter and increment the age.
Age is new state tracked in the proposer.

```diff
 type myProposer struct {
+    age       uint64     
     ballot    Ballot
     preparers []Preparer
     accepters []Accepter
 }
```

The implementation of that method is simple and left as an exercise. But now we
can write step 2b in our GC function. Since this new method needs the ballot
number counter from the tombstone, and we've decided that our GC function is
called _after_ the tombstone write has already been made the client, we need to
take that bit of information in as a parameter.

```diff
-func GarbageCollect(key string, proposers []Proposer) error {
+func GarbageCollect(key string, tombstone Ballot, proposers []Proposer) error {
```

Which we can now use.

```go
    // 2b
    for _, proposer := range proposers {
        if err := proposer.FastForwardIncrement(key, tombstone); err != nil {
            return err
        }
    }
```

#### Delete step 2c

> Connects to each acceptor and asks it to reject messages from proposers if
> their age is younger than the corresponding age from the previous step.

Now we learn that acceptors need to track ages, too. But we have two problems:
each proposer has _its own age_, and in the GC function we don't yet know what
they are! I think this is an unintended consequence of an assumption by the
author that we've implemented the round trip optimization, and that all requests
for a given key will go through a single acceptor. That's not true for us.

So I think we have to collect ages from proposers as an output of step 2b, and I
think we have to pass them to acceptors in this step 2c.

```diff
 type Proposer interface {
     Propose(key string, f ChangeFunc) (new State, err error)
 
     AddAccepter(target Accepter) error
     AddPreparer(target Preparer) error
     RemovePreparer(target Preparer) error
     RemoveAccepter(target Accepter) error

     FullIdentityRead(key string) (current State, err error)
-    FastForwardIncrement(key string, tombstone Ballot) error
+    FastForwardIncrement(key string, tombstone Ballot) (age uint64, err error)
 }
```

```diff
 type Acceptor interface {
     Preparer
     Accepter
+    GarbageCollecter
 }

+type GarbageCollecter interface {
+    RejectByAge(ages []uint64) error
+}
```

Observe that the rejection based on age has no connection to the key being
garbage collected, the age is used as a means to establish causality between
nodes independent of key. So RejectByAge doesn't take a key, just a set of ages.

But this surfaces another problem. We're taking a set of ages, but we don't have
a way to associate them to specific proposers, which we need to do. One way
would be to pass a map of some proposer ID to its age. Another (I think better)
way would be to define an age as a 2-tuple of the age counter and the
corresponding proposer ID.

```go
type Age struct {
    Counter uint64
    ID      string
}
```

Eagle-eyed observers will note this is basically a copy of the Ballot struct.
Further optimizations based on that observation may be possible.
For now, let's use what we have.

```diff
 type myProposer struct {
-    age       uint64
+    age       Age
     ballot    Ballot
     preparers []Preparer
     accepters []Accepter
 }

 type Proposer interface {
     Propose(key string, f ChangeFunc) (new State, err error)
 
     AddAccepter(target Accepter) error
     AddPreparer(target Preparer) error
     RemovePreparer(target Preparer) error
     RemoveAccepter(target Accepter) error

     FullIdentityRead(key string) (current State, err error)
-    FastForwardIncrement(key string, tombstone Ballot) (age uint64, err error)
+    FastForwardIncrement(key string, tombstone Ballot) (age Age, err error)
 }

 type GarbageCollecter interface {
-    RejectByAge(ages []uint64) error
+    RejectByAge(ages []Age) error
 }
```

Modifying or implementing those functions is left as an exercise for the reader.
Now we can modify the second step, and write the third step, in our GC function.

```go
    // 2b
    var ages []Age
    for _, proposer := range proposers {
        age, err := proposer.FastForwardIncrement(key, tombstone)
        if err != nil {
            return err
        }
        ages = append(ages, age)
    }
 
    // 2c
    for _, acceptor := range acceptors {
        if err := acceptor.RejectByAge(ages); err != nil {
            return err
        }
    }
```

#### Delete step 2d

> For each acceptor, remove the register if its value is the tombstone
> from the 2a step.

This is straightforward, except for one thing: we haven't collected the value
from step 2a yet!

```diff
     // 2a
     proposer := proposers[rand.Intn(len(proposers))]
-    if _, err := proposer.FullIdentityRead(key); err != nil {
+    killstate, err := proposer.FullIdentityRead(key)
+    if err != nil {
         return err
     }
```

Now we can define a new RemoveIfEqual method for the acceptors.

```diff
 type GarbageCollecter interface {
     RejectByAge(ages []Age) error
+    RemoveIfEqual(key string, s State) error
 }
```

And finish out our GC function.

```go
    // 2d
    for _, acceptor := range acceptors {
        if err := acceptor.RemoveIfEqual(key, killstate); err != nil {
            return err
        }
    }

    return nil
}
```

If this step fails, it means the key no longer has the tombstone (empty) value.
That means someone came in and set the key to some other value before we could
GC the space. And _that_ means we shouldn't try to GC anymore, as the value
isn't deleted! This is probably worth signaling differently to the caller, in
the form of a different type of error, so they can stop their retries.

> Each step of the GC process is idempotent so if any acceptor or proposer is
> down the process reschedules itself.

This is a lovely property which means our GC function can _fail fast_ on any
error, and the calling context can just retry until success. How nice.

> Invalidation and the cache and age check are necessary to eliminate the lost
> delete anomaly, a situation where a proposer with cached value, or a message
> delayed by a channel, [will] overwrite the combstone without a causal link.
> The update of the counters is necessary to avoid the lost update anomaly, a
> situation when a concurrently-updated value has [a] lesser ballot number,
> [and] a reader prioritizes the tombstone over the value.

Sure.

—

Observe that deletes are more like configuration changes than reads or writes
(proposals), because the process requires specific access to all proposers and
acceptors individually. I think it implies a system design that looks either
like this:

```
+------+-----+    +-----------+   +-----------+
| User | Set |--->| Proposers |-->| Acceptors |
| API  +-----+    |           |   |           |  
|      | Get |--->|           |   |           |
|      +-----+    |           |   |           |
|      | Del |--->|           |   |           |
|      |     |    +-----------+   |           |  
|      |     |------------------->|           |
+------+-----+                    +-----------+
```

Or, maybe equivalently, like this:

```
+------+-----+    +------------+   +-----------+   +-----------+
| User | Set |--->| Controller |-->| Proposers |-->| Acceptors |
| API  +-----+    |            |   |           |   |           |  
|      | Get |--->|            |-->|           |   |           |
|      +-----+    |            |   |           |   |           |
|      | Del |--->|            |-->|           |   |           |
+------+-----+    |            |   +-----------+   |           |  
                  |            |------------------>|           |
                  +------------+                   +-----------+
```

That is, the user API can't make do with access to an arbitrary proposer. Either
the user API, or a proxy for the user API, has to have access to the complete
set of all nodes in the cluster. Put still another way, a proposer's Propose
method is not a sufficient user API.

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
viable. For example, a configuration management system like Chef could drive
node provisioning. Or, a CASPaxos operator could be written for Kubernetes. As
always, carefully consider your operational requirements to determine which
approach is best for your use-case.

### API boundaries

TODO

### Cluster membership

TODO

### Fast delete, slow delete

TODO
