package protocol

import "fmt"

// Ballot models a ballot number, which are maintained by proposers and cached
// by acceptors.
//
// From the paper: "It's convenient to use tuples as ballot numbers. To generate
// it a proposer combines its numerical ID with a local increasing counter:
// (counter, ID)."
type Ballot struct {
	Counter uint64
	ID      string
}

func (b *Ballot) inc() Ballot {
	b.Counter++
	return *b // copy
}

func (b *Ballot) isZero() bool {
	return b.Counter == 0 && b.ID == ""
}

// From the paper: "To compare ballot tuples, we should compare the first
// component of the tuples and use ID only as a tiebreaker."
func (b *Ballot) greaterThan(other Ballot) bool {
	if b.Counter == other.Counter {
		return b.ID > other.ID
	}
	return b.Counter > other.Counter
}

func (b Ballot) String() string {
	if b.isZero() {
		return "ø"
	}
	return fmt.Sprintf("%d/%s", b.Counter, b.ID)
}
