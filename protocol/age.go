package protocol

import "fmt"

// Age of a proposer, incremented by GC process.
//
// rystsov: "In theory we can use ballot numbers as age, but in practice if we
// use caching (1 RTT optimization) we don't want to do it, because then a
// deletion of a single key will invalidate all caches, which isn't a nice
// behavior. Acceptors should keep the age of proposers. It can be zero in the
// beginning, and the GC process is responsible for updating it. Proposers
// should include their age as part of each message they send."
type Age struct {
	Counter uint64
	ID      string
}

func (a *Age) inc() Age {
	a.Counter++
	return *a // copy
}

// youngerThan uses the language from the paper.
func (a Age) youngerThan(other Age) bool {
	return a.Counter < other.Counter // ignoring ID
}

func (a Age) String() string {
	return fmt.Sprintf("%s:%d", a.ID, a.Counter)
}
