package caspaxos

import (
	"testing"
)

func TestZeroBallotAlwaysLoses(t *testing.T) {
	// We rely on this property in a few places.
	for _, input := range []Ballot{
		{Counter: 0, ID: 1},
		{Counter: 1, ID: 0},
		{Counter: 1, ID: 2},
		{Counter: 2, ID: 1},
		{Counter: 2, ID: 2},
	} {
		t.Run(input.String(), func(t *testing.T) {
			var zero Ballot
			if zero.greaterThan(input) {
				t.Fatal("this ballot isn't greater than the zero ballot")
			}
		})
	}
}

func TestBallotIncrement(t *testing.T) {
	var (
		orig Ballot
		prev = orig.Counter
		next = orig.inc()
	)
	if want, have := (prev + 1), next.Counter; want != have {
		t.Fatalf("returned ballot number: want %d, have %d", want, have)
	}
	if want, have := (prev + 1), orig.Counter; want != have {
		t.Fatalf("persistent ballot number: want %d, have %d", want, have)
	}
}
