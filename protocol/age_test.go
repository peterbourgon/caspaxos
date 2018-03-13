package protocol

import "testing"

func TestZeroAgeAlwaysLoses(t *testing.T) {
	// We rely on this property in a few places.
	for _, input := range []Age{
		{Counter: 0, ID: "a"},
		{Counter: 1, ID: ""},
		{Counter: 1, ID: "b"},
		{Counter: 2, ID: "a"},
		{Counter: 2, ID: "b"},
	} {
		t.Run(input.String(), func(t *testing.T) {
			var zero Age
			if input.youngerThan(zero) {
				t.Fatal("this age isn't younger than the zero age")
			}
		})
	}
}
