package caspaxos

import "testing"

func changeFuncInitializeOnlyOnce(s string) ChangeFunc {
	return func(x []byte) []byte {
		if x == nil {
			return []byte(s)
		}
		return x
	}
}

func changeFuncRead(x []byte) []byte {
	return x
}

type testWriter struct{ t *testing.T }

func (tw testWriter) Write(p []byte) (int, error) {
	tw.t.Logf("%s", string(p))
	return len(p), nil
}
