package extension

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/peterbourgon/caspaxos/protocol"
	"github.com/pkg/errors"
)

// Proposer models serializable methods of a proposer. ChangeFuncs aren't
// serializable, so this means the Propose method is dropped in favor of Read
// and CAS. The CAS protocol is taken from the paper, with a (Version, Value)
// tuple.
type Proposer interface {
	protocol.Addresser
	Read(ctx context.Context, key string) (version uint64, value []byte, err error)
	CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error)
	protocol.ConfigurationChanger
	protocol.FastForwarder
	protocol.AcceptorLister
}

// CASError indicates a conflict during CAS, likely a version conflict.
type CASError struct{ Err error }

// Error implements the error interface.
func (e CASError) Error() string {
	return fmt.Sprintf("CAS error: %v", e.Err)
}

//
//
//

// SimpleProposer wraps a protocol.Proposer and implements extension.Proposer.
type SimpleProposer struct {
	protocol.Proposer
}

var _ Proposer = (*SimpleProposer)(nil)

// NewSimpleProposer returns a usable SimpleProposer wrapping the given
// protocol.Proposer.
func NewSimpleProposer(target protocol.Proposer) *SimpleProposer {
	return &SimpleProposer{target}
}

// Propose blocks calls to protocol.Proposer by panicking.
func (p SimpleProposer) Propose(ctx context.Context, key string, f protocol.ChangeFunc) (state []byte, b protocol.Ballot, err error) {
	panic("can't call Propose on extension.SimpleProposer")
}

// Read implements Proposer.
func (p SimpleProposer) Read(ctx context.Context, key string) (version uint64, value []byte, err error) {
	state, _, err := p.Proposer.Propose(ctx, key, func(x []byte) []byte { return x })
	if err != nil {
		return version, value, err
	}
	return parseVersionValue(state)
}

// CAS implements Proposer.
func (p SimpleProposer) CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error) {
	receivedState, _, err := p.Proposer.Propose(ctx, key, func(actualState []byte) []byte {
		actualVersion, _, err := parseVersionValue(actualState)
		if err != nil {
			return actualState // fail
		}
		if currentVersion != actualVersion {
			return actualState // fail
		}
		return makeVersionValue(currentVersion+1, nextValue) // success
	})
	if err != nil {
		return version, value, CASError{Err: err}
	}
	receivedVersion, receivedValue, err := parseVersionValue(receivedState)
	if err != nil {
		return version, value, err
	}
	if !bytes.Equal(receivedValue, nextValue) { // TODO(pb): is this check actually necessary?
		return version, value, CASError{Err: errors.New("CAS unsuccessful")}
	}
	return receivedVersion, receivedValue, err
}

func makeVersionValue(version uint64, value []byte) (state []byte) {
	state = make([]byte, 8+len(value))
	binary.LittleEndian.PutUint64(state[:8], version)
	copy(state[8:], value)
	return state
}

func parseVersionValue(state []byte) (version uint64, value []byte, err error) {
	if len(state) == 0 {
		return version, value, nil
	}
	if len(state) < 8 {
		return version, value, errors.New("expectation failed: state slice is too small (programmer error)")
	}
	version = binary.LittleEndian.Uint64(state[:8])
	value = state[8:]
	return version, value, nil
}
