package httpapi

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/peterbourgon/caspaxos/protocol"
)

// HTTPClient models *http.Client.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

func setAge(h http.Header, age protocol.Age) {
	h.Set(ageHeaderKey, age.String())
}

func setAges(h http.Header, ages []protocol.Age) {
	for _, age := range ages {
		h.Add(ageHeaderKey, age.String())
	}
}

func setBallot(h http.Header, b protocol.Ballot) {
	h.Set(ballotHeaderKey, b.String())
}

func getAge(h http.Header) protocol.Age {
	counter, id := getHeaderPair(h.Get(ageHeaderKey), ageRe)
	return protocol.Age{Counter: counter, ID: id}
}

func getAges(h http.Header) (ages []protocol.Age) {
	for _, s := range h[ageHeaderKey] {
		counter, id := getHeaderPair(s, ageRe)
		ages = append(ages, protocol.Age{Counter: counter, ID: id})
	}
	return ages
}

func getBallot(h http.Header) protocol.Ballot {
	s := h.Get(ballotHeaderKey)
	if s == "ø" {
		return protocol.Ballot{}
	}
	counter, id := getHeaderPair(s, ballotRe)
	return protocol.Ballot{Counter: counter, ID: id}
}

func getHeaderPair(s string, re *regexp.Regexp) (uint64, string) {
	matches := re.FindAllStringSubmatch(s, 1)
	if len(matches) != 1 {
		panic("bad input: '" + s + "' (regex: " + re.String() + ")")
	}
	if len(matches[0]) != 3 {
		panic("bad input: '" + s + "' (regex: " + re.String() + ")")
	}
	counterstr, id := matches[0][1], matches[0][2]
	counter, _ := strconv.ParseUint(counterstr, 10, 64)
	return counter, id
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
		return version, value, errors.New("state slice is too small")
	}
	version = binary.LittleEndian.Uint64(state[:8])
	value = state[8:]
	return version, value, nil
}

func setVersion(h http.Header, version uint64) {
	h.Set(versionHeaderKey, fmt.Sprint(version))
}

func getVersion(h http.Header) uint64 {
	version, _ := strconv.ParseUint(h.Get(versionHeaderKey), 10, 64)
	return version
}
