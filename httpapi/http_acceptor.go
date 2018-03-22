package httpapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	"github.com/peterbourgon/caspaxos/protocol"
)

// AcceptorServer provides an HTTP interface to an acceptor.
type AcceptorServer struct {
	acceptor protocol.Acceptor
	*mux.Router
}

var _ http.Handler = (*AcceptorServer)(nil)

const (
	ageHeaderKey      = "X-Caspaxos-Age"
	ageHeaderRegex    = "^([0-9]+):(.*)$"
	ballotHeaderKey   = "X-Caspaxos-Ballot"
	ballotHeaderRegex = "^([0-9]+)/(.*)$"
	versionHeaderKey  = "X-Caspaxos-Ext-Version"
)

var (
	ageRe    = regexp.MustCompile(ageHeaderRegex)
	ballotRe = regexp.MustCompile(ballotHeaderRegex)
)

// NewAcceptorServer returns a usable AcceptorServer wrapping the passed acceptor.
func NewAcceptorServer(acceptor protocol.Acceptor) *AcceptorServer {
	as := &AcceptorServer{
		acceptor: acceptor,
	}
	r := mux.NewRouter()
	{
		r.StrictSlash(true)
		r.Methods("POST").Path("/prepare/{key}").HeadersRegexp(ageHeaderKey, ageHeaderRegex, ballotHeaderKey, ballotHeaderRegex).HandlerFunc(as.handlePrepare)
		r.Methods("POST").Path("/accept/{key}").HeadersRegexp(ageHeaderKey, ageHeaderRegex, ballotHeaderKey, ballotHeaderRegex).HandlerFunc(as.handleAccept)
		r.Methods("POST").Path("/reject-by-age").HeadersRegexp(ageHeaderKey, ageHeaderRegex).HandlerFunc(as.handleRejectByAge)
		r.Methods("POST").Path("/remove-if-equal/{key}").HandlerFunc(as.handleRemoveIfEqual)
	}
	as.Router = r
	return as
}

// Prepare(ctx context.Context, key string, age Age, b Ballot) (value []byte, current Ballot, err error)
func (as *AcceptorServer) handlePrepare(w http.ResponseWriter, r *http.Request) {
	var (
		key, _ = url.PathUnescape(mux.Vars(r)["key"])
		age    = getAge(r.Header)
		b      = getBallot(r.Header)
	)

	state, ballot, err := as.acceptor.Prepare(r.Context(), key, age, b)
	setBallot(w.Header(), ballot)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(state)
}

// Accept(ctx context.Context, key string, age Age, b Ballot, value []byte) error
func (as *AcceptorServer) handleAccept(w http.ResponseWriter, r *http.Request) {
	var (
		key, _ = url.PathUnescape(mux.Vars(r)["key"])
		age    = getAge(r.Header)
		b      = getBallot(r.Header)
	)

	state, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := as.acceptor.Accept(r.Context(), key, age, b, state); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "OK")
}

// RejectByAge(ctx context.Context, ages []Age) error
func (as *AcceptorServer) handleRejectByAge(w http.ResponseWriter, r *http.Request) {
	ages := getAges(r.Header)
	if err := as.acceptor.RejectByAge(r.Context(), ages); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "OK")
}

// RemoveIfEqual(ctx context.Context, key string, state []byte) error
func (as *AcceptorServer) handleRemoveIfEqual(w http.ResponseWriter, r *http.Request) {
	key, _ := url.PathUnescape(mux.Vars(r)["key"])

	state, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := as.acceptor.RemoveIfEqual(r.Context(), key, state); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "OK")
}

//
//
//

// AcceptorClient implements the protocol.Acceptor interface by making calls to
// a remote AcceptorServer.
type AcceptorClient struct {
	// HTTPClient to make requests. Optional.
	// If nil, http.DefaultClient is used.
	Client interface {
		Do(*http.Request) (*http.Response, error)
	}

	// URL of the remote AcceptorServer.
	// Only scheme and host are used.
	URL *url.URL
}

// Address implements protocol.Acceptor.
func (ac AcceptorClient) Address() string {
	return ac.URL.String()
}

// Prepare implements protocol.Acceptor.
func (ac AcceptorClient) Prepare(ctx context.Context, key string, age protocol.Age, b protocol.Ballot) (value []byte, current protocol.Ballot, err error) {
	u := *ac.URL
	u.Path = fmt.Sprintf("/prepare/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), nil)
	req = req.WithContext(ctx)
	setAge(req.Header, age)
	setBallot(req.Header, b)

	resp, err := ac.httpClient().Do(req)
	if err != nil {
		return value, current, err
	}

	current = getBallot(resp.Header)

	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return value, current, errors.New(strings.TrimSpace(string(buf)))
	}

	value, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return value, current, err
	}

	return value, current, nil
}

// Accept implements protocol.Acceptor.
func (ac AcceptorClient) Accept(ctx context.Context, key string, age protocol.Age, b protocol.Ballot, value []byte) error {
	u := *ac.URL
	u.Path = fmt.Sprintf("/accept/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), bytes.NewReader(value))
	req = req.WithContext(ctx)
	setAge(req.Header, age)
	setBallot(req.Header, b)

	resp, err := ac.httpClient().Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(buf)))
	}

	return nil
}

// RejectByAge implements protocol.Acceptor.
func (ac AcceptorClient) RejectByAge(ctx context.Context, ages []protocol.Age) error {
	u := *ac.URL
	u.Path = "/reject-by-age"
	req, _ := http.NewRequest("POST", u.String(), nil)
	req = req.WithContext(ctx)
	setAges(req.Header, ages)

	resp, err := ac.httpClient().Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(buf)))
	}

	return nil
}

// RemoveIfEqual implements protocol.Acceptor.
func (ac AcceptorClient) RemoveIfEqual(ctx context.Context, key string, state []byte) error {
	u := *ac.URL
	u.Path = fmt.Sprintf("/remove-if-equal/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), bytes.NewReader(state))
	req = req.WithContext(ctx)

	resp, err := ac.httpClient().Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(buf)))
	}

	return nil
}

func (ac AcceptorClient) httpClient() interface {
	Do(*http.Request) (*http.Response, error)
} {
	client := ac.Client
	if client == nil {
		client = http.DefaultClient
	}
	return client
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
