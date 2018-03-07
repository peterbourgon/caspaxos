package httpapi

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/peterbourgon/caspaxos/protocol"
)

// AcceptorServer wraps a protocol.Acceptor and provides a basic HTTP API.
// It's an internal API; it should only be called by proposers, via the
// AcceptorClient.
//
//     POST /prepare/{key}
//         Prepare request for the given key.
//         Expects and returns "X-Caspaxos-Ballot: Counter/ID" header.
//         Returns 412 Precondition Failed on protocol error.
//
//     POST /accept/{key}?value={value}
//         Accept request for the given key and value. Value may be empty.
//         Expects "X-Caspaxos-Ballot: Counter/ID" header.
//         Returns 406 Not Acceptable on protocol error.
//
type AcceptorServer struct {
	http.Handler
	acceptor protocol.Acceptor
	logger   log.Logger
}

// NewAcceptorServer returns an AcceptorServer wrapping the provided acceptor.
// The AcceptorServer is an http.Handler and can ServeHTTP.
func NewAcceptorServer(acceptor protocol.Acceptor, logger log.Logger) AcceptorServer {
	as := AcceptorServer{
		acceptor: acceptor,
		logger:   logger,
	}
	{
		r := mux.NewRouter().StrictSlash(true)
		r = r.Methods("POST").HeadersRegexp(ballotHeaderKey, "([0-9]+)/([0-9]+)").Subrouter()
		r.HandleFunc("/prepare/{key}", as.handlePrepare)
		r.HandleFunc("/accept/{key}", as.handleAccept)
		as.Handler = r
	}
	return as
}

func (as AcceptorServer) handlePrepare(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{w, http.StatusOK}
	defer func(begin time.Time) {
		level.Info(as.logger).Log(
			"handler", "handlePrepare",
			"method", r.Method,
			"url", r.URL.String(),
			ballotHeaderKey, r.Header.Get(ballotHeaderKey),
			"took", time.Since(begin),
			"status", iw.code,
		)
	}(time.Now())
	w = iw

	key := mux.Vars(r)["key"]
	if key == "" {
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	b, err := header2ballot(r.Header)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(as.logger).Log("invoking", "Prepare", "key", key, "B", b)
	val, current, err := as.acceptor.Prepare(r.Context(), key, b)
	level.Debug(as.logger).Log("resultof", "Prepare", "val", string(val), "current", current, "err", err)
	ballot2header(current, w.Header())
	if err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%s", val)
}

func (as AcceptorServer) handleAccept(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{w, http.StatusOK}
	defer func(begin time.Time) {
		level.Info(as.logger).Log(
			"handler", "handleAccept",
			"method", r.Method,
			"url", r.URL.String(),
			ballotHeaderKey, r.Header.Get(ballotHeaderKey),
			"took", time.Since(begin),
			"status", iw.code,
		)
	}(time.Now())
	w = iw

	vars := mux.Vars(r)
	key := vars["key"]
	if key == "" {
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	var valueBytes []byte
	if value := r.URL.Query().Get("value"); value != "" {
		valueBytes = []byte(value)
	}

	b, err := header2ballot(r.Header)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(as.logger).Log("invoking", "Accept", "key", key, "B", b, "value", string(valueBytes))
	if err = as.acceptor.Accept(r.Context(), key, b, valueBytes); err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "OK")
}

// AcceptorClient implements protocol.Acceptor by making HTTP requests to a
// remote AcceptorServer.
type AcceptorClient struct {
	// URL of the remote acceptor HTTP API. Required.
	URL *url.URL

	// HTTPClient used to make remote HTTP requests. Optional.
	// By default, http.DefaultClient is used.
	HTTPClient interface {
		Do(*http.Request) (*http.Response, error)
	}
}

// Address implements protocol.Acceptor, returning the wrapped URL.
func (ac AcceptorClient) Address() string {
	return ac.URL.String()
}

// Prepare implements protocol.Acceptor by making an HTTP request to the remote
// acceptor API.
func (ac AcceptorClient) Prepare(ctx context.Context, key string, b protocol.Ballot) (value []byte, current protocol.Ballot, err error) {
	client := ac.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	u := *ac.URL
	u.Path = fmt.Sprintf("/prepare/%s", url.PathEscape(key))
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, protocol.Ballot{}, errors.Wrap(err, "constructing HTTP request")
	}

	ballot2header(b, req.Header)
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return nil, protocol.Ballot{}, errors.Wrap(err, "executing HTTP request")
	}

	current, err = header2ballot(resp.Header)
	if err != nil {
		return nil, protocol.Ballot{}, errors.Wrap(err, "extracting response ballot")
	}

	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return nil, current, fmt.Errorf("%s (%s)", resp.Status, strings.TrimSpace(string(buf)))
	}

	value, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, current, errors.Wrap(err, "consuming response value")
	}

	if len(value) == 0 {
		value = nil
	}
	return value, current, nil
}

// Accept implements protocol.Acceptor by making an HTTP request to the remote
// acceptor API.
func (ac AcceptorClient) Accept(ctx context.Context, key string, b protocol.Ballot, value []byte) error {
	client := ac.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	u := *ac.URL
	u.Path = fmt.Sprintf("/accept/%s", url.PathEscape(key))
	if value != nil {
		u.RawQuery = fmt.Sprintf("value=%s", url.QueryEscape(string(value)))
	}
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return errors.Wrap(err, "constructing HTTP request")
	}

	ballot2header(b, req.Header)
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "executing HTTP request")
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	return nil
}

const ballotHeaderKey = "X-Caspaxos-Ballot"

func header2ballot(h http.Header) (protocol.Ballot, error) {
	ballot := h.Get(ballotHeaderKey)
	if ballot == "" {
		return protocol.Ballot{}, fmt.Errorf("%s not provided", ballotHeaderKey)
	}
	tokens := strings.SplitN(ballot, "/", 2)
	if len(tokens) != 2 {
		return protocol.Ballot{}, fmt.Errorf("%s has invalid format", ballotHeaderKey)
	}
	counter, err := strconv.ParseUint(tokens[0], 10, 64)
	if err != nil {
		return protocol.Ballot{}, fmt.Errorf("%s has invalid Counter value %q", ballotHeaderKey, tokens[0])
	}
	id, err := strconv.ParseUint(tokens[1], 10, 64)
	if err != nil {
		return protocol.Ballot{}, fmt.Errorf("%s has invalid ID value %q", ballotHeaderKey, tokens[1])
	}
	return protocol.Ballot{Counter: counter, ID: id}, nil
}

func ballot2header(b protocol.Ballot, h http.Header) {
	h.Set(ballotHeaderKey, fmt.Sprintf("%d/%d", b.Counter, b.ID))
}
