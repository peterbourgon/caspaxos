package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/internal/eventsource"
	"github.com/peterbourgon/caspaxos/protocol"
)

// AcceptorServer wraps an extension.Acceptor and provides its methodset over
// HTTP. Best used with AcceptorClient.
type AcceptorServer struct {
	acceptor extension.Acceptor
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
		r.Methods("POST").Path("/watch/{key}").HandlerFunc(as.handleWatch)
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

// Watch(ctx context.Context, key string, states chan<- []byte) error
func (as *AcceptorServer) handleWatch(w http.ResponseWriter, r *http.Request) {
	var (
		key, _      = url.PathUnescape(mux.Vars(r)["key"])
		states      = make(chan []byte)
		errs        = make(chan error)
		enc         = eventsource.NewEncoder(w)
		ctx, cancel = context.WithCancel(r.Context())
	)

	go func() {
		errs <- as.acceptor.Watch(ctx, key, states)
	}()

	// via eventsource.Handler.ServeHTTP
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Vary", "Accept")
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)

	for {
		select {
		case state := <-states:
			if err := enc.Encode(eventsource.Event{Data: state}); err != nil {
				cancel()
				<-errs
				return
			}

		case <-errs:
			cancel() // no-op for linter
			return   // the watcher goroutine is dead
		}
	}
}

//
//
//

// AcceptorClient implements the extension.Acceptor interface by making calls to
// a remote AcceptorServer.
type AcceptorClient struct {
	// HTTPClient to make requests. Optional.
	// If nil, http.DefaultClient is used.
	Client HTTPClient

	// URL of the remote AcceptorServer.
	// Only scheme and host are used.
	URL *url.URL

	// WatchRetry is the time between reconnect attempts
	// if a Watch session goes bad. Default of 1 second.
	WatchRetry time.Duration
}

var _ extension.Acceptor = (*AcceptorClient)(nil)

// Address implements extension.Acceptor.
func (ac AcceptorClient) Address() string {
	return ac.URL.String()
}

// Prepare implements extension.Acceptor.
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
	return value, current, err
}

// Accept implements extension.Acceptor.
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

// RejectByAge implements extension.Acceptor.
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

// RemoveIfEqual implements extension.Acceptor.
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

// Watch implements extension.Acceptor.
func (ac AcceptorClient) Watch(ctx context.Context, key string, states chan<- []byte) error {
	u := *ac.URL
	u.Path = fmt.Sprintf("/watch/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), nil)
	req = req.WithContext(ctx)

	retry := ac.WatchRetry
	if retry <= 0 {
		retry = time.Second
	}

	s := eventsource.New(req, retry) // TODO(pb): this uses DefaultClient
	defer s.Close()

	for {
		ev, err := s.Read()
		if err != nil {
			return errors.Wrap(err, "remote acceptor EventSource error")
		}
		states <- ev.Data
	}
}

func (ac AcceptorClient) httpClient() HTTPClient {
	client := ac.Client
	if client == nil {
		client = http.DefaultClient
	}
	return client
}
