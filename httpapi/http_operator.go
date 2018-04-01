package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/internal/eventsource"
	"github.com/peterbourgon/caspaxos/protocol"
)

// OperatorServer provides an HTTP interface to an operator node.
type OperatorServer struct {
	operator   extension.Operator
	httpClient HTTPClient
	*mux.Router
}

var _ http.Handler = (*OperatorServer)(nil)

// NewOperatorServer returns a usable OperatorServer wrapping the passed
// operator node. Many methods are implemented as blind proxies to proposer or
// acceptor nodes, for which the HTTP client is used; if nil, http.DefaultClient
// is used.
func NewOperatorServer(op extension.Operator, c HTTPClient) *OperatorServer {
	if c == nil {
		c = http.DefaultClient
	}
	os := &OperatorServer{
		operator:   op,
		httpClient: c,
	}
	r := mux.NewRouter()
	{
		r.StrictSlash(true)
		r.Methods("GET").Path("/cluster-state").HandlerFunc(os.handleClusterState)
		r.Methods("POST").Path("/read/{key}").HandlerFunc(os.handleRead)
		r.Methods("POST").Path("/cas/{key}").HandlerFunc(os.handleCAS)
		r.Methods("POST").Path("/watch/{key}").HandlerFunc(os.handleWatch)
		r.Methods("GET").Path("/list-acceptors").HandlerFunc(os.handleListAcceptors)
		r.Methods("POST").Path("/add-acceptor").HandlerFunc(os.handleAddAcceptor)
		r.Methods("POST").Path("/remove-acceptor").HandlerFunc(os.handleRemoveAcceptor)
		r.Methods("GET").Path("/list-proposers").HandlerFunc(os.handleListProposers)
	}
	os.Router = r
	return os
}

// ClusterState(ctx context.Context) (s extension.ClusterState, err error)
func (os *OperatorServer) handleClusterState(w http.ResponseWriter, r *http.Request) {
	s, err := os.operator.ClusterState(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(s)
}

// Read(ctx context.Context, key string) (version uint64, value []byte, err error)
func (os *OperatorServer) handleRead(w http.ResponseWriter, r *http.Request) {
	// Pick any proposer to service the read.
	a, err := os.operator.ListProposers(r.Context())
	if err != nil {
		http.Error(w, errors.Wrap(err, "listing current proposers").Error(), http.StatusServiceUnavailable)
		return
	}

	// That proposer is returned as an address, which we assume is a URL.
	addr := a[rand.Intn(len(a))]
	u, err := url.Parse(addr)
	if err != nil {
		http.Error(w, errors.Wrap(err, "parsing proposer address").Error(), http.StatusInternalServerError)
		return
	}

	// Construct an HTTP ProposerClient around that URL and invoke the read.
	var (
		target = ProposerClient{Client: os.httpClient, URL: u}
		key    = mux.Vars(r)["key"]
	)
	version, value, err := target.Read(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the response.
	setVersion(w.Header(), version)
	w.Write(value)
}

// CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error)
func (os *OperatorServer) handleCAS(w http.ResponseWriter, r *http.Request) {
	// Pick any proposer to service the CAS.
	a, err := os.operator.ListProposers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// That proposer is returned as an address, which we assume is a URL.
	addr := a[rand.Intn(len(a))]
	u, err := url.Parse(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Construct an HTTP ProposerClient around that URL and invoke the CAS.
	var (
		target         = ProposerClient{Client: os.httpClient, URL: u}
		key            = mux.Vars(r)["key"]
		currentVersion = getVersion(r.Header)
		nextValue, _   = ioutil.ReadAll(r.Body)
	)
	version, value, err := target.CAS(r.Context(), key, currentVersion, nextValue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the response.
	setVersion(w.Header(), version)
	w.Write(value)
}

// Watch(ctx context.Context, key string, values chan<- []byte) error
func (os *OperatorServer) handleWatch(w http.ResponseWriter, r *http.Request) {
	// Pick any acceptor to service the CAS.
	a, err := os.operator.ListAcceptors(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// That acceptor is returned as an address, which we assume is a URL.
	addr := a[rand.Intn(len(a))]
	u, err := url.Parse(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Construct an HTTP AcceptorClient around that URL and invoke the watch.
	var (
		target         = AcceptorClient{Client: os.httpClient, URL: u}
		key            = mux.Vars(r)["key"]
		states         = make(chan []byte)
		errs           = make(chan error, 1)
		enc            = eventsource.NewEncoder(w)
		subctx, cancel = context.WithCancel(r.Context())
	)

	// Invoke the watch.
	go func() {
		errs <- target.Watch(subctx, key, states)
	}()

	// via eventsource.Handler.ServeHTTP
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Vary", "Accept")
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)

	for {
		select {
		case state := <-states:
			// The acceptor's watch method yields states.
			// But we serve the user API, which wants values.
			_, value, err := parseVersionValue(state)
			if err != nil {
				cancel()
				<-errs
				return
			}
			if err := enc.Encode(eventsource.Event{Data: value}); err != nil {
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

// ListAcceptors(ctx context.Context) ([]string, error)
func (os *OperatorServer) handleListAcceptors(w http.ResponseWriter, r *http.Request) {
	a, err := os.operator.ListAcceptors(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(a)
}

// AddAcceptor(ctx context.Context, target protocol.Acceptor) error
func (os *OperatorServer) handleAddAcceptor(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	target := AcceptorClient{Client: os.httpClient, URL: u}
	if err := os.operator.AddAcceptor(r.Context(), target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "OK")
}

// RemoveAcceptor(ctx context.Context, target protocol.Acceptor) error
func (os *OperatorServer) handleRemoveAcceptor(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	target := AcceptorClient{Client: os.httpClient, URL: u}
	if err := os.operator.RemoveAcceptor(r.Context(), target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "OK")
}

// ListProposers(ctx context.Context) ([]string, error)
func (os *OperatorServer) handleListProposers(w http.ResponseWriter, r *http.Request) {
	a, err := os.operator.ListProposers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(a)
}

//
//
//

// OperatorClient implements the extension.Operator interface by making
// calls to a remote OperatorServer.
type OperatorClient struct {
	// HTTPClient to make requests. Optional.
	// If nil, http.DefaultClient is used.
	Client HTTPClient

	// URL of the remote OperatorServer.
	// Only scheme and host are used.
	URL *url.URL

	// WatchRetry is the time between reconnect attempts
	// if a Watch session goes bad. Default of 1 second.
	WatchRetry time.Duration
}

var _ extension.Operator = (*OperatorClient)(nil)

// Address implements extension.Operator.
func (oc OperatorClient) Address() string {
	return oc.URL.String()
}

// Read implements extension.Operator.
func (oc OperatorClient) Read(ctx context.Context, key string) (version uint64, value []byte, err error) {
	u := *oc.URL
	u.Path = fmt.Sprintf("/read/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), nil)
	req = req.WithContext(ctx)
	resp, err := oc.httpClient().Do(req)
	if err != nil {
		return 0, nil, err
	}

	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return 0, nil, errors.New(strings.TrimSpace(string(buf)))
	}

	version = getVersion(resp.Header)
	value, err = ioutil.ReadAll(resp.Body)
	return version, value, err
}

// CAS implements extension.Operator.
func (oc OperatorClient) CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error) {
	u := *oc.URL
	u.Path = fmt.Sprintf("/cas/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), bytes.NewReader(nextValue))
	setVersion(req.Header, currentVersion)
	req = req.WithContext(ctx)
	resp, err := oc.httpClient().Do(req)
	if err != nil {
		return version, value, err
	}

	switch {
	case resp.StatusCode == http.StatusPreconditionFailed: // 412 -> CASError
		buf, _ := ioutil.ReadAll(resp.Body)
		return version, value, extension.CASError{Err: errors.New(strings.TrimSpace(string(buf)))}
	case resp.StatusCode != http.StatusOK:
		buf, _ := ioutil.ReadAll(resp.Body)
		return version, value, errors.New(strings.TrimSpace(string(buf)))
	}

	version = getVersion(resp.Header)
	value, err = ioutil.ReadAll(resp.Body)
	return version, value, err
}

// Watch implements extension.Operator.
func (oc OperatorClient) Watch(ctx context.Context, key string, values chan<- []byte) error {
	u := *oc.URL
	u.Path = fmt.Sprintf("/watch/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), nil)
	req = req.WithContext(ctx)

	retry := oc.WatchRetry
	if retry <= 0 {
		retry = time.Second
	}

	s := eventsource.New(req, retry) // TODO(pb): this uses DefaultClient
	defer s.Close()

	for {
		ev, err := s.Read()
		if err != nil {
			return errors.Wrap(err, "remote operator EventSource error")
		}
		values <- ev.Data
	}
}

// ClusterState implements extension.Operator.
func (oc OperatorClient) ClusterState(ctx context.Context) (s extension.ClusterState, err error) {
	u := *oc.URL
	u.Path = "/cluster-state"
	req, _ := http.NewRequest("GET", u.String(), nil)
	req = req.WithContext(ctx)
	resp, err := oc.httpClient().Do(req)
	if err != nil {
		return s, errors.Wrap(err, "querying remote operator server")
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		body := strings.TrimSpace(string(buf))
		return s, errors.Wrapf(err, "remote operator server returned %d (%s)", resp.StatusCode, body)
	}
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

// ListAcceptors implements extension.Operator.
func (oc OperatorClient) ListAcceptors(ctx context.Context) (a []string, err error) {
	u := *oc.URL
	u.Path = "/list-acceptors"
	req, _ := http.NewRequest("GET", u.String(), nil)
	req = req.WithContext(ctx)
	resp, err := oc.httpClient().Do(req)
	if err != nil {
		return a, errors.Wrap(err, "querying remote operator server")
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		body := strings.TrimSpace(string(buf))
		return a, errors.Wrapf(err, "remote operator server returned %d (%s)", resp.StatusCode, body)
	}
	return a, json.NewDecoder(resp.Body).Decode(&a)
}

// AddAcceptor implements extension.Operator.
func (oc OperatorClient) AddAcceptor(ctx context.Context, target protocol.Acceptor) error {
	u := *oc.URL
	u.Path = "/add-acceptor"
	req, _ := http.NewRequest("POST", u.String(), nil)
	req.Body = ioutil.NopCloser(strings.NewReader(target.Address()))
	req = req.WithContext(ctx)
	resp, err := oc.httpClient().Do(req)
	if err != nil {
		return errors.Wrap(err, "querying remote operator server")
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		body := strings.TrimSpace(string(buf))
		return errors.Wrapf(err, "remote operator server returned %d (%s)", resp.StatusCode, body)
	}
	return nil
}

// RemoveAcceptor implements extension.Operator.
func (oc OperatorClient) RemoveAcceptor(ctx context.Context, target protocol.Acceptor) error {
	u := *oc.URL
	u.Path = "/remove-acceptor"
	req, _ := http.NewRequest("POST", u.String(), nil)
	req.Body = ioutil.NopCloser(strings.NewReader(target.Address()))
	req = req.WithContext(ctx)
	resp, err := oc.httpClient().Do(req)
	if err != nil {
		return errors.Wrap(err, "querying remote operator server")
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		body := strings.TrimSpace(string(buf))
		return errors.Wrapf(err, "remote operator server returned %d (%s)", resp.StatusCode, body)
	}
	return nil
}

// ListProposers implements extension.Operator.
func (oc OperatorClient) ListProposers(ctx context.Context) (a []string, err error) {
	u := *oc.URL
	u.Path = "/list-proposers"
	req, _ := http.NewRequest("GET", u.String(), nil)
	req = req.WithContext(ctx)
	resp, err := oc.httpClient().Do(req)
	if err != nil {
		return a, errors.Wrap(err, "querying remote operator server")
	}
	return a, json.NewDecoder(resp.Body).Decode(&a)
}

func (oc OperatorClient) httpClient() HTTPClient {
	client := oc.Client
	if client == nil {
		client = http.DefaultClient
	}
	return client
}
