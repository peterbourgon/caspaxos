package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"

	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/protocol"
)

// ProposerServer wraps an extension.Proposer and provides its methodset over
// HTTP. Best used with ProposerClient.
type ProposerServer struct {
	proposer protocol.Proposer
	*mux.Router
}

var _ http.Handler = (*ProposerServer)(nil)

// NewProposerServer returns a usable ProposerServer wrapping the passed proposer.
func NewProposerServer(proposer protocol.Proposer) *ProposerServer {
	ps := &ProposerServer{
		proposer: proposer,
	}
	r := mux.NewRouter()
	{
		r.StrictSlash(true)
		r.Methods("POST").Path("/read/{key}").HandlerFunc(ps.handleRead)
		r.Methods("POST").Path("/cas/{key}").HandlerFunc(ps.handleCAS)
		r.Methods("POST").Path("/identity-read/{key}").HandlerFunc(ps.handleIdentityRead)
		r.Methods("POST").Path("/add-accepter").HandlerFunc(ps.handleAddAccepter)
		r.Methods("POST").Path("/add-preparer").HandlerFunc(ps.handleAddPreparer)
		r.Methods("POST").Path("/remove-preparer").HandlerFunc(ps.handleRemovePreparer)
		r.Methods("POST").Path("/remove-accepter").HandlerFunc(ps.handleRemoveAccepter)
		r.Methods("POST").Path("/full-identity-read/{key}").HandlerFunc(ps.handleFullIdentityRead)
		r.Methods("POST").Path("/fast-forward-increment/{key}").HandlerFunc(ps.handleFastForwardIncrement)
		r.Methods("GET").Path("/list-preparers").HandlerFunc(ps.handleListPreparers)
		r.Methods("GET").Path("/list-accepters").HandlerFunc(ps.handleListAccepters)
	}
	ps.Router = r
	return ps
}

func (ps *ProposerServer) handleRead(w http.ResponseWriter, r *http.Request) {
	var (
		key, _ = url.PathUnescape(mux.Vars(r)["key"])
		read   = func(x []byte) []byte { return x }
	)

	state, b, err := ps.proposer.Propose(r.Context(), key, read)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	version, value, err := parseVersionValue(state)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setBallot(w.Header(), b) // not strictly necessary, I don't think
	setVersion(w.Header(), version)
	w.Write(value)
}

func (ps *ProposerServer) handleCAS(w http.ResponseWriter, r *http.Request) {
	var (
		key, _         = url.PathUnescape(mux.Vars(r)["key"])
		currentVersion = getVersion(r.Header)
		nextValue, _   = ioutil.ReadAll(r.Body)
	)

	cas := func(x []byte) []byte {
		if version, _, err := parseVersionValue(x); err == nil && version == currentVersion {
			return makeVersionValue(version+1, nextValue)
		}
		return x
	}

	state, _, err := ps.proposer.Propose(r.Context(), key, cas)
	if _, ok := err.(protocol.ConflictError); ok {
		http.Error(w, err.Error(), http.StatusPreconditionFailed) // ConflictError -> 412 (CASError)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	version, value, err := parseVersionValue(state)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setVersion(w.Header(), version)
	w.Write(value)
}

func (ps *ProposerServer) handleIdentityRead(w http.ResponseWriter, r *http.Request) {
	ps.handleRead(w, r)
}

// AddAccepter(target Acceptor) error
func (ps *ProposerServer) handleAddAccepter(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := AcceptorClient{URL: u}
	if err := ps.proposer.AddAccepter(target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "OK")
}

// AddPreparer(target Acceptor) error
func (ps *ProposerServer) handleAddPreparer(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := AcceptorClient{URL: u}
	if err := ps.proposer.AddPreparer(target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "OK")
}

// RemovePreparer(target Acceptor) error
func (ps *ProposerServer) handleRemovePreparer(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := AcceptorClient{URL: u}
	if err := ps.proposer.RemovePreparer(target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "OK")
}

// RemoveAccepter(target Acceptor) error
func (ps *ProposerServer) handleRemoveAccepter(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := AcceptorClient{URL: u}
	if err := ps.proposer.RemoveAccepter(target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "OK")
}

// FullIdentityRead(ctx context.Context, key string) (state []byte, err error)
func (ps *ProposerServer) handleFullIdentityRead(w http.ResponseWriter, r *http.Request) {
	key, _ := url.PathUnescape(mux.Vars(r)["key"])
	state, b, err := ps.proposer.FullIdentityRead(r.Context(), key)
	setBallot(w.Header(), b)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(state)
}

// FastForwardIncrement(ctx context.Context, key string, tombstone Ballot) (Age, error)
func (ps *ProposerServer) handleFastForwardIncrement(w http.ResponseWriter, r *http.Request) {
	var (
		key, _    = url.PathUnescape(mux.Vars(r)["key"])
		tombstone = getBallot(r.Header)
	)
	age, err := ps.proposer.FastForwardIncrement(r.Context(), key, tombstone)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setAge(w.Header(), age)
	fmt.Fprintln(w, "OK")
}

func (ps *ProposerServer) handleListPreparers(w http.ResponseWriter, r *http.Request) {
	addrs, err := ps.proposer.ListPreparers()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, addr := range addrs {
		fmt.Fprintln(w, addr)
	}
}

func (ps *ProposerServer) handleListAccepters(w http.ResponseWriter, r *http.Request) {
	addrs, err := ps.proposer.ListAccepters()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, addr := range addrs {
		fmt.Fprintln(w, addr)
	}
}

//
//
//

// ProposerClient implements the extension.Proposer interface by making calls to
// a remote ProposerServer.
type ProposerClient struct {
	// HTTPClient to make requests. Optional.
	// If nil, http.DefaultClient is used.
	Client HTTPClient

	// URL of the remote ProposerServer.
	// Only scheme and host are used.
	URL *url.URL
}

var _ extension.Proposer = (*ProposerClient)(nil)

// Address implements extension.Proposer.
func (pc ProposerClient) Address() string {
	return pc.URL.String()
}

// Read implements extension.Proposer.
func (pc ProposerClient) Read(ctx context.Context, key string) (version uint64, value []byte, err error) {
	u := *pc.URL
	u.Path = fmt.Sprintf("/read/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), nil)
	req = req.WithContext(ctx)
	resp, err := pc.httpClient().Do(req)
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

// CAS implements extension.Proposer.
func (pc ProposerClient) CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error) {
	u := *pc.URL
	u.Path = fmt.Sprintf("/cas/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), bytes.NewReader(nextValue))
	setVersion(req.Header, currentVersion)
	req = req.WithContext(ctx)
	resp, err := pc.httpClient().Do(req)
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

// IdentityRead implements extension.Proposer.
func (pc ProposerClient) IdentityRead(ctx context.Context, key string) error {
	_, _, err := pc.readVia(ctx, fmt.Sprintf("/identity-read/%s", url.PathEscape(key)))
	return err
}

// AddAccepter implements extension.Proposer.
func (pc ProposerClient) AddAccepter(target protocol.Acceptor) error {
	return pc.postString("/add-accepter", target.Address())
}

// AddPreparer implements extension.Proposer.
func (pc ProposerClient) AddPreparer(target protocol.Acceptor) error {
	return pc.postString("/add-preparer", target.Address())
}

// RemovePreparer implements extension.Proposer.
func (pc ProposerClient) RemovePreparer(target protocol.Acceptor) error {
	return pc.postString("/remove-preparer", target.Address())
}

// RemoveAccepter implements extension.Proposer.
func (pc ProposerClient) RemoveAccepter(target protocol.Acceptor) error {
	return pc.postString("/remove-accepter", target.Address())
}

// FullIdentityRead implements extension.Proposer.
func (pc ProposerClient) FullIdentityRead(ctx context.Context, key string) (state []byte, b protocol.Ballot, err error) {
	return pc.readVia(ctx, fmt.Sprintf("/full-identity-read/%s", url.PathEscape(key)))
}

// FastForwardIncrement implements extension.Proposer.
func (pc ProposerClient) FastForwardIncrement(ctx context.Context, key string, tombstone protocol.Ballot) (protocol.Age, error) {
	u := *pc.URL
	u.Path = fmt.Sprintf("/fast-forward-increment/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), nil)
	setBallot(req.Header, tombstone)
	resp, err := pc.httpClient().Do(req)
	if err != nil {
		return protocol.Age{}, err
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return protocol.Age{}, errors.New(strings.TrimSpace(string(buf)))
	}
	return getAge(resp.Header), nil
}

// ListPreparers implements extension.Proposer.
func (pc ProposerClient) ListPreparers() ([]string, error) {
	u := *pc.URL
	u.Path = "/list-preparers"
	req, _ := http.NewRequest("GET", u.String(), nil)
	resp, err := pc.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.New(strings.TrimSpace(string(buf)))
	}
	var (
		addrs []string
		s     = bufio.NewScanner(resp.Body)
	)
	for s.Scan() {
		addrs = append(addrs, s.Text())
	}
	return addrs, nil
}

// ListAccepters implements extension.Proposer.
func (pc ProposerClient) ListAccepters() ([]string, error) {
	u := *pc.URL
	u.Path = "/list-accepters"
	req, _ := http.NewRequest("GET", u.String(), nil)
	resp, err := pc.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.New(strings.TrimSpace(string(buf)))
	}
	var (
		addrs []string
		s     = bufio.NewScanner(resp.Body)
	)
	for s.Scan() {
		addrs = append(addrs, s.Text())
	}
	return addrs, nil
}

func (pc ProposerClient) readVia(ctx context.Context, fullPath string) (state []byte, b protocol.Ballot, err error) {
	u := *pc.URL
	u.Path = fullPath
	req, _ := http.NewRequest("POST", u.String(), nil)
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	resp, err := pc.httpClient().Do(req)
	if err != nil {
		return state, b, err
	}
	b = getBallot(resp.Header)
	buf, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return state, b, errors.New(strings.TrimSpace(string(buf)))
	}
	state = buf
	return state, b, nil
}

func (pc ProposerClient) postString(path, body string) error {
	u := *pc.URL
	u.Path = path
	req, _ := http.NewRequest("POST", u.String(), strings.NewReader(body))
	resp, err := pc.httpClient().Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(buf)))
	}
	return nil
}

func (pc ProposerClient) httpClient() HTTPClient {
	client := pc.Client
	if client == nil {
		client = http.DefaultClient
	}
	return client
}
