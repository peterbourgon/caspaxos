package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/internal/eventsource"
)

// UserNodeServer provides an HTTP interface to an user node.
type UserNodeServer struct {
	userNode extension.UserNode
	*mux.Router
}

var _ http.Handler = (*UserNodeServer)(nil)

// NewUserNodeServer returns a usable UserNodeServer wrapping the passed
// user node.
func NewUserNodeServer(un extension.UserNode) *UserNodeServer {
	uns := &UserNodeServer{
		userNode: un,
	}
	r := mux.NewRouter()
	{
		r.StrictSlash(true)
		r.Methods("POST").Path("/read/{key}").HandlerFunc(uns.handleRead)
		r.Methods("POST").Path("/cas/{key}").HandlerFunc(uns.handleCAS)
		r.Methods("POST").Path("/watch/{key}").HandlerFunc(uns.handleWatch)
	}
	uns.Router = r
	return uns
}

func (uns *UserNodeServer) handleRead(w http.ResponseWriter, r *http.Request) {
	key, _ := url.PathUnescape(mux.Vars(r)["key"])
	version, value, err := uns.userNode.Read(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setVersion(w.Header(), version)
	w.Write(value)
}

func (uns *UserNodeServer) handleCAS(w http.ResponseWriter, r *http.Request) {
	var (
		key, _         = url.PathUnescape(mux.Vars(r)["key"])
		currentVersion = getVersion(r.Header)
		nextValue, _   = ioutil.ReadAll(r.Body)
	)

	version, value, err := uns.userNode.CAS(r.Context(), key, currentVersion, nextValue)
	if _, ok := err.(extension.CASError); ok {
		http.Error(w, err.Error(), http.StatusPreconditionFailed) // CASError -> 412
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setVersion(w.Header(), version)
	w.Write(value)
}

// Watch(ctx context.Context, key string, values chan<- []byte) error
func (uns *UserNodeServer) handleWatch(w http.ResponseWriter, r *http.Request) {
	var (
		key, _      = url.PathUnescape(mux.Vars(r)["key"])
		values      = make(chan []byte)
		errs        = make(chan error)
		enc         = eventsource.NewEncoder(w)
		ctx, cancel = context.WithCancel(r.Context())
	)

	go func() {
		errs <- uns.userNode.Watch(ctx, key, values)
	}()

	// via eventsource.Handler.ServeHTTP
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Vary", "Accept")
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)

	for {
		select {
		case value := <-values:
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

//
//
//

// UserNodeClient implements the extension.UserNode interface by making
// calls to a remote UserNodeServer.
type UserNodeClient struct {
	// HTTPClient to make requests. Optional.
	// If nil, http.DefaultClient is used.
	Client interface {
		Do(*http.Request) (*http.Response, error)
	}

	// URL of the remote AcceptorServer.
	// Only scheme and host are used.
	URL *url.URL

	// WatchRetry is the time between reconnect attempts
	// if a Watch session goes bad. Default of 1 second.
	WatchRetry time.Duration
}

var _ extension.UserNode = (*UserNodeClient)(nil)

// Address implements UserNode.
func (unc UserNodeClient) Address() string {
	return unc.URL.String()
}

// Read implements UserNode.
func (unc UserNodeClient) Read(ctx context.Context, key string) (version uint64, value []byte, err error) {
	u := *unc.URL
	u.Path = fmt.Sprintf("/read/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), nil)
	req = req.WithContext(ctx)
	resp, err := unc.httpClient().Do(req)
	if err != nil {
		return 0, nil, err
	}

	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return 0, nil, errors.New(strings.TrimSpace(string(buf)))
	}

	version = getVersion(resp.Header)
	value, _ = ioutil.ReadAll(resp.Body)
	return version, value, nil
}

// CAS implements UserNode.
func (unc UserNodeClient) CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error) {
	u := *unc.URL
	u.Path = fmt.Sprintf("/cas/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), bytes.NewReader(nextValue))
	setVersion(req.Header, currentVersion)
	req = req.WithContext(ctx)
	resp, err := unc.httpClient().Do(req)
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
	value, _ = ioutil.ReadAll(resp.Body)
	return version, value, nil

}

// Watch implements UserNode.
func (unc UserNodeClient) Watch(ctx context.Context, key string, values chan<- []byte) error {
	u := *unc.URL
	u.Path = fmt.Sprintf("/watch/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), nil)
	req = req.WithContext(ctx)

	retry := unc.WatchRetry
	if retry <= 0 {
		retry = time.Second
	}

	s := eventsource.New(req, retry) // TODO(pb): this uses DefaultClient
	defer s.Close()

	for {
		ev, err := s.Read()
		if err != nil {
			return errors.Wrap(err, "remote user node EventSource error")
		}
		values <- ev.Data
	}
}

func (unc UserNodeClient) httpClient() interface {
	Do(*http.Request) (*http.Response, error)
} {
	client := unc.Client
	if client == nil {
		client = http.DefaultClient
	}
	return client
}
