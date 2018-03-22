package httpapi

import (
	"context"
	"encoding/json"
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

// OperatorNodeServer provides an HTTP interface to an operator node.
type OperatorNodeServer struct {
	operatorNode extension.OperatorNode
	*mux.Router
}

var _ http.Handler = (*OperatorNodeServer)(nil)

// NewOperatorNodeServer returns a usable OperatorNodeServer wrapping the passed
// operator node.
func NewOperatorNodeServer(on extension.OperatorNode) *OperatorNodeServer {
	ons := &OperatorNodeServer{
		operatorNode: on,
	}
	r := mux.NewRouter()
	{
		r.StrictSlash(true)
		r.Methods("GET").Path("/cluster-state").HandlerFunc(ons.handleClusterState)
		r.Methods("POST").Path("/grow-cluster").HandlerFunc(ons.handleGrowCluster)
		r.Methods("POST").Path("/shrink-cluster").HandlerFunc(ons.handleShrinkCluster)
		r.Methods("POST").Path("/garbage-collect/{key}").HandlerFunc(ons.handleGarbageCollect)
	}
	ons.Router = r
	return ons
}

func (ons *OperatorNodeServer) handleClusterState(w http.ResponseWriter, r *http.Request) {
	s, err := ons.operatorNode.ClusterState(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(s)
}

func (ons *OperatorNodeServer) handleGrowCluster(w http.ResponseWriter, r *http.Request) {
	ons.handleClusterChange(w, r, ons.operatorNode.GrowCluster)
}

func (ons *OperatorNodeServer) handleShrinkCluster(w http.ResponseWriter, r *http.Request) {
	ons.handleClusterChange(w, r, ons.operatorNode.ShrinkCluster)
}

func (ons *OperatorNodeServer) handleClusterChange(w http.ResponseWriter, r *http.Request, method func(context.Context, protocol.Acceptor) error) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := AcceptorClient{URL: u} // TODO(pb): HTTP client
	if err := method(r.Context(), target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "OK")
}

func (ons *OperatorNodeServer) handleGarbageCollect(w http.ResponseWriter, r *http.Request) {
	key, _ := url.PathUnescape(mux.Vars(r)["key"])
	if err := ons.operatorNode.GarbageCollect(r.Context(), key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	fmt.Fprintln(w, "OK")
}

//
//
//

// OperatorNodeClient implements the extension.OperatorNode interface by making
// calls to a remote OperatorNodeServer.
type OperatorNodeClient struct {
	// HTTPClient to make requests. Optional.
	// If nil, http.DefaultClient is used.
	Client interface {
		Do(*http.Request) (*http.Response, error)
	}

	// URL of the remote AcceptorServer.
	// Only scheme and host are used.
	URL *url.URL
}

var _ extension.OperatorNode = (*OperatorNodeClient)(nil)

// Address implements OperatorNode.
func (onc OperatorNodeClient) Address() string {
	return onc.URL.String()
}

// ClusterState implements OperatorNode.
func (onc OperatorNodeClient) ClusterState(ctx context.Context) (s extension.ClusterState, err error) {
	u := *onc.URL
	u.Path = "/cluster-state"
	req, _ := http.NewRequest("GET", u.String(), nil)
	req = req.WithContext(ctx)

	resp, err := onc.httpClient().Do(req)
	if err != nil {
		return s, err
	}

	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return s, errors.New(strings.TrimSpace(string(buf)))
	}

	err = json.NewDecoder(resp.Body).Decode(&s)
	return s, err
}

// GrowCluster implements OperatorNode.
func (onc OperatorNodeClient) GrowCluster(ctx context.Context, target protocol.Acceptor) error {
	return onc.configChange(ctx, target, "/grow-cluster")
}

// ShrinkCluster implements OperatorNode.
func (onc OperatorNodeClient) ShrinkCluster(ctx context.Context, target protocol.Acceptor) error {
	return onc.configChange(ctx, target, "/shrink-cluster")
}

func (onc OperatorNodeClient) configChange(ctx context.Context, target protocol.Acceptor, path string) error {
	u := *onc.URL
	u.Path = path
	req, _ := http.NewRequest("POST", u.String(), strings.NewReader(target.Address()))
	req = req.WithContext(ctx)

	resp, err := onc.httpClient().Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(buf)))
	}

	return nil
}

// GarbageCollect implements OperatorNode.
func (onc OperatorNodeClient) GarbageCollect(ctx context.Context, key string) error {
	u := *onc.URL
	u.Path = fmt.Sprintf("/garbage-collect/%s", url.PathEscape(key))
	req, _ := http.NewRequest("POST", u.String(), nil)
	req = req.WithContext(ctx)

	resp, err := onc.httpClient().Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(buf)))
	}

	return nil
}

func (onc OperatorNodeClient) httpClient() interface {
	Do(*http.Request) (*http.Response, error)
} {
	client := onc.Client
	if client == nil {
		client = http.DefaultClient
	}
	return client
}
