package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"github.com/peterbourgon/caspaxos/extension"
	"github.com/peterbourgon/caspaxos/protocol"
)

// OperatorServer provides an HTTP interface to an operator node.
type OperatorServer struct {
	operatorNode extension.Operator
	*mux.Router
}

var _ http.Handler = (*OperatorServer)(nil)

// NewOperatorServer returns a usable OperatorServer wrapping the passed
// operator node.
func NewOperatorServer(on extension.Operator) *OperatorServer {
	ons := &OperatorServer{
		operatorNode: on,
	}
	r := mux.NewRouter()
	{
		r.StrictSlash(true)
		r.Methods("GET").Path("/cluster-state").HandlerFunc(ons.handleClusterState)
		// TODO(pb): implement all methods
	}
	ons.Router = r
	return ons
}

func (ons *OperatorServer) handleClusterState(w http.ResponseWriter, r *http.Request) {
	s, err := ons.operatorNode.ClusterState(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(s)
}

//
//
//

// OperatorClient implements the extension.Operator interface by making
// calls to a remote OperatorServer.
type OperatorClient struct {
	// HTTPClient to make requests. Optional.
	// If nil, http.DefaultClient is used.
	Client interface {
		Do(*http.Request) (*http.Response, error)
	}

	// URL of the remote AcceptorServer.
	// Only scheme and host are used.
	URL *url.URL
}

var _ extension.Operator = (*OperatorClient)(nil)

// Address implements Operator.
func (oc OperatorClient) Address() string {
	return oc.URL.String()
}

// Read implements Operator.
func (oc OperatorClient) Read(ctx context.Context, key string) (version uint64, value []byte, err error) {
	return version, value, errors.New("OperatorClient Read not yet implemented")
}

// CAS implements Operator.
func (oc OperatorClient) CAS(ctx context.Context, key string, currentVersion uint64, nextValue []byte) (version uint64, value []byte, err error) {
	return version, value, errors.New("OperatorClient CAS not yet implemented")
}

// Watch implements Operator.
func (oc OperatorClient) Watch(ctx context.Context, key string, values chan<- []byte) error {
	return errors.New("OperatorClient Watch not yet implemented")
}

// ClusterState implements Operator.
func (oc OperatorClient) ClusterState(ctx context.Context) (s extension.ClusterState, err error) {
	return s, errors.New("OperatorClient ClusterState not yet implemented")
}

// ListAcceptors implements Operator.
func (oc OperatorClient) ListAcceptors(ctx context.Context) ([]string, error) {
	return nil, errors.New("OperatorClient ListAcceptors not yet implemented")
}

// AddAcceptor implements Operator.
func (oc OperatorClient) AddAcceptor(ctx context.Context, target protocol.Acceptor) error {
	return errors.New("OperatorClient AddAcceptor not yet implemented")
}

// RemoveAcceptor implements Operator.
func (oc OperatorClient) RemoveAcceptor(ctx context.Context, target protocol.Acceptor) error {
	return errors.New("OperatorClient RemoveAcceptor not yet implemented")
}

// ListProposers implements Operator.
func (oc OperatorClient) ListProposers(ctx context.Context) ([]string, error) {
	return nil, errors.New("OperatorClient ListProposers not yet implemented")
}

// AddProposer implements Operator.
func (oc OperatorClient) AddProposer(ctx context.Context, target protocol.Proposer) error {
	return errors.New("OperatorClient AddProposer not yet implemented")
}

// RemoveProposer implements Operator.
func (oc OperatorClient) RemoveProposer(ctx context.Context, target protocol.Proposer) error {
	return errors.New("OperatorClient RemoveProposer not yet implemented")
}

func (oc OperatorClient) httpClient() interface {
	Do(*http.Request) (*http.Response, error)
} {
	client := oc.Client
	if client == nil {
		client = http.DefaultClient
	}
	return client
}
