package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/pborman/uuid"
)

// PeerConfig describes how to start up a peer in the cluster.
type PeerConfig struct {
	// The type of peer that we claim to be. An arbitrary string that can be
	// queried-for.
	//
	// Required.
	PeerType string

	// The address we bind the listener for cluster communications. Hostname or
	// IP address, not including port.
	//
	// Required.
	BindAddr string

	// The port we bind the listener for cluster communications.
	//
	// Required.
	BindPort int

	// The address we advertise into the cluster, which we claim we can be
	// reached on for cluster communications. Hostname or IP address, not
	// including port.
	//
	// Optional. If not provided, we'll deduce something based on the bind
	// address.
	AdvertiseAddr string

	// The port we advertise into the cluster, which we claim we can be reached
	// on for cluster communications.
	//
	// Optional. If not provided, we'll take the bind port.
	AdvertisePort int

	// It's assumed that all cluster peers run some kind of API which other
	// peers want to talk to. This is the address that we claim that API can be
	// reached on, as a hostname or IP address. It's combined with the APIPort
	// and returned by queries.
	//
	// Optional. If not provided, we'll take the cluster advertise address.
	APIAddr string

	// It's assumed that all cluster peers run some kind of API which other
	// peers want to talk to. This is the port that we claim that API can be
	// reached on. It's combined with the APIAddr and returned by queries.
	//
	// Required.
	APIPort int

	// A set of host:port strings of other peers in the cluster. We'll use this
	// to seed our initial discovery.
	//
	// Technically optional, but if you don't provide at least one initial peer,
	// you'll be forever alone.
	InitialPeers []string

	// Callback is invoked whenever the cluster membership changes, with the
	// peers that have just joined, have just left, and currently comprise the
	// cluster. Peers are grouped by peer type, and identified by their API host
	// and port.
	//
	// Optional.
	Callback func(joins, leaves, current map[string][]string)

	// Cluster state changes and other diagnostic information is sent via this
	// logger, including notification of several conditions that will prevent
	// the cluster from working as expected.
	//
	// Optional, but recommended.
	Logger log.Logger
}

// Peer represents this node in the cluster. It's essentially an opinionated and
// simplified wrapper around memberlist.
type Peer struct {
	ml *memberlist.Memberlist
	d  *delegate
}

// NewPeer constructs a peer in a cluster. The returned peer is already running;
// callers should be sure to call Leave, at some point.
func NewPeer(config PeerConfig) (*Peer, error) {
	// Validate config.
	if config.PeerType == "" {
		return nil, errors.New("must provide PeerType")
	}
	if config.BindAddr == "" {
		return nil, errors.New("must provide BindAddr")
	}
	if config.BindPort == 0 {
		return nil, errors.New("must provide BindPort")
	}
	if config.APIPort == 0 {
		return nil, errors.New("must provide APIPort")
	}
	if config.Logger == nil {
		config.Logger = log.NewNopLogger()
	}

	// Create a memberlist delegate.
	d := newDelegate(config.Callback, config.Logger)
	mlconfig := memberlist.DefaultLANConfig()
	{
		mlconfig.Name = uuid.New()
		mlconfig.BindAddr = config.BindAddr
		mlconfig.BindPort = config.BindPort
		if config.AdvertiseAddr != "" {
			level.Debug(config.Logger).Log("advertise_addr", config.AdvertiseAddr, "advertise_port", config.AdvertisePort)
			mlconfig.AdvertiseAddr = config.AdvertiseAddr
			mlconfig.AdvertisePort = config.AdvertisePort
		}
		mlconfig.LogOutput = ioutil.Discard
		mlconfig.Delegate = d
		mlconfig.Events = d
	}
	ml, err := memberlist.Create(mlconfig)
	if err != nil {
		return nil, err
	}

	// Bootstrap the delegate.
	d.init(mlconfig.Name, config.PeerType, ml.LocalNode().Addr.String(), config.APIPort, ml.NumMembers)
	n, _ := ml.Join(config.InitialPeers)
	level.Debug(config.Logger).Log("Join", n)
	if len(config.InitialPeers) > 0 {
		go warnIfAlone(ml, config.Logger, 5*time.Second)
	}

	// Return the usable peer.
	return &Peer{
		ml: ml,
		d:  d,
	}, nil
}

func warnIfAlone(ml *memberlist.Memberlist, logger log.Logger, d time.Duration) {
	for range time.Tick(d) {
		if n := ml.NumMembers(); n <= 1 {
			level.Warn(logger).Log("NumMembers", n, "msg", "I appear to be alone in the cluster")
		}
	}
}

// Leave the cluster, waiting up to timeout.
func (p *Peer) Leave(timeout time.Duration) error {
	return p.ml.Leave(timeout)
}

// Query for API host:port strings. Match peer types passing the given function.
func (p *Peer) Query(f func(peerType string) bool) []string {
	return p.d.query(f)
}

// Name (unique ID) of this peer in the cluster.
func (p *Peer) Name() string {
	return p.ml.LocalNode().Name
}

// ClusterSize returns the total size of the cluster, from this peer's perspective.
func (p *Peer) ClusterSize() int {
	return p.ml.NumMembers()
}

// State returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) State() map[string]interface{} {
	return map[string]interface{}{
		"self":     p.ml.LocalNode(),
		"members":  p.ml.Members(),
		"n":        p.ml.NumMembers(),
		"delegate": p.d.state(),
	}
}

// delegate manages gossiped data: the set of peers, their type, and API port.
// Clients must invoke init before the delegate can be used.
// Inspired by https://github.com/asim/memberlist/blob/master/memberlist.go
type delegate struct {
	mtx      sync.RWMutex
	bcast    *memberlist.TransmitLimitedQueue
	data     map[string]peerInfo
	callback callbackFunc
	logger   log.Logger
}

type peerInfo struct {
	Type    string `json:"type"`
	APIAddr string `json:"api_addr"`
	APIPort int    `json:"api_port"`
}

type callbackFunc func(joins, leaves, current map[string][]string)

func newDelegate(callback callbackFunc, logger log.Logger) *delegate {
	return &delegate{
		bcast:    nil,
		data:     map[string]peerInfo{},
		callback: callback,
		logger:   logger,
	}
}

func (d *delegate) init(myName string, peerType string, apiAddr string, apiPort int, numNodes func() int) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	// As far as I can tell, it is only luck which ensures the d.bcast isn't
	// used (via GetBroadcasts) before we have a chance to create it here. But I
	// don't see a way to wire up the components (in NewPeer) that doesn't
	// involve this roundabout sort of initialization. Shrug!
	d.bcast = &memberlist.TransmitLimitedQueue{
		NumNodes:       numNodes,
		RetransmitMult: 3,
	}
	d.data[myName] = peerInfo{peerType, apiAddr, apiPort}
}

func (d *delegate) query(pass func(string) bool) (res []string) {
	for _, info := range d.state() {
		if pass(info.Type) {
			res = append(res, net.JoinHostPort(info.APIAddr, strconv.Itoa(info.APIPort)))
		}
	}
	return res
}

func (d *delegate) state() map[string]peerInfo {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	res := map[string]peerInfo{}
	for k, v := range d.data {
		res[k] = v
	}
	return res
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
// Implements memberlist.Delegate.
func (d *delegate) NodeMeta(limit int) []byte {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	return []byte{} // no metadata
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed.
// Implements memberlist.Delegate.
func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}
	var data map[string]peerInfo
	if err := json.Unmarshal(b, &data); err != nil {
		level.Error(d.logger).Log("method", "NotifyMsg", "b", strings.TrimSpace(string(b)), "err", err)
		return
	}
	d.mtx.Lock()
	defer d.mtx.Unlock()
	for k, v := range data {
		// Removing data is handled by NotifyLeave
		d.data[k] = v
	}
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
// Implements memberlist.Delegate.
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	if d.bcast == nil {
		panic("GetBroadcast before init")
	}
	return d.bcast.GetBroadcasts(overhead, limit)
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
// Implements memberlist.Delegate.
func (d *delegate) LocalState(join bool) []byte {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	buf, err := json.Marshal(d.data)
	if err != nil {
		panic(err)
	}
	return buf
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
// Implements memberlist.Delegate.
func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		level.Debug(d.logger).Log("method", "MergeRemoteState", "join", join, "buf_sz", 0)
		return
	}
	var data map[string]peerInfo
	if err := json.Unmarshal(buf, &data); err != nil {
		level.Error(d.logger).Log("method", "MergeRemoteState", "err", err)
		return
	}
	d.mtx.Lock()
	defer d.mtx.Unlock()
	for k, v := range data {
		d.data[k] = v
	}
}

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
// Implements memberlist.EventDelegate.
func (d *delegate) NotifyJoin(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyJoin", "node", n.Name, "addr", fmt.Sprintf("%s:%d", n.Addr, n.Port))
	if d.callback != nil {
		d.mtx.Lock()
		defer d.mtx.Unlock()
		var (
			info     = d.data[n.Name]
			hostport = net.JoinHostPort(info.APIAddr, strconv.Itoa(info.APIPort))
			joins    = map[string][]string{info.Type: []string{hostport}}
			leaves   = map[string][]string{}
			current  = byType(d.data)
		)
		d.callback(joins, leaves, current)
	}
}

// NotifyUpdate is invoked when a node is detected to have updated, usually
// involving the meta data. The Node argument must not be modified.
// Implements memberlist.EventDelegate.
func (d *delegate) NotifyUpdate(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyUpdate", "node", n.Name, "addr", fmt.Sprintf("%s:%d", n.Addr, n.Port))
	if d.callback != nil {
		d.mtx.Lock()
		defer d.mtx.Unlock()
		var (
			joins   = map[string][]string{}
			leaves  = map[string][]string{}
			current = byType(d.data)
		)
		d.callback(joins, leaves, current)
	}
}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
// Implements memberlist.EventDelegate.
func (d *delegate) NotifyLeave(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyLeave", "node", n.Name, "addr", fmt.Sprintf("%s:%d", n.Addr, n.Port))
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.callback != nil {
		var (
			info     = d.data[n.Name]
			hostport = net.JoinHostPort(info.APIAddr, strconv.Itoa(info.APIPort))
			joins    = map[string][]string{}
			leaves   = map[string][]string{info.Type: []string{hostport}}
			current  = byType(d.data)
		)
		d.callback(joins, leaves, current)
	}
	delete(d.data, n.Name)
}

func byType(data map[string]peerInfo) map[string][]string {
	current := map[string][]string{}
	for _, info := range data {
		hostport := net.JoinHostPort(info.APIAddr, strconv.Itoa(info.APIPort))
		current[info.Type] = append(current[info.Type], hostport)
	}
	return current
}
