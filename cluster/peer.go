package cluster

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/pborman/uuid"
)

// Config for our node in the cluster.
// All fields are mandatory unless otherwise noted.
type Config struct {
	ClusterHost   string
	ClusterPort   int
	AdvertiseHost string // optional; by default, ClusterHost is used
	AdvertisePort int    // optional; by default, ClusterPort is used
	APIHost       string
	APIPort       int
	Type          string
	InitialPeers  []string
	Logger        log.Logger
}

func (c *Config) normalize() {
	if c.Logger == nil {
		c.Logger = log.NewNopLogger()
	}
}

// Peer models a node in the cluster.
type Peer struct {
	ml *memberlist.Memberlist
	d  *delegate
}

// NewPeer joins the cluster. Don't forget to leave it, at some point.
func NewPeer(config Config) (*Peer, error) {
	var (
		d = newDelegate(config.Logger)
		c = memberlist.DefaultLANConfig()
	)
	{
		c.Name = uuid.New()
		c.BindAddr = config.ClusterHost
		c.BindPort = config.ClusterPort
		c.AdvertiseAddr = config.AdvertiseHost
		c.AdvertisePort = config.AdvertisePort
		c.LogOutput = ioutil.Discard
		c.Delegate = d
		c.Events = d
	}

	ml, err := memberlist.Create(c)
	if err != nil {
		return nil, err
	}

	d.initialize(c.Name, config.Type, config.APIHost, config.APIPort, ml.NumMembers)
	n, _ := ml.Join(config.InitialPeers)
	level.Debug(config.Logger).Log("Join", n)

	return &Peer{
		ml: ml,
		d:  d,
	}, nil
}

// Leave the cluster, waiting up to timeout for a confirmation.
func (p *Peer) Leave(timeout time.Duration) error {
	return p.ml.Leave(timeout)
}

// Query for other peers in the cluster whose type passes the function f.
// Results are returned as matching API host:port pairs.
func (p *Peer) Query(f func(peerType string) bool) (apiHostPorts []string) {
	for _, v := range p.d.query(f) {
		apiHostPort := net.JoinHostPort(v.APIHost, fmt.Sprint(v.APIPort))
		apiHostPorts = append(apiHostPorts, apiHostPort)
	}
	return apiHostPorts
}

//
//
//

type delegate struct {
	mtx    sync.RWMutex
	bcast  *memberlist.TransmitLimitedQueue
	data   map[string]peerInfo
	logger log.Logger
}

type peerInfo struct {
	Type    string `json:"type"`
	APIHost string `json:"api_host"`
	APIPort int    `json:"api_port"`
}

func newDelegate(logger log.Logger) *delegate {
	return &delegate{
		data:   map[string]peerInfo{},
		logger: logger,
	}
}

func (d *delegate) initialize(id string, peerType string, apiHost string, apiPort int, numNodes func() int) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	d.bcast = &memberlist.TransmitLimitedQueue{
		NumNodes:       numNodes,
		RetransmitMult: 3,
	}
	d.data[id] = peerInfo{peerType, apiHost, apiPort}
}

func (d *delegate) query(f func(peerType string) bool) map[string]peerInfo {
	res := map[string]peerInfo{}
	for k, info := range d.state() {
		if f(info.Type) {
			res[k] = info
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
	buf, _ := json.Marshal(d.data)
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
}

// NotifyUpdate is invoked when a node is detected to have updated, usually
// involving the meta data. The Node argument must not be modified.
// Implements memberlist.EventDelegate.
func (d *delegate) NotifyUpdate(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyUpdate", "node", n.Name, "addr", fmt.Sprintf("%s:%d", n.Addr, n.Port))
}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
// Implements memberlist.EventDelegate.
func (d *delegate) NotifyLeave(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyLeave", "node", n.Name, "addr", fmt.Sprintf("%s:%d", n.Addr, n.Port))
	d.mtx.Lock()
	defer d.mtx.Unlock()
	delete(d.data, n.Name)
}
