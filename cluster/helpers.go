package cluster

import (
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// HostPorts are deduced via CalculateHostPorts.
type HostPorts struct {
	BindHost      string
	BindPort      int
	AdvertiseHost string
	AdvertisePort int
}

// CalculateHostPorts will take a user-specified cluster bind address (required)
// and explicit advertise address (optional), along with a default port for
// cluster communications (required) and a set of cluster peer host:ports
// (optional), and deduce the best concrete hosts and ports to bind to and
// advertise within the cluster, suitable for passing to memberlist. We also
// figures out some notices and warnings along the way, and log them to the
// passed logger.
func CalculateHostPorts(bindAddr, advertiseAddr string, defaultClusterPort int, clusterPeers []string, logger log.Logger) (res HostPorts, err error) {
	_, _, res.BindHost, res.BindPort, err = ParseAddr(bindAddr, defaultClusterPort)
	if err != nil {
		return res, errors.Wrapf(err, "couldn't parse bind address %s", bindAddr)
	}

	if advertiseAddr != "" {
		_, _, res.AdvertiseHost, res.AdvertisePort, err = ParseAddr(advertiseAddr, defaultClusterPort)
		if err != nil {
			return res, errors.Wrapf(err, "couldn't parse advertise address %s", advertiseAddr)
		}
	}

	advertiseIP, err := CalculateAdvertiseIP(res.BindHost, res.AdvertiseHost, net.DefaultResolver, logger)
	if err != nil {
		return res, errors.Wrap(err, "couldn't deduce an advertise IP")
	}

	if hasNonlocal(clusterPeers) && isUnroutable(advertiseIP.String()) {
		level.Warn(logger).Log("err", "this node advertises itself on an unroutable IP", "ip", advertiseIP.String())
		level.Warn(logger).Log("err", "this node will be unreachable in the cluster")
		level.Warn(logger).Log("err", "provide an explicit advertise address as a routable IP address or hostname")
	}

	level.Info(logger).Log(
		"user_bind_host", res.BindHost,
		"user_advertise_host", res.AdvertiseHost,
		"calculated_advertise_ip", advertiseIP,
	)

	res.AdvertiseHost = advertiseIP.String()
	if res.AdvertisePort == 0 {
		res.AdvertisePort = res.BindPort
	}
	return res, nil
}

// ParseAddr liberally accepts a wide variety of addr formats, along with a
// default port, and returns a well-defined network, address, host, and port.
//
//     "udp://host:1234", 80 => udp, host:1234, host, 1234
//     "host:1234", 80       => tcp, host:1234, host, 1234
//     "host", 80            => tcp, host:80,   host, 80
//
func ParseAddr(addr string, defaultPort int) (network, address, host string, port int, err error) {
	u, err := url.Parse(strings.ToLower(addr))
	if err != nil {
		return network, address, host, port, err
	}

	switch {
	case u.Scheme == "" && u.Opaque == "" && u.Host == "" && u.Path != "": // "host"
		u.Scheme, u.Opaque, u.Host, u.Path = "tcp", "", net.JoinHostPort(u.Path, strconv.Itoa(defaultPort)), ""
	case u.Scheme != "" && u.Opaque != "" && u.Host == "" && u.Path == "": // "host:1234"
		u.Scheme, u.Opaque, u.Host, u.Path = "tcp", "", net.JoinHostPort(u.Scheme, u.Opaque), ""
	case u.Scheme != "" && u.Opaque == "" && u.Host != "" && u.Path == "": // "tcp://host[:1234]"
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			u.Host = net.JoinHostPort(u.Host, strconv.Itoa(defaultPort))
		}
	default:
		return network, address, host, port, errors.Errorf("%s: unsupported address format", addr)
	}

	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		return network, address, host, port, err
	}
	port, err = strconv.Atoi(portStr)
	if err != nil {
		return network, address, host, port, err
	}

	return u.Scheme, u.Host, host, port, nil
}

func hasNonlocal(clusterPeers []string) bool {
	for _, peer := range clusterPeers {
		if host, _, err := net.SplitHostPort(peer); err == nil {
			peer = host
		}
		if ip := net.ParseIP(peer); ip != nil && !ip.IsLoopback() {
			return true
		} else if ip == nil && strings.ToLower(peer) != "localhost" {
			return true
		}
	}
	return false
}

func isUnroutable(addr string) bool {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	if ip := net.ParseIP(addr); ip != nil && (ip.IsUnspecified() || ip.IsLoopback()) {
		return true // typically 0.0.0.0 or localhost
	} else if ip == nil && strings.ToLower(addr) == "localhost" {
		return true
	}
	return false
}
