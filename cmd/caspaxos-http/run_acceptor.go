package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/peterbourgon/caspaxos"
	"github.com/peterbourgon/caspaxos/cluster"
	"github.com/peterbourgon/caspaxos/httpapi"
	"github.com/pkg/errors"
)

func runAcceptor(args []string) error {
	flagset := flag.NewFlagSet("acceptor", flag.ExitOnError)
	var (
		debug                = flagset.Bool("debug", false, "log debug information")
		apiAddr              = flagset.String("api", defaultAPIAddr, "listen address for HTTP API")
		clusterBindAddr      = flagset.String("cluster", defaultClusterAddr, "listen address for cluster comms")
		clusterAdvertiseAddr = flagset.String("cluster.advertise-addr", "", "optional, explicit address to advertise in cluster")
		clusterPeers         = stringslice{}
	)
	flagset.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	flagset.Usage = usageFor(flagset, "caspaxos-http acceptor [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	// Build a logger.
	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		lv := level.AllowInfo()
		if *debug {
			lv = level.AllowDebug()
		}
		logger = level.NewFilter(logger, lv)
	}

	// Parse API addresses.
	var apiNetwork string
	var apiHost string
	var apiPort int
	{
		var err error
		apiNetwork, _, apiHost, apiPort, err = cluster.ParseAddr(*apiAddr, defaultAPIPort)
		if err != nil {
			return err
		}
	}

	// Parse cluster comms addresses.
	var chp cluster.HostPorts
	{
		var err error
		chp, err = cluster.CalculateHostPorts(
			*clusterBindAddr, *clusterAdvertiseAddr,
			defaultClusterPort, clusterPeers, logger,
		)
		if err != nil {
			return errors.Wrap(err, "calculating cluster hosts and ports")
		}
	}

	// Construct a peer.
	var peer *cluster.Peer
	{
		var err error
		peer, err = cluster.NewPeer(
			chp.BindHost, chp.BindPort,
			chp.AdvertiseHost, chp.AdvertisePort,
			clusterPeers, cluster.NodeTypeAcceptor, apiPort,
			log.With(logger, "component", "cluster"),
		)
		if err != nil {
			return err
		}
		defer func() {
			if err := peer.Leave(time.Second); err != nil {
				level.Warn(logger).Log("op", "peer.Leave", "err", err)
			}
		}()
	}

	// Construct the acceptor.
	var acceptor caspaxos.Acceptor
	{
		acceptor = caspaxos.NewMemoryAcceptor(
			net.JoinHostPort(chp.AdvertiseHost, strconv.Itoa(chp.AdvertisePort)),
			log.With(logger, "component", "acceptor"),
		)
		// TODO(pb): wire up configuration changes
	}

	// Set up the API listener and server.
	var apiListener net.Listener
	{
		var err error
		apiListener, err = net.Listen(apiNetwork, net.JoinHostPort(apiHost, strconv.Itoa(apiPort)))
		if err != nil {
			return err
		}
		defer func() {
			if err := apiListener.Close(); err != nil {
				level.Warn(logger).Log("op", "apiListener.Close", "err", err)
			}
		}()
	}

	var g run.Group
	{
		// Serve the HTTP API.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		server := &http.Server{
			Handler: httpapi.NewAcceptorServer(acceptor, log.With(logger, "component", "api")),
		}
		level.Info(logger).Log("component", "api", "addr", apiListener.Addr().String())
		g.Add(func() error {
			return server.Serve(apiListener)
		}, func(error) {
			server.Shutdown(ctx)
		})
	}
	{
		// Listen for ctrl-C.
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
			select {
			case sig := <-c:
				return fmt.Errorf("received signal %s", sig)
			case <-ctx.Done():
				return ctx.Err()
			}
		}, func(error) {
			cancel()
		})
	}
	return g.Run()
}
