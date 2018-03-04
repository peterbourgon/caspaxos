package main

import (
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"net"
	"net/http"
	"net/url"
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

func runProposer(args []string) error {
	flagset := flag.NewFlagSet("proposer", flag.ExitOnError)
	var (
		debug                = flagset.Bool("debug", false, "log debug information")
		apiAddr              = flagset.String("api", defaultAPIAddr, "listen address for HTTP API")
		clusterBindAddr      = flagset.String("cluster", defaultClusterAddr, "listen address for cluster comms")
		clusterAdvertiseAddr = flagset.String("cluster.advertise-addr", "", "optional, explicit address to advertise in cluster")
		clusterPeers         = stringslice{}
	)
	flagset.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	flagset.Usage = usageFor(flagset, "caspaxos-http proposer [flags]")
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
			clusterPeers, cluster.NodeTypeProposer, apiPort,
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

	// Lol.
	{
		level.Info(logger).Log("msg", "waiting 1s for acceptors to appear")
		time.Sleep(1 * time.Second)
		level.Info(logger).Log("acceptors", fmt.Sprintf("%v", peer.Current(cluster.NodeTypeAcceptor)))
	}

	// Use the peer to get an initial set of acceptors.
	var initialAcceptors []caspaxos.Acceptor
	{
		for _, hostport := range peer.Current(cluster.NodeTypeAcceptor) {
			initialAcceptors = append(initialAcceptors, httpapi.AcceptorClient{
				URL: &url.URL{Scheme: "http", Host: hostport},
			})
		}
		level.Debug(logger).Log("initial_acceptors", len(initialAcceptors))
	}

	// Generate a unique ID for this proposer.
	var id uint64
	{
		h := crc32.NewIEEE()
		fmt.Fprint(h, chp.AdvertiseHost)
		fmt.Fprint(h, chp.AdvertisePort)
		id = uint64(h.Sum32())
		level.Debug(logger).Log("proposer_id", id)
	}

	// Construct the proposer.
	var proposer caspaxos.Proposer
	{
		proposer = caspaxos.NewLocalProposer(
			id,
			log.With(logger, "component", "proposer"),
			initialAcceptors...,
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
			Handler: httpapi.NewProposerServer(proposer, log.With(logger, "component", "api")),
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
