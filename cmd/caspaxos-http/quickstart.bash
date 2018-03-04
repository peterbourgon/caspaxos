#!/usr/bin/env bash

trap 'kill 0' SIGTERM

for i in `seq 1 3`
do
  sleep 0.25
  caspaxos-http acceptor \
    -debug                            \
    -api     tcp://127.0.0.1:100${i}0 \
    -cluster tcp://127.0.0.1:100${i}1 \
    $PEERS 2>&1 | sed -e "s/^/[A$i] /" &
  PEERS="$PEERS -peer 127.0.0.1:100${i}1"
done

for i in `seq 1 3`
do
  sleep 0.25
  caspaxos-http proposer \
    -debug                            \
    -api     tcp://127.0.0.1:200${i}0 \
    -cluster tcp://127.0.0.1:200${i}1 \
    $PEERS 2>&1 | sed -e "s/^/[P$i] /" &
  PEERS="$PEERS -peer 127.0.0.1:200${i}1"
done

wait

