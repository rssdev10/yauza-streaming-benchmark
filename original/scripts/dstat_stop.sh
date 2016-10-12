#!/usr/bin/env bash

source $(dirname "$0")/config_helper.sh

for node in $(get_slaves_and_masters); do
  ssh $node "hostname; kill `cat $OUT_DIR/dstat_$node.pid`; rm $OUT_DIR/dstat_$node.pid";
done
