#!/usr/bin/env bash

source $(dirname "$0")/config_helper.sh

for node in `cat $CONF_DIR/slaves | grep -v "^\s*[#;]"`; do
  ssh $node "hostname; kill `cat $OUT_DIR/dstat_$node.pid`; rm $OUT_DIR/dstat_$node.pid";
done
