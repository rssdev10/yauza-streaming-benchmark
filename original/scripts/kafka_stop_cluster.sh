#!/usr/bin/env bash

source $(dirname "$0")/config_helper.sh

for node in $(get_slaves); do
  ssh $node "echo "$node stopping Kafka..."; $SCRIPTS_DIR/kill_by_template.sh kafka.Kafka; rm -rf /tmp/yauza/kafka" &
done

