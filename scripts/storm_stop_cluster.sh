#!/usr/bin/env bash

source $(dirname "$0")/config_helper.sh

for node in `cat $CONF_DIR/slaves | grep -v "^\s*[#;]"`; do
  ssh $node "echo "$node stopping Storm supervisor..."; $SCRIPTS_DIR/kill_by_template.sh daemon.name=supervisor" &
done

$SCRIPTS_DIR/kill_by_template.sh daemon.name=nimbus
$SCRIPTS_DIR/kill_by_template.sh daemon.name=ui
$SCRIPTS_DIR/kill_by_template.sh daemon.name=logviewer
 
