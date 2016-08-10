#!/usr/bin/env bash

source $(dirname "$0")/config_helper.sh

mkdir -p $OUT_DIR

for node in `cat $CONF_DIR/slaves | grep -v "^\s*[#;]"`; do
  cmd="${DSTAT_PATH}dstat --epoch --cpu --mem --net --disk --noheaders --nocolor --output $OUT_DIR/dstat_$node.log"
  ssh $node "hostname; rm -f $OUT_DIR/dstat_$node.log; nohup $cmd > /dev/null 2>&1 & echo \$! > '$OUT_DIR/dstat_$node.pid'";
done
