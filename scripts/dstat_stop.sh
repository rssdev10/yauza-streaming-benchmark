#!/usr/bin/sh

ROOT_DIR=`readlink -f $(dirname "$0")/..`
CONF_DIR=$ROOT_DIR/conf
OUT_DIR=${ROOT_DIR}/output

mkdir -p $OUT_DIR

for node in `cat $CONF_DIR/slaves | grep -v "^\s*[#;]"`; do
  ssh $node "hostname; kill `cat $OUT_DIR/dstat_$node.pid`; rm $OUT_DIR/dstat_$node.pid";
done
