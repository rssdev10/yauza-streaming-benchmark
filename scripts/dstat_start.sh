#!/usr/bin/sh

ROOT_DIR=`realpath $(dirname "$0")/..`
CONF_DIR=$ROOT_DIR/conf
OUT_DIR=${ROOT_DIR}/output

mkdir -p $OUT_DIR

for node in `cat $CONF_DIR/slaves | grep -v "^\s*[#;]"`; do
  cmd="dstat --epoch --cpu --mem --net --disk --noheaders --nocolor --output $OUT_DIR/dstat_$node.log"
  ssh $node "hostname; rm $OUT_DIR/dstat_$node.log; nohup $cmd > /dev/null 2>&1 & echo \$! > '$OUT_DIR/dstat_$node.pid'";
done
