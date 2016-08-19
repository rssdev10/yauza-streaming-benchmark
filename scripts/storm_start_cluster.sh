#!/usr/bin/env bash

source $(dirname "$0")/config_helper.sh

STORM_PATH=$1

if [ -z "$STORM_PATH" ]; then
  echo "Please specify path of Storm"
  exit 0
fi


STORM_PATH=`readlink -f $STORM_PATH`
echo "$STORM_PATH"
${STORM_PATH}/bin/storm nimbus &
${STORM_PATH}/bin/storm ui &
${STORM_PATH}/bin/storm logviewer &

sleep 10

for node in $(get_slaves); do
  cmd="${STORM_PATH}/bin/storm supervisor"
  ssh $node "echo "$node: starting Storm supervisor..."; nohup $cmd > /dev/null 2>&1 &" &
done

