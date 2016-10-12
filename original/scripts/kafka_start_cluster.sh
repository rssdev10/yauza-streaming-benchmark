#!/usr/bin/env bash

source $(dirname "$0")/config_helper.sh

KAFKA_PATH=$1

if [ -z "$KAFKA_PATH" ]; then
  echo "Please specify path of Kafka"
  exit 0
fi


KAFKA_PATH=`readlink -f $KAFKA_PATH`
echo "$KAFKA_PATH"

counter=1
for node in $(get_slaves_and_masters); do
  sed "s/%BROKER.ID%/${counter}/" ${KAFKA_PATH}/config/server.properties.template > ${KAFKA_PATH}/config/server.properties.$node
  cmd="${KAFKA_PATH}/bin/kafka-server-start.sh ${KAFKA_PATH}/config/server.properties.$node"
  ssh $node "echo "$node: starting Kafka..."; nohup $cmd > /dev/null 2>&1 &" &
  let counter=counter+1
done

sleep 10

