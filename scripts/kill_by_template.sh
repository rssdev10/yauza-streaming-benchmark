#!/usr/bin/env bash

pid_match() {
  local SN=`basename "$0"`
  local VAL=`ps -aef | grep "$1" | grep -vE "grep|$SN" | awk '{print \$2}' |  head -1`
  echo $VAL
}

MATCH="$1"
PID=`pid_match "$MATCH"`

if [[ "$PID" -ne "" ]];
then
  kill "$PID"
  sleep 1
  CHECK_AGAIN=`pid_match "$MATCH"`
  if [[ "$CHECK_AGAIN" -ne "" ]];
  then
    kill -9 "$CHECK_AGAIN"
  fi
fi

