#!/usr/bin/sh

export BUNDLE_GID=yauza.benchmark
export BUNDLE_AID=yauza-benchmark
export BUNDLE_PKG=yauza.benchmark

export BUNDLE_BIN=~/bundles/bin
#export BUNDLE_SRC=~/bundles/src
export BUNDLE_SRC=`pwd`


#rm -Rf "$BUNDLE_BIN/$BUNDLE_AID"
#cd $BUNDLE_SRC/$BUNDLE_AID

mvn deploy -Pdev -DskipTests

#mvn clean deploy -Pdev

#mvn clean package -Pdev
