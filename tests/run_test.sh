#!/bin/bash

test_path="/tmp/.chronus_test_path"
rm -rf $test_path
mkdir -p $test_path
mkdir -p $test_path/bin
cd ../cmd/influxd/ && go build && mv influxd $test_path/bin/influxd
cd -

cd ../cmd/metad/ && go build && mv metad $test_path/bin/metad
cd -

#cd $test_dir/ && ./influxd config > influxd.conf && ./metad config > metad.conf

go test

killall -9 influxd metad
