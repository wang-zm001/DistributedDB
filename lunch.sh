#!/bin/bash
set -e

trap 'killall DistributedDB' SIGINT

cd $(dirname $0)

killall DistributedDB || true
sleep 0.1

go install -v

go build

./DistributedDB -db-location=num0.db -config-file=sharding.toml -shard=num0 &
./DistributedDB -db-location=num1.db -config-file=sharding.toml -shard=num1 &
# ./DistributedDB -db-location=num2.db -config-file=sharding.toml -shard=num2 &
# ./DistributedDB -db-location=num3.db -config-file=sharding.toml -shard=num3 &

wait
