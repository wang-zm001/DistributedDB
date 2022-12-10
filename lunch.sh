#!/bin/bash
set -e

trap 'killall DistributedDB' SIGINT

cd $(dirname $0)

killall DistributedDB || true
sleep 0.1

go install -v

go build

./DistributedDB -db-location=./db/file/num0.db -http-addr=192.168.253.1:8080 -config-file=sharding.toml -shard=num0 &
./DistributedDB -db-location=./db/file/num0-r.db -http-addr=192.168.253.1:8090 -config-file=sharding.toml -shard=num0 -replica &

# ./DistributedDB -db-location=num1.db -http-addr=127.0.0.1:8081 -config-file=sharding.toml -shard=num1 &
# ./DistributedDB -db-location=num1-r.db -http-addr=127.0.0.1:8091 -config-file=sharding.toml -shard=num1 -replica &

# ./DistributedDB -db-location=num2.db -http-addr=127.0.0.1:8082 -config-file=sharding.toml -shard=num2 &
# ./DistributedDB -db-location=num2-r.db -http-addr=127.0.0.1:8092 -config-file=sharding.toml -shard=num2 -replica &

# ./DistributedDB -db-location=num3.db -http-addr=127.0.0.1:8083 -config-file=sharding.toml -shard=num3 &
# ./DistributedDB -db-location=num3-r.db -http-addr=127.0.0.1:8093 -config-file=sharding.toml -shard=num3 -replica &

wait
