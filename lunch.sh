#!/bin/bash
set -e

trap 'killall DistributedDB' SIGINT

cd $(dirname $0)

killall DistributedDB || true
sleep 0.1

go install -v

go build

./DistributedDB -db-location=./db/file/db0.db -http-addr=127.0.0.1:8080 -shard=db0 &
./DistributedDB -db-location=./db/file/db0-r.db -http-addr=127.0.0.1:8090 -shard=db0 -replica &

./DistributedDB -db-location=./db/file/db1.db -http-addr=127.0.0.1:8081  -shard=db1 &
./DistributedDB -db-location=./db/file/db1-r.db -http-addr=127.0.0.1:8091 -shard=db1 -replica &

# ./DistributedDB -db-location=./db/file/db2.db -http-addr=127.0.0.1:8082 -shard=db2 &
# ./DistributedDB -db-location=./db/file/db2-r.db -http-addr=127.0.0.1:8092 -shard=db2 -replica &

# ./DistributedDB -db-location=./db/file/db3.db -http-addr=127.0.0.1:8083  -shard=db3 &
# ./DistributedDB -db-location=./db/file/db3-r.db -http-addr=127.0.0.1:8093 -shard=db3 -replica &

wait
