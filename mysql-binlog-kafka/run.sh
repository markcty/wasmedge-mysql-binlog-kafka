#!/bin/bash

cargo build --target=wasm32-wasi

wasmedge \
  --env SLEEP_TIME=1000 \
  --env SQL_USERNAME=root \
  --env SQL_PASSWORD=password \
  --env SQL_HOSTNAME=localhost \
  --env SQL_PORT=3306 \
  --env SQL_DATABASE=mysql \
  --env KAFKA_URL=localhost:9092 target/wasm32-wasi/debug/mysql-binlog-kafka.wasm
