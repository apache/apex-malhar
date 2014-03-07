#!/bin/sh
export STATIC_DIR=/dist
export PORT=3003
export GATEWAY_HOST=localhost
export GATEWAY_PORT=9090
export MACHINE_REDIS_HOST=
export MACHINE_REDIS_PORT=
export MACHINE_REDIS_DB_INDEX=
export ADS_REDIS_HOST=
export ADS_REDIS_PORT=
export MONGODB_HOST=

node app.js