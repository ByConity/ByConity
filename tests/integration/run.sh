#!/bin/bash

cd /
mkdir -p test_output
export CLICKHOUSE_TESTS_JSON_PARAMS_PATH=/home/code/tests/integration/params.json
export CLICKHOUSE_TESTS_REPO_PATH=/home/code/
export CLICKHOUSE_TESTS_BUILD_PATH=/clickhouse/bin
export CLICKHOUSE_TESTS_RESULT_PATH=/test_output
export CLICKHOUSE_BINARY_PATH=/clickhouse/bin/clickhouse-server
export CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH=/clickhouse/bin/clickhouse-odbc-bridge
export CLICKHOUSE_TESTS_LIBRARY_BRIDGE_BIN_PATH=/clickhouse/bin/clickhouse-library-bridge
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=/clickhouse/etc/clickhouse-server
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=/clickhouse/bin/clickhouse-server
export CLICKHOUSE_TESTS_INTEGRATION_PATH=/home/code/tests/integration
export CLICKHOUSE_SRC_DIR=/home/code/src
export LANG=en_GB.UTF-8

python3 /home/code/tests/integration/ci-runner.py