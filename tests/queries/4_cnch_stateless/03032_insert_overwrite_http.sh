#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "CREATE TABLE ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http (id UInt64) ENGINE = CnchMergeTree ORDER BY id"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "INSERT INTO ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http VALUES (0)"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "SELECT * FROM ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http ORDER BY id"

echo "---- INSERT VALUES enable_optimizer = 0"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}&enable_optimizer=0" -d "INSERT OVERWRITE ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http VALUES (1)"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "SELECT * FROM ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http ORDER BY id"

echo "---- INSERT VALUES enable_optimizer = 1"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}&enable_optimizer=1" -d "INSERT OVERWRITE ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http VALUES (2)"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "SELECT * FROM ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http ORDER BY id"

echo "---- INSERT SELECT enable_optimizer = 0"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}&enable_optimizer=0" -d "INSERT OVERWRITE ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http SELECT number as id FROM numbers(3)"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "SELECT * FROM ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http ORDER BY id"

echo "---- INSERT SELECT enable_optimizer = 1"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}&enable_optimizer=1" -d "INSERT OVERWRITE ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http SELECT number as id FROM numbers(4)"
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "SELECT * FROM ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http ORDER BY id"

${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.test_insert_overwrite_http"
