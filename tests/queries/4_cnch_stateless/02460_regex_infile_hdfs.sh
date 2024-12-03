#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_regex_infile;"

${CLICKHOUSE_CLIENT} --query "SELECT '1' INTO OUTFILE 'hdfs://${HDFS_PATH_ROOT}/outfile_02460/clickhouse_outfile_1.csv' FORMAT CSV COMPRESSION 'none' SETTINGS outfile_in_server_with_tcp = 1, overwrite_current_file=1;"

${CLICKHOUSE_CLIENT} --query "SELECT '2' INTO OUTFILE 'hdfs://${HDFS_PATH_ROOT}/outfile_02460/clickhouse_outfile_2.csv' FORMAT CSV COMPRESSION 'none' SETTINGS outfile_in_server_with_tcp = 1, overwrite_current_file=1;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_regex_infile;"

${CLICKHOUSE_CLIENT} --query "Create TABLE test_regex_infile (a UInt8) ENGINE = CnchMergeTree() ORDER BY a;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_regex_infile FORMAT CSV INFILE 'hdfs://${HDFS_PATH_ROOT}/outfile_02460/clickhouse_outfile_1.csv';"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_regex_infile;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_regex_infile FORMAT CSV INFILE 'hdfs://${HDFS_PATH_ROOT}/outfile_02460/*';"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_regex_infile;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_regex_infile FORMAT CSV INFILE 'hdfs://${HDFS_PATH_ROOT}/outfile_02460/clickhouse_outfile_?.csv';"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_regex_infile;"
