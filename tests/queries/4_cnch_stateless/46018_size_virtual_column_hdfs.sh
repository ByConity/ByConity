#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "SELECT * FROM numbers(10) INTO OUTFILE 'hdfs://${HDFS_PATH_ROOT}/test_46018/clickhouse_outfile_1.csv' FORMAT CSV COMPRESSION 'none' SETTINGS outfile_in_server_with_tcp = 1, overwrite_current_file=1;"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM numbers(1000) INTO OUTFILE 'hdfs://${HDFS_PATH_ROOT}/test_46018/clickhouse_outfile_2.csv' FORMAT CSV COMPRESSION 'none' SETTINGS outfile_in_server_with_tcp = 1, overwrite_current_file=1;"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM CnchHDFS('hdfs://${HDFS_PATH_ROOT}/test_46018/*.csv', 'number UInt64', 'CSV', 'none');"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM CnchHDFS('hdfs://${HDFS_PATH_ROOT}/test_46018/*.csv', 'number UInt64', 'CSV', 'none') where _size > 1000;"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM CnchHDFS('hdfs://${HDFS_PATH_ROOT}/test_46018/*.csv', 'number UInt64', 'CSV', 'none') where _size < 1000;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE hdfs_table (number UInt64) ENGINE = CnchHDFS('hdfs://${HDFS_PATH_ROOT}/test_46018/*.csv', 'CSV', 'none');"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM hdfs_table;"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM hdfs_table where _size > 1000;"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM hdfs_table where _size < 1000;"
