#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# create files using compression method and without it to check that both queries work correct
${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World! From client first.') INTO OUTFILE 'hdfs://${HDFS_PATH_ROOT}/test_comp_for_input_and_output.gz' FORMAT TabSeparated SETTINGS outfile_in_server_with_tcp=1, overwrite_current_file=1;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World! From client second.') INTO OUTFILE 'hdfs://${HDFS_PATH_ROOT}/test_comp_for_input_and_output_without_gz' FORMAT TabSeparated COMPRESSION 'GZ' SETTINGS outfile_in_server_with_tcp=1, overwrite_current_file=1;"

# create table to check inserts
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_compression_keyword;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_compression_keyword (text String) Engine=CnchMergeTree() ORDER BY text;"

# insert them
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_compression_keyword FORMAT TabSeparated INFILE 'hdfs://${HDFS_PATH_ROOT}/test_comp_for_input_and_output.gz';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_compression_keyword FORMAT TabSeparated INFILE 'hdfs://${HDFS_PATH_ROOT}/test_comp_for_input_and_output.gz' COMPRESSION 'gz';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_compression_keyword FORMAT TabSeparated INFILE 'hdfs://${HDFS_PATH_ROOT}/test_comp_for_input_and_output_without_gz' COMPRESSION 'gz';"

# check result
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_compression_keyword order by text;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_compression_keyword;"
