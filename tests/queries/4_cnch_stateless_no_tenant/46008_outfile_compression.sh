#!/usr/bin/env bash

# set -x

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT * FROM (SELECT 'Hello, World! From client_1.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client_1.gz' COMPRESSION 'auto'";
gunzip ${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client_1.gz
cat ${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client_1


${CLICKHOUSE_CLIENT} --query="SELECT * FROM (SELECT 'Hello, World! From client_2.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client_2.gz' COMPRESSION 'gz' LEVEL 9";
gunzip ${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client_2.gz
cat ${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client_2

rm -f "${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client_1"
rm -f "${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client_2"