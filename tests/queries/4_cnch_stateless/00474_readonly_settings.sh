#!/usr/bin/env bash

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="select toUInt64(pow(2, 62)) as value format JSON" --output_format_json_quote_64bit_integers=0 | grep value
$CLICKHOUSE_CLIENT --query="select toUInt64(pow(2, 62)) as value format JSON" --output_format_json_quote_64bit_integers=1 | grep value

$CLICKHOUSE_CLIENT --readonly=1 --multiquery --query="set output_format_json_quote_64bit_integers=1 ; select toUInt64(pow(2, 63)) as value format JSON" --server_logs_file=/dev/null 2>&1 | grep -o "value\|Cannot modify 'output_format_json_quote_64bit_integers' setting in readonly mode"
$CLICKHOUSE_CLIENT --readonly=1 --multiquery --query="set output_format_json_quote_64bit_integers=0 ; select toUInt64(pow(2, 63)) as value format JSON" --server_logs_file=/dev/null 2>&1 | grep -o "value\|Cannot modify 'output_format_json_quote_64bit_integers' setting in readonly mode"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1" | grep value
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0" | grep value

RAW_URL=${CLICKHOUSE_URL};
# remove change from ci
RAW_URL=`echo "${RAW_URL}" | sed "s@enable_optimizer=1&@@g"`
RAW_URL=`echo "${RAW_URL}" | sed "s@enable_optimizer=0&@@g"`
RAW_URL=`echo "${RAW_URL}" | sed "s@enable_optimizer_fallback=1&@@g"`
RAW_URL=`echo "${RAW_URL}" | sed "s@enable_optimizer_fallback=0&@@g"`
RAW_URL=`echo "${RAW_URL}" | sed "s@bsp_mode=1&@@g"`
RAW_URL=`echo "${RAW_URL}" | sed "s@tenant_id=1234&@@g"`
RAW_URL=`echo "${RAW_URL}" | sed "s@enable_nexus_fs=1&@@g"`

${CLICKHOUSE_CURL} -sS "${RAW_URL}&session_id=readonly&session_timeout=3600" -d 'SET readonly = 1'
${CLICKHOUSE_CURL} -sS "${RAW_URL}&session_id=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1" 2>&1 | grep -o "value\|Cannot modify 'output_format_json_quote_64bit_integers' setting in readonly mode"
${CLICKHOUSE_CURL} -sS "${RAW_URL}&session_id=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0" 2>&1 | grep -o "value\|Cannot modify 'output_format_json_quote_64bit_integers' setting in readonly mode"
