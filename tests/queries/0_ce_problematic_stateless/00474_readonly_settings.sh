#!/usr/bin/env bash

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="select toUInt64(pow(2, 62)) as value format JSON" --output_format_json_quote_64bit_integers=0 | grep value
$CLICKHOUSE_CLIENT --query="select toUInt64(pow(2, 62)) as value format JSON" --output_format_json_quote_64bit_integers=1 | grep value

$CLICKHOUSE_CLIENT --readonly=1 --multiquery --query="set output_format_json_quote_64bit_integers=1 ; select toUInt64(pow(2, 63)) as value format JSON" --server_logs_file=/dev/null 2>&1 | grep -o 'value\|Cannot modify .* setting in readonly mode'
$CLICKHOUSE_CLIENT --readonly=1 --multiquery --query="set output_format_json_quote_64bit_integers=0 ; select toUInt64(pow(2, 63)) as value format JSON" --server_logs_file=/dev/null 2>&1 | grep -o 'value\|Cannot modify .* setting in readonly mode'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1" | grep value
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0" | grep value

# remove client option for readonly
# since client option like --enable_optimizer is also a modification
CLICKHOUSE_URL_RAW=`echo ${CLICKHOUSE_URL} | sed "s@${CLICKHOUSE_ADDITIONAL_CLIENT_URL_PARAMS}&@@g"`

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL_RAW}&session_id=readonly&session_timeout=3600" -d 'SET readonly = 1'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL_RAW}&session_id=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1" 2>&1 | grep -o 'value\|Cannot modify .* setting in readonly mode.'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL_RAW}&session_id=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0" 2>&1 | grep -o 'value\|Cannot modify .* setting in readonly mode'
