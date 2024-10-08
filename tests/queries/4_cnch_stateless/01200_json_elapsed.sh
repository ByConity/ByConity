#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

run_one_sql() {
    curlTime=$(${CLICKHOUSE_CURL_COMMAND} -o test_01200_json_elapsed.txt -s -w '%{time_total}\n' "${CLICKHOUSE_PURE_URL}/?query=$1")
    cat test_01200_json_elapsed.txt| awk -v curlTime=$curlTime '/elapsed/ {printf (curlTime >= $2) }'
    echo
    cat test_01200_json_elapsed.txt| awk '/elapsed/ {printf ($2 > 1) }'
    echo
    rm test_01200_json_elapsed.txt
}

#  WITH ( SELECT sleepEachRow(1)) AS sub SELECT sum(number) from (SELECT number FROM system.numbers LIMIT 3) FORMAT JSON settings enable_optimizer=0;
q1="WITH%20%28%20SELECT%20sleepEachRow%281%29%29%20AS%20sub%20SELECT%20sum%28number%29%20from%20%28SELECT%20number%20FROM%20system.numbers%20LIMIT%2010%20%29%20FORMAT%20JSON%20settings%20enable_optimizer%3D0"
run_one_sql $q1
