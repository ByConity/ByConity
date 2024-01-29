#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

THREADS=8
RAND=$(($RANDOM))
LIMIT=10000
function run_selects()
{
    thread_num=$1
    # This is different from the test on ce. We only check for information schema here.
    # Cnch has quite a number of system tables that are problematic to SELECT * from. E.g. some mandate a WHERE clause.
    readarray -t tables_arr < <(${CLICKHOUSE_CLIENT} -q "SELECT database || '.' || name FROM system.tables
    WHERE database in ('information_schema', 'INFORMATION_SCHEMA')
    AND sipHash64(name || toString($RAND)) % $THREADS = $thread_num")

    for t in "${tables_arr[@]}"
    do
        ${CLICKHOUSE_CLIENT} -q "SELECT * FROM $t LIMIT $LIMIT FORMAT Null" # Suppress style check: database=$CLICKHOUSE_DATABASEs
    done
}

for ((i=0; i<THREADS; i++)) do
    run_selects "$i" &
done

wait
