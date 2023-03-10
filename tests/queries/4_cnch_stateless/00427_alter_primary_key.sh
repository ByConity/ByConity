#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

function perform()
{
    local query=$1
    TZ=UTC $CLICKHOUSE_CLIENT \
        --use_client_time_zone=1 \
        --input_format_values_interpret_expressions=0 \
        --query "$query" 2>/dev/null
    if [ "$?" -ne 0 ]; then
        echo "query failed"
    fi
}

perform "DROP TABLE IF EXISTS alter"
perform "CREATE TABLE alter (d Date, a Enum8('foo'=1), b DateTime, c DateTime) ENGINE=CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY (a, b, toTime(c)) SETTINGS index_granularity = 8192"

perform "INSERT INTO alter VALUES ('2017-02-09', 'foo', '2017-02-09 00:00:00', '2017-02-09 00:00:00')"

# Must fail because d is used as as a date column in CnchMergeTree
perform "ALTER TABLE alter MODIFY COLUMN d UInt16"

perform "ALTER TABLE alter MODIFY COLUMN a Enum8('foo'=1, 'bar'=2)"
perform "ALTER TABLE alter MODIFY COLUMN b UInt32"

# Must fail because column c is used in primary key via an expression.
perform "ALTER TABLE alter MODIFY COLUMN c UInt32"

perform "INSERT INTO alter VALUES ('2017-02-09', 'bar', 1486598400, '2017-02-09 00:00:00')"

perform "SELECT d FROM alter WHERE a = 'bar'"

perform "SELECT a, b, b = toUnixTimestamp(c) FROM alter ORDER BY a FORMAT TSV"

perform "DROP TABLE alter"
