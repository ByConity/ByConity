#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="drop table if exists test.test_stats"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.test_stats(x UInt64, id UInt64) Engine=CnchMergeTree order by id;"
${CLICKHOUSE_CLIENT} --query="insert into test.test_stats select cityHash64(intDiv(number, 10)), number from system.numbers limit 10000000"
echo "create stats2"
${CLICKHOUSE_CLIENT} --query="create stats test.test_stats settings statistics_enable_sample=1, create_stats_time_output=0, statistics_sample_row_count=1000000"
INPUT=`${CLICKHOUSE_CLIENT} --query="show stats test.test_stats"`

# echo identifier, <NOTHING>, full_count
echo "$INPUT" | grep "test_stats\.\*"

DATA=(`echo "$INPUT" | grep "test_stats\.x"`)
# echo identifier, type, nonnull_count, null_count
echo ${DATA[0]} ${DATA[1]} ${DATA[2]} ${DATA[3]}
# test NDV
if ((${DATA[4]} >= 900000 && ${DATA[4]} <= 1100000)); then
echo "GOOD NDV for x"
else
echo "BAD NDV ${DATA[4]} for x"
fi

# echo identifier, <NOTHING>, full_count
DATA=(`echo "$INPUT" | grep "test_stats\.id"`)
# echo identifier, type, nonnull_count, null_count
echo ${DATA[0]} ${DATA[1]} ${DATA[2]} ${DATA[3]}
# test NDV
if ((${DATA[4]} >= 9000000 && ${DATA[4]} <= 11000000)); then
echo "GOOD NDV for id"
else
echo "BAD NDV ${DATA[4]} for id"
fi
