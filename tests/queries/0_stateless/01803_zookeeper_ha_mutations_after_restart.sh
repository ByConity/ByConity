#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_restart_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_restart_r2"

# set max_replicated_mutations_in_queue = 1 so that part is altered one by one
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ha_mut_restart_r1(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ha_mut_restart', 'r1') PARTITION BY d ORDER BY x SETTINGS max_replicated_mutations_in_queue = 1"
sleep 1
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ha_mut_restart_r2(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ha_mut_restart', 'r2') PARTITION BY d ORDER BY x"

${CLICKHOUSE_CLIENT} --query="INSERT INTO ha_mut_restart_r1 VALUES ('2021-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO ha_mut_restart_r1 VALUES ('2021-01-01', 2, 'b')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO ha_mut_restart_r1 VALUES ('2021-01-01', 3, 'c')"

# needs at least 9 sec (3s for each of the 3 parts) to finish
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ha_mut_restart_r1 fastdelete x where sleepEachRow(3)"

# wait until we've altered one part
for i in {1..100}
do
    sleep 0.1
    if [[ $(${CLICKHOUSE_CLIENT} --query="SELECT min(parts_to_do) FROM system.mutations WHERE database='$CLICKHOUSE_DATABASE' AND table='ha_mut_restart_r1' AND mutation_id='0000000000'") -eq 2 ]]; then
        break
    fi

    if [[ $i -eq 100 ]]; then
        echo "Timed out while waiting for mutation to execute!"
    fi
done

# detach & attach table to simulate server restarts
${CLICKHOUSE_CLIENT} --query="DETACH TABLE ha_mut_restart_r1"
${CLICKHOUSE_CLIENT} --query="ATTACH TABLE ha_mut_restart_r1"
# check that mutation can finish after server restarts
${CLICKHOUSE_CLIENT} --query="system sync mutation ha_mut_restart_r1" 2> /dev/null
${CLICKHOUSE_CLIENT} --query="SELECT 'r1', min(is_done) FROM system.mutations WHERE database='$CLICKHOUSE_DATABASE' AND table='ha_mut_restart_r1' AND mutation_id='0000000000'"
${CLICKHOUSE_CLIENT} --query="SELECT name FROM system.parts WHERE database='$CLICKHOUSE_DATABASE' AND table='ha_mut_restart_r1' AND active order by name"
${CLICKHOUSE_CLIENT} --query="system sync mutation ha_mut_restart_r2" 2> /dev/null
${CLICKHOUSE_CLIENT} --query="SELECT 'r2', min(is_done) FROM system.mutations WHERE database='$CLICKHOUSE_DATABASE' AND table='ha_mut_restart_r2' AND mutation_id='0000000000'"
${CLICKHOUSE_CLIENT} --query="SELECT name FROM system.parts WHERE database='$CLICKHOUSE_DATABASE' AND table='ha_mut_restart_r2' AND active order by name"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_restart_r1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ha_mut_restart_r2"
