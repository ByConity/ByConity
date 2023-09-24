#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_part_split_alloc;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_part_split_alloc (id UInt32, code UInt32, record String) ENGINE = CnchMergeTree() PARTITION BY id CLUSTER BY code INTO 4 BUCKETS ORDER BY id;"

# Check if part allocation has been split for partially clustered table
${CLICKHOUSE_CLIENT} --query="SELECT count(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'CnchPartAllocationSplits')]) from system.query_log WHERE has (databases, currentDatabase()) FORMAT CSV;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE test_part_split_alloc SELECT toUInt32(number/100), toUInt32(number/5), concat('record', toString(number)) FROM system.numbers LIMIT 3000;"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE test_part_split_alloc MODIFY CLUSTER BY code into 8 BUCKETS;"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE test_part_split_alloc SELECT toUInt32(number/100), toUInt32(number/5), concat('record', toString(number)) FROM system.numbers LIMIT 500;"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test_part_split_alloc;" > /dev/null 2> /dev/null;
${CLICKHOUSE_CLIENT} --query="SELECT sum(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'CnchPartAllocationSplits')]) >= 1 from system.query_log WHERE has (databases, currentDatabase()) FORMAT CSV;"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test_part_split_alloc WHERE code = 0 ORDER BY id LIMIT 1 FORMAT CSV;"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test_part_split_alloc FORMAT CSV;"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test_part_split_alloc;"
