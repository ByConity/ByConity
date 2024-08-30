#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


COL_NUMBER=5000

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET max_query_size=0;
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.big_table;
EOF

QUERY=$(
  echo "CREATE TABLE test.big_table(p_date Date, id Int32, "
  for i in $(seq 1 5000); do
    printf %s, "col_$i String COMMENT 'Add some column comment to make the column definition suprisingly loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong' "
  done
  echo "last_column String) ENGINE=CnchMergeTree PARTITION BY p_date ORDER BY id"
)

echo $QUERY | $CLICKHOUSE_CLIENT -n --max_query_size 0

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET max_query_size=0;
SELECT countIf(length(definition) > 1000000) FROM system.cnch_tables where database='test' AND name='big_table';
ALTER TABLE test.big_table ADD COLUMN extra String;
SELECT count() FROM system.cnch_columns WHERE database='test' AND table='big_table' AND name='extra';
DROP TABLE test.big_table;
EOF

rm -f $TEMP_QUERY_FILE