#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CURDIR/../shell_config.sh

set -e -o pipefail

### Create table stuff
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.insert_infile;

CREATE TABLE test.insert_infile (d Date, id Int32, val Int32)
    ENGINE = CnchMergeTree PARTITION BY d ORDER BY id UNIQUE KEY id;

EOF

### Test insert into infile
$CLICKHOUSE_CLIENT --query="INSERT INTO test.insert_infile INFILE '$CURDIR/10104_uniquekey_insert_infile.data'"
$CLICKHOUSE_CLIENT --query "SELECT * FROM test.insert_infile;"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test.insert_infile;"
