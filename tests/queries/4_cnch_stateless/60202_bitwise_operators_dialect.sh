#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. $CURDIR/../shell_config.sh

helper() {
    local sql
    sql=$1
    echo 'query: '"$sql"
    $CLICKHOUSE_CLIENT --dialect_type='CLICKHOUSE' --query "$sql" 2>&1 >/dev/null | grep -c -m 1 "Code: 62. DB::Exception"
    $CLICKHOUSE_CLIENT --dialect_type='ANSI' --query "$sql" 2>&1 >/dev/null | grep -c -m 1 "Code: 62. DB::Exception"
    $CLICKHOUSE_CLIENT --dialect_type='MYSQL' --query "$sql"
}

helper 'SELECT 1 | 2'
helper 'SELECT 1 & 2' 
helper 'SELECT 1 << 2'
helper 'SELECT 1 >> 2'
helper 'SELECT 1 ^ 2'
