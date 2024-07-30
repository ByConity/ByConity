#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "create database if not exists db1"
${CLICKHOUSE_CLIENT} --query "create table if not exists db1.tb(x int, y int) engine=CnchMergeTree() order by x"
${CLICKHOUSE_CLIENT} --query "alter table db1.tb add index vix y TYPE SET(100) granularity 2"
${CLICKHOUSE_CLIENT} --query "insert into db1.tb select number, 2 * number from numbers(10)"

${CLICKHOUSE_CLIENT} --query "select name from system.cnch_databases where name = 'db1'"
${CLICKHOUSE_CLIENT} --query "select database, name, sorting_key from system.cnch_tables where database = 'db1' and name = 'tb'"
${CLICKHOUSE_CLIENT} --query "select database, table, name from system.cnch_columns where database = 'db1' and table = 'tb'"
${CLICKHOUSE_CLIENT} --query "select distinct database, table from system.cnch_parts where database = 'db1' and table = 'tb' and visible=1 settings enable_multiple_tables_for_cnch_parts=1"

USER_NUM=0
helper() {
    local test_name
    local grant
    test_name="$1"
    grant="$2"

    : $((USER_NUM++))
    NEW_USER=65007_user"$USER_NUM"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $NEW_USER"
    ${CLICKHOUSE_CLIENT} --query "CREATE USER $NEW_USER IDENTIFIED WITH plaintext_password BY 'password'"
    if test -n "$grant"
    then
        ${CLICKHOUSE_CLIENT} --query 'GRANT '"$grant"' TO '"$NEW_USER"
    fi

    TENANTED_USER=$NEW_USER
    [ -v TENANT_ID ] && TENANTED_USER="${TENANT_ID}\`$NEW_USER"

    echo "$test_name"
    ${CLICKHOUSE_CLIENT} --user "$TENANTED_USER"  --password 'password'  --query "select name from system.cnch_databases where name = 'db1'"
    ${CLICKHOUSE_CLIENT} --user "$TENANTED_USER"  --password 'password'  --query "select database, name, sorting_key from system.cnch_tables where database = 'db1' and name = 'tb'"
    ${CLICKHOUSE_CLIENT} --user "$TENANTED_USER"  --password 'password'  --query "select database, table, name from system.cnch_columns where database = 'db1' and table = 'tb'"
    ${CLICKHOUSE_CLIENT} --user "$TENANTED_USER"  --password 'password'  --query "select distinct database, table from system.cnch_parts where database = 'db1' and table = 'tb' and visible=1 settings enable_multiple_tables_for_cnch_parts=1"
    ${CLICKHOUSE_CLIENT} --user "$TENANTED_USER"  --password 'password'  --query "select name from system.data_skipping_indices"

    if [ "$USER_NUM" -lt 5 ]
    then
      ${CLICKHOUSE_CLIENT} --user "$TENANTED_USER"  --password 'password'  --query "show users" 2>&1 | grep -c -m 1 "Code: 497"
    else 
      ${CLICKHOUSE_CLIENT} --user "$TENANTED_USER"  --password 'password'  --query "show users" | grep -c -m 1 "65007_user"
    fi

    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $NEW_USER"
}

helper 'no grant'
helper 'show databases grant' 'SHOW DATABASES ON db1.*'
helper 'show tables grant' 'SHOW TABLES ON db1.*'
helper 'show columns grant' 'SHOW COLUMNS ON db1.*'
helper 'show users grant' 'SHOW USERS ON *.*'

${CLICKHOUSE_CLIENT} --query "DROP DATABASE db1"
