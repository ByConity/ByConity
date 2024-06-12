#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TENANT_ID=1234

${CLICKHOUSE_CLIENT} --tenant_id="$TENANT_ID" --query "drop database if exists 65007_tenant_system_cnch_databases"
${CLICKHOUSE_CLIENT} --tenant_id="$TENANT_ID" --query "create database 65007_tenant_system_cnch_databases"

${CLICKHOUSE_CLIENT} --tenant_id="$TENANT_ID" --query "select name from system.cnch_databases where name='65007_tenant_system_cnch_databases'"
${CLICKHOUSE_CLIENT} --tenant_id="$((TENANT_ID + 1))" --query "select name from system.cnch_databases where name='65007_tenant_system_cnch_databases'"

${CLICKHOUSE_CLIENT} --tenant_id="$TENANT_ID" --query "drop database 65007_tenant_system_cnch_databases"