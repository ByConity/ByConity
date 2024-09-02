#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

set -e
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Check that information_schema is always visible
NEW_USER=01161_user
${CLICKHOUSE_CLIENT} --query='DROP USER IF EXISTS '"$NEW_USER"
${CLICKHOUSE_CLIENT} --query='CREATE USER '"$NEW_USER"' IDENTIFIED WITH plaintext_password BY '"'"'password'"'"
TENANTED_USER=$NEW_USER
[ -v TENANT_ID ] && TENANTED_USER="${TENANT_ID}\`$NEW_USER"
${CLICKHOUSE_CLIENT} --user="$TENANTED_USER" --password=password --query='SHOW DATABASES LIKE '"'"'information_schema'"'"
${CLICKHOUSE_CLIENT} --user="$TENANTED_USER" --password=password --query='SHOW DATABASES LIKE '"'"'INFORMATION_SCHEMA'"'"
${CLICKHOUSE_CLIENT} --query='DROP USER '"$NEW_USER"
