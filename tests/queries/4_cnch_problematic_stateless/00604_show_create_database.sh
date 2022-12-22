#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CUR_DIR/../shell_config.sh

CUR_FILE=$(basename ${BASH_SOURCE[0]})
REF_FILE="${CUR_FILE%.*}".reference

GENERATED_UUID=`${CLICKHOUSE_CLIENT} --query="SELECT generateUUIDv4()"`

CREATE_QUERY="CREATE DATABASE IF NOT EXISTS test_show_create_database UUID '$GENERATED_UUID'"

echo "CREATE DATABASE test_show_create_database UUID \'$GENERATED_UUID\' ENGINE = Cnch" > $CUR_DIR/$REF_FILE

${CLICKHOUSE_CLIENT} --query="CREATE DATABASE IF NOT EXISTS test_show_create_database UUID '$GENERATED_UUID'"
${CLICKHOUSE_CLIENT} --query="SHOW CREATE DATABASE test_show_create_database"
${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS test_show_create_database"
