#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# Mysql database already has testing table `test.test` with data. See docker-compose-integration-test.yml

${CLICKHOUSE_CLIENT} --query="CREATE DATABASE materialized_my_sql_02478 ENGINE=CnchMaterializedMySQL('mysql_server:3306', 'test', 'root', 'password');"
${CLICKHOUSE_CLIENT} --query="SYSTEM START MATERIALIZEDMYSQL materialized_my_sql_02478;"

# Step 3: Check Consistency.

## Try until sync is started.
db_sync_finished() {
	local RET=""
	RET=$(${CLICKHOUSE_CLIENT} --query="SELECT sync_threads_number FROM system.cnch_materialized_mysql where mysql_database = 'test'")
	if [[ -z $RET ]] || [[ $RET == "0" ]]; then
		return 1
	else
		return 0
	fi
}

n=0
maxAttempts=60

until db_sync_finished; do
	sleep 1
	((n++))
	if [ $n -eq $maxAttempts ]; then
		echo "Timeout!"
		set -x
		${CLICKHOUSE_CLIENT} --query="SELECT * FROM materialized_my_sql_02478.test"
		${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.cnch_materialized_mysql where database = 'materialized_my_sql_02478'"
		${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.cnch_materialized_mysql"
		exit 1
	fi
done
sleep 1

${CLICKHOUSE_CLIENT} --query="SELECT id, column1 FROM materialized_my_sql_02478.test ORDER BY id;"

# Step 4: Drop extra databases we've created.
${CLICKHOUSE_CLIENT} --query="DROP DATABASE materialized_my_sql_02478;"
