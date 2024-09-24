#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


($CLICKHOUSE_CLIENT --query="explain select 1 as a from system.one where (select dummy from system.one) settings enable_optimizer=1, early_execute_scalar_subquery = 0"  |  grep -iq 'join' ) || echo 'fail 1'
($CLICKHOUSE_CLIENT --query="explain select 1 as a from system.one where (select dummy from system.one) settings enable_optimizer=1, early_execute_scalar_subquery = 1"  |  grep -iq 'join' ) && echo 'fail 2'
($CLICKHOUSE_CLIENT --query="explain select 1 as a from system.one where 1 in (select dummy from system.one) settings enable_optimizer=1, early_execute_in_subquery = 0"  |  grep -iq 'join' ) || echo 'fail 3'
($CLICKHOUSE_CLIENT --query="explain select 1 as a from system.one where 1 in (select dummy from system.one) settings enable_optimizer=1, early_execute_in_subquery = 1"  |  grep -iq 'join' ) && echo 'fail 4'
echo "done"
