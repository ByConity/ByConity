#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

($CLICKHOUSE_CLIENT --query="explain select 1 from (select 1 as a, NULL as b, 3 as c, NULL as d) s join (select 1 as a, NULL as b, 3 as c, NULL as d) t on s.a = t.a and s.b IS NOT DISTINCT FROM t.b and s.c = t.c and s.d <=> t.d settings enable_optimizer = 1"  |  grep -iq 'null aware' ) || echo 'fail'
($CLICKHOUSE_CLIENT --query="explain select 1 from (select 1 as a, NULL as b, 3 as c, NULL as d) s join (select 1 as a, NULL as b, 3 as c, NULL as d) t on s.a = t.a and s.b IS NOT DISTINCT FROM t.b and s.c = t.c and s.d <=> t.d settings enable_optimizer = 1"  |  grep -iq 'Filter' ) && echo 'fail'

exit 0
