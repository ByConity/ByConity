#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t40111_tf"
$CLICKHOUSE_CLIENT --query="CREATE TABLE t40111_tf(a Int32, b Int32, c Int32, d Int32) ENGINE = CnchMergeTree() ORDER BY tuple()"
$CLICKHOUSE_CLIENT --query="INSERT INTO t40111_tf SELECT rand(1) % 5 as a, rand(2) % 7 as b, rand(3) % 11 as c, rand(4) % 10 as d FROM system.numbers LIMIT 100000"

tmp_file1=$(mktemp)
tmp_file2=$(mktemp)

### test grouping sets
sql_temp=$(cat <<EOF
SELECT
    a, b, c, sum(d)
FROM t40111_tf
GROUP BY #GROUPING
#ORDERING
LIMIT #LIMIT_NUM
EOF
)

sql=$(echo $sql_temp | sed "s/#GROUPING/GROUPING SETS ((a, b), (b, c))/" | sed "s/#ORDERING/ORDER BY b/" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) || echo "1.1 plan fail"
# the validation logic is: we limit enough rows and check a subset
$CLICKHOUSE_CLIENT --query="$sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=0" | awk '{if($2==0) print}' | sort >$tmp_file1
$CLICKHOUSE_CLIENT --query="$sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | awk '{if($2==0) print}' | sort >$tmp_file2
( diff $tmp_file1 $tmp_file2 &>/dev/null ) || echo "1.1 result fail"

sql=$(echo $sql_temp | sed "s/#GROUPING/GROUPING SETS ((a, b), (b, c))/" | sed "s/#ORDERING/ORDER BY a, b/" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) && echo "1.2 plan fail"

sql=$(echo $sql_temp | sed "s/#GROUPING/GROUPING SETS ((a, b), (b, c))/" | sed "s/#ORDERING//" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) || echo "1.3 plan fail"

sql=$(echo $sql_temp | sed "s/#GROUPING/GROUPING SETS ((a), (b, c))/" | sed "s/#ORDERING//" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) && echo "1.4 plan fail"

### test normal aggregation
sql_temp=$(cat <<EOF
SELECT
    a, b, sum(d)
FROM t40111_tf
GROUP BY #GROUPING
#ORDERING
LIMIT #LIMIT_NUM
EOF
)

sql=$(echo $sql_temp | sed "s/#GROUPING/a, b/" | sed "s/#ORDERING/ORDER BY b, a/" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) || echo "2.1 plan fail"

sql=$(echo $sql_temp | sed "s/#GROUPING/a, b/" | sed "s/#ORDERING/ORDER BY b/" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) || echo "2.2 plan fail"
$CLICKHOUSE_CLIENT --query="$sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=0" | awk '{if($2==0) print}' | sort >$tmp_file1
$CLICKHOUSE_CLIENT --query="$sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | awk '{if($2==0) print}' | sort >$tmp_file2
( diff $tmp_file1 $tmp_file2 &>/dev/null ) || echo "2.2 result fail"

sql=$(echo $sql_temp | sed "s/#GROUPING/a, b/" | sed "s/#ORDERING//" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) || echo "2.3 plan fail"

### test distinct
sql_temp=$(cat <<EOF
SELECT
    DISTINCT #DISTINCT_COLUMNS
FROM t40111_tf
#ORDERING
LIMIT #LIMIT_NUM
EOF
)

sql=$(echo $sql_temp | sed "s/#DISTINCT_COLUMNS/a, b/" | sed "s/#ORDERING/ORDER BY b, a/" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) || echo "3.1 plan fail"

sql=$(echo $sql_temp | sed "s/#DISTINCT_COLUMNS/a, b/" | sed "s/#ORDERING/ORDER BY b/" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) || echo "3.2 plan fail"
$CLICKHOUSE_CLIENT --query="$sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=0" | awk '{if($2==0) print}' | sort >$tmp_file1
$CLICKHOUSE_CLIENT --query="$sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | awk '{if($2==0) print}' | sort >$tmp_file2
( diff $tmp_file1 $tmp_file2 &>/dev/null ) || echo "3.2 result fail"

sql=$(echo $sql_temp | sed "s/#DISTINCT_COLUMNS/a, b/" | sed "s/#ORDERING//" | sed "s/#LIMIT_NUM/20/")
( $CLICKHOUSE_CLIENT --query="EXPLAIN $sql SETTINGS enable_optimizer=1, enable_create_topn_filtering_for_aggregating=1" | grep -iq 'TopNFiltering' ) || echo "3.3 plan fail"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t40111_tf"

exit 0
