#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="create database if not exists t40112"
$CLICKHOUSE_CLIENT --query="drop table if exists t40112.t40112_read_metrics"
$CLICKHOUSE_CLIENT --query="drop table if exists t40112.t40112_read_metrics_complex_partition_key"
$CLICKHOUSE_CLIENT --query="drop table if exists t40112.t40112_read_metrics_modulo_legacy"

$CLICKHOUSE_CLIENT <<EOF
create table t40112.t40112_read_metrics
(
    date Date,
    uid Int32,
    int_vid Array(Int32) BitmapIndex,
    events Int64,
    PROJECTION proj1 (
        SELECT date, uid, sum(events) GROUP BY date, uid
    )
) engine = CnchMergeTree() partition by date order by uid settings min_bytes_for_wide_part = 0, index_granularity = 1024, enable_build_ab_index = 1;
EOF

$CLICKHOUSE_CLIENT <<EOF
create table t40112.t40112_read_metrics_complex_partition_key
(
    date Date,
    uid Int32,
    int_vid Array(Int32) BitmapIndex,
    events Int64
) engine = CnchMergeTree() partition by toStartOfYear(date) order by uid settings min_bytes_for_wide_part = 0, index_granularity = 1024, enable_build_ab_index = 1;
EOF

$CLICKHOUSE_CLIENT <<EOF
create table t40112.t40112_read_metrics_modulo_legacy
(
    date_id Int32,
    uid Int32,
    int_vid Array(Int32) BitmapIndex,
    events Int64
) engine = CnchMergeTree() partition by date_id % 100 order by uid settings min_bytes_for_wide_part = 0, index_granularity = 1024, enable_build_ab_index = 1;
EOF

$CLICKHOUSE_CLIENT --query="insert into t40112.t40112_read_metrics select '2019-01-01' as date, number / 1024 as uid, range(intDiv(number, 1024) + 1) as int_vid, 1 as events from system.numbers limit 2048"
$CLICKHOUSE_CLIENT --query="insert into t40112.t40112_read_metrics select '2019-01-02' as date, number / 1024 as uid, range(intDiv(number, 1024) + 1) as int_vid, 1 as events from system.numbers limit 2048"

$CLICKHOUSE_CLIENT --query="insert into t40112.t40112_read_metrics_complex_partition_key select '2018-12-31' as date, number / 1024 as uid, range(intDiv(number, 1024) + 1) as int_vid, 1 as events from system.numbers limit 2048"
$CLICKHOUSE_CLIENT --query="insert into t40112.t40112_read_metrics_complex_partition_key select '2019-01-02' as date, number / 1024 as uid, range(intDiv(number, 1024) + 1) as int_vid, 1 as events from system.numbers limit 2048"

$CLICKHOUSE_CLIENT --query="insert into t40112.t40112_read_metrics_modulo_legacy select 1 as date, number / 1024 as uid, range(intDiv(number, 1024) + 1) as int_vid, 1 as events from system.numbers limit 2048"
$CLICKHOUSE_CLIENT --query="insert into t40112.t40112_read_metrics_modulo_legacy select 2 as date, number / 1024 as uid, range(intDiv(number, 1024) + 1) as int_vid, 1 as events from system.numbers limit 2048"

# $CLICKHOUSE_CLIENT --query="optimize table t40112.t40112_read_metrics"
# $CLICKHOUSE_CLIENT --query="optimize table t40112.t40112_read_metrics_complex_partition_key"
# $CLICKHOUSE_CLIENT --query="optimize table t40112.t40112_read_metrics_modulo_legacy"

$CLICKHOUSE_CLIENT --query="select sum(1) as parts, sum(marks_count) as marks from system.cnch_parts where database = 't40112' and table = 't40112_read_metrics' and active format JSONEachRow"

uuid=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 6; echo)

# case 1: bitmap index
# ( $CLICKHOUSE_CLIENT --query="explain pipeline select sum(events) from t40112.t40112_read_metrics where date = '2019-01-02' and uid = 1 and arraySetCheck(int_vid, [1]) settings enable_optimizer = 1, enable_ab_index_optimization = 1, optimizer_projection_support = 0" | grep -iq "parts from bitmap index" ) || echo "bitmap index not work"
$CLICKHOUSE_CLIENT --query_id "test40112_case1_bitmap_index_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where date = '2019-01-02' and uid = 1 and arraySetCheck(int_vid, [1]) settings enable_optimizer = 1, enable_ab_index_optimization = 1, optimizer_projection_support = 0 FORMAT Null"

# case 1: normal read
$CLICKHOUSE_CLIENT --query_id "test40112_case1_normal_read_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where date = '2019-01-02' and uid = 1 and arraySetCheck(int_vid, [1]) settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0 FORMAT Null"

# case 2: projection
# ( $CLICKHOUSE_CLIENT --query="explain pipeline select sum(events) from t40112.t40112_read_metrics where date = '2019-01-02' and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 1" | grep -iq "parts from projection" ) || echo "projection not work"
# $CLICKHOUSE_CLIENT --query_id "test40112_case2_projection_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where date = '2019-01-02' and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 1 FORMAT Null"

# case 2: normal read
$CLICKHOUSE_CLIENT --query_id "test40112_case2_normal_read_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where date = '2019-01-02' and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0 FORMAT Null"

# case 3: bitmap index
# ( $CLICKHOUSE_CLIENT --query="explain pipeline select sum(events) from t40112.t40112_read_metrics where date in ('2019-01-02', '2018-12-30', '2018-12-31') and uid = 1 and arraySetCheck(int_vid, [1]) settings enable_optimizer = 1, enable_ab_index_optimization = 1, optimizer_projection_support = 0" | grep -iq "parts from bitmap index" ) || echo "bitmap index not work"
$CLICKHOUSE_CLIENT --query_id "test40112_case3_bitmap_index_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where date in ('2019-01-02', '2018-12-30', '2018-12-31') and uid = 1 and arraySetCheck(int_vid, [1]) settings enable_optimizer = 1, enable_ab_index_optimization = 1, optimizer_projection_support = 0 FORMAT Null"

# case 3: normal read
$CLICKHOUSE_CLIENT --query_id "test40112_case3_normal_read_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where date in ('2019-01-02', '2018-12-30', '2018-12-31') and uid = 1 and arraySetCheck(int_vid, [1]) settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0 FORMAT Null"

# case 4: projection
# ( $CLICKHOUSE_CLIENT --query="explain pipeline select sum(events) from t40112.t40112_read_metrics where date in ('2019-01-02', '2018-12-30', '2018-12-31') and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 1" | grep -iq "parts from projection" ) || echo "projection not work"
# $CLICKHOUSE_CLIENT --query_id "test40112_case4_projection_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where date in ('2019-01-02', '2018-12-30', '2018-12-31') and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 1 FORMAT Null"

# case 4: normal read
$CLICKHOUSE_CLIENT --query_id "test40112_case4_normal_read_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where date in ('2019-01-02', '2018-12-30', '2018-12-31') and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0 FORMAT Null"

# case 5: multiIf optimize enabled
( $CLICKHOUSE_CLIENT --query="explain select sum(events) from t40112.t40112_read_metrics where multiIf(date < '2018-12-31', 0, date > '2019-01-01', 0, 1) and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0, allow_multi_if_const_optimize = 1" | grep -iq "Partition filter: multiIf" ) || echo "case 5.1 unexpected plan"
$CLICKHOUSE_CLIENT --query_id "test40112_case5_multiIf_optimize_enabled_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where multiIf(date < '2018-12-31', 0, date > '2019-01-01', 0, 1) and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0, allow_multi_if_const_optimize = 1 FORMAT Null"

# case 5: multiIf optimize disabled
( $CLICKHOUSE_CLIENT --query="explain select sum(events) from t40112.t40112_read_metrics where multiIf(date < '2018-12-31', 0, date > '2019-01-01', 0, 1) and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0, allow_multi_if_const_optimize = 0" | grep -iq "Partition filter: multiIf" ) && echo "case 5.2 unexpected plan"
$CLICKHOUSE_CLIENT --query_id "test40112_case5_multiIf_optimize_disabled_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics where multiIf(date < '2018-12-31', 0, date > '2019-01-01', 0, 1) and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0, allow_multi_if_const_optimize = 0 FORMAT Null"

# case 6: complex partition key, normal read
$CLICKHOUSE_CLIENT --query_id "test40112_case6_normal_read_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics_complex_partition_key where toStartOfYear(date) = '2019-01-01' and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0 FORMAT Null"

# case 7: modulo legacy, normal read
$CLICKHOUSE_CLIENT --query_id "test40112_case7_normal_read_${uuid}" --query="select sum(events) from t40112.t40112_read_metrics_modulo_legacy where date_id % 100 = 1 and uid = 1 settings enable_optimizer = 1, enable_ab_index_optimization = 0, optimizer_projection_support = 0 FORMAT Null"

$CLICKHOUSE_CLIENT --query="system flush logs"
sleep 30 # wait for workers flushing logs

print_read_metrics ()
{
    $CLICKHOUSE_CLIENT --query="SELECT any(left(initial_query_id, -7)), sum(ProfileEvents{'TotalPartitions'}), sum(ProfileEvents{'PrunedPartitions'}), sum(ProfileEvents{'SelectedParts'}) FROM cnch(server, system.query_log) WHERE initial_query_id = '$1'"
    $CLICKHOUSE_CLIENT --query="SELECT any(left(initial_query_id, -7)), sum(ProfileEvents{'SelectedMarks'}), sum(ProfileEvents{'SelectedRanges'}) FROM cnch(worker, system.query_log) WHERE initial_query_id = '$1'"
}

print_read_metrics "test40112_case1_bitmap_index_${uuid}"

print_read_metrics "test40112_case1_normal_read_${uuid}"

print_read_metrics "test40112_case2_projection_${uuid}"

print_read_metrics "test40112_case2_normal_read_${uuid}"

print_read_metrics "test40112_case3_bitmap_index_${uuid}"

print_read_metrics "test40112_case3_normal_read_${uuid}"

print_read_metrics "test40112_case4_projection_${uuid}"

print_read_metrics "test40112_case4_normal_read_${uuid}"

print_read_metrics "test40112_case5_multiIf_optimize_enabled_${uuid}"

# this test is unstable for multi-server, since setting `allow_multi_if_const_optimize = 0` is not passed to the host server when fetch partition remotely
# print_read_metrics "test40112_case5_multiIf_optimize_disabled_${uuid}"

print_read_metrics "test40112_case6_normal_read_${uuid}"

print_read_metrics "test40112_case7_normal_read_${uuid}"

$CLICKHOUSE_CLIENT --query="drop table if exists t40112.t40112_read_metrics"
$CLICKHOUSE_CLIENT --query="drop table if exists t40112.t40112_read_metrics_complex_partition_key"
$CLICKHOUSE_CLIENT --query="drop table if exists t40112.t40112_read_metrics_modulo_legacy"
