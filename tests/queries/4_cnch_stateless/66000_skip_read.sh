#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function wait_for_log_flush() {
    query_id=${1}
    expect_log_num=${2}

    while true
    do
        current_log_num=`${CLICKHOUSE_CLIENT} --query "SELECT COUNT() FROM cnch(vw_default, system.query_log) WHERE initial_query_id = '${query_id}' and type > 1"`

        if [ "${current_log_num}" -ge "${expect_log_num}" ]
        then
            return 0
        fi

        sleep 1
    done
}

QUERY_ID_PREFIX=`cat /proc/sys/kernel/random/uuid`
WORKER_COUNT=`${CLICKHOUSE_CLIENT} --query "SELECT COUNT() FROM system.workers WHERE vw_name = 'vw_default'"`

${CLICKHOUSE_CLIENT} --multiquery <<EOF
select 'Test skip read for merge tree table';

drop table if exists test_skip_read sync;
create table test_skip_read(key Int, value String, index val_ivt value type inverted('char_sep', '{"seperators":", "}') granularity 1) engine = CnchMergeTree order by key settings index_granularity = 5, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

insert into test_skip_read select number, concat(toString(number), ',', toString(number + 1)) from system.numbers limit 16;
EOF

${CLICKHOUSE_CLIENT} -q "select 'Without inverted index: ';"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_merge_tree_without_inverted" -q "select * from test_skip_read where hasTokenBySeperator(value, '7', ', ') settings filtered_ratio_to_use_skip_read = 0, log_queries = 1, enable_inverted_index=0, enable_optimizer = 0;"
wait_for_log_flush "${QUERY_ID_PREFIX}_merge_tree_without_inverted" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_merge_tree_without_inverted' and type > 1;"

${CLICKHOUSE_CLIENT} -q "select 'Without skip read: ';"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_merge_tree_without_skip" -q "select * from test_skip_read where hasTokenBySeperator(value, '7', ', ') settings filtered_ratio_to_use_skip_read = 0, log_queries = 1, enable_optimizer = 0;"
wait_for_log_flush "${QUERY_ID_PREFIX}_merge_tree_without_skip" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_merge_tree_without_skip' and type > 1;"

${CLICKHOUSE_CLIENT} -q "select 'With skip read: ';"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_merge_tree_with_skip" -q "select * from test_skip_read where hasTokenBySeperator(value, '7', ', ') settings filtered_ratio_to_use_skip_read = 1, log_queries = 1, enable_optimizer = 0;"
wait_for_log_flush "${QUERY_ID_PREFIX}_merge_tree_with_skip" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_merge_tree_with_skip' and type > 1;"


${CLICKHOUSE_CLIENT} --multiquery <<EOF
select 'Test skip read for unique merge tree';

drop table if exists test_skip_read_unique sync;
create table test_skip_read_unique(key Int, value String, index val_ivt value type inverted('char_sep', '{"seperators":", "}') granularity 1) engine = CnchMergeTree() order by key unique key sipHash64(value) settings index_granularity = 5, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

insert into test_skip_read_unique select number, concat(toString(number), ',', toString(number + 1)) from system.numbers limit 16;
EOF

${CLICKHOUSE_CLIENT} -q "select 'Without inverted index: ';"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_unique_tree_without_inverted" -q "select * from test_skip_read_unique where hasTokenBySeperator(value, '7', ', ') settings filtered_ratio_to_use_skip_read = 0, log_queries = 1, enable_inverted_index=0, enable_optimizer = 0;"
wait_for_log_flush "${QUERY_ID_PREFIX}_unique_tree_without_inverted" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_unique_tree_without_inverted' and type > 1;"

${CLICKHOUSE_CLIENT} -q "select 'Without skip read: ';"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_unique_tree_without_skip" -q "select * from test_skip_read_unique where hasTokenBySeperator(value, '7', ', ') settings filtered_ratio_to_use_skip_read = 0, log_queries = 1, enable_optimizer = 0;"
wait_for_log_flush "${QUERY_ID_PREFIX}_unique_tree_without_skip" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_unique_tree_without_skip' and type > 1;"

${CLICKHOUSE_CLIENT} -q "select 'With skip read: ';"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_unique_tree_with_skip" -q "select * from test_skip_read_unique where hasTokenBySeperator(value, '7', ', ') settings filtered_ratio_to_use_skip_read = 1, log_queries = 1, enable_optimizer = 0;"
wait_for_log_flush "${QUERY_ID_PREFIX}_unique_tree_with_skip" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_unique_tree_with_skip' and type > 1;"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
select 'Test skip read for map column';

drop table if exists test_skip_read_map sync;
create table test_skip_read_map (value String, str_map Map(String, String), index val_ivt value type inverted('char_sep', '{"seperators":","}') granularity 1) engine = CnchMergeTree order by tuple() settings index_granularity = 5, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

select 'Test read empty map';

insert into test_skip_read_map select toString(number % 2), map() from system.numbers limit 16;

select * from test_skip_read_map prewhere hasTokenBySeperator(value, '0', ',') settings filtered_ratio_to_use_skip_read = 1;

truncate table test_skip_read_map sync;

select 'Test read map';

insert into test_skip_read_map select toString(number % 2), map(number % 5, number + 1) from system.numbers limit 16;

select * from test_skip_read_map where hasTokenBySeperator(value, '0', ',') settings filtered_ratio_to_use_skip_read = 1;
EOF

${CLICKHOUSE_CLIENT} --multiquery <<EOF
select 'Test skip read for array column';

drop table if exists test_skip_read_array sync;
create table test_skip_read_array (value String, int_arr Array(UInt32), index val_ivt value type inverted('char_sep', '{"seperators":","}') granularity 1) engine = CnchMergeTree order by tuple() settings index_granularity = 5, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

insert into test_skip_read_array select toString(number % 2), arrayWithConstant(number, number) from system.numbers limit 16;

select * from test_skip_read_array prewhere hasTokenBySeperator(value, '0', ',') settings filtered_ratio_to_use_skip_read = 1;
EOF

${CLICKHOUSE_CLIENT} --multiquery <<EOF
select 'Test skip read with virtual column';

drop table if exists test_skip_read_virt_col sync;
create table test_skip_read_virt_col (value String, index value_ivt value type inverted granularity 1) engine = CnchMergeTree order by tuple() settings index_granularity = 5, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

insert into test_skip_read_virt_col select toString(number % 2) from system.numbers limit 16;

select value, _part_offset, _part_row_number from test_skip_read_virt_col prewhere value = '0' settings filtered_ratio_to_use_skip_read = 1;
EOF

${CLICKHOUSE_CLIENT} --multiquery <<EOF
select 'Test skip read with defaults';

drop table if exists test_skip_read_fill_default sync;
create table test_skip_read_fill_default (value String, str_map Map(String, String), index value_ivt value type inverted granularity 1) engine = CnchMergeTree order by tuple() settings index_granularity = 5, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

insert into test_skip_read_fill_default select toString(number % 2), map(number % 2, number % 2) from system.numbers limit 16;

select str_map, str_map{'0'}, str_map{'2'} from test_skip_read_fill_default where hasTokenBySeperator(value, '0', ',') settings filtered_ratio_to_use_skip_read = 1;

EOF

${CLICKHOUSE_CLIENT} --multiquery <<EOF
select 'Test skip read with evaluate defaults';

drop table if exists test_skip_read_evaluate_default sync;
create table test_skip_read_evaluate_default (value String, index value_ivt value type inverted granularity 1) engine = CnchMergeTree order by tuple() settings index_granularity = 5, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

insert into test_skip_read_evaluate_default select toString(number % 2) from system.numbers limit 16 settings insert_null_as_default = 1;

alter table test_skip_read_evaluate_default add column dft String default concat(value, ',');

select dft from test_skip_read_evaluate_default where hasTokenBySeperator(value, '0', ',') settings filtered_ratio_to_use_skip_read = 1;

EOF
