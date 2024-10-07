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
drop table if exists multi_idx_triple_col;

create table multi_idx_triple_col(v0 String, v1 String, v2 String, index v0_ivt v0 type inverted() granularity 1, index v1_ivt v1 type inverted() granularity 1, index v2_ivt v2 type inverted() granularity 1) engine = CnchMergeTree order by tuple() settings index_granularity = 3;

insert into multi_idx_triple_col select toString(number), toString(100 + number), toString(200 + number) from system.numbers limit 11;
EOF

echo "Equal"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq" -q "select * from multi_idx_triple_col where v0 = '3' settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Not Equal"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_ne" -q "select * from multi_idx_triple_col where v0 != '3' settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "In"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_in" -q "select * from multi_idx_triple_col where v0 in ['0', '3'] settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Not In"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_not_in" -q "select * from multi_idx_triple_col where v0 not in ['0', '6'] settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "multiSearchAny"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_ms" -q "select * from multi_idx_triple_col where multiSearchAny(v0, ['0', '7']) settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Not multiSearchAny"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_not_ms" -q "select * from multi_idx_triple_col where not multiSearchAny(v0, ['3', '8']) settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal and equal"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_and_eq" -q "select * from multi_idx_triple_col where v0 = '0' and v2 = '202' settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal and in"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_and_in" -q "select * from multi_idx_triple_col where v0 = '7' and v2 in ['202', '206'] settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal and multiSearchAny"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_and_ms" -q "select * from multi_idx_triple_col where v0 = '3' and multiSearchAny(v2, ['204', '206']) settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal and constant"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_and_const" -q "select * from multi_idx_triple_col where v0 = '6' and 1 settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "In and multiSearchAny"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_in_and_ms" -q "select * from multi_idx_triple_col where v0 in ['3', '6'] and multiSearchAny(v2, ['204', '207']) settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "In and constant"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_in_and_const" -q "select * from multi_idx_triple_col where v0 in ['3', '6'] and 1 settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "multiSearchAny and constant"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_ms_and_const" -q "select * from multi_idx_triple_col where multiSearchAny(v0, ['3', '6']) and 1 settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal or equal"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_or_eq" -q "select * from multi_idx_triple_col where v0 = '0' or v2 = '203' settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal or in"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_or_in" -q "select * from multi_idx_triple_col where v0 = '0' or v2 in ['202', '209'] settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal or mulitSearchAny"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_or_ms" -q "select * from multi_idx_triple_col where v0 = '3' or multiSearchAny(v2, ['206', '209']) settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal or constant"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_or_const" -q "select * from multi_idx_triple_col where v0 = '0' or 1 settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "In or multiSearchAny"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_in_or_ms" -q "select * from multi_idx_triple_col where v0 in ['3', '6'] or multiSearchAny(v2, ['204', '209']) settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "In or constant"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_in_or_const" -q "select * from multi_idx_triple_col where v0 in ['3', '9'] or 1 settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "multiSearchAny or constant"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_ms_or_const" -q "select * from multi_idx_triple_col where multiSearchAny(v0, ['0', '3']) or 1 settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal and not equal"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_and_ne" -q "select * from multi_idx_triple_col where v0 = '0' and v2 != '203' settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal or not equal"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_or_ne" -q "select * from multi_idx_triple_col where v0 = '0' or v2 != '206' settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal and not in or mulitSearchAny"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_and_not_in_or_ms" -q "select * from multi_idx_triple_col where v0 = '0' and (v1 not in ['203', '206'] or multiSearchAny(v2, ['206', '207'])) settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

echo "Equal and not multiSearchAny or not equal"
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}_eq_and_not_ms_or_ne" -q "select * from multi_idx_triple_col where v0 = '0' and (not multiSearchAny(v1, ['203', '209']) or v2 != '210') settings enable_optimizer = 0, log_queries = 1, multi_idx_filter_for_ivt = 1"

# Check mark range filter info
echo "Check info for equal"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq' and type > 1;"

echo "Check info for not equal"
wait_for_log_flush "${QUERY_ID_PREFIX}_ne" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_ne' and type > 1;"

echo "Check info for in"
wait_for_log_flush "${QUERY_ID_PREFIX}_in" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_in' and type > 1;"

echo "Check info for not in"
wait_for_log_flush "${QUERY_ID_PREFIX}_not_in" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_not_in' and type > 1;"

echo "Check info for multisearchany"
wait_for_log_flush "${QUERY_ID_PREFIX}_ms" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_ms' and type > 1;"

echo "Check info for not multisearchany"
wait_for_log_flush "${QUERY_ID_PREFIX}_not_ms" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_not_ms' and type > 1;"

echo "Check info for equal and equal"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_and_eq" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_and_eq' and type > 1;"

echo "Check info for equal and in"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_and_in" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_and_in' and type > 1;"

echo "Check info for equal and multisearchany"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_and_ms" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_and_ms' and type > 1;"

echo "Check info for equal and constant"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_and_const" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_and_const' and type > 1;"

echo "Check info for in and multisearchany"
wait_for_log_flush "${QUERY_ID_PREFIX}_in_and_ms" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_in_and_ms' and type > 1;"

echo "Check info for in and constant"
wait_for_log_flush "${QUERY_ID_PREFIX}_in_and_const" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_in_and_const' and type > 1;"

echo "Check info for multisearchany and const"
wait_for_log_flush "${QUERY_ID_PREFIX}_ms_and_const" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_ms_and_const' and type > 1;"

echo "Check info for equal or equal"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_or_eq" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_or_eq' and type > 1;"

echo "Check info for equal or in"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_or_in" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_or_in' and type > 1;"

echo "Check info for equal or mulitsearchany"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_or_ms" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_or_ms' and type > 1;"

echo "Check info for equal or const"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_or_const" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_or_const' and type > 1;"

echo "Check info for in or multisearch"
wait_for_log_flush "${QUERY_ID_PREFIX}_in_or_ms" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_in_or_ms' and type > 1;"

echo "Check info for in or const"
wait_for_log_flush "${QUERY_ID_PREFIX}_in_or_const" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_in_or_const' and type > 1;"

echo "Check info for multisearchany or const"
wait_for_log_flush "${QUERY_ID_PREFIX}_ms_or_const" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_ms_or_const' and type > 1;"

echo "Check info for equal and not equal"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_and_ne" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_and_ne' and type > 1;"

echo "Check info for equal or not equal"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_or_ne" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_or_ne' and type > 1;"

echo "Check info for equal and not in or mulitsearchany"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_and_not_in_or_ms" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_and_not_in_or_ms' and type > 1;"

echo "Check info for equal and not multisearchany or not equal"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_and_not_ms_or_ne" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_and_not_ms_or_ne' and type > 1;"
