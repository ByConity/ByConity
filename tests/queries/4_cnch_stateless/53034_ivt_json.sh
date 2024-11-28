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

echo "TEST 1"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS test.test_inverted_with_json;

set allow_experimental_object_type = 1;

CREATE TABLE test.test_inverted_with_json
(
    key UInt64,
    doc JSON,
    INDEX doc_idx doc TYPE inverted('token','{"subkey":["key1","key2","key3"]}') GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key 
SETTINGS index_granularity = 1;

INSERT INTO test.test_inverted_with_json VALUES (0, '{"key1": "value10", "key2": "value20", "key3": "value30"}'),(1, '{"key1": "value11", "key2": "value21", "key3": "value31"}'),(2, '{"key1": "value12", "key2": "value22", "key3": "value32"}'), (3, '{"key1": "value13", "key2": "value23", "key3": "value33"}'),(4, '{"key1": "value14", "key2": "value24", "key3": "value34"}');
EOF

echo "Equal"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_eq" -q "select * from test.test_inverted_with_json where get_json_object(doc, '$.key3') = 'value31' SETTINGS enable_optimizer = 1,block_json_query_in_optimizer = 0, optimize_json_function_to_subcolumn=1"

echo "hasToken"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_has_token" -q "select * from test.test_inverted_with_json where hasToken(get_json_object(doc, '$.key1'), 'value10') SETTINGS enable_optimizer = 1,block_json_query_in_optimizer = 0, optimize_json_function_to_subcolumn=1"

echo "Check info for Equal"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq' and type > 1;"

echo "Check info for hasToken"
wait_for_log_flush "${QUERY_ID_PREFIX}_has_token" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_has_token' and type > 1;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test.test_inverted_with_json"


echo "TEST 2"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
set allow_experimental_object_type = 1;

DROP TABLE IF EXISTS test.test_inverted_with_json_all;

CREATE TABLE test.test_inverted_with_json_all
(
    key UInt64,
    doc Object('json'),
    INDEX doc_idx doc TYPE inverted('token','{"subkey":["*"]}') GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key
SETTINGS index_granularity = 1;

INSERT INTO test.test_inverted_with_json_all VALUES (0, '{"key1": "value10", "key2": "value20", "key3": "value30"}'),(1, '{"key1": "value11", "key2": "value21", "key3": "value31"}'),(2, '{"key1": "value12", "key2": "value22", "key3": "value32"}'), (3, '{"key1": "value13", "key2": "value23", "key3": "value33"}'),(4, '{"key1": "value14", "key2": "value24", "key3": "value34"}');
EOF

echo "Equal"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_eq_all" -q "select * from test.test_inverted_with_json_all where get_json_object(doc, '$.key3') = 'value31' SETTINGS enable_optimizer = 1,block_json_query_in_optimizer = 0, optimize_json_function_to_subcolumn=1"

echo "hasToken"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_has_token_all" -q "select * from test.test_inverted_with_json_all where hasToken(get_json_object(doc, '$.key1'), 'value10') SETTINGS enable_optimizer = 1,block_json_query_in_optimizer = 0, optimize_json_function_to_subcolumn=1"

echo "Check info for Equal"
wait_for_log_flush "${QUERY_ID_PREFIX}_eq_all" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_eq_all' and type > 1;"

echo "Check info for hasToken"
wait_for_log_flush "${QUERY_ID_PREFIX}_has_token_all" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_has_token_all' and type > 1;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test.test_inverted_with_json_all"

echo "TEST 3"

${CLICKHOUSE_CLIENT} --multiquery <<EOF

set allow_experimental_object_type = 1;

DROP TABLE IF EXISTS test.test_inverted_with_json_complex;

CREATE TABLE test.test_inverted_with_json_complex
(
    key UInt64,
    doc JSON,
    INDEX doc_idx doc TYPE inverted('token','{"subkey":["*"]}') GRANULARITY 1
)
ENGINE = CnchMergeTree()
ORDER BY key 
SETTINGS index_granularity = 1;

INSERT INTO test.test_inverted_with_json_complex VALUES (0, '{"key": [{"key2": "value0", "key3": "value0", "key1": "value0"}, {"key2": "value0", "key3": "value0", "key1": "value0"}]}');
INSERT INTO test.test_inverted_with_json_complex VALUES (1, '{"key": [{"key2": "value10", "key3": "value10", "key1": "value10"}, {"key2": "value10", "key3": "value10", "key1": "value10"}]}');
EOF


echo "has0"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_has_0" -q "select * from test.test_inverted_with_json_complex where has(doc.key.key1,'value0') SETTINGS enable_optimizer = 0"

echo "has10"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_has_10" -q "select * from test.test_inverted_with_json_complex where has(doc.key.key1,'value10') SETTINGS enable_optimizer = 0"

echo "Check info for has0"
wait_for_log_flush "${QUERY_ID_PREFIX}_has_0" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_has_0' and type > 1;"

echo "Check info for has10"
wait_for_log_flush "${QUERY_ID_PREFIX}_has_10" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_has_10' and type > 1;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test.test_inverted_with_json_complex"


echo "TEST 4 with complex json file"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
set allow_experimental_object_type = 1;

DROP TABLE IF EXISTS test.test_inverted_with_json_all;

CREATE TABLE test.test_inverted_with_json_all
(
    key UInt64,
    doc Object('json'),
    INDEX doc_idx doc TYPE inverted('token', '{"subkey":["*"]}') GRANULARITY 1
)
ENGINE = CnchMergeTree
ORDER BY key
SETTINGS index_granularity = 1;
EOF

echo \
'{"key": 0, "doc": {"key": [{"key1": ["value11_1_0", "value11_2_0", "value11_3_0"], "key2": "value120", "key3": "value130", "key4": [{"key1": ["value4111_1_0", "value4111_2_0", "value4111_3_0"], "key2": "value41210", "key3": "value41310"}, {"key1": ["value4112_1_0", "value4112_2_0", "value4112_3_0"], "key2": "value41220", "key3": "value41320"}]}, {"key1": ["value21_1_0", "value21_2_0", "value21_3_0"], "key2": "value22_0", "key3": "value23_0", "key4": [{"key1": ["value4213_1_0", "value4213_2_0", "value4213_3_0"], "key2": "value4223_0", "key3": "value4233_0"}]}]}}
{"key": 1, "doc": {"key": [{"key1": ["value11_1_1", "value11_2_1", "value11_3_1"], "key2": "value121", "key3": "value131", "key4": [{"key1": ["value4111_1_1", "value4111_2_1", "value4111_3_1"], "key2": "value41211", "key3": "value41311"}, {"key1": ["value4112_1_1", "value4112_2_1", "value4112_3_1"], "key2": "value41221", "key3": "value41321"}]}, {"key1": ["value21_1_1", "value21_2_1", "value21_3_1"], "key2": "value22_1", "key3": "value23_1", "key4": [{"key1": ["value4213_1_1", "value4213_2_1", "value4213_3_1"], "key2": "value4223_1", "key3": "value4233_1"}]}]}}
{"key": 2, "doc": {"key": [{"key1": ["value11_1_2", "value11_2_2", "value11_3_2"], "key2": "value122", "key3": "value132", "key4": [{"key1": ["value4111_1_2", "value4111_2_2", "value4111_3_2"], "key2": "value41212", "key3": "value41312"}, {"key1": ["value4112_1_2", "value4112_2_2", "value4112_3_2"], "key2": "value41222", "key3": "value41322"}]}, {"key1": ["value21_1_2", "value21_2_2", "value21_3_2"], "key2": "value22_2", "key3": "value23_2", "key4": [{"key1": ["value4213_1_2", "value4213_2_2", "value4213_3_2"], "key2": "value4223_2", "key3": "value4233_2"}]}]}}
{"key": 3, "doc": {"key": [{"key1": ["value11_1_3", "value11_2_3", "value11_3_3"], "key2": "value123", "key3": "value133", "key4": [{"key1": ["value4111_1_3", "value4111_2_3", "value4111_3_3"], "key2": "value41213", "key3": "value41313"}, {"key1": ["value4112_1_3", "value4112_2_3", "value4112_3_3"], "key2": "value41223", "key3": "value41323"}]}, {"key1": ["value21_1_3", "value21_2_3", "value21_3_3"], "key2": "value22_3", "key3": "value23_3", "key4": [{"key1": ["value4213_1_3", "value4213_2_3", "value4213_3_3"], "key2": "value4223_3", "key3": "value4233_3"}]}]}}
{"key": 4, "doc": {"key": [{"key1": ["value11_1_4", "value11_2_4", "value11_3_4"], "key2": "value124", "key3": "value134", "key4": [{"key1": ["value4111_1_4", "value4111_2_4", "value4111_3_4"], "key2": "value41214", "key3": "value41314"}, {"key1": ["value4112_1_4", "value4112_2_4", "value4112_3_4"], "key2": "value41224", "key3": "value41324"}]}, {"key1": ["value21_1_4", "value21_2_4", "value21_3_4"], "key2": "value22_4", "key3": "value23_4", "key4": [{"key1": ["value4213_1_4", "value4213_2_4", "value4213_3_4"], "key2": "value4223_4", "key3": "value4233_4"}]}]}}
{"key": 5, "doc": {"key": [{"key1": ["value11_1_5", "value11_2_5", "value11_3_5"], "key2": "value125", "key3": "value135", "key4": [{"key1": ["value4111_1_5", "value4111_2_5", "value4111_3_5"], "key2": "value41215", "key3": "value41315"}, {"key1": ["value4112_1_5", "value4112_2_5", "value4112_3_5"], "key2": "value41225", "key3": "value41325"}]}, {"key1": ["value21_1_5", "value21_2_5", "value21_3_5"], "key2": "value22_5", "key3": "value23_5", "key4": [{"key1": ["value4213_1_5", "value4213_2_5", "value4213_3_5"], "key2": "value4223_5", "key3": "value4233_5"}]}]}}
{"key": 6, "doc": {"key": [{"key1": ["value11_1_6", "value11_2_6", "value11_3_6"], "key2": "value126", "key3": "value136", "key4": [{"key1": ["value4111_1_6", "value4111_2_6", "value4111_3_6"], "key2": "value41216", "key3": "value41316"}, {"key1": ["value4112_1_6", "value4112_2_6", "value4112_3_6"], "key2": "value41226", "key3": "value41326"}]}, {"key1": ["value21_1_6", "value21_2_6", "value21_3_6"], "key2": "value22_6", "key3": "value23_6", "key4": [{"key1": ["value4213_1_6", "value4213_2_6", "value4213_3_6"], "key2": "value4223_6", "key3": "value4233_6"}]}]}}
{"key": 7, "doc": {"key": [{"key1": ["value11_1_7", "value11_2_7", "value11_3_7"], "key2": "value127", "key3": "value137", "key4": [{"key1": ["value4111_1_7", "value4111_2_7", "value4111_3_7"], "key2": "value41217", "key3": "value41317"}, {"key1": ["value4112_1_7", "value4112_2_7", "value4112_3_7"], "key2": "value41227", "key3": "value41327"}]}, {"key1": ["value21_1_7", "value21_2_7", "value21_3_7"], "key2": "value22_7", "key3": "value23_7", "key4": [{"key1": ["value4213_1_7", "value4213_2_7", "value4213_3_7"], "key2": "value4223_7", "key3": "value4233_7"}]}]}}
{"key": 8, "doc": {"key": [{"key1": ["value11_1_8", "value11_2_8", "value11_3_8"], "key2": "value128", "key3": "value138", "key4": [{"key1": ["value4111_1_8", "value4111_2_8", "value4111_3_8"], "key2": "value41218", "key3": "value41318"}, {"key1": ["value4112_1_8", "value4112_2_8", "value4112_3_8"], "key2": "value41228", "key3": "value41328"}]}, {"key1": ["value21_1_8", "value21_2_8", "value21_3_8"], "key2": "value22_8", "key3": "value23_8", "key4": [{"key1": ["value4213_1_8", "value4213_2_8", "value4213_3_8"], "key2": "value4223_8", "key3": "value4233_8"}]}]}}
{"key": 9, "doc": {"key": [{"key1": ["value11_1_9", "value11_2_9", "value11_3_9"], "key2": "value129", "key3": "value139", "key4": [{"key1": ["value4111_1_9", "value4111_2_9", "value4111_3_9"], "key2": "value41219", "key3": "value41319"}, {"key1": ["value4112_1_9", "value4112_2_9", "value4112_3_9"], "key2": "value41229", "key3": "value41329"}]}, {"key1": ["value21_1_9", "value21_2_9", "value21_3_9"], "key2": "value22_9", "key3": "value23_9", "key4": [{"key1": ["value4213_1_9", "value4213_2_9", "value4213_3_9"], "key2": "value4223_9", "key3": "value4233_9"}]}]}}' \
| ${CLICKHOUSE_CLIENT} -q "INSERT INTO test.test_inverted_with_json_all FORMAT JSONEachRow"


echo "complex_all_has"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_complex_all_has" -q "SELECT * FROM test.test_inverted_with_json_all WHERE has(doc.key.key3, 'value130') SETTINGS enable_optimizer = 0"

echo "Check info for complex_all_has"
wait_for_log_flush "${QUERY_ID_PREFIX}_complex_all_has" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_complex_all_has' and type > 1;"


echo "TEST 5 with complex json file"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
set allow_experimental_object_type = 1;

DROP TABLE IF EXISTS test.test_inverted_with_json_key;

CREATE TABLE test.test_inverted_with_json_key
(
    key UInt64,
    doc Object('json'),
    INDEX doc_idx doc TYPE inverted('token', '{"subkey":["key.key1", "key.key2","key.key4"]}') GRANULARITY 1
)
ENGINE = CnchMergeTree
ORDER BY key
SETTINGS index_granularity = 1;
EOF

echo \
'{"key": 0, "doc": {"key": [{"key1": ["value11_1_0", "value11_2_0", "value11_3_0"], "key2": "value120", "key3": "value130", "key4": [{"key1": ["value4111_1_0", "value4111_2_0", "value4111_3_0"], "key2": "value41210", "key3": "value41310"}, {"key1": ["value4112_1_0", "value4112_2_0", "value4112_3_0"], "key2": "value41220", "key3": "value41320"}]}, {"key1": ["value21_1_0", "value21_2_0", "value21_3_0"], "key2": "value22_0", "key3": "value23_0", "key4": [{"key1": ["value4213_1_0", "value4213_2_0", "value4213_3_0"], "key2": "value4223_0", "key3": "value4233_0"}]}]}}
{"key": 1, "doc": {"key": [{"key1": ["value11_1_1", "value11_2_1", "value11_3_1"], "key2": "value121", "key3": "value131", "key4": [{"key1": ["value4111_1_1", "value4111_2_1", "value4111_3_1"], "key2": "value41211", "key3": "value41311"}, {"key1": ["value4112_1_1", "value4112_2_1", "value4112_3_1"], "key2": "value41221", "key3": "value41321"}]}, {"key1": ["value21_1_1", "value21_2_1", "value21_3_1"], "key2": "value22_1", "key3": "value23_1", "key4": [{"key1": ["value4213_1_1", "value4213_2_1", "value4213_3_1"], "key2": "value4223_1", "key3": "value4233_1"}]}]}}
{"key": 2, "doc": {"key": [{"key1": ["value11_1_2", "value11_2_2", "value11_3_2"], "key2": "value122", "key3": "value132", "key4": [{"key1": ["value4111_1_2", "value4111_2_2", "value4111_3_2"], "key2": "value41212", "key3": "value41312"}, {"key1": ["value4112_1_2", "value4112_2_2", "value4112_3_2"], "key2": "value41222", "key3": "value41322"}]}, {"key1": ["value21_1_2", "value21_2_2", "value21_3_2"], "key2": "value22_2", "key3": "value23_2", "key4": [{"key1": ["value4213_1_2", "value4213_2_2", "value4213_3_2"], "key2": "value4223_2", "key3": "value4233_2"}]}]}}
{"key": 3, "doc": {"key": [{"key1": ["value11_1_3", "value11_2_3", "value11_3_3"], "key2": "value123", "key3": "value133", "key4": [{"key1": ["value4111_1_3", "value4111_2_3", "value4111_3_3"], "key2": "value41213", "key3": "value41313"}, {"key1": ["value4112_1_3", "value4112_2_3", "value4112_3_3"], "key2": "value41223", "key3": "value41323"}]}, {"key1": ["value21_1_3", "value21_2_3", "value21_3_3"], "key2": "value22_3", "key3": "value23_3", "key4": [{"key1": ["value4213_1_3", "value4213_2_3", "value4213_3_3"], "key2": "value4223_3", "key3": "value4233_3"}]}]}}
{"key": 4, "doc": {"key": [{"key1": ["value11_1_4", "value11_2_4", "value11_3_4"], "key2": "value124", "key3": "value134", "key4": [{"key1": ["value4111_1_4", "value4111_2_4", "value4111_3_4"], "key2": "value41214", "key3": "value41314"}, {"key1": ["value4112_1_4", "value4112_2_4", "value4112_3_4"], "key2": "value41224", "key3": "value41324"}]}, {"key1": ["value21_1_4", "value21_2_4", "value21_3_4"], "key2": "value22_4", "key3": "value23_4", "key4": [{"key1": ["value4213_1_4", "value4213_2_4", "value4213_3_4"], "key2": "value4223_4", "key3": "value4233_4"}]}]}}
{"key": 5, "doc": {"key": [{"key1": ["value11_1_5", "value11_2_5", "value11_3_5"], "key2": "value125", "key3": "value135", "key4": [{"key1": ["value4111_1_5", "value4111_2_5", "value4111_3_5"], "key2": "value41215", "key3": "value41315"}, {"key1": ["value4112_1_5", "value4112_2_5", "value4112_3_5"], "key2": "value41225", "key3": "value41325"}]}, {"key1": ["value21_1_5", "value21_2_5", "value21_3_5"], "key2": "value22_5", "key3": "value23_5", "key4": [{"key1": ["value4213_1_5", "value4213_2_5", "value4213_3_5"], "key2": "value4223_5", "key3": "value4233_5"}]}]}}
{"key": 6, "doc": {"key": [{"key1": ["value11_1_6", "value11_2_6", "value11_3_6"], "key2": "value126", "key3": "value136", "key4": [{"key1": ["value4111_1_6", "value4111_2_6", "value4111_3_6"], "key2": "value41216", "key3": "value41316"}, {"key1": ["value4112_1_6", "value4112_2_6", "value4112_3_6"], "key2": "value41226", "key3": "value41326"}]}, {"key1": ["value21_1_6", "value21_2_6", "value21_3_6"], "key2": "value22_6", "key3": "value23_6", "key4": [{"key1": ["value4213_1_6", "value4213_2_6", "value4213_3_6"], "key2": "value4223_6", "key3": "value4233_6"}]}]}}
{"key": 7, "doc": {"key": [{"key1": ["value11_1_7", "value11_2_7", "value11_3_7"], "key2": "value127", "key3": "value137", "key4": [{"key1": ["value4111_1_7", "value4111_2_7", "value4111_3_7"], "key2": "value41217", "key3": "value41317"}, {"key1": ["value4112_1_7", "value4112_2_7", "value4112_3_7"], "key2": "value41227", "key3": "value41327"}]}, {"key1": ["value21_1_7", "value21_2_7", "value21_3_7"], "key2": "value22_7", "key3": "value23_7", "key4": [{"key1": ["value4213_1_7", "value4213_2_7", "value4213_3_7"], "key2": "value4223_7", "key3": "value4233_7"}]}]}}
{"key": 8, "doc": {"key": [{"key1": ["value11_1_8", "value11_2_8", "value11_3_8"], "key2": "value128", "key3": "value138", "key4": [{"key1": ["value4111_1_8", "value4111_2_8", "value4111_3_8"], "key2": "value41218", "key3": "value41318"}, {"key1": ["value4112_1_8", "value4112_2_8", "value4112_3_8"], "key2": "value41228", "key3": "value41328"}]}, {"key1": ["value21_1_8", "value21_2_8", "value21_3_8"], "key2": "value22_8", "key3": "value23_8", "key4": [{"key1": ["value4213_1_8", "value4213_2_8", "value4213_3_8"], "key2": "value4223_8", "key3": "value4233_8"}]}]}}
{"key": 9, "doc": {"key": [{"key1": ["value11_1_9", "value11_2_9", "value11_3_9"], "key2": "value129", "key3": "value139", "key4": [{"key1": ["value4111_1_9", "value4111_2_9", "value4111_3_9"], "key2": "value41219", "key3": "value41319"}, {"key1": ["value4112_1_9", "value4112_2_9", "value4112_3_9"], "key2": "value41229", "key3": "value41329"}]}, {"key1": ["value21_1_9", "value21_2_9", "value21_3_9"], "key2": "value22_9", "key3": "value23_9", "key4": [{"key1": ["value4213_1_9", "value4213_2_9", "value4213_3_9"], "key2": "value4223_9", "key3": "value4233_9"}]}]}}' \
| ${CLICKHOUSE_CLIENT} -q "INSERT INTO test.test_inverted_with_json_key FORMAT JSONEachRow"


echo "complex_key_has_without_index"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_complex_key_has_without_index" -q "SELECT * FROM test.test_inverted_with_json_key WHERE has(doc.key.key3, 'value130') SETTINGS enable_optimizer = 0"

echo "Check info for complex_key_has_without_index"
wait_for_log_flush "${QUERY_ID_PREFIX}_complex_key_has_without_index" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_complex_key_has_without_index' and type > 1;"


echo "complex_key_has_with_index"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_complex_key_has_with_index" -q "SELECT * FROM test.test_inverted_with_json_key WHERE has(doc.key.key2, 'value120') SETTINGS enable_optimizer = 0"

echo "Check info for complex_key_has_with_index"
wait_for_log_flush "${QUERY_ID_PREFIX}_complex_key_has_with_index" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_complex_key_has_with_index' and type > 1;"


echo "TEST 6 with complex json file"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
set allow_experimental_object_type = 1;

DROP TABLE IF EXISTS test.test_inverted_complex_json;

CREATE TABLE test.test_inverted_complex_json
(
    key UInt64,
    doc Object('json'),
    INDEX doc_idx doc TYPE inverted('standard', '{"subkey":["*"]}') GRANULARITY 1
)
ENGINE = CnchMergeTree
ORDER BY key
SETTINGS index_granularity = 1;
EOF

echo \
'{"key": 0, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value10"}}}}}}, "key2": "value20", "key3": "value30"}}
{"key": 1, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value11"}}}}}}, "key2": "value21", "key3": "value31"}}
{"key": 2, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value12"}}}}}}, "key2": "value22", "key3": "value32"}}
{"key": 3, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value13"}}}}}}, "key2": "value23", "key3": "value33"}}
{"key": 4, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value14"}}}}}}, "key2": "value24", "key3": "value34"}}
{"key": 5, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value15"}}}}}}, "key2": "value25", "key3": "value35"}}
{"key": 6, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value16"}}}}}}, "key2": "value26", "key3": "value36"}}
{"key": 7, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value17"}}}}}}, "key2": "value27", "key3": "value37"}}
{"key": 8, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value18"}}}}}}, "key2": "value28", "key3": "value38"}}
{"key": 9, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value19"}}}}}}, "key2": "value29", "key3": "value39"}}' \
| ${CLICKHOUSE_CLIENT} -q "INSERT INTO test.test_inverted_complex_json FORMAT JSONEachRow"

echo "check hasTokens"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_complex_all_has_tokens_index" -q "SELECT * FROM test.test_inverted_complex_json WHERE hasTokens(doc.key1.key11.key12.key13.key14.key15.key1 , 'value10') SETTINGS enable_optimizer = 0"

echo "Check info for complex_all_has_tokens_index"
wait_for_log_flush "${QUERY_ID_PREFIX}_complex_all_has_tokens_index" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_complex_all_has_tokens_index' and type > 1;"





echo "TEST 7 with complex json file"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
set allow_experimental_object_type = 1;

DROP TABLE IF EXISTS test.test_inverted_key1_complex_json;

CREATE TABLE test.test_inverted_key1_complex_json
(
    key UInt64,
    doc Object('json'),
    INDEX doc_idx doc TYPE inverted('standard', '{"subkey":["key1"]}') GRANULARITY 1
)
ENGINE = CnchMergeTree
ORDER BY key
SETTINGS index_granularity = 1;
EOF

echo \
'{"key": 0, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value10"}}}}}}, "key2": "value20", "key3": "value30"}}
{"key": 1, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value11"}}}}}}, "key2": "value21", "key3": "value31"}}
{"key": 2, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value12"}}}}}}, "key2": "value22", "key3": "value32"}}
{"key": 3, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value13"}}}}}}, "key2": "value23", "key3": "value33"}}
{"key": 4, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value14"}}}}}}, "key2": "value24", "key3": "value34"}}
{"key": 5, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value15"}}}}}}, "key2": "value25", "key3": "value35"}}
{"key": 6, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value16"}}}}}}, "key2": "value26", "key3": "value36"}}
{"key": 7, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value17"}}}}}}, "key2": "value27", "key3": "value37"}}
{"key": 8, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value18"}}}}}}, "key2": "value28", "key3": "value38"}}
{"key": 9, "doc": {"key1": {"key11": {"key12": {"key13": {"key14": {"key15": {"key1": "value19"}}}}}}, "key2": "value29", "key3": "value39"}}' \
| ${CLICKHOUSE_CLIENT} -q "INSERT INTO test.test_inverted_key1_complex_json FORMAT JSONEachRow"

echo "check hasTokens"
${CLICKHOUSE_CLIENT} --query_id  "${QUERY_ID_PREFIX}_test_inverted_key1_complex_json" -q "SELECT * FROM test.test_inverted_key1_complex_json WHERE hasTokens(doc.key1.key11.key12.key13.key14.key15.key1 , 'value10') SETTINGS enable_optimizer = 0"

echo "Check info for test_inverted_key1_complex_json"
wait_for_log_flush "${QUERY_ID_PREFIX}_test_inverted_key1_complex_json" ${WORKER_COUNT}
${CLICKHOUSE_CLIENT} -q "select sum(ProfileEvents['TotalSkippedGranules']), sum(ProfileEvents['TotalGranulesCount']) from cnch(vw_default, system.query_log) where initial_query_id = '${QUERY_ID_PREFIX}_test_inverted_key1_complex_json' and type > 1;"

