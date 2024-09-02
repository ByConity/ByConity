#!/bin/bash

set -e -x -a
TEST_OUTPUT=${TEST_OUTPUT:-all_test_output} 
WORK_DIR=${WORK_DIR:-.}
RUN_SAMPLE=${RUN_SAMPLE:-false}
echo "Start running test on byconity k8s cluster with test output at : $TEST_OUTPUT"
export CLICKHOUSE_PORT_TCP=$(nslookup -type=SRV _PORT0._tcp.byconity-server-headless | head -n 4 | tail -n 1 | awk '{print $6}')
export CLICKHOUSE_PORT_HTTP=$(nslookup -type=SRV _PORT2._tcp.byconity-server-headless | head -n 4 | tail -n 1 | awk '{print $6}')
export CLICKHOUSE_HOST=$(nslookup byconity-server-headless | head -n 5 | tail -n 1 | awk '{print $2}')

export CNCH_WRITE_WORKER_HOST=$(clickhouse client --host $CLICKHOUSE_HOST --port $CLICKHOUSE_PORT_TCP -q "SELECT host FROM system.workers WHERE vw_name='vw_write' LIMIT 1")

export CNCH_WRITE_WORKER_PORT_TCP=$(clickhouse client --host $CLICKHOUSE_HOST --port $CLICKHOUSE_PORT_TCP -q "SELECT tcp_port FROM system.workers WHERE vw_name='vw_write' LIMIT 1")

echo "Finish export CLICKHOUSE_HOST=$CLICKHOUSE_HOST CLICKHOUSE_PORT_TCP=$CLICKHOUSE_PORT_TCP CLICKHOUSE_PORT_HTTP=$CLICKHOUSE_PORT_HTTP CNCH_WRITE_WORKER_HOST=$CNCH_WRITE_WORKER_HOST CNCH_WRITE_WORKER_PORT_TCP=$CNCH_WRITE_WORKER_PORT_TCP"

mkdir -p $TEST_OUTPUT
mkdir -p $WORK_DIR
rm -rf $TEST_OUTPUT/*


# ------------------- 1
echo "Test suite External table start"
mkdir -p $WORK_DIR/external_table_output
export EXTRA_OPTIONS='enable_optimizer_fallback=0'

if [ "$RUN_SAMPLE" = 'true' ]; then
    SAMPLE='11008_no_ip_port_dictionary*'
else
    SAMPLE=''
fi

./clickhouse-test --use-skip-list --stop --hung-check --jobs 4 --order asc -q ./queries --run cnch_external_table --client-option ${EXTRA_OPTIONS} --print-time $SAMPLE 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a $WORK_DIR/external_table_output/test_result.txt

./process_functional_tests_result.py --in-results-dir $WORK_DIR/external_table_output/ --out-results-file $TEST_OUTPUT/external_table_test_results.txt --out-status-file $TEST_OUTPUT/check_status.tsv || echo -e "failure\tCannot parse results" > $TEST_OUTPUT/check_status.tsv
mv $WORK_DIR/external_table_output/test_result.txt $TEST_OUTPUT/external_table.log
rmdir $WORK_DIR/external_table_output

echo "Test suite External table finish"
# ------------------- 2
echo "Test suite FuntionalStateless (w.o optimizer) start"
mkdir -p $WORK_DIR/wo_optimizer
export EXTRA_OPTIONS='enable_optimizer_fallback=0'

if [ "$RUN_SAMPLE" = 'true' ]; then
    SAMPLE='00001_select_1*'
else
    SAMPLE=''
fi

./clickhouse-test --use-skip-list --stop --hung-check --jobs 4 --order asc -q ./queries --run cnch_stateless --client-option ${EXTRA_OPTIONS} --print-time $SAMPLE 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a $WORK_DIR/wo_optimizer/test_result.txt

./process_functional_tests_result.py --in-results-dir $WORK_DIR/wo_optimizer/ --out-results-file $TEST_OUTPUT/wo_optimizer_test_results.txt --out-status-file $TEST_OUTPUT/check_status.tsv || echo -e "failure\tCannot parse results" > $TEST_OUTPUT/check_status.tsv

mv $WORK_DIR/wo_optimizer/test_result.txt $TEST_OUTPUT/wo_optimizer.log
rmdir $WORK_DIR/wo_optimizer
echo "Test suite FuntionalStateless (w.o optimizer) finish"
# ------------------- 3
echo "Test suite FuntionalStateless no tenant (w.o optimizer) start"
mkdir -p $WORK_DIR/no_tenant_wo_optimizer
export EXTRA_OPTIONS='enable_optimizer_fallback=0'

if [ "$RUN_SAMPLE" = 'true' ]; then
    SAMPLE='00210_handle_nullable_array.*'
else
    SAMPLE=''
fi

./clickhouse-test --use-skip-list --stop --hung-check --jobs 4 --order asc -q ./queries --run cnch_stateless_no_tenant --client-option ${EXTRA_OPTIONS} --print-time $SAMPLE 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a $WORK_DIR/no_tenant_wo_optimizer/test_result.txt

./process_functional_tests_result.py --in-results-dir $WORK_DIR/no_tenant_wo_optimizer/ --out-results-file $TEST_OUTPUT/no_tenant_wo_optimizer_test_results.txt --out-status-file $TEST_OUTPUT/check_status.tsv || echo -e "failure\tCannot parse results" > $TEST_OUTPUT/check_status.tsv

mv $WORK_DIR/no_tenant_wo_optimizer/test_result.txt $TEST_OUTPUT/no_tenant_wo_optimizer.log
rmdir $WORK_DIR/no_tenant_wo_optimizer
echo "Test suite FuntionalStateless no tenant (w.o optimizer) finish"
# ------------------- 4
echo "Test suite FuntionalStateless (w optimizer) start"
mkdir -p $WORK_DIR/w_optimizer
export EXTRA_OPTIONS='enable_optimizer_fallback=0 enable_optimizer=1'

if [ "$RUN_SAMPLE" = 'true' ]; then
    SAMPLE='00001_select_1*'
else
    SAMPLE=''
fi

./clickhouse-test --use-skip-list --stop --hung-check --jobs 4 --order asc -q ./queries --run cnch_stateless --client-option ${EXTRA_OPTIONS} --print-time $SAMPLE 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a $WORK_DIR/w_optimizer/test_result.txt

./process_functional_tests_result.py --in-results-dir $WORK_DIR/w_optimizer/ --out-results-file $TEST_OUTPUT/w_optimizer.txt --out-status-file $TEST_OUTPUT/check_status.tsv || echo -e "failure\tCannot parse results" > $TEST_OUTPUT/check_status.tsv

mv $WORK_DIR/w_optimizer/test_result.txt $TEST_OUTPUT/w_optimizer.log
rmdir $WORK_DIR/w_optimizer
echo "Test suite FuntionalStateless (w optimizer) finish"
# ------------------- 5
echo "Test suite FuntionalStateless (w optimizer + bsp_mode) start"
mkdir -p $WORK_DIR/w_optimizer_bsp
export EXTRA_OPTIONS='enable_optimizer_fallback=0 enable_optimizer=1 bsp_mode=1 enable_runtime_filter=0'

if [ "$RUN_SAMPLE" = 'true' ]; then
    SAMPLE='00001_select_1*'
else
    SAMPLE=''
fi

./clickhouse-test --use-skip-list --stop --hung-check --jobs 4 --order asc -q ./queries --run cnch_stateless --client-option ${EXTRA_OPTIONS} --skip 00956_join_use_nulls_with_array_column --print-time $SAMPLE 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a $WORK_DIR/w_optimizer_bsp/test_result.txt

./process_functional_tests_result.py --in-results-dir $WORK_DIR/w_optimizer_bsp/ --out-results-file $TEST_OUTPUT/w_optimizer_bsp_test_results.txt --out-status-file $TEST_OUTPUT/check_status.tsv || echo -e "failure\tCannot parse results" > $TEST_OUTPUT/check_status.tsv

mv $WORK_DIR/w_optimizer_bsp/test_result.txt $TEST_OUTPUT/w_optimizer_bsp.log
rmdir $WORK_DIR/w_optimizer_bsp
echo "Test suite FuntionalStateless (w optimizer + bsp_mode) finish"
# ------------------- 6
echo "Test suite FuntionalStateless no tenant (w optimizer) start"
mkdir -p $WORK_DIR/no_tenant_w_optimizer
export EXTRA_OPTIONS='enable_optimizer_fallback=0 enable_optimizer=1'

if [ "$RUN_SAMPLE" = 'true' ]; then
    SAMPLE='00210_handle_nullable_array*'
else
    SAMPLE=''
fi

./clickhouse-test --use-skip-list --stop --hung-check --jobs 4 --order asc -q ./queries --run cnch_stateless_no_tenant --client-option ${EXTRA_OPTIONS} --print-time $SAMPLE 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a $WORK_DIR/no_tenant_w_optimizer/test_result.txt

./process_functional_tests_result.py --in-results-dir $WORK_DIR/no_tenant_w_optimizer/ --out-results-file $TEST_OUTPUT/no_tenant_w_optimizer_test_results.txt --out-status-file $TEST_OUTPUT/check_status.tsv || echo -e "failure\tCannot parse results" > $TEST_OUTPUT/check_status.tsv

mv $WORK_DIR/no_tenant_w_optimizer/test_result.txt $TEST_OUTPUT/no_tenant_w_optimizer.log
rmdir $WORK_DIR/no_tenant_w_optimizer
echo "Test suite FuntionalStateless no tenant (w optimizer) finish"
# ------------------- 7
echo "Test suite FuntionalStateless no tenant (w optimizer + bsp_mode) start"
mkdir -p $WORK_DIR/no_tenant_w_optimizer_bsp
export EXTRA_OPTIONS='enable_optimizer_fallback=0 enable_optimizer=1 bsp_mode=1 enable_runtime_filter=0'

if [ "$RUN_SAMPLE" = 'true' ]; then
    SAMPLE='00210_handle_nullable_array*'
else
    SAMPLE=''
fi

./clickhouse-test --use-skip-list --stop --hung-check --jobs 4 --order asc -q ./queries --run cnch_stateless_no_tenant --client-option ${EXTRA_OPTIONS} --skip 00956_join_use_nulls_with_array_column --print-time $SAMPLE 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a $WORK_DIR/no_tenant_w_optimizer_bsp/test_result.txt

./process_functional_tests_result.py --in-results-dir $WORK_DIR/no_tenant_w_optimizer_bsp/ --out-results-file $TEST_OUTPUT/no_tenant_w_optimizer_bsp_test_results.txt --out-status-file $TEST_OUTPUT/check_status.tsv || echo -e "failure\tCannot parse results" > $TEST_OUTPUT/check_status.tsv

mv $WORK_DIR/no_tenant_w_optimizer_bsp/test_result.txt $TEST_OUTPUT/no_tenant_w_optimizer_bsp.log
rmdir $WORK_DIR/no_tenant_w_optimizer_bsp
echo "Test suite FuntionalStateless no tenant (w optimizer) finish"
# ------------------- 8
echo "Test suite ClickHouse SQL start"
mkdir -p $WORK_DIR/ck_sql
export EXTRA_OPTIONS='enable_optimizer_fallback=0'

if [ "$RUN_SAMPLE" = 'true' ]; then
    SAMPLE='00049_any_left_join*'
else
    SAMPLE=''
fi

./clickhouse-test --use-skip-list --stop --hung-check --jobs 4 --order asc -q ./queries --run clickhouse_sql --client-option ${EXTRA_OPTIONS} --print-time $SAMPLE 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a $WORK_DIR/ck_sql/test_result.txt

./process_functional_tests_result.py --in-results-dir $WORK_DIR/ck_sql/ --out-results-file $TEST_OUTPUT/ck_sql_test_results.txt --out-status-file $TEST_OUTPUT/check_status.tsv || echo -e "failure\tCannot parse results" > $TEST_OUTPUT/check_status.tsv

mv $WORK_DIR/ck_sql/test_result.txt $TEST_OUTPUT/ck_sql.log
rmdir $WORK_DIR/ck_sql
echo "Test suite FuntionalStateless no tenant (w optimizer) finish"
# -------------------

cat $TEST_OUTPUT/check_status.tsv

while true; do echo "in the loop"; sleep 20; done
