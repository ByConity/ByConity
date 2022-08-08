#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CURDIR/../shell_config.sh

CI_ROOT="/cnch/bin"
UUID=$(cat /proc/sys/kernel/random/uuid)

if [ -d "$CI_ROOT" ]; then
    # -- ci env
    # -- Install pyarrow
    # -- NOTE: we redirect everything into /dev/null to supress some WARNINGs raised by pip
    python3 -m pip install pyarrow > /dev/null 2>&1

    HDFS_PATH="hdfs://10.8.147.86:31222/test_uniquekey_batch_loading/$UUID"
    PART_WRITER_TOOL="/cnch/bin/part_writer_tool"
else
    # --- local env
    # --- NOTE: still need to install pyarrow, but not every time
    HDFS_PATH="hdfs://10.8.157.153:31222/test_uniquekey_batch_loading/$UUID"
    PART_WRITER_TOOL="$CURDIR/../../../../build/dbms/src/FormaterTool/tests/part_writer_tool"
fi


# Given a src parquent file and dst location, this function will write parts
# from parquet file into the dst location folder
# Usage: part_witer_tool src_parquent_file dst_location table_schema
function part_writer_tool() {
    SRC_PARQUET_FILE="$1"
    DST_LOCATION="$2"
    TABLE_SCHEMA=$(echo "$3" | sed 's/ENGINE = CnchMergeTree//g')
    SETTINGS="settings cnch=1"
    NNPROXY="nnproxy='data.bytehouse.nnproxy-hostnetwork.service.lf'" # TODO: do we still need this on CI?
    $PART_WRITER_TOOL "load Parquet file '$SRC_PARQUET_FILE' as table $TABLE_SCHEMA location '$DST_LOCATION' $SETTINGS, $NNPROXY" > /dev/null 2>&1
}

function run_query() {
    $CLICKHOUSE_CLIENT -n -m --query "$1"
}

# --------------------------------------------------------------------------------

function test_simple() {
    # -- Define table schema
    TEST_DATABASE="test_batch_loading"
    MIRROR_TABLE="$TEST_DATABASE.mirror"
    TARGET_TABLE="$TEST_DATABASE.target"
    MIRROR_SCHEMA="$MIRROR_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree PARTITION BY d ORDER BY (d, k)"
    TARGET_SCHEMA="$TARGET_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree PARTITION BY d ORDER BY (d, k) UNIQUE KEY k "

    # -- Genearte test data
    PARQUET_FILE_ONE="$CURDIR/data_parquet_1"
    PARQUET_FILE_TWO="$CURDIR/data_parquet_2"
    PART_OUTPUT_DIR="$HDFS_PATH/"
    ATTCH_PARTS_DIR=$(echo $PART_OUTPUT_DIR | sed 's|hdfs:.*:[0-9]*/|/|g')
    python3 $CURDIR/10054_uniquekey_batch_loading.run $PARQUET_FILE_ONE $PARQUET_FILE_TWO

    # -- Use part writer to write parts
    part_writer_tool "$PARQUET_FILE_ONE" "$PART_OUTPUT_DIR" "$TARGET_SCHEMA"

    # -- Main tests
    #
    run_query "DROP DATABASE IF EXISTS $TEST_DATABASE"
    run_query "CREATE DATABASE IF NOT EXISTS $TEST_DATABASE"

    run_query "CREATE TABLE $MIRROR_SCHEMA"
    run_query "SYSTEM STOP MERGES $MIRROR_TABLE" # NOTE: stop merges for mirror table
    run_query "ALTER TABLE $MIRROR_TABLE ATTACH PARTS FROM '$ATTCH_PARTS_DIR'"
    run_query "SELECT '--test_simple----mirror--'"
    run_query "SELECT count() FROM system.cnch_parts where database='$TEST_DATABASE' and table='mirror'"
    run_query "SELECT * FROM $MIRROR_TABLE ORDER BY (d, k)"

    #
    run_query "ALTER TABLE $MIRROR_TABLE DETACH PARTITION '2022-03-29'"

    run_query "CREATE TABLE $TARGET_SCHEMA"
    run_query "ALTER TABLE $TARGET_TABLE DROP PARTITION '2022-03-29'"
    run_query "ALTER TABLE $TARGET_TABLE DROP PARTITION '2022-03-28'"
    run_query "ALTER TABLE $TARGET_TABLE ATTACH DETACHED PARTITION '2022-03-29' FROM $MIRROR_TABLE"
    run_query "ALTER TABLE $TARGET_TABLE ATTACH DETACHED PARTITION '2022-03-28' FROM $MIRROR_TABLE"
    #
    run_query "SELECT '--test_simple----target--'"
    run_query "SELECT count() FROM system.cnch_parts where database='$TEST_DATABASE' and table='target'"
    run_query "SELECT * FROM $TARGET_TABLE ORDER BY (d, k)"

    run_query "DROP DATABASE IF EXISTS test_batch_loading;"
}

function test_expression_unique_key() {
    # -- Define table schema
    TEST_DATABASE="test_batch_loading"
    MIRROR_TABLE="$TEST_DATABASE.mirror"
    TARGET_TABLE="$TEST_DATABASE.target"
    MIRROR_SCHEMA="$MIRROR_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree PARTITION BY d ORDER BY (d, k)"
    TARGET_SCHEMA="$TARGET_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree PARTITION BY d ORDER BY (d, k) UNIQUE KEY sipHash64(k)"

    # -- Genearte test data
    PARQUET_FILE_ONE="$CURDIR/data_parquet_1"
    PARQUET_FILE_TWO="$CURDIR/data_parquet_2"
    PART_OUTPUT_DIR="$HDFS_PATH/"
    ATTCH_PARTS_DIR=$(echo $PART_OUTPUT_DIR | sed 's|hdfs:.*:[0-9]*/|/|g')
    python3 $CURDIR/10054_uniquekey_batch_loading.run $PARQUET_FILE_ONE $PARQUET_FILE_TWO

    # -- Use part writer to write parts
    part_writer_tool "$PARQUET_FILE_ONE" "$PART_OUTPUT_DIR" "$TARGET_SCHEMA"

    # -- Main tests
    #
    run_query "DROP DATABASE IF EXISTS $TEST_DATABASE"
    run_query "CREATE DATABASE IF NOT EXISTS $TEST_DATABASE"

    run_query "CREATE TABLE $MIRROR_SCHEMA"
    run_query "SYSTEM STOP MERGES $MIRROR_TABLE" # NOTE: stop merges for mirror table
    run_query "ALTER TABLE $MIRROR_TABLE ATTACH PARTS FROM '$ATTCH_PARTS_DIR'"
    run_query "SELECT '--test_expression_unique_key----mirror--'"
    run_query "SELECT count() FROM system.cnch_parts where database='$TEST_DATABASE' and table='mirror'"
    run_query "SELECT * FROM $MIRROR_TABLE ORDER BY (d, k)"

    #
    run_query "ALTER TABLE $MIRROR_TABLE DETACH PARTITION '2022-03-28'"
    run_query "ALTER TABLE $MIRROR_TABLE DETACH PARTITION '2022-03-29'"

    run_query "CREATE TABLE $TARGET_SCHEMA"
    run_query "ALTER TABLE $TARGET_TABLE DROP PARTITION '2022-03-28'"
    run_query "ALTER TABLE $TARGET_TABLE DROP PARTITION '2022-03-29'"
    run_query "ALTER TABLE $TARGET_TABLE ATTACH DETACHED PARTITION '2022-03-28' FROM $MIRROR_TABLE"
    run_query "ALTER TABLE $TARGET_TABLE ATTACH DETACHED PARTITION '2022-03-29' FROM $MIRROR_TABLE"
    #
    run_query "SELECT '--test_expression_unique_key----target--'"
    run_query "SELECT count() FROM system.cnch_parts where database='$TEST_DATABASE' and table='target'"
    run_query "SELECT * FROM $TARGET_TABLE ORDER BY (d, k)"

    run_query "DROP DATABASE IF EXISTS test_batch_loading;"
}

function test_table_level() {
    # -- Define table schema
    TEST_DATABASE="test_batch_loading"
    MIRROR_TABLE="$TEST_DATABASE.mirror"
    TARGET_TABLE="$TEST_DATABASE.target"
    MIRROR_SCHEMA="$MIRROR_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree PARTITION BY d ORDER BY (d, k)"
    TARGET_SCHEMA="$TARGET_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree PARTITION BY d ORDER BY (d, k) UNIQUE KEY sipHash64(k)"

    # -- Genearte test data
    PARQUET_FILE_ONE="$CURDIR/data_parquet_1"
    PARQUET_FILE_TWO="$CURDIR/data_parquet_2"
    PART_OUTPUT_DIR="$HDFS_PATH/"
    ATTCH_PARTS_DIR=$(echo $PART_OUTPUT_DIR | sed 's|hdfs:.*:[0-9]*/|/|g')
    python3 $CURDIR/10054_uniquekey_batch_loading.run $PARQUET_FILE_ONE $PARQUET_FILE_TWO

    # -- Use part writer to write parts
    part_writer_tool "$PARQUET_FILE_ONE" "$PART_OUTPUT_DIR" "$TARGET_SCHEMA"

    # -- Main tests
    #
    run_query "DROP DATABASE IF EXISTS $TEST_DATABASE"
    run_query "CREATE DATABASE IF NOT EXISTS $TEST_DATABASE"

    run_query "CREATE TABLE $MIRROR_SCHEMA"
    run_query "SYSTEM STOP MERGES $MIRROR_TABLE" # NOTE: stop merges for mirror table
    run_query "ALTER TABLE $MIRROR_TABLE ATTACH PARTS FROM '$ATTCH_PARTS_DIR'"
    run_query "SELECT '--test_table_level----mirror--'"
    run_query "SELECT count() FROM system.cnch_parts where database='$TEST_DATABASE' and table='mirror'"
    run_query "SELECT * FROM $MIRROR_TABLE ORDER BY (d, k)"

    #
    run_query "ALTER TABLE $MIRROR_TABLE DETACH PARTITION '2022-03-28'"
    run_query "ALTER TABLE $MIRROR_TABLE DETACH PARTITION '2022-03-29'"

    run_query "CREATE TABLE $TARGET_SCHEMA SETTINGS partition_level_unique_keys=0" # NOTE: table level unique
    run_query "ALTER TABLE $TARGET_TABLE DROP PARTITION '2022-03-28'"
    run_query "ALTER TABLE $TARGET_TABLE DROP PARTITION '2022-03-29'"
    run_query "ALTER TABLE $TARGET_TABLE ATTACH DETACHED PARTITION '2022-03-28' FROM $MIRROR_TABLE"
    run_query "ALTER TABLE $TARGET_TABLE ATTACH DETACHED PARTITION '2022-03-29' FROM $MIRROR_TABLE"
    #
    run_query "SELECT '--test_table_level----target--'"
    run_query "SELECT count() FROM system.cnch_parts where database='$TEST_DATABASE' and table='target'"
    run_query "SELECT * FROM $TARGET_TABLE ORDER BY (d, k)"

    run_query "DROP DATABASE IF EXISTS test_batch_loading;"
}

function parallel_attach_parts_to_mirror() {
    part_writer_tool "$1" "$2" "$TARGET_SCHEMA"
    local attach_dir=$(echo $2 | sed 's|hdfs:.*:[0-9]*/|/|g')
    run_query "ALTER TABLE $MIRROR_TABLE ATTACH PARTS FROM '$attach_dir'"
}

function parallel_attach_partition_to_target() {
    run_query "ALTER TABLE $TARGET_TABLE DROP PARTITION '$1'"
    run_query "ALTER TABLE $TARGET_TABLE ATTACH DETACHED PARTITION '$1' FROM $MIRROR_TABLE"
}

function test_parallel_loading() {
    # -- Define table schema
    TEST_DATABASE="test_batch_loading"
    MIRROR_TABLE="$TEST_DATABASE.mirror"
    TARGET_TABLE="$TEST_DATABASE.target"
    MIRROR_SCHEMA="$MIRROR_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree PARTITION BY d ORDER BY (d, k)"
    TARGET_SCHEMA="$TARGET_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree PARTITION BY d ORDER BY (d, k) UNIQUE KEY sipHash64(k)"

    # -- Genearte test data
    PARQUET_FILE_ONE="$CURDIR/data_parquet_1"
    PARQUET_FILE_TWO="$CURDIR/data_parquet_2"
    PART_OUTPUT_ONE="$HDFS_PATH/output_1"
    PART_OUTPUT_TWO="$HDFS_PATH/output_2"
    python3 $CURDIR/10054_uniquekey_batch_loading.run $PARQUET_FILE_ONE $PARQUET_FILE_TWO

    # -- Main tests
    #
    run_query "DROP DATABASE IF EXISTS $TEST_DATABASE"
    run_query "CREATE DATABASE IF NOT EXISTS $TEST_DATABASE"
    run_query "CREATE TABLE $MIRROR_SCHEMA"
    run_query "SYSTEM STOP MERGES $MIRROR_TABLE" # NOTE: stop merges for mirror table
    run_query "CREATE TABLE $TARGET_SCHEMA SETTINGS partition_level_unique_keys=0"

    # -- Parallel loading two parquet files to mirror table
    parallel_attach_parts_to_mirror "$PARQUET_FILE_ONE" "$PART_OUTPUT_ONE" &
    parallel_attach_parts_to_mirror "$PARQUET_FILE_TWO" "$PART_OUTPUT_TWO" &
    wait

    run_query "ALTER TABLE $MIRROR_TABLE DETACH PARTITION '2022-03-28'"
    run_query "ALTER TABLE $MIRROR_TABLE DETACH PARTITION '2022-03-29'"

    # -- NOTE: the attach order here certainly will affect the final result for table level unique
    parallel_attach_partition_to_target "2022-03-28"
    parallel_attach_partition_to_target "2022-03-29"
    # wait

    run_query "SELECT '--test_parallel_loading----target--'"
    run_query "SELECT * FROM $TARGET_TABLE ORDER BY (d, k)"

    run_query "DROP DATABASE IF EXISTS $TEST_DATABASE"
}

function test_version_column_not_supported()
{
    # -- Define table schema
    TEST_DATABASE="test_batch_loading"
    MIRROR_TABLE="$TEST_DATABASE.mirror"
    TARGET_TABLE="$TEST_DATABASE.target"
    MIRROR_SCHEMA="$MIRROR_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree() PARTITION BY d ORDER BY (d, k)"
    TARGET_SCHEMA="$TARGET_TABLE(d Date, k UInt32, v String, e Array(Int32), m Map(Int64, String)) ENGINE = CnchMergeTree(d) PARTITION BY d ORDER BY (d, k) UNIQUE KEY sipHash64(k)"

    # -- Main tests
    #
    run_query "DROP DATABASE IF EXISTS $TEST_DATABASE"
    run_query "CREATE DATABASE IF NOT EXISTS $TEST_DATABASE"
    run_query "CREATE TABLE $MIRROR_SCHEMA"
    run_query "SYSTEM STOP MERGES $MIRROR_TABLE" # NOTE: stop merges for mirror table
    run_query "CREATE TABLE $TARGET_SCHEMA SETTINGS partition_level_unique_keys=0"

    # https://stackoverflow.com/questions/962255/how-to-store-standard-error-in-a-variable
    set +e  # allow fail
    run_query "SELECT '--test_version_column_not_supported-----'"
    { ERROR=$(run_query "ALTER TABLE $TARGET_TABLE ATTACH DETACHED PARTITION '2021-03-28' FROM $MIRROR_TABLE" 2>&1 1>&$out); } {out}>&1
    echo "$ERROR" | grep -q "Attach parition to a storage with version column is not supported" && echo "OK"
    set -e # disallow fail
}

function main() {
    test_version_column_not_supported
    test_simple
    test_expression_unique_key
    test_table_level
    test_parallel_loading
}


main
