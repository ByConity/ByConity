#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# create files using compression method and without it to check that both queries work correct
${CLICKHOUSE_CLIENT} --query "SELECT toInt32(number % 10) as tag_id, groupArray(number) as uids, toDate('2024-01-01') as p_date from numbers(100000000, 1000000) GROUP BY tag_id INTO OUTFILE 'hdfs://${HDFS_PATH_ROOT}/test_bitmap_format_input_array.parquet' FORMAT Parquet Compression 'none' SETTINGS outfile_in_server_with_tcp=1, overwrite_current_file=1;"

# create table to check inserts
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_bitmap_format_input_00828;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_bitmap_format_input_00828 (tag_id Int32, uids BitMap64, p_date Date) Engine=CnchMergeTree() ORDER BY tag_id PARTITION BY p_date;"

# insert them
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_bitmap_format_input_00828 FORMAT Parquet INFILE 'hdfs://${HDFS_PATH_ROOT}/test_bitmap_format_input_array.parquet';"

# export the data with BitMap64 in column, and in Parquet it will be converted to LIST of UInt64
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_bitmap_format_input_00828 INTO OUTFILE 'hdfs://${HDFS_PATH_ROOT}/test_bitmap_format_output_bitmap.parquet' FORMAT Parquet Compression 'none' SETTINGS outfile_in_server_with_tcp=1, overwrite_current_file=1;"

# check result
${CLICKHOUSE_CLIENT} --query "SELECT 'from array', tag_id, bitmapCardinality(uids) as cnt FROM test_bitmap_format_input_00828 order by tag_id, p_date;"

# re-insert into into the bitmap table, checking that the conversion is ok
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test_bitmap_format_input_00828;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_bitmap_format_input_00828 FORMAT Parquet INFILE 'hdfs://${HDFS_PATH_ROOT}/test_bitmap_format_output_bitmap.parquet';"

# check result
${CLICKHOUSE_CLIENT} --query "SELECT 'from bitmap', tag_id, bitmapCardinality(uids) as cnt FROM test_bitmap_format_input_00828 order by tag_id, p_date;"

## Now test Array(Decimal) type -> Bitmap64, those decimal values can be converted to UInt64 will be successfully converted to BitMap64
## otherwise expression will be thrown
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test_bitmap_format_input_00828;"

${CLICKHOUSE_CLIENT} --query "select 100 as tag_id, array(cast(toUInt64(-2), 'Decimal128(2)'), cast(toUInt64(-3), 'Decimal128(2)')) as uids, toDate('2024-01-02') as p_date into outfile 'hdfs://${HDFS_PATH_ROOT}/test_bitmap_format_large_uint_2_decimal.parquet' format Parquet Compression 'none' SETTINGS outfile_in_server_with_tcp=1, overwrite_current_file=1;"

# insert the file above is ok
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_bitmap_format_input_00828 FORMAT Parquet INFILE 'hdfs://${HDFS_PATH_ROOT}/test_bitmap_format_large_uint_2_decimal.parquet';"

${CLICKHOUSE_CLIENT} --query "select 'valid decimal', * from test_bitmap_format_input_00828 order by tag_id, p_date;"

${CLICKHOUSE_CLIENT} --query "select 101 as tag_id, array(cast('1111111111111111111111', 'Decimal128(2)'), cast('1111111111111111111112', 'Decimal128(2)')) as uids, toDate('2024-01-03') as p_date into outfile 'hdfs://${HDFS_PATH_ROOT}/test_bitmap_format_overflow_decimal.parquet' format Parquet  Compression 'none' SETTINGS outfile_in_server_with_tcp=1, overwrite_current_file=1;"

# insert the file above will fail
set +e
{ ERROR=$(${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_bitmap_format_input_00828 FORMAT Parquet INFILE 'hdfs://${HDFS_PATH_ROOT}/test_bitmap_format_overflow_decimal.parquet';" 2>&1 1>&$out); } {out}>&1
echo "$ERROR" | grep -q "Convert overflow:" && echo "OK - Overflow decimal not allowed"
set -e

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_bitmap_format_input_00828;"
