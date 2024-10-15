use test;
DROP TABLE IF EXISTS test48068;
CREATE TABLE test48068 (d Date DEFAULT '2000-01-01', x UInt8) ENGINE = CnchMergeTree order by x;
INSERT INTO test48068 (x) SELECT toUInt8(number) AS x FROM system.numbers LIMIT 256;
set max_block_size = 10;
set max_insert_block_size = 10;
set min_insert_block_size_rows = 10;
set enable_pushdown_filter_through_stateful = 0;
select idx from (select x,rowNumberInBlock() as idx from test.test48068 order by idx desc) where x = 1;

DROP TABLE IF EXISTS test48068;
