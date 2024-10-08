set enable_optimizer=1;

drop table if exists t48025_abl;
create table t48025_abl(x UInt8) ENGINE=CnchMergeTree() order by x;

EXPLAIN SELECT sum(x) FROM t48025_abl;
SET enable_push_partial_block_list = 'sum';
EXPLAIN SELECT sum(x) FROM t48025_abl;

drop table if exists t48025_abl;