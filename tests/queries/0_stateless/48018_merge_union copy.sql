set enable_optimizer=1;
set enable_optimizer_white_list=0;

DROP TABLE IF EXISTS with_union;
DROP TABLE IF EXISTS with_union_local;
CREATE TABLE with_union_local (a Int32, b UInt8) ENGINE = MergeTree() partition by a order by a;
create table with_union as with_union_local engine = Distributed(test_shard_localhost, currentDatabase(), with_union_local);

explain select count(1) from (select * from with_union union all select * from with_union where b < 8  union all select * from with_union);

DROP TABLE IF EXISTS with_union;
DROP TABLE IF EXISTS with_union_local;
