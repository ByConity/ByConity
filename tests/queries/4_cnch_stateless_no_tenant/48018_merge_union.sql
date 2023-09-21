set enable_optimizer=1;
use test;
DROP TABLE IF EXISTS with_union;
CREATE TABLE with_union (a Int32, b UInt8) ENGINE = CnchMergeTree() partition by a order by a;

explain select count(1) from (select * from with_union union all select * from with_union where b < 8  union all select * from with_union);

DROP TABLE IF EXISTS with_union;
