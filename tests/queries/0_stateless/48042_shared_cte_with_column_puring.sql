
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t1_local;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t2_local;
CREATE TABLE t1_local (a UInt32, b UInt32, c Nullable(UInt64)) ENGINE = MergeTree() partition by a order by a;
create table t1 (a UInt32, b UInt32, c Nullable(UInt64)) engine = Distributed(test_shard_localhost, currentDatabase(), t1_local);
CREATE TABLE t2_local (a UInt32, b UInt32, c Nullable(UInt64)) ENGINE = MergeTree() partition by a order by a;
create table t2 (a UInt32, b UInt32, c Nullable(UInt64)) engine = Distributed(test_shard_localhost, currentDatabase(), t2_local);

with cte1 as (
    select t1.* from t1 left join t2 on t1.a = t2.b
), cte2 as (
    select cte1.b from cte1 where a != 1
), cte3 as (
    select cte1.a from cte1 left join cte2 on cte1.a = cte2.b
)
select count() from cte2, cte3, cte3 cte4;