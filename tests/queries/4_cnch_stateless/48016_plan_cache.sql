set enable_optimizer=1;
set enable_plan_cache=1;

DROP TABLE IF EXISTS cache;
DROP TABLE IF EXISTS cache2;
CREATE TABLE cache (a UInt32, b UInt32, c Nullable(UInt64)) ENGINE = CnchMergeTree() partition by a order by a;
CREATE TABLE cache2 (a UInt32, b UInt32, c Nullable(UInt64)) ENGINE = CnchMergeTree() partition by a order by a;

insert into cache values(1, 2, 3)(2, 3, 4)(3, 4, 5)(4, 5, 6)(3, 2 ,1)(4, 3, 2)(5, 4, null);
insert into cache2 values(3, 4, 5)(4, 5, 6)(3, 2 ,1)(4, 3, 2)(5, 4, null);

select c1.a from cache c1, cache2 c2 where c1.a=c2.b order by c1.a;

select c1.b from cache c1, cache2 c2 where c1.a=c2.b group by c1.b order by c1.b limit 10;

with t1 as
         (select c1.b as a from cache c1, cache2 c2 where c1.a=c2.b group by c1.b order by c1.b limit 10)
select
    *
from
    (select a
     from t1
     where t1.a < 5
     union all
     select a
     from t1
     where t1.a > 5
    )order by a;

select c1.a from cache c1, cache2 c2 where c1.a=c2.b order by c1.a;

select c1.b from cache c1, cache2 c2 where c1.a=c2.b group by c1.b order by c1.b limit 10;

with t1 as
         (select c1.b as a from cache c1, cache2 c2 where c1.a=c2.b group by c1.b order by c1.b limit 10)
select
    *
from
    (select a
     from t1
     where t1.a < 5
     union all
     select a
     from t1
     where t1.a > 5
    )order by a;

select * from cache format Null;
ALTER TABLE cache drop column c;
select * from cache format Null;
select c1.a from cache c1, cache2 c2 where c1.a=c2.b order by c1.a;

ALTER TABLE cache ADD column c Nullable(UInt64);
select * from cache format Null;

select count() from cache2;
insert into cache2 values(1,2,3);
select count() from cache2;

DROP TABLE IF EXISTS cache;
DROP TABLE IF EXISTS cache2;
