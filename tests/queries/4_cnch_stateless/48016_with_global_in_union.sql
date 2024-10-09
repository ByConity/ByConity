DROP TABLE IF EXISTS with_union;
CREATE TABLE with_union (a Int32, b UInt8) ENGINE = CnchMergeTree() partition by a order by a;
insert into with_union values(5, 6)(6, 7)(7, 8)(8, 9)(10, 11);

select '=====';
select * from (with x as (select * from with_union) select * from x union all select * from x) order by a;
select '=====';
select * from (with x as (select * from with_union) select * from with_union union all select * from with_union) order by a;
select '=====';
select * from (((with x as (select * from with_union) select * from x  union all select * from x ) union all  select * from with_union)) order by a;
select '=====';
select * from (((with x as (select * from with_union) select * from with_union union all select * from with_union ) union all select * from with_union)) order by a;
select '=====';
select * from (((with x as (select * from with_union) select * from x union all select * from x ) union all select * from with_union)) order by a;
select '=====';
select * from (((with x as (select * from with_union) select * from with_union union all select * from with_union) union all select * from with_union)) order by a;
select '=====';
select * from ((select * from with_union union all (with x as (select * from with_union) select * from x union all select * from x))) order by a;
select '=====';
select * from ((select * from with_union union all (with x as (select * from with_union) select * from with_union union all select * from with_union))) order by a;

select '=====';
select * from (((with x as (select * from with_union where a < 8) select * from x  where b < 8 union all select * from x ) union all  select * from with_union)) order by a;
select '=====';
select * from (((with x as (select * from with_union where a < 8) select * from with_union where b < 8 union all select * from with_union ) union all  select * from with_union)) order by a;
select '=====';
select * from (((with x as (select * from with_union where a < 8) select * from x where b < 8  union all select * from x ) union all  select * from with_union)) order by a;
select '=====';
select * from (((with x as (select * from with_union where a < 8) select * from with_union where b < 8  union all select * from with_union ) union all  select * from with_union)) order by a;
select '=====';
select * from ((select * from with_union union all (with x as (select * from with_union where a < 8) select * from x where b < 8  union all select * from x))) order by a;
select '=====';
select * from ((select * from with_union union all (with x as (select * from with_union where a < 8) select * from with_union where b < 8  union all select * from with_union))) order by a;

DROP TABLE IF EXISTS with_union;
