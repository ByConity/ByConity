set enable_optimizer=1;
set wait_intermediate_result_cache=0;
set enable_intermediate_result_cache=1;

DROP TABLE if exists cache_table_all;

CREATE TABLE cache_table_all(c1 UInt64, c2 UInt64) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1;

insert into cache_table_all values (1, 1);

select count(c2) from cache_table_all where c2 > 10000;
select count(c2) from cache_table_all where c2 > 10000;

select sum(c2) from cache_table_all where c2 > 10000;
select sum(c2) from cache_table_all where c2 > 10000;

select countDistinct(c2) from cache_table_all where c2 > 10000;
select countDistinct(c2) from cache_table_all where c2 > 10000;

select c2 from cache_table_all where c2 > 10000 group by c2;
select c2 from cache_table_all where c2 > 10000 group by c2;

select countDistinct(c2), count(c2) from cache_table_all where c2 > 10000;
select countDistinct(c2), count(c2) from cache_table_all where c2 > 10000;

insert into cache_table_all values (1, 10001);

select count(c2) from cache_table_all where c2 > 10000;
select count(c2) from cache_table_all where c2 > 10000;

select sum(c2) from cache_table_all where c2 > 10000;
select sum(c2) from cache_table_all where c2 > 10000;

select countDistinct(c2) from cache_table_all where c2 > 10000;
select countDistinct(c2) from cache_table_all where c2 > 10000;

select c2 from cache_table_all where c2 > 10000 group by c2;
select c2 from cache_table_all where c2 > 10000 group by c2;

select countDistinct(c2), count(c2) from cache_table_all where c2 > 10000;
select countDistinct(c2), count(c2) from cache_table_all where c2 > 10000;

DROP TABLE if exists cache_table_all;
