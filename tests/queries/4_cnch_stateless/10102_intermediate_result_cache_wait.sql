set enable_optimizer=1;

DROP TABLE if exists cache_table_all;

CREATE TABLE cache_table_all(c1 UInt64, c2 String) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1;

insert into cache_table_all values (1, 'a');
select sleep(3);
select sleep(3);
insert into cache_table_all values (2, 'b');

select c1 from cache_table_all group by c1 order by c1 settings enable_intermediate_result_cache=1, wait_intermediate_result_cache=3;

DROP TABLE cache_table_all;
