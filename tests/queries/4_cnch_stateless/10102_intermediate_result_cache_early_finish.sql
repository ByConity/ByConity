set enable_optimizer=1;
set wait_intermediate_result_cache=0;
set enable_intermediate_result_cache=1;
set enable_intermediate_result_cache_streaming=1;
set max_block_size=1024;

DROP TABLE if exists early_finish_all;

CREATE TABLE early_finish_all(c1 UInt64, c2 UInt64) ENGINE = CnchMergeTree partition by c1 ORDER BY c1;

insert into early_finish_all values(1, 1);
insert into early_finish_all select 2, number from numbers(100000000);

select /*early_finish_a*/* from (select c1 from early_finish_all where c1 = 1 union all select count(c2) from early_finish_all where c1 = 2 and c2 != 0) limit 1;
select sleep(3);
select /*early_finish_b*/* from (select c1 from early_finish_all where c1 = 1 union all select count(c2) from early_finish_all where c1 = 2 and c2 != 0) limit 1;
select sleep(3);

select /*early_finish_c*/count(c2) from early_finish_all where c1 = 2 and c2 != 0;
select /*early_finish_d*/count(c2) from early_finish_all where c1 = 2 and c2 != 0;

select /*early_finish_e*/count(c2) from early_finish_all where c1 = 2 and c2 != 1;
select /*early_finish_f*/* from (select c1 from early_finish_all where c1 = 1 union all select count(c2) from early_finish_all where c1 = 2 and c2 != 1) limit 1;

DROP TABLE early_finish_all;
