
set enable_ab_index_optimization = 1;
set enable_optimizer = 1;
set max_threads = 8;
set exchange_source_pipeline_threads = 1;
set optimizer_index_projection_support=1;
set prefer_localhost_replica = 1;

set enable_intermediate_result_cache=1;
set wait_intermediate_result_cache=0;
set enable_optimizer_fallback=0;

drop table if exists cache_index_expression;

create table cache_index_expression (p_date Date, id Int32, vids Array(Int32) BitmapIndex, id1 Int64, id2 String, id3 Nullable(Int64), id4 Nullable(String)) 
    engine = CnchMergeTree partition by p_date order by id settings min_bytes_for_wide_part = 0, enable_build_ab_index = 0;


insert into cache_index_expression select '2023-01-01', number, [number % 3],  number+1, toString(number+2), number+3, toString(number+4) from numbers(5);

explain stats=0 
select sum(id) from cache_index_expression
    where arraySetCheck(vids, 1) and id1 in (2,3,4) and id2 in ('3', '4', '5');

select sum(id) from cache_index_expression
    where arraySetCheck(vids, 1) and id1 in (2,3,4) and id2 in ('3', '4', '5');

select sum(id) from cache_index_expression
    where arraySetCheck(vids, 1) and id1 in (2,3,4) and id2 in ('3', '4', '5');

select sum(id) from cache_index_expression
    where arraySetCheck(vids, 1) and id1 in (2,3,4) and id2 in ('3', '4', '5')
settings enable_intermediate_result_cache=0;

explain stats=0 
select sum(id), id1 as id_1 from cache_index_expression
    where arraySetCheck(vids, 1) and id_1 in (2,3,4) and id2 in ('3', '4', '5') group by id_1;

select sum(id), id1 as id_1 from cache_index_expression
    where arraySetCheck(vids, 1) and id_1 in (2,3,4) and id2 in ('3', '4', '5') group by id_1;

select sum(id), id1 as id_1 from cache_index_expression
    where arraySetCheck(vids, 1) and id_1 in (2,3,4) and id2 in ('3', '4', '5') group by id_1;

select sum(id), id1 as id_1 from cache_index_expression
    where arraySetCheck(vids, 1) and id_1 in (2,3,4) and id2 in ('3', '4', '5') group by id_1
settings enable_intermediate_result_cache=0;