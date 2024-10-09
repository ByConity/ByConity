
drop table if exists test_bitmap_index_with_optimizer;

set enable_optimizer = 1;

CREATE TABLE test_bitmap_index_with_optimizer (`p_date` Date, `id` Int32, `vids` Array(Int32) BitMapIndex) 
ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY id settings enable_build_ab_index = 0, index_granularity = 8192;

insert into test_bitmap_index_with_optimizer select '2022-01-01', number, [number % 5, number % 50, number % 100] from numbers(100);

select 'enable_ab_index_optimization = 0;';
set enable_ab_index_optimization = 0;

select count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, 1) or arraySetCheck(vids, 2);
select count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2));

select arraySetCheck(vids, 1) from test_bitmap_index_with_optimizer format Null;
select arraySetCheck(vids, (1,2)) from test_bitmap_index_with_optimizer format Null;

select arraySetCheck(vids, (1,2)) from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2)) format Null;
select arraySetCheck(vids, 1) from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2)) format Null;

select p_date, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by p_date;
select vids, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by vids format Null;
select p_date, vids, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by p_date, vids format Null;

select multiIf(arraySetCheck(vids, 1), 1, arraySetCheck(vids, 2), 2, 3) from test_bitmap_index_with_optimizer 
    where arraySetCheck(vids, 1) or arraySetCheck(vids, 2) or arraySetCheck(vids, 3) format Null;
select multiIf(arraySetCheck(vids, 1), 1, arraySetCheck(vids, 2), 2, 3) as key, count() from test_bitmap_index_with_optimizer 
    where arraySetCheck(vids, 1) or arraySetCheck(vids, 2) or arraySetCheck(vids, 3) group by key format Null;

select 'enable_ab_index_optimization = 1;';
set enable_ab_index_optimization = 1;

select count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, 1) or arraySetCheck(vids, 2);
select count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2));

select arraySetCheck(vids, 1) from test_bitmap_index_with_optimizer format Null;
select arraySetCheck(vids, (1,2)) from test_bitmap_index_with_optimizer format Null;

select arraySetCheck(vids, (1,2)) from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2)) format Null;
select arraySetCheck(vids, 1) from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2)) format Null;

select p_date, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by p_date;
select vids, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by vids format Null;
select p_date, vids, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by p_date, vids format Null;

select multiIf(arraySetCheck(vids, 1), 1, arraySetCheck(vids, 2), 2, 3) from test_bitmap_index_with_optimizer 
    where arraySetCheck(vids, 1) or arraySetCheck(vids, 2) or arraySetCheck(vids, 3) format Null;
select multiIf(arraySetCheck(vids, 1), 1, arraySetCheck(vids, 2), 2, 3) as key, count() from test_bitmap_index_with_optimizer 
    where arraySetCheck(vids, 1) or arraySetCheck(vids, 2) or arraySetCheck(vids, 3) group by key format Null;

alter table test_bitmap_index_with_optimizer modify setting enable_build_ab_index = 1;
insert into test_bitmap_index_with_optimizer select '2022-01-01', number, [number % 5, number % 50, number % 100] from numbers(100);

select 'enable_ab_index_optimization = 0;';
set enable_ab_index_optimization = 0;

select arraySetCheck(vids, 1) from test_bitmap_index_with_optimizer format Null;
select arraySetCheck(vids, (1,2)) from test_bitmap_index_with_optimizer format Null;

select arraySetCheck(vids, (1,2)) from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2)) format Null;
select arraySetCheck(vids, 1) from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2)) format Null;

select p_date, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by p_date;
select vids, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by vids format Null;
select p_date, vids, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by p_date, vids format Null;

select multiIf(arraySetCheck(vids, 1), 1, arraySetCheck(vids, 2), 2, 3) from test_bitmap_index_with_optimizer 
    where arraySetCheck(vids, 1) or arraySetCheck(vids, 2) or arraySetCheck(vids, 3) format Null;
select multiIf(arraySetCheck(vids, 1), 1, arraySetCheck(vids, 2), 2, 3) as key, count() from test_bitmap_index_with_optimizer 
    where arraySetCheck(vids, 1) or arraySetCheck(vids, 2) or arraySetCheck(vids, 3) group by key format Null;

select 'enable_ab_index_optimization = 1;';
set enable_ab_index_optimization = 1;

select arraySetCheck(vids, 1) from test_bitmap_index_with_optimizer format Null;
select arraySetCheck(vids, (1,2)) from test_bitmap_index_with_optimizer format Null;

select arraySetCheck(vids, (1,2)) from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2)) format Null;
select arraySetCheck(vids, 1) from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2)) format Null;

select p_date, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by p_date;
select vids, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by vids format Null;
select p_date, vids, count() from test_bitmap_index_with_optimizer where arraySetCheck(vids, (1,2,3)) group by p_date, vids format Null;

select multiIf(arraySetCheck(vids, 1), 1, arraySetCheck(vids, 2), 2, 3) from test_bitmap_index_with_optimizer 
    where arraySetCheck(vids, 1) or arraySetCheck(vids, 2) or arraySetCheck(vids, 3) format Null;
select multiIf(arraySetCheck(vids, 1), 1, arraySetCheck(vids, 2), 2, 3) as key, count() from test_bitmap_index_with_optimizer 
    where arraySetCheck(vids, 1) or arraySetCheck(vids, 2) or arraySetCheck(vids, 3) group by key format Null;

drop table test_bitmap_index_with_optimizer;