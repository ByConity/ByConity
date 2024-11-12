drop table if exists test_columndiff_20022;

create table if not exists test_columndiff_20022 (type String, p_date Date, id_map BitMap64) Engine=CnchMergeTree order by p_date;

insert into test_columndiff_20022 values ('a', '2021-07-01', [1,2,3,4,5,11]);
insert into test_columndiff_20022 values ('a', '2021-07-02', [1,2,3,4,11,12]);
insert into test_columndiff_20022 values ('a', '2021-07-03', [1,2,3,11,12,13]);
insert into test_columndiff_20022 values ('a', '2021-07-04', [1,2,11,12,13,14]);
insert into test_columndiff_20022 values ('a', '2021-07-05', [1,11,12,13,14,15]);

insert into test_columndiff_20022 values ('b', '2021-07-01', [1,2,3,4,5,11]);
insert into test_columndiff_20022 values ('b', '2021-07-02', [1,2,3,4,11,12]);
insert into test_columndiff_20022 values ('b', '2021-07-03', [1,2,3,11,12,13]);

-- compute the difference between the day after 'today' and 'today', return a sum value
select
    'today_and_tomorrow',
    tupleElement(res_tuple, 1) as p_date,
    tupleElement(res_tuple, 2)[1] as latter_former_cnt
from (
    select arrayJoin(res) as res_tuple from (
        select bitmapColumnDiff(0, 'backward', 1)(p_date, id_map) as res from test_columndiff_20022 where type = 'a'
))
order by p_date;

-- compute both the difference between the day after 'today' and 'today', as well as that of 'today' and the day after 'today', return result bitmap
select
    'both',
    tupleElement(res_tuple, 1) as p_date,
    tupleElement(res_tuple, 2)[1] as former_latter,
    tupleElement(res_tuple, 2)[2] as latter_former
from (
    select arrayJoin(res) as res_tuple from (
        select bitmapColumnDiff(1, 'bidirection', 1)(p_date, id_map) as res from test_columndiff_20022 where type = 'a'
))
order by p_date;

-- compute the difference between 'today' and the 2nd day after 'today' (step=2), return result bitmap
select
    'today_and_2_days_later',
    tupleElement(res_tuple, 1) as p_date,
    tupleElement(res_tuple, 2)[1] as latter_former
from (
    select arrayJoin(res) as res_tuple from (
        select bitmapColumnDiff(1, 'forward', 2)(p_date, id_map) as res from test_columndiff_20022 where type = 'a'
))
order by p_date;


-- compute both the difference between the day after 'today' and 'today', as well as that of 'today' and the day after 'today', return result bitmap
select
    'both_group_by',
    type,
    tupleElement(res_tuple, 1) as p_date,
    tupleElement(res_tuple, 2)[1] as former_latter,
    tupleElement(res_tuple, 2)[2] as latter_former
from (
    select type, arrayJoin(res) as res_tuple from (
        select type, bitmapColumnDiff(1, 'bidirection', 1)(p_date, id_map) as res
        from test_columndiff_20022
        group by type
    ))
order by type, p_date;

--- different diff_key type
drop table if exists test_columndiff_20022;
create table if not exists test_columndiff_20022 (type String, p_date String, id_map BitMap64) Engine=CnchMergeTree order by p_date;

insert into test_columndiff_20022 values ('a', '2021-07-01', [1,2,3,4,5,11]);
insert into test_columndiff_20022 values ('a', '2021-07-02', [1,2,3,4,11,12]);
insert into test_columndiff_20022 values ('a', '2021-07-03', [1,2,3,11,12,13]);
insert into test_columndiff_20022 values ('a', '2021-07-04', [1,2,11,12,13,14]);
insert into test_columndiff_20022 values ('a', '2021-07-05', [1,11,12,13,14,15]);

-- compute both the difference between the day after 'today' and 'today', as well as that of 'today' and the day after 'today', return result bitmap
select
    'both',
    tupleElement(res_tuple, 1) as p_date,
    tupleElement(res_tuple, 2)[1] as former_latter,
    tupleElement(res_tuple, 2)[2] as latter_former
from (
    select arrayJoin(res) as res_tuple from (
        select bitmapColumnDiff(1, 'bidirection', 1)(p_date, id_map) as res from test_columndiff_20022 where type = 'a'
))
order by p_date;

drop table if exists test_columndiff_20022;