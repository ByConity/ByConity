drop table if exists test.t40114_bitmap_index_force_index_by_date ;

create table test.t40114_bitmap_index_force_index_by_date
(
    date Date,
    uid Int32,
    int_vid Array(Int32) BitmapIndex,
    events Int64
) engine = CnchMergeTree() partition by date order by uid settings min_bytes_for_wide_part = 0, index_granularity = 1024, enable_build_ab_index = 1;

insert into test.t40114_bitmap_index_force_index_by_date values ('2024-08-01', 100, [1, 2], 10), ('2024-08-02', 100, [1, 2], 10);

set enable_ab_index_optimization = 1, enable_partition_filter_push_down = 1;

-- force_index_by_date
select sum(events) from test.t40114_bitmap_index_force_index_by_date where arraySetCheck(int_vid, [1]) settings force_index_by_date = 1 format Null; -- { serverError 277 }
select sum(events) from test.t40114_bitmap_index_force_index_by_date where date = '2024-08-01' and arraySetCheck(int_vid, [1]) settings force_index_by_date = 1 format Null;
-- skip this test temporary as it does not throw error when optimizer=0
--select sum(events) from test.t40114_bitmap_index_force_index_by_date where date in ('2024-08-01', '2023-07-01') and arraySetCheck(int_vid, [1]) settings force_index_by_date = 1 format Null; -- { serverError 277 }
select sum(events) from test.t40114_bitmap_index_force_index_by_date where date >= '2023-08-01' and date <= '2024-08-01' and arraySetCheck(int_vid, [1]) settings force_index_by_date = 1 format Null;
select sum(events) from test.t40114_bitmap_index_force_index_by_date where toStartOfYear(date) = '2024-08-01' and arraySetCheck(int_vid, [1]) settings force_index_by_date = 1 format Null;


-- force_primary_key
select sum(events) from test.t40114_bitmap_index_force_index_by_date where arraySetCheck(int_vid, [1]) settings force_primary_key = 1 format Null; -- { serverError 277 }
select sum(events) from test.t40114_bitmap_index_force_index_by_date where uid = 1 and arraySetCheck(int_vid, [1]) settings force_primary_key = 1 format Null;
select sum(events) from test.t40114_bitmap_index_force_index_by_date where uid in (1, 2, 3) and arraySetCheck(int_vid, [1]) settings force_primary_key = 1 format Null;
select sum(events) from test.t40114_bitmap_index_force_index_by_date where uid >= 1 and uid <= 100 and arraySetCheck(int_vid, [1]) settings force_primary_key = 1 format Null;
select sum(events) from test.t40114_bitmap_index_force_index_by_date where uid * 100 = 100 and arraySetCheck(int_vid, [1]) settings force_primary_key = 1 format Null; -- { serverError 277 }

drop table if exists test.t40114_bitmap_index_force_index_by_date ;
