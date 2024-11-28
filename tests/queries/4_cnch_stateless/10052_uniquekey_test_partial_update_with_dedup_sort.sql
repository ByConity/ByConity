SET enable_staging_area_for_write=0, enable_unique_partial_update = 1;
DROP TABLE IF EXISTS unique_partial_update_with_dedup_sort;
DROP TABLE IF EXISTS no_unique;
CREATE TABLE no_unique
(
    `p_date` Date,
    `id` UInt32,
    `number` UInt32,
    `content` String,
    `extra` String,
    `is_deleted` UInt32
)
ENGINE = CnchMergeTree
PARTITION BY p_date
ORDER BY id;

-- 1000 insert
insert into no_unique(p_date, id, number, is_deleted) select '2020-01-01', number, number, 0 from system.numbers limit 1000;
-- 1000 delete
insert into no_unique(p_date, id, number, is_deleted) select '2020-01-01', number, number+1000, 1 from system.numbers limit 1000;
-- 1000 insert
insert into no_unique(p_date, id, number, is_deleted) select '2020-01-01', number, number+2000, 0 from system.numbers limit 1000;

CREATE TABLE unique_partial_update_with_dedup_sort
(
    `p_date` Date,
    `id` UInt32,
    `number` UInt32,
    `content` String,
    `extra` String
)
ENGINE = CnchMergeTree
PARTITION BY p_date
ORDER BY id
UNIQUE KEY id
SETTINGS enable_unique_partial_update = 1;

insert into unique_partial_update_with_dedup_sort (p_date, id, number, content, extra, _delete_flag_) select * from no_unique order by number;

select count() from unique_partial_update_with_dedup_sort;

DROP TABLE IF EXISTS unique_partial_update_with_dedup_sort;
DROP TABLE IF EXISTS no_unique;
