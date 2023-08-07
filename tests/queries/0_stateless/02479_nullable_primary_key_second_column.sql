drop table if exists test_table;

create table test_table (A Nullable(String), B Nullable(String)) engine = CnchMergeTree order by (A,B) settings index_granularity = 1, allow_nullable_key=1;

insert into test_table values ('a', 'b'), ('a', null), (null, 'b');

select * from test_table where B is null;

drop table test_table;

DROP TABLE IF EXISTS dm_metric_small;
CREATE TABLE dm_metric_small (`is_search_manual` Nullable(Int32), `app_code` Nullable(Int64), `bi_inventory_type` Nullable(Int64)) ENGINE = CnchMergeTree ORDER BY (`bi_inventory_type`, `app_code`, `is_search_manual`) SETTINGS index_granularity = 1, allow_nullable_key=1;
INSERT INTO TABLE dm_metric_small VALUES (1,2,20016), (1,28,28016), (0,28,28035), (0,4,40016), (1,4,40016), (1,4,40016), (1,4,40086), (NULL,52,52016), (0,8,80016), (0,8,80027);
SELECT * FROM dm_metric_small WHERE is_search_manual IS NULL;
DROP TABLE dm_metric_small;
