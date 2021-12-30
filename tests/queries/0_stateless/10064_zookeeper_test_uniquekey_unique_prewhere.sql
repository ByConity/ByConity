set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.unique_prewhere;
drop table if exists test.unique_prewhere_large;
drop table if exists test.unique_prewhere_3;

create table test.unique_prewhere (id Int32, s String, m1 Int32) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_prewhere', 'r1') order by id unique key id;
INSERT INTO test.unique_prewhere VALUES (10001, 'BJ', 10), (10002, 'SH', 20), (10003, 'BJ', 30), (10004, 'SH',40), (10005, 'BJ', 50), (10006, 'BJ', 60);
INSERT INTO test.unique_prewhere VALUES (10004, 'SH', 400), (10005, 'BJ', 500);

-- FIXME (UNIQUE KEY): uncomment these after done: src/Storages/MergeTree/MergeTreeRangeReader.cpp:1034
-- select 'prewhere-only', s from test.unique_prewhere prewhere s='BJ' order by s;
-- select 'where-only', id, s, m1 from test.unique_prewhere where s='BJ' order by id settings optimize_move_to_prewhere = 0;
-- select 'both', id, s, m1 from test.unique_prewhere prewhere s='BJ' where m1 >= 30 order by id settings optimize_move_to_prewhere = 0;
-- select 'filter-optimized', id, s, m1 from test.unique_prewhere prewhere s='SH' where m1 >= 30 order by id settings optimize_move_to_prewhere = 0;
-- select 'all-filtered', sum(m1) from test.unique_prewhere prewhere s='NYK';
-- select 'all-filtered-where', sum(m1) from test.unique_prewhere where s='NYK' settings optimize_move_to_prewhere = 0;
-- select 'none-filtered', sum(m1) from test.unique_prewhere prewhere m1 < 1000;
-- select 'none-filtered-where', sum(m1) from test.unique_prewhere where m1 < 1000 settings optimize_move_to_prewhere = 0;
-- select 'const-true', sum(m1) from test.unique_prewhere prewhere 1;
-- select 'const-false', sum(m1) from test.unique_prewhere prewhere 0;

-- create table test.unique_prewhere_large (id Int32, val Int32, granule Int32) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_prewhere_large', 'r1') order by id unique key id SETTINGS index_granularity=8192;
-- INSERT INTO test.unique_prewhere_large select number, number, number / 8192 from system.numbers limit 100000;
-- select 'large', sum(val) from test.unique_prewhere_large;
-- select 'large-90%', sum(val) from test.unique_prewhere_large prewhere (val % 10) > 0;
-- INSERT INTO test.unique_prewhere_large select number, 0, number / 8192  from (select number from system.numbers limit 100000) where number % 3 == 0;
-- select 'large-60%', sum(val) from test.unique_prewhere_large prewhere (val % 10) > 0;
-- INSERT INTO test.unique_prewhere_large select number, 0, number / 8192  from (select number from system.numbers limit 100000) where number % 7 == 0;
-- select 'large-50%', sum(val) from test.unique_prewhere_large prewhere (val % 10) > 0;
-- INSERT INTO test.unique_prewhere_large select number, 0, number / 8192  from (select number from system.numbers limit 100000) where number % 8 == 0;
-- select 'large-40%', sum(val) from test.unique_prewhere_large prewhere (val % 10) > 0;
-- INSERT INTO test.unique_prewhere_large select number, 0, granule from (select number, toInt32(number / 8192) as granule from system.numbers limit 100000) where granule in (7, 8);
-- select 'large-final', sum(val) from test.unique_prewhere_large prewhere (val % 10) > 0;

-- -- set index_granularity = 4 to test granule skipping using delete bitmap
-- -- set preferred_block_size_bytes = 1 to read one row at a time
-- select 'test unique_prewhere_3';
-- create table test.unique_prewhere_3 (c1 Int64, c2 Int64, c3 String) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_prewhere_3', 'r1') order by c1 unique key c1 SETTINGS index_granularity=4;
-- insert into test.unique_prewhere_3 select number, number, '0123456789' from system.numbers limit 10;
-- select count(1), sum(c1 = c2), sum(length(c3)) from test.unique_prewhere_3;
-- select 'update the first and last two rows';
-- insert into test.unique_prewhere_3 values (0, 1, '0123456789'), (1, 2, '0123456789'), (8, 9, '0123456789'), (9, 10, '0123456789');
-- select 'normal read', count(1), sum(c1 = c2), sum(length(c3)) from test.unique_prewhere_3 prewhere c2 < 100;
-- select 'small read', count(1), sum(c1 = c2), sum(length(c3)) from test.unique_prewhere_3 prewhere c2 < 100 settings preferred_block_size_bytes = 1;
-- select 'update the first granule';
-- insert into test.unique_prewhere_3 select number, number + 1, '0123456789' from system.numbers limit 5;
-- select 'normal read', count(1), sum(c1 = c2), sum(length(c3)) from test.unique_prewhere_3 prewhere c2 < 100;
-- select 'small read', count(1), sum(c1 = c2), sum(length(c3)) from test.unique_prewhere_3 prewhere c2 < 100 settings preferred_block_size_bytes = 1;
-- select 'normal read: bitmap filter union prewhere filter';
-- select * from test.unique_prewhere_3 prewhere c2 % 2 = 1 order by c1;
-- select 'small read: bitmap filter union prewhere filter';
-- select * from test.unique_prewhere_3 prewhere c2 % 2 = 1 order by c1 settings preferred_block_size_bytes = 1;


drop table if exists test.unique_prewhere;
drop table if exists test.unique_prewhere_large;
drop table if exists test.unique_prewhere_3;
