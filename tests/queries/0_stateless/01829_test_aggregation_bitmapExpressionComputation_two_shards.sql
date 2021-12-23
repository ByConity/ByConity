drop table if exists test.test_bitmap64_all;
drop table if exists test.test_bitmap64;

create table test.test_bitmap64 (p_date Date, tag_id Int64, uids BitMap64, shard_id Int64) engine = HaMergeTree('/clickhouse/tables/test/test_bitmap64', '1') partition by (p_date, shard_id) order by tag_id settings index_granularity = 128;
create table test.test_bitmap64_all (p_date Date, tag_id Int64, uids BitMap64, shard_id Int64) engine = Distributed(test_cluster_two_shards_localhost, 'test', 'test_bitmap64');

insert into table test.test_bitmap64 values ('2019-01-01', 1, [1], 1);
insert into table test.test_bitmap64 values ('2019-01-01', 2, [1, 2], 1);
insert into table test.test_bitmap64 values ('2019-01-01', 3, [1, 2, 3], 2);
insert into table test.test_bitmap64 values ('2019-01-01', 4, [1, 2, 3, 4], 2);
insert into table test.test_bitmap64 values ('2019-01-01', 5, [1], 3);
insert into table test.test_bitmap64 values ('2019-01-01', 6, [2], 3);
insert into table test.test_bitmap64 values ('2019-01-01', 7, [3], 4);
insert into table test.test_bitmap64 values ('2019-01-01', 8, [4], 4);
insert into table test.test_bitmap64 values ('2019-01-01', 9, [1, 2, 3, 4], 5);
insert into table test.test_bitmap64 values ('2019-01-01', 10, [1, 2, 3, 4, 5], 5);

select bitmapCount('1 | 2')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 & 2')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 | 2 | 3')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 | 2 & 3')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 | (2 & 3)')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 | (2 & 3) | 4')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 | 2 | 3 | 4 & 5')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 & ( 2 | 3 | 4 ) & 5')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 | 2 | 3 | 4  | 5 & 6')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 & ( 2 | 3 | 4  | 5) & 6')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 & ( 2 | 3) & (4 | 5) & (6 | 7)')(tag_id, uids) from test.test_bitmap64_all;

select bitmapCount('1')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('2 ~ 1')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 | 2 | 3 ~ 2')(tag_id, uids) from test.test_bitmap64_all;
select bitmapCount('1 | 2 | ( 3 ~ 2 )')(tag_id, uids) from test.test_bitmap64_all;

select bitmapExtract('1 | 2')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 & 2')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 | 2 | 3')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 | 2 & 3')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 | (2 & 3)')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 | (2 & 3) | 4')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 | 2 | 3 | 4 & 5')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 & ( 2 | 3 | 4 ) & 5')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 | 2 | 3 | 4  | 5 & 6')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 & ( 2 | 3 | 4  | 5) & 6')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 & ( 2 | 3) & (4 | 5) & (6 | 7)')(tag_id, uids) from test.test_bitmap64_all;

select bitmapExtract('1')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('2 ~ 1')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 | 2 | 3 ~ 2')(tag_id, uids) from test.test_bitmap64_all;
select bitmapExtract('1 | 2 | ( 3 ~ 2 )')(tag_id, uids) from test.test_bitmap64_all;

select bitmapMultiCount('1', '_1|2', '_2&3')(tag_id, uids) from test.test_bitmap64_all;
select bitmapMultiCount('2', '3', '2&3', '2|3', '_3|_4')(tag_id, uids) from test.test_bitmap64_all;

drop table if exists test.test_bitmap64_all;
drop table if exists test.test_bitmap64;