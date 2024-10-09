drop table if exists array_join_nullable_48027;
CREATE TABLE array_join_nullable_48027 (`hash_uid` UInt64, `data` Nullable(Array(String))) ENGINE = CnchMergeTree() CLUSTER BY cityHash64(hash_uid) INTO 4 BUCKETS ORDER BY hash_uid SETTINGS index_granularity = 8192;
insert into array_join_nullable_48027 values(3780948099449230809,NULL)(16497613678337237923, ['ss'])(5233239545758093304,NULL);

SELECT arrayJoin(data) AS a
FROM array_join_nullable_48027
GROUP BY a;

select x, count(*) from (select arrayJoin(data)as x from array_join_nullable_48027) group by x;
select sum(length(x)) from (select arrayJoin(data)as x from array_join_nullable_48027);
select sum(length(x)) from (select arrayJoin(data)as x from array_join_nullable_48027) SETTINGS enable_push_partial_agg=0;

drop table if exists array_join_nullable_48027;
