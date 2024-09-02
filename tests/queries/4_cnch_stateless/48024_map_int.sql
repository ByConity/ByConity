DROP TABLE IF EXISTS abtest_live_platform_10_min;
CREATE TABLE abtest_live_platform_10_min (
    `st` Int64, 
    `gmv` Float64, 
    `liveWindowSt` Int64,
    `isEcomLong` Int64, 
    `actions` Map(Int32, Int64), 
    `uid` String, 
    `ab_version` Array(Int64) BLOOM, 
    `channel_id` Int64, 
    `si_time` Int64, 
    `scene` Array(Int64)
) ENGINE = CnchMergeTree 
ORDER BY (uid, channel_id, cityHash64(uid))
SETTINGS index_granularity = 8192;

insert into abtest_live_platform_10_min values (1,1,1,1,{1:1,-1:-1},'1', [1],1,1,[1]);

SELECT sum(actions{1}) AS impr, sum(actions{-1}) AS show 
FROM abtest_live_platform_10_min
WHERE arraySetCheck(ab_version, 1);

