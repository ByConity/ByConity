drop table if exists ods_video_search_word_rank_tend_data_dcc;
CREATE TABLE ods_video_search_word_rank_tend_data_dcc
(
    `uid` String COMMENT 'dcc用户Id',
    `video_id` String COMMENT '视频Id',
    `key_word` String COMMENT '搜索词',
    `rank` Nullable(Int64) COMMENT '排名',
    `pt` Int64 COMMENT '更新时间',
    `ip` Nullable(String) COMMENT '请求排名ip列表多个使用,分割'
)
ENGINE = CnchMergeTree
ORDER BY (uid, video_id, key_word, pt)
UNIQUE KEY (uid, video_id, key_word, pt)
SETTINGS partition_level_unique_keys = 0, storage_policy = 'cnch_default_hdfs', allow_nullable_key = 1, storage_dialect_type = 'MYSQL', index_granularity = 8192
COMMENT '视频关键词趋势图';

insert into ods_video_search_word_rank_tend_data_dcc(uid, pt) values('abc', 20240701);

set dialect_type='MYSQL';
SELECT if(length(pt) = 13, pt, unix_timestamp(pt) * 1000) FROM ods_video_search_word_rank_tend_data_dcc;
SELECT uid from ods_video_search_word_rank_tend_data_dcc where pt != '';

drop table ods_video_search_word_rank_tend_data_dcc;
