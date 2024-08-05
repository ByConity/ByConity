create database if not exists dict_db;
create database if not exists bitmap_db;

create table if not exists dict_db.tag_bitmaps_did_cdp_dict_20014 (`key` UInt64, `value` UInt64, `split_id` UInt64,  BITENGINE_CONSTRAINT key_constraint CHECK toUInt64(intHash64(`key`) % 2)) ENGINE = CnchMergeTree CLUSTER BY `split_id` INTO 2 BUCKETS PRIMARY KEY `key` ORDER BY `key`;

CREATE TABLE if not exists bitmap_db.tag_bitmaps_did_cdp_20014 (`split_id` UInt64, `tag_id` Int32, `p_date` Date, `tag_value_double` Float64, `tag_value` String, `id_map_cnt` UInt64, `id_type` Int32, `id_map` BitMap64 BitEngineEncode, `app_id` Int32, `tag_type` Int8) ENGINE = CnchMergeTree PARTITION BY (toDate(toStartOfDay(`p_date`)), `tag_id`, `tag_type`, `id_type`) CLUSTER BY `split_id` INTO 2 BUCKETS PRIMARY KEY (`tag_value`, `tag_value_double`, cityHash64(`tag_value`)) ORDER BY (`tag_value`, `tag_value_double`, cityHash64(`tag_value`)) SETTINGS underlying_dictionary_tables = '{"id_map":"`dict_db`.`tag_bitmaps_did_cdp_dict_20014`"}';

insert into bitmap_db.tag_bitmaps_did_cdp_20014 select 0, 1014834, '2024-03-14', 0, 'aaa', 2, 1358, arrayToBitmap([48,66]), 0, 2;

select DecodeBitmap(id_map, 'bitmap_db', 'tag_bitmaps_did_cdp_20014', 'id_map')
from (
    select id_map from bitmap_db.tag_bitmaps_did_cdp_20014 where tag_id = 1014834
);

SELECT *
FROM
(
    SELECT toUInt64(base_id) AS base_id
    FROM
    (
        SELECT
            toUInt64(0) AS base_id,
            map('', '') AS string_map,
            map('', 0) AS bigint_map,
            map('', 0) AS double_map,
            map('', '') AS date_map,
            map('', '') AS datetime_map,
            map('', ['']) AS array_string_map,
            map('', [0]) AS array_bigint_map,
            map('', [0]) AS array_double_map,
            map('', ['']) AS array_date_map,
            map('', ['']) AS array_datetime_map,
            NULL AS id_type,
            NULL AS p_date
        FROM numbers(0)
    )
    WHERE (((p_date >= '2023-06-27') AND (p_date <= '2023-06-27')) AND (bigint_map{'5002743'} = 0)) AND (id_type = 1358)
) as l inner join (
select arrayJoin(bitmapToArrayWithDecode(id_map, 'bitmap_db', 'tag_bitmaps_did_cdp_20014', 'id_map')) as id
from (
    select bitmapExtract('0')(idx, id_map) as id_map, split_id
    from (
        select bitmapColumnOr(id_map) as id_map,
        toInt32(0) as idx,
        split_id
        from bitmap_db.tag_bitmaps_did_cdp_20014 where tag_id = 1014834
        group by split_id
    )
    group by split_id
) SETTINGS dict_table_full_mode = 1
    ) as r on l.base_id = r.id ORDER BY base_id, id;


drop table bitmap_db.tag_bitmaps_did_cdp_20014;
drop table dict_db.tag_bitmaps_did_cdp_dict_20014;
drop database bitmap_db;
drop database dict_db;