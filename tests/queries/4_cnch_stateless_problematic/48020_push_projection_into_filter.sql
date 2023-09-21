select arraySetCheck(arr, 1)
from (select *
      from (select dummy + 1 as id, [dummy, dummy + 1] as arr from system.one)
      where id > 0);

drop TABLE if exists life_scenes_data_realtime_v2;
CREATE TABLE if not exists life_scenes_data_realtime_v2
(
    `uuid`                             UUID,
    `p_date`                           Date,
    `score`                            Nullable(Float64),
    `vid_array`                        Array(Int32) BLOOM
) ENGINE = CnchMergeTree PARTITION BY toDate(p_date) ORDER BY uuid;

SELECT DISTINCT (
                    case
                        when arraySetCheck(vid_array, 6110241) then 6110241
                        when arraySetCheck(vid_array, 6110242) then 6110242
                        when arraySetCheck(vid_array, 6110243) then 6110243
                        end as v_id_abc
                    ) AS _1700028133846
FROM (select `vid_array` as `vid_array`,
             `score`     as `score`,
             `p_date`    as `p_date`
      from life_scenes_data_realtime_v2
      where (
                    (p_date >= '2023-05-09')
                    AND (p_date <= '2023-05-09')
                )) LIMIT 1000;

drop TABLE if exists life_scenes_data_realtime_v2;
