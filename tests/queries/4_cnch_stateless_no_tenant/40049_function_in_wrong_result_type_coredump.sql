CREATE DATABASE IF NOT EXISTS `test`;
DROP TABLE IF EXISTS `test`.`t40049`;

SET dialect_type='CLICKHOUSE';
SET enable_optimizer=1;

CREATE TABLE `test`.`t40049`(
    `event_time` DateTime,
    `query` Nullable(String)
) ENGINE = CnchMergeTree
ORDER BY (`event_time`)
SETTINGS index_granularity = 8192;

INSERT INTO `test`.`t40049` (query, event_time) VALUES ('ss', '2023-06-13 02:59:38');

SELECT
    key1,
    key2,
    key3
FROM `test`.`t40049`
GROUP BY
extract(query, 'xxx') IN ('1','2','3','4','5','6') as key1,
formatDateTime(
    toStartOfInterval(event_time, INTERVAL 1 day),
    '%Y-%m-%d'
) as key2,
replaceAll(
    extract(
        replaceAll(query, 'xx', ''),
        'xxx'
    ),
    '_local',
    ''
) as key3;

DROP TABLE IF EXISTS `test`.`t40049`;
