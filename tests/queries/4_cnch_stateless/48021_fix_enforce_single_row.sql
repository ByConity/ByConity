drop TABLE if exists fensishu_local;

CREATE TABLE fensishu_local (`createdTime` DateTime, `total` Int64) ENGINE = CnchMergeTree PRIMARY KEY `createdTime` ORDER BY `createdTime`;
SELECT count(`jintianrenshu`)
FROM
    (
        SELECT
            max(`total`) AS jintianrenshu,
            toDate(`createdTime`) AS shijian,

            (
                SELECT max(`total`)
                FROM `fensishu_local`
                WHERE toDate(`createdTime`) = addDays(today(), -30)
                GROUP BY toDate(`createdTime`)
            ) AS sanshitianrenshu,

            (
                SELECT max(`total`)
                FROM `fensishu_local`
                WHERE toDate(`createdTime`) = addDays(today(), -60)
                GROUP BY toDate(`createdTime`)
            ) AS liushitianrenshu
        FROM `fensishu_local`
        WHERE `shijian` = today()
        GROUP BY toDate(`createdTime`)
    )
LIMIT 1000;

drop TABLE if exists fensishu_local;
