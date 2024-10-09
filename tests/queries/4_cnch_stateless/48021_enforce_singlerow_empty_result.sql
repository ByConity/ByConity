drop TABLE if exists fensishu;

CREATE TABLE fensishu (`createdTime` DateTime, `total` Int64) ENGINE = CnchMergeTree PRIMARY KEY `createdTime` ORDER BY `createdTime` SETTINGS cnch_server_vw = 'server_vw_default', index_granularity = 8192;

SELECT count(`jintianrenshu`)
FROM
    (
        SELECT
            max(`total`) AS jintianrenshu,
            toDate(`createdTime`) AS shijian,

            (
                SELECT max(`total`)
                FROM `fensishu`
                WHERE toDate(`createdTime`) = addDays(today(), -30)
                GROUP BY toDate(`createdTime`)
            ) AS sanshitianrenshu,

            (
                SELECT max(`total`)
                FROM `fensishu`
                WHERE toDate(`createdTime`) = addDays(today(), -60)
                GROUP BY toDate(`createdTime`)
            ) AS liushitianrenshu
        FROM `fensishu`
        WHERE `shijian` = today()
        GROUP BY toDate(`createdTime`)
    )
LIMIT 1000;

drop TABLE if exists fensishu;
