DROP TABLE IF EXISTS db;

CREATE TABLE tb
(
    date Date,
    `index` Int32,
    value Int32,
    idx Int32 ALIAS `index`
)
ENGINE = CnchMergeTree
PARTITION BY date
ORDER BY (date, `index`)
SETTINGS enable_late_materialize = 1;

insert into tb values ('2017-12-15', 1, 1);

SET force_primary_key = 1;

select * from tb where `index` >= 0 AND `index` <= 2;
select * from tb where idx >= 0 AND idx <= 2;

DROP TABLE tb;
