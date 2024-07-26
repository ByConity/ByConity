DROP TABLE if exists broken_part;
CREATE TABLE broken_part
(
    event_date Date,
    lc1 LowCardinality(String),
    lc2 LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, lc1)
SETTINGS min_bytes_for_wide_part = 1048576;

insert into broken_part select '2023-10-01', number, number from system.numbers limit 100000;
insert into broken_part select '2023-10-01', number, number from system.numbers limit 100000;
optimize table broken_part final;
insert into broken_part select '2023-10-01', number, number from system.numbers limit 1;

SELECT name, part_type, rows, lowCardinalityIsNoneEncoded(lc1) lcet1, lowCardinalityIsNoneEncoded(lc2) lcet2
FROM broken_part a, system.parts b
WHERE b.table = 'broken_part' AND _part = name
GROUP BY name, part_type, rows, lcet1, lcet2
ORDER BY name;

optimize table broken_part final;

SELECT name, part_type, rows, lowCardinalityIsNoneEncoded(lc1) lcet1, lowCardinalityIsNoneEncoded(lc2) lcet2
FROM broken_part a, system.parts b
WHERE b.table = 'broken_part' AND _part = name
GROUP BY name, part_type, rows, lcet1, lcet2
ORDER BY name;

select count(event_date), count(lc1), count(lc2) from broken_part;

DROP TABLE if exists broken_part;
