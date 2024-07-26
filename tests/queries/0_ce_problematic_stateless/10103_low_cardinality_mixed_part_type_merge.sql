DROP TABLE if exists broken_part;
CREATE TABLE broken_part
(
    event_date Date,
    lc LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date)
SETTINGS min_bytes_for_wide_part = 1048576;

insert into broken_part select '2023-10-01', number from system.numbers limit 100000;
insert into broken_part select '2023-10-01', number from system.numbers limit 100000;
optimize table broken_part final;
insert into broken_part select '2023-10-01', number from system.numbers limit 1;

SELECT name, part_type, rows, lowCardinalityIsNoneEncoded(lc) lcet
FROM broken_part a, system.parts b
WHERE b.table = 'broken_part' AND _part = name
GROUP BY name, part_type, rows, lcet
ORDER BY name;

optimize table broken_part final;

SELECT name, part_type, rows, lowCardinalityIsNoneEncoded(lc) lcet
FROM broken_part a, system.parts b
WHERE b.table = 'broken_part' AND _part = name
GROUP BY name, part_type, rows, lcet
ORDER BY name;

select count(event_date), count(lc) from broken_part;

DROP TABLE if exists broken_part;
