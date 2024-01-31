DROP TABLE IF EXISTS nested;

CREATE TABLE nested (x UInt64, filter UInt8, n Nested(a UInt64)) ENGINE = CnchMergeTree ORDER BY x;
INSERT INTO nested SELECT number, number % 2, range(number % 10) FROM system.numbers LIMIT 100000;

ALTER TABLE nested ADD COLUMN n.b Array(UInt64);
SELECT DISTINCT n.b FROM nested PREWHERE filter ORDER BY n.b;

ALTER TABLE nested ADD COLUMN n.c Array(UInt64) DEFAULT arrayMap(x -> x * 2, n.a);
SELECT DISTINCT n.c FROM nested PREWHERE filter ORDER BY n.c;
SELECT DISTINCT n.a, n.c FROM nested PREWHERE filter ORDER BY n.a;

DROP TABLE nested;
