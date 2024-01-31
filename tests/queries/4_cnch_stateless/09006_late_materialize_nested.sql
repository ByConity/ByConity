DROP TABLE IF EXISTS nested;

CREATE TABLE nested (x UInt64, filter UInt8, n Nested(a UInt64)) ENGINE = CnchMergeTree ORDER BY x SETTINGS enable_late_materialize = 1;
INSERT INTO nested SELECT number, number % 2, range(number % 10) FROM system.numbers LIMIT 100000;

ALTER TABLE nested ADD COLUMN n.b Array(UInt64);
SELECT DISTINCT n.b FROM nested WHERE filter settings enable_optimizer = 0; --When the enable_optimizer , the order of the results returned changes @wanghaoyu

ALTER TABLE nested ADD COLUMN n.c Array(UInt64) DEFAULT arrayMap(x -> x * 2, n.a);
SELECT DISTINCT n.c FROM nested WHERE filter settings enable_optimizer = 0; --When the enable_optimizer , the order of the results returned changes @wanghaoyu
SELECT DISTINCT n.a, n.c FROM nested WHERE filter settings enable_optimizer = 0; --When the enable_optimizer , the order of the results returned changes @wanghaoyu

DROP TABLE nested;
