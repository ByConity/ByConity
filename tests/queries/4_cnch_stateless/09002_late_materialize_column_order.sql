DROP TABLE IF EXISTS prewhere;

CREATE TABLE prewhere (d Date, a String, b String) ENGINE = CnchMergeTree PARTITION BY toMonth(d) ORDER BY d SETTINGS enable_late_materialize = 1;
INSERT INTO prewhere VALUES ('2015-01-01', 'hello', 'world');

ALTER TABLE prewhere ADD COLUMN a1 String AFTER a;
INSERT INTO prewhere VALUES ('2015-01-01', 'hello1', 'xxx', 'world1');

SELECT d, a, a1, b FROM prewhere WHERE a LIKE 'hello%' ORDER BY a1;

DROP TABLE prewhere;
