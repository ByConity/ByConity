DROP TABLE IF EXISTS test_a;
DROP TABLE IF EXISTS test_b;

CREATE TABLE test_a
(
    OldColumn String DEFAULT '',
    EventDate Date DEFAULT toDate(EventTime),
    EventTime DateTime
) ENGINE = CnchMergeTree PARTITION BY toMonth(EventDate) ORDER BY EventTime SETTINGS enable_late_materialize = 1;

CREATE TABLE test_b
(
    OldColumn String DEFAULT '',
    NewColumn String DEFAULT '',
    EventDate Date DEFAULT toDate(EventTime),
    EventTime DateTime
) ENGINE = CnchMergeTree PARTITION BY toMonth(EventDate) ORDER BY EventTime SETTINGS enable_late_materialize = 1;

INSERT INTO test_a (OldColumn, EventTime) VALUES('1', now());

INSERT INTO test_b (OldColumn, NewColumn, EventTime) VALUES('1', '1a', now());
INSERT INTO test_b (OldColumn, NewColumn, EventTime) VALUES('2', '2a', now());

ALTER TABLE test_a ADD COLUMN NewColumn String DEFAULT '' AFTER OldColumn;

INSERT INTO test_a (OldColumn, NewColumn, EventTime) VALUES('2', '2a', now());

SELECT NewColumn
FROM test_a
INNER JOIN
(SELECT OldColumn, NewColumn FROM test_b) s
Using OldColumn
WHERE NewColumn != '';

DROP TABLE test_a;
DROP TABLE test_b;
