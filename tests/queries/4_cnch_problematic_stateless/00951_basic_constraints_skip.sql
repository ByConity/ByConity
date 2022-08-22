
DROP TABLE IF EXISTS test_constraints;
CREATE TABLE test_constraints
(
        a       UInt32,
        b       UInt32,
        CONSTRAINT b_constraint CHECK b > 0
)
ENGINE = CnchMergeTree ORDER BY (a);

SET constraint_skip_violate=1;

INSERT INTO test_constraints VALUES (1, 2);
SELECT * FROM test_constraints ORDER BY a;

INSERT INTO test_constraints VALUES (3, 4), (1, 0);
SELECT * FROM test_constraints ORDER BY a;

DROP TABLE test_constraints;

CREATE TABLE test_constraints
(
        a       UInt32,
        b       UInt32,
        CONSTRAINT b_constraint CHECK b > 10,
        CONSTRAINT a_constraint CHECK a < 10
)
ENGINE = CnchMergeTree ORDER BY (a);

INSERT INTO test_constraints VALUES (1, 2);
SELECT * FROM test_constraints ORDER BY a;

INSERT INTO test_constraints VALUES (5, 16), (10, 11);
SELECT * FROM test_constraints ORDER BY a;

INSERT INTO test_constraints VALUES (7, 18), (0, 11);
SELECT * FROM test_constraints ORDER BY a;

DROP TABLE test_constraints;

CREATE TABLE test_constraints
(
        a       UInt32,
        b       UInt32,
        CONSTRAINT b_constraint CHECK b > 10,
        CONSTRAINT a_constraint CHECK a < 10,
        CONSTRAINT a_constraint2 CHECK a > 5
)
ENGINE = CnchMergeTree ORDER BY (a);

INSERT INTO test_constraints VALUES (5, 11), (6, 12), (6, 10), (10, 12);
SELECT * FROM test_constraints ORDER BY a;

DROP TABLE test_constraints;
