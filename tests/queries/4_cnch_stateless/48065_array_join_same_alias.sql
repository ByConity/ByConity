DROP TABLE IF EXISTS arrays_test;
CREATE TABLE arrays_test
(
    s String NOT NULL,
    arr Array(UInt8)
) ENGINE = CnchMergeTree() order by s;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);

SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr AS arr2
SETTINGS enable_optimizer = 0;
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr AS arr
SETTINGS enable_optimizer = 0;
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr
SETTINGS enable_optimizer = 0;
select '---';
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr AS arr2
SETTINGS enable_optimizer = 1;
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr AS arr
SETTINGS enable_optimizer = 1;
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr
SETTINGS enable_optimizer = 1;
