SELECT [[[[],[]]]];
SELECT [[1], []];
SELECT [[[[],['']]]];
SELECT concat([], ['Hello'], []);
SELECT arrayPushBack([], 1), arrayPushFront([[]], []);
USE test;
DROP TABLE IF EXISTS arr;
CREATE TABLE arr (x Array(String), y Nullable(String), z Array(Array(Nullable(String)))) 
ENGINE = CnchMergeTree ORDER BY x;
INSERT INTO arr SELECT [], NULL, [[], [NULL], [NULL, 'Hello']];
SELECT * FROM arr;
DROP TABLE arr;
