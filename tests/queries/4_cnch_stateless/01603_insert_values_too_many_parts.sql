DROP TABLE IF EXISTS insert_values_too_many_parts;

CREATE TABLE insert_values_too_many_parts (x UInt64)
ENGINE = CnchMergeTree ORDER BY x
SETTINGS parts_to_throw_insert = 2, insert_check_parts_interval = 1;

SYSTEM STOP MERGES insert_values_too_many_parts;

INSERT INTO insert_values_too_many_parts VALUES (0);
INSERT INTO insert_values_too_many_parts VALUES (1);
SELECT count() FROM insert_values_too_many_parts;

SELECT sleepEachRow(1) FROM numbers(2) FORMAT Null;
INSERT INTO insert_values_too_many_parts VALUES (2); -- { serverError 252 }

ALTER TABLE insert_values_too_many_parts MODIFY SETTING parts_to_throw_insert = 10;
INSERT INTO insert_values_too_many_parts VALUES (2);
SELECT count() FROM insert_values_too_many_parts;

-- Check max_parts_in_total
ALTER TABLE insert_values_too_many_parts MODIFY SETTING max_parts_in_total = 5;

INSERT INTO insert_values_too_many_parts VALUES (3);
INSERT INTO insert_values_too_many_parts VALUES (4);
SELECT count() FROM insert_values_too_many_parts;

SELECT sleepEachRow(1) FROM numbers(2) FORMAT Null;
INSERT INTO insert_values_too_many_parts VALUES (5); -- { serverError 252 }

ALTER TABLE insert_values_too_many_parts MODIFY SETTING max_parts_in_total = 10;
INSERT INTO insert_values_too_many_parts VALUES (5);
SELECT count() FROM insert_values_too_many_parts;

DROP TABLE IF EXISTS insert_values_too_many_parts;