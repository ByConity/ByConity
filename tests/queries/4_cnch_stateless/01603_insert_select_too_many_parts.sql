DROP TABLE IF EXISTS too_many_parts;

CREATE TABLE too_many_parts (date Date, x UInt64)
ENGINE = CnchMergeTree PARTITION BY date ORDER BY x
SETTINGS parts_to_throw_insert = 5, insert_check_parts_interval = 1;

SYSTEM STOP MERGES too_many_parts;

SET max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

INSERT INTO too_many_parts SELECT '2024-01-01' as date, number as x FROM numbers(10);
SELECT count() FROM too_many_parts;

SELECT sleepEachRow(1) FROM numbers(2) FORMAT Null;
-- exception as parts number in the partition (10) is larger than parts_to_throw_insert (5)
INSERT INTO too_many_parts SELECT '2024-01-01' as date, number as x FROM numbers(10); -- { serverError 252 }

ALTER TABLE too_many_parts MODIFY SETTING parts_to_throw_insert = 25;

INSERT INTO too_many_parts SELECT '2024-01-01' as date, number as x FROM numbers(10);
SELECT count() FROM too_many_parts;

-- Check max_parts_in_total
ALTER TABLE too_many_parts MODIFY SETTING max_parts_in_total = 25;

INSERT INTO too_many_parts SELECT '2024-01-02' as date, number as x FROM numbers(10);
SELECT count() FROM too_many_parts;

SELECT sleepEachRow(1) FROM numbers(2) FORMAT Null;
-- exception as total parts number (30) is larger than parts_to_throw_insert (25)
INSERT INTO too_many_parts SELECT '2024-01-02' as date, number as x FROM numbers(10); -- { serverError 252 }

ALTER TABLE too_many_parts MODIFY SETTING max_parts_in_total = 50;
INSERT INTO too_many_parts SELECT '2024-01-02' as date, number as x FROM numbers(10);
SELECT count() FROM too_many_parts;

DROP TABLE too_many_parts;
