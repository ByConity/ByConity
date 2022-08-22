SET send_logs_level = 'none';
SET role_id = 0;



DROP TABLE IF EXISTS col_masking_view;
DROP TABLE IF EXISTS col_masking_m_view;
DROP TABLE IF EXISTS col_masking;
DROP TABLE IF EXISTS col_masking2;

DROP MASKING POLICY IF EXISTS credit_card_mask_test;

CREATE MASKING POLICY credit_card_mask_test AS (val String) -> CASE WHEN current_role() = 0 THEN concat('**', substr(val, 1, 3), '**') ELSE val END;
CREATE TABLE col_masking (`name` String, `age` Int64, `credit_card_num` String) ENGINE = CnchMergeTree() PARTITION BY `name` PRIMARY KEY `name` ORDER BY `name`;
CREATE TABLE col_masking2 (`name` String, `age` Int64, `credit_card_num` String MASK credit_card_mask_test) ENGINE = CnchMergeTree() PARTITION BY `name` PRIMARY KEY `name` ORDER BY `name`;

-- Ensure masking is applied to all values in the target column.
INSERT INTO col_masking VALUES ('Jane', 18, '1234-5678-9123'), ('Bob', 24, '8765-5678-9123');
SELECT * FROM col_masking ORDER BY name FORMAT CSV;
ALTER TABLE col_masking MODIFY COLUMN credit_card_num SET MASKING POLICY credit_card_mask_test;
SELECT * FROM col_masking ORDER BY name FORMAT CSV;

-- WHERE clause
-- No result should be printed
SELECT * FROM col_masking WHERE credit_card_num = '1234-5678-9123' ORDER BY name FORMAT CSV;
-- Result should be printed for these SELECT queries
SELECT * FROM col_masking WHERE credit_card_num = '**123**' ORDER BY name FORMAT CSV;
SELECT credit_card_num FROM col_masking WHERE credit_card_num = '**123**' ORDER BY name FORMAT CSV;

-- VIEW queries
CREATE VIEW col_masking_view AS SELECT credit_card_num FROM col_masking WHERE credit_card_num LIKE '**%**';
SELECT * FROM col_masking_view ORDER BY credit_card_num;
-- MATERIALIZED VIEW stores underlying data. Masking is applied on top of data and hence results will not be masked.
CREATE MATERIALIZED VIEW col_masking_m_view TO col_masking AS SELECT credit_card_num FROM col_masking WHERE credit_card_num LIKE '**%**';
SELECT * FROM col_masking_m_view ORDER BY credit_card_num;

-- JOIN queries
INSERT INTO col_masking2 VALUES ('Doe', 17, '1234-5678-9123');
SELECT * FROM col_masking2 JOIN col_masking ON col_masking2.credit_card_num = col_masking.credit_card_num FORMAT CSV;

-- WITH queries
-- WITH credit_card_num as card SELECT name, age, card, credit_card_num FROM col_masking FORMAT CSV;
-- "Bob",24,"**876**","**876**"
-- "Jane",18,"**123**","**123**"

-- Subqueries
SELECT * FROM col_masking WHERE credit_card_num IN (SELECT credit_card_num FROM col_masking2) ORDER BY name FORMAT CSV;
SELECT * FROM col_masking WHERE credit_card_num IN (SELECT credit_card_num FROM col_masking) ORDER BY name FORMAT CSV;

-- ALTER queries
ALTER MASKING POLICY credit_card_mask_test AS (val String) -> CASE WHEN 123 IN (123, 345) THEN concat('$$', substr(val, 1, 3), '$$') ELSE val END;
SELECT * FROM col_masking ORDER BY name FORMAT CSV;
SELECT * FROM col_masking2 ORDER BY name FORMAT CSV;

-- RENAME DATABASE query
RENAME DATABASE test to test1;
SELECT * FROM test1.col_masking ORDER BY name FORMAT CSV;
SELECT * FROM test1.col_masking2 ORDER BY name FORMAT CSV;
RENAME DATABASE test1 to test;

-- UNSET queries
ALTER TABLE col_masking MODIFY COLUMN credit_card_num UNSET MASKING POLICY;
ALTER TABLE col_masking2 MODIFY COLUMN credit_card_num UNSET MASKING POLICY;
SELECT * FROM col_masking ORDER BY name FORMAT CSV;
SELECT * FROM col_masking2 ORDER BY name FORMAT CSV;

-- DROP POLICIES
DROP MASKING POLICY credit_card_mask_test;

DROP TABLE col_masking_view;
DROP TABLE col_masking_m_view;
DROP TABLE col_masking;
DROP TABLE col_masking2;

SELECT '---row policy---';
DROP TABLE IF EXISTS row_policy;
DROP TABLE IF EXISTS row_policy2;
DROP TABLE IF EXISTS row_policy3;
DROP ROW ACCESS POLICY IF EXISTS rp;
DROP ROW ACCESS POLICY IF EXISTS lambda_rp;

CREATE ROW ACCESS POLICY rp AS (x UInt64, y UInt64) -> CASE WHEN current_role() = 1 THEN x < 3 WHEN current_role() = 2 THEN y < 3 ELSE 1 END;
CREATE ROW ACCESS POLICY lambda_rp AS (x Array(UInt64)) -> CASE WHEN empty(arrayFilter(x -> x > 10, x)) THEN 1 ELSE 0 END;
CREATE TABLE row_policy (v UInt64, w UInt64) Engine = CnchMergeTree ORDER BY v;
CREATE TABLE row_policy2 (a UInt64, b UInt64) Engine = CnchMergeTree ORDER BY a;
CREATE TABLE row_policy3 (arr Array(UInt64)) Engine = CnchMergeTree ORDER BY arr;

ALTER TABLE row_policy SET ROW ACCESS POLICY rp ON (v, w);
ALTER TABLE row_policy3 SET ROW ACCESS POLICY lambda_rp ON (arr);
INSERT INTO TABLE row_policy VALUES (1, 5), (4, 2);
INSERT INTO TABLE row_policy2 VALUES (1, 1), (2, 2);
INSERT INTO TABLE row_policy3 VALUES ([1,2,3]), ([10,11,12]);

SELECT v, w FROM row_policy FORMAT CSV;

SET role_id = 1;
SELECT v, w FROM row_policy FORMAT CSV;
SELECT * FROM row_policy JOIN row_policy2 ON row_policy.v = row_policy2.a FORMAT CSV;

SET role_id = 2;
SELECT v, w FROM row_policy FORMAT CSV;

SELECT * FROM row_policy3 FORMAT CSV;

ALTER TABLE row_policy UNSET ROW ACCESS POLICY;

DROP TABLE row_policy;
DROP TABLE row_policy2;
DROP TABLE row_policy3;
DROP ROW ACCESS POLICY rp;
DROP ROW ACCESS POLICY lambda_rp;
