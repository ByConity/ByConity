

DROP TABLE IF EXISTS alter;

CREATE TABLE alter (x Int32, y Int32, p_date Date) Engine = CnchMergeTree PARTITION BY p_date ORDER BY y;
INSERT INTO alter VALUES (1, 0, '20210222');
ALTER TABLE alter MODIFY COLUMN x String;
SELECT * FROM alter ORDER BY y;

INSERT INTO alter VALUES ('a', 1, '20210222'), ('2', 2, '20210222');
ALTER TABLE alter MODIFY COLUMN x Int32;
SELECT * FROM alter ORDER BY y;

ALTER TABLE alter MODIFY COLUMN x Nullable(String);
INSERT INTO alter VALUES (Null, 3, '20210222');
ALTER TABLE alter MODIFY COLUMN x Decimal32(2);
SELECT * FROM alter ORDER BY y;

-- Can't convert.
ALTER TABLE alter MODIFY COLUMN x Array(String); -- { serverError 53 }
ALTER TABLE alter MODIFY COLUMN x FixedString(10); -- { serverError 48 }

ALTER TABLE alter add column z Nullable(String) AFTER y;
SELECT * FROM alter ORDER BY y;

INSERT INTO alter (x, y, z, p_date) values (3.14, 4, 'test', '20210222');
SELECT sleep(1) format Null;  -- maybe do merge parts
SELECT * FROM alter ORDER BY y;

DROP TABLE alter;
