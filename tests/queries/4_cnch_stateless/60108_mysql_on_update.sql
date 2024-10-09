set dialect_type='MYSQL';

DROP TABLE IF EXISTS 60108_mysql_create_ddl;
DROP TABLE IF EXISTS 60108_mysql_create_ddl1;
DROP TABLE IF EXISTS 60108_mysql_create_ddl2;
DROP TABLE IF EXISTS 60108_mysql_create_ddl3;

-- CURRENT_TIMESTAMP functionality
CREATE TABLE 60108_mysql_create_ddl
(
    `id` Int,
    `dt` DateTime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `dt2` DateTime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `dt3` DateTime DEFAULT '1970-01-01 10:00:00' ON UPDATE now(),
    `val` Int,
    PRIMARY KEY(id)
);

-- Implicit Conversion
CREATE TABLE 60108_mysql_create_ddl1
(
    `id` Int,
    `dt` Int DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `dt2` String DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `val` Int,
    PRIMARY KEY(id)
);

INSERT INTO 60108_mysql_create_ddl(id, val) VALUES (1, 10);
INSERT INTO 60108_mysql_create_ddl(id, val) VALUES (2, 20);

INSERT INTO 60108_mysql_create_ddl1(id, val) VALUES (1, 10);
INSERT INTO 60108_mysql_create_ddl1(id, val) VALUES (2, 20);

SELECT id, dt = dt2 FROM 60108_mysql_create_ddl ORDER BY id;

UPDATE 60108_mysql_create_ddl SET val = 100 WHERE id = 1;
UPDATE 60108_mysql_create_ddl1 SET val = 100 WHERE id = 1;

SELECT id, dt = dt2, dt3 > '2024-01-01', val FROM 60108_mysql_create_ddl WHERE id = 1;

set dialect_type='CLICKHOUSE';

UPDATE 60108_mysql_create_ddl SET val = 200 WHERE id = 2;

SELECT id, dt = dt2, dt3 > '2024-01-01', val FROM 60108_mysql_create_ddl WHERE id = 2;

DROP TABLE IF EXISTS 60108_mysql_create_ddl;

UPDATE 60108_mysql_create_ddl1 SET val = 200 WHERE id = 2;
SELECT id, dt2 > '2024-01-01', val FROM 60108_mysql_create_ddl1 WHERE id = 2;

DROP TABLE IF EXISTS 60108_mysql_create_ddl1;

set dialect_type='MYSQL';
-- Int and String
CREATE TABLE 60108_mysql_create_ddl2
(
    `id` Int,
    `dt` Int ON UPDATE 1 + 1,
    `dt2` String DEFAULT 'insert' ON UPDATE 'update',
    `val` Int,
    PRIMARY KEY(id)
);

INSERT INTO 60108_mysql_create_ddl2(id, val) VALUES (1, 10);
INSERT INTO 60108_mysql_create_ddl2(id, val) VALUES (2, 20);

SELECT dt = 2, dt2 = 'insert' FROM 60108_mysql_create_ddl2;

UPDATE 60108_mysql_create_ddl2 SET val = 100 WHERE id = 1;

SELECT id, dt = 2, dt2 = 'update' FROM 60108_mysql_create_ddl2 WHERE id = 1;

set dialect_type='CLICKHOUSE';

UPDATE 60108_mysql_create_ddl2 SET val = 100 WHERE id = 1;

SELECT id, dt = 2, dt2 = 'update' FROM 60108_mysql_create_ddl2 WHERE id = 1;

DROP TABLE IF EXISTS 60108_mysql_create_ddl2;

set dialect_type='MYSQL';

-- CREATE TABLE 60108_mysql_create_ddl3
-- (
--     `id` Int,
--     `dt` Array(String) ON UPDATE CURRENT_TIMESTAMP,
--     `dt2` String DEFAULT 'insert' ON UPDATE 'update',
--     `val` Int,
--     PRIMARY KEY(id)
-- ); -- { serverError 53 }

DROP TABLE IF EXISTS 60108_mysql_create_ddl3;
