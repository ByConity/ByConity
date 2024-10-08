-- case 1. Create and Describe with ANSI dialect
SET dialect_type='ANSI';
DROP TABLE IF EXISTS t1;
CREATE TABLE t1
(
    a DATE,
    b Int64 NOT NULL,
    c FixedString(100),
    d Array(Int32 NULL) NOT NULL,
    e Nullable(Array(Array(Float32) NULL)),
    f Array(Array(Nullable(Array(LowCardinality(String NULL)))) NOT NULL) NULL,
    g Tuple(s String NOT NULL, i Nullable(Nullable(Int64)), arr Array(UUID NOT NULL) NULL) NOT NULL,
    i Nested
        (
        ss String NULL,
        ii Nullable(Boolean),
        aa Array(Array(Enum('hello' = 1, 'world' = 2) NULL) NULL)
        ) NOT NULL
)
    ENGINE=CnchMergeTree() ORDER BY (a, b) SETTINGS allow_nullable_key = 1;

ALTER TABLE t1 ADD COLUMN added1 Array(Array(UInt128 NOT NULL) NULL);
ALTER TABLE t1 ADD COLUMN added2 Nullable(Array(String NULL));

SHOW CREATE TABLE t1 SETTINGS dialect_type='ANSI';
DESC TABLE t1 SETTINGS dialect_type='ANSI';


-- case 2. Create with ANSI dialect and Describe with CLICKHOUSE dialect
SHOW CREATE TABLE t1 SETTINGS dialect_type='CLICKHOUSE';
DESC TABLE t1 SETTINGS dialect_type='CLICKHOUSE';


-- case 3. Create and Describe with CLICKHOUSE dialect
SET dialect_type='CLICKHOUSE';
DROP TABLE IF EXISTS t1;
CREATE TABLE t1
(
    a DATE,
    b Int64 NOT NULL,
    c FixedString(100),
    d Array(Int32 NULL) NOT NULL,
    e Nullable(Array(Array(Float32) NULL)),
    f Array(Array(Nullable(Array(LowCardinality(String NULL)))) NOT NULL) NULL,
    g Tuple(s String NOT NULL, i Nullable(Nullable(Int64)), arr Array(UUID NOT NULL) NULL) NOT NULL,
    i Nested
        (
        ss String NULL,
        ii Nullable(Boolean),
        aa Array(Array(Enum('hello' = 1, 'world' = 2) NULL) NULL)
        ) NOT NULL
)
    ENGINE=CnchMergeTree() ORDER BY (a, b) SETTINGS allow_nullable_key = 1;

ALTER TABLE t1 ADD COLUMN added1 Array(Array(UInt128 NOT NULL) NULL);
ALTER TABLE t1 ADD COLUMN added2 Nullable(Array(String NULL));

SHOW CREATE TABLE t1 SETTINGS dialect_type='CLICKHOUSE';
DESC TABLE t1 SETTINGS dialect_type='CLICKHOUSE';


-- case 4. Create with CLICKHOUSE dialect and Describe with ANSI dialect
SHOW CREATE TABLE t1 SETTINGS dialect_type='ANSI';
DESC TABLE t1 SETTINGS dialect_type='ANSI';
