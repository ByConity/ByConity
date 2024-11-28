DROP TABLE IF EXISTS 03001_map_unsupported_types;

-- Map unsupported key types
SELECT cast(map('a', 'b'), 'Map(Nullable(String), String)'); -- { serverError 36 }
SELECT cast(map(array('a'), 'b'), 'Map(Array(String), String)'); -- { serverError 36 }
SELECT cast(map(tuple('a'), 'b'), 'Map(Tuple(String), String)'); -- { serverError 36 }
SELECT cast(map(map('a', 'b'), 'b'), 'Map(Map(String, String), String)'); -- { serverError 36 }


SET dialect_type = 'CLICKHOUSE';
-- Byte map unsupported value types in table
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Nullable(String))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, LowCardinality(String))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Tuple(String))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Array(Tuple(String)))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, String))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Array(Map(String, String)))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }


-- Some checks in creating table in clickhouse dialect
CREATE TABLE 03001_map_unsupported_types (a Int, b Nullable(Map(String, Int))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Nullable(Map(String, Int)) BYTE) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Nullable(Map(String, Int)) KV) ENGINE = CnchMergeTree ORDER BY a;
DROP TABLE 03001_map_unsupported_types;

CREATE TABLE 03001_map_unsupported_types (a Int, b Array(Map(String, Int))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Array(Map(String, Int)) BYTE) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Array(Map(String, Int)) KV) ENGINE = CnchMergeTree ORDER BY a;
DROP TABLE 03001_map_unsupported_types;

CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, Int))) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, Int)) BYTE) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, Int)) KV) ENGINE = CnchMergeTree ORDER BY a;
DROP TABLE 03001_map_unsupported_types;


SET dialect_type = 'MYSQL';
-- Some checks in creating table in mysql dialect
-- In mysql dialect, will add Null and KV flag automatically
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Int) BYTE) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Int) KV) ENGINE = CnchMergeTree ORDER BY a;
DESC 03001_map_unsupported_types;
DROP TABLE 03001_map_unsupported_types;
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Int)) ENGINE = CnchMergeTree ORDER BY a;
DESC 03001_map_unsupported_types;
DROP TABLE 03001_map_unsupported_types;
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Int) NOT NULL BYTE) ENGINE = CnchMergeTree ORDER BY a;
DESC 03001_map_unsupported_types;
DROP TABLE 03001_map_unsupported_types;

CREATE TABLE 03001_map_unsupported_types (a Int, b Array(Map(String, Int)) BYTE) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Array(Map(String, Int)) KV) ENGINE = CnchMergeTree ORDER BY a;
DESC 03001_map_unsupported_types;
DROP TABLE 03001_map_unsupported_types;
CREATE TABLE 03001_map_unsupported_types (a Int, b Array(Map(String, Int))) ENGINE = CnchMergeTree ORDER BY a;
DESC 03001_map_unsupported_types;
DROP TABLE 03001_map_unsupported_types;

CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, Int)) BYTE) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, Int)) KV) ENGINE = CnchMergeTree ORDER BY a;
DESC 03001_map_unsupported_types;
DROP TABLE 03001_map_unsupported_types;
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, Int))) ENGINE = CnchMergeTree ORDER BY a;
DESC 03001_map_unsupported_types;
DROP TABLE 03001_map_unsupported_types;

CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, Int)) NOT NULL BYTE) ENGINE = CnchMergeTree ORDER BY a; -- { serverError 36 }
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, Int)) NOT NULL KV) ENGINE = CnchMergeTree ORDER BY a;
DESC 03001_map_unsupported_types;
DROP TABLE 03001_map_unsupported_types;
CREATE TABLE 03001_map_unsupported_types (a Int, b Map(String, Map(String, Int)) NOT NULL) ENGINE = CnchMergeTree ORDER BY a;
DESC 03001_map_unsupported_types;
DROP TABLE 03001_map_unsupported_types;
