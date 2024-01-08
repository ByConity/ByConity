CREATE DATABASE IF NOT EXISTS test_rd11003;
DROP TABLE IF EXISTS test_rd11003.table_for_complex_hash_dict;
CREATE TABLE test_rd11003.table_for_complex_hash_dict(k1 String, k2 Int32, a UInt64, b Int32, c String) ENGINE = CnchMergeTree() ORDER BY (k1, k2);

INSERT INTO test_rd11003.table_for_complex_hash_dict VALUES (toString(1), 3, 100, -100, 'clickhouse'), (toString(2), -1, 3, 4, 'database'), (toString(5), -3, 6, 7, 'columns'), (toString(10), -20, 9, 8, '');
INSERT INTO test_rd11003.table_for_complex_hash_dict SELECT toString(number), number + 1, 0, -1, 'a' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370;
INSERT INTO test_rd11003.table_for_complex_hash_dict SELECT toString(number), number + 10, 0, -1, 'b' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370, 370;
INSERT INTO test_rd11003.table_for_complex_hash_dict SELECT toString(number), number + 100, 0, -1, 'c' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 700, 370;

DROP DICTIONARY IF EXISTS test_rd11003.dict_complex_hash;
CREATE DICTIONARY test_rd11003.dict_complex_hash(k1 String, k2 Int32, a UInt64 DEFAULT 0, b Int32 DEFAULT -1, c String DEFAULT 'none') PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(USER 'default' TABLE 'table_for_complex_hash_dict' PASSWORD '' DB 'test_rd11003')) LIFETIME(MIN 1000 MAX 2000) LAYOUT(COMPLEX_KEY_HASHED());

SELECT dictGetUInt64('test_rd11003.dict_complex_hash', 'a', tuple('1', toInt32(3)));
SELECT dictGetInt32('test_rd11003.dict_complex_hash', 'b', tuple('1', toInt32(3)));
SELECT dictGetString('test_rd11003.dict_complex_hash', 'c', tuple('1', toInt32(3)));

SELECT dictGetUInt64('test_rd11003.dict_complex_hash', 'a', tuple('1', toInt32(3)));
SELECT dictGetInt32('test_rd11003.dict_complex_hash', 'b', tuple('1', toInt32(3)));
SELECT dictGetString('test_rd11003.dict_complex_hash', 'c', tuple('1', toInt32(3)));

SELECT dictGetUInt64('test_rd11003.dict_complex_hash', 'a', tuple('2', toInt32(-1)));
SELECT dictGetInt32('test_rd11003.dict_complex_hash', 'b', tuple('2', toInt32(-1)));
SELECT dictGetString('test_rd11003.dict_complex_hash', 'c', tuple('2', toInt32(-1)));

SELECT dictGetUInt64('test_rd11003.dict_complex_hash', 'a', tuple('5', toInt32(-3)));
SELECT dictGetInt32('test_rd11003.dict_complex_hash', 'b', tuple('5', toInt32(-3)));
SELECT dictGetString('test_rd11003.dict_complex_hash', 'c', tuple('5', toInt32(-3)));

SELECT dictGetUInt64('test_rd11003.dict_complex_hash', 'a', tuple('10', toInt32(-20)));
SELECT dictGetInt32('test_rd11003.dict_complex_hash', 'b', tuple('10', toInt32(-20)));
SELECT dictGetString('test_rd11003.dict_complex_hash', 'c', tuple('10', toInt32(-20)));

DROP DICTIONARY IF EXISTS test_rd11003.dict_complex_hash;
DROP TABLE IF EXISTS test_rd11003.table_for_complex_hash_dict;
DROP DATABASE IF EXISTS test_rd11003;
