CREATE DATABASE IF NOT EXISTS `t.test_rd11000` Engine = Cnch;
DROP TABLE IF EXISTS `t.test_rd11000`.table_for_flat_dict;
CREATE TABLE `t.test_rd11000`.table_for_flat_dict(id UInt64, a UInt64, b Int32, c String) ENGINE = CnchMergeTree() ORDER BY id;
INSERT INTO `t.test_rd11000`.table_for_flat_dict VALUES (1, 100, -100, 'clickhouse'), (2, 3, 4, 'database'), (5, 6, 7, 'columns'), (10, 9, 8, '');
INSERT INTO `t.test_rd11000`.table_for_flat_dict SELECT number, 0, -1, 'a' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370;
INSERT INTO `t.test_rd11000`.table_for_flat_dict SELECT number, 0, -1, 'b' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370, 370;
INSERT INTO `t.test_rd11000`.table_for_flat_dict SELECT number, 0, -1, 'c' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 700, 370;

DROP DICTIONARY IF EXISTS `t.test_rd11000`.dict_flat;
CREATE DICTIONARY `t.test_rd11000`.dict_flat(id UInt64, a UInt64 DEFAULT 0, b Int32 DEFAULT -1, c String DEFAULT 'none') PRIMARY KEY id SOURCE(CLICKHOUSE(USER 'default' TABLE 'table_for_flat_dict' PASSWORD '' DB 't.test_rd11000')) LIFETIME(MIN 1000 MAX 2000) LAYOUT(FLAT());

SELECT dictGetInt32('`t.test_rd11000`.dict_flat', 'b', toUInt64(1));
SELECT dictGetInt32('`t.test_rd11000`.dict_flat', 'b', toUInt64(4));
SELECT dictGetUInt64('`t.test_rd11000`.dict_flat', 'a', toUInt64(5));
SELECT dictGetUInt64('`t.test_rd11000`.dict_flat', 'a', toUInt64(6));
SELECT dictGetString('`t.test_rd11000`.dict_flat', 'c', toUInt64(2));
SELECT dictGetString('`t.test_rd11000`.dict_flat', 'c', toUInt64(3));

DROP DICTIONARY IF EXISTS `t.test_rd11000`.dict_flat;
DROP TABLE IF EXISTS `t.test_rd11000`.table_for_flat_dict;
DROP DATABASE IF EXISTS `t.test_rd11000`;
