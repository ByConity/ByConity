DROP TABLE IF EXISTS test.table_for_no_ip_port_dict;
CREATE TABLE test.table_for_no_ip_port_dict(id UInt64, a UInt64, b Int32, c String) ENGINE = CnchMergeTree() ORDER BY id;
INSERT INTO test.table_for_no_ip_port_dict VALUES (1, 100, -100, 'clickhouse'), (2, 3, 4, 'database'), (5, 6, 7, 'columns'), (10, 9, 8, '');
INSERT INTO test.table_for_no_ip_port_dict SELECT number, 0, -1, 'a' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370;
INSERT INTO test.table_for_no_ip_port_dict SELECT number, 0, -1, 'b' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370, 370;
INSERT INTO test.table_for_no_ip_port_dict SELECT number, 0, -1, 'c' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 700, 370;

DROP DICTIONARY IF EXISTS test.dict_flat_no_ip_port;
CREATE DICTIONARY test.dict_flat_no_ip_port(id UInt64, a UInt64 DEFAULT 0, b Int32 DEFAULT -1, c String DEFAULT 'none') PRIMARY KEY id SOURCE(CLICKHOUSE(USER 'default' TABLE 'table_for_no_ip_port_dict' PASSWORD '' DB 'test')) LIFETIME(MIN 1000 MAX 2000) LAYOUT(FLAT());

SELECT dictGetInt32('test.dict_flat_no_ip_port', 'b', toUInt64(1));
SELECT dictGetInt32('test.dict_flat_no_ip_port', 'b', toUInt64(4));
SELECT dictGetUInt64('test.dict_flat_no_ip_port', 'a', toUInt64(5));
SELECT dictGetUInt64('test.dict_flat_no_ip_port', 'a', toUInt64(6));
SELECT dictGetString('test.dict_flat_no_ip_port', 'c', toUInt64(2));
SELECT dictGetString('test.dict_flat_no_ip_port', 'c', toUInt64(3));

DROP DICTIONARY IF EXISTS test.dict_flat_no_ip_port;
DROP TABLE IF EXISTS test.table_for_no_ip_port_dict;
