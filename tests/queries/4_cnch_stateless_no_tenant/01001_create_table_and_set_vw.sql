DROP TABLE IF EXISTS t_no_vw;
DROP TABLE IF EXISTS t_query_setting;
DROP TABLE IF EXISTS t_table_setting;
DROP TABLE IF EXISTS t_table_setting2;
DROP TABLE IF EXISTS t_table_setting3;

-- by default, virtual_warehouse_write is empty.
CREATE TABLE t_no_vw(k Int32) ENGINE = CnchMergeTree() ORDER BY k;
SHOW CREATE TABLE t_no_vw;

SET virtual_warehouse_write = 'vw_write';
CREATE TABLE t_query_setting(k Int32) ENGINE = CnchMergeTree() ORDER BY k;
SHOW CREATE TABLE t_query_setting;

CREATE TABLE t_table_setting(k Int32) ENGINE = CnchMergeTree() ORDER BY k SETTINGS cnch_vw_write = 'vw_default';
SHOW CREATE TABLE t_table_setting;

SET virtual_warehouse_write = '';
CREATE TABLE t_table_setting2(k Int32) ENGINE = CnchMergeTree() ORDER BY k SETTINGS cnch_vw_write = 'vw_default' SETTINGS virtual_warehouse_write = 'vw_write';
SHOW CREATE TABLE t_table_setting2;

CREATE TABLE t_table_setting3(k Int32) ENGINE = CnchMergeTree() ORDER BY k SETTINGS cnch_vw_write = 'vw_write' SETTINGS virtual_warehouse_write = 'vw_default';
SHOW CREATE TABLE t_table_setting3;

DROP TABLE t_no_vw;
DROP TABLE t_query_setting;
DROP TABLE t_table_setting;
DROP TABLE t_table_setting2;
DROP TABLE t_table_setting3;
