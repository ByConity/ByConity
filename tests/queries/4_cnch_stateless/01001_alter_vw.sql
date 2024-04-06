DROP TABLE IF EXISTS t_alter_vw;
CREATE TABLE t_alter_vw(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY k ORDER BY m;
ALTER TABLE t_alter_vw MODIFY SETTING cnch_vw_write = 'asdfj'; -- { serverError 5025 }
ALTER TABLE t_alter_vw MODIFY SETTING cnch_vw_default = 'hello'; -- { serverError 5025 }
ALTER TABLE t_alter_vw MODIFY SETTING cnch_vw_write = 'vw_default' FORMAT Null;
ALTER TABLE t_alter_vw MODIFY SETTING cnch_vw_default = 'vw_write' FORMAT Null;

DROP TABLE t_alter_vw;

DROP TABLE IF EXISTS t_check_vw_when_create;
CREATE TABLE t_check_vw_when_create(k Int32, m Int32) ENGINE = CnchMergeTree() ORDER BY k SETTINGS cnch_vw_write = 'vw_xxx'; -- { serverError 5025 }
CREATE TABLE t_check_vw_when_create(k Int32, m Int32) ENGINE = CnchMergeTree() ORDER BY k SETTINGS cnch_vw_default = 'vw_xxx'; -- { serverError 5025 }
CREATE TABLE t_check_vw_when_create(k Int32, m Int32) ENGINE = CnchMergeTree() ORDER BY k SETTINGS cnch_vw_write = 'vw_default';
DROP TABLE t_check_vw_when_create;
