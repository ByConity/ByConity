DROP TABLE IF EXISTS test.t_alter_vw;
CREATE TABLE test.t_alter_vw(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY k ORDER BY m;
ALTER TABLE test.t_alter_vw MODIFY SETTING cnch_vw_write = 'asdfj'; -- { serverError 5025 }
ALTER TABLE test.t_alter_vw MODIFY SETTING cnch_vw_default = 'hello'; -- { serverError 5025 }
ALTER TABLE test.t_alter_vw MODIFY SETTING cnch_vw_write = 'vw_default' FORMAT Null;
ALTER TABLE test.t_alter_vw MODIFY SETTING cnch_vw_default = 'vw_write' FORMAT Null;

DROP TABLE test.t_alter_vw;
