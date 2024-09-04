DROP database if exists explain_setting;
create database explain_setting;

use explain_setting;
DROP TABLE IF EXISTS explain_setting.test4601;

CREATE TABLE explain_setting.test4601 (event_time DateTime, id UInt64) ENGINE = CnchMergeTree ORDER BY id;
set enable_optimizer=1;
explain verbose=0,stats=0 select distinct id from explain_setting.test4601 settings enable_distinct_to_aggregate=0;
explain verbose=0,stats=0 select distinct id from explain_setting.test4601 settings enable_distinct_to_aggregate=1;

DROP TABLE IF EXISTS explain_setting.test4601;
DROP database if exists explain_setting;
