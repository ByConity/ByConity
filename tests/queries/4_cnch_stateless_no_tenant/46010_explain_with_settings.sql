DROP database if exists explain_setting;
create database explain_setting;

use explain_setting;
DROP TABLE IF EXISTS explain_setting.setting_1;

CREATE TABLE explain_setting.setting_1 (event_time DateTime, id UInt64, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(m1) PARTITION BY toDate(event_time) ORDER BY id UNIQUE KEY id;

explain select distinct id from explain_setting.setting_1 limit 1 settings enable_optimizer=0 format Null;
explain select distinct id from explain_setting.setting_1 limit 1 settings enable_optimizer=1 format Null;

DROP TABLE IF EXISTS explain_setting.setting_1;
DROP database if exists explain_setting;
