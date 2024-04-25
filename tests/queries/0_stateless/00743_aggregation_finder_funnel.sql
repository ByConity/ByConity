USE test;

DROP TABLE IF EXISTS funnel_test;
CREATE TABLE funnel_test (uid UInt32 default 1, timestamp UInt64, event UInt32, prop String) engine=CnchMergeTree() order by uid;
INSERT INTO funnel_test (timestamp, event, prop) values (86400,1000,'a'),(86401,1001,'b'),(86402,1002,'c'),(86403,1003,'d'),(86404,1004,'e'),(86405,1005,'f'),(86406,1006,'g');

SELECT uid, finderFunnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001) FROM funnel_test GROUP BY uid;
SELECT uid, finderFunnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002) FROM funnel_test GROUP BY uid;
SELECT uid, finderFunnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003) FROM funnel_test GROUP BY uid;
SELECT uid, finderFunnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test GROUP BY uid;

SELECT uid, finderFunnel(1, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test GROUP BY uid;
SELECT uid, finderFunnel(2, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test GROUP BY uid;
SELECT uid, finderFunnel(3, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test GROUP BY uid;

SELECT uid, finderFunnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1010) FROM funnel_test GROUP BY uid;

SELECT uid, finderFunnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1006) FROM funnel_test GROUP BY uid;

SELECT uid, finderFunnel(86400, 0, 86400, 2)(timestamp, timestamp, event = 1001, event = 1002) FROM funnel_test GROUP BY uid;

DROP TABLE IF EXISTS funnel_test_2;
CREATE TABLE funnel_test_2 (uid UInt32 default 1, timestamp UInt64, event UInt32, prop String) engine=CnchMergeTree() order by uid;
INSERT INTO funnel_test_2 (timestamp, event, prop) values (129600,1000,'a'),(143997,1000, 'a'),(143998,1001,'b'),(143999,1003,'d'),(144000,1002,'c'),(172799,1000,'a'),(172800,1003,'d'),(172801,1000,'a'),(172802,1001,'b'),(172803,1002,'c'),(172804,1003,'d'),(172805,1004,'f'),(216000,1000,'a'),(216001,1001,'b'),(216002,1001,'b'),(216003,1002,'c');

-- tips
-- timestamp            utc-0                   utc-8
-- 129600              1970-01-02 12:00:00     1970-01-02 20:00:00
-- 144000              1970-01-02 04:00:00     1970-01-03 00:00:00
-- 172800              1970-01-03 00:00:00     1970-01-03 08:00:00
-- 216000              1970-01-03 12:00:00     1970-01-03 20:00:00
SELECT uid, finderFunnel(86400, 129600, 86400, 1)(timestamp, timestamp, event = 1000, event = 1001, event = 1002) FROM funnel_test_2 GROUP BY uid;
SELECT uid, finderFunnel(86400000, 129600, 86400, 1, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, event = 1000, event = 1001, event = 1002) FROM funnel_test_2 GROUP BY uid;
SELECT uid, finderFunnel(86400000, 129600, 86400, 1, 1, 'Etc/GMT')(timestamp, timestamp*1000, event = 1000, event = 1001, event = 1002) FROM funnel_test_2 GROUP BY uid;

SELECT uid, finderFunnel(86400, 129600, 86400, 1)(timestamp, timestamp, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_2 GROUP BY uid;
SELECT uid, finderFunnel(86400000, 129600, 86400, 1, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_2 GROUP BY uid;
SELECT uid, finderFunnel(86400000, 129600, 86400, 1, 1, 'Etc/GMT')(timestamp, timestamp*1000, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_2 GROUP BY uid;

SELECT uid, finderFunnel(86400, 129600, 86400, 2)(timestamp, timestamp, event = 1000, event = 1001, event = 1002) FROM funnel_test_2 GROUP BY uid;
SELECT uid, finderFunnel(86400000, 129600, 86400, 2, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, event = 1000, event = 1001, event = 1002) FROM funnel_test_2 GROUP BY uid;
SELECT uid, finderFunnel(86400000, 129600, 86400, 2, 1, 'Etc/GMT')(timestamp, timestamp*1000, event = 1000, event = 1001, event = 1002) FROM funnel_test_2 GROUP BY uid;

SELECT uid, finderFunnel(86400, 129600, 86400, 2)(timestamp, timestamp, event = 1000, event = 1001, event = 1002) FROM funnel_test_2 GROUP BY uid;
SELECT uid, finderFunnel(86400000, 57600, 86400, 2, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, event = 1000, event = 1001, event = 1002) FROM funnel_test_2 GROUP BY uid;
SELECT uid, finderFunnel(86400000, 86400, 86400, 2, 1, 'Etc/GMT')(timestamp, timestamp*1000, event = 1000, event = 1001, event = 1002) FROM funnel_test_2 GROUP BY uid;

DROP TABLE IF EXISTS funnel_test_3;
CREATE TABLE funnel_test_3 (uid UInt32 default 1, timestamp UInt64, event UInt32, prop String) engine=CnchMergeTree() order by uid;
INSERT INTO funnel_test_3 (timestamp, event, prop) values
(86400, 1001, 'a'),(86401, 1002, 'b'), (86402, 1001, 'b'), (86403, 1003, 'b'), (86404, 1001, 'a'), (86405, 1001, 'd'), (86406, 1002, 'd'), (86407, 1003, 'b'), (86408, 1004, 'd'), (86410, 1003, 'f'), (86413, 1001, 'w');

SELECT uid, finderFunnel(5, 86400, 1, 14, 7)(timestamp, timestamp, prop, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14, 5)(timestamp, timestamp, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14, 6)(timestamp, timestamp, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14, 10)(timestamp, timestamp, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14, 14)(timestamp, timestamp, prop, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;

SELECT uid, finderFunnel(5, 86400, 1, 14, 7, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, prop, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14, 5, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14, 6, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14, 10, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14, 14, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, prop, prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;
SELECT uid, finderFunnel(5, 86400, 1, 14, 0, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test_3 GROUP BY uid;

DROP TABLE funnel_test;
DROP TABLE funnel_test_2;
DROP TABLE funnel_test_3;

DROP TABLE IF EXISTS tob_apps_test;
CREATE TABLE tob_apps_test
(
    `app_id` UInt32,
    `app_name` String DEFAULT '',
    `app_version` String DEFAULT '',
    `hash_uid` UInt64,
    `server_time` UInt64,
    `time` UInt64,
    `event` String,
    `user_unique_id` String,
    `event_date` Date,
    `age` String,
    `tea_app_id` UInt32
)
ENGINE = CnchMergeTree()
PARTITION BY (tea_app_id, event_date)
ORDER BY (tea_app_id, event, event_date, hash_uid, user_unique_id)
SAMPLE BY hash_uid
SETTINGS index_granularity = 8192;
insert into tob_apps_test FORMAT JSONEachRow {"app_id":237094,"app_name":"rangers_11363_manmanbuy","app_version":"4.2.11","hash_uid":6424789042916211101,"server_time":1668124421,"time":1668124410851,"event":"ChaResultPage_Page_Ex","user_unique_id":"73ac252e-79cb-491e-ba55-4e7f6f0ea07e","event_date":"2022-11-11","tea_app_id":317923} {"app_id":237094,"app_name":"rangers_11363_manmanbuy","app_version":"4.2.11","hash_uid":6424789042916211101,"server_time":1668180107,"time":1668180062220,"event":"ChaResultPage_Page_Ex","user_unique_id":"73ac252e-79cb-491e-ba55-4e7f6f0ea07e","event_date":"2022-11-11","tea_app_id":317923} {"app_id":237094,"app_name":"rangers_11363_manmanbuy","app_version":"4.2.11","hash_uid":6424789042916211101,"server_time":1667837016,"time":1667836997455,"event":"ChaResultPage_Page_Ex","user_unique_id":"73ac252e-79cb-491e-ba55-4e7f6f0ea07e","event_date":"2022-11-08","tea_app_id":317923} {"app_id":237094,"app_name":"rangers_11363_manmanbuy","app_version":"4.2.11","hash_uid":6424789042916211101,"server_time":1667775720,"time":1667775685578,"event":"ChaResultPage_Page_Ex","user_unique_id":"73ac252e-79cb-491e-ba55-4e7f6f0ea07e","event_date":"2022-11-07","tea_app_id":317923} {"app_id":237094,"app_name":"rangers_11363_manmanbuy","app_version":"4.2.11","hash_uid":6424789042916211101,"server_time":1667836567,"time":1667836540094,"event":"ChaResultPage_Page_Ex","user_unique_id":"73ac252e-79cb-491e-ba55-4e7f6f0ea07e","event_date":"2022-11-07","tea_app_id":317923} {"app_id":237094,"app_name":"rangers_11363_manmanbuy","app_version":"4.2.11","hash_uid":6424789042916211101,"server_time":1667775847,"time":1667775822677,"event":"ChaResultPage_Page_Ex","user_unique_id":"73ac252e-79cb-491e-ba55-4e7f6f0ea07e","event_date":"2022-11-07","tea_app_id":317923};
SELECT     hash_uid,     finderFunnel(600000, 1667750400, 86400, 7, 0, 0, 'Asia/Shanghai')(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), unifyNull(event = 'ChaResultPage_Page_Ex'), unifyNull(event = 'ChaResultPage_Bashou_Click'), unifyNull(event = 'ChaResultPage_Page_Ex')) AS funnel_tmp_res FROM tob_apps_test AS et WHERE (tea_app_id = 317923) AND ((event = 'ChaResultPage_Page_Ex') OR (event = 'ChaResultPage_Bashou_Click') OR (event = 'ChaResultPage_Page_Ex')) AND (hash_uid = 6424789042916211101) GROUP BY hash_uid;
insert into tob_apps_test FORMAT JSONEachRow {"event":"REPORT_ACTION","tea_app_id":8000931,"time":1671521325899,"age":"1"} {"event":"REPORT_ACTION","tea_app_id":8000931,"time":1671521347366,"age":"2"} {"event":"REPORT_ACTION","tea_app_id":8000931,"time":1671522622879,"age":"3"} {"event":"REPORT_ACTION","tea_app_id":8000931,"time":1671522625000,"age":"3"};
SELECT finderFunnel(8640000, 1671408000, 86400, 3, 3, 0, 'UTC')(if(time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 100, time > 2000000000, toUInt64(time / 10), time), assumeNotNull(age), assumeNotNull(age), unifyNull(event = 'REPORT_ACTION'), unifyNull(event = 'REPORT_ACTION')) AS funnel_tmp_res     FROM tob_apps_test AS et     WHERE tea_app_id = 8000931     GROUP BY hash_uid;
SELECT finderFunnel(60000, 1671408000, 86400, 3, 3, 0, 'UTC')(if(time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 100, time > 2000000000, toUInt64(time / 10), time), assumeNotNull(age), assumeNotNull(age), unifyNull(event = 'REPORT_ACTION'), unifyNull(event = 'REPORT_ACTION')) AS funnel_tmp_res     FROM tob_apps_test AS et     WHERE tea_app_id = 8000931     GROUP BY hash_uid;
DROP TABLE tob_apps_test;

-- for step execute

-- for same event
DROP TABLE IF EXISTS test_funnel1;
DROP TABLE IF EXISTS test_funnel2;

CREATE TABLE test_funnel1 (`time` UInt64, `e1` UInt8, `e2` UInt8, `e3` UInt8, `e4` UInt8) ENGINE = Log;
CREATE TABLE test_funnel2 (`time` UInt64, `e1` UInt8, `e2` UInt8, `e3` UInt8, `e4` UInt8) ENGINE = Log;

insert into test_funnel1 values(1683806898328,1,0,0,0),(1683806898330,0,1,0,0),(1683806900508,0,1,0,0),(1683806901916,0,1,0,0),(1683806903204,0,1,0,0),(1683806904194,0,1,0,0),(1683806906858,0,1,0,0),(1683806907932,0,1,0,0),(1683806908930,0,1,0,0),(1683806914819,0,0,1,0),(1683806925396,0,0,1,0),(1683806927194,0,1,0,0),(1683806927361,0,0,0,0),(1683806930130,0,1,0,0),(1683806930217,0,0,0,0),(1683806934089,0,0,0,0),(1683806934578,1,0,0,0),(1683806935168,0,0,0,0),(1683806940616,0,1,0,0),(1683806940680,0,0,0,0),(1683806944511,0,0,0,0),(1683806948680,0,0,0,0),(1683806953143,0,0,0,0),(1683806958020,0,0,0,0),(1683806958949,0,0,0,0),(1683806972320,0,0,0,0),(1683806974256,0,0,0,0),(1683806978544,0,0,0,0),(1683806979767,0,0,0,0),(1683806990827,0,0,0,0),(1683806994935,0,0,0,0),(1683806995520,1,0,0,0),(1683806996079,0,0,0,0),(1683807006954,0,1,0,0),(1683807007032,0,0,0,0),(1683807012072,0,0,0,0),(1683807014154,0,1,0,0),(1683807014206,0,0,0,0),(1683807022045,0,1,0,0),(1683807025518,0,0,0,0),(1683807029040,0,0,0,0),(1683807030402,0,0,0,0),(1683807033422,0,0,0,0),(1683807034006,1,0,0,0),(1683807034556,0,0,0,0),(1683807037709,0,1,0,0),(1683807042345,0,1,0,0),(1683807043705,0,1,0,0),(1683807044729,0,1,0,0),(1683807059821,0,0,0,0),(1683807062608,0,0,0,0),(1683807064490,0,1,0,0),(1683807065231,0,0,0,1);
insert into test_funnel2 values(1683806898328,1,0,0,0),(1683806898330,0,1,0,0),(1683806900508,0,1,0,0),(1683806901916,0,1,0,0),(1683806903204,0,1,0,0),(1683806904194,0,1,0,0),(1683806906858,0,1,0,0),(1683806907932,0,1,0,0),(1683806908930,0,1,0,0),(1683806914819,1,0,1,0),(1683806925396,1,0,1,0),(1683806927194,0,1,0,0),(1683806927361,1,0,0,0),(1683806930130,0,1,0,0),(1683806930217,0,0,0,0),(1683806934089,0,0,0,0),(1683806934578,1,0,0,0),(1683806935168,1,0,0,0),(1683806940616,0,1,0,0),(1683806940680,1,0,0,0),(1683806944511,1,0,0,0),(1683806948680,1,0,0,0),(1683806953143,0,0,0,0),(1683806958020,0,0,0,0),(1683806958949,0,0,0,0),(1683806972320,0,0,0,0),(1683806974256,0,0,0,0),(1683806978544,0,0,0,0),(1683806979767,0,0,0,0),(1683806990827,0,0,0,0),(1683806994935,0,0,0,0),(1683806995520,1,0,0,0),(1683806996079,1,0,0,0),(1683807006954,0,1,0,0),(1683807007032,1,0,0,0),(1683807012072,1,0,0,0),(1683807014154,0,1,0,0),(1683807014206,1,0,0,0),(1683807022045,0,1,0,0),(1683807025518,1,0,0,0),(1683807029040,1,0,0,0),(1683807030402,0,0,0,0),(1683807033422,0,0,0,0),(1683807034006,1,0,0,0),(1683807034556,1,0,0,0),(1683807037709,0,1,0,0),(1683807042345,0,1,0,0),(1683807043705,0,1,0,0),(1683807044729,0,1,0,0),(1683807059821,1,0,0,0),(1683807062608,1,0,0,0),(1683807064490,0,1,0,0),(1683807065231,1,0,0,1);

SELECT finderFunnel(0, 1682870400, 86400000, 25, 0, 1, 'Asia/Shanghai', 1)(toUInt64(time / 1000), multiIf(time <= 2000000000, time * 1000, time), e1, e2, e3, e4) AS funnel_tmp_res FROM test_funnel1;
SELECT finderFunnel(0, 1682870400, 86400000, 25, 0, 1, 'Asia/Shanghai', 1)(toUInt64(time / 1000), multiIf(time <= 2000000000, time * 1000, time), e1, e2, e3, e4) AS funnel_tmp_res FROM test_funnel2;

DROP TABLE IF EXISTS test_funnel1;
DROP TABLE IF EXISTS test_funnel2;
