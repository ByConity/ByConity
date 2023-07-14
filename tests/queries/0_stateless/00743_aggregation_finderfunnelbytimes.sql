use test;
DROP TABLE IF EXISTS funnel_time_test;
CREATE TABLE funnel_time_test
(
    `uid` UInt32 DEFAULT 1,
    `timestamp` UInt64,
    `event` String,
    `prop` String DEFAULT 'x'
)
ENGINE = CnchMergeTree()  order by uid;
INSERT INTO funnel_time_test (timestamp, event) values (86400,'A'),(86402,'B'),(86404,'A'),(86458,'B'),(86459,'C'),(86461,'A'),(86462,'B'),(86463,'B'),(86465,'C'),(86467,'A'),(86468,'C'),(86469,'D'),(86518,'C');
SELECT uid, finderFunnelByTimes(60, 86400, 86400, 1)(timestamp, timestamp, event = 'A',event='B',event='C') FROM funnel_time_test GROUP BY uid;
SELECT funnelRepByTimes(1,3)(res) from (SELECT  finderFunnelByTimes(60, 86400, 86400, 1)(timestamp, timestamp, event = 'A', event = 'B', event = 'C') as res FROM funnel_time_test GROUP BY uid );
DROP TABLE funnel_time_test;

DROP TABLE IF EXISTS funnel_time_test_2;
CREATE TABLE funnel_time_test_2
(
    `hash_uid` UInt64 DEFAULT 1,
    `server_time` UInt64,
    `time` UInt64,
    `event` String,
    `prop` Nullable(String)
)
ENGINE = CnchMergeTree() ORDER BY hash_uid;
INSERT INTO funnel_time_test_2 FORMAT JSONEachRow {"hash_uid":6489260730227591921,"server_time":1661933683,"time":1661933683829,"event":"academy_header_click","prop":null} {"hash_uid":6489260730227591921,"server_time":1661933597,"time":1661933597257,"event":"academy_header_click","prop":null} {"hash_uid":6489260730227591921,"server_time":1661933679,"time":1661933679615,"event":"academy_page_visitor","prop":"header1"} {"hash_uid":6489260730227591921,"server_time":1661934030,"time":1661934030226,"event":"academy_page_visitor","prop":"header1"} {"hash_uid":6489260730227591921,"server_time":1661934142,"time":1661934142037,"event":"academy_page_visitor","prop":"header1"} {"hash_uid":6489260730227591921,"server_time":1661933610,"time":1661933610558,"event":"academy_page_visitor","prop":"header1"};
SELECT finderFunnelByTimes(600000, 1661875200, 86400, 1, 0, 0, 'Asia/Shanghai')(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), unifyNull((event = 'academy_page_visitor') AND multiIf(event = 'academy_page_visitor', ifNull(prop, 'null') IN ('header1'), ifNull(prop, 'null') IN ('header1'))), unifyNull(event = 'academy_header_click'), unifyNull(event = 'academy_search_product_matrix_click')) AS funnel_tmp_res FROM funnel_time_test_2 GROUP BY hash_uid;
DROP TABLE funnel_time_test_2;

DROP TABLE IF EXISTS funnel_app_test;
CREATE TABLE funnel_app_test
(
    `hash_uid` UInt64 DEFAULT 1,
    `server_time` UInt64,
    `time` UInt64,
    `event` String,
    `prop`  Nullable(UInt32)
)
ENGINE = CnchMergeTree()  order by hash_uid;
INSERT INTO funnel_app_test(server_time,time,event,prop) FORMAT JSONEachRow {"server_time":1644112724,"time":1644112724202,"event":"app_launch","prop":23005} {"server_time":1644112738,"time":1644112738565,"event":"app_launch","prop":23005} {"server_time":1644112899,"time":1644112899564,"event":"app_launch","prop":22001} {"server_time":1644112944,"time":1644112944093,"event":"app_launch","prop":22001} {"server_time":1644113124,"time":1644113124129,"event":"app_launch","prop":22001} {"server_time":1644113170,"time":1644113170807,"event":"app_launch","prop":22001} {"server_time":1644113244,"time":1644113244058,"event":"generateOrder","prop":null} {"server_time":1644113262,"time":1644113262919,"event":"generateOrder","prop":null} {"server_time":1644113283,"time":1644113283813,"event":"app_launch","prop":22001} {"server_time":1644113288,"time":1644113288047,"event":"paySuccess","prop":null} {"server_time":1644113307,"time":1644113307025,"event":"app_launch","prop":22001} {"server_time":1644113319,"time":1644113319619,"event":"app_launch","prop":22001} {"server_time":1644113328,"time":1644113328579,"event":"app_launch","prop":22001} {"server_time":1644113338,"time":1644113338468,"event":"app_launch","prop":22001} {"server_time":1644113363,"time":1644113363540,"event":"app_launch","prop":22001} {"server_time":1644114952,"time":1644114952684,"event":"app_launch","prop":26004};
SELECT     hash_uid,     finderGroupFunnelByTimes(600000, 1644076800, 86400, 1, 1, 0, 0, 'Asia/Shanghai')(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), assumeNotNull(if(event = 'app_launch', prop, NULL)), assumeNotNull(unifyNull(event = 'app_launch')), assumeNotNull(unifyNull(event = 'generateOrder')), assumeNotNull(unifyNull(event = 'paySuccess'))) AS funnel_res FROM funnel_app_test AS et GROUP BY hash_uid;
SELECT     funnel_res.1 AS col1,     funnelRepByTimes(1, 3)(funnel_res.2) AS col2     from( SELECT     hash_uid,     arrayJoin(finderGroupFunnelByTimes(600000, 1644076800, 86400, 1, 1, 0, 0, 'Asia/Shanghai')(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), assumeNotNull(if(event = 'app_launch', prop, NULL)), assumeNotNull(unifyNull(event = 'app_launch')), assumeNotNull(unifyNull(event = 'generateOrder')), assumeNotNull(unifyNull(event = 'paySuccess')))) AS funnel_res FROM funnel_app_test AS et GROUP BY hash_uid) group by col1 order by col1;
SELECT     funnel_res.1 AS col1,     funnelRep2ByTimes(1, 3,[0, 3600000, 7200000, 10800000, 14400000, 18000000, 21600000, 25200000, 28800000, 32400000, 36000000, 39600000, 43200000, 46800000, 50400000, 54000000, 57600000, 61200000, 64800000, 68400000, 72000000, 75600000, 79200000, 82800000, 86400000])(funnel_res.2,funnel_res.3) AS col2     from( SELECT     hash_uid,     arrayJoin(finderGroupFunnelByTimes(600000, 1644076800, 86400, 1, 1, 0, 0, 'Asia/Shanghai',1,0)(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), assumeNotNull(if(event = 'app_launch', prop, NULL)), assumeNotNull(unifyNull(event = 'app_launch')), assumeNotNull(unifyNull(event = 'generateOrder')), assumeNotNull(unifyNull(event = 'paySuccess')))) AS funnel_res FROM funnel_app_test AS et GROUP BY hash_uid) group by col1 order by col1;

SELECT     hash_uid,     finderGroupFunnelByTimes(600000, 1644076800, 86400, 1, 1, 0, 0, 'Asia/Shanghai')(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), if(event = 'app_launch', prop, 0), unifyNull(event = 'app_launch'), unifyNull(event = 'generateOrder'), unifyNull(event = 'paySuccess')) AS funnel_res FROM funnel_app_test AS et GROUP BY hash_uid;
SELECT     funnel_res.1 AS col1,     funnelRepByTimes(1, 3)(funnel_res.2) AS col2     from( SELECT     hash_uid,     arrayJoin(assumeNotNull(finderGroupFunnelByTimes(600000, 1644076800, 86400, 1, 1, 0, 0, 'Asia/Shanghai')(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), if(event = 'app_launch', prop, 0), unifyNull(event = 'app_launch'), unifyNull(event = 'generateOrder'), unifyNull(event = 'paySuccess')))) AS funnel_res FROM funnel_app_test AS et GROUP BY hash_uid) group by col1 order by col1;
SELECT     funnel_res.1 AS col1,     funnelRep2ByTimes(1, 3,[0, 3600000, 7200000, 10800000, 14400000, 18000000, 21600000, 25200000, 28800000, 32400000, 36000000, 39600000, 43200000, 46800000, 50400000, 54000000, 57600000, 61200000, 64800000, 68400000, 72000000, 75600000, 79200000, 82800000, 86400000])(funnel_res.2,funnel_res.3) AS col2     from( SELECT     hash_uid,     arrayJoin(assumeNotNull(finderGroupFunnelByTimes(600000, 1644076800, 86400, 1, 1, 0, 0, 'Asia/Shanghai',1,0)(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), if(event = 'app_launch', prop, 0), unifyNull(event = 'app_launch'), unifyNull(event = 'generateOrder'), unifyNull(event = 'paySuccess')))) AS funnel_res FROM funnel_app_test AS et GROUP BY hash_uid) group by col1 order by col1;

SELECT     hash_uid,     finderGroupFunnelByTimes(600000, 1644076800, 86400, 1, 1, 0, 0, 'Asia/Shanghai')(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), if(event = 'app_launch', prop, NULL), unifyNull(event = 'app_launch'), unifyNull(event = 'generateOrder'), unifyNull(event = 'paySuccess')) AS funnel_res FROM funnel_app_test AS et GROUP BY hash_uid;
SELECT     funnel_res.1 AS col1,     funnelRepByTimes(1, 3)(funnel_res.2) AS col2     from( SELECT     hash_uid,     arrayJoin(finderGroupFunnelByTimes(600000, 1644076800, 86400, 1, 1, 0, 0, 'Asia/Shanghai')(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), if(event = 'app_launch', prop, NULL), unifyNull(event = 'app_launch'), unifyNull(event = 'generateOrder'), unifyNull(event = 'paySuccess'))) AS funnel_res FROM funnel_app_test AS et GROUP BY hash_uid) group by col1 order by col1;
SELECT     funnel_res.1 AS col1,     funnelRep2ByTimes(1, 3,[0, 3600000, 7200000, 10800000, 14400000, 18000000, 21600000, 25200000, 28800000, 32400000, 36000000, 39600000, 43200000, 46800000, 50400000, 54000000, 57600000, 61200000, 64800000, 68400000, 72000000, 75600000, 79200000, 82800000, 86400000])(funnel_res.2,funnel_res.3) AS col2     from( SELECT     hash_uid,     arrayJoin(finderGroupFunnelByTimes(600000, 1644076800, 86400, 1, 1, 0, 0, 'Asia/Shanghai',1,0)(multiIf(server_time < 1609948800, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), if(event = 'app_launch', prop, NULL), unifyNull(event = 'app_launch'), unifyNull(event = 'generateOrder'), unifyNull(event = 'paySuccess'))) AS funnel_res FROM funnel_app_test AS et GROUP BY hash_uid) group by col1 order by col1;

DROP TABLE funnel_app_test;

CREATE TABLE funnel_app_test
(
    `tea_app_id` UInt32,
    `hash_uid`   UInt32,
    `time`       UInt64,
    `event`      String,
    `prop1`      Nullable(String),
    `prop2`      Nullable(String)
) ENGINE = CnchMergeTree() ORDER BY (tea_app_id, event, hash_uid) SAMPLE BY hash_uid SETTINGS index_granularity = 8192;
insert into funnel_app_test (tea_app_id, hash_uid, time, event, prop1, prop2) VALUES (1, 1, 1678360026459, 'chat_relation_available', 'girl', ''),(1, 1, 1678372786031, 'chat_relation_available', 'girl', ''),(1, 1, 1678373006462, 'chat_relation_available', 'girl', ''),(1, 1, 1678375681230, 'chat_relation_available', 'girl', ''),(1, 1, 1678376858244, 'chat_relation_available', 'girl', ''),(1, 1, 1678377686610, 'chat_relation_available', 'girl', ''),(1, 1, 1678377725140, 'chat_relation_available', 'girl', ''),(1, 1, 1678378834400, 'chat_relation_gift_click', 'girl', ''),(1, 1, 1678381813408, 'chat_relation_available', 'girl', ''),(1, 1, 1678381858494, 'chat_relation_gift_click', 'girl', ''),(1, 1, 1678415449203, 'chat_relation_gift_click', 'girl', ''),(1, 1, 1678432935208, 'chat_relation_available', 'girl', '');
select hash_uid, finderGroupFunnelByTimes(8640000, 1678291200, 86400, 1, 1, 0, 0, 'Asia/Shanghai')(toUInt64(time / 1000), case when time <= 2000000000 then time * 100 when time > 2000000000 then toUInt64(time / 10) else time end, prop1,unifyNull(((event = 'chat_relation_available'))),unifyNull(((event = 'chat_relation_gift_click')))) AS funnel_res from funnel_app_test et where ((event = 'chat_relation_available') or (event = 'chat_relation_gift_click')) group by et.hash_uid;
DROP TABLE IF EXISTS funnel_app_test;
