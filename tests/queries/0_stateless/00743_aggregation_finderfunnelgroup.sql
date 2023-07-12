USE test;

DROP TABLE IF EXISTS funnel_group_test;
CREATE TABLE funnel_group_test (uid UInt32 default 1, timestamp UInt64, event UInt32, prop String) engine=CnchMergeTree() order by uid;
INSERT INTO funnel_group_test (timestamp, event, prop) values (86400,1000,'a'),(86401,1001,'b'),(86402,1002,'c'),(86403,1003,'d'),(86404,1004,'e'),(86405,1005,'f'),(86406,1006,'g');

SELECT uid, finderGroupFunnel(86400, 86400, 86400, 1, 1)(timestamp, timestamp, 1, event = 1001) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400, 86400, 86400, 1, 1)(timestamp, timestamp, 1, event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400, 86400, 86400, 1, 1)(timestamp, timestamp, 1, event = 1001, event = 1002, event = 1003) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400, 86400, 86400, 1, 1)(timestamp, timestamp, 1, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(1, 86400, 86400, 1, 1)(timestamp, timestamp, 1, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(2, 86400, 86400, 1, 1)(timestamp, timestamp, 1, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(3, 86400, 86400, 1, 1)(timestamp, timestamp, 1, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 86400, 86400, 1, 1)(timestamp, timestamp, 1, event = 1001, event = 1002, event = 1010) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 86400, 86400, 1, 1)(timestamp, timestamp, 1, event = 1001, event = 1002, event = 1006) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 86400, 86400, 1, 1)(timestamp, timestamp, if(prop='b', 1, 0), event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400, 86400, 86400, 1, 2)(timestamp, timestamp, if(prop='b', 1, 0), event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 86400, 86400, 1, 1)(timestamp, timestamp, 'a', event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 0, 86400, 2, 1)(timestamp, timestamp, event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;

DROP TABLE funnel_group_test;

CREATE TABLE funnel_group_test (uid UInt32 default 1, timestamp UInt64, event UInt32, prop String) engine=CnchMergeTree() order by uid;
INSERT INTO funnel_group_test (timestamp, event, prop) values (129600,1000,'a'),(143997,1000, 'a'),(143998,1001,'b'),(143999,1003,'d'),(144000,1002,'c'),(172799,1000,'a'),(172800,1003,'d'),(172801,1000,'a'),(172802,1001,'b'),(172803,1002,'c'),(172804,1003,'d'),(172805,1004,'f'),(216000,1000,'a'),(216001,1001,'b'),(216002,1001,'b'),(216003,1002,'c');

-- tips
-- timestamp            utc-0                   utc-8
-- 129600              1970-01-02 12:00:00     1970-01-02 20:00:00
-- 144000              1970-01-02 04:00:00     1970-01-03 00:00:00
-- 172800              1970-01-03 00:00:00     1970-01-03 08:00:00
-- 216000              1970-01-03 12:00:00     1970-01-03 20:00:00
SELECT uid, finderGroupFunnel(86400, 129600, 86400, 1, 1)(timestamp, timestamp, 1, event = 1000, event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 1, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, 1, event = 1000, event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 1, 1, 'Etc/GMT')(timestamp, timestamp*1000, 1, event = 1000, event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 129600, 86400, 1, 1)(timestamp, timestamp, 1, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 1, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, 1, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 1, 1, 'Etc/GMT')(timestamp, timestamp*1000, 1, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 129600, 86400, 1, 1)(timestamp, timestamp, if(prop='a', 1, 0), event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 1, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, if(prop='a', 1, 0), event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 1, 1, 'Etc/GMT')(timestamp, timestamp*1000, if(prop='a', 1, 0), event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 129600, 86400, 1, 2)(timestamp, timestamp, if(prop='a', 1, 0), event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 2, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, if(prop='a', 1, 0), event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 2, 1, 'Etc/GMT')(timestamp, timestamp*1000, if(prop='a', 1, 0), event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 129600, 86400, 1, 1)(timestamp, timestamp, 'a', event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 1, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, 'a', event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 1, 1, 1, 'Etc/GMT')(timestamp, timestamp*1000, 'a', event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(86400, 129600, 86400, 2, 1)(timestamp, timestamp, 1, event = 1000, event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 2, 1, 1, 'Asia/Shanghai')(timestamp, timestamp*1000, 1, event = 1000, event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(86400000, 129600, 86400, 2, 1, 1, 'Etc/GMT')(timestamp, timestamp*1000, 1, event = 1000, event = 1001, event = 1002) FROM funnel_group_test GROUP BY uid;

DROP TABLE IF EXISTS funnel_group_test;
CREATE TABLE funnel_group_test (uid UInt32 default 1, timestamp UInt64, event UInt32, prop String) engine=CnchMergeTree() order by uid;
INSERT INTO funnel_group_test (timestamp, event, prop) values (86400, 1001, '1'),(86401, 1001, '2'), (86402, 1002, '2'), (86403, 1001, '1'), (86404, 1004, '1'), (86405, 1003, '2'), (86406, 1004, '2'), (86407, 1001, '1'), (86408, 1002, '2'),(86409, 1002, '1'), (86410, 1001, '2'), (86411, 1003, '1'), (86412, 1002, '2'), (86413, 1003, '2'), (86414, 1004, '1'), (86415, 1004, '2') , (86416, 1001, '1'), (86417, 1003, '2'), (86418, 1002, '2'), (86419, 1001, '1'), (86420, 1003, '1'), (86421, 1001, '1'), (86422, 1004, '2'), (86423, 1002, '1');

SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 3)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 5)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 3)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 5)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 3)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 5)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 6)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 10)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 6)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 10)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 6)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 10)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 3)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 5)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 3)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 5)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 3)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 5)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 6)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 10)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 6)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 10)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 6)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 10)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 5, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 3, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 5, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 6, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 10, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 6, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 3, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 1, 5, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 3, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 2, 10, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 6, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;
SELECT uid, finderGroupFunnel(5, 86400, 1, 24, 4, 10, 0, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', 1, 2), prop, prop, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_group_test GROUP BY uid;

DROP TABLE funnel_group_test;
