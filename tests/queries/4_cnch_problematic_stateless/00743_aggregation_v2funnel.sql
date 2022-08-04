USE test;

DROP TABLE IF EXISTS funnel_test;
CREATE TABLE funnel_test (uid UInt32 default 1, timestamp UInt32, event UInt32, prop String) engine=CnchMergeTree() ORDER BY uid;
INSERT INTO funnel_test (timestamp, event, prop) values (86400,1000,'a'),(86401,1001,'b'),(86402,1002,'c'),(86403,1003,'d'),(86404,1004,'e'),(86405,1005,'f'),(86406,1006,'g');

SELECT uid, v2funnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001) FROM funnel_test GROUP BY uid;
SELECT uid, v2funnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002) FROM funnel_test GROUP BY uid;
SELECT uid, v2funnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003) FROM funnel_test GROUP BY uid;
SELECT uid, v2funnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test GROUP BY uid;

SELECT uid, v2funnel(1, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test GROUP BY uid;
SELECT uid, v2funnel(2, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test GROUP BY uid;
SELECT uid, v2funnel(3, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1003, event = 1004) FROM funnel_test GROUP BY uid;

SELECT uid, v2funnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1010) FROM funnel_test GROUP BY uid;

SELECT uid, v2funnel(86400, 86400, 86400, 1)(timestamp, timestamp, event = 1001, event = 1002, event = 1006) FROM funnel_test GROUP BY uid;

SELECT uid, v2funnel(86400, 0, 86400, 2)(timestamp, timestamp, event = 1001, event = 1002) FROM funnel_test GROUP BY uid;

INSERT INTO funnel_test values (2,86400,1000,'a'),(2,86401,1001,'b'),(2,86402,1002,'c'),(2,86403,1003,'d'),(2,86404,1004,'e'),(2,86405,1005,'f'),(2,86406,1006,'g');
SELECT v2funnel(86400, 0, 86400, 2)(timestamp, timestamp, event = 1001, event = 1002) FROM funnel_test GROUP BY uid INTEREST EVENTS('a') SETTINGS exchange_parallel_size = 2;

DROP TABLE funnel_test;