use test;
DROP TABLE IF EXISTS funnel_rep_test;
CREATE TABLE funnel_rep_test (uid UInt32 default 1, timestamp UInt64, event UInt32, prop String) engine=CnchMergeTree() order by uid;
INSERT INTO funnel_rep_test (timestamp, event, prop) values (86400, 1001, '1'),(86401, 1001, '2'), (86402, 1002, '2'), (86403, 1001, '1'), (86404, 1004, '1'), (86405, 1003, '2'), (86406, 1004, '2'), (86407, 1001, '1'), (86408, 1002, '2'),(86409, 1002, '1'), (86410, 1001, '2'), (86411, 1003, '1'), (86412, 1002, '2'), (86413, 1003, '2'), (86414, 1004, '1'), (86415, 1004, '2') , (86416, 1001, '1'), (86417, 1003, '2'), (86418, 1002, '2'), (86419, 1001, '1'), (86420, 1003, '1'), (86421, 1001, '1'), (86422, 1004, '2'), (86423, 1002, '1');

SELECT
    funnel_tmp_res.1 AS col1,
    funnelRep(5, 4)(funnel_tmp_res.2) AS col2
FROM
(
    SELECT arrayJoin(finderGroupFunnel(5, 86400, 1, 5, 3, 0, 1, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), event = 1001, event = 1002, event = 1003, event = 1004)) AS funnel_tmp_res
    FROM funnel_rep_test
    GROUP BY uid
)
GROUP BY col1
ORDER BY col2 ASC;

SELECT
    funnel_tmp_res.1 AS col1,
    funnelRep2(5, 4, 1, [0, 2, 4, 6])(funnel_tmp_res.2, funnel_tmp_res.3) AS col2
FROM
(
    SELECT arrayJoin(finderGroupFunnel(5, 86400, 1, 5, 3, 0, 1, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), event = 1001, event = 1002, event = 1003, event = 1004)) AS funnel_tmp_res
    FROM funnel_rep_test
    GROUP BY uid
)
GROUP BY col1
ORDER BY col2 ASC;

SELECT
    funnel_tmp_res.1 AS col1,
    funnelRep2(5, 4, 0, [0, 12, 24, 36])(funnel_tmp_res.2, funnel_tmp_res.3) AS col2
FROM
(
    SELECT arrayJoin(finderGroupFunnel(5, 86400, 1, 5, 3, 0, 1, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), event = 1001, event = 1002, event = 1003, event = 1004)) AS funnel_tmp_res
    FROM funnel_rep_test
    GROUP BY uid
)
GROUP BY col1
ORDER BY col2 ASC;

SELECT
    funnel_tmp_res.1 AS col1,
    funnelRep3(24, 4)(funnel_tmp_res.2, funnel_tmp_res.3) AS col2
FROM
(
    SELECT arrayJoin(finderGroupFunnel(5, 86400, 1, 24, 4, 0, 1, 'Asia/Shanghai', 1)(timestamp, timestamp, if(prop = '1', '1', '2'), event = 1001, event = 1002, event = 1003, event = 1004)) AS funnel_tmp_res
    FROM funnel_rep_test
    GROUP BY uid
)
GROUP BY col1
ORDER BY col2 ASC;

DROP TABLE IF EXISTS funnel_rep_test;