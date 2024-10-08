DROP TABLE IF EXISTS test_pathfind;

CREATE TABLE test_pathfind (`uid` String, `event` String, `time` UInt64, `prop` String, `extra_prop_1` String, `extra_prop_2` String) ENGINE = CnchMergeTree() ORDER BY uid SETTINGS index_granularity = 8192;
INSERT INTO test_pathfind VALUES('1','e1',1,'p1','ep1-1','ep2-1'),('1','e2',2,'p2','ep1-2','ep2-2'),('1','e2',3,'p3','ep1-3','ep2-3'),('1','e2',4,'p4','ep1-4','ep2-4'),('1','e2',5,'p5','ep1-5','ep2-5'),('1','e1',6,'p6','ep1-6','ep2-6'),('1','e2',7,'p7','ep1-7','ep2-7'),('1','e1',8,'p8','ep1-8','ep2-8'),('1','e3',9,'p9','ep1-9','ep2-9');

SELECT funnelPathSplit(4, 10, 3, [0])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;
SELECT funnelPathSplit(4, 10, 3, [1])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;
SELECT funnelPathSplit(4, 10, 3, [2])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;
SELECT funnelPathSplit(4, 10, 3, [3])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;
SELECT funnelPathSplit(4, 10, 3, [5])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;
SELECT funnelPathSplit(4, 10, 3, [7])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid; -- { serverError 36 }

SELECT funnelPathSplitR(4, 2, 3, [0])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;
SELECT funnelPathSplitD(4, 3, 3, [1])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;
SELECT funnelPathSplitRD(4, 1, 3, [2])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;
SELECT funnelPathSplit(4, 4, 3, [3])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;
SELECT funnelPathSplit(4, 5, 3, [5])(time, multiIf(event = 'e1', 1, event = 'e2', 2, event = 'e3', 3, 0) AS e, prop, extra_prop_1, extra_prop_2) AS paths FROM test_pathfind GROUP BY uid;

DROP TABLE IF EXISTS test_pathfind;
