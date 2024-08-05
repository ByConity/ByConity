CREATE TABLE 10300_bsp_schedule_timeout(a UInt32) ENGINE=CnchMergeTree() ORDER BY a;
INSERT INTO 10300_bsp_schedule_timeout (a) VALUES (1);
SELECT sleepEachRow(2) FROM 10300_bsp_schedule_timeout settings bsp_mode=1,enable_optimizer=1,max_execution_time=1; -- { serverError 49 }
