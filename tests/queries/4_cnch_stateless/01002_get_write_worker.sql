DROP TABLE IF EXISTS test.t_w;

SELECT getWriteWorker('test', 't_w', 'tcp'); -- { serverError 60 }
SELECT getWriteWorkers('test', 't_w', 'tcp'); -- { serverError 60 }

CREATE TABLE test.t_w(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY k ORDER BY m;

SELECT getWriteWorker('test', 't_w', 'tcp') FORMAT Null;
SELECT getWriteWorkers('test', 't_w', 'tcp') FORMAT Null;

DROP TABLE test.t_w;
