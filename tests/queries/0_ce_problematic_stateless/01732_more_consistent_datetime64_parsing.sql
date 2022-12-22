CREATE TEMPORARY TABLE t (i UInt8, x DateTime64(3, 'UTC'));
INSERT INTO t VALUES (1, 1111111111222);
INSERT INTO t VALUES (2, 1111111111.222);
INSERT INTO t VALUES (3, '1111111111222');
INSERT INTO t VALUES (4, '1111111111.222');
INSERT INTO t VALUES (5, '2020-01-01 05:05:30.2222+12:30');
INSERT INTO t VALUES (6, '2020-01-01 05:05:30.2222-12:30');
SELECT * FROM t ORDER BY i;

CREATE TEMPORARY TABLE t2 (i UInt8, x DateTime64(3, 'Asia/Shanghai'));
-- Insert 1 and 2 both should lead to the same time.
INSERT INTO t2 VALUES (1, '2020-03-07 00:00:00.2222'::DateTime64(3, 'Asia/Kolkata'));
INSERT INTO t2 VALUES (2, '2020-03-07 00:00:00.2222+05:30');
INSERT INTO t2 VALUES (3, '2020-03-07 00:00:00.2222'::DateTime64(3, 'America/New_York'));
-- America/New_York is UTC-4 but due to DST, it's UTC-5, so insert 3 and 4 will result in different times.
INSERT INTO t2 VALUES (4, '2020-03-07 00:00:00.2222-04:00');
select * from t2 order by i;

SELECT toDateTime64(1111111111.222, 3);
SELECT toDateTime64('1111111111.222', 3);
SELECT toDateTime64('1111111111222', 3);
SELECT ignore(toDateTime64(1111111111222, 3)); -- This gives somewhat correct but unexpected result
