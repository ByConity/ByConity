SELECT '05:20:30.55555'::Time;
SELECT '05:20:30.55555'::Time(6);
SELECT '05:20:30.55555'::Time(9);
SELECT '05:20:30'::Time;


SELECT toTimeType('05:20:30.55555', 3);
SELECT toTimeType('23:20:30.55', 3);
SELECT toTimeType('05:20:30', 3);


DROP TABLE IF EXISTS table_time_type;
CREATE TABLE table_time_type
(
    a Int64,
    b String,
    c DateTime(8)
) ENGINE=CnchMergeTree() ORDER BY a;

INSERT INTO table_time_type VALUES (1, '2023-05-11 18:58:22', '2023-05-11 18:58:22')(2, '18:58:22', '2023-05-11 18:58:22')(3, '2023-05-11 18:58:22.1232', '2023-05-11 18:58:22.1232')(4, '2023-05-11 18:58:22.32', '2023-05-11 18:58:22.32')(5, '2023-05-11 18:58:22.123232', '2023-05-11 18:58:22.123232');

SELECT toTimeType(b), toTimeType(c) FROM table_time_type;

DROP TABLE table_time_type;