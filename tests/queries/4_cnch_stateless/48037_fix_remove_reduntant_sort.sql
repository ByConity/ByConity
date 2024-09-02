drop table if exists database_214;
create database if not exists database_214;
CREATE TABLE if not exists database_214.fill (
    date Date, val Int, str String, a Decimal(10,2), date_time DateTime
) ENGINE = CnchMergeTree order by val;
INSERT INTO database_214.fill VALUES (toDate('2019-05-24'), 13, 'sd0', 12.2, '2022-01-01 10:00:00'),(toDate('2019-05-10'), 16, 'vp7', 1.2, '2022-01-01 10:00:00'),(toDate('2019-05-25'), 17, '0ei', 13.2, '2022-01-01 10:00:00'),(toDate('2019-05-30'), 18, '3kd', 35.2, '2022-01-01 10:00:00'), (toDate('2019-05-15'), 27, 'enb',64.5, '2022-01-01 10:00:00');
SELECT count(*) FROM (
    SELECT * FROM database_214.fill ORDER BY 
    date WITH fill FROM toDate('2019-05-01') TO toDate('2019-05-10') STEP 3, 
    val WITH fill FROM 101 TO 125 STEP 8,
    date_time WITH fill FROM toDateTime('2022-01-01 10:00:10') TO toDateTime('2022-01-01 10:00:20') STEP 3
) format Null;

select * from database_214.fill order by val limit 1 format Null;

drop table if exists database_214;
