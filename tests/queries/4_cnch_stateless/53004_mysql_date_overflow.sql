-- date, date32, datetime, datetime64 
-- throw_on_date_overflow 0 1 
-- null or not null 
-- csv, TabSeparated, json, values 

use test;
set dialect_type='MYSQL';
SET throw_on_date_overflow = 0;

SELECT 'Date NOT NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d Date NOT NULL) engine=CnchMergeTree() order by x;
INSERT INTO date_overflow VALUES (0, '0001-01-01'), (1, '1970-01-01'), (2, '1970-01-02'), (3, '2149-06-05'), (4, '2149-06-06'), (9, '9999-12-31');
INSERT INTO date_overflow FORMAT CSV 5,'1969-12-31'
INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2149-06-07"}, {"x": 7, "d": "2024-13-01"}
INSERT INTO date_overflow FORMAT JSONEachRow  {"x": 8, "d": "2024-04-31"}
SELECT * from date_overflow order by x;

SELECT 'Date NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d Nullable(Date)) engine=CnchMergeTree() order by tuple();
INSERT INTO date_overflow VALUES (0, '0001-01-01'), (1, '1970-01-01'), (2, '1970-01-02'), (3, '2149-06-05'), (4, '2149-06-06');
INSERT INTO date_overflow FORMAT CSV 5,'1969-12-31'
INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2149-06-07"}, {"x": 7, "d": "2024-13-01"}
SELECT * from date_overflow order by x;


SELECT 'Date32 NOT NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d Date32 NOT NULL) engine=CnchMergeTree() order by x;
INSERT INTO date_overflow VALUES (0, '0001-01-01'), (1, '1900-01-01'), (2, '1900-01-02'), (3, '2299-12-30'), (4, '2299-12-31')
INSERT INTO date_overflow FORMAT CSV 5,'1899-12-31'
INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2300-01-01"}, {"x": 7, "d": "2024-13-01"}
SELECT * from date_overflow order by x;


SELECT 'Date32 NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d Nullable(Date32)) engine=CnchMergeTree() order by x;
INSERT INTO date_overflow VALUES (0, '0001-01-01'), (1, '1900-01-01'), (2, '1900-01-02'), (3, '2299-12-30'), (4, '2299-12-31');
INSERT INTO date_overflow FORMAT CSV 5,'1899-12-31'
INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2300-01-01"}, {"x": 7, "d": "2024-13-01"}
SELECT * from date_overflow order by x;


SELECT 'DateTime NOT NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d DateTime('UTC') NOT NULL) engine=CnchMergeTree() order by x;
INSERT INTO date_overflow VALUES (0, '0001-01-01 00:00:00'), (1, '1970-01-01 00:00:00'), (2, '1970-01-02 00:00:00'), (3, '2106-02-07 06:28:15'), (4, '2106-02-07 06:28:16');
INSERT INTO date_overflow FORMAT CSV 5,'1969-12-31 23:59:59'
INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2107-02-08 06:28:16"}, {"x": 7, "d": "2024-13-01 11:12:12"}
SELECT * from date_overflow order by x;


SELECT 'DateTime UTC+8 NOT NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d DateTime('Etc/GMT-8') NOT NULL) engine=CnchMergeTree() order by x;
INSERT INTO date_overflow VALUES (0, '0001-01-01 00:00:00'), (1, '1970-01-01 00:00:00'), (2, '1970-01-02 00:00:00'), (3, '2106-02-07 06:28:15'), (4, '2106-02-07 06:28:16');
INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "1970-01-01 08:00:00"}, {"x": 7, "d": "1970-01-01 07:59:59"}, {"x": 8, "d": "2106-02-07 14:28:15"}, {"x": 9, "d": "2106-02-07 14:28:16"}
SELECT * from date_overflow order by x;


SELECT 'DateTime NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d Nullable(DateTime('UTC'))) engine=CnchMergeTree() order by x;
INSERT INTO date_overflow VALUES (0, '0001-01-01 00:00:00'), (1, '1970-01-01 23:59:59'), (2, '1970-01-02 00:00:00'), (3, '2106-02-06 23:59:59'), (4, '2106-02-07 00:00:00');
INSERT INTO date_overflow FORMAT CSV 5,'1969-12-31 23:59:59'
INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2106-02-07 06:28:16"}, {"x": 7, "d": "2024-13-01 11:12:12"}
SELECT * from date_overflow order by x;


SELECT 'DateTime64 NOT NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d DateTime64(3, 'UTC') NOT NULL) engine=CnchMergeTree() order by x;
INSERT INTO date_overflow VALUES (0, '0001-01-01 00:00:00.000'), (1, '1900-01-01 00:00:00.000'), (2, '1900-01-02 00:00:00'), (3, '2299-12-31 23:59:59.9999'), (4, '2300-01-01 00:00:00');
INSERT INTO date_overflow FORMAT CSV 5,'1899-12-31 23:59:59.999'
INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2300-01-02 00:00:00"}, {"x": 7, "d": "2024-13-01 11:12:12.123"}
SELECT * from date_overflow order by x;


SELECT 'DateTime64 UTC+8 NOT NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d DateTime64(3, 'Etc/GMT-8') NOT NULL) engine=CnchMergeTree() order by x;
INSERT INTO date_overflow VALUES (0, '0001-01-01 00:00:00.000'), (1, '1900-01-01 00:00:00.000'), (2, '1900-01-02 00:00:00'), (3, '2299-12-31 23:59:59.9999'), (4, '2300-01-01 07:59:59.999'), (5, '2300-01-01 08:00:00');
INSERT INTO date_overflow VALUES (6, '1900-01-01 07:00:00.000'), (7, '1900-01-01 08:00:00.000'), (8, '1900-01-01 07:59:59.999')
SELECT * from date_overflow order by x;


SELECT 'DateTime64 NULL';
DROP TABLE IF EXISTS date_overflow;
CREATE TABLE date_overflow (x Int NOT NULL, d Nullable(DateTime64(3, 'UTC'))) engine=CnchMergeTree() order by x;
INSERT INTO date_overflow VALUES (0, '0001-01-01 00:00:00.000'), (1, '1900-01-01 00:00:00.000'), (2, '1900-01-02 00:00:00'), (3, '2299-12-30 23:59:59.9999'), (4, '2299-12-31 00:00:00');
INSERT INTO date_overflow FORMAT CSV 5,'1899-12-31 23:59:59.999'
INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2300-01-01 00:00:00"}, {"x": 7, "d": "2024-13-01 11:12:12.123"}
SELECT * from date_overflow order by x;
