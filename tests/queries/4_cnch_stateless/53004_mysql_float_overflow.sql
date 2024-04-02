-- float, double
-- throw_on_numeric_overflow 0 1 
-- null or not null 
-- csv, TabSeparated, json, values 

set dialect_type='MYSQL';

SELECT 'Float NOT NULL';
DROP TABLE IF EXISTS overflow;
CREATE TABLE overflow (x Int NOT NULL, d Float NOT NULL) engine=CnchMergeTree() order by x;
INSERT INTO overflow VALUES (0, 1.23), (1, inf), (2, -inf), (3, -nan), (4, 3.402823466E+38), (5, 3.402823466E+39);
INSERT INTO overflow FORMAT CSV 6, 1234567890123456789012345678901234567890
INSERT INTO overflow FORMAT JSONEachRow {"x": 7, "d": -1234567890123456789012345678901234567890}, {"x": 8, "d":1234567890123456789.012345678901234567890} 
INSERT INTO overflow FORMAT JSONEachRow  {"x": 9, "d": 1.234567890123456789012345678901234567890}
SELECT * from overflow order by x;

SELECT 'Float NULL';
DROP TABLE IF EXISTS overflow;
CREATE TABLE overflow (x Int NOT NULL, d Float NULL) engine=CnchMergeTree() order by x;
INSERT INTO overflow VALUES (0, 1.23), (1, inf), (2, -inf), (3, -nan), (4, 3.402823466E+38), (5, 3.402823466E+39);
INSERT INTO overflow FORMAT CSV 6, 1234567890123456789012345678901234567890
INSERT INTO overflow FORMAT JSONEachRow {"x": 7, "d": -1234567890123456789012345678901234567890}, {"x": 8, "d":1234567890123456789.012345678901234567890} 
INSERT INTO overflow FORMAT JSONEachRow  {"x": 9, "d": 1.234567890123456789012345678901234567890}
SELECT * from overflow order by x;


SELECT 'Double NOT NULL';
DROP TABLE IF EXISTS overflow;
CREATE TABLE overflow (x Int NOT NULL, d Double NOT NULL) engine=CnchMergeTree() order by x;
INSERT INTO overflow VALUES (0, 1.23), (1, inf), (2, -inf), (3, -nan), (4, 1.79769e+308), (5, 1.79769e+309);
INSERT INTO overflow FORMAT CSV 6, 1234567890123456789012345678901234567890
INSERT INTO overflow FORMAT JSONEachRow {"x": 7, "d": -1.79769e+308}, {"x": 8, "d": -1.79769e+309} 
INSERT INTO overflow FORMAT JSONEachRow  {"x": 9, "d": 1.234567890123456789012345678901234567890}
SELECT * from overflow order by x;

SELECT 'Double NULL';
DROP TABLE IF EXISTS overflow;
CREATE TABLE overflow (x Int NOT NULL, d Double NULL) engine=CnchMergeTree() order by x;
INSERT INTO overflow VALUES (0, 1.23), (1, inf), (2, -inf), (3, -nan), (4, 1.79769e+308), (5, 1.79769e+309);
INSERT INTO overflow FORMAT CSV 6, 1234567890123456789012345678901234567890
INSERT INTO overflow FORMAT JSONEachRow {"x": 7, "d": -1.79769e+308}, {"x": 8, "d": -1.79769e+309} 
INSERT INTO overflow FORMAT JSONEachRow  {"x": 9, "d": 1.234567890123456789012345678901234567890}
SELECT * from overflow order by x;
