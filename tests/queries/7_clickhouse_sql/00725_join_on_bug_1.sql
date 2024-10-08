DROP TABLE IF EXISTS a1;
DROP TABLE IF EXISTS a2;
CREATE TABLE a1(a UInt8, b UInt8) ENGINE=CnchMergeTree ORDER BY a;
CREATE TABLE a2(a UInt8, b UInt8) ENGINE=CnchMergeTree ORDER BY a;
INSERT INTO a1 VALUES (1, 1), (1, 2), (2, 3);
INSERT INTO a2 VALUES (1, 2), (1, 3), (1, 4);
SELECT * FROM a1 as a left JOIN a2 as b on a.a=b.a ORDER BY b SETTINGS join_default_strictness='ANY';
SELECT '-';
SELECT a1.*, a2.* FROM a1 ANY LEFT JOIN a2 USING a ORDER BY b;
DROP TABLE IF EXISTS a1;
DROP TABLE IF EXISTS a2;
