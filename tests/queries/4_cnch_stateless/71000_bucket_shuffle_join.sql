use test;
DROP TABLE IF EXISTS normal;
DROP TABLE IF EXISTS bucket_dtspartition;
DROP TABLE IF EXISTS bucket_dtspartition_with_range;
DROP TABLE IF EXISTS bucket_user_expression;
DROP TABLE IF EXISTS bucket_siphash;
DROP TABLE IF EXISTS bucket_siphash_split_number;
DROP TABLE IF EXISTS bucket_siphash_with_range;

CREATE TABLE normal (a String, b UInt64) ENGINE = CnchMergeTree() PARTITION BY a ORDER BY a;
CREATE TABLE bucket_dtspartition (c String, d UInt64) ENGINE = CnchMergeTree() PARTITION BY c CLUSTER BY  d INTO 4 BUCKETS SPLIT_NUMBER 60 ORDER BY c;
CREATE TABLE bucket_dtspartition_with_range (c String, d UInt64) ENGINE = CnchMergeTree() PARTITION BY c CLUSTER BY  d INTO 4 BUCKETS SPLIT_NUMBER 60 WITH_RANGE ORDER BY c;
CREATE TABLE bucket_user_expression (c String, d UInt64) ENGINE = CnchMergeTree() PARTITION BY c CLUSTER BY expression d INTO 4 BUCKETS ORDER BY c;
CREATE TABLE bucket_siphash (d String, e UInt64, f String) ENGINE = CnchMergeTree() PARTITION BY d CLUSTER BY (e, f) INTO 4 BUCKETS ORDER BY d;
CREATE TABLE bucket_siphash_split_number (d String, e UInt64, f String) ENGINE = CnchMergeTree() PARTITION BY d CLUSTER BY (e, f) INTO 4 BUCKETS SPLIT_NUMBER 10 ORDER BY d;
CREATE TABLE bucket_siphash_with_range (d String, e UInt64, f String) ENGINE = CnchMergeTree() PARTITION BY d CLUSTER BY (e, f) INTO 4 BUCKETS SPLIT_NUMBER 10 WITH_RANGE ORDER BY d;


INSERT INTO normal VALUES ('n1', 1)('n2', 2)('n3', 7)('n3', 25);
INSERT INTO bucket_dtspartition VALUES ('cdw_1', 1)('cdw_2', 2)('cdw_1', 5)('cdw_3', 25);
INSERT INTO bucket_dtspartition_with_range VALUES ('cdw_1', 1)('cdw_2', 2)('cdw_1', 5)('cdw_1', 6)('cdw_1', 7)('cdw_3', 25);
INSERT INTO bucket_user_expression VALUES ('cdw_1', 0) ('cdw_1', 1)('cdw_2', 2)('cdw_3', 3);
INSERT INTO bucket_siphash VALUES ('cdw_1', 1, 'n1')('cdw_2', 2, 'n2' )('cdw_1', 5, 'n5')('cdw_3', 25,'n1');
INSERT INTO bucket_siphash_split_number VALUES ('cdw_1', 1, 'n1')('cdw_2', 2, 'n2' )('cdw_1', 5, 'n5')('cdw_3', 25,'n1');
INSERT INTO bucket_siphash_with_range VALUES ('cdw_1', 1, 'n1')('cdw_2', 2, 'n2' )('cdw_1', 5, 'n5')('cdw_3', 25,'n1');

SET enable_optimizer=1;
SET bsp_mode=0; -- bsp mode does not support bucket join

SET enum_replicate_no_stats=0,enable_bucket_shuffle=1;
SELECT 'enable bucket shuffle';
SELECT 'dtspartition';
SELECT *　FROM test.bucket_dtspartition b　LEFT JOIN test.normal n ON b.d=n.b ORDER BY d;
SELECT *　FROM test.bucket_dtspartition b　Right JOIN test.normal n ON b.d=n.b ORDER BY b;
SELECT *　FROM test.bucket_dtspartition_with_range b　LEFT JOIN test.normal n ON b.d=n.b ORDER BY d;
SELECT *　FROM test.bucket_dtspartition_with_range b　RIGHT JOIN test.normal n ON b.d=n.b ORDER BY b;
SELECT 'toUInt64';
SELECT * FROM test.bucket_user_expression b LEFT JOIN test.normal n ON b.d=n.b ORDER BY d;
SELECT * FROM test.bucket_user_expression b RIGHT JOIN test.normal n ON b.d=n.b ORDER BY b;
SELECT 'sipHash';
SELECT * FROM test.bucket_siphash b LEFT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY e;
SELECT * FROM test.bucket_siphash b RIGHT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY b;
SELECT * FROM test.bucket_siphash_split_number b LEFT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY e;
SELECT * FROM test.bucket_siphash_split_number b RIGHT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY b;
SELECT * FROM test.bucket_siphash_with_range b LEFT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY e;
SELECT * FROM test.bucket_siphash_with_range b RIGHT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY b;
SET enum_replicate_no_stats=1,enable_bucket_shuffle=0;
SELECT 'disable bucket shuffle';
SELECT 'dtspartition';
SELECT *　FROM test.bucket_dtspartition b　LEFT JOIN test.normal n ON b.d=n.b ORDER BY d;
SELECT *　FROM test.bucket_dtspartition b　Right JOIN test.normal n ON b.d=n.b ORDER BY b;
SELECT *　FROM test.bucket_dtspartition_with_range b　LEFT JOIN test.normal n ON b.d=n.b ORDER BY d;
SELECT *　FROM test.bucket_dtspartition_with_range b　RIGHT JOIN test.normal n ON b.d=n.b ORDER BY b;
SELECT 'toUInt64';
SELECT * FROM test.bucket_user_expression b LEFT JOIN test.normal n ON b.d=n.b ORDER BY d;
SELECT * FROM test.bucket_user_expression b RIGHT JOIN test.normal n ON b.d=n.b ORDER BY b;
SELECT 'sipHash';
SELECT * FROM test.bucket_siphash b LEFT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY e;
SELECT * FROM test.bucket_siphash b RIGHT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY b;
SELECT * FROM test.bucket_siphash_split_number b LEFT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY e;
SELECT * FROM test.bucket_siphash_split_number b RIGHT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY b;
SELECT * FROM test.bucket_siphash_with_range b LEFT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY e;
SELECT * FROM test.bucket_siphash_with_range b RIGHT JOIN test.normal n ON b.e=n.b AND b.f = n.a ORDER BY b;
