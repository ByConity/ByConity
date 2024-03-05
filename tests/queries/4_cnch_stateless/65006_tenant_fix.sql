DROP DATABASE IF EXISTS test_db;
CREATE DATABASE test_db;

SELECT '----- check for X.Y.Z selecting ------';
CREATE TABLE test_db.test_tb (arr Array(String), id UInt32) Engine = CnchMergeTree ORDER BY tuple();
insert into test_db.test_tb values (['Abc','Df','Q'], 2), (['Abc','DEFQ'], 3), (['ABC','Q','ERT'], 1), (['Ab','ber'], 4), (['A'], 0);
select test_db.test_tb.id, test_tb.id, id from test_db.test_tb;
select test_db.test_tb.arr[id], test_tb.arr[id], arr[id], arr[test_db.test_tb.id] from test_db.test_tb;

SELECT '----- check for X.Y.Z selecting for multi-table------';
CREATE TABLE test_db.test_tb1 (arr Array(String), id UInt32) Engine = CnchMergeTree ORDER BY tuple();
insert into test_db.test_tb1 values (['Abc','Df','Q'], 0), (['Abc','DEFQ'], 2), (['ABC','Q','ERT'], 1), (['Ab','ber'], 3), (['A'], 4);
select test_db.test_tb.id, test_db.test_tb1.id, id, arr[id], arr[test_db.test_tb.id], test_db.test_tb.arr[id] from test_db.test_tb join test_db.test_tb1 on test_db.test_tb.id = test_db.test_tb1.id and test_tb.id = test_db.test_tb1.id;