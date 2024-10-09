select 'create table, column +type +NULL';
DROP TABLE IF EXISTS null_before1;
CREATE TABLE null_before1 (id INT NULL, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before1;

select 'create table, column +type +NOT NULL';
DROP TABLE IF EXISTS null_before2;
CREATE TABLE null_before2 (id INT NOT NULL, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before2;

select 'create table, column +type +NULL +DEFAULT';
DROP TABLE IF EXISTS null_before3;
CREATE TABLE null_before3 (id INT NULL DEFAULT 1, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before3;

select 'create table, column +type +NOT NULL +DEFAULT';
DROP TABLE IF EXISTS null_before4;
CREATE TABLE null_before4 (id INT NOT NULL DEFAULT 1, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before4;

select 'create table, column +type +DEFAULT +NULL';
DROP TABLE IF EXISTS null_before5;
CREATE TABLE null_before5 (id INT DEFAULT 1 NULL, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before5;

select 'create table, column +type +DEFAULT +NOT NULL';
DROP TABLE IF EXISTS null_before;
CREATE TABLE null_before (id INT DEFAULT 1 NOT NULL, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before;

select 'create table, column -type +NULL +DEFAULT';
DROP TABLE IF EXISTS null_before6;
CREATE TABLE null_before6 (id NULL DEFAULT 1, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before6;

select 'create table, column -type +NOT NULL +DEFAULT';
DROP TABLE IF EXISTS null_before7;
CREATE TABLE null_before7 (id NOT NULL DEFAULT 1, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before7;

select 'create table, column -type +DEFAULT +NULL';
DROP TABLE IF EXISTS null_before8;
CREATE TABLE null_before8 (id DEFAULT 1 NULL, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before8;

select 'create table, column -type +DEFAULT +NOT NULL';
DROP TABLE IF EXISTS null_before9;
CREATE TABLE null_before9 (id DEFAULT 1 NOT NULL, pid Int32 NOT NULL) ENGINE=CnchMergeTree() ORDER BY pid;
DESCRIBE TABLE null_before9;

DROP TABLE IF EXISTS null_before1;
DROP TABLE IF EXISTS null_before2;
DROP TABLE IF EXISTS null_before3;
DROP TABLE IF EXISTS null_before4;
DROP TABLE IF EXISTS null_before5;
DROP TABLE IF EXISTS null_before6;
DROP TABLE IF EXISTS null_before7;
DROP TABLE IF EXISTS null_before8;
DROP TABLE IF EXISTS null_before9;
