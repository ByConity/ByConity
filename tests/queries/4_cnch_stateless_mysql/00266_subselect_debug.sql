-- The include statement below is a temp one for tests that are yet to
--be ported to run with InnoDB,
--but needs to be kept for tests that would need MyISAM in future.
--source include/force_myisam_default.inc

--source include/have_debug.inc

--
-- Bug #37627: Killing query with sum(exists()) or avg(exists()) reproducibly
-- crashes server
--
drop table if exists t1;
CREATE TABLE t1(id INT);
INSERT INTO t1 VALUES (1),(2),(3),(4);
INSERT INTO t1 SELECT a.id FROM t1 a,t1 b,t1 c,t1 d;
-- Setup the mysqld to crash at certain point
-- SET @orig_debug = @@debug;
-- SET SESSION debug="d,subselect_exec_fail";
SELECT SUM(EXISTS(SELECT RAND() FROM t1)) FROM t1;
SELECT REVERSE(EXISTS(SELECT RAND() FROM t1));
-- SET SESSION debug=@orig_debug;
DROP TABLE t1;
