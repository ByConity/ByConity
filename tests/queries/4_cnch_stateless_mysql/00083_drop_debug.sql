-- The include statement below is a temp one for tests that are yet to
--be ported to run with InnoDB,
--but needs to be kept for tests that would need MyISAM in future.
--source include/force_myisam_default.inc

-- 
-- DROP-related tests which execution requires debug server.
--
--source include/have_debug.inc

--##########################################################################
--echo
--echo # --
--echo # -- Bug#43138: DROP DATABASE failure does not clean up message list.
--echo # --
--echo

--disable_warnings
DROP DATABASE IF EXISTS mysql_test;
--enable_warnings

--echo
CREATE DATABASE mysql_test;
CREATE TABLE mysql_test.t1(a INT);
CREATE TABLE mysql_test.t2(b INT);
CREATE TABLE mysql_test.t3(c INT);

--echo
-- SET SESSION DEBUG = "+d,bug43138";

--echo
--sorted_result
DROP DATABASE mysql_test;

--echo
-- SET SESSION DEBUG = "-d,bug43138";

--echo
--echo # --
--echo # -- End of Bug#43138.
--echo # --

--##########################################################################
