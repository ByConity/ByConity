-- The include statement below is a temp one for tests that are yet to
--be ported to run with InnoDB,
--but needs to be kept for tests that would need MyISAM in future.
--source include/force_myisam_default.inc

--Want to skip this test from daily Valgrind execution
--source include/no_valgrind_without_big.inc
-- Turn on compression between the client and server
-- and run a number of tests

-- Can't test with embedded server
-- source include/not_embedded.inc

-- source include/have_compress.inc

-- Save the initial number of concurrent sessions
--source include/count_sessions.inc


-- connect (comp_con,localhost,root,,,,,COMPRESS);

-- Check compression turned on
SHOW STATUS LIKE 'Compression';
--disable_warnings
select * from information_schema.session_status where variable_name= 'COMPRESSION';
--enable_warnings

-- Source select test case
-- source include/common-tests.inc

-- Check compression turned on
SHOW STATUS LIKE 'Compression';

-- connection default;
-- disconnect comp_con;

-- Wait till all disconnects are completed
--source include/wait_until_count_sessions.inc

