--source include/have_debug.inc

--echo #
--echo # Start of 5.7 tests
--echo #

--echo #
--echo # BUG#20507804 "FAILING ASSERTION: TRX->READ_ONLY && TRX->AUTO_COMMIT
--echo #               && TRX->ISOLATION_LEVEL==1".
--echo #

--echo # New connection which is closed after the test is needed to reproduce
--echo # the original problem.
-- connect (con1,localhost,root,,);

-- SET DEBUG='+d,mysql_lock_tables_kill_query';
SELECT CONVERT_TZ('2003-10-26 01:00:00', 'GMT', 'UTC');

-- disconnect con1;
--source include/wait_until_disconnected.inc
-- connection default;

--echo #
--echo # End of 5.7 tests
--echo #

