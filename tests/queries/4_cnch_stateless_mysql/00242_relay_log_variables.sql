
--source include/not_embedded.inc
--source include/not_relay_log_info_table.inc

--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
SHOW VARIABLES LIKE 'relay_log%'
