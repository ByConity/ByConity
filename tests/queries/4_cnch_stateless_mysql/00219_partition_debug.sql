-- Partitioning test that require debug features

--source include/have_debug.inc
--source include/have_partition.inc

--echo #
--echo # Bug#13737949: CRASH IN HA_PARTITION::INDEX_INIT
--echo # Bug#18694052: SERVER CRASH IN HA_PARTITION::INIT_RECORD_PRIORITY_QUEUE
--echo #
CREATE TABLE t1 (a INT, b VARCHAR(64), KEY(b,a))
PARTITION BY HASH (a) PARTITIONS 3;
INSERT INTO t1 VALUES (1, '1'), (2, '2'), (3, '3'), (4, 'Four'), (5, 'Five'),
(6, 'Six'), (7, 'Seven'), (8, 'Eight'), (9, 'Nine');
-- SET SESSION debug='+d,partition_fail_index_init';
--error ER_NO_PARTITION_FOR_GIVEN_VALUE
SELECT * FROM t1 WHERE b = 'Seven';
-- SET SESSION debug='-d,partition_fail_index_init';
SELECT * FROM t1 WHERE b = 'Seven';
DROP TABLE t1;


--echo #
--echo # Bug #30355485	CRASH WHEN DOING ANY OPERATION ON SUB-PARTITIONED TABLE
--echo #

--source include/not_embedded.inc
--let $MYSQLD_DATADIR= `select @@datadir`
CREATE TABLE t1 (a INT, b INT) ENGINE = INNODB
PARTITION BY RANGE(a) SUBPARTITION BY HASH(b)
( PARTITION p0 VALUES LESS THAN (10) (SUBPARTITION s0, SUBPARTITION s1));

--disable_query_log
-- call mtr.add_suppression('InnoDB: Operating system error number 2 in a file operation');
-- call mtr.add_suppression('InnoDB: The error means the system cannot find the path specified');
-- call mtr.add_suppression('InnoDB: Cannot open datafile for read-only:');
-- call mtr.add_suppression('InnoDB: If you are installing InnoDB, remember that you must create directories yourself, InnoDB does not create them');
-- call mtr.add_suppression('InnoDB: Could not find a valid tablespace file for');
-- call mtr.add_suppression('InnoDB: Ignoring tablespace');
-- call mtr.add_suppression('InnoDB: Failed to find tablespace for table');
-- call mtr.add_suppression('InnoDB: Cannot calculate statistics for table');
-- call mtr.add_suppression('InnoDB: Missing .ibd file for table');
--enable_query_log

--echo # Shut down the server
--source include/shutdown_mysqld.inc
--echo # Removing test/t1#P#p0#SP#s0.ibd manually
--remove_file $MYSQLD_DATADIR/test/t1#P#p0#SP#s0.ibd
--source include/start_mysqld.inc

--error ER_NO_SUCH_TABLE
SELECT * FROM t1;
DROP TABLE t1;
