-- Windows-specific partition tests
--source include/windows.inc
--source include/have_partition.inc

-- These tests contain Windows specific directory/file format.

--
-- Bug 25141: Crash Server on Partitioning command
--
-- Bug#30459: Partitioning across disks failing on Windows
-- updated this test, since symlinked files are not supported on Windows
-- (not the same as symlinked directories that have a special hack
-- on windows). This test is not dependent on have_symlink.

--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings

CREATE TABLE t1 (
  c1 int(10) unsigned NOT NULL,
  c2 varchar(30) NOT NULL,
  c3 smallint(5) unsigned DEFAULT NULL,
  PRIMARY KEY (c1)
) ENGINE = MYISAM;
INSERT INTO t1 VALUES (NULL, 'first', 1);
INSERT INTO t1 VALUES (NULL, 'second', 2);
INSERT INTO t1 VALUES (NULL, 'third', 3);
SHOW CREATE TABLE t1;
INSERT INTO t1 VALUES (NULL, 'last', 4);
SHOW CREATE TABLE t1;
DROP TABLE t1;
