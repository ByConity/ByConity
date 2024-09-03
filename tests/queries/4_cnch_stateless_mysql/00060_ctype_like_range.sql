--source include/have_debug.inc
--source include/have_ucs2.inc
--source include/have_utf16.inc
--source include/have_utf32.inc
--source include/have_utf8mb4.inc

--disable_warnings
DROP TABLE IF EXISTS t1;
DROP VIEW IF EXISTS v1;
--enable_warnings

CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, a VARBINARY(32));
INSERT INTO t1 (a) VALUES (''),('_'),('%'),('\_'),('\%'),('\\');
INSERT INTO t1 (a) VALUES ('a'),('c');
INSERT INTO t1 (a) VALUES ('a_'),('c_');
INSERT INTO t1 (a) VALUES ('a%'),('c%');
INSERT INTO t1 (a) VALUES ('aa'),('cc'),('ch');
INSERT INTO t1 (a) VALUES ('aa_'),('cc_'),('ch_');
INSERT INTO t1 (a) VALUES ('aa%'),('cc%'),('ch%');
INSERT INTO t1 (a) VALUES ('aaa'),('ccc'),('cch');
INSERT INTO t1 (a) VALUES ('aaa_'),('ccc_'),('cch_');
INSERT INTO t1 (a) VALUES ('aaa%'),('ccc%'),('cch%');
INSERT INTO t1 (a) VALUES ('aaaaaaaaaaaaaaaaaaaa');
INSERT INTO t1 (a) VALUES ('caaaaaaaaaaaaaaaaaaa');

CREATE VIEW v1 AS
  SELECT id, 'a' AS name, a AS val FROM t1
UNION
  SELECT id, 'sp', REPEAT('-', 32) AS sep FROM t1
ORDER BY id;
SELECT * FROM v1 order by name;

DROP VIEW v1;
DROP TABLE t1;
