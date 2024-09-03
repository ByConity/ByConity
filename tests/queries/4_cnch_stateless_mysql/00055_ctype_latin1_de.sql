-- The include statement below is a temp one for tests that are yet to
--be ported to run with InnoDB,
--but needs to be kept for tests that would need MyISAM in future.
--source include/force_myisam_default.inc

--
-- Test latin_de character set
--

-- set names latin1;
-- set @@collation_connection=latin1_german2_ci;

-- select @@collation_connection;

--disable_warnings
drop table if exists t1;
--enable_warnings

create table t1 (a char (20) not null, b int not null auto_increment, index (a,b));
insert into t1 (a) values ('�'),('ac'),('ae'),('ad'),('�c'),('aeb');
insert into t1 (a) values ('�c'),('uc'),('ue'),('ud'),('�'),('ueb'),('uf');
insert into t1 (a) values ('�'),('oc'),('�a'),('oe'),('od'),('�c'),('oeb');
insert into t1 (a) values ('s'),('ss'),('�'),('�b'),('ssa'),('ssc'),('�a');
insert into t1 (a) values ('e�'),('u�'),('�o'),('��'),('��a'),('aeae');
insert into t1 (a) values ('q'),('a'),('u'),('o'),('�'),('�'),('a');
select a,b from t1 order by a,b;
select a,b from t1 order by upper(a),b;
select a from t1 order by a desc;
check table t1;
select * from t1 where a like '�%';
select * from t1 where a like binary '%�%';
select * from t1 where a like '%�%';
select * from t1 where a like '%U%';
select * from t1 where a like '%ss%';
drop table t1;

-- The following should all be true
select strcmp('�','ae'),strcmp('ae','�'),strcmp('aeq','�q'),strcmp('�q','aeq');
select strcmp('ss','�'),strcmp('�','ss'),strcmp('�s','sss'),strcmp('�q','ssq');

-- The following should all return -1
select strcmp('�','af'),strcmp('a','�'),strcmp('��','aeq'),strcmp('��','aeaeq');
select strcmp('ss','�a'),strcmp('�','ssa'),strcmp('s�a','sssb'),strcmp('s','�');
select strcmp('�','o�'),strcmp('�','u�'),strcmp('�','oeb');

-- The following should all return 1
select strcmp('af','�'),strcmp('�','a'),strcmp('aeq','��'),strcmp('aeaeq','��');
select strcmp('�a','ss'),strcmp('ssa','�'),strcmp('sssb','s�a'),strcmp('�','s');
select strcmp('u','�a'),strcmp('u','�');

--
-- overlapping combo's
--
select strcmp('s�', '�a'), strcmp('a�', '�x');
--
-- Test bug report #152 (problem with index on latin1_de)
--

--
-- The below checks both binary and character comparisons.
--
create table t1 (word varchar(255) not null, word2 varchar(255) not null default '', index(word));
show create table t1;
insert into t1 (word) values ('ss'),(0xDF),(0xE4),('ae');
update t1 set word2=word;
select word, word=binary 0xdf as t from t1 having t > 0;
select word, word=cast(0xdf AS CHAR) as t from t1 having t > 0;
select * from t1 where word=binary 0xDF;
select * from t1 where word=CAST(0xDF as CHAR);
select * from t1 where word2=binary 0xDF;
select * from t1 where word2=CAST(0xDF as CHAR);
select * from t1 where word='ae';
select * from t1 where word= 0xe4 or word=CAST(0xe4 as CHAR);
select * from t1 where word between binary 0xDF and binary 0xDF;
select * from t1 where word between CAST(0xDF AS CHAR) and CAST(0xDF AS CHAR);
select * from t1 where word like 'ae';
select * from t1 where word like 'AE';
select * from t1 where word like binary 0xDF;
select * from t1 where word like CAST(0xDF as CHAR);
drop table t1;

--
-- Bug #5447 Select does not find records
--
CREATE TABLE t1 (
  autor varchar(80) NOT NULL default '',
  PRIMARY KEY  (autor)
);
INSERT INTO t1 VALUES ('Powell, B.'),('Powell, Bud.'),('Powell, L. H.'),('Power, H.'),
('Poynter, M. A. L. Lane'),('Poynting, J. H. und J. J. Thomson.'),('Pozzi, S(amuel-Jean).'),
('Pozzi, Samuel-Jean.'),('Pozzo, A.'),('Pozzoli, Serge.');
SELECT * FROM t1 WHERE autor LIKE 'Poz%' ORDER BY autor;
DROP TABLE t1;

--
-- Test of special character in german collation
--

CREATE TABLE t1 (
s1 CHAR(5) CHARACTER SET latin1 COLLATE latin1_german2_ci
);
show create table t1;
INSERT INTO t1 VALUES ('�');
INSERT INTO t1 VALUES ('ue');
SELECT DISTINCT s1 FROM t1;
SELECT s1,COUNT(*) FROM t1 GROUP BY s1;
SELECT COUNT(DISTINCT s1) FROM t1;
-- SELECT FIELD('ue',s1), FIELD('�',s1), s1='ue', s1='�' FROM t1;
DROP TABLE t1;

-- source include/ctype_filesort.inc
-- source include/ctype_german.inc

--
-- Bug#7878 with utf8_general_ci, equals (=) has problem with
-- accent insensitivity.
-- Although originally this problem was found with UTF8 character set,
-- '=' behaved wrong for latin1_german2_ci as well.
-- Let's check it does not work incorrect anymore.
-- 
-- SET NAMES latin1;
CREATE TABLE t1 (
  col1 varchar(255) NOT NULL default ''
) ENGINE=MyISAM DEFAULT CHARSET=latin1 collate latin1_german2_ci;
INSERT INTO t1 VALUES ('�'),('ss'),('ss');
ALTER TABLE t1 ADD KEY ifword(col1);
SELECT * FROM t1 WHERE col1='�' ORDER BY col1, BINARY col1;
DROP TABLE t1;

-- End of 4.1 tests

--
-- Bug#9509
--
create table t1 (s1 char(5) character set latin1 collate latin1_german2_ci);
insert into t1 values (0xf6) /* this is o-umlaut */;
select * from t1 where length(s1)=1 and s1='oe';
drop table t1;

--echo End of 5.1 tests


--echo #
--echo # Start of 5.6 tests
--echo #

--echo #
--echo # WL#3664 WEIGHT_STRING
--echo #

-- set @@collation_connection=latin1_german2_ci;
--source include/weight_string.inc
--source include/weight_string_euro.inc
--source include/weight_string_l1.inc
select hex(weight_string('�'));
select hex(weight_string('�'));
select hex(weight_string('�'));
select hex(weight_string('�'));
select hex(weight_string('�'));
select hex(weight_string('�'));
select hex(weight_string('S'));
select hex(weight_string('s'));
select hex(weight_string('�'));
select hex(weight_string('�' as char(1)));
select hex(weight_string('�' as char(1)));
select hex(weight_string('�' as char(1)));
select hex(weight_string('�' as char(1)));
select hex(weight_string('x�' as char(2)));
select hex(weight_string('x�' as char(2)));
select hex(weight_string('x�' as char(2)));
select hex(weight_string('x�' as char(2)));

--echo #
--echo # End of 5.6 tests
--echo #
