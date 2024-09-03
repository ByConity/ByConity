--disable_warnings
drop table if exists t0,t1,t2,t3,t4,t5;
--enable_warnings

CREATE TABLE t1 (
  grp int(11) default NULL,
  a bigint(20) unsigned default NULL,
  c char(10) NOT NULL default ''
) ENGINE=MyISAM;
INSERT INTO t1 VALUES (1,1,'a'),(2,2,'b'),(2,3,'c'),(3,4,'E'),(3,5,'C'),(3,6,'D'),(NULL,NULL,'');
create table t2 (id int, a bigint unsigned not null, c char(10), d int, primary key (a));
insert into t2 values (1,1,'a',1),(3,4,'A',4),(3,5,'B',5),(3,6,'C',6),(4,7,'D',7);

select t1.*,t2.* from t1 JOIN t2 ON t1.a=t2.a;
select t1.*,t2.* from t1 left join t2 on (t1.a=t2.a) order by t1.grp,t1.a,t2.c;
select t1.*,t2.* from t2 left outer join t1 on (t1.a=t2.a);
select t1.*,t2.* from t1 as t0,{ oj t2 left outer join t1 on (t1.a=t2.a) } WHERE t0.a=2;
select t1.*,t2.* from t1 left join t2 using (a);
select t1.*,t2.* from t1 left join t2 using (a) where t1.a=t2.a;
select t1.*,t2.* from t1 left join t2 using (a,c);
--sorted_result
select t1.*,t2.* from t1 left join t2 using (c);
select t1.*,t2.* from t1 natural left outer join t2;

select t1.*,t2.* from t1 left join t2 on (t1.a=t2.a) where t2.id=3;
select t1.*,t2.* from t1 left join t2 on (t1.a=t2.a) where t2.id is null;

explain select t1.*,t2.* from t1,t2 where t1.a=t2.a and isnull(t2.a)=1;
explain select t1.*,t2.* from t1 left join t2 on t1.a=t2.a where isnull(t2.a)=1;

--sorted_result
select t1.*,t2.*,t3.a from t1 left join t2 on (t1.a=t2.a) left join t1 as t3 on (t2.a=t3.a);

-- # The next query should rearange the left joins to get this to work
--error 1054
explain select t1.*,t2.*,t3.a from t1 left join t2 on (t3.a=t2.a) left join t1 as t3 on (t1.a=t3.a);
--error 1054
select t1.*,t2.*,t3.a from t1 left join t2 on (t3.a=t2.a) left join t1 as t3 on (t1.a=t3.a);

-- # The next query should give an error in MySQL
--error 1054
select t1.*,t2.*,t3.a from t1 left join t2 on (t3.a=t2.a) left join t1 as t3 on (t2.a=t3.a);

-- # Test of inner join
select t1.*,t2.* from t1 inner join t2 using (a);
select t1.*,t2.* from t1 inner join t2 on (t1.a=t2.a);
select t1.*,t2.* from t1 natural join t2;

drop table t1,t2;

-- #
-- # Test of left join bug
-- #

CREATE TABLE t1 (
 usr_id INT unsigned NOT NULL,
 uniq_id INT unsigned NOT NULL AUTO_INCREMENT,
        start_num INT unsigned NOT NULL DEFAULT 1,
        increment INT unsigned NOT NULL DEFAULT 1,
 PRIMARY KEY (uniq_id),
 INDEX usr_uniq_idx (usr_id, uniq_id),
 INDEX uniq_usr_idx (uniq_id, usr_id)
);
CREATE TABLE t2 (
 id INT unsigned NOT NULL DEFAULT 0,
 usr2_id INT unsigned NOT NULL DEFAULT 0,
 max INT unsigned NOT NULL DEFAULT 0,
 c_amount INT unsigned NOT NULL DEFAULT 0,
 d_max INT unsigned NOT NULL DEFAULT 0,
 d_num INT unsigned NOT NULL DEFAULT 0,
 orig_time INT unsigned NOT NULL DEFAULT 0,
 c_time INT unsigned NOT NULL DEFAULT 0,
 active ENUM ('no','yes') NOT NULL,
 PRIMARY KEY (id,usr2_id),
 INDEX id_idx (id),
 INDEX usr2_idx (usr2_id)
);
INSERT INTO t1 VALUES (3,NULL,0,50),(3,NULL,0,200),(3,NULL,0,25),(3,NULL,0,84676),(3,NULL,0,235),(3,NULL,0,10),(3,NULL,0,3098),(3,NULL,0,2947),(3,NULL,0,8987),(3,NULL,0,8347654),(3,NULL,0,20398),(3,NULL,0,8976),(3,NULL,0,500),(3,NULL,0,198);

-- #1st select shows that one record is returned with null entries for the right
-- #table, when selecting on an id that does not exist in the right table t2
SELECT t1.usr_id,t1.uniq_id,t1.increment,
t2.usr2_id,t2.c_amount,t2.max
FROM t1
LEFT JOIN t2 ON t2.id = t1.uniq_id
WHERE t1.uniq_id = 4
ORDER BY t2.c_amount;

-- # The same with RIGHT JOIN
SELECT t1.usr_id,t1.uniq_id,t1.increment,
t2.usr2_id,t2.c_amount,t2.max
FROM t2
RIGHT JOIN t1 ON t2.id = t1.uniq_id
WHERE t1.uniq_id = 4
ORDER BY t2.c_amount;

INSERT INTO t2 VALUES (2,3,3000,6000,0,0,746584,837484,'yes');
--error ER_DUP_ENTRY
INSERT INTO t2 VALUES (2,3,3000,6000,0,0,746584,837484,'yes');
INSERT INTO t2 VALUES (7,3,1000,2000,0,0,746294,937484,'yes');

-- #3rd select should show that one record is returned with null entries for the
-- # right table, when selecting on an id that does not exist in the right table
-- # t2 but this select returns an empty set!!!!
SELECT t1.usr_id,t1.uniq_id,t1.increment,t2.usr2_id,t2.c_amount,t2.max FROM t1 LEFT JOIN t2 ON t2.id = t1.uniq_id WHERE t1.uniq_id = 4 ORDER BY t2.c_amount;
--source include/turn_off_only_full_group_by.inc
SELECT t1.usr_id,t1.uniq_id,t1.increment,t2.usr2_id,t2.c_amount,t2.max FROM t1 LEFT JOIN t2 ON t2.id = t1.uniq_id WHERE t1.uniq_id = 4 GROUP BY t2.c_amount;
--source include/restore_sql_mode_after_turn_off_only_full_group_by.inc
-- # Removing the ORDER BY works:
SELECT t1.usr_id,t1.uniq_id,t1.increment,t2.usr2_id,t2.c_amount,t2.max FROM t1 LEFT JOIN t2 ON t2.id = t1.uniq_id WHERE t1.uniq_id = 4;

drop table t1,t2;

-- #
-- # Test of LEFT JOIN with const tables (failed for frankie@etsetb.upc.es)
-- #

CREATE TABLE t1 (
  cod_asig int(11) DEFAULT '0' NOT NULL,
  desc_larga_cat varchar(80) DEFAULT '' NOT NULL,
  desc_larga_cas varchar(80) DEFAULT '' NOT NULL,
  desc_corta_cat varchar(40) DEFAULT '' NOT NULL,
  desc_corta_cas varchar(40) DEFAULT '' NOT NULL,
  cred_total double(3,1) DEFAULT '0.0' NOT NULL,
  pre_requisit int(11),
  co_requisit int(11),
  preco_requisit int(11),
  PRIMARY KEY (cod_asig)
);

INSERT INTO t1 VALUES (10360,'asdfggfg','Introduccion a los  Ordenadores I','asdfggfg','Introduccio Ordinadors I',6.0,NULL,NULL,NULL);
INSERT INTO t1 VALUES (10361,'Components i Circuits Electronics I','Componentes y Circuitos Electronicos I','Components i Circuits Electronics I','Comp. i Circ. Electr. I',6.0,NULL,NULL,NULL);
INSERT INTO t1 VALUES (10362,'Laboratori d`Ordinadors','Laboratorio de Ordenadores','Laboratori d`Ordinadors','Laboratori Ordinadors',4.5,NULL,NULL,NULL);
INSERT INTO t1 VALUES (10363,'Tecniques de Comunicacio Oral i Escrita','Tecnicas de Comunicacion Oral y Escrita','Tecniques de Comunicacio Oral i Escrita','Tec. Com. Oral i Escrita',4.5,NULL,NULL,NULL);
INSERT INTO t1 VALUES (11403,'Projecte Fi de Carrera','Proyecto Fin de Carrera','Projecte Fi de Carrera','PFC',9.0,NULL,NULL,NULL);
INSERT INTO t1 VALUES (11404,'+lgebra lineal','Algebra lineal','+lgebra lineal','+lgebra lineal',15.0,NULL,NULL,NULL);
INSERT INTO t1 VALUES (11405,'+lgebra lineal','Algebra lineal','+lgebra lineal','+lgebra lineal',18.0,NULL,NULL,NULL);
INSERT INTO t1 VALUES (11406,'Calcul Infinitesimal','Cßlculo Infinitesimal','Calcul Infinitesimal','Calcul Infinitesimal',15.0,NULL,NULL,NULL);

CREATE TABLE t2 (
  idAssignatura int(11) DEFAULT '0' NOT NULL,
  Grup int(11) DEFAULT '0' NOT NULL,
  Places smallint(6) DEFAULT '0' NOT NULL,
  PlacesOcupades int(11) DEFAULT '0',
  PRIMARY KEY (idAssignatura,Grup)
);


INSERT INTO t2 VALUES (10360,12,333,0);
INSERT INTO t2 VALUES (10361,30,2,0);
INSERT INTO t2 VALUES (10361,40,3,0);
INSERT INTO t2 VALUES (10360,45,10,0);
INSERT INTO t2 VALUES (10362,10,12,0);
INSERT INTO t2 VALUES (10360,55,2,0);
INSERT INTO t2 VALUES (10360,70,0,0);
INSERT INTO t2 VALUES (10360,565656,0,0);
INSERT INTO t2 VALUES (10360,32767,7,0);
INSERT INTO t2 VALUES (10360,33,8,0);
INSERT INTO t2 VALUES (10360,7887,85,0);
INSERT INTO t2 VALUES (11405,88,8,0);
INSERT INTO t2 VALUES (10360,0,55,0);
INSERT INTO t2 VALUES (10360,99,0,0);
INSERT INTO t2 VALUES (11411,30,10,0);
INSERT INTO t2 VALUES (11404,0,0,0);
INSERT INTO t2 VALUES (10362,11,111,0);
INSERT INTO t2 VALUES (10363,33,333,0);
INSERT INTO t2 VALUES (11412,55,0,0);
INSERT INTO t2 VALUES (50003,66,6,0);
INSERT INTO t2 VALUES (11403,5,0,0);
INSERT INTO t2 VALUES (11406,11,11,0);
INSERT INTO t2 VALUES (11410,11410,131,0);
INSERT INTO t2 VALUES (11416,11416,32767,0);
INSERT INTO t2 VALUES (11409,0,0,0);

CREATE TABLE t3 (
  id int(11) NOT NULL auto_increment,
  dni_pasaporte char(16) DEFAULT '' NOT NULL,
  idPla int(11) DEFAULT '0' NOT NULL,
  cod_asig int(11) DEFAULT '0' NOT NULL,
  any smallint(6) DEFAULT '0' NOT NULL,
  quatrimestre smallint(6) DEFAULT '0' NOT NULL,
  estat char(1) DEFAULT 'M' NOT NULL,
  PRIMARY KEY (id),
  UNIQUE dni_pasaporte (dni_pasaporte,idPla),
  UNIQUE dni_pasaporte_2 (dni_pasaporte,idPla,cod_asig,any,quatrimestre)
);

INSERT INTO t3 VALUES (1,'11111111',1,10362,98,1,'M');

CREATE TABLE t4 (
  id int(11) NOT NULL auto_increment,
  papa int(11) DEFAULT '0' NOT NULL,
  fill int(11) DEFAULT '0' NOT NULL,
  idPla int(11) DEFAULT '0' NOT NULL,
  PRIMARY KEY (id),
  KEY papa (idPla,papa),
  UNIQUE papa_2 (idPla,papa,fill)
);

INSERT INTO t4 VALUES (1,-1,10360,1);
INSERT INTO t4 VALUES (2,-1,10361,1);
INSERT INTO t4 VALUES (3,-1,10362,1);

SELECT DISTINCT fill,desc_larga_cat,cred_total,Grup,Places,PlacesOcupades FROM t4 LEFT JOIN t3 ON t3.cod_asig=fill AND estat='S'   AND dni_pasaporte='11111111'   AND t3.idPla=1 , t2,t1 WHERE fill=t1.cod_asig   AND Places>PlacesOcupades   AND fill=idAssignatura   AND t4.idPla=1   AND papa=-1;

SELECT DISTINCT fill,t3.idPla FROM t4 LEFT JOIN t3 ON t3.cod_asig=t4.fill AND t3.estat='S' AND t3.dni_pasaporte='1234' AND t3.idPla=1 ;

INSERT INTO t3 VALUES (3,'1234',1,10360,98,1,'S');
SELECT DISTINCT fill,t3.idPla FROM t4 LEFT JOIN t3 ON t3.cod_asig=t4.fill AND t3.estat='S' AND t3.dni_pasaporte='1234' AND t3.idPla=1 ;

drop table t1,t2,t3,test.t4;

-- #
-- # Test of IS NULL on AUTO_INCREMENT with LEFT JOIN
-- #

CREATE TABLE t1 (
  id smallint(5) unsigned NOT NULL auto_increment,
  name char(60) DEFAULT '' NOT NULL,
  PRIMARY KEY (id)
);
INSERT INTO t1 VALUES (1,'Antonio Paz');
INSERT INTO t1 VALUES (2,'Lilliana Angelovska');
INSERT INTO t1 VALUES (3,'Thimble Smith');

CREATE TABLE t2 (
  id smallint(5) unsigned NOT NULL auto_increment,
  owner smallint(5) unsigned DEFAULT '0' NOT NULL,
  name char(60),
  PRIMARY KEY (id)
);
INSERT INTO t2 VALUES (1,1,'El Gato');
INSERT INTO t2 VALUES (2,1,'Perrito');
INSERT INTO t2 VALUES (3,3,'Happy');

--sorted_result
select t1.name, t2.name, t2.id from t1 left join t2 on (t1.id = t2.owner);
select t1.name, t2.name, t2.id from t1 left join t2 on (t1.id = t2.owner) where t2.id is null;
explain select t1.name, t2.name, t2.id from t1 left join t2 on (t1.id = t2.owner) where t2.id is null;
explain select t1.name, t2.name, t2.id from t1 left join t2 on (t1.id = t2.owner) where t2.name is null;
select count(*) from t1 left join t2 on (t1.id = t2.owner);

--sorted_result
select t1.name, t2.name, t2.id from t2 right join t1 on (t1.id = t2.owner);
select t1.name, t2.name, t2.id from t2 right join t1 on (t1.id = t2.owner) where t2.id is null;
explain select t1.name, t2.name, t2.id from t2 right join t1 on (t1.id = t2.owner) where t2.id is null;
explain select t1.name, t2.name, t2.id from t2 right join t1 on (t1.id = t2.owner) where t2.name is null;
select count(*) from t2 right join t1 on (t1.id = t2.owner);

--sorted_result
select t1.name, t2.name, t2.id,t3.id from t2 right join t1 on (t1.id = t2.owner) left join t1 as t3 on t3.id=t2.owner;
select t1.name, t2.name, t2.id,t3.id from t1 right join t2 on (t1.id = t2.owner) right join t1 as t3 on t3.id=t2.owner;
select t1.name, t2.name, t2.id, t2.owner, t3.id from t1 left join t2 on (t1.id = t2.owner) right join t1 as t3 on t3.id=t2.owner;

drop table t1,t2;

create table t1 (id int not null, str char(10), index(str));
insert into t1 values (1, null), (2, null), (3, 'foo'), (4, 'bar');
select * from t1 where str is not null order by id;
select * from t1 where str is null;
drop table t1;

-- #
-- # Test wrong LEFT JOIN query
-- #

CREATE TABLE t1 (
  t1_id bigint(21) NOT NULL auto_increment,
  PRIMARY KEY (t1_id)
);
CREATE TABLE t2 (
  t2_id bigint(21) NOT NULL auto_increment,
  PRIMARY KEY (t2_id)
);
CREATE TABLE t3 (
  t3_id bigint(21) NOT NULL auto_increment,
  PRIMARY KEY (t3_id)
);
CREATE TABLE t4 (
  seq_0_id bigint(21) DEFAULT '0' NOT NULL,
  seq_1_id bigint(21) DEFAULT '0' NOT NULL,
  KEY seq_0_id (seq_0_id),
  KEY seq_1_id (seq_1_id)
);
CREATE TABLE t5 (
  seq_0_id bigint(21) DEFAULT '0' NOT NULL,
  seq_1_id bigint(21) DEFAULT '0' NOT NULL,
  KEY seq_1_id (seq_1_id),
  KEY seq_0_id (seq_0_id)
);

insert into t1 values (1);
insert into t2 values (1);
insert into t3 values (1);
insert into t4 values (1,1);
insert into t5 values (1,1);

--error 1054
explain select * from t3 left join t4 on t4.seq_1_id = t2.t2_id left join t1 on t1.t1_id = t4.seq_0_id left join t5 on t5.seq_0_id = t1.t1_id left join t2 on t2.t2_id = t5.seq_1_id where t3.t3_id = 23;

drop table t1,t2,t3,t4,t5;

-- #
-- # Another LEFT JOIN problem
-- # (The problem was that the result changed when we added ORDER BY)
-- #

create table t1 (n int, m int, o int, key(n));
create table t2 (n int not null, m int, o int, primary key(n));
insert into t1 values (1, 2, 11), (1, 2, 7), (2, 2, 8), (1,2,9),(1,3,9);
insert into t2 values (1, 2, 3),(2, 2, 8), (4,3,9),(3,2,10);
select t1.*, t2.* from t1 left join t2 on t1.n = t2.n and
t1.m = t2.m where t1.n = 1;
select t1.*, t2.* from t1 left join t2 on t1.n = t2.n and
t1.m = t2.m where t1.n = 1 order by t1.o,t1.m;
drop table t1,t2;

-- # Test bug with NATURAL join:

CREATE TABLE t1 (id1 INT NOT NULL PRIMARY KEY, dat1 CHAR(1), id2 INT);   
INSERT INTO t1 VALUES (1,'a',1);
INSERT INTO t1 VALUES (2,'b',1);
INSERT INTO t1 VALUES (3,'c',2);

CREATE TABLE t2 (id2 INT NOT NULL PRIMARY KEY, dat2 CHAR(1));   
INSERT INTO t2 VALUES (1,'x');
INSERT INTO t2 VALUES (2,'y');
INSERT INTO t2 VALUES (3,'z');

SELECT t2.id2 FROM t2 LEFT OUTER JOIN t1 ON t1.id2 = t2.id2 WHERE id1 IS NULL;
SELECT t2.id2 FROM t2 NATURAL LEFT OUTER JOIN t1 WHERE id1 IS NULL;

drop table t1,t2;

create table t1 ( color varchar(20), name varchar(20) );
insert into t1 values ( 'red', 'apple' );
insert into t1 values ( 'yellow', 'banana' );
insert into t1 values ( 'green', 'lime' );
insert into t1 values ( 'black', 'grape' );
insert into t1 values ( 'blue', 'blueberry' );
create table t2 ( count int, color varchar(20) );
insert into t2 values (10, 'green');
insert into t2 values (5, 'black');
insert into t2 values (15, 'white');
insert into t2 values (7, 'green');
select * from t1;
select * from t2;
select * from t2 natural join t1;
select t2.count, t1.name from t2 natural join t1;
select t2.count, t1.name from t2 inner join t1 using (color);
drop table t1;
drop table t2;

-- #
-- # Test of LEFT JOIN + GROUP FUNCTIONS within functions:
-- #

CREATE TABLE t1 (
  pcode varchar(8) DEFAULT '' NOT NULL
);
INSERT INTO t1 VALUES ('kvw2000'),('kvw2001'),('kvw3000'),('kvw3001'),('kvw3002'),('kvw3500'),('kvw3501'),('kvw3502'),('kvw3800'),('kvw3801'),('kvw3802'),('kvw3900'),('kvw3901'),('kvw3902'),('kvw4000'),('kvw4001'),('kvw4002'),('kvw4200'),('kvw4500'),('kvw5000'),('kvw5001'),('kvw5500'),('kvw5510'),('kvw5600'),('kvw5601'),('kvw6000'),('klw1000'),('klw1020'),('klw1500'),('klw2000'),('klw2001'),('klw2002'),('kld2000'),('klw2500'),('kmw1000'),('kmw1500'),('kmw2000'),('kmw2001'),('kmw2100'),('kmw3000'),('kmw3200');
CREATE TABLE t2 (
  pcode varchar(8) DEFAULT '' NOT NULL,
  KEY pcode (pcode)
);
INSERT INTO t2 VALUES ('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw2000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3000'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw3500'),('kvw6000'),('kvw6000'),('kld2000');
--source include/turn_off_only_full_group_by.inc

SELECT t1.pcode, IF(ISNULL(t2.pcode), 0, COUNT(*)) AS count FROM t1
LEFT JOIN t2 ON t1.pcode = t2.pcode GROUP BY t1.pcode;
SELECT SQL_BIG_RESULT t1.pcode, IF(ISNULL(t2.pcode), 0, COUNT(*)) AS count FROM t1 LEFT JOIN t2 ON t1.pcode = t2.pcode GROUP BY t1.pcode;

--source include/restore_sql_mode_after_turn_off_only_full_group_by.inc
drop table t1,t2;

-- #
-- # Another left join problem
-- #
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
CREATE TABLE t1 (
  id int(11),
  pid int(11),
  rep_del tinyint(4),
  KEY id (id),
  KEY pid (pid)
);
INSERT INTO t1 VALUES (1,NULL,NULL);
INSERT INTO t1 VALUES (2,1,NULL);
select * from t1 LEFT JOIN t1 t2 ON (t1.id=t2.pid) AND t2.rep_del IS NULL;
create index rep_del ON t1(rep_del);
select * from t1 LEFT JOIN t1 t2 ON (t1.id=t2.pid) AND t2.rep_del IS NULL;
drop table t1;

CREATE TABLE t1 (
  id int(11) DEFAULT '0' NOT NULL,
  name tinytext DEFAULT '' NOT NULL,
  UNIQUE id (id)
);
INSERT INTO t1 VALUES (1,'yes'),(2,'no');
CREATE TABLE t2 (
  id int(11) DEFAULT '0' NOT NULL,
  idx int(11) DEFAULT '0' NOT NULL,
  UNIQUE id (id,idx)
);
INSERT INTO t2 VALUES (1,1);
explain SELECT * from t1 left join t2 on t1.id=t2.id where t2.id IS NULL;
SELECT * from t1 left join t2 on t1.id=t2.id where t2.id IS NULL;
drop table t1,t2;
SET sql_mode = default;
-- #
-- # Test problem with using key_column= constant in ON and WHERE
-- #
create table t1 (bug_id mediumint, reporter mediumint);
create table t2 (bug_id mediumint, who mediumint, index(who));
insert into t2 values (1,1),(1,2);
insert into t1 values (1,1),(2,1);
SELECT * FROM t1 LEFT JOIN t2 ON (t1.bug_id =  t2.bug_id AND  t2.who = 2) WHERE  (t1.reporter = 2 OR t2.who = 2);
drop table t1,t2;

-- #
-- # Test problem with LEFT JOIN

create table t1 (fooID smallint unsigned auto_increment, primary key (fooID));
create table t2 (fooID smallint unsigned not null, barID smallint unsigned not null, primary key (fooID,barID));
insert into t1 (fooID) values (10),(20),(30);
insert into t2 values (10,1),(20,2),(30,3);
explain select * from t2 left join t1 on t1.fooID = t2.fooID and t1.fooID = 30;
select * from t2 left join t1 on t1.fooID = t2.fooID and t1.fooID = 30;
--sorted_result
select * from t2 left join t1 ignore index(primary) on t1.fooID = t2.fooID and t1.fooID = 30;
drop table t1,t2;

create table t1 (i int);
create table t2 (i int);
create table t3 (i int);
insert into t1 values(1),(2);
insert into t2 values(2),(3);
insert into t3 values(2),(4);
--sorted_result
select * from t1 natural left join t2 natural left join t3;
select * from t1 natural left join t2 where (t2.i is not null)=0;
--sorted_result
select * from t1 natural left join t2 where (t2.i is not null) is not null;
select * from t1 natural left join t2 where (i is not null)=0;
--sorted_result
select * from t1 natural left join t2 where (i is not null) is not null;
drop table t1,t2,t3;

-- #
-- # Test of USING
-- #
create table t1 (f1 integer,f2 integer,f3 integer);
create table t2 (f2 integer,f4 integer);
create table t3 (f3 integer,f5 integer);
select * from t1
         left outer join t2 using (f2)
         left outer join t3 using (f3);
drop table t1,t2,t3;

create table t1 (a1 int, a2 int);
create table t2 (b1 int not null, b2 int);
create table t3 (c1 int, c2 int);

insert into t1 values (1,2), (2,2), (3,2);
insert into t2 values (1,3), (2,3);
insert into t3 values (2,4),        (3,4);

select * from t1 left join t2  on  b1 = a1 left join t3  on  c1 = a1  and  b1 is null;
explain select * from t1 left join t2  on  b1 = a1 left join t3  on  c1 = a1  and  b1 is null;

drop table t1, t2, t3;
