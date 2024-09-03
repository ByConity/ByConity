--disable_warnings
drop table if exists t1,t2,t3;
--enable_warnings
CREATE TABLE t1 (
  id int(6) DEFAULT '0' NOT NULL,
  idservice int(5),
  clee char(20) NOT NULL,
  flag char(1),
  KEY id (id),
  PRIMARY KEY (clee)
);

INSERT INTO t1 VALUES (2,4,'6067169d','Y');
INSERT INTO t1 VALUES (2,5,'606716d1','Y');
INSERT INTO t1 VALUES (2,1,'606717c1','Y');
INSERT INTO t1 VALUES (3,1,'6067178d','Y');
INSERT INTO t1 VALUES (2,6,'60671515','Y');
INSERT INTO t1 VALUES (2,7,'60671569','Y');
INSERT INTO t1 VALUES (2,3,'dd','Y');

CREATE TABLE t2 (
  id int(6) NOT NULL auto_increment,
  description varchar(40) NOT NULL,
  idform varchar(40),
  ordre int(6) unsigned DEFAULT '0' NOT NULL,
  image varchar(60),
  PRIMARY KEY (id),
  KEY id (id,ordre)
);

-- #
-- # Dumping data for table 't2'
-- #

INSERT INTO t2 VALUES (1,'Emettre un appel d''offres','en_construction.html',10,'emettre.gif');
INSERT INTO t2 VALUES (2,'Emettre des soumissions','en_construction.html',20,'emettre.gif');
INSERT INTO t2 VALUES (7,'Liste des t2','t2_liste_form.phtml',51060,'link.gif');
INSERT INTO t2 VALUES (8,'Consulter les soumissions','consulter_soumissions.phtml',200,'link.gif');
INSERT INTO t2 VALUES (9,'Ajouter un type de materiel','typeMateriel_ajoute_form.phtml',51000,'link.gif');
INSERT INTO t2 VALUES (10,'Lister/modifier un type de materiel','typeMateriel_liste_form.phtml',51010,'link.gif');
INSERT INTO t2 VALUES (3,'Créer une fiche de client','clients_ajoute_form.phtml',40000,'link.gif');
INSERT INTO t2 VALUES (4,'Modifier des clients','en_construction.html',40010,'link.gif');
INSERT INTO t2 VALUES (5,'Effacer des clients','en_construction.html',40020,'link.gif');
INSERT INTO t2 VALUES (6,'Ajouter un service','t2_ajoute_form.phtml',51050,'link.gif');


select t1.id,t1.idservice,t2.ordre,t2.description  from t1, t2 where t1.id = 2   and t1.idservice = t2.id  order by t2.ordre;
 
drop table t1,t2;

-- #
-- # Test of ORDER BY on concat() result
-- #

create table t1 (first char(10),last char(10));
insert into t1 values ('Michael','Widenius');
insert into t1 values ('Allan','Larsson');
insert into t1 values ('David','Axmark');
select concat(first,' ',last) as name from t1 order by name;
select concat(last,' ',first) as name from t1 order by name;
drop table t1;

-- #
-- # bug in distinct + order by
-- #

create table t1 (i int);
insert into t1 values(1),(2),(1),(2),(1),(2),(3);
select distinct i from t1;
select distinct i from t1 order by rand(5);
select distinct i from t1 order by i desc;
select distinct i from t1 order by 1-i;
select distinct i from t1 order by mod(i,2),i;
drop table t1;

-- #
-- # bug#3681
-- #

create table t1 ( pk     int primary key, name   varchar(255) not null, number varchar(255) not null);
insert into t1 values (1, 'Gamma',     '123'), (2, 'Gamma Ext', '123a'), (3, 'Alpha',     '001'), (4, 'Beta',      '200c');
select distinct t1.name as "Building Name",t1.number as "Building Number" from t1 order by t1.name asc;
drop table t1;


-- #
-- # Order by on first index part
-- #

create table t1 (id int not null,col1 int not null,col2 int not null,index(col1));
insert into t1 values(1,2,2),(2,2,1),(3,1,2),(4,1,1),(5,1,4),(6,2,3),(7,3,1),(8,2,4);
select * from t1 order by col1,col2;
select col1 from t1 order by id;
select col1 as id from t1 order by id;
select concat(col1) as id from t1 order by id;
drop table t1;

-- #
-- # Test of order by on field()
-- #

CREATE TABLE t1 (id int auto_increment primary key,aika varchar(40),aikakentta  timestamp);
insert into t1 (aika) values ('Keskiviikko');
insert into t1 (aika) values ('Tiistai');
insert into t1 (aika) values ('Maanantai');
insert into t1 (aika) values ('Sunnuntai');

SELECT FIELD(SUBSTRING(t1.aika,1,2),'Ma','Ti','Ke','To','Pe','La','Su') AS test FROM t1 ORDER by test;
drop table t1;

-- #
-- # Test of ORDER BY on IF
-- #

CREATE TABLE t1
(
  a          int unsigned       NOT NULL,
  b          int unsigned       NOT NULL,
  c          int unsigned       NOT NULL,
  UNIQUE KEY(a),
  INDEX(b),
  INDEX(c)
);

CREATE TABLE t2
(
  c          int unsigned       NOT NULL,
  i          int unsigned       NOT NULL,
  INDEX(c)
);

CREATE TABLE t3
(
  c          int unsigned       NOT NULL,
  v          varchar(64),
  INDEX(c)
);

INSERT INTO t1 VALUES (1,1,1);
INSERT INTO t1 VALUES (2,1,2);
INSERT INTO t1 VALUES (3,2,1);
INSERT INTO t1 VALUES (4,2,2);
INSERT INTO t2 VALUES (1,50);
INSERT INTO t2 VALUES (2,25);
INSERT INTO t3 VALUES (1,'123 Park Place');
INSERT INTO t3 VALUES (2,'453 Boardwalk');

SELECT    a,b,if(b = 1,i,if(b = 2,v,''))
FROM      t1
LEFT JOIN t2 USING(c)
LEFT JOIN t3 ON t3.c = t1.c;

SELECT    a,b,if(b = 1,i,if(b = 2,v,''))
FROM      t1
LEFT JOIN t2 ON t1.c = t2.c
LEFT JOIN t3 ON t3.c = t1.c;

SELECT    a,b,if(b = 1,i,if(b = 2,v,''))
FROM      t1
LEFT JOIN t2 USING(c)
LEFT JOIN t3 ON t3.c = t1.c
ORDER BY a;

SELECT    a,b,if(b = 1,i,if(b = 2,v,''))
FROM      t1
LEFT JOIN t2 ON t1.c = t2.c
LEFT JOIN t3 ON t3.c = t1.c
ORDER BY a;

drop table t1,t2,t3;

-- #
-- # Test of ORDER BY (Bug found by Dean Edmonds)
-- #

create table t1 (ID int not null primary key, TransactionID int not null);
insert into t1 (ID, TransactionID) values  (1,  87), (2,  89), (3,  92), (4,  94), (5,  486), (6,  490), (7,  753), (9,  828), (10, 832), (11, 834), (12, 840);
create table t2 (ID int not null primary key, GroupID int not null);
 insert into t2 (ID, GroupID) values (87,  87), (89,  89), (92,  92), (94,  94), (486, 486), (490, 490),(753, 753), (828, 828), (832, 832), (834, 834), (840, 840);
create table t3 (ID int not null primary key, DateOfAction date not null);
insert into t3 (ID, DateOfAction) values  (87,  '1999-07-19'), (89,  '1999-07-19'), (92,  '1999-07-19'), (94,  '1999-07-19'), (486, '1999-07-18'), (490, '2000-03-27'), (753, '2000-03-28'), (828, '1999-07-27'), (832, '1999-07-27'),(834, '1999-07-27'), (840, '1999-07-27');
select t3.DateOfAction, t1.TransactionID from t1 join t2 join t3 where t2.ID = t1.TransactionID and t3.ID = t2.GroupID order by t3.DateOfAction, t1.TransactionID; 
select t3.DateOfAction, t1.TransactionID from t1 join t2 join t3 where t2.ID = t1.TransactionID and t3.ID = t2.GroupID order by t1.TransactionID,t3.DateOfAction; 
drop table t1,t2,t3;

-- #bug reported by Wouter de Jong

CREATE TABLE t1 (
  member_id int(11) NOT NULL auto_increment,
  inschrijf_datum varchar(20) NOT NULL default '',
  lastchange_datum varchar(20) NOT NULL default '',
  nickname varchar(20) NOT NULL default '',
  password varchar(8) NOT NULL default '',
  voornaam varchar(30) NOT NULL default '',
  tussenvoegsels varchar(10) NOT NULL default '',
  achternaam varchar(50) NOT NULL default '',
  straat varchar(100) NOT NULL default '',
  postcode varchar(10) NOT NULL default '',
  wijk varchar(40) NOT NULL default '',
  plaats varchar(50) NOT NULL default '',
  telefoon varchar(10) NOT NULL default '',
  geboortedatum date NOT NULL default '0000-00-00',
  geslacht varchar(5) NOT NULL default '',
  email varchar(80) NOT NULL default '',
  uin varchar(15) NOT NULL default '',
  homepage varchar(100) NOT NULL default '',
  internet varchar(15) NOT NULL default '',
  scherk varchar(30) NOT NULL default '',
  favo_boek varchar(50) NOT NULL default '',
  favo_tijdschrift varchar(50) NOT NULL default '',
  favo_tv varchar(50) NOT NULL default '',
  favo_eten varchar(50) NOT NULL default '',
  favo_muziek varchar(30) NOT NULL default '',
  info text NOT NULL default '',
  ipnr varchar(30) NOT NULL default '',
  PRIMARY KEY  (member_id)
) ENGINE=MyISAM PACK_KEYS=1;

insert into t1 (member_id) values (1),(2),(3);
select member_id, nickname, voornaam FROM t1
ORDER by lastchange_datum DESC LIMIT 2;
drop table t1;

-- #
-- # Test optimization of ORDER BY DESC
-- #

create table t1 (a int not null, b int, c varchar(10), key (a, b, c));
insert into t1 values (1, NULL, NULL), (1, NULL, 'b'), (1, 1, NULL), (1, 1, 'b'), (1, 1, 'b'), (2, 1, 'a'), (2, 1, 'b'), (2, 2, 'a'), (2, 2, 'b'), (2, 3, 'c'),(1,3,'b');

explain select * from t1 where (a = 1 and b is null and c = 'b') or (a > 2) order by a desc;
select * from t1 where (a = 1 and b is null and c = 'b') or (a > 2) order by a desc;
explain select * from t1 where a >= 1 and a < 3 order by a desc;
select * from t1 where a >= 1 and a < 3 order by a desc;
explain select * from t1 where a = 1 order by a desc, b desc;
select * from t1 where a = 1 order by a desc, b desc;
explain select * from t1 where a = 1 and b is null order by a desc, b desc;
select * from t1 where a = 1 and b is null order by a desc, b desc;
explain select * from t1 where a >= 1 and a < 3 and b >0 order by a desc,b desc;
explain select * from t1 where a = 2 and b >0 order by a desc,b desc;
explain select * from t1 where a = 2 and b is null order by a desc,b desc;
explain select * from t1 where a = 2 and (b is null or b > 0) order by a
desc,b desc;
explain select * from t1 where a = 2 and b > 0 order by a desc,b desc;
explain select * from t1 where a = 2 and b < 2 order by a desc,b desc;
explain select * from t1 where a = 1 order by b desc;
select * from t1 where a = 1 order by b desc;
-- #
-- # Test things when we don't have NULL keys
-- #

alter table t1 modify b int not null, modify c varchar(10) not null;
explain select * from t1 order by a, b, c;
select * from t1 order by a, b, c;
explain select * from t1 order by a desc, b desc, c desc;
select * from t1 order by a desc, b desc, c desc;
# test multiple ranges, NO_MAX_RANGE and EQ_RANGE
explain select * from t1 where (a = 1 and b = 1 and c = 'b') or (a > 2) order by a desc;
select * from t1 where (a = 1 and b = 1 and c = 'b') or (a > 2) order by a desc;
# test NEAR_MAX, NO_MIN_RANGE
explain select * from t1 where a < 2 and b <= 1 order by a desc, b desc;
select * from t1 where a < 2 and b <= 1 order by a desc, b desc;
select count(*) from t1 where a < 5 and b > 0;
select * from t1 where a < 5 and b > 0 order by a desc,b desc;
# test HA_READ_AFTER_KEY (at the end of the file), NEAR_MIN
explain select * from t1 where a between 1 and 3 and b <= 1 order by a desc, b desc;
select * from t1 where a between 1 and 3 and b <= 1 order by a desc, b desc;
# test HA_READ_AFTER_KEY (in the middle of the file)
explain select * from t1 where a between 0 and 1 order by a desc, b desc;
select * from t1 where a between 0 and 1 order by a desc, b desc;
drop table t1;


CREATE TABLE t1 (
  gid int(10) unsigned NOT NULL auto_increment,
  cid smallint(5) unsigned NOT NULL default '0',
  PRIMARY KEY  (gid),
  KEY component_id (cid)
) ENGINE=MyISAM;
INSERT INTO t1 VALUES (103853,108),(103867,108),(103962,108),(104505,108),(104619,108),(104620,108);
ALTER TABLE t1 add skr int(10) not null;

CREATE TABLE t2 (
  gid int(10) unsigned NOT NULL default '0',
  uid smallint(5) unsigned NOT NULL default '1',
  sid tinyint(3) unsigned NOT NULL default '1',
  PRIMARY KEY  (gid),
  KEY uid (uid),
  KEY status_id (sid)
) ENGINE=MyISAM;
INSERT INTO t2 VALUES (103853,250,5),(103867,27,5),(103962,27,5),(104505,117,5),(104619,75,5),(104620,15,5);

CREATE TABLE t3 (
  uid smallint(6) NOT NULL auto_increment,
  PRIMARY KEY  (uid)
) ENGINE=MyISAM;
INSERT INTO t3 VALUES (1),(15),(27),(75),(117),(250);
ALTER TABLE t3 add skr int(10) not null;

select t1.gid, t2.sid, t3.uid from t2, t1, t3 where t2.gid = t1.gid and t2.uid = t3.uid order by t3.uid, t1.gid;
select t1.gid, t2.sid, t3.uid from t3, t2, t1 where t2.gid = t1.gid and t2.uid = t3.uid order by t3.uid, t1.gid;

-- # The following ORDER BY can be optimimized
EXPLAIN select t1.gid, t2.sid, t3.uid from t3, t2, t1 where t2.gid = t1.gid and t2.uid = t3.uid order by t1.gid, t3.uid;
EXPLAIN SELECT t1.gid, t3.uid from t1, t3 where t1.gid = t3.uid order by t1.gid,t3.skr;

-- # The following ORDER BY can't be optimimized
EXPLAIN SELECT t1.gid, t2.sid, t3.uid from t2, t1, t3 where t2.gid = t1.gid and t2.uid = t3.uid order by t3.uid, t1.gid;
EXPLAIN SELECT t1.gid, t3.uid from t1, t3 where t1.gid = t3.uid order by t3.skr,t1.gid;
EXPLAIN SELECT t1.gid, t3.uid from t1, t3 where t1.skr = t3.uid order by t1.gid,t3.skr;
drop table t1,t2,t3;

-- #
-- # Test of bug when doing an ORDER BY with const items
-- #

CREATE TABLE t1 (
  `titre` char(80) NOT NULL default '',
  `numeropost` mediumint(8) unsigned NOT NULL auto_increment,
  `date` datetime NOT NULL default '0000-00-00 00:00:00',
  `auteur` char(35) NOT NULL default '',
  `icone` tinyint(2) unsigned NOT NULL default '0',
  `lastauteur` char(35) NOT NULL default '',
  `nbrep` smallint(6) unsigned NOT NULL default '0',
  `dest` char(35) NOT NULL default '',
  `lu` tinyint(1) unsigned NOT NULL default '0',
  `vue` mediumint(8) unsigned NOT NULL default '0',
  `ludest` tinyint(1) unsigned NOT NULL default '0',
  `ouvert` tinyint(1) unsigned NOT NULL default '1',
  PRIMARY KEY  (`numeropost`),
  KEY `date` (`date`),
  KEY `dest` (`dest`,`ludest`),
  KEY `auteur` (`auteur`,`lu`),
  KEY `auteur_2` (`auteur`,`date`),
  KEY `dest_2` (`dest`,`date`)
) CHECKSUM=1;

CREATE TABLE t2 (
  `numeropost` mediumint(8) unsigned NOT NULL default '0',
  `pseudo` char(35) NOT NULL default '',
  PRIMARY KEY  (`numeropost`,`pseudo`),
  KEY `pseudo` (`pseudo`)
);

INSERT INTO t1 (titre,auteur,dest) VALUES ('test','joce','bug');
INSERT INTO t2 (numeropost,pseudo) VALUES (1,'joce'),(1,'bug');
SELECT titre,t1.numeropost,auteur,icone,nbrep,0,date,vue,ouvert,lastauteur,dest FROM t2 LEFT JOIN t1 USING(numeropost) WHERE t2.pseudo='joce' ORDER BY date DESC LIMIT 0,30;
SELECT titre,numeropost,auteur,icone,nbrep,0,date,vue,ouvert,lastauteur,dest FROM t2 LEFT JOIN t1 USING(numeropost) WHERE t2.pseudo='joce' ORDER BY date DESC LIMIT 0,30;
SELECT titre,t1.numeropost,auteur,icone,nbrep,'0',date,vue,ouvert,lastauteur,dest FROM t2 LEFT JOIN t1 USING(numeropost) WHERE t2.pseudo='joce' ORDER BY date DESC LIMIT 0,30;
SELECT titre,numeropost,auteur,icone,nbrep,'0',date,vue,ouvert,lastauteur,dest FROM t2 LEFT JOIN t1 USING(numeropost) WHERE t2.pseudo='joce' ORDER BY date DESC LIMIT 0,30;
drop table t1,t2;

-- #
-- # Test order by with NULL values
-- #
CREATE TABLE t1 (a int, b int);
INSERT INTO t1 VALUES (1, 2);
INSERT INTO t1 VALUES (3, 4);
INSERT INTO t1 VALUES (5, NULL);
SELECT * FROM t1 ORDER BY b;
SELECT * FROM t1 ORDER BY b DESC;
SELECT * FROM t1 ORDER BY (a + b);
SELECT * FROM t1 ORDER BY (a + b) DESC;
DROP TABLE t1;

-- #
-- # Test of test_if_subkey() function
-- #
CREATE TABLE t1 (
  FieldKey varchar(36) NOT NULL default '',
  LongVal bigint(20) default NULL,
  StringVal mediumtext,
  KEY FieldKey (FieldKey),
  KEY LongField (FieldKey,LongVal),
  KEY StringField (FieldKey,StringVal(32))
);
INSERT INTO t1 VALUES ('0',3,'0'),('0',2,'1'),('0',1,'2'),('1',2,'1'),('1',1,'3'), ('1',0,'2'),('2',3,'0'),('2',2,'1'),('2',1,'2'),('2',3,'0'),('2',2,'1'),('2',1,'2'),('3',2,'1'),('3',1,'2'),('3','3','3');
EXPLAIN SELECT * FROM t1 WHERE FieldKey = '1' ORDER BY LongVal;
SELECT * FROM t1 WHERE FieldKey = '1' ORDER BY LongVal;
EXPLAIN SELECT * FROM t1 ignore index (FieldKey, LongField) WHERE FieldKey > '2' ORDER BY LongVal;
SELECT * FROM t1 WHERE FieldKey > '2' ORDER BY LongVal;
EXPLAIN SELECT * FROM t1 WHERE FieldKey > '2' ORDER BY FieldKey, LongVal;
SELECT * FROM t1 WHERE FieldKey > '2' ORDER BY FieldKey, LongVal;
DROP TABLE t1;
-- #
-- # Bug #1945 - Crashing bug with bad User Variables in UPDATE ... ORDER BY ...
-- #
CREATE TABLE t1 (a INT, b INT);
SET @id=0;
UPDATE t1 SET a=0 ORDER BY (a=@id), b;
DROP TABLE t1;

-- #
-- # Bug when doing an order by on a 1 byte string (Bug #2147)
-- #

CREATE TABLE t1 (  id smallint(6) unsigned NOT NULL default '0',  menu tinyint(4) NOT NULL default '0',  KEY id (id),  KEY menu (menu)) ENGINE=MyISAM;
INSERT INTO t1 VALUES (11384, 2),(11392, 2);
SELECT id FROM t1 WHERE id <11984 AND menu =2 ORDER BY id DESC LIMIT 1 ;
drop table t1;

-- #
-- # REF_OR_NULL optimization + filesort (bug #2419)
-- #

create table t1(a int, b int, index(b));
insert into t1 values (2, 1), (1, 1), (4, NULL), (3, NULL), (6, 2), (5, 2);
explain select * from t1 where b=1 or b is null order by a;
select * from t1 where b=1 or b is null order by a;
explain select * from t1 where b=2 or b is null order by a;
select * from t1 where b=2 or b is null order by a;
drop table t1;

-- #
-- # Bug #3155 - Strange results with index (x, y) ... WHERE ... ORDER BY pk
-- #

create table t1 (a int not null auto_increment, b int not null, c int not null, d int not null,
key(a,b,d), key(c,b,a));
create table t2 like t1;
insert into t1 values (NULL, 1, 2, 0), (NULL, 2, 1, 1), (NULL, 3, 4, 2), (NULL, 4, 3, 3);
insert into t2 select null, b, c, d from t1;
insert into t1 select null, b, c, d from t2;
insert into t2 select null, b, c, d from t1;
insert into t1 select null, b, c, d from t2;
insert into t2 select null, b, c, d from t1;
insert into t1 select null, b, c, d from t2;
insert into t2 select null, b, c, d from t1;
insert into t1 select null, b, c, d from t2;
insert into t2 select null, b, c, d from t1;
insert into t1 select null, b, c, d from t2;
optimize table t1;
set @row=10;
insert into t1 select 1, b, c + (@row:=@row - 1) * 10, d - @row from t2 limit 10;
select * from t1 where a=1 and b in (1) order by c, b, a;
select * from t1 where a=1 and b in (1);
drop table t1, t2;

-- #
-- # Bug #4302
-- # Ambiguos order by when renamed column is identical to another in result.
-- # Should not fail and prefer column from t1 for sorting.
-- #
create table t1 (col1 int, col int);
create table t2 (col2 int, col int);
insert into t1 values (1,1),(2,2),(3,3);
insert into t2 values (1,3),(2,2),(3,1);
select t1.* , t2.col as t2_col from t1 left join t2 on (t1.col1=t2.col2)
  order by col;

-- #
-- # Let us also test various ambiguos and potentially ambiguos cases 
-- # related to aliases
-- #
--error 1052
select col1 as col, col from t1 order by col;
--error 1052
select t1.col as c1, t2.col as c2 from t1, t2 where t1.col1=t2.col2
  order by col;
--error 1052
select t1.col as c1, t2.col as c2 from t1, t2 where t1.col1=t2.col2
  order by col;
--error 1052
select col1 from t1, t2 where t1.col1=t2.col2 order by col;
--error 1052
select t1.col as t1_col, t2.col2 from t1, t2 where t1.col1=t2.col2
  order by col;

select t1.col as t1_col, t2.col from t1, t2 where t1.col1=t2.col2
  order by col;
select col2 as c, col as c from t2 order by col;
select col2 as col, col as col2 from t2 order by col; 
select t2.col2, t2.col, t2.col from t2 order by col;

select t2.col2 as col from t2 order by t2.col;
select t2.col2 as col, t2.col from t2 order by t2.col;
select t2.col2, t2.col, t2.col from t2 order by t2.col;

drop table t1, t2;

-- #
-- # Bug #5428: a problem with small max_sort_length value
-- #

create table t1 (a char(25));
insert into t1 set a = repeat('x', 20);
insert into t1 set a = concat(repeat('x', 19), 'z');
insert into t1 set a = concat(repeat('x', 19), 'ab');
insert into t1 set a = concat(repeat('x', 19), 'aa');
-- set max_sort_length=20;
select a from t1 order by a;
drop table t1;

-- #
-- # Bug #7331
-- #

create table t1 (
  `sid` decimal(8,0) default null,
  `wnid` varchar(11) not null default '',
  key `wnid14` (`wnid`(4)),
  key `wnid` (`wnid`)
) engine=myisam default charset=latin1;

insert into t1 (`sid`, `wnid`) values
('10100','01019000000'),('37986','01019000000'),('37987','01019010000'),
('39560','01019090000'),('37989','01019000000'),('37990','01019011000'),
('37991','01019011000'),('37992','01019019000'),('37993','01019030000'),
('37994','01019090000'),('475','02070000000'),('25253','02071100000'),
('25255','02071100000'),('25256','02071110000'),('25258','02071130000'),
('25259','02071190000'),('25260','02071200000'),('25261','02071210000'),
('25262','02071290000'),('25263','02071300000'),('25264','02071310000'),
('25265','02071310000'),('25266','02071320000'),('25267','02071320000'),
('25269','02071330000'),('25270','02071340000'),('25271','02071350000'),
('25272','02071360000'),('25273','02071370000'),('25281','02071391000'),
('25282','02071391000'),('25283','02071399000'),('25284','02071400000'),
('25285','02071410000'),('25286','02071410000'),('25287','02071420000'),
('25288','02071420000'),('25291','02071430000'),('25290','02071440000'),
('25292','02071450000'),('25293','02071460000'),('25294','02071470000'),
('25295','02071491000'),('25296','02071491000'),('25297','02071499000');

explain select * from t1 where wnid like '0101%' order by wnid;

select * from t1 where wnid like '0101%' order by wnid;

drop table t1;

-- #
-- # Bug #7672 - a wrong result for a select query in braces followed by order by
-- #

CREATE TABLE t1 (a int);
INSERT INTO t1 VALUES (2), (1), (1), (2), (1);
SELECT a FROM t1 ORDER BY a;
(SELECT a FROM t1) ORDER BY a;
DROP TABLE t1;

-- #
-- # Bug #18767: global ORDER BY applied to a SELECT with ORDER BY either was
-- #             ignored or 'concatened' to the latter. 

CREATE TABLE t1 (a int, b int);
INSERT INTO t1 VALUES (1,30), (2,20), (1,10), (2,30), (1,20), (2,10);

(SELECT b,a FROM t1 ORDER BY a,b) ORDER BY b,a;
(SELECT b FROM t1 ORDER BY b DESC) ORDER BY b ASC;
(SELECT b,a FROM t1 ORDER BY b,a) ORDER BY a,b;
(SELECT b,a FROM t1 ORDER by b,a LIMIT 3) ORDER by a,b;

DROP TABLE t1;

-- #
-- # Bug #22457: Column alias in ORDER BY works, but not if in an expression
-- #

CREATE TABLE t1 (a INT); INSERT INTO t1 VALUES (1),(2);
SELECT a + 1 AS num FROM t1 ORDER BY 30 - num;
SELECT CONCAT('test', a) AS str FROM t1 ORDER BY UPPER(str);
--source include/turn_off_only_full_group_by.inc
SELECT a + 1 AS num FROM t1 GROUP BY 30 - num;
--source include/restore_sql_mode_after_turn_off_only_full_group_by.inc
SELECT a + 1 AS num FROM t1 HAVING 30 - num;
--error ER_BAD_FIELD_ERROR
SELECT a + 1 AS num, num + 1 FROM t1;
SELECT a + 1 AS num, (select num + 2 FROM t1 LIMIT 1) FROM t1;
--error ER_BAD_FIELD_ERROR
SELECT a.a + 1 AS num FROM t1 a JOIN t1 b ON num = b.a;
DROP TABLE t1;

-- #
-- # Bug#25126: Reference to non-existant column in UPDATE...ORDER BY... 
-- #       crashes server
-- #
CREATE TABLE bug25126 (
  val int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY
);
--error 1054
UPDATE bug25126 SET MissingCol = MissingCol;
--error 1054
UPDATE bug25126 SET val = val ORDER BY MissingCol;
UPDATE bug25126 SET val = val ORDER BY val;
UPDATE bug25126 SET val = 1 ORDER BY val;
--error 1054
UPDATE bug25126 SET val = 1 ORDER BY MissingCol;
--error 1054
UPDATE bug25126 SET val = 1 ORDER BY val, MissingCol;
--error 1054
UPDATE bug25126 SET val = MissingCol ORDER BY MissingCol;
--error 1054
UPDATE bug25126 SET MissingCol = 1 ORDER BY val, MissingCol;
--error 1054
UPDATE bug25126 SET MissingCol = 1 ORDER BY MissingCol;
--error 1054
UPDATE bug25126 SET MissingCol = val ORDER BY MissingCol;
--error 1054
UPDATE bug25126 SET MissingCol = MissingCol ORDER BY MissingCol;
DROP TABLE bug25126;

-- #
-- # Bug #25427: crash when order by expression contains a name
-- #             that cannot be resolved unambiguously               
-- #

CREATE TABLE t1 (a int);

SELECT p.a AS val, q.a AS val1 FROM t1 p, t1 q ORDER BY val > 1;
--error 1052
SELECT p.a AS val, q.a AS val FROM t1 p, t1 q ORDER BY val;
--error 1052
SELECT p.a AS val, q.a AS val FROM t1 p, t1 q ORDER BY val > 1;

DROP TABLE t1;

-- #
-- # Bug #27532: ORDER/GROUP BY expressions with IN/BETWEEN and NOT IN/BETWEEN
-- #                          

CREATE TABLE t1 (a int);
INSERT INTO t1 VALUES (3), (2), (4), (1);

SELECT a, IF(a IN (2,3), a, a+10) FROM t1
  ORDER BY IF(a IN (2,3), a, a+10);
SELECT a, IF(a NOT IN (2,3), a, a+10) FROM t1 
  ORDER BY IF(a NOT IN (2,3), a, a+10);
SELECT a, IF(a IN (2,3), a, a+10) FROM t1 
  ORDER BY IF(a NOT IN (2,3), a, a+10);

SELECT a, IF(a BETWEEN 2 AND 3, a, a+10) FROM t1
  ORDER BY IF(a BETWEEN 2 AND 3, a, a+10);
SELECT a, IF(a NOT BETWEEN 2 AND 3, a, a+10) FROM t1 
  ORDER BY IF(a NOT BETWEEN 2 AND 3, a, a+10);
SELECT a, IF(a BETWEEN 2 AND 3, a, a+10) FROM t1 
  ORDER BY IF(a NOT BETWEEN 2 AND 3, a, a+10);

SELECT IF(a IN (1,2), a, '') as x1, IF(a NOT IN (1,2), a, '') as x2
  FROM t1 GROUP BY x1, x2;
SELECT IF(a IN (1,2), a, '') as x1, IF(a NOT IN (1,2), a, '') as x2
  FROM t1 GROUP BY x1, IF(a NOT IN (1,2), a, '');

-- # The remaining queries are for better coverage
SELECT a, a IN (1,2) FROM t1 ORDER BY a IN (1,2);
SELECT a FROM t1 ORDER BY a IN (1,2);
SELECT a+10 FROM t1 ORDER BY a IN (1,2);
SELECT a, IF(a IN (1,2), a, a+10) FROM t1
  ORDER BY IF(a IN (3,4), a, a+10);   
DROP TABLE t1;

-- # End of 4.1
create table t1 (a int not null, b  int not null, c int not null);
insert t1 values (1,1,1),(1,1,2),(1,2,1);
select a, b from t1 group by a, b order by sum(c);
drop table t1;

-- #
-- # Bug#21302: Result not properly sorted when using an ORDER BY on a second 
-- #             table in a join
-- #
CREATE TABLE t1 (a int, b int, PRIMARY KEY  (a));
INSERT INTO t1 VALUES (1,1), (2,2), (3,3);

explain SELECT t1.b as a, t2.b as c FROM 
 t1 LEFT JOIN t1 t2 ON (t1.a = t2.a AND t2.a = 2) 
ORDER BY c;
SELECT t2.b as c FROM 
 t1 LEFT JOIN t1 t2 ON (t1.a = t2.a AND t2.a = 2) 
ORDER BY c;

# check that it still removes sort of const table
explain SELECT t1.b as a, t2.b as c FROM 
 t1 JOIN t1 t2 ON (t1.a = t2.a AND t2.a = 2)  
ORDER BY c;

CREATE TABLE t2 LIKE t1;
INSERT INTO t2 SELECT * from t1;
CREATE TABLE t3 LIKE t1;
INSERT INTO t3 SELECT * from t1;
CREATE TABLE t4 LIKE t1;
INSERT INTO t4 SELECT * from t1;
INSERT INTO t1 values (0,0),(4,4);

SELECT t2.b FROM t1 LEFT JOIN (t2, t3 LEFT JOIN t4 ON t3.a=t4.a)
ON (t1.a=t2.a AND t1.b=t3.b) order by t2.b;

DROP TABLE t1,t2,t3,t4;

-- #
-- # Bug#25376: Incomplete setup of ORDER BY clause results in a wrong result.
-- #
create table t1 (a int, b int, c int);
insert into t1 values (1,2,3), (9,8,3), (19,4,3), (1,4,9);
select a,(sum(b)/sum(c)) as ratio from t1 group by a order by sum(b)/sum(c) asc;
drop table t1;

-- #
-- # Bug#26672: Incorrect SEC_TO_TIME() casting in ORDER BY
-- #
CREATE TABLE t1 (a INT UNSIGNED NOT NULL, b TIME);
INSERT INTO t1 (a) VALUES (100000), (0), (100), (1000000),(10000), (1000), (10);
UPDATE t1 SET b = SEC_TO_TIME(a);

-- # Correct ORDER
SELECT a, b FROM t1 ORDER BY b DESC;

-- # must be ordered as the above
SELECT a, b FROM t1 ORDER BY SEC_TO_TIME(a) DESC;

DROP TABLE t1;

-- #
-- # BUG#16590: Optimized does not do right 'const' table pre-read
-- #
CREATE TABLE t1 (a INT, b INT, PRIMARY KEY (a), UNIQUE KEY b (b));
INSERT INTO t1 VALUES (1,1),(2,2);

CREATE TABLE t2 (a INT, b INT, KEY a (a,b));
INSERT INTO t2 VALUES (1,1),(1,2),(2,1),(2,2);

EXPLAIN SELECT 1 FROM t1,t2 WHERE t1.b=2 AND t1.a=t2.a ORDER BY t2.b;

DROP TABLE t1,t2;

-- # End of 5.0

-- #
-- # Bug #28404: query with ORDER BY and ref access
-- #

CREATE TABLE t1(
  id int auto_increment PRIMARY KEY, c2 int, c3 int, INDEX k2(c2), INDEX k3(c3));

INSERT INTO t1 (c2,c3) VALUES
 (31,34),(35,38),(34,31),(32,35),(31,39),
 (11,14),(15,18),(14,11),(12,15),(11,19);

INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
INSERT INTO t1 (c2,c3) SELECT c2,c3 FROM t1;
UPDATE t1 SET c2=20 WHERE id%100 = 0;
SELECT COUNT(*) FROM t1;

CREATE TABLE t2 LIKE t1;
INSERT INTO t2 SELECT * FROM t1 ORDER BY id;

EXPLAIN SELECT id,c3 FROM t2 WHERE c2=11 ORDER BY c3 LIMIT 20;
EXPLAIN SELECT id,c3 FROM t2 WHERE c2=11 ORDER BY c3 LIMIT 4000;
EXPLAIN SELECT id,c3 FROM t2 WHERE c2 BETWEEN 10 AND 12 ORDER BY c3 LIMIT 20;
EXPLAIN SELECT id,c3 FROM t2 WHERE c2 BETWEEN 20 AND 30 ORDER BY c3 LIMIT 4000;

SELECT id,c3 FROM t2 WHERE c2=11 ORDER BY c3 LIMIT 20;

DROP TABLE t1,t2;
