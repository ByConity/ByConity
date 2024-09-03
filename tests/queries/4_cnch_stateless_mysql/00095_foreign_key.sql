-- # The include statement below is a temp one for tests that are yet to
-- #be ported to run with InnoDB,
-- #but needs to be kept for tests that would need MyISAM in future.
-- --source include/force_myisam_default.inc

-- #
-- # Test syntax of foreign keys
-- #

--disable_warnings
drop table if exists t1;
--enable_warnings

create table t1 (
	a int not null references t2,
	b int not null references t2 (c),
	primary key (a,b),
	foreign key (a) references t3 match full,
	foreign key (a) references t3 match partial,
	foreign key (a,b) references t3 (c,d) on delete no action
	  on update no action,
	foreign key (a,b) references t3 (c,d) on update cascade,
	foreign key (a,b) references t3 (c,d) on delete set default,
	foreign key (a,b) references t3 (c,d) on update set null);

create index a on t1 (a);
create unique index b on t1 (a,b);
drop table t1;

-- # End of 4.1 tests

-- #
-- # Bug#34455 (Ambiguous foreign keys syntax is accepted)
-- #

--disable_warnings
drop table if exists t_34455;
--enable_warnings

-- # 2 match clauses, illegal
--error ER_PARSE_ERROR
create table t_34455 (
  a int not null,
  foreign key (a) references t3 (a) match full match partial);

-- # match after on delete, illegal
--error ER_PARSE_ERROR
create table t_34455 (
  a int not null,
  foreign key (a) references t3 (a) on delete set default match full);

-- # match after on update, illegal
--error ER_PARSE_ERROR
create table t_34455 (
  a int not null,
  foreign key (a) references t3 (a) on update set default match full);

-- # 2 on delete clauses, illegal
--error ER_PARSE_ERROR
create table t_34455 (
  a int not null,
  foreign key (a) references t3 (a)
  on delete set default on delete set default);

-- # 2 on update clauses, illegal
--error ER_PARSE_ERROR
create table t_34455 (
  a int not null,
  foreign key (a) references t3 (a)
  on update set default on update set default);

create table t_34455 (a int not null);

-- # 2 match clauses, illegal
--error ER_PARSE_ERROR
alter table t_34455
  add foreign key (a) references t3 (a) match full match partial);

-- # match after on delete, illegal
--error ER_PARSE_ERROR
alter table t_34455
  add foreign key (a) references t3 (a) on delete set default match full);

-- # match after on update, illegal
--error ER_PARSE_ERROR
alter table t_34455
  add foreign key (a) references t3 (a) on update set default match full);

-- # 2 on delete clauses, illegal
--error ER_PARSE_ERROR
alter table t_34455
  add foreign key (a) references t3 (a)
  on delete set default on delete set default);

-- # 2 on update clauses, illegal
--error ER_PARSE_ERROR
alter table t_34455
  add foreign key (a) references t3 (a)
  on update set default on update set default);

drop table t_34455;


--echo #
--echo # Bug#20752436: INNODB: FAILING ASSERTION: 0 IN FILE HANDLER0ALTER.CC
--echo # LINE 6647
--echo #
--echo # Verify that index types that cannot be used as foreign keys are
--echo # ignored when creating foreign keys.

-- let $SAVED_foreign_key_checks= `SELECT @@foreign_key_checks`;
-- # set @@foreign_key_checks=0;

CREATE TABLE t1(a CHAR(100), b GEOMETRY NOT NULL) ENGINE InnoDB;

--echo # Creating a foreign key on a GEOMETRY column is not supported
--error ER_BLOB_KEY_WITHOUT_LENGTH
ALTER TABLE t1 ADD CONSTRAINT fi_b FOREIGN KEY(b) REFERENCES ti2(b);

--echo # Adds FULLTEXT and SPATAL indices which cannot be used as foreign keys
ALTER TABLE t1 ADD FULLTEXT INDEX(a), ADD SPATIAL INDEX(b);

--echo # Adds a foreign key on column with FULLTEXT index.
--echo # The FULLTEXT index cannot be used and the generated key must be kept
ALTER TABLE t1 ADD CONSTRAINT fi_a FOREIGN KEY(a) REFERENCES ti2(a);
SHOW INDEXES FROM t1;

--echo # Attempt to add a foreign key on column with SPATIAL index.
--echo # The SPATIAL index cannot be used so this becomes an attempt at
--echo # creating a foreign key on a GEOMETRY column which is not supported
--error ER_BLOB_KEY_WITHOUT_LENGTH
ALTER TABLE t1 ADD CONSTRAINT fi_b FOREIGN KEY(b) REFERENCES ti2(b);

DROP TABLE t1;
-- eval set @@foreign_key_checks= $SAVED_foreign_key_checks;
