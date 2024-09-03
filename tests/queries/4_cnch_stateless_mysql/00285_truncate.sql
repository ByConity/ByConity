
--
-- Test of truncate
--
--disable_warnings
drop table if exists t1, t2;
--enable_warnings

create table t1 (a integer, b integer,c1 CHAR(10));
insert into t1 (a) values (1),(2);
truncate table t1;
select count(*) from t1;
insert into t1 values(1,2,'test');
select count(*) from t1;
-- delete from t1;
select * from t1;
drop table t1;

--
-- test autoincrement with TRUNCATE; verifying difference with DELETE
--

create table t1 (a integer auto_increment primary key);
insert into t1 (a) values (NULL),(NULL);
truncate table t1;
insert into t1 (a) values (NULL),(NULL);
SELECT * from t1;
-- delete from t1;
insert into t1 (a) values (NULL),(NULL);
SELECT * from t1;
drop table t1;

-- Verifying that temp tables are handled the same way

create temporary table t1 (a integer auto_increment primary key);
insert into t1 (a) values (NULL),(NULL);
truncate table t1;
insert into t1 (a) values (NULL),(NULL);
SELECT * from t1;
-- delete from t1;
insert into t1 (a) values (NULL),(NULL);
SELECT * from t1;
drop table t1;

-- End of 4.1 tests
