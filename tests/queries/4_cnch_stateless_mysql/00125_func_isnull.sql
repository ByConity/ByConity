
--
-- test of ISNULL()
--

--disable_warnings
drop table if exists t1;
--enable_warnings

create table t1 (id int not null primary key, mydate date not null);
insert into t1 values (0,'2002-05-01'),(0,'2002-05-01'),(0,'2002-05-01');
-- flush tables;
select * from t1 where isnull(to_days(mydate));
drop table t1;

--
-- Bug #41371    Select returns 1 row with condition 'col is not null and col is null'
--

CREATE TABLE t1 (id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY(id));
INSERT INTO t1( id ) VALUES ( NULL );
SELECT t1.id  FROM t1  WHERE (id  is not null and id is null );
DROP TABLE t1;

-- End of 5.1 tests

