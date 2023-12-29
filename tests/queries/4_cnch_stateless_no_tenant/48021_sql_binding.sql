CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.bindings;
use test;

drop global binding if exists '^explain.*select.*from system\.one';
drop global binding if exists select * from test.bindings;
drop global binding if exists '^select \'bbb\'';
drop global binding if exists select * from test.bindings order by a;
drop global binding if exists '^select \* from test\.bindings.*';
drop global binding if exists '^select \* from test\.bindin.*';

CREATE TABLE test.bindings(a Int32, b Int32) ENGINE = CnchMergeTree() PARTITION BY `a` PRIMARY KEY `a` ORDER BY `a`;
insert into test.bindings values(1,2)(2,3)(3,4);
set send_logs_level='error';
-- test session sql binding
create session binding select * FROM test.bindings order by a using select a+1 from test.bindings order by a;
show bindings;
set use_sql_binding = 0;
select * from test.bindings order by a;
set use_sql_binding = 1;
select * from test.bindings ORDER BY a;
Select * From test.bindings order By a;
Select * From test.bindings order by a;
drop session binding select * from test.bindings order by a;
show bindings;

-- test drop session sql binding use uuid
create session binding select * from test.bindings using select b+1 from test.bindings order by a;
show bindings; 

-- re-generate uuid if AST serialization has been changed
drop session binding uuid '32b6b495-200b-1786-3558-aea29e03e240';
show bindings;

-- test session regular expression binding
create session binding '(^select \'bbb*\')|(^select \'aaa*\')' settings enable_execute_query=0;
show bindings;
select 'bbbb';
select 'aaaa';
drop session binding '(^select \'bbb*\')|(^select \'aaa*\')';

set enable_optimizer=1;
explain select * from system.one;
create session binding '^explain.*select.*from system\.one' settings enable_execute_query = 0;
explain select * from system.one;
drop session binding '^explain.*select.*from system\.one';
show bindings;

-- test global regular expression binding
set enable_optimizer=1;
explain select * from system.one;
create global binding '^explain.*select.*from system\.one' settings enable_execute_query = 0;
explain select * from system.one;
drop global binding '^explain.*select.*from system\.one';
show bindings;

create global binding '^select \'bbb\'' settings enable_execute_query=0;
select 'bbb';
create global binding select * from test.bindings using select a+1 from test.bindings order by a;
select * from test.bindings;
show bindings;
drop global binding select * from test.bindings;
drop global binding '^select \'bbb\'';
SELECT updateBindingCache() as a FROM cnch(server, system.one) group by a;
show bindings;

-- test binding match priority
create session binding select * from test.bindings  order by a using select a+1 from test.bindings order by a;
create session binding '^select \* from test\.bindings.*' settings enable_execute_query=0;
create global binding select * from test.bindings order by a using select a+2 from test.bindings order by a;
create global binding '^select \* from test\.bindings.*' settings enable_execute_query=0;
show bindings;
select * from test.bindings order by a;
drop session binding select * from test.bindings order by a;
select * from test.bindings order by a;
drop session binding '^select \* from test\.bindings.*';
select * from test.bindings order by a;
drop global binding select * from test.bindings order by a;
select * from test.bindings order by a;
drop global binding '^select \* from test\.bindings.*';

-- Different regular expression binding can be matched
create session binding '^select \* from test\.bindings.*' settings enable_execute_query=0;
create session binding '^select \* from test\.bindin.*' settings enable_optimizer=0;

show bindings;
select * from test.bindings order by a;
drop session binding '^select \* from test\.bindin.*';
select * from test.bindings order by a;
drop session binding '^select \* from test\.bindings.*';
show bindings;

create global binding '^select \* from test\.bindings.*' settings enable_execute_query=0;
create global binding '^select \* from test\.bindin.*' settings enable_optimizer=0;

show bindings;
select * from test.bindings order by a;
drop global binding '^select \* from test\.bindin.*';
select * from test.bindings order by a;
drop global binding '^select \* from test\.bindings.*';
show bindings;

DROP TABLE IF EXISTS test.bindings;
