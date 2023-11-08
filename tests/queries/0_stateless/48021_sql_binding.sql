CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.bindings;
DROP TABLE IF EXISTS test.bindings_local;

use test;
CREATE TABLE test.bindings_local(a Int32, b Int32) ENGINE = MergeTree() PARTITION BY `a` PRIMARY KEY `a` ORDER BY `a`;
create table bindings as bindings_local engine = Distributed(test_shard_localhost, currentDatabase(), bindings_local);
insert into test.bindings values(1,2)(2,3)(3,4);

set use_sql_binding=1;
-- test drop session sql binding use uuid
create session binding select * from test.bindings using select b+1 from test.bindings order by a;
show bindings;
drop session binding uuid '3eab67dd-f380-71e5-00d0-6c5cb21c8693';
show bindings;

-- test ignore case
create session binding select * FROM test.bindings using select b+1 from test.bindings order by a;
show bindings;
SELECT * FROM test.bindings;
Select * from test.bindings;
drop session binding select * from test.bindings;
show bindings;

-- test session regular expression binding
set enable_optimizer=1;
explain select * from system.one;
create session binding '^explain.*select.*from system\\.one' settings enable_execute_query = 0;
explain select * from system.one;
drop session binding '^explain.*select.*from system\\.one';
show bindings;

-- test global regular expression binding
set enable_optimizer=1;
explain select * from system.one;
create global binding '^explain.*select.*from system\\.one' settings enable_execute_query = 0;
explain select * from system.one;
drop global binding '^explain.*select.*from system\\.one';
show bindings;

-- test binding match priority
create session binding select * from test.bindings  order by a using select a+1 from test.bindings order by a;
create session binding '^select \\* from test\\.bindings.*' settings enable_execute_query=0;
create global binding select * from test.bindings order by a using select a+2 from test.bindings order by a;
create global binding '^select \\* from test\\.bindings.*' settings enable_execute_query=0;
show bindings;
select * from test.bindings order by a;
drop session binding select * from test.bindings order by a;
select * from test.bindings order by a;
drop session binding '^select \\* from test\\.bindings.*';
select * from test.bindings order by a;
drop global binding select * from test.bindings order by a;
select * from test.bindings order by a;
drop global binding '^select \\* from test\\.bindings.*';

-- Different regular expression binding can be matched
create session binding '^select \\* from test\\.bindings.*' settings enable_execute_query=0;
create session binding '^select \\* from test\\.bindin.*' settings enable_optimizer=0;

show bindings;
select * from test.bindings order by a;
drop session binding '^select \\* from test\\.bindin.*';
select * from test.bindings order by a;
drop session binding '^select \\* from test\\.bindings.*';
show bindings;

DROP TABLE IF EXISTS test.bindings;
DROP TABLE IF EXISTS test.bindings_local;
