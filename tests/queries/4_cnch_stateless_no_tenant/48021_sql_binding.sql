CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.bindings;
use test;

set tenant_id='12345';
drop global binding if exists '^explain.*select.*from system\.one.*';
drop session binding if exists select * from test.bindings  order by a;

set tenant_id='';
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
drop session binding uuid '142afa3b-d741-6912-0d42-e644f491156c';
show bindings;

-- test session regular expression binding
create session binding '(^select \'bbb*\')|(^select \'aaa*\')' settings query_dry_run_mode = 'skip_execute_query';
show bindings;
select 'bbbb';
select 'aaaa';
drop session binding '(^select \'bbb*\')|(^select \'aaa*\')';

set enable_optimizer=1;
explain select * from system.one;
create session binding '^explain.*select.*from system\.one' settings query_dry_run_mode = 'skip_execute_query';
explain select * from system.one;
drop session binding '^explain.*select.*from system\.one';
show bindings;

-- test global regular expression binding
set enable_optimizer=1;
explain select * from system.one;
create global binding '^explain.*select.*from system\.one' settings query_dry_run_mode = 'skip_execute_query';
explain select * from system.one;
drop global binding '^explain.*select.*from system\.one';
show bindings;

create global binding '^select \'bbb\'' settings query_dry_run_mode = 'skip_execute_query';
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
create session binding '^select \* from test\.bindings.*' settings query_dry_run_mode = 'skip_execute_query';
create global binding select * from test.bindings order by a using select a+2 from test.bindings order by a;
create global binding '^select \* from test\.bindings.*' settings query_dry_run_mode = 'skip_execute_query';
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
create session binding '^select \* from test\.bindings.*' settings query_dry_run_mode = 'skip_execute_query';
create session binding '^select \* from test\.bindin.*' settings enable_optimizer=0;

show bindings;
select * from test.bindings order by a;
drop session binding '^select \* from test\.bindin.*';
select * from test.bindings order by a;
drop session binding '^select \* from test\.bindings.*';
show bindings;

create global binding '^select \* from test\.bindings.*' settings query_dry_run_mode = 'skip_execute_query';
create global binding '^select \* from test\.bindin.*' settings enable_optimizer=0;

show bindings;
select * from test.bindings order by a;
drop global binding '^select \* from test\.bindin.*';
select * from test.bindings order by a;
drop global binding '^select \* from test\.bindings.*';
show bindings;

set tenant_id='';
create session binding select * from test.bindings  order by a using select a+1 from test.bindings order by a;
create global binding '^select \* from test\.bindings.*' settings query_dry_run_mode = 'skip_execute_query';
set tenant_id='12345';
show bindings;
create session binding select * from test.bindings  order by a using select a+1 from test.bindings order by a;
create global binding '^explain.*select.*from system\.one.*' settings query_dry_run_mode = 'skip_execute_query';

-- test replace
create global binding or replace '^explain.*select.*from system\.one.*' settings query_dry_run_mode = 'skip_execute_query';

-- test if not exists
create global binding if not exists '^explain.*select.*from system\.one.*' settings query_dry_run_mode = 'skip_execute_query';

show bindings;
explain select * from system.one;

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.bindings;
CREATE TABLE test.bindings(a Int32, b Int32) ENGINE = CnchMergeTree() PARTITION BY `a` PRIMARY KEY `a` ORDER BY `a`;
insert into test.bindings values(1,2)(2,3)(3,4);
select * from test.bindings order by a;

drop global binding '^explain.*select.*from system\.one.*';

set tenant_id='';
show bindings;

drop global binding '^select \* from test\.bindings.*';
drop session binding select * from test.bindings  order by a;
drop session binding uuid '5ebc74a1-9944-b052-9841-e30966b7c0fb';

DROP TABLE IF EXISTS test.bindings;
