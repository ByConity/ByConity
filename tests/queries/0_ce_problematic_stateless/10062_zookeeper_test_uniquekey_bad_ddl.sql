set database_atomic_wait_for_drop_and_detach_synchronously = 1;
CREATE DATABASE IF NOT EXISTS test;

drop table if exists test.ha_unique_bad1;
drop table if exists test.ha_unique_bad2;
drop table if exists test.ha_unique_bad3;
drop table if exists test.ha_unique_bad4;
drop table if exists test.ha_unique_bad5;

create table test.ha_unique_bad1 (d Date, i Int32, s String) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/ha_unique_bad1') partition by d unique key i order by (i, s); -- { serverError 36 }
create table test.ha_unique_bad2 (d Date, i Int32, s String) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/ha_unique_bad2', '1') partition by d order by (i, s); -- { serverError 49 }
create table test.ha_unique_bad3 (d Date, i Nullable(Int32), s String) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/ha_unique_bad3', '1') partition by d unique key i order by s; -- { serverError 44 }
create table test.ha_unique_bad5 (d Date, i Int32, s String) ENGINE=MergeTree partition by d unique key i order by s; -- { serverError 36 }

drop table if exists test.ha_unique_bad1;
drop table if exists test.ha_unique_bad2;
drop table if exists test.ha_unique_bad3;
drop table if exists test.ha_unique_bad4;
drop table if exists test.ha_unique_bad5;
