drop table if exists test.test_create_local settings database_atomic_wait_for_drop_and_detach_synchronously=1;;
drop table if exists test.test_tmp_1;
drop table if exists test.test_tmp_2;
drop table if exists test.test_tmp_3;

create table test.test_create_local (tag Int32, id_map BitMap64 BitEngineEncode , p_date Date) engine=HaMergeTree('/clikhouse/test/test.test_create_local_12345/1', 'r1') order by tag partition by p_date TTL p_date + INTERVAL 1 DAY;


create table test.test_tmp_1 as test.test_create_local IGNORE REPLICATED;
show create test.test_tmp_1;

create table test.test_tmp_2 as test.test_create_local IGNORE REPLICATED BITENGINEENCODE;
show create test.test_tmp_2;

create table test.test_tmp_3 as test.test_create_local IGNORE REPLICATED TTL;
show create test.test_tmp_3;


drop table test.test_create_local settings database_atomic_wait_for_drop_and_detach_synchronously=1;;
drop table test.test_tmp_1;
drop table test.test_tmp_2;
drop table test.test_tmp_3;