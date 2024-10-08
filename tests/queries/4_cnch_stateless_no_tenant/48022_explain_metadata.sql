DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS metadata2;
CREATE TABLE metadata(a UInt32, b UInt32) ENGINE = CnchMergeTree() partition by a order by a;
CREATE TABLE metadata2 (a UInt32, b UInt32, c Nullable(UInt64)) ENGINE = CnchMergeTree() partition by a order by a;

explain metadata select '1';

explain metadata select 1+1;

explain metadata select * from metadata;

explain metadata select t1.a, t2.b from metadata t1 join metadata2 t2 on t1.a=t2.a settings enable_optimizer=1;

explain metadata select t1.a, t2.b, t2.a+1 from metadata t1 join metadata2 t2 on t1.a=t2.a;

explain metadata insert into metadata select t1.a as a, t2.b as b from metadata t1 join metadata2 t2 on t1.a=t2.a settings enable_optimizer=1;

explain metadata insert into metadata2 (a, b) select t1.a as a, t2.b as b from metadata t1 join metadata2 t2 on t1.a=t2.a settings enable_optimizer=1;

explain metadata select * from metadata order by a limit 3 with ties;

explain metadata insert into metadata select t1.a as a, t2.b as b from metadata t1  ANY FULL JOIN metadata2 t2 using(a);

explain metadata select name from cnch(server, system.settings);

explain metadata select * from cnch(server, system, one);

explain metadata select sum(*) from numbers(10);

explain metadata select count() from metadata;

explain metadata select count(*) from metadata;

DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS metadata2;

DROP TABLE IF EXISTS metadata3;

CREATE TABLE metadata3 (a UInt32, b UInt32, c Alias a + 1) ENGINE = CnchMergeTree() partition by a order by a;

explain metadata select b from metadata3;
explain metadata select c from metadata3;

DROP TABLE IF EXISTS metadata3;


set enable_optimizer=1;
set enable_optimizer_fallback=0;

explain metadata select getSetting('enable_optimizer'), getSetting('enable_optimizer_fallback');
explain metadata ignore_format=1 select 1+1 format JSON;
