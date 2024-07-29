set create_stats_time_output=0;
drop table if exists t;
create table t(id UInt64, params Map(String, UInt64)) ENGINE=CnchMergeTree order by id;

create stats t;
show stats t;

create stats t(id2); -- { serverError 36 }
create stats t(params); -- { serverError 36 }

create stats t(`__params__'a'`, `__params__'b'`, `__params__'c'`, `__params__'d'`);
show stats t;

drop stats t;
show stats t;
insert into t values(1, {'a': 10, 'b':100})(2, {'a': 20, 'c':200});


create stats t(`__params__'a'`, `__params__'b'`, `__params__'c'`, `__params__'d'`);
show stats t;
show column_stats t;
drop stats t;
show stats t;

create stats t(`__params__'a'`, `__params__'b'`, `__params__'c'`, `__params__'d'`);
show stats t;


insert into t select number, map('a', number * 10, 'e', number * 50) from system.numbers limit 100;

select 'expected to update a and id';
create stats t;
show stats t;
drop table if exists t;
