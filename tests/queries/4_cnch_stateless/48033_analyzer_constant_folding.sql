DROP TABLE IF EXISTS constant_folding;
set enable_optimizer=1;

CREATE TABLE constant_folding(`a` UInt64, `map1` Map(String, UInt64) ) ENGINE = CnchMergeTree ORDER BY a;

insert into constant_folding values(1, {'r0_viking_recall_local_life_gmv_cnt':1});

SELECT map1{concat_ws('_', 'r0', 'viking_recall_local_life_gmv', 'cnt')} FROM constant_folding;

SELECT extractGroups('hello world', CAST('(\\w+) (\\w+)' as FixedString(11)));

SELECT * FROM constant_folding where length(extractGroups('hello world', CAST('(\\w+) (\\w+)' as FixedString(11)))) = 2;

explain SELECT * FROM constant_folding where length(extractGroups('hello world', CAST('(\\w+) (\\w+)' as FixedString(11)))) = 2;

explain SELECT extractGroups('hello world', CAST('(\\w+) (\\w+)' as FixedString(11)));

explain SELECT map1{concat_ws('_', 'r0', 'viking_recall_local_life_gmv', 'cnt')} FROM constant_folding;

DROP TABLE IF EXISTS constant_folding;
