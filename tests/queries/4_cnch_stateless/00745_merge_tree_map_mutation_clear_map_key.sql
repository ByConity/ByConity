DROP TABLE IF EXISTS 00745_merge_tree_map_mutation_clear_map_key sync;

--- map kv type doesn't support clear map key command
CREATE TABLE 00745_merge_tree_map_mutation_clear_map_key (n UInt8, `string_map` Map(String, String) KV) Engine=CnchMergeTree ORDER BY n;
ALTER TABLE 00745_merge_tree_map_mutation_clear_map_key clear map key string_map('fake'); -- { serverError 44 }
DROP TABLE 00745_merge_tree_map_mutation_clear_map_key sync;

--- test clear map key
SELECT 'test wide part, compact map, clear map key';

CREATE TABLE 00745_merge_tree_map_mutation_clear_map_key (n UInt8, `string_map` Map(String, String), `int_map` Map(UInt64, UInt64), `float_map` Map(Float32, Float32)) Engine=CnchMergeTree ORDER BY n;

system start merges 00745_merge_tree_map_mutation_clear_map_key;

insert into 00745_merge_tree_map_mutation_clear_map_key values (1, {'k1': 'v1', 'k2': 'v2'}, {221: 10, 2: 20}, {1.11: 11.1, 1.112: 22.2});
SELECT 'select * from 00745_merge_tree_map_mutation_clear_map_key';
select * from 00745_merge_tree_map_mutation_clear_map_key settings allow_map_access_without_key = 0; -- { serverError 48 }
select * from 00745_merge_tree_map_mutation_clear_map_key;

select 'clear some map keys';
ALTER TABLE 00745_merge_tree_map_mutation_clear_map_key clear map key string_map('k2'), clear map key int_map(2), clear map key float_map(1.11) settings mutations_sync=1;

-- MergeMutateThread will wait ~80 seconds before scheduling
-- manipulations maybe unfinished even if add mutations_sync=1, sleep here is slow but can make ci stable
select sleepEachRow(3) from numbers(30) format Null;

SELECT 'select * from 00745_merge_tree_map_mutation_clear_map_key';
select * from 00745_merge_tree_map_mutation_clear_map_key;

DROP TABLE 00745_merge_tree_map_mutation_clear_map_key sync;
