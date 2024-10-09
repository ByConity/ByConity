DROP TABLE IF EXISTS symbol_allocator;
CREATE TABLE symbol_allocator (a UInt32, _1 UInt32, _2 UInt32, _ UInt32) ENGINE = CnchMergeTree() partition by a order by a;

select _1, _2 from symbol_allocator;
select _1, _ from symbol_allocator;

select _, _1, _2 from symbol_allocator;

DROP TABLE IF EXISTS symbol_allocator;
