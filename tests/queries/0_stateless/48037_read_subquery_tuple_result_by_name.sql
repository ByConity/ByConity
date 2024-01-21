select tupleElement((select dummy as x, dummy + 1 as y from system.one), 'x');
select tupleElement((select 1 as x, 2 as y), 'x');
select tupleElement((select 1, 2), '1');
select tupleElement((select 1, 1), '1'); -- this query can run successfully by luck
