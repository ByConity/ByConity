drop table if exists ary;

set dialect_type = 'MYSQL';
set enable_implicit_arg_type_convert = 1;

create table ary(a Nullable(int), b array(int), c array(array(int)), d array(Nullable(float)), e array(FixedString(3)), f array(LowCardinality(Nullable(String))), g Nullable(int), h Array(DateTime)) engine=CnchMergeTree() order by a;

insert into ary values (1, [1], [[1]], [], ['1.2'], [NULL, NULL], NULL, ['2023-11-11', '2011-11-11', '2025-11-11']);
insert into ary values (NULL, [1,2,3], [[1], [2,3]], [1.2, NULL], ['1.2', '2'], ['hello', 'world'], NULL, ['2023-11-11 11:11:11', '2023-11-11 11:11:12', '2023-11-11 11:11:10']);

select '"array_join"';
select array_join(b, 'x'), array_join(b, 100), array_join(b, 78.9), array_join(b, toDate('2024-02-12')), array_join(b, NULL), array_join(b, a), array_join(b, g) from ary order by a;
select array_join(d, 'x'), array_join(e, 'x'), array_join(f, 'x') from ary order by a;
select array_join(d, NULL), array_join(e, NULL), array_join(f, NULL) from ary order by a;
select array_join([1,2,3], 'x');
select array_join(['hello', 'world'], 'x');
select array_join([1,NULL,2], 'x');
select array_join([], 'x');
select array_join([NULL,NULL], 'x');

select array_join(c, 'x') from ary order by a; -- { serverError 43 }

select '"size"';
select size(b), size(c), size(c[1]), size(d), size(e), size(f) from ary order by a;


select '"element_at"';
select element_at(b, 1), element_at(b, 100) from ary order by a; 
select element_at(b, -1), element_at(b, -100) from ary order by a;
select element_at(b, NULL) from ary order by a;
select element_at(b, a) from ary order by a;
select element_at(b, g) from ary order by a;
select element_at(c, 1) from ary order by a;
select element_at(d, 1), element_at(d, 100) from ary order by a;
select element_at(e, -2) from ary order by a;
select element_at(f, 2) from ary order by a;

select element_at([1,2,3], 1), element_at([1,2,3], -3), element_at([1,2,3], 3);
select element_at([1,2,3], -4), element_at([1,2,3], 4), element_at([1,2,3], NULL);
select element_at([NULL, 1, 2], 1), element_at([NULL, 1, 2], NULL);

select element_at(b, 0) from ary order by a; -- { serverError 135 }
select element_at(b, 1.2) from ary order by a; -- { serverError 43 }
select element_at([1,2,3], 0); -- { serverError 135 }

select '"slice"';
select slice(b, 1, 1), slice(b, 100, 100) from ary order by a; 
select slice(b, 0, NULL) from ary order by a; 
select slice(b, NULL, -1) from ary order by a;; 
select slice(b, 1, NULL) from ary order by a;; 
select slice(b, NULL, 1) from ary order by a;; 
select slice(b, a, a) from ary order by a;
select slice(b, g, g) from ary order by a;

select slice(c, 1, 100) from ary order by a;
select slice(d, 1, 1), slice(d, 10, 100) from ary order by a;
select slice(e, -1, 3) from ary order by a;
select slice(f, 1, 3) from ary order by a;

select slice([1,2,3], -1, 2);
select slice([NULL, NULL], 1, 2);
select slice(b, 0) from ary order by a; -- {serverError 42 }
select slice(b, 0, 1) from ary order by a; -- { serverError 135 }
select slice(b, 1, -1) from ary order by a; -- { serverError 36 }
select slice(b, 1.1, 1) from ary order by a; -- { serverError 43 }
select slice(b, 1, 1.1) from ary order by a; -- { serverError 43 }


select '"contains"';
select contains(b, 1) from ary order by a;
select contains(b, 1.1) from ary order by a;
select contains(b, '1.1') from ary order by a;
select contains(b, toDate32('2024-01-01')) from ary order by a;
select contains(b, NULL) from ary order by a;  
select contains(b, a) from ary order by a;
select contains(b, g) from ary order by a;
select contains(c, NULL) from ary order by a; 
select contains(d, NULL) from ary order by a;  

-- TODO adb diffs 1.2 and 1.20 for implicit convert to/from string
-- cnch parses 1.2 into 1.20000005 for d due to the fast parsing method in readFloatText.h 
-- shift10() returns a double 0.200000000000000000003. when added into a float 1, 
-- it becomes 1.20000005; therefore all following sqls return 0
select contains(d, 1.2) from ary order by a;  
select contains(d, '1.2') from ary order by a;
select contains(d, 1.20) from ary order by a;
select contains(d, '1.20') from ary order by a;
select contains(d, '1.20abc') from ary order by a;

select contains(e, 1.20) from ary order by a;
select contains(e, 1.2) from ary order by a;
select contains(e, '1.20') from ary order by a;
select contains(e, '1.2') from ary order by a;
select contains(e, 2.31) from ary order by a;
select contains(f, NULL) from ary order by a;
select contains(f, 'hello') from ary order by a;

-- cnch stores the literal numeric values as decimals, 
-- which are not allowed in adb. TODO parse the values as floats.
-- select contains([1.1,2.2,3.0], 2.2);
-- select contains([1.1,2.2,3.0], 2.20);
-- select contains([1.1,2.2,3.0], '2.2');
-- select contains([1.1,2.2,3.0], '2.20');

select contains([], 1);
select contains([], NULL);
select contains([NULL, NULL], 1);
select contains([NULL, NULL], NULL);

select contains(c, 1) from ary order by a; -- { serverError 386 }
select contains(c, [1]) from ary order by a;

select '"array_position"';
select array_position(b, 1) from ary order by a;
select array_position(b, 1.1) from ary order by a;
select array_position(b, '1.1') from ary order by a;
select array_position(b, toDate32('2024-01-01')) from ary order by a;
select array_position(b, NULL) from ary order by a;  
select array_position(b, a) from ary order by a;
select array_position(b, g) from ary order by a;

select array_position(c, NULL) from ary order by a; 

select array_position(d, 1.2) from ary order by a;  
select array_position(d, '1.2') from ary order by a;
select array_position(d, 1.20) from ary order by a;
select array_position(d, '1.20') from ary order by a;
select array_position(d, '1.20abc') from ary order by a;

select array_position(e, 1.20) from ary order by a;
select array_position(e, 1.2) from ary order by a;
select array_position(e, '1.20') from ary order by a;
select array_position(e, '1.2') from ary order by a;
select array_position(e, 2.31) from ary order by a;
select array_position(f, NULL) from ary order by a;
select array_position(f, 'hello') from ary order by a;

-- select array_position([1.1,2.2,3.0], 2.2);
-- select array_position([1.1,2.2,3.0], 2.20);
-- select array_position([1.1,2.2,3.0], '2.2');
-- select array_position([1.1,2.2,3.0], '2.20');

select array_position([], 1);
select array_position([], NULL);
select array_position([NULL, NULL], 1);
select array_position([NULL, NULL], NULL);

select array_position(c, 1) from ary order by a; -- { serverError 386 }
select array_position(c, [1]) from ary order by a;


select '"array_remove"';
select array_remove(b, 1) from ary order by a;
select array_remove(b, 1.1) from ary order by a;
select array_remove(b, '1.1') from ary order by a;
select array_remove(b, toDate32('2024-01-01')) from ary order by a;
select array_remove(b, NULL) from ary order by a;  
select array_remove(b, a) from ary order by a;
select array_remove(b, g) from ary order by a;

select array_remove(d, NULL) from ary order by a;  

select array_remove(d, 1.2) from ary order by a;  
select array_remove(d, '1.2') from ary order by a;
select array_remove(d, 1.20) from ary order by a;
select array_remove(d, '1.20') from ary order by a;
select array_remove(d, '1.20abc') from ary order by a;


select array_remove(e, 1.20) from ary order by a;
select array_remove(e, 1.2) from ary order by a;
select array_remove(e, '1.20') from ary order by a;
select array_remove(e, '1.2') from ary order by a;
select array_remove(e, 2.31) from ary order by a;
select array_remove(f, NULL) from ary order by a;
select array_remove(f, 'hello') from ary order by a;

-- select array_remove([1.1,2.2,3.0], 2.2);
-- select array_remove([1.1,2.2,3.0], 2.20);
-- select array_remove([1.1,2.2,3.0], '2.2');
-- select array_remove([1.1,2.2,3.0], '2.20');
--
select array_remove([], 1);
select array_remove([], NULL);
select array_remove([NULL, NULL], 1);
select array_remove([NULL, NULL], NULL);

select array_remove(c, NULL) from ary order by a; 
select array_remove(c, 1) from ary order by a; -- { serverError 386 }

select array_remove(c, [1]) from ary order by a; 


select '"concat"';
select concat(b, []) from ary order by a;
select concat(b, [NULL, NULL]) from ary order by a;
select concat(b, [100, 101]) from ary order by a;
select concat(b, [100.1, 101.1]) from ary order by a; 
select concat(b, ['200', '201']) from ary order by a;
select concat(b, [100, NULL, 101]) from ary order by a;
select concat(b, ['abc', 'def']) from ary order by a;

select concat(c, [100,101]) from ary order by a;  -- { serverError 53 }

select concat(c, [[100,101], [102, 103]]) from ary order by a;
select concat(d, [100, NULL, 101]) from ary order by a;
select concat(e, [100, 101]) from ary order by a;
select concat(e, ['abc', 'def']) from ary order by a;
select concat(e, ['abcde', 'fgd']) from ary order by a;

select concat(f, ['xxxxx', 'yyyyy']) from ary order by a;
select concat(f, [toDateTime('2024-02-12 12:12:12')]) from ary order by a;

select concat([1,2,3], [3,5]);
select concat([1,2,3], [3,5]);
select concat([1,2,3], [3.0,5.3]);
select concat([1,2,3], [NULL, 1]);
select concat([1,2,3], [NULL, NULL]);
select concat([1,NULL,3], [NULL, NULL]);
select concat([], []);
select concat([], [NULL, NULL]);
select concat([NULL], [NULL, NULL]);

select '"array_union"';
select array_union(b, []) from ary order by a;

select array_union(b, [100, 101]) from ary order by a;
select array_union(b, [100.1, 101.1]) from ary order by a; 
select array_union(b, ['200', '201']) from ary order by a;
select array_union(b, [100, NULL, 101]) from ary order by a;
select array_union(b, ['abc', 'def']) from ary order by a;
select array_union(b, [NULL, NULL]) from ary order by a;

select array_union(c, [100,101]) from ary order by a; -- { serverError 53 }

select array_union(c, [[100,101], [102, 103]]) from ary order by a;
select array_union(d, [100, NULL, 101]) from ary order by a;
select array_union(e, [100, 101]) from ary order by a;
select array_union(e, ['abc', 'def']) from ary order by a;
select array_union(e, ['abcde', 'fgd']) from ary order by a;

select array_union(f, ['xxxxx', 'yyyyy']) from ary order by a;
select array_union(f, [toDateTime('2024-02-12 12:12:12')]) from ary order by a;

select array_union([1,2,3], [3,5]);
select array_union([1,2,3], [3.0,5.3]);
select array_union([1,2,3], [NULL, 1]);
select array_union([1,2,3], [NULL, NULL]);
select array_union([1,NULL,3], [NULL, NULL]);
select array_union([], []);
select array_union([], [NULL, NULL]);
select array_union([NULL], [NULL, NULL]);

select '"array_intersect"';
select array_intersect(b, []) from ary order by a;
select array_intersect(b, [1, 2, 4]) from ary order by a;
-- select array_intersect(b, [1.0, 2.0, 1.3, 100.4]) from ary order by a; 
select array_intersect(b, ['2', '2.0']) from ary order by a;
select array_intersect(b, [100, NULL, 101]) from ary order by a;
select array_intersect(b, ['2', 'def']) from ary order by a;

select array_intersect(b, [NULL, NULL]) from ary order by a;

select array_intersect(c, [2, 3]) from ary order by a;  -- { serverError 53 }

select array_intersect(c, [[2], [2, 3]]) from ary order by a;
-- select array_intersect(c, [[2.0], [2.0, 3]]) from ary order by a;
select array_intersect(d, [1, NULL, 101]) from ary order by a;
select array_intersect(e, [1.2, 101]) from ary order by a;
select array_intersect(e, ['1.2', 'def']) from ary order by a;
select array_intersect(e, ['abcde', 'fgd']) from ary order by a;

select array_intersect(f, ['hello', 'yyyyy']) from ary order by a;
select array_intersect(f, [toDateTime('2024-02-12 12:12:12')]) from ary order by a;

select array_intersect([1,2,3], [3,5]);
-- select array_intersect([1,2,3], [3.0,5.3]);
select array_intersect([1,2,3], [NULL, 1]);

select array_intersect([], []);

select '"array_max/min"';
select array_min(b), array_min(c), array_min(d), array_min(e), array_min(f), array_min(h) from ary order by a;
select array_max(b), array_max(c), array_max(d), array_max(e), array_max(f), array_max(h) from ary order by a;

select '"reverse"';
select reverse(c[2]) from ary order by a;
select reverse(b), reverse(d), reverse(e), reverse(f), reverse(h) from ary order by a;

select '"shuffle"';
select shuffle(b), shuffle(c), shuffle(d), shuffle(e), shuffle(f) from ary format Null; 
select shuffle([1,2,3]), shuffle([NULL, NULL]), shuffle([1.2, 2.3, 3.4]), shuffle([[1], [2,3]]), shuffle(['ab', 'bc', 'de']) format Null;

select '"has"';
select has('[1, 2, 3]', 2);  -- { serverError 43 }
select has([1,2,3], '2');
