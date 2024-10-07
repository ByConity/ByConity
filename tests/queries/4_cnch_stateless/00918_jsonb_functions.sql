SELECT '--json_object--';
select json_object('{a, 1, b, "def", c, 3.5}');
select json_object('{{a, 1},{b, "def"},{c, 3.5}}');
select json_object('{a, b}', '{1,2}');
select json_object('{a, b}', '{1,2');

select '--json_build_array--';
select json_build_array(1,2,'3',4,5);


select '--jsonb_extract--';
-- set enable_optimizer = 0;
select jsonb_extract('{"a":1, "b":2}'::JSONB, '$.a');
select jsonb_extract('{"a":1, "b":2'::JSONB, '$.a');
select jsonb_extract('{"a":{"b":2}}'::JSONB, '$.a.b');
select jsonb_extract('{"a":{"b":2}}'::JSONB, '$.c');
