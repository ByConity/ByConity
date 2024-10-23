select name, value
from system.settings
where name in
  ('dialect_type', 'join_use_nulls', 'allow_extended_type_conversion', 'cast_keep_nullable')
order by name settings dialect_type = 'ANSI';

select '--';

select name, value
from system.settings
where name in
  ('dialect_type', 'join_use_nulls', 'allow_extended_type_conversion', 'cast_keep_nullable')
order by name settings dialect_type = 'CLICKHOUSE';
