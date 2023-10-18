SET union_default_mode = '';
select 1 UNION select 1 UNION ALL select 1; -- { serverError 558 }
