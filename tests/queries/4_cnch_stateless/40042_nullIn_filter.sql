set transform_null_in = 1;

select 1 from (select NULL as a) where a in NULL;
select 2 from (select 1 as a) where a in NULL;
select 3 from (select NULL as a) where not(a not in NULL);
select 4 from (select 1 as a) where not(a not in NULL);
