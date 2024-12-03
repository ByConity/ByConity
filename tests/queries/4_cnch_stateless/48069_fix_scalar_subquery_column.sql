set early_execute_scalar_subquery=1;
with (select '1') as num select if (num = 1,2,3);
select if ((select '1') = 1,2,3);
select 1 where (select '1') = 1;
