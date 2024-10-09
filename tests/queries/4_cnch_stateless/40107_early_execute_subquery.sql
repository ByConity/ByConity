-- { echo }
set early_execute_scalar_subquery = 0, early_execute_in_subquery = 0;

select (select dummy from system.one) as a from system.one;
select 1 in (select dummy from system.one) as a from system.one;
select 1 as a from system.one where (select dummy from system.one);
select 1 as a from system.one where 1 in (select dummy from system.one);
select (select dummy from system.one where 1 = 0) as a from system.one;
select 1 in (select dummy from system.one where 1 = 0) as a from system.one;
select 1 as a from system.one where (select dummy from system.one where 1 = 0);
select 1 as a from system.one where 1 in (select dummy from system.one where 1 = 0);

set early_execute_scalar_subquery = 1, early_execute_in_subquery = 1;

select (select dummy from system.one) as a from system.one;
select 1 in (select dummy from system.one) as a from system.one;
select 1 as a from system.one where (select dummy from system.one);
select 1 as a from system.one where 1 in (select dummy from system.one);
select (select dummy from system.one where 1 = 0) as a from system.one;
select 1 in (select dummy from system.one where 1 = 0) as a from system.one;
select 1 as a from system.one where (select dummy from system.one where 1 = 0);
select 1 as a from system.one where 1 in (select dummy from system.one where 1 = 0);

select 1 as a from system.one where (1,2) in (select dummy+1,dummy+2 from system.one);
select 1 as a from system.one where (1,2) in (select dummy+1,dummy+2 from system.one where 1 = 0);
select 1 as a from system.one where (1,2,3) in (select dummy+1,dummy+2,dummy+3 from system.one where 1 = 0);
