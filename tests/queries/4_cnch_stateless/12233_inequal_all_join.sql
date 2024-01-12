drop table if exists test_inequal_join_all_12233;
drop table if exists department_inequal_join_all_12233;
drop table if exists employee_inequal_join_all_12233;

--- all right join only executed optimizer can get correct result, global right  When the data at each node is not orthogonal, a right join will result in an error.
set enable_optimizer = 1;

create table test_inequal_join_all_12233 (p_date Date, id Int32) engine = CnchMergeTree partition by p_date order by id;
insert into test_inequal_join_all_12233 select '2023-01-01', number from numbers(10);

create table department_inequal_join_all_12233(
  department_id UInt64,
  department_name String,
  region String,
  create_date Date
) ENGINE = CnchMergeTree partition by toDate(create_date)
order by
  (department_id);

create table employee_inequal_join_all_12233 (
    name String,
    department_id UInt64,
    salary Nullable(Float64),
    salary_date Date
  ) ENGINE = CnchMergeTree partition by toDate(salary_date)
order by
  (department_id);

insert into
  department_inequal_join_all_12233
values (31, 'Sales', 'China', '2023-11-01'), (31, 'Sales', 'USA', '2023-11-02'), (33, 'Engineering', 'France', '2023-11-02'),(34, 'Clerical', 'China', '2023-11-03'),(35, 'Marketing', 'England', '2023-11-03');

insert into
  employee_inequal_join_all_12233
values ('Rafferty', 31, 2000, '2023-10-02'),('Jones', 33, 8000, '2023-10-02'),('Heisenberg', 33, 3000, '2023-10-02'),('Robinson', 34, 4000, '2023-10-02'),('Smith', 34, 5000, '2023-10-02'),('Allen', 38, 10000, '2023-10-02');

set join_use_nulls =1;
select '----------------------join_use_nulls=1--------------------';
select '-----------SQL-1---------';
select count() from test_inequal_join_all_12233 as a left join test_inequal_join_all_12233 as b on a.p_date = b.p_date and a.id > b.id;

select '-----------SQL-2---------';
select * from test_inequal_join_all_12233 as a left join test_inequal_join_all_12233 as b on a.p_date = b.p_date and a.id > b.id order by a.id;

select '-----------SQL-3---------';
select count() from test_inequal_join_all_12233 as a right join test_inequal_join_all_12233 as b on a.p_date = b.p_date and a.id > b.id;

select '-----------SQL-4---------';
select * from test_inequal_join_all_12233 as a right join test_inequal_join_all_12233 as b on a.p_date = b.p_date and a.id > b.id order by b.id;

select '-----------SQL-5---------';
select
  e.name,
  max(e.salary)
from
  employee_inequal_join_all_12233 as e
  left join department_inequal_join_all_12233 as d on e.department_id = d.department_id
  and e.salary > 3000
  and e.name != 'Jones'
group by
  e.name
order by
  e.name;

select '-----------SQL-6---------';
select
  e.name,
  max(e.salary)
from
  employee_inequal_join_all_12233 as e
  right join department_inequal_join_all_12233 as d on e.department_id = d.department_id
  and e.salary > 3000
  and e.name != 'Jones'
group by
  e.name
order by
  e.name;

select '-----------SQL-7---------';
select
  *
from
  employee_inequal_join_all_12233 as e
  left join department_inequal_join_all_12233 as d on e.department_id = d.department_id
  and e.salary > 1000
  and e.name != 'Jones'
order by
  e.name, d.region;

select '-----------SQL-8---------';
select
  *
from
  employee_inequal_join_all_12233 as e
  right join department_inequal_join_all_12233 as d on e.department_id = d.department_id
  and e.salary > 1000
  and e.name != 'Jones'
order by
  e.name, d.region;

select '-----------SQL-9---------';
select
  *
from
  employee_inequal_join_all_12233 as e
  left join department_inequal_join_all_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (e.salary < 5000)
  and (d.department_name != 'Sales') order by e.department_id, d.region;

select '-----------SQL-10---------';
select
  *
from
  employee_inequal_join_all_12233 as e
  right join department_inequal_join_all_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (e.salary < 5000)
  and (d.department_name != 'Sales') order by e.department_id, d.region;

set join_use_nulls =0;
select '----------------------join_use_nulls=0--------------------';
select '-----------SQL-11---------';
select count() from test_inequal_join_all_12233 as a left join test_inequal_join_all_12233 as b on a.p_date = b.p_date and a.id > b.id;

select '-----------SQL-12---------';
select * from test_inequal_join_all_12233 as a left join test_inequal_join_all_12233 as b on a.p_date = b.p_date and a.id > b.id order by a.id;

select '-----------SQL-13---------';
select count() from test_inequal_join_all_12233 as a right join test_inequal_join_all_12233 as b on a.p_date = b.p_date and a.id > b.id;

select '-----------SQL-14---------';
select * from test_inequal_join_all_12233 as a right join test_inequal_join_all_12233 as b on a.p_date = b.p_date and a.id > b.id order by b.id;

select '-----------SQL-15---------';
select
  e.name,
  max(e.salary)
from
  employee_inequal_join_all_12233 as e
  left join department_inequal_join_all_12233 as d on e.department_id = d.department_id
  and e.salary > 3000
  and e.name != 'Jones'
group by
  e.name
order by
  e.name;

select '-----------SQL-16---------';
select
  e.name,
  max(e.salary)
from
  employee_inequal_join_all_12233 as e
  right join department_inequal_join_all_12233 as d on e.department_id = d.department_id
  and e.salary > 3000
  and e.name != 'Jones'
group by
  e.name
order by
  e.name;

select '-----------SQL-17---------';
select
  *
from
  employee_inequal_join_all_12233 as e
  left join department_inequal_join_all_12233 as d on e.department_id = d.department_id
  and e.salary > 1000
  and e.name != 'Jones'
order by
  e.name, d.region;

select '-----------SQL-18---------';
select
  *
from
  employee_inequal_join_all_12233 as e
  right join department_inequal_join_all_12233 as d on e.department_id = d.department_id
  and e.salary > 1000
  and e.name != 'Jones'
order by
  e.name, d.region;

select '-----------SQL-19---------';
select
  *
from
  employee_inequal_join_all_12233 as e
  left join department_inequal_join_all_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (e.salary < 5000)
  and (d.department_name != 'Sales') order by e.department_id, d.region;

select '-----------SQL-20---------';
select
  *
from
  employee_inequal_join_all_12233 as e
  left join department_inequal_join_all_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (d.department_name = 'Sales') order by e.name;

select '-----------SQL-21---------';
select
  *
from
  employee_inequal_join_all_12233 as e
  right join department_inequal_join_all_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (e.salary < 5000)
  and (d.department_name != 'Sales') order by e.department_id, d.region;

select '-----------SQL-22---------';
SELECT *
FROM employee_inequal_join_all_12233 AS e
LEFT JOIN department_inequal_join_all_12233 AS d ON (e.department_id = d.department_id) AND (e.salary > 3000) AND (d.department_name = e.name)
ORDER BY e.name ASC;

drop table if exists test_inequal_join_all_12233;
drop table if exists department_inequal_join_all_12233;
drop table if exists employee_inequal_join_all_12233;