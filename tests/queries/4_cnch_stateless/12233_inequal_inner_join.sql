drop table if exists test_inequal_join_inner_12233;
drop table if exists department_inequal_join_inner_12233;
drop table if exists employee_inequal_join_inner_12233;

create table test_inequal_join_inner_12233 (p_date Date, id Int32) engine = CnchMergeTree partition by p_date order by id;

insert into test_inequal_join_inner_12233 select '2023-01-01', number from numbers(10);

select '-----------SQL-1---------';
select count() from test_inequal_join_inner_12233 as a join test_inequal_join_inner_12233 as b on a.p_date = b.p_date and a.id > b.id;

select '-----------SQL-2---------';
select * from test_inequal_join_inner_12233 as a join test_inequal_join_inner_12233 as b on a.p_date = b.p_date and a.id > b.id and a.id > 5 order by a.id;

create table department_inequal_join_inner_12233(
  department_id UInt64,
  department_name String,
  region String,
  create_date Date
) ENGINE = CnchMergeTree partition by toDate(create_date)
order by
  (department_id);

create table employee_inequal_join_inner_12233 (
    name String,
    department_id UInt64,
    salary Nullable(Float64),
    salary_date Date
  ) ENGINE = CnchMergeTree partition by toDate(salary_date)
order by
  (department_id);

insert into
  department_inequal_join_inner_12233
values (31, 'Sales', 'China', '2023-11-01'), (31, 'Sales', 'USA', '2023-11-02'), (33, 'Engineering', 'France', '2023-11-02'),(34, 'Clerical', 'China', '2023-11-03'),(35, 'Marketing', 'England', '2023-11-03');

insert into
  employee_inequal_join_inner_12233
values ('Rafferty', 31, 2000, '2023-10-02'),('Jones', 33, 8000, '2023-10-02'),('Heisenberg', 33, 3000, '2023-10-02'),('Robinson', 34, 4000, '2023-10-02'),('Smith', 34, 5000, '2023-10-02'),('Allen', 38, 10000, '2023-10-02');

select '-----------SQL-3---------';
select
  e.name,
  max(e.salary)
from
  employee_inequal_join_inner_12233 as e 
  join department_inequal_join_inner_12233 as d on e.department_id = d.department_id
  and e.salary > 3000
  and e.name != 'Jones'
group by
  e.name
order by
  e.name;

select '-----------SQL-4---------';
select
  *
from
  employee_inequal_join_inner_12233 as e 
  join department_inequal_join_inner_12233 as d on e.department_id = d.department_id
  and e.salary > 4000
  and e.name != 'Jones'
order by
  e.name, d.region;

select '-----------SQL-5---------';
select
  *
from
  employee_inequal_join_inner_12233 as e 
  join department_inequal_join_inner_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (d.department_name = 'Sales') order by e.name;

select '-----------SQL-6---------';
select
  *
from
  employee_inequal_join_inner_12233 as e 
  join department_inequal_join_inner_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (e.salary < 5000)
  and (d.department_name != 'Sales') order by e.department_id, d.region;


drop table if exists test_inequal_join_inner_12233;
drop table if exists department_inequal_join_inner_12233;
drop table if exists employee_inequal_join_inner_12233;
