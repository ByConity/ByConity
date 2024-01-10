drop table if exists test_inequal_join_anti_12233;
drop table if exists department_inequal_join_anti_12233;
drop table if exists employee_inequal_join_anti_12233;

--- anti right join only executed optimizer can get correct result, global right  When the data at each node is not orthogonal, a right join will result in an error.
set enable_optimizer = 1;

create table test_inequal_join_anti_12233 (p_date Date, id Int32) engine = CnchMergeTree partition by p_date order by id;

insert into test_inequal_join_anti_12233 select '2023-01-01', number from numbers(10);

select '-----------SQL-1---------';
select count() from test_inequal_join_anti_12233 as a left anti join test_inequal_join_anti_12233 as b on a.p_date = b.p_date and a.id > b.id;

select '-----------SQL-2---------';
select a.* from test_inequal_join_anti_12233 as a left anti join test_inequal_join_anti_12233 as b on a.p_date = b.p_date and a.id > b.id order by a.id;

select '-----------SQL-3---------';
select count() from test_inequal_join_anti_12233 as a right anti join test_inequal_join_anti_12233 as b on a.p_date = b.p_date and a.id > b.id;

select '-----------SQL-4---------';
select b.* from test_inequal_join_anti_12233 as a right anti join test_inequal_join_anti_12233 as b on a.p_date = b.p_date and a.id < b.id order by b.id;

create table department_inequal_join_anti_12233(
  department_id UInt64,
  department_name String,
  region String,
  create_date Date
) ENGINE = CnchMergeTree partition by toDate(create_date)
order by
  (department_id);

create table employee_inequal_join_anti_12233 (
    name String,
    department_id UInt64,
    salary Nullable(Float64),
    salary_date Date
  ) ENGINE = CnchMergeTree partition by toDate(salary_date)
order by
  (department_id);

insert into
  department_inequal_join_anti_12233
values (31, 'Sales', 'China', '2023-11-01'), (31, 'Sales', 'USA', '2023-11-02'), (33, 'Engineering', 'France', '2023-11-02'),(34, 'Clerical', 'China', '2023-11-03'),(35, 'Marketing', 'England', '2023-11-03');

insert into
  employee_inequal_join_anti_12233
values ('Rafferty', 31, 2000, '2023-10-02'),('Jones', 33, 8000, '2023-10-02'),('Heisenberg', 33, 3000, '2023-10-02'),('Robinson', 34, 4000, '2023-10-02'),('Smith', 34, 5000, '2023-10-02'),('Allen', 38, 10000, '2023-10-02');

---left anti join with aggregate function
select '-----------SQL-5---------';
select
  e.name,
  max(e.salary)
from
  employee_inequal_join_anti_12233 as e 
  left anti join department_inequal_join_anti_12233 as d on e.department_id = d.department_id
  and e.salary > 3000
  and e.name != 'Jones'
group by
  e.name
order by
  e.name;

select '-----------SQL-6---------';
select
  d.department_id,
  count()
from
  employee_inequal_join_anti_12233 as e 
  right anti join department_inequal_join_anti_12233 as d on e.department_id = d.department_id
  and e.salary > 3000
  and e.name != 'Jones'
group by
  d.department_id
order by
  d.department_id;

---left anti join 
---only select left table columns but ch could return all, later fix select * from e left anti join d 
---standard left anti join when group by can only accept left table columns otherwise report error
select '-----------SQL-7---------';
select
  e.*
from
  employee_inequal_join_anti_12233 as e 
  left anti join department_inequal_join_anti_12233 as d on e.department_id = d.department_id
  and e.salary > 4000
  and e.name != 'Jones'
order by
  e.name;

select '-----------SQL-8---------';
select
  d.*
from
  employee_inequal_join_anti_12233 as e 
  right anti join department_inequal_join_anti_12233 as d on e.department_id = d.department_id
  and e.salary > 4000
  and e.name != 'Jones'
order by
  d.department_id, d.region;

---right anti join 
---only select right table columns but ch could return all, later fix select * from e right anti join d 
---standard right anti join when group by can only accept left table columns otherwise report error
select '-----------SQL-9---------';
select
  e.*
from
  employee_inequal_join_anti_12233 as e 
  left anti join department_inequal_join_anti_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (e.salary < 5000)
  and (d.department_name != 'Sales') order by e.name;

select '-----------SQL-10---------';
select
  e.*
from
  employee_inequal_join_anti_12233 as e 
  left anti join department_inequal_join_anti_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (d.department_name = 'Sales') order by e.name;

select '-----------SQL-11---------';
select
  d.*
from
  employee_inequal_join_anti_12233 as e 
  right anti join department_inequal_join_anti_12233 as d on (e.department_id = d.department_id)
  and (e.salary > 3000)
  and (e.salary < 5000)
  and (d.department_name != 'Sales') order by d.department_id, d.region;

drop table if exists test_inequal_join_anti_12233;
drop table if exists department_inequal_join_anti_12233;
drop table if exists employee_inequal_join_anti_12233;



