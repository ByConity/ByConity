drop table if exists cust;
drop table if exists cust_local;
drop table if exists web;
drop table if exists web_local;

create table cust
(
  sk Int32,
  c_customer_id String,
  item Nullable(Int64),
  constraint un unique (sk)
) ENGINE = CnchMergeTree
ORDER BY sk;

CREATE TABLE web
(
  ws_item_sk Int64,
  sk Nullable(Int64),
  price Nullable(Float),
  constraint fk4 foreign key(sk) references cust(sk)
) ENGINE = CnchMergeTree 
order by ws_item_sk;

insert into cust values(1, 'id1', 10000);
insert into cust values(2, 'id2', 20000);
insert into cust values(3, 'id3', 30000);

insert into web values(100, 1, 1000.2);
insert into web values(200, 2, 2000.3);
insert into web values(300, null, 3003.5);

set enable_optimizer=1;
set enable_eliminate_join_by_fk=1;
set enable_eliminate_complicated_pk_fk_join=1;
set join_use_nulls=1;
set enum_replicate_no_stats=0;

select 'real pk?';
SELECT count()
FROM web AS a3,
(
    SELECT b.sk AS bsk
    FROM cust AS b, web AS a1
    WHERE (a1.price < '2000.0') AND (a1.sk = b.sk)
) AS t
WHERE t.bsk = a3.sk;

select 'OK: fk inner join pk';
EXPLAIN
SELECT count()
FROM web AS a, cust AS b
WHERE a.sk = b.sk;

SELECT count()
FROM web AS a, cust AS b
WHERE a.sk = b.sk;

select 'OK: fk left outer join pk with join filter';
EXPLAIN
SELECT count()
FROM web AS a left outer join cust AS b
on a.sk = b.sk and a.price < 2000;

SELECT count()
FROM web AS a left outer join cust AS b
on a.sk = b.sk and a.price < 2000;

select 'OK: fk left outer join pk';
EXPLAIN
SELECT a.price
FROM web AS a left join cust AS b on a.sk = b.sk
order by a.price limit 10;

SELECT a.price
FROM web AS a left join cust AS b on a.sk = b.sk
order by a.price limit 10;

select 'OK: pk right outer join fk';
EXPLAIN
SELECT a.price
FROM cust AS b right join web AS a on a.sk = b.sk
order by a.price limit 10;

SELECT a.price
FROM cust AS b right join web AS a on a.sk = b.sk
order by a.price limit 10;

select 'not OK: fk right outer join pk';
EXPLAIN
SELECT a.price
FROM web AS a right join cust AS b on a.sk = b.sk
order by a.price limit 10;

SELECT a.price
FROM web AS a right join cust AS b on a.sk = b.sk
order by a.price limit 10;

select 'OK: fk semi join pk';
EXPLAIN
SELECT a.price
FROM web AS a semi join cust AS b on a.sk = b.sk
order by a.price limit 10;

SELECT a.price
FROM web AS a semi join cust AS b on a.sk = b.sk
order by a.price limit 10;

select 'not OK: another incompeleted pk table';
EXPLAIN 
with tmp as (
  SELECT b.sk as bbsk
  FROM  cust AS b left outer join (select sum(sk) as bsk from cust group by c_customer_id)
  on bsk = b.sk
)
select * from tmp, web
where tmp.bbsk = web.sk
order by web.price;

with tmp as (
  SELECT b.sk as bbsk
  FROM  cust AS b left outer join (select sum(sk) as bsk from cust group by c_customer_id)
  on bsk = b.sk
)
select * from tmp, web
where tmp.bbsk = web.sk
order by web.price;


select 'not OK: under bottom join, pk right outer join others';
explain
with tmp as (
  SELECT b.sk as bbsk
  FROM  cust AS b right outer join (select ws_item_sk as wsk from web)
  on wsk = b.sk
)
select * from tmp, web
where tmp.bbsk = web.sk
order by web.price;

with tmp as (
  SELECT b.sk as bbsk
  FROM  cust AS b right outer join (select ws_item_sk as wsk from web)
  on wsk = b.sk
)
select * from tmp, web
where tmp.bbsk = web.sk
order by web.price;

select 'not OK: unique self join with select *';
explain
select * from cust a, cust b
where a.sk = b.sk;

set enable_eliminate_complicated_pk_fk_join_without_top_join=1;

alter table cust add constraint un2 unique (c_customer_id);

select 'OK: with top union';
explain
select c_customer_id customer_id,
  's' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 2000
group by c_customer_id
union all
select c_customer_id customer_id,
  'c' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 3000
group by c_customer_id
union all
select c_customer_id customer_id,
  'w' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 4000
group by c_customer_id;

select count() from(
select c_customer_id customer_id,
  's' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 2000
group by c_customer_id
union all
select c_customer_id customer_id,
  'c' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 3000
group by c_customer_id
union all
select c_customer_id customer_id,
  'w' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 4000
group by c_customer_id);


select 'not OK: with top union, but pk of one child lost';
explain
select c_customer_id customer_id,
  's' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 2001
  and c_customer_id = 'notmatch'
group by c_customer_id
union all
select c_customer_id customer_id,
  'c' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 3001
group by c_customer_id
union all
select c_customer_id customer_id,
  'w' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 4001
group by c_customer_id;

select count() from(
select c_customer_id customer_id,
  's' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 2000
  and isNotNull(c_customer_id)
group by c_customer_id
union all
select c_customer_id customer_id,
  'c' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 3000
group by c_customer_id
union all
select c_customer_id customer_id,
  'w' sale_type
from cust c,
  web w
where c.sk = w.sk
  and w.price < 4000
group by c_customer_id);
