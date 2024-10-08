drop table if exists pp;
drop table if exists c;
drop table if exists c1;

CREATE TABLE pp (
  id          UInt8 NOT NULL,
  description String NOT NULL
) engine = CnchMergeTree() order by id;

CREATE TABLE c (
  id          UInt8 NOT NULL,
  parent_id   UInt8 NOT NULL,
  description String NOT NULL,
  CONSTRAINT unq1 UNIQUE (parent_id)
) engine = CnchMergeTree() order by id;

CREATE TABLE c1 (
  id          UInt8 NOT NULL,
  parent_id   UInt8 NOT NULL,
  parent_id1   UInt8 NOT NULL,
  description String NOT NULL,
  CONSTRAINT UNIQUE (parent_id)
) engine = CnchMergeTree() order by id;
show create table c1;
drop table if exists c1;

INSERT INTO pp VALUES (1, 'PARENT ONE');
INSERT INTO pp VALUES (2, 'PARENT TWO');

INSERT INTO c VALUES (1, 1, 'CHILD ONE');
INSERT INTO c VALUES (2, 1, 'CHILD ONE');
INSERT INTO c VALUES (3, 2, 'CHILD TWO');
INSERT INTO c VALUES (4, 2, 'CHILD TWO');


set enable_optimizer=1;
SELECT c.id, c.parent_id, c.description
FROM   c c
       JOIN pp p ON c.parent_id = p.id
       ORDER BY c.id;

set enable_group_by_keys_pruning=1;
EXPLAIN SELECT c.id
FROM   c c
       JOIN pp p ON c.parent_id = p.id
       WHERE c.id = 1
       GROUP BY c.id;

SELECT c.id
FROM   c c
       JOIN pp p ON c.parent_id = p.id
       WHERE c.id = 1
       GROUP BY c.id;

alter table c add constraint my_name_1 unique (id, description);
show create table c;
alter table c drop UNIQUE unq1;
alter table c drop unique my_name_1;
show create table c;