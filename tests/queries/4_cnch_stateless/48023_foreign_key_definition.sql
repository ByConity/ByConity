set dialect_type='ANSI';
set enable_optimizer=1;

drop table if exists parent;
drop table if exists child;
drop table if exists child1;

CREATE TABLE parent (
  id          UInt8,
  description String
) engine = CnchMergeTree() order by id;

CREATE TABLE child (
  id          UInt8,
  parent_id   Nullable(UInt8),
  description String,
  CONSTRAINT fk1 FOREIGN KEY (parent_id) REFERENCES parent(id)
) engine = CnchMergeTree() order by id;
show create table child;

CREATE TABLE child1 (
  id          UInt8,
  parent_id   Nullable(UInt8),
  parent_id1   Nullable(UInt8),
  description String,
  CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent(id),
  FOREIGN KEY (parent_id1) REFERENCES parent(id)
) engine = CnchMergeTree() order by id;
show create table child1;
DROP TABLE child1;

INSERT INTO parent VALUES (1, 'PARENT ONE');
INSERT INTO parent VALUES (2, 'PARENT TWO');

INSERT INTO child VALUES (1, null, 'CHILD NULL');
INSERT INTO child VALUES (1, 1, 'CHILD ONE');
INSERT INTO child VALUES (2, 1, 'CHILD ONE');
INSERT INTO child VALUES (3, 2, 'CHILD TWO');
INSERT INTO child VALUES (4, 2, 'CHILD TWO');

set enable_eliminate_join_by_fk=1;

EXPLAIN SELECT c.id, c.parent_id, c.description
FROM   child c
       JOIN parent p ON c.parent_id = p.id
       ORDER BY c.id;

SELECT c.id, c.parent_id, c.description
FROM   child c
       JOIN parent p ON c.parent_id = p.id
       ORDER BY c.id;

alter table child add constraint my_name_1 foreign key (id) references parent(id);
show create table child;

alter table child drop foreign key fk1;
alter table child drop foreign key my_name_1;
show create table child;