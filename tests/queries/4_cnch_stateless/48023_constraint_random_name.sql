set dialect_type='MYSQL';

drop table if exists child_random_constraint;
drop table if exists child_random_constraint1;
drop table if exists parent_random_constraint;

CREATE TABLE parent_random_constraint (
  id          UInt8,
  description String
) engine = CnchMergeTree() order by id;

CREATE TABLE child_random_constraint (
  id          UInt8,
  parent_id   Nullable(UInt8),
  description String,
  CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent_random_constraint(id),
  CONSTRAINT UNIQUE (id),
  CONSTRAINT CHECK id > 2
) engine = CnchMergeTree() order by id;

CREATE TABLE child_random_constraint1 (
  id          UInt8,
  parent_id   Nullable(UInt8),
  description String,
  CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent_random_constraint(id),
  FOREIGN KEY (description) REFERENCES parent_random_constraint(description)
) engine = CnchMergeTree() order by id;

SHOW CREATE TABLE child_random_constraint1;

DROP TABLE child_random_constraint1;
DROP TABLE child_random_constraint;
DROP TABLE parent_random_constraint;
