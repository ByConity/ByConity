drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

CREATE TABLE t1
(
    `u1` Int64 NOT NULL,
    `v1` Int64 NULL,
    CONSTRAINT un1 UNIQUE (u1)
)
ENGINE = CnchMergeTree
ORDER BY u1;

CREATE TABLE t2
(
    `u2` Int64 NOT NULL,
    `v2` Int64 NULL,
    CONSTRAINT un1 UNIQUE (u2)
)
ENGINE = CnchMergeTree
ORDER BY u2;

CREATE TABLE t3
(
    `f1` Int64 NULL,
    `f2` Int64 NULL,
    `v3` Int64 NOT NULL,
    CONSTRAINT fk1 FOREIGN KEY (f1) REFERENCES t1(u1),
    CONSTRAINT fk2 FOREIGN KEY (f2) REFERENCES t2(u2)
)
ENGINE = CnchMergeTree
ORDER BY v3;

insert into t1 values (1,1);
insert into t2 values (2,2);
insert into t3 values (1,2,3);

set enable_optimizer=1;
set enable_eliminate_join_by_fk=1;
set enable_eliminate_complicated_pk_fk_join=1;

with cte as(select u1 from t1, t2, t3 where t3.f1 = t1.u1 and t3.f2 = t2.u2 group by u1) select toTypeName(u1) from cte group by u1
settings cte_mode='ENFORCED', enable_simplify_expression_rewrite=0;