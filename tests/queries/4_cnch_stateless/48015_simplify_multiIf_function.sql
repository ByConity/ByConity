set enable_optimizer=1;

DROP TABLE IF EXISTS multiIf;
CREATE TABLE multiIf
(
    a Int32,
    b UInt8
) ENGINE = CnchMergeTree()
ORDER BY a;

insert into multiIf values(5, 6)(6, 7)(7, 8)(8, 9)(10, 11);

SELECT *
FROM multiIf
WHERE multiIf(
              (a = 6), a + a,
              (6 = 6), b + a,
              (1 = 3), b + b, a*a) > 10
order by a;

SELECT *
FROM multiIf
WHERE case
          when (1 = 6) then a+a
          when (2 = 6) then a+b
          when (a = 3) then b+b
          else a * a
    end  > 10
order by a;


explain SELECT *
FROM multiIf
WHERE multiIf(
              (a = 6), a + a,
              (6 = 6), b + a,
              (1 = 3), b + b, a*a) > 10
order by a;

explain SELECT *
FROM multiIf
WHERE case
          when (1 = 6) then a+a
          when (2 = 6) then a+b
          when (a = 3) then b+b
          else a * a
          end  > 10
order by a;

SELECT *
FROM multiIf
WHERE multiIf(
              (1 = 2), a + 1,
              (2 = 2), a + 2,
              (3 = 3), a + 3, 0) < 10
order by a;

SELECT *
FROM multiIf
WHERE multiIf(
              (1 = 2), a + 1,
              (2 = 1), a + 2,
              (3 = 1), a + 3, 0) < 10
order by a;

explain SELECT *
FROM multiIf
WHERE multiIf(
              (1 = 2), a + 1,
              (2 = 2), a + 2,
              (3 = 3), a + 3, 0) < 10
order by a;

explain SELECT *
FROM multiIf
WHERE multiIf(
              (1 = 2), a + 1,
              (2 = 1), a + 2,
              (3 = 1), a + 3, 0) < 10
order by a;

SELECT *
FROM multiIf
WHERE case
          when (1 = 3) then a+3
          when (2 = 2) then a+4
          when (3 = 3) then a+5
          else 0 end  < 10
order by a;

SELECT *
FROM multiIf
WHERE case
          when (1 = 3) then a+3
          when (2 = 1) then a+4
          when (3 = 1) then a+5
          else 0 end  < 10
order by a;

explain SELECT *
FROM multiIf
WHERE case
          when (1 = 3) then a+3
          when (2 = 2) then a+4
          when (3 = 3) then a+5
          else 0 end  < 10
order by a;

explain SELECT *
FROM multiIf
WHERE case
          when (1 = 3) then a+3
          when (2 = 1) then a+4
          when (3 = 1) then a+5
          else 0 end  < 10
order by a;

DROP TABLE IF EXISTS multiIf;
DROP TABLE IF EXISTS multiIf_local;
