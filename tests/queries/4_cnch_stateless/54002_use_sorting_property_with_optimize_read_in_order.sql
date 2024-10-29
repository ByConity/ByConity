drop table if exists 54002_table;

create table 54002_table 
(
    a Int32,
    b String
) ENGINE=CnchMergeTree() order by a;

insert into 54002_table values (2, '2'), (3, '3'), (1, '1'), (199, '199'), (0, '0');

select * from 54002_table order by a desc settings optimize_read_in_order=0;
