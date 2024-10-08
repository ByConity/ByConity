create table if not exists 03035_table (a int, b Date) engine = CnchMergeTree order by a partition by b;

insert into 03035_table values (1, '2024-08-23');

select count() from system.cnch_parts_columns where database = currentDatabase(0) and table = '03035_table' and column = 'a' and column_data_bytes_on_disk > 0 and column_data_compressed_bytes > 0 and column_data_uncompressed_bytes > 0;
select count() from system.cnch_parts_columns where database = currentDatabase(0) and table = '03035_table' and column = 'b' and column_data_bytes_on_disk > 0 and column_data_compressed_bytes > 0 and column_data_uncompressed_bytes > 0;

drop table if exists 03035_table;