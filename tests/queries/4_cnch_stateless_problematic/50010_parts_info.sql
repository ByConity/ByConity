select 'Test drop range.';

DROP TABLE IF EXISTS pi;

CREATE TABLE pi
(
    `name` String,
    `country` String,
    `asdf` Int
)
ENGINE = CnchMergeTree
PARTITION BY name
ORDER BY name;

SYSTEM START MERGES pi;

insert into pi values ('a', 'a', 1);
insert into pi values ('a', 'a', 2);

-- Parts' numbers and size should match.
select equals( 
    (
        select count(), sum(bytes_on_disk) from system.cnch_parts
        where database = currentDatabase(1) and table = 'pi' and part_type <= 2
    ), 
    (
        select total_parts_number, total_parts_size from system.cnch_parts_info
        where database = currentDatabase(1) and table = 'pi'
    )
);

-- Row numbers should match.
select total_rows_count from system.cnch_parts_info where database = currentDatabase(1) and table = 'pi';

-- Action: mutation.
ALTER TABLE pi DROP COLUMN asdf;

-- Make sure that mutation is finished
SYSTEM START MERGES pi;
SYSTEM STOP MERGES pi;
OPTIMIZE TABLE pi SETTINGS mutations_sync = 1;

-- Parts' numbers and size should match.
select equals( 
    (
        select count(), sum(bytes_on_disk) from system.cnch_parts
        where database = currentDatabase(1) and table = 'pi' and part_type <= 2
    ), 
    (
        select total_parts_number, total_parts_size from system.cnch_parts_info 
        where database = currentDatabase(1) and table = 'pi'
    )
);

-- Row numbers should match.
select total_rows_count from system.cnch_parts_info where database = currentDatabase(1) and table = 'pi';

-- Action: mutation.
ALTER TABLE pi DROP COLUMN country;

OPTIMIZE TABLE pi SETTINGS mutations_sync = 1;


-- Parts' numbers and size should match.
select equals( 
    (
        select count(), sum(bytes_on_disk) from system.cnch_parts
        where database = currentDatabase(1) and table = 'pi' and part_type <= 2
    ), 
    (
        select total_parts_number, total_parts_size from system.cnch_parts_info 
        where database = currentDatabase(1) and table = 'pi'
    )
);

-- Row numbers should match.
select total_rows_count from system.cnch_parts_info where database = currentDatabase(1) and table = 'pi';

TRUNCATE TABLE pi;

select total_parts_number, total_rows_count, total_parts_size from
system.cnch_parts_info where database = currentDatabase(1) and table = 'pi';

DROP TABLE IF EXISTS pi;

---------

select 'Test droped part.';

DROP TABLE IF EXISTS pi;

CREATE TABLE pi
(
    `name` String,
    `country` String,
    `asdf` Int
)
ENGINE = CnchMergeTree
PARTITION BY name
ORDER BY name;

SYSTEM START MERGES pi;

insert into pi values ('a', 'a', 1);


-- Parts' numbers and size should match.
select equals( 
    (
        select count(), sum(bytes_on_disk) from system.cnch_parts
        where database = currentDatabase(1) and table = 'pi' and part_type <= 2
    ), 
    (
        select total_parts_number, total_parts_size from system.cnch_parts_info 
        where database = currentDatabase(1) and table = 'pi'
    )
);

-- Row numbers should match.
select total_rows_count from system.cnch_parts_info where database = currentDatabase(1) and table = 'pi';

-- Action: mutation.
ALTER TABLE pi DROP COLUMN asdf;

-- Make sure that mutation is finished
SYSTEM START MERGES pi;
SYSTEM STOP MERGES pi;
OPTIMIZE TABLE pi SETTINGS mutations_sync = 1;

-- Parts' numbers and size should match.
select equals( 
    (
        select count(), sum(bytes_on_disk) from system.cnch_parts
        where database = currentDatabase(1) and table = 'pi' and part_type <= 2
    ), 
    (
        select total_parts_number, total_parts_size from system.cnch_parts_info 
        where database = currentDatabase(1) and table = 'pi'
    )
);

-- Row numbers should match.
select total_rows_count from system.cnch_parts_info where database = currentDatabase(1) and table = 'pi';

-- Action: mutation.
ALTER TABLE pi DROP COLUMN country;

OPTIMIZE TABLE pi SETTINGS mutations_sync = 1;


-- Parts' numbers and size should match.
select equals( 
    (
        select count(), sum(bytes_on_disk) from system.cnch_parts
        where database = currentDatabase(1) and table = 'pi' and part_type <= 2
    ), 
    (
        select total_parts_number, total_parts_size from system.cnch_parts_info 
        where database = currentDatabase(1) and table = 'pi'
    )
);

-- Row numbers should match.
select total_rows_count from system.cnch_parts_info where database = currentDatabase(1) and table = 'pi';

TRUNCATE TABLE pi;

select total_parts_number, total_rows_count, total_parts_size from
system.cnch_parts_info where database = currentDatabase(1) and table = 'pi';

DROP TABLE IF EXISTS pi;
