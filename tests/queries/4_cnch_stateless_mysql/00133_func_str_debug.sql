--source include/have_debug_sync.inc

--echo # Bug#20554017 CONCAT MAY INCORRECTLY COPY OVERLAPPING STRINGS

-- SET @old_debug= @@session.debug;
-- SET session debug='d,force_fake_uuid';

select concat('111','11111111111111111111111111',
          substring_index(lower(hex(toUUID('00000000-80e7-46f8-0000-9d773a2fd319'))),0,1.111111e+308));

select concat_ws(',','111','11111111111111111111111111',
             substring_index(lower(hex(toUUID('00000000-80e7-46f8-0000-9d773a2fd319'))),0,1.111111e+308));

-- SET session debug= @old_debug;
