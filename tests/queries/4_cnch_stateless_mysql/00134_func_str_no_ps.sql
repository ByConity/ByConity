-- Some of the queries give different set of warnings with --ps-protocol
-- if (`SELECT $PS_PROTOCOL + $CURSOR_PROTOCOL > 0`)
-- {
--    --skip Need normal protocol
-- }

--echo #
--echo # Bug#20315088 LCASE/LTRIM, SOURCE AND DESTINATION OVERLAP IN MEMCPY
--echo #

select lcase(ltrim(from_unixtime(0,' %T ')));
select '' <= lcase(trim(leading 1 from 12222)) not between '1' and '2';
select decode(substring(sha1('1'),'11'),25);
select encode(mid(sysdate(),'5',1),'11');
select upper(substring(1.111111111111111111 from '2n'));
select nullif(1,'-' between lcase(right(11111111,' 7,]' ))and '1');
select upper(right(198039009115594390000000000000000000000.000000,35));
select concat('111','11111111111111111111111111',
          substring_index(uuid(),0,1.111111e+308));
select replace(ltrim(from_unixtime(0,' %T ')), '0', '1');
select insert(ltrim(from_unixtime(0,' %T ')), 2, 1, 'hi');

-- set @old_collation_connection=@@collation_connection;
-- set collation_connection='utf8_general_ci';
select replace(ltrim(from_unixtime(0,' %T ')), '0', '1');
-- set collation_connection=@old_collation_connection;
