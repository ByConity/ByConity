select 1 where 1 and rand() format Null;
select 1 where toUInt8(rand()) and (select 1234) format Null;
