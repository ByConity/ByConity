select datetime '2001-09-28';
select (datetime '2001-09-28' + interval 7 day) x, toTypeName(x);
select (datetime '2001-10-01' - interval 7 day) x, toTypeName(x);
select (datetime '2001-09-28' + 7) x, toTypeName(x);
select (datetime '2001-10-01' - 7) x, toTypeName(x);
select (datetime '2001-09-28' + interval 1 hour) x, toTypeName(x);
select (datetime '2001-09-28' - interval 1 hour) x, toTypeName(x);
select (datetime '2001-10-01' - datetime '2001-09-28') x, toTypeName(x);
select datetime '2001-09-28 01:00:00' + interval 23 hour;
select datetime '2001-09-28 23:00:00' - interval 23 hour;

-- TODO: return interval
select (datetime '2001-09-29 03:00:00' - datetime '2001-09-27 12:00:00') x, toTypeName(x); -- interval '1 day 15:00:00'