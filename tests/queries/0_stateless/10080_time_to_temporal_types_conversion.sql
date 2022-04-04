select CAST(CAST('05:05:05.5', 'Time(2)'), 'DateTime64(2)'); -- { serverError 70 }
select CAST(CAST('05:05:05.5', 'Time(2)'), 'DateTime'); -- { serverError 70 }
select CAST(CAST('05:05:05.5', 'Time(2)'), 'Date'); -- { serverError 70 }