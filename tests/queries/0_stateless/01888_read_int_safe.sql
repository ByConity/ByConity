select toInt64('--1'); 
select toInt64('+-1'); 
select toInt64('++1'); 
select toInt64('++'); 
select toInt64('+'); 
select toInt64('1+1'); 
select toInt64('1-1'); 
-- select toInt64(''); -- { serverError 32 }
select toInt64('1');
select toInt64('-1');
