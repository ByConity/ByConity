--
-- BUG#33509: Server crashes with number of recursive subqueries=61
--  (the query may or may not fail with an error so we're using it with SP 
--  
create table t1 (a int not null);

select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( 
select a from t1 where a in ( select a from t1) 
)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));

drop table t1;

