SELECT log_with_base(10, 100);
SELECT log_with_base(10, -100);
SELECT log_with_base(0, 100);
SELECT log_with_base(10, 0);
SELECT log_with_base(1, 100);
SELECT log_with_base(5, 5);
SELECT log_with_base(0.5, 100);
SELECT log_with_base(10, 0.5);
SELECT log_with_base(10, 1000000000);
SELECT log_with_base(10, 0.000000001);
SELECT log_with_base(10, NULL);
SELECT log_with_base(NULL, 100);
SELECT log_with_base(NULL, NULL);

select log_with_base(2, 0);
select log_with_base(2, 1);
select log_with_base(2, 2);
select log_with_base(2, 4);
select sum(abs(log_with_base(2, exp2(x)) - x) < 1.0e-9) / count() from system.one array join range(1000) as x;