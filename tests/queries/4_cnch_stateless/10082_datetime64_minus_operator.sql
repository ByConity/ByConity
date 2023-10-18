select '2020-01-07 05:05:00'::DateTime64 - '2020-01-07 04:05:00'::DateTime64;
select '2020-01-07 05:05:00'::DateTime64(3, 'UTC') - '2020-01-07 04:05:00'::DateTime64(3, 'Asia/Shanghai');
select '2020-01-07 05:05:00'::DateTime64(3, 'UTC') - '2020-01-07 04:05:00'::DateTime64(4, 'Asia/Shanghai');
select '1920-01-07 05:05:00'::DateTime64(3, 'UTC') - '1920-01-07 04:05:00'::DateTime64(3, 'Asia/Shanghai');
select '1920-01-07 05:05:00'::DateTime64(3, 'UTC') - '1920-01-07 04:05:00'::DateTime64(4, 'Asia/Shanghai');
select '2020-01-07 05:05:00'::DateTime64(3, 'UTC') - '1920-01-07 05:05:00'::DateTime64(3, 'Asia/Shanghai');
select '2261-01-07 05:05:00'::DateTime64(3, 'UTC') - '2261-01-07 04:05:00'::DateTime64(3, 'Asia/Shanghai');
