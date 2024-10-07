DROP TABLE IF EXISTS store_sales;
DROP TABLE IF EXISTS date_dim;
CREATE TABLE store_sales
(
    ss_sold_date_sk Nullable(Int64),
    ss_sold_time_sk Nullable(Int64),
    ss_item_sk Int64 NOT NULL,
    ss_customer_sk Nullable(Int64),
    ss_cdemo_sk Nullable(Int64),
    ss_hdemo_sk Nullable(Int64),
    ss_addr_sk Nullable(Int64),
    ss_store_sk Nullable(Int64),
    ss_promo_sk Nullable(Int64),
    ss_ticket_number Nullable(Int64),
    ss_quantity Nullable(Int64),
    ss_wholesale_cost Nullable(Float64),
    ss_list_price Nullable(Float64),
    ss_sales_price Nullable(Float64),
    ss_ext_discount_amt Nullable(Float64),
    ss_ext_sales_price Nullable(Float64),
    ss_ext_wholesale_cost Nullable(Float64),
    ss_ext_list_price Nullable(Float64),
    ss_ext_tax Nullable(Float64),
    ss_coupon_amt Nullable(Float64),
    ss_net_paid Nullable(Float64),
    ss_net_paid_inc_tax Nullable(Float64),
    ss_net_profit Nullable(Float64)
) ENGINE = CnchMergeTree() order by ss_item_sk;

create table date_dim (
  d_date_sk Int64 NOT NULL,
  d_date_id Nullable(String),
  d_date Nullable(date),
  d_month_seq Nullable(Int64),
  d_week_seq Nullable(Int64),
  d_quarter_seq Nullable(Int64),
  d_year Nullable(Int64),
  d_dow Nullable(Int64),
  d_moy Nullable(Int64),
  d_dom Nullable(Int64),
  d_qoy Nullable(Int64),
  d_fy_year Nullable(Int64),
  d_fy_quarter_seq Nullable(Int64),
  d_fy_week_seq Nullable(Int64),
  d_day_name Nullable(String),
  d_quarter_name Nullable(String),
  d_holiday Nullable(String),
  d_weekend Nullable(String),
  d_following_holiday Nullable(String),
  d_first_dom Nullable(Int64),
  d_last_dom Nullable(Int64),
  d_same_day_ly Nullable(Int64),
  d_same_day_lq Nullable(Int64),
  d_current_day Nullable(String),
  d_current_week Nullable(String),
  d_current_month Nullable(String),
  d_current_quarter Nullable(String),
  d_current_year Nullable(String)
) ENGINE = CnchMergeTree() 
ORDER BY d_date_sk;

set dialect_type = 'ANSI';
set enable_expand_distinct_optimization=0;
set enable_eager_aggregation=1;
set agg_push_down_threshold=0;

select 'simple case, all aggFuncs are from the same side';
explain select d_qoy, d_year,sum(ss_ext_sales_price), any(ss_sold_time_sk), countDistinct(ss_customer_sk) as store_sales
 from store_sales,date_dim
 where ss_sold_date_sk = d_date_sk
 group by d_qoy, d_year;


select 'can not pushdown, not all aggFuncs are from the same side';
explain select d_qoy, d_year,sum(ss_ext_sales_price), any(ss_sold_time_sk), countDistinct(d_current_day) as store_sales
 from store_sales,date_dim
 where ss_sold_date_sk = d_date_sk
 group by d_qoy, d_year;


select 'projection before agg, only push agg';
explain with wscs as
 (select
    ss_sold_date_sk sold_date_sk,
    ss_ext_sales_price sales_price
    from store_sales)
select d_week_seq,
    sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
    sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
    sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
    sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
    sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
    sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
    sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
from wscs
     ,date_dim
where d_date_sk = sold_date_sk
group by d_week_seq;


select 'projection before agg, push agg with projection';
explain select d_date_id
       ,d_year dyear
       ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
       ,'s' sale_type
from store_sales
     ,date_dim
where ss_sold_date_sk = d_date_sk
group by d_date_id
        ,d_year;