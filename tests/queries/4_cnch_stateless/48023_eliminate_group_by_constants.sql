drop table if exists store_sales;
drop table if exists store_sales_local;
drop table if exists web_sales;
drop table if exists web_sales_local;
drop table if exists catalog_sales;
drop table if exists catalog_sales_local;
drop table if exists item;
drop table if exists item_local;

create table store_sales (
  ss_item_sk Int64,
  ss_store_sk Nullable(Int64)
) ENGINE = CnchMergeTree() 
order by ss_item_sk;

create table web_sales (
  ws_item_sk Int64,
  ws_ship_customer_sk Nullable(Int64)
) ENGINE = CnchMergeTree() 
order by ws_item_sk;

create table catalog_sales (
  cs_ship_addr_sk Nullable(Int64),
  cs_item_sk Int64
) ENGINE = CnchMergeTree()
order by cs_item_sk;

create table item (
  i_item_sk Int64,
  i_item_id Nullable(String),
  i_category Nullable(String)
) ENGINE = CnchMergeTree() 
ORDER BY i_item_sk;


set dialect_type='ANSI';
set enable_optimizer=1;
set enable_group_by_keys_pruning=1;
set enum_replicate_no_stats=0;

explain select channel, nn, i_category, COUNT(*) sales_cnt FROM (
    SELECT 'store' as channel, ss_store_sk nn, i_category
    FROM store_sales, item
    WHERE ss_store_sk IS NULL
    AND ss_item_sk=i_item_sk
    UNION ALL
    SELECT 'web' as channel, ws_ship_customer_sk nn, i_category
    FROM web_sales, item
    WHERE ws_ship_customer_sk IS NULL
    AND ws_item_sk=i_item_sk
    UNION ALL
    SELECT 'catalog' as channel, cs_ship_addr_sk nn, i_category
    FROM catalog_sales, item
    WHERE cs_ship_addr_sk IS NULL
    AND cs_item_sk=i_item_sk) foo
    GROUP BY channel, nn, i_category
    ORDER BY channel, nn, i_category
    LIMIT 100;
