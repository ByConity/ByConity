
CREATE TABLE customer_address (
  `ca_address_sk` Int32,
  `ca_address_id` String,
  `ca_street_number` LowCardinality(Nullable(String)),
  `ca_street_name` LowCardinality(Nullable(String)),
  `ca_street_type` LowCardinality(Nullable(String)),
  `ca_suite_number` LowCardinality(Nullable(String)),
  `ca_city` LowCardinality(Nullable(String)),
  `ca_county` LowCardinality(Nullable(String)),
  `ca_state` FixedString(2),
  `ca_zip` LowCardinality(Nullable(String)),
  `ca_country` LowCardinality(Nullable(String)),
  `ca_gmt_offset` Nullable(Float32),
  `ca_location_type` LowCardinality(Nullable(String)),
  constraint un1 unique(ca_address_sk),
  constraint un2 unique(ca_address_id)
) ENGINE = CnchMergeTree() 
ORDER BY ca_address_sk
CLUSTER BY (ca_address_sk) INTO 3 BUCKETS;

create table customer_demographics (
  `cd_demo_sk` Int32,
  `cd_gender` FixedString(1),
  `cd_marital_status` FixedString(1),
  `cd_education_status` LowCardinality(String),
  `cd_purchase_estimate` Int16,
  `cd_credit_rating` LowCardinality(String),
  `cd_dep_count` Int8,
  `cd_dep_employed_count` Int8,
  `cd_dep_college_count` Int8
) ENGINE = CnchMergeTree() 
ORDER BY cd_demo_sk
CLUSTER BY(cd_demo_sk) INTO 3 BUCKETS;

create table date_dim (
  `d_date_sk` Int32,
  `d_date_id` String,
  `d_date` Date32,
  `d_month_seq` Int16,
  `d_week_seq` Int16,
  `d_quarter_seq` Int16,
  `d_year` Int16,
  `d_dow` Int8,
  `d_moy` Int8,
  `d_dom` Int8,
  `d_qoy` Int8,
  `d_fy_year` Int16,
  `d_fy_quarter_seq` Int16,
  `d_fy_week_seq` Int16,
  `d_day_name` LowCardinality(String),
  `d_quarter_name` LowCardinality(String),
  `d_holiday` FixedString(1),
  `d_weekend` FixedString(1),
  `d_following_holiday` FixedString(1),
  `d_first_dom` Int32,
  `d_last_dom` Int32,
  `d_same_day_ly` Int32,
  `d_same_day_lq` Int32,
  `d_current_day` FixedString(1),
  `d_current_week` FixedString(1),
  `d_current_month` FixedString(1),
  `d_current_quarter` FixedString(1),
  `d_current_year` FixedString(1),
  constraint un1 unique(d_date_sk),
  constraint un2 unique(d_date_id)
) ENGINE = CnchMergeTree() 
ORDER BY d_date_sk
CLUSTER BY (d_date_sk) INTO 3 BUCKETS;

create table income_band (
  `ib_income_band_sk` Int8,
  `ib_lower_bound` Int32,
  `ib_upper_bound` Int32,
  constraint un1 unique(ib_income_band_sk)
) ENGINE = CnchMergeTree() 
ORDER BY ib_income_band_sk
CLUSTER BY (ib_income_band_sk) INTO 3 BUCKETS;

create table household_demographics (
  `hd_demo_sk` Int16,
  `hd_income_band_sk` Int8,
  `hd_buy_potential` LowCardinality(String),
  `hd_dep_count` Int8,
  `hd_vehicle_count` Int8,
  constraint fk1 foreign key(hd_income_band_sk) references income_band(ib_income_band_sk),
  constraint un1 unique(hd_demo_sk)
) ENGINE = CnchMergeTree() 
ORDER BY hd_demo_sk
CLUSTER BY (hd_demo_sk) INTO 3 BUCKETS;


CREATE TABLE customer (
  `c_customer_sk` Int32,
  `c_customer_id` FixedString(16),
  `c_current_cdemo_sk` Nullable(Int32),
  `c_current_hdemo_sk` Nullable(Int16),
  `c_current_addr_sk` Int32,
  `c_first_shipto_date_sk` Nullable(Int32),
  `c_first_sales_date_sk` Nullable(Int32),
  `c_salutation` LowCardinality(Nullable(String)),
  `c_first_name` LowCardinality(Nullable(FixedString(11))),
  `c_last_name` LowCardinality(Nullable(FixedString(13))),
  `c_preferred_cust_flag` FixedString(1),
  `c_birth_day` Nullable(Int8),
  `c_birth_month` Nullable(Int8),
  `c_birth_year` Nullable(Int16),
  `c_birth_country` LowCardinality(Nullable(String)),
  `c_login` LowCardinality(Nullable(String)),
  `c_email_address` Nullable(String),
  `c_last_review_date_sk` LowCardinality(Nullable(String)),
  constraint fk1 foreign key(c_current_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk2 foreign key(c_current_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk3 foreign key(c_current_addr_sk) references customer_address(ca_address_sk),
  constraint fk4 foreign key(c_first_shipto_date_sk) references date_dim(d_date_sk),
  constraint fk5 foreign key(c_first_sales_date_sk) references date_dim(d_date_sk),
  constraint fk6 foreign key(c_last_review_date_sk) references date_dim(d_date_sk),
  constraint un1 unique(c_customer_sk),
  constraint un2 unique(c_customer_id)
) ENGINE = CnchMergeTree() 
ORDER BY c_customer_sk
CLUSTER BY (c_customer_sk) INTO 3 BUCKETS;

create table web_page (
  `wp_web_page_sk` Int16,
  `wp_web_page_id` String,
  `wp_rec_start_date` Nullable(Date),
  `wp_rec_end_date` Nullable(Date),
  `wp_creation_date_sk` Nullable(Int32),
  `wp_access_date_sk` Nullable(Int32),
  `wp_autogen_flag` FixedString(1),
  `wp_customer_sk` Nullable(Int32),
  `wp_url` LowCardinality(Nullable(String)),
  `wp_type` LowCardinality(Nullable(String)),
  `wp_char_count` Nullable(Int16),
  `wp_link_count` Nullable(Int8),
  `wp_image_count` Nullable(Int8),
  `wp_max_ad_count` Nullable(Int8),
  constraint fk1 foreign key(wp_creation_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(wp_access_date_sk) references date_dim(d_date_sk),
  constraint fk3 foreign key(wp_customer_sk) references customer(c_customer_sk),
  constraint un1 unique(wp_web_page_sk)
) ENGINE = CnchMergeTree() 
ORDER BY wp_web_page_sk
CLUSTER BY (wp_web_page_sk) INTO 3 BUCKETS;

create table item (
  `i_item_sk` Int32,
  `i_item_id` String,
  `i_rec_start_date` Nullable(Date),
  `i_rec_end_date` Nullable(Date),
  `i_item_desc` LowCardinality(Nullable(String)),
  `i_current_price` Nullable(Float32),
  `i_wholesale_cost` Nullable(Float32),
  `i_brand_id` Nullable(Int32),
  `i_brand` LowCardinality(Nullable(String)),
  `i_class_id` Nullable(Int8),
  `i_class` LowCardinality(Nullable(String)),
  `i_category_id` Nullable(Int8),
  `i_category` LowCardinality(Nullable(String)),
  `i_manufact_id` Nullable(Int16),
  `i_manufact` LowCardinality(Nullable(String)),
  `i_size` LowCardinality(Nullable(String)),
  `i_formulation` Nullable(String),
  `i_color` LowCardinality(Nullable(String)),
  `i_units` LowCardinality(Nullable(String)),
  `i_container` LowCardinality(Nullable(String)),
  `i_manager_id` Nullable(Int8),
  `i_product_name` LowCardinality(Nullable(String)),
  constraint un1 unique(i_item_sk)
) ENGINE = CnchMergeTree() 
ORDER BY i_item_sk
CLUSTER BY (i_item_sk) INTO 3 BUCKETS;

create table promotion (
  `p_promo_sk` Int16,
  `p_promo_id` String,
  `p_start_date_sk` Nullable(Int32),
  `p_end_date_sk` Nullable(Int32),
  `p_item_sk` Nullable(Int32),
  `p_cost` Nullable(Float64),
  `p_response_target` Nullable(Int8),
  `p_promo_name` LowCardinality(Nullable(String)),
  `p_channel_dmail` FixedString(1),
  `p_channel_email` FixedString(1),
  `p_channel_catalog` FixedString(1),
  `p_channel_tv` FixedString(1),
  `p_channel_radio` FixedString(1),
  `p_channel_press` FixedString(1),
  `p_channel_event` FixedString(1),
  `p_channel_demo` FixedString(1),
  `p_channel_details` Nullable(String),
  `p_purpose` LowCardinality(Nullable(String)),
  `p_discount_active` FixedString(1),
  constraint fk1 foreign key(p_start_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(p_end_date_sk) references date_dim(d_date_sk),
  constraint fk3 foreign key(p_item_sk) references item(i_item_sk),
  constraint un1 unique(p_promo_sk),
  constraint un2 unique(p_promo_id)
) ENGINE = CnchMergeTree() 
ORDER BY p_promo_sk
CLUSTER BY (p_promo_sk) INTO 3 BUCKETS;

create table reason (
  `r_reason_sk` Int8,
  `r_reason_id` String,
  `r_reason_desc` LowCardinality(String),
  constraint un1 unique(r_reason_sk),
  constraint un2 unique(r_reason_id)
) ENGINE = CnchMergeTree() 
ORDER BY r_reason_sk
CLUSTER BY (r_reason_sk) INTO 3 BUCKETS;

create table ship_mode (
  `sm_ship_mode_sk` Int8,
  `sm_ship_mode_id` String,
  `sm_type` LowCardinality(String),
  `sm_code` LowCardinality(String),
  `sm_carrier` LowCardinality(String),
  `sm_contract` String,
  constraint un1 unique(sm_ship_mode_sk),
  constraint un2 unique(sm_ship_mode_id)
) ENGINE = CnchMergeTree() 
ORDER BY sm_ship_mode_sk
CLUSTER BY (sm_ship_mode_sk) INTO 3 BUCKETS;

create table store (
  `s_store_sk` Int16,
  `s_store_id` LowCardinality(String),
  `s_rec_start_date` Nullable(Date),
  `s_rec_end_date` Nullable(Date),
  `s_closed_date_sk` Nullable(Int32),
  `s_store_name` LowCardinality(Nullable(String)),
  `s_number_employees` Nullable(Int16),
  `s_floor_space` Nullable(Int32),
  `s_hours` LowCardinality(Nullable(String)),
  `s_manager` Nullable(String),
  `s_market_id` Nullable(Int8),
  `s_geography_class` LowCardinality(Nullable(String)),
  `s_market_desc` Nullable(String),
  `s_market_manager` Nullable(String),
  `s_division_id` Nullable(Int8),
  `s_division_name` LowCardinality(Nullable(String)),
  `s_company_id` Nullable(Int8),
  `s_company_name` LowCardinality(Nullable(String)),
  `s_street_number` LowCardinality(Nullable(String)),
  `s_street_name` LowCardinality(Nullable(String)),
  `s_street_type` LowCardinality(Nullable(String)),
  `s_suite_number` LowCardinality(Nullable(String)),
  `s_city` LowCardinality(Nullable(String)),
  `s_county` LowCardinality(Nullable(String)),
  `s_state` FixedString(2),
  `s_zip` LowCardinality(Nullable(String)),
  `s_country` LowCardinality(Nullable(String)),
  `s_gmt_offset` Nullable(Float32),
  `s_tax_precentage` Nullable(Float32),
  constraint fk1 foreign key(s_closed_date_sk) references date_dim(d_date_sk),
  constraint un1 unique(s_store_sk)
) ENGINE = CnchMergeTree() 
ORDER BY s_store_sk
CLUSTER BY (s_store_sk) INTO 3 BUCKETS;

create table time_dim (
  `t_time_sk` Int32,
  `t_time_id` String,
  `t_time` Int32,
  `t_hour` Int8,
  `t_minute` Int8,
  `t_second` Int8,
  `t_am_pm` FixedString(2),
  `t_shift` LowCardinality(String),
  `t_sub_shift` LowCardinality(String),
  `t_meal_time` LowCardinality(Nullable(String)),
  constraint un1 unique(t_time_sk),
  constraint un2 unique(t_time_id)
) ENGINE = CnchMergeTree() 
ORDER BY t_time_sk
CLUSTER BY (t_time_sk) INTO 3 BUCKETS;

create table warehouse (
  `w_warehouse_sk` Int8,
  `w_warehouse_id` String,
  `w_warehouse_name` LowCardinality(Nullable(String)),
  `w_warehouse_sq_ft` Nullable(Int32),
  `w_street_number` Nullable(String),
  `w_street_name` Nullable(String),
  `w_street_type` Nullable(String),
  `w_suite_number` Nullable(String),
  `w_city` LowCardinality(String),
  `w_county` LowCardinality(String),
  `w_state` FixedString(2),
  `w_zip` String,
  `w_country` LowCardinality(String),
  `w_gmt_offset` Nullable(Float32),
  constraint un1 unique(w_warehouse_sk),
  constraint un2 unique(w_warehouse_id)
) ENGINE = CnchMergeTree() 
ORDER BY w_warehouse_sk
CLUSTER BY (w_warehouse_sk) INTO 3 BUCKETS;

create table web_site (
  `web_site_sk` Int8,
  `web_site_id` LowCardinality(String),
  `web_rec_start_date` Nullable(Date),
  `web_rec_end_date` Nullable(Date),
  `web_name` LowCardinality(Nullable(String)),
  `web_open_date_sk` Nullable(Int32),
  `web_close_date_sk` Nullable(Int32),
  `web_class` LowCardinality(Nullable(String)),
  `web_manager` Nullable(String),
  `web_mkt_id` Nullable(Int8),
  `web_mkt_class` Nullable(String),
  `web_mkt_desc` String,
  `web_market_manager` Nullable(String),
  `web_company_id` Int8,
  `web_company_name` LowCardinality(Nullable(String)),
  `web_street_number` Nullable(String),
  `web_street_name` Nullable(String),
  `web_street_type` String,
  `web_suite_number` String,
  `web_city` Nullable(String),
  `web_county` Nullable(String),
  `web_state` FixedString(2),
  `web_zip` String,
  `web_country` LowCardinality(Nullable(String)),
  `web_gmt_offset` Nullable(Float32),
  `web_tax_percentage` Nullable(Float32),
  constraint fk1 foreign key(web_open_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(web_close_date_sk) references date_dim(d_date_sk),
  constraint un1 unique(web_site_sk)
) ENGINE = CnchMergeTree() 
ORDER BY web_site_sk
CLUSTER BY (web_site_sk) INTO 3 BUCKETS;

create table call_center (
  `cc_call_center_sk` Int8,
  `cc_call_center_id` LowCardinality(String),
  `cc_rec_start_date` Date,
  `cc_rec_end_date` Nullable(Date),
  `cc_closed_date_sk` Nullable(Int32),
  `cc_open_date_sk` Int32,
  `cc_name` LowCardinality(String),
  `cc_class` LowCardinality(String),
  `cc_employees` Int32,
  `cc_sq_ft` Int32,
  `cc_hours` LowCardinality(String),
  `cc_manager` LowCardinality(String),
  `cc_mkt_id` Int8,
  `cc_mkt_class` String,
  `cc_mkt_desc` String,
  `cc_market_manager` String,
  `cc_division` Int8,
  `cc_division_name` LowCardinality(String),
  `cc_company` Int8,
  `cc_company_name` LowCardinality(String),
  `cc_street_number` String,
  `cc_street_name` String,
  `cc_street_type` String,
  `cc_suite_number` String,
  `cc_city` Nullable(String),
  `cc_county` LowCardinality(String),
  `cc_state` FixedString(2),
  `cc_zip` String,
  `cc_country` LowCardinality(String),
  `cc_gmt_offset` Float32,
  `cc_tax_percentage` Float32,
  constraint fk1 foreign key(cc_closed_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(cc_open_date_sk) references date_dim(d_date_sk),
  constraint un1 unique(cc_call_center_sk)
) ENGINE = CnchMergeTree() 
ORDER BY cc_call_center_sk
CLUSTER BY (cc_call_center_sk) INTO 3 BUCKETS;


create table catalog_page (
  `cp_catalog_page_sk` Int16,
  `cp_catalog_page_id` String,
  `cp_start_date_sk` Nullable(Int32),
  `cp_end_date_sk` Nullable(Int32),
  `cp_department` LowCardinality(Nullable(String)),
  `cp_catalog_number` Nullable(Int8),
  `cp_catalog_page_number` Nullable(Int16),
  `cp_description` Nullable(String),
  `cp_type` LowCardinality(Nullable(String)),
  constraint fk1 foreign key(cp_start_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(cp_end_date_sk) references date_dim(d_date_sk),
  constraint un1 unique(cp_catalog_page_sk),
  constraint un2 unique(cp_catalog_page_id)
) ENGINE = CnchMergeTree() 
ORDER BY cp_catalog_page_sk
CLUSTER BY (cp_catalog_page_sk) INTO 3 BUCKETS;


create table catalog_returns (
  `cr_returned_date_sk` Int32,
  `cr_returned_time_sk` Int32,
  `cr_item_sk` Int32,
  `cr_refunded_customer_sk` Nullable(Int32),
  `cr_refunded_cdemo_sk` Nullable(Int32),
  `cr_refunded_hdemo_sk` Nullable(Int16),
  `cr_refunded_addr_sk` Nullable(Int32),
  `cr_returning_customer_sk` Nullable(Int32),
  `cr_returning_cdemo_sk` Nullable(Int32),
  `cr_returning_hdemo_sk` Nullable(Int16),
  `cr_returning_addr_sk` Nullable(Int32),
  `cr_call_center_sk` Nullable(Int8),
  `cr_catalog_page_sk` Nullable(Int16),
  `cr_ship_mode_sk` Nullable(Int8),
  `cr_warehouse_sk` Nullable(Int8),
  `cr_reason_sk` Nullable(Int8),
  `cr_order_number` Int64,
  `cr_return_quantity` Nullable(Int8),
  `cr_return_amount` Nullable(Float32),
  `cr_return_tax` Nullable(Float32),
  `cr_return_amt_inc_tax` Nullable(Float32),
  `cr_fee` Nullable(Float32),
  `cr_return_ship_cost` Nullable(Float32),
  `cr_refunded_cash` Nullable(Float32),
  `cr_reversed_charge` Nullable(Float32),
  `cr_store_credit` Nullable(Float32),
  `cr_net_loss` Nullable(Float32),
  constraint fk1 foreign key(cr_returned_time_sk) references time_dim(t_time_sk),
  constraint fk2 foreign key(cr_returned_date_sk) references date_dim(d_date_sk),
  constraint fk3 foreign key(cr_item_sk) references item(i_item_sk),
  constraint fk4 foreign key(cr_refunded_customer_sk) references customer(c_customer_sk),
  constraint fk5 foreign key(cr_refunded_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk6 foreign key(cr_refunded_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk7 foreign key(cr_refunded_addr_sk) references customer_address(ca_address_sk),
  constraint fk8 foreign key(cr_returning_customer_sk) references customer(c_customer_sk),
  constraint fk9 foreign key(cr_returning_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk10 foreign key(cr_returning_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk11 foreign key(cr_returning_addr_sk) references customer_address(ca_address_sk),
  constraint fk12 foreign key(cr_call_center_sk) references call_center(cc_call_center_sk),
  constraint fk13 foreign key(cr_catalog_page_sk) references catalog_page(cp_catalog_page_sk),
  constraint fk14 foreign key(cr_ship_mode_sk) references ship_mode(sm_ship_mode_sk),
  constraint fk15 foreign key(cr_warehouse_sk) references warehouse(w_warehouse_sk),
  constraint fk16 foreign key(cr_reason_sk) references reason(r_reason_sk),
  constraint un1 unique(cr_item_sk, cr_order_number)
) ENGINE = CnchMergeTree()
ORDER BY cr_item_sk
CLUSTER BY (cr_item_sk) INTO 3 BUCKETS;

create table catalog_sales (
  `cs_sold_date_sk` Nullable(Int32),
  `cs_sold_time_sk` Nullable(Int32),
  `cs_ship_date_sk` Nullable(Int32),
  `cs_bill_customer_sk` Nullable(Int32),
  `cs_bill_cdemo_sk` Nullable(Int32),
  `cs_bill_hdemo_sk` Nullable(Int16),
  `cs_bill_addr_sk` Nullable(Int32),
  `cs_ship_customer_sk` Nullable(Int32),
  `cs_ship_cdemo_sk` Nullable(Int32),
  `cs_ship_hdemo_sk` Nullable(Int16),
  `cs_ship_addr_sk` Nullable(Int32),
  `cs_call_center_sk` Nullable(Int8),
  `cs_catalog_page_sk` Nullable(Int16),
  `cs_ship_mode_sk` Nullable(Int8),
  `cs_warehouse_sk` Nullable(Int8),
  `cs_item_sk` Int32,
  `cs_promo_sk` Nullable(Int16),
  `cs_order_number` Int64,
  `cs_quantity` Nullable(Int8),
  `cs_wholesale_cost` Nullable(Float32),
  `cs_list_price` Nullable(Float32),
  `cs_sales_price` Nullable(Float32),
  `cs_ext_discount_amt` Nullable(Float32),
  `cs_ext_sales_price` Nullable(Float32),
  `cs_ext_wholesale_cost` Nullable(Float32),
  `cs_ext_list_price` Nullable(Float32),
  `cs_ext_tax` Nullable(Float32),
  `cs_coupon_amt` Nullable(Float32),
  `cs_ext_ship_cost` Nullable(Float32),
  `cs_net_paid` Nullable(Float32),
  `cs_net_paid_inc_tax` Nullable(Float32),
  `cs_net_paid_inc_ship` Float32,
  `cs_net_paid_inc_ship_tax` Float32,
  `cs_net_profit` Float32,
  constraint fk1 foreign key(cs_sold_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(cs_sold_time_sk) references time_dim(t_time_sk),
  constraint fk3 foreign key(cs_ship_date_sk) references date_dim(d_date_sk),
  constraint fk4 foreign key(cs_bill_customer_sk) references customer(c_customer_sk),
  constraint fk5 foreign key(cs_bill_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk6 foreign key(cs_bill_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk7 foreign key(cs_bill_addr_sk) references customer_address(ca_address_sk),
  constraint fk8 foreign key(cs_ship_customer_sk) references customer(c_customer_sk),
  constraint fk9 foreign key(cs_ship_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk10 foreign key(cs_ship_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk11 foreign key(cs_ship_addr_sk) references customer_address(ca_address_sk),
  constraint fk12 foreign key(cs_call_center_sk) references call_center(cc_call_center_sk),
  constraint fk13 foreign key(cs_catalog_page_sk) references catalog_page(cp_catalog_page_sk),
  constraint fk14 foreign key(cs_ship_mode_sk) references ship_mode(sm_ship_mode_sk),
  constraint fk15 foreign key(cs_warehouse_sk) references warehouse(w_warehouse_sk),
  constraint fk16 foreign key(cs_item_sk) references item(i_item_sk),
  constraint fk17 foreign key(cs_promo_sk) references promotion(p_promo_sk),
  constraint un1 unique(cs_item_sk, cs_order_number)
) ENGINE = CnchMergeTree()
ORDER BY cs_item_sk
CLUSTER BY (cs_item_sk) INTO 3 BUCKETS;

create table inventory (
  `inv_date_sk` Int32,
  `inv_item_sk` Int32,
  `inv_warehouse_sk` Int8,
  `inv_quantity_on_hand` Nullable(Int16),
  constraint fk1 foreign key(inv_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(inv_item_sk) references item(i_item_sk),
  constraint fk3 foreign key(inv_warehouse_sk) references warehouse(w_warehouse_sk),
  constraint un1 unique(inv_date_sk, inv_item_sk, inv_warehouse_sk)
) ENGINE = CnchMergeTree() 
ORDER BY inv_item_sk
CLUSTER BY (inv_item_sk) INTO 3 BUCKETS;

create table store_returns (
  `sr_returned_date_sk` Nullable(Int32),
  `sr_return_time_sk` Nullable(Int32),
  `sr_item_sk` Int32,
  `sr_customer_sk` Nullable(Int32),
  `sr_cdemo_sk` Nullable(Int32),
  `sr_hdemo_sk` Nullable(Int16),
  `sr_addr_sk` Nullable(Int32),
  `sr_store_sk` Nullable(Int16),
  `sr_reason_sk` Nullable(Int8),
  `sr_ticket_number` Int64,
  `sr_return_quantity` Nullable(Int8),
  `sr_return_amt` Nullable(Float32),
  `sr_return_tax` Nullable(Float32),
  `sr_return_amt_inc_tax` Nullable(Float32),
  `sr_fee` Nullable(Float32),
  `sr_return_ship_cost` Nullable(Float32),
  `sr_refunded_cash` Nullable(Float32),
  `sr_reversed_charge` Nullable(Float32),
  `sr_store_credit` Nullable(Float32),
  `sr_net_loss` Nullable(Float32),
  constraint fk1 foreign key(sr_returned_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(sr_return_time_sk) references time_dim(t_time_sk),
  constraint fk3 foreign key(sr_item_sk) references item(i_item_sk),
  constraint fk4 foreign key(sr_customer_sk) references customer(c_customer_sk),
  constraint fk5 foreign key(sr_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk6 foreign key(sr_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk7 foreign key(sr_addr_sk) references customer_address(ca_address_sk),
  constraint fk8 foreign key(sr_store_sk) references store(s_store_sk),
  constraint fk9 foreign key(sr_reason_sk) references reason(r_reason_sk),
  constraint un1 unique(sr_item_sk, sr_ticket_number)
) ENGINE = CnchMergeTree() 
ORDER BY sr_item_sk
CLUSTER BY (sr_item_sk) INTO 3 BUCKETS;

create table store_sales (
  `ss_sold_date_sk` Nullable(Int32),
  `ss_sold_time_sk` Nullable(Int32),
  `ss_item_sk` Int32,
  `ss_customer_sk` Nullable(Int32),
  `ss_cdemo_sk` Nullable(Int32),
  `ss_hdemo_sk` Nullable(Int16),
  `ss_addr_sk` Nullable(Int32),
  `ss_store_sk` Nullable(Int16),
  `ss_promo_sk` Nullable(Int16),
  `ss_ticket_number` Int64,
  `ss_quantity` Nullable(Int8),
  `ss_wholesale_cost` Nullable(Float32),
  `ss_list_price` Nullable(Float32),
  `ss_sales_price` Nullable(Float32),
  `ss_ext_discount_amt` Nullable(Float32),
  `ss_ext_sales_price` Nullable(Float32),
  `ss_ext_wholesale_cost` Nullable(Float32),
  `ss_ext_list_price` Nullable(Float32),
  `ss_ext_tax` Nullable(Float32),
  `ss_coupon_amt` Nullable(Float32),
  `ss_net_paid` Nullable(Float32),
  `ss_net_paid_inc_tax` Nullable(Float32),
  `ss_net_profit` Nullable(Float32),
  constraint fk1 foreign key(ss_sold_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(ss_sold_time_sk) references time_dim(t_time_sk),
  constraint fk3 foreign key(ss_item_sk) references item(i_item_sk),
  constraint fk4 foreign key(ss_customer_sk) references customer(c_customer_sk),
  constraint fk5 foreign key(ss_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk6 foreign key(ss_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk7 foreign key(ss_addr_sk) references customer_address(ca_address_sk),
  constraint fk8 foreign key(ss_store_sk) references store(s_store_sk),
  constraint fk9 foreign key(ss_promo_sk) references promotion(p_promo_sk),
  constraint un1 unique(ss_item_sk, ss_ticket_number)
) ENGINE = CnchMergeTree() 
ORDER BY ss_item_sk
CLUSTER BY (ss_item_sk) INTO 3 BUCKETS;

create table web_returns (
  `wr_returned_date_sk` Nullable(Int32),
  `wr_returned_time_sk` Nullable(Int32),
  `wr_item_sk` Int32,
  `wr_refunded_customer_sk` Nullable(Int32),
  `wr_refunded_cdemo_sk` Nullable(Int32),
  `wr_refunded_hdemo_sk` Nullable(Int16),
  `wr_refunded_addr_sk` Nullable(Int32),
  `wr_returning_customer_sk` Nullable(Int32),
  `wr_returning_cdemo_sk` Nullable(Int32),
  `wr_returning_hdemo_sk` Nullable(Int16),
  `wr_returning_addr_sk` Nullable(Int32),
  `wr_web_page_sk` Nullable(Int16),
  `wr_reason_sk` Nullable(Int8),
  `wr_order_number` Int64,
  `wr_return_quantity` Nullable(Int8),
  `wr_return_amt` Nullable(Float32),
  `wr_return_tax` Nullable(Float32),
  `wr_return_amt_inc_tax` Nullable(Float32),
  `wr_fee` Nullable(Float32),
  `wr_return_ship_cost` Nullable(Float32),
  `wr_refunded_cash` Nullable(Float32),
  `wr_reversed_charge` Nullable(Float32),
  `wr_account_credit` Nullable(Float32),
  `wr_net_loss` Nullable(Float32),
  constraint fk1 foreign key(wr_returned_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(wr_returned_time_sk) references time_dim(t_time_sk),
  constraint fk3 foreign key(wr_item_sk) references item(i_item_sk),
  constraint fk4 foreign key(wr_refunded_customer_sk) references customer(c_customer_sk),
  constraint fk5 foreign key(wr_refunded_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk6 foreign key(wr_refunded_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk7 foreign key(wr_refunded_addr_sk) references customer_address(ca_address_sk),
  constraint fk8 foreign key(wr_returning_customer_sk) references customer(c_customer_sk),
  constraint fk9 foreign key(wr_returning_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk10 foreign key(wr_returning_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk11 foreign key(wr_returning_addr_sk) references customer_address(ca_address_sk),
  constraint fk12 foreign key(wr_web_page_sk) references web_page(wp_web_page_sk),
  constraint fk13 foreign key(wr_reason_sk) references reason(r_reason_sk),
  constraint un1 unique(wr_item_sk, wr_order_number)
) ENGINE = CnchMergeTree() 
ORDER BY wr_item_sk
CLUSTER BY (wr_item_sk) INTO 3 BUCKETS;

create table web_sales (
  `ws_sold_date_sk` Nullable(Int32),
  `ws_sold_time_sk` Nullable(Int32),
  `ws_ship_date_sk` Nullable(Int32),
  `ws_item_sk` Int32,
  `ws_bill_customer_sk` Nullable(Int32),
  `ws_bill_cdemo_sk` Nullable(Int32),
  `ws_bill_hdemo_sk` Nullable(Int16),
  `ws_bill_addr_sk` Nullable(Int32),
  `ws_ship_customer_sk` Nullable(Int32),
  `ws_ship_cdemo_sk` Nullable(Int32),
  `ws_ship_hdemo_sk` Nullable(Int16),
  `ws_ship_addr_sk` Nullable(Int32),
  `ws_web_page_sk` Nullable(Int16),
  `ws_web_site_sk` Nullable(Int8),
  `ws_ship_mode_sk` Nullable(Int8),
  `ws_warehouse_sk` Nullable(Int8),
  `ws_promo_sk` Nullable(Int16),
  `ws_order_number` Int64,
  `ws_quantity` Nullable(Int8),
  `ws_wholesale_cost` Nullable(Float32),
  `ws_list_price` Nullable(Float32),
  `ws_sales_price` Nullable(Float32),
  `ws_ext_discount_amt` Nullable(Float32),
  `ws_ext_sales_price` Nullable(Float32),
  `ws_ext_wholesale_cost` Nullable(Float32),
  `ws_ext_list_price` Nullable(Float32),
  `ws_ext_tax` Nullable(Float32),
  `ws_coupon_amt` Nullable(Float32),
  `ws_ext_ship_cost` Nullable(Float32),
  `ws_net_paid` Nullable(Float32),
  `ws_net_paid_inc_tax` Nullable(Float32),
  `ws_net_paid_inc_ship` Float32,
  `ws_net_paid_inc_ship_tax` Float32,
  `ws_net_profit` Float32,
  constraint fk1 foreign key(ws_sold_date_sk) references date_dim(d_date_sk),
  constraint fk2 foreign key(ws_sold_time_sk) references time_dim(t_time_sk),
  constraint fk3 foreign key(ws_ship_date_sk) references date_dim(d_date_sk),
  constraint fk4 foreign key(ws_item_sk) references item(i_item_sk),
  constraint fk5 foreign key(ws_bill_customer_sk) references customer(c_customer_sk),
  constraint fk6 foreign key(ws_bill_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk7 foreign key(ws_bill_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk8 foreign key(ws_bill_addr_sk) references customer_address(ca_address_sk),
  constraint fk9 foreign key(ws_ship_customer_sk) references customer(c_customer_sk),
  constraint fk10 foreign key(ws_ship_cdemo_sk) references customer_demographics(cd_demo_sk),
  constraint fk11 foreign key(ws_ship_hdemo_sk) references household_demographics(hd_demo_sk),
  constraint fk12 foreign key(ws_ship_addr_sk) references customer_address(ca_address_sk),
  constraint fk13 foreign key(ws_web_page_sk) references web_page(wp_web_page_sk),
  constraint fk14 foreign key(ws_web_site_sk) references web_site(web_site_sk),
  constraint fk15 foreign key(ws_ship_mode_sk) references ship_mode(sm_ship_mode_sk),
  constraint fk16 foreign key(ws_warehouse_sk) references warehouse(w_warehouse_sk),
  constraint fk17 foreign key(ws_promo_sk) references promotion(p_promo_sk),
  constraint un1 unique(ws_item_sk, ws_order_number)
) ENGINE = CnchMergeTree() 
ORDER BY ws_item_sk
CLUSTER BY (ws_item_sk) INTO 3 BUCKETS;


