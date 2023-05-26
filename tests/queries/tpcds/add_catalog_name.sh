sed -i -e 's/,/, /g' $1

sed -i \
-e 's/\bcall_center\b/ hive_hdfs.tpcds1g.call_center/g' \
-e 's/\bcatalog_page\b/ hive_hdfs.tpcds1g.catalog_page/g' \
-e 's/\bcatalog_returns\b/ hive_hdfs.tpcds1g.catalog_returns/g' \
-e 's/\bcatalog_sales\b/ hive_hdfs.tpcds1g.catalog_sales/g' \
-e 's/\bcustomer\b/ hive_hdfs.tpcds1g.customer/g' \
-e 's/\bcustomer_address\b/ hive_hdfs.tpcds1g.customer_address/g' \
-e 's/\bcustomer_demographics\b/ hive_hdfs.tpcds1g.customer_demographics/g' \
-e 's/\bdate_dim\b/ hive_hdfs.tpcds1g.date_dim/g' \
-e 's/\bhousehold_demographics\b/ hive_hdfs.tpcds1g.household_demographics/g' \
-e 's/\bincome_band\b/ hive_hdfs.tpcds1g.income_band/g' \
-e 's/\binventory\b/ hive_hdfs.tpcds1g.inventory/g' \
-e 's/\bitem\b/ hive_hdfs.tpcds1g.item/g' \
-e 's/\bpromotion\b/ hive_hdfs.tpcds1g.promotion/g' \
-e 's/\breason\b/ hive_hdfs.tpcds1g.reason/g' \
-e 's/\bship_mode\b/ hive_hdfs.tpcds1g.ship_mode/g' \
-e 's/\bstore\b/ hive_hdfs.tpcds1g.store/g' \
-e 's/\bstore_returns\b/ hive_hdfs.tpcds1g.store_returns/g' \
-e 's/\bstore_sales\b/ hive_hdfs.tpcds1g.store_sales/g' \
-e 's/\btime_dim\b/ hive_hdfs.tpcds1g.time_dim/g' \
-e 's/\bwarehouse\b/ hive_hdfs.tpcds1g.warehouse/g' \
-e 's/\bweb_page\b/ hive_hdfs.tpcds1g.web_page/g' \
-e 's/\bweb_returns\b/ hive_hdfs.tpcds1g.web_returns/g' \
-e 's/\bweb_sales\b/ hive_hdfs.tpcds1g.web_sales/g' \
-e 's/\bweb_site\b/ hive_hdfs.tpcds1g.web_site/g' \
6_tpcds_with_catalog/*.sql