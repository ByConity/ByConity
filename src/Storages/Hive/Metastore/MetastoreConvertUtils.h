#pragma once


#include <Core/Types.h>
#include <aws/core/utils/DateTime.h>
#include <aws/glue/model/ColumnStatistics.h>
#include <aws/glue/model/Partition.h>
#include <aws/glue/model/SerDeInfo.h>
#include <aws/glue/model/StorageDescriptor.h>
#include <aws/glue/model/Table.h>
#include <hivemetastore/hive_metastore_types.h>
#include "IMetaClient.h"
namespace DB::MetastoreConvertUtils
{
ApacheHive::Table convertTable(const Aws::Glue::Model::Table & glue_table);

ApacheHive::Partition convertPartition(const Aws::Glue::Model::Partition & glue_partition);

void convertSD(const Aws::Glue::Model::StorageDescriptor & glue_sd, ApacheHive::StorageDescriptor & hive_sd);

void convertSerdeInfo(const Aws::Glue::Model::SerDeInfo & glue_serde, ApacheHive::SerDeInfo & hive_serde);

void convertOrder(const Aws::Glue::Model::Order & glue_order, ApacheHive::Order & hive_order);
ApacheHive::Order convertOrder(const Aws::Glue::Model::Order & glue_order);

void convertTable(const Aws::Glue::Model::Table & glue_table, ApacheHive::Table & hive_table);
int32_t convertDateTime(const Aws::Utils::DateTime & aws_date_time);
ApacheHive::FieldSchema convertFieldSchema(const Aws::Glue::Model::Column & glue_col);

void convertSkewedInfo(const Aws::Glue::Model::SkewedInfo & glue_skewed, ApacheHive::SkewedInfo hive_skewed);

ApacheHive::ColumnStatisticsObj convertTableStatistics(const Aws::Glue::Model::ColumnStatistics & glue_stat);

std::string concatPartitionValues(const Strings & partition_keys, const Strings & vals);

std::pair<int64_t, ApacheHive::TableStatsResult> merge_partition_stats(
    const ApacheHive::Table & table,
    const std::vector<ApacheHive::Partition> & partitions,
    const ApacheHive::PartitionsStatsResult & partition_stats);

struct PartitionStatsWithRowNumber
{
    int64_t row_count;
    std::map<String, const ApacheHive::ColumnStatisticsObj &> col_stats;
};

bool merge_column_stats(
    const String & col_name, const std::map<String, PartitionStatsWithRowNumber> & partition_infos, ApacheHive::ColumnStatisticsObj & ret);
}
