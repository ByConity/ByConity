#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <hive_metastore_types.h>
#include <Storages/Hive/Metastore/MetastoreConvertUtils.h>
#include <aws/core/utils/DateTime.h>
#include <aws/glue/model/Column.h>
#include <aws/glue/model/ColumnStatistics.h>
#include <aws/glue/model/ColumnStatisticsType.h>
#include <aws/glue/model/DecimalNumber.h>
#include <aws/glue/model/SkewedInfo.h>
#include <aws/glue/model/Table.h>
#include "common/types.h"
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int UNKNOWN_TYPE;
extern const int NO_HIVEMETASTORE;
extern const int BAD_ARGUMENTS;
extern const int NETWORK_ERROR;
}
namespace DB::MetastoreConvertUtils
{
namespace GlueModel = Aws::Glue::Model;
ApacheHive::Table convertTable(const Aws::Glue::Model::Table & glue_table)
{
    ApacheHive::Table hive_table;
    convertTable(glue_table, hive_table);
    return hive_table;
}

ApacheHive::Partition convertPartition(const Aws::Glue::Model::Partition & glue_partition)
{
    ApacheHive::Partition hive_partition;
    hive_partition.createTime = convertDateTime(glue_partition.GetCreationTime());
    hive_partition.dbName = glue_partition.GetDatabaseName();
    hive_partition.tableName = glue_partition.GetTableName();
    hive_partition.lastAccessTime = convertDateTime(glue_partition.GetLastAccessTime());
    for (const auto & p : glue_partition.GetParameters())
    {
        hive_partition.parameters.emplace(p);
    }


    for (const auto & v : glue_partition.GetValues())
    {
        hive_partition.values.emplace_back(v);
    }
    convertSD(glue_partition.GetStorageDescriptor(), hive_partition.sd);

    hive_partition.__isset.createTime = glue_partition.CreationTimeHasBeenSet();
    hive_partition.__isset.lastAccessTime = glue_partition.LastAccessTimeHasBeenSet();
    hive_partition.__isset.dbName = glue_partition.DatabaseNameHasBeenSet();
    hive_partition.__isset.tableName = glue_partition.TableNameHasBeenSet();
    hive_partition.__isset.parameters = glue_partition.ParametersHasBeenSet();
    hive_partition.__isset.values = glue_partition.ValuesHasBeenSet();
    hive_partition.__isset.sd = glue_partition.StorageDescriptorHasBeenSet();

    return hive_partition;
}


void convertTable(const Aws::Glue::Model::Table & glue_table, ApacheHive::Table & hive_table)
{
    hive_table.dbName = glue_table.GetDatabaseName();
    hive_table.tableName = glue_table.GetName();
    hive_table.createTime = convertDateTime(glue_table.GetCreateTime());
    hive_table.owner = glue_table.GetOwner();
    hive_table.lastAccessTime = convertDateTime(glue_table.GetLastAccessTime());
    hive_table.retention = glue_table.GetRetention();
    // TODO(renming):: Check Hudi/Iceberg
    MetastoreConvertUtils::convertSD(glue_table.GetStorageDescriptor(), hive_table.sd);
    for (const auto & partition_key : glue_table.GetPartitionKeys())
    {
        hive_table.partitionKeys.emplace_back(convertFieldSchema(partition_key));
    }
    for (const auto & p : glue_table.GetParameters())
    {
        hive_table.parameters.emplace(p);
    }
    hive_table.viewOriginalText = glue_table.GetViewOriginalText();
    hive_table.viewExpandedText = glue_table.GetViewExpandedText();
    hive_table.tableType = glue_table.GetTableType();

    hive_table.__isset.dbName = glue_table.DatabaseNameHasBeenSet();
    hive_table.__isset.tableName = glue_table.NameHasBeenSet();
    hive_table.__isset.createTime = glue_table.CreateTimeHasBeenSet();
    hive_table.__isset.owner = glue_table.OwnerHasBeenSet();
    hive_table.__isset.lastAccessTime = glue_table.LastAccessTimeHasBeenSet();
    hive_table.__isset.retention = glue_table.RetentionHasBeenSet();
    hive_table.__isset.partitionKeys = glue_table.PartitionKeysHasBeenSet();
    hive_table.__isset.parameters = glue_table.ParametersHasBeenSet();
    hive_table.__isset.viewOriginalText = glue_table.ViewOriginalTextHasBeenSet();
    hive_table.__isset.viewExpandedText = glue_table.ViewExpandedTextHasBeenSet();
    hive_table.__isset.tableType = glue_table.TableTypeHasBeenSet();
}

int32_t convertDateTime(const Aws::Utils::DateTime & aws_date_time)
{
    return static_cast<int32_t>(aws_date_time.Millis() / 1000);
}
ApacheHive::FieldSchema convertFieldSchema(const GlueModel::Column & glue_col)
{
    ApacheHive::FieldSchema schema;
    schema.comment = glue_col.GetComment();
    schema.name = glue_col.GetName();
    schema.type = glue_col.GetType();
    schema.__isset.comment = glue_col.CommentHasBeenSet();
    schema.__isset.name = glue_col.NameHasBeenSet();
    schema.__isset.type = glue_col.TypeHasBeenSet();

    return schema;
}

void convertSD(const Aws::Glue::Model::StorageDescriptor & glue_sd, ApacheHive::StorageDescriptor & hive_sd)
{
    for (const auto & col : glue_sd.GetColumns())
    {
        hive_sd.cols.emplace_back(convertFieldSchema(col));
    }
    hive_sd.location = glue_sd.GetLocation();
    hive_sd.inputFormat = glue_sd.GetInputFormat();
    hive_sd.outputFormat = glue_sd.GetOutputFormat();
    hive_sd.compressed = glue_sd.GetCompressed();
    hive_sd.numBuckets = glue_sd.GetNumberOfBuckets();
    convertSerdeInfo(glue_sd.GetSerdeInfo(), hive_sd.serdeInfo);
    hive_sd.bucketCols = glue_sd.GetBucketColumns();
    for (const auto & glue_order : glue_sd.GetSortColumns())
    {
        hive_sd.sortCols.emplace_back(convertOrder(glue_order));
    }
    hive_sd.parameters = glue_sd.GetParameters();
    convertSkewedInfo(glue_sd.GetSkewedInfo(), hive_sd.skewedInfo);
    hive_sd.storedAsSubDirectories = glue_sd.GetStoredAsSubDirectories();

    hive_sd.__isset.cols = glue_sd.ColumnsHasBeenSet();
    hive_sd.__isset.location = glue_sd.LocationHasBeenSet();
    hive_sd.__isset.inputFormat = glue_sd.InputFormatHasBeenSet();
    hive_sd.__isset.outputFormat = glue_sd.OutputFormatHasBeenSet();
    hive_sd.__isset.compressed = glue_sd.CompressedHasBeenSet();
    hive_sd.__isset.numBuckets = glue_sd.NumberOfBucketsHasBeenSet();
    hive_sd.__isset.serdeInfo = glue_sd.SerdeInfoHasBeenSet();
    hive_sd.__isset.bucketCols = glue_sd.BucketColumnsHasBeenSet();
    hive_sd.__isset.sortCols = glue_sd.SortColumnsHasBeenSet();
    hive_sd.__isset.parameters = glue_sd.ParametersHasBeenSet();
    hive_sd.__isset.skewedInfo = glue_sd.SkewedInfoHasBeenSet();
    hive_sd.__isset.storedAsSubDirectories = glue_sd.StoredAsSubDirectoriesHasBeenSet();
}

void convertSerdeInfo(const Aws::Glue::Model::SerDeInfo & glue_serde, ApacheHive::SerDeInfo & hive_serde)
{
    hive_serde.name = glue_serde.GetName();
    hive_serde.serializationLib = glue_serde.GetSerializationLibrary();
    hive_serde.parameters = glue_serde.GetParameters();
    hive_serde.__isset.name = glue_serde.NameHasBeenSet();
    hive_serde.__isset.serializationLib = glue_serde.SerializationLibraryHasBeenSet();
    hive_serde.__isset.parameters = glue_serde.ParametersHasBeenSet();
}

void convertOrder(const Aws::Glue::Model::Order & glue_order, ApacheHive::Order & hive_order)
{
    hive_order.col = glue_order.GetColumn();
    hive_order.order = glue_order.GetSortOrder();
    hive_order.__isset.col = glue_order.ColumnHasBeenSet();
    hive_order.__isset.order = glue_order.SortOrderHasBeenSet();
}


ApacheHive::Order convertOrder(const Aws::Glue::Model::Order & glue_order)
{
    ApacheHive::Order hive_order;
    convertOrder(glue_order, hive_order);
    return hive_order;
}

void convertSkewedInfo(const Aws::Glue::Model::SkewedInfo & glue_skewed, ApacheHive::SkewedInfo hive_skewed)
{
    for (const auto & col : glue_skewed.GetSkewedColumnNames())
    {
        hive_skewed.skewedColNames.emplace_back(col);
    }
    // column values is a string separted by '$' in glue.
    for (const auto & val : glue_skewed.GetSkewedColumnValues())
    {
        hive_skewed.skewedColValues.emplace_back();
        auto & v = hive_skewed.skewedColValues.back();
        std::istringstream input;
        input.str(val);
        for (String line; std::getline(input, line, '$');)
        {
            v.emplace_back(line);
        }
    }
    for (const auto & p : glue_skewed.GetSkewedColumnValueLocationMaps())
    {
        hive_skewed.skewedColValueLocationMaps.emplace(p);
    }
    hive_skewed.__isset.skewedColNames = glue_skewed.SkewedColumnNamesHasBeenSet();
    hive_skewed.__isset.skewedColValues = glue_skewed.SkewedColumnValuesHasBeenSet();
    hive_skewed.__isset.skewedColValueLocationMaps = glue_skewed.SkewedColumnValueLocationMapsHasBeenSet();
}


static int64_t daysFromEpoch(const Aws::Utils::DateTime & aws_time)
{
    return std::chrono::duration_cast<std::chrono::days>(aws_time.UnderlyingTimestamp().time_since_epoch()).count();
}


static void copyDecimal(const Aws::Glue::Model::DecimalNumber & aws_decimal, ApacheHive::Decimal & hive_decimal)
{
    hive_decimal.scale = aws_decimal.GetScale();
    hive_decimal.unscaled.append(
        reinterpret_cast<char *>(aws_decimal.GetUnscaledValue().GetUnderlyingData()), aws_decimal.GetUnscaledValue().GetLength());
}
ApacheHive::ColumnStatisticsObj convertTableStatistics(const Aws::Glue::Model::ColumnStatistics & glue_stat)
{
    using GlueType = Aws::Glue::Model::ColumnStatisticsType;
    namespace GlueTypeMapper = Aws::Glue::Model::ColumnStatisticsTypeMapper;
    const auto & data = glue_stat.GetStatisticsData();
    const auto & type = data.GetType();
    const auto & glue_col_name = glue_stat.GetColumnName();
    ApacheHive::ColumnStatisticsObj ret;
    switch (type)
    {
        case GlueType::BOOLEAN: {
            const auto & glue_boolean = data.GetBooleanColumnStatisticsData();
            ApacheHive::BooleanColumnStatsData hive_boolean;
            hive_boolean.__set_numTrues(glue_boolean.GetNumberOfTrues());
            hive_boolean.__set_numFalses(glue_boolean.GetNumberOfFalses());
            hive_boolean.__set_numNulls(glue_boolean.GetNumberOfNulls());
            ret.statsData.__set_booleanStats(hive_boolean);

            ret.colName = glue_col_name;
            ret.colType = "BOOLEAN";
            return ret;
        }
        case GlueType::DATE: {
            const auto & glue_date = data.GetDateColumnStatisticsData();
            ApacheHive::DateColumnStatsData hive_date;
            if (glue_date.MaximumValueHasBeenSet())
            {
                hive_date.highValue.daysSinceEpoch = daysFromEpoch(glue_date.GetMaximumValue());
                hive_date.__isset.highValue = true;
            }
            if (glue_date.MinimumValueHasBeenSet())
            {
                hive_date.lowValue.daysSinceEpoch = daysFromEpoch(glue_date.GetMinimumValue());
                hive_date.__isset.lowValue = true;
            }
            hive_date.numDVs = glue_date.GetNumberOfDistinctValues();
            hive_date.numNulls = glue_date.GetNumberOfNulls();
            ret.statsData.__set_dateStats(hive_date);

            ret.colName = glue_col_name;
            ret.colType = "DATE";
            return ret;
        }
        case GlueType::DECIMAL: {
            const auto & glue_decimal = data.GetDecimalColumnStatisticsData();
            ApacheHive::DecimalColumnStatsData hive_decimal;
            if (glue_decimal.MaximumValueHasBeenSet())
            {
                copyDecimal(glue_decimal.GetMaximumValue(), hive_decimal.highValue);
                hive_decimal.__isset.highValue = true;
            }
            if (glue_decimal.MinimumValueHasBeenSet())
            {
                copyDecimal(glue_decimal.GetMinimumValue(), hive_decimal.lowValue);
                hive_decimal.__isset.lowValue = true;
            }
            hive_decimal.numDVs = glue_decimal.GetNumberOfDistinctValues();
            hive_decimal.numNulls = glue_decimal.GetNumberOfNulls();
            ret.statsData.__set_decimalStats(hive_decimal);

            ret.colName = glue_col_name;
            ret.colType = "DECIMAL";
            return ret;
        }
        case GlueType::DOUBLE: {
            const auto & glue_double = data.GetDoubleColumnStatisticsData();
            ApacheHive::DoubleColumnStatsData hive_double;
            if (glue_double.MaximumValueHasBeenSet())
            {
                hive_double.highValue = glue_double.GetMaximumValue();
                hive_double.__isset.highValue = true;
            }
            if (glue_double.MinimumValueHasBeenSet())
            {
                hive_double.lowValue = glue_double.GetMinimumValue();
                hive_double.__isset.lowValue = true;
            }
            hive_double.numDVs = glue_double.NumberOfDistinctValuesHasBeenSet();
            hive_double.numNulls = glue_double.GetNumberOfNulls();
            ret.statsData.__set_doubleStats(hive_double);

            ret.colName = glue_col_name;
            ret.colType = "DOUBLE";
            return ret;
        }
        case GlueType::LONG: {
            const auto & glue_long = data.GetLongColumnStatisticsData();
            ApacheHive::LongColumnStatsData hive_long;
            if (glue_long.MaximumValueHasBeenSet())
            {
                hive_long.highValue = glue_long.GetMaximumValue();
                hive_long.__isset.highValue = true;
            }
            if (glue_long.MinimumValueHasBeenSet())
            {
                hive_long.lowValue = glue_long.GetMinimumValue();
                hive_long.__isset.lowValue = true;
            }

            hive_long.numDVs = glue_long.NumberOfDistinctValuesHasBeenSet();
            hive_long.numNulls = glue_long.GetNumberOfNulls();
            ret.statsData.__set_longStats(hive_long);
            hive_long.printTo(std::cout);
            std::cout << std::endl;

            ret.colName = glue_col_name;
            ret.colType = "LONG";
            return ret;
        }

        case GlueType::STRING: {
            const auto & glue_str = data.GetStringColumnStatisticsData();
            ApacheHive::StringColumnStatsData hive_str;
            hive_str.avgColLen = glue_str.GetAverageLength();
            hive_str.maxColLen = glue_str.GetMaximumLength();
            hive_str.numNulls = glue_str.GetNumberOfNulls();
            hive_str.numDVs = glue_str.GetNumberOfDistinctValues();
            ret.statsData.__set_stringStats(hive_str);

            ret.colName = glue_col_name;
            ret.colType = "STRING";
            return ret;
        }
        case GlueType::BINARY: {
            const auto & glue_binary = data.GetBinaryColumnStatisticsData();
            ApacheHive::BinaryColumnStatsData hive_binary;
            hive_binary.avgColLen = glue_binary.GetAverageLength();
            hive_binary.maxColLen = glue_binary.GetMaximumLength();
            hive_binary.numNulls = glue_binary.GetNumberOfNulls();
            ret.statsData.__set_binaryStats(hive_binary);

            ret.colName = glue_col_name;
            ret.colType = "BINARY";
            return ret;
        }
        default:
            throw DB::Exception(
                ErrorCodes::UNKNOWN_TYPE, "unknown glue type {} in statistics data", GlueTypeMapper::GetNameForColumnStatisticsType(type));
    }
}


std::string concatPartitionValues(const Strings & partition_keys, const Strings & vals)
{
    assert(partition_keys.size() == vals.size());
    std::ostringstream ss;
    for (size_t i = 0; i < partition_keys.size(); i++)
    {
        ss << partition_keys[i] << '=' << vals[i] << ((i == partition_keys.size() - 1) ? "" : "/");
    }
    return ss.str();
}


std::pair<int64_t, ApacheHive::TableStatsResult> merge_partition_stats(
    const ApacheHive::Table & table,
    const std::vector<ApacheHive::Partition> & partitions,
    const ApacheHive::PartitionsStatsResult & partition_stats)
{
    Strings partition_keys;
    for (const auto & field : table.partitionKeys)
    {
        partition_keys.emplace_back(field.name);
    }
    Strings partition_strs;
    std::map<String, PartitionStatsWithRowNumber> partition_stats_with_row_number;
    int64_t sum_row_count = 0;

    {
        for (const auto & p : partitions)
        {
            const auto & vals = p.values;
            if (vals.size() != partition_keys.size())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "size of partition keys and values does not match, "
                    "number of keys {}, number of values {}",
                    partition_keys.size(),
                    vals.size());
            }
            auto partition_name = MetastoreConvertUtils::concatPartitionValues(partition_keys, vals);
            auto it = p.parameters.find("numRows");
            if (it == p.parameters.end())
            {
                throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "partition {} does not contain numRows", partition_name);
            }
            int row_count = std::stol(it->second);
            sum_row_count += row_count;
            partition_stats_with_row_number[partition_name] = {.row_count = row_count};
        }
    }


    if (partition_stats.partStats.size() == 1)
    {
        ApacheHive::TableStatsResult result;
        result.tableStats = std::move((*partition_stats.partStats.begin()).second);

        return {sum_row_count, result};
    }

    std::set<String> valid_col_names; // the colums that have stats. it should be calculated as
        // the appeared columns in each partition.

    for (const auto & c : partition_stats.partStats.begin()->second)
    {
        valid_col_names.emplace(c.colName);
    }

    for (const auto & partition_info : partition_stats.partStats)
    {
        const std::vector<ApacheHive::ColumnStatisticsObj> & col_stats = partition_info.second;
        std::map<String, const ApacheHive::ColumnStatisticsObj &> col_map; // col_name -> col_stat
        std::vector<String> valid_col_for_this_partition;
        for (const auto & stat_obj : col_stats)
        {
            if (valid_col_names.contains(stat_obj.colName))
            {
                col_map.emplace(stat_obj.colName, stat_obj);
                valid_col_for_this_partition.emplace_back(stat_obj.colName);
            }
        }
        // the new valid_col_names = (old valid_col_names) diff (columns appeared in
        // this partition)
        if (valid_col_for_this_partition.size() != valid_col_names.size())
        {
            valid_col_names = {valid_col_for_this_partition.begin(), valid_col_for_this_partition.end()};
        }

        partition_stats_with_row_number[partition_info.first].col_stats = std::move(col_map);
    }

    std::vector<ApacheHive::ColumnStatisticsObj> merged_stats;
    for (const auto & col_name : valid_col_names)
    {
        ApacheHive::ColumnStatisticsObj merged_col_stat;
        bool can_merge = merge_column_stats(col_name, partition_stats_with_row_number, merged_col_stat);
        if (can_merge)
            merged_stats.emplace_back(std::move(merged_col_stat));
    }

    ApacheHive::TableStatsResult result;
    result.tableStats = std::move(merged_stats);
    return {sum_row_count, result};
}


#define SET_BIGGER(ret, left, OP, right, cmp_val, T, real_val) \
    ret.real_val = ((left).__isset.real_val ? std::optional<T>{(cmp_val)(left)} : std::nullopt)OP( \
                       (right).__isset.real_val ? std::optional<T>((cmp_val)(right)) : std::nullopt) \
        ? (left).real_val \
        : (right).real_val; \
    (ret).__isset.real_val = (left).__isset.real_val || (right).__isset.real_val


bool merge_column_stats(
    const String & col_name, const std::map<String, PartitionStatsWithRowNumber> & partition_infos, ApacheHive::ColumnStatisticsObj & ret)
{
    struct ColumnStatisticsWithRows
    {
        int64_t row_count;
        ApacheHive::ColumnStatisticsData col_stat;
    };

    const auto & first_partition_info = partition_infos.begin()->second;
    ColumnStatisticsWithRows init{
        .row_count = first_partition_info.row_count, .col_stat = first_partition_info.col_stats.at(col_name).statsData};

    ret.colName = col_name;
    ret.colType = first_partition_info.col_stats.at(col_name).colType;

    auto start_it = partition_infos.begin();
    ++start_it;
    const auto & isset = init.col_stat.__isset;
    if (isset.binaryStats)
    {
        ret.statsData
            = std::accumulate(
                  start_it,
                  partition_infos.end(),
                  init,
                  [&col_name](
                      const ColumnStatisticsWithRows & left,
                      const std::map<String, const PartitionStatsWithRowNumber &>::value_type right_ref) -> ColumnStatisticsWithRows {
                      const auto & left_stat = left.col_stat.binaryStats;
                      const auto & right_stat = right_ref.second.col_stats.at(col_name).statsData.binaryStats;
                      auto left_row_count = left.row_count;
                      auto right_row_count = right_ref.second.row_count;
                      auto left_nonnull_count = left.row_count - left_stat.numNulls;
                      auto right_nonnull_count = right_row_count - right_stat.numNulls;

                      ColumnStatisticsWithRows merged;
                      ApacheHive::BinaryColumnStatsData merged_stat;
                      merged_stat.__set_maxColLen(std::max(left_stat.maxColLen, right_stat.maxColLen));
                      merged_stat.__set_avgColLen(
                          (left_row_count + right_row_count != 0)
                              ? (left_nonnull_count * left_stat.avgColLen + right_nonnull_count * right_stat.avgColLen)
                                  / (left_nonnull_count + right_nonnull_count)
                              : 0.0);
                      merged_stat.__set_numNulls(left_stat.numNulls + right_stat.numNulls);


                      merged.col_stat.__set_binaryStats(std::move(merged_stat));
                      merged.row_count = left_row_count + right_row_count;
                      return merged;
                  })
                  .col_stat;
        return true;
    }
    else if (isset.booleanStats)
    {
        ret.statsData
            = std::accumulate(
                  start_it,
                  partition_infos.end(),
                  init,
                  [&col_name](
                      const ColumnStatisticsWithRows & left,
                      const std::map<String, const PartitionStatsWithRowNumber &>::value_type right_ref) -> ColumnStatisticsWithRows {
                      const auto & left_stat = left.col_stat.booleanStats;
                      const auto & right_stat = right_ref.second.col_stats.at(col_name).statsData.booleanStats;
                      auto left_row_count = left.row_count;
                      auto right_row_count = right_ref.second.row_count;

                      ColumnStatisticsWithRows merged;
                      ApacheHive::BooleanColumnStatsData merged_stat;
                      merged_stat.__set_numTrues(left_stat.numTrues + right_stat.numTrues);
                      merged_stat.__set_numFalses(left_stat.numFalses + right_stat.numFalses);
                      merged_stat.__set_numNulls(left_stat.numNulls + right_stat.numNulls);

                      merged.col_stat.__set_booleanStats(std::move(merged_stat));
                      merged.row_count = left_row_count + right_row_count;
                      return merged;
                  })
                  .col_stat;
        return true;
    }
    else if (isset.dateStats)
    {
        ret.statsData
            = std::accumulate(
                  start_it,
                  partition_infos.end(),
                  init,
                  [&col_name](
                      const ColumnStatisticsWithRows & left,
                      const std::map<String, const PartitionStatsWithRowNumber &>::value_type right_ref) -> ColumnStatisticsWithRows {
                      const auto & left_stat = left.col_stat.dateStats;
                      const auto & right_stat = right_ref.second.col_stats.at(col_name).statsData.dateStats;
                      auto left_row_count = left.row_count;
                      auto right_row_count = right_ref.second.row_count;

                      ColumnStatisticsWithRows merged;
                      ApacheHive::DateColumnStatsData merged_stat;
                      auto high_epoch = [](const ApacheHive::DateColumnStatsData & t) -> int64_t { return t.highValue.daysSinceEpoch; };
                      auto low_epoch = [](const ApacheHive::DateColumnStatsData & t) -> int64_t {
                          return -t.lowValue.daysSinceEpoch;
                      }; // inverse the order.
                      SET_BIGGER(merged_stat, left_stat, >, right_stat, high_epoch, int64_t, highValue);
                      SET_BIGGER(merged_stat, left_stat, >, right_stat, low_epoch, int64_t, lowValue);

                      merged_stat.__set_numDVs(std::max(left_stat.numDVs, right_stat.numDVs));
                      merged_stat.__set_numNulls(left_stat.numNulls + right_stat.numNulls);

                      merged.col_stat.__set_dateStats(std::move(merged_stat));
                      merged.row_count = left_row_count + right_row_count;
                      return merged;
                  })
                  .col_stat;
        return true;
    }
    else if (isset.decimalStats)
    {
        auto parse_decimal = [](const ApacheHive::Decimal & decimal) -> double {
            if (decimal.unscaled.empty())
                return 0;

            bool is_first = true;
            double value = 0;
            for (auto byte : decimal.unscaled)
            {
                if (is_first)
                {
                    value = static_cast<Int8>(byte);
                    is_first = false;
                    continue;
                }
                value = value * 256 + static_cast<UInt8>(byte);
            }
            value *= std::pow(10, -decimal.scale);
            return value;
        };
        ret.statsData
            = std::accumulate(
                  start_it,
                  partition_infos.end(),
                  init,
                  [&col_name, &parse_decimal](
                      const ColumnStatisticsWithRows & left,
                      const std::map<String, const PartitionStatsWithRowNumber &>::value_type right_ref) -> ColumnStatisticsWithRows {
                      const auto & left_stat = left.col_stat.decimalStats;
                      const auto & right_stat = right_ref.second.col_stats.at(col_name).statsData.decimalStats;
                      auto left_row_count = left.row_count;
                      auto right_row_count = right_ref.second.row_count;

                      ColumnStatisticsWithRows merged;
                      ApacheHive::DecimalColumnStatsData merged_stat;
                      auto get_high_decimal
                          = [&parse_decimal](const ApacheHive::DecimalColumnStatsData & data) { return parse_decimal(data.highValue); };
                      auto get_low_decimal = [&parse_decimal](const ApacheHive::DecimalColumnStatsData & data) {
                          return -parse_decimal(data.lowValue);
                      }; // inverse the order.
                      SET_BIGGER(merged_stat, left_stat, >, right_stat, get_high_decimal, double, highValue);
                      SET_BIGGER(merged_stat, left_stat, >, right_stat, get_low_decimal, double, lowValue);
                      merged_stat.__set_numDVs(std::max(left_stat.numDVs, right_stat.numDVs));
                      merged_stat.__set_numNulls(left_stat.numNulls + right_stat.numNulls);

                      merged.col_stat.__set_decimalStats(std::move(merged_stat));
                      merged.row_count = left_row_count + right_row_count;
                      return merged;
                  })
                  .col_stat;
        return true;
    }
    else if (isset.doubleStats)
    {
        ret.statsData
            = std::accumulate(
                  start_it,
                  partition_infos.end(),
                  init,
                  [&col_name](
                      const ColumnStatisticsWithRows & left,
                      const std::map<String, const PartitionStatsWithRowNumber &>::value_type right_ref) -> ColumnStatisticsWithRows {
                      const auto & left_stat = left.col_stat.doubleStats;
                      const auto & right_stat = right_ref.second.col_stats.at(col_name).statsData.doubleStats;
                      auto left_row_count = left.row_count;
                      auto right_row_count = right_ref.second.row_count;

                      ColumnStatisticsWithRows merged;
                      ApacheHive::DoubleColumnStatsData merged_stat;
                      auto get_high_value = [](const ApacheHive::DoubleColumnStatsData & data) { return data.highValue; };
                      auto get_low_value
                          = [](const ApacheHive::DoubleColumnStatsData & data) { return -data.highValue; }; // inverse the order.
                      SET_BIGGER(merged_stat, left_stat, >, right_stat, get_high_value, double, highValue);
                      SET_BIGGER(merged_stat, left_stat, >, right_stat, get_low_value, double, lowValue);

                      merged_stat.__set_numDVs(std::max(left_stat.numDVs, right_stat.numDVs));
                      merged_stat.__set_numNulls(left_stat.numNulls + right_stat.numNulls);

                      merged.col_stat.__set_doubleStats(std::move(merged_stat));
                      merged.row_count = left_row_count + right_row_count;
                      return merged;
                  })
                  .col_stat;
        return true;
    }
    else if (isset.longStats)
    {
        ret.statsData
            = std::accumulate(
                  start_it,
                  partition_infos.end(),
                  init,
                  [&col_name](
                      const ColumnStatisticsWithRows & left,
                      const std::map<String, const PartitionStatsWithRowNumber &>::value_type right_ref) -> ColumnStatisticsWithRows {
                      const auto & left_stat = left.col_stat.longStats;
                      const auto & right_stat = right_ref.second.col_stats.at(col_name).statsData.longStats;
                      auto left_row_count = left.row_count;
                      auto right_row_count = right_ref.second.row_count;

                      ColumnStatisticsWithRows merged;
                      ApacheHive::LongColumnStatsData merged_stat;
                      auto get_high_value = [](const ApacheHive::LongColumnStatsData & data) { return data.highValue; };
                      auto get_low_value = [](const ApacheHive::LongColumnStatsData & data) { return -data.highValue; };
                      SET_BIGGER(merged_stat, left_stat, >, right_stat, get_high_value, double, highValue);
                      SET_BIGGER(merged_stat, left_stat, >, right_stat, get_low_value, double, lowValue); // inverse the order.
                      merged_stat.__set_numDVs(std::max(left_stat.numDVs, right_stat.numDVs));
                      merged_stat.__set_numNulls(left_stat.numNulls + right_stat.numNulls);

                      merged.col_stat.__set_longStats(std::move(merged_stat));
                      merged.row_count = left_row_count + right_row_count;
                      return merged;
                  })
                  .col_stat;
        return true;
    }
    else if (isset.stringStats)
    {
        ret.statsData
            = std::accumulate(
                  start_it,
                  partition_infos.end(),
                  init,
                  [&col_name](
                      const ColumnStatisticsWithRows & left,
                      const std::map<String, const PartitionStatsWithRowNumber &>::value_type right_ref) -> ColumnStatisticsWithRows {
                      const auto & left_stat = left.col_stat.stringStats;
                      const auto & right_stat = right_ref.second.col_stats.at(col_name).statsData.stringStats;
                      auto left_row_count = left.row_count;
                      auto right_row_count = right_ref.second.row_count;
                      auto left_nonnull_count = left.row_count - left_stat.numNulls;
                      auto right_nonnull_count = right_row_count - right_stat.numNulls;


                      ColumnStatisticsWithRows merged;
                      ApacheHive::StringColumnStatsData merged_stat;
                      merged_stat.__set_maxColLen(std::max(left_stat.maxColLen, right_stat.maxColLen));
                      merged_stat.__set_avgColLen(
                          (left_row_count + right_row_count != 0)
                              ? (left_nonnull_count * left_stat.avgColLen + right_nonnull_count * right_stat.avgColLen)
                                  / (left_nonnull_count + right_nonnull_count)
                              : 0.0);
                      merged_stat.__set_numNulls(left_stat.numNulls + right_stat.numNulls);
                      merged_stat.__set_numDVs(std::max(left_stat.numDVs, right_stat.numDVs));

                      merged.col_stat.__set_stringStats(std::move(merged_stat));
                      merged.row_count = left_row_count + right_row_count;
                      return merged;
                  })
                  .col_stat;
        return true;
    }
    return false;
}

}
