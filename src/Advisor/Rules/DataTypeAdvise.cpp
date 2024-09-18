#include <Advisor/Rules/DataTypeAdvise.h>

#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/WorkloadTableStats.h>
#include <Core/Types.h>
#include <Core/Field.h>
#include <Core/QualifiedTableName.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Poco/Logger.h>
#include "Interpreters/StorageID.h"
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Statistics/TypeUtils.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

WorkloadAdvises DataTypeAdvisor::analyze(AdvisorContext & context) const
{
    WorkloadAdvises res;
    for (auto & [table_name, workload_table] : context.tables.getTables())
    {
        auto basic_stats = workload_table->getStats().getBasicStats();
        if (!basic_stats.get())
            throw Exception("Empty statistics when analyzing data types for table " + table_name.getFullName(), ErrorCodes::LOGICAL_ERROR);

        auto storage = DatabaseCatalog::instance().getTable(StorageID{table_name.database, table_name.table}, context.session_context);
        auto columns = storage->getInMemoryMetadataPtr()->getColumns().getAll();
        auto extended_stats
            = workload_table->getStats().collectExtendedStats(context.session_context, table_name.database, table_name.table, columns);

        if (!extended_stats.get())
            throw Exception("Empty extended statistics when analyzing data types for table " + table_name.getFullName(), ErrorCodes::LOGICAL_ERROR);

        const auto local_table = workload_table->getTablePtr();
        if (!dynamic_cast<const StorageCnchMergeTree *>(local_table.get()))
            throw Exception("Table " + table_name.getFullName() + " is not merge tree table",  ErrorCodes::UNKNOWN_TABLE);

        UInt64 row_count = basic_stats->getRowCount();
        auto & table_stats = basic_stats->getSymbolStatistics();

        for (auto & [column_name, symbol_stats] : table_stats)
        {
            if (symbol_stats->getNullsCount() == row_count) /// all nulls
                continue;

            const auto & column_type = local_table->getInMemoryMetadataPtr()->getColumns().getPhysical(column_name).type;
            auto decayed_type = Statistics::decayDataType(column_type);

            bool is_string = decayed_type->getTypeId() == TypeIndex::String || decayed_type->getTypeId() == TypeIndex::FixedString;
            bool is_unsigned_integer = decayed_type->isValueRepresentedByUnsignedInteger() && decayed_type->isSummable();
            bool is_integer = decayed_type->isValueRepresentedByInteger() && decayed_type->isSummable();

            String optimized_type;
            if ((is_string && string_type_advisor->checkAndApply(local_table, symbol_stats, extended_stats->at(column_name), row_count, optimized_type))
                || (is_unsigned_integer && integer_type_advisor->checkAndApply(local_table, symbol_stats, decayed_type, true, optimized_type))
                || (is_integer && integer_type_advisor->checkAndApply(local_table, symbol_stats, decayed_type, false, optimized_type)))
            {
                res.emplace_back(std::make_shared<DataTypeAdvise>(table_name, column_name, column_type->getName(), optimized_type));
            }
        }
    }
    return res;
}

bool DataTypeAdvisor::StringTypeAdvisor::checkAndApply(const StoragePtr & local_table, const SymbolStatisticsPtr & symbol_stats, WorkloadExtendedStat & extended_symbol_stats, UInt64 row_count, String & optimized_type)
{
    const auto & nulls_count = symbol_stats->getNullsCount();

    /// check date
    const Field & count_to_date = extended_symbol_stats[WorkloadExtendedStatsType::COUNT_TO_DATE_OR_NULL];
    bool all_date = !count_to_date.isNull() ? count_to_date.get<UInt64>() + nulls_count == row_count : false;
    if (all_date)
    {
        optimized_type = nulls_count > 0 ? "Nullable(Date)" : "Date";
        return true;
    }

    /// check date time
    const Field & count_to_date_time = extended_symbol_stats[WorkloadExtendedStatsType::COUNT_TO_DATE_TIME_OR_NULL];
    bool all_date_time = !count_to_date_time.isNull() ? count_to_date_time.get<UInt64>() + nulls_count == row_count : false;
    if (all_date_time)
    {
        optimized_type = nulls_count > 0 ? "Nullable(DateTime)" : "DateTime";
        return true;
    }

    /// check uint32
    const Field & count_to_uint32 = extended_symbol_stats[WorkloadExtendedStatsType::COUNT_TO_UINT32_OR_NULL];
    bool all_unsigned_integer = !count_to_uint32.isNull() ? count_to_uint32.get<UInt64>() + nulls_count == row_count : false;
    if (all_unsigned_integer)
    {
        optimized_type = nulls_count > 0 ? "Nullable(UInt32)" : "UInt32";
        return true;
    }

    /// check float32
    const Field & count_to_float32 = extended_symbol_stats[WorkloadExtendedStatsType::COUNT_TO_FLOAT32_OR_NULL];
    bool all_float32 = !count_to_float32.isNull() ? count_to_float32.get<UInt64>() + nulls_count == row_count : false;
    if (all_float32)
    {
        optimized_type = nulls_count > 0 ? "Nullable(Float32)" : "Float32";
        return true;
    }

    /// check (global) low cardinality
    const auto & ndv = symbol_stats->getNdv();
    const auto * merge_tree_storage = dynamic_cast<const StorageCnchMergeTree *>(local_table.get());
    bool can_be_inside_low_cardinality = ndv < merge_tree_storage->getSettings()->low_cardinality_ndv_threshold && ndv + nulls_count != row_count;
    if (can_be_inside_low_cardinality)
    {
        String nested_type = nulls_count > 0 ? "Nullable(String)" : "String";
        optimized_type = "LowCardinality(" + nested_type + ")";
        return true;
    }

    /// check fixed string
    const auto & avg_len = symbol_stats->getAvg();
    bool is_fixed_size = false; /// TODO
    if (is_fixed_size)
    {
        optimized_type = nulls_count > 0 ? "Nullable(FixedString("+ toString(avg_len) +"))" : "FixedString(" + toString(avg_len) + ")";
        return true;
    }

    return false;
}

bool DataTypeAdvisor::IntegerTypeAdvisor::checkAndApply(
    [[maybe_unused]] const StoragePtr & local_table,
    const SymbolStatisticsPtr & symbol_stats,
    const DataTypePtr & decayed_original_type,
    bool is_unsigned_type,
    String & optimized_type)
{
    const auto & nulls_count = symbol_stats->getNullsCount();
    const auto & max = symbol_stats->getMax();

    DataTypePtr new_type = nullptr;
    if (is_unsigned_type)
    {
        if (max <= std::numeric_limits<UInt8>::max()) new_type = std::make_shared<DataTypeUInt8>();
        else if (max <= std::numeric_limits<UInt16>::max()) new_type = std::make_shared<DataTypeUInt16>();
        else if (max <= std::numeric_limits<UInt32>::max()) new_type = std::make_shared<DataTypeUInt32>();
    }
    else
    {
        if (max <= std::numeric_limits<Int8>::max()) new_type = std::make_shared<DataTypeInt8>();
        else if (max <= std::numeric_limits<Int16>::max()) new_type = std::make_shared<DataTypeInt16>();
        else if (max <= std::numeric_limits<Int32>::max()) new_type = std::make_shared<DataTypeInt32>();
    }

    if (new_type && new_type->getTypeId() < decayed_original_type->getTypeId())
    {
        optimized_type = nulls_count > 0 ? "Nullable(" + new_type->getName() + ")" : new_type->getName();
        return true;
    }

    return false;
}

}
