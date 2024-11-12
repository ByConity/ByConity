#pragma once

#include <Common/Logger.h>
#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/WorkloadTableStats.h>
#include <Core/Types.h>
#include <Core/QualifiedTableName.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Poco/Logger.h>
#include <Storages/IStorage_fwd.h>

#include <memory>
#include <optional>

namespace DB
{

class DataTypeAdvisor : public IWorkloadAdvisor
{
public:
    DataTypeAdvisor()
    {
        string_type_advisor = std::unique_ptr<StringTypeAdvisor>();
        integer_type_advisor = std::unique_ptr<IntegerTypeAdvisor>();
    }
    String getName() const override { return "DataTypeAdvisor"; }
    WorkloadAdvises analyze(AdvisorContext & context) const override;

private:
    class StringTypeAdvisor
    {
    public:
        bool checkAndApply(const StoragePtr & local_table, const SymbolStatisticsPtr & symbol_stats, WorkloadExtendedStat & extended_symbol_stats, UInt64 row_count, String & optimized_type);
    };

    class IntegerTypeAdvisor
    {
    public:
        bool checkAndApply(const StoragePtr & local_table, const SymbolStatisticsPtr & symbol_stats, const DataTypePtr & decayed_original_type, bool is_unsigned_type, String & optimized_type);
    };

    std::unique_ptr<StringTypeAdvisor> string_type_advisor;
    std::unique_ptr<IntegerTypeAdvisor> integer_type_advisor;
};

class DataTypeAdvise : public IWorkloadAdvise
{
public:
    DataTypeAdvise(
        const QualifiedTableName & table_, const String & column_name_, const String & original_type_, const String & new_type_)
        : table(table_), column_name(column_name_), original_type(original_type_), new_type(new_type_)
    {
    }

    String apply([[maybe_unused]] WorkloadTables & tables) override
    {
        /// TODO: modify ddl
        return "";
    }

    QualifiedTableName getTable() override { return table; }
    std::optional<String> getColumnName() override { return {column_name}; }
    String getAdviseType() override { return "Data Type"; }
    String getOriginalValue() override { return original_type; }
    String getOptimizedValue() override { return new_type; }

private:
    QualifiedTableName table;
    String column_name;
    String original_type;
    String new_type;

    const LoggerPtr log = getLogger("DataTypeAdvise");
};

}
