#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>

#include <utility>

namespace DB
{
PlanNodeStatistics::PlanNodeStatistics(UInt64 row_count_, std::unordered_map<String, SymbolStatisticsPtr> symbol_statistics_)
    : row_count(row_count_), symbol_statistics(std::move(symbol_statistics_))
{
}

SymbolStatisticsPtr PlanNodeStatistics::getSymbolStatistics(const String & symbol)
{
    if (symbol_statistics.contains(symbol))
    {
        return symbol_statistics[symbol];
    }
    return SymbolStatistics::UNKNOWN;
}

UInt64 PlanNodeStatistics::getOutputSizeInBytes() const
{
    size_t row_size = 0;
    for (auto & symbols : symbol_statistics)
    {
        if (!symbols.second->isUnknown())
        {
            row_size += symbols.second->getOutputSizeInBytes();
        }
    }
    return row_size * row_count;
}

String PlanNodeStatistics::toString() const
{
    std::stringstream details;
    details << "RowCount: " << row_count << "\\n";
    details << "DataSize: " << std::to_string(getOutputSizeInBytes()) << "\\n";
    details << "Symbol\\n";
    for (auto & symbol : symbol_statistics)
    {
        details << symbol.first << ": " << symbol.second->getNdv() << ", " << symbol.second->getMin() << ", " << symbol.second->getMax()
                << ", hist:" << symbol.second->getHistogram().getBuckets().size() << "\\n";
    }
    return details.str();
}

Poco::JSON::Object::Ptr PlanNodeStatistics::toJson() const
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);
    json->set("rowCont", row_count);

    Poco::JSON::Array symbol_statistics_json_array;
    for (auto & item : symbol_statistics)
    {
        Poco::JSON::Object::Ptr symbol_statistics_json = new Poco::JSON::Object;
        symbol_statistics_json->set("symbol", item.first);
        symbol_statistics_json->set("statistics", item.second->toJson());
        symbol_statistics_json_array.add(symbol_statistics_json);
    }

    json->set("symbolStatistics", symbol_statistics_json_array);
    return json;
}

}
