#include "PrewhereAdvisor.h"
#include "ColumnUsageExtractor.h"
#include "Columns/IColumn.h"
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include "Core/Field.h"
#include "IO/WriteIntText.h"
#include "QueryPlan/PlanSerDerHelper.h"
#include "SampleColumnReader.h"
#include "Statistics.h"
#include "SchemaAdvisorHelpers.h"
#include "PotentialColumn.h"

#include <cstddef>
#include <iostream>
#include <utility>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include "Common/Exception.h"
#include "common/types.h"
#include <Common/ThreadPool.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>

namespace DB
{

PrewhereAdvisor::PrewhereAdvisor(
    MockEnvironment & env_,
    const po::variables_map & options_,
    size_t sample_row_number_,
    size_t max_threads_,
    Float64 mark_filter_threshold_,
    Float64 top_3_mark_filter_threshold_)
    : env(env_)
    , options(options_)
    , sample_row_number(sample_row_number_)
    , max_threads(max_threads_)
    , mark_filter_threshold(mark_filter_threshold_)
    , top_3_mark_filter_threshold(top_3_mark_filter_threshold_) 
    , sample_mark_number(static_cast<Float64>(sample_row_number_) / 8192)
    , column_size_threshold(128 * sample_row_number_)
{
}

Float64 PrewhereAdvisor::calcMarkFilterRatio(const Field & field) const
{
    const auto & array_field = DB::safeGet<Array>(field);

    size_t total_marks = 0;
    for (const auto & tuple_field : array_field)
    {
        total_marks += DB::safeGet<UInt64>(DB::safeGet<Tuple>(tuple_field)[1]);
    }

    Float64 avg_mark_occurrence_rate = static_cast<Float64>(total_marks) / array_field.size();

    return avg_mark_occurrence_rate / sample_mark_number;
}

std::pair<Float64, Float64> PrewhereAdvisor::calcMarkFilterRatio(const ColumnPtr & column) const
{
    const auto * array_column = typeid_cast<const ColumnArray *>(column.get());
    const auto * tuple_column = typeid_cast<const ColumnTuple *>((array_column->getDataPtr()).get());
    const auto * mark_count_column = typeid_cast<const ColumnUInt64 *>(tuple_column->getColumns()[1].get());

    size_t total_marks = 0;
    std::priority_queue<UInt64, std::vector<UInt64>, std::greater<UInt64>> top_3_mark_pq;
    for (size_t i = 0; i < mark_count_column->size(); i++)
    {   
        auto current_mark = mark_count_column->get64(i);
        total_marks += current_mark;

        if (top_3_mark_pq.size() < 3) 
        {
            top_3_mark_pq.push(current_mark);
        } 
        else if (current_mark > top_3_mark_pq.top()) 
        {
            top_3_mark_pq.pop();
            top_3_mark_pq.push(current_mark);
        }
    }
    size_t queue_size = top_3_mark_pq.size();

    size_t top_3_mark_sum = 0;
    while(!top_3_mark_pq.empty())
    {
        top_3_mark_sum += top_3_mark_pq.top();
        top_3_mark_pq.pop();
    }

    Float64 avg_mark_occurrence_rate = static_cast<Float64>(total_marks) / mark_count_column->size();
    Float64 top_3_mark_occurrence_rate = static_cast<Float64>(top_3_mark_sum) / queue_size;

    return std::make_pair(avg_mark_occurrence_rate / sample_mark_number, top_3_mark_occurrence_rate / sample_mark_number);
}

void PrewhereAdvisor::execute()
{
    auto context = createContext(options, env);
    auto queries = loadQueries(options);

    LOG_DEBUG(getLogger("PrewhereAdvisor"), "++++++++++ begin to executor prewhere advisor ++++++++++");

    ColumnUsageExtractor extractor(context, max_threads);
    auto column_usages = extractor.extractColumnUsages(queries);
    auto prewhere_usages = extractor.extractUsageForPrewhere(column_usages);

    LOG_DEBUG(getLogger("PrewhereAdvisor"), "Extracted {} prewhere_usages usages for prewhere", prewhere_usages.size());

    CountByGranularity count_by_granularity;

    for (const auto & prewhere_usage : prewhere_usages)
    {
        auto column_info = prewhere_usage.first;

        auto storage = MockEnvironment::tryGetLocalTable(column_info.database, column_info.table, context);
        if (!storage)
            throw Exception(column_info.database + "(" + column_info.table + "): can not find local table.", ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);

        auto metadata = storage->getInMemoryMetadataCopy();
        auto column_and_type = metadata.getColumns().tryGetColumn(GetColumnsOptions::Kind::AllPhysical, column_info.column);
        if (!column_and_type)
            continue;

        auto column_type = column_and_type->type;

        if (isArray(column_type))
            continue;

        std::vector<std::string> data_path_list;
      
        boost::split(data_path_list, options["data-path-list"].as<std::string>(), boost::is_any_of(" ,"));
        for (auto & path : data_path_list)
        {
            if (!endsWith(path, "/"))
                path = path.append("/");
        }

        std::string absolute_part_path;
        try
        {
            absolute_part_path = selectPartPath(options, data_path_list, storage->getStorageID().getDatabaseName(), storage->getStorageID().getTableName(), sample_row_number);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART)
            {
                LOG_DEBUG(
                        getLogger("PrewhereAdvisor"),
                        "Can't find suitable part for table " + column_info.database + "." + column_info.table
                            + ", maybe because of the total part rows < " + std::to_string(sample_row_number));
                continue;
            }
            else
                throw e;
        }

        SampleColumnReader reader(absolute_part_path + "/", 0, sample_row_number);
        ColumnPtr column;
        try
        {
            column = reader.readColumn({prewhere_usage.first.column, column_type});
        } 
        catch (...)
        {   
            // Just skip the column if it can't be read
            LOG_DEBUG(
                getLogger("PrewhereAdvisor"),
                "Can't read column file " + prewhere_usage.first.column + " from table " + column_info.database + "." + column_info.table
                        + ", error message: "
                    + getCurrentExceptionMessage(true));
            continue;
        }

        std::pair<Float64, Float64> mark_filter_pair;
        try
        {
            mark_filter_pair  = calcMarkFilterRatio(count_by_granularity.executeOnColumn(column, column_type));
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::BAD_ARGUMENTS)
            {
                LOG_DEBUG(
                        getLogger("PrewhereAdvisor"), "Error while calculate mark filter ratio, error message: " + e.message());
                continue;
            }
            else
                throw e;
        }

        LOG_DEBUG(getLogger("PrewhereAdvisor"), "Column {} mark filter ratio is {}, top-3 mark filter ratio is {}, column size is {} MB", column_info.column, mark_filter_pair.first, mark_filter_pair.second, column->byteSize()/1000000);

        if (((mark_filter_pair.first <= mark_filter_threshold && mark_filter_pair.second <= top_3_mark_filter_threshold) || (mark_filter_pair.first < 0.1 && mark_filter_pair.second < 0.76)) && column->byteSize() < column_size_threshold)
            potential_columns.insert({std::move(column_info), mark_filter_pair});
    }
}

/// TODO: seperate two indices
void PrewhereAdvisor::serializeJson(WriteBuffer & buf, bool /* verbose */)
{
    bool first = true;
    writeString("\"prewhere\":[", buf);
    for (auto & [column_info, mark_filter_ratio] : potential_columns)
    {
        if (first)
            first = false;
        else
            writeString(",", buf);

        writeString(R"({"db":")", buf);
        writeString(column_info.database, buf);
        writeString(R"(","table":")", buf);
        writeString(column_info.table, buf);
        writeString(R"(","column_name":")", buf);
        writeString(column_info.column, buf);

        writeString(R"(","mark_filter_ratio":")", buf);
        writeString(toString(mark_filter_ratio.first), buf);
        writeString(R"(","top_3_mark_filter_ratio":")", buf);
        writeString(toString(mark_filter_ratio.second), buf);

        writeString("\"}", buf);
    }
    writeString("]", buf);
}

}
