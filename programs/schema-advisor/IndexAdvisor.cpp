#include "IndexAdvisor.h"
#include "ColumnUsageExtractor.h"
#include "IO/WriteIntText.h"
#include "SampleColumnReader.h"
#include "Statistics.h"
#include "SchemaAdvisorHelpers.h"
#include "PotentialColumn.h"

#include <iostream>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include "Common/Exception.h"
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

static constexpr double ADVISOR_HIGH_CARDINALITY_NDV_THRESHOLD = 0.33;

IndexAdvisor::IndexAdvisor(
    MockEnvironment & env_,
    const po::variables_map & options_,
    size_t sample_row_number_,
    size_t max_threads_)
    : env(env_)
    , options(options_)
    , sample_row_number(sample_row_number_)
    , max_threads(max_threads_)
{
}

// High_Cardinality_Threshold: 
//      20w / 65536 = sample_row_number / ndv => ndv ～ 1/3 sample_row_number
// For skip index, ndv > High_Cardinality_Threshold
// For bitmap index, 10 < ndv <= High_Cardinality_Threshold
bool checkColumnCardinality(String column_name, size_t ndv, size_t sample_row_number, PotentialIndexType & type)
{
    bool basic_cardinality = ndv > 10;
    bool high_cardinality = ndv > ADVISOR_HIGH_CARDINALITY_NDV_THRESHOLD * sample_row_number;

    auto get_ndv_check_msg = [&]() -> String
    {
        if (type == PotentialIndexType::BITMAP_INDEX)
        {
            if (!basic_cardinality)
                return fmt::format("Column {} skipped because the ndv ({}) is less than 10", column_name, ndv);
            if (high_cardinality)
                type = PotentialIndexType::SEGMENT_BITMAP_INDEX;
        }
        if (type == PotentialIndexType::BLOOM_FILTER)
        {
            if (!high_cardinality)
                return fmt::format("Column {} skipped because of the array ndv({}) / sample_rows({}) < threshold({})", column_name, ndv, sample_row_number, ADVISOR_HIGH_CARDINALITY_NDV_THRESHOLD);
        }
        return "";
    };

    auto check_ndv_msg = get_ndv_check_msg();
    if (!check_ndv_msg.empty())
    {
        LOG_DEBUG(getLogger("ColumnUsageExtractor"), check_ndv_msg);
        return false;
    }
    return true;
};

void IndexAdvisor::execute()
{
    auto context = createContext(options, env);
    auto queries = loadQueries(options);

    LOG_DEBUG(getLogger("ColumnUsageExtractor"), "++++++++++ begin to executor index advisor ++++++++++");

    ColumnUsageExtractor extractor(context, max_threads);
    auto column_usages = extractor.extractColumnUsages(queries);
    auto skip_index_usages = extractor.extractUsageForSkipIndex(column_usages);

    auto make_predicate_info = [&](PredicateInfos & predicate_infos, ColumnUsageType predicate_type, const std::vector<ColumnUsage> & column_usages_) {
        PredicateExpressions predicate_expressions;
        size_t total_predicate_expression = 0;
        for (const auto & equality_usage : column_usages_)
        {
            auto & count = predicate_expressions[equality_usage.expression];
            ++count;
            total_predicate_expression++;
        }
        predicate_infos.insert({predicate_type, {predicate_expressions, total_predicate_expression}});
    };

    UniExtract uniq_extract;
    for (const auto & index_usage : skip_index_usages)
    {
        auto column_info = index_usage.first;

        auto storage = MockEnvironment::tryGetLocalTable(column_info.database, column_info.table, context);
        if (!storage)
            throw Exception(column_info.database + "(" + column_info.table + "): can not find local table.", ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);

        auto metadata = storage->getInMemoryMetadataCopy();
        auto column_and_type = metadata.getColumns().tryGetColumn(GetColumnsOptions::Kind::AllPhysical, column_info.column);
        if (!column_and_type)
            continue;

        auto column_type = column_and_type->type;

        bool check_bitmap_index = false;
        bool already_bitmap_index = false;
        if (isArray(column_type))
        {
            if (column_type->isBitmapIndex() || column_type->isSegmentBitmapIndex())
            {
                LOG_DEBUG(getLogger("ColumnUsageExtractor"), "Column " + column_info.column + " skipped because has already been a bitmap index column");
                // continue;
                already_bitmap_index = true;
            }
            check_bitmap_index = true;
        }

        std::vector<std::string> data_path_list;
        // if (options.count("path"))
        // {
        //     std::string path = options["path"].as<std::string>();
        //     if (!endsWith(path, "/"))
        //         path.append("/");
        //     data_path_list.emplace_back(path);
        // }
        // else
        // {            
            boost::split(data_path_list, options["data-path-list"].as<std::string>(), boost::is_any_of(" ,"));
            for (auto & i : data_path_list)
            {
                if (!endsWith(i, "/"))
                    i = i.append("/");
            }
        // }

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
                        getLogger("ColumnUsageExtractor"),
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
            column = reader.readColumn({index_usage.first.column, column_type});
        } 
        catch (...)
        {   
            // Just skip the column if it can't be read
            LOG_DEBUG(
                getLogger("ColumnUsageExtractor"),
                "Can't read column file " + index_usage.first.column + " from table " + column_info.database + "." + column_info.table
                        + ", error message: "
                    + getCurrentExceptionMessage(true));
            continue;
        }

        if (check_bitmap_index)
        {
            size_t ndv = uniq_extract.executeOnColumnArray(column, column_type).get<UInt64>();
            auto bitmap_index_type = already_bitmap_index ? PotentialIndexType::ALREADY_BITMAP_INDEX : PotentialIndexType::BITMAP_INDEX;
            if (!checkColumnCardinality(column_info.getFullName(), ndv, sample_row_number, bitmap_index_type))
                continue;

            StatisticInfo statistic_info{ndv, sample_row_number, std::get<2>(index_usage.second)};

            PredicateInfos predicate_infos;
            make_predicate_info(predicate_infos, ColumnUsageType::ARRAY_SET_FUNCTION, std::get<0>(index_usage.second));

            PotentialColumnInfo potential_column{bitmap_index_type, statistic_info, predicate_infos};
            potential_columns.insert({std::move(column_info), std::move(potential_column)});

            continue;
        }
        
        // All following: check skip index 
        size_t ndv = uniq_extract.executeOnColumn(column, column_type).get<UInt64>();
        auto skip_index_type = PotentialIndexType::BLOOM_FILTER;
        if (!checkColumnCardinality(column_info.getFullName(), ndv, sample_row_number, skip_index_type))
            continue;

        StatisticInfo statistic_info{ndv, sample_row_number, std::get<2>(index_usage.second)};

        PredicateInfos predicate_infos;
        make_predicate_info(predicate_infos, ColumnUsageType::EQUALITY_PREDICATE, std::get<0>(index_usage.second));
        make_predicate_info(predicate_infos, ColumnUsageType::IN_PREDICATE, std::get<1>(index_usage.second));

        PotentialColumnInfo potential_column{skip_index_type, statistic_info, predicate_infos};
        potential_columns.insert({std::move(column_info), std::move(potential_column)});
    }

    LOG_DEBUG(getLogger("ColumnUsageExtractor"), "Extracted {} column usages", potential_columns.size());
    for ([[maybe_unused]] auto & [column_info, potential_column] : potential_columns)
    {
        std::stringstream ss;
        ss << column_info.getFullName() << "\tindex_type:" << toString(potential_column.index_type)
           << "\tsample_ndv:" << potential_column.statistic_info.sample_ndv
           << "\tsample_row_num:" << potential_column.statistic_info.sample_row_num << "\ttarget_expression_cnt:"
           << potential_column.predicate_infos[ColumnUsageType::EQUALITY_PREDICATE].total
                + potential_column.predicate_infos[ColumnUsageType::IN_PREDICATE].total
                + potential_column.predicate_infos[ColumnUsageType::ARRAY_SET_FUNCTION].total
           << "\ttotal_expr count:" << potential_column.statistic_info.total_predicates;

        LOG_DEBUG(getLogger("ColumnUsageExtractor"), ss.str());
    }
}

/// TODO: seperate two indices
void IndexAdvisor::serializeJson(WriteBuffer & buf, bool /* verbose */)
{
    bool first = true;
    writeString("\"index\":[", buf);
    for (auto & [column_info, potential_column] : potential_columns)
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

        writeString(R"(","index_type":")", buf);
        writeString(toString(potential_column.index_type), buf);

        writeString(R"(","sample_ndv":")", buf);
        writeIntText(potential_column.statistic_info.sample_ndv, buf);
        writeString(R"(","sample_row_num":")", buf);
        writeIntText(potential_column.statistic_info.sample_row_num, buf);

        // The usage type (EQUALITY_PREDICATE + IN_PREDICATE) and (ARRAY_SET_FUNCTION)
        // will not appear at the same time, so we can simply add the cnt
        size_t target_expression_cnt = potential_column.predicate_infos[ColumnUsageType::EQUALITY_PREDICATE].total
            + potential_column.predicate_infos[ColumnUsageType::IN_PREDICATE].total
            + potential_column.predicate_infos[ColumnUsageType::ARRAY_SET_FUNCTION].total;
        writeString(R"(","target_expression_cnt":")", buf);
        writeIntText(target_expression_cnt, buf);
        writeString(R"(","total_expression_cnt":")", buf);
        writeIntText(potential_column.statistic_info.total_predicates, buf);
        writeString("\"}", buf);
    }
    writeString("]", buf);
}

}
