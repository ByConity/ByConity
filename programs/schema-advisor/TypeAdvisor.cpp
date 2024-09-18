#include "TypeAdvisor.h"
#include "ColumnUsageExtractor.h"
#include "Core/Types.h"
#include "SampleColumnReader.h"
#include "Statistics.h"

#include <iostream>
#include <boost/algorithm/string/join.hpp>
#include <boost/program_options.hpp>

#include <Common/ThreadPool.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/MapHelpers.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

namespace DB
{

TypeAdvisor::TypeAdvisor(
    MockEnvironment & env_,
    const po::variables_map & options_,
    const ColumnsDescription & column_descs_,
    std::string absolute_part_path_,
    size_t sample_row_number_,
    size_t max_threads_,
    bool lc_only_,
    Float64 scanned_count_threshold_for_lc_,
    Float64 cardinality_ratio_threshold_for_lc_)
    : env(env_)
    , options(options_)
    , column_descs(column_descs_)
    , absolute_part_path(absolute_part_path_ + "/")
    , sample_row_number(sample_row_number_)
    , max_threads(max_threads_)
    , lc_only(lc_only_)
    , scanned_count_threshold_for_lc(scanned_count_threshold_for_lc_)
    , cardinality_ratio_threshold_for_lc(cardinality_ratio_threshold_for_lc_)
{
    parseCodecCandidates();
}

void TypeAdvisor::parseCodecCandidates()
{

}

DataTypePtr decayDataType(DataTypePtr type)
{
    if (type->isNullable())
        return dynamic_cast<const DataTypeNullable *>(type.get())->getNestedType();
    return type;
}

TypeAdvisor::TypeRecommendation buildTypeRecommendation(std::string column_name, std::string origin_type, bool is_type_nullable, std::string optimized_type)
{
    return {column_name, is_type_nullable ? "Nullable(" + origin_type + ")" : origin_type, is_type_nullable ? "Nullable(" + optimized_type + ")" : optimized_type};
}

void TypeAdvisor::adviseLowCardinality()
{
    auto context = createContext(options, env);
    auto queries = loadQueries(options);

    ColumnUsageExtractor extractor(context, max_threads);
    auto column_usages = extractor.extractColumnUsages(queries);
    auto type_usages = extractor.extractUsageForLowCardinality(column_usages);

    LOG_DEBUG(getLogger("TypeAdvisor"), "Extract {} candidate columns, {}, {}", type_usages.size(), scanned_count_threshold_for_lc, cardinality_ratio_threshold_for_lc);

    UniExtract uniq_extract;
    for (const auto & type_usage : type_usages)
    {
        if (type_usage.second < queries.size() * scanned_count_threshold_for_lc)
        {
            LOG_DEBUG(getLogger("TypeAdvisor"), "Do not Recommend lowcardinality column {}, scanned count is {}", type_usage.first.column, type_usage.second);
            continue;
        }

        auto column_info = type_usage.first;
        if (isMapImplicitKey(column_info.column))
            continue;

        auto storage = MockEnvironment::tryGetLocalTable(column_info.database, column_info.table, context);
        if (!storage)
            throw Exception(column_info.database + "(" + column_info.table + "): can not find local table.", ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);

        auto metadata = storage->getInMemoryMetadataCopy();
        auto column_and_type = metadata.getColumns().tryGetColumn(GetColumnsOptions::Kind::AllPhysical, column_info.column);
        if (!column_and_type)
            continue;

        auto column_type = column_and_type->type;
        if (column_type->getTypeId() == TypeIndex::LowCardinality || !isString(decayDataType(column_type)))
            continue;

        SampleColumnReader reader(absolute_part_path + "/", 0, sample_row_number);
        ColumnPtr column;
        try
        {
            column = reader.readColumn({type_usage.first.column, column_type});
        } 
        catch (...)
        {   
            // Just skip the column if it can't be read
            LOG_DEBUG(
                getLogger("TypeAdvisor"),
                "Can't read column file " + type_usage.first.column + " from table " + column_info.database + "." + column_info.table
                        + ", error message: "
                    + getCurrentExceptionMessage(true));
            continue;
        }
        
        // All following: check skip index 
        size_t ndv = uniq_extract.executeOnColumn(column, column_type).get<UInt64>();

        if (ndv > sample_row_number * cardinality_ratio_threshold_for_lc)
        {
            LOG_DEBUG(getLogger("TypeAdvisor"), "Do not Recommend lowcardinality column {}, scanned count is {}, ndv is {}", type_usage.first.column, type_usage.second, ndv);
            continue;
        }

        LOG_DEBUG(getLogger("TypeAdvisor"), "Recommend lowcardinality column {}, scanned count is {}, ndv is {}", type_usage.first.column, type_usage.second, ndv);

        type_recommendations.push_back({column_and_type->name
                                              , column_and_type->type->isNullable() ? "Nullable(String)" : "String"
                                              , column_and_type->type->isNullable() ? "LowCardinality(Nullable(String))" : "LowCardinality(String)"});}

}

void TypeAdvisor::execute()
{
    if (lc_only)
        return adviseLowCardinality();

    UniExtract uniqExtractFunc;
    Max maxFunc;
    Min minFunc;
    SampleColumnReader reader(absolute_part_path, 0, sample_row_number);
    for (const NameAndTypePair & name_and_type : column_descs.getOrdinary())
    {
        auto decayed_type = decayDataType(name_and_type.type);

        bool is_string = decayed_type->getTypeId() == TypeIndex::String;
        bool is_float_64 = decayed_type->getTypeId() == TypeIndex::Float64;
        bool is_unsigned_integer = decayed_type->isValueRepresentedByUnsignedInteger() && decayed_type->isSummable();
        bool is_integer = decayed_type->isValueRepresentedByInteger() && decayed_type->isSummable();

        if (is_string)
        {
            ColumnPtr column = reader.readColumn(name_and_type);
            auto ndv = uniqExtractFunc.executeOnColumn(column, name_and_type.type).get<UInt64>();
            if (ndv < ADVISOR_LOW_CARDINALITY_NDV_THRESHOLD)
                type_recommendations.push_back({name_and_type.name
                                              , name_and_type.type->isNullable() ? "Nullable(String)" : "String"
                                              , name_and_type.type->isNullable() ? "LowCardinality(Nullable(String))" : "LowCardinality(String)"});
        }
        else if (is_float_64)
        {
            ColumnPtr column = reader.readColumn(name_and_type);
            auto max = maxFunc.executeOnColumn(column, name_and_type.type).get<Float64>();
            auto min = minFunc.executeOnColumn(column, name_and_type.type).get<Float64>();
            if (min >= std::numeric_limits<Float32>::min() && max <= std::numeric_limits<Float32>::max())
                type_recommendations.push_back({name_and_type.name
                                              , name_and_type.type->isNullable() ? "Nullable(Float64)" : "Float64"
                                              , name_and_type.type->isNullable() ? "Nullable(Float32)" : "Float32"});
        }
        else if (is_unsigned_integer)
        {
            if (decayed_type->getTypeId() == TypeIndex::UInt8) /// skip UInt8
                continue;

            ColumnPtr column = reader.readColumn(name_and_type);
            auto max = maxFunc.executeOnColumn(column, name_and_type.type).get<UInt64>();
            if (max <= std::numeric_limits<UInt8>::max())
                type_recommendations.push_back(buildTypeRecommendation(name_and_type.name, decayed_type->getName(), name_and_type.type->isNullable(), "UInt8"));
            else if (max <= std::numeric_limits<UInt16>::max())
                type_recommendations.push_back(buildTypeRecommendation(name_and_type.name, decayed_type->getName(), name_and_type.type->isNullable(), "UInt16"));
            else if (max <= std::numeric_limits<UInt32>::max())
                type_recommendations.push_back(buildTypeRecommendation(name_and_type.name, decayed_type->getName(), name_and_type.type->isNullable(), "UInt32"));
        }
        else if (is_integer)
        {
            if (decayed_type->getTypeId() == TypeIndex::Int8) /// skip Int8
                continue;

            ColumnPtr column = reader.readColumn(name_and_type);
            auto max = maxFunc.executeOnColumn(column, name_and_type.type).get<Int64>();
            auto min = minFunc.executeOnColumn(column, name_and_type.type).get<Int64>();
            if (min >= std::numeric_limits<Int8>::min() && max <= std::numeric_limits<Int8>::max())
                type_recommendations.push_back(buildTypeRecommendation(name_and_type.name, decayed_type->getName(), name_and_type.type->isNullable(), "Int8"));
            else if (min >= std::numeric_limits<Int16>::min() && max <= std::numeric_limits<Int16>::max())
                type_recommendations.push_back(buildTypeRecommendation(name_and_type.name, decayed_type->getName(), name_and_type.type->isNullable(), "Int16"));
            else if (min >= std::numeric_limits<Int32>::min() && max <= std::numeric_limits<Int32>::max())
                type_recommendations.push_back(buildTypeRecommendation(name_and_type.name, decayed_type->getName(), name_and_type.type->isNullable(), "Int32"));
        }
        /// TODO(weiping.qw): add more rules
    }
}

void TypeAdvisor::serializeJson(WriteBuffer & buf, [[maybe_unused]] bool verbose)
{
    bool first = true;
    writeString("\"type\":[", buf);
    for (const auto & entry : type_recommendations)
    {
        if (first)
            first = false;
        else
            writeString(",", buf);
        std::string column_name = entry.column_name;
        writeString("{\"name\":\"", buf);
        writeString(column_name, buf);
        writeString("\",", buf);
        std::string column_origin_type = entry.origin_type;
        std::string column_optimized_type = entry.optimized_type;
        writeString("\"origin\":\"", buf);
        writeString(column_origin_type, buf);
        writeString("\",", buf);
        writeString("\"optimized\":\"", buf);
        writeString(column_optimized_type, buf);
        writeString("\"}", buf);
    }
    writeString("]", buf);
}

}
