#pragma once

#include <boost/program_options.hpp>

#include "SchemaAdvisorHelpers.h"

#include <Compression/ICompressionCodec.h>
#include <IO/WriteBuffer.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace po = boost::program_options;

class TypeAdvisor
{
public:
    struct TypeRecommendation
    {
        TypeRecommendation(
            std::string column_name_,
            std::string origin_type_,
            std::string optimized_type_
        ) : column_name(column_name_)
          , origin_type(origin_type_)
          , optimized_type(optimized_type_) {}

        std::string column_name;
        std::string origin_type;
        std::string optimized_type;
    };

private:
    static constexpr const size_t ADVISOR_LOW_CARDINALITY_NDV_THRESHOLD = 65535;

    MockEnvironment & env;
    po::variables_map options;
    const ColumnsDescription column_descs;
    Codecs codecs_to_compare;
    std::string absolute_part_path;
    const size_t sample_row_number;
    [[maybe_unused]] const size_t max_threads;
    std::vector<TypeRecommendation> type_recommendations;
    const bool lc_only;
    const Float64 scanned_count_threshold_for_lc;
    const Float64 cardinality_ratio_threshold_for_lc;

    void parseCodecCandidates();

    void adviseLowCardinality();

public:
    TypeAdvisor(
        MockEnvironment & env_,
        const po::variables_map & options_,
        const ColumnsDescription & column_descs_,
        std::string absolute_part_path_,
        size_t sample_row_number_,
        size_t max_threads_,
        bool lc_only_,
        Float64 scanned_count_threshold_for_lc_,
        Float64 cardinality_ratio_threshold_for_lc_);

    virtual ~TypeAdvisor() = default;

    void execute();
    void serializeJson(WriteBuffer & buf, bool verbose = false);
};

}
