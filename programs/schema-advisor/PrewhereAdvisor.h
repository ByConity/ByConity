#pragma once

#include "MockEnvironment.h"
#include "PotentialColumn.h"

#include <boost/program_options/variables_map.hpp>

#include <Compression/ICompressionCodec.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace po = boost::program_options;

class PrewhereAdvisor
{
private:
    MockEnvironment & env;
    po::variables_map options;
    const size_t sample_row_number;
    const size_t max_threads;
    const Float64 mark_filter_threshold;
    const Float64 top_3_mark_filter_threshold;
    const Float64 sample_mark_number;
    // We do not pushdown the column if the field size > 128 bytes
    const size_t column_size_threshold;

    PotentialPrewhereColumns potential_columns;

    Float64 calcMarkFilterRatio(const Field & field) const;
    std::pair<Float64, Float64> calcMarkFilterRatio(const ColumnPtr & column) const;
public:
    PrewhereAdvisor(
        MockEnvironment & env_,
        const po::variables_map & options_,
        size_t sample_row_number_,
        size_t max_threads_,
        Float64 mark_filter_threshold_,
        Float64 top_3_mark_filter_threshold_);

    virtual ~PrewhereAdvisor() = default;

    void execute();
    void serializeJson(WriteBuffer & buf, bool verbose = false);
};

}
