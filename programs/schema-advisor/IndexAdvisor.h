#pragma once

#include "MockEnvironment.h"
#include "PotentialColumn.h"

#include <boost/program_options/variables_map.hpp>

#include <Compression/ICompressionCodec.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace po = boost::program_options;

class IndexAdvisor
{
private:
    MockEnvironment & env;
    po::variables_map options;
    const size_t sample_row_number;
    const size_t max_threads;

    PotentialColumns potential_columns;

public:
    IndexAdvisor(
        MockEnvironment & env_,
        const po::variables_map & options_,
        size_t sample_row_number,
        size_t max_threads);

    virtual ~IndexAdvisor() = default;

    void execute();
    void serializeJson(WriteBuffer & buf, bool verbose = false);
};

}
