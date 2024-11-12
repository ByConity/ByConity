#pragma once

#include <boost/program_options.hpp>

#include "SchemaAdvisorHelpers.h"

#include <Compression/ICompressionCodec.h>
#include <IO/WriteBuffer.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace po = boost::program_options;

class CodecAdvisor
{
private:
    SamplingColumnFiles column_files_to_sample;
    Codecs codecs_to_compare;
    const std::string absolute_part_path;
    const size_t sample_row_number;
    const size_t max_threads;
    unsigned block_size;

    void parseCodecCandidates(const po::variables_map & options);
    void setSamplingColumnFiles(const std::string & part_path, const ColumnsDescription & column_descs);

public:
    CodecAdvisor(
        const po::variables_map & options,
        const ColumnsDescription & column_descs,
        std::string absolute_part_path,
        size_t sample_row_number,
        size_t max_threads);

    virtual ~CodecAdvisor() = default;

    void execute();
    void serializeJson(WriteBuffer & buf, bool verbose = false);
};

}
