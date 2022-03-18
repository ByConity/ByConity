#pragma once

#include <Common/Exception.h>
#include <Poco/Zip/ZipLocalFileHeader.h>
#include <Poco/Delegate.h>
#include <string>


namespace DB
{
#define TEMP_SUFIX ".tmp"

using DelegatePair = std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string>;

/***
 * Helper class for compressing/uncompression part files when interact with HDFS. Please note that 
 * we choose a algorithm with low compress rate to speed up processing. Do not realy on this for data compression.
 */

class ZipHelper
{
public:
    using PocoDelegate = Poco::Delegate<ZipHelper, DelegatePair>;

    static void zipFile(const std::string & source, const std::string & target);
    void unzipFile(const std::string & source, const std::string & target);

private:
    void setSource(const std::string & source);
    
    [[noreturn]]		
    void onDecompressError(const void* , DelegatePair & info);

    std::string source_file;
};

}
