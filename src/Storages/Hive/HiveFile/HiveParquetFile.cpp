#include "Storages/Hive/HiveFile/HiveParquetFile.h"
#if USE_HIVE

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

HiveParquetFile::HiveParquetFile() = default;
HiveParquetFile::~HiveParquetFile() = default;

}

#endif
