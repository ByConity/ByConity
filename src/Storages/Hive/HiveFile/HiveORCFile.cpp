#include "Storages/Hive/HiveFile/HiveORCFile.h"
#if USE_HIVE

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

HiveORCFile::HiveORCFile() = default;
HiveORCFile::~HiveORCFile() = default;

};

#endif
