#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "Storages/Hive/HiveFile/IHiveFile.h"

namespace DB
{
class HiveORCFile : public IHiveFile
{
public:
    HiveORCFile();
    ~HiveORCFile() override;

};
}

#endif
