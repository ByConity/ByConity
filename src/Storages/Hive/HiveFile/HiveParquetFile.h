#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "Processors/ISource.h"
#include "Storages/Hive/HiveFile/IHiveFile.h"

namespace DB
{
class HiveParquetFile : public IHiveFile
{
public:
    HiveParquetFile();
    ~HiveParquetFile() override;
};

}
#endif
