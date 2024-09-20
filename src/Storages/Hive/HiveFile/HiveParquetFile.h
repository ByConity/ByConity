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

    std::optional<size_t> numRows() override;


private:
    std::optional<size_t> num_rows;
};

}
#endif
