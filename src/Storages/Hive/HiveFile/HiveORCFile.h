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

    std::optional<size_t> numRows() override;

    SourcePtr getReader(const Block & block, const std::shared_ptr<IHiveFile::ReadParams> & params) override;

private:
    std::optional<size_t> num_rows;
};
}

#endif
