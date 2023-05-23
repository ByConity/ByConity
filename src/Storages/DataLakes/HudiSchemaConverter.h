#pragma once

#include "Common/config.h"

#if USE_JAVA_EXTENSIONS

#include "Storages/StorageInMemoryMetadata.h"

namespace DB
{
namespace Protos { class HudiTable; }

class HudiSchemaConverter
{
public:
    explicit HudiSchemaConverter(std::shared_ptr<Protos::HudiTable> hudi_meta);
    StorageInMemoryMetadata create();

private:
    std::shared_ptr<Protos::HudiTable> table;
};

}

#endif
