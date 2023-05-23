#include <Storages/DataLakes/HudiSchemaConverter.h>
#include <hudi.pb.h>
#include <Poco/Logger.h>
#include "Core/NamesAndTypes.h"
#include "Storages/Hive/HiveSchemaConverter.h"
#include "Storages/StorageInMemoryMetadata.h"
#include "common/logger_useful.h"

namespace DB
{
HudiSchemaConverter::HudiSchemaConverter(std::shared_ptr<Protos::HudiTable> hudi_meta) : table(std::move(hudi_meta))
{
}

StorageInMemoryMetadata HudiSchemaConverter::create()
{
    StorageInMemoryMetadata metadata;
    NamesAndTypesList names_types;
    for (const auto & hive_col : table->columns())
    {
        auto data_type = HiveSchemaConverter::hiveTypeToCHType(hive_col.type(), true);
        if (data_type)
            names_types.emplace_back(NameAndTypePair{hive_col.name(), data_type});
    }

    metadata.setColumns(ColumnsDescription(names_types));
    /// TODO: set partition key column

    LOG_DEBUG(&Poco::Logger::get("HudiSchemaConverter"), "converted column {}", metadata.getColumns().toString());
    return metadata;
}

}
