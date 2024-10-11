#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemNexusFS.h>
#include <Storages/NexusFS/NexusFS.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemNexusFS::getNamesAndTypes()
{
    return {
        {"sub_file_path", std::make_shared<DataTypeString>()},
        {"total_size", std::make_shared<DataTypeUInt64>()},
        {"cached_size", std::make_shared<DataTypeUInt64>()},
        {"total_segments", std::make_shared<DataTypeUInt64>()},
        {"cached_segments", std::make_shared<DataTypeUInt64>()},
    };
}


StorageSystemNexusFS::StorageSystemNexusFS(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

void StorageSystemNexusFS::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto nexus_fs = context->getNexusFS();
    if (!nexus_fs)
        return;
    auto files = nexus_fs->getFileCachedStates();
    for (const auto & file : files)
    {
        res_columns[0]->insert(file.file_path);
        res_columns[1]->insert(file.total_size);
        res_columns[2]->insert(file.cached_size);
        res_columns[3]->insert(file.total_segments);
        res_columns[4]->insert(file.cached_segments);
    }
}

}
