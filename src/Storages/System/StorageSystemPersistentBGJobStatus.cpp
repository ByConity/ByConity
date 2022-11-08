#include <Storages/System/StorageSystemPersistentBGJobStatus.h>
#include <Core/NamesAndTypes.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <Catalog/Catalog.h>

namespace DB
{
    NamesAndTypesList StorageSystemPersistentBGJobStatus::getNamesAndTypes()
    {
        return
        {
            {"type", std::make_shared<DataTypeString>()},
            {"uuid", std::make_shared<DataTypeUUID>()},
            {"status", std::make_shared<DataTypeString>()},
        };
    }

    void StorageSystemPersistentBGJobStatus::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
    {
        /// TODO: can optimize further to reduce number of request by checking where condition

        const std::vector<CnchBGThreadType> types {
            CnchBGThreadType::MergeMutate,
            CnchBGThreadType::Clustering,
            CnchBGThreadType::PartGC,
            CnchBGThreadType::Consumer,
            CnchBGThreadType::DedupWorker
        };

        std::shared_ptr<Catalog::Catalog> catalog = context->getCnchCatalog();
        for (CnchBGThreadType type : types)
        {
            std::unordered_map<UUID, CnchBGThreadStatus> statuses = catalog->getBGJobStatuses(type);
            std::for_each(statuses.begin(), statuses.end(),
                [type, & res_columns] (const auto & p)
                {
                    size_t column_num = 0;
                    res_columns[column_num++]->insert(toString(type));
                    res_columns[column_num++]->insert(p.first);
                    res_columns[column_num++]->insert(toString(p.second));
                }
            );
        }
    }
} // end namespace
