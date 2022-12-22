#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/System/StorageSystemCnchDictionaries.h>
#include <Common/Status.h>

namespace DB
{
NamesAndTypesList StorageSystemCnchDictionaries::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"definition", std::make_shared<DataTypeString>()},
        {"is_detached", std::make_shared<DataTypeUInt8>()},
        {"is_deleted", std::make_shared<DataTypeUInt8>()},
        {"update_time", std::make_shared<DataTypeDateTime>()}
    };
}

void StorageSystemCnchDictionaries::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & ) const
{
    Catalog::CatalogPtr cnch_catalog = context->getCnchCatalog();

    if (context->getServerType() == ServerType::cnch_server && cnch_catalog)
    {
        Catalog::Catalog::DataModelDictionaries res =
            cnch_catalog->getAllDictionaries();
        for (size_t i = 0, size = res.size(); i != size; ++i)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(res[i].database());
            res_columns[col_num++]->insert(res[i].name());
            res_columns[col_num++]->insert(RPCHelpers::createUUID(res[i].uuid()));
            res_columns[col_num++]->insert(res[i].definition());
            res_columns[col_num++]->insert(Status::isDetached(res[i].status())) ;
            res_columns[col_num++]->insert(Status::isDeleted(res[i].status())) ;
            res_columns[col_num++]->insert(res[i].last_modification_time()) ;
        }
    }
}
}
