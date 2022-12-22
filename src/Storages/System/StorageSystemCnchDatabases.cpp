#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemCnchDatabases.h>
#include <Common/Status.h>

namespace DB
{

NamesAndTypesList StorageSystemCnchDatabases::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUUID>())},
        {"txn_id", std::make_shared<DataTypeUInt64>()},
        {"previous_version", std::make_shared<DataTypeUInt64>()},
        {"commit_time", std::make_shared<DataTypeDateTime>()},
    };
}

void StorageSystemCnchDatabases::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & ) const
{
    Catalog::CatalogPtr cnch_catalog = context->getCnchCatalog();

    if (context->getServerType() == ServerType::cnch_server && cnch_catalog)
    {
        //get CNCH databases
        Catalog::Catalog::DataModelDBs res;
        res = cnch_catalog->getAllDataBases();
        for (size_t i = 0, size = res.size(); i != size; ++i)
        {
            if (!Status::isDeleted(res[i].status()))
            {
                size_t col_num = 0;
                res_columns[col_num++]->insert(res[i].name());
                /// fill database uuid if it exists, otherwise the field will be NULL
                if (res[i].has_uuid())
                    res_columns[col_num++]->insert(RPCHelpers::createUUID(res[i].uuid()));
                else
                    res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insert(res[i].txnid());
                res_columns[col_num++]->insert(res[i].previous_version());
                auto commit_time = (res[i].commit_time() >> 18) ; // first 48 bits represent times 
                res_columns[col_num++]->insert(commit_time/1000) ;// convert to seconds
            }
        }
    }
}

}
