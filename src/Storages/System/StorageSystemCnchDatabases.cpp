/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Access/ContextAccess.h>
#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemCnchCommon.h>
#include <Storages/System/StorageSystemCnchDatabases.h>
#include <Transaction/TxnTimestamp.h>
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
    auto cnch_catalog = context->tryGetCnchCatalog();
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);
    if (context->getServerType() == ServerType::cnch_server && cnch_catalog)
    {
        //get CNCH databases
        Catalog::Catalog::DataModelDBs res;
        res = cnch_catalog->getAllDataBases();
        const String & tenant_id = context->getTenantId();

        for (size_t i = 0, size = res.size(); i != size; ++i)
        {
            if (!Status::isDeleted(res[i].status()))
            {
                size_t col_num = 0;
                std::optional<String> stripped_database_name = filterAndStripDatabaseNameIfTenanted(tenant_id, res[i].name());
                if (!stripped_database_name.has_value())
                    continue;

                if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, res[i].name()))
                    continue;

                res_columns[col_num++]->insert(*std::move(stripped_database_name));
                /// fill database uuid if it exists, otherwise the field will be NULL
                if (res[i].has_uuid())
                    res_columns[col_num++]->insert(RPCHelpers::createUUID(res[i].uuid()));
                else
                    res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insert(res[i].txnid());
                res_columns[col_num++]->insert(res[i].previous_version());
                res_columns[col_num++]->insert(TxnTimestamp(res[i].commit_time()).toSecond());
            }
        }
    }
}

}
