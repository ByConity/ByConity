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

#include "DDLCreateAction.h"

#include <Catalog/Catalog.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

namespace DB
{

void DDLCreateAction::executeV1(TxnTimestamp commit_time)
{
    Catalog::CatalogPtr cnch_catalog = global_context.getCnchCatalog();

    if (params.is_database)
    {
        /// create database
        assert(!params.attach);
        cnch_catalog->createDatabase(params.storage_id.database_name, params.storage_id.uuid, txn_id, commit_time, params.statement, params.engine_name);
    }
    else if (!params.is_dictionary)
    {
        /// create table
        /// updateTsCache(params.storage_id.uuid, commit_time);
        if (params.attach)
            cnch_catalog->attachTable(params.storage_id.database_name, params.storage_id.table_name, commit_time);
        else
            cnch_catalog->createTable(params.storage_id, params.statement, "", txn_id, commit_time);
    }
    else
    {
        /// for dictionary
        if (params.attach)
            cnch_catalog->attachDictionary(params.storage_id.database_name, params.storage_id.table_name);
        else
            cnch_catalog->createDictionary(params.storage_id, params.statement);
    }
}

void DDLCreateAction::abort() {}

}
