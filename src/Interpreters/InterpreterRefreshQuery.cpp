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

#include <CloudServices/CnchServerResource.h>
#include <Interpreters/InterpreterRefreshQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTRefreshQuery.h>
#include <Storages/StorageMaterializedView.h>
#include <Interpreters/InterpreterSetQuery.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }

    BlockIO InterpreterRefreshQuery::execute()
    {
        const auto & refresh = query_ptr->as<ASTRefreshQuery &>();
        if (refresh.settings_ast)
            InterpreterSetQuery(refresh.settings_ast, getContext()).executeForCurrentContext();
        StoragePtr storage_ptr = DatabaseCatalog::instance().getTable({refresh.database, refresh.table}, getContext());
        if (auto * view = dynamic_cast<StorageMaterializedView *>(storage_ptr.get()))
        {
            view->refresh(refresh.partition, getContext(), refresh.async);
        }
        else
        {
            String db_str = refresh.database.empty() ? "" : backQuoteIfNeed(refresh.database) + ".";
            throw Exception("Table " + db_str + backQuoteIfNeed(refresh.table) +
                            " isn't a materialized view, can't be refreshed.", ErrorCodes::LOGICAL_ERROR);
        }
        getContext()->getCnchServerResource()->skipCleanWorker();
        return {};
    }
}
