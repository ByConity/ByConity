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
#include <CloudServices/CnchRefreshMaterializedViewThread.h>

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
        auto database = refresh.database.empty() ? getContext()->getCurrentDatabase() : refresh.database;
        StoragePtr storage_ptr = DatabaseCatalog::instance().getTable({database, refresh.table}, getContext());
        if (auto * view = dynamic_cast<StorageMaterializedView *>(storage_ptr.get()))
        {
            if (view->async())
            {
                if (getContext()->getSettingsRef().async_mv_refresh_task_submit_to_bg_thread)
                {
                    auto bg_thread = getContext()->tryGetCnchBGThread(CnchBGThreadType::CnchRefreshMaterializedView, view->getStorageID());
                    if (bg_thread)
                    {
                        auto * refresh_thread = dynamic_cast<CnchRefreshMaterializedViewThread *>(bg_thread.get());
                        refresh_thread->start();
                    }
                    else
                    {
                        AsyncRefreshParamPtrs refersh_params = view->getAsyncRefreshParams(getContext(), true);
                        if (!refersh_params.empty())
                            view->refreshAsync(refersh_params[0], getContext());
                    }
                }
                else
                {
                    AsyncRefreshParamPtrs refersh_params = view->getAsyncRefreshParams(getContext(), true);
                    if (!refersh_params.empty())
                        view->refreshAsync(refersh_params[0], getContext());
                }
            }
            else
            {
                if (refresh.where_expr)
                    view->refreshWhere(refresh.where_expr, getContext(), refresh.async);
                else if (refresh.partition)
                    view->refresh(refresh.partition, getContext(), refresh.async);
            }
        }
        else
            throw Exception("Table " + backQuoteIfNeed(database) + backQuoteIfNeed(refresh.table) +
                            " isn't a materialized view, can't be refreshed.", ErrorCodes::LOGICAL_ERROR);
        getContext()->getCnchServerResource()->skipCleanWorker();
        return {};
    }
}
