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

#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <ResourceManagement/VirtualWarehouseType.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class Context;
class MergeTreeMetaBase;

class VirtualWarehouseHandleImpl;
using VirtualWarehouseHandle = std::shared_ptr<VirtualWarehouseHandleImpl>;

class WorkerGroupHandleImpl;
using WorkerGroupHandle = std::shared_ptr<WorkerGroupHandleImpl>;
using VirtualWarehouseType = ResourceManagement::VirtualWarehouseType;

const static std::string EMPTY_VIRTUAL_WAREHOUSE_NAME;

bool trySetVirtualWarehouse(const ASTPtr & ast, ContextMutablePtr & context);
bool trySetVirtualWarehouseAndWorkerGroup(const ASTPtr & ast, ContextMutablePtr & context, bool use_router = false);
bool trySetVirtualWarehouseAndWorkerGroup(const std::string & vw_name, ContextMutablePtr & context);
std::string tryGetVirtualWarehouseName(const ASTPtr & ast, ContextMutablePtr & context);

/// Won't set virtual warehouse
VirtualWarehouseHandle getVirtualWarehouseForTable(const MergeTreeMetaBase & storage, const ContextPtr & context);

/// Won't set virtual warehouse or worker group
WorkerGroupHandle getWorkerGroupForTable(const MergeTreeMetaBase & storage, const ContextPtr & context);
WorkerGroupHandle getWorkerGroupForTable(ContextPtr local_context, StoragePtr storage);

}
