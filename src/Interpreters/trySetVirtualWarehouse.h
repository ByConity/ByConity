#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

class Context;
class MergeTreeMetaBase;

class VirtualWarehouseHandleImpl;
using VirtualWarehouseHandle = std::shared_ptr<VirtualWarehouseHandleImpl>;

class WorkerGroupHandleImpl;
using WorkerGroupHandle = std::shared_ptr<WorkerGroupHandleImpl>;

bool trySetVirtualWarehouse(const ASTPtr & ast, Context & context);
bool trySetVirtualWarehouseAndWorkerGroup(const ASTPtr & ast, Context & context);

/// Won't set virtual warehouse
VirtualWarehouseHandle getVirtualWarehouseForTable(const MergeTreeMetaBase & storage, const Context & context);

/// Won't set virtual warehouse or worker group
WorkerGroupHandle getWorkerGroupForTable(const MergeTreeMetaBase & storage, const Context & context);

}
