#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Interpreters/QueryLog.h>

namespace DB
{

void IInterpreterUnionOrSelectQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const
{
    elem.query_kind = "Select";
}

void IInterpreterUnionOrSelectQuery::setQuota(QueryPipeline & pipeline) const
{
    std::shared_ptr<const EnabledQuota> quota;

    if (!options.ignore_quota && (options.to_stage == QueryProcessingStage::Complete))
        quota = context->getQuota();

    pipeline.setQuota(quota);
}

std::set<StorageID> IInterpreterUnionOrSelectQuery::getUsedStorageIDs() const
{
    return used_storage_ids;
}

void IInterpreterUnionOrSelectQuery::addUsedStorageIDs(const std::set<StorageID> & used_storage_ids_)
{
    used_storage_ids.insert(used_storage_ids_.begin(), used_storage_ids_.end());
}

void IInterpreterUnionOrSelectQuery::addUsedStorageID(const StorageID & storage_id)
{
    used_storage_ids.insert(storage_id);
}

void IInterpreterUnionOrSelectQuery::setHasAllUsedStorageIDs(bool val)
{
    has_all_used_storage_ids = val;
}

bool IInterpreterUnionOrSelectQuery::hasAllUsedStorageIDs() const
{
    return has_all_used_storage_ids;
}

}
