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

}
