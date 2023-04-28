#include <Interpreters/PlanSegmentHelper.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include "Interpreters/Context_fwd.h"

namespace DB
{

bool PlanSegmentHelper::supportDistributedStages(const ASTPtr & node)
{
    /// TODO: support insert select
    if (node->as<ASTSelectQuery>() || node->as<ASTSelectWithUnionQuery>())
    {
        if (hasJoin(node))
            return true;
    }
    
    return false;
}

bool PlanSegmentHelper::hasJoin(const ASTPtr & node)
{
    if (!node)
        return false;

    if (node->as<ASTTableJoin>())
        return true;
    else
    {
        for (const auto & child: node->children)
        {
            if (hasJoin(child))
                return true;
        }
    }

    return false; 
}

}
