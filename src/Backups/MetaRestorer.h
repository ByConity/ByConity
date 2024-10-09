#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{
namespace MetaRestorer
{
    void restoreDatabase(const std::shared_ptr<ASTCreateQuery> & create_query, const ContextPtr & context);

    void restoreTable(
        const std::shared_ptr<ASTCreateQuery> & create_query,
        std::optional<std::vector<ASTPtr>> previous_versions_part_columns,
        const ContextPtr & context);
}
}
