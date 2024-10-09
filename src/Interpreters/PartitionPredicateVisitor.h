#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace DB
{

struct PartitionPredicateMatcher
{
    struct Data : public WithContext
    {
        Data(ContextPtr context_, StoragePtr storage, const ASTPtr & filter) : WithContext(context_)
        {
            std::optional<bool> valid = MergeTreeDataSelectExecutor::isValidPartitionFilter(storage, filter, context_);
            if (valid.has_value())
                match.emplace(*valid);
            else
            {
                match_names = storage->getInMemoryMetadataPtr()->getPartitionKey().column_names;
                Names virtual_key_names = storage->getVirtuals().getNames();
                match_names.insert(match_names.end(), virtual_key_names.begin(), virtual_key_names.end());
            }
        }
        bool getMatch() const
        {
            return match.has_value() && *match;
        }

        Names match_names;
        std::optional<bool> match = std::nullopt;
    };

    static void visit(const ASTPtr & ast, Data & data);

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        // traverse logic is handled in the `visit` method
        return false;
    }
};

using PartitionPredicateVisitor = ConstInDepthNodeVisitor<PartitionPredicateMatcher, true>;
}
