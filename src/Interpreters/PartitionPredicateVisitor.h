#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST_fwd.h>
namespace DB
{

struct PartitionPredicateMatcher
{
    struct Data : public WithContext
    {
        Data(ContextPtr context_, Names match_names_) : WithContext(context_), match_names(std::move(match_names_))
        {
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
