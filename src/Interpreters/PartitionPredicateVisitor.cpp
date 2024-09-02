#include <Functions/FunctionFactory.h>
#include <Interpreters/PartitionPredicateVisitor.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/KeyDescription.h>

namespace DB
{
void PartitionPredicateMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (data.match.has_value())
        return;
    if (data.match_names.end() != std::find(data.match_names.begin(), data.match_names.end(), ast->getColumnName()))
    {
        if (!data.match.has_value())
            data.match.emplace(true);
        return;
    }

    const auto * function = ast->as<ASTFunction>();
    if (function)
    {
        if (function->name == "arrayJoin" || function->is_window_function || function->name == "or" || function->name == "xor"
            || KeyDescription::moduloToModuloLegacyRecursive(ast->clone()))
        {
            data.match.emplace(false);
            return;
        }

        const auto & function_builder = FunctionFactory::instance().tryGet(function->name, data.getContext());
        if (function_builder)
        {
            if (function_builder->isStateful() || !function_builder->isDeterministicInScopeOfQuery())
            {
                data.match.emplace(false);
                return;
            }
        }
    }

    if (ast->as<ASTIdentifier>())
    {
        data.match.emplace(false);
        return;
    }

    for (const auto & child : ast->children)
        if (auto * subquery = child->as<ASTSubquery>(); !subquery)
            visit(child, data);
}
}
