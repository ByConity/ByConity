#include <QueryPlan/planning_models.h>

namespace DB
{

std::optional<String> FieldSymbolInfo::tryGetSubColumnSymbol(const SubColumnID & sub_column_id) const
{
    if (auto it = sub_column_symbols.find(sub_column_id); it != sub_column_symbols.end())
        return it->second;

    return std::nullopt;
}

RelationPlan RelationPlan::withNewRoot(PlanNodePtr new_root) const
{
    return {std::move(new_root), field_symbol_infos};
}

const String & RelationPlan::getFirstPrimarySymbol() const
{
    return field_symbol_infos.front().getPrimarySymbol();
}

}
