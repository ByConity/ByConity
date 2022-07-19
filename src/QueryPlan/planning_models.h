#pragma once

#include <Core/Types.h>
#include <Analyzers/Analysis.h>
#include <Analyzers/SubColumnID.h>
#include <QueryPlan/PlanNode.h>

namespace DB
{

struct FieldSymbolInfo
{
    using SubColumnToSymbol = std::unordered_map<SubColumnID, String, SubColumnID::Hash>;

    String primary_symbol;
    SubColumnToSymbol sub_column_symbols;

    FieldSymbolInfo(String primary_symbol_ = ""): primary_symbol(std::move(primary_symbol_)) // NOLINT(google-explicit-constructor)
    {}

    FieldSymbolInfo(String primary_symbol_, SubColumnToSymbol sub_column_symbols_)
        : primary_symbol(std::move(primary_symbol_)), sub_column_symbols(std::move(sub_column_symbols_))
    {}

    const String & getPrimarySymbol() const
    {
        return primary_symbol;
    }

    std::optional<String> tryGetSubColumnSymbol(const SubColumnID & sub_column_id) const;
};

using FieldSymbolInfos = std::vector<FieldSymbolInfo>;

struct RelationPlan
{
    PlanNodePtr root;
    FieldSymbolInfos field_symbol_infos;

    RelationPlan() = default;
    RelationPlan(PlanNodePtr root_, FieldSymbolInfos field_symbol_infos_)
        : root(std::move(root_)), field_symbol_infos(std::move(field_symbol_infos_))
    {
    }

    RelationPlan withNewRoot(PlanNodePtr new_root) const;
    PlanNodePtr getRoot() const { return root; }
    const FieldSymbolInfos & getFieldSymbolInfos() const { return field_symbol_infos; }
    const String & getFirstPrimarySymbol() const;
};

using RelationPlans = std::vector<RelationPlan>;

using CTERelationPlans = std::unordered_map<CTEId, RelationPlan>;
}
