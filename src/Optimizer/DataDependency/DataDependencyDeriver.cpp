#include <algorithm>
#include <iterator>
#include <Core/Names.h>
#include <Interpreters/StorageID.h>
#include <Optimizer/DataDependency/DataDependencyDeriver.h>
#include <Optimizer/DataDependency/ForeignKeysTuple.h>
#include <Optimizer/DataDependency/FunctionalDependency.h>
#include <Optimizer/DataDependency/InclusionDependency.h>
#include <Optimizer/DomainTranslator.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Storages/ForeignKeysDescription.h>
#include <Storages/StorageDistributed.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/DataDependency/DataDependency.h>
#include <Optimizer/DataDependency/DependencyUtils.h>
#include <Optimizer/SymbolUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <QueryPlan/Assignment.h>

namespace DB
{

DataDependency DataDependencyDeriver::deriveDataDependency(
    QueryPlanStepPtr step, CTEInfo & cte_info, ContextMutablePtr & context)
{
    DataDependencyVector property_set;
    return deriveDataDependency(step, property_set, cte_info, context);
}

DataDependency DataDependencyDeriver::deriveDataDependency(
    QueryPlanStepPtr step, DataDependency & input_property, CTEInfo & cte_info, ContextMutablePtr & context)
{
    DataDependencyVector input_data_dependencies;
    input_data_dependencies.emplace_back(input_property);
    return deriveDataDependency(step, input_data_dependencies, cte_info, context);
}

DataDependency DataDependencyDeriver::deriveDataDependency(
    QueryPlanStepPtr step,
    DataDependencyVector & input_data_dependencies,
    CTEInfo & cte_info,
    ContextMutablePtr & context)
{
    DataDependencyDeriverContext deriver_context{input_data_dependencies, cte_info, context};
    static DataDependencyDeriverVisitor visitor{};
    return VisitorUtil::accept(step, visitor, deriver_context);
}

DataDependency
DataDependencyDeriver::deriveStorageDataDependency(const StoragePtr & storage, ContextMutablePtr &)
{
    FunctionalDependencies functional_dependencies;
    InclusionDependency inclusion_dependency;

    // fill functioonal_dependencies
    std::vector<Names> unique_columns = storage->getInMemoryMetadataPtr()->getUniqueNotEnforced().getUniqueNames();
    Names all_columns = storage->getInMemoryMetadataPtr()->getColumns().getAll().getNames();

    for (const auto & unique_names : unique_columns)
    {
        for (const auto & dependent_column : all_columns)
        {
            std::unordered_set us(unique_names.begin(), unique_names.end());
            if (!us.contains(dependent_column))
                functional_dependencies.update(FunctionalDependency{us, {dependent_column}});
        }
    }

    // fill inclusion_dependency using pk
    for (const auto & unique_names : unique_columns)
    {
        if (unique_names.size() == 1)
        {
            inclusion_dependency.emplace(
                unique_names[0], std::pair<bool, String>{false, storage->getStorageID().getTableName() + '.' + unique_names[0]});
        }
    }

    // fill inclusion_dependency using fk
    auto fk_tuples = storage->getInMemoryMetadataPtr()->getForeignKeys().getForeignKeysTuple();
    for (const auto & fk_tuple : fk_tuples)
    {
        inclusion_dependency.emplace(
            fk_tuple.fk_column_name, std::pair<bool, String>{true, fk_tuple.ref_table_name + '.' + fk_tuple.ref_column_name});
    }

    return DataDependency{functional_dependencies, inclusion_dependency};
}

DataDependency DataDependencyDeriverVisitor::visitStep(const IQueryPlanStep &, DataDependencyDeriverContext & context)
{
    if (!context.getInput().empty())
        return context.getInput()[0].clearFunctionalDependency();
    return DataDependency{};
}

DataDependency DataDependencyDeriverVisitor::visitProjectionStep(const ProjectionStep & step, DataDependencyDeriverContext & context)
{
    const auto & assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    std::unordered_map<String, String> revert_identifies;

    for (auto & item : identities)
    {
        revert_identifies[item.second] = item.first;
    }

    DataDependency translated;

    if (!context.getInput().empty())
        translated = context.getInput()[0].translate(revert_identifies);
    
    translated.getFunctionalDependenciesRef().eraseNotExist(step.getOutputStream().header.getNames());

    return translated;
}

DataDependency DataDependencyDeriverVisitor::visitTableScanStep(const TableScanStep & step, DataDependencyDeriverContext & context)
{
    NameToNameMap translation;
    for (const auto & item : step.getColumnAlias())
        translation.emplace(item.first, item.second);

    return DataDependencyDeriver::deriveStorageDataDependency(step.getStorage(), context.getContext())
        .translate(translation);
}

DataDependency DataDependencyDeriverVisitor::visitJoinStep(const JoinStep & step, DataDependencyDeriverContext & context)
{
    std::unordered_map<String, String> identities;
    for (const auto & item : step.getOutputStream().header)
    {
        identities[item.name] = item.name;
    }

    DataDependency translated;
    if (step.getKind() == ASTTableJoin::Kind::Inner && !step.getLeftKeys().empty())
    {
        // functional dependency:
        DataDependency left_translated = context.getInput()[0].translate(identities);
        DataDependency right_translated = context.getInput()[1].translate(identities);
        translated.setFunctionalDependencies(left_translated.getFunctionalDependenciesRef() | right_translated.getFunctionalDependenciesRef());

        // inclusion dependency:
        translated.setInclusionDependency(left_translated.getInclusionDependencyRef() | right_translated.getInclusionDependencyRef());
        // remove all pk info if fitler has side-effect.
        std::erase_if(translated.getInclusionDependencyRef(), [&](const auto & pair) { return !pair.second.first; });
    }

    return translated;
}

DataDependency DataDependencyDeriverVisitor::visitFilterStep(const FilterStep & step, DataDependencyDeriverContext & context)
{
    // new functional dependency cases:
    // a->b, b->a when a = b in filter.

    auto data_dependency = context.getInput()[0];
    if (step.getFilter())
    {
        // functional dependency, TODO@lijinzhi.zx: use in join::filter?
        NamesAndFunctions equal_funcs = CollectIncludeFunction::collect(step.getFilter(), {"equals"}, context.getContext());
        for (const auto & pair : equal_funcs)
        {
            auto function = pair.second->as<ASTFunction &>();
            if (function.arguments->getChildren().size() == 2)
            {
                if (auto * arg1 = function.arguments->getChildren()[0]->as<ASTIdentifier>())
                {
                    if (auto * arg2 = function.arguments->getChildren()[1]->as<ASTIdentifier>())
                    {
                        data_dependency.getFunctionalDependenciesRef().update(
                            FunctionalDependency({arg1->name()}, {arg2->name()}));
                        data_dependency.getFunctionalDependenciesRef().update(
                            FunctionalDependency({arg2->name()}, {arg1->name()}));
                    }
                }
            }
        }
    
        // inclusion dependency:
        // remove all pk info if fitler has side-effect.
        if (!PredicateUtils::isTruePredicate(step.getFilter()))
            std::erase_if(data_dependency.getInclusionDependencyRef(), [&](const auto & pair) { return !pair.second.first; });
    }


    return data_dependency;
}

DataDependency DataDependencyDeriverVisitor::visitAggregatingStep(const AggregatingStep & step, DataDependencyDeriverContext & context)
{
    if (step.getKeys().empty() || !step.isNormal())
        return context.getInput()[0].clearFunctionalDependency();
    
    auto data_dependency = context.getInput()[0];
    NameSet group_by_keys(step.getKeys().begin(), step.getKeys().end());

    std::erase_if(data_dependency.getInclusionDependencyRef(), [&](const auto & pair) { return !group_by_keys.contains(pair.first) && !pair.second.first; });

    return data_dependency;
}

DataDependency DataDependencyDeriverVisitor::visitUnionStep(const UnionStep & step, DataDependencyDeriverContext & context)
{
    std::vector<DataDependency> transformed_children_prop;
    const auto & output_to_inputs = step.getOutToInputs();
    size_t index = 0;
    for (const auto & child_prop : context.getInput())
    {
        NameToNameMap mapping;
        for (const auto & output_to_input : output_to_inputs)
        {
            mapping[output_to_input.second[index]] = output_to_input.first;
        }
        index++;
        transformed_children_prop.emplace_back(child_prop.translate(mapping));
    }


    DataDependency result = transformed_children_prop[0];
    for (size_t i = 1; i < transformed_children_prop.size(); i++)
    {
        result.setFunctionalDependencies(result.getFunctionalDependenciesRef() | transformed_children_prop[i].getFunctionalDependenciesRef());
        result.setInclusionDependency(result.getInclusionDependencyRef() | transformed_children_prop[i].getInclusionDependencyRef());
    }

    return result;
}

DataDependency DataDependencyDeriverVisitor::visitExchangeStep(const ExchangeStep &, DataDependencyDeriverContext & context)
{
    return context.getInput()[0];
}

DataDependency DataDependencyDeriverVisitor::visitLimitStep(const LimitStep &, DataDependencyDeriverContext & context)
{
    return context.getInput()[0];
}

DataDependency DataDependencyDeriverVisitor::visitSortingStep(const SortingStep &, DataDependencyDeriverContext & context)
{
    return context.getInput()[0];
}

DataDependency DataDependencyDeriverVisitor::visitCTERefStep(const CTERefStep & step, DataDependencyDeriverContext & context)
{
    if (context.getInput().empty())
        return DataDependency{};

    std::unordered_map<String, String> revert_identifies;
    for (const auto & item : step.getOutputColumns())
    {
        revert_identifies[item.second] = item.first;
    }

    return context.getInput()[0].translate(revert_identifies);
}

}
