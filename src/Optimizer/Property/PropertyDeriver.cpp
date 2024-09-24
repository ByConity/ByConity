/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <memory>
#include <Optimizer/Property/PropertyDeriver.h>

#include <Core/Names.h>
#include <Interpreters/StorageID.h>
#include <MergeTreeCommon/assignCnchParts.h>
#include <Optimizer/DomainTranslator.h>
#include <Optimizer/ExpressionRewriter.h>
#include <Optimizer/ExpressionUtils.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTClusterByElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/ExchangeStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/UnionStep.h>
#include <Poco/StringTokenizer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OPTIMIZER_NONSUPPORT;
}

Property PropertyDeriver::deriveProperty(QueryPlanStepPtr step, ContextMutablePtr & context, const Property & require)
{
    PropertySet property_set;
    return deriveProperty(step, property_set, require, context);
}

Property
PropertyDeriver::deriveProperty(QueryPlanStepPtr step, Property & input_property, const Property & require, ContextMutablePtr & context)
{
    PropertySet input_properties = std::vector<Property>();
    input_properties.emplace_back(input_property);
    auto result = deriveProperty(step, input_properties, require, context);
    if (step->getType() != IQueryPlanStep::Type::Exchange)
    {
        if (result.getNodePartitioning().getComponent() == Partitioning::Component::ANY)
        {
            result.getNodePartitioningRef().setComponent(input_property.getNodePartitioning().getComponent());
        }
    }

    return result;
}

Property PropertyDeriver::deriveProperty(
    QueryPlanStepPtr step, PropertySet & input_properties, const Property & require, ContextMutablePtr & context)
{
    DeriverContext deriver_context{input_properties, require, context};
    DeriverVisitor visitor{};
    auto result = VisitorUtil::accept(step, visitor, deriver_context);
    if (step->getType() != IQueryPlanStep::Type::Exchange)
    {
        if (result.getNodePartitioning().getComponent() == Partitioning::Component::ANY && !input_properties.empty())
        {
            result.getNodePartitioningRef().setComponent(input_properties[0].getNodePartitioning().getComponent());
        }
    }

    return result;
}

static String getClusterByHint(const StoragePtr & storage)
{
    if (auto * merge_tree = dynamic_cast<MergeTreeMetaBase *>(storage.get()))
        return merge_tree->getSettings()->cluster_by_hint.toString();
    return "";
}

Property PropertyDeriver::deriveStorageProperty(const StoragePtr & storage, const Property & required, ContextMutablePtr & context)
{
    if (storage->getDatabaseName() == "system")
    {
        auto node = Partitioning(Partitioning::Handle::SINGLE);
        node.setComponent(Partitioning::Component::COORDINATOR);
        return Property{node, Partitioning(Partitioning::Handle::ARBITRARY)};
    }
    Sorting sorting;
    const auto & descs = storage->getInMemoryMetadataPtr()->sorting_key;

    for (size_t i = 0; i < descs.column_names.size(); i++)
    {
        if (Utils::canIgnoreNullsDirection(descs.data_types[i]))
            sorting.emplace_back(SortColumn(descs.column_names[i], SortOrder::ASC_ANY));
        else
            sorting.emplace_back(SortColumn(descs.column_names[i], SortOrder::ASC_NULLS_FIRST));
    }

    bool use_reverse_sorting = !required.getSorting().empty()
        && (required.getSorting()[0].getOrder() == SortOrder::DESC_ANY || required.getSorting()[0].getOrder() == SortOrder::DESC_NULLS_FIRST
            || required.getSorting()[0].getOrder() == SortOrder::DESC_NULLS_LAST);
    if (use_reverse_sorting)
        sorting = sorting.toReverseOrder();

    auto metadata = storage->getInMemoryMetadataPtr();
    Names cluster_by;
    UInt64 buckets = 0;

    auto normalize_ast = [&](ASTPtr sharding_key) -> std::pair<Names, ASTPtr> {
        SymbolVisitor visitor;
        Names partition_keys;
        SymbolVisitorContext symbol_context;
        ASTVisitorUtil::accept(sharding_key, visitor, symbol_context);

        ConstASTMap expression_map;
        size_t index = 0;
        for (auto symbol : symbol_context.result)
        {
            ASTPtr name = std::make_shared<ASTIdentifier>(symbol);
            ASTPtr id = std::make_shared<ASTIdentifier>("$" + std::to_string(index));
            if (!expression_map.contains(name))
            {
                expression_map[name] = ConstHashAST::make(id);
                partition_keys.emplace_back(symbol);
                index++;
            }
        }

        auto result_ast = ExpressionRewriter::rewrite(sharding_key, expression_map);

        if (auto * cluster_by_ast_element = result_ast->as<ASTClusterByElement>())
        {
            cluster_by_ast_element->children.pop_back();
        }

        return {partition_keys, result_ast};
    };

    ASTPtr ast;
    if (storage->isBucketTable())
    {
        bool clustered = storage->isTableClustered(context);
        if (clustered)
        {
            if (auto cluster_by_hint = getClusterByHint(storage); !cluster_by_hint.empty())
            {
                Poco::StringTokenizer tokenizer(cluster_by_hint, ",", 0x11);
                for (const auto & cluster_by_column : tokenizer)
                    cluster_by.push_back(cluster_by_column);
                buckets = 0;
            }
            else
            {
                auto [columns, rewritten] = normalize_ast(metadata->cluster_by_key.definition_ast);
                cluster_by = columns;
                ast = rewritten;
            }
            buckets = metadata->getBucketNumberFromClusterByKey();
        }
    }

    if (!cluster_by.empty())
    {
#if 0
                NameToNameMap translation;
                auto id_to_table = merge_tree->parseUnderlyingDictionaryTables(merge_tree->settings.underlying_dictionary_tables);
                Names sec_cols;
                for (const auto & item : id_to_table)
                {
                    sec_cols.emplace_back(item.first);
                }
#endif
        return Property{
            Partitioning{
                Partitioning::Handle::BUCKET_TABLE,
                cluster_by,
                true,
                buckets,
                ast,
                true,
                Partitioning::Component::ANY,
                false,
                satisfyBucketWorkerRelation(storage, *context)},
            Partitioning{},
            sorting};
    }

    return Property{Partitioning(Partitioning::Handle::UNKNOWN), Partitioning(Partitioning::Handle::UNKNOWN), sorting};
}

Property PropertyDeriver::deriveStoragePropertyWhatIfMode(
    const StoragePtr & storage, ContextMutablePtr & context, const Property & required_property)
{
    Property actual_storage_property = deriveStorageProperty(storage, required_property, context);

    const auto & table_layout = required_property.getTableLayout();

    if (!table_layout.contains(storage->getStorageID().getQualifiedName()))
        return actual_storage_property;

    auto what_if_table_partitioning = table_layout.at(storage->getStorageID().getQualifiedName());

    if (what_if_table_partitioning.isStarPartitioned()) // use required property to calculate lower bound
        return required_property;

    Names cluster_by{what_if_table_partitioning.getPartitionKey().column};
    // the bucket number is only used for matching, can be set to anything
    UInt64 buckets = (actual_storage_property.getNodePartitioning().getHandle() == Partitioning::Handle::BUCKET_TABLE)
        ? actual_storage_property.getNodePartitioning().getBuckets()
        : context->getSettingsRef().memory_catalog_worker_size;

    Partitioning new_partitioning{
        Partitioning::Handle::BUCKET_TABLE, cluster_by, true, buckets, nullptr, true, Partitioning::Component::ANY};
    actual_storage_property.setNodePartitioning(new_partitioning);

    return actual_storage_property;
}

Property DeriverVisitor::visitStep(const IQueryPlanStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitOffsetStep(const OffsetStep &, DeriverContext & context)
{
    return Property{
        context.getInput()[0].getNodePartitioning(), Partitioning(Partitioning::Handle::SINGLE), context.getInput()[0].getSorting()};
}

Property DeriverVisitor::visitTotalsHavingStep(const TotalsHavingStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitFinishSortingStep(const FinishSortingStep & step, DeriverContext & context)
{
    auto prop = context.getInput()[0];
    Sorting sorting;
    for (auto item : step.getResultDescription())
    {
        sorting.emplace_back(item);
    }

    prop.setSorting(sorting);
    return prop;
}

Property DeriverVisitor::visitPartitionTopNStep(const PartitionTopNStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitBufferStep(const BufferStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitFinalSampleStep(const FinalSampleStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitLocalExchangeStep(const LocalExchangeStep & step, DeriverContext & context)
{
    Property output = context.getInput()[0];
    output.setStreamPartitioning(step.getSchema());
    return output.clearSorting();
}

Property DeriverVisitor::visitIntermediateResultCacheStep(const IntermediateResultCacheStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitProjectionStep(const ProjectionStep & step, DeriverContext & context)
{
    const auto & assignments = step.getAssignments();

    if (!context.getInput()[0].getNodePartitioning().getColumns().empty()
        && context.getContext()->getSettingsRef().enable_injective_in_property)
    {
        for (const auto & item : assignments)
        {
            if (item.second->as<ASTFunction>())
            {
                try
                {
                    auto partition_col = context.getInput()[0].getNodePartitioning().getColumns();
                    NameSet partition_col_set{partition_col.begin(), partition_col.end()};
                    if (FunctionIsInjective::isInjective(
                            item.second, context.getContext(), step.getInputStreams()[0].getNamesAndTypes(), partition_col_set))
                    {
                        auto prop = context.getInput()[0];
                        prop.getNodePartitioningRef().setColumns({item.first});
                        return prop;
                    }
                }
                catch (...)
                {
                }
            }
        }
    }

    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    std::unordered_map<String, String> revert_identifies;

    // TODO(gouguilin): check isBitEngineEncodeDecodeFunction in functions
    // TODO(gouguilin):     when bitengine is ready
    bool has_bitmap_func = false;

    for (auto & item : identities)
    {
        revert_identifies[item.second] = item.first;
    }
    Property translated;
    if (!context.getInput().empty())
    {
        translated = context.getInput()[0].translate(revert_identifies);
    }

    // if partition columns are pruned, the output data has no property.
    if (translated.getNodePartitioning().getColumns().size() != context.getInput()[0].getNodePartitioning().getColumns().size())
    {
        return Property{};
    }

    if (translated.getStreamPartitioning().getColumns().size() != context.getInput()[0].getStreamPartitioning().getColumns().size())
    {
        // TODO stream partition
    }
    if (has_bitmap_func)
    {
        translated.getNodePartitioningRef().setComponent(Partitioning::Component::WORKER);
    }
    return translated;
}

Property DeriverVisitor::visitFilterStep(const FilterStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitJoinStep(const JoinStep & step, DeriverContext & context)
{
    std::unordered_map<String, String> identities;
    for (const auto & item : step.getOutputStream().header)
    {
        identities[item.name] = item.name;
    }

    Property translated;

    if (step.getKind() == ASTTableJoin::Kind::Inner || step.getKind() == ASTTableJoin::Kind::Cross)
    {
        Property left_translated = context.getInput()[0].translate(identities);
        Property right_translated = context.getInput()[1].translate(identities);

        translated = left_translated;

        // if partition columns are pruned, the output data has no property.
        if (translated.getNodePartitioning().getColumns().size() != context.getInput()[0].getNodePartitioning().getColumns().size())
        {
            translated.setNodePartitioning({});
        }
        if (translated.getStreamPartitioning().getColumns().size() != context.getInput()[0].getStreamPartitioning().getColumns().size())
        {
            translated.setStreamPartitioning({});
        }
    }

    if (step.getKind() == ASTTableJoin::Kind::Left || step.getKind() == ASTTableJoin::Kind::Right)
    {
        Property left_translated = context.getInput()[0].translate(identities);
        translated = left_translated;

        // if partition columns are pruned, the output data has no property.
        if (translated.getNodePartitioning().getColumns().size() != context.getInput()[0].getNodePartitioning().getColumns().size())
        {
            translated.setNodePartitioning({});
        }
        if (translated.getStreamPartitioning().getColumns().size() != context.getInput()[0].getStreamPartitioning().getColumns().size())
        {
            // TODO stream partition
        }
    }

    if (step.getKind() == ASTTableJoin::Kind::Full)
    {
        return Property{};
    }

    translated = translated.clearSorting();
    return translated;
}

Property DeriverVisitor::visitArrayJoinStep(const ArrayJoinStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitAggregatingStep(const AggregatingStep &, DeriverContext & context)
{
    auto prop = context.getInput()[0].clearSorting();
    return prop;
}

Property DeriverVisitor::visitMarkDistinctStep(const MarkDistinctStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitMergingAggregatedStep(const MergingAggregatedStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitUnionStep(const UnionStep & step, DeriverContext & context)
{
    Property first_child_property = context.getInput()[0];
    if (first_child_property.getNodePartitioning().getHandle() == Partitioning::Handle::SINGLE)
    {
        bool all_single = true;
        for (const auto & input : context.getInput())
        {
            all_single &= input.getNodePartitioning().getHandle() == Partitioning::Handle::SINGLE;
        }

        if (all_single)
        {
            if (step.isLocal())
            {
                return Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}};
            }
            else
            {
                return Property{Partitioning{Partitioning::Handle::SINGLE}};
            }
        }
    }

    std::vector<Property> transformed_children_prop;
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

    if (first_child_property.getNodePartitioning().getHandle() == Partitioning::Handle::FIXED_HASH
        || first_child_property.getNodePartitioning().getHandle() == Partitioning::Handle::BUCKET_TABLE)
    {
        const Names & keys = first_child_property.getNodePartitioning().getColumns();
        Names output_keys;
        bool match = true;
        bool satisfy_worker = true;
        bool bucket_size_match = true;
        for (auto & transformed : transformed_children_prop)
        {
            if (transformed.getNodePartitioning().getBuckets() != first_child_property.getNodePartitioning().getBuckets())
            {
                bucket_size_match = false;
            }
            transformed.getNodePartitioningRef().setBuckets(0);
            if (!(transformed.getNodePartitioning() == transformed_children_prop[0].getNodePartitioning()))
            {
                match = false;
            }
            satisfy_worker &= transformed.getNodePartitioning().isSatisfyWorker();
        }

        if (!satisfy_worker)
        {
            match &= bucket_size_match;
        }

        if (match && keys.size() == transformed_children_prop[0].getNodePartitioning().getColumns().size())
        {
            output_keys = transformed_children_prop[0].getNodePartitioning().getColumns();
        }
        if (step.isLocal())
        {
            return Property{
                Partitioning{
                    first_child_property.getNodePartitioning().getHandle(),
                    output_keys,
                    true,
                    first_child_property.getNodePartitioning().getBuckets(),
                    first_child_property.getNodePartitioning().getBucketExpr(),
                    first_child_property.getNodePartitioning().isEnforceRoundRobin(),
                    first_child_property.getNodePartitioning().getComponent(),
                    false,
                    satisfy_worker},
                Partitioning{Partitioning::Handle::SINGLE}};
        }
        else
        {
            return Property{Partitioning{
                first_child_property.getNodePartitioning().getHandle(),
                output_keys,
                true,
                first_child_property.getNodePartitioning().getBuckets(),
                first_child_property.getNodePartitioning().getBucketExpr(),
                first_child_property.getNodePartitioning().isEnforceRoundRobin(),
                first_child_property.getNodePartitioning().getComponent(),
                false,
                satisfy_worker}};
        }
    }
    return Property{};
}

Property DeriverVisitor::visitExceptStep(const ExceptStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitIntersectStep(const IntersectStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitIntersectOrExceptStep(const IntersectOrExceptStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitExchangeStep(const ExchangeStep & step, DeriverContext & context)
{
    const ExchangeMode & mode = step.getExchangeMode();
    if (mode == ExchangeMode::GATHER)
    {
        Property output = context.getInput()[0];
        output.setNodePartitioning(Partitioning{Partitioning::Handle::SINGLE});
        return output.clearSorting();
    }

    if (mode == ExchangeMode::REPARTITION)
    {
        Property output = context.getInput()[0];
        output.setNodePartitioning(step.getSchema());
        output.setStreamPartitioning(step.getSchema());
        return output.clearSorting();
    }

    if (mode == ExchangeMode::BROADCAST)
    {
        Property output = context.getInput()[0];
        output.setNodePartitioning(Partitioning{Partitioning::Handle::FIXED_BROADCAST});
        return output.clearSorting();
    }

    if (mode == ExchangeMode::LOCAL_NO_NEED_REPARTITION)
    {
        Property output = context.getInput()[0];
        return output.clearSorting();
    }

    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitRemoteExchangeSourceStep(const RemoteExchangeSourceStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitTableScanStep(const TableScanStep & step, DeriverContext & context)
{
    NameToNameMap translation;
    for (const auto & item : step.getColumnAlias())
        translation.emplace(item.first, item.second);

    if (!context.getRequire().getTableLayout().empty())
        return PropertyDeriver::deriveStoragePropertyWhatIfMode(step.getStorage(), context.getContext(), context.getRequire())
            .translate(translation);

    return PropertyDeriver::deriveStorageProperty(step.getStorage(), context.getRequire(), context.getContext()).translate(translation);
}

Property DeriverVisitor::visitReadNothingStep(const ReadNothingStep &, DeriverContext &)
{
    return Property{Partitioning(Partitioning::Handle::SINGLE), Partitioning(Partitioning::Handle::ARBITRARY)};
}

Property DeriverVisitor::visitReadStorageRowCountStep(const ReadStorageRowCountStep &, DeriverContext &)
{
    return Property{Partitioning(Partitioning::Handle::SINGLE), Partitioning(Partitioning::Handle::ARBITRARY)};
}

Property DeriverVisitor::visitValuesStep(const ValuesStep &, DeriverContext &)
{
    return Property{Partitioning(Partitioning::Handle::SINGLE), Partitioning(Partitioning::Handle::ARBITRARY)};
}

Property DeriverVisitor::visitLimitStep(const LimitStep &, DeriverContext & context)
{
    return Property{
        context.getInput()[0].getNodePartitioning(), Partitioning(Partitioning::Handle::SINGLE), context.getInput()[0].getSorting()};
}

Property DeriverVisitor::visitLimitByStep(const LimitByStep &, DeriverContext & context)
{
    return context.getInput()[0].withStreamPartitioning(Partitioning{Partitioning::Handle::SINGLE});
}

Property DeriverVisitor::visitSortingStep(const SortingStep & step, DeriverContext & context)
{
    auto prop = context.getInput()[0];
    Sorting sorting;
    for (auto item : step.getSortDescription())
    {
        sorting.emplace_back(item);
    }
    prop.setSorting(sorting);
    return prop;
}

Property DeriverVisitor::visitMergeSortingStep(const MergeSortingStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitPartialSortingStep(const PartialSortingStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitMergingSortedStep(const MergingSortedStep &, DeriverContext & context)
{
    return Property{context.getInput()[0].getNodePartitioning(), Partitioning(Partitioning::Handle::SINGLE)};
}

Property DeriverVisitor::visitDistinctStep(const DistinctStep & step, DeriverContext & context)
{
    auto result = context.getInput()[0];
    result.clearSorting();
    if (!step.preDistinct())
    {
        result.setStreamPartitioning(Partitioning{Partitioning::Handle::SINGLE});
    }
    return result;
}

Property DeriverVisitor::visitExtremesStep(const ExtremesStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitWindowStep(const WindowStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting().withStreamPartitioning({});
}

Property DeriverVisitor::visitApplyStep(const ApplyStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitEnforceSingleRowStep(const EnforceSingleRowStep &, DeriverContext & context)
{
    return context.getInput()[0].withStreamPartitioning(Partitioning{Partitioning::Handle::SINGLE});
}

Property DeriverVisitor::visitAssignUniqueIdStep(const AssignUniqueIdStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitCTERefStep(const CTERefStep & cte_step, DeriverContext & context)
{
    if (context.getInput().size() != 1)
        throw Exception("Input porporties should be set for cte property derive", ErrorCodes::LOGICAL_ERROR);
    auto prop = context.getInput()[0];
    return prop.translate(cte_step.getReverseOutputColumns());
}

Property DeriverVisitor::visitExplainAnalyzeStep(const ExplainAnalyzeStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitTopNFilteringStep(const TopNFilteringStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitFillingStep(const FillingStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitTableWriteStep(const TableWriteStep &, DeriverContext & context)
{
    auto prop = context.getInput()[0];
    prop.getNodePartitioningRef().setComponent(Partitioning::Component::WORKER);
    if (context.getInput()[0].getNodePartitioning().getHandle() == Partitioning::Handle::SINGLE)
    {
        prop.getNodePartitioningRef().setComponent(Partitioning::Component::COORDINATOR);
    }
    return prop;
}

Property DeriverVisitor::visitTableFinishStep(const TableFinishStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitOutfileWriteStep(const OutfileWriteStep &, DeriverContext & context)
{
    auto prop = context.getInput()[0];
    prop.getNodePartitioningRef().setComponent(Partitioning::Component::WORKER);
    if (context.getInput()[0].getNodePartitioning().getHandle() == Partitioning::Handle::SINGLE)
    {
        prop.getNodePartitioningRef().setComponent(Partitioning::Component::COORDINATOR);
    }
    return prop;
}

Property DeriverVisitor::visitOutfileFinishStep(const OutfileFinishStep &, DeriverContext & context)
{
    return context.getInput()[0];
}


Property DeriverVisitor::visitMultiJoinStep(const MultiJoinStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitExpandStep(const ExpandStep &, DeriverContext & context)
{
    auto prop = context.getInput()[0].clearSorting();
    prop.getNodePartitioningRef().resetIfPartitionHandle();
    prop.getStreamPartitioningRef().resetIfPartitionHandle();
    return prop;
}

}
