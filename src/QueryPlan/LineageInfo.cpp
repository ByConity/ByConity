#include <memory>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <QueryPlan/LineageInfo.h>

#include <Columns/ColumnNullable.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Storages/StorageDistributed.h>

namespace DB
{
void LineageInfoVisitor::visitPlanNode(PlanNodeBase & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitPartitionTopNNode(PartitionTopNNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitRemoteExchangeSourceNode(RemoteExchangeSourceNode &, LineageInfoContext &)
{
    throw Exception("Not impl LineageInfo", ErrorCodes::NOT_IMPLEMENTED);
}

void LineageInfoVisitor::visitOffsetNode(OffsetNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitBufferNode(BufferNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitFinishSortingNode(FinishSortingNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitISourceNodeWithoutStorage(
    PlanNodeBase & node, LineageInfoContext & lineage_info_context, DatabaseAndTableName source_table)
{
    const auto * step = node.getStep().get();
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    SourceInfo new_expression_or_value_source_info;
    if (!source_table.first.empty())
        new_expression_or_value_source_info.source_tables.emplace_back(source_table);
    for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
        outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitReadNothingNode(ReadNothingNode & node, LineageInfoContext & lineage_info_context)
{
    visitISourceNodeWithoutStorage(node, lineage_info_context);
}

void LineageInfoVisitor::visitValuesNode(ValuesNode & node, LineageInfoContext & lineage_info_context)
{
    visitISourceNodeWithoutStorage(node, lineage_info_context);
}

void LineageInfoVisitor::visitReadStorageRowCountNode(ReadStorageRowCountNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    visitISourceNodeWithoutStorage(node, lineage_info_context, {step->getStorageID().getDatabaseName(), step->getStorageID().getTableName()});
}

void LineageInfoVisitor::visitExtremesNode(ExtremesNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitLimitNode(LimitNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitFinalSampleNode(FinalSampleNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitMultiJoinNode(MultiJoinNode &, LineageInfoContext &)
{
    throw Exception("Not impl LineageInfo", ErrorCodes::NOT_IMPLEMENTED);
}

void LineageInfoVisitor::visitEnforceSingleRowNode(EnforceSingleRowNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitLocalExchangeNode(LocalExchangeNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitLimitByNode(LimitByNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitWindowNode(WindowNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);

    auto input_stream_lineages = lineage_info_context.output_stream_lineages;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    std::unordered_map<String, const WindowFunctionDescription &> column_name_to_window_desc;
    for (const auto & function : step->getFunctions())
    {
        column_name_to_window_desc.emplace(function.column_name, function);
    }

    for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        if (input_stream_lineages.contains(output_name))
        {
            new_output_stream_lineages.emplace(output_name, input_stream_lineages.at(output_name));
            continue;
        }
        else
        {
            auto outputstream_info = std::make_shared<OutputStreamInfo>();
            outputstream_info->output_name = output_name;
            String new_expression_name = output_name;
            if (column_name_to_window_desc.contains(output_name))
            {
                const auto & window_desc = column_name_to_window_desc.at(output_name);
                for (const auto & arg_name : window_desc.argument_names)
                {
                    if (input_stream_lineages.contains(arg_name))
                        outputstream_info->add(input_stream_lineages.at(arg_name));
                }
                if (outputstream_info->isEmpty())
                    new_expression_name = window_desc.function_node->getColumnName();
            }

            if (outputstream_info->isEmpty())
            {
                new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
                outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
            }
            new_output_stream_lineages.emplace(output_name, outputstream_info);
        }
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitArrayJoinNode(ArrayJoinNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::addNewExpressionSource(PlanNodeBase & node, LineageInfoContext & lineage_info_context, Names new_header_names)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    for (auto & new_header_name : new_header_names)
    {
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = new_header_name;
        new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_header_name);
        outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_header_name));
        lineage_info_context.output_stream_lineages.emplace(outputstream_info->output_name, std::move(outputstream_info));
    }
    expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
}

void LineageInfoVisitor::visitExpandNode(ExpandNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    addNewExpressionSource(node, lineage_info_context, {step->getGroupIdSymbol()});
}

void LineageInfoVisitor::visitMarkDistinctNode(MarkDistinctNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    addNewExpressionSource(node, lineage_info_context, {step->getMarkerSymbol()});
}

void LineageInfoVisitor::visitSortingNode(SortingNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitMergeSortingNode(MergeSortingNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitMergingSortedNode(MergingSortedNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitPartialSortingNode(PartialSortingNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitDistinctNode(DistinctNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitAssignUniqueIdNode(AssignUniqueIdNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    addNewExpressionSource(node, lineage_info_context, {step->getUniqueId()});
}

void LineageInfoVisitor::visitExchangeNode(ExchangeNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    std::vector<LineageInfoContext> children_lineage_info_context{node.getChildren().size()};
    for (size_t i = 0; i < node.getChildren().size(); ++i)
    {
        VisitorUtil::accept(node.getChildren()[i], *this, children_lineage_info_context[i]);
        lineage_info_context.tables.insert(
            lineage_info_context.tables.end(),
            children_lineage_info_context[i].tables.begin(),
            children_lineage_info_context[i].tables.end());
    }

    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    const auto & output_to_inputs = step->getOutToInputs();
    for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;

        if (output_to_inputs.contains(output_name))
        {
            const auto & input_stream_list = output_to_inputs.at(output_name);
            for (size_t i = 0; i < input_stream_list.size(); ++i)
                outputstream_info->add(children_lineage_info_context[i].output_stream_lineages.at(input_stream_list[i]));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitCTERefNode(CTERefNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    auto cte_id = step->getId();
    if (!cte_visit_results.contains(cte_id))
    {
        LineageInfoContext cte_lineage_info_context;
        cte_helper.accept(cte_id, *this, cte_lineage_info_context);
        cte_visit_results.emplace(cte_id, cte_lineage_info_context);
    }

    if (cte_visit_results.contains(cte_id))
    {
        lineage_info_context = cte_visit_results.at(cte_id);
        const auto & output_to_input = step->getOutputColumns();
        auto input_stream_lineages = lineage_info_context.output_stream_lineages;
        std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
        for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
        {
            auto output_name = output_column_and_type.name;
            auto outputstream_info = std::make_shared<OutputStreamInfo>();
            outputstream_info->output_name = output_name;
            if (output_to_input.contains(output_name) && input_stream_lineages.contains(output_to_input.at(output_name)))
                outputstream_info->add(input_stream_lineages.at(output_to_input.at(output_name)));
            new_output_stream_lineages.emplace(output_name, outputstream_info);
        }
        lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
    }
}
void LineageInfoVisitor::visitExplainAnalyzeNode(ExplainAnalyzeNode &, LineageInfoContext &)
{
    throw Exception("Not impl LineageInfo", ErrorCodes::NOT_IMPLEMENTED);
}

void LineageInfoVisitor::visitIntermediateResultCacheNode(IntermediateResultCacheNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitTopNFilteringNode(TopNFilteringNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitOutfileWriteNode(OutfileWriteNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitOutfileFinishNode(OutfileFinishNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitFillingNode(FillingNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitTotalsHavingNode(TotalsHavingNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}


void LineageInfoVisitor::visitTableWriteNode(TableWriteNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitTableFinishNode(TableFinishNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);

    const auto * step = node.getStep().get();
    auto target = step->getTarget();
    auto column_to_input_column = target->getTableColumnToInputColumnMap(step->getOutputStream().getNames());

    insert_info = std::make_shared<InsertInfo>();
    insert_info->database = target->getStorage()->getStorageID().getDatabaseName();
    insert_info->table = target->getStorage()->getStorageID().getTableName();
    for (const auto & [insert_column, input_column] : column_to_input_column)
    {
        InsertInfo::InsertColumnInfo insert_column_info;
        insert_column_info.insert_column_name = insert_column;
        insert_column_info.input_column = input_column;
        insert_info->insert_columns_info.emplace_back(insert_column_info);
    }
}

void LineageInfoVisitor::visitMergingAggregatedNode(MergingAggregatedNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    NameSet keys{step->getKeys().begin(), step->getKeys().end()};
    auto input_stream_lineages = lineage_info_context.output_stream_lineages;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        String new_expression_name = output_name;
        if (input_stream_lineages.contains(output_name))
        {
            outputstream_info->add(input_stream_lineages.at(output_name));
        }
        else
        {
            for (const auto & agg : step->getAggregates())
            {
                if (agg.column_name == output_name)
                {
                    for (const auto & argument_name : agg.argument_names)
                    {
                        if (input_stream_lineages.contains(argument_name))
                            outputstream_info->add(input_stream_lineages.at(argument_name));
                    }
                }
                if (outputstream_info->isEmpty())
                    new_expression_name = agg.function->getDescription();
            }
        }

        if (outputstream_info->isEmpty())
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitSetOperationNode(PlanNodeBase & node, LineageInfoContext & lineage_info_context)
{
    const auto & step = dynamic_cast<const SetOperationStep &>(*node.getStep());
    std::vector<LineageInfoContext> children_lineage_info_context{node.getChildren().size()};
    for (size_t i = 0; i < node.getChildren().size(); ++i)
    {
        VisitorUtil::accept(node.getChildren()[i], *this, children_lineage_info_context[i]);
        lineage_info_context.tables.insert(
            lineage_info_context.tables.end(),
            children_lineage_info_context[i].tables.begin(),
            children_lineage_info_context[i].tables.end());
    }

    SourceInfo new_expression_or_value_source_info;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    const auto & output_to_inputs = step.getOutToInputs();
    for (const auto & output_column_and_type : step.getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        if (output_to_inputs.contains(output_name))
        {
            const auto & input_stream_list = output_to_inputs.at(output_name);
            for (size_t i = 0; i < input_stream_list.size(); ++i)
                outputstream_info->add(children_lineage_info_context[i].output_stream_lineages.at(input_stream_list[i]));
        }
        else
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitExceptNode(ExceptNode & node, LineageInfoContext & lineage_info_context)
{
    visitSetOperationNode(node, lineage_info_context);
}

void LineageInfoVisitor::visitUnionNode(UnionNode & node, LineageInfoContext & lineage_info_context)
{
    visitSetOperationNode(node, lineage_info_context);
}

void LineageInfoVisitor::visitIntersectNode(IntersectNode & node, LineageInfoContext & lineage_info_context)
{
    visitSetOperationNode(node, lineage_info_context);
}

void LineageInfoVisitor::visitIntersectOrExceptNode(IntersectOrExceptNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    std::vector<LineageInfoContext> children_lineage_info_context{node.getChildren().size()};
    for (size_t i = 0; i < node.getChildren().size(); ++i)
    {
        VisitorUtil::accept(node.getChildren()[i], *this, children_lineage_info_context[i]);
        lineage_info_context.tables.insert(
            lineage_info_context.tables.end(),
            children_lineage_info_context[i].tables.begin(),
            children_lineage_info_context[i].tables.end());
    }

    SourceInfo new_expression_or_value_source_info;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        for (auto & child_lineage_info_context : children_lineage_info_context)
        {
            if (child_lineage_info_context.output_stream_lineages.contains(output_name))
                outputstream_info->add(child_lineage_info_context.output_stream_lineages.at(output_name));
        }

        if (outputstream_info->isEmpty())
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
        }

        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }

    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitApplyNode(ApplyNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    LineageInfoContext left_lineage_info_context;
    VisitorUtil::accept(node.getChildren()[0], *this, left_lineage_info_context);

    LineageInfoContext right_lineage_info_context;
    VisitorUtil::accept(node.getChildren()[1], *this, right_lineage_info_context);

    lineage_info_context.tables.insert(
        lineage_info_context.tables.end(), left_lineage_info_context.tables.begin(), left_lineage_info_context.tables.end());
    lineage_info_context.tables.insert(
        lineage_info_context.tables.end(), right_lineage_info_context.tables.begin(), right_lineage_info_context.tables.end());

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    auto & left_output_stream_lineages = left_lineage_info_context.output_stream_lineages;
    auto & right_output_stream_lineages = right_lineage_info_context.output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        if (left_output_stream_lineages.contains(output_name))
        {
            lineage_info_context.output_stream_lineages.emplace(output_name, left_output_stream_lineages.at(output_name));
        }
        else if (right_output_stream_lineages.contains(output_name))
        {
            lineage_info_context.output_stream_lineages.emplace(output_name, right_output_stream_lineages.at(output_name));
        }
        else
        {
            auto outputstream_info = std::make_shared<OutputStreamInfo>();
            outputstream_info->output_name = output_name;
            String new_expression_name = output_name;
            if (step->getAssignment().first == output_name)
            {
                RequiredSourceColumnsVisitor::Data col_context;
                RequiredSourceColumnsVisitor(col_context).visit(step->getAssignment().second->clone());
                NameSet required_symbols = col_context.requiredColumns();
                for (const auto & symbol : required_symbols)
                {
                    if (left_output_stream_lineages.contains(symbol))
                        outputstream_info->add(left_output_stream_lineages.at(symbol));
                    else if (right_output_stream_lineages.contains(symbol))
                        outputstream_info->add(right_output_stream_lineages.at(symbol));
                    else
                        new_expression_name = step->getAssignment().second->getColumnName();
                }
            }
            if (outputstream_info->isEmpty())
            {
                new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
                outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
            }
            lineage_info_context.output_stream_lineages.emplace(output_name, std::move(outputstream_info));
        }
    }

    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
}

void LineageInfoVisitor::visitAggregatingNode(AggregatingNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    NameSet keys{step->getKeys().begin(), step->getKeys().end()};
    auto input_stream_lineages = lineage_info_context.output_stream_lineages;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        String new_expression_name = output_name;
        if (keys.contains(output_name) && input_stream_lineages.contains(output_name))
        {
            outputstream_info->add(input_stream_lineages.at(output_name));
        }
        else
        {
            for (const auto & agg : step->getAggregates())
            {
                if (agg.column_name == output_name)
                {
                    for (const auto & argument_name : agg.argument_names)
                    {
                        if (input_stream_lineages.contains(argument_name))
                            outputstream_info->add(input_stream_lineages.at(argument_name));
                    }

                    if (outputstream_info->isEmpty())
                        new_expression_name = agg.function->getDescription();
                }
            }
        }

        if (outputstream_info->isEmpty())
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitJoinNode(JoinNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    LineageInfoContext left_lineage_info_context;
    VisitorUtil::accept(node.getChildren()[0], *this, left_lineage_info_context);

    LineageInfoContext right_lineage_info_context;
    VisitorUtil::accept(node.getChildren()[1], *this, right_lineage_info_context);

    lineage_info_context.tables.insert(
        lineage_info_context.tables.end(), left_lineage_info_context.tables.begin(), left_lineage_info_context.tables.end());
    lineage_info_context.tables.insert(
        lineage_info_context.tables.end(), right_lineage_info_context.tables.begin(), right_lineage_info_context.tables.end());

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    auto & left_output_stream_lineages = left_lineage_info_context.output_stream_lineages;
    auto & right_output_stream_lineages = right_lineage_info_context.output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        if (left_output_stream_lineages.contains(output_name))
        {
            lineage_info_context.output_stream_lineages.emplace(output_name, left_output_stream_lineages.at(output_name));
        }
        else if (right_output_stream_lineages.contains(output_name))
        {
            lineage_info_context.output_stream_lineages.emplace(output_name, right_output_stream_lineages.at(output_name));
        }
        else
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
            auto outputstream_info = std::make_shared<OutputStreamInfo>();
            outputstream_info->output_name = output_name;
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
            lineage_info_context.output_stream_lineages.emplace(output_name, std::move(outputstream_info));
        }
    }

    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
}

void LineageInfoVisitor::visitFilterNode(FilterNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitProjectionNode(ProjectionNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
    const auto & assignments = step->getAssignments();
    if (assignments.empty())
        return;

    auto input_stream_lineages = lineage_info_context.output_stream_lineages;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    for (const auto & output_column_and_type : step->getOutputStream().getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        String new_expression_name = output_name;
        if (assignments.contains(output_name))
        {
            RequiredSourceColumnsVisitor::Data col_context;
            RequiredSourceColumnsVisitor(col_context).visit(assignments.at(output_name)->clone());
            NameSet required_symbols = col_context.requiredColumns();
            for (const auto & symbol : required_symbols)
            {
                if (input_stream_lineages.contains(symbol))
                    outputstream_info->add(input_stream_lineages.at(symbol));
            }
            if (outputstream_info->isEmpty())
                new_expression_name = assignments.at(output_name)->getColumnName();
        }

        if (outputstream_info->isEmpty())
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}


void LineageInfoVisitor::visitTableScanNode(TableScanNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();

    String database_name;
    String table_name;
    const auto & table_expression = getTableExpression(*step->getQueryInfo().getSelectQuery(), 0);
    if (table_expression && table_expression->table_function)
    {
        auto storage = context->getQueryContext()->executeTableFunction(table_expression->table_function);
        if (const auto * distributed = dynamic_cast<StorageDistributed *>(storage.get()))
        {
            database_name = distributed->getRemoteDatabaseName();
            table_name = distributed->getRemoteTableName();
        }
    }

    if (database_name.empty() || table_name.empty())
    {
        database_name = step->getStorageID().database_name;
        table_name = step->getStorageID().table_name;
    }

    // get source column name
    String full_table_name = database_name + "." + table_name;
    SourceInfo column_source_info;
    SourceInfo new_expression_or_value_source_info;
    if (!table_sources.contains(full_table_name))
        table_sources.emplace(full_table_name, column_source_info);

    column_source_info = table_sources.at(full_table_name);
    column_source_info.source_tables.emplace_back(database_name, table_name);
    new_expression_or_value_source_info.source_tables.emplace_back(database_name, table_name);
    lineage_info_context.tables.emplace_back(database_name, table_name);

    const auto & columns_and_alias = step->getColumnToAliasMap();
    for (const auto & column_and_alias : columns_and_alias)
    {
        if (column_source_info.contains(column_and_alias.first))
            continue;
        column_source_info.add(column_id_allocator->nextId(), column_and_alias.first);
    }

    // analyze the mapping relationship between column name and outputStream
    const auto & alias_to_columns = step->getAliasToColumnMap();
    const DataStream & table_output_stream = step->getTableOutputStream();
    for (const auto & output_column_and_type : table_output_stream.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        if (alias_to_columns.contains(output_name) && column_source_info.contains(alias_to_columns.at(output_name)))
            outputstream_info->column_ids.emplace(column_source_info.getIdByName(alias_to_columns.at(output_name)));
        else if (!step->hasInlineExpressions() && step->getInlineExpressions().contains(output_name))
        {
            const auto & inline_expression = step->getInlineExpressions().at(output_name);
            RequiredSourceColumnsVisitor::Data col_context;
            RequiredSourceColumnsVisitor(col_context).visit(inline_expression->clone());
            NameSet required_cols = col_context.requiredColumns();
            for (const auto & col : required_cols)
            {
                if (column_source_info.contains(col))
                    outputstream_info->column_ids.emplace(column_source_info.getIdByName(col));
            }
        }
        else
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
        }
        lineage_info_context.output_stream_lineages.emplace(output_name, std::move(outputstream_info));
    }
    table_sources[full_table_name] = std::move(column_source_info);
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
}

}
