#include <utility>
#include <Optimizer/Rewriter/OptimizeTrivialCount.h>

#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/Void.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <QueryPlan/SymbolMapper.h>
#include "Optimizer/OptimizerMetrics.h"


namespace DB
{

bool OptimizeTrivialCount::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    if (context->getSettingsRef().max_parallel_replicas > 1)
        return false;
    TrivialCountVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
    return true;
}

PlanNodePtr TrivialCountVisitor::visitAggregatingNode(AggregatingNode & node, Void & v)
{
    //check function, there is only one aggregation function in the AggregatingNode, and the size of output header columns is 1;
    const auto & agg_step = dynamic_cast<const AggregatingStep &>(*node.getStep().get());
    if (agg_step.getParams().aggregates.size() != 1 ||
            !typeid_cast<const AggregateFunctionCount *>(agg_step.getParams().aggregates[0].function.get()) ||
            agg_step.getOutputStream().header.columns() != 1)
        return visitPlanNode(node, v);

    CountContextVisitor visitor;
    TrivialCountContext count_context;
    // get the filters in FilterNode, get the query and storage in TableScanNode
    VisitorUtil::accept(node.getChildren()[0], visitor, count_context);

    // There are other nodes below this node that change the number of output lines except for the FilterNode
    if (count_context.has_other_node || !count_context.query || count_context.pushdowned_index_projection)
        return visitPlanNode(node, v);

    // check query
    auto select_query = count_context.query->as<ASTSelectQuery &>();
    if (select_query.sampleSize() || select_query.sampleOffset())
        return visitPlanNode(node, v);

    // check storage
    auto storage = count_context.storage;
    if (!storage ||
        storage->getName() == "MaterializeMySQL" ||
        !storage->supportsTrivialCount() ||
        (storage && context->getRowPolicyCondition(storage->getStorageID().getDatabaseName(), storage->getStorageID().getTableName(), RowPolicy::SELECT_FILTER))
        )
        return visitPlanNode(node, v);

    count_context.filters = replaceColumnsAlias(count_context.filters, count_context.column_alias);
    auto filters = count_context.filters;
    if (select_query.prewhere())
        filters.emplace_back(select_query.prewhere());
    if(select_query.where())
        filters.emplace_back(select_query.where());

    NameSet required = getRequiredColumns(filters);

    // Check if requeried columns are partition keys
    if (!required.empty())
    {
        const auto & partition_desc = storage->getInMemoryMetadataPtr()->getPartitionKey();
        if (partition_desc.expression)
        {
            auto partition_columns = partition_desc.expression->getRequiredColumns();
            partition_columns.push_back("_part");
            partition_columns.push_back("_partition_id");
            partition_columns.push_back("_part_uuid");
            partition_columns.push_back("_partition_value");
            partition_columns.push_back("_bucket_number");
            for (const auto & required_column : required)
            {
                if (std::find(partition_columns.begin(), partition_columns.end(), required_column) == partition_columns.end())
                    return visitPlanNode(node, v);
            }
        }
        else
            return visitPlanNode(node, v);
    }

    // set query.select() to select count() ...
    ASTPtr count_func;
    count_func = makeASTFunction("count");
    select_query.setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    select_query.select()->children.reserve(1);
    select_query.select()->children.emplace_back(count_func);

    // try to get the num_rows in storage, if failed，do not optimize.
    std::optional<UInt64> num_rows{};
    if (filters.empty())
    {
        // if filters is empty，Get row count directly
        num_rows = storage->totalRows(context);
    }
    else
    {
        // if filters is not empty，build query with filters and prewhere, then 'Interpreter' the query and get the query_info

        /// create ASTSelectQuery for "SELECT count(argument) FROM table PREWHERE expression/WHERE expression"
        if (select_query.where())
            count_context.filters.emplace_back(select_query.where());
        ASTPtr where_function;
        if (count_context.filters.size() > 1)
            where_function = makeASTFunction("and", count_context.filters);
        else if (count_context.filters.size() == 1)
            where_function = count_context.filters[0];
        if (where_function)
            select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_function));

        auto interpreter = std::make_shared<InterpreterSelectQuery>(select_query.clone(), context, SelectQueryOptions());
        if (!interpreter->getSyntaxAnalyzerResult()->optimize_trivial_count)
            return visitPlanNode(node, v);

        SelectQueryInfo temp_query_info;
        temp_query_info.query = interpreter->getQuery();
        temp_query_info.syntax_analyzer_result = interpreter->getSyntaxAnalyzerResult();
        temp_query_info.sets = interpreter->getQueryAnalyzer()->getPreparedSets();
        num_rows = storage->totalRowsByPartitionPredicate(temp_query_info, context);
    }

    if (!num_rows)
        return visitPlanNode(node, v);

    auto read_row_count= std::make_shared<ReadStorageRowCountStep>(node.getCurrentDataStream().header,
                                                                    select_query.clone(),
                                                                    agg_step.getParams().aggregates[0],
                                                                    agg_step.isFinal(),
                                                                    storage->getStorageID());
    read_row_count->setNumRows(num_rows.value());                                                            
    auto new_child_node= PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(read_row_count), {});
    return new_child_node->shared_from_this();
}

NameSet TrivialCountVisitor::getRequiredColumns(ASTs & filters)
{
    // get required columns in asts
    NameSet required;
    for (auto & filter_ast: filters)
    {
        RequiredSourceColumnsVisitor::Data col_context;
        RequiredSourceColumnsVisitor(col_context).visit(filter_ast);
        NameSet required_col = col_context.requiredColumns();
        required.insert(required_col.begin(),required_col.end());
    }
    return required;
}

ASTs TrivialCountVisitor::replaceColumnsAlias(ASTs & filters, NamesWithAliases & column_alias)
{
    if (filters.empty())
        return {};
    ASTs res;
    for (auto & filter_ast: filters)
    {
        auto filter_conjuncts = PredicateUtils::extractConjuncts(filter_ast);
        std::unordered_map<String, String> inv_alias;
        for (auto & item : column_alias)
            inv_alias.emplace(item.second, item.first);
        auto mapper = SymbolMapper::simpleMapper(inv_alias);

        std::vector<ConstASTPtr> conjuncts;
        for (auto & filter : filter_conjuncts)
        {
            bool has_in = false;
            auto symbols = SymbolsExtractor::extract(filter);
            for (const auto & item : symbols)
                has_in |= inv_alias.contains(item);

            if (has_in)
                conjuncts.emplace_back(mapper.map(filter));
            else
                conjuncts.emplace_back(filter);
        }
        auto new_filter = PredicateUtils::combineConjuncts(conjuncts);
        res.emplace_back(new_filter);
    }
    return res;
}

void CountContextVisitor::visitProjectionNode(ProjectionNode & node, TrivialCountContext & context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, context);
}

void CountContextVisitor::visitFilterNode(FilterNode & node, TrivialCountContext & context)
{
    if (node.getStep()->getFilter())
        context.filters.emplace_back(node.getStep()->getFilter()->clone());
    VisitorUtil::accept(node.getChildren()[0], *this, context);
}

void CountContextVisitor::visitSortingNode(SortingNode & node, TrivialCountContext & context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, context);
}

void CountContextVisitor::visitTableScanNode(TableScanNode & node, TrivialCountContext & context)
{
    auto storage = node.getStep()->getStorage();
    context.storage = node.getStep()->getStorage();
    context.query = node.getStep()->getQueryInfo().query->clone();
    context.column_alias = node.getStep()->getColumnAlias();
    context.pushdowned_index_projection = node.getStep()->hasInlineExpressions();
}

void CountContextVisitor::visitPlanNode(PlanNodeBase & , TrivialCountContext & context)
{
    context.has_other_node = true;
}

}
