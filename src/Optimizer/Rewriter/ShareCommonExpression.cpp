#include <Optimizer/Rewriter/ShareCommonExpression.h>

#include <Optimizer/EqualityASTMap.h>
#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/ProjectionPlanner.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanVisitor.h>

#include <fmt/format.h>
#include <common/logger_useful.h>


#include <algorithm>
#include <functional>

namespace DB
{
namespace
{
    struct DFSNode
    {
        PlanNodeBase * node;
        size_t explore_child = 0;

        explicit DFSNode(PlanNodeBase * node_) : node(node_)
        {
        }
    };

    using CachedExpressions = std::unordered_map<ConstASTPtr, ASTPtr>;

    bool isExpressionSharableStep(const PlanNodeBase * node, ContextMutablePtr context)
    {
        assert(node != nullptr);
        auto type = node->getStep()->getType();

        // for now simply skip this complex cases, see ASSERT-386
        if (node->getChildren().size() > 1)
            return false;

        if (type == IQueryPlanStep::Type::Projection)
        {
            const auto & projection = dynamic_cast<ProjectionStep &>(*node->getStep());
            for (const auto & assignment : projection.getAssignments())
                if (ExpressionDeterminism::canChangeOutputRows(assignment.second, context))
                    return false;
            return !projection.isFinalProject();
        }

        if (type == IQueryPlanStep::Type::Filter)
        {
            const auto & filter = dynamic_cast<FilterStep &>(*node->getStep());
            return !ExpressionDeterminism::canChangeOutputRows(filter.getFilter(), context);
        }

        if (type == IQueryPlanStep::Type::TableScan)
        {
            const auto & table_scan = dynamic_cast<TableScanStep &>(*node->getStep());
            return !!dynamic_cast<StorageCnchMergeTree *>(table_scan.getStorage().get());
        }

        const static std::unordered_set<IQueryPlanStep::Type> sharable_steps{
            // TODO: some steps are not supported by SymbolTransformMap
            IQueryPlanStep::Type::Filter,
            IQueryPlanStep::Type::TableScan,
            // IQueryPlanStep::Type::Window,
            IQueryPlanStep::Type::Sorting,
            // IQueryPlanStep::Type::PartialSorting,
            // IQueryPlanStep::Type::MergeSorting,
            // IQueryPlanStep::Type::MergingSorted,
            // IQueryPlanStep::Type::Limit,
            // IQueryPlanStep::Type::Offset,
            // IQueryPlanStep::Type::LocalExchange,
            IQueryPlanStep::Type::Exchange,
        };

        return sharable_steps.count(type);
    }

    struct ExpressionInfo
    {
        ASTPtr ast; // normalized expression
        int occurence = 1;
        int complexity;
        bool belong_to_prewhere;
        String symbol;

        ExpressionInfo(ASTPtr ast_, int complexity_, bool belong_to_prewhere_)
            : ast(std::move(ast_)), complexity(complexity_), belong_to_prewhere(belong_to_prewhere_)
        {
        }

        void addOccurence(int occur)
        {
            occurence += occur;
        }

        UInt64 getCost() const
        {
            return (occurence - 1) * complexity;
        }

        String toString() const
        {
            return fmt::format(
                "[ast = {}, complexity = {}, occurence = {}, cost = {}]", serializeAST(*ast), complexity, occurence, getCost());
        }

        bool operator<(const ExpressionInfo & other) const
        {
            if (occurence != other.occurence)
                return occurence < other.occurence;
            if (complexity != other.complexity)
                return complexity < other.complexity;
            return ast->getColumnName() < other.ast->getColumnName();
        }
    };

    bool compExpressionInfoRef(const std::reference_wrapper<ExpressionInfo> & a, const std::reference_wrapper<ExpressionInfo> & b)
    {
        return a.get() < b.get();
    }

    struct ExpressionInfos : public EqualityASTMap<ExpressionInfo>
    {
        void update(const ExpressionInfos & other)
        {
            for (const auto & [key, info] : other)
            {
                if (auto iter = find(key); iter != end())
                    iter->second.addOccurence(info.occurence);
                else
                    emplace(key, info);
            }
        }

        String toString() const
        {
            String res;
            for (const auto & [key, info] : *this)
                res += info.toString() + " | ";
            return res;
        }
    };

    // calculate complexity of sub-expressions and save them into ExpressionInfos
    class FindSharableExpressionASTVisitor : public ASTVisitor<std::optional<int>, const Void>
    {
    public:
        FindSharableExpressionASTVisitor(bool belong_to_prewhere_, ContextPtr ctx_)
            : belong_to_prewhere(belong_to_prewhere_), ctx(std::move(ctx_))
        {
        }
        std::optional<int> visitNode(ASTPtr &, const Void &) override
        {
            return 0;
        }
        std::optional<int> visitASTFunction(ASTPtr & node, const Void & context) override;

        ExpressionInfos expression_infos;
        bool belong_to_prewhere;
        ContextPtr ctx;
    };

    std::optional<int> FindSharableExpressionASTVisitor::visitASTFunction(ASTPtr & node, const Void & context)
    {
        auto & function = node->as<ASTFunction &>();
        if (RuntimeFilterUtils::isInternalRuntimeFilter(node) || ctx->isNonDeterministicFunction(function.name))
            return {};

        int complexity = 1; // complexity for this node

        if (function.arguments)
            for (auto & argument : function.arguments->children)
            {
                int child_complexity;
                auto * arg_func = argument->as<ASTFunction>();

                // TODO: currently it's hard to get the determinism of a lambda,
                // so not share expressions with lambdas
                if (arg_func && arg_func->name == "lambda")
                    return {};
                else
                {
                    auto optional_child_complexity = ASTVisitorUtil::accept(argument, *this, context);
                    if (!optional_child_complexity)
                        return {};
                    child_complexity = *optional_child_complexity;
                }

                complexity += child_complexity;
            }

        // a sub-expression only count 1 occurrence for the whole expression
        expression_infos.emplace(node, ExpressionInfo(node, complexity, belong_to_prewhere));
        return complexity;
    }

    // normalize and collect sub-expressions
    class FindSharableExpressionPlanVisitor : public PlanNodeVisitor<void, const Void>
    {
    public:
        FindSharableExpressionPlanVisitor(
            ContextPtr context_,
            SymbolTransformMap & symbol_transform_,
            ExpressionInfos & expression_infos_,
            CachedExpressions & cached_expressions_)
            : context(std::move(context_))
            , symbol_transform(symbol_transform_)
            , expression_infos(expression_infos_)
            , cached_expressions(cached_expressions_)
        {
        }
        void visitPlanNode(PlanNodeBase &, const Void &) override
        {
        }
        void visitTableScanNode(TableScanNode & node, const Void &) override;
        void visitFilterNode(FilterNode & node, const Void &) override;
        void visitProjectionNode(ProjectionNode & node, const Void &) override;

        ContextPtr context;
        SymbolTransformMap & symbol_transform;
        ExpressionInfos & expression_infos;
        CachedExpressions & cached_expressions;
    };

    void FindSharableExpressionPlanVisitor::visitTableScanNode(TableScanNode & node, const Void &)
    {
        if (context->getSettingsRef().enable_common_expression_sharing_for_prewhere)
        {
            auto & step = dynamic_cast<TableScanStep &>(*node.getStep().get());
            if (auto prewhere = step.getPrewhere())
            {
                auto normalized_prewhere = IdentifierToColumnReference::rewrite(step.getStorage().get(), node.getId(), prewhere);
                FindSharableExpressionASTVisitor ast_visitor(true, context);
                ASTVisitorUtil::accept(normalized_prewhere, ast_visitor, {});
                expression_infos.update(ast_visitor.expression_infos);
                cached_expressions.emplace(prewhere, normalized_prewhere);
            }
        }
    }

    void FindSharableExpressionPlanVisitor::visitFilterNode(FilterNode & node, const Void &)
    {
        auto & step = dynamic_cast<FilterStep &>(*node.getStep().get());
        FindSharableExpressionASTVisitor ast_visitor(false, context);
        const auto & filter = step.getFilter();
        auto normalized_filter = symbol_transform.inlineReferences(filter);
        ASTVisitorUtil::accept(normalized_filter, ast_visitor, {});
        expression_infos.update(ast_visitor.expression_infos);
        cached_expressions.emplace(filter, normalized_filter);
    }

    void FindSharableExpressionPlanVisitor::visitProjectionNode(ProjectionNode & node, const Void &)
    {
        auto & step = dynamic_cast<ProjectionStep &>(*node.getStep().get());
        FindSharableExpressionASTVisitor ast_visitor(false, context);
        for (const auto & [symbol, expression] : step.getAssignments())
        {
            auto normalized_expression = symbol_transform.inlineReferences(expression);
            ASTVisitorUtil::accept(normalized_expression, ast_visitor, {});
            cached_expressions.emplace(expression, normalized_expression);
        }
        expression_infos.update(ast_visitor.expression_infos);
    }

    constexpr int INIT = 0;
    constexpr int NOT_PRUNE = 1;
    constexpr int PRUNE = 3;

    class PruneSharableExpressionASTVisitor : public ASTVisitor<void, const int>
    {
    public:
        PruneSharableExpressionASTVisitor(ExpressionInfos & expression_infos_, EqualityASTMap<int> & prune_results_, UInt64 threshold_)
            : expression_infos(expression_infos_), prune_results(prune_results_), threshold(threshold_)
        {
        }

        void visitNode(ASTPtr &, const int &) override
        {
        }
        void visitASTFunction(ASTPtr & node, const int & parent_occurence) override;

        ExpressionInfos & expression_infos;
        EqualityASTMap<int> & prune_results;
        UInt64 threshold;
    };

    void PruneSharableExpressionASTVisitor::visitASTFunction(ASTPtr & node, const int & parent_occurence)
    {
        // It's a bit tricky that we does not early return here. The reason is, we need to update the prune
        // result of some non-topest shareable expressions, which are mistakenly marked "non-pruned"
        // when they are visited in the for loop of pruneSharableExpressions.
        auto & result = prune_results[node];
        bool seen = (result != INIT);

        auto iter = expression_infos.find(node);
        if (iter == expression_infos.end())
            return;

        const auto & expression_info = iter->second;
        bool prune = expression_info.getCost() < threshold || expression_info.occurence <= parent_occurence;
        result |= prune ? PRUNE : NOT_PRUNE;

        ASTFunction & function = node->as<ASTFunction &>();
        if (!seen && function.arguments)
            for (auto & argument : function.arguments->children)
                ASTVisitorUtil::accept(argument, *this, expression_info.occurence);
    }

    // filter out the *topest* sub-exprssions whose cost >= common_expression_sharing_threshold
    void pruneSharableExpressions(ExpressionInfos & expression_infos, UInt64 threshold)
    {
        // use a mark-sweep style
        EqualityASTMap<int> prune_results;
        PruneSharableExpressionASTVisitor prune_visitor(expression_infos, prune_results, threshold);

        for (auto & it : expression_infos)
        {
            const int init_occur = 0;
            ASTVisitorUtil::accept(it.second.ast, prune_visitor, init_occur);
        }

        for (auto it = expression_infos.begin(); it != expression_infos.end();)
        {
            const auto & res = prune_results.at(it->first);
            assert(res == PRUNE || res == NOT_PRUNE);
            if (res == PRUNE)
                it = expression_infos.erase(it);
            else
                ++it;
        }
    }

    // Input plan: original bottom
    // Output plan: original bottom -> intermediate projection
    PlanNodePtr computeSharableExpression(
        PlanNodeBase * bottom, ExpressionInfos & expression_infos, SymbolTranslationMap & symbol_translation, ContextMutablePtr context)
    {
        std::vector<std::reference_wrapper<ExpressionInfo>> expressions;
        for (auto & info : expression_infos)
            expressions.push_back(info.second);

        std::sort(expressions.begin(), expressions.end(), compExpressionInfoRef);

        assert(bottom->getChildren().size() == 1);
        auto & source = bottom->getChildren().front();
        ProjectionPlanner projection_planner{source, context};

        for (auto & expr_info : expressions)
        {
            // normalized expressions do need to be translated if the bottom is not a TableScan
            const auto & normalized_expr = expr_info.get().ast;
            expr_info.get().symbol = projection_planner.addColumn(normalized_expr).first;
            symbol_translation.addTranslation(normalized_expr, expr_info.get().symbol);
        }

        return projection_planner.build();
    }

    // Input plan: original bottom (TableScan)
    // Output plan: intermediate projection -> new bottom(TableScan with inline expressions)
    PlanNodePtr computeSharableExpressionForTableScan(
        PlanNodeBase * bottom, ExpressionInfos & expression_infos, SymbolTranslationMap & symbol_translation, ContextMutablePtr context)
    {
        std::vector<std::reference_wrapper<ExpressionInfo>> prewhere_expressions;
        std::vector<std::reference_wrapper<ExpressionInfo>> other_expressions;

        for (auto & info : expression_infos)
        {
            if (info.second.belong_to_prewhere)
                prewhere_expressions.push_back(info.second);
            else
                other_expressions.push_back(info.second);
        }

        // sort expression to make explain stable
        std::sort(prewhere_expressions.begin(), prewhere_expressions.end(), compExpressionInfoRef);
        std::sort(other_expressions.begin(), other_expressions.end(), compExpressionInfoRef);

        // build TableScan inline expression & add translation
        auto & table_scan = dynamic_cast<TableScanStep &>(*bottom->getStep().get());
        auto & symbol_allocator = context->getSymbolAllocator();
        auto inline_expressions = table_scan.getInlineExpressions();
        for (auto & info : prewhere_expressions)
        {
            auto translated_expr = ColumnReferenceToIdentifier::rewrite(info.get().ast, true);
            info.get().symbol = symbol_allocator->newSymbol(translated_expr);
            inline_expressions.emplace(info.get().symbol, translated_expr);
        }
        table_scan.setInlineExpressions(inline_expressions, context);
        // add symbol translation
        const auto * storage = table_scan.getStorage().get();
        auto unique_id = bottom->getId();
        for (const auto & [column, alias] : table_scan.getColumnAlias())
            symbol_translation.addTranslation(std::make_shared<ASTTableColumnReference>(storage, unique_id, column), alias);
        // add inline expression translation
        for (const auto & [alias, expr] : table_scan.getInlineExpressions())
            symbol_translation.addStorageTranslation(expr->clone(), alias, storage, unique_id);

        // build intermediate projection & add translation
        ProjectionPlanner projection_planner{bottom->shared_from_this(), context};
        for (auto & info : other_expressions)
        {
            const auto & normalized_expr = info.get().ast;
            auto translated_expr = symbol_translation.translate(normalized_expr);
            info.get().symbol = projection_planner.addColumn(translated_expr).first;
            symbol_translation.addTranslation(normalized_expr, info.get().symbol);
        }

        return projection_planner.build();
    }

    // handle cases when b = 0 and a underflow
    inline bool greaterOrEquals(size_t a, size_t b)
    {
        return static_cast<Int32>(a) >= static_cast<Int32>(b);
    }

    class FoldExpressionPlanVisitor : public PlanNodeVisitor<PlanNodePtr, PlanNodePtr>
    {
    public:
        FoldExpressionPlanVisitor(
            const CachedExpressions & cached_expressions_,
            const ExpressionInfos & expression_infos_,
            const SymbolTransformMap & symbol_transform_,
            SymbolTranslationMap & symbol_translation_)
            : cached_expressions(cached_expressions_)
            , expression_infos(expression_infos_)
            , symbol_transform(symbol_transform_)
            , symbol_translation(symbol_translation_)
        {
        }
        PlanNodePtr visitPlanNode(PlanNodeBase & node, PlanNodePtr & cur_node) override;
        PlanNodePtr visitProjectionNode(ProjectionNode & node, PlanNodePtr & cur_node) override;
        PlanNodePtr visitFilterNode(FilterNode & node, PlanNodePtr & cur_node) override;

    private:
        const CachedExpressions & cached_expressions;
        const ExpressionInfos & expression_infos;
        const SymbolTransformMap & symbol_transform;
        SymbolTranslationMap & symbol_translation;
    };

    PlanNodePtr FoldExpressionPlanVisitor::visitPlanNode(PlanNodeBase & node, PlanNodePtr & cur_node)
    {
        // ASSERT-386
        assert(node.getChildren().size() == 1);
        node.replaceChildren(PlanNodes{cur_node});
        return node.shared_from_this();
    }

    PlanNodePtr FoldExpressionPlanVisitor::visitProjectionNode(ProjectionNode & node, PlanNodePtr & cur_node)
    {
        // fold expression & reconstruct translation map, since some symbols may be pruned
        auto & projection_step = dynamic_cast<ProjectionStep &>(*node.getStep().get());
        auto assignments = projection_step.getAssignments();
        auto name_to_type = projection_step.getNameToType();
        auto input_types = cur_node->getCurrentDataStream().getNamesToTypes();
        SymbolTranslationMap new_translation;

        for (auto & [symbol, expr] : assignments)
        {
            const auto & normalized_expr = cached_expressions.at(expr);
            expr = symbol_translation.translate(normalized_expr);
            new_translation.addTranslation(symbol_transform.inlineReferences(symbol), symbol);
        }

        for (const auto & info : expression_infos)
        {
            assignments.emplace(info.second.symbol, std::make_shared<ASTIdentifier>(info.second.symbol));
            name_to_type.emplace(info.second.symbol, input_types.at(info.second.symbol));
            new_translation.addTranslation(info.second.ast, info.second.symbol);
        }

        symbol_translation = std::move(new_translation);

        auto new_projection_step = std::make_shared<ProjectionStep>(
            cur_node->getCurrentDataStream(), assignments, name_to_type, projection_step.isFinalProject(), projection_step.isIndexProject(), projection_step.getHints());

        return PlanNodeBase::createPlanNode(node.getId(), new_projection_step, PlanNodes{cur_node}, node.getStatistics());
    }

    PlanNodePtr FoldExpressionPlanVisitor::visitFilterNode(FilterNode & node, PlanNodePtr & cur_node)
    {
        auto & filter_step = dynamic_cast<FilterStep &>(*node.getStep().get());
        const auto & normalized_expr = cached_expressions.at(filter_step.getFilter());
        auto translate_expr = symbol_translation.translate(normalized_expr);
        filter_step.setFilter(translate_expr);
        node.replaceChildren(PlanNodes{cur_node});
        return node.shared_from_this();
    }
};

/// 1. Traverse the plan to find plan segments, which consist of expression sharable steps.
/// 2. Normalize expressions and find sharable expressions, which should satisfy:
///    (expression_occurrence - 1) * expression_complexity >= common_expression_sharing_threshold.
/// 3. Compute sharable expression before the segment. That is, adding a intermediate projection node to
///    compute those expression, or pushing them down to the inline expressions of table scan(to share with PREWHERE).
/// 4. Fold other expressions to use sharable expression.
/// 5. Add a output projection node after the segment, to ensure the output columns consistent
///    with that before rewritting.
PlanNodePtr ShareCommonExpression::rewriteImpl(PlanNodePtr root, ContextMutablePtr context)
{
    assert(root != nullptr);
    LoggerPtr logger = getLogger("ShareCommonExpression");
    std::vector<DFSNode> stack;
    stack.emplace_back(root.get());

    while (!stack.empty())
    {
        auto & back = stack.back();

        // traverse children
        if (back.explore_child < back.node->getChildren().size())
        {
            ++back.explore_child;
            stack.emplace_back(back.node->getChildren().at(back.explore_child - 1).get());
            continue;
        }

        auto & bottom = back.node;
        if (!isExpressionSharableStep(bottom, context))
        {
            stack.pop_back();
            continue;
        }

        auto top_idx = stack.size() - 1;

        while (top_idx > 0 && isExpressionSharableStep(stack.at(top_idx - 1).node, context))
            --top_idx;

        // skip segments with single step
        if (top_idx == stack.size() - 1)
        {
            stack.pop_back();
            continue;
        }

        auto & top = stack.at(top_idx).node;

        LOG_TRACE(
            logger, "Detect sharable expression for segement whose top node id: {}, bottom node id: {}", top->getId(), bottom->getId());

        // find sharable expression
        SymbolTransformMap symbol_transform; // for expression normalizing
        ExpressionInfos expression_infos; // store sharable expressions
        CachedExpressions cached_expressions; // cache normalized expressions
        do
        {
            auto res = SymbolTransformMap::buildFrom(*top, bottom->getId());
            if (!res.has_value())
            {
                LOG_WARNING(logger, "build transform map failed, this normally indicates a name ambiguity");
                break;
            }
            symbol_transform = std::move(*res);

            FindSharableExpressionPlanVisitor find_sharable_expr_visitor(context, symbol_transform, expression_infos, cached_expressions);
            for (auto idx = stack.size() - 1; greaterOrEquals(idx, top_idx); --idx)
                VisitorUtil::accept(*stack.at(idx).node, find_sharable_expr_visitor, {});
            LOG_TRACE(logger, "collected expression infos: {}", expression_infos.toString());
            pruneSharableExpressions(
                expression_infos, std::max(context->getSettingsRef().common_expression_sharing_threshold.value, UInt64{1}));
            LOG_TRACE(logger, "sharable expression infos: {}", expression_infos.toString());
        } while (false);

        if (!expression_infos.empty())
        {
            SymbolTranslationMap symbol_translation;
            Names output_names = top->getOutputNames();

            // construct intermediate projection or inline expressions to compute sharable expression,
            // also build the initial symbol translation map and remember allocated symbols
            PlanNodePtr cur_node;
            if (bottom->getType() == IQueryPlanStep::Type::TableScan)
                cur_node = computeSharableExpressionForTableScan(bottom, expression_infos, symbol_translation, context);
            else if (const auto * source_step = dynamic_cast<ISourceStep *>(bottom->getStep().get()))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Common expression sharing optimization is not supported by step kind {}",
                    source_step->getName());
            else
                cur_node = computeSharableExpression(bottom, expression_infos, symbol_translation, context);

            // visit steps and fold expression, skip bottom if it's a table scan
            size_t next_node_idx = bottom->getType() == IQueryPlanStep::Type::TableScan ? stack.size() - 1 : stack.size();
            FoldExpressionPlanVisitor fold_visitor{cached_expressions, expression_infos, symbol_transform, symbol_translation};

            while (greaterOrEquals(--next_node_idx, top_idx))
                cur_node = VisitorUtil::accept(*stack.at(next_node_idx).node, fold_visitor, cur_node);

            // add output projection to make output column consistent
            PlanNodePtr output_node;
            {
                ProjectionPlanner output_planner{cur_node, context};
                output_node = output_planner.build(output_names);
            }

            if (top_idx > 0)
            {
                auto & frame = stack.at(top_idx - 1);
                PlanNodes new_children = frame.node->getChildren();
                new_children.at(frame.explore_child - 1) = output_node;
                frame.node->replaceChildren(new_children);
            }
            else
                root = output_node;
        }

        // pop nodes until top, this prevents exploring other segments(formed by other children).
        // the reason is, after a top projection is added, the stack is stale.
        for (auto count = stack.size() - top_idx; count > 0; --count)
            stack.pop_back();
    }

    return root;
}

bool ShareCommonExpression::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    auto & cte_info = plan.getCTEInfo();

    for (auto & cte : cte_info.getCTEs())
        cte_info.update(cte.first, rewriteImpl(cte.second, context));

    plan.update(rewriteImpl(plan.getPlanNode(), context));
    return true;
}
}
