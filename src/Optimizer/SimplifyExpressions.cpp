#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SimplifyExpressions.h>
#include <Parsers/formatAST.h>

namespace DB
{
ConstASTPtr CommonPredicatesRewriter::rewrite(const ConstASTPtr & predicate, ContextMutablePtr & context)
{
    CommonPredicatesRewriter rewriter;
    NodeContext node_context{.root = NodeContext::Root::ROOT_NODE, .context = context};
    return ASTVisitorUtil::accept(predicate, rewriter, node_context);
}

ConstASTPtr CommonPredicatesRewriter::visitNode(const ConstASTPtr & node, NodeContext & context)
{
    ASTs children;
    for (ConstASTPtr child : node->children)
    {
        ASTPtr ast = ASTVisitorUtil::accept(child, *this, context)->clone();
        children.emplace_back(ast);
    }
    auto new_node = node->clone();
    new_node->replaceChildren(children);
    return new_node;
}

ConstASTPtr CommonPredicatesRewriter::visitASTFunction(const ConstASTPtr & node, NodeContext & node_context)
{
    auto & fun = node->as<ASTFunction &>();
    if (fun.name == PredicateConst::AND || fun.name == PredicateConst::OR)
    {
        std::vector<ConstASTPtr> extracted_predicates = PredicateUtils::extractPredicate(node);
        std::vector<ConstASTPtr> result;
        for (auto & predicate : extracted_predicates)
        {
            NodeContext child_context{.root = NodeContext::Root::NOT_ROOT_NODE, .context = node_context.context};
            auto rewritten = process(predicate, child_context);
            result.emplace_back(rewritten);
        }
        ASTPtr combined_predicate = PredicateUtils::combinePredicates(fun.name, result);
        auto combined_fun = combined_predicate->as<ASTFunction>();
        if (combined_fun == nullptr || (combined_fun->name != PredicateConst::AND && combined_fun->name != PredicateConst::OR))
        {
            return combined_predicate;
        }
        auto simplified = PredicateUtils::extractCommonPredicates(combined_predicate, node_context.context);
        // Prefer AND at the root if possible
        auto simplified_fun = simplified->as<ASTFunction>();
        if (node_context.root == NodeContext::Root::ROOT_NODE && simplified_fun && simplified_fun->name == PredicateConst::OR)
        {
            return PredicateUtils::distributePredicate(simplified, node_context.context);
        }
        return simplified;
    }
    return node;
}

ConstASTPtr CommonPredicatesRewriter::process(const ConstASTPtr & node, NodeContext & context)
{
    return ASTVisitorUtil::accept(node, *this, context);
}

ConstASTPtr SwapPredicateRewriter::rewrite(const ConstASTPtr & predicate, ContextMutablePtr &)
{
    SwapPredicateRewriter visitor;
    Void visitor_context{};
    return ASTVisitorUtil::accept(predicate, visitor, visitor_context);
}

ConstASTPtr SwapPredicateRewriter::visitNode(const ConstASTPtr & node, Void & context)
{
    ASTs children;
    for (ConstASTPtr child : node->children)
    {
        ASTPtr ast = ASTVisitorUtil::accept(child, *this, context)->clone();
        children.emplace_back(ast);
    }
    auto new_node = node->clone();
    new_node->replaceChildren(children);
    return new_node;
}

ConstASTPtr SwapPredicateRewriter::visitASTFunction(const ConstASTPtr & predicate, Void & context)
{
    auto & function = predicate->as<ASTFunction &>();
    if (function.name == "and")
    {
        std::vector<ConstASTPtr> conjuncts = PredicateUtils::extractConjuncts(predicate);
        ASTs reordered_conjunct;
        for (ConstASTPtr conjunct : conjuncts)
        {
            ASTPtr ast = ASTVisitorUtil::accept(conjunct, *this, context)->clone();
            reordered_conjunct.emplace_back(ast);
        }
        return makeASTFunction("and", reordered_conjunct);
    }
    if (function.name == "or")
    {
        std::vector<ConstASTPtr> disjuncts = PredicateUtils::extractDisjuncts(predicate);
        ASTs reordered_disjuncts;
        for (ConstASTPtr disjunct : disjuncts)
        {
            ASTPtr ast = ASTVisitorUtil::accept(disjunct, *this, context)->clone();
            reordered_disjuncts.emplace_back(ast);
        }
        return makeASTFunction("or", reordered_disjuncts);
    }
    if (function.name == "not")
    {
        ConstASTPtr sub = function.arguments->getChildren()[0];
        ASTPtr reorder_sub = ASTVisitorUtil::accept(sub, *this, context)->clone();
        return makeASTFunction("not", reorder_sub);
    }
    if (function.name == "equals")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("equals", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "notEquals")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("notEquals", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "greater")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("less", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "greaterOrEquals")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("lessOrEquals", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "less")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("greater", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    if (function.name == "lessOrEquals")
    {
        if (!function.arguments->getChildren()[0]->as<ASTIdentifier>() && function.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            return makeASTFunction("greaterOrEquals", ASTs{function.arguments->getChildren()[1], function.arguments->getChildren()[0]});
        }
        return predicate->clone();
    }
    return predicate;
}

}
