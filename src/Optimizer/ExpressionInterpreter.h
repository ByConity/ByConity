#pragma once

#include <Core/Field.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Optimizer/LiteralEncoder.h>
#include <Analyzers/TypeAnalyzer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST.h>

#include <functional>
#include <stack>
#include <variant>

namespace DB
{

using IdentifierResolver = std::function<std::optional<Field>(const ASTIdentifier &, const IDataType &)>;
using AstOrFieldWithType = std::pair<DataTypePtr, std::variant<ASTPtr, Field>>;

struct ExpressionInterpreterSettings
{
    const IdentifierResolver & identifier_resolver;
};

/** ExpressionInterpreter evaluates an expression with following functionalities:
  *   1. Constant folding, i.e. constant expression will be substituted with its result.
  *      Note that the expression don't have to be completely constant. E.g. `a + (1 + 2)`
  *      will be evaluated to `a + 3`. Also note that not all functions are suitable for
  *      constant folding, counter examples: undeterministic functions, aggregate functions,
  *      IN expressions...;
  *   2. Identifier resolution. Identifiers in the expression can be substituted with
  *      constants. This is mainly used for Outer Join Optimization;
  *   3. Null simplify. Functions calls with null constant argument can be substituted with
  *      null. See also PreparedFunctionImpl::useDefaultImplementationForNulls;
  *   4. Function simplify. Some functions can be simplify by its intrinsic logic, even if
  *      not all arguments are constants. E.g., `X OR 1` can be simplified to `1`;
  *
  * The return value may be either
  *   1. a Field, if there is no unresolved identifier, non-deterministic function calls,
  *      un-SuitableForConstantFolding function calls or subquery functions.
  *   2. or an IAST, otherwise.
  */
class ExpressionInterpreter
{
public:
    static ASTPtr optimizePredicate(
        const ConstASTPtr & node,
        ContextMutablePtr context,
        const NamesAndTypes & column_types,
        const IdentifierResolver & resolver = no_op_resolver);

    static std::optional<Field>
    calculateConstantExpression(const ConstASTPtr & node, ContextMutablePtr context, const TypeAnalyzer& type_analyzer);

    /**
     * @return AstOrFieldWithType maybe nullable if node is not supported.
     */
    static AstOrFieldWithType evaluate(
        const ConstASTPtr & node, ContextMutablePtr context, const TypeAnalyzer& type_analyzer, const ExpressionInterpreterSettings & settings);

    const static IdentifierResolver no_op_resolver;

    struct EvaluateContext : boost::noncopyable
    {
        ContextMutablePtr context;
        const TypeAnalyzer& type_analyzer;
        const ExpressionInterpreterSettings & settings;
        std::vector<ConstASTPtr> parents;

        EvaluateContext(ContextMutablePtr context_, const TypeAnalyzer& type_analyzer_, const ExpressionInterpreterSettings & settings_)
            : context(context_), type_analyzer(type_analyzer_), settings(settings_)
        {
        }

        DataTypePtr getType(const ConstASTPtr & node) const { return type_analyzer.getType(node); }

        bool parentIsFunctionLogical()
        {
            if (!parents.empty())
            {
                if (const auto * func = parents.back()->as<const ASTFunction>())
                {
                    return func->name == "and" || func->name == "or" || func->name == "not" || func->name == "xor";
                }
            }

            return false;
        }
    };

    // Within the interpreter, we use column instead of field for performance.
    using AstOrColumnWithType = std::pair<DataTypePtr, std::variant<ASTPtr, ColumnPtr>>;

    class Visitor : public ConstASTVisitor<AstOrColumnWithType, EvaluateContext>
    {
    public:
        AstOrColumnWithType process(const ConstASTPtr & node, EvaluateContext & context, const ConstASTPtr & parent = nullptr)
        {
            if (parent)
                context.parents.push_back(parent);

            auto res = ASTVisitorUtil::accept(node, *this, context);

            if (parent)
                context.parents.pop_back();

            return res;
        }

        AstOrColumnWithType visitASTLiteral(const ConstASTPtr & node, EvaluateContext & context) override;
        AstOrColumnWithType visitASTIdentifier(const ConstASTPtr & node, EvaluateContext & context) override;
        AstOrColumnWithType visitASTSubquery(const ConstASTPtr & node, EvaluateContext & context) override;
        AstOrColumnWithType visitASTFunction(const ConstASTPtr & node, EvaluateContext & context) override;

        static inline AstOrColumnWithType toColumnWithType(const DataTypePtr & type, const Field & field)
        {
            ColumnPtr column = type->createColumnConst(1, convertFieldToType(field, *type));
            return std::make_pair(type, column);
        }

        static inline ASTPtr toAST(const AstOrColumnWithType & ast_or_col, EvaluateContext & context, bool enforce_type = true)
        {
            if (std::holds_alternative<ASTPtr>(ast_or_col.second))
                return std::get<ASTPtr>(ast_or_col.second);
            else if (enforce_type)
                return LiteralEncoder::encode((*std::get<ColumnPtr>(ast_or_col.second))[0], ast_or_col.first, context.context);
            else
                return std::make_shared<ASTLiteral>((*std::get<ColumnPtr>(ast_or_col.second))[0]);
        }

        static inline AstOrColumnWithType rewriteToAST(const ASTPtr & ast, EvaluateContext & context)
        {
            ConstASTPtr constptr = ast;
            return {context.getType(constptr), ast};
        }

        AstOrColumnWithType processOrdinaryFunction(const ConstASTPtr & node, EvaluateContext & context);

        static ColumnsWithTypeAndName convertToColumns(const std::vector<AstOrColumnWithType> & arguments);

        // Create a new ASTFunction by replacing the original arguments with the evaluated arguments.
        // This method should add appropriate type conversions for literals.
        static ASTPtr createFunctionWithEvaluatedArgs(
            const ASTFunction & function, const std::vector<AstOrColumnWithType> & evaluated_args, EvaluateContext & evaluate_context);

        // IN operator has many use cases in clickhouse, we have optimized some ones of them. For others,
        // we may leverage ActionsVisitor to do more optimization. Below are details of what have been done:
        // 1. scalar-type-expr in scalar-type-expr: optimized
        // 2. scalar-type-expr in tuple-type-expr: optimized
        // 3. tuple-type-expr in tuple-type-expr: not optimized
        // 4. tuple-type-expr in nested-tuple-type-expr(e.g. (1, 1) in ((1 , 1), (2, 2))): not optimized
        // 5. scalar-type-expr in subquery: optimized, for this case the optimization is doing constant folding for left side
        // 6. tuple-type-expr in subquery: not optimized
        AstOrColumnWithType simplifyIn(const ConstASTPtr & node, EvaluateContext & evaluate_context);
    };
};

}
