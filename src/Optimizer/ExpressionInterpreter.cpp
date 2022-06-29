#include <Optimizer/ExpressionInterpreter.h>

#include <Analyzers/ASTEquals.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/InternalFunctionsDynamicFilter.h>
#include <Optimizer/FunctionInvoker.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Utils.h>
#include <Interpreters/ActionsVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

const IdentifierResolver ExpressionInterpreter::no_op_resolver     // NOLINT(cert-err58-cpp)
    = [](const ASTIdentifier &, const IDataType &) { return std::nullopt; };

AstOrFieldWithType ExpressionInterpreter::evaluate(
    const ConstASTPtr & node, ContextMutablePtr context, const TypeAnalyzer& type_analyzer, const ExpressionInterpreterSettings & settings)
{
    Visitor visitor;
    EvaluateContext evaluate_ctx{context, type_analyzer, settings};
    auto res = visitor.process(node, evaluate_ctx);

    if (std::holds_alternative<ASTPtr>(res.second))
        return std::make_pair(res.first, std::get<ASTPtr>(res.second));
    else
        return std::make_pair(res.first, (*std::get<ColumnPtr>(res.second))[0]);
}

static ASTPtr optimizerConjunctPredicate(const ConstASTPtr & node)
{
    // TODO@wangtao: optimize inside evaluate?
    //               optimize NULL, optimize OR, optimize recursive
    auto conjuncts = PredicateUtils::extractConjuncts(node);
    ASTSet<ConstASTPtr> conjuncts_set;
    std::vector<ASTPtr> simplify_conjuncts;
    for (auto & conjunct : conjuncts)
    {
        if (conjunct->as<ASTFunction>())
        {
            const auto & fun = conjunct->as<ASTFunction &>();
            auto & children = fun.arguments->getChildren();
            // "a" = "a"
            if (Poco::toLower(fun.name) == "equals" && children.size() == 2 && children[0]->as<ASTIdentifier>() && children[1]->as<ASTIdentifier>()
                && children[0]->as<ASTIdentifier &>().name() == children[1]->as<ASTIdentifier &>().name())
                continue;

            // cast(1, 'UInt8') produced by PredicatePushdown push filter generated from removeApply
            if (Poco::toLower(fun.name) == "cast" && children.size() == 2 && children[0]->as<ASTLiteral>() && children[1]->as<ASTLiteral>()
                && children[0]->getColumnName() == "1"
                && (children[1]->getColumnName() == "'UInt8'" || children[1]->getColumnName() == "'Nullable(UInt8)'"))
                continue;

            // if(expr, null, 0) return false.
            // this simplifies filter produced by PredicatePushdown like
            // if(isNull(`ws_order_number`), NULL, cast(multiIf(`build_side_non_null_symbol` = 1, 1, NULL, 0, 0), 'UInt8'))
            if (Poco::toLower(fun.name) == "if" && children.size() == 3 && PredicateUtils::isFalsePredicate(children[1])
                && PredicateUtils::isFalsePredicate(children[2]))
                return PredicateConst ::FALSE_VALUE;

            // if(isNull(expr), NULL, 1) return isNotNull(expr)
            if (Poco::toLower(fun.name) == "if" && children.size() == 3 && children[0]->as<ASTFunction>()
                && Poco::toLower(children[0]->as<ASTFunction &>().name) == "isnull"
                && PredicateUtils::isFalsePredicate(children[1])
                && PredicateUtils::isTruePredicate(children[2]))
            {
                auto rewrite = makeASTFunction("isNotNull", children[0]->as<ASTFunction &>().arguments->getChildren());
                if (conjuncts_set.contains(conjunct))
                    continue;
                conjuncts_set.emplace(rewrite);
                simplify_conjuncts.emplace_back(rewrite);
                continue;
            }
        }
        // "1"
        if (conjunct->as<ASTLiteral>())
        {
            const auto & literal = conjunct->as<ASTLiteral &>();
            if (literal.getColumnName() == "1")
                continue;
        }
        if (conjuncts_set.contains(conjunct))
            continue;
        conjuncts_set.emplace(conjunct);
        simplify_conjuncts.emplace_back(conjunct->clone());
    }
    if (simplify_conjuncts.empty())
        return PredicateConst::TRUE_VALUE;
    if (simplify_conjuncts.size() == 1)
        return simplify_conjuncts[0];
    // call makeASTFunction here instead of PredicateUtils::combineConjuncts because of the later sort asts.
    return makeASTFunction("and", simplify_conjuncts);
}

ASTPtr ExpressionInterpreter::optimizePredicate(
    const ConstASTPtr & node, ContextMutablePtr context, const NamesAndTypes & column_types, const IdentifierResolver & resolver)
{
    if (!context->getSettingsRef().enable_simplify_expression)
        return node->clone();

    // TODO exception handle, return origin expression if exception happens.
    ExpressionInterpreterSettings settings{.identifier_resolver = resolver};
    auto type_analyzer = TypeAnalyzer::create(context, column_types);
    auto res = evaluate(node, context, type_analyzer, settings);

    if (std::holds_alternative<ASTPtr>(res.second))
        return optimizerConjunctPredicate(std::get<ASTPtr>(res.second));

    Field field = std::get<Field>(res.second);
    DataTypePtr & type = res.first;

    if (isNativeNumber(removeNullable(type)))
    {
        UInt8 x = !field.isNull() && applyVisitor(FieldVisitorConvertToNumber<bool>(), field);
        field = x;
    }

    return std::make_shared<ASTLiteral>(field);
}

std::optional<Field>
ExpressionInterpreter::calculateConstantExpression(const ConstASTPtr & node, ContextMutablePtr context, const TypeAnalyzer& type_analyzer)
{
    ExpressionInterpreterSettings settings{.identifier_resolver = no_op_resolver};
    auto res = evaluate(node, context, type_analyzer, settings);

    if (std::holds_alternative<ASTPtr>(res.second))
        return std::nullopt;

    return std::get<Field>(res.second);
}

using EvaluateContext = ExpressionInterpreter::EvaluateContext;
using AstOrColumnWithType = ExpressionInterpreter::AstOrColumnWithType;
using Visitor = ExpressionInterpreter::Visitor;

AstOrColumnWithType Visitor::visitASTLiteral(const ConstASTPtr & node, EvaluateContext & context)
{
    auto thistype = context.getType(node);
    return toColumnWithType(thistype, node->as<ASTLiteral &>().value);
}

AstOrColumnWithType Visitor::visitASTIdentifier(const ConstASTPtr & node, EvaluateContext & context)
{
    auto thistype = context.getType(node);
    std::optional<Field> resolution = context.settings.identifier_resolver(node->as<ASTIdentifier &>(), *thistype);

    if (resolution)
        return toColumnWithType(thistype, *resolution);
    else
        return {thistype, node->clone()};
}

AstOrColumnWithType Visitor::visitASTSubquery(const ConstASTPtr & node, EvaluateContext &)
{
    return {nullptr, node->clone()};
}

AstOrColumnWithType Visitor::visitASTFunction(const ConstASTPtr & node, EvaluateContext & context)
{
    const auto & function = node->as<const ASTFunction &>();

    // lambda argument, aggregate function, navigation function, exists subquery, str_to_map...
    if (!FunctionFactory::instance().tryGet(function.name, context.context))
        return {nullptr, node->clone()}; // NOLINT(bugprone-branch-clone)
    else if (function.name == InternalFunctionDynamicFilter::name)
        return {nullptr, node->clone()};
    else if (function.name == "str_to_map")
        return {nullptr, node->clone()};
    else if (function.name == "arraySetCheck" || function.name == "arrayJoin")
        return {context.getType(node), node->clone()};
    else if (function.name == "in" || function.name == "globalIn" || function.name == "notIn" || function.name == "globalNotIn")
        return simplifyIn(node, context);
    // TODO: support nullIn
    else if (function.name == "nullIn" || function.name == "notNullIn" || function.name == "globalNullIn" || function.name == "globalNotNullIn")
        return {nullptr, node->clone()};
    else
        return processOrdinaryFunction(node, context);
}

using namespace FunctionsLogicalDetail;

template <typename FunctionName, typename FunctionImpl>
struct LogicalFunctionRewriter
{

static bool apply(const ASTFunction & function, std::vector<AstOrColumnWithType> evaluated_args,
                  AstOrColumnWithType & simplify_result, const DataTypePtr & function_return_type,
                  EvaluateContext & evaluate_context)
{

    if (function.name != FunctionName::name)
        return false;

    bool has_const = false;
    Ternary::ResultType const_res = 0;
    bool has_nullable_unresolved_arg = false;
    bool has_nullable_constant_arg = false;

    for (int i = static_cast<int>(evaluated_args.size()) - 1; i >= 0; --i)
    {
        if (std::holds_alternative<ASTPtr>(evaluated_args[i].second))
        {
            has_nullable_unresolved_arg |= evaluated_args[i].first->isNullable();
        }
        else
        {
            has_nullable_constant_arg |= evaluated_args[i].first->isNullable();

            auto field = (*std::get<ColumnPtr>(evaluated_args[i].second))[0];
            Ternary::ResultType value = field.isNull() ? Ternary::makeValue(false, true) :
                                                       Ternary::makeValue(applyVisitor(FieldVisitorConvertToNumber<bool>(), field));

            if (has_const)
            {
                const_res = FunctionImpl::apply(const_res, value);
            }
            else
            {
                const_res = value;
                has_const = true;
            }

            evaluated_args.erase(evaluated_args.begin() + i);
        }
    }

    auto ternary_to_field = [](Ternary::ResultType value) -> Field
    {
        if (value == Ternary::False)
            return 0U;
        else if (value == Ternary::True)
            return 1U;
        else if (value == Ternary::Null)
            return Null{};
        else
            throw Exception("Unknown ternary value: " + std::to_string(value), ErrorCodes::LOGICAL_ERROR);
    };

    // Rewrite strategies:
    // 1. rewrite to saturated value
    if (has_const && FunctionImpl::isSaturatedValueTernary(const_res))
    {
        simplify_result = Visitor::toColumnWithType(function_return_type, ternary_to_field(const_res));
    }
    // 2. remove neutral constant value
    else if (has_const && FunctionImpl::isNeutralValueTernary(const_res)
             && (evaluated_args.size() >= 2 || evaluate_context.parentIsFunctionLogical())
             && (!function_return_type->isNullable() || has_nullable_unresolved_arg))
    {
        ASTPtr rewritten;
        if (evaluated_args.size() > 1)
        {
            rewritten = Visitor::createFunctionWithEvaluatedArgs(function, evaluated_args, evaluate_context);
        }
        else
        {
            rewritten = std::get<ASTPtr>(evaluated_args[0].second);
        }

        simplify_result = Visitor::rewriteToAST(rewritten, evaluate_context);
    }
    // 3. only combine constants
    else
    {
        if (has_const)
        {
            auto const_field = ternary_to_field(const_res);
            DataTypePtr const_type;

            if (const_field.isNull())
            {
                const_type = makeNullable(std::make_shared<DataTypeNothing>());
            }
            else
            {
                const_type = has_nullable_constant_arg ? makeNullable(std::make_shared<DataTypeUInt8>()) : std::make_shared<DataTypeUInt8>();
            }
            evaluated_args.push_back(Visitor::toColumnWithType(const_type, const_field));
        }

        simplify_result = Visitor::rewriteToAST(Visitor::createFunctionWithEvaluatedArgs(function, evaluated_args, evaluate_context),
                                                evaluate_context);
    }

    return true;
}

};

using AndRewriter = LogicalFunctionRewriter<NameAnd, AndImpl>;
using OrRewriter = LogicalFunctionRewriter<NameOr, OrImpl>;

template <typename FunctionName, typename SimplifyLogic>
bool simplifyFunction(const ASTFunction & function, SimplifyLogic && do_simplify)
{
    if (function.name != FunctionName::name)
        return false;

    return do_simplify();
}

struct NameIsNull
{
    static constexpr auto name = "isNull";
};

struct NameIsNotNull
{
    static constexpr auto name = "isNotNull";
};

AstOrColumnWithType Visitor::processOrdinaryFunction(const ConstASTPtr & node, EvaluateContext & context)
{
    const auto & function = node->as<const ASTFunction &>();
    auto overload_resolver = FunctionFactory::instance().get(function.name, context.context);
    std::vector<AstOrColumnWithType> evaluated_args;
    bool all_const = true;
    bool has_null_constant = false;
    bool unknown_type_arg_exists = false;

    // Evaluate argument recursively, also check if all arguments are constant & there are null constant arguments
    if (function.arguments)
    {
        const auto & args = function.arguments->as<const ASTExpressionList &>().children;
        std::transform(args.begin(), args.end(), std::back_inserter(evaluated_args), [&, this](auto & child) {
            auto res = process(child, context, node);

            if (!res.first)
                unknown_type_arg_exists = true;

            if (std::holds_alternative<ASTPtr>(res.second))
                all_const = false;
            else if (std::get<ColumnPtr>(res.second)->onlyNull())
                has_null_constant = true;
            return res;
        });
    }

    if (unknown_type_arg_exists)
        return {nullptr, createFunctionWithEvaluatedArgs(function, evaluated_args, context)};

    auto columns_with_type = convertToColumns(evaluated_args);
    FunctionBasePtr function_base = overload_resolver->build(columns_with_type);
    auto thistype = function_base->getResultType();
    Utils::checkState(thistype != nullptr, "Function return type is null: " + serializeAST(*node));

    // Constant folding.
    // Note that the prerequisite of constant folding in ce slightly diff with that in cnch, this is intentional. i.e.,
    //   In ce, constant folding requires `function_base->isSuitableForConstantFolding() == true` and `isColumnConst(*res_col)`
    //   In cnch, constant folding requires `function_base->isDeterministic() == true` and `function_base->isSuitableForConstantFolding() == true`
    // This is because some functions do not satisfy `isColumnConst(*res_col)` in cnch, which cause constant folding not work and
    // furthermore block other optimizations(e.g. outer join to inner join)
    if (function_base->isSuitableForConstantFolding())
    {
        ColumnPtr res_col;

        if (all_const)
        {
            res_col = function_base->execute(columns_with_type, thistype, 1, false);
        }
        else
        {
            res_col = function_base->getConstantResultForNonConstArguments(columns_with_type, thistype);
        }

        if (res_col && isColumnConst(*res_col) && res_col->size() == 1)
            return {thistype, res_col};
    }

    // Null simplify.
    if (has_null_constant && overload_resolver->useDefaultImplementationForNulls())
        return toColumnWithType(thistype, Null());

    // Simplify function.
    // TODO: simplify CASE expr
    bool simplified;
    AstOrColumnWithType simplify_result;

    simplified = AndRewriter::apply(function, evaluated_args, simplify_result, thistype, context)
        || OrRewriter::apply(function, evaluated_args, simplify_result, thistype, context)
        || simplifyFunction<NameIsNull>(function,
                [&]()
                {
                    if (!evaluated_args.at(0).first->isNullable())
                    {
                        simplify_result = toColumnWithType(thistype, 0U);
                        return true;
                    }

                    return false;
                })
        || simplifyFunction<NameIsNotNull>(function,
                [&]()
                {
                    if (!evaluated_args.at(0).first->isNullable())
                    {
                        simplify_result = toColumnWithType(thistype, 1U);
                        return true;
                    }

                    return false;
                });

    if (simplified)
        return simplify_result;
    else
        return {thistype, createFunctionWithEvaluatedArgs(function, evaluated_args, context)};
}

ColumnsWithTypeAndName Visitor::convertToColumns(const std::vector<AstOrColumnWithType> & arguments)
{
    ColumnsWithTypeAndName columns(arguments.size());
    int col_idx = 0;

    std::transform(arguments.begin(), arguments.end(), columns.begin(), [&col_idx](const auto & arg) -> ColumnWithTypeAndName {
        ColumnPtr col = std::holds_alternative<ColumnPtr>(arg.second) ? std::get<ColumnPtr>(arg.second) : nullptr;
        String col_name = "_arg" + std::to_string(++col_idx);
        return ColumnWithTypeAndName(col, arg.first, col_name);
    });

    return columns;
}

ASTPtr Visitor::createFunctionWithEvaluatedArgs(
    const ASTFunction & function, const std::vector<AstOrColumnWithType> & evaluated_args, EvaluateContext & evaluate_context)
{
    ASTs new_args(evaluated_args.size());
    //auto arg_to_ast = std::bind(&Visitor::toAST, std::placeholders::_1, evaluate_context); // NOLINT(modernize-avoid-bind)
    // std::transform(evaluated_args.begin(), evaluated_args.end(), new_args.begin(), arg_to_ast);
    std::transform(evaluated_args.begin(), evaluated_args.end(), new_args.begin(), [&](const AstOrColumnWithType& ast_or_column_with_type){
        return toAST(ast_or_column_with_type, evaluate_context);
    });

    auto replaced_func = std::make_shared<ASTFunction>(function);
    replaced_func->children.clear();
    replaced_func->arguments = std::make_shared<ASTExpressionList>(function.arguments->as<ASTExpressionList &>().separator);
    replaced_func->arguments->children = new_args;
    replaced_func->children.push_back(replaced_func->arguments);

    return replaced_func;
}

AstOrColumnWithType Visitor::simplifyIn(const ConstASTPtr & node, EvaluateContext & context)
{
    const auto & function = node->as<const ASTFunction &>();
    auto in_value = process(function.arguments->as<ASTExpressionList &>().children[0], context, node);
    bool in_value_is_constant = std::holds_alternative<ColumnPtr>(in_value.second);
    auto & in_source = function.arguments->as<ASTExpressionList &>().children[1];

    if (!in_value.first)
    {
        auto rewritten_expr = makeASTFunction(function.name, toAST(in_value, context), in_source);
        return {nullptr, rewritten_expr};
    }

    auto thistype = context.getType(node);

    // special case: x in (NULL), NULL in (....)
    if (thistype->onlyNull())
        return {nullptr, node->clone()};

    if (removeNullable(in_value.first)->getTypeId() == TypeIndex::Tuple)
        return {thistype, node->clone()};

    if (!in_value_is_constant)
    {
        const auto * in_source_as_function = in_source->as<ASTFunction>();
        if (!in_source_as_function || (in_source_as_function->name != "tuple" && in_source_as_function->name != "array"))
        {
            auto evaluated_in_source = process(in_source, context, node);
            return rewriteToAST(makeASTFunction(function.name, toAST(in_value, context), toAST(evaluated_in_source, context)), context);
        }

        // process elements of in list
        auto & in_list = in_source_as_function->arguments->as<ASTExpressionList &>().children;
        ASTs unresolved_elems;

        for (auto & in_list_elem : in_list)
        {
            auto evaluate_elem = toAST(process(in_list_elem, context, node), context, false);
            unresolved_elems.push_back(evaluate_elem);
        }

        auto rewritten_in_value = toAST(in_value, context);
        auto rewritten_in_source = makeASTFunction("tuple", std::move(unresolved_elems));
        return rewriteToAST(makeASTFunction(function.name, rewritten_in_value, rewritten_in_source), context);
    }

    // constant folding for IN function(see also ActionsVisitor)
    auto & left_arg_type = in_value.first;
    DataTypes set_element_types = {left_arg_type};
    const auto * left_tuple_type = typeid_cast<const DataTypeTuple *>(left_arg_type.get());
    if (left_tuple_type && left_tuple_type->getElements().size() != 1)
        set_element_types = left_tuple_type->getElements();

    for (auto & element_type : set_element_types)
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(element_type.get()))
            element_type = low_cardinality_type->getDictionaryType();

    Block block;
    auto right_arg_function = std::dynamic_pointer_cast<ASTFunction>(in_source);
    if (right_arg_function && (right_arg_function->name == "tuple" || right_arg_function->name == "array"))
        block = createBlockForSet(left_arg_type, right_arg_function, set_element_types, context.context);
    else
        block = createBlockForSet(left_arg_type, in_source, set_element_types, context.context);

    const auto & settings = context.context->getSettingsRef();
    SizeLimits size_limits {settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode};
    SetPtr set = std::make_shared<Set>(size_limits, false,settings.transform_null_in);
    set->setHeader(block.cloneEmpty());
    set->insertFromBlock(block);
    set->finishInsert();

    auto column_set = ColumnSet::create(1, set);
    ColumnPtr const_column_set = ColumnConst::create(std::move(column_set), 1);
    ColumnsWithTypeAndName columns_with_types;
    columns_with_types.emplace_back(std::get<ColumnPtr>(in_value.second), in_value.first, "");
    columns_with_types.emplace_back(const_column_set, std::make_shared<DataTypeSet>(), "");
    auto result = FunctionInvoker::execute(function.name, columns_with_types, context.context);
    return toColumnWithType(result.type, result.value);
}

}
