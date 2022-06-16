#include <Optimizer/Utils.h>

#include <Functions/FunctionFactory.h>
#include <Optimizer/ExpressionExtractor.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace Utils
{

void assertIff(bool expression1, bool expression2)
{
    bool expression = (!(expression1) || (expression2)) && (!(expression2) || (expression1));
    if (!expression)
        throw Exception("Illegal State", ErrorCodes::LOGICAL_ERROR);
}

void checkState(bool expression)
{
    if (!expression)
    {
        throw Exception("Illegal State", ErrorCodes::LOGICAL_ERROR);
    }
}

void checkState(bool expression, const String & msg)
{
    if (!expression)
    {
        throw Exception("Illegal State: " + msg, ErrorCodes::LOGICAL_ERROR);
    }
}

void checkArgument(bool expression)
{
    if (!expression)
    {
        throw Exception("Illegal Argument", ErrorCodes::LOGICAL_ERROR);
    }
}

void checkArgument(bool expression, const String & msg)
{
    if (!expression)
    {
        throw Exception("Illegal Argument: " + msg, ErrorCodes::LOGICAL_ERROR);
    }
}

bool isIdentity(const Assignments & assignments)
{
    bool is_identity = true;
    for (auto & assignment : assignments)
    {
        String symbol = assignment.first;
        auto value = assignment.second;
        if (value->as<const ASTIdentifier>())
        {
            auto & identifier = value->as<const ASTIdentifier &>();
            if (symbol != identifier.name())
            {
                is_identity = false;
                break;
            }
        }
        else
        {
            is_identity = false;
            break;
        }
    }
    return is_identity;
}

bool isIdentity(const ProjectionStep & step)
{
    return !step.isFinalProject() && step.getDynamicFilters().empty() && Utils::isIdentity(step.getAssignments());
}

std::unordered_map<String, String> computeIdentityTranslations(Assignments & assignments)
{
    std::unordered_map<String, String> output_to_input;
    for (auto & assignment : assignments)
    {
        if (auto identifier = assignment.second->as<ASTIdentifier>())
        {
            output_to_input[assignment.first] = identifier->name();
        }
    }
    return output_to_input;
}

bool checkFunctionName(const ASTFunction & function, const String & expect_name)
{
    if (function.name == expect_name)
        return true;

    auto res = FunctionFactory::instance().getCanonicalName(function.name);

    if (res)
    {
        auto & canonical_name = *res;
        return canonical_name == expect_name ||
            (FunctionFactory::instance().isCaseInsensitive(canonical_name) && canonical_name == Poco::toLower(expect_name));
    }

    return false;
}

bool astNodeEquals(const ASTFunction & left, const ASTFunction & right)
{
    return left.name == right.name &&
        left.window_name == right.window_name &&
        astTreeEquals(left.window_definition, right.window_definition);
}

bool astNodeEquals(const ASTLiteral & left, const ASTLiteral & right)
{
    return left.value == right.value;
}

bool astNodeEquals(const ASTIdentifier & left, const ASTIdentifier & right)
{
    return left.name() == right.name();
}

bool astNodeEquals(const ASTExpressionList &, const ASTExpressionList &)
{
    return true;
}

bool astNodeEquals(const ASTWindowDefinition & left, const ASTWindowDefinition & right)
{
    return left.parent_window_name == right.parent_window_name &&
        left.frame_type == right.frame_type &&
        left.frame_begin_type == right.frame_begin_type &&
        astTreeEquals(left.frame_begin_offset, right.frame_begin_offset) &&
        left.frame_begin_preceding == right.frame_begin_preceding &&
        left.frame_end_type == right.frame_end_type&&
        astTreeEquals(left.frame_end_offset, right.frame_end_offset) &&
        left.frame_end_preceding == right.frame_end_preceding;
}

bool astNodeEquals(const ASTSubquery & left, const ASTSubquery & right)
{
    return left.cte_name == right.cte_name && left.database_of_view == right.database_of_view;
}

bool astNodeEquals(const ASTOrderByElement & left, const ASTOrderByElement & right)
{
    return left.direction == right.direction && left.nulls_direction == right.nulls_direction
        && left.nulls_direction_was_explicitly_specified == right.nulls_direction_was_explicitly_specified
        && left.with_fill == right.with_fill && astTreeEquals(left.collation, right.collation)
        && astTreeEquals(left.fill_from, right.fill_from) && astTreeEquals(left.fill_to, right.fill_to)
        && astTreeEquals(left.fill_step, right.fill_step);
}

bool astNodeEquals(const ASTSetQuery & left, const ASTSetQuery & right)
{
    return left.changes == right.changes;
}

bool astTreeEquals(const IAST & left, const IAST & right)
{
    if (left.getType() != right.getType())
        return false;

    if (left.children.size() != right.children.size())
        return false;

    bool node_equals;

    switch (left.getType())
    {
        case ASTType::ASTLiteral:
            node_equals = astNodeEquals(left.as<ASTLiteral &>(), right.as<ASTLiteral &>());
            break;
        case ASTType::ASTFunction:
            node_equals = astNodeEquals(left.as<ASTFunction &>(), right.as<ASTFunction &>());
            break;
        case ASTType::ASTIdentifier:
            node_equals = astNodeEquals(left.as<ASTIdentifier &>(), right.as<ASTIdentifier &>());
            break;
        case ASTType::ASTExpressionList:
            node_equals = astNodeEquals(left.as<ASTExpressionList &>(), right.as<ASTExpressionList &>());
            break;
        case ASTType::ASTWindowDefinition:
            node_equals = astNodeEquals(left.as<ASTWindowDefinition &>(), right.as<ASTWindowDefinition &>());
            break;
        case ASTType::ASTSubquery:
            node_equals = astNodeEquals(left.as<ASTSubquery&>(), right.as<ASTSubquery&>());
            break;
        case ASTType::ASTArrayJoin:
            node_equals = left.as<ASTArrayJoin&>().kind == right.as<ASTArrayJoin&>().kind;
            break;
        case ASTType::ASTOrderByElement:
            node_equals = astNodeEquals(left.as<ASTOrderByElement&>(), right.as<ASTOrderByElement&>());
            break;
        case ASTType::ASTSetQuery:
            node_equals = astNodeEquals(left.as<ASTSetQuery&>(), right.as<ASTSetQuery&>());
            break;
        case ASTType::ASTSelectQuery:
        case ASTType::ASTSelectWithUnionQuery:
        case ASTType::ASTTablesInSelectQuery:
        case ASTType::ASTTablesInSelectQueryElement:
        case ASTType::ASTTableExpression:
        default:
            node_equals = true;
    }

    if (!node_equals)
        return false;

    for (int i = 0; i < static_cast<int>(left.children.size()); ++i)
        if (!astTreeEquals(left.children[i], right.children[i]))
            return false;

    return true;
}

bool ConstASTPtrOrdering::operator()(const ConstASTPtr & predicate_1, const ConstASTPtr & predicate_2) const
{
    size_t symbol_size_1 = SymbolsExtractor::extract(predicate_1).size();
    size_t symbol_size_2 = SymbolsExtractor::extract(predicate_2).size();
    if (symbol_size_1 != symbol_size_2)
        return symbol_size_1 < symbol_size_2;

    size_t sub_expression_size_1 = SubExpressionExtractor::extract(predicate_1).size();
    size_t sub_expression_size_2 = SubExpressionExtractor::extract(predicate_2).size();
    if (sub_expression_size_1 != sub_expression_size_2)
        return sub_expression_size_1 < sub_expression_size_2;

    return predicate_1->getColumnName() < predicate_2->getColumnName();
}

}
}
