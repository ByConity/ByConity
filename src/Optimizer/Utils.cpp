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
