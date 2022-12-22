#include <Analyzers/function_utils.h>
#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

FunctionType getFunctionType(const ASTFunction & function, ContextPtr context)
{
    if (function.is_window_function)
        return FunctionType::WINDOW_FUNCTION;
    else if (AggregateFunctionFactory::instance().isAggregateFunctionName(function.name))
        return FunctionType::AGGREGATE_FUNCTION;
    else if (function.name == "grouping")
        return FunctionType::GROUPING_OPERATION;
    else if (functionIsInSubquery(function))
        return FunctionType::IN_SUBQUERY;
    else if (function.name == "exists")
        return FunctionType::EXISTS_SUBQUERY;
    else if (function.name == "lambda")
        return FunctionType::LAMBDA_EXPRESSION;
    else if (FunctionFactory::instance().tryGet(function.name, context))
        return FunctionType::FUNCTION;
    else
        return FunctionType::UNKNOWN;
}

ASTs getLambdaExpressionArguments(ASTFunction & lambda)
{
    if (lambda.arguments->children.size() != 2)
        throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * lambda_args_tuple = lambda.arguments->children.at(0)->as<ASTFunction>();

    if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
        throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

    return lambda_args_tuple->arguments->children;
}

ASTPtr getLambdaExpressionBody(ASTFunction & lambda)
{
    if (lambda.arguments->children.size() != 2)
        throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return lambda.arguments->children.at(1);
}

bool isComparisonFunction(const ASTFunction & function)
{
    return function.name == "equals" || function.name == "less" || function.name == "lessOrEquals"
        || function.name == "greater" || function.name == "greaterOrEquals";
}

bool functionIsInSubquery(const ASTFunction & function)
{
    return (function.name == "in" || function.name == "notIn" || function.name == "globalIn" || function.name == "globalNotIn")
        && function.arguments->children.size() == 2
        && function.arguments->children[1]->as<ASTSubquery>();
}

}
