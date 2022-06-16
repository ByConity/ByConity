#pragma once

#include <Parsers/ASTFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

enum class FunctionType
{
    UNKNOWN,
    FUNCTION,
    AGGREGATE_FUNCTION,
    WINDOW_FUNCTION,
    GROUPING_OPERATION,
    LAMBDA_EXPRESSION,
    EXISTS_SUBQUERY,
    IN_SUBQUERY,
};

FunctionType getFunctionType(const ASTFunction & function, ContextPtr context);

ASTs getLambdaExpressionArguments(ASTFunction & lambda);
ASTPtr getLambdaExpressionBody(ASTFunction & lambda);

bool isComparisonFunction(const ASTFunction & function);
bool functionIsInSubquery(const ASTFunction & function);

}
