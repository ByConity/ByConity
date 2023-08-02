#include "UserDefinedSQLFunctionVisitor.h"

#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExternalFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

bool UserDefinedSQLFunctionMatcher::isSysFunc(const String & name)
{
    if (UserDefinedSQLFunctionFactory::instance().has(name) || UserDefinedExternalFunctionFactory::instance().has(name))
        return false;

    if (FunctionFactory::instance().hasNameOrAlias(name))
        return true;

    if (AggregateFunctionFactory::instance().isAggregateFunctionName(name))
        return true;

    if (TableFunctionFactory::instance().isTableFunctionName(name))
        return true;

    return false;
}

bool UserDefinedSQLFunctionMatcher::resolveFuncName(ASTFunction * fn, const String & db, String & name)
{
    if (isSysFunc(fn->name))
        return false;

    if (!fn->database.empty())
    {
        name = fn->name;
        return true;
    }

    name = UserDefinedSQLFunctionFactory::getResolvedFunctionName(db, fn->name);
    fn->name = name;
    return true;
}

void UserDefinedSQLFunctionMatcher::checkLocalVersions(
    ASTPtr & ast, ContextMutablePtr ctx, std::unordered_map<String, size_t> & mismatched_udfs)
{
    ASTFunction * fn = ast->as<ASTFunction>();

    if (!fn)
        return;

    const String & db = ctx->getCurrentDatabase();
    String name;

    if (!resolveFuncName(fn, db, name))
        return;

#define VERSION_MATCH(v1, v2) ((v1) == 0 || (v1) == (v2))
    uint64_t * version_ptr = nullptr;
    UserDefinedSQLFunctionFactory::instance().tryGetVersion(name, &version_ptr);
    if (version_ptr && VERSION_MATCH(fn->version, *version_ptr))
    {
        return;
    }

    version_ptr = nullptr;
    UserDefinedExternalFunctionFactory::instance().tryGetVersion(name, &version_ptr);

    if (version_ptr && VERSION_MATCH(fn->version, *version_ptr))
    {
        ctx->setExternalUDFMapEntry(name, *version_ptr);
        return;
    }

    mismatched_udfs[name] = fn->version;
}

template <typename P, typename T>
void UserDefinedSQLFunctionMatcher::traverse(P & ast, ContextMutablePtr ctx, T & arg)
{
    checkLocalVersions(ast, ctx, arg);

    for (P & child : ast->children)
        traverse(child, ctx, arg);
}

void UserDefinedSQLFunctionMatcher::loadUDFs(ASTPtr & ast, ContextMutablePtr context)
{
    std::unordered_map<String, size_t> mismatched_udfs;

    traverse(ast, context, mismatched_udfs);

    auto & loader = context->getUserDefinedSQLObjectsLoader();

    loader.checkAndLoadUDFFromStorage(mismatched_udfs, context);
}

bool UserDefinedSQLFunctionMatcher::needChildVisit(const ASTPtr &, const ASTPtr &)
{
    return true;
}

ASTPtr
UserDefinedSQLFunctionMatcher::tryToReplaceFunction(const ASTFunction & function, std::unordered_set<std::string> & udf_in_replace_process)
{
    if (udf_in_replace_process.find(function.name) != udf_in_replace_process.end())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Recursive function call detected during function call {}", function.name);

    auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(function.name);
    if (!user_defined_function)
        return nullptr;

    const auto & function_arguments_list = function.children.at(0)->as<ASTExpressionList>();
    auto & function_arguments = function_arguments_list->children;

    const auto & create_function_query = user_defined_function->as<ASTCreateFunctionQuery>();
    auto & function_core_expression = create_function_query->function_core->children.at(0);

    const auto & identifiers_expression_list = function_core_expression->children.at(0)->children.at(0)->as<ASTExpressionList>();
    const auto & identifiers_raw = identifiers_expression_list->children;

    if (function_arguments.size() != identifiers_raw.size())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Function {} expects {} arguments actual arguments {}",
            create_function_query->getFunctionName(),
            identifiers_raw.size(),
            function_arguments.size());

    std::unordered_map<std::string, ASTPtr> identifier_name_to_function_argument;

    for (size_t parameter_index = 0; parameter_index < identifiers_raw.size(); ++parameter_index)
    {
        const auto & identifier = identifiers_raw[parameter_index]->as<ASTIdentifier>();
        const auto & function_argument = function_arguments[parameter_index];
        const auto & identifier_name = identifier->name();

        identifier_name_to_function_argument.emplace(identifier_name, function_argument);
    }

    auto [it, _] = udf_in_replace_process.emplace(function.name);

    auto function_body_to_update = function_core_expression->children.at(1)->clone();

    auto expression_list = std::make_shared<ASTExpressionList>();
    expression_list->children.emplace_back(std::move(function_body_to_update));

    std::stack<ASTPtr> ast_nodes_to_update;
    ast_nodes_to_update.push(expression_list);

    while (!ast_nodes_to_update.empty())
    {
        auto ast_node_to_update = ast_nodes_to_update.top();
        ast_nodes_to_update.pop();

        for (auto & child : ast_node_to_update->children)
        {
            if (auto * inner_function = child->as<ASTFunction>())
            {
                auto replace_result = tryToReplaceFunction(*inner_function, udf_in_replace_process);
                if (replace_result)
                    child = replace_result;
            }

            auto identifier_name_opt = tryGetIdentifierName(child);
            if (identifier_name_opt)
            {
                auto function_argument_it = identifier_name_to_function_argument.find(*identifier_name_opt);

                if (function_argument_it == identifier_name_to_function_argument.end())
                    continue;

                auto child_alias = child->tryGetAlias();
                child = function_argument_it->second->clone();

                if (!child_alias.empty())
                    child->setAlias(child_alias);

                continue;
            }

            ast_nodes_to_update.push(child);
        }
    }

    udf_in_replace_process.erase(it);

    function_body_to_update = expression_list->children[0];

    auto function_alias = function.tryGetAlias();

    if (!function_alias.empty())
        function_body_to_update->setAlias(function_alias);

    return function_body_to_update;
}


void UserDefinedSQLFunctionMatcher::visit(ASTPtr & ast, Data & data)
{
    if (!data.getContext()->hasQueryContext())
        return;

    auto * function = ast->as<ASTFunction>();
    if (!function)
        return;

    const auto & query_context = data.getContext()->getQueryContext();
    loadUDFs(ast, query_context);

    std::unordered_set<std::string> udf_in_replace_process;
    std::unordered_map<String, size_t> external_udfs;
    auto replace_result = tryToReplaceFunction(*function, udf_in_replace_process);
    if (replace_result)
    {
        ast = replace_result;
    }
}


}
