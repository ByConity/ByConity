#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>

#include <vector>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/UserDefinedExternalFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/broadcastUDFQueryToCluster.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
    extern const int CANNOT_DROP_FUNCTION;
    extern const int CANNOT_CREATE_RECURSIVE_FUNCTION;
    extern const int UNSUPPORTED_METHOD;
}


namespace
{
    void validateFunctionRecursiveness(const IAST & node, const String & function_to_create)
    {
        for (const auto & child : node.children)
        {
            auto function_name_opt = tryGetFunctionName(child);
            if (function_name_opt && function_name_opt.value() == function_to_create)
                throw Exception(ErrorCodes::CANNOT_CREATE_RECURSIVE_FUNCTION, "You cannot create recursive function");

            validateFunctionRecursiveness(*child, function_to_create);
        }
    }

    void validateFunction(ASTPtr function, const String & name)
    {
        ASTFunction * lambda_function = function->as<ASTFunction>();

        if (!lambda_function)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Expected function, got: {}", function->formatForErrorMessage());

        auto & lambda_function_expression_list = lambda_function->arguments->children;

        if (lambda_function_expression_list.size() != 2)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have arguments and body");

        const ASTFunction * tuple_function_arguments = lambda_function_expression_list[0]->as<ASTFunction>();

        if (!tuple_function_arguments || !tuple_function_arguments->arguments)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have valid arguments");

        std::unordered_set<String> arguments;

        for (const auto & argument : tuple_function_arguments->arguments->children)
        {
            const auto * argument_identifier = argument->as<ASTIdentifier>();

            if (!argument_identifier)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda argument must be identifier");

            const auto & argument_name = argument_identifier->name();
            auto [_, inserted] = arguments.insert(argument_name);
            if (!inserted)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Identifier {} already used as function parameter", argument_name);
        }

        ASTPtr function_body = lambda_function_expression_list[1];
        if (!function_body)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have valid function body");

        validateFunctionRecursiveness(*function_body, name);
    }

    ASTPtr normalizeCreateFunctionQuery(const IAST & create_function_query)
    {
        auto ptr = create_function_query.clone();
        auto & res = typeid_cast<ASTCreateFunctionQuery &>(*ptr);
        res.if_not_exists = false;
        res.or_replace = false;
        FunctionNameNormalizer().visit(res.function_core.get());
        return ptr;
    }
}


UserDefinedSQLFunctionFactory & UserDefinedSQLFunctionFactory::instance()
{
    static UserDefinedSQLFunctionFactory result;
    return result;
}

void UserDefinedSQLFunctionFactory::checkCanBeRegistered(
    const ContextPtr &, const String & function_name, const IAST & create_function_query)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

    if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

    auto ast = assert_cast<const ASTCreateFunctionQuery &>(create_function_query);
    if (!ast.is_lambda)
        return;

    validateFunction(ast.function_core, function_name);
}

void UserDefinedSQLFunctionFactory::checkCanBeUnregistered(const ContextPtr &, const String & function_name)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name) || AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);
}


bool UserDefinedSQLFunctionFactory::registerFunction(
    const ContextMutablePtr & context,
    const String & resolved_function_name,
    const String & function_name,
    ASTPtr create_function_query,
    bool throw_if_exists,
    bool replace_if_exists)
{
    auto ast = assert_cast<const ASTCreateFunctionQuery &>(*create_function_query);
    auto & external = UserDefinedExternalFunctionFactory::instance();

    checkCanBeRegistered(context, function_name, *create_function_query);
    create_function_query = normalizeCreateFunctionQuery(*create_function_query);

    std::lock_guard lock{mutex};
    auto it = function_name_to_create_query_map.find(resolved_function_name);
    if (it != function_name_to_create_query_map.end() || external.has(resolved_function_name))
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User-defined function '{}' already exists", resolved_function_name);
        else if (!replace_if_exists)
            return false;
    }

    try
    {
        if (!ast.is_lambda)
            return external.setFunction(resolved_function_name, *create_function_query, context);
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while storing user defined function {}", backQuote(resolved_function_name)));
        throw;
    }

    function_name_to_create_query_map[resolved_function_name] = create_function_query;
    return true;
}

bool UserDefinedSQLFunctionFactory::unregisterFunction(
    const ContextMutablePtr & context, const String & db_name, const String & function_name, bool throw_if_not_exists, bool cnch_local)
{
    checkCanBeUnregistered(context, function_name);

    const String & resolved_function_name = getResolvedFunctionName(db_name, function_name);
    if (cnch_local)
    {
        return dropFunctionLocally(context, resolved_function_name, throw_if_not_exists);
    }

    try
    {
        auto & loader = context->getUserDefinedSQLObjectsLoader();
        bool removed = loader.removeObjectOnCatalog(db_name, function_name);
        if (!removed)
            return false;
    }
    catch (Exception & e)
    {
        if (throw_if_not_exists)
            throw e;
    }

    if (context->getServerType() != ServerType::cnch_server)
    {
        return true;
    }

    dropFunctionLocally(context, resolved_function_name, throw_if_not_exists);
    return true;
}

bool UserDefinedSQLFunctionFactory::dropFunctionLocally(
    const ContextMutablePtr & context, const String & resolved_function_name, bool throw_if_not_exists)
{
    auto & external = UserDefinedExternalFunctionFactory::instance();
    UserDefinedSQLObjectType obj_type = UserDefinedSQLObjectType::ExternalFunction;
    std::lock_guard lock(mutex);
    auto it = function_name_to_create_query_map.find(resolved_function_name);

    // erase map for lambda udfs
    if (it != function_name_to_create_query_map.end())
    {
        function_name_to_create_query_map.erase(resolved_function_name);
        return true;
    }

    if (!external.has(resolved_function_name))
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", resolved_function_name);
        else
            return false;
    }

    try
    {
        auto & loader = context->getUserDefinedSQLObjectsLoader();
        bool removed = loader.removeObject(obj_type, resolved_function_name, throw_if_not_exists);
        if (!removed)
            return false;
        external.removeFunction(resolved_function_name);
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while removing user defined function {}", backQuote(resolved_function_name)));
        throw;
    }
    return true;
}

ASTPtr UserDefinedSQLFunctionFactory::get(const String & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query_map.find(function_name);
    if (it == function_name_to_create_query_map.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "The function name '{}' is not registered", function_name);

    return it->second;
}

ASTPtr UserDefinedSQLFunctionFactory::tryGet(const String & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query_map.find(function_name);
    if (it == function_name_to_create_query_map.end())
        return nullptr;

    return it->second;
}


bool UserDefinedSQLFunctionFactory::tryGetVersion(const String & function_name, uint64_t ** version_ptr) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query_map.find(function_name);
    if (it != function_name_to_create_query_map.end())
    {
        *version_ptr = &it->second->as<ASTCreateFunctionQuery>()->version;
        return true;
    }

    return false;
}

bool UserDefinedSQLFunctionFactory::has(const String & function_name) const
{
    return tryGet(function_name) != nullptr;
}

std::vector<std::string> UserDefinedSQLFunctionFactory::getAllRegisteredNames() const
{
    std::vector<std::string> registered_names;

    std::lock_guard lock(mutex);
    registered_names.reserve(function_name_to_create_query_map.size());

    for (const auto & [name, _] : function_name_to_create_query_map)
        registered_names.emplace_back(name);

    return registered_names;
}

bool UserDefinedSQLFunctionFactory::empty() const
{
    std::lock_guard lock(mutex);
    return function_name_to_create_query_map.empty();
}

void UserDefinedSQLFunctionFactory::backup(BackupEntriesCollector &, const String &) const
{
}

void UserDefinedSQLFunctionFactory::restore(RestorerFromBackup &, const String &)
{
}

void UserDefinedSQLFunctionFactory::setAllFunctions(const std::vector<std::pair<String, ASTPtr>> & new_functions)
{
    std::unordered_map<String, ASTPtr> normalized_functions;
    for (const auto & [function_name, create_query] : new_functions)
        normalized_functions[function_name] = normalizeCreateFunctionQuery(*create_query);

    std::lock_guard lock(mutex);
    function_name_to_create_query_map = std::move(normalized_functions);
}

std::vector<std::pair<String, ASTPtr>> UserDefinedSQLFunctionFactory::getAllFunctions() const
{
    std::lock_guard lock{mutex};
    std::vector<std::pair<String, ASTPtr>> all_functions;
    all_functions.reserve(function_name_to_create_query_map.size());
    std::copy(function_name_to_create_query_map.begin(), function_name_to_create_query_map.end(), std::back_inserter(all_functions));
    return all_functions;
}

void UserDefinedSQLFunctionFactory::setFunction(const String & function_name, const IAST & create_function_query, const ContextPtr &)
{
    std::lock_guard lock(mutex);
    function_name_to_create_query_map[function_name] = normalizeCreateFunctionQuery(create_function_query);
}

void UserDefinedSQLFunctionFactory::removeFunction(const String & function_name)
{
    std::lock_guard lock(mutex);
    function_name_to_create_query_map.erase(function_name);
}

void UserDefinedSQLFunctionFactory::removeAllFunctionsExcept(const Strings & function_names_to_keep)
{
    boost::container::flat_set<std::string_view> names_set_to_keep{function_names_to_keep.begin(), function_names_to_keep.end()};
    std::lock_guard lock(mutex);
    for (auto it = function_name_to_create_query_map.begin(); it != function_name_to_create_query_map.end();)
    {
        auto current = it++;
        if (!names_set_to_keep.contains(current->first))
            function_name_to_create_query_map.erase(current);
    }
}

std::unique_lock<std::recursive_mutex> UserDefinedSQLFunctionFactory::getLock() const
{
    return std::unique_lock{mutex};
}

String UserDefinedSQLFunctionFactory::getResolvedFunctionName(const String & database_name, const String & function_name)
{
    if (function_name.empty())
        throw Exception("Function name can't be empty ", ErrorCodes::UNKNOWN_FUNCTION);
    if (database_name.empty())
        return function_name;
    // A hack, fix later with parser
    if (function_name.find('.') != String::npos)
    {
        return function_name;
    }
    return database_name + "." + function_name;
}


}
