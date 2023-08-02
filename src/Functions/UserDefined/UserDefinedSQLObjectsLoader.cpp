#include "Functions/UserDefined/UserDefinedSQLObjectsLoader.h"

#include <Catalog/Catalog.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/atomicRename.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <common/logger_useful.h>
#include "Functions/UserDefined/UserDefinedExternalFunctionFactory.h"
#include "Functions/UserDefined/UserDefinedSQLFunctionFactory.h"
#include "Functions/UserDefined/UserDefinedSQLObjectType.h"

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <Interpreters/InterpreterCreateFunctionQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Logger.h>

#include <filesystem>
#include <unordered_map>
#include <unordered_set>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
}


namespace
{
    /// Converts a path to an absolute path and append it with a separator.
    String makeDirectoryPathCanonical(const String & directory_path)
    {
        auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path);
        if (canonical_directory_path.has_filename())
            canonical_directory_path += std::filesystem::path::preferred_separator;
        return canonical_directory_path;
    }
}

UserDefinedSQLObjectsLoader::UserDefinedSQLObjectsLoader(const ContextPtr & global_context_, const String & dir_path_)
    : global_context(global_context_)
    , dir_path{makeDirectoryPathCanonical(dir_path_)}
    , log{&Poco::Logger::get("UserDefinedSQLObjectsLoader")}
{
    createDirectory();
}


ASTPtr UserDefinedSQLObjectsLoader::tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name)
{
    return tryLoadObject(object_type, object_name, getFilePath(object_type, object_name), /* check_file_exists= */ true);
}


ASTPtr UserDefinedSQLObjectsLoader::tryLoadObject(
    UserDefinedSQLObjectType object_type, const String & object_name, const String & path, bool check_file_exists)
{
    LOG_DEBUG(log, "Loading user defined object {} from file {}", backQuote(object_name), path);

    try
    {
        if (check_file_exists && !fs::exists(path))
            return nullptr;

        /// There is .sql file with user defined object creation statement.
        ReadBufferFromFile in(path);

        String object_create_query;
        readStringUntilEOF(object_create_query, in);

        switch (object_type)
        {
            case UserDefinedSQLObjectType::ExternalFunction:
            case UserDefinedSQLObjectType::Function: {
                ParserCreateFunctionQuery parser;
                ASTPtr ast = parseQuery(
                    parser,
                    object_create_query.data(),
                    object_create_query.data() + object_create_query.size(),
                    "",
                    0,
                    global_context->getSettingsRef().max_parser_depth);
                UserDefinedSQLFunctionFactory::checkCanBeRegistered(global_context, object_name, *ast);
                return ast;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while loading user defined SQL object {} from path {}", backQuote(object_name), path));
        return nullptr; /// Failed to load this sql object, will ignore it
    }
}


void UserDefinedSQLObjectsLoader::checkAndLoadObjects()
{
    if (!objects_loaded)
        loadObjectsImpl();
}


void UserDefinedSQLObjectsLoader::reloadObjects()
{
    loadObjectsImpl();
}


void UserDefinedSQLObjectsLoader::loadObjectsImpl()
{
    LOG_INFO(log, "Loading user defined objects from {}", dir_path);
    createDirectory();

    std::vector<std::pair<String, ASTPtr>> function_names_and_queries;
    std::vector<std::pair<String, ASTPtr>> external_functions;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        if (it->isDirectory())
            continue;

        const String & file_name = it.name();
        if (!endsWith(file_name, ".sql"))
            continue;

        UserDefinedSQLObjectType obj_type;
        size_t prefix_length;
        if (startsWith(file_name, "external_"))
        {
            obj_type = UserDefinedSQLObjectType::ExternalFunction;
            prefix_length = strlen("external_");
        }
        else if (startsWith(file_name, "function_"))
        {
            obj_type = UserDefinedSQLObjectType::Function;
            prefix_length = strlen("function_");
        }
        else
        {
            continue;
        }

        size_t suffix_length = strlen(".sql");
        String function_name = unescapeForFileName(file_name.substr(prefix_length, file_name.length() - prefix_length - suffix_length));

        if (function_name.empty())
            continue;

        ASTPtr ast = tryLoadObject(obj_type, function_name, dir_path + it.name(), /* check_file_exists= */ false);
        if (ast)
        {
            if (obj_type == UserDefinedSQLObjectType::Function)
                function_names_and_queries.emplace_back(std::move(function_name), ast);
            else
                external_functions.emplace_back(std::move(function_name), ast);
        }
    }

    UserDefinedSQLFunctionFactory::instance().setAllFunctions(function_names_and_queries);
    UserDefinedExternalFunctionFactory::instance().setAllFunctions(external_functions, global_context);
    objects_loaded = true;

    LOG_DEBUG(log, "User defined objects loaded");
}

template <class T>
void UserDefinedSQLObjectsLoader::reloadObject(const String & object_name, ASTPtr ast)
{
    auto & factory = T::instance();
    if (ast)
        factory.setFunction(object_name, *ast, global_context);
    else
        factory.removeFunction(object_name);
}

void UserDefinedSQLObjectsLoader::reloadObject(UserDefinedSQLObjectType object_type, const String & object_name)
{
    createDirectory();
    auto ast = tryLoadObject(object_type, object_name);

    if (object_type == UserDefinedSQLObjectType::ExternalFunction)
        reloadObject<UserDefinedExternalFunctionFactory>(object_name, ast);
    else
        reloadObject<UserDefinedSQLFunctionFactory>(object_name, ast);
}

void UserDefinedSQLObjectsLoader::createDirectory()
{
    std::error_code create_dir_error_code;
    fs::create_directories(dir_path, create_dir_error_code);
    if (!fs::exists(dir_path) || !fs::is_directory(dir_path) || create_dir_error_code)
        throw Exception(
            "Couldn't create directory " + dir_path + " reason: '" + create_dir_error_code.message() + "'",
            ErrorCodes::DIRECTORY_DOESNT_EXIST);
}


bool UserDefinedSQLObjectsLoader::storeObject(
    UserDefinedSQLObjectType object_type,
    const String & object_name,
    const IAST & create_object_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings & settings)
{
    String file_path = getFilePath(object_type, object_name);
    LOG_DEBUG(log, "Storing user-defined object {} to file {}", backQuote(object_name), file_path);

    if (fs::exists(file_path))
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User-defined function '{}' already exists", object_name);
        else if (!replace_if_exists)
            return false;
    }

    WriteBufferFromOwnString create_statement_buf;
    formatAST(create_object_query, create_statement_buf, false);
    writeChar('\n', create_statement_buf);
    String create_statement = create_statement_buf.str();

    String temp_file_path = file_path + ".tmp";

    try
    {
        WriteBufferFromFile out(temp_file_path, create_statement.size());
        writeString(create_statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();

        if (replace_if_exists)
            fs::rename(temp_file_path, file_path);
        else
            renameNoReplace(temp_file_path, file_path);
    }
    catch (...)
    {
        fs::remove(temp_file_path);
        throw;
    }

    LOG_TRACE(log, "Object {} stored", backQuote(object_name));
    return true;
}


bool UserDefinedSQLObjectsLoader::removeObject(UserDefinedSQLObjectType object_type, const String & object_name, bool throw_if_not_exists)
{
    String file_path = getFilePath(object_type, object_name);
    LOG_DEBUG(log, "Removing user defined object {} stored in file {}", backQuote(object_name), file_path);

    bool existed = fs::remove(file_path);

    if (!existed)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", object_name);
        else
            return false;
    }

    LOG_TRACE(log, "Object {} removed", backQuote(object_name));
    return true;
}

bool UserDefinedSQLObjectsLoader::storeObjectOnCatalog(
    const String & database_name, const String & object_name, const IAST & create_object_query)
{
    WriteBufferFromOwnString buf;
    formatAST(create_object_query, buf, false);

    String create_statement = buf.str();
    auto catalog = global_context->getCnchCatalog();
    catalog->createUDF(database_name, object_name, create_statement);
    LOG_TRACE(log, "Object {} stored", backQuote(object_name));
    return true;
}

bool UserDefinedSQLObjectsLoader::removeObjectOnCatalog(const String & database_name, const String & object_name)
{
    auto catalog = global_context->getCnchCatalog();
    catalog->dropUDF(database_name, object_name);
    LOG_TRACE(log, "Object {} removed", backQuote(object_name));
    return true;
}

void UserDefinedSQLObjectsLoader::checkAndLoadUDFFromStorage(
    const std::unordered_map<String, size_t> & udfs, ContextMutablePtr query_context)
{
    if (udfs.empty())
    {
        return;
    }
    uint64_t * ver_ptr;
    std::unordered_set<String> udf_to_fetch_from_storage;
    UserDefinedSQLFunctionFactory & factory = UserDefinedSQLFunctionFactory::instance();
    UserDefinedExternalFunctionFactory & externals = UserDefinedExternalFunctionFactory::instance();

    for (const auto & udf : udfs)
    {
        ver_ptr = nullptr;

        if (factory.tryGetVersion(udf.first, &ver_ptr) || externals.tryGetVersion(udf.first, &ver_ptr))
        {
            if (*ver_ptr == udf.second)
            {
                continue;
            }

            // Drop query if version check is required and it doesn't match existing version.
            // TODO: don't drop function, instead use create of replace function.
            // There won't be concurrency issues anymore.
            factory.dropFunctionLocally(query_context, udf.first, false);
        }

        udf_to_fetch_from_storage.insert(udf.first);
    }

    if (!udf_to_fetch_from_storage.empty())
    {
        loadUDFFromStorage(udf_to_fetch_from_storage, query_context);
    }
}

void UserDefinedSQLObjectsLoader::loadUDFFromStorage(
    const std::unordered_set<String> & udf_to_fetch_from_storage, ContextMutablePtr query_context)
{
    if (udf_to_fetch_from_storage.empty())
    {
        return;
    }
    std::vector<Protos::DataModelUDF> models;

    LOG_DEBUG(log, "Loading " + std::to_string(udf_to_fetch_from_storage.size()) + " udfs from ByteKV");

    {
        auto c = query_context->tryGetCnchCatalog();

        if (!c)
            return;

        models = c->getUDFByName(udf_to_fetch_from_storage);
    }

    for (size_t i = 0; i < udf_to_fetch_from_storage.size(); i++)
    {
        auto udfModel = models[i];
        if (udfModel.function_definition().empty())
        {
            continue;
        }
        auto object_create_query = udfModel.function_definition();
        ParserCreateFunctionQuery parser;
        ASTPtr ast = parseQuery(
            parser,
            object_create_query.data(),
            object_create_query.data() + object_create_query.size(),
            "not in file ",
            0,
            query_context->getSettingsRef().max_parser_depth);

        InterpreterCreateFunctionQuery interpreter(ast, query_context, true /*is internal*/);
        interpreter.execute();

        ASTCreateFunctionQuery & create_function_query = ast->as<ASTCreateFunctionQuery &>();
        if (!create_function_query.is_lambda)
        {
            query_context->setExternalUDFMapEntry(create_function_query.function_name, create_function_query.version);
        }
    }
}


String UserDefinedSQLObjectsLoader::getFilePath(UserDefinedSQLObjectType object_type, const String & object_name) const
{
    String file_path;
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function: {
            file_path = dir_path + "function_" + object_name + ".sql";
            break;
        }
        case UserDefinedSQLObjectType::ExternalFunction: {
            file_path = dir_path + "external_" + object_name + ".sql";
            break;
        }
    }
    return file_path;
}

}
