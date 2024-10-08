#include <Interpreters/PreparedStatement/PreparedStatementLoaderFromDisk.h>
#include <Interpreters/PreparedStatement/PreparedStatementManager.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/atomicRename.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <common/logger_useful.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Protos/plan_node.pb.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Logger.h>

#include <filesystem>
#include <memory>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int PREPARED_STATEMENT_ALREADY_EXISTS;
    extern const int PREPARED_STATEMENT_NOT_EXISTS;
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

PreparedStatementLoaderFromDisk::PreparedStatementLoaderFromDisk(const String & dir_path_)
    : dir_path{makeDirectoryPathCanonical(dir_path_)}, log{getLogger("PreparedStatementLoaderFromDisk")}
{
    createDirectory();
}

std::optional<Protos::PreparedStatement> PreparedStatementLoaderFromDisk::tryGetPreparedObject(
    const String & statement_name, const String & path, ContextPtr /*context*/, bool check_file_exists)
{
    std::shared_lock lock(mutex);
    LOG_DEBUG(log, "Loading prepared statement {} from file {}", backQuote(statement_name), path);

    try
    {
        if (check_file_exists && !fs::exists(path))
            return std::nullopt;

        std::ifstream fin(path, std::ios::binary);
        Protos::PreparedStatement pb;
        pb.ParseFromIstream(&fin);
        return pb;
    }
    catch (...)
    {
        tryLogCurrentException(
            log, fmt::format("while loading prepared statement SQL object {} from path {}", backQuote(statement_name), path));
        return std::nullopt; /// Failed to load this sql object, will ignore it
    }
}

NamesAndPreparedStatements PreparedStatementLoaderFromDisk::getAllObjects(ContextPtr context)
{
    LOG_DEBUG(log, "Loading Prepared Statements from {}", dir_path);
    NamesAndPreparedStatements statements;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        if (it->isDirectory())
            continue;

        const String & file_name = it.name();
        if (!endsWith(file_name, ".bin"))
            continue;

        size_t prefix_length;
        if (startsWith(file_name, "prepared_"))
        {
            prefix_length = strlen("prepared_");
        }
        else
        {
            continue;
        }

        size_t suffix_length = strlen(".bin");
        String statement_name = unescapeForFileName(file_name.substr(prefix_length, file_name.length() - prefix_length - suffix_length));

        if (statement_name.empty())
            continue;

        auto statement = tryGetPreparedObject(statement_name, dir_path + it.name(), context, /* check_file_exists= */ false);
        if (!statement)
            continue;
        statements.emplace_back(std::move(statement_name), std::move(*statement));
    }

    LOG_DEBUG(log, "Prepared Statements loaded");
    return statements;
}

void PreparedStatementLoaderFromDisk::createDirectory()
{
    std::error_code create_dir_error_code;
    fs::create_directories(dir_path, create_dir_error_code);
    if (!fs::exists(dir_path) || !fs::is_directory(dir_path) || create_dir_error_code)
        throw Exception(
            "Couldn't create directory " + dir_path + " reason: '" + create_dir_error_code.message() + "'",
            ErrorCodes::DIRECTORY_DOESNT_EXIST);
}


bool PreparedStatementLoaderFromDisk::storeObject(
    const String & statement_name, const Protos::PreparedStatement & prepared_statement, bool throw_if_exists, bool replace_if_exists)
{
    std::unique_lock lock(mutex);
    String file_path = getFilePath(statement_name);
    LOG_DEBUG(log, "Storing Prepared Statement {} to file {}", backQuote(statement_name), file_path);

    if (fs::exists(file_path))
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::PREPARED_STATEMENT_ALREADY_EXISTS, "Prepared Statement '{}' already exists", statement_name);
        else if (!replace_if_exists)
            return false;
    }

    String temp_file_path = file_path + ".tmp";
    try
    {
        std::ofstream fout(temp_file_path, std::ios::binary);
        prepared_statement.SerializeToOstream(&fout);

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

    LOG_DEBUG(log, "Prepared Statement {} stored", backQuote(statement_name));
    return true;
}


bool PreparedStatementLoaderFromDisk::removeObject(const String & statement_name, bool throw_if_not_exists)
{
    std::unique_lock lock(mutex);
    String file_path = getFilePath(statement_name);
    LOG_DEBUG(log, "Removing Prepared Statement object {} stored in file {}", backQuote(statement_name), file_path);

    bool existed = fs::remove(file_path);

    if (!existed)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::PREPARED_STATEMENT_NOT_EXISTS, "Prepared Statement '{}' doesn't exist", statement_name);
        else
            return false;
    }

    LOG_DEBUG(log, "Prepared Statement {} removed", backQuote(statement_name));
    return true;
}


String PreparedStatementLoaderFromDisk::getFilePath(const String & statement_name) const
{
    String file_path = dir_path + "prepared_" + statement_name + ".bin";
    return file_path;
}

}
