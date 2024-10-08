#pragma once

#include <Common/Logger.h>
#include <Core/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <shared_mutex>

namespace DB
{

namespace Protos
{
    class PreparedStatement;
}

using NamesAndPreparedStatements = std::vector<std::pair<String, Protos::PreparedStatement>>;

class PreparedStatementLoaderFromDisk
{
public:
    explicit PreparedStatementLoaderFromDisk(const String & dir_path_);

    bool storeObject(
        const String & statement_name, const Protos::PreparedStatement & prepared_statement, bool throw_if_exists, bool replace_if_exists);

    bool removeObject(const String & statement_name, bool throw_if_not_exists);

    NamesAndPreparedStatements getAllObjects(ContextPtr context);

private:
    void createDirectory();
    std::optional<Protos::PreparedStatement>
    tryGetPreparedObject(const String & statement_name, const String & file_path, ContextPtr context, bool check_file_exists);
    String getFilePath(const String & statement_name) const;

    String dir_path;
    mutable std::shared_mutex mutex;
    LoggerPtr log;
};

}
