#pragma once

#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_set>

namespace DB
{

/// Loads user-defined sql objects from a specified folder.
class UserDefinedSQLObjectsLoader : public IUserDefinedSQLObjectsLoader
{
public:
    UserDefinedSQLObjectsLoader(const ContextPtr & global_context_, const String & dir_path_);

    void checkAndLoadObjects() override;

    void reloadObjects() override;

    void reloadObject(UserDefinedSQLObjectType object_type, const String & object_name) override;

    bool storeObject(
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        const IAST & create_object_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    bool removeObject(UserDefinedSQLObjectType object_type, const String & object_name, bool throw_if_not_exists) override;

    bool storeObjectOnCatalog(
        const String & database_name, const String & object_name, const IAST & create_object_query) override;

    bool removeObjectOnCatalog(const String & database_name, const String & object_name) override;
    void checkAndLoadUDFFromStorage(
        const std::unordered_map<String, size_t> & udfs,
        ContextMutablePtr query_context) override;

private:
    void createDirectory();
    void loadObjectsImpl();
    ASTPtr tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name);
    ASTPtr
    tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name, const String & file_path, bool check_file_exists);
    String getFilePath(UserDefinedSQLObjectType object_type, const String & object_name) const;
    template <class T>
    void reloadObject(const String & object_name, ASTPtr ast);
    void loadUDFFromStorage(
        const std::unordered_set<String> & udf_to_fetch_from_storage,
        ContextMutablePtr query_context);

    ContextPtr global_context;
    String dir_path;
    Poco::Logger * log;
    std::atomic<bool> objects_loaded = false;
};

}
