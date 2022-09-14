#pragma once
#include <Interpreters/Context.h>
namespace DB
{

class ASTSetQuery;
class SettingsChanges;

enum class AlterTTLType
{
    add_ttl,
    modify_ttl,
    delete_ttl,
    none
};

bool createDatabaseInCatalog(
    ContextPtr global_context,
    const String & database_name,
    Poco::Logger& logger);

/// Detects change in table schema. Does not support modification of primary/partition keys
String makeAlterColumnQuery(
    const String& database,
    const String& table,
    const Block& expected,
    const Block& actual);

AlterTTLType getAlterTTLType(
    const String& ttl,
    StoragePtr& storage,
    Poco::Logger& logger);

/// Adds TTL if not present
String makeAlterTTLQuery(
    const String & database,
    const String & table,
    const String & ttl,
    AlterTTLType type,
    Poco::Logger& logger);

// Supports altering of settings
String makeAlterSettingsQuery(
    const String & database,
    const String & table,
    StoragePtr storage,
    ASTSetQuery * settings,
    Poco::Logger & logger);

bool createCnchTable(
    ContextPtr global_context,
    const String & database,
    const String & table,
    const String & query,
    Poco::Logger & logger);

bool prepareCnchTable(
    ContextPtr global_context,
    const String & database,
    const String & table,
    const String & create_query,
    Poco::Logger & logger);

bool syncTableSchema(
    ContextPtr global_context,
    const String & database,
    const String & table,
    const Block & expected_block,
    const String & ttl,
    const SettingsChanges & expected_settings,
    Poco::Logger & logger);

bool createView(
    ContextPtr global_context,
    const String & database,
    const String & table,
    Poco::Logger & logger);

}/// end namespace
