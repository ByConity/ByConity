#pragma once

#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
class Context;
class ASTCreateQuery;
class ASTSetQuery;

/// see Databases/DatabaseOnDisk.h
extern String getObjectDefinitionFromCreateQuery(const ASTPtr & query, std::optional<bool> attach = std::nullopt);

std::shared_ptr<ASTCreateQuery> getASTCreateQueryFromString(const String & query, const Context & context);
std::shared_ptr<ASTCreateQuery> getASTCreateQueryFromStorage(const IStorage & storage);

StoragePtr createStorageFromQuery(const String & query, const Context & context);

void replaceCnchWithCloud(ASTCreateQuery & ast, const String & new_table_name, const String & cnch_db, const String & cnch_table);

void modifyOrAddSetting(ASTSetQuery & ast, const String & name, Field value);
void modifyOrAddSetting(ASTCreateQuery & ast, const String & name, Field value);

}
