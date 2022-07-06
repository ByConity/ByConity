#pragma once

#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class ASTCreateQuery;
class ASTSetQuery;

/// see Databases/DatabaseOnDisk.h
extern String getObjectDefinitionFromCreateQuery(const ASTPtr & query, std::optional<bool> attach);

std::shared_ptr<ASTCreateQuery> getASTCreateQueryFromString(const String & query, const ContextPtr & context);
std::shared_ptr<ASTCreateQuery> getASTCreateQueryFromStorage(const IStorage & storage);

StoragePtr createStorageFromQuery(const String & query, const ContextPtr & context);

void replaceCnchWithCloud(ASTCreateQuery & create_query, const String & new_table_name, const String & cnch_db, const String & cnch_table);

void modifyOrAddSetting(ASTSetQuery & set_query, const String & name, Field value);
void modifyOrAddSetting(ASTCreateQuery & create_query, const String & name, Field value);

}
