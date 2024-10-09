#include <Backups/BackupRenamingConfig.h>
#include <Parsers/ASTBackupQuery.h>


namespace DB
{
using Kind = ASTBackupQuery::Kind;
using ElementType = ASTBackupQuery::ElementType;

void BackupRenamingConfig::setNewTableName(const DatabaseAndTableName & old_table_name, const DatabaseAndTableName & new_table_name)
{
    old_to_new_table_names[old_table_name] = new_table_name;
}

void BackupRenamingConfig::setNewDatabaseName(const String & old_database_name, const String & new_database_name)
{
    old_to_new_database_names[old_database_name] = new_database_name;
}

void BackupRenamingConfig::setFromBackupQuery(const ASTBackupQuery & backup_query)
{
    setFromBackupQueryElements(backup_query.elements);
}

void BackupRenamingConfig::setFromBackupQueryElements(const ASTBackupQuery::Elements & backup_query_elements)
{
    for (const auto & element : backup_query_elements)
    {
        switch (element.type)
        {
            case ElementType::TABLE: {
                setNewTableName({element.database_name, element.table_name}, {element.new_database_name, element.new_table_name});
                break;
            }

            case ASTBackupQuery::DATABASE: {
                setNewDatabaseName(element.database_name, element.new_database_name);
                break;
            }
        }
    }
}

DatabaseAndTableName BackupRenamingConfig::getNewTableName(const DatabaseAndTableName & old_table_name) const
{
    auto it = old_to_new_table_names.find(old_table_name);
    if (it != old_to_new_table_names.end())
        return it->second;
    return {getNewDatabaseName(old_table_name.first), old_table_name.second};
}

const String & BackupRenamingConfig::getNewDatabaseName(const String & old_database_name) const
{
    auto it = old_to_new_database_names.find(old_database_name);
    if (it != old_to_new_database_names.end())
        return it->second;
    return old_database_name;
}
}
