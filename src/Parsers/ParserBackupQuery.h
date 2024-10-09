#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/**
 * BACKUP
 *     DATABASE database_name [AS database_name_in_backup] [EXCEPT TABLE|TABLES ...]
 *     | TABLE [db.]table_name [AS [db.]table_name_in_backup] [PARTITION[S] partition_expr [,...]]
 * TO DISK('<disk_name>', '<path>/')
 * [SETTINGS ...]
 *
 * RESTORE
 *     DATABASE database_name_in_backup [AS database_name] [EXCEPT TABLE|TABLES ...]
 *     | TABLE [db.]table_name_in_backup [AS [db.]table_name] [PARTITION[S] partition_expr [,...]]
 * FROM DISK('<disk_name>', '<path>/')
 * [SETTINGS ...];
 * 
 */
class ParserBackupQuery : public IParserBase
{
protected:
    const char * getName() const override { return "BACKUP or RESTORE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
