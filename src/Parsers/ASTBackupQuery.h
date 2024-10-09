#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST.h>


namespace DB
{
using Strings = std::vector<String>;
using DatabaseAndTableName = std::pair<String, String>;
class ASTFunction;


/** BACKUP 
  *     DATABASE database_name [AS database_name_in_backup] [EXCEPT TABLES ...]
  *     | TABLE [db.]table_name [AS [db.]table_name_in_backup] [PARTITION[S] partition_id [,...]]
  * TO DISK('<disk_name>', '<path>/')
  * [SETTINGS ...]
  *
  * RESTORE
  *     DATABASE database_name_in_backup [AS database_name] [EXCEPT TABLES ...]
  *     | TABLE [db.]table_name_in_backup [AS [db.]table_name] [PARTITION[S] partition_id [,...]]
  * FROM DISK('<disk_name>', '<path>/');
  *
  * Notes:
  * RESTORE doesn't drop any data, it either creates a table or appends an existing table with restored data.
  * This behaviour can cause data duplication.
  * If appending isn't possible because the existing table has incompatible format then RESTORE will throw an exception.
  *
  * The "AS" clause is useful to backup or restore under another name.
  * For the BACKUP command this clause allows to set the name which an object will have inside the backup.
  * And for the RESTORE command this clause allows to set the name which an object will have after RESTORE has finished.
  */
class ASTBackupQuery : public IAST
{
public:
    enum Kind
    {
        BACKUP,
        RESTORE,
    };
    Kind kind = Kind::BACKUP;

    enum ElementType
    {
        TABLE,
        DATABASE,
    };

    struct Element
    {
        ElementType type;
        String table_name;
        String database_name;
        String new_table_name; /// usually the same as `table_name`, can be different in case of using AS <new_name>
        String new_database_name; /// usually the same as `database_name`, can be different in case of using AS <new_name>
        std::optional<ASTs> partitions;
        std::set<DatabaseAndTableName> except_tables;

        void setCurrentDatabase(const String & current_database);
    };

    using Elements = std::vector<Element>;
    static void setCurrentDatabase(Elements & elements, const String & current_database);
    void setCurrentDatabase(const String & current_database) { setCurrentDatabase(elements, current_database); }

    Elements elements;

    ASTFunction * backup_disk = nullptr;

    ASTPtr settings;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};

}
